/**
 * CopiBot – IA + Ventas + Soporte + GCal — Build R6.3 (2025-09-25)
 * Cambios clave R6.3:
 *  - FIX webhook: no usar variables antes de desestructurar ctx.
 *  - Inventario: si no hay stock igual muestra producto (mezcla con “sobre pedido”).
 *  - Búsqueda robusta: fallback amplio (ignoreFamily) antes de pedir “compatibles”.
 *  - Sesión dual-key: KV si existe binding, si no Supabase (tabla wa_session).
 */

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);

      // --- Webhook verify (WhatsApp) ---
      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      // --- Manual cron ---
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      // --- WhatsApp webhook principal ---
      if (req.method === 'POST' && url.pathname === '/') {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        // Desestructurar PRIMERO para evitar ReferenceError
        const { mid, from, fromE164, profileName, textRaw, msgType } = ctx;
        const originalText = (textRaw || '').trim();
        const lowered = originalText.toLowerCase();
        const ntext = normalizeWithAliases(originalText);

        // —— Debug de entrada (si lo necesitas, no rompe flujo)
        if (String(env.DEBUG_WEBHOOK || '').toLowerCase() === 'true') {
          try { await sendWhatsAppText(env, fromE164, `✅ webhook ok: "${originalText.slice(0, 50)}"`); } catch {}
          return ok('EVENT_RECEIVED');
        }

        // ===== Session =====
        const now = new Date();
        let session = await loadSessionMulti(env, from, fromE164); // KV si existe; fallback Supabase
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;

        // Nombre suave por profile
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // Idempotencia por mid
        if (session?.data?.last_mid && session.data.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // NO-TEXTO (excepto interactive -> ya se mapea a text)
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, `¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂`);
          await saveSessionMulti(env, session, from, fromE164);
          return ok('EVENT_RECEIVED');
        }

        /* ============================================================
         *  BARRERA 0: si veníamos en Soporte, Soporte tiene prioridad
         * ============================================================ */
        if (session.stage?.startsWith('sv_') || session?.data?.intent_lock === 'support') {
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        /* ============================================================
         *  REGLA DURA: texto parece Marca+Modelo (sin palabra de compra)
         * ============================================================ */
        const SALES_WORDS = /\b(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio)\b/i;
        const pmGuard = parseBrandModel(ntext);
        if (pmGuard?.modelo && !SALES_WORDS.test(ntext)) {
          session.data.intent_lock = 'support';
          if (!session.stage?.startsWith('sv_')) {
            session.stage = 'sv_collect';
            session.data.sv_need_next = 'falla';
            session.data.sv = session.data.sv || {};
            if (pmGuard.marca) session.data.sv.marca = pmGuard.marca;
            if (pmGuard.modelo) session.data.sv.modelo = pmGuard.modelo;
          }
          await saveSessionMulti(env, session, from, fromE164);
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        /* =================== Saludo =================== */
        if (RX_GREET.test(lowered)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          session.data.last_greet_at = now.toISOString();
          await saveSessionMulti(env, session, from, fromE164);
          return ok('EVENT_RECEIVED');
        }

        /* ====== Comandos universales de soporte ====== */
        if (/\b(cancel(a|ar).*(cita|visita|servicio))\b/i.test(lowered)) {
          session.data.intent_lock = 'support';
          await svCancel(env, session, fromE164);
          await saveSessionMulti(env, session, from, fromE164);
          return ok('EVENT_RECEIVED');
        }
        if (/\b(reprogram|mueve|cambia|modif)\w*/i.test(lowered)) {
          const when = parseNaturalDateTime(lowered, env);
          if (when?.start) {
            session.data.intent_lock = 'support';
            await svReschedule(env, session, fromE164, when);
            await saveSessionMulti(env, session, from, fromE164);
            return ok('EVENT_RECEIVED');
          }
        }
        if (/\b(cu[aá]ndo|cuando).*(cita|visita|servicio)\b/i.test(lowered)) {
          session.data.intent_lock = 'support';
          await svWhenIsMyVisit(env, session, fromE164);
          await saveSessionMulti(env, session, from, fromE164);
          return ok('EVENT_RECEIVED');
        }

        /* =================== Intenciones =================== */
        const supportIntent = isSupportIntent(ntext) || (await intentIs(env, originalText, 'support'));
        if (supportIntent) {
          session.data.intent_lock = 'support';
          await saveSessionMulti(env, session, from, fromE164);
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        const salesIntent = RX_INV_Q.test(ntext) || (await intentIs(env, originalText, 'sales'));

        /* =================== Ventas =================== */
        if (salesIntent) {
          const handled = await startSalesFromQuery(env, session, fromE164, originalText, ntext, now);
          return handled;
        }

        // ===== Stages de ventas activos =====
        if (session.stage === 'ask_qty') return await handleAskQty(env, session, fromE164, originalText, lowered, ntext, now);
        if (session.stage === 'cart_open') return await handleCartOpen(env, session, fromE164, originalText, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, originalText);
        if (session.stage?.startsWith('collect_')) return await handleCollectSequential(env, session, fromE164, originalText, now);

        // ===== FAQs =====
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSessionMulti(env, session, from, fromE164);
          return ok('EVENT_RECEIVED');
        }

        // ===== Fallback IA =====
        const reply = await aiSmallTalk(env, session, 'fallback', originalText);
        await sendWhatsAppText(env, fromE164, reply);
        await saveSessionMulti(env, session, from, fromE164);
        return ok('EVENT_RECEIVED');
      }

      return new Response('Not found', { status: 404 });
    } catch (e) {
      console.error('Worker error', e);
      return ok('EVENT_RECEIVED');
    }
  },

  async scheduled(_event, env) {
    try { await cronReminders(env); } catch (e) { console.error('cron error', e); }
  }
};

/* ========================================================================== */
/* ============================ Constantes/Regex ============================ */
/* ========================================================================== */

const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_Q = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro|yellow|black|bk|k)\b/i;

function isSupportIntent(ntext='') {
  const t = `${ntext}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|prioridad)/.test(t);
  const hasDevice = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase = /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);
  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

/* ========================================================================== */
/* =============================== Helpers ================================== */
/* ========================================================================== */

const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalizeBase(s=''){ return String(s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return String(s||'').replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function ok(s='ok'){ return new Response(s, { status: 200 }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }

function dlog(env, ...args){ if ((env.DEBUG||'').toString().toLowerCase()==='true'){ console.log(...args); } }
function jlog(env, type, obj){
  if ((env.DEBUG_JSON||'').toString().toLowerCase()!=='true') return;
  try{ console.log(`[DBG:${type}]`, JSON.stringify(obj)); }catch{}
}

function fmtDate(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }
  catch{ return new Date(d).toLocaleDateString('es-MX'); }
}
function fmtTime(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }
  catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; }
}
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function priceWithIVA(n){ const v=Number(n||0); return `${formatMoneyMXN(v)} + IVA`; }

/* ========================================================================== */
/* =========================== Circuit Breaker ============================== */
/* ========================================================================== */

const CB_KEYS = { supabase: 'cb_supabase', gcal: 'cb_gcal' };

async function cbGet(env, key){
  try{
    const raw = await kvGet(env, `cb:${key}`);
    return raw ? JSON.parse(raw) : { fails:0, until:0 };
  }catch{ return { fails:0, until:0 }; }
}
async function cbFail(env, key){
  try{
    const limit = Number(env.CB_FAIL_LIMIT||3);
    const cooldown = Number(env.CB_COOLDOWN_SEC||60);
    const st = await cbGet(env, key);
    st.fails = (st.fails||0)+1;
    if (st.fails>=limit){ st.until = Date.now()+cooldown*1000; }
    await kvPut(env, `cb:${key}`, JSON.stringify(st));
  }catch{}
}
async function cbOk(env, key){
  try{ await kvPut(env, `cb:${key}`, JSON.stringify({fails:0, until:0})); }catch{}
}
async function cbOpen(env, key){
  const st = await cbGet(env, key);
  return Date.now() < Number(st.until||0);
}

/* ========================================================================== */
/* ============================ Fetch robusto =============================== */
/* ========================================================================== */

async function fetchWithTimeout(url, opts={}, { retries=2, timeouts=[500,1000,2000], cbKey=null, env=null } = {}){
  let lastErr = null;
  for (let i=0; i<=retries; i++){
    const ctrl = new AbortController();
    const to = setTimeout(()=>ctrl.abort(), timeouts[Math.min(i, timeouts.length-1)]);
    try{
      const r = await fetch(url, { ...opts, signal: ctrl.signal });
      clearTimeout(to);
      if (!r.ok){
        lastErr = new Error(`HTTP ${r.status}`);
        if (env && cbKey) await cbFail(env, cbKey);
      } else {
        if (env && cbKey) await cbOk(env, cbKey);
        return r;
      }
    }catch(e){
      clearTimeout(to);
      lastErr = e;
      if (env && cbKey) await cbFail(env, cbKey);
    }
    // backoff simple
    await new Promise(res=>setTimeout(res, 150*(i+1)));
  }
  throw lastErr || new Error('fetchWithTimeout failed');
}

/* ========================================================================== */
/* ================================ KV Proxy ================================= */
/* ========================================================================== */
/** KV si existe binding COPIBOT_KV; si no, emulamos con Supabase wa_session */
async function kvGet(env, key){
  if (env.COPIBOT_KV && env.COPIBOT_KV.get) {
    return await env.COPIBOT_KV.get(key);
  }
  // Fallback a Supabase (tabla wa_session)
  try{
    const r = await sbGet(env, 'wa_session', { query: `select=val&key=eq.${encodeURIComponent(key)}&limit=1` });
    return r?.[0]?.val || null;
  }catch{ return null; }
}
async function kvPut(env, key, val, ttlSec=7*24*60*60){
  if (env.COPIBOT_KV && env.COPIBOT_KV.put) {
    try{ await env.COPIBOT_KV.put(key, val, { expirationTtl: ttlSec }); }catch{}
    return;
  }
  // Fallback Supabase
  try{
    await sbUpsert(env, 'wa_session', [{ key, val }], { onConflict: 'key', returning: 'minimal' });
  }catch{}
}

/* ========================================================================== */
/* =========================== Métricas simples ============================= */
/* ========================================================================== */

async function metricInc(env, k){
  try{
    const cur = Number(await kvGet(env, `metric:${k}`) || 0);
    await kvPut(env, `metric:${k}`, String(cur+1), 30*24*60*60);
  }catch{}
}

/* ========================================================================== */
/* =============================== Trazas =================================== */
/* ========================================================================== */

function newTraceId(){ return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`; }


/* ========================================================================== */
/* ================================= IA ===================================== */
/* ========================================================================== */

async function aiCall(env, messages, {json=false}={}) {
  const OPENAI_KEY = env.OPENAI_API_KEY || env.OPENAI_KEY;
  const MODEL = env.LLM_MODEL || env.OPENAI_NLU_MODEL || env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  if (!OPENAI_KEY) return null;
  const body = { model: MODEL, messages, temperature: json ? 0 : 0.3, ...(json ? { response_format: { type: "json_object" } } : {}) };
  try{
    const r = await fetchWithTimeout('https://api.openai.com/v1/chat/completions', {
      method:'POST',
      headers:{ 'Authorization':`Bearer ${OPENAI_KEY}`, 'Content-Type':'application/json' },
      body: JSON.stringify(body)
    }, { retries: 2, cbKey: null, env });
    if (!r.ok) { dlog(env,'aiCall http', r.status); return null; }
    const j = await r.json();
    return j?.choices?.[0]?.message?.content || '';
  }catch(e){
    dlog(env,'aiCall err', e);
    return null;
  }
}

async function aiSmallTalk(env, session, mode='general', userText=''){
  const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde breve, humano y útil. Máx 1 emoji.`;
  const prompt = mode==='fallback'
    ? `El usuario dijo: """${userText}""". Responde breve y ofrece inventario o soporte si aplica.`
    : `El usuario dijo: """${userText}""". Responde breve.`;
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 👋 ¿En qué te ayudo?`);
}

async function intentIs(env, text, expected){
  try{
    const sys = `Clasifica en JSON {"intent":"support|sales|faq|smalltalk"}. Sólo una palabra. Idioma es-MX.`;
    const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
    const j = JSON.parse(out||'{}'); return j?.intent === expected;
  }catch{return false;}
}

/** IA opcional para reforzar NER de inventario */
async function aiExtractTonerQuery(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Extrae de una consulta (es-MX) sobre tóners los campos { "familia": "versant|docucolor|primelink|versalink|altalink|apeos|c70|", "color": "yellow|magenta|cyan|black|null", "subfamilia": "string|null", "cantidad": "number|null" } en JSON. No inventes.`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try { return JSON.parse(out||'{}'); } catch { return null; }
}

/* ========================================================================== */
/* =============================== WhatsApp ================================= */
/* ========================================================================== */

function extractWhatsAppContext(payload) {
  try {
    const value = payload?.entry?.[0]?.changes?.[0]?.value;
    const msg = value?.messages?.[0];
    if (!msg) return null;

    let textRaw = '';
    let msgType = 'text';

    if (msg.type === 'text') textRaw = msg.text?.body || '';
    else if (msg.type === 'interactive' && msg.interactive?.type === 'button_reply') {
      textRaw = msg.interactive?.button_reply?.title || msg.interactive?.button_reply?.id || '';
      msgType = 'text';
    } else if (msg.type === 'interactive' && msg.interactive?.type === 'list_reply') {
      textRaw = msg.interactive?.list_reply?.title || msg.interactive?.list_reply?.id || '';
      msgType = 'text';
    } else {
      msgType = 'media';
    }

    const from = msg.from;
    const fromE164 = `+${from}`;
    const mid = msg.id || `${Date.now()}_${Math.random()}`;
    const profileName = value?.contacts?.[0]?.profile?.name || '';
    return { msg, from, fromE164, mid, textRaw, profileName, msgType };
  } catch { return null; }
}

async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) { console.warn('WA env missing'); return; }
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = { messaging_product: 'whatsapp', to: String(toE164||'').replace(/\D/g, ''), text: { body: String(body ?? '') } };
  try{
    const r = await fetchWithTimeout(url, {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.WA_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }, { retries: 2, env });
    if (!r.ok) console.warn('sendWhatsAppText', r.status, await r.text());
  }catch(e){ console.warn('sendWhatsAppText error', e); }
}

async function notifySupport(env, body) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  await sendWhatsAppText(env, to, `🛎️ *Soporte*\n${body}`);
}

/* ========================================================================== */
/* =========================== Sesión (KV / Supabase) ======================= */
/* ========================================================================== */

async function loadSessionMulti(env, from, fromE164){
  try{
    // Intenta KV
    const a = await kvGet(env, `sess:${fromE164}`);
    if (a) return JSON.parse(a);
    const b = await kvGet(env, `sess:${from}`);
    if (b) return JSON.parse(b);

    // Fallback Supabase (wa_session)
    const row = await sbGet(env, 'wa_session', { query: `select=val&key=eq.${encodeURIComponent('sess:'+fromE164)}&limit=1` });
    if (row && row[0]?.val) return JSON.parse(row[0].val);

    const row2 = await sbGet(env, 'wa_session', { query: `select=val&key=eq.${encodeURIComponent('sess:'+from)}&limit=1` });
    if (row2 && row2[0]?.val) return JSON.parse(row2[0].val);

    return { from, stage:'idle', data:{} };
  }catch{
    return { from, stage:'idle', data:{} };
  }
}
async function saveSessionMulti(env, sess, from, fromE164){
  try{
    const val = JSON.stringify(sess);
    await kvPut(env, `sess:${from}`, val, 60*60*24*7);
    await kvPut(env, `sess:${fromE164}`, val, 60*60*24*7);
  }catch{}
}

/* ========================================================================== */
/* ============================== Inventario/Ventas ========================= */
/* ========================================================================== */

/* --- Patrones y parsers de carrito --- */
const RX_WANT_QTY = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+(\d+)\b/i;
const RX_ADD_ITEM = /\b(agrega(?:me)?|añade|mete|pon|suma|incluye)\b/i;

function parseQty(text, fallback = 1) {
  const m = text.match(RX_WANT_QTY);
  const q = m ? Number(m[2]) : null;
  if (q && q > 0) return q;
  const n = (text.match(/\b(\d+)\b/) || [null, null])[1];
  return n ? Number(n) : fallback;
}

function pushCart(session, product, qty, backorder = false) {
  session.data = session.data || {};
  session.data.cart = session.data.cart || [];
  const key = `${product?.sku || product?.id || product?.nombre}${backorder?'_bo':''}`;
  const existing = session.data.cart.find(i => i.key === key);
  if (existing) existing.qty += qty;
  else session.data.cart.push({ key, product, qty, backorder });
}

function addWithStockSplit(session, product, qty){
  const s = numberOrZero(product?.stock);
  const take = Math.min(s, qty);
  const rest = Math.max(0, qty - take);
  if (take > 0) pushCart(session, product, take, false);
  if (rest > 0) pushCart(session, product, rest, true);
}

/* --- Normalizador de aliases de familias/modelos --- */
function normalizeWithAliases(s=''){
  const t = normalizeBase(s);
  const aliases = [
    ['verzan','versant'], ['versan','versant'], ['vrsant','versant'], ['verzand','versant'],
    ['dococolor','docucolor'], ['docucolour','docucolor'], ['docu color','docucolor'],
    ['versa link','versalink'], ['altaling','altalink'], ['alta link','altalink'],
    ['prime link','primelink'], ['prime-link','primelink'], ['prymelink','primelink'],
    ['fuji film','fujifilm'], ['docucolor 5560','docucolor 550/560/570']
  ];
  let out = t;
  for (const [bad, good] of aliases){
    out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  }
  return out;
}

/* --- Render del producto en copy de WhatsApp --- */
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1) ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

/* --- Flujo: pedir cantidad y abrir carrito --- */
async function handleAskQty(env, session, toE164, text, lowered, ntext, now){
  const cand = session.data?.last_candidate;
  if (!cand) {
    session.stage = 'cart_open';
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, 'No alcancé a ver el artículo. ¿Lo repetimos o buscas otro? 🙂');
    return ok('EVENT_RECEIVED');
  }
  const qty = parseQty(lowered, 1);
  addWithStockSplit(session, cand, qty);
  session.stage = 'cart_open';
  await saveSessionMulti(env, session, session.from, toE164);
  const s = numberOrZero(cand.stock);
  const bo = Math.max(0, qty - Math.min(s, qty));
  const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} *sobre pedido*)` : '';
  await sendWhatsAppText(env, toE164, `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${nota}\n\n¿Deseas agregar algo más o *finalizamos*?`);
  return ok('EVENT_RECEIVED');
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext, now) {
  session.data = session.data || {};
  const cart = session.data.cart || [];

  const RX_DONE = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;
  const RX_NEG_NO = /\b(no|nel|ahorita no)\b/i;

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate) {
      addWithStockSplit(session, session.data.last_candidate, 1);
    }
    session.stage = 'await_invoice';
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `Perfecto 🙌 ¿La cotizamos *con factura* o *sin factura*?`);
    return ok('EVENT_RECEIVED');
  }

  const RX_YES_CONFIRM = /\b(s[ií]|sí|si|claro|va|dale|correcto|ok|afirmativo|hazlo|agr[eé]ga(lo)?|añade|m[eé]te|pon(lo)?)\b/i;
  if (RX_YES_CONFIRM.test(lowered)) {
    const c = session.data?.last_candidate;
    if (c) {
      session.stage = 'ask_qty';
      await saveSessionMulti(env, session, session.from, toE164);
      const s = numberOrZero(c.stock);
      await sendWhatsAppText(env, toE164, `De acuerdo. ¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto iría *sobre pedido*)`);
      return ok('EVENT_RECEIVED');
    }
  }

  if (RX_WANT_QTY.test(lowered)) {
    session.stage = 'ask_qty';
    await saveSessionMulti(env, session, session.from, toE164);
    const c = session.data?.last_candidate;
    const s = numberOrZero(c?.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto iría *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  if (RX_ADD_ITEM.test(lowered) || RX_INV_Q.test(ntext)) {
    const cleanQ = lowered.replace(RX_ADD_ITEM, '').trim() || ntext;

    // IA opcional para NER
    const extracted = await aiExtractTonerQuery(env, cleanQ).catch(()=>null);
    const enrichedQ = enrichQueryFromAI(cleanQ, extracted);

    const best = await findBestProduct(env, enrichedQ); // familia+color = filtro duro si viene
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSessionMulti(env, session, session.from, toE164);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
      return ok('EVENT_RECEIVED');
    } else {
      await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. ¿Busco otra opción o lo revisa un asesor?`);
      await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
      await saveSessionMulti(env, session, session.from, toE164);
      return ok('EVENT_RECEIVED');
    }
  }

  await sendWhatsAppText(env, toE164, `Te leo 🙂. Puedo agregar el artículo visto, buscar otro o *finalizar* si ya está completo.`);
  await saveSessionMulti(env, session, session.from, toE164);
  return ok('EVENT_RECEIVED');
}

/* --- Hints de familia/color desde texto --- */
function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};
  // Familia
  if (/\bversant\b/.test(t) || /\b(80|180|2100|280|4100)\b/.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/.test(t) || /\bprimelink\b/.test(t)) out.family = 'primelink';
  else if (/\bapeos\b/.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/.test(t)) out.family = 'c70';

  // Color
  if (/\b(amarillo|yellow)\b/.test(t)) out.color = 'yellow';
  else if (/\bmagenta\b/.test(t)) out.color = 'magenta';
  else if (/\b(cyan|cian)\b/.test(t)) out.color = 'cyan';
  else if (/\b(negro|black|bk|k)\b/.test(t)) out.color = 'black';

  return out;
}

function productHasColor(p, colorCode){
  if (!colorCode) return true;
  const s = `${normalizeBase([p?.nombre, p?.sku, p?.marca].join(' '))}`;
  const map = {
    yellow:[/\bamarillo\b/i, /\byellow\b/i, /(^|[\s\-_\/])y($|[\s\-_\/])/i, /(^|[\s\-_\/])ylw($|[\s\-_\/])/i],
    magenta:[/\bmagenta\b/i, /(^|[\s\-_\/])m($|[\s\-_\/])/i],
    cyan:[/\bcyan\b/i, /\bcian\b/i, /(^|[\s\-_\/])c($|[\s\-_\/])/i],
    black:[/\bnegro\b/i, /\bblack\b/i, /(^|[\s\-_\/])k($|[\s\-_\/])/i, /(^|[\s\-_\/])bk($|[\s\-_\/])/i],
  };
  const arr = map[colorCode] || [];
  return arr.some(rx => rx.test(p?.nombre) || rx.test(p?.sku) || rx.test(s));
}

function productMatchesFamily(p, family){
  if (!family) return true;
  const s = normalizeBase([p?.nombre, p?.sku, p?.marca, p?.compatible].join(' '));

  if (family==='versant'){
    const hit = /\bversant\b/i.test(s) || /\b(80|180|2100|280|4100)\b/i.test(s);
    const bad = /(c60|c70|c75|docucolor|prime\s*link|primelink|altalink|versa\s*link|\b550\b|\b560\b|\b570\b)/i.test(s);
    return hit && !bad;
  }
  if (family==='docucolor'){
    const hit = /\b(docucolor|550\/560\/570|550|560|570)\b/i.test(s);
    const bad = /(versant|primelink|altalink|versalink|c60|c70|c75|2100|180|280|4100)\b/i.test(s);
    return hit && !bad;
  }
  if (family==='c70') return /\bc(60|70|75)\b/i.test(s) || s.includes('c60') || s.includes('c70') || s.includes('c75');
  if (family==='primelink') return /\bprime\s*link\b/i.test(s) || /\bprimelink\b/i.test(s);
  if (family==='versalink') return /\bversa\s*link\b/i.test(s) || /\bversalink\b/i.test(s);
  if (family==='altalink') return /\balta\s*link\b/i.test(s) || /\baltalink\b/i.test(s);
  if (family==='apeos') return /\bapeos\b/i.test(s);
  return s.includes(family);
}

function extractColorWord(text=''){
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return 'yellow';
  if (/\bmagenta\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'black';
  return null;
}

function enrichQueryFromAI(q, ai){
  if (!ai) return q;
  let out = q;
  if (ai.familia && !new RegExp(`\\b${ai.familia}\\b`).test(out)) out += ` ${ai.familia}`;
  if (ai.color && !new RegExp(`\\b(${ai.color}|amarillo|magenta|cyan|cian|negro|black|bk|k|yellow)\\b`).test(out)) {
    const map = {yellow:'amarillo', magenta:'magenta', cyan:'cyan', black:'negro'};
    out += ` ${map[ai.color] || ai.color}`;
  }
  if (ai.subfamilia && !out.includes(ai.subfamilia)) out += ` ${ai.subfamilia}`;
  if (ai.cantidad && !/\b\d+\b/.test(out)) out += ` ${ai.cantidad}`;
  return out;
}

/* === findBestProduct: siempre devuelve la mejor opción posible (con o sin stock) === */
async function findBestProduct(env, queryText, opts = {}) {
  const hints = extractModelHints(queryText);
  const colorCode = hints.color || extractColorWord(queryText);

  const pick = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = colorCode ? arr.filter(p => productHasColor(p, colorCode)) : arr.slice();

    if (hints.family && !opts.ignoreFamily) {
      pool = pool.filter(p => productMatchesFamily(p, hints.family));
      if (!pool.length) return null;
    }

    // Orden: con stock primero, luego precio ascendente; pero si nadie tiene stock, igual devuelve algo
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });

    return pool[0] || null;
  };

  try {
    // RPC de similitud (si lo tienes creado)
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 40 }) || [];
    const best = pick(res);
    if (best) return best;
  } catch {}

  // Si venía familia, busca por ilike familia
  if (hints.family && !opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like},compatible.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
      // Si no encontró estrictamente, relaja familia
    } catch {}
  }

  // Búsqueda general por “toner” como fallback
  try {
    const like = encodeURIComponent(`%toner%`);
    const r = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
    }) || [];
    const best = pick(r);
    if (best) return best;
  } catch {}

  return null;
}

/* --- Arranque de ventas desde texto libre --- */
async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  const extracted = await aiExtractTonerQuery(env, ntext).catch(()=>null);
  const enrichedQ = enrichQueryFromAI(ntext, extracted);
  const best = await findBestProduct(env, enrichedQ);

  const hints = extractModelHints(enrichedQ);

  if (!best && hints.family) {
    // No hay match; pero mantenemos conversación amable
    await sendWhatsAppText(env, toE164, `Ese modelo parece estar *sobre pedido* o sin disponibilidad inmediata. ¿Busco una opción *compatible* o lo ve un asesor humano?`);
    await notifySupport(env, `Inventario sin match (family=${hints.family}). +${session.from}: ${text}`);
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  if (best) {
    session.stage = 'ask_qty';
    session.data.cart = session.data.cart || [];
    session.data.last_candidate = best;
    await saveSessionMulti(env, session, session.from, toE164);
    const s = numberOrZero(best.stock);
    await sendWhatsAppText(
      env, toE164,
      `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
    );
    return ok('EVENT_RECEIVED');
  } else {
    await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. Te conecto con un asesor…`);
    await notifySupport(env, `Inventario sin match. +${session.from}: ${text}`);
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }
}

/* ========================================================================== */
/* ============================== Cliente/Pedido ============================ */
/* ========================================================================== */

/* --- Flujo de factura / sin factura --- */
async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  // Salida amable si el usuario se desvía sin contestar
  if (!/\b(con|sin)\b|\bfactura\b/i.test(lowered) && /hola|gracias|buen[oa]s|ok/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', originalText);
    await sendWhatsAppText(env, toE164, friendly);
    if (!promptedRecently(session, 'invoice', 3*60*1000)) {
      await sendWhatsAppText(env, toE164, `Por cierto, ¿la quieres *con factura* o *sin factura*?`);
    }
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  const saysNo = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  if (!saysYes && !saysNo) {
    if (!promptedRecently(session, 'invoice', 90*1000)) {
      await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
    }
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  session.data.requires_invoice = !!saysYes;

  await preloadCustomerIfAny(env, session);

  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const need = firstMissing(list, session.data.customer);
  if (need) {
    session.stage = `collect_${need}`;
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, askLabelWithPrefill(session.data.customer, need));
    return ok('EVENT_RECEIVED');
  }

  // Si ya está todo capturado, creamos pedido
  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) {
    await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
    await notifySupport(env, `Nuevo pedido #${res.pedido_id ?? '—'}\nCliente: ${session.data.customer?.nombre || 'N/D'} (${toE164})\nFactura: ${session.data.requires_invoice ? 'Sí' : 'No'}`);
  } else {
    await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor para confirmar detalles. 🙌`);
    await notifySupport(env, `Pedido (parcial) ${toE164}. Revisar en Supabase.\nError: ${res?.error || 'N/A'}`);
  }

  session.stage = 'idle';
  session.data.cart = [];
  await saveSessionMulti(env, session, session.from, toE164);
  await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más en este momento? (Sí / No)`);
  return ok('EVENT_RECEIVED');
}

/* --- Captura UNO A UNO con prefill y confirmación --- */
const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL = {
  nombre:'Nombre / Razón Social',
  rfc:'RFC',
  email:'Email',
  calle:'Calle',
  numero:'Número',
  colonia:'Colonia',
  cp:'Código Postal'
};

function firstMissing(list, c={}){ for (const k of list){ if (!truthy(c[k])) return k; } return null; }

function askLabelWithPrefill(c={}, k){
  const base = `¿${LABEL[k]}?`;
  if (truthy(c[k])) return `${base}\nTengo: *${c[k]}*. Si está bien, responde “ok”; si no, envíame el dato correcto.`;
  return base;
}

function parseCustomerFragment(field, text){
  const t = text;
  if (/^\s*ok\s*$/i.test(t)) return '__USE_PREFILL__';

  if (field==='nombre') return clean(t);
  if (field==='rfc'){ const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{2,3})\b/i); return m ? m[1].toUpperCase() : clean(t).toUpperCase(); }
  if (field==='email'){ const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); return m ? m[0].toLowerCase() : clean(t).toLowerCase(); }
  if (field==='numero'){ const m = t.match(/\b(\d+[A-Z]?)\b/i); return m ? m[1] : clean(t); }
  if (field==='cp'){ const m = t.match(/\b(\d{5})\b/); return m ? m[1] : clean(t); }
  return clean(t);
}

async function handleCollectSequential(env, session, toE164, text, now){
  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;

  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = session.stage.replace('collect_','');

  const value = parseCustomerFragment(field, text);
  if (value !== '__USE_PREFILL__') {
    c[field] = value;
  } else if (!truthy(c[field])) {
    // Dijo "ok" pero no hay prefill
    await sendWhatsAppText(env, toE164, `No tenía un dato previo para *${LABEL[field]}*. Por favor, indícalo.`);
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  // Autocompletar ciudad/estado por CP si no están
  if (field==='cp' && !c.ciudad) {
    const info = await cityFromCP(env, c.cp);
    if (info) {
      c.ciudad = info.ciudad || info.municipio || c.ciudad;
      c.estado = info.estado || c.estado;
    }
  }

  await saveSessionMulti(env, session, session.from, toE164);

  const nextField = firstMissing(list, c);
  if (nextField){
    session.stage = `collect_${nextField}`;
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, askLabelWithPrefill(c, nextField));
    return ok('EVENT_RECEIVED');
  }

  // Confirmación amable de datos antes de crear el pedido
  const resumen = [
    `• Nombre: ${c.nombre || '—'}`,
    session.data.requires_invoice ? `• RFC: ${c.rfc || '—'}` : null,
    `• Email: ${c.email || '—'}`,
    `• Dirección: ${[c.calle, c.numero, c.colonia, c.cp].filter(Boolean).join(' ') || '—'}`,
    c.ciudad ? `• Ciudad: ${c.ciudad}` : null,
    c.estado ? `• Estado: ${c.estado}` : null
  ].filter(Boolean).join('\n');

  await sendWhatsAppText(env, toE164, `Perfecto, tengo estos datos:\n${resumen}\n\nSi todo está *correcto*, responde “ok”. Si deseas *corregir* algo, escribe el campo y el valor (ej: "email correo@dominio.com").`);

  // Pequeño “modo edición” inline durante 2 turnos
  session.stage = 'await_confirm_customer';
  session.data.confirm_attempts = 0;
  await saveSessionMulti(env, session, session.from, toE164);
  return ok('EVENT_RECEIVED');
}

/* --- Confirmación/Edición de datos capturados --- */
async function handleConfirmCustomer(env, session, toE164, text){
  const c = session.data?.customer || {};
  const t = normalizeBase(text);

  // Aceptación
  if (/^\s*ok\s*$/i.test(text) || /\bcorrect(o|a)\b/i.test(t)) {
    const res = await createOrderFromSession(env, session, toE164);
    if (res?.ok) {
      await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
      await notifySupport(env, `Nuevo pedido #${res.pedido_id ?? '—'}\nCliente: ${c?.nombre || 'N/D'} (${toE164})\nFactura: ${session.data.requires_invoice ? 'Sí' : 'No'}`);
    } else {
      await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
      await notifySupport(env, `Pedido (parcial) ${toE164}. Error: ${res?.error || 'N/A'}`);
    }
    session.stage = 'idle';
    session.data.cart = [];
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `¿Puedo ayudarte con algo más? (Sí / No)`);
    return ok('EVENT_RECEIVED');
  }

  // Edición “campo valor”
  const editMap = { nombre:'nombre', rfc:'rfc', email:'email', calle:'calle', numero:'numero', colonia:'colonia', cp:'cp', ciudad:'ciudad', estado:'estado' };
  const m = t.match(/\b(nombre|rfc|email|calle|numero|colonia|cp|ciudad|estado)\b\s+(.+)/i);
  if (m) {
    const field = editMap[m[1].toLowerCase()];
    const raw = text.replace(/^\s*(nombre|rfc|email|calle|numero|colonia|cp|ciudad|estado)\b\s*/i, '');
    c[field] = parseCustomerFragment(field, raw);
    if (field==='cp' && !c.ciudad) {
      const info = await cityFromCP(env, c.cp);
      if (info) {
        c.ciudad = info.ciudad || info.municipio || c.ciudad;
        c.estado = info.estado || c.estado;
      }
    }
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `Listo, actualicé *${LABEL[field] || field}* ✅\n¿Confirmamos? Escribe “ok” o corrige otro dato.`);
    return ok('EVENT_RECEIVED');
  }

  // Re-pedir confirmación con límite de intentos
  session.data.confirm_attempts = Number(session.data.confirm_attempts||0) + 1;
  if (session.data.confirm_attempts >= 2) {
    await sendWhatsAppText(env, toE164, `Si te parece, lo revisa un asesor y te confirma. 🙂`);
    await notifySupport(env, `Confirmación de datos no resuelta con ${toE164}.`);
    session.stage = 'idle';
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, `¿Confirmamos con “ok” o deseas editar algún dato? (ej: "email correo@dominio.com")`);
  await saveSessionMulti(env, session, session.from, toE164);
  return ok('EVENT_RECEIVED');
}

/* --- Cliente en base + creación de pedido + decremento de stock --- */
async function preloadCustomerIfAny(env, session){
  try{
    const r = await sbGet(env, 'cliente', { query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${session.from}&limit=1` });
    if (r && r[0]) {
      session.data.customer = { ...(session.data.customer||{}), ...r[0] };
    }
  }catch(e){ console.warn('preloadCustomerIfAny', e); }
}

async function ensureClienteFields(env, cliente_id, c){
  try{
    const patch = {};
    ['nombre','rfc','email','calle','numero','colonia','ciudad','estado','cp','telefono'].forEach(k=>{ if (truthy(c[k])) patch[k]=c[k]; });
    if (Object.keys(patch).length>0) await sbPatch(env, 'cliente', patch, `id=eq.${cliente_id}`);
  }catch(e){ console.warn('ensureClienteFields', e); }
}

async function createOrderFromSession(env, session, toE164) {
  try {
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok: false, error: 'empty cart' };
    const c = session.data.customer || {};
    let cliente_id = null;

    // Localiza o crea cliente por teléfono o email
    try {
      const exist = await sbGet(env, 'cliente', {
        query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email || '')})&limit=1`
      });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch {}

    if (!cliente_id) {
      const ins = await sbUpsert(env, 'cliente', [{
        nombre: c.nombre || null, rfc: c.rfc || null, email: c.email || null,
        telefono: session.from || null, calle: c.calle || null, numero: c.numero || null,
        colonia: c.colonia || null, ciudad: c.ciudad || null, estado: c.estado || null, cp: c.cp || null
      }], { onConflict: 'telefono', returning: 'representation' });
      cliente_id = ins?.data?.[0]?.id || null;
    } else {
      await ensureClienteFields(env, cliente_id, { ...c, telefono: session.from });
    }

    let total = 0;
    for (const it of cart) total += Number(it.product?.precio || 0) * Number(it.qty || 1);

    const p = await sbUpsert(env, 'pedido', [{
      cliente_id, total, moneda: 'MXN', estado: 'nuevo', created_at: new Date().toISOString()
    }], { returning: 'representation' });
    const pedido_id = p?.data?.[0]?.id;

    const items = cart.map(it => ({
      pedido_id, producto_id: it.product?.id || null, sku: it.product?.sku || null,
      nombre: it.product?.nombre || null, qty: it.qty, precio_unitario: Number(it.product?.precio || 0),
      backorder: !!it.backorder
    }));
    await sbUpsert(env, 'pedido_item', items, { returning: 'minimal' });

    // Decremento de stock por RPC (hasta stock actual)
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = numberOrZero(row?.[0]?.stock);
        const toDec = Math.min(current, Number(it.qty||0));
        if (toDec > 0 && !it.backorder) await sbRpc(env, 'decrement_stock', { in_sku: sku, in_by: toDec });
      } catch(e){ console.warn('stock dec', e); }
    }

    return { ok: true, pedido_id, total };
  } catch (e) {
    console.warn('createOrderFromSession', e);
    return { ok: false, error: String(e) };
  }
}

/* --- Utilidades de dirección/CP --- */
async function cityFromCP(env, cp){
  if (!cp) return null;
  const tables = (env.CP_TABLE || 'sepomex_cp_v,cp_mx').split(',');
  for (const t of tables){
    try{
      const r = await sbGet(env, t.trim(), { query: `select=cp,ciudad,municipio,estado&cp=eq.${encodeURIComponent(cp)}&limit=1` });
      if (r && r[0]) return r[0];
    }catch{}
  }
  return null;
}


/* ========================================================================== */
/* ============================== SOPORTE TÉCNICO =========================== */
/* ========================================================================== */

/* ---------- Utilidades específicas de Soporte (sin colisiones) ----------- */

function SV_displayField(k){
  const map = {
    modelo:'marca y modelo',
    falla:'descripción breve de la falla',
    nombre:'Nombre o Razón Social',
    email:'email',
    calle:'calle', numero:'número', colonia:'colonia', ciudad:'ciudad', estado:'estado', cp:'código postal',
    horario:'día y hora (10:00–15:00)'
  };
  return map[k] || k;
}

function SV_quickHelp(ntext){
  const t = ntext || '';
  if (/\batasc(a|o)|se atora|se traba|arrugad/i.test(t)){
    return `Veamos rápido 🧰\n1) Apaga y enciende el equipo.\n2) Revisa bandejas y retira papel atorado.\n3) Abre y cierra el fusor con cuidado.\nSi sigue igual, agendamos visita para diagnóstico.`;
  }
  if (/\bno imprime\b/.test(t)){
    return `Probemos rápido 🧰\n1) Reinicia la impresora.\n2) Verifica tóner y que puertas estén cerradas.\n3) Intenta imprimir una página de prueba.\nSi persiste, agendamos visita.`;
  }
  if (/\bmancha|l[ií]ne?a|calidad\b/.test(t)){
    return `Sugerencia rápida 🎯\n1) Imprime un patrón de prueba.\n2) Revisa niveles y reinstala tóners.\n3) Limpia rodillos si es posible.\nSi no mejora, te agendo visita para revisión.`;
  }
  return null;
}

/* Marca + Modelo (robusto, sin colisiones) */
function SV_parseBrandModel(text=''){
  const t = normalizeWithAliases(text);
  let marca = null;
  if (/\bxerox\b/i.test(t)) marca = 'Xerox';
  else if (/\bfujifilm|fuji\s?film\b/i.test(t)) marca = 'Fujifilm';

  const norm = t.replace(/\s+/g,' ').trim();

  const mDocu = norm.match(/\bdocu\s*color\s*(550|560|570)\b/i) || norm.match(/\bdocucolor\s*(550|560|570)\b/i);
  if (mDocu) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${mDocu[1]}` };

  const mVers = norm.match(/\bversant\s*(80|180|2100|280|4100)\b/i);
  if (mVers) return { marca: marca || 'Xerox', modelo: `VERSANT ${mVers[1]}` };

  const mVL = norm.match(/\b(versalink|versa\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mVL) return { marca: marca || 'Xerox', modelo: `${mVL[1].replace(/\s/,'').toUpperCase()} ${mVL[2].toUpperCase()}` };

  const mAL = norm.match(/\b(altalink|alta\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mAL) return { marca: marca || 'Xerox', modelo: `${mAL[1].replace(/\s/,'').toUpperCase()} ${mAL[2].toUpperCase()}` };

  const mPL = norm.match(/\b(primelink|prime\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mPL) return { marca: marca || 'Xerox', modelo: `${mPL[1].replace(/\s/,'').toUpperCase()} ${mPL[2].toUpperCase()}` };

  const mApeos = norm.match(/\bapeos\s*([a-z0-9\-]+)?\b/i);
  if (mApeos) return { marca: marca || 'Fujifilm', modelo: `APEOS${mApeos[1] ? ' ' + mApeos[1].toUpperCase() : ''}`.trim() };

  const mSeries = norm.match(/\b([cb]\d{2,4})\b/i);
  if (mSeries) return { marca: marca || 'Xerox', modelo: mSeries[1].toUpperCase() };

  const m550 = norm.match(/\b(550|560|570)\b/);
  if (/\bdocu\s*color\b/i.test(norm) && m550) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${m550[1]}` };

  if (/\bdocu\s*color\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'DOCUCOLOR' };
  if (/\bversant\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'VERSANT' };

  return { marca, modelo: null };
}

function SV_extractSvInfo(text) {
  const t = normalizeWithAliases(text);
  const out = {};

  if (/xerox/i.test(t)) out.marca = 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(t)) out.marca = 'Fujifilm';

  const pm = SV_parseBrandModel(text);
  if (pm.marca) out.marca = out.marca || pm.marca;
  if (pm.modelo) out.modelo = pm.modelo;

  const err = t.match(/\berror\s*([0-9\-]+)\b/i);
  if (err) out.error_code = err[1];

  if (/no imprime/i.test(t)) out.falla = 'No imprime';
  if (/atasc(a|o)|se atora|se traba|arrugad(i|o)|saca el papel/i.test(t)) out.falla = 'Atasco/arrugado de papel';
  if (/mancha|calidad|linea|l[ií]nea/i.test(t)) out.falla = 'Calidad de impresión';
  if (/\b(parado|urgente|producci[oó]n detenida|parada)\b/i.test(t)) out.prioridad = 'alta';

  // Dirección suelta
  const mCol = text.match(/\bcol(?:\.|onia)?\s+([a-z0-9\-\sáéíóúñ]+)\b/i);
  if (mCol) out.colonia = clean(mCol[1]);
  const mCal1 = text.match(/\bcalle\s+([a-z0-9\-\sáéíóúñ]+)\b/i);
  if (mCal1) out.calle = clean(mCal1[1]);
  const mCal2 = text.match(/\ben\s+([a-z0-9\-\sáéíóúñ]+)\s+#?\d+\b/i);
  if (!out.calle && mCal2) out.calle = clean(mCal2[1]);
  const em = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (em) out.email = em[0].toLowerCase();
  const cp = text.match(/\b(\d{5})\b/);
  if (cp) out.cp = cp[1];
  const num = text.match(/\b(\d+[A-Z]?)\b/);
  if (num) out.numero = num[1];

  return out;
}

function SV_svFillFromAnswer(sv, field, text){
  const pm = SV_parseBrandModel(text);
  if (field === 'modelo') {
    if (pm.marca) sv.marca = pm.marca;
    if (pm.modelo) sv.modelo = pm.modelo;
    if (!sv.modelo) sv.modelo = clean(text);
    return;
  }
  if (field === 'falla') { sv.falla = clean(text); return; }
  if (field === 'nombre') { sv.nombre = clean(text); return; }
  if (field === 'email') { const em = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); sv.email = em ? em[0].toLowerCase() : clean(text).toLowerCase(); return; }
  if (field === 'calle') { sv.calle = clean(text); return; }
  if (field==='numero'){ const mnum = text.match(/\b(\d+[A-Z]?)\b/); sv.numero = mnum?mnum[1]:clean(text); return; }
  if (field==='colonia'){ sv.colonia = clean(text); return; }
  if (field==='ciudad'){ sv.ciudad = clean(text); return; }
  if (field==='estado'){ sv.estado = clean(text); return; }
  if (field==='cp'){ const mcp = text.match(/\b(\d{5})\b/); sv.cp = mcp?mcp[1]:clean(text); return; }
  if (field==='horario'){ return; }
}

/* --------------------------- Circuit breaker SV --------------------------- */

async function SV_cbCanUse(env, key){
  try{
    const raw = await env.wa_sessions.get(`cb:${key}`, 'json');
    if (!raw) return true;
    const until = Number(raw.until||0);
    return Date.now() >= until;
  }catch{ return true; }
}

async function SV_cbRegisterFail(env, key){
  try{
    const limit = Number(env.CB_FAIL_LIMIT || 3);
    const cooldown = Number(env.CB_COOLDOWN_SEC || 60) * 1000;
    const raw = await env.wa_sessions.get(`cb:${key}`, 'json') || { fails:0, until:0 };
    const fails = (raw.fails||0)+1;
    if (fails >= limit){
      await env.wa_sessions.put(`cb:${key}`, JSON.stringify({ fails:0, until: Date.now()+cooldown }), { expirationTtl: 3600 });
    }else{
      await env.wa_sessions.put(`cb:${key}`, JSON.stringify({ fails, until:0 }), { expirationTtl: 3600 });
    }
  }catch{}
}

async function SV_cbRegisterOk(env, key){
  try{ await env.wa_sessions.delete(`cb:${key}`); }catch{}
}

/* --------------------------- Retry util (backoff) ------------------------- */

async function SV_withRetry(fn, { tries=[500,1000,2000] }={}){
  let lastErr = null;
  for (let i=0;i<tries.length;i++){
    try{
      const r = await fn();
      return r;
    }catch(e){
      lastErr = e;
      if (i<tries.length-1) await new Promise(res=>setTimeout(res, tries[i]));
    }
  }
  throw lastErr || new Error('retry_failed');
}

/* --------------------------- GCal helpers (prefijo) ----------------------- */

async function SV_gcalToken(env) {
  if (!env.GCAL_REFRESH_TOKEN || !env.GCAL_CLIENT_ID || !env.GCAL_CLIENT_SECRET) return null;
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: env.GCAL_CLIENT_ID, client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN, grant_type: 'refresh_token'
    })
  });
  if (!r.ok) return null;
  const j = await r.json(); return j.access_token;
}

async function SV_gcalCreateEvent(env, calendarId, { summary, description, start, end, timezone }) {
  const token = await SV_gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  const body = { summary, description, start: { dateTime: start, timeZone: timezone }, end: { dateTime: end, timeZone: timezone } };
  const r = await SV_withRetry(()=>fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) }));
  if (!r.ok) return null;
  return await r.json();
}

async function SV_gcalPatchEvent(env, calendarId, eventId, patch, tokenForced=null) {
  const token = tokenForced || (await SV_gcalToken(env)); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await SV_withRetry(()=>fetch(url, { method: 'PATCH', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(patch) }));
  if (!r.ok) return null;
  return await r.json();
}

async function SV_gcalDeleteEvent(env, calendarId, eventId) {
  const token = await SV_gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  await SV_withRetry(()=>fetch(url, { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } })).catch(()=>{});
}

async function SV_isBusy(env, calendarId, startISO, endISO) {
  const token = await SV_gcalToken(env); if (!token) return false;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?timeMin=${encodeURIComponent(startISO)}&timeMax=${encodeURIComponent(endISO)}&singleEvents=true&orderBy=startTime`;
  const r = await SV_withRetry(()=>fetch(url, { headers: { Authorization: `Bearer ${token}` } }));
  if (!r.ok) return false;
  const j = await r.json();
  return Array.isArray(j.items) && j.items.length > 0;
}

async function SV_findNearestFreeSlot(env, calendarId, when, tz) {
  if (!calendarId) return when;
  let curStart = new Date(when.start);
  let curEnd = new Date(when.end);
  for (let i=0;i<6;i++) {
    const busy = await SV_isBusy(env, calendarId, curStart.toISOString(), curEnd.toISOString());
    if (!busy) break;
    curStart = new Date(curStart.getTime()+30*60*1000);
    curEnd = new Date(curStart.getTime()+60*60*1000);
    const h = curStart.getHours();
    if (h >= 15) {
      curStart.setDate(curStart.getDate()+1); curStart.setHours(10,0,0,0);
      curEnd = new Date(curStart.getTime()+60*60*1000);
    }
  }
  return { start: curStart.toISOString(), end: curEnd.toISOString() };
}

async function SV_getCalendarPool(env) {
  try{ return await sbGet(env, 'calendar_pool', { query: 'select=gcal_id,name,active&active=is.true' }) || []; }catch{ return []; }
}
function SV_pickCalendarFromPool(pool) { return pool?.[0] || null; }

function SV_renderOsDescription(phone, sv) {
  return [
    `Cliente: +${phone} (${sv.nombre || 'N/D'} / ${sv.email || 'sin email'})`,
    `Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
    `Falla: ${sv.falla || 'N/D'}${sv.error_code ? ' (Error ' + sv.error_code + ')' : ''}`,
    `Prioridad: ${sv.prioridad || 'media'}`,
    `Dirección: ${sv.calle || ''} ${sv.numero || ''}, ${sv.colonia || ''}, ${sv.ciudad || ''}, ${sv.estado || ''}, CP ${sv.cp || ''}`
  ].join('\n');
}

async function SV_getLastOpenOS(env, phone) {
  try {
    const c = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    const cid = c?.[0]?.id;
    if (!cid) return null;
    const r = await sbGet(env, 'orden_servicio', {
      query: `select=id,estado,ventana_inicio,ventana_fin,calendar_id,gcal_event_id,cliente_id&cliente_id=eq.${cid}&order=ventana_inicio.desc&limit=1`
    });
    if (r && r[0] && ['agendado','reprogramado','confirmado','pendiente'].includes(r[0].estado)) return r[0];
  } catch {}
  return null;
}

async function SV_upsertClienteByPhone(env, phone){
  try{
    const exist = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (exist && exist[0]) return exist[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.data?.[0]?.id || null;
  }catch(e){ return null; }
}

/* ---------------------- Fechas/ventana (re-uso seguro) -------------------- */

function SV_parseNaturalDateTime(text, env) {
  const tz = env.TZ || 'America/Mexico_City';
  const base = new Date(new Date().toLocaleString('en-US', { timeZone: tz }));
  let d = new Date(base);
  let targetDay = null;
  const t = normalizeBase(text || '');

  if (/\bhoy\b/i.test(t)) targetDay = 0;
  else if (/\bmañana\b/i.test(t) || /\bmanana\b/i.test(t)) targetDay = 1;
  else {
    const days = ['domingo','lunes','martes','miércoles','miercoles','jueves','viernes','sábado','sabado'];
    for (let i=0;i<days.length;i++) {
      if (new RegExp(`\\b${days[i]}\\b`, 'i').test(t)) {
        const today = base.getDay();
        const want = i%7;
        let delta = (want - today + 7) % 7;
        if (delta===0) delta = 7;
        targetDay = delta;
        break;
      }
    }
  }
  if (targetDay!==null) d.setDate(d.getDate()+targetDay);

  let hour = null, minute = 0;
  const m = t.match(/\b(\d{1,2})(?:[:\.](\d{2}))?\s*(am|pm)?\b/i);
  if (m) {
    hour = Number(m[1]); minute = m[2]?Number(m[2]):0; const ampm = (m[3]||'').toLowerCase();
    if (ampm==='pm' && hour<12) hour+=12;
    if (ampm==='am' && hour===12) hour=0;
  } else if (/\bmediod[ií]a\b/i.test(t)) { hour = 12; minute=0; }

  if (hour===null){
    const m2 = t.match(/\b(a\s+las\s+)?(\d{1,2})\b/i);
    if (m2){ hour = Number(m2[2]); }
    if (/\btarde\b/i.test(t) && hour && hour<12) hour += 12;
  }

  if (targetDay===null && hour===null) return null;
  if (hour===null) hour = 12;
  d.setHours(hour, minute, 0, 0);
  const start = d.toISOString();
  const end = new Date(d.getTime()+60*60*1000).toISOString();
  return { start, end };
}

function SV_clampToWindow(when, tz) {
  const start = new Date(when.start);
  const hours = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone:tz }).format(start));
  let newStart = new Date(start);
  if (hours < 10) newStart.setHours(10,0,0,0);
  if (hours >= 15) newStart.setHours(14,0,0,0);
  const newEnd = new Date(newStart.getTime()+60*60*1000);
  return { start: newStart.toISOString(), end: newEnd.toISOString() };
}

/* --------------------------- Acciones rápidas SV -------------------------- */

async function svCancel(env, session, toE164) {
  const os = await SV_getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para cancelar.`); return; }
  if (os.gcal_event_id && os.calendar_id) await SV_gcalDeleteEvent(env, os.calendar_id, os.gcal_event_id);
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'cancelada', cancel_reason: 'cliente' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *cancelado* tu visita. Si necesitas agendar otra, aquí estoy 🙂`);
}

async function svReschedule(env, session, toE164, when) {
  const os = await SV_getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para reprogramar.`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  const chosen = SV_clampToWindow(when, tz);
  const slot = await SV_findNearestFreeSlot(env, os.calendar_id, chosen, tz);
  if (os.gcal_event_id && os.calendar_id) {
    await SV_gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, { start: { dateTime: slot.start, timeZone: tz }, end: { dateTime: slot.end, timeZone: tz } });
  }
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'reprogramado', ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString() }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *reprogramado* tu visita a:\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}* ✅`);
}

async function svWhenIsMyVisit(env, session, toE164) {
  const os = await SV_getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita programada. ¿Agendamos una?`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164, `Tu próxima visita: *${fmtDate(os.ventana_inicio, tz)}*, de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*. Estado: ${os.estado}.`);
}

/* ---------------------- Flujo principal de Soporte SV --------------------- */

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    session.data.intent_lock = 'support';
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    // Si venía juntando datos, toma la última respuesta para ese campo
    const wasCollecting = session.stage === 'sv_collect';
    const prevNeeded = session.data.sv_need_next || null;
    if (wasCollecting && prevNeeded) {
      SV_svFillFromAnswer(sv, prevNeeded, text);
    }

    // Enriquecer con extracción libre
    Object.assign(sv, SV_extractSvInfo(text));

    // Intento de fecha/hora
    if (!sv.when) {
      const dt = SV_parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    // Bienvenida (si primera vez)
    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Lamento la falla 😕. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* del problema.`);
    }

    // FAQ express
    const quick = SV_quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick);
      await sendWhatsAppText(env, toE164, `¿Te propongo una *visita técnica* para revisarlo y dejarlo funcionando? (Sí / No)`);
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    // Prefill de cliente si existe
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;
    if (!sv.calle && truthy(c.calle)) { sv.calle=c.calle; sv.numero=c.numero; sv.colonia=c.colonia; sv.ciudad=c.ciudad; sv.estado=c.estado; sv.cp=c.cp; }

    // ¿Qué nos falta?
    const needed = [];
    if (!(truthy(sv.marca) && truthy(sv.modelo))) needed.push('modelo');
    if (!truthy(sv.falla)) needed.push('falla');
    if (!truthy(sv.calle)) needed.push('calle');
    if (!truthy(sv.numero)) needed.push('numero');
    if (!truthy(sv.colonia)) needed.push('colonia');
    if (!truthy(sv.cp)) needed.push('cp');
    if (!sv.when?.start) needed.push('horario');
    if (!truthy(sv.nombre)) needed.push('nombre');
    if (!truthy(sv.email)) needed.push('email');

    if (needed.length) {
      session.stage = 'sv_collect';
      session.data.sv_need_next = needed[0];
      await saveSessionMulti(env, session, session.from, toE164);

      const Q = {
        modelo: '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox DocuColor 550*)',
        falla: '¿Me describes brevemente la *falla*? (p.ej., “*atasco en fusor*”, “*no imprime*”)',
        calle: '¿En qué *calle* está el equipo?',
        numero: '¿Qué *número* es?',
        colonia: '¿*Colonia*?',
        cp: '¿*Código Postal* (5 dígitos)?',
        horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (ej: “*mañana 12:30*”)',
        nombre: '¿A nombre de quién registramos la visita?',
        email: '¿Cuál es tu *email* para enviarte confirmaciones?'
      };

      let pre = '';
      if (truthy(sv.marca) && truthy(sv.modelo) && needed[0] === 'falla') pre = `Anoté: *${sv.marca} ${sv.modelo}*.\n`;
      await sendWhatsAppText(env, toE164, pre + (Q[needed[0]] || `¿Me ayudas con ${SV_displayField(needed[0])}?`));
      return ok('EVENT_RECEIVED');
    }

    // Confirmación amable de la dirección/datos antes de agendar
    if (session.stage !== 'sv_confirm_dir' && session.stage !== 'sv_scheduled') {
      const resumen = [
        `• Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
        `• Falla: ${sv.falla || '—'}`,
        `• Dirección: ${[sv.calle, sv.numero, sv.colonia, sv.cp].filter(Boolean).join(' ')}`,
        sv.ciudad ? `• Ciudad: ${sv.ciudad}` : null,
        sv.estado ? `• Estado: ${sv.estado}` : null,
        `• Contacto: ${sv.nombre || '—'} / ${sv.email || '—'}`
      ].filter(Boolean).join('\n');

      await sendWhatsAppText(env, toE164, `Perfecto. Tengo esto:\n${resumen}\n\n¿*Confirmamos* la visita? Responde “ok”. Si deseas *editar* algún dato, escribe “campo valor” (ej: "colonia Centro").`);
      session.stage = 'sv_confirm_dir';
      session.data.sv_confirm_attempts = 0;
      await saveSessionMulti(env, session, session.from, toE164);
      return ok('EVENT_RECEIVED');
    }

    // Si estamos en confirmación, procesar edición/ok
    if (session.stage === 'sv_confirm_dir') {
      const t = normalizeBase(text);
      if (/^\s*ok\s*$/i.test(text) || /\bconfirm(o|a|ar)\b/i.test(t)) {
        // Pasar a agendar
      } else {
        const editMap = { nombre:'nombre', email:'email', calle:'calle', numero:'numero', colonia:'colonia', cp:'cp', ciudad:'ciudad', estado:'estado', modelo:'modelo', falla:'falla' };
        const m = t.match(/\b(nombre|email|calle|numero|colonia|cp|ciudad|estado|modelo|falla)\b\s+(.+)/i);
        if (m) {
          const field = editMap[m[1].toLowerCase()];
          const raw = text.replace(/^\s*(nombre|email|calle|numero|colonia|cp|ciudad|estado|modelo|falla)\b\s*/i, '');
          if (field === 'modelo' || field === 'falla') SV_svFillFromAnswer(sv, field, raw);
          else sv[field] = field==='cp' ? (raw.match(/\b(\d{5})\b/)?.[1] || clean(raw)) : clean(raw);
          await saveSessionMulti(env, session, session.from, toE164);
          await sendWhatsAppText(env, toE164, `Listo, actualicé *${SV_displayField(field)}* ✅\n¿Confirmamos? Escribe “ok” o corrige otro dato.`);
          return ok('EVENT_RECEIVED');
        }
        session.data.sv_confirm_attempts = Number(session.data.sv_confirm_attempts||0)+1;
        if (session.data.sv_confirm_attempts >= 2) {
          await sendWhatsAppText(env, toE164, `Si te parece, lo revisa un asesor y te confirma. 🙂`);
          await notifySupport(env, `Confirmación de dirección no resuelta con ${toE164}.`);
          session.stage = 'idle';
          await saveSessionMulti(env, session, session.from, toE164);
          return ok('EVENT_RECEIVED');
        }
        await sendWhatsAppText(env, toE164, `¿Confirmamos con “ok” o deseas editar algún dato? (ej: "calle Reforma")`);
        await saveSessionMulti(env, session, session.from, toE164);
        return ok('EVENT_RECEIVED');
      }
    }

    // ——— Agendar (con tolerancia a fallos / cola manual) ———
    const tz = env.TZ || 'America/Mexico_City';
    const chosen = SV_clampToWindow(sv.when, tz);

    const cliente_id = await SV_upsertClienteByPhone(env, session.from);
    try {
      await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email, calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp, telefono: session.from });
    } catch {}

    // Pool calendario
    let pool = [];
    try { pool = await SV_getCalendarPool(env) || []; } catch(e){}
    const cal = SV_pickCalendarFromPool(pool);

    let slot = chosen, event = null, calName = '';
    let usedCalendar = null;

    const canUseCal = await SV_cbCanUse(env, 'gcal');

    if (canUseCal && cal && env.GCAL_REFRESH_TOKEN && env.GCAL_CLIENT_ID && env.GCAL_CLIENT_SECRET) {
      try {
        slot = await SV_findNearestFreeSlot(env, cal.gcal_id, chosen, tz);
        event = await SV_gcalCreateEvent(env, cal.gcal_id, {
          summary: `Visita técnica: ${(sv.marca || '')} ${(sv.modelo || '')}`.trim(),
          description: SV_renderOsDescription(session.from, sv),
          start: slot.start, end: slot.end, timezone: tz,
        });
        calName = cal.name || '';
        usedCalendar = cal.gcal_id || null;
        await SV_cbRegisterOk(env, 'gcal');
      } catch (e) {
        await SV_cbRegisterFail(env, 'gcal');
      }
    }

    // Crear OS (si falla Supabase, aplicar breaker y fallback)
    let osId = null; 
    let estado = event ? 'agendado' : 'pendiente'; // si no hay calendar, pendiente
    const canUseSb = await SV_cbCanUse(env, 'supabase');
    try {
      if (!canUseSb) throw new Error('cb_supabase_open');
      const osBody = [{
        cliente_id, marca: sv.marca || null, modelo: sv.modelo || null, falla_descripcion: sv.falla || null,
        prioridad: sv.prioridad || 'media', estado,
        ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(),
        gcal_event_id: event?.id || null, calendar_id: usedCalendar,
        calle: sv.calle || null, numero: sv.numero || null, colonia: sv.colonia || null, ciudad: sv.ciudad || null, estado: sv.estado || null, cp: sv.cp || null,
        created_at: new Date().toISOString(), erp_sync_status: 'pending'
      }];
      const os = await SV_withRetry(()=>sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' }));
      osId = os?.data?.[0]?.id || null;
      await SV_cbRegisterOk(env, 'supabase');
    } catch (e) {
      await SV_cbRegisterFail(env, 'supabase');
      estado = 'pendiente_api';
      await notifySupport(env, `OS *pendiente_api* para ${toE164} (no se pudo escribir en Supabase). Revisar manualmente.\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla}`);
    }

    if (event) {
      await sendWhatsAppText(
        env, toE164,
        `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\nDirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}\nTécnico asignado: ${calName || 'por confirmar'}.\n\nSi necesitas reprogramar o cancelar, dímelo con confianza.`
      );
      session.stage = 'sv_scheduled';
    } else {
      await sendWhatsAppText(env, toE164, `Tengo tus datos ✍️. En breve te confirmo el horario exacto por este medio.`);
      await notifySupport(env, `OS *pendiente/agendar* para ${toE164}\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla}\nDirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad||''}\nNombre: ${sv.nombre} | Email: ${sv.email}`);
      session.stage = 'sv_scheduled';
    }

    session.data.sv.os_id = osId;
    session.data.sv.gcal_event_id = event?.id || null;

    // Liberamos candado para siguientes conversaciones
    session.data.intent_lock = null;

    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');

  } catch (e) {
    try{
      const need = session?.data?.sv_need_next || 'modelo';
      await sendWhatsAppText(env, toE164, `Gracias por la info. Para avanzar, ¿${SV_displayField(need)}?`);
    }catch{
      await sendWhatsAppText(env, toE164, `Tomé tu solicitud de soporte. Si te parece, seguimos con los datos para agendar o te contacto enseguida 🙌`);
    }
    return ok('EVENT_RECEIVED');
  }
}

/* ========================================================================== */
/* ===================== ADMIN / OBSERVABILIDAD / MÉTRICAS ================== */
/* ========================================================================== */
/* Este bloque agrega utilidades opcionales para trazar, medir y depurar.
   No interfiere con la lógica principal si no se usan.                    */

/* ------------------------------ Trazas DEBUG ------------------------------ */

function OBS_nowISO(){ try{ return new Date().toISOString(); }catch{ return String(Date.now()); } }
function OBS_randId(){ return Math.random().toString(36).slice(2,10); }

/** Genera o reutiliza un trace_id. Puedes guardarlo en session.data.trace_id */
function OBS_startTrace(prev=null){ return prev || `tr_${OBS_randId()}_${Date.now()}`; }

/** Log humano legible (sólo si DEBUG=true) */
function OBS_log(env, trace_id, stage, message, extra={}){
  try{
    if ((env.DEBUG||'').toString().toLowerCase()!=='true') return;
    const pfx = trace_id ? `[${trace_id}]` : '';
    // eslint-disable-next-line no-console
    console.log(`${pfx}[${stage}] ${message}`, extra && Object.keys(extra).length ? extra : '');
  }catch{}
}

/** Log estructurado (sólo si DEBUG_JSON=true) */
function OBS_logJSON(env, payload) {
  try{
    if ((env.DEBUG_JSON||'').toString().toLowerCase()!=='true') return;
    const row = {
      ts: OBS_nowISO(),
      ...payload
    };
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(row));
  }catch{}
}

/* ----------------------------- Métricas simples --------------------------- */
/* Guardadas en KV (wa_sessions) con prefijo "met:".
   - Contadores: MET_inc(env, 'sv_agendado')
   - Timing (promedio): MET_timing(env, 'sv_lead_time_ms', deltaMs)
*/

async function MET_inc(env, key, by=1){
  try{
    const k = `met:cnt:${key}`;
    const cur = Number(await env.wa_sessions.get(k) || '0');
    const val = cur + Number(by||1);
    await env.wa_sessions.put(k, String(val), { expirationTtl: 60*60*24*30 });
  }catch{}
}

async function MET_getCount(env, key){
  try{ return Number(await env.wa_sessions.get(`met:cnt:${key}`)||'0'); }catch{ return 0; }
}

/** Acumula muestras para calcular promedio más tarde */
async function MET_timing(env, key, ms){
  try{
    const base = `met:t:${key}`;
    const total = Number(await env.wa_sessions.get(`${base}:sum`)||'0') + Number(ms||0);
    const count = Number(await env.wa_sessions.get(`${base}:n`)||'0') + 1;
    await env.wa_sessions.put(`${base}:sum`, String(total), { expirationTtl: 60*60*24*30 });
    await env.wa_sessions.put(`${base}:n`, String(count), { expirationTtl: 60*60*24*30 });
  }catch{}
}

async function MET_getTimingAvg(env, key){
  try{
    const base = `met:t:${key}`;
    const total = Number(await env.wa_sessions.get(`${base}:sum`)||'0');
    const count = Number(await env.wa_sessions.get(`${base}:n`)||'0');
    if (count<=0) return 0;
    return total / count;
  }catch{ return 0; }
}

/* ---------------------------- Cron (placeholders) ------------------------- */
/* Si quieres ampliar recordatorios o agregados de métricas por cron,
   aquí tienes helpers seguros (no chocan con cronReminders() si ya existe). */

async function OBS_cronMetricsSnapshot(env){
  try{
    const ag = await MET_getCount(env, 'sv_agendado');
    const re = await MET_getCount(env, 'sv_reprogramado');
    const ca = await MET_getCount(env, 'sv_cancelado');
    const lead = await MET_getTimingAvg(env, 'sv_lead_time_ms');
    OBS_log(env, 'cron', 'metrics', 'snapshot', { agendado:ag, reprogramado:re, cancelado:ca, lead_avg_ms:Math.round(lead) });
    OBS_logJSON(env, { type:'metrics_snapshot', agendado:ag, reprogramado:re, cancelado:ca, lead_avg_ms:Math.round(lead) });
    return { ok:true, ag, re, ca, lead_avg_ms:Math.round(lead) };
  }catch(e){
    OBS_log(env, 'cron', 'metrics', 'snapshot_error', { error:String(e) });
    return { ok:false, error:String(e) };
  }
}

/* ------------------------- Notas de instrumentación ----------------------- */
/* Dónde invocar (si quieres métricas enriquecidas sin alterar lógica):
   - Al confirmar visita (event creado o estado 'pendiente'):
       await MET_inc(env, 'sv_agendado');
       // si guardas el primer timestamp por teléfono:
       const firstTs = Number(await env.wa_sessions.get(`ut:first:${session.from}`) || Date.now());
       await MET_timing(env, 'sv_lead_time_ms', Date.now() - firstTs);

   - Al reprogramar:
       await MET_inc(env, 'sv_reprogramado');

   - Al cancelar:
       await MET_inc(env, 'sv_cancelado');

   - Trazas útiles:
       OBS_log(env, trace_id, session.stage, 'mensaje', { intent_lock: session.data?.intent_lock });
       OBS_logJSON(env, { trace_id, stage: session.stage, lock: session.data?.intent_lock, sv: session.data?.sv });

   Estos hooks son opcionales. Si no los agregas, el bot funciona igual. */
/* ========================================================================== */
/* ============================== FIN BLOQUE 7/7 ============================ */
/* ========================================================================== */
