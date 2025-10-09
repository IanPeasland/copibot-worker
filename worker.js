/**
 * CopiBot – Conversacional con IA + Ventas + Soporte Técnico + GCal + Supabase
 * Build: “Borbón-R8.1” – Adaptado a wa_session(from,stage,data,updated_at,expires_at)
 * Cambios clave:
 *  - Persistencia compatible con esquema actual (from/stage/data/updated_at).
 *  - Anti re-orden por timestamp (data.last_ts) + idempotencia por mid.
 *  - Saludo no degrada etapas críticas; fast-path de cantidad también en await_choice.
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // ===== Verificación webhook de WhatsApp (GET /) =====
    if (req.method === 'GET' && url.pathname === '/') {
      const mode = url.searchParams.get('hub.mode');
      const token = url.searchParams.get('hub.verify_token');
      const challenge = url.searchParams.get('hub.challenge');
      if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
        return new Response(challenge, { status: 200 });
      }
      return new Response('Forbidden', { status: 403 });
    }

    // ===== Healthcheck rápido =====
    if (req.method === 'GET' && url.pathname === '/health') {
      const have = {
        WA_TOKEN: !!env.WA_TOKEN,
        PHONE_ID: !!env.PHONE_ID,
        VERIFY_TOKEN: !!env.VERIFY_TOKEN,
        SUPABASE_URL: !!env.SUPABASE_URL,
        SUPABASE_ANON_KEY: !!env.SUPABASE_ANON_KEY,
        GCAL_REFRESH_TOKEN: !!env.GCAL_REFRESH_TOKEN,
        TZ: env.TZ || 'America/Mexico_City'
      };
      return new Response(JSON.stringify({ ok: true, have, now: new Date().toISOString() }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // ===== Cron manual =====
    if (req.method === 'POST' && url.pathname === '/cron') {
      const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
      if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
      const out = await cronReminders(env);
      return new Response(`cron ok ${JSON.stringify(out)}`, { status: 200 });
    }

    // ===== Webhook principal de WhatsApp =====
    if (req.method === 'POST' && url.pathname === '/') {
      try {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw, msgType, ts } = ctx;
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text);

        // Sesión desde Supabase
        const now = new Date();
        let session = await loadSession(env, from); // <-- lee from/stage/data/updated_at
        session.data  = session.data  || {};
        session.stage = session.stage || 'idle';
        session.from  = from;

        // Autocompletar nombre si lo tenemos
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // ==== Anti re-orden por timestamp (tolerante a desfaces) ====
        const msgTs = Number(ts || Date.now());     // ms
        let lastTs = Number(session?.data?.last_ts || 0);
        const nowMs = Date.now();

        // Si last_ts quedó "en el futuro" (>10 min), lo ignoramos
        if (lastTs > nowMs + 10 * 60 * 1000) {
        lastTs = 0;
        }

        // Solo descartamos si el mensaje es >5s MÁS viejo que el último
        if (lastTs && (msgTs + 5000) <= lastTs) {
        return ok('EVENT_RECEIVED');
        }

        // Guardamos el mayor para no retroceder
        session.data.last_ts = Math.max(msgTs, lastTs);

        // Idempotencia por MID
        if (session?.data?.last_mid && session.data.last_mid === mid) {
          return ok('EVENT_RECEIVED');
        }
        session.data.last_mid = mid;

        // No-texto → pedir texto
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, '¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂');
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ======= FAST-PATH: cantidad aun si se perdió la etapa =======
        if (session?.data?.last_candidate && looksLikeQuantity(lowered)) {
          return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        }

        // Intenciones (para casos normales)
        const supportIntent = isSupportIntent(ntext) || (await intentIs(env, text, 'support'));
        const salesIntent   = RX_INV_Q.test(ntext)   || (await intentIs(env, text, 'sales'));
        const isGreet       = RX_GREET.test(lowered);

        // ===== Etapas de ventas activas =====
        if (session.stage === 'ask_qty')       return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open')     return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage && session.stage.startsWith('collect_')) {
          return await handleCollectSequential(env, session, fromE164, text, now);
        }

        // ===== Comandos universales de soporte =====
        if (/\b(cancel(a|ar).*(cita|visita|servicio))\b/i.test(lowered)) {
          await svCancel(env, session, fromE164);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }
        if (/\b(reprogram|mueve|cambia|modif)\w*/i.test(lowered)) {
          const when = parseNaturalDateTime(lowered, env);
          if (when?.start) {
            await svReschedule(env, session, fromE164, when);
            await saveSession(env, session, now);
            return ok('EVENT_RECEIVED');
          }
        }
        if (/\b(cu[aá]ndo|cuando).*(cita|visita|servicio)\b/i.test(lowered)) {
          await svWhenIsMyVisit(env, session, fromE164);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Saludo genuino (NO degrada etapas críticas) =====
        const CRITICAL_STAGES = ['ask_qty','cart_open','await_invoice','sv_collect'];
        if (isGreet) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          session.data.last_greet_at = now.toISOString();

          if (session.stage && session.stage !== 'idle' && !CRITICAL_STAGES.includes(session.stage)) {
            session.data.last_stage = session.stage;
            session.stage = 'await_choice';
          }
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Menú "continuar o nuevo" =====
        if (session.stage === 'await_choice') {
          // Fast-path: si hay last_candidate y parece cantidad → tratar como ask_qty
          if (session?.data?.last_candidate && looksLikeQuantity(lowered)) {
            session.stage = 'ask_qty';
            await saveSession(env, session, now);
            return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
          }

          if (supportIntent) {
            session.stage = 'sv_collect';
            await saveSession(env, session, now);
            return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support', forceWelcome: true });
          }
          if (salesIntent) {
            session.data.last_stage = 'idle';
            session.stage = 'idle';
            await saveSession(env, session, now);
            return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
          }
          if (isContinueish(lowered)) {
            session.stage = session?.data?.last_stage || 'idle';
            await saveSession(env, session, now);
            const prompt = buildResumePrompt(session);
            await sendWhatsAppText(env, fromE164, `Va. ${prompt}`);
            return ok('EVENT_RECEIVED');
          }
          if (isStartNewish(lowered)) {
            session.data.last_stage = 'idle';
            session.stage = 'idle';
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, 'Perfecto, cuéntame qué necesitas (soporte, cotización, etc.). 🙂');
            return ok('EVENT_RECEIVED');
          }
          await sendWhatsAppText(env, fromE164, '¿Prefieres continuar con lo pendiente o empezamos algo nuevo?');
          return ok('EVENT_RECEIVED');
        }

        // ===== Cambio a soporte si aplica =====
        if (supportIntent || session.stage?.startsWith('sv_')) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
        }

        // ===== Ventas por intención =====
        if (salesIntent) {
          if (session.stage !== 'idle') {
            await sendWhatsAppText(env, fromE164, 'Te ayudo con inventario. Dejo lo otro en pausa un momento.');
            session.data.last_stage = session.stage;
            session.stage = 'idle';
            await saveSession(env, session, now);
          }
          return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
        }

        // ===== FAQs rápidas =====
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Fallback IA breve =====
        const reply = await aiSmallTalk(env, session, 'fallback', text);
        await sendWhatsAppText(env, fromE164, reply);
        await saveSession(env, session, now);
        return ok('EVENT_RECEIVED');

      } catch (e) {
        console.error('Worker error', e);
        try {
          const body = await safeJson(req).catch(() => ({}));
          const ctx2 = extractWhatsAppContext(body);
          if (ctx2?.fromE164) {
            await sendWhatsAppText(env, ctx2.fromE164, 'Tu mensaje llegó, tuve un problema momentáneo pero ya estoy encima 🙂');
          }
        } catch {}
        return ok('EVENT_RECEIVED');
      }
    }

    // Rutas no contempladas
    return new Response('Not found', { status: 404 });
  },

  // ===== CRON opcional =====
  async scheduled(event, env, ctx) {
    try {
      const out = await cronReminders(env);
      console.log('cron run', out);
    } catch (e) {
      console.error('cron error', e);
    }
  }
};

/* ============================ HTTP utils & cron ============================ */
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
function ok(s='ok'){ return new Response(s, { status: 200 }); }
async function cronReminders(env){ return { ok:true, ts: Date.now() }; }

/* ========================================================================== */
/* ==================== Intents, Regex, Normalización ======================= */
/* ========================================================================== */

const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_Q = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro|yellow|black|bk|k)\b/i;

function isContinueish(t){ return /\b(continuar|continuemos|seguir|retomar|reanudar|continuo|contin[uú]o)\b/i.test(t); }
function isStartNewish(t){ return /\b(empezar|nuevo|desde cero|otra cosa|otro|iniciar|empecemos)\b/i.test(t); }

function isSupportIntent(ntext='') {
  const t = `${ntext}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|marca c[oó]digo|c[oó]digo)/.test(t);
  const hasDevice  = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase     = /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);
  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_NEG_NO = /\b(no|nel|ahorita no)\b/i;
const RX_DONE   = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;
const RX_YES    = /\b(s[ií]|sí|si|claro|va|dale|sale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo|de acuerdo|me sirve)\b/i;

function isYesish(t){ return RX_YES.test(t); }
function isNoish(t){ return RX_NEG_NO.test(t) || RX_DONE.test(t); }

/* ============================ Normalización & Utils ======================= */
const firstWord   = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function priceWithIVA(n){ return `${formatMoneyMXN(Number(n||0))} + IVA`; }

function normalizeWithAliases(s=''){
  const t = normalizeBase(s);
  const aliases = [
    ['verzan','versant'], ['verzand','versant'], ['versan','versant'], ['vrsant','versant'],
    ['dococolor','docucolor'], ['docucolour','docucolor'], ['docu color','docucolor'],
    ['versa link','versalink'], ['altaling','altalink'], ['alta link','altalink'],
    ['prime link','primelink'], ['prime-link','primelink'], ['prymelink','primelink'],
    ['fuji film','fujifilm'], ['docucolor 5560','docucolor 550/560/570']
  ];
  let out = t;
  for (const [bad, good] of aliases) out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  return out;
}

/* ========================================================================== */
/* ========================= WhatsApp send & notify ========================= */
/* ========================================================================== */

async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) { console.warn('WA env missing'); return; }
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = { messaging_product: 'whatsapp', to: toE164.replace(/\D/g, ''), text: { body } };
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${env.WA_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!r.ok) console.warn('sendWhatsAppText', r.status, await r.text());
}

async function notifySupport(env, body) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  await sendWhatsAppText(env, to, `🛎️ *Soporte*\n${body}`);
}

/* ========================================================================== */
/* =============================== IA Helpers =============================== */
/* ========================================================================== */

async function aiCall(env, messages, {json=false}={}) {
  const OPENAI_KEY = env.OPENAI_API_KEY || env.OPENAI_KEY;
  const MODEL = env.LLM_MODEL || env.OPENAI_NLU_MODEL || env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  if (!OPENAI_KEY) return null;
  const body = { model: MODEL, messages, temperature: json ? 0 : 0.3, ...(json ? { response_format: { type: "json_object" } } : {}) };
  const r = await fetch('https://api.openai.com/v1/chat/completions', {
    method:'POST',
    headers:{ 'Authorization':`Bearer ${OPENAI_KEY}`, 'Content-Type':'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) { console.warn('aiCall', r.status, await r.text()); return null; }
  const j = await r.json();
  return j?.choices?.[0]?.message?.content || '';
}

async function aiSmallTalk(env, session, mode='general', userText=''){
  const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde con calidez humana, breve y claro. Máx. 1 emoji. Evita listas salvo necesidad.`;
  const prompt = mode==='fallback'
    ? `El usuario dijo: """${userText}""". Responde breve, útil y amable. Si no hay contexto, ofrece inventario o soporte.`
    : `El usuario dijo: """${userText}""". Responde breve y amable.`;
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 👋 ¿En qué te puedo ayudar?`);
}

/** Clasificador IA (opcional) */
async function intentIs(env, text, expected){
  try{
    const out = await aiClassifyIntent(env, text);
    return out?.intent === expected;
  }catch{ return false; }
}

async function aiClassifyIntent(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Clasifica texto (es-MX) en JSON: { "intent": "support|sales|faq|smalltalk" }`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try { return JSON.parse(out||'{}'); } catch { return null; }
}

/** IA opcional para reforzar NER de inventario */
async function aiExtractTonerQuery(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Extrae de una consulta (es-MX) sobre tóners los campos { "familia": "versant|docucolor|primelink|versalink|altalink|apeos|c70|", "color": "yellow|magenta|cyan|black|null", "subfamilia": "string|null", "cantidad": "number|null" } en JSON. No inventes.`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try { return JSON.parse(out||'{}'); } catch { return null; }
}

/* ========================================================================== */
/* ============================ Sesión (Supabase) =========================== */
/* ========================================================================== */

/** Lee sesión del esquema actual: wa_session(from, stage, data, updated_at, expires_at) */
async function loadSession(env, phone) {
  try {
    const r = await sbGet(env, 'wa_session', {
      query:
        `select=from,stage,data,updated_at,expires_at&` +
        `from=eq.${encodeURIComponent(phone)}&` +
        `order=updated_at.desc&limit=1`
    });

    if (Array.isArray(r) && r[0]) {
      return {
        from: r[0].from,
        stage: r[0].stage || 'idle',
        data:  r[0].data  || {}
      };
    }

    return { from: phone, stage: 'idle', data: {} };
  } catch (e) {
    console.warn('loadSession error', e);
    return { from: phone, stage: 'idle', data: {} };
  }
}

/** Guarda sesión al esquema actual (upsert por from) */
async function saveSession(env, session, now = new Date()) {
  try {
    await sbUpsert(env, 'wa_session', [{
      from: session.from,
      stage: session.stage || 'idle',
      data:  session.data  || {},
      updated_at: now.toISOString()
      // expires_at opcional si manejas TTL
    }], {
      onConflict: 'from',
      returning: 'minimal'
    });
  } catch (e) {
    console.warn('saveSession error', e);
  }
}

/* ========================================================================== */
/* ============================ Cantidades robustas ========================= */
/* ========================================================================== */

const NUM_WORDS = {
  'cero':0, 'una':1, 'uno':1, 'un':1, 'dos':2, 'tres':3, 'cuatro':4, 'cinco':5, 'seis':6, 'siete':7, 'ocho':8, 'nueve':9, 'diez':10,
  'once':11, 'doce':12, 'docena':12, 'media':0.5, 'media docena':6
};

function looksLikeQuantityStrict(t=''){
  const hasDigit = /\b\d+\b/.test(t);
  const hasWord  = Object.keys(NUM_WORDS).some(w => new RegExp(`\\b${w}\\b`, 'i').test(t));
  return hasDigit || hasWord;
}

const RX_WANT_QTY = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+(\d+)\b/i;

function parseQty(text, fallback = 1) {
  const t = normalizeBase(text);

  const mCmd = t.match(RX_WANT_QTY);
  if (mCmd) {
    const q = Number(mCmd[2]);
    if (q > 0) return q;
  }

  if (/\bmedia\s+docena\b/i.test(t)) return 6;
  if (/\bdocena\b/i.test(t)) return 12;

  for (const [w, n] of Object.entries(NUM_WORDS)) {
    if (new RegExp(`\\b${w}\\b`, 'i').test(t)) {
      const q = Number(n);
      if (q > 0) return q;
    }
  }

  const m = t.match(/\b(\d+)\b/);
  if (m) {
    const q = Number(m[1]);
    if (q > 0) return q;
  }

  return fallback;
}

/* ========================================================================== */
/* ========================= Inventario & Carrito =========================== */
/* ========================================================================== */

const RX_ADD_ITEM = /\b(agrega(?:me)?|añade|mete|pon|suma|incluye)\b/i;

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

function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

/* ---- Etapa: pedir cantidad para el último candidato (con verificación estricta) ---- */
async function handleAskQty(env, session, toE164, text, lowered, ntext, now){
  const cand = session.data?.last_candidate;
  if (!cand) {
    // No hay candidato guardado; pasa a cart_open pero pide repetir el artículo
    session.stage = 'cart_open';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, 'No alcancé a ver el artículo. ¿Lo repetimos o buscas otro? 🙂');
    return ok('EVENT_RECEIVED');
  }

  // 1) Cierre desde ask_qty: respétalo SOLO si ya hay algo en carrito
  if (RX_DONE.test(lowered)) {
    const cart = session.data?.cart || [];
    if (cart.length > 0) {
      session.stage = 'await_invoice';
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `Perfecto 🙌 ¿La cotizamos *con factura* o *sin factura*?`);
      return ok('EVENT_RECEIVED');
    }
    // Si no hay nada en el carrito, no cierro; pido la cantidad explícita
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(env, toE164, `Aún no he agregado piezas. ¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  // 2) Saludos u otros textos que NO sean cantidad → NO agregar nada
  if (!looksLikeQuantityStrict(lowered)) {
    const s = numberOrZero(cand.stock);
    // Si fue saludo, respondemos amable y repreguntamos
    if (RX_GREET.test(lowered)) {
      await sendWhatsAppText(env, toE164, `¡Hola! 🙂`);
    }
    await sendWhatsAppText(env, toE164, `Para avanzar, dime *cuántas piezas* necesitas. (hay ${s} en stock; el resto sería *sobre pedido*)`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  // 3) Ahora sí, texto parece cantidad → parsear y agregar
  const qty = parseQty(lowered, 1);
  if (!Number.isFinite(qty) || qty <= 0) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(env, toE164, `Necesito un número de piezas (hay ${s} en stock).`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  addWithStockSplit(session, cand, qty);

  // 4) Pasamos a carrito abierto
  session.stage = 'cart_open';
  await saveSession(env, session, now);

  const s = numberOrZero(cand.stock);
  const bo = Math.max(0, qty - Math.min(s, qty));
  const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} sobre pedido)` : '';
  await sendWhatsAppText(env, toE164,
    `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${nota}\n\n` +
    `¿Deseas *agregar algo más* o *finalizamos*?`
  );
  return ok('EVENT_RECEIVED');
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext, now) {
  session.data = session.data || {};
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate) {
      addWithStockSplit(session, session.data.last_candidate, 1);
    }
    session.stage = 'await_invoice';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Perfecto 🙌 ¿La cotizamos *con factura* o *sin factura*?`);
    return ok('EVENT_RECEIVED');
  }

  const RX_YES_CONFIRM = /\b(s[ií]|sí|si|claro|va|dale|correcto|ok|afirmativo|hazlo|agr[eé]ga(lo)?|añade|m[eé]te|pon(lo)?)\b/i;
  if (RX_YES_CONFIRM.test(lowered)) {
    const c = session.data?.last_candidate;
    if (c) {
      session.stage = 'ask_qty';
      await saveSession(env, session, now);
      const s = numberOrZero(c.stock);
      await sendWhatsAppText(env, toE164, `De acuerdo. ¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto iría *sobre pedido*)`);
      return ok('EVENT_RECEIVED');
    }
  }

  if (looksLikeQuantity(lowered)) {
    const c = session.data?.last_candidate;
    if (c) {
      session.stage = 'ask_qty';
      await saveSession(env, session, now);
      const s = numberOrZero(c.stock);
      await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto iría *sobre pedido*)`);
      return ok('EVENT_RECEIVED');
    }
  }

  if (RX_ADD_ITEM.test(lowered) || RX_INV_Q.test(ntext)) {
    const cleanQ = lowered.replace(RX_ADD_ITEM, '').trim() || ntext;

    const extracted = await aiExtractTonerQuery(env, cleanQ).catch(()=>null);
    const enrichedQ = enrichQueryFromAI(cleanQ, extracted);

    const best = await findBestProduct(env, enrichedQ);
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSession(env, session, now);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164,
        `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
      );
      return ok('EVENT_RECEIVED');
    } else {
      const hints = extractModelHints(enrichedQ);
      if (hints.family) {
        session.data.last_candidate = {
          id: null, sku: null,
          nombre: `Tóner ${hints.family.toUpperCase()}${hints.color?` ${hints.color}`:''} (sobre pedido)`,
          marca: 'Xerox',
          precio: 0,
          stock: 0
        };
        session.stage = 'ask_qty';
        await saveSession(env, session, now);
        await sendWhatsAppText(env, toE164, `Ese modelo se maneja *sobre pedido*. Si deseas, indícame *cuántas piezas* y lo agrego al carrito; también puedo buscar *compatibles* si lo prefieres.`);
        return ok('EVENT_RECEIVED');
      }
      await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. ¿Busco otra opción o lo revisa un asesor?`);
      await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }
  }

  await sendWhatsAppText(env, toE164, `Te leo 🙂. Puedo agregar el artículo mostrado, buscar otro o *finalizar* si ya está completo.`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
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

async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  const extracted = await aiExtractTonerQuery(env, ntext).catch(()=>null);
  const enrichedQ = enrichQueryFromAI(ntext, extracted);
  const best = await findBestProduct(env, enrichedQ);
  const hints = extractModelHints(enrichedQ);

  if (best) {
    session.stage = 'ask_qty';
    session.data.cart = session.data.cart || [];
    session.data.last_candidate = best;
    await saveSession(env, session, now);
    const s = numberOrZero(best.stock);
    await sendWhatsAppText(env, toE164,
      `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
    );
    return ok('EVENT_RECEIVED');
  }

  if (!best && hints.family) {
    session.data.last_candidate = {
      id: null, sku: null,
      nombre: `Tóner ${hints.family.toUpperCase()}${hints.color?` ${hints.color}`:''} (sobre pedido)`,
      marca: 'Xerox', precio: 0, stock: 0
    };
    session.stage = 'ask_qty';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Ese modelo se maneja *sobre pedido*. Dime *cuántas piezas* y lo agrego; si deseas, también busco *compatibles*.`);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. Te conecto con un asesor…`);
  await notifySupport(env, `Inventario sin match. +${session.from}: ${text}`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* ============================ Matching de productos ======================= */

function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};

  if (/\bversant\b/i.test(t) || /\b(80|180|2100|280|4100)\b/i.test(t)) out.family = 'versant';
  else if (/\bdocu\s*color\b/i.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/i.test(t) || /\bprimelink\b/i.test(t)) out.family = 'primelink';
  else if (/\bversa\s*link\b/i.test(t) || /\bversalink\b/i.test(t)) out.family = 'versalink';
  else if (/\balta\s*link\b/i.test(t) || /\baltalink\b/i.test(t)) out.family = 'altalink';
  else if (/\bapeos\b/i.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/i.test(t)) out.family = 'c70';

  const c = extractColorWord(t);
  if (c) out.color = c;

  return out;
}

function extractColorWord(text=''){
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return 'yellow';
  if (/\bmagenta\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'black';
  return null;
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
    const bad = /(docu\s*color|primelink|alta\s*link|versa\s*link|\bc(60|70|75)\b|\b550\b|\b560\b|\b570\b)/i.test(s);
    return hit && !bad;
  }
  if (family==='docucolor'){
    const hit = /\b(docu\s*color|550\/560\/570|550|560|570)\b/i.test(s);
    const bad = /(versant|primelink|alta\s*link|versa\s*link|\b2100\b|\b180\b|\b280\b|\b4100\b)/i.test(s);
    return hit && !bad;
  }
  if (family==='c70') return /\bc(60|70|75)\b/i.test(s) || s.includes('c60') || s.includes('c70') || s.includes('c75');
  if (family==='primelink') return /\bprime\s*link\b/i.test(s) || /\bprimelink\b/i.test(s);
  if (family==='versalink') return /\bversa\s*link\b/i.test(s) || /\bversalink\b/i.test(s);
  if (family==='altalink')  return /\balta\s*link\b/i.test(s) || /\baltalink\b/i.test(s);
  if (family==='apeos')     return /\bapeos\b/i.test(s);
  return s.includes(family);
}

async function findBestProduct(env, queryText, opts = {}) {
  const hints = extractModelHints(queryText);
  const colorCode = hints.color || extractColorWord(queryText);
  const debug = (env.DEBUG === 'true');

  const scoreAndPick = (arr=[]) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = arr.slice();

    if (hints.family && !opts.ignoreFamily) pool = pool.filter(p => productMatchesFamily(p, hints.family));
    if (colorCode) pool = pool.filter(p => productHasColor(p, colorCode));
    if (!pool.length) return null;

    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });
    return pool[0] || null;
  };

  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 40 }) || [];
    const pick1 = scoreAndPick(res);
    if (debug) console.log('[INV] RPC matches:', res?.length || 0, 'pick:', pick1?.sku);
    if (pick1) return pick1;
  } catch (e) {
    if (debug) console.log('[INV] RPC error', e);
  }

  if (hints.family && !opts.ignoreFamily) {
    try {
      const likeFam = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&` +
               `or=(nombre.ilike.${likeFam},sku.ilike.${likeFam},marca.ilike.${likeFam},compatible.ilike.${likeFam})&` +
               `order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const pick2 = scoreAndPick(r);
      if (debug) console.log('[INV] Family scan:', hints.family, 'cands:', r?.length || 0, 'pick:', pick2?.sku);
      if (pick2) return pick2;
    } catch (e) {
      if (debug) console.log('[INV] family scan error', e);
    }
  }

  try {
    const likeToner = encodeURIComponent(`%toner%`);
    const r2 = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&` +
             `or=(nombre.ilike.${likeToner},sku.ilike.${likeToner})&` +
             `order=stock.desc.nullslast,precio.asc&limit=400`
    }) || [];
    const pick3 = scoreAndPick(r2);
    if (debug) console.log('[INV] Broad scan (toner) cands:', r2?.length || 0, 'pick:', pick3?.sku);
    if (pick3) return pick3;
  } catch (e) {
    if (debug) console.log('[INV] broad scan error', e);
  }

  return null;
}

/* ========================================================================== */
/* ============================== FAQs rápidas ============================== */
/* ========================================================================== */

async function maybeFAQ(env, ntext) {
  const faqs = {
    'horario': 'Horario de atención: Lunes a Viernes 9:00-18:00, Sábados 9:00-14:00',
    'ubicacion': 'Estamos en Av. Tecnológico #123, Industrial, Monterrey, NL',
    'contacto': 'Tel: 81 1234 5678 | Email: ventas@cpdigital.com.mx',
    'empresa': 'CP Digital - Especialistas en equipos de impresión Xerox y Fujifilm'
  };
  const patterns = {
    'horario': /\b(horario|hora|atencion|abierto|cierra|abre)\b/i,
    'ubicacion': /\b(donde|ubicacion|direccion|sucursal|local|tienda)\b/i,
    'contacto': /\b(contacto|telefono|tel|email|correo|whatsapp)\b/i,
    'empresa': /\b(empresa|quienes|somos|compania|negocio)\b/i
  };
  for (const [key, pattern] of Object.entries(patterns)) {
    if (pattern.test(ntext)) return faqs[key];
  }
  return null;
}

/* ========================================================================== */
/* =============================== Cierre venta ============================= */
/* ========================================================================== */

async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Perfecto, quedo al pendiente. Si necesitas algo más, aquí estoy 🙂`);
    return ok('EVENT_RECEIVED');
  }

  const saysNo  = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};

  if (!saysYes && !saysNo && /hola|cómo estás|como estas|gracias/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', originalText);
    await sendWhatsAppText(env, toE164, friendly);
    if (!promptedRecently(session, 'invoice', 3*60*1000)) {
      await sendWhatsAppText(env, toE164, `Por cierto, ¿la quieres *con factura* o *sin factura*?`);
    }
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  if (saysYes || saysNo) {
    session.data.requires_invoice = !!saysYes;
    await preloadCustomerIfAny(env, session);
    const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
    const need = firstMissing(list, session.data.customer);
    if (need) {
      session.stage = `collect_${need}`;
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `¿${LABEL[need]}?`);
      return ok('EVENT_RECEIVED');
    }
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
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más en este momento? (Sí / No)`);
    return ok('EVENT_RECEIVED');
  }

  if (!promptedRecently(session, 'invoice', 2*60*1000)) {
    await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  }
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL     = { nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', cp:'Código Postal' };

function firstMissing(list, c={}){ for (const k of list){ if (!truthy(c[k])) return k; } return null; }

function parseCustomerFragment(field, text){
  const t = text;
  if (field==='nombre') return clean(t);
  if (field==='rfc'){ const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i); return m ? m[1].toUpperCase() : clean(t).toUpperCase(); }
   if (field==='email'){ const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); return m ? m[0].toLowerCase() : clean(t).toLowerCase(); }
  if (field==='numero'){ const m = t.match(/\b(\d+[A-Z]?)\b/i); return m ? m[1] : clean(t); }
  if (field==='cp'){ const m = t.match(/\b(\d{5})\b/); return m ? m[1] : clean(t); }
  return clean(t);
}

function promptedRecently(session, key, msWindow){
  const k = `_prompted_${key}`;
  const last = session?.data?.[k] ? new Date(session.data[k]).getTime() : 0;
  const now = Date.now();
  if (now - last < msWindow) return true;
  session.data[k] = new Date(now).toISOString();
  return false;
}

function buildResumePrompt(session){
  const st = session?.stage || 'idle';
  if (st === 'ask_qty' && session?.data?.last_candidate) {
    const s = numberOrZero(session.data.last_candidate.stock);
    return `Estábamos revisando *cantidad* del artículo mostrado (hay ${s} en stock).`;
  }
  if (st === 'cart_open') {
    const cart = session?.data?.cart || [];
    return cart.length ? `Traías un carrito abierto: ${summaryCart(cart)}.` : `Estábamos por abrir carrito.`;
  }
  if (st.startsWith('collect_')) {
    const field = st.replace('collect_','');
    return `Nos quedamos capturando *${LABEL[field] || field}*.`;
  }
  if (st.startsWith('sv_')) return `Continuábamos con los datos para *soporte técnico*.`;
  return `¿Continuamos desde donde nos quedamos o empezamos algo nuevo?`;
}

async function handleCollectSequential(env, session, toE164, text, now){
  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;
  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = session.stage.replace('collect_','');
  c[field] = parseCustomerFragment(field, text);
  if (field==='cp' && !c.ciudad) {
    const info = await cityFromCP(env, c.cp);
    if (info) {
      c.ciudad = info.ciudad || info.municipio || c.ciudad;
      c.estado = info.estado || c.estado;
    }
  }
  await saveSession(env, session, now);
  const nextField = firstMissing(list, c);
  if (nextField){
    session.stage = `collect_${nextField}`;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿${LABEL[nextField]}?`);
    return ok('EVENT_RECEIVED');
  }
  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) {
    await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
    await notifySupport(env, `Nuevo pedido #${res.pedido_id ?? '—'}\nCliente: ${c.nombre} (${toE164})`);
  } else {
    await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
    await notifySupport(env, `Pedido (parcial) ${toE164}. Error: ${res?.error || 'N/A'}`);
  }
  session.stage = 'idle';
  session.data.cart = [];
  await saveSession(env, session, now);
  await sendWhatsAppText(env, toE164, `¿Puedo ayudarte con algo más? (Sí / No)`);
  return ok('EVENT_RECEIVED');
}

/* --------------------------- Resúmenes de carrito ------------------------ */
function summaryCart(cart = []) { return cart.map(i => `${i.product?.nombre} x ${i.qty}${i.backorder ? ' (sobre pedido)' : ''}`).join('; '); }
function splitCart(cart = []){ return { inStockList: cart.filter(i => !i.backorder), backOrderList: cart.filter(i => i.backorder) }; }

/* ========================================================================== */
/* ============================= Cliente & Pedido ========================== */
/* ========================================================================== */

async function preloadCustomerIfAny(env, session){
  try{
    const r = await sbGet(env, 'cliente', { query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${session.from}&limit=1` });
    if (r && r[0]) session.data.customer = { ...(session.data.customer||{}), ...r[0] };
  }catch(e){ console.warn('preloadCustomerIfAny', e); }
}

async function ensureClienteFields(env, cliente_id, c){
  try{
    const patch = {};
    ['nombre','rfc','email','calle','numero','colonia','ciudad','estado','cp'].forEach(k=>{ if (truthy(c[k])) patch[k]=c[k]; });
    if (Object.keys(patch).length>0) await sbPatch(env, 'cliente', patch, `id=eq.${cliente_id}`);
  }catch(e){ console.warn('ensureClienteFields', e); }
}

async function createOrderFromSession(env, session, toE164) {
  try {
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok: false, error: 'empty cart' };
    const c = session.data.customer || {};
    let cliente_id = null;

    // Buscar cliente por teléfono o email
    try {
      const exist = await sbGet(env, 'cliente', {
        query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email || '')})&limit=1`
      });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch (_) {}

    // Crear si no existe
    if (!cliente_id) {
      const ins = await sbUpsert(env, 'cliente', [{
        nombre: c.nombre || null,
        rfc: c.rfc || null,
        email: c.email || null,
        telefono: session.from || null,
        calle: c.calle || null,
        numero: c.numero || null,
        colonia: c.colonia || null,
        ciudad: c.ciudad || null,
        estado: c.estado || null,
        cp: c.cp || null
      }], { onConflict: 'telefono', returning: 'representation' });
      cliente_id = ins?.data?.[0]?.id || null;
    } else {
      await ensureClienteFields(env, cliente_id, c);
    }

    // Total
    let total = 0;
    for (const it of cart) total += Number(it.product?.precio || 0) * Number(it.qty || 1);

    // Crear pedido
    const p = await sbUpsert(env, 'pedido', [{
      cliente_id,
      total,
      moneda: 'MXN',
      estado: 'nuevo',
      created_at: new Date().toISOString()
    }], { returning: 'representation' });
    const pedido_id = p?.data?.[0]?.id;

    // Items
    const items = cart.map(it => ({
      pedido_id,
      producto_id: it.product?.id || null,
      sku: it.product?.sku || null,
      nombre: it.product?.nombre || null,
      qty: it.qty,
      precio_unitario: Number(it.product?.precio || 0)
    }));
    await sbUpsert(env, 'pedido_item', items, { returning: 'minimal' });

    // Decremento de stock por RPC (solo stock real)
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = numberOrZero(row?.[0]?.stock);
        const toDec = Math.min(current, Number(it.qty||0));
        if (toDec > 0) await sbRpc(env, 'decrement_stock', { in_sku: sku, in_by: toDec });
      } catch(e){ console.warn('stock dec', e); }
    }

    return { ok: true, pedido_id, total };
  } catch (e) {
    console.warn('createOrderFromSession', e);
    return { ok: false, error: String(e) };
  }
}

/* ========================================================================== */
/* ============================= Supabase Helpers ========================== */
/* ========================================================================== */

async function sbGet(env, table, { query }){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, {
    headers: { apikey: env.SUPABASE_ANON_KEY, Authorization: `Bearer ${env.SUPABASE_ANON_KEY}` }
  });
  if (!r.ok) { console.warn('sbGet', table, await r.text()); return null; }
  return await r.json();
}

async function sbUpsert(env, table, rows, { onConflict, returning='representation' }={}){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${onConflict?`?on_conflict=${onConflict}`:''}`;
  const r = await fetch(url, {
    method:'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json',
      Prefer:`resolution=merge-duplicates,return=${returning}`
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) { console.warn('sbUpsert', table, await r.text()); return null; }
  const data = returning==='minimal' ? null : await r.json();
  return { data };
}

async function sbPatch(env, table, patch, filter){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method:'PATCH',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json',
      Prefer:'return=minimal'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', table, await r.text());
}

async function sbRpc(env, fn, params){
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method:'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json'
    },
    body: JSON.stringify(params||{})
  });
  if (!r.ok) { console.warn('sbRpc', fn, await r.text()); return null; }
  return await r.json();
}

/* ========================================================================== */
/* ============================== SEPOMEX helper =========================== */
/* ========================================================================== */
async function cityFromCP(env, cp){
  try {
    const r = await sbGet(env, 'sepomex_cp', { query: `cp=eq.${encodeURIComponent(cp)}&select=cp,estado,municipio,ciudad&limit=1` });
    return r?.[0] || null;
  } catch { return null; }
}

/* ========================================================================== */
/* =============================== SOPORTE ================================== */
/* ========================================================================== */

function parseBrandModel(text=''){
  const t = normalizeWithAliases(text);
  let marca = null;
  if (/\bxerox\b/i.test(t)) marca = 'Xerox';
  else if (/\bfujifilm|fuji\s?film\b/i.test(t)) marca = 'Fujifilm';

  const norm = t.replace(/\s+/g,' ').trim();
  const mDocu = norm.match(/\bdocu ?color\s*(550|560|570)\b/i);
  if (mDocu) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${mDocu[1]}` };

  const mVers = norm.match(/\bversant\s*(80|180|2100|280|4100)\b/i);
  if (mVers) return { marca: marca || 'Xerox', modelo: `VERSANT ${mVers[1]}` };

  const mVL = norm.match(/\b(versalink|versa ?link)\s*([a-z0-9\-]+)\b/i);
  if (mVL) return { marca: marca || 'Xerox', modelo: `${mVL[1].replace(/\s/,'').toUpperCase()} ${mVL[2].toUpperCase()}` };

  const mAL = norm.match(/\b(altalink|alta ?link)\s*([a-z0-9\-]+)\b/i);
  if (mAL) return { marca: marca || 'Xerox', modelo: `${mAL[1].replace(/\s/,'').toUpperCase()} ${mAL[2].toUpperCase()}` };

  const mPL = norm.match(/\b(primelink|prime ?link)\s*([a-z0-9\-]+)\b/i);
  if (mPL) return { marca: marca || 'Xerox', modelo: `${mPL[1].replace(/\s/,'').toUpperCase()} ${mPL[2].toUpperCase()}` };

  const mSeries = norm.match(/\b([cb]\d{2,4})\b/i);
  if (mSeries) return { marca, modelo: mSeries[1].toUpperCase() };

  const m550 = norm.match(/\b(550|560|570)\b/);
  if (!marca && /\bxerox\b/i.test(norm)) marca = 'Xerox';
  if (/\bdocu ?color\b/i.test(norm) && m550) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${m550[1]}` };

  return { marca, modelo: null };
}

function extractSvInfo(text) {
  const t = normalizeWithAliases(text);
  const out = {};
  if (/xerox/i.test(t)) out.marca = 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(t)) out.marca = 'Fujifilm';

  const pm = parseBrandModel(text);
  if (pm.marca && !out.marca) out.marca = pm.marca;
  if (pm.modelo) out.modelo = pm.modelo;

  const err = t.match(/\berror\s*([0-9\-]+)\b/i);
  if (err) out.error_code = err[1];

  if (/no imprime/i.test(t)) out.falla = 'No imprime';
  if (/atasc(a|o)|se atora|se traba|arrugad(i|o)|saca el papel/i.test(t)) out.falla = 'Atasco/arrugado de papel';
  if (/mancha|calidad|linea|l[ií]nea/i.test(t)) out.falla = 'Calidad de impresión';
  if (/\b(parado|urgente|producci[oó]n detenida|parada)\b/i.test(t)) out.prioridad = 'alta';

  const loose = parseAddressLoose(text);
  Object.assign(out, loose);

  const d = parseCustomerText(text);
  if (d.calle) out.calle = d.calle;
  if (d.numero) out.numero = d.numero;
  if (d.colonia) out.colonia = d.colonia;
  if (d.cp) out.cp = d.cp;
  if (d.ciudad) out.ciudad = d.ciudad;
  if (d.estado) out.estado = d.estado;

  return out;
}

function svFillFromAnswer(sv, field, text, env){
  const pm = parseBrandModel(text);
  if (field === 'modelo') {
    if (pm.marca) sv.marca = pm.marca;
    if (pm.modelo) sv.modelo = pm.modelo;
    if (!sv.modelo) sv.modelo = clean(text);
    return;
  }
  if (field === 'falla')   { sv.falla = clean(text); return; }
  if (field === 'nombre')  { sv.nombre = clean(text); return; }
  if (field === 'email')   { const em = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); sv.email = em ? em[0].toLowerCase() : clean(text).toLowerCase(); return; }
  if (field === 'calle')   { sv.calle = clean(text); return; }
  if (field === 'numero')  { const mnum = text.match(/\b(\d+[A-Z]?)\b/); sv.numero = mnum?mnum[1]:clean(text); return; }
  if (field === 'colonia') { sv.colonia = clean(text); return; }
  if (field === 'ciudad')  { sv.ciudad = clean(text); return; }
  if (field === 'estado')  { sv.estado = clean(text); return; }
  if (field === 'cp')      { const mcp = text.match(/\b(\d{5})\b/); sv.cp = mcp?mcp[1]:clean(text); return; }
  if (field === 'horario') { const dt = parseNaturalDateTime(text, env); if (dt?.start) sv.when = dt; return; }
}

function displayFieldSupport(k){
  const map = {
    modelo:'marca y modelo', falla:'descripción breve de la falla', nombre:'Nombre o Razón Social', email:'email',
    calle:'calle', numero:'número', colonia:'colonia', ciudad:'ciudad o municipio', estado:'estado', cp:'código postal', horario:'día y hora (10:00–15:00)'
  };
  return map[k]||k;
}

function quickHelp(ntext){
  if (/\batasc(a|o)|se atora|se traba|arrugad/i.test(ntext)){
    return `Veamos rápido 🧰\n1) Apaga y enciende el equipo.\n2) Revisa bandejas y retira papel atorado.\n3) Abre y cierra el fusor con cuidado.\nSi sigue igual, agendamos visita para diagnóstico.`;
  }
  if (/\bno imprime\b/.test(ntext)){
    return `Probemos rápido 🧰\n1) Reinicia la impresora.\n2) Verifica tóner y que puertas estén cerradas.\n3) Intenta imprimir una página de prueba.\nSi persiste, agendamos visita.`;
  }
  if (/\bmancha|l[ií]ne?a|calidad\b/.test(ntext)){
    return `Sugerencia rápida 🎯\n1) Imprime un patrón de prueba.\n2) Revisa niveles y reinstala tóners.\n3) Limpia rodillos si es posible.\nSi no mejora, te agendo visita para revisión.`;
  }
  return null;
}

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    session.data.last_intent = 'support';
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    if (session.stage === 'sv_collect' && session.data.sv_need_next) {
      svFillFromAnswer(sv, session.data.sv_need_next, text, env);
      await saveSession(env, session, now);
    }

    const mined = extractSvInfo(text);
    if (!sv.marca && mined.marca) sv.marca = mined.marca;
    if (!sv.modelo && mined.modelo) sv.modelo = mined.modelo;
    if (!sv.falla && mined.falla) sv.falla = mined.falla;
    if (!sv.when && mined.when) sv.when = mined.when;
    ['calle','numero','colonia','cp','ciudad','estado','error_code','prioridad','nombre','email'].forEach(k=>{
      if (!truthy(sv[k]) && truthy(mined[k])) sv[k]=mined[k];
    });

    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Lamento escuchar la falla 😕. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* del problema.`);
    }

    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick);
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;

    const needed = [];
    if (!(truthy(sv.marca) && truthy(sv.modelo))) {
      const pmNow = parseBrandModel(text);
      if (pmNow.marca && !sv.marca) sv.marca = pmNow.marca;
      if (pmNow.modelo && !sv.modelo) sv.modelo = pmNow.modelo;
    }
    if (!(truthy(sv.marca) && truthy(sv.modelo))) needed.push('modelo');

    if (!truthy(sv.falla))  needed.push('falla');
    if (!truthy(sv.calle))  needed.push('calle');
    if (!truthy(sv.numero)) needed.push('numero');
    if (!truthy(sv.colonia))needed.push('colonia');
    if (!truthy(sv.cp))     needed.push('cp');
    if (!sv.when?.start)    needed.push('horario');
    if (!truthy(sv.nombre)) needed.push('nombre');
    if (!truthy(sv.email))  needed.push('email');

    if (needed.length) {
      session.stage = 'sv_collect';
      session.data.sv_need_next = needed[0];
      await saveSession(env, session, now);
      const Q = {
        modelo: '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox DocuColor 550* o *Xerox Versant 180*)',
        falla: '¿Me describes brevemente la *falla*? (p.ej., "*atasco en fusor*", "*no imprime*")',
        calle: '¿En qué *calle* está el equipo?',
        numero: '¿Qué *número* es?',
        colonia: '¿*Colonia*?',
        cp: '¿*Código Postal* (5 dígitos)?',
        horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (ej: "*mañana 12:30*")',
        nombre: '¿A nombre de quién registramos la visita?',
        email: '¿Cuál es tu *email* para enviarte confirmaciones?'
      };
      await sendWhatsAppText(env, toE164, Q[needed[0]]);
      return ok('EVENT_RECEIVED');
    }

    // === Agenda (GCal) + OS ===
    let pool = [];
    try { pool = await getCalendarPool(env) || []; } catch(e){ console.warn('[GCal] pool', e); }
    const cal = pickCalendarFromPool(pool);
    const tz = env.TZ || 'America/Mexico_City';
    const chosen = clampToWindow(sv.when, tz);

    const cliente_id = await upsertClienteByPhone(env, session.from);
    try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email, calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp }); } catch (_) {}

    let slot = chosen, event = null, calName = '';
    if (cal && env.GCAL_REFRESH_TOKEN && env.GCAL_CLIENT_ID && env.GCAL_CLIENT_SECRET) {
      try {
        slot = await findNearestFreeSlot(env, cal.gcal_id, chosen, tz);
        event = await gcalCreateEvent(env, cal.gcal_id, {
          summary: `Visita técnica: ${(sv.marca || '')} ${(sv.modelo || '')}`.trim(),
          description: renderOsDescription(session.from, sv),
          start: slot.start, end: slot.end, timezone: tz,
        });
        calName = cal.name || '';
      } catch (e) { console.warn('[GCal] create error', e); }
    }

    // Crear OS
    let osId = null; let estado = event ? 'agendado' : 'pendiente';
    try {
      const osBody = [{
        cliente_id, marca: sv.marca || null, modelo: sv.modelo || null, falla_descripcion: sv.falla || null,
        prioridad: sv.prioridad || 'media', estado,
        ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(),
        gcal_event_id: event?.id || null, calendar_id: cal?.gcal_id || null,
        calle: sv.calle || null, numero: sv.numero || null, colonia: sv.colonia || null, ciudad: sv.ciudad || null, estado: sv.estado || null, cp: sv.cp || null,
        created_at: new Date().toISOString()
      }];
      const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
      osId = os?.data?.[0]?.id || null;
    } catch (e) { console.warn('[Supabase] OS upsert', e); estado = 'pendiente'; }

    if (event) {
      await sendWhatsAppText(env, toE164,
        `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\nDirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}\nTécnico asignado: ${calName || 'por confirmar'}.\n\nSi necesitas reprogramar o cancelar, dímelo con confianza.`
      );
      session.stage = 'sv_scheduled';
    } else {
      await sendWhatsAppText(env, toE164, `Tengo tus datos ✍️. En breve te confirmo el horario exacto por este medio.`);
      await notifySupport(env, `OS *pendiente/agendar* para ${toE164}\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla}\nDirección: ${sv.calle} ${sv.numero} ${sv.colonia}, CP ${sv.cp} ${sv.ciudad||''}\nNombre: ${sv.nombre} | Email: ${sv.email}`);
      session.stage = 'sv_scheduled';
    }

    session.data.sv.os_id = osId;
    session.data.sv.gcal_event_id = event?.id || null;
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');

  } catch (e) {
    console.warn('[SUPPORT] handleSupport catch', e);
    try{
      const need = session?.data?.sv_need_next || 'modelo';
      await sendWhatsAppText(env, toE164, `Gracias por la info. Para avanzar, ¿${displayFieldSupport(need)}?`);
    }catch{
      await sendWhatsAppText(env, toE164, `Tomé tu solicitud de soporte. Si te parece, seguimos con los datos para agendar o te contacto enseguida 🙌`);
    }
    return ok('EVENT_RECEIVED');
  }
}

async function svCancel(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para cancelar.`); return; }
  if (os.gcal_event_id && os.calendar_id) await gcalDeleteEvent(env, os.calendar_id, os.gcal_event_id);
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'cancelada', cancel_reason: 'cliente' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *cancelado* tu visita. Si necesitas agendar otra, aquí estoy 🙂`);
}

async function svReschedule(env, session, toE164, when) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para reprogramar.`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  const chosen = clampToWindow(when, tz);
  const slot = await findNearestFreeSlot(env, os.calendar_id, chosen, tz);
  if (os.gcal_event_id && os.calendar_id) {
    await gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, { start: { dateTime: slot.start, timeZone: tz }, end: { dateTime: slot.end, timeZone: tz } });
  }
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'reprogramado', ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString() }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *reprogramado* tu visita a:\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}* ✅`);
}

async function svWhenIsMyVisit(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  const tz = env.TZ || 'America/Mexico_City';
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita programada. ¿Agendamos una?`); return; }
  await sendWhatsAppText(env, toE164, `Tu próxima visita: *${fmtDate(os.ventana_inicio, tz)}*, de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*. Estado: ${os.estado}.`);
}

/* ========================================================================== */
/* ============================ Google Calendar ============================ */
/* ========================================================================== */

async function gcalToken(env) {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: env.GCAL_CLIENT_ID, client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN, grant_type: 'refresh_token'
    })
  });
  if (!r.ok) { console.warn('gcal token', await r.text()); return null; }
  const j = await r.json(); return j.access_token;
}

async function gcalCreateEvent(env, calendarId, { summary, description, start, end, timezone }) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  const body = { summary, description, start: { dateTime: start, timeZone: timezone }, end: { dateTime: end, timeZone: timezone } };
  const r = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!r.ok) { console.warn('gcal create', await r.text()); return null; }
  return await r.json();
}

async function gcalPatchEvent(env, calendarId, eventId, patch) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await fetch(url, { method: 'PATCH', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(patch) });
  if (!r.ok) { console.warn('gcal patch', await r.text()); return null; }
  return await r.json();
}

async function gcalDeleteEvent(env, calendarId, eventId) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await fetch(url, { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) console.warn('gcal delete', await r.text());
}

async function isBusy(env, calendarId, startISO, endISO) {
  const token = await gcalToken(env); if (!token) return false;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?timeMin=${encodeURIComponent(startISO)}&timeMax=${encodeURIComponent(endISO)}&singleEvents=true&orderBy=startTime`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) { console.warn('gcal list', await r.text()); return false; }
  const j = await r.json();
  return Array.isArray(j.items) && j.items.length > 0;
}

async function findNearestFreeSlot(env, calendarId, when, tz) {
  if (!calendarId) return when;
  let curStart = new Date(when.start);
  let curEnd = new Date(when.end);
  for (let i=0;i<6;i++) {
    const busy = await isBusy(env, calendarId, curStart.toISOString(), curEnd.toISOString());
    if (!busy) break;
    curStart = new Date(curStart.getTime()+30*60*1000);
    curEnd = new Date(curStart.getTime()+30*60*1000);
    const h = curStart.getHours();
    if (h >= 15) {
      curStart.setDate(curStart.getDate()+1); curStart.setHours(10,0,0,0);
      curEnd = new Date(curStart.getTime()+60*60*1000);
    }
  }
  return { start: curStart.toISOString(), end: curEnd.toISOString() };
}

/* ========================================================================== */
/* ============================ Pool calendarios =========================== */
/* ========================================================================== */
async function getCalendarPool(env) {
  const r = await sbGet(env, 'calendar_pool', { query: 'select=gcal_id,name,active&active=is.true' });
  return Array.isArray(r) ? r : [];
}
function pickCalendarFromPool(pool) { return pool?.[0] || null; }

function renderOsDescription(phone, sv) {
  return [
    `Cliente: +${phone} (${sv.nombre || 'N/D'} / ${sv.email || 'sin email'})`,
    `Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
    `Falla: ${sv.falla || 'N/D'}${sv.error_code ? ' (Error ' + sv.error_code + ')' : ''}`,
    `Prioridad: ${sv.prioridad || 'media'}`,
    `Dirección: ${sv.calle || ''} ${sv.numero || ''}, ${sv.colonia || ''}, ${sv.ciudad || ''}, ${sv.estado || ''}, CP ${sv.cp || ''}`
  ].join('\n');
}

async function getLastOpenOS(env, phone) {
  try {
    const c = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    const cid = c?.[0]?.id;
    if (!cid) return null;
    const r = await sbGet(env, 'orden_servicio', {
      query: `select=id,estado,ventana_inicio,ventana_fin,calendar_id,gcal_event_id,cliente_id&cliente_id=eq.${cid}&order=ventana_inicio.desc&limit=1`
    });
    if (r && r[0] && ['agendado','reprogramado','confirmado'].includes(r[0].estado)) return r[0];
  } catch {}
  return null;
}

async function upsertClienteByPhone(env, phone) {
  try {
    const exist = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (exist && exist[0]?.id) return exist[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.data?.[0]?.id || null;
  } catch (e) { console.warn('upsertClienteByPhone', e); return null; }
}

/* ========================================================================== */
/* =============================== Fechas ================================== */
/* ========================================================================== */

function fmtDate(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }
  catch{ return new Date(d).toLocaleDateString('es-MX'); }
}

function fmtTime(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }
  catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; }
}

function parseNaturalDateTime(text, env) {
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

function clampToWindow(when, tz) {
  const start = new Date(when.start);
  const hours = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone:tz }).format(start));
  let newStart = new Date(start);
  if (hours < 10) newStart.setHours(10,0,0,0);
  if (hours >= 15) newStart.setHours(14,0,0,0);
  const newEnd = new Date(newStart.getTime()+60*60*1000);
  return { start: newStart.toISOString(), end: newEnd.toISOString() };
}

/* ========================================================================== */
/* =================== Dirección & Cliente (heurísticos) ==================== */
/* ========================================================================== */

function parseAddressLoose(text=''){
  const out = {};
  const mcp = text.match(/\bcp\s*(\d{5})\b/i) || text.match(/\b(\d{5})\b/);
  if (mcp) out.cp = mcp[1];
  const calle = text.match(/\bcalle\s+([a-z0-9\s\.#\-]+)\b/i);
  if (calle) out.calle = clean(calle[1]);
  const num = text.match(/\bn[uú]mero\s+(\d+[A-Z]?)\b/i);
  if (num) out.numero = num[1];
  const col = text.match(/\bcolonia\s+([a-z0-9\s\.\-]+)\b/i);
  if (col) out.colonia = clean(col[1]);
  return out;
}

function parseCustomerText(text=''){
  const out = {};
  const email = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (email) out.email = email[0].toLowerCase();
  const nombre = text.match(/\b(soy|me llamo)\s+([a-záéíóúñ\s]{3,})/i);
  if (nombre) out.nombre = toTitleCase(firstWord(nombre[2]));
  return out;
}

/* ========================================================================== */
/* ======================== WhatsApp Context Extractor ====================== */
/* ========================================================================== */

function extractWhatsAppContext(payload) {
  try {
    const entry = payload?.entry?.[0];
    const changes = entry?.changes?.[0];
    const value = changes?.value;
    const message = value?.messages?.[0];
    if (!message) return null;

    const from = message.from;
    const fromE164 = `+${from}`;
    const mid = message.id;
    const profileName = value?.contacts?.[0]?.profile?.name || '';
    const ts = Number(message.timestamp ? `${message.timestamp}000` : Date.now()); // epoch sec → ms

    let textRaw = '';
    let msgType = 'unknown';

    if (message.text) {
      textRaw = message.text.body || '';
      msgType = 'text';
    } else if (message.interactive) {
      if (message.interactive.type === 'button_reply') {
        textRaw = message.interactive.button_reply?.title || '';
        msgType = 'interactive_button';
      } else if (message.interactive.type === 'list_reply') {
        textRaw = message.interactive.list_reply?.title || '';
        msgType = 'interactive_list';
      }
    }

    return { from, fromE164, mid, profileName, textRaw, msgType, ts };
  } catch (e) {
    console.warn('extractWhatsAppContext error', e);
    return null;
  }
}



