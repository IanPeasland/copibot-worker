/**
 * CopiBot – IA + Ventas + Soporte + GCal — Build R6.3 (2025-09)
 * Cambios R6.3:
 *  - Sesión dual-key (from & fromE164) para no perder sv_collect / candados.
 *  - Regla dura: texto con marca+modelo (sin palabras de compra) => soporte.
 *  - ACK explícito de marca/modelo en soporte, prompts seguros y FAQs rápidas.
 *  - Ventas: mantiene copy de producto y “sobre pedido” sin cortar el flujo.
 *  - Dirección confirmable y edición antes de agendar; múltiples direcciones por cliente.
 *  - Limpieza de duplicados y orden único de funciones para evitar errores de build.
 */

/* ========================================================================== */
/* =============================== Export =================================== */
/* ========================================================================== */

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

  // Desestructura ctx ANTES de usar variables en debug
  const { mid, from, fromE164, profileName, textRaw, msgType } = ctx;
  const originalText = (textRaw || '').trim();
  const lowered = originalText.toLowerCase();
  const ntext = normalizeWithAliases(originalText);

  // ——— DEBUG WEBHOOK (salida temprana) ———
  if (env.DEBUG_WEBHOOK && String(env.DEBUG_WEBHOOK).toLowerCase() === 'true') {
    try {
      await sendWhatsAppText(env, fromE164, `✅ webhook ok: "${(textRaw || '').slice(0, 50)}"`);
    } catch (e) {
      console.error('[debug] sendWhatsAppText error', e);
    }
    await saveSessionMulti(env, session, from, fromE164);
    return ok('EVENT_RECEIVED');
  }
  
        const { mid, from, fromE164, profileName, textRaw, msgType } = ctx;
        const originalText = (textRaw || '').trim();
        const lowered = originalText.toLowerCase();
        const ntext = normalizeWithAliases(originalText);

        // ===== Session =====
        const now = new Date();
        let session = await loadSessionMulti(env, from, fromE164); // dual-key
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
         *  SALUDO TEMPRANO (failsafe)
         *  Se ejecuta ANTES de cualquier otra lógica para garantizar respuesta.
         * ============================================================ */
        if (RX_GREET.test(lowered)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          session.data.last_greet_at = now.toISOString();
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
         *  REGLA DURA: Marca+Modelo (sin palabras de compra) => soporte
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
      // Failsafe: intentar responder algo al usuario si podemos.
      try {
        const reqClone = await req.clone().text().catch(()=>null);
        // Intentamos tomar el phone rápido del body por si es el webhook de WhatsApp
        let to = null;
        try {
          const j = reqClone ? JSON.parse(reqClone) : null;
          const v = j?.entry?.[0]?.changes?.[0]?.value;
          const m = v?.messages?.[0];
          if (m?.from) to = `+${m.from}`;
        } catch {}
        if (to) await sendWhatsAppText(env, to, 'Tuvimos un problema momentáneo al procesar tu mensaje. ¿Puedes repetirlo, por favor? 🙏');
      } catch {}
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
function dlog(env, ...args){ if ((env.DEBUG||'').toString().toLowerCase()==='true') console.log('[DBG]', ...args); }

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
/* ================================= IA ===================================== */
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
  const payload = { messaging_product: 'whatsapp', to: toE164.replace(/\D/g, ''), text: { body: String(body ?? '') } };
  const r = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${env.WA_TOKEN}`, 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!r.ok) console.warn('sendWhatsAppText', r.status, await r.text());
}

async function notifySupport(env, body) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  await sendWhatsAppText(env, to, `🛎️ *Soporte*\n${body}`);
}

/* ========================================================================== */
/* ============================ Sesión (Supabase) ============================ */
/* ========================================================================== */

/**
 * Migramos la sesión a Supabase (tabla wa_session) para no depender de KV.
 * - Mantiene API: loadSessionMulti / saveSessionMulti (no cambies llamadas).
 * - TTL lógico: 90 días (limpieza en cronReminders).
 *
 * Esquema esperado (ya lo tienes, pero por si acaso):
 *
 * CREATE TABLE IF NOT EXISTS public.wa_session (
 *   phone       text PRIMARY KEY,
 *   data        jsonb NOT NULL DEFAULT '{}'::jsonb,
 *   stage       text  NOT NULL DEFAULT 'idle',
 *   updated_at  timestamptz NOT NULL DEFAULT now()
 * );
 * CREATE INDEX IF NOT EXISTS idx_wa_session_updated_at ON public.wa_session(updated_at);
 */

const SESS_TTL_DAYS = 90;

// Cache de proceso (fallback mínimo por request burst). No garantiza cross-instance.
// Expira a los ~5 minutos para no causar efectos raros entre despliegues.
const MEM_CACHE = new Map();
const MEM_TTL_MS = 5 * 60 * 1000;

function memGet(key) {
  const it = MEM_CACHE.get(key);
  if (!it) return null;
  if (Date.now() - it.t > MEM_TTL_MS) { MEM_CACHE.delete(key); return null; }
  return it.v;
}
function memSet(key, val) { MEM_CACHE.set(key, { v: val, t: Date.now() }); }

/**
 * Normaliza una sesión base.
 */
function blankSession(from) {
  return { from, stage: 'idle', data: {} };
}

/**
 * Carga por cualquiera de las dos llaves (from / fromE164).
 * Devuelve un objeto sesión siempre (aunque no exista en BD).
 */
async function loadSessionMulti(env, from, fromE164) {
  try {
    const k1 = `sess:${fromE164}`;
    const k2 = `sess:${from}`;

    // 1) Intenta cache en memoria
    const mem = memGet(k1) || memGet(k2);
    if (mem) return mem;

    // 2) Lee Supabase por phone = fromE164 o from (el que venga con +)
    const phone = String(fromE164 || from || '').trim();
    if (!phone) return blankSession(from);

    const q = `select=phone,data,stage,updated_at&phone=eq.${encodeURIComponent(phone)}&limit=1`;
    const row = await sbGet(env, 'wa_session', { query: q });
    if (row && row[0]) {
      const sess = {
        from,
        stage: row[0].stage || 'idle',
        data: row[0].data || {},
      };
      memSet(k1, sess);
      memSet(k2, sess);
      return sess;
    }

    // 3) Si no existe, crea default en memoria (no escribe aún)
    const fresh = blankSession(from);
    memSet(k1, fresh); memSet(k2, fresh);
    return fresh;
  } catch (e) {
    console.warn('[session] loadSessionMulti error', e);
    // En peor caso, sesión efímera.
    return blankSession(from);
  }
}

/**
 * Guarda/upsertea la sesión en Supabase con updated_at=now()
 * y también la sibila en la cache de proceso.
 */
async function saveSessionMulti(env, session, from, fromE164) {
  try {
    const phone = String(fromE164 || from || '').trim();
    if (!phone) return;

    const payload = [{
      phone,
      data: session?.data || {},
      stage: session?.stage || 'idle',
      updated_at: new Date().toISOString()
    }];

    // upsert por PK phone
    await sbUpsert(env, 'wa_session', payload, { onConflict: 'phone', returning: 'minimal' });

    // cache
    const k1 = `sess:${fromE164}`;
    const k2 = `sess:${from}`;
    memSet(k1, session);
    memSet(k2, session);
  } catch (e) {
    console.warn('[session] saveSessionMulti error', e);
  }
}

/* ========================================================================== */
/* ============================ Limpieza (cron) ============================== */
/* ========================================================================== */

/**
 * El cronReminders ya existe en tu worker; aquí sólo agregamos
 * una mini-limpieza de sesiones > 90 días para no crecer indefinidamente.
 * Llama a esta función dentro de cronReminders(env).
 */
async function cleanupOldSessions(env) {
  try {
    // Elimina lógicamente: si tienes RLS estricta, puedes cambiar por un UPDATE a estado "archivado" o similar.
    // Aquí usamos RPC para no habilitar DELETE abierto; si no tienes RPC, usa PATCH con policy.
    // Como fallback simple: UPDATE y deja que un job de mantenimiento las borre.
    const sql =
      `delete from wa_session where updated_at < now() - interval '${SESS_TTL_DAYS} days';`;
    await sbRpc(env, 'exec_sql', { sql }).catch(() => null);
  } catch (e) {
    console.warn('[session] cleanupOldSessions', e);
  }
}

// Si no tienes la función RPC exec_sql, omite el borrado (no es crítico).
// También puedes crearla así (opcional):
// CREATE OR REPLACE FUNCTION public.exec_sql(sql text) RETURNS void AS $$ BEGIN EXECUTE sql; END; $$ LANGUAGE plpgsql SECURITY DEFINER;

/* ========================================================================== */
/* ============================== Ventas ==================================== */
/* ========================================================================== */

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

/** Render compacto y claro (si stock=0, muestra “sobre pedido”, no corta flujo). */
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

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
  const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} sobre pedido)` : '';
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

    // —— Búsqueda escalonada
    let best = await findBestProduct(env, enrichedQ, { relaxed: false });
    if (!best) best = await findBestProduct(env, enrichedQ, { relaxed: true });
    if (!best) best = await findBestProduct(env, 'toner', { relaxed: true, ignoreFamily: true });

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

async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `Perfecto, quedo al pendiente. Si necesitas algo más, aquí estoy 🙂`);
    return ok('EVENT_RECEIVED');
  }

  const saysNo = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};

  if (!saysYes && !saysNo && /hola|cómo estás|como estas|gracias/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', originalText);
    await sendWhatsAppText(env, toE164, friendly);
    if (!promptedRecently(session, 'invoice', 3*60*1000)) {
      await sendWhatsAppText(env, toE164, `Por cierto, ¿la quieres *con factura* o *sin factura*?`);
    }
    await saveSessionMulti(env, session, session.from, toE164);
    return ok('EVENT_RECEIVED');
  }

  if (saysYes || saysNo) {
    session.data.requires_invoice = !!saysYes;
    await preloadCustomerIfAny(env, session);
    const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
    const need = firstMissing(list, session.data.customer);
    if (need) {
      session.stage = `collect_${need}`;
      await saveSessionMulti(env, session, session.from, toE164);
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
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más en este momento? (Sí / No)`);
    return ok('EVENT_RECEIVED');
  }

  if (!promptedRecently(session, 'invoice', 2*60*1000)) {
    await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  }
  await saveSessionMulti(env, session, session.from, toE164);
  return ok('EVENT_RECEIVED');
}

/* ====== Captura UNO A UNO (ventas) ====== */
const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL = { nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', cp:'Código Postal' };

function firstMissing(list, c={}){ for (const k of list){ if (!truthy(c[k])) return k; } return null; }

/* =============== Inventario & Búsquedas =============== */

function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};
  if (/\bversant\b/.test(t) || /\b(80|180|2100|280|4100)\b/.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/.test(t) || /\bprimelink\b/.test(t)) out.family = 'primelink';
  else if (/\bapeos\b/.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/.test(t)) out.family = 'c70';

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

/** Búsqueda escalonada (estricta → relajada → genérica) */
async function findBestProduct(env, queryText, opts = {}) {
  const { relaxed = false, ignoreFamily = false } = opts;
  const hints = extractModelHints(queryText);
  const colorCode = hints.color || extractColorWord(queryText);

  const pick = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = colorCode ? arr.filter(p => productHasColor(p, colorCode)) : arr.slice();
    if (hints.family && !relaxed && !ignoreFamily) {
      const familyPool = pool.filter(p => productMatchesFamily(p, hints.family));
      pool = familyPool.length ? familyPool : pool;
    }
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });
    return pool[0] || null;
  };

  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 50 }) || [];
    const best = pick(res);
    if (best) return best;
  } catch (e) {
    console.warn('[findBestProduct] rpc match_products_trgm', e);
  }

  const fam = (!ignoreFamily && hints.family) ? hints.family : null;
  if (fam && !relaxed) {
    try {
      const like = encodeURIComponent(`%${fam}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like},compatible.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
    } catch (e) {
      console.warn('[findBestProduct] LIKE familia', e);
    }
  }

  try {
    const like = encodeURIComponent(`%toner%`);
    const r = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},tipo.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
    }) || [];
    const best = pick(r);
    if (best) return best;
  } catch (e) {
    console.warn('[findBestProduct] LIKE toner', e);
  }

  return null;
}

function extractColorWord(text=''){
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return 'yellow';
  if (/\bmagenta\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'black';
  return null;
}

async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  const extracted = await aiExtractTonerQuery(env, ntext).catch(()=>null);
  const enrichedQ = enrichQueryFromAI(ntext, extracted);

  let best = await findBestProduct(env, enrichedQ, { relaxed: false });
  if (!best) best = await findBestProduct(env, enrichedQ, { relaxed: true });
  if (!best) best = await findBestProduct(env, 'toner', { relaxed: true, ignoreFamily: true });

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
/* =============================== SOPORTE ================================== */
/* ========================================================================== */

/** Marca+Modelo a partir de texto del usuario (con marcas implícitas). */
function parseBrandModel(text=''){
  const t = normalizeWithAliases(text);
  let marca = null;

  // Marca explícita
  if (/\bxerox\b/i.test(t)) marca = 'Xerox';
  else if (/\bfujifilm|fuji\s?film\b/i.test(t)) marca = 'Fujifilm';

  const norm = t.replace(/\s+/g,' ').trim();

  // DocuColor ⇒ Xerox
  const mDocu = norm.match(/\bdocu\s*color\s*(550|560|570)\b/i) || norm.match(/\bdocucolor\s*(550|560|570)\b/i);
  if (mDocu) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${mDocu[1]}` };

  // Versant ⇒ Xerox
  const mVers = norm.match(/\bversant\s*(80|180|2100|280|4100)\b/i);
  if (mVers) return { marca: marca || 'Xerox', modelo: `VERSANT ${mVers[1]}` };

  // VersaLink / AltaLink / PrimeLink ⇒ Xerox
  const mVL = norm.match(/\b(versalink|versa\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mVL) return { marca: marca || 'Xerox', modelo: `${mVL[1].replace(/\s/,'').toUpperCase()} ${mVL[2].toUpperCase()}` };

  const mAL = norm.match(/\b(altalink|alta\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mAL) return { marca: marca || 'Xerox', modelo: `${mAL[1].replace(/\s/,'').toUpperCase()} ${mAL[2].toUpperCase()}` };

  const mPL = norm.match(/\b(primelink|prime\s*link)\s*([a-z0-9\-]+)\b/i);
  if (mPL) return { marca: marca || 'Xerox', modelo: `${mPL[1].replace(/\s/,'').toUpperCase()} ${mPL[2].toUpperCase()}` };

  // Apeos ⇒ Fujifilm
  const mApeos = norm.match(/\bapeos\s*([a-z0-9\-]+)?\b/i);
  if (mApeos) return { marca: marca || 'Fujifilm', modelo: `APEOS${mApeos[1] ? ' ' + mApeos[1].toUpperCase() : ''}`.trim() };

  // Series C/B
  const mSeries = norm.match(/\b([cb]\d{2,4})\b/i);
  if (mSeries) return { marca: marca || 'Xerox', modelo: mSeries[1].toUpperCase() };

  // Sólo “docucolor” o “versant”
  if (/\bdocu\s*color\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'DOCUCOLOR' };
  if (/\bversant\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'VERSANT' };

  return { marca, modelo: null };
}

/** Extrae info útil para soporte (falla/urgencia/dirección/email/CP…). */
function extractSvInfo(text) {
  const t = normalizeWithAliases(text);
  const out = {};

  // Marca/modelo
  const pm = parseBrandModel(text);
  if (pm.marca) out.marca = pm.marca;
  if (pm.modelo) out.modelo = pm.modelo;

  // Señales de marca suelta
  if (/xerox/i.test(t)) out.marca = out.marca || 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(t)) out.marca = out.marca || 'Fujifilm';

  // Error explícito
  const err = t.match(/\berror\s*([0-9\-]+)\b/i);
  if (err) out.error_code = err[1];

  // Falla común
  if (/no imprime/i.test(t)) out.falla = out.falla || 'No imprime';
  if (/atasc(a|o)|se atora|se traba|arrugad(i|o)|fusor/i.test(t)) out.falla = out.falla || 'Atasco/arrugado de papel';
  if (/mancha|calidad|linea|l[ií]nea/i.test(t)) out.falla = out.falla || 'Calidad de impresión';
  if (/\b(parado|urgente|producci[oó]n detenida|parada)\b/i.test(t)) out.prioridad = 'alta';

  // Datos de contacto / dirección (sueltos)
  const loose = parseAddressLoose(text);
  Object.assign(out, loose);

  const d = parseCustomerText(text);
  if (d.calle) out.calle = d.calle;
  if (d.numero) out.numero = d.numero;
  if (d.colonia) out.colonia = d.colonia;
  if (d.cp) out.cp = d.cp;

  return out;
}

/** Aplica la respuesta del usuario al campo que se está pidiendo en soporte. */
function svFillFromAnswer(sv, field, text){
  const pm = parseBrandModel(text);
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
  if (field === 'numero') { const mnum = text.match(/\b(\d+[A-Z]?)\b/); sv.numero = mnum?mnum[1]:clean(text); return; }
  if (field === 'colonia') { sv.colonia = clean(text); return; }
  if (field === 'ciudad') { sv.ciudad = clean(text); return; }
  if (field === 'estado') { sv.estado = clean(text); return; }
  if (field === 'cp') { const mcp = text.match(/\b(\d{5})\b/); sv.cp = mcp?mcp[1]:clean(text); return; }
  if (field === 'horario') { return; }
}

/** Texto amable con consejos rápidos y naturales, sin cortar el flujo. */
function quickHelp(ntext){
  const t = ntext || '';
  if (/\batasc(a|o)|se atora|se traba|arrugad|fusor/i.test(t)){
    return `Veamos rápido 🧰\n1) Apaga y enciende el equipo.\n2) Retira papel atorado de bandejas y fusor (con el equipo apagado).\n3) Abre y cierra el fusor con cuidado.\nSi sigue igual, te propongo agendar visita para diagnóstico.`;
  }
  if (/\bno imprime\b/.test(t)){
    return `Probemos rápido 🧰\n1) Reinicia la impresora.\n2) Verifica tóner/puertas.\n3) Intenta una página de prueba.\nSi persiste, te propongo visita técnica.`;
  }
  if (/\bmancha|l[ií]ne?a|calidad\b/.test(t)){
    return `Sugerencia rápida 🎯\n1) Imprime un patrón de prueba.\n2) Revisa niveles/reinstala tóners.\n3) Limpia rodillos si es posible.\nSi no mejora, conviene visita para revisión.`;
  }
  return null;
}

function displayFieldSupport(k){
  const map = {
    modelo:'marca y modelo', falla:'descripción breve de la falla', nombre:'Nombre o Razón Social', email:'email',
    calle:'calle', numero:'número', colonia:'colonia', ciudad:'ciudad o municipio', estado:'estado', cp:'código postal', horario:'día y hora (10:00–15:00)'
  };
  return map[k]||k;
}

/* =================== Direcciones: múltiples por cliente ==================== */

async function getClienteByPhone(env, phone){
  try{
    const r = await sbGet(env, 'cliente', { query: `select=id,nombre,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${phone}&limit=1` });
    return (r && r[0]) ? r[0] : null;
  }catch{ return null; }
}

async function listCustomerAddresses(env, cliente_id){
  try{
    const r = await sbGet(env, 'cliente_direccion', {
      query: `select=id,alias,calle,numero,colonia,ciudad,estado,cp,is_default&cliente_id=eq.${cliente_id}&order=is_default.desc,id.asc`
    });
    return Array.isArray(r) ? r : [];
  }catch{ return []; }
}

async function upsertAddress(env, cliente_id, dir){
  try{
    const row = [{
      cliente_id,
      alias: dir.alias || null,
      calle: dir.calle || null,
      numero: dir.numero || null,
      colonia: dir.colonia || null,
      ciudad: dir.ciudad || null,
      estado: dir.estado || null,
      cp: dir.cp || null,
      is_default: dir.is_default === true
    }];
    const ins = await sbUpsert(env, 'cliente_direccion', row, { onConflict: 'id', returning: 'representation' });
    return ins?.data?.[0] || null;
  }catch{ return null; }
}

async function markDefaultAddress(env, cliente_id, id){
  try{
    // Quita default a las demás
    await sbPatch(env, 'cliente_direccion', { is_default: false }, `cliente_id=eq.${cliente_id}`);
    // Marca esta
    await sbPatch(env, 'cliente_direccion', { is_default: true }, `id=eq.${id}`);
  }catch{}
}

function renderAddress(dir){
  const line = `${dir.calle || ''} ${dir.numero || ''}, ${dir.colonia || ''}, ${dir.cp || ''} ${dir.ciudad || ''} ${dir.estado || ''}`.replace(/\s+/g,' ').trim();
  const alias = dir.alias ? ` (${dir.alias})` : '';
  return `${line}${alias}`;
}

/** Si el usuario escribe “cambiar dirección”, “otra sucursal”, etc. */
function wantsChangeAddress(text){
  const t = normalizeBase(text);
  return /\b(cambiar|otra|nueva)\s+(direccion|direcci[oó]n|sucursal)\b/.test(t) || /\b(usa|usar)\s+otra\b/.test(t);
}

/* ========================== Flujo principal soporte ======================== */

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    const beforeStage = session.stage;
    const beforeLock = session?.data?.intent_lock;

    // 🔒 Mantener lock de soporte
    session.data.intent_lock = 'support';
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    const wasCollecting = session.stage === 'sv_collect';
    const prevNeeded = session.data.sv_need_next || null;

    // 1) Mapear respuesta previa ANTES de recalcular faltantes
    if (wasCollecting && prevNeeded) {
      svFillFromAnswer(sv, prevNeeded, text);
    }

    // 2) Completar con extractores (sin pisar válidos)
    const extra = extractSvInfo(text);
    for (const k of Object.keys(extra)) { if (!truthy(sv[k])) sv[k] = extra[k]; }

    // 3) Parse de fecha/hora si viene natural
    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    // 4) Bienvenida una vez, tono humano
    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `Gracias por escribirnos. Lamento la falla 😕. Cuéntame la *marca y modelo* del equipo y una breve *descripción* del problema. Voy ayudándote y, si gustas, agendamos visita.`);
    }

    // 5) Consejos rápidos (si aplica) y propuesta suave de visita
    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `${quick}\n\n¿Te parece si *agendamos una visita* para revisarlo en sitio? (Responde *sí* o *no*)`);
      // No cerramos todavía; seguimos a recolectar si faltan datos
    }

    // 6) Cargar datos cliente si existen (no pedir de nuevo)
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;

    // 7) Direcciones existentes (múltiples)
    let clienteRow = await getClienteByPhone(env, session.from);
    let addrList = [];
    if (clienteRow?.id) {
      addrList = await listCustomerAddresses(env, clienteRow.id);
      // Si no hay direcciones en tabla, pero cliente tiene campos sueltos, proponer guardarla
      if (!addrList.length && (c.calle || sv.calle)) {
        const dirProto = {
          alias: 'Principal',
          calle: sv.calle || c.calle,
          numero: sv.numero || c.numero,
          colonia: sv.colonia || c.colonia,
          ciudad: sv.ciudad || c.ciudad,
          estado: sv.estado || c.estado,
          cp: sv.cp || c.cp,
          is_default: true
        };
        const ins = await upsertAddress(env, clienteRow.id, dirProto);
        if (ins) addrList = [ins];
      }
    }

    // 8) Calcular faltantes mínimos para agendar
    const needed = [];
    if (!(truthy(sv.marca) && truthy(sv.modelo))) needed.push('modelo');
    if (!truthy(sv.falla)) needed.push('falla');

    // Dirección: si hay lista de direcciones, no exigimos capturar calle/numero/colonia/cp de nuevo,
    // solo confirmación más adelante. Si no hay lista ni datos en sv, pedimos.
    const hasAnyAddress = addrList.length > 0 || (truthy(sv.calle) && truthy(sv.numero) && truthy(sv.colonia) && truthy(sv.cp));
    if (!hasAnyAddress) {
      if (!truthy(sv.calle)) needed.push('calle');
      if (!truthy(sv.numero)) needed.push('numero');
      if (!truthy(sv.colonia)) needed.push('colonia');
      if (!truthy(sv.cp)) needed.push('cp');
    }

    if (!sv.when?.start) needed.push('horario');
    if (!truthy(sv.nombre)) needed.push('nombre');
    if (!truthy(sv.email)) needed.push('email');

    // 9) Detección de intención “sí/no” para visita (fluido, sin botones)
    const y = /\b(s[ií]|sí|si|ok|va|dale|de acuerdo|hagamos|agenda|agendar|visita|que venga)\b/i.test(lowered);
    const n = !y && /\b(no|luego|despu[eé]s|ahorita no)\b/i.test(lowered);

    // Si usuario quiere cambiar dirección, habilitar selección/captura
    const wantsChange = wantsChangeAddress(lowered);

    // 10) Si faltan datos, recolecta progresiva
    if (needed.length || wantsChange) {
      session.stage = 'sv_collect';
      // Si “cambiar dirección” y hay direcciones previas, pedir texto de la nueva o alias
      if (wantsChange) {
        session.data.sv_need_next = 'calle';
        await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
        await sendWhatsAppText(env, toE164, `De acuerdo, usemos *otra dirección*. Por favor dime:\n1) *Calle*\n2) *Número*\n3) *Colonia*\n4) *CP*\n(En un solo mensaje o uno por uno.)`);
        return ok('EVENT_RECEIVED');
      }

      session.data.sv_need_next = needed[0];
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);

      // Pregunta segura + ACK de marca/modelo si ya los tenemos
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
      await sendWhatsAppText(env, toE164, pre + (Q[needed[0]] || '¿Me ayudas con ese dato, por favor?'));

      // Si el usuario ya había dicho “sí” a agendar, mantenemos ese contexto
      if (y && !session.data.sv_wants_visit) session.data.sv_wants_visit = true;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      return ok('EVENT_RECEIVED');
    }

    // 11) Llegados aquí: tenemos TODO para agendar. Confirmemos dirección antes.
    // Elegir dirección efectiva:
    let effectiveDir = null;

    if (addrList.length > 0) {
      // Toma default o la primera
      effectiveDir = addrList.find(d => d.is_default) || addrList[0];
      // Mostrar y pedir confirmación (fluido)
      await sendWhatsAppText(env, toE164,
        `Confirmemos los datos para la visita:\n` +
        `• Equipo: *${sv.marca} ${sv.modelo}*\n` +
        `• Falla: *${sv.falla}*\n` +
        `• Dirección: *${renderAddress(effectiveDir)}*\n` +
        `• Contacto: *${sv.nombre}* / ${sv.email}\n\n` +
        `¿Los datos son *correctos*? (Responde *sí* o escribe “cambiar dirección” para usar otra/sucursal.)`
      );
      // Esperar confirmación del usuario
      session.stage = 'sv_confirm';
      session.data.sv_effective_dir_id = effectiveDir.id;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      return ok('EVENT_RECEIVED');
    } else {
      // No hay tabla de direcciones pero sí campos en sv (los pidió arriba)
      effectiveDir = {
        alias: 'Principal',
        calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp
      };
      await sendWhatsAppText(env, toE164,
        `Confirmemos:\n` +
        `• Equipo: *${sv.marca} ${sv.modelo}*\n` +
        `• Falla: *${sv.falla}*\n` +
        `• Dirección: *${renderAddress(effectiveDir)}*\n` +
        `• Contacto: *${sv.nombre}* / ${sv.email}\n\n` +
        `¿Los datos son *correctos*? (sí / cambiar dirección)`
      );
      session.stage = 'sv_confirm';
      session.data.sv_effective_dir_inline = effectiveDir; // para usar si no hay en tabla
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      return ok('EVENT_RECEIVED');
    }

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

/* =================== Confirmación final y agendado ======================== */
/** Llamar cuando estamos en stage 'sv_confirm' y el usuario responde. */
async function handleSupportConfirm(env, session, toE164, lowered){
  // “cambiar dirección” en confirm
  if (wantsChangeAddress(lowered)) {
    session.stage = 'sv_collect';
    session.data.sv_need_next = 'calle';
    await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    await sendWhatsAppText(env, toE164, `Perfecto, vamos a usar *otra dirección*. Dime:\nCalle, Número, Colonia y CP.`);
    return ok('EVENT_RECEIVED');
  }

  const y = /\b(s[ií]|sí|si|ok|correcto|as[ií] es|est[aá] bien|confirmo)\b/i.test(lowered);
  const n = !y && /\b(no|incorrecto|cambiar)\b/i.test(lowered);

  if (!y && !n) {
    await sendWhatsAppText(env, toE164, `¿Confirmas que los datos son *correctos*? (sí / cambiar dirección)`);
    return ok('EVENT_RECEIVED');
  }

  if (n) {
    session.stage = 'sv_collect';
    session.data.sv_need_next = 'calle';
    await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    await sendWhatsAppText(env, toE164, `De acuerdo, actualicemos. Dime la *calle*, *número*, *colonia* y *CP* de la sucursal.`);
    return ok('EVENT_RECEIVED');
  }

  // y === true ⇒ agendar
  const sv = session.data.sv || {};
  const tz = env.TZ || 'America/Mexico_City';
  const chosen = clampToWindow(sv.when, tz);

  const cliente_id = await upsertClienteByPhone(env, session.from);
  try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email }); } catch {}

  // Dirección efectiva: si venía ID, úsala, si venía inline, crearla y marcar default
  let effectiveDir = null;
  if (session.data.sv_effective_dir_id) {
    const list = await listCustomerAddresses(env, cliente_id);
    effectiveDir = list.find(d => d.id === session.data.sv_effective_dir_id) || list[0] || null;
  } else if (session.data.sv_effective_dir_inline) {
    const ins = await upsertAddress(env, cliente_id, { ...session.data.sv_effective_dir_inline, is_default: true });
    if (ins) { await markDefaultAddress(env, cliente_id, ins.id); effectiveDir = ins; }
  }

  // Intento crear calendar si hay credenciales
  let pool = [];
  try { pool = await getCalendarPool(env) || []; } catch(e){ console.warn('[GCal] pool', e); }
  const cal = pickCalendarFromPool(pool);

  let slot = chosen, event = null, calName = '';
  if (cal && env.GCAL_REFRESH_TOKEN && env.GCAL_CLIENT_ID && env.GCAL_CLIENT_SECRET) {
    try {
      slot = await findNearestFreeSlot(env, cal.gcal_id, chosen, tz);
      event = await gcalCreateEvent(env, cal.gcal_id, {
        summary: `Visita técnica: ${(sv.marca || '')} ${(sv.modelo || '')}`.trim(),
        description: renderOsDescription(session.from, { ...sv, ...effectiveDir }),
        start: slot.start, end: slot.end, timezone: tz,
      });
      calName = cal.name || '';
    } catch (e) { console.warn('[GCal] create error', e); }
  }

  // Crear OS
  let osId = null; let estado = event ? 'agendado' : 'pendiente';
  try {
    const osBody = [{
      cliente_id,
      marca: sv.marca || null, modelo: sv.modelo || null, falla_descripcion: sv.falla || null,
      prioridad: sv.prioridad || 'media', estado,
      ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(),
      gcal_event_id: event?.id || null, calendar_id: cal?.gcal_id || null,
      calle: effectiveDir?.calle || sv.calle || null,
      numero: effectiveDir?.numero || sv.numero || null,
      colonia: effectiveDir?.colonia || sv.colonia || null,
      ciudad: effectiveDir?.ciudad || sv.ciudad || null,
      estado: effectiveDir?.estado || sv.estado || null,
      cp: effectiveDir?.cp || sv.cp || null,
      created_at: new Date().toISOString()
    }];
    const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
    osId = os?.data?.[0]?.id || null;
  } catch (e) { console.warn('[Supabase] OS upsert', e); estado = 'pendiente'; }

  if (event) {
    await sendWhatsAppText(
      env, toE164,
      `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\n` +
      `Dirección: ${renderAddress(effectiveDir || {})}\nTécnico asignado: ${calName || 'por confirmar'}.\n\n` +
      `Si necesitas reprogramar o cancelar, dímelo con confianza.`
    );
    session.stage = 'sv_scheduled';
  } else {
    await sendWhatsAppText(env, toE164, `Tengo tus datos ✍️. En breve te confirmo el horario exacto por este medio.`);
    await notifySupport(env, `OS *pendiente/agendar* para ${toE164}\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla}\n` +
      `Dir: ${renderAddress(effectiveDir || {})}\nNombre: ${sv.nombre} | Email: ${sv.email}`);
    session.stage = 'sv_scheduled';
  }

  session.data.sv.os_id = osId;
  session.data.sv.gcal_event_id = event?.id || null;
  session.data.intent_lock = null; // liberar al final

  await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
  return ok('EVENT_RECEIVED');
}

/* ================= Acciones rápidas: cancelar / mover / consultar ========= */

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
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita programada. ¿Agendamos una?`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164, `Tu próxima visita: *${fmtDate(os.ventana_inicio, tz)}*, de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*. Estado: ${os.estado}.`);
}

/* ====================== Utilidades OS / Calendario ======================== */

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

function renderOsDescription(phone, sv) {
  return [
    `Cliente: +${phone} (${sv.nombre || 'N/D'} / ${sv.email || 'sin email'})`,
    `Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
    `Falla: ${sv.falla || 'N/D'}${sv.error_code ? ' (Error ' + sv.error_code + ')' : ''}`,
    `Prioridad: ${sv.prioridad || 'media'}`,
    `Dirección: ${sv.calle || ''} ${sv.numero || ''}, ${sv.colonia || ''}, ${sv.ciudad || ''}, ${sv.estado || ''}, CP ${sv.cp || ''}`
  ].join('\n');
}

/* ========================================================================== */
/* ======================= Google Calendar / Calendario ===================== */
/* ========================================================================== */

/** Intercambia refresh_token por access_token para GCal. */
async function gcalToken(env) {
  if (!env.GCAL_CLIENT_ID || !env.GCAL_CLIENT_SECRET || !env.GCAL_REFRESH_TOKEN) return null;
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: env.GCAL_CLIENT_ID,
      client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    })
  });
  if (!r.ok) { console.warn('gcal token', await r.text()); return null; }
  const j = await r.json(); return j.access_token;
}

/** Crea evento en Google Calendar. */
async function gcalCreateEvent(env, calendarId, { summary, description, start, end, timezone }) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  const body = {
    summary,
    description,
    start: { dateTime: start, timeZone: timezone },
    end: { dateTime: end, timeZone: timezone }
  };
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) { console.warn('gcal create', await r.text()); return null; }
  return await r.json();
}

/** Edita (PATCH) un evento en Google Calendar. */
async function gcalPatchEvent(env, calendarId, eventId, patch) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(patch)
  });
  if (!r.ok) { console.warn('gcal patch', await r.text()); return null; }
  return await r.json();
}

/** Elimina evento en Google Calendar. */
async function gcalDeleteEvent(env, calendarId, eventId) {
  const token = await gcalToken(env); if (!token) return null;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await fetch(url, { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) console.warn('gcal delete', await r.text());
}

/** Consulta si hay choque de horario. */
async function isBusy(env, calendarId, startISO, endISO) {
  const token = await gcalToken(env); if (!token) return false;
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events?timeMin=${encodeURIComponent(startISO)}&timeMax=${encodeURIComponent(endISO)}&singleEvents=true&orderBy=startTime`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) { console.warn('gcal list', await r.text()); return false; }
  const j = await r.json();
  return Array.isArray(j.items) && j.items.length > 0;
}

/** Busca el siguiente slot libre cercano, en saltos de 30 min y ventana 10–15h. */
async function findNearestFreeSlot(env, calendarId, when, tz) {
  if (!calendarId) return when;
  let curStart = new Date(when.start);
  let curEnd = new Date(when.end);
  for (let i=0;i<6;i++) { // 6 intentos de 30 min
    const busy = await isBusy(env, calendarId, curStart.toISOString(), curEnd.toISOString());
    if (!busy) break;
    curStart = new Date(curStart.getTime()+30*60*1000);
    curEnd = new Date(curStart.getTime()+30*60*1000);
    const localH = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone: tz }).format(curStart));
    if (localH >= 15) {
      // Salta a siguiente día a las 10:00
      const local = new Date(new Date().toLocaleString('en-US', { timeZone: tz }));
      // Ajusta curStart al TZ “tz” manteniendo ISO
      const next = new Date(curStart);
      next.setDate(next.getDate()+1);
      next.setHours(10,0,0,0);
      curStart = next;
      curEnd = new Date(curStart.getTime()+60*60*1000);
    }
  }
  return { start: curStart.toISOString(), end: curEnd.toISOString() };
}

/* ============================= Pool de calendarios ======================== */

/** Lista de técnicos/calendarios activos desde Supabase. */
async function getCalendarPool(env) {
  try {
    const r = await sbGet(env, 'calendar_pool', { query: 'select=gcal_id,name,active&active=is.true' });
    return Array.isArray(r) ? r : [];
  } catch (e) {
    console.warn('[GCal] getCalendarPool', e);
    return [];
  }
}

/** Estrategia simple: el primero activo (puedes rotar en el futuro). */
function pickCalendarFromPool(pool) {
  return pool?.[0] || null;
}

/* ========================================================================== */
/* ============================== FAQs rápidas ============================== */
/* ========================================================================== */

/**
 * Respuestas rápidas de “preguntas frecuentes”.
 * Primero intenta buscar en la tabla company_info (contenido administrable),
 * si no, cae en plantillas locales por intent.
 */
async function maybeFAQ(env, ntext) {
  try {
    const like = encodeURIComponent(`%${(ntext || '').slice(0, 80)}%`);
    const r = await sbGet(env, 'company_info', {
      query: `select=key,content,tags&or=(key.ilike.${like},content.ilike.${like},tags.ilike.${like})&limit=1`
    });
    if (r && r[0]?.content) return r[0].content;
  } catch (e) {
    console.warn('[FAQ] company_info lookup', e);
  }

  // Plantillas fallback por patrones comunes
  if (/\b(qu[ié]nes?\s+son|sobre\s+ustedes|qu[eé]\s+es\s+cp(\s+digital)?|h[aá]blame\s+de\s+ustedes)\b/i.test(ntext)) {
    return '¡Hola! Somos *CP Digital*. Ayudamos a empresas con consumibles y refacciones para impresoras Xerox y Fujifilm, y brindamos visitas de soporte técnico. Cotizamos, vendemos con o sin factura y agendamos servicio en tu horario 🙂';
  }
  if (/\b(horario|horarios|a\s+qu[eé]\s+hora)\b/i.test(ntext)) {
    return 'Horario de visitas: *10:00–15:00* (lun–vie). Entregas y atención por WhatsApp todo el día.';
  }
  if (/\b(d[oó]nde\s+est[aá]n|ubicaci[oó]n|direcci[oó]n)\b/i.test(ntext)) {
    return 'Tenemos presencia en Guanajuato (León y Celaya) y coordinamos entregas/servicios a nivel nacional.';
  }
  if (/\b(contacto|whats(app)?|tel[eé]fono|llamar|correo|email)\b/i.test(ntext)) {
    return `Puedes escribirnos por aquí o al WhatsApp de soporte: ${env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164 || 'disponible en tu ficha de cliente'}.`;
  }
  return null;
}

/* ========================================================================== */
/* ============================== Cron sencillo ============================= */
/* ========================================================================== */

/**
 * Punto de extensión para recordatorios/notificaciones.
 * Actualmente no hace nada costoso para evitar tiempos de ejecución.
 * Puedes: recordar citas del mismo día, pedidos pendientes, etc.
 */
async function cronReminders(env){
  // Ejemplo de hook futuro:
  // - Buscar ordenes_servicio con ventana_inicio hoy y notificar técnico/cliente.
  // - Recordar pedidos con estado “nuevo” > 48h.
  return { ok: true, ts: new Date().toISOString() };
}






