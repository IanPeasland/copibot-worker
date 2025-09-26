/**
 * CopiBot – IA + Ventas + Soporte + GCal — Build R6.4 (2025-09)
 * Cambios R6.4:
 *  - Persistencia de stage robusta en Supabase (sin perder ask_qty/cart).
 *  - Seguro de carrito: si llega “quiero X” en idle con last_candidate, reanuda ask_qty.
 *  - Micro-guardas: si llega número suelto y hay candidato, asume cantidad.
 *  - Logs claros de entrada y guardado de sesión.
 */

export default {
  async fetch(req, env, ctx) {
    try {
      const url = new URL(req.url);

      // --- Webhook verify (GET /) ---
      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      // --- Healthcheck (GET /health) ---
      if (req.method === 'GET' && url.pathname === '/health') {
        return new Response(JSON.stringify({
          ok: true,
          now: new Date().toISOString(),
          WA: !!env.WA_TOKEN,
          SUPABASE: !!env.SUPABASE_URL,
          OPENAI: !!env.OPENAI_API_KEY,
        }), { status: 200 });
      }

      // --- Cron (POST /cron) ---
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        // TODO: cronReminders(env)
        return new Response('cron ok', { status: 200 });
      }

      // --- WhatsApp Webhook (POST /) ---
      if (req.method === 'POST' && url.pathname === '/') {
        return await handleWebhook(req, env);
      }

      return new Response('Not found', { status: 404 });
    } catch (err) {
      console.error('Top-level error', err);
      return new Response('Internal Error', { status: 500 });
    }
  }
};

/* ========================================================================== */
/* =============================== Helpers ================================= */
/* ========================================================================== */

function ok(s = 'ok') {
  return new Response(s, { status: 200 });
}

async function safeJson(req) {
  try {
    return await req.json();
  } catch (e) {
    return null;
  }
}

/* ------------------ Supabase REST helpers ------------------ */
async function sbGet(env, table, opts = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${opts.query||''}`;
  const r = await fetch(url, { headers: { apikey: env.SUPABASE_ANON_KEY }});
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function sbUpsert(env, table, rows, { onConflict, returning }={}) {
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/${table}`);
  if (onConflict) url.searchParams.set('on_conflict', onConflict);
  if (returning) url.searchParams.set('returning', returning);
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      'Content-Type': 'application/json',
      Prefer: 'resolution=merge-duplicates'
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

/* ------------------ Sesión en wa_session ------------------ */

async function loadSession(env, phone) {
  let s = null;
  try {
    const r = await sbGet(env, 'wa_session', {
      query: `phone=eq.${phone}&limit=1&select=session_data,last_updated`
    });
    if (r && r[0]?.session_data) {
      s = r[0].session_data;
      s.last_updated = r[0].last_updated;
    }
  } catch (e) { console.warn('loadSession', e); }

  s = s || { from: phone, stage:'idle', data:{} };
  s.from = s.from || phone;
  s.stage = s.stage || 'idle';
  s.data  = s.data || {};
  return s;
}

async function saveSession(env, session, now = new Date()) {
  const sessionData = {
    from: session.from,
    stage: session.stage || 'idle',
    data: session.data || {},
    last_updated: now.toISOString()
  };

  try {
    await sbUpsert(env, 'wa_session', [{
      phone: session.from,
      session_data: sessionData,
      last_updated: sessionData.last_updated
    }], { onConflict: 'phone', returning: 'minimal' });
    console.log('SESSION SAVE', { phone: session.from, stage: session.stage });
  } catch (e) { console.warn('saveSession', e); }
}

/* ========================================================================== */
/* ========================= WhatsApp I/O & IA ============================== */
/* ========================================================================== */

/** Extrae el contexto útil del payload de WhatsApp */
function extractWhatsAppContext(payload) {
  try {
    const entry = payload?.entry?.[0];
    const changes = entry?.changes?.[0];
    const value = changes?.value;
    const message = value?.messages?.[0];
    if (!message) return null;

    const from = message.from;                 // 521xxx...
    const fromE164 = `+${from}`;
    const mid = message.id;
    const profileName = value?.contacts?.[0]?.profile?.name || '';

    let textRaw = '';
    let msgType = 'unknown';
    if (message.text) {
      textRaw = message.text.body || '';
      msgType = 'text';
    } else if (message.interactive) {
      const kind = message.interactive.type;
      msgType = `interactive_${kind}`;
      if (kind === 'button_reply') textRaw = message.interactive.button_reply?.title || '';
      if (kind === 'list_reply')   textRaw = message.interactive.list_reply?.title || '';
    }

    return { from, fromE164, mid, profileName, textRaw, msgType };
  } catch (e) {
    console.warn('extractWhatsAppContext error', e);
    return null;
  }
}

/** Envío de texto hacia WhatsApp (Graph API) */
async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) { console.warn('WA env missing'); return; }
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to: toE164.replace(/\D/g, ''),
    text: { body }
  };
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${env.WA_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  if (!r.ok) console.warn('sendWhatsAppText', r.status, await r.text());
}

async function notifySupport(env, body) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  await sendWhatsAppText(env, to, `🛎️ *Soporte*\n${body}`);
}

/* --------------------------- IA helpers --------------------------- */

async function aiCall(env, messages, { json=false } = {}) {
  const OPENAI_KEY = env.OPENAI_API_KEY || env.OPENAI_KEY;
  const MODEL = env.LLM_MODEL || env.OPENAI_NLU_MODEL || env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  if (!OPENAI_KEY) return null;

  const body = {
    model: MODEL,
    messages,
    temperature: json ? 0 : 0.3,
    ...(json ? { response_format: { type: 'json_object' } } : {})
  };

  const r = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${OPENAI_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) { console.warn('aiCall', r.status, await r.text()); return null; }

  const j = await r.json();
  return j?.choices?.[0]?.message?.content || '';
}

async function aiSmallTalk(env, session, mode='general', userText='') {
  const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde con calidez humana, breve y claro. Máx. 1 emoji. Evita listas salvo necesidad.`;
  const prompt = mode==='fallback'
    ? `Usuario: """${userText}""". Responde breve, útil y amable. Si no hay contexto, ofrece inventario o soporte.`
    : `Usuario: """${userText}""". Responde breve y amable.`;
  const out = await aiCall(env, [{ role:'system', content: sys }, { role:'user', content: prompt }], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 👋 ¿En qué te puedo ayudar?`);
}

/** Clasificador ligero de intención (opcional) */
async function aiClassifyIntent(env, text) {
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Clasifica el texto (es-MX) en JSON: {"intent":"support|sales|faq|smalltalk"}`;
  const out = await aiCall(env, [{ role:'system', content: sys }, { role:'user', content: text }], { json:true });
  try { return JSON.parse(out || '{}'); } catch { return null; }
}

async function intentIs(env, text, expected) {
  try {
    const res = await aiClassifyIntent(env, text);
    return res?.intent === expected;
  } catch { return false; }
}

/* ========================================================================== */
/* ===================== Normalización & patrones =========================== */
/* ========================================================================== */

const RX_GREET  = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_Q  = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro|yellow|black|bk|k)\b/i;

const RX_NEG_NO = /\b(no|nel|ahorita no)\b/i;
const RX_DONE   = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;
const RX_YES    = /\b(s[ií]|sí|si|claro|va|dale|sale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo|de acuerdo|me sirve)\b/i;

function isContinueish(t){ return /\b(continuar|continuemos|seguir|retomar|reanudar|continuo|contin[uú]o)\b/i.test(t); }
function isStartNewish(t){ return /\b(empezar|nuevo|desde cero|otra cosa|otro|iniciar|empecemos)\b/i.test(t); }
function isYesish(t){ return RX_YES.test(t); }
function isNoish(t){ return RX_NEG_NO.test(t) || RX_DONE.test(t); }

function firstWord(s=''){ return (s||'').trim().split(/\s+/)[0] || ''; }
function toTitleCase(s=''){ return s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : ''; }
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }

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
  for (const [bad, good] of aliases) {
    out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  }
  return out;
}

/* ========================================================================== */
/* ============================ Webhook handler ============================= */
/* ========================================================================== */

async function handleWebhook(req, env) {
  try {
    const body = await safeJson(req);
    const ctx = extractWhatsAppContext(body);
    if (!ctx) return ok('EVENT_RECEIVED');

    const { mid, from, fromE164, profileName, textRaw, msgType } = ctx;
    const text = (textRaw || '').trim();
    const lowered = text.toLowerCase();
    const ntext = normalizeWithAliases(text);

    // --- Cargar sesión (Supabase) ---
    let session = await loadSession(env, from);
    session.from = from;
    session.data = session.data || {};
    session.stage = session.stage || 'idle';

    // Nombre para trato humano (si no está)
    if (profileName && !session?.data?.customer?.nombre) {
      session.data.customer = session.data.customer || {};
      session.data.customer.nombre = toTitleCase(firstWord(profileName));
    }

    console.log('WA IN', { fromE164, mid, stage_in: session.stage, text: textRaw });

    // --- Idempotencia por MID ---
    if (session?.data?.last_mid && session.data.last_mid === mid) {
      return ok('EVENT_RECEIVED');
    }
    session.data.last_mid = mid;

    // --- Mensaje no-texto: pedir texto ---
    if (msgType !== 'text') {
      await sendWhatsAppText(env, fromE164, '¿Podrías describirme con palabras lo que necesitas? Así te ayudo más rápido 🙂');
      await saveSession(env, session, new Date());
      return ok('EVENT_RECEIVED');
    }

    // --- Intenciones (heurística + IA opcional) ---
    const supportIntent = isSupportIntent(ntext) || (await intentIs(env, text, 'support'));
    const salesIntent   = RX_INV_Q.test(ntext)   || (await intentIs(env, text, 'sales'));
    const isGreet       = RX_GREET.test(lowered);

    // =========================================================================
    // 1) PRIORIDAD: stages de VENTAS activos ANTES que cualquier saludo
    // =========================================================================
    if (session.stage === 'ask_qty')       return await handleAskQty(env, session, fromE164, text, lowered, ntext, new Date());
    if (session.stage === 'cart_open')     return await handleCartOpen(env, session, fromE164, text, lowered, ntext, new Date());
    if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, new Date(), text);
    if (session.stage && session.stage.startsWith('collect_')) {
      return await handleCollectSequential(env, session, fromE164, text, new Date());
    }

    // =========================================================================
    // 2) Seguro de carrito: si se “perdió” el stage pero hay last_candidate
    //    - Frases tipo “quiero 2”, o número suelto, reencauzan a ask_qty.
    // =========================================================================
    if (session.stage === 'idle' && session?.data?.last_candidate) {
      const looksQty = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+\d+\b/i.test(lowered);
      const onlyNumber = /^\s*\d+\s*$/.test(lowered);
      if (looksQty || onlyNumber) {
        session.stage = 'ask_qty';
        await saveSession(env, session, new Date());
        return await handleAskQty(env, session, fromE164, text, lowered, ntext, new Date());
      }
    }

    // =========================================================================
    // 3) Comandos universales de SOPORTE (cancelar/reprogramar/cuándo)
    // =========================================================================
    if (/\b(cancel(a|ar).*(cita|visita|servicio))\b/i.test(lowered)) {
      await svCancel(env, session, fromE164);
      await saveSession(env, session, new Date());
      return ok('EVENT_RECEIVED');
    }
    if (/\b(reprogram|mueve|cambia|modif)\w*/i.test(lowered)) {
      const when = parseNaturalDateTime(lowered, env);
      if (when?.start) {
        await svReschedule(env, session, fromE164, when);
        await saveSession(env, session, new Date());
        return ok('EVENT_RECEIVED');
      }
    }
    if (/\b(cu[aá]ndo|cuando).*(cita|visita|servicio)\b/i.test(lowered)) {
      await svWhenIsMyVisit(env, session, fromE164);
      await saveSession(env, session, new Date());
      return ok('EVENT_RECEIVED');
    }

    // =========================================================================
    // 4) Saludo genuino (sin romper stages previos)
    // =========================================================================
    if (isGreet) {
      const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
      await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
      session.data.last_greet_at = new Date().toISOString();

      if (session.stage !== 'idle') {
        session.data.last_stage = session.stage;
        session.stage = 'await_choice';
        await saveSession(env, session, new Date());
        return ok('EVENT_RECEIVED');
      }
      await saveSession(env, session, new Date());
      return ok('EVENT_RECEIVED');
    }

    // =========================================================================
    // 5) Menú continuar / nuevo si estamos en await_choice
    // =========================================================================
    if (session.stage === 'await_choice') {
      if (supportIntent) {
        session.stage = 'sv_collect';
        await saveSession(env, session, new Date());
        return await handleSupport(env, session, fromE164, text, lowered, ntext, new Date(), { intent: 'support', forceWelcome: true });
      }
      if (salesIntent) {
        session.data.last_stage = 'idle';
        session.stage = 'idle';
        await saveSession(env, session, new Date());
        return await startSalesFromQuery(env, session, fromE164, text, ntext, new Date());
      }
      if (isContinueish(lowered)) {
        session.stage = session?.data?.last_stage || 'idle';
        await saveSession(env, session, new Date());
        const prompt = buildResumePrompt(session);
        await sendWhatsAppText(env, fromE164, `Va. ${prompt}`);
        return ok('EVENT_RECEIVED');
      }
      if (isStartNewish(lowered)) {
        session.data.last_stage = 'idle';
        session.stage = 'idle';
        await saveSession(env, session, new Date());
        await sendWhatsAppText(env, fromE164, 'Perfecto, cuéntame qué necesitas (soporte, cotización, etc.). 🙂');
        return ok('EVENT_RECEIVED');
      }
      await sendWhatsAppText(env, fromE164, '¿Prefieres continuar con lo pendiente o empezamos algo nuevo?');
      return ok('EVENT_RECEIVED');
    }

    // =========================================================================
    // 6) SOPORTE cuando aplica (intención o ya en sv_*)
    // =========================================================================
    if (supportIntent || session.stage?.startsWith('sv_')) {
      return await handleSupport(env, session, fromE164, text, lowered, ntext, new Date(), { intent: 'support' });
    }

    // =========================================================================
    // 7) VENTAS por intención
    // =========================================================================
    if (salesIntent) {
      if (session.stage !== 'idle') {
        await sendWhatsAppText(env, fromE164, 'Te ayudo con inventario. Dejo lo otro en pausa un momento.');
        session.data.last_stage = session.stage;
        session.stage = 'idle';
        await saveSession(env, session, new Date());
      }
      return await startSalesFromQuery(env, session, fromE164, text, ntext, new Date());
    }

    // =========================================================================
    // 8) FAQs rápidas
    // =========================================================================
    const faqAns = await maybeFAQ(env, ntext);
    if (faqAns) {
      await sendWhatsAppText(env, fromE164, faqAns);
      await saveSession(env, session, new Date());
      return ok('EVENT_RECEIVED');
    }

    // =========================================================================
    // 9) Fallback IA breve
    // =========================================================================
    const reply = await aiSmallTalk(env, session, 'fallback', text);
    await sendWhatsAppText(env, fromE164, reply);
    await saveSession(env, session, new Date());
    return ok('EVENT_RECEIVED');

  } catch (e) {
    console.error('handleWebhook error', e);
    // Notificación amable al usuario
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

/* ========================================================================== */
/* ========================== Intención de soporte ========================== */
/* ========================================================================== */

function isSupportIntent(ntext='') {
  const t = `${ntext}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|marca c[oó]digo|c[oó]digo)/.test(t);
  const hasDevice  = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase     = /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);
  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

/* ========================================================================== */
/* ======================= Prompt de reanudación simple ===================== */
/* ========================================================================== */

function buildResumePrompt(session){
  if (session.stage?.startsWith('sv_')) return 'seguimos con el soporte pendiente.';
  if (session.stage === 'await_invoice') return 'estábamos por cotizar con/sin factura.';
  if (session.stage === 'cart_open') return 'estábamos con tu carrito abierto.';
  return '¿en qué te ayudo?';
}

/* ========================================================================== */
/* =========================== Ventas & Carrito ============================= */
/* ========================================================================== */

/* --------- Utilidades numéricas y de formato --------- */
function numberOrZero(n){ const v = Number(n||0); return Number.isFinite(v)?v:0; }

function formatMoneyMXN(n){
  const v = Number(n||0);
  try { return new Intl.NumberFormat('es-MX',{ style:'currency', currency:'MXN', maximumFractionDigits:2 }).format(v); }
  catch { return `$${v.toFixed(2)}`; }
}
function priceWithIVA(n){ return `${formatMoneyMXN(Number(n||0))} + IVA`; }

/* --------- FAQ rápidas (keywords locales) --------- */
async function maybeFAQ(env, ntext) {
  const faqs = {
    'horario': 'Horario de atención: Lunes a Viernes 9:00–18:00, Sábados 9:00–14:00',
    'ubicacion': 'Estamos en Av. Tecnológico #123, Industrial, Monterrey, NL',
    'contacto': 'Tel: 81 1234 5678 | Email: ventas@cpdigital.com.mx',
    'empresa': 'CP Digital — Especialistas en impresión Xerox y Fujifilm'
  };
  const pats = {
    'horario':  /\b(horario|hora|atencion|abierto|cierra|abre)\b/i,
    'ubicacion':/\b(donde|ubicacion|direcci[oó]n|sucursal|local|tienda)\b/i,
    'contacto': /\b(contacto|telefono|tel|email|correo|whatsapp)\b/i,
    'empresa':  /\b(empresa|quienes|somos|compa[nñ]ia|negocio)\b/i
  };
  for (const [k, rx] of Object.entries(pats)) if (rx.test(ntext)) return faqs[k];
  return null;
}

/* --------- Regex & parsers de cantidad/artículos --------- */
const RX_WANT_QTY = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+(\d+)\b/i;
const RX_ADD_ITEM = /\b(agrega(?:me)?|añade|mete|pon|suma|incluye)\b/i;

function parseQty(text, fallback=1){
  const m = text.match(RX_WANT_QTY);
  const q = m ? Number(m[2]) : null;
  if (q && q>0) return q;
  const n = (text.match(/\b(\d+)\b/) || [null,null])[1];
  return n ? Number(n) : fallback;
}

/* --------- Estructuras de carrito --------- */
function pushCart(session, product, qty, backorder=false){
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
  if (take>0) pushCart(session, product, take, false);
  if (rest>0) pushCart(session, product, rest, true);
}

function renderProducto(p){
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s>0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

/* --------- Heurísticas de modelo/color --------- */
function extractColorWord(text=''){
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return 'yellow';
  if (/\bmagenta\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'black';
  return null;
}

function extractModelHints(text=''){
  const t = normalizeWithAliases(text);
  const out = {};
  if (/\bversant\b/i.test(t) || /\b(80|180|2100|280|4100)\b/i.test(t)) out.family = 'versant';
  else if (/\bdocu\s*color\b/i.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/i.test(t) || /\bprimelink\b/i.test(t)) out.family = 'primelink';
  else if (/\bversa\s*link\b/i.test(t) || /\bversalink\b/i.test(t)) out.family = 'versalink';
  else if (/\balta\s*link\b/i.test(t)  || /\baltalink\b/i.test(t))  out.family = 'altalink';
  else if (/\bapeos\b/i.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/i.test(t)) out.family = 'c70';
  const c = extractColorWord(t);
  if (c) out.color = c;
  return out;
}

function productHasColor(p, colorCode){
  if (!colorCode) return true;
  const s = `${normalizeBase([p?.nombre,p?.sku,p?.marca].join(' '))}`;
  const map = {
    yellow:[/\bamarillo\b/i,/\byellow\b/i,/(^|[\s\/\-_])y($|[\s\/\-_])/i,/(^|[\s\/\-_])ylw($|[\s\/\-_])/i],
    magenta:[/\bmagenta\b/i,/(^|[\s\/\-_])m($|[\s\/\-_])/i],
    cyan:[/\bcyan\b/i,/\bcian\b/i,/(^|[\s\/\-_])c($|[\s\/\-_])/i],
    black:[/\bnegro\b/i,/\bblack\b/i,/(^|[\s\/\-_])k($|[\s\/\-_])/i,/(^|[\s\/\-_])bk($|[\s\/\-_])/i],
  };
  const arr = map[colorCode] || [];
  return arr.some(rx => rx.test(p?.nombre)||rx.test(p?.sku)||rx.test(s));
}

function productMatchesFamily(p, family){
  if (!family) return true;
  const s = normalizeBase([p?.nombre,p?.sku,p?.marca,p?.compatible].join(' '));
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
  if (family==='c70')       return /\bc(60|70|75)\b/i.test(s) || s.includes('c60') || s.includes('c70') || s.includes('c75');
  if (family==='primelink') return /\bprime\s*link\b/i.test(s) || /\bprimelink\b/i.test(s);
  if (family==='versalink') return /\bversa\s*link\b/i.test(s) || /\bversalink\b/i.test(s);
  if (family==='altalink')  return /\balta\s*link\b/i.test(s)  || /\baltalink\b/i.test(s);
  if (family==='apeos')     return /\bapeos\b/i.test(s);
  return s.includes(family);
}

/* --------- Enriquecimiento IA de consulta --------- */
async function aiExtractTonerQuery(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Extrae de una consulta (es-MX) sobre tóners los campos JSON:
{ "familia":"versant|docucolor|primelink|versalink|altalink|apeos|c70|null",
  "color":"yellow|magenta|cyan|black|null",
  "subfamilia":"string|null",
  "cantidad":number|null }`;
  const out = await aiCall(env, [{role:'system',content:sys},{role:'user',content:text}], { json:true });
  try { return JSON.parse(out||'{}'); } catch { return null; }
}

function enrichQueryFromAI(q, ai){
  if (!ai) return q;
  let out = q;
  if (ai.familia && !new RegExp(`\\b${ai.familia}\\b`).test(out)) out += ` ${ai.familia}`;
  if (ai.color && !/\b(amarillo|magenta|cyan|cian|negro|black|bk|k|yellow)\b/i.test(out)) {
    const map = { yellow:'amarillo', magenta:'magenta', cyan:'cyan', black:'negro' };
    out += ` ${map[ai.color] || ai.color}`;
  }
  if (ai.subfamilia && !out.includes(ai.subfamilia)) out += ` ${ai.subfamilia}`;
  if (ai.cantidad && !/\b\d+\b/.test(out)) out += ` ${ai.cantidad}`;
  return out;
}

/* --------- Matching principal de inventario --------- */
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

  // 1) RPC similitud
  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 40 }) || [];
    const pick1 = scoreAndPick(res);
    if (debug) console.log('[INV] RPC matches:', res?.length || 0, 'pick:', pick1?.sku);
    if (pick1) return pick1;
  } catch (e) { if (debug) console.log('[INV] RPC error', e); }

  // 2) Vista por familia
  if (hints.family && !opts.ignoreFamily) {
    try {
      const likeFam = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${likeFam},sku.ilike.${likeFam},marca.ilike.${likeFam},compatible.ilike.${likeFam})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const pick2 = scoreAndPick(r);
      if (debug) console.log('[INV] Family scan:', hints.family, 'cands:', r?.length || 0, 'pick:', pick2?.sku);
      if (pick2) return pick2;
    } catch (e) { if (debug) console.log('[INV] family scan error', e); }
  }

  // 3) Broad scan "toner"
  try {
    const likeToner = encodeURIComponent(`%toner%`);
    const r2 = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${likeToner},sku.ilike.${likeToner})&order=stock.desc.nullslast,precio.asc&limit=400`
    }) || [];
    const pick3 = scoreAndPick(r2);
    if (debug) console.log('[INV] Broad scan (toner) cands:', r2?.length || 0, 'pick:', pick3?.sku);
    if (pick3) return pick3;
  } catch (e) { if (debug) console.log('[INV] broad scan error', e); }

  return null;
}

/* --------- Entrada de ventas desde texto libre --------- */
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
    await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
    return ok('EVENT_RECEIVED');
  }

  if (!best && hints.family) {
    session.data.last_candidate = {
      id: null, sku: null,
      nombre: `Tóner ${hints.family.toUpperCase()}${hints.color?` ${hints.color}`:''} (sobre pedido)`,
      marca: 'Xerox',
      precio: 0, stock: 0
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

/* --------- Añadir cantidad del candidato y abrir carrito --------- */
async function handleAskQty(env, session, toE164, text, lowered, ntext, now){
  const cand = session.data?.last_candidate;
  if (!cand) {
    session.stage = 'cart_open';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, 'No alcancé a ver el artículo. ¿Lo repetimos o buscas otro? 🙂');
    return ok('EVENT_RECEIVED');
  }

  const qty = parseQty(lowered, 1);
  addWithStockSplit(session, cand, qty);
  session.stage = 'cart_open';
  await saveSession(env, session, now);

  const s = numberOrZero(cand.stock);
  const bo = Math.max(0, qty - Math.min(s, qty));
  const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} sobre pedido)` : '';
  await sendWhatsAppText(env, toE164, `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${nota}\n\n¿Deseas agregar algo más o *finalizamos*?`);
  return ok('EVENT_RECEIVED');
}

/* --------- Operaciones con el carrito abierto --------- */
async function handleCartOpen(env, session, toE164, text, lowered, ntext, now){
  session.data = session.data || {};
  const cart = session.data.cart || [];

  // Finalizar = pasar a factura/sin factura
  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length>0)) {
    if (!cart.length && session.data.last_candidate) addWithStockSplit(session, session.data.last_candidate, 1);
    session.stage = 'await_invoice';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Perfecto 🙌 ¿La cotizamos *con factura* o *sin factura*?`);
    return ok('EVENT_RECEIVED');
  }

  // Micro-guarda: número suelto ⇒ pedir piezas del candidato
  if (/^\s*\d+\s*$/.test(lowered) && session.data?.last_candidate){
    session.stage = 'ask_qty';
    await saveSession(env, session, now);
    const c = session.data.last_candidate;
    const s = numberOrZero(c?.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto iría *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  // Confirmaciones como “ok / agrégalo”
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

  // Petición de agregar otro artículo o nueva consulta de inventario
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
      await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
      return ok('EVENT_RECEIVED');
    } else {
      const hints = extractModelHints(enrichedQ);
      if (hints.family) {
        session.data.last_candidate = {
          id: null, sku: null,
          nombre: `Tóner ${hints.family.toUpperCase()}${hints.color?` ${hints.color}`:''} (sobre pedido)`,
          marca: 'Xerox',
          precio: 0, stock: 0
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

  // Keep-alive conversacional
  await sendWhatsAppText(env, toE164, `Te leo 🙂. Puedo agregar el artículo visto, buscar otro o *finalizar* si ya está completo.`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* --------- Factura o sin factura (paso siguiente) --------- */
/** Nota: esta función depende de utilidades de cliente/pedido que vienen en el Bloque 5 */
async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText=''){
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
    await preloadCustomerIfAny(env, session); // Bloque 5
    const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP; // Bloque 5
    const need = firstMissing(list, session.data.customer);            // Bloque 5
    if (need) {
      session.stage = `collect_${need}`;
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `¿${LABEL[need]}?`);         // Bloque 5
      return ok('EVENT_RECEIVED');
    }

    // Crear pedido directo si ya tenemos todos los datos
    const res = await createOrderFromSession(env, session, toE164);    // Bloque 5
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

/* --------- Anti-spam de recordatorio --------- */
function promptedRecently(session, key, ms){
  session.data = session.data || {};
  const k = `prompted_${key}_at`;
  const last = session.data[k] ? new Date(session.data[k]).getTime() : 0;
  const now = Date.now();
  if (now - last < ms) return true;
  session.data[k] = new Date().toISOString();
  return false;
}

/* ========================================================================== */
/* =========================== Cliente & Pedido ============================= */
/* ========================================================================== */

/* ---- Flujo de captura de datos (según con/sin factura) ---- */
const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL     = {
  nombre:'Nombre / Razón Social',
  rfc:'RFC',
  email:'Email',
  calle:'Calle',
  numero:'Número',
  colonia:'Colonia',
  cp:'Código Postal'
};

function firstMissing(list, c={}){ for (const k of list){ if (!c || !String(c[k]??'').trim()) return k; } return null; }

function parseCustomerFragment(field, text){
  const t = String(text||'');
  if (field==='nombre') return t.trim();
  if (field==='rfc'){
    const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i);
    return (m ? m[1] : t).toUpperCase().trim();
  }
  if (field==='email'){
    const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return (m ? m[0] : t).toLowerCase().trim();
  }
  if (field==='numero'){
    const m = t.match(/\b(\d+[A-Z]?)\b/i);
    return (m ? m[1] : t).trim();
  }
  if (field==='cp'){
    const m = t.match(/\b(\d{5})\b/);
    return (m ? m[1] : t).trim();
  }
  return t.trim();
}

/** Captura secuencial de datos de cliente (llamado desde handleWebhook) */
async function handleCollectSequential(env, session, toE164, text, now){
  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;

  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = String(session.stage||'').replace('collect_','');

  // Guardar respuesta del campo actual
  c[field] = parseCustomerFragment(field, text);

  // Enriquecer ciudad/estado desde CP si aplica
  if (field==='cp' && c.cp && !c.ciudad) {
    const info = await cityFromCP(env, c.cp);
    if (info) {
      c.ciudad = info.ciudad || info.municipio || c.ciudad;
      c.estado = info.estado || c.estado;
    }
  }

  await saveSession(env, session, now);

  // Buscar siguiente campo
  const nextField = firstMissing(list, c);
  if (nextField){
    session.stage = `collect_${nextField}`;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿${LABEL[nextField]}?`);
    return ok('EVENT_RECEIVED');
  }

  // Si ya está completo, crear pedido
  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) {
    await sendWhatsAppText(env, toE164,
      `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\n` +
      `Un asesor te confirmará entrega y forma de pago.`
    );
    await notifySupport(env,
      `Nuevo pedido #${res.pedido_id ?? '—'}\n` +
      `Cliente: ${c.nombre || 'N/D'} (${toE164})`
    );
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

/* ---- Precarga/actualización de cliente ---- */

async function preloadCustomerIfAny(env, session){
  try {
    const r = await sbGet(env, 'cliente', {
      query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${session.from}&limit=1`
    });
    if (r && r[0]) session.data.customer = { ...(session.data.customer||{}), ...r[0] };
  } catch (e){ console.warn('preloadCustomerIfAny', e); }
}

async function ensureClienteFields(env, cliente_id, c){
  try{
    const patch = {};
    ['nombre','rfc','email','calle','numero','colonia','ciudad','estado','cp'].forEach(k=>{
      if (c && String(c[k]??'').trim()) patch[k] = c[k];
    });
    if (Object.keys(patch).length>0) {
      await sbPatch(env, 'cliente', patch, `id=eq.${cliente_id}`);
    }
  } catch(e){ console.warn('ensureClienteFields', e); }
}

async function upsertClienteByPhone(env, phone) {
  try {
    const exist = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (exist && exist[0]?.id) return exist[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.[0]?.id || ins?.data?.[0]?.id || null;
  } catch (e) { console.warn('upsertClienteByPhone', e); return null; }
}

/* ---- Crear Pedido + Items + Decremento de stock ---- */

async function createOrderFromSession(env, session, toE164) {
  try {
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok: false, error: 'empty cart' };

    const c = session.data.customer || {};
    let cliente_id = null;

    // Buscar/crear cliente
    try {
      const exist = await sbGet(env, 'cliente', {
        query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email || '')})&limit=1`
      });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch {}

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
      cliente_id = ins?.[0]?.id || ins?.data?.[0]?.id || null;
    } else {
      await ensureClienteFields(env, cliente_id, c);
    }

    // Total
    let total = 0;
    for (const it of cart) total += Number(it.product?.precio || 0) * Number(it.qty || 1);

    // Crear pedido
    const p = await sbUpsert(env, 'pedido', [{
      cliente_id, total, moneda: 'MXN', estado: 'nuevo', created_at: new Date().toISOString()
    }], { returning: 'representation' });
    const pedido_id = p?.[0]?.id || p?.data?.[0]?.id;

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

    // Decremento de stock (sólo lo que está en stock)
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = Number(row?.[0]?.stock || 0);
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

/* ---- SEPOMEX lookup (cp → ciudad/estado) ---- */
async function cityFromCP(env, cp){
  try {
    const r = await sbGet(env, 'sepomex_cp', {
      query: `cp=eq.${encodeURIComponent(cp)}&select=cp,estado,municipio,ciudad&limit=1`
    });
    return (r && r[0]) ? r[0] : null;
  } catch { return null; }
}

/* ========================================================================== */
/* ====================== Supabase helpers (extras) ========================= */
/* ========================================================================== */

/** PATCH genérico (return=minimal) */
async function sbPatch(env, table, patch, filter){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method:'PATCH',
    headers:{
      apikey: env.SUPABASE_ANON_KEY,
      Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json',
      Prefer:'return=minimal'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', table, await r.text());
}

/** RPC genérico */
async function sbRpc(env, fn, params){
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method:'POST',
    headers:{
      apikey: env.SUPABASE_ANON_KEY,
      Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json'
    },
    body: JSON.stringify(params||{})
  });
  if (!r.ok) { console.warn('sbRpc', fn, await r.text()); return null; }
  return r.json();
}

/* ========================================================================== */
/* ================================ Soporte ================================= */
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

function extractSvInfo(text='') {
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
  if (/mancha|calidad|l[ií]nea|linea/i.test(t)) out.falla = 'Calidad de impresión';
  if (/\b(parado|urgente|producci[oó]n detenida|parada)\b/i.test(t)) out.prioridad = 'alta';

  Object.assign(out, parseAddressLoose(text));
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

    // Si veníamos pidiendo un campo específico
    if (session.stage === 'sv_collect' && session.data.sv_need_next) {
      svFillFromAnswer(sv, session.data.sv_need_next, text, env);
      await saveSession(env, session, now);
    }

    // Enriquecer con el mensaje actual
    const mined = extractSvInfo(text);
    if (!sv.marca && mined.marca) sv.marca = mined.marca;
    if (!sv.modelo && mined.modelo) sv.modelo = mined.modelo;
    if (!sv.falla && mined.falla) sv.falla = mined.falla;
    if (!sv.when && mined.when) sv.when = mined.when;
    ['calle','numero','colonia','cp','ciudad','estado','error_code','prioridad','nombre','email'].forEach(k=>{
      if (!sv[k] && mined[k]) sv[k]=mined[k];
    });

    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    // Bienvenida de soporte (una vez)
    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Lamento la falla 😕. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* del problema.`);
    }

    // Tips rápidos según la falla
    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick);
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    // Prellenar con datos del cliente si existen
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && c.nombre) sv.nombre = c.nombre;
    if (!sv.email && c.email) sv.email = c.email;

    // Campos faltantes
    const needed = [];
    const pmNow = parseBrandModel(text);
    if (!(sv.marca && sv.modelo)) {
      if (pmNow.marca && !sv.marca) sv.marca = pmNow.marca;
      if (pmNow.modelo && !sv.modelo) sv.modelo = pmNow.modelo;
    }
    if (!(sv.marca && sv.modelo)) needed.push('modelo');

    if (!sv.falla)  needed.push('falla');
    if (!sv.calle)  needed.push('calle');
    if (!sv.numero) needed.push('numero');
    if (!sv.colonia)needed.push('colonia');
    if (!sv.cp)     needed.push('cp');
    if (!sv.when?.start) needed.push('horario');
    if (!sv.nombre) needed.push('nombre');
    if (!sv.email)  needed.push('email');

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
    try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email, calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp }); } catch {}

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

    // Crear Orden de Servicio
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
      osId = os?.[0]?.id || os?.data?.[0]?.id || null;
    } catch (e) { console.warn('[Supabase] OS upsert', e); estado = 'pendiente'; }

    if (event) {
      await sendWhatsAppText(env, toE164,
        `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\n` +
        `Dirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}\n` +
        `Técnico asignado: ${calName || 'por confirmar'}.\n\nSi necesitas reprogramar o cancelar, dímelo con confianza.`
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

/* ========================================================================== */
/* ============================ Google Calendar ============================= */
/* ========================================================================== */

function fmtDate(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }
  catch{ return new Date(d).toLocaleDateString('es-MX'); }
}
function fmtTime(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }
  catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; }
}

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

async function getCalendarPool(env) {
  try{
    const r = await sbGet(env, 'calendar_pool', { query: 'select=gcal_id,name,active&active=is.true' });
    return Array.isArray(r) ? r : [];
  } catch(e){ console.warn('calendar_pool', e); return []; }
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

/* ========================================================================== */
/* ============================ Fechas y horas ============================== */
/* ========================================================================== */

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
  const localeHour = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone:tz }).format(start));
  let newStart = new Date(start);
  if (localeHour < 10) newStart.setHours(10,0,0,0);
  if (localeHour >= 15) newStart.setHours(14,0,0,0);
  const newEnd = new Date(newStart.getTime()+60*60*1000);
  return { start: newStart.toISOString(), end: newEnd.toISOString() };
}

