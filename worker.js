/**
 * CopiBot – IA + Ventas + Soporte + GCal — Build R6.3 (2025-09)
 * FIXES R6.3 (CRÍTICO):
 *  - Sesión TRIPLE-KEY (from, fromE164, fromDigits) para no perder sv_collect / intent_lock.
 *  - Idempotencia por mid al inicio del manejo de evento.
 *  - Barrera 0: si stage sv_* o intent_lock='support' ⇒ forzar handleSupport (antes de saludos/ventas/FAQ).
 *  - Regla dura: texto con marca+modelo (sin palabras de compra) ⇒ fijar support, sv_collect, sv_need_next='falla', guardar de inmediato y llamar soporte.
 *  - handleSupport mapea respuesta previa (svFillFromAnswer) ANTES de recalcular faltantes.
 *  - Guardados antes/después de enviar cuando cambia stage/intent_lock para sobrevivir reconexiones.
 *  - Logs detallados bajo env.DEBUG (claves usadas, cambios de stage/lock, marca/modelo/falla finales).
 *
 * MEJORAS (sin botones, todo por texto):
 *  - “Quick help” (FAQ express) + recomendación conversacional de visita; luego confirmación/edición de dirección.
 *  - Uso de datos de cliente existentes (no volver a pedir) y/libreta de direcciones múltiples por cliente.
 *  - Confirmación previa a agendar: mostrar dirección/nombre detectados y permitir “cambiar dirección”, “agregar nueva” o “usar otra”.
 *  - Ventas mantiene flujo y registro; soporte siempre prioritario por Barrera 0.
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
        const ctx = extractWhatsAppContext(payload); // R6.3: incluye fromDigits
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, fromDigits, profileName, textRaw, msgType } = ctx;
        const originalText = (textRaw || '').trim();
        const lowered = originalText.toLowerCase();
        const ntext = normalizeWithAliases(originalText);
        const now = new Date();

        // ===== Session (TRIPLE-KEY) =====
        let session = await loadSessionMulti(env, from, fromE164, fromDigits);
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;
        session.fromE164 = fromE164;
        session.fromDigits = fromDigits;

        dlog(env, '[sess.load]', {
          try_from: from, try_e164: fromE164, try_digits: fromDigits,
          loaded_stage: session.stage, lock: session?.data?.intent_lock
        });

        // Idempotencia por mid — temprano
        if (session?.data?.last_mid && session.data.last_mid === mid) {
          dlog(env, '[mid] duplicate, skipping', mid);
          return ok('EVENT_RECEIVED');
        }
        session.data.last_mid = mid;

        // Nombre suave por profile
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // NO-TEXTO (excepto interactive -> ya se mapea a text)
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, `¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂`);
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          return ok('EVENT_RECEIVED');
        }

        /* ============================================================
         *  BARRERA 0 (soporte tiene prioridad absoluta)
         * ============================================================ */
        if (session.stage?.startsWith('sv_') || session?.data?.intent_lock === 'support') {
          dlog(env, '[barrera0] support-priority', { stage: session.stage, lock: session?.data?.intent_lock });
          // Guardamos por si venimos de reconexión con cambios
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        /* ============================================================
         *  REGLA DURA: Marca+Modelo (sin “compra”) ⇒ Soporte
         * ============================================================ */
        const SALES_WORDS = /\b(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio)\b/i;
        const pmGuard = parseBrandModel(ntext);
        if (pmGuard?.modelo && !SALES_WORDS.test(ntext)) {
          session.data.intent_lock = 'support';
          // inicia colecta si veníamos en idle
          if (!session.stage?.startsWith('sv_')) {
            session.stage = 'sv_collect';
            session.data.sv_need_next = 'falla';
            session.data.sv = session.data.sv || {};
            if (pmGuard.marca) session.data.sv.marca = pmGuard.marca;
            if (pmGuard.modelo) session.data.sv.modelo = pmGuard.modelo;
          }
          // Guardado inmediato en las 3 llaves — evita “saludo” en el siguiente turno
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          dlog(env, '[hard-rule] modelo=>support', {
            marca: session.data.sv?.marca, modelo: session.data.sv?.modelo,
            stage: session.stage, need: session.data.sv_need_next
          });
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        /* =================== Saludo (no intrusivo) =================== */
        if (RX_GREET.test(lowered)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          session.data.last_greet_at = now.toISOString();
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          return ok('EVENT_RECEIVED');
        }

        /* ====== Comandos universales de soporte ====== */
        if (/\b(cancel(a|ar).*(cita|visita|servicio))\b/i.test(lowered)) {
          session.data.intent_lock = 'support';
          await svCancel(env, session, fromE164);
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          return ok('EVENT_RECEIVED');
        }
        if (/\b(reprogram|mueve|cambia|modif)\w*/i.test(lowered)) {
          const when = parseNaturalDateTime(lowered, env);
          if (when?.start) {
            session.data.intent_lock = 'support';
            await svReschedule(env, session, fromE164, when);
            await saveSessionMulti(env, session, from, fromE164, fromDigits);
            return ok('EVENT_RECEIVED');
          }
        }
        if (/\b(cu[aá]ndo|cuando).*(cita|visita|servicio)\b/i.test(lowered)) {
          session.data.intent_lock = 'support';
          await svWhenIsMyVisit(env, session, fromE164);
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          return ok('EVENT_RECEIVED');
        }

        /* =================== Intenciones =================== */
        const supportIntent = isSupportIntent(ntext) || (await intentIs(env, originalText, 'support'));
        if (supportIntent) {
          session.data.intent_lock = 'support'; // 🔒
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          const handled = await handleSupport(env, session, fromE164, originalText, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        const salesIntent = RX_INV_Q.test(ntext) || (await intentIs(env, originalText, 'sales'));

        /* =================== Ventas =================== */
        if (salesIntent) {
          const handled = await startSalesFromQuery(env, session, fromE164, originalText, ntext, now);
          return handled;
        }

        // ===== Stages de ventas (si quedaron activos) =====
        if (session.stage === 'ask_qty') return await handleAskQty(env, session, fromE164, originalText, lowered, ntext, now);
        if (session.stage === 'cart_open') return await handleCartOpen(env, session, fromE164, originalText, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, originalText);
        if (session.stage?.startsWith('collect_')) return await handleCollectSequential(env, session, fromE164, originalText, now);

        // ===== FAQs =====
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSessionMulti(env, session, from, fromE164, fromDigits);
          return ok('EVENT_RECEIVED');
        }

        // ===== Fallback IA =====
        const reply = await aiSmallTalk(env, session, 'fallback', originalText);
        await sendWhatsAppText(env, fromE164, reply);
        await saveSessionMulti(env, session, from, fromE164, fromDigits);
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
  const hasDevice  = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase     = /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);
  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

/* ========================================================================== */
/* =============================== Helpers ================================== */
/* ========================================================================== */

const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return String(s||'').replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function ok(s='ok'){ return new Response(s, { status: 200 }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
function dlog(env, ...args){ if ((env.DEBUG||'').toString().toLowerCase()==='true') console.log(...args); }

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
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde breve, humano y útil. Máx 1 emoji. Mantén el tono cordial.`;
  const prompt = mode==='fallback'
    ? `El usuario dijo: """${userText}""". Responde breve y ofrece inventario o soporte si aplica, sin botones.`
    : `El usuario dijo: """${userText}""". Responde breve.`;
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 👋 ¿En qué te ayudo?`);
}

async function intentIs(env, text, expected){
  try{
    const sys = `Clasifica en JSON {"intent":"support|sales|faq|smalltalk"} para el texto en es-MX. Devuelve solo JSON.`;
    const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
    const j = JSON.parse(out||'{}'); return j?.intent === expected;
  }catch{return false;}
}

/** IA opcional para reforzar NER de inventario (familia/color/cantidad) */
async function aiExtractTonerQuery(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Extrae de una consulta (es-MX) sobre tóners los campos { "familia": "versant|docucolor|primelink|versalink|altalink|apeos|c70|", "color": "yellow|magenta|cyan|black|null", "subfamilia": "string|null", "cantidad": "number|null" } en JSON. No inventes.`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try { return JSON.parse(out||'{}'); } catch { return null; }
}


/* ========================================================================== */
/* ============================== WhatsApp I/O ============================== */
/* ========================================================================== */

/** Extrae contexto relevante del webhook de WhatsApp.
 *  R6.3: devuelve también fromDigits para la sesión triple-key e idempotencia por mid.
 */
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

    const from = msg.from; // WhatsApp manda solo dígitos con código de país
    const fromDigits = String(from || '').replace(/\D/g, '');
    const fromE164 = `+${fromDigits}`;
    const mid = msg.id || `${Date.now()}_${Math.random()}`;
    const profileName = value?.contacts?.[0]?.profile?.name || '';

    return { msg, from, fromDigits, fromE164, mid, textRaw, profileName, msgType };
  } catch {
    return null;
  }
}

async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) { console.warn('WA env missing'); return; }
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to: String(toE164 || '').replace(/\D/g, ''),
    text: { body: String(body ?? '') }
  };
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
/* =============================== Sesión (KV) ============================== */
/* ========================================================================== */
/** R6.3: triple-key (from, fromE164, fromDigits) para recuperar la sesión sin
 *  importar el formato en que llegue el remitente (con/sin +, etc).
 *  TTL 7 días. Guardamos en las tres claves para sobrevivir reconexiones.
 */
async function loadSessionMulti(env, from, fromE164, fromDigits){
  try{
    const a = await env.COPIBOT_KV.get(`sess:${fromE164}`, 'json');
    if (a) return a;
    const b = await env.COPIBOT_KV.get(`sess:${fromDigits}`, 'json');
    if (b) return b;
    const c = await env.COPIBOT_KV.get(`sess:${from}`, 'json');
    return c || { from, fromE164, fromDigits, stage:'idle', data:{} };
  }catch{
    return { from, fromE164, fromDigits, stage:'idle', data:{} };
  }
}

async function saveSessionMulti(env, sess, from, fromE164, fromDigits){
  try{
    const val = JSON.stringify(sess);
    const ttl = 60*60*24*7; // 7 días
    await env.COPIBOT_KV.put(`sess:${from}`, val, { expirationTtl: ttl });
    await env.COPIBOT_KV.put(`sess:${fromE164}`, val, { expirationTtl: ttl });
    if (fromDigits) await env.COPIBOT_KV.put(`sess:${fromDigits}`, val, { expirationTtl: ttl });
    dlog(env, '[sess.save]', {
      stage: sess.stage,
      lock: sess?.data?.intent_lock,
      need: sess?.data?.sv_need_next,
      keys: { from, fromE164, fromDigits }
    });
  }catch(e){ console.warn('saveSessionMulti', e); }
}

/* ========================================================================== */
/* =============== Normalización / Aliases / Hints de modelo ================ */
/* ========================================================================== */

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
  for (const [bad, good] of aliases){
    out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  }
  return out;
}

function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};
  // —— Familia
  if (/\bversant\b/.test(t) || /\b(80|180|2100|280|4100)\b/.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/.test(t) || /\bprimelink\b/.test(t)) out.family = 'primelink';
  else if (/\bapeos\b/.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/.test(t)) out.family = 'c70';

  // —— Color
  if (/\b(amarillo|yellow)\b/.test(t)) out.color = 'yellow';
  else if (/\bmagenta\b/.test(t)) out.color = 'magenta';
  else if (/\b(cyan|cian)\b/.test(t)) out.color = 'cyan';
  else if (/\b(negro|black|bk|k)\b/.test(t)) out.color = 'black';

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

/* ========================================================================== */
/* ===================== Inventario: matching y rendering =================== */
/* ========================================================================== */

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

/** Render corto de producto para WhatsApp */
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1) ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

/* === findBestProduct: Color y Familia = filtros duros === */
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

    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });

    return pool[0] || null;
  };

  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 30 }) || [];
    const best = pick(res);
    if (best) return best;
  } catch {}

  if (hints.family && !opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like},compatible.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
      return null;
    } catch {}
  }

  if (!hints.family || opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%toner%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
    } catch {}
  }

  return null;
}

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

/** 
 * Render de producto orientado a conversión:
 * - Si stock=0, NO mostramos “0 pzas”, sólo “Entrega: sobre pedido”.
 * - Siempre mantenemos el copy de producto y el precio.
 */
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `Entrega: *sobre pedido* (confirmamos tiempo)`;
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

    const best = await findBestProduct(env, enrichedQ); // familia+color = filtro duro
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSessionMulti(env, session, session.from, toE164);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
      return ok('EVENT_RECEIVED');
    } else {
      // Ofrecer compatibles solo si hay familia clara
      const hints = extractModelHints(enrichedQ);
      if (hints.family && ((env.STRICT_FAMILY_MATCH||'').toString().toLowerCase() !== 'true')) {
        session.stage = 'await_compatibles';
        session.data.pending_query = enrichedQ;
        await saveSessionMulti(env, session, session.from, toE164);
        await sendWhatsAppText(env, toE164, `Ese modelo está *sobre pedido* o sin disponibilidad directa. ¿Te muestro opciones *compatibles* en otra línea?`);
        return ok('EVENT_RECEIVED');
      }
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

/* =============== Inventario & Pedido (búsqueda) =============== */

function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};
  // —— Familia
  if (/\bversant\b/.test(t) || /\b(80|180|2100|280|4100)\b/.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/.test(t) || /\b(550|560|570)\b/.test(t)) out.family = 'docucolor';
  else if (/\bprime\s*link\b/.test(t) || /\bprimelink\b/.test(t)) out.family = 'primelink';
  else if (/\bapeos\b/.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/.test(t)) out.family = 'c70';

  // —— Color
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

/** 
 * findBestProduct:
 * - Siempre permite elegir aunque stock sea 0 (luego se parte en stock/sobre pedido).
 * - Prioriza stock>0; si todos son 0, igual devuelve el más barato.
 */
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

    // Prioriza con-stock, pero si no hay, regresa el mejor aunque sea sobre pedido
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });

    return pool[0] || null;
  };

  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 30 }) || [];
    const best = pick(res);
    if (best) return best;
  } catch {}

  if (hints.family && !opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like},compatible.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
      return null;
    } catch {}
  }

  if (!hints.family || opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%toner%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
    } catch {}
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
  const best = await findBestProduct(env, enrichedQ);

  const hints = extractModelHints(enrichedQ);

  if (!best && hints.family) {
    session.stage = 'await_compatibles';
    session.data.pending_query = enrichedQ;
    await saveSessionMulti(env, session, session.from, toE164);
    await sendWhatsAppText(env, toE164, `Ese modelo está *sobre pedido* o sin disponibilidad directa. ¿Quieres que te muestre opciones *compatibles* en otra línea?`);
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
/* ============================ Cliente / Pedido ============================ */
/* ========================================================================== */

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
      await ensureClienteFields(env, cliente_id, c);
    }

    let total = 0;
    for (const it of cart) total += Number(it.product?.precio || 0) * Number(it.qty || 1);

    const p = await sbUpsert(env, 'pedido', [{
      cliente_id, total, moneda: 'MXN', estado: 'nuevo', created_at: new Date().toISOString()
    }], { returning: 'representation' });
    const pedido_id = p?.data?.[0]?.id;

    const items = cart.map(it => ({
      pedido_id, producto_id: it.product?.id || null, sku: it.product?.sku || null,
      nombre: it.product?.nombre || null, qty: it.qty, precio_unitario: Number(it.product?.precio || 0)
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
/* ================================ Supabase ================================= */
/* ========================================================================== */

async function sbGet(env, table, { query }) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, { headers: { apikey: env.SUPABASE_ANON_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE || env.SUPABASE_ANON_KEY}` } });
  if (!r.ok) { console.warn('sbGet', r.status, await r.text()); return null; }
  return await r.json();
}

async function sbUpsert(env, table, rows, { onConflict, returning='representation' } = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${onConflict ? `?on_conflict=${encodeURIComponent(onConflict)}` : ''}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE || env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      Prefer: `resolution=merge-duplicates,return=${returning}`
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) { console.warn('sbUpsert', r.status, await r.text()); return null; }
  const data = returning==='minimal' ? null : await r.json();
  return { data };
}

async function sbPatch(env, table, patch, where) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${where}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE || env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', r.status, await r.text());
}

async function sbRpc(env, fn, args) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE || env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(args || {})
  });
  if (!r.ok) { console.warn('sbRpc', r.status, await r.text()); return null; }
  return await r.json();
}

/* ========================================================================== */
/* =============================== SOPORTE ================================== */
/* ========================================================================== */

/** Detecta marca/modelo a partir de texto libre */
function parseBrandModel(text=''){
  const t = normalizeWithAliases(text);
  let marca = null;

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

  // C60/C70/C75 / C/B series
  const mSeries = norm.match(/\b([cb]\d{2,4})\b/i);
  if (mSeries) return { marca: marca || 'Xerox', modelo: mSeries[1].toUpperCase() };

  // Números DocuColor si dijo docucolor
  const m550 = norm.match(/\b(550|560|570)\b/);
  if (/\bdocu\s*color\b/i.test(norm) && m550) return { marca: marca || 'Xerox', modelo: `DOCUCOLOR ${m550[1]}` };

  // Sólo “docucolor” / “versant”
  if (/\bdocu\s*color\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'DOCUCOLOR' };
  if (/\bversant\b/i.test(norm)) return { marca: marca || 'Xerox', modelo: 'VERSANT' };

  return { marca, modelo: null };
}

/** Extrae campos típicos del flujo sv */
function extractSvInfo(text) {
  const t = normalizeWithAliases(text);
  const out = {};

  if (/xerox/i.test(t)) out.marca = 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(t)) out.marca = 'Fujifilm';

  const pm = parseBrandModel(text);
  if (pm.marca) out.marca = out.marca || pm.marca;
  if (pm.modelo) out.modelo = pm.modelo;

  const err = t.match(/\berror\s*([0-9\-]+)\b/i);
  if (err) out.error_code = err[1];

  if (/no imprime/i.test(t)) out.falla = 'No imprime';
  if (/atasc(a|o)|se atora|se traba|arrugad(i|o)|saca el papel|fusor/i.test(t)) out.falla = 'Atasco/arrugado de papel';
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

/** Mapea respuesta puntual a sv */
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

function displayFieldSupport(k){
  const map = {
    modelo:'marca y modelo', falla:'descripción breve de la falla', nombre:'Nombre o Razón Social', email:'email',
    calle:'calle', numero:'número', colonia:'colonia', ciudad:'ciudad o municipio', estado:'estado', cp:'código postal', horario:'día y hora (10:00–15:00)'
  };
  return map[k]||k;
}

/** Consejos rápidos según síntomas */
function quickHelp(ntext){
  if (/\batasc(a|o)|se atora|se traba|arrugad|fusor\b/.test(ntext)){
    return `Tip rápido 🧰\n1) Apaga/enciende el equipo.\n2) Retira papel atorado (bandejas/puertas).\n3) Abre/cierra el fusor con cuidado.\nSi sigue igual, conviene visita para diagnóstico.`;
  }
  if (/\bno imprime\b/.test(ntext)){
    return `Prueba esto 🧰\n1) Reinicia impresora.\n2) Verifica tóner y puertas cerradas.\n3) Imprime página de prueba.\nSi persiste, mejor agendamos visita.`;
  }
  if (/\bmancha|l[ií]ne?a|calidad\b/.test(ntext)){
    return `Sugerencia 🎯\n1) Imprime patrón de prueba.\n2) Reinstala tóners y limpia rodillos.\nSi no mejora, te envío técnico para revisión.`;
  }
  return null;
}

/* ========================================================================== */
/* ============================ Multi-direcciones =========================== */
/* ========================================================================== */

/** Obtiene (o crea) cliente por teléfono */
async function upsertClienteByPhone(env, phone){
  try{
    const exist = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (exist && exist[0]) return exist[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.data?.[0]?.id || null;
  }catch(e){ return null; }
}

/** Direcciones: lista del cliente */
async function listDirecciones(env, cliente_id){
  try{
    const r = await sbGet(env, 'cliente_direccion', { query: `select=id,alias,calle,numero,colonia,ciudad,estado,cp,is_default&cliente_id=eq.${cliente_id}&order=is_default.desc,created_at.asc` });
    return Array.isArray(r) ? r : [];
  }catch{ return []; }
}

/** Inserta/actualiza dirección y devuelve registro */
async function upsertDireccion(env, cliente_id, dir, setDefault=false){
  try{
    const row = [{
      cliente_id,
      alias: dir.alias || null,
      calle: dir.calle || null, numero: dir.numero || null, colonia: dir.colonia || null, ciudad: dir.ciudad || null, estado: dir.estado || null, cp: dir.cp || null,
      is_default: !!setDefault
    }];
    const ins = await sbUpsert(env, 'cliente_direccion', row, { returning: 'representation' });
    if (setDefault && ins?.data?.[0]?.id){
      // quita default a otras
      await sbPatch(env, 'cliente_direccion', { is_default: false }, `cliente_id=eq.${cliente_id}&id=neq.${ins.data[0].id}`);
    }
    return ins?.data?.[0] || null;
  }catch(e){ console.warn('upsertDireccion', e); return null; }
}

/** Selecciona una dirección preferente a partir de sv o default */
async function ensureDireccionForSV(env, cliente_id, sv){
  const list = await listDirecciones(env, cliente_id);
  const asText = (d)=>`${d.calle||''} ${d.numero||''}, ${d.colonia||''}, ${d.ciudad||''}, ${d.estado||''}, CP ${d.cp||''}`.replace(/\s+/g,' ').trim();
  // si sv trae una dirección completa, intenta matchear
  if (sv.calle && sv.numero && sv.colonia && sv.cp){
    const probe = { calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp };
    const match = list.find(d =>
      normalizeBase(asText(d)) === normalizeBase(asText(probe))
    );
    if (match) return { direccion: match, list };
    // si no existe, inserta como nueva (no default por ahora; se pide confirmación)
    const inserted = await upsertDireccion(env, cliente_id, probe, false);
    return { direccion: inserted, list: [inserted, ...list] };
  }
  // fallback: usa default si hay
  const def = list.find(d => d.is_default) || list[0] || null;
  return { direccion: def, list };
}

/* ========================================================================== */
/* ================================ Dirección =============================== */
/* ========================================================================== */

function parseCustomerText(text=''){
  const t = text;
  const out = {};
  const em = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (em) out.email = em[0].toLowerCase();
  const cp = t.match(/\b(\d{5})\b/);
  if (cp) out.cp = cp[1];
  const num = t.match(/\b(\d+[A-Z]?)\b/);
  if (num) out.numero = num[1];
  return out;
}

function parseAddressLoose(text=''){
  const out = {};
  const mCol = text.match(/\bcol(?:\.|onia)?\s+([a-z0-9\-\sáéíóúñ]+)\b/i);
  if (mCol) out.colonia = clean(mCol[1]);

  const mCal1 = text.match(/\bcalle\s+([a-z0-9\-\sáéíóúñ]+)\b/i);
  if (mCal1) out.calle = clean(mCal1[1]);
  const mCal2 = text.match(/\ben\s+([a-z0-9\-\sáéíóúñ]+)\s+#?\d+\b/i);
  if (!out.calle && mCal2) out.calle = clean(mCal2[1]);

  const mCd = text.match(/\b(le[oó]n|celaya|irapuato|gto\.?|guanajuato|quer[eé]taro|cdmx|ciudad de m[eé]xico)\b/i);
  if (mCd) out.ciudad = toTitleCase(mCd[1].normalize('NFD').replace(/[\u0300-\u036f]/g,''));
  const mSt = text.match(/\b(guanajuato|quer[eé]taro|jalisco|michoac[aá]n|estado de m[eé]xico)\b/i);
  if (mSt) out.estado = toTitleCase(mSt[1].normalize('NFD').replace(/[\u0300-\u036f]/g,''));
  return out;
}

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
/* ============================== Google Calendar ========================== */
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
  for (let i=0;i<6;i++) { // 6 intentos de 30 min
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

/* ============================ Pool calendarios + util OS ============================ */
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

/* ========================================================================== */
/* ============================== Reglas de confirmación ==================== */
/* ========================================================================== */

function wantsYes(t){ return /\b(s[ií]|sí|si|correcto|as[ií] está|va|ok|listo)\b/i.test(t||''); }
function wantsChange(t){ return /\b(cambiar|editar|corregir|modificar)\b/i.test(t||''); }
function asksOtherAddress(t){ return /\b(otra\s+direcci[oó]n|otra\s+sucursal|diferente\s+direcci[oó]n)\b/i.test(t||''); }
function fieldToEdit(t){
  if (/\bnombre\b/i.test(t)) return 'nombre';
  if (/\bemail\b/i.test(t)) return 'email';
  if (/\bcalle\b/i.test(t)) return 'calle';
  if (/\bn[uú]mero\b/i.test(t)) return 'numero';
  if (/\bcolonia\b/i.test(t)) return 'colonia';
  if (/\b(c[d.]?p|c[oó]digo\s+postal)\b/i.test(t)) return 'cp';
  if (/\bciudad|municipio\b/i.test(t)) return 'ciudad';
  if (/\bestado\b/i.test(t)) return 'estado';
  return null;
}

/* ========================================================================== */
/* ================================ handleSupport =========================== */
/* ========================================================================== */

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    const beforeStage = session.stage;
    const beforeLock = session?.data?.intent_lock;

    // Candado del flujo soporte
    session.data.intent_lock = 'support';
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    const wasCollecting = session.stage === 'sv_collect';
    const prevNeeded = session.data.sv_need_next || null;

    // Mapear respuesta previa (antes de recalcular)
    if (wasCollecting && prevNeeded) svFillFromAnswer(sv, prevNeeded, text);

    // Completar con extractores
    const extra = extractSvInfo(text);
    for (const k of Object.keys(extra)) { if (!truthy(sv[k])) sv[k] = extra[k]; }

    // Interpretar horario
    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    // Bienvenida / empatía una sola vez
    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `Gracias por avisar. Lamento la falla 😕. Ayúdame con la *marca y el modelo* del equipo y una breve *descripción* del problema.`);
    }

    // Tips rápidos (si aplica)
    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, quick + `\n\n¿Deseas que *agendemos* una visita para revisar tu equipo?`);
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    // Precarga de datos de cliente si existen
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;
    // Si no hay dirección en sv, intenta tomar default de cliente_direccion
    const cliente_id = await upsertClienteByPhone(env, session.from);

    // === Confirmaciones previas de datos (nuevo mini-flujo) ===
    // Etapas:
    //   sv_collect -> (cuando ya no falte nada) sv_confirm -> sv_confirm_edit? -> sv_confirm (loop) -> sv_confirm_ok -> agenda/OS
    //   sv_add_address (colecta dirección adicional) -> back to sv_confirm
    const askOrder = ['modelo','falla','calle','numero','colonia','cp','horario','nombre','email'];
    const needed = [];
    if (!(truthy(sv.marca) && truthy(sv.modelo))) needed.push('modelo');
    if (!truthy(sv.falla)) needed.push('falla');

    // Dirección preferida: si faltan campos fuertes, se pedirán; si no, confirmar
    if (!truthy(sv.calle)) needed.push('calle');
    if (!truthy(sv.numero)) needed.push('numero');
    if (!truthy(sv.colonia)) needed.push('colonia');
    if (!truthy(sv.cp)) needed.push('cp');

    if (!sv.when?.start) needed.push('horario');
    if (!truthy(sv.nombre)) needed.push('nombre');
    if (!truthy(sv.email)) needed.push('email');

    // Estado de edición
    if (session.stage === 'sv_confirm_edit') {
      const field = session.data.sv_edit_field;
      if (field) {
        svFillFromAnswer(sv, field, text);
        session.data.sv_edit_field = null;
        session.stage = 'sv_confirm';
        await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      }
    }

    // Añadir dirección adicional
    if (session.stage === 'sv_add_address') {
      // Reusar secuencia de captura: calle -> numero -> colonia -> cp -> ciudad -> estado
      const addrFields = ['calle','numero','colonia','cp','ciudad','estado'];
      const addrField = session.data.sv_addr_need || 'calle';
      svFillFromAnswer(sv, addrField, text);
      const nextIdx = addrFields.indexOf(addrField)+1;
      const nextField = addrFields[nextIdx];
      if (nextField) {
        session.data.sv_addr_need = nextField;
        await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
        await sendWhatsAppText(env, toE164, `¿${displayFieldSupport(nextField)}?`);
        return ok('EVENT_RECEIVED');
      } else {
        // Guardar nueva dirección y volver a confirmación
        if (cliente_id) await upsertDireccion(env, cliente_id, { calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp }, false);
        session.stage = 'sv_confirm';
        session.data.sv_addr_need = null;
        await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      }
    }

    // Si aún faltan datos base, seguimos en recolección
    if (needed.length) {
      session.stage = 'sv_collect';
      // prioridad a lo primero de askOrder que falte
      const nextAsk = askOrder.find(k => needed.includes(k));
      session.data.sv_need_next = nextAsk;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);

      let pre = '';
      if (truthy(sv.marca) && truthy(sv.modelo) && nextAsk === 'falla') pre = `Anoté: *${sv.marca} ${sv.modelo}*.\n`;
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
      await sendWhatsAppText(env, toE164, pre + (Q[nextAsk] || '¿Me ayudas con ese dato, por favor?'));
      return ok('EVENT_RECEIVED');
    }

    // ===== Confirmación de datos previos a agendar =====
    // A estas alturas tenemos nombre/email/dirección; confirmamos y permitimos editar/otra dirección.
    if (session.stage !== 'sv_confirm') {
      session.stage = 'sv_confirm';
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    }

    // Asegurar dirección preferida en tabla y mostrar resumen
    const { direccion, list } = cliente_id ? await ensureDireccionForSV(env, cliente_id, sv) : { direccion: null, list: [] };
    const dir = direccion || { calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp };
    const dirText = `${dir.calle || sv.calle} ${dir.numero || sv.numero}, ${dir.colonia || sv.colonia}, ${dir.cp || sv.cp} ${dir.ciudad || sv.ciudad || ''}${dir.estado? ', '+dir.estado:''}`.replace(/\s+/g,' ').trim();

    // Intentos de entender intención en confirm
    if (wantsYes(lowered)) {
      // Confirmado: agendamos
      const tz = env.TZ || 'America/Mexico_City';
      const chosen = clampToWindow(sv.when, tz);

      // Garantiza datos de cliente
      try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email }); } catch {}
      // Actualiza dirección confirmada como default si no había
      if (cliente_id && direccion && !direccion.is_default) {
        try {
          await sbPatch(env, 'cliente_direccion', { is_default: true }, `id=eq.${direccion.id}`);
          await sbPatch(env, 'cliente_direccion', { is_default: false }, `cliente_id=eq.${cliente_id}&id=neq.${direccion.id}`);
        } catch{}
      }

      // Pool & Calendar
      let pool = [];
      try { pool = await getCalendarPool(env) || []; } catch(e){ console.warn('[GCal] pool', e); }
      const cal = pickCalendarFromPool(pool);

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
          cliente_id,
          marca: sv.marca || null, modelo: sv.modelo || null, falla_descripcion: sv.falla || null,
          prioridad: sv.prioridad || 'media', estado,
          ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(),
          gcal_event_id: event?.id || null, calendar_id: cal?.gcal_id || null,
          calle: dir.calle || sv.calle || null, numero: dir.numero || sv.numero || null, colonia: dir.colonia || sv.colonia || null,
          ciudad: dir.ciudad || sv.ciudad || null, estado: dir.estado || sv.estado || null, cp: dir.cp || sv.cp || null,
          created_at: new Date().toISOString()
        }];
        const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
        osId = os?.data?.[0]?.id || null;
      } catch (e) { console.warn('[Supabase] OS upsert', e); estado = 'pendiente'; }

      if (event) {
        await sendWhatsAppText(
          env, toE164,
          `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\nDirección: ${dirText}\nTécnico asignado: ${calName || 'por confirmar'}.\n\nSi necesitas reprogramar o cancelar, dímelo con confianza.`
        );
        session.stage = 'sv_scheduled';
      } else {
        await sendWhatsAppText(env, toE164, `Tomé tus datos ✍️. En breve te confirmo el horario exacto por este medio.`);
        await notifySupport(env, `OS *pendiente/agendar* para ${toE164}\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla}\nDirección: ${dirText}\nNombre: ${sv.nombre} | Email: ${sv.email}`);
        session.stage = 'sv_scheduled';
      }

      session.data.sv.os_id = osId;
      session.data.sv.gcal_event_id = event?.id || null;
      // liberar lock al FINAL
      session.data.intent_lock = null;

      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      return ok('EVENT_RECEIVED');
    }

    if (asksOtherAddress(lowered)) {
      // Inicia captura de nueva dirección adicional
      session.stage = 'sv_add_address';
      session.data.sv_addr_need = 'calle';
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `Perfecto. Vamos a registrar *otra dirección*.\n¿*Calle*?`);
      return ok('EVENT_RECEIVED');
    }

    if (wantsChange(lowered)) {
      const f = fieldToEdit(lowered) || 'calle';
      session.stage = 'sv_confirm_edit';
      session.data.sv_edit_field = f;
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `¿Cuál es el nuevo valor para *${displayFieldSupport(f)}*?`);
      return ok('EVENT_RECEIVED');
    }

    // Mensaje de confirmación (con pista para editar/otra)
    await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    await sendWhatsAppText(env, toE164,
      `Confírmame por favor:\n• *Equipo:* ${sv.marca||''} ${sv.modelo||''}\n• *Falla:* ${sv.falla}\n• *Nombre:* ${sv.nombre}\n• *Email:* ${sv.email}\n• *Dirección:* ${dirText}\n• *Horario tentativo:* ${sv.when ? fmtTime(sv.when.start, env.TZ||'America/Mexico_City') : '10:00–15:00'}\n\nResponde *sí* para confirmar, o escribe *cambiar nombre/calle/colonia/cp/email* para editar, o *otra dirección* para agregar una nueva.`
    );
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

/* ========================================================================== */
/* ============================ Utilidades soporte ========================== */
/* ========================================================================== */

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

/* ========================================================================== */
/* ================================ Fechas ================================== */
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
  const hours = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone:tz }).format(start));
  let newStart = new Date(start);
  if (hours < 10) newStart.setHours(10,0,0,0);
  if (hours >= 15) newStart.setHours(14,0,0,0);
  const newEnd = new Date(newStart.getTime()+60*60*1000);
  return { start: newStart.toISOString(), end: newEnd.toISOString() };
}

/* ========================================================================== */
/* ============================== FAQs rápidas ============================== */
/* ========================================================================== */

/**
 * Busca respuesta corta en base de conocimiento.
 * - Primero intenta coincidencia básica en Supabase (tabla company_info).
 * - Luego cae a FAQs comunes por regex.
 */
async function maybeFAQ(env, ntext) {
  try {
    if (truthy(ntext)) {
      const like = encodeURIComponent(`%${ntext.slice(0, 80)}%`);
      // Ajusta el nombre de tabla/columnas si las tienes diferentes
      const r = await sbGet(env, 'company_info', { query: `select=key,content,tags&or=(key.ilike.${like},content.ilike.${like})&limit=1` });
      if (r && r[0]?.content) return r[0].content;
    }
  } catch {}

  // Fallbacks cortos
  if (/\b(qu[ié]nes?\s+son|sobre\s+ustedes|qu[eé]\s+es\s+cp(\s+digital)?|h[aá]blame\s+de\s+ustedes)\b/i.test(ntext)) {
    return '¡Hola! Somos *CP Digital*. Ayudamos a empresas con consumibles y refacciones para impresoras Xerox y Fujifilm, y brindamos visitas de soporte técnico. Cotizamos, vendemos con o sin factura y agendamos servicio 🙂';
  }
  if (/\b(horario|horarios|a\s+qu[eé]\s+hora)\b/i.test(ntext)) {
    return 'Horario de visitas: *10:00–15:00* (lun–vie). Entregas y atención por WhatsApp todo el día.';
  }
  if (/\b(d[oó]nde\s+est[aá]n|ubicaci[oó]n|direcci[oó]n)\b/i.test(ntext)) {
    return 'Tenemos presencia en Guanajuato (León y Celaya) y coordinamos entregas/servicios a nivel nacional.';
  }
  if (/\b(contacto|whats(app)?|tel[eé]fono|llamar|correo|email)\b/i.test(ntext)) {
    const wa = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164 || '';
    return `Puedes escribirnos por aquí o al WhatsApp de soporte${wa ? `: ${wa}` : ''}.`;
  }
  return null;
}

/* ========================================================================== */
/* ========================= Compatibles (sales helper) ===================== */
/* ========================================================================== */

/**
 * Stage: await_compatibles
 * Cuando en ventas no hay match directo pero sí familia, preguntamos si quiere ver compatibles.
 * Este handler procesa la respuesta:
 *  - "sí": busca alternativas ignorando family (o cambiando a otra sugerida) y ofrece una.
 *  - "no": vuelve al flujo de carrito sin cambios, sugiriendo contacto con asesor.
 */
async function handleAwaitCompatibles(env, session, toE164, originalText, lowered, ntext, now) {
  const YES = /\b(s[ií]|sí|si|ok|va|dale|muestra|ens[eñ]a|otra|alternativa)\b/i;
  const NO  = /\b(no|nel|gracias|luego|despu[eé]s)\b/i;

  const prevQ = session?.data?.pending_query || '';
  if (!prevQ) {
    session.stage = 'cart_open';
    await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    await sendWhatsAppText(env, toE164, `No tengo guardada la consulta anterior. ¿Qué modelo/toner te interesa?`);
    return ok('EVENT_RECEIVED');
  }

  if (YES.test(lowered)) {
    // Buscar compatibles ignorando la familia dura
    const best = await findBestProduct(env, prevQ, { ignoreFamily: true });
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164,
        `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
      );
      return ok('EVENT_RECEIVED');
    } else {
      // Nada compatible claro → Asesor
      session.stage = 'cart_open';
      await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
      await sendWhatsAppText(env, toE164, `No encontré una alternativa directa 😕. Te conecto con un asesor para validar compatibilidad.`);
      await notifySupport(env, `Compatibles sin match. ${toE164}: ${prevQ}`);
      return ok('EVENT_RECEIVED');
    }
  }

  if (NO.test(lowered)) {
    session.stage = 'cart_open';
    await saveSessionMulti(env, session, session.from, toE164, session.fromDigits);
    await sendWhatsAppText(env, toE164, `Sin problema. ¿Busco otra opción o finalizamos?`);
    return ok('EVENT_RECEIVED');
  }

  // Re-pregunta amable
  await sendWhatsAppText(env, toE164, `¿Quieres que te muestre *compatibles* en otra línea? (Responde: *sí* o *no*)`);
  return ok('EVENT_RECEIVED');
}

/* ========================================================================== */
/* ============================== Cron/Recordatorios ======================== */
/* ========================================================================== */

/**
 * cronReminders:
 * - Envía recordatorios de visita técnica que ocurren mañana (rango configurable).
 * - Evita duplicados con una marca simple en OS (columna reminder_sent_at).
 * Requiere tabla 'orden_servicio' con campos:
 *   id, cliente_id, ventana_inicio, ventana_fin, estado, reminder_sent_at
 * y tabla 'cliente' con 'telefono'.
 */
async function cronReminders(env){
  try{
    const tz = env.TZ || 'America/Mexico_City';
    const now = new Date(new Date().toLocaleString('en-US', { timeZone: tz }));
    const start = new Date(now); start.setDate(start.getDate()+1); start.setHours(0,0,0,0);
    const end   = new Date(start); end.setHours(23,59,59,999);

    const q = `select=id,cliente_id,ventana_inicio,ventana_fin,estado,reminder_sent_at
      &estado=in.(agendado,reprogramado,confirmado)
      &ventana_inicio=gte.${encodeURIComponent(start.toISOString())}
      &ventana_inicio=lte.${encodeURIComponent(end.toISOString())}
      &order=ventana_inicio.asc`;
    const os = await sbGet(env, 'orden_servicio', { query: q }) || [];
    if (!os.length) return { ok:true, count:0 };

    // Traer teléfonos
    const byCid = {};
    for (const row of os){
      if (!row.cliente_id) continue;
      if (!byCid[row.cliente_id]) {
        const c = await sbGet(env, 'cliente', { query: `select=id,telefono,nombre,email&id=eq.${row.cliente_id}&limit=1` });
        byCid[row.cliente_id] = c?.[0] || null;
      }
    }

    let count = 0;
    for (const row of os){
      if (row.reminder_sent_at) continue;
      const c = byCid[row.cliente_id];
      const to = c?.telefono ? `+${String(c.telefono).replace(/\D/g,'')}` : null;
      if (!to) continue;
      const msg =
        `Recordatorio 🗓️\nMañana tenemos tu visita técnica:\n` +
        `• De *${fmtTime(row.ventana_inicio, tz)}* a *${fmtTime(row.ventana_fin, tz)}*\n` +
        `Si necesitas *reprogramar* o *cancelar*, respóndeme por aquí.`;
      await sendWhatsAppText(env, to, msg);
      await sbUpsert(env, 'orden_servicio', [{ id: row.id, reminder_sent_at: new Date().toISOString() }], { returning: 'minimal' });
      count++;
    }
    return { ok:true, count };
  }catch(e){
    console.warn('cronReminders error', e);
    return { ok:false, error: String(e) };
  }
}




