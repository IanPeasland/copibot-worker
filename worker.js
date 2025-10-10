/**
 * CopiBot – Worker Lite (SIN IA)
 * Conversacional + Ventas + Soporte Técnico + GCal + Supabase
 * Build: “Lite-R11”
 *
 * Env vars (Cloudflare):
 *  WA_TOKEN, PHONE_ID, VERIFY_TOKEN
 *  SUPABASE_URL, SUPABASE_ANON_KEY
 *  GCAL_CLIENT_ID, GCAL_CLIENT_SECRET, GCAL_REFRESH_TOKEN
 *  SUPPORT_WHATSAPP (o SUPPORT_PHONE_E164)
 *  TZ (ej. America/Mexico_City)
 *  CRON_SECRET (para /cron)
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // Verificación webhook
    if (req.method === 'GET' && url.pathname === '/') {
      const mode = url.searchParams.get('hub.mode');
      const token = url.searchParams.get('hub.verify_token');
      const challenge = url.searchParams.get('hub.challenge');
      if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
        return new Response(challenge, { status: 200 });
      }
      return new Response('Forbidden', { status: 403 });
    }

    // Health
    if (req.method === 'GET' && url.pathname === '/health') {
      return json({
        ok: true,
        have: {
          WA_TOKEN: !!env.WA_TOKEN,
          PHONE_ID: !!env.PHONE_ID,
          VERIFY_TOKEN: !!env.VERIFY_TOKEN,
          SUPABASE_URL: !!env.SUPABASE_URL,
          SUPABASE_ANON_KEY: !!env.SUPABASE_ANON_KEY,
          GCAL_REFRESH_TOKEN: !!env.GCAL_REFRESH_TOKEN,
          TZ: env.TZ || 'America/Mexico_City'
        },
        now: new Date().toISOString()
      });
    }

    // Cron manual
    if (req.method === 'POST' && url.pathname === '/cron') {
      const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
      if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
      const out = await cronReminders(env);
      return new Response(`cron ok ${JSON.stringify(out)}`, { status: 200 });
    }

    // Webhook principal
    if (req.method === 'POST' && url.pathname === '/') {
      try {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw, msgType, ts, media } = ctx;

        // Sesión
        const now = new Date();
        let session = await loadSession(env, from);
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;

        // Nombre por defecto
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // Idempotencia básica
        const msgTs = Number(ts || Date.now());
        let lastTs = Number(session?.data?.last_ts || 0);
        const nowMs = Date.now();
        if (lastTs > nowMs + 10 * 60 * 1000) lastTs = 0;
        if (lastTs && (msgTs + 5000) <= lastTs) return ok('EVENT_RECEIVED');
        session.data.last_ts = Math.max(msgTs, lastTs);
        const lastMid = session?.data?.last_mid || null;
        if (lastMid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // Mensaje no texto
        if (msgType === 'audio') {
          await forwardAudioToSupport(env, media, fromE164);
          await sendWhatsAppText(env, fromE164, 'Lo siento, todavía no puedo escuchar audios. Ya avisé a soporte y en breve te contactan. Si quieres, escríbeme el detalle y te ayudo por aquí 🙂');
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, '¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂');
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // Texto normalizado
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text);

        // Saludo: limpia estado de ventas viejo
        if (RX_GREET.test(lowered)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          session.data.last_candidate = null;
          session.data.cart = [];
          session.stage = 'idle';
          await saveSession(env, session, now);
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          return ok('EVENT_RECEIVED');
        }

        // Etapas
        if (session.stage === 'ask_qty')       return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open')     return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage && session.stage.startsWith('collect_')) {
          return await handleCollectSequential(env, session, fromE164, text, now);
        }
        if (session.stage?.startsWith('sv_')) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, {});
        }

        // Intenciones
        const supportIntent = isSupportIntent(ntext);
        const salesIntent   = isSalesIntent(ntext);

        // Comandos soporte
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

        // Soporte primero
        if (supportIntent) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
        }

        // Ventas
        if (salesIntent) {
          session.data.last_stage = session.stage;
          session.stage = 'idle';
          await saveSession(env, session, now);
          return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
        }

        // Fallback
        await sendWhatsAppText(env, fromE164, 'Te leo. Puedo cotizar consumibles/refacciones o agendar *soporte técnico*. ¿Qué necesitas?');
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

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env) {
    try { await cronReminders(env); } catch (e) { console.error('cron error', e); }
  }
};

/* ---------- Utils ---------- */
function ok(s='ok'){ return new Response(s, { status: 200 }); }
function json(obj){ return new Response(JSON.stringify(obj), { status: 200, headers:{'Content-Type':'application/json'} }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
async function cronReminders(){ return { ok:true }; }

const firstWord   = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function priceWithIVA(n){ return `${formatMoneyMXN(Number(n||0))} + IVA`; }

/* ---------- Intents ---------- */
const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;

function normalizeWithAliases(s=''){
  const t = normalizeBase(s);
  const aliases = [
    ['verzan','versant'], ['versan','versant'], ['versa link','versalink'], ['alta link','altalink'],
    ['docu color','docucolor'], ['prime link','primelink'], ['fuji film','fujifilm']
  ];
  let out = t;
  for (const [bad, good] of aliases) out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  return out;
}

const RX_INV_KWS = /(toner|t[óo]ner|cartucho|developer|unidad de revelado|refacci[oó]n|precio|costo|sku|pieza|compatible)\b/i;
const RX_MODEL_KWS = /(versant|docucolor|primelink|versalink|altalink|apeos|work ?center|workcentre|c\d{2,4}|b\d{2,4}|550|560|570|2100|180|280|4100|c70|c60|c75)\b/i;
function isSalesIntent(ntext=''){ return RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext); }

function isSupportIntent(ntext='') {
  const t = `${ntext}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|mancha|l[ií]nea|ruido)/.test(t);
  const hasDevice  = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  return (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_NEG_NO = /\b(no|nel|ahorita no|no gracias)\b/i;
const RX_DONE   = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;

/* ---------- WhatsApp send ---------- */
async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) return;
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = { messaging_product: 'whatsapp', to: toE164.replace(/\D/g, ''), text: { body } };
  const r = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${env.WA_TOKEN}`, 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!r.ok) console.warn('sendWhatsAppText', r.status, await r.text());
}

async function notifySupport(env, body) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  await sendWhatsAppText(env, to, `🛎️ *Soporte*\n${body}`);
}
async function forwardAudioToSupport(env, media, fromE164) {
  const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
  if (!to) return;
  const txt = `Audio recibido de ${fromE164} (id:${media?.id || 'N/A'}). Revísalo en WhatsApp Business / Meta.`;
  await sendWhatsAppText(env, to, `🗣️ *Audio reenviado*\n${txt}`);
}

/* ---------- Sesión (Supabase) ---------- */
async function loadSession(env, phone) {
  try {
    const r = await sbGet(env, 'wa_session', {
      query: `select=from,stage,data,updated_at,expires_at&from=eq.${encodeURIComponent(phone)}&order=updated_at.desc&limit=1`
    });
    if (Array.isArray(r) && r[0]) return { from: r[0].from, stage: r[0].stage || 'idle', data: r[0].data || {} };
    return { from: phone, stage: 'idle', data: {} };
  } catch {
    return { from: phone, stage: 'idle', data: {} };
  }
}
async function saveSession(env, session, now = new Date()) {
  try {
    await sbUpsert(env, 'wa_session', [{
      from: session.from, stage: session.stage || 'idle', data: session.data || {}, updated_at: now.toISOString()
    }], { onConflict: 'from', returning: 'minimal' });
  } catch {}
}

/* ---------- Cantidades ---------- */
const NUM_WORDS = { 'cero':0,'una':1,'uno':1,'un':1,'dos':2,'tres':3,'cuatro':4,'cinco':5,'seis':6,'siete':7,'ocho':8,'nueve':9,'diez':10,'once':11,'doce':12,'docena':12,'media':0.5,'media docena':6 };
function looksLikeQuantityStrict(t=''){ const hasDigit=/\b\d+\b/.test(t); const hasWord=Object.keys(NUM_WORDS).some(w=>new RegExp(`\\b${w}\\b`,'i').test(t)); return hasDigit||hasWord; }
function parseQty(text, fallback=1){
  const t = normalizeBase(text);
  if (/\bmedia\s+docena\b/i.test(t)) return 6;
  if (/\bdocena\b/i.test(t)) return 12;
  for (const [w,n] of Object.entries(NUM_WORDS)) if (new RegExp(`\\b${w}\\b`,'i').test(t)) return Number(n);
  const m = t.match(/\b(\d+)\b/); if (m) return Number(m[1]);
  return fallback;
}

/* ---------- Inventario & Carrito ---------- */
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
  if (take > 0) pushCart(session, product, take, false);
  if (rest > 0) pushCart(session, product, rest, true);
}
function renderProducto(p, queryText=''){
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  const q = normalizeWithAliases(queryText);
  const famHit = /(versant|docu\s*color|primelink|versa\s*link|alta\s*link|apeos|c\d{2,4}|b\d{2,4})/i.test(q);
  const tip = famHit ? `\n\nEste suele ser el indicado para tu equipo.` : '';
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}${tip}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${Math.max(0, s)} en stock y el resto sería *sobre pedido*.`;
}

/* Etapas de carrito */
async function handleAskQty(env, session, toE164, text, lowered){
  const cand = session.data?.last_candidate;

  if (RX_NEG_NO.test(lowered)) {
    session.data.last_candidate = null;
    session.stage = 'cart_open';
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, 'Sin problema. ¿Busco otra opción? Dime modelo/color o lo que necesitas.');
    return ok('EVENT_RECEIVED');
  }

  if (RX_DONE.test(lowered)) {
    const cart = session.data?.cart || [];
    if (cart.length > 0) {
      session.stage = 'await_invoice';
      await saveSession(env, session);
      await sendWhatsAppText(env, toE164, '¿La cotizamos *con factura* o *sin factura*?');
      return ok('EVENT_RECEIVED');
    }
  }

  if (!cand) {
    session.stage = 'cart_open';
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, '¿Qué artículo necesitas? (modelo/color, p. ej. "tóner amarillo versant")');
    return ok('EVENT_RECEIVED');
  }

  if (!looksLikeQuantityStrict(lowered)) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(env, toE164, `¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    await saveSession(env, session);
    return ok('EVENT_RECEIVED');
  }

  const qty = parseQty(lowered, 1);
  if (!Number.isFinite(qty) || qty <= 0) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(env, toE164, `Necesito un número de piezas (hay ${s} en stock).`);
    await saveSession(env, session);
    return ok('EVENT_RECEIVED');
  }

  addWithStockSplit(session, cand, qty);
  session.stage = 'cart_open';
  await saveSession(env, session);

  const s = numberOrZero(cand.stock);
  const bo = Math.max(0, qty - Math.min(s, qty));
  const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} sobre pedido)` : '';
  await sendWhatsAppText(env, toE164, `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${nota}\n\n¿Deseas *agregar algo más* o *finalizamos*?`);
  return ok('EVENT_RECEIVED');
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext){
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate) addWithStockSplit(session, session.data.last_candidate, 1);
    session.stage = 'await_invoice';
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, '¿La quieres *con factura* o *sin factura*?');
    return ok('EVENT_RECEIVED');
  }

  if (looksLikeQuantityStrict(lowered) && session.data?.last_candidate) {
    session.stage = 'ask_qty';
    await saveSession(env, session);
    const s = numberOrZero(session.data.last_candidate.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  if (RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext)) {
    return await startSalesFromQuery(env, session, toE164, text, ntext);
  }

  await sendWhatsAppText(env, toE164, 'Te leo 🙂. Puedo agregar un artículo nuevo, buscar otro o *finalizar* si ya está completo.');
  await saveSession(env, session);
  return ok('EVENT_RECEIVED');
}

async function startSalesFromQuery(env, session, toE164, text, ntext){
  session.data.last_candidate = null; // limpia candidatos viejos
  await saveSession(env, session);

  const best = await findBestProduct(env, ntext);
  if (best) {
    session.stage = 'ask_qty';
    session.data.last_candidate = best;
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, renderProducto(best, ntext));
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, 'No encontré una coincidencia directa 😕. ¿Me das el *modelo* y el *color* del consumible? (ej. *tóner amarillo Versant 180*)');
  await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
  await saveSession(env, session);
  return ok('EVENT_RECEIVED');
}

/* ---------- Matching ---------- */
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
    yellow:[/\bamarillo\b/i, /\byellow\b/i, /(^|[\s\-_\/])y($|[\s\-_\/])/i],
    magenta:[/\bmagenta\b/i, /(^|[\s\-_\/])m($|[\s\-_\/])/i],
    cyan:[/\bcyan\b/i, /\bcian\b/i, /(^|[\s\-_\/])c($|[\s\-_\/])/i],
    black:[/\bnegro\b/i, /\bblack\b/i, /(^|[\s\-_\/])k($|[\s\-_\/])/i, /(^|[\s\-_\/])bk($|[\s\-_\/])/i],
  };
  const arr = map[colorCode] || [];
  return arr.some(rx => rx.test(p?.nombre) || rx.test(p?.sku) || rx.test(s));
}
function productMatchesFamily(p, text=''){
  const t = normalizeWithAliases(text);
  const s = normalizeBase([p?.nombre, p?.sku, p?.marca, p?.compatible, p?.tipo].join(' '));
  const fams = [
    ['versant', /(versant|80|180|2100|280|4100)\b/i, /(docu\s*color|primelink|alta\s*link|versa\s*link|c(60|70|75)|550|560|570)/i],
    ['docucolor', /(docu\s*color|550|560|570)\b/i, /(versant|primelink|alta\s*link|versa\s*link|2100|180|280|4100)/i],
    ['primelink', /(prime\s*link|primelink)\b/i, /(versant|versa\s*link|alta\s*link)/i],
    ['versalink', /(versa\s*link|versalink)\b/i, /(versant|prime\s*link|alta\s*link)/i],
    ['altalink', /(alta\s*link|altalink)\b/i, /(versant|prime\s*link|versa\s*link)/i],
    ['apeos', /\bapeos\b/i, null],
    ['c70', /\bc(60|70|75)\b/i, null]
  ];
  for (const [, hit, bad] of fams) {
    if (hit.test(t)) {
      if (bad && bad.test(s)) return false;
      return hit.test(s);
    }
  }
  return true;
}

async function findBestProduct(env, queryText) {
  const colorCode = extractColorWord(queryText);
  const forceToner = /(toner|t[óo]ner|cartucho)/i.test(queryText) || !!colorCode;

  const mustBeToner = (p) => {
    if (!forceToner) return true;
    const nom = `${p?.nombre || ''} ${p?.tipo || ''}`;
    return /\bton(e|é)r|t[oó]ner|cartucho/i.test(nom);
  };

  const scoreAndPick = (arr=[]) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = arr.slice();

    // Toner estricto cuando corresponde
    pool = pool.filter(mustBeToner);

    // Familia
    pool = pool.filter(p => productMatchesFamily(p, queryText));

    // Color obligatorio si viene en la consulta
    if (colorCode) pool = pool.filter(p => productHasColor(p, colorCode));

    if (!pool.length) return null;

    // Prioriza stock y luego menor precio
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });
    return pool[0] || null;
  };

  // 1) RPC fuzzy (si existe)
  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 80 }) || [];
    const pick1 = scoreAndPick(res);
    if (pick1) return pick1;
  } catch {}

  // 2) LIKE directo a la vista de inventario
  try {
    const like = forceToner ? encodeURIComponent('%toner%') : encodeURIComponent('%');
    const r2 = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=800`
    }) || [];
    const pick2 = scoreAndPick(r2);
    if (pick2) return pick2;
  } catch {}

  return null;
}

/* ---------- FAQs / cierre venta ---------- */
async function maybeFAQ(env, ntext) {
  const faqs = {
    'horario': 'Horario de atención: Lunes a Viernes 9:00-18:00, Sábados 9:00-14:00',
    'ubicacion': 'Estamos en Av. Tecnológico #123, Industrial, Monterrey, NL',
    'contacto': 'Tel: 461 230 4861 | Email: ventas@cpdigital.com.mx',
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

async function handleAwaitInvoice(env, session, toE164, lowered){
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, 'Perfecto, quedo al pendiente. Si necesitas algo más, aquí estoy 🙂');
    return ok('EVENT_RECEIVED');
  }

  const saysNo  = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};

  if (saysYes || saysNo) {
    session.data.requires_invoice = !!saysYes;
    await preloadCustomerIfAny(env, session);
    const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
    const need = firstMissing(list, session.data.customer);
    if (need) {
      session.stage = `collect_${need}`;
      await saveSession(env, session);
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
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más en este momento? (Sí / No)`);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  await saveSession(env, session);
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
async function handleCollectSequential(env, session, toE164, text){
  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;
  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = session.stage.replace('collect_','');
  c[field] = parseCustomerFragment(field, text);
  if (field==='cp' && !c.ciudad) {
    const info = await cityFromCP(env, c.cp);
    if (info) { c.ciudad = info.ciudad || info.municipio || c.ciudad; c.estado = info.estado || c.estado; }
  }
  await saveSession(env, session);
  const nextField = firstMissing(list, c);
  if (nextField){
    session.stage = `collect_${nextField}`;
    await saveSession(env, session);
    await sendWhatsAppText(env, toE164, `¿${LABEL[nextField]}?`);
    return ok('EVENT_RECEIVED');
  }
  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) {
    await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
    await notifySupport(env, `Nuevo pedido ${c.nombre ? '# ' + c.nombre : ''} (${toE164})`);
  } else {
    await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
    await notifySupport(env, `Pedido (parcial) ${toE164}. Error: ${res?.error || 'N/A'}`);
  }
  session.stage = 'idle';
  session.data.cart = [];
  await saveSession(env, session);
  await sendWhatsAppText(env, toE164, `¿Puedo ayudarte con algo más? (Sí / No)`);
  return ok('EVENT_RECEIVED');
}

/* ---------- Cliente & Pedido ---------- */
async function preloadCustomerIfAny(env, session){
  try{
    const r = await sbGet(env, 'cliente', { query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${session.from}&limit=1` });
    if (r && r[0]) session.data.customer = { ...(session.data.customer||{}), ...r[0] };
  }catch{}
}
async function ensureClienteFields(env, cliente_id, c){
  try{
    const patch = {};
    ['nombre','rfc','email','calle','numero','colonia','ciudad','estado','cp'].forEach(k=>{ if (truthy(c[k])) patch[k]=c[k]; });
    if (Object.keys(patch).length>0) await sbPatch(env, 'cliente', patch, `id=eq.${cliente_id}`);
  }catch{}
}
async function createOrderFromSession(env, session){
  try {
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok: false, error: 'empty cart' };
    const c = session.data.customer || {};
    let cliente_id = null;

    try {
      const exist = await sbGet(env, 'cliente', { query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email || '')})&limit=1` });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch {}

    if (!cliente_id) {
      const ins = await sbUpsert(env, 'cliente', [{
        nombre: c.nombre || null, rfc: c.rfc || null, email: c.email || null, telefono: session.from || null,
        calle: c.calle || null, numero: c.numero || null, colonia: c.colonia || null, ciudad: c.ciudad || null,
        estado: c.estado || null, cp: c.cp || null
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

    // Decremento (solo lo disponible)
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = numberOrZero(row?.[0]?.stock);
        const toDec = Math.min(current, Number(it.qty||0));
        if (toDec > 0) await sbRpc(env, 'decrement_stock', { in_sku: sku, in_by: toDec });
      } catch {}
    }

    return { ok: true, pedido_id, total };
  } catch (e) {
    console.warn('createOrderFromSession', e);
    return { ok: false, error: String(e) };
  }
}

/* ---------- Supabase helpers ---------- */
async function sbGet(env, table, { query }){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, { headers: { apikey: env.SUPABASE_ANON_KEY, Authorization: `Bearer ${env.SUPABASE_ANON_KEY}` } });
  if (!r.ok) { console.warn('sbGet', table, await r.text()); return null; }
  return await r.json();
}
async function sbUpsert(env, table, rows, { onConflict, returning='representation' }={}){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${onConflict?`?on_conflict=${onConflict}`:''}`;
  const r = await fetch(url, {
    method:'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json', Prefer:`resolution=merge-duplicates,return=${returning}`
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
      apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json', Prefer:'return=minimal'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', table, await r.text());
}
async function sbRpc(env, fn, params){
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method:'POST',
    headers: { apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`, 'Content-Type':'application/json' },
    body: JSON.stringify(params||{})
  });
  if (!r.ok) { console.warn('sbRpc', fn, await r.text()); return null; }
  return await r.json();
}

/* ---------- SEPOMEX ---------- */
async function cityFromCP(env, cp){
  try { const r = await sbGet(env, 'sepomex_cp', { query: `cp=eq.${encodeURIComponent(cp)}&select=cp,estado,municipio,ciudad&limit=1` }); return r?.[0] || null; }
  catch { return null; }
}

/* =============================== SOPORTE + GCal ============================ */
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
  Object.assign(out, parseAddressLoose(text));
  const d = parseCustomerText(text);
  if (d.calle) out.calle = d.calle;
  if (d.numero) out.numero = d.numero;
  if (d.colonia) out.colonia = d.colonia;
  if (d.cp) out.cp = d.cp;
  if (d.ciudad) out.ciudad = d.ciudad;
  if (d.estado) out.estado = d.estado;
  if (d.nombre) out.nombre = d.nombre;
  if (d.email) out.email = d.email;
  return out;
}
function parseAddressLoose(text=''){
  const out = {};
  const mcp = text.match(/\bcp\s*(\d{5})\b/i) || text.match(/\b(\d{5})\b/);
  if (mcp) out.cp = mcp[1];
  const calle = text.match(/\bcalle\s+([a-z0-9\s\.#\-]+)\b/i); if (calle) out.calle = clean(calle[1]);
  const num = text.match(/\bn[uú]mero\s+(\d+[A-Z]?)\b/i);    if (num) out.numero = num[1];
  const col = text.match(/\bcolonia\s+([a-z0-9\s\.\-]+)\b/i); if (col) out.colonia = clean(col[1]);
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
async function handleSupport(env, session, toE164, text, lowered, ntext, now){
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
    ['marca','modelo','falla','calle','numero','colonia','cp','ciudad','estado','error_code','prioridad','nombre','email']
      .forEach(k=>{ if (!truthy(sv[k]) && truthy(mined[k])) sv[k]=mined[k]; });

    if (!sv._welcomed) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Te ayudo con soporte técnico 👨‍🔧. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* de la falla.`);
    }

    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;

    const needed = [];
    if (!(truthy(sv.marca) && truthy(sv.modelo))) needed.push('modelo');
    if (!truthy(sv.falla))  needed.push('falla');
    if (!truthy(sv.calle))  needed.push('calle');
    if (!truthy(sv.numero)) needed.push('numero');
    if (!truthy(sv.colonia))needed.push('colonia');
    if (!truthy(sv.cp))     needed.push('cp');
    if (!truthy(sv.nombre)) needed.push('nombre');
    if (!truthy(sv.email))  needed.push('email');
    if (!sv.when?.start)    needed.push('horario');

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

    // Agenda GCal + OS
    let pool = [];
    try { pool = await getCalendarPool(env) || []; } catch(e){ console.warn('[GCal] pool', e); }
    const candidates = pool.filter(p => p.active !== false).slice(0,2);
    const cal = candidates[0] || pool[0] || null;
    const tz = env.TZ || 'America/Mexico_City';
    const when = parseNaturalDateTime(lowered, env) || sv.when || { start:new Date().toISOString(), end:new Date(Date.now()+60*60*1000).toISOString() };
    const slot = clampToWindow(when, tz);

    const cliente_id = await upsertClienteByPhone(env, session.from);
    try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email, calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp }); } catch {}

    let event = null; let calName = cal?.name || '';
    if (cal && env.GCAL_REFRESH_TOKEN && env.GCAL_CLIENT_ID && env.GCAL_CLIENT_SECRET) {
      try {
        const nearest = await findNearestFreeSlot(env, cal.calendar_id, slot, tz);
        const osTmpNumber = Math.floor(Date.now()/1000);
        const summary = `${sv.nombre || 'Cliente'} — ${sv.modelo || 'Equipo'} — OS#${osTmpNumber}`;
        event = await gcalCreateEvent(env, cal.calendar_id, {
          summary,
          description: renderOsDescription(session.from, sv),
          start: nearest.start, end: nearest.end, timezone: tz
        });
        slot.start = nearest.start; slot.end = nearest.end;
      } catch (e) { console.warn('[GCal] create error', e); }
    }

    let osId = null; let estado = event ? 'agendado' : 'pendiente';
    try {
      const osBody = [{
        cliente_id, marca: sv.marca || null, modelo: sv.modelo || null, falla_descripcion: sv.falla || null,
        prioridad: sv.prioridad || 'media', estado,
        ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(),
        gcal_event_id: event?.id || null, calendar_id: cal?.calendar_id || null,
        calle: sv.calle || null, numero: sv.numero || null, colonia: sv.colonia || null, ciudad: sv.ciudad || null, estado: sv.estado || null, cp: sv.cp || null,
        created_at: new Date().toISOString()
      }];
      const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
      osId = os?.data?.[0]?.id || null;
      if (event && osId) {
        const pretty = `${sv.nombre || 'Cliente'} — ${sv.modelo || 'Equipo'} — OS#${osId}`;
        await gcalPatchEvent(env, cal.calendar_id, event.id, { summary: pretty });
      }
    } catch (e) { console.warn('[Supabase] OS upsert', e); estado = 'pendiente'; }

    if (event) {
      await sendWhatsAppText(env, toE164,
        `¡Listo! Agendé tu visita 🙌\n*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*\nDirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}\nTécnico asignado: ${calName || 'por confirmar'}.\n\nSi necesitas reprogramar o cancelar, dímelo con confianza.`
      );
      session.stage = 'sv_scheduled';
    } else {
      await sendWhatsAppText(env, toE164, `Tomé tus datos ✍️. En breve te confirmo el horario exacto por este medio.`);
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
function displayFieldSupport(k){
  const map = {
    modelo:'marca y modelo', falla:'descripción breve de la falla', nombre:'Nombre o Razón Social', email:'email',
    calle:'calle', numero:'número', colonia:'colonia', ciudad:'ciudad o municipio', estado:'estado', cp:'código postal', horario:'día y hora (10:00–15:00)'
  };
  return map[k]||k;
}
function svFillFromAnswer(sv, field, text, env){
  const pm = parseBrandModel(text);
  if (field === 'modelo') { if (pm.marca) sv.marca = pm.marca; if (pm.modelo) sv.modelo = pm.modelo; if (!sv.modelo) sv.modelo = clean(text); return; }
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
async function getCalendarPool(env) {
  try {
    return await sbGet(env, 'calendar_pool', { query: 'select=name,calendar_id,active&active=is.true&order=created_at.asc' }) || [];
  } catch { return []; }
}
async function upsertClienteByPhone(env, phone) {
  try {
    const found = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (found && found[0]) return found[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.data?.[0]?.id || null;
  } catch { return null; }
}
async function getLastOpenOS(env, phone) {
  try {
    const qCli = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    const cid = qCli?.[0]?.id;
    if (!cid) return null;
    const os = await sbGet(env, 'orden_servicio', {
      query: `select=id,cliente_id,estado,ventana_inicio,ventana_fin,gcal_event_id,calendar_id&cliente_id=eq.${cid}&` +
             `or=(estado.eq.agendado,estado.eq.pendiente)&order=ventana_inicio.desc&limit=1`
    });
    return os?.[0] || null;
  } catch { return null; }
}
function renderOsDescription(from, sv) {
  return [
    `WhatsApp: ${from}`,
    `Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
    `Falla: ${sv.falla || ''}`,
    sv.error_code ? `Código de error: ${sv.error_code}` : null,
    `Dirección: ${[sv.calle, sv.numero, sv.colonia, sv.cp, sv.ciudad || '', sv.estado || ''].filter(Boolean).join(' ')}`,
    `Prioridad: ${sv.prioridad || 'media'}`
  ].filter(Boolean).join('\n');
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
  const slot = clampToWindow(when, tz);
  if (os.gcal_event_id && os.calendar_id) {
    try {
      const nearest = await findNearestFreeSlot(env, os.calendar_id, slot, tz);
      await gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, { start: nearest.start, end: nearest.end, timezone: tz });
      slot.start = nearest.start; slot.end = nearest.end;
    } catch (e) { console.warn('[GCal] resched', e); }
  }
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, ventana_inicio: slot.start, ventana_fin: slot.end, estado: 'agendado' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `Actualicé tu visita: *${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*.`);
}
async function svWhenIsMyVisit(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita activa. Si gustas la agendamos 🙂`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164, `Tu visita está para *${fmtDate(os.ventana_inicio, tz)}* de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*.`);
}

/* --------- GCal OAuth + API --------- */
async function gcalAccessToken(env) {
  const url = 'https://oauth2.googleapis.com/token';
  const body = {
    client_id: env.GCAL_CLIENT_ID,
    client_secret: env.GCAL_CLIENT_SECRET,
    grant_type: 'refresh_token',
    refresh_token: env.GCAL_REFRESH_TOKEN
  };
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(`gcal token: ${r.status} ${await r.text()}`);
  const j = await r.json();
  return j.access_token;
}
async function gcalCreateEvent(env, calendarId, { summary, description, start, end, timezone }) {
  const token = await gcalAccessToken(env);
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  const body = {
    summary, description,
    start: { dateTime: new Date(start).toISOString(), timeZone: timezone },
    end:   { dateTime: new Date(end).toISOString(),   timeZone: timezone }
  };
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`gcal create: ${r.status} ${await r.text()}`);
  return await r.json();
}
async function gcalPatchEvent(env, calendarId, eventId, { summary, description, start, end, timezone }) {
  const token = await gcalAccessToken(env);
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const body = {};
  if (summary) body.summary = summary;
  if (description) body.description = description;
  if (start && end) {
    body.start = { dateTime: new Date(start).toISOString(), timeZone: timezone };
    body.end   = { dateTime: new Date(end).toISOString(),   timeZone: timezone };
  }
  const r = await fetch(url, {
    method: 'PATCH',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`gcal patch: ${r.status} ${await r.text()}`);
  return await r.json();
}
async function gcalDeleteEvent(env, calendarId, eventId) {
  try {
    const token = await gcalAccessToken(env);
    const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
    const r = await fetch(url, { method: 'DELETE', headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) console.warn('gcal delete', r.status, await r.text());
  } catch (e) { console.warn('gcalDeleteEvent', e); }
}

/* --------- Horarios --------- */
function parseNaturalDateTime(text, env) {
  const tz = env?.TZ || 'America/Mexico_City';
  const now = new Date();
  const lower = (text || '').toLowerCase();
  let d = new Date(now);
  if (/\bpasado\s*ma[ñn]ana\b/.test(lower)) d.setDate(d.getDate() + 2);
  else if (/\bma[ñn]ana\b/.test(lower)) d.setDate(d.getDate() + 1);
  else if (/\bhoy\b/.test(lower)) { /* same */ }
  let hh = 12, mm = 0;
  const m1 = lower.match(/\b(\d{1,2})[:\.](\d{2})\b/);
  const m2 = lower.match(/\b(?:a\s*las\s*)?(\d{1,2})\b/);
  if (m1) { hh = Number(m1[1]); mm = Number(m1[2]); }
  else if (m2) { hh = Number(m2[1]); }
  if (/\bpm\b/.test(lower) && hh < 12) hh += 12;
  if (/\bam\b/.test(lower) && hh === 12) hh = 0;
  const start = new Date(d.getFullYear(), d.getMonth(), d.getDate(), hh, mm);
  const end = new Date(start.getTime() + 60 * 60 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}
function clampToWindow(when) {
  const start = new Date(when.start);
  const end = new Date(when.end);
  const sh = start.getHours();
  if (sh < 10) { start.setHours(10, 0); end.setTime(start.getTime() + 60 * 60 * 1000); }
  if (sh > 15) { start.setHours(15, 0); end.setTime(start.getTime() + 60 * 60 * 1000); }
  return { start: start.toISOString(), end: end.toISOString() };
}
function fmtDate(iso, tz) {
  try { return new Intl.DateTimeFormat('es-MX', { timeZone: tz, dateStyle: 'full' }).format(new Date(iso)); }
  catch { return new Date(iso).toDateString(); }
}
function fmtTime(iso, tz) {
  try { return new Intl.DateTimeFormat('es-MX', { timeZone: tz, timeStyle: 'short' }).format(new Date(iso)); }
  catch { return new Date(iso).toLocaleTimeString(); }
}

/* ============================ WhatsApp extractor ============================ */
function extractWhatsAppContext(body) {
  try {
    const entry = body?.entry?.[0];
    const changes = entry?.changes?.[0];
    const value = changes?.value;
    const msg = value?.messages?.[0];
    if (!msg) return null;
    const from = msg.from;
    const fromE164 = `+${from}`;
    const mid = msg.id;
    const profileName = value?.contacts?.[0]?.profile?.name || '';
    const ts = msg.timestamp;
    const type = msg.type;
    if (type === 'text') {
      return { mid, from, fromE164, profileName, ts, msgType: 'text', textRaw: msg.text?.body || '' };
    }
    if (type === 'audio') {
      return { mid, from, fromE164, profileName, ts, msgType: 'audio', media: { id: msg.audio?.id } };
    }
    return { mid, from, fromE164, profileName, ts, msgType: type, textRaw: '' };
  } catch { return null; }
}

/* ============================ GCal Busy Helper ============================ */
async function findNearestFreeSlot(env, calendarId, slot, tz) {
  // Implementación simple: usa el slot tal cual; puedes mejorar consultando busy API
  return { start: slot.start, end: slot.end };
}

