/**
 * CopiBot – Worker Lite (SIN IA)
 * Conversacional + Ventas + Soporte Técnico + GCal + Supabase
 * Build: “Lite-R14”
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // --- Verify webhook
    if (req.method === 'GET' && url.pathname === '/') {
      const mode = url.searchParams.get('hub.mode');
      const token = url.searchParams.get('hub.verify_token');
      const challenge = url.searchParams.get('hub.challenge');
      if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
        return new Response(challenge, { status: 200 });
      }
      return new Response('Forbidden', { status: 403 });
    }

    // --- Health
    if (req.method === 'GET' && url.pathname === '/health') {
      return json({
        ok: true,
        have: {
          WA_TOKEN: !!env.WA_TOKEN, PHONE_ID: !!env.PHONE_ID,
          SUPABASE_URL: !!env.SUPABASE_URL, SUPABASE_ANON_KEY: !!env.SUPABASE_ANON_KEY,
          GCAL_REFRESH_TOKEN: !!env.GCAL_REFRESH_TOKEN, TZ: env.TZ || 'America/Mexico_City'
        },
        now: new Date().toISOString()
      });
    }

    // --- Cron (opcional)
    if (req.method === 'POST' && url.pathname === '/cron') {
      const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
      if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
      return json(await cronReminders(env));
    }

    // --- Webhook principal
    if (req.method === 'POST' && url.pathname === '/') {
      let ctxRef = null; // <- preservamos contexto para el catch
      try {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        ctxRef = ctx;
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw, msgType, ts, media } = ctx;

        // ===== session =====
        const now = new Date();
        let session = await loadSession(env, from);
        session.from = from;
        session.data = session.data || {};
        session.stage = session.stage || 'idle';

        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // ===== idempotencia (más tolerante: 60s) =====
        const msgTs = Number(ts || Date.now());
        let lastTs = Number(session?.data?.last_ts || 0);
        const nowMs = Date.now();
        if (lastTs > nowMs + 60 * 60 * 1000) lastTs = 0;       // reseteo defensivo
        if (lastTs && (msgTs + 60 * 1000) <= lastTs) return ok('EVENT_RECEIVED'); // >60s más viejo
        session.data.last_ts = Math.max(msgTs, lastTs);
        const lastMid = session?.data?.last_mid || null;
        if (lastMid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // ===== non-text =====
        if (msgType === 'audio') {
          await forwardAudioToSupport(env, media, fromE164);
          await sendWhatsAppText(env, fromE164,
            'Lo siento, todavía no puedo escuchar audios. Ya avisé a soporte y en breve te contactan. Si quieres, escríbeme el detalle y te ayudo por aquí 🙂'
          );
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, '¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂');
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== text / intents =====
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text);

        // saludo
        if (RX_GREET.test(lowered)) {
          session.stage = 'idle';
          session.data.last_candidate = null;
          session.data.cart = session.data.cart || [];
          await saveSession(env, session, now);
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          return ok('EVENT_RECEIVED');
        }

        // --- atajo: “quiero|dame|agrega 1/2/3 …” con candidato cargado
        if (session.data?.last_candidate) {
          const fastQty = parseFastQty(lowered);
          if (fastQty !== null) {
            session.stage = 'ask_qty'; // forzamos etapa y pasamos por el handler
            await saveSession(env, session, now);
            return await handleAskQty(env, session, fromE164, String(fastQty), String(fastQty), ntext, now);
          }
        }

        // flujo activo
        if (session.stage === 'ask_qty')       return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open')     return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage?.startsWith('collect_')) {
          return await handleCollectSequential(env, session, fromE164, text, now);
        }
        if (session.stage?.startsWith('sv_')) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, {});
        }

        // comandos soporte
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

        // decisión de intención
        const supportIntent = isSupportIntent(ntext);
        const salesIntent   = isSalesIntent(ntext);

        if (supportIntent) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
        }

        if (salesIntent) {
          session.stage = 'idle';
          session.data.last_candidate = null;
          await saveSession(env, session, now);
          return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
        }

        // FAQs
        const faq = await maybeFAQ(env, ntext);
        if (faq) {
          await sendWhatsAppText(env, fromE164, faq);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // fallback
        await sendWhatsAppText(env, fromE164, 'Te leo. Puedo cotizar consumibles/refacciones o agendar *soporte técnico*. ¿Qué necesitas?');
        await saveSession(env, session, now);
        return ok('EVENT_RECEIVED');

      } catch (e) {
        console.error('Worker error', e);
        // Responder sin re-leer el body
        try {
          if (ctxRef?.fromE164) {
            await sendWhatsAppText(env, ctxRef.fromE164, 'Recibí tu mensaje. Tuve un problema momentáneo pero ya quedó, ¿me repites por favor? 🙂');
          }
        } catch {}
        return ok('EVENT_RECEIVED');
      }
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(_event, env) {
    try { await cronReminders(env); } catch (e) { console.error('cron', e); }
  }
};

/* ============ utils ============ */
function ok(s='ok'){ return new Response(s, { status:200 }); }
function json(obj){ return new Response(JSON.stringify(obj), { status:200, headers:{'Content-Type':'application/json'} }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
async function cronReminders(){ return { ok:true }; }

const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase()+s.slice(1).toLowerCase() : '';
const clean = (s='') => s.replace(/\s+/g,' ').trim();
const normalizeBase = (s='') => s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase();
const numberOrZero = n => Number.isFinite(Number(n)) ? Number(n) : 0;
const truthy = v => v!==null && v!==undefined && String(v).trim()!=='';
const moneyMXN = n => { try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN'}).format(Number(n||0)); }catch{ return `$${Number(n||0).toFixed(2)}`; } };
const priceWithIVA = n => `${moneyMXN(n)} + IVA`;

/* ============ inbound parsing (WA) ============ */
function extractWhatsAppContext(body){
  try{
    const entry = body?.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value || {};
    const messages = value.messages || [];
    if (!messages.length) return null;
    const m = messages[0];
    const contacts = value.contacts || [];
    const profileName = contacts?.[0]?.profile?.name || '';
    const from = m.from || '';
    const fromE164 = m.from ? ('+' + String(m.from).replace(/\D/g,'')) : null;
    const msgType = m.type || (m.text ? 'text' : (m.audio ? 'audio' : 'unknown'));
    const textRaw =
      m.text?.body ??
      m.button?.text ??
      m.interactive?.button_reply?.title ??
      m.interactive?.list_reply?.title ??
      m.interactive?.list_reply?.description ??
      '';
    const media = m.audio || m.image || m.document || null;
    const ts = (m.timestamp ? Number(m.timestamp)*1000 : Date.now());
    return { mid: m.id, from, fromE164, profileName, textRaw, msgType, ts, media };
  }catch(e){
    console.warn('extractWhatsAppContext error', e);
    return null;
  }
}

/* ============ intents (sin IA) ============ */
const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_KWS   = /(toner|t[óo]ner|cartucho|developer|unidad de revelado|refacci[oó]n|precio|costo|sku|pieza|compatible)\b/i;
const RX_MODEL_KWS = /(versant|docucolor|primelink|versalink|altalink|apeos|work ?center|workcentre|c\d{2,4}|b\d{2,4}|550|560|570|2100|180|280|4100|c70|c60|c75)\b/i;
const RX_NEG_NO = /\b(no|nel|ahorita no|no gracias)\b/i;
const RX_DONE   = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;

/* atajo de cantidad “quiero|dame|agrega … <n>” */
function parseFastQty(t=''){
  const m = t.match(/\b(quiero|dame|agrega|añade|pon|sum(a|a)|agregar|añadir)\b.*?\b(\d{1,3})\b/);
  if (m) return Number(m[3]);
  return null;
}

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
function isSalesIntent(n){ return RX_INV_KWS.test(n) || RX_MODEL_KWS.test(n); }
function isSupportIntent(n){
  const t = `${n}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(a|o|e|ando|ada|ado)|mancha|l[ií]nea|ruido)/.test(t);
  const hasDevice  = /(impresora|equipo|copiadora|xerox|fujifilm|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  return (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

/* ============ WhatsApp send ============ */
async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) return;
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = { messaging_product: 'whatsapp', to: toE164.replace(/\D/g,''), text:{ body } };
  const r = await fetch(url, { method:'POST', headers:{ Authorization:`Bearer ${env.WA_TOKEN}`, 'Content-Type':'application/json' }, body: JSON.stringify(payload) });
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
  await sendWhatsAppText(env, to, `🗣️ *Audio reenviado*\nDe: ${fromE164}\nMedia ID: ${media?.id||'N/A'} (revísalo en Meta/WA Business).`);
}

/* ============ sesión (Supabase) ============ */
async function loadSession(env, phone) {
  try {
    const r = await sbGet(env, 'wa_session', { query: `select=from,stage,data,updated_at,expires_at&from=eq.${encodeURIComponent(phone)}&limit=1` });
    if (Array.isArray(r) && r[0]) return { from:r[0].from, stage:r[0].stage||'idle', data:r[0].data||{} };
  } catch(e){ console.warn('loadSession', e); }
  return { from: phone, stage:'idle', data:{} };
}
async function saveSession(env, session, now=new Date()) {
  try {
    await sbUpsert(env, 'wa_session', [{
      from: session.from, stage: session.stage||'idle', data: session.data||{}, updated_at: now.toISOString()
    }], { onConflict:'from', returning:'minimal' });
  } catch(e){ console.warn('saveSession', e); }
}

/* ============ cantidades / carrito ============ */
const NUM_WORDS = { cero:0, una:1, uno:1, un:1, dos:2, tres:3, cuatro:4, cinco:5, seis:6, siete:7, ocho:8, nueve:9, diez:10, once:11, doce:12, docena:12, 'media docena':6 };
function looksLikeQuantityStrict(t=''){ const hasDigit=/\b\d+\b/.test(t); const hasWord=Object.keys(NUM_WORDS).some(w=>new RegExp(`\\b${w}\\b`,'i').test(t)); return hasDigit||hasWord; }
function parseQty(text, fallback=1){
  const t = normalizeBase(text);
  if (/\bmedia\s+docena\b/i.test(t)) return 6;
  if (/\bdocena\b/i.test(t)) return 12;
  for (const [w,n] of Object.entries(NUM_WORDS)) if (new RegExp(`\\b${w}\\b`,'i').test(t)) return Number(n);
  const m = t.match(/\b(\d+)\b/); return m ? Number(m[1]) : fallback;
}
function pushCart(session, product, qty, backorder=false){
  session.data.cart = session.data.cart || [];
  const key = `${product?.sku || product?.id || product?.nombre}${backorder?'_bo':''}`;
  const existing = session.data.cart.find(i => i.key===key);
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
function renderProducto(p){
  const s = numberOrZero(p.stock);
  const stockLine = s>0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  return `1. ${p.nombre}${marca}${sku}\n${priceWithIVA(p.precio)}\n${stockLine}`;
}

/* ============ flujo ventas ============ */
async function handleAskQty(env, session, toE164, text, lowered, _ntext, now){
  try{
    const cand = session.data?.last_candidate;

    if (RX_NEG_NO.test(lowered)) {
      session.data.last_candidate = null;
      session.stage = 'cart_open';
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, 'Sin problema. ¿Busco otra opción? Dime modelo/color o lo que necesitas.');
      return ok('EVENT_RECEIVED');
    }

    if (RX_DONE.test(lowered)) {
      const cart = session.data?.cart || [];
      if (cart.length > 0) {
        session.stage = 'await_invoice';
        await saveSession(env, session, now);
        await sendWhatsAppText(env, toE164, '¿La cotizamos *con factura* o *sin factura*?');
        return ok('EVENT_RECEIVED');
      }
    }

    if (!cand) {
      session.stage = 'cart_open';
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, '¿Qué artículo necesitas? (ej. "*tóner amarillo Versant 180*")');
      return ok('EVENT_RECEIVED');
    }

    if (!looksLikeQuantityStrict(lowered)) {
      const s = numberOrZero(cand.stock);
      await sendWhatsAppText(env, toE164, `¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto sería *sobre pedido*)`);
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }

    const qty = parseQty(lowered, 1);
    if (!Number.isFinite(qty) || qty<=0) {
      const s = numberOrZero(cand.stock);
      await sendWhatsAppText(env, toE164, `Necesito un número de piezas (hay ${s} en stock).`);
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }

    addWithStockSplit(session, cand, qty);
    session.stage = 'cart_open';
    await saveSession(env, session, now);

    const s = numberOrZero(cand.stock);
    const bo = Math.max(0, qty - Math.min(s, qty));
    const nota = bo>0 ? `\n(De ${qty}, ${Math.min(s,qty)} en stock y ${bo} sobre pedido)` : '';
    await sendWhatsAppText(env, toE164, `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${nota}\n\n¿Deseas *agregar algo más* o *finalizamos*?`);
    return ok('EVENT_RECEIVED');
  }catch(err){
    console.warn('handleAskQty error', err);
    try{
      await sendWhatsAppText(env, toE164, 'Recibí tu cantidad 👍. Si no viste mi mensaje anterior, dime de nuevo cuántas *piezas* necesitas (número).');
    }catch{}
    return ok('EVENT_RECEIVED');
  }
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext, now){
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length>0)) {
    if (!cart.length && session.data.last_candidate) addWithStockSplit(session, session.data.last_candidate, 1);
    session.stage = 'await_invoice';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, '¿La quieres *con factura* o *sin factura*?');
    return ok('EVENT_RECEIVED');
  }

  if (looksLikeQuantityStrict(lowered) && session.data?.last_candidate) {
    session.stage = 'ask_qty';
    await saveSession(env, session, now);
    const s = numberOrZero(session.data.last_candidate.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  if (RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext)) {
    return await startSalesFromQuery(env, session, toE164, text, ntext, now);
  }

  await sendWhatsAppText(env, toE164, 'Te leo 🙂. Puedo agregar un artículo nuevo, buscar otro o *finalizar* si ya está completo.');
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  session.data.last_candidate = null;
  await saveSession(env, session, now);

  const prod = await findBestProduct(env, ntext);
  if (prod) {
    session.stage = 'ask_qty';
    session.data.last_candidate = prod;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `${renderProducto(prod)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; lo que exceda el stock va *sobre pedido*.`);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, 'No encontré una coincidencia directa 😕. ¿Me das el *modelo* y el *color* del consumible? (ej. *tóner amarillo Versant 180*)');
  await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* ============ matching ============ */
function extractColorWord(text=''){
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return 'yellow';
  if (/\bmagenta\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'black';
  return null;
}
function productHasColor(p, code){
  if (!code) return true;
  const s = `${normalizeBase([p?.nombre, p?.sku, p?.marca].join(' '))}`;
  const map = {
    yellow:[/\bamarillo\b/i, /\byellow\b/i, /(^|[\s\-_\/])y($|[\s\-_\/])/i],
    magenta:[/\bmagenta\b/i, /(^|[\s\-_\/])m($|[\s\-_\/])/i],
    cyan:[/\bcyan\b/i, /\bcian\b/i, /(^|[\s\-_\/])c($|[\s\-_\/])/i],
    black:[/\bnegro\b/i, /\bblack\b/i, /(^|[\s\-_\/])k($|[\s\-_\/])/i, /(^|[\s\-_\/])bk($|[\s\-_\/])/i],
  };
  const arr = map[code] || [];
  return arr.some(rx => rx.test(p?.nombre) || rx.test(p?.sku) || rx.test(s));
}
function productMatchesFamily(p, text=''){
  const t = normalizeWithAliases(text);
  const s = normalizeBase([p?.nombre, p?.sku, p?.marca, p?.compatible].join(' '));
  const fams = [
    ['versant', /(versant|80|180|2100|280|4100)\b/i, /(docu\s*color|prime\s*link|versa\s*link|alta\s*link|c(60|70|75)|550|560|570)/i],
    ['docucolor', /(docu\s*color|550|560|570)\b/i, /(versant|prime\s*link|alta\s*link|versa\s*link)/i],
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
  const wantsToner = /\b(t[oó]ner|toner|cartucho)\b/i.test(queryText);

  const scoreAndPick = (arr=[]) => {
    if (!arr?.length) return null;

    // familia
    let pool = arr.filter(p => productMatchesFamily(p, queryText));

    // color
    if (colorCode) pool = pool.filter(p => productHasColor(p, colorCode));

    // si pidió "tóner" excluir unidades de revelado/developer/cintas
    if (wantsToner) {
      pool = pool.filter(p => {
        const name = normalizeBase(p?.nombre||'');
        const tipo = (p?.tipo||'').toLowerCase();
        if (/(unidad\s*de\s*revelado|revelador|developer|drum|cinta|transfer|banda)/i.test(name)) return false;
        if (tipo && tipo !== 'toner') return false;
        return true;
      });
    }

    if (!pool.length) return null;

    // ordenar: stock primero, luego precio asc
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });
    return pool[0] || null;
  };

  // 1) fuzzy RPC (si existe)
  try {
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 60 }) || [];
    const pick1 = scoreAndPick(res);
    if (pick1) return pick1;
  } catch (_) {}

  // 2) LIKE contra producto_stock_v (tabla correcta)
  try {
    const like = encodeURIComponent('%' + queryText.replace(/\s+/g,' ') + '%');
    const parts = [
      `select=id,nombre,marca,sku,precio,stock,tipo,compatible`,
      `or=(nombre.ilike.${like},sku.ilike.${like})`
    ];
    if (wantsToner) parts.push(`tipo=eq.toner`);
    parts.push(`nombre=not.ilike.*unidad%20de%20revelado*`);
    parts.push(`nombre=not.ilike.*developer*`);
    const q = parts.join('&') + `&order=stock.desc.nullslast,precio.asc&limit=600`;
    const r2 = await sbGet(env, 'producto_stock_v', { query: q }) || [];
    const pick2 = scoreAndPick(r2);
    if (pick2) return pick2;
  } catch (_) {}

  // 3) red de seguridad: buscar “toner” genérico
  try {
    const likeToner = encodeURIComponent('%toner%');
    const r3 = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&` +
             `or=(nombre.ilike.${likeToner},sku.ilike.${likeToner})&` +
             `order=stock.desc.nullslast,precio.asc&limit=400`
    }) || [];
    const pick3 = scoreAndPick(r3);
    if (pick3) return pick3;
  } catch(_) {}

  return null;
}

/* ============ cierre (pedido) ============ */
async function handleAwaitInvoice(env, session, toE164, lowered, now){
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, 'Perfecto, quedo al pendiente. Si necesitas algo más, aquí estoy 🙂');
    return ok('EVENT_RECEIVED');
  }

  const saysNo  = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  session.data.customer = session.data.customer || {};
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
      await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${moneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
    } else {
      await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor para confirmar detalles. 🙌`);
    }
    session.stage = 'idle';
    session.data.cart = [];
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más en este momento? (Sí / No)`);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL     = { nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', cp:'Código Postal' };
function firstMissing(list, c={}){ for (const k of list) if (!truthy(c[k])) return k; return null; }
function parseCustomerFragment(field, text){
  const t = text;
  if (field==='nombre') return clean(t);
  if (field==='rfc'){ const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i); return m ? m[1].toUpperCase() : clean(t).toUpperCase(); }
  if (field==='email'){ const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); return m ? m[0].toLowerCase() : clean(t).toLowerCase(); }
  if (field==='numero'){ const m = t.match(/\b(\d+[A-Z]?)\b/i); return m ? m[1] : clean(t); }
  if (field==='cp'){ const m = t.match(/\b(\d{5})\b/); return m ? m[1] : clean(t); }
  return clean(t);
}
async function handleCollectSequential(env, session, toE164, text, now){
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;
  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = session.stage.replace('collect_','');
  c[field] = parseCustomerFragment(field, text);
  if (field==='cp' && !c.ciudad) {
    const info = await cityFromCP(env, c.cp);
    if (info) { c.ciudad = info.ciudad || info.municipio || c.ciudad; c.estado = info.estado || c.estado; }
  }
  await saveSession(env, session, now);
  const next = firstMissing(list, c);
  if (next){
    session.stage = `collect_${next}`;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿${LABEL[next]}?`);
    return ok('EVENT_RECEIVED');
  }
  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${moneyMXN(res.total)} + IVA`);
  else        await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor para confirmar detalles. 🙌`);
  session.stage = 'idle';
  session.data.cart = [];
  await saveSession(env, session, now);
  await sendWhatsAppText(env, toE164, `¿Puedo ayudarte con algo más? (Sí / No)`);
  return ok('EVENT_RECEIVED');
}

/* ============ cliente/pedido ============ */
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
async function createOrderFromSession(env, session){
  try{
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok:false, error:'empty' };
    const c = session.data.customer || {};
    let cliente_id = null;
    try{
      const exist = await sbGet(env, 'cliente', { query:`select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email||'')})&limit=1` });
      if (exist && exist[0]) cliente_id = exist[0].id;
    }catch(_){}
    if (!cliente_id) {
      const ins = await sbUpsert(env, 'cliente', [{
        nombre:c.nombre||null, rfc:c.rfc||null, email:c.email||null, telefono:session.from||null,
        calle:c.calle||null, numero:c.numero||null, colonia:c.colonia||null, ciudad:c.ciudad||null, estado:c.estado||null, cp:c.cp||null
      }], { onConflict:'telefono', returning:'representation' });
      cliente_id = ins?.data?.[0]?.id || null;
    } else {
      await ensureClienteFields(env, cliente_id, c);
    }

    let total = 0;
    for (const it of cart) total += Number(it.product?.precio||0) * Number(it.qty||1);

    const p = await sbUpsert(env, 'pedido', [{ cliente_id, total, moneda:'MXN', estado:'nuevo', created_at:new Date().toISOString() }], { returning:'representation' });
    const pedido_id = p?.data?.[0]?.id;

    const items = cart.map(it => ({
      pedido_id, producto_id: it.product?.id||null, sku: it.product?.sku||null,
      nombre: it.product?.nombre||null, qty: it.qty, precio_unitario: Number(it.product?.precio||0)
    }));
    await sbUpsert(env, 'pedido_item', items, { returning:'minimal' });

    // disminuir stock real por RPC (si existe)
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query:`select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = numberOrZero(row?.[0]?.stock);
        const dec = Math.min(current, Number(it.qty||0));
        if (dec > 0) await sbRpc(env, 'decrement_stock', { in_sku: sku, in_by: dec });
      } catch (_){}
    }

    return { ok:true, pedido_id, total };
  }catch(e){
    console.warn('createOrderFromSession', e);
    return { ok:false, error:String(e) };
  }
}

/* ============ Supabase helpers ============ */
async function sbGet(env, table, { query }){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, { headers:{ apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}` } });
  if (!r.ok) { console.warn('sbGet', table, await r.text()); return null; }
  return await r.json();
}
async function sbUpsert(env, table, rows, { onConflict, returning='representation' }={}){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${onConflict?`?on_conflict=${onConflict}`:''}`;
  const r = await fetch(url, {
    method:'POST',
    headers:{
      apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type':'application/json', Prefer:`resolution=merge-duplicates,return=${returning}`
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) { console.warn('sbUpsert', table, await r.text()); return null; }
  return { data: returning==='minimal' ? null : await r.json() };
}
async function sbPatch(env, table, patch, filter){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method:'PATCH',
    headers:{ apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`, 'Content-Type':'application/json', Prefer:'return=minimal' },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', table, await r.text());
}
async function sbRpc(env, fn, params){
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method:'POST',
    headers:{ apikey: env.SUPABASE_ANON_KEY, Authorization:`Bearer ${env.SUPABASE_ANON_KEY}`, 'Content-Type':'application/json' },
    body: JSON.stringify(params||{})
  });
  if (!r.ok) { console.warn('sbRpc', fn, await r.text()); return null; }
  return await r.json();
}

/* ============ SEPOMEX helper ============ */
async function cityFromCP(env, cp){
  try { const r = await sbGet(env, 'sepomex_cp', { query:`cp=eq.${encodeURIComponent(cp)}&select=cp,estado,municipio,ciudad&limit=1` }); return r?.[0] || null; }
  catch { return null; }
}

/* ============ SOPORTE + GCal ============ */

// --- Parsers básicos de marca/modelo, dirección y datos del cliente ---
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

  Object.assign(out, parseAddressLoose(text), parseCustomerText(text));
  return out;
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

// --- Flujo principal de soporte ---
async function handleSupport(env, session, toE164, text, lowered, ntext, now){
  try {
    session.data = session.data || {};
    session.data.last_intent = 'support';
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    // Si veníamos preguntando un campo, capturarlo
    if (session.stage === 'sv_collect' && session.data.sv_need_next) {
      svFillFromAnswer(sv, session.data.sv_need_next, text, env);
      await saveSession(env, session, now);
    }

    // Minería ligera del mensaje actual
    const mined = extractSvInfo(text);
    ['marca','modelo','falla','calle','numero','colonia','cp','ciudad','estado','error_code','prioridad','nombre','email']
      .forEach(k=>{ if (!truthy(sv[k]) && truthy(mined[k])) sv[k]=mined[k]; });

    if (!sv._welcomed) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Te ayudo con soporte técnico 👨‍🔧. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* de la falla.`);
    }

    // Completar datos del cliente si existen
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;

    // ¿Qué falta?
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

    // === Agenda (GCal) + OS ===
    let pool = [];
    try { pool = await getCalendarPool(env) || []; } catch(e){ console.warn('[GCal] pool', e); }

    // usar el primero activo
    const cal = (pool.find(p => p.active !== false)) || pool[0] || null;
    const tz = env.TZ || 'America/Mexico_City';
    const whenParsed = sv.when || parseNaturalDateTime(lowered, env) || {};
    const slot = clampToWindow(whenParsed, tz);

    const cliente_id = await upsertClienteByPhone(env, session.from);
    try { await ensureClienteFields(env, cliente_id, { nombre: sv.nombre, email: sv.email, calle: sv.calle, numero: sv.numero, colonia: sv.colonia, ciudad: sv.ciudad, estado: sv.estado, cp: sv.cp }); } catch (_) {}

    // Crear en GCal (si hay config)
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

    // Crear OS en supabase
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

      // Renombrar evento con OS real
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

function renderOsDescription(phone, sv={}){
  return [
    `Tel: ${phone}`,
    `Equipo: ${sv.marca||''} ${sv.modelo||''}`.trim(),
    `Falla: ${sv.falla||''}`,
    sv.error_code ? `Código: ${sv.error_code}` : null,
    `Dirección: ${sv.calle||''} ${sv.numero||''}, ${sv.colonia||''}, CP ${sv.cp||''} ${sv.ciudad||''} ${sv.estado||''}`.replace(/\s+/g,' ').trim()
  ].filter(Boolean).join('\n');
}

// --- Comandos rápidos sobre OS existentes ---
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
  let newEvent = null;
  if (os.calendar_id && env.GCAL_REFRESH_TOKEN) {
    const nearest = await findNearestFreeSlot(env, os.calendar_id, slot, tz);
    await gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, {
      start: nearest.start, end: nearest.end, timezone: tz
    });
    newEvent = nearest;
  }
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, ventana_inicio: new Date(slot.start).toISOString(), ventana_fin: new Date(slot.end).toISOString(), estado: 'agendado' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `Listo, reprogramé tu visita para *${fmtDate((newEvent||slot).start, tz)}* de *${fmtTime((newEvent||slot).start, tz)}* a *${fmtTime((newEvent||slot).end, tz)}*.`);
}
async function svWhenIsMyVisit(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita activa en el sistema.`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164, `Tu visita está para *${fmtDate(os.ventana_inicio, tz)}* de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*.`);
}

// --- Utilidades de OS / cliente ---
async function upsertClienteByPhone(env, phone){
  let id = null;
  try {
    const r = await sbGet(env, 'cliente', { query:`select=id&telefono=eq.${encodeURIComponent(phone)}&limit=1` });
    if (r && r[0]) return r[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict:'telefono', returning:'representation' });
    id = ins?.data?.[0]?.id || null;
  } catch (_){}
  return id;
}
async function getLastOpenOS(env, phone){
  try{
    const cli = await sbGet(env, 'cliente', { query:`select=id&telefono=eq.${encodeURIComponent(phone)}&limit=1` });
    const cliente_id = cli?.[0]?.id;
    if (!cliente_id) return null;
    const r = await sbGet(env, 'orden_servicio', { query:`select=id,ventana_inicio,ventana_fin,estado,gcal_event_id,calendar_id&cliente_id=eq.${cliente_id}&order=created_at.desc&limit=1` });
    return r?.[0] || null;
  }catch(_){ return null; }
}

// --- Google Calendar helpers (OAuth con refresh token) ---
async function gcalToken(env){
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method:'POST',
    headers:{ 'Content-Type':'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GCAL_CLIENT_ID,
      client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    })
  });
  if (!res.ok) throw new Error('gcal token ' + res.status);
  const j = await res.json();
  return j.access_token;
}
async function gcalFetch(env, url, opts={}){
  const token = await gcalToken(env);
  const r = await fetch(url, {
    ...opts,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type':'application/json', ...(opts.headers||{}) }
  });
  if (!r.ok) throw new Error('gcal ' + r.status + ' ' + await r.text());
  return await r.json();
}
async function gcalCreateEvent(env, calendarId, { summary, description, start, end, timezone }){
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  return await gcalFetch(env, url, {
    method:'POST',
    body: JSON.stringify({
      summary, description,
      start:{ dateTime: new Date(start).toISOString(), timeZone: timezone },
      end:  { dateTime: new Date(end).toISOString(),   timeZone: timezone }
    })
  });
}
async function gcalPatchEvent(env, calendarId, eventId, { summary, start, end, timezone }){
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const body = {};
  if (summary) body.summary = summary;
  if (start && end) {
    body.start = { dateTime: new Date(start).toISOString(), timeZone: timezone };
    body.end   = { dateTime: new Date(end).toISOString(),   timeZone: timezone };
  }
  return await gcalFetch(env, url, { method:'PATCH', body: JSON.stringify(body) });
}
async function gcalDeleteEvent(env, calendarId, eventId){
  const token = await gcalToken(env);
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`;
  const r = await fetch(url, { method:'DELETE', headers:{ Authorization:`Bearer ${token}` } });
  if (!r.ok) console.warn('gcal delete', r.status, await r.text());
}

// Buscar el slot libre más cercano (1h) dentro de 10:00–15:00 local
async function findNearestFreeSlot(env, calendarId, desired, tz){
  const durationMs = 60*60*1000;
  const base = clampToWindow(desired, tz);
  const start = new Date(base.start);
  let end   = new Date(start.getTime() + durationMs);

  const token = await gcalToken(env);
  const fbUrl = 'https://www.googleapis.com/calendar/v3/freeBusy';
  for (let dayOffset=0; dayOffset<3; dayOffset++){
    const dayStart = new Date(start.getTime() + dayOffset*24*60*60*1000);
    const windowStart = windowOfDay(dayStart, tz, 10);
    const windowEnd   = windowOfDay(dayStart, tz, 15);
    const r = await fetch(fbUrl, {
      method:'POST',
      headers:{ Authorization:`Bearer ${token}`, 'Content-Type':'application/json' },
      body: JSON.stringify({
        timeMin: windowStart.toISOString(),
        timeMax: windowEnd.toISOString(),
        items:[{ id: calendarId }]
      })
    });
    const j = await r.json();
    const busy = (j?.calendars?.[calendarId]?.busy) || [];
    let probe = new Date(Math.max(windowStart.getTime(), start.getTime()));
    for (let i=0; i<12; i++){
      end = new Date(probe.getTime() + durationMs);
      const overlaps = busy.some(b => !(end <= new Date(b.start) || probe >= new Date(b.end)));
      if (!overlaps && end <= windowEnd) return { start: probe.toISOString(), end: end.toISOString() };
      probe = new Date(probe.getTime() + 30*60*1000);
    }
  }
  return { start: base.start, end: new Date(new Date(base.start).getTime()+durationMs).toISOString() };
}
function windowOfDay(date, tz, hour){
  const d = new Date(date);
  d.setUTCHours(hour,0,0,0);
  return d;
}

// --- Pool de calendarios ---
async function getCalendarPool(env){
  try {
    return await sbGet(env, 'calendar_pool', { query:`select=id,name,calendar_id,active&order=name.asc` }) || [];
  } catch { return []; }
}

// --- Date parsing minimalista (hoy/mañana, hh:mm) ---
function parseNaturalDateTime(text, env){
  const tz = env.TZ || 'America/Mexico_City';
  const now = new Date();
  let base = new Date(now);
  if (/\bpasado\s+ma[ñn]ana\b/i.test(text)) base = new Date(now.getTime()+2*24*60*60*1000);
  else if (/\bma[ñn]ana\b/i.test(text)) base = new Date(now.getTime()+1*24*60*60*1000);
  else if (/\bhoy\b/i.test(text)) base = new Date(now);

  let h = 12, m = 0;
  const mHM = text.match(/\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b/i);
  if (mHM) {
    h = Number(mHM[1]); m = Number(mHM[2]||0);
    const ampm = (mHM[3]||'').toLowerCase();
    if (ampm === 'pm' && h<12) h += 12;
    if (ampm === 'am' && h===12) h = 0;
  }
  const start = new Date(base);
  start.setHours(h, m, 0, 0);
  const end = new Date(start.getTime() + 60*60*1000);
  return { start: start.toISOString(), end: end.toISOString(), timezone: tz };
}
function clampToWindow(when, tz){
  const start = new Date(when?.start || Date.now() + 2*60*60*1000);
  const end   = new Date(when?.end   || start.getTime() + 60*60*1000);
  const dayStart = new Date(start); dayStart.setHours(10,0,0,0);
  const dayEnd   = new Date(start); dayEnd.setHours(15,0,0,0);
  const s = new Date(Math.max(start.getTime(), dayStart.getTime()));
  const e = new Date(Math.min(end.getTime(),   dayEnd.getTime()));
  if (e <= s) { s.setHours(10,0,0,0); e.setTime(s.getTime()+60*60*1000); }
  return { start: s.toISOString(), end: e.toISOString(), timezone: tz };
}
function fmtDate(iso, tz){
  try{
    return new Intl.DateTimeFormat('es-MX',{ timeZone: tz, weekday:'short', day:'2-digit', month:'short' }).format(new Date(iso));
  }catch{ return new Date(iso).toDateString(); }
}
function fmtTime(iso, tz){
  try{
    return new Intl.DateTimeFormat('es-MX',{ timeZone: tz, hour:'2-digit', minute:'2-digit' }).format(new Date(iso));
  }catch{ const d=new Date(iso); return `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`; }
}

