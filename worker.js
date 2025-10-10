/**
 * CopiBot – Worker Lite (SIN IA)
 * Conversacional + Ventas + Soporte Técnico + GCal + Supabase
 * Build: “Lite-R10”
 *
 * Variables esperadas en Cloudflare:
 *  - WA_TOKEN, PHONE_ID, VERIFY_TOKEN
 *  - SUPABASE_URL, SUPABASE_ANON_KEY
 *  - GCAL_CLIENT_ID, GCAL_CLIENT_SECRET, GCAL_REFRESH_TOKEN
 *  - SUPPORT_WHATSAPP (o SUPPORT_PHONE_E164)
 *  - TZ (ej. America/Mexico_City)
 *  - CRON_SECRET (para /cron)
 *
 * Tablas requeridas (schema public):
 *  - wa_session(from text pk, stage text, data jsonb, updated_at timestamptz, expires_at timestamptz?)
 *  - producto_stock_v (id, nombre, marca, sku, precio, stock, tipo, compatible)
 *  - cliente (id uuid pk, nombre, rfc, email, telefono, calle, numero, colonia, ciudad, estado, cp)
 *  - pedido(id uuid, cliente_id uuid, total numeric, moneda text, estado text, created_at timestamptz)
 *  - pedido_item(pedido_id uuid, producto_id uuid, sku text, nombre text, qty int, precio_unitario numeric)
 *  - orden_servicio(id uuid, cliente_id uuid, marca text, modelo text, falla_descripcion text, prioridad text,
 *                   estado text, ventana_inicio timestamptz, ventana_fin timestamptz,
 *                   gcal_event_id text, calendar_id text,
 *                   calle text, numero text, colonia text, ciudad text, estado text, cp text, created_at timestamptz)
 *  - calendar_pool(id uuid, name text, calendar_id text, active bool)
 *
 * Nota: SIN IA — la intención se detecta con regex robustas.
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // ===== Verificación webhook de WhatsApp =====
    if (req.method === 'GET' && url.pathname === '/') {
      const mode = url.searchParams.get('hub.mode');
      const token = url.searchParams.get('hub.verify_token');
      const challenge = url.searchParams.get('hub.challenge');
      if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
        return new Response(challenge, { status: 200 });
      }
      return new Response('Forbidden', { status: 403 });
    }

    // ===== Health =====
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
      return json({ ok: true, have, now: new Date().toISOString() });
    }

    // ===== Cron manual (opcional) =====
    if (req.method === 'POST' && url.pathname === '/cron') {
      const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
      if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
      const out = await cronReminders(env);
      return new Response(`cron ok ${JSON.stringify(out)}`, { status: 200 });
    }

    // ===== Webhook principal =====
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

        // Autocompletar nombre (si no tenemos)
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // ==== Anti re-orden + Idempotencia ====
        const msgTs = Number(ts || Date.now());
        let lastTs = Number(session?.data?.last_ts || 0);
        const nowMs = Date.now();

        if (lastTs > nowMs + 10 * 60 * 1000) lastTs = 0; // corrupt future
        if (lastTs && (msgTs + 5000) <= lastTs) return ok('EVENT_RECEIVED'); // mensaje viejo -> no responder

        session.data.last_ts = Math.max(msgTs, lastTs);

        const lastMid = session?.data?.last_mid || null;
        if (lastMid === mid) return ok('EVENT_RECEIVED'); // duplicado exacto
        session.data.last_mid = mid;

        // ===== Tipos no-texto =====
        // 1) Audio -> reenviar a soporte + respuesta automática al cliente
        if (msgType === 'audio') {
          await forwardAudioToSupport(env, media, fromE164);
          await sendWhatsAppText(env, fromE164,
            'Lo siento, todavía no puedo escuchar audios. Ya avisé a soporte y en breve te contactan. Si quieres, escríbeme el detalle y te ayudo por aquí 🙂'
          );
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // 2) No texto cualquiera -> pedir texto
        if (msgType !== 'text') {
          await sendWhatsAppText(env, fromE164, '¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂');
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Normalización / intención =====
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text);

        // Saludo: limpiar estado de ventas viejo y saludar
        if (RX_GREET.test(lowered)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          // limpiar estado de ventas viejo
          session.data.last_candidate = null;
          session.data.cart = session.data.cart || [];
          session.stage = 'idle';
          await saveSession(env, session, now);
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          return ok('EVENT_RECEIVED');
        }

        // Etapas activas (ventas/soporte)
        if (session.stage === 'ask_qty')       return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open')     return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage && session.stage.startsWith('collect_')) {
          return await handleCollectSequential(env, session, fromE164, text, now);
        }
        if (session.stage?.startsWith('sv_')) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, {});
        }

        // Intenciones estáticas (SIN IA)
        const supportIntent = isSupportIntent(ntext);
        const salesIntent   = isSalesIntent(ntext);

        // Comandos universales de soporte
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

        // Soporte primero si la frase es de falla
        if (supportIntent) {
          return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
        }

        // Ventas por intención clara
        if (salesIntent) {
          if (session.stage !== 'idle') session.data.last_stage = session.stage;
          session.stage = 'idle';
          await saveSession(env, session, now);
          return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
        }

        // FAQs rápidas
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // Fallback mínimo (sin IA)
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

  // Cron Cloudflare (opcional)
  async scheduled(event, env, ctx) {
    try {
      const out = await cronReminders(env);
      console.log('cron run', out);
    } catch (e) {
      console.error('cron error', e);
    }
  }
};

/* ============================ Utils ============================ */
function ok(s='ok'){ return new Response(s, { status: 200 }); }
function json(obj){ return new Response(JSON.stringify(obj), { status: 200, headers:{'Content-Type':'application/json'} }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
async function cronReminders(env){ return { ok:true, ts: Date.now() }; }

const firstWord   = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function priceWithIVA(n){ return `${formatMoneyMXN(Number(n||0))} + IVA`; }

/* ============================ Intents (SIN IA) ============================ */
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

/* ============================ WhatsApp send ============================ */
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

async function forwardAudioToSupport(env, media, fromE164) {
  try {
    const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
    if (!to) return;
    const txt = `Audio recibido de ${fromE164} (id:${media?.id || 'N/A'}). Revísalo en WhatsApp Business / Meta.`;
    await sendWhatsAppText(env, to, `🗣️ *Audio reenviado*\n${txt}`);
  } catch (e) { console.warn('forwardAudioToSupport', e); }
}

/* ============================ Sesión (Supabase) ============================ */
async function loadSession(env, phone) {
  try {
    const r = await sbGet(env, 'wa_session', {
      query:
        `select=from,stage,data,updated_at,expires_at&` +
        `from=eq.${encodeURIComponent(phone)}&order=updated_at.desc&limit=1`
    });
    if (Array.isArray(r) && r[0]) {
      return { from: r[0].from, stage: r[0].stage || 'idle', data: r[0].data || {} };
    }
    return { from: phone, stage: 'idle', data: {} };
  } catch (e) {
    console.warn('loadSession error', e);
    return { from: phone, stage: 'idle', data: {} };
  }
}

async function saveSession(env, session, now = new Date()) {
  try {
    await sbUpsert(env, 'wa_session', [{
      from: session.from,
      stage: session.stage || 'idle',
      data:  session.data  || {},
      updated_at: now.toISOString()
    }], { onConflict: 'from', returning: 'minimal' });
  } catch (e) { console.warn('saveSession error', e); }
}

/* ============================ Cantidades ============================ */
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

/* ============================ Inventario & Carrito ============================ */
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

  // “Este suele ser…”
  const q = normalizeWithAliases(queryText);
  const famHit = /(versant|docu\s*color|primelink|versa\s*link|alta\s*link|apeos|c\d{2,4}|b\d{2,4})/i.test(q);
  const tip = famHit ? `\n\nEste suele ser el indicado para tu equipo.` : '';

  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}${tip}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${Math.max(0, s)} en stock y el resto sería *sobre pedido*.`;
}

/* ---- ASK_QTY (con rechazo) ---- */
async function handleAskQty(env, session, toE164, text, lowered, ntext, now){
  const cand = session.data?.last_candidate;

  // Rechazo del artículo o "no"
  if (RX_NEG_NO.test(lowered)) {
    session.data.last_candidate = null;
    session.stage = 'cart_open';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, 'Sin problema. ¿Busco otra opción? Dime modelo/color o lo que necesitas.');
    return ok('EVENT_RECEIVED');
  }

  // Cierre desde ask_qty: solo si ya hay algo en carrito
  if (RX_DONE.test(lowered)) {
    const cart = session.data?.cart || [];
    if (cart.length > 0) {
      session.stage = 'await_invoice';
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, '¿La cotizamos *con factura* o *sin factura*?');
      return ok('EVENT_RECEIVED');
    }
  }

  // No hay candidato guardado
  if (!cand) {
    session.stage = 'cart_open';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, '¿Qué artículo necesitas? (modelo/color, p. ej. "tóner amarillo versant")');
    return ok('EVENT_RECEIVED');
  }

  // Si no parece cantidad, repreguntar con contexto
  if (!looksLikeQuantityStrict(lowered)) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(env, toE164, `¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  // Agregar
  const qty = parseQty(lowered, 1);
  if (!Number.isFinite(qty) || qty <= 0) {
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
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext, now){
  session.data = session.data || {};
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate) addWithStockSplit(session, session.data.last_candidate, 1);
    session.stage = 'await_invoice';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, '¿La quieres *con factura* o *sin factura*?');
    return ok('EVENT_RECEIVED');
  }

  // Cantidad “suelta” -> pedir cantidad del último candidato, si existe
  if (looksLikeQuantityStrict(lowered) && session.data?.last_candidate) {
    session.stage = 'ask_qty';
    await saveSession(env, session, now);
    const s = numberOrZero(session.data.last_candidate.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto sería *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  // Intento de buscar / agregar algo
  if (RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext)) {
    return await startSalesFromQuery(env, session, toE164, text, ntext, now);
  }

  await sendWhatsAppText(env, toE164, 'Te leo 🙂. Puedo agregar un artículo nuevo, buscar otro o *finalizar* si ya está completo.');
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  // Cada nueva consulta limpia candidato previo para evitar “colas” viejas
  session.data = session.data || {};
  session.data.last_candidate = null;
  await saveSession(env, session, now);

  const best = await findBestProduct(env, ntext);
  if (best) {
    session.stage = 'ask_qty';
    session.data.last_candidate = best;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, renderProducto(best, ntext));
    return ok('EVENT_RECEIVED');
  }

  // Sin match claro, no pasamos a ask_qty
  await sendWhatsAppText(env, toE164, 'No encontré una coincidencia directa 😕. ¿Me das el *modelo* y el *color* del consumible? (ej. *tóner amarillo Versant 180*)');
  await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* ============================ Matching simple ============================ */
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
  const s = normalizeBase([p?.nombre, p?.sku, p?.marca, p?.compatible].join(' '));
  const fams = [
    ['versant', /(versant|80|180|2100|280|4100)\b/i, /(docu\s*color|primelink|alta\s*link|versa\s*link|c(60|70|75)|550|560|570)/i],
    ['docucolor', /(docu\s*color|550|560|570)\b/i, /(versant|primelink|alta\s*link|versa\s*link|2100|180|280|4100)/i],
    ['primelink', /(prime\s*link|primelink)\b/i, /(versant|versa\s*link|alta\s*link)/i],
    ['versalink', /(versa\s*link|versalink)\b/i, /(versant|prime\s*link|alta\s*link)/i],
    ['altalink', /(alta\s*link|altalink)\b/i, /(versant|prime\s*link|versa\s*link)/i],
    ['apeos', /\bapeos\b/i, null],
    ['c70', /\bc(60|70|75)\b/i, null]
  ];
  for (const [name, hit, bad] of fams) {
    if (hit.test(t)) {
      if (bad && bad.test(s)) return false;
      return hit.test(s);
    }
  }
  // Sin familia clara -> permitir
  return true;
}

async function findBestProduct(env, queryText, opts = {}) {
  const colorCode = extractColorWord(queryText);

  const scoreAndPick = (arr=[]) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = arr.slice();

    // Filtrar por familia (evita mezclar Versant vs DocuColor, etc.)
    pool = pool.filter(p => productMatchesFamily(p, queryText));

    // Si entendimos color, ES OBLIGATORIO que lo traiga
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
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 50 }) || [];
    const pick1 = scoreAndPick(res);
    if (pick1) return pick1;
  } catch (_) {}

  // 2) LIKE por texto principal
  try {
    const likeToner = /\bton(e|é)r|t[oó]ner|cartucho/i.test(queryText) ? '%toner%' : '%';
    const like = encodeURIComponent(likeToner);
    const r2 = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&` +
             `or=(nombre.ilike.${like},sku.ilike.${like})&` +
             `order=stock.desc.nullslast,precio.asc&limit=500`
    }) || [];
    const pick2 = scoreAndPick(r2);
    if (pick2) return pick2;
  } catch (_) {}

  return null;
}

/* ============================ FAQs ============================ */
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

/* ============================ Cierre venta ============================ */
async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSession(env, session, now);
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

  await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
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

async function handleCollectSequential(env, session, toE164, text, now){
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
    await notifySupport(env, `Nuevo pedido #${c.nombre ? c.nombre : ''} (${toE164})`);
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

/* ============================= Cliente & Pedido ============================ */
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
        nombre: c.nombre || null, rfc: c.rfc || null, email: c.email || null, telefono: session.from || null,
        calle: c.calle || null, numero: c.numero || null, colonia: c.colonia || null, ciudad: c.ciudad || null,
        estado: c.estado || null, cp: c.cp || null
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
      cliente_id, total, moneda: 'MXN', estado: 'nuevo', created_at: new Date().toISOString()
    }], { returning: 'representation' });
    const pedido_id = p?.data?.[0]?.id;

    // Items
    const items = cart.map(it => ({
      pedido_id, producto_id: it.product?.id || null, sku: it.product?.sku || null,
      nombre: it.product?.nombre || null, qty: it.qty, precio_unitario: Number(it.product?.precio || 0)
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
      } catch(e){ /* noop */ }
    }

    console.warn('createOrderFromSession', e);
    return { ok: false, error: String(e) };
  }
}

/* ============================ Supabase Helpers ============================ */
async function sbGet(env, table, { query }) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`
    }
  });
  if (!r.ok) { console.warn('sbGet', table, await r.text()); return null; }
  return await r.json();
}
async function sbUpsert(env, table, rows, { onConflict, returning = 'representation' } = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${onConflict ? `?on_conflict=${onConflict}` : ''}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      Prefer: `resolution=merge-duplicates,return=${returning}`
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) { console.warn('sbUpsert', table, await r.text()); return null; }
  const data = returning === 'minimal' ? null : await r.json();
  return { data };
}
async function sbPatch(env, table, patch, filter) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) console.warn('sbPatch', table, await r.text());
}
async function sbRpc(env, fn, params) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params || {})
  });
  if (!r.ok) { console.warn('sbRpc', fn, await r.text()); return null; }
  return await r.json();
}

/* ============================ SEPOMEX helper ============================ */
async function cityFromCP(env, cp) {
  try {
    const r = await sbGet(env, 'sepomex_cp', { query: `cp=eq.${encodeURIComponent(cp)}&select=cp,estado,municipio,ciudad&limit=1` });
    return r?.[0] || null;
  } catch { return null; }
}

/* =============================== SOPORTE + GCal ============================ */
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

  // Reprogramar en GCal si existe
  if (os.gcal_event_id && os.calendar_id) {
    try {
      const nearest = await findNearestFreeSlot(env, os.calendar_id, slot, tz);
      await gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, {
        start: nearest.start, end: nearest.end, timezone: tz
      });
      slot.start = nearest.start; slot.end = nearest.end;
    } catch (e) { console.warn('[GCal] resched', e); }
  }

  await sbUpsert(env, 'orden_servicio', [{
    id: os.id, ventana_inicio: slot.start, ventana_fin: slot.end, estado: 'agendado'
  }], { returning: 'minimal' });

  await sendWhatsAppText(env, toE164,
    `Actualicé tu visita: *${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*.`
  );
}
async function svWhenIsMyVisit(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita activa. Si gustas la agendamos 🙂`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164,
    `Tu visita está para *${fmtDate(os.ventana_inicio, tz)}* de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*.`
  );
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

/* --------- Horarios/fechas naturales --------- */
function parseNaturalDateTime(text, env) {
  const tz = env?.TZ || 'America/Mexico_City';
  const now = new Date();
  const lower = (text || '').toLowerCase();

  // Día base
  let d = new Date(now);
  if (/\bpasado\s*ma[ñn]ana\b/.test(lower)) d.setDate(d.getDate() + 2);
  else if (/\bma[ñn]ana\b/.test(lower)) d.setDate(d.getDate() + 1);
  else if (/\bhoy\b/.test(lower)) { /* same */ }

  // Hora
  let hh = 12, mm = 0; // default mediodía
  const m1 = lower.match(/\b(\d{1,2})[:\.](\d{2})\b/);
  const m2 = lower.match(/\b(?:a\s*las\s*)?(\d{1,2})\b/);
  if (m1) { hh = Number(m1[1]); mm = Number(m1[2]); }
  else if (m2) { hh = Number(m2[1]); }

  // AM/PM heurística
  if (/\bpm\b/.test(lower) && hh < 12) hh += 12;
  if (/\bam\b/.test(lower) && hh === 12) hh = 0;

  const start = new Date(d.getFullYear(), d.getMonth(), d.getDate(), hh, mm);
  const end = new Date(start.getTime() + 60 * 60 * 1000); // 1h
  return { start: start.toISOString(), end: end.toISOString() };
}
function clampToWindow(when, tz) {
  const start = new Date(when.start);
  const end = new Date(when.end);
  const sh = start.getHours();
  if (sh < 10) { start.setHours(10, 0); end.setTime(start.getTime() + 60 * 60 * 1000); }
  if (sh > 15) { start.setHours(15, 0); end.setTime(start.getTime() + 60 * 60 * 1000); }
  return { start: start.toISOString(), end: end.toISOString() };
}
function fmtDate(iso, tz) {
  try {
    return new Intl.DateTimeFormat('es-MX', { timeZone: tz, dateStyle: 'full' }).format(new Date(iso));
  } catch { return new Date(iso).toDateString(); }
}
function fmtTime(iso, tz) {
  try {
    return new Intl.DateTimeFormat('es-MX', { timeZone: tz, timeStyle: 'short' }).format(new Date(iso));
  } catch { return new Date(iso).toLocaleTimeString(); }
}

/* ============================ WhatsApp extractor ============================ */
function extractWhatsAppContext(body) {
  try {
    const entry = body?.entry?.[0];
    const changes = entry?.changes?.[0];
    const value = changes?.value;
    const msg = value?.messages?.[0];
    if (!msg) return null;

    const from = msg.from; // string sin + (E164 limpio)
    const fromE164 = `+${from}`;
    const mid = msg.id;
    const profileName = value?.contacts?.[0]?.profile?.name || '';
    const ts = msg.timestamp;
    const type = msg.type;

    if (type === 'text') {
      return {
        mid, from, fromE164, profileName, ts,
        msgType: 'text',
        textRaw: msg.text?.body || ''
      };
    }
    if (type === 'audio') {
      return {
        mid, from, fromE164, profileName, ts,
        msgType: 'audio',
        media: { id: msg.audio?.id }
      };
    }
    // cualquier otro
    return { mid, from, fromE164, profileName, ts, msgType: type, textRaw: '' };
  } catch {
    return null;
  }
}

   
