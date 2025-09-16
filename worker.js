/**
 * CopiBot – Conversacional con IA (OpenAI) + Ventas + Soporte Técnico + GCal
 * Build: “Borbón r5”
 *
 * Cambios clave r5:
 * - (FIX) Saludo sin duplicar la pregunta “¿Deseas continuar o empezar otro?” usando
 *   un anti-repetición (ask_choice throttle) y saludo sin esa pregunta embebida.
 * - (FIX) Búsqueda de inventario “family & color strict” por defecto (Versant, VersaLink, AltaLink…);
 *   si el usuario menciona familia y no hay match, se ofrece mostrar compatibles (no se devuelve otra familia).
 * - Tono cálido consistente (1 emoji máximo) y logs de intención.
 * - Mejoras de robustez en soporte/ventas sin cambiar firmas de funciones existentes.
 */

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);

      // ===== Webhook verify (Meta/WhatsApp)
      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      // ===== Cron endpoint
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      // ===== WhatsApp webhook
      if (req.method === 'POST' && url.pathname === '/') {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw } = ctx;
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalize(text);
        const now = new Date();

        // ---- Session
        let session = await loadSession(env, from);
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;

        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // Idempotencia
        if (session?.data?.last_mid && session.data.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // Universales soporte
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

        // ====== Intentos (determinista + IA)
        const supportFlag = isSupportIntent(ntext) || (await intentIs(env, text, 'support'));
        const salesFlag   = RX_INV_Q.test(ntext) || (await intentIs(env, text, 'sales'));

        // ====== Si estamos en elección (continuar / empezar otro)
        if (session.stage === 'await_choice') {
          if (supportFlag) {
            session.stage = 'sv_collect';
            await saveSession(env, session, now);
            return await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent:'support', forceWelcome:true });
          }
          if (salesFlag) {
            session.data.last_stage = 'idle';
            session.stage = 'idle';
            await saveSession(env, session, now);
            return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
          }
          if (isContinueish(lowered)) {
            session.stage = session?.data?.last_stage || 'idle';
            await saveSession(env, session, now);
            const prompt = buildResumePrompt(session);
            await sendWhatsAppText(env, fromE164, `Perfecto 🙌\n${prompt}`);
            return ok('EVENT_RECEIVED');
          }
          if (isStartNewish(lowered)) {
            session.data.last_stage = 'idle';
            session.stage = 'idle';
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, `De acuerdo, empezamos desde cero. Cuéntame qué necesitas (*soporte*, *cotización*, etc.). 🙂`);
            return ok('EVENT_RECEIVED');
          }
          await askChoiceOnce(env, session, fromE164);
          return ok('EVENT_RECEIVED');
        }

        // ====== Saludo (sin duplicar)
        const isGreet = RX_GREET.test(lowered);
        if (isGreet && shouldAutogreet(session, now)) {
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          if (session.stage !== 'idle') {
            await sendWhatsAppText(env, fromE164, `Hola${nombre?`, ${nombre}`:''} 🙌`);
            session.data.last_greet_at = now.toISOString();
            session.data.last_stage = session.stage;
            session.stage = 'await_choice';
            await saveSession(env, session, now);
            await askChoiceOnce(env, session, fromE164); // throttle anti-duplicado
            return ok('EVENT_RECEIVED');
          } else {
            await sendWhatsAppText(env, fromE164, `Hola${nombre?`, ${nombre}`:''} 🙌 ¿En qué te ayudo hoy?`);
            session.data.last_greet_at = now.toISOString();
            await saveSession(env, session, now);
            return ok('EVENT_RECEIVED');
          }
        }

        // ====== Cambio de intención en caliente
        if (supportFlag || session.stage?.startsWith('sv_')) {
          console.log('[SUPPORT] intent', { from, stage: session.stage });
          const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent:'support' });
          return handled;
        }
        if (salesFlag) {
          console.log('[SALES] intent', { from, stage: session.stage });
          if (session.stage !== 'idle') {
            await sendWhatsAppText(env, fromE164, `Entendido. Pauso tu trámite anterior y reviso *inventario*. 🧰`);
            session.data.last_stage = session.stage;
            session.stage = 'idle';
            await saveSession(env, session, now);
          }
          const handled = await startSalesFromQuery(env, session, fromE164, text, ntext, now);
          return handled;
        }

        // ====== Ventas (stages)
        if (session.stage === 'ask_qty')            return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open')          return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice')      return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage?.startsWith('collect_'))  return await handleCollectSequential(env, session, fromE164, text, now);

        // ====== Reanudación por saludo con flujo activo
        if (session.stage !== 'idle' && isGreet) {
          session.data.last_stage = session.stage;
          session.stage = 'await_choice';
          await saveSession(env, session, now);
          await askChoiceOnce(env, session, fromE164);
          return ok('EVENT_RECEIVED');
        }

        // ====== FAQs
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ====== Fallback
        const reply = await aiSmallTalk(env, session, 'fallback', text);
        await sendWhatsAppText(env, fromE164, reply);
        await saveSession(env, session, now);
        return ok('EVENT_RECEIVED');
      }

      return new Response('Not found', { status: 404 });
    } catch (e) {
      console.error('Worker error', e);
      return ok('EVENT_RECEIVED');
    }
  },

  async scheduled(event, env, ctx) {
    try {
      const out = await cronReminders(env);
      console.log('cron run', out);
    } catch (e) {
      console.error('cron error', e);
    }
  }
};

/* ============================ Regex / Intents ============================ */
const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_Q  = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro|yellow|black|bk|k)\b/i;

function isContinueish(t){ return /\b(continuar|continuemos|seguir|retomar|reanudar|continuo|contin[uú]o)\b/i.test(t); }
function isStartNewish(t){ return /\b(empezar|nuevo|desde cero|otra cosa|otro|iniciar|empecemos)\b/i.test(t); }

/** Soporte: intención determinista */
function isSupportIntent(ntext='') {
  const t = ` ${ntext} `;
  const hasProblem =
    /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|c[oó]digo)/.test(t);
  const hasDevice =
    /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase =
    /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);

  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_NEG_NO = /\b(no|nel|ahorita no)\b/i;
const RX_DONE = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;

const RX_YES = /\b(s[ií]|sí|si|claro|va|dale|sale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo|de acuerdo|me sirve)\b/i;

/* ============================ Helpers ============================ */
const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalize(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function fmtDate(d, tz){ try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }catch{ return new Date(d).toLocaleDateString('es-MX'); } }
function fmtTime(d, tz){ try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; } }
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function shouldAutogreet(session, now){
  const last = session?.data?.last_greet_at ? Date.parse(session.data.last_greet_at) : 0;
  return (now.getTime() - last) > 8*60*60*1000;
}
function promptedRecently(session, key, ms=5*60*1000){
  session.data.prompts = session.data.prompts || {};
  const last = session.data.prompts[key] ? Date.parse(session.data.prompts[key]) : 0;
  const okk = (Date.now() - last) < ms;
  session.data.prompts[key] = new Date().toISOString();
  return okk;
}
async function askChoiceOnce(env, session, toE164){
  if (!promptedRecently(session, 'ask_choice', 12*1000)) {
    await sendWhatsAppText(env, toE164, `¿*Deseas continuar* con tu trámite pendiente o *empezar otro*?`);
  }
}

/* ============================ IA ============================ */
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
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde cálido, breve y claro; máximo 1 emoji.`;
  let prompt = '';
  if (mode === 'fallback') {
    prompt = `El usuario dijo: """${userText}""".
Responde breve, útil y amable. Si no hay contexto, ofrece inventario o soporte.`;
  } else {
    prompt = `Saluda de forma breve y cálida${nombre?` usando el nombre "${nombre}"`:''}. Solo saludo, sin preguntas adicionales.`;
  }
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 🙌`);
}
async function intentIs(env, text, expected){
  try{
    const out = await aiClassifyIntent(env, text);
    return out?.intent === expected;
  }catch{return false;}
}
async function aiClassifyIntent(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Clasifica texto (es-MX) en JSON:
{ "intent": "support|sales|faq|smalltalk" }`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try { return JSON.parse(out||'{}'); } catch { return null; }
}

/* ============================ WhatsApp ============================ */
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
function extractWhatsAppContext(payload) {
  try {
    const value = payload?.entry?.[0]?.changes?.[0]?.value;
    const msg = value?.messages?.[0];
    if (!msg || msg.type !== 'text') return null;
    const from = msg.from;
    const fromE164 = `+${from}`;
    const mid = msg.id || `${Date.now()}_${Math.random()}`;
    const textRaw = msg.text?.body || '';
    const profileName = value?.contacts?.[0]?.profile?.name || '';
    return { msg, from, fromE164, mid, textRaw, profileName };
  } catch { return null; }
}

/* ============================ Ventas / Carrito ============================ */
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
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}
function priceWithIVA(n){ const v=Number(n||0); return `${formatMoneyMXN(v)} + IVA`; }

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

  if (RX_WANT_QTY.test(lowered)) {
    session.stage = 'ask_qty';
    await saveSession(env, session, now);
    const c = session.data?.last_candidate;
    const s = numberOrZero(c?.stock);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto iría *sobre pedido*)`);
    return ok('EVENT_RECEIVED');
  }

  if (RX_ADD_ITEM.test(lowered) || RX_INV_Q.test(ntext)) {
    const cleanQ = lowered.replace(RX_ADD_ITEM, '').trim() || ntext;
    const best = await findBestProduct(env, cleanQ);
    const hints = extractModelHints(cleanQ);
    const strict = strictFamilyDefault(env);
    if (!best && hints.family && strict) {
      session.stage = 'await_compatibles';
      session.data.pending_query = cleanQ;
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `No vi disponibilidad *${hints.family}* ahora mismo 😕. ¿Te muestro opciones *compatibles*?`);
      return ok('EVENT_RECEIVED');
    }
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSession(env, session, now);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Cuántas piezas agrego? (hay ${s} en stock; el resto sería *sobre pedido*)`);
      return ok('EVENT_RECEIVED');
    } else {
      await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. ¿Busco otra opción o lo revisa un asesor?`);
      await notifySupport(env, `Inventario sin match. ${toE164}: ${text}`);
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }
  }

  await sendWhatsAppText(env, toE164, `Te leo 🙂. Puedo agregar el artículo visto, buscar otro o *finalizar* si ya está completo.`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  const saysNo  = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes = !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};

  if (!saysYes && !saysNo && /hola|cómo estás|como estas|gracias/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', originalText);
    if (!promptedRecently(session, 'invoice', 3*60*1000)) {
      await sendWhatsAppText(env, toE164, `${friendly}\nPor cierto, ¿la quieres *con factura* o *sin factura*?`);
    } else {
      await sendWhatsAppText(env, toE164, friendly);
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
      await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
      await notifySupport(env, `Pedido (parcial) ${toE164}. Revisar en Supabase.\nError: ${res?.error || 'N/A'}`);
    }
    session.stage = 'post_order';
    session.data.cart = [];
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, '¿Puedo ayudarte con algo más? (Sí / No)');
    return ok('EVENT_RECEIVED');
  }

  if (!promptedRecently(session, 'invoice', 2*60*1000)) {
    await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  }
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* Captura UNO A UNO (ventas) */
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

function parseCustomerFragment(field, text){
  const t = text;
  if (field==='nombre') return clean(t);
  if (field==='rfc'){
    const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i);
    return m ? m[1].toUpperCase() : clean(t).toUpperCase();
  }
  if (field==='email'){
    const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return m ? m[0].toLowerCase() : clean(t).toLowerCase();
  }
  if (field==='numero'){
    const m = t.match(/\b(\d+[A-Z]?)\b/i);
    return m ? m[1] : clean(t);
  }
  if (field==='cp'){
    const m = t.match(/\b(\d{5})\b/);
    return m ? m[1] : clean(t);
  }
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
    await notifySupport(env, `Nuevo pedido #${res.pedido_id ?? '—'}\nCliente: ${c.nombre} (${toE164})\nFactura: ${session.data.requires_invoice ? 'Sí' : 'No'}`);
  } else {
    await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
    await notifySupport(env, `Pedido (parcial) ${toE164}. Revisar en Supabase.\nError: ${res?.error || 'N/A'}`);
  }

  session.stage = 'post_order';
  session.data.cart = [];
  await saveSession(env, session, now);
  await sendWhatsAppText(env, toE164, '¿Puedo ayudarte con algo más? (Sí / No)');
  return ok('EVENT_RECEIVED');
}

/* ============================ Inventario ============================ */
function extractModelHints(text='') {
  const t = normalize(text);
  const out = {};
  if (/\bversant\b/i.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/i.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/i.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/i.test(t)) out.family = 'docucolor';
  else if (/\bapeos\b/i.test(t)) out.family = 'apeos';
  else if (/\bc(60|70|75)\b/i.test(t)) out.family = 'c70';
  return out;
}
function extractColor(text='') {
  const t = normalize(text);
  if (/\b(amarillo|yellow|ylw|y)\b/i.test(t)) return 'amarillo';
  if (/\b(magenta|m)\b/i.test(t)) return 'magenta';
  if (/\b(cyan|cian|c)\b/i.test(t)) return 'cyan';
  if (/\b(negro|black|bk|k)\b/i.test(t)) return 'negro';
  return null;
}
function productHasColor(p, color){
  if (!color) return true;
  const s = normalize([p?.nombre, p?.sku].join(' '));
  const map = {
    amarillo: ['amarillo','yellow','ylw',' y '],
    magenta: ['magenta',' m '],
    cyan: ['cyan','cian',' cyan '],
    negro: ['negro','black','bk',' k ']
  };
  const keys = map[color] || [];
  return keys.some(k => s.includes(k));
}
function productMatchesFamily(p, family){
  if (!family) return true;
  const s = normalize([p?.nombre, p?.sku, p?.marca].join(' '));
  if (family==='c70') return /\bc(60|70|75)\b/i.test(s) || s.includes('c60') || s.includes('c70') || s.includes('c75');
  return s.includes(family);
}
const strictFamilyDefault = (env)=> ((env.STRICT_FAMILY_MATCH ?? 'true').toString().toLowerCase() === 'true');

async function findBestProduct(env, queryText, opts = {}) {
  const hints = extractModelHints(queryText);
  const color = extractColor(queryText);
  const strict = strictFamilyDefault(env);

  const pick = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return null;
    let pool = arr.slice();

    // Color primero
    pool = pool.filter(p => productHasColor(p, color));

    // Familia estricta si aplica
    if (hints.family && !opts.ignoreFamily) {
      const famPool = pool.filter(p => productMatchesFamily(p, hints.family));
      if (famPool.length) pool = famPool;
      else if (strict) return null; // deja que el caller ofrezca "compatibles"
    }

    // Heurística de orden: stock desc, score desc, precio asc
    pool.sort((a,b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      const sc = numberOrZero(b.score||0) - numberOrZero(a.score||0);
      if (sc !== 0) return sc;
      return numberOrZero(a.precio||0) - numberOrZero(b.precio||0);
    });
    return pool[0] || null;
  };

  try {
    // 1) fuzzy RPC
    const res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 15 });
    const best = pick(res);
    if (best) return best;
  } catch {}

  // 2) family LIKE
  if (hints.family) {
    try {
      const like = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=50`
      });
      const best = pick(r);
      if (best) return best;
      if (strict && !opts.ignoreFamily) return null;
    } catch {}
  }

  // 3) toner general
  try {
    const r = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock,tipo&tipo=eq.toner&order=stock.desc.nullslast,precio.asc&limit=120`
    });
    const best = pick(r);
    if (best) return best;
  } catch {}

  // 4) fallback LIKE toner
  try {
    const like = encodeURIComponent(`%toner%`);
    const r = await sbGet(env, 'producto_stock_v', {
      query: `select=id,nombre,marca,sku,precio,stock&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=120`
    });
    const best = pick(r);
    if (best) return best;
  } catch {}

  return null;
}

async function startSalesFromQuery(env, session, toE164, text, ntext, now){
  const best = await findBestProduct(env, ntext);
  const hints = extractModelHints(ntext || text);
  const strict = strictFamilyDefault(env);

  if (!best && hints.family && strict) {
    session.stage = 'await_compatibles';
    session.data.pending_query = ntext || text;
    await saveSession(env, session, now);
    await sendWhatsAppText(
      env,
      toE164,
      `No encontré disponibilidad *${hints.family}* ahora mismo 😕. ¿Te muestro opciones *compatibles*?`
    );
    return ok('EVENT_RECEIVED');
  }
  if (best) {
    session.stage = 'ask_qty';
    session.data.cart = session.data.cart || [];
    session.data.last_candidate = best;
    await saveSession(env, session, now);
    const s = numberOrZero(best.stock);
    await sendWhatsAppText(
      env,
      toE164,
      `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
    );
    return ok('EVENT_RECEIVED');
  } else {
    await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. Te conecto con un asesor humano…`);
    await notifySupport(env, `Inventario sin match. +${session.from}: ${text}`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }
}

/* ====== Cliente/Pedidos (sin cambios funcionales relevantes) ====== */
async function preloadCustomerIfAny(env, session){
  try{
    const r = await sbGet(env, 'cliente', { query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,cp&telefono=eq.${session.from}&limit=1` });
    if (r && r[0]) {
      session.data.customer = { ...(session.data.customer||{}), ...r[0] };
    }
  }catch(e){ console.warn('preloadCustomerIfAny', e); }
}

async function ensureClienteFields(env, cliente_id, c){
  try{
    const patch = {};
    ['nombre','rfc','email','calle','numero','colonia','ciudad','cp'].forEach(k=>{ if (truthy(c[k])) patch[k]=c[k]; });
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
      const exist = await sbGet(env, 'cliente', { query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(c.email || '')})&limit=1` });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch {}
    if (!cliente_id) {
      const ins = await sbUpsert(env, 'cliente', [{
        nombre: c.nombre || null, rfc: c.rfc || null, email: c.email || null, telefono: session.from || null,
        calle: c.calle || null, numero: c.numero || null, colonia: c.colonia || null, ciudad: c.ciudad || null, cp: c.cp || null
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
      pedido_id,
      producto_id: it.product?.id || null,
      sku: it.product?.sku || null,
      nombre: it.product?.nombre || null,
      qty: it.qty,
      precio_unitario: Number(it.product?.precio || 0)
    }));
    await sbUpsert(env, 'pedido_item', items, { returning: 'minimal' });

    // decremento de stock (si existe RPC)
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

/* ============================ SOPORTE ============================ */
/* (Mantenemos la firma; flujo ya robusto en builds previas) */
function extractSvInfo(text) {
  const out = {};
  if (/xerox/i.test(text)) out.marca = 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(text)) out.marca = 'Fujifilm';

  const m = text.match(/(versant\s*\d+\/\d+|versant\s*\d+|versalink\s*\w+|altalink\s*\w+|docucolor\s*\d+|c\d{2,4}|b\d{2,4})/i);
  if (m) out.modelo = m[1].toUpperCase();

  const err = text.match(/\berror\s*([0-9\-]+)\b/i);
  if (err) out.error_code = err[1];
  if (/no imprime/i.test(text)) out.falla = 'No imprime';
  if (/atasc(a|o)|se atora|se traba|arrugad(i|o)|saca el papel/i.test(text)) out.falla = 'Atasco/arrugado de papel';
  if (/mancha|calidad|linea|l[ií]nea/i.test(text)) out.falla = 'Calidad de impresión';

  if (/\b(parado|urgente|producci[oó]n detenida|parada)\b/i.test(text)) out.prioridad = 'alta';

  const loose = parseAddressLoose(text);
  Object.assign(out, loose);

  const d = parseCustomerText(text);
  if (d.calle) out.calle = d.calle;
  if (d.numero) out.numero = d.numero;
  if (d.colonia) out.colonia = d.colonia;
  if (d.cp) out.cp = d.cp;
  if (d.ciudad) out.ciudad = d.ciudad;

  return out;
}

function svFillFromAnswer(sv, field, text, env){
  const t = text.trim();
  if (field === 'modelo') {
    const m = t.match(/(xerox|fujifilm|fuji\s?film)?\s*(versant\s*\d+\/\d+|versant\s*\d+|versalink\s*\w+|altalink\s*\w+|docucolor\s*\d+|c\d{2,4}|b\d{2,4})/i);
    if (m) {
      if (m[1]) sv.marca = /fuji/i.test(m[1]) ? 'Fujifilm' : 'Xerox';
      sv.modelo = m[2].toUpperCase();
    } else {
      sv.modelo = t; // libre
    }
    return;
  }
  if (field === 'falla') { sv.falla = t; return; }
  if (field === 'calle') { sv.calle = clean(t); return; }
  if (field === 'numero') { const m = t.match(/\b(\d+[A-Z]?)\b/); sv.numero = m?m[1]:clean(t); return; }
  if (field === 'colonia') { sv.colonia = clean(t); return; }
  if (field === 'cp') { const m = t.match(/\b(\d{5})\b/); sv.cp = m?m[1]:clean(t); return; }
  if (field === 'horario') {
    const dt = parseNaturalDateTime(t, env);
    if (dt?.start) sv.when = dt;
    return;
  }
}

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    if (session.stage === 'sv_collect' && session.data.sv_need_next) {
      svFillFromAnswer(sv, session.data.sv_need_next, text, env);
    }

    Object.assign(sv, extractSvInfo(text));
    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) sv.when = dt;
    }

    if (!sv._welcomed) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Lamento la falla 😕. Vamos a ayudarte. ¿Me confirmas *marca/modelo* y una breve *descripción* del problema?`);
    }

    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick);
    }

    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    const needed = [];
    if (!truthy(sv.marca) && !truthy(sv.modelo)) needed.push('modelo');
    if (!truthy(sv.falla)) needed.push('falla');
    if (!truthy(sv.calle)) needed.push('calle');
    if (!truthy(sv.numero)) needed.push('numero');
    if (!truthy(sv.colonia)) needed.push('colonia');
    if (!truthy(sv.cp)) needed.push('cp');
    if (!sv.when?.start) needed.push('horario');

    if (needed.length) {
      session.stage = 'sv_collect';
      session.data.sv_need_next = needed[0];
      await saveSession(env, session, now);
      const q = {
        modelo: '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox Versant 180*)',
        falla: 'Describe brevemente la falla (p.ej., “*atasco en fusor*”, “*no imprime*”).',
        calle: '¿Cuál es la *calle* donde estará el equipo?',
        numero: '¿Qué *número* es?',
        colonia: '¿*Colonia*?',
        cp: '¿*Código Postal* (5 dígitos)?',
        horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (puedes decir “*mañana 12:30*”)'
      }[needed[0]];
      await sendWhatsAppText(env, toE164, q);
      return ok('EVENT_RECEIVED');
    }

    // Agenda
    const pool = await getCalendarPool(env);
    const cal = pickCalendarFromPool(pool);
    if (!cal) {
      await sendWhatsAppText(env, toE164, `Ahora mismo no veo disponibilidad automática. Te contacto para ofrecer opciones. 🙏`);
      await notifySupport(env, `Sin calendar activo para OS. ${toE164}`);
      session.stage = 'idle';
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }

    const tz = env.TZ || 'America/Mexico_City';
    const chosen = clampToWindow(sv.when, tz);
    const slot = await findNearestFreeSlot(env, cal.gcal_id, chosen, tz);

    const event = await gcalCreateEvent(env, cal.gcal_id, {
      summary: `Visita técnica: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
      description: renderOsDescription(session.from, sv),
      start: slot.start,
      end: slot.end,
      timezone: tz,
    });

    const cliente_id = await upsertClienteByPhone(env, session.from);
    const osBody = [{
      cliente_id,
      marca: sv.marca || null,
      modelo: sv.modelo || null,
      falla_descripcion: sv.falla || null,
      prioridad: sv.prioridad || 'media',
      estado: 'agendado',
      ventana_inicio: new Date(slot.start).toISOString(),
      ventana_fin: new Date(slot.end).toISOString(),
      gcal_event_id: event?.id || null,
      calendar_id: cal.gcal_id || null,
      calle: sv.calle || null,
      numero: sv.numero || null,
      colonia: sv.colonia || null,
      ciudad: sv.ciudad || null,
      cp: sv.cp || null,
      created_at: new Date().toISOString()
    }];
    const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
    const osId = os?.data?.[0]?.id;

    await sendWhatsAppText(
      env,
      toE164,
      `¡Listo! Agendé tu visita 🙌
*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*
Dirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}
Técnico asignado: ${cal.name || 'por confirmar'}.

Si necesitas reprogramar o cancelar, dímelo con confianza.`
    );

    session.stage = 'sv_scheduled';
    session.data.sv.os_id = osId;
    session.data.sv.gcal_event_id = event?.id || null;
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  } catch (e) {
    console.error('handleSupport error', e);
    await sendWhatsAppText(env, toE164, 'Recibí tu mensaje de *soporte*. Te contacto enseguida para ayudarte 🙌');
    return ok('EVENT_RECEIVED');
  }
}

function quickHelp(ntext){
  if (/\batasc(a|o)|se atora|se traba|arrugad/i.test(ntext)){
    return `Veamos rápido 🧰
1) Apaga y enciende el equipo.
2) Revisa bandejas y retira papel atorado.
3) Abre y cierra el fusor con cuidado.
Si sigue igual, agendamos visita para diagnóstico.`;
  }
  if (/\bno imprime\b/.test(ntext)){
    return `Probemos rápido 🧰
1) Reinicia la impresora.
2) Verifica tóner y que todas las puertas estén bien cerradas.
3) Intenta imprimir una página de prueba.
Si persiste, agendamos visita.`;
  }
  if (/\bmancha|l[ií]ne?a|calidad\b/.test(ntext)){
    return `Sugerencia rápida 🎯
1) Imprime un patrón de prueba.
2) Revisa niveles y remueve/coloca de nuevo los tóners.
3) Limpia rodillos si es posible.
Si no mejora, te agendo visita para revisión.`;
  }
  return null;
}

async function svCancel(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para cancelar.`); return; }
  if (os.gcal_event_id && os.calendar_id) await gcalDeleteEvent(env, os.calendar_id, os.gcal_event_id);
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'cancelada', cancel_reason: 'cliente' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *cancelado* tu visita. Si necesitas agendar otra, aquí estoy. 😊`);
}
async function svReschedule(env, session, toE164, when) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para reprogramar.`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  const chosen = clampToWindow(when, tz);
  const slot = await findNearestFreeSlot(env, os.calendar_id, chosen, tz);
  if (os.gcal_event_id && os.calendar_id) {
    await gcalPatchEvent(env, os.calendar_id, os.gcal_event_id, {
      start: { dateTime: slot.start, timeZone: tz },
      end: { dateTime: slot.end, timeZone: tz }
    });
  }
  await sbUpsert(env, 'orden_servicio', [{
    id: os.id,
    estado: 'reprogramado',
    ventana_inicio: new Date(slot.start).toISOString(),
    ventana_fin: new Date(slot.end).toISOString()
  }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *reprogramado* tu visita a:
*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}* ✅`);
}
async function svWhenIsMyVisit(env, session, toE164) {
  const os = await getLastOpenOS(env, session.from);
  if (!os) { await sendWhatsAppText(env, toE164, `No veo una visita programada. ¿Agendamos una?`); return; }
  const tz = env.TZ || 'America/Mexico_City';
  await sendWhatsAppText(env, toE164, `Tu próxima visita: *${fmtDate(os.ventana_inicio, tz)}*, de *${fmtTime(os.ventana_inicio, tz)}* a *${fmtTime(os.ventana_fin, tz)}*. Estado: ${os.estado}.`);
}

/* ============================ FAQs ============================ */
async function maybeFAQ(env, ntext) {
  try {
    const like = encodeURIComponent(`%${ntext.slice(0, 60)}%`);
    const r = await sbGet(env, 'company_info', {
      query: `select=key,content,tags&or=(key.ilike.${like},content.ilike.${like})&limit=1`
    });
    if (r && r[0]?.content) return r[0].content;
  } catch {}
  if (/\b(qu[ié]nes?\s+son|sobre\s+ustedes|qu[eé]\s+es\s+cp(\s+digital)?|h[aá]blame\s+de\s+ustedes)\b/i.test(ntext)) {
    return '¡Hola! Somos *CP Digital*. Ayudamos a empresas con consumibles y refacciones para impresoras Xerox y Fujifilm, y brindamos visitas de soporte técnico. Cotizamos, vendemos con o sin factura y agendamos servicio en tu horario.';
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

/* ============================ Fechas ============================ */
function parseNaturalDateTime(text, env) {
  const tz = env.TZ || 'America/Mexico_City';
  const now = new Date();
  const base = new Date(now.toLocaleString('en-US', { timeZone: tz }));
  let d = new Date(base);
  let targetDay = null;

  if (/\b(hoy)\b/i.test(text)) targetDay = 0;
  else if (/\b(ma[ñn]ana)\b/i.test(text)) targetDay = 1;
  else {
    const days = ['domingo','lunes','martes','miércoles','miercoles','jueves','viernes','sábado','sabado'];
    for (let i=0;i<days.length;i++) {
      if (new RegExp(`\\b${days[i]}\\b`,`i`).test(text)) {
        const today = base.getDay();
        const want = i%7;
        let delta = (want - today + 7) % 7;
        if (delta===0) delta = 7;
        targetDay = delta; break;
      }
    }
  }
  if (targetDay!==null) d.setDate(d.getDate()+targetDay);

  let hour = null, minute = 0;
  const m = text.match(/\b(\d{1,2})(?:[:\.](\d{2}))?\s*(am|pm|a\.?m\.?|p\.?m\.?)?\b/i);
  if (m) {
    hour = Number(m[1]); minute = m[2]?Number(m[2]):0;
    const ampm = (m[3]||'').toLowerCase();
    if (/pm/.test(ampm) && hour<12) hour+=12;
    if (/am/.test(ampm) && hour===12) hour=0;
  } else if (/\b(mediod[ií]a)\b/i.test(text)) { hour = 12; minute=0; }

  if (targetDay===null && hour===null) return null;
  if (hour===null) hour = 12;

  d.setHours(hour, minute,analysis draft
