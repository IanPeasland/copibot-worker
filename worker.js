/**
 * CopiBot – Conversacional con IA (OpenAI) + Ventas + Soporte Técnico + GCal
 * Oct/2025 – build “Borbón-R4”
 *
 * R4 (sobre R3):
 * - Saludo inicial más natural (ya no confirma nada en “hola”).
 * - Soporte: primer pedido de marca/modelo empático y humano.
 * - Reconocimiento de modelo mejorado:
 *     · normalización + alias comunes (verzan→versant, dococolor→docucolor, etc.)
 *     · regex tolerantes (versant/docucolor/versalink/altalink/primelink/apeos/C/B series)
 *     · IA opcional: aiExtractTonerQuery() para {familia,color,subfamilia,cantidad}
 * - Matching inventario:
 *     · Color = filtro duro
 *     · Familia = filtro duro (nunca mezcla, salvo que el usuario acepte ver “compatibles”)
 *     · Versant NUNCA trae DocuColor y viceversa
 *     · Subfamilia sólo refina; si sin stock → “sobre pedido” y ofrece compatibles
 * - Compatibles: sólo si el cliente lo aprueba (stage await_compatibles)
 * - UX: mensajes cálidos, humanos; máx. 1 emoji por mensaje; sin “trámite” ni frases robóticas
 */

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);

      // Webhook verify (Meta/WhatsApp)
      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      // Cron
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      // WhatsApp webhook
      if (req.method === 'POST' && url.pathname === '/') {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw } = ctx;
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text); // ← alias + normalización
        const now = new Date();

        // ===== Session =====
        let session = await loadSession(env, from);
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;

        // Nombre por profile
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // Idempotencia básica por mid
        if (session?.data?.last_mid && session.data.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // ===== Comandos universales (soporte) =====
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

        // ===== Global intent switch =====
        const supportIntent = isSupportIntent(ntext) || (await intentIs(env, text, 'support'));
        const salesIntent = RX_INV_Q.test(ntext) || (await intentIs(env, text, 'sales'));

        // ===== Saludo =====
        const isGreet = RX_GREET.test(lowered);
        if (isGreet) {
          // Limpia candidatos para que "hola" nunca confirme nada
          if (session?.data?.last_candidate) delete session.data.last_candidate;
          if (session?.data?.pending_query) delete session.data.pending_query;

          // Saludo humano y natural
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre || ''));
          await sendWhatsAppText(env, fromE164, `¡Hola${nombre ? ' ' + nombre : ''}! ¿En qué te puedo ayudar hoy? 👋`);
          session.data.last_greet_at = now.toISOString();

          // Si había algo a medias, ofrece continuar sin sonar robótico
          if (session.stage !== 'idle') {
            session.data.last_stage = session.stage;
            session.stage = 'await_choice';
            await saveSession(env, session, now);
            return ok('EVENT_RECEIVED');
          }
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Elección de continuar o empezar otro =====
        if (session.stage === 'await_choice') {
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
            await sendWhatsAppText(env, fromE164, `Perfecto, cuéntame qué necesitas (soporte, cotización, etc.). 🙂`);
            return ok('EVENT_RECEIVED');
          }
          await sendWhatsAppText(env, fromE164, `¿Prefieres continuar con lo pendiente o empezamos algo nuevo?`);
          return ok('EVENT_RECEIVED');
        }

        // ===== Compatibles (aprobación) =====
        if (session.stage === 'await_compatibles') {
          if (isYesish(lowered)) {
            // Buscar mejores opciones ignorando familia (compatibles)
            const q = session.data?.pending_query || ntext || lowered;
            const bestCompat = await findBestProduct(env, q, { ignoreFamily: true });
            session.stage = 'ask_qty';
            session.data.last_candidate = bestCompat || null;
            await saveSession(env, session, now);
            if (bestCompat) {
              const s = numberOrZero(bestCompat.stock);
              await sendWhatsAppText(env, fromE164, `${renderProducto(bestCompat)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
            } else {
              await sendWhatsAppText(env, fromE164, `No veo compatibles claros ahora mismo. Si gustas, lo revisa un asesor.`);
              await notifySupport(env, `Compatibles sin match. +${session.from}: ${q}`);
              session.stage = 'idle';
              await saveSession(env, session, now);
            }
            return ok('EVENT_RECEIVED');
          } else if (isNoish(lowered)) {
            session.stage = 'idle';
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, `De acuerdo. Si te parece, puedo dejarlo *sobre pedido* o buscamos otro artículo.`);
            return ok('EVENT_RECEIVED');
          } else {
            await sendWhatsAppText(env, fromE164, `¿Quieres que te muestre *compatibles* de otra línea? (Sí/No)`);
            return ok('EVENT_RECEIVED');
          }
        }

        // ===== Cambio de intención en caliente (prioridad soporte) =====
        if (supportIntent || session.stage?.startsWith('sv_')) {
          const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        // ===== A ventas por intención =====
        if (salesIntent) {
          if (session.stage !== 'idle') {
            await sendWhatsAppText(env, fromE164, `Te ayudo con inventario. Dejo lo otro en pausa un momento.`);
            session.data.last_stage = session.stage;
            session.stage = 'idle';
            await saveSession(env, session, now);
          }
          const handled = await startSalesFromQuery(env, session, fromE164, text, ntext, now);
          return handled;
        }

        // ===== Stages de ventas =====
        if (session.stage === 'ask_qty') return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open') return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage?.startsWith('collect_')) return await handleCollectSequential(env, session, fromE164, text, now);

        // ===== FAQs =====
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // ===== Fallback humano =====
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
const RX_INV_Q = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro|yellow|black|bk|k)\b/i;

function isContinueish(t){ return /\b(continuar|continuemos|seguir|retomar|reanudar|continuo|contin[uú]o)\b/i.test(t); }
function isStartNewish(t){ return /\b(empezar|nuevo|desde cero|otra cosa|otro|iniciar|empecemos)\b/i.test(t); }

/** Determinista intención soporte (con ntext normalizado) */
function isSupportIntent(ntext='') {
  const t = `${ntext}`;
  const hasProblem = /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|marca c[oó]digo|c[oó]digo)/.test(t);
  const hasDevice = /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase = /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);
  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_NEG_NO = /\b(no|nel|ahorita no)\b/i;
const RX_DONE = /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;
const RX_YES = /\b(s[ií]|sí|si|claro|va|dale|sale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo|de acuerdo|me sirve)\b/i;

function isYesish(t){ return RX_YES.test(t); }
function isNoish(t){ return RX_NEG_NO.test(t) || RX_DONE.test(t); }

/* ============================ Helpers ============================ */
const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalizeBase(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }

function normalizeWithAliases(s=''){
  const t = normalizeBase(s);
  const aliases = [
    ['verzan','versant'], ['verzand','versant'], ['versan','versant'], ['vrsant','versant'],
    ['dococolor','docucolor'], ['docucolour','docucolor'], ['docu color','docucolor'],
    ['versalink','versalink'], ['versa link','versalink'], ['altaling','altalink'], ['alta link','altalink'],
    ['prime link','primelink'], ['prime-link','primelink'], ['prymelink','primelink'],
    ['fujifilm','fujifilm'], ['fuji film','fujifilm'], ['docucolor 5560','docucolor 550/560/570']
  ];
  let out = t;
  for (const [bad, good] of aliases){
    out = out.replace(new RegExp(`\\b${bad}\\b`, 'g'), good);
  }
  return out;
}

function fmtDate(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }
  catch{ return new Date(d).toLocaleDateString('es-MX'); }
}
function fmtTime(d, tz){
  try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }
  catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; }
}
function formatMoneyMXN(n){
  const v=Number(n||0);
  try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }
  catch{ return `$${v.toFixed(2)}`; }
}
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function priceWithIVA(n){ const v=Number(n||0); return `${formatMoneyMXN(v)} + IVA`; }

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
  const sys = `Eres CopiBot de CP Digital (es-MX). Responde con calidez humana, breve y claro. Máx. 1 emoji. Evita listas salvo necesidad.`;
  let prompt = '';
  if (mode === 'fallback') {
    prompt = `El usuario dijo: """${userText}""". Responde breve, útil y amable. Si no hay contexto, ofrece inventario o soporte.`;
  } else {
    prompt = `El usuario dijo: """${userText}""". Responde breve y amable.`;
  }
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 👋 ¿En qué te puedo ayudar?`);
}

/** Clasificador IA (opcional) */
async function intentIs(env, text, expected){
  try{
    const out = await aiClassifyIntent(env, text);
    return out?.intent === expected;
  }catch{return false;}
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

    // Intento de NER con IA si ambiguo
    const extracted = await aiExtractTonerQuery(env, cleanQ).catch(()=>null);
    const enrichedQ = enrichQueryFromAI(cleanQ, extracted);

    const best = await findBestProduct(env, enrichedQ); // familia+color = duro
    if (best) {
      session.data.last_candidate = best;
      session.stage = 'ask_qty';
      await saveSession(env, session, now);
      const s = numberOrZero(best.stock);
      await sendWhatsAppText(env, toE164, `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`);
      return ok('EVENT_RECEIVED');
    } else {
      // Ofrecer compatibles si detectamos familia en la consulta original o vía IA
      const hints = extractModelHints(enrichedQ);
      if (hints.family && ((env.STRICT_FAMILY_MATCH||'').toString().toLowerCase() !== 'true')) {
        session.stage = 'await_compatibles';
        session.data.pending_query = enrichedQ;
        await saveSession(env, session, now);
        await sendWhatsAppText(env, toE164, `Ese modelo está *sobre pedido* o sin disponibilidad. ¿Quieres que te muestre opciones *compatibles* en otra línea?`);
        return ok('EVENT_RECEIVED');
      }
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
  // “No” → cerrar a idle sin saludo automático
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = 'idle';
    await saveSession(env, session, now);
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

/* Captura UNO A UNO (ventas) */
const FLOW_FACT = ['nombre','rfc','email','calle','numero','colonia','cp'];
const FLOW_SHIP = ['nombre','email','calle','numero','colonia','cp'];
const LABEL = { nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', cp:'Código Postal' };

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

function summaryCart(cart = []) { return cart.map(i => `${i.product?.nombre} x ${i.qty}${i.backorder ? ' (sobre pedido)' : ''}`).join('; '); }
function splitCart(cart = []){ return { inStockList: cart.filter(i => !i.backorder), backOrderList: cart.filter(i => i.backorder) }; }

/* =============== Inventario & Pedido =============== */
function extractModelHints(text='') {
  const t = normalizeWithAliases(text);
  const out = {};
  if (/\bversant\b/.test(t) || /\b(550|560|570|80|180|2100)\b/.test(t)) out.family = 'versant';
  else if (/\bversa[-\s]?link\b/.test(t)) out.family = 'versalink';
  else if (/\balta[-\s]?link\b/.test(t)) out.family = 'altalink';
  else if (/\bdocu(color)?\b/.test(t)) out.family = 'docucolor';
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
    const hit = /\b(versant|550|560|570|80\b|180\b|2100)\b/i.test(s);
    const bad = /(c60|c70|c75|docucolor|prime\s*link|primelink|altalink|versa\s*link)/i.test(s);
    return hit && !bad;
  }
  if (family==='docucolor'){
    const hit = /\b(docucolor|550\/560\/570|550|560|570)\b/i.test(s);
    const bad = /(versant|primelink|altalink|versalink|c60|c70|c75)/i.test(s);
    return hit && !bad;
  }
  if (family==='c70') return /\bc(60|70|75)\b/i.test(s) || s.includes('c60') || s.includes('c70') || s.includes('c75');
  if (family==='primelink') return /\bprime\s*link\b/i.test(s) || /\bprimelink\b/i.test(s);
  if (family==='versalink') return /\bversa\s*link\b/i.test(s) || /\bversalink\b/i.test(s);
  if (family==='altalink') return /\balta\s*link\b/i.test(s) || /\baltalink\b/i.test(s);
  if (family==='apeos') return /\bapeos\b/i.test(s);
  return s.includes(family);
}

/* === findBestProduct: Color y Familia = filtros duros === */
async function findBestProduct(env, queryText, opts = {}) {
  const hints = extractModelHints(queryText);
  const strict = true; // familia siempre duro en R4
  const colorCode = hints.color || extractColorWord(queryText);

  // Si IA extrae familia/color (por enrichQueryFromAI) ya vienen en queryText

  const pick = (arr) => {
    if (!Array.isArray(arr) || !arr.length) return null;

    // Color obligatorio si lo pidieron
    let pool = colorCode ? arr.filter(p => productHasColor(p, colorCode)) : arr.slice();
    if (!pool.length) return null;

    // Familia DURA: filtra; salvo ignoreFamily explícito (compatibles aprobados)
    if (hints.family && !opts.ignoreFamily) {
      pool = pool.filter(p => productMatchesFamily(p, hints.family));
      if (!pool.length) return null;
    }

    // Orden por score: stock > precio asc
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

  // Búsqueda por familia explícita (si hay)
  if (hints.family && !opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%${hints.family}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like},compatible.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      const best = pick(r);
      if (best) return best;
      return null; // familia dura → no regresamos otras familias aquí
    } catch {}
  }

  // Búsqueda general “toner” (sin familia indicada y sin compatibles)
  if (!hints.family || opts.ignoreFamily) {
    try {
      const like = encodeURIComponent(`%toner%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock,tipo,compatible&or=(nombre.ilike.${like},sku.ilike.${like})&order=stock.desc.nullslast,precio.asc&limit=200`
      }) || [];
      // Si ignoreFamily=true, no filtramos por familia; si no, se filtra por hints arriba
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
    // No hay exacto de la familia pedida → ofrecer compatibles
    session.stage = 'await_compatibles';
    session.data.pending_query = enrichedQ;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Ese modelo está *sobre pedido* o sin disponibilidad. ¿Quieres que te muestre opciones *compatibles* en otra línea?`);
    return ok('EVENT_RECEIVED');
  }

  if (best) {
    session.stage = 'ask_qty';
    session.data.cart = session.data.cart || [];
    session.data.last_candidate = best;
    await saveSession(env, session, now);
    const s = numberOrZero(best.stock);
    await sendWhatsAppText(
      env, toE164,
      `${renderProducto(best)}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${s} en stock y el resto sería *sobre pedido*.`
    );
    return ok('EVENT_RECEIVED');
  } else {
    await sendWhatsAppText(env, toE164, `No encontré una coincidencia directa 😕. Te conecto con un asesor…`);
    await notifySupport(env, `Inventario sin match. +${session.from}: ${text}`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }
}

/* ====== Cliente ====== */
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

/* ============================ SOPORTE ============================ */
function extractSvInfo(text) {
  const t = normalizeWithAliases(text);
  const out = {};
  if (/xerox/i.test(t)) out.marca = 'Xerox';
  else if (/fujifilm|fuji\s?film/i.test(t)) out.marca = 'Fujifilm';

  // Modelo tolerante (permite fallos de usuario)
  const m = t.match(/(versant[\s-]*\d+(?:\/\d+)?|versalink\s*\w+|altalink\s*\w+|docucolor\s*[\w/]+|prime\s*link\s*\w+|primelink\s*\w+|c\d{2,4}|b\d{2,4}|\b(550|560|570|80|180|2100)\b)/i);
  if (m) out.modelo = m[0].toUpperCase().replace(/\s+/g,' ');

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
  const t = normalizeWithAliases(text).trim();
  if (field === 'modelo') {
    const m = t.match(/(xerox|fujifilm|fuji\s?film)?\s*(versant[\s-]*\d+(?:\/\d+)?|versalink\s*\w+|altalink\s*\w+|docucolor\s*[\w/]+|prime\s*link\s*\w+|primelink\s*\w+|c\d{2,4}|b\d{2,4}|\b(550|560|570|80|180|2100)\b)/i);
    if (m) {
      if (m[1]) sv.marca = /fuji/i.test(m[1]) ? 'Fujifilm' : 'Xerox';
      sv.modelo = m[2] ? m[2].toUpperCase() : m[0].toUpperCase();
    } else {
      sv.modelo = clean(text);
    }
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
  if (field === 'horario') { const dt = parseNaturalDateTime(text, env); if (dt?.start) sv.when = dt; return; }
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

    // Bienvenida empática (una sola vez)
    if (!sv._welcomed || intent?.forceWelcome) {
      sv._welcomed = true;
      await sendWhatsAppText(env, toE164, `Lamento escuchar la falla 😕. Dime por favor la *marca y el modelo* del equipo y una breve *descripción* del problema.`);
    }

    // Quick help (una sola vez)
    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick);
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    // Prefill cliente
    await preloadCustomerIfAny(env, session);
    const c = session.data.customer || {};
    if (!sv.nombre && truthy(c.nombre)) sv.nombre = c.nombre;
    if (!sv.email && truthy(c.email)) sv.email = c.email;

    // Campos necesarios (orden requerido)
    const needed = [];
    if (!truthy(sv.marca) && !truthy(sv.modelo)) needed.push('modelo');
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
      await saveSession(env, session, now);
      const Q = {
        modelo: '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox Versant 180*)',
        falla: '¿Me describes brevemente la *falla*? (p.ej., “*atasco en fusor*”, “*no imprime*”)',
        calle: '¿En qué *calle* está el equipo?',
        numero: '¿Qué *número* es?',
        colonia: '¿*Colonia*?',
        cp: '¿*Código Postal* (5 dígitos)?',
        horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (ej: “*mañana 12:30*”)',
        nombre: '¿A nombre de quién registramos la visita?',
        email: '¿Cuál es tu *email* para enviarte confirmaciones?'
      };
      await sendWhatsAppText(env, toE164, Q[needed[0]]);
      return ok('EVENT_RECEIVED');
    }

    // Todo listo → agendar
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

/* ============================ FAQs ============================ */
async function maybeFAQ(env, ntext) {
  try {
    const like = encodeURIComponent(`%${ntext.slice(0, 60)}%`);
    const r = await sbGet(env, 'company_info', { query: `select=key,content,tags&or=(key.ilike.${like},content.ilike.${like})&limit=1` });
    if (r && r[0]?.content) return r[0].content;
  } catch {}
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

/* ============================ Fechas ============================ */
function parseNaturalDateTime(text, env) {
  const tz = env.TZ || 'America/Mexico_City';
  const base = new Date(new Date().toLocaleString('en-US', { timeZone: tz }));
  let d = new Date(base);
  let targetDay = null;
  const t = normalizeBase(text);

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

/* ============================ Google Calendar ============================ */
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
    // si nos salimos de la ventana, mover al día hábil siguiente 10:00
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

async function upsertClienteByPhone(env, phone) {
  try {
    const ex = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    if (ex && ex[0]?.id) return ex[0].id;
    const ins = await sbUpsert(env, 'cliente', [{ telefono: phone }], { onConflict: 'telefono', returning: 'representation' });
    return ins?.data?.[0]?.id || null;
  } catch { return null; }
}

/* ============================ Dirección laxa ============================ */
function parseAddressLoose(text=''){
  const out = {};
  const mcp = text.match(/\b(\d{5})\b/); if (mcp) out.cp = mcp[1];
  const mnum = text.match(/\b(\d+[A-Z]?)\b/); if (mnum) out.numero = mnum[1];
  if (out.cp) {
    const pre = text.split(out.cp)[0];
    const parts = pre.split(',').map(s=>s.trim()).filter(Boolean);
    if (parts.length >= 1) out.colonia = parts[parts.length-1];
  }
  const mcalle = text.match(/([A-Za-zÁÉÍÓÚÜÑ0-9 .\-']+)\s+(\d+[A-Z]?)/i);
  if (mcalle) out.calle = clean(mcalle[1]);
  return out;
}

/* ============================ SEPOMEX ============================ */
async function cityFromCP(env, cp) {
  try {
    const r = await sbGet(env, 'sepomex_raw', { query: `select=d_mnpio,d_estado,d_ciudad&d_codigo=eq.${encodeURIComponent(cp)}&limit=1` });
    if (r && r[0]) { return { municipio: r[0].d_mnpio || null, estado: r[0].d_estado || null, ciudad: r[0].d_ciudad || null }; }
  } catch {}
  return null;
}

/* ============================ Supabase helpers ============================ */
function sb(env){ const key = env.SUPABASE_SERVICE_ROLE || env.SUPABASE_KEY; return { url:`${env.SUPABASE_URL}/rest/v1`, key }; }

async function sbGet(env, table, { query='', headers={} }={}) {
  const b = sb(env);
  const url = `${b.url}/${table}${query?`?${query}`:''}`;
  const r = await fetch(url, { headers:{ apikey:b.key, Authorization:`Bearer ${b.key}`, ...headers } });
  if (r.status===204) return [];
  if (!r.ok){ console.warn('sbGet', table, r.status, await r.text()); return null; }
  try { return await r.json(); } catch { return null; }
}

async function sbUpsert(env, table, body, { onConflict='', returning='representation', headers={} }={}) {
  const b = sb(env);
  const url = `${b.url}/${table}`;
  const h = { apikey:b.key, Authorization:`Bearer ${b.key}`, 'Content-Type':'application/json', Prefer:`resolution=merge-duplicates${onConflict?`,on_conflict=${onConflict}`:''},return=${returning}`, ...headers };
  try{
    const r = await fetch(url, { method:'POST', headers:h, body: JSON.stringify(body) });
    const text = await r.text(); let data=null; try{ data = text ? JSON.parse(text) : null; }catch{}
    if(!r.ok) console.warn('sbUpsert', table, r.status, text);
    return { data, status:r.status };
  }catch(e){ console.warn('sbUpsert error', table, e); return { data:null, status:500 }; }
}

async function sbPatch(env, table, body, filter){
  const b = sb(env);
  const url = `${b.url}/${table}?${filter}`;
  try{
    const r = await fetch(url, { method:'PATCH', headers:{ apikey:b.key, Authorization:`Bearer ${b.key}`, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
    if(!r.ok) console.warn('sbPatch', table, r.status, await r.text());
  }catch(e){ console.warn('sbPatch err', table, e); }
}

async function sbRpc(env, fn, args){
  const b = sb(env);
  const url = `${b.url}/rpc/${fn}`;
  try{
    const r = await fetch(url, { method:'POST', headers:{ apikey:b.key, Authorization:`Bearer ${b.key}`, 'Content-Type':'application/json' }, body: JSON.stringify(args || {}) });
    if (!r.ok) { console.warn('sbRpc', fn, r.status, await r.text()); return null; }
    const text = await r.text(); try{ return text ? JSON.parse(text) : null; }catch{ return null; }
  }catch(e){ console.warn('sbRpc err', fn, e); return null; }
}

/* ============================ Sesiones ============================ */
async function loadSession(env, from){
  try{
    const r = await sbGet(env, 'wa_session', { query:`select=from,stage,data,updated_at,expires_at&from=eq.${from}` });
    if (r && r[0]) return r[0];
  }catch(e){ console.warn('loadSession', e); }
  return { from, stage:'idle', data:{} };
}

function promptedRecently(session, key, ms=5*60*1000){
  session.data.prompts = session.data.prompts || {};
  const last = session.data.prompts[key] ? Date.parse(session.data.prompts[key]) : 0;
  const okk = (Date.now() - last) < ms;
  session.data.prompts[key] = new Date().toISOString();
  return okk;
}

async function saveSession(env, session, at=new Date()){
  const days = Number(env.SESSION_TTL_DAYS || 90);
  const exp = new Date(at.getTime()+days*24*60*60*1000).toISOString();
  const body=[{ from:session.from, stage:session.stage||'idle', data:session.data||{}, updated_at:new Date().toISOString(), expires_at: exp }];
  await sbUpsert(env, 'wa_session', body, { onConflict:'from', returning:'minimal' });
}

/* ============================ Cron: recordatorios ============================ */
async function cronReminders(env) {
  const now = new Date();
  const fromISO = new Date(now.getTime() - 2 * 60 * 60 * 1000).toISOString();
  const toISO = new Date(now.getTime() + 26 * 60 * 60 * 1000).toISOString();
  const rows = await sbGet(env, 'orden_servicio', {
    query: `select=id,cliente_id,ventana_inicio,remind_24h_sent,remind_1h_sent,estado&estado=in.(agendado,reprogramado)&ventana_inicio=gte.${fromISO}&ventana_inicio=lte.${toISO}`
  }) || [];
  let sent = 0;
  for (const os of rows) {
    const when = new Date(os.ventana_inicio);
    const soon24h = Date.now() + 24 * 60 * 60 * 1000;
    const soon1h = Date.now() + 60 * 60 * 1000;
    const phone = await phoneForCliente(env, os.cliente_id);
    if (!phone) continue;

    if (!os.remind_24h_sent && Math.abs(+when - soon24h) < 15 * 60 * 1000) {
      await sendWhatsAppText(env, `+${phone}`, `Recordatorio 📅 Mañana tenemos tu visita técnica.`);
      await sbUpsert(env, 'orden_servicio', [{ id: os.id, remind_24h_sent: true }], { returning: 'minimal' });
      sent++;
    }
    if (!os.remind_1h_sent && Math.abs(+when - soon1h) < 15 * 60 * 1000) {
      await sendWhatsAppText(env, `+${phone}`, `Recordatorio ⏰ En 1 hora estaremos contigo para tu visita técnica.`);
      await sbUpsert(env, 'orden_servicio', [{ id: os.id, remind_1h_sent: true }], { returning: 'minimal' });
      sent++;
    }
  }
  return { checked: rows.length, sent };
}

async function phoneForCliente(env, id) {
  if (!id) return null;
  const r = await sbGet(env, 'cliente', { query: `select=telefono&id=eq.${id}&limit=1` });
  return r?.[0]?.telefono || null;
}

/* ============================ Util ============================ */
function ok(msg='OK'){ return new Response(msg, { status: 200 }); }
async function safeJson(req){ try{ return await req.json(); } catch { return {}; } }

function parseCustomerText(text) {
  const out = {}, t = text;
  const mName = t.match(/(?:raz[oó]n social|nombre)\s*[:\-]\s*(.+)$/i); if (mName) out.nombre = clean(mName[1]);
  const mRFC = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i); if (mRFC) out.rfc = mRFC[1].toUpperCase();
  const mMail = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i); if (mMail) out.email = mMail[0].toLowerCase();
  const mCP = t.match(/\b(\d{5})\b/); if (mCP) out.cp = mCP[1];
  const mCalle = t.match(/\b(calle|av(enida)?|avenida|blvd|boulevard|prolongaci[oó]n|camino|andador|privada|paseo|prol\.?)\s+([^\n,]+)\b/i);
  if (mCalle) out.calle = clean(`${mCalle[3]}`);
  const mNum = t.match(/\b(no\.?|n[úu]mero|num)\s*[:\- ]\s*(\d+[A-Z]?)\b/i); if (mNum) out.numero = mNum[2];
  const mCol = t.match(/\b(colonia|col\.)\s*[:\-]?\s*([A-Za-z0-9 áéíóúñ\-\.'\/]+)\b/i);
  if (mCol) out.colonia = clean(mCol[2]);
  else {
    const m2 = t.match(/\b(fracc(ionamiento)?|residencial|barrio|villa[s]?|villas?)\s+([A-Za-z0-9 áéíóúñ\-\.'\/]+)\b/i);
    if (m2) out.colonia = clean(m2[3] || m2[4] || m2[2]);
  }
  const mCity = t.match(/\b(ciudad|cd\.?|municipio)\s*[:\- ]\s*([A-Za-z áéíóúñ\.\-\/]+)\b/i);
  if (mCity) out.ciudad = clean(mCity[2]);
  const mState = t.match(/\b(estado)\s*[:\- ]\s*([A-Za-z áéíóúñ\.\-\/]+)\b/i);
  if (mState) out.estado = clean(mState[2]);
  return out;
}

function displayField(k){
  const map={ nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', ciudad:'Ciudad', cp:'CP' };
  return map[k]||k;
}

function buildResumePrompt(session){
  const st = session?.stage || 'idle';
  if (st === 'await_invoice') return '¿La cotizamos con factura o sin factura?';
  if (st === 'cart_open') return '¿Lo agrego al carrito o prefieres otra opción?';
  if (st && st.startsWith('collect_')) {
    const k = st.replace('collect_','');
    return `¿${displayField(k)}?`;
    }
  if (st === 'sv_collect') {
    const need = session?.data?.sv_need_next || 'modelo';
    const q = {
      modelo: '¿Qué marca y modelo es tu impresora (p.ej., Xerox Versant 180)?',
      falla: 'Cuéntame brevemente la falla (p.ej., “atasco en fusor”, “no imprime”).',
      calle: '¿Cuál es la *calle* donde estará el equipo?',
      numero: '¿Qué *número* es?',
      colonia: '¿*Colonia*?',
      cp: '¿*Código Postal* (5 dígitos)?',
      horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (puedes decir “mañana 12:30”)',
      nombre: '¿A nombre de quién registramos la visita?',
      email: '¿Cuál es tu email?'
    };
    return q[need] || '¿Podrías compartirme el dato pendiente para continuar?';
  }
  if (st === 'sv_scheduled') return '¿Necesitas reprogramar o añadir algún detalle?';
  return '¿En qué te ayudo hoy?';
}
