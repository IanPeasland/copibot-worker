/**
 * CopiBot – Conversacional con IA (OpenAI) + Ventas + Soporte Técnico + GCal
 * FUSIÓN:
 * - Reanudar flujo con estado: await_resume (sin “responde sí/no” obligatorio)
 * - Búsqueda por familia (Versant/VersaLink/AltaLink/DocuColor/Cxx) y color (magenta/amarillo/cyan/negro)
 * - STRICT_FAMILY_MATCH (si no hay match exacto, pregunta por compatibles)
 * - NUEVO: estado await_compatibles para confirmar y buscar ignorando familia
 * - “sí, agrégalo / hazlo” detecta en cualquier parte
 * - “con/sin” → factura
 * - Logger opcional DEBUG_LOG → wa_debug
 */

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);

      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      if (req.method === 'POST' && url.pathname === '/') {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw } = ctx;
        const text = (textRaw || '').trim();
        const lowered = text.toLowerCase();
        const ntext = normalize(text);
        const now = new Date();

        let session = await loadSession(env, from);
        session.data = session.data || {};
        session.stage = session.stage || 'idle';
        session.from = from;

        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // idempotencia
        if (session?.data?.last_mid && session.data.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // Comandos universales
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

        // Saludo estando en flujo activo → preguntar y pausar
        const isGreet = RX_GREET.test(lowered);
        if (isGreet && session.stage !== 'idle') {
          const friendly = await aiSmallTalk(env, session, 'general', text);
          await sendWhatsAppText(env, fromE164, `${friendly}\n¿Deseas continuar con tu trámite?`);
          session.data.last_stage = session.stage;
          session.stage = 'await_resume';
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // Reanudación de flujo
        if (session.stage === 'await_resume') {
          if (RX_YES.test(lowered)) {
            session.stage = session?.data?.last_stage || 'idle';
            await saveSession(env, session, now);
            const prompt = buildResumePrompt(session);
            await sendWhatsAppText(env, fromE164, prompt);
            return ok('EVENT_RECEIVED');
          }
          if (RX_NO.test(lowered)) {
            session.stage = 'idle';
            if (session?.data) delete session.data.last_stage;
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, 'De acuerdo. ¿En qué te ayudo hoy?');
            return ok('EVENT_RECEIVED');
          }
          await sendWhatsAppText(env, fromE164, '¿Deseas continuar con tu trámite?');
          return ok('EVENT_RECEIVED');
        }

        // === Compatibles: etapa para confirmar búsqueda relajada (ignora familia)
        if (session.stage === 'await_compatibles') {
          const baseQ = session.data?.pending_query || ntext || text;
          if (RX_YES.test(lowered)) {
            const best = await findBestProduct(env, baseQ, { ignoreFamily: true });
            if (best) {
              session.stage = 'cart_open';
              session.data.cart = session.data.cart || [];
              session.data.last_candidate = best;
              await saveSession(env, session, now);
              await sendWhatsAppText(env, fromE164, `${renderProducto(best)}\n\n¿Lo agrego o busco otra opción?`);
            } else {
              await sendWhatsAppText(env, fromE164, `No encontré una opción compatible clara 😕. Te conecto con un asesor.`);
              await notifySupport(env, `Compatibles sin match +${from}: ${baseQ}`);
              session.stage = 'idle';
              await saveSession(env, session, now);
            }
            return ok('EVENT_RECEIVED');
          }
          if (RX_NO.test(lowered)) {
            session.stage = 'idle';
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, `Perfecto. ¿En qué más te ayudo?`);
            return ok('EVENT_RECEIVED');
          }
          // Cualquier otro texto en este stage => tratar como consulta de compatibles
          {
            const best = await findBestProduct(env, ntext || text, { ignoreFamily: true });
            if (best) {
              session.stage = 'cart_open';
              session.data.cart = session.data.cart || [];
              session.data.last_candidate = best;
              await saveSession(env, session, now);
              await sendWhatsAppText(env, fromE164, `${renderProducto(best)}\n\n¿Lo agrego o busco otra opción?`);
            } else {
              await sendWhatsAppText(env, fromE164, `Sigo sin ver opción clara. Te conecto con un asesor 👍`);
              await notifySupport(env, `Compatibles sin match (texto nuevo) +${from}: ${text}`);
              session.stage = 'idle';
              await saveSession(env, session, now);
            }
            return ok('EVENT_RECEIVED');
          }
        }

        // Saludo automático en idle
        const mayGreet = isGreet && shouldAutogreet(session, now) && session.stage === 'idle';
        if (mayGreet) {
          const g = await aiSmallTalk(env, session, 'greeting');
          await sendWhatsAppText(env, fromE164, g);
          session.data.last_greet_at = now.toISOString();
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        const hardSupport = RX_SUPPORT.test(ntext);
        const looksInv = RX_INV_Q.test(ntext);

        // FAQs
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        const intent = await aiClassifyIntent(env, text);

        // SOPORTE
        if (hardSupport || intent.intent === 'support' || session.stage?.startsWith('sv_')) {
          const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, intent);
          return handled;
        }

        // VENTAS
        if (session.stage === 'cart_open') {
          const handled = await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
          return handled;
        }
        if (session.stage === 'await_invoice') {
          const handled = await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
          return handled;
        }
        if (session.stage?.startsWith('collect_')) {
          const handled = await handleCollectSequential(env, session, fromE164, text, now);
          return handled;
        }

        // Arranque ventas (idle)
        if (session.stage === 'idle' && looksInv) {
          const best = await findBestProduct(env, ntext);
          const hints = extractModelHints(ntext || text);
          const strict = (env.STRICT_FAMILY_MATCH || '').toString().toLowerCase() === 'true';
          if (!best && hints.family && strict) {
            session.stage = 'await_compatibles';
            session.data.pending_query = ntext || text;
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164,
              `No encontré disponibilidad *${hints.family}* ahora mismo 😕. ¿Te muestro opciones *compatibles*?`);
            return ok('EVENT_RECEIVED');
          }
          if (best) {
            session.stage = 'cart_open';
            session.data.cart = session.data.cart || [];
            session.data.last_candidate = best;
            await saveSession(env, session, now);
            await sendWhatsAppText(
              env,
              fromE164,
              `${renderProducto(best)}\n\n¿Te funciona? Puedo *agregarlo* o *buscar otra opción*.`
            );
            return ok('EVENT_RECEIVED');
          } else {
            await sendWhatsAppText(env, fromE164, `No encontré una coincidencia directa 😕. Te conecto con un asesor humano…`);
            await notifySupport(env, `Inventario sin match. +${from}: ${text}`);
            await saveSession(env, session, now);
            return ok('EVENT_RECEIVED');
          }
        }

        // Small talk
        if (intent.intent === 'smalltalk') {
          const reply = await aiSmallTalk(env, session, 'general', text);
          await sendWhatsAppText(env, fromE164, `${reply}`);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // Fallback
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

/* ============================ Regex ============================ */
const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;
const RX_INV_Q  = /(toner|tóner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro)/i;
const RX_SUPPORT = /(soporte|servicio|visita|no imprime|atasc(a|o)|atasco|falla|error|mantenimiento|se atora|se traba|atasca el papel|saca el papel|mancha|línea|linea)/i;

const RX_ADD_ITEM = /\b(agrega(?:me)?|añade|mete|pon|suma|incluye)\b/i;
const RX_DONE = /\b(es(ta)? (todo|suficiente)|ser[ií]a todo|nada m[aá]s|con eso|as[ií] est[aá] bien|ya qued[oó]|listo|est[aá] listo)\b/i;
const RX_NEG_NO = /\bno\b/i;
const RX_WANT_QTY = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+(\d+)\b/i;

// Sí / No
const RX_YES = /\b(s[ií]|sí|si|claro|va|dale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo)\b/i;
const RX_NO  = /\b(no|luego|despu[eé]s|pausa|ahorita no|cancelar|det[eé]n|mejor no)\b/i;

/* ============================ Ayudas ============================ */
const firstWord = (s='') => (s||'').trim().split(/\s+/)[0] || '';
const toTitleCase = (s='') => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
function normalize(s=''){ return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/\s+/g,' ').trim().toLowerCase(); }
function clean(s=''){ return s.replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).trim()!==''; }
function fmtDate(d, tz){ try{ return new Intl.DateTimeFormat('es-MX',{dateStyle:'full',timeZone:tz}).format(new Date(d)); }catch{ return new Date(d).toLocaleDateString('es-MX'); } }
function fmtTime(d, tz){ try{ return new Intl.DateTimeFormat('es-MX',{timeStyle:'short',timeZone:tz}).format(new Date(d)); }catch{ const x=new Date(d); return `${x.getHours()}:${String(x.getMinutes()).padStart(2,'0')}`; } }
function formatMoneyMXN(n){ const v=Number(n||0); try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }catch{ return `$${v.toFixed(2)}`; } }
function numberOrZero(n){ const v=Number(n||0); return Number.isFinite(v)?v:0; }
function priceWithIVA(n){ const v=Number(n||0); return `${formatMoneyMXN(v)} + IVA`; }
function shouldAutogreet(session, now){
  const last = session?.data?.last_greet_at ? Date.parse(session.data.last_greet_at) : 0;
  return (now.getTime() - last) > 24*60*60*1000;
}
function promptedRecently(session, key, ms=5*60*1000){
  session.data.prompts = session.data.prompts || {};
  const last = session.data.prompts[key] ? Date.parse(session.data.prompts[key]) : 0;
  const ok = (Date.now() - last) < ms;
  session.data.prompts[key] = new Date().toISOString();
  return ok;
}

/* ============================ IA ============================ */
async function aiCall(env, messages, {json=false}={}){
  const OPENAI_KEY = env.OPENAI_API_KEY || env.OPENAI_KEY;
  const MODEL = env.LLM_MODEL || env.OPENAI_NLU_MODEL || env.OPENAI_FALLBACK_MODEL || 'gpt-4o-mini';
  if (!OPENAI_KEY) return null;
  const body = {
    model: MODEL,
    messages,
    temperature: json ? 0 : 0.6,
    ...(json ? { response_format: { type: "json_object" } } : {})
  };
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
  const sys = `Eres CopiBot, asistente cálido, claro y breve de CP Digital (es-MX).
- Responde con tono humano (máximo 1 emoji).
- Evita listas innecesarias; conversa como persona.`; 
  let prompt = '';
  if (mode === 'greeting') {
    prompt = `Saluda de forma breve y cálida. Incluye el nombre si lo tienes (“${nombre}”). Cierra con: "¿Qué necesitas hoy?"`;
  } else if (mode === 'fallback') {
    prompt = `El usuario dijo: """${userText}""".
Contesta breve, útil y amable. Si no hay contexto, ofrece inventario o soporte.`;
  } else {
    prompt = `El usuario dijo: """${userText}""". Responde breve y amigable.`;
  }
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 🙌 ¿En qué te ayudo hoy?`);
}

async function aiClassifyIntent(env, text){
  const sys = `Clasifica texto del usuario en JSON.
Campos: intent in ["support","sales","faq","smalltalk"], severity in ["alta","media","baja"] (si intent="support").
Reglas:
- "atasco", "no imprime", "error", "servicio", "visita" => support
- "toner", "precio", "SKU", colores => sales
- "quiénes son", "horarios", "dónde están" => faq
- otro => smalltalk
Responde sólo JSON.`;
  const out = await aiCall(env, [{role:'system', content: sys},{role:'user', content: text}], {json:true});
  try{ return JSON.parse(out||'{}'); }catch{ return { intent:'smalltalk' }; }
}

/* ============================ WhatsApp ============================ */
async function sendWhatsAppText(env, toE164, body) {
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
function parseQty(text, fallback = 1) {
  const m = text.match(RX_WANT_QTY);
  const q = m ? Number(m[2]) : null;
  return Number.isFinite(q) && q > 0 ? q : fallback;
}
function pushCart(session, product, qty, backorder = false) {
  session.data = session.data || {};
  session.data.cart = session.data.cart || [];
  const key = product?.id || product?.sku || product?.nombre;
  const existing = session.data.cart.find(i => i.key === key);
  if (existing) existing.qty += qty;
  else session.data.cart.push({ key, product, qty, backorder });
}
function renderProducto(p) {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : '';
  const marca = p.marca ? `\nMarca: ${p.marca}` : '';
  const s = numberOrZero(p.stock);
  const stockLine = s > 0
    ? `${s} pzas en stock`
    : `0 pzas — *sobre pedido* (lo pedimos para ti)`;
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\nEste suele ser el indicado para tu equipo.`;
}

async function handleCartOpen(env, session, toE164, text, lowered, ntext, now) {
  session.data = session.data || {};
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate) {
      pushCart(session, session.data.last_candidate, 1, (numberOrZero(session.data.last_candidate.stock) <= 0));
    }
    session.stage = 'await_invoice';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Perfecto 🙌 ¿La cotizamos *con factura* o *sin factura*?`);
    return ok('EVENT_RECEIVED');
  }

  // “sí, agrégalo” / cantidades
  const RX_YES_CONFIRM = /\b(s[ií]|sí|si|claro|va|dale|correcto|ok|afirmativo|hazlo|agr[eé]ga(lo)?|añade|m[eé]te|pon(lo)?)\b/i;
  if (RX_YES_CONFIRM.test(lowered) || RX_WANT_QTY.test(lowered)) {
    const qty = parseQty(lowered, 1);
    const cand = session.data?.last_candidate;
    if (cand) {
      pushCart(session, cand, qty, (numberOrZero(cand.stock) <= 0));
      await saveSession(env, session, now);
      await sendWhatsAppText(
        env,
        toE164,
        `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(cand.precio)}${(numberOrZero(cand.stock) <= 0 ? ' (sobre pedido)' : '')}\n\n¿Deseas agregar algo más?`
      );
      await logDecision(env, { type:'cart_yes', from: session.from, stage: session.stage, text, last_candidate: cand?.sku || null });
      return ok('EVENT_RECEIVED');
    }
  }

  if (RX_ADD_ITEM.test(lowered)) {
    const cleanQ = lowered.replace(RX_ADD_ITEM, '').trim() || ntext;
    const best = await findBestProduct(env, cleanQ);
    if (best) {
      const qty = parseQty(lowered, 1);
      pushCart(session, best, qty, (numberOrZero(best.stock) <= 0));
      session.data.last_candidate = best;
      await saveSession(env, session, now);
      await sendWhatsAppText(
        env,
        toE164,
        `Sumé:\n• ${best.nombre} x ${qty} ${priceWithIVA(best.precio)}${(numberOrZero(best.stock) <= 0 ? ' (sobre pedido)' : '')}\n\n¿Quieres agregar algo más?`
      );
      return ok('EVENT_RECEIVED');
    } else {
      await sendWhatsAppText(env, toE164, `No encontré coincidencia directa 😕. ¿Busco otra opción o lo revisa un asesor?`);
      await notifySupport(env, `Inventario sin match (agrega). ${toE164}: ${text}`);
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }
  }

  if (RX_INV_Q.test(ntext)) {
    const alt = await findBestProduct(env, ntext);
    const hints = extractModelHints(ntext);
    const strict = (env.STRICT_FAMILY_MATCH || '').toString().toLowerCase() === 'true';
    if (!alt && hints.family && strict) {
      session.stage = 'await_compatibles';
      session.data.pending_query = ntext;
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `No encontré *${hints.family}* en catálogo ahora. ¿Quieres ver opciones *compatibles*?`);
      return ok('EVENT_RECEIVED');
    }
    if (alt) {
      session.data.last_candidate = alt;
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `${renderProducto(alt)}\n\n¿Lo agrego o prefieres otra opción?`);
      return ok('EVENT_RECEIVED');
    }
  }

  if (/^(ok|gracias|como estas|¿?cómo estás\??|hola)$/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', text);
    await sendWhatsAppText(env, toE164, `${friendly}\nSi gustas, puedo agregar el visto, buscar otro o finalizar.`);
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  await sendWhatsAppText(env, toE164, `Te leo 😊. Puedo agregar el artículo visto, buscar otro o finalizar si ya está completo.`);
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

async function handleAwaitInvoice(env, session, toE164, lowered, now, originalText='') {
  const yes = /\b(s[ií]|sí|si|con(\s+factura)?|factura|con)\b/i.test(lowered);
  const no  = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);

  session.data = session.data || {};
  session.data.customer = session.data.customer || {};

  if (!yes && !no && /hola|cómo estás|como estas|gracias/i.test(lowered)) {
    const friendly = await aiSmallTalk(env, session, 'general', originalText);
    if (!promptedRecently(session, 'invoice', 3*60*1000)) {
      await sendWhatsAppText(env, toE164, `${friendly}\nPor cierto, ¿la quieres *con factura* o *sin factura*?`);
    } else {
      await sendWhatsAppText(env, toE164, friendly);
    }
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');
  }

  if (yes) {
    session.data.requires_invoice = true;
    session.stage = 'collect_nombre';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Perfecto. ¿Me compartes *Nombre / Razón Social*?`);
    return ok('EVENT_RECEIVED');
  }
  if (no) {
    session.data.requires_invoice = false;
    session.stage = 'collect_nombre';
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `Va. ¿Con qué *Nombre / contacto* dejamos la entrega?`);
    return ok('EVENT_RECEIVED');
  }

  if (!promptedRecently(session, 'invoice', 2*60*1000)) {
    await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  }
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

/* Captura UNO A UNO */
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

  const idx = list.indexOf(field);
  const nextField = list[idx+1];

  if (nextField){
    session.stage = `collect_${nextField}`;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿${LABEL[nextField]}?`);
    return ok('EVENT_RECEIVED');
  }

  const res = await createOrderFromSession(env, session, toE164);
  if (res?.ok) {
    const { inStockList, backOrderList } = splitCart(session.data.cart);
    const notaStock = [
      inStockList.length ? `Artículos con stock:\n${inStockList.map(i=>`• ${i.product?.nombre} x ${i.qty}`).join('\n')}` : '',
      backOrderList.length ? `\nSobre pedido:\n${backOrderList.map(i=>`• ${i.product?.nombre} x ${i.qty}`).join('\n')}` : ''
    ].filter(Boolean).join('\n');
    await sendWhatsAppText(env, toE164, `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(res.total)} + IVA\nUn asesor te confirmará entrega y forma de pago.`);
    await notifySupport(env, `Nuevo pedido #${res.pedido_id}\nCliente: ${c.nombre} (${toE164})\n${notaStock || '—'}\nFactura: ${session.data.requires_invoice ? 'Sí' : 'No'}`);
  } else {
    await sendWhatsAppText(env, toE164, `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`);
    await notifySupport(env, `Pedido (parcial) ${toE164}. Revisar en Supabase.\nError: ${res?.error || 'N/A'}`);
  }

  session.stage = 'idle';
  session.data.cart = [];
  await saveSession(env, session, now);

  const close = await aiSmallTalk(env, session, 'general', 'cierre-pedido');
  await sendWhatsAppText(env, toE164, close || `¿En qué más te ayudo hoy? 😊`);
  return ok('EVENT_RECEIVED');
}

function summaryCart(cart = []) {
  return cart.map(i => `${i.product?.nombre} x ${i.qty}${i.backorder ? ' (sobre pedido)' : ''}`).join('; ');
}
function splitCart(cart = []){
  const inStockList = cart.filter(i => !i.backorder);
  const backOrderList = cart.filter(i => i.backorder);
  return { inStockList, backOrderList };
}

/* =============== Inventario & Pedido =============== */
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
  const s = normalize([p?.nombre, p?.sku].join(' ')); // FIX: comilla correcta
  const map = {
    amarillo: ['amarillo','yellow','ylw','y'],
    magenta: ['magenta','m '],        // espacio para evitar falsos positivos
    cyan: ['cyan','cian',' c '],
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

async function findBestProduct(env, queryText, opts = {}) {
  let res = null;
  try {
    res = await sbRpc(env, 'match_products_trgm', { q: queryText, match_count: 8 });
  } catch(e) {}

  if (!Array.isArray(res) || !res.length) {
    try {
      const like = encodeURIComponent(`%${queryText.slice(0, 60)}%`);
      const r = await sbGet(env, 'producto_stock_v', {
        query: `select=id,nombre,marca,sku,precio,stock&or=(nombre.ilike.${like},sku.ilike.${like},marca.ilike.${like})&order=stock.desc.nullslast&limit=8`
      });
      res = r || [];
    } catch {}
  }

  if (!Array.isArray(res) || !res.length) return null;

  const hints = extractModelHints(queryText);
  const color = extractColor(queryText);
  const strict = (env.STRICT_FAMILY_MATCH || '').toString().toLowerCase() === 'true';

  let pool = res.slice();

  // filtrar por color siempre
  pool = pool.filter(p => productHasColor(p, color));

  // familia: sólo si hay pista de familia y NO estamos ignorando familia
  if (hints.family && !opts.ignoreFamily) {
    const famPool = pool.filter(p => productMatchesFamily(p, hints.family));
    if (famPool.length) {
      pool = famPool;
    } else if (strict) {
      return null; // respetar estricto
    }
  }

  pool.sort((a,b) => numberOrZero(b.score||0) - numberOrZero(a.score||0));
  return pool[0] || null;
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

    // decremento de stock simple
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, 'producto_stock_v', { query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1` });
        const current = numberOrZero(row?.[0]?.stock);
        const toDec = Math.min(current, Number(it.qty||0));
        if (toDec > 0) {
          await sbRpc(env, 'decrement_stock', { in_sku: sku, in_by: toDec });
        }
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

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  session.data = session.data || {};
  session.data.sv = session.data.sv || {};
  const sv = session.data.sv;

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
      modelo: '¿Qué marca y modelo es tu impresora (p.ej., Xerox Versant 180)?',
      falla: 'Cuéntame brevemente la falla (p.ej., “atasco en fusor”, “no imprime”).',
      calle: '¿Cuál es la *calle* donde estará el equipo?',
      numero: '¿Qué *número* es?',
      colonia: '¿*Colonia*?',
      cp: '¿*Código Postal* (5 dígitos)?',
      horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (puedes decir “mañana 12:30”)'
    }[needed[0]];
    await sendWhatsAppText(env, toE164, q);
    return ok('EVENT_RECEIVED');
  }

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
  const m = text.match(/\b(\d{1,2})(?:[:\.](\d{2}))?\s*(am|pm)?\b/i);
  if (m) {
    hour = Number(m[1]); minute = m[2]?Number(m[2]):0;
    const ampm = (m[3]||'').toLowerCase();
    if (ampm==='pm' && hour<12) hour+=12;
    if (ampm==='am' && hour===12) hour=0;
  } else if (/\b(mediod[ií]a)\b/i.test(text)) { hour = 12; minute=0; }

  if (targetDay===null && hour===null) return null;
  if (hour===null) hour = 12;

  d.setHours(hour, minute, 0, 0);
  const start = d.toISOString();
  const end = new Date(d.getTime()+60*60*1000).toISOString();
  return { start, end };
}
function clampToWindow(when, tz) {
  const start = new Date(when.start);
  const sH = Number(new Intl.DateTimeFormat('es-MX', { hour:'2-digit', hour12:false, timeZone:tz }).format(start));
  let newStart = new Date(start);
  if (sH < 10) newStart.setHours(10,0,0,0);
  if (sH >= 15) newStart.setHours(14,0,0,0);
  const newEnd = new Date(newStart.getTime()+60*60*1000);
  return { start: newStart.toISOString(), end: newEnd.toISOString() };
}

/* ============================ Google Calendar ============================ */
async function gcalToken(env) {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON to=python.exec code_executor=1 어요 to=python code
::contentReference[oaicite:0]{index=0}
