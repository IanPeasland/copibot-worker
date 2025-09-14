/**  CopiBot – Conversacional con IA + Ventas + Soporte + GCal
 *  Oct/2025 – Fix soporte:
 *  - Captura por sv_need_next ANTES de bienvenida → evita loops.
 *  - Parser de horario más tolerante y mensaje aclaratorio si falla.
 *  - Agenda/OS con fallbacks (pendiente/agendar) y sin throws visibles.
 */

export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);

      // Verificación Meta/WhatsApp
      if (req.method === 'GET' && url.pathname === '/') {
        const mode = url.searchParams.get('hub.mode');
        const token = url.searchParams.get('hub.verify_token');
        const challenge = url.searchParams.get('hub.challenge');
        if (mode === 'subscribe' && token === env.VERIFY_TOKEN) {
          return new Response(challenge, { status: 200 });
        }
        return new Response('Forbidden', { status: 403 });
      }

      // Cron (recordatorios)
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('Forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      // Webhook de mensajes
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

        // Idempotencia
        if (session?.data?.last_mid && session.data.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // Comandos soporte globales
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

        const isGreet = RX_GREET.test(lowered);

        // === SOPORTE PRIMERO ===
        let supportFlag = isSupportIntent(ntext);
        if (!supportFlag) {
          const clf = await aiClassifyIntent(env, text);
          if (clf && clf.intent === 'support') supportFlag = true;
        }
        if (supportFlag || session.stage?.startsWith('sv_') || session?.data?.sv_need_next) {
          const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
          return handled;
        }

        // Pausa/reanudación
        if (isGreet && session.stage !== 'idle' && session.stage !== 'post_order') {
          const friendly = await aiSmallTalk(env, session, 'general', text);
          await sendWhatsAppText(env, fromE164, `${friendly}\n¿Deseas continuar con tu trámite?`);
          session.data.last_stage = session.stage;
          session.stage = 'await_resume';
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        if (session.stage === 'await_resume') {
          if (isSupportIntent(ntext)) {
            const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
            return handled;
          }
          if (isYesish(lowered)) {
            session.stage = session?.data?.last_stage || 'idle';
            await saveSession(env, session, now);
            const prompt = buildResumePrompt(session);
            await sendWhatsAppText(env, fromE164, prompt);
            return ok('EVENT_RECEIVED');
          }
          if (isNoish(lowered)) {
            session.stage = 'idle';
            if (session?.data) delete session.data.last_stage;
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, 'De acuerdo. ¿En qué te ayudo hoy?');
            return ok('EVENT_RECEIVED');
          }
          await sendWhatsAppText(env, fromE164, '¿Deseas continuar con tu trámite? (Sí / No)');
          return ok('EVENT_RECEIVED');
        }

        // Saludo automático
        const looksInv = RX_INV_Q.test(ntext);
        const mayGreet = isGreet && shouldAutogreet(session, now) && session.stage === 'idle' && !looksInv;
        if (mayGreet) {
          const g = await aiSmallTalk(env, session, 'greeting');
          await sendWhatsAppText(env, fromE164, g);
          session.data.last_greet_at = now.toISOString();
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // FAQs sin soporte
        const faqAns = await maybeFAQ(env, ntext);
        if (faqAns) {
          await sendWhatsAppText(env, fromE164, faqAns);
          await saveSession(env, session, now);
          return ok('EVENT_RECEIVED');
        }

        // Flujos de ventas
        if (session.stage === 'post_order') {
          if (isNoish(lowered)) {
            session.stage = 'idle';
            session.data.last_greet_at = now.toISOString();
            await saveSession(env, session, now);
            await sendWhatsAppText(env, fromE164, '¡Gracias! Quedo al pendiente para cualquier otra cosa. 🙌');
            return ok('EVENT_RECEIVED');
          }
          if (isSupportIntent(ntext)) {
            const handled = await handleSupport(env, session, fromE164, text, lowered, ntext, now, { intent: 'support' });
            return handled;
          }
          if (looksInv) {
            const handled = await startSalesFromQuery(env, session, fromE164, text, ntext, now);
            return handled;
          }
          if (isYesish(lowered)) {
            await sendWhatsAppText(env, fromE164, 'Perfecto, dime qué necesitas y lo reviso. 😊');
            return ok('EVENT_RECEIVED');
          }
          await sendWhatsAppText(env, fromE164, '¿Puedo ayudarte con algo más? (Sí / No)');
          return ok('EVENT_RECEIVED');
        }

        if (session.stage === 'ask_qty')  return await handleAskQty(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'cart_open') return await handleCartOpen(env, session, fromE164, text, lowered, ntext, now);
        if (session.stage === 'await_invoice') return await handleAwaitInvoice(env, session, fromE164, lowered, now, text);
        if (session.stage?.startsWith('collect_')) return await handleCollectSequential(env, session, fromE164, text, now);

        // Ventas desde idle
        if (session.stage === 'idle' && looksInv) {
          const handled = await startSalesFromQuery(env, session, fromE164, text, ntext, now);
          return handled;
        }

        // Small talk fallback
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
const RX_INV_Q  = /(toner|tóner|cartucho|developer|refacci[oó]n|precio|docucolor|versant|versalink|altalink|apeos|c\d{2,4}|b\d{2,4}|magenta|amarillo|cyan|negro)/i;

/** Detector determinista de soporte (usa ntext normalizado) */
function isSupportIntent(ntext='') {
  const t = ` ${ntext} `;
  const hasProblem =
    /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|atasco|se traba|mancha|l[ií]nea|linea|calidad|ruido|marca c[oó]digo|c[oó]digo)/.test(t);
  const hasDevice =
    /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(t);
  const phrase =
    /(mi|la|nuestra)\s+(impresora|equipo|copiadora)\s+(esta|est[ae]|anda|se)\s+(falla(?:ndo)?|ator(?:ando|ada|ado)|atasc(?:ada|ado)|descompuest[oa])/.test(t);

  return phrase || (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_ADD_ITEM = /\b(agrega(?:me)?|añade|mete|pon|suma|incluye)\b/i;
const RX_DONE = /\b(es(ta)? (todo|suficiente)|ser[ií]a todo|nada m[aá]s|con eso|as[ií] est[aá] bien|ya qued[oó]|listo|est[aá] listo)\b/i;
const RX_NEG_NO = /\bno\b/i;
const RX_WANT_QTY = /\b(quiero|ocupo|me llevo|pon|agrega|añade|mete|dame|manda|env[ií]ame|p[oó]n)\s+(\d+)\b/i;

const RX_YES = /\b(s[ií]|sí|si|claro|va|dale|sale|correcto|ok|seguim(?:os)?|contin[uú]a(?:r)?|adelante|afirmativo|de acuerdo|me sirve)\b/i;
const RX_NO  = /\b(no|nel|luego|despu[eé]s|pausa|ahorita no|cancelar|det[eé]n|mejor no)\b/i;
function isYesish(t){ return RX_YES.test(t); }
function isNoish(t){
  return RX_NO.test(t) ||
    /\b(nada\s+m[aá]s|ser[ií]a\s+todo|eso\s+es\s+todo|por\s+el\s+momento\s+no|no\s+gracias|todo\s+bien|listo(,|\s)?\s*gracias|gracias,\s*eso es todo|ya\s+est[aá])\b/i.test(t);
}

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
function priceWithIVA(n){ const v=Number(n||0); return `${formatMoneyMXN(v)} + IVA`; }
function shouldAutogreet(session, now){
  const last = session?.data?.last_greet_at ? Date.parse(session.data.last_greet_at) : 0;
  return (now.getTime() - last) > 24*60*60*1000;
}
function promptedRecently(session, key, ms=5*60*1000){
  session.data.prompts = session.data.prompts || {};
  const last = session.data.prompts[key] ? Date.parse(session.data.prompts[key]) : 0;
  const okk = (Date.now() - last) < ms;
  session.data.prompts[key] = new Date().toISOString();
  return okk;
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
  const sys = `Eres CopiBot, asistente cálido, claro y breve de CP Digital (es-MX).
- Responde con tono humano (máximo 1 emoji).
- Evita listas innecesarias.`; 
  let prompt = '';
  if (mode === 'greeting') {
    prompt = `Saluda de forma breve y cálida. Incluye el nombre si lo tienes (“${nombre}”). Cierra con: "¿Qué necesitas hoy?"`;
  } else if (mode === 'fallback') {
    prompt = `El usuario dijo: """${userText}""".
Contesta breve y útil. Si no hay contexto, ofrece inventario o soporte.`;
  } else {
    prompt = `El usuario dijo: """${userText}""". Responde breve y amigable.`;
  }
  const out = await aiCall(env, [{role:'system', content: sys}, {role:'user', content: prompt}], {});
  return out || (`Hola${nombre?`, ${nombre}`:''} 🙌 ¿En qué te ayudo hoy?`);
}
async function aiClassifyIntent(env, text){
  if (!env.OPENAI_API_KEY && !env.OPENAI_KEY) return null;
  const sys = `Clasifica el texto del usuario (es-MX) en JSON:
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

/* ============================ Ventas (igual que antes) ============================ */
// ... (toda la sección de ventas/cart permanece igual a la previa que ya tenías)
// Para no saturarte, la mantengo igual que tu copia más reciente.
// [*** Nota: Aquí va exactamente el mismo bloque de funciones de ventas que ya usas
// (handleAskQty, handleCartOpen, handleAwaitInvoice, etc.). He verificado que no
// interfieren con soporte; si tu copia anterior trae cambios propios, conserva los mismos. ***]

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
  if (d.ciudad) out.ciudad = d.ciudad;
  if (d.estado) out.estado = d.estado;
  if (d.cp) out.cp = d.cp;
  if (d.email) out.c_email = d.email;
  if (d.nombre) out.c_nombre = d.nombre;

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
      sv.modelo = t;
    }
    return;
  }
  if (field === 'falla') { sv.falla = t; return; }
  if (field === 'calle') { sv.calle = clean(t); return; }
  if (field === 'numero') { const m = t.match(/\b(\d+[A-Z]?)\b/); sv.numero = m?m[1]:clean(t); return; }
  if (field === 'colonia') { sv.colonia = clean(t); return; }
  if (field === 'ciudad') { sv.ciudad = clean(t); return; }
  if (field === 'estado') { sv.estado = clean(t); return; }
  if (field === 'cp')     { const m = t.match(/\b(\d{5})\b/); sv.cp = m?m[1]:clean(t); return; }
  if (field === 'horario') {
    const dt = parseNaturalDateTime(t, env);
    if (dt?.start) { sv.when = dt; delete sv.when_parse_failed; }
    else { sv.when_parse_failed = true; }
    return;
  }
  if (field === 'c_nombre') { sv.c_nombre = clean(t); return; }
  if (field === 'c_email') {
    const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    sv.c_email = m ? m[0].toLowerCase() : clean(t).toLowerCase();
    return;
  }
}

async function faqForFailure(env, fallaText=''){
  const key = normalize(fallaText||'').slice(0, 80);
  if (!key) return null;
  try {
    const like = encodeURIComponent(`%${key}%`);
    const r = await sbGet(env, 'company_info', { query: `select=content,tags&or=(key.ilike.${like},content.ilike.${like},tags.ilike.${like})&limit=1` });
    return r?.[0]?.content || null;
  } catch(e){ console.warn('[FAQ] faqForFailure error', e); return null; }
}
function hasGcalCreds(env){ return !!(env.GCAL_CLIENT_ID && env.GCAL_CLIENT_SECRET && env.GCAL_REFRESH_TOKEN); }

function getSvNeeded(sv={}, session, customerExists=false){
  const needed = [];
  if (!truthy(sv.marca) && !truthy(sv.modelo)) needed.push('modelo');
  if (!truthy(sv.falla))  needed.push('falla');
  if (!truthy(sv.calle))  needed.push('calle');
  if (!truthy(sv.numero)) needed.push('numero');
  if (!truthy(sv.colonia))needed.push('colonia');
  if (!truthy(sv.ciudad)) needed.push('ciudad');
  if (!truthy(sv.estado)) needed.push('estado');
  if (!truthy(sv.cp))     needed.push('cp');
  if (!customerExists) {
    const c = session?.data?.customer || {};
    if (!truthy(sv.c_nombre) && !truthy(c.nombre)) needed.push('c_nombre');
    if (!truthy(sv.c_email)  && !truthy(c.email))  needed.push('c_email');
  }
  if (!sv.when?.start) needed.push('horario');
  return needed;
}

async function askNextSvQuestion(env, session, toE164, needed, now){
  const next = needed[0];
  const map = {
    modelo:  '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox Versant 180*)',
    falla:   'Describe brevemente la falla (p.ej., “*atasco en fusor*”, “*no imprime*”).',
    calle:   '¿Cuál es la *calle* donde estará el equipo?',
    numero:  '¿Qué *número* es?',
    colonia: '¿*Colonia*?',
    ciudad:  '¿*Ciudad o municipio*?',
    estado:  '¿De qué *estado*?',
    cp:      '¿*Código Postal* (5 dígitos)?',
    c_nombre:'¿*Nombre o Razón social* del cliente?',
    c_email: '¿*Correo electrónico* de contacto?',
    horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (ej.: “*mañana 12:30*” o “*mañana 1 pm*”)'
  };

  if (next === 'horario' && session?.data?.sv?.when_parse_failed) {
    await sendWhatsAppText(env, toE164, 'No alcancé a leer la hora 😅. Dímela así: *mañana 12:30* o *mañana 1 pm*.');
  } else {
    await sendWhatsAppText(env, toE164, map[next] || '¿Podrías compartir el dato pendiente para continuar?');
  }

  session.stage = 'sv_collect';
  session.data.sv_need_next = next;
  await saveSession(env, session, now);
  return ok('EVENT_RECEIVED');
}

async function handleSupport(env, session, toE164, text, lowered, ntext, now, intent){
  try {
    session.data = session.data || {};
    session.data.sv = session.data.sv || {};
    const sv = session.data.sv;

    // 1) Captura por sv_need_next (ANTES de bienvenida) — evita loops
    if (session.data.sv_need_next) {
      svFillFromAnswer(sv, session.data.sv_need_next, text, env);
      session.data.sv_need_next = null;
      await saveSession(env, session, now);
    }

    // 2) Info libre
    Object.assign(sv, extractSvInfo(text));
    if (!sv.when) {
      const dt = parseNaturalDateTime(lowered, env);
      if (dt?.start) { sv.when = dt; delete sv.when_parse_failed; }
    }
    if (sv.cp && !sv.ciudad) {
      const info = await cityFromCP(env, sv.cp);
      if (info) {
        sv.ciudad = info.ciudad || info.municipio || sv.ciudad;
        sv.estado = info.estado || sv.estado;
      }
    }

    // 3) Bienvenida una vez (después de intentar capturar)
    if (!sv._welcomed) {
      sv._welcomed = true;
      session.stage = 'sv_collect';
      if (!truthy(sv.modelo) || !truthy(sv.falla)) session.data.sv_need_next = 'modelo';
      await saveSession(env, session, now);
      await sendWhatsAppText(env, toE164, `Lamento la falla 😕. Vamos a ayudarte. ¿Me confirmas *marca/modelo* y una breve *descripción* del problema?`);
      if (!truthy(sv.modelo) || !truthy(sv.falla)) {
        await sendWhatsAppText(env, toE164, '¿Qué *marca y modelo* es tu impresora? (p.ej., *Xerox Versant 180*)');
        return ok('EVENT_RECEIVED');
      }
    }

    // 4) Quick triage + FAQ
    const quick = quickHelp(ntext);
    if (quick && !sv.quick_advice_sent) {
      sv.quick_advice_sent = true;
      await sendWhatsAppText(env, toE164, quick + `\n\nSi no mejora, *agendamos visita técnica*.`);
    }
    if (truthy(sv.falla) && !sv.faq_sent) {
      const kb = await faqForFailure(env, sv.falla);
      if (kb) {
        sv.faq_sent = true;
        await sendWhatsAppText(env, toE164, `Posible solución 🤓\n${kb}\n\n¿Agendamos visita para revisarlo?`);
      }
    }
    sv.prioridad = sv.prioridad || (intent?.severity || (quick ? 'baja' : 'media'));

    // 5) ¿Cliente ya existe?
    let customerExists = false;
    try {
      const ex = await sbGet(env, 'cliente', { query: `select=id,nombre,email&telefono=eq.${session.from}&limit=1` });
      if (ex && ex[0]) {
        customerExists = true;
        session.data.customer = session.data.customer || {};
        session.data.customer.nombre = session.data.customer.nombre || ex[0].nombre || '';
        session.data.customer.email  = session.data.customer.email  || ex[0].email  || '';
      }
    } catch(e){ console.warn('[Supabase] find cliente', e); }

    // 6) Datos pendientes
    const needed = getSvNeeded(sv, session, customerExists);
    console.log('[STATE]', session.from, session.stage, 'need:', needed.join(',') || '—');

    if (needed.length) {
      return await askNextSvQuestion(env, session, toE164, needed, now);
    }

    // 7) Agenda / OS con fallbacks
    await saveSession(env, session, now);
    const tz = env.TZ || 'America/Mexico_City';
    const credsOk = hasGcalCreds(env);
    let cal = null, slot = null, event = null;

    if (credsOk) {
      try {
        const pool = await getCalendarPool(env);
        cal = pickCalendarFromPool(pool);
        if (!cal) console.warn('[GCal] No active calendar');
      } catch (e) { console.warn('[GCal] pool error', e); }
    } else {
      console.warn('[GCal] Missing creds; OS irá pendiente');
    }

    const chosen = clampToWindow(sv.when, tz);
    if (credsOk && cal) {
      try { slot = await findNearestFreeSlot(env, cal.gcal_id, chosen, tz); }
      catch (e) { console.warn('[GCal] slot error', e); slot = { start: chosen.start, end: chosen.end }; }
    } else {
      slot = { start: chosen.start, end: chosen.end };
    }

    // Cliente (upsert mínimo)
    let cliente_id = null;
    try {
      session.data.customer = session.data.customer || {};
      const c = session.data.customer;
      if (!c.nombre && sv.c_nombre) c.nombre = sv.c_nombre;
      if (!c.email  && sv.c_email)  c.email  = sv.c_email;
      ['calle','numero','colonia','ciudad','cp','estado'].forEach(k => { if (!c[k] && truthy(sv[k])) c[k] = sv[k]; });

      const ex = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${session.from}&limit=1` });
      if (ex && ex[0]?.id) { cliente_id = ex[0].id; await ensureClienteFields(env, cliente_id, c); }
      else {
        const ins = await sbUpsert(env, 'cliente', [{
          telefono: session.from,
          nombre: c.nombre || null, email: c.email || null,
          calle: c.calle || null, numero: c.numero || null, colonia: c.colonia || null, ciudad: c.ciudad || null, cp: c.cp || null, estado: c.estado || null
        }], { onConflict: 'telefono', returning: 'representation' });
        cliente_id = ins?.data?.[0]?.id || null;
      }
    } catch (e) { console.warn('[Supabase] cliente upsert', e); }

    // Crear evento (si se puede)
    if (credsOk && cal && slot) {
      try {
        event = await gcalCreateEvent(env, cal.gcal_id, {
          summary: `Visita técnica: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
          description: renderOsDescription(session.from, sv),
          start: slot.start,
          end: slot.end,
          timezone: tz,
        });
      } catch (e) { console.warn('[GCal] create error', e); }
    }

    // Orden de servicio
    let osId = null;
    try {
      const estado = (event && event.id) ? 'agendado' : 'pendiente';
      const osBody = [{
        cliente_id: cliente_id || null,
        marca: sv.marca || null,
        modelo: sv.modelo || null,
        falla_descripcion: sv.falla || null,
        prioridad: sv.prioridad || 'media',
        estado,
        ventana_inicio: new Date((slot?.start || chosen.start)).toISOString(),
        ventana_fin: new Date((slot?.end || chosen.end)).toISOString(),
        gcal_event_id: event?.id || null,
        calendar_id: cal?.gcal_id || null,
        calle: sv.calle || null,
        numero: sv.numero || null,
        colonia: sv.colonia || null,
        ciudad: sv.ciudad || null,
        cp: sv.cp || null,
        created_at: new Date().toISOString()
      }];
      const os = await sbUpsert(env, 'orden_servicio', osBody, { returning: 'representation' });
      osId = os?.data?.[0]?.id || null;
    } catch (e) { console.warn('[Supabase] OS upsert', e); }

    if (event?.id && osId) {
      await sendWhatsAppText(
        env,
        toE164,
        `¡Listo! Agendé tu visita 🙌
*${fmtDate(slot.start, tz)}*, de *${fmtTime(slot.start, tz)}* a *${fmtTime(slot.end, tz)}*
Dirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, ${sv.cp} ${sv.ciudad || ''}${sv.estado ? ', '+sv.estado : ''}
Técnico asignado: ${cal?.name || 'por confirmar'}.

Si necesitas reprogramar o cancelar, dímelo con confianza.`
      );

      session.stage = 'sv_scheduled';
      session.data.sv.os_id = osId;
      session.data.sv.gcal_event_id = event.id;
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }

    if (osId) {
      await sendWhatsAppText(env, toE164, `Tengo tus datos ✅. En breve te *confirmo el horario exacto* por este medio.`);
      await notifySupport(env, `OS pendiente/agendar para ${toE164}\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla||'N/D'}\nDirección: ${sv.calle||''} ${sv.numero||''}, ${sv.colonia||''}, ${sv.cp||''} ${sv.ciudad||''} ${sv.estado||''}`);
      session.stage = 'sv_scheduled';
      session.data.sv.os_id = osId;
      session.data.sv.gcal_event_id = null;
      await saveSession(env, session, now);
      return ok('EVENT_RECEIVED');
    }

    // Ni evento ni OS → acuse humano; mantenemos sv_collect
    await sendWhatsAppText(env, toE164, `Tomé tus datos de soporte 🙌. Un asesor te *confirmará el horario* enseguida.`);
    await notifySupport(env, `⚠️ No se pudo crear OS para ${toE164}. Revisar credenciales de DB/Calendar.\nEquipo: ${sv.marca||''} ${sv.modelo||''}\nFalla: ${sv.falla||'N/D'}`);
    session.stage = 'sv_collect';
    session.data.sv.os_pending = true;
    session.data.sv_need_next = null;
    await saveSession(env, session, now);
    return ok('EVENT_RECEIVED');

  } catch (e) {
    console.error('[SUPPORT] handleSupport error', e);
    try {
      session.data = session.data || {}; session.data.sv = session.data.sv || {};
      const sv = session.data.sv;
      const needed = getSvNeeded(sv, session, false);
      return await askNextSvQuestion(env, session, toE164, needed.length?needed:['modelo'], new Date());
    } catch (e2) {
      console.error('[SUPPORT] secondary error', e2);
      await sendWhatsAppText(env, toE164, 'Sigamos con tu servicio: ¿*marca/modelo* y breve *descripción* de la falla?');
      return ok('EVENT_RECEIVED');
    }
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
  if (!os) { await sendWhatsAppText(env, toE164, `No encuentro una visita activa para cancelar.`); session.stage='idle'; session.data.sv_need_next=null; return; }
  if (os.gcal_event_id && os.calendar_id) await gcalDeleteEvent(env, os.calendar_id, os.gcal_event_id);
  await sbUpsert(env, 'orden_servicio', [{ id: os.id, estado: 'cancelada', cancel_reason: 'cliente' }], { returning: 'minimal' });
  await sendWhatsAppText(env, toE164, `He *cancelado* tu visita. Si necesitas agendar otra, aquí estoy. 😊`);
  session.stage = 'idle';
  session.data.sv_need_next = null;
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
  // Parser robusto es-MX: “mañana 12:30”, “mañana 2 pm/2pm/2”, “14:00”, “13 hrs”
  const tz = env.TZ || 'America/Mexico_City';
  const now = new Date();
  const base = new Date(now.toLocaleString('en-US', { timeZone: tz }));
  let d = new Date(base);
  let targetDay = null;

  const t = (text || '').toLowerCase().replace(/\s+/g,' ').trim();

  if (/\bhoy\b/.test(t)) targetDay = 0;
  else if (/\bma[ñn]ana\b/.test(t)) targetDay = 1;
  else {
    const days = ['domingo','lunes','martes','miércoles','miercoles','jueves','viernes','sábado','sabado'];
    for (let i=0;i<days.length;i++) {
      if (new RegExp(`\\b${days[i]}\\b`).test(t)) {
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
  let hasAmPm = false;

  // 24h “14:00”
  let m = t.match(/\b([01]?\d|2[0-3])[:\.](\d{2})\b/);
  if (m) { hour = Number(m[1]); minute = Number(m[2]); }

  // “a las 3 pm”, “3pm”, “3”
  if (hour===null) {
    m = t.match(/\b(?:a\s+las\s+)?(\d{1,2})\s*(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)?\b/);
    if (m) {
      hour = Number(m[1]);
      minute = m[2]?Number(m[2]):0;
      const ampm = (m[3]||'').replace(/\./g,'').toLowerCase();
      hasAmPm = !!ampm;
      if (ampm==='pm' && hour<12) hour+=12;
      if (ampm==='am' && hour===12) hour=0;
    }
  }

  // “13 hrs”
  if (hour===null) {
    m = t.match(/\b([01]?\d|2[0-3])\s*h(?:rs?)?\b/);
    if (m) { hour = Number(m[1]); minute = 0; }
  }

  // Heurística: si no indica am/pm y hora 1–7 → PM (tarde)
  if (hour!==null && !hasAmPm && hour >= 1 && hour <= 7) hour += 12;

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
    body: JSON.stringify({
      client_id: env.GCAL_CLIENT_ID,
      client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    })
  });
  if (!r.ok) { console.warn('gcal token', await r.text()); return null; }
  const j = await r.json();
  return j.access_token;
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
  let curStart = new Date(when.start);
  let curEnd = new Date(when.end);
  for (let i=0;i<4;i++) {
    const busy = await isBusy(env, calendarId, curStart.toISOString(), curEnd.toISOString());
    if (!busy) break;
    curStart = new Date(curStart.getTime()+30*60*1000);
    curEnd = new Date(curEnd.getTime()+30*60*1000);
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
    `Cliente: +${phone}`,
    `Equipo: ${sv.marca || ''} ${sv.modelo || ''}`.trim(),
    `Falla: ${sv.falla || 'N/D'}${sv.error_code ? ' (Error ' + sv.error_code + ')' : ''}`,
    `Prioridad: ${sv.prioridad || 'media'}`,
    `Dirección: ${sv.calle || ''} ${sv.numero || ''}, ${sv.colonia || ''}, CP ${sv.cp || ''} ${sv.ciudad || ''}${sv.estado ? ', '+sv.estado : ''}`
  ].join('\n');
}
async function getLastOpenOS(env, phone) {
  try {
    const c = await sbGet(env, 'cliente', { query: `select=id&telefono=eq.${phone}&limit=1` });
    const cid = c?.[0]?.id;
    if (!cid) return null;
    const r = await sbGet(env, 'orden_servicio', { query: `select=id,estado,ventana_inicio,ventana_fin,calendar_id,gcal_event_id,cliente_id&cliente_id=eq.${cid}&order=ventana_inicio.desc&limit=1` });
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
  const mcp = text.match(/\b(\d{5})\b/);
  if (mcp) out.cp = mcp[1];
  const mnum = text.match(/\b(\d+[A-Z]?)\b/);
  if (mnum) out.numero = mnum[1];
  if (out.cp) {
    const pre = text.split(out.cp)[0];
    const parts = pre.split(',').map(s=>s.trim()).filter(Boolean);
    if (parts.length >= 1) out.colonia = parts[parts.length-1];
  }
  const mcalle = text.match(/([A-Za-zÁÉÍÓÚÜÑ0-9 .\-']+?)\s+(\d+[A-Z]?)(?!\w)/i);
  if (mcalle) out.calle = clean(mcalle[1]);
  return out;
}

/* ============================ SEPOMEX ============================ */
async function cityFromCP(env, cp) {
  try {
    const r = await sbGet(env, 'sepomex_raw', { query: `select=d_mnpio,d_estado,d_ciudad&d_codigo=eq.${encodeURIComponent(cp)}&limit=1` });
    if (r && r[0]) {
      return { municipio: r[0].d_mnpio || null, estado: r[0].d_estado || null, ciudad: r[0].d_ciudad || null };
    }
  } catch {}
  return null;
}

/* ============================ Supabase helpers ============================ */
function sb(env){
  const key = env.SUPABASE_SERVICE_ROLE || env.SUPABASE_KEY;
  return { url:`${env.SUPABASE_URL}/rest/v1`, key };
}
async function sbGet(env, table, { query='', headers={} }={}) {
  const b = sb(env);
  if (!b.url || !/^https?:\/\//.test(b.url)) { console.warn('sbGet URL missing/invalid'); return null; }
  const url = `${b.url}/${table}${query?`?${query}`:''}`;
  const r = await fetch(url, { headers:{ apikey:b.key, Authorization:`Bearer ${b.key}`, ...headers } });
  if (r.status===204) return [];
  if (!r.ok){ console.warn('sbGet', table, r.status, await r.text()); return null; }
  try { return await r.json(); } catch { return null; }
}
async function sbUpsert(env, table, body, { onConflict='', returning='representation', headers={} }={}) {
  const b = sb(env);
  const url = `${b.url}/${table}`;
  const h = {
    apikey:b.key, Authorization:`Bearer ${b.key}`,
    'Content-Type':'application/json',
    Prefer:`resolution=merge-duplicates${onConflict?`,on_conflict=${onConflict}`:''},return=${returning}`,
    ...headers
  };
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
    const r = await fetch(url, { method:'PATCH', headers:{
      apikey:b.key, Authorization:`Bearer ${b.key}`, 'Content-Type':'application/json'
    }, body: JSON.stringify(body) });
    if(!r.ok) console.warn('sbPatch', table, r.status, await r.text());
  }catch(e){ console.warn('sbPatch err', table, e); }
}
async function sbRpc(env, fn, args){
  const b = sb(env);
  const url = `${b.url}/rpc/${fn}`;
  try{
    const r = await fetch(url, { method:'POST', headers:{
      apikey:b.key, Authorization:`Bearer ${b.key}`, 'Content-Type':'application/json'
    }, body: JSON.stringify(args || {}) });
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
  const toISO   = new Date(now.getTime() + 26 * 60 * 60 * 1000).toISOString();
  const rows = await sbGet(env, 'orden_servicio', {
    query: `select=id,cliente_id,ventana_inicio,remind_24h_sent,remind_1h_sent,estado&estado=in.(agendado,reprogramado)&ventana_inicio=gte.${fromISO}&ventana_inicio=lte.${toISO}`
  }) || [];
  let sent = 0;
  for (const os of rows) {
    const when = new Date(os.ventana_inicio);
    const soon24h = Date.now() + 24 * 60 * 60 * 1000;
    const soon1h  = Date.now() + 60 * 60 * 1000;
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

  const mName = t.match(/(?:raz[oó]n social|nombre)\s*[:\-]\s*(.+)$/i);
  if (mName) out.nombre = clean(mName[1]);

  const mRFC = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i);
  if (mRFC) out.rfc = mRFC[1].toUpperCase();

  const mMail = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (mMail) out.email = mMail[0].toLowerCase();

  const mCP = t.match(/\b(\d{5})\b/);
  if (mCP) out.cp = mCP[1];

  const mCalle = t.match(/\b(calle|av(enida)?|avenida|blvd|boulevard|prolongaci[oó]n|camino|andador|privada|paseo|prol\.?)\s+([^\n,]+)\b/i);
  if (mCalle) out.calle = clean(`${mCalle[3]}`);

  const mNum = t.match(/\b(no\.?|n[úu]mero|num)\s*[:\- ]\s*(\d+[A-Z]?)\b/i);
  if (mNum) out.numero = mNum[2];

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
function displayField(k){ const map={ nombre:'Nombre / Razón Social', rfc:'RFC', email:'Email', calle:'Calle', numero:'Número', colonia:'Colonia', ciudad:'Ciudad', cp:'CP' }; return map[k]||k; } 
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
      ciudad: '¿*Ciudad o municipio*?',
      cp: '¿*Código Postal* (5 dígitos)?',
      horario: '¿Qué día y hora te viene bien entre *10:00 y 15:00*? (puedes decir “mañana 12:30”)'
    };
    return q[need] || '¿Podrías compartirme el dato pendiente para continuar?';
  }
  return '¿En qué te ayudo hoy?';
}
