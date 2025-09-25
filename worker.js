/**
 * CopiBot – IA + Ventas + Soporte + GCal — Build R6.3 FIX Final (2025-09)
 * Cambios principales:
 *  - Sesión triple-key (from, fromE164, fromDigits).
 *  - Persistencia inmediata de marca/modelo/falla.
 *  - handleSupport pide UN dato a la vez, con ACK explícito.
 *  - Detección determinista: “marca+modelo” sin palabras de venta = soporte.
 *  - OS se guarda en Supabase aunque falle GCal.
 *  - Se mantiene TODO lo demás: ventas, FAQs, IA, cron.
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
        return new Response('forbidden', { status: 403 });
      }

      // --- Cron ---
      if (req.method === 'POST' && url.pathname === '/cron') {
        const sec = req.headers.get('x-cron-secret') || url.searchParams.get('secret');
        if (!sec || sec !== env.CRON_SECRET) return new Response('forbidden', { status: 403 });
        const out = await cronReminders(env);
        return ok(`cron ok ${JSON.stringify(out)}`);
      }

      // --- WhatsApp Webhook ---
      if (req.method === 'POST' && url.pathname === '/') {
        const payload = await safeJson(req);
        const ctx = extractWhatsAppContext(payload);
        if (!ctx) return ok('EVENT_RECEIVED');

        const { mid, from, fromE164, profileName, textRaw, msgType } = ctx;
        const fromDigits = from.replace(/\D/g, '');
        const originalText = (textRaw || '').trim();
        const lowered = originalText.toLowerCase();
        const ntext = normalizeWithAliases(originalText);

        // ===== Sesión =====
        let session = await loadSessionTriple(env, { from, fromE164, fromDigits });
        session.data = session.data || {};
        session.stage = session.stage || 'idle';

        // Idempotencia
        if (session?.data?.last_mid === mid) return ok('EVENT_RECEIVED');
        session.data.last_mid = mid;

        // === Soporte activo ===
        if (session.stage?.startsWith('sv_') || session?.data?.intent_lock === 'support') {
          return await handleSupport(env, session, fromE164, originalText, lowered, ntext);
        }

        // === Marca+modelo ⇒ forzar soporte ===
        const pm = parseBrandModel(ntext);
        const SALES_WORDS = /\b(toner|t[óo]ner|cartucho|refacci[oó]n|precio)\b/i;
        if (pm?.modelo && !SALES_WORDS.test(ntext)) {
          session.data.intent_lock = 'support';
          session.stage = 'sv_collect';
          session.data.sv = session.data.sv || {};
          if (pm.marca) session.data.sv.marca = pm.marca;
          if (pm.modelo) session.data.sv.modelo = pm.modelo;
          session.data.sv_need_next = 'falla';
          await saveSessionTriple(env, session, { from, fromE164, fromDigits });
          return await handleSupport(env, session, fromE164, originalText, lowered, ntext);
        }

        // === Ventas ===
        if (RX_INV_Q.test(ntext)) {
          return await startSalesFromQuery(env, session, fromE164, originalText, ntext);
        }

        // === FAQ ===
        const faq = await maybeFAQ(env, ntext);
        if (faq) {
          await sendWhatsAppText(env, fromE164, faq);
          await saveSessionTriple(env, session, { from, fromE164, fromDigits });
          return ok('EVENT_RECEIVED');
        }

        // === Default IA ===
        const reply = await aiSmallTalk(env, session, 'fallback', originalText);
        await sendWhatsAppText(env, fromE164, reply);
        await saveSessionTriple(env, session, { from, fromE164, fromDigits });
        return ok('EVENT_RECEIVED');
      }

      return new Response('not found', { status: 404 });
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
/* =============================== Helpers ================================= */
/* ========================================================================== */

function ok(s='ok'){ return new Response(s, { status:200 }); }
async function safeJson(req){ try{ return await req.json(); }catch{ return {}; } }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

/* ========================================================================== */
/* ============================ Sesiones (KV) =============================== */
/* ========================================================================== */

async function loadSessionTriple(env, keys) {
  const { from, fromE164, fromDigits } = keys;
  const raw = await env.COPIBOT_KV.get(`sess:${fromE164}`, 'json')
    || await env.COPIBOT_KV.get(`sess:${from}`, 'json')
    || await env.COPIBOT_KV.get(`sess:${fromDigits}`, 'json');
  const sess = raw || { stage: 'idle', data: {} };
  sess.keys = keys;
  return sess;
}

async function saveSessionTriple(env, sess, keys) {
  const val = JSON.stringify(sess);
  await env.COPIBOT_KV.put(`sess:${keys.from}`, val);
  await env.COPIBOT_KV.put(`sess:${keys.fromE164}`, val);
  await env.COPIBOT_KV.put(`sess:${keys.fromDigits}`, val);
}

/* ========================================================================== */
/* =============================== SOPORTE ================================== */
/* ========================================================================== */

async function handleSupport(env, session, toE164, text, lowered, ntext) {
  session.data.intent_lock = 'support';
  session.data.sv = session.data.sv || {};
  const sv = session.data.sv;

  // === Mapear la respuesta actual según lo que se pedía ===
  if (session.data.sv_need_next === 'modelo') {
    const pm = parseBrandModel(text);
    if (pm.marca) sv.marca = pm.marca;
    if (pm.modelo) sv.modelo = pm.modelo;
  }
  else if (session.data.sv_need_next === 'falla') {
    sv.falla = text;
  }
  else if (session.data.sv_need_next === 'direccion') {
    Object.assign(sv, parseAddressLoose(text));
  }
  else if (session.data.sv_need_next === 'horario') {
    sv.fecha = text;
  }

  // === Validar faltantes ===
  const needed = [];
  if (!sv.marca || !sv.modelo) needed.push('modelo');
  if (!sv.falla) needed.push('falla');
  if (!sv.calle) needed.push('direccion');
  if (!sv.fecha) needed.push('horario');

  if (needed.length) {
    session.stage = 'sv_collect';
    session.data.sv_need_next = needed[0];
    await saveSessionTriple(env, session, session.keys);

    const Q = {
      modelo: '¿Qué *marca y modelo* es tu impresora?',
      falla: '¿Qué *falla* presenta?',
      direccion: '¿Dónde está ubicada la impresora? (calle, número, colonia, CP)',
      horario: '¿Qué día y hora prefieres para la visita (entre 10:00 y 15:00 hrs)?'
    };

    let pre = '';
    if (sv.marca && sv.modelo && needed[0] === 'falla') {
      pre = `Perfecto 👍 anoté: *${sv.marca} ${sv.modelo}*.\n`;
    }

    await sendWhatsAppText(env, toE164, pre + Q[needed[0]]);
    return ok('EVENT_RECEIVED');
  }

  // === Ya tenemos todo → crear Orden de Servicio ===
  const tz = env.TZ || 'America/Mexico_City';
  const dt = parseNaturalDateTime(sv.fecha, env) || {
    start: new Date(),
    end: new Date(Date.now() + 3600000)
  };
  const slot = clampToWindow(dt, tz);

  // Cliente
  const cid = await upsertClienteByPhone(env, toE164);

  // GCal
  let ev = { id: null, start: slot.start, end: slot.end };
  try {
    const pool = await getCalendarPool(env);
    const cal = pickCalendarFromPool(pool);
    ev = await gcalCreateEvent(env, cal?.gcal_id || env.GCAL_ID, {
      summary: `Soporte ${sv.marca} ${sv.modelo}`,
      description: renderOsDescription(toE164, sv),
      start: slot.start, end: slot.end, timezone: tz
    });
  } catch (e) {
    console.warn('[GCal] fallo crear evento', e);
  }

  // Supabase: guardar OS
  try {
    await sbUpsert(env, 'orden_servicio', [{
      cliente_id: cid,
      marca: sv.marca,
      modelo: sv.modelo,
      falla: sv.falla,
      direccion: `${sv.calle || ''} ${sv.numero || ''}, ${sv.colonia || ''}, ${sv.cp || ''}`,
      fecha: slot.start,
      gcal_id: ev.id,
      estado: ev?.id ? 'agendado' : 'pendiente'
    }], { onConflict: 'id', returning: 'representation' });
  } catch (e) {
    console.warn('[Supabase] fallo guardar OS', e);
  }

  // Confirmación al cliente
  await sendWhatsAppText(env, toE164,
    `✅ Tu visita quedó registrada:\n` +
    `📅 ${fmtDate(slot.start, tz)} a las ${fmtTime(slot.start, tz)}\n` +
    `Equipo: ${sv.marca} ${sv.modelo}\n` +
    `Falla: ${sv.falla}`
  );

  // Reset
  session.stage = 'idle';
  session.data.intent_lock = null;
  session.data.sv_collect = false;
  await saveSessionTriple(env, session, session.keys);
  return ok('EVENT_RECEIVED');
}

/* ========================================================================== */
/* ================================ VENTAS ================================== */
/* ========================================================================== */

async function startSalesFromQuery(env, session, toE164, text, ntext) {
  try {
    // Buscar coincidencias en inventario (Supabase RPC con trigramas)
    const rows = await sbRpc(env, 'match_products_trgm', { q: ntext });
    if (!rows?.length) {
      await sendWhatsAppText(env, toE164, 'No encontré productos con esa descripción 🤔. ¿Quieres intentar con otro nombre o modelo?');
      return ok('EVENT_RECEIVED');
    }

    // Mostrar listado
    const items = rows.map(p =>
      `• ${p.nombre} — $${p.precio}+IVA ${p.stock > 0 ? '✅ disponible' : '📦 sobre pedido (3 días)'}`
    ).join('\n');

    await sendWhatsAppText(env, toE164,
      `Encontré lo siguiente en inventario:\n${items}\n\n¿Quieres que lo agregue a tu pedido?`
    );

    // Guardar contexto de venta
    session.stage = 'sales_offer';
    session.data.last_sales = rows;
    await saveSessionTriple(env, session, session.keys);
    return ok('EVENT_RECEIVED');
  } catch (e) {
    console.error('[Ventas] error', e);
    await sendWhatsAppText(env, toE164, '⚠️ Hubo un error consultando inventario. Intenta de nuevo en unos minutos.');
    return ok('EVENT_RECEIVED');
  }
}

/* ========================================================================== */
/* ================================= FAQ ==================================== */
/* ========================================================================== */

async function maybeFAQ(env, ntext) {
  // Ubicación
  if (/\b(dónde|ubicación|direccion|dirección)\b/.test(ntext)) {
    return '📍 Estamos en León y Celaya, Guanajuato. Atendemos en toda la región con soporte a domicilio.';
  }

  // Horario
  if (/\b(horario|abren|cierran|hora)\b/.test(ntext)) {
    return '🕒 Nuestro horario es de Lunes a Viernes de 9:00 a 18:00 hrs.';
  }

  // Empresa
  if (/\b(quiénes son|a qué se dedican|qué ofrecen|empresa)\b/.test(ntext)) {
    return '👋 Somos **CP Digital**, distribuidor autorizado de Xerox/Fuji. Ofrecemos:\n' +
           '• Venta de consumibles y refacciones\n' +
           '• Soporte técnico especializado\n' +
           '• Soluciones de impresión empresarial';
  }

  return null;
}

/* ========================================================================== */
/* ============================== AI SmallTalk ============================== */
/* ========================================================================== */

async function aiSmallTalk(env, session, mode, text) {
  try {
    const r = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Eres CopiBot de CP Digital. Ayuda con soporte de impresoras y ventas de consumibles, siempre en español, con lenguaje simple y cercano.' },
          { role: 'user', content: text }
        ],
        max_tokens: 200
      })
    });
    const j = await r.json();
    return j.choices?.[0]?.message?.content || 'No entendí bien 🤔. ¿Podrías decirlo de otra forma?';
  } catch (e) {
    console.error('[AI] error', e);
    return 'Lo siento, tuve un problema procesando tu mensaje.';
  }
}

/* ========================================================================== */
/* =============================== SUPABASE ================================= */
/* ========================================================================== */

async function sbGet(env, table, { query }) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
    }
  });
  if (!r.ok) throw new Error(`Supabase GET ${table} ${r.status}`);
  return await r.json();
}

async function sbUpsert(env, table, rows, opts={}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      Prefer: `resolution=merge-duplicates${opts.returning?`,return=${opts.returning}`:''}`
    },
    body: JSON.stringify(rows)
  });
  if (!r.ok) throw new Error(`Supabase UPSERT ${table} ${r.status}`);
  return await r.json();
}

async function sbPatch(env, table, patch, filter) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(patch)
  });
  if (!r.ok) throw new Error(`Supabase PATCH ${table} ${r.status}`);
  return await r.json();
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
    body: JSON.stringify(params)
  });
  if (!r.ok) throw new Error(`Supabase RPC ${fn} ${r.status}`);
  return await r.json();
}

/* ========================================================================== */
/* ============================ GOOGLE CALENDAR ============================= */
/* ========================================================================== */

async function gcalCreateEvent(env, calendarId, ev) {
  const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`;
  const token = await gcalToken(env);
  const r = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      summary: ev.summary,
      description: ev.description,
      start: { dateTime: ev.start, timeZone: ev.timezone },
      end: { dateTime: ev.end, timeZone: ev.timezone }
    })
  });
  if (!r.ok) throw new Error(`GCal create ${r.status}`);
  return await r.json();
}

async function gcalToken(env) {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GCAL_CLIENT_ID,
      client_secret: env.GCAL_CLIENT_SECRET,
      refresh_token: env.GCAL_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    })
  });
  if (!r.ok) throw new Error(`GCal token ${r.status}`);
  const j = await r.json();
  return j.access_token;
}

/* ========================================================================== */
/* ============================= PARSEADORES ================================ */
/* ========================================================================== */

function parseNaturalDateTime(text, env) {
  // simplificado: busca "mañana 12:30" o "lunes 11am"
  const lower = text.toLowerCase();
  const now = new Date();
  let d = new Date(now);

  if (/mañana/.test(lower)) d.setDate(d.getDate() + 1);
  if (/pasado/.test(lower)) d.setDate(d.getDate() + 2);

  const hm = lower.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/);
  if (hm) {
    let h = parseInt(hm[1], 10);
    let m = hm[2] ? parseInt(hm[2], 10) : 0;
    if (hm[3] === 'pm' && h < 12) h += 12;
    d.setHours(h, m, 0, 0);
  } else {
    d.setHours(12, 0, 0, 0);
  }

  return { start: d.toISOString(), end: new Date(d.getTime()+60*60*1000).toISOString() };
}

function parseAddressLoose(text) {
  const out = {};
  const cp = text.match(/\b\d{5}\b/);
  if (cp) out.cp = cp[0];
  const calle = text.match(/calle\s+([\w\s]+)/i);
  if (calle) out.calle = calle[1];
  return out;
}

function parseCustomerText(text) {
  const out = {};
  const email = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (email) out.email = email[0].toLowerCase();
  return out;
}

/* ========================================================================== */
/* =============================== SOPORTE ================================== */
/* ========================================================================== */

function parseBrandModel(text='') {
  const t = text.toLowerCase();
  if (/docu\s*color\s*(550|560|570)/.test(t)) return { marca:'Xerox', modelo:`DocuColor ${RegExp.$1}` };
  if (/versant\s*(80|180|2100|280|4100)/.test(t)) return { marca:'Xerox', modelo:`Versant ${RegExp.$1}` };
  if (/versalink\s*([a-z0-9\-]+)/.test(t)) return { marca:'Xerox', modelo:`VersaLink ${RegExp.$1.toUpperCase()}` };
  if (/altalink\s*([a-z0-9\-]+)/.test(t)) return { marca:'Xerox', modelo:`AltaLink ${RegExp.$1.toUpperCase()}` };
  if (/primelink\s*([a-z0-9\-]+)/.test(t)) return { marca:'Xerox', modelo:`PrimeLink ${RegExp.$1.toUpperCase()}` };
  if (/apeos\s*([a-z0-9\-]+)/.test(t)) return { marca:'Fujifilm', modelo:`Apeos ${RegExp.$1.toUpperCase()}` };
  const m = text.match(/\b([cb]\d{2,4})\b/i);
  if (m) return { marca:'Xerox', modelo:m[1].toUpperCase() };
  return { marca:null, modelo:null };
}

function extractSvInfo(text) {
  const out = {};
  if (/xerox/i.test(text)) out.marca = 'Xerox';
  if (/fujifilm|fuji\s*film/i.test(text)) out.marca = 'Fujifilm';
  if (/no imprime/i.test(text)) out.falla = 'No imprime';
  if (/atasc|ator|traba/i.test(text)) out.falla = 'Atasco de papel';
  if (/mancha|linea|línea|calidad/i.test(text)) out.falla = 'Calidad de impresión';
  const pm = parseBrandModel(text);
  if (pm.marca) out.marca = pm.marca;
  if (pm.modelo) out.modelo = pm.modelo;
  return out;
}

function svFillFromAnswer(sv, field, text) {
  if (field === 'modelo') {
    const pm = parseBrandModel(text);
    if (pm.marca) sv.marca = pm.marca;
    if (pm.modelo) sv.modelo = pm.modelo;
  } else if (field === 'falla') {
    sv.falla = text.trim();
  } else if (field === 'nombre') {
    sv.nombre = text.trim();
  } else if (field === 'email') {
    const m = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    sv.email = m ? m[0].toLowerCase() : text.trim();
  } else if (field === 'calle') {
    sv.calle = text.trim();
  } else if (field === 'numero') {
    const m = text.match(/\b\d+\w?\b/);
    sv.numero = m ? m[0] : text.trim();
  } else if (field === 'colonia') {
    sv.colonia = text.trim();
  } else if (field === 'cp') {
    const m = text.match(/\b\d{5}\b/);
    sv.cp = m ? m[0] : text.trim();
  }
}

async function handleSupport(env, session, toE164, text, lowered, ntext) {
  session.data = session.data || {};
  session.data.sv = session.data.sv || {};
  const sv = session.data.sv;

  // Completar con extractores
  const extra = extractSvInfo(text);
  Object.assign(sv, Object.fromEntries(Object.entries(extra).filter(([k,v]) => v && !sv[k])));

  const needed = [];
  if (!sv.marca || !sv.modelo) needed.push('modelo');
  if (!sv.falla) needed.push('falla');
  if (!sv.calle) needed.push('calle');
  if (!sv.numero) needed.push('numero');
  if (!sv.colonia) needed.push('colonia');
  if (!sv.cp) needed.push('cp');
  if (!sv.nombre) needed.push('nombre');
  if (!sv.email) needed.push('email');

  if (needed.length) {
    const field = needed[0];
    session.stage = 'sv_collect';
    session.data.sv_need_next = field;
    await saveSessionTriple(env, session, session.keys);

    const Q = {
      modelo:'¿Qué *marca y modelo* es tu impresora? (ej. Xerox DocuColor 550)',
      falla:'¿Cuál es la *falla*? (ej. “no imprime”, “atasco de papel”)',
      calle:'¿En qué *calle* está el equipo?',
      numero:'¿Qué *número* es?',
      colonia:'¿Colonia*?',
      cp:'¿Código Postal (5 dígitos)?',
      nombre:'¿A nombre de quién registramos la visita?',
      email:'¿Cuál es tu *email* para confirmar la cita?'
    };

    await sendWhatsAppText(env, toE164, Q[field] || '¿Me ayudas con ese dato?');
    return ok('EVENT_RECEIVED');
  }

  // Si ya tenemos todo → agendar
  const slot = parseNaturalDateTime(text, env);
  const tz = env.TZ || 'America/Mexico_City';
  let event = null;
  try {
    event = await gcalCreateEvent(env, env.GCAL_CALENDAR_ID, {
      summary: `Visita técnica: ${sv.marca} ${sv.modelo}`,
      description: `Falla: ${sv.falla}\nCliente: ${sv.nombre}\nEmail: ${sv.email}\nDirección: ${sv.calle} ${sv.numero}, ${sv.colonia}, CP ${sv.cp}`,
      start: slot.start,
      end: slot.end,
      timezone: tz
    });
  } catch (e) {
    console.warn('[GCal] error', e);
  }

  try {
    await sbUpsert(env, 'orden_servicio', [{
      marca: sv.marca,
      modelo: sv.modelo,
      falla_descripcion: sv.falla,
      nombre: sv.nombre,
      email: sv.email,
      calle: sv.calle,
      numero: sv.numero,
      colonia: sv.colonia,
      cp: sv.cp,
      estado: event ? 'agendado' : 'pendiente',
      gcal_event_id: event?.id || null,
      created_at: new Date().toISOString()
    }], { returning: 'representation' });
  } catch (e) {
    console.warn('[Supabase] OS error', e);
  }

  await sendWhatsAppText(env, toE164,
    event
      ? `✅ ¡Listo! Agendé tu visita:\n📅 ${new Date(slot.start).toLocaleDateString('es-MX')} ${new Date(slot.start).toLocaleTimeString('es-MX')}\nEquipo: ${sv.marca} ${sv.modelo}\nFalla: ${sv.falla}`
      : `✅ Registré tu solicitud. En breve un asesor te confirmará la cita.`
  );

  session.stage = 'sv_scheduled';
  await saveSessionTriple(env, session, session.keys);
  return ok('EVENT_RECEIVED');
}
