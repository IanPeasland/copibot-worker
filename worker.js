// ============================================================================
// CP Digital - WhatsApp Worker (versión consolidada)
// Requisitos de entorno (Cloudflare Variables/Secrets):
//   WA_TOKEN, PHONE_ID, SUPABASE_URL, SUPABASE_ANON_KEY
// Opcionales:
//   TZ="America/Mexico_City", DEBUG=true|false, DEBUG_JSON=true|false, DEBUG_WEBHOOK=true|false
// ============================================================================

// ---- Utilidades generales ---------------------------------------------------
const TZ = 'America/Mexico_City';

function ok(s='ok'){ return new Response(s, { status: 200 }); }
function badRequest(s='Bad Request'){ return new Response(s, { status: 400 }); }
function forbidden(s='Forbidden'){ return new Response(s, { status: 403 }); }
function serverError(s='Server Error'){ return new Response(s, { status: 500 }); }

async function safeJson(req){
  try{ return await req.json(); }catch{return {}; }
}

function dlog(env, ...args){
  if ((env.DEBUG||'').toString().toLowerCase() === 'true') console.log(...args);
}

function fmtDate(d, tz=TZ){
  try{
    const dt = new Date(d);
    return dt.toLocaleDateString('es-MX', { timeZone: tz });
  }catch{ return String(d); }
}
function fmtTime(d, tz=TZ){
  try{
    const dt = new Date(d);
    return dt.toLocaleTimeString('es-MX', { timeZone: tz, hour: '2-digit', minute: '2-digit' });
  }catch{ return String(d); }
}

function normalizeBase(s=''){
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g,'');
}
function clean(s=''){ return String(s||'').replace(/\s+/g,' ').trim(); }
function truthy(v){ return v!==null && v!==undefined && String(v).length>0; }

// ---- WhatsApp Cloud API helpers --------------------------------------------
async function sendWhatsAppText(env, toE164, text){
  if(!toE164 || !truthy(text)) return;
  const url = `https://graph.facebook.com/v19.0/${env.PHONE_ID}/messages`;
  const body = {
    messaging_product: 'whatsapp',
    to: toE164.replace(/\D/g,''),
    text: { body: text }
  };
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${env.WA_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  if(!r.ok){
    const t = await r.text().catch(()=> '');
    console.error('[WA send error]', r.status, t.slice(0,400));
  }
}

// ---- Supabase REST (PostgREST) ---------------------------------------------
async function sbGet(env, pathWithQuery){
  const url = `${env.SUPABASE_URL}/rest/v1/${pathWithQuery}`;
  const r = await fetch(url, {
    headers: {
      'apikey': env.SUPABASE_ANON_KEY,
      'Authorization': `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Accept': 'application/json'
    }
  });
  if(!r.ok){ throw new Error(`Supabase GET ${r.status}`); }
  return await r.json();
}

async function sbUpsert(env, table, rows){
  const url = `${env.SUPABASE_URL}/rest/v1/${table}`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'apikey': env.SUPABASE_ANON_KEY,
      'Authorization': `Bearer ${env.SUPABASE_ANON_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(rows)
  });
  if(!r.ok){
    const t = await r.text().catch(()=> '');
    throw new Error(`Supabase UPSERT ${table} ${r.status} ${t.slice(0,200)}`);
  }
  return await r.json().catch(()=> (Array.isArray(rows)?rows:[rows]));
}

// ---- Sesión simple en tabla wa_session  ------------------------------------
// estructura esperada: wa_session(phone text PK, data jsonb, updated_at timestamptz default now())
async function loadSession(env, phoneE164){
  const phone = phoneE164.replace(/\D/g,'');
  const rows = await sbGet(env, `wa_session?phone=eq.${phone}&select=data`);
  return rows?.[0]?.data || {};
}
async function saveSession(env, phoneE164, data){
  const phone = phoneE164.replace(/\D/g,'');
  await sbUpsert(env, 'wa_session', [{ phone, data }]);
}

// ---- NLP-lite e intentos ----------------------------------------------------
const RX_GREET = /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|hola!*)$/i;
const RX_INV_Q = /(toner|t[óo]ner|cartucho|developer|refacci[oó]n|drum|banda|fusor|rodillo|c[óo]digo)/i;

function firstWord(s=''){ return (s||'').trim().split(/\s+/)[0] || ''; }
function toTitleCase(s=''){ return s ? s.charAt(0).toUpperCase()+s.slice(1) : s; }

function isSupportIntent(ntext=''){
  return /(falla|atasc|no imprime|error|marca c[oó]digo|servicio|soporte|mantenimiento)/i.test(ntext);
}
function isInventoryIntent(ntext=''){
  return RX_INV_Q.test(ntext);
}

function extractModelHints(text=''){
  // palabras clave como "versant 80/180", "c70", "prime link c9065"
  const t = normalizeBase(text.toLowerCase());
  const hints = [];
  const rx = /(versant\s*\d{2,3}|c\d{2,3}|prime\s*link\s*c?\d{3,4}|wc\s*\d{3,4}|workcentre\s*\d{3,4}|dc\s*\d{3,4}|docucolor\s*\d{3,4})/gi;
  let m; while((m = rx.exec(t))){ hints.push(m[0]); }
  return hints;
}
function extractColorWord(text=''){
  const t = normalizeBase(text.toLowerCase());
  if(/\b(amarill[oa]|yellow|y)\b/.test(t)) return 'Y';
  if(/\b(magenta|m)\b/.test(t)) return 'M';
  if(/\b(cyan|cian|c)\b/.test(t)) return 'C';
  if(/\b(negro|black|k)\b/.test(t)) return 'K';
  return null;
}

function productHasColor(p, colorCode){
  if(!colorCode) return true;
  const n = normalizeBase(`${p?.nombre||''} ${p?.compatible||''} ${p?.sku||''}`).toLowerCase();
  if(colorCode==='Y') return /\b(amarill|yellow|y)\b/.test(n);
  if(colorCode==='M') return /\b(magenta|m)\b/.test(n);
  if(colorCode==='C') return /\b(cyan|cian|c)\b/.test(n);
  if(colorCode==='K') return /\b(negro|black|k)\b/.test(n);
  return true;
}

function productMatchesFamily(p, family){
  if(!family) return true;
  const n = normalizeBase(`${p?.nombre||''} ${p?.compatible||''}`).toLowerCase();
  const f = normalizeBase(family).toLowerCase();
  return n.includes(f.replace(/\s+/g,' '));
}

// ---- Inventario: búsqueda en vista producto_stock_v ------------------------
// Reglas: si stock==0 => mostrar "0 pzas — sobre pedido" (no ocultar)
async function findBestProduct(env, queryText, opts={}){
  const term = clean(queryText);
  if(!truthy(term)) return null;

  // construimos un OR ilike simple para nombre, marca, compatible, sku:
  // or=(nombre.ilike.*term*,marca.ilike.*term*,compatible.ilike.*term*,sku.eq.term)
  const enc = encodeURIComponent;
  const ilike = `*${term.replace(/[%*]/g,'')}*`;
  const or = `or=(nombre.ilike.${enc(ilike)},marca.ilike.${enc(ilike)},compatible.ilike.${enc(ilike)},sku.eq.${enc(term)})`;
  const path = `producto_stock_v?select=id,sku,nombre,marca,compatible,tipo,precio,moneda,created_at,stock&${or}&limit=25`;

  const rows = await sbGet(env, path);
  if(!rows?.length) return null;

  // Filtrado fino por color/familia si se detecta
  const color = extractColorWord(term);
  const families = extractModelHints(term);

  let cand = rows.filter(p => productHasColor(p, color));
  if(families.length){
    cand = cand.filter(p => families.some(f => productMatchesFamily(p, f)));
  }
  if(!cand.length) cand = rows;

  // escoge el más "específico": mayor coincidencia en nombre
  cand.sort((a,b)=>{
    const na = normalizeBase(a.nombre||'').toLowerCase();
    const nb = normalizeBase(b.nombre||'').toLowerCase();
    const score = (n)=> (families.some(f=> n.includes(normalizeBase(f).toLowerCase()))?2:0)
                     + (color && productHasColor({nombre:n}, color)?1:0)
                     + (n.includes(normalizeBase(term).toLowerCase())?1:0);
    return score(nb) - score(na);
  });

  return cand[0];
}

function formatMoneyMXN(n){
  const v = Number(n||0);
  try{ return new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:2}).format(v); }
  catch{ return `${v.toFixed(2)} MXN`; }
}

function renderProducto(p){
  const precioTxt = `${formatMoneyMXN(p.precio)} + IVA`;
  const stockTxt = `${Number(p.stock||0)} pzas — ${Number(p.stock||0)>0?'en stock':'sobre pedido'}`;
  return [
    `1. ${p.nombre}`,
    `Marca: ${p.marca || '—'}`,
    `SKU: ${p.sku}`,
    `${precioTxt}`,
    `${stockTxt}`,
    ``,
    `Este suele ser el indicado para tu equipo.`
  ].join('\n');
}

// Flujo de búsqueda-respuesta para ventas
async function startSalesFromQuery(env, session, toE164, text){
  const p = await findBestProduct(env, text);
  if(!p){
    await sendWhatsAppText(env, toE164,
      'No encontré ese modelo exacto. ¿Me das el modelo de tu equipo (por ejemplo: Versant 180, C70, Prime Link C9065)?');
    return;
  }
  const msg = `${renderProducto(p)}\n\n¿Te funciona?\nSi sí, dime cuántas piezas; hay ${Number(p.stock||0)} en stock y el resto sería sobre pedido.`;
  await sendWhatsAppText(env, toE164, msg);

  // guardamos último producto sugerido en sesión
  session.lastProduct = { sku: p.sku, nombre: p.nombre, precio: p.precio, stock: Number(p.stock||0) };
  await saveSession(env, toE164, session);
}

// ---- Parser robusto de WhatsApp webhook ------------------------------------
function extractWhatsAppContext(payload) {
  try {
    const entry = Array.isArray(payload?.entry) ? payload.entry[0] : null;
    const change = Array.isArray(entry?.changes) ? entry.changes[0] : null;
    const value = change?.value || {};
    const msg = Array.isArray(value?.messages) ? value.messages[0] : null;

    if (!msg) {
      return {
        msg: null,
        mid: value?.statuses?.[0]?.id || `${Date.now()}_${Math.random()}`,
        from: value?.statuses?.[0]?.recipient_id || value?.contacts?.[0]?.wa_id || null,
        fromE164: value?.statuses?.[0]?.recipient_id
          ? `+${value.statuses[0].recipient_id}`
          : (value?.contacts?.[0]?.wa_id ? `+${value.contacts[0].wa_id}` : null),
        profileName: value?.contacts?.[0]?.profile?.name || '',
        textRaw: '',
        msgType: 'event'
      };
    }

    const mid = msg.id || `${Date.now()}_${Math.random()}`;
    const from = msg.from || value?.contacts?.[0]?.wa_id || null;
    const fromE164 = from ? `+${String(from).replace(/\D/g, '')}` : null;
    const profileName = value?.contacts?.[0]?.profile?.name || '';

    let textRaw = '';
    let msgType = msg.type || 'text';

    if (msgType === 'text') {
      textRaw = msg.text?.body || '';
    } else if (msgType === 'interactive') {
      if (msg.interactive?.type === 'button_reply') {
        textRaw = msg.interactive.button_reply?.title || msg.interactive.button_reply?.id || '';
      } else if (msg.interactive?.type === 'list_reply') {
        textRaw = msg.interactive.list_reply?.title || msg.interactive.list_reply?.id || '';
      } else {
        textRaw = '';
      }
      msgType = 'text';
    } else if (msgType === 'button') {
      textRaw = msg?.button?.text || msg?.button?.payload || '';
      msgType = 'text';
    } else if (['image','audio','video','document','sticker','contacts','location'].includes(msgType)) {
      textRaw = '';
      msgType = 'media';
    } else {
      textRaw = msg?.text?.body || '';
      if (!textRaw) msgType = 'media';
    }

    return { msg, from, fromE164, mid, textRaw, profileName, msgType };
  } catch {
    return {
      msg: null, from: null, fromE164: null,
      mid: `${Date.now()}_${Math.random()}`, textRaw: '',
      profileName: '', msgType: 'event'
    };
  }
}

// ---- Router principal (muy simple y seguro) --------------------------------
async function handleIncoming(env, session, ctx){
  const to = ctx.fromE164;
  const raw = clean(ctx.textRaw||'');
  const lowered = normalizeBase(raw.toLowerCase());

  // Saludo
  if (RX_GREET.test(raw)) {
    await sendWhatsAppText(env, to, `Hola ${ctx.profileName || ''}! 👋 ¿En qué te puedo ayudar hoy?`);
    return;
  }

  // Inventario / ventas
  if (isInventoryIntent(lowered)) {
    await startSalesFromQuery(env, session, to, raw);
    return;
  }

  // Soporte (simplificado, reutiliza tu backend en siguientes versiones)
  if (isSupportIntent(lowered)) {
    await sendWhatsAppText(env, to,
      'Gracias por la info. Para avanzar, ¿marca y modelo de la impresora? y una breve descripción de la falla.');
    session.flow = 'support';
    await saveSession(env, to, session);
    return;
  }

  // Si veníamos del flujo de soporte y el usuario ya describió:
  if (session.flow === 'support' && raw.length > 5) {
    await sendWhatsAppText(env, to,
      'Anotado. Puedo agendar una visita de técnico. ¿Confirmamos con tu dirección registrada o deseas editarla? (Escribe: confirmar / editar)');
    session.flow = 'support_confirm';
    session.support_note = raw;
    await saveSession(env, to, session);
    return;
  }

  // Último recurso
  await sendWhatsAppText(env, to,
    '¿Buscas consumibles/refacciones o necesitas soporte técnico? (Escribe: consumibles / soporte)');
}

// ---- Handler fetch ----------------------------------------------------------
async function routeHealth(env){
  const have = {
    WA_TOKEN: !!env.WA_TOKEN,
    PHONE_ID: !!env.PHONE_ID,
    VERIFY_TOKEN: !!env.VERIFY_TOKEN,
    SUPABASE_URL: !!env.SUPABASE_URL,
    SUPABASE_ANON_KEY: !!env.SUPABASE_ANON_KEY,
    TZ: TZ
  };
  return new Response(JSON.stringify({ ok:true, have, now: new Date().toISOString() }), {
    status: 200, headers: { 'Content-Type':'application/json' }
  });
}

async function routeSelfTest(env, url){
  const to = clean(url.searchParams.get('to')||'');
  if(!to) return badRequest('missing to');
  await sendWhatsAppText(env, to, 'Prueba directa desde el Worker ✅');
  return ok('sent');
}

async function routeWebhook(env, req){
  const payload = await safeJson(req);
  const ctx = extractWhatsAppContext(payload);
  if(!ctx) return ok('EVENT_RECEIVED');

  // Logging opcional
  try{
    const dbg = (env.DEBUG||'').toString().toLowerCase()==='true';
    const dbgJson = (env.DEBUG_JSON||'').toString().toLowerCase()==='true';
    if(dbgJson) console.log('[webhook.json]', JSON.stringify(payload).slice(0,4000));
    else if(dbg) console.log('[webhook]', { from: ctx.fromE164, type: ctx.msgType, preview: (ctx.textRaw||'').slice(0,80) });
  }catch{}

  // Eventos de estatus (delivered/read)
  if(!ctx.msg && ctx.msgType==='event'){
    return ok('EVENT_RECEIVED');
  }

  // Echo de depuración si quieres ver vida
  if((env.DEBUG_WEBHOOK||'').toString().toLowerCase()==='true'){
    try { await sendWhatsAppText(env, ctx.fromE164, `🔎 Webhook OK (${ctx.msgType}) "${(ctx.textRaw||'').slice(0,100)}"`); } catch {}
  }

  // Carga/guarda de sesión y enrutamiento
  const session = await loadSession(env, ctx.fromE164).catch(()=> ({}));
  await handleIncoming(env, session||{}, ctx);

  return ok('EVENT_RECEIVED');
}

// ---- Export default ---------------------------------------------------------
export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // GET: verificación Webhook (si lo usas) y health
    if (req.method === 'GET' && url.pathname === '/health') {
      return routeHealth(env);
    }
    if (req.method === 'GET' && url.pathname === '/selftest') {
      return routeSelfTest(env, url);
    }

    // WhatsApp Webhook (POST /)
    if (req.method === 'POST' && url.pathname === '/') {
      return routeWebhook(env, req);
    }

    return ok('ok');
  }
};
