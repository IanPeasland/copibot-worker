/**
 * CopiBot – Worker Lite (SIN IA)
 * Conversacional + Ventas + Soporte Técnico + (GCal opcional) + Supabase
 * Build: “Lite-R12-toner-fix”
 *
 * Tablas (schema public) esperadas:
 *  - wa_session(from text pk, stage text, data jsonb, updated_at timestamptz)
 *  - producto_stock_v(id, nombre, marca, sku, precio, stock, tipo, compatible)
 *  - cliente, pedido, pedido_item
 *  - orden_servicio, calendar_pool (si usas soporte + GCal)
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    // Verificación Webhook WhatsApp
    if (req.method === "GET" && url.pathname === "/") {
      const mode = url.searchParams.get("hub.mode");
      const token = url.searchParams.get("hub.verify_token");
      const challenge = url.searchParams.get("hub.challenge");
      if (mode === "subscribe" && token === env.VERIFY_TOKEN) {
        return new Response(challenge, { status: 200 });
      }
      return new Response("Forbidden", { status: 403 });
    }

    // Health
    if (req.method === "GET" && url.pathname === "/health") {
      return json({
        ok: true,
        now: new Date().toISOString(),
        have: {
          WA_TOKEN: !!env.WA_TOKEN,
          PHONE_ID: !!env.PHONE_ID,
          SUPABASE_URL: !!env.SUPABASE_URL,
          SUPABASE_ANON_KEY: !!env.SUPABASE_ANON_KEY,
        },
      });
    }

    // Cron manual
    if (req.method === "POST" && url.pathname === "/cron") {
      const sec =
        req.headers.get("x-cron-secret") || url.searchParams.get("secret");
      if (!sec || sec !== env.CRON_SECRET)
        return new Response("Forbidden", { status: 403 });
      return json({ ok: true, ts: Date.now() });
    }

    // Webhook principal
    if (req.method === "POST" && url.pathname === "/") {
      try {
        const body = await safeJson(req);
        const ctx = extractWhatsAppContext(body);
        if (!ctx) return ok("EVENT_RECEIVED");

        const { from, fromE164, textRaw, msgType, profileName, ts, mid, media } =
          ctx;

        // Sesión
        const now = new Date();
        let session = await loadSession(env, from);
        session.data = session.data || {};
        session.from = from;
        session.stage = session.stage || "idle";

        // idempotencia simple
        const msgTs = Number(ts || Date.now());
        const lastTs = Number(session?.data?.last_ts || 0);
        if (lastTs && msgTs + 5000 <= lastTs) return ok("EVENT_RECEIVED");
        if (session?.data?.last_mid === mid) return ok("EVENT_RECEIVED");
        session.data.last_ts = msgTs;
        session.data.last_mid = mid;

        // nombre visible
        if (profileName && !session?.data?.customer?.nombre) {
          session.data.customer = session.data.customer || {};
          session.data.customer.nombre = toTitleCase(firstWord(profileName));
        }

        // Audios -> a soporte
        if (msgType === "audio") {
          await forwardAudioToSupport(env, media, fromE164);
          await sendWhatsAppText(
            env,
            fromE164,
            "Lo siento, aún no puedo escuchar audios. Ya avisé a soporte y en breve te contactan. Si gustas, escríbeme el detalle y te ayudo por aquí 🙂"
          );
          await saveSession(env, session, now);
          return ok("EVENT_RECEIVED");
        }
        if (msgType !== "text") {
          await sendWhatsAppText(
            env,
            fromE164,
            "¿Podrías escribirme con palabras lo que necesitas? Así te ayudo más rápido 🙂"
          );
          await saveSession(env, session, now);
          return ok("EVENT_RECEIVED");
        }

        // Texto
        const text = (textRaw || "").trim();
        const lowered = text.toLowerCase();
        const ntext = normalizeWithAliases(text);

        // Saludo => limpiar candidato viejo y saludar
        if (RX_GREET.test(lowered)) {
          session.data.last_candidate = null;
          session.stage = "idle";
          await saveSession(env, session, now);
          const nombre = toTitleCase(firstWord(session?.data?.customer?.nombre));
          await sendWhatsAppText(
            env,
            fromE164,
            `¡Hola${nombre ? " " + nombre : ""}! ¿En qué te puedo ayudar hoy? 👋`
          );
          return ok("EVENT_RECEIVED");
        }

        // Etapas activas
        if (session.stage === "ask_qty")
          return await handleAskQty(
            env,
            session,
            fromE164,
            text,
            lowered,
            ntext,
            now
          );
        if (session.stage === "cart_open")
          return await handleCartOpen(
            env,
            session,
            fromE164,
            text,
            lowered,
            ntext,
            now
          );
        if (session.stage === "await_invoice")
          return await handleAwaitInvoice(
            env,
            session,
            fromE164,
            lowered,
            now,
            text
          );
        if (session.stage && session.stage.startsWith("collect_"))
          return await handleCollectSequential(env, session, fromE164, text, now);

        // Intenciones básicas (sin IA)
        const supportIntent = isSupportIntent(ntext);
        const salesIntent = isSalesIntent(ntext);

        if (supportIntent) {
          // para este build nos enfocamos en ventas; soporte queda igual que antes
          // (si en lo tuyo ya estaba funcionando, lo puedes re-conectar aquí)
          await sendWhatsAppText(
            env,
            fromE164,
            "Te ayudo con soporte técnico 👨‍🔧. Cuéntame marca y modelo del equipo y una breve descripción de la falla."
          );
          return ok("EVENT_RECEIVED");
        }

        if (salesIntent) {
          // limpiar candidato viejo SIEMPRE en nueva búsqueda
          session.data.last_candidate = null;
          session.stage = "idle";
          await saveSession(env, session, now);
          return await startSalesFromQuery(env, session, fromE164, text, ntext, now);
        }

        // Fallback
        await sendWhatsAppText(
          env,
          fromE164,
          "Puedo cotizar *consumibles/refacciones* o agendar *soporte técnico*. ¿Qué necesitas?"
        );
        await saveSession(env, session, now);
        return ok("EVENT_RECEIVED");
      } catch (e) {
        console.error("worker error", e);
        return ok("EVENT_RECEIVED");
      }
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // opcional
  },
};

/* ───────────────────────────── Utils ───────────────────────────── */
function ok(s = "ok") {
  return new Response(s, { status: 200 });
}
function json(obj) {
  return new Response(JSON.stringify(obj), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}
async function safeJson(req) {
  try {
    return await req.json();
  } catch {
    return {};
  }
}
const firstWord = (s = "") => (s || "").trim().split(/\s+/)[0] || "";
const toTitleCase = (s = "") =>
  s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : "";
function clean(s = "") {
  return s.replace(/\s+/g, " ").trim();
}
function normalizeBase(s = "") {
  return s
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}
function numberOrZero(n) {
  const v = Number(n || 0);
  return Number.isFinite(v) ? v : 0;
}
function formatMoneyMXN(n) {
  const v = Number(n || 0);
  try {
    return new Intl.NumberFormat("es-MX", {
      style: "currency",
      currency: "MXN",
      maximumFractionDigits: 2,
    }).format(v);
  } catch {
    return `$${v.toFixed(2)}`;
  }
}
function priceWithIVA(n) {
  return `${formatMoneyMXN(Number(n || 0))} + IVA`;
}

/* ───────────────────── Intents (sin IA) ───────────────────── */
const RX_GREET =
  /^(hola+|buen[oa]s|qué onda|que tal|saludos|hey|buen dia|buenas|holi+)\b/i;

function normalizeWithAliases(s = "") {
  const t = normalizeBase(s);
  const aliases = [
    ["verzan", "versant"],
    ["versan", "versant"],
    ["versa link", "versalink"],
    ["alta link", "altalink"],
    ["docu color", "docucolor"],
    ["prime link", "primelink"],
    ["fuji film", "fujifilm"],
  ];
  let out = t;
  for (const [bad, good] of aliases)
    out = out.replace(new RegExp(`\\b${bad}\\b`, "g"), good);
  return out;
}

const RX_INV_KWS =
  /(toner|t[óo]ner|cartucho|developer|unidad de revelado|refacci[oó]n|precio|costo|sku|pieza|compatible)\b/i;
const RX_MODEL_KWS =
  /(versant|docucolor|primelink|versalink|altalink|apeos|work ?center|workcentre|c\d{2,4}|b\d{2,4}|550|560|570|2100|180|280|4100|c70|c60|c75)\b/i;

function isSalesIntent(ntext = "") {
  return RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext);
}
function isSupportIntent(ntext = "") {
  const t = `${ntext}`;
  const hasProblem =
    /(falla(?:ndo)?|fallo|problema|descompuest[oa]|no imprime|no escanea|no copia|no prende|no enciende|se apaga|error|atasc|ator(?:a|o|e|ando|ada|ado)|mancha|l[ií]nea|ruido)/.test(
      t
    );
  const hasDevice =
    /(impresora|equipo|copiadora|xerox|fujifilm|fuji\s?film|versant|versalink|altalink|docucolor|c\d{2,4}|b\d{2,4})/.test(
      t
    );
  return (hasProblem && hasDevice) || /\b(soporte|servicio|visita)\b/.test(t);
}

const RX_NEG_NO = /\b(no|nel|ahorita no|no gracias)\b/i;
const RX_DONE =
  /\b(es(ta)?\s*todo|ser[ií]a\s*todo|nada\s*m[aá]s|con\s*eso|as[ií]\s*est[aá]\s*bien|ya\s*qued[oó]|listo|finaliza(r|mos)?|termina(r)?)\b/i;

/* ─────────────────── WhatsApp send helpers ─────────────────── */
async function sendWhatsAppText(env, toE164, body) {
  if (!env.WA_TOKEN || !env.PHONE_ID) return;
  const url = `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`;
  const payload = {
    messaging_product: "whatsapp",
    to: toE164.replace(/\D/g, ""),
    text: { body },
  };
  const r = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.WA_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!r.ok) console.warn("sendWhatsAppText", r.status, await r.text());
}
async function forwardAudioToSupport(env, media, fromE164) {
  try {
    const to = env.SUPPORT_WHATSAPP || env.SUPPORT_PHONE_E164;
    if (!to) return;
    const txt = `Audio recibido de ${fromE164} (id:${media?.id || "N/A"}). Revísalo en WhatsApp Business / Meta.`;
    await sendWhatsAppText(env, to, `🗣️ *Audio reenviado*\n${txt}`);
  } catch {}
}

/* ───────────────────────── Sesión (Supabase) ───────────────────────── */
async function loadSession(env, phone) {
  try {
    const r = await sbGet(env, "wa_session", {
      query:
        `select=from,stage,data,updated_at&` +
        `from=eq.${encodeURIComponent(phone)}&limit=1`,
    });
    if (Array.isArray(r) && r[0])
      return {
        from: r[0].from,
        stage: r[0].stage || "idle",
        data: r[0].data || {},
      };
    return { from: phone, stage: "idle", data: {} };
  } catch (e) {
    console.warn("loadSession", e);
    return { from: phone, stage: "idle", data: {} };
  }
}
async function saveSession(env, session, now = new Date()) {
  try {
    await sbUpsert(
      env,
      "wa_session",
      [
        {
          from: session.from,
          stage: session.stage || "idle",
          data: session.data || {},
          updated_at: now.toISOString(),
        },
      ],
      { onConflict: "from", returning: "minimal" }
    );
  } catch (e) {
    console.warn("saveSession", e);
  }
}

/* ─────────────── Cantidades ─────────────── */
const NUM_WORDS = {
  cero: 0,
  una: 1,
  uno: 1,
  un: 1,
  dos: 2,
  tres: 3,
  cuatro: 4,
  cinco: 5,
  seis: 6,
  siete: 7,
  ocho: 8,
  nueve: 9,
  diez: 10,
  once: 11,
  doce: 12,
  docena: 12,
  "media docena": 6,
};
function looksLikeQuantityStrict(t = "") {
  const hasDigit = /\b\d+\b/.test(t);
  const hasWord = Object.keys(NUM_WORDS).some((w) =>
    new RegExp(`\\b${w}\\b`, "i").test(t)
  );
  return hasDigit || hasWord;
}
function parseQty(text, fallback = 1) {
  const t = normalizeBase(text);
  if (/\bmedia\s+docena\b/i.test(t)) return 6;
  if (/\bdocena\b/i.test(t)) return 12;
  for (const [w, n] of Object.entries(NUM_WORDS))
    if (new RegExp(`\\b${w}\\b`, "i").test(t)) return Number(n);
  const m = t.match(/\b(\d+)\b/);
  if (m) return Number(m[1]);
  return fallback;
}

/* ─────────────── Inventario & Carrito ─────────────── */
function pushCart(session, product, qty, backorder = false) {
  session.data = session.data || {};
  session.data.cart = session.data.cart || [];
  const key = `${product?.sku || product?.id || product?.nombre}${
    backorder ? "_bo" : ""
  }`;
  const existing = session.data.cart.find((i) => i.key === key);
  if (existing) existing.qty += qty;
  else session.data.cart.push({ key, product, qty, backorder });
}
function addWithStockSplit(session, product, qty) {
  const s = numberOrZero(product?.stock);
  const take = Math.min(s, qty);
  const rest = Math.max(0, qty - take);
  if (take > 0) pushCart(session, product, take, false);
  if (rest > 0) pushCart(session, product, rest, true);
}

function renderProducto(p, queryText = "") {
  const precio = priceWithIVA(p.precio);
  const sku = p.sku ? `\nSKU: ${p.sku}` : "";
  const marca = p.marca ? `\nMarca: ${p.marca}` : "";
  const s = numberOrZero(p.stock);
  const stockLine = s > 0 ? `${s} pzas en stock` : `0 pzas — *sobre pedido*`;
  // Formato solicitado
  return `1. ${p.nombre}${marca}${sku}\n${precio}\n${stockLine}\n\n¿Te funciona?\nSi sí, dime *cuántas piezas*; hay ${Math.max(
    0,
    s
  )} en stock y el resto sería *sobre pedido*.`;
}

/* ─────────────── Flujo ventas ─────────────── */
async function handleAskQty(
  env,
  session,
  toE164,
  text,
  lowered,
  ntext,
  now
) {
  const cand = session.data?.last_candidate;

  if (RX_NEG_NO.test(lowered)) {
    session.data.last_candidate = null;
    session.stage = "cart_open";
    await saveSession(env, session, now);
    await sendWhatsAppText(
      env,
      toE164,
      "Sin problema. ¿Busco otra opción? Dime modelo/color o lo que necesitas."
    );
    return ok("EVENT_RECEIVED");
  }

  // Si no hay candidato (por si acaso)
  if (!cand) {
    session.stage = "cart_open";
    await saveSession(env, session, now);
    await sendWhatsAppText(
      env,
      toE164,
      "¿Qué artículo necesitas? (modelo/color, p. ej. “tóner amarillo versant”)"
    );
    return ok("EVENT_RECEIVED");
  }

  if (!looksLikeQuantityStrict(lowered)) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(
      env,
      toE164,
      `¿Cuántas *piezas* necesitas? (hay ${s} en stock; el resto sería *sobre pedido*)`
    );
    await saveSession(env, session, now);
    return ok("EVENT_RECEIVED");
  }

  const qty = parseQty(lowered, 1);
  if (!Number.isFinite(qty) || qty <= 0) {
    const s = numberOrZero(cand.stock);
    await sendWhatsAppText(
      env,
      toE164,
      `Necesito un número de piezas (hay ${s} en stock).`
    );
    await saveSession(env, session, now);
    return ok("EVENT_RECEIVED");
  }

  addWithStockSplit(session, cand, qty);
  session.stage = "cart_open";
  await saveSession(env, session, now);

  const s = numberOrZero(cand.stock);
  const bo = Math.max(0, qty - Math.min(s, qty));
  const nota =
    bo > 0
      ? `\n(De ${qty}, ${Math.min(s, qty)} en stock y ${bo} sobre pedido)`
      : "";
  await sendWhatsAppText(
    env,
    toE164,
    `Añadí 🛒\n• ${cand.nombre} x ${qty} ${priceWithIVA(
      cand.precio
    )}${nota}\n\n¿Deseas *agregar algo más* o *finalizamos*?`
  );
  return ok("EVENT_RECEIVED");
}

async function handleCartOpen(
  env,
  session,
  toE164,
  text,
  lowered,
  ntext,
  now
) {
  session.data = session.data || {};
  const cart = session.data.cart || [];

  if (RX_DONE.test(lowered) || (RX_NEG_NO.test(lowered) && cart.length > 0)) {
    if (!cart.length && session.data.last_candidate)
      addWithStockSplit(session, session.data.last_candidate, 1);
    session.stage = "await_invoice";
    await saveSession(env, session, now);
    await sendWhatsAppText(
      env,
      toE164,
      "¿La quieres *con factura* o *sin factura*?"
    );
    return ok("EVENT_RECEIVED");
  }

  if (looksLikeQuantityStrict(lowered) && session.data?.last_candidate) {
    session.stage = "ask_qty";
    await saveSession(env, session, now);
    const s = numberOrZero(session.data.last_candidate.stock);
    await sendWhatsAppText(
      env,
      toE164,
      `Perfecto. ¿Cuántas *piezas* en total? (hay ${s} en stock; el resto sería *sobre pedido*)`
    );
    return ok("EVENT_RECEIVED");
  }

  if (RX_INV_KWS.test(ntext) || RX_MODEL_KWS.test(ntext)) {
    return await startSalesFromQuery(env, session, toE164, text, ntext, now);
  }

  await sendWhatsAppText(
    env,
    toE164,
    "Te leo 🙂. Puedo agregar un artículo nuevo, buscar otro o *finalizar* si ya está completo."
  );
  await saveSession(env, session, now);
  return ok("EVENT_RECEIVED");
}

async function startSalesFromQuery(env, session, toE164, text, ntext, now) {
  // limpia candidato viejo SIEMPRE
  session.data = session.data || {};
  session.data.last_candidate = null;
  await saveSession(env, session, now);

  const best = await findBestProduct(env, ntext);
  if (best) {
    session.stage = "ask_qty";
    session.data.last_candidate = best;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, renderProducto(best, ntext));
    return ok("EVENT_RECEIVED");
  }

  await sendWhatsAppText(
    env,
    toE164,
    "No encontré una coincidencia directa 😕. ¿Me das el *modelo* y el *color* del consumible? (ej. *tóner amarillo Versant 180*)"
  );
  await saveSession(env, session, now);
  return ok("EVENT_RECEIVED");
}

/* ─────────────── Matching ─────────────── */
function extractColorWord(text = "") {
  const t = normalizeWithAliases(text);
  if (/\b(amarillo|yellow)\b/i.test(t)) return "yellow";
  if (/\bmagenta\b/i.test(t)) return "magenta";
  if (/\b(cyan|cian)\b/i.test(t)) return "cyan";
  if (/\b(negro|black|bk|k)\b/i.test(t)) return "black";
  return null;
}

function productHasColor(p, colorCode) {
  if (!colorCode) return true;
  const s = `${normalizeBase([p?.nombre, p?.sku, p?.marca].join(" "))}`;
  const map = {
    yellow: [/\bamarillo\b/i, /\byellow\b/i, /(^|[\s\-_\/])y($|[\s\-_\/])/i],
    magenta: [/\bmagenta\b/i, /(^|[\s\-_\/])m($|[\s\-_\/])/i],
    cyan: [/\bcyan\b/i, /\bcian\b/i, /(^|[\s\-_\/])c($|[\s\-_\/])/i],
    black: [
      /\bnegro\b/i,
      /\bblack\b/i,
      /(^|[\s\-_\/])k($|[\s\-_\/])/i,
      /(^|[\s\-_\/])bk($|[\s\-_\/])/i,
    ],
  };
  const arr = map[colorCode] || [];
  return arr.some(
    (rx) => rx.test(p?.nombre) || rx.test(p?.sku) || rx.test(s)
  );
}

function productMatchesFamily(p, text = "") {
  const t = normalizeWithAliases(text);
  const s = normalizeBase([p?.nombre, p?.sku, p?.marca, p?.compatible].join(" "));
  const fams = [
    [
      "versant",
      /(versant|80|180|2100|280|4100)\b/i,
      /(docu\s*color|primelink|alta\s*link|versa\s*link|c(60|70|75)|550|560|570)/i,
    ],
    [
      "docucolor",
      /(docu\s*color|550|560|570)\b/i,
      /(versant|primelink|alta\s*link|versa\s*link|2100|180|280|4100)/i,
    ],
    ["primelink", /(prime\s*link|primelink)\b/i, /(versant|versa\s*link|alta\s*link)/i],
    ["versalink", /(versa\s*link|versalink)\b/i, /(versant|prime\s*link|alta\s*link)/i],
    ["altalink", /(alta\s*link|altalink)\b/i, /(versant|prime\s*link|versa\s*link)/i],
    ["apeos", /\bapeos\b/i, null],
    ["c70", /\bc(60|70|75)\b/i, null],
  ];
  for (const [, hit, bad] of fams) {
    if (hit.test(t)) {
      if (bad && bad.test(s)) return false;
      return hit.test(s);
    }
  }
  return true;
}

function userWantsToner(queryText = "") {
  return /\bton(e|é)r|t[oó]ner\b/i.test(queryText);
}

function nameLooksLikeRevelado(p) {
  return /\bunidad\s+de\s+revelado\b/i.test(p?.nombre || "");
}

function nameLooksLikeToner(p) {
  return /\btoner\b/i.test(p?.nombre || "");
}

async function findBestProduct(env, queryText) {
  const wantsToner = userWantsToner(queryText);
  const colorCode = extractColorWord(queryText);

  const scoreAndPick = (arr = []) => {
    if (!Array.isArray(arr) || !arr.length) return null;

    // 1) Excluir UNIDAD DE REVELADO si el usuario pide toner
    if (wantsToner) arr = arr.filter((p) => !nameLooksLikeRevelado(p));

    // 2) Filtrar por familia (evita mezclar Versant / DocuColor)
    arr = arr.filter((p) => productMatchesFamily(p, queryText));

    // 3) Si el usuario dio color, obligar a que coincida
    if (colorCode) arr = arr.filter((p) => productHasColor(p, colorCode));

    // 4) Si el usuario dijo “tóner”, priorizar nombres con TONER
    if (wantsToner) {
      const tOnly = arr.filter((p) => nameLooksLikeToner(p));
      if (tOnly.length) arr = tOnly;
    }

    if (!arr.length) return null;

    // 5) Orden: stock primero, luego precio
    arr.sort((a, b) => {
      const sa = numberOrZero(a.stock) > 0 ? 1 : 0;
      const sb = numberOrZero(b.stock) > 0 ? 1 : 0;
      if (sa !== sb) return sb - sa;
      return numberOrZero(a.precio || 0) - numberOrZero(b.precio || 0);
    });

    return arr[0] || null;
  };

  // A) RPC fuzzy si existe
  try {
    const res =
      (await sbRpc(env, "match_products_trgm", {
        q: queryText,
        match_count: 60,
      })) || [];
    const pick = scoreAndPick(res);
    if (pick) return pick;
  } catch {}

  // B) Consulta REST con filtros
  try {
    // si pide toner, buscamos nombre ILIKE %toner% y EXCLUIMOS %revelado%
    const likeToner = wantsToner ? "%toner%" : "%";
    const q = [
      "select=id,nombre,marca,sku,precio,stock,tipo,compatible",
      `or=(nombre.ilike.${encodeURIComponent(
        likeToner
      )},sku.ilike.${encodeURIComponent(likeToner)})`,
    ];

    if (wantsToner) {
      // excluye revelado
      q.push(`nombre.not.ilike.${encodeURIComponent("%revelado%")}`);
    }

    // familia “versant/docucolor/…” si aparece en el texto
    const fam = [];
    if (/versant/i.test(queryText)) fam.push("%versant%");
    if (/docu\s*color|550|560|570/i.test(queryText)) fam.push("%docu%");
    if (/primelink/i.test(queryText)) fam.push("%prime%");
    if (/versalink/i.test(queryText)) fam.push("%versal%");
    if (/altalink/i.test(queryText)) fam.push("%altalink%");
    if (/apeos/i.test(queryText)) fam.push("%apeos%");
    if (fam.length) {
      q.push(
        `nombre.ilike.${encodeURIComponent(fam[0])}${
          fam[1] ? `&nombre.ilike=${encodeURIComponent(fam[1])}` : ""
        }`
      );
    }

    q.push("order=stock.desc.nullslast,precio.asc");
    q.push("limit=600");

    const r2 =
      (await sbGet(env, "producto_stock_v", {
        query: q.join("&"),
      })) || [];

    const pick2 = scoreAndPick(r2);
    if (pick2) return pick2;
  } catch (e) {
    console.warn("findBestProduct REST", e);
  }

  return null;
}

/* ─────────────── Cierre de venta (igual que antes) ─────────────── */
const FLOW_FACT = ["nombre", "rfc", "email", "calle", "numero", "colonia", "cp"];
const FLOW_SHIP = ["nombre", "email", "calle", "numero", "colonia", "cp"];
const LABEL = {
  nombre: "Nombre / Razón Social",
  rfc: "RFC",
  email: "Email",
  calle: "Calle",
  numero: "Número",
  colonia: "Colonia",
  cp: "Código Postal",
};
function firstMissing(list, c = {}) {
  for (const k of list) if (!String(c[k] ?? "").trim()) return k;
  return null;
}
function parseCustomerFragment(field, text) {
  const t = text;
  if (field === "nombre") return clean(t);
  if (field === "rfc") {
    const m = t.match(/\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b/i);
    return m ? m[1].toUpperCase() : clean(t).toUpperCase();
  }
  if (field === "email") {
    const m = t.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return m ? m[0].toLowerCase() : clean(t).toLowerCase();
  }
  if (field === "numero") {
    const m = t.match(/\b(\d+[A-Z]?)\b/i);
    return m ? m[1] : clean(t);
  }
  if (field === "cp") {
    const m = t.match(/\b(\d{5})\b/);
    return m ? m[1] : clean(t);
  }
  return clean(t);
}

async function handleAwaitInvoice(env, session, toE164, lowered, now) {
  if (/\b(no|gracias|todo bien)\b/i.test(lowered)) {
    session.stage = "idle";
    await saveSession(env, session, now);
    await sendWhatsAppText(
      env,
      toE164,
      "Perfecto, quedo al pendiente. Si necesitas algo más, aquí estoy 🙂"
    );
    return ok("EVENT_RECEIVED");
  }

  const saysNo = /\b(sin(\s+factura)?|sin|no)\b/i.test(lowered);
  const saysYes =
    !saysNo && /\b(s[ií]|sí|si|con(\s+factura)?|con|factura)\b/i.test(lowered);

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
      return ok("EVENT_RECEIVED");
    }
    const res = await createOrderFromSession(env, session);
    if (res?.ok) {
      await sendWhatsAppText(
        env,
        toE164,
        `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(
          res.total
        )} + IVA\nUn asesor te confirmará entrega y forma de pago.`
      );
    } else {
      await sendWhatsAppText(
        env,
        toE164,
        `Creé tu solicitud y la pasé a un asesor para confirmar detalles. 🙌`
      );
    }
    session.stage = "idle";
    session.data.cart = [];
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿Te ayudo con algo más? (Sí / No)`);
    return ok("EVENT_RECEIVED");
  }

  await sendWhatsAppText(env, toE164, `¿La quieres con factura o sin factura?`);
  await saveSession(env, session, now);
  return ok("EVENT_RECEIVED");
}

async function handleCollectSequential(env, session, toE164, text, now) {
  session.data = session.data || {};
  session.data.customer = session.data.customer || {};
  const c = session.data.customer;
  const list = session.data.requires_invoice ? FLOW_FACT : FLOW_SHIP;
  const field = session.stage.replace("collect_", "");
  c[field] = parseCustomerFragment(field, text);
  await saveSession(env, session, now);
  const nextField = firstMissing(list, c);
  if (nextField) {
    session.stage = `collect_${nextField}`;
    await saveSession(env, session, now);
    await sendWhatsAppText(env, toE164, `¿${LABEL[nextField]}?`);
    return ok("EVENT_RECEIVED");
  }
  const res = await createOrderFromSession(env, session);
  if (res?.ok) {
    await sendWhatsAppText(
      env,
      toE164,
      `¡Listo! Generé tu solicitud 🙌\n*Total estimado:* ${formatMoneyMXN(
        res.total
      )} + IVA\nUn asesor te confirmará entrega y forma de pago.`
    );
  } else {
    await sendWhatsAppText(
      env,
      toE164,
      `Creé tu solicitud y la pasé a un asesor humano para confirmar detalles. 🙌`
    );
  }
  session.stage = "idle";
  session.data.cart = [];
  await saveSession(env, session, now);
  await sendWhatsAppText(env, toE164, `¿Puedo ayudarte con algo más? (Sí / No)`);
  return ok("EVENT_RECEIVED");
}

/* ─────────────── Cliente & Pedido ─────────────── */
async function preloadCustomerIfAny(env, session) {
  try {
    const r = await sbGet(env, "cliente", {
      query: `select=nombre,rfc,email,calle,numero,colonia,ciudad,estado,cp&telefono=eq.${session.from}&limit=1`,
    });
    if (r && r[0])
      session.data.customer = { ...(session.data.customer || {}), ...r[0] };
  } catch {}
}

async function ensureClienteFields(env, cliente_id, c) {
  try {
    const patch = {};
    ["nombre", "rfc", "email", "calle", "numero", "colonia", "ciudad", "estado", "cp"].forEach(
      (k) => {
        if (String(c[k] ?? "").trim()) patch[k] = c[k];
      }
    );
    if (Object.keys(patch).length > 0)
      await sbPatch(env, "cliente", patch, `id=eq.${cliente_id}`);
  } catch {}
}

async function createOrderFromSession(env, session) {
  try {
    const cart = session.data?.cart || [];
    if (!cart.length) return { ok: false, error: "empty cart" };
    const c = session.data.customer || {};
    let cliente_id = null;

    try {
      const exist = await sbGet(env, "cliente", {
        query: `select=id,telefono,email&or=(telefono.eq.${session.from},email.eq.${encodeURIComponent(
          c.email || ""
        )})&limit=1`,
      });
      if (exist && exist[0]) cliente_id = exist[0].id;
    } catch {}

    if (!cliente_id) {
      const ins = await sbUpsert(
        env,
        "cliente",
        [
          {
            nombre: c.nombre || null,
            rfc: c.rfc || null,
            email: c.email || null,
            telefono: session.from || null,
            calle: c.calle || null,
            numero: c.numero || null,
            colonia: c.colonia || null,
            ciudad: c.ciudad || null,
            estado: c.estado || null,
            cp: c.cp || null,
          },
        ],
        { onConflict: "telefono", returning: "representation" }
      );
      cliente_id = ins?.data?.[0]?.id || null;
    } else {
      await ensureClienteFields(env, cliente_id, c);
    }

    let total = 0;
    for (const it of cart)
      total += Number(it.product?.precio || 0) * Number(it.qty || 1);

    const p = await sbUpsert(
      env,
      "pedido",
      [
        {
          cliente_id,
          total,
          moneda: "MXN",
          estado: "nuevo",
          created_at: new Date().toISOString(),
        },
      ],
      { returning: "representation" }
    );
    const pedido_id = p?.data?.[0]?.id;

    const items = cart.map((it) => ({
      pedido_id,
      producto_id: it.product?.id || null,
      sku: it.product?.sku || null,
      nombre: it.product?.nombre || null,
      qty: it.qty,
      precio_unitario: Number(it.product?.precio || 0),
    }));
    await sbUpsert(env, "pedido_item", items, { returning: "minimal" });

    // decremento de stock solo si hay stock real
    for (const it of cart) {
      const sku = it.product?.sku;
      if (!sku) continue;
      try {
        const row = await sbGet(env, "producto_stock_v", {
          query: `select=sku,stock&sku=eq.${encodeURIComponent(sku)}&limit=1`,
        });
        const current = numberOrZero(row?.[0]?.stock);
        const toDec = Math.min(current, Number(it.qty || 0));
        if (toDec > 0)
          await sbRpc(env, "decrement_stock", { in_sku: sku, in_by: toDec });
      } catch {}
    }

    return { ok: true, pedido_id, total };
  } catch (e) {
    console.warn("createOrderFromSession", e);
    return { ok: false, error: String(e) };
  }
}

/* ─────────────── Supabase helpers ─────────────── */
async function sbGet(env, table, { query }) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${query}`;
  const r = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
    },
  });
  if (!r.ok) {
    console.warn("sbGet", table, await r.text());
    return null;
  }
  return await r.json();
}
async function sbUpsert(env, table, rows, { onConflict, returning = "representation" } = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}${
    onConflict ? `?on_conflict=${onConflict}` : ""
  }`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      "Content-Type": "application/json",
      Prefer: `resolution=merge-duplicates,return=${returning}`,
    },
    body: JSON.stringify(rows),
  });
  if (!r.ok) {
    console.warn("sbUpsert", table, await r.text());
    return null;
  }
  const data = returning === "minimal" ? null : await r.json();
  return { data };
}
async function sbPatch(env, table, patch, filter) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filter}`;
  const r = await fetch(url, {
    method: "PATCH",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(patch),
  });
  if (!r.ok) console.warn("sbPatch", table, await r.text());
}
async function sbRpc(env, fn, params) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fn}`;
  const r = await fetch(url, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(params || {}),
  });
  if (!r.ok) {
    console.warn("sbRpc", fn, await r.text());
    return null;
  }
  return await r.json();
}

/* ─────────────── WhatsApp Webhook parser ─────────────── */
function extractWhatsAppContext(payload) {
  try {
    const entry = payload?.entry?.[0];
    const changes = entry?.changes?.[0];
    const msgs = changes?.value?.messages;
    if (!msgs || !msgs[0]) return null;
    const m = msgs[0];
    const from = m?.from;
    const fromE164 = `+${from}`;
    const textRaw = m?.text?.body || m?.interactive?.body?.text || "";
    const msgType = m?.type || (m?.audio ? "audio" : "text");
    const media = m?.audio || null;
    const profileName = changes?.value?.contacts?.[0]?.profile?.name || "";
    const ts = Number(m?.timestamp || Date.now() / 1000) * 1000;
    const mid = m?.id;
    return { from, fromE164, textRaw, msgType, media, profileName, ts, mid };
  } catch {
    return null;
  }
}
