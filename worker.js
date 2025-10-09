/**
 * ============================================================
 * CopiBot-Lite R7.0 (2025-10)
 * CP Digital (México) — Ventas y Soporte técnico sin IA
 * ============================================================
 * Funciones:
 *  - Búsqueda de consumibles por modelo o texto
 *  - Carrito multi-artículo ( stock + backorder )
 *  - Pedidos con / sin factura ( cliente + pedido + pedido_item )
 *  - Flujo de soporte técnico (detector de falla → orden → Google Calendar )
 *  - Reagendar / cancelar visita 
 *  - Reenvío automático de audios a SUPPORT_WHATSAPP
 *  - Sesiones persistentes en Supabase ( TTL 90 días )
 *  - Sin dependencias IA u OpenAI
 * ============================================================
 */

export default {
  async fetch(req, env) {
    const url = new URL(req.url)
    if (req.method === "GET" && url.pathname === "/")
      return new Response(url.searchParams.get("hub.challenge") || "ok")
    if (url.pathname === "/health")
      return new Response(
        JSON.stringify({ ok: true, time: new Date().toISOString() }),
        { headers: { "Content-Type": "application/json" } }
      )

    if (req.method !== "POST") return new Response("ok")
    try {
      const body = await req.json()
      const entry = body.entry?.[0]
      const change = entry?.changes?.[0]?.value
      const msg = change?.messages?.[0]
      if (!msg) return ok("EVENT_RECEIVED")

      const from = msg.from
      const mid = msg.id
      const type = msg.type
      const text = type === "text" ? msg.text.body.trim() : ""
      const lowered = text.toLowerCase()

      // --- manejo de sesión ---
      const session = await getSession(env, from)
      if (session.data.last_mid === mid) return ok("DUPLICATE")
      session.data.last_mid = mid

      // --- manejar audio ---
      if (type === "audio") {
        await sendWhatsAppText(
          env,
          from,
          "Lo siento, aún no puedo escuchar audios, pero te comuniqué con soporte y en un momento se ponen en contacto contigo 🙂"
        )
        await forwardToSupport(env, msg)
        return ok("EVENT_RECEIVED")
      }

      // --- detectar intención soporte ---
      const supportKeywords = [
        "falla",
        "no imprime",
        "atasco",
        "error",
        "servicio",
        "soporte",
        "revisión",
        "no enciende",
      ]
      if (supportKeywords.some((k) => lowered.includes(k))) {
        session.stage = "sv_collect"
        session.data.sv = { ...session.data.sv, falla: text }
        await saveSession(env, session)
        await handleSupport(env, session, from)
        return ok("EVENT_RECEIVED")
      }

      // --- flujo de ventas ---
      if (await looksLikeProductQuery(env, lowered)) {
        session.stage = "ask_qty"
        session.data.last_candidate = await findProduct(env, lowered)
        await saveSession(env, session)
        await sendProductCard(env, from, session.data.last_candidate)
        return ok("EVENT_RECEIVED")
      }

      // --- pedir cantidad ---
      if (session.stage === "ask_qty" && looksLikeQuantity(lowered)) {
        const qty = parseQuantity(lowered)
        if (!session.data.last_candidate) {
          await sendWhatsAppText(
            env,
            from,
            "No alcancé a ver el artículo anterior. ¿Podrías repetirlo?"
          )
          return ok("EVENT_RECEIVED")
        }
        addToCart(session, qty)
        session.stage = "cart_open"
        await saveSession(env, session)
        await sendCartStatus(env, from, session)
        return ok("EVENT_RECEIVED")
      }

      // --- finalizar compra ---
      if (session.stage === "cart_open" && /finaliz|listo|eso|nada más/.test(lowered)) {
        session.stage = "await_invoice"
        await saveSession(env, session)
        await sendWhatsAppText(
          env,
          from,
          "Perfecto ✋ ¿La cotizamos con factura o sin factura?"
        )
        return ok("EVENT_RECEIVED")
      }

      // --- con o sin factura ---
      if (session.stage === "await_invoice") {
        const fact = /factur/i.test(lowered)
        await createOrder(env, from, session, fact)
        session.stage = "idle"
        session.data.cart = []
        await saveSession(env, session)
        await sendWhatsAppText(
          env,
          from,
          "✅ ¡Listo! Generé tu solicitud. Un asesor confirmará entrega y forma de pago."
        )
        return ok("EVENT_RECEIVED")
      }

      // --- fallback amistoso ---
      await sendWhatsAppText(
        env,
        from,
        "¡Hola! Soy CopiBot Lite de CP Digital. Puedo ayudarte con consumibles o agendar soporte técnico. 🙂"
      )
      await saveSession(env, session)
      return ok("EVENT_RECEIVED")
    } catch (err) {
      console.error(err)
      return ok("EVENT_RECEIVED")
    }
  },
}

/* ============================================================
   🔧 Utilidades principales
   ============================================================ */
async function ok(msg) {
  return new Response(msg)
}

async function getSession(env, from) {
  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/wa_session?select=*&&from=eq.${from}`, {
    headers: { apikey: env.SUPABASE_ANON_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE}` },
  })
  const data = await res.json()
  return data[0] || { from, stage: "idle", data: {} }
}

async function saveSession(env, session) {
  await fetch(`${env.SUPABASE_URL}/rest/v1/wa_session`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates",
    },
    body: JSON.stringify(session),
  })
}

/* ============================================================
   🧾 Ventas
   ============================================================ */
async function looksLikeProductQuery(env, text) {
  const q = encodeURIComponent(text)
  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/rpc/match_products_trgm?q=${q}&match_count=1`,
    { headers: { apikey: env.SUPABASE_ANON_KEY } }
  )
  const r = await res.json()
  return Array.isArray(r) && r.length > 0
}

async function findProduct(env, text) {
  const q = encodeURIComponent(text)
  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/rpc/match_products_trgm?q=${q}&match_count=1`,
    { headers: { apikey: env.SUPABASE_ANON_KEY } }
  )
  const r = await res.json()
  return r?.[0]
}

function looksLikeQuantity(t) {
  return /\b\d+\b|uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez/i.test(t)
}

function parseQuantity(t) {
  const map = {
    uno: 1,
    dos: 2,
    tres: 3,
    cuatro: 4,
    cinco: 5,
    seis: 6,
    siete: 7,
    ocho: 8,
    nueve: 9,
    diez: 10,
  }
  const num = parseInt(t.match(/\d+/)?.[0] || 0)
  return num || map[t.trim().toLowerCase()] || 1
}

function addToCart(session, qty) {
  const cand = session.data.last_candidate
  if (!cand) return
  session.data.cart = session.data.cart || []
  session.data.cart.push({ ...cand, qty })
}

async function sendCartStatus(env, to, session) {
  const items = session.data.cart.map(
    (c) => `• ${c.nombre} x${c.qty} — $${c.precio} + IVA`
  )
  await sendWhatsAppText(
    env,
    to,
    `Añadí 🛒\n${items.join("\n")}\n¿Deseas agregar algo más o finalizamos?`
  )
}

async function createOrder(env, to, session, factura) {
  const total = session.data.cart.reduce(
    (sum, c) => sum + (parseFloat(c.precio) || 0) * (c.qty || 1),
    0
  )
  const payload = {
    cliente_tel: to,
    factura,
    total,
    items: session.data.cart,
  }
  await fetch(`${env.SUPABASE_URL}/rest/v1/pedido`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  })
}

/* ============================================================
   🧰 Soporte técnico
   ============================================================ */
async function handleSupport(env, session, from) {
  const sv = session.data.sv
  const cliente = await getClient(env, from)
  const equipo = sv.equipo || "No especificado"
  const falla = sv.falla || "Sin descripción"

  const os_payload = {
    cliente_id: cliente?.id,
    equipo,
    falla,
    telefono: from,
  }

  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/orden_servicio`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(os_payload),
  })
  const data = await res.json()
  const osId = data?.[0]?.id || Date.now()

  await createCalendarEvent(env, cliente?.nombre || "Cliente", equipo, osId)
  await sendWhatsAppText(
    env,
    from,
    `✅ Generé tu orden de servicio #${osId}. Un técnico se pondrá en contacto contigo para agendar la visita.`
  )
}

async function createCalendarEvent(env, nombre, equipo, osId) {
  const payload = {
    summary: `${nombre} – ${equipo} – OS#${osId}`,
    description: "Visita técnica CP Digital",
    start: { dateTime: nextWorkSlot("10:00"), timeZone: env.TZ },
    end: { dateTime: nextWorkSlot("11:00"), timeZone: env.TZ },
  }
  await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/create_gcal_event`, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  })
}

function nextWorkSlot(hora) {
  const d = new Date()
  d.setHours(parseInt(hora.split(":")[0]), parseInt(hora.split(":")[1]), 0, 0)
  if (d.getHours() >= 15) d.setDate(d.getDate() + 1)
  return d.toISOString()
}

/* ============================================================
   📡 WhatsApp helpers
   ============================================================ */
async function sendWhatsAppText(env, to, text) {
  await fetch(
    `https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.WA_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        to,
        type: "text",
        text: { body: text },
      }),
    }
  )
}

async function sendProductCard(env, to, prod) {
  const text = `1️⃣ ${prod.nombre}\nSKU: ${prod.sku}\n$${prod.precio} + IVA\n${prod.stock} pzas\n¿Te funciona?`
  await sendWhatsAppText(env, to, text)
}

async function forwardToSupport(env, msg) {
  const payload = {
    messaging_product: "whatsapp",
    to: env.SUPPORT_WHATSAPP,
    type: msg.type,
    [msg.type]: msg[msg.type],
  }
  await fetch(`https://graph.facebook.com/v20.0/${env.PHONE_ID}/messages`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.WA_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  })
}

/* ============================================================
   🔍 Helpers de clientes
   ============================================================ */
async function getClient(env, tel) {
  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/cliente?telefono=eq.${tel}&select=*`,
    { headers: { apikey: env.SUPABASE_ANON_KEY } }
  )
  const data = await res.json()
  return data?.[0]
}
