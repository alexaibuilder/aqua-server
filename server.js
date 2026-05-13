// ALEXPRO GAMES multiplayer server
// Supports: aqua, ziam, dungeons, yuambcraft (with persistent SMP world)
//
// PERSISTENCE: yuambcraft block changes are saved to Neon every 30 seconds.
// On startup, the server loads the saved world from Neon.
// Requires DATABASE_URL env var (same Neon connection string as your Vercel site).

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;

// ─── NEON ──
let neonSql = null;
if (DATABASE_URL) {
  try {
    const { neon } = require("@neondatabase/serverless");
    neonSql = neon(DATABASE_URL);
    console.log("[smp] Neon Postgres connected for persistence");
  } catch (e) {
    console.error("[smp] Could not load @neondatabase/serverless — run npm install");
  }
}

// ─── WORLD STATE ──
const worldState = {
  blockChanges: new Map(),
  lastSaveAt: Date.now(),
  dirty: false,
};

async function loadWorldFromDB() {
  if (!neonSql) return;
  try {
    await neonSql`
      CREATE TABLE IF NOT EXISTS yuambcraft_world (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        saved_at TIMESTAMP DEFAULT NOW()
      )
    `;
    const rows = await neonSql`SELECT data FROM yuambcraft_world ORDER BY id DESC LIMIT 1`;
    if (rows.length > 0 && rows[0].data && rows[0].data.blocks) {
      for (const [key, id] of Object.entries(rows[0].data.blocks)) {
        worldState.blockChanges.set(key, id);
      }
      console.log("[smp] Loaded " + worldState.blockChanges.size + " block changes from DB");
    }
  } catch (e) {
    console.error("[smp] Error loading world:", e.message);
  }
}

async function saveWorldToDB() {
  if (!neonSql) return;
  if (!worldState.dirty) return;
  try {
    const obj = {};
    for (const [k, v] of worldState.blockChanges) obj[k] = v;
    await neonSql`INSERT INTO yuambcraft_world (data) VALUES (${{ blocks: obj }})`;
    await neonSql`
      DELETE FROM yuambcraft_world
      WHERE id NOT IN (SELECT id FROM yuambcraft_world ORDER BY id DESC LIMIT 5)
    `;
    worldState.dirty = false;
    worldState.lastSaveAt = Date.now();
    console.log("[smp] Saved " + worldState.blockChanges.size + " block changes");
  } catch (e) {
    console.error("[smp] Error saving world:", e.message);
  }
}

setInterval(saveWorldToDB, 30000);
process.on("SIGTERM", async () => {
  console.log("[smp] SIGTERM — saving world");
  await saveWorldToDB();
  process.exit(0);
});
loadWorldFromDB();

// ─── ROOMS ──
const rooms = new Map();

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      rooms: rooms.size,
      connections: countConnections(),
      worldBlocks: worldState.blockChanges.size,
      lastSave: worldState.lastSaveAt,
    }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ALEXPRO GAMES multiplayer server is running.\n");
});

function countConnections() {
  let n = 0;
  for (const set of rooms.values()) n += set.size;
  return n;
}

const wss = new WebSocket.Server({ server });

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(msg)); } catch {}
  }
}

function broadcast(roomId, msg, exclude) {
  const set = rooms.get(roomId);
  if (!set) return;
  const data = JSON.stringify(msg);
  for (const peer of set) {
    if (peer === exclude) continue;
    if (peer.readyState === WebSocket.OPEN) {
      try { peer.send(data); } catch {}
    }
  }
}

function joinRoom(ws, roomId) {
  if (ws.roomId && rooms.has(ws.roomId)) {
    const set = rooms.get(ws.roomId);
    set.delete(ws);
    broadcast(ws.roomId, { type: "leave", userId: ws.userId }, ws);
    if (set.size === 0) rooms.delete(ws.roomId);
  }
  ws.roomId = roomId;
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  const peers = [];
  for (const peer of rooms.get(roomId)) {
    if (peer === ws) continue;
    peers.push({
      userId: peer.userId,
      username: peer.username,
      x: peer.x || 0, y: peer.y || 0, z: peer.z || 0,
      yaw: peer.yaw || 0, hp: peer.hp || 20,
      dimension: peer.dimension || "overworld",
      selectedItem: peer.selectedItem || 0,
    });
  }
  send(ws, { type: "roster", peers });

  // Send saved world state to yuambcraft players
  if (ws.gameKind === "yuambcraft") {
    const blocks = [];
    for (const [key, id] of worldState.blockChanges) {
      const parts = key.split(",");
      blocks.push({ x: +parts[0], y: +parts[1], z: +parts[2], id, dim: parts[3] });
    }
    send(ws, { type: "world_init", blocks });
    console.log("[smp] Sent " + blocks.length + " saved blocks to " + ws.username);
  }

  broadcast(roomId, {
    type: "join",
    userId: ws.userId,
    username: ws.username,
    x: ws.x || 0, y: ws.y || 0, z: ws.z || 0,
    yaw: ws.yaw || 0, hp: ws.hp || 20,
  }, ws);
}

function sanitizeChat(text) {
  if (typeof text !== "string") return null;
  const t = text.trim().slice(0, 200);
  return t.length > 0 ? t : null;
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== "string") return;

    switch (msg.type) {
      case "hello": {
        ws.userId = String(msg.userId || "guest-" + Math.random().toString(36).slice(2, 8));
        ws.username = String(msg.username || "Player").slice(0, 24);
        ws.gameKind = String(msg.gameKind || "aqua");
        ws.x = Number(msg.x) || 0;
        ws.y = Number(msg.y) || 30;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        ws.hp = Number(msg.hp) || 20;
        ws.dimension = msg.dimension || "overworld";

        let roomId;
        if (ws.gameKind === "yuambcraft") roomId = "yuambcraft:global";
        else if (ws.gameKind === "dungeons") roomId = "dungeons:" + (msg.dungeonRoom || "global");
        else if (ws.gameKind === "ziam") roomId = "ziam:global";
        else if (ws.gameKind === "aqua") roomId = "aqua:" + (msg.hostId || ws.userId);
        else roomId = ws.gameKind + ":global";
        joinRoom(ws, roomId);
        break;
      }

      case "pos": {
        if (!ws.roomId) return;
        ws.x = Number(msg.x) || 0;
        ws.y = Number(msg.y) || 0;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        if (typeof msg.hp === "number") ws.hp = msg.hp;
        if (typeof msg.selectedItem === "number") ws.selectedItem = msg.selectedItem;
        broadcast(ws.roomId, {
          type: "pos", userId: ws.userId,
          x: ws.x, y: ws.y, z: ws.z, yaw: ws.yaw,
          hp: ws.hp, selectedItem: ws.selectedItem,
        }, ws);
        break;
      }

      case "chat": {
        if (!ws.roomId) return;
        const text = sanitizeChat(msg.text);
        if (!text) return;
        broadcast(ws.roomId, {
          type: "chat", userId: ws.userId, username: ws.username,
          text, ts: Date.now(),
        }, null);
        break;
      }

      // YUAMBCRAFT with persistence
      case "block": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        const x = Number(msg.x) | 0;
        const y = Number(msg.y) | 0;
        const z = Number(msg.z) | 0;
        const id = Number(msg.id) | 0;
        const dim = msg.dimension || "overworld";
        const key = x + "," + y + "," + z + "," + dim;
        worldState.blockChanges.set(key, id);
        worldState.dirty = true;
        broadcast(ws.roomId, { type: "block", userId: ws.userId, x, y, z, id, dimension: dim }, ws);
        break;
      }

      case "swing": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, { type: "swing", userId: ws.userId }, ws);
        break;
      }

      case "dimension": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        ws.dimension = String(msg.dimension || "overworld");
        broadcast(ws.roomId, {
          type: "dimension", userId: ws.userId, dimension: ws.dimension,
        }, ws);
        break;
      }

      case "item_drop": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "item_drop",
          dropId: String(msg.dropId),
          itemId: Number(msg.itemId) | 0,
          count: Number(msg.count) | 0,
          x: Number(msg.x), y: Number(msg.y), z: Number(msg.z),
          dimension: msg.dimension || "overworld",
        }, ws);
        break;
      }

      case "item_pickup": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "item_pickup", dropId: String(msg.dropId), userId: ws.userId,
        }, null);
        break;
      }

      case "mob_spawn": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_spawn",
          mobId: String(msg.mobId),
          mobType: String(msg.mobType),
          x: Number(msg.x), y: Number(msg.y), z: Number(msg.z),
          dimension: msg.dimension || "overworld",
        }, ws);
        break;
      }

      case "mob_state": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_state", mobs: msg.mobs,
          dimension: msg.dimension || "overworld",
        }, ws);
        break;
      }

      case "mob_die": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, { type: "mob_die", mobId: String(msg.mobId) }, null);
        break;
      }

      case "mob_hit": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_hit", mobId: String(msg.mobId),
          damage: Number(msg.damage) || 1, attackerId: ws.userId,
        }, ws);
        break;
      }

      case "player_attack": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "player_attack", userId: ws.userId,
          targetId: String(msg.targetId),
          damage: Number(msg.damage) || 1,
        }, null);
        break;
      }

      case "host_claim": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, { type: "host_claim", userId: ws.userId }, ws);
        break;
      }

      case "ziam_respawn":
      case "ziam_style":
      case "enemy_hit":
      case "enemy_state":
      case "stage_advance": {
        if (!ws.roomId) return;
        broadcast(ws.roomId, Object.assign({}, msg, { userId: ws.userId }), ws);
        break;
      }

      case "ping": {
        send(ws, { type: "pong", ts: Date.now() });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (ws.roomId && rooms.has(ws.roomId)) {
      const set = rooms.get(ws.roomId);
      set.delete(ws);
      broadcast(ws.roomId, { type: "leave", userId: ws.userId }, ws);
      if (set.size === 0) rooms.delete(ws.roomId);
    }
  });

  ws.on("error", () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping(function () {});
  });
}, 30000);

server.listen(PORT, function () {
  console.log("ALEXPRO GAMES multiplayer server listening on :" + PORT);
});
