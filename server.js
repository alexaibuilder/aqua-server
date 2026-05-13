// ALEXPRO GAMES multiplayer server
// Supports: aqua, ziam, dungeons, yuambcraft
// Hosts WebSocket rooms for various games. Each gameKind has its own room/relay rules.

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

// ─── STATE ──────────────────────────────────────────────────────────────
// rooms: Map<roomId, Set<WebSocket>>
const rooms = new Map();

// Per-socket metadata is set on the ws object:
//   ws.userId, ws.username, ws.roomId, ws.gameKind
//   ws.x, ws.y, ws.z, ws.yaw, ws.hp
//   Yuambcraft-only: ws.dimension, ws.selectedItem, ws.skin

// ─── HTTP ──
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size, connections: countConnections() }));
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

// ─── HELPERS ──
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
  // Leave previous room
  if (ws.roomId && rooms.has(ws.roomId)) {
    const set = rooms.get(ws.roomId);
    set.delete(ws);
    broadcast(ws.roomId, { type: "leave", userId: ws.userId }, ws);
    if (set.size === 0) rooms.delete(ws.roomId);
  }
  ws.roomId = roomId;
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);

  // Send roster of current peers to the new player
  const peers = [];
  for (const peer of rooms.get(roomId)) {
    if (peer === ws) continue;
    peers.push({
      userId: peer.userId,
      username: peer.username,
      x: peer.x ?? 0, y: peer.y ?? 0, z: peer.z ?? 0,
      yaw: peer.yaw ?? 0, hp: peer.hp ?? 20,
      dimension: peer.dimension || "overworld",
      selectedItem: peer.selectedItem || 0,
    });
  }
  send(ws, { type: "roster", peers });

  // Notify others
  broadcast(roomId, {
    type: "join",
    userId: ws.userId,
    username: ws.username,
    x: ws.x ?? 0, y: ws.y ?? 0, z: ws.z ?? 0,
    yaw: ws.yaw ?? 0, hp: ws.hp ?? 20,
  }, ws);
}

function sanitizeChat(text) {
  if (typeof text !== "string") return null;
  const t = text.trim().slice(0, 200);
  return t.length > 0 ? t : null;
}

// ─── WEBSOCKET HANDLER ──
wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.type !== "string") return;

    switch (msg.type) {
      case "hello": {
        // Initial handshake. Assigns userId/username and joins a room.
        ws.userId = String(msg.userId || "guest-" + Math.random().toString(36).slice(2, 8));
        ws.username = String(msg.username || "Player").slice(0, 24);
        ws.gameKind = String(msg.gameKind || "aqua");
        ws.x = Number(msg.x) || 0;
        ws.y = Number(msg.y) || 30;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        ws.hp = Number(msg.hp) || 20;
        ws.dimension = msg.dimension || "overworld";

        // Determine roomId based on gameKind
        let roomId;
        if (ws.gameKind === "yuambcraft") {
          // Yuambcraft has ONE global world for now
          roomId = "yuambcraft:global";
        } else if (ws.gameKind === "dungeons") {
          roomId = "dungeons:" + (msg.dungeonRoom || "global");
        } else if (ws.gameKind === "ziam") {
          roomId = "ziam:global";
        } else if (ws.gameKind === "aqua") {
          roomId = "aqua:" + (msg.hostId || ws.userId);
        } else {
          roomId = ws.gameKind + ":global";
        }
        joinRoom(ws, roomId);
        break;
      }

      case "pos": {
        // Position update — applies to any game
        if (!ws.roomId) return;
        ws.x = Number(msg.x) || 0;
        ws.y = Number(msg.y) || 0;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        if (typeof msg.hp === "number") ws.hp = msg.hp;
        if (typeof msg.selectedItem === "number") ws.selectedItem = msg.selectedItem;
        broadcast(ws.roomId, {
          type: "pos",
          userId: ws.userId,
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
          type: "chat",
          userId: ws.userId,
          username: ws.username,
          text,
          ts: Date.now(),
        }, null);
        break;
      }

      // ─── YUAMBCRAFT-SPECIFIC ──

      case "block": {
        // Player placed or broke a block — broadcast to everyone
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "block",
          userId: ws.userId,
          x: Number(msg.x) | 0,
          y: Number(msg.y) | 0,
          z: Number(msg.z) | 0,
          id: Number(msg.id) | 0,
          dimension: msg.dimension || "overworld",
        }, ws);
        break;
      }

      case "swing": {
        // Player swung their hand/tool — purely visual broadcast
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, { type: "swing", userId: ws.userId }, ws);
        break;
      }

      case "dimension": {
        // Player switched dimension (overworld/nether)
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        ws.dimension = String(msg.dimension || "overworld");
        broadcast(ws.roomId, {
          type: "dimension",
          userId: ws.userId,
          dimension: ws.dimension,
        }, ws);
        break;
      }

      case "item_drop": {
        // A dropped item appeared on the ground
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
        // Someone picked up a dropped item — remove for everyone
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "item_pickup",
          dropId: String(msg.dropId),
          userId: ws.userId,
        }, null);
        break;
      }

      case "mob_spawn": {
        // Host-only: a new mob exists
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
        // Host broadcasts mob positions
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_state",
          mobs: msg.mobs,
          dimension: msg.dimension || "overworld",
        }, ws);
        break;
      }

      case "mob_die": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_die",
          mobId: String(msg.mobId),
        }, null);
        break;
      }

      case "mob_hit": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, {
          type: "mob_hit",
          mobId: String(msg.mobId),
          damage: Number(msg.damage) || 1,
          attackerId: ws.userId,
        }, ws);
        break;
      }

      case "host_claim": {
        // First connected client claims host. Server doesn't enforce —
        // clients sort by userId and the lowest is host.
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        broadcast(ws.roomId, { type: "host_claim", userId: ws.userId }, ws);
        break;
      }

      // ─── EXISTING GAMES (kept for backward compat) ──
      case "ziam_respawn":
      case "ziam_style":
      case "enemy_hit":
      case "enemy_state":
      case "stage_advance": {
        if (!ws.roomId) return;
        broadcast(ws.roomId, { ...msg, userId: ws.userId }, ws);
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

  ws.on("error", () => { /* close handles it */ });
});

// Heartbeat: drop dead connections
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping(() => {});
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`ALEXPRO GAMES multiplayer server listening on :${PORT}`);
});
