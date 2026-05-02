// Aqua Tycoon WebSocket multiplayer server
// Runs on Railway. Players connect via WebSocket, server broadcasts position
// updates and chat messages to everyone on the same island.

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

// ─── STATE ──────────────────────────────────────────────────────────────
// rooms: Map<hostUserId, Set<WebSocket>>
//   Each "room" is a player's island. Anyone visiting that island is in
//   the room, plus the host themselves.
const rooms = new Map();

// Per-socket metadata
//   ws.userId      — who they are
//   ws.username    — display name
//   ws.hostId      — which island they're on (their own or someone else's)
//   ws.x, ws.z, ws.yaw — last known position
//   ws.hp          — visitor HP (for fence damage)

// ─── HTTP SERVER (just so Railway has something to health-check) ──────────
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", rooms: rooms.size, connections: countConnections() }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Aqua Tycoon multiplayer server is running.\n");
});

function countConnections() {
  let n = 0;
  for (const set of rooms.values()) n += set.size;
  return n;
}

// ─── WEBSOCKET ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

function joinRoom(ws, hostId) {
  // Leave previous room if any
  if (ws.hostId && rooms.has(ws.hostId)) {
    const set = rooms.get(ws.hostId);
    set.delete(ws);
    // Tell the room the player left
    broadcast(ws.hostId, { type: "leave", userId: ws.userId }, ws);
    if (set.size === 0) rooms.delete(ws.hostId);
  }
  ws.hostId = hostId;
  if (!rooms.has(hostId)) rooms.set(hostId, new Set());
  rooms.get(hostId).add(ws);
  // Send the new player a roster of who's already here
  const peers = [];
  for (const peer of rooms.get(hostId)) {
    if (peer === ws) continue;
    peers.push({
      userId: peer.userId,
      username: peer.username,
      x: peer.x ?? 0, z: peer.z ?? 0, yaw: peer.yaw ?? 0,
      hp: peer.hp ?? 100,
    });
  }
  send(ws, { type: "roster", peers });
  // Tell others someone joined
  broadcast(hostId, {
    type: "join",
    userId: ws.userId,
    username: ws.username,
    x: ws.x ?? 0, z: ws.z ?? 0, yaw: ws.yaw ?? 0, hp: ws.hp ?? 100,
  }, ws);
}

function broadcast(hostId, msg, exclude) {
  const set = rooms.get(hostId);
  if (!set) return;
  const data = JSON.stringify(msg);
  for (const peer of set) {
    if (peer === exclude) continue;
    if (peer.readyState === WebSocket.OPEN) peer.send(data);
  }
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

// Light text sanitization for chat — block obvious slurs, cap length
function sanitizeChat(text) {
  if (typeof text !== "string") return null;
  let t = text.trim().slice(0, 200);
  if (!t) return null;
  const banned = ["nigger", "faggot", "retard", "kys"];
  const lower = t.toLowerCase();
  for (const b of banned) if (lower.includes(b)) return null;
  return t;
}

wss.on("connection", (ws) => {
  ws.userId = null;
  ws.hostId = null;
  ws.x = 0; ws.z = 0; ws.yaw = 0; ws.hp = 100;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    switch (msg.type) {
      case "hello": {
        // First message: { type: "hello", userId, username, hostId }
        ws.userId = String(msg.userId || "").slice(0, 100);
        ws.username = String(msg.username || "Anonymous").slice(0, 30);
        if (!ws.userId) { ws.close(); return; }
        const hostId = String(msg.hostId || ws.userId).slice(0, 100);
        joinRoom(ws, hostId);
        break;
      }
      case "switch_room": {
        // Player started visiting a different island (or returned home)
        const hostId = String(msg.hostId || ws.userId || "").slice(0, 100);
        if (!hostId || !ws.userId) return;
        joinRoom(ws, hostId);
        break;
      }
      case "pos": {
        // Position update — broadcast to everyone else in the same room
        if (!ws.hostId || !ws.userId) return;
        ws.x = Number(msg.x) || 0;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        ws.hp = typeof msg.hp === "number" ? msg.hp : ws.hp;
        broadcast(ws.hostId, {
          type: "pos",
          userId: ws.userId,
          x: ws.x, z: ws.z, yaw: ws.yaw, hp: ws.hp,
        }, ws);
        break;
      }
      case "chat": {
        if (!ws.hostId || !ws.userId) return;
        const text = sanitizeChat(msg.text);
        if (!text) return;
        // Echo to the entire room INCLUDING the sender, so everyone's UIs match.
        broadcast(ws.hostId, {
          type: "chat",
          userId: ws.userId,
          username: ws.username,
          text,
          ts: Date.now(),
        }, null);
        break;
      }
      case "weather": {
        // Host broadcasts their weather to everyone visiting them.
        // We trust the host (they're the one whose room this is).
        if (!ws.hostId || !ws.userId) return;
        // Only forward weather if the sender IS the host of the room
        if (ws.userId !== ws.hostId) return;
        const w = msg.weather;
        if (w !== "clear" && w !== "cloudy" && w !== "rain" && w !== "storm" && w !== "fog") return;
        broadcast(ws.hostId, { type: "weather", weather: w }, ws);
        break;
      }
      case "ping": {
        send(ws, { type: "pong", ts: Date.now() });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (ws.hostId && rooms.has(ws.hostId)) {
      const set = rooms.get(ws.hostId);
      set.delete(ws);
      broadcast(ws.hostId, { type: "leave", userId: ws.userId }, ws);
      if (set.size === 0) rooms.delete(ws.hostId);
    }
  });

  ws.on("error", () => {
    // Just let close handle it
  });
});

server.listen(PORT, () => {
  console.log(`Aqua Tycoon multiplayer server listening on :${PORT}`);
});
