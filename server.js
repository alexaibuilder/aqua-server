// ALEXPRO GAMES multiplayer server
// Hosts WebSocket rooms for both Aqua Tycoon (island visiting + chat) and
// Ziam Battlegrounds (real-time PvP combat with abilities).

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const rooms = new Map();

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
    const p = {
      userId: peer.userId,
      username: peer.username,
      x: peer.x ?? 0, z: peer.z ?? 0, yaw: peer.yaw ?? 0,
      hp: peer.hp ?? 100,
    };
    if (ws.gameKind === "ziam") {
      p.style = peer.style || "fire";
      p.kills = peer.kills || 0;
      p.deaths = peer.deaths || 0;
    }
    peers.push(p);
  }
  send(ws, { type: "roster", peers });
  const joinMsg = {
    type: "join",
    userId: ws.userId,
    username: ws.username,
    x: ws.x ?? 0, z: ws.z ?? 0, yaw: ws.yaw ?? 0, hp: ws.hp ?? 100,
  };
  if (ws.gameKind === "ziam") {
    joinMsg.style = ws.style || "fire";
    joinMsg.kills = ws.kills || 0;
    joinMsg.deaths = ws.deaths || 0;
  }
  broadcast(roomId, joinMsg, ws);
}

function sanitizeChat(text) {
  if (typeof text !== "string") return null;
  let t = text.trim().slice(0, 200);
  if (!t) return null;
  const banned = ["nigger", "faggot", "retard", "kys"];
  const lower = t.toLowerCase();
  for (const b of banned) if (lower.includes(b)) return null;
  return t;
}

const VALID_ZIAM_STYLES = new Set(["fire", "ice", "light", "shadow", "earth", "ziamgod", "yuamb", "water", "gardenbloom", "blood", "wind"]);

wss.on("connection", (ws) => {
  ws.userId = null;
  ws.roomId = null;
  ws.x = 0; ws.z = 0; ws.yaw = 0; ws.hp = 100;
  ws.gameKind = "aqua";

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    switch (msg.type) {
      case "hello": {
        ws.userId = String(msg.userId || "").slice(0, 100);
        ws.username = String(msg.username || "Anonymous").slice(0, 30);
        if (!ws.userId) { ws.close(); return; }
        ws.gameKind = msg.gameKind === "ziam" ? "ziam" : "aqua";
        if (ws.gameKind === "ziam") {
          ws.style = VALID_ZIAM_STYLES.has(msg.style) ? msg.style : "fire";
          ws.kills = 0;
          ws.deaths = 0;
          ws.hp = 100;
          joinRoom(ws, "ziam:global");
        } else {
          const hostId = String(msg.hostId || ws.userId).slice(0, 100);
          joinRoom(ws, "aqua:" + hostId);
        }
        break;
      }
      case "switch_room": {
        if (ws.gameKind !== "aqua") return;
        const hostId = String(msg.hostId || ws.userId || "").slice(0, 100);
        if (!hostId || !ws.userId) return;
        joinRoom(ws, "aqua:" + hostId);
        break;
      }
      case "pos": {
        if (!ws.roomId || !ws.userId) return;
        ws.x = Number(msg.x) || 0;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        if (typeof msg.hp === "number") ws.hp = msg.hp;
        broadcast(ws.roomId, {
          type: "pos",
          userId: ws.userId,
          x: ws.x, z: ws.z, yaw: ws.yaw, hp: ws.hp,
          y: typeof msg.y === "number" ? msg.y : 0,
          anim: msg.anim || null,
        }, ws);
        break;
      }
      case "chat": {
        if (!ws.roomId || !ws.userId) return;
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
      case "weather": {
        if (ws.gameKind !== "aqua") return;
        if (!ws.roomId || !ws.userId) return;
        if (ws.roomId !== "aqua:" + ws.userId) return;
        const w = msg.weather;
        if (w !== "clear" && w !== "cloudy" && w !== "rain" && w !== "storm" && w !== "fog") return;
        broadcast(ws.roomId, { type: "weather", weather: w }, ws);
        break;
      }
      // ─── ZIAM BATTLEGROUNDS ──
      case "ziam_attack": {
        if (ws.gameKind !== "ziam" || !ws.roomId) return;
        const attackId = String(msg.attackId || "").slice(0, 40);
        if (!attackId) return;
        broadcast(ws.roomId, {
          type: "ziam_attack",
          userId: ws.userId,
          attackId,
          ts: Date.now(),
        }, ws);
        break;
      }
      case "ziam_hit": {
        if (ws.gameKind !== "ziam" || !ws.roomId) return;
        const targetId = String(msg.targetId || "").slice(0, 100);
        const damage = Math.min(150, Math.max(0, Number(msg.damage) || 0));
        const knockback = msg.knockback || { x: 0, z: 0, y: 0 };
        const attackId = String(msg.attackId || "punch").slice(0, 40);
        if (!targetId || damage <= 0) return;
        const set = rooms.get(ws.roomId);
        if (!set) return;
        for (const peer of set) {
          if (peer.userId === targetId) {
            send(peer, {
              type: "ziam_take_hit",
              fromUserId: ws.userId,
              fromUsername: ws.username,
              damage,
              knockback,
              attackId,
              ts: Date.now(),
            });
            break;
          }
        }
        break;
      }
      case "ziam_death": {
        if (ws.gameKind !== "ziam" || !ws.roomId) return;
        const killerId = String(msg.killerId || "").slice(0, 100);
        const killAttackId = String(msg.attackId || "punch").slice(0, 40);
        ws.deaths = (ws.deaths || 0) + 1;
        const set = rooms.get(ws.roomId);
        let killerName = "Anonymous";
        if (set && killerId) {
          for (const peer of set) {
            if (peer.userId === killerId) {
              peer.kills = (peer.kills || 0) + 1;
              killerName = peer.username || "Anonymous";
              break;
            }
          }
        }
        broadcast(ws.roomId, {
          type: "ziam_killfeed",
          killerId, killerName,
          victimId: ws.userId, victimName: ws.username,
          attackId: killAttackId,
          ts: Date.now(),
        }, null);
        broadcast(ws.roomId, {
          type: "ziam_stats",
          userId: ws.userId,
          kills: ws.kills || 0,
          deaths: ws.deaths || 0,
        }, null);
        if (killerId && set) {
          for (const peer of set) {
            if (peer.userId === killerId) {
              broadcast(ws.roomId, {
                type: "ziam_stats",
                userId: peer.userId,
                kills: peer.kills || 0,
                deaths: peer.deaths || 0,
              }, null);
              break;
            }
          }
        }
        break;
      }
      case "ziam_respawn": {
        if (ws.gameKind !== "ziam" || !ws.roomId) return;
        ws.hp = 100;
        ws.x = Number(msg.x) || 0;
        ws.z = Number(msg.z) || 0;
        ws.yaw = Number(msg.yaw) || 0;
        broadcast(ws.roomId, {
          type: "ziam_respawn",
          userId: ws.userId,
          x: ws.x, z: ws.z, yaw: ws.yaw,
        }, null);
        break;
      }
      case "ziam_style": {
        if (ws.gameKind !== "ziam" || !ws.roomId) return;
        const style = VALID_ZIAM_STYLES.has(msg.style) ? msg.style : null;
        if (!style) return;
        ws.style = style;
        broadcast(ws.roomId, {
          type: "ziam_style",
          userId: ws.userId,
          style,
        }, null);
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

  ws.on("error", () => { });
});

server.listen(PORT, () => {
  console.log(`ALEXPRO GAMES multiplayer server listening on :${PORT}`);
});
