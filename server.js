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

// ─── PVP RANKED 1v1 ──
const pvpQueue = [];              // [{ ws, userId, username, rank, joinedAt }]
const activeMatches = new Map();  // matchId -> { aWs, bWs, aId, bId, aHp, bHp, startTime, aName, bName }

// ─── PER-PLAYER SAVE (in-memory, wiped on Railway restart) ──
// Keyed by userId. Stores last known position + inventory + vitals.
const playerData = new Map();

// ─── SMP FEATURES STATE ──
// Leaderboard stats: userId -> { username, kills, blocks, golems, deaths }
const leaderboard = new Map();
// Friends graph: userId -> Set<userId>
const friends = new Map();
// Pending TP requests: userId -> { fromId, fromName, ts }
const tpRequests = new Map();
// Daily challenges - resets every 24h
const DAILY_CHALLENGES_POOL = [
  { id: 'mine_50_stone', name: 'Mine 50 stone blocks', goal: 50, type: 'block_mine', blockId: 3, reward: '⭐ 10 emeralds' },
  { id: 'kill_5_zombies', name: 'Kill 5 zombies', goal: 5, type: 'mob_kill', mobType: 'zombie', reward: '⭐ Iron sword' },
  { id: 'kill_3_skeletons', name: 'Defeat 3 skeletons', goal: 3, type: 'mob_kill', mobType: 'skeleton', reward: '⭐ 16 arrows' },
  { id: 'defeat_iron_golem', name: 'Defeat an Iron Golem', goal: 1, type: 'mob_kill', mobType: 'iron_golem', reward: '⭐ 3 iron' },
  { id: 'defeat_diamond_golem', name: 'Defeat a Diamond Golem', goal: 1, type: 'mob_kill', mobType: 'diamond_golem', reward: '⭐ 2 diamond' },
  { id: 'place_100_blocks', name: 'Place 100 blocks', goal: 100, type: 'block_place', reward: '⭐ 16 cobble' },
  { id: 'mine_diamond', name: 'Mine a diamond ore', goal: 1, type: 'block_mine', blockId: 13, reward: '⭐ Hero Sword' },
];
let dailyChallenges = [];
let dailyResetAt = 0;
// Player challenge progress: userId -> { challengeId: progress, completed: Set<challengeId> }
const playerChallenges = new Map();
function pickDailyChallenges() {
  const shuffled = [...DAILY_CHALLENGES_POOL].sort(() => Math.random() - 0.5);
  dailyChallenges = shuffled.slice(0, 3);
  dailyResetAt = Date.now() + 24 * 60 * 60 * 1000;
  playerChallenges.clear();
  console.log("[smp] New daily challenges:", dailyChallenges.map(c => c.name).join(", "));
}
pickDailyChallenges();
setInterval(() => {
  if (Date.now() > dailyResetAt) pickDailyChallenges();
}, 60 * 1000);

function getLeaderboardSnap() {
  const arr = [...leaderboard.values()];
  arr.sort((a, b) => (b.kills + b.golems * 3 + Math.floor(b.blocks / 100)) - (a.kills + a.golems * 3 + Math.floor(a.blocks / 100)));
  return arr.slice(0, 10);
}

function broadcastQueueSize() {
  const size = pvpQueue.length;
  for (const entry of pvpQueue) {
    if (entry.ws.readyState === WebSocket.OPEN) {
      try { entry.ws.send(JSON.stringify({ type: "pvp_queue_size", size })); } catch {}
    }
  }
}

function tryMakeMatch() {
  while (pvpQueue.length >= 2) {
    const a = pvpQueue.shift();
    const b = pvpQueue.shift();
    if (!a || !b) continue;
    if (a.ws.readyState !== WebSocket.OPEN || b.ws.readyState !== WebSocket.OPEN) continue;
    if (a.userId === b.userId) { pvpQueue.unshift(b); continue; }
    const matchId = "m_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8);
    activeMatches.set(matchId, {
      aWs: a.ws, bWs: b.ws,
      aId: a.userId, bId: b.userId,
      aName: a.username, bName: b.username,
      aHp: 10, bHp: 10,
      startTime: Date.now(),
    });
    a.ws.matchId = matchId;
    b.ws.matchId = matchId;
    try {
      a.ws.send(JSON.stringify({
        type: "pvp_match_found", matchId, role: "a",
        opponentId: b.userId, opponentName: b.username, opponentRank: b.rank,
      }));
      b.ws.send(JSON.stringify({
        type: "pvp_match_found", matchId, role: "b",
        opponentId: a.userId, opponentName: a.username, opponentRank: a.rank,
      }));
      console.log("[pvp] match " + matchId + ": " + a.username + " vs " + b.username);
    } catch {}
  }
  broadcastQueueSize();
}

function endMatch(matchId, winnerRole, reason) {
  const m = activeMatches.get(matchId);
  if (!m) return;
  activeMatches.delete(matchId);
  if (m.aWs) m.aWs.matchId = null;
  if (m.bWs) m.bWs.matchId = null;
  const winnerWs = winnerRole === "a" ? m.aWs : winnerRole === "b" ? m.bWs : null;
  const loserWs  = winnerRole === "a" ? m.bWs : winnerRole === "b" ? m.aWs : null;
  try { if (winnerWs && winnerWs.readyState === WebSocket.OPEN) winnerWs.send(JSON.stringify({ type: "pvp_match_end", result: "won", reason })); } catch {}
  try { if (loserWs  && loserWs.readyState  === WebSocket.OPEN) loserWs.send(JSON.stringify({ type: "pvp_match_end", result: "lost", reason })); } catch {}
  if (!winnerWs) {
    try { if (m.aWs && m.aWs.readyState === WebSocket.OPEN) m.aWs.send(JSON.stringify({ type: "pvp_match_end", result: "draw", reason })); } catch {}
    try { if (m.bWs && m.bWs.readyState === WebSocket.OPEN) m.bWs.send(JSON.stringify({ type: "pvp_match_end", result: "draw", reason })); } catch {}
  }
}

// Match timeout sweeper (5-minute cap)
setInterval(() => {
  const now = Date.now();
  for (const [matchId, m] of activeMatches) {
    if (now - m.startTime > 5 * 60 * 1000) {
      let winnerRole = null;
      if (m.aHp > m.bHp) winnerRole = "a";
      else if (m.bHp > m.aHp) winnerRole = "b";
      endMatch(matchId, winnerRole, "timeout");
    }
  }
}, 30000);

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      status: "ok",
      rooms: rooms.size,
      connections: countConnections(),
      worldBlocks: worldState.blockChanges.size,
      lastSave: worldState.lastSaveAt,
      pvpQueue: pvpQueue.length,
      pvpMatches: activeMatches.size,
      savedPlayers: playerData.size,
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
  ws.matchId = null;
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

        // YUAMBCRAFT: restore saved player state if we have it
        if (ws.gameKind === "yuambcraft" && playerData.has(ws.userId)) {
          const saved = playerData.get(ws.userId);
          ws.x = saved.x;
          ws.y = saved.y;
          ws.z = saved.z;
          ws.yaw = saved.yaw;
          ws.hp = saved.hp;
          ws.dimension = saved.dimension || "overworld";
          // Tell the client what we have
          send(ws, {
            type: "player_restore",
            x: saved.x, y: saved.y, z: saved.z,
            yaw: saved.yaw, hp: saved.hp, hunger: saved.hunger,
            dimension: saved.dimension,
            hotbar: saved.hotbar,
            storage: saved.storage,
          });
          console.log("[smp] Restored player " + ws.username + " @ " + saved.x.toFixed(1) + "," + saved.y.toFixed(1) + "," + saved.z.toFixed(1));
        }

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
        // YUAMBCRAFT: also update player's persistent slot with latest position/hp
        if (ws.gameKind === "yuambcraft" && ws.userId) {
          const prev = playerData.get(ws.userId) || {};
          playerData.set(ws.userId, {
            ...prev,
            x: ws.x, y: ws.y, z: ws.z, yaw: ws.yaw, hp: ws.hp,
            dimension: ws.dimension || prev.dimension || "overworld",
            savedAt: Date.now(),
          });
        }
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

      // YUAMBCRAFT: per-player save (in-memory)
      case "player_save": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        playerData.set(ws.userId, {
          x: Number(msg.x) || ws.x || 0,
          y: Number(msg.y) || ws.y || 30,
          z: Number(msg.z) || ws.z || 0,
          yaw: Number(msg.yaw) || 0,
          hp: typeof msg.hp === "number" ? msg.hp : 20,
          hunger: typeof msg.hunger === "number" ? msg.hunger : 20,
          dimension: msg.dimension || "overworld",
          hotbar: Array.isArray(msg.hotbar) ? msg.hotbar : null,
          storage: Array.isArray(msg.storage) ? msg.storage : null,
          savedAt: Date.now(),
        });
        break;
      }

      // ============ SMP FEATURES ============
      // 👥 PLAYER LIST: respond with snapshot of all connected players
      case "player_list_request": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        const peers = [];
        const set = rooms.get(ws.roomId);
        if (set) {
          for (const peer of set) {
            const isFriend = friends.has(ws.userId) && friends.get(ws.userId).has(peer.userId);
            peers.push({
              userId: peer.userId,
              username: peer.username,
              hp: peer.hp || 20,
              x: peer.x || 0, y: peer.y || 0, z: peer.z || 0,
              friend: isFriend,
              isMe: peer.userId === ws.userId,
            });
          }
        }
        send(ws, { type: "player_list", players: peers });
        break;
      }

      // 🏠 SETHOME / HOME: store a home location per player
      case "sethome": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const prev = playerData.get(ws.userId) || {};
        playerData.set(ws.userId, { ...prev, home: { x: ws.x, y: ws.y, z: ws.z, dimension: ws.dimension || "overworld" } });
        send(ws, { type: "system_message", text: "🏠 Home set at " + Math.floor(ws.x) + ", " + Math.floor(ws.y) + ", " + Math.floor(ws.z) });
        break;
      }
      case "gohome": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const data = playerData.get(ws.userId);
        if (!data || !data.home) {
          send(ws, { type: "system_message", text: "⚠️ No home set. Use /sethome to set one." });
          return;
        }
        send(ws, { type: "player_teleport", x: data.home.x, y: data.home.y + 1, z: data.home.z, reason: "🏠 Teleported home" });
        break;
      }

      // 📜 LEADERBOARD: server tracks + broadcasts
      case "leaderboard_request": {
        if (ws.gameKind !== "yuambcraft") return;
        send(ws, { type: "leaderboard", entries: getLeaderboardSnap() });
        break;
      }
      case "stat_report": {
        // Client reports a stat event (kill, block, etc)
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        if (!leaderboard.has(ws.userId)) {
          leaderboard.set(ws.userId, { userId: ws.userId, username: ws.username, kills: 0, blocks: 0, golems: 0, deaths: 0 });
        }
        const stat = leaderboard.get(ws.userId);
        stat.username = ws.username; // keep updated
        if (msg.event === "kill") stat.kills++;
        else if (msg.event === "block") stat.blocks++;
        else if (msg.event === "golem_kill") stat.golems++;
        else if (msg.event === "death") stat.deaths++;

        // Daily challenge tracking
        if (!playerChallenges.has(ws.userId)) {
          playerChallenges.set(ws.userId, { progress: {}, completed: new Set() });
        }
        const pc = playerChallenges.get(ws.userId);
        for (const ch of dailyChallenges) {
          if (pc.completed.has(ch.id)) continue;
          let bump = 0;
          if (ch.type === "mob_kill" && msg.event === "kill" && msg.mobType === ch.mobType) bump = 1;
          else if (ch.type === "mob_kill" && msg.event === "golem_kill" && msg.mobType === ch.mobType) bump = 1;
          else if (ch.type === "block_mine" && msg.event === "block" && msg.action === "mine" && msg.blockId === ch.blockId) bump = 1;
          else if (ch.type === "block_place" && msg.event === "block" && msg.action === "place") bump = 1;
          if (bump) {
            pc.progress[ch.id] = (pc.progress[ch.id] || 0) + bump;
            if (pc.progress[ch.id] >= ch.goal) {
              pc.completed.add(ch.id);
              send(ws, { type: "challenge_complete", challenge: ch });
              broadcast(ws.roomId, { type: "system_message", text: "🏆 " + ws.username + " completed: " + ch.name }, null);
            } else {
              send(ws, { type: "challenge_progress", challengeId: ch.id, progress: pc.progress[ch.id], goal: ch.goal });
            }
          }
        }
        break;
      }

      // 🏆 DAILY CHALLENGES
      case "challenges_request": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const pc = playerChallenges.get(ws.userId) || { progress: {}, completed: new Set() };
        send(ws, {
          type: "challenges",
          challenges: dailyChallenges.map(ch => ({
            ...ch,
            progress: pc.progress[ch.id] || 0,
            completed: pc.completed.has(ch.id),
          })),
          resetsIn: Math.max(0, dailyResetAt - Date.now()),
        });
        break;
      }

      // 🎯 TP REQUESTS
      case "tp_request": {
        if (ws.gameKind !== "yuambcraft" || !ws.roomId) return;
        const targetName = String(msg.target || "").toLowerCase();
        if (!targetName) return;
        // Find target ws
        const set = rooms.get(ws.roomId);
        let target = null;
        if (set) {
          for (const peer of set) {
            if (peer.username.toLowerCase() === targetName) { target = peer; break; }
          }
        }
        if (!target) {
          send(ws, { type: "system_message", text: "⚠️ Player '" + msg.target + "' not found." });
          return;
        }
        if (target === ws) {
          send(ws, { type: "system_message", text: "⚠️ Can't teleport to yourself." });
          return;
        }
        tpRequests.set(target.userId, { fromId: ws.userId, fromName: ws.username, ts: Date.now() });
        send(target, { type: "tp_incoming", fromName: ws.username, fromId: ws.userId });
        send(ws, { type: "system_message", text: "🎯 TP request sent to " + target.username });
        break;
      }
      case "tp_accept": {
        if (ws.gameKind !== "yuambcraft") return;
        const req = tpRequests.get(ws.userId);
        if (!req || (Date.now() - req.ts) > 30000) {
          send(ws, { type: "system_message", text: "⚠️ No pending TP request." });
          return;
        }
        tpRequests.delete(ws.userId);
        // Tell the requester to teleport to us
        const set = rooms.get(ws.roomId);
        if (set) {
          for (const peer of set) {
            if (peer.userId === req.fromId) {
              send(peer, { type: "player_teleport", x: ws.x, y: ws.y + 1, z: ws.z, reason: "🎯 Teleported to " + ws.username });
              send(ws, { type: "system_message", text: "✅ " + req.fromName + " teleported to you." });
              break;
            }
          }
        }
        break;
      }
      case "tp_deny": {
        if (ws.gameKind !== "yuambcraft") return;
        const req = tpRequests.get(ws.userId);
        if (!req) return;
        tpRequests.delete(ws.userId);
        const set = rooms.get(ws.roomId);
        if (set) {
          for (const peer of set) {
            if (peer.userId === req.fromId) {
              send(peer, { type: "system_message", text: "❌ " + ws.username + " denied your TP request." });
              break;
            }
          }
        }
        break;
      }

      // 💍 FRIENDS
      case "friend_add": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const targetName = String(msg.target || "").toLowerCase();
        // Find user by username (search all rooms + saved)
        let targetId = null, targetUsername = null;
        const set = rooms.get(ws.roomId);
        if (set) {
          for (const peer of set) {
            if (peer.username.toLowerCase() === targetName) { targetId = peer.userId; targetUsername = peer.username; break; }
          }
        }
        if (!targetId) {
          send(ws, { type: "system_message", text: "⚠️ Player '" + msg.target + "' must be online to add as friend." });
          return;
        }
        if (!friends.has(ws.userId)) friends.set(ws.userId, new Set());
        friends.get(ws.userId).add(targetId);
        send(ws, { type: "system_message", text: "💍 Added " + targetUsername + " as friend." });
        break;
      }
      case "friend_remove": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const targetName = String(msg.target || "").toLowerCase();
        const set = rooms.get(ws.roomId);
        let removed = false;
        if (friends.has(ws.userId)) {
          const friendSet = friends.get(ws.userId);
          // Find by username (need to look up id)
          for (const fid of [...friendSet]) {
            // Find their last-known username
            if (set) {
              for (const peer of set) {
                if (peer.userId === fid && peer.username.toLowerCase() === targetName) {
                  friendSet.delete(fid); removed = true; break;
                }
              }
            }
            if (removed) break;
          }
        }
        send(ws, { type: "system_message", text: removed ? "❌ Removed friend." : "⚠️ Friend not found." });
        break;
      }
      case "friends_request": {
        if (ws.gameKind !== "yuambcraft" || !ws.userId) return;
        const friendIds = friends.has(ws.userId) ? [...friends.get(ws.userId)] : [];
        const list = [];
        const set = rooms.get(ws.roomId);
        for (const fid of friendIds) {
          let online = false, name = "(unknown)";
          if (set) {
            for (const peer of set) {
              if (peer.userId === fid) { online = true; name = peer.username; break; }
            }
          }
          list.push({ userId: fid, username: name, online });
        }
        send(ws, { type: "friends_list", friends: list });
        break;
      }


      // ============ PVP RANKED 1v1 ============
      case "pvp_queue": {
        if (ws.gameKind !== "yuambcraft") return;
        for (let i = pvpQueue.length - 1; i >= 0; i--) {
          if (pvpQueue[i].userId === ws.userId) pvpQueue.splice(i, 1);
        }
        if (ws.matchId) return; // already in a match
        pvpQueue.push({
          ws,
          userId: ws.userId,
          username: msg.username || ws.username || "Player",
          rank: msg.rank || { tier: 0, division: 0, stars: 0 },
          joinedAt: Date.now(),
        });
        console.log("[pvp] " + ws.username + " joined queue (" + pvpQueue.length + ")");
        tryMakeMatch();
        break;
      }

      case "pvp_leave_queue": {
        for (let i = pvpQueue.length - 1; i >= 0; i--) {
          if (pvpQueue[i].userId === ws.userId) pvpQueue.splice(i, 1);
        }
        broadcastQueueSize();
        break;
      }

      case "pvp_pos": {
        const m = activeMatches.get(msg.matchId);
        if (!m) return;
        const fromA = (m.aId === ws.userId);
        const targetWs = fromA ? m.bWs : m.aWs;
        if (!targetWs) return;
        if (typeof msg.hp === "number") { if (fromA) m.aHp = msg.hp; else m.bHp = msg.hp; }
        if (targetWs.readyState === WebSocket.OPEN) {
          try {
            targetWs.send(JSON.stringify({
              type: "pvp_opponent_pos",
              matchId: msg.matchId,
              x: Number(msg.x) || 0, y: Number(msg.y) || 0, z: Number(msg.z) || 0,
              yaw: Number(msg.yaw) || 0, hp: Number(msg.hp) || 0,
            }));
          } catch {}
        }
        break;
      }

      case "pvp_hit": {
        const m = activeMatches.get(msg.matchId);
        if (!m) return;
        const fromA = (m.aId === ws.userId);
        const targetWs = fromA ? m.bWs : m.aWs;
        if (!targetWs || targetWs.readyState !== WebSocket.OPEN) return;
        const damage = Math.max(0, Math.min(10, Number(msg.damage) | 0));
        try {
          targetWs.send(JSON.stringify({
            type: "pvp_opponent_hit", matchId: msg.matchId, damage,
          }));
        } catch {}
        break;
      }

      case "pvp_died": {
        const m = activeMatches.get(msg.matchId);
        if (!m) return;
        const fromA = (m.aId === ws.userId);
        const winnerWs = fromA ? m.bWs : m.aWs;
        try {
          if (winnerWs && winnerWs.readyState === WebSocket.OPEN) {
            winnerWs.send(JSON.stringify({ type: "pvp_opponent_died", matchId: msg.matchId }));
          }
        } catch {}
        console.log("[pvp] match " + msg.matchId + " ended: " + (fromA ? m.bName : m.aName) + " won");
        activeMatches.delete(msg.matchId);
        if (m.aWs) m.aWs.matchId = null;
        if (m.bWs) m.bWs.matchId = null;
        break;
      }

      case "pvp_result": {
        console.log("[pvp] " + ws.username + " reports " + (msg.won ? "WIN" : "LOSS"));
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
    // Remove from PvP queue
    for (let i = pvpQueue.length - 1; i >= 0; i--) {
      if (pvpQueue[i].userId === ws.userId) pvpQueue.splice(i, 1);
    }
    // Forfeit active PvP match
    if (ws.matchId) {
      const m = activeMatches.get(ws.matchId);
      if (m) {
        const winnerRole = (m.aId === ws.userId) ? "b" : "a";
        endMatch(ws.matchId, winnerRole, "opponent disconnected");
      }
    }
    broadcastQueueSize();

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
