// Simple room relay server for Godot games (WebSocket)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 16;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Game relay server is running");
});
const wss = new WebSocketServer({ server, maxPayload: 1 << 20 });

const rooms = new Map(); // code -> { host, hostName, players: Map(id -> {ws,name}), nextId, started }

const tx = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function rosterOf(room) {
  const list = [{ id: 1, name: room.hostName }];
  for (const [id, p] of room.players) list.push({ id, name: p.name });
  return list;
}
function sendAll(room, obj) {
  tx(room.host, obj);
  for (const p of room.players.values()) tx(p.ws, obj);
}
const sendRoster = (room) => sendAll(room, { t: "roster", players: rosterOf(room) });

function newCode() {
  let code;
  do { code = String(1000 + Math.floor(Math.random() * 9000)); } while (rooms.has(code));
  return code;
}
const cleanName = (n, fallback) => String(n || "").trim().slice(0, 20) || fallback;

function relay(ws, data) {
  const ctx = ws.ctx;
  if (!ctx || data.length < 4) return;
  const target = data.readInt32LE(0);
  const room = ctx.room;
  const dest = target === 1 ? room.host : (room.players.get(target) || {}).ws;
  if (!dest || dest.readyState !== 1) return;
  const out = Buffer.allocUnsafe(data.length);
  out.writeInt32LE(ctx.id, 0);   // replace target with sender id
  data.copy(out, 4, 4);
  dest.send(out, { binary: true });
}

function handle(ws, m) {
  switch (m.t) {
    case "host": {
      if (ws.ctx) return;
      const code = newCode();
      const room = { host: ws, hostName: cleanName(m.name, "Host"), players: new Map(), nextId: 2, started: false };
      rooms.set(code, room);
      ws.ctx = { room, id: 1, code };
      tx(ws, { t: "hosted", code });
      sendRoster(room);
      break;
    }
    case "join": {
      if (ws.ctx) return;
      const room = rooms.get(String(m.code || "").trim());
      if (!room) return tx(ws, { t: "error", msg: "Room not found" });
      if (room.started) return tx(ws, { t: "error", msg: "Game already started" });
      if (room.players.size + 1 >= MAX_PLAYERS) return tx(ws, { t: "error", msg: "Room is full" });
      const id = room.nextId++;
      room.players.set(id, { ws, name: cleanName(m.name, "Player" + id) });
      ws.ctx = { room, id };
      tx(ws, { t: "joined", id, code: String(m.code).trim() });
      tx(room.host, { t: "peer_joined", id });
      sendRoster(room);
      break;
    }
    case "name": {
      if (!ws.ctx) return;
      const { room, id } = ws.ctx;
      if (id === 1) room.hostName = cleanName(m.name, "Host");
      else if (room.players.has(id)) room.players.get(id).name = cleanName(m.name, "Player" + id);
      sendRoster(room);
      break;
    }
    case "start": {
      if (!ws.ctx || ws.ctx.id !== 1) return;
      ws.ctx.room.started = true;
      sendAll(ws.ctx.room, { t: "start" });
      break;
    }
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.ctx = null;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (data, isBinary) => {
    if (isBinary) return relay(ws, data);
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    if (m && typeof m === "object") handle(ws, m);
  });
  ws.on("close", () => {
    const ctx = ws.ctx;
    if (!ctx) return;
    const { room, id, code } = ctx;
    if (id === 1) {
      for (const p of room.players.values()) { tx(p.ws, { t: "closed" }); p.ws.close(); }
      rooms.delete(code);
    } else if (room.players.delete(id)) {
      tx(room.host, { t: "peer_left", id });
      sendRoster(room);
    }
  });
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

server.listen(PORT, () => console.log("Relay listening on " + PORT));
