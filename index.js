// Cover Fire relay server for Cloudflare Workers (free plan, never sleeps)
// One "Room" object per room code. Players connect with wss://.../?room=1234

export default {
  async fetch(request, env) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Game relay server is running", {
        headers: { "content-type": "text/plain" },
      });
    }
    const room = (new URL(request.url).searchParams.get("room") || "").trim();
    if (!/^\d{4}$/.test(room)) return new Response("bad room code", { status: 400 });
    const id = env.ROOMS.idFromName(room);
    return env.ROOMS.get(id).fetch(request);
  },
};

const MAX_PLAYERS = 16;

export class Room {
  constructor(ctx, env) {
    this.code = "";
    this.host = null;
    this.hostName = "Host";
    this.players = new Map(); // id -> { ws, name }
    this.ids = new Map();     // ws -> id
    this.nextId = 2;
    this.started = false;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    this.code = new URL(request.url).searchParams.get("room") || "";
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.attach(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  attach(ws) {
    ws.addEventListener("message", (ev) => {
      const d = ev.data;
      if (typeof d === "string") {
        let m;
        try { m = JSON.parse(d); } catch { return; }
        if (m && typeof m === "object") this.handle(ws, m);
      } else {
        this.relay(ws, d);
      }
    });
    ws.addEventListener("close", () => this.leave(ws));
    ws.addEventListener("error", () => this.leave(ws));
  }

  tx(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }
  sendAll(obj) {
    if (this.host) this.tx(this.host, obj);
    for (const p of this.players.values()) this.tx(p.ws, obj);
  }
  roster() {
    const list = [{ id: 1, name: this.hostName }];
    for (const [id, p] of this.players) list.push({ id, name: p.name });
    return list;
  }
  sendRoster() {
    this.sendAll({ t: "roster", players: this.roster() });
  }
  clean(n, fallback) {
    return String(n || "").trim().slice(0, 20) || fallback;
  }

  handle(ws, m) {
    switch (m.t) {
      case "host": {
        if (this.ids.has(ws)) return;
        if (this.host) return this.tx(ws, { t: "error", msg: "taken" });
        this.host = ws;
        this.hostName = this.clean(m.name, "Host");
        this.started = false;
        this.ids.set(ws, 1);
        this.tx(ws, { t: "hosted", code: this.code });
        this.sendRoster();
        break;
      }
      case "join": {
        if (this.ids.has(ws)) return;
        if (!this.host) return this.tx(ws, { t: "error", msg: "Room not found" });
        if (this.started) return this.tx(ws, { t: "error", msg: "Game already started" });
        if (this.players.size + 1 >= MAX_PLAYERS) return this.tx(ws, { t: "error", msg: "Room is full" });
        const id = this.nextId++;
        this.players.set(id, { ws, name: this.clean(m.name, "Player" + id) });
        this.ids.set(ws, id);
        this.tx(ws, { t: "joined", id, code: this.code });
        this.tx(this.host, { t: "peer_joined", id });
        this.sendRoster();
        break;
      }
      case "name": {
        const id = this.ids.get(ws);
        if (id === undefined) return;
        if (id === 1) this.hostName = this.clean(m.name, "Host");
        else if (this.players.has(id)) this.players.get(id).name = this.clean(m.name, "Player" + id);
        this.sendRoster();
        break;
      }
      case "start": {
        if (this.ids.get(ws) !== 1) return;
        this.started = true;
        this.sendAll({ t: "start" });
        break;
      }
      // "ping" just keeps the connection alive
    }
  }

  relay(ws, buf) {
    const from = this.ids.get(ws);
    if (from === undefined || !(buf instanceof ArrayBuffer) || buf.byteLength < 4) return;
    const target = new DataView(buf).getInt32(0, true);
    const dest = target === 1 ? this.host : (this.players.get(target) || {}).ws;
    if (!dest) return;
    const out = new Uint8Array(buf.byteLength);
    out.set(new Uint8Array(buf));
    new DataView(out.buffer).setInt32(0, from, true); // replace target with sender id
    try { dest.send(out); } catch {}
  }

  leave(ws) {
    const id = this.ids.get(ws);
    if (id === undefined) return;
    this.ids.delete(ws);
    if (id === 1) {
      const others = [...this.players.values()];
      this.host = null;
      this.players.clear();
      this.ids.clear();
      this.nextId = 2;
      this.started = false;
      for (const p of others) {
        this.tx(p.ws, { t: "closed" });
        try { p.ws.close(); } catch {}
      }
    } else if (this.players.delete(id)) {
      if (this.host) this.tx(this.host, { t: "peer_left", id });
      this.sendRoster();
    }
  }
}
