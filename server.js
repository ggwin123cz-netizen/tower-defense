// Tower Defense multiplayer server: serves the game and relays messages between the players of a room.
// The host's browser runs the game; this server only manages rooms, chat and reconnecting.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8765;
const MAX_PLAYERS = 4;
const RESERVE_MS = 3 * 60 * 1000; // how long a dropped player's slot is kept during a game
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const PAGE = path.join(__dirname, 'index.html');

const rooms = new Map();

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  if (url === '/' || url === '/index.html') {
    fs.readFile(PAGE, (err, data) => {
      if (err) { res.writeHead(500); return res.end('Game file missing'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  // Test harness, only present on the developer's machine (the preview folder is not deployed)
  if (url === '/sim.js' && fs.existsSync(path.join(__dirname, 'preview', 'sim.js'))) {
    res.writeHead(200, { 'Content-Type': 'text/javascript', 'Cache-Control': 'no-store' });
    return fs.createReadStream(path.join(__dirname, 'preview', 'sim.js')).pipe(res);
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 512 * 1024, perMessageDeflate: { threshold: 512 } });

const clean = (s, max, fallback) => {
  s = String(s || '').replace(/[\u0000-\u001f<>&"']/g, '').trim().slice(0, max);
  return s || fallback;
};
const send = (ws, obj) => {
  if (ws && ws.readyState === 1) ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj));
};
const newCode = () => {
  for (let tries = 0; tries < 200; tries++) {
    let c = '';
    for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
};
const roster = room => [...room.members.values()].map(m => ({ id: m.id, name: m.name, host: m.id === 1, away: !!m.away }));
const broadcast = (room, obj, exceptId) => {
  const s = JSON.stringify(obj);
  for (const m of room.members.values()) if (m.id !== exceptId) send(m.ws, s);
};
const attach = (room, m, ws) => { m.ws = ws; m.away = false; ws.pid = m.id; ws.room = room; };

// abrupt: the socket dropped (not an explicit leave). During a game a dropped guest keeps their slot for a while.
function leave(ws, abrupt) {
  const room = ws.room;
  if (!room) return;
  const id = ws.pid;
  const m = room.members.get(id);
  ws.room = null;
  if (!m || m.ws !== ws) return;
  if (id === 1) {
    // The host runs the game, so the room ends when the host leaves.
    broadcast(room, { t: 'closed', why: 'The host left the game.' }, 1);
    for (const k of room.members.values()) { if (k.timer) clearTimeout(k.timer); if (k.ws) k.ws.room = null; }
    room.members.delete(1);
    rooms.delete(room.code);
    return;
  }
  const drop = () => {
    if (room.members.get(id) !== m) return;
    if (m.timer) clearTimeout(m.timer);
    room.members.delete(id);
    send(room.host, { t: 'gone', id, final: 1 });
    broadcast(room, { t: 'players', players: roster(room) });
  };
  if (abrupt && room.playing) {
    m.ws = null; m.away = true;
    m.timer = setTimeout(drop, RESERVE_MS);
    send(room.host, { t: 'gone', id });
    broadcast(room, { t: 'players', players: roster(room) });
    return;
  }
  drop();
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.room = null;
  ws.bucket = 120;
  ws.lastChat = 0;
  ws.on('pong', () => { ws.alive = true; });
  const refill = setInterval(() => { ws.bucket = Math.min(240, ws.bucket + 120); }, 1000);

  ws.on('message', raw => {
    ws.alive = true;
    if (ws.bucket-- <= 0) return;
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const room = ws.room;

    if (m.t === 'ping') return send(ws, { t: 'pong' });

    if (m.t === 'create') {
      if (room) leave(ws, false);
      const code = newCode();
      if (!code) return send(ws, { t: 'error', why: 'The server is full. Try again later.' });
      const r = { code, members: new Map(), host: ws, settings: null, playing: false };
      const tok = crypto.randomBytes(8).toString('hex');
      const mem = { id: 1, name: clean(m.name, 16, 'Host'), ws, tok, away: false };
      ws.pid = 1;
      ws.room = r;
      r.members.set(1, mem);
      rooms.set(code, r);
      return send(ws, { t: 'joined', you: 1, code, tok, players: roster(r), settings: null, playing: false });
    }

    if (m.t === 'join') {
      if (room) leave(ws, false);
      const code = String(m.code || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
      const r = rooms.get(code);
      if (!r) return send(ws, { t: 'error', why: 'Room "' + code + '" was not found. Check the code.', gone: 1 });
      const name = clean(m.name, 16, 'Player');
      // Coming back after a dropped connection or a page refresh
      const tok = typeof m.tok === 'string' ? m.tok : '';
      if (tok) {
        for (const k of r.members.values()) {
          if (k.id !== 1 && k.tok === tok) {
            if (k.timer) clearTimeout(k.timer);
            if (k.ws && k.ws !== ws) { k.ws.room = null; try { k.ws.close(); } catch (e) {} }
            attach(r, k, ws);
            send(ws, { t: 'joined', you: k.id, code, tok, players: roster(r), settings: r.settings, playing: r.playing, back: true });
            broadcast(r, { t: 'players', players: roster(r) }, k.id);
            return send(r.host, { t: 'peer', id: k.id, name: k.name, info: m.info || null, back: true });
          }
        }
      }
      if (r.members.size >= MAX_PLAYERS) return send(ws, { t: 'error', why: 'This room is full (4 players).' });
      let id = 2;
      while (r.members.has(id)) id++;
      const mem = { id, name, ws, tok: crypto.randomBytes(8).toString('hex'), away: false };
      r.members.set(id, mem);
      ws.pid = id;
      ws.room = r;
      send(ws, { t: 'joined', you: id, code, tok: mem.tok, players: roster(r), settings: r.settings, playing: r.playing });
      broadcast(r, { t: 'players', players: roster(r) }, id);
      return send(r.host, { t: 'peer', id, name, info: m.info || null });
    }

    if (!room) return;

    if (m.t === 'leave') return leave(ws, false);

    if (m.t === 'chat') {
      const now = Date.now();
      if (now - ws.lastChat < 400) return;
      ws.lastChat = now;
      const mem = room.members.get(ws.pid);
      const text = clean(m.text, 140, '');
      if (!mem || !text) return;
      // Only the host may post system lines
      if (m.sys && ws.pid === 1) return broadcast(room, { t: 'chat', sys: 1, text });
      return broadcast(room, { t: 'chat', from: ws.pid, name: mem.name, text });
    }

    // Host-only messages
    if (ws.pid === 1) {
      if (m.t === 'settings') {
        room.settings = m.settings || null;
        return broadcast(room, { t: 'settings', settings: room.settings }, 1);
      }
      if (m.t === 'start') {
        room.playing = true;
        return broadcast(room, { t: 'start', settings: m.settings, players: roster(room) }, 1);
      }
      if (m.t === 'lobby') {
        room.playing = false;
        // Players who dropped during the game are gone for good now
        for (const k of [...room.members.values()]) if (k.away) { if (k.timer) clearTimeout(k.timer); room.members.delete(k.id); }
        broadcast(room, { t: 'players', players: roster(room) }, 1);
        return broadcast(room, { t: 'lobby' }, 1);
      }
      if (m.t === 'kick') {
        const k = room.members.get(m.id);
        if (k && k.id !== 1) {
          send(k.ws, { t: 'closed', why: 'The host removed you from the room.' });
          if (k.ws) k.ws.room = null;
          if (k.timer) clearTimeout(k.timer);
          room.members.delete(k.id);
          broadcast(room, { t: 'players', players: roster(room) });
        }
        return;
      }
      if (m.t === 'h2g') {
        // Forward the payload as-is to one guest, or to every guest
        const s = JSON.stringify({ t: 'm', d: m.d });
        if (m.to) { const k = room.members.get(m.to); if (k) send(k.ws, s); }
        else for (const k of room.members.values()) if (k.id !== 1) send(k.ws, s);
        return;
      }
      return;
    }

    // Guest messages go to the host only
    if (m.t === 'g2h') return send(room.host, { t: 'm', from: ws.pid, d: m.d });
  });

  ws.on('close', () => { clearInterval(refill); leave(ws, true); });
  ws.on('error', () => {});
});

// Keep connections alive through proxies and drop dead ones
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Tower Defense server on port ' + PORT));
