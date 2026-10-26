// Tower Defense multiplayer server: serves the game and relays messages between the players of a room.
// The host's browser runs the game; this server only manages rooms and forwards messages.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8765;
const MAX_PLAYERS = 4;
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

const cleanName = (s, fallback) => {
  s = String(s || '').replace(/[\u0000-\u001f<>&"']/g, '').trim().slice(0, 16);
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
const roster = room => [...room.members.values()].map(m => ({ id: m.id, name: m.name, host: m.id === 1 }));
const broadcast = (room, obj, exceptId) => {
  const s = JSON.stringify(obj);
  for (const m of room.members.values()) if (m.id !== exceptId) send(m.ws, s);
};

function leave(ws) {
  const room = ws.room;
  if (!room) return;
  const id = ws.pid;
  ws.room = null;
  room.members.delete(id);
  if (id === 1) {
    // The host runs the game, so the room ends when the host leaves.
    broadcast(room, { t: 'closed', why: 'The host left the game.' });
    for (const m of room.members.values()) m.ws.room = null;
    rooms.delete(room.code);
    return;
  }
  send(room.host, { t: 'gone', id });
  broadcast(room, { t: 'players', players: roster(room) });
  if (!room.members.size) rooms.delete(room.code);
}

wss.on('connection', ws => {
  ws.alive = true;
  ws.room = null;
  ws.bucket = 120;
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
      if (room) leave(ws);
      const code = newCode();
      if (!code) return send(ws, { t: 'error', why: 'The server is full. Try again later.' });
      const r = { code, members: new Map(), host: ws, settings: null, nextId: 2, playing: false };
      ws.pid = 1;
      ws.room = r;
      r.members.set(1, { id: 1, name: cleanName(m.name, 'Host'), ws });
      rooms.set(code, r);
      return send(ws, { t: 'joined', you: 1, code, players: roster(r), settings: null, playing: false });
    }

    if (m.t === 'join') {
      if (room) leave(ws);
      const code = String(m.code || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
      const r = rooms.get(code);
      if (!r) return send(ws, { t: 'error', why: 'Room "' + code + '" was not found. Check the code.' });
      if (r.playing) return send(ws, { t: 'error', why: 'This game has already started. Ask the host to go back to the lobby.' });
      if (r.members.size >= MAX_PLAYERS) return send(ws, { t: 'error', why: 'This room is full (4 players).' });
      let id = 2;
      while (r.members.has(id)) id++;
      ws.pid = id;
      ws.room = r;
      const name = cleanName(m.name, 'Player ' + id);
      r.members.set(id, { id, name, ws });
      send(ws, { t: 'joined', you: id, code, players: roster(r), settings: r.settings, playing: r.playing });
      broadcast(r, { t: 'players', players: roster(r) }, id);
      return send(r.host, { t: 'peer', id, name, info: m.info || null });
    }

    if (!room) return;

    if (m.t === 'leave') return leave(ws);

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
        return broadcast(room, { t: 'lobby' }, 1);
      }
      if (m.t === 'kick') {
        const k = room.members.get(m.id);
        if (k && k.id !== 1) {
          send(k.ws, { t: 'closed', why: 'The host removed you from the room.' });
          leave(k.ws);
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

  ws.on('close', () => { clearInterval(refill); leave(ws); });
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
