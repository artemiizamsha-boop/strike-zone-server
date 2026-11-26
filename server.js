/* Strike Zone multiplayer relay. No dependencies: only Node.js 18+ built-ins.
   Run:  node server.js     (PORT env var, default 8080)
   Clients connect with a WebSocket, join named rooms and publish a "presence" object that everybody in the room sees. */
const http = require('http'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080, MAX_ROOM = +process.env.MAX_ROOM || 12, MAX_MSG = 4096;
const rooms = new Map();          // room name -> Map(peerId -> peer)
let nextId = 1;

/* ---------- minimal WebSocket (RFC 6455) ---------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function frame(op, payload) {
  const b = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const n = b.length;
  const h = n < 126 ? Buffer.from([0x80 | op, n]) : n < 65536 ? Buffer.from([0x80 | op, 126, n >> 8, n & 255]) : (() => { const x = Buffer.alloc(10); x[0] = 0x80 | op; x[1] = 127; x.writeBigUInt64BE(BigInt(n), 2); return x; })();
  return Buffer.concat([h, b]);
}
function accept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) return socket.destroy();
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + GUID).digest('base64') + '\r\n\r\n');
  socket.setNoDelay(true);
  const peer = { id: 'p' + (nextId++).toString(36) + crypto.randomBytes(3).toString('hex'), socket, rooms: new Set(), pres: {}, buf: Buffer.alloc(0), frag: null, tokens: 60, last: Date.now(), alive: true };
  peer.send = o => { if (!socket.destroyed) socket.write(frame(1, JSON.stringify(o))); };
  socket.on('data', d => {
    peer.buf = Buffer.concat([peer.buf, d]);
    if (peer.buf.length > MAX_MSG * 4) return socket.destroy();
    for (;;) {
      const b = peer.buf; if (b.length < 2) return;
      const op = b[0] & 15, fin = !!(b[0] & 128), masked = !!(b[1] & 128); let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (!masked || len > MAX_MSG) return socket.destroy();
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      peer.buf = b.subarray(off + 4 + len);
      if (op === 8) { socket.end(frame(8, Buffer.alloc(0))); return; }
      if (op === 9) { socket.write(frame(10, data)); continue; }
      if (op === 10) { peer.alive = true; continue; }
      if (op === 1 || op === 2) peer.frag = fin ? null : { parts: [data] };
      if (op === 0 && peer.frag) { peer.frag.parts.push(data); if (fin) { const all = Buffer.concat(peer.frag.parts); peer.frag = null; onMessage(peer, all.toString()); } continue; }
      if ((op === 1 || op === 2) && fin) onMessage(peer, data.toString());
    }
  });
  const gone = () => { if (peer.dead) return; peer.dead = true; for (const r of [...peer.rooms]) leave(peer, r); };
  socket.on('close', gone); socket.on('error', gone);
  peer.send({ t: 'w', id: peer.id });
  return peer;
}

/* ---------- rooms ---------- */
const okName = s => typeof s === 'string' && /^[\w-]{1,32}$/.test(s);
function bcast(room, msg, except) { const r = rooms.get(room); if (!r) return; const s = JSON.stringify(msg); for (const p of r.values()) if (p !== except && !p.socket.destroyed) p.socket.write(frame(1, s)); }
function join(peer, name) {
  let r = rooms.get(name); if (!r) rooms.set(name, r = new Map());
  if (peer.rooms.has(name)) return;
  if (r.size >= MAX_ROOM) return peer.send({ t: 'e', r: name, m: 'full' });
  r.set(peer.id, peer); peer.rooms.add(name);
  peer.send({ t: 's', r: name, peers: [...r.values()].map(p => [p.id, p.pres[name] || {}]) });
  bcast(name, { t: 'p', r: name, peer: peer.id, p: peer.pres[name] || {} }, peer);
}
function leave(peer, name) {
  const r = rooms.get(name); if (!r || !peer.rooms.has(name)) return;
  r.delete(peer.id); peer.rooms.delete(name); delete peer.pres[name];
  if (!r.size) rooms.delete(name); else bcast(name, { t: 'x', r: name, peer: peer.id });
}
function onMessage(peer, text) {
  const now = Date.now(); peer.tokens = Math.min(60, peer.tokens + (now - peer.last) / 1000 * 40); peer.last = now;   // ~40 msgs/s
  if (peer.tokens < 1) return; peer.tokens--;
  let m; try { m = JSON.parse(text); } catch { return; }
  if (!m || !okName(m.r)) return;
  if (m.t === 'j') join(peer, m.r);
  else if (m.t === 'l') leave(peer, m.r);
  else if (m.t === 'p' && peer.rooms.has(m.r) && m.p && typeof m.p === 'object' && !Array.isArray(m.p)) {
    const cur = peer.pres[m.r] || (peer.pres[m.r] = {});
    for (const k of Object.keys(m.p)) { if (m.p[k] === null) delete cur[k]; else cur[k] = m.p[k]; }   // shallow merge, null removes a key
    bcast(m.r, { t: 'p', r: m.r, peer: peer.id, p: cur }, peer);
  }
}

/* ---------- http ---------- */
const fs = require('fs'), path = require('path');
const STATIC = { '/': 'index.html', '/index.html': 'index.html', '/manifest.webmanifest': 'manifest.webmanifest', '/sw.js': 'sw.js', '/icon-192.png': 'icon-192.png', '/icon-512.png': 'icon-512.png' };
const MIME = { html: 'text/html; charset=utf-8', webmanifest: 'application/manifest+json', js: 'text/javascript; charset=utf-8', png: 'image/png' };
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0], f = STATIC[url];
  if (f) {   // the game itself: open this server's address in any browser, or install it as an app
    fs.readFile(path.join(__dirname, f), (err, data) => {
      if (err) { res.writeHead(404); return res.end('not found'); }
      res.writeHead(200, { 'Content-Type': MIME[f.split('.').pop()] || 'application/octet-stream', 'Cache-Control': f === 'sw.js' || f === 'index.html' ? 'no-cache' : 'public, max-age=86400' });
      res.end(data);
    });
    return;
  }
  res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });   // /health and anything else
  res.end(JSON.stringify({ ok: true, game: 'strike-zone', rooms: rooms.size }));
});
server.on('upgrade', (req, socket) => accept(req, socket));
setInterval(() => { for (const r of rooms.values()) for (const p of r.values()) { if (!p.alive) { p.socket.destroy(); continue; } p.alive = false; if (!p.socket.destroyed) p.socket.write(frame(9, Buffer.alloc(0))); } }, 25000).unref();
server.listen(PORT, () => console.log('Strike Zone relay listening on :' + PORT));
