'use strict';
/*
  FragX — game server
  1) serves index.html (the game) over HTTP
  2) WebSocket relay on the same address (the game connects to wss://<your-host>)

  Protocol (taken from index.html):
    server -> client  {t:'w', id}                          welcome, your peer id
    client -> server  {t:'j', r}                           join room r
                      {t:'p', r, p}                        merge presence p into your room state (null = delete key)
                      {t:'l', r}                           leave room r
    server -> client  {t:'s', r, peers:[[id,presence]]}    snapshot after join
                      {t:'p', r, peer, p}                  full presence of a peer changed
                      {t:'x', r, peer}                     peer left
                      {t:'e', r}                           room is full
    accounts (8-digit ID):
                      {t:'hello', id, n}                   sign in
                      {t:'who', id}            -> {t:'who', id, found, n}
                      {t:'fr_add', to}         -> {t:'frn', from, n} to the other player
                      {t:'dm', to, txt}        -> {t:'dm', from, n, txt, ts[, old:true]}
                      {t:'lb_submit', k, d, hs, r, e}
                      {t:'lb_get'}             -> {t:'lb', rows:[{id,n,k,d,hs,w,m,e}]}
*/

const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'strike-zone.json');
const LOBBY = 'sz-lobby';
const MAX_ROOM = 16;               // players per room (the lobby has no limit)
const MAX_PRESENCE = 100000;       // bytes of one serialized presence message
const ID_RE = /^[1-9]\d{7}$/;
const ROOM_RE = /^[a-z0-9_-]{1,40}$/i;

/* ---------------- storage (JSON file, saved every few seconds) ---------------- */
let db = { users: {}, lb: {}, q: {} };   // users[id]={n,ts}  lb[id]=row  q[id]=[queued dm / friend requests]
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); } catch (e) { /* first start */ }
let dirty = false;
const save = () => { dirty = true; };
function flush() {
  if (!dirty) return;
  dirty = false;
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) { console.error('save failed:', e.message); }
}
setInterval(flush, 5000).unref();

/* ---------------- static files ---------------- */
const STATIC = new Set(['index.html', 'sw.js', 'manifest.json', 'manifest.webmanifest', 'favicon.ico', 'icon-192.png', 'icon-512.png']);
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json', '.webmanifest': 'application/manifest+json', '.ico': 'image/x-icon', '.png': 'image/png'
};
const fileCache = new Map();   // name -> {etag, raw, gz}

function getFile(name) {
  const file = path.join(__dirname, name);
  let st;
  try { st = fs.statSync(file); } catch (e) { return null; }
  const etag = '"' + st.size + '-' + Math.floor(st.mtimeMs) + '"';
  let c = fileCache.get(name);
  if (!c || c.etag !== etag) {
    const raw = fs.readFileSync(file);
    c = { etag, raw, gz: zlib.gzipSync(raw, { level: 6 }) };
    fileCache.set(name, c);
  }
  return c;
}

function onRequest(req, res) {
  let name;
  try { name = decodeURIComponent(new URL(req.url, 'http://x').pathname).replace(/^\/+/, '') || 'index.html'; }
  catch (e) { res.writeHead(400); return res.end(); }

  if (name === 'health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, players: wss.clients.size, rooms: rooms.size }));
  }
  if (!STATIC.has(name)) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }

  const c = getFile(name);
  if (!c) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
  if (req.headers['if-none-match'] === c.etag) { res.writeHead(304); return res.end(); }

  const h = {
    'Content-Type': TYPES[path.extname(name)] || 'application/octet-stream',
    'Cache-Control': 'no-cache', 'ETag': c.etag, 'Vary': 'Accept-Encoding'
  };
  if (/\bgzip\b/.test(req.headers['accept-encoding'] || '')) {
    h['Content-Encoding'] = 'gzip'; h['Content-Length'] = c.gz.length;
    res.writeHead(200, h); return res.end(c.gz);
  }
  h['Content-Length'] = c.raw.length;
  res.writeHead(200, h); res.end(c.raw);
}

const server = http.createServer(onRequest);
const wss = new WebSocketServer({ server, maxPayload: 128 * 1024, perMessageDeflate: false });

/* ---------------- rooms ---------------- */
const rooms = new Map();   // name -> Map(ws -> presence)

const tx = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

function bcast(room, str, except) {
  for (const w of room.keys()) if (w !== except && w.readyState === 1) w.send(str);
}

function join(ws, name) {
  let room = rooms.get(name);
  if (!room) { room = new Map(); rooms.set(name, room); }
  if (!room.has(ws)) {
    if (name !== LOBBY && room.size >= MAX_ROOM) {
      if (!room.size) rooms.delete(name);
      return tx(ws, { t: 'e', r: name, e: 'full' });
    }
    room.set(ws, {});
    ws.rooms.add(name);
  }
  const peers = [];
  for (const [w, p] of room) if (w !== ws) peers.push([w.pid, p]);
  tx(ws, { t: 's', r: name, peers });
}

function leave(ws, name) {
  const room = rooms.get(name);
  if (!room || !room.delete(ws)) return;
  ws.rooms.delete(name);
  if (!room.size) rooms.delete(name);
  else bcast(room, JSON.stringify({ t: 'x', r: name, peer: ws.pid }));
}

function presence(ws, name, p) {
  const room = rooms.get(name);
  if (!room || !room.has(ws) || !p || typeof p !== 'object' || Array.isArray(p)) return;
  const next = Object.assign({}, room.get(ws));
  for (const k in p) {
    if (k === '__proto__') continue;
    if (p[k] === null) delete next[k]; else next[k] = p[k];
  }
  // the client replaces the peer's presence with what it receives, so we always send the full merged object
  const str = JSON.stringify({ t: 'p', r: name, peer: ws.pid, p: next });
  if (str.length > MAX_PRESENCE) return;
  room.set(ws, next);
  bcast(room, str, ws);
}

/* ---------------- accounts: friends, private messages ---------------- */
const online = new Map();   // account id -> Set(ws)
const cleanName = n => String(n || '').replace(/[\u0000-\u001f]/g, '').slice(0, 16);

function unbind(ws) {
  if (!ws.acc) return;
  const s = online.get(ws.acc);
  if (s) { s.delete(ws); if (!s.size) online.delete(ws.acc); }
  ws.acc = null;
}

function queue(id, item, cap) {
  const q = db.q[id] || (db.q[id] = []);
  q.push(item);
  while (q.length > cap) q.shift();
  save();
}

// deliver to every open tab of this account; if nobody is online, keep it for the next sign-in
function deliver(id, msg, cap) {
  const s = online.get(id);
  if (s && s.size) { for (const w of s) tx(w, msg); return; }
  queue(id, Object.assign({}, msg, msg.t === 'dm' ? { old: true } : {}), cap);
}

function hello(ws, id, n) {
  id = String(id || '');
  if (!ID_RE.test(id)) return;
  n = cleanName(n);
  if (ws.acc !== id) {
    unbind(ws);
    ws.acc = id;
    (online.get(id) || online.set(id, new Set()).get(id)).add(ws);
  }
  ws.accName = n || id;
  db.users[id] = { n: ws.accName, ts: Date.now() };
  if (db.lb[id]) db.lb[id].n = ws.accName;
  save();
  const q = db.q[id];
  if (q && q.length) { delete db.q[id]; for (const m of q) tx(ws, m); save(); }
}

/* ---------------- leaderboard ---------------- */
const int = (x, max) => Math.max(0, Math.min(max, Math.floor(+x) || 0));
let lbCache = { at: 0, rows: [] };

function lbSubmit(ws, m) {
  if (!ws.acc) return;
  const now = Date.now();
  if (now - (ws.lastLb || 0) < 3000) return;
  ws.lastLb = now;
  const row = db.lb[ws.acc] || (db.lb[ws.acc] = { id: ws.acc, n: ws.accName, k: 0, d: 0, hs: 0, w: 0, m: 0, e: 0 });
  row.n = ws.accName;
  row.k = int(m.k, 1e7);               // the client sends its running totals
  row.d = int(m.d, 1e7);
  row.hs = Math.min(int(m.hs, 1e7), row.k);
  row.e = int(m.e, 10000);
  row.m++;                              // one submit = one finished match
  if (m.r === 'w') row.w++;
  row.ts = now;
  lbCache.at = 0;
  save();
}

function lbRows() {
  const now = Date.now();
  if (now - lbCache.at > 3000) {
    lbCache = {
      at: now,
      rows: Object.values(db.lb)
        .sort((a, b) => (b.e - a.e) || (b.k - a.k))
        .slice(0, 500)
        .map(({ id, n, k, d, hs, w, m, e }) => ({ id, n, k, d, hs, w, m, e }))
    };
  }
  return lbCache.rows;
}

/* ---------------- connections ---------------- */
wss.on('connection', ws => {
  ws.pid = crypto.randomBytes(6).toString('hex');
  ws.rooms = new Set();
  ws.acc = null; ws.accName = '';
  ws.alive = true; ws.tok = 150; ws.tt = Date.now();
  tx(ws, { t: 'w', id: ws.pid });

  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {});

  ws.on('message', (data, isBinary) => {
    ws.alive = true;
    if (isBinary) return;
    // flood protection: ~100 messages per second, bursts up to 150
    const now = Date.now();
    ws.tok = Math.min(150, ws.tok + (now - ws.tt) * 0.1); ws.tt = now;
    if (ws.tok < 1) return;
    ws.tok--;

    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    switch (m.t) {
      case 'j': {
        const r = String(m.r || '');
        if (ROOM_RE.test(r)) join(ws, r);
        break;
      }
      case 'l': {
        const r = String(m.r || '');
        if (ROOM_RE.test(r)) leave(ws, r);
        break;
      }
      case 'p': {
        const r = String(m.r || '');
        if (ROOM_RE.test(r)) presence(ws, r, m.p);
        break;
      }
      case 'hello':
        hello(ws, m.id, m.n);
        break;
      case 'who': {
        const id = String(m.id || '');
        if (!ID_RE.test(id)) break;
        const u = db.users[id];
        tx(ws, { t: 'who', id, found: !!u, n: u ? u.n : '' });
        break;
      }
      case 'fr_add': {
        const to = String(m.to || '');
        if (!ws.acc || !ID_RE.test(to) || to === ws.acc || !db.users[to]) break;
        const q = db.q[to];
        if (!online.get(to) && q && q.some(x => x.t === 'frn' && x.from === ws.acc)) break;   // no duplicate requests
        deliver(to, { t: 'frn', from: ws.acc, n: ws.accName }, 50);
        break;
      }
      case 'dm': {
        const to = String(m.to || '');
        const txt = String(m.txt || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 200);
        if (!ws.acc || !txt || !ID_RE.test(to) || to === ws.acc || !db.users[to]) break;
        if (now - (ws.lastDm || 0) < 400) break;
        ws.lastDm = now;
        deliver(to, { t: 'dm', from: ws.acc, n: ws.accName, txt, ts: now }, 100);
        break;
      }
      case 'lb_submit':
        lbSubmit(ws, m);
        break;
      case 'lb_get':
        tx(ws, { t: 'lb', rows: lbRows() });
        break;
    }
  });

  ws.on('close', () => {
    for (const name of [...ws.rooms]) leave(ws, name);
    unbind(ws);
  });
});

// keep connections alive (Render closes idle ones) and drop dead sockets
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  }
}, 25000).unref();

server.listen(PORT, () => console.log('FragX server on port ' + PORT));

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => { flush(); process.exit(0); });
}