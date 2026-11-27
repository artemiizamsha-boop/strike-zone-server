/* Strike Zone server: rooms/presence relay, accounts, friends, DMs, leaderboard.
   Run: npm install && node server.js        (PORT env var, DATA_DIR env var for storage)
   Client protocol (JSON over WebSocket):
     -> hello{id,n}  who{id}  fr_add{to}  dm{to,txt}  lb_get  lb_submit{k,d,hs,r,e}
     -> j{r}  l{r}  p{r,p}           (join / leave / merge presence patch in a room)
     <- w{id}  s{r,peers}  p{r,peer,p}  x{r,peer}  e{r}  who{id,found,n}  frn{from,n}  dm{from,n,txt,ts,old}  lb{rows}  */
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DATA_FILE = path.join(DATA_DIR, 'data.json');
const ROOM_CAP = 10, LOBBY = 'sz-lobby', MAX_ROOMS_PER_CONN = 4;
const ID_RE = /^[1-9]\d{7}$/;

/* ---------- persistent store ---------- */
let DB = { users: {}, stats: {}, inbox: {} };   // users[id]={n}, stats[id]={n,k,d,hs,w,l,dr,m,e}, inbox[id]=[msgs]
try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); } catch (e) {}
let dirty = false;
const save = () => { if (!dirty) return; dirty = false;
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(DATA_FILE + '.tmp', JSON.stringify(DB)); fs.renameSync(DATA_FILE + '.tmp', DATA_FILE); } catch (e) { console.error('save failed', e.message); dirty = true; } };
setInterval(save, 10000).unref();
const touch = () => { dirty = true; };

/* ---------- state ---------- */
const rooms = new Map();          // name -> Map(peerId -> {ws, p})
const online = new Map();         // accountId -> Set(ws)
let seq = 0;
const cleanName = s => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 16);
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const toUser = (id, o) => { const s = online.get(id); if (!s) return false; s.forEach(w => send(w, o)); return s.size > 0; };
const queue = (id, o) => { const q = DB.inbox[id] || (DB.inbox[id] = []); q.push(o); if (q.length > 100) q.shift(); touch(); };

function leaveRoom(ws, name) {
  const r = rooms.get(name); if (!r) return;
  if (r.delete(ws.pid)) r.forEach(m => send(m.ws, { t: 'x', r: name, peer: ws.pid }));
  if (!r.size) rooms.delete(name);
  ws.rooms.delete(name);
}

/* ---------- message handlers ---------- */
const H = {
  hello(ws, m) {
    const id = String(m.id || ''), n = cleanName(m.n);
    if (!ID_RE.test(id) || !n) return;
    if (ws.acc && ws.acc !== id) { const s = online.get(ws.acc); if (s) { s.delete(ws); if (!s.size) online.delete(ws.acc); } }
    ws.acc = id;
    if (!online.has(id)) online.set(id, new Set());
    online.get(id).add(ws);
    const u = DB.users[id] || (DB.users[id] = {});
    if (u.n !== n) { u.n = n; touch(); }
    if (DB.stats[id] && DB.stats[id].n !== n) { DB.stats[id].n = n; touch(); }
    const q = DB.inbox[id];                       // deliver anything received while offline
    if (q && q.length) { q.forEach(o => send(ws, o)); delete DB.inbox[id]; touch(); }
  },
  who(ws, m) {
    const id = String(m.id || ''), u = ID_RE.test(id) && DB.users[id];
    send(ws, { t: 'who', id, found: !!u, n: u ? u.n : '' });
  },
  fr_add(ws, m) {
    const to = String(m.to || ''); if (!ws.acc || !ID_RE.test(to) || to === ws.acc || !DB.users[to]) return;
    const o = { t: 'frn', from: ws.acc, n: DB.users[ws.acc].n };
    if (!toUser(to, o)) queue(to, o);
  },
  dm(ws, m) {
    const to = String(m.to || ''), txt = String(m.txt == null ? '' : m.txt).slice(0, 200);
    if (!ws.acc || !ID_RE.test(to) || !txt.trim() || !DB.users[to]) return;
    const now = Date.now();
    if (now - (ws.lastDm || 0) < 250) return; ws.lastDm = now;      // flood guard
    const o = { t: 'dm', from: ws.acc, n: DB.users[ws.acc].n, txt, ts: now };
    if (!toUser(to, o)) queue(to, Object.assign({}, o, { old: 1 }));
  },
  lb_get(ws) {
    const rows = Object.entries(DB.stats).map(([id, s]) => ({ id, n: s.n, k: s.k, d: s.d, hs: s.hs, w: s.w, m: s.m, e: s.e | 0 }));
    send(ws, { t: 'lb', rows });
  },
  lb_submit(ws, m) {
    if (!ws.acc || !DB.users[ws.acc]) return;
    const now = Date.now(); if (now - (ws.lastLb || 0) < 20000) return; ws.lastLb = now;   // one result per 20 s
    const cl = (v, max) => Math.max(0, Math.min(max, v | 0));
    const k = cl(m.k, 60), d = cl(m.d, 60), hs = Math.min(cl(m.hs, 60), k), r = m.r;
    if (r !== 'w' && r !== 'l' && r !== 'd') return;
    const s = DB.stats[ws.acc] || (DB.stats[ws.acc] = { n: DB.users[ws.acc].n, k: 0, d: 0, hs: 0, w: 0, l: 0, dr: 0, m: 0, e: 0 });
    s.n = DB.users[ws.acc].n; s.k += k; s.d += d; s.hs += hs; s.m++;
    if (r === 'w') s.w++; else if (r === 'l') s.l++; else s.dr = (s.dr | 0) + 1;
    if (m.e != null) s.e = cl(m.e, 99999);
    touch();
  },
  j(ws, m) {
    const name = String(m.r || '').slice(0, 40); if (!name) return;
    if (ws.rooms.has(name)) { /* re-join after reconnect keeps same peer */ }
    else if (ws.rooms.size >= MAX_ROOMS_PER_CONN) return send(ws, { t: 'e', r: name });
    let r = rooms.get(name);
    if (!r) rooms.set(name, r = new Map());
    if (!r.has(ws.pid) && name !== LOBBY && r.size >= ROOM_CAP) return send(ws, { t: 'e', r: name });
    const mine = r.get(ws.pid) || { ws, p: {} }; mine.ws = ws; r.set(ws.pid, mine); ws.rooms.add(name);
    send(ws, { t: 's', r: name, peers: [...r].filter(([id]) => id !== ws.pid).map(([id, x]) => [id, x.p]) });
    r.forEach((x, id) => { if (id !== ws.pid) send(x.ws, { t: 'p', r: name, peer: ws.pid, p: mine.p }); });
  },
  l(ws, m) { leaveRoom(ws, String(m.r || '')); },
  p(ws, m) {
    const name = String(m.r || ''), r = rooms.get(name), me = r && r.get(ws.pid);
    if (!me || !m.p || typeof m.p !== 'object') return;
    for (const k in m.p) { if (m.p[k] === null) delete me.p[k]; else me.p[k] = m.p[k]; }
    if (JSON.stringify(me.p).length > 8000) { me.p = {}; return; }      // runaway presence guard
    r.forEach((x, id) => { if (id !== ws.pid) send(x.ws, { t: 'p', r: name, peer: ws.pid, p: me.p }); });
  }
};

/* ---------- http + ws ---------- */
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
  res.end('Strike Zone server OK · online ' + wss.clients.size + '\n');      // health check / keeps Render awake when pinged
});
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
wss.on('connection', ws => {
  ws.pid = 'p' + (++seq).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.rooms = new Set(); ws.alive = true; ws.acc = '';
  ws.on('pong', () => { ws.alive = true; });
  send(ws, { t: 'w', id: ws.pid });
  let budget = 0, t0 = Date.now();
  ws.on('message', data => {
    const now = Date.now(); if (now - t0 > 1000) { t0 = now; budget = 0; }
    if (++budget > 60) return;                                     // 60 msgs / s per connection
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m.t !== 'string' || !Object.prototype.hasOwnProperty.call(H, m.t)) return;
    try { H[m.t](ws, m); } catch (e) { console.error('handler', m.t, e.message); }
  });
  ws.on('close', () => {
    [...ws.rooms].forEach(n => leaveRoom(ws, n));
    const s = ws.acc && online.get(ws.acc); if (s) { s.delete(ws); if (!s.size) online.delete(ws.acc); }
  });
  ws.on('error', () => {});
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (e) {} }), 30000).unref();

process.on('SIGTERM', () => { dirty = true; save(); process.exit(0); });
process.on('SIGINT', () => { dirty = true; save(); process.exit(0); });
server.listen(PORT, () => console.log('Strike Zone server on :' + PORT));
