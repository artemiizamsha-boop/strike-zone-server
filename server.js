// Strike Zone server: static files + WebSocket (rooms, friends, chat, leaderboard). No npm packages needed.
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const PORT=process.env.PORT||3000,ROOT=__dirname,DB_FILE=process.env.DB_FILE||path.join(ROOT,'data.json');
const MIME={'.html':'text/html; charset=utf-8','.js':'text/javascript','.css':'text/css','.json':'application/json','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon','.mp3':'audio/mpeg','.ogg':'audio/ogg','.webmanifest':'application/manifest+json'};

/* ---------- storage (JSON file; on free Render disk it is wiped on redeploy) ---------- */
let DB={u:{},pend:{},dm:{}};
try{const d=JSON.parse(fs.readFileSync(DB_FILE,'utf8'));if(d&&d.u)DB=Object.assign(DB,d)}catch(e){}
const UP=process.env.UPSTASH_REDIS_REST_URL,UT=process.env.UPSTASH_REDIS_REST_TOKEN;
const up=(cmd)=>fetch(UP,{method:'POST',headers:{Authorization:'Bearer '+UT,'Content-Type':'application/json'},body:JSON.stringify(cmd)}).then(r=>r.json());
if(UP&&UT)up(['GET','sz_db']).then(r=>{if(r&&r.result){const d=JSON.parse(r.result);if(d&&d.u)DB=Object.assign(DB,d);console.log('db loaded from Upstash')}}).catch(e=>console.log('upstash load failed',e.message));
let dirty=false;const touch=()=>{dirty=true};
setInterval(()=>{if(!dirty)return;dirty=false;const j=JSON.stringify(DB);try{fs.writeFileSync(DB_FILE+'.tmp',j);fs.renameSync(DB_FILE+'.tmp',DB_FILE)}catch(e){}if(UP&&UT)up(['SET','sz_db',j]).catch(()=>{});},5000);
const idOk=x=>typeof x==='string'&&/^[1-9]\d{7}$/.test(x);
const cl=(s,n)=>String(s==null?'':s).replace(/[\u0000-\u001f<>]/g,'').slice(0,n);

/* ---------- http ---------- */
const srv=http.createServer((req,res)=>{
  let u=decodeURIComponent((req.url||'/').split('?')[0]);
  if(u==='/health'){res.writeHead(200);return res.end('ok')}
  if(u==='/')u='/index.html';
  const rel=path.normalize(u);if(/data\.json|server\.js|node_modules/.test(rel)||rel.includes('..')){res.writeHead(404);return res.end('not found')}
  const f=path.join(ROOT,rel),f2=path.join(ROOT,'..',rel);
  const go=(p,alt)=>fs.readFile(p,(e,b)=>{if(e){if(alt)return go(alt);res.writeHead(404);return res.end('not found')}
    res.writeHead(200,{'Content-Type':MIME[path.extname(p)]||'application/octet-stream','Cache-Control':'no-cache'});res.end(b)});go(f,f2)});

/* ---------- minimal WebSocket ---------- */
const clients=new Set(),chans=new Map();let seq=0;
function frame(op,buf){const n=buf.length;let h;if(n<126)h=Buffer.from([0x80|op,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}return Buffer.concat([h,buf])}
function send(c,o){if(c.dead)return;try{c.sock.write(frame(1,Buffer.from(JSON.stringify(o))))}catch(e){}}
srv.on('upgrade',(req,sock)=>{
  const k=req.headers['sec-websocket-key'];if(!k||(req.headers.upgrade||'').toLowerCase()!=='websocket'){sock.destroy();return}
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(k+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')+'\r\n\r\n');
  sock.setNoDelay(true);
  const c={sock,peer:'p'+(++seq).toString(36)+crypto.randomBytes(3).toString('hex'),id:'',n:'',pres:{},rate:{},buf:Buffer.alloc(0),frag:null,dead:false,alive:true};
  clients.add(c);send(c,{t:'w',id:c.peer});
  sock.on('data',d=>{c.buf=Buffer.concat([c.buf,d]);if(c.buf.length>1<<20){drop(c);return}parse(c)});
  sock.on('close',()=>drop(c));sock.on('error',()=>drop(c));});
function parse(c){
  for(;;){const b=c.buf;if(b.length<2)return;const fin=!!(b[0]&0x80),op=b[0]&15,m=!!(b[1]&0x80);let len=b[1]&127,o=2;
    if(len===126){if(b.length<4)return;len=b.readUInt16BE(2);o=4}else if(len===127){if(b.length<10)return;len=Number(b.readBigUInt64BE(2));o=10}
    if(len>65536){drop(c);return}
    const need=o+(m?4:0)+len;if(b.length<need)return;
    let p=b.slice(o+(m?4:0),need);if(m){const mk=b.slice(o,o+4);p=Buffer.from(p);for(let i=0;i<p.length;i++)p[i]^=mk[i&3]}
    c.buf=b.slice(need);
    if(op===8){drop(c);return}
    if(op===9){try{c.sock.write(frame(10,p))}catch(e){}continue}
    if(op===10){c.alive=true;continue}
    if(op===1||op===0){if(op===1)c.frag=[];if(!c.frag)continue;c.frag.push(p);if(fin){const s=Buffer.concat(c.frag).toString('utf8');c.frag=null;let o2;try{o2=JSON.parse(s)}catch(e){continue}try{onMsg(c,o2)}catch(e){}}}}}
function drop(c){if(c.dead)return;c.dead=true;clients.delete(c);try{c.sock.destroy()}catch(e){}for(const ch of Object.keys(c.pres))leave(c,ch)}
setInterval(()=>{for(const c of clients){if(!c.alive){drop(c);continue}c.alive=false;try{c.sock.write(frame(9,Buffer.alloc(0)))}catch(e){}}},25000);

/* ---------- rooms (protocol of the game's relay client: j / l / p -> w / s / p / x / e) ---------- */
const rooms=new Map();
const members=r=>rooms.get(r)||(rooms.set(r,new Set()),rooms.get(r));
const fan=(r,m,except)=>{const s=rooms.get(r);if(!s)return;const d=frame(1,Buffer.from(JSON.stringify(m)));for(const c of s)if(c!==except&&!c.dead){try{c.sock.write(d)}catch(e){}}};
function join(c,r){if(!/^[\w-]{1,24}$/.test(r))return;const s=members(r);
  if(c.pres[r]){send(c,{t:'s',r,peers:[...s].filter(x=>x!==c).map(x=>[x.peer,x.pres[r]||{}])});return}
  if(s.size>=(r==='sz-lobby'?600:12)){send(c,{t:'e',r});return}
  s.add(c);c.pres[r]={};send(c,{t:'s',r,peers:[...s].filter(x=>x!==c).map(x=>[x.peer,x.pres[r]||{}])});fan(r,{t:'p',r,peer:c.peer,p:{}},c)}
function leave(c,r){const s=rooms.get(r);if(s){s.delete(c);if(!s.size)rooms.delete(r);else fan(r,{t:'x',r,peer:c.peer})}delete c.pres[r]}
function byId(id){const a=[];for(const c of clients)if(c.id===id)a.push(c);return a}
const lim=(c,k,ms)=>{const n=Date.now();if(c.rate[k]&&n-c.rate[k]<ms)return false;c.rate[k]=n;return true};

/* ---------- messages ---------- */
function onMsg(c,m){
  switch(m.t){
  case 'hello':{const id=cl(m.id,8),n=cl(m.n,16);if(!idOk(id)){c.id='';return}c.id=id;c.n=n||'Player';
    const u=DB.u[id]||(DB.u[id]={n:c.n,k:0,d:0,hs:0,m:0,w:0,l:0,t:Date.now()});u.n=c.n;u.last=Date.now();touch();
    const pend=DB.pend[id];if(pend&&pend.length){for(const f of pend)send(c,{t:'frn',from:f.id,n:f.n});delete DB.pend[id];touch()}
    const dm=DB.dm[id];if(dm&&dm.length){for(const x of dm)send(c,{t:'dm',from:x.f,n:x.n,txt:x.x,ts:x.ts,old:1});delete DB.dm[id];touch()}
    send(c,{t:'hello_ok',id});break}
  case 'j':join(c,cl(m.r,24));break;
  case 'l':leave(c,cl(m.r,24));break;
  case 'p':{const r=cl(m.r,24);if(!c.pres[r]||!m.p||typeof m.p!=='object')return;const p=Object.assign({},c.pres[r]);for(const k in m.p){if(m.p[k]===null)delete p[k];else p[k]=m.p[k]}
    if(JSON.stringify(p).length>4000)return;c.pres[r]=p;fan(r,{t:'p',r,peer:c.peer,p},c);break}
  case 'who':{const id=cl(m.id,8),u=DB.u[id];send(c,{t:'who',id,found:!!u,n:u?u.n:'',on:byId(id).length>0});break}
  case 'online':{const ids=Array.isArray(m.ids)?m.ids.slice(0,100):[];const r={};for(const i of ids){const id=cl(i,8);const u=DB.u[id];r[id]=u?{n:u.n,on:byId(id).length>0}:null}send(c,{t:'online',r});break}
  case 'fr_add':{if(!c.id||!lim(c,'fr',1500))return;const to=cl(m.to,8);if(!DB.u[to]||to===c.id)return;const t=byId(to);
    if(t.length)for(const x of t)send(x,{t:'frn',from:c.id,n:c.n});else{const a=DB.pend[to]||(DB.pend[to]=[]);if(!a.some(f=>f.id===c.id)){a.push({id:c.id,n:c.n});if(a.length>30)a.shift();touch()}}break}
  case 'dm':{if(!c.id||!lim(c,'dm',300))return;const to=cl(m.to,8),txt=cl(m.txt,200).trim();if(!txt||!DB.u[to])return;const t=byId(to),ts=Date.now();
    if(t.length)for(const x of t)send(x,{t:'dm',from:c.id,n:c.n,txt,ts});else{const a=DB.dm[to]||(DB.dm[to]=[]);a.push({f:c.id,n:c.n,x:txt,ts});if(a.length>60)a.shift();touch()}
    send(c,{t:'dm_ok',to,ts});break}
  case 'lb_submit':{if(!c.id||!lim(c,'lb',15000))return;const u=DB.u[c.id];if(!u)return;
    const k=Math.max(0,Math.min(80,m.k|0)),d=Math.max(0,Math.min(80,m.d|0)),hs=Math.max(0,Math.min(k,m.hs|0)),r=m.r==='w'?'w':m.r==='l'?'l':'d';
    u.k+=k;u.d+=d;u.hs+=hs;u.m++;if(r==='w')u.w++;else if(r==='l')u.l++;u.last=Date.now();touch();send(c,{t:'lb_ok'});break}
  case 'lb_get':{if(!lim(c,'lbg',1000))return;const a=Object.keys(DB.u).filter(i=>DB.u[i].m>0).map(i=>{const u=DB.u[i];return{id:i,n:u.n,k:u.k,d:u.d,hs:u.hs,m:u.m,w:u.w}}).sort((x,y)=>y.k-x.k).slice(0,100);send(c,{t:'lb',rows:a});break}
  }}
srv.listen(PORT,()=>console.log('Strike Zone server on :'+PORT));
