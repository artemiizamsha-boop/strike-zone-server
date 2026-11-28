// FragX: network first, cache only as offline fallback
const C='fragx-v1';
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('fetch',e=>{
  const r=e.request;
  if(r.method!=='GET'||new URL(r.url).origin!==location.origin||r.headers.has('range'))return;
  e.respondWith(fetch(r,{cache:'no-cache'}).then(res=>{
    if(res.status===200){const copy=res.clone();e.waitUntil(caches.open(C).then(c=>c.put(r,copy)));}
    return res;
  }).catch(()=>caches.match(r).then(m=>m||Response.error())));
});
