// RooKoo service worker: network-first for same-origin assets with an offline
// cache fallback, so the app is installable and still opens without a network.
// Cross-origin requests (Nostr relays, the MediaPipe CDN) are left untouched.
const CACHE = 'rookoo-v1';
const SHELL = [
    './',
    './index.html',
    './style.css',
    './app.js',
    './nostr-p2p.js',
    './nostr-deps.js',
    './store.js',
    './icon.png',
    './icon-192.png',
    './icon-512.png',
    './manifest.webmanifest',
];

self.addEventListener('install', (event) => {
    event.waitUntil(
        caches.open(CACHE).then((c) => c.addAll(SHELL)).catch(() => {})
    );
    self.skipWaiting();
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys()
            .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
            .then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', (event) => {
    const req = event.request;
    if (req.method !== 'GET') return;
    let url;
    try { url = new URL(req.url); } catch { return; }
    if (url.origin !== self.location.origin) return; // relays / CDN / model

    event.respondWith(
        fetch(req)
            .then((res) => {
                if (res && res.ok) {
                    const copy = res.clone();
                    caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {});
                }
                return res;
            })
            .catch(async () => {
                const cached = await caches.match(req);
                if (cached) return cached;
                if (req.mode === 'navigate') {
                    return (await caches.match('./index.html')) || Response.error();
                }
                return Response.error();
            })
    );
});
