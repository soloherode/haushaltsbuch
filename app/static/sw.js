// Haushaltsbuch – Service Worker
//
// Zweck: nur Installierbarkeit als PWA und ein bisschen Robustheit beim
// App-Start (gecachte Fonts/Vendor-JS/Icons). Bewusst KEIN Caching von
// index.html, der API oder /login – das sind Finanzdaten bzw. Login-Logik,
// die immer frisch vom Server kommen müssen.

const CACHE_VERSION = 'v1';
const CACHE_NAME = `haushaltsbuch-shell-${CACHE_VERSION}`;

const SHELL_ASSETS = [
  '/static/fonts/fonts.css',
  '/static/fonts/inter-latin.woff2',
  '/static/fonts/inter-latin-ext.woff2',
  '/static/fonts/jetbrains-mono-latin.woff2',
  '/static/fonts/jetbrains-mono-latin-ext.woff2',
  '/static/vendor/chart.umd.min.js',
  '/static/vendor/d3-array.min.js',
  '/static/vendor/d3-path.min.js',
  '/static/vendor/d3-shape.min.js',
  '/static/vendor/d3-sankey.min.js',
  '/static/icons/icon-192.png',
  '/static/icons/icon-512.png',
  '/static/manifest.webmanifest',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);
  const isShellAsset = url.origin === self.location.origin
    && SHELL_ASSETS.includes(url.pathname);

  if (!isShellAsset) return; // alles andere (API, index.html, /login) unangetastet lassen

  event.respondWith(
    caches.match(request).then((cached) => {
      const network = fetch(request).then((response) => {
        if (response.ok) {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(request, copy));
        }
        return response;
      }).catch(() => cached);
      return cached || network;
    })
  );
});
