const CACHE_NAME = "salon-karola-icons-v3-__APP_VERSION__";
const STATIC_ASSETS = [
  "/manifest.webmanifest",
  "/manifest.json",
  "/static/style.css",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png",
  "/static/icons/maskable-192.png",
  "/static/icons/maskable-512.png",
  "/static/icons/apple-touch-icon.png",
  "/static/favicon.png",
];

function isSameOrigin(urlString) {
  try {
    return new URL(urlString, self.location.origin).origin === self.location.origin;
  } catch (error) {
    return false;
  }
}

async function safePrecache() {
  const cache = await caches.open(CACHE_NAME);
  await Promise.allSettled(
    STATIC_ASSETS.map(async (assetUrl) => {
      try {
        const response = await fetch(assetUrl, { cache: "no-store" });
        if (response && response.ok) {
          await cache.put(assetUrl, response.clone());
        }
      } catch (error) {}
    })
  );
}

function offlineResponse() {
  return new Response(
    "<!doctype html><html lang='de'><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Salon Karola App</title><body style='font-family:sans-serif;padding:24px;background:#f7f3ee;color:#1f1f1f'><h1>Verbindung fehlgeschlagen</h1><p>Verbindung zur Salon Karola App konnte nicht hergestellt werden.</p><button onclick='location.reload()'>Erneut versuchen</button></body></html>",
    {
      status: 503,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    }
  );
}

self.addEventListener("install", (event) => {
  event.waitUntil(safePrecache().finally(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    try {
      const keys = await caches.keys();
      await Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)));
    } catch (error) {}
    await self.clients.claim();
  })());
});

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  if (!event.request || event.request.method !== "GET") return;
  if (!isSameOrigin(event.request.url)) return;

  const url = new URL(event.request.url);
  if (url.pathname.startsWith("/api/")) return;

  if (event.request.mode === "navigate") {
    event.respondWith((async () => {
      try {
        return await fetch(event.request, { cache: "no-store" });
      } catch (error) {
        const cachedLogin = await caches.match("/login");
        return cachedLogin || offlineResponse();
      }
    })());
    return;
  }

  if (!url.pathname.startsWith("/static/") && url.pathname !== "/manifest.webmanifest" && url.pathname !== "/manifest.json") {
    return;
  }

  event.respondWith((async () => {
    try {
      const cached = await caches.match(event.request);
      if (cached) {
        event.waitUntil((async () => {
          try {
            const fresh = await fetch(event.request, { cache: "no-store" });
            if (fresh && fresh.ok) {
              const cache = await caches.open(CACHE_NAME);
              await cache.put(event.request, fresh.clone());
            }
          } catch (error) {}
        })());
        return cached;
      }

      const response = await fetch(event.request, { cache: "no-store" });
      if (response && response.ok) {
        const cache = await caches.open(CACHE_NAME);
        await cache.put(event.request, response.clone());
      }
      return response;
    } catch (error) {
      const fallback = await caches.match(event.request);
      if (fallback) return fallback;
      return new Response("", { status: 204 });
    }
  })());
});
