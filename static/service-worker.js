const CACHE_NAME = "salon-karola-__APP_VERSION__";
const STATIC_ASSETS = [
  "/manifest.webmanifest",
  "/static/style.css",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png",
  "/static/icons/apple-touch-icon.png",
  "/static/push-icon.png",
  "/static/push-badge.png",
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

  if (!url.pathname.startsWith("/static/") && url.pathname !== "/manifest.webmanifest") {
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

self.addEventListener("push", (event) => {
  event.waitUntil((async () => {
    try {
      let data = { title: "Salon Karola", body: "Neue Benachrichtigung", url: "/calendar" };
      try {
        if (event.data) data = Object.assign(data, event.data.json());
      } catch (error) {}
      await self.registration.showNotification(data.title || "Salon Karola", {
        body: data.body || "Neue Nachricht",
        icon: "/static/push-icon.png",
        badge: "/static/push-badge.png",
        data: { url: data.url || "/calendar" },
        tag: data.tag || `push-${Date.now()}`,
        renotify: true,
        requireInteraction: false,
      });
    } catch (error) {}
  })());
});

self.addEventListener("notificationclick", (event) => {
  event.waitUntil((async () => {
    try {
      event.notification.close();
      const targetUrl = (event.notification.data && event.notification.data.url) || "/calendar";
      const clientsList = await clients.matchAll({ type: "window", includeUncontrolled: true });
      for (const client of clientsList) {
        if (client.url && client.url.startsWith(self.location.origin)) {
          try {
            if ("focus" in client) await client.focus();
            if ("navigate" in client) await client.navigate(targetUrl);
            return;
          } catch (error) {}
        }
      }
      if (clients.openWindow) await clients.openWindow(targetUrl);
    } catch (error) {}
  })());
});
