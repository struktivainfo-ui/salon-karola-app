const CACHE_NAME = "salon-karola-v7-2-calendar-push-2026-04-08";
const STATIC_URLS = [
  "/static/style.css",
  "/static/icon-192.png",
  "/static/icon-512.png",
  "/static/push-icon.png",
  "/static/push-badge.png",
  "/manifest.webmanifest",
  "/static/sounds/start-chime.wav"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_URLS)));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)));
    await self.clients.claim();
  })());
});

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  const url = new URL(event.request.url);

  if (event.request.mode === "navigate" || url.pathname === "/" || url.pathname === "/calendar" || url.pathname === "/database-tools" || url.pathname === "/templates" || url.pathname === "/appointments" || url.pathname === "/whatsapp") {
    event.respondWith(
      fetch(event.request, { cache: "no-store" }).catch(() => caches.match("/login"))
    );
    return;
  }

  if (!url.pathname.startsWith("/static/") && url.pathname !== "/manifest.webmanifest") {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((response) => {
      if (response) return response;
      return fetch(event.request).then((networkResponse) => {
        const copy = networkResponse.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return networkResponse;
      });
    })
  );
});

self.addEventListener("push", (event) => {
  let data = { title: "Salon Karola", body: "Neuer Termin", url: "/calendar" };
  try {
    if (event.data) data = Object.assign(data, event.data.json());
  } catch (e) {}

  event.waitUntil((async () => {
    const all = await self.registration.getNotifications();
    const targetTag = data.tag || `push-${Date.now()}`;
    all.filter((n) => n.tag === targetTag).forEach((n) => n.close());
    return self.registration.showNotification(data.title || "Salon Karola", {
      body: data.body || "Neue Benachrichtigung",
      icon: "/static/push-icon.png",
      badge: "/static/push-badge.png",
      data: { url: data.url || "/calendar" },
      tag: targetTag,
      renotify: true,
      requireInteraction: false,
    });
  })());
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || "/calendar";
  event.waitUntil(clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
    for (const client of clientList) {
      if ("focus" in client) {
        client.navigate(targetUrl);
        return client.focus();
      }
    }
    if (clients.openWindow) {
      return clients.openWindow(targetUrl);
    }
  }));
});
