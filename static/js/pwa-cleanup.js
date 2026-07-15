(function () {
  "use strict";

  window.addEventListener("beforeinstallprompt", function (event) {
    event.preventDefault();
  });

  async function cleanupLegacyPwaState() {
    var result = { registrations: 0, caches: 0 };

    try {
      if ("serviceWorker" in navigator) {
        var registrations = await navigator.serviceWorker.getRegistrations();
        result.registrations = registrations.length;
        await Promise.all(registrations.map(function (registration) {
          return registration.unregister().catch(function () { return false; });
        }));
      }
    } catch (_error) {}

    try {
      if ("caches" in window) {
        var cacheKeys = await caches.keys();
        result.caches = cacheKeys.length;
        await Promise.all(cacheKeys.map(function (key) {
          return caches.delete(key).catch(function () { return false; });
        }));
      }
    } catch (_error) {}

    return result;
  }

  window.__salonKarolaPwaCleanup = cleanupLegacyPwaState;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", cleanupLegacyPwaState, { once: true });
  } else {
    cleanupLegacyPwaState();
  }
})();
