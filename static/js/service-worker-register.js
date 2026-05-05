(function () {
  var out = document.getElementById("out");
  var registerBtn = document.getElementById("registerBtn");
  var unregisterBtn = document.getElementById("unregisterBtn");
  var clearCacheBtn = document.getElementById("clearCacheBtn");

  function write(message) {
    if (out) out.textContent = String(message || "");
  }

  async function registerWorker() {
    if (!("serviceWorker" in navigator)) {
      write("Fehler: Service Worker wird nicht unterstuetzt.");
      return;
    }
    try {
      var reg = await navigator.serviceWorker.register("/service-worker.js", { updateViaCache: "none" });
      write("OK: Service Worker registriert.\nScope: " + reg.scope);
    } catch (error) {
      write("Fehler bei Registrierung: " + String(error));
    }
  }

  async function unregisterWorker() {
    try {
      if (!("serviceWorker" in navigator)) {
        write("Service Worker nicht verfuegbar.");
        return;
      }
      var regs = await navigator.serviceWorker.getRegistrations();
      await Promise.all(regs.map(function (r) { return r.unregister().catch(function () { return false; }); }));
      write("OK: " + regs.length + " Service Worker deregistriert.");
    } catch (error) {
      write("Fehler beim Deregistrieren: " + String(error));
    }
  }

  async function clearCaches() {
    try {
      if (!("caches" in window)) {
        write("Cache API nicht verfuegbar.");
        return;
      }
      var keys = await caches.keys();
      await Promise.all(keys.map(function (k) { return caches.delete(k).catch(function () { return false; }); }));
      write("OK: " + keys.length + " Cache(s) geloescht.");
    } catch (error) {
      write("Fehler beim Cache-Loeschen: " + String(error));
    }
  }

  if (registerBtn) registerBtn.addEventListener("click", registerWorker);
  if (unregisterBtn) unregisterBtn.addEventListener("click", unregisterWorker);
  if (clearCacheBtn) clearCacheBtn.addEventListener("click", clearCaches);
})();
