(function () {
  var out = document.getElementById("out");
  var initBtn = document.getElementById("initPushBtn");
  var sendBtn = document.getElementById("sendTestPushBtn");
  var flags = window.__pushFlags || {};

  function write(message) {
    if (out) out.textContent = String(message || "");
  }

  async function initPush() {
    if (!flags.enabled) {
      write("Push ist deaktiviert. Bitte ENABLE_PUSH=true setzen.");
      return;
    }
    try {
      if (typeof window.safeFetch !== "function") {
        write("Fehler: safeFetch nicht verfügbar.");
        return;
      }
      var result = await window.safeFetch("/api/push/public-key", { cache: "no-store", credentials: "same-origin", timeoutMs: 12000 });
      if (!result.ok) {
        write("Fehler beim Laden der Push-Konfiguration: " + result.error);
        return;
      }
      var data = result.data || {};
      if (data.ok === false) {
        write("Fehler beim Laden der Push-Konfiguration: " + (data.error || "Unbekannter Fehler"));
        return;
      }
      if (data.service_worker_required && !data.service_worker_enabled) {
        write("Push benötigt Service Worker. Bitte ENABLE_SERVICE_WORKER=true setzen.");
        return;
      }
      write("OK: Push-Konfiguration geladen.\nWebPush: " + (data.enabled ? "aktiv" : "inaktiv") + "\nFirebase: " + (data.native_enabled ? "aktiv" : "inaktiv"));
    } catch (error) {
      write("Fehler bei Push-Initialisierung: " + String(error));
    }
  }

  async function sendTestPush() {
    if (!flags.enabled) {
      write("Push ist deaktiviert. Bitte ENABLE_PUSH=true setzen.");
      return;
    }
    try {
      if (typeof window.safeFetch !== "function") {
        write("Fehler: safeFetch nicht verfügbar.");
        return;
      }
      var result = await window.safeFetch("/api/push/ping?staff_name=Sven", { cache: "no-store", credentials: "same-origin", timeoutMs: 12000 });
      var data = result.data || {};
      if (!result.ok || data.ok === false) {
        write("Fehler beim Test-Push: " + (data.error || result.error || "Unbekannter Fehler"));
        return;
      }
      write("OK: Test-Push ausgeführt.\nGesendet: " + (((data.result || {}).sent) || 0) + "\nGeräte: " + (data.device_count || 0));
    } catch (error) {
      write("Fehler beim Senden: " + String(error));
    }
  }

  if (initBtn) initBtn.addEventListener("click", initPush);
  if (sendBtn) sendBtn.addEventListener("click", sendTestPush);
})();
