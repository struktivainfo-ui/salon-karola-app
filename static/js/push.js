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
      var res = await fetch("/api/push/public-key", { cache: "no-store", credentials: "same-origin" });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || data.ok === false) {
        write("Fehler beim Laden der Push-Konfiguration: HTTP " + res.status);
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
      var res = await fetch("/api/push/ping?staff_name=Sven", { cache: "no-store", credentials: "same-origin" });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok || data.ok === false) {
        write("Fehler beim Test-Push: " + (data.error || ("HTTP " + res.status)));
        return;
      }
      write("OK: Test-Push ausgefuehrt.\nGesendet: " + (((data.result || {}).sent) || 0) + "\nGeraete: " + (data.device_count || 0));
    } catch (error) {
      write("Fehler beim Senden: " + String(error));
    }
  }

  if (initBtn) initBtn.addEventListener("click", initPush);
  if (sendBtn) sendBtn.addEventListener("click", sendTestPush);
})();
