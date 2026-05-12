(function () {
  const out = document.getElementById("out");
  const initBtn = document.getElementById("initPushBtn");
  const sendBtn = document.getElementById("sendTestPushBtn");
  const flags = window.__pushFlags || {};
  let busy = false;

  function write(message) {
    if (out) out.textContent = String(message || "");
  }

  function isAndroidWebView() {
    try {
      const ua = String(navigator.userAgent || "");
      return /; wv\)/i.test(ua) || /\bVersion\/[\d.]+\s+Chrome\/[\d.]+\s+Mobile/i.test(ua) || !!window.Capacitor;
    } catch (_error) {
      return false;
    }
  }

  function setBusy(next) {
    busy = !!next;
    if (initBtn) initBtn.disabled = busy;
    if (sendBtn) sendBtn.disabled = busy;
  }

  function toUint8Array(base64Url) {
    const cleaned = String(base64Url || "").trim();
    if (!cleaned) throw new Error("VAPID Public Key fehlt.");
    const padding = "=".repeat((4 - (cleaned.length % 4)) % 4);
    const base64 = (cleaned + padding).replace(/-/g, "+").replace(/_/g, "/");
    const raw = atob(base64);
    const arr = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i += 1) arr[i] = raw.charCodeAt(i);
    return arr;
  }

  async function sf(url, options) {
    if (typeof window.safeFetch !== "function") throw new Error("safeFetch nicht verfügbar.");
    const result = await window.safeFetch(url, Object.assign({
      timeoutMs: 12000,
      credentials: "same-origin",
      cache: "no-store",
    }, options || {}));
    if (!result.ok) throw new Error(result.error || "Anfrage fehlgeschlagen.");
    return result.data || {};
  }

  async function initPush() {
    if (busy) return;
    if (!flags.enabled) {
      write("Push ist serverseitig deaktiviert.");
      return;
    }
    if (isAndroidWebView()) {
      write("Diese Android-App verwendet keine Web-Push-Benachrichtigungen. Für native Push-Benachrichtigungen ist Firebase/FCM erforderlich.");
      return;
    }

    setBusy(true);
    try {
      if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) {
        write("Push wird auf diesem Gerät oder in dieser App-Ansicht nicht unterstützt.");
        return;
      }
      if (!flags.swEnabled) {
        write("Push benötigt Service Worker. Bitte ENABLE_SERVICE_WORKER=true setzen.");
        return;
      }

      const config = await sf("/api/push/public-key");
      if (!config.enabled || !config.public_key) {
        write(config.error || "Push ist serverseitig noch nicht bereit.");
        return;
      }

      const registration = await navigator.serviceWorker.register("/service-worker.js", { updateViaCache: "none" });
      let permission = Notification.permission;
      if (permission === "default") permission = await Notification.requestPermission();
      if (permission === "denied") {
        write("Benachrichtigungen wurden blockiert. Bitte in den App-/Browser-Einstellungen wieder erlauben.");
        return;
      }
      if (permission !== "granted") {
        write("Push konnte nicht aktiviert werden.");
        return;
      }

      let subscription = await registration.pushManager.getSubscription();
      if (!subscription) {
        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: toUint8Array(config.public_key),
        });
      }
      const payload = (typeof subscription.toJSON === "function")
        ? subscription.toJSON()
        : JSON.parse(JSON.stringify(subscription || {}));
      const save = await sf("/api/push/subscribe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ subscription: payload, staff_name: "Sven", device_name: "Testseite" }),
      });
      write("Push aktiv. Geräte: " + (save.device_count || 0));
    } catch (error) {
      write("Push konnte nicht aktiviert werden: " + String(error && error.message ? error.message : error));
    } finally {
      setBusy(false);
    }
  }

  async function sendTestPush() {
    if (busy) return;
    if (!flags.enabled) {
      write("Push ist serverseitig deaktiviert.");
      return;
    }
    if (isAndroidWebView()) {
      write("In der Android-App wird Web-Push nicht verwendet. Teste stattdessen native Firebase/FCM-Benachrichtigungen.");
      return;
    }

    setBusy(true);
    try {
      const result = await sf("/api/push/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ staff_name: "Sven" }),
      });
      write("Test-Push gesendet. Gesendet: " + (result.sent || 0));
    } catch (error) {
      write("Test-Push fehlgeschlagen: " + String(error && error.message ? error.message : error));
    } finally {
      setBusy(false);
    }
  }

  if (initBtn) initBtn.addEventListener("click", initPush);
  if (sendBtn) sendBtn.addEventListener("click", sendTestPush);
})();
