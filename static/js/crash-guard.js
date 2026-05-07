(function () {
  if (window.__skCrashGuardInstalled) return;
  window.__skCrashGuardInstalled = true;

  function safeStore(message) {
    try {
      if (!window.localStorage) return;
      var key = "sk_last_runtime_errors";
      var list = [];
      try {
        list = JSON.parse(localStorage.getItem(key) || "[]");
        if (!Array.isArray(list)) list = [];
      } catch (_error) {
        list = [];
      }
      list.unshift(String(message || "Unbekannter Fehler"));
      if (list.length > 10) list.length = 10;
      localStorage.setItem(key, JSON.stringify(list));
    } catch (_error) {}
  }

  function handle(source, value) {
    var text = "[" + source + "] " + (value && value.message ? value.message : String(value || "Unbekannter Fehler"));
    try { console.error("Salon Karola Crash Guard:", text, value); } catch (_error) {}
    safeStore(text);
  }

  window.addEventListener("error", function (event) {
    handle("window.onerror", event && (event.error || event.message));
  });

  window.addEventListener("unhandledrejection", function (event) {
    handle("unhandledrejection", event && event.reason);
  });
})();
