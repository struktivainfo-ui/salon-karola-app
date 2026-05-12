(function () {
  if (window.safeFetch) return;

  window.safeFetch = async function safeFetch(url, options) {
    var opts = options || {};
    var timeoutMs = Number(opts.timeoutMs || 12000);
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    var timer = null;
    if (controller) {
      timer = window.setTimeout(function () { controller.abort(); }, timeoutMs);
    }
    try {
      var fetchOptions = Object.assign({}, opts);
      delete fetchOptions.timeoutMs;
      if (controller) fetchOptions.signal = controller.signal;
      var response = await fetch(url, fetchOptions);
      var text = await response.text();
      var data = null;
      try { data = text ? JSON.parse(text) : null; } catch (_e) {}
      if (!response.ok) {
        return { ok: false, status: response.status, error: (data && (data.error || data.message)) || ("HTTP " + response.status), data: data, text: text };
      }
      return { ok: true, status: response.status, data: data, text: text };
    } catch (error) {
      var message = (error && error.name === "AbortError") ? "Zeitüberschreitung bei der Verbindung." : String(error && error.message ? error.message : error);
      return { ok: false, status: 0, error: message, data: null, text: "" };
    } finally {
      if (timer) window.clearTimeout(timer);
    }
  };
})();
