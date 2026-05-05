(function () {
  if (window.__safeStartJsLoaded) return;
  window.__safeStartJsLoaded = true;
  try {
    var info = document.getElementById("safeInfo");
    if (info && !info.textContent) {
      info.textContent = "Safe-Start Kontrollzentrale bereit.";
    }
  } catch (error) {
    console.error("safe-start.js", error);
  }
})();
