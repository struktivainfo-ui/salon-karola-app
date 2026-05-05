(function () {
  if (window.__loginJsLoaded) return;
  window.__loginJsLoaded = true;
  try {
    var passwordInput = document.getElementById("password");
    if (passwordInput) passwordInput.setAttribute("autocapitalize", "off");
  } catch (error) {
    console.error("login.js", error);
  }
})();
