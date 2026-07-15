const fs = require("fs");
const path = require("path");

function filesUnder(root, extensions) {
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) return filesUnder(fullPath, extensions);
    return extensions.includes(path.extname(entry.name)) ? [fullPath] : [];
  });
}

const browserFiles = [
  ...filesUnder("templates", [".html"]),
  ...filesUnder(path.join("static", "js"), [".js"]),
];
const failures = [];

for (const file of browserFiles) {
  const text = fs.readFileSync(file, "utf8");
  if (/rel=["']manifest["']/i.test(text)) failures.push(`${file}: manifest link found`);
  if (/serviceWorker\.register\s*\(/.test(text)) failures.push(`${file}: service worker registration found`);
  if (/deferredPrompt|__skBonuscardSavePrompt|\.prompt\s*\(/.test(text)) failures.push(`${file}: saved install prompt found`);
  if (file !== path.join("static", "js", "pwa-cleanup.js") && /beforeinstallprompt/i.test(text)) {
    failures.push(`${file}: install prompt listener found outside cleanup blocker`);
  }
  if (/App installieren|Zum Startbildschirm|Zum Home-Bildschirm|Kundenkarte (?:auf dem Handy )?speichern/i.test(text)) {
    failures.push(`${file}: install UI text found`);
  }
}

const cleanup = fs.readFileSync(path.join("static", "js", "pwa-cleanup.js"), "utf8");
for (const required of [
  "beforeinstallprompt",
  "event.preventDefault()",
  "navigator.serviceWorker.getRegistrations()",
  "registration.unregister()",
  "caches.keys()",
  "caches.delete(key)",
]) {
  if (!cleanup.includes(required)) failures.push(`pwa-cleanup.js: missing ${required}`);
}

const app = fs.readFileSync("salon_karola_legacy.py", "utf8");
if (!app.includes("ENABLE_SERVICE_WORKER = False")) failures.push("service worker feature is not hard-disabled");
if (!/Response\("\{\}\\n", status=410/.test(app)) failures.push("manifest endpoint is not disabled");

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log("PWA installability disabled check passed");
