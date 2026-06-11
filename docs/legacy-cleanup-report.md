# Legacy-Cleanup-Report

Stand: 2026-06-11

## Ziel

Kleiner, sicherer Legacy-Cleanup fuer bereits eingegrenzte Admin-/Kalender-/Altdateien ohne Aenderung aktiver Hauptpfade, ohne Datenbankeingriffe und ohne Risiko fuer Android/PWA.

## Freigaben

Automatisch freigegeben und archiviert wurden nur Kandidaten, die alle folgenden Punkte erfuellten:

- eindeutiger Status `LEGACY`
- keine aktive `render_template`-Nutzung
- keine aktive Verlinkung in Navigation oder JavaScript
- kein Service-Worker-, Manifest- oder Capacitor-Bezug
- aktive Alternative im heutigen Hauptfluss vorhanden
- archivierende statt destruktive Bereinigung

## Archivierte Dateien

Nach `legacy_archive/root_html/` verschoben:

- `calendar.html`
- `appointments.html`
- `database_tools.html`
- `import.html`

Nach `legacy_archive/templates/` verschoben:

- `templates/admin_calendar.html`
- `templates/admin_start.html`
- `templates/calendar_simple.html`
- `templates/appointments_simple.html`
- `templates/staff_appointments_center.html`

## Bewusst beibehaltene Alias-Routen

- `/dashboard`
- `/admin`
- `/admin/start`
- `/admin/calendar`
- `/admin/appointments`
- `/admin/backup`
- `/staff/calendar`
- `/staff/appointments`
- `/staff/day/<date>`

## Bewusst nicht angefasste Dateien / Pfade

- `templates/admin_appointments.html`
- `templates/staff_new_appointment.html`
- `templates/calendar.html`
- `templates/appointments.html`
- Route `/dashboard-legacy`
- Legacy-Flow `/appointments?legacy=1`
- `www/index.html`
- `/export/customers.csv`

## Begruendung pro Entscheidung

- Die archivierten Root-Level-HTML-Dateien waren keine aktiven Flask-Templates und hatten keine aktive Web-, PWA- oder Android-Referenz.
- Die archivierten Template-Dateien hatten keine aktuelle Render-Route und keine sichtbare Verlinkung im aktiven App-Fluss.
- Die nicht angefassten Legacy-Dateien haengen entweder noch an indirekten Legacy-Flows oder sind fuer die aktuelle Freigabelogik nicht eindeutig genug abgesichert.
- Alias-Routen bleiben bewusst erhalten, damit alte Bookmarks und historische Einstiege weiter sauber auf Hauptpfade fuehren.

## Naechste Schritte

- Separater Entscheidungs-Schritt fuer die verbleibenden Legacy-Templates `templates/calendar.html`, `templates/appointments.html`, `templates/admin_appointments.html` und `templates/staff_new_appointment.html`
- Danach gezielter CSRF-/POST-Sicherheitscheck fuer sensible Formulare und Admin-Aktionen

## Kontrollierter git-rm-Schritt

Entfernt am 2026-06-11:

- `legacy_archive/templates/admin_start.html`

Warum entfernt:

- bereits archivierte Legacy-Datei ohne aktive Flask-Render-Nutzung
- Route `/admin/start` bleibt erhalten, rendert die Datei aber nicht mehr, sondern redirectet auf `/admin/dashboard`
- keine Referenz in JavaScript, Service Worker, Manifest, PWA, Android/Capacitor oder aktiven Templates gefunden
- moderne Hauptpfade fuer Admin und Mitarbeiter sind vorhanden

Durchgefuehrte Pruefungen:

- `git status`
- Referenzsuche in Python, `render_template`, Templates, JavaScript, Service Worker, Manifest, `www`, Android und Capacitor
- `python -m compileall .`
- Flask-Testclient fuer Hauptpfade `Heute`, `Kalender`, `Termine`, `Kunden`, `Vorlagen`, `Admin`
- erneute Kontrolle von `git status` und Cache-/Manifest-Referenzen

Sicherheits- und Datenhinweis:

- keine Kundendaten geaendert
- keine Datenbank geaendert
- keine `.env`-Dateien geaendert
