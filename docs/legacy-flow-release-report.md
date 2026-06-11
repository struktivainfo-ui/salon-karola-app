# Legacy-Flow-Release-Report

Stand: 2026-06-11

## Gepruefte Flows

- `/appointments?legacy=1`
- `/dashboard-legacy`

## Gefundene Referenzen

### `/appointments?legacy=1`

- Der Query-Parameter `legacy=1` wurde nur in `appointments_hub()` ausgewertet.
- Aktive Navigation und Buttons verlinken `appointments_hub`, aber ohne `legacy=1`.
- Keine JavaScript-, Service-Worker-, Manifest- oder Capacitor-Referenz auf `legacy=1` gefunden.
- Moderne Alternative ist der normale `/appointments`-Einstieg in den aktuellen Kalender-Flow.

### `/dashboard-legacy`

- Es existierte eine eigene Route mit alter Query-/Listenlogik.
- Es wurde keine aktive Verlinkung auf `/dashboard-legacy` gefunden.
- Keine JavaScript-, Service-Worker-, Manifest- oder Capacitor-Referenz gefunden.
- Moderne Alternative ist `/admin/dashboard`.

## Entscheidung pro Flow

- `/appointments?legacy=1`: nicht mehr als eigenstaendigen Legacy-Template-Flow pflegen; stattdessen in den modernen Hauptpfad aufloesen.
- `/dashboard-legacy`: als Kompatibilitaets-URL behalten, aber nur noch auf das kanonische Admin-Dashboard redirecten.

## Freigabeentscheidung

- `/appointments?legacy=1`: `JA`
  Grund: keine aktive Referenz, klare moderne Alternative, geringe Umstellung auf Hauptpfad.
- `/dashboard-legacy`: `JA`
  Grund: keine aktive Referenz, klare moderne Alternative, sicherer Redirect statt alter Speziallogik.

## Durchgefuehrte Massnahmen

- `appointments_hub()` behandelt `GET /appointments` jetzt immer als modernen Kalender-Einstieg, unabhaengig von `legacy=1`.
- `dashboard_legacy()` ist jetzt ein reiner Redirect auf `admin_dashboard`.

## Nicht angefasste Risiken

- `templates/admin_appointments.html` und `templates/staff_new_appointment.html` bleiben vorerst im aktiven Template-Ordner, obwohl der Legacy-GET-Flow sie nicht mehr direkt erreicht.
- `templates/calendar.html` und `templates/appointments.html` bleiben ebenfalls unangetastet.
- Die naechste geplante Pruefung ist bewusst **nicht** hier enthalten: separater CSRF-/POST-Sicherheitscheck.
