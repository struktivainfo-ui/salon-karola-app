# Kalender-/Admin-Legacy-Audit

Stand: 2026-06-11

## Kurzfazit

- Kanonische Hauptpfade sind derzeit `/admin/dashboard`, `/calendar`, `/appointments`, `/staff/today`, `/admin/settings`, `/database-tools` und `/import`.
- Die eigentliche aktive Kalender-Oberflaeche rendert `templates/calendar_compact.html`.
- Mehrere aeltere Kalender-/Termin-Templates liegen noch im Repo, werden aber im heutigen Hauptfluss nicht mehr direkt gerendert.
- Root-Level-HTML-Dateien fuer Kalender/Termine/Import/Backup werden weder ueber Flask-Templates noch ueber Manifest/Capacitor direkt genutzt und sind deshalb starke Legacy-Kandidaten.
- `www/index.html` bleibt als Android/Capacitor-Shell **UNSICHER** und darf in diesem Schritt nicht entfernt werden.

## Gefundene relevante Template-/HTML-Dateien

- `templates/base.html`
- `templates/admin_base.html`
- `templates/admin_dashboard.html`
- `templates/admin_settings.html`
- `templates/admin_automation.html`
- `templates/admin_calendar.html`
- `templates/admin_appointments.html`
- `templates/admin_start.html`
- `templates/admin_customers.html`
- `templates/admin_customer_detail.html`
- `templates/calendar_compact.html`
- `templates/calendar.html`
- `templates/calendar_simple.html`
- `templates/appointments.html`
- `templates/appointments_simple.html`
- `templates/staff_today.html`
- `templates/staff_new_appointment.html`
- `templates/staff_appointments_center.html`
- `templates/database_tools.html`
- `templates/import.html`
- `calendar.html`
- `appointments.html`
- `database_tools.html`
- `import.html`
- `www/index.html`

## Audit-Tabelle: Templates / HTML-Dateien

| Bereich | Datei / Template | Route / Pfad | Endpoint / Funktion | Verlinkt von | Status | Begruendung | Empfehlung |
|---|---|---|---|---|---|---|---|
| Layout | `templates/base.html` | indirekt | Basislayout | fast alle aktiven Admin-/Staff-Seiten | AKTIV | zentrales Navigations- und App-Layout | behalten |
| Layout | `templates/admin_base.html` | indirekt | Basislayout Admin | aktive Admin-Templates per `extends` | AKTIV | aktives Admin-Layout | behalten |
| Admin | `templates/admin_dashboard.html` | `/admin/dashboard` | `admin_dashboard` | Admin-Navigation, `/dashboard`, `/admin` | AKTIV | heutiger Admin-Hauptscreen | behalten |
| Admin | `templates/admin_settings.html` | `/admin/settings` | `admin_settings` | Admin-Navigation, Dashboard | AKTIV | aktive Einstellungen-Seite | behalten |
| Admin | `templates/admin_automation.html` | `/admin/automation` | `admin_automation` | Admin-Navigation, Dashboard | AKTIV | aktive Admin-Unterseite | behalten |
| Admin | `templates/admin_customers.html` | `/customers/search` im Admin-World-Kontext | `customer_search_page` | Admin-Navigation "Kunden", Dashboard | AKTIV | aktive Admin-Kundenansicht | behalten |
| Admin | `templates/admin_customer_detail.html` | `/customer/<id>` und `/admin/customers/<id>` im Admin-World-Kontext | `customer_detail`, `admin_customer_detail_alias` | aus Kundenlisten | AKTIV | aktive Admin-Detailansicht | behalten |
| Kalender | `templates/calendar_compact.html` | `/calendar` | `calendar_view` | Hauptnavigation, Dashboard, Staff-Links, Alias-Routen | AKTIV | aktueller zentraler Kalender-Hauptscreen | behalten |
| Mitarbeiter | `templates/staff_today.html` | `/staff/today` | `staff_today` | Staff-Navigation, `/salon`, interne Buttons | AKTIV | heutige Mitarbeiter-Heute-Ansicht | behalten |
| Technik | `templates/database_tools.html` | `/database-tools` und `/admin/backup` | `database_tools`, `admin_backup_alias` | Admin-Navigation, Admin-Settings, Admin-Automation | AKTIV | aktive Backup-/Technik-Seite | behalten |
| Technik | `templates/import.html` | `/import` | `import_customers` | Admin-Settings, `salon_home.html` | AKTIV | aktiver CSV-Import-Screen | behalten |
| Kalender | `templates/admin_calendar.html` | keine direkte aktive Render-Route gefunden | - | keine aktive Navigation | LEGACY | fruehere Kalender-Variante; heutiger Hauptscreen ist `calendar_compact.html` | spaeter manuell pruefen, nicht loeschen |
| Termine | `templates/admin_appointments.html` | nur implizit ueber `/appointments?legacy=1` im Admin-Kontext | `appointments_hub` | keine sichtbare Navigation | LEGACY | alte Formular-/Terminlisten-Ansicht, Hauptfluss geht heute ueber `/calendar` | spaeter manuell pruefen |
| Admin | `templates/admin_start.html` | keine aktive Render-Route; `/admin/start` redirectet | `admin_start` | keine aktive Navigation | LEGACY | Alt-Entry-Template nicht mehr direkt genutzt | spaeter manuell pruefen |
| Kalender | `templates/calendar.html` | keine direkte aktive Render-Route gefunden | - | keine aktive Navigation | LEGACY | aeltere Kalender-Variante neben `calendar_compact.html` | spaeter manuell pruefen |
| Kalender | `templates/calendar_simple.html` | keine direkte aktive Render-Route gefunden | - | keine aktive Navigation | LEGACY | vereinfachte Alt-Kalenderansicht ohne aktuellen Render-Pfad | spaeter manuell pruefen |
| Termine | `templates/appointments.html` | keine direkte aktive Render-Route gefunden | - | keine aktive Navigation | LEGACY | alte Terminansicht; heutiger Flow laeuft ueber `/appointments` -> `/calendar` | spaeter manuell pruefen |
| Termine | `templates/appointments_simple.html` | keine direkte aktive Render-Route gefunden | - | keine aktive Navigation | LEGACY | vereinfachte Alt-Terminsicht ohne aktiven Render-Pfad | spaeter manuell pruefen |
| Termine | `templates/staff_new_appointment.html` | nur implizit ueber `/appointments?legacy=1` im Staff-Kontext | `appointments_hub` | keine sichtbare Navigation | LEGACY | alte separate Termin-Erstellungsseite; Staff-Hauptfluss nutzt heute Kalender-Day/Book | spaeter manuell pruefen |
| Termine | `templates/staff_appointments_center.html` | keine direkte aktive Render-Route; `/staff/appointments` redirectet | `staff_appointments_center` | keine aktive Navigation | LEGACY | Alt-Mittelpunkt fuer Termine, heute nur Redirect auf Kalender | spaeter manuell pruefen |
| Root HTML | `calendar.html` | keine Flask-Route, nicht in `templates/` | - | keine Referenz in Manifest/Service Worker gefunden | LEGACY | Root-Level-Duplikat, von Flask nicht als Template genutzt | spaeter manuell pruefen |
| Root HTML | `appointments.html` | keine Flask-Route, nicht in `templates/` | - | keine Referenz in Manifest/Service Worker gefunden | LEGACY | Root-Level-Duplikat, nicht im aktiven Web-Flow | spaeter manuell pruefen |
| Root HTML | `database_tools.html` | keine Flask-Route, nicht in `templates/` | - | keine Referenz in Manifest/Service Worker gefunden | LEGACY | Root-Level-Duplikat zur aktiven Template-Version | spaeter manuell pruefen |
| Root HTML | `import.html` | keine Flask-Route, nicht in `templates/` | - | keine Referenz in Manifest/Service Worker gefunden | LEGACY | Root-Level-Duplikat zur aktiven Template-Version | spaeter manuell pruefen |
| Android / PWA | `www/index.html` | Android/Capacitor WebView-Startdatei | Capacitor Shell | `capacitor.config.json` (`webDir=www`) | UNSICHER | nicht Teil des Flask-Template-Systems, aber klar Teil des Android-Build-Kontexts | behalten, nur separat pruefen |

## Audit-Tabelle: Pfade / Routen

| Bereich | Datei / Template | Route / Pfad | Endpoint / Funktion | Verlinkt von | Status | Begruendung | Empfehlung |
|---|---|---|---|---|---|---|---|
| Kalender | `templates/calendar_compact.html` | `/calendar` | `calendar_view` | Hauptnavigation, Dashboard, Staff-Seiten, Redirect-Ziele | AKTIV | kanonischer Kalender-Hauptpfad | behalten |
| Termine | `templates/calendar_compact.html` als Redirect-Ziel, sonst Alt-Template bei Legacy-Flag | `/appointments` | `appointments_hub` | Hauptnavigation "Termine", Dashboard, viele Kalender-Aktionen | AKTIV | zentraler Termin-Endpunkt; GET leitet heute in den Kalenderfluss um, POST verarbeitet Termin-Aktionen | behalten |
| Mitarbeiter | `templates/staff_today.html` | `/staff/today` | `staff_today` | Staff-Navigation, `/salon` | AKTIV | kanonische Heute-Ansicht fuer Mitarbeiter | behalten |
| Admin | `templates/admin_dashboard.html` | `/admin/dashboard` | `admin_dashboard` | Admin-Navigation, Alias-Routen `/dashboard` und `/admin` | AKTIV | kanonischer Admin-Hauptpfad | behalten |
| Admin | `templates/admin_settings.html` | `/admin/settings` | `admin_settings` | Admin-Navigation, Dashboard | AKTIV | aktive Einstellungen | behalten |
| Technik | `templates/database_tools.html` | `/database-tools` | `database_tools` | Admin-Settings, Admin-Automation, Backup-Link im Menue | AKTIV | kanonischer Backup-/Export-/Importtechnik-Pfad | behalten |
| Technik | `templates/import.html` | `/import` | `import_customers` | Admin-Settings, `salon_home.html` | AKTIV | aktiver CSV-Import-Pfad | behalten |
| Technik | Datei-Download | `/database/export` | `export_database` | `templates/database_tools.html` | AKTIV | aktiver SQLite-Export | behalten |
| Technik | Datei-Download | `/database/backup-zip` | `export_database_zip` | `templates/database_tools.html` | AKTIV | aktiver ZIP-Backup-Export | behalten |
| Technik | Datei-Download | `/export/customers.csv` | `export_customers` | keine direkte sichtbare Verlinkung in geprueften Templates | UNSICHER | Route existiert und ist fachlich plausibel, aber sichtbare Verlinkung im geprueften UI fehlt | manuell pruefen, nicht entfernen |
| Admin Alias | Redirect | `/dashboard` | `dashboard` | keine aktive Navigation; alte Bookmarks wahrscheinlich | ALIAS | redirectet auf `/admin/dashboard` | behalten als Kompatibilitaetspfad |
| Admin Alias | Redirect | `/admin` | `admin_home` | keine aktive Navigation; externer Einstieg moeglich | ALIAS | redirectet auf `/admin/dashboard` | behalten |
| Admin Alias | Redirect | `/admin/start` | `admin_start` | keine aktive Navigation | ALIAS | historischer Einstieg, redirectet auf `/admin/dashboard` | behalten, spaeter neu bewerten |
| Kalender Alias | Redirect | `/admin/calendar` | `admin_calendar_alias` | keine aktive Navigation; Alt-Links moeglich | ALIAS | redirectet auf den kanonischen Kalenderpfad `/calendar` | behalten |
| Termine Alias | Redirect | `/admin/appointments` | `admin_appointments_alias` | keine aktive Navigation; Alt-Links moeglich | ALIAS | redirectet auf `/appointments` | behalten |
| Technik Alias | gleiches Ziel wie aktive Technik-Seite | `/admin/backup` | `admin_backup_alias` | Admin-Navigation "Backup / Technik" | ALIAS | historischer Admin-Pfad, liefert denselben Screen wie `/database-tools` | behalten |
| Mitarbeiter Alias | Redirect | `/staff/calendar` | `staff_calendar` | keine direkte Hauptnavigation | ALIAS | redirectet auf `/calendar` im Staff-Kontext | behalten |
| Mitarbeiter Alias | Redirect | `/staff/appointments` | `staff_appointments_center` | keine direkte Hauptnavigation | ALIAS | redirectet auf `/calendar` im Staff-Kontext | behalten |
| Mitarbeiter Alias | Redirect | `/staff/day/<date>` | `staff_day_view` | keine sichtbare Hauptnavigation | ALIAS | Datums-Alias fuer die Heute-Ansicht | behalten |
| Admin Legacy | `templates/admin_dashboard.html` | `/dashboard-legacy` | `dashboard_legacy` | keine aktive Navigation | LEGACY | alte Dashboard-Variante mit eigener Query-Logik neben modernem Dashboard | spaeter manuell pruefen |
| Termine Legacy | `templates/admin_appointments.html` oder `templates/staff_new_appointment.html` | `/appointments?legacy=1` | `appointments_hub` | keine sichtbaren Links im geprueften UI | LEGACY | expliziter Legacy-Switch fuer alte Formularansichten | spaeter manuell pruefen |

## Navigation / sichtbare Links

Aktive, sichtbare Hauptlinks:

- `templates/base.html` verlinkt fuer Admin auf `/admin/dashboard`, `/calendar`, `/admin/customers`, `/admin/templates`, `/admin/settings`, `/appointments`, `/admin/backup`, `/admin/automation`.
- `templates/base.html` verlinkt fuer Staff auf `/staff/today`, `/calendar`, `/staff/customers`, `/staff/more`.
- `templates/admin_dashboard.html` verlinkt auf `calendar_view`, `appointments_hub`, `customer_search_page`, `admin_settings`, `admin_staff_alias`, `admin_backup_alias`.
- `templates/admin_settings.html` verlinkt auf `database_tools` und `import_customers`.
- `templates/database_tools.html` verlinkt auf `/database/export` und `/database/backup-zip`.

Keine sicheren Link-Korrekturen noetig:

- In den geprueften Navigationen zeigen die sichtbaren Links bereits auf die heute bevorzugten Hauptpfade oder auf bewusst erhaltene Alias-Routen.
- Deshalb wurde in diesem Schritt **kein sichtbarer Link umgebogen**.

## Kleine sichere Korrekturen in diesem Schritt

- Alias-Kommentare in `salon_karola_legacy.py` fuer `/dashboard`, `/admin` und `/admin/backup` ergaenzt.

## Hinweise fuer spaetere Bereinigung

Kandidaten fuer spaetere Entfernung nach manueller Freigabe:

- `templates/admin_calendar.html`
- `templates/admin_appointments.html`
- `templates/admin_start.html`
- `templates/calendar.html`
- `templates/calendar_simple.html`
- `templates/appointments.html`
- `templates/appointments_simple.html`
- `templates/staff_new_appointment.html`
- `templates/staff_appointments_center.html`
- Root-Level-HTML: `calendar.html`, `appointments.html`, `database_tools.html`, `import.html`
- Route `/dashboard-legacy`
- Legacy-Switch `/appointments?legacy=1`

Unsicher und separat pruefen:

- `www/index.html` wegen Android/Capacitor-Kontext
- `/export/customers.csv` wegen vorhandener Route ohne sichtbaren Link im geprueften UI
