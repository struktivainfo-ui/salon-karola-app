# Push-Removal-Report

Stand: 2026-06-11

## Warum Push entfernt wurde

Push-Benachrichtigungen waren in der aktuellen App nicht aktiv und nicht produktiv genutzt. Der sichtbare Push-Bereich hat die Bedienung verkompliziert und zusätzliche Wartungslast erzeugt, ohne einen echten Betriebsnutzen zu liefern.

## Entfernte sichtbare Bereiche

- Push-Kontrollzentrum
- Push-Links in Admin-Navigation und Einstellungen
- Push-Schnellaktionen auf Start- und Verwaltungsseiten
- Push-Hinweise und Aktivierungsbereich im Mitarbeiter-Mehr-Menue
- Push-Testseite im sicheren Startfluss

## Entfernte oder deaktivierte Routen

- `/push` leitet jetzt auf `/admin/settings` weiter
- `/admin/push` leitet jetzt auf `/admin/settings` weiter
- `/test-push` leitet jetzt auf `/safe-start` weiter
- alle `api/push/*`- und `api/fcm/*`-Endpunkte antworten jetzt mit `410 Gone` und einem klaren Hinweis, dass Push nicht Bestandteil der App ist

## Python- und Service-Worker-Anpassungen

- Push-Helfer wurden auf sichere No-Op-Stubs zurueckgestellt
- Termin- und Automationslogik sendet keine Push-Benachrichtigungen mehr
- Service Worker behaelt Cache-/Offline-Funktionen, aber keine Push- oder Notification-Events mehr
- gemeinsame Client-Boot-Schalter in `templates/base.html` deaktivieren jede automatische Push-Initialisierung
- die Datenbanktabelle `push_subscriptions` bleibt bewusst unberuehrt und ungenutzt bestehen

## Abhaengigkeiten

Entfernt:

- `pywebpush`
- `cryptography`
- `google-auth`
- `@capacitor/push-notifications`
- Android-Firebase-Messaging-Anbindung und zugehoerige Service-Klasse

Bewusst behalten:

- bestehende Service-Worker-Grundstruktur fuer PWA-/Cache-Verhalten
- bestehende Datenbankstruktur ohne Migration

## Gepruefte Android-/Capacitor-Stellen

- `package.json`
- `package-lock.json`
- `android/capacitor.settings.gradle`
- `android/app/capacitor.build.gradle`
- `android/app/build.gradle`
- `android/build.gradle`
- `android/app/src/main/AndroidManifest.xml`

## Durchgefuehrte Pruefungen

- `python -m compileall .`
- Flask-App-Import ueber `from app import app`
- lokale Test-Requests fuer Hauptpfade und Redirects
- Pruefung der Redirects fuer `/push`, `/admin/push` und `/test-push`
- Pruefung, dass Push- und FCM-Endpunkte nicht mehr produktiv arbeiten
- Suchlauf auf entfernte Web-, Service-Worker- und Android-Push-Referenzen

## Risiken

- In `templates/base.html` gibt es noch gemeinsame Client-Boot-Logik, die frueher auch Push beruehrt hat; funktional ist Push abgeschaltet, aber dieser Block bleibt ein Kandidat fuer einen spaeteren separaten JS-Aufraeumschritt.
- Android-Build wurde in diesem Schritt nicht komplett als APK/AAB gebaut; die Konfigurationsbereinigung wurde statisch geprueft.

## Daten- und Sicherheits-Hinweis

- keine Kundendaten geaendert
- keine Datenbankmigration ausgefuehrt
- keine `.env`-Dateien geaendert
- keine Zugangsdaten ins Repository geschrieben
