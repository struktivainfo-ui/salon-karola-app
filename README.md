# Salon Karola CRM

Diese Version enthält:
- optimierte Handy-Ansicht
- PWA-Installation mit App-Icon
- Kontakte, Termine, Kalender und Mail-Vorlagen
- CSV-Import / CSV-Export
- Datenbank-Import / Datenbank-Export
- automatische Geburtstagsmails und Terminerinnerungen
- Push-Benachrichtigungen für Geräte

## Deployment auf Render
- Repository mit GitHub verbinden
- in Render eine Persistent Disk anlegen und unter `/var/data` mounten
- `DATABASE_PATH` auf `/var/data/salon_karola.db` setzen
- Start Command: `gunicorn app:app --workers 1 --threads 2 --timeout 120`
- `SECRET_KEY`, `ADMIN_USERNAME` und `ADMIN_PASSWORD` als Environment Variables setzen

## Persistenz-Hinweis
- Kern-Daten (Kunden, Termine, Vorlagen, Einstellungen) werden in SQLite gespeichert.
- Für Render ist eine Persistent Disk unter `/var/data` zwingend empfohlen, sonst können Daten nach Redeploy/Neustart verloren gehen.
- Vorlagen werden nur beim erstmaligen Fehlen als Standard angelegt und nicht bei jedem Start überschrieben.

## Wichtige Hinweise
- lokale Datenbankdateien, Backups und `.env` gehören nicht ins Git-Repository
- Mail- und Push-Funktionen erst nach dem Setzen der echten Zugangsdaten produktiv nutzen

## Versionshistorie
Releases und Changelogs: siehe [CHANGELOG.md](CHANGELOG.md) und GitHub Releases.
