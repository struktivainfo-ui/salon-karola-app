# Salon Karola CRM

Diese Version enth?lt:
- optimierte Handy-Ansicht
- PWA-Installation mit App-Icon
- Kontakte, Termine, Kalender und Mail-Vorlagen
- CSV-Import / CSV-Export
- Datenbank-Import / Datenbank-Export
- automatische Geburtstagsmails und Terminerinnerungen
- Push-Benachrichtigungen f?r Ger?te

## Deployment auf Render
- Repository mit GitHub verbinden
- in Render eine Persistent Disk anlegen und unter `/var/data` mounten
- `DATABASE_PATH` auf `/var/data/salon_karola.db` setzen
- Start Command: `gunicorn --workers 1 --bind 0.0.0.0:$PORT wsgi:application`
- `SECRET_KEY`, `ADMIN_USERNAME` und `ADMIN_PASSWORD` als Environment Variables setzen

## Wichtige Hinweise
- lokale Datenbankdateien, Backups und `.env` geh?ren nicht ins Git-Repository
- Mail- und Push-Funktionen erst nach dem Setzen der echten Zugangsdaten produktiv nutzen
