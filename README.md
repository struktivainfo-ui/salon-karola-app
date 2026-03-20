# Salon Karola CRM - Version 2 Premium

Diese Version enthält:
- große und übersichtliche Handy-Ansicht
- PWA-Installation mit App-Icon
- Kontakte, Termine, Kalender, Mail-Vorlagen
- CSV-Import / CSV-Export
- Datenbank-Import / Datenbank-Export
- automatische Geburtstagsmails und Terminerinnerungen

## Deployment
- Dateien in GitHub hochladen
- in Render die Persistent Disk unter `/opt/render/project/src/data` nutzen
- Start Command: `gunicorn --workers 1 --bind 0.0.0.0:$PORT wsgi:application`


Version 2.6:
- Tagesstart-Dashboard
- offene Erinnerungen in den nächsten 24 Stunden
- Kalender- und Workflow-Feinschliff
- Versions-Texte bereinigt
