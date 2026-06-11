# Salon Karola App

Interne Salon-App fuer Terminplanung, Kundenpflege und betriebliche Tagesablaeufe bei Salon Karola.

## Zweck
Die Anwendung buendelt die wichtigsten internen Arbeitsablaeufe in einer geschuetzten Web- und Android-App:
- Kalender- und Terminverwaltung fuer Admin und Mitarbeitende
- Kundensuche, Kundendetails und Terminhistorie
- E-Mail-Vorlagen, Erinnerungen und Geburtstagsaktionen
- Push-Benachrichtigungen fuer registrierte Geraete
- einfache Betriebs- und Diagnosewerkzeuge fuer den internen Einsatz

## Wichtigste Funktionen
- Monats- und Tageskalender fuer den Salonbetrieb
- getrennte Admin- und Mitarbeiteransicht
- Kontakt- und Kundendatenpflege
- Terminanlage, Terminbearbeitung und Statuspflege
- Mail-Vorlagen fuer Geburtstage und Terminerinnerungen
- Push- und Android-App-Anbindung
- Import-, Backup- und Diagnosehilfen fuer den Betrieb

## Projektstruktur
- `salon_karola/`: neue Paketoberflaeche fuer App-Struktur, Services, Routes und Hilfsimporte
- `salon_karola_legacy.py`: bestehende Laufzeitlogik, aktuell noch zentrale Implementierung
- `templates/`: aktiv genutzte Jinja-Templates der Flask-App
- `static/`: aktiv genutzte Styles, JavaScript-Dateien, Icons und PWA-Ressourcen
- `www/`: Web-Container fuer die Android/Capacitor-App

## Deployment auf Render
1. Repository mit Render verbinden.
2. Persistent Disk anlegen und unter `/var/data` mounten.
3. Environment Variables setzen:
   `SECRET_KEY`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, optional SMTP-, Push- und Firebase-Zugangsdaten.
4. `DATABASE_PATH=/var/data/salon_karola.db` und `BACKUP_DIR=/var/data/backups` setzen.
5. Start Command verwenden:
   `gunicorn app:app --workers 1 --threads 2 --timeout 120`

## Datenschutz und Sicherheit
- Keine Kunden-, Termin- oder Zugangsdaten ins Repository committen.
- `SECRET_KEY` muss in produktionsaehnlichen Umgebungen als Umgebungsvariable gesetzt sein.
- Mail-, Push- und Firebase-Zugangsdaten ausschliesslich ueber sichere Environment Variables verwalten.
- Die App ist fuer den internen Salonbetrieb gedacht und sollte nicht ohne Authentifizierung oeffentlich freigegeben werden.
- Admin-Bereiche wie Backup, Import, Export, Push-Verwaltung und Einstellungen sind fuer den Admin-Account vorgesehen.
- In produktionsaehnlichen Umgebungen sollte die App nur ueber HTTPS betrieben werden, damit `SESSION_COOKIE_SECURE` aktiv bleibt.

## Backup-Hinweise
- Die produktive SQLite-Datenbank muss auf einer Persistent Disk liegen.
- Lokale Datenbankdateien, Exporte und Backups gehoeren nicht ins Repository.
- Vor Importen oder Strukturarbeiten sollte immer ein aktuelles Datenbank-Backup vorhanden sein.
- Standardvorlagen werden nur bei Bedarf erzeugt und nicht bei jedem Start ueberschrieben.

## STRUKTIVA-Referenz
Diese Anwendung wird als interne Salon-Loesung im STRUKTIVA-Umfeld weiterentwickelt: pragmatisch im Alltag, stabil im Betrieb und mit Fokus auf saubere, nachvollziehbare Weiterentwicklung statt schneller Einmal-Loesungen.

## Versionshistorie
Releases und Changelogs: siehe [CHANGELOG.md](CHANGELOG.md) und GitHub Releases.
