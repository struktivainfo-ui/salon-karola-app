# Auth- und Sicherheits-Audit

Stand: 2026-06-11

## Kurzfazit

- Die App besitzt bereits ein funktionierendes Login mit Session, Admin-Pruefung und optionalen Passkeys.
- Serverseitiger Schutz fuer sensible Admin-Bereiche ist vorhanden und deckt Backup, Import, Export, Einstellungen, Push-Verwaltung und Templates ab.
- Mitarbeiter koennen die Alltagsbereiche nutzen, werden aber bei Admin-Routen auf `/staff/today` zurueckgeleitet.
- Zwei kleine Risiken wurden direkt gehaertet: ungepruefte `next`-Redirects nach dem Login und fehlende explizite Session-Cookie-Flags.

## Gefundene Auth-Funktionen

- `login_required`
- `admin_required`
- `staff_or_admin_required`
- `login_user`
- `login`
- `logout`
- Passwort-Hashing via `generate_password_hash` / `check_password_hash`
- Staff-/Admin-Rollenlogik ueber `staff_users.is_admin` und `ADMIN_STAFF_NAMES`
- WebAuthn / Passkey-Login und Passkey-Registrierung

## Login / Logout / Rollen

- Login erfolgt ueber `/login` mit Namensauswahl (`staff_name`) und Passwort.
- Beim erfolgreichen Login werden Session-Werte fuer `admin_logged_in`, `admin_name`, `staff_name`, `username` und `ui_world` gesetzt.
- Rollen sind derzeit praktisch `Admin` und `Mitarbeiter`; Admin ist aktuell an `staff_name == Sven` gekoppelt.
- Logout erfolgt ueber `/logout`, leert die Session und leitet zur Login-Seite zurueck.

## Schutzstatus

Oeffentlich:

- `/login`
- `/logout`
- `/api/passkeys/auth/options`
- `/api/passkeys/auth/verify`
- `/api/passkeys/status`
- PWA-/Manifest-/Service-Worker-Ressourcen
- statische Assets

Login erforderlich:

- `/calendar`
- `/appointments`
- `/staff/today`
- `/customers/search`
- `/whatsapp`
- Termin- und Kunden-APIs
- Push-Subscribe-/Unsubscribe-APIs

Admin erforderlich:

- `/admin`
- `/admin/dashboard`
- `/admin/calendar`
- `/admin/appointments`
- `/admin/templates`
- `/admin/backup`
- `/admin/settings`
- `/admin/automation`
- `/push`
- `/staff` (Mitarbeiterverwaltung)
- `/templates`
- `/import`
- `/logs`
- `/database-tools`
- `/database/export`
- `/database/backup-zip`
- `/database/backup/<filename>`
- `/export/customers.csv`
- sensible Push-/FCM-Admin-APIs

## Direkt umgesetzte Haertungen

- `safe_next_target(...)` eingefuehrt, damit Login-Redirects nur interne Ziele unterhalb der App akzeptieren.
- `login_required` uebergibt nur noch bereinigte interne `next`-Ziele.
- `login()` verwendet bereinigte `next`-Ziele und faellt sonst sauber auf den Rollen-Standardpfad zurueck.
- `login_user()` leert die bisherige Session vor dem Neuaufbau, damit keine alten Session-Reste erhalten bleiben.
- Session-Cookies explizit gehaertet:
  - `SESSION_COOKIE_HTTPONLY = True`
  - `SESSION_COOKIE_SAMESITE = "Lax"`
  - `SESSION_COOKIE_SECURE = production_like_runtime()`

## Gepruefte Risiken

- Kein hartcodiertes produktives Passwort im Repo gefunden.
- `SECRET_KEY` wird aus `SECRET_KEY` oder `FLASK_SECRET_KEY` geladen.
- In produktionsaehnlichen Umgebungen erzwingt die App einen gesetzten `SECRET_KEY`.
- Lokal bleibt bewusst ein unsicherer Dev-Fallback aktiv; das ist fuer Entwicklung okay, aber nicht fuer Produktion.
- Backup-Downloads pruefen den Zielpfad bereits gegen Path Traversal.
- Import/Restore-Aktionen liegen hinter Admin-Schutz und nutzen Dateiendungspruefung sowie `MAX_CONTENT_LENGTH = 4 MB`.

## Offene Punkte / TODO

- Kein vollstaendiger CSRF-Schutz fuer Formular-POSTs vorhanden; das sollte als separater, kleiner Sicherheitscheck folgen.
- Einige GET-Routen loesen nicht-destruktive, aber sensible Downloads aus; sie sind admin-geschuetzt, koennten spaeter zusaetzlich mit bestaetigtem POST-Flow verfeinert werden.
- Die Rollenlogik ist funktional, aber eng an feste Staff-Namen gekoppelt; fuer diesen Schritt wurde das bewusst nicht umgebaut.
- Login-Fehlermeldungen sind teilweise noch etwas spezifisch (`Benutzer nicht gefunden`, `noch kein Passwort angelegt`); fachlich okay, aber spaeter vereinheitbar.
