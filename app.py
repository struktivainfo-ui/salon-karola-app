
import base64
import csv
import io
import os
import re
import shutil
import smtplib
import sqlite3
import tempfile
import zipfile
import json
import calendar as pycalendar
import requests
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
)

try:
    from pywebpush import WebPushException, webpush
except Exception:
    WebPushException = Exception
    webpush = None

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "salon_karola.db")))
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Berlin")
os.environ.setdefault("TZ", APP_TIMEZONE)
try:
    import time as _time
    if hasattr(_time, "tzset"):
        _time.tzset()
except Exception:
    pass

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.secret_key = os.getenv("SECRET_KEY", "salon-karola-ultra-secret")
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024


@app.after_request
def add_no_cache_headers(response):
    try:
        if request.path in ["/", "/login", "/calendar", "/database-tools", "/templates", "/admin/reset-birthdate-template"] or response.mimetype in {"text/html", "application/javascript", "application/manifest+json"}:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
    except Exception:
        pass
    return response

APP_VERSION = "Salon Karola CRM v7.1 Stable Logic Final - Clean Fix"
STAFF_OPTIONS = ["Alle", "Ute", "Jessi"]
MANUAL_PLACEHOLDER_LASTNAME = "__MANUELLER_TERMIN__"
MANUAL_PLACEHOLDER_FIRSTNAME = "Versteckter Kontakt"
AUTO_BACKUP_KEEP = int(os.getenv("AUTO_BACKUP_KEEP", "21"))

scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)
AUTOMATION_MIN_INTERVAL_SECONDS = int(os.getenv("AUTOMATION_MIN_INTERVAL_SECONDS", "300"))


# ---------- Auth ----------
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


# ---------- Database helpers ----------
def get_db():
    if "db" not in g:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def get_setting(key, default=""):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_setting(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO app_settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()


def visible_customer_condition(alias="c"):
    prefix = f"{alias}." if alias else ""
    return f"COALESCE({prefix}_name, '') <> '{MANUAL_PLACEHOLDER_LASTNAME}'"


def ensure_manual_placeholder_customer(conn):
    row = conn.execute(
        "SELECT _id FROM _Customers WHERE COALESCE(_name, '') = ? LIMIT 1",
        (MANUAL_PLACEHOLDER_LASTNAME,),
    ).fetchone()
    if row:
        return row[0] if not isinstance(row, sqlite3.Row) else row["_id"]
    cur = conn.execute(
        """
        INSERT INTO _Customers(_name, _firstname, _mail, _birthdate, _notes, Customer_Adresse, Customer_PersönlichesTelefon, Customer_Mobiltelefon, Customer_Postleitzahl, Customer_Stadt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            MANUAL_PLACEHOLDER_LASTNAME,
            MANUAL_PLACEHOLDER_FIRSTNAME,
            None,
            None,
            "Interner technischer Platzhalter für manuell erfasste Termine.",
            "",
            "",
            "",
            "",
            "",
        ),
    )
    return cur.lastrowid


def acquire_automation_lock(ttl_seconds=120):
    now = datetime.now()
    lock_until = (now + timedelta(seconds=ttl_seconds)).isoformat(timespec="seconds")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", ("automation:lock_until",)).fetchone()
        existing_until = _parse_dt_safe(row[0]) if row and row[0] else None
        if existing_until and existing_until > now:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO app_settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("automation:lock_until", lock_until),
        )
        conn.commit()
        return True


def release_automation_lock():
    try:
        set_setting("automation:lock_until", "")
    except Exception:
        pass

def table_columns(table_name):
    try:
        rows = get_db().execute(f'PRAGMA table_info("{table_name}")').fetchall()
        return [row[1] for row in rows]
    except Exception:
        return []


def customer_columns():
    return set(table_columns("_Customers"))


def customer_has_column(column_name):
    return column_name in customer_columns()


def customer_contact_select_sql():
    cols = customer_columns()
    email_sql = "COALESCE(_mail, '')" if "_mail" in cols else "''"
    mobile_sql = "COALESCE(Customer_Mobiltelefon, '')" if "Customer_Mobiltelefon" in cols else "''"
    phone_sql = "COALESCE(Customer_PersönlichesTelefon, '')" if "Customer_PersönlichesTelefon" in cols else "''"
    city_sql = "COALESCE(Customer_Stadt, '')" if "Customer_Stadt" in cols else "''"
    return email_sql, mobile_sql, phone_sql, city_sql




# ---------- Setup ----------
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _Customers (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                _name TEXT,
                _firstname TEXT,
                _mail TEXT,
                _birthdate TEXT,
                _notes TEXT,
                Customer_Adresse TEXT,
                Customer_PersönlichesTelefon TEXT,
                Customer_Mobiltelefon TEXT,
                Customer_Postleitzahl TEXT,
                Customer_Stadt TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                appointment_at TEXT NOT NULL,
                notes TEXT,
                reminder_hours INTEGER DEFAULT 24,
                reminder_sent_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES _Customers(_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                email_type TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                recipient TEXT,
                sent_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS customer_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(customer_id, tag),
                FOREIGN KEY(customer_id) REFERENCES _Customers(_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL UNIQUE,
                subscription_json TEXT NOT NULL,
                staff_name TEXT NOT NULL DEFAULT 'Ute',
                device_name TEXT,
                user_agent TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_at TEXT,
                last_success_at TEXT,
                last_error TEXT,
                last_test_at TEXT,
                fail_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                display_name TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _MailTemplates (
                id TEXT PRIMARY KEY,
                subject TEXT NOT NULL,
                body TEXT NOT NULL
            )
            """
        )

        admin_user = os.getenv("ADMIN_USERNAME", "karola")
        admin_password = os.getenv("ADMIN_PASSWORD", "Karola123!")

        exists = conn.execute("SELECT 1 FROM staff_users WHERE username = ?", (admin_user,)).fetchone()
        if not exists:
            conn.execute(
                """
                INSERT INTO staff_users(username, password, display_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    admin_user,
                    admin_password,
                    "Salon Karola Admin",
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )

        if not conn.execute("SELECT 1 FROM _MailTemplates WHERE id='appointment'").fetchone():
            conn.execute(
                "INSERT INTO _MailTemplates(id, subject, body) VALUES (?, ?, ?)",
                (
                    "appointment",
                    "Terminerinnerung für {name}",
                    "Hallo {name},\n\nwir erinnern dich an deinen Termin am {termin}.\n\nHerzliche Grüße\nSalon Karola",
                ),
            )

        if not conn.execute("SELECT 1 FROM _MailTemplates WHERE id='birthdate'").fetchone():
            conn.execute(
                "INSERT INTO _MailTemplates(id, subject, body) VALUES (?, ?, ?)",
                (
                    "birthdate",
                    "Alles Gute zum Geburtstag, {vorname}! 🎉",
                    "Liebe/r {name},\n\ndas Team vom Salon Karola wünscht dir einen wunderschönen Geburtstag und freut sich auf deinen nächsten Besuch.\n\nHerzliche Grüße\nSalon Karola",
                ),
            )

        def add_column_if_missing(table_name, column_name, column_sql, *, fill_sql=None):
            existing = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
            if column_name in existing:
                return
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
            if fill_sql:
                conn.execute(fill_sql)

        add_column_if_missing("_Customers", "_mail", "TEXT")
        add_column_if_missing("_Customers", "_birthdate", "TEXT")
        add_column_if_missing("_Customers", "_notes", "TEXT")
        add_column_if_missing("_Customers", "Customer_Adresse", "TEXT")
        add_column_if_missing("_Customers", "Customer_PersönlichesTelefon", "TEXT")
        add_column_if_missing("_Customers", "Customer_Mobiltelefon", "TEXT")
        add_column_if_missing("_Customers", "Customer_Postleitzahl", "TEXT")
        add_column_if_missing("_Customers", "Customer_Stadt", "TEXT")
        add_column_if_missing(
            "_Customers",
            "created_at",
            "TEXT",
            fill_sql="UPDATE _Customers SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL OR created_at = ''",
        )

        add_column_if_missing("appointments", "status", "TEXT DEFAULT 'geplant'")
        add_column_if_missing("appointments", "staff_name", "TEXT DEFAULT 'Ute'")
        add_column_if_missing("appointments", "created_by", "TEXT DEFAULT ''")
        add_column_if_missing(
            "appointments",
            "updated_at",
            "TEXT",
            fill_sql="UPDATE appointments SET updated_at = COALESCE(created_at, CURRENT_TIMESTAMP) WHERE updated_at IS NULL OR updated_at = ''",
        )
        add_column_if_missing("appointments", "manual_firstname", "TEXT DEFAULT ''")
        add_column_if_missing("appointments", "manual_lastname", "TEXT DEFAULT ''")
        add_column_if_missing("appointments", "manual_phone", "TEXT DEFAULT ''")
        add_column_if_missing("appointments", "manual_email", "TEXT DEFAULT ''")

        ensure_manual_placeholder_customer(conn)
        conn.commit()


def ensure_default_admin(force_reset=False):
    admin_user = (os.getenv("ADMIN_USERNAME") or "admin").strip() or "admin"
    admin_password = os.getenv("ADMIN_PASSWORD") or "1234"
    display_name = os.getenv("ADMIN_DISPLAY_NAME") or "Salon Karola Admin"

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                display_name TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

        row = conn.execute("SELECT id FROM staff_users WHERE username = ?", (admin_user,)).fetchone()
        if row and force_reset:
            conn.execute(
                "UPDATE staff_users SET password = ?, display_name = ? WHERE username = ?",
                (admin_password, display_name, admin_user),
            )
        elif not row:
            conn.execute(
                """
                INSERT INTO staff_users(username, password, display_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (admin_user, admin_password, display_name, datetime.now().isoformat(timespec="seconds")),
            )

        fallback_user = "admin"
        fallback_password = "1234"
        fallback_row = conn.execute("SELECT id FROM staff_users WHERE username = ?", (fallback_user,)).fetchone()
        if fallback_row:
            conn.execute(
                "UPDATE staff_users SET password = ?, display_name = ? WHERE username = ?",
                (fallback_password, "Salon Karola Admin", fallback_user),
            )
        else:
            conn.execute(
                """
                INSERT INTO staff_users(username, password, display_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (fallback_user, fallback_password, "Salon Karola Admin", datetime.now().isoformat(timespec="seconds")),
            )
        conn.commit()


# ---------- Mail ----------
def email_delivery_mode():
    requested = (os.getenv("EMAIL_PROVIDER") or os.getenv("MAIL_PROVIDER") or "auto").strip().lower()
    if requested in {"resend", "smtp"}:
        return requested
    if (os.getenv("RESEND_API_KEY") or "").strip():
        return "resend"
    return "smtp"


def resend_ready():
    api_key = (os.getenv("RESEND_API_KEY") or "").strip()
    sender = (os.getenv("RESEND_FROM") or os.getenv("SMTP_SENDER") or "").strip()
    return bool(api_key and sender)


def smtp_ready():
    needed = ["SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_SENDER"]
    return all((os.getenv(k) or "").strip() for k in needed)


def mail_ready():
    mode = email_delivery_mode()
    if mode == "resend":
        return resend_ready()
    return smtp_ready()


def send_email_via_resend(to_email, subject, body):
    api_key = (os.getenv("RESEND_API_KEY") or "").strip()
    sender = (os.getenv("RESEND_FROM") or "").strip()
    reply_to = (os.getenv("RESEND_REPLY_TO") or "").strip()

    if not api_key:
        raise RuntimeError("Resend ist nicht vollständig konfiguriert. RESEND_API_KEY fehlt.")
    if not sender:
        raise RuntimeError("Resend ist nicht vollständig konfiguriert. RESEND_FROM fehlt.")

    payload = {
        "from": sender,
        "to": [to_email],
        "subject": subject,
        "text": body,
    }
    if reply_to:
        payload["reply_to"] = reply_to

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "SalonKarolaApp/3.4.1",
    }

    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Resend Fehler: HTTP {response.status_code} {response.text}")
        return response.json()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Resend Netzwerkfehler: {exc}") from exc


def send_email_via_smtp(to_email, subject, body):
    host = (os.getenv("SMTP_HOST") or "").strip()
    port = int((os.getenv("SMTP_PORT") or "587").strip())
    username = (os.getenv("SMTP_USERNAME") or "").strip()
    password = os.getenv("SMTP_PASSWORD") or ""
    sender = (os.getenv("SMTP_SENDER") or username or "").strip()
    use_tls = (os.getenv("SMTP_USE_TLS", "true") or "true").lower() == "true"
    use_ssl = (os.getenv("SMTP_USE_SSL", "false") or "false").lower() == "true"

    if not all([host, port, username, password, sender]):
        raise RuntimeError("SMTP ist nicht vollständig konfiguriert. Bitte SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD und SMTP_SENDER prüfen.")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, timeout=25) as server:
                server.ehlo()
                server.login(username, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=25) as server:
                server.ehlo()
                if use_tls:
                    server.starttls()
                    server.ehlo()
                server.login(username, password)
                server.send_message(msg)
    except smtplib.SMTPAuthenticationError as exc:
        raise RuntimeError("SMTP Anmeldung fehlgeschlagen. Benutzername oder Passwort stimmen nicht.") from exc
    except smtplib.SMTPConnectError as exc:
        raise RuntimeError("SMTP Verbindung fehlgeschlagen. Host oder Port bitte prüfen.") from exc
    except OSError as exc:
        raise RuntimeError(f"SMTP Netzwerkfehler: {exc}") from exc


def send_email(to_email, subject, body):
    mode = email_delivery_mode()
    if mode == "resend":
        return send_email_via_resend(to_email, subject, body)
    if mode == "smtp":
        return send_email_via_smtp(to_email, subject, body)
    raise RuntimeError("Kein gültiger Mail-Provider konfiguriert.")


# ---------- Utilities ----------
def customer_full_name(customer):
    return f"{customer['_firstname'] or ''} {customer['_name'] or ''}".strip() or "Kunde"


def customer_phone(customer):
    return customer["Customer_Mobiltelefon"] or customer["Customer_PersönlichesTelefon"] or ""


def render_template_text(template_id, customer, appointment=None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        template = conn.execute("SELECT subject, body FROM _MailTemplates WHERE id = ?", (template_id,)).fetchone()

    full_name = customer_full_name(customer)
    when = ""
    if appointment and appointment["appointment_at"]:
        when = datetime.fromisoformat(appointment["appointment_at"]).strftime("%d.%m.%Y um %H:%M")

    placeholders = {
        "{vorname}": customer["_firstname"] or "",
        "{nachname}": customer["_name"] or "",
        "{name}": full_name,
        "{email}": customer["_mail"] or "",
        "{telefon}": customer_phone(customer),
        "{termin}": when,
        "{salon}": "Salon Karola",
    }

    defaults = {
        "birthdate": (
            "Alles Gute zum Geburtstag, {vorname}! 🎉",
            "Liebe/r {name},\n\ndas Team vom Salon Karola wünscht dir einen wunderschönen Geburtstag und freut sich auf deinen nächsten Besuch.\n\nHerzliche Grüße\nSalon Karola\nOstlandstraße 3\n75365 Calw-Wimberg\n07051/6344",
        ),
        "appointment": (
            "Terminerinnerung für {name}",
            "Hallo {name},\n\nwir erinnern dich an deinen Termin am {termin}.\n\nBei Fragen erreichst du uns unter 07051/6344.\n\nHerzliche Grüße\nSalon Karola",
        ),
    }

    subject, body = defaults[template_id]
    if template:
        subject = template["subject"] or subject
        body = template["body"] or body

    for key, value in placeholders.items():
        subject = subject.replace(key, value)
        body = body.replace(key, value)
    return subject, body


def log_email(customer_id, email_type, subject, body, recipient, status, error_message=None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO email_log(customer_id, email_type, subject, body, recipient, sent_at, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                email_type,
                subject,
                body,
                recipient,
                datetime.now().isoformat(timespec="seconds"),
                status,
                error_message,
            ),
        )
        conn.commit()


def safe_count(query, params=()):
    try:
        row = get_db().execute(query, params).fetchone()
        if row is None:
            return 0
        return int(row[0] or 0)
    except Exception as exc:
        try:
            set_setting("dashboard:last_safe_count_error", f"{query} | {exc}")
        except Exception:
            pass
        return 0


def normalized_phone_number(raw_value):
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return ""
    keep = []
    for idx, ch in enumerate(raw_value):
        if ch.isdigit():
            keep.append(ch)
        elif ch == "+" and idx == 0:
            keep.append(ch)
    number = "".join(keep)
    if number.startswith("00"):
        number = "+" + number[2:]
    elif number.startswith("0"):
        number = "+49" + number[1:]
    elif number and not number.startswith("+"):
        number = "+" + number
    return number


def phone_href(value):
    number = normalized_phone_number(value)
    return f"tel:{number}" if number else ""


def whatsapp_link(customer, text=None):
    number = normalized_phone_number(customer_phone(customer)).replace("+", "")
    if not number:
        return ""
    text = text or f"Hallo {customer_full_name(customer)}, hier ist Salon Karola."
    return f"https://wa.me/{number}?text={quote(text)}"


def appointment_whatsapp_text(customer, appointment):
    when = "deinem Termin"
    if appointment and appointment["appointment_at"]:
        try:
            when = datetime.fromisoformat(str(appointment["appointment_at"])).strftime("%d.%m.%Y um %H:%M")
        except Exception:
            when = str(appointment["appointment_at"])
    return f"Hallo {customer_full_name(customer)}, hier ist Salon Karola. Dein Termin ist am {when}. Bitte gib uns kurz Bescheid, falls sich etwas ändert."


def comeback_whatsapp_text(customer):
    return f"Hallo {customer_full_name(customer)}, hier ist Salon Karola. Wir haben dich schon länger nicht gesehen und würden uns freuen, dich bald wieder bei uns begrüßen zu dürfen. Melde dich gern für deinen nächsten Termin."


def get_automation_status():
    return {
        "last_run_at": get_setting("automation:last_run_at"),
        "last_run_summary": get_setting("automation:last_run_summary"),
        "last_run_error": get_setting("automation:last_run_error"),
        "scheduler_interval_minutes": get_setting("automation:scheduler_interval_minutes", "15") or "15",
    }


def _get_app_setting(key, default=""):
    try:
        return get_setting(key, default)
    except Exception:
        return default


def _ensure_vapid_keys():
    public_key = (os.getenv("VAPID_PUBLIC_KEY") or _get_app_setting("push:vapid_public_key", "")).strip()
    private_key = (os.getenv("VAPID_PRIVATE_KEY") or _get_app_setting("push:vapid_private_key", "")).strip()
    if public_key and private_key:
        return public_key, private_key

    try:
        private_obj = ec.generate_private_key(ec.SECP256R1())
        private_numbers = private_obj.private_numbers()
        private_bytes = private_numbers.private_value.to_bytes(32, "big")
        public_bytes = private_obj.public_key().public_bytes(
            encoding=serialization.Encoding.X962,
            format=serialization.PublicFormat.UncompressedPoint,
        )
        public_key = _b64url_encode(public_bytes)
        private_key = _b64url_encode(private_bytes)
        set_setting("push:vapid_public_key", public_key)
        set_setting("push:vapid_private_key", private_key)
        set_setting("push:vapid_generated_at", datetime.now().isoformat(timespec="seconds"))
        return public_key, private_key
    except Exception:
        return public_key, private_key


def _b64url_encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value):
    value = (value or "").strip()
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _normalize_vapid_public_key(value):
    raw_value = (value or "").strip()
    if not raw_value:
        return ""
    if re.fullmatch(r"[0-9a-fA-F]+", raw_value) and len(raw_value) % 2 == 0:
        try:
            raw = bytes.fromhex(raw_value)
            if len(raw) == 65 and raw[0] == 4:
                return _b64url_encode(raw)
        except Exception:
            pass
    try:
        raw = _b64url_decode(raw_value)
        if len(raw) == 65 and raw[0] == 4:
            return _b64url_encode(raw)
    except Exception:
        pass
    return raw_value


def _normalize_vapid_private_key(value):
    raw_value = (value or "").strip()
    if not raw_value:
        return ""
    if re.fullmatch(r"\d+", raw_value):
        try:
            number = int(raw_value)
            raw = number.to_bytes(max(32, (number.bit_length() + 7) // 8), "big")
            if len(raw) > 32:
                raw = raw[-32:]
            return _b64url_encode(raw.rjust(32, b"\x00"))
        except Exception:
            pass
    if re.fullmatch(r"[0-9a-fA-F]+", raw_value) and len(raw_value) % 2 == 0:
        try:
            raw = bytes.fromhex(raw_value)
            if len(raw) == 32:
                return _b64url_encode(raw)
            if len(raw) > 32:
                return _b64url_encode(raw)
        except Exception:
            pass
    return raw_value


def vapid_ready():
    public_key, private_key = _ensure_vapid_keys()
    return bool(_normalize_vapid_public_key(public_key) and _normalize_vapid_private_key(private_key) and webpush)


def vapid_public_key():
    public_key, _ = _ensure_vapid_keys()
    return _normalize_vapid_public_key(public_key)


def vapid_private_key():
    _, private_key = _ensure_vapid_keys()
    return _normalize_vapid_private_key(private_key)


def _push_device_label(row):
    label = (row["device_name"] if "device_name" in row.keys() else "") or ""
    if label.strip():
        return label.strip()
    user_agent = (row["user_agent"] if "user_agent" in row.keys() else "") or ""
    user_agent_l = user_agent.lower()
    if "iphone" in user_agent_l:
        return "iPhone"
    if "ipad" in user_agent_l:
        return "iPad"
    if "android" in user_agent_l:
        return "Android-Gerät"
    if "windows" in user_agent_l:
        return "Windows-Gerät"
    return "Gerät"


def _touch_push_subscription(subscription_id, **values):
    if not values:
        return
    db = get_db()
    columns = []
    params = []
    for key, value in values.items():
        columns.append(f"{key} = ?")
        params.append(value)
    params.append(subscription_id)
    db.execute(f"UPDATE push_subscriptions SET {', '.join(columns)} WHERE id = ?", params)
    db.commit()


def push_devices_for_staff(staff_name=None):
    db = get_db()
    params = []
    query = "SELECT * FROM push_subscriptions"
    if staff_name in ("Ute", "Jessi"):
        query += " WHERE staff_name = ?"
        params.append(staff_name)
    query += " ORDER BY staff_name ASC, COALESCE(last_success_at, last_seen_at, updated_at, created_at) DESC"
    rows = db.execute(query, params).fetchall()
    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "staff_name": row["staff_name"] or "Ute",
            "device_name": _push_device_label(row),
            "user_agent": (row["user_agent"] or "")[:180],
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
            "last_seen_at": row["last_seen_at"] or "",
            "last_success_at": row["last_success_at"] or "",
            "last_test_at": row["last_test_at"] or "",
            "last_error": row["last_error"] or "",
            "fail_count": int(row["fail_count"] or 0),
            "endpoint_tail": (row["endpoint"] or "")[-32:],
        })
    return items


def webpush_send_to_subscription_row(row, title, body, url="/calendar"):
    db = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    try:
        subscription = json.loads(row["subscription_json"])
        webpush(
            subscription_info=subscription,
            data=json.dumps({"title": title, "body": body, "url": url}),
            vapid_private_key=vapid_private_key(),
            vapid_claims={"sub": os.getenv("VAPID_CLAIMS_SUBJECT", "mailto:push@salonkarola.local")},
            ttl=60 * 60 * 6,
        )
        _touch_push_subscription(
            row["id"],
            last_success_at=now,
            last_error="",
            fail_count=0,
            updated_at=now,
            last_seen_at=now,
        )
        return {"ok": True, "sent": 1, "skipped": 0, "errors": []}
    except WebPushException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code in (404, 410):
            db.execute("DELETE FROM push_subscriptions WHERE id = ?", (row["id"],))
            db.commit()
            return {"ok": False, "sent": 0, "skipped": 1, "errors": ["Gerät war nicht mehr gültig und wurde entfernt."]}
        fail_count = int(row["fail_count"] or 0) + 1
        _touch_push_subscription(
            row["id"],
            last_error=str(exc)[:500],
            fail_count=fail_count,
            updated_at=now,
        )
        return {"ok": False, "sent": 0, "skipped": 0, "errors": [str(exc)]}
    except Exception as exc:
        fail_count = int(row["fail_count"] or 0) + 1
        _touch_push_subscription(
            row["id"],
            last_error=str(exc)[:500],
            fail_count=fail_count,
            updated_at=now,
        )
        return {"ok": False, "sent": 0, "skipped": 0, "errors": [str(exc)]}


def webpush_send_to_staff(target_staff, title, body, url="/calendar"):
    if not vapid_ready():
        return {"sent": 0, "skipped": 0, "errors": ["VAPID nicht konfiguriert"]}

    db = get_db()
    rows = db.execute(
        """
        SELECT * FROM push_subscriptions
        WHERE staff_name = ?
        ORDER BY updated_at DESC
        """,
        (target_staff,),
    ).fetchall()

    sent = 0
    skipped = 0
    errors = []
    for row in rows:
        result = webpush_send_to_subscription_row(row, title, body, url)
        sent += int(result.get("sent") or 0)
        skipped += int(result.get("skipped") or 0)
        errors.extend(result.get("errors") or [])
    return {"sent": sent, "skipped": skipped, "errors": errors}


def webpush_send_to_all_staff(title, body, url="/calendar"):
    totals = {"sent": 0, "skipped": 0, "errors": []}
    for target_staff in ("Ute", "Jessi"):
        result = webpush_send_to_staff(target_staff, title, body, url)
        totals["sent"] += int(result.get("sent") or 0)
        totals["skipped"] += int(result.get("skipped") or 0)
        totals["errors"].extend(result.get("errors") or [])
    return totals


def _push_birthday_message(customer):
    first_name = (customer["_firstname"] or "").strip()
    last_name = (customer["_name"] or "").strip()
    customer_name = f"{first_name} {last_name}".strip() or "Kundin"
    return webpush_send_to_all_staff(
        "Heute Geburtstag",
        f"{customer_name} hat heute Geburtstag. Gratulation nicht vergessen.",
        "/"
    )


def _push_appointment_reminder(appt):
    first_name = (appt["_firstname"] or "").strip()
    last_name = (appt["_name"] or "").strip()
    customer_name = f"{first_name} {last_name}".strip() or "Kundin"
    staff_name = (appt["staff_name"] or "Ute").strip() or "Ute"
    try:
        when_label = datetime.fromisoformat(str(appt["appointment_at"])).strftime("%d.%m.%Y um %H:%M")
    except Exception:
        when_label = str(appt["appointment_at"])
    return webpush_send_to_staff(
        staff_name,
        "Termin-Erinnerung",
        f"{customer_name} • {appt['title']} • {when_label}",
        "/calendar?view=day"
    )


def notify_other_staff_for_appointment(customer_id, title, appointment_at, staff_name, actor_name, manual_name=""):
    actor = (actor_name or staff_name or "Ute").strip() or "Ute"
    target = "Jessi" if actor == "Ute" else "Ute"
    customer_name = (manual_name or "").strip()
    if not customer_name and customer_id:
        customer = get_db().execute(
            "SELECT _firstname, _name FROM _Customers WHERE _id = ?",
            (customer_id,),
        ).fetchone()
        if customer:
            customer_name = f"{customer['_firstname'] or ''} {customer['_name'] or ''}".strip()
    customer_name = customer_name or "Kundin"
    try:
        when_label = datetime.fromisoformat(str(appointment_at)).strftime("%d.%m.%Y um %H:%M")
    except Exception:
        when_label = str(appointment_at)

    push_title = f"Neuer Termin von {actor}"
    push_body = f"{customer_name} • {title} • {when_label} • zuständig: {staff_name}"
    return webpush_send_to_staff(target, push_title, push_body, "/calendar?view=day")


def opening_hours_for_date(date_obj):
    if date_obj.weekday() <= 4:
        return ("09:00", "17:45")
    if date_obj.weekday() == 5:
        return ("08:30", "12:45")
    return None


def build_day_timeline(selected_date, staff="Alle"):
    hours = opening_hours_for_date(selected_date)
    if not hours:
        return {"open": False, "slots": [], "open_label": "Sonntag geschlossen"}

    start_str, end_str = hours
    start_dt = datetime.fromisoformat(f"{selected_date.isoformat()}T{start_str}")
    end_dt = datetime.fromisoformat(f"{selected_date.isoformat()}T{end_str}")
    rows = _fetch_calendar_appointments(start_dt.replace(hour=0, minute=0), end_dt + timedelta(minutes=30), staff)

    slots = []
    pointer = start_dt
    while pointer <= end_dt:
        slot_end = pointer + timedelta(minutes=15)
        slot_items = []
        for row in rows:
            try:
                appt_dt = datetime.fromisoformat(str(row["appointment_at"]))
            except Exception:
                continue
            if pointer <= appt_dt < slot_end:
                slot_items.append(_calendar_event_dict(row))
        slots.append({
            "time": pointer.strftime("%H:%M"),
            "items": slot_items,
            "is_now": datetime.now().date() == selected_date and pointer.strftime("%H:%M") == datetime.now().strftime("%H:%M"),
        })
        pointer += timedelta(minutes=15)

    return {
        "open": True,
        "slots": slots,
        "open_label": f"Geöffnet: {start_str} – {end_str} Uhr",
    }


# ---------- Automated jobs ----------
def run_birthday_job():
    today = datetime.now().strftime("%m-%d")
    current_year = datetime.now().year

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        customers = conn.execute(
            """
            SELECT * FROM _Customers
            WHERE _mail IS NOT NULL AND TRIM(_mail) <> ''
              AND _birthdate IS NOT NULL AND _birthdate <> ''
              AND strftime('%m-%d', _birthdate) = ?
            """,
            (today,),
        ).fetchall()

    checked = 0
    sent = 0
    errors = 0
    push_sent = 0

    for customer in customers:
        checked += 1
        mail_key = f"birthday:{customer['_id']}:{current_year}"
        if get_setting(mail_key):
            continue

        subject, body = render_template_text("birthdate", customer)
        try:
            send_email(customer["_mail"], subject, body)
            set_setting(mail_key, datetime.now().isoformat(timespec="seconds"))
            log_email(customer["_id"], "birthday", subject, body, customer["_mail"], "sent")
            push_result = _push_birthday_message(customer)
            push_sent += int(push_result.get("sent") or 0)
            sent += 1
        except Exception as exc:
            log_email(customer["_id"], "birthday", subject, body, customer["_mail"], "error", str(exc))
            errors += 1

    return {"checked": checked, "sent": sent, "errors": errors, "push_sent": push_sent}


def run_appointment_job():
    now = datetime.now()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        appointments = conn.execute(
            """
            SELECT a.*, c.*,
                   COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
                   COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
                   COALESCE(NULLIF(a.manual_email, ''), c._mail) AS _mail
            FROM appointments a
            JOIN _Customers c ON c._id = a.customer_id
            WHERE a.reminder_sent_at IS NULL
              AND COALESCE(NULLIF(a.manual_email, ''), c._mail) IS NOT NULL AND TRIM(COALESCE(NULLIF(a.manual_email, ''), c._mail)) <> ''
            ORDER BY a.appointment_at ASC
            """
        ).fetchall()

        checked = 0
        sent = 0
        errors = 0
        push_sent = 0

        for appt in appointments:
            checked += 1
            appt_time = datetime.fromisoformat(appt["appointment_at"])
            reminder_at = appt_time - timedelta(hours=int(appt["reminder_hours"] or 24))
            if now < reminder_at:
                continue

            subject, body = render_template_text("appointment", appt, appt)
            try:
                send_email(appt["_mail"], subject, body)
                conn.execute(
                    "UPDATE appointments SET reminder_sent_at = ? WHERE id = ?",
                    (datetime.now().isoformat(timespec="seconds"), appt["id"]),
                )
                conn.commit()
                log_email(appt["customer_id"], "appointment", subject, body, appt["_mail"], "sent")
                push_result = _push_appointment_reminder(appt)
                push_sent += int(push_result.get("sent") or 0)
                sent += 1
            except Exception as exc:
                log_email(appt["customer_id"], "appointment", subject, body, appt["_mail"], "error", str(exc))
                errors += 1

    return {"checked": checked, "sent": sent, "errors": errors, "push_sent": push_sent}


def scheduler_tick():
    started_at = datetime.now().isoformat(timespec="seconds")
    try:
        auto_backup = run_auto_backup_if_due()
        birthday_result = run_birthday_job()
        appointment_result = run_appointment_job()
        summary = (
            f"Geburtstage geprüft: {birthday_result['checked']}, Mails: {birthday_result['sent']}, Push: {birthday_result.get('push_sent', 0)}, Fehler: {birthday_result['errors']} | "
            f"Termine geprüft: {appointment_result['checked']}, Mails: {appointment_result['sent']}, Push: {appointment_result.get('push_sent', 0)}, Fehler: {appointment_result['errors']}"
        )
        if auto_backup:
            summary += f" | Auto-Backup: {auto_backup.name}"
        set_setting("automation:last_run_at", started_at)
        set_setting("automation:last_run_summary", summary)
        set_setting("automation:last_run_error", "")
        return {"ok": True, "summary": summary}
    except Exception as exc:
        set_setting("automation:last_run_at", started_at)
        set_setting("automation:last_run_error", str(exc))
        raise


def _parse_dt_safe(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _normalize_staff_name(value, default="Ute"):
    value = (value or default or "Ute").strip()
    return value if value in {"Ute", "Jessi", "Alle"} else default


def _safe_int(value, default=0, minimum=None, maximum=None):
    try:
        result = int(value)
    except Exception:
        result = default
    if minimum is not None:
        result = max(minimum, result)
    if maximum is not None:
        result = min(maximum, result)
    return result


def run_automation_if_due(force=False):
    now = datetime.now()
    last_run = _parse_dt_safe(get_setting("automation:last_run_at"))
    if not force and last_run and (now - last_run).total_seconds() < AUTOMATION_MIN_INTERVAL_SECONDS:
        return {"ok": True, "skipped": True}
    if not acquire_automation_lock(ttl_seconds=max(90, AUTOMATION_MIN_INTERVAL_SECONDS)):
        return {"ok": True, "skipped": True, "locked": True}
    try:
        return scheduler_tick()
    except Exception as exc:
        try:
            set_setting("automation:last_run_error", str(exc))
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        release_automation_lock()


@app.before_request
def opportunistic_automation_runner():
    if request.endpoint in {"static", "manifest", "service_worker"}:
        return None
    if request.method != "GET":
        return None
    run_automation_if_due(force=False)
    return None


# ---------- Dashboard ----------
def dashboard_stats():
    db = get_db()

    total_customers = direct_customer_count_from_file()
    total_emails = 0
    total_mobile = 0
    try:
        email_sql, mobile_sql, phone_sql, _ = customer_contact_select_sql()
        customer_rows = db.execute(
            f"""
            SELECT _id, {email_sql} AS _mail,
                   {mobile_sql} AS Customer_Mobiltelefon,
                   {phone_sql} AS Customer_PersönlichesTelefon
            FROM _Customers
            """
        ).fetchall()
        total_customers = max(total_customers, len(customer_rows))
        total_emails = sum(1 for row in customer_rows if str(row["_mail"]).strip())
        total_mobile = sum(
            1
            for row in customer_rows
            if str(row["Customer_Mobiltelefon"]).strip() or str(row["Customer_PersönlichesTelefon"]).strip()
        )
    except Exception:
        total_customers = max(total_customers, safe_count("SELECT COUNT(*) FROM _Customers"))
        if customer_has_column("_mail"):
            total_emails = safe_count("SELECT COUNT(*) FROM _Customers WHERE _mail IS NOT NULL AND TRIM(_mail) <> ''")
        else:
            total_emails = 0
        mobile_parts = []
        if customer_has_column("Customer_Mobiltelefon"):
            mobile_parts.append("(Customer_Mobiltelefon IS NOT NULL AND TRIM(Customer_Mobiltelefon) <> '')")
        if customer_has_column("Customer_PersönlichesTelefon"):
            mobile_parts.append("(Customer_PersönlichesTelefon IS NOT NULL AND TRIM(Customer_PersönlichesTelefon) <> '')")
        total_mobile = safe_count(f"SELECT COUNT(*) FROM _Customers WHERE {' OR '.join(mobile_parts)}") if mobile_parts else 0
    upcoming_appointments = safe_count(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ?",
        (datetime.now().isoformat(timespec="minutes"),),
    )
    sent_today = safe_count(
        "SELECT COUNT(*) FROM email_log WHERE date(sent_at) = date('now', 'localtime') AND status = 'sent'"
    )

    birthdays_30 = 0
    if customer_has_column("_birthdate"):
        birthday_rows = db.execute("SELECT _birthdate FROM _Customers WHERE _birthdate IS NOT NULL AND TRIM(_birthdate) <> ''").fetchall()
    else:
        birthday_rows = []

    for row in birthday_rows:
        try:
            birthdate = datetime.fromisoformat(str(row["_birthdate"])).date()
            today = datetime.now().date()
            next_birthday = birthdate.replace(year=today.year)
            if next_birthday < today:
                next_birthday = birthdate.replace(year=today.year + 1)
            if (next_birthday - today).days <= 30:
                birthdays_30 += 1
        except Exception:
            continue

    try:
        inactive_60 = db.execute(
        """
        SELECT COUNT(*)
        FROM _Customers c
        LEFT JOIN (
            SELECT customer_id, MAX(appointment_at) AS last_appointment_at
            FROM appointments
            GROUP BY customer_id
        ) a ON a.customer_id = c._id
        WHERE a.last_appointment_at IS NULL OR a.last_appointment_at < ?
        """,
        ((datetime.now() - timedelta(days=60)).isoformat(timespec="minutes"),),
    ).fetchone()[0]
    except Exception:
        inactive_60 = 0

    today_start = datetime.now().strftime("%Y-%m-%dT00:00")
    tomorrow_start = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00")
    week_end = (datetime.now() + timedelta(days=7)).isoformat(timespec="minutes")

    appointments_today = safe_count(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ? AND appointment_at < ?",
        (today_start, tomorrow_start),
    )
    appointments_week = safe_count(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ? AND appointment_at < ?",
        (datetime.now().isoformat(timespec="minutes"), week_end),
    )

    return {
        "total_customers": total_customers,
        "total_emails": total_emails,
        "total_mobile": total_mobile,
        "upcoming_appointments": upcoming_appointments,
        "sent_today": sent_today,
        "birthdays_30": birthdays_30,
        "inactive_60": inactive_60,
        "appointments_today": appointments_today,
        "appointments_week": appointments_week,
    }



def staff_dashboard_counts():
    db = get_db()
    today_start = datetime.now().strftime("%Y-%m-%dT00:00")
    tomorrow_start = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00")
    rows = db.execute(
        """
        SELECT COALESCE(staff_name, 'Ute') AS staff_name, COUNT(*) AS cnt
        FROM appointments
        WHERE appointment_at >= ? AND appointment_at < ?
        GROUP BY COALESCE(staff_name, 'Ute')
        ORDER BY CASE COALESCE(staff_name, 'Ute')
            WHEN 'Ute' THEN 1
            WHEN 'Jessi' THEN 2
            ELSE 99
        END,
        COALESCE(staff_name, 'Ute')
        """,
        (today_start, tomorrow_start),
    ).fetchall()

    data = {"Ute": 0, "Jessi": 0}
    for row in rows:
        staff_name = (row["staff_name"] or "Ute").strip() if isinstance(row["staff_name"], str) else "Ute"
        if staff_name in data:
            data[staff_name] = int(row["cnt"] or 0)

    return [
        {"staff_name": "Ute", "count": data["Ute"]},
        {"staff_name": "Jessi", "count": data["Jessi"]},
    ]


def today_appointments(limit=20):
    start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)
    return get_db().execute(
        """
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_Mobiltelefon) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_PersönlichesTelefon) AS Customer_PersönlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ? AND a.appointment_at < ?
        ORDER BY a.appointment_at ASC
        LIMIT ?
        """,
        (start_dt.isoformat(timespec="minutes"), end_dt.isoformat(timespec="minutes"), limit),
    ).fetchall()


def due_reminders(limit=20):
    now = datetime.now()
    upcoming_limit = now + timedelta(hours=24)
    return get_db().execute(
        """
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_email, ''), c._mail) AS _mail,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_Mobiltelefon) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_PersönlichesTelefon) AS Customer_PersönlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ? AND a.appointment_at <= ?
          AND a.reminder_sent_at IS NULL
        ORDER BY a.appointment_at ASC
        LIMIT ?
        """,
        (now.isoformat(timespec="minutes"), upcoming_limit.isoformat(timespec="minutes"), limit),
    ).fetchall()


def upcoming_birthdays(limit=12):
    rows = get_db().execute(
        """
        SELECT * FROM _Customers
        WHERE _birthdate IS NOT NULL AND TRIM(_birthdate) <> ''
        """
    ).fetchall()

    today = datetime.now().date()
    enriched = []
    for row in rows:
        try:
            birthdate = datetime.fromisoformat(str(row["_birthdate"])).date()
            next_birthday = birthdate.replace(year=today.year)
            if next_birthday < today:
                next_birthday = birthdate.replace(year=today.year + 1)
            enriched.append((next_birthday, row))
        except Exception:
            continue

    enriched.sort(key=lambda item: item[0])
    return [row for _, row in enriched[:limit]]


def inactive_customers(limit=12):
    rows = get_db().execute(
        """
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
        WHERE COALESCE(c._name, '') <> '__MANUELLER_TERMIN__'
        GROUP BY c._id
        HAVING last_appointment_at IS NULL OR last_appointment_at < ?
        ORDER BY COALESCE(last_appointment_at, '') ASC, c._name ASC, c._firstname ASC
        LIMIT ?
        """,
        ((datetime.now() - timedelta(days=60)).isoformat(timespec="minutes"), limit),
    ).fetchall()
    return rows


def customer_activity_status(last_appointment_at):
    if not last_appointment_at:
        return "neu"
    try:
        last_dt = datetime.fromisoformat(str(last_appointment_at))
        days = (datetime.now() - last_dt).days
        if days <= 60:
            return "aktiv"
        if days <= 120:
            return "beobachten"
        return "rueckholung"
    except Exception:
        return "neu"


def next_appointments(limit=12):
    return get_db().execute(
        """
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_Mobiltelefon) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_PersönlichesTelefon) AS Customer_PersönlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ?
        ORDER BY a.appointment_at ASC
        LIMIT ?
        """,
        (datetime.now().isoformat(timespec="minutes"), limit),
    ).fetchall()


# ---------- PWA ----------
@app.route("/manifest.webmanifest")
def manifest():
    return send_from_directory(app.static_folder, "manifest.webmanifest", mimetype="application/manifest+json")


@app.route("/service-worker.js")
def service_worker():
    return send_from_directory(app.static_folder, "service-worker.js", mimetype="application/javascript")


# ---------- Routes ----------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        user = db.execute(
            "SELECT * FROM staff_users WHERE username = ? AND password = ?",
            (username, password),
        ).fetchone()
        if user:
            session["admin_logged_in"] = True
            session["admin_name"] = user["display_name"] or user["username"]
            flash("Login erfolgreich.")
            return redirect(request.args.get("next") or url_for("index"))
        flash("Login fehlgeschlagen.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Du wurdest abgemeldet.")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    db = get_db()
    q = request.args.get("q", "").strip()
    tag = request.args.get("tag", "").strip()
    sort = (request.args.get("sort") or "az").strip().lower()
    if sort not in {"az", "za", "recent"}:
        sort = "az"

    base_query = """
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
    """
    params = []
    conditions = [visible_customer_condition("c")]

    if tag:
        base_query += " JOIN customer_tags t ON t.customer_id = c._id"
        conditions.append("t.tag = ?")
        params.append(tag)

    if q:
        like = f"%{q}%"
        customer_cols = customer_columns()
        search_parts = []
        for column_name in ["_name", "_firstname", "_mail", "Customer_Mobiltelefon", "Customer_PersönlichesTelefon", "Customer_Stadt"]:
            if column_name in customer_cols:
                search_parts.append(f"c.{column_name} LIKE ?")
                params.append(like)
        if search_parts:
            conditions.append("(" + " OR ".join(search_parts) + ")")

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    order_sql = "COALESCE(c._name, ''), COALESCE(c._firstname, '')"
    if sort == "za":
        order_sql = "COALESCE(c._name, '') DESC, COALESCE(c._firstname, '') DESC"
    elif sort == "recent":
        order_sql = "MAX(a.appointment_at) DESC, COALESCE(c._name, ''), COALESCE(c._firstname, '')"

    base_query += f" GROUP BY c._id ORDER BY {order_sql} LIMIT 200"
    customers = db.execute(base_query, params).fetchall()
    tags = db.execute("SELECT tag, COUNT(*) AS cnt FROM customer_tags GROUP BY tag ORDER BY tag").fetchall()

    stats = dashboard_stats()
    stats["direct_customer_count"] = direct_customer_count_from_file()
    if stats.get("direct_customer_count") is not None:
        stats["total_customers"] = int(stats.get("direct_customer_count") or 0)
    if customers and not stats.get("total_customers"):
        stats["total_customers"] = len(customers)
    if customers and not stats.get("total_emails"):
        stats["total_emails"] = sum(1 for row in customers if ((row["_mail"] if "_mail" in row.keys() else "") or "").strip())
    if customers and not stats.get("total_mobile"):
        stats["total_mobile"] = sum(1 for row in customers if (((row["Customer_Mobiltelefon"] if "Customer_Mobiltelefon" in row.keys() else "") or (row["Customer_PersönlichesTelefon"] if "Customer_PersönlichesTelefon" in row.keys() else "") or "").strip()))

    return render_template(
        "index.html",
        customers=customers,
        q=q,
        tag=tag,
        sort=sort,
        stats=stats,
        upcoming=next_appointments(),
        birthdays=upcoming_birthdays(),
        tags=tags,
        smtp_ready=smtp_ready(),
        automation={**get_automation_status(), "dashboard_error": get_setting("dashboard:last_safe_count_error")},
        inactive=inactive_customers(),
        today_items=today_appointments(),
        due_items=due_reminders(),
        staff_counts=staff_dashboard_counts(),
        now=datetime.now(),
        current_endpoint="index",
        app_version=APP_VERSION,
        deploy_marker=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        db_path=str(DB_PATH),
    )


@app.route("/customers/search")
@login_required
def customer_search_page():
    db = get_db()
    q = request.args.get("q", "").strip()
    tag = request.args.get("tag", "").strip()
    sort = (request.args.get("sort") or "az").strip().lower()
    if sort not in {"az", "za", "recent"}:
        sort = "az"

    base_query = """
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
    """
    params = []
    conditions = [visible_customer_condition("c")]

    if tag:
        base_query += " JOIN customer_tags t ON t.customer_id = c._id"
        conditions.append("t.tag = ?")
        params.append(tag)

    if q:
        like = f"%{q}%"
        customer_cols = customer_columns()
        search_parts = []
        for column_name in ["_name", "_firstname", "_mail", "Customer_Mobiltelefon", "Customer_PersönlichesTelefon", "Customer_Stadt"]:
            if column_name in customer_cols:
                search_parts.append(f"c.{column_name} LIKE ?")
                params.append(like)
        if search_parts:
            conditions.append("(" + " OR ".join(search_parts) + ")")

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    order_sql = "COALESCE(c._name, '') COLLATE NOCASE, COALESCE(c._firstname, '') COLLATE NOCASE"
    if sort == "za":
        order_sql = "COALESCE(c._name, '') COLLATE NOCASE DESC, COALESCE(c._firstname, '') COLLATE NOCASE DESC"
    elif sort == "recent":
        order_sql = "MAX(a.appointment_at) DESC, COALESCE(c._name, '') COLLATE NOCASE, COALESCE(c._firstname, '') COLLATE NOCASE"

    base_query += f" GROUP BY c._id ORDER BY {order_sql} LIMIT 300"
    customers = db.execute(base_query, params).fetchall()
    tags = db.execute("SELECT tag, COUNT(*) AS cnt FROM customer_tags GROUP BY tag ORDER BY tag").fetchall()
    return render_template(
        "customer_search.html",
        customers=customers,
        q=q,
        tag=tag,
        sort=sort,
        tags=tags,
        current_endpoint="customer_search_page",
        app_version=APP_VERSION,
    )


@app.route("/customers/birthdays")
@login_required
def customer_birthdays_page():
    return render_template(
        "customer_birthdays.html",
        birthdays=upcoming_birthdays(limit=60),
        current_endpoint="customer_birthdays_page",
        app_version=APP_VERSION,
    )


@app.route("/customers/inactive")
@login_required
def customer_inactive_page():
    return render_template(
        "customer_inactive.html",
        inactive=inactive_customers(limit=80),
        current_endpoint="customer_inactive_page",
        app_version=APP_VERSION,
    )


@app.route("/customer/new", methods=["GET", "POST"])
@login_required
def customer_new():
    if request.method == "POST":
        db = get_db()
        cur = db.execute(
            """
            INSERT INTO _Customers(_name, _firstname, _mail, _birthdate, _notes, Customer_Adresse, Customer_PersönlichesTelefon, Customer_Mobiltelefon, Customer_Postleitzahl, Customer_Stadt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form.get("name", "").strip(),
                request.form.get("firstname", "").strip(),
                request.form.get("mail", "").strip(),
                request.form.get("birthdate") or None,
                request.form.get("notes", "").strip(),
                request.form.get("address", "").strip(),
                request.form.get("phone", "").strip(),
                request.form.get("mobile", "").strip(),
                request.form.get("zip", "").strip(),
                request.form.get("city", "").strip(),
            ),
        )
        db.commit()
        save_tags(cur.lastrowid, request.form.get("tags", ""))
        flash("Kontakt wurde hinzugefügt.")
        return redirect(url_for("customer_detail", customer_id=cur.lastrowid))
    return render_template("customer_form.html", customer=None, appointments=[], logs=[], tags_text="", wa_link="", current_endpoint="customer_new", customer_status="neu", app_version=APP_VERSION)


@app.route("/customer/<int:customer_id>", methods=["GET", "POST"])
@login_required
def customer_detail(customer_id):
    db = get_db()
    if request.method == "POST":
        db.execute(
            """
            UPDATE _Customers
            SET _name=?, _firstname=?, _mail=?, _birthdate=?, _notes=?,
                Customer_Adresse=?, Customer_PersönlichesTelefon=?, Customer_Mobiltelefon=?, Customer_Postleitzahl=?, Customer_Stadt=?
            WHERE _id=?
            """,
            (
                request.form.get("name", "").strip(),
                request.form.get("firstname", "").strip(),
                request.form.get("mail", "").strip(),
                request.form.get("birthdate") or None,
                request.form.get("notes", "").strip(),
                request.form.get("address", "").strip(),
                request.form.get("phone", "").strip(),
                request.form.get("mobile", "").strip(),
                request.form.get("zip", "").strip(),
                request.form.get("city", "").strip(),
                customer_id,
            ),
        )
        db.commit()
        save_tags(customer_id, request.form.get("tags", ""))
        flash("Kontakt wurde aktualisiert.")
        return redirect(url_for("customer_detail", customer_id=customer_id))

    customer = db.execute("SELECT * FROM _Customers WHERE _id = ?", (customer_id,)).fetchone()
    if not customer:
        flash("Kontakt nicht gefunden.")
        return redirect(url_for("index"))

    appointments = db.execute(
        "SELECT * FROM appointments WHERE customer_id = ? ORDER BY appointment_at DESC",
        (customer_id,),
    ).fetchall()
    logs = db.execute(
        "SELECT * FROM email_log WHERE customer_id = ? ORDER BY sent_at DESC LIMIT 25",
        (customer_id,),
    ).fetchall()
    tags_text = ", ".join(
        r["tag"] for r in db.execute("SELECT tag FROM customer_tags WHERE customer_id = ? ORDER BY tag", (customer_id,)).fetchall()
    )
    next_appt = None
    for appt in appointments:
        try:
            if appt["appointment_at"] and datetime.fromisoformat(str(appt["appointment_at"])) >= datetime.now():
                next_appt = appt
                break
        except Exception:
            continue

    return render_template(
        "customer_form.html",
        customer=customer,
        appointments=appointments,
        logs=logs,
        tags_text=tags_text,
        wa_link=whatsapp_link(customer),
        wa_comeback_link=whatsapp_link(customer, comeback_whatsapp_text(customer)),
        wa_next_appt_link=whatsapp_link(customer, appointment_whatsapp_text(customer, next_appt)) if next_appt else "",
        customer_status=customer_activity_status(appointments[0]["appointment_at"]) if appointments else "neu",
        current_endpoint="customer_detail",
        app_version=APP_VERSION,
    )




@app.route("/api/customer/<int:customer_id>/summary")
@login_required
def customer_summary_api(customer_id):
    db = get_db()
    customer = db.execute("SELECT * FROM _Customers WHERE _id = ?", (customer_id,)).fetchone()
    if not customer:
        return jsonify({"ok": False, "error": "Kontakt nicht gefunden."}), 404

    latest_appointment = db.execute(
        "SELECT appointment_at, title, staff_name, status FROM appointments WHERE customer_id = ? ORDER BY appointment_at DESC LIMIT 1",
        (customer_id,),
    ).fetchone()
    tags = [
        r["tag"]
        for r in db.execute("SELECT tag FROM customer_tags WHERE customer_id = ? ORDER BY tag", (customer_id,)).fetchall()
    ]
    return jsonify({
        "ok": True,
        "item": {
            "id": customer["_id"],
            "name": customer_full_name(customer),
            "email": customer["_mail"] or "",
            "phone": customer_phone(customer),
            "city": customer["Customer_Stadt"] or "",
            "address": customer["Customer_Adresse"] or "",
            "zip": customer["Customer_Postleitzahl"] or "",
            "notes": customer["_notes"] or "",
            "birthdate": customer["_birthdate"] or "",
            "tags": tags,
            "latest_appointment": dict(latest_appointment) if latest_appointment else None,
            "detail_url": url_for("customer_detail", customer_id=customer_id),
            "call_url": normalized_phone_number(customer_phone(customer)) and f"tel:{normalized_phone_number(customer_phone(customer))}" or "",
        }
    })


def save_tags(customer_id, tags_text):
    tags = sorted({t.strip() for t in tags_text.split(",") if t.strip()})
    db = get_db()
    db.execute("DELETE FROM customer_tags WHERE customer_id = ?", (customer_id,))
    for tag in tags:
        db.execute(
            "INSERT OR IGNORE INTO customer_tags(customer_id, tag, created_at) VALUES (?, ?, ?)",
            (customer_id, tag, datetime.now().isoformat(timespec="seconds")),
        )
    db.commit()


@app.route("/appointment/new/<int:customer_id>", methods=["POST"])
@login_required
def appointment_new(customer_id):
    db = get_db()
    title = request.form.get("title", "Salon-Termin").strip() or "Salon-Termin"
    appointment_at = request.form["appointment_at"]
    status = request.form.get("status", "geplant").strip() or "geplant"
    staff_name = request.form.get("staff_name", "Ute").strip() or "Ute"
    actor_name = request.form.get("actor_name", "").strip() or staff_name
    db.execute(
        """
        INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, created_at, status, staff_name, created_by, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer_id,
            title,
            appointment_at,
            request.form.get("notes", "").strip(),
            int(request.form.get("reminder_hours", "24") or 24),
            datetime.now().isoformat(timespec="seconds"),
            status,
            staff_name,
            actor_name,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    db.commit()
    notify_result = notify_other_staff_for_appointment(customer_id, title, appointment_at, staff_name, actor_name)
    flash_msg = "Termin wurde gespeichert."
    if vapid_ready() and notify_result.get("sent", 0) > 0:
        flash_msg += f" Hintergrund-Push gesendet: {notify_result['sent']}."
    elif not vapid_ready():
        flash_msg += " Push ist noch nicht komplett aktiv – bitte VAPID-Keys in Render setzen."
    flash(flash_msg)
    return redirect(url_for("customer_detail", customer_id=customer_id))


@app.route("/appointment/edit/<int:appointment_id>", methods=["POST"])
@login_required
def appointment_edit(appointment_id):
    db = get_db()
    row = db.execute("SELECT customer_id FROM appointments WHERE id = ?", (appointment_id,)).fetchone()
    if not row:
        flash("Termin nicht gefunden.")
        return redirect(url_for("index"))

    appointment_at = request.form.get("appointment_at", "").strip()
    if not appointment_at:
        flash("Bitte ein Datum und eine Uhrzeit für den Termin angeben.")
        return redirect(url_for("customer_detail", customer_id=row["customer_id"]))

    db.execute(
        """
        UPDATE appointments
        SET title = ?, appointment_at = ?, notes = ?, reminder_hours = ?, status = ?, staff_name = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            request.form.get("title", "Salon-Termin").strip() or "Salon-Termin",
            appointment_at,
            request.form.get("notes", "").strip(),
            int(request.form.get("reminder_hours", "24") or 24),
            request.form.get("status", "geplant").strip() or "geplant",
            request.form.get("staff_name", "Ute").strip() or "Ute",
            datetime.now().isoformat(timespec="seconds"),
            appointment_id,
        ),
    )
    db.commit()
    flash("Termin wurde aktualisiert.")
    return redirect(url_for("customer_detail", customer_id=row["customer_id"]))


@app.route("/appointment/delete/<int:appointment_id>", methods=["POST"])
@login_required
def appointment_delete(appointment_id):
    db = get_db()
    row = db.execute("SELECT customer_id FROM appointments WHERE id = ?", (appointment_id,)).fetchone()
    if row:
        db.execute("DELETE FROM appointments WHERE id = ?", (appointment_id,))
        db.commit()
        flash("Termin wurde gelöscht.")
        return redirect(url_for("customer_detail", customer_id=row["customer_id"]))
    flash("Termin nicht gefunden.")
    return redirect(url_for("index"))


@app.route("/appointment/status/<int:appointment_id>", methods=["POST"])
@login_required
def appointment_update_status(appointment_id):
    db = get_db()
    status = (request.form.get("status") or "geplant").strip()
    row = db.execute("SELECT customer_id FROM appointments WHERE id = ?", (appointment_id,)).fetchone()
    if not row:
        flash("Termin nicht gefunden.")
        return redirect(url_for("index"))
    db.execute("UPDATE appointments SET status = ? WHERE id = ?", (status, appointment_id))
    db.commit()
    flash("Terminstatus wurde aktualisiert.")
    return redirect(url_for("customer_detail", customer_id=row["customer_id"]))





GERMAN_WEEKDAYS = [
    "Montag",
    "Dienstag",
    "Mittwoch",
    "Donnerstag",
    "Freitag",
    "Samstag",
    "Sonntag",
]

GERMAN_MONTHS = [
    "Januar",
    "Februar",
    "März",
    "April",
    "Mai",
    "Juni",
    "Juli",
    "August",
    "September",
    "Oktober",
    "November",
    "Dezember",
]


def weekday_name_de(date_obj):
    return GERMAN_WEEKDAYS[date_obj.weekday()]


def month_name_de(month_number):
    return GERMAN_MONTHS[month_number - 1]


def format_day_label_de(date_obj):
    return f"{weekday_name_de(date_obj)}, {date_obj.strftime('%d.%m.%Y')}"


def format_month_label_de(date_obj):
    return f"{month_name_de(date_obj.month)} {date_obj.year}"


def _parse_date(value):
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return datetime.now().date()


def parse_iso_date(value):
    return _parse_date(value)


def _calendar_event_dict(appt):
    status = appt["status"] or "geplant"
    try:
        time_short = datetime.fromisoformat(str(appt["appointment_at"])).strftime("%H:%M")
    except Exception:
        time_short = str(appt["appointment_at"])[11:16]
    return {
        "id": appt["id"],
        "customer_id": appt["customer_id"],
        "title": appt["title"],
        "appointment_at": appt["appointment_at"],
        "time_short": time_short,
        "status": status,
        "status_class": status.lower().replace(" ", "-"),
        "staff_name": appt["staff_name"] or "Ute",
        "firstname": appt["_firstname"],
        "lastname": appt["_name"],
        "customer_name": f"{appt['_firstname'] or ''} {appt['_name'] or ''}".strip(),
        "phone": appt["Customer_Mobiltelefon"] or appt["Customer_PersönlichesTelefon"] or "-",
        "notes": appt["notes"] or "",
    }


def _fetch_calendar_appointments(start_dt, end_dt, staff="Alle"):
    db = get_db()
    query = """
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_Mobiltelefon) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), c.Customer_PersönlichesTelefon) AS Customer_PersönlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ? AND a.appointment_at < ?
    """
    params = [start_dt.isoformat(timespec="minutes"), end_dt.isoformat(timespec="minutes")]

    if staff and staff != "Alle":
        query += " AND COALESCE(a.staff_name, 'Ute') = ?"
        params.append(staff)

    query += " ORDER BY a.appointment_at ASC"
    return db.execute(query, tuple(params)).fetchall()


def _build_day_view(selected_date, staff="Alle"):
    start_dt = datetime.combine(selected_date, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    rows = _fetch_calendar_appointments(start_dt, end_dt, staff)
    return {
        "selected_date": selected_date.isoformat(),
        "items": [_calendar_event_dict(r) for r in rows],
        "label": format_day_label_de(selected_date),
        "timeline": build_day_timeline(selected_date, staff),
    }


def _build_week_view(selected_date, staff="Alle"):
    monday = selected_date - timedelta(days=selected_date.weekday())
    sunday = monday + timedelta(days=6)
    start_dt = datetime.combine(monday, datetime.min.time())
    end_dt = datetime.combine(sunday + timedelta(days=1), datetime.min.time())
    rows = _fetch_calendar_appointments(start_dt, end_dt, staff)

    by_day = {(monday + timedelta(days=i)).isoformat(): [] for i in range(7)}
    for row in rows:
        day_key = str(row["appointment_at"])[:10]
        by_day.setdefault(day_key, []).append(_calendar_event_dict(row))

    days = []
    for i in range(7):
        current = monday + timedelta(days=i)
        days.append({
            "date": current.isoformat(),
            "name": weekday_name_de(current),
            "label": current.strftime("%d.%m."),
            "items": by_day.get(current.isoformat(), []),
            "is_today": current == datetime.now().date(),
        })
    return {
        "selected_date": selected_date.isoformat(),
        "monday": monday.isoformat(),
        "sunday": sunday.isoformat(),
        "days": days,
        "label": f"{monday.strftime('%d.%m.%Y')} – {sunday.strftime('%d.%m.%Y')}",
    }


def _build_month_view(selected_date, staff="Alle"):
    first_day = selected_date.replace(day=1)
    start_weekday = first_day.weekday()
    start_cell = first_day - timedelta(days=start_weekday)
    end_cell = start_cell + timedelta(days=42)

    rows = _fetch_calendar_appointments(
        datetime.combine(start_cell, datetime.min.time()),
        datetime.combine(end_cell, datetime.min.time()),
        staff,
    )
    by_day = {}
    for row in rows:
        day_key = str(row["appointment_at"])[:10]
        by_day.setdefault(day_key, []).append(_calendar_event_dict(row))

    cells = []
    for i in range(42):
        current = start_cell + timedelta(days=i)
        cells.append({
            "date": current.isoformat(),
            "day": current.day,
            "items": by_day.get(current.isoformat(), []),
            "in_month": current.month == first_day.month,
            "is_today": current == datetime.now().date(),
        })

    weeks = [cells[i:i+7] for i in range(0, 42, 7)]
    return {
        "selected_date": selected_date.isoformat(),
        "month_label": format_month_label_de(first_day),
        "weeks": weeks,
        "weekday_headers": ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"],
    }


def _calendar_nav_date(selected_date, view, step):
    if view == "day":
        return (selected_date + timedelta(days=step)).isoformat()
    if view == "week":
        return (selected_date + timedelta(days=7 * step)).isoformat()

    month = selected_date.month - 1 + step
    year = selected_date.year + month // 12
    month = month % 12 + 1
    day = min(selected_date.day, pycalendar.monthrange(year, month)[1])
    return selected_date.replace(year=year, month=month, day=day).isoformat()


@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments_hub():
    db = get_db()

    if request.method == "POST":
        customer_id_raw = (request.form.get("customer_id") or "").strip()
        appointment_at = (request.form.get("appointment_at") or "").strip()
        title = (request.form.get("title") or "Salon-Termin").strip() or "Salon-Termin"
        status = (request.form.get("status") or "geplant").strip() or "geplant"
        staff_name = _normalize_staff_name(request.form.get("staff_name"), default="Ute")
        actor_name = _normalize_staff_name(request.form.get("actor_name") or staff_name, default=staff_name)
        notes = (request.form.get("notes") or "").strip()
        reminder_hours = _safe_int(request.form.get("reminder_hours", "24") or 24, default=24, minimum=0, maximum=720)
        manual_firstname = (request.form.get("manual_firstname") or "").strip()
        manual_lastname = (request.form.get("manual_lastname") or "").strip()
        manual_phone = (request.form.get("manual_phone") or "").strip()
        manual_email = (request.form.get("manual_email") or "").strip()
        manual_name = " ".join(part for part in [manual_firstname, manual_lastname] if part).strip()

        if not appointment_at:
            flash("Bitte Datum und Uhrzeit für den Termin angeben.")
            return redirect(url_for("appointments_hub"))
        if not _parse_dt_safe(appointment_at):
            flash("Das Termin-Datum ist ungültig. Bitte Datum und Uhrzeit neu wählen.")
            return redirect(url_for("appointments_hub"))

        customer_id = None
        notify_customer_id = None
        if customer_id_raw.isdigit():
            customer_id = int(customer_id_raw)
            notify_customer_id = customer_id
            customer_row = db.execute("SELECT _id FROM _Customers WHERE _id = ?", (customer_id,)).fetchone()
            if not customer_row:
                flash("Der ausgewählte Kontakt wurde nicht gefunden.")
                return redirect(url_for("appointments_hub"))
        else:
            if not (manual_firstname or manual_lastname):
                flash("Bitte einen Kontakt aus der Datenbank wählen oder Name für den manuellen Termin eintragen.")
                return redirect(url_for("appointments_hub"))
            customer_id = ensure_manual_placeholder_customer(db)
            notify_customer_id = None

        db.execute(
            """
            INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, created_at, status, staff_name, created_by, updated_at, manual_firstname, manual_lastname, manual_phone, manual_email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                title,
                appointment_at,
                notes,
                reminder_hours,
                datetime.now().isoformat(timespec="seconds"),
                status,
                staff_name,
                actor_name,
                datetime.now().isoformat(timespec="seconds"),
                manual_firstname,
                manual_lastname,
                manual_phone,
                manual_email,
            ),
        )
        db.commit()
        try:
            notify_result = notify_other_staff_for_appointment(
                notify_customer_id,
                title,
                appointment_at,
                staff_name,
                actor_name,
                manual_name=manual_name,
            )
        except Exception as e:
            app.logger.exception("Notify Fehler bei Terminanlage: %s", e)
            notify_result = {"sent": 0, "error": str(e)}
        flash_msg = "Termin wurde gespeichert."
        if not customer_id_raw.isdigit() and (manual_firstname or manual_lastname):
            flash_msg += " Manueller Kontakt wurde nicht in der Kundenliste gespeichert."
        if vapid_ready() and notify_result.get("sent", 0) > 0:
            flash_msg += f" Hintergrund-Push gesendet: {notify_result['sent']}."
        elif not vapid_ready():
            flash_msg += " Push ist noch nicht komplett aktiv – bitte VAPID-Keys in Render setzen."
        flash(flash_msg)
        return redirect(url_for("appointments_hub"))

    customers = db.execute(
        """
        SELECT _id, COALESCE(_firstname, '') AS firstname, COALESCE(_name, '') AS lastname,
               COALESCE(Customer_Mobiltelefon, Customer_PersönlichesTelefon, '') AS phone
        FROM _Customers
        WHERE COALESCE(_name, '') <> '__MANUELLER_TERMIN__'
        ORDER BY COALESCE(_name, '') COLLATE NOCASE ASC, COALESCE(_firstname, '') COLLATE NOCASE ASC
        LIMIT 500
        """
    ).fetchall()
    today_rows = today_appointments(limit=50)
    today_split = {"Ute": [], "Jessi": []}
    for row in today_rows:
        staff_name = (row["staff_name"] or "Ute") if row["staff_name"] in {"Ute", "Jessi"} else "Ute"
        today_split.setdefault(staff_name, []).append(row)

    prefill_at = (request.args.get("appointment_at") or "").strip()
    if not prefill_at:
        prefill_at = datetime.now().replace(second=0, microsecond=0).isoformat(timespec="minutes")
    prefill_staff = _normalize_staff_name(request.args.get("staff"), default="Ute")
    if prefill_staff == "Alle":
        prefill_staff = "Ute"
    prefill_source = (request.args.get("source") or "manual").strip()

    return render_template(
        "appointments.html",
        customers=customers,
        today_split=today_split,
        upcoming=next_appointments(limit=20),
        current_endpoint="appointments_hub",
        app_version=APP_VERSION,
        prefill_at=prefill_at,
        prefill_staff=prefill_staff,
        prefill_source=prefill_source,
    )


@app.route("/calendar")
@login_required
def calendar_view():
    view = (request.args.get("view") or "week").strip().lower()
    if view not in {"day", "week", "month"}:
        view = "week"

    selected_date = parse_iso_date(request.args.get("date"))
    staff = (request.args.get("staff") or "Alle").strip()
    if staff not in STAFF_OPTIONS:
        staff = "Alle"

    day_view = _build_day_view(selected_date, staff) if view == "day" else None
    week_view = _build_week_view(selected_date, staff) if view == "week" else None
    month_view = _build_month_view(selected_date, staff) if view == "month" else None
    split_day_views = None
    if view == "day" and staff == "Alle":
        split_day_views = {
            "Ute": _build_day_view(selected_date, "Ute"),
            "Jessi": _build_day_view(selected_date, "Jessi"),
        }

    return render_template(
        "calendar.html",
        view=view,
        staff=staff,
        selected_date=selected_date.isoformat(),
        prev_date=_calendar_nav_date(selected_date, view, -1),
        next_date=_calendar_nav_date(selected_date, view, 1),
        today_date=datetime.now().date().isoformat(),
        day_view=day_view,
        week_view=week_view,
        month_view=month_view,
        split_day_views=split_day_views,
        current_endpoint="calendar_view",
        app_version=APP_VERSION,
    )


@app.route("/api/appointments/feed")
@login_required
def appointments_feed():
    since = (request.args.get("since") or "").strip()
    db = get_db()

    query = """
        SELECT a.id, a.title, a.appointment_at, a.created_at, a.updated_at, a.staff_name, a.status,
               COALESCE(NULLIF(a.created_by, ''), COALESCE(a.staff_name, 'Ute')) AS created_by,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
    """
    params = []
    if since:
        query += " WHERE COALESCE(a.updated_at, a.created_at) > ?"
        params.append(since)
    query += " ORDER BY COALESCE(a.updated_at, a.created_at) ASC LIMIT 25"

    rows = db.execute(query, tuple(params)).fetchall()
    items = []
    for row in rows:
        customer_name = f"{row['_firstname'] or ''} {row['_name'] or ''}".strip() or "Kundin"
        try:
            appointment_label = datetime.fromisoformat(str(row["appointment_at"])).strftime("%d.%m.%Y um %H:%M")
        except Exception:
            appointment_label = str(row["appointment_at"])
        items.append({
            "id": row["id"],
            "title": row["title"],
            "appointment_at": row["appointment_at"],
            "appointment_label": appointment_label,
            "customer_name": customer_name,
            "staff_name": row["staff_name"] or "Ute",
            "status": row["status"] or "geplant",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or row["created_at"] or "",
            "created_by": row["created_by"] or (row["staff_name"] or "Ute"),
        })

    return {"items": items, "server_time": datetime.now().isoformat(timespec="seconds")}


@app.route("/api/push/public-key")
@login_required
def push_public_key():
    return {"public_key": vapid_public_key(), "enabled": vapid_ready(), "format": "base64url", "generated": bool(_get_app_setting("push:vapid_generated_at", ""))}


@app.route("/api/push/status")
@login_required
def push_status():
    staff_name = (request.args.get("staff_name") or "Ute").strip() or "Ute"
    if staff_name not in ("Ute", "Jessi"):
        staff_name = "Ute"
    enabled = vapid_ready()
    permission = None
    count = 0
    try:
        row = get_db().execute(
            "SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?",
            (staff_name,),
        ).fetchone()
        count = int(row["cnt"] or 0) if row else 0
    except Exception:
        count = 0
    return {"enabled": enabled, "staff_name": staff_name, "subscriptions": count}


@app.route("/api/push/overview")
@login_required
def push_overview():
    db = get_db()
    total_devices = 0
    total_active = 0
    try:
        row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions").fetchone()
        total_devices = int(row["cnt"] or 0) if row else 0
        row_ok = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE COALESCE(last_error, '') = ''").fetchone()
        total_active = int(row_ok["cnt"] or 0) if row_ok else 0
    except Exception:
        pass
    return {
        "ok": True,
        "enabled": vapid_ready(),
        "generated_keys": bool(_get_app_setting("push:vapid_generated_at", "")),
        "total_devices": total_devices,
        "active_devices": total_active,
        "ute_devices": len(push_devices_for_staff("Ute")),
        "jessi_devices": len(push_devices_for_staff("Jessi")),
        "last_run_at": get_setting("automation:last_run_at", ""),
        "last_run_summary": get_setting("automation:last_run_summary", ""),
        "last_run_error": get_setting("automation:last_run_error", ""),
    }


@app.route("/api/push/devices")
@login_required
def push_devices():
    staff_name = (request.args.get("staff_name") or "").strip()
    if staff_name not in ("Ute", "Jessi"):
        staff_name = None
    return {"ok": True, "items": push_devices_for_staff(staff_name)}


@app.route("/api/push/device/<int:subscription_id>/test")
@login_required
def push_test_device(subscription_id):
    row = get_db().execute("SELECT * FROM push_subscriptions WHERE id = ?", (subscription_id,)).fetchone()
    if not row:
        return {"ok": False, "error": "Gerät nicht gefunden."}, 404
    label = _push_device_label(row)
    result = webpush_send_to_subscription_row(
        row,
        f"Test-Push für {label}",
        f"Dieses Gerät ist für {row['staff_name'] or 'Ute'} aktiv.",
        "/calendar",
    )
    _touch_push_subscription(subscription_id, last_test_at=datetime.now().isoformat(timespec="seconds"))
    return {"ok": True, "result": result, "device_name": label}


@app.route("/api/push/device/<int:subscription_id>", methods=["DELETE"])
@login_required
def push_delete_device(subscription_id):
    db = get_db()
    db.execute("DELETE FROM push_subscriptions WHERE id = ?", (subscription_id,))
    db.commit()
    return {"ok": True}


@app.route("/api/push/subscribe", methods=["POST"])
@login_required
def push_subscribe():
    payload = request.get_json(silent=True) or {}
    subscription = payload.get("subscription") or {}
    if isinstance(subscription, str):
        try:
            subscription = json.loads(subscription)
        except Exception:
            subscription = {}
    if not isinstance(subscription, dict):
        try:
            subscription = dict(subscription)
        except Exception:
            subscription = {}
    endpoint = (subscription.get("endpoint") or payload.get("endpoint") or "").strip()
    staff_name = _normalize_staff_name(payload.get("staff_name"), default="Ute")
    device_name = (payload.get("device_name") or "").strip()[:80]
    if not endpoint:
        return {"ok": False, "error": "Keine Subscription empfangen."}, 400

    now = datetime.now().isoformat(timespec="seconds")
    db = get_db()
    db.execute(
        """
        INSERT INTO push_subscriptions(endpoint, subscription_json, staff_name, device_name, user_agent, created_at, updated_at, last_seen_at, last_error, fail_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(endpoint) DO UPDATE SET
            subscription_json = excluded.subscription_json,
            staff_name = excluded.staff_name,
            device_name = excluded.device_name,
            user_agent = excluded.user_agent,
            updated_at = excluded.updated_at,
            last_seen_at = excluded.last_seen_at,
            last_error = '',
            fail_count = 0
        """,
        (endpoint, json.dumps(subscription), staff_name, device_name, request.headers.get("User-Agent", "")[:500], now, now, now, "", 0),
    )
    db.commit()
    row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?", (staff_name,)).fetchone()
    return {"ok": True, "staff_name": staff_name, "device_name": device_name, "device_count": int(row["cnt"] or 0) if row else 0}


@app.route("/api/push/unsubscribe", methods=["POST"])
@login_required
def push_unsubscribe():
    payload = request.get_json(silent=True) or {}
    endpoint = ((payload.get("subscription") or {}).get("endpoint") or "").strip()
    if endpoint:
        db = get_db()
        db.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        db.commit()
    return {"ok": True}


@app.route("/api/push/ping")
@login_required
def push_ping():
    staff_name = _normalize_staff_name(request.args.get("staff_name"), default="Ute")
    if staff_name == "Alle":
        result = webpush_send_to_all_staff("Salon Karola Push aktiv", "Test-Push an alle registrierten Geräte.", "/calendar")
        devices = push_devices_for_staff(None)
        device_count = len(devices)
    else:

        devices = push_devices_for_staff(staff_name)
        device_count = len(devices)
        result = webpush_send_to_staff(staff_name, "Salon Karola Push aktiv", f"Dieses Handy ist jetzt für {staff_name} registriert.", "/calendar")
    return {"ok": True, "result": result, "enabled": vapid_ready(), "devices": devices, "device_count": device_count}


@app.route("/push")
@login_required
def push_center():
    return render_template(
        "push.html",
        current_endpoint="push_center",
        app_version=APP_VERSION,
    )


@app.route("/whatsapp")
@login_required
def whatsapp_hub():
    db = get_db()
    q = request.args.get("q", "").strip()
    query = """
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
    """
    params = []
    conditions = ["COALESCE(c.Customer_Mobiltelefon, c.Customer_PersönlichesTelefon, '') <> ''"]
    if q:
        like = f"%{q}%"
        conditions.append("(c._name LIKE ? OR c._firstname LIKE ? OR c.Customer_Mobiltelefon LIKE ? OR c.Customer_PersönlichesTelefon LIKE ?)")
        params.extend([like, like, like, like])
    query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY c._id ORDER BY c._name, c._firstname LIMIT 200"
    customers = db.execute(query, params).fetchall()

    next_appt_map = {}
    for row in db.execute(
        "SELECT * FROM appointments WHERE appointment_at IS NOT NULL ORDER BY appointment_at ASC"
    ).fetchall():
        cid = row["customer_id"]
        if cid not in next_appt_map:
            next_appt_map[cid] = row

    return render_template(
        "whatsapp.html",
        customers=customers,
        next_appt_map=next_appt_map,
        q=q,
        current_endpoint="whatsapp_hub",
        app_version=APP_VERSION,
    )


@app.route("/templates", methods=["GET", "POST"])
@login_required
def templates_view():
    db = get_db()
    if request.method == "POST":
        for template_id in ["birthdate", "appointment"]:
            subject = request.form.get(f"{template_id}_subject", "").strip()
            body = request.form.get(f"{template_id}_body", "").strip()

            combined = f"{subject}\n{body}".lower()
            if "matthias" in combined:
                flash("Feste Namen wie 'Matthias' sind in Vorlagen nicht erlaubt. Bitte Platzhalter wie {name} verwenden.")
                return redirect(url_for("templates_view"))
            if template_id == "birthdate" and "{name}" not in body and "{vorname}" not in body:
                flash("Die Geburtstagsvorlage muss mindestens {name} oder {vorname} enthalten.")
                return redirect(url_for("templates_view"))

            existing = db.execute("SELECT rowid FROM _MailTemplates WHERE id = ? LIMIT 1", (template_id,)).fetchone()
            if existing:
                db.execute("UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?", (subject, body, template_id))
            else:
                db.execute("INSERT INTO _MailTemplates(id, subject, body) VALUES (?, ?, ?)", (template_id, subject, body))
        db.commit()
        flash("Vorlagen wurden gespeichert.")
        return redirect(url_for("templates_view"))

    templates = {
        r["id"]: r
        for r in db.execute("SELECT * FROM _MailTemplates WHERE id IN ('birthdate','appointment')").fetchall()
    }
    return render_template("templates.html", templates=templates, current_endpoint="templates_view", app_version=APP_VERSION)


@app.route("/admin/reset-birthdate-template")
@login_required
def reset_birthdate_template():
    db = get_db()
    subject = "Alles Gute zum Geburtstag, {vorname}! 🎉"
    body = (
        "Lieber {name}, alles Gute zum Geburtstag! 🎂!\n\n"
        "Alles Gute zum Geburtstag! 🎂\n"
        "Wir vom Salon Karola wünschen Ihnen einen wunderschönen Tag.\n\n"
        "Herzliche Grüße\n"
        "Salon Karola"
    )
    db.execute(
        """
        INSERT INTO _MailTemplates(id, subject, body)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            subject = excluded.subject,
            body = excluded.body
        """,
        ("birthdate", subject, body),
    )
    db.commit()
    flash("Geburtstagsvorlage wurde zurückgesetzt.")
    return redirect(url_for("templates_view"))


@app.route("/send-test/<int:customer_id>/<template_id>")
@login_required
def send_test(customer_id, template_id):
    db = get_db()
    customer = db.execute("SELECT * FROM _Customers WHERE _id = ?", (customer_id,)).fetchone()
    if not customer or not customer["_mail"]:
        flash("Dieser Kontakt hat keine E-Mail-Adresse.")
        return redirect(url_for("customer_detail", customer_id=customer_id))

    appointment = db.execute(
        "SELECT * FROM appointments WHERE customer_id = ? ORDER BY appointment_at ASC LIMIT 1",
        (customer_id,),
    ).fetchone()
    subject, body = render_template_text(template_id, customer, appointment)

    try:
        send_email(customer["_mail"], f"TEST: {subject}", body)
        log_email(customer_id, f"test_{template_id}", f"TEST: {subject}", body, customer["_mail"], "sent")
        flash("Test-E-Mail wurde versendet.")
    except Exception as exc:
        log_email(customer_id, f"test_{template_id}", f"TEST: {subject}", body, customer["_mail"], "error", str(exc))
        flash(f"Test-E-Mail fehlgeschlagen: {exc}")
    return redirect(url_for("customer_detail", customer_id=customer_id))


@app.route("/automation/run")
@login_required
def run_automation_now():
    result = run_automation_if_due(force=True)
    flash(f"Automatiklauf wurde manuell ausgeführt. {result['summary']}")
    return redirect(url_for("index"))


@app.route("/export/customers.csv")
@login_required
def export_customers():
    rows = get_db().execute("SELECT * FROM _Customers ORDER BY _name, _firstname").fetchall()
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["ID", "Vorname", "Nachname", "E-Mail", "Geburtstag", "Telefon", "Mobil", "Adresse", "PLZ", "Stadt", "Notizen"])
    for row in rows:
        writer.writerow([
            row["_id"], row["_firstname"], row["_name"], row["_mail"], row["_birthdate"], row["Customer_PersönlichesTelefon"],
            row["Customer_Mobiltelefon"], row["Customer_Adresse"], row["Customer_Postleitzahl"], row["Customer_Stadt"], row["_notes"]
        ])
    return Response(output.getvalue(), mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=salon_karola_kunden.csv"})


@app.route("/import", methods=["GET", "POST"])
@login_required
def import_customers():
    if request.method == "POST":
        file = request.files.get("csv_file")
        if not file or not file.filename:
            flash("Bitte eine CSV-Datei auswählen.")
            return redirect(url_for("import_customers"))

        content = file.stream.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content), delimiter=";")
        db = get_db()
        inserted = 0
        for row in reader:
            email = (row.get("E-Mail") or row.get("Email") or "").strip()
            firstname = (row.get("Vorname") or "").strip()
            lastname = (row.get("Nachname") or "").strip()
            if not (firstname or lastname or email):
                continue
            db.execute(
                """
                INSERT INTO _Customers(_name, _firstname, _mail, _birthdate, _notes, Customer_Adresse, Customer_PersönlichesTelefon, Customer_Mobiltelefon, Customer_Postleitzahl, Customer_Stadt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lastname, firstname, email,
                    (row.get("Geburtstag") or "").strip() or None,
                    (row.get("Notizen") or "").strip(),
                    (row.get("Adresse") or "").strip(),
                    (row.get("Telefon") or "").strip(),
                    (row.get("Mobil") or "").strip(),
                    (row.get("PLZ") or "").strip(),
                    (row.get("Stadt") or "").strip(),
                ),
            )
            inserted += 1
        db.commit()
        flash(f"{inserted} Kontakte importiert.")
        return redirect(url_for("index"))
    return render_template("import.html", current_endpoint="import_customers")


@app.route("/logs")
@login_required
def email_logs():
    logs = get_db().execute("SELECT * FROM email_log ORDER BY sent_at DESC LIMIT 200").fetchall()
    return render_template("logs.html", logs=logs, current_endpoint="email_logs")


# ---------- Template filters ----------
@app.template_filter("dt")
def format_dt(value):
    if not value:
        return "Kein Termin"
    try:
        return datetime.fromisoformat(str(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(value)


@app.template_filter("birthday")
def format_birthday(value):
    if not value:
        return "Kein Datum"
    try:
        return datetime.fromisoformat(str(value)).strftime("%d.%m.")
    except Exception:
        return str(value)


@app.template_filter("phone_href")
def format_phone_href(value):
    return phone_href(value)


@app.context_processor
def inject_globals():
    return {
        "admin_name": session.get("admin_name"),
        "customer_activity_status": customer_activity_status,
        "whatsapp_link": whatsapp_link,
        "phone_href": phone_href,
        "app_version": APP_VERSION,
    }


def boot_app():
    init_db()
    run_auto_backup_if_due()
    ensure_default_admin(force_reset=True)
    _ensure_vapid_keys()
    interval_minutes = int(os.getenv("AUTOMATION_INTERVAL_MINUTES", "5"))
    set_setting("automation:scheduler_interval_minutes", str(interval_minutes))
    if not scheduler.running:
        existing_jobs = {job.id for job in scheduler.get_jobs()}
        if "automation_loop" not in existing_jobs:
            scheduler.add_job(
                scheduler_tick,
                "interval",
                minutes=interval_minutes,
                id="automation_loop",
                replace_existing=True,
            )
        scheduler.start()
    try:
        run_automation_if_due(force=True)
    except Exception:
        pass


# ---------- Database import / export ----------
BACKUP_DIR = BASE_DIR / "backups"
BACKUP_DIR.mkdir(exist_ok=True)


def close_live_db_connection():
    db = g.pop("db", None)
    if db is not None:
        db.close()


def timestamp_slug():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def inspect_sqlite_database(path):
    info = {"tables": [], "counts": {}, "has_customers": False, "has_appointments": False, "has_templates": False}
    with sqlite3.connect(path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        tables = [row[0] for row in rows]
        info["tables"] = tables
        info["has_customers"] = "_Customers" in tables
        info["has_appointments"] = "appointments" in tables
        info["has_templates"] = "_MailTemplates" in tables
        for table in ["_Customers", "appointments", "_MailTemplates", "email_log", "customer_tags", "staff_users"]:
            if table in tables:
                try:
                    if table == "_Customers":
                        info["counts"][table] = conn.execute(f"SELECT COUNT(*) FROM _Customers WHERE COALESCE(_name, '') <> '{MANUAL_PLACEHOLDER_LASTNAME}'").fetchone()[0]
                    else:
                        info["counts"][table] = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                except Exception:
                    info["counts"][table] = "—"
    return info


def direct_customer_count_from_file():
    try:
        info = inspect_sqlite_database(DB_PATH) if DB_PATH.exists() else None
        if info:
            return int(info.get("counts", {}).get("_Customers", 0) or 0)
    except Exception as exc:
        try:
            set_setting("dashboard:last_direct_count_error", str(exc))
        except Exception:
            pass
    return 0


def backup_current_database(label="manual"):
    if not DB_PATH.exists():
        return None
    backup_path = BACKUP_DIR / f"salon_karola_{label}_{timestamp_slug()}.sqlite"
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def cleanup_old_backups(keep=AUTO_BACKUP_KEEP):
    files = sorted(BACKUP_DIR.glob("*.sqlite"), reverse=True)
    for extra in files[keep:]:
        extra.unlink(missing_ok=True)


def run_auto_backup_if_due(force=False):
    if not DB_PATH.exists():
        return None
    today_key = datetime.now().strftime("%Y-%m-%d")
    last_backup_key = get_setting("backup:last_auto_date")
    if not force and last_backup_key == today_key:
        return None
    backup_path = backup_current_database("auto")
    set_setting("backup:last_auto_date", today_key)
    if backup_path:
        set_setting("backup:last_auto_file", backup_path.name)
    cleanup_old_backups()
    return backup_path


def replace_database_from_upload(uploaded_file):
    suffix = Path(uploaded_file.filename or "").suffix or ".sqlite"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        uploaded_file.save(tmp.name)
        tmp_path = Path(tmp.name)

    info = inspect_sqlite_database(tmp_path)
    if not info["has_customers"]:
        tmp_path.unlink(missing_ok=True)
        raise ValueError("Die importierte Datenbank enthält keine _Customers-Tabelle.")

    backup_path = backup_current_database("before_replace")
    close_live_db_connection()
    if DB_PATH.exists():
        DB_PATH.unlink()
    shutil.copy2(tmp_path, DB_PATH)
    tmp_path.unlink(missing_ok=True)
    init_db()
    ensure_default_admin(force_reset=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    return backup_path, inspect_sqlite_database(DB_PATH)


def merge_database_from_upload(uploaded_file):
    suffix = Path(uploaded_file.filename or "").suffix or ".sqlite"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        uploaded_file.save(tmp.name)
        tmp_path = Path(tmp.name)

    source_info = inspect_sqlite_database(tmp_path)
    if not source_info["has_customers"]:
        tmp_path.unlink(missing_ok=True)
        raise ValueError("Die importierte Datenbank enthält keine _Customers-Tabelle.")

    backup_path = backup_current_database("before_merge")
    init_db()
    ensure_default_admin(force_reset=False)
    dest = sqlite3.connect(DB_PATH)
    dest.row_factory = sqlite3.Row
    src = sqlite3.connect(tmp_path)
    src.row_factory = sqlite3.Row
    merged = {"customers": 0, "appointments": 0, "templates": 0}

    try:
        existing_keys = set()
        for row in dest.execute("SELECT _firstname, _name, COALESCE(_mail, '') as mail FROM _Customers"):
            existing_keys.add(((row["_firstname"] or "").strip().lower(), (row["_name"] or "").strip().lower(), (row["mail"] or "").strip().lower()))

        src_tables = set(source_info["tables"])

        if "_Customers" in src_tables:
            for row in src.execute("SELECT * FROM _Customers"):
                keys = row.keys()
                key = (
                    ((row["_firstname"] if "_firstname" in keys else "") or "").strip().lower(),
                    ((row["_name"] if "_name" in keys else "") or "").strip().lower(),
                    ((row["_mail"] if "_mail" in keys else "") or "").strip().lower(),
                )
                if key in existing_keys:
                    continue
                dest.execute(
                    """
                    INSERT INTO _Customers(_name, _firstname, _mail, _birthdate, _notes, Customer_Adresse, Customer_PersönlichesTelefon, Customer_Mobiltelefon, Customer_Postleitzahl, Customer_Stadt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["_name"] if "_name" in keys else None,
                        row["_firstname"] if "_firstname" in keys else None,
                        row["_mail"] if "_mail" in keys else None,
                        row["_birthdate"] if "_birthdate" in keys else None,
                        row["_notes"] if "_notes" in keys else None,
                        row["Customer_Adresse"] if "Customer_Adresse" in keys else None,
                        row["Customer_PersönlichesTelefon"] if "Customer_PersönlichesTelefon" in keys else None,
                        row["Customer_Mobiltelefon"] if "Customer_Mobiltelefon" in keys else None,
                        row["Customer_Postleitzahl"] if "Customer_Postleitzahl" in keys else None,
                        row["Customer_Stadt"] if "Customer_Stadt" in keys else None,
                    ),
                )
                existing_keys.add(key)
                merged["customers"] += 1

        if "_MailTemplates" in src_tables:
            for row in src.execute("SELECT id, subject, body FROM _MailTemplates"):
                exists = dest.execute("SELECT 1 FROM _MailTemplates WHERE id = ?", (row["id"],)).fetchone()
                if exists:
                    dest.execute("UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?", (row["subject"], row["body"], row["id"]))
                else:
                    dest.execute("INSERT INTO _MailTemplates(id, subject, body) VALUES (?, ?, ?)", (row["id"], row["subject"], row["body"]))
                    merged["templates"] += 1

        if "appointments" in src_tables and "_Customers" in src_tables:
            src_customers = src.execute("SELECT _id, _firstname, _name, COALESCE(_mail, '') as _mail FROM _Customers").fetchall()
            customer_map = {}
            for row in src_customers:
                key = (
                    (row["_firstname"] or "").strip().lower(),
                    (row["_name"] or "").strip().lower(),
                    (row["_mail"] or "").strip().lower(),
                )
                dest_customer = dest.execute(
                    """
                    SELECT _id FROM _Customers
                    WHERE lower(COALESCE(_firstname, '')) = ?
                      AND lower(COALESCE(_name, '')) = ?
                      AND lower(COALESCE(_mail, '')) = ?
                    LIMIT 1
                    """,
                    key,
                ).fetchone()
                if dest_customer:
                    customer_map[row["_id"]] = dest_customer["_id"]

            for row in src.execute("SELECT * FROM appointments"):
                mapped_customer = customer_map.get(row["customer_id"])
                if not mapped_customer:
                    continue
                duplicate = dest.execute(
                    "SELECT 1 FROM appointments WHERE customer_id = ? AND appointment_at = ? AND title = ? LIMIT 1",
                    (mapped_customer, row["appointment_at"], row["title"]),
                ).fetchone()
                if duplicate:
                    continue
                row_keys = row.keys()
                dest.execute(
                    """
                    INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, reminder_sent_at, created_at, status, staff_name, created_by, updated_at, manual_firstname, manual_lastname, manual_phone, manual_email)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mapped_customer,
                        row["title"],
                        row["appointment_at"],
                        row["notes"] if "notes" in row_keys else "",
                        row["reminder_hours"] if "reminder_hours" in row_keys else 24,
                        row["reminder_sent_at"] if "reminder_sent_at" in row_keys else None,
                        row["created_at"] if "created_at" in row_keys else datetime.now().isoformat(timespec="seconds"),
                        row["status"] if "status" in row_keys and row["status"] else "geplant",
                        row["staff_name"] if "staff_name" in row_keys and row["staff_name"] else "Ute",
                        row["created_by"] if "created_by" in row_keys and row["created_by"] else (row["staff_name"] if "staff_name" in row_keys and row["staff_name"] else "Ute"),
                        row["updated_at"] if "updated_at" in row_keys and row["updated_at"] else (row["created_at"] if "created_at" in row_keys else datetime.now().isoformat(timespec="seconds")),
                        row["manual_firstname"] if "manual_firstname" in row_keys else "",
                        row["manual_lastname"] if "manual_lastname" in row_keys else "",
                        row["manual_phone"] if "manual_phone" in row_keys else "",
                        row["manual_email"] if "manual_email" in row_keys else "",
                    ),
                )
                merged["appointments"] += 1
        dest.commit()
    finally:
        src.close()
        dest.close()
        tmp_path.unlink(missing_ok=True)

    return backup_path, merged, inspect_sqlite_database(DB_PATH)


@app.route("/database-tools", methods=["GET", "POST"])
@login_required
def database_tools():
    db_info = inspect_sqlite_database(DB_PATH) if DB_PATH.exists() else None
    backup_files = sorted(BACKUP_DIR.glob("*.sqlite"), reverse=True)[:10]
    if request.method == "POST":
        action = request.form.get("action", "replace")
        file = request.files.get("db_file")
        if not file or not file.filename:
            flash("Bitte eine Datenbank-Datei auswählen (.sqlite, .db oder .backup).")
            return redirect(url_for("database_tools"))

        ext = Path(file.filename).suffix.lower()
        if ext not in {".sqlite", ".db", ".backup"}:
            flash("Bitte eine gültige SQLite-Datei hochladen (.sqlite, .db oder .backup).")
            return redirect(url_for("database_tools"))

        try:
            if action == "merge":
                backup_path, merged, info_after = merge_database_from_upload(file)
                flash(
                    f"Datenbank zusammengeführt. Neue Kontakte: {merged['customers']}, neue Termine: {merged['appointments']}, neue Vorlagen: {merged['templates']}. Aktuell insgesamt: {info_after['counts'].get('_Customers', 0)} Kontakte. Backup: {backup_path.name if backup_path else 'keins'}"
                )
            else:
                backup_path, info_after = replace_database_from_upload(file)
                flash(
                    f"Datenbank komplett ersetzt. Aktuell: {info_after['counts'].get('_Customers', 0)} Kontakte und {info_after['counts'].get('appointments', 0)} Termine. Backup: {backup_path.name if backup_path else 'keins'}"
                )
        except Exception as exc:
            flash(f"Datenbank-Import fehlgeschlagen: {exc}")
        return redirect(url_for("database_tools"))
    return render_template("database_tools.html", db_info=db_info, backup_files=backup_files, current_endpoint="database_tools", app_version=APP_VERSION)


@app.route("/database/export")
@login_required
def export_database():
    init_db()
    if not DB_PATH.exists():
        flash("Es wurde noch keine Datenbank gefunden.")
        return redirect(url_for("database_tools"))
    return send_file(DB_PATH, as_attachment=True, download_name=f"salon_karola_export_{timestamp_slug()}.sqlite", mimetype="application/octet-stream")


@app.route("/database/backup-zip")
@login_required
def export_database_zip():
    init_db()
    if not DB_PATH.exists():
        flash("Es wurde noch keine Datenbank gefunden.")
        return redirect(url_for("database_tools"))
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(DB_PATH, arcname="salon_karola.db")
    mem.seek(0)
    return send_file(mem, as_attachment=True, download_name=f"salon_karola_backup_{timestamp_slug()}.zip", mimetype="application/zip")


@app.route("/database/backup/<path:filename>")
@login_required
def download_backup(filename):
    file_path = BACKUP_DIR / filename
    if not file_path.exists():
        flash("Backup-Datei wurde nicht gefunden.")
        return redirect(url_for("database_tools"))
    return send_file(file_path, as_attachment=True, download_name=file_path.name, mimetype="application/octet-stream")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "run-jobs":
        init_db()
        result = scheduler_tick()
        print(result["summary"])
    else:
        boot_app()
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
