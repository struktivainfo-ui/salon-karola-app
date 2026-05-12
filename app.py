
import base64
import csv
import io
import os
import re
import shutil
import smtplib
import secrets
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
    jsonify,
    flash,
    g,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from webauthn import (
        generate_authentication_options,
        generate_registration_options,
        options_to_json,
        verify_authentication_response,
        verify_registration_response,
    )
    from webauthn.helpers.structs import (
        AuthenticationCredential,
        AuthenticatorSelectionCriteria,
        PublicKeyCredentialDescriptor,
        RegistrationCredential,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    )
except Exception:
    generate_authentication_options = None
    generate_registration_options = None
    options_to_json = None
    verify_authentication_response = None
    verify_registration_response = None
    AuthenticationCredential = None
    AuthenticatorSelectionCriteria = None
    PublicKeyCredentialDescriptor = None
    RegistrationCredential = None
    ResidentKeyRequirement = None
    UserVerificationRequirement = None

try:
    from pywebpush import WebPushException, webpush
except Exception:
    WebPushException = Exception
    webpush = None

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2 import service_account
except Exception:
    GoogleAuthRequest = None
    service_account = None

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
app.secret_key = os.getenv("SECRET_KEY") or os.getenv("FLASK_SECRET_KEY") or os.urandom(32).hex()
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024
app.permanent_session_lifetime = timedelta(days=45)


@app.after_request
def add_no_cache_headers(response):
    try:
        if response.mimetype == "text/html" and "charset=" not in (response.headers.get("Content-Type", "").lower()):
            response.headers["Content-Type"] = "text/html; charset=utf-8"
        elif response.mimetype == "application/json" and "charset=" not in (response.headers.get("Content-Type", "").lower()):
            response.headers["Content-Type"] = "application/json; charset=utf-8"
        elif response.mimetype == "application/manifest+json" and "charset=" not in (response.headers.get("Content-Type", "").lower()):
            response.headers["Content-Type"] = "application/manifest+json; charset=utf-8"
        if request.path in ["/", "/login", "/safe-start", "/diagnose", "/calendar", "/database-tools", "/templates", "/api/templates/live"] or response.mimetype in {"text/html", "application/javascript", "application/json", "application/manifest+json"}:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
    except Exception:
        pass
    return response

APP_VERSION = "Salon Karola App 2026-05-07-production-rebuild-1"


def env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


SAFE_MODE = env_bool("SAFE_MODE", False)
ENABLE_PUSH = env_bool("ENABLE_PUSH", not SAFE_MODE)
ENABLE_SCHEDULER = env_bool("ENABLE_SCHEDULER", not SAFE_MODE)
ENABLE_SERVICE_WORKER = env_bool("ENABLE_SERVICE_WORKER", not SAFE_MODE)
ENABLE_FIREBASE = env_bool("ENABLE_FIREBASE", not SAFE_MODE)
CONFIGURED_STAFF_MEMBERS = ["Ute", "Jessi", "Sven"]
ADMIN_STAFF_NAMES = {"Sven"}
PRIMARY_BOOKING_STAFF = ["Ute", "Jessi"]
SERVICE_PRESETS = [
    {"id": "schneiden", "label": "Schneiden", "active": 30, "processing": 0},
    {"id": "foehnen", "label": "Foehnen", "active": 30, "processing": 0},
    {"id": "waschen", "label": "Waschen", "active": 15, "processing": 0},
    {"id": "legen", "label": "Legen", "active": 30, "processing": 0},
    {"id": "dauerwelle", "label": "Dauerwelle", "active": 45, "processing": 45},
    {"id": "farbe", "label": "Farbe", "active": 30, "processing": 45},
    {"id": "straehnen", "label": "Straehnen", "active": 45, "processing": 45},
    {"id": "sonstiges", "label": "Sonstiges", "active": 30, "processing": 0},
]
SERVICE_PRESET_MAP = {item["id"]: item for item in SERVICE_PRESETS}
STAFF_MEMBERS = list(CONFIGURED_STAFF_MEMBERS)
STAFF_OPTIONS = ["Alle", *STAFF_MEMBERS]
DEFAULT_STAFF = STAFF_MEMBERS[0]
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
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Bitte zuerst anmelden."}), 401
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


def admin_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if not is_admin_session():
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Dieser Bereich ist nur fuer Sven/Admin sichtbar."}), 403
            flash("Dieser Bereich ist nur fuer Sven/Admin sichtbar.")
            return redirect(url_for("staff_today"))
        return view(*args, **kwargs)
    return wrapped


def staff_or_admin_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        return view(*args, **kwargs)
    return wrapped


def safe_user_error():
    return "Die Aktion konnte nicht ausgeführt werden. Bitte erneut versuchen oder Sven informieren."


@app.errorhandler(404)
def handle_not_found(exc):
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": "Der angefragte Bereich wurde nicht gefunden."}), 404
    if session.get("admin_logged_in"):
        flash("Diese Seite wurde nicht gefunden.")
        return redirect(url_for("calendar_view"))
    return redirect(url_for("login"))


@app.errorhandler(500)
def handle_internal_server_error(exc):
    try:
        app.logger.exception("500 auf %s", request.path, exc_info=exc)
    except Exception:
        pass

    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": safe_user_error()}), 500

    if session.get("admin_logged_in") and request.endpoint != "calendar_view":
        try:
            flash(safe_user_error())
        except Exception:
            pass
        return redirect(url_for("calendar_view"))

    return (
        "<h1>Interner Serverfehler</h1><p>Die Seite konnte gerade nicht geladen werden.</p>",
        500,
        {"Content-Type": "text/html; charset=utf-8"},
    )


def hash_password(password):
    return generate_password_hash(password or "")


def password_is_hashed(value):
    value = (value or "").strip()
    return value.startswith("pbkdf2:") or value.startswith("scrypt:")


def verify_password(stored_password, candidate_password):
    stored_password = stored_password or ""
    candidate_password = candidate_password or ""
    if password_is_hashed(stored_password):
        try:
            return check_password_hash(stored_password, candidate_password)
        except Exception:
            return False
    return stored_password == candidate_password


def staff_account_configs():
    accounts = []
    for member in CONFIGURED_STAFF_MEMBERS:
        env_prefix = member.upper()
        username_default = "sven" if member == "Sven" else member.lower()
        password_fallback = os.getenv("ADMIN_PASSWORD") if member == "Sven" else ""
        username = (os.getenv(f"{env_prefix}_USERNAME") or (os.getenv("ADMIN_USERNAME") if member == "Sven" else "") or username_default).strip() or username_default
        password = os.getenv(f"{env_prefix}_PASSWORD") or password_fallback
        display_name = (os.getenv(f"{env_prefix}_DISPLAY_NAME") or member).strip() or member
        accounts.append({"staff_name": member, "username": username, "password": password, "display_name": display_name})
    return accounts


def get_staff_members(db=None):
    fallback = list(dict.fromkeys(CONFIGURED_STAFF_MEMBERS))
    try:
        conn = db or get_db()
        rows = conn.execute("SELECT staff_name FROM staff_users WHERE COALESCE(staff_name, '') <> '' ORDER BY created_at ASC, id ASC").fetchall()
    except Exception:
        rows = []
    names = []
    for row in rows:
        name = (row["staff_name"] or "").strip()
        if name and name not in names:
            names.append(name)
    return names or fallback


def get_staff_options(db=None):
    return ["Alle", *get_staff_members(db)]


def get_default_staff(db=None):
    members = get_staff_members(db)
    return members[0] if members else DEFAULT_STAFF


def normalize_service_selection(raw_value):
    if isinstance(raw_value, (list, tuple)):
        items = raw_value
    else:
        items = str(raw_value or "").split(",")
    selected = []
    for item in items:
        key = re.sub(r"[^a-z0-9_-]+", "", str(item).strip().lower())
        if key and key in SERVICE_PRESET_MAP and key not in selected:
            selected.append(key)
    return selected


def service_summary_from_selection(selected_services):
    labels = [SERVICE_PRESET_MAP[item]["label"] for item in selected_services if item in SERVICE_PRESET_MAP]
    return " + ".join(labels)


def service_time_defaults(selected_services):
    active = 0
    processing = 0
    for item in selected_services:
        preset = SERVICE_PRESET_MAP.get(item)
        if not preset:
            continue
        active += int(preset.get("active") or 0)
        processing += int(preset.get("processing") or 0)
    return active or 30, processing


def rounded_duration(value, default=30, minimum=0, maximum=480):
    minutes = _safe_int(value, default=default, minimum=minimum, maximum=maximum)
    if minutes <= 0:
        return 0
    return max(15, int((minutes + 14) // 15) * 15)


def appointment_payload_from_form(form, *, db=None):
    default_staff = get_default_staff(db)
    selected_services = normalize_service_selection(
        form.getlist("selected_services") or form.get("selected_services")
    )
    computed_summary = service_summary_from_selection(selected_services)
    service_summary = (form.get("service_summary") or computed_summary).strip() or computed_summary
    default_active, default_processing = service_time_defaults(selected_services)
    if not selected_services:
        default_active, default_processing = 30, 0

    appointment_at = (form.get("appointment_at") or "").strip()
    active_until = (form.get("active_until") or "").strip()
    finish_at = (form.get("finish_at") or "").strip()
    start_dt = _parse_dt_safe(appointment_at)
    active_until_dt = _parse_dt_safe(active_until)
    finish_dt = _parse_dt_safe(finish_at)

    duration_minutes = rounded_duration(
        form.get("duration_minutes"),
        default=default_active,
        minimum=15,
        maximum=480,
    )
    processing_minutes = rounded_duration(
        form.get("processing_minutes"),
        default=default_processing,
        minimum=0,
        maximum=480,
    )

    if start_dt and active_until_dt and active_until_dt > start_dt:
        active_minutes = int((active_until_dt - start_dt).total_seconds() // 60)
        duration_minutes = rounded_duration(active_minutes, default=default_active, minimum=15, maximum=480)

    if start_dt and finish_dt and finish_dt > start_dt:
        total_minutes = int((finish_dt - start_dt).total_seconds() // 60)
        remaining_minutes = max(0, total_minutes - duration_minutes)
        processing_minutes = rounded_duration(remaining_minutes, default=default_processing, minimum=0, maximum=480)
    elif start_dt and active_until_dt and active_until_dt <= start_dt:
        duration_minutes = rounded_duration(default_active, default=30, minimum=15, maximum=480)

    raw_title = (form.get("title") or "").strip()
    title = raw_title if raw_title and raw_title != "Salon-Termin" else (service_summary or "Salon-Termin")
    return {
        "title": title,
        "service_codes": ",".join(selected_services),
        "service_summary": service_summary,
        "duration_minutes": duration_minutes,
        "processing_minutes": processing_minutes,
        "status": (form.get("status") or "geplant").strip() or "geplant",
        "staff_name": _normalize_staff_name(form.get("staff_name"), default=default_staff, db=db),
        "notes": (form.get("notes") or "").strip(),
        "reminder_hours": _safe_int(form.get("reminder_hours", "24") or 24, default=24, minimum=0, maximum=720),
    }


def default_login_options(db=None):
    config_map = {item["staff_name"]: item for item in staff_account_configs()}
    return [{"staff_name": name, "label": (config_map.get(name, {}).get("display_name") or name)} for name in get_staff_members(db)]


def user_has_password(user_row):
    if not user_row:
        return False
    return bool((user_row["password"] or "").strip())


def fetch_user_for_staff(conn, staff_name):
    return conn.execute(
        "SELECT * FROM staff_users WHERE staff_name = ? LIMIT 1",
        (staff_name,),
    ).fetchone()


def resolve_staff_name_for_user(user_row, db=None):
    if not user_row:
        return get_default_staff(db)
    staff_members = get_staff_members(db)
    staff_name = (user_row["staff_name"] or "").strip() if "staff_name" in user_row.keys() else ""
    if staff_name in staff_members:
        return staff_name
    username = (user_row["username"] or "").strip().lower() if "username" in user_row.keys() else ""
    display_name = (user_row["display_name"] or "").strip().lower() if "display_name" in user_row.keys() else ""
    for member in staff_members:
        if username == member.lower() or display_name == member.lower():
            return member
    return get_default_staff(db)


def is_admin_staff_name(staff_name, db=None):
    normalized = _normalize_staff_name(staff_name, default=get_default_staff(db), db=db)
    return normalized in ADMIN_STAFF_NAMES


def is_admin_session():
    return bool(session.get("admin_logged_in")) and is_admin_staff_name(session.get("staff_name"))


def current_ui_world():
    if not session.get("admin_logged_in"):
        return "public"
    if is_admin_session():
        return "staff" if (session.get("ui_world") or "admin").strip().lower() == "staff" else "admin"
    return "staff"


def is_staff_world_session():
    return bool(session.get("admin_logged_in")) and current_ui_world() == "staff"


def is_admin_world_session():
    return bool(session.get("admin_logged_in")) and current_ui_world() == "admin"


def set_ui_world(world):
    next_world = (world or "").strip().lower()
    if not is_admin_session():
        session["ui_world"] = "staff"
        return "staff"
    session["ui_world"] = "staff" if next_world == "staff" else "admin"
    return session["ui_world"]


def default_route_after_login(staff_name):
    if SAFE_MODE:
        if is_admin_staff_name(staff_name):
            return url_for("test_admin_dashboard")
        return url_for("test_staff_today")
    if is_admin_staff_name(staff_name):
        return url_for("admin_home")
    return url_for("staff_today")


def current_staff_name(db=None):
    if session.get("admin_logged_in"):
        return _normalize_staff_name(session.get("staff_name"), default=get_default_staff(db), db=db)
    return get_default_staff(db)


def staff_members_for_simple_mode(db=None):
    members = get_staff_members(db)
    preferred = [name for name in PRIMARY_BOOKING_STAFF if name in members]
    if preferred:
        return preferred
    without_admin = [name for name in members if not is_admin_staff_name(name, db=db)]
    return without_admin or members


def default_staff_for_simple_mode(db=None):
    current = current_staff_name(db)
    simple_members = staff_members_for_simple_mode(db)
    if current in simple_members:
        return current
    return simple_members[0] if simple_members else get_default_staff(db)


def login_user(user, *, staff_name=None, remember_device=False):
    resolved_staff = staff_name or resolve_staff_name_for_user(user)
    session.permanent = bool(remember_device)
    session["admin_logged_in"] = True
    session["admin_name"] = user["display_name"] or user["username"]
    session["staff_name"] = resolved_staff
    session["username"] = user["username"]
    session["ui_world"] = "admin" if is_admin_staff_name(resolved_staff) else "staff"


def passkeys_ready():
    return all([
        generate_authentication_options,
        generate_registration_options,
        options_to_json,
        verify_authentication_response,
        verify_registration_response,
        AuthenticationCredential,
        AuthenticatorSelectionCriteria,
        PublicKeyCredentialDescriptor,
        RegistrationCredential,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    ])


def current_passkey_rp_id():
    configured = (os.getenv("WEBAUTHN_RP_ID") or "").strip()
    if configured:
        return configured
    host = (request.host or "").split(":", 1)[0].strip().lower()
    return host or "localhost"


def current_passkey_origin():
    configured = (os.getenv("WEBAUTHN_ORIGIN") or "").strip()
    if configured:
        return configured.rstrip("/")
    return request.url_root.rstrip("/")


def current_passkey_rp_name():
    return (os.getenv("WEBAUTHN_RP_NAME") or "Salon Karola").strip() or "Salon Karola"


def passkey_credentials_for_user(db, user_id):
    return db.execute(
        "SELECT * FROM webauthn_credentials WHERE user_id = ? ORDER BY created_at ASC",
        (user_id,),
    ).fetchall()


def find_passkey_by_credential_id(db, credential_id):
    return db.execute(
        "SELECT wc.*, su.username, su.display_name, su.staff_name FROM webauthn_credentials wc JOIN staff_users su ON su.id = wc.user_id WHERE wc.credential_id = ? LIMIT 1",
        (credential_id,),
    ).fetchone()


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


CUSTOMER_PERSONAL_PHONE_ALIASES = [
    "Customer_PersoenlichesTelefon",
    "Customer_Pers?nlichesTelefon",
    "Customer_Pers??nlichesTelefon",
    "Customer_Pers?nlichesTelefon",
]


def customer_personal_phone_column(cols=None):
    cols = list(cols or customer_columns())
    for column_name in CUSTOMER_PERSONAL_PHONE_ALIASES:
        if column_name in cols:
            return column_name
    for column_name in cols:
        lowered = str(column_name).lower()
        if lowered.startswith("customer_pers") and "telefon" in lowered:
            return column_name
    return CUSTOMER_PERSONAL_PHONE_ALIASES[0]


def customer_column_reference(column_name, prefix=""):
    cols = customer_columns()
    if column_name not in cols:
        return None
    qualifier = f"{prefix}." if prefix else ""
    return f'{qualifier}"{column_name}"'


def customer_mobile_reference(prefix=""):
    return customer_column_reference("Customer_Mobiltelefon", prefix)


def customer_personal_phone_reference(prefix=""):
    column_name = customer_personal_phone_column()
    return customer_column_reference(column_name, prefix)


def customer_contact_select_sql(prefix=""):
    email_ref = customer_column_reference("_mail", prefix)
    mobile_ref = customer_mobile_reference(prefix)
    phone_ref = customer_personal_phone_reference(prefix)
    city_ref = customer_column_reference("Customer_Stadt", prefix)
    email_sql = f"COALESCE({email_ref}, '')" if email_ref else "''"
    mobile_sql = f"COALESCE({mobile_ref}, '')" if mobile_ref else "''"
    phone_sql = f"COALESCE({phone_ref}, '')" if phone_ref else "''"
    city_sql = f"COALESCE({city_ref}, '')" if city_ref else "''"
    return email_sql, mobile_sql, phone_sql, city_sql


# ---------- Setup ----------
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
            CREATE TABLE IF NOT EXISTS appointment_pings (
                token TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_by TEXT DEFAULT '',
                used_at TEXT
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
                provider TEXT NOT NULL DEFAULT 'webpush',
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
                staff_name TEXT DEFAULT '',
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS webauthn_credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                credential_id TEXT NOT NULL UNIQUE,
                public_key TEXT NOT NULL,
                sign_count INTEGER NOT NULL DEFAULT 0,
                transports TEXT,
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                FOREIGN KEY(user_id) REFERENCES staff_users(id)
            )
            """
        )

        sync_default_mail_templates(conn)

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
        add_column_if_missing("appointments", "service_codes", "TEXT DEFAULT ''")
        add_column_if_missing("appointments", "service_summary", "TEXT DEFAULT ''")
        add_column_if_missing("appointments", "duration_minutes", "INTEGER DEFAULT 30")
        add_column_if_missing("appointments", "processing_minutes", "INTEGER DEFAULT 0")
        add_column_if_missing(
            "push_subscriptions",
            "provider",
            "TEXT DEFAULT 'webpush'",
            fill_sql="UPDATE push_subscriptions SET provider = CASE WHEN lower(COALESCE(endpoint, '')) LIKE 'fcm:%' THEN 'fcm' ELSE 'webpush' END WHERE provider IS NULL OR provider = ''",
        )
        add_column_if_missing("staff_users", "staff_name", "TEXT DEFAULT ''", fill_sql="UPDATE staff_users SET staff_name = CASE lower(COALESCE(display_name, username, '')) WHEN 'ute' THEN 'Ute' WHEN 'jessi' THEN 'Jessi' WHEN 'sven' THEN 'Sven' ELSE COALESCE(staff_name, '') END")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_appointments_at ON appointments(appointment_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_appointments_staff_at ON appointments(staff_name, appointment_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_appointments_customer_at ON appointments(customer_id, appointment_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_name ON _Customers(_name, _firstname)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_birthdate ON _Customers(_birthdate)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_push_staff ON push_subscriptions(staff_name)")

        ensure_manual_placeholder_customer(conn)
        conn.commit()


def ensure_default_admin(force_reset=False):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                display_name TEXT,
                staff_name TEXT DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )

        rows = conn.execute("SELECT id, username, password, display_name, staff_name FROM staff_users").fetchall()
        for row in rows:
            stored_password = row[2] or ""
            desired_staff = row[4] or ""
            if not desired_staff:
                username_l = (row[1] or "").strip().lower()
                display_l = (row[3] or "").strip().lower()
                for member in STAFF_MEMBERS:
                    if username_l == member.lower() or display_l == member.lower():
                        desired_staff = member
                        break
            updates = []
            params = []
            if stored_password and not password_is_hashed(stored_password):
                updates.append("password = ?")
                params.append(hash_password(stored_password))
            if desired_staff and desired_staff != (row[4] or ""):
                updates.append("staff_name = ?")
                params.append(desired_staff)
            if updates:
                params.append(row[0])
                conn.execute(f"UPDATE staff_users SET {', '.join(updates)} WHERE id = ?", params)

        for account in staff_account_configs():
            existing = conn.execute("SELECT id, password FROM staff_users WHERE staff_name = ? LIMIT 1", (account["staff_name"],)).fetchone()
            if existing:
                if force_reset and account["password"]:
                    conn.execute(
                        "UPDATE staff_users SET username = ?, password = ?, display_name = ?, staff_name = ? WHERE id = ?",
                        (account["username"], hash_password(account["password"]), account["display_name"], account["staff_name"], existing[0]),
                    )
                elif account["display_name"]:
                    duplicate_username = conn.execute(
                        "SELECT id FROM staff_users WHERE username = ? AND id <> ? LIMIT 1",
                        (account["username"], existing[0]),
                    ).fetchone()
                    if duplicate_username:
                        conn.execute(
                            "UPDATE staff_users SET display_name = ?, staff_name = ? WHERE id = ?",
                            (account["display_name"], account["staff_name"], existing[0]),
                        )
                    else:
                        conn.execute(
                            "UPDATE staff_users SET username = ?, display_name = ?, staff_name = ? WHERE id = ?",
                            (account["username"], account["display_name"], account["staff_name"], existing[0]),
                        )
                continue

            conn.execute(
                """
                INSERT INTO staff_users(username, password, display_name, staff_name, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    account["username"],
                    hash_password(account["password"]) if account["password"] else "",
                    account["display_name"],
                    account["staff_name"],
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
        conn.commit()


# ---------- Mail ----------
def email_delivery_mode():
    requested = (os.getenv("EMAIL_PROVIDER") or os.getenv("MAIL_PROVIDER") or "").strip().lower()
    if requested in {"resend", "smtp"}:
        return requested
    if smtp_ready():
        return "smtp"
    if resend_ready():
        return "resend"
    return "none"


def resend_ready():
    api_key = (os.getenv("RESEND_API_KEY") or "").strip()
    sender = (os.getenv("RESEND_FROM") or "").strip()
    return bool(api_key and sender)


def smtp_ready():
    needed = ["SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_SENDER"]
    return all((os.getenv(k) or "").strip() for k in needed)


def mail_ready():
    mode = email_delivery_mode()
    if mode == "resend":
        return resend_ready()
    if mode == "smtp":
        return smtp_ready()
    return False


def mail_status_summary():
    resend_from = (os.getenv("RESEND_FROM") or "").strip()
    resend_from_domain = ""
    if "@" in resend_from:
        resend_from_domain = resend_from.split("@", 1)[1].strip(" >")
    requested_provider = (os.getenv("EMAIL_PROVIDER") or os.getenv("MAIL_PROVIDER") or "").strip()
    mode = email_delivery_mode()
    return {
        "requested_provider": requested_provider or "auto",
        "provider": mode,
        "mail_ready": mail_ready(),
        "resend_ready": resend_ready(),
        "smtp_ready": smtp_ready(),
        "resend_from_set": bool(resend_from),
        "resend_from": resend_from,
        "resend_from_domain": resend_from_domain,
        "resend_from_is_onboarding": resend_from.lower().endswith("@resend.dev"),
        "resend_api_key_set": bool((os.getenv("RESEND_API_KEY") or "").strip()),
        "smtp_host_set": bool((os.getenv("SMTP_HOST") or "").strip()),
        "smtp_sender_set": bool((os.getenv("SMTP_SENDER") or "").strip()),
    }


def sms_delivery_mode():
    requested = (os.getenv("SMS_PROVIDER") or "auto").strip().lower()
    if requested in {"twilio", "none", "off", "disabled"}:
        return requested
    if (os.getenv("TWILIO_ACCOUNT_SID") or "").strip() and (os.getenv("TWILIO_AUTH_TOKEN") or "").strip():
        return "twilio"
    return "none"


def sms_ready():
    mode = sms_delivery_mode()
    if mode != "twilio":
        return False
    account_sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = (os.getenv("TWILIO_FROM_NUMBER") or "").strip()
    messaging_service_sid = (os.getenv("TWILIO_MESSAGING_SERVICE_SID") or "").strip()
    return bool(account_sid and auth_token and (from_number or messaging_service_sid))


def whatsapp_delivery_mode():
    requested = (os.getenv("WHATSAPP_PROVIDER") or "auto").strip().lower()
    if requested in {"twilio", "none", "off", "disabled"}:
        return requested
    if (os.getenv("TWILIO_ACCOUNT_SID") or "").strip() and (os.getenv("TWILIO_AUTH_TOKEN") or "").strip() and (os.getenv("TWILIO_WHATSAPP_FROM") or "").strip():
        return "twilio"
    return "none"


def whatsapp_ready():
    mode = whatsapp_delivery_mode()
    if mode != "twilio":
        return False
    return bool(
        (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
        and (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
        and (os.getenv("TWILIO_WHATSAPP_FROM") or "").strip()
    )


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
        if response.status_code == 403:
            raise RuntimeError(
                "Resend blockiert den Versand, weil die Absenderdomain nicht verifiziert ist oder RESEND_FROM auf die falsche Domain zeigt."
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
    if not (to_email or "").strip():
        raise RuntimeError("Empfänger-E-Mail fehlt.")
    if not mail_ready():
        raise RuntimeError("E-Mail-Versand ist nicht konfiguriert.")
    mode = email_delivery_mode()
    if mode == "resend":
        return send_email_via_resend(to_email, subject, body)
    if mode == "smtp":
        return send_email_via_smtp(to_email, subject, body)
    raise RuntimeError("E-Mail-Versand ist nicht konfiguriert.")


def send_sms_via_twilio(to_number, body):
    account_sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = (os.getenv("TWILIO_FROM_NUMBER") or "").strip()
    messaging_service_sid = (os.getenv("TWILIO_MESSAGING_SERVICE_SID") or "").strip()

    if not account_sid or not auth_token:
        raise RuntimeError("Twilio ist nicht vollstÃ¤ndig konfiguriert. TWILIO_ACCOUNT_SID oder TWILIO_AUTH_TOKEN fehlt.")
    if not from_number and not messaging_service_sid:
        raise RuntimeError("Twilio ist nicht vollstÃ¤ndig konfiguriert. Bitte TWILIO_FROM_NUMBER oder TWILIO_MESSAGING_SERVICE_SID setzen.")

    normalized_to = normalized_phone_number(to_number)
    if not normalized_to:
        raise RuntimeError("Es ist keine gÃ¼ltige Mobil- oder Telefonnummer fÃ¼r SMS vorhanden.")

    payload = {
        "To": normalized_to,
        "Body": body,
    }
    if messaging_service_sid:
        payload["MessagingServiceSid"] = messaging_service_sid
    else:
        payload["From"] = from_number

    try:
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            auth=(account_sid, auth_token),
            data=payload,
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Twilio Fehler: HTTP {response.status_code} {response.text}")
        return response.json()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Twilio Netzwerkfehler: {exc}") from exc


def send_sms(to_number, body):
    mode = sms_delivery_mode()
    if mode == "twilio":
        return send_sms_via_twilio(to_number, body)
    raise RuntimeError("Kein SMS-Provider konfiguriert. Bitte TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN und TWILIO_FROM_NUMBER oder TWILIO_MESSAGING_SERVICE_SID setzen.")


def send_whatsapp_via_twilio(to_number, body):
    account_sid = (os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = (os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_number = (os.getenv("TWILIO_WHATSAPP_FROM") or "").strip()

    if not account_sid or not auth_token or not from_number:
        raise RuntimeError("Twilio WhatsApp ist nicht vollstÃ¤ndig konfiguriert. Bitte TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN und TWILIO_WHATSAPP_FROM setzen.")

    normalized_to = normalized_phone_number(to_number)
    if not normalized_to:
        raise RuntimeError("Es ist keine gÃ¼ltige Mobil- oder Telefonnummer fÃ¼r WhatsApp vorhanden.")

    payload = {
        "From": from_number if from_number.startswith("whatsapp:") else f"whatsapp:{from_number}",
        "To": f"whatsapp:{normalized_to}",
        "Body": body,
    }

    try:
        response = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
            auth=(account_sid, auth_token),
            data=payload,
            timeout=30,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Twilio WhatsApp Fehler: HTTP {response.status_code} {response.text}")
        return response.json()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Twilio WhatsApp Netzwerkfehler: {exc}") from exc


def send_whatsapp(to_number, body):
    mode = whatsapp_delivery_mode()
    if mode == "twilio":
        return send_whatsapp_via_twilio(to_number, body)
    raise RuntimeError("Kein WhatsApp-Provider konfiguriert. Bitte TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN und TWILIO_WHATSAPP_FROM setzen.")


# ---------- Utilities ----------
def _row_value(row, *keys, default=""):
    if row is None:
        return default
    aliases = {
        "Customer_PersoenlichesTelefon": [
            "Customer_PersoenlichesTelefon",
            "Customer_Pers?nlichesTelefon",
            "Customer_Pers??nlichesTelefon",
            "Customer_Pers?nlichesTelefon",
        ],
        "Customer_Pers?nlichesTelefon": [
            "Customer_PersoenlichesTelefon",
            "Customer_Pers?nlichesTelefon",
            "Customer_Pers??nlichesTelefon",
            "Customer_Pers?nlichesTelefon",
        ],
    }
    available = set(row.keys()) if hasattr(row, "keys") else set()
    for key in keys:
        for candidate in aliases.get(key, [key]):
            if candidate in available:
                value = row[candidate]
                if value is None:
                    continue
                if isinstance(value, str):
                    if value.strip():
                        return value
                else:
                    return value
    return default


def customer_full_name(customer):
    first_name = _row_value(customer, "_firstname") or ""
    last_name = _row_value(customer, "_name") or ""
    return f"{first_name} {last_name}".strip() or "Kunde"


def customer_phone(customer):
    return _row_value(customer, "Customer_Mobiltelefon", "Customer_PersoenlichesTelefon") or ""


def sync_default_mail_templates(conn):
    defaults = {
        "birthdate": {
            "subject": "Salon Karola Happy Birthday",
            "body": '''Lieber {name} alles gute zum Geburtstag 🎂!

Alles gute zum Geburtstag! 🎂 Wir vom Salon Karola möchten Ihnen an Ihrem besonderen Tag ein strahlendes Lächeln ins Gesicht zaubern.

Als kleines Geschenk und um Ihren Ehrentag gebührend zu feiern, schenken wir Ihnen 10% Rabatt auf Ihre nächste Behandlung in unserem Salon! 💇‍♀️✨ Zeigen Sie diese E-Mail einfach bei Ihrem nächsten Besuch vor.

Wir wünschen Ihnen einen wunderschönen Tag, Gesundheit und viele schöne Momente.

Herzliche Grüße
Ihr Team vom Salon Karola''',
        },
        "appointment": {
            "subject": "Terminerinnerung für {name}",
            "body": '''Hallo {name},

wir erinnern dich an deinen Termin am {termin}.

Bei Fragen erreichst du uns unter 07051/6344.

Herzliche Grüße
Salon Karola''',
        },
    }

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _MailTemplates (
            id TEXT PRIMARY KEY,
            subject TEXT NOT NULL,
            body TEXT NOT NULL
        )
        """
    )

    for template_id, template in defaults.items():
        row = conn.execute(
            "SELECT subject, body FROM _MailTemplates WHERE id = ?",
            (template_id,),
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO _MailTemplates(id, subject, body) VALUES (?, ?, ?)",
                (template_id, template["subject"], template["body"]),
            )
            continue

        subject = (row[0] or "").strip()
        body = (row[1] or "").strip()

        reset_needed = False
        if not subject or not body:
            reset_needed = True
        if "Matthias" in subject or "Matthias" in body:
            reset_needed = True
        if template_id == "birthdate" and "{name}" not in body:
            reset_needed = True
        if template_id == "appointment" and "{termin}" not in body:
            reset_needed = True

        if reset_needed:
            conn.execute(
                "UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?",
                (template["subject"], template["body"], template_id),
            )

    clean_templates = {
        "birthdate": (
            "Salon Karola wünscht alles Gute zum Geburtstag, {vorname}",
            "Liebe/r {name},\n\n"
            "das Team vom Salon Karola wünscht Ihnen alles Gute zum Geburtstag und einen wunderschönen Tag.\n\n"
            "Als kleine Aufmerksamkeit schenken wir Ihnen 10 % Rabatt auf Ihre nächste Behandlung in unserem Salon.\n\n"
            "Zeigen Sie diese E-Mail einfach bei Ihrem nächsten Besuch vor.\n\n"
            "Herzliche Grüße\n"
            "Ihr Team vom Salon Karola",
        ),
        "appointment": (
            "Terminerinnerung Salon Karola",
            "Hallo {name},\n\n"
            "wir erinnern Sie an Ihren Termin am {termin}.\n\n"
            "Falls Sie den Termin nicht wahrnehmen können, geben Sie uns bitte rechtzeitig Bescheid.\n\n"
            "Telefon: 07051/6344\n\n"
            "Herzliche Grüße\n"
            "Ihr Team vom Salon Karola",
        ),
    }
    for template_id, values in clean_templates.items():
        row = conn.execute("SELECT subject, body FROM _MailTemplates WHERE id = ? LIMIT 1", (template_id,)).fetchone()
        subject = (row[0] or "") if row else ""
        body = (row[1] or "") if row else ""
        if ("Ã" in subject or "Ã" in body or "Matthias" in subject or "Matthias" in body or not subject.strip() or not body.strip()):
            conn.execute("UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?", (values[0], values[1], template_id))


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

    defaults = {
        "birthdate": (
            "Salon Karola wünscht alles Gute zum Geburtstag, {vorname}",
            "Liebe/r {name},\n\n"
            "das Team vom Salon Karola wünscht Ihnen alles Gute zum Geburtstag und einen wunderschönen Tag.\n\n"
            "Als kleine Aufmerksamkeit schenken wir Ihnen 10 % Rabatt auf Ihre nächste Behandlung in unserem Salon.\n\n"
            "Zeigen Sie diese E-Mail einfach bei Ihrem nächsten Besuch vor.\n\n"
            "Herzliche Grüße\n"
            "Ihr Team vom Salon Karola",
        ),
        "appointment": (
            "Terminerinnerung Salon Karola",
            "Hallo {name},\n\n"
            "wir erinnern Sie an Ihren Termin am {termin}.\n\n"
            "Falls Sie den Termin nicht wahrnehmen können, geben Sie uns bitte rechtzeitig Bescheid.\n\n"
            "Telefon: 07051/6344\n\n"
            "Herzliche Grüße\n"
            "Ihr Team vom Salon Karola",
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


def delivery_already_sent_today(message_type, customer_id=None, recipient="", subject=""):
    recipient = (recipient or "").strip()
    subject = (subject or "").strip()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM email_log
            WHERE email_type = ?
              AND (? IS NULL OR customer_id = ?)
              AND recipient = ?
              AND (? = '' OR subject = ?)
              AND status = 'sent'
              AND date(sent_at) = date('now', 'localtime')
            LIMIT 1
            """,
            (message_type, customer_id, customer_id, recipient, subject, subject),
        ).fetchone()
    return row is not None


def email_already_sent_today(email_type, customer_id=None, recipient="", subject=""):
    return delivery_already_sent_today(email_type, customer_id=customer_id, recipient=recipient, subject=subject)


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


FCM_SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]


def _json_file_if_exists(path):
    try:
        file_path = Path(path)
        if file_path.exists():
            return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def firebase_service_account_info():
    raw_json = (os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw_json:
        try:
            return json.loads(raw_json)
        except Exception:
            pass
    raw_b64 = (os.getenv("FIREBASE_SERVICE_ACCOUNT_B64") or "").strip()
    if raw_b64:
        try:
            return json.loads(base64.b64decode(raw_b64).decode("utf-8"))
        except Exception:
            pass
    file_path = (os.getenv("FIREBASE_SERVICE_ACCOUNT_FILE") or "").strip()
    if file_path:
        info = _json_file_if_exists(file_path)
        if info:
            return info
    info = _json_file_if_exists(BASE_DIR / "firebase-service-account.json")
    if info:
        return info
    return None


def firebase_project_id():
    explicit = (os.getenv("FIREBASE_PROJECT_ID") or "").strip()
    if explicit:
        return explicit
    service_info = firebase_service_account_info() or {}
    if service_info.get("project_id"):
        return str(service_info.get("project_id")).strip()
    google_services = _json_file_if_exists(BASE_DIR / "android" / "app" / "google-services.json") or {}
    project_info = google_services.get("project_info") or {}
    if project_info.get("project_id"):
        return str(project_info.get("project_id")).strip()
    return ""


def fcm_ready():
    if not ENABLE_PUSH or not ENABLE_FIREBASE:
        return False
    return bool(firebase_project_id() and firebase_service_account_info() and GoogleAuthRequest and service_account)


def push_delivery_ready():
    if not ENABLE_PUSH:
        return False
    return bool(vapid_ready() or fcm_ready())


def fcm_access_token():
    info = firebase_service_account_info()
    if not info:
        raise RuntimeError("Firebase Service Account fehlt.")
    if not GoogleAuthRequest or not service_account:
        raise RuntimeError("google-auth ist auf dem Server nicht installiert.")
    credentials = service_account.Credentials.from_service_account_info(info, scopes=FCM_SCOPES)
    credentials.refresh(GoogleAuthRequest())
    if not credentials.token:
        raise RuntimeError("FCM Zugriffstoken konnte nicht erzeugt werden.")
    return credentials.token


def _push_provider(row):
    provider = ((row["provider"] if "provider" in row.keys() else "") or "").strip().lower()
    if provider:
        return provider
    endpoint = ((row["endpoint"] if "endpoint" in row.keys() else "") or "").strip().lower()
    if endpoint.startswith("fcm:"):
        return "fcm"
    return "webpush"


def fcm_send_to_token(device_token, title, body, url="/calendar"):
    project_id = firebase_project_id()
    if not project_id:
        raise RuntimeError("Firebase Projekt-ID fehlt.")
    access_token = fcm_access_token()
    response = requests.post(
        f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        },
        json={
            "message": {
                "token": device_token,
                "notification": {"title": title, "body": body},
                "data": {
                    "title": title,
                    "body": body,
                    "url": url,
                },
                "android": {
                    "priority": "high",
                    "notification": {
                        "channel_id": "salon_karola_default",
                        "click_action": "OPEN_ACTIVITY_1",
                    },
                },
            }
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"FCM Fehler: HTTP {response.status_code} {response.text}")
    return response.json() if response.text else {"ok": True}


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
    active_staff = get_staff_members(db)
    params = []
    query = "SELECT * FROM push_subscriptions"
    if staff_name in active_staff:
        query += " WHERE staff_name = ?"
        params.append(staff_name)
    query += " ORDER BY staff_name ASC, COALESCE(last_success_at, last_seen_at, updated_at, created_at) DESC"
    rows = db.execute(query, params).fetchall()
    items = []
    for row in rows:
        items.append({
            "id": row["id"],
            "staff_name": row["staff_name"] or DEFAULT_STAFF,
            "device_name": _push_device_label(row),
            "user_agent": (row["user_agent"] or "")[:180],
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
            "last_seen_at": row["last_seen_at"] or "",
            "last_success_at": row["last_success_at"] or "",
            "last_test_at": row["last_test_at"] or "",
            "last_error": row["last_error"] or "",
            "fail_count": int(row["fail_count"] or 0),
            "provider": _push_provider(row),
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
        error_text = str(exc)
        mismatch = status_code == 403 and ("vapid" in error_text.lower() and "credential" in error_text.lower())
        if status_code in (404, 410) or mismatch:
            db.execute("DELETE FROM push_subscriptions WHERE id = ?", (row["id"],))
            db.commit()
            if mismatch:
                return {"ok": False, "sent": 0, "skipped": 1, "errors": ["Gerät war mit alten Push-Schlüsseln registriert und wurde entfernt. Bitte Push auf diesem Gerät neu aktivieren."]}
            return {"ok": False, "sent": 0, "skipped": 1, "errors": ["Gerät war nicht mehr gültig und wurde entfernt."]}
        fail_count = int(row["fail_count"] or 0) + 1
        _touch_push_subscription(
            row["id"],
            last_error=error_text[:500],
            fail_count=fail_count,
            updated_at=now,
        )
        return {"ok": False, "sent": 0, "skipped": 0, "errors": [error_text]}
    except Exception as exc:
        fail_count = int(row["fail_count"] or 0) + 1
        _touch_push_subscription(
            row["id"],
            last_error=str(exc)[:500],
            fail_count=fail_count,
            updated_at=now,
        )
        return {"ok": False, "sent": 0, "skipped": 0, "errors": [str(exc)]}


def fcm_send_to_subscription_row(row, title, body, url="/calendar"):
    db = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    try:
        payload = json.loads(row["subscription_json"])
        token = (payload.get("token") or "").strip()
        if not token:
            raise RuntimeError("FCM-Token fehlt in der Registrierung.")
        fcm_send_to_token(token, title, body, url)
        _touch_push_subscription(
            row["id"],
            last_success_at=now,
            last_error="",
            fail_count=0,
            updated_at=now,
            last_seen_at=now,
        )
        return {"ok": True, "sent": 1, "skipped": 0, "errors": []}
    except Exception as exc:
        error_text = str(exc)
        normalized = error_text.upper()
        if "UNREGISTERED" in normalized or "REGISTRATION_TOKEN_NOT_REGISTERED" in normalized or "NOT_FOUND" in normalized:
            db.execute("DELETE FROM push_subscriptions WHERE id = ?", (row["id"],))
            db.commit()
            return {"ok": False, "sent": 0, "skipped": 1, "errors": ["Android-App war nicht mehr gueltig registriert und wurde entfernt. Bitte Push in der App neu aktivieren."]}
        fail_count = int(row["fail_count"] or 0) + 1
        _touch_push_subscription(
            row["id"],
            last_error=error_text[:500],
            fail_count=fail_count,
            updated_at=now,
        )
        return {"ok": False, "sent": 0, "skipped": 0, "errors": [error_text]}


def send_push_to_subscription_row(row, title, body, url="/calendar"):
    provider = _push_provider(row)
    if provider == "fcm":
        if not fcm_ready():
            return {"ok": False, "sent": 0, "skipped": 0, "errors": ["FCM ist serverseitig noch nicht konfiguriert."]}
        return fcm_send_to_subscription_row(row, title, body, url)
    if not vapid_ready():
        return {"ok": False, "sent": 0, "skipped": 0, "errors": ["VAPID ist serverseitig noch nicht konfiguriert."]}
    return webpush_send_to_subscription_row(row, title, body, url)


def webpush_send_to_staff(target_staff, title, body, url="/calendar"):
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
        result = send_push_to_subscription_row(row, title, body, url)
        sent += int(result.get("sent") or 0)
        skipped += int(result.get("skipped") or 0)
        errors.extend(result.get("errors") or [])
    return {"sent": sent, "skipped": skipped, "errors": errors}


def webpush_send_to_all_staff(title, body, url="/calendar"):
    totals = {"sent": 0, "skipped": 0, "errors": []}
    for target_staff in get_staff_members():
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
    staff_name = (appt["staff_name"] or DEFAULT_STAFF).strip() or DEFAULT_STAFF
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

    actor = _normalize_staff_name(actor_name or staff_name, default=DEFAULT_STAFF)
    targets = other_staff_members(actor)
    customer_name = (manual_name or "").strip()
    if customer_id:
        customer = get_db().execute(
            "SELECT _firstname, _name FROM _Customers WHERE _id = ?",
            (customer_id,),
        ).fetchone()
        if customer:
            customer_name = f"{customer['_firstname'] or ''} {customer['_name'] or ''}".strip() or customer_name
    if not customer_name:
        customer_name = "Kundin"
    try:
        when_label = datetime.fromisoformat(str(appointment_at)).strftime("%d.%m.%Y um %H:%M")
    except Exception:
        when_label = str(appointment_at)

    push_title = f"Neuer Termin von {actor}"
    push_body = f"{customer_name} - {title} - {when_label} - zustaendig: {staff_name}"
    totals = {"sent": 0, "skipped": 0, "errors": []}
    for target in targets:
        result = webpush_send_to_staff(target, push_title, push_body, "/calendar?view=day")
        totals["sent"] += int(result.get("sent") or 0)
        totals["skipped"] += int(result.get("skipped") or 0)
        totals["errors"].extend(result.get("errors") or [])
    return totals


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
            active_minutes = rounded_duration(row["duration_minutes"] if "duration_minutes" in row.keys() else 30, default=30, minimum=15)
            processing_minutes = rounded_duration(row["processing_minutes"] if "processing_minutes" in row.keys() else 0, default=0, minimum=0)
            active_end = appt_dt + timedelta(minutes=active_minutes)
            processing_end = active_end + timedelta(minutes=processing_minutes)
            phase = None
            if appt_dt < slot_end and active_end > pointer:
                phase = "active"
            elif processing_minutes and active_end < slot_end and processing_end > pointer:
                phase = "processing"
            elif processing_minutes and active_end == pointer and processing_end > pointer:
                phase = "processing"
            if not phase:
                continue
            item = _calendar_event_dict(row).copy()
            item["slot_phase"] = phase
            item["slot_label"] = item.get("service_summary") or item["title"]
            item["duration_label"] = f"{active_minutes} Min aktiv"
            item["processing_label"] = f"{processing_minutes} Min Einwirkzeit" if processing_minutes else ""
            slot_items.append(item)
        slots.append({
            "time": pointer.strftime("%H:%M"),
            "items": slot_items,
            "is_now": datetime.now().date() == selected_date and pointer.strftime("%H:%M") == datetime.now().strftime("%H:%M"),
        })
        pointer += timedelta(minutes=15)

    return {
        "open": True,
        "slots": slots,
        "open_label": f"Geoeffnet: {start_str} - {end_str} Uhr",
    }



# ---------- Automated jobs ----------
def run_birthday_job():
    if not mail_ready():
        return {
            "ok": False,
            "checked": 0,
            "sent": 0,
            "skipped": 0,
            "errors": 0,
            "message": "E-Mail-Versand ist nicht konfiguriert.",
            "push_sent": 0,
        }

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
    skipped = 0
    errors = 0
    push_sent = 0

    for customer in customers:
        checked += 1
        mail_key = f"birthday:{customer['_id']}:{current_year}"
        subject, body = render_template_text("birthdate", customer)
        if get_setting(mail_key) or email_already_sent_today("birthday", customer_id=customer["_id"], recipient=customer["_mail"], subject=subject):
            skipped += 1
            continue

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

    return {
        "ok": True,
        "checked": checked,
        "sent": sent,
        "skipped": skipped,
        "errors": errors,
        "message": f"Geburtstagslauf: geprüft={checked}, gesendet={sent}, übersprungen={skipped}, Fehler={errors}",
        "push_sent": push_sent,
    }


def run_appointment_job():
    if not mail_ready():
        return {
            "ok": False,
            "checked": 0,
            "sent": 0,
            "skipped": 0,
            "errors": 0,
            "message": "E-Mail-Versand ist nicht konfiguriert.",
            "push_sent": 0,
        }

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
            ORDER BY a.appointment_at ASC
            """
        ).fetchall()

        checked = 0
        sent = 0
        skipped = 0
        errors = 0
        push_sent = 0

        for appt in appointments:
            checked += 1
            try:
                appt_time = datetime.fromisoformat(appt["appointment_at"])
            except Exception:
                skipped += 1
                continue

            normalized_status = (appt["status"] or "geplant").strip().lower()
            if normalized_status in {"erledigt", "storniert", "nicht erschienen"}:
                skipped += 1
                continue
            if appt_time <= now:
                skipped += 1
                continue

            reminder_at = appt_time - timedelta(hours=int(appt["reminder_hours"] or 24))
            if now < reminder_at:
                skipped += 1
                continue

            subject, body = render_template_text("appointment", appt, appt)
            reminder_email = (appt["_mail"] or "").strip()
            if not reminder_email:
                skipped += 1
                continue

            already_sent = delivery_already_sent_today(
                "appointment_email",
                customer_id=appt["customer_id"],
                recipient=reminder_email,
                subject=subject,
            )
            if already_sent:
                skipped += 1
                continue

            try:
                send_email(reminder_email, subject, body)
                log_email(appt["customer_id"], "appointment_email", subject, body, reminder_email, "sent")
                conn.execute(
                    "UPDATE appointments SET reminder_sent_at = ? WHERE id = ?",
                    (datetime.now().isoformat(timespec="seconds"), appt["id"]),
                )
                conn.commit()
                push_result = _push_appointment_reminder(appt)
                push_sent += int(push_result.get("sent") or 0)
                sent += 1
            except Exception as exc:
                log_email(appt["customer_id"], "appointment_email", subject, body, reminder_email, "error", str(exc))
                errors += 1

    return {
        "ok": True,
        "checked": checked,
        "sent": sent,
        "skipped": skipped,
        "errors": errors,
        "message": f"Terminerinnerung: geprüft={checked}, gesendet={sent}, übersprungen={skipped}, Fehler={errors}",
        "push_sent": push_sent,
    }


def scheduler_tick():
    started_at = datetime.now().isoformat(timespec="seconds")
    try:
        auto_backup = run_auto_backup_if_due()
        birthday_result = run_birthday_job()
        appointment_result = run_appointment_job()
        summary = (
            f"Geburtstage geprüft: {birthday_result.get('checked', 0)}, Mails: {birthday_result.get('sent', 0)}, "
            f"übersprungen: {birthday_result.get('skipped', 0)}, Fehler: {birthday_result.get('errors', 0)} | "
            f"Termine geprüft: {appointment_result.get('checked', 0)}, Erinnerungen: {appointment_result.get('sent', 0)}, "
            f"übersprungen: {appointment_result.get('skipped', 0)}, Fehler: {appointment_result.get('errors', 0)}"
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


def _appointment_ping_cleanup(db=None):
    conn = db or get_db()
    conn.execute(
        "DELETE FROM appointment_pings WHERE expires_at < ?",
        (datetime.now().isoformat(timespec="seconds"),),
    )


def _appointment_ping_payload(raw_payload, *, db=None):
    conn = db or get_db()
    active_staff = get_staff_members(conn)
    default_staff = get_default_staff(conn)
    payload = dict(raw_payload or {})

    customer_id_raw = str(payload.get("customer_id") or "").strip()
    customer_id = customer_id_raw if customer_id_raw.isdigit() else ""
    if customer_id:
        customer_exists = conn.execute("SELECT 1 FROM _Customers WHERE _id = ? LIMIT 1", (int(customer_id),)).fetchone()
        if not customer_exists:
            customer_id = ""

    selected_services = normalize_service_selection(payload.get("selected_services") or "")
    appointment_at = (payload.get("appointment_at") or "").strip()
    active_until = (payload.get("active_until") or "").strip()
    finish_at = (payload.get("finish_at") or "").strip()
    status = (payload.get("status") or "geplant").strip() or "geplant"
    if status not in {"geplant", "bestaetigt", "erledigt", "nicht erschienen"}:
        status = "geplant"

    staff_name = _normalize_staff_name(payload.get("staff_name"), default=default_staff, db=conn)
    if staff_name not in active_staff:
        staff_name = default_staff

    return {
        "customer_id": customer_id,
        "selected_services": ",".join(selected_services),
        "service_summary": service_summary_from_selection(selected_services),
        "appointment_at": appointment_at if _parse_dt_safe(appointment_at) else "",
        "active_until": active_until if _parse_dt_safe(active_until) else "",
        "finish_at": finish_at if _parse_dt_safe(finish_at) else "",
        "staff_name": staff_name,
        "status": status,
        "reminder_hours": _safe_int(payload.get("reminder_hours"), default=24, minimum=0, maximum=168),
        "notes": (payload.get("notes") or "").strip()[:1000],
        "manual_firstname": (payload.get("manual_firstname") or "").strip()[:80],
        "manual_lastname": (payload.get("manual_lastname") or "").strip()[:80],
        "manual_phone": (payload.get("manual_phone") or "").strip()[:80],
        "manual_email": (payload.get("manual_email") or "").strip()[:120],
        "duration_minutes": _safe_int(payload.get("duration_minutes"), default=30, minimum=15, maximum=480),
        "processing_minutes": _safe_int(payload.get("processing_minutes"), default=0, minimum=0, maximum=480),
    }


def create_appointment_ping(raw_payload, *, created_by="", ttl_minutes=180, db=None):
    conn = db or get_db()
    _appointment_ping_cleanup(conn)
    payload = _appointment_ping_payload(raw_payload, db=conn)
    token = secrets.token_urlsafe(9)
    created_at = datetime.now()
    expires_at = created_at + timedelta(minutes=max(10, ttl_minutes))
    conn.execute(
        """
        INSERT INTO appointment_pings(token, payload_json, created_at, expires_at, created_by)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            token,
            json.dumps(payload, ensure_ascii=True),
            created_at.isoformat(timespec="seconds"),
            expires_at.isoformat(timespec="seconds"),
            (created_by or "").strip()[:80],
        ),
    )
    return {
        "token": token,
        "payload": payload,
        "created_at": created_at.isoformat(timespec="seconds"),
        "expires_at": expires_at.isoformat(timespec="seconds"),
    }


def get_appointment_ping(token, *, mark_used=False, db=None):
    conn = db or get_db()
    _appointment_ping_cleanup(conn)
    token = (token or "").strip()
    if not token:
        return None
    row = conn.execute(
        """
        SELECT token, payload_json, created_at, expires_at, created_by, used_at
        FROM appointment_pings
        WHERE token = ?
        LIMIT 1
        """,
        (token,),
    ).fetchone()
    if not row:
        return None
    try:
        expires_at = datetime.fromisoformat(str(row["expires_at"]))
    except Exception:
        return None
    if expires_at < datetime.now():
        conn.execute("DELETE FROM appointment_pings WHERE token = ?", (token,))
        return None
    if mark_used and not row["used_at"]:
        conn.execute(
            "UPDATE appointment_pings SET used_at = ? WHERE token = ?",
            (datetime.now().isoformat(timespec="seconds"), token),
        )
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except Exception:
        payload = {}
    return {
        "token": row["token"],
        "payload": _appointment_ping_payload(payload, db=conn),
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "created_by": row["created_by"] or "",
        "used_at": row["used_at"] or "",
    }


def _normalize_staff_name(value, default=DEFAULT_STAFF, db=None):
    staff_options = get_staff_options(db)
    fallback = default if default in staff_options else get_default_staff(db)
    value = (value or fallback or get_default_staff(db)).strip()
    return value if value in staff_options else fallback


def other_staff_members(current_staff, db=None):
    staff_members = get_staff_members(db)
    normalized = _normalize_staff_name(current_staff, default=get_default_staff(db), db=db)
    if normalized == "Alle":
        return list(staff_members)
    return [name for name in staff_members if name != normalized]


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
    if SAFE_MODE or not ENABLE_SCHEDULER:
        return None
    if request.endpoint in {"static", "manifest", "service_worker"}:
        return None
    if request.method != "GET":
        return None
    try:
        init_db()
        run_automation_if_due(force=False)
    except Exception as exc:
        try:
            app.logger.warning("Automation runner ?bersprungen: %s", exc)
        except Exception:
            pass
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
            WHEN 'Sven' THEN 3
            ELSE 99
        END,
        COALESCE(staff_name, 'Ute')
        """,
        (today_start, tomorrow_start),
    ).fetchall()

    members = get_staff_members(db)
    default_staff = get_default_staff(db)
    data = {name: 0 for name in members}
    for row in rows:
        staff_name = (row["staff_name"] or default_staff).strip() if isinstance(row["staff_name"], str) else default_staff
        if staff_name in data:
            data[staff_name] = int(row["cnt"] or 0)

    return [{"staff_name": name, "count": data.get(name, 0)} for name in members]


def today_appointments(limit=20):
    start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    return get_db().execute(
        f"""
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), {mobile_sql}) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), {phone_sql}) AS Customer_PersoenlichesTelefon
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
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    return get_db().execute(
        f"""
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_email, ''), c._mail) AS _mail,
               COALESCE(NULLIF(a.manual_phone, ''), {mobile_sql}) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), {phone_sql}) AS Customer_PersoenlichesTelefon
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
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    return get_db().execute(
        f"""
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), {mobile_sql}) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), {phone_sql}) AS Customer_PersoenlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ?
        ORDER BY a.appointment_at ASC
        LIMIT ?
        """,
        (datetime.now().isoformat(timespec="minutes"), limit),
    ).fetchall()


def appointments_for_day(selected_date, staff_name="Alle", limit=120, db=None):
    conn = db or get_db()
    day_start = datetime.combine(selected_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    query = f"""
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), {mobile_sql}) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), {phone_sql}) AS Customer_PersoenlichesTelefon,
               COALESCE(NULLIF(a.manual_email, ''), c._mail) AS _mail
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ? AND a.appointment_at < ?
    """
    params = [day_start.isoformat(timespec="minutes"), day_end.isoformat(timespec="minutes")]
    if staff_name and staff_name != "Alle":
        query += " AND COALESCE(a.staff_name, ?) = ?"
        params.extend([get_default_staff(conn), staff_name])
    query += " ORDER BY a.appointment_at ASC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, tuple(params)).fetchall()
    items = []
    for row in rows:
        dt = _parse_dt_safe(row["appointment_at"])
        phone_value = customer_phone(row)
        items.append(
            {
                "id": row["id"],
                "customer_id": row["customer_id"],
                "customer_name": customer_full_name(row) or "Manueller Termin",
                "service_label": (row["service_summary"] or row["title"] or "Termin").strip(),
                "staff_name": row["staff_name"] or get_default_staff(conn),
                "status": row["status"] or "geplant",
                "time_label": dt.strftime("%H:%M") if dt else "--:--",
                "appointment_label": dt.strftime("%d.%m.%Y %H:%M") if dt else (row["appointment_at"] or ""),
                "notes_short": ((row["notes"] or "").strip()[:90]),
                "phone": phone_value,
                "call_url": phone_href(phone_value),
                "whatsapp_url": whatsapp_link(row),
                "edit_url": (
                    url_for("customer_detail", customer_id=row["customer_id"]) + f"#appointment-{row['id']}"
                    if is_admin_world_session()
                    else url_for("staff_appointment_edit", appointment_id=row["id"])
                ),
                "detail_url": url_for("customer_detail", customer_id=row["customer_id"]),
            }
        )
    return items


def appointment_duplicate_exists(customer_id, appointment_at, title, staff_name, *, manual_firstname="", manual_lastname="", manual_phone="", db=None):
    conn = db or get_db()
    row = conn.execute(
        """
        SELECT id FROM appointments
        WHERE customer_id = ?
          AND appointment_at = ?
          AND COALESCE(title, '') = ?
          AND COALESCE(staff_name, '') = ?
          AND COALESCE(manual_firstname, '') = ?
          AND COALESCE(manual_lastname, '') = ?
          AND COALESCE(manual_phone, '') = ?
        LIMIT 1
        """,
        (customer_id, appointment_at, title or "", staff_name or "", manual_firstname or "", manual_lastname or "", manual_phone or ""),
    ).fetchone()
    return bool(row)


def customer_search_results(q="", limit=120, db=None):
    conn = db or get_db()
    q = (q or "").strip()
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    query = f"""
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at,
               COALESCE({mobile_sql}, '') AS mobile_phone,
               COALESCE({phone_sql}, '') AS phone_phone
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
        WHERE {visible_customer_condition('c')}
    """
    params = []
    if q:
        like = f"%{q}%"
        query += f"""
            AND (
                COALESCE(c._name, '') LIKE ?
                OR COALESCE(c._firstname, '') LIKE ?
                OR COALESCE({mobile_sql}, '') LIKE ?
                OR COALESCE({phone_sql}, '') LIKE ?
                OR COALESCE(c._mail, '') LIKE ?
            )
        """
        params.extend([like, like, like, like, like])
    query += """
        GROUP BY c._id
        ORDER BY COALESCE(c._name, '') COLLATE NOCASE ASC, COALESCE(c._firstname, '') COLLATE NOCASE ASC
        LIMIT ?
    """
    params.append(limit)
    return conn.execute(query, tuple(params)).fetchall()


# ---------- PWA ----------
# ---------- PWA ----------
@app.route("/manifest.webmanifest")
@app.route("/manifest.json")
def manifest():
    manifest_path = Path(app.static_folder) / "manifest.webmanifest"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["start_url"] = "/"
        payload["scope"] = "/"
        payload["name"] = "Salon Karola App"
        payload["short_name"] = "Salon Karola"
        payload["description"] = "Termin- und Kunden-App für Salon Karola"
        payload["orientation"] = "portrait"
        payload["background_color"] = "#0f172a"
        payload["theme_color"] = "#0f172a"
        payload["icons"] = [
            {"src": "/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/static/icons/maskable-192.png", "sizes": "192x192", "type": "image/png", "purpose": "maskable"},
            {"src": "/static/icons/maskable-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ]
        response = jsonify(payload)
        response.mimetype = "application/manifest+json"
        return response
    except Exception:
        return send_from_directory(app.static_folder, "manifest.webmanifest", mimetype="application/manifest+json")


@app.route("/service-worker.js")
def service_worker():
    if SAFE_MODE or not ENABLE_SERVICE_WORKER:
        content = """self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});
"""
        response = Response(content, mimetype="application/javascript")
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response
    service_worker_path = Path(app.static_folder) / "service-worker.js"
    try:
        content = service_worker_path.read_text(encoding="utf-8").replace("__APP_VERSION__", APP_VERSION)
    except Exception:
        app.logger.exception("Service Worker konnte nicht geladen werden.")
        content = """const CACHE_NAME = 'salon-karola-fallback';
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});
"""
    response = Response(content, mimetype="application/javascript")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ---------- Routes ----------
@app.route("/safe-start")
def safe_start():
    diagnose_url = "/diagnose?safe=1"
    login_url = "/login"
    test_login_url = "/test-login"
    test_staff_url = "/test-staff-today"
    test_admin_url = "/test-admin-dashboard"
    test_sw_url = "/test-service-worker"
    test_push_url = "/test-push"
    html = f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Salon Karola App</title>
  <style>
    body{{font-family:Arial,sans-serif;background:#f5f1ea;color:#1b1b1b;padding:24px;line-height:1.5;}}
    .card{{max-width:560px;margin:4vh auto;background:#fff;border-radius:14px;padding:22px;box-shadow:0 10px 28px rgba(0,0,0,.08);}}
    h1{{margin:0 0 10px;font-size:1.5rem;}}
    p{{margin:0 0 16px;}}
    .row{{display:flex;gap:10px;flex-wrap:wrap;}}
    .btn{{display:inline-block;padding:10px 14px;border-radius:10px;text-decoration:none;border:1px solid #111;color:#111;background:#fff;font-weight:600;}}
  </style>
</head>
<body>
  <section class="card">
    <h1>Salon Karola App</h1>
    <p>App wurde im sicheren Modus gestartet.</p>
    <div class="row">
      <a class="btn" href="{login_url}">Zur Anmeldung</a>
      <a class="btn" href="{test_login_url}">Login testen</a>
      <a class="btn" href="{test_staff_url}">Mitarbeiter Heute testen</a>
      <a class="btn" href="{test_admin_url}">Admin Dashboard testen</a>
      <a class="btn" href="{test_sw_url}">Service Worker Test</a>
      <a class="btn" href="{test_push_url}">Push Test</a>
      <a class="btn" href="{diagnose_url}">Diagnose anzeigen</a>
      <a class="btn" href="#" id="clearCacheBtn">Cache loeschen</a>
    </div>
    <p id="safeInfo" style="margin-top:12px;font-size:.92rem;color:#444;"></p>
  </section>
  <script>
    async function disableServiceWorkerForDebug() {{
      var msg = [];
      try {{
        if ("serviceWorker" in navigator) {{
          var regs = await navigator.serviceWorker.getRegistrations();
          msg.push("Service Worker: " + regs.length + " gefunden");
          await Promise.all(regs.map(function (r) {{ return r.unregister().catch(function () {{ return false; }}); }}));
        }}
      }} catch (e) {{
        msg.push("SW-Fehler: " + String(e));
      }}
      try {{
        if ("caches" in window) {{
          var keys = await caches.keys();
          msg.push("Caches: " + keys.length + " gefunden");
          await Promise.all(keys.map(function (k) {{ return caches.delete(k).catch(function () {{ return false; }}); }}));
        }}
      }} catch (e) {{
        msg.push("Cache-Fehler: " + String(e));
      }}
      try {{ localStorage.setItem("sk_sw_disabled_debug", "1"); }} catch (e) {{}}
      var info = document.getElementById("safeInfo");
      if (info) info.textContent = msg.join(" | ") || "Safe-Mode aktiv.";
    }}
    document.getElementById("clearCacheBtn").addEventListener("click", function (event) {{
      event.preventDefault();
      disableServiceWorkerForDebug();
    }});
    disableServiceWorkerForDebug();
  </script>
  <script src="/static/js/safe-start.js?v={APP_VERSION}"></script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@app.route("/diagnose")
def diagnose():
    safe_access = SAFE_MODE or (request.args.get("safe") or "").strip() == "1"
    if not safe_access and not is_admin_session():
        return redirect(url_for("login"))

    diagnostics = {
        "app_version": APP_VERSION,
        "server_time": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(DB_PATH),
        "database_reachable": False,
        "customer_count": "n/a",
        "appointment_count": "n/a",
        "push_configured": False,
        "scheduler_enabled": ENABLE_SCHEDULER,
        "scheduler_running": False,
        "service_worker_enabled": ENABLE_SERVICE_WORKER and not SAFE_MODE,
        "safe_mode": SAFE_MODE,
        "env_flags": {
            "SAFE_MODE": SAFE_MODE,
            "ENABLE_PUSH": ENABLE_PUSH,
            "ENABLE_SCHEDULER": ENABLE_SCHEDULER,
            "ENABLE_SERVICE_WORKER": ENABLE_SERVICE_WORKER,
            "ENABLE_FIREBASE": ENABLE_FIREBASE,
        },
        "user_agent": request.headers.get("User-Agent", ""),
        "manifest_url": "/manifest.json",
        "manifest_reachable": False,
        "manifest_content_type": "",
        "manifest_name": "",
        "manifest_short_name": "",
        "manifest_start_url": "n/a",
        "manifest_icons_count": 0,
        "manifest_icons": [],
        "icon_checks": {},
        "utf8_test": "ä ö ü Ä Ö Ü ß",
        "pywebpush_available": bool(webpush),
        "vapid_public_available": False,
        "vapid_private_available": False,
        "push_subscription_count": 0,
        "push_last_error": "",
        "push_last_error_at": "",
        "mail_status": {},
        "mail_last_error": "",
        "mail_last_error_at": "",
    }
    try:
        db = get_db()
        diagnostics["database_reachable"] = True
        row_customers = db.execute("SELECT COUNT(*) AS cnt FROM _Customers").fetchone()
        row_appointments = db.execute("SELECT COUNT(*) AS cnt FROM appointments").fetchone()
        diagnostics["customer_count"] = int(row_customers["cnt"] or 0) if row_customers else 0
        diagnostics["appointment_count"] = int(row_appointments["cnt"] or 0) if row_appointments else 0
    except Exception as exc:
        diagnostics["database_reachable"] = False
        diagnostics["database_error"] = str(exc)
    try:
        diagnostics["scheduler_running"] = bool(getattr(scheduler, "running", False))
    except Exception:
        diagnostics["scheduler_running"] = False
    try:
        diagnostics["push_configured"] = bool(ENABLE_PUSH and push_delivery_ready())
    except Exception:
        diagnostics["push_configured"] = False
    try:
        diagnostics["mail_status"] = mail_status_summary()
        row_mail_error = db.execute(
            "SELECT error_message, sent_at FROM email_log WHERE COALESCE(status, '') = 'error' AND COALESCE(error_message, '') <> '' ORDER BY sent_at DESC LIMIT 1"
        ).fetchone()
        if row_mail_error:
            diagnostics["mail_last_error"] = row_mail_error["error_message"] or ""
            diagnostics["mail_last_error_at"] = row_mail_error["sent_at"] or ""
    except Exception as exc:
        diagnostics["mail_error"] = str(exc)
    try:
        row_push = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions").fetchone()
        diagnostics["push_subscription_count"] = int(row_push["cnt"] or 0) if row_push else 0
        row_push_error = db.execute(
            "SELECT last_error, updated_at FROM push_subscriptions WHERE COALESCE(last_error, '') <> '' ORDER BY COALESCE(updated_at, created_at) DESC LIMIT 1"
        ).fetchone()
        if row_push_error:
            diagnostics["push_last_error"] = row_push_error["last_error"] or ""
            diagnostics["push_last_error_at"] = row_push_error["updated_at"] or ""
    except Exception as exc:
        diagnostics["push_error"] = str(exc)
    try:
        manifest_response = manifest()
        diagnostics["manifest_content_type"] = manifest_response.headers.get("Content-Type", "")
        manifest_payload = json.loads(manifest_response.get_data(as_text=True))
        diagnostics["manifest_reachable"] = True
        diagnostics["manifest_name"] = manifest_payload.get("name", "")
        diagnostics["manifest_short_name"] = manifest_payload.get("short_name", "")
        diagnostics["manifest_start_url"] = manifest_payload.get("start_url", "n/a")
        diagnostics["manifest_icons"] = [icon.get("src", "") for icon in manifest_payload.get("icons", []) if isinstance(icon, dict)]
        diagnostics["manifest_icons_count"] = len(diagnostics["manifest_icons"])
    except Exception as exc:
        diagnostics["manifest_reachable"] = False
        diagnostics["manifest_error"] = str(exc)
    try:
        for icon_path in ["/static/icons/icon-192.png", "/static/icons/icon-512.png", "/static/icons/apple-touch-icon.png", "/static/icons/maskable-192.png", "/static/icons/maskable-512.png"]:
            full_path = BASE_DIR / icon_path.lstrip("/")
            exists = full_path.exists()
            diagnostics["icon_checks"][icon_path] = {
                "reachable": exists,
                "http_status": 200 if exists else 404,
                "content_type": "image/png" if exists else "",
                "bytes": (full_path.stat().st_size if exists else 0),
            }
    except Exception as exc:
        diagnostics["icon_error"] = str(exc)
    try:
        diagnostics["vapid_public_available"] = bool(vapid_public_key())
    except Exception:
        diagnostics["vapid_public_available"] = False
    try:
        diagnostics["vapid_private_available"] = bool(vapid_private_key())
    except Exception:
        diagnostics["vapid_private_available"] = False

    payload = json.dumps(diagnostics, ensure_ascii=False, indent=2)
    html = f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Diagnose - Salon Karola App</title>
  <style>
    body{{font-family:Arial,sans-serif;background:#111;color:#f6f6f6;padding:18px;}}
    .card{{max-width:860px;margin:0 auto;background:#1e1e1e;border-radius:12px;padding:16px;}}
    pre{{white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.5;}}
    .row{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;}}
    .btn{{display:inline-block;padding:8px 12px;border-radius:8px;text-decoration:none;border:1px solid #aaa;color:#fff;}}
  </style>
</head>
<body>
  <section class="card">
    <div class="row">
      <a class="btn" href="/safe-start">Sicherer Start</a>
      <a class="btn" href="/login">Zur Anmeldung</a>
      <a class="btn" href="/admin/icon-check">Icon-Check</a>
      <a class="btn" href="#" onclick="location.reload();return false;">Erneut laden</a>
      <a class="btn" href="#" id="diagResetCache">App-Cache zurücksetzen</a>
    </div>
    <pre id="diagServer">{payload}</pre>
    <pre id="diagClient"></pre>
  </section>
  <script>
    (function () {{
      var lines = [];
      try {{
        lines.push("localStorage verfügbar: ja");
        lines.push("JS-Fehler: " + (localStorage.getItem("sk_last_runtime_errors") || "[]"));
        lines.push("API-Fehler: " + (localStorage.getItem("sk_last_api_errors") || "[]"));
      }} catch (e) {{
        lines.push("localStorage verfügbar: nein");
        lines.push("localStorage Fehler: " + String(e));
      }}
      try {{
        lines.push("Notification verfügbar: " + (("Notification" in window) ? "ja" : "nein"));
        lines.push("PushManager verfügbar: " + (("PushManager" in window) ? "ja" : "nein"));
        lines.push("ServiceWorker verfügbar: " + (("serviceWorker" in navigator) ? "ja" : "nein"));
        lines.push("Notification.permission: " + (("Notification" in window) ? Notification.permission : "n/a"));
      }} catch (e) {{
        lines.push("Client Push-Check Fehler: " + String(e));
      }}
      try {{
        if ("serviceWorker" in navigator) {{
          navigator.serviceWorker.getRegistration().then(function(reg) {{
            lines.push("ServiceWorker registriert: " + (reg ? "ja" : "nein"));
            if (reg && reg.pushManager) {{
              reg.pushManager.getSubscription().then(function(sub) {{
                lines.push("Push-Subscription vorhanden: " + (sub ? "ja" : "nein"));
                document.getElementById("diagClient").textContent = lines.join("\\n");
              }}).catch(function(err) {{
                lines.push("Push-Subscription Fehler: " + String(err));
                document.getElementById("diagClient").textContent = lines.join("\\n");
              }});
            }} else {{
              document.getElementById("diagClient").textContent = lines.join("\\n");
            }}
          }}).catch(function(err) {{
            lines.push("ServiceWorker-Registrierung Fehler: " + String(err));
            document.getElementById("diagClient").textContent = lines.join("\\n");
          }});
        }}
      }} catch (e) {{
        lines.push("ServiceWorker Diagnosefehler: " + String(e));
      }}
      try {{
        var resetBtn = document.getElementById("diagResetCache");
        if (resetBtn) {{
          resetBtn.addEventListener("click", function (event) {{
            event.preventDefault();
            (async function () {{
              try {{
                if ("serviceWorker" in navigator) {{
                  var regs = await navigator.serviceWorker.getRegistrations();
                  await Promise.all(regs.map(function (reg) {{ return reg.unregister().catch(function () {{ return false; }}); }}));
                }}
                if ("caches" in window) {{
                  var keys = await caches.keys();
                  await Promise.all(keys.map(function (key) {{ return caches.delete(key).catch(function () {{ return false; }}); }}));
                }}
                alert("Service Worker und Cache wurden zurückgesetzt. Bitte Seite neu laden.");
              }} catch (err) {{
                alert("Cache-Reset fehlgeschlagen: " + String(err));
              }}
            }})();
          }});
        }}
      }} catch (e) {{}}
      try {{
        document.getElementById("diagClient").textContent = lines.join("\\n");
      }} catch (e) {{}}
    }})();
  </script>
</body>
</html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/icon-check")
@admin_required
def admin_icon_check():
    icon_paths = [
        "/static/icons/icon-192.png",
        "/static/icons/icon-512.png",
        "/static/icons/apple-touch-icon.png",
        "/static/icons/maskable-192.png",
        "/static/icons/maskable-512.png",
    ]
    cards = []
    for icon_path in icon_paths:
        full_path = BASE_DIR / icon_path.lstrip("/")
        exists = full_path.exists()
        cards.append(
            {
                "path": icon_path,
                "exists": exists,
                "status": 200 if exists else 404,
                "content_type": "image/png" if exists else "",
                "bytes": full_path.stat().st_size if exists else 0,
            }
        )

    cards_html = []
    for row in cards:
        cards_html.append(
            f"""
            <article class="card">
              <h3>{row["path"]}</h3>
              <img src="{row["path"]}?v={APP_VERSION}" alt="{row["path"]}" style="width:96px;height:96px;border-radius:16px;border:1px solid #ddd;background:#0f172a;" />
              <p>Status: {"OK" if row["exists"] else "Fehler"} | HTTP: {row["status"]} | Typ: {row["content_type"] or "-"} | Größe: {row["bytes"]} Bytes</p>
            </article>
            """
        )

    page = f"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Icon Check - Salon Karola</title>
  <style>
    body{{font-family:Arial,sans-serif;background:#f7f3ee;color:#1f1f1f;padding:16px;}}
    .wrap{{max-width:980px;margin:0 auto;}}
    .row{{display:flex;gap:12px;flex-wrap:wrap;}}
    .card{{background:#fff;border-radius:12px;padding:12px;min-width:250px;flex:1 1 250px;border:1px solid #e8dece;}}
    .btn{{display:inline-block;padding:10px 12px;border:1px solid #222;border-radius:8px;text-decoration:none;color:#111;background:#fff;font-weight:600;}}
  </style>
</head>
<body>
  <section class="wrap">
    <p><a class="btn" href="/admin">Zurück zum Admin</a> <a class="btn" href="/diagnose">Zur Diagnose</a></p>
    <h1>Icon Check</h1>
    <p>Manifest: <a href="/manifest.json?v={APP_VERSION}" target="_blank" rel="noopener">/manifest.json</a></p>
    <div class="row">{''.join(cards_html)}</div>
  </section>
</body>
</html>"""
    return Response(page, mimetype="text/html")


@app.route("/test-login")
def test_login():
    login_options = default_login_options()
    options_html = "\n".join(
        [f'<option value="{option["staff_name"]}">{option["label"]}</option>' for option in login_options]
    )
    html = f"""<!doctype html>
<html lang="de"><head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Test Login - Salon Karola App</title>
  <style>body{{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}}.card{{max-width:620px;margin:0 auto;background:#fff;padding:18px;border-radius:12px}}label{{display:block;margin-top:10px}}input,select{{width:100%;padding:10px}}button,a{{display:inline-block;margin-top:12px;padding:10px 14px;border:1px solid #222;border-radius:8px;background:#fff;color:#111;text-decoration:none;font-weight:600}}</style>
</head><body>
  <section class="card">
    <h1>Test Login</h1>
    <p>Status: OK | Route: /test-login | Push: inaktiv | Service Worker: inaktiv | Firebase: inaktiv | Scheduler: {"aktiv" if ENABLE_SCHEDULER and not SAFE_MODE else "inaktiv"}</p>
    <form method="post" action="/login">
      <input type="hidden" name="action" value="login">
      <label for="staff_name">Name</label>
      <select id="staff_name" name="staff_name" required>{options_html}</select>
      <label for="password">Passwort</label>
      <input id="password" type="password" name="password" autocomplete="current-password" required>
      <button type="submit">Einloggen</button>
    </form>
    <a href="/safe-start">Zurück zu Safe-Start</a>
  </section>
  <script src="/static/js/login.js?v={APP_VERSION}"></script>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/test-staff-today")
@login_required
def test_staff_today():
    try:
        db = get_db()
        staff_name = _normalize_staff_name(session.get("staff_name"), default=default_staff_for_simple_mode(db), db=db)
        selected_date = datetime.now().date()
        items = appointments_for_day(selected_date, staff_name, limit=40, db=db)
        rows = []
        for item in items[:40]:
            call_url = item.get("call_url") or ""
            wa_url = item.get("whatsapp_url") or ""
            row = f"<li><strong>{item.get('time_label','')}</strong> - {item.get('customer_name','')} ({item.get('staff_name','')})"
            if call_url:
                row += f' | <a href="{call_url}">Telefon</a>'
            if wa_url:
                row += f' | <a href="{wa_url}" target="_blank" rel="noopener">WhatsApp</a>'
            row += "</li>"
            rows.append(row)
        list_html = "<ul>" + "\n".join(rows) + "</ul>" if rows else "<p>Heute keine Termine.</p>"
        status = "OK"
        error_text = ""
    except Exception as exc:
        list_html = ""
        status = "Fehler"
        error_text = str(exc)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Test Mitarbeiter Heute</title>
<style>body{{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}}.card{{max-width:760px;margin:0 auto;background:#fff;padding:18px;border-radius:12px}}a{{color:#111}} .err{{color:#9a1f1f;font-weight:700}}</style></head><body>
<section class="card"><h1>Test Mitarbeiter Heute</h1>
<p>Status: {status} | Route: /test-staff-today | Push: inaktiv | Service Worker: inaktiv | Firebase: inaktiv | Scheduler: {"aktiv" if ENABLE_SCHEDULER and not SAFE_MODE else "inaktiv"}</p>
{f'<p class="err">Fehler: {error_text}</p>' if error_text else list_html}
<a href="/safe-start">Zurück zu Safe-Start</a></section></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/test-admin-dashboard")
@admin_required
def test_admin_dashboard():
    status = "OK"
    error_text = ""
    customers = today_count = next_7_days = 0
    try:
        db = get_db()
        row_customers = db.execute("SELECT COUNT(*) AS cnt FROM _Customers WHERE COALESCE(_name, '') <> ?", (MANUAL_PLACEHOLDER_LASTNAME,)).fetchone()
        customers = int(row_customers["cnt"] or 0) if row_customers else 0
        day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        week_end = day_start + timedelta(days=7)
        row_today = db.execute("SELECT COUNT(*) AS cnt FROM appointments WHERE appointment_at >= ? AND appointment_at < ?", (day_start.isoformat(timespec="minutes"), day_end.isoformat(timespec="minutes"))).fetchone()
        row_week = db.execute("SELECT COUNT(*) AS cnt FROM appointments WHERE appointment_at >= ? AND appointment_at < ?", (day_start.isoformat(timespec="minutes"), week_end.isoformat(timespec="minutes"))).fetchone()
        today_count = int(row_today["cnt"] or 0) if row_today else 0
        next_7_days = int(row_week["cnt"] or 0) if row_week else 0
    except Exception as exc:
        status = "Fehler"
        error_text = str(exc)
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Test Admin Dashboard</title>
<style>body{{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}}.card{{max-width:760px;margin:0 auto;background:#fff;padding:18px;border-radius:12px}}.grid{{display:grid;grid-template-columns:repeat(3,minmax(120px,1fr));gap:10px}}.box{{border:1px solid #ddd;border-radius:8px;padding:12px}}a{{display:inline-block;margin-top:12px;color:#111}}</style></head><body>
<section class="card"><h1>Test Admin Dashboard</h1>
<p>Status: {status} | Route: /test-admin-dashboard | Push: inaktiv | Service Worker: inaktiv | Firebase: inaktiv | Scheduler: {"aktiv" if ENABLE_SCHEDULER and not SAFE_MODE else "inaktiv"}</p>
{f'<p style="color:#9a1f1f;font-weight:700">Fehler: {error_text}</p>' if error_text else f'<div class="grid"><div class="box"><strong>Kunden</strong><div>{customers}</div></div><div class="box"><strong>Termine heute</strong><div>{today_count}</div></div><div class="box"><strong>Naechste 7 Tage</strong><div>{next_7_days}</div></div></div>'}
<a href="/safe-start">Zurück zu Safe-Start</a></section></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/test-service-worker")
@login_required
def test_service_worker():
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Test Service Worker</title>
<style>body{{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}}.card{{max-width:760px;margin:0 auto;background:#fff;padding:18px;border-radius:12px}}button,a{{display:inline-block;margin:8px 8px 0 0;padding:10px 14px;border:1px solid #222;border-radius:8px;background:#fff;color:#111;text-decoration:none;font-weight:600}}pre{{background:#111;color:#f5f5f5;padding:12px;border-radius:8px;white-space:pre-wrap}}</style></head><body>
<section class="card"><h1>Service Worker Test</h1>
<p>Status: bereit | Route: /test-service-worker | Push: inaktiv | Service Worker: testbar | Firebase: inaktiv | Scheduler: {"aktiv" if ENABLE_SCHEDULER and not SAFE_MODE else "inaktiv"}</p>
<button id="registerBtn">Service Worker registrieren</button>
<button id="unregisterBtn">Service Worker deregistrieren</button>
<button id="clearCacheBtn">Cache loeschen</button>
<a href="/safe-start">Zurück zu Safe-Start</a>
<pre id="out">Warte auf Test...</pre></section>
<script src="/static/js/service-worker-register.js?v={APP_VERSION}"></script>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/test-push")
@admin_required
def test_push():
    html = f"""<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Test Push</title>
<style>body{{font-family:Arial,sans-serif;background:#f4efe8;color:#1f1f1f;padding:20px}}.card{{max-width:760px;margin:0 auto;background:#fff;padding:18px;border-radius:12px}}button,a{{display:inline-block;margin:8px 8px 0 0;padding:10px 14px;border:1px solid #222;border-radius:8px;background:#fff;color:#111;text-decoration:none;font-weight:600}}pre{{background:#111;color:#f5f5f5;padding:12px;border-radius:8px;white-space:pre-wrap}}</style></head><body>
<section class="card"><h1>Push Test</h1>
<p>Status: bereit | Route: /test-push | Push: {"aktivierbar" if ENABLE_PUSH and not SAFE_MODE else "inaktiv"} | Service Worker: {"aktivierbar" if ENABLE_SERVICE_WORKER and not SAFE_MODE else "inaktiv"} | Firebase: {"aktivierbar" if ENABLE_FIREBASE and not SAFE_MODE else "inaktiv"} | Scheduler: {"aktiv" if ENABLE_SCHEDULER and not SAFE_MODE else "inaktiv"}</p>
<button id="initPushBtn">Push initialisieren</button>
<button id="sendTestPushBtn">Test Push senden</button>
<a href="/safe-start">Zurück zu Safe-Start</a>
<pre id="out">Warte auf Test...</pre></section>
<script>window.__pushFlags = {{ enabled: {str(ENABLE_PUSH and not SAFE_MODE).lower()}, swEnabled: {str(ENABLE_SERVICE_WORKER and not SAFE_MODE).lower()}, firebaseEnabled: {str(ENABLE_FIREBASE and not SAFE_MODE).lower()} }};</script>
<script src="/static/js/safe-fetch.js?v={APP_VERSION}"></script>
<script src="/static/js/push.js?v={APP_VERSION}"></script>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/login", methods=["GET", "POST"])
def login():
    login_options = default_login_options()
    db = get_db()
    if request.method == "POST":
        action = (request.form.get("action") or "login").strip().lower()
        selected_staff = _normalize_staff_name(request.form.get("staff_name"), default=get_default_staff(db), db=db)
        user = fetch_user_for_staff(db, selected_staff)
        remember_device = (request.form.get("remember_device") or "").strip().lower() in {"1", "true", "on", "yes"}

        if action == "setup":
            password = request.form.get("new_password", "")
            password_confirm = request.form.get("new_password_confirm", "")
            if not user:
                flash("Benutzer konnte nicht vorbereitet werden.")
            elif len(password) < 4:
                flash("Bitte ein Passwort mit mindestens 4 Zeichen w?hlen.")
            elif password != password_confirm:
                flash("Die beiden Passw?rter stimmen nicht ?berein.")
            else:
                db.execute(
                    "UPDATE staff_users SET password = ? WHERE id = ?",
                    (hash_password(password), user["id"]),
                )
                db.commit()
                login_user(user, staff_name=selected_staff, remember_device=remember_device)
                flash(f"Passwort fuer {selected_staff} gespeichert. Willkommen, {selected_staff}.")
                return redirect(request.args.get("next") or default_route_after_login(selected_staff))
        else:
            password = request.form.get("password", "")
            if not user:
                flash("Benutzer nicht gefunden.")
            elif not user_has_password(user):
                flash(f"Fuer {selected_staff} wurde noch kein Passwort angelegt. Bitte zuerst registrieren.")
            elif verify_password(user["password"], password):
                if not password_is_hashed(user["password"]):
                    db.execute(
                        "UPDATE staff_users SET password = ? WHERE id = ?",
                        (hash_password(password), user["id"]),
                    )
                    db.commit()
                staff_name = resolve_staff_name_for_user(user, db=db)
                login_user(user, staff_name=staff_name, remember_device=remember_device)
                flash(f"Login erfolgreich: {staff_name}.")
                return redirect(request.args.get("next") or default_route_after_login(staff_name))
            else:
                flash("Login fehlgeschlagen.")

    setup_states = {}
    for option in login_options:
        user = fetch_user_for_staff(db, option["staff_name"])
        setup_states[option["staff_name"]] = {"has_password": user_has_password(user)}
    return render_template(
        "login.html",
        login_options=login_options,
        setup_states=setup_states,
        passkeys_ready=passkeys_ready(),
        current_endpoint="login",
    )


@app.route("/api/passkeys/register/options", methods=["POST"])
@login_required
def passkey_register_options():
    if not passkeys_ready():
        return {"ok": False, "error": "Passkeys sind serverseitig noch nicht aktiv."}, 503
    db = get_db()
    user = db.execute("SELECT * FROM staff_users WHERE username = ? LIMIT 1", (session.get("username"),)).fetchone()
    if not user:
        return {"ok": False, "error": "Benutzer nicht gefunden."}, 404
    existing = passkey_credentials_for_user(db, user["id"])
    options = generate_registration_options(
        rp_id=current_passkey_rp_id(),
        rp_name=current_passkey_rp_name(),
        user_id=str(user["id"]).encode("utf-8"),
        user_name=user["username"],
        user_display_name=user["display_name"] or user["username"],
        exclude_credentials=[PublicKeyCredentialDescriptor(id=_b64url_decode(row["credential_id"])) for row in existing],
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.PREFERRED,
            user_verification=UserVerificationRequirement.PREFERRED,
        ),
    )
    session["passkey_register_challenge"] = _b64url_encode(options.challenge)
    return Response(options_to_json(options), mimetype="application/json")


@app.route("/api/passkeys/register/verify", methods=["POST"])
@login_required
def passkey_register_verify():
    if not passkeys_ready():
        return {"ok": False, "error": "Passkeys sind serverseitig noch nicht aktiv."}, 503
    challenge = session.get("passkey_register_challenge")
    if not challenge:
        return {"ok": False, "error": "Passkey-Registrierung muss neu gestartet werden."}, 400
    payload = request.get_json(silent=True) or {}
    db = get_db()
    user = db.execute("SELECT * FROM staff_users WHERE username = ? LIMIT 1", (session.get("username"),)).fetchone()
    if not user:
        return {"ok": False, "error": "Benutzer nicht gefunden."}, 404
    try:
        if hasattr(RegistrationCredential, "parse_obj"):
            credential = RegistrationCredential.parse_obj(payload)
        else:
            credential = RegistrationCredential.parse_raw(json.dumps(payload))
        verification = verify_registration_response(
            credential=credential,
            expected_challenge=_b64url_decode(challenge),
            expected_rp_id=current_passkey_rp_id(),
            expected_origin=current_passkey_origin(),
            require_user_verification=True,
        )
    except Exception as exc:
        return {"ok": False, "error": f"Passkey konnte nicht gespeichert werden: {exc}"}, 400
    transports = payload.get("response", {}).get("transports") or []
    db.execute(
        """
        INSERT INTO webauthn_credentials(user_id, credential_id, public_key, sign_count, transports, created_at, last_used_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(credential_id) DO UPDATE SET
            user_id = excluded.user_id,
            public_key = excluded.public_key,
            sign_count = excluded.sign_count,
            transports = excluded.transports,
            last_used_at = excluded.last_used_at
        """,
        (
            user["id"],
            _b64url_encode(verification.credential_id),
            _b64url_encode(verification.credential_public_key),
            int(verification.sign_count or 0),
            json.dumps(transports),
            datetime.now().isoformat(timespec="seconds"),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    db.commit()
    session.pop("passkey_register_challenge", None)
    return {"ok": True, "message": "Fingerabdruck/Passkey wurde f?r dieses Ger?t gespeichert."}


@app.route("/api/passkeys/auth/options", methods=["POST"])
def passkey_auth_options():
    if not passkeys_ready():
        return {"ok": False, "error": "Passkeys sind serverseitig noch nicht aktiv."}, 503
    payload = request.get_json(silent=True) or {}
    selected_staff = _normalize_staff_name(payload.get("staff_name"), default=DEFAULT_STAFF)
    db = get_db()
    user = fetch_user_for_staff(db, selected_staff)
    if not user:
        return {"ok": False, "error": "Benutzer nicht gefunden."}, 404
    credentials = passkey_credentials_for_user(db, user["id"])
    if not credentials:
        return {"ok": False, "error": f"F?r {selected_staff} ist auf diesem Konto noch kein Fingerabdruck/Passkey eingerichtet."}, 400
    options = generate_authentication_options(
        rp_id=current_passkey_rp_id(),
        allow_credentials=[PublicKeyCredentialDescriptor(id=_b64url_decode(row["credential_id"])) for row in credentials],
        user_verification=UserVerificationRequirement.PREFERRED,
    )
    session["passkey_auth_challenge"] = _b64url_encode(options.challenge)
    session["passkey_auth_staff"] = selected_staff
    session["passkey_auth_remember"] = bool(payload.get("remember_device"))
    return Response(options_to_json(options), mimetype="application/json")


@app.route("/api/passkeys/auth/verify", methods=["POST"])
def passkey_auth_verify():
    if not passkeys_ready():
        return {"ok": False, "error": "Passkeys sind serverseitig noch nicht aktiv."}, 503
    payload = request.get_json(silent=True) or {}
    challenge = session.get("passkey_auth_challenge")
    selected_staff = _normalize_staff_name(session.get("passkey_auth_staff"), default=DEFAULT_STAFF)
    if not challenge:
        return {"ok": False, "error": "Passkey-Anmeldung muss neu gestartet werden."}, 400
    credential_id = (((payload.get("id") or "").strip()) or ((payload.get("rawId") or "").strip()))
    if not credential_id:
        return {"ok": False, "error": "Passkey-ID fehlt."}, 400
    db = get_db()
    row = find_passkey_by_credential_id(db, credential_id)
    if not row:
        return {"ok": False, "error": "Passkey nicht gefunden."}, 404
    if resolve_staff_name_for_user(row) != selected_staff:
        return {"ok": False, "error": "Passkey geh?rt nicht zum gew?hlten Namen."}, 400
    try:
        if hasattr(AuthenticationCredential, "parse_obj"):
            credential = AuthenticationCredential.parse_obj(payload)
        else:
            credential = AuthenticationCredential.parse_raw(json.dumps(payload))
        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=_b64url_decode(challenge),
            expected_rp_id=current_passkey_rp_id(),
            expected_origin=current_passkey_origin(),
            credential_public_key=_b64url_decode(row["public_key"]),
            credential_current_sign_count=int(row["sign_count"] or 0),
            require_user_verification=True,
        )
    except Exception as exc:
        return {"ok": False, "error": f"Passkey-Anmeldung fehlgeschlagen: {exc}"}, 400
    db.execute(
        "UPDATE webauthn_credentials SET sign_count = ?, last_used_at = ? WHERE id = ?",
        (int(verification.new_sign_count or 0), datetime.now().isoformat(timespec="seconds"), row["id"]),
    )
    db.commit()
    login_user(row, staff_name=selected_staff, remember_device=bool(session.get("passkey_auth_remember")))
    session.pop("passkey_auth_challenge", None)
    session.pop("passkey_auth_staff", None)
    session.pop("passkey_auth_remember", None)
    return {"ok": True, "redirect_url": url_for("calendar_view")}


@app.route("/api/passkeys/status")
def passkey_status():
    enabled = passkeys_ready()
    configured = bool((request.is_secure or current_passkey_origin().startswith("https://")) and current_passkey_rp_id())
    return {"ok": True, "enabled": enabled and configured, "backend_ready": enabled, "logged_in": bool(session.get("admin_logged_in")), "staff_name": session.get("staff_name") or DEFAULT_STAFF}


@app.route("/logout")
def logout():
    session.clear()
    flash("Du wurdest abgemeldet.")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    if SAFE_MODE:
        return redirect(url_for("safe_start"))
    if is_admin_session():
        return redirect(url_for("admin_home"))
    return redirect(url_for("staff_today"))


@app.route("/dashboard")
@login_required
def dashboard():
    return redirect(url_for("admin_automation"))


@app.route("/admin/start")
@admin_required
def admin_start():
    return render_template(
        "admin_start.html",
        current_endpoint="admin_start",
        app_version=APP_VERSION,
    )


@app.route("/app/staff")
@login_required
def switch_to_staff_app():
    set_ui_world("staff")
    return redirect(url_for("staff_today"))


@app.route("/app/admin")
@admin_required
def switch_to_admin_app():
    set_ui_world("admin")
    return redirect(url_for("admin_dashboard"))


@app.route("/staff/today")
@login_required
def staff_today():
    set_ui_world("staff")
    return redirect(url_for("calendar_view", view="day"))


@app.route("/staff/appointment/new", methods=["GET", "POST"])
@staff_or_admin_required
def staff_new_appointment():
    set_ui_world("staff")
    return appointments_hub()


@app.route("/staff/appointments")
@staff_or_admin_required
def staff_appointments_center():
    set_ui_world("staff")
    db = get_db()
    selected_date = parse_iso_date(request.args.get("date"))
    own_staff = default_staff_for_simple_mode(db)
    staff_options = [name for name in get_staff_options(db) if name != "Sven"]
    mine_param = (request.args.get("mine") or "").strip().lower()
    mine_mode = mine_param in {"1", "true", "yes", "on"}

    staff = (request.args.get("staff") or own_staff).strip()
    if staff not in staff_options and staff != "Alle":
        staff = own_staff
    if not is_admin_session():
        if mine_mode:
            staff = own_staff
        elif staff == "Sven":
            staff = own_staff

    staff_label = own_staff if mine_mode else staff
    if not staff_label:
        staff_label = own_staff
    if staff_label == "Alle" and not is_admin_session():
        staff_label = own_staff

    today_items = appointments_for_day(selected_date, staff_label, limit=50, db=db)
    tomorrow_items = appointments_for_day(selected_date + timedelta(days=1), staff_label, limit=50, db=db)
    upcoming_items = upcoming_appointments(limit=10, staff_name=staff_label if staff_label != "Alle" else "Alle", db=db)

    return render_template(
        "staff_appointments_center.html",
        current_endpoint="staff_appointments_center",
        app_version=APP_VERSION,
        selected_date=selected_date.isoformat(),
        selected_date_label=selected_date.strftime("%d.%m.%Y"),
        today_date=datetime.now().date().isoformat(),
        tomorrow_date=(datetime.now().date() + timedelta(days=1)).isoformat(),
        staff=staff,
        mine_mode=mine_mode,
        own_staff=own_staff,
        bookable_staff=staff_members_for_simple_mode(db) or get_staff_members(db),
        today_items=today_items,
        tomorrow_items=tomorrow_items,
        upcoming_items=upcoming_items,
    )


@app.route("/staff/customers")
@staff_or_admin_required
def staff_customers():
    set_ui_world("staff")
    return customer_search_page()


@app.route("/staff/customers/<int:customer_id>", methods=["GET", "POST"])
@staff_or_admin_required
def staff_customer_detail(customer_id):
    set_ui_world("staff")
    return customer_detail(customer_id)


@app.route("/staff/more")
@staff_or_admin_required
def staff_more():
    set_ui_world("staff")
    return salon_reminders()


@app.route("/salon")
@login_required
def salon_home():
    if is_admin_world_session():
        return redirect(url_for("admin_dashboard"))
    set_ui_world("staff")
    db = get_db()
    today = datetime.now().date()
    own_staff = default_staff_for_simple_mode(db)
    own_appointments = appointments_for_day(today, own_staff, db=db)
    all_appointments = appointments_for_day(today, "Alle", db=db)
    reminder_items = due_reminders(limit=8)
    birthday_items = upcoming_birthdays(limit=8)
    return render_template(
        "staff_today.html",
        today_date=today.isoformat(),
        own_staff=own_staff,
        own_appointments=own_appointments,
        all_appointments=all_appointments,
        reminder_items=reminder_items,
        birthday_items=birthday_items,
        current_endpoint="salon_home",
        app_version=APP_VERSION,
    )


@app.route("/salon/reminders")
@login_required
def salon_reminders():
    if is_admin_world_session():
        return redirect(url_for("customer_birthdays_page"))
    return render_template(
        "staff_more.html",
        reminder_items=due_reminders(limit=40),
        birthday_items=upcoming_birthdays(limit=20),
        current_endpoint="salon_reminders",
        app_version=APP_VERSION,
    )


@app.route("/dashboard-legacy")
@admin_required
def dashboard_legacy():
    set_ui_world("admin")
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
        "admin_dashboard.html",
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
        current_endpoint="admin_dashboard",
        app_version=APP_VERSION,
        deploy_marker=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        db_path=str(DB_PATH),
    )


@app.route("/customers/search")
@login_required
def customer_search_page():
    db = get_db()
    is_admin_world = is_admin_world_session()
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

    limit = 300 if is_admin_world else 20
    if not is_admin_world and not q:
        customers = []
    else:
        base_query += f" GROUP BY c._id ORDER BY {order_sql} LIMIT {limit}"
        customers = db.execute(base_query, params).fetchall()
    tags = db.execute("SELECT tag, COUNT(*) AS cnt FROM customer_tags GROUP BY tag ORDER BY tag").fetchall()
    template_name = "admin_customers.html" if is_admin_world else "staff_customer_search.html"
    return render_template(
        template_name,
        customers=customers,
        q=q,
        tag=tag,
        sort=sort,
        tags=tags,
        current_endpoint="customer_search_page",
        app_version=APP_VERSION,
    )


@app.route("/admin")
@admin_required
def admin_home():
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/dashboard")
@admin_required
def admin_dashboard():
    set_ui_world("admin")
    db = get_db()
    today = datetime.now().date()
    next_week = today + timedelta(days=7)
    stats = dashboard_stats()
    counts = db.execute(
        """
        SELECT
            SUM(CASE WHEN DATE(appointment_at) = DATE(?) THEN 1 ELSE 0 END) AS today_count,
            SUM(CASE WHEN DATE(appointment_at) BETWEEN DATE(?) AND DATE(?) THEN 1 ELSE 0 END) AS week_count
        FROM appointments
        """,
        (today.isoformat(), today.isoformat(), next_week.isoformat()),
    ).fetchone()
    birthdays_today = db.execute(
        "SELECT COUNT(*) AS cnt FROM _Customers WHERE strftime('%m-%d', _birthdate) = strftime('%m-%d', ?)",
        (today.isoformat(),),
    ).fetchone()
    upcoming = next_appointments(limit=14)
    due_items = due_reminders(limit=14)
    return render_template(
        "admin_dashboard.html",
        stats=stats,
        upcoming=upcoming,
        due_items=due_items,
        staff_counts=staff_dashboard_counts(),
        today_items=today_appointments(limit=20),
        now=datetime.now(),
        today_count=int((counts["today_count"] if counts and counts["today_count"] is not None else 0)),
        week_count=int((counts["week_count"] if counts and counts["week_count"] is not None else 0)),
        birthdays_today=int((birthdays_today["cnt"] if birthdays_today and birthdays_today["cnt"] is not None else 0)),
        current_endpoint="admin_dashboard",
        app_version=APP_VERSION,
    )


@app.route("/admin/calendar")
@admin_required
def admin_calendar_alias():
    set_ui_world("admin")
    return redirect(url_for("calendar_view", view=request.args.get("view") or "week", date=request.args.get("date"), staff=request.args.get("staff")))


@app.route("/admin/customers")
@admin_required
def admin_customers_alias():
    set_ui_world("admin")
    return customer_search_page()


@app.route("/admin/appointments")
@admin_required
def admin_appointments_alias():
    set_ui_world("admin")
    return appointments_hub()


@app.route("/admin/push")
@admin_required
def admin_push_alias():
    set_ui_world("admin")
    return push_center()


@app.route("/admin/templates")
@admin_required
def admin_templates_alias():
    set_ui_world("admin")
    return templates_view()


@app.route("/admin/backup")
@admin_required
def admin_backup_alias():
    set_ui_world("admin")
    return database_tools()


@app.route("/admin/staff")
@admin_required
def admin_staff_alias():
    set_ui_world("admin")
    return staff_management()


@app.route("/admin/customers/<int:customer_id>", methods=["GET", "POST"])
@admin_required
def admin_customer_detail_alias(customer_id):
    set_ui_world("admin")
    return customer_detail(customer_id)


@app.route("/admin/automation")
@admin_required
def admin_automation():
    set_ui_world("admin")
    db = get_db()
    mail_status = mail_status_summary()
    email_sent_today_row = db.execute(
        "SELECT COUNT(*) AS cnt FROM email_log WHERE date(sent_at) = date('now', 'localtime') AND status = 'sent'"
    ).fetchone()
    email_sent_today = int(email_sent_today_row["cnt"] or 0) if email_sent_today_row else 0
    latest_mail = db.execute(
        "SELECT recipient, email_type, status, sent_at, error_message FROM email_log ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    last_mail_error = db.execute(
        "SELECT error_message, sent_at FROM email_log WHERE COALESCE(status, '') = 'error' AND COALESCE(error_message, '') <> '' ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    recent_logs = db.execute(
        "SELECT email_type, recipient, status, sent_at, error_message FROM email_log ORDER BY sent_at DESC LIMIT 20"
    ).fetchall()
    return render_template(
        "admin_automation.html",
        automation=get_automation_status(),
        mail_status=mail_status,
        email_sent_today=email_sent_today,
        latest_mail=latest_mail,
        last_mail_error=last_mail_error,
        recent_logs=recent_logs,
        scheduler_enabled=ENABLE_SCHEDULER and not SAFE_MODE,
        safe_mode=SAFE_MODE,
        current_endpoint="admin_automation",
        app_version=APP_VERSION,
    )


@app.route("/admin/automation/run-birthday", methods=["POST"])
@admin_required
def admin_run_birthday():
    result = run_birthday_job()
    flash(result.get("message") or f"Geburtstagsjob: geprüft={result.get('checked', 0)}, gesendet={result.get('sent', 0)}, Fehler={result.get('errors', 0)}")
    return redirect(url_for("admin_automation"))


@app.route("/admin/automation/run-appointments", methods=["POST"])
@admin_required
def admin_run_appointments():
    result = run_appointment_job()
    flash(result.get("message") or f"Terminerinnerung: geprüft={result.get('checked', 0)}, gesendet={result.get('sent', 0)}, Fehler={result.get('errors', 0)}")
    return redirect(url_for("admin_automation"))


@app.route("/admin/automation/run-all", methods=["POST"])
@admin_required
def admin_run_all_automation():
    try:
        result = scheduler_tick()
        flash(result.get("summary") or "Automationen wurden ausgeführt.")
    except Exception as exc:
        flash(f"Automationen fehlgeschlagen: {exc}")
    return redirect(url_for("admin_automation"))


@app.route("/admin/automation/test-email", methods=["POST"])
@admin_required
def admin_test_email():
    to_email = (request.form.get("to_email") or "").strip()
    if not to_email:
        flash("Bitte eine Test-E-Mail-Adresse eingeben.")
        return redirect(url_for("admin_automation"))
    if not mail_ready():
        flash("E-Mail-Versand ist nicht konfiguriert.")
        return redirect(url_for("admin_automation"))
    subject = "Salon Karola Testmail"
    body = "Diese Testmail bestätigt, dass der E-Mail-Versand funktioniert."
    try:
        send_email(to_email, subject, body)
        log_email(None, "test_email", subject, body, to_email, "sent")
        flash(f"Testmail wurde an {to_email} gesendet.")
    except Exception as exc:
        log_email(None, "test_email", subject, body, to_email, "error", str(exc))
        flash(f"Testmail fehlgeschlagen: {exc}")
    return redirect(url_for("admin_automation"))


@app.route("/admin/settings")
@admin_required
def admin_settings():
    set_ui_world("admin")
    return render_template(
        "admin_settings.html",
        current_endpoint="admin_settings",
        app_version=APP_VERSION,
    )


@app.route("/api/customers/quick-search")
@login_required
def customer_quick_search():
    q = request.args.get("q", "").strip()
    in_admin_world = is_admin_world_session()
    items = []
    for row in customer_search_results(q, limit=12):
        mobile = (row["mobile_phone"] or "").strip() if "mobile_phone" in row.keys() else ""
        phone = (row["phone_phone"] or "").strip() if "phone_phone" in row.keys() else ""
        primary_phone = mobile or phone
        items.append(
            {
                "id": row["_id"],
                "name": customer_full_name(row),
                "phone": primary_phone,
                "email": row["_mail"] or "",
                "detail_url": (url_for("customer_detail", customer_id=row["_id"]) if in_admin_world else url_for("staff_customer_detail", customer_id=row["_id"])),
                "new_appointment_url": (url_for("appointments_hub", customer_id=row["_id"]) if in_admin_world else url_for("staff_new_appointment", customer_id=row["_id"])),
                "call_url": phone_href(primary_phone) if primary_phone else "",
                "whatsapp_url": whatsapp_link(row),
            }
        )
    return {"ok": True, "items": items}


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
    if not is_admin_session():
        flash("Neue Kunden legt Sven/Admin im Verwaltungsbereich an.")
        return redirect(url_for("customer_search_page"))
    if request.method == "POST":
        db = get_db()
        phone_column = customer_personal_phone_column()
        cur = db.execute(
            f"""
            INSERT INTO _Customers(_name, _firstname, _mail, _birthdate, _notes, Customer_Adresse, "{phone_column}", Customer_Mobiltelefon, Customer_Postleitzahl, Customer_Stadt)
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
        flash("Kontakt wurde hinzugefuegt.")
        return redirect(url_for("customer_detail", customer_id=cur.lastrowid))
    return render_template("customer_form.html", customer=None, appointments=[], logs=[], tags_text="", wa_link="", current_endpoint="customer_new", customer_status="neu", app_version=APP_VERSION)


@app.route("/customer/<int:customer_id>", methods=["GET", "POST"])
@login_required
def customer_detail(customer_id):
    db = get_db()
    if request.method == "POST":
        phone_column = customer_personal_phone_column()
        db.execute(
            f"""
            UPDATE _Customers
            SET _name=?, _firstname=?, _mail=?, _birthdate=?, _notes=?,
                Customer_Adresse=?, "{phone_column}"=?, Customer_Mobiltelefon=?, Customer_Postleitzahl=?, Customer_Stadt=?
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

    appt_limit = 40 if is_admin_world_session() else 8
    appointments = db.execute(
        "SELECT * FROM appointments WHERE customer_id = ? ORDER BY appointment_at DESC LIMIT ?",
        (customer_id, appt_limit),
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
        "admin_customer_detail.html" if is_admin_world_session() else "staff_customer_detail.html",
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
        simple_staff_members=staff_members_for_simple_mode(db),
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
    payload = appointment_payload_from_form(request.form, db=db)
    appointment_at = (request.form.get("appointment_at") or "").strip()
    actor_name = _normalize_staff_name(request.form.get("actor_name") or payload["staff_name"], default=payload["staff_name"], db=db)
    if not appointment_at or not _parse_dt_safe(appointment_at):
        flash("Bitte ein gültiges Datum und eine Uhrzeit für den Termin angeben.")
        return redirect(url_for("customer_detail", customer_id=customer_id))
    customer_row = db.execute("SELECT _id FROM _Customers WHERE _id = ?", (customer_id,)).fetchone()
    if not customer_row:
        flash("Der ausgewählte Kontakt wurde nicht gefunden.")
        return redirect(url_for("customer_search_page"))
    if appointment_duplicate_exists(customer_id, appointment_at, payload["title"], payload["staff_name"], db=db):
        flash("Dieser Termin wurde bereits gespeichert.")
        return redirect(url_for("customer_detail", customer_id=customer_id))
    db.execute(
        """
        INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, created_at, status, staff_name, created_by, updated_at, service_codes, service_summary, duration_minutes, processing_minutes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer_id,
            payload["title"],
            appointment_at,
            payload["notes"],
            payload["reminder_hours"],
            datetime.now().isoformat(timespec="seconds"),
            payload["status"],
            payload["staff_name"],
            actor_name,
            datetime.now().isoformat(timespec="seconds"),
            payload["service_codes"],
            payload["service_summary"],
            payload["duration_minutes"],
            payload["processing_minutes"],
        ),
    )
    db.commit()
    try:
        notify_result = notify_other_staff_for_appointment(customer_id, payload["title"], appointment_at, payload["staff_name"], actor_name)
    except Exception as exc:
        app.logger.exception("Notify Fehler bei Terminanlage: %s", exc)
        notify_result = {"sent": 0}
    flash_msg = "Termin wurde gespeichert."
    if push_delivery_ready() and notify_result.get("sent", 0) > 0 and is_admin_session():
        flash_msg += f" Hintergrund-Push gesendet: {notify_result['sent']}."
    elif not vapid_ready() and is_admin_session():
        flash_msg += " Push ist noch nicht komplett aktiv - bitte VAPID-Keys in Render setzen."
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
    if not appointment_at or not _parse_dt_safe(appointment_at):
        flash("Bitte ein gültiges Datum und eine Uhrzeit für den Termin angeben.")
        return redirect(url_for("customer_detail", customer_id=row["customer_id"]))

    payload = appointment_payload_from_form(request.form, db=db)
    db.execute(
        """
        UPDATE appointments
        SET title = ?, appointment_at = ?, notes = ?, reminder_hours = ?, status = ?, staff_name = ?, updated_at = ?, service_codes = ?, service_summary = ?, duration_minutes = ?, processing_minutes = ?
        WHERE id = ?
        """,
        (
            payload["title"],
            appointment_at,
            payload["notes"],
            payload["reminder_hours"],
            payload["status"],
            payload["staff_name"],
            datetime.now().isoformat(timespec="seconds"),
            payload["service_codes"],
            payload["service_summary"],
            payload["duration_minutes"],
            payload["processing_minutes"],
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
    allowed_status = {"geplant", "erledigt", "abgesagt", "verschoben", "no_show"}
    status = (request.form.get("status") or "geplant").strip()
    if status not in allowed_status:
        status = "geplant"
    row = db.execute("SELECT customer_id FROM appointments WHERE id = ?", (appointment_id,)).fetchone()
    if not row:
        flash("Termin nicht gefunden.")
        return redirect(url_for("index"))
    db.execute("UPDATE appointments SET status = ? WHERE id = ?", (status, appointment_id))
    db.commit()
    flash("Terminstatus wurde aktualisiert.")
    return redirect(url_for("customer_detail", customer_id=row["customer_id"]))


@app.route("/staff/appointment/<int:appointment_id>/edit", methods=["GET", "POST"])
@staff_or_admin_required
def staff_appointment_edit(appointment_id):
    set_ui_world("staff")
    db = get_db()
    row = db.execute(
        """
        SELECT a.*, c._id AS customer_id_ref,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname, '') AS customer_firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name, '') AS customer_lastname
        FROM appointments a
        LEFT JOIN _Customers c ON c._id = a.customer_id
        WHERE a.id = ?
        LIMIT 1
        """,
        (appointment_id,),
    ).fetchone()
    if not row:
        flash("Termin wurde nicht gefunden.")
        return redirect(url_for("staff_today"))

    if request.method == "POST":
        appointment_at = (request.form.get("appointment_at") or "").strip()
        if not appointment_at or not _parse_dt_safe(appointment_at):
            flash("Bitte ein gültiges Datum und eine Uhrzeit auswählen.")
            return redirect(url_for("staff_appointment_edit", appointment_id=appointment_id))

        allowed_status = {"geplant", "erledigt", "abgesagt", "verschoben", "no_show"}
        next_status = (request.form.get("status") or "geplant").strip()
        if next_status not in allowed_status:
            next_status = "geplant"

        selected_ids = [part.strip() for part in request.form.getlist("selected_services") if part.strip()]
        if not selected_ids:
            selected_ids = [part.strip() for part in (request.form.get("selected_services") or "").split(",") if part.strip()]
        selected_services = [SERVICE_PRESET_MAP[item] for item in selected_ids if item in SERVICE_PRESET_MAP]
        service_summary = ", ".join(item["label"] for item in selected_services)
        duration_minutes = sum(int(item.get("active", 0) or 0) for item in selected_services) or 30
        processing_minutes = sum(int(item.get("processing", 0) or 0) for item in selected_services)
        service_codes = ",".join(item["id"] for item in selected_services)
        staff_name = _normalize_staff_name(
            request.form.get("staff_name"),
            default=default_staff_for_simple_mode(db),
            db=db,
        )

        db.execute(
            """
            UPDATE appointments
            SET appointment_at = ?, staff_name = ?, status = ?, notes = ?, service_codes = ?, service_summary = ?, duration_minutes = ?, processing_minutes = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                appointment_at,
                staff_name,
                next_status,
                (request.form.get("notes") or "").strip(),
                service_codes,
                service_summary,
                duration_minutes,
                processing_minutes,
                datetime.now().isoformat(timespec="seconds"),
                appointment_id,
            ),
        )
        db.commit()
        flash("Termin wurde aktualisiert.")
        return redirect(url_for("staff_today"))

    return render_template(
        "staff_appointment_edit.html",
        appt=row,
        service_presets=SERVICE_PRESETS,
        simple_staff_members=staff_members_for_simple_mode(db),
        current_endpoint="staff_appointment_edit",
        app_version=APP_VERSION,
    )





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
    duration_minutes = int(appt["duration_minutes"] or 30) if "duration_minutes" in appt.keys() else 30
    processing_minutes = int(appt["processing_minutes"] or 0) if "processing_minutes" in appt.keys() else 0
    return {
        "id": appt["id"],
        "customer_id": appt["customer_id"],
        "title": appt["title"],
        "service_codes": (appt["service_codes"] or "") if "service_codes" in appt.keys() else "",
        "service_summary": (appt["service_summary"] or "") if "service_summary" in appt.keys() else "",
        "duration_minutes": duration_minutes,
        "processing_minutes": processing_minutes,
        "appointment_at": appt["appointment_at"],
        "time_short": time_short,
        "status": status,
        "status_class": status.lower().replace(" ", "-"),
        "staff_name": appt["staff_name"] or "Ute",
        "firstname": appt["_firstname"],
        "lastname": appt["_name"],
        "customer_name": f"{appt['_firstname'] or ''} {appt['_name'] or ''}".strip(),
        "phone": customer_phone(appt) or "-",
        "notes": appt["notes"] or "",
    }



def _fetch_calendar_appointments(start_dt, end_dt, staff="Alle"):
    db = get_db()
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    query = f"""
        SELECT a.*,
               COALESCE(NULLIF(a.manual_firstname, ''), c._firstname) AS _firstname,
               COALESCE(NULLIF(a.manual_lastname, ''), c._name) AS _name,
               COALESCE(NULLIF(a.manual_phone, ''), {mobile_sql}) AS Customer_Mobiltelefon,
               COALESCE(NULLIF(a.manual_phone, ''), {phone_sql}) AS Customer_PersoenlichesTelefon
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
        payload = appointment_payload_from_form(request.form, db=db)
        actor_name = _normalize_staff_name(request.form.get("actor_name") or payload["staff_name"], default=payload["staff_name"], db=db)
        manual_firstname = (request.form.get("manual_firstname") or "").strip()
        manual_lastname = (request.form.get("manual_lastname") or "").strip()
        manual_phone = (request.form.get("manual_phone") or "").strip()
        manual_email = (request.form.get("manual_email") or "").strip()
        manual_name = " ".join(part for part in [manual_firstname, manual_lastname] if part).strip()
        manual_placeholder_customer_id = None

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
                flash("Bitte einen Kontakt aus der Datenbank wählen oder einen Namen für den manuellen Termin eintragen.")
                return redirect(url_for("appointments_hub"))
            manual_placeholder_customer_id = ensure_manual_placeholder_customer(db)
            customer_id = manual_placeholder_customer_id

        if appointment_duplicate_exists(
            customer_id,
            appointment_at,
            payload["title"],
            payload["staff_name"],
            manual_firstname=manual_firstname,
            manual_lastname=manual_lastname,
            manual_phone=manual_phone,
            db=db,
        ):
            flash("Dieser Termin wurde bereits gespeichert.")
            return redirect(url_for("appointments_hub"))

        db.execute(
            """
            INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, created_at, status, staff_name, created_by, updated_at, manual_firstname, manual_lastname, manual_phone, manual_email, service_codes, service_summary, duration_minutes, processing_minutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                payload["title"],
                appointment_at,
                payload["notes"],
                payload["reminder_hours"],
                datetime.now().isoformat(timespec="seconds"),
                payload["status"],
                payload["staff_name"],
                actor_name,
                datetime.now().isoformat(timespec="seconds"),
                manual_firstname,
                manual_lastname,
                manual_phone,
                manual_email,
                payload["service_codes"],
                payload["service_summary"],
                payload["duration_minutes"],
                payload["processing_minutes"],
            ),
        )
        db.commit()
        try:
            notify_result = notify_other_staff_for_appointment(
                notify_customer_id,
                payload["title"],
                appointment_at,
                payload["staff_name"],
                actor_name,
                manual_name=manual_name,
            )
        except Exception as e:
            app.logger.exception("Notify Fehler bei Terminanlage: %s", e)
            notify_result = {"sent": 0, "error": str(e)}
        flash_msg = "Termin wurde gespeichert."
        if not customer_id_raw.isdigit() and (manual_firstname or manual_lastname):
            flash_msg += " Manueller Kontakt wurde nicht in der Kundenliste gespeichert."
        if push_delivery_ready() and notify_result.get("sent", 0) > 0 and is_admin_session():
            flash_msg += f" Hintergrund-Push gesendet: {notify_result['sent']}."
        elif not vapid_ready() and is_admin_session():
            flash_msg += " Push ist noch nicht komplett aktiv - bitte VAPID-Keys in Render setzen."
        flash(flash_msg)
        if is_admin_world_session():
            return redirect(url_for("appointments_hub"))
        return redirect(url_for("staff_today"))

    email_sql, mobile_sql, phone_sql, _ = customer_contact_select_sql()
    if is_admin_world_session():
        customers = db.execute(
            f"""
            SELECT _id, COALESCE(_firstname, '') AS firstname, COALESCE(_name, '') AS lastname,
                   COALESCE({mobile_sql}, {phone_sql}, '') AS phone
            FROM _Customers
            WHERE COALESCE(_name, '') <> '__MANUELLER_TERMIN__'
            ORDER BY COALESCE(_name, '') COLLATE NOCASE ASC, COALESCE(_firstname, '') COLLATE NOCASE ASC
            LIMIT 300
            """
        ).fetchall()
    else:
        customers = []
    today_rows = today_appointments(limit=50)
    active_staff = get_staff_members(db)
    simple_staff_members = staff_members_for_simple_mode(db)
    default_staff = get_default_staff(db)
    today_split = {name: [] for name in active_staff}
    for row in today_rows:
        staff_name = row["staff_name"] if row["staff_name"] in active_staff else default_staff
        today_split.setdefault(staff_name, []).append(row)

    prefill_at = (request.args.get("appointment_at") or "").strip()
    if not prefill_at:
        prefill_at = datetime.now().replace(second=0, microsecond=0).isoformat(timespec="minutes")
    prefill_staff = _normalize_staff_name(request.args.get("staff"), default=get_default_staff(db), db=db)
    if prefill_staff == "Alle":
        prefill_staff = DEFAULT_STAFF
    prefill_source = (request.args.get("source") or "manual").strip()
    prefill_payload = {
        "customer_id": (request.args.get("customer_id") or "").strip(),
        "selected_services": "",
        "service_summary": "",
        "appointment_at": prefill_at,
        "active_until": "",
        "finish_at": "",
        "staff_name": default_staff_for_simple_mode(db) if not is_admin_session() else prefill_staff,
        "status": "geplant",
        "reminder_hours": 24,
        "notes": "",
        "manual_firstname": "",
        "manual_lastname": "",
        "manual_phone": "",
        "manual_email": "",
        "duration_minutes": 30,
        "processing_minutes": 0,
    }
    ping_token = (request.args.get("ping") or "").strip()
    if ping_token:
        ping = get_appointment_ping(ping_token, mark_used=True, db=db)
        if ping:
            prefill_payload.update(ping["payload"])
            prefill_at = prefill_payload["appointment_at"] or prefill_at
            prefill_staff = prefill_payload["staff_name"] or prefill_staff
            prefill_source = "qr_ping"
        else:
            flash("QR-Ping nicht gefunden oder abgelaufen.")

    template_name = "admin_appointments.html" if is_admin_world_session() else "staff_new_appointment.html"
    return render_template(
        template_name,
        customers=customers,
        today_split=today_split,
        upcoming=next_appointments(limit=20),
        current_endpoint="appointments_hub",
        app_version=APP_VERSION,
        prefill_at=prefill_at,
        prefill_staff=prefill_staff,
        prefill_source=prefill_source,
        prefill_payload=prefill_payload,
        service_presets=SERVICE_PRESETS,
        simple_staff_members=simple_staff_members,
    )


@app.route("/appointments/ping/<token>")
@login_required
def appointment_ping_open(token):
    return redirect(url_for("appointments_hub", ping=(token or "").strip(), source="qr_ping"))


@app.route("/api/appointments/ping", methods=["POST"])
@login_required
def appointment_ping_create():
    db = get_db()
    payload = request.get_json(silent=True) or {}
    created_by = session.get("staff_name") or session.get("admin_username") or ""
    ping = create_appointment_ping(payload, created_by=created_by, db=db)
    db.commit()
    link_url = url_for("appointment_ping_open", token=ping["token"], _external=True)
    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=320x320&data={quote(link_url, safe='')}"
    return jsonify(
        {
            "ok": True,
            "token": ping["token"],
            "url": link_url,
            "qr_url": qr_url,
            "expires_at": ping["expires_at"],
            "payload": ping["payload"],
        }
    )



@app.route("/calendar")
@login_required
def calendar_view():
    default_view = "week" if is_admin_world_session() else "day"
    view = (request.args.get("view") or default_view).strip().lower()
    if view not in {"day", "week", "month"}:
        view = "week"

    selected_date = parse_iso_date(request.args.get("date"))
    db = get_db()
    staff_options = get_staff_options(db)
    own_staff = default_staff_for_simple_mode(db)
    mine_param = (request.args.get("mine") or "").strip().lower()
    mine_mode = mine_param in {"1", "true", "yes", "on"}
    if not is_admin_session() and not mine_param and not (request.args.get("staff") or "").strip():
        mine_mode = True

    staff = (request.args.get("staff") or ("Alle" if is_admin_session() else own_staff)).strip()
    if staff not in staff_options:
        staff = "Alle" if is_admin_session() else own_staff
    if not is_admin_session():
        if mine_mode:
            staff = own_staff
        elif staff == "Sven":
            staff = own_staff

    day_view = _build_day_view(selected_date, staff) if view == "day" else None
    week_view = _build_week_view(selected_date, staff) if view == "week" else None
    month_view = _build_month_view(selected_date, staff) if view == "month" else None
    split_day_views = None
    if view == "day" and staff == "Alle":
        members = get_staff_members(db)
        preferred_members = [name for name in ["Ute", "Jessi"] if name in members]
        remaining_members = [name for name in members if name not in preferred_members]
        ordered_members = preferred_members + remaining_members
        split_day_views = {name: _build_day_view(selected_date, name) for name in ordered_members}
    simple_day_items = appointments_for_day(selected_date, staff, db=db) if view == "day" else []

    chip_dates = []
    for offset in range(0, 6):
        chip_day = selected_date + timedelta(days=offset)
        chip_dates.append(
            {
                "date": chip_day.isoformat(),
                "label": chip_day.strftime("%a %d"),
                "is_today": chip_day == datetime.now().date(),
            }
        )

    template_name = "admin_calendar.html" if is_admin_world_session() else "staff_today.html"
    return render_template(
        template_name,
        view=view,
        staff=staff,
        selected_date=selected_date.isoformat(),
        selected_date_label=selected_date.strftime("%d.%m.%Y"),
        today_date_obj=datetime.now().date(),
        prev_date=_calendar_nav_date(selected_date, view, -1),
        next_date=_calendar_nav_date(selected_date, view, 1),
        today_date=datetime.now().date().isoformat(),
        day_view=day_view,
        week_view=week_view,
        month_view=month_view,
        split_day_views=split_day_views,
        simple_day_items=simple_day_items,
        mine_mode=mine_mode,
        own_staff=own_staff,
        current_endpoint="calendar_view",
        app_version=APP_VERSION,
        bookable_staff=simple_staff_members if (simple_staff_members := staff_members_for_simple_mode(db)) else get_staff_members(db),
        staff_day_chip_dates=chip_dates,
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
            "staff_name": row["staff_name"] or DEFAULT_STAFF,
            "status": row["status"] or "geplant",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or row["created_at"] or "",
            "created_by": row["created_by"] or (row["staff_name"] or DEFAULT_STAFF),
        })

    return {"ok": True, "items": items, "server_time": datetime.now().isoformat(timespec="seconds")}


@app.route("/api/push/public-key")
@login_required
def push_public_key():
    if not ENABLE_PUSH:
        return {
            "ok": True,
            "public_key": "",
            "publicKey": "",
            "enabled": False,
            "native_enabled": False,
            "delivery_enabled": False,
            "disabled": True,
            "error": "Push ist serverseitig deaktiviert.",
        }
    public_key = vapid_public_key()
    webpush_enabled = vapid_ready()
    return {
        "ok": True,
        "public_key": public_key,
        "publicKey": public_key,
        "enabled": webpush_enabled,
        "native_enabled": fcm_ready(),
        "delivery_enabled": push_delivery_ready(),
        "service_worker_required": True,
        "service_worker_enabled": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
        "format": "base64url",
        "generated": bool(_get_app_setting("push:vapid_generated_at", "")),
        "pywebpush_available": bool(webpush),
        "error": "" if webpush_enabled else "VAPID oder pywebpush nicht bereit.",
    }


@app.route("/api/push/status")
@login_required
def push_status():
    db = get_db()
    staff_members = get_staff_members(db)
    counts_by_staff = {}
    for member in staff_members:
        row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?", (member,)).fetchone()
        counts_by_staff[member] = int(row["cnt"] or 0) if row else 0
    total_row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions").fetchone()
    total_devices = int(total_row["cnt"] or 0) if total_row else 0
    last_error_row = db.execute(
        "SELECT last_error, updated_at FROM push_subscriptions WHERE COALESCE(last_error, '') <> '' ORDER BY COALESCE(updated_at, created_at) DESC LIMIT 1"
    ).fetchone()
    last_error = (last_error_row["last_error"] if last_error_row else "") or ""
    last_error_at = (last_error_row["updated_at"] if last_error_row else "") or ""

    if not ENABLE_PUSH:
        return {
            "ok": True,
            "enabled": False,
            "device_count": 0,
            "staff_name": _normalize_staff_name(request.args.get("staff_name"), default=DEFAULT_STAFF),
            "disabled": True,
            "enable_push": False,
            "enable_service_worker": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
            "webpush_enabled": False,
            "native_enabled": False,
            "pywebpush_available": bool(webpush),
            "service_worker_required": True,
            "service_worker_enabled": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
            "counts_by_staff": counts_by_staff,
            "total_devices": total_devices,
            "last_error": last_error,
            "last_error_at": last_error_at,
            "error": "Push ist serverseitig deaktiviert.",
        }
    staff_name = _normalize_staff_name(request.args.get("staff_name"), default=DEFAULT_STAFF)
    if staff_name == "Alle":
        staff_name = DEFAULT_STAFF
    enabled = push_delivery_ready()
    count = 0
    try:
        row = db.execute(
            "SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?",
            (staff_name,),
        ).fetchone()
        count = int(row["cnt"] or 0) if row else 0
    except Exception:
        count = 0
    return {
        "ok": True,
        "enabled": enabled,
        "staff_name": staff_name,
        "subscriptions": count,
        "device_count": count,
        "enable_push": bool(ENABLE_PUSH and not SAFE_MODE),
        "enable_service_worker": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
        "webpush_enabled": vapid_ready(),
        "native_enabled": fcm_ready(),
        "pywebpush_available": bool(webpush),
        "service_worker_required": True,
        "service_worker_enabled": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
        "counts_by_staff": counts_by_staff,
        "total_devices": total_devices,
        "last_error": last_error,
        "last_error_at": last_error_at,
    }


@app.route("/api/push/overview")
@admin_required
def push_overview():
    if not ENABLE_PUSH:
        return {"ok": True, "enabled": False, "webpush_enabled": False, "native_enabled": False, "generated_keys": False, "total_devices": 0, "active_devices": 0, "counts_by_staff": {}, "disabled": True}
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
        "enabled": push_delivery_ready(),
        "webpush_enabled": vapid_ready(),
        "native_enabled": fcm_ready(),
        "vapid_ready": vapid_ready(),
        "pywebpush_available": bool(webpush),
        "service_worker_required": True,
        "service_worker_enabled": bool(ENABLE_SERVICE_WORKER and not SAFE_MODE),
        "generated_keys": bool(_get_app_setting("push:vapid_generated_at", "")),
        "total_devices": total_devices,
        "active_devices": total_active,
        "counts_by_staff": {name: len(push_devices_for_staff(name)) for name in get_staff_members(db)},
        "last_run_at": get_setting("automation:last_run_at", ""),
        "last_run_summary": get_setting("automation:last_run_summary", ""),
        "last_run_error": get_setting("automation:last_run_error", ""),
    }


@app.route("/api/push/devices")
@admin_required
def push_devices():
    if not ENABLE_PUSH:
        return {"ok": True, "items": [], "disabled": True}
    db = get_db()
    active_staff = get_staff_members(db)
    staff_name = (request.args.get("staff_name") or "").strip()
    if staff_name not in active_staff:
        staff_name = None
    return {"ok": True, "items": push_devices_for_staff(staff_name)}


@app.route("/api/push/device/<int:subscription_id>/test")
@admin_required
def push_test_device(subscription_id):
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
    row = get_db().execute("SELECT * FROM push_subscriptions WHERE id = ?", (subscription_id,)).fetchone()
    if not row:
        return {"ok": False, "error": "Gerät nicht gefunden."}, 404
    label = _push_device_label(row)
    result = send_push_to_subscription_row(
        row,
        f"Test-Push für {label}",
        f"Dieses Gerät ist für {row['staff_name'] or 'Ute'} aktiv.",
        "/calendar",
    )
    _touch_push_subscription(subscription_id, last_test_at=datetime.now().isoformat(timespec="seconds"))
    return {"ok": True, "result": result, "device_name": label}


@app.route("/api/push/devices/cleanup", methods=["POST"])
@admin_required
def push_cleanup_devices():
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
    db = get_db()
    before_row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions").fetchone()
    before = int(before_row["cnt"] or 0) if before_row else 0
    db.execute("DELETE FROM push_subscriptions WHERE COALESCE(last_error, '') <> '' OR COALESCE(fail_count, 0) >= 3")
    db.commit()
    after_row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions").fetchone()
    after = int(after_row["cnt"] or 0) if after_row else 0
    return {"ok": True, "removed": max(before - after, 0), "remaining": after}


@app.route("/api/push/device/<int:subscription_id>", methods=["DELETE"])
@admin_required
def push_delete_device(subscription_id):
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
    db = get_db()
    db.execute("DELETE FROM push_subscriptions WHERE id = ?", (subscription_id,))
    db.commit()
    return {"ok": True}


@app.route("/api/push/subscribe", methods=["POST"])
@login_required
def push_subscribe():
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
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
    keys = subscription.get("keys") or {}
    auth_key = (keys.get("auth") or "").strip() if isinstance(keys, dict) else ""
    p256dh_key = (keys.get("p256dh") or "").strip() if isinstance(keys, dict) else ""
    requested_staff = payload.get("staff_name") or session.get("staff_name")
    staff_name = _normalize_staff_name(requested_staff, default=session.get("staff_name") or DEFAULT_STAFF)
    device_name = (payload.get("device_name") or "").strip()[:80]
    if not endpoint or not auth_key or not p256dh_key:
        return {"ok": False, "error": "Unvollständige Push-Subscription empfangen."}, 400

    now = datetime.now().isoformat(timespec="seconds")
    db = get_db()
    if device_name:
        db.execute(
            """
            DELETE FROM push_subscriptions
            WHERE endpoint <> ?
              AND staff_name = ?
              AND COALESCE(device_name, '') = ?
              AND COALESCE(user_agent, '') = ?
            """,
            (endpoint, staff_name, device_name, request.headers.get("User-Agent", "")[:500]),
        )
    db.execute(
        """
        INSERT INTO push_subscriptions(endpoint, subscription_json, provider, staff_name, device_name, user_agent, created_at, updated_at, last_seen_at, last_error, fail_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(endpoint) DO UPDATE SET
            subscription_json = excluded.subscription_json,
            provider = excluded.provider,
            staff_name = excluded.staff_name,
            device_name = excluded.device_name,
            user_agent = excluded.user_agent,
            updated_at = excluded.updated_at,
            last_seen_at = excluded.last_seen_at,
            last_error = '',
            fail_count = 0
        """,
        (endpoint, json.dumps(subscription), "webpush", staff_name, device_name, request.headers.get("User-Agent", "")[:500], now, now, now, "", 0),
    )
    db.commit()
    row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?", (staff_name,)).fetchone()
    return {
        "ok": True,
        "staff_name": staff_name,
        "device_name": device_name,
        "device_count": int(row["cnt"] or 0) if row else 0,
        "provider": "webpush",
        "message": "Push aktiviert.",
    }


@app.route("/api/push/native-subscribe", methods=["POST"])
@login_required
def push_native_subscribe():
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
    payload = request.get_json(silent=True) or {}
    token = (payload.get("token") or "").strip()
    if not token:
        return {"ok": False, "error": "FCM-Token fehlt."}, 400
    requested_staff = payload.get("staff_name") or session.get("staff_name")
    staff_name = _normalize_staff_name(requested_staff, default=session.get("staff_name") or DEFAULT_STAFF)
    device_name = (payload.get("device_name") or "").strip()[:80]
    platform = (payload.get("platform") or "android").strip()[:40] or "android"
    endpoint = f"fcm:{token}"
    now = datetime.now().isoformat(timespec="seconds")
    user_agent = request.headers.get("User-Agent", "")[:500]
    db = get_db()
    if device_name:
        db.execute(
            """
            DELETE FROM push_subscriptions
            WHERE endpoint <> ?
              AND provider = 'fcm'
              AND staff_name = ?
              AND COALESCE(device_name, '') = ?
            """,
            (endpoint, staff_name, device_name),
        )
    db.execute(
        """
        INSERT INTO push_subscriptions(endpoint, subscription_json, provider, staff_name, device_name, user_agent, created_at, updated_at, last_seen_at, last_error, fail_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(endpoint) DO UPDATE SET
            subscription_json = excluded.subscription_json,
            provider = excluded.provider,
            staff_name = excluded.staff_name,
            device_name = excluded.device_name,
            user_agent = excluded.user_agent,
            updated_at = excluded.updated_at,
            last_seen_at = excluded.last_seen_at,
            last_error = '',
            fail_count = 0
        """,
        (endpoint, json.dumps({"token": token, "platform": platform, "source": "capacitor"}), "fcm", staff_name, device_name, user_agent, now, now, now, "", 0),
    )
    db.commit()
    row = db.execute("SELECT COUNT(*) AS cnt FROM push_subscriptions WHERE staff_name = ?", (staff_name,)).fetchone()
    return {"ok": True, "staff_name": staff_name, "device_name": device_name, "device_count": int(row["cnt"] or 0) if row else 0, "provider": "fcm", "native_enabled": fcm_ready()}


@app.route("/api/push/unsubscribe", methods=["POST"])
@login_required
def push_unsubscribe():
    if not ENABLE_PUSH:
        return {"ok": True, "disabled": True}
    payload = request.get_json(silent=True) or {}
    endpoint = ((payload.get("subscription") or {}).get("endpoint") or payload.get("endpoint") or "").strip()
    token = (payload.get("token") or "").strip()
    if not endpoint and token:
        endpoint = f"fcm:{token}"
    if endpoint:
        db = get_db()
        db.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        db.commit()
    return {"ok": True}


@app.route("/api/push/ping")
@login_required
def push_ping():
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert.", "enabled": False}, 503
    staff_name = _normalize_staff_name(request.args.get("staff_name"), default=DEFAULT_STAFF)
    actor_name = _normalize_staff_name(session.get("staff_name"), default=DEFAULT_STAFF)
    if staff_name == "Alle":
        result = webpush_send_to_all_staff(
            "Salon Karola Test-Push",
            f"Test-Push von {actor_name} an alle registrierten Geräte.",
            "/calendar",
        )
        devices = push_devices_for_staff(None)
        device_count = len(devices)
    else:
        devices = push_devices_for_staff(staff_name)
        device_count = len(devices)
        result = webpush_send_to_staff(
            staff_name,
            f"Salon Karola Test-Push für {staff_name}",
            f"Test-Push von {actor_name} an {staff_name}.",
            "/calendar",
        )
    return {"ok": True, "result": result, "enabled": push_delivery_ready(), "webpush_enabled": vapid_ready(), "native_enabled": fcm_ready(), "devices": devices, "device_count": device_count}


@app.route("/api/push/test", methods=["POST"])
@login_required
def push_test():
    payload = request.get_json(silent=True) or {}
    staff_name = _normalize_staff_name(payload.get("staff_name") or request.args.get("staff_name"), default=DEFAULT_STAFF)
    if not ENABLE_PUSH:
        return {"ok": False, "error": "Push ist deaktiviert."}, 503
    if staff_name == "Alle":
        result = webpush_send_to_all_staff(
            "Salon Karola Test-Push",
            f"Test-Push von {_normalize_staff_name(session.get('staff_name'), default=DEFAULT_STAFF)} an alle registrierten Geräte.",
            "/calendar",
        )
    else:
        result = webpush_send_to_staff(
            staff_name,
            f"Salon Karola Test-Push für {staff_name}",
            f"Test-Push an {staff_name}.",
            "/calendar",
        )
    sent = int(result.get("sent", 0) or 0)
    errors = result.get("errors", []) or []
    if sent > 0:
        return {"ok": True, "sent": sent, "errors": errors}
    return {"ok": False, "error": "Push konnte nicht zugestellt werden.", "details": errors, "sent": sent}, 502


@app.route("/push")
@admin_required
def push_center():
    return render_template(
        "push.html",
        current_endpoint="push_center",
        app_version=APP_VERSION,
    )


@app.route("/staff", methods=["GET", "POST"])
@admin_required
def staff_management():
    db = get_db()
    if request.method == "POST":
        staff_id = (request.form.get("staff_id") or "").strip()
        staff_name = (request.form.get("staff_name") or "").strip()
        display_name = (request.form.get("display_name") or staff_name).strip()
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password", "")
        current_id = int(staff_id) if staff_id.isdigit() else None

        if not staff_name:
            flash("Bitte einen Mitarbeiternamen eintragen.")
        elif not username:
            flash("Bitte einen Benutzernamen eintragen.")
        else:
            username_row = db.execute("SELECT id FROM staff_users WHERE username = ? LIMIT 1", (username,)).fetchone()
            staff_row = db.execute("SELECT id FROM staff_users WHERE staff_name = ? LIMIT 1", (staff_name,)).fetchone()
            if username_row and username_row["id"] != current_id:
                flash("Dieser Benutzername ist schon vergeben.")
            elif staff_row and staff_row["id"] != current_id:
                flash("Dieser Mitarbeitername ist schon vergeben.")
            elif current_id:
                sql = "UPDATE staff_users SET username = ?, display_name = ?, staff_name = ?"
                params = [username, display_name or staff_name, staff_name]
                if password:
                    sql += ", password = ?"
                    params.append(hash_password(password))
                sql += " WHERE id = ?"
                params.append(current_id)
                db.execute(sql, params)
                db.commit()
                flash(f"Mitarbeiter {staff_name} wurde aktualisiert.")
            else:
                db.execute(
                    """
                    INSERT INTO staff_users(username, password, display_name, staff_name, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (username, hash_password(password) if password else "", display_name or staff_name, staff_name, datetime.now().isoformat(timespec="seconds")),
                )
                db.commit()
                flash(f"Mitarbeiter {staff_name} wurde angelegt.")
        return redirect(url_for("staff_management"))

    staff_users = db.execute(
        """
        SELECT id, username, display_name, staff_name, created_at,
               CASE WHEN COALESCE(password, '') <> '' THEN 1 ELSE 0 END AS has_password
        FROM staff_users
        ORDER BY created_at ASC, id ASC
        """
    ).fetchall()
    return render_template("staff.html", staff_users=staff_users, current_endpoint="staff_management", app_version=APP_VERSION)


@app.route("/whatsapp")
@login_required
def whatsapp_hub():
    db = get_db()
    q = request.args.get("q", "").strip()
    mobile_sql = customer_mobile_reference("c") or "''"
    phone_sql = customer_personal_phone_reference("c") or "''"
    query = f"""
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
    """
    params = []
    conditions = [f"COALESCE({mobile_sql}, {phone_sql}, '') <> ''"]
    if q:
        like = f"%{q}%"
        search_parts = ["c._name LIKE ?", "c._firstname LIKE ?", f"{mobile_sql} LIKE ?"]
        params.extend([like, like, like])
        if phone_sql != "''":
            search_parts.append(f"{phone_sql} LIKE ?")
            params.append(like)
        conditions.append("(" + " OR ".join(search_parts) + ")")
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


@app.route("/api/templates/live")
@admin_required
def templates_live_api():
    db = get_db()
    sync_default_mail_templates(db)
    templates = {
        r["id"]: {"subject": r["subject"], "body": r["body"]}
        for r in db.execute("SELECT * FROM _MailTemplates WHERE id IN ('birthdate','appointment')").fetchall()
    }
    return jsonify({"ok": True, "templates": templates, "app_version": APP_VERSION, "db_path": str(DB_PATH)})


@app.route("/templates", methods=["GET", "POST"])
@admin_required
def templates_view():
    db = get_db()
    sync_default_mail_templates(db)
    if request.method == "POST":
        for template_id in ["birthdate", "appointment"]:
            subject = request.form.get(f"{template_id}_subject", "").strip()
            body = request.form.get(f"{template_id}_body", "").strip()
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
    return render_template("templates.html", templates=templates, current_endpoint="templates_view")


@app.route("/send-test/<int:customer_id>/<template_id>")
@admin_required
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
@admin_required
def run_automation_now():
    result = run_automation_if_due(force=True)
    flash(f"Automatiklauf wurde manuell ausgeführt. {result['summary']}")
    return redirect(url_for("index"))


@app.route("/export/customers.csv")
@admin_required
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
@admin_required
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
@admin_required
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
    ui_world = current_ui_world()
    endpoint_name = request.endpoint or ""
    push_manual_page = endpoint_name in {"push_center", "test_push", "salon_reminders", "staff_more"}
    sw_manual_page = endpoint_name in {"test_service_worker", "test_push"}
    return {
        "admin_name": session.get("admin_name"),
        "logged_in_staff": session.get("staff_name") or get_default_staff(),
        "is_admin": is_admin_session(),
        "is_employee_mode": ui_world == "staff",
        "ui_world": ui_world,
        "is_staff_world": ui_world == "staff",
        "is_admin_world": ui_world == "admin",
        "login_options": default_login_options(),
        "customer_activity_status": customer_activity_status,
        "customer_full_name": customer_full_name,
        "whatsapp_link": whatsapp_link,
        "phone_href": phone_href,
        "customer_phone": customer_phone,
        "row_value": _row_value,
        "app_version": APP_VERSION,
        "staff_members": get_staff_members(),
        "simple_staff_members": staff_members_for_simple_mode(),
        "default_staff": get_default_staff(),
        "simple_default_staff": default_staff_for_simple_mode(),
        "passkeys_ready": passkeys_ready(),
        "service_presets": SERVICE_PRESETS,
        "safe_mode": SAFE_MODE,
        "enable_push": ENABLE_PUSH and not SAFE_MODE,
        "enable_scheduler": ENABLE_SCHEDULER and not SAFE_MODE,
        "enable_service_worker": ENABLE_SERVICE_WORKER and not SAFE_MODE,
        "enable_firebase": ENABLE_FIREBASE and not SAFE_MODE,
        "allow_auto_push_boot": False,
        "allow_auto_service_worker_boot": False,
        "allow_manual_push_controls": push_manual_page,
        "allow_manual_service_worker_controls": sw_manual_page,
    }


def boot_app():
    init_db()
    ensure_default_admin(force_reset=False)
    if ENABLE_PUSH and not SAFE_MODE:
        try:
            _ensure_vapid_keys()
        except Exception as exc:
            try:
                app.logger.warning("VAPID-Key-Erzeugung uebersprungen: %s", exc)
            except Exception:
                pass
    if ENABLE_SCHEDULER and not SAFE_MODE:
        try:
            interval_minutes = int(os.getenv("AUTOMATION_INTERVAL_MINUTES", "15"))
            if interval_minutes < 5:
                interval_minutes = 5
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
            run_automation_if_due(force=True)
        except Exception as exc:
            try:
                app.logger.warning("Scheduler-Start uebersprungen: %s", exc)
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
    ensure_default_admin(force_reset=False)
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
@admin_required
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
@admin_required
def export_database():
    init_db()
    if not DB_PATH.exists():
        flash("Es wurde noch keine Datenbank gefunden.")
        return redirect(url_for("database_tools"))
    return send_file(DB_PATH, as_attachment=True, download_name=f"salon_karola_export_{timestamp_slug()}.sqlite", mimetype="application/octet-stream")


@app.route("/database/backup-zip")
@admin_required
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
@admin_required
def download_backup(filename):
    file_path = (BACKUP_DIR / filename).resolve()
    backup_root = BACKUP_DIR.resolve()
    try:
        file_path.relative_to(backup_root)
    except ValueError:
        flash("Ung?ltiger Backup-Pfad.")
        return redirect(url_for("database_tools"))
    if not file_path.exists() or not file_path.is_file():
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

