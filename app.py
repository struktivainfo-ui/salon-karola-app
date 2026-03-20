
import csv
import io
import os
import shutil
import smtplib
import sqlite3
import tempfile
import zipfile
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from urllib.parse import quote

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
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

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "salon_karola.db")))

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.secret_key = os.getenv("SECRET_KEY", "salon-karola-ultra-secret")
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024

scheduler = BackgroundScheduler(timezone=os.getenv("APP_TIMEZONE", "Europe/Berlin"))


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

        columns = [row[1] for row in conn.execute("PRAGMA table_info(appointments)").fetchall()]
        if "status" not in columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN status TEXT DEFAULT 'geplant'")

        conn.commit()


# ---------- Mail ----------
def send_email(to_email, subject, body):
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("SMTP_SENDER", username or "")
    use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

    if not all([host, port, username, password, sender]):
        raise RuntimeError("SMTP ist nicht vollständig konfiguriert. Bitte .env prüfen.")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=20) as server:
        if use_tls:
            server.starttls()
        server.login(username, password)
        server.send_message(msg)


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


def smtp_ready():
    needed = ["SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_SENDER"]
    return all(os.getenv(k) for k in needed)


def whatsapp_link(customer, text=None):
    number = "".join(ch for ch in customer_phone(customer) if ch.isdigit())
    if number.startswith("0"):
        number = "49" + number[1:]
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
            sent += 1
        except Exception as exc:
            log_email(customer["_id"], "birthday", subject, body, customer["_mail"], "error", str(exc))
            errors += 1

    return {"checked": checked, "sent": sent, "errors": errors}


def run_appointment_job():
    now = datetime.now()

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        appointments = conn.execute(
            """
            SELECT a.*, c.*
            FROM appointments a
            JOIN _Customers c ON c._id = a.customer_id
            WHERE a.reminder_sent_at IS NULL
              AND c._mail IS NOT NULL AND TRIM(c._mail) <> ''
            ORDER BY a.appointment_at ASC
            """
        ).fetchall()

        checked = 0
        sent = 0
        errors = 0

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
                sent += 1
            except Exception as exc:
                log_email(appt["customer_id"], "appointment", subject, body, appt["_mail"], "error", str(exc))
                errors += 1

    return {"checked": checked, "sent": sent, "errors": errors}


def scheduler_tick():
    started_at = datetime.now().isoformat(timespec="seconds")
    try:
        birthday_result = run_birthday_job()
        appointment_result = run_appointment_job()
        summary = (
            f"Geburtstage geprüft: {birthday_result['checked']}, gesendet: {birthday_result['sent']}, Fehler: {birthday_result['errors']} | "
            f"Termine geprüft: {appointment_result['checked']}, gesendet: {appointment_result['sent']}, Fehler: {appointment_result['errors']}"
        )
        set_setting("automation:last_run_at", started_at)
        set_setting("automation:last_run_summary", summary)
        set_setting("automation:last_run_error", "")
        return {"ok": True, "summary": summary}
    except Exception as exc:
        set_setting("automation:last_run_at", started_at)
        set_setting("automation:last_run_error", str(exc))
        raise


# ---------- Dashboard ----------
def dashboard_stats():
    db = get_db()
    total_customers = db.execute("SELECT COUNT(*) FROM _Customers").fetchone()[0]
    total_emails = db.execute("SELECT COUNT(*) FROM _Customers WHERE _mail IS NOT NULL AND TRIM(_mail) <> ''").fetchone()[0]
    total_mobile = db.execute("SELECT COUNT(*) FROM _Customers WHERE Customer_Mobiltelefon IS NOT NULL AND TRIM(Customer_Mobiltelefon) <> ''").fetchone()[0]
    upcoming_appointments = db.execute(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ?",
        (datetime.now().isoformat(timespec="minutes"),),
    ).fetchone()[0]
    sent_today = db.execute(
        "SELECT COUNT(*) FROM email_log WHERE date(sent_at) = date('now', 'localtime') AND status = 'sent'"
    ).fetchone()[0]

    birthdays_30 = 0
    for row in db.execute("SELECT _birthdate FROM _Customers WHERE _birthdate IS NOT NULL AND TRIM(_birthdate) <> ''").fetchall():
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

    today_start = datetime.now().strftime("%Y-%m-%dT00:00")
    tomorrow_start = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00")
    week_end = (datetime.now() + timedelta(days=7)).isoformat(timespec="minutes")

    appointments_today = db.execute(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ? AND appointment_at < ?",
        (today_start, tomorrow_start),
    ).fetchone()[0]
    appointments_week = db.execute(
        "SELECT COUNT(*) FROM appointments WHERE appointment_at >= ? AND appointment_at < ?",
        (datetime.now().isoformat(timespec="minutes"), week_end),
    ).fetchone()[0]

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
        SELECT a.*, c._firstname, c._name, c.Customer_Mobiltelefon, c.Customer_PersönlichesTelefon
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

    base_query = """
        SELECT c.*, MAX(a.appointment_at) AS last_appointment_at
        FROM _Customers c
        LEFT JOIN appointments a ON a.customer_id = c._id
    """
    params = []
    conditions = []

    if tag:
        base_query += " JOIN customer_tags t ON t.customer_id = c._id"
        conditions.append("t.tag = ?")
        params.append(tag)

    if q:
        like = f"%{q}%"
        conditions.append(
            "(c._name LIKE ? OR c._firstname LIKE ? OR c._mail LIKE ? OR c.Customer_Mobiltelefon LIKE ? OR c.Customer_PersönlichesTelefon LIKE ? OR c.Customer_Stadt LIKE ?)"
        )
        params.extend([like, like, like, like, like, like])

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    base_query += " GROUP BY c._id ORDER BY c._name, c._firstname LIMIT 200"
    customers = db.execute(base_query, params).fetchall()
    tags = db.execute("SELECT tag, COUNT(*) AS cnt FROM customer_tags GROUP BY tag ORDER BY tag").fetchall()

    return render_template(
        "index.html",
        customers=customers,
        q=q,
        tag=tag,
        stats=dashboard_stats(),
        upcoming=next_appointments(),
        birthdays=upcoming_birthdays(),
        tags=tags,
        smtp_ready=smtp_ready(),
        automation=get_automation_status(),
        inactive=inactive_customers(),
        now=datetime.now(),
        current_endpoint="index",
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
    return render_template("customer_form.html", customer=None, appointments=[], logs=[], tags_text="", wa_link="", current_endpoint="customer_new", customer_status="neu")


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
    )


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
    db.execute(
        """
        INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer_id,
            request.form.get("title", "Salon-Termin").strip() or "Salon-Termin",
            request.form["appointment_at"],
            request.form.get("notes", "").strip(),
            int(request.form.get("reminder_hours", "24") or 24),
            datetime.now().isoformat(timespec="seconds"),
            request.form.get("status", "geplant").strip() or "geplant",
        ),
    )
    db.commit()
    flash("Termin wurde gespeichert.")
    return redirect(url_for("customer_detail", customer_id=customer_id))


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


@app.route("/calendar")
@login_required
def calendar_view():
    db = get_db()
    start = request.args.get("start") or datetime.now().strftime("%Y-%m-01")
    end_dt = datetime.fromisoformat(start) + timedelta(days=45)
    appointments = db.execute(
        """
        SELECT a.*, c._firstname, c._name, c.Customer_Mobiltelefon, c.Customer_PersönlichesTelefon
        FROM appointments a
        JOIN _Customers c ON c._id = a.customer_id
        WHERE a.appointment_at >= ? AND a.appointment_at < ?
        ORDER BY a.appointment_at ASC
        """,
        (start, end_dt.isoformat(timespec="minutes")),
    ).fetchall()
    grouped = {}
    for appt in appointments:
        day = appt["appointment_at"][:10]
        grouped.setdefault(day, []).append(appt)
    return render_template("calendar.html", grouped=grouped, start=start, current_endpoint="calendar_view")


@app.route("/templates", methods=["GET", "POST"])
@login_required
def templates_view():
    db = get_db()
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
    result = scheduler_tick()
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


@app.context_processor
def inject_globals():
    return {
        "admin_name": session.get("admin_name"),
        "automation": get_automation_status() if session.get("admin_logged_in") else {},
    }


def boot_app():
    init_db()
    interval_minutes = int(os.getenv("AUTOMATION_INTERVAL_MINUTES", "15"))
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
                    info["counts"][table] = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                except Exception:
                    info["counts"][table] = "—"
    return info


def backup_current_database(label="manual"):
    if not DB_PATH.exists():
        return None
    backup_path = BACKUP_DIR / f"salon_karola_{label}_{timestamp_slug()}.sqlite"
    shutil.copy2(DB_PATH, backup_path)
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
    shutil.copy2(tmp_path, DB_PATH)
    tmp_path.unlink(missing_ok=True)
    init_db()
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
                dest.execute(
                    """
                    INSERT INTO appointments(customer_id, title, appointment_at, notes, reminder_hours, reminder_sent_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        mapped_customer,
                        row["title"],
                        row["appointment_at"],
                        row["notes"],
                        row["reminder_hours"],
                        row["reminder_sent_at"],
                        row["created_at"],
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
                    f"Datenbank zusammengeführt. Neue Kontakte: {merged['customers']}, neue Termine: {merged['appointments']}, neue Vorlagen: {merged['templates']}. Backup: {backup_path.name if backup_path else 'keins'}"
                )
            else:
                backup_path, info_after = replace_database_from_upload(file)
                flash(
                    f"Datenbank komplett ersetzt. Aktuelle Kontakte: {info_after['counts'].get('_Customers', 0)}. Backup: {backup_path.name if backup_path else 'keins'}"
                )
        except Exception as exc:
            flash(f"Datenbank-Import fehlgeschlagen: {exc}")
        return redirect(url_for("database_tools"))
    return render_template("database_tools.html", db_info=db_info, backup_files=backup_files, current_endpoint="database_tools")


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
