import os
import io
import csv
import shutil
import sqlite3
import zipfile
from datetime import datetime, date
from functools import wraps

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    send_file,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "salon-karola-secret")

DB_PATH = os.environ.get("DATABASE_PATH", "salon_karola.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    db = get_db()

    db.execute("""
    CREATE TABLE IF NOT EXISTS staff_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS _Customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        phone TEXT,
        email TEXT,
        birthday TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS _Appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        appointment_at TEXT,
        service TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS _MailTemplates (
        id TEXT PRIMARY KEY,
        subject TEXT,
        body TEXT
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS _EmailLogs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recipient TEXT,
        subject TEXT,
        status TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    user = db.execute(
        "SELECT id FROM staff_users WHERE username = ?",
        ("karola",)
    ).fetchone()

    if not user:
        db.execute(
            "INSERT INTO staff_users (username, password) VALUES (?, ?)",
            ("karola", "Karola123!")
        )

    birthday_tpl = db.execute(
        "SELECT id FROM _MailTemplates WHERE id = ?",
        ("birthday",)
    ).fetchone()

    if not birthday_tpl:
        db.execute(
            "INSERT INTO _MailTemplates (id, subject, body) VALUES (?, ?, ?)",
            (
                "birthday",
                "Alles Gute zum Geburtstag von Salon Karola",
                "Herzlichen Glückwunsch zum Geburtstag! Ihr Team von Salon Karola."
            )
        )

    appointment_tpl = db.execute(
        "SELECT id FROM _MailTemplates WHERE id = ?",
        ("appointment",)
    ).fetchone()

    if not appointment_tpl:
        db.execute(
            "INSERT INTO _MailTemplates (id, subject, body) VALUES (?, ?, ?)",
            (
                "appointment",
                "Terminerinnerung von Salon Karola",
                "Dies ist Ihre Erinnerung an Ihren Termin bei Salon Karola."
            )
        )

    db.commit()
    db.close()


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


@app.route("/login", methods=["GET", "POST"])
def login():
    init_db()

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        db = get_db()
        user = db.execute(
            "SELECT * FROM staff_users WHERE username = ? AND password = ?",
            (username, password),
        ).fetchone()
        db.close()

        if user:
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            next_url = request.args.get("next") or url_for("index")
            return redirect(next_url)

        flash("Falscher Benutzername oder falsches Passwort.")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    init_db()
    db = get_db()

    customers = db.execute("""
        SELECT id, name, phone, email, birthday, notes, created_at
        FROM _Customers
        ORDER BY id DESC
    """).fetchall()

    appointments = db.execute("""
        SELECT a.id, a.customer_id, a.appointment_at, a.service, a.notes,
               c.name AS customer_name
        FROM _Appointments a
        LEFT JOIN _Customers c ON c.id = a.customer_id
        ORDER BY a.appointment_at ASC
        LIMIT 20
    """).fetchall()

    today_str = date.today().isoformat()
    todays_birthdays = db.execute("""
        SELECT *
        FROM _Customers
        WHERE birthday IS NOT NULL
          AND birthday != ''
          AND substr(birthday, 6, 5) = ?
        ORDER BY name ASC
    """, (today_str[5:],)).fetchall()

    db.close()

    return render_template(
        "index.html",
        customers=customers,
        appointments=appointments,
        todays_birthdays=todays_birthdays,
        today=today_str,
        now=datetime.now(),
    )


@app.route("/customer/new", methods=["GET", "POST"])
@login_required
def customer_new():
    init_db()
    db = get_db()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        email = request.form.get("email", "").strip()
        birthday = request.form.get("birthday", "").strip()
        notes = request.form.get("notes", "").strip()

        db.execute("""
            INSERT INTO _Customers (name, phone, email, birthday, notes)
            VALUES (?, ?, ?, ?, ?)
        """, (name, phone, email, birthday, notes))
        db.commit()
        db.close()
        flash("Kontakt wurde gespeichert.")
        return redirect(url_for("index"))

    db.close()
    return render_template("customer_form.html", customer=None)


@app.route("/calendar")
@login_required
def calendar_view():
    init_db()
    db = get_db()

    appointments = db.execute("""
        SELECT a.id, a.customer_id, a.appointment_at, a.service, a.notes,
               c.name AS customer_name
        FROM _Appointments a
        LEFT JOIN _Customers c ON c.id = a.customer_id
        ORDER BY a.appointment_at ASC
    """).fetchall()

    customers = db.execute("""
        SELECT id, name
        FROM _Customers
        ORDER BY name ASC
    """).fetchall()

    db.close()
    return render_template("calendar.html", appointments=appointments, customers=customers, now=datetime.now())


@app.route("/mail-templates", methods=["GET", "POST"])
@login_required
def mail_templates():
    init_db()
    db = get_db()

    if request.method == "POST":
        birthday_subject = request.form.get("birthday_subject", "")
        birthday_body = request.form.get("birthday_body", "")
        appointment_subject = request.form.get("appointment_subject", "")
        appointment_body = request.form.get("appointment_body", "")

        db.execute(
            "UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?",
            (birthday_subject, birthday_body, "birthday"),
        )
        db.execute(
            "UPDATE _MailTemplates SET subject = ?, body = ? WHERE id = ?",
            (appointment_subject, appointment_body, "appointment"),
        )
        db.commit()
        flash("Mail-Vorlagen gespeichert.")

    templates = db.execute("SELECT * FROM _MailTemplates").fetchall()
    tpl_map = {row["id"]: row for row in templates}
    db.close()

    return render_template("templates.html", templates=tpl_map)


@app.route("/logs")
@login_required
def logs_view():
    init_db()
    db = get_db()
    logs = db.execute("""
        SELECT *
        FROM _EmailLogs
        ORDER BY id DESC
        LIMIT 100
    """).fetchall()
    db.close()
    return render_template("logs.html", logs=logs)


@app.route("/database")
@login_required
def database_tools():
    return render_template("database_tools.html")


@app.route("/database/export")
@login_required
def database_export():
    init_db()
    if not os.path.exists(DB_PATH):
        flash("Keine Datenbank gefunden.")
        return redirect(url_for("database_tools"))

    return send_file(
        DB_PATH,
        as_attachment=True,
        download_name="salon_karola_export.sqlite"
    )


@app.route("/database/export-zip")
@login_required
def database_export_zip():
    init_db()
    if not os.path.exists(DB_PATH):
        flash("Keine Datenbank gefunden.")
        return redirect(url_for("database_tools"))

    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(DB_PATH, arcname="salon_karola.sqlite")
    memory_file.seek(0)

    return send_file(
        memory_file,
        as_attachment=True,
        download_name="salon_karola_backup.zip",
        mimetype="application/zip",
    )


@app.route("/database/import", methods=["GET", "POST"])
@login_required
def database_import():
    init_db()

    if request.method == "POST":
        uploaded = request.files.get("database_file")
        mode = request.form.get("mode", "replace")

        if not uploaded or uploaded.filename == "":
            flash("Bitte eine Datei auswählen.")
            return redirect(url_for("database_import"))

        filename = uploaded.filename.lower()
        if not (
            filename.endswith(".sqlite")
            or filename.endswith(".db")
            or filename.endswith(".backup")
        ):
            flash("Nur .sqlite, .db oder .backup sind erlaubt.")
            return redirect(url_for("database_import"))

        temp_path = DB_PATH + ".upload"
        uploaded.save(temp_path)

        if mode == "replace":
            backup_path = DB_PATH + ".bak"
            if os.path.exists(DB_PATH):
                shutil.copy2(DB_PATH, backup_path)
            shutil.move(temp_path, DB_PATH)
            init_db()
            flash("Datenbank wurde ersetzt.")
            return redirect(url_for("database_tools"))

        if mode == "merge":
            target = get_db()
            source = sqlite3.connect(temp_path)
            source.row_factory = sqlite3.Row

            # customers
            try:
                source_customers = source.execute("""
                    SELECT name, phone, email, birthday, notes
                    FROM _Customers
                """).fetchall()

                for row in source_customers:
                    exists = target.execute("""
                        SELECT id FROM _Customers
                        WHERE name = ? AND COALESCE(email, '') = COALESCE(?, '')
                    """, (row["name"], row["email"])).fetchone()

                    if not exists:
                        target.execute("""
                            INSERT INTO _Customers (name, phone, email, birthday, notes)
                            VALUES (?, ?, ?, ?, ?)
                        """, (
                            row["name"],
                            row["phone"],
                            row["email"],
                            row["birthday"],
                            row["notes"],
                        ))
            except sqlite3.Error:
                pass

            # templates
            try:
                source_templates = source.execute("""
                    SELECT id, subject, body
                    FROM _MailTemplates
                """).fetchall()

                for row in source_templates:
                    exists = target.execute(
                        "SELECT id FROM _MailTemplates WHERE id = ?",
                        (row["id"],)
                    ).fetchone()

                    if exists:
                        target.execute("""
                            UPDATE _MailTemplates
                            SET subject = ?, body = ?
                            WHERE id = ?
                        """, (row["subject"], row["body"], row["id"]))
                    else:
                        target.execute("""
                            INSERT INTO _MailTemplates (id, subject, body)
                            VALUES (?, ?, ?)
                        """, (row["id"], row["subject"], row["body"]))
            except sqlite3.Error:
                pass

            target.commit()
            target.close()
            source.close()
            os.remove(temp_path)

            flash("Datenbank wurde zusammengeführt.")
            return redirect(url_for("database_tools"))

    return render_template("import.html")


@app.route("/csv/import", methods=["GET", "POST"])
@login_required
def csv_import():
    init_db()

    if request.method == "POST":
        uploaded = request.files.get("csv_file")
        if not uploaded or uploaded.filename == "":
            flash("Bitte CSV-Datei auswählen.")
            return redirect(url_for("csv_import"))

        content = uploaded.read().decode("utf-8", errors="ignore").splitlines()
        reader = csv.DictReader(content)
        db = get_db()

        for row in reader:
            db.execute("""
                INSERT INTO _Customers (name, phone, email, birthday, notes)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row.get("name", ""),
                row.get("phone", ""),
                row.get("email", ""),
                row.get("birthday", ""),
                row.get("notes", ""),
            ))

        db.commit()
        db.close()
        flash("CSV importiert.")
        return redirect(url_for("index"))

    return render_template("import.html")


@app.route("/csv/export")
@login_required
def csv_export():
    init_db()
    db = get_db()
    rows = db.execute("""
        SELECT name, phone, email, birthday, notes
        FROM _Customers
        ORDER BY id DESC
    """).fetchall()
    db.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "phone", "email", "birthday", "notes"])

    for row in rows:
        writer.writerow([
            row["name"],
            row["phone"],
            row["email"],
            row["birthday"],
            row["notes"],
        ])

    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    mem.seek(0)

    return send_file(
        mem,
        as_attachment=True,
        download_name="customers.csv",
        mimetype="text/csv",
    )


@app.route("/run-jobs")
@login_required
def run_jobs():
    flash("Automatik wurde manuell gestartet.")
    return redirect(url_for("index"))


with app.app_context():
    init_db()


if __name__ == "__main__":
    app.run(debug=True)
