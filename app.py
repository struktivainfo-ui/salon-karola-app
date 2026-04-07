
import os
from pathlib import Path
from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DATABASE_PATH", str(BASE_DIR / "salon_karola.db")))
APP_VERSION = "v-debug-1.0"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/debug-db")
def debug_db():
    db = get_db()
    try:
        row = db.execute(
            "SELECT id, subject, body FROM _MailTemplates WHERE id = 'birthdate'"
        ).fetchone()
    except Exception as e:
        row = None

    return jsonify({
        "db_path": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "db_size": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
        "pid": os.getpid(),
        "app_version": APP_VERSION,
        "birthdate_subject": row["subject"] if row else None,
        "birthdate_body_start": (row["body"][:200] if row and row["body"] else None),
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
