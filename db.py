import sqlite3
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent / "data" / "registrations.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS registrations (
    app_id          TEXT PRIMARY KEY,
    reg_number      TEXT NOT NULL,
    web_transact    TEXT,
    status          TEXT,
    status_id       INTEGER,
    address         TEXT,
    app_reg_date    INTEGER,     -- Unix timestamp: submission
    last_act_date   INTEGER,     -- Unix timestamp: last status change
    applicants_json TEXT,
    is_relevant     INTEGER NOT NULL,
    raw_json        TEXT,
    first_seen_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_app_reg_date  ON registrations(app_reg_date);
CREATE INDEX IF NOT EXISTS idx_last_act_date ON registrations(last_act_date);
CREATE INDEX IF NOT EXISTS idx_is_relevant   ON registrations(is_relevant);
"""

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA)

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()