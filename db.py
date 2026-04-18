import sqlite3
from pathlib import Path
from contextlib import contextmanager

DB_PATH = Path(__file__).parent / "data" / "registrations.db"

# Rows created before the village column existed are backfilled with this.
LEGACY_DEFAULT_VILLAGE = "წინამძღვრიანთკარი"

TABLE_SCHEMA = """
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
    village         TEXT,        -- search term that first found this row
    first_seen_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

INDEX_SCHEMA = """
CREATE INDEX IF NOT EXISTS idx_app_reg_date  ON registrations(app_reg_date);
CREATE INDEX IF NOT EXISTS idx_last_act_date ON registrations(last_act_date);
CREATE INDEX IF NOT EXISTS idx_is_relevant   ON registrations(is_relevant);
CREATE INDEX IF NOT EXISTS idx_village       ON registrations(village);
"""


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(TABLE_SCHEMA)
        _migrate_village_column(conn)
        conn.executescript(INDEX_SCHEMA)


def _migrate_village_column(conn):
    """Idempotent: adds the village column on pre-existing DBs, backfills NULLs."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(registrations)")}
    if "village" not in cols:
        conn.execute("ALTER TABLE registrations ADD COLUMN village TEXT")
    conn.execute(
        "UPDATE registrations SET village = ? WHERE village IS NULL",
        (LEGACY_DEFAULT_VILLAGE,),
    )


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
