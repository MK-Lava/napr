import json
import sys
import time
import argparse
from datetime import datetime

import requests

from db import init_db, get_conn
from villages import VILLAGES

# Windows' default cp1252 stdout can't print Georgian village names.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

API_URL = "https://naprweb.reestri.gov.ge/api/search"
RELEVANCE_KEYWORD = "საკუთრების უფლების რეგისტრაცია"
REQUEST_DELAY_SECONDS = 1.0     # be polite to the server
REQUEST_TIMEOUT_SECONDS = 60    # date-filtered queries are notably slower
MAX_PAGES_SAFETY = 500          # hard ceiling to prevent runaway loops

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://naprweb.reestri.gov.ge",
    "Referer": "https://naprweb.reestri.gov.ge/_dea/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}


def _to_api_date(iso: str | None) -> str | None:
    # API wants day-first DD.MM.YYYY; ISO YYYY-MM-DD returns HTTP 500.
    if not iso:
        return None
    return datetime.strptime(iso, "%Y-%m-%d").strftime("%d.%m.%Y")


def fetch_page(page: int, village: str, date_from: str | None) -> dict:
    payload = {
        "page": page, "search": "", "regno": "",
        "datefrom": _to_api_date(date_from), "dateto": None,
        "person": "", "address": village, "cadcode": "",
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    r.raise_for_status()
    return r.json()


def is_relevant(web_transact: str | None) -> bool:
    return RELEVANCE_KEYWORD in (web_transact or "")


def row_from_api(raw: dict, village: str) -> dict:
    return {
        "app_id":          raw["appID"],
        "reg_number":      raw["regNumber"],
        "web_transact":    raw.get("webTransact"),
        "status":          raw.get("status"),
        "status_id":       int(raw["statusId"])    if raw.get("statusId")    else None,
        "address":         raw.get("address"),
        "app_reg_date":    int(raw["appRegDate"])  if raw.get("appRegDate")  else None,
        "last_act_date":   int(raw["lastActDate"]) if raw.get("lastActDate") else None,
        "applicants_json": json.dumps(raw.get("applicants", []), ensure_ascii=False),
        "is_relevant":     1 if is_relevant(raw.get("webTransact")) else 0,
        "raw_json":        json.dumps(raw, ensure_ascii=False),
        "village":         village,
    }


# village is set on INSERT only; on conflict it's left alone (frozen at first-seen).
UPSERT_SQL = """
INSERT INTO registrations (
    app_id, reg_number, web_transact, status, status_id,
    address, app_reg_date, last_act_date, applicants_json,
    is_relevant, raw_json, village
) VALUES (
    :app_id, :reg_number, :web_transact, :status, :status_id,
    :address, :app_reg_date, :last_act_date, :applicants_json,
    :is_relevant, :raw_json, :village
)
ON CONFLICT(app_id) DO UPDATE SET
    status          = excluded.status,
    status_id       = excluded.status_id,
    last_act_date   = excluded.last_act_date,
    web_transact    = excluded.web_transact,
    applicants_json = excluded.applicants_json,
    raw_json        = excluded.raw_json,
    last_seen_at    = CURRENT_TIMESTAMP;
"""


def upsert_rows(conn, rows):
    """Returns (new_count, updated_count)."""
    new_count = updated_count = 0
    for row in rows:
        existing = conn.execute(
            "SELECT last_act_date, status_id FROM registrations WHERE app_id = ?",
            (row["app_id"],),
        ).fetchone()
        conn.execute(UPSERT_SQL, row)
        if existing is None:
            new_count += 1
        elif (existing["last_act_date"] != row["last_act_date"]
              or existing["status_id"] != row["status_id"]):
            updated_count += 1
    return new_count, updated_count


def scrape_village(conn, village: str, date_from: str | None, max_pages: int | None):
    total_new = total_updated = 0
    ceiling = max_pages if max_pages is not None else MAX_PAGES_SAFETY
    header = f"=== {village}"
    if date_from:
        header += f" (from {date_from})"
    header += f" — up to {ceiling} page(s) ==="
    print(f"\n{header}")

    for page in range(1, ceiling + 1):
        try:
            data = fetch_page(page, village, date_from)
        except requests.RequestException as e:
            print(f"  [warn] page {page} failed: {e} — sleeping 5s and retrying")
            time.sleep(5)
            try:
                data = fetch_page(page, village, date_from)
            except requests.RequestException as e2:
                print(f"  [error] page {page} failed twice: {e2} — stopping this village")
                break

        applist = data.get("applist", [])
        if not applist:
            print(f"  page {page}: empty, stopping")
            break

        rows = [row_from_api(r, village) for r in applist]
        n, u = upsert_rows(conn, rows)
        total_new += n
        total_updated += u

        # Print every page in small runs; every 10th page in long backfills
        if max_pages is None and page % 10 != 0 and n + u == 0:
            pass  # stay quiet on long backfills with no changes
        else:
            print(f"  page {page}: {len(rows)} fetched, {n} new, {u} updated")

        time.sleep(REQUEST_DELAY_SECONDS)

    return total_new, total_updated


def scrape(max_pages: int | None):
    """max_pages=None means 'keep going until empty'. Applies per village."""
    init_db()
    grand_new = grand_updated = 0
    with get_conn() as conn:
        for v in VILLAGES:
            n, u = scrape_village(conn, v["name"], v.get("date_from"), max_pages)
            grand_new += n
            grand_updated += u

    print(f"\nDone. {grand_new} new, {grand_updated} updated "
          f"across {len(VILLAGES)} village(s).")


def inspect():
    """Quick sanity check — print stats about the DB."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM registrations").fetchone()["c"]
        relevant = conn.execute(
            "SELECT COUNT(*) AS c FROM registrations WHERE is_relevant = 1"
        ).fetchone()["c"]
        print(f"Total records:    {total}")
        print(f"Relevant (ownership registrations): {relevant}")

        print("\nBy village:")
        rows = conn.execute("""
            SELECT COALESCE(village, '(null)') AS village,
                   COUNT(*) AS n,
                   SUM(is_relevant) AS relevant
            FROM registrations GROUP BY village ORDER BY n DESC
        """).fetchall()
        for r in rows:
            print(f"  {r['village']}: {r['n']} total, {r['relevant']} relevant")

        print("\nBy year (relevant only, based on last_act_date):")
        rows = conn.execute("""
            SELECT strftime('%Y', datetime(last_act_date, 'unixepoch')) AS yr,
                   COUNT(*) AS n
            FROM registrations
            WHERE is_relevant = 1 AND last_act_date IS NOT NULL
            GROUP BY yr ORDER BY yr
        """).fetchall()
        for r in rows:
            print(f"  {r['yr']}: {r['n']}")

        print("\n5 most recent relevant registrations:")
        rows = conn.execute("""
            SELECT reg_number, village, address,
                   datetime(last_act_date, 'unixepoch') AS completed_at
            FROM registrations
            WHERE is_relevant = 1
            ORDER BY last_act_date DESC LIMIT 5
        """).fetchall()
        for r in rows:
            print(f"  {r['reg_number']} | {r['completed_at']} | {r['village']} | {r['address']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["probe", "daily", "backfill", "inspect"],
        default="daily",
        help="probe=page 1 only; daily=first 3 pages; backfill=all pages; inspect=DB stats",
    )
    args = parser.parse_args()

    if args.mode == "probe":
        scrape(max_pages=1)
    elif args.mode == "daily":
        scrape(max_pages=3)
    elif args.mode == "backfill":
        scrape(max_pages=None)
    elif args.mode == "inspect":
        inspect()
