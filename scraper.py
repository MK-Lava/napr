import json
import time
import argparse
from datetime import datetime

import requests

from db import init_db, get_conn

API_URL = "https://naprweb.reestri.gov.ge/api/search"
SEARCH_ADDRESS = "წინამძღვრიანთკარი"
RELEVANCE_KEYWORD = "საკუთრების უფლების რეგისტრაცია"
REQUEST_DELAY_SECONDS = 1.0     # be polite to the server
MAX_PAGES_SAFETY = 500          # hard ceiling to prevent runaway loops

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://naprweb.reestri.gov.ge",
    "Referer": "https://naprweb.reestri.gov.ge/_dea/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
}


def fetch_page(page: int) -> dict:
    payload = {
        "page": page, "search": "", "regno": "",
        "datefrom": None, "dateto": None,
        "person": "", "address": SEARCH_ADDRESS, "cadcode": "",
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def is_relevant(web_transact: str | None) -> bool:
    return RELEVANCE_KEYWORD in (web_transact or "")


def row_from_api(raw: dict) -> dict:
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
    }


UPSERT_SQL = """
INSERT INTO registrations (
    app_id, reg_number, web_transact, status, status_id,
    address, app_reg_date, last_act_date, applicants_json,
    is_relevant, raw_json
) VALUES (
    :app_id, :reg_number, :web_transact, :status, :status_id,
    :address, :app_reg_date, :last_act_date, :applicants_json,
    :is_relevant, :raw_json
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


def scrape(max_pages: int | None):
    """max_pages=None means 'keep going until we get an empty page'."""
    init_db()
    total_new = total_updated = 0
    ceiling = max_pages if max_pages is not None else MAX_PAGES_SAFETY

    with get_conn() as conn:
        for page in range(1, ceiling + 1):
            try:
                data = fetch_page(page)
            except requests.RequestException as e:
                print(f"  [warn] page {page} failed: {e} — sleeping 5s and retrying")
                time.sleep(5)
                try:
                    data = fetch_page(page)
                except requests.RequestException as e2:
                    print(f"  [error] page {page} failed twice: {e2} — stopping")
                    break

            applist = data.get("applist", [])
            if not applist:
                print(f"  page {page}: empty, stopping")
                break

            rows = [row_from_api(r) for r in applist]
            n, u = upsert_rows(conn, rows)
            total_new += n
            total_updated += u

            # Print every page in small runs; every 10th page in long backfills
            if max_pages is None and page % 10 != 0 and n + u == 0:
                pass  # stay quiet on long backfills with no changes
            else:
                print(f"  page {page}: {len(rows)} fetched, {n} new, {u} updated")

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\nDone. {total_new} new, {total_updated} updated.")


def inspect():
    """Quick sanity check — print stats about the DB."""
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM registrations").fetchone()["c"]
        relevant = conn.execute(
            "SELECT COUNT(*) AS c FROM registrations WHERE is_relevant = 1"
        ).fetchone()["c"]
        print(f"Total records:    {total}")
        print(f"Relevant (ownership registrations): {relevant}")

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
            SELECT reg_number, address,
                   datetime(last_act_date, 'unixepoch') AS completed_at
            FROM registrations
            WHERE is_relevant = 1
            ORDER BY last_act_date DESC LIMIT 5
        """).fetchall()
        for r in rows:
            print(f"  {r['reg_number']} | {r['completed_at']} | {r['address']}")


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