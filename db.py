"""
db.py — Shared database helpers for T2D Exec pipelines
=======================================================
CSV read/write for ceo_database.csv and date parsing utilities
used across all pipeline scripts.
"""

import csv
import re
from datetime import date as date_cls
from pathlib import Path

# ── Schema ────────────────────────────────────────────────────────────────────

DB_FIELDS = ["Ticker", "Company Name", "CEO", "CEO Start Date"]

# ── CSV helpers ───────────────────────────────────────────────────────────────

def read_db(path: str) -> list[dict]:
    """Read ceo_database.csv and return all rows as a list of dicts."""
    if not Path(path).exists():
        return []
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_db(path: str, rows: list[dict]) -> None:
    """Write rows back to ceo_database.csv, preserving any extra columns."""
    if not rows:
        return
    all_fields = list(DB_FIELDS)
    for row in rows:
        for k in row:
            if k not in all_fields:
                all_fields.append(k)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(rows)


# ── Date parsing ──────────────────────────────────────────────────────────────

def parse_date(s: str | None) -> date_cls | None:
    """Parse YYYY-MM-DD or M/D/YYYY into a date object. Returns None if blank/invalid."""
    if not s:
        return None
    try:
        return date_cls.fromisoformat(s.strip())
    except ValueError:
        pass
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", (s or "").strip())
    if m:
        try:
            return date_cls(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass
    return None
