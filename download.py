#!/usr/bin/env python3
"""
download.py - Phase 1: Download DEF 14A Proxy Filings
=======================================================
Downloads DEF 14A filings for every ticker in query_context.csv.
Files saved to: filings/{TICKER}/{comp_year}_DEF14A_{filing_date}.html

Comp year = filing year - 1
  e.g. proxy filed 2025-01-10 covers comp year 2024

Outputs:
  filings/{TICKER}/*.html   one file per proxy
  manifest.csv              full index of every filing + status

Safe to re-run - skips already downloaded files.

Usage:
    source venv/bin/activate
    python3 download.py
"""

import argparse
import csv
from pathlib import Path

from edgar_client import (
    build_session, fetch, get_cik_map,
    SEC_SUBMISSIONS_URL, SEC_SUBMISSIONS_OLD, SEC_ARCHIVES_BASE,
)

# ── Config ────────────────────────────────────────────────────────────────────

TICKERS_CSV     = "./query_context.csv"
FILINGS_DIR     = "./filings"
MANIFEST_CSV    = "./manifest.csv"
FILINGS_TO_PULL = 3   # 3 filings back = comp years 2023-2025


# ── Tickers ───────────────────────────────────────────────────────────────────

def load_unique_tickers(path: str) -> list[str]:
    seen, tickers = set(), []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("Ticker") or "").strip().upper()
            if t and t not in seen:
                seen.add(t)
                tickers.append(t)
    return sorted(tickers)


# ── DEF 14A discovery ─────────────────────────────────────────────────────────

def get_def14a_filings(session, cik: str, max_filings: int = FILINGS_TO_PULL) -> list[dict]:
    cik_padded = cik.zfill(10)
    data = fetch(session, SEC_SUBMISSIONS_URL.format(cik_padded=cik_padded)).json()

    recent = data.get("filings", {}).get("recent", {})
    forms   = recent.get("form", [])
    accs    = recent.get("accessionNumber", [])
    dates   = recent.get("filingDate", [])
    docs    = recent.get("primaryDocument", [])

    results = []
    for form, acc, date, doc in zip(forms, accs, dates, docs):
        if form not in ("DEF 14A", "DEF14A"):
            continue
        acc_nodash = acc.replace("-", "")
        results.append({
            "accession":   acc,
            "filing_date": date,
            "comp_year":   int(date[:4]) - 1,
            "doc_url":     f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{doc}",
        })
        if len(results) >= max_filings:
            break

    # Pull older pages if needed
    if len(results) < max_filings:
        for file_entry in data.get("files", []):
            if len(results) >= max_filings:
                break
            try:
                older = fetch(session, SEC_SUBMISSIONS_OLD.format(filename=file_entry["name"])).json()
                o = older.get("filings", {})
                for form, acc, date, doc in zip(
                    o.get("form", []), o.get("accessionNumber", []),
                    o.get("filingDate", []), o.get("primaryDocument", [])
                ):
                    if form not in ("DEF 14A", "DEF14A"):
                        continue
                    acc_nodash = acc.replace("-", "")
                    results.append({
                        "accession":   acc,
                        "filing_date": date,
                        "comp_year":   int(date[:4]) - 1,
                        "doc_url":     f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{doc}",
                    })
                    if len(results) >= max_filings:
                        break
            except Exception:
                pass

    return results


# ── Download ──────────────────────────────────────────────────────────────────

def download_filing(session, ticker: str, filing: dict) -> dict:
    ticker_dir = Path(FILINGS_DIR) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{filing['comp_year']}_DEF14A_{filing['filing_date']}.html"
    filepath = ticker_dir / filename

    row = {
        "ticker":       ticker,
        "comp_year":    filing["comp_year"],
        "filing_date":  filing["filing_date"],
        "accession":    filing["accession"],
        "doc_url":      filing["doc_url"],
        "local_path":   str(filepath),
        "status":       "",
        "file_size_kb": "",
        "error":        "",
    }

    if filepath.exists() and filepath.stat().st_size > 1000:
        row["status"] = "exists"
        row["file_size_kb"] = round(filepath.stat().st_size / 1024, 1)
        return row

    try:
        resp = fetch(session, filing["doc_url"])
        filepath.write_text(resp.text, encoding="utf-8")
        row["status"] = "downloaded"
        row["file_size_kb"] = round(filepath.stat().st_size / 1024, 1)
    except Exception as e:
        row["status"] = "failed"
        row["error"] = str(e)

    return row


# ── Manifest ──────────────────────────────────────────────────────────────────

def write_manifest(rows: list[dict]) -> None:
    fields = ["ticker", "comp_year", "filing_date", "accession",
              "doc_url", "local_path", "status", "file_size_kb", "error"]
    with open(MANIFEST_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nManifest saved: {MANIFEST_CSV} ({len(rows)} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(tickers: list[str] | None = None):
    if not tickers:
        tickers = load_unique_tickers(TICKERS_CSV)
    print(f"Processing {len(tickers)} ticker(s)")

    session = build_session()
    cik_map = get_cik_map(session)

    Path(FILINGS_DIR).mkdir(exist_ok=True)

    manifest_rows = []
    downloaded = skipped = failed = 0
    no_cik = []
    no_filings = []

    for i, ticker in enumerate(tickers, 1):
        cik = cik_map.get(ticker)
        if not cik:
            print(f"[{i:3d}/{len(tickers)}] {ticker:<6}  NO CIK")
            no_cik.append(ticker)
            continue

        try:
            filings = get_def14a_filings(session, cik)
        except Exception as e:
            print(f"[{i:3d}/{len(tickers)}] {ticker:<6}  ERROR: {e}")
            continue

        if not filings:
            print(f"[{i:3d}/{len(tickers)}] {ticker:<6}  NO DEF 14A FOUND")
            no_filings.append(ticker)
            continue

        summary = []
        for filing in filings:
            row = download_filing(session, ticker, filing)
            manifest_rows.append(row)
            if row["status"] == "downloaded":
                downloaded += 1
                summary.append(f"{filing['comp_year']}✓")
            elif row["status"] == "exists":
                skipped += 1
                summary.append(f"{filing['comp_year']}~")
            else:
                failed += 1
                summary.append(f"{filing['comp_year']}✗")

        print(f"[{i:3d}/{len(tickers)}] {ticker:<6}  {' '.join(summary)}")

    write_manifest(manifest_rows)

    print(f"\n{'─'*50}")
    print(f"Downloaded : {downloaded}")
    print(f"Skipped    : {skipped} (already existed)")
    print(f"Failed     : {failed}")
    print(f"No CIK     : {len(no_cik)}")
    print(f"No filings : {len(no_filings)}")
    print(f"\nFiles in ./{FILINGS_DIR}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download DEF 14A proxy filings")
    parser.add_argument("--tickers", help="Comma-separated tickers (default: read from query_context.csv)")
    args = parser.parse_args()
    tickers = (
        [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if args.tickers else None
    )
    run(tickers=tickers)
