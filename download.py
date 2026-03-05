#!/usr/bin/env python3
"""
download.py - Phase 1: Download DEF 14A Proxy Filings
=======================================================
Downloads DEF 14A filings for every ticker in query_context.csv.
Files saved to: filings/{TICKER}/{comp_year}_DEF14A_{filing_date}.html

Comp year is inferred from the proxy text when possible.
Fallback: filing year - 1
  e.g. proxy filed 2025-01-10 often covers comp year 2024,
  but off-calendar fiscal years can still be comp year 2025.

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
import re
from pathlib import Path

from edgar_client import (
    build_session, fetch, get_ciks,
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


def infer_comp_year(html: str, filing_date: str) -> int:
    """Infer the compensation fiscal year from the proxy text.

    Most proxies cover the prior calendar year, but companies with non-December
    fiscal year ends often report a fiscal year equal to the filing year.
    """
    try:
        filing_year = int(filing_date[:4])
    except (TypeError, ValueError):
        filing_year = 0

    fallback = filing_year - 1 if filing_year else 0
    if not html:
        return fallback

    # Cover-page fiscal year references usually appear near the top of the file.
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    head = text[:20000]

    patterns = [
        r"\bfiscal\s+year\s+ended\s+[A-Za-z]+\s+\d{1,2},\s+(20\d{2})\b",
        r"\bfiscal\s+year\s+ended\s+[A-Za-z]+\s+(20\d{2})\b",
        r"\bfor\s+fiscal\s+(20\d{2})\b",
        r"\bfiscal\s+year\s+(20\d{2})\b",
        r"\bfiscal\s+(20\d{2})\b",
        r"\b(20\d{2})\s+proxy\s+statement\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, head, re.IGNORECASE)
        if not match:
            continue
        year = int(match.group(1))
        if filing_year and year not in (filing_year - 1, filing_year):
            continue
        return year

    return fallback


def existing_filing_path(ticker_dir: Path, filing_date: str) -> Path | None:
    matches = sorted(ticker_dir.glob(f"*_DEF14A_{filing_date}.html"))
    for path in matches:
        try:
            if path.stat().st_size > 1000:
                return path
        except OSError:
            continue
    return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_filing(session, ticker: str, filing: dict) -> dict:
    ticker_dir = Path(FILINGS_DIR) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    filepath = existing_filing_path(ticker_dir, filing["filing_date"])

    row = {
        "ticker":       ticker,
        "comp_year":    filing["comp_year"],
        "filing_date":  filing["filing_date"],
        "accession":    filing["accession"],
        "doc_url":      filing["doc_url"],
        "local_path":   str(filepath) if filepath else "",
        "status":       "",
        "file_size_kb": "",
        "error":        "",
    }

    if filepath:
        html = filepath.read_text(encoding="utf-8", errors="replace")
        row["comp_year"] = infer_comp_year(html, filing["filing_date"])
        row["local_path"] = str(filepath)
        row["status"] = "exists"
        row["file_size_kb"] = round(filepath.stat().st_size / 1024, 1)
        return row

    try:
        resp = fetch(session, filing["doc_url"])
        row["comp_year"] = infer_comp_year(resp.text, filing["filing_date"])
        filename = f"{row['comp_year']}_DEF14A_{filing['filing_date']}.html"
        filepath = ticker_dir / filename
        filepath.write_text(resp.text, encoding="utf-8")
        row["local_path"] = str(filepath)
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
    if not rows and Path(MANIFEST_CSV).exists():
        with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
            existing = list(csv.DictReader(f))
        print(f"\nManifest unchanged: {MANIFEST_CSV} ({len(existing)} rows)")
        return
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
    cik_map = get_ciks(session, tickers)

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
                summary.append(f"{row['comp_year']}✓")
            elif row["status"] == "exists":
                skipped += 1
                summary.append(f"{row['comp_year']}~")
            else:
                failed += 1
                summary.append(f"{row['comp_year']}✗")

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
