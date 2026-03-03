#!/usr/bin/env python3
"""
download_8k.py - Download Targeted 8-K Item 5.02 Filings
==========================================================
Uses the cached proxy-derived CEO start date as a search hint to find the specific
8-K that announced each CEO's appointment, then downloads only that file.

The cached hint date is used ONLY to narrow the search window.
Claude independently extracts the exact date in extract_8k.py.

Strategy:
  1. Parse cached hint date → compute -90/+40-day search window
  2. Fetch SEC submissions metadata (no downloads yet)
  3. Traverse filing pages until we reach filings in that window
  4. Filter to 8-K / 8-K/A with Item 5.02 in the window
  5. Download only those (typically 1-2 per ticker)

Outputs:
  filings_8k/{TICKER}/*.html   targeted 8-K files
  8k_manifest.csv              index with proxy/context hints for extract phase

Safe to re-run — skips already downloaded files.

Usage:
    source venv/bin/activate
    python3 download_8k.py --tickers AAPL,MSFT,NVDA
    python3 download_8k.py --tickers AAPL              # single ticker
    python3 download_8k.py --context-csv my.csv --tickers AAPL,MSFT
"""

import argparse
import csv
import re
from datetime import date as date_cls, timedelta
from pathlib import Path
from urllib.parse import urlencode

import requests

from edgar_client import (
    build_session, fetch, get_cik_map,
    SEC_SUBMISSIONS_URL, SEC_SUBMISSIONS_OLD, SEC_ARCHIVES_BASE, EFTS_SEARCH_URL,
)

# ── Config ────────────────────────────────────────────────────────────────────

CONTEXT_CSV   = "./query_context.csv"
FILINGS_DIR   = "./filings_8k"
MANIFEST_CSV  = "./8k_manifest.csv"
WINDOW_BEFORE = 90     # days before the reference date to search
WINDOW_AFTER  = 40     # days after the reference date to search


# ── Date helpers ──────────────────────────────────────────────────────────────

def parse_context_date(s: str) -> date_cls | None:
    """Parse M/D/YYYY or MM/DD/YYYY → date. Returns None if blank/invalid."""
    s = s.strip()
    if not s:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        try:
            return date_cls(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass
    return None


def in_window(filing_date_str: str, center: date_cls) -> bool:
    """Return True if filing_date_str (YYYY-MM-DD) is within [center-90, center+40]."""
    try:
        fd = date_cls.fromisoformat(filing_date_str)
        return (center - timedelta(days=WINDOW_BEFORE)) <= fd <= (center + timedelta(days=WINDOW_AFTER))
    except ValueError:
        return False


def too_old(filing_date_str: str, center: date_cls) -> bool:
    """Return True if filing is older than (center - WINDOW_BEFORE) — safe to stop paging."""
    try:
        fd = date_cls.fromisoformat(filing_date_str)
        return fd < (center - timedelta(days=WINDOW_BEFORE))
    except ValueError:
        return False


# ── Context loader ────────────────────────────────────────────────────────────

def load_context(path: str, tickers: list[str]) -> dict[str, dict]:
    """Load cached context rows for the requested tickers.

    Returns {TICKER: {ticker, company_name, ceo_name, start_date, start_date_parsed}}
    """
    want = {t.upper() for t in tickers}
    result: dict[str, dict] = {}
    if not Path(path).exists():
        return result
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get("Ticker") or "").strip().upper()
            if ticker not in want:
                continue
            sd_str = (row.get("CEO Start Date") or "").strip()
            result[ticker] = {
                "ticker":        ticker,
                "company_name":  (row.get("Company Name") or "").strip(),
                "ceo_name":      (row.get("CEO") or "").strip(),
                "start_date":    sd_str,
                "start_date_parsed": parse_context_date(sd_str),
            }
    return result


# ── 8-K discovery with date-window filtering ──────────────────────────────────

def find_502_in_window(
    session: requests.Session,
    cik: str,
    center: date_cls | None,
) -> tuple[str, list[dict]]:
    """Find Item 5.02 8-K filings within ±WINDOW_DAYS of center date.

    If center is None (no hint date), returns the 5 most recent Item 5.02
    filings as a fallback (user will need to inspect manually).

    Traverses older submission pages until the window is fully covered or we
    go past the oldest possible relevant filing.
    """
    cik_padded = cik.zfill(10)
    try:
        data = fetch(session, SEC_SUBMISSIONS_URL.format(cik_padded=cik_padded)).json()
    except Exception as e:
        print(f"    submissions fetch error: {e}")
        return "", []

    sec_name: str = data.get("name", "")
    hits: list[dict] = []
    stopped_early = False

    def _scan_block(filings_block: dict) -> bool:
        """Scan one filings block. Returns True if we should stop paging."""
        forms = filings_block.get("form", [])
        accs  = filings_block.get("accessionNumber", [])
        dates = filings_block.get("filingDate", [])
        docs  = filings_block.get("primaryDocument", [])
        items = filings_block.get("items", [])

        for form, acc, date, doc, item in zip(forms, accs, dates, docs, items):
            # If no center date: collect up to 20 most recent Item 5.02 filings
            if center is None:
                if len(hits) >= 20:
                    return True
                if form in ("8-K", "8-K/A") and "5.02" in (item or ""):
                    acc_nodash = acc.replace("-", "")
                    hits.append({
                        "accession":   acc,
                        "filing_date": date,
                        "items":       item,
                        "doc_url": f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{doc}",
                    })
                continue

            # With a center date: stop once we've gone past the window
            if too_old(date, center):
                return True  # signal caller to stop paging

            if form in ("8-K", "8-K/A") and "5.02" in (item or ""):
                if in_window(date, center):
                    acc_nodash = acc.replace("-", "")
                    hits.append({
                        "accession":   acc,
                        "filing_date": date,
                        "items":       item,
                        "doc_url": f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{doc}",
                    })

        return False  # keep paging

    # Scan most-recent block first
    if _scan_block(data.get("filings", {}).get("recent", {})):
        stopped_early = True

    # Traverse older pages until we've covered the window
    if not stopped_early:
        for file_entry in data.get("files", []):
            try:
                older = fetch(
                    session,
                    SEC_SUBMISSIONS_OLD.format(filename=file_entry["name"])
                ).json()
                if _scan_block(older):
                    break
            except Exception:
                pass

    return sec_name, hits


def efts_search_appointment(
    session: requests.Session,
    ceo_name: str,
    cik: str,
    center: date_cls,
    window_days: int = 540,
) -> list[dict]:
    """Fallback historical 8-K search around the proxy hint year via EFTS."""
    tokens = [t for t in re.split(r"[\s.,]+", ceo_name.strip()) if len(t) > 1]
    if not tokens:
        return []

    first = tokens[0]
    last = tokens[-1]
    cik_padded = cik.zfill(10)
    start_dt = (center - timedelta(days=window_days)).isoformat()
    end_dt = (center + timedelta(days=window_days)).isoformat()

    queries = []
    if len(tokens) >= 2:
        queries.append(f'"{first} {last}" "Chief Executive"')
    queries.append(f'"{last}" "Chief Executive Officer"')
    queries.append(f'"{last}" CEO')

    seen: set[str] = set()
    results: list[dict] = []

    for query in queries:
        params = {
            "q": query,
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": start_dt,
            "enddt": end_dt,
        }
        try:
            hits = (
                fetch(session, EFTS_SEARCH_URL + "?" + urlencode(params))
                .json()
                .get("hits", {})
                .get("hits", [])
            )
        except Exception:
            continue

        for hit in hits:
            src = hit.get("_source", {})
            if cik_padded not in src.get("ciks", []):
                continue
            acc = src.get("adsh", "")
            filing_date = src.get("file_date", "")
            if not acc or not filing_date or acc in seen:
                continue
            hit_id = hit.get("_id", "")
            if ":" not in hit_id:
                continue

            _, filename = hit_id.split(":", 1)
            acc_nodash = acc.replace("-", "")
            seen.add(acc)
            results.append({
                "accession": acc,
                "filing_date": filing_date,
                "items": "EFTS",
                "doc_url": f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{filename}",
            })

        if results:
            break

    results.sort(key=lambda r: abs((date_cls.fromisoformat(r["filing_date"]) - center).days))
    return results[:10]


# ── Download ──────────────────────────────────────────────────────────────────

def download_filing(session: requests.Session, ticker: str, filing: dict) -> dict:
    ticker_dir = Path(FILINGS_DIR) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)

    acc_nodash = filing["accession"].replace("-", "")
    filename   = f"{filing['filing_date']}_8K_{acc_nodash}.html"
    filepath   = ticker_dir / filename

    row = {
        "accession":    filing["accession"],
        "filing_date":  filing["filing_date"],
        "items":        filing["items"],
        "doc_url":      filing["doc_url"],
        "local_path":   str(filepath),
        "status":       "",
        "file_size_kb": "",
        "error":        "",
    }

    if filepath.exists() and filepath.stat().st_size > 500:
        row["status"]       = "exists"
        row["file_size_kb"] = round(filepath.stat().st_size / 1024, 1)
        return row

    try:
        resp = fetch(session, filing["doc_url"])
        filepath.write_text(resp.text, encoding="utf-8")
        row["status"]       = "downloaded"
        row["file_size_kb"] = round(filepath.stat().st_size / 1024, 1)
    except Exception as e:
        row["status"] = "failed"
        row["error"]  = str(e)

    return row


# ── Manifest ──────────────────────────────────────────────────────────────────

MANIFEST_FIELDS = [
    "ticker", "company_name", "ceo_name_proxy", "start_date_proxy",
    "filing_date", "accession", "items", "doc_url",
    "local_path", "status", "file_size_kb", "error",
]


def write_manifest(rows: list[dict]) -> None:
    def normalize_manifest_row(row: dict) -> dict:
        normalized = dict(row)
        if not normalized.get("ceo_name_proxy"):
            normalized["ceo_name_proxy"] = normalized.get("ceo_name_airtable", "")
        if not normalized.get("start_date_proxy"):
            normalized["start_date_proxy"] = normalized.get("start_date_airtable", "")
        return {field: normalized.get(field, "") for field in MANIFEST_FIELDS}

    # Append to existing manifest or create new
    exists   = Path(MANIFEST_CSV).exists()
    existing = {}
    if exists:
        with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                existing[r["accession"]] = normalize_manifest_row(r)

    for r in rows:
        existing[r["accession"]] = normalize_manifest_row(r)

    with open(MANIFEST_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(existing.values())
    print(f"\nManifest saved: {MANIFEST_CSV} ({len(existing)} total rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(context_csv: str, tickers: list[str]) -> None:
    companies = load_context(context_csv, tickers)
    for ticker in tickers:
        companies.setdefault(ticker.upper(), {
            "ticker": ticker.upper(),
            "company_name": "",
            "ceo_name": "",
            "start_date": "",
            "start_date_parsed": None,
        })

    print(f"Processing {len(companies)} ticker(s): {', '.join(sorted(companies))}\n")

    session = build_session()
    cik_map = get_cik_map(session)

    Path(FILINGS_DIR).mkdir(exist_ok=True)

    manifest_rows: list[dict] = []
    downloaded = skipped = failed = no_cik = no_filings = 0

    for i, ticker in enumerate(sorted(companies), 1):
        company = companies[ticker]
        cik     = cik_map.get(ticker)
        center  = company["start_date_parsed"]

        if not cik:
            print(f"[{i}/{len(companies)}] {ticker:<6}  NO CIK")
            no_cik += 1
            continue

        window_desc = (
            f"-{WINDOW_BEFORE}/+{WINDOW_AFTER}d of {company['start_date']}"
            if center else "no date → 20 most recent"
        )
        print(f"[{i}/{len(companies)}] {ticker:<6}  {company['ceo_name'] or '?'}  "
              f"Searching {window_desc}")

        sec_name, filings = find_502_in_window(session, cik, center)
        if sec_name and not company["company_name"]:
            company["company_name"] = sec_name

        if not filings and center and company["ceo_name"]:
            filings = efts_search_appointment(session, company["ceo_name"], cik, center)
            if filings:
                print("           no 5.02 filings in submissions window; using historical EFTS fallback")

        if not filings:
            print(f"           NO Item 5.02 8-Ks found in window")
            no_filings += 1
            continue

        for filing in filings:
            row = download_filing(session, ticker, filing)
            manifest_rows.append({
                "ticker":              ticker,
                "company_name":        company["company_name"],
                "ceo_name_proxy":      company["ceo_name"],
                "start_date_proxy":    company["start_date"],
                **row,
            })
            status = row["status"]
            icon   = {"downloaded": "✓", "exists": "~", "failed": "✗"}.get(status, "?")
            print(f"           {filing['filing_date']} {icon}  "
                  f"({row.get('file_size_kb', '?')} KB)  {row.get('error', '')}")
            if status == "downloaded":
                downloaded += 1
            elif status == "exists":
                skipped += 1
            else:
                failed += 1

    write_manifest(manifest_rows)

    print(f"\n{'─'*55}")
    print(f"Downloaded  : {downloaded}")
    print(f"Skipped     : {skipped} (already existed)")
    print(f"Failed      : {failed}")
    print(f"No CIK      : {no_cik}")
    print(f"No filings  : {no_filings}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download targeted 8-K Item 5.02 filings for CEO appointment dates"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--tickers",
        help="Comma-separated tickers to process (e.g. AAPL,MSFT,NVDA)"
    )
    group.add_argument(
        "--all", action="store_true",
        help="Process every ticker in the context CSV"
    )
    parser.add_argument(
        "--context-csv", default=CONTEXT_CSV,
        help=f"Context CSV path (default: {CONTEXT_CSV})"
    )
    args = parser.parse_args()

    if args.all:
        all_tickers: list[str] = []
        if Path(args.context_csv).exists():
            with open(args.context_csv, encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    t = (row.get("Ticker") or "").strip().upper()
                    if t:
                        all_tickers.append(t)
        tickers = sorted(set(all_tickers))
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    run(args.context_csv, tickers)
