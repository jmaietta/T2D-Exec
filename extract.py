#!/usr/bin/env python3
"""
extract.py - Phase 2: Extract CEO Compensation from Downloaded Proxies
========================================================================
Usage:
    source ~/Desktop/ceorater-fundamentals/venv/bin/activate
    python3 extract.py              # run all
    python3 extract.py --ticker AAPL  # test one ticker
    python3 extract.py --force      # reprocess everything
"""

import argparse
import csv
import json
import os
import re
import time
import warnings
from pathlib import Path

import anthropic
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

MANIFEST_CSV   = "./manifest.csv"
RESULTS_CSV    = "./ceo_comp_results.csv"
PROGRESS_CSV   = "./extract_progress.csv"
MODEL          = "claude-sonnet-4-6"
MAX_TOKENS     = 1500
API_DELAY      = 3.0
MAX_TEXT_CHARS = 25000


def load_manifest(ticker_filter=None):
    if not Path(MANIFEST_CSV).exists():
        print(f"ERROR: {MANIFEST_CSV} not found. Run download.py first.")
        raise SystemExit(1)
    want = {t.upper() for t in ticker_filter} if ticker_filter else None
    rows = []
    with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row["status"] not in ("downloaded", "exists"):
                continue
            if want and row["ticker"].upper() not in want:
                continue
            if not Path(row["local_path"]).exists():
                continue
            rows.append(row)

    # Preserve every downloaded proxy so we can extract multiple compensation
    # years per ticker, but keep the processing order stable and recent-first.
    rows.sort(key=lambda row: (row["ticker"], row["filing_date"]), reverse=True)

    print(f"Manifest: {len(rows)} filings ready")
    return rows


def load_progress():
    done = set()
    if Path(PROGRESS_CSV).exists():
        with open(PROGRESS_CSV, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                done.add(f"{row['ticker']}|{row['comp_year']}|{row['accession']}")
    return done


def mark_progress(ticker, comp_year, accession):
    exists = Path(PROGRESS_CSV).exists()
    with open(PROGRESS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "comp_year", "accession"])
        if not exists:
            writer.writeheader()
        writer.writerow({"ticker": ticker, "comp_year": comp_year, "accession": accession})


def score_table(table):
    raw_text = table.get_text(" ", strip=True)
    text = raw_text.lower()
    score = 0

    if "summary compensation table" in text:
        score += 10
    if "chief executive" in text or "principal executive" in text:
        score += 8
    if "salary" in text:
        score += 3
    if "total" in text:
        score += 2
    if "bonus" in text:
        score += 1
    if "stock" in text or "option" in text:
        score += 1

    year_matches = re.findall(r"\b(202[2-9])\b", raw_text)
    score += min(len(year_matches), 6) * 2

    dollar_matches = re.findall(r"\$[\d,]+", raw_text)
    score += min(len(dollar_matches), 5) * 2

    rows = table.find_all("tr")
    if len(rows) < 4:
        score -= 8
    elif len(rows) >= 6:
        score += 3

    return score


def extract_text(html):
    soup = BeautifulSoup(html, "lxml")

    sct_table = None
    best_score = 0

    for table in soup.find_all("table"):
        s = score_table(table)
        if s > best_score:
            best_score = s
            sct_table = table

    if sct_table and best_score >= 5:
        rows_text = []
        for tr in sct_table.find_all("tr"):
            cells = []
            for td in tr.find_all(["td", "th"]):
                cell = td.get_text(" ", strip=True)
                cell = re.sub(r"\s+", " ", cell).strip()
                cells.append(cell)
            if any(cells):
                rows_text.append("\t".join(cells))
        table_str = "\n".join(rows_text)

        prev = sct_table.find_previous(string=re.compile(
            r"summary compensation", re.IGNORECASE
        ))
        context = ""
        if prev:
            context = f"[Section: {prev.strip()[:300]}]\n\n"

        result = context + "SUMMARY COMPENSATION TABLE (tab-separated):\n" + table_str
        return result[:MAX_TEXT_CHARS]

    full_text = soup.get_text(" ")
    full_text = re.sub(r"\s+", " ", full_text).strip()
    matches = list(re.finditer(r"summary compensation table", full_text, re.IGNORECASE))
    if matches:
        idx = matches[-1].start()
        start = max(0, idx - 300)
        return full_text[start: start + MAX_TEXT_CHARS]
    mid = len(full_text) // 2
    return full_text[mid: mid + MAX_TEXT_CHARS]


PROMPT = """You are analyzing a proxy statement (DEF 14A) for {ticker}. Filed: {filing_date}

The data below is a tab-separated rendering of the Summary Compensation Table.
Each row is a table row. Each tab separates a cell.

Your job: extract ONLY the row(s) where the person's title is "Chief Executive Officer", "CEO", or "Principal Executive Officer".

STRICT RULES:
1. CEO NAME: Must be a real person's name (First Last). NEVER return a title like "Chief Executive Officer" or "Former" or "President" as the name. Strip any footnote numbers like "(2)" or "(7)" from names.
2. FISCAL YEAR: Use the year number from the Year column exactly as shown.
3. TOTAL COMPENSATION: Use the TOTAL column (rightmost dollar column). It is in dollars NOT thousands. Typical range: $500,000 to $200,000,000. If a value looks absurdly large (e.g. $3,099,277,395) it is a parsing error — correct it using context.
4. If two CEOs appear due to a mid-year transition, return both as separate objects.
5. If you cannot find a CEO row with a real person's name, return [].
6. Strip all footnote markers from names before returning.

Return ONLY a valid JSON array, no other text:
[
  {{"name": "Full Name", "title": "Chief Executive Officer", "fiscal_year": 2024, "total_comp": 15234567.00, "partial_year": false, "notes": ""}}
]

Proxy table data:
{proxy_text}"""


def call_claude(client, ticker, filing_date, proxy_text):
    time.sleep(API_DELAY)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": PROMPT.format(
                ticker=ticker, filing_date=filing_date, proxy_text=proxy_text
            )}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        try:
            result = json.loads(raw)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            result = json.loads(match.group())
            if isinstance(result, list):
                return result
        print(f"    Could not parse: {raw[:150]}")
        return []
    except Exception as e:
        print(f"    API error: {e}")
        return []


def run(ticker_filter=None, force=False):
    """ticker_filter: list of tickers, or None for all."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set — check your .env file")
        raise SystemExit(1)

    client   = anthropic.Anthropic(api_key=api_key)
    manifest = load_manifest(ticker_filter)
    done     = set() if force else load_progress()
    pending  = [f for f in manifest
                if force or f"{f['ticker']}|{f['comp_year']}|{f['accession']}" not in done]

    print(f"Pending : {len(pending)}")
    print(f"Skipping: {len(manifest) - len(pending)} already done\n")

    fields = ["ticker", "comp_year", "filing_date", "accession",
              "ceo_name", "title", "total_comp", "partial_year", "notes"]

    write_mode   = "w" if (force or not Path(RESULTS_CSV).exists()) else "a"
    write_header = write_mode == "w"
    total_rows   = 0
    total_failed = 0

    with open(RESULTS_CSV, write_mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()

        for i, filing in enumerate(pending, 1):
            ticker    = filing["ticker"]
            comp_year = filing["comp_year"]
            print(f"[{i:4d}/{len(pending)}] {ticker:<6}  {comp_year}", end="  ", flush=True)

            try:
                html = Path(filing["local_path"]).read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                print(f"read error: {e}")
                continue

            extracted = call_claude(client, ticker, filing["filing_date"], extract_text(html))
            rows_written = 0

            for row in extracted:
                if row.get("total_comp") is None:
                    continue
                writer.writerow({
                    "ticker":       ticker,
                    "comp_year":    row.get("fiscal_year", comp_year),
                    "filing_date":  filing["filing_date"],
                    "accession":    filing["accession"],
                    "ceo_name":     row.get("name", ""),
                    "title":        row.get("title", ""),
                    "total_comp":   row.get("total_comp", ""),
                    "partial_year": row.get("partial_year", False),
                    "notes":        row.get("notes", ""),
                })
                rows_written += 1

            f.flush()
            mark_progress(ticker, comp_year, filing["accession"])

            if rows_written:
                print(f"{rows_written} row(s)")
                total_rows += rows_written
            else:
                print("no CEO found")
                total_failed += 1

    print(f"\n{'─'*50}")
    print(f"Extracted : {total_rows} rows")
    print(f"No CEO    : {total_failed} filings")
    print(f"Results   : {RESULTS_CSV}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", help="Comma-separated tickers e.g. AAPL,MSFT")
    parser.add_argument("--force", action="store_true", help="Reprocess everything")
    args = parser.parse_args()
    tickers = (
        [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if args.tickers else None
    )
    run(ticker_filter=tickers, force=args.force)
