#!/usr/bin/env python3
"""
lookup_proxy_ceo.py — Find CEO name and start date from the most recent proxy filing
=====================================================================================
For new tickers with no cached query context, this script:
  1. Looks up the company's CIK from SEC
  2. Finds the most recent DEF 14A (proxy statement) on EDGAR
  3. Downloads it and asks Claude: "Who is the CEO and when did they start?"
  4. Writes the result to a local context CSV for the rest of the pipeline

Run automatically by ceorater.py for new tickers before download_8k.py.
The date discovered here lets download_8k.py search in the right ±180-day window
and extract_8k.py use the right name as a reference.

Usage:
    venv/bin/python3 lookup_proxy_ceo.py --tickers SNOW,UBER --context-csv query_context.csv
"""

import argparse
import json
import re
import time
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

from edgar_client import (
    build_session, fetch, get_cik_map, find_latest_proxy,
    SEC_SUBMISSIONS_URL, QUARTERLY_INDEX_URL,
)
from db import read_db, write_db, DB_FIELDS

# ── Config ────────────────────────────────────────────────────────────────────

MODEL          = "claude-sonnet-4-6"
MAX_TOKENS     = 512
API_DELAY      = 3.0
MAX_TEXT_CHARS = 15000
DEFAULT_CONTEXT_CSV = "./query_context.csv"


# ── EDGAR: find IPO date ─────────────────────────────────────────────────────

QUARTERLY_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx"


def _search_quarterly_index(session: requests.Session, cik: str,
                             year: int, quarter: int) -> str | None:
    """Search one quarterly full-index file for the earliest 424B4 by CIK.

    The company.idx line format is (roughly):
      Company Name (62 chars) | Form Type (17 chars) | CIK (9 chars) | Date (10 chars) | Filename
    Uses regex to avoid fixed-column fragility.
    """
    url = QUARTERLY_INDEX_URL.format(year=year, q=quarter)
    try:
        resp = fetch(session, url)
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    cik_int = str(int(cik))
    # Match lines where form=424B4, exact CIK, then filing date
    pattern = re.compile(
        r'424B4\s+' + re.escape(cik_int) + r'\s+(\d{4}-\d{2}-\d{2})'
    )
    earliest: str | None = None
    for line in resp.text.splitlines():
        m = pattern.search(line)
        if m:
            date = m.group(1)
            if earliest is None or date < earliest:
                earliest = date
    return earliest


def get_ipo_date(session: requests.Session, cik: str, search_from_year: int = 1993) -> str | None:
    """Return the IPO date (YYYY-MM-DD) from the 424B4 final prospectus filing.

    Strategy:
    1. Check the EDGAR submissions API (covers recent 1-2 years for active filers)
    2. If not found, scan EDGAR quarterly full-index files year by year from
       search_from_year until a 424B4 is found (handles pre-2024 IPOs)
    """
    from datetime import date as date_cls

    cik_padded = cik.zfill(10)
    earliest_424: str | None = None

    # Step 1: submissions API
    try:
        data = fetch(session, SEC_SUBMISSIONS_URL.format(cik_padded=cik_padded)).json()

        def scan(page: dict) -> None:
            nonlocal earliest_424
            for form, date in zip(page.get("form", []), page.get("filingDate", [])):
                if form == "424B4":
                    if earliest_424 is None or date < earliest_424:
                        earliest_424 = date

        scan(data.get("filings", {}).get("recent", {}))
        for file_entry in data.get("files", []):
            try:
                older = fetch(session, f"https://data.sec.gov/submissions/{file_entry['name']}").json()
                scan(older)
            except Exception:
                pass
    except Exception:
        pass

    if earliest_424:
        return earliest_424

    # Step 2: scan quarterly full-index files from search_from_year forward
    current_year = date_cls.today().year
    for year in range(search_from_year, current_year + 1):
        for quarter in range(1, 5):
            result = _search_quarterly_index(session, cik, year, quarter)
            if result:
                return result

    return None


# ── Text extraction ───────────────────────────────────────────────────────────

def extract_proxy_text(html: str, company: str = "") -> str:
    """Pull the most relevant text from a proxy — CEO bio section preferred."""
    soup = BeautifulSoup(html, "lxml")
    lines = []
    for raw in soup.get_text("\n").splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line)

    title_re = re.compile(
        r"\b("
        r"chairman and chief executive officer|"
        r"chairman and ceo|"
        r"chief executive officer and director|"
        r"chief executive officer|"
        r"chief executive|"
        r"president and chief executive officer|"
        r"chairman and chief executive"
        r")\b",
        re.I,
    )
    year_range_re = re.compile(r"\(\s*(\d{4})\s*-\s*(present|\d{4})\s*\)", re.I)
    director_since_re = re.compile(r"director\s+since\s*:?\s*(\d{4}|new nominee)", re.I)
    explicit_since_re = re.compile(r"\bsince\s+(?:[A-Za-z]+\s+)?\d{4}\b", re.I)
    person_re = re.compile(r"^[A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){1,3}$")
    generic_re = re.compile(r"\bour\s+ceo\b|named executive|shareholder|compensation", re.I)
    company_re = None
    company_key = re.sub(r"[^a-z0-9]+", " ", company.lower()).strip()
    if company_key:
        company_re = re.compile(re.escape(company_key), re.I)

    candidates: list[tuple[int, int, str]] = []
    for idx, line in enumerate(lines):
        if not title_re.search(line):
            continue

        start = max(0, idx - 10)
        end = min(len(lines), idx + 9)
        window = lines[start:end]
        snippet = "\n".join(window)

        score = 0
        if year_range_re.search(line):
            score += 8
        if explicit_since_re.search(snippet):
            score += 6
        if director_since_re.search(snippet):
            score += 4
        if any(person_re.match(candidate) for candidate in window):
            score += 5
        if "chairman and chief executive officer" in line.lower():
            score += 3
        if company_re and company_re.search(re.sub(r"[^a-z0-9]+", " ", snippet.lower())):
            score += 7
        if generic_re.search(snippet):
            score -= 4
        if "letter from our chairman and chief executive officer" in snippet.lower():
            score -= 2

        if score > 0:
            candidates.append((score, idx, snippet))

    if candidates:
        ranked = sorted(candidates, key=lambda item: (-item[0], item[1]))
        top = []
        seen = set()
        limit = 1 if ranked[0][0] >= 12 else 2
        for _, _, snippet in ranked[:limit]:
            if snippet in seen:
                continue
            seen.add(snippet)
            top.append(snippet)
        combined = "\n\n".join(top)
        return combined[:MAX_TEXT_CHARS]

    full = re.sub(r"\s+", " ", soup.get_text(" ")).strip()
    return full[:MAX_TEXT_CHARS]


# ── Claude prompt ─────────────────────────────────────────────────────────────

PROMPT = """\
You are analyzing the most recent proxy statement (DEF 14A) for {company} ({ticker}), \
filed on {filing_date}.

Your task: identify the CURRENT Chief Executive Officer and when they first became CEO.

Look for any of these patterns in the text:
  - "has served as Chief Executive Officer since [date]"
  - "was appointed [Chief Executive Officer / CEO] on [date]"
  - "became Chief Executive Officer effective [date]"
  - "joined as CEO on [date]"
  - "Chairman and Chief Executive Officer (2021-present)"
  - "Chief Executive Officer and Director (2014-2021)"
  - "Director Since: 2014"
  - Summary Compensation Table — CEO is typically the first person listed
  - Executive Officers section listing names and start dates
  - Biographical sections describing tenure

RULES:
1. Return the CURRENT CEO's full name exactly as written (no titles, no footnote numbers).
2. For the start date, return the most specific date available:
     - Full date  → use start_date: "YYYY-MM-DD"
     - Month+year → use start_month: "YYYY-MM"
     - Year only  → use start_year: YYYY (integer)
3. Treat title-history bullets with year ranges as explicit evidence. Example:
   "Chief Executive Officer and Director (2014-2021)" means the CEO start year is 2014.
4. Do NOT guess beyond what is explicitly stated in the text.
5. If multiple CEOs are named (transition period), return the most recently appointed one.
6. If you cannot determine the CEO with confidence, set found=false.

Return ONLY valid JSON, no other text:
{{"found": true, "ceo_name": "Full Name", "start_date": "YYYY-MM-DD", "start_month": null, "start_year": null, "notes": ""}}

Proxy text:
{text}"""


# ── Claude call ───────────────────────────────────────────────────────────────

def call_claude(client: anthropic.Anthropic, ticker: str, company: str,
                filing_date: str, text: str) -> dict:
    time.sleep(API_DELAY)
    prompt = PROMPT.format(
        company=company, ticker=ticker, filing_date=filing_date, text=text
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        try:
            result = json.loads(raw)
            if isinstance(result, dict):
                return result
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            if isinstance(result, dict):
                return result
        return {"found": False, "notes": f"parse_error: {raw[:80]}"}
    except Exception as e:
        return {"found": False, "notes": f"api_error: {e}"}


# ── Date formatting ───────────────────────────────────────────────────────────

def result_to_date_str(result: dict) -> str:
    """Convert Claude's result to a M/D/YYYY string for the local context CSV."""
    if result.get("start_date"):
        try:
            from datetime import date
            d = date.fromisoformat(result["start_date"])
            return f"{d.month}/{d.day}/{d.year}"
        except ValueError:
            pass
    if result.get("start_month"):
        m = re.match(r"(\d{4})-(\d{2})", result["start_month"])
        if m:
            return f"{int(m.group(2))}/1/{m.group(1)}"
    if result.get("start_year"):
        return f"1/1/{result['start_year']}"
    return ""


# ── Main ─────────────────────────────────────────────────────────────────────

def run(tickers: list[str], db_path: str = DEFAULT_CONTEXT_CSV, force: bool = False) -> None:
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set")
        return

    client  = anthropic.Anthropic(api_key=api_key)
    session = build_session()

    # Only process tickers that have no CEO name in the local context file,
    # unless this is an explicit re-run.
    db_rows = read_db(db_path)
    db_map  = {r["Ticker"].upper(): r for r in db_rows}

    need_lookup = []
    for t in tickers:
        row = db_map.get(t.upper(), {})
        if force or not row.get("CEO"):
            need_lookup.append(t.upper())

    if not need_lookup:
        print("  All tickers already have CEO data — skipping proxy lookup. Use --force to re-run.")
        return

    print(f"\nStep 0 — Proxy CEO lookup for {len(need_lookup)} new ticker(s)...")
    print("─" * 55)

    print("  Fetching CIK map from SEC...")
    cik_map = get_cik_map(session)

    updated = 0
    for ticker in need_lookup:
        cik = cik_map.get(ticker)
        if not cik:
            print(f"  {ticker:<6}  NO CIK — skipping")
            continue

        proxy_info = find_latest_proxy(session, cik)
        if not proxy_info:
            print(f"  {ticker:<6}  No DEF 14A found — skipping")
            continue

        print(f"  {ticker:<6}  Downloading proxy ({proxy_info['filing_date']})...", end=" ", flush=True)
        try:
            resp = fetch(session, proxy_info["url"])
            html = resp.text
        except Exception as e:
            print(f"download error: {e}")
            continue

        company = proxy_info["company_name"]
        text    = extract_proxy_text(html, company)
        result  = call_claude(client, ticker, company, proxy_info["filing_date"], text)

        if not result.get("found"):
            print(f"not found  ({result.get('notes', '')})")
            continue

        ceo_name  = result.get("ceo_name", "").strip()

        # Year-only result: check if CEO started before the IPO (founder case)
        # If so, use the IPO date instead of a placeholder 1/1/YYYY
        if result.get("start_year") and not result.get("start_date") and not result.get("start_month"):
            start_year = int(result["start_year"])
            ipo_date   = get_ipo_date(session, cik, search_from_year=start_year + 1)
            if ipo_date and int(ipo_date[:4]) > start_year:
                # Founder CEO — started before IPO, use IPO date
                d        = ipo_date  # YYYY-MM-DD
                date_str = f"{int(d[5:7])}/{int(d[8:10])}/{d[:4]}"
                print(f"found  {ceo_name}  |  {date_str}  (IPO date — founder pre-{start_year})")
            else:
                date_str = result_to_date_str(result)
                print(f"found  {ceo_name}  |  {date_str}  (year only)")
        else:
            date_str  = result_to_date_str(result)
            date_type = ("full date" if result.get("start_date")
                         else "month+year" if result.get("start_month")
                         else "no date")
            print(f"found  {ceo_name}  |  {date_str}  ({date_type})")

        # Write back to local context cache
        row = db_map.get(ticker, {"Ticker": ticker, "Company Name": company,
                                  "CEO": "", "CEO Start Date": ""})
        row["CEO"]            = ceo_name
        row["Company Name"]   = row.get("Company Name") or company
        if date_str:
            row["CEO Start Date"] = date_str
        db_map[ticker] = row
        updated += 1

    if updated:
        # Merge updated rows back into the full db_rows list
        for i, row in enumerate(db_rows):
            t = row["Ticker"].upper()
            if t in db_map:
                db_rows[i] = db_map[t]
        # Add any tickers that weren't in db_rows at all
        existing = {r["Ticker"].upper() for r in db_rows}
        for t, row in db_map.items():
            if t not in existing:
                db_rows.append(row)
        write_db(db_path, db_rows)
        print(f"\n  Updated {updated} ticker(s) in local context cache.")
    else:
        print("\n  No new data found from proxies.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Look up CEO name and start date from the most recent proxy filing"
    )
    parser.add_argument("--tickers",
                        help="Comma-separated tickers, e.g. SNOW,UBER")
    parser.add_argument("--force", action="store_true",
                        help="Re-run proxy lookup even if the ticker already has cached CEO data")
    args = parser.parse_args()

    raw = args.tickers
    interactive = False
    if not raw:
        interactive = True
        raw = input("Enter ticker(s), comma-separated: ").strip()
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tickers:
        raise SystemExit("No tickers provided.")
    run(tickers, force=(args.force or interactive))
