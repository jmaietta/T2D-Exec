#!/usr/bin/env python3
"""
extract_8k.py - Extract CEO Start Dates from 8-K Filings via Claude
=====================================================================
Reads the targeted 8-K files downloaded by download_8k.py and asks
Claude to extract the exact CEO appointment date (day/month/year).

The proxy/context hint date is shown in the output for comparison only — Claude
never sees it, so its extraction is fully independent.

Usage:
    source venv/bin/activate
    python3 extract_8k.py --tickers AAPL,MSFT,NVDA
    python3 extract_8k.py --tickers AAPL              # single ticker
    python3 extract_8k.py --tickers AAPL --force      # reprocess
"""

import argparse
import csv
import json
import os
import re
import time
from datetime import date as date_cls, timedelta
from pathlib import Path
from urllib.parse import urlencode

import anthropic
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import requests

from edgar_client import build_session, fetch, get_cik_map, SEC_ARCHIVES_BASE, EFTS_SEARCH_URL
from db import parse_date

# ── Config ────────────────────────────────────────────────────────────────────

FILINGS_8K_DIR    = "./filings_8k"
EFTS_PROGRESS_CSV = "./efts_progress.csv"

# Foreign/non-US filers — no SEC 8-K filings, skip Phase 3
NO_8K_TICKERS = frozenset([
    "ARM", "ASML", "AZN", "BF.B", "CCEP", "DAY", "ERIE",
    "GFS", "PDD", "SHOP", "TRI", "BRK.B",
])

# ── Pipeline constants ─────────────────────────────────────────────────────────

MANIFEST_CSV        = "./8k_manifest.csv"
PROXY_MANIFEST_CSV  = "./manifest.csv"       # DEF 14A manifest from download.py
PROXY_FILINGS_DIR   = "./filings"            # DEF 14A HTML files from download.py
CONTEXT_CSV         = "./query_context.csv"
RESULTS_CSV         = "./8k_results.csv"
PROGRESS_CSV        = "./8k_progress.csv"
PROXY_PROGRESS_CSV  = "./proxy_progress.csv"
MODEL               = "claude-sonnet-4-6"
MAX_TOKENS          = 512
API_DELAY           = 3.0
MAX_TEXT_CHARS      = 6000

# Locate the CEO appointment section in 8-K text
ITEM_502_RE = re.compile(r"item\s+5\.02", re.I)
CEO_TITLE_RE = re.compile(
    r"chief\s+executive\s+officer|chief\s+executive|\bCEO\b|principal\s+executive\s+officer",
    re.I,
)

PROMPT = """\
You are analyzing an 8-K filing for {company} ({ticker}), filed on {filing_date}.

This filing was retrieved because it contains Item 5.02 (Departure or Appointment \
of Certain Officers) and was filed around the time {ceo_name} became CEO.

Your task: determine whether this filing ANNOUNCES THE APPOINTMENT of {ceo_name} \
as a top executive, and if so, extract the EXACT effective date.

The executive title may appear as any of:
  - Chief Executive Officer
  - Chief Executive
  - CEO
  - Principal Executive Officer

RULES:
1. "found" must be true ONLY if this filing explicitly announces, reports, or \
confirms the appointment/election of {ceo_name} to a CEO/PEO role. Use language \
like "appointed", "elected", "named", "will serve as", "effective [date]".
2. If {ceo_name} is merely REFERENCED as the current CEO in the context of \
another announcement (e.g. a colleague's departure, a restructuring, or \
a compensation arrangement for someone else), return found=false.
3. Extract the EFFECTIVE date of appointment — the date they actually took the role.
   This may differ from the filing date (e.g. "effective March 15" in a March 18 filing).
4. Return date as YYYY-MM-DD. If only month+year given, use day 01.
5. If this filing announces a DIFFERENT person as CEO/PEO (not {ceo_name}), \
set found=false and describe in notes.
6. If no CEO/PEO appointment is mentioned, set found=false.
7. Do not guess — only extract what is explicitly stated.
8. If the appointment is described as "Interim", "Acting", or "temporary", set "interim": true.

Return ONLY valid JSON, no other text:
{{"found": true, "ceo_name": "Full Name", "effective_date": "YYYY-MM-DD", "interim": false, "notes": ""}}

8-K filing text:
{text}"""


PROMPT_NO_NAME = """\
You are analyzing an 8-K filing for {company} ({ticker}), filed on {filing_date}.

This filing contains Item 5.02 (Departure or Appointment of Certain Officers).

Your task: determine whether this filing ANNOUNCES THE APPOINTMENT of a new \
Chief Executive Officer (or equivalent), and if so, extract their name and the \
exact effective date.

The executive title may appear as any of:
  - Chief Executive Officer
  - Chief Executive
  - CEO
  - Principal Executive Officer
  - President and Chief Executive Officer

RULES:
1. "found" must be true ONLY if someone is being APPOINTED (not departing, \
resigning, or retiring) to a CEO/PEO role.
2. Do NOT set found=true for a CFO, COO, CLO, or other non-CEO appointment.
3. Extract the EFFECTIVE date — the date they actually take the role, \
not the filing date. If only month+year given, use day 01.
4. Return date as YYYY-MM-DD.
5. If no CEO appointment is mentioned, set found=false.
6. Do not guess — only extract what is explicitly stated.
7. If the appointment is described as "Interim", "Acting", or "temporary", set "interim": true.

Return ONLY valid JSON, no other text:
{{"found": true, "ceo_name": "Full Name", "effective_date": "YYYY-MM-DD", "interim": false, "notes": ""}}

8-K filing text:
{text}"""


PROXY_PROMPT = """\
You are analyzing a proxy statement (DEF 14A) for {company} ({ticker}).

We need to know exactly when {ceo_name} first became Chief Executive Officer \
(or equivalent: CEO, President & CEO, Principal Executive Officer).

Search the text for biographical or compensation narrative such as:
  - "has served as CEO/Chief Executive Officer since [date]"
  - "was appointed/elected/named CEO on [date]"
  - "became Chief Executive Officer effective [date]"
  - "joined as CEO on [date]"
  - "was named President and CEO on [date]"
  - "Chairman and Chief Executive Officer (2021-present)"
  - "Chief Executive Officer and Director (2014-2021)"
  - "Director Since: 2014"

RULES:
1. Only extract a date for {ceo_name} specifically — not a predecessor.
2. If a specific day+month+year is given, use it exactly.
3. If only a month+year is given (e.g. "since January 2014"), return YYYY-MM-01.
4. If only a year is given (e.g. "since 2014"), return YYYY-01-01.
5. Treat title-history bullets with year ranges as explicit evidence. Example:
   "Chief Executive Officer and Director (2014-2021)" means the CEO start year is 2014.
6. Do not guess beyond what is explicitly stated.
7. If you cannot find when {ceo_name} became CEO, return found=false.

Return ONLY valid JSON, no other text:
{{"found": true/false, "ceo_name": "Full Name or null", \
"effective_date": "YYYY-MM-DD or null", "notes": ""}}

Proxy text:
{text}"""


# ── HTTP session (Phase 3 EFTS) ───────────────────────────────────────────────


def efts_search_ceo(
    session: requests.Session,
    ceo_name: str,
    cik: str,
    center: date_cls,
    window_days: int = 365,
) -> list[dict]:
    """Search EFTS for 8-Ks that mention this CEO in an appointment context.

    Strategy 1 (precise):  "First Last" "Chief Executive"
    Strategy 2 (fallback): "Last" "Chief Executive Officer"

    Filters results to this company via the ciks list (padded CIK).
    Returns list of {accession, filing_date, doc_url} sorted closest-to-center first.
    """
    tokens = [t for t in re.split(r"[\s.,]+", ceo_name.strip())
              if len(t) > 1 and t.lower() not in SUFFIXES]
    if not tokens:
        return []

    last, first = tokens[-1], tokens[0]
    cik_padded = cik.zfill(10)
    start_dt = (center - timedelta(days=window_days)).isoformat()
    end_dt   = (center + timedelta(days=window_days)).isoformat()

    queries = []
    if len(tokens) >= 2:
        queries.append(f'"{first} {last}" "Chief Executive"')
    queries.append(f'"{last}" "Chief Executive Officer"')

    seen: set[str] = set()
    results: list[dict] = []

    for query in queries:
        params = {"q": query, "forms": "8-K",
                  "dateRange": "custom", "startdt": start_dt, "enddt": end_dt}
        try:
            hits = (fetch(session, EFTS_SEARCH_URL + "?" + urlencode(params))
                    .json().get("hits", {}).get("hits", []))
        except Exception as e:
            print(f"    EFTS error: {e}")
            continue

        for hit in hits:
            src = hit.get("_source", {})
            if cik_padded not in src.get("ciks", []):
                continue
            acc = src.get("adsh", "")
            if not acc or acc in seen:
                continue
            seen.add(acc)
            # _id is "accession:filename" — extract doc URL directly
            hit_id  = hit.get("_id", "")
            doc_url = None
            if ":" in hit_id:
                _, filename = hit_id.split(":", 1)
                acc_nodash = acc.replace("-", "")
                doc_url = (f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}"
                           f"/{acc_nodash}/{filename}")
            results.append({
                "accession":   acc,
                "filing_date": src.get("file_date", ""),
                "doc_url":     doc_url,
            })

        if results:
            break   # first strategy with company-specific hits is sufficient

    results.sort(key=lambda r: abs((date_cls.fromisoformat(r["filing_date"]) - center).days)
                               if r.get("filing_date") else 9999)
    return results


def get_primary_doc_url(
    session: requests.Session, cik: str, accession: str
) -> str | None:
    """Parse the filing's index.htm to return the primary .htm document URL."""
    acc_nodash = accession.replace("-", "")
    index_url  = (
        f"{SEC_ARCHIVES_BASE}/edgar/data/{cik}/{acc_nodash}/{accession}-index.htm"
    )
    try:
        soup  = BeautifulSoup(fetch(session, index_url).text, "lxml")
        table = (soup.find("table", summary="Document Format Files")
                 or soup.find("table", class_="tableFile"))
        if not table:
            return None
        for tr in table.find_all("tr")[1:]:
            tds  = tr.find_all("td")
            if len(tds) < 3:
                continue
            link = tds[2].find("a")
            if not link:
                continue
            href = link.get("href", "")
            if href.lower().endswith((".htm", ".html")):
                return "https://www.sec.gov" + href if href.startswith("/") else href
    except Exception:
        pass
    return None


def load_efts_progress() -> set[str]:
    done: set[str] = set()
    if Path(EFTS_PROGRESS_CSV).exists():
        with open(EFTS_PROGRESS_CSV, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                done.add(f"{row['ticker']}|{row['accession']}")
    return done


def mark_efts_progress(ticker: str, accession: str) -> None:
    exists = Path(EFTS_PROGRESS_CSV).exists()
    with open(EFTS_PROGRESS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "accession"])
        if not exists:
            writer.writeheader()
        writer.writerow({"ticker": ticker, "accession": accession})


# ── Text extraction ───────────────────────────────────────────────────────────

def extract_8k_text(html: str) -> str:
    """Pull the CEO appointment section from 8-K HTML.

    Priority:
      1. Text around Item 5.02 heading
      2. Text around first CEO title mention
      3. First MAX_TEXT_CHARS of document
    """
    soup = BeautifulSoup(html, "lxml")
    text = re.sub(r"\s+", " ", soup.get_text(" ")).strip()

    for pattern in [ITEM_502_RE, CEO_TITLE_RE]:
        m = re.search(pattern, text) if isinstance(pattern, re.Pattern) else re.search(pattern, text, re.I)
        if m:
            start = max(0, m.start() - 200)
            return text[start: start + MAX_TEXT_CHARS]

    return text[:MAX_TEXT_CHARS]


def extract_proxy_text(html: str, ceo_name: str) -> str:
    """Pull the CEO biography / tenure section from a DEF 14A proxy HTML.

    Looks for the last name near 'since', 'appointed', 'became', etc.
    Falls back to first mention of the name if no tenure phrase found.
    """
    soup = BeautifulSoup(html, "lxml")
    text = re.sub(r"\s+", " ", soup.get_text(" ")).strip()

    # Build last-name key (strip suffixes)
    name_parts = re.split(r"[\s.,]+", ceo_name.lower().strip())
    name_parts = [p for p in name_parts if len(p) > 1 and p not in SUFFIXES]
    last_name = name_parts[-1] if name_parts else ""

    if last_name:
        tenure_words = r"(?:since|appointed|elected|named|became|effective|joined)"
        patterns = [
            rf"{re.escape(last_name)}.{{0,200}}{tenure_words}",
            rf"{tenure_words}.{{0,200}}{re.escape(last_name)}",
            rf"chief\s+executive\s+officer.{{0,300}}since",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.I | re.DOTALL)
            if m:
                start = max(0, m.start() - 300)
                return text[start: start + MAX_TEXT_CHARS]

        # Fallback: just find the name
        m = re.search(re.escape(last_name), text, re.I)
        if m:
            start = max(0, m.start() - 200)
            return text[start: start + MAX_TEXT_CHARS]

    return text[:MAX_TEXT_CHARS]


def load_context(path: str = CONTEXT_CSV) -> dict[str, dict]:
    """Return {TICKER: {company_name, ceo_name, start_date}} from the local context CSV."""
    result: dict[str, dict] = {}
    if not Path(path).exists():
        return result
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            t = (row.get("Ticker") or "").strip().upper()
            if t:
                result[t] = {
                    "company_name":  (row.get("Company Name") or "").strip(),
                    "ceo_name":      (row.get("CEO") or "").strip(),
                    "start_date":    (row.get("CEO Start Date") or "").strip(),
                }
    return result


def load_proxy_manifest() -> dict[str, list[dict]]:
    """Return {TICKER: [row, ...]} from manifest.csv (DEF 14A files), newest first."""
    result: dict[str, list[dict]] = {}
    if not Path(PROXY_MANIFEST_CSV).exists():
        return result
    with open(PROXY_MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("status") not in ("downloaded", "exists"):
                continue
            if not Path(row.get("local_path", "")).exists():
                continue
            t = row["ticker"].upper()
            result.setdefault(t, []).append(row)
    # Sort newest proxy first per ticker
    for t in result:
        result[t].sort(key=lambda r: r.get("filing_date", ""), reverse=True)
    return result


# ── Progress tracking ─────────────────────────────────────────────────────────

def load_progress() -> set[str]:
    done: set[str] = set()
    if Path(PROGRESS_CSV).exists():
        with open(PROGRESS_CSV, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                done.add(f"{row['ticker']}|{row['accession']}")
    return done


def mark_progress(ticker: str, accession: str) -> None:
    exists = Path(PROGRESS_CSV).exists()
    with open(PROGRESS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "accession"])
        if not exists:
            writer.writeheader()
        writer.writerow({"ticker": ticker, "accession": accession})


def load_proxy_progress() -> set[str]:
    done: set[str] = set()
    if Path(PROXY_PROGRESS_CSV).exists():
        with open(PROXY_PROGRESS_CSV, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                done.add(f"{row['ticker']}|{row['accession']}")
    return done


def mark_proxy_progress(ticker: str, accession: str) -> None:
    exists = Path(PROXY_PROGRESS_CSV).exists()
    with open(PROXY_PROGRESS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "accession"])
        if not exists:
            writer.writeheader()
        writer.writerow({"ticker": ticker, "accession": accession})


# ── Manifest ──────────────────────────────────────────────────────────────────

def load_manifest(tickers: list[str]) -> dict[str, list[dict]]:
    """Load 8k_manifest.csv rows for the requested tickers, grouped by ticker."""
    if not Path(MANIFEST_CSV).exists():
        print(f"ERROR: {MANIFEST_CSV} not found. Run download_8k.py first.")
        raise SystemExit(1)

    want = {t.upper() for t in tickers}
    by_ticker: dict[str, list[dict]] = {}

    with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            t = row["ticker"].upper()
            if t not in want:
                continue
            if row["status"] not in ("downloaded", "exists"):
                continue
            if not Path(row["local_path"]).exists():
                continue
            by_ticker.setdefault(t, []).append(row)

    # Oldest → newest per ticker so Claude sees them in chronological order
    for t in by_ticker:
        by_ticker[t].sort(key=lambda r: r["filing_date"])

    return by_ticker


# ── Claude call ───────────────────────────────────────────────────────────────

def call_claude(
    client: anthropic.Anthropic,
    ticker: str,
    company: str,
    ceo_name: str,
    filing_date: str,
    text: str,
    proxy_mode: bool = False,
) -> dict:
    time.sleep(API_DELAY)
    if proxy_mode:
        prompt = PROXY_PROMPT.format(
            company=company,
            ticker=ticker,
            ceo_name=ceo_name,
            text=text,
        )
    elif not ceo_name:
        prompt = PROMPT_NO_NAME.format(
            company=company,
            ticker=ticker,
            filing_date=filing_date,
            text=text,
        )
    else:
        prompt = PROMPT.format(
            company=company,
            ticker=ticker,
            filing_date=filing_date,
            ceo_name=ceo_name,
            text=text,
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

        return {"found": False, "ceo_name": None, "effective_date": None,
                "notes": f"parse_error: {raw[:80]}"}

    except Exception as e:
        return {"found": False, "ceo_name": None, "effective_date": None,
                "notes": f"api_error: {e}"}


def has_api_error(result: dict) -> bool:
    """Return True when Claude failed before producing a usable response."""
    return "api_error:" in (result.get("notes") or "")


# ── Date comparison ───────────────────────────────────────────────────────────


def compare_dates(proxy_str: str | None, sec_str: str | None) -> tuple[str, str]:
    """Return (match_status, days_diff_str)."""
    d_sec = parse_date(sec_str)
    d_at  = parse_date(proxy_str)

    if d_sec is None:
        return "not_found", ""
    if d_at is None:
        return "no_proxy_date", ""

    diff = (d_sec - d_at).days

    if d_sec == d_at:
        status = "exact_match"
    elif d_sec.year == d_at.year and d_sec.month == d_at.month:
        status = "month_match"
    elif d_sec.year == d_at.year:
        status = "year_match"
    else:
        status = "mismatch"

    return status, str(diff)


# ── Main ──────────────────────────────────────────────────────────────────────

# ── Name matching ─────────────────────────────────────────────────────────────

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def names_match(proxy_name: str, sec_name: str) -> bool:
    """Fuzzy name match handling middle initials, nicknames, and last-name-only.

    Examples that match:
      'Brian R. Niccol'  vs 'Brian Niccol'         — strips middle initial
      'Jonathon Nudi'    vs 'Jon Nudi'              — prefix match on first name
      'William H. Rogers Jr.' vs 'Rogers'           — last-name-only fallback
      'J. Kent Masters, Jr.' vs 'Kent Masters'      — comma + suffix stripped
      'Philip B. Daniele, III' vs 'Philip Daniele'  — comma + suffix stripped
    """
    if not proxy_name:
        return True  # no proxy hint name — accept anything

    def key_tokens(name: str) -> list[str]:
        tokens = re.split(r"[\s.,]+", name.lower().strip())
        # Drop single-char initials and common suffixes
        return [t for t in tokens if len(t) > 1 and t not in SUFFIXES]

    a_tokens = key_tokens(proxy_name)
    b_tokens = key_tokens(sec_name)

    if not a_tokens or not b_tokens:
        return False

    a_set = set(a_tokens)
    b_set = set(b_tokens)

    # Exact token-set subset match (shorter ⊆ longer)
    shorter = a_set if len(a_set) <= len(b_set) else b_set
    longer  = a_set if len(a_set) >  len(b_set) else b_set
    if shorter.issubset(longer):
        return True

    # Prefix match on first names (handles Jon / Jonathon, Bill / William)
    first_a = a_tokens[0]
    first_b = b_tokens[0]
    last_a  = a_tokens[-1]
    last_b  = b_tokens[-1]
    if (first_a.startswith(first_b) or first_b.startswith(first_a)) and last_a == last_b:
        return True

    # Last-name-only fallback (handles cases where SEC filing uses surname only)
    if last_a == last_b:
        return True

    return False


RESULT_FIELDS = [
    "ticker", "company_name",
    "ceo_name_proxy", "start_date_proxy",
    "ceo_name_8k", "start_date_8k", "filing_date_8k", "accession_8k",
    "match_status", "days_diff", "notes", "source",
]


def normalize_result_row(row: dict) -> dict:
    normalized = dict(row)
    if not normalized.get("ceo_name_proxy"):
        normalized["ceo_name_proxy"] = normalized.get("ceo_name_airtable", "")
    if not normalized.get("start_date_proxy"):
        normalized["start_date_proxy"] = normalized.get("start_date_airtable", "")
    normalized.setdefault("source", "8k")
    return {field: normalized.get(field, "") for field in RESULT_FIELDS}


def run(tickers: list[str], force: bool = False, context_csv: str = CONTEXT_CSV) -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set — check your .env file")
        raise SystemExit(1)

    client          = anthropic.Anthropic(api_key=api_key)
    by_ticker       = load_manifest(tickers)
    done            = set() if force else load_progress()
    proxy_done      = set() if force else load_proxy_progress()
    proxy_by_ticker = load_proxy_manifest()
    context_rows    = load_context(context_csv)

    # Note tickers with no 8-Ks — they'll fall back to proxy
    missing_8k = [t for t in tickers if t.upper() not in by_ticker]
    if missing_8k:
        print(f"Note: {len(missing_8k)} ticker(s) have no 8-K downloads "
              f"(will use proxy fallback if available)")

    # Load existing results so we can merge rather than overwrite when --force
    # is used on a subset of tickers.
    existing_results: dict[str, dict] = {}
    if Path(RESULTS_CSV).exists():
        with open(RESULTS_CSV, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                normalized = normalize_result_row(row)
                existing_results[normalized["ticker"]] = normalized

    retry_tickers = {
        ticker for ticker, row in existing_results.items()
        if "api_error:" in (row.get("notes") or "")
    }

    # Save a copy before popping so we can restore rows that end up all_skipped
    saved_results: dict[str, dict] = dict(existing_results)

    # Remove rows for tickers we're about to (re)process
    for t in [t.upper() for t in tickers]:
        existing_results.pop(t, None)

    new_results: list[dict] = []
    counts: dict[str, int] = {}

    STATUS_ICON = {
        "exact_match": "✓ EXACT", "month_match": "~ MONTH",
        "year_match": "≈ YEAR",   "mismatch": "✗ MISMATCH",
        "not_found": "? NOT FOUND", "no_proxy_date": "NEW",
    }

    for i, ticker in enumerate(sorted(by_ticker), 1):
        filings       = by_ticker[ticker]
        sample        = filings[0]
        company       = sample.get("company_name", "")
        ceo_proxy  = sample.get("ceo_name_proxy", "")
        date_proxy = sample.get("start_date_proxy", "")

        print(f"\n[{i}/{len(by_ticker)}] {ticker}  |  {ceo_proxy or '?'}  "
              f"|  {len(filings)} 8-K(s) to check")

        best: dict | None = None   # filing where Claude confirmed this CEO
        all_notes: list[str] = []
        all_skipped = True  # track whether every filing was skipped

        for filing in filings:
            accession   = filing["accession"]
            filing_date = filing["filing_date"]
            key         = f"{ticker}|{accession}"

            if key in done and ticker not in retry_tickers:
                print(f"  {filing_date}  [skipped — already processed]")
                continue

            all_skipped = False

            try:
                html = Path(filing["local_path"]).read_text(
                    encoding="utf-8", errors="replace"
                )
            except Exception as e:
                print(f"  {filing_date}  read error: {e}")
                mark_progress(ticker, accession)
                continue

            text   = extract_8k_text(html)
            result = call_claude(client, ticker, company, ceo_proxy,
                                 filing_date, text)
            if not has_api_error(result):
                mark_progress(ticker, accession)

            found          = result.get("found", False)
            ceo_8k         = (result.get("ceo_name") or "").strip()
            effective_date = (result.get("effective_date") or "").strip()
            notes          = (result.get("notes") or "").strip()

            icon = "✓" if found else "–"
            print(f"  {filing_date}  {icon}  "
                  f"name={ceo_8k or '-':30s}  "
                  f"date={effective_date or '-':12s}  "
                  f"{notes[:50] if notes else ''}")

            if notes:
                all_notes.append(f"{filing_date}: {notes}")

            if found and ceo_8k:
                is_interim = result.get("interim", False)
                if names_match(ceo_proxy, ceo_8k):
                    candidate = {
                        "ceo_name_8k":    ceo_8k,
                        "start_date_8k":  effective_date,
                        "filing_date_8k": filing_date,
                        "accession_8k":   accession,
                        "interim":        is_interim,
                    }
                    if best is None:
                        best = candidate
                    elif best.get("interim") and not is_interim:
                        best = candidate  # upgrade from interim to permanent
                    elif not best.get("interim") and not is_interim:
                        # Both permanent — prefer the one closest to the hint date
                        # If no reference date, prefer the most recent appointment
                        d_at   = parse_date(date_proxy)
                        d_best = parse_date(best["start_date_8k"])
                        d_cand = parse_date(candidate["start_date_8k"])
                        if d_at and d_best and d_cand:
                            if abs((d_cand - d_at).days) < abs((d_best - d_at).days):
                                best = candidate
                        elif not d_at and d_best and d_cand and d_cand > d_best:
                            best = candidate  # no ref date → prefer most recent

        # If every filing was already processed, restore the previous result row
        if all_skipped:
            prev = saved_results.get(ticker)
            if prev:
                cached_date = prev.get("start_date_8k") or "no date"
                print(f"  → [cached: {cached_date}]")
                if prev.get("match_status") == "not_found":
                    # Keep prior evidence, but still run proxy/EFTS fallback in case
                    # new proxy files exist or the fallback logic has improved.
                    new_results.append(prev)
                else:
                    existing_results[ticker] = prev
            else:
                print(f"  → [all filings already processed — no prior result to restore]")
            continue

        # Determine match status
        if best:
            match_status, days_diff = compare_dates(
                date_proxy, best["start_date_8k"]
            )
        else:
            match_status, days_diff = "not_found", ""
            best = {"ceo_name_8k": "", "start_date_8k": "",
                    "filing_date_8k": "", "accession_8k": ""}

        counts[match_status] = counts.get(match_status, 0) + 1

        print(f"  → {STATUS_ICON.get(match_status, match_status)}"
              f"  date={best['start_date_8k'] or '-'}"
              f"  diff={days_diff + 'd' if days_diff else 'n/a'}")

        new_results.append({
            "ticker":              ticker,
            "company_name":        company,
            "ceo_name_proxy":      ceo_proxy,
            "start_date_proxy":    date_proxy,
            "ceo_name_8k":         best["ceo_name_8k"],
            "start_date_8k":       best["start_date_8k"],
            "filing_date_8k":      best["filing_date_8k"],
            "accession_8k":        best["accession_8k"],
            "match_status":        match_status,
            "days_diff":           days_diff,
            "notes":               " | ".join(all_notes)[:500],
            "source":              "8k",
        })

    # ── Phase 2: Proxy fallback ───────────────────────────────────────────────
    # For every requested ticker that still has no result (not in manifest at
    # all, or processed above as not_found), attempt to extract the CEO start
    # date from the most recent DEF 14A proxy we already have on disk.

    results_so_far = {r["ticker"] for r in new_results} | set(existing_results)
    not_found_8k   = {r["ticker"] for r in new_results if r["match_status"] == "not_found"}
    needs_proxy    = sorted(
        (set(t.upper() for t in tickers) - results_so_far)   # never processed
        | not_found_8k                                         # processed but empty
    )

    proxy_counts: dict[str, int] = {}

    if needs_proxy:
        print(f"\n{'─'*55}")
        print(f"Phase 2 — Proxy fallback: {len(needs_proxy)} ticker(s)")

    for ticker in needs_proxy:
        proxy_filings = proxy_by_ticker.get(ticker, [])

        # Pull cached context — try multiple sources in priority order
        context_row = next(
            (r for r in new_results if r["ticker"] == ticker),
            existing_results.get(ticker, {}),
        )
        company    = context_row.get("company_name", "")
        ceo_proxy  = context_row.get("ceo_name_proxy", "")
        date_proxy = context_row.get("start_date_proxy", "")

        # 8-K manifest rows carry proxy-derived context too
        if (not company or not ceo_proxy) and ticker in by_ticker:
            s = by_ticker[ticker][0]
            company    = company or s.get("company_name", "")
            ceo_proxy  = ceo_proxy or s.get("ceo_name_proxy", "")
            date_proxy = date_proxy or s.get("start_date_proxy", "")

        # Final fallback: read directly from the local context CSV
        if (not company or not ceo_proxy) and ticker in context_rows:
            cached = context_rows[ticker]
            company    = company or cached["company_name"]
            ceo_proxy  = ceo_proxy or cached["ceo_name"]
            date_proxy = date_proxy or cached["start_date"]

        print(f"\n  [proxy] {ticker}  |  {ceo_proxy or '?'}")

        if not proxy_filings:
            print(f"    no proxy files on disk — skipping")
            # Write not_found if ticker had no result at all
            if ticker not in results_so_far:
                new_results.append({
                    "ticker": ticker, "company_name": company,
                    "ceo_name_proxy": ceo_proxy,
                    "start_date_proxy": date_proxy,
                    "ceo_name_8k": "", "start_date_8k": "",
                    "filing_date_8k": "", "accession_8k": "",
                    "match_status": "not_found", "days_diff": "",
                    "notes": "no 8-K in EDGAR window; no proxy file on disk",
                    "source": "none",
                })
                # Remove from not_found_8k set so we don't double-write below
                not_found_8k.discard(ticker)
            continue

        # Use the most recent proxy
        proxy_row  = proxy_filings[0]
        accession  = proxy_row["accession"]
        filing_date= proxy_row["filing_date"]
        local_path = proxy_row["local_path"]
        key        = f"{ticker}|{accession}"

        if key in proxy_done and not force and ticker not in retry_tickers:
            # Restore previous result from saved_results so it isn't lost
            prev = saved_results.get(ticker)
            if prev:
                print(f"    {filing_date}  [proxy skipped — already processed]")
                new_results = [r for r in new_results if r["ticker"] != ticker]
                new_results.append(prev)
                continue
            print(f"    {filing_date}  [proxy cache exists but no saved result — reprocessing]")

        try:
            html = Path(local_path).read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"    read error: {e}")
            mark_proxy_progress(ticker, accession)
            continue

        text   = extract_proxy_text(html, ceo_proxy)
        result = call_claude(client, ticker, company, ceo_proxy,
                             filing_date, text, proxy_mode=True)
        if not has_api_error(result):
            mark_proxy_progress(ticker, accession)

        found              = result.get("found", False)
        ceo_proxy_match    = (result.get("ceo_name") or "").strip()
        effective_date     = (result.get("effective_date") or "").strip()
        notes              = (result.get("notes") or "").strip()

        icon = "✓" if found else "–"
        print(f"    {filing_date} {icon}  "
              f"name={ceo_proxy_match or '-':30s}  "
              f"date={effective_date or '-':12s}  "
              f"{notes[:60] if notes else ''}")

        if found and ceo_proxy_match and names_match(ceo_proxy, ceo_proxy_match):
            match_status, days_diff = compare_dates(date_proxy, effective_date)
        else:
            match_status, days_diff = "not_found", ""
            effective_date = ""
            ceo_proxy_match = ""

        proxy_counts[match_status] = proxy_counts.get(match_status, 0) + 1

        STATUS_ICON = {
            "exact_match": "✓ EXACT", "month_match": "~ MONTH",
            "year_match": "≈ YEAR",   "mismatch": "✗ MISMATCH",
            "not_found": "? NOT FOUND", "no_proxy_date": "NEW",
        }
        print(f"    → {STATUS_ICON.get(match_status, match_status)}"
              f"  proxy={effective_date or '-'}")

        result_row = {
            "ticker":              ticker,
            "company_name":        company,
            "ceo_name_proxy":      ceo_proxy,
            "start_date_proxy":    date_proxy,
            "ceo_name_8k":         ceo_proxy_match,
            "start_date_8k":       effective_date,
            "filing_date_8k":      filing_date,
            "accession_8k":        accession,
            "match_status":        match_status,
            "days_diff":           days_diff,
            "notes":               notes[:500],
            "source":              "proxy",
        }

        # If this ticker was in new_results as not_found, replace it
        new_results = [r for r in new_results if r["ticker"] != ticker]
        new_results.append(result_row)

    _LABEL = {"exact_match": "confirmed", "month_match": "month match",
              "year_match": "year match", "mismatch": "mismatch",
              "not_found": "not found", "no_proxy_date": "new"}
    if proxy_counts:
        print(f"\nProxy fallback results:")
        for status, count in sorted(proxy_counts.items()):
            print(f"  {_LABEL.get(status, status):<22}: {count}")

    # Build mutable map for Phase 3 in-place updates
    all_results_map: dict[str, dict] = {
        r["ticker"]: r
        for r in list(existing_results.values()) + new_results
    }

    # ── Phase 3: EFTS full-text search ───────────────────────────────────────
    # For every REQUESTED ticker still not_found that isn't a known foreign filer,
    # search EFTS by CEO name + "Chief Executive" across all historical 8-Ks.
    want_set = {t.upper() for t in tickers}
    efts_needed = sorted(
        t for t, r in all_results_map.items()
        if r["match_status"] == "not_found"
        and t not in NO_8K_TICKERS
        and t in want_set
    )
    efts_counts: dict[str, int] = {}

    if efts_needed:
        print(f"\n{'─'*55}")
        print(f"Phase 3 — EFTS full-text search: {len(efts_needed)} ticker(s)")
        http_session = build_session()
        cik_map      = get_cik_map(http_session)
        efts_done    = set() if force else load_efts_progress()

        for ticker in efts_needed:
            row          = all_results_map[ticker]
            ceo_proxy  = row.get("ceo_name_proxy", "")
            date_proxy = row.get("start_date_proxy", "")
            company      = row.get("company_name", "")
            center       = parse_date(date_proxy)
            cik          = cik_map.get(ticker)

            if not cik or not center or not ceo_proxy:
                print(f"\n  [efts] {ticker}  no CIK/date/name — skipping")
                continue

            print(f"\n  [efts] {ticker}  |  {ceo_proxy}")
            filings = efts_search_ceo(http_session, ceo_proxy, cik, center)

            if not filings:
                print(f"    no EFTS results")
                continue
            print(f"    {len(filings)} filing(s) to check")

            best: dict | None = None
            all_notes: list[str] = []

            for filing in filings:
                acc, filing_date = filing["accession"], filing["filing_date"]
                key = f"{ticker}|{acc}"

                if key in efts_done and not force and ticker not in retry_tickers:
                    print(f"    {filing_date}  [skipped]")
                    continue

                doc_url = filing.get("doc_url") or get_primary_doc_url(http_session, cik, acc)
                if not doc_url:
                    print(f"    {filing_date}  can't resolve doc URL")
                    mark_efts_progress(ticker, acc)
                    continue

                ticker_dir = Path(FILINGS_8K_DIR) / ticker
                ticker_dir.mkdir(parents=True, exist_ok=True)
                filepath = (ticker_dir /
                            f"{filing_date}_8K_{acc.replace('-', '')}_efts.html")

                if not (filepath.exists() and filepath.stat().st_size > 500):
                    try:
                        filepath.write_text(
                            fetch(http_session, doc_url).text, encoding="utf-8"
                        )
                    except Exception as e:
                        print(f"    {filing_date}  download error: {e}")
                        mark_efts_progress(ticker, acc)
                        continue

                try:
                    html = filepath.read_text(encoding="utf-8", errors="replace")
                except Exception as e:
                    print(f"    {filing_date}  read error: {e}")
                    mark_efts_progress(ticker, acc)
                    continue

                text   = extract_8k_text(html)
                result = call_claude(client, ticker, company, ceo_proxy,
                                     filing_date, text)
                if not has_api_error(result):
                    mark_efts_progress(ticker, acc)

                found          = result.get("found", False)
                ceo_8k         = (result.get("ceo_name") or "").strip()
                effective_date = (result.get("effective_date") or "").strip()
                notes          = (result.get("notes") or "").strip()
                if notes:
                    all_notes.append(f"{filing_date}: {notes}")

                icon = "✓" if found else "–"
                print(f"    {filing_date}  {icon}  "
                      f"name={ceo_8k or '-':30s}  date={effective_date or '-':12s}  "
                      f"{notes[:50] if notes else ''}")

                if found and ceo_8k and names_match(ceo_proxy, ceo_8k):
                    candidate = {
                        "ceo_name_8k":    ceo_8k,
                        "start_date_8k":  effective_date,
                        "filing_date_8k": filing_date,
                        "accession_8k":   acc,
                        "interim":        result.get("interim", False),
                    }
                    if best is None:
                        best = candidate
                    elif best.get("interim") and not candidate.get("interim"):
                        best = candidate

            if best:
                match_status, days_diff = compare_dates(
                    date_proxy, best["start_date_8k"]
                )
                efts_counts[match_status] = efts_counts.get(match_status, 0) + 1
                all_results_map[ticker] = {
                    **row,
                    "ceo_name_8k":    best["ceo_name_8k"],
                    "start_date_8k":  best["start_date_8k"],
                    "filing_date_8k": best["filing_date_8k"],
                    "accession_8k":   best["accession_8k"],
                    "match_status":   match_status,
                    "days_diff":      days_diff,
                    "notes":          " | ".join(all_notes)[:500],
                    "source":         "efts",
                }
                _si = {"exact_match": "✓ EXACT", "month_match": "~ MONTH",
                       "year_match":  "≈ YEAR",  "mismatch":    "✗ MISMATCH"}
                print(f"    → {_si.get(match_status, match_status)}"
                      f"  efts={best['start_date_8k']}")

    if efts_counts:
        print(f"\nEFTS fallback results:")
        for status, count in sorted(efts_counts.items()):
            print(f"  {_LABEL.get(status, status):<22}: {count}")

    # ── Final write ───────────────────────────────────────────────────────────
    all_results = list(all_results_map.values())
    with open(RESULTS_CSV, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=RESULT_FIELDS)
        writer.writeheader()
        writer.writerows(normalize_result_row(row) for row in all_results)

    print(f"\n{'─'*55}")
    print(f"Results: {RESULTS_CSV}  ({len(all_results)} total rows)")
    print(f"\nNew/updated this run:")
    for status, count in sorted(counts.items()):
        print(f"  {_LABEL.get(status, status):<22}: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract CEO start dates from 8-Ks and compare to cached query hints"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--tickers",
        help="Comma-separated tickers (e.g. AAPL,MSFT,NVDA)"
    )
    group.add_argument(
        "--all", action="store_true",
        help="Process every ticker in 8k_manifest.csv"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Reprocess already-completed tickers"
    )
    parser.add_argument(
        "--context-csv", default=CONTEXT_CSV,
        help=f"Path to context CSV (default: {CONTEXT_CSV})"
    )
    args = parser.parse_args()

    context_csv = args.context_csv

    if args.all:
        all_tickers: list[str] = []
        if Path(context_csv).exists():
            with open(context_csv, encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    t = (row.get("Ticker") or "").strip().upper()
                    if t:
                        all_tickers.append(t)
        tickers = sorted(set(all_tickers))
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    run(tickers, force=args.force, context_csv=context_csv)
