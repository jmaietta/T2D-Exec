"""
edgar_client.py — Shared EDGAR HTTP client for T2D Exec pipelines
==================================================================
Provides rate-limited HTTP access to SEC EDGAR APIs and common
lookup functions used by all pipeline scripts.
"""

import json
import time
import csv
import re
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Constants ─────────────────────────────────────────────────────────────────

USER_AGENT    = "t2d-exec contact@ceorater.com"
REQUEST_DELAY = 0.15   # seconds between requests (~7 req/sec — SEC rate limit)
CACHE_DIR     = Path(__file__).parent / ".cache"
CIK_CACHE_PATH = CACHE_DIR / "company_tickers.json"
TICKER_CIK_CACHE_PATH = CACHE_DIR / "ticker_cik_cache.csv"
CIK_CACHE_TTL = 24 * 60 * 60  # 24 hours

SEC_TICKERS_URL     = "https://www.sec.gov/files/company_tickers.json"
SEC_BROWSE_COMPANY_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&owner=exclude&count=10&CIK={query}"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik_padded}.json"
SEC_SUBMISSIONS_OLD = "https://data.sec.gov/submissions/{filename}"
SEC_ARCHIVES_BASE   = "https://www.sec.gov/Archives"
EFTS_SEARCH_URL     = "https://efts.sec.gov/LATEST/search-index"
QUARTERLY_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx"

# ── Session ───────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


_last_req = 0.0


def fetch(session: requests.Session, url: str, timeout: int | tuple[int, int] = (5, 20)) -> requests.Response:
    """Rate-limited GET request respecting SEC's ~7 req/sec limit."""
    global _last_req
    elapsed = time.monotonic() - _last_req
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_req = time.monotonic()
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


# ── CIK lookup ────────────────────────────────────────────────────────────────

def _normalize_cik_data(data: dict) -> dict[str, str]:
    return {
        str(v["ticker"]).upper(): str(v["cik_str"])
        for v in data.values()
        if v.get("ticker") and v.get("cik_str")
    }


def _read_bulk_cik_cache() -> dict[str, str]:
    if not CIK_CACHE_PATH.exists():
        return {}
    data = json.loads(CIK_CACHE_PATH.read_text(encoding="utf-8"))
    return _normalize_cik_data(data)


def _read_ticker_cik_cache() -> dict[str, str]:
    if not TICKER_CIK_CACHE_PATH.exists():
        return {}
    cached: dict[str, str] = {}
    with open(TICKER_CIK_CACHE_PATH, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ticker = (row.get("ticker") or "").strip().upper()
            cik = (row.get("cik") or "").strip()
            if ticker and cik:
                cached[ticker] = cik
    return cached


def _write_ticker_cik_cache(cached: dict[str, str]) -> None:
    with open(TICKER_CIK_CACHE_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "cik"])
        writer.writeheader()
        for ticker in sorted(cached):
            writer.writerow({"ticker": ticker, "cik": cached[ticker]})


def _merge_ticker_cache(rows: dict[str, str]) -> None:
    if not rows:
        return
    CACHE_DIR.mkdir(exist_ok=True)
    cached = _read_ticker_cik_cache()
    changed = False
    for ticker, cik in rows.items():
        if ticker and cik and cached.get(ticker) != cik:
            cached[ticker] = cik
            changed = True
    if changed:
        _write_ticker_cik_cache(cached)


def lookup_cik_for_ticker(session: requests.Session, ticker: str) -> str | None:
    """Resolve one ticker directly via SEC's browse-company page."""
    query = (ticker or "").strip().upper()
    if not query:
        return None

    url = SEC_BROWSE_COMPANY_URL.format(query=quote(query))
    try:
        resp = fetch(session, url, timeout=(5, 10))
    except Exception:
        return None

    text = resp.text
    patterns = [
        r"<cik>\s*0*(\d+)\s*</cik>",
        r"\bCIK(?:=|%3D)0*(\d{1,10})\b",
        r"\bCIK#?:?\s*0*(\d{1,10})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            return str(int(match.group(1)))
        except ValueError:
            continue
    return None

def get_cik_map(session: requests.Session) -> dict[str, str]:
    """Return {TICKER: CIK} for all companies on SEC EDGAR."""
    CACHE_DIR.mkdir(exist_ok=True)

    if CIK_CACHE_PATH.exists():
        age = time.time() - CIK_CACHE_PATH.stat().st_mtime
        if age <= CIK_CACHE_TTL:
            print("Loading ticker→CIK map from local cache...")
            cik_map = _read_bulk_cik_cache()
            _merge_ticker_cache(cik_map)
            return cik_map

    print("Fetching ticker→CIK map from SEC...")
    try:
        data = fetch(session, SEC_TICKERS_URL).json()
        CIK_CACHE_PATH.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        if CIK_CACHE_PATH.exists():
            print("Using stale local ticker→CIK cache (SEC fetch failed)...")
            cik_map = _read_bulk_cik_cache()
            _merge_ticker_cache(cik_map)
            return cik_map
        else:
            raise

    cik_map = _normalize_cik_data(data)
    _merge_ticker_cache(cik_map)
    return cik_map


def get_ciks(session: requests.Session, tickers: list[str]) -> dict[str, str]:
    """Return {TICKER: CIK} for the requested tickers.

    Uses the small per-ticker cache first, then the local bulk cache, and only
    fetches SEC's full ticker file when the requested symbols are still missing.
    """
    want = [(t or "").strip().upper() for t in tickers if (t or "").strip()]
    if not want:
        return {}

    CACHE_DIR.mkdir(exist_ok=True)
    resolved: dict[str, str] = {}

    ticker_cache = _read_ticker_cik_cache()
    for ticker in want:
        if ticker in ticker_cache:
            resolved[ticker] = ticker_cache[ticker]

    missing = [ticker for ticker in want if ticker not in resolved]
    if not missing:
        print("Loading requested ticker→CIK mapping from local cache...")
        return resolved

    bulk_cache = _read_bulk_cik_cache()
    learned_from_bulk = {
        ticker: bulk_cache[ticker]
        for ticker in missing
        if ticker in bulk_cache
    }
    if learned_from_bulk:
        print("Loading requested ticker→CIK mapping from local SEC cache...")
        resolved.update(learned_from_bulk)
        _merge_ticker_cache(learned_from_bulk)

    missing = [ticker for ticker in want if ticker not in resolved]
    if not missing:
        return resolved

    print(f"Resolving {len(missing)} ticker(s) directly from SEC...")
    direct = {}
    for ticker in missing:
        cik = lookup_cik_for_ticker(session, ticker)
        if cik:
            direct[ticker] = cik
    if direct:
        resolved.update(direct)
        _merge_ticker_cache(direct)

    missing = [ticker for ticker in want if ticker not in resolved]
    if not missing:
        return resolved

    full_map = get_cik_map(session)
    learned = {
        ticker: full_map[ticker]
        for ticker in missing
        if ticker in full_map
    }
    resolved.update(learned)
    _merge_ticker_cache(learned)
    return resolved


# ── Proxy filing lookup ───────────────────────────────────────────────────────

def find_latest_proxy(session: requests.Session, cik: str) -> dict | None:
    """Return {url, filing_date, company_name} for the most recent DEF 14A, or None."""
    cik_padded = cik.zfill(10)
    cik_int    = str(int(cik))
    try:
        data = fetch(session, SEC_SUBMISSIONS_URL.format(cik_padded=cik_padded)).json()
    except Exception as e:
        print(f"    submissions fetch error: {e}")
        return None

    company_name = data.get("name", "")
    recent = data.get("filings", {}).get("recent", {})
    forms  = recent.get("form", [])
    accs   = recent.get("accessionNumber", [])
    dates  = recent.get("filingDate", [])
    docs   = recent.get("primaryDocument", [])

    for form, acc, date, doc in zip(forms, accs, dates, docs):
        if form in ("DEF 14A", "DEF14A"):
            acc_nodash = acc.replace("-", "")
            url = f"{SEC_ARCHIVES_BASE}/edgar/data/{cik_int}/{acc_nodash}/{doc}"
            return {"url": url, "filing_date": date, "company_name": company_name}

    return None
