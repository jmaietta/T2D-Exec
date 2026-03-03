"""
edgar_client.py — Shared EDGAR HTTP client for T2D Exec pipelines
==================================================================
Provides rate-limited HTTP access to SEC EDGAR APIs and common
lookup functions used by all pipeline scripts.
"""

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Constants ─────────────────────────────────────────────────────────────────

USER_AGENT    = "t2d-exec contact@ceorater.com"
REQUEST_DELAY = 0.15   # seconds between requests (~7 req/sec — SEC rate limit)

SEC_TICKERS_URL     = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik_padded}.json"
SEC_SUBMISSIONS_OLD = "https://data.sec.gov/submissions/{filename}"
SEC_ARCHIVES_BASE   = "https://www.sec.gov/Archives"
EFTS_SEARCH_URL     = "https://efts.sec.gov/LATEST/search-index"
QUARTERLY_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx"

# ── Session ───────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


_last_req = 0.0


def fetch(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
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

def get_cik_map(session: requests.Session) -> dict[str, str]:
    """Return {TICKER: CIK} for all companies on SEC EDGAR."""
    print("Fetching ticker→CIK map from SEC...")
    data = fetch(session, SEC_TICKERS_URL).json()
    return {
        str(v["ticker"]).upper(): str(v["cik_str"])
        for v in data.values()
        if v.get("ticker") and v.get("cik_str")
    }


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
