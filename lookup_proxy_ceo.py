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
import csv
import json
import re
import time
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
from env_utils import load_local_env
from sec_filing_parser import parse_filing, text_lines, extract_ixbrl_peo_name

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
load_local_env(Path(__file__).parent / ".env", override=True)

from edgar_client import (
    build_session, fetch, get_ciks, find_latest_proxy,
    SEC_SUBMISSIONS_URL, QUARTERLY_INDEX_URL,
)
from db import read_db, write_db, DB_FIELDS

# ── Config ────────────────────────────────────────────────────────────────────

MODEL          = "claude-sonnet-4-6"
MAX_TOKENS     = 512
API_DELAY      = 3.0
MAX_TEXT_CHARS = 15000
DEFAULT_CONTEXT_CSV = "./query_context.csv"
MANIFEST_CSV   = "./manifest.csv"
LLM_CACHE_DIR  = Path(__file__).parent / ".cache" / "llm_proxy_ceo"
NAME_LINE_RE   = re.compile(r"^[A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){1,3}$")
CEO_TITLE_RE   = re.compile(
    r"\b("
    r"chairman\s*(?:and|&)\s*chief executive officer|"
    r"chairman\s*(?:and|&)\s*ceo|"
    r"chief executive officer and director|"
    r"chief executive officer|"
    r"chief executive|"
    r"president\s*(?:and|&)\s*chief executive officer|"
    r"president\s*(?:and|&)\s*ceo|"
    r"principal executive officer|"
    r"\bceo\b"
    r")\b",
    re.I,
)
NON_PERSON_NAME_TOKENS = {
    "annual", "board", "chief", "committee", "compensation", "contents",
    "corporate", "director", "directors", "executive", "fiscal", "meeting",
    "nominee", "nominees", "officer", "officers", "president", "proxy",
    "statement", "stockholder", "stockholders", "table",
}
HONORIFIC_PREFIX_RE = re.compile(r"^(mr|ms|mrs|dr|sir)\.?\s+", re.I)


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
    lines = text_lines(html)

    title_re = CEO_TITLE_RE
    year_range_re = re.compile(r"\(\s*(\d{4})\s*-\s*(present|\d{4})\s*\)", re.I)
    director_since_re = re.compile(r"director\s+since\s*:?\s*(\d{4}|new nominee)", re.I)
    explicit_since_re = re.compile(r"\bsince\s+(?:[A-Za-z]+\s+)?\d{4}\b", re.I)
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
        if any(NAME_LINE_RE.match(candidate) for candidate in window):
            score += 5
        normalized_line = line.lower().replace("&", " and ")
        if "chairman and chief executive officer" in normalized_line:
            score += 3
        if company_re and company_re.search(re.sub(r"[^a-z0-9]+", " ", snippet.lower())):
            score += 7
        if generic_re.search(snippet):
            score -= 4
        normalized_snippet = snippet.lower().replace("&", " and ")
        if "letter from our chairman and chief executive officer" in normalized_snippet:
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

    soup = BeautifulSoup(html, "lxml")
    full = re.sub(r"\s+", " ", soup.get_text(" ")).strip()
    return full[:MAX_TEXT_CHARS]


def extract_summary_comp_table_text(html: str) -> str:
    """Extract a compact text version of the Summary Compensation Table, if present."""
    soup = BeautifulSoup(html, "lxml")
    best_table = None
    best_score = -1
    for table in soup.find_all("table"):
        raw = table.get_text(" ", strip=True)
        text = raw.lower()
        score = 0
        if "summary compensation" in text:
            score += 10
        if "chief executive" in text or "ceo" in text or "principal executive" in text:
            score += 6
        if "salary" in text:
            score += 2
        if "total" in text:
            score += 2
        if score > best_score:
            best_score = score
            best_table = table

    if not best_table or best_score < 5:
        return ""

    rows = []
    for tr in best_table.find_all("tr"):
        cells = []
        for td in tr.find_all(["td", "th"]):
            cell = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
            if cell:
                cells.append(cell)
        if cells:
            rows.append("\t".join(cells))
    return "\n".join(rows)[:MAX_TEXT_CHARS]


def _proxy_lines(html: str) -> list[str]:
    return text_lines(html)


def _extract_date_from_text(text: str) -> dict:
    month_names = ("(?:January|February|March|April|May|June|July|August|"
                   "September|October|November|December)")

    full_date_patterns = [
        rf"\b(?:since|effective|appointed|named|elected|became|joined)\b[^.:\n]{{0,60}}?\b({month_names}\s+\d{{1,2}},\s+\d{{4}})\b",
        rf"\b({month_names}\s+\d{{1,2}},\s+\d{{4}})\b",
    ]
    for pattern in full_date_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            raw = match.group(1)
            from datetime import datetime
            try:
                d = datetime.strptime(raw, "%B %d, %Y").date()
                return {"start_date": d.isoformat(), "start_month": None, "start_year": None}
            except ValueError:
                pass

    month_year_patterns = [
        rf"\b(?:since|effective|appointed|named|elected|became|joined)\b[^.:\n]{{0,60}}?\b({month_names}\s+\d{{4}})\b",
        rf"\b({month_names}\s+\d{{4}})\b",
    ]
    for pattern in month_year_patterns:
        match = re.search(pattern, text, re.I)
        if match:
            raw = match.group(1)
            from datetime import datetime
            try:
                d = datetime.strptime(raw, "%B %Y")
                return {"start_date": None, "start_month": f"{d.year:04d}-{d.month:02d}", "start_year": None}
            except ValueError:
                pass

    range_match = re.search(r"\(\s*(20\d{2})\s*[-\u2013\u2014]\s*(?:present|20\d{2})\s*\)", text, re.I)
    if range_match:
        return {"start_date": None, "start_month": None, "start_year": int(range_match.group(1))}

    titled_range = re.search(
        r"\b(20\d{2})\s*[-\u2013\u2014]\s*(?:present|20\d{2})\s*:\s*.*?"
        r"(chief\s+executive\s+officer|chief\s+executive|\bceo\b|principal\s+executive\s+officer)",
        text,
        re.I,
    )
    if titled_range:
        return {"start_date": None, "start_month": None, "start_year": int(titled_range.group(1))}

    since_year = re.search(r"\b(?:since|appointed|named|elected|became|joined)\b[^.:\n]{0,40}?\b(20\d{2})\b", text, re.I)
    if since_year:
        return {"start_date": None, "start_month": None, "start_year": int(since_year.group(1))}

    director_since = re.search(r"director\s+since\s*:?\s*(20\d{2})", text, re.I)
    if director_since:
        return {"start_date": None, "start_month": None, "start_year": int(director_since.group(1))}

    standalone_year = re.search(r"(?:^|\n)\s*(20\d{2})\s*(?:\n|$)", text, re.I)
    if standalone_year:
        return {"start_date": None, "start_month": None, "start_year": int(standalone_year.group(1))}

    return {"start_date": None, "start_month": None, "start_year": None}


def _extract_ceo_tenure_date_strict(text: str) -> dict:
    """Extract CEO start date using explicit CEO-tenure phrasing only."""
    month_names = ("(?:January|February|March|April|May|June|July|August|"
                   "September|October|November|December)")
    t = _normalize_person_name(text)
    if not t:
        return {"start_date": None, "start_month": None, "start_year": None}

    full_patterns = [
        rf"\b(?:assumed\s+the\s+role\s+of|assumed|was\s+named|named|appointed|became|joined\s+as)\b"
        rf"[^.\n]{{0,90}}\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        rf"[^.\n]{{0,60}}\b(?:on|since|effective)\s+({month_names}\s+\d{{1,2}},\s+\d{{4}})\b",
        rf"\b(?:has\s+served\s+as|has\s+been)\b[^.\n]{{0,60}}"
        rf"\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        rf"[^.\n]{{0,40}}\bsince\s+({month_names}\s+\d{{1,2}},\s+\d{{4}})\b",
    ]
    for pattern in full_patterns:
        m = re.search(pattern, t, re.I)
        if not m:
            continue
        raw = m.group(1)
        try:
            from datetime import datetime
            d = datetime.strptime(raw, "%B %d, %Y").date()
            return {"start_date": d.isoformat(), "start_month": None, "start_year": None}
        except ValueError:
            continue

    month_patterns = [
        rf"\b(?:assumed\s+the\s+role\s+of|assumed|was\s+named|named|appointed|became|joined\s+as)\b"
        rf"[^.\n]{{0,90}}\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        rf"[^.\n]{{0,60}}\b(?:on|since|effective)\s+({month_names}\s+\d{{4}})\b",
        rf"\b(?:has\s+served\s+as|has\s+been)\b[^.\n]{{0,60}}"
        rf"\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        rf"[^.\n]{{0,40}}\bsince\s+({month_names}\s+\d{{4}})\b",
    ]
    for pattern in month_patterns:
        m = re.search(pattern, t, re.I)
        if not m:
            continue
        raw = m.group(1)
        try:
            from datetime import datetime
            d = datetime.strptime(raw, "%B %Y")
            return {"start_date": None, "start_month": f"{d.year:04d}-{d.month:02d}", "start_year": None}
        except ValueError:
            continue

    range_match = re.search(
        r"\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        r"[^.\n]{0,60}\(\s*(20\d{2})\s*[-\u2013\u2014]\s*(?:present|20\d{2})\s*\)",
        t,
        re.I,
    )
    if range_match:
        return {"start_date": None, "start_month": None, "start_year": int(range_match.group(1))}

    since_year = re.search(
        r"\b(?:has\s+served\s+as|has\s+been|was\s+named|named|appointed|became|assumed)\b"
        r"[^.\n]{0,90}\b(?:chief\s+executive\s+officer|ceo|principal\s+executive\s+officer)\b"
        r"[^.\n]{0,40}\bsince\s+(20\d{2})\b",
        t,
        re.I,
    )
    if since_year:
        return {"start_date": None, "start_month": None, "start_year": int(since_year.group(1))}

    return {"start_date": None, "start_month": None, "start_year": None}


def _extract_name_from_line(line: str) -> str:
    def _looks_like_person_name(value: str) -> bool:
        value = value.strip()
        if not NAME_LINE_RE.match(value):
            return False
        tokens = [t.lower() for t in re.split(r"\s+", value) if t]
        if len(tokens) < 2:
            return False
        if any(token in NON_PERSON_NAME_TOKENS for token in tokens):
            return False
        return True

    inline = re.match(
        r"^\s*([A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){1,3})\s*(?:,|-|[|])\s*.*$",
        line,
    )
    if inline and CEO_TITLE_RE.search(line):
        candidate = inline.group(1).strip()
        if _looks_like_person_name(candidate):
            return candidate
    if _looks_like_person_name(line):
        return line.strip()
    return ""


def _normalize_person_name(name: str) -> str:
    value = re.sub(r"\s+", " ", (name or "").strip())
    if not value:
        return ""
    value = re.sub(r"\s*\(\d+\)\s*$", "", value).strip()
    value = HONORIFIC_PREFIX_RE.sub("", value).strip()
    return value


def _expand_to_full_name(name: str, lines: list[str]) -> str:
    """Expand abbreviated names (e.g., 'Mr. Snow') using nearby proxy text."""
    cleaned = _normalize_person_name(name)
    if not cleaned:
        return ""
    parts = [p for p in re.split(r"\s+", cleaned) if p]
    if len(parts) >= 2 and not re.match(r"^[A-Z]\.?$", parts[0]):
        return cleaned

    last = parts[-1].lower().strip(".,") if parts else ""
    if not last:
        return cleaned

    pattern = re.compile(
        rf"\b([A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){{1,3}})\b[^.\n]{{0,120}}\b{re.escape(last)}\b",
        re.I,
    )
    for line in lines:
        if not CEO_TITLE_RE.search(line):
            continue
        m = pattern.search(line)
        if not m:
            continue
        candidate = _normalize_person_name(m.group(1))
        if candidate and last in candidate.lower():
            return candidate

    name_line_re = re.compile(
        rf"^[A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){{1,3}}$"
    )
    for line in lines:
        candidate = _normalize_person_name(line)
        if not candidate:
            continue
        if not name_line_re.match(candidate):
            continue
        if candidate.lower().split()[-1].strip(".,") == last:
            return candidate
    return cleaned


def extract_proxy_ceo_deterministic(html: str, company: str = "") -> dict:
    """Deterministically extract CEO name and rough start date from proxy text."""
    lines = _proxy_lines(html)
    best = None
    company_key = re.sub(r"[^a-z0-9]+", " ", (company or "").lower()).strip()

    for idx, line in enumerate(lines):
        if not CEO_TITLE_RE.search(line):
            continue

        start = max(0, idx - 6)
        end = min(len(lines), idx + 4)
        window = lines[start:end]
        window_text = "\n".join(window)

        name = _extract_name_from_line(line)
        name_abs_idx = idx
        if not name:
            for offset, cand in enumerate(window):
                maybe = _extract_name_from_line(cand)
                if maybe and not CEO_TITLE_RE.search(maybe):
                    name = maybe
                    name_abs_idx = start + offset
                    break

        if not name:
            continue

        focused_start = max(0, min(name_abs_idx, idx) - 1)
        focused_end = min(len(lines), max(name_abs_idx, idx) + 5)
        focused_text = "\n".join(lines[focused_start:focused_end])
        date_info = _extract_date_from_text(focused_text)
        has_date = bool(
            date_info["start_date"] or date_info["start_month"] or date_info["start_year"]
        )
        normalized_window = re.sub(r"[^a-z0-9]+", " ", window_text.lower())
        normalized_line = re.sub(r"[^a-z0-9&]+", " ", line.lower())
        score = 0
        if has_date:
            score += 8
        if company_key and company_key in normalized_window:
            score += 10
        if " our ceo " in f" {normalized_window} " or " our chief executive officer " in f" {normalized_window} ":
            score += 5
        if "chairman and ceo" in normalized_line or "chairman and chief executive officer" in normalized_line:
            score += 4
        if "," in line and company_key and company_key not in normalized_window:
            score -= 6

        candidate = {
            "found": True,
            "ceo_name": _normalize_person_name(name),
            "start_date": date_info["start_date"],
            "start_month": date_info["start_month"],
            "start_year": date_info["start_year"],
            "notes": "deterministic_proxy_match" if has_date else "deterministic_name_only",
            "_score": score,
        }

        if best is None:
            best = candidate
            continue

        best_score = best.get("_score", 0)
        if score > best_score:
            best = candidate

    if best is not None:
        best.pop("_score", None)
        return best

    return {"found": False, "ceo_name": "", "start_date": None, "start_month": None, "start_year": None, "notes": "deterministic_not_found"}


def extract_proxy_start_for_named_ceo(html: str, company: str, ceo_name: str) -> dict:
    """Extract only the rough CEO start date for a specific known CEO name."""
    if not ceo_name:
        return {"found": False, "ceo_name": "", "start_date": None, "start_month": None, "start_year": None, "notes": "named_ceo_missing"}

    lines = _proxy_lines(html)
    company_key = re.sub(r"[^a-z0-9]+", " ", (company or "").lower()).strip()
    best = None
    clean_name = _normalize_person_name(ceo_name)
    target = clean_name.lower()

    for idx, line in enumerate(lines):
        if target not in line.lower():
            continue
        start = max(0, idx - 2)
        end = min(len(lines), idx + 8)
        window = lines[start:end]
        window_text = "\n".join(window)
        if not CEO_TITLE_RE.search(window_text):
            continue

        date_info = _extract_ceo_tenure_date_strict(window_text)
        has_date = bool(date_info["start_date"] or date_info["start_month"] or date_info["start_year"])
        score = 0
        if has_date:
            score += 10
        normalized_window = re.sub(r"[^a-z0-9]+", " ", window_text.lower())
        if company_key and company_key in normalized_window:
            score += 8
        if "former chief executive officer" in normalized_window or "retired from his role as ceo" in normalized_window:
            score -= 12

        candidate = {
            "found": True,
            "ceo_name": clean_name,
            "start_date": date_info["start_date"],
            "start_month": date_info["start_month"],
            "start_year": date_info["start_year"],
            "notes": "ixbrl_peo_name_targeted" if has_date else "ixbrl_peo_name_only",
            "_score": score,
        }
        if best is None or candidate["_score"] > best["_score"]:
            best = candidate

    if best is not None:
        best.pop("_score", None)
        return best

    return {"found": True, "ceo_name": clean_name, "start_date": None, "start_month": None, "start_year": None, "notes": "ixbrl_peo_name_only"}


def load_local_proxies(ticker: str) -> list[dict]:
    """Return local proxies for ticker from manifest.csv, newest first."""
    if not Path(MANIFEST_CSV).exists():
        return []
    rows: list[dict] = []
    with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("ticker", "").strip().upper() != ticker.upper():
                continue
            if row.get("status") not in ("downloaded", "exists"):
                continue
            local_path = Path(row.get("local_path", ""))
            if not local_path.exists():
                continue
            rows.append({
                "filing_date": row.get("filing_date", ""),
                "local_path": str(local_path),
            })
    rows.sort(key=lambda row: row["filing_date"], reverse=True)
    return rows


def _result_sort_key(result: dict) -> str:
    if result.get("start_date"):
        return result["start_date"]
    if result.get("start_month"):
        return f"{result['start_month']}-01"
    if result.get("start_year"):
        return f"{int(result['start_year']):04d}-01-01"
    return "9999-12-31"


def backfill_named_ceo_start_from_proxy_history(ticker: str, company: str, ceo_name: str, skip_filing_date: str = "") -> dict | None:
    """Search prior local proxies for an explicit CEO start date for the same person."""
    candidates: list[dict] = []
    for proxy in load_local_proxies(ticker):
        filing_date = proxy.get("filing_date", "")
        if skip_filing_date and filing_date == skip_filing_date:
            continue
        try:
            html = Path(proxy["local_path"]).read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        result = extract_proxy_start_for_named_ceo(html, company, ceo_name)
        if not result.get("found"):
            continue
        if not (result.get("start_date") or result.get("start_month") or result.get("start_year")):
            continue
        result["notes"] = "proxy_history_backfill"
        result["_sort_key"] = _result_sort_key(result)
        candidates.append(result)
    if not candidates:
        return None
    candidates.sort(key=lambda r: r["_sort_key"])
    best = candidates[0]
    best.pop("_sort_key", None)
    return best


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
  - "Chairman & Chief Executive Officer (2021-present)"
  - "Chairman & CEO (2021-present)"
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

TABLE_PROMPT = """\
You are analyzing only the Summary Compensation Table from a proxy statement (DEF 14A) for {company} ({ticker}).

Identify the CURRENT principal executive officer / CEO from the table.

Rules:
1. Return only the row for the person whose title is CEO, Chief Executive Officer, Chairman and CEO, President and CEO, or Principal Executive Officer.
2. Do not return a non-CEO chair, independent director, or other named executive.
3. Return the person's full name exactly as shown, without footnote markers.
4. If no CEO row is present, return found=false.

Return ONLY valid JSON:
{{"found": true, "ceo_name": "Full Name", "title": "Chief Executive Officer"}}

Summary Compensation Table:
{text}"""


# ── Claude call ───────────────────────────────────────────────────────────────

def call_claude(client: anthropic.Anthropic, ticker: str, company: str,
                filing_date: str, text: str, accession: str = "") -> dict:
    cache_path = None
    if accession:
        LLM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe = accession.replace("/", "_")
        cache_path = LLM_CACHE_DIR / f"{ticker}_{safe}.json"
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if isinstance(cached, dict):
                    return cached
            except Exception:
                pass
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
                if cache_path:
                    cache_path.write_text(json.dumps(result), encoding="utf-8")
                return result
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            if isinstance(result, dict):
                if cache_path:
                    cache_path.write_text(json.dumps(result), encoding="utf-8")
                return result
        return {"found": False, "notes": f"parse_error: {raw[:80]}"}
    except Exception as e:
        return {"found": False, "notes": f"api_error: {e}"}


def call_claude_table_name(client: anthropic.Anthropic, ticker: str, company: str,
                           table_text: str, accession: str = "") -> dict:
    if not table_text:
        return {"found": False, "ceo_name": "", "notes": "no_summary_comp_table"}
    cache_path = None
    if accession:
        LLM_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe = accession.replace("/", "_")
        cache_path = LLM_CACHE_DIR / f"{ticker}_{safe}_table_name.json"
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if isinstance(cached, dict):
                    return cached
            except Exception:
                pass
    time.sleep(API_DELAY)
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": TABLE_PROMPT.format(
                company=company, ticker=ticker, text=table_text
            )}],
        )
        raw = msg.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        result = json.loads(raw) if raw.startswith("{") else json.loads(re.search(r"\{.*\}", raw, re.DOTALL).group())
        if isinstance(result, dict):
            if cache_path:
                cache_path.write_text(json.dumps(result), encoding="utf-8")
            return result
    except Exception as e:
        return {"found": False, "ceo_name": "", "notes": f"api_error: {e}"}
    return {"found": False, "ceo_name": "", "notes": "parse_error"}


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


def load_latest_local_proxy(ticker: str) -> dict | None:
    """Return the newest local proxy for ticker from manifest.csv, if available."""
    if not Path(MANIFEST_CSV).exists():
        return None

    rows = []
    with open(MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("ticker", "").strip().upper() != ticker.upper():
                continue
            if row.get("status") not in ("downloaded", "exists"):
                continue
            local_path = Path(row.get("local_path", ""))
            if not local_path.exists():
                continue
            rows.append({
                "filing_date": row.get("filing_date", ""),
                "local_path": str(local_path),
            })

    if not rows:
        return None

    rows.sort(key=lambda row: row["filing_date"], reverse=True)
    return rows[0]


# ── Main ─────────────────────────────────────────────────────────────────────

def run(tickers: list[str], db_path: str = DEFAULT_CONTEXT_CSV, force: bool = False) -> None:
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = None
    if api_key:
        try:
            client = anthropic.Anthropic(api_key=api_key, timeout=45.0)
        except TypeError:
            client = anthropic.Anthropic(api_key=api_key)
    session = build_session()

    # Only skip when both CEO name and a proxy-derived rough start date exist,
    # unless this is an explicit re-run.
    db_rows = read_db(db_path)
    db_map  = {r["Ticker"].upper(): r for r in db_rows}

    need_lookup = []
    for t in tickers:
        row = db_map.get(t.upper(), {})
        if force or not row.get("CEO") or not row.get("CEO Start Date"):
            need_lookup.append(t.upper())

    if not need_lookup:
        print("  All tickers already have CEO name and start date — skipping proxy lookup. Use --force to re-run.")
        return

    print(f"\nStep 0 — Proxy CEO lookup for {len(need_lookup)} new ticker(s)...")
    print("─" * 55)

    print("  Resolving ticker→CIK...")
    cik_map = get_ciks(session, need_lookup)

    updated = 0
    for ticker in need_lookup:
        company = db_map.get(ticker, {}).get("Company Name", "")
        filing_date = ""
        local_proxy = load_latest_local_proxy(ticker)
        if local_proxy:
            filing_date = local_proxy["filing_date"]
            print(f"  {ticker:<6}  Using local DEF 14A ({filing_date})")
            if not company:
                cik = cik_map.get(ticker)
                if cik:
                    try:
                        sub = fetch(session, SEC_SUBMISSIONS_URL.format(cik_padded=cik.zfill(10))).json()
                        company = (sub.get("name") or "").strip()
                    except Exception:
                        pass
            try:
                html = Path(local_proxy["local_path"]).read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                print(f"  {ticker:<6}  local proxy read error: {e}")
                continue
        else:
            cik = cik_map.get(ticker)
            if not cik:
                print(f"  {ticker:<6}  NO CIK — skipping")
                continue

            print(f"  {ticker:<6}  Looking up latest DEF 14A...", end=" ", flush=True)
            proxy_info = find_latest_proxy(session, cik)
            if not proxy_info:
                print("no DEF 14A found")
                continue
            filing_date = proxy_info["filing_date"]
            print(filing_date)

            print(f"  {ticker:<6}  Downloading proxy ({filing_date})...", end=" ", flush=True)
            try:
                resp = fetch(session, proxy_info["url"])
                html = resp.text
            except Exception as e:
                print(f"download error: {e}")
                continue
            company = company or proxy_info["company_name"]
        parsed = parse_filing(
            html,
            accession=f"{ticker}_{filing_date}",
            source_path=(local_proxy["local_path"] if local_proxy else ""),
        )
        print(f"  {ticker:<6}  Parser mode: {parsed['parser_mode']} ({parsed.get('fact_source', 'n/a')})")

        text    = extract_proxy_text(html, company)
        proxy_lines = _proxy_lines(html)
        ixbrl_peo_name = extract_ixbrl_peo_name(parsed)
        if ixbrl_peo_name:
            ixbrl_peo_name = _expand_to_full_name(ixbrl_peo_name, proxy_lines)
            result = extract_proxy_start_for_named_ceo(html, company, ixbrl_peo_name)
            print(f"  {ticker:<6}  iXBRL PEO name...", end=" ", flush=True)
            print(f"found  ({ixbrl_peo_name})")
        else:
            table_name_result = {"found": False, "ceo_name": "", "notes": "table_name_not_run"}
            if client is not None:
                table_text = extract_summary_comp_table_text(html)
                if table_text:
                    print(f"  {ticker:<6}  Asking Claude for CEO from Summary Compensation Table...", end=" ", flush=True)
                    table_name_result = call_claude_table_name(
                        client, ticker, company, table_text, accession=f"{ticker}_{filing_date}"
                    )
                    if table_name_result.get("found") and table_name_result.get("ceo_name"):
                        print(f"found  ({table_name_result.get('ceo_name')})")
                    else:
                        print(f"not found  ({table_name_result.get('notes', '')})")
            if table_name_result.get("found") and table_name_result.get("ceo_name"):
                table_ceo = _expand_to_full_name(table_name_result.get("ceo_name", ""), proxy_lines)
                result = extract_proxy_start_for_named_ceo(html, company, table_ceo)
                result["ceo_name"] = table_ceo
                if result.get("notes", "").startswith("ixbrl_"):
                    result["notes"] = "table_name_targeted"
            else:
                result = extract_proxy_ceo_deterministic(html, company)
        has_any_date = bool(
            result.get("start_date") or result.get("start_month") or result.get("start_year")
        )
        if result.get("found") and has_any_date and not ixbrl_peo_name:
            print(f"  {ticker:<6}  Deterministic CEO extraction...", end=" ", flush=True)
            print(f"found  ({result.get('notes', '')})")
        elif not has_any_date:
            # Before calling the LLM for date inference, try deterministic
            # backfill from older local proxies for the same ticker/CEO.
            backfill_early = backfill_named_ceo_start_from_proxy_history(
                ticker=ticker,
                company=company,
                ceo_name=result.get("ceo_name", ""),
                skip_filing_date=filing_date,
            )
            if backfill_early:
                result.update({
                    "start_date": backfill_early.get("start_date"),
                    "start_month": backfill_early.get("start_month"),
                    "start_year": backfill_early.get("start_year"),
                    "notes": backfill_early.get("notes", result.get("notes", "")),
                })
                print(f"  {ticker:<6}  Proxy history backfill... found {result_to_date_str(result)}")
                has_any_date = True

        if not has_any_date:
            if result.get("found") and not ixbrl_peo_name:
                print(f"  {ticker:<6}  Deterministic CEO extraction...", end=" ", flush=True)
                print("found name only; asking Claude for date")
            if client is None:
                print(f"  {ticker:<6}  no deterministic match and ANTHROPIC_API_KEY is not set")
                continue
            print(f"  {ticker:<6}  Asking Claude for CEO/start date...", end=" ", flush=True)
            claude_result = call_claude(client, ticker, company, filing_date, text, accession=f"{ticker}_{filing_date}")
            if result.get("found") and claude_result.get("found"):
                result = {
                    "found": True,
                    "ceo_name": result.get("ceo_name") or claude_result.get("ceo_name", ""),
                    "start_date": claude_result.get("start_date"),
                    "start_month": claude_result.get("start_month"),
                    "start_year": claude_result.get("start_year"),
                    "notes": claude_result.get("notes", ""),
                }
            elif result.get("found") and not claude_result.get("found"):
                if claude_result.get("notes"):
                    result["notes"] = f"{result.get('notes', '')}; claude_date_unavailable: {claude_result.get('notes')}".strip("; ")
            else:
                result = claude_result

        if not result.get("found"):
            print(f"not found  ({result.get('notes', '')})")
            continue

        filing_year = 0
        try:
            filing_year = int((filing_date or "")[:4])
        except ValueError:
            filing_year = 0

        weak_or_missing_date = not (
            result.get("start_date") or result.get("start_month") or result.get("start_year")
        )
        if result.get("start_year") and filing_year and int(result["start_year"]) >= filing_year - 1:
            weak_or_missing_date = True

        if weak_or_missing_date:
            backfill = backfill_named_ceo_start_from_proxy_history(
                ticker=ticker,
                company=company,
                ceo_name=result.get("ceo_name", ""),
                skip_filing_date=filing_date,
            )
            if backfill:
                result.update({
                    "start_date": backfill.get("start_date"),
                    "start_month": backfill.get("start_month"),
                    "start_year": backfill.get("start_year"),
                    "notes": backfill.get("notes", result.get("notes", "")),
                })
                print(f"  {ticker:<6}  Proxy history backfill... found {result_to_date_str(result)}")

        ceo_name  = _normalize_person_name(result.get("ceo_name", "").strip())

        # Use the proxy-derived rough date directly. The exact date is refined
        # later from the appointment 8-K, so do not do a slow IPO backfill here.
        if result.get("start_year") and not result.get("start_date") and not result.get("start_month"):
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
