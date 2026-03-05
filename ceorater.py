#!/usr/bin/env python3
"""
ceorater.py — CEO Start Date Lookup Tool
=========================================
Interactive CLI menu for looking up CEO names and start dates
from SEC filings. Feed in any ticker; the tool handles the rest.

Usage:
    venv/bin/python3 ceorater.py
"""

import argparse
import csv
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from db import read_db as _read_db, write_db as _write_db

# ── Paths ─────────────────────────────────────────────────────────────────────

PROXY_DIR        = Path(__file__).parent
DOT_VENV_PYTHON  = PROXY_DIR / ".venv" / "bin" / "python3"
VENV_PYTHON      = PROXY_DIR / "venv" / "bin" / "python3"
PYTHON           = str(
    DOT_VENV_PYTHON if DOT_VENV_PYTHON.exists()
    else VENV_PYTHON if VENV_PYTHON.exists()
    else Path(sys.executable)
)
CONTEXT_CSV      = str(PROXY_DIR / "query_context.csv")
RESULTS_CSV      = str(PROXY_DIR / "8k_results.csv")
MANIFEST_8K_CSV  = str(PROXY_DIR / "8k_manifest.csv")
PROXY_MANIFEST_CSV = str(PROXY_DIR / "manifest.csv")


# ── Internal database helpers ─────────────────────────────────────────────────

def read_db() -> list[dict]:
    return _read_db(CONTEXT_CSV)


def write_db(rows: list[dict]) -> None:
    _write_db(CONTEXT_CSV, rows)


def ensure_tickers_in_db(tickers: list[str]) -> list[str]:
    """Add any tickers not already in the query cache with blank CEO/date.
    Returns list of newly added tickers."""
    rows = read_db()
    existing = {r["Ticker"].upper() for r in rows}
    added = []
    for t in tickers:
        if t not in existing:
            rows.append({"Ticker": t, "Company Name": "", "CEO": "", "CEO Start Date": ""})
            added.append(t)
    if added:
        write_db(rows)
    return added


def read_manifest_companies() -> dict[str, str]:
    """Return {TICKER: company_name} from the 8k_manifest (populated from SEC on every run)."""
    if not Path(MANIFEST_8K_CSV).exists():
        return {}
    companies: dict[str, str] = {}
    with open(MANIFEST_8K_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            t = row.get("ticker", "").upper()
            name = row.get("company_name", "").strip()
            if t and name and t not in companies:
                companies[t] = name
    return companies


def read_proxy_manifest_comp_years() -> dict[tuple[str, str], str]:
    """Return {(TICKER, ACCESSION): comp_year} from the current proxy manifest."""
    if not Path(PROXY_MANIFEST_CSV).exists():
        return {}
    years: dict[tuple[str, str], str] = {}
    with open(PROXY_MANIFEST_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ticker = row.get("ticker", "").upper()
            accession = row.get("accession", "").strip()
            comp_year = row.get("comp_year", "").strip()
            if ticker and accession and comp_year:
                years[(ticker, accession)] = comp_year
    return years


# ── Results helpers ───────────────────────────────────────────────────────────

def read_results() -> list[dict]:
    if not Path(RESULTS_CSV).exists():
        return []
    with open(RESULTS_CSV, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def next_output_path() -> Path:
    """Return CEO_MM_DD_YYYY_vN.csv, incrementing N until unused."""
    today = date.today().strftime("%m_%d_%Y")
    n = 1
    while True:
        p = PROXY_DIR / f"CEO_{today}_v{n}.csv"
        if not p.exists():
            return p
        n += 1


def write_output_csv(tickers: list[str]) -> Path | None:
    """Write a clean versioned output CSV for the given tickers."""
    results = read_results()
    ticker_set = {t.upper() for t in tickers}
    rows = [r for r in results if r["ticker"].upper() in ticker_set] if tickers else results
    context_map = {r["Ticker"].upper(): r for r in read_db()}

    out_fields = ["ticker", "company_name", "ceo_name", "start_date", "source",
                  "match_status", "notes"]
    out_path = next_output_path()

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        if rows:
            for r in rows:
                writer.writerow({
                    "ticker":       r.get("ticker", ""),
                    "company_name": r.get("company_name", ""),
                    "ceo_name":     _normalize_person_name(r.get("ceo_name_8k") or r.get("ceo_name_proxy", "")),
                    "start_date":   r.get("start_date_8k", ""),
                    "source":       r.get("source", ""),
                    "match_status": r.get("match_status", ""),
                    "notes":        r.get("notes", ""),
                })
        else:
            fallback_tickers = sorted(ticker_set) if tickers else sorted(context_map)
            if not fallback_tickers:
                return None
            for ticker in fallback_tickers:
                row = context_map.get(ticker, {})
                writer.writerow({
                    "ticker":       ticker,
                    "company_name": row.get("Company Name", ""),
                    "ceo_name":     _normalize_person_name(row.get("CEO", "")),
                    "start_date":   row.get("CEO Start Date", ""),
                    "source":       "context",
                    "match_status": "not_run",
                    "notes":        "No 8-K result row yet; exported from local context cache.",
                })
    return out_path


def most_recent_output() -> Path | None:
    """Return the most recently modified CEO_*.csv file."""
    files = sorted(PROXY_DIR.glob("CEO_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


# ── Subprocess runner ─────────────────────────────────────────────────────────

def run_cmd(label: str, cmd: list[str]) -> int:
    """Run a command and stream its output."""
    print(f"  {label}...", end="", flush=True)
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(PROXY_DIR),
        env=env,
    )
    saw_output = False
    if proc.stdout is not None:
        for line in proc.stdout:
            if not saw_output:
                print()
                saw_output = True
            print(line, end="")
    proc.wait()
    if proc.returncode != 0:
        if saw_output:
            print(f"  {label}: FAILED")
        else:
            print(" FAILED")
    else:
        if saw_output:
            print(f"  {label}: done")
        else:
            print(" done")
    return proc.returncode


def tickers_missing_proxy_context(tickers: list[str]) -> list[str]:
    """Return tickers missing proxy-derived CEO context in the local cache."""
    rows = read_db()
    db_map = {r["Ticker"].upper(): r for r in rows}
    missing = []
    for ticker in tickers:
        row = db_map.get(ticker, {})
        if not row.get("CEO") or not row.get("CEO Start Date"):
            missing.append(ticker)
    return missing


def run_pipeline(tickers: list[str]) -> None:
    """Run the intended pipeline: proxy context/comp first, then 8-K date refinement."""
    ticker_arg = ",".join(tickers)
    print()

    # Step 0: download recent proxies first so proxy CEO lookup can reuse the
    # local filing instead of performing a second proxy download.
    if run_cmd("Downloading proxy filings", [
        PYTHON, "download.py",
        "--tickers", ticker_arg,
    ]) != 0:
        return

    # Step 1: refresh proxy context every run using the local latest proxy. This
    # avoids stale bad cache and remains fast because the proxy file is already on disk.
    if run_cmd("Looking up CEO in proxy", [
        PYTHON, "lookup_proxy_ceo.py",
        "--tickers", ticker_arg,
        "--force",
    ]) != 0:
        return

    if run_cmd("Extracting compensation", [
        PYTHON, "extract.py",
        "--tickers", ticker_arg,
    ]) != 0:
        return

    # Step 2: use the proxy-derived date as the search hint for the 8-K lookup.
    if run_cmd("Downloading 8-K filings", [
        PYTHON, "download_8k.py",
        "--tickers", ticker_arg,
        "--context-csv", CONTEXT_CSV,
    ]) != 0:
        return

    if run_cmd("Extracting CEO name and start date", [
        PYTHON, "extract_8k.py",
        "--tickers", ticker_arg,
        "--context-csv", CONTEXT_CSV,
    ]) != 0:
        return


def run_pipeline_all() -> None:
    """Run the full pipeline for all tickers currently in the local query cache."""
    tickers = [row["Ticker"].strip().upper() for row in read_db() if row.get("Ticker", "").strip()]
    if not tickers:
        print("\n  No tickers found in the local cache.")
        return
    run_pipeline(tickers)


# ── Display helpers ───────────────────────────────────────────────────────────

def read_comp_results() -> list[dict]:
    comp_csv = str(PROXY_DIR / "ceo_comp_results.csv")
    if not Path(comp_csv).exists():
        return []
    with open(comp_csv, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _names_loosely_match(a: str, b: str) -> bool:
    """True if two CEO name strings refer to the same person (case-insensitive last-name check)."""
    a, b = a.lower().strip(), b.lower().strip()
    if not a or not b:
        return False
    return a in b or b in a or a.split()[-1] == b.split()[-1]


def _normalize_person_name(name: str) -> str:
    value = (name or "").strip()
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"^(mr|ms|mrs|dr|sir)\.?\s+", "", value, flags=re.I)
    return value.strip()


def print_ticker_results(tickers: list[str]) -> None:
    results   = read_results()
    comp_rows = read_comp_results()
    ticker_set = {t.upper() for t in tickers}
    manifest_companies = read_manifest_companies()
    manifest_comp_years = read_proxy_manifest_comp_years()
    context_rows = {r["Ticker"].upper(): r for r in read_db()}
    result_rows = {
        r["ticker"].upper(): r
        for r in results
        if r["ticker"].upper() in ticker_set
    }

    # Group all comp rows by ticker
    comp_all: dict[str, list[dict]] = {}
    for r in comp_rows:
        t = r["ticker"].upper()
        if t in ticker_set:
            comp_all.setdefault(t, []).append(r)

    print()
    for ticker in sorted(ticker_set):
        result_row = result_rows.get(ticker, {})
        context_row = context_rows.get(ticker, {})
        company = (
            result_row.get("company_name")
            or context_row.get("Company Name", "")
            or manifest_companies.get(ticker, "")
        )
        ceo = (
            result_row.get("ceo_name_8k")
            or context_row.get("CEO", "")
            or result_row.get("ceo_name_proxy")
            or "—"
        )
        ceo = _normalize_person_name(ceo) or "—"
        date_ = (
            result_row.get("start_date_8k")
            or context_row.get("CEO Start Date", "")
            or "—"
        )

        # Filter comp to the current CEO, deduplicate by year (keep highest total)
        rows_for_ceo = [
            c for c in comp_all.get(ticker, [])
            if (
                (
                    not (c.get("ceo_name") or "").strip()
                    or ceo == "—"
                    or _names_loosely_match(ceo, c.get("ceo_name", ""))
                )
                and (
                    (ticker, c.get("accession", "").strip()) not in manifest_comp_years
                    or str(c.get("comp_year", "")).strip()
                    == manifest_comp_years[(ticker, c.get("accession", "").strip())]
                )
            )
        ]
        by_year: dict[str, dict] = {}
        for c in rows_for_ceo:
            year = c.get("comp_year", "")
            try:
                total = float(c.get("total_comp") or 0)
            except (ValueError, TypeError):
                total = 0
            if year not in by_year:
                by_year[year] = c
            else:
                try:
                    prev = float(by_year[year].get("total_comp") or 0)
                except (ValueError, TypeError):
                    prev = 0
                if total > prev:
                    by_year[year] = c
        comp = sorted(by_year.values(), key=lambda x: x.get("comp_year", ""))

        print(f"  Ticker Symbol  : {ticker}")
        print(f"  Company Name   : {company}")
        print(f"  CEO Name       : {ceo}")
        print(f"  CEO Start Date : {date_}")
        if comp:
            print(f"  CEO Comp       :")
            for c in comp:
                year  = c.get("comp_year", "?")
                total = c.get("total_comp", "")
                try:
                    total_fmt = f"${float(total):>14,.0f}"
                except (ValueError, TypeError):
                    total_fmt = "—"
                print(f"    {year}  :  {total_fmt}")
        else:
            print(f"  CEO Comp       : not yet extracted")
        print()


def print_summary() -> None:
    recent = most_recent_output()
    results = read_results()

    if not results:
        print("\n  No results yet. Run option 1 first.")
        return

    counts = Counter(r["match_status"] for r in results)
    sources = Counter(r.get("source", "") for r in results)
    total = len(results)
    found = total - counts.get("not_found", 0)

    print()
    if recent:
        print(f"  Most recent output : {recent.name}")
    print(f"  Total tickers      : {total}")
    print()
    print(f"  {'exact_match':<22}: {counts['exact_match']:>4}  ({100*counts['exact_match']/total:.1f}%)")
    print(f"  {'month_match':<22}: {counts['month_match']:>4}  ({100*counts['month_match']/total:.1f}%)")
    print(f"  {'year_match':<22}: {counts['year_match']:>4}  ({100*counts['year_match']/total:.1f}%)")
    print(f"  {'mismatch':<22}: {counts['mismatch']:>4}  ({100*counts['mismatch']/total:.1f}%)")
    print(f"  {'not_found':<22}: {counts['not_found']:>4}  ({100*counts['not_found']/total:.1f}%)")
    print()
    print(f"  Coverage  : {found}/{total}  =  {100*found/total:.1f}%")
    src_str = "  ".join(f"{k}={v}" for k, v in sorted(sources.items()) if k)
    print(f"  Sources   : {src_str}")


def print_not_found() -> None:
    results = read_results()
    rows = [r for r in results if r["match_status"] == "not_found"]

    if not rows:
        print("\n  No not-found tickers.")
        return

    rows.sort(key=lambda x: x["ticker"])
    print()
    print(f"  {'#':<4}  {'Ticker':<8}  {'CEO':<35}  {'DB Date'}")
    print("  " + "─" * 68)
    for i, r in enumerate(rows, 1):
        ceo  = r.get("ceo_name_proxy") or "—"
        date_ = r.get("start_date_proxy") or "—"
        print(f"  {i:<4}  {r['ticker']:<8}  {ceo:<35}  {date_}")
    print(f"\n  Total not-found: {len(rows)}")


# ── Menu actions ──────────────────────────────────────────────────────────────

def action_lookup() -> None:
    raw = input("\n  Enter ticker(s), comma-separated (or press Enter for all): ").strip()

    if not raw:
        confirm = input("  Run for ALL tickers in local cache? [y/N]: ").strip().lower()
        if confirm != "y":
            return
        run_pipeline_all()
        out = write_output_csv([])
        if out:
            print(f"\n  Output saved: {out.name}")
        print_summary()
        return

    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tickers:
        return

    ensure_tickers_in_db(tickers)

    run_pipeline(tickers)

    out = write_output_csv(tickers)
    if out:
        print(f"\n  Output saved: {out.name}")

    print_ticker_results(tickers)


def action_view_results() -> None:
    print_summary()


def action_not_found() -> None:
    print_not_found()
    raw = input("\n  Re-run any? Enter ticker(s) or press Enter to skip: ").strip()
    if raw:
        tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
        if tickers:
            ensure_tickers_in_db(tickers)
            run_pipeline(tickers)
            out = write_output_csv(tickers)
            if out:
                print(f"\n  Output saved: {out.name}")
            print_ticker_results(tickers)


# ── Main loop ─────────────────────────────────────────────────────────────────

MENU = """
T2D Exec — CEO Lookup
══════════════════════════════════
  1. Look up CEO by Ticker Symbol
  2. View results summary
  3. View / re-run not-found tickers
  4. Exit
"""


def main(cli_tickers: list[str] | None = None) -> None:
    if cli_tickers:
        ensure_tickers_in_db(cli_tickers)
        run_pipeline(cli_tickers)
        out = write_output_csv(cli_tickers)
        if out:
            print(f"\n  Output saved: {out.name}")
        print_ticker_results(cli_tickers)
        return

    while True:
        print(MENU)
        choice = input("  Choice: ").strip()

        if choice == "1":
            action_lookup()
        elif choice == "2":
            action_view_results()
        elif choice == "3":
            action_not_found()
        elif choice in ("4", "q", "exit", "quit"):
            print("\n  Goodbye.\n")
            break
        else:
            print("  Invalid choice. Enter 1, 2, 3, or 4.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Query SEC filings for company name, CEO, CEO start date, and CEO compensation"
    )
    parser.add_argument(
        "tickers_positional",
        nargs="*",
        help="Ticker symbols for a one-shot lookup (e.g. MSFT AMD)",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated tickers for a one-shot lookup (skips the menu)",
    )
    args = parser.parse_args()
    cli_tickers = None
    if args.tickers:
        cli_tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.tickers_positional:
        parsed: list[str] = []
        for raw in args.tickers_positional:
            parsed.extend([t.strip().upper() for t in raw.split(",") if t.strip()])
        cli_tickers = parsed
    main(cli_tickers=cli_tickers)
