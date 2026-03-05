# T2D Exec

T2D Exec is a ticker-driven SEC query engine for CEO research. Give it one or more ticker symbols and it will pull public EDGAR filings, identify the current CEO, recover the CEO start date, and extract recent compensation.

The intended workflow is one command, end to end.

## What It Does

- Reads the latest DEF 14A proxy to identify the current CEO and extract a rough tenure hint.
- Uses that proxy hint to find 8-K (Item 5.02) filings in a tight date window around the expected appointment.
- Also runs a recent Item 5.02 sweep (about 18 months) to detect post-proxy CEO transitions.
- Extracts the exact CEO start date from the 8-K when available.
- Falls back to proxy-derived evidence when no exact 8-K date can be confirmed.
- Downloads recent proxy filings and extracts CEO compensation by year.

## Run

Interactive menu:

```bash
.venv/bin/python3 ceorater.py
# or
python3 ceorater.py
```

One-shot query for one or more tickers:

```bash
.venv/bin/python3 ceorater.py MSFT
.venv/bin/python3 ceorater.py MSFT ADSK AMD
# or
python3 ceorater.py MSFT
python3 ceorater.py MSFT ADSK AMD
```

Example output:

```text
  Ticker Symbol  : MSFT
  Company Name   : MICROSOFT CORP
  CEO Name       : Satya Nadella
  CEO Start Date : 2014-02-04
  CEO Comp       :
    2023  :  $    48,512,537
    2024  :  $    79,106,183
    2025  :  $    96,496,790
```

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Set your Anthropic API key in `.env`:

```env
ANTHROPIC_API_KEY=sk-ant-...
```

## Core Files

- `ceorater.py`: main CLI and pipeline orchestrator
- `lookup_proxy_ceo.py`: proxy-first CEO discovery
- `download_8k.py`: 8-K retrieval (proxy-date window + recent 5.02 sweep + historical fallback)
- `extract_8k.py`: CEO name and start-date extraction with current-CEO supersession handling
- `download.py`: proxy download for compensation extraction
- `extract.py`: compensation extraction
- `edgar_client.py`: shared SEC EDGAR client
- `sec_filing_parser.py`: structured SEC filing parser (HTML/XML/iXBRL + optional Arelle path)
- `env_utils.py`: local `.env` loader helpers

## Internal State

This repo is a query engine, not a database. It may create local cache and artifact files while it runs, including:

- `query_context.csv`
- `8k_manifest.csv`
- `8k_results.csv`
- `ceo_comp_results.csv`
- `CEO_MM_DD_YYYY_vN.csv`

These are generated artifacts and are ignored by git.

## Notes

- SEC access is public but rate-limited.
- Extraction is parser-first (deterministic/iXBRL) with Anthropic-assisted fallback where needed.
- Some extraction steps use Anthropic models, so a valid API key is required.
- Exact CEO dates are preferred from 8-K filings; proxy dates are used as fallback evidence when no appointment filing is available.
- A ticker may return a current-CEO transition outcome when a newer recent 5.02 appointment supersedes the proxy CEO.

## Reprocessing

When logic changes or cached filings already exist, force reprocessing:

```bash
python3 extract_8k.py --tickers MSFT --force
python3 ceorater.py MSFT
```
