#!/usr/bin/env python3
"""SEC filing parsing helpers.

Best-practice goals:
- detect inline XBRL/iXBRL filings
- parse XHTML/XML with lxml instead of flattening everything as generic HTML
- cache parsed artifacts by accession
- expose structured contexts/facts for downstream extraction
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from lxml import etree

CACHE_DIR = Path(__file__).parent / ".cache" / "parsed_filings"
ARELLE_CACHE_DIR = Path(__file__).parent / ".cache" / "arelle"
CACHE_VERSION = 2


def _local_name(tag: object) -> str:
    if not isinstance(tag, str):
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.split(":", 1)[-1]
    return tag


def detect_parser_mode(raw: str) -> str:
    sample = raw[:20000].lower()
    if "<ix:" in sample or "xmlns:ix=" in sample or "<xbrli:" in sample:
        return "ixbrl"
    if sample.lstrip().startswith("<?xml"):
        return "xml"
    return "html"


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _prefixed_name(el: etree._Element) -> str:
    local = _local_name(el.tag)
    if not local:
        return ""
    qname = etree.QName(el)
    ns = qname.namespace
    prefix = None
    if ns:
        for candidate_prefix, candidate_ns in (el.nsmap or {}).items():
            if candidate_ns == ns and candidate_prefix:
                prefix = candidate_prefix
                break
    if prefix:
        return f"{prefix}:{local}"
    return local


def _extract_lines(root: etree._Element) -> list[str]:
    lines: list[str] = []
    text = "\n".join(t for t in root.itertext())
    for raw in text.splitlines():
        line = _normalize_text(raw)
        if line:
            lines.append(line)
    return lines


def _parse_contexts(root: etree._Element) -> dict[str, dict]:
    contexts: dict[str, dict] = {}
    for el in root.iter():
        if not isinstance(getattr(el, "tag", None), str):
            continue
        if _local_name(el.tag) != "context":
            continue
        ctx_id = el.attrib.get("id", "")
        if not ctx_id:
            continue
        start_date = ""
        end_date = ""
        instant = ""
        dimensions: dict[str, str] = {}
        for child in el.iter():
            if not isinstance(getattr(child, "tag", None), str):
                continue
            local = _local_name(child.tag)
            try:
                text = _normalize_text("".join(child.itertext()))
            except Exception:
                continue
            if local == "startDate":
                start_date = text
            elif local == "endDate":
                end_date = text
            elif local == "instant":
                instant = text
            elif local == "explicitMember":
                dim = child.attrib.get("dimension", "")
                if dim:
                    dimensions[dim] = text
        contexts[ctx_id] = {
            "start_date": start_date,
            "end_date": end_date,
            "instant": instant,
            "dimensions": dimensions,
        }
    return contexts


def _parse_facts(root: etree._Element, contexts: dict[str, dict]) -> list[dict]:
    facts: list[dict] = []
    for el in root.iter():
        if not isinstance(getattr(el, "tag", None), str):
            continue
        local = _local_name(el.tag)
        context_ref = el.attrib.get("contextRef", "")
        if not context_ref:
            continue
        if local in {"context", "unit"}:
            continue
        name = el.attrib.get("name", "") or _prefixed_name(el)
        value = _normalize_text("".join(el.itertext()))
        if not name or not value:
            continue
        facts.append({
            "name": name,
            "local_name": _local_name(el.tag if el.attrib.get("name", "") == "" else name.split(":", 1)[-1]),
            "context_ref": context_ref,
            "value": value,
            "context": contexts.get(context_ref, {}),
            "decimals": el.attrib.get("decimals", ""),
            "format": el.attrib.get("format", ""),
        })
    return facts


def _arelle_cmd() -> str | None:
    exe_dir = Path(sys.executable).resolve().parent
    for candidate in (
        exe_dir / "arelleCmdLine",
        exe_dir / "arellecmdline",
    ):
        if candidate.exists():
            return str(candidate)
    for name in ("arelleCmdLine", "arellecmdline"):
        path = shutil.which(name)
        if path:
            return path
    return None


def _extract_with_arelle(source_path: str | Path, accession: str) -> Path | None:
    cmd = _arelle_cmd()
    if not cmd:
        return None
    source = Path(source_path)
    if not source.exists():
        return None

    ARELLE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe = accession.replace("/", "_")
    out_path = ARELLE_CACHE_DIR / f"{safe}.xbrl"
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    try:
        subprocess.run(
            [cmd, "--file", str(source), "--saveInstance", str(out_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
    except Exception:
        return None

    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path
    return None


def _parse_with_arelle_api(source_path: str | Path) -> tuple[dict[str, dict], list[dict]] | None:
    try:
        from arelle import Cntlr
    except Exception:
        return None

    os.environ.setdefault("XDG_CONFIG_HOME", str((Path(__file__).parent / ".cache").resolve()))
    cntlr = None
    model = None
    try:
        cntlr = Cntlr.Cntlr(hasGui=False, disable_persistent_config=True)
        try:
            cntlr.webCache.workOffline = os.environ.get("ARELLE_OFFLINE", "") == "1"
        except Exception:
            pass
        model = cntlr.modelManager.load(str(Path(source_path).resolve()))
        facts_in_instance = list(getattr(model, "factsInInstance", []) or [])
        if not facts_in_instance:
            return None

        contexts: dict[str, dict] = {}
        arelle_facts: list[dict] = []
        for fact in facts_in_instance:
            ctx = getattr(fact, "context", None)
            ctx_id = getattr(fact, "contextID", "") or getattr(ctx, "id", "")
            if ctx_id and ctx_id not in contexts:
                start_date = ""
                end_date = ""
                instant = ""
                try:
                    if getattr(ctx, "isStartEndPeriod", False):
                        if getattr(ctx, "startDatetime", None):
                            start_date = ctx.startDatetime.date().isoformat()
                        if getattr(ctx, "endDatetime", None):
                            end_date = ctx.endDatetime.date().isoformat()
                    elif getattr(ctx, "isInstantPeriod", False) and getattr(ctx, "instantDatetime", None):
                        instant = ctx.instantDatetime.date().isoformat()
                except Exception:
                    pass
                dimensions: dict[str, str] = {}
                for dim_qname, dim_value in getattr(ctx, "qnameDims", {}).items():
                    try:
                        dimensions[str(dim_qname)] = str(getattr(dim_value, "memberQname", "") or dim_value)
                    except Exception:
                        continue
                contexts[ctx_id] = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "instant": instant,
                    "dimensions": dimensions,
                }

            qname = getattr(fact, "qname", None)
            local_name = getattr(qname, "localName", "") if qname is not None else ""
            prefixed = str(qname) if qname is not None else local_name
            value = _normalize_text(getattr(fact, "value", ""))
            if not prefixed or not ctx_id or not value:
                continue
            arelle_facts.append({
                "name": prefixed,
                "local_name": local_name or prefixed.split(":", 1)[-1],
                "context_ref": ctx_id,
                "value": value,
                "context": contexts.get(ctx_id, {}),
                "decimals": getattr(fact, "decimals", ""),
                "format": "",
            })
        return contexts, arelle_facts
    except Exception:
        return None
    finally:
        try:
            if cntlr and model:
                cntlr.modelManager.close(model)
        except Exception:
            pass


def _cache_path(accession: str) -> Path | None:
    if not accession:
        return None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe = accession.replace("/", "_")
    return CACHE_DIR / f"{safe}.json"


def parse_filing(raw: str, accession: str = "", source_path: str | Path | None = None) -> dict:
    cache_path = _cache_path(accession)
    if cache_path and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("cache_version") == CACHE_VERSION:
                return cached
        except Exception:
            pass

    parser_mode = detect_parser_mode(raw)
    if parser_mode in {"ixbrl", "xml"}:
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(raw.encode("utf-8", errors="replace"), parser=parser)
    else:
        root = etree.HTML(raw)

    lines = _extract_lines(root)
    contexts: dict[str, dict] = {}
    facts: list[dict] = []
    fact_source = "none"

    if parser_mode == "ixbrl":
        arelle_result = _parse_with_arelle_api(source_path) if source_path else None
        if arelle_result is not None:
            contexts, facts = arelle_result
            parser_mode = "ixbrl_arelle"
            fact_source = "arelle"
        else:
            arelle_instance = _extract_with_arelle(source_path, accession) if source_path and accession else None
            if arelle_instance is not None:
                try:
                    parser = etree.XMLParser(recover=True, huge_tree=True)
                    instance_root = etree.fromstring(
                        arelle_instance.read_bytes(), parser=parser
                    )
                    contexts = _parse_contexts(instance_root)
                    facts = _parse_facts(instance_root, contexts)
                    parser_mode = "ixbrl_arelle"
                    fact_source = "arelle_cli"
                except Exception:
                    contexts = _parse_contexts(root)
                    facts = _parse_facts(root, contexts)
                    fact_source = "inline"
            else:
                contexts = _parse_contexts(root)
                facts = _parse_facts(root, contexts)
                fact_source = "inline"
    elif parser_mode == "xml":
        contexts = _parse_contexts(root)
        facts = _parse_facts(root, contexts)
        fact_source = "xml"

    doc = {
        "cache_version": CACHE_VERSION,
        "parser_mode": parser_mode,
        "fact_source": fact_source,
        "lines": lines,
        "contexts": contexts,
        "facts": facts,
    }
    if cache_path:
        try:
            cache_path.write_text(json.dumps(doc), encoding="utf-8")
        except Exception:
            pass
    return doc


def text_lines(raw: str, accession: str = "") -> list[str]:
    return parse_filing(raw, accession=accession).get("lines", [])


def extract_ixbrl_peo_total_comp(parsed: dict) -> list[dict]:
    """Extract PEO total compensation facts from inline XBRL, when present."""
    if parsed.get("parser_mode") != "ixbrl":
        return []

    rows_by_year: dict[int, dict] = {}
    for fact in parsed.get("facts", []):
        if fact.get("name") != "ecd:PeoTotalCompAmt" and fact.get("local_name") != "PeoTotalCompAmt":
            continue
        ctx = fact.get("context") or {}
        end_date = (ctx.get("end_date") or "").strip()
        if not re.match(r"\d{4}-\d{2}-\d{2}$", end_date):
            continue
        try:
            year = int(end_date[:4])
            total = float(fact.get("value", "").replace(",", ""))
        except ValueError:
            continue
        existing = rows_by_year.get(year)
        if existing is None or total > existing["total_comp"]:
            rows_by_year[year] = {
                "fiscal_year": year,
                "total_comp": total,
                "concept": fact.get("name", ""),
                "source": "ixbrl_fact",
            }
    return [rows_by_year[y] for y in sorted(rows_by_year)]


def extract_ixbrl_peo_name(parsed: dict) -> str:
    """Extract the named Principal Executive Officer from inline XBRL, when present."""
    for fact in parsed.get("facts", []):
        if fact.get("name") == "ecd:PeoName" or fact.get("local_name") == "PeoName":
            value = _normalize_text(fact.get("value", ""))
            if value:
                return value
    return ""
