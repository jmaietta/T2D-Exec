#!/usr/bin/env python3
"""Minimal environment loading helpers.

Loads a local .env file even when python-dotenv is not installed.
"""

from __future__ import annotations

import os
from pathlib import Path


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _load_env_file(dotenv_path: Path, override: bool = True) -> bool:
    if not dotenv_path.exists():
        return False

    loaded_any = False
    for raw_line in dotenv_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        if not override and key in os.environ:
            continue

        os.environ[key] = _strip_quotes(value)
        loaded_any = True

    return loaded_any


def load_local_env(dotenv_path: Path, override: bool = True) -> bool:
    """Load .env using python-dotenv if available, else use a minimal parser."""
    try:
        from dotenv import load_dotenv as _load_dotenv
    except ImportError:
        return _load_env_file(dotenv_path, override=override)
    return bool(_load_dotenv(dotenv_path=dotenv_path, override=override))
