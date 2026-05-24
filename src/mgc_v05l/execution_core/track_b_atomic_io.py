"""Shared atomic file helpers for Track B execution_core artifacts."""

from __future__ import annotations

import json
import os
import threading
import uuid
from pathlib import Path
from typing import Any, Mapping


def unique_atomic_temp_path(path: Path) -> Path:
    """Return a same-directory temp path unique to this process/thread/write."""

    return path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp")


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> Path:
    """Write JSON with a unique temp file and atomic same-directory replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = unique_atomic_temp_path(path)
    try:
        tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
    return path
