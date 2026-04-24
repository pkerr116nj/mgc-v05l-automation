"""Stable JSON rendering for validation reports."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .models import ValidationReport


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def render_json_report(report: ValidationReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True, default=_json_default) + "\n"
