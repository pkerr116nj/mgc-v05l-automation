"""Canonical standalone strategy identity helpers."""

from __future__ import annotations

import re
from typing import Any

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_CAMEL_BOUNDARY_RE = re.compile(r"([a-z0-9])([A-Z])")


def build_standalone_strategy_identity(
    *,
    instrument: Any,
    lane_id: Any = None,
    strategy_name: Any = None,
    source_family: Any = None,
    lane_name: Any = None,
    explicit_root: Any = None,
) -> dict[str, str]:
    identity_root_source = (
        explicit_root
        or lane_name
        or _preferred_family_label(source_family)
        or _preferred_slug_label(strategy_name)
        or lane_id
        or "unknown_strategy"
    )
    identity_root = _normalize_identity_component(identity_root_source, fallback="unknown_strategy")
    instrument_label = str(instrument or "UNKNOWN").strip().upper() or "UNKNOWN"
    standalone_strategy_id = f"{identity_root}__{instrument_label}"
    return {
        "standalone_strategy_id": standalone_strategy_id,
        "standalone_strategy_key": standalone_strategy_id,
        "standalone_strategy_root": identity_root,
        "standalone_strategy_label": f"{identity_root}__{instrument_label}",
        "strategy_family": str(source_family or "UNKNOWN").strip() or "UNKNOWN",
        "instrument": instrument_label,
        "lane_id": str(lane_id or "").strip(),
    }


def _preferred_family_label(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.upper() == "UNKNOWN":
        return None
    return text


def _preferred_slug_label(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if "/" in text:
        return None
    return text


def _normalize_identity_component(value: Any, *, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    text = _CAMEL_BOUNDARY_RE.sub(r"\1_\2", text)
    normalized = _NON_ALNUM_RE.sub("_", text.lower()).strip("_")
    return normalized or fallback
