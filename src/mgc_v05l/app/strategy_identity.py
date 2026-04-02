"""Canonical standalone strategy identity helpers."""

from __future__ import annotations

import re
from typing import Any

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_CAMEL_BOUNDARY_RE = re.compile(r"([a-z0-9])([A-Z])")


def build_standalone_strategy_identity(
    *,
    instrument: Any = None,
    lane_id: Any = None,
    strategy_name: Any = None,
    source_family: Any = None,
    lane_name: Any = None,
    explicit_root: Any = None,
    explicit_id: Any = None,
    explicit_label: Any = None,
    identity_components: Any = None,
    identity_variant: Any = None,
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
    instrument_label = str(instrument or "").strip().upper()
    normalized_components = _normalize_identity_components(identity_components)
    if not normalized_components and identity_variant is not None:
        normalized_variant = _normalize_identity_component(identity_variant, fallback="")
        if normalized_variant:
            normalized_components = (normalized_variant,)
    if not normalized_components and instrument_label:
        normalized_components = (instrument_label,)
    explicit_strategy_id = str(explicit_id or "").strip()
    standalone_strategy_id = explicit_strategy_id or "__".join(
        [identity_root, *normalized_components] if normalized_components else [identity_root]
    )
    standalone_strategy_label = str(explicit_label or "").strip() or standalone_strategy_id
    return {
        "standalone_strategy_id": standalone_strategy_id,
        "standalone_strategy_key": standalone_strategy_id,
        "standalone_strategy_root": identity_root,
        "standalone_strategy_label": standalone_strategy_label,
        "strategy_family": str(source_family or "UNKNOWN").strip() or "UNKNOWN",
        "instrument": instrument_label,
        "lane_id": str(lane_id or "").strip(),
        "identity_components": list(normalized_components),
        "legacy_instrument_derived_identity": not explicit_strategy_id and not bool(identity_components) and bool(instrument_label),
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


def _normalize_identity_components(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    normalized: list[str] = []
    for item in values:
        component = _normalize_identity_component(item, fallback="")
        if component:
            normalized.append(component)
    return tuple(normalized)
