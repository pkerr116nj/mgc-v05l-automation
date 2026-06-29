"""DMC active-authority helpers for Track B artifacts.

These helpers are descriptive filters only. They do not create authority,
broker actions, gates, or blockers; consumers decide how to use the result.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


_TRUE_INACTIVE_FLAGS = ("historical_only", "diagnostic_only", "invalidated_by_current_truth")
_FALSE_INACTIVE_FLAGS = ("current_scope_active",)
_METADATA_KEYS = ("dmc_metadata", "metadata", "current_truth_invalidation")


def is_active_authority_row(row: Mapping[str, Any]) -> bool:
    """Return whether a row should be treated as active current authority."""

    return is_active_authority_payload(row)


def active_authority_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Return only rows that are active current authority."""

    return [row for row in rows if is_active_authority_row(row)]


def is_active_authority_payload(payload: Mapping[str, Any]) -> bool:
    """Return whether a payload should be treated as active current authority."""

    if not payload:
        return True
    for candidate in _authority_flag_sources(payload):
        if _marks_inactive(candidate):
            return False
    return True


def active_authority_classification(
    payload: Mapping[str, Any],
    *,
    classification_key: str = "classification",
    inactive_value: str | None = None,
) -> str | None:
    """Return classification only when the payload is active authority."""

    if not is_active_authority_payload(payload):
        return inactive_value
    value = payload.get(classification_key)
    return str(value or "")


def _authority_flag_sources(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    sources: list[Mapping[str, Any]] = [payload]
    for key in _METADATA_KEYS:
        value = payload.get(key)
        if isinstance(value, Mapping):
            sources.append(value)
    return sources


def _marks_inactive(payload: Mapping[str, Any]) -> bool:
    for key in _TRUE_INACTIVE_FLAGS:
        if payload.get(key) is True:
            return True
    for key in _FALSE_INACTIVE_FLAGS:
        if payload.get(key) is False:
            return True
    return False
