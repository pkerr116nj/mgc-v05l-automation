"""Typed config loading helpers for flat YAML strategy settings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .settings import StrategySettings


_ADDITIVE_JSON_ARRAY_KEYS = {
    "probationary_paper_lanes_json",
}


def load_settings_from_files(paths: Iterable[str | Path]) -> StrategySettings:
    """Load and merge flat YAML config files into typed strategy settings."""
    merged: dict[str, Any] = {}
    for path in paths:
        _merge_flat_yaml(merged, _parse_flat_yaml(Path(path)))
    return StrategySettings.model_validate(merged)


def _merge_flat_yaml(merged: dict[str, Any], overlay: dict[str, Any]) -> None:
    for key, value in overlay.items():
        if key in _ADDITIVE_JSON_ARRAY_KEYS and key in merged:
            merged[key] = _merge_json_array_scalars(merged[key], value, key=key)
            continue
        merged[key] = value


def _parse_flat_yaml(path: Path) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, raw_value = line.partition(":")
        if not _:
            raise ValueError(f"Invalid config line in {path}: {raw_line}")
        values[key.strip()] = _parse_scalar(raw_value.strip())
    return values


def _parse_scalar(raw_value: str) -> Any:
    if raw_value.startswith('"') and raw_value.endswith('"'):
        return raw_value[1:-1]
    if raw_value.startswith("'") and raw_value.endswith("'"):
        return raw_value[1:-1]

    normalized = raw_value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False

    if raw_value.lstrip("-").isdigit():
        return int(raw_value)

    return raw_value


def _merge_json_array_scalars(existing: Any, incoming: Any, *, key: str) -> str:
    existing_rows = _parse_json_array_scalar(existing, key=key)
    incoming_rows = _parse_json_array_scalar(incoming, key=key)
    return json.dumps([*existing_rows, *incoming_rows], separators=(",", ":"))


def _parse_json_array_scalar(value: Any, *, key: str) -> list[Any]:
    if isinstance(value, str):
        parsed = json.loads(value)
    else:
        parsed = value
    if not isinstance(parsed, list):
        raise ValueError(f"Expected JSON array for additive config key {key!r}.")
    return list(parsed)
