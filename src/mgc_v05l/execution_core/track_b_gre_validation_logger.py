"""Diagnostic-only validation logger for Gold Regime Engine outputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, append_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import (
    DEFAULT_OUTPUT_ROOT,
    GOLD_REGIME_OUTPUT_DIR,
    LATEST_GOLD_REGIME_JSON,
)


SCHEMA_VERSION = "track_b_gre_validation_row_v1"
SUMMARY_SCHEMA_VERSION = "track_b_gre_validation_summary_v1"
VALIDATION_ROWS_JSONL = "gre_validation_rows.jsonl"
LATEST_VALIDATION_SUMMARY_JSON = "latest_gre_validation_summary.json"
LATEST_VALIDATION_SUMMARY_MD = "latest_gre_validation_summary.md"
HORIZONS_MINUTES = (5, 15, 30, 60)

PENDING_FORWARD_DATA = "PENDING_FORWARD_DATA"
PARTIAL_FORWARD_DATA = "PARTIAL_FORWARD_DATA"
VALIDATED = "VALIDATED"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass(frozen=True)
class ValidationResult:
    row: dict[str, Any]
    summary: dict[str, Any]
    rows_path: Path
    summary_path: Path
    markdown_path: Path


def run_gre_validation_logger(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    gre_path: Path | None = None,
    max_jsonl_row_bytes: int | None = None,
    max_snapshot_bytes: int | None = None,
) -> ValidationResult:
    """Append one diagnostic validation row for the latest GRE output."""

    generated_at = _coerce_now(now)
    gre_artifact = gre_path or output_root / GOLD_REGIME_OUTPUT_DIR / LATEST_GOLD_REGIME_JSON
    gre_report = _read_json(gre_artifact)
    candles, candle_sources = _load_gold_validation_candles(output_root)
    row = build_gre_validation_row(
        gre_report,
        candles=candles,
        generated_at=generated_at,
        gre_path=gre_artifact,
        candle_sources=candle_sources,
    )
    out_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / VALIDATION_ROWS_JSONL
    jsonl_config = (
        BoundedJsonlConfig(max_row_bytes=max_jsonl_row_bytes)
        if max_jsonl_row_bytes is not None
        else BoundedJsonlConfig()
    )
    append_bounded_jsonl(rows_path, row, config=jsonl_config)
    summary = build_validation_summary(row, rows_path=rows_path, generated_at=generated_at)
    summary_path = out_dir / LATEST_VALIDATION_SUMMARY_JSON
    snapshot_config = (
        BoundedSnapshotConfig(max_bytes=max_snapshot_bytes)
        if max_snapshot_bytes is not None
        else BoundedSnapshotConfig()
    )
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    markdown_path = out_dir / LATEST_VALIDATION_SUMMARY_MD
    markdown_path.write_text(render_validation_summary_markdown(summary), encoding="utf-8")
    return ValidationResult(
        row=row,
        summary=summary,
        rows_path=rows_path,
        summary_path=summary_path,
        markdown_path=markdown_path,
    )


def build_gre_validation_row(
    gre_report: Mapping[str, Any],
    *,
    candles: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    gre_path: Path | str,
    candle_sources: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    gre_generated_at = _parse_datetime(gre_report.get("generated_at"))
    label = str(gre_report.get("regime_label") or "UNKNOWN")
    direction = str(gre_report.get("directional_bias") or "UNKNOWN")
    source_refs = dict(gre_report.get("source_refs") or {})
    source_refs["gre_report"] = str(gre_path)
    for key, value in (candle_sources or {}).items():
        source_refs[f"validation_{key}"] = value
    forward = _forward_metrics(
        candles,
        gre_generated_at=gre_generated_at,
        regime_label=label,
        directional_bias=direction,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "gre_generated_at": gre_generated_at.isoformat() if gre_generated_at else None,
        "instrument": gre_report.get("instrument") or "GOLD",
        "contract": _contract_from_report(gre_report),
        "session": gre_report.get("session_label"),
        "regime_label": label,
        "confidence": _bounded_int(gre_report.get("confidence"), low=0, high=100),
        "directional_bias": direction,
        "diagnostic_only": True,
        "positive_evidence": list(gre_report.get("positive_evidence") or []),
        "negative_evidence": list(gre_report.get("negative_evidence") or []),
        "conflicts": list(gre_report.get("conflicting_evidence") or []),
        "missing_providers": list(gre_report.get("missing_evidence") or []),
        "source_refs": source_refs,
        "forward_returns": forward["forward_returns"],
        "mfe": forward["mfe"],
        "mae": forward["mae"],
        "best_observed_holding_window": forward["best_observed_holding_window"],
        "direction_correctness": forward["direction_correctness"],
        "validation_status": forward["validation_status"],
        "available_horizons": forward["available_horizons"],
        "missing_horizons": forward["missing_horizons"],
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "submit_allowed": False,
        "order_intent_created": False,
        "broker_state_mutated": False,
    }


def build_validation_summary(
    latest_row: Mapping[str, Any],
    *,
    rows_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    rows = _read_jsonl_tail(rows_path, limit=500)
    status_counts: dict[str, int] = {}
    label_counts: dict[str, int] = {}
    directional_rows = 0
    correct_rows = 0
    for row in rows:
        status = str(row.get("validation_status") or "UNKNOWN")
        label = str(row.get("regime_label") or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        label_counts[label] = label_counts.get(label, 0) + 1
        correctness = row.get("direction_correctness")
        if correctness in {True, False}:
            directional_rows += 1
            correct_rows += 1 if correctness is True else 0
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "rows_path": str(rows_path),
        "row_sample_count": len(rows),
        "validation_status_counts": status_counts,
        "regime_label_counts": label_counts,
        "directional_correctness_rate": round(correct_rows / directional_rows, 4) if directional_rows else None,
        "latest_validation": {
            "gre_generated_at": latest_row.get("gre_generated_at"),
            "regime_label": latest_row.get("regime_label"),
            "confidence": latest_row.get("confidence"),
            "validation_status": latest_row.get("validation_status"),
            "available_horizons": latest_row.get("available_horizons"),
            "missing_horizons": latest_row.get("missing_horizons"),
            "direction_correctness": latest_row.get("direction_correctness"),
            "best_observed_holding_window": latest_row.get("best_observed_holding_window"),
        },
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
    }


def render_validation_summary_markdown(summary: Mapping[str, Any]) -> str:
    latest = summary.get("latest_validation") if isinstance(summary.get("latest_validation"), Mapping) else {}
    return (
        "# GRE Validation Summary\n\n"
        f"Generated: {summary.get('generated_at')}\n\n"
        f"Diagnostic only: `{summary.get('diagnostic_only')}`\n\n"
        f"Rows sampled: `{summary.get('row_sample_count')}`\n\n"
        f"Status counts: `{summary.get('validation_status_counts')}`\n\n"
        f"Regime counts: `{summary.get('regime_label_counts')}`\n\n"
        f"Directional correctness rate: `{summary.get('directional_correctness_rate')}`\n\n"
        "## Latest Validation\n\n"
        f"- GRE generated at: `{latest.get('gre_generated_at')}`\n"
        f"- Regime: `{latest.get('regime_label')}`\n"
        f"- Confidence: `{latest.get('confidence')}`\n"
        f"- Status: `{latest.get('validation_status')}`\n"
        f"- Available horizons: `{latest.get('available_horizons')}`\n"
        f"- Missing horizons: `{latest.get('missing_horizons')}`\n"
        f"- Direction correctness: `{latest.get('direction_correctness')}`\n"
        f"- Best observed holding window: `{latest.get('best_observed_holding_window')}`\n"
    )


def _forward_metrics(
    candles: Sequence[Mapping[str, Any]],
    *,
    gre_generated_at: datetime | None,
    regime_label: str,
    directional_bias: str,
) -> dict[str, Any]:
    if gre_generated_at is None:
        return _empty_forward(INSUFFICIENT_DATA, missing=list(HORIZONS_MINUTES))
    normalized = [row for row in (_normalize_bar(item) for item in candles) if row is not None]
    if not normalized:
        return _empty_forward(PENDING_FORWARD_DATA, missing=list(HORIZONS_MINUTES))
    reference = _reference_bar(normalized, gre_generated_at)
    if reference is None:
        return _empty_forward(INSUFFICIENT_DATA, missing=list(HORIZONS_MINUTES))
    ref_close = float(reference["close"])
    forward_returns: dict[str, float] = {}
    available: list[int] = []
    missing: list[int] = []
    horizon_bars: dict[int, Mapping[str, Any]] = {}
    for horizon in HORIZONS_MINUTES:
        target = gre_generated_at + timedelta(minutes=horizon)
        bar = _first_bar_at_or_after(normalized, target)
        if bar is None:
            missing.append(horizon)
            continue
        available.append(horizon)
        horizon_bars[horizon] = bar
        forward_returns[f"{horizon}m"] = round(float(bar["close"]) - ref_close, 6)
    future_rows = [row for row in normalized if gre_generated_at < row["timestamp"] <= gre_generated_at + timedelta(minutes=60)]
    if future_rows:
        max_high = max(float(row["high"]) for row in future_rows)
        min_low = min(float(row["low"]) for row in future_rows)
        mfe, mae = _mfe_mae(regime_label, directional_bias, ref_close, max_high, min_low)
    else:
        mfe = None
        mae = None
    best_window = _best_window(forward_returns, regime_label, directional_bias)
    correctness = _direction_correctness(forward_returns, regime_label, directional_bias)
    if len(available) == len(HORIZONS_MINUTES):
        status = VALIDATED
    elif available:
        status = PARTIAL_FORWARD_DATA
    else:
        status = PENDING_FORWARD_DATA
    return {
        "validation_status": status,
        "forward_returns": forward_returns,
        "available_horizons": [f"{item}m" for item in available],
        "missing_horizons": [f"{item}m" for item in missing],
        "mfe": mfe,
        "mae": mae,
        "best_observed_holding_window": best_window,
        "direction_correctness": correctness,
    }


def _empty_forward(status: str, *, missing: Sequence[int]) -> dict[str, Any]:
    return {
        "validation_status": status,
        "forward_returns": {},
        "available_horizons": [],
        "missing_horizons": [f"{item}m" for item in missing],
        "mfe": None,
        "mae": None,
        "best_observed_holding_window": None,
        "direction_correctness": None,
    }


def _mfe_mae(regime_label: str, directional_bias: str, ref_close: float, max_high: float, min_low: float) -> tuple[float | None, float | None]:
    direction = _direction_sign(regime_label, directional_bias)
    if direction is None:
        return None, None
    if direction > 0:
        return round(max_high - ref_close, 6), round(min_low - ref_close, 6)
    return round(ref_close - min_low, 6), round(ref_close - max_high, 6)


def _best_window(forward_returns: Mapping[str, float], regime_label: str, directional_bias: str) -> str | None:
    direction = _direction_sign(regime_label, directional_bias)
    if direction is None or not forward_returns:
        return None
    scored = {key: direction * value for key, value in forward_returns.items()}
    return max(scored, key=lambda key: scored[key])


def _direction_correctness(forward_returns: Mapping[str, float], regime_label: str, directional_bias: str) -> bool | None:
    direction = _direction_sign(regime_label, directional_bias)
    if direction is None or not forward_returns:
        return None
    preferred = forward_returns.get("60m")
    if preferred is None:
        longest_key = sorted(forward_returns, key=lambda item: int(item.removesuffix("m")))[-1]
        preferred = forward_returns[longest_key]
    return bool(direction * preferred > 0)


def _direction_sign(regime_label: str, directional_bias: str) -> int | None:
    if regime_label == "LONG" or directional_bias == "BULLISH":
        return 1
    if regime_label == "SHORT" or directional_bias == "BEARISH":
        return -1
    return None


def _load_gold_validation_candles(output_root: Path) -> tuple[tuple[dict[str, Any], ...], dict[str, str]]:
    sources: dict[str, str] = {}
    for symbol in ("GC", "MGC"):
        path = output_root / "phase1_runtime_market_data" / symbol / "1m" / "latest_runtime_candles.json"
        payload = _read_json(path)
        bars = payload.get("bars") or payload.get("candles") or []
        if isinstance(bars, Sequence) and not isinstance(bars, (str, bytes)) and bars:
            sources[f"{symbol}_1m"] = str(path)
            return tuple(dict(row) for row in bars if isinstance(row, Mapping)), sources
    return (), sources


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _read_jsonl_tail(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines[-limit:]:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _normalize_bar(row: Mapping[str, Any]) -> dict[str, Any] | None:
    timestamp = _parse_datetime(row.get("bar_end") or row.get("timestamp") or row.get("ts") or row.get("bar_start"))
    if timestamp is None:
        return None
    try:
        return {
            "timestamp": timestamp,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _reference_bar(rows: Sequence[Mapping[str, Any]], timestamp: datetime) -> Mapping[str, Any] | None:
    candidates = [row for row in rows if row["timestamp"] <= timestamp]
    if candidates:
        return candidates[-1]
    return rows[0] if rows else None


def _first_bar_at_or_after(rows: Sequence[Mapping[str, Any]], timestamp: datetime) -> Mapping[str, Any] | None:
    for row in rows:
        if row["timestamp"] >= timestamp:
            return row
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return datetime.now(UTC)


def _contract_from_report(gre_report: Mapping[str, Any]) -> str | None:
    symbols = gre_report.get("symbols")
    if isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes)) and symbols:
        return str(symbols[0])
    return None


def _bounded_int(value: Any, *, low: int, high: int) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(low, min(high, parsed))
