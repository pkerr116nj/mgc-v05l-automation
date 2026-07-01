"""Historical diagnostic observation generator for the Gold Regime Engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import (
    DEFAULT_OUTPUT_ROOT,
    GOLD_REGIME_OUTPUT_DIR,
    GoldRegimePlugin,
    RegimeEngineContext,
)
from mgc_v05l.execution_core.track_b_gre_research_analyzer import build_gre_research_analyzer, render_gre_research_analyzer_markdown
from mgc_v05l.execution_core.track_b_gre_validation_logger import (
    HORIZONS_MINUTES,
    build_gre_validation_row,
    build_validation_summary,
    render_validation_summary_markdown,
)
from mgc_v05l.execution_core.track_b_gre_validation_scorecard import (
    build_gre_validation_scorecard,
    render_gre_scorecard_markdown,
)


SCHEMA_VERSION = "track_b_gre_historical_backfill_v1"
BACKFILL_ROWS_JSONL = "gre_backfill_observations.jsonl"
LATEST_BACKFILL_SUMMARY_JSON = "latest_gre_backfill_summary.json"
LATEST_BACKFILL_SUMMARY_MD = "latest_gre_backfill_summary.md"
LATEST_BACKFILL_SCORECARD_JSON = "latest_gre_backfill_scorecard.json"
LATEST_BACKFILL_SCORECARD_MD = "latest_gre_backfill_scorecard.md"
LATEST_BACKFILL_ANALYZER_JSON = "latest_gre_backfill_analyzer.json"
LATEST_BACKFILL_ANALYZER_MD = "latest_gre_backfill_analyzer.md"


@dataclass(frozen=True)
class BackfillResult:
    summary: dict[str, Any]
    scorecard: dict[str, Any]
    analyzer: dict[str, Any]
    rows_path: Path
    summary_path: Path
    scorecard_path: Path
    analyzer_path: Path


def run_gre_historical_backfill(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    cadence_minutes: int = 5,
    max_observations: int = 200,
    max_jsonl_row_bytes: int | None = None,
    max_snapshot_bytes: int | None = None,
) -> BackfillResult:
    generated_at = _coerce_now(now)
    if cadence_minutes <= 0:
        raise ValueError("cadence_minutes must be positive")
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    gold_dir.mkdir(parents=True, exist_ok=True)
    candles_by_symbol = _load_gold_candles(output_root)
    rows = generate_backfill_observations(
        candles_by_symbol=candles_by_symbol,
        generated_at=generated_at,
        cadence_minutes=cadence_minutes,
        max_observations=max_observations,
        output_root=output_root,
    )
    rows_path = gold_dir / BACKFILL_ROWS_JSONL
    jsonl_config = (
        BoundedJsonlConfig(max_row_bytes=max_jsonl_row_bytes)
        if max_jsonl_row_bytes is not None
        else BoundedJsonlConfig()
    )
    write_bounded_jsonl(rows_path, rows, config=jsonl_config)
    summary = build_backfill_summary(
        rows,
        candles_by_symbol=candles_by_symbol,
        generated_at=generated_at,
        cadence_minutes=cadence_minutes,
        rows_path=rows_path,
    )
    snapshot_config = (
        BoundedSnapshotConfig(max_bytes=max_snapshot_bytes)
        if max_snapshot_bytes is not None
        else BoundedSnapshotConfig()
    )
    summary_path = gold_dir / LATEST_BACKFILL_SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    (gold_dir / LATEST_BACKFILL_SUMMARY_MD).write_text(render_backfill_summary_markdown(summary), encoding="utf-8")
    scorecard = build_gre_validation_scorecard(rows, generated_at=generated_at, rows_path=rows_path)
    scorecard_path = gold_dir / LATEST_BACKFILL_SCORECARD_JSON
    write_bounded_snapshot_json(scorecard_path, scorecard, config=snapshot_config)
    (gold_dir / LATEST_BACKFILL_SCORECARD_MD).write_text(render_gre_scorecard_markdown(scorecard), encoding="utf-8")
    validation_summary = build_validation_summary(rows[-1] if rows else {}, rows_path=rows_path, generated_at=generated_at)
    analyzer = build_gre_research_analyzer(
        gre_report=_gre_report_from_backfill_row(rows[-1]) if rows else {},
        scorecard=scorecard,
        validation_summary=validation_summary,
        validation_rows=rows,
        generated_at=generated_at,
    )
    analyzer["backfill"] = True
    analyzer_path = gold_dir / LATEST_BACKFILL_ANALYZER_JSON
    write_bounded_snapshot_json(analyzer_path, analyzer, config=snapshot_config)
    (gold_dir / LATEST_BACKFILL_ANALYZER_MD).write_text(render_gre_research_analyzer_markdown(analyzer), encoding="utf-8")
    return BackfillResult(
        summary=summary,
        scorecard=scorecard,
        analyzer=analyzer,
        rows_path=rows_path,
        summary_path=summary_path,
        scorecard_path=scorecard_path,
        analyzer_path=analyzer_path,
    )


def generate_backfill_observations(
    *,
    candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    generated_at: datetime,
    cadence_minutes: int,
    max_observations: int,
    output_root: Path | str,
) -> list[dict[str, Any]]:
    primary_1m = tuple(candles_by_symbol.get("GC", {}).get("1m") or candles_by_symbol.get("MGC", {}).get("1m") or ())
    primary_5m = tuple(candles_by_symbol.get("GC", {}).get("5m") or candles_by_symbol.get("MGC", {}).get("5m") or ())
    if not primary_1m or not primary_5m:
        return []
    candidate_times = _candidate_times(primary_1m, cadence_minutes=cadence_minutes)
    rows: list[dict[str, Any]] = []
    for observation_time in candidate_times:
        context_candles = _slice_context(candles_by_symbol, observation_time=observation_time)
        if len(context_candles.get("GC", {}).get("1m", ())) < 8 and len(context_candles.get("MGC", {}).get("1m", ())) < 8:
            continue
        report = GoldRegimePlugin().evaluate(
            RegimeEngineContext(
                instrument="GOLD",
                symbols=("GC", "MGC"),
                candles_by_symbol_timeframe=context_candles,
                source_refs=_source_refs(output_root),
                analytics={},
                generated_at=observation_time,
            )
        )
        row = build_gre_validation_row(
            report,
            candles=primary_1m,
            generated_at=generated_at,
            gre_path="BACKFILL_GENERATED_GRE_OBSERVATION",
            candle_sources={"backfill_GC_1m": str(Path(output_root) / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json")},
        )
        row["backfill"] = True
        row["observation_cadence_minutes"] = cadence_minutes
        row["lookahead_safe"] = True
        row["classification_candle_max_ts"] = _max_context_timestamp(context_candles)
        row["source_mode"] = "HISTORICAL_GRE_BACKFILL_FROM_RETAINED_CANDLES"
        rows.append(row)
        if len(rows) >= max_observations:
            break
    return rows


def build_backfill_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    generated_at: datetime,
    cadence_minutes: int,
    rows_path: Path,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("validation_status") or "UNKNOWN")
        regime = str(row.get("regime_label") or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    windows = _coverage_windows(candles_by_symbol)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "backfill": True,
        "rows_path": str(rows_path),
        "observation_count": len(rows),
        "validated_observations": status_counts.get("VALIDATED", 0),
        "partial_observations": status_counts.get("PARTIAL_FORWARD_DATA", 0),
        "pending_observations": status_counts.get("PENDING_FORWARD_DATA", 0),
        "insufficient_observations": status_counts.get("INSUFFICIENT_DATA", 0),
        "cadence_minutes": cadence_minutes,
        "status_counts": status_counts,
        "regime_counts": regime_counts,
        "available_windows": windows,
        "coverage_assessment": _coverage_assessment(rows, windows),
        "lookahead_bias_guard": "classification uses only candles at or before observation timestamp; validation uses later candles only",
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def render_backfill_summary_markdown(summary: Mapping[str, Any]) -> str:
    return (
        "# GRE Historical Backfill Summary\n\n"
        f"Generated: {summary.get('generated_at')}\n\n"
        f"Observation count: `{summary.get('observation_count')}`\n\n"
        f"Validated observations: `{summary.get('validated_observations')}`\n\n"
        f"Cadence minutes: `{summary.get('cadence_minutes')}`\n\n"
        f"Coverage: {summary.get('coverage_assessment')}\n\n"
        f"Status counts: `{summary.get('status_counts')}`\n\n"
        f"Regime counts: `{summary.get('regime_counts')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def _load_gold_candles(output_root: Path) -> dict[str, dict[str, tuple[dict[str, Any], ...]]]:
    result: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    for symbol in ("GC", "MGC"):
        result[symbol] = {}
        for timeframe in ("1m", "5m"):
            path = output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
            payload = _read_json(path)
            result[symbol][timeframe] = _extract_bars(payload)
    return result


def _extract_bars(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    rows = payload.get("bars") or payload.get("candles") or []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return ()
    normalized = [_normalize_bar(row) for row in rows if isinstance(row, Mapping)]
    return tuple(row for row in normalized if row is not None)


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
            "volume": row.get("volume"),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _candidate_times(rows: Sequence[Mapping[str, Any]], *, cadence_minutes: int) -> list[datetime]:
    times: list[datetime] = []
    last: datetime | None = None
    for row in rows:
        timestamp = row["timestamp"]
        if last is None or timestamp >= last + timedelta(minutes=cadence_minutes):
            times.append(timestamp)
            last = timestamp
    return times


def _slice_context(
    candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    *,
    observation_time: datetime,
) -> dict[str, dict[str, tuple[dict[str, Any], ...]]]:
    sliced: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    for symbol, by_timeframe in candles_by_symbol.items():
        sliced[symbol] = {}
        for timeframe, rows in by_timeframe.items():
            historical = tuple(dict(row) for row in rows if row["timestamp"] <= observation_time)
            sliced[symbol][timeframe] = historical
    return sliced


def _max_context_timestamp(candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]]) -> str | None:
    timestamps = [
        row["timestamp"]
        for by_timeframe in candles_by_symbol.values()
        for rows in by_timeframe.values()
        for row in rows
        if isinstance(row.get("timestamp"), datetime)
    ]
    return max(timestamps).isoformat() if timestamps else None


def _coverage_windows(candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]]) -> dict[str, Any]:
    windows: dict[str, Any] = {}
    for symbol, by_timeframe in candles_by_symbol.items():
        windows[symbol] = {}
        for timeframe, rows in by_timeframe.items():
            if not rows:
                windows[symbol][timeframe] = {"bar_count": 0}
                continue
            windows[symbol][timeframe] = {
                "bar_count": len(rows),
                "first_ts": rows[0]["timestamp"].isoformat(),
                "last_ts": rows[-1]["timestamp"].isoformat(),
            }
    return windows


def _coverage_assessment(rows: Sequence[Mapping[str, Any]], windows: Mapping[str, Any]) -> str:
    if not rows:
        return "INSUFFICIENT_RETAINED_HISTORY_FOR_BACKFILL"
    if len(rows) < 20:
        return "SHALLOW_RETAINED_HISTORY_DIAGNOSTIC_ONLY"
    return "BACKFILL_OBSERVATIONS_AVAILABLE_FROM_RETAINED_CANDLES"


def _source_refs(output_root: Path | str) -> dict[str, str]:
    root = Path(output_root)
    return {
        "phase1_GC_1m": str(root / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json"),
        "phase1_GC_5m": str(root / "phase1_runtime_market_data" / "GC" / "5m" / "latest_runtime_candles.json"),
        "phase1_MGC_1m": str(root / "phase1_runtime_market_data" / "MGC" / "1m" / "latest_runtime_candles.json"),
        "phase1_MGC_5m": str(root / "phase1_runtime_market_data" / "MGC" / "5m" / "latest_runtime_candles.json"),
    }


def _gre_report_from_backfill_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "track_b_gold_regime_engine_v1",
        "generated_at": row.get("gre_generated_at"),
        "plugin_id": "GRE",
        "instrument": row.get("instrument") or "GOLD",
        "symbols": ["GC", "MGC"],
        "session_label": row.get("session"),
        "regime_label": row.get("regime_label"),
        "confidence": row.get("confidence"),
        "directional_bias": row.get("directional_bias"),
        "diagnostic_only": True,
        "positive_evidence": list(row.get("positive_evidence") or []),
        "negative_evidence": list(row.get("negative_evidence") or []),
        "conflicting_evidence": list(row.get("conflicts") or []),
        "missing_evidence": list(row.get("missing_providers") or []),
        "source_refs": dict(row.get("source_refs") or {}),
    }


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = _parse_datetime(value)
    return parsed if parsed else datetime.now(UTC)
