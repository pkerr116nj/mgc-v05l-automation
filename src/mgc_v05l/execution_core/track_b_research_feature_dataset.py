"""Canonical Research Feature Dataset MVP for Track B research."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.session_phase_labels import label_session_phase, phase_coarse_session_group


SCHEMA_VERSION = "track_b_research_feature_dataset_v1"
SUMMARY_SCHEMA_VERSION = "track_b_research_feature_dataset_summary_v1"
RESEARCH_FEATURE_DATASET_DIR = Path("research") / "canonical_research_feature_dataset"
RESEARCH_FEATURE_DATASET_JSONL = "research_feature_dataset.jsonl"
LATEST_RESEARCH_FEATURE_DATASET_SUMMARY_JSON = "latest_research_feature_dataset_summary.json"
LATEST_RESEARCH_FEATURE_DATASET_SUMMARY_MD = "latest_research_feature_dataset_summary.md"
RESEARCH_FEATURE_DATASET_SCHEMA_MD = "research_feature_dataset_schema.md"
RESEARCH_FEATURE_DATASET_MIGRATION_NOTES_MD = "research_feature_dataset_migration_notes.md"
DEFAULT_INSTRUMENTS = ("GC", "MGC")
DEFAULT_TIMEFRAME = "1m"
HORIZONS_MINUTES = (5, 15, 30, 60)


@dataclass(frozen=True)
class ResearchFeatureDatasetResult:
    rows: tuple[dict[str, Any], ...]
    summary: dict[str, Any]
    rows_path: Path
    summary_path: Path
    summary_markdown_path: Path
    schema_path: Path
    migration_notes_path: Path


def run_research_feature_dataset_builder(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    instruments: Sequence[str] = DEFAULT_INSTRUMENTS,
    timeframe: str = DEFAULT_TIMEFRAME,
    cadence_minutes: int = 5,
    max_rows: int = 500,
    backfill: bool = True,
    max_jsonl_row_bytes: int | None = None,
    max_snapshot_bytes: int | None = None,
) -> ResearchFeatureDatasetResult:
    """Build and publish the diagnostic-only canonical research feature dataset."""

    if cadence_minutes <= 0:
        raise ValueError("cadence_minutes must be positive")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    generated_at = _coerce_now(now)
    dataset_dir = output_root / RESEARCH_FEATURE_DATASET_DIR
    dataset_dir.mkdir(parents=True, exist_ok=True)
    candles_by_symbol = _load_candles(output_root, instruments=instruments, timeframe=timeframe)
    auxiliary_sources = _detect_auxiliary_sources(output_root)
    rows = tuple(
        build_research_feature_rows(
            candles_by_symbol=candles_by_symbol,
            generated_at=generated_at,
            timeframe=timeframe,
            cadence_minutes=cadence_minutes,
            max_rows=max_rows,
            backfill=backfill,
            output_root=output_root,
            auxiliary_sources=auxiliary_sources,
        )
    )
    rows_path = dataset_dir / RESEARCH_FEATURE_DATASET_JSONL
    jsonl_config = (
        BoundedJsonlConfig(max_row_bytes=max_jsonl_row_bytes)
        if max_jsonl_row_bytes is not None
        else BoundedJsonlConfig()
    )
    write_bounded_jsonl(rows_path, rows, config=jsonl_config)
    summary = build_research_feature_dataset_summary(
        rows,
        candles_by_symbol=candles_by_symbol,
        generated_at=generated_at,
        timeframe=timeframe,
        cadence_minutes=cadence_minutes,
        rows_path=rows_path,
        auxiliary_sources=auxiliary_sources,
    )
    snapshot_config = (
        BoundedSnapshotConfig(max_bytes=max_snapshot_bytes)
        if max_snapshot_bytes is not None
        else BoundedSnapshotConfig()
    )
    summary_path = dataset_dir / LATEST_RESEARCH_FEATURE_DATASET_SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    summary_markdown_path = dataset_dir / LATEST_RESEARCH_FEATURE_DATASET_SUMMARY_MD
    summary_markdown_path.write_text(render_research_feature_dataset_summary_markdown(summary), encoding="utf-8")
    schema_path = dataset_dir / RESEARCH_FEATURE_DATASET_SCHEMA_MD
    schema_path.write_text(render_research_feature_dataset_schema_markdown(), encoding="utf-8")
    migration_notes_path = dataset_dir / RESEARCH_FEATURE_DATASET_MIGRATION_NOTES_MD
    migration_notes_path.write_text(render_research_feature_dataset_migration_notes_markdown(), encoding="utf-8")
    return ResearchFeatureDatasetResult(
        rows=rows,
        summary=summary,
        rows_path=rows_path,
        summary_path=summary_path,
        summary_markdown_path=summary_markdown_path,
        schema_path=schema_path,
        migration_notes_path=migration_notes_path,
    )


def build_research_feature_rows(
    *,
    candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    generated_at: datetime,
    timeframe: str,
    cadence_minutes: int,
    max_rows: int,
    backfill: bool,
    output_root: Path | str,
    auxiliary_sources: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sources = dict(auxiliary_sources or {})
    for symbol in sorted(candles_by_symbol):
        candles = tuple(candles_by_symbol.get(symbol) or ())
        vwap_by_index = _session_vwap_by_index(candles, timeframe=timeframe)
        for idx in _candidate_indexes(candles, cadence_minutes=cadence_minutes):
            candle = candles[idx]
            row = _build_row(
                symbol=symbol,
                candles=candles,
                index=idx,
                vwap_state=vwap_by_index.get(idx, {}),
                generated_at=generated_at,
                timeframe=timeframe,
                cadence_minutes=cadence_minutes,
                backfill=backfill,
                output_root=Path(output_root),
                auxiliary_sources=sources,
            )
            rows.append(row)
    rows.sort(key=lambda item: (str(item.get("observation_time")), str(item.get("contract"))))
    return rows[:max_rows]


def build_research_feature_dataset_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    generated_at: datetime,
    timeframe: str,
    cadence_minutes: int,
    rows_path: Path,
    auxiliary_sources: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sessions: dict[str, int] = {}
    instruments: set[str] = set()
    contracts: set[str] = set()
    available_features: set[str] = set()
    missing_features: set[str] = set()
    forward_coverage = {f"{horizon}m": 0 for horizon in HORIZONS_MINUTES}
    vwap_summary = _empty_vwap_summary()
    for row in rows:
        sessions[str(row.get("session") or "UNKNOWN")] = sessions.get(str(row.get("session") or "UNKNOWN"), 0) + 1
        instruments.add(str(row.get("instrument") or "UNKNOWN"))
        contracts.add(str(row.get("contract") or "UNKNOWN"))
        for key, value in dict(row.get("feature_availability") or {}).items():
            (available_features if value else missing_features).add(str(key))
        for key, value in dict(row.get("forward_returns") or {}).items():
            if value is not None:
                forward_coverage[str(key)] = forward_coverage.get(str(key), 0) + 1
        _update_vwap_summary(vwap_summary, row)
    windows = _coverage_windows(candles_by_symbol)
    first_observation, last_observation = _observation_window(rows)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "backfill": True,
        "rows_path": str(rows_path),
        "observation_count": len(rows),
        "timeframe": timeframe,
        "cadence_minutes": cadence_minutes,
        "time_coverage": {
            "first_observation_time": first_observation,
            "last_observation_time": last_observation,
            "coverage_minutes": _coverage_minutes(first_observation, last_observation),
        },
        "instruments": sorted(instruments),
        "contracts": sorted(contracts),
        "sessions": dict(sorted(sessions.items())),
        "available_features": sorted(available_features),
        "missing_features": sorted(missing_features),
        "forward_outcome_coverage": forward_coverage,
        "mfe_mae_available_count": sum(1 for row in rows if row.get("mfe") is not None and row.get("mae") is not None),
        "vwap_coverage": _finalize_vwap_summary(vwap_summary),
        "source_windows": windows,
        "auxiliary_sources": dict(auxiliary_sources or {}),
        "readiness_for_gre": _readiness_for_gre(rows, windows),
        "readiness_for_future_plugins": _future_plugin_readiness(contracts),
        "schema_notes_path": str(rows_path.with_name(RESEARCH_FEATURE_DATASET_SCHEMA_MD)),
        "migration_notes_path": str(rows_path.with_name(RESEARCH_FEATURE_DATASET_MIGRATION_NOTES_MD)),
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def render_research_feature_dataset_summary_markdown(summary: Mapping[str, Any]) -> str:
    return (
        "# Canonical Research Feature Dataset Summary\n\n"
        f"Generated: {summary.get('generated_at')}\n\n"
        f"Observation count: `{summary.get('observation_count')}`\n\n"
        f"Timeframe: `{summary.get('timeframe')}`\n\n"
        f"Cadence minutes: `{summary.get('cadence_minutes')}`\n\n"
        f"Coverage: `{summary.get('time_coverage')}`\n\n"
        f"Instruments: `{summary.get('instruments')}`\n\n"
        f"Contracts: `{summary.get('contracts')}`\n\n"
        f"Sessions: `{summary.get('sessions')}`\n\n"
        f"Available features: `{summary.get('available_features')}`\n\n"
        f"Missing features: `{summary.get('missing_features')}`\n\n"
        f"Forward outcome coverage: `{summary.get('forward_outcome_coverage')}`\n\n"
        f"VWAP coverage: `{summary.get('vwap_coverage')}`\n\n"
        f"Readiness for GRE: `{summary.get('readiness_for_gre')}`\n\n"
        f"Future plugin readiness: `{summary.get('readiness_for_future_plugins')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def render_research_feature_dataset_schema_markdown() -> str:
    return """# Canonical Research Feature Dataset Schema

Each `research_feature_dataset.jsonl` row is one diagnostic observation timestamp.

Required groups:

- General: `schema_version`, `generated_at`, `observation_time`.
- Identity: `instrument`, `contract`, `session`, `session_label`, `timeframe`.
- Market: `open`, `high`, `low`, `close`, `volume`.
- Derived: `candle_body`, `candle_range`, `direction`, `trend_lookback`, `feature_availability`.
- VWAP: `has_vwap`, `vwap`, `distance_from_vwap_points`, `distance_from_vwap_pct`, `vwap_relation`.
- Forward outcomes: `forward_returns` for 5m, 15m, 30m, and 60m when retained candles allow it.
- Validation: `mfe`, `mae`, and `mfe_mae_basis`.
- Research metadata: `diagnostic_only`, `backfill`, `source_refs`, and authority flags set false.

Unavailable feature providers are explicit false flags rather than inferred values.
"""


def render_research_feature_dataset_migration_notes_markdown() -> str:
    return """# CRFD Migration Notes

The MVP dataset is a research source only. Existing GRE, validation, scorecard,
and analyzer flows continue to read their current inputs.

Future migration slices can adopt CRFD by:

1. Reading rows for the relevant instrument family and session.
2. Filtering by `feature_availability` instead of assuming feature presence.
3. Using `source_refs` to audit the retained candle window behind a score.
4. Keeping regime plugin scoring unchanged until a separate research validation
   phase proves a CRFD-backed feature provider is useful.

Future NRE, ERE, and TRE plugins can reuse the row schema, but need their own
instrument coverage and feature-provider readiness before scoring.
"""


def _build_row(
    *,
    symbol: str,
    candles: Sequence[Mapping[str, Any]],
    index: int,
    vwap_state: Mapping[str, Any],
    generated_at: datetime,
    timeframe: str,
    cadence_minutes: int,
    backfill: bool,
    output_root: Path,
    auxiliary_sources: Mapping[str, Any],
) -> dict[str, Any]:
    candle = candles[index]
    observation_time = candle["timestamp"]
    session_label = label_session_phase(observation_time)
    close = float(candle["close"])
    open_price = float(candle["open"])
    high = float(candle["high"])
    low = float(candle["low"])
    feature_availability = _feature_availability(auxiliary_sources, has_vwap=bool(vwap_state.get("has_vwap")))
    forward_returns = _forward_returns(candles, index=index, ref_close=close)
    mfe, mae = _mfe_mae(candles, index=index, ref_close=close)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "observation_time": observation_time.isoformat(),
        "instrument": _instrument_family(symbol),
        "contract": symbol,
        "session": phase_coarse_session_group(session_label),
        "session_label": session_label,
        "timeframe": timeframe,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": candle.get("volume"),
        "candle_body": close - open_price,
        "candle_range": high - low,
        "direction": _direction(close - open_price),
        "trend_lookback": _trend_lookback(candles, index=index),
        "has_vwap": bool(vwap_state.get("has_vwap")),
        "vwap": vwap_state.get("vwap"),
        "distance_from_vwap": vwap_state.get("distance_from_vwap_points"),
        "distance_from_vwap_points": vwap_state.get("distance_from_vwap_points"),
        "distance_from_vwap_pct": vwap_state.get("distance_from_vwap_pct"),
        "vwap_relation": vwap_state.get("vwap_relation") or "unavailable",
        "vwap_session": vwap_state.get("vwap_session") or phase_coarse_session_group(session_label),
        "vwap_source_timeframe": vwap_state.get("vwap_source_timeframe") or timeframe,
        "vwap_unavailable_reason": vwap_state.get("vwap_unavailable_reason"),
        "vwap_slope": vwap_state.get("vwap_slope"),
        "vwap_reclaim_candidate": bool(vwap_state.get("vwap_reclaim_candidate")),
        "vwap_rejection_candidate": bool(vwap_state.get("vwap_rejection_candidate")),
        "trend_overlay": _research_reference(auxiliary_sources.get("trend_overlay")),
        "side_session_statistics": _research_reference(auxiliary_sources.get("side_session_attribution")),
        "forward_path_refs": _research_reference(auxiliary_sources.get("forward_path_capture")),
        "forward_returns": forward_returns,
        "available_forward_horizons": [key for key, value in forward_returns.items() if value is not None],
        "missing_forward_horizons": [key for key, value in forward_returns.items() if value is None],
        "mfe": mfe,
        "mae": mae,
        "mfe_mae_basis": "long_directional_reference_from_observation_close",
        "feature_availability": feature_availability,
        "diagnostic_only": True,
        "backfill": backfill,
        "lookahead_safe": True,
        "classification_feature_max_ts": observation_time.isoformat(),
        "observation_cadence_minutes": cadence_minutes,
        "source_refs": {
            "phase1_candles": str(output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"),
            **{key: str(value.get("path")) for key, value in auxiliary_sources.items() if isinstance(value, Mapping) and value.get("exists")},
        },
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def _load_candles(output_root: Path, *, instruments: Sequence[str], timeframe: str) -> dict[str, tuple[dict[str, Any], ...]]:
    result: dict[str, tuple[dict[str, Any], ...]] = {}
    for symbol in instruments:
        path = output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
        result[str(symbol).upper()] = _extract_bars(_read_json(path))
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


def _candidate_indexes(rows: Sequence[Mapping[str, Any]], *, cadence_minutes: int) -> list[int]:
    indexes: list[int] = []
    last: datetime | None = None
    for idx, row in enumerate(rows):
        timestamp = row.get("timestamp")
        if not isinstance(timestamp, datetime):
            continue
        if last is None or timestamp >= last + timedelta(minutes=cadence_minutes):
            indexes.append(idx)
            last = timestamp
    return indexes


def _forward_returns(candles: Sequence[Mapping[str, Any]], *, index: int, ref_close: float) -> dict[str, float | None]:
    observation_time = candles[index]["timestamp"]
    result: dict[str, float | None] = {}
    for horizon in HORIZONS_MINUTES:
        target = observation_time + timedelta(minutes=horizon)
        future = next((row for row in candles[index + 1 :] if row["timestamp"] >= target), None)
        result[f"{horizon}m"] = None if future is None else float(future["close"]) - ref_close
    return result


def _mfe_mae(candles: Sequence[Mapping[str, Any]], *, index: int, ref_close: float) -> tuple[float | None, float | None]:
    observation_time = candles[index]["timestamp"]
    horizon_end = observation_time + timedelta(minutes=max(HORIZONS_MINUTES))
    future_rows = [row for row in candles[index + 1 :] if row["timestamp"] <= horizon_end]
    if not future_rows:
        return None, None
    return max(float(row["high"]) - ref_close for row in future_rows), min(float(row["low"]) - ref_close for row in future_rows)


def _trend_lookback(candles: Sequence[Mapping[str, Any]], *, index: int, window: int = 5) -> dict[str, Any]:
    start = max(0, index - window + 1)
    sample = candles[start : index + 1]
    if len(sample) < 2:
        return {"bars": len(sample), "return": 0.0, "direction": "FLAT"}
    trend_return = float(sample[-1]["close"]) - float(sample[0]["close"])
    return {"bars": len(sample), "return": trend_return, "direction": _direction(trend_return)}


def _session_vwap_by_index(candles: Sequence[Mapping[str, Any]], *, timeframe: str) -> dict[int, dict[str, Any]]:
    states: dict[int, dict[str, Any]] = {}
    current_session: str | None = None
    cumulative_price_volume = 0.0
    cumulative_volume = 0.0
    previous_vwap: float | None = None
    previous_relation: str | None = None
    for idx, candle in enumerate(candles):
        timestamp = candle.get("timestamp")
        session = phase_coarse_session_group(label_session_phase(timestamp)) if isinstance(timestamp, datetime) else "UNKNOWN"
        if session != current_session:
            current_session = session
            cumulative_price_volume = 0.0
            cumulative_volume = 0.0
            previous_vwap = None
            previous_relation = None
        volume = _positive_volume(candle.get("volume"))
        if volume is None:
            states[idx] = _unavailable_vwap_state(
                session=session,
                timeframe=timeframe,
                reason="missing_or_zero_volume",
            )
            continue
        typical_price = (float(candle["high"]) + float(candle["low"]) + float(candle["close"])) / 3.0
        cumulative_price_volume += typical_price * volume
        cumulative_volume += volume
        if cumulative_volume <= 0:
            states[idx] = _unavailable_vwap_state(
                session=session,
                timeframe=timeframe,
                reason="no_positive_session_volume",
            )
            continue
        vwap = cumulative_price_volume / cumulative_volume
        close = float(candle["close"])
        distance_points = close - vwap
        distance_pct = None if vwap == 0 else (distance_points / vwap) * 100.0
        relation = _vwap_relation(distance_points)
        states[idx] = {
            "has_vwap": True,
            "vwap": vwap,
            "distance_from_vwap_points": distance_points,
            "distance_from_vwap_pct": distance_pct,
            "vwap_relation": relation,
            "vwap_session": session,
            "vwap_source_timeframe": timeframe,
            "vwap_unavailable_reason": None,
            "vwap_slope": None if previous_vwap is None else vwap - previous_vwap,
            "vwap_reclaim_candidate": previous_relation == "below_vwap" and relation == "above_vwap",
            "vwap_rejection_candidate": previous_relation == "above_vwap" and relation == "below_vwap",
        }
        previous_vwap = vwap
        previous_relation = relation
    return states


def _unavailable_vwap_state(*, session: str, timeframe: str, reason: str) -> dict[str, Any]:
    return {
        "has_vwap": False,
        "vwap": None,
        "distance_from_vwap_points": None,
        "distance_from_vwap_pct": None,
        "vwap_relation": "unavailable",
        "vwap_session": session,
        "vwap_source_timeframe": timeframe,
        "vwap_unavailable_reason": reason,
        "vwap_slope": None,
        "vwap_reclaim_candidate": False,
        "vwap_rejection_candidate": False,
    }


def _positive_volume(value: Any) -> float | None:
    try:
        volume = float(value)
    except (TypeError, ValueError):
        return None
    if volume <= 0:
        return None
    return volume


def _vwap_relation(distance_points: float) -> str:
    if abs(distance_points) < 1e-12:
        return "at_vwap"
    if distance_points > 0:
        return "above_vwap"
    return "below_vwap"


def _feature_availability(auxiliary_sources: Mapping[str, Any], *, has_vwap: bool) -> dict[str, bool]:
    return {
        "has_vwap": has_vwap,
        "has_anchor_vwap": False,
        "has_prior_session": False,
        "has_overnight_range": False,
        "has_opening_range": False,
        "has_trend_overlay": bool(dict(auxiliary_sources.get("trend_overlay") or {}).get("exists")),
        "has_side_session_stats": bool(dict(auxiliary_sources.get("side_session_attribution") or {}).get("exists")),
        "has_forward_path_refs": bool(dict(auxiliary_sources.get("forward_path_capture") or {}).get("exists")),
    }


def _empty_vwap_summary() -> dict[str, Any]:
    return {
        "available_count": 0,
        "unavailable_count": 0,
        "unavailable_reasons": {},
        "relation_counts": {
            "above_vwap": 0,
            "below_vwap": 0,
            "at_vwap": 0,
            "unavailable": 0,
        },
        "distance_sum": 0.0,
        "distance_count": 0,
    }


def _update_vwap_summary(summary: dict[str, Any], row: Mapping[str, Any]) -> None:
    relation = str(row.get("vwap_relation") or "unavailable")
    relation_counts = dict(summary.get("relation_counts") or {})
    relation_counts[relation] = relation_counts.get(relation, 0) + 1
    summary["relation_counts"] = relation_counts
    if row.get("has_vwap") is True:
        summary["available_count"] = int(summary.get("available_count") or 0) + 1
        distance = row.get("distance_from_vwap_points")
        if distance is not None:
            summary["distance_sum"] = float(summary.get("distance_sum") or 0.0) + float(distance)
            summary["distance_count"] = int(summary.get("distance_count") or 0) + 1
        return
    summary["unavailable_count"] = int(summary.get("unavailable_count") or 0) + 1
    reason = str(row.get("vwap_unavailable_reason") or "unknown")
    reasons = dict(summary.get("unavailable_reasons") or {})
    reasons[reason] = reasons.get(reason, 0) + 1
    summary["unavailable_reasons"] = reasons


def _finalize_vwap_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    distance_count = int(summary.get("distance_count") or 0)
    distance_sum = float(summary.get("distance_sum") or 0.0)
    return {
        "available_count": int(summary.get("available_count") or 0),
        "unavailable_count": int(summary.get("unavailable_count") or 0),
        "unavailable_reasons": dict(summary.get("unavailable_reasons") or {}),
        "relation_counts": dict(summary.get("relation_counts") or {}),
        "average_distance_from_vwap_points": None if distance_count == 0 else distance_sum / distance_count,
    }


def _detect_auxiliary_sources(output_root: Path) -> dict[str, dict[str, Any]]:
    candidates = {
        "trend_overlay": output_root
        / "trend_continuation_overlay"
        / "latest_trend_continuation_overlay_summary.json",
        "side_session_attribution": output_root
        / "strategy_performance"
        / "side_session_attribution"
        / "latest_side_session_attribution_summary.json",
        "forward_path_capture": output_root
        / "strategy_performance"
        / "side_session_attribution"
        / "forward_path_capture.jsonl",
    }
    return {
        key: {"path": str(path), "exists": path.exists(), "research_reference_only": True}
        for key, path in candidates.items()
    }


def _research_reference(source: Any) -> dict[str, Any]:
    source_map = dict(source or {})
    return {
        "available": bool(source_map.get("exists")),
        "source_ref": source_map.get("path"),
        "research_reference_only": True,
        "time_aligned": False,
    }


def _coverage_windows(candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    windows: dict[str, Any] = {}
    for symbol, rows in candles_by_symbol.items():
        if not rows:
            windows[symbol] = {"bar_count": 0}
            continue
        windows[symbol] = {
            "bar_count": len(rows),
            "first_ts": rows[0]["timestamp"].isoformat(),
            "last_ts": rows[-1]["timestamp"].isoformat(),
        }
    return windows


def _observation_window(rows: Sequence[Mapping[str, Any]]) -> tuple[str | None, str | None]:
    times = sorted(str(row.get("observation_time")) for row in rows if row.get("observation_time"))
    return (times[0], times[-1]) if times else (None, None)


def _coverage_minutes(first: str | None, last: str | None) -> float | None:
    first_dt = _parse_datetime(first)
    last_dt = _parse_datetime(last)
    if first_dt is None or last_dt is None:
        return None
    return max((last_dt - first_dt).total_seconds() / 60.0, 0.0)


def _readiness_for_gre(rows: Sequence[Mapping[str, Any]], windows: Mapping[str, Any]) -> str:
    if not rows:
        return "NO_RESEARCH_FEATURE_ROWS"
    first, last = _observation_window(rows)
    coverage = _coverage_minutes(first, last) or 0.0
    if len(rows) < 30 or coverage < 120:
        return "SHALLOW_HISTORY_DIAGNOSTIC_ONLY"
    if not any(row.get("forward_returns") for row in rows):
        return "NEEDS_FORWARD_OUTCOME_COVERAGE"
    return "READY_FOR_GRE_RESEARCH_BACKFILL"


def _future_plugin_readiness(contracts: set[str]) -> dict[str, str]:
    contract_set = {item.upper() for item in contracts}
    return {
        "GRE": "READY_FOR_RESEARCH_INPUT" if contract_set.intersection({"GC", "MGC"}) else "NOT_READY_INSTRUMENTS_MISSING",
        "NRE": "READY_FOR_RESEARCH_INPUT" if contract_set.intersection({"NQ", "MNQ"}) else "NOT_READY_INSTRUMENTS_MISSING",
        "ERE": "READY_FOR_RESEARCH_INPUT" if contract_set.intersection({"ES", "MES"}) else "NOT_READY_INSTRUMENTS_MISSING",
        "TRE": "READY_FOR_RESEARCH_INPUT" if contract_set.intersection({"ZB", "ZF", "ZN", "ZT"}) else "NOT_READY_INSTRUMENTS_MISSING",
    }


def _instrument_family(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized in {"GC", "MGC"}:
        return "GOLD"
    if normalized in {"NQ", "MNQ"}:
        return "NASDAQ"
    if normalized in {"ES", "MES"}:
        return "ES_SP"
    if normalized in {"ZB", "ZF", "ZN", "ZT"}:
        return "TREASURY"
    return normalized


def _direction(value: float) -> str:
    if value > 0:
        return "UP"
    if value < 0:
        return "DOWN"
    return "FLAT"


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
