"""Historical diagnostic observation generator for the Gold Regime Engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_canonical_research_data_provider import (
    build_research_data_provider,
    canonical_candles_as_mappings,
)
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
from mgc_v05l.execution_core.track_b_research_feature_dataset import run_research_feature_dataset_builder


SCHEMA_VERSION = "track_b_gre_historical_backfill_v1"
BACKFILL_ROWS_JSONL = "gre_backfill_observations.jsonl"
LATEST_BACKFILL_SUMMARY_JSON = "latest_gre_backfill_summary.json"
LATEST_BACKFILL_SUMMARY_MD = "latest_gre_backfill_summary.md"
LATEST_BACKFILL_SCORECARD_JSON = "latest_gre_backfill_scorecard.json"
LATEST_BACKFILL_SCORECARD_MD = "latest_gre_backfill_scorecard.md"
LATEST_BACKFILL_ANALYZER_JSON = "latest_gre_backfill_analyzer.json"
LATEST_BACKFILL_ANALYZER_MD = "latest_gre_backfill_analyzer.md"
HISTORICAL_BACKFILL_SUMMARY_JSON = "historical_gre_backfill_summary.json"
HISTORICAL_BACKFILL_SUMMARY_MD = "historical_gre_backfill_summary.md"
HISTORICAL_VALIDATION_SUMMARY_JSON = "historical_gre_validation_summary.json"
HISTORICAL_VALIDATION_SUMMARY_MD = "historical_gre_validation_summary.md"
HISTORICAL_SCORECARD_JSON = "historical_gre_scorecard.json"
HISTORICAL_SCORECARD_MD = "historical_gre_scorecard.md"
HISTORICAL_ANALYZER_JSON = "historical_gre_analyzer.json"
HISTORICAL_ANALYZER_MD = "historical_gre_analyzer.md"
HISTORICAL_VS_RETAINED_COMPARISON_JSON = "historical_gre_vs_retained_comparison.json"
HISTORICAL_VS_RETAINED_COMPARISON_MD = "historical_gre_vs_retained_comparison.md"
DEFAULT_MAX_SOURCE_CANDLES = 5000


@dataclass(frozen=True)
class BackfillResult:
    summary: dict[str, Any]
    validation_summary: dict[str, Any]
    scorecard: dict[str, Any]
    analyzer: dict[str, Any]
    comparison: dict[str, Any]
    rows_path: Path
    summary_path: Path
    validation_summary_path: Path
    scorecard_path: Path
    analyzer_path: Path
    comparison_path: Path


def run_gre_historical_backfill(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    cadence_minutes: int = 5,
    max_observations: int = 200,
    provider: str = "retained",
    research_store_root: Path | None = None,
    max_source_candles: int | None = DEFAULT_MAX_SOURCE_CANDLES,
    crfd_max_rows: int | None = None,
    max_jsonl_row_bytes: int | None = None,
    max_snapshot_bytes: int | None = None,
) -> BackfillResult:
    generated_at = _coerce_now(now)
    if cadence_minutes <= 0:
        raise ValueError("cadence_minutes must be positive")
    if max_observations <= 0:
        raise ValueError("max_observations must be positive")
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    gold_dir.mkdir(parents=True, exist_ok=True)
    retained_reference = {
        "backfill_summary": _read_json(gold_dir / LATEST_BACKFILL_SUMMARY_JSON),
        "scorecard": _read_json(gold_dir / "latest_gre_scorecard.json"),
    }
    provider_id = str(provider).strip().lower()
    crfd_result = run_research_feature_dataset_builder(
        output_root=output_root,
        now=generated_at,
        instruments=("GC", "MGC"),
        timeframe="1m",
        cadence_minutes=cadence_minutes,
        max_rows=crfd_max_rows or max_observations * 2,
        backfill=True,
        provider=provider_id,
        research_store_root=research_store_root,
        max_source_candles=max_source_candles,
    )
    candles_by_symbol, provider_metadata = _load_gold_candles(
        output_root,
        provider=provider_id,
        research_store_root=research_store_root,
        max_source_candles=max_source_candles,
    )
    rows = generate_backfill_observations(
        candles_by_symbol=candles_by_symbol,
        crfd_rows=crfd_result.rows,
        generated_at=generated_at,
        cadence_minutes=cadence_minutes,
        max_observations=max_observations,
        output_root=output_root,
        provider_metadata=provider_metadata,
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
        provider_metadata=provider_metadata,
        crfd_summary=crfd_result.summary,
    )
    snapshot_config = (
        BoundedSnapshotConfig(max_bytes=max_snapshot_bytes)
        if max_snapshot_bytes is not None
        else BoundedSnapshotConfig()
    )
    summary_path = gold_dir / LATEST_BACKFILL_SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    (gold_dir / LATEST_BACKFILL_SUMMARY_MD).write_text(render_backfill_summary_markdown(summary), encoding="utf-8")
    write_bounded_snapshot_json(gold_dir / HISTORICAL_BACKFILL_SUMMARY_JSON, summary, config=snapshot_config)
    (gold_dir / HISTORICAL_BACKFILL_SUMMARY_MD).write_text(render_backfill_summary_markdown(summary), encoding="utf-8")
    scorecard = build_gre_validation_scorecard(rows, generated_at=generated_at, rows_path=rows_path)
    scorecard_path = gold_dir / LATEST_BACKFILL_SCORECARD_JSON
    write_bounded_snapshot_json(scorecard_path, scorecard, config=snapshot_config)
    (gold_dir / LATEST_BACKFILL_SCORECARD_MD).write_text(render_gre_scorecard_markdown(scorecard), encoding="utf-8")
    write_bounded_snapshot_json(gold_dir / HISTORICAL_SCORECARD_JSON, scorecard, config=snapshot_config)
    (gold_dir / HISTORICAL_SCORECARD_MD).write_text(render_gre_scorecard_markdown(scorecard), encoding="utf-8")
    validation_summary = build_validation_summary(rows[-1] if rows else {}, rows_path=rows_path, generated_at=generated_at)
    validation_summary_path = gold_dir / HISTORICAL_VALIDATION_SUMMARY_JSON
    write_bounded_snapshot_json(validation_summary_path, validation_summary, config=snapshot_config)
    (gold_dir / HISTORICAL_VALIDATION_SUMMARY_MD).write_text(render_validation_summary_markdown(validation_summary), encoding="utf-8")
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
    write_bounded_snapshot_json(gold_dir / HISTORICAL_ANALYZER_JSON, analyzer, config=snapshot_config)
    (gold_dir / HISTORICAL_ANALYZER_MD).write_text(render_gre_research_analyzer_markdown(analyzer), encoding="utf-8")
    comparison = build_historical_vs_retained_comparison(
        generated_at=generated_at,
        retained_reference=retained_reference,
        historical_summary=summary,
        historical_scorecard=scorecard,
        historical_analyzer=analyzer,
    )
    comparison_path = gold_dir / HISTORICAL_VS_RETAINED_COMPARISON_JSON
    write_bounded_snapshot_json(comparison_path, comparison, config=snapshot_config)
    (gold_dir / HISTORICAL_VS_RETAINED_COMPARISON_MD).write_text(render_historical_vs_retained_comparison_markdown(comparison), encoding="utf-8")
    return BackfillResult(
        summary=summary,
        validation_summary=validation_summary,
        scorecard=scorecard,
        analyzer=analyzer,
        comparison=comparison,
        rows_path=rows_path,
        summary_path=summary_path,
        validation_summary_path=validation_summary_path,
        scorecard_path=scorecard_path,
        analyzer_path=analyzer_path,
        comparison_path=comparison_path,
    )


def generate_backfill_observations(
    *,
    candles_by_symbol: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    crfd_rows: Sequence[Mapping[str, Any]] | None = None,
    generated_at: datetime,
    cadence_minutes: int,
    max_observations: int,
    output_root: Path | str,
    provider_metadata: Mapping[str, Any] | None = None,
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
                analytics={"crfd_rows": tuple(crfd_rows or ())},
                generated_at=observation_time,
            )
        )
        row = build_gre_validation_row(
            report,
            candles=primary_1m,
            generated_at=generated_at,
            gre_path="BACKFILL_GENERATED_GRE_OBSERVATION",
            candle_sources={"backfill_GC_1m": _provider_candle_source(provider_metadata, output_root)},
        )
        row["backfill"] = True
        row["historical_provider_backfill"] = str(dict(provider_metadata or {}).get("provider_id") or "retained") != "retained"
        row["observation_cadence_minutes"] = cadence_minutes
        row["lookahead_safe"] = True
        row["classification_candle_max_ts"] = _max_context_timestamp(context_candles)
        row["source_mode"] = _source_mode(provider_metadata)
        row["provider_metadata"] = dict(provider_metadata or {})
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
    provider_metadata: Mapping[str, Any] | None = None,
    crfd_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    regime_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("validation_status") or "UNKNOWN")
        regime = str(row.get("regime_label") or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    windows = _coverage_windows(candles_by_symbol)
    scorecard_probe = build_gre_validation_scorecard(rows, generated_at=generated_at, rows_path=rows_path)
    provider_id = str(dict(provider_metadata or {}).get("provider_id") or "retained")
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
        "validation_percentages": _validation_percentages(rows),
        "confidence_distribution": _confidence_distribution(rows),
        "feature_usage": _feature_usage(rows),
        "missing_provider_frequency": _missing_provider_frequency(rows),
        "available_windows": windows,
        "coverage_assessment": _coverage_assessment(rows, windows, provider_id=provider_id),
        "provider_metadata": dict(provider_metadata or {}),
        "provider_id": provider_id,
        "provider_kind": dict(provider_metadata or {}).get("provider_kind") or "retained_phase1_candles",
        "crfd_summary": {
            "observation_count": crfd_summary.get("observation_count") if isinstance(crfd_summary, Mapping) else None,
            "time_coverage": crfd_summary.get("time_coverage") if isinstance(crfd_summary, Mapping) else None,
            "vwap_coverage": crfd_summary.get("vwap_coverage") if isinstance(crfd_summary, Mapping) else None,
            "anchored_vwap_coverage": crfd_summary.get("anchored_vwap_coverage") if isinstance(crfd_summary, Mapping) else None,
            "readiness_for_gre": crfd_summary.get("readiness_for_gre") if isinstance(crfd_summary, Mapping) else None,
        },
        "scorecard_readiness": scorecard_probe.get("readiness_assessment", {}),
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
        f"Provider: `{summary.get('provider_id')}` / `{summary.get('provider_kind')}`\n\n"
        f"CRFD summary: `{summary.get('crfd_summary')}`\n\n"
        f"Validation percentages: `{summary.get('validation_percentages')}`\n\n"
        f"Confidence distribution: `{summary.get('confidence_distribution')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def build_historical_vs_retained_comparison(
    *,
    generated_at: datetime,
    retained_reference: Mapping[str, Any],
    historical_summary: Mapping[str, Any],
    historical_scorecard: Mapping[str, Any],
    historical_analyzer: Mapping[str, Any],
) -> dict[str, Any]:
    retained_summary = retained_reference.get("backfill_summary") if isinstance(retained_reference.get("backfill_summary"), Mapping) else {}
    retained_scorecard = retained_reference.get("scorecard") if isinstance(retained_reference.get("scorecard"), Mapping) else {}
    return {
        "schema_version": "track_b_historical_gre_vs_retained_comparison_v1",
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "scoring_changed": False,
        "retained_reference": _comparison_side(retained_summary, retained_scorecard),
        "historical_parquet": _comparison_side(historical_summary, historical_scorecard),
        "feature_coverage_delta": {
            "vwap_historical": (historical_summary.get("crfd_summary") or {}).get("vwap_coverage"),
            "avwap_historical": (historical_summary.get("crfd_summary") or {}).get("anchored_vwap_coverage"),
        },
        "historical_analyzer_sample_status": (historical_analyzer.get("sample_assessment") or {}).get("sample_status"),
        "assessment": _comparison_assessment(retained_summary, historical_summary),
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def render_historical_vs_retained_comparison_markdown(comparison: Mapping[str, Any]) -> str:
    return (
        "# Historical GRE vs Retained GRE Comparison\n\n"
        f"Generated: `{comparison.get('generated_at')}`\n\n"
        f"Scoring changed: `{comparison.get('scoring_changed')}`\n\n"
        f"Retained reference: `{comparison.get('retained_reference')}`\n\n"
        f"Historical parquet: `{comparison.get('historical_parquet')}`\n\n"
        f"Feature coverage delta: `{comparison.get('feature_coverage_delta')}`\n\n"
        f"Assessment: `{comparison.get('assessment')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def _load_gold_candles(
    output_root: Path,
    *,
    provider: str = "retained",
    research_store_root: Path | None = None,
    max_source_candles: int | None = DEFAULT_MAX_SOURCE_CANDLES,
) -> tuple[dict[str, dict[str, tuple[dict[str, Any], ...]]], dict[str, Any]]:
    provider_id = str(provider).strip().lower()
    if provider_id == "retained":
        return _load_retained_gold_candles(output_root), {
            "provider_id": "retained",
            "provider_kind": "retained_phase1_candles",
            "max_source_candles": max_source_candles,
        }
    data_provider = build_research_data_provider(provider_id, output_root=output_root, research_store_root=research_store_root)
    loaded = canonical_candles_as_mappings(data_provider.load_candles(symbols=("GC", "MGC"), timeframe="1m"))
    loaded = _limit_candles_by_symbol(loaded, max_source_candles=max_source_candles)
    result: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    for symbol, rows in loaded.items():
        normalized = tuple(dict(row) for row in rows)
        result[symbol] = {
            "1m": normalized,
            "5m": _derive_5m_bars(normalized),
        }
    metadata = data_provider.provider_metadata()
    metadata["max_source_candles"] = max_source_candles
    return result, metadata


def _load_retained_gold_candles(output_root: Path) -> dict[str, dict[str, tuple[dict[str, Any], ...]]]:
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


def _coverage_assessment(rows: Sequence[Mapping[str, Any]], windows: Mapping[str, Any], *, provider_id: str = "retained") -> str:
    if not rows:
        if provider_id == "retained":
            return "INSUFFICIENT_RETAINED_HISTORY_FOR_BACKFILL"
        return "INSUFFICIENT_HISTORY_FOR_BACKFILL"
    if len(rows) < 20:
        if provider_id == "retained":
            return "SHALLOW_RETAINED_HISTORY_DIAGNOSTIC_ONLY"
        return "SHALLOW_HISTORY_DIAGNOSTIC_ONLY"
    if provider_id == "retained":
        return "BACKFILL_OBSERVATIONS_AVAILABLE_FROM_RETAINED_CANDLES"
    return "BACKFILL_OBSERVATIONS_AVAILABLE"


def _limit_candles_by_symbol(
    candles_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    max_source_candles: int | None,
) -> dict[str, tuple[dict[str, Any], ...]]:
    if max_source_candles is None:
        return {symbol: tuple(dict(row) for row in rows) for symbol, rows in candles_by_symbol.items()}
    limit = max(int(max_source_candles), 0)
    if limit <= 0:
        return {symbol: () for symbol in candles_by_symbol}
    return {symbol: tuple(dict(row) for row in tuple(rows)[-limit:]) for symbol, rows in candles_by_symbol.items()}


def _derive_5m_bars(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    normalized = [dict(row) for row in rows if isinstance(row.get("timestamp"), datetime)]
    if not normalized:
        return ()
    buckets: dict[datetime, list[dict[str, Any]]] = {}
    for row in normalized:
        ts = row["timestamp"]
        minute = (ts.minute // 5) * 5
        bucket_ts = ts.replace(minute=minute, second=0, microsecond=0)
        buckets.setdefault(bucket_ts, []).append(row)
    derived: list[dict[str, Any]] = []
    for bucket_ts in sorted(buckets):
        bucket = sorted(buckets[bucket_ts], key=lambda item: item["timestamp"])
        if not bucket:
            continue
        volumes = [row.get("volume") for row in bucket if row.get("volume") is not None]
        try:
            volume = sum(float(item) for item in volumes) if volumes else None
        except (TypeError, ValueError):
            volume = None
        derived.append(
            {
                "timestamp": bucket[-1]["timestamp"],
                "open": float(bucket[0]["open"]),
                "high": max(float(row["high"]) for row in bucket),
                "low": min(float(row["low"]) for row in bucket),
                "close": float(bucket[-1]["close"]),
                "volume": volume,
                "source_ref": bucket[-1].get("source_ref"),
                "derived_timeframe": "5m",
            }
        )
    return tuple(derived)


def _validation_percentages(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    total = max(1, len(rows))
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("validation_status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return {status: round(count / total, 4) for status, count in sorted(counts.items())}


def _confidence_distribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    bands = {"0-20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80-100": 0}
    for row in rows:
        try:
            confidence = int(row.get("confidence"))
        except (TypeError, ValueError):
            continue
        if confidence < 20:
            bands["0-20"] += 1
        elif confidence < 40:
            bands["20-40"] += 1
        elif confidence < 60:
            bands["40-60"] += 1
        elif confidence < 80:
            bands["60-80"] += 1
        else:
            bands["80-100"] += 1
    return bands


def _feature_usage(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for key in ("positive_evidence", "negative_evidence", "conflicts"):
            values = row.get(key)
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
                continue
            for item in values:
                if isinstance(item, Mapping):
                    feature = str(item.get("feature") or "UNKNOWN")
                    counts[feature] = counts.get(feature, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _missing_provider_frequency(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        providers = row.get("missing_providers")
        if not isinstance(providers, Sequence) or isinstance(providers, (str, bytes)):
            continue
        for provider in providers:
            text = str(provider)
            counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _provider_candle_source(provider_metadata: Mapping[str, Any] | None, output_root: Path | str) -> str:
    metadata = dict(provider_metadata or {})
    if metadata.get("root"):
        return str(metadata["root"])
    return str(Path(output_root) / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json")


def _source_mode(provider_metadata: Mapping[str, Any] | None) -> str:
    provider_id = str(dict(provider_metadata or {}).get("provider_id") or "retained")
    if provider_id == "retained":
        return "HISTORICAL_GRE_BACKFILL_FROM_RETAINED_CANDLES"
    return f"HISTORICAL_GRE_BACKFILL_FROM_{provider_id.upper()}_CANDLES"


def _comparison_side(summary: Mapping[str, Any], scorecard: Mapping[str, Any]) -> dict[str, Any]:
    overall = scorecard.get("overall_metrics") if isinstance(scorecard.get("overall_metrics"), Mapping) else {}
    return {
        "observation_count": summary.get("observation_count") or overall.get("total_observations"),
        "validated_observations": summary.get("validated_observations") or overall.get("validated_observations"),
        "coverage_assessment": summary.get("coverage_assessment"),
        "provider_id": summary.get("provider_id"),
        "regime_counts": summary.get("regime_counts"),
        "confidence_distribution": summary.get("confidence_distribution"),
        "readiness": (scorecard.get("readiness_assessment") or {}).get("classification")
        if isinstance(scorecard.get("readiness_assessment"), Mapping)
        else None,
        "vwap_coverage": (summary.get("crfd_summary") or {}).get("vwap_coverage") if isinstance(summary.get("crfd_summary"), Mapping) else None,
        "anchored_vwap_coverage": (summary.get("crfd_summary") or {}).get("anchored_vwap_coverage")
        if isinstance(summary.get("crfd_summary"), Mapping)
        else None,
    }


def _comparison_assessment(retained_summary: Mapping[str, Any], historical_summary: Mapping[str, Any]) -> str:
    retained_count = _safe_int(retained_summary.get("observation_count"))
    historical_count = _safe_int(historical_summary.get("observation_count"))
    if historical_count > retained_count:
        return "HISTORICAL_PROVIDER_EXPANDS_GRE_RESEARCH_CORPUS_WITHOUT_SCORING_CHANGES"
    if historical_count:
        return "HISTORICAL_PROVIDER_AVAILABLE_BUT_NOT_LARGER_THAN_RETAINED_REFERENCE"
    return "NO_HISTORICAL_GRE_OBSERVATIONS_AVAILABLE"


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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
