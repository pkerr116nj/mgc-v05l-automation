"""Controlled probabilistic Asia Drift continuation research pass."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from ...app.session_phase_labels import label_session_phase
from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest
from ..warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, RECOVERY_CONFIRMED, build_feature_rows
from .models import AsiaDriftFeatureRow

NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_SYMBOLS: tuple[str, ...] = ("GC", "MGC", "ES", "MES", "NQ", "MNQ")
ROOT_GROUPS = {
    "GC": "GOLD",
    "MGC": "GOLD",
    "ES": "SPX",
    "MES": "SPX",
    "NQ": "NDX",
    "MNQ": "NDX",
}
DEVELOPMENT_END = datetime.fromisoformat("2024-12-31T23:59:00-05:00")
HOLDOUT_START = datetime.fromisoformat("2025-01-01T00:00:00-05:00")
PROBABILISTIC_PASS1_VERSION = "asia_drift_probabilistic_pass1_v1"


@dataclass(frozen=True)
class ProbabilisticPass1Artifacts:
    root_dir: Path
    candidate_dataset_csv: Path
    candidate_dataset_parquet: Path
    outcome_dataset_csv: Path
    outcome_dataset_parquet: Path
    per_instrument_probability_csv: Path
    pooled_summary_csv: Path
    mfe_mae_distribution_csv: Path
    confirmation_lift_csv: Path
    regime_summary_csv: Path
    summary_json: Path
    summary_markdown: Path
    storage_manifest: Path


def run_probabilistic_pass1(
    *,
    warehouse_root: Path,
    output_dir: Path,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    normalized_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols}))
    effective_start_ts = start_ts or datetime.fromisoformat("2020-01-01T18:00:00-05:00")
    effective_end_ts = end_ts or datetime.fromisoformat("2026-04-21T23:59:00-04:00")
    warehouse_root = warehouse_root.resolve()
    output_dir = output_dir.resolve()
    connection = _try_connect_duckdb()

    try:
        candidate_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
        cross_asset_seed: list[dict[str, Any]] = []
        coverage_summary: dict[str, Any] = {}

        for symbol in normalized_symbols:
            bars_5m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_5m",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="5m",
            )
            feature_rows = build_feature_rows(bars_5m=bars_5m, calibration_profile_name=RECOVERY_CONFIRMED)
            slope_15m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_15m",
                    symbol=symbol,
                    start_ts=effective_start_ts,
                    end_ts=effective_end_ts,
                    timeframe="15m",
                ),
                lookback_bars=3,
            )
            direction_60m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_60m",
                    symbol=symbol,
                    start_ts=effective_start_ts,
                    end_ts=effective_end_ts,
                    timeframe="60m",
                ),
                lookback_bars=3,
            )
            direction_240m_lookup = _build_bar_slope_lookup(
                _load_warehouse_bars(
                    connection=connection,
                    warehouse_root=warehouse_root,
                    dataset_name="derived_bars_240m",
                    symbol=symbol,
                    start_ts=effective_start_ts,
                    end_ts=effective_end_ts,
                    timeframe="240m",
                ),
                lookback_bars=3,
            )
            daily_rows = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_daily",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="daily",
            )
            daily_regime_lookup = _build_daily_regime_lookup(daily_rows)

            candidate_rows: list[dict[str, Any]] = []
            for feature in feature_rows:
                candidate = _build_candidate_row(
                    feature=feature,
                    symbol=symbol,
                    slope_15m_lookup=slope_15m_lookup,
                    direction_60m_lookup=direction_60m_lookup,
                    direction_240m_lookup=direction_240m_lookup,
                    daily_regime_lookup=daily_regime_lookup,
                )
                if candidate is None:
                    continue
                candidate_rows.append(candidate)
                cross_asset_seed.append(
                    {
                        "candidate_id": candidate["candidate_id"],
                        "decision_ts": candidate["decision_ts"],
                        "direction": candidate["direction"],
                        "root_group": candidate["root_group"],
                        "symbol": symbol,
                    }
                )
            candidate_rows_by_symbol[symbol] = candidate_rows
            coverage_summary[symbol] = {
                "candidate_count": len(candidate_rows),
                "feature_row_count": len(feature_rows),
                "coverage_start": bars_5m[0].end_ts.isoformat() if bars_5m else None,
                "coverage_end": bars_5m[-1].end_ts.isoformat() if bars_5m else None,
            }

        confirmation_map = _build_cross_asset_confirmation_map(cross_asset_seed)
        candidate_rows = _flatten_candidate_rows(candidate_rows_by_symbol, confirmation_map=confirmation_map)

        outcome_rows: list[dict[str, Any]] = []
        for symbol in normalized_symbols:
            symbol_candidates = [row for row in candidate_rows if row["instrument"] == symbol]
            raw_series = _load_raw_outcome_series(
                connection=connection,
                warehouse_root=warehouse_root,
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
            )
            for candidate in symbol_candidates:
                outcome_rows.append(_label_outcome_row(candidate=candidate, raw_series=raw_series))
    finally:
        if connection is not None:
            connection.close()

    layout = build_layout(output_dir)
    candidate_dataset_csv = layout["features"] / "asia_drift_probabilistic_pass1_candidates.csv"
    candidate_dataset_parquet = layout["features"] / "asia_drift_probabilistic_pass1_candidates.parquet"
    outcome_dataset_csv = layout["signals"] / "asia_drift_probabilistic_pass1_outcomes.csv"
    outcome_dataset_parquet = layout["signals"] / "asia_drift_probabilistic_pass1_outcomes.parquet"
    per_instrument_probability_csv = layout["reports"] / "asia_drift_probabilistic_pass1_per_instrument_probability.csv"
    pooled_summary_csv = layout["reports"] / "asia_drift_probabilistic_pass1_pooled_summary.csv"
    mfe_mae_distribution_csv = layout["reports"] / "asia_drift_probabilistic_pass1_mfe_mae_distribution.csv"
    confirmation_lift_csv = layout["reports"] / "asia_drift_probabilistic_pass1_confirmation_lift.csv"
    regime_summary_csv = layout["reports"] / "asia_drift_probabilistic_pass1_regime_summary.csv"
    summary_json = layout["reports"] / "asia_drift_probabilistic_pass1_summary.json"
    summary_markdown = layout["reports"] / "asia_drift_probabilistic_pass1_summary.md"

    _write_csv(candidate_dataset_csv, candidate_rows)
    _write_csv(outcome_dataset_csv, outcome_rows)
    materialize_parquet_dataset(candidate_dataset_parquet, candidate_rows)
    materialize_parquet_dataset(outcome_dataset_parquet, outcome_rows)

    per_instrument_probability_rows = _build_per_instrument_probability_rows(outcome_rows)
    pooled_summary_rows = _build_pooled_summary_rows(outcome_rows)
    mfe_mae_distribution_rows = _build_mfe_mae_distribution_rows(outcome_rows)
    confirmation_lift_rows = _build_confirmation_lift_rows(outcome_rows)
    regime_summary_rows = _build_regime_summary_rows(outcome_rows)

    _write_csv(per_instrument_probability_csv, per_instrument_probability_rows)
    _write_csv(pooled_summary_csv, pooled_summary_rows)
    _write_csv(mfe_mae_distribution_csv, mfe_mae_distribution_rows)
    _write_csv(confirmation_lift_csv, confirmation_lift_rows)
    _write_csv(regime_summary_csv, regime_summary_rows)

    summary_payload = {
        "module": "asia_drift_probabilistic_pass1",
        "version": PROBABILISTIC_PASS1_VERSION,
        "setup_family": "asia_drift_continuation_only",
        "decision_timeframe": "5m",
        "symbols": list(normalized_symbols),
        "scope": {
            "start_ts": effective_start_ts.isoformat(),
            "end_ts": effective_end_ts.isoformat(),
            "development_end": DEVELOPMENT_END.isoformat(),
            "holdout_start": HOLDOUT_START.isoformat(),
        },
        "coverage_summary": coverage_summary,
        "row_counts": {
            "candidate_rows": len(candidate_rows),
            "outcome_rows": len(outcome_rows),
        },
        "artifact_paths": {
            "candidate_dataset_csv": str(candidate_dataset_csv),
            "candidate_dataset_parquet": str(candidate_dataset_parquet),
            "outcome_dataset_csv": str(outcome_dataset_csv),
            "outcome_dataset_parquet": str(outcome_dataset_parquet),
            "per_instrument_probability_csv": str(per_instrument_probability_csv),
            "pooled_summary_csv": str(pooled_summary_csv),
            "mfe_mae_distribution_csv": str(mfe_mae_distribution_csv),
            "confirmation_lift_csv": str(confirmation_lift_csv),
            "regime_summary_csv": str(regime_summary_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": {
            "development_rows": sum(1 for row in outcome_rows if row["sample_split"] == "development"),
            "holdout_rows": sum(1 for row in outcome_rows if row["sample_split"] == "holdout"),
            "holdout_pooled_continuation_probability_60m": _pooled_probability(outcome_rows, split="holdout"),
            "development_pooled_continuation_probability_60m": _pooled_probability(outcome_rows, split="development"),
        },
        "notes": [
            "Research-only probabilistic continuation pass.",
            "No hard trading thresholds were optimized.",
            "Cross-asset confirmation uses different root groups only.",
        ],
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(_render_markdown(summary_payload, pooled_summary_rows, confirmation_lift_rows), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_probabilistic_pass1",
            "version": PROBABILISTIC_PASS1_VERSION,
            "artifact_paths": summary_payload["artifact_paths"],
            "candidate_row_count": len(candidate_rows),
            "outcome_row_count": len(outcome_rows),
        },
    )

    artifacts = ProbabilisticPass1Artifacts(
        root_dir=layout["root"],
        candidate_dataset_csv=candidate_dataset_csv,
        candidate_dataset_parquet=candidate_dataset_parquet,
        outcome_dataset_csv=outcome_dataset_csv,
        outcome_dataset_parquet=outcome_dataset_parquet,
        per_instrument_probability_csv=per_instrument_probability_csv,
        pooled_summary_csv=pooled_summary_csv,
        mfe_mae_distribution_csv=mfe_mae_distribution_csv,
        confirmation_lift_csv=confirmation_lift_csv,
        regime_summary_csv=regime_summary_csv,
        summary_json=summary_json,
        summary_markdown=summary_markdown,
        storage_manifest=layout["storage_manifest"],
    )
    return {
        "artifacts": {
            "output_dir": str(artifacts.root_dir),
            "candidate_dataset_csv": str(artifacts.candidate_dataset_csv),
            "candidate_dataset_parquet": str(artifacts.candidate_dataset_parquet),
            "outcome_dataset_csv": str(artifacts.outcome_dataset_csv),
            "outcome_dataset_parquet": str(artifacts.outcome_dataset_parquet),
            "per_instrument_probability_csv": str(artifacts.per_instrument_probability_csv),
            "pooled_summary_csv": str(artifacts.pooled_summary_csv),
            "mfe_mae_distribution_csv": str(artifacts.mfe_mae_distribution_csv),
            "confirmation_lift_csv": str(artifacts.confirmation_lift_csv),
            "regime_summary_csv": str(artifacts.regime_summary_csv),
            "summary_json": str(artifacts.summary_json),
            "summary_markdown": str(artifacts.summary_markdown),
            "storage_manifest": str(artifacts.storage_manifest),
        },
        "summary": summary_payload,
    }


def _build_candidate_row(
    *,
    feature: AsiaDriftFeatureRow,
    symbol: str,
    slope_15m_lookup: dict[str, Any],
    direction_60m_lookup: dict[str, Any],
    direction_240m_lookup: dict[str, Any],
    daily_regime_lookup: dict[str, Any],
) -> dict[str, Any] | None:
    if not feature.in_scope or not feature.entry_window_open or not feature.anchor_observed:
        return None
    if feature.session_bar_index < 8:
        return None
    if feature.regime not in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}:
        return None

    direction = "LONG" if feature.regime == ASIA_DRIFT_LONG else "SHORT"
    root_group = ROOT_GROUPS.get(symbol, symbol)
    vwap_separation_atr = (
        feature.signed_vwap_displacement_long
        if direction == "LONG"
        else feature.signed_vwap_displacement_short
    )
    slope_5m_value = feature.slope_combo_long if direction == "LONG" else feature.slope_combo_short
    slope_5m_state = _classify_signed_state(slope_5m_value)
    fifteen_state = _lookup_state(slope_15m_lookup, feature.decision_ts)
    sixty_state = _lookup_state(direction_60m_lookup, feature.decision_ts)
    twoforty_state = _lookup_state(direction_240m_lookup, feature.decision_ts)
    sixty_match = int(_state_matches_direction(sixty_state["state"], direction))
    twoforty_match = int(_state_matches_direction(twoforty_state["state"], direction))
    daily_regime = _lookup_daily_regime(daily_regime_lookup, feature.decision_ts)
    mandatory_exit_ts = _mandatory_exit_ts(feature.local_session_date, feature.decision_ts)

    return {
        "candidate_id": f"{symbol}|{feature.decision_ts.isoformat()}|{direction}",
        "instrument": symbol,
        "root_group": root_group,
        "decision_ts": feature.decision_ts,
        "local_session_date": feature.local_session_date.isoformat(),
        "asia_drift_session_id": feature.asia_drift_session_id,
        "sample_split": _sample_split(feature.decision_ts),
        "setup_family": "ASIA_DRIFT_CONTINUATION",
        "direction": direction,
        "decision_close": feature.close,
        "subphase": feature.subphase,
        "drift_strength": feature.long_drift_strength if direction == "LONG" else feature.short_drift_strength,
        "regime_persistence_label": feature.regime_persistence_label,
        "recovery_label": feature.recovery_label,
        "vwap_separation_atr": round(vwap_separation_atr, 6),
        "slope_5m_value": round(slope_5m_value, 6),
        "slope_5m_state": slope_5m_state,
        "slope_15m_value": round(float(fifteen_state["value"]), 6),
        "slope_15m_state": str(fifteen_state["state"]),
        "direction_60m_value": round(float(sixty_state["value"]), 6),
        "direction_60m_state": str(sixty_state["state"]),
        "direction_240m_value": round(float(twoforty_state["value"]), 6),
        "direction_240m_state": str(twoforty_state["state"]),
        "higher_timeframe_agreement_count": sixty_match + twoforty_match,
        "higher_timeframe_agreement_state": _agreement_state(sixty_match + twoforty_match),
        "compression_state": _compression_state(feature),
        "disorder_state": _disorder_state(feature),
        "cross_asset_confirmation": False,
        "cross_asset_confirming_root_count": 0,
        "daily_regime_bucket": daily_regime,
        "session_end_ts": mandatory_exit_ts,
    }


def _build_cross_asset_confirmation_map(seed_rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[tuple[datetime, str], dict[str, set[str]]] = {}
    for row in seed_rows:
        key = (row["decision_ts"], str(row["direction"]))
        grouped.setdefault(key, {})
        grouped[key].setdefault(str(row["root_group"]), set()).add(str(row["candidate_id"]))

    payload: dict[str, dict[str, Any]] = {}
    for groups in grouped.values():
        root_names = sorted(groups.keys())
        for root_name, candidate_ids in groups.items():
            confirming_count = len([name for name in root_names if name != root_name])
            for candidate_id in candidate_ids:
                payload[str(candidate_id)] = {
                    "cross_asset_confirmation": confirming_count > 0,
                    "cross_asset_confirming_root_count": confirming_count,
                }
    return payload


def _flatten_candidate_rows(
    candidate_rows_by_symbol: dict[str, list[dict[str, Any]]],
    *,
    confirmation_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in sorted(candidate_rows_by_symbol):
        for row in candidate_rows_by_symbol[symbol]:
            confirmation = confirmation_map.get(str(row["candidate_id"]), {})
            rows.append({**row, **confirmation})
    return rows


def _label_outcome_row(*, candidate: dict[str, Any], raw_series: dict[str, Any]) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    closes = raw_series["closes"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]

    direction = str(candidate["direction"])
    decision_ts = candidate["decision_ts"]
    entry_price = float(candidate["decision_close"])
    idx_start = bisect_right(timestamps, decision_ts)
    idx_15 = bisect_right(timestamps, decision_ts + timedelta(minutes=15))
    idx_60 = bisect_right(timestamps, decision_ts + timedelta(minutes=60))
    idx_session = bisect_right(timestamps, candidate["session_end_ts"])

    forward_return_15m = _signed_return(direction, entry_price, _last_value(closes, idx_start, idx_15))
    forward_return_60m = _signed_return(direction, entry_price, _last_value(closes, idx_start, idx_60))
    session_end_return = _signed_return(direction, entry_price, _last_value(closes, idx_start, idx_session))

    window_highs = highs[idx_start:idx_60]
    window_lows = lows[idx_start:idx_60]
    window_ts = timestamps[idx_start:idx_60]
    mfe_points = 0.0
    mae_points = 0.0
    time_to_mfe_minutes: int | None = None
    time_to_mae_minutes: int | None = None
    if window_ts:
        favorable_values = [
            (float(high) - entry_price) if direction == "LONG" else (entry_price - float(low))
            for high, low in zip(window_highs, window_lows, strict=False)
        ]
        adverse_values = [
            (entry_price - float(low)) if direction == "LONG" else (float(high) - entry_price)
            for high, low in zip(window_highs, window_lows, strict=False)
        ]
        mfe_points = max(favorable_values)
        mae_points = max(adverse_values)
        time_to_mfe_minutes = _minutes_to_first(window_ts, decision_ts, favorable_values, mfe_points)
        time_to_mae_minutes = _minutes_to_first(window_ts, decision_ts, adverse_values, mae_points)

    resolution_proxy, time_to_resolution_proxy = _resolution_proxy(
        time_to_mfe_minutes=time_to_mfe_minutes,
        time_to_mae_minutes=time_to_mae_minutes,
    )
    return {
        "candidate_id": candidate["candidate_id"],
        "instrument": candidate["instrument"],
        "sample_split": candidate["sample_split"],
        "direction": direction,
        "decision_ts": decision_ts,
        "forward_return_15m": round(forward_return_15m, 6) if forward_return_15m is not None else None,
        "forward_return_60m": round(forward_return_60m, 6) if forward_return_60m is not None else None,
        "session_end_return": round(session_end_return, 6) if session_end_return is not None else None,
        "continuation_60m": bool(forward_return_60m is not None and forward_return_60m > 0.0),
        "mfe_60m_points": round(mfe_points, 6),
        "mae_60m_points": round(mae_points, 6),
        "time_to_mfe_minutes": time_to_mfe_minutes,
        "time_to_mae_minutes": time_to_mae_minutes,
        "resolution_proxy": resolution_proxy,
        "time_to_resolution_proxy_minutes": time_to_resolution_proxy,
        "cross_asset_confirmation": bool(candidate["cross_asset_confirmation"]),
        "cross_asset_confirming_root_count": int(candidate["cross_asset_confirming_root_count"]),
        "daily_regime_bucket": candidate["daily_regime_bucket"],
        "higher_timeframe_agreement_state": candidate["higher_timeframe_agreement_state"],
        "compression_state": candidate["compression_state"],
        "disorder_state": candidate["disorder_state"],
    }


def _build_per_instrument_probability_rows(outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_metric_rows(
        outcome_rows,
        group_keys=("sample_split", "instrument"),
    )


def _build_pooled_summary_rows(outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_metric_rows(
        outcome_rows,
        group_keys=("sample_split",),
    )


def _build_confirmation_lift_rows(outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_metric_rows(
        outcome_rows,
        group_keys=("sample_split", "cross_asset_confirmation"),
    )


def _build_regime_summary_rows(outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_metric_rows(
        outcome_rows,
        group_keys=("sample_split", "daily_regime_bucket", "higher_timeframe_agreement_state"),
    )


def _build_mfe_mae_distribution_rows(outcome_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in outcome_rows:
        grouped.setdefault((str(row["sample_split"]), str(row["instrument"])), []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        rows = grouped[key]
        mfe_values = [float(row["mfe_60m_points"]) for row in rows]
        mae_values = [float(row["mae_60m_points"]) for row in rows]
        payload.append(
            {
                "sample_split": key[0],
                "instrument": key[1],
                "row_count": len(rows),
                "mfe_p25": round(_quantile(mfe_values, 0.25), 6),
                "mfe_p50": round(_quantile(mfe_values, 0.50), 6),
                "mfe_p75": round(_quantile(mfe_values, 0.75), 6),
                "mfe_p90": round(_quantile(mfe_values, 0.90), 6),
                "mae_p25": round(_quantile(mae_values, 0.25), 6),
                "mae_p50": round(_quantile(mae_values, 0.50), 6),
                "mae_p75": round(_quantile(mae_values, 0.75), 6),
                "mae_p90": round(_quantile(mae_values, 0.90), 6),
            }
        )
    return payload


def _group_metric_rows(
    rows: Sequence[dict[str, Any]],
    *,
    group_keys: Sequence[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(tuple(row[key] for key in group_keys), []).append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = {group_key: key[index] for index, group_key in enumerate(group_keys)}
        record.update(
            {
                "row_count": len(bucket),
                "continuation_probability_60m": round(
                    sum(1 for item in bucket if item["continuation_60m"]) / max(len(bucket), 1),
                    6,
                ),
                "avg_forward_return_15m": round(_mean(item["forward_return_15m"] for item in bucket), 6),
                "avg_forward_return_60m": round(_mean(item["forward_return_60m"] for item in bucket), 6),
                "avg_session_end_return": round(_mean(item["session_end_return"] for item in bucket), 6),
                "avg_mfe_60m_points": round(_mean(item["mfe_60m_points"] for item in bucket), 6),
                "avg_mae_60m_points": round(_mean(item["mae_60m_points"] for item in bucket), 6),
            }
        )
        payload.append(record)
    return payload


def _load_warehouse_bars(
    *,
    connection: Any | None,
    warehouse_root: Path,
    dataset_name: str,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    timeframe: str,
) -> list[ResearchBar]:
    dataset_root = build_warehouse_layout(warehouse_root)[dataset_name]
    rows = _read_dataset_rows(
        connection=connection,
        dataset_root=dataset_root / f"symbol={symbol}",
        columns=("symbol", "bar_ts", "open", "high", "low", "close", "volume"),
        start_ts=start_ts,
        end_ts=end_ts,
    )
    bars: list[ResearchBar] = []
    timeframe_minutes = _timeframe_minutes(timeframe)
    for row in rows:
        end_bar_ts = _coerce_ts(row["bar_ts"])
        bars.append(
            ResearchBar(
                instrument=str(row["symbol"]).upper(),
                timeframe=timeframe,
                start_ts=end_bar_ts - timedelta(minutes=timeframe_minutes),
                end_ts=end_bar_ts,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                session_label=label_session_phase(end_bar_ts),
                session_segment=_base_session_segment(label_session_phase(end_bar_ts)),
                source="warehouse",
            )
        )
    return bars


def _load_raw_outcome_series(
    *,
    connection: Any | None,
    warehouse_root: Path,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
) -> dict[str, Any]:
    dataset_root = build_warehouse_layout(warehouse_root)["raw_bars_1m"]
    rows = _read_dataset_rows(
        connection=connection,
        dataset_root=dataset_root / f"symbol={symbol}",
        columns=("bar_ts", "high", "low", "close"),
        start_ts=start_ts,
        end_ts=end_ts,
    )
    return {
        "timestamps": [_coerce_ts(row["bar_ts"]) for row in rows],
        "highs": [float(row["high"]) for row in rows],
        "lows": [float(row["low"]) for row in rows],
        "closes": [float(row["close"]) for row in rows],
    }


def _read_dataset_rows(
    *,
    connection: Any | None,
    dataset_root: Path,
    columns: Sequence[str],
    start_ts: datetime,
    end_ts: datetime,
) -> list[dict[str, Any]]:
    if connection is not None:
        try:
            return _read_dataset_rows_duckdb(
                connection=connection,
                dataset_root=dataset_root,
                columns=columns,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        except Exception:
            pass
    return _read_dataset_rows_pyarrow(
        dataset_root=dataset_root,
        columns=columns,
        start_ts=start_ts,
        end_ts=end_ts,
    )


def _read_dataset_rows_duckdb(
    *,
    connection: Any,
    dataset_root: Path,
    columns: Sequence[str],
    start_ts: datetime,
    end_ts: datetime,
) -> list[dict[str, Any]]:
    glob_path = str(dataset_root / "year=*" / "shard_id=*" / "bars.parquet").replace("'", "''")
    column_sql = ", ".join(columns)
    cursor = connection.execute(
        f"""
        select {column_sql}
        from read_parquet('{glob_path}', union_by_name=true)
        where bar_ts >= ? and bar_ts <= ?
        order by bar_ts
        """,
        [start_ts, end_ts],
    )
    rows = cursor.fetchall()
    return [
        {column: value for column, value in zip(columns, row, strict=False)}
        for row in rows
    ]


def _read_dataset_rows_pyarrow(
    *,
    dataset_root: Path,
    columns: Sequence[str],
    start_ts: datetime,
    end_ts: datetime,
) -> list[dict[str, Any]]:
    pyarrow = _require_pyarrow_dataset()
    parquet_files = sorted(dataset_root.rglob("bars.parquet"))
    if not parquet_files:
        return []
    dataset = pyarrow.dataset([str(path) for path in parquet_files], format="parquet", partitioning="hive")
    table = dataset.to_table(
        columns=list(columns),
        filter=(pyarrow.field("bar_ts") >= pyarrow.scalar(start_ts.astimezone(UTC)))
        & (pyarrow.field("bar_ts") <= pyarrow.scalar(end_ts.astimezone(UTC))),
    )
    rows = table.to_pylist()
    rows.sort(key=lambda row: _coerce_ts(row["bar_ts"]))
    return rows


def _build_bar_slope_lookup(bars: Sequence[ResearchBar], *, lookback_bars: int) -> dict[str, Any]:
    timestamps = [bar.end_ts for bar in bars]
    states: list[dict[str, Any]] = []
    for index, bar in enumerate(bars):
        if index < lookback_bars:
            states.append({"state": "UNKNOWN", "value": 0.0})
            continue
        prior = bars[index - lookback_bars]
        range_baseline = median(item.range_points for item in bars[max(0, index - lookback_bars) : index + 1])
        normalized = (bar.close - prior.close) / max(range_baseline, 1e-9)
        states.append({"state": _classify_signed_state(normalized), "value": normalized})
    return {"timestamps": timestamps, "states": states}


def _build_daily_regime_lookup(bars: Sequence[ResearchBar]) -> dict[str, Any]:
    timestamps = [bar.end_ts for bar in bars]
    states: list[str] = []
    ranges = [bar.range_points for bar in bars]
    for index, bar in enumerate(bars):
        if index == 0:
            states.append("UNKNOWN")
            continue
        prior_ranges = ranges[max(0, index - 5) : index]
        range_state = "EXPANDED" if prior_ranges and bar.range_points > median(prior_ranges) else "NORMAL"
        if bar.close > bar.open:
            direction = "UP"
        elif bar.close < bar.open:
            direction = "DOWN"
        else:
            direction = "FLAT"
        states.append(f"{direction}_{range_state}")
    return {"timestamps": timestamps, "states": states}


def _lookup_state(lookup: dict[str, Any], ts: datetime) -> dict[str, Any]:
    timestamps = lookup["timestamps"]
    idx = bisect_right(timestamps, ts) - 1
    if idx < 0:
        return {"state": "UNKNOWN", "value": 0.0}
    return lookup["states"][idx]


def _lookup_daily_regime(lookup: dict[str, Any], ts: datetime) -> str:
    timestamps = lookup["timestamps"]
    idx = bisect_right(timestamps, ts) - 1
    if idx <= 0:
        return "UNKNOWN"
    return str(lookup["states"][idx - 1])


def _compression_state(feature: AsiaDriftFeatureRow) -> str:
    if feature.compression_followed_by_drift_expansion:
        return "COMPRESSED_DRIFT_EXPANSION"
    if feature.realized_volatility_ratio < 0.85 and feature.bar_overlap_ratio_8 > 0.55:
        return "COMPRESSED"
    if feature.realized_volatility_ratio > 1.25:
        return "EXPANDED"
    return "NORMAL"


def _disorder_state(feature: AsiaDriftFeatureRow) -> str:
    if feature.post_spike_instability and feature.chop_veto:
        return "POST_SPIKE_AND_CHOP"
    if feature.post_spike_instability:
        return "POST_SPIKE"
    if feature.chop_veto:
        return "CHOPPY"
    if feature.reversal_frequency_12 > 0.45:
        return "REVERSAL_HEAVY"
    return "ORDERLY"


def _agreement_state(count: int) -> str:
    if count >= 2:
        return "FULL"
    if count == 1:
        return "PARTIAL"
    return "NONE"


def _state_matches_direction(state: str, direction: str) -> bool:
    if direction == "LONG":
        return state == "UP"
    return state == "DOWN"


def _mandatory_exit_ts(session_date_str: date | str, decision_ts: datetime) -> datetime:
    session_date = session_date_str if isinstance(session_date_str, date) else date.fromisoformat(session_date_str)
    local_dt = datetime.combine(session_date + timedelta(days=1), time(2, 55), tzinfo=NEW_YORK)
    return local_dt.astimezone(decision_ts.tzinfo or UTC)


def _sample_split(ts: datetime) -> str:
    if ts <= DEVELOPMENT_END:
        return "development"
    if ts >= HOLDOUT_START:
        return "holdout"
    return "excluded"


def _classify_signed_state(value: float, deadband: float = 0.10) -> str:
    if value > deadband:
        return "UP"
    if value < -deadband:
        return "DOWN"
    return "FLAT"


def _base_session_segment(label: str) -> str:
    if label.startswith("ASIA"):
        return "ASIA"
    if label.startswith("LONDON"):
        return "LONDON"
    if label.startswith("US"):
        return "US"
    return "UNKNOWN"


def _timeframe_minutes(timeframe: str) -> int:
    mapping = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "60m": 60,
        "240m": 240,
        "daily": 1440,
    }
    return mapping[timeframe]


def _coerce_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _signed_return(direction: str, entry_price: float, exit_price: float | None) -> float | None:
    if exit_price is None:
        return None
    raw = float(exit_price) - entry_price
    return raw if direction == "LONG" else -raw


def _last_value(values: Sequence[float], start_idx: int, end_idx: int) -> float | None:
    if end_idx <= start_idx:
        return None
    return float(values[end_idx - 1])


def _minutes_to_first(
    timestamps: Sequence[datetime],
    decision_ts: datetime,
    values: Sequence[float],
    target: float,
) -> int | None:
    if target <= 0.0:
        return None
    for ts, value in zip(timestamps, values, strict=False):
        if abs(value - target) <= 1e-9:
            return int((ts - decision_ts).total_seconds() // 60)
    return None


def _resolution_proxy(*, time_to_mfe_minutes: int | None, time_to_mae_minutes: int | None) -> tuple[str, int | None]:
    if time_to_mfe_minutes is None and time_to_mae_minutes is None:
        return "UNRESOLVED", None
    if time_to_mfe_minutes is None:
        return "ADVERSE_FIRST", time_to_mae_minutes
    if time_to_mae_minutes is None:
        return "FAVORABLE_FIRST", time_to_mfe_minutes
    if time_to_mfe_minutes < time_to_mae_minutes:
        return "FAVORABLE_FIRST", time_to_mfe_minutes
    if time_to_mae_minutes < time_to_mfe_minutes:
        return "ADVERSE_FIRST", time_to_mae_minutes
    return "TIE", time_to_mfe_minutes


def _mean(values: Sequence[float | None] | Any) -> float:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return 0.0
    return sum(filtered) / len(filtered)


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _pooled_probability(rows: Sequence[dict[str, Any]], *, split: str) -> float:
    filtered = [row for row in rows if row["sample_split"] == split]
    if not filtered:
        return 0.0
    return round(sum(1 for row in filtered if row["continuation_60m"]) / len(filtered), 6)


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = [_json_ready(row) for row in rows]
    fieldnames: list[str] = []
    for row in serialized:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in serialized:
            writer.writerow(row)


def _render_markdown(
    summary_payload: dict[str, Any],
    pooled_summary_rows: Sequence[dict[str, Any]],
    confirmation_lift_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# Asia Drift Probabilistic Pass 1",
        "",
        "## Scope",
        f"- Symbols: `{','.join(summary_payload['symbols'])}`",
        f"- Setup family: `{summary_payload['setup_family']}`",
        f"- Decision timeframe: `{summary_payload['decision_timeframe']}`",
        f"- Window: `{summary_payload['scope']['start_ts']}` to `{summary_payload['scope']['end_ts']}`",
        "",
        "## Headline",
        f"- Development rows: `{summary_payload['headline']['development_rows']}`",
        f"- Holdout rows: `{summary_payload['headline']['holdout_rows']}`",
        f"- Development 60m continuation probability: `{summary_payload['headline']['development_pooled_continuation_probability_60m']}`",
        f"- Holdout 60m continuation probability: `{summary_payload['headline']['holdout_pooled_continuation_probability_60m']}`",
        "",
        "## Pooled Summary",
    ]
    for row in pooled_summary_rows:
        lines.append(
            f"- `{row['sample_split']}` count={row['row_count']} "
            f"p60={row['continuation_probability_60m']} avg60={row['avg_forward_return_60m']} "
            f"avgMFE={row['avg_mfe_60m_points']} avgMAE={row['avg_mae_60m_points']}"
        )
    lines.extend(["", "## Confirmation Lift"])
    for row in confirmation_lift_rows:
        lines.append(
            f"- `{row['sample_split']}` confirmation={row['cross_asset_confirmation']} "
            f"count={row['row_count']} p60={row['continuation_probability_60m']} "
            f"avg60={row['avg_forward_return_60m']}"
        )
    return "\n".join(lines) + "\n"


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _try_connect_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        return None
    return duckdb.connect(database=":memory:")


def _require_pyarrow_dataset():
    try:
        import pyarrow.dataset as dataset  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("Probabilistic pass requires pyarrow research dependency.") from exc
    return dataset
