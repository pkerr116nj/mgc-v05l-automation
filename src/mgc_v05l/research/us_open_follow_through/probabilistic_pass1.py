"""Controlled observational baseline for U.S. open continuation / reversal behavior."""

from __future__ import annotations

import csv
import json
from bisect import bisect_right
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from ..asia_drift.probabilistic_pass1 import (
    _build_bar_slope_lookup,
    _coerce_ts,
    _load_raw_outcome_series,
    _load_warehouse_bars,
    _lookup_state,
    _mean,
    _quantile,
    _sample_split,
    _try_connect_duckdb,
    _write_csv,
)
from ..regime.vix_join import attach_vix_asof
from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, materialize_parquet_dataset, write_storage_manifest


NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES", "NQ", "MNQ")
CLUSTER_MAP = {
    "ES": "SPX",
    "MES": "SPX",
    "NQ": "NDX",
    "MNQ": "NDX",
}
PREMARKET_START = time(4, 0)
OPEN_START = time(9, 30)
DECISION_TIME = time(10, 0)
AFTERNOON_CHECKPOINT = time(15, 30)
SESSION_CLOSE = time(16, 0)
OVERNIGHT_START = time(18, 0)
DEVELOPMENT_END = datetime.fromisoformat("2024-12-31T23:59:00-05:00")
HOLDOUT_START = datetime.fromisoformat("2025-01-01T00:00:00-05:00")
PASS_VERSION = "us_open_probabilistic_pass1_v1"
HORIZONS: tuple[tuple[str, timedelta | None], ...] = (
    ("30m", timedelta(minutes=30)),
    ("60m", timedelta(minutes=60)),
    ("120m", timedelta(minutes=120)),
    ("1530", None),
    ("close", None),
)


def run_probabilistic_pass1(
    *,
    warehouse_root: Path,
    output_dir: Path,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    normalized_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols}))
    effective_start_ts = start_ts or datetime.fromisoformat("2020-01-01T00:00:00-05:00")
    effective_end_ts = end_ts or datetime.fromisoformat("2026-04-21T23:59:00-04:00")
    warehouse_root = warehouse_root.resolve()
    output_dir = output_dir.resolve()

    connection = _try_connect_duckdb()
    candidate_rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    coverage_summary: dict[str, Any] = {}
    raw_outcome_by_symbol: dict[str, dict[str, Any]] = {}
    try:
        for symbol in normalized_symbols:
            bars_1m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="raw_bars_1m",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="1m",
            )
            bars_5m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_5m",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="5m",
            )
            bars_15m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_15m",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="15m",
            )
            bars_60m = _load_warehouse_bars(
                connection=connection,
                warehouse_root=warehouse_root,
                dataset_name="derived_bars_60m",
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
                timeframe="60m",
            )
            raw_outcome_by_symbol[symbol] = _load_raw_outcome_series(
                connection=connection,
                warehouse_root=warehouse_root,
                symbol=symbol,
                start_ts=effective_start_ts,
                end_ts=effective_end_ts,
            )
            candidate_rows = _build_candidate_rows(
                symbol=symbol,
                bars_1m=bars_1m,
                slope_5m_lookup=_build_bar_slope_lookup(bars_5m, lookback_bars=3),
                slope_15m_lookup=_build_bar_slope_lookup(bars_15m, lookback_bars=3),
                direction_60m_lookup=_build_bar_slope_lookup(bars_60m, lookback_bars=3),
            )
            candidate_rows_by_symbol[symbol] = candidate_rows
            coverage_summary[symbol] = {
                "candidate_count": len(candidate_rows),
                "bar_count_1m": len(bars_1m),
                "coverage_start": bars_1m[0].end_ts.isoformat() if bars_1m else None,
                "coverage_end": bars_1m[-1].end_ts.isoformat() if bars_1m else None,
            }
    finally:
        if connection is not None:
            connection.close()

    candidate_rows = _apply_cross_index_confirmation(candidate_rows_by_symbol)
    candidate_rows = attach_vix_asof(candidate_rows, vix_rows=load_vol_regime_rows(warehouse_root))
    outcome_rows = [
        _label_outcome_row(candidate=row, raw_series=raw_outcome_by_symbol[str(row["instrument"])])
        for row in candidate_rows
    ]

    layout = build_layout(output_dir)
    candidate_dataset_csv = layout["features"] / "us_open_probabilistic_pass1_candidates.csv"
    candidate_dataset_parquet = layout["features"] / "us_open_probabilistic_pass1_candidates.parquet"
    outcome_dataset_csv = layout["signals"] / "us_open_probabilistic_pass1_outcomes.csv"
    outcome_dataset_parquet = layout["signals"] / "us_open_probabilistic_pass1_outcomes.parquet"
    per_instrument_summary_csv = layout["reports"] / "us_open_probabilistic_pass1_per_instrument_summary.csv"
    pooled_summary_csv = layout["reports"] / "us_open_probabilistic_pass1_pooled_index_summary.csv"
    vix_conditioned_summary_csv = layout["reports"] / "us_open_probabilistic_pass1_vix_conditioned_summary.csv"
    dev_holdout_comparison_csv = layout["reports"] / "us_open_probabilistic_pass1_dev_holdout_comparison.csv"
    summary_json = layout["reports"] / "us_open_probabilistic_pass1_summary.json"
    summary_markdown = layout["reports"] / "us_open_probabilistic_pass1_summary.md"

    _write_csv(candidate_dataset_csv, candidate_rows)
    _write_csv(outcome_dataset_csv, outcome_rows)
    materialize_parquet_dataset(candidate_dataset_parquet, candidate_rows)
    materialize_parquet_dataset(outcome_dataset_parquet, outcome_rows)

    per_instrument_rows = _build_metric_rows(outcome_rows, group_keys=("sample_split", "instrument"))
    pooled_rows = _build_metric_rows(outcome_rows, group_keys=("sample_split",))
    vix_rows = _build_metric_rows(outcome_rows, group_keys=("sample_split", "vix_level_bucket", "vix_change_bucket"))
    comparison_rows = _build_dev_holdout_comparison_rows(outcome_rows)

    _write_csv(per_instrument_summary_csv, per_instrument_rows)
    _write_csv(pooled_summary_csv, pooled_rows)
    _write_csv(vix_conditioned_summary_csv, vix_rows)
    _write_csv(dev_holdout_comparison_csv, comparison_rows)

    summary_payload = {
        "module": "us_open_probabilistic_pass1",
        "version": PASS_VERSION,
        "setup_family": "us_open_follow_through_failed_move_baseline",
        "decision_timestamp_et": "10:00",
        "symbols": list(normalized_symbols),
        "scope": {
            "start_ts": effective_start_ts.isoformat(),
            "end_ts": effective_end_ts.isoformat(),
            "development_end": DEVELOPMENT_END.isoformat(),
            "holdout_start": HOLDOUT_START.isoformat(),
        },
        "candidate_definition": {
            "opening_drive_window": "09:30-10:00 ET",
            "candidate_rule": "all non-flat opening drives",
            "direction_definition": "10:00 close relative to 09:30 opening-drive open",
            "no_threshold_tuning": True,
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
            "per_instrument_summary_csv": str(per_instrument_summary_csv),
            "pooled_summary_csv": str(pooled_summary_csv),
            "vix_conditioned_summary_csv": str(vix_conditioned_summary_csv),
            "dev_holdout_comparison_csv": str(dev_holdout_comparison_csv),
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "storage_manifest": str(layout["storage_manifest"]),
        },
        "headline": _build_headline(outcome_rows),
        "notes": [
            "Research-only observational U.S. open baseline.",
            "No trading rules or thresholds were created.",
            "No explicit failed-move taxonomy is included yet.",
        ],
    }
    summary_json.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(
        _render_markdown(summary_payload, pooled_rows, per_instrument_rows, vix_rows, comparison_rows),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "us_open_probabilistic_pass1",
            "version": PASS_VERSION,
            "artifact_paths": summary_payload["artifact_paths"],
            "candidate_row_count": len(candidate_rows),
            "outcome_row_count": len(outcome_rows),
        },
    )
    return {
        "artifacts": summary_payload["artifact_paths"],
        "summary": summary_payload,
    }


def _build_candidate_rows(
    *,
    symbol: str,
    bars_1m: Sequence[ResearchBar],
    slope_5m_lookup: dict[str, Any],
    slope_15m_lookup: dict[str, Any],
    direction_60m_lookup: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped: dict[date, list[ResearchBar]] = defaultdict(list)
    overnight_grouped: dict[date, list[ResearchBar]] = defaultdict(list)
    for bar in bars_1m:
        local_start = bar.start_ts.astimezone(NEW_YORK)
        local_end = bar.end_ts.astimezone(NEW_YORK)
        if local_start.time() < OVERNIGHT_START and local_end.time() <= SESSION_CLOSE:
            grouped[local_start.date()].append(bar)
        if local_start.time() >= OVERNIGHT_START:
            overnight_grouped[local_start.date() + timedelta(days=1)].append(bar)
        elif local_start.time() < OPEN_START:
            overnight_grouped[local_start.date()].append(bar)

    payload: list[dict[str, Any]] = []
    for session_date in sorted(grouped):
        day_bars = sorted(grouped[session_date], key=lambda item: item.end_ts)
        opening_bars = [
            bar
            for bar in day_bars
            if OPEN_START <= bar.start_ts.astimezone(NEW_YORK).time() < DECISION_TIME
        ]
        if not opening_bars:
            continue
        decision_bar = max(opening_bars, key=lambda item: item.end_ts)
        if decision_bar.end_ts.astimezone(NEW_YORK).time() != DECISION_TIME:
            continue
        opening_open = opening_bars[0].open
        decision_close = decision_bar.close
        raw_open_drive = decision_close - opening_open
        if abs(raw_open_drive) <= 1e-9:
            continue
        direction = "UP" if raw_open_drive > 0.0 else "DOWN"
        signed_open_drive = raw_open_drive if direction == "UP" else -raw_open_drive
        opening_range_size = max(bar.high for bar in opening_bars) - min(bar.low for bar in opening_bars)
        opening_vwap = _volume_weighted_price(opening_bars)
        vwap_relation = _signed_state(decision_close - opening_vwap, deadband=0.0)
        session_open_bar = opening_bars[0]
        overnight_bars = overnight_grouped.get(session_date, [])
        overnight_direction, overnight_return = _window_direction(overnight_bars)
        slope_5m = _lookup_state(slope_5m_lookup, decision_bar.end_ts)
        slope_15m = _lookup_state(slope_15m_lookup, decision_bar.end_ts)
        trend_60m = _lookup_state(direction_60m_lookup, decision_bar.end_ts)
        premarket_bars = [
            bar
            for bar in day_bars
            if PREMARKET_START <= bar.start_ts.astimezone(NEW_YORK).time() < OPEN_START
        ]
        payload.append(
            {
                "candidate_id": f"{symbol}|{session_date.isoformat()}|{decision_bar.end_ts.isoformat()}",
                "instrument": symbol,
                "cluster": CLUSTER_MAP[symbol],
                "decision_ts": decision_bar.end_ts,
                "local_session_date": session_date.isoformat(),
                "sample_split": _sample_split(decision_bar.end_ts),
                "setup_family": "US_OPEN_FOLLOW_THROUGH_FAILED_MOVE_BASELINE",
                "direction": direction,
                "decision_close": decision_close,
                "opening_drive_open": opening_open,
                "opening_drive_return_points": round(raw_open_drive, 6),
                "opening_drive_signed_return_points": round(signed_open_drive, 6),
                "opening_range_size_points": round(opening_range_size, 6),
                "opening_vwap": round(opening_vwap, 6),
                "vwap_relation_10": vwap_relation,
                "vwap_displacement_points": round(decision_close - opening_vwap, 6),
                "premarket_range_points": round(_window_range(premarket_bars), 6),
                "premarket_bar_count": len(premarket_bars),
                "opening_bar_count": len(opening_bars),
                "slope_5m_state": str(slope_5m["state"]),
                "slope_5m_value": round(float(slope_5m["value"]), 6),
                "slope_15m_state": str(slope_15m["state"]),
                "slope_15m_value": round(float(slope_15m["value"]), 6),
                "direction_60m_state": str(trend_60m["state"]),
                "direction_60m_value": round(float(trend_60m["value"]), 6),
                "trend_60m_agreement": bool(_direction_matches_state(direction, str(trend_60m["state"]))),
                "overnight_direction": overnight_direction,
                "overnight_return_points": round(overnight_return, 6),
                "cross_index_confirmation": False,
                "cross_index_confirming_symbol_count": 0,
                "cross_index_peer_clusters": "",
            }
        )
    return payload


def _apply_cross_index_confirmation(rows_by_symbol: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], set[str]] = {}
    flattened: list[dict[str, Any]] = []
    for symbol in sorted(rows_by_symbol):
        for row in rows_by_symbol[symbol]:
            key = (str(row["local_session_date"]), str(row["direction"]))
            grouped.setdefault(key, set()).add(str(row["cluster"]))
            flattened.append(dict(row))
    payload: list[dict[str, Any]] = []
    for row in flattened:
        key = (str(row["local_session_date"]), str(row["direction"]))
        peer_clusters = sorted(cluster for cluster in grouped[key] if cluster != str(row["cluster"]))
        payload.append(
            {
                **row,
                "cross_index_confirmation": bool(peer_clusters),
                "cross_index_confirming_symbol_count": len(peer_clusters),
                "cross_index_peer_clusters": ",".join(peer_clusters),
            }
        )
    return payload


def _label_outcome_row(*, candidate: dict[str, Any], raw_series: dict[str, Any]) -> dict[str, Any]:
    timestamps = raw_series["timestamps"]
    highs = raw_series["highs"]
    lows = raw_series["lows"]
    closes = raw_series["closes"]

    decision_ts = _coerce_ts(candidate["decision_ts"])
    direction = str(candidate["direction"])
    entry_price = float(candidate["decision_close"])
    start_idx = bisect_right(timestamps, decision_ts)

    outcome_payload: dict[str, Any] = {
        "candidate_id": candidate["candidate_id"],
        "instrument": candidate["instrument"],
        "sample_split": candidate["sample_split"],
        "direction": direction,
        "decision_ts": decision_ts,
        "opening_drive_signed_return_points": candidate.get("opening_drive_signed_return_points"),
        "vix_level_bucket": candidate.get("vix_level_bucket"),
        "vix_change_bucket": candidate.get("vix_change_bucket"),
        "vix_combined_bucket": candidate.get("vix_combined_bucket"),
        "cross_index_confirmation": bool(candidate["cross_index_confirmation"]),
        "trend_60m_agreement": bool(candidate["trend_60m_agreement"]),
    }

    checkpoint_map = {
        "30m": decision_ts + timedelta(minutes=30),
        "60m": decision_ts + timedelta(minutes=60),
        "120m": decision_ts + timedelta(minutes=120),
        "1530": datetime.combine(_coerce_ts(candidate["decision_ts"]).astimezone(NEW_YORK).date(), AFTERNOON_CHECKPOINT, tzinfo=NEW_YORK).astimezone(UTC),
        "close": datetime.combine(_coerce_ts(candidate["decision_ts"]).astimezone(NEW_YORK).date(), SESSION_CLOSE, tzinfo=NEW_YORK).astimezone(UTC),
    }

    for label, checkpoint_ts in checkpoint_map.items():
        end_idx = bisect_right(timestamps, checkpoint_ts)
        exit_price = closes[end_idx - 1] if end_idx > start_idx else None
        signed_return = _signed_return(direction, entry_price, exit_price)
        outcome_payload[f"forward_return_{label}"] = round(signed_return, 6) if signed_return is not None else None
        outcome_payload[f"continuation_{label}"] = bool(signed_return is not None and signed_return > 0.0)
        outcome_payload[f"reversal_{label}"] = bool(signed_return is not None and signed_return < 0.0)

    horizon_120_end_idx = bisect_right(timestamps, checkpoint_map["120m"])
    close_end_idx = bisect_right(timestamps, checkpoint_map["close"])
    horizon_120 = _path_stats(
        direction=direction,
        entry_price=entry_price,
        decision_ts=decision_ts,
        timestamps=timestamps[start_idx:horizon_120_end_idx],
        highs=highs[start_idx:horizon_120_end_idx],
        lows=lows[start_idx:horizon_120_end_idx],
    )
    close_path = _path_stats(
        direction=direction,
        entry_price=entry_price,
        decision_ts=decision_ts,
        timestamps=timestamps[start_idx:close_end_idx],
        highs=highs[start_idx:close_end_idx],
        lows=lows[start_idx:close_end_idx],
    )
    outcome_payload.update(
        {
            "mfe_120m_points": round(horizon_120["mfe_points"], 6),
            "mae_120m_points": round(horizon_120["mae_points"], 6),
            "time_to_peak_favorable_120m_minutes": horizon_120["time_to_peak_favorable_minutes"],
            "time_to_peak_adverse_120m_minutes": horizon_120["time_to_peak_adverse_minutes"],
            "mfe_close_points": round(close_path["mfe_points"], 6),
            "mae_close_points": round(close_path["mae_points"], 6),
            "time_to_peak_favorable_close_minutes": close_path["time_to_peak_favorable_minutes"],
            "time_to_peak_adverse_close_minutes": close_path["time_to_peak_adverse_minutes"],
            "close_to_entry_outcome": _close_outcome_bucket(outcome_payload.get("forward_return_close")),
        }
    )
    return outcome_payload


def _build_metric_rows(rows: Sequence[dict[str, Any]], *, group_keys: Sequence[str]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row[key] for key in group_keys)].append(row)
    payload: list[dict[str, Any]] = []
    for key in sorted(grouped):
        bucket = grouped[key]
        record = {group_key: key[index] for index, group_key in enumerate(group_keys)}
        record["row_count"] = len(bucket)
        for horizon_label, _ in HORIZONS:
            record[f"continuation_probability_{horizon_label}"] = round(
                sum(1 for item in bucket if item.get(f"continuation_{horizon_label}")) / max(len(bucket), 1),
                6,
            )
            record[f"reversal_probability_{horizon_label}"] = round(
                sum(1 for item in bucket if item.get(f"reversal_{horizon_label}")) / max(len(bucket), 1),
                6,
            )
            record[f"avg_forward_return_{horizon_label}"] = round(
                _mean(item.get(f"forward_return_{horizon_label}") for item in bucket),
                6,
            )
        record["avg_mfe_120m_points"] = round(_mean(item.get("mfe_120m_points") for item in bucket), 6)
        record["avg_mae_120m_points"] = round(_mean(item.get("mae_120m_points") for item in bucket), 6)
        record["avg_opening_drive_signed_return_points"] = round(
            _mean(item.get("opening_drive_signed_return_points") for item in bucket),
            6,
        ) if "opening_drive_signed_return_points" in bucket[0] else 0.0
        payload.append(record)
    return payload


def _build_dev_holdout_comparison_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    all_groups = {"POOLED": rows}
    for instrument in sorted({str(row["instrument"]) for row in rows}):
        all_groups[instrument] = [row for row in rows if str(row["instrument"]) == instrument]
    for label, bucket in all_groups.items():
        development = [row for row in bucket if row["sample_split"] == "development"]
        holdout = [row for row in bucket if row["sample_split"] == "holdout"]
        payload.append(
            {
                "bucket": label,
                "development_row_count": len(development),
                "holdout_row_count": len(holdout),
                "development_continuation_probability_60m": round(
                    sum(1 for row in development if row["continuation_60m"]) / max(len(development), 1),
                    6,
                ) if development else 0.0,
                "holdout_continuation_probability_60m": round(
                    sum(1 for row in holdout if row["continuation_60m"]) / max(len(holdout), 1),
                    6,
                ) if holdout else 0.0,
                "development_avg_forward_return_60m": round(_mean(row.get("forward_return_60m") for row in development), 6),
                "holdout_avg_forward_return_60m": round(_mean(row.get("forward_return_60m") for row in holdout), 6),
                "development_avg_forward_return_close": round(_mean(row.get("forward_return_close") for row in development), 6),
                "holdout_avg_forward_return_close": round(_mean(row.get("forward_return_close") for row in holdout), 6),
            }
        )
    return payload


def _build_headline(outcome_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    development = [row for row in outcome_rows if row["sample_split"] == "development"]
    holdout = [row for row in outcome_rows if row["sample_split"] == "holdout"]
    return {
        "development_rows": len(development),
        "holdout_rows": len(holdout),
        "development_pooled_continuation_probability_60m": round(
            sum(1 for row in development if row["continuation_60m"]) / max(len(development), 1),
            6,
        ) if development else 0.0,
        "holdout_pooled_continuation_probability_60m": round(
            sum(1 for row in holdout if row["continuation_60m"]) / max(len(holdout), 1),
            6,
        ) if holdout else 0.0,
        "development_pooled_avg_forward_return_60m": round(_mean(row.get("forward_return_60m") for row in development), 6),
        "holdout_pooled_avg_forward_return_60m": round(_mean(row.get("forward_return_60m") for row in holdout), 6),
    }


def _render_markdown(
    summary_payload: dict[str, Any],
    pooled_rows: Sequence[dict[str, Any]],
    per_instrument_rows: Sequence[dict[str, Any]],
    vix_rows: Sequence[dict[str, Any]],
    comparison_rows: Sequence[dict[str, Any]],
) -> str:
    headline = summary_payload["headline"]
    lines = [
        "# US Open Probabilistic Pass 1",
        "",
        "## Scope",
        "",
        f"- symbols: {', '.join(summary_payload['symbols'])}",
        "- decision timestamp: `10:00 ET`",
        "- opening drive window: `09:30-10:00 ET`",
        "- design: observational baseline only",
        "",
        "## Headline",
        "",
        f"- development rows: `{headline['development_rows']}`",
        f"- holdout rows: `{headline['holdout_rows']}`",
        f"- development pooled continuation 60m: `{headline['development_pooled_continuation_probability_60m']:.2%}`",
        f"- holdout pooled continuation 60m: `{headline['holdout_pooled_continuation_probability_60m']:.2%}`",
        f"- development pooled avg 60m return: `{headline['development_pooled_avg_forward_return_60m']:.3f}`",
        f"- holdout pooled avg 60m return: `{headline['holdout_pooled_avg_forward_return_60m']:.3f}`",
        "",
        "## Pooled Summary",
        "",
    ]
    for row in pooled_rows:
        lines.append(
            f"- `{row['sample_split']}`: rows `{row['row_count']}`, cont60 `{row['continuation_probability_60m']:.2%}`, "
            f"rev60 `{row['reversal_probability_60m']:.2%}`, avg60 `{row['avg_forward_return_60m']:.3f}`, avgClose `{row['avg_forward_return_close']:.3f}`"
        )
    lines.extend(["", "## Per Instrument", ""])
    for row in per_instrument_rows:
        lines.append(
            f"- `{row['sample_split']} {row['instrument']}`: rows `{row['row_count']}`, cont60 `{row['continuation_probability_60m']:.2%}`, "
            f"avg60 `{row['avg_forward_return_60m']:.3f}`, avgClose `{row['avg_forward_return_close']:.3f}`"
        )
    top_vix = sorted(vix_rows, key=lambda item: (item["sample_split"], -item["row_count"]))[:8]
    lines.extend(["", "## VIX Conditioning", ""])
    for row in top_vix:
        lines.append(
            f"- `{row['sample_split']} {row['vix_level_bucket']}/{row['vix_change_bucket']}`: rows `{row['row_count']}`, "
            f"cont60 `{row['continuation_probability_60m']:.2%}`, avg60 `{row['avg_forward_return_60m']:.3f}`"
        )
    lines.extend(["", "## Dev vs Holdout", ""])
    for row in comparison_rows:
        lines.append(
            f"- `{row['bucket']}`: dev rows `{row['development_row_count']}`, holdout rows `{row['holdout_row_count']}`, "
            f"dev cont60 `{row['development_continuation_probability_60m']:.2%}`, holdout cont60 `{row['holdout_continuation_probability_60m']:.2%}`"
        )
    return "\n".join(lines) + "\n"


def _window_direction(bars: Sequence[ResearchBar]) -> tuple[str, float]:
    if not bars:
        return "UNKNOWN", 0.0
    ordered = sorted(bars, key=lambda item: item.end_ts)
    raw = ordered[-1].close - ordered[0].open
    if raw > 0.0:
        return "UP", raw
    if raw < 0.0:
        return "DOWN", raw
    return "FLAT", raw


def _window_range(bars: Sequence[ResearchBar]) -> float:
    if not bars:
        return 0.0
    return max(bar.high for bar in bars) - min(bar.low for bar in bars)


def _signed_state(value: float, *, deadband: float = 0.10) -> str:
    if value > deadband:
        return "ABOVE"
    if value < -deadband:
        return "BELOW"
    return "AT"


def _direction_matches_state(direction: str, state: str) -> bool:
    return (direction == "UP" and state == "UP") or (direction == "DOWN" and state == "DOWN")


def _volume_weighted_price(bars: Sequence[ResearchBar]) -> float:
    total_volume = sum(max(bar.volume, 0) for bar in bars)
    if total_volume <= 0:
        return bars[-1].close
    return sum(bar.close * max(bar.volume, 0) for bar in bars) / total_volume


def _signed_return(direction: str, entry_price: float, exit_price: float | None) -> float | None:
    if exit_price is None:
        return None
    raw = float(exit_price) - entry_price
    return raw if direction == "UP" else -raw


def _path_stats(
    *,
    direction: str,
    entry_price: float,
    decision_ts: datetime,
    timestamps: Sequence[datetime],
    highs: Sequence[float],
    lows: Sequence[float],
) -> dict[str, Any]:
    if not timestamps:
        return {
            "mfe_points": 0.0,
            "mae_points": 0.0,
            "time_to_peak_favorable_minutes": None,
            "time_to_peak_adverse_minutes": None,
        }
    favorable_values = [
        (float(high) - entry_price) if direction == "UP" else (entry_price - float(low))
        for high, low in zip(highs, lows, strict=False)
    ]
    adverse_values = [
        (entry_price - float(low)) if direction == "UP" else (float(high) - entry_price)
        for high, low in zip(highs, lows, strict=False)
    ]
    mfe_points = max(favorable_values) if favorable_values else 0.0
    mae_points = max(adverse_values) if adverse_values else 0.0
    return {
        "mfe_points": mfe_points,
        "mae_points": mae_points,
        "time_to_peak_favorable_minutes": _minutes_to_first_peak(timestamps, decision_ts, favorable_values, mfe_points),
        "time_to_peak_adverse_minutes": _minutes_to_first_peak(timestamps, decision_ts, adverse_values, mae_points),
    }


def _minutes_to_first_peak(
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


def _close_outcome_bucket(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value > 0.0:
        return "CONTINUATION"
    if value < 0.0:
        return "REVERSAL"
    return "FLAT"
