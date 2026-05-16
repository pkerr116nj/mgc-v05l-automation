"""Offline warehouse archive builder for Track B Entry Acceptance research.

This command reads research warehouse bars/features only. It does not consume
live runtime state, run lanes, submit orders, calculate PnL, or change strategy
behavior.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.execution_core.track_b_entry_acceptance import (
    INPUT_MODE_REPLAY_RESEARCH,
    SOURCE_CATEGORY_RESEARCH,
    EntryAcceptanceThresholds,
    build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload,
    build_entry_acceptance_state,
)
from mgc_v05l.research.warehouse_historical_evaluator._warehouse_common import read_parquet_rows


DEFAULT_OUTPUT_ROOT = Path("outputs/reports/entry_acceptance_research")
DEFAULT_WAREHOUSE_ROOT = Path("outputs/warehouse_historical_evaluator_basket_q1_structural_exit_fix")
PILOT_FAMILY = "asiaEarlyNormalBreakoutRetestHoldLong"
LEGACY_FAMILY = "asiaEarlyNormalBreakoutRetestHoldTurn"
EXACT_FLAG_COLUMN = "asia_early_normal_breakout_retest_hold_long_turn_candidate"
STATE_KEY = "asia_early_normal_breakout_retest_hold_long_state"
FEATURES_KEY = "asia_early_normal_breakout_retest_hold_long_features"
RULE_ID = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
TIMEFRAME = "5m"
NY = ZoneInfo("America/New_York")

BREAKOUT_ABS_SLOPE_MAX = Decimal("0.20")
BREAKOUT_MIN_RANGE_EXPANSION_RATIO = Decimal("0.85")
BREAKOUT_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")
ANTI_CHURN_BARS = 5


@dataclass(frozen=True)
class WarehouseInputs:
    instrument: str
    bars_path: Path
    features_path: Path
    lane_candidates_path: Path | None


def build_report(
    *,
    warehouse_root: Path = DEFAULT_WAREHOUSE_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    instruments: Sequence[str] = ("MGC", "GC"),
    year: str | None = None,
    shard_id: str | None = None,
) -> dict[str, Any]:
    """Build an enriched offline archive from warehouse 5m bars/features."""

    warehouse_root = Path(warehouse_root)
    _reject_non_research_warehouse_root(warehouse_root)
    inputs = _discover_warehouse_inputs(
        warehouse_root=warehouse_root,
        instruments=instruments,
        year=year,
        shard_id=shard_id,
    )
    rows: list[dict[str, Any]] = []
    source_summaries: list[dict[str, Any]] = []
    for item in inputs:
        instrument_rows = _archive_rows_for_input(item)
        rows.extend(instrument_rows)
        source_summaries.append(_source_summary(item, instrument_rows))

    output_root.mkdir(parents=True, exist_ok=True)
    archive_jsonl = output_root / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive.jsonl"
    summary_json = output_root / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive_summary.json"
    summary_md = output_root / "asia_early_normal_breakout_retest_hold_enriched_candidate_archive_summary.md"
    scoring_json = output_root / "asia_early_normal_breakout_retest_hold_acceptance_distribution_v2.json"
    scoring_md = output_root / "asia_early_normal_breakout_retest_hold_acceptance_distribution_v2.md"
    forward_json = output_root / "asia_early_normal_breakout_retest_hold_forward_return_diagnostic_v1.json"
    forward_md = output_root / "asia_early_normal_breakout_retest_hold_forward_return_diagnostic_v1.md"
    backtest_json = output_root / "asia_early_normal_breakout_retest_hold_diagnostic_backtest_v1.json"
    backtest_md = output_root / "asia_early_normal_breakout_retest_hold_diagnostic_backtest_v1.md"

    archive_jsonl.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    scoring_report = score_archive_rows(
        rows,
        archive_path=archive_jsonl,
        scoring_json=scoring_json,
        scoring_md=scoring_md,
        forward_json=forward_json,
        forward_md=forward_md,
        backtest_json=backtest_json,
        backtest_md=backtest_md,
    )
    report = _summarize_archive(
        rows,
        warehouse_root=warehouse_root,
        output_root=output_root,
        source_summaries=source_summaries,
        archive_jsonl=archive_jsonl,
        summary_json=summary_json,
        summary_md=summary_md,
        scoring_report=scoring_report,
    )
    summary_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_md.write_text(_markdown_report(report), encoding="utf-8")
    return report


def _archive_rows_for_input(inputs: WarehouseInputs) -> list[dict[str, Any]]:
    bars = _read_parquet_records(inputs.bars_path)
    features = _read_parquet_records(inputs.features_path)
    lane_candidates = _read_parquet_records(inputs.lane_candidates_path) if inputs.lane_candidates_path else []
    return build_archive_rows(
        instrument=inputs.instrument,
        bars=bars,
        features=features,
        lane_candidates=lane_candidates,
        bars_source_path=inputs.bars_path,
        features_source_path=inputs.features_path,
        lane_candidates_source_path=inputs.lane_candidates_path,
    )


def build_archive_rows(
    *,
    instrument: str,
    bars: Sequence[Mapping[str, Any]],
    features: Sequence[Mapping[str, Any]],
    lane_candidates: Sequence[Mapping[str, Any]] = (),
    bars_source_path: Path | None = None,
    features_source_path: Path | None = None,
    lane_candidates_source_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Construct enriched candidate/envelope rows from completed 5m bars."""

    sorted_bars = sorted((bar for bar in bars if str(bar.get("timeframe") or TIMEFRAME) == TIMEFRAME), key=_bar_ts)
    features_by_ts = {_iso(row.get("decision_ts")): row for row in features}
    lane_by_feature_bar_id = {
        str(row.get("feature_bar_id")): row
        for row in lane_candidates
        if str(row.get("family") or "") == LEGACY_FAMILY
    }
    lane_by_ts = {
        _iso(row.get("decision_ts")): row
        for row in lane_candidates
        if str(row.get("family") or "") == LEGACY_FAMILY
    }

    rows: list[dict[str, Any]] = []
    prior_exact_index: int | None = None
    for index, current in enumerate(sorted_bars):
        ts = _iso(current.get("bar_ts"))
        feature_row = features_by_ts.get(ts, {})
        exact_flag = feature_row.get(EXACT_FLAG_COLUMN) is True
        feature_bar_id = str(feature_row.get("bar_id") or f"{instrument}:5m:{ts}")
        lane_candidate = lane_by_feature_bar_id.get(feature_bar_id) or lane_by_ts.get(ts)
        prior_bars_since_setup = 1000 if prior_exact_index is None else max(0, index - prior_exact_index)
        row = _archive_row(
            instrument=instrument,
            index=index,
            bars=sorted_bars,
            features_by_ts=features_by_ts,
            feature_row=feature_row,
            exact_flag=exact_flag,
            lane_candidate=lane_candidate,
            prior_bars_since_setup=prior_bars_since_setup,
            bars_source_path=bars_source_path,
            features_source_path=features_source_path,
            lane_candidates_source_path=lane_candidates_source_path,
        )
        rows.append(row)
        if exact_flag:
            prior_exact_index = index
    return rows


def score_archive_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    archive_path: Path,
    scoring_json: Path | None = None,
    scoring_md: Path | None = None,
    forward_json: Path | None = None,
    forward_md: Path | None = None,
    backtest_json: Path | None = None,
    backtest_md: Path | None = None,
) -> dict[str, Any]:
    """Score enriched archive rows with the offline Entry Acceptance scorer."""

    scored_rows: list[dict[str, Any]] = []
    thresholds = EntryAcceptanceThresholds(
        min_completed_candles=3,
        preferred_completed_candles=8,
        stale_after_intervals=999999,
        max_missing_gap_intervals=999999,
    )
    for row in rows:
        payload = _scorer_payload_from_archive_row(row, archive_path=archive_path)
        state = build_entry_acceptance_state(
            payload,
            now=row.get("timestamp"),
            thresholds=thresholds,
        )
        scored_rows.append(_scored_row(row, state))

    report = _summarize_scored_rows(
        scored_rows,
        archive_path=archive_path,
        scoring_json=scoring_json,
        scoring_md=scoring_md,
    )
    forward_report = _forward_return_diagnostic(
        rows=rows,
        scored_rows=scored_rows,
        archive_path=archive_path,
        forward_json=forward_json,
        forward_md=forward_md,
    )
    report["forward_return_diagnostic_json"] = forward_report.get("forward_json")
    report["forward_return_diagnostic_markdown"] = forward_report.get("forward_markdown")
    report["forward_return_diagnostic_summary"] = {
        "candidate_bucket_counts": forward_report.get("candidate_bucket_counts"),
        "deduped_episode_counts": forward_report.get("deduped_episode_counts"),
        "forward_return_table": forward_report.get("forward_return_table"),
        "classifications_interpretation": forward_report.get("preliminary_interpretation"),
    }
    backtest_report = _diagnostic_backtest(
        rows=rows,
        scored_rows=scored_rows,
        archive_path=archive_path,
        backtest_json=backtest_json,
        backtest_md=backtest_md,
    )
    report["diagnostic_backtest_json"] = backtest_report.get("backtest_json")
    report["diagnostic_backtest_markdown"] = backtest_report.get("backtest_markdown")
    report["diagnostic_backtest_summary"] = {
        "episode_counts": backtest_report.get("episode_counts"),
        "headline_comparison": backtest_report.get("headline_comparison"),
        "preliminary_interpretation": backtest_report.get("preliminary_interpretation"),
    }
    if scoring_json is not None:
        scoring_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if scoring_md is not None:
        scoring_md.write_text(_scoring_markdown_report(report), encoding="utf-8")
    if forward_json is not None:
        forward_json.write_text(json.dumps(forward_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if forward_md is not None:
        forward_md.write_text(_forward_markdown_report(forward_report), encoding="utf-8")
    if backtest_json is not None:
        backtest_json.write_text(json.dumps(backtest_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if backtest_md is not None:
        backtest_md.write_text(_backtest_markdown_report(backtest_report), encoding="utf-8")
    return report


def _archive_row(
    *,
    instrument: str,
    index: int,
    bars: Sequence[Mapping[str, Any]],
    features_by_ts: Mapping[str | None, Mapping[str, Any]],
    feature_row: Mapping[str, Any],
    exact_flag: bool,
    lane_candidate: Mapping[str, Any] | None,
    prior_bars_since_setup: int,
    bars_source_path: Path | None,
    features_source_path: Path | None,
    lane_candidates_source_path: Path | None,
) -> dict[str, Any]:
    current = bars[index]
    ts = _iso(current.get("bar_ts"))
    session = _research_session_phase(_parse_datetime(current.get("bar_ts")))
    prior = bars[index - 2] if index >= 2 else None
    breakout = bars[index - 1] if index >= 1 else None
    breakout_features = _features_for_prior_bar(bars, index, features_by_ts, feature_row)
    enriched = _breakout_retest_hold_context(
        prior_bar=prior,
        breakout_bar=breakout,
        signal_bar=current,
        breakout_feature_row=breakout_features,
        signal_feature_row=feature_row,
    )
    no_snap_conflict = feature_row.get("bull_snap_turn_candidate") is not True
    state = {
        "derivative_phase": session,
        "session_asia": session.startswith("ASIA"),
        "allow_asia": True,
        "asia_early_or_gc_mgc_london_open": _asia_early_or_gc_mgc_london_open(_parse_datetime(current.get("bar_ts"))),
        "no_first_bull_snap_turn": no_snap_conflict,
        "prior_bars_since_long_setup": prior_bars_since_setup,
        "prior_bars_since_long_setup_gt_anti_churn": prior_bars_since_setup > ANTI_CHURN_BARS,
        "timeframe": TIMEFRAME,
    }
    lane_candidate_id = None if lane_candidate is None else lane_candidate.get("candidate_id")
    scoring_candles = [_candle_payload(bar) for bar in bars[max(0, index - 2) : index + 1]]
    return {
        "schema_version": "track_b_entry_acceptance_enriched_candidate_archive_v1",
        "instrument": instrument,
        "timestamp": ts,
        "session": session,
        "timeframe": TIMEFRAME,
        "candidate_family": PILOT_FAMILY,
        "source_event_family": LEGACY_FAMILY,
        "candidate_flag": exact_flag,
        "current_exact_rule_flag": exact_flag,
        "candidate_id": lane_candidate_id,
        "feature_bar_id": feature_row.get("bar_id") or f"{instrument}:5m:{ts}",
        "signal_candle": _candle_payload(current),
        "scoring_candles": scoring_candles,
        "breakout_breaks_prior_1_high": enriched["breakout_breaks_prior_1_high"],
        "signal_retests_and_holds_breakout_level": enriched["signal_retests_and_holds_breakout_level"],
        "breakout_bar_slope_is_flat": enriched["breakout_bar_slope_is_flat"],
        "breakout_bar_expansion_is_normal": enriched["breakout_bar_expansion_is_normal"],
        "breakout_level": _json_number(enriched["breakout_level"]),
        "breakout_reference": {
            "prior_bar_timestamp": _iso(prior.get("bar_ts")) if prior else None,
            "breakout_bar_timestamp": _iso(breakout.get("bar_ts")) if breakout else None,
            "breakout_bar_high": _json_number(_decimal_or_none(breakout.get("high")) if breakout else None),
            "prior_bar_high": _json_number(_decimal_or_none(prior.get("high")) if prior else None),
        },
        "retest_depth": _json_number(enriched["retest_depth_ticks_or_points"]),
        "retest_depth_ticks_or_points": _json_number(enriched["retest_depth_ticks_or_points"]),
        "retest_depth_normalized": _json_number(enriched["retest_depth_normalized"]),
        "hold_margin": _json_number(enriched["hold_margin_ticks_or_points"]),
        "hold_margin_ticks_or_points": _json_number(enriched["hold_margin_ticks_or_points"]),
        "hold_margin_normalized": _json_number(enriched["hold_margin_normalized"]),
        "bars_since_breakout": enriched["bars_since_breakout"],
        "bars_since_retest": enriched["bars_since_retest"],
        "range_expansion_ratio": _json_number(enriched["range_expansion_ratio"]),
        "close_location": _json_number(enriched["close_location"]),
        "body_to_range_ratio": _json_number(enriched["body_to_range_ratio"]),
        "breakout_normalized_slope": _json_number(enriched["breakout_normalized_slope"]),
        "churn_score": _json_number(_churn_score(prior_bars_since_setup)),
        "snap_turn_conflict_strength": 0.0 if no_snap_conflict else 1.0,
        "bull_snap_turn_candidate": feature_row.get("bull_snap_turn_candidate") is True,
        "bear_snap_turn_candidate": feature_row.get("bear_snap_turn_candidate") is True,
        "state_context": state,
        "source_feature_values": {
            "atr": _json_number(_decimal_or_none(feature_row.get("atr"))),
            "bar_range": _json_number(_decimal_or_none(feature_row.get("bar_range"))),
            "body_size": _json_number(_decimal_or_none(feature_row.get("body_size"))),
            "vol_ratio": _json_number(_decimal_or_none(feature_row.get("vol_ratio"))),
            "velocity": _json_number(_decimal_or_none(feature_row.get("velocity"))),
            "velocity_delta": _json_number(_decimal_or_none(feature_row.get("velocity_delta"))),
            "vwap": _json_number(_decimal_or_none(feature_row.get("vwap"))),
        },
        "data_quality": {
            "has_min_breakout_history": index >= 2,
            "has_feature_row": bool(feature_row),
            "has_lane_candidate_row": lane_candidate is not None,
            "enriched_field_completeness": _enriched_field_completeness(enriched),
        },
        "provenance": {
            "source_mode": "RESEARCH_WAREHOUSE_OFFLINE",
            "runtime_truth": False,
            "strategy_behavior_changed": False,
            "trade_simulation_performed": False,
            "pnl_calculated": False,
            "bars_source_path": str(bars_source_path) if bars_source_path else None,
            "features_source_path": str(features_source_path) if features_source_path else None,
            "lane_candidates_source_path": str(lane_candidates_source_path) if lane_candidates_source_path else None,
            "bar_provenance_tag": current.get("provenance_tag"),
            "feature_provenance_tag": feature_row.get("provenance_tag"),
            "feature_materialized_ts": _iso(feature_row.get("materialized_ts")),
            "source_data_source": current.get("source_data_source"),
            "derived_rule": current.get("derived_rule"),
            "materialized_from_raw_version": current.get("materialized_from_raw_version"),
        },
        "safety_flags": {
            "strategy_authority": False,
            "broker_state_mutated": False,
            "submit_attempted": False,
            "order_intent_created": False,
            "lifecycle_mutated": False,
            "runtime_trade_eligible": False,
        },
    }


def _scorer_payload_from_archive_row(row: Mapping[str, Any], *, archive_path: Path) -> dict[str, Any]:
    signal_candle = row.get("signal_candle") if isinstance(row.get("signal_candle"), Mapping) else {}
    state_context = row.get("state_context") if isinstance(row.get("state_context"), Mapping) else {}
    event_payload = {
        "instrument_family": row.get("instrument"),
        "source_id": row.get("feature_bar_id"),
        "lane_id": row.get("candidate_id") or row.get("feature_bar_id"),
        "strategy_id": RULE_ID,
        "signal_family": RULE_ID,
        "rule_mode": RULE_ID,
        "timeframe": row.get("timeframe") or TIMEFRAME,
        "candle_timestamp": row.get("timestamp"),
        "observed_at": row.get("timestamp"),
        "generated_at": row.get("timestamp"),
        "open": signal_candle.get("open"),
        "high": signal_candle.get("high"),
        "low": signal_candle.get("low"),
        "close": signal_candle.get("close"),
        "last": signal_candle.get("close"),
        "volume": signal_candle.get("volume"),
        "metadata": {
            "source_payload_path": str(archive_path),
            "source_bar_count": len(row.get("scoring_candles") or []),
            "source_artifact_mode": "RESEARCH_WAREHOUSE_OFFLINE",
            STATE_KEY: dict(state_context),
            FEATURES_KEY: _feature_payload_from_archive_row(row),
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    return build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event_payload,
        completed_candles=list(row.get("scoring_candles") or []),
        input_source_path=archive_path,
        input_source_category=SOURCE_CATEGORY_RESEARCH,
        input_mode=INPUT_MODE_REPLAY_RESEARCH,
    )


def _feature_payload_from_archive_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "feature_version": "track_b_entry_acceptance_research_archive_v1",
        "calibration_profile": "warehouse_q1_2024_structural_exit_fix",
        "breakout_normalized_slope": row.get("breakout_normalized_slope"),
        "breakout_abs_slope_max": float(BREAKOUT_ABS_SLOPE_MAX),
        "breakout_range_expansion_ratio": row.get("range_expansion_ratio"),
        "breakout_min_range_expansion_ratio": float(BREAKOUT_MIN_RANGE_EXPANSION_RATIO),
        "breakout_max_range_expansion_ratio": float(BREAKOUT_MAX_RANGE_EXPANSION_RATIO),
        "breakout_level": row.get("breakout_level"),
        "retest_depth_ticks_or_points": row.get("retest_depth_ticks_or_points"),
        "retest_depth_normalized": row.get("retest_depth_normalized"),
        "hold_margin_ticks_or_points": row.get("hold_margin_ticks_or_points"),
        "hold_margin_normalized": row.get("hold_margin_normalized"),
        "bars_since_breakout": row.get("bars_since_breakout"),
        "bars_since_retest": row.get("bars_since_retest"),
        "range_expansion_ratio": row.get("range_expansion_ratio"),
        "close_location": row.get("close_location"),
        "body_to_range_ratio": row.get("body_to_range_ratio"),
        "prior_bars_since_long_setup": (row.get("state_context") or {}).get("prior_bars_since_long_setup")
        if isinstance(row.get("state_context"), Mapping)
        else None,
        "anti_churn_bars": ANTI_CHURN_BARS,
        "anti_churn_margin_bars": (row.get("state_context") or {}).get("prior_bars_since_long_setup", 0)
        - ANTI_CHURN_BARS
        if isinstance(row.get("state_context"), Mapping)
        else None,
        "churn_score": row.get("churn_score"),
        "snap_turn_conflict_strength": row.get("snap_turn_conflict_strength"),
        "breakout_bar_slope_is_flat": row.get("breakout_bar_slope_is_flat") is True,
        "breakout_bar_expansion_is_normal": row.get("breakout_bar_expansion_is_normal") is True,
        "breakout_breaks_prior_1_high": row.get("breakout_breaks_prior_1_high") is True,
        "signal_retests_and_holds_breakout_level": row.get("signal_retests_and_holds_breakout_level") is True,
    }


def _scored_row(row: Mapping[str, Any], state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "instrument": row.get("instrument"),
        "timestamp": row.get("timestamp"),
        "session": row.get("session"),
        "timeframe": row.get("timeframe"),
        "candidate_family": row.get("candidate_family"),
        "current_exact_rule_flag": row.get("current_exact_rule_flag") is True,
        "feature_bar_id": row.get("feature_bar_id"),
        "candidate_id": row.get("candidate_id"),
        "acceptance_class": state.get("acceptance_class"),
        "acceptance_score": state.get("acceptance_score"),
        "confidence": state.get("confidence"),
        "supporting_reasons": state.get("supporting_reasons") or [],
        "failure_reasons": state.get("failure_reasons") or [],
        "dimension_scores": state.get("dimension_scores") or {},
        "numeric_context": {
            "breakout_level": row.get("breakout_level"),
            "retest_depth": row.get("retest_depth"),
            "hold_margin": row.get("hold_margin"),
            "range_expansion_ratio": row.get("range_expansion_ratio"),
            "close_location": row.get("close_location"),
            "body_to_range_ratio": row.get("body_to_range_ratio"),
            "churn_score": row.get("churn_score"),
            "snap_turn_conflict_strength": row.get("snap_turn_conflict_strength"),
        },
        "safety_flags": state.get("safety_flags") or {},
    }


def _summarize_scored_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    archive_path: Path,
    scoring_json: Path | None,
    scoring_md: Path | None,
) -> dict[str, Any]:
    class_counts = Counter(str(row.get("acceptance_class") or "UNKNOWN") for row in scored_rows)
    instrument_counts = Counter(str(row.get("instrument") or "UNKNOWN") for row in scored_rows)
    cross_tab: dict[str, dict[str, int]] = {}
    for row in scored_rows:
        flag = str(row.get("current_exact_rule_flag") is True)
        klass = str(row.get("acceptance_class") or "UNKNOWN")
        cross_tab.setdefault(flag, {})
        cross_tab[flag][klass] = cross_tab[flag].get(klass, 0) + 1
    supporting = Counter(reason for row in scored_rows for reason in row.get("supporting_reasons", []))
    failures = Counter(reason for row in scored_rows for reason in row.get("failure_reasons", []))
    near_degraded_non_exact = [
        row
        for row in scored_rows
        if row.get("current_exact_rule_flag") is not True
        and row.get("acceptance_class") in {"NEAR_STRUCTURAL_MATCH", "DEGRADED_BUT_VALID_MATCH"}
    ]
    report = {
        "schema_version": "track_b_entry_acceptance_historical_scoring_v2",
        "pilot_family": PILOT_FAMILY,
        "mode": "RESEARCH_WAREHOUSE_ARCHIVE_SCORING_ONLY",
        "archive_path": str(archive_path),
        "scoring_json": str(scoring_json) if scoring_json else None,
        "scoring_markdown": str(scoring_md) if scoring_md else None,
        "authoritative_runtime_truth": False,
        "strategy_behavior_changed": False,
        "trade_simulation_performed": False,
        "pnl_calculated": False,
        "total_rows_scanned": len(scored_rows),
        "acceptance_class_counts": dict(sorted(class_counts.items())),
        "counts_by_instrument": dict(sorted(instrument_counts.items())),
        "exact_candidate_flag_acceptance_class_cross_tab": {
            key: dict(sorted(value.items())) for key, value in sorted(cross_tab.items())
        },
        "near_degraded_not_exact_rule_flag_count": len(near_degraded_non_exact),
        "near_degraded_not_exact_rule_flag_breakdown": dict(
            sorted(Counter(str(row.get("acceptance_class")) for row in near_degraded_non_exact).items())
        ),
        "top_supporting_reasons": _top_counts(supporting),
        "top_failure_reasons": _top_counts(failures),
        "sample_rows": {
            "exact": _scoring_examples(scored_rows, "EXACT_STRUCTURAL_MATCH"),
            "near": _scoring_examples(scored_rows, "NEAR_STRUCTURAL_MATCH"),
            "degraded": _scoring_examples(scored_rows, "DEGRADED_BUT_VALID_MATCH"),
            "invalid": _scoring_examples(scored_rows, "STRUCTURALLY_INVALID"),
        },
        "classifications_look_sane": _classifications_look_sane(scored_rows),
        "sanity_notes": _scoring_sanity_notes(scored_rows),
    }
    return report


def _top_counts(counter: Counter[str], *, limit: int = 10) -> list[dict[str, Any]]:
    return [{"reason": reason, "count": count} for reason, count in counter.most_common(limit)]


def _scoring_examples(rows: Sequence[Mapping[str, Any]], acceptance_class: str) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for row in rows:
        if row.get("acceptance_class") != acceptance_class:
            continue
        examples.append(
            {
                "instrument": row.get("instrument"),
                "timestamp": row.get("timestamp"),
                "session": row.get("session"),
                "current_exact_rule_flag": row.get("current_exact_rule_flag"),
                "feature_bar_id": row.get("feature_bar_id"),
                "acceptance_score": row.get("acceptance_score"),
                "confidence": row.get("confidence"),
                "supporting_reasons": row.get("supporting_reasons"),
                "failure_reasons": row.get("failure_reasons"),
                "numeric_context": row.get("numeric_context"),
            }
        )
        if len(examples) >= 3:
            break
    return examples


def _classifications_look_sane(rows: Sequence[Mapping[str, Any]]) -> bool:
    if not rows:
        return False
    class_counts = Counter(str(row.get("acceptance_class") or "UNKNOWN") for row in rows)
    low_conf_share = class_counts.get("LOW_CONFIDENCE_INSUFFICIENT_DATA", 0) / len(rows)
    exact_flag_rows = [row for row in rows if row.get("current_exact_rule_flag") is True]
    exact_flag_valid = sum(
        1
        for row in exact_flag_rows
        if row.get("acceptance_class")
        in {"EXACT_STRUCTURAL_MATCH", "NEAR_STRUCTURAL_MATCH", "DEGRADED_BUT_VALID_MATCH"}
    )
    has_near_or_degraded = bool(
        class_counts.get("NEAR_STRUCTURAL_MATCH", 0) or class_counts.get("DEGRADED_BUT_VALID_MATCH", 0)
    )
    return low_conf_share < 0.02 and bool(exact_flag_rows) and exact_flag_valid > 0 and has_near_or_degraded


def _scoring_sanity_notes(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return ["No enriched archive rows were available to score."]
    notes = [
        "Rows were scored from the enriched warehouse candidate/envelope archive, not compact strategy-study artifacts.",
        "Distribution includes all completed Q1 2024 5m rows, so structurally invalid rows are expected to dominate.",
        "Near/degraded non-exact rows are useful near-miss candidates for later review; no PnL was calculated.",
    ]
    if any(row.get("acceptance_class") == "LOW_CONFIDENCE_INSUFFICIENT_DATA" for row in rows):
        notes.append("Low-confidence rows are expected at shard warm-up where fewer than three scoring candles exist.")
    return notes


def _scoring_markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Entry Acceptance Historical Scoring v2",
        "",
        f"- pilot_family: `{report['pilot_family']}`",
        f"- mode: `{report['mode']}`",
        f"- total_rows_scanned: `{report['total_rows_scanned']}`",
        f"- acceptance_class_counts: `{report['acceptance_class_counts']}`",
        f"- counts_by_instrument: `{report['counts_by_instrument']}`",
        f"- exact_candidate_flag_acceptance_class_cross_tab: `{report['exact_candidate_flag_acceptance_class_cross_tab']}`",
        f"- near_degraded_not_exact_rule_flag_count: `{report['near_degraded_not_exact_rule_flag_count']}`",
        f"- classifications_look_sane: `{report['classifications_look_sane']}`",
        "",
        "## Top Reasons",
        "",
        f"- supporting: `{report['top_supporting_reasons']}`",
        f"- failures: `{report['top_failure_reasons']}`",
        "",
        "## Notes",
        "",
        *[f"- {note}" for note in report["sanity_notes"]],
        "",
    ]
    return "\n".join(lines)


def _forward_return_diagnostic(
    *,
    rows: Sequence[Mapping[str, Any]],
    scored_rows: Sequence[Mapping[str, Any]],
    archive_path: Path,
    forward_json: Path | None,
    forward_md: Path | None,
) -> dict[str, Any]:
    paired = _with_instrument_indices(
        [_diagnostic_candidate(row, scored) for row, scored in zip(rows, scored_rows, strict=False)]
    )
    instrument_rows = _instrument_rows(paired)
    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in _candidate_bucket_order()}
    for candidate in paired:
        bucket = _candidate_bucket(candidate)
        if bucket:
            buckets[bucket].append(candidate)

    density = _candidate_density(buckets=buckets, all_rows=paired)
    forward_table: dict[str, dict[str, Any]] = {}
    mfe_mae_table: dict[str, dict[str, Any]] = {}
    for bucket, candidates in buckets.items():
        forward_table[bucket] = {
            f"{horizon}_bars": _return_stats(
                [
                    _forward_return(candidate, instrument_rows=instrument_rows, horizon=horizon)
                    for candidate in candidates
                ]
            )
            for horizon in (3, 6, 12, 24)
        }
        mfe_mae_table[bucket] = {
            f"{horizon}_bars": _mfe_mae_stats(
                [
                    _mfe_mae(candidate, instrument_rows=instrument_rows, horizon=horizon)
                    for candidate in candidates
                ]
            )
            for horizon in (6, 12, 24)
        }

    report = {
        "schema_version": "track_b_entry_acceptance_forward_return_diagnostic_v1",
        "pilot_family": PILOT_FAMILY,
        "mode": "RESEARCH_WAREHOUSE_FORWARD_RETURN_DIAGNOSTIC_ONLY",
        "archive_path": str(archive_path),
        "forward_json": str(forward_json) if forward_json else None,
        "forward_markdown": str(forward_md) if forward_md else None,
        "authoritative_runtime_truth": False,
        "strategy_behavior_changed": False,
        "trade_simulation_performed": False,
        "pnl_calculated": False,
        "transaction_costs_modeled": False,
        "source_period": _source_period(rows),
        "total_archive_rows": len(rows),
        "candidate_bucket_counts": {bucket: len(candidates) for bucket, candidates in buckets.items()},
        "candidate_density": density,
        "deduped_episode_counts": {
            bucket: _dedup_episode_counts(candidates) for bucket, candidates in buckets.items()
        },
        "consecutive_bar_clustering": {
            bucket: _cluster_summary(candidates) for bucket, candidates in buckets.items()
        },
        "forward_return_table": forward_table,
        "mfe_mae_table": mfe_mae_table,
        "preliminary_interpretation": _forward_interpretation(
            bucket_counts={bucket: len(candidates) for bucket, candidates in buckets.items()},
            forward_table=forward_table,
            mfe_mae_table=mfe_mae_table,
        ),
    }
    return report


def _diagnostic_candidate(row: Mapping[str, Any], scored: Mapping[str, Any]) -> dict[str, Any]:
    signal_candle = row.get("signal_candle") if isinstance(row.get("signal_candle"), Mapping) else {}
    timestamp = _parse_datetime(row.get("timestamp"))
    return {
        "instrument": row.get("instrument"),
        "timestamp": row.get("timestamp"),
        "timestamp_dt": timestamp,
        "local_date": timestamp.astimezone(NY).date().isoformat(),
        "year_quarter": _year_quarter(timestamp),
        "session_day": f"{timestamp.astimezone(NY).date().isoformat()}|{row.get('session')}",
        "session": row.get("session"),
        "timeframe": row.get("timeframe"),
        "current_exact_rule_flag": row.get("current_exact_rule_flag") is True,
        "acceptance_class": scored.get("acceptance_class"),
        "acceptance_score": _float_or_none(scored.get("acceptance_score")),
        "bar_index": None,
        "open": _float_or_none(signal_candle.get("open")),
        "high": _float_or_none(signal_candle.get("high")),
        "low": _float_or_none(signal_candle.get("low")),
        "close": _float_or_none(signal_candle.get("close")),
    }


def _with_instrument_indices(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(candidate) for candidate in candidates]
    counters: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: (str(item.get("instrument")), item["timestamp_dt"])):
        instrument = str(row.get("instrument"))
        index = counters.get(instrument, 0)
        row["bar_index"] = index
        counters[instrument] = index + 1
    return rows


def _instrument_rows(candidates: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for candidate in candidates:
        grouped.setdefault(str(candidate.get("instrument")), []).append(candidate)
    for instrument, rows in grouped.items():
        grouped[instrument] = sorted(rows, key=lambda item: int(item.get("bar_index") or 0))
    return grouped


def _candidate_bucket(candidate: Mapping[str, Any]) -> str | None:
    acceptance_class = str(candidate.get("acceptance_class") or "")
    score = _float_or_none(candidate.get("acceptance_score")) or 0.0
    if candidate.get("current_exact_rule_flag") is True:
        return "current_exact_rule_flag_true"
    if acceptance_class == "EXACT_STRUCTURAL_MATCH":
        return "exact_structural_match_flag_false"
    if acceptance_class == "NEAR_STRUCTURAL_MATCH" and score >= 0.80:
        return "near_structural_match_score_gte_0_80"
    if acceptance_class == "NEAR_STRUCTURAL_MATCH" and 0.75 <= score < 0.80:
        return "near_structural_match_score_0_75_to_0_80"
    if acceptance_class == "NEAR_STRUCTURAL_MATCH" and 0.70 <= score < 0.75:
        return "near_structural_match_score_0_70_to_0_75"
    if acceptance_class == "DEGRADED_BUT_VALID_MATCH":
        return "degraded_but_valid_match"
    if acceptance_class == "STRUCTURALLY_INVALID":
        return "structurally_invalid_negative_control"
    return None


def _candidate_bucket_order() -> tuple[str, ...]:
    return (
        "current_exact_rule_flag_true",
        "exact_structural_match_flag_false",
        "near_structural_match_score_gte_0_80",
        "near_structural_match_score_0_75_to_0_80",
        "near_structural_match_score_0_70_to_0_75",
        "degraded_but_valid_match",
        "structurally_invalid_negative_control",
    )


def _candidate_density(
    *,
    buckets: Mapping[str, Sequence[Mapping[str, Any]]],
    all_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    calendar_days_by_instrument = {
        instrument: len({str(row.get("local_date")) for row in all_rows if row.get("instrument") == instrument})
        for instrument in sorted({str(row.get("instrument")) for row in all_rows})
    }
    session_days_by_instrument = {
        instrument: len({str(row.get("session_day")) for row in all_rows if row.get("instrument") == instrument})
        for instrument in sorted({str(row.get("instrument")) for row in all_rows})
    }
    by_bucket: dict[str, Any] = {}
    for bucket, candidates in buckets.items():
        by_instrument = Counter(str(candidate.get("instrument") or "UNKNOWN") for candidate in candidates)
        by_class = Counter(str(candidate.get("acceptance_class") or "UNKNOWN") for candidate in candidates)
        by_bucket[bucket] = {
            "count": len(candidates),
            "by_instrument": dict(sorted(by_instrument.items())),
            "by_acceptance_class": dict(sorted(by_class.items())),
            "avg_candidates_per_calendar_day_by_instrument": {
                instrument: _round(
                    by_instrument.get(instrument, 0) / max(1, calendar_days_by_instrument.get(instrument, 0))
                )
                for instrument in sorted(calendar_days_by_instrument)
            },
            "avg_candidates_per_session_day_by_instrument": {
                instrument: _round(
                    by_instrument.get(instrument, 0) / max(1, session_days_by_instrument.get(instrument, 0))
                )
                for instrument in sorted(session_days_by_instrument)
            },
        }
    return {
        "calendar_days_by_instrument": calendar_days_by_instrument,
        "session_days_by_instrument": session_days_by_instrument,
        "by_bucket": by_bucket,
    }


def _dedup_episode_counts(candidates: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {f"{cooldown}_bar_cooldown": _episode_count(candidates, cooldown=cooldown) for cooldown in (1, 3, 6, 12)}


def _episode_count(candidates: Sequence[Mapping[str, Any]], *, cooldown: int) -> int:
    count = 0
    last_by_instrument: dict[str, datetime] = {}
    for candidate in sorted(candidates, key=lambda item: (str(item.get("instrument")), item["timestamp_dt"])):
        instrument = str(candidate.get("instrument"))
        timestamp = candidate["timestamp_dt"]
        previous = last_by_instrument.get(instrument)
        if previous is None or _bar_gap(previous, timestamp) > cooldown:
            count += 1
        last_by_instrument[instrument] = timestamp
    return count


def _cluster_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    clusters: list[int] = []
    current_len = 0
    previous_key: tuple[str, datetime] | None = None
    for candidate in sorted(candidates, key=lambda item: (str(item.get("instrument")), item["timestamp_dt"])):
        key = (str(candidate.get("instrument")), candidate["timestamp_dt"])
        if previous_key and previous_key[0] == key[0] and _bar_gap(previous_key[1], key[1]) == 1:
            current_len += 1
        else:
            if current_len:
                clusters.append(current_len)
            current_len = 1
        previous_key = key
    if current_len:
        clusters.append(current_len)
    return {
        "cluster_count": len(clusters),
        "singleton_clusters": sum(1 for value in clusters if value == 1),
        "avg_cluster_length": _round(sum(clusters) / len(clusters)) if clusters else 0.0,
        "median_cluster_length": _round(median(clusters)) if clusters else 0.0,
        "max_cluster_length": max(clusters) if clusters else 0,
    }


def _forward_return(
    candidate: Mapping[str, Any],
    *,
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    horizon: int,
) -> float | None:
    future = _future_candidate(candidate, instrument_rows=instrument_rows, horizon=horizon)
    entry_close = _float_or_none(candidate.get("close"))
    future_close = _float_or_none(future.get("close")) if future else None
    if entry_close is None or future_close is None:
        return None
    return future_close - entry_close


def _mfe_mae(
    candidate: Mapping[str, Any],
    *,
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    horizon: int,
) -> dict[str, float] | None:
    future_rows = _future_window(candidate, instrument_rows=instrument_rows, horizon=horizon)
    entry_close = _float_or_none(candidate.get("close"))
    if entry_close is None or len(future_rows) < horizon:
        return None
    highs = [_float_or_none(row.get("high")) for row in future_rows]
    lows = [_float_or_none(row.get("low")) for row in future_rows]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    if not valid_highs or not valid_lows:
        return None
    return {"mfe": max(valid_highs) - entry_close, "mae": min(valid_lows) - entry_close}


def _future_candidate(
    candidate: Mapping[str, Any],
    *,
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    horizon: int,
) -> Mapping[str, Any] | None:
    future_rows = _future_window(candidate, instrument_rows=instrument_rows, horizon=horizon)
    return future_rows[-1] if len(future_rows) >= horizon else None


def _future_window(
    candidate: Mapping[str, Any],
    *,
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    horizon: int,
) -> list[Mapping[str, Any]]:
    instrument = str(candidate.get("instrument"))
    start = int(candidate.get("bar_index") or 0) + 1
    return list(instrument_rows.get(instrument, ())[start : start + horizon])


def _return_stats(values: Sequence[float | None]) -> dict[str, Any]:
    resolved = [value for value in values if value is not None]
    positives = [value for value in resolved if value > 0]
    negatives = [value for value in resolved if value < 0]
    mean = sum(resolved) / len(resolved) if resolved else None
    std = _stddev(resolved)
    return {
        "sample_count": len(resolved),
        "average": _round(mean),
        "median": _round(median(resolved)) if resolved else None,
        "win_rate": _round(len(positives) / len(resolved)) if resolved else None,
        "profit_factor_proxy": _profit_factor_proxy(positives, negatives),
        "sharpe_like_mean_over_std": _round(mean / std) if mean is not None and std else None,
    }


def _mfe_mae_stats(values: Sequence[Mapping[str, float] | None]) -> dict[str, Any]:
    resolved = [value for value in values if value is not None]
    mfes = [float(value["mfe"]) for value in resolved]
    maes = [float(value["mae"]) for value in resolved]
    return {
        "sample_count": len(resolved),
        "avg_mfe": _round(sum(mfes) / len(mfes)) if mfes else None,
        "median_mfe": _round(median(mfes)) if mfes else None,
        "avg_mae": _round(sum(maes) / len(maes)) if maes else None,
        "median_mae": _round(median(maes)) if maes else None,
    }


def _profit_factor_proxy(positives: Sequence[float], negatives: Sequence[float]) -> float | None:
    total_positive = sum(positives)
    total_negative = abs(sum(negatives))
    if total_negative == 0:
        return None if total_positive == 0 else math.inf
    return _round(total_positive / total_negative)


def _stddev(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _bar_gap(previous: datetime, current: datetime) -> int:
    return int(round((current - previous).total_seconds() / 300))


def _forward_interpretation(
    *,
    bucket_counts: Mapping[str, int],
    forward_table: Mapping[str, Mapping[str, Mapping[str, Any]]],
    mfe_mae_table: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> list[str]:
    notes = [
        "This is a forward-return diagnostic over completed 5m bars, not a strategy backtest.",
        "No transaction costs, slippage, position sizing, lifecycle, or exit rules were modeled.",
    ]
    exact_flag = forward_table.get("current_exact_rule_flag_true", {}).get("12_bars", {})
    near_high = forward_table.get("near_structural_match_score_gte_0_80", {}).get("12_bars", {})
    invalid = forward_table.get("structurally_invalid_negative_control", {}).get("12_bars", {})
    notes.append(
        "12-bar average forward returns: "
        f"flag=true `{exact_flag.get('average')}`, "
        f"near>=0.80 `{near_high.get('average')}`, "
        f"invalid-control `{invalid.get('average')}`."
    )
    if bucket_counts.get("near_structural_match_score_gte_0_80", 0) > bucket_counts.get("current_exact_rule_flag_true", 0):
        notes.append("Near>=0.80 materially expands sample count relative to the current exact-rule flag.")
    near_mfe = mfe_mae_table.get("near_structural_match_score_gte_0_80", {}).get("12_bars", {})
    if near_mfe.get("avg_mfe") is not None and near_mfe.get("avg_mae") is not None:
        notes.append(
            "Near>=0.80 has 12-bar avg MFE/MAE of "
            f"`{near_mfe.get('avg_mfe')}` / `{near_mfe.get('avg_mae')}` points."
        )
    return notes


def _forward_markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Entry Acceptance Forward-Return Diagnostic",
        "",
        f"- pilot_family: `{report['pilot_family']}`",
        f"- mode: `{report['mode']}`",
        f"- source_period: `{report['source_period']}`",
        f"- total_archive_rows: `{report['total_archive_rows']}`",
        f"- candidate_bucket_counts: `{report['candidate_bucket_counts']}`",
        f"- transaction_costs_modeled: `{report['transaction_costs_modeled']}`",
        "",
        "## Density",
        "",
        f"`{report['candidate_density']}`",
        "",
        "## Deduped Episodes",
        "",
        f"`{report['deduped_episode_counts']}`",
        "",
        "## Forward Returns",
        "",
        f"`{report['forward_return_table']}`",
        "",
        "## MFE/MAE",
        "",
        f"`{report['mfe_mae_table']}`",
        "",
        "## Preliminary Interpretation",
        "",
        *[f"- {note}" for note in report["preliminary_interpretation"]],
        "",
    ]
    return "\n".join(lines)


def _diagnostic_backtest(
    *,
    rows: Sequence[Mapping[str, Any]],
    scored_rows: Sequence[Mapping[str, Any]],
    archive_path: Path,
    backtest_json: Path | None,
    backtest_md: Path | None,
) -> dict[str, Any]:
    paired = _with_instrument_indices(
        [_diagnostic_candidate(row, scored) for row, scored in zip(rows, scored_rows, strict=False)]
    )
    instrument_rows = _instrument_rows(paired)
    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in _candidate_bucket_order()}
    for candidate in paired:
        bucket = _candidate_bucket(candidate)
        if bucket:
            buckets[bucket].append(candidate)
    episodes = {bucket: _dedup_candidates(candidates, cooldown=12) for bucket, candidates in buckets.items()}
    results: dict[str, Any] = {}
    results_by_instrument: dict[str, Any] = {}
    for bucket, candidates in episodes.items():
        bucket_results: dict[str, Any] = {}
        bucket_by_instrument: dict[str, Any] = {}
        for entry_mode in ("next_bar_open", "next_bar_close"):
            bucket_results[entry_mode] = {}
            bucket_by_instrument[entry_mode] = {}
            for horizon in (6, 12, 24):
                trades = [
                    trade
                    for trade in (
                        _diagnostic_trade(
                            candidate,
                            instrument_rows=instrument_rows,
                            entry_mode=entry_mode,
                            horizon=horizon,
                        )
                        for candidate in candidates
                    )
                    if trade is not None
                ]
                bucket_results[entry_mode][f"{horizon}_bars"] = {
                    f"{cost}_point_cost": _trade_stats(trades, round_trip_cost=cost)
                    for cost in (0.0, 0.2, 0.5)
                }
                bucket_by_instrument[entry_mode][f"{horizon}_bars"] = {
                    instrument: {
                        f"{cost}_point_cost": _trade_stats(
                            [trade for trade in trades if trade.get("instrument") == instrument],
                            round_trip_cost=cost,
                        )
                        for cost in (0.0, 0.2, 0.5)
                    }
                    for instrument in sorted({str(trade.get("instrument")) for trade in trades})
                }
        results[bucket] = bucket_results
        results_by_instrument[bucket] = bucket_by_instrument

    headline = _backtest_headline(results)
    report = {
        "schema_version": "track_b_entry_acceptance_diagnostic_backtest_v1",
        "pilot_family": PILOT_FAMILY,
        "mode": "RESEARCH_DIAGNOSTIC_BACKTEST_ONLY",
        "archive_path": str(archive_path),
        "backtest_json": str(backtest_json) if backtest_json else None,
        "backtest_markdown": str(backtest_md) if backtest_md else None,
        "authoritative_runtime_truth": False,
        "strategy_behavior_changed": False,
        "live_or_runtime_artifacts_used": False,
        "broker_state_mutated": False,
        "lane_execution_performed": False,
        "transaction_costs_modeled": True,
        "cost_sensitivity_points_round_trip": [0.0, 0.2, 0.5],
        "entry_modes": ["next_bar_open", "next_bar_close"],
        "exit_horizons_bars": [6, 12, 24],
        "dedupe_cooldown_bars": 12,
        "source_period": _source_period(rows),
        "candidate_counts": {bucket: len(candidates) for bucket, candidates in buckets.items()},
        "episode_counts": {bucket: len(candidates) for bucket, candidates in episodes.items()},
        "results": results,
        "results_by_instrument": results_by_instrument,
        "results_by_year_quarter": _backtest_results_by_year_quarter(episodes, instrument_rows),
        "headline_comparison": headline,
        "preliminary_interpretation": _backtest_interpretation(headline, results),
    }
    return report


def _dedup_candidates(candidates: Sequence[Mapping[str, Any]], *, cooldown: int) -> list[Mapping[str, Any]]:
    selected: list[Mapping[str, Any]] = []
    last_by_instrument: dict[str, datetime] = {}
    for candidate in sorted(candidates, key=lambda item: (str(item.get("instrument")), item["timestamp_dt"])):
        instrument = str(candidate.get("instrument"))
        timestamp = candidate["timestamp_dt"]
        previous = last_by_instrument.get(instrument)
        if previous is None or _bar_gap(previous, timestamp) > cooldown:
            selected.append(candidate)
            last_by_instrument[instrument] = timestamp
    return selected


def _diagnostic_trade(
    candidate: Mapping[str, Any],
    *,
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    entry_mode: str,
    horizon: int,
) -> dict[str, Any] | None:
    instrument = str(candidate.get("instrument"))
    rows = instrument_rows.get(instrument, ())
    entry_index = int(candidate.get("bar_index") or 0) + 1
    exit_index = entry_index + horizon - 1
    if entry_index >= len(rows) or exit_index >= len(rows):
        return None
    entry_bar = rows[entry_index]
    exit_bar = rows[exit_index]
    entry_price_key = "open" if entry_mode == "next_bar_open" else "close"
    entry_price = _float_or_none(entry_bar.get(entry_price_key))
    exit_price = _float_or_none(exit_bar.get("close"))
    if entry_price is None or exit_price is None:
        return None
    window = rows[entry_index : exit_index + 1]
    highs = [_float_or_none(row.get("high")) for row in window]
    lows = [_float_or_none(row.get("low")) for row in window]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    if not valid_highs or not valid_lows:
        return None
    return {
        "instrument": instrument,
        "candidate_timestamp": candidate.get("timestamp"),
        "entry_timestamp": entry_bar.get("timestamp"),
        "exit_timestamp": exit_bar.get("timestamp"),
        "entry_mode": entry_mode,
        "horizon_bars": horizon,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return": exit_price - entry_price,
        "mfe": max(valid_highs) - entry_price,
        "mae": min(valid_lows) - entry_price,
    }


def _trade_stats(trades: Sequence[Mapping[str, Any]], *, round_trip_cost: float) -> dict[str, Any]:
    net_returns = [float(trade["gross_return"]) - round_trip_cost for trade in trades]
    gross_returns = [float(trade["gross_return"]) for trade in trades]
    positives = [value for value in net_returns if value > 0]
    negatives = [value for value in net_returns if value < 0]
    mean = sum(net_returns) / len(net_returns) if net_returns else None
    std = _stddev(net_returns)
    mfes = [float(trade["mfe"]) for trade in trades]
    maes = [float(trade["mae"]) for trade in trades]
    return {
        "trade_count": len(trades),
        "round_trip_cost_points": round_trip_cost,
        "average_return": _round(mean),
        "median_return": _round(median(net_returns)) if net_returns else None,
        "average_gross_return": _round(sum(gross_returns) / len(gross_returns)) if gross_returns else None,
        "win_rate": _round(len(positives) / len(net_returns)) if net_returns else None,
        "profit_factor_proxy": _profit_factor_proxy(positives, negatives),
        "sharpe_like_mean_over_std": _round(mean / std) if mean is not None and std else None,
        "max_drawdown_proxy": _round(_max_drawdown(net_returns)),
        "avg_mfe": _round(sum(mfes) / len(mfes)) if mfes else None,
        "median_mfe": _round(median(mfes)) if mfes else None,
        "avg_mae": _round(sum(maes) / len(maes)) if maes else None,
        "median_mae": _round(median(maes)) if maes else None,
    }


def _max_drawdown(returns: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _backtest_headline(results: Mapping[str, Any]) -> dict[str, Any]:
    baseline = (
        results.get("current_exact_rule_flag_true", {})
        .get("next_bar_open", {})
        .get("12_bars", {})
        .get("0.5_point_cost", {})
    )
    near_high = (
        results.get("near_structural_match_score_gte_0_80", {})
        .get("next_bar_open", {})
        .get("12_bars", {})
        .get("0.5_point_cost", {})
    )
    lower_near = (
        results.get("near_structural_match_score_0_70_to_0_75", {})
        .get("next_bar_open", {})
        .get("12_bars", {})
        .get("0.5_point_cost", {})
    )
    degraded = (
        results.get("degraded_but_valid_match", {})
        .get("next_bar_open", {})
        .get("12_bars", {})
        .get("0.5_point_cost", {})
    )
    return {
        "comparison_basis": "next_bar_open_entry_12_bar_exit_0.5_point_round_trip_cost",
        "baseline_exact_flag_true": baseline,
        "near_score_gte_0_80": near_high,
        "near_gte_0_80_survives_costs_better_than_baseline": _avg_return(near_high) is not None
        and _avg_return(baseline) is not None
        and _avg_return(near_high) > _avg_return(baseline),
        "lower_near_0_70_to_0_75": lower_near,
        "degraded": degraded,
    }


def _backtest_results_by_year_quarter(
    episodes: Mapping[str, Sequence[Mapping[str, Any]]],
    instrument_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for bucket, candidates in episodes.items():
        by_period: dict[str, list[Mapping[str, Any]]] = {}
        for candidate in candidates:
            by_period.setdefault(str(candidate.get("year_quarter") or "UNKNOWN"), []).append(candidate)
        output[bucket] = {}
        for period, period_candidates in sorted(by_period.items()):
            trades = [
                trade
                for trade in (
                    _diagnostic_trade(
                        candidate,
                        instrument_rows=instrument_rows,
                        entry_mode="next_bar_open",
                        horizon=12,
                    )
                    for candidate in period_candidates
                )
                if trade is not None
            ]
            output[bucket][period] = {
                "episode_count": len(period_candidates),
                "next_bar_open_12_bars_0.5_point_cost": _trade_stats(trades, round_trip_cost=0.5),
            }
    return output


def _avg_return(stats: Mapping[str, Any]) -> float | None:
    return _float_or_none(stats.get("average_return")) if isinstance(stats, Mapping) else None


def _backtest_interpretation(headline: Mapping[str, Any], results: Mapping[str, Any]) -> list[str]:
    baseline = headline.get("baseline_exact_flag_true") if isinstance(headline.get("baseline_exact_flag_true"), Mapping) else {}
    near_high = headline.get("near_score_gte_0_80") if isinstance(headline.get("near_score_gte_0_80"), Mapping) else {}
    lower_near = headline.get("lower_near_0_70_to_0_75") if isinstance(headline.get("lower_near_0_70_to_0_75"), Mapping) else {}
    degraded = headline.get("degraded") if isinstance(headline.get("degraded"), Mapping) else {}
    notes = [
        "This is a transparent offline diagnostic backtest, not a final strategy backtest.",
        "It uses long-only next-bar entries, fixed horizon exits, 12-bar candidate de-duping, and simple point-cost sensitivity.",
        "No stops, slippage model, sizing, lifecycle rules, governance, or broker behavior were modeled.",
    ]
    notes.append(
        "At next-bar-open / 12-bar exit / 0.5-point cost, baseline avg return is "
        f"`{baseline.get('average_return')}` over `{baseline.get('trade_count')}` episodes; "
        f"near>=0.80 avg return is `{near_high.get('average_return')}` over `{near_high.get('trade_count')}` episodes."
    )
    if headline.get("near_gte_0_80_survives_costs_better_than_baseline") is True:
        notes.append("Near>=0.80 survives the simple cost sensitivity better than the current exact-rule baseline on this diagnostic.")
    else:
        notes.append("Near>=0.80 does not beat the current exact-rule baseline after simple costs on the headline comparison.")
    if (_avg_return(lower_near) or 0.0) <= 0.0:
        notes.append("Near 0.70-0.75 should be discarded or held out for now based on the headline costed result.")
    if (_avg_return(degraded) or 0.0) <= 0.0:
        notes.append("Degraded candidates should be discarded or held out for now based on the headline costed result.")
    return notes


def _backtest_markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Entry Acceptance Diagnostic Backtest v1",
        "",
        f"- pilot_family: `{report['pilot_family']}`",
        f"- mode: `{report['mode']}`",
        f"- source_period: `{report['source_period']}`",
        f"- dedupe_cooldown_bars: `{report['dedupe_cooldown_bars']}`",
        f"- cost_sensitivity_points_round_trip: `{report['cost_sensitivity_points_round_trip']}`",
        f"- episode_counts: `{report['episode_counts']}`",
        "",
        "## Headline",
        "",
        f"`{report['headline_comparison']}`",
        "",
        "## Results",
        "",
        f"`{report['results']}`",
        "",
        "## By Instrument",
        "",
        f"`{report['results_by_instrument']}`",
        "",
        "## By Year/Quarter",
        "",
        f"`{report['results_by_year_quarter']}`",
        "",
        "## Preliminary Interpretation",
        "",
        *[f"- {note}" for note in report["preliminary_interpretation"]],
        "",
    ]
    return "\n".join(lines)


def _breakout_retest_hold_context(
    *,
    prior_bar: Mapping[str, Any] | None,
    breakout_bar: Mapping[str, Any] | None,
    signal_bar: Mapping[str, Any],
    breakout_feature_row: Mapping[str, Any],
    signal_feature_row: Mapping[str, Any],
) -> dict[str, Decimal | bool | int | None]:
    if prior_bar is None or breakout_bar is None:
        return {
            "breakout_normalized_slope": None,
            "breakout_range_expansion_ratio": None,
            "breakout_level": None,
            "retest_depth_ticks_or_points": None,
            "retest_depth_normalized": None,
            "hold_margin_ticks_or_points": None,
            "hold_margin_normalized": None,
            "bars_since_breakout": None,
            "bars_since_retest": None,
            "range_expansion_ratio": None,
            "close_location": _close_location(signal_bar),
            "body_to_range_ratio": _body_to_range(signal_bar),
            "breakout_bar_slope_is_flat": False,
            "breakout_bar_expansion_is_normal": False,
            "breakout_breaks_prior_1_high": False,
            "signal_retests_and_holds_breakout_level": False,
        }

    prior_high = _decimal(prior_bar.get("high"))
    prior_close = _decimal(prior_bar.get("close"))
    breakout_high = _decimal(breakout_bar.get("high"))
    breakout_close = _decimal(breakout_bar.get("close"))
    signal_low = _decimal(signal_bar.get("low"))
    signal_close = _decimal(signal_bar.get("close"))
    breakout_level = breakout_high
    signal_atr = _positive_decimal(signal_feature_row.get("atr"))
    breakout_atr = _positive_decimal(breakout_feature_row.get("atr"))
    breakout_range = _decimal(breakout_bar.get("high")) - _decimal(breakout_bar.get("low"))
    breakout_velocity = _decimal_or_none(breakout_feature_row.get("velocity")) or Decimal("0")
    breakout_slope = _normalized(breakout_velocity, breakout_atr)
    range_expansion = _normalized(breakout_range, breakout_atr)
    signal_retests = signal_low <= breakout_level
    signal_holds = signal_close >= breakout_level
    retest_depth = max(Decimal("0"), breakout_level - signal_low)
    hold_margin = signal_close - breakout_level
    return {
        "breakout_normalized_slope": breakout_slope,
        "breakout_range_expansion_ratio": range_expansion,
        "breakout_level": breakout_level,
        "retest_depth_ticks_or_points": retest_depth,
        "retest_depth_normalized": _normalized(retest_depth, signal_atr),
        "hold_margin_ticks_or_points": hold_margin,
        "hold_margin_normalized": _normalized(hold_margin, signal_atr),
        "bars_since_breakout": 1,
        "bars_since_retest": 0 if signal_retests else None,
        "range_expansion_ratio": range_expansion,
        "close_location": _close_location(signal_bar),
        "body_to_range_ratio": _body_to_range(signal_bar),
        "breakout_bar_slope_is_flat": abs(breakout_slope) <= BREAKOUT_ABS_SLOPE_MAX,
        "breakout_bar_expansion_is_normal": (
            range_expansion > BREAKOUT_MIN_RANGE_EXPANSION_RATIO
            and range_expansion < BREAKOUT_MAX_RANGE_EXPANSION_RATIO
        ),
        "breakout_breaks_prior_1_high": breakout_high > prior_high and breakout_close >= prior_close,
        "signal_retests_and_holds_breakout_level": signal_retests and signal_holds,
    }


def _features_for_prior_bar(
    bars: Sequence[Mapping[str, Any]],
    index: int,
    features_by_ts: Mapping[str | None, Mapping[str, Any]],
    current_feature_row: Mapping[str, Any],
) -> Mapping[str, Any]:
    if index < 1:
        return {}
    breakout_ts = _iso(bars[index - 1].get("bar_ts"))
    return features_by_ts.get(breakout_ts, current_feature_row)


def _summarize_archive(
    rows: Sequence[Mapping[str, Any]],
    *,
    warehouse_root: Path,
    output_root: Path,
    source_summaries: Sequence[Mapping[str, Any]],
    archive_jsonl: Path,
    summary_json: Path,
    summary_md: Path,
    scoring_report: Mapping[str, Any],
) -> dict[str, Any]:
    counts_by_instrument = Counter(str(row.get("instrument") or "UNKNOWN") for row in rows)
    candidate_flags = Counter(str(row.get("current_exact_rule_flag")) for row in rows)
    sessions = Counter(str(row.get("session") or "UNKNOWN") for row in rows)
    completeness = _field_completeness(rows)
    return {
        "schema_version": "track_b_entry_acceptance_enriched_candidate_archive_summary_v1",
        "pilot_family": PILOT_FAMILY,
        "mode": "RESEARCH_WAREHOUSE_OFFLINE_ONLY",
        "warehouse_root": str(warehouse_root),
        "output_root": str(output_root),
        "archive_jsonl": str(archive_jsonl),
        "summary_json": str(summary_json),
        "summary_markdown": str(summary_md),
        "acceptance_scoring_report_json": scoring_report.get("scoring_json"),
        "acceptance_scoring_report_markdown": scoring_report.get("scoring_markdown"),
        "forward_return_diagnostic_json": scoring_report.get("forward_return_diagnostic_json"),
        "forward_return_diagnostic_markdown": scoring_report.get("forward_return_diagnostic_markdown"),
        "diagnostic_backtest_json": scoring_report.get("diagnostic_backtest_json"),
        "diagnostic_backtest_markdown": scoring_report.get("diagnostic_backtest_markdown"),
        "authoritative_runtime_truth": False,
        "strategy_behavior_changed": False,
        "trade_simulation_performed": False,
        "pnl_calculated": False,
        "source_period": _source_period(rows),
        "total_rows": len(rows),
        "row_counts_by_instrument": dict(sorted(counts_by_instrument.items())),
        "exact_candidate_flag_counts": dict(sorted(candidate_flags.items())),
        "session_counts": dict(sorted(sessions.items())),
        "source_summaries": list(source_summaries),
        "enriched_field_completeness": completeness,
        "entry_acceptance_scoring_ready": _entry_acceptance_scoring_ready(rows, completeness),
        "entry_acceptance_scoring_v2": {
            "total_rows_scanned": scoring_report.get("total_rows_scanned"),
            "acceptance_class_counts": scoring_report.get("acceptance_class_counts"),
            "exact_candidate_flag_acceptance_class_cross_tab": scoring_report.get(
                "exact_candidate_flag_acceptance_class_cross_tab"
            ),
            "near_degraded_not_exact_rule_flag_count": scoring_report.get(
                "near_degraded_not_exact_rule_flag_count"
            ),
            "classifications_look_sane": scoring_report.get("classifications_look_sane"),
        },
        "scoring_readiness_notes": _scoring_readiness_notes(rows, completeness),
        "safety_flags": {
            "strategy_authority": False,
            "broker_state_mutated": False,
            "submit_attempted": False,
            "order_intent_created": False,
            "lifecycle_mutated": False,
            "runtime_trade_eligible": False,
        },
    }


def _source_summary(inputs: WarehouseInputs, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "instrument": inputs.instrument,
        "bars_path": str(inputs.bars_path),
        "features_path": str(inputs.features_path),
        "lane_candidates_path": str(inputs.lane_candidates_path) if inputs.lane_candidates_path else None,
        "row_count": len(rows),
        "source_period": _source_period(rows),
        "exact_candidate_flag_count": sum(1 for row in rows if row.get("current_exact_rule_flag") is True),
    }


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Entry Acceptance Enriched Candidate Archive",
        "",
        f"- pilot_family: `{report['pilot_family']}`",
        f"- mode: `{report['mode']}`",
        f"- source_period: `{report['source_period']}`",
        f"- total_rows: `{report['total_rows']}`",
        f"- row_counts_by_instrument: `{report['row_counts_by_instrument']}`",
        f"- exact_candidate_flag_counts: `{report['exact_candidate_flag_counts']}`",
        f"- entry_acceptance_scoring_ready: `{report['entry_acceptance_scoring_ready']}`",
        f"- entry_acceptance_scoring_v2: `{report['entry_acceptance_scoring_v2']}`",
        "",
        "## Field Completeness",
        "",
        f"`{report['enriched_field_completeness']}`",
        "",
        "## Notes",
        "",
        *[f"- {note}" for note in report["scoring_readiness_notes"]],
        "",
    ]
    return "\n".join(lines)


def _field_completeness(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int | float]]:
    fields = (
        "breakout_level",
        "retest_depth",
        "hold_margin",
        "range_expansion_ratio",
        "close_location",
        "body_to_range_ratio",
        "churn_score",
        "snap_turn_conflict_strength",
    )
    result: dict[str, dict[str, int | float]] = {}
    denominator = len(rows) or 1
    for field in fields:
        present = sum(1 for row in rows if row.get(field) is not None)
        result[field] = {"present": present, "missing": len(rows) - present, "present_ratio": round(present / denominator, 6)}
    return result


def _enriched_field_completeness(enriched: Mapping[str, Any]) -> float:
    fields = (
        "breakout_level",
        "retest_depth_ticks_or_points",
        "hold_margin_ticks_or_points",
        "range_expansion_ratio",
        "close_location",
        "body_to_range_ratio",
    )
    present = sum(1 for field in fields if enriched.get(field) is not None)
    return round(present / len(fields), 6)


def _entry_acceptance_scoring_ready(rows: Sequence[Mapping[str, Any]], completeness: Mapping[str, Mapping[str, Any]]) -> bool:
    if not rows:
        return False
    exact_true = sum(1 for row in rows if row.get("current_exact_rule_flag") is True)
    required_fields = ("breakout_level", "retest_depth", "hold_margin", "range_expansion_ratio")
    return exact_true > 0 and all(float(completeness[field]["present_ratio"]) > 0.99 for field in required_fields)


def _scoring_readiness_notes(rows: Sequence[Mapping[str, Any]], completeness: Mapping[str, Mapping[str, Any]]) -> list[str]:
    if not rows:
        return ["No warehouse rows were available to archive."]
    notes = [
        "Archive rows are generated from completed warehouse 5m bars plus shared feature exact-rule flags.",
        "This is suitable for Entry Acceptance distribution scoring; it is not a PnL backtest.",
    ]
    if any(float(value["present_ratio"]) < 1.0 for value in completeness.values()):
        notes.append("First bars in each shard may lack breakout history; downstream scoring should treat them as low-confidence or invalid context.")
    return notes


def _source_period(rows: Sequence[Mapping[str, Any]]) -> dict[str, str | None]:
    timestamps = sorted(str(row.get("timestamp")) for row in rows if row.get("timestamp"))
    return {"start": timestamps[0] if timestamps else None, "end": timestamps[-1] if timestamps else None}


def _discover_warehouse_inputs(
    *,
    warehouse_root: Path,
    instruments: Sequence[str],
    year: str | None,
    shard_id: str | None,
) -> list[WarehouseInputs]:
    discovered: list[WarehouseInputs] = []
    for instrument in instruments:
        pattern = f"datasets/derived_bars_5m/symbol={instrument}/year=*/shard_id=*/bars.parquet"
        for bars_path in sorted(warehouse_root.glob(pattern)):
            parts = bars_path.parts
            discovered_year = _partition_value(parts, "year")
            discovered_shard = _partition_value(parts, "shard_id")
            if year is not None and discovered_year != year:
                continue
            if shard_id is not None and discovered_shard != shard_id:
                continue
            features_path = (
                warehouse_root
                / "datasets"
                / "shared_features_5m"
                / f"symbol={instrument}"
                / f"year={discovered_year}"
                / f"shard_id={discovered_shard}"
                / "features.parquet"
            )
            if not features_path.exists():
                continue
            lane_candidates_path = (
                warehouse_root
                / "datasets"
                / "lane_candidates"
                / f"symbol={instrument}"
                / f"year={discovered_year}"
                / f"shard_id={discovered_shard}"
                / "candidates.parquet"
            )
            discovered.append(
                WarehouseInputs(
                    instrument=instrument,
                    bars_path=bars_path,
                    features_path=features_path,
                    lane_candidates_path=lane_candidates_path if lane_candidates_path.exists() else None,
                )
            )
    if not discovered:
        raise FileNotFoundError("no compatible warehouse 5m bar/shared-feature partitions were found.")
    return discovered


def _partition_value(parts: Sequence[str], key: str) -> str:
    prefix = f"{key}="
    for part in parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    raise ValueError(f"partition key missing from path: {key}")


def _warehouse_inputs(*, warehouse_root: Path, instrument: str, year: str, shard_id: str) -> WarehouseInputs:
    base = warehouse_root / "datasets"
    bars_path = base / "derived_bars_5m" / f"symbol={instrument}" / f"year={year}" / f"shard_id={shard_id}" / "bars.parquet"
    features_path = (
        base / "shared_features_5m" / f"symbol={instrument}" / f"year={year}" / f"shard_id={shard_id}" / "features.parquet"
    )
    lane_candidates_path = (
        base / "lane_candidates" / f"symbol={instrument}" / f"year={year}" / f"shard_id={shard_id}" / "candidates.parquet"
    )
    for path in (bars_path, features_path):
        if not path.exists():
            raise FileNotFoundError(f"required warehouse source missing: {path}")
    return WarehouseInputs(
        instrument=instrument,
        bars_path=bars_path,
        features_path=features_path,
        lane_candidates_path=lane_candidates_path if lane_candidates_path.exists() else None,
    )


def _read_parquet_records(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    try:
        return read_parquet_rows(path)
    except RuntimeError:
        import pandas as pd  # type: ignore[import-not-found]

        frame = pd.read_parquet(path)
        return [dict(row) for row in frame.to_dict(orient="records")]


def _reject_non_research_warehouse_root(path: Path) -> None:
    text = str(path)
    blocked_tokens = ("/runtime/", "operator_dashboard", "paper_leak_test", "paper_session", "broker_truth")
    if any(token in text for token in blocked_tokens):
        raise ValueError("refusing non-research/live/runtime-like source path.")
    if "warehouse_historical_evaluator" not in text:
        raise ValueError("source must be a warehouse_historical_evaluator research artifact.")


def _research_session_phase(timestamp: datetime) -> str:
    local_time = timestamp.astimezone(NY).time()
    if time(18, 0) <= local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time < time(23, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_OPEN"
    if time(5, 30) <= local_time < time(8, 30):
        return "LONDON_LATE"
    if time(9, 0) <= local_time < time(9, 30):
        return "US_PREOPEN_OPENING"
    if time(9, 30) <= local_time < time(10, 0):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 0) <= local_time < time(10, 30):
        return "US_OPEN_LATE"
    if time(11, 0) <= local_time < time(13, 30):
        return "US_MIDDAY"
    if time(13, 30) <= local_time < time(16, 0):
        return "US_LATE"
    return "OUT_OF_SCOPE"


def _asia_early_or_gc_mgc_london_open(timestamp: datetime) -> bool:
    phase = _research_session_phase(timestamp)
    if phase == "ASIA_EARLY":
        return True
    local_time = timestamp.astimezone(NY).time()
    return phase == "LONDON_OPEN" and local_time in {time(3, 5), time(3, 10), time(3, 15)}


def _close_location(bar: Mapping[str, Any]) -> Decimal | None:
    high = _decimal(bar.get("high"))
    low = _decimal(bar.get("low"))
    close = _decimal(bar.get("close"))
    width = high - low
    return None if width <= 0 else (close - low) / width


def _body_to_range(bar: Mapping[str, Any]) -> Decimal | None:
    high = _decimal(bar.get("high"))
    low = _decimal(bar.get("low"))
    open_price = _decimal(bar.get("open"))
    close = _decimal(bar.get("close"))
    width = high - low
    return None if width <= 0 else abs(close - open_price) / width


def _candle_payload(bar: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": _iso(bar.get("bar_ts")),
        "open": _json_number(_decimal_or_none(bar.get("open"))),
        "high": _json_number(_decimal_or_none(bar.get("high"))),
        "low": _json_number(_decimal_or_none(bar.get("low"))),
        "close": _json_number(_decimal_or_none(bar.get("close"))),
        "volume": _json_number(_decimal_or_none(bar.get("volume"))),
        "timeframe": str(bar.get("timeframe") or TIMEFRAME),
        "completed": True,
    }


def _churn_score(prior_bars_since_setup: int) -> Decimal:
    if prior_bars_since_setup > ANTI_CHURN_BARS:
        return Decimal("0")
    return Decimal(ANTI_CHURN_BARS + 1 - prior_bars_since_setup) / Decimal(ANTI_CHURN_BARS + 1)


def _normalized(value: Decimal, denominator: Decimal) -> Decimal:
    return Decimal("0") if denominator <= 0 else value / denominator


def _positive_decimal(value: Any) -> Decimal:
    resolved = _decimal_or_none(value)
    return resolved if resolved is not None and resolved > 0 else Decimal("0.01")


def _decimal(value: Any) -> Decimal:
    resolved = _decimal_or_none(value)
    return resolved if resolved is not None else Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        text = str(value)
        if text in {"NaT", "nan", "NaN"}:
            return None
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _json_number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        return value
    resolved = _decimal_or_none(value)
    return None if resolved is None else float(resolved)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(resolved):
        return None
    return resolved


def _round(value: float | int | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isinf(value):
        return value
    return round(float(value), digits)


def _bar_ts(row: Mapping[str, Any]) -> str:
    return _iso(row.get("bar_ts"))


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value)
    if text in {"NaT", "nan", "NaN"}:
        return None
    return text


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=NY)
    return parsed


def _year_quarter(timestamp: datetime) -> str:
    local = timestamp.astimezone(NY)
    quarter = ((local.month - 1) // 3) + 1
    return f"{local.year}Q{quarter}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--instrument", action="append", help="Instrument symbol to include; defaults to MGC and GC.")
    parser.add_argument("--year", default=None, help="Optional year partition filter; defaults to all available.")
    parser.add_argument("--shard-id", default=None, help="Optional shard_id partition filter; defaults to all available.")
    args = parser.parse_args(argv)
    instruments = tuple(args.instrument) if args.instrument else ("MGC", "GC")
    report = build_report(
        warehouse_root=args.warehouse_root,
        output_root=args.output_root,
        instruments=instruments,
        year=args.year,
        shard_id=args.shard_id,
    )
    print(
        json.dumps(
            {
                "archive_jsonl": report["archive_jsonl"],
                "acceptance_scoring_report_json": report["acceptance_scoring_report_json"],
                "forward_return_diagnostic_json": report["forward_return_diagnostic_json"],
                "diagnostic_backtest_json": report["diagnostic_backtest_json"],
                "summary_json": report["summary_json"],
                "total_rows": report["total_rows"],
                "entry_acceptance_scoring_ready": report["entry_acceptance_scoring_ready"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
