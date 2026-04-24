"""Research-only mapping from trade artifacts into cross-asset confirmation context."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from ..trend_participation.storage import build_layout, write_storage_manifest
from .cross_asset_confirmation import TIER_A, TIER_B, TIER_C, TIER_DIVERGENT, TIER_NONE, TIER_NO_PARTNER
from .session_scope import DEFAULT_SESSION_CONFIG, _session_date


NEW_YORK = ZoneInfo("America/New_York")
OUTCOME_BUCKET_CONFIRMED = "CONFIRMED_TIER"
OUTCOME_BUCKET_NO_CONFIRMATION = "NO_CONFIRMATION"
OUTCOME_BUCKET_CONTAMINATED = "CONTAMINATED"
OUTCOME_BUCKET_INSUFFICIENT = "INSUFFICIENT_SIGNAL"
OUTCOME_BUCKET_OUT_OF_SCOPE = "OUT_OF_SCOPE"
OUTCOME_BUCKET_UNMAPPED = "UNMAPPED_SESSION"


def run_cross_asset_trade_mapping(
    *,
    output_dir: Path,
    cross_asset_output_dir: Path,
    multi_year_output_dir: Path,
    warehouse_root: Path | None = None,
    runtime_bridge_dir: Path | None = None,
    operator_dashboard_dir: Path | None = None,
    recent_session_count: int = 20,
) -> dict[str, Any]:
    signal_context = _load_signal_context(
        cross_asset_output_dir=cross_asset_output_dir,
        multi_year_output_dir=multi_year_output_dir,
    )
    inventory_rows, mappable_trade_rows, insufficient_rows = _inventory_trade_sources(
        warehouse_root=warehouse_root,
        runtime_bridge_dir=runtime_bridge_dir,
        operator_dashboard_dir=operator_dashboard_dir,
    )
    mapped_rows = _map_trades(
        trade_rows=mappable_trade_rows,
        confirmation_by_session=signal_context["confirmation_by_session"],
        availability_by_session=signal_context["availability_by_session"],
        early_features_by_session=signal_context["early_features_by_session"],
    )
    tier_summary = _tier_outcome_summary(mapped_rows)
    counterfactual = _counterfactual_summary(mapped_rows)
    recent_signal = _recent_signal_summary(
        confirmation_rows=signal_context["confirmation_rows"],
        recent_session_count=recent_session_count,
    )
    insufficient_report = _insufficient_data_report(
        insufficient_rows=insufficient_rows,
        mapped_rows=mapped_rows,
        inventory_rows=inventory_rows,
        recent_signal=recent_signal,
    )
    recommendation = _recommendation(
        mapped_rows=mapped_rows,
        counterfactual=counterfactual,
        recent_signal=recent_signal,
        insufficient_report=insufficient_report,
    )
    payload = {
        "module": "Asia Drift Cross-Asset Trade Mapping",
        "objective": (
            "Research-only mapping of historical paper/forced/live-candidate trade artifacts into the CROSS_ASSET_CONFIRMATION "
            "framework. This pass inventories available trade evidence, attaches no-future-leakage session confirmation tiers "
            "where possible, compares outcomes by tier, estimates a Tier A/B filter counterfactual, and checks whether the signal "
            "is still present in the most recent available structural sessions."
        ),
        "cross_asset_output_dir": str(cross_asset_output_dir.resolve()),
        "multi_year_output_dir": str(multi_year_output_dir.resolve()),
        "trade_artifact_inventory": inventory_rows,
        "mapped_trade_rows": mapped_rows,
        "tier_outcome_summary": tier_summary,
        "counterfactual_filter_summary": counterfactual,
        "recent_signal_summary": recent_signal,
        "insufficient_data_report": insufficient_report,
        "recommendation": recommendation,
    }
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_cross_asset_trade_mapping_from_rows(
    *,
    output_dir: Path,
    confirmation_rows: Sequence[dict[str, Any]],
    availability_by_session: dict[str, dict[str, Any]],
    early_features_by_session: dict[str, dict[str, Any]],
    trade_inventory_rows: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, Any]],
    insufficient_rows: Sequence[dict[str, Any]] | None = None,
    recent_session_count: int = 20,
) -> dict[str, Any]:
    mapped_rows = _map_trades(
        trade_rows=trade_rows,
        confirmation_by_session={str(row["primary_session_id"]): dict(row) for row in confirmation_rows},
        availability_by_session=availability_by_session,
        early_features_by_session=early_features_by_session,
    )
    tier_summary = _tier_outcome_summary(mapped_rows)
    counterfactual = _counterfactual_summary(mapped_rows)
    recent_signal = _recent_signal_summary(
        confirmation_rows=confirmation_rows,
        recent_session_count=recent_session_count,
    )
    insufficient_report = _insufficient_data_report(
        insufficient_rows=list(insufficient_rows or []),
        mapped_rows=mapped_rows,
        inventory_rows=list(trade_inventory_rows),
        recent_signal=recent_signal,
    )
    payload = {
        "module": "Asia Drift Cross-Asset Trade Mapping",
        "objective": "Research-only synthetic/unit-test mapping payload.",
        "cross_asset_output_dir": "synthetic",
        "multi_year_output_dir": "synthetic",
        "trade_artifact_inventory": list(trade_inventory_rows),
        "mapped_trade_rows": mapped_rows,
        "tier_outcome_summary": tier_summary,
        "counterfactual_filter_summary": counterfactual,
        "recent_signal_summary": recent_signal,
        "insufficient_data_report": insufficient_report,
        "recommendation": _recommendation(
            mapped_rows=mapped_rows,
            counterfactual=counterfactual,
            recent_signal=recent_signal,
            insufficient_report=insufficient_report,
        ),
    }
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _load_signal_context(*, cross_asset_output_dir: Path, multi_year_output_dir: Path) -> dict[str, Any]:
    confirmation_rows = _read_csv_rows(
        cross_asset_output_dir / "signals" / "asia_drift_cross_asset_confirmation_sessions.csv"
    )
    confirmation_by_session = {str(row["primary_session_id"]): row for row in confirmation_rows}

    early_rows = _read_csv_rows(
        multi_year_output_dir / "signals" / "asia_drift_multi_year_early_feature_matrix.csv"
    )
    early_features_by_session = {str(row["asia_drift_session_id"]): row for row in early_rows}

    availability_by_session: dict[str, dict[str, Any]] = {}
    for instrument in ("gc", "mgc", "es", "mes", "nq", "mnq"):
        instrument_dir = multi_year_output_dir / instrument
        if not instrument_dir.exists():
            continue
        feature_files = sorted(instrument_dir.glob("*/features/asia_drift_feature_rows.csv"))
        for feature_file in feature_files:
            for session_id, session_rows in _feature_rows_by_session(feature_file).items():
                availability_by_session[session_id] = _session_feature_availability(session_rows)

    return {
        "confirmation_rows": confirmation_rows,
        "confirmation_by_session": confirmation_by_session,
        "availability_by_session": availability_by_session,
        "early_features_by_session": early_features_by_session,
    }


def _inventory_trade_sources(
    *,
    warehouse_root: Path | None,
    runtime_bridge_dir: Path | None,
    operator_dashboard_dir: Path | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    inventory_rows: list[dict[str, Any]] = []
    mappable_trade_rows: list[dict[str, Any]] = []
    insufficient_rows: list[dict[str, Any]] = []

    if warehouse_root is not None:
        warehouse_trades = _load_warehouse_closed_trades(warehouse_root)
        inventory_rows.append(_inventory_summary("warehouse_closed_trades", warehouse_trades, outcome_available=True, provenance_field="provenance_tag"))
        mappable_trade_rows.extend(warehouse_trades)

    if runtime_bridge_dir is not None:
        runtime_trades = _load_runtime_bridge_trades(runtime_bridge_dir)
        inventory_rows.append(_inventory_summary("runtime_bridge_closed_trades", runtime_trades, outcome_available=True, provenance_field="source_provenance_tag"))
        mappable_trade_rows.extend(runtime_trades)

    if operator_dashboard_dir is not None:
        latest_fills = _load_operator_rows(operator_dashboard_dir / "paper_latest_fills_snapshot.json", row_key="rows")
        inventory_rows.append(_inventory_summary("operator_dashboard_latest_fills", latest_fills, outcome_available=False, provenance_field=None))
        insufficient_rows.append(
            {
                "source_name": "operator_dashboard_latest_fills",
                "reason": "fill_events_without_entry_exit_pairing",
                "row_count": len(latest_fills),
                "date_range": _date_range_for_rows(latest_fills, "fill_timestamp"),
            }
        )

        latest_intents = _load_operator_rows(operator_dashboard_dir / "paper_latest_intents_snapshot 4.json", row_key="rows")
        inventory_rows.append(_inventory_summary("operator_dashboard_latest_intents", latest_intents, outcome_available=False, provenance_field=None))
        insufficient_rows.append(
            {
                "source_name": "operator_dashboard_latest_intents",
                "reason": "intent_rows_without_closed_trade_outcomes",
                "row_count": len(latest_intents),
                "date_range": _date_range_for_rows(latest_intents, "created_at"),
            }
        )

        session_close_rows = _load_operator_rows(operator_dashboard_dir / "paper_session_close_review_latest 2.json", row_key="rows")
        inventory_rows.append(_inventory_summary("operator_dashboard_session_close_review", session_close_rows, outcome_available=False, provenance_field=None))
        insufficient_rows.append(
            {
                "source_name": "operator_dashboard_session_close_review",
                "reason": "lane_level_session_summary_not_trade_level",
                "row_count": len(session_close_rows),
                "date_range": _singleton_date_range(
                    _load_single_json(operator_dashboard_dir / "paper_session_close_review_latest 2.json").get("session_date")
                ),
            }
        )

    return inventory_rows, mappable_trade_rows, insufficient_rows


def _load_warehouse_closed_trades(warehouse_root: Path) -> list[dict[str, Any]]:
    dataset_root = warehouse_root / "datasets" / "lane_closed_trades"
    rows: list[dict[str, Any]] = []
    for parquet_path in sorted(dataset_root.glob("symbol=*/year=*/shard_id=*/closed_trades.parquet")):
        table_rows = pq.ParquetFile(parquet_path).read().to_pylist()
        for row in table_rows:
            rows.append(
                {
                    "source_name": "warehouse_closed_trades",
                    "strategy_family": row.get("family"),
                    "lane_id": row.get("lane_id"),
                    "instrument": row.get("symbol"),
                    "entry_ts": _iso_or_none(row.get("entry_ts")),
                    "exit_ts": _iso_or_none(row.get("exit_ts")),
                    "side": row.get("side"),
                    "net_pnl_cash": _as_float(row.get("pnl")),
                    "pnl_points": _as_float(row.get("pnl_points")),
                    "hold_minutes": _as_float(row.get("hold_minutes")),
                    "trade_id": row.get("trade_id"),
                    "candidate_id": row.get("candidate_id"),
                    "provenance_tag": row.get("provenance_tag"),
                    "outcome_available": row.get("pnl") is not None,
                }
            )
    return rows


def _load_runtime_bridge_trades(runtime_bridge_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _read_jsonl_rows(runtime_bridge_dir / "trades.jsonl"):
        rows.append(
            {
                "source_name": "runtime_bridge_closed_trades",
                "strategy_family": row.get("strategy_family"),
                "lane_id": row.get("lane_id"),
                "instrument": row.get("instrument"),
                "entry_ts": row.get("entry_ts"),
                "exit_ts": row.get("exit_ts"),
                "side": row.get("side"),
                "net_pnl_cash": _as_float(row.get("realized_pnl_cash")),
                "pnl_points": _as_float(row.get("pnl_points")),
                "hold_minutes": _as_float(row.get("hold_minutes")),
                "trade_id": row.get("trade_id") or row.get("research_trade_id"),
                "candidate_id": row.get("research_candidate_id"),
                "provenance_tag": row.get("source_provenance_tag"),
                "outcome_available": row.get("realized_pnl_cash") is not None,
            }
        )
    return rows


def _map_trades(
    *,
    trade_rows: Sequence[dict[str, Any]],
    confirmation_by_session: dict[str, dict[str, Any]],
    availability_by_session: dict[str, dict[str, Any]],
    early_features_by_session: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    mapped_rows: list[dict[str, Any]] = []
    for trade in trade_rows:
        instrument = str(trade.get("instrument") or "").upper()
        entry_ts = _parse_dt(trade.get("entry_ts"))
        if not instrument or entry_ts is None:
            continue
        local_entry = entry_ts.astimezone(NEW_YORK)
        session_date = _session_date(local_entry, session_anchor=DEFAULT_SESSION_CONFIG.session_anchor)
        session_id = f"{instrument.lower()}__{session_date.isoformat()}__asia_drift_v1"
        confirmation_row = confirmation_by_session.get(session_id)
        partner_session_id = str(confirmation_row.get("confirmation_partner_session_id")) if confirmation_row else None
        primary_availability = availability_by_session.get(session_id)
        partner_availability = availability_by_session.get(partner_session_id) if partner_session_id else None
        available_after = _max_dt(
            _parse_dt((primary_availability or {}).get("early_window_end_ts")),
            _parse_dt((partner_availability or {}).get("early_window_end_ts")),
        )
        in_cross_asset_window = _in_cross_asset_window(local_entry)
        signal_available = bool(confirmation_row and in_cross_asset_window and available_after is not None and entry_ts >= available_after)

        early_feature_row = early_features_by_session.get(session_id) if signal_available else None
        contamination_flag = _as_bool((confirmation_row or {}).get("contamination_flag"))
        tier = str((confirmation_row or {}).get("confirmation_tier") or "")
        if confirmation_row is None:
            bucket = OUTCOME_BUCKET_UNMAPPED
        elif not in_cross_asset_window:
            bucket = OUTCOME_BUCKET_OUT_OF_SCOPE
        elif not signal_available:
            bucket = OUTCOME_BUCKET_INSUFFICIENT
        elif contamination_flag:
            bucket = OUTCOME_BUCKET_CONTAMINATED
        elif tier in {TIER_A, TIER_B, TIER_C}:
            bucket = OUTCOME_BUCKET_CONFIRMED
        else:
            bucket = OUTCOME_BUCKET_NO_CONFIRMATION

        mapped_rows.append(
            {
                **trade,
                "local_session_date": session_date.isoformat(),
                "mapped_session_id": session_id,
                "entry_local_ts": local_entry.isoformat(),
                "entry_in_cross_asset_window": in_cross_asset_window,
                "session_confirmation_tier": tier or None,
                "session_contamination_flag": contamination_flag if confirmation_row is not None else None,
                "pre_entry_signal_available": signal_available,
                "pre_entry_confirmation_bucket": bucket,
                "pre_entry_confirmation_tier": tier if signal_available else None,
                "signal_available_after_ts": available_after.isoformat() if available_after is not None else None,
                "confirmation_partner_instrument": (confirmation_row or {}).get("confirmation_partner_instrument"),
                "direction_agreement": _coerce_optional_bool((confirmation_row or {}).get("direction_agreement"), signal_available),
                "signed_vwap_alignment": _coerce_optional_bool((confirmation_row or {}).get("signed_vwap_alignment"), signal_available),
                "directional_efficiency_alignment": _coerce_optional_bool((confirmation_row or {}).get("directional_efficiency_alignment"), signal_available),
                "drift_context_agreement": _coerce_optional_bool((confirmation_row or {}).get("drift_context_agreement"), signal_available),
                "impulse_timing_agreement": _coerce_optional_bool((confirmation_row or {}).get("impulse_timing_agreement"), signal_available),
                "failed_mean_reversion_alignment": _coerce_optional_bool((confirmation_row or {}).get("failed_mean_reversion_alignment"), signal_available),
                "vwap_noise_suppressed": _coerce_optional_bool((confirmation_row or {}).get("vwap_noise_suppressed"), signal_available),
                "lead_lag_category": (confirmation_row or {}).get("lead_lag_category") if signal_available else None,
                "lead_asset": (confirmation_row or {}).get("lead_asset") if signal_available else None,
                "early_signed_vwap_displacement_peak": early_feature_row.get("early_signed_vwap_displacement_peak") if early_feature_row else None,
                "early_drift_context_fraction": early_feature_row.get("early_drift_context_fraction") if early_feature_row else None,
                "early_failed_countertrend_rate": early_feature_row.get("early_failed_countertrend_rate") if early_feature_row else None,
                "early_vwap_reclaim_rate": early_feature_row.get("early_vwap_reclaim_rate") if early_feature_row else None,
                "early_post_spike_fraction": early_feature_row.get("early_post_spike_fraction") if early_feature_row else None,
                "early_deep_damage_fraction": early_feature_row.get("early_deep_damage_fraction") if early_feature_row else None,
                "outcome_positive": (_as_float(trade.get("net_pnl_cash")) or 0.0) > 0 if trade.get("outcome_available") else None,
            }
        )
    return mapped_rows


def _tier_outcome_summary(mapped_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    bucketed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in mapped_rows:
        bucketed[str(row["pre_entry_confirmation_bucket"])].append(row)
    summaries = [_outcome_bucket_summary(name=bucket, rows=rows) for bucket, rows in sorted(bucketed.items())]
    return {
        "bucket_summaries": summaries,
        "confirmed_trade_count": sum(row["sample_size"] for row in summaries if row["bucket_name"] == OUTCOME_BUCKET_CONFIRMED),
        "mapped_trade_count": len(mapped_rows),
    }


def _counterfactual_summary(mapped_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    eligible = [row for row in mapped_rows if _as_bool(row.get("pre_entry_signal_available")) and row.get("outcome_available")]
    allowed = [row for row in eligible if row.get("pre_entry_confirmation_tier") in {TIER_A, TIER_B}]
    blocked = [row for row in eligible if row.get("pre_entry_confirmation_tier") not in {TIER_A, TIER_B}]
    return {
        "eligible_trade_count": len(eligible),
        "allowed_tier_ab_count": len(allowed),
        "blocked_count": len(blocked),
        "trade_count_reduction": _ratio(len(blocked), len(eligible)),
        "allowed_summary": _simple_trade_group_summary("allowed_tier_ab", allowed),
        "blocked_summary": _simple_trade_group_summary("blocked_non_tier_ab", blocked),
    }


def _recent_signal_summary(*, confirmation_rows: Sequence[dict[str, Any]], recent_session_count: int) -> dict[str, Any]:
    metal_rows = [row for row in confirmation_rows if str(row.get("primary_instrument")) in {"MGC", "GC"}]
    sorted_rows = sorted(
        metal_rows,
        key=lambda row: (str(row.get("local_session_date") or ""), str(row.get("primary_instrument") or "")),
    )
    recent_rows = sorted_rows[-recent_session_count:] if recent_session_count > 0 else sorted_rows
    tier_ab_recent = [row for row in recent_rows if str(row.get("confirmation_tier")) in {TIER_A, TIER_B}]
    tier_ab_global = [row for row in metal_rows if str(row.get("confirmation_tier")) in {TIER_A, TIER_B}]

    def _row_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
        return {
            "sample_size": len(rows),
            "coverage_start": min((str(row.get("local_session_date") or "") for row in rows), default=None),
            "coverage_end": max((str(row.get("local_session_date") or "") for row in rows), default=None),
            "tier_ab_count": len([row for row in rows if str(row.get("confirmation_tier")) in {TIER_A, TIER_B}]),
            "directional_resolution_rate": _ratio(
                sum(1 for row in rows if _as_bool(row.get("directional_resolution"))),
                len(rows),
            ),
            "contamination_rate": _ratio(
                sum(1 for row in rows if _as_bool(row.get("contamination_flag"))),
                len(rows),
            ),
            "median_continuation_atr": _median(
                [_as_float(row.get("continuation_magnitude_atr")) for row in rows if row.get("continuation_magnitude_atr") is not None]
            ),
        }

    if not recent_rows:
        status = "insufficient_recent_sessions"
    elif not tier_ab_recent:
        status = "signal_not_present_in_recent_sessions"
    else:
        status = "signal_present_but_small_sample"

    return {
        "recent_session_count_requested": recent_session_count,
        "recent_session_summary": _row_summary(recent_rows),
        "recent_tier_ab_summary": _row_summary(tier_ab_recent),
        "march_april_tier_ab_summary": _row_summary(tier_ab_global),
        "status": status,
    }


def _insufficient_data_report(
    *,
    insufficient_rows: Sequence[dict[str, Any]],
    mapped_rows: Sequence[dict[str, Any]],
    inventory_rows: Sequence[dict[str, Any]],
    recent_signal: dict[str, Any],
) -> dict[str, Any]:
    mapped_sources = Counter(str(row.get("source_name") or "UNKNOWN") for row in mapped_rows)
    missing_overlap = sum(1 for row in mapped_rows if row.get("pre_entry_confirmation_tier") in {None, ""})
    return {
        "insufficient_sources": list(insufficient_rows),
        "mapped_source_counts": dict(mapped_sources),
        "mapped_trade_count": len(mapped_rows),
        "mapped_without_pre_entry_tier": missing_overlap,
        "recent_signal_status": recent_signal.get("status"),
        "notes": [
            "Operator dashboard fills and intents are inventory-visible but do not provide closed-trade pairing for outcome analysis.",
            "Historical warehouse closed trades do not extend into the March-April 2026 clean confirmation cluster, which limits overlap-based inference.",
            "Recent structural signal activity can be evaluated through session labels even when current trade artifacts remain outcome-thin.",
        ],
    }


def _recommendation(
    *,
    mapped_rows: Sequence[dict[str, Any]],
    counterfactual: dict[str, Any],
    recent_signal: dict[str, Any],
    insufficient_report: dict[str, Any],
) -> dict[str, Any]:
    allowed = counterfactual.get("allowed_summary") or {}
    recent_status = str(recent_signal.get("status") or "")
    if allowed.get("sample_size", 0) <= 2 or recent_status != "signal_present_but_small_sample":
        recommendation = "more_paper_collection_only"
        reason = (
            "Cross-asset confirmation remains interesting structurally, but mapped trade overlap is too thin and the recent-signal slice "
            "is still a small-sample observation rather than a durable operational base."
        )
    else:
        recommendation = "no_live_consideration_yet"
        reason = (
            "Even where Tier A/B looks cleaner, the trade-artifact overlap is still too concentrated to justify any live consideration."
        )
    return {
        "recommendation": recommendation,
        "reason": reason,
        "mapped_trade_count": len(mapped_rows),
        "eligible_counterfactual_trade_count": counterfactual.get("eligible_trade_count"),
        "recent_signal_status": recent_status,
        "missing_evidence": insufficient_report.get("notes"),
    }


def _inventory_summary(
    source_name: str,
    rows: Sequence[dict[str, Any]],
    *,
    outcome_available: bool,
    provenance_field: str | None,
) -> dict[str, Any]:
    instruments = Counter(str(row.get("instrument") or "UNKNOWN") for row in rows)
    strategies = Counter(str(row.get("lane_id") or row.get("strategy_family") or "UNKNOWN") for row in rows)
    provenance_count = sum(1 for row in rows if provenance_field and row.get(provenance_field))
    all_fields = sorted({key for row in rows for key in row})
    return {
        "source_name": source_name,
        "trade_count": len(rows),
        "date_range": _date_range_for_rows(rows, "entry_ts", fallback_field="fill_timestamp"),
        "instrument_breakdown": dict(instruments),
        "strategy_breakdown_top": dict(strategies.most_common(12)),
        "fields_available": all_fields,
        "entry_timestamp_available": any(row.get("entry_ts") for row in rows),
        "direction_available": any(row.get("side") for row in rows),
        "outcome_available": outcome_available,
        "layer1_provenance_exists": provenance_count > 0,
        "provenance_coverage_rate": _ratio(provenance_count, len(rows)),
    }


def _simple_trade_group_summary(group_name: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [_as_float(row.get("net_pnl_cash")) for row in rows if row.get("net_pnl_cash") is not None]
    positives = sum(1 for row in rows if row.get("outcome_positive") is True)
    negatives = sum(1 for row in rows if row.get("outcome_positive") is False)
    return {
        "group_name": group_name,
        "sample_size": len(rows),
        "positive_rate": _ratio(positives, len(rows)),
        "negative_rate": _ratio(negatives, len(rows)),
        "avg_net_pnl_cash": mean(pnls) if pnls else None,
        "median_net_pnl_cash": median(pnls) if pnls else None,
        "avg_hold_minutes": mean([_as_float(row.get("hold_minutes")) for row in rows if row.get("hold_minutes") is not None]) if rows else None,
    }


def _outcome_bucket_summary(*, name: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [_as_float(row.get("net_pnl_cash")) for row in rows if row.get("net_pnl_cash") is not None]
    hold_minutes = [_as_float(row.get("hold_minutes")) for row in rows if row.get("hold_minutes") is not None]
    positives = sum(1 for row in rows if row.get("outcome_positive") is True)
    negatives = sum(1 for row in rows if row.get("outcome_positive") is False)
    return {
        "bucket_name": name,
        "sample_size": len(rows),
        "positive_count": positives,
        "negative_count": negatives,
        "positive_rate": _ratio(positives, len(rows)),
        "avg_net_pnl_cash": mean(pnls) if pnls else None,
        "median_net_pnl_cash": median(pnls) if pnls else None,
        "avg_hold_minutes": mean(hold_minutes) if hold_minutes else None,
        "instrument_breakdown": dict(Counter(str(row.get("instrument") or "UNKNOWN") for row in rows)),
        "source_breakdown": dict(Counter(str(row.get("source_name") or "UNKNOWN") for row in rows)),
    }


def _feature_rows_by_session(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _read_csv_rows(path):
        grouped[str(row["asia_drift_session_id"])].append(row)
    for rows in grouped.values():
        rows.sort(key=lambda row: _parse_dt(row.get("decision_ts")) or datetime.min.replace(tzinfo=NEW_YORK))
    return grouped


def _session_feature_availability(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    in_scope_rows = [row for row in rows if _as_bool(row.get("in_scope"), default=False)] or list(rows)
    candidate_index = next(
        (
            index
            for index, row in enumerate(in_scope_rows)
            if str(row.get("regime") or "") in {"ASIA_DRIFT_LONG", "ASIA_DRIFT_SHORT"}
        ),
        None,
    )
    early_rows = (
        in_scope_rows[candidate_index : candidate_index + 12]
        if candidate_index is not None
        else in_scope_rows[: min(12, len(in_scope_rows))]
    )
    if not early_rows:
        return {}
    return {
        "early_window_start_ts": early_rows[0].get("decision_ts"),
        "early_window_end_ts": early_rows[-1].get("decision_ts"),
        "early_bar_count": len(early_rows),
        "candidate_bar_index": candidate_index,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _load_operator_rows(path: Path, *, row_key: str) -> list[dict[str, Any]]:
    payload = _load_single_json(path)
    rows = payload.get(row_key) if isinstance(payload, dict) else None
    return list(rows) if isinstance(rows, list) else []


def _load_single_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _date_range_for_rows(rows: Sequence[dict[str, Any]], primary_field: str, *, fallback_field: str | None = None) -> dict[str, Any] | None:
    timestamps = [
        _parse_dt(row.get(primary_field) or (row.get(fallback_field) if fallback_field else None))
        for row in rows
    ]
    present = [ts for ts in timestamps if ts is not None]
    if not present:
        return None
    return {"start": min(present).isoformat(), "end": max(present).isoformat()}


def _singleton_date_range(value: Any) -> dict[str, Any] | None:
    if value in {None, ""}:
        return None
    return {"start": str(value), "end": str(value)}


def _in_cross_asset_window(local_entry: datetime) -> bool:
    anchor = DEFAULT_SESSION_CONFIG.session_anchor
    mandatory_exit = DEFAULT_SESSION_CONFIG.mandatory_exit
    local_time = local_entry.timetz().replace(tzinfo=None)
    return local_time >= anchor or local_time <= mandatory_exit


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    inventory_path = layout["reports"] / "asia_drift_cross_asset_trade_inventory.json"
    mapped_trades_path = layout["trades"] / "asia_drift_cross_asset_mapped_trades.csv"
    tier_summary_json_path = layout["reports"] / "asia_drift_cross_asset_trade_tier_summary.json"
    tier_summary_md_path = layout["reports"] / "asia_drift_cross_asset_trade_tier_summary.md"
    counterfactual_json_path = layout["reports"] / "asia_drift_cross_asset_trade_counterfactual.json"
    recent_signal_json_path = layout["reports"] / "asia_drift_cross_asset_recent_signal.json"
    insufficient_json_path = layout["reports"] / "asia_drift_cross_asset_insufficient_data.json"
    summary_json_path = layout["reports"] / "asia_drift_cross_asset_trade_mapping_summary.json"
    summary_md_path = layout["reports"] / "asia_drift_cross_asset_trade_mapping_summary.md"

    inventory_path.write_text(json.dumps(payload.get("trade_artifact_inventory") or [], indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(mapped_trades_path, payload.get("mapped_trade_rows") or [])
    tier_summary_json_path.write_text(json.dumps(payload.get("tier_outcome_summary") or {}, indent=2, sort_keys=True), encoding="utf-8")
    tier_summary_md_path.write_text(_render_tier_summary_markdown(payload), encoding="utf-8")
    counterfactual_json_path.write_text(json.dumps(payload.get("counterfactual_filter_summary") or {}, indent=2, sort_keys=True), encoding="utf-8")
    recent_signal_json_path.write_text(json.dumps(payload.get("recent_signal_summary") or {}, indent=2, sort_keys=True), encoding="utf-8")
    insufficient_json_path.write_text(json.dumps(payload.get("insufficient_data_report") or {}, indent=2, sort_keys=True), encoding="utf-8")
    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path.write_text(_render_summary_markdown(payload), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_cross_asset_trade_mapping",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "inventory_path": str(inventory_path),
                "mapped_trades_path": str(mapped_trades_path),
                "tier_summary_json_path": str(tier_summary_json_path),
                "tier_summary_md_path": str(tier_summary_md_path),
                "counterfactual_json_path": str(counterfactual_json_path),
                "recent_signal_json_path": str(recent_signal_json_path),
                "insufficient_json_path": str(insufficient_json_path),
                "summary_json_path": str(summary_json_path),
                "summary_md_path": str(summary_md_path),
            },
        },
    )
    return {
        "inventory_path": str(inventory_path),
        "mapped_trades_path": str(mapped_trades_path),
        "tier_summary_json_path": str(tier_summary_json_path),
        "tier_summary_md_path": str(tier_summary_md_path),
        "counterfactual_json_path": str(counterfactual_json_path),
        "recent_signal_json_path": str(recent_signal_json_path),
        "insufficient_json_path": str(insufficient_json_path),
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Cross-Asset Trade Mapping",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Inventory",
    ]
    for row in payload.get("trade_artifact_inventory") or []:
        lines.append(
            f"- {row.get('source_name')}: trades={row.get('trade_count')} entry_ts={row.get('entry_timestamp_available')} "
            f"outcome={row.get('outcome_available')} provenance={row.get('layer1_provenance_exists')} range={row.get('date_range')}"
        )
    lines.extend(["", "## Tier Outcome Summary"])
    for row in (payload.get("tier_outcome_summary") or {}).get("bucket_summaries") or []:
        lines.append(
            f"- {row['bucket_name']}: sample_size={row['sample_size']} positive_rate={row['positive_rate']} "
            f"avg_pnl={row['avg_net_pnl_cash']}"
        )
    lines.extend(["", "## Counterfactual"])
    counterfactual = payload.get("counterfactual_filter_summary") or {}
    lines.append(
        f"- eligible={counterfactual.get('eligible_trade_count')} allowed_tier_ab={counterfactual.get('allowed_tier_ab_count')} "
        f"blocked={counterfactual.get('blocked_count')} reduction={counterfactual.get('trade_count_reduction')}"
    )
    lines.extend(["", "## Recent Signal"])
    recent = payload.get("recent_signal_summary") or {}
    lines.append(
        f"- status={recent.get('status')} recent_summary={recent.get('recent_session_summary')} "
        f"recent_tier_ab={recent.get('recent_tier_ab_summary')}"
    )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation={recommendation.get('recommendation')}")
    lines.append(f"- reason={recommendation.get('reason')}")
    return "\n".join(lines) + "\n"


def _render_tier_summary_markdown(payload: dict[str, Any]) -> str:
    lines = ["# Cross-Asset Trade Tier Outcome Summary", ""]
    for row in (payload.get("tier_outcome_summary") or {}).get("bucket_summaries") or []:
        lines.append(
            f"- {row['bucket_name']}: sample_size={row['sample_size']} positive_rate={row['positive_rate']} "
            f"avg_pnl={row['avg_net_pnl_cash']} instruments={row['instrument_breakdown']}"
        )
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    if value in {None, ""}:
        return None
    return str(value)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value in {None, ""}:
        return None
    return datetime.fromisoformat(str(value))


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _coerce_optional_bool(value: Any, enabled: bool) -> bool | None:
    if not enabled:
        return None
    return _as_bool(value)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _median(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    return median(clean) if clean else None


def _max_dt(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None
