"""Publish parallel ATP loosened-rule historical playback studies."""

from __future__ import annotations

import argparse
import json
import os
import platform
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from ..research.trend_participation.phase3_timing import ATP_REPLAY_EXIT_POLICY_FIXED_TARGET
from ..config_models import load_settings_from_files
from . import strategy_universe_retest as retest
from .session_phase_labels import label_session_phase

REPO_ROOT = Path.cwd()
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "atp_loosened_history_v1"
DEFAULT_EXIT_POLICY_MATRIX_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "atp_exit_policy_matrix_v1"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_START_TIMESTAMP = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
DEFAULT_END_TIMESTAMP = datetime.fromisoformat("2026-04-11T00:00:00+00:00")
DEFAULT_STUDY_SUFFIX = "_loosened_v1"
DEFAULT_LABEL_SUFFIX = " [Loosened v1]"
DEFAULT_TARGET_CONFIGS = (
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_promotion_1_075r_favorable_only.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml",
)
PRODUCTION_TRACK_OVERLAY_ID = "atp_us_late_2bar_no_traction_plus_adverse"
PRODUCTION_TRACK_DEFAULTS = {
    "min_favorable_excursion_r": 0.25,
    "adverse_excursion_abort_r": 0.65,
    "logic_mode": "all",
    "apply_subwindows": ["US_LATE"],
}

DEFAULT_EXIT_POLICY_MATRIX_POLICIES = (
    "fixed_target_time_stop",
    "target_checkpoint_trail",
    "target_checkpoint_trail_long_hold",
    "target_checkpoint_no_traction_abort",
)
DEFAULT_SAFE_MODE = True


def _safe_parallel_cap() -> int:
    # macOS research runs have proven much more fragile under concurrent replay pressure.
    return 1 if platform.system() == "Darwin" else 2


def _resolve_research_max_workers(
    *,
    job_count: int,
    requested_max_workers: int | None,
    safe_mode: bool,
    unsafe_cap: int,
) -> int:
    if job_count <= 0:
        return 1
    cpu_cap = max(1, os.cpu_count() or 1)
    if requested_max_workers is not None:
        return max(1, min(job_count, requested_max_workers))
    cap = _safe_parallel_cap() if safe_mode else max(1, min(cpu_cap, unsafe_cap))
    return max(1, min(job_count, cap))


def _overlay_abort_reasons(
    *,
    entry_fill_price: float,
    risk_points: float,
    bars: Sequence[Any],
    min_favorable_excursion_r: float,
    adverse_excursion_abort_r: float,
    logic_mode: str,
) -> list[str]:
    if len(bars) < 2:
        return []
    first_two = list(bars[:2])
    running_mfe = max(float(candidate.high) - entry_fill_price for candidate in first_two)
    running_mae = max(entry_fill_price - float(candidate.low) for candidate in first_two)
    no_traction = running_mfe < (max(risk_points, 1e-9) * float(min_favorable_excursion_r))
    bad_adverse = running_mae >= (max(risk_points, 1e-9) * float(adverse_excursion_abort_r))
    reasons: list[str] = []
    if no_traction:
        reasons.append("no_traction")
    if bad_adverse:
        reasons.append("adverse_excursion")
    normalized_logic = str(logic_mode or "all").strip().lower()
    if normalized_logic == "any":
        return reasons
    if no_traction and bad_adverse:
        return reasons
    return []


def _apply_production_track_overlay(
    *,
    trade_rows: Sequence[dict[str, Any]],
    bars_1m: Sequence[Any],
    point_value: Decimal,
    runtime_overlay_id: str | None,
    runtime_overlay_params: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if str(runtime_overlay_id or "").strip() != PRODUCTION_TRACK_OVERLAY_ID:
        return list(trade_rows)
    profile = dict(PRODUCTION_TRACK_DEFAULTS)
    profile.update(dict(runtime_overlay_params or {}))
    bars_by_instrument: dict[str, list[Any]] = {}
    for bar in sorted(bars_1m, key=lambda item: item.end_ts):
        bars_by_instrument.setdefault(str(getattr(bar, "instrument", "")), []).append(bar)
    adjusted_rows: list[dict[str, Any]] = []
    for row in trade_rows:
        trade_record = row.get("trade_record")
        if trade_record is None or str(getattr(trade_record, "side", "")).upper() != "LONG":
            adjusted_rows.append(dict(row))
            continue
        entry_ts = getattr(trade_record, "entry_ts", None)
        exit_ts = getattr(trade_record, "exit_ts", None)
        if entry_ts is None or exit_ts is None:
            adjusted_rows.append(dict(row))
            continue
        if label_session_phase(entry_ts) not in set(profile.get("apply_subwindows") or ()):
            adjusted_rows.append(dict(row))
            continue
        instrument_bars = bars_by_instrument.get(str(getattr(trade_record, "instrument", "")), [])
        post_entry_bars = [
            candidate
            for candidate in instrument_bars
            if candidate.end_ts > entry_ts and candidate.end_ts <= exit_ts
        ]
        reasons = _overlay_abort_reasons(
            entry_fill_price=float(getattr(trade_record, "entry_price")),
            risk_points=abs(float(getattr(trade_record, "entry_price")) - float(getattr(trade_record, "stop_price"))),
            bars=post_entry_bars,
            min_favorable_excursion_r=float(profile.get("min_favorable_excursion_r") or 0.0),
            adverse_excursion_abort_r=float(profile.get("adverse_excursion_abort_r") or 0.0),
            logic_mode=str(profile.get("logic_mode") or "all"),
        )
        if not reasons or len(post_entry_bars) < 2:
            adjusted_rows.append(dict(row))
            continue
        overlay_bar = post_entry_bars[1]
        if overlay_bar.end_ts >= exit_ts:
            adjusted_rows.append(dict(row))
            continue
        adjusted_exit_price = float(overlay_bar.close) - 0.25
        adjusted_pnl_points = adjusted_exit_price - float(getattr(trade_record, "entry_price"))
        adjusted_pnl_cash = adjusted_pnl_points * float(point_value) - float(getattr(trade_record, "fees_paid", 1.5))
        adjusted_trade_record = replace(
            trade_record,
            exit_ts=overlay_bar.end_ts,
            exit_price=adjusted_exit_price,
            pnl_points=adjusted_pnl_points,
            pnl_cash=adjusted_pnl_cash,
            gross_pnl_cash=(float(overlay_bar.close) - float(getattr(trade_record, "entry_price"))) * float(point_value),
            bars_held_1m=2,
            hold_minutes=2.0,
            exit_reason="atp_companion_us_late_no_traction_adverse_abort",
            stopout=False,
        )
        adjusted_rows.append(
            {
                **dict(row),
                "exit_timestamp": overlay_bar.end_ts.isoformat(),
                "exit_price": round(adjusted_exit_price, 6),
                "exit_reason": "atp_companion_us_late_no_traction_adverse_abort",
                "realized_pnl": round(adjusted_pnl_cash, 6),
                "trade_record": adjusted_trade_record,
            }
        )
    return adjusted_rows


def _evaluate_loosened_lane_worker(
    *,
    lane: dict[str, Any],
    historical_playback_dir: str,
    source_database_paths: Sequence[str],
    start_timestamp_iso: str,
    end_timestamp_iso: str,
    exit_policy: str,
) -> dict[str, Any]:
    symbol = str(lane["symbol"])
    start_timestamp = datetime.fromisoformat(start_timestamp_iso)
    end_timestamp = datetime.fromisoformat(end_timestamp_iso)
    bar_source_index = retest._discover_best_sources(
        symbols={symbol},
        timeframes={"1m", "5m"},
        sqlite_paths=tuple(Path(path) for path in source_database_paths),
    )
    current = retest._evaluate_atp_lane(
        symbol=symbol,
        allowed_sessions=set(str(session) for session in (lane.get("allowed_sessions") or [])),
        point_value=Decimal(str(lane["point_value"])),
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        include_prior_comparator=False,
        quality_bucket_policy=str(lane.get("quality_bucket_policy") or "").strip() or None,
        allow_pre_5m_context_participation=bool(lane.get("allow_pre_5m_context_participation", False)),
        sides=tuple(
            side
            for side, sources in (
                ("LONG", lane.get("long_sources") or ()),
                ("SHORT", lane.get("short_sources") or ()),
            )
            if sources
        ) or ("LONG",),
        exit_policy=exit_policy,
    )
    result: dict[str, Any] = {
        "strategy_id": str(lane["strategy_id"]),
        "display_name": str(lane["display_name"]),
        "symbol": symbol,
        "status": str(lane["lane_status"]),
        "execution_model": retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
        "study_mode": "research_execution_mode",
    }
    if current is None:
        result["normalized_result"] = None
        result["study_row"] = None
        result["trade_count"] = 0
        return result
    current_trade_rows = _apply_production_track_overlay(
        trade_rows=current["trade_rows"],
        bars_1m=current["bars_1m"],
        point_value=Decimal(str(lane["point_value"])),
        runtime_overlay_id=str(lane.get("runtime_overlay_id") or "").strip() or None,
        runtime_overlay_params=dict(lane.get("runtime_overlay_params") or {}),
    )
    metrics = retest._summarize_trade_rows(current_trade_rows, bar_count=max(len(current["bars_1m"]), 1))
    normalized_result = retest._normalize_result_row(
        {
            "strategy_id": lane["strategy_id"],
            "display_name": lane["display_name"],
            "status": lane["lane_status"],
            "family": lane["family"],
            "symbol": lane["symbol"],
            "cohort": lane["cohort"],
            "metrics": metrics,
            "prior_method_comparison": retest._empty_summary(),
            "material_improvement": "parallel_variant_only",
            "recommendation": "parallel_loosened_variant",
            "data_limit_status": retest._probationary_data_limit_status(
                symbol=symbol,
                bar_source_index=bar_source_index,
                current=current,
            ),
            "execution_model": retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
            "prior_execution_model": None,
            "coverage": {
                "raw_market_data": retest._source_range_payload(bar_source_index.get(symbol, {}).get("1m")),
                "derived_playback": retest._trade_range_payload(current_trade_rows),
                "closed_trade_economics": retest._trade_range_payload(current_trade_rows, field_name="exit_timestamp"),
            },
        }
    )
    study_payload = retest._build_synthetic_strategy_study(
        symbol=symbol,
        study_id=str(lane["strategy_id"]),
        display_name=str(lane["display_name"]),
        strategy_family=str(lane["family"]),
        study_mode="research_execution_mode",
        bars_1m=list(current["bars_1m"]),
        trade_rows=current_trade_rows,
        point_value=Decimal(str(lane["point_value"])),
        candidate_id=lane.get("candidate_id"),
        entry_model="CURRENT_CANDLE_VWAP",
        execution_model_label=retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
        pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
        lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
        intrabar_execution_authoritative=True,
        authoritative_intrabar_available=True,
    )
    study_path_pair = retest._write_study_payload(
        payload=study_payload,
        artifact_prefix=f"historical_playback_{lane['strategy_id']}",
        historical_playback_dir=Path(historical_playback_dir),
    )
    result["normalized_result"] = normalized_result
    result["study_row"] = {
        "strategy_id": lane["strategy_id"],
        "symbol": lane["symbol"],
        "label": lane["display_name"],
        "study_mode": "research_execution_mode",
        "execution_model": retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
        "summary_payload": retest._study_summary_payload(metrics, lane["lane_status"]),
        "strategy_study_json_path": str(study_path_pair["json"]),
        "strategy_study_markdown_path": str(study_path_pair["markdown"]),
    }
    result["trade_count"] = len(current_trade_rows)
    return result


def _evaluate_exit_policy_worker(
    *,
    lane: dict[str, Any],
    source_database_paths: Sequence[str],
    start_timestamp_iso: str,
    end_timestamp_iso: str,
    exit_policy: str,
) -> dict[str, Any]:
    symbol = str(lane["symbol"])
    start_timestamp = datetime.fromisoformat(start_timestamp_iso)
    end_timestamp = datetime.fromisoformat(end_timestamp_iso)
    bar_source_index = retest._discover_best_sources(
        symbols={symbol},
        timeframes={"1m", "5m"},
        sqlite_paths=tuple(Path(path) for path in source_database_paths),
    )
    current = retest._evaluate_atp_lane(
        symbol=symbol,
        allowed_sessions=set(str(session) for session in (lane.get("allowed_sessions") or [])),
        point_value=Decimal(str(lane["point_value"])),
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        include_prior_comparator=False,
        quality_bucket_policy=str(lane.get("quality_bucket_policy") or "").strip() or None,
        allow_pre_5m_context_participation=bool(lane.get("allow_pre_5m_context_participation", False)),
        sides=tuple(
            side
            for side, sources in (
                ("LONG", lane.get("long_sources") or ()),
                ("SHORT", lane.get("short_sources") or ()),
            )
            if sources
        ) or ("LONG",),
        exit_policy=exit_policy,
    )
    result: dict[str, Any] = {
        "strategy_id": str(lane["strategy_id"]),
        "display_name": str(lane["display_name"]),
        "symbol": symbol,
        "exit_policy": exit_policy,
    }
    if current is None:
        result["metrics"] = None
        result["exit_mix"] = {}
        result["trade_count"] = 0
        return result
    current_trade_rows = _apply_production_track_overlay(
        trade_rows=current["trade_rows"],
        bars_1m=current["bars_1m"],
        point_value=Decimal(str(lane["point_value"])),
        runtime_overlay_id=str(lane.get("runtime_overlay_id") or "").strip() or None,
        runtime_overlay_params=dict(lane.get("runtime_overlay_params") or {}),
    )
    metrics = retest._summarize_trade_rows(current_trade_rows, bar_count=max(len(current["bars_1m"]), 1))
    exit_reason_counts: dict[str, int] = {}
    for trade_row in current_trade_rows:
        reason = str(trade_row.get("exit_reason") or "UNKNOWN")
        exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1
    result["metrics"] = metrics
    result["trade_count"] = len(current_trade_rows)
    result["exit_mix"] = {
        reason: {
            "count": count,
            "pct": round((count / max(len(current_trade_rows), 1)) * 100.0, 2),
        }
        for reason, count in sorted(exit_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    }
    return result


def run_atp_loosened_history_publish(
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    start_timestamp: datetime = DEFAULT_START_TIMESTAMP,
    end_timestamp: datetime = DEFAULT_END_TIMESTAMP,
    source_database_paths: Sequence[str | Path] | None = None,
    preserve_base: bool = False,
    study_suffix: str = DEFAULT_STUDY_SUFFIX,
    label_suffix: str = DEFAULT_LABEL_SUFFIX,
    target_configs: Sequence[str | Path] | None = None,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    safe_mode: bool = DEFAULT_SAFE_MODE,
    max_workers: int | None = None,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)

    resolved_configs = tuple(Path(path) for path in (target_configs or DEFAULT_TARGET_CONFIGS))
    loosened_lanes = [
        _load_loosened_lane_from_config(
            config_path=config_path,
            study_suffix=study_suffix,
            label_suffix=label_suffix,
        )
        for config_path in resolved_configs
    ]
    active_symbols = {str(row["symbol"]) for row in loosened_lanes}
    bar_source_index = retest._discover_best_sources(
        symbols=active_symbols,
        timeframes={"1m", "5m"},
        sqlite_paths=source_database_paths or (DEFAULT_SOURCE_DB,),
    )
    normalized_results: list[dict[str, Any]] = []
    new_studies: list[dict[str, Any]] = []
    execution_model = retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP
    resolved_source_paths = [str(path) for path in (source_database_paths or (DEFAULT_SOURCE_DB,))]
    resolved_max_workers = _resolve_research_max_workers(
        job_count=len(loosened_lanes),
        requested_max_workers=max_workers,
        safe_mode=safe_mode,
        unsafe_cap=3,
    )
    completed = 0
    if resolved_max_workers == 1:
        for lane in loosened_lanes:
            payload = _evaluate_loosened_lane_worker(
                lane=lane,
                historical_playback_dir=str(historical_playback_dir),
                source_database_paths=resolved_source_paths,
                start_timestamp_iso=start_timestamp.isoformat(),
                end_timestamp_iso=end_timestamp.isoformat(),
                exit_policy=exit_policy,
            )
            completed += 1
            print(
                f"[{completed}/{len(loosened_lanes)}] {payload['strategy_id']} "
                f"symbol={payload['symbol']} trades={payload['trade_count']}",
                flush=True,
            )
            normalized_result = payload.get("normalized_result")
            study_row = payload.get("study_row")
            if normalized_result is not None:
                normalized_results.append(dict(normalized_result))
            if study_row is not None:
                new_studies.append(dict(study_row))
    else:
        with ProcessPoolExecutor(max_workers=resolved_max_workers) as executor:
            future_map = {
                executor.submit(
                    _evaluate_loosened_lane_worker,
                    lane=lane,
                    historical_playback_dir=str(historical_playback_dir),
                    source_database_paths=resolved_source_paths,
                    start_timestamp_iso=start_timestamp.isoformat(),
                    end_timestamp_iso=end_timestamp.isoformat(),
                    exit_policy=exit_policy,
                ): lane
                for lane in loosened_lanes
            }
            for future in as_completed(future_map):
                lane = future_map[future]
                payload = future.result()
                completed += 1
                print(
                    f"[{completed}/{len(loosened_lanes)}] {payload['strategy_id']} "
                    f"symbol={payload['symbol']} trades={payload['trade_count']}",
                    flush=True,
                )
                normalized_result = payload.get("normalized_result")
                study_row = payload.get("study_row")
                if normalized_result is not None:
                    normalized_results.append(dict(normalized_result))
                if study_row is not None:
                    new_studies.append(dict(study_row))

    latest_manifest = _latest_manifest_path(historical_playback_dir)
    existing_studies = _load_manifest_study_rows(latest_manifest) if latest_manifest is not None else []
    merged_studies = _merge_study_rows(existing_studies=existing_studies, new_studies=new_studies)
    manifest_run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    manifest_path = retest._write_historical_playback_manifest(
        studies=merged_studies,
        run_stamp=manifest_run_stamp,
        historical_playback_dir=historical_playback_dir,
        shard_config=retest.RetestShardConfig(),
    )

    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "window": {
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": end_timestamp.isoformat(),
        },
        "study_suffix": study_suffix,
        "label_suffix": label_suffix,
        "exit_policy": exit_policy,
        "safe_mode": bool(safe_mode),
        "max_workers": int(resolved_max_workers),
        "target_configs": [str(path) for path in resolved_configs],
        "historical_playback_manifest": str(manifest_path),
        "latest_manifest_before_merge": str(latest_manifest) if latest_manifest is not None else None,
        "result_count": len(normalized_results),
        "results": normalized_results,
        "published_strategy_ids": [str(row.get("strategy_id") or "") for row in new_studies],
        "merged_study_count": len(merged_studies),
    }
    json_path = report_dir / "atp_loosened_history_v1.json"
    markdown_path = report_dir / "atp_loosened_history_v1.md"
    json_path.write_text(json.dumps(retest._json_ready(report_payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_report_markdown(report_payload), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
        "historical_playback_manifest_path": manifest_path,
    }


def run_atp_exit_policy_matrix(
    *,
    report_dir: Path = DEFAULT_EXIT_POLICY_MATRIX_REPORT_DIR,
    start_timestamp: datetime = DEFAULT_START_TIMESTAMP,
    end_timestamp: datetime = DEFAULT_END_TIMESTAMP,
    source_database_paths: Sequence[str | Path] | None = None,
    target_configs: Sequence[str | Path] | None = None,
    policies: Sequence[str] | None = None,
    safe_mode: bool = DEFAULT_SAFE_MODE,
    max_workers: int | None = None,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    resolved_configs = tuple(Path(path) for path in (target_configs or DEFAULT_TARGET_CONFIGS))
    resolved_policies = tuple(str(policy).strip() for policy in (policies or DEFAULT_EXIT_POLICY_MATRIX_POLICIES) if str(policy).strip())
    lanes = [
        _load_loosened_lane_from_config(
            config_path=config_path,
            study_suffix="",
            label_suffix="",
        )
        for config_path in resolved_configs
    ]
    jobs = [(lane, policy) for lane in lanes for policy in resolved_policies]
    resolved_max_workers = _resolve_research_max_workers(
        job_count=len(jobs),
        requested_max_workers=max_workers,
        safe_mode=safe_mode,
        unsafe_cap=4,
    )
    grouped_results: dict[str, dict[str, Any]] = {
        str(lane["strategy_id"]): {
            "strategy_id": str(lane["strategy_id"]),
            "display_name": str(lane["display_name"]),
            "symbol": str(lane["symbol"]),
            "config_path": str(lane["config_path"]),
            "policies": {},
        }
        for lane in lanes
    }
    resolved_source_paths = [str(path) for path in (source_database_paths or (DEFAULT_SOURCE_DB,))]
    completed = 0
    try:
        if resolved_max_workers == 1:
            for lane, policy in jobs:
                payload = _evaluate_exit_policy_worker(
                    lane=lane,
                    source_database_paths=resolved_source_paths,
                    start_timestamp_iso=start_timestamp.isoformat(),
                    end_timestamp_iso=end_timestamp.isoformat(),
                    exit_policy=policy,
                )
                completed += 1
                print(
                    f"[{completed}/{len(jobs)}] {payload['strategy_id']} "
                    f"policy={policy} trades={payload.get('trade_count', 0)}",
                    flush=True,
                )
                grouped_results[str(lane["strategy_id"])]["policies"][policy] = {
                    "metrics": payload.get("metrics"),
                    "trade_count": int(payload.get("trade_count") or 0),
                    "exit_mix": dict(payload.get("exit_mix") or {}),
                }
        else:
            with ProcessPoolExecutor(max_workers=resolved_max_workers) as executor:
                future_map = {
                    executor.submit(
                        _evaluate_exit_policy_worker,
                        lane=lane,
                        source_database_paths=resolved_source_paths,
                        start_timestamp_iso=start_timestamp.isoformat(),
                        end_timestamp_iso=end_timestamp.isoformat(),
                        exit_policy=policy,
                    ): (lane, policy)
                    for lane, policy in jobs
                }
                for future in as_completed(future_map):
                    lane, policy = future_map[future]
                    payload = future.result()
                    completed += 1
                    print(
                        f"[{completed}/{len(jobs)}] {payload['strategy_id']} "
                        f"policy={policy} trades={payload.get('trade_count', 0)}",
                        flush=True,
                    )
                    grouped_results[str(lane["strategy_id"])]["policies"][policy] = {
                        "metrics": payload.get("metrics"),
                        "trade_count": int(payload.get("trade_count") or 0),
                        "exit_mix": dict(payload.get("exit_mix") or {}),
                    }
    except PermissionError:
        for lane, policy in jobs:
            payload = _evaluate_exit_policy_worker(
                lane=lane,
                source_database_paths=resolved_source_paths,
                start_timestamp_iso=start_timestamp.isoformat(),
                end_timestamp_iso=end_timestamp.isoformat(),
                exit_policy=policy,
            )
            completed += 1
            print(
                f"[{completed}/{len(jobs)}] {payload['strategy_id']} "
                f"policy={policy} trades={payload.get('trade_count', 0)}",
                flush=True,
            )
            grouped_results[str(lane["strategy_id"])]["policies"][policy] = {
                "metrics": payload.get("metrics"),
                "trade_count": int(payload.get("trade_count") or 0),
                "exit_mix": dict(payload.get("exit_mix") or {}),
            }
    ordered_results = [grouped_results[key] for key in sorted(grouped_results)]
    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "window": {
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": end_timestamp.isoformat(),
        },
        "safe_mode": bool(safe_mode),
        "max_workers": int(resolved_max_workers),
        "policies": list(resolved_policies),
        "target_configs": [str(path) for path in resolved_configs],
        "results": ordered_results,
    }
    json_path = report_dir / "atp_exit_policy_matrix_v1.json"
    markdown_path = report_dir / "atp_exit_policy_matrix_v1.md"
    json_path.write_text(json.dumps(retest._json_ready(report_payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_exit_policy_matrix_markdown(report_payload), encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
    }


def _build_loosened_probationary_universe(
    *,
    target_configs: Sequence[str | Path],
    study_suffix: str,
    label_suffix: str,
) -> dict[str, Any]:
    probationary_lanes: list[dict[str, Any]] = []
    for config_path in target_configs:
        lane = _load_loosened_lane_from_config(
            config_path=Path(config_path),
            study_suffix=study_suffix,
            label_suffix=label_suffix,
        )
        probationary_lanes.append(lane)
    return {
        "atp_lanes": [],
        "atp_promotion_candidates": [],
        "approved_quant_lanes": [],
        "probationary_lanes": probationary_lanes,
        "expanded_universe": [
            {
                "symbol": str(row["symbol"]),
                "family": str(row.get("family") or row.get("strategy_family") or ""),
                "status": str(row["lane_status"]),
                "cohort": str(row.get("cohort") or row.get("research_cohort") or ""),
                "data_status": "pending",
            }
            for row in probationary_lanes
        ],
    }


def _load_loosened_lane_from_config(
    *,
    config_path: Path,
    study_suffix: str,
    label_suffix: str,
) -> dict[str, Any]:
    settings = load_settings_from_files([REPO_ROOT / "config" / "base.yaml", config_path])
    lane_rows = list(settings.probationary_paper_lane_specs)
    if len(lane_rows) != 1:
        raise ValueError(f"Expected exactly one probationary lane in {config_path}, found {len(lane_rows)}")
    raw_lane = dict(lane_rows[0])
    standalone_strategy_id = str(
        raw_lane.get("standalone_strategy_id")
        or retest._probationary_standalone_strategy_id(settings, raw_lane)
        or raw_lane.get("lane_id")
        or config_path.stem
    ).strip()
    symbol = str(raw_lane.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError(f"Lane config {config_path} is missing symbol")
    lane_status = "approved_probationary" if not bool(raw_lane.get("non_approved", True)) else "active_research_candidate"
    display_name = str(raw_lane.get("display_name") or raw_lane.get("lane_id") or standalone_strategy_id)
    strategy_id = f"{standalone_strategy_id}{study_suffix}"
    return {
        **raw_lane,
        "strategy_id": strategy_id,
        "study_id": strategy_id,
        "standalone_strategy_id": strategy_id,
        "config_path": str(config_path),
        "display_name": f"{display_name}{label_suffix}",
        "symbol": symbol,
        "lane_status": lane_status,
        "study_mode": "research_execution_mode",
        "family": str(raw_lane.get("strategy_family") or "active_trend_participation_engine"),
        "research_cohort": "ATP_EXPERIMENTAL_LOOSENED_V1",
        "cohort": "ATP_EXPERIMENTAL_LOOSENED_V1",
        "reference_lane": False,
        "lane_type": "probationary",
        "strategy_identity_root": raw_lane.get("strategy_identity_root") or "ATP_COMPANION_V1",
        "identity_variant": f"{str(raw_lane.get('identity_variant') or 'loosened_v1').strip()}",
    }


def _latest_manifest_path(playback_dir: Path) -> Path | None:
    manifests = sorted(playback_dir.glob("historical_playback_*.manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1] if manifests else None


def _load_manifest_study_rows(manifest_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for row in payload.get("symbols") or []:
        rows.append(
            {
                "strategy_id": _study_row_strategy_id(row),
                "symbol": row.get("symbol"),
                "label": row.get("label"),
                "study_mode": row.get("study_mode"),
                "execution_model": row.get("execution_model"),
                "summary_payload": row.get("summary_payload"),
                "strategy_study_json_path": row.get("strategy_study_json_path"),
                "strategy_study_markdown_path": row.get("strategy_study_markdown_path"),
            }
        )
    return rows


def _merge_study_rows(
    *,
    existing_studies: Sequence[dict[str, Any]],
    new_studies: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in existing_studies:
        merged[_study_row_strategy_id(row)] = dict(row)
    for row in new_studies:
        merged[_study_row_strategy_id(row)] = dict(row)
    return [merged[key] for key in sorted(merged)]


def _study_row_strategy_id(row: dict[str, Any]) -> str:
    for candidate in (
        row.get("strategy_id"),
        row.get("study_id"),
        (row.get("catalog_entry") or {}).get("strategy_id"),
        (row.get("catalog_entry") or {}).get("study_id"),
        (row.get("study_preview") or {}).get("primary_standalone_strategy_id"),
    ):
        if str(candidate or "").strip():
            return str(candidate).strip()
    study_json_path = str(row.get("strategy_study_json_path") or "").strip()
    if study_json_path:
        payload = json.loads(Path(study_json_path).read_text(encoding="utf-8"))
        meta = dict(payload.get("meta") or {})
        strategy_id = str(meta.get("standalone_strategy_id") or meta.get("strategy_id") or "").strip()
        if strategy_id:
            return strategy_id
    raise ValueError(f"Unable to determine strategy id for study row: {row}")


def _render_report_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Loosened History v1",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Window: `{payload['window']['start_timestamp']}` -> `{payload['window']['end_timestamp']}`",
        f"- Historical playback manifest: `{payload['historical_playback_manifest']}`",
        f"- Published strategy count: `{len(payload.get('published_strategy_ids') or [])}`",
        "",
        "## Published Strategies",
    ]
    for strategy_id in payload.get("published_strategy_ids") or []:
        lines.append(f"- `{strategy_id}`")
    lines.extend(["", "## Results"])
    for row in payload.get("results") or []:
        metrics = row.get("metrics") or {}
        lines.extend(
            [
                f"### {row.get('display_name')}",
                f"- Strategy ID: `{row.get('strategy_id')}`",
                f"- Status: `{row.get('status')}`",
                f"- Symbol: `{row.get('symbol')}`",
                f"- Trades: `{metrics.get('trade_count')}`",
                f"- Net P&L: `{metrics.get('net_pnl')}`",
                f"- Avg trade: `{metrics.get('average_trade')}`",
                f"- Profit factor: `{metrics.get('profit_factor')}`",
                f"- Max drawdown: `{metrics.get('max_drawdown')}`",
                f"- Recommendation: `{row.get('recommendation')}`",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def _render_exit_policy_matrix_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Exit Policy Matrix v1",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Window: `{payload['window']['start_timestamp']}` -> `{payload['window']['end_timestamp']}`",
        f"- Policies: {', '.join(f'`{policy}`' for policy in (payload.get('policies') or []))}",
        "",
        "## Results",
    ]
    for row in payload.get("results") or []:
        lines.extend(
            [
                f"### {row.get('display_name')}",
                f"- Strategy ID: `{row.get('strategy_id')}`",
                f"- Symbol: `{row.get('symbol')}`",
                f"- Config: `{row.get('config_path')}`",
            ]
        )
        for policy, policy_payload in sorted((row.get("policies") or {}).items()):
            metrics = dict(policy_payload.get("metrics") or {})
            exit_mix = dict(policy_payload.get("exit_mix") or {})
            top_reason = next(iter(exit_mix), None)
            lines.extend(
                [
                    f"#### {policy}",
                    f"- Trades: `{policy_payload.get('trade_count')}`",
                    f"- Net P&L: `{metrics.get('net_pnl')}`",
                    f"- Profit factor: `{metrics.get('profit_factor')}`",
                    f"- Max drawdown: `{metrics.get('max_drawdown')}`",
                    f"- Top exit: `{top_reason}`",
                ]
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-loosened-history-publish")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="Output report directory.")
    parser.add_argument("--historical-playback-dir", default=str(DEFAULT_HISTORICAL_PLAYBACK_DIR), help="Historical playback directory.")
    parser.add_argument("--start", default=DEFAULT_START_TIMESTAMP.isoformat(), help="Inclusive ISO timestamp.")
    parser.add_argument("--end", default=DEFAULT_END_TIMESTAMP.isoformat(), help="Inclusive ISO timestamp.")
    parser.add_argument(
        "--study-suffix",
        default=DEFAULT_STUDY_SUFFIX,
        help="Suffix appended to standalone strategy IDs for the published parallel studies.",
    )
    parser.add_argument(
        "--label-suffix",
        default=DEFAULT_LABEL_SUFFIX,
        help="Suffix appended to display labels for the published parallel studies.",
    )
    parser.add_argument(
        "--target-config",
        action="append",
        default=[],
        help="Explicit ATP config path to publish. May be supplied multiple times. Defaults to the built-in loosened set.",
    )
    parser.add_argument(
        "--source-db",
        action="append",
        default=[str(DEFAULT_SOURCE_DB)],
        help="SQLite source DB path. May be supplied multiple times.",
    )
    parser.add_argument(
        "--exit-policy",
        default=ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
        help="ATP replay exit policy for the published studies.",
    )
    parser.add_argument(
        "--matrix",
        action="store_true",
        help="Run an exit-policy comparison matrix instead of publishing studies.",
    )
    parser.add_argument(
        "--policy",
        action="append",
        default=[],
        help="Explicit exit policy to compare when --matrix is set. May be supplied multiple times.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.matrix:
        result = run_atp_exit_policy_matrix(
            report_dir=Path(args.report_dir),
            start_timestamp=datetime.fromisoformat(args.start),
            end_timestamp=datetime.fromisoformat(args.end),
            target_configs=[Path(path) for path in args.target_config] if args.target_config else None,
            source_database_paths=[Path(path) for path in args.source_db],
            policies=args.policy if args.policy else None,
        )
        print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
        return 0
    result = run_atp_loosened_history_publish(
        report_dir=Path(args.report_dir),
        historical_playback_dir=Path(args.historical_playback_dir),
        start_timestamp=datetime.fromisoformat(args.start),
        end_timestamp=datetime.fromisoformat(args.end),
        study_suffix=str(args.study_suffix),
        label_suffix=str(args.label_suffix),
        target_configs=[Path(path) for path in args.target_config] if args.target_config else None,
        source_database_paths=[Path(path) for path in args.source_db],
        exit_policy=str(args.exit_policy),
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
