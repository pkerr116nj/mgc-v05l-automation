"""Registry-driven rolling-entry retest and historical back-cast population."""

from __future__ import annotations

import json
import sqlite3
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import fmean
from typing import Any, Sequence
from uuid import uuid4

from ..config_models import EnvironmentMode, ExecutionTimeframeRole, load_settings_from_files
from ..domain.models import Bar
from ..execution.execution_engine import ExecutionEngine
from ..market_data.bar_builder import BarBuilder
from ..market_data.provider_config import load_market_data_providers_config
from ..market_data.provider_models import MarketDataUseCase
from ..market_data.replay_feed import ReplayFeed
from ..market_data.sqlite_playback import SQLiteHistoricalBarSource
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..research.bar_resampling import build_resampled_bars
from ..research.quant_futures import _FrameSeries, _align_timestamps, _build_feature_rows
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.phase2_continuation import ENTRY_ELIGIBLE, classify_entry_states
from ..research.trend_participation.phase3_timing import (
    ATP_TIMING_ACTIVATION_COMPLETED_5M,
    ATP_TIMING_ACTIVATION_ROLLING_5M,
    VWAP_CHASE_RISK,
    classify_timing_states,
    classify_vwap_price_quality,
)
from ..research.trend_participation.storage import (
    load_sqlite_bars,
    normalize_and_check_bars,
    resample_bars_from_1m,
    rolling_window_bars_from_1m,
)
from ..strategy.strategy_engine import StrategyEngine
from .paper_lane_analyst_pack import REQUIRED_CANDIDATE_SPECS
from .replay_reporting import build_session_lookup, build_trade_ledger
from .approved_quant_lanes.evaluator import evaluate_approved_lane, lane_rejection_reason
from .approved_quant_lanes.specs import approved_quant_lane_specs
from .replay_base_preservation import DEFAULT_REPORT_DIR as DEFAULT_REPLAY_PRESERVATION_REPORT_DIR
from .replay_base_preservation import preserve_replay_base
from .strategy_runtime_registry import (
    build_runtime_settings,
    build_standalone_strategy_definitions,
)
from .strategy_study import (
    build_strategy_study_catalog_entry,
    build_strategy_study_preview,
    compact_strategy_study_payload,
    write_strategy_study_json,
    write_strategy_study_markdown,
)

REPO_ROOT = Path.cwd()
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "strategy_universe_retest"
DEFAULT_HISTORICAL_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"
DEFAULT_CONFIG_PATHS = (REPO_ROOT / "config" / "base.yaml", REPO_ROOT / "config" / "replay.yaml")
PROBATIONARY_CONFIG_PATHS = (REPO_ROOT / "config" / "base.yaml", REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml")

APPROVED_QUANT_POINT_VALUES: dict[str, Decimal] = {
    "MGC": Decimal("10"),
    "GC": Decimal("100"),
    "PL": Decimal("50"),
    "HG": Decimal("25000"),
    "QC": Decimal("25000"),
    "CL": Decimal("1000"),
    "ES": Decimal("50"),
    "6E": Decimal("125000"),
    "6J": Decimal("12500000"),
}
RESEARCH_CONTRACT_POINT_VALUES: dict[str, Decimal] = {
    **APPROVED_QUANT_POINT_VALUES,
    "NG": Decimal("10000"),
    "6B": Decimal("62500"),
    "MBT": Decimal("0.1"),
}
PROBATIONARY_LONG_ONLY_FAMILIES = {
    "usLatePauseResumeLongTurn",
    "asiaEarlyNormalBreakoutRetestHoldTurn",
}
PROBATIONARY_SHORT_ONLY_FAMILIES = {"asiaEarlyPauseResumeShortTurn"}


@dataclass(frozen=True)
class SourceSelection:
    symbol: str
    timeframe: str
    data_source: str
    sqlite_path: Path
    row_count: int
    start_ts: str | None
    end_ts: str | None


def run_strategy_universe_retest(
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
    source_database_paths: Sequence[str | Path] | None = None,
    preserve_base: bool = True,
) -> dict[str, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    replay_preservation = (
        preserve_replay_base(report_dir=DEFAULT_REPLAY_PRESERVATION_REPORT_DIR)
        if preserve_base
        else None
    )

    bar_source_index = _discover_best_sources(
        symbols={
            "MGC",
            "GC",
            "PL",
            "HG",
            "QC",
            "CL",
            "ES",
            "6E",
            "6J",
            "6B",
            "NG",
            "MBT",
        },
        timeframes={"1m", "5m"},
        sqlite_paths=source_database_paths,
    )
    studies: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []

    studies.extend(
        _run_atp_retests(
            report_rows=report_rows,
            bar_source_index=bar_source_index,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    )
    studies.extend(
        _run_approved_quant_retests(
            report_rows=report_rows,
            bar_source_index=bar_source_index,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    )
    studies.extend(
        _run_probationary_family_retests(
            report_rows=report_rows,
            bar_source_index=bar_source_index,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    )
    normalized_results = [_normalize_result_row(row) for row in report_rows]

    run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    manifest_path = _write_historical_playback_manifest(
        studies=studies,
        run_stamp=run_stamp,
        historical_playback_dir=historical_playback_dir,
    )
    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "methodology": {
            "summary": (
                "ATP retests rebuild 5m context on a rolling 1m-updated basis, while approved quant lanes "
                "reuse their frozen rule families against rolling 5m context and 1m execution."
            ),
            "rolling_5m_interpretation": (
                "Each completed 1m candle is paired with the latest causal 5-minute lookback window ending on "
                "that minute. Structure and readiness stay 5m-defined, execution stays 1m, and chase-risk "
                "entries remain blocked by default."
            ),
            "data_limitation": (
                "1m history coverage is limited to the locally available SQLite windows discovered in this repo. "
                "Longer 5m context is stitched in from the deepest available 5m stores for the same symbols."
            ),
            "window": {
                "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
                "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
            },
            "previously_omitted_now_included": (
                "Older non-ATP probationary branch families and next-tier candidate equivalents from the paper-lane "
                "analyst pack are now included in the retest universe, with explicit data-limit flags where needed."
            ),
            "previously_omitted_now_included_families": sorted(
                {
                    str(candidate["branch"]).strip()
                    for candidate in REQUIRED_CANDIDATE_SPECS
                }
            ),
        },
        "expanded_universe": _expanded_universe_manifest(bar_source_index),
        "source_selection": {
            symbol: {
                timeframe: {
                    "data_source": selection.data_source,
                    "sqlite_path": str(selection.sqlite_path),
                    "row_count": selection.row_count,
                    "start_ts": selection.start_ts,
                    "end_ts": selection.end_ts,
                }
                for timeframe, selection in by_timeframe.items()
            }
            for symbol, by_timeframe in sorted(bar_source_index.items())
        },
        "results": normalized_results,
        "historical_playback_manifest": str(manifest_path),
        "replay_base_preservation": replay_preservation,
    }
    json_path = report_dir / "strategy_universe_retest.json"
    markdown_path = report_dir / "strategy_universe_retest.md"
    json_path.write_text(json.dumps(_json_ready(report_payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_report_markdown(report_payload).strip() + "\n", encoding="utf-8")
    return {
        "report_json_path": json_path,
        "report_markdown_path": markdown_path,
        "historical_playback_manifest_path": manifest_path,
    }


def _discover_best_sources(
    *,
    symbols: set[str],
    timeframes: set[str],
    sqlite_paths: Sequence[str | Path] | None = None,
) -> dict[str, dict[str, SourceSelection]]:
    selections: dict[str, dict[str, SourceSelection]] = defaultdict(dict)
    provider_config = load_market_data_providers_config()
    candidate_paths = (
        [Path(path).resolve() for path in sqlite_paths]
        if sqlite_paths is not None
        else sorted(REPO_ROOT.glob("*.sqlite3"))
    )
    for sqlite_path in candidate_paths:
        try:
            connection = sqlite3.connect(sqlite_path)
            rows = connection.execute(
                """
                select symbol, timeframe, data_source, count(*) as row_count, min(end_ts) as start_ts, max(end_ts) as end_ts
                from bars
                group by symbol, timeframe, data_source
                """
            ).fetchall()
            connection.close()
        except sqlite3.Error:
            continue
        for symbol, timeframe, data_source, row_count, start_ts, end_ts in rows:
            normalized_symbol = str(symbol or "").strip().upper()
            normalized_timeframe = str(timeframe or "").strip().lower()
            normalized_data_source = str(data_source or "").strip()
            if normalized_symbol not in symbols or normalized_timeframe not in timeframes:
                continue
            candidate = SourceSelection(
                symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                data_source=normalized_data_source,
                sqlite_path=sqlite_path.resolve(),
                row_count=int(row_count or 0),
                start_ts=str(start_ts) if start_ts else None,
                end_ts=str(end_ts) if end_ts else None,
            )
            current = selections.get(normalized_symbol, {}).get(normalized_timeframe)
            if current is None or _source_selection_key(candidate, provider_config) > _source_selection_key(current, provider_config):
                selections[normalized_symbol][normalized_timeframe] = candidate
    return {symbol: dict(by_timeframe) for symbol, by_timeframe in selections.items()}


def _source_selection_key(selection: SourceSelection, provider_config) -> tuple[int, int, str, str]:
    preferred_sources = list(
        provider_config.preferred_data_sources(MarketDataUseCase.HISTORICAL_RESEARCH, selection.timeframe)
    )
    try:
        precedence = len(preferred_sources) - preferred_sources.index(selection.data_source)
    except ValueError:
        precedence = 0
    return (
        precedence,
        selection.row_count,
        selection.end_ts or "",
        str(selection.sqlite_path),
    )


def _run_atp_retests(
    *,
    report_rows: list[dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    study_rows: list[dict[str, Any]] = []
    atp_lanes = [
        {
            "study_id": "atp_companion_v1__benchmark_mgc_asia_us",
            "symbol": "MGC",
            "display_name": "ATP Companion Baseline v1 / MGC / Asia+US",
            "lane_status": "approved",
            "study_mode": "baseline_parity_mode",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("10"),
            "candidate_id": None,
        },
        {
            "study_id": "atp_companion_v1__candidate_gc_asia_us",
            "symbol": "GC",
            "display_name": "ATP Companion Candidate v1 / GC / Asia+US",
            "lane_status": "active_research_candidate",
            "study_mode": "research_execution_mode",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("100"),
            "candidate_id": None,
        },
        {
            "study_id": "atp_companion_v1__candidate_pl_asia_us",
            "symbol": "PL",
            "display_name": "ATP Companion Candidate v1 / PL / Asia+US",
            "lane_status": "active_research_candidate",
            "study_mode": "research_execution_mode",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("50"),
            "candidate_id": None,
        },
    ]
    for lane in atp_lanes:
        result = _evaluate_atp_lane(
            symbol=str(lane["symbol"]),
            allowed_sessions=set(lane["allowed_sessions"]),
            point_value=Decimal(str(lane["point_value"])),
            bar_source_index=bar_source_index,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        if result is None:
            continue
        study_path_pair = _write_study_payload(
            payload=_build_synthetic_strategy_study(
                symbol=str(lane["symbol"]),
                study_id=str(lane["study_id"]),
                display_name=str(lane["display_name"]),
                strategy_family="active_trend_participation_engine",
                study_mode=str(lane["study_mode"]),
                bars_1m=result["bars_1m"],
                trade_rows=result["trade_rows"],
                point_value=Decimal(str(lane["point_value"])),
                candidate_id=None,
                entry_model="CURRENT_CANDLE_VWAP",
                pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
            ),
            artifact_prefix=f"historical_playback_{str(lane['study_id'])}",
        )
        study_rows.append(
            {
                "symbol": lane["symbol"],
                "label": lane["display_name"],
                "study_mode": lane["study_mode"],
                "summary_payload": _study_summary_payload(result["summary"], lane["study_status"] if "study_status" in lane else lane["lane_status"]),
                "strategy_study_json_path": str(study_path_pair["json"]),
                "strategy_study_markdown_path": str(study_path_pair["markdown"]),
            }
        )
        report_rows.append(
            {
                "strategy_id": lane["study_id"],
                "display_name": lane["display_name"],
                "status": lane["lane_status"],
                "family": "ATP benchmark/candidate",
                "symbol": lane["symbol"],
                "cohort": "ATP_CORE",
                "metrics": result["summary"],
                "prior_method_comparison": result["prior_summary"],
                "material_improvement": _material_improvement(result["summary"], result["prior_summary"]),
                "recommendation": _atp_recommendation(result["summary"], lane_status=str(lane["lane_status"])),
                "data_limit_status": _symbol_data_limit_status(symbol=str(lane["symbol"]), bar_source_index=bar_source_index),
            }
        )

        if lane["symbol"] != "MGC":
            continue
        for candidate_id in ("promotion_1_050r_neutral_plus", "promotion_1_075r_neutral_plus", "promotion_1_075r_favorable_only"):
            candidate = candidate_defs[candidate_id]
            candidate_rows = []
            for trade_row in result["trade_rows"]:
                window = [bar for bar in result["bars_1m"] if trade_row["entry_timestamp"] <= bar.end_ts.isoformat() <= trade_row["exit_timestamp"]]
                add_result = evaluate_promotion_add_candidate(
                    trade=trade_row["trade_record"],
                    minute_bars=window,
                    candidate=candidate,
                    point_value=float(lane["point_value"]),
                )
                candidate_rows.append(
                    {
                        "trade_id": str(add_result.get("decision_ts") or add_result.get("entry_ts") or len(candidate_rows)),
                        "entry_timestamp": trade_row["entry_timestamp"],
                        "exit_timestamp": trade_row["exit_timestamp"],
                        "entry_price": trade_row["entry_price"],
                        "exit_price": trade_row["exit_price"],
                        "side": trade_row["side"],
                        "family": trade_row["family"],
                        "entry_session_phase": trade_row["entry_session_phase"],
                        "exit_reason": trade_row["exit_reason"],
                        "realized_pnl": add_result["pnl_cash"],
                        "vwap_price_quality_state": add_result.get("add_price_quality_state") or trade_row["vwap_price_quality_state"],
                    }
                )
            candidate_summary = _summarize_trade_rows(candidate_rows, bar_count=len(result["bars_1m"]))
            study_id = f"atp_companion_v1__{candidate_id}"
            study_path_pair = _write_study_payload(
                payload=_build_synthetic_strategy_study(
                    symbol="MGC",
                    study_id=study_id,
                    display_name=f"ATP Companion / {candidate_id}",
                    strategy_family="active_trend_participation_engine",
                    study_mode="research_execution_mode",
                    bars_1m=result["bars_1m"],
                    trade_rows=candidate_rows,
                    point_value=Decimal("10"),
                    candidate_id=candidate_id,
                    entry_model="CURRENT_CANDLE_VWAP",
                    pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                    lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
                ),
                artifact_prefix=f"historical_playback_{study_id}",
            )
            study_rows.append(
                {
                    "symbol": "MGC",
                    "label": f"ATP Companion / {candidate_id}",
                    "study_mode": "research_execution_mode",
                    "summary_payload": _study_summary_payload(candidate_summary, "research_candidate"),
                    "strategy_study_json_path": str(study_path_pair["json"]),
                    "strategy_study_markdown_path": str(study_path_pair["markdown"]),
                }
            )
            report_rows.append(
                {
                    "strategy_id": study_id,
                    "display_name": f"ATP Companion / {candidate_id}",
                    "status": "active_research_candidate" if candidate_id == "promotion_1_075r_favorable_only" else "retained_candidate",
                    "family": "ATP promotion/add branch",
                    "symbol": "MGC",
                    "cohort": "ATP_PROMOTION_BRANCH",
                    "metrics": candidate_summary,
                    "prior_method_comparison": result["summary"],
                    "material_improvement": _material_improvement(candidate_summary, result["summary"]),
                    "recommendation": _atp_candidate_recommendation(candidate_id, candidate_summary, result["summary"]),
                    "data_limit_status": _symbol_data_limit_status(symbol="MGC", bar_source_index=bar_source_index),
                }
            )
    return study_rows


def _evaluate_atp_lane(
    *,
    symbol: str,
    allowed_sessions: set[str],
    point_value: Decimal,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    loaded = _load_symbol_context(
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if loaded is None:
        return None
    rolling_features = build_feature_states(bars_5m=loaded["combined_rolling_5m"], bars_1m=loaded["bars_1m"])
    rolling_ts = {bar.end_ts for bar in loaded["rolling_5m"]}
    rolling_entry_states = classify_entry_states(
        feature_rows=[row for row in rolling_features if row.decision_ts in rolling_ts],
        allowed_sessions=frozenset(allowed_sessions),
    )
    rolling_timing_states = classify_timing_states(
        entry_states=rolling_entry_states,
        bars_1m=loaded["bars_1m"],
        entry_activation_basis=ATP_TIMING_ACTIVATION_ROLLING_5M,
    )
    rolling_package = _rebuild_atp_trades(
        symbol=symbol,
        bars_1m=loaded["bars_1m"],
        timing_states=rolling_timing_states,
        point_value=float(point_value),
    )

    completed_features = build_feature_states(bars_5m=loaded["window_completed_5m"], bars_1m=loaded["bars_1m"])
    completed_entry_states = classify_entry_states(
        feature_rows=completed_features,
        allowed_sessions=frozenset(allowed_sessions),
    )
    completed_timing_states = classify_timing_states(
        entry_states=completed_entry_states,
        bars_1m=loaded["bars_1m"],
        entry_activation_basis=ATP_TIMING_ACTIVATION_COMPLETED_5M,
    )
    completed_package = _rebuild_atp_trades(
        symbol=symbol,
        bars_1m=loaded["bars_1m"],
        timing_states=completed_timing_states,
        point_value=float(point_value),
    )
    return {
        "bars_1m": loaded["bars_1m"],
        "trade_rows": rolling_package["trade_rows"],
        "summary": _summarize_trade_rows(rolling_package["trade_rows"], bar_count=len(loaded["bars_1m"])),
        "prior_summary": _summarize_trade_rows(completed_package["trade_rows"], bar_count=len(loaded["bars_1m"])),
    }


def _rebuild_atp_trades(
    *,
    symbol: str,
    bars_1m: list[ResearchBar],
    timing_states: Sequence[Any],
    point_value: float,
) -> dict[str, Any]:
    trade_rows: list[dict[str, Any]] = []
    bars_by_timestamp = {bar.end_ts.isoformat(): bar for bar in bars_1m}
    minute_ts = [bar.end_ts for bar in bars_1m]
    next_entry_index = 0
    for state in timing_states:
        if not state.executable_entry or state.entry_ts is None or state.entry_price is None:
            continue
        entry_index = bisect_left(minute_ts, state.entry_ts)
        if entry_index < next_entry_index or entry_index >= len(bars_1m):
            continue
        entry_bar = bars_1m[entry_index]
        average_range = max(float(state.feature_snapshot.get("average_range") or 0.25), 0.25)
        risk = max(average_range * 0.85, 0.25)
        stop_price = float(state.feature_snapshot.get("decision_bar_low") or state.entry_price) - risk
        target_price = float(state.entry_price) + (risk * 1.6)
        exit_index = min(entry_index + 24, len(bars_1m) - 1)
        exit_price = float(bars_1m[exit_index].close)
        exit_reason = "time_stop"
        for probe_index in range(entry_index, exit_index + 1):
            probe_bar = bars_1m[probe_index]
            if probe_bar.low <= stop_price and probe_bar.high >= target_price:
                exit_index = probe_index
                exit_price = stop_price
                exit_reason = "stop_first_conflict"
                break
            if probe_bar.low <= stop_price:
                exit_index = probe_index
                exit_price = stop_price
                exit_reason = "stop"
                break
            if probe_bar.high >= target_price:
                exit_index = probe_index
                exit_price = target_price
                exit_reason = "target"
                break
        pnl_points = exit_price - float(state.entry_price)
        trade_rows.append(
            {
                "trade_id": f"{symbol}|{state.decision_ts.isoformat()}",
                "entry_timestamp": entry_bar.end_ts.isoformat(),
                "exit_timestamp": bars_1m[exit_index].end_ts.isoformat(),
                "entry_price": round(float(state.entry_price), 6),
                "exit_price": round(exit_price, 6),
                "side": "LONG",
                "family": state.family_name,
                "entry_session_phase": state.session_segment,
                "exit_reason": exit_reason,
                "realized_pnl": round(pnl_points * point_value, 6),
                "vwap_price_quality_state": state.vwap_price_quality_state,
                "trade_record": type(
                    "TradeProxy",
                    (),
                    {
                        "entry_ts": entry_bar.end_ts,
                        "exit_ts": bars_1m[exit_index].end_ts,
                        "decision_ts": state.decision_ts,
                        "entry_price": float(state.entry_price),
                        "exit_price": exit_price,
                        "stop_price": stop_price,
                        "pnl_cash": pnl_points * point_value,
                        "hold_minutes": float(exit_index - entry_index + 1),
                        "bars_held_1m": exit_index - entry_index + 1,
                        "side": "LONG",
                        "session_segment": state.session_segment,
                        "mfe_points": max(float(bar.high) - float(state.entry_price) for bar in bars_1m[entry_index : exit_index + 1]),
                        "mae_points": max(float(state.entry_price) - float(bar.low) for bar in bars_1m[entry_index : exit_index + 1]),
                        "family": state.family_name,
                        "exit_reason": exit_reason,
                    },
                )(),
            }
        )
        next_entry_index = exit_index + 1
    return {"trade_rows": trade_rows}


def _run_approved_quant_retests(
    *,
    report_rows: list[dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    study_rows: list[dict[str, Any]] = []
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            result = _evaluate_approved_quant_lane_symbol(
                spec=spec,
                symbol=symbol,
                bar_source_index=bar_source_index,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            if result is None:
                continue
            study_id = f"{spec.lane_name}__{symbol}"
            study_path_pair = _write_study_payload(
                payload=_build_synthetic_strategy_study(
                    symbol=symbol,
                    study_id=study_id,
                    display_name=f"{spec.lane_name} / {symbol}",
                    strategy_family=spec.family,
                    study_mode="baseline_parity_mode",
                    bars_1m=result["bars_1m"],
                    trade_rows=result["trade_rows"],
                    point_value=APPROVED_QUANT_POINT_VALUES.get(symbol, Decimal("1")),
                    candidate_id=None,
                    entry_model="CURRENT_CANDLE_VWAP",
                    pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                    lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
                ),
                artifact_prefix=f"historical_playback_{study_id}",
            )
            study_rows.append(
                {
                    "symbol": symbol,
                    "label": f"{spec.lane_name} / {symbol}",
                    "study_mode": "baseline_parity_mode",
                    "summary_payload": _study_summary_payload(result["summary"], "approved"),
                    "strategy_study_json_path": str(study_path_pair["json"]),
                    "strategy_study_markdown_path": str(study_path_pair["markdown"]),
                }
            )
            report_rows.append(
                {
                    "strategy_id": study_id,
                    "display_name": f"{spec.lane_name} / {symbol}",
                    "status": "approved",
                    "family": spec.family,
                    "symbol": symbol,
                    "cohort": "APPROVED_QUANT",
                    "metrics": result["summary"],
                    "prior_method_comparison": result["prior_summary"],
                    "material_improvement": _material_improvement(result["summary"], result["prior_summary"]),
                    "recommendation": _approved_quant_recommendation(result["summary"]),
                    "data_limit_status": _symbol_data_limit_status(symbol=symbol, bar_source_index=bar_source_index),
                }
            )
    return study_rows


def _evaluate_approved_quant_lane_symbol(
    *,
    spec,
    symbol: str,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    loaded = _load_symbol_context(
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if loaded is None:
        return None
    settings = load_settings_from_files(DEFAULT_CONFIG_PATHS)
    bar_builder = BarBuilder(settings)
    completed_context_bars = [_research_bar_to_domain_bar(bar) for bar in loaded["completed_5m_history"]]
    higher = {
        timeframe: _FrameSeries.from_bars(build_resampled_bars(completed_context_bars, target_timeframe=timeframe, bar_builder=bar_builder).bars)
        for timeframe in ("60m", "240m", "720m", "1440m")
    }
    execution = _FrameSeries.from_bars([_research_bar_to_domain_bar(bar) for bar in loaded["combined_rolling_5m"]])
    alignments = {timeframe: _align_timestamps(execution.timestamps, frame.timestamps) for timeframe, frame in higher.items()}
    features = _build_feature_rows(execution=execution, higher=higher, alignments=alignments)
    rolling_ts = {bar.end_ts for bar in loaded["rolling_5m"]}
    minute_ts = [bar.end_ts for bar in loaded["bars_1m"]]
    point_value = float(APPROVED_QUANT_POINT_VALUES.get(symbol, Decimal("1")))

    trade_rows: list[dict[str, Any]] = []
    next_available_index = 0
    for bar_5m, feature in zip(loaded["combined_rolling_5m"], features, strict=True):
        if bar_5m.end_ts not in rolling_ts or not feature.get("ready"):
            continue
        minute_index = bisect_left(minute_ts, bar_5m.end_ts)
        if minute_index >= len(loaded["bars_1m"]) or loaded["bars_1m"][minute_index].end_ts != bar_5m.end_ts:
            continue
        if minute_index < next_available_index:
            continue
        rejection = lane_rejection_reason(spec=spec, session_label=str(feature["session_label"]), feature=feature)
        if rejection is not None:
            continue
        minute_bar = loaded["bars_1m"][minute_index]
        entry_price = float(minute_bar.close)
        entry_quality = classify_vwap_price_quality(
            side=spec.direction,
            entry_price=entry_price,
            bar_vwap=(minute_bar.high + minute_bar.low + minute_bar.close) / 3.0,
            band_reference=max(minute_bar.range_points, float(feature["risk_unit"]), 1e-9),
        )
        if entry_quality == VWAP_CHASE_RISK:
            continue
        risk = max(float(feature["risk_unit"]), 1e-6)
        stop_price = entry_price - spec.stop_r * risk if spec.direction == "LONG" else entry_price + spec.stop_r * risk
        target_price = (
            entry_price + spec.target_r * risk
            if spec.target_r is not None and spec.direction == "LONG"
            else entry_price - spec.target_r * risk
            if spec.target_r is not None
            else None
        )
        last_index = min(minute_index + (spec.hold_bars * 5) - 1, len(loaded["bars_1m"]) - 1)
        exit_index = last_index
        exit_price = float(loaded["bars_1m"][last_index].close)
        exit_reason = "time_exit"
        for probe_index in range(minute_index, last_index + 1):
            probe_bar = loaded["bars_1m"][probe_index]
            high = float(probe_bar.high)
            low = float(probe_bar.low)
            close = float(probe_bar.close)
            if spec.direction == "LONG":
                stop_hit = low <= stop_price
                target_hit = target_price is not None and high >= target_price
                structural_invalidation = False
            else:
                stop_hit = high >= stop_price
                target_hit = target_price is not None and low <= target_price
                structural_invalidation = (
                    spec.structural_invalidation_r is not None and close >= entry_price + spec.structural_invalidation_r * risk
                )
            if stop_hit and target_hit:
                exit_index = probe_index
                exit_price = stop_price
                exit_reason = "stop_first_conflict"
                break
            if stop_hit:
                exit_index = probe_index
                exit_price = stop_price
                exit_reason = "stop"
                break
            if target_hit and target_price is not None:
                exit_index = probe_index
                exit_price = target_price
                exit_reason = "target"
                break
            if structural_invalidation:
                exit_index = probe_index
                exit_price = close
                exit_reason = "structural_invalidation"
                break
        realized_pnl = (
            (exit_price - entry_price) * point_value
            if spec.direction == "LONG"
            else (entry_price - exit_price) * point_value
        )
        trade_rows.append(
            {
                "trade_id": f"{spec.lane_id}|{symbol}|{bar_5m.end_ts.isoformat()}",
                "entry_timestamp": minute_bar.end_ts.isoformat(),
                "exit_timestamp": loaded["bars_1m"][exit_index].end_ts.isoformat(),
                "entry_price": round(entry_price, 6),
                "exit_price": round(exit_price, 6),
                "side": spec.direction,
                "family": spec.family,
                "entry_session_phase": str(feature["session_label"]),
                "exit_reason": exit_reason,
                "realized_pnl": round(realized_pnl, 6),
                "vwap_price_quality_state": entry_quality,
            }
        )
        next_available_index = exit_index + 1

    completed_execution = _FrameSeries.from_bars([_research_bar_to_domain_bar(bar) for bar in loaded["window_completed_5m"]])
    completed_alignments = {
        timeframe: _align_timestamps(completed_execution.timestamps, frame.timestamps)
        for timeframe, frame in higher.items()
    }
    completed_features = _build_feature_rows(execution=completed_execution, higher=higher, alignments=completed_alignments)
    prior = evaluate_approved_lane(
        spec=spec,
        symbol_store={symbol: {"execution": completed_execution, "features": completed_features}},
    )
    prior_trade_rows = [
        {
            "trade_id": row.get("signal_timestamp") or row.get("entry_timestamp"),
            "entry_timestamp": row["entry_timestamp"],
            "exit_timestamp": row["exit_timestamp"],
            "entry_price": row["entry_price"],
            "exit_price": row["exit_price"],
            "side": row["direction"],
            "family": spec.family,
            "entry_session_phase": row["session_label"],
            "exit_reason": row["exit_reason"],
            "realized_pnl": round(float(row["gross_r"]) * float(APPROVED_QUANT_POINT_VALUES.get(symbol, Decimal("1"))), 6),
            "vwap_price_quality_state": None,
        }
        for row in prior["trades"]
        if row["symbol"] == symbol
    ]
    return {
        "bars_1m": loaded["bars_1m"],
        "trade_rows": trade_rows,
        "summary": _summarize_trade_rows(trade_rows, bar_count=len(loaded["bars_1m"])),
        "prior_summary": _summarize_trade_rows(prior_trade_rows, bar_count=max(len(loaded["window_completed_5m"]), 1)),
    }


def _run_probationary_family_retests(
    *,
    report_rows: list[dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    study_rows: list[dict[str, Any]] = []
    settings = load_settings_from_files(PROBATIONARY_CONFIG_PATHS)
    explicit_lane_rows = list(settings.probationary_paper_lane_specs)
    admitted_pairs = {
        (str(raw.get("symbol") or "").strip().upper(), _resolve_probationary_branch(raw))
        for raw in explicit_lane_rows
    }
    expanded_rows = [
        {
            **raw,
            "lane_status": "approved_probationary",
            "research_cohort": "ADMITTED_COMPARATOR",
            "display_name": raw.get("display_name") or f"{raw.get('symbol')} / {_resolve_probationary_branch(raw)}",
        }
        for raw in explicit_lane_rows
    ]
    for candidate in REQUIRED_CANDIDATE_SPECS:
        symbol = str(candidate["instrument"]).strip().upper()
        branch = str(candidate["branch"]).strip()
        if (symbol, branch) in admitted_pairs:
            continue
        expanded_rows.append(
            {
                "lane_id": f"{symbol.lower()}_{branch}",
                "display_name": f"{symbol} / {branch}",
                "symbol": symbol,
                "strategy_family": branch,
                "lane_status": "retained_candidate",
                "research_cohort": str(candidate["cohort"]),
                "session_restriction": _probationary_session_restriction(branch),
                "long_sources": [branch] if branch in PROBATIONARY_LONG_ONLY_FAMILIES else [],
                "short_sources": [branch] if branch in PROBATIONARY_SHORT_ONLY_FAMILIES else [],
                "point_value": str(RESEARCH_CONTRACT_POINT_VALUES.get(symbol, Decimal("1"))),
            }
        )

    for lane in expanded_rows:
        symbol = str(lane.get("symbol") or "").strip().upper()
        family = _resolve_probationary_branch(lane)
        display_name = str(lane.get("display_name") or f"{symbol} / {family}")
        cohort = str(lane.get("research_cohort") or "UNSPECIFIED")
        lane_status = str(lane.get("lane_status") or "retained_candidate")
        current = _run_probationary_lane_playback(
            lane=lane,
            base_settings=settings,
            bar_source_index=bar_source_index,
            source_timeframe="1m",
            target_timeframe="1m",
            environment_mode=EnvironmentMode.RESEARCH_EXECUTION,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        prior = _run_probationary_lane_playback(
            lane=lane,
            base_settings=settings,
            bar_source_index=bar_source_index,
            source_timeframe="5m",
            target_timeframe="5m",
            environment_mode=EnvironmentMode.BASELINE_PARITY,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        data_limit_status = _probationary_data_limit_status(
            symbol=symbol,
            bar_source_index=bar_source_index,
            current=current,
        )
        metrics = _empty_summary() if current is None else _summarize_trade_rows(current["trade_rows"], bar_count=current["bar_count"])
        if current is not None:
            metrics["vwap_breakdown"] = current["vwap_breakdown"]
        prior_summary = _empty_summary() if prior is None else _summarize_trade_rows(prior["trade_rows"], bar_count=prior["bar_count"])
        report_rows.append(
            {
                "strategy_id": str(current["standalone_strategy_id"]) if current is not None else f"{symbol}__{family}",
                "display_name": display_name,
                "status": lane_status,
                "family": family,
                "symbol": symbol,
                "cohort": cohort,
                "metrics": metrics,
                "prior_method_comparison": prior_summary,
                "material_improvement": _material_improvement(metrics, prior_summary),
                "recommendation": _probationary_bucket(
                    current=metrics,
                    prior=prior_summary,
                    lane_status=lane_status,
                    data_limit_status=data_limit_status,
                ),
                "data_limit_status": data_limit_status,
                "methodology_note": (
                    "1m execution / 5m context timing is applied, but shared CURRENT_CANDLE_VWAP execution truth "
                    "is not implemented for this family yet."
                ),
            }
        )
        if current is None:
            continue
        if current.get("study_json_path") and current.get("study_markdown_path"):
            study_rows.append(
                {
                    "symbol": symbol,
                    "label": display_name,
                    "study_mode": "research_execution_mode",
                    "summary_payload": _study_summary_payload(metrics, lane_status),
                    "strategy_study_json_path": str(current["study_json_path"]),
                    "strategy_study_markdown_path": str(current["study_markdown_path"]),
                }
            )
    return study_rows


def _run_probationary_lane_playback(
    *,
    lane: dict[str, Any],
    base_settings,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    source_timeframe: str,
    target_timeframe: str,
    environment_mode: EnvironmentMode,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    symbol = str(lane.get("symbol") or "").strip().upper()
    source_selection = bar_source_index.get(symbol, {}).get(source_timeframe)
    if source_selection is None:
        return None
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=[lane])
    if not definitions:
        return None
    definition = definitions[0]
    runtime_settings = build_runtime_settings(base_settings, definition).model_copy(
        update={
            "environment_mode": environment_mode,
            "timeframe": target_timeframe,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": target_timeframe,
            "artifact_timeframe": target_timeframe,
            "context_timeframes": ("5m",),
            "execution_timeframe_role": (
                ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY
                if target_timeframe == "1m"
                else ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION
            ),
            "database_url": (
                f"sqlite:///{(DEFAULT_HISTORICAL_PLAYBACK_DIR / f'tmp_retest__{definition.standalone_strategy_id}__{target_timeframe}__{uuid4().hex}.sqlite3').resolve()}"
            ),
        }
    )
    loaded = SQLiteHistoricalBarSource(source_selection.sqlite_path, runtime_settings).load_bars(
        symbol=symbol,
        source_timeframe=source_timeframe,
        target_timeframe=target_timeframe,
        data_source=None,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    repositories = RepositorySet(build_engine(runtime_settings.database_url), runtime_identity=definition.runtime_identity)
    repositories.bars.save = lambda *args, **kwargs: None
    repositories.features.save = lambda *args, **kwargs: None
    repositories.signals.save = lambda *args, **kwargs: None
    repositories.processed_bars.mark_processed = lambda *args, **kwargs: None
    repositories.alerts.save = lambda *args, **kwargs: None
    repositories.fault_events.save = lambda *args, **kwargs: None
    strategy_engine = StrategyEngine(
        settings=runtime_settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(),
        runtime_identity=definition.runtime_identity,
    )
    if strategy_engine._state_repository is not None:
        strategy_engine._state_repository.save_snapshot = lambda *args, **kwargs: None
    for bar in loaded.playback_bars:
        strategy_engine.process_bar(bar)
    order_intent_rows = repositories.order_intents.list_all()
    fill_rows = repositories.fills.list_all()
    bars = list(loaded.playback_bars)
    ledger = build_trade_ledger(
        order_intent_rows,
        fill_rows,
        build_session_lookup(bars),
        point_value=definition.point_value,
        bars=bars,
    )
    trade_rows = _ledger_rows_to_trade_rows(ledger=ledger, bars=bars)
    result: dict[str, Any] = {
        "standalone_strategy_id": definition.standalone_strategy_id,
        "trade_rows": trade_rows,
        "bar_count": len(bars),
        "vwap_breakdown": _generic_vwap_breakdown(trade_rows=trade_rows, source_bars=bars),
    }
    if target_timeframe == "1m":
        study_payload = _build_synthetic_strategy_study(
            symbol=symbol,
            study_id=definition.standalone_strategy_id,
            display_name=str(lane.get("display_name") or definition.standalone_strategy_id),
            strategy_family=definition.strategy_family,
            study_mode="research_execution_mode",
            bars_1m=bars,
            trade_rows=trade_rows,
            point_value=definition.point_value,
            candidate_id=str(lane.get("lane_id") or "") or None,
            entry_model="REPLAY_NEXT_BAR_OPEN",
            pnl_truth_basis="PERSISTED_RUNTIME_TRUTH",
            lifecycle_truth_class="REPLAY_FILL_NEXT_BAR_OPEN",
            intrabar_execution_authoritative=False,
            authoritative_intrabar_available=False,
        )
        study_json_path = DEFAULT_HISTORICAL_PLAYBACK_DIR / f"historical_playback_{definition.standalone_strategy_id}.strategy_study.json"
        study_markdown_path = DEFAULT_HISTORICAL_PLAYBACK_DIR / f"historical_playback_{definition.standalone_strategy_id}.strategy_study.md"
        write_strategy_study_json(study_payload, study_json_path)
        write_strategy_study_markdown(study_payload, study_markdown_path)
        result["study_json_path"] = study_json_path
        result["study_markdown_path"] = study_markdown_path
    return result


def _ledger_rows_to_trade_rows(*, ledger: Sequence[Any], bars: Sequence[Bar]) -> list[dict[str, Any]]:
    bars_by_start_ts = {bar.start_ts.isoformat(): bar for bar in bars}
    rows: list[dict[str, Any]] = []
    for row in ledger:
        entry_bar = bars_by_start_ts.get(row.entry_ts.isoformat())
        exit_bar = bars_by_start_ts.get(row.exit_ts.isoformat())
        rows.append(
            {
                "trade_id": str(row.trade_id),
                "entry_timestamp": (entry_bar.end_ts if entry_bar is not None else row.entry_ts).isoformat(),
                "exit_timestamp": (exit_bar.end_ts if exit_bar is not None else row.exit_ts).isoformat(),
                "entry_price": round(float(row.entry_px), 6),
                "exit_price": round(float(row.exit_px), 6),
                "side": row.direction,
                "family": row.setup_family,
                "entry_session_phase": row.entry_session_phase,
                "exit_reason": row.exit_reason,
                "realized_pnl": round(float(row.net_pnl), 6),
            }
        )
    return rows


def _generic_vwap_breakdown(*, trade_rows: Sequence[dict[str, Any]], source_bars: Sequence[Bar]) -> dict[str, int]:
    bars_by_ts = {
        timestamp: bar
        for bar in source_bars
        for timestamp in (bar.start_ts.isoformat(), bar.end_ts.isoformat())
    }
    counts: Counter[str] = Counter()
    for trade in trade_rows:
        entry_ts = str(trade.get("entry_timestamp") or "")
        bar = bars_by_ts.get(entry_ts)
        if bar is None:
            continue
        quality = classify_vwap_price_quality(
            side=str(trade.get("side") or "LONG"),
            entry_price=float(trade.get("entry_price") or 0.0),
            bar_vwap=float((bar.high + bar.low + bar.close) / Decimal("3")),
            band_reference=max(float(bar.high - bar.low), 1e-9),
        )
        counts[quality] += 1
    return dict(counts)


def _resolve_probationary_branch(raw: dict[str, Any]) -> str:
    for key in ("strategy_family", "family", "source_family"):
        value = str(raw.get(key) or "").strip()
        if value:
            return value
    for key in ("long_sources", "short_sources"):
        values = [str(item) for item in list(raw.get(key) or []) if str(item).strip()]
        if values:
            return values[0]
    return ""


def _probationary_session_restriction(branch: str) -> str:
    if branch == "usLatePauseResumeLongTurn":
        return "US_LATE"
    if branch in {"asiaEarlyNormalBreakoutRetestHoldTurn", "asiaEarlyPauseResumeShortTurn"}:
        return "ASIA_EARLY"
    return ""


def _probationary_data_limit_status(
    *,
    symbol: str,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    current: dict[str, Any] | None,
) -> str:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return "no_local_1m_history"
    if completed_source is None:
        return "derived_5m_from_1m"
    if minute_source.row_count < 1500:
        return "thin_local_1m_window"
    if current is None:
        return "runtime_or_methodology_limited"
    return "none"


def _expanded_universe_manifest(bar_source_index: dict[str, dict[str, SourceSelection]]) -> list[dict[str, str]]:
    settings = load_settings_from_files(PROBATIONARY_CONFIG_PATHS)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    rows.extend(
        [
            {
                "symbol": "MGC",
                "family": "active_trend_participation_engine",
                "status": "approved",
                "cohort": "ATP_CORE",
                "data_status": _symbol_data_limit_status(symbol="MGC", bar_source_index=bar_source_index),
            },
            {
                "symbol": "GC",
                "family": "active_trend_participation_engine",
                "status": "active_research_candidate",
                "cohort": "ATP_CORE",
                "data_status": _symbol_data_limit_status(symbol="GC", bar_source_index=bar_source_index),
            },
            {
                "symbol": "PL",
                "family": "active_trend_participation_engine",
                "status": "active_research_candidate",
                "cohort": "ATP_CORE",
                "data_status": _symbol_data_limit_status(symbol="PL", bar_source_index=bar_source_index),
            },
            {
                "symbol": "MGC",
                "family": "promotion_1_050r_neutral_plus",
                "status": "retained_candidate",
                "cohort": "ATP_PROMOTION_BRANCH",
                "data_status": _symbol_data_limit_status(symbol="MGC", bar_source_index=bar_source_index),
            },
            {
                "symbol": "MGC",
                "family": "promotion_1_075r_neutral_plus",
                "status": "retained_candidate",
                "cohort": "ATP_PROMOTION_BRANCH",
                "data_status": _symbol_data_limit_status(symbol="MGC", bar_source_index=bar_source_index),
            },
            {
                "symbol": "MGC",
                "family": "promotion_1_075r_favorable_only",
                "status": "active_research_candidate",
                "cohort": "ATP_PROMOTION_BRANCH",
                "data_status": _symbol_data_limit_status(symbol="MGC", bar_source_index=bar_source_index),
            },
        ]
    )
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            rows.append(
                {
                    "symbol": symbol,
                    "family": spec.family,
                    "status": "approved",
                    "cohort": "APPROVED_QUANT",
                    "data_status": _symbol_data_limit_status(symbol=symbol, bar_source_index=bar_source_index),
                }
            )
    for raw in list(settings.probationary_paper_lane_specs):
        symbol = str(raw.get("symbol") or "").strip().upper()
        family = _resolve_probationary_branch(raw)
        seen.add((symbol, family))
        rows.append(
            {
                "symbol": symbol,
                "family": family,
                "status": "approved_probationary",
                "cohort": "ADMITTED_COMPARATOR",
                "data_status": _probationary_data_limit_status(symbol=symbol, bar_source_index=bar_source_index, current={}),
            }
        )
    for row in REQUIRED_CANDIDATE_SPECS:
        symbol = str(row["instrument"]).strip().upper()
        family = str(row["branch"]).strip()
        if (symbol, family) in seen:
            continue
        rows.append(
            {
                "symbol": symbol,
                "family": family,
                "status": "retained_candidate",
                "cohort": str(row["cohort"]),
                "data_status": _probationary_data_limit_status(symbol=symbol, bar_source_index=bar_source_index, current={}),
            }
        )
    return sorted(rows, key=lambda item: (item["cohort"], item["symbol"], item["family"]))


def _symbol_data_limit_status(*, symbol: str, bar_source_index: dict[str, dict[str, SourceSelection]]) -> str:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return "no_local_1m_history"
    if completed_source is None:
        return "derived_5m_from_1m"
    if minute_source.row_count < 1500:
        return "thin_local_1m_window"
    return "none"


def _load_symbol_context(
    *,
    symbol: str,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return None
    bars_1m, _ = normalize_and_check_bars(
        bars=load_sqlite_bars(
            sqlite_path=minute_source.sqlite_path,
            instrument=symbol,
            timeframe="1m",
            data_source=minute_source.data_source,
            start_ts=start_timestamp,
            end_ts=end_timestamp,
        ),
        timeframe="1m",
    )
    native_completed_5m_history: list[ResearchBar] = []
    if completed_source is not None:
        native_completed_5m_history, _ = normalize_and_check_bars(
            bars=load_sqlite_bars(
                sqlite_path=completed_source.sqlite_path,
                instrument=symbol,
                timeframe="5m",
                data_source=completed_source.data_source,
                start_ts=start_timestamp,
                end_ts=end_timestamp,
            ),
            timeframe="5m",
        )
    derived_completed_5m_history = resample_bars_from_1m(bars_1m=bars_1m, target_timeframe="5m")
    completed_5m_history = (
        derived_completed_5m_history
        if len(derived_completed_5m_history) >= len(native_completed_5m_history)
        else native_completed_5m_history
    )
    rolling_5m = rolling_window_bars_from_1m(bars_1m=bars_1m)
    if not bars_1m or not completed_5m_history or not rolling_5m:
        return None
    first_rolling_ts = rolling_5m[0].end_ts
    last_minute_ts = bars_1m[-1].end_ts
    combined_rolling_5m = [bar for bar in completed_5m_history if bar.end_ts < first_rolling_ts] + rolling_5m
    window_completed_5m = [bar for bar in completed_5m_history if bars_1m[0].end_ts <= bar.end_ts <= last_minute_ts]
    return {
        "bars_1m": bars_1m,
        "completed_5m_history": completed_5m_history,
        "rolling_5m": rolling_5m,
        "combined_rolling_5m": combined_rolling_5m,
        "window_completed_5m": window_completed_5m,
    }


def _study_summary_payload(summary: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "status": status,
        "trade_count": summary["trade_count"],
        "net_pnl": summary["net_pnl"],
        "profit_factor": summary["profit_factor"],
        "win_rate": summary["win_rate"],
    }


def _build_synthetic_strategy_study(
    *,
    symbol: str,
    study_id: str,
    display_name: str,
    strategy_family: str,
    study_mode: str,
    bars_1m: Sequence[Any],
    trade_rows: Sequence[dict[str, Any]],
    point_value: Decimal,
    candidate_id: str | None,
    entry_model: str,
    pnl_truth_basis: str,
    lifecycle_truth_class: str,
    intrabar_execution_authoritative: bool = True,
    authoritative_intrabar_available: bool = True,
) -> dict[str, Any]:
    entries_by_ts = {str(row["entry_timestamp"]): row for row in trade_rows}
    exits_by_ts = {str(row["exit_timestamp"]): row for row in trade_rows}
    cumulative_realized = Decimal("0")
    rows: list[dict[str, Any]] = []
    pnl_points: list[dict[str, Any]] = []
    for bar in bars_1m:
        exit_row = exits_by_ts.get(bar.end_ts.isoformat())
        if exit_row is not None:
            cumulative_realized += Decimal(str(exit_row["realized_pnl"]))
        rows.append(
            {
                "bar_id": f"{symbol}|1m|{bar.end_ts.isoformat()}",
                "timestamp": bar.end_ts.isoformat(),
                "start_timestamp": bar.start_ts.isoformat(),
                "end_timestamp": bar.end_ts.isoformat(),
                "open": str(bar.open),
                "high": str(bar.high),
                "low": str(bar.low),
                "close": str(bar.close),
                "session_vwap": str(
                    (
                        Decimal(str(bar.high))
                        + Decimal(str(bar.low))
                        + Decimal(str(bar.close))
                    )
                    / Decimal("3")
                ),
                "entry_marker": bar.end_ts.isoformat() in entries_by_ts,
                "exit_marker": bar.end_ts.isoformat() in exits_by_ts,
                "fill_marker": bar.end_ts.isoformat() in entries_by_ts or bar.end_ts.isoformat() in exits_by_ts,
                "cumulative_realized_pnl": str(cumulative_realized),
                "cumulative_total_pnl": str(cumulative_realized),
            }
        )
        pnl_points.append(
            {
                "point_id": f"{study_id}|{bar.end_ts.isoformat()}",
                "bar_id": f"{symbol}|1m|{bar.end_ts.isoformat()}",
                "timestamp": bar.end_ts.isoformat(),
                "cumulative_realized": str(cumulative_realized),
                "unrealized_pnl": "0",
                "cumulative_total": str(cumulative_realized),
            }
        )
    summary = _summarize_trade_rows(list(trade_rows), bar_count=len(bars_1m))
    trade_events = []
    for row in trade_rows:
        trade_events.append(
            {
                "event_id": f"{row['trade_id']}|ENTRY",
                "linked_bar_id": f"{symbol}|1m|{row['entry_timestamp']}",
                "event_type": "ENTRY_FILL",
                "execution_event_type": "FILL",
                "side": row["side"],
                "family": row["family"],
                "event_timestamp": row["entry_timestamp"],
                "event_price": row["entry_price"],
                "entry_model": entry_model,
                "truth_authority": "RESEARCH_BACKCAST",
            }
        )
        trade_events.append(
            {
                "event_id": f"{row['trade_id']}|EXIT",
                "linked_bar_id": f"{symbol}|1m|{row['exit_timestamp']}",
                "event_type": "EXIT_FILL",
                "execution_event_type": "FILL",
                "side": row["side"],
                "family": row["family"],
                "event_timestamp": row["exit_timestamp"],
                "event_price": row["exit_price"],
                "reason": row["exit_reason"],
                "entry_model": entry_model,
                "truth_authority": "RESEARCH_BACKCAST",
            }
        )
    return {
        "contract_version": "strategy_study_v3",
        "generated_at": datetime.now(UTC).isoformat(),
        "symbol": symbol,
        "timeframe": "1m",
        "standalone_strategy_id": study_id,
        "strategy_family": strategy_family,
        "point_value": str(point_value),
        "bars": rows,
        "pnl_points": pnl_points,
        "trade_events": trade_events,
        "execution_slices": [
            {
                "slice_id": f"{study_id}|slice|{bar.end_ts.isoformat()}",
                "linked_bar_id": f"{symbol}|1m|{bar.end_ts.isoformat()}",
                "timestamp": bar.end_ts.isoformat(),
                "start_timestamp": bar.start_ts.isoformat(),
                "end_timestamp": bar.end_ts.isoformat(),
                "close": str(bar.close),
                "high": str(bar.high),
                "low": str(bar.low),
            }
            for bar in bars_1m
        ],
        "summary": {
            "bar_count": len(bars_1m),
            "total_trades": summary["trade_count"],
            "long_trades": summary["long_trades"],
            "short_trades": summary["short_trades"],
            "winners": summary["winners"],
            "losers": summary["losers"],
            "profit_factor": summary["profit_factor"],
            "cumulative_realized_pnl": summary["net_pnl"],
            "cumulative_total_pnl": summary["net_pnl"],
            "max_drawdown": summary["max_drawdown"],
            "closed_trade_breakdown": [
                {
                    "trade_id": row["trade_id"],
                    "family": row["family"],
                    "side": row["side"],
                    "entry_timestamp": row["entry_timestamp"],
                    "exit_timestamp": row["exit_timestamp"],
                    "entry_price": row["entry_price"],
                    "exit_price": row["exit_price"],
                    "realized_pnl": row["realized_pnl"],
                    "exit_reason": row["exit_reason"],
                    "entry_session_phase": row["entry_session_phase"],
                }
                for row in trade_rows
            ],
            "session_trade_breakdown": summary["session_breakdown"],
            "trade_family_breakdown": summary["trade_family_breakdown"],
            "latest_trade_summary": summary["latest_trade_summary"],
            "atp_summary": {
                "available": strategy_family == "active_trend_participation_engine",
                "timing_available": strategy_family == "active_trend_participation_engine",
                "top_atp_blocker_codes": [],
            },
        },
        "meta": {
            "study_id": study_id,
            "strategy_id": study_id,
            "candidate_id": candidate_id,
            "study_mode": study_mode,
            "entry_model": entry_model,
            "active_entry_model": entry_model,
            "supported_entry_models": [entry_model],
            "entry_model_supported": True,
            "execution_truth_emitter": "strategy_universe_retest_backcast",
            "intrabar_execution_authoritative": intrabar_execution_authoritative,
            "authoritative_intrabar_available": authoritative_intrabar_available,
            "authoritative_entry_truth_available": intrabar_execution_authoritative,
            "authoritative_exit_truth_available": intrabar_execution_authoritative,
            "authoritative_trade_lifecycle_available": intrabar_execution_authoritative,
            "pnl_truth_basis": pnl_truth_basis,
            "lifecycle_truth_class": lifecycle_truth_class,
            "truth_provenance": {
                "runtime_context": "RESEARCH_BACKCAST",
                "run_lane": "BENCHMARK_REPLAY" if study_mode == "baseline_parity_mode" else "RESEARCH_EXECUTION",
                "artifact_context": "STRATEGY_UNIVERSE_RETEST_BACKCAST",
                "persistence_origin": "RESEARCH_SYNTHETIC_ARTIFACT",
                "study_mode": study_mode,
                "artifact_rebuilt": False,
            },
            "timeframe_truth": {
                "structural_signal_timeframe": "5m",
                "execution_timeframe": "1m",
                "artifact_timeframe": "1m",
                "execution_timeframe_role": "execution_detail_only",
            },
        },
    }


def _write_study_payload(*, payload: dict[str, Any], artifact_prefix: str) -> dict[str, Path]:
    json_path = DEFAULT_HISTORICAL_PLAYBACK_DIR / f"{artifact_prefix}.strategy_study.json"
    markdown_path = DEFAULT_HISTORICAL_PLAYBACK_DIR / f"{artifact_prefix}.strategy_study.md"
    payload = compact_strategy_study_payload(payload)
    write_strategy_study_json(payload, json_path)
    write_strategy_study_markdown(payload, markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def _write_historical_playback_manifest(
    *,
    studies: Sequence[dict[str, Any]],
    run_stamp: str,
    historical_playback_dir: Path,
) -> Path:
    manifest_path = historical_playback_dir / f"historical_playback_{run_stamp}.manifest.json"
    run_timestamp = datetime.now(UTC).isoformat()
    symbol_entries: list[dict[str, Any]] = []
    for study in studies:
        strategy_study_json_path = str(study["strategy_study_json_path"])
        strategy_study_markdown_path = str(study["strategy_study_markdown_path"])
        study_payload = json.loads(Path(strategy_study_json_path).read_text(encoding="utf-8"))
        symbol_entries.append(
            {
                "symbol": study["symbol"],
                "label": study.get("label"),
                "study_mode": study.get("study_mode"),
                "summary_path": None,
                "summary_payload": study.get("summary_payload"),
                "strategy_study_json_path": strategy_study_json_path,
                "strategy_study_markdown_path": strategy_study_markdown_path,
                "study_preview": build_strategy_study_preview(study_payload),
                "catalog_entry": build_strategy_study_catalog_entry(
                    payload=study_payload,
                    run_stamp=run_stamp,
                    run_timestamp=run_timestamp,
                    manifest_path=str(manifest_path),
                    summary_path=None,
                    strategy_study_json_path=strategy_study_json_path,
                    strategy_study_markdown_path=strategy_study_markdown_path,
                    label=str(study.get("label") or study.get("symbol") or "study"),
                ),
            }
        )
    payload = {
        "run_stamp": run_stamp,
        "symbols": symbol_entries,
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest_path


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _research_bar_to_domain_bar(bar: ResearchBar) -> Bar:
    return Bar(
        bar_id=f"{bar.instrument}|{bar.timeframe}|{bar.end_ts.isoformat()}",
        symbol=bar.instrument,
        timeframe=bar.timeframe,
        start_ts=bar.start_ts,
        end_ts=bar.end_ts,
        open=Decimal(str(bar.open)),
        high=Decimal(str(bar.high)),
        low=Decimal(str(bar.low)),
        close=Decimal(str(bar.close)),
        volume=bar.volume,
        is_final=True,
        session_asia=bar.session_segment == "ASIA",
        session_london=bar.session_segment == "LONDON",
        session_us=bar.session_segment == "US",
        session_allowed=True,
    )


def _summarize_trade_rows(trade_rows: Sequence[dict[str, Any]], *, bar_count: int) -> dict[str, Any]:
    realized_values = [float(row["realized_pnl"]) for row in trade_rows]
    positive = [value for value in realized_values if value > 0.0]
    negative = [value for value in realized_values if value < 0.0]
    winners = sum(1 for value in realized_values if value > 0.0)
    losers = sum(1 for value in realized_values if value < 0.0)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in realized_values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    by_session: dict[str, list[float]] = defaultdict(list)
    by_vwap: Counter[str] = Counter()
    by_family: dict[str, list[float]] = defaultdict(list)
    for row in trade_rows:
        by_session[str(row.get("entry_session_phase") or "UNKNOWN")].append(float(row["realized_pnl"]))
        if row.get("vwap_price_quality_state"):
            by_vwap[str(row["vwap_price_quality_state"])] += 1
        by_family[str(row.get("family") or "UNKNOWN")].append(float(row["realized_pnl"]))
    session_breakdown = [
        {
            "session": session,
            "trade_count": len(values),
            "wins": sum(1 for value in values if value > 0.0),
            "losses": sum(1 for value in values if value < 0.0),
            "realized_pnl": round(sum(values), 6),
            "latest_trade_timestamp": None,
        }
        for session, values in sorted(by_session.items())
    ]
    trade_family_breakdown = [
        {
            "family": family,
            "trade_count": len(values),
            "wins": sum(1 for value in values if value > 0.0),
            "losses": sum(1 for value in values if value < 0.0),
            "realized_pnl": round(sum(values), 6),
        }
        for family, values in sorted(by_family.items())
    ]
    latest_trade = (
        {
            key: value
            for key, value in trade_rows[-1].items()
            if key
            in {
                "trade_id",
                "entry_timestamp",
                "exit_timestamp",
                "entry_price",
                "exit_price",
                "side",
                "family",
                "entry_session_phase",
                "exit_reason",
                "realized_pnl",
                "vwap_price_quality_state",
            }
        }
        if trade_rows
        else None
    )
    return {
        "trade_count": len(trade_rows),
        "net_pnl": round(sum(realized_values), 6),
        "average_trade": round(fmean(realized_values), 6) if realized_values else 0.0,
        "profit_factor": round(sum(positive) / abs(sum(negative)), 6) if negative else None if not positive else "inf",
        "win_rate": round((winners / len(realized_values)) * 100.0, 4) if realized_values else 0.0,
        "max_drawdown": round(max_drawdown, 6),
        "entries_per_100_bars": round((len(trade_rows) / max(bar_count, 1)) * 100.0, 6),
        "winners": winners,
        "losers": losers,
        "long_trades": sum(1 for row in trade_rows if str(row.get("side") or "").upper() == "LONG"),
        "short_trades": sum(1 for row in trade_rows if str(row.get("side") or "").upper() == "SHORT"),
        "session_breakdown": session_breakdown,
        "trade_family_breakdown": trade_family_breakdown,
        "vwap_breakdown": dict(by_vwap),
        "latest_trade_summary": latest_trade,
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "trade_count": 0,
        "net_pnl": 0.0,
        "average_trade": 0.0,
        "profit_factor": None,
        "win_rate": 0.0,
        "max_drawdown": 0.0,
        "entries_per_100_bars": 0.0,
        "winners": 0,
        "losers": 0,
        "long_trades": 0,
        "short_trades": 0,
        "session_breakdown": [],
        "trade_family_breakdown": [],
        "vwap_breakdown": {},
        "latest_trade_summary": None,
    }


def _normalize_result_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["bucket"] = _result_bucket(normalized)
    return normalized


def _material_improvement(current: dict[str, Any], prior: dict[str, Any]) -> str:
    pnl_delta = float(current.get("net_pnl") or 0.0) - float(prior.get("net_pnl") or 0.0)
    pf_delta = (0.0 if prior.get("profit_factor") in (None, "inf") else float(prior.get("profit_factor")))  # type: ignore[arg-type]
    current_pf = 0.0 if current.get("profit_factor") in (None, "inf") else float(current.get("profit_factor"))  # type: ignore[arg-type]
    if pnl_delta > 0.0 and current_pf >= pf_delta:
        return "economics_improved"
    if float(current.get("trade_count") or 0) > float(prior.get("trade_count") or 0) and current_pf < pf_delta:
        return "quantity_up_quality_down"
    return "mixed_or_flat"


def _approved_quant_recommendation(summary: dict[str, Any]) -> str:
    if float(summary["net_pnl"]) > 0.0 and float(summary["profit_factor"] or 0.0) >= 1.0:
        return "retain_approved"
    return "review_required"


def _atp_recommendation(summary: dict[str, Any], *, lane_status: str) -> str:
    if lane_status == "approved":
        return "retain_approved" if float(summary["net_pnl"]) >= 0.0 else "benchmark_review"
    return "retain_candidate" if float(summary["net_pnl"]) >= 0.0 else "reject_candidate"


def _atp_candidate_recommendation(candidate_id: str, summary: dict[str, Any], baseline_summary: dict[str, Any]) -> str:
    if candidate_id == "promotion_1_075r_favorable_only" and float(summary["net_pnl"]) >= float(baseline_summary["net_pnl"]):
        return "active_research_candidate"
    if float(summary["net_pnl"]) >= 0.0:
        return "retained_candidate"
    return "reject_candidate"


def _probationary_bucket(
    *,
    current: dict[str, Any],
    prior: dict[str, Any],
    lane_status: str,
    data_limit_status: str,
) -> str:
    if data_limit_status != "none":
        return "interesting_but_not_clean_enough"
    net = float(current.get("net_pnl") or 0.0)
    pf = float(current.get("profit_factor") or 0.0)
    improved = _material_improvement(current, prior) == "economics_improved"
    if lane_status == "approved_probationary" and improved and net > 0.0 and pf >= 1.0:
        return "improved_approved"
    if lane_status != "approved_probationary" and net > 0.0 and pf >= 1.0:
        return "promotable_now"
    if net > 0.0 and pf >= 0.9:
        return "retained_candidate"
    if net > -50.0 or pf >= 0.8:
        return "interesting_but_not_clean_enough"
    return "still_reject"


def _result_bucket(row: dict[str, Any]) -> str:
    recommendation = str(row.get("recommendation") or "")
    status = str(row.get("status") or "")
    material_improvement = str(row.get("material_improvement") or "")
    data_limit_status = str(row.get("data_limit_status") or "none")
    metrics = dict(row.get("metrics") or {})
    net = float(metrics.get("net_pnl") or 0.0)
    pf_raw = metrics.get("profit_factor")
    pf = 999.0 if pf_raw == "inf" else float(pf_raw or 0.0)
    if data_limit_status != "none":
        return "interesting_but_not_clean_enough"
    if recommendation == "promotable_now":
        return "promotable_now"
    if status in {"approved", "approved_probationary"} and material_improvement == "economics_improved" and net > 0.0 and pf >= 1.0:
        return "improved_approved"
    if recommendation in {"retain_candidate", "retained_candidate", "active_research_candidate", "retain_approved"}:
        return "retained_candidate"
    if recommendation in {"reject_candidate", "still_reject"}:
        return "still_reject"
    return "interesting_but_not_clean_enough"


def _render_report_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Strategy Universe Retest",
        "",
        "## Methodology",
        f"- {payload['methodology']['summary']}",
        f"- {payload['methodology']['rolling_5m_interpretation']}",
        f"- {payload['methodology']['data_limitation']}",
        f"- {payload['methodology']['previously_omitted_now_included']}",
        f"- Families newly included from the omitted branch set: `{', '.join(payload['methodology']['previously_omitted_now_included_families'])}`",
        "",
        "## Expanded Universe",
    ]
    for row in payload.get("expanded_universe") or []:
        lines.append(
            f"- `{row['symbol']}` / `{row['family']}` / `{row['status']}` / `{row['cohort']}` / data `{row['data_status']}`"
        )
    lines.extend(
        [
            "",
            "## Bucket Summary",
        ]
    )
    grouped_results: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in payload.get("results") or []:
        grouped_results[str(row.get("bucket") or "uncategorized")].append(row)
    for bucket in (
        "improved_approved",
        "promotable_now",
        "retained_candidate",
        "interesting_but_not_clean_enough",
        "still_reject",
    ):
        bucket_rows = grouped_results.get(bucket) or []
        lines.append(f"- `{bucket}`: `{len(bucket_rows)}`")
    lines.extend(
        [
            "",
            "## Results",
        ]
    )
    for row in payload.get("results") or []:
        metrics = row["metrics"]
        lines.extend(
            [
                f"### {row['display_name']}",
                f"- Status: `{row['status']}`",
                f"- Symbol: `{row['symbol']}`",
                f"- Cohort: `{row.get('cohort') or 'N/A'}`",
                f"- Bucket: `{row.get('bucket') or 'uncategorized'}`",
                f"- Trades: `{metrics['trade_count']}`",
                f"- Net P&L: `{metrics['net_pnl']}`",
                f"- Avg trade: `{metrics['average_trade']}`",
                f"- Profit factor: `{metrics['profit_factor']}`",
                f"- Win rate: `{metrics['win_rate']}`",
                f"- Max drawdown: `{metrics['max_drawdown']}`",
                f"- Entries / 100 bars: `{metrics['entries_per_100_bars']}`",
                f"- VWAP breakdown: `{metrics.get('vwap_breakdown')}`",
                f"- Improvement verdict: `{row['material_improvement']}`",
                f"- Recommendation: `{row['recommendation']}`",
                f"- Data limit: `{row.get('data_limit_status') or 'none'}`",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    result = run_strategy_universe_retest()
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
