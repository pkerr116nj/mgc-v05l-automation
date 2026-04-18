"""Registry-driven rolling-entry retest and historical back-cast population."""

from __future__ import annotations

import json
import sqlite3
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import fmean
from time import perf_counter
from typing import Any, Sequence
from uuid import uuid4

from ..config_models import EnvironmentMode, ExecutionTimeframeRole, load_settings_from_files
from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, ShortEntryFamily
from ..domain.models import Bar, SignalPacket
from ..execution.execution_engine import ExecutionEngine
from ..execution.order_models import FillEvent, OrderIntent
from ..indicators.feature_engine import IncrementalFeatureComputer
from ..market_data.bar_builder import BarBuilder
from ..market_data.provider_config import load_market_data_providers_config
from ..market_data.provider_models import MarketDataUseCase
from ..market_data.replay_feed import ReplayFeed
from ..market_data.session_clock import classify_sessions
from ..market_data.sqlite_playback import SQLiteHistoricalBarSource
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..research.bar_resampling import build_resampled_bars
from ..research.platform import (
    SourceSelection as PlatformSourceSelection,
    discover_best_sources as discover_best_sources_platform,
    load_symbol_context as load_symbol_context_platform,
)
from ..research.quant_futures import _FrameSeries, _align_timestamps, _build_feature_rows
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.phase2_continuation import ENTRY_ELIGIBLE
from ..research.trend_participation.phase3_timing import (
    ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    ATP_TIMING_ACTIVATION_COMPLETED_5M,
    ATP_TIMING_ACTIVATION_ROLLING_5M,
    VWAP_CHASE_RISK,
    VWAP_NEUTRAL,
    classify_vwap_price_quality,
)
from ..research.trend_participation.substrate import ensure_atp_feature_bundle, ensure_atp_scope_bundle
from ..research.trend_participation.storage import (
    load_sqlite_bars,
    normalize_and_check_bars,
    resample_bars_from_1m,
    rolling_window_bars_from_1m,
)
from ..signals.asia_vwap_reclaim import evaluate_asia_vwap_reclaim
from ..signals.bear_snap import evaluate_bear_snap
from ..signals.bull_snap import evaluate_bull_snap
from ..signals.entry_resolver import resolve_entries
from ..strategy.strategy_engine import StrategyEngine
from ..strategy.exit_engine import evaluate_exits
from ..strategy.risk_engine import compute_risk_context
from ..strategy.state_machine import (
    increment_bars_in_trade,
    transition_on_entry_fill,
    transition_on_exit_fill,
    transition_to_ready,
    update_additive_short_peak_state,
)
from ..strategy.trade_state import build_initial_state
from ..strategy.strategy_engine import (
    _bar_matches_probationary_session_restriction,
    _empty_signal_packet_payload,
    _gc_mgc_asia_retest_hold_london_open_extension_matches,
    _next_counter,
    _signal_present,
)
from .paper_lane_analyst_pack import REQUIRED_CANDIDATE_SPECS
from .replay_reporting import build_session_lookup, build_trade_ledger
from .approved_quant_lanes.evaluator import evaluate_approved_lane, lane_rejection_reason
from .approved_quant_lanes.specs import approved_quant_lane_specs
from .replay_base_preservation import DEFAULT_REPORT_DIR as DEFAULT_REPLAY_PRESERVATION_REPORT_DIR
from .replay_base_preservation import preserve_replay_base
from .strategy_runtime_registry import (
    StandaloneStrategyRuntimeInstance,
    StrategyRuntimeRegistry,
    _build_strategy_engine_instance,
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


def _research_platform_root() -> Path:
    return REPO_ROOT / "outputs" / "research_platform"


def _atp_substrate_root() -> Path:
    return _research_platform_root() / "atp_substrate"
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
_SYMBOL_CONTEXT_CACHE: dict[tuple[Any, ...], dict[str, Any] | None] = {}
_PROBATIONARY_PLAYBACK_CACHE: dict[tuple[Any, ...], dict[str, Any] | None] = {}
_PROBATIONARY_COMPILED_LONG_SOURCE_KEYS: dict[str, str] = {
    "usLatePauseResumeLongTurn": "us_late_pause_resume_long_turn_candidate",
    "asiaEarlyNormalBreakoutRetestHoldTurn": "asia_early_normal_breakout_retest_hold_long_turn_candidate",
}
_PROBATIONARY_COMPILED_SHORT_SOURCE_KEYS: dict[str, str] = {
    "asiaEarlyPauseResumeShortTurn": "asia_early_pause_resume_short_turn_candidate",
}
EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN = "LEGACY_NEXT_BAR_OPEN"
EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP = "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP"
EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED = "PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED"
EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP = "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP"
EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP = "ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP"
EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP = "APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP"
EXECUTION_MODEL_APPROVED_QUANT_COMPLETED_5M_RULES = "APPROVED_QUANT_COMPLETED_5M_RULES"
_PROBATIONARY_LONG_ENTRY_RAW_KEYS: tuple[str, ...] = (
    "first_bull_snap_turn",
    "asia_vwap_long_signal",
    "midday_pause_resume_long_turn_candidate",
    "us_late_breakout_retest_hold_long_turn_candidate",
    "us_late_failed_move_reversal_long_turn_candidate",
    "us_late_pause_resume_long_turn_candidate",
    "asia_early_breakout_retest_hold_long_turn_candidate",
    "asia_early_normal_breakout_retest_hold_long_turn_candidate",
    "asia_late_pause_resume_long_turn_candidate",
    "asia_late_flat_pullback_pause_resume_long_turn_candidate",
    "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate",
)
_PROBATIONARY_SHORT_ENTRY_RAW_KEYS: tuple[str, ...] = (
    "first_bear_snap_turn",
    "derivative_bear_turn_candidate",
    "derivative_bear_additive_turn_candidate",
    "midday_compressed_rebound_failed_move_reversal_short_turn_candidate",
    "midday_compressed_failed_move_reversal_short_turn_candidate",
    "midday_expanded_pause_resume_short_turn_candidate",
    "midday_compressed_pause_resume_short_turn_candidate",
    "midday_pause_resume_short_turn_candidate",
    "london_late_pause_resume_short_turn_candidate",
    "asia_early_expanded_breakout_retest_hold_short_turn_candidate",
    "asia_early_compressed_pause_resume_short_turn_candidate",
    "asia_early_pause_resume_short_turn_candidate",
)

SourceSelection = PlatformSourceSelection


@dataclass(frozen=True)
class RetestShardConfig:
    shard_months: int = 3
    warmup_days: int = 14
    source_timeframe: str = "1m"
    structural_timeframe: str = "5m"
    execution_timeframe: str = "1m"
    artifact_mode: str = "selective_rich"
    rich_artifact_buckets: tuple[str, ...] = ("promotable_now", "retained_candidate")
    include_reference_lanes: bool = True
    probationary_fast_path_mode: str = "disabled"
    probationary_current_execution_model: str = EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
    probationary_prior_execution_model: str = EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED


@dataclass(frozen=True)
class RetestShardWindow:
    symbol: str
    shard_id: str
    evaluation_start: datetime
    evaluation_end: datetime
    load_start: datetime
    load_end: datetime


@dataclass
class TimingBreakdown:
    total_wall_seconds: float = 0.0
    load_seconds: float = 0.0
    resample_seconds: float = 0.0
    lane_evaluation_seconds: float = 0.0
    artifact_generation_seconds: float = 0.0
    detector_seconds: float = 0.0
    detector_triggered_lane_count: int = 0
    detector_skipped_lane_count: int = 0
    by_symbol: dict[str, float] = field(default_factory=dict)
    by_shard: dict[str, dict[str, float]] = field(default_factory=dict)
    by_group: dict[str, float] = field(default_factory=dict)

    def add_symbol_time(self, symbol: str, seconds: float) -> None:
        self.by_symbol[symbol] = round(float(self.by_symbol.get(symbol, 0.0)) + float(seconds), 6)

    def add_shard_time(self, shard_id: str, key: str, seconds: float) -> None:
        shard = self.by_shard.setdefault(shard_id, {})
        shard[key] = round(float(shard.get(key, 0.0)) + float(seconds), 6)

    def add_group_time(self, group: str, seconds: float) -> None:
        self.by_group[group] = round(float(self.by_group.get(group, 0.0)) + float(seconds), 6)


def _run_strategy_universe_retest_legacy(
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
    source_database_paths: Sequence[str | Path] | None = None,
    preserve_base: bool = True,
) -> dict[str, Path]:
    _SYMBOL_CONTEXT_CACHE.clear()
    _PROBATIONARY_PLAYBACK_CACHE.clear()
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
            historical_playback_dir=historical_playback_dir,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    )
    studies.extend(
        _run_approved_quant_retests(
            report_rows=report_rows,
            bar_source_index=bar_source_index,
            historical_playback_dir=historical_playback_dir,
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
            "execution_contracts": _execution_contracts_payload(shard_config=config),
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


def run_strategy_universe_retest(
    *,
    report_dir: Path = DEFAULT_REPORT_DIR,
    historical_playback_dir: Path = DEFAULT_HISTORICAL_PLAYBACK_DIR,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
    source_database_paths: Sequence[str | Path] | None = None,
    preserve_base: bool = True,
    shard_config: RetestShardConfig | None = None,
    include_validation_slice: bool = False,
) -> dict[str, Path]:
    config = shard_config or RetestShardConfig()
    _SYMBOL_CONTEXT_CACHE.clear()
    _PROBATIONARY_PLAYBACK_CACHE.clear()
    report_dir.mkdir(parents=True, exist_ok=True)
    historical_playback_dir.mkdir(parents=True, exist_ok=True)
    replay_preservation = (
        preserve_replay_base(report_dir=DEFAULT_REPLAY_PRESERVATION_REPORT_DIR)
        if preserve_base
        else None
    )

    universe = _build_active_universe()
    active_symbols = {str(row["symbol"]) for row in universe["expanded_universe"]}
    bar_source_index = _discover_best_sources(
        symbols=active_symbols,
        timeframes={"1m", "5m"},
        sqlite_paths=source_database_paths,
    )
    symbol_windows = _build_symbol_windows(
        symbols=active_symbols,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        shard_config=config,
    )

    timing = TimingBreakdown()
    timing_total_started = perf_counter()
    aggregates = _run_sharded_universe(
        universe=universe,
        symbol_windows=symbol_windows,
        bar_source_index=bar_source_index,
        shard_config=config,
        timing=timing,
    )
    timing.total_wall_seconds = round(perf_counter() - timing_total_started, 6)

    normalized_results, selected_studies = _finalize_sharded_results(
        aggregates=aggregates,
        bar_source_index=bar_source_index,
        historical_playback_dir=historical_playback_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        shard_config=config,
        timing=timing,
    )

    run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    manifest_path = _write_historical_playback_manifest(
        studies=selected_studies,
        run_stamp=run_stamp,
        historical_playback_dir=historical_playback_dir,
    )
    validation = (
        _build_validation_slice_comparison(
            report_dir=report_dir,
            historical_playback_dir=historical_playback_dir,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            source_database_paths=source_database_paths,
            bar_source_index=bar_source_index,
            shard_config=config,
        )
        if include_validation_slice
        else {"status": "skipped"}
    )
    report_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "methodology": {
            "summary": (
                "Strategy-universe retests now shard by symbol and quarter-sized evaluation windows, with "
                "shared symbol-window preparation and selective rich-artifact emission."
            ),
            "rolling_5m_interpretation": (
                "Each completed 1m candle is paired with the latest causal 5-minute lookback window ending on "
                "that minute. Structure and readiness stay 5m-defined, execution stays 1m, and chase-risk "
                "entries remain blocked by default."
            ),
            "data_limitation": (
                "Raw canonical 1m coverage, derived timeframe coverage, emitted playback coverage, closed-trade "
                "economics, and app-visible coverage remain explicitly separated in output artifacts."
            ),
            "window": {
                "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
                "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
            },
            "sharding": {
                "processing_unit": "(symbol, window)",
                "shard_months": config.shard_months,
                "warmup_days": config.warmup_days,
                "artifact_mode": config.artifact_mode,
                "rich_artifact_buckets": list(config.rich_artifact_buckets),
            },
            "previously_omitted_now_included": (
                "Older non-ATP probationary branch families and next-tier candidate equivalents from the paper-lane "
                "analyst pack remain included in the retest universe, but rich artifacts are no longer emitted "
                "for every lane by default."
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
        "coverage_summary": _coverage_summary_payload(normalized_results, selected_studies),
        "historical_playback_manifest": str(manifest_path),
        "replay_base_preservation": replay_preservation,
        "performance": _timing_payload(timing=timing, shard_count=len(symbol_windows)),
        "validation_slice": validation,
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


def _build_active_universe() -> dict[str, Any]:
    atp_lanes = [
        {
            "strategy_id": "atp_companion_v1__benchmark_mgc_asia_us",
            "study_id": "atp_companion_v1__benchmark_mgc_asia_us",
            "symbol": "MGC",
            "display_name": "ATP Companion Baseline v1 / MGC / Asia+US",
            "lane_status": "approved",
            "study_mode": "baseline_parity_mode",
            "family": "active_trend_participation_engine",
            "cohort": "ATP_CORE",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("10"),
            "candidate_id": None,
            "reference_lane": True,
            "lane_type": "atp_core",
        },
        {
            "strategy_id": "atp_companion_v1__candidate_gc_asia_us",
            "study_id": "atp_companion_v1__candidate_gc_asia_us",
            "symbol": "GC",
            "display_name": "ATP Companion Candidate v1 / GC / Asia+US",
            "lane_status": "active_research_candidate",
            "study_mode": "research_execution_mode",
            "family": "active_trend_participation_engine",
            "cohort": "ATP_CORE",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("100"),
            "candidate_id": None,
            "reference_lane": True,
            "lane_type": "atp_core",
        },
        {
            "strategy_id": "atp_companion_v1__candidate_pl_asia_us",
            "study_id": "atp_companion_v1__candidate_pl_asia_us",
            "symbol": "PL",
            "display_name": "ATP Companion Candidate v1 / PL / Asia+US",
            "lane_status": "active_research_candidate",
            "study_mode": "research_execution_mode",
            "family": "active_trend_participation_engine",
            "cohort": "ATP_CORE",
            "allowed_sessions": {"ASIA", "US"},
            "point_value": Decimal("50"),
            "candidate_id": None,
            "reference_lane": True,
            "lane_type": "atp_core",
        },
    ]
    promotion_candidates = []
    for candidate_id in ("promotion_1_050r_neutral_plus", "promotion_1_075r_neutral_plus", "promotion_1_075r_favorable_only"):
        promotion_candidates.append(
            {
                "strategy_id": f"atp_companion_v1__{candidate_id}",
                "study_id": f"atp_companion_v1__{candidate_id}",
                "symbol": "MGC",
                "display_name": f"ATP Companion / {candidate_id}",
                "lane_status": "active_research_candidate" if candidate_id == "promotion_1_075r_favorable_only" else "retained_candidate",
                "study_mode": "research_execution_mode",
                "family": "ATP promotion/add branch",
                "cohort": "ATP_PROMOTION_BRANCH",
                "allowed_sessions": {"ASIA", "US"},
                "point_value": Decimal("10"),
                "candidate_id": candidate_id,
                "reference_lane": False,
                "lane_type": "atp_promotion",
            }
        )

    approved_quant = []
    for spec in approved_quant_lane_specs():
        for symbol in spec.symbols:
            approved_quant.append(
                {
                    "strategy_id": f"{spec.lane_name}__{symbol}",
                    "study_id": f"{spec.lane_name}__{symbol}",
                    "symbol": symbol,
                    "display_name": f"{spec.lane_name} / {symbol}",
                    "lane_status": "approved",
                    "study_mode": "baseline_parity_mode",
                    "family": spec.family,
                    "cohort": "APPROVED_QUANT",
                    "point_value": APPROVED_QUANT_POINT_VALUES.get(symbol, Decimal("1")),
                    "spec": spec,
                    "reference_lane": True,
                    "lane_type": "approved_quant",
                }
            )

    settings = load_settings_from_files(PROBATIONARY_CONFIG_PATHS)
    probationary = []
    explicit_lane_rows = list(settings.probationary_paper_lane_specs)
    admitted_pairs = {
        (str(raw.get("symbol") or "").strip().upper(), _resolve_probationary_branch(raw))
        for raw in explicit_lane_rows
    }
    for raw in explicit_lane_rows:
        symbol = str(raw.get("symbol") or "").strip().upper()
        family = _resolve_probationary_branch(raw)
        strategy_id = _probationary_standalone_strategy_id(settings, raw) or str(raw.get("lane_id") or f"{symbol}__{family}")
        probationary.append(
            {
                **raw,
                "strategy_id": strategy_id,
                "study_id": strategy_id,
                "symbol": symbol,
                "display_name": raw.get("display_name") or f"{symbol} / {family}",
                "lane_status": "approved_probationary",
                "research_cohort": "ADMITTED_COMPARATOR",
                "family": family,
                "cohort": "ADMITTED_COMPARATOR",
                "point_value": str(raw.get("point_value") or RESEARCH_CONTRACT_POINT_VALUES.get(symbol, Decimal("1"))),
                "reference_lane": True,
                "lane_type": "probationary",
            }
        )
    for candidate in REQUIRED_CANDIDATE_SPECS:
        symbol = str(candidate["instrument"]).strip().upper()
        branch = str(candidate["branch"]).strip()
        if (symbol, branch) in admitted_pairs:
            continue
        raw_lane = {
            "lane_id": f"{symbol.lower()}_{branch}",
            "display_name": f"{symbol} / {branch}",
            "symbol": symbol,
            "strategy_family": branch,
            "session_restriction": _probationary_session_restriction(branch),
            "long_sources": [branch] if branch in PROBATIONARY_LONG_ONLY_FAMILIES else [],
            "short_sources": [branch] if branch in PROBATIONARY_SHORT_ONLY_FAMILIES else [],
            "point_value": str(RESEARCH_CONTRACT_POINT_VALUES.get(symbol, Decimal("1"))),
        }
        strategy_id = _probationary_standalone_strategy_id(settings, raw_lane) or f"{symbol}__{branch}"
        probationary.append(
            {
                **raw_lane,
                "strategy_id": strategy_id,
                "study_id": strategy_id,
                "family": branch,
                "lane_status": "retained_candidate",
                "research_cohort": str(candidate["cohort"]),
                "cohort": str(candidate["cohort"]),
                "reference_lane": False,
                "lane_type": "probationary",
            }
        )

    all_rows = [*atp_lanes, *promotion_candidates, *approved_quant, *probationary]
    return {
        "atp_lanes": atp_lanes,
        "atp_promotion_candidates": promotion_candidates,
        "approved_quant_lanes": approved_quant,
        "probationary_lanes": probationary,
        "expanded_universe": [
            {
                "symbol": str(row["symbol"]),
                "family": str(row.get("family") or row.get("strategy_family") or ""),
                "status": str(row["lane_status"]),
                "cohort": str(row.get("cohort") or row.get("research_cohort") or ""),
                "data_status": "pending",
            }
            for row in all_rows
        ],
    }


def _probationary_standalone_strategy_id(base_settings, lane: dict[str, Any]) -> str | None:
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=[lane])
    if not definitions:
        return None
    return str(definitions[0].standalone_strategy_id)


def _build_symbol_windows(
    *,
    symbols: set[str],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    shard_config: RetestShardConfig,
) -> list[RetestShardWindow]:
    windows: list[RetestShardWindow] = []
    for symbol in sorted(symbols):
        minute_source = bar_source_index.get(symbol, {}).get("1m")
        if minute_source is None or minute_source.start_ts is None or minute_source.end_ts is None:
            continue
        symbol_start = datetime.fromisoformat(minute_source.start_ts)
        symbol_end = datetime.fromisoformat(minute_source.end_ts)
        evaluation_start = max(symbol_start, start_timestamp) if start_timestamp is not None else symbol_start
        evaluation_end = min(symbol_end, end_timestamp) if end_timestamp is not None else symbol_end
        if evaluation_start > evaluation_end:
            continue
        shard_start = evaluation_start
        index = 0
        while shard_start <= evaluation_end:
            shard_end = min(_add_months(shard_start, shard_config.shard_months) - timedelta(minutes=1), evaluation_end)
            load_start = max(symbol_start, shard_start - timedelta(days=shard_config.warmup_days))
            windows.append(
                RetestShardWindow(
                    symbol=symbol,
                    shard_id=f"{symbol}:{index}:{shard_start.date().isoformat()}:{shard_end.date().isoformat()}",
                    evaluation_start=shard_start,
                    evaluation_end=shard_end,
                    load_start=load_start,
                    load_end=shard_end,
                )
            )
            shard_start = shard_end + timedelta(minutes=1)
            index += 1
    return windows


def _add_months(value: datetime, months: int) -> datetime:
    total_months = (value.year * 12 + value.month - 1) + months
    year = total_months // 12
    month = total_months % 12 + 1
    day = min(value.day, _days_in_month(year, month))
    return value.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=UTC)
    current_month = datetime(year, month, 1, tzinfo=UTC)
    return (next_month - current_month).days


def _discover_best_sources(
    *,
    symbols: set[str],
    timeframes: set[str],
    sqlite_paths: Sequence[str | Path] | None = None,
) -> dict[str, dict[str, SourceSelection]]:
    return discover_best_sources_platform(
        symbols=symbols,
        timeframes=timeframes,
        sqlite_paths=sqlite_paths if sqlite_paths is not None else sorted(REPO_ROOT.glob("*.sqlite3")),
    )


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


def _run_sharded_universe(
    *,
    universe: dict[str, Any],
    symbol_windows: Sequence[RetestShardWindow],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    shard_config: RetestShardConfig,
    timing: TimingBreakdown,
) -> dict[str, dict[str, Any]]:
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    probationary_settings = load_settings_from_files(PROBATIONARY_CONFIG_PATHS)
    symbol_to_atp = _group_rows_by_symbol(universe["atp_lanes"])
    symbol_to_promotions = _group_rows_by_symbol(universe["atp_promotion_candidates"])
    symbol_to_approved = _group_rows_by_symbol(universe["approved_quant_lanes"])
    symbol_to_probationary = _group_rows_by_symbol(universe["probationary_lanes"])
    aggregates = _initialize_lane_aggregates(
        universe=universe,
        bar_source_index=bar_source_index,
        shard_config=shard_config,
    )

    for window in symbol_windows:
        shard_started = perf_counter()
        symbol = window.symbol
        loaded_context = None
        if symbol in symbol_to_atp or symbol in symbol_to_approved or symbol in symbol_to_promotions:
            load_started = perf_counter()
            loaded_context = _load_symbol_context(
                symbol=symbol,
                bar_source_index=bar_source_index,
                start_timestamp=window.load_start,
                end_timestamp=window.load_end,
            )
            elapsed = perf_counter() - load_started
            timing.load_seconds = round(timing.load_seconds + elapsed, 6)
            timing.add_shard_time(window.shard_id, "shared_context_load_seconds", elapsed)

        lane_started = perf_counter()
        atp_started = perf_counter()
        for lane in symbol_to_atp.get(symbol, []):
            result = _evaluate_atp_lane(
                symbol=symbol,
                allowed_sessions=set(lane["allowed_sessions"]),
                point_value=Decimal(str(lane["point_value"])),
                bar_source_index=bar_source_index,
                start_timestamp=window.load_start,
                end_timestamp=window.load_end,
                loaded_context=loaded_context,
            )
            if result is None:
                continue
            current_rows = _filter_trade_rows_to_window(
                result["trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            prior_rows = _filter_trade_rows_to_window(
                result["prior_trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            current_bar_count = _count_research_bars_in_window(result["bars_1m"], window)
            _accumulate_lane_rows(
                aggregate=aggregates[str(lane["strategy_id"])],
                current_trade_rows=current_rows,
                current_bar_count=current_bar_count,
                prior_trade_rows=prior_rows,
                prior_bar_count=current_bar_count,
            )
            if lane["study_id"] != "atp_companion_v1__benchmark_mgc_asia_us":
                continue
            for candidate_lane in symbol_to_promotions.get(symbol, []):
                candidate = candidate_defs[str(candidate_lane["candidate_id"])]
                candidate_rows = _build_atp_candidate_trade_rows(
                    symbol=symbol,
                    candidate=candidate,
                    bars_1m=result["bars_1m"],
                    trade_rows=current_rows,
                    point_value=float(lane["point_value"]),
                )
                _accumulate_lane_rows(
                    aggregate=aggregates[str(candidate_lane["strategy_id"])],
                    current_trade_rows=candidate_rows,
                    current_bar_count=current_bar_count,
                    prior_trade_rows=current_rows,
                    prior_bar_count=current_bar_count,
                )
        atp_elapsed = perf_counter() - atp_started
        timing.add_group_time("ATP", atp_elapsed)
        timing.add_shard_time(window.shard_id, "atp_evaluation_seconds", atp_elapsed)

        approved_started = perf_counter()
        for lane in symbol_to_approved.get(symbol, []):
            result = _evaluate_approved_quant_lane_symbol(
                spec=lane["spec"],
                symbol=symbol,
                bar_source_index=bar_source_index,
                start_timestamp=window.load_start,
                end_timestamp=window.load_end,
                loaded_context=loaded_context,
            )
            if result is None:
                continue
            current_rows = _filter_trade_rows_to_window(
                result["trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            prior_rows = _filter_trade_rows_to_window(
                result["prior_trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            current_bar_count = _count_research_bars_in_window(result["bars_1m"], window)
            prior_bar_count = _count_research_bars_in_window(result["completed_5m_bars"], window)
            _accumulate_lane_rows(
                aggregate=aggregates[str(lane["strategy_id"])],
                current_trade_rows=current_rows,
                current_bar_count=current_bar_count,
                prior_trade_rows=prior_rows,
                prior_bar_count=prior_bar_count,
            )
        approved_elapsed = perf_counter() - approved_started
        timing.add_group_time("APPROVED_QUANT", approved_elapsed)
        timing.add_shard_time(window.shard_id, "approved_quant_evaluation_seconds", approved_elapsed)

        probationary_current_results: dict[str, dict[str, Any]] = {}
        probationary_prior_results: dict[str, dict[str, Any]] = {}
        if symbol in symbol_to_probationary:
            prep_started = perf_counter()
            probationary_bundle = _prepare_probationary_playback_bundle(
                symbol=symbol,
                base_settings=probationary_settings,
                bar_source_index=bar_source_index,
                start_timestamp=window.load_start,
                end_timestamp=window.load_end,
            )
            prep_elapsed = perf_counter() - prep_started
            timing.load_seconds = round(timing.load_seconds + float(probationary_bundle.get("load_seconds", 0.0)), 6) if probationary_bundle else timing.load_seconds
            timing.resample_seconds = round(timing.resample_seconds + float(probationary_bundle.get("resample_seconds", 0.0)), 6) if probationary_bundle else timing.resample_seconds
            timing.add_shard_time(window.shard_id, "probationary_prep_seconds", prep_elapsed)
            probationary_started = perf_counter()
            if probationary_bundle is not None:
                probationary_lanes = symbol_to_probationary.get(symbol, [])
                current_triggered_lanes = list(probationary_lanes)
                prior_triggered_lanes = list(probationary_lanes)
                if shard_config.probationary_fast_path_mode != "disabled":
                    detector_started = perf_counter()
                    structural_detector_bars = list(probationary_bundle["structural_bars_5m"])
                    current_detector = _run_probationary_trigger_detector_from_bars(
                        lanes=probationary_lanes,
                        base_settings=probationary_settings,
                        environment_mode=EnvironmentMode.RESEARCH_EXECUTION,
                        bars=structural_detector_bars,
                        target_timeframe="5m",
                    )
                    prior_detector = _run_probationary_trigger_detector_from_bars(
                        lanes=probationary_lanes,
                        base_settings=probationary_settings,
                        environment_mode=EnvironmentMode.BASELINE_PARITY,
                        bars=structural_detector_bars,
                        target_timeframe="5m",
                    )
                    detector_elapsed = perf_counter() - detector_started
                    current_triggered_lanes = [
                        lane
                        for lane in probationary_lanes
                        if bool((current_detector.get(str(lane["strategy_id"])) or {}).get("triggered"))
                    ]
                    prior_triggered_lanes = [
                        lane
                        for lane in probationary_lanes
                        if bool((prior_detector.get(str(lane["strategy_id"])) or {}).get("triggered"))
                    ]
                    detector_triggered = len(
                        {
                            *(str(lane["strategy_id"]) for lane in current_triggered_lanes),
                            *(str(lane["strategy_id"]) for lane in prior_triggered_lanes),
                        }
                    )
                    detector_skipped = max(len(probationary_lanes) - detector_triggered, 0)
                    timing.detector_seconds = round(timing.detector_seconds + detector_elapsed, 6)
                    timing.detector_triggered_lane_count += detector_triggered
                    timing.detector_skipped_lane_count += detector_skipped
                    timing.add_group_time("PROBATIONARY_DETECTOR", detector_elapsed)
                    timing.add_shard_time(window.shard_id, "probationary_detector_seconds", detector_elapsed)
                    timing.add_shard_time(window.shard_id, "probationary_detector_triggered_lanes", detector_triggered)
                    timing.add_shard_time(window.shard_id, "probationary_detector_skipped_lanes", detector_skipped)
                probationary_current_results = _run_probationary_registry_playback_from_bars(
                    lanes=current_triggered_lanes,
                    base_settings=probationary_settings,
                    environment_mode=EnvironmentMode.RESEARCH_EXECUTION,
                    bars=list(probationary_bundle["current_bars"]),
                    structural_bars=list(probationary_bundle["structural_bars_5m"]),
                    target_timeframe="1m",
                    execution_model_label=shard_config.probationary_current_execution_model,
                )
                probationary_prior_results = _run_probationary_registry_playback_from_bars(
                    lanes=prior_triggered_lanes,
                    base_settings=probationary_settings,
                    environment_mode=EnvironmentMode.BASELINE_PARITY,
                    bars=list(probationary_bundle["current_bars"]),
                    structural_bars=list(probationary_bundle["structural_bars_5m"]),
                    target_timeframe="1m",
                    execution_model_label=shard_config.probationary_prior_execution_model,
                )
            probationary_elapsed = perf_counter() - probationary_started
            timing.add_group_time("PROBATIONARY", probationary_elapsed)
            timing.add_shard_time(window.shard_id, "probationary_evaluation_seconds", probationary_elapsed)
        for lane in symbol_to_probationary.get(symbol, []):
            current = probationary_current_results.get(str(lane["strategy_id"]))
            prior = probationary_prior_results.get(str(lane["strategy_id"]))
            current_rows = [] if current is None else _filter_trade_rows_to_window(
                current["trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            prior_rows = [] if prior is None else _filter_trade_rows_to_window(
                prior["trade_rows"],
                evaluation_start=window.evaluation_start,
                evaluation_end=window.evaluation_end,
            )
            current_bar_count = 0 if probationary_bundle is None else _count_domain_bars_in_window(probationary_bundle["current_bars"], window)
            prior_bar_count = 0 if probationary_bundle is None else _count_domain_bars_in_window(probationary_bundle["prior_bars"], window)
            _accumulate_lane_rows(
                aggregate=aggregates[str(lane["strategy_id"])],
                current_trade_rows=current_rows,
                current_bar_count=current_bar_count,
                prior_trade_rows=prior_rows,
                prior_bar_count=prior_bar_count,
            )
        lane_elapsed = perf_counter() - lane_started
        timing.lane_evaluation_seconds = round(timing.lane_evaluation_seconds + lane_elapsed, 6)
        timing.add_symbol_time(symbol, perf_counter() - shard_started)
        timing.add_shard_time(window.shard_id, "lane_evaluation_seconds", lane_elapsed)
        timing.add_shard_time(window.shard_id, "total_shard_seconds", perf_counter() - shard_started)

    return aggregates


def _group_rows_by_symbol(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append(dict(row))
    return grouped


def _initialize_lane_aggregates(
    *,
    universe: dict[str, Any],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    shard_config: RetestShardConfig | None = None,
) -> dict[str, dict[str, Any]]:
    config = shard_config or RetestShardConfig()
    aggregates: dict[str, dict[str, Any]] = {}
    for key in ("atp_lanes", "atp_promotion_candidates", "approved_quant_lanes", "probationary_lanes"):
        for lane in universe[key]:
            symbol = str(lane["symbol"])
            aggregates[str(lane["strategy_id"])] = {
                "meta": {
                    "strategy_id": str(lane["strategy_id"]),
                    "display_name": str(lane["display_name"]),
                    "status": str(lane["lane_status"]),
                    "family": str(lane.get("family") or lane.get("strategy_family") or ""),
                    "symbol": symbol,
                    "cohort": str(lane.get("cohort") or lane.get("research_cohort") or ""),
                    "reference_lane": bool(lane.get("reference_lane")),
                    "study_mode": str(lane.get("study_mode") or "research_execution_mode"),
                    "point_value": Decimal(str(lane.get("point_value") or RESEARCH_CONTRACT_POINT_VALUES.get(symbol, Decimal("1")))),
                    "candidate_id": lane.get("candidate_id"),
                    "lane_type": str(lane.get("lane_type") or ""),
                    "execution_model": _current_execution_model_label(
                        str(lane.get("lane_type") or ""),
                        shard_config=config,
                    ),
                    "prior_execution_model": _prior_execution_model_label(
                        str(lane.get("lane_type") or ""),
                        shard_config=config,
                    ),
                    "raw_lane": dict(lane),
                    "data_limit_status": (
                        _probationary_data_limit_status(symbol=symbol, bar_source_index=bar_source_index, current={})
                        if str(lane.get("lane_type")) == "probationary"
                        else _symbol_data_limit_status(symbol=symbol, bar_source_index=bar_source_index)
                    ),
                },
                "current_trade_rows": [],
                "prior_trade_rows": [],
                "current_bar_count": 0,
                "prior_bar_count": 0,
                "eligible_window_count": 0,
            }
    return aggregates


def _accumulate_lane_rows(
    *,
    aggregate: dict[str, Any],
    current_trade_rows: Sequence[dict[str, Any]],
    current_bar_count: int,
    prior_trade_rows: Sequence[dict[str, Any]],
    prior_bar_count: int,
    eligible: bool = True,
) -> None:
    aggregate["current_trade_rows"].extend(dict(row) for row in current_trade_rows)
    aggregate["prior_trade_rows"].extend(dict(row) for row in prior_trade_rows)
    aggregate["current_bar_count"] = int(aggregate.get("current_bar_count", 0)) + int(current_bar_count)
    aggregate["prior_bar_count"] = int(aggregate.get("prior_bar_count", 0)) + int(prior_bar_count)
    if eligible:
        aggregate["eligible_window_count"] = int(aggregate.get("eligible_window_count", 0)) + 1


def _filter_trade_rows_to_window(
    trade_rows: Sequence[dict[str, Any]],
    *,
    evaluation_start: datetime,
    evaluation_end: datetime,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in trade_rows:
        entry_timestamp = datetime.fromisoformat(str(row["entry_timestamp"]))
        if evaluation_start <= entry_timestamp <= evaluation_end:
            filtered.append(dict(row))
    return filtered


def _count_research_bars_in_window(bars: Sequence[ResearchBar], window: RetestShardWindow) -> int:
    return sum(1 for bar in bars if window.evaluation_start <= bar.end_ts <= window.evaluation_end)


def _count_domain_bars_in_window(bars: Sequence[Bar], window: RetestShardWindow) -> int:
    return sum(1 for bar in bars if window.evaluation_start <= bar.end_ts <= window.evaluation_end)


def _build_atp_candidate_trade_rows(
    *,
    symbol: str,
    candidate,
    bars_1m: Sequence[ResearchBar],
    trade_rows: Sequence[dict[str, Any]],
    point_value: float,
) -> list[dict[str, Any]]:
    candidate_rows: list[dict[str, Any]] = []
    for trade_row in trade_rows:
        window = [bar for bar in bars_1m if trade_row["entry_timestamp"] <= bar.end_ts.isoformat() <= trade_row["exit_timestamp"]]
        add_result = evaluate_promotion_add_candidate(
            trade=trade_row["trade_record"],
            minute_bars=window,
            candidate=candidate,
            point_value=point_value,
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
    return candidate_rows


def _prepare_probationary_playback_bundle(
    *,
    symbol: str,
    base_settings,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> dict[str, Any] | None:
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return None
    cache_key = (
        symbol,
        str(minute_source.sqlite_path),
        minute_source.data_source,
        str(completed_source.sqlite_path) if completed_source is not None else None,
        completed_source.data_source if completed_source is not None else None,
        start_timestamp.isoformat(),
        end_timestamp.isoformat(),
    )
    cached = _PROBATIONARY_PLAYBACK_CACHE.get(cache_key)
    if cached is not None or cache_key in _PROBATIONARY_PLAYBACK_CACHE:
        return cached

    lane = {
        "symbol": symbol,
        "display_name": symbol,
        "long_sources": ["usLatePauseResumeLongTurn"],
        "point_value": str(RESEARCH_CONTRACT_POINT_VALUES.get(symbol, Decimal("1"))),
    }
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=[lane])
    if not definitions:
        _PROBATIONARY_PLAYBACK_CACHE[cache_key] = None
        return None
    runtime_settings = build_runtime_settings(base_settings, definitions[0])

    load_started = perf_counter()
    current_loaded = SQLiteHistoricalBarSource(minute_source.sqlite_path, runtime_settings).load_bars(
        symbol=symbol,
        source_timeframe="1m",
        target_timeframe="1m",
        data_source=minute_source.data_source,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    current_bars = _clip_domain_bars_to_exact_window(
        current_loaded.playback_bars,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    load_elapsed = perf_counter() - load_started

    resample_elapsed = 0.0
    if completed_source is not None and _selection_overlaps_window(
        completed_source,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    ):
        prior_started = perf_counter()
        prior_loaded = SQLiteHistoricalBarSource(completed_source.sqlite_path, runtime_settings).load_bars(
            symbol=symbol,
            source_timeframe="5m",
            target_timeframe="5m",
            data_source=completed_source.data_source,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        prior_elapsed = perf_counter() - prior_started
        load_elapsed += prior_elapsed
    else:
        prior_started = perf_counter()
        prior_loaded = SQLiteHistoricalBarSource(minute_source.sqlite_path, runtime_settings).load_bars(
            symbol=symbol,
            source_timeframe="1m",
            target_timeframe="5m",
            data_source=minute_source.data_source,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        resample_elapsed = perf_counter() - prior_started
    prior_bars = _clip_domain_bars_to_exact_window(
        prior_loaded.playback_bars,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )

    payload = {
        "current_bars": list(current_bars),
        "prior_bars": list(prior_bars),
        "structural_bars_5m": list(prior_bars),
        "load_seconds": round(load_elapsed, 6),
        "resample_seconds": round(resample_elapsed, 6),
    }
    _PROBATIONARY_PLAYBACK_CACHE[cache_key] = payload
    return payload


def _selection_overlaps_window(
    selection: SourceSelection,
    *,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> bool:
    if not selection.start_ts or not selection.end_ts:
        return False
    selection_start = datetime.fromisoformat(selection.start_ts)
    selection_end = datetime.fromisoformat(selection.end_ts)
    return selection_end >= start_timestamp and selection_start <= end_timestamp


def _run_probationary_registry_playback_from_bars(
    *,
    lanes: Sequence[dict[str, Any]],
    base_settings,
    environment_mode: EnvironmentMode,
    bars: Sequence[Bar],
    structural_bars: Sequence[Bar] | None = None,
    target_timeframe: str,
    execution_model_label: str,
) -> dict[str, dict[str, Any]]:
    if not lanes:
        return {}
    execution_bars = list(bars)
    if not execution_bars:
        return {}
    structural_bar_series = list(structural_bars) if structural_bars is not None else list(bars)
    shared_context = _build_probationary_shared_signal_context(
        lanes=lanes,
        base_settings=base_settings,
        structural_bars=structural_bar_series,
    )
    results: dict[str, dict[str, Any]] = {}
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=lanes)
    if not definitions:
        return {}
    by_strategy_id = {definition.standalone_strategy_id: definition for definition in definitions}
    for lane in lanes:
        strategy_id = str(lane["strategy_id"])
        definition = by_strategy_id.get(strategy_id)
        if definition is None:
            continue
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
                "database_url": "sqlite:///:memory:",
            }
        )
        trade_rows = _run_probationary_historical_executor(
            lane=lane,
            settings=runtime_settings,
            point_value=definition.point_value,
            execution_bars=execution_bars,
            shared_context=shared_context,
            execution_model_label=execution_model_label,
        )
        results[strategy_id] = {
            "standalone_strategy_id": strategy_id,
            "trade_rows": trade_rows,
            "bar_count": len(execution_bars),
            "vwap_breakdown": _generic_vwap_breakdown(trade_rows=trade_rows, source_bars=execution_bars),
            "execution_model": execution_model_label,
        }
    return results


def _run_probationary_trigger_detector_from_bars(
    *,
    lanes: Sequence[dict[str, Any]],
    base_settings,
    environment_mode: EnvironmentMode,
    bars: Sequence[Bar],
    target_timeframe: str,
) -> dict[str, dict[str, Any]]:
    if not lanes:
        return {}
    symbol = str(lanes[0].get("symbol") or "").strip().upper()
    long_sources = sorted(
        {
            str(source).strip()
            for lane in lanes
            for source in list(lane.get("long_sources") or [])
            if str(source).strip()
        }
    )
    short_sources = sorted(
        {
            str(source).strip()
            for lane in lanes
            for source in list(lane.get("short_sources") or [])
            if str(source).strip()
        }
    )
    if not long_sources and not short_sources:
        return {}
    detector_lane = {
        "lane_id": f"{symbol.lower()}__probationary_detector",
        "display_name": f"{symbol} / probationary detector",
        "symbol": symbol,
        "long_sources": long_sources,
        "short_sources": short_sources,
        "session_restriction": "",
    }
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=[detector_lane])
    if not definitions:
        return {}
    definition = definitions[0]
    runtime_settings = build_runtime_settings(base_settings, definition).model_copy(
        update={
            "environment_mode": environment_mode,
            "timeframe": "5m",
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "5m",
            "artifact_timeframe": "5m",
            "context_timeframes": ("5m",),
            "execution_timeframe_role": ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION,
            "database_url": "sqlite:///:memory:",
        }
    )
    event_table = _compiled_probationary_family_event_table(
        bars=bars,
        settings=runtime_settings,
    )

    detected: dict[str, dict[str, Any]] = {}
    for lane in lanes:
        lane_sources = {
            *(str(source).strip() for source in list(lane.get("long_sources") or []) if str(source).strip()),
            *(str(source).strip() for source in list(lane.get("short_sources") or []) if str(source).strip()),
        }
        lane_source_events = {
            source: list(event_table.get(source) or [])
            for source in lane_sources
            if event_table.get(source)
        }
        lane_source_counts = {source: len(events) for source, events in lane_source_events.items()}
        detected[str(lane["strategy_id"])] = {
            "triggered": bool(lane_source_counts),
            "signal_count": sum(lane_source_counts.values()),
            "source_counts": lane_source_counts,
            "first_signal_timestamp": min(
                (events[0] for events in lane_source_events.values() if events),
                default=None,
            ),
        }
    return detected


def _compiled_probationary_family_event_table(*, bars: Sequence[Bar], settings) -> dict[str, list[str]]:
    event_table: dict[str, list[str]] = {
        **{source: [] for source in _PROBATIONARY_COMPILED_LONG_SOURCE_KEYS},
        **{source: [] for source in _PROBATIONARY_COMPILED_SHORT_SOURCE_KEYS},
    }
    if not bars:
        return event_table
    history: list[Bar] = []
    feature_history = []
    state = build_initial_state(bars[0].start_ts if bars else datetime.now(UTC))
    feature_computer = IncrementalFeatureComputer(settings)
    for raw_bar in bars:
        if not raw_bar.is_final:
            continue
        bar = classify_sessions(raw_bar, settings)
        feature_packet = feature_computer.compute_next(bar, state)
        history.append(bar)
        feature_history.append(feature_packet)
        bull = evaluate_bull_snap(history, feature_packet, state, settings, feature_history)
        bear = evaluate_bear_snap(history, feature_packet, state, settings, feature_history)
        asia = evaluate_asia_vwap_reclaim(history, feature_history, state, settings)
        long_entry_raw = any(
            bool(bull.get(key)) if key in bull else bool(asia.get(key))
            for key in _PROBATIONARY_LONG_ENTRY_RAW_KEYS
        )
        short_entry_raw = any(bool(bear.get(key)) for key in _PROBATIONARY_SHORT_ENTRY_RAW_KEYS)
        bar_end_ts = bar.end_ts.isoformat()
        for source, key in _PROBATIONARY_COMPILED_LONG_SOURCE_KEYS.items():
            if bull.get(key):
                event_table[source].append(bar_end_ts)
        for source, key in _PROBATIONARY_COMPILED_SHORT_SOURCE_KEYS.items():
            if bear.get(key):
                event_table[source].append(bar_end_ts)
        signal_present = bool(
            long_entry_raw
            or short_entry_raw
            or asia["asia_reclaim_bar_raw"]
            or bull["bull_snap_turn_candidate"]
            or bear["bear_snap_turn_candidate"]
            or bear["derivative_bear_turn_candidate"]
        )
        state = replace(
            state,
            last_swing_low=feature_packet.last_swing_low,
            last_swing_high=feature_packet.last_swing_high,
            asia_reclaim_bar_low=bar.low if asia["asia_reclaim_bar_raw"] else state.asia_reclaim_bar_low,
            asia_reclaim_bar_high=bar.high if asia["asia_reclaim_bar_raw"] else state.asia_reclaim_bar_high,
            asia_reclaim_bar_vwap=feature_packet.vwap if asia["asia_reclaim_bar_raw"] else state.asia_reclaim_bar_vwap,
            bars_since_bull_snap=_next_counter_value(state.bars_since_bull_snap, bull["bull_snap_turn_candidate"]),
            bars_since_bear_snap=_next_counter_value(state.bars_since_bear_snap, bear["bear_snap_turn_candidate"]),
            bars_since_asia_reclaim=_next_counter_value(state.bars_since_asia_reclaim, asia["asia_reclaim_bar_raw"]),
            bars_since_asia_vwap_signal=_next_counter_value(state.bars_since_asia_vwap_signal, asia["asia_vwap_long_signal"]),
            bars_since_long_setup=_next_counter_value(state.bars_since_long_setup, long_entry_raw),
            bars_since_short_setup=_next_counter_value(state.bars_since_short_setup, short_entry_raw),
            last_signal_bar_id=feature_packet.bar_id if signal_present else state.last_signal_bar_id,
            updated_at=bar.end_ts,
        )
    return event_table


def _build_probationary_shared_signal_context(
    *,
    lanes: Sequence[dict[str, Any]],
    base_settings,
    structural_bars: Sequence[Bar],
) -> dict[str, Any]:
    symbol = str(lanes[0].get("symbol") or "").strip().upper()
    union_long_sources = sorted(
        {
            str(source).strip()
            for lane in lanes
            for source in list(lane.get("long_sources") or [])
            if str(source).strip()
        }
    )
    union_short_sources = sorted(
        {
            str(source).strip()
            for lane in lanes
            for source in list(lane.get("short_sources") or [])
            if str(source).strip()
        }
    )
    shared_lane = {
        "lane_id": f"{symbol.lower()}__probationary_shared_executor",
        "display_name": f"{symbol} / probationary shared executor",
        "symbol": symbol,
        "long_sources": union_long_sources,
        "short_sources": union_short_sources,
        "session_restriction": "",
    }
    definitions = build_standalone_strategy_definitions(base_settings, runtime_lanes=[shared_lane])
    definition = definitions[0]
    settings = build_runtime_settings(base_settings, definition).model_copy(
        update={
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "timeframe": "5m",
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "5m",
            "artifact_timeframe": "5m",
            "context_timeframes": ("5m",),
            "execution_timeframe_role": ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION,
        }
    )

    signal_state = transition_to_ready(build_initial_state(structural_bars[0].start_ts), structural_bars[0].start_ts)
    history: list[Bar] = []
    feature_history: list[Any] = []
    feature_computer = IncrementalFeatureComputer(settings)
    signals_by_bar_id: dict[str, SignalPacket] = {}
    features_by_bar_id: dict[str, Any] = {}
    bar_index_by_id: dict[str, int] = {}

    for index, raw_bar in enumerate(structural_bars):
        if not raw_bar.is_final:
            continue
        bar = classify_sessions(raw_bar, settings)
        feature_packet = feature_computer.compute_next(bar, signal_state)
        history.append(bar)
        feature_history.append(feature_packet)
        signal_packet = _build_probationary_signal_packet(
            history=history,
            feature_packet=feature_packet,
            feature_history=feature_history,
            state=signal_state,
            settings=settings,
        )
        signals_by_bar_id[bar.bar_id] = signal_packet
        features_by_bar_id[bar.bar_id] = feature_packet
        bar_index_by_id[bar.bar_id] = index
        signal_state = _advance_probationary_signal_state(
            state=signal_state,
            bar=bar,
            feature_packet=feature_packet,
            signal_packet=signal_packet,
        )

    return {
        "settings": settings,
        "structural_bars": list(structural_bars),
        "signals_by_bar_id": signals_by_bar_id,
        "features_by_bar_id": features_by_bar_id,
        "bar_index_by_id": bar_index_by_id,
    }


def _build_probationary_signal_packet(
    *,
    history: Sequence[Bar],
    feature_packet,
    feature_history: Sequence[Any],
    state,
    settings,
) -> SignalPacket:
    bull = evaluate_bull_snap(history, feature_packet, state, settings, feature_history)
    bear = evaluate_bear_snap(history, feature_packet, state, settings, feature_history)
    asia = evaluate_asia_vwap_reclaim(history, feature_history, state, settings)
    payload = _empty_signal_packet_payload(feature_packet.bar_id)
    payload.update(
        {
            "bull_snap_downside_stretch_ok": bull["bull_snap_downside_stretch_ok"],
            "bull_snap_range_ok": bull["bull_snap_range_ok"],
            "bull_snap_body_ok": bull["bull_snap_body_ok"],
            "bull_snap_close_strong": bull["bull_snap_close_strong"],
            "bull_snap_velocity_ok": bull["bull_snap_velocity_ok"],
            "bull_snap_reversal_bar": bull["bull_snap_reversal_bar"],
            "bull_snap_location_ok": bull["bull_snap_location_ok"],
            "bull_snap_raw": bull["bull_snap_raw"],
            "bull_snap_turn_candidate": bull["bull_snap_turn_candidate"],
            "first_bull_snap_turn": bull["first_bull_snap_turn"],
            "below_vwap_recently": asia["below_vwap_recently"],
            "reclaim_range_ok": asia["reclaim_range_ok"],
            "reclaim_vol_ok": asia["reclaim_vol_ok"],
            "reclaim_color_ok": asia["reclaim_color_ok"],
            "reclaim_close_ok": asia["reclaim_close_ok"],
            "asia_reclaim_bar_raw": asia["asia_reclaim_bar_raw"],
            "asia_hold_bar": asia["asia_hold_bar"],
            "asia_hold_close_vwap_ok": asia["asia_hold_close_vwap_ok"],
            "asia_hold_low_ok": asia["asia_hold_low_ok"],
            "asia_hold_bar_ok": asia["asia_hold_bar_ok"],
            "asia_acceptance_bar": asia["asia_acceptance_bar"],
            "asia_acceptance_close_high_ok": asia["asia_acceptance_close_high_ok"],
            "asia_acceptance_close_vwap_ok": asia["asia_acceptance_close_vwap_ok"],
            "asia_acceptance_bar_ok": asia["asia_acceptance_bar_ok"],
            "asia_vwap_long_signal": asia["asia_vwap_long_signal"],
            "midday_pause_resume_long_turn_candidate": bull["midday_pause_resume_long_turn_candidate"],
            "us_late_breakout_retest_hold_long_turn_candidate": bull["us_late_breakout_retest_hold_long_turn_candidate"],
            "us_late_failed_move_reversal_long_turn_candidate": bull["us_late_failed_move_reversal_long_turn_candidate"],
            "us_late_pause_resume_long_turn_candidate": bull["us_late_pause_resume_long_turn_candidate"],
            "asia_early_breakout_retest_hold_long_turn_candidate": bull["asia_early_breakout_retest_hold_long_turn_candidate"],
            "asia_early_normal_breakout_retest_hold_long_turn_candidate": bull["asia_early_normal_breakout_retest_hold_long_turn_candidate"],
            "asia_late_pause_resume_long_turn_candidate": bull["asia_late_pause_resume_long_turn_candidate"],
            "asia_late_flat_pullback_pause_resume_long_turn_candidate": bull["asia_late_flat_pullback_pause_resume_long_turn_candidate"],
            "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate": bull["asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate"],
            "bear_snap_up_stretch_ok": bear["bear_snap_up_stretch_ok"],
            "bear_snap_range_ok": bear["bear_snap_range_ok"],
            "bear_snap_body_ok": bear["bear_snap_body_ok"],
            "bear_snap_close_weak": bear["bear_snap_close_weak"],
            "bear_snap_velocity_ok": bear["bear_snap_velocity_ok"],
            "bear_snap_reversal_bar": bear["bear_snap_reversal_bar"],
            "bear_snap_location_ok": bear["bear_snap_location_ok"],
            "bear_snap_raw": bear["bear_snap_raw"],
            "bear_snap_turn_candidate": bear["bear_snap_turn_candidate"],
            "first_bear_snap_turn": bear["first_bear_snap_turn"],
            "derivative_bear_slope_ok": bear["derivative_bear_slope_ok"],
            "derivative_bear_curvature_ok": bear["derivative_bear_curvature_ok"],
            "derivative_bear_turn_candidate": bear["derivative_bear_turn_candidate"],
            "derivative_bear_additive_turn_candidate": bear["derivative_bear_additive_turn_candidate"],
            "midday_compressed_failed_move_reversal_short_turn_candidate": bear["midday_compressed_failed_move_reversal_short_turn_candidate"],
            "midday_compressed_rebound_failed_move_reversal_short_turn_candidate": bear["midday_compressed_rebound_failed_move_reversal_short_turn_candidate"],
            "midday_expanded_pause_resume_short_turn_candidate": bear["midday_expanded_pause_resume_short_turn_candidate"],
            "midday_compressed_pause_resume_short_turn_candidate": bear["midday_compressed_pause_resume_short_turn_candidate"],
            "midday_pause_resume_short_turn_candidate": bear["midday_pause_resume_short_turn_candidate"],
            "london_late_pause_resume_short_turn_candidate": bear["london_late_pause_resume_short_turn_candidate"],
            "asia_early_expanded_breakout_retest_hold_short_turn_candidate": bear["asia_early_expanded_breakout_retest_hold_short_turn_candidate"],
            "asia_early_compressed_pause_resume_short_turn_candidate": bear["asia_early_compressed_pause_resume_short_turn_candidate"],
            "asia_early_pause_resume_short_turn_candidate": bear["asia_early_pause_resume_short_turn_candidate"],
        }
    )
    return resolve_entries(SignalPacket(**payload), state, settings)


def _advance_probationary_signal_state(*, state, bar: Bar, feature_packet, signal_packet: SignalPacket):
    return replace(
        state,
        last_swing_low=feature_packet.last_swing_low,
        last_swing_high=feature_packet.last_swing_high,
        asia_reclaim_bar_low=bar.low if signal_packet.asia_reclaim_bar_raw else state.asia_reclaim_bar_low,
        asia_reclaim_bar_high=bar.high if signal_packet.asia_reclaim_bar_raw else state.asia_reclaim_bar_high,
        asia_reclaim_bar_vwap=feature_packet.vwap if signal_packet.asia_reclaim_bar_raw else state.asia_reclaim_bar_vwap,
        bars_since_bull_snap=_next_counter(state.bars_since_bull_snap, signal_packet.bull_snap_turn_candidate),
        bars_since_bear_snap=_next_counter(state.bars_since_bear_snap, signal_packet.bear_snap_turn_candidate),
        bars_since_asia_reclaim=_next_counter(state.bars_since_asia_reclaim, signal_packet.asia_reclaim_bar_raw),
        bars_since_asia_vwap_signal=_next_counter(state.bars_since_asia_vwap_signal, signal_packet.asia_vwap_long_signal),
        bars_since_long_setup=_next_counter(state.bars_since_long_setup, signal_packet.long_entry_raw),
        bars_since_short_setup=_next_counter(state.bars_since_short_setup, signal_packet.short_entry_raw),
        last_signal_bar_id=signal_packet.bar_id if _signal_present(signal_packet) else state.last_signal_bar_id,
        updated_at=bar.end_ts,
    )


def _run_probationary_historical_executor(
    *,
    lane: dict[str, Any],
    settings,
    point_value: Decimal,
    execution_bars: Sequence[Bar],
    shared_context: dict[str, Any],
    execution_model_label: str,
) -> list[dict[str, Any]]:
    if not execution_bars:
        return []
    structural_bars = list(shared_context["structural_bars"])
    signals_by_bar_id = dict(shared_context["signals_by_bar_id"])
    features_by_bar_id = dict(shared_context["features_by_bar_id"])
    structural_index = -1
    order_intent_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    pending: dict[str, Any] | None = None
    use_same_bar_executable_vwap = (
        execution_model_label == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
    )
    state = transition_to_ready(build_initial_state(execution_bars[0].start_ts), execution_bars[0].start_ts)
    latest_structural_signal: SignalPacket | None = None
    latest_structural_feature = None
    current_structural_history: list[Bar] = []

    for execution_bar in execution_bars:
        if pending is not None and str(pending["intent"].bar_id) != str(execution_bar.bar_id):
            fill = FillEvent(
                order_intent_id=pending["intent"].order_intent_id,
                intent_type=pending["intent"].intent_type,
                order_status=OrderStatus.FILLED,
                fill_timestamp=execution_bar.start_ts,
                fill_price=execution_bar.open,
                broker_order_id=pending["intent"].order_intent_id,
                quantity=pending["intent"].quantity,
            )
            fill_rows.append(_fill_row_payload(fill))
            if fill.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
                state = transition_on_entry_fill(
                    state=state,
                    fill_event=fill,
                    signal_bar_id=str(pending["signal_bar_id"]),
                    long_entry_family=pending["long_entry_family"],
                    short_entry_family=pending["short_entry_family"],
                    short_entry_source=pending["short_entry_source"],
                )
            else:
                state = transition_on_exit_fill(state, fill)
            pending = None

        advanced = False
        while structural_index + 1 < len(structural_bars) and structural_bars[structural_index + 1].end_ts <= execution_bar.end_ts:
            structural_index += 1
            structural_bar = structural_bars[structural_index]
            latest_structural_signal = signals_by_bar_id.get(structural_bar.bar_id)
            latest_structural_feature = features_by_bar_id.get(structural_bar.bar_id)
            current_structural_history = structural_bars[: structural_index + 1]
            if latest_structural_signal is not None and latest_structural_feature is not None:
                state = _advance_probationary_signal_state(
                    state=state,
                    bar=structural_bar,
                    feature_packet=latest_structural_feature,
                    signal_packet=latest_structural_signal,
                )
                if state.position_side != PositionSide.FLAT:
                    state = increment_bars_in_trade(state, structural_bar.end_ts)
            advanced = True

        if latest_structural_signal is None or latest_structural_feature is None or not current_structural_history:
            continue

        signal_packet = replace(latest_structural_signal, bar_id=execution_bar.bar_id)
        signal_packet = _apply_probationary_runtime_entry_controls(
            bar=execution_bar,
            signal_packet=signal_packet,
            settings=settings,
        )
        risk_context = compute_risk_context(
            current_structural_history,
            replace(latest_structural_feature, bar_id=execution_bar.bar_id),
            state,
            settings,
        )
        state = replace(
            state,
            long_be_armed=risk_context.long_break_even_armed,
            short_be_armed=risk_context.short_break_even_armed,
            updated_at=execution_bar.end_ts,
        )
        state = update_additive_short_peak_state(
            state,
            execution_bar,
            risk_context,
            settings,
            execution_bar.end_ts,
        )
        exit_decision = evaluate_exits(
            current_structural_history,
            replace(latest_structural_feature, bar_id=execution_bar.bar_id),
            state,
            risk_context,
            settings,
        )
        intent = _maybe_create_probationary_order_intent(
            bar=execution_bar,
            signal_packet=signal_packet,
            state=state,
            exit_decision=exit_decision,
            settings=settings,
            warmup_complete=(structural_index + 1) >= settings.warmup_bars_required(),
        )
        if intent is None:
            continue
        if use_same_bar_executable_vwap:
            immediate_fill = _build_probationary_same_bar_fill(
                intent=intent,
                bar=execution_bar,
                feature_packet=latest_structural_feature,
            )
            if immediate_fill is None:
                continue
            order_intent_rows.append(_order_intent_row_payload(intent))
            fill_rows.append(_fill_row_payload(immediate_fill))
            if immediate_fill.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
                state = transition_on_entry_fill(
                    state=state,
                    fill_event=immediate_fill,
                    signal_bar_id=execution_bar.bar_id,
                    long_entry_family=_resolve_probationary_long_entry_family(signal_packet),
                    short_entry_family=_resolve_probationary_short_entry_family(signal_packet),
                    short_entry_source=(
                        signal_packet.short_entry_source
                        if immediate_fill.intent_type == OrderIntentType.SELL_TO_OPEN
                        else None
                    ),
                )
            else:
                state = transition_on_exit_fill(state, immediate_fill)
            continue
        order_intent_rows.append(_order_intent_row_payload(intent))
        pending = {
            "intent": intent,
            "signal_bar_id": execution_bar.bar_id if intent.is_entry else None,
            "long_entry_family": _resolve_probationary_long_entry_family(signal_packet),
            "short_entry_family": _resolve_probationary_short_entry_family(signal_packet),
            "short_entry_source": signal_packet.short_entry_source if intent.intent_type == OrderIntentType.SELL_TO_OPEN else None,
        }

    ledger = build_trade_ledger(
        order_intent_rows,
        fill_rows,
        build_session_lookup(list(execution_bars)),
        point_value=point_value,
        bars=list(execution_bars),
    )
    return _ledger_rows_to_trade_rows(ledger=ledger, bars=execution_bars)


def _build_probationary_same_bar_fill(
    *,
    intent: OrderIntent,
    bar: Bar,
    feature_packet,
) -> FillEvent | None:
    fill_price = bar.close
    if intent.is_entry:
        side = "LONG" if intent.intent_type == OrderIntentType.BUY_TO_OPEN else "SHORT"
        quality = classify_vwap_price_quality(
                side=side,
                entry_price=float(fill_price),
                bar_vwap=float((bar.high + bar.low + bar.close) / Decimal("3")),
                band_reference=max(
                    float(bar.high - bar.low),
                    float(getattr(feature_packet, "atr", Decimal("0"))),
                    1e-9,
                ),
            )
        if not _probationary_entry_quality_allowed(quality=quality, entry_price=float(fill_price), bar=bar, feature_packet=feature_packet):
            return None
    return FillEvent(
        order_intent_id=intent.order_intent_id,
        intent_type=intent.intent_type,
        order_status=OrderStatus.FILLED,
        fill_timestamp=bar.start_ts,
        fill_price=fill_price,
        broker_order_id=intent.order_intent_id,
        quantity=intent.quantity,
    )


def _probationary_entry_quality_allowed(*, quality: str, entry_price: float, bar: Bar, feature_packet) -> bool:
    if quality == VWAP_CHASE_RISK:
        return False
    if quality != VWAP_NEUTRAL:
        return True
    typical_price = float((bar.high + bar.low + bar.close) / Decimal("3"))
    band_reference = max(
        float(bar.high - bar.low),
        float(getattr(feature_packet, "atr", Decimal("0"))),
        1e-9,
    )
    neutral_band = max(float(band_reference), 1e-9) * 0.10
    return abs(entry_price - typical_price) <= neutral_band * 0.5


def _apply_probationary_runtime_entry_controls(*, bar: Bar, signal_packet: SignalPacket, settings) -> SignalPacket:
    packet = signal_packet
    if packet.long_entry and packet.long_entry_source is not None:
        block_reason = _blocked_probationary_long_entry_reason(bar=bar, source=str(packet.long_entry_source), settings=settings)
        if block_reason is not None:
            packet = replace(packet, long_entry=False, long_entry_source=None)
    if packet.short_entry and packet.short_entry_source is not None:
        block_reason = _blocked_probationary_short_entry_reason(bar=bar, source=str(packet.short_entry_source), settings=settings)
        if block_reason is not None:
            packet = replace(packet, short_entry=False, short_entry_source=None)
    return packet


def _blocked_probationary_long_entry_reason(*, bar: Bar, source: str, settings) -> str | None:
    if (
        settings.us_late_pause_resume_long_exclude_1755_carryover
        and source == "usLatePauseResumeLongTurn"
        and bar.end_ts.astimezone(settings.timezone_info).time().strftime("%H:%M:%S") == "16:55:00"
    ):
        return "us_late_1755_carryover_exclusion"
    if (
        settings.probationary_paper_lane_session_restriction
        and not _gc_mgc_asia_retest_hold_london_open_extension_matches(bar=bar, source=source, timezone_info=settings.timezone_info)
        and not _bar_matches_probationary_session_restriction(bar, settings.probationary_paper_lane_session_restriction, settings.timezone_info)
    ):
        return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
    if settings.probationary_enforce_approved_branches and source not in settings.approved_long_entry_sources:
        return "probationary_long_source_not_allowlisted"
    return None


def _blocked_probationary_short_entry_reason(*, bar: Bar, source: str, settings) -> str | None:
    if (
        settings.probationary_paper_lane_session_restriction
        and not _bar_matches_probationary_session_restriction(bar, settings.probationary_paper_lane_session_restriction, settings.timezone_info)
    ):
        return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
    if settings.probationary_enforce_approved_branches and source not in settings.approved_short_entry_sources:
        return "probationary_short_source_not_allowlisted"
    return None


def _maybe_create_probationary_order_intent(
    *,
    bar: Bar,
    signal_packet: SignalPacket,
    state,
    exit_decision,
    settings,
    warmup_complete: bool,
) -> OrderIntent | None:
    if not warmup_complete:
        return None
    if _probationary_entry_side_allowed("LONG", state, settings) or _probationary_entry_side_allowed("SHORT", state, settings):
        if state.entries_enabled and not state.operator_halt and not state.same_underlying_entry_hold:
            if signal_packet.long_entry and _probationary_entry_side_allowed("LONG", state, settings):
                return OrderIntent(
                    order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
                    bar_id=bar.bar_id,
                    symbol=settings.symbol,
                    intent_type=OrderIntentType.BUY_TO_OPEN,
                    quantity=int(settings.trade_size),
                    created_at=bar.end_ts,
                    reason_code=signal_packet.long_entry_source or "longEntry",
                )
            if signal_packet.short_entry and _probationary_entry_side_allowed("SHORT", state, settings):
                return OrderIntent(
                    order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_OPEN.value}",
                    bar_id=bar.bar_id,
                    symbol=settings.symbol,
                    intent_type=OrderIntentType.SELL_TO_OPEN,
                    quantity=int(settings.trade_size),
                    created_at=bar.end_ts,
                    reason_code=signal_packet.short_entry_source or "shortEntry",
                )
        return None
    if state.position_side == PositionSide.LONG and state.exits_enabled and exit_decision.long_exit:
        return OrderIntent(
            order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_CLOSE.value}",
            bar_id=bar.bar_id,
            symbol=settings.symbol,
            intent_type=OrderIntentType.SELL_TO_CLOSE,
            quantity=state.internal_position_qty,
            created_at=bar.end_ts,
            reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "longExit",
        )
    if state.position_side == PositionSide.SHORT and state.exits_enabled and exit_decision.short_exit:
        return OrderIntent(
            order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_CLOSE.value}",
            bar_id=bar.bar_id,
            symbol=settings.symbol,
            intent_type=OrderIntentType.BUY_TO_CLOSE,
            quantity=state.internal_position_qty,
            created_at=bar.end_ts,
            reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "shortExit",
        )
    return None


def _probationary_warmup_complete(state, settings) -> bool:
    counter = state.bars_since_long_setup if state.bars_since_long_setup is not None else 0
    counter = max(counter, state.bars_since_short_setup if state.bars_since_short_setup is not None else 0)
    return counter >= settings.warmup_bars_required()


def _probationary_entry_side_allowed(side: str, state, settings) -> bool:
    normalized_side = str(side or "").strip().upper()
    if normalized_side not in {"LONG", "SHORT"}:
        return False
    if state.open_broker_order_id is not None:
        return False
    if normalized_side == "LONG" and state.position_side == PositionSide.SHORT:
        return False
    if normalized_side == "SHORT" and state.position_side == PositionSide.LONG:
        return False
    if state.position_side == PositionSide.FLAT:
        return True
    if str(settings.add_direction_policy.value) != "SAME_DIRECTION_ONLY":
        return False
    if normalized_side != state.position_side.value:
        return False
    return _probationary_can_add(state, settings)


def _probationary_can_add(state, settings) -> bool:
    if state.position_side == PositionSide.FLAT:
        return True
    if str(settings.participation_policy.value) == "SINGLE_ENTRY_ONLY":
        return False
    entry_leg_count = len(state.open_entry_legs)
    if entry_leg_count <= 0:
        return False
    if entry_leg_count >= int(settings.max_concurrent_entries):
        return False
    if (entry_leg_count - 1) >= int(settings.max_adds_after_entry):
        return False
    next_quantity = state.internal_position_qty + int(settings.trade_size)
    max_position_quantity = settings.max_position_quantity or (int(settings.trade_size) * int(settings.max_concurrent_entries))
    return next_quantity <= int(max_position_quantity)


def _resolve_probationary_long_entry_family(signal_packet: SignalPacket) -> LongEntryFamily:
    return LongEntryFamily.VWAP if signal_packet.long_entry_source == "asiaVWAPLongSignal" else LongEntryFamily.K


def _resolve_probationary_short_entry_family(signal_packet: SignalPacket) -> ShortEntryFamily:
    source = str(signal_packet.short_entry_source or "")
    if source in {"asiaEarlyCompressedPauseResumeShortTurn", "asiaEarlyPauseResumeShortTurn"}:
        return ShortEntryFamily.ASIA_EARLY_PAUSE_RESUME_SHORT
    if source == "londonLatePauseResumeShortTurn":
        return ShortEntryFamily.LONDON_LATE_PAUSE_RESUME_SHORT
    if source in {"usMiddayCompressedReboundFailedMoveReversalShortTurn", "usMiddayCompressedFailedMoveReversalShortTurn"}:
        return ShortEntryFamily.FAILED_MOVE_REVERSAL_SHORT
    if source in {"usMiddayExpandedPauseResumeShortTurn", "usMiddayCompressedPauseResumeShortTurn", "usMiddayPauseResumeShortTurn"}:
        return ShortEntryFamily.MIDDAY_PAUSE_RESUME_SHORT
    if source == "usDerivativeBearAdditiveTurn":
        return ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE
    if source == "usDerivativeBearTurn":
        return ShortEntryFamily.DERIVATIVE_BEAR
    return ShortEntryFamily.BEAR_SNAP if source == "firstBearSnapTurn" else ShortEntryFamily.NONE


def _order_intent_row_payload(intent: OrderIntent) -> dict[str, Any]:
    return {
        "order_intent_id": intent.order_intent_id,
        "bar_id": intent.bar_id,
        "symbol": intent.symbol,
        "intent_type": intent.intent_type.value,
        "quantity": intent.quantity,
        "created_at": intent.created_at.isoformat(),
        "reason_code": intent.reason_code,
    }


def _fill_row_payload(fill: FillEvent) -> dict[str, Any]:
    return {
        "order_intent_id": fill.order_intent_id,
        "intent_type": fill.intent_type.value,
        "order_status": fill.order_status.value,
        "fill_timestamp": fill.fill_timestamp.isoformat(),
        "fill_price": str(fill.fill_price) if fill.fill_price is not None else None,
        "broker_order_id": fill.broker_order_id,
        "quantity": fill.quantity,
    }


def _next_counter_value(current: int | None, reset: bool) -> int:
    if reset:
        return 0
    return (current if current is not None else 1000) + 1


def _run_probationary_lane_playback_from_bars(
    *,
    lane: dict[str, Any],
    base_settings,
    environment_mode: EnvironmentMode,
    bars: Sequence[Bar],
    target_timeframe: str,
) -> dict[str, Any] | None:
    symbol = str(lane.get("symbol") or "").strip().upper()
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
            "database_url": "sqlite:///:memory:",
        }
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
    for bar in bars:
        strategy_engine.process_bar(bar)
    ledger = build_trade_ledger(
        repositories.order_intents.list_all(),
        repositories.fills.list_all(),
        build_session_lookup(list(bars)),
        point_value=definition.point_value,
        bars=list(bars),
    )
    trade_rows = _ledger_rows_to_trade_rows(ledger=ledger, bars=bars)
    return {
        "standalone_strategy_id": definition.standalone_strategy_id,
        "trade_rows": trade_rows,
        "bar_count": len(bars),
        "vwap_breakdown": _generic_vwap_breakdown(trade_rows=trade_rows, source_bars=bars),
    }


def _finalize_sharded_results(
    *,
    aggregates: dict[str, dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    historical_playback_dir: Path,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    shard_config: RetestShardConfig,
    timing: TimingBreakdown,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    report_rows: list[dict[str, Any]] = []
    study_rows: list[dict[str, Any]] = []
    bars_cache: dict[str, Sequence[Any]] = {}
    for strategy_id, aggregate in sorted(aggregates.items()):
        meta = dict(aggregate["meta"])
        current_rows = sorted(aggregate["current_trade_rows"], key=lambda row: str(row.get("entry_timestamp") or ""))
        prior_rows = sorted(aggregate["prior_trade_rows"], key=lambda row: str(row.get("entry_timestamp") or ""))
        metrics = _summarize_trade_rows(current_rows, bar_count=max(int(aggregate["current_bar_count"]), 1))
        prior_summary = _summarize_trade_rows(prior_rows, bar_count=max(int(aggregate["prior_bar_count"]), 1))

        recommendation = _lane_recommendation(
            meta=meta,
            metrics=metrics,
            prior_summary=prior_summary,
            data_limit_status=str(meta["data_limit_status"]),
        )
        report_row = {
            "strategy_id": strategy_id,
            "display_name": meta["display_name"],
            "status": meta["status"],
            "family": meta["family"],
            "symbol": meta["symbol"],
            "cohort": meta["cohort"],
            "metrics": metrics,
            "prior_method_comparison": prior_summary,
            "material_improvement": _material_improvement(metrics, prior_summary),
            "recommendation": recommendation,
            "data_limit_status": meta["data_limit_status"],
            "artifact_mode": shard_config.artifact_mode,
            "reference_lane": bool(meta.get("reference_lane")),
            "execution_model": meta.get("execution_model"),
            "prior_execution_model": meta.get("prior_execution_model"),
            "eligibility_status": (
                "eligible"
                if int(aggregate.get("eligible_window_count", 0)) > 0
                else "missing_canonical_overlap"
            ),
            "coverage": {
                "raw_market_data": _source_range_payload(bar_source_index.get(meta["symbol"], {}).get("1m")),
                "derived_playback": _trade_range_payload(current_rows),
                "closed_trade_economics": _trade_range_payload(current_rows, field_name="exit_timestamp"),
            },
        }
        normalized = _normalize_result_row(report_row)
        report_rows.append(normalized)
        if not _should_emit_rich_artifact(normalized, shard_config=shard_config):
            continue
        symbol = str(meta["symbol"])
        if symbol not in bars_cache:
            full_context = _load_symbol_context(symbol=symbol, bar_source_index=bar_source_index)
            full_bars = [] if full_context is None else list(full_context["bars_1m"])
            bars_cache[symbol] = _clip_research_bars_to_exact_window(
                full_bars,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        bars_1m = bars_cache[symbol]
        payload = _build_synthetic_strategy_study(
            symbol=symbol,
            study_id=strategy_id,
            display_name=str(meta["display_name"]),
            strategy_family=str(meta["family"]),
            study_mode=str(meta["study_mode"]),
            bars_1m=bars_1m,
            trade_rows=current_rows,
            point_value=Decimal(str(meta["point_value"])),
            candidate_id=meta.get("candidate_id"),
            entry_model=(
                "CURRENT_CANDLE_VWAP"
                if meta.get("lane_type") != "probationary"
                or meta.get("execution_model") == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
                else "REPLAY_NEXT_BAR_OPEN"
            ),
            execution_model_label=str(meta.get("execution_model") or ""),
            pnl_truth_basis=(
                "ENRICHED_EXECUTION_TRUTH"
                if meta.get("lane_type") != "probationary"
                else "HISTORICAL_EXECUTABLE_CLOSE_PROXY_TRUTH"
                if meta.get("execution_model") == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
                else "PERSISTED_RUNTIME_TRUTH"
            ),
            lifecycle_truth_class=(
                "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
                if meta.get("lane_type") != "probationary"
                else "REPLAY_EXECUTABLE_1M_VWAP_GATED"
                if meta.get("execution_model") == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
                else "REPLAY_FILL_NEXT_BAR_OPEN"
            ),
            intrabar_execution_authoritative=(
                meta.get("lane_type") != "probationary"
                or meta.get("execution_model") == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
            ),
            authoritative_intrabar_available=(
                meta.get("lane_type") != "probationary"
                or meta.get("execution_model") == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
            ),
        )
        artifact_started = perf_counter()
        study_path_pair = _write_study_payload(
            payload=payload,
            artifact_prefix=f"historical_playback_{strategy_id}",
            historical_playback_dir=historical_playback_dir,
        )
        timing.artifact_generation_seconds = round(
            timing.artifact_generation_seconds + (perf_counter() - artifact_started),
            6,
        )
        study_rows.append(
            {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "label": str(meta["display_name"]),
                "study_mode": str(meta["study_mode"]),
                "execution_model": str(meta.get("execution_model") or ""),
                "summary_payload": _study_summary_payload(metrics, str(meta["status"])),
                "strategy_study_json_path": str(study_path_pair["json"]),
                "strategy_study_markdown_path": str(study_path_pair["markdown"]),
            }
        )
    return report_rows, study_rows


def _lane_recommendation(
    *,
    meta: dict[str, Any],
    metrics: dict[str, Any],
    prior_summary: dict[str, Any],
    data_limit_status: str,
) -> str:
    lane_type = str(meta.get("lane_type") or "")
    if lane_type == "approved_quant":
        return _approved_quant_recommendation(metrics)
    if lane_type in {"atp_core", "atp_promotion"}:
        if meta.get("candidate_id"):
            return _atp_candidate_recommendation(str(meta["candidate_id"]), metrics, prior_summary)
        return _atp_recommendation(metrics, lane_status=str(meta["status"]))
    return _probationary_bucket(
        current=metrics,
        prior=prior_summary,
        lane_status=str(meta["status"]),
        data_limit_status=data_limit_status,
    )


def _source_range_payload(selection: SourceSelection | None) -> dict[str, Any] | None:
    if selection is None:
        return None
    return {
        "start": selection.start_ts,
        "end": selection.end_ts,
        "data_source": selection.data_source,
        "sqlite_path": str(selection.sqlite_path),
    }


def _trade_range_payload(trade_rows: Sequence[dict[str, Any]], field_name: str = "entry_timestamp") -> dict[str, Any] | None:
    timestamps = sorted(str(row.get(field_name) or "") for row in trade_rows if row.get(field_name))
    if not timestamps:
        return None
    return {"start": timestamps[0], "end": timestamps[-1], "count": len(timestamps)}


def _should_emit_rich_artifact(row: dict[str, Any], *, shard_config: RetestShardConfig) -> bool:
    if shard_config.artifact_mode == "compact_only":
        return False
    if shard_config.artifact_mode == "rich_all":
        return True
    if str(row.get("bucket") or "") in shard_config.rich_artifact_buckets:
        return True
    return bool(row.get("reference_lane") and shard_config.include_reference_lanes)


def _build_validation_slice_comparison(
    *,
    report_dir: Path,
    historical_playback_dir: Path,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
    source_database_paths: Sequence[str | Path] | None,
    bar_source_index: dict[str, dict[str, SourceSelection]],
    shard_config: RetestShardConfig,
) -> dict[str, Any]:
    candidate_starts = [
        datetime.fromisoformat(selection.start_ts)
        for symbol, by_timeframe in bar_source_index.items()
        for timeframe, selection in by_timeframe.items()
        if timeframe == "1m" and selection.start_ts
    ]
    if not candidate_starts:
        return {"status": "not_run"}
    validation_start = max(start_timestamp, min(candidate_starts)) if start_timestamp is not None else min(candidate_starts)
    validation_end = min(
        validation_start + timedelta(days=10),
        end_timestamp if end_timestamp is not None else validation_start + timedelta(days=10),
    )
    legacy_paths = _run_strategy_universe_retest_legacy(
        report_dir=report_dir / "_validation_legacy",
        historical_playback_dir=historical_playback_dir / "_validation_legacy",
        start_timestamp=validation_start,
        end_timestamp=validation_end,
        source_database_paths=source_database_paths,
        preserve_base=False,
    )
    shard_paths = run_strategy_universe_retest(
        report_dir=report_dir / "_validation_sharded",
        historical_playback_dir=historical_playback_dir / "_validation_sharded",
        start_timestamp=validation_start,
        end_timestamp=validation_end,
        source_database_paths=source_database_paths,
        preserve_base=False,
        shard_config=RetestShardConfig(
            shard_months=shard_config.shard_months,
            warmup_days=shard_config.warmup_days,
            artifact_mode="compact_only",
            rich_artifact_buckets=shard_config.rich_artifact_buckets,
            include_reference_lanes=shard_config.include_reference_lanes,
        ),
        include_validation_slice=False,
    )
    legacy_payload = json.loads(Path(legacy_paths["report_json_path"]).read_text(encoding="utf-8"))
    sharded_payload = json.loads(Path(shard_paths["report_json_path"]).read_text(encoding="utf-8"))
    legacy_rows = {row["strategy_id"]: row for row in legacy_payload.get("results") or []}
    sharded_rows = {row["strategy_id"]: row for row in sharded_payload.get("results") or []}
    mismatches: list[dict[str, Any]] = []
    for strategy_id, legacy_row in legacy_rows.items():
        sharded_row = sharded_rows.get(strategy_id)
        if sharded_row is None:
            mismatches.append({"strategy_id": strategy_id, "reason": "missing_in_sharded"})
            continue
        for key in ("trade_count", "net_pnl", "profit_factor", "win_rate"):
            if legacy_row["metrics"].get(key) != sharded_row["metrics"].get(key):
                mismatches.append(
                    {
                        "strategy_id": strategy_id,
                        "metric": key,
                        "legacy": legacy_row["metrics"].get(key),
                        "sharded": sharded_row["metrics"].get(key),
                    }
                )
                break
    return {
        "status": "completed",
        "validation_start": validation_start.isoformat(),
        "validation_end": validation_end.isoformat(),
        "legacy_report_json_path": str(legacy_paths["report_json_path"]),
        "sharded_report_json_path": str(shard_paths["report_json_path"]),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:20],
    }


def _timing_payload(*, timing: TimingBreakdown, shard_count: int) -> dict[str, Any]:
    return {
        "total_wall_seconds": timing.total_wall_seconds,
        "bar_load_seconds": round(timing.load_seconds, 6),
        "resample_seconds": round(timing.resample_seconds, 6),
        "lane_evaluation_seconds": round(timing.lane_evaluation_seconds, 6),
        "artifact_generation_seconds": round(timing.artifact_generation_seconds, 6),
        "detector_seconds": round(timing.detector_seconds, 6),
        "detector_triggered_lane_count": timing.detector_triggered_lane_count,
        "detector_skipped_lane_count": timing.detector_skipped_lane_count,
        "shard_count": shard_count,
        "by_symbol": timing.by_symbol,
        "by_group": timing.by_group,
        "by_shard": timing.by_shard,
    }


def _coverage_summary_payload(results: Sequence[dict[str, Any]], studies: Sequence[dict[str, Any]]) -> dict[str, Any]:
    rich_strategy_ids = {str(study.get("strategy_id") or "") for study in studies}
    eligible = [row for row in results if str(row.get("eligibility_status") or "") == "eligible"]
    missing = [row for row in results if str(row.get("eligibility_status") or "") != "eligible"]
    zero_trade = [row for row in eligible if int((row.get("metrics") or {}).get("trade_count") or 0) == 0]
    nonzero = [row for row in eligible if int((row.get("metrics") or {}).get("trade_count") or 0) > 0]
    compact_ranges = [
        cov["derived_playback"]
        for row in eligible
        for cov in [dict(row.get("coverage") or {})]
        if cov.get("derived_playback")
    ]
    rich_ranges = [
        {
            "strategy_id": row["strategy_id"],
            "start": cov["derived_playback"]["start"],
            "end": cov["derived_playback"]["end"],
        }
        for row in results
        for cov in [dict(row.get("coverage") or {})]
        if row["strategy_id"] in rich_strategy_ids
        and cov.get("derived_playback")
    ]
    return {
        "eligible_lane_count": len(eligible),
        "missing_lane_count": len(missing),
        "zero_trade_lane_count": len(zero_trade),
        "nonzero_trade_lane_count": len(nonzero),
        "rich_study_count": len(studies),
        "compact_output_start": min((item["start"] for item in compact_ranges), default=None),
        "compact_output_end": max((item["end"] for item in compact_ranges), default=None),
        "rich_output_start": min((item["start"] for item in rich_ranges), default=None),
        "rich_output_end": max((item["end"] for item in rich_ranges), default=None),
    }


def _run_atp_retests(
    *,
    report_rows: list[dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    historical_playback_dir: Path,
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
                execution_model_label=EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
            ),
            artifact_prefix=f"historical_playback_{str(lane['study_id'])}",
            historical_playback_dir=historical_playback_dir,
        )
        study_rows.append(
                {
                    "symbol": lane["symbol"],
                    "label": lane["display_name"],
                    "study_mode": lane["study_mode"],
                    "execution_model": EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
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
                "execution_model": EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                "prior_execution_model": EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP,
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
                    execution_model_label=EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                    pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                    lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
                ),
                artifact_prefix=f"historical_playback_{study_id}",
                historical_playback_dir=historical_playback_dir,
            )
            study_rows.append(
                {
                    "symbol": "MGC",
                    "label": f"ATP Companion / {candidate_id}",
                    "study_mode": "research_execution_mode",
                    "execution_model": EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
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
                    "execution_model": EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                    "prior_execution_model": EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP,
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
    loaded_context: dict[str, Any] | None = None,
    include_prior_comparator: bool = True,
    quality_bucket_policy: str | None = None,
    allow_pre_5m_context_participation: bool = False,
    sides: tuple[str, ...] = ("LONG",),
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    loaded = loaded_context or _load_symbol_context(
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if loaded is None:
        return None
    minute_source = bar_source_index.get(symbol, {}).get("1m")
    completed_source = bar_source_index.get(symbol, {}).get("5m")
    if minute_source is None:
        return None
    selected_sources = {
        "1m": {
            "symbol": minute_source.symbol,
            "timeframe": minute_source.timeframe,
            "data_source": minute_source.data_source,
            "sqlite_path": str(minute_source.sqlite_path.resolve()),
            "row_count": minute_source.row_count,
            "start_ts": minute_source.start_ts,
            "end_ts": minute_source.end_ts,
        },
        "5m": None
        if completed_source is None
        else {
            "symbol": completed_source.symbol,
            "timeframe": completed_source.timeframe,
            "data_source": completed_source.data_source,
            "sqlite_path": str(completed_source.sqlite_path.resolve()),
            "row_count": completed_source.row_count,
            "start_ts": completed_source.start_ts,
            "end_ts": completed_source.end_ts,
        },
    }
    bundle_root = _atp_substrate_root()
    source_db = minute_source.sqlite_path.resolve()
    evaluation_start = start_timestamp or loaded["bars_1m"][0].end_ts
    evaluation_end = end_timestamp or loaded["bars_1m"][-1].end_ts

    rolling_features = build_feature_states(bars_5m=loaded["combined_rolling_5m"], bars_1m=loaded["bars_1m"])
    rolling_feature_bundle = ensure_atp_feature_bundle(
        bundle_root=bundle_root,
        source_db=source_db,
        symbol=symbol,
        selected_sources=selected_sources,
        start_timestamp=evaluation_start,
        end_timestamp=evaluation_end,
        feature_scope="rolling_scope",
        feature_rows=rolling_features,
    )
    rolling_scope_bundle = ensure_atp_scope_bundle(
        bundle_root=bundle_root,
        source_db=source_db,
        symbol=symbol,
        selected_sources=selected_sources,
        start_timestamp=evaluation_start,
        end_timestamp=evaluation_end,
        allowed_sessions=tuple(sorted(allowed_sessions)),
        point_value=float(point_value),
        bars_1m=loaded["bars_1m"],
        feature_bundle=rolling_feature_bundle,
        entry_activation_basis=ATP_TIMING_ACTIVATION_ROLLING_5M,
        quality_bucket_policy=quality_bucket_policy,
        allow_pre_5m_context_participation=allow_pre_5m_context_participation,
        sides=sides,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )

    completed_trade_rows: list[dict[str, Any]] = []
    completed_summary = _empty_summary()
    if include_prior_comparator:
        completed_features = build_feature_states(bars_5m=loaded["window_completed_5m"], bars_1m=loaded["bars_1m"])
        completed_feature_bundle = ensure_atp_feature_bundle(
            bundle_root=bundle_root,
            source_db=source_db,
            symbol=symbol,
            selected_sources=selected_sources,
            start_timestamp=evaluation_start,
            end_timestamp=evaluation_end,
            feature_scope="completed_5m_window",
            feature_rows=completed_features,
        )
        completed_scope_bundle = ensure_atp_scope_bundle(
            bundle_root=bundle_root,
            source_db=source_db,
            symbol=symbol,
            selected_sources=selected_sources,
            start_timestamp=evaluation_start,
            end_timestamp=evaluation_end,
            allowed_sessions=tuple(sorted(allowed_sessions)),
            point_value=float(point_value),
            bars_1m=loaded["bars_1m"],
            feature_bundle=completed_feature_bundle,
            entry_activation_basis=ATP_TIMING_ACTIVATION_COMPLETED_5M,
            quality_bucket_policy=quality_bucket_policy,
            allow_pre_5m_context_participation=allow_pre_5m_context_participation,
            sides=sides,
            exit_policy=exit_policy,
            variant_overrides=variant_overrides,
        )
        completed_trade_rows = completed_scope_bundle.trade_rows
        completed_summary = _summarize_trade_rows(completed_trade_rows, bar_count=len(loaded["bars_1m"]))

    return {
        "bars_1m": loaded["bars_1m"],
        "trade_rows": rolling_scope_bundle.trade_rows,
        "prior_trade_rows": completed_trade_rows,
        "summary": _summarize_trade_rows(rolling_scope_bundle.trade_rows, bar_count=len(loaded["bars_1m"])),
        "prior_summary": completed_summary,
    }


def _run_approved_quant_retests(
    *,
    report_rows: list[dict[str, Any]],
    bar_source_index: dict[str, dict[str, SourceSelection]],
    historical_playback_dir: Path,
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
                    execution_model_label=EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                    pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
                    lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
                ),
                artifact_prefix=f"historical_playback_{study_id}",
                historical_playback_dir=historical_playback_dir,
            )
            study_rows.append(
                {
                    "symbol": symbol,
                    "label": f"{spec.lane_name} / {symbol}",
                    "study_mode": "baseline_parity_mode",
                    "execution_model": EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP,
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
                    "execution_model": EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP,
                    "prior_execution_model": EXECUTION_MODEL_APPROVED_QUANT_COMPLETED_5M_RULES,
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
    loaded_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    loaded = loaded_context or _load_symbol_context(
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
        "completed_5m_bars": loaded["window_completed_5m"],
        "trade_rows": trade_rows,
        "prior_trade_rows": prior_trade_rows,
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
                    "Probationary historical back-cast currently uses explicit 5m-context / 1m-stream NEXT_BAR_OPEN "
                    "fills. It is intentionally distinct from ATP executable/VWAP-aware timing."
                ),
                "execution_model": EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN,
                "prior_execution_model": EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN,
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
                    "execution_model": EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN,
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
            "database_url": "sqlite:///:memory:",
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
    clipped_bars = _clip_domain_bars_to_exact_window(
        loaded.playback_bars,
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
    for bar in clipped_bars:
        strategy_engine.process_bar(bar)
    order_intent_rows = repositories.order_intents.list_all()
    fill_rows = repositories.fills.list_all()
    bars = list(clipped_bars)
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
            execution_model_label=(
                EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN
                if environment_mode == EnvironmentMode.RESEARCH_EXECUTION
                else EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN
            ),
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
    bars_by_timestamp = {
        timestamp: bar
        for bar in bars
        for timestamp in (bar.start_ts.isoformat(), bar.end_ts.isoformat())
    }
    rows: list[dict[str, Any]] = []
    for row in ledger:
        entry_bar = bars_by_start_ts.get(row.entry_ts.isoformat())
        exit_bar = bars_by_start_ts.get(row.exit_ts.isoformat())
        entry_timestamp = (entry_bar.end_ts if entry_bar is not None else row.entry_ts).isoformat()
        vwap_quality_state = None
        quality_bar = bars_by_timestamp.get(entry_timestamp)
        if quality_bar is not None:
            vwap_quality_state = classify_vwap_price_quality(
                side=row.direction,
                entry_price=float(row.entry_px),
                bar_vwap=float((quality_bar.high + quality_bar.low + quality_bar.close) / Decimal("3")),
                band_reference=max(float(quality_bar.high - quality_bar.low), 1e-9),
            )
        rows.append(
            {
                "trade_id": str(row.trade_id),
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": (exit_bar.end_ts if exit_bar is not None else row.exit_ts).isoformat(),
                "entry_price": round(float(row.entry_px), 6),
                "exit_price": round(float(row.exit_px), 6),
                "side": row.direction,
                "family": row.setup_family,
                "entry_session_phase": row.entry_session_phase,
                "exit_reason": row.exit_reason,
                "realized_pnl": round(float(row.net_pnl), 6),
                "vwap_price_quality_state": vwap_quality_state,
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
    return load_symbol_context_platform(
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


def _study_summary_payload(summary: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "status": status,
        "trade_count": summary["trade_count"],
        "net_pnl": summary["net_pnl"],
        "profit_factor": summary["profit_factor"],
        "win_rate": summary["win_rate"],
    }


def _clip_research_bars_to_exact_window(
    bars: Sequence[ResearchBar],
    *,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> list[ResearchBar]:
    clipped = list(bars)
    if start_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts >= start_timestamp]
    if end_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts <= end_timestamp]
    return clipped


def _clip_domain_bars_to_exact_window(
    bars: Sequence[Bar],
    *,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> list[Bar]:
    clipped = list(bars)
    if start_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts >= start_timestamp]
    if end_timestamp is not None:
        clipped = [bar for bar in clipped if bar.end_ts <= end_timestamp]
    return clipped


def _sample_bar_indexes(*, total_count: int, max_items: int, anchor_positions: set[int]) -> list[int]:
    if total_count <= 0 or max_items <= 0:
        return []
    if total_count <= max_items:
        return list(range(total_count))

    keep_indexes: set[int] = {0, total_count - 1}
    normalized_anchors = sorted(index for index in anchor_positions if 0 <= index < total_count)
    anchor_capacity = max(max_items - len(keep_indexes), 0)
    if anchor_capacity > 0 and normalized_anchors:
        if len(normalized_anchors) <= anchor_capacity:
            keep_indexes.update(normalized_anchors)
        else:
            for slot in range(anchor_capacity):
                if anchor_capacity == 1:
                    keep_indexes.add(normalized_anchors[0])
                    break
                anchor_index = round(slot * (len(normalized_anchors) - 1) / (anchor_capacity - 1))
                keep_indexes.add(normalized_anchors[anchor_index])

    remaining_capacity = max(max_items - len(keep_indexes), 0)
    if remaining_capacity > 0:
        for slot in range(1, remaining_capacity + 1):
            candidate = round(slot * (total_count - 1) / (remaining_capacity + 1))
            keep_indexes.add(min(max(candidate, 0), total_count - 1))

    return sorted(keep_indexes)[:max_items]


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
    execution_model_label: str,
    pnl_truth_basis: str,
    lifecycle_truth_class: str,
    intrabar_execution_authoritative: bool = True,
    authoritative_intrabar_available: bool = True,
) -> dict[str, Any]:
    entries_by_ts = {str(row["entry_timestamp"]): row for row in trade_rows}
    exits_by_ts = {str(row["exit_timestamp"]): row for row in trade_rows}
    timestamp_to_index = {bar.end_ts.isoformat(): index for index, bar in enumerate(bars_1m)}
    anchor_positions = {
        timestamp_to_index[timestamp]
        for timestamp in tuple(entries_by_ts) + tuple(exits_by_ts)
        if timestamp in timestamp_to_index
    }
    bar_indexes = _sample_bar_indexes(
        total_count=len(bars_1m),
        max_items=4000,
        anchor_positions=anchor_positions,
    )
    execution_indexes = _sample_bar_indexes(
        total_count=len(bars_1m),
        max_items=6000,
        anchor_positions=anchor_positions,
    )
    bar_index_set = set(bar_indexes)
    execution_index_set = set(execution_indexes)
    cumulative_realized = Decimal("0")
    rows: list[dict[str, Any]] = []
    pnl_points: list[dict[str, Any]] = []
    execution_slices: list[dict[str, Any]] = []
    for index, bar in enumerate(bars_1m):
        exit_row = exits_by_ts.get(bar.end_ts.isoformat())
        if exit_row is not None:
            cumulative_realized += Decimal(str(exit_row["realized_pnl"]))
        if index in bar_index_set:
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
        if index in execution_index_set:
            execution_slices.append(
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
        "execution_slices": execution_slices,
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
            "execution_model": execution_model_label,
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
            "series_compaction": {
                "applied": len(rows) != len(bars_1m) or len(execution_slices) != len(bars_1m),
                "bars_original_count": len(bars_1m),
                "bars_compacted_count": len(rows),
                "pnl_points_original_count": len(bars_1m),
                "pnl_points_compacted_count": len(pnl_points),
                "execution_slices_original_count": len(bars_1m),
                "execution_slices_compacted_count": len(execution_slices),
                "trade_events_count": len(trade_events),
                "compaction_policy": "pre_sampled_preserve_edges_and_trade_linked_points",
                "precompacted": True,
            },
        },
    }


def _write_study_payload(*, payload: dict[str, Any], artifact_prefix: str, historical_playback_dir: Path) -> dict[str, Path]:
    json_path = historical_playback_dir / f"{artifact_prefix}.strategy_study.json"
    markdown_path = historical_playback_dir / f"{artifact_prefix}.strategy_study.md"
    meta = dict(payload.get("meta") or {})
    series_compaction = dict(meta.get("series_compaction") or {})
    if not bool(series_compaction.get("precompacted")):
        payload = compact_strategy_study_payload(payload)
    write_strategy_study_json(payload, json_path)
    write_strategy_study_markdown(payload, markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def _write_historical_playback_manifest(
    *,
    studies: Sequence[dict[str, Any]],
    run_stamp: str,
    historical_playback_dir: Path,
    shard_config: RetestShardConfig | None = None,
) -> Path:
    manifest_path = historical_playback_dir / f"historical_playback_{run_stamp}.manifest.json"
    run_timestamp = datetime.now(UTC).isoformat()
    symbol_entries: list[dict[str, Any]] = []
    study_catalog_entries: list[dict[str, Any]] = []
    for study in studies:
        strategy_study_json_path = str(study["strategy_study_json_path"])
        strategy_study_markdown_path = str(study["strategy_study_markdown_path"])
        study_payload = json.loads(Path(strategy_study_json_path).read_text(encoding="utf-8"))
        catalog_entry = build_strategy_study_catalog_entry(
            payload=study_payload,
            run_stamp=run_stamp,
            run_timestamp=run_timestamp,
            manifest_path=str(manifest_path),
            summary_path=None,
            strategy_study_json_path=strategy_study_json_path,
            strategy_study_markdown_path=strategy_study_markdown_path,
            label=str(study.get("label") or study.get("symbol") or "study"),
        )
        symbol_entries.append(
            {
                "symbol": study["symbol"],
                "label": study.get("label"),
                "study_mode": study.get("study_mode"),
                "execution_model": study.get("execution_model") or dict(study_payload.get("meta") or {}).get("execution_model"),
                "summary_path": None,
                "summary_payload": study.get("summary_payload"),
                "strategy_study_json_path": strategy_study_json_path,
                "strategy_study_markdown_path": strategy_study_markdown_path,
                "study_preview": build_strategy_study_preview(study_payload),
                "catalog_entry": catalog_entry,
            }
        )
        study_catalog_entries.append(
            {
                **dict(catalog_entry),
                "local_artifact_paths": {
                    **dict(catalog_entry.get("local_artifact_paths") or {}),
                    "manifest": str(manifest_path),
                    "summary": None,
                    "strategy_study_json": strategy_study_json_path,
                    "strategy_study_markdown": strategy_study_markdown_path,
                },
                "artifact_paths": {
                    **dict(catalog_entry.get("artifact_paths") or {}),
                    "manifest": str(manifest_path),
                    "summary": None,
                    "strategy_study_json": strategy_study_json_path,
                    "strategy_study_markdown": strategy_study_markdown_path,
                },
            }
        )
    payload = {
        "run_stamp": run_stamp,
        "execution_contracts": _execution_contracts_payload(shard_config=shard_config),
        "studies": study_catalog_entries,
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


def _execution_contracts_payload(*, shard_config: RetestShardConfig | None = None) -> dict[str, Any]:
    config = shard_config or RetestShardConfig()
    probationary_current_label = config.probationary_current_execution_model
    probationary_prior_label = config.probationary_prior_execution_model
    if probationary_current_label == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP:
        probationary_current_payload = {
            "label": probationary_current_label,
            "decision_basis": "5m_context_with_1m_execution_stream",
            "fill_policy": "CURRENT_MINUTE_CLOSE_PROXY_WITH_VWAP_QUALITY_GATE",
            "price_quality_gate": ["VWAP_FAVORABLE", "VWAP_NEUTRAL_TIGHT"],
            "blocked_price_quality": ["VWAP_CHASE_RISK"],
            "notes": (
                "Probationary historical back-cast now uses explicit same-minute executable timing with "
                "VWAP discipline. Neutral-quality entries require an additional tight-distance check and "
                "this remains distinct from ATP semantics even though both are executable-timing models."
            ),
        }
    else:
        probationary_current_payload = {
            "label": probationary_current_label,
            "decision_basis": "5m_context_with_1m_execution_stream",
            "fill_policy": "NEXT_BAR_OPEN_ON_1M_EXECUTION_STREAM",
            "notes": "Deprecated probationary comparison truth retained only for controlled A/B measurement.",
        }
    if probationary_prior_label == EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED:
        probationary_prior_payload = {
            "label": probationary_prior_label,
            "decision_basis": "5m_context_with_1m_execution_stream",
            "fill_policy": "NEXT_BAR_OPEN_ON_1M_EXECUTION_STREAM",
            "notes": "Deprecated probationary comparison truth retained only for controlled A/B measurement.",
        }
    else:
        probationary_prior_payload = {
            "label": probationary_prior_label,
            "decision_basis": "completed_bar",
            "fill_policy": "NEXT_BAR_OPEN",
            "notes": "Legacy comparison label for non-probationary paths.",
        }
    return {
        "legacy_benchmark_parity": {
            "label": EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN,
            "decision_basis": "completed_bar",
            "fill_policy": "NEXT_BAR_OPEN",
            "notes": "Preserved for baseline-parity replay comparisons.",
        },
        "atp_companion_current": {
            "label": EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
            "decision_basis": "rolling_5m_context_on_completed_1m",
            "fill_policy": "same_bar_or_current_minute_executable_timing",
            "price_quality_gate": ["VWAP_FAVORABLE", "VWAP_NEUTRAL"],
            "blocked_price_quality": ["VWAP_CHASE_RISK"],
            "notes": "Frozen ATP companion current back-cast semantics.",
        },
        "atp_companion_prior": {
            "label": EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP,
            "decision_basis": "completed_5m_close_with_1m_executable_timing",
            "fill_policy": "same_bar_or_current_minute_executable_timing",
            "price_quality_gate": ["VWAP_FAVORABLE", "VWAP_NEUTRAL"],
            "blocked_price_quality": ["VWAP_CHASE_RISK"],
            "notes": "ATP prior-method comparison in this retest is completed-5m activation, not legacy parity.",
        },
        "approved_quant_current": {
            "label": EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP,
            "decision_basis": "rolling_5m_context_on_completed_1m",
            "fill_policy": "current_minute_close_proxy_with_vwap_quality_gate",
            "price_quality_gate": ["VWAP_FAVORABLE", "VWAP_NEUTRAL"],
            "blocked_price_quality": ["VWAP_CHASE_RISK"],
        },
        "approved_quant_prior": {
            "label": EXECUTION_MODEL_APPROVED_QUANT_COMPLETED_5M_RULES,
            "decision_basis": "completed_5m_rule_evaluation",
            "fill_policy": "lane_defined_legacy_rule_path",
        },
        "probationary_current": probationary_current_payload,
        "probationary_deprecated_comparison": probationary_prior_payload,
    }


def _current_execution_model_label(lane_type: str, *, shard_config: RetestShardConfig | None = None) -> str:
    if lane_type in {"atp_core", "atp_promotion"}:
        return EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP
    if lane_type == "approved_quant":
        return EXECUTION_MODEL_APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP
    if lane_type == "probationary":
        return (shard_config or RetestShardConfig()).probationary_current_execution_model
    return EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN


def _prior_execution_model_label(lane_type: str, *, shard_config: RetestShardConfig | None = None) -> str:
    if lane_type in {"atp_core", "atp_promotion"}:
        return EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP
    if lane_type == "approved_quant":
        return EXECUTION_MODEL_APPROVED_QUANT_COMPLETED_5M_RULES
    if lane_type == "probationary":
        return (shard_config or RetestShardConfig()).probationary_prior_execution_model
    return EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN


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
