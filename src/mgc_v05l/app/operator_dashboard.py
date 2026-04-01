"""Local operator dashboard server backed by real runtime wrappers and artifacts."""

from __future__ import annotations

import csv
import contextvars
import errno
import hashlib
import html
import json
import logging
import math
import os
import re
import sqlite3
import statistics
import subprocess
import tempfile
import threading
from collections import Counter
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import parse_qs, urlparse

from ..config_models import load_data_storage_policy, load_settings_from_files
from .historical_playback import ensure_strategy_study_artifacts
from .strategy_study import normalize_strategy_study_payload
from .replay_reporting import build_summary_metrics, build_trade_ledger
from .session_phase_labels import label_session_phase
from .approved_quant_lanes.dashboard_payloads import load_approved_quant_baselines_snapshot
from .dashboard_registry import build_dashboard_lane_registry
from .experimental_canaries_dashboard_payloads import load_experimental_canaries_snapshot
from .operator_surface import build_operator_surface
from .probationary_runtime import REALIZED_LOSER_SESSION_OVERRIDE_ACTION, submit_probationary_operator_control
from .strategy_analysis import build_strategy_analysis_payload
from .strategy_identity import build_standalone_strategy_identity
from .strategy_runtime_registry import build_standalone_strategy_definitions
from .tracked_paper_strategies import build_tracked_paper_strategies_payload
from ..market_data import (
    SchwabAuthError,
    SchwabOAuthClient,
    SchwabQuoteHttpClient,
    SchwabTokenStore,
    UrllibJsonTransport,
    load_schwab_market_data_config,
)
from ..market_data.schwab_models import HttpRequest
from ..production_link import ProductionLinkActionError, SchwabProductionLinkService

REPO_ROOT = Path(__file__).resolve().parents[3]
ASSET_DIR = Path(__file__).resolve().parent / "dashboard_assets"
DEFAULT_REFRESH_INTERVAL_SECONDS = 15
DEFAULT_POLL_INTERVAL_SECONDS = 30
PAPER_RUNTIME_AUTO_RECOVERY_BACKOFF_SECONDS = 30
PAPER_RUNTIME_AUTO_RECOVERY_SUCCESS_WINDOW_SECONDS = 300
PAPER_RUNTIME_AUTO_RECOVERY_ACTION = "auto-start-paper"
DEFAULT_RUNTIME_SUPERVISOR_RESTART_WINDOW_SECONDS = 900
DEFAULT_RUNTIME_SUPERVISOR_MAX_AUTO_RESTARTS_PER_WINDOW = 3
DEFAULT_RUNTIME_SUPERVISOR_RESTART_BACKOFF_SECONDS = 60
DEFAULT_RUNTIME_SUPERVISOR_RESTART_SUPPRESSION_SECONDS = 900
DEFAULT_RUNTIME_SUPERVISOR_FAILURE_COOLDOWN_SECONDS = 180
MARKET_INDEX_CACHE_SECONDS = 10
TREASURY_CURVE_CACHE_SECONDS = 30
MARKET_INDEX_CONFIG_PATH = REPO_ROOT / "config" / "schwab.local.json"
MARKET_INDEX_LABEL_ORDER = ("DJIA", "SPX", "NDX", "RUT", "GOLD", "VIX")
MARKET_INDEX_DISPLAY_META = {
    "DJIA": {"name": "Dow Jones", "source_type": "cash_index"},
    "SPX": {"name": "S&P 500", "source_type": "cash_index"},
    "NDX": {"name": "Nasdaq 100", "source_type": "cash_index"},
    "RUT": {"name": "Russell 2000", "source_type": "cash_index"},
    "GOLD": {"name": "Gold Futures", "source_type": "future"},
    "VIX": {"name": "VIX", "source_type": "cash_index"},
}
TREASURY_TENOR_ORDER = ("3M", "5Y", "10Y", "30Y")
TREASURY_TENOR_DISPLAY_META = {
    "3M": {"name": "3M", "source_type": "cash_treasury_yield", "source_note": "Direct Schwab source is the 13-week T-bill yield index benchmark."},
    "5Y": {"name": "5Y", "source_type": "cash_treasury_yield", "source_note": "Direct Schwab source is the CBOE 5-year Treasury yield index."},
    "10Y": {"name": "10Y", "source_type": "cash_treasury_yield", "source_note": "Direct Schwab source is the CBOE 10-year Treasury yield index."},
    "30Y": {"name": "30Y", "source_type": "cash_treasury_yield", "source_note": "Direct Schwab source is the CBOE 30-year Treasury yield index."},
}
TREASURY_AUDITED_UNAVAILABLE_TENORS = ("1M", "6M", "1Y", "2Y", "3Y", "7Y", "20Y")
PROMOTED_PAPER_MODEL_SPECS = (
    {"source": "usLatePauseResumeLongTurn", "side": "LONG"},
    {"source": "asiaEarlyNormalBreakoutRetestHoldTurn", "side": "LONG"},
    {"source": "asiaEarlyPauseResumeShortTurn", "side": "SHORT"},
)
PROMOTED_PAPER_MODEL_SIDE_BY_SOURCE = {str(row["source"]): str(row["side"]) for row in PROMOTED_PAPER_MODEL_SPECS}
PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD = 3
PAPER_EXECUTION_CANARY_LANE_MODE = "PAPER_EXECUTION_CANARY"
PAPER_EXECUTION_CANARY_SIGNAL_SOURCE = "paperExecutionCanary"
PAPER_EXECUTION_CANARY_ENTRY_REASON = "paperExecutionCanaryEntry"
PAPER_EXECUTION_CANARY_EXIT_REASON = "paperExecutionCanaryExitNextBar"
ATPE_CANARY_RUNTIME_KIND = "atpe_canary_observer"
ATP_COMPANION_BENCHMARK_RUNTIME_KIND = "atp_companion_benchmark_paper"
GC_MGC_ACCEPTANCE_RUNTIME_KIND = "gc_mgc_london_open_acceptance_temp_paper"
STRATEGY_HISTORY_SESSION_BUCKETS = ("ASIA_EARLY", "ASIA_LATE", "LONDON_OPEN", "LONDON_LATE", "US_MIDDAY", "US_LATE", "UNKNOWN")
_TRANSPORT_LOGGER = logging.getLogger(__name__)
_SNAPSHOT_WARNINGS: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "operator_dashboard_snapshot_warnings",
    default=None,
)
_SESSION_CLOSE_REVIEW_CANONICAL_RE = re.compile(r"^(?P<session_date>\d{4}-\d{2}-\d{2})\.json$")
_SESSION_CLOSE_REVIEW_TIMESTAMPED_RE = re.compile(r"^(?P<session_date>\d{4}-\d{2}-\d{2})_.+\.json$")
_TEMP_PAPER_OVERLAY_SPECS = (
    {
        "overlay_id": "atpe_canary",
        "flag": "--include-atpe-canary",
        "config_path": "config/probationary_pattern_engine_paper_atpe_canary.yaml",
        "runtime_kinds": {ATPE_CANARY_RUNTIME_KIND},
        "lane_id_prefixes": ("atpe_",),
        "source_families": {"active_trend_participation_engine"},
        "label": "ATPE temporary paper canary",
    },
    {
        "overlay_id": "gc_mgc_acceptance",
        "flag": "--include-gc-mgc-acceptance",
        "config_path": "config/probationary_pattern_engine_paper_gc_mgc_acceptance.yaml",
        "runtime_kinds": {GC_MGC_ACCEPTANCE_RUNTIME_KIND},
        "lane_id_prefixes": ("gc_mgc_london_open_acceptance_continuation_long__",),
        "source_families": {"gc_mgc_london_open_acceptance_continuation_long"},
        "label": "GC/MGC London-open temporary paper branch",
    },
)


def _historical_strategy_study_status(
    strategy_study_payload: dict[str, Any] | None,
    *,
    run_loaded: bool,
) -> dict[str, Any]:
    rows = []
    if isinstance(strategy_study_payload, dict):
        rows = list(strategy_study_payload.get("bars") or strategy_study_payload.get("rows") or [])
    summary = dict(strategy_study_payload.get("summary") or {}) if isinstance(strategy_study_payload, dict) else {}
    atp_summary = dict(summary.get("atp_summary") or {}) if isinstance(summary, dict) else {}
    meta = dict(strategy_study_payload.get("meta") or {}) if isinstance(strategy_study_payload, dict) else {}
    timeframe_truth = dict(meta.get("timeframe_truth") or {}) if isinstance(meta, dict) else {}
    artifact_found = strategy_study_payload is not None
    if artifact_found and atp_summary.get("available") is True:
        mode = "ATP_ENHANCED"
    elif artifact_found:
        mode = "LEGACY_ONLY"
    else:
        mode = "NO_DATA"
    return {
        "label": "Replay Strategy Study",
        "replay_only": True,
        "hint": "Available after a replay/historical playback run with strategy-study artifacts.",
        "run_loaded": run_loaded,
        "artifact_found": artifact_found,
        "artifact_row_count": len(rows),
        "base_timeframe": timeframe_truth.get("artifact_timeframe") or meta.get("context_resolution") or (strategy_study_payload.get("timeframe") if isinstance(strategy_study_payload, dict) else None),
        "structural_signal_timeframe": timeframe_truth.get("structural_signal_timeframe") or meta.get("context_resolution"),
        "execution_resolution": timeframe_truth.get("execution_timeframe") or meta.get("execution_resolution"),
        "execution_timeframe_role": timeframe_truth.get("execution_timeframe_role"),
        "study_mode": meta.get("study_mode") or "baseline_parity_mode",
        "atp_timing_available": atp_summary.get("timing_available") is True,
        "mode": mode,
    }


def _historical_playback_study_catalog_payload(items: Sequence[dict[str, Any]]) -> dict[str, Any]:
    strategy_ids = sorted({str(item.get("strategy_id") or "").strip() for item in items if str(item.get("strategy_id") or "").strip()})
    candidate_ids = sorted({str(item.get("candidate_id") or "").strip() for item in items if str(item.get("candidate_id") or "").strip()})
    study_modes = sorted({str(item.get("study_mode") or "").strip() for item in items if str(item.get("study_mode") or "").strip()})
    entry_models = sorted({str(item.get("entry_model") or "").strip() for item in items if str(item.get("entry_model") or "").strip()})
    supported_entry_models = sorted(
        {
            str(model).strip()
            for item in items
            for model in list(item.get("supported_entry_models") or [])
            if str(model).strip()
        }
    )
    pnl_truth_bases = sorted({str(item.get("pnl_truth_basis") or "").strip() for item in items if str(item.get("pnl_truth_basis") or "").strip()})
    lifecycle_truth_classes = sorted(
        {str(item.get("lifecycle_truth_class") or "").strip() for item in items if str(item.get("lifecycle_truth_class") or "").strip()}
    )
    symbols = sorted({str(item.get("symbol") or "").strip() for item in items if str(item.get("symbol") or "").strip()})
    execution_resolutions = sorted({str(item.get("execution_resolution") or "").strip() for item in items if str(item.get("execution_resolution") or "").strip()})
    context_resolutions = sorted({str(item.get("context_resolution") or "").strip() for item in items if str(item.get("context_resolution") or "").strip()})
    return {
        "selected_study_key": items[0].get("study_key") if items else None,
        "items": list(items),
        "facets": {
            "strategy_ids": strategy_ids,
            "candidate_ids": candidate_ids,
            "study_modes": study_modes,
            "entry_models": entry_models,
            "supported_entry_models": supported_entry_models,
            "pnl_truth_bases": pnl_truth_bases,
            "lifecycle_truth_classes": lifecycle_truth_classes,
            "symbols": symbols,
            "context_resolutions": context_resolutions,
            "execution_resolutions": execution_resolutions,
        },
    }


@dataclass(frozen=True)
class DashboardServerInfo:
    host: str
    port: int
    url: str
    pid: int
    started_at: str
    build_stamp: str
    info_file: str | None


class OperatorDashboardService:
    """Aggregate runtime status and execute real operator scripts for the dashboard."""

    def __init__(self, repo_root: Path, server_info: DashboardServerInfo | None = None) -> None:
        self._repo_root = repo_root
        self._dashboard_artifacts_dir = repo_root / "outputs" / "operator_dashboard"
        self._dashboard_artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._build_stamp = dashboard_build_stamp()
        self._server_info = server_info
        self._auth_cache_path = self._dashboard_artifacts_dir / "auth_gate_latest.json"
        self._action_log_path = self._dashboard_artifacts_dir / "action_log.jsonl"
        self._risk_ack_path = self._dashboard_artifacts_dir / "paper_risk_ack.json"
        self._review_state_path = self._dashboard_artifacts_dir / "paper_review_state.json"
        self._session_signoff_path = self._dashboard_artifacts_dir / "paper_session_signoff.json"
        self._carry_forward_path = self._dashboard_artifacts_dir / "paper_carry_forward_state.json"
        self._pre_session_review_path = self._dashboard_artifacts_dir / "paper_pre_session_review.json"
        self._paper_run_starts_path = self._dashboard_artifacts_dir / "paper_run_starts.jsonl"
        self._paper_run_start_blocks_path = self._dashboard_artifacts_dir / "paper_run_start_blocks.jsonl"
        self._paper_current_run_path = self._dashboard_artifacts_dir / "paper_current_run_start.json"
        self._paper_runtime_recovery_path = self._dashboard_artifacts_dir / "paper_runtime_recovery.json"
        self._paper_runtime_supervisor_events_path = self._dashboard_artifacts_dir / "paper_runtime_supervisor_events.jsonl"
        self._paper_continuity_path = self._dashboard_artifacts_dir / "paper_session_continuity.json"
        self._paper_performance_path = self._dashboard_artifacts_dir / "paper_performance_snapshot.json"
        self._paper_strategy_performance_path = self._dashboard_artifacts_dir / "paper_strategy_performance_snapshot.json"
        self._paper_strategy_trade_log_path = self._dashboard_artifacts_dir / "paper_strategy_trade_log_snapshot.json"
        self._paper_strategy_attribution_path = self._dashboard_artifacts_dir / "paper_strategy_attribution_snapshot.json"
        self._paper_signal_intent_fill_audit_path = self._dashboard_artifacts_dir / "paper_signal_intent_fill_audit_snapshot.json"
        self._paper_exit_parity_summary_path = self._dashboard_artifacts_dir / "paper_exit_parity_summary_snapshot.json"
        self._paper_broker_truth_shadow_validation_path = self._dashboard_artifacts_dir / "paper_broker_truth_shadow_validation_snapshot.json"
        self._shadow_live_shadow_summary_path = self._dashboard_artifacts_dir / "shadow_live_shadow_summary_snapshot.json"
        self._shadow_live_strategy_pilot_summary_path = self._dashboard_artifacts_dir / "shadow_live_strategy_pilot_summary_snapshot.json"
        self._signal_selectivity_analysis_path = self._dashboard_artifacts_dir / "signal_selectivity_analysis_snapshot.json"
        self._paper_live_timing_summary_path = self._dashboard_artifacts_dir / "paper_live_timing_summary_snapshot.json"
        self._paper_live_timing_validation_path = self._dashboard_artifacts_dir / "paper_live_timing_validation_snapshot.json"
        self._paper_soak_validation_path = self._dashboard_artifacts_dir / "paper_soak_validation_snapshot.json"
        self._paper_soak_extended_path = self._dashboard_artifacts_dir / "paper_soak_extended_snapshot.json"
        self._paper_soak_unattended_path = self._dashboard_artifacts_dir / "paper_soak_unattended_snapshot.json"
        self._same_underlying_conflict_review_state_path = (
            self._dashboard_artifacts_dir / "same_underlying_conflict_review_state.json"
        )
        self._same_underlying_conflict_review_history_path = (
            self._dashboard_artifacts_dir / "same_underlying_conflict_review_history.jsonl"
        )
        self._same_underlying_conflict_events_path = (
            self._dashboard_artifacts_dir / "same_underlying_conflict_events.jsonl"
        )
        self._paper_history_path = self._dashboard_artifacts_dir / "paper_history_snapshot.json"
        self._paper_session_shape_path = self._dashboard_artifacts_dir / "paper_session_shape_snapshot.json"
        self._paper_session_branch_contribution_path = self._dashboard_artifacts_dir / "paper_session_branch_contribution_snapshot.json"
        self._paper_session_event_timeline_path = self._dashboard_artifacts_dir / "paper_session_event_timeline_snapshot.json"
        self._paper_readiness_path = self._dashboard_artifacts_dir / "paper_readiness_snapshot.json"
        self._paper_approved_models_path = self._dashboard_artifacts_dir / "paper_approved_models_snapshot.json"
        self._paper_non_approved_lanes_path = self._dashboard_artifacts_dir / "paper_non_approved_lanes_snapshot.json"
        self._paper_temporary_paper_strategies_path = (
            self._dashboard_artifacts_dir / "paper_temporary_paper_strategies_snapshot.json"
        )
        self._paper_tracked_strategies_path = self._dashboard_artifacts_dir / "paper_tracked_strategies_snapshot.json"
        self._paper_tracked_strategy_details_path = (
            self._dashboard_artifacts_dir / "paper_tracked_strategy_details_snapshot.json"
        )
        self._strategy_analysis_path = self._dashboard_artifacts_dir / "strategy_analysis_snapshot.json"
        self._paper_temporary_paper_runtime_integrity_path = (
            self._dashboard_artifacts_dir / "paper_temporary_paper_runtime_integrity_snapshot.json"
        )
        self._paper_lane_activity_path = self._dashboard_artifacts_dir / "paper_lane_activity_snapshot.json"
        self._paper_exceptions_path = self._dashboard_artifacts_dir / "paper_exceptions_snapshot.json"
        self._paper_soak_session_path = self._dashboard_artifacts_dir / "paper_soak_session_snapshot.json"
        self._paper_soak_evidence_dir = self._dashboard_artifacts_dir / "paper_soak_evidence"
        self._paper_soak_evidence_dir.mkdir(parents=True, exist_ok=True)
        self._paper_soak_evidence_latest_json_path = self._dashboard_artifacts_dir / "paper_soak_evidence_latest.json"
        self._paper_soak_evidence_latest_md_path = self._dashboard_artifacts_dir / "paper_soak_evidence_latest.md"
        self._paper_session_close_review_dir = self._dashboard_artifacts_dir / "paper_session_close_reviews"
        self._paper_session_close_review_dir.mkdir(parents=True, exist_ok=True)
        self._paper_session_lane_history_dir = self._dashboard_artifacts_dir / "paper_session_lane_history"
        self._paper_session_lane_history_dir.mkdir(parents=True, exist_ok=True)
        self._paper_session_close_review_latest_json_path = self._dashboard_artifacts_dir / "paper_session_close_review_latest.json"
        self._paper_session_close_review_latest_md_path = self._dashboard_artifacts_dir / "paper_session_close_review_latest.md"
        self._paper_session_close_review_history_json_path = self._paper_session_close_review_dir / "history_index.json"
        self._paper_session_close_review_history_md_path = self._paper_session_close_review_dir / "history_index.md"
        self._paper_latest_fills_path = self._dashboard_artifacts_dir / "paper_latest_fills_snapshot.json"
        self._paper_latest_intents_path = self._dashboard_artifacts_dir / "paper_latest_intents_snapshot.json"
        self._paper_latest_blotter_path = self._dashboard_artifacts_dir / "paper_latest_blotter_snapshot.json"
        self._paper_position_state_path = self._dashboard_artifacts_dir / "paper_position_state_snapshot.json"
        self._historical_playback_dir = self._repo_root / "outputs" / "historical_playback"
        self._historical_playback_snapshot_path = self._dashboard_artifacts_dir / "historical_playback_snapshot.json"
        self._production_link_snapshot_path = self._dashboard_artifacts_dir / "production_link_snapshot.json"
        self._approved_quant_baselines_path = self._repo_root / "outputs" / "probationary_quant_baselines" / "approved_quant_baselines_snapshot.json"
        self._approved_quant_current_status_path = self._repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.json"
        self._approved_quant_current_status_md_path = self._repo_root / "outputs" / "probationary_quant_baselines" / "current_active_baseline_status.md"
        self._experimental_canaries_dir = self._repo_root / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine"
        self._experimental_canaries_snapshot_path = self._experimental_canaries_dir / "experimental_canaries_snapshot.json"
        self._experimental_canaries_snapshot_md_path = self._experimental_canaries_dir / "experimental_canaries_snapshot.md"
        self._experimental_canaries_operator_summary_path = self._experimental_canaries_dir / "operator_summary.md"
        self._operator_surface_path = self._dashboard_artifacts_dir / "operator_surface_snapshot.json"
        self._research_daily_capture_status_path = self._dashboard_artifacts_dir / "research_daily_capture_status.json"
        self._market_index_strip_path = self._dashboard_artifacts_dir / "market_index_strip_snapshot.json"
        self._market_index_diagnostics_path = self._dashboard_artifacts_dir / "market_index_strip_diagnostics.json"
        self._treasury_curve_path = self._dashboard_artifacts_dir / "treasury_curve_snapshot.json"
        self._treasury_curve_diagnostics_path = self._dashboard_artifacts_dir / "treasury_curve_diagnostics.json"
        self._treasury_symbol_audit_path = self._dashboard_artifacts_dir / "treasury_symbol_audit.json"
        self._market_index_cache: dict[str, Any] | None = None
        self._market_index_cache_at: datetime | None = None
        self._treasury_curve_cache: dict[str, Any] | None = None
        self._treasury_curve_cache_at: datetime | None = None
        self._dashboard_probe_lock = threading.Lock()
        self._snapshot_lock = threading.RLock()
        self._dashboard_probe: dict[str, Any] = {
            "state": "not_ready",
            "ready": False,
            "operator_surface_loadable": False,
            "api_dashboard_responding": False,
            "generated_at": None,
            "checked_at": None,
            "error": None,
        }
        self._data_policy = load_data_storage_policy(self._repo_root)
        self._research_history_database_path = self._data_policy.resolve_path(self._data_policy.storage_layout.runtime_replay_database_path)
        self._research_daily_capture_latest_path = (
            self._data_policy.resolve_path(self._data_policy.storage_layout.research_root) / "daily_capture" / "latest.json"
        )
        self._production_link_service = SchwabProductionLinkService(repo_root)

    def _hydrate_paper_dashboard_context(self, paper: dict[str, Any]) -> dict[str, Any]:
        review_payload = self._review_payload(paper, "paper")
        risk_state = self._paper_risk_state(paper, review_payload)
        closeout_state = self._paper_closeout_state(paper, review_payload, risk_state)
        carry_forward = self._paper_carry_forward_state(paper, review_payload)
        pre_session_review = self._paper_pre_session_review_state(carry_forward)
        paper_run_start = self._paper_run_start_state()
        paper_continuity = self._paper_session_continuity(
            paper,
            review_payload,
            carry_forward,
            pre_session_review,
            paper_run_start,
        )
        paper_session_event_timeline = self._paper_session_event_timeline_payload(
            paper=paper,
            review_payload=review_payload,
            risk_state=risk_state,
            carry_forward=carry_forward,
            pre_session_review=pre_session_review,
        )
        paper["experimental_canaries"] = load_experimental_canaries_snapshot(self._experimental_canaries_snapshot_path)
        paper["approved_models"] = self._paper_approved_models_payload(paper)
        paper["non_approved_lanes"] = self._paper_non_approved_lanes_payload(paper)
        paper["lane_activity"] = self._paper_lane_activity_payload(paper)
        paper["activity_proof"] = self._paper_activity_proof_payload(paper)
        paper["readiness"] = self._paper_readiness_payload(paper)
        paper["exceptions"] = self._paper_exceptions_payload(paper, review_payload)
        paper["entry_eligibility"] = self._paper_entry_eligibility_payload(paper, pre_session_review)
        paper["soak_session"] = self._paper_soak_session_payload(paper, review_payload)
        paper_session_close_review = self._paper_session_close_review_payload(
            paper=paper,
            review_payload=review_payload,
            closeout_state=closeout_state,
        )
        paper_session_close_review = self._paper_session_close_review_with_history(paper_session_close_review)
        return {
            "paper": paper,
            "review_payload": review_payload,
            "risk_state": risk_state,
            "closeout_state": closeout_state,
            "carry_forward": carry_forward,
            "pre_session_review": pre_session_review,
            "paper_run_start": paper_run_start,
            "paper_continuity": paper_continuity,
            "paper_session_event_timeline": paper_session_event_timeline,
            "paper_session_close_review": paper_session_close_review,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._snapshot_lock:
            warning_token = _SNAPSHOT_WARNINGS.set([])
            try:
                generated_at = datetime.now(timezone.utc).isoformat()
                shadow = self._runtime_snapshot("shadow")
                paper = self._runtime_snapshot("paper")
                historical_playback = self._historical_playback_payload()
                auth_status = self._load_or_refresh_auth_gate_result(run_if_missing=True)

                review = {
                    "shadow": self._review_payload(shadow, "shadow"),
                }
                paper_context = self._hydrate_paper_dashboard_context(paper)
                pre_recovery_context = paper_context
                review["paper"] = paper_context["review_payload"]
                runtime_recovery_payload, refreshed_paper, auto_recovery_result = self._paper_runtime_recovery_payload(
                    paper=paper_context["paper"],
                    auth_status=auth_status,
                    carry_forward=paper_context["carry_forward"],
                    pre_session_review=paper_context["pre_session_review"],
                    closeout_state=paper_context["closeout_state"],
                )
                if refreshed_paper is not None:
                    paper_context = self._hydrate_paper_dashboard_context(refreshed_paper)
                    review["paper"] = paper_context["review_payload"]
                if auto_recovery_result is not None:
                    pre_snapshot_context = self._paper_runtime_recovery_snapshot_context(
                        paper=paper,
                        carry_forward=pre_recovery_context["carry_forward"],
                        pre_session_review=pre_recovery_context["pre_session_review"],
                        closeout_state=pre_recovery_context["closeout_state"],
                    )
                    if runtime_recovery_payload.get("status") == "AUTO_RESTART_SUCCEEDED":
                        post_snapshot_context = self._paper_runtime_recovery_snapshot_context(
                            paper=paper_context["paper"],
                            carry_forward=paper_context["carry_forward"],
                            pre_session_review=paper_context["pre_session_review"],
                            closeout_state=paper_context["closeout_state"],
                        )
                        self._record_paper_start_success(pre_snapshot_context, post_snapshot_context, auto_recovery_result)
                    else:
                        self._record_paper_start_block(pre_snapshot_context, auto_recovery_result)
                    self._log_action(auto_recovery_result)
                    paper_context["paper_run_start"] = self._paper_run_start_state()
                    paper_context["paper_continuity"] = self._paper_session_continuity(
                        paper_context["paper"],
                        paper_context["review_payload"],
                        paper_context["carry_forward"],
                        paper_context["pre_session_review"],
                        paper_context["paper_run_start"],
                    )
                paper = paper_context["paper"]
                active_runtime = self._active_runtime(paper, shadow)
                active_status = active_runtime["status"]
                paper["runtime_recovery"] = runtime_recovery_payload
                run_started_at = _parse_iso_datetime((paper_context["paper_run_start"].get("current") or {}).get("timestamp"))
                runtime_uptime_seconds = (
                    max(int((datetime.now(timezone.utc) - run_started_at.astimezone(timezone.utc)).total_seconds()), 0)
                    if paper.get("running") and run_started_at is not None
                    else None
                )
                paper["soak_continuity"] = {
                    "runtime_uptime_seconds": runtime_uptime_seconds,
                    "current_runtime_started_at": (paper_context["paper_run_start"].get("current") or {}).get("timestamp"),
                    "last_restart_time": runtime_recovery_payload.get("last_restart_attempt_at") or runtime_recovery_payload.get("attempted_at"),
                    "restart_count_window": runtime_recovery_payload.get("restart_attempts_in_window"),
                    "restart_budget_window": runtime_recovery_payload.get("max_auto_restarts_per_window"),
                    "last_restore_result": active_status.get("startup_restore_result"),
                    "last_restore_completed_at": active_status.get("last_restore_completed_at"),
                    "restore_unresolved_issue": active_status.get("restore_unresolved_issue"),
                    "healthy_soak": bool(
                        paper.get("running")
                        and active_status.get("health_status") == "HEALTHY"
                        and not active_status.get("restore_unresolved_issue")
                        and runtime_recovery_payload.get("manual_action_required") is not True
                    ),
                }
                paper["live_timing_summary"] = self._paper_live_timing_summary_payload(paper)
                paper["live_timing_validation"] = self._paper_live_timing_validation_payload(paper)
                paper["broker_truth_shadow_validation"] = self._paper_broker_truth_shadow_validation_payload(paper)
                paper["soak_validation"] = self._paper_soak_validation_payload(paper)
                paper["soak_extended"] = self._paper_soak_extended_payload(paper)
                paper["soak_unattended"] = self._paper_soak_unattended_payload(paper)
                risk_state = paper_context["risk_state"]
                closeout_state = paper_context["closeout_state"]
                carry_forward = paper_context["carry_forward"]
                pre_session_review = paper_context["pre_session_review"]
                paper_run_start = paper_context["paper_run_start"]
                paper_continuity = paper_context["paper_continuity"]
                paper_session_event_timeline = paper_context["paper_session_event_timeline"]
                paper_session_close_review = paper_context["paper_session_close_review"]
                market_context = self._market_index_strip_payload()
                market_context["debug"] = self._market_index_debug_payload(market_context)
                treasury_curve = self._treasury_curve_payload()
                approved_quant_baselines = load_approved_quant_baselines_snapshot(self._approved_quant_baselines_path)
                approved_quant_baselines.setdefault("artifacts", {})
                approved_quant_baselines["artifacts"].update(
                    {
                        "snapshot": "/api/operator-artifact/approved-quant-baselines",
                        "current_status_json": "/api/operator-artifact/approved-quant-baselines-current-status",
                        "current_status_markdown": "/api/operator-artifact/approved-quant-baselines-current-status-md",
                    }
                )
                lane_registry = build_dashboard_lane_registry(
                    approved_quant_baselines=approved_quant_baselines,
                    paper_approved_models=paper["approved_models"],
                    paper_non_approved_lanes=paper["non_approved_lanes"],
                )
                runtime_lookup: dict[str, dict[str, Any]] = {}
                for row in ((paper.get("runtime_registry") or {}).get("rows") or []):
                    standalone_strategy_id = str(row.get("standalone_strategy_id") or "").strip()
                    lane_id = str(row.get("lane_id") or "").strip()
                    if standalone_strategy_id:
                        runtime_lookup[standalone_strategy_id] = row
                    if lane_id:
                        runtime_lookup[lane_id] = row
                lane_registry["rows"] = [
                    _annotate_runtime_identity_state(row, runtime_lookup)
                    for row in list(lane_registry.get("rows") or [])
                ]
                for section in lane_registry.get("sections", []):
                    section["rows"] = [
                        _annotate_runtime_identity_state(row, runtime_lookup)
                        for row in list(section.get("rows") or [])
                    ]
                paper["approved_models"] = self._align_approved_models_to_lane_registry(
                    payload=paper["approved_models"],
                    lane_registry=lane_registry,
                )
                paper["non_approved_lanes"] = self._align_non_approved_lanes_to_lane_registry(
                    payload=paper["non_approved_lanes"],
                    lane_registry=lane_registry,
                )
                paper["temporary_paper_strategies"] = self._paper_temporary_paper_strategies_payload(
                    paper["non_approved_lanes"]
                )
                paper["temporary_paper_runtime_integrity"] = self._paper_temporary_paper_runtime_integrity_payload(paper)
                temporary_paper_trade_log = _temporary_paper_trade_log_rows(
                    non_approved_lanes=paper["non_approved_lanes"]
                )
                if temporary_paper_trade_log:
                    strategy_performance = dict(paper.get("strategy_performance") or {})
                    merged_trade_log = list(strategy_performance.get("trade_log") or [])
                    merged_trade_log.extend(temporary_paper_trade_log)
                    deduped_trade_log: dict[str, dict[str, Any]] = {}
                    for row in merged_trade_log:
                        row_id = str(row.get("id") or row.get("trade_id") or len(deduped_trade_log))
                        existing = deduped_trade_log.get(row_id)
                        if existing is None:
                            deduped_trade_log[row_id] = row
                            continue
                        deduped_trade_log[row_id] = {
                            **row,
                            **existing,
                            "paper_strategy_class": existing.get("paper_strategy_class") or row.get("paper_strategy_class"),
                            "metrics_bucket": existing.get("metrics_bucket") or row.get("metrics_bucket"),
                            "runtime_instance_present": existing.get("runtime_instance_present", row.get("runtime_instance_present")),
                            "runtime_state_loaded": existing.get("runtime_state_loaded", row.get("runtime_state_loaded")),
                        }
                    merged_trade_log = sorted(
                        deduped_trade_log.values(),
                        key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""),
                        reverse=True,
                    )
                    strategy_performance["trade_log"] = merged_trade_log
                    strategy_performance["attribution"] = _build_strategy_attribution_payload(merged_trade_log)
                    notes = list(strategy_performance.get("notes") or [])
                    extra_note = (
                        "Experimental temporary paper strategy trades are included in the closed-trade ledger for operator visibility, "
                        "while still remaining separate from approved/admitted metrics buckets."
                    )
                    if extra_note not in notes:
                        notes.append(extra_note)
                    strategy_performance["notes"] = notes
                    strategy_performance["trade_log_scope"] = (
                        "Closed trades paired from persisted lane-local intents and fills across approved/admitted paper lanes plus experimental temporary paper strategies."
                    )
                    paper["strategy_performance"] = strategy_performance
                paper["tracked_strategies"] = build_tracked_paper_strategies_payload(
                    repo_root=self._repo_root,
                    paper=paper,
                    generated_at=generated_at,
                )
                lane_registry["diagnostics"] = self._lane_registry_diagnostics(
                    lane_registry=lane_registry,
                    approved_quant_baselines=approved_quant_baselines,
                    paper_approved_models=paper["approved_models"],
                    paper_non_approved_lanes=paper["non_approved_lanes"],
                )
                operator_surface = build_operator_surface(
                    generated_at=generated_at,
                    global_payload={
                        "paper_label": "RUNNING" if paper["running"] else "STOPPED",
                        "current_session_date": active_status["session_date"],
                        "market_data_label": active_status["market_data_semantics"],
                        "runtime_health_label": active_status["health_status"],
                        "fault_state": active_status["fault_state"],
                        "desk_clean_label": "DESK GUARDED" if carry_forward["active"] else "DESK CLEAN",
                        "paper_run_ready_label": pre_session_review["readiness_label"],
                        "last_processed_bar_timestamp": active_status["last_processed_bar_end_ts"],
                        "last_update_timestamp": active_status["last_update_ts"],
                    },
                    auth_status=auth_status,
                    paper=paper,
                    approved_quant_baselines=approved_quant_baselines,
                    market_context=market_context,
                    treasury_curve=treasury_curve,
                )
                research_capture = self._research_daily_capture_payload(generated_at=generated_at)
                paper["signal_intent_fill_audit"] = self._paper_signal_intent_fill_audit_payload(
                    paper={
                        **paper,
                        "operator_surface": operator_surface,
                        "approved_quant_baselines": approved_quant_baselines,
                    },
                    session_date=str(paper.get("status", {}).get("session_date") or ""),
                    root_db_path=_path_or_none(paper.get("db_path")),
                )
                paper["exit_parity_summary"] = self._paper_exit_parity_summary_payload(paper)
                shadow["live_shadow_summary"] = self._shadow_live_shadow_summary_payload(shadow)
                shadow["live_strategy_pilot_summary"] = self._shadow_live_strategy_pilot_summary_payload(shadow)
                shadow["signal_selectivity_analysis"] = self._signal_selectivity_analysis_payload(shadow)
                paper["session_event_timeline"] = paper_session_event_timeline
                _write_json_file(self._review_state_path, closeout_state)
                _write_json_file(self._paper_continuity_path, paper_continuity)
                _write_json_file(self._paper_performance_path, paper["performance"])
                _write_json_file(self._paper_strategy_performance_path, paper["strategy_performance"])
                _write_json_file(
                    self._paper_strategy_trade_log_path,
                    {
                        "rows": paper["strategy_performance"].get("trade_log") or [],
                        "scope": paper["strategy_performance"].get("trade_log_scope"),
                        "notes": paper["strategy_performance"].get("trade_log_notes") or [],
                    },
                )
                _write_json_file(self._paper_strategy_attribution_path, paper["strategy_performance"].get("attribution") or {})
                _write_json_file(self._paper_signal_intent_fill_audit_path, paper.get("signal_intent_fill_audit") or {})
                _write_json_file(self._paper_exit_parity_summary_path, paper.get("exit_parity_summary") or {})
                _write_json_file(self._paper_broker_truth_shadow_validation_path, paper.get("broker_truth_shadow_validation") or {})
                _write_json_file(self._shadow_live_shadow_summary_path, shadow.get("live_shadow_summary") or {})
                _write_json_file(self._shadow_live_strategy_pilot_summary_path, shadow.get("live_strategy_pilot_summary") or {})
                _write_json_file(self._signal_selectivity_analysis_path, shadow.get("signal_selectivity_analysis") or {})
                _write_json_file(self._paper_live_timing_summary_path, paper.get("live_timing_summary") or {})
                _write_json_file(self._paper_live_timing_validation_path, paper.get("live_timing_validation") or {})
                _write_json_file(self._paper_soak_validation_path, paper.get("soak_validation") or {})
                _write_json_file(self._paper_soak_extended_path, paper.get("soak_extended") or {})
                _write_json_file(self._paper_soak_unattended_path, paper.get("soak_unattended") or {})
                _write_json_file(self._paper_history_path, paper["history"])
                _write_json_file(self._paper_session_shape_path, paper["session_shape"])
                _write_json_file(self._paper_session_branch_contribution_path, paper["branch_session_contribution"])
                _write_json_file(self._paper_session_event_timeline_path, paper["session_event_timeline"])
                _write_json_file(self._paper_readiness_path, paper["readiness"])
                _write_json_file(self._paper_approved_models_path, paper["approved_models"])
                _write_json_file(self._paper_non_approved_lanes_path, paper["non_approved_lanes"])
                _write_json_file(self._paper_temporary_paper_strategies_path, paper["temporary_paper_strategies"])
                _write_json_file(self._paper_tracked_strategies_path, paper["tracked_strategies"])
                _write_json_file(
                    self._paper_tracked_strategy_details_path,
                    {
                        "generated_at": paper["tracked_strategies"].get("generated_at"),
                        "default_strategy_id": paper["tracked_strategies"].get("default_strategy_id"),
                        "details_by_strategy_id": paper["tracked_strategies"].get("details_by_strategy_id") or {},
                    },
                )
                _write_json_file(self._paper_temporary_paper_runtime_integrity_path, paper["temporary_paper_runtime_integrity"])
                _write_json_file(self._paper_lane_activity_path, paper["lane_activity"])
                _write_json_file(self._paper_exceptions_path, paper["exceptions"])
                _write_json_file(self._paper_soak_session_path, paper["soak_session"])
                _write_json_file(self._operator_surface_path, operator_surface)
                _write_json_file(self._research_daily_capture_status_path, research_capture)
                _write_json_file(self._paper_session_close_review_latest_json_path, paper_session_close_review)
                _atomic_write_text(
                    self._paper_session_close_review_latest_md_path,
                    _paper_session_close_review_markdown(paper_session_close_review),
                )
                session_close_archive_json_path = self._paper_session_close_review_dir / f"{paper_session_close_review['session_date']}.json"
                session_close_archive_md_path = self._paper_session_close_review_dir / f"{paper_session_close_review['session_date']}.md"
                archive_timestamp_slug = _safe_archive_timestamp_slug(str(paper_session_close_review.get("generated_at") or generated_at))
                session_close_timestamped_archive_json_path = (
                    self._paper_session_close_review_dir / f"{paper_session_close_review['session_date']}_{archive_timestamp_slug}.json"
                )
                session_close_timestamped_archive_md_path = (
                    self._paper_session_close_review_dir / f"{paper_session_close_review['session_date']}_{archive_timestamp_slug}.md"
                )
                _write_json_file(session_close_archive_json_path, paper_session_close_review)
                _atomic_write_text(session_close_archive_md_path, _paper_session_close_review_markdown(paper_session_close_review))
                _write_json_file(session_close_timestamped_archive_json_path, paper_session_close_review)
                _atomic_write_text(
                    session_close_timestamped_archive_md_path,
                    _paper_session_close_review_markdown(paper_session_close_review),
                )
                _write_json_file(self._paper_session_close_review_history_json_path, paper_session_close_review.get("history_summary") or {})
                _atomic_write_text(
                    self._paper_session_close_review_history_md_path,
                    _paper_session_close_review_history_markdown(paper_session_close_review.get("history_summary") or {}),
                )
                _write_json_file(self._paper_latest_fills_path, {"rows": paper["latest_fills"]})
                _write_json_file(self._paper_latest_intents_path, {"rows": paper["latest_intents"]})
                _write_json_file(
                    self._paper_latest_blotter_path,
                    {"rows": paper["latest_blotter_rows"], "blotter_path": paper["blotter_path"]},
                )
                _write_json_file(self._paper_position_state_path, paper["position"])
                _write_json_file(self._market_index_strip_path, market_context)
                _write_json_file(self._market_index_diagnostics_path, market_context.get("diagnostics", {}))
                _write_json_file(self._treasury_curve_path, treasury_curve)
                _write_json_file(self._treasury_curve_diagnostics_path, treasury_curve.get("diagnostics", {}))
                _write_json_file(self._historical_playback_snapshot_path, historical_playback)
                production_link = self._production_link_service.snapshot()
                same_underlying_conflicts = _build_same_underlying_conflicts(
                    paper=paper,
                    production_link=production_link,
                    generated_at=generated_at,
                )
                same_underlying_conflicts = self._apply_same_underlying_conflict_review_state(
                    same_underlying_conflicts,
                    generated_at=generated_at,
                )
                same_underlying_conflicts["events"] = self._same_underlying_conflict_events_payload(paper=paper)
                same_underlying_conflicts["summary"] = {
                    **dict(same_underlying_conflicts.get("summary") or {}),
                    "recent_event_count": len(list(((same_underlying_conflicts.get("events") or {}).get("rows") or [])[:10])),
                }
                same_underlying_conflict_lookup = _same_underlying_conflict_lookup(
                    same_underlying_conflicts.get("rows") or []
                )
                same_underlying_entry_block_lookup = _same_underlying_entry_block_lookup(
                    (same_underlying_conflicts.get("events") or {}).get("rows") or []
                )
                paper["runtime_registry"]["rows"] = _annotate_same_underlying_conflict_metadata(
                    list((paper.get("runtime_registry") or {}).get("rows") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                paper["strategy_performance"]["rows"] = _annotate_same_underlying_conflict_metadata(
                    list((paper.get("strategy_performance") or {}).get("rows") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                paper["strategy_performance"]["trade_log"] = _annotate_same_underlying_conflict_metadata(
                    list((paper.get("strategy_performance") or {}).get("trade_log") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                paper["strategy_performance"]["execution_likelihood"]["rows"] = _annotate_same_underlying_conflict_metadata(
                    list(((paper.get("strategy_performance") or {}).get("execution_likelihood") or {}).get("rows") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                paper["signal_intent_fill_audit"]["rows"] = _annotate_same_underlying_conflict_metadata(
                    list((paper.get("signal_intent_fill_audit") or {}).get("rows") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                lane_registry["rows"] = _annotate_same_underlying_conflict_metadata(
                    list(lane_registry.get("rows") or []),
                    same_underlying_conflict_lookup,
                    same_underlying_entry_block_lookup,
                )
                for section in lane_registry.get("sections", []):
                    section["rows"] = _annotate_same_underlying_conflict_metadata(
                        list(section.get("rows") or []),
                        same_underlying_conflict_lookup,
                        same_underlying_entry_block_lookup,
                    )
                strategy_analysis = build_strategy_analysis_payload(
                    historical_playback=historical_playback,
                    paper=paper,
                    runtime_registry=paper.get("runtime_registry"),
                    lane_registry=lane_registry,
                    generated_at=generated_at,
                )
                _write_json_file(self._strategy_analysis_path, strategy_analysis)
                _write_json_file(self._production_link_snapshot_path, production_link)

                action_log = _tail_jsonl(self._action_log_path, 20)
                snapshot_warnings = list(_SNAPSHOT_WARNINGS.get() or [])
                degraded_sections = _summarize_snapshot_warnings(snapshot_warnings)
                return {
                    "generated_at": generated_at,
                    "dashboard_meta": {
                        "build_stamp": self._build_stamp,
                        "version_label": f"dashboard-{self._build_stamp[:10]}",
                        "server_pid": self._server_info.pid if self._server_info else os.getpid(),
                        "server_started_at": self._server_info.started_at if self._server_info else None,
                        "server_url": self._server_info.url if self._server_info else None,
                        "server_host": self._server_info.host if self._server_info else None,
                        "server_port": self._server_info.port if self._server_info else None,
                        "degraded": bool(degraded_sections),
                        "warning_count": len(snapshot_warnings),
                    },
                    "refresh": {
                        "default_interval_seconds": DEFAULT_REFRESH_INTERVAL_SECONDS,
                        "options_seconds": [0, 5, 10, 15, 30, 60],
                        "last_refreshed_at": generated_at,
                    },
                    "global": {
                        "mode": "PAPER" if paper["running"] else "SHADOW" if shadow["running"] else "IDLE",
                        "mode_label": "PAPER" if paper["running"] else "SHADOW" if shadow["running"] else "IDLE",
                        "live_disabled": True,
                        "auth": auth_status,
                        "auth_ready": bool(auth_status.get("runtime_ready")),
                        "auth_label": "AUTH READY" if auth_status.get("runtime_ready") else "AUTH NOT READY",
                        "desk_clean_label": "DESK GUARDED" if carry_forward["active"] else "DESK CLEAN",
                        "desk_clean": not carry_forward["active"],
                        "paper_run_ready": bool(pre_session_review["ready_for_run"]),
                        "paper_run_ready_label": pre_session_review["readiness_label"],
                        "market_data_status": active_status["market_data_semantics"],
                        "market_data_label": active_status["market_data_semantics"],
                        "runtime_health": active_status["health_status"],
                        "runtime_health_label": active_status["health_status"],
                        "reconciliation_status": active_status["reconciliation_semantics"],
                        "fault_state": active_status["fault_state"],
                        "shadow_running": shadow["running"],
                        "paper_running": paper["running"],
                        "shadow_label": "RUNNING" if shadow["running"] else "STOPPED",
                        "paper_label": "RUNNING" if paper["running"] else "STOPPED",
                        "last_processed_bar_timestamp": active_status["last_processed_bar_end_ts"],
                        "last_update_timestamp": active_status["last_update_ts"],
                        "current_session_date": active_status["session_date"],
                        "stale": active_status["stale"],
                        "artifact_age_seconds": active_status["artifact_age_seconds"],
                    },
                    "degraded_sections": degraded_sections,
                    "dashboard_warnings": snapshot_warnings,
                    "market_context": market_context,
                    "treasury_curve": treasury_curve,
                    "approved_quant_baselines": approved_quant_baselines,
                    "lane_registry": lane_registry,
                    "operator_surface": operator_surface,
                    "shadow": shadow,
                    "paper": paper,
                    "active_decision_mode": "paper" if paper["running"] or paper["status"]["last_update_ts"] else "shadow",
                    "action_log": action_log,
                    "manual_controls": self._manual_controls_snapshot(paper),
                    "paper_operator_state": paper["operator_state"],
                    "review": review,
                    "paper_risk_state": risk_state,
                    "paper_closeout": closeout_state,
                    "paper_session_close_review": paper_session_close_review,
                    "paper_carry_forward": carry_forward,
                    "paper_pre_session_review": pre_session_review,
                    "paper_run_start": paper_run_start,
                    "paper_continuity": paper_continuity,
                    "historical_playback": historical_playback,
                    "strategy_analysis": strategy_analysis,
                    "research_capture": research_capture,
                    "production_link": production_link,
                    "same_underlying_conflicts": same_underlying_conflicts,
                }
            finally:
                _SNAPSHOT_WARNINGS.reset(warning_token)

    def _research_daily_capture_payload(self, *, generated_at: str) -> dict[str, Any]:
        latest_manifest = _load_json_file(self._research_daily_capture_latest_path)
        status_rows = _load_research_capture_status_rows(self._research_history_database_path)
        run_rows = _load_research_capture_run_rows(self._research_history_database_path, limit=20)
        attempted_symbols = list(latest_manifest.get("attempted_symbols") or [])
        succeeded_symbols = list(latest_manifest.get("succeeded_symbols") or [])
        failed_symbols = list(latest_manifest.get("failed_symbols") or [])
        last_attempted_at = latest_manifest.get("capture_completed_at") or latest_manifest.get("capture_started_at")
        last_succeeded_at = _latest_success_timestamp(status_rows)
        run_status = str(latest_manifest.get("status") or "no_run")
        freshness_state = _research_capture_freshness_state(last_attempted_at, now=datetime.now(timezone.utc))
        status_line = _research_capture_status_line(
            run_status=run_status,
            freshness_state=freshness_state,
            attempted_symbols=attempted_symbols,
            succeeded_symbols=succeeded_symbols,
            failed_symbols=failed_symbols,
            last_attempted_at=last_attempted_at,
        )
        return {
            "generated_at": generated_at,
            "run_status": run_status,
            "freshness_state": freshness_state,
            "stale": freshness_state == "stale",
            "last_attempted_at": last_attempted_at,
            "last_succeeded_at": last_succeeded_at,
            "attempted_symbols": attempted_symbols,
            "succeeded_symbols": succeeded_symbols,
            "failed_symbols": failed_symbols,
            "attempted_count": len(attempted_symbols),
            "succeeded_count": len(succeeded_symbols),
            "failed_count": len(failed_symbols),
            "target_count": latest_manifest.get("target_count"),
            "success_count": latest_manifest.get("success_count"),
            "failure_count": latest_manifest.get("failure_count"),
            "target_rows": list(latest_manifest.get("target_rows") or []),
            "results": list(latest_manifest.get("results") or []),
            "status_rows": status_rows,
            "recent_runs": run_rows,
            "research_database_path": str(self._research_history_database_path),
            "latest_manifest_path": str(self._research_daily_capture_latest_path),
            "latest_manifest_present": self._research_daily_capture_latest_path.exists(),
            "status_line": status_line,
            "expected_schedule": {
                "cadence": "daily",
                "stale_after_hours": 36,
            },
        }

    def dashboard_snapshot(self) -> dict[str, Any]:
        try:
            snapshot = self.snapshot()
        except Exception as exc:
            self._record_dashboard_probe(snapshot=None, error=exc)
            raise
        self._record_dashboard_probe(snapshot=snapshot, error=None)
        return snapshot

    def prime_dashboard_health(self) -> None:
        with self._dashboard_probe_lock:
            if self._dashboard_probe.get("state") == "starting":
                return
            self._dashboard_probe["state"] = "starting"
            self._dashboard_probe["checked_at"] = datetime.now(timezone.utc).isoformat()
            self._dashboard_probe["error"] = None
        try:
            self.dashboard_snapshot()
        except Exception:
            return

    def health_payload(self) -> dict[str, Any]:
        generated_at = datetime.now(timezone.utc).isoformat()
        server_info = self._server_info
        with self._dashboard_probe_lock:
            probe = dict(self._dashboard_probe)
        status = "ok" if probe.get("ready") else "starting" if probe.get("state") == "starting" else "degraded"
        payload: dict[str, Any] = {
            "status": status,
            "ready": bool(probe.get("ready")),
            "generated_at": generated_at,
            "build_stamp": self._build_stamp,
            "version_label": f"dashboard-{self._build_stamp[:10]}",
            "pid": server_info.pid if server_info else os.getpid(),
            "started_at": server_info.started_at if server_info else None,
            "url": server_info.url if server_info else None,
            "host": server_info.host if server_info else None,
            "port": server_info.port if server_info else None,
            "info_file": server_info.info_file if server_info else None,
            "endpoints": {
                "health": "/health",
                "dashboard": "/api/dashboard",
            },
            "checks": {
                "operator_surface_loadable": {
                    "ok": bool(probe.get("operator_surface_loadable")),
                    "detail": probe.get("operator_surface_detail")
                    or "No successful dashboard probe has been recorded yet.",
                },
                "api_dashboard_responding": {
                    "ok": bool(probe.get("api_dashboard_responding")),
                    "detail": probe.get("api_dashboard_detail")
                    or "No successful dashboard probe has been recorded yet.",
                },
            },
        }
        payload["latest_dashboard_generated_at"] = probe.get("generated_at")
        payload["last_probe_at"] = probe.get("checked_at")
        if probe.get("error"):
            payload["error"] = probe["error"]
        return payload

    def _record_dashboard_probe(self, *, snapshot: dict[str, Any] | None, error: Exception | None) -> None:
        checked_at = datetime.now(timezone.utc).isoformat()
        with self._dashboard_probe_lock:
            self._dashboard_probe["checked_at"] = checked_at
            if error is not None:
                detail = f"{type(error).__name__}: {error}"
                self._dashboard_probe.update(
                    {
                        "state": "degraded",
                        "ready": False,
                        "operator_surface_loadable": False,
                        "operator_surface_detail": detail,
                        "api_dashboard_responding": False,
                        "api_dashboard_detail": detail,
                        "generated_at": None,
                        "error": detail,
                    }
                )
                return

            operator_surface = snapshot.get("operator_surface") if snapshot is not None else None
            operator_surface_loadable = isinstance(operator_surface, dict) and bool(operator_surface)
            self._dashboard_probe.update(
                {
                    "state": "ready" if operator_surface_loadable else "degraded",
                    "ready": operator_surface_loadable,
                    "operator_surface_loadable": operator_surface_loadable,
                    "operator_surface_detail": (
                        "operator_surface payload loaded from the current dashboard snapshot."
                        if operator_surface_loadable
                        else "Dashboard snapshot returned an empty operator_surface payload."
                    ),
                    "api_dashboard_responding": snapshot is not None,
                    "api_dashboard_detail": (
                        "Dashboard snapshot generated successfully for /api/dashboard."
                        if snapshot is not None
                        else "No dashboard snapshot has been generated yet."
                    ),
                    "generated_at": snapshot.get("generated_at") if snapshot is not None else None,
                    "error": None,
                }
            )

    def run_action(self, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        command_map = {
            "start-shadow": ["bash", "scripts/run_probationary_shadow.sh", "--background"],
            "stop-shadow": ["bash", "scripts/stop_probationary_shadow.sh"],
            "start-paper": None,
            "stop-paper": ["bash", "scripts/stop_probationary_paper_soak.sh"],
            "generate-daily-summary": ["bash", "scripts/run_probationary_daily_summary.sh"],
            "generate-paper-summary": ["bash", "scripts/run_probationary_paper_summary.sh"],
            "auth-gate-check": ["bash", "scripts/run_schwab_auth_gate.sh"],
            "refresh-market-strip": None,
            "paper-halt-entries": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "halt_entries"],
            "paper-resume-entries": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "resume_entries"],
            "paper-clear-fault": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "clear_fault"],
            "paper-clear-risk-halts": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "clear_risk_halts"],
            "paper-force-reconcile": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "force_reconcile"],
            "paper-flatten-and-halt": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "flatten_and_halt"],
            "paper-stop-after-cycle": ["bash", "scripts/run_probationary_operator_control.sh", "--action", "stop_after_cycle"],
            "refresh-status": None,
        }
        if action not in command_map:
            if action == "paper-force-lane-resume-session-override":
                result = self._queue_paper_lane_session_override(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "same-underlying-acknowledge":
                result = self._acknowledge_same_underlying_conflict(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "same-underlying-mark-observational":
                result = self._mark_same_underlying_conflict_observational(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "same-underlying-hold-entries":
                result = self._hold_same_underlying_entries(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "same-underlying-clear-hold":
                result = self._clear_same_underlying_hold(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "same-underlying-reset-review":
                result = self._reset_same_underlying_review(payload)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "capture-paper-soak-evidence":
                result = self._capture_paper_soak_evidence()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "acknowledge-paper-risk":
                result = self._acknowledge_paper_risk()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "sign-off-paper-session":
                result = self._sign_off_paper_session()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "acknowledge-inherited-risk":
                result = self._acknowledge_inherited_risk()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "resolve-inherited-risk":
                result = self._resolve_inherited_risk()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "complete-pre-session-review":
                result = self._complete_pre_session_review()
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
            if action == "restart-paper-with-temp-paper":
                pre_snapshot = self.snapshot()
                command, metadata = self._paper_start_command_with_enabled_temp_paper(pre_snapshot)
                if command is None:
                    result = self._result_record(
                        action=action,
                        ok=False,
                        command=None,
                        output=(
                            "Enabled temporary paper lanes are missing a startup mapping. "
                            f"Unresolved lane ids: {', '.join(metadata['unresolved_lane_ids']) or 'none'}"
                        ),
                    )
                    result["temp_paper_runtime_request"] = metadata
                    self._log_action(result)
                    result["snapshot"] = self.snapshot()
                    return result
                stop_result = subprocess.run(
                    ["bash", "scripts/stop_probationary_paper_soak.sh"],
                    cwd=self._repo_root,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                start_result = subprocess.run(
                    command,
                    cwd=self._repo_root,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                combined_stdout = "\n".join(part for part in [(stop_result.stdout or "").strip(), (start_result.stdout or "").strip()] if part)
                combined_stderr = "\n".join(part for part in [(stop_result.stderr or "").strip(), (start_result.stderr or "").strip()] if part)
                output = combined_stdout or combined_stderr or "Restart completed without output."
                result = self._result_record(
                    action=action,
                    ok=stop_result.returncode == 0 and start_result.returncode == 0,
                    command=command,
                    output=output,
                    returncode=start_result.returncode,
                    stdout=combined_stdout,
                    stderr=combined_stderr,
                )
                result["temp_paper_runtime_request"] = metadata
                post_snapshot = self.snapshot()
                mismatch = self._temporary_paper_runtime_mismatch(post_snapshot)
                if result["ok"] and mismatch["mismatch"]:
                    result["ok"] = False
                    result["message"] = "Restart Paper Soak With Temp Paper failed"
                    result["output"] = (
                        "Enabled temporary paper lanes were not loaded into the restarted runtime. "
                        f"Missing lane ids: {', '.join(mismatch['missing_lane_ids']) or 'none'} | "
                        f"Unresolved lane ids: {', '.join(mismatch['unresolved_lane_ids']) or 'none'}"
                    )
                self._log_action(result)
                result["snapshot"] = post_snapshot
                return result
            raise ValueError(f"Unsupported dashboard action: {action}")

        if action == "refresh-status":
            result = self._result_record(
                action=action,
                ok=True,
                command=None,
                output="Status refreshed from live artifacts.",
            )
            self._log_action(result)
            result["snapshot"] = self.snapshot()
            return result
        if action == "refresh-market-strip":
            self._market_index_cache = None
            self._market_index_cache_at = None
            result = self._result_record(
                action=action,
                ok=True,
                command=None,
                output="Forced a fresh market-index fetch through the live dashboard path.",
            )
            self._log_action(result)
            result["snapshot"] = self.snapshot()
            return result

        pre_snapshot = self.snapshot() if action == "start-paper" else None
        precheck = self._prechecked_action_result(action)
        if precheck is not None:
            if action == "start-paper":
                self._record_paper_start_block(pre_snapshot or self.snapshot(), precheck)
            self._log_action(precheck)
            precheck["snapshot"] = self.snapshot()
            return precheck

        command = command_map[action]
        if action == "start-paper":
            command, metadata = self._paper_start_command_with_enabled_temp_paper(pre_snapshot or self.snapshot())
            if command is None:
                result = self._result_record(
                    action=action,
                    ok=False,
                    command=None,
                    output=(
                        "Enabled temporary paper lanes are missing a startup mapping. "
                        f"Unresolved lane ids: {', '.join(metadata['unresolved_lane_ids']) or 'none'}"
                    ),
                )
                result["temp_paper_runtime_request"] = metadata
                self._record_paper_start_block(pre_snapshot or self.snapshot(), result)
                self._log_action(result)
                result["snapshot"] = self.snapshot()
                return result
        completed = subprocess.run(
            command,
            cwd=self._repo_root,
            text=True,
            capture_output=True,
            check=False,
        )
        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        output = stdout or stderr or "Command completed without output."
        result = self._result_record(
            action=action,
            ok=completed.returncode == 0,
            command=command,
            output=output,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
        )
        if action == "start-paper":
            result["temp_paper_runtime_request"] = metadata
        if action == "auth-gate-check" and completed.returncode == 0:
            parsed = _parse_json_output(stdout or stderr)
            if parsed is not None:
                self._auth_cache_path.write_text(
                    json.dumps(parsed, sort_keys=True, indent=2) + "\n",
                    encoding="utf-8",
                )
                result["auth"] = parsed
        if action == "start-paper":
            post_snapshot = self.snapshot()
            mismatch = self._temporary_paper_runtime_mismatch(post_snapshot)
            if completed.returncode == 0 and post_snapshot["paper"]["running"] and not mismatch["mismatch"]:
                self._record_paper_start_success(pre_snapshot or post_snapshot, post_snapshot, result)
            else:
                if completed.returncode == 0 and mismatch["mismatch"]:
                    result["ok"] = False
                    result["message"] = "Start Paper Soak failed"
                    result["output"] = (
                        "Paper soak started without all enabled temporary paper lanes. "
                        f"Missing lane ids: {', '.join(mismatch['missing_lane_ids']) or 'none'} | "
                        f"Unresolved lane ids: {', '.join(mismatch['unresolved_lane_ids']) or 'none'}"
                    )
                self._record_paper_start_block(pre_snapshot or post_snapshot, result)
        self._log_action(result)
        result["snapshot"] = self.snapshot()
        return result

    def run_production_action(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        result = self._production_link_service.run_action(action, payload)
        result_payload = self._result_record(
            action=f"production-{action}",
            ok=bool(result.get("ok")),
            command=None,
            output=str(result.get("output") or result.get("message") or ""),
        )
        result_payload["message"] = str(result.get("message") or result_payload["message"])
        result_payload["production_link"] = result.get("production_link") or self._production_link_service.snapshot()
        self._log_action(result_payload)
        result_payload["snapshot"] = self.snapshot()
        return result_payload

    def _load_same_underlying_conflict_review_store(self) -> dict[str, Any]:
        payload = _read_json(self._same_underlying_conflict_review_state_path)
        records = payload.get("records") or {}
        if not isinstance(records, dict):
            records = {}
        return {
            "records": {str(key): dict(value) for key, value in records.items() if isinstance(value, dict)},
            "updated_at": payload.get("updated_at"),
        }

    def _write_same_underlying_conflict_review_store(self, records: dict[str, dict[str, Any]], *, updated_at: str) -> None:
        active_entry_holds = sorted(
            instrument
            for instrument, record in records.items()
            if record.get("hold_new_entries") is True and record.get("entry_hold_effective") is True
        )
        _write_json_file(
            self._same_underlying_conflict_review_state_path,
            {
                "updated_at": updated_at,
                "records": records,
                "active_entry_holds": active_entry_holds,
                "history_path": str(self._same_underlying_conflict_review_history_path.resolve()),
                "events_path": str(self._same_underlying_conflict_events_path.resolve()),
            },
        )

    def _append_same_underlying_conflict_review_history(
        self,
        *,
        event: str,
        instrument: str,
        previous_state: dict[str, Any] | None,
        current_state: dict[str, Any],
        operator_label: str | None = None,
        note: str | None = None,
        auth_metadata: dict[str, Any] | None = None,
    ) -> None:
        effective_auth_metadata = dict(auth_metadata or {})
        if not effective_auth_metadata:
            effective_auth_metadata = {
                "local_operator_identity": current_state.get("last_local_operator_identity"),
                "auth_method": current_state.get("last_auth_method"),
                "authenticated_at": current_state.get("last_authenticated_at"),
                "auth_session_id": current_state.get("last_auth_session_id"),
                "operator_authenticated": bool(current_state.get("last_operator_authenticated")),
                "requested_operator_label": current_state.get("last_requested_operator_label"),
            }
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "instrument": instrument,
            "operator_label": operator_label or "manual operator",
            "note": note,
            "previous_state": previous_state or {},
            "current_state": current_state,
            "local_operator_identity": effective_auth_metadata.get("local_operator_identity"),
            "auth_method": effective_auth_metadata.get("auth_method"),
            "authenticated_at": effective_auth_metadata.get("authenticated_at"),
            "auth_session_id": effective_auth_metadata.get("auth_session_id"),
            "operator_authenticated": bool(effective_auth_metadata.get("operator_authenticated")),
            "requested_operator_label": effective_auth_metadata.get("requested_operator_label"),
        }
        _append_jsonl(self._same_underlying_conflict_review_history_path, payload)

    def _append_same_underlying_conflict_event(
        self,
        *,
        event_type: str,
        instrument: str,
        current_conflict: dict[str, Any] | None,
        current_state: dict[str, Any],
        operator_label: str | None = None,
        note: str | None = None,
        automatic: bool,
        extra: dict[str, Any] | None = None,
        auth_metadata: dict[str, Any] | None = None,
    ) -> None:
        conflict = dict(current_conflict or {})
        effective_auth_metadata = dict(auth_metadata or {})
        if not effective_auth_metadata:
            effective_auth_metadata = {
                "local_operator_identity": current_state.get("last_local_operator_identity"),
                "auth_method": current_state.get("last_auth_method"),
                "authenticated_at": current_state.get("last_authenticated_at"),
                "auth_session_id": current_state.get("last_auth_session_id"),
                "operator_authenticated": bool(current_state.get("last_operator_authenticated")),
                "requested_operator_label": current_state.get("last_requested_operator_label"),
            }
        standalone_strategy_ids = [
            str(value or "")
            for value in list(conflict.get("standalone_strategy_ids") or current_state.get("current_material_state", {}).get("standalone_strategy_ids") or [])
            if str(value or "")
        ]
        payload = {
            "event_type": event_type,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "instrument": instrument,
            "standalone_strategy_ids": standalone_strategy_ids,
            "conflict_fingerprint": current_state.get("current_conflict_fingerprint")
            or current_state.get("reviewed_conflict_fingerprint")
            or conflict.get("conflict_fingerprint"),
            "conflict_version": current_state.get("current_conflict_fingerprint")
            or current_state.get("reviewed_conflict_fingerprint")
            or conflict.get("conflict_fingerprint"),
            "severity": current_state.get("severity_at_review") or conflict.get("severity"),
            "conflict_kind": current_state.get("conflict_kind_at_review") or conflict.get("conflict_kind"),
            "operator_label": operator_label or "manual operator",
            "local_operator_identity": effective_auth_metadata.get("local_operator_identity"),
            "auth_method": effective_auth_metadata.get("auth_method"),
            "authenticated_at": effective_auth_metadata.get("authenticated_at"),
            "auth_session_id": effective_auth_metadata.get("auth_session_id"),
            "operator_authenticated": bool(effective_auth_metadata.get("operator_authenticated")),
            "requested_operator_label": effective_auth_metadata.get("requested_operator_label"),
            "note": note,
            "automatic": automatic,
            "operator_triggered": not automatic,
            "hold_new_entries": bool(current_state.get("hold_new_entries")),
            "entry_hold_effective": bool(current_state.get("entry_hold_effective")),
            "review_state_status": current_state.get("state_status"),
            "hold_effective_now": bool(current_state.get("hold_effective_now")),
            "hold_expired": bool(current_state.get("hold_expired")),
            "hold_expired_at": current_state.get("hold_expired_at"),
            "hold_state_reason": current_state.get("hold_state_reason"),
        }
        if extra:
            payload.update(extra)
        payload["event_id"] = hashlib.sha256(
            json.dumps(
                {
                    "event_type": payload["event_type"],
                    "occurred_at": payload["occurred_at"],
                    "instrument": payload["instrument"],
                    "standalone_strategy_ids": payload["standalone_strategy_ids"],
                    "conflict_fingerprint": payload["conflict_fingerprint"],
                    "operator_label": payload["operator_label"],
                    "note": payload["note"],
                    "automatic": payload["automatic"],
                    "entry_hold_effective": payload["entry_hold_effective"],
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        _append_jsonl(self._same_underlying_conflict_events_path, payload)

    def _same_underlying_conflict_events_payload(self, *, paper: dict[str, Any]) -> dict[str, Any]:
        persisted_rows = list(reversed(_tail_jsonl(self._same_underlying_conflict_events_path, 200)))
        operator_controls_path = _path_or_none((paper.get("artifacts") or {}).get("operator_controls"))
        if operator_controls_path is None:
            artifacts_dir = _path_or_none(paper.get("artifacts_dir"))
            operator_controls_path = artifacts_dir / "operator_controls.jsonl" if artifacts_dir is not None else None
        runtime_operator_controls = _all_jsonl_rows(operator_controls_path) if operator_controls_path is not None else []
        runtime_rows: list[dict[str, Any]] = []
        for row in runtime_operator_controls:
            event_type = str(row.get("event_type") or "")
            action = str(row.get("action") or "")
            if event_type != "entry_blocked_by_same_underlying_hold" and action != "same_underlying_entry_hold_blocked":
                continue
            normalized = {
                "event_id": str(row.get("event_id") or ""),
                "event_type": "entry_blocked_by_same_underlying_hold",
                "occurred_at": row.get("occurred_at") or row.get("blocked_at") or row.get("applied_at") or row.get("logged_at"),
                "instrument": str(row.get("instrument") or row.get("symbol") or "").strip().upper(),
                "standalone_strategy_ids": [
                    str(value or "")
                    for value in [
                        row.get("blocked_standalone_strategy_id"),
                        row.get("standalone_strategy_id"),
                    ]
                    if str(value or "")
                ],
                "conflict_fingerprint": row.get("conflict_fingerprint"),
                "conflict_version": row.get("conflict_fingerprint"),
                "severity": row.get("severity") or "BLOCKING",
                "conflict_kind": row.get("conflict_kind") or "multiple_runtime_instances_same_instrument",
                "operator_label": row.get("operator_label") or "automatic runtime control",
                "local_operator_identity": row.get("local_operator_identity"),
                "auth_method": row.get("auth_method"),
                "authenticated_at": row.get("authenticated_at"),
                "auth_session_id": row.get("auth_session_id"),
                "operator_authenticated": bool(row.get("operator_authenticated")),
                "requested_operator_label": row.get("requested_operator_label"),
                "note": row.get("block_reason") or row.get("message"),
                "automatic": True,
                "operator_triggered": False,
                "hold_new_entries": True,
                "entry_hold_effective": True,
                "review_state_status": row.get("review_state_status") or "HOLDING",
                "hold_effective_now": True,
                "hold_expired": False,
                "hold_expired_at": None,
                "hold_state_reason": row.get("block_reason") or row.get("message"),
                "blocked_standalone_strategy_id": row.get("blocked_standalone_strategy_id") or row.get("standalone_strategy_id"),
                "blocked_strategy_family": row.get("strategy_family"),
                "blocked_reason": row.get("block_reason") or row.get("message"),
                "blocked_bar_id": row.get("bar_id"),
                "source": "paper_runtime_operator_controls",
            }
            normalized["event_id"] = normalized["event_id"] or hashlib.sha256(
                json.dumps(
                    {
                        "event_type": normalized["event_type"],
                        "occurred_at": normalized["occurred_at"],
                        "instrument": normalized["instrument"],
                        "blocked_standalone_strategy_id": normalized["blocked_standalone_strategy_id"],
                        "blocked_bar_id": normalized["blocked_bar_id"],
                    },
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            runtime_rows.append(normalized)

        merged_by_id: dict[str, dict[str, Any]] = {}
        for row in [*persisted_rows, *runtime_rows]:
            event_id = str(row.get("event_id") or "").strip()
            if not event_id:
                event_id = hashlib.sha256(json.dumps(row, sort_keys=True).encode("utf-8")).hexdigest()
            merged_by_id[event_id] = dict(row)
        rows = sorted(
            merged_by_id.values(),
            key=lambda row: str(row.get("occurred_at") or row.get("logged_at") or ""),
            reverse=True,
        )
        latest_event = rows[0] if rows else None
        latest_entry_blocked_event = next(
            (row for row in rows if str(row.get("event_type") or "") == "entry_blocked_by_same_underlying_hold"),
            None,
        )
        return {
            "rows": rows,
            "latest_event": latest_event,
            "latest_entry_blocked_event": latest_entry_blocked_event,
            "summary": {
                "event_count": len(rows),
                "entry_blocked_count": sum(
                    1 for row in rows if str(row.get("event_type") or "") == "entry_blocked_by_same_underlying_hold"
                ),
                "automatic_count": sum(1 for row in rows if row.get("automatic") is True),
                "operator_triggered_count": sum(1 for row in rows if row.get("operator_triggered") is True),
                "latest_occurred_at": latest_event.get("occurred_at") if latest_event else None,
                "affected_instruments": sorted(
                    {str(row.get("instrument") or "").strip().upper() for row in rows if row.get("instrument")}
                ),
            },
            "artifacts": {
                "events": str(self._same_underlying_conflict_events_path.resolve()),
                "review_history": str(self._same_underlying_conflict_review_history_path.resolve()),
            },
        }

    def _apply_same_underlying_conflict_review_state(
        self,
        conflicts: dict[str, Any],
        *,
        generated_at: str,
    ) -> dict[str, Any]:
        store = self._load_same_underlying_conflict_review_store()
        records = dict(store.get("records") or {})
        active_instruments: set[str] = set()
        changed = False
        enriched_rows: list[dict[str, Any]] = []

        for row in list(conflicts.get("rows") or []):
            instrument = str(row.get("instrument") or "").strip().upper()
            if not instrument:
                enriched_rows.append(dict(row))
                continue
            active_instruments.add(instrument)
            current_material_state = _same_underlying_conflict_material_state(row)
            current_fingerprint = _same_underlying_conflict_fingerprint(row)
            previous_record = dict(records.get(instrument) or _default_same_underlying_conflict_review_record(instrument))
            record = dict(previous_record)
            record["instrument"] = instrument
            record["current_conflict_fingerprint"] = current_fingerprint
            record["current_material_state"] = current_material_state
            record["hold_expiry_enforced"] = bool(record.get("hold_expiry_enforced"))
            record["hold_expired"] = bool(record.get("hold_expired"))
            previous_status = str(previous_record.get("state_status") or "OPEN")
            hold_expires_at = _parse_iso_datetime(str(record.get("hold_expires_at") or ""))
            generated_at_dt = _parse_iso_datetime(generated_at) or datetime.now(timezone.utc)
            hold_expired_now = bool(record.get("hold_new_entries")) and hold_expires_at is not None and hold_expires_at <= generated_at_dt
            if hold_expired_now:
                expiry_reason = (
                    f"Same-underlying entry hold expired at {hold_expires_at.isoformat()}; "
                    "new entries are no longer blocked automatically."
                )
                if previous_record.get("hold_expiry_enforced") is not True:
                    changed = True
                    expired_state = {
                        **record,
                        "hold_new_entries": False,
                        "entry_hold_effective": False,
                        "hold_expired": True,
                        "hold_expired_at": generated_at,
                        "hold_expiry_enforced": True,
                        "hold_effective_now": False,
                        "hold_state_reason": expiry_reason,
                        "state_status": "HOLD_EXPIRED",
                    }
                    self._append_same_underlying_conflict_review_history(
                        event="hold_expired",
                        instrument=instrument,
                        previous_state=previous_record,
                        current_state=expired_state,
                        operator_label=str(
                            previous_record.get("hold_set_by")
                            or previous_record.get("acknowledged_by")
                            or previous_record.get("override_set_by")
                            or "automatic expiry"
                        ),
                        note=expiry_reason,
                    )
                    self._append_same_underlying_conflict_event(
                        event_type="conflict_hold_expired",
                        instrument=instrument,
                        current_conflict=row,
                        current_state=expired_state,
                        operator_label=str(
                            previous_record.get("hold_set_by")
                            or previous_record.get("acknowledged_by")
                            or previous_record.get("override_set_by")
                            or "automatic expiry"
                        ),
                        note=expiry_reason,
                        automatic=True,
                    )
                record["hold_new_entries"] = False
                record["entry_hold_effective"] = False
                record["hold_expired"] = True
                record["hold_expired_at"] = generated_at
                record["hold_expiry_enforced"] = True
                record["hold_effective_now"] = False
                record["hold_state_reason"] = expiry_reason
            elif record.get("hold_new_entries") is True:
                record["entry_hold_effective"] = True
                record["hold_effective_now"] = True
                record["hold_state_reason"] = (
                    str(record.get("hold_reason") or "").strip()
                    or f"New entries held by operator for same-underlying conflict review on {instrument}."
                )
            else:
                record["entry_hold_effective"] = False
                record["hold_effective_now"] = False
                if record.get("hold_expired") is True:
                    record["hold_state_reason"] = str(record.get("hold_state_reason") or "").strip() or "Hold expired."
                else:
                    record["hold_state_reason"] = str(record.get("hold_reason") or "").strip() or None
            reviewed_fingerprint = str(record.get("reviewed_conflict_fingerprint") or "").strip() or None
            state_changed_since_review = bool(reviewed_fingerprint and reviewed_fingerprint != current_fingerprint)
            reopened_reason = ""
            if state_changed_since_review:
                reopened_reason = _same_underlying_conflict_reopened_reason(
                    instrument=instrument,
                    previous_material_state=previous_record.get("reviewed_material_state"),
                    current_material_state=current_material_state,
                )
            auto_reopened = bool(reopened_reason)
            if auto_reopened:
                if (
                    previous_record.get("auto_reopen_required") is not True
                    or str(previous_record.get("reopened_reason") or "") != reopened_reason
                    or str(previous_record.get("current_conflict_fingerprint") or "") != current_fingerprint
                ):
                    changed = True
                    self._append_same_underlying_conflict_review_history(
                        event="auto_reopened",
                        instrument=instrument,
                        previous_state=previous_record,
                        current_state={
                            **record,
                            "state_status": "STALE",
                            "auto_reopen_required": True,
                            "stale_since": previous_record.get("stale_since") or generated_at,
                            "reopened_reason": reopened_reason,
                        },
                        operator_label=str(previous_record.get("acknowledged_by") or previous_record.get("hold_set_by") or previous_record.get("override_set_by") or "manual operator"),
                        note=reopened_reason,
                    )
                    self._append_same_underlying_conflict_event(
                        event_type="conflict_auto_reopened",
                        instrument=instrument,
                        current_conflict=row,
                        current_state={
                            **record,
                            "state_status": "STALE",
                            "auto_reopen_required": True,
                            "stale_since": previous_record.get("stale_since") or generated_at,
                            "reopened_reason": reopened_reason,
                        },
                        operator_label=str(previous_record.get("acknowledged_by") or previous_record.get("hold_set_by") or previous_record.get("override_set_by") or "manual operator"),
                        note=reopened_reason,
                        automatic=True,
                        extra={"reopened_reason": reopened_reason},
                    )
                record["state_status"] = "STALE"
                record["auto_reopen_required"] = True
                record["stale_since"] = previous_record.get("stale_since") or generated_at
                record["reopened_reason"] = reopened_reason
            else:
                benign_state_refresh = bool(state_changed_since_review)
                if benign_state_refresh:
                    record["reviewed_conflict_fingerprint"] = current_fingerprint
                    record["reviewed_material_state"] = current_material_state
                    record["severity_at_review"] = row.get("severity")
                    record["conflict_kind_at_review"] = row.get("conflict_kind")
                next_status = "OPEN"
                if record.get("hold_new_entries") is True:
                    next_status = "HOLDING"
                elif record.get("hold_expired") is True:
                    next_status = "HOLD_EXPIRED"
                elif record.get("override_observational_only") is True:
                    next_status = "OVERRIDDEN"
                elif record.get("acknowledged") is True:
                    next_status = "ACKNOWLEDGED"
                if (
                    previous_status != next_status
                    or previous_record.get("auto_reopen_required")
                    or previous_record.get("reopened_reason")
                    or previous_record.get("stale_since")
                    or benign_state_refresh
                    or str(previous_record.get("current_conflict_fingerprint") or "") != current_fingerprint
                ):
                    changed = True
                record["state_status"] = next_status
                record["auto_reopen_required"] = False
                record["stale_since"] = None
                record["reopened_reason"] = None
            record["exit_actions_still_allowed"] = True
            records[instrument] = record
            enriched_rows.append(
                {
                    **row,
                    "conflict_fingerprint": current_fingerprint,
                    "review_state_status": record.get("state_status"),
                    "acknowledged": bool(record.get("acknowledged")),
                    "acknowledged_at": record.get("acknowledged_at"),
                    "acknowledged_by": record.get("acknowledged_by"),
                    "acknowledgement_note": record.get("acknowledgement_note"),
                    "hold_new_entries": bool(record.get("hold_new_entries")),
                    "hold_reason": record.get("hold_reason"),
                    "hold_set_at": record.get("hold_set_at"),
                    "hold_set_by": record.get("hold_set_by"),
                    "hold_expires_at": record.get("hold_expires_at"),
                    "hold_expired": bool(record.get("hold_expired")),
                    "hold_expired_at": record.get("hold_expired_at"),
                    "hold_expiry_enforced": bool(record.get("hold_expiry_enforced")),
                    "hold_effective_now": bool(record.get("hold_effective_now")),
                    "hold_state_reason": record.get("hold_state_reason"),
                    "override_observational_only": bool(record.get("override_observational_only")),
                    "override_reason": record.get("override_reason"),
                    "override_set_at": record.get("override_set_at"),
                    "override_set_by": record.get("override_set_by"),
                    "stale_since": record.get("stale_since"),
                    "reopened_reason": record.get("reopened_reason"),
                    "entry_hold_effective": bool(record.get("entry_hold_effective")),
                    "exit_actions_still_allowed": True,
                    "severity_at_review": record.get("severity_at_review"),
                    "conflict_kind_at_review": record.get("conflict_kind_at_review"),
                    "auto_reopen_required": bool(record.get("auto_reopen_required")),
                    "last_local_operator_identity": record.get("last_local_operator_identity"),
                    "last_auth_method": record.get("last_auth_method"),
                    "last_authenticated_at": record.get("last_authenticated_at"),
                    "last_auth_session_id": record.get("last_auth_session_id"),
                    "last_operator_authenticated": bool(record.get("last_operator_authenticated")),
                    "last_requested_operator_label": record.get("last_requested_operator_label"),
                }
            )

        for instrument, previous_record in list(records.items()):
            if instrument in active_instruments:
                continue
            record = dict(previous_record)
            if record.get("current_conflict_fingerprint") is not None or record.get("entry_hold_effective") is not False:
                changed = True
            record["current_conflict_fingerprint"] = None
            record["current_material_state"] = None
            record["entry_hold_effective"] = False
            record["hold_effective_now"] = False
            record["exit_actions_still_allowed"] = True
            record["auto_reopen_required"] = False
            record["stale_since"] = None
            record["reopened_reason"] = None
            if record.get("hold_new_entries") is True:
                record["state_status"] = "HOLDING"
            elif record.get("hold_expired") is True:
                record["state_status"] = "HOLD_EXPIRED"
            elif record.get("override_observational_only") is True:
                record["state_status"] = "OVERRIDDEN"
            elif record.get("acknowledged") is True:
                record["state_status"] = "ACKNOWLEDGED"
            else:
                record["state_status"] = "OPEN"
            records[instrument] = record

        summary = _same_underlying_conflict_review_summary(enriched_rows)
        if changed or str(store.get("updated_at") or "") != generated_at:
            self._write_same_underlying_conflict_review_store(records, updated_at=generated_at)
        return {
            **conflicts,
            "rows": enriched_rows,
            "summary": {
                **dict(conflicts.get("summary") or {}),
                **summary,
            },
            "review_artifacts": {
                "state": str(self._same_underlying_conflict_review_state_path.resolve()),
                "history": str(self._same_underlying_conflict_review_history_path.resolve()),
                "events": str(self._same_underlying_conflict_events_path.resolve()),
            },
        }

    def _same_underlying_conflict_row(self, instrument: str) -> dict[str, Any]:
        normalized_instrument = str(instrument or "").strip().upper()
        if not normalized_instrument:
            raise ValueError("instrument is required for same-underlying conflict actions.")
        snapshot = self.snapshot()
        rows = list((snapshot.get("same_underlying_conflicts") or {}).get("rows") or [])
        for row in rows:
            if str(row.get("instrument") or "").strip().upper() == normalized_instrument:
                return dict(row)
        raise ValueError(f"No active same-underlying conflict is currently surfaced for {normalized_instrument}.")

    def _operator_label_from_payload(self, payload: dict[str, Any]) -> str:
        return (
            str(
                payload.get("local_operator_identity")
                or payload.get("operator_label")
                or payload.get("acknowledged_by")
                or "manual operator"
            ).strip()
            or "manual operator"
        )

    def _local_auth_metadata_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        local_operator_identity = str(payload.get("local_operator_identity") or "").strip() or None
        auth_method = str(payload.get("auth_method") or "").strip() or None
        authenticated_at = str(payload.get("authenticated_at") or "").strip() or None
        auth_session_id = str(payload.get("auth_session_id") or "").strip() or None
        requested_operator_label = str(payload.get("requested_operator_label") or payload.get("operator_label") or "").strip() or None
        return {
            "local_operator_identity": local_operator_identity,
            "auth_method": auth_method,
            "authenticated_at": authenticated_at,
            "auth_session_id": auth_session_id,
            "operator_authenticated": bool(local_operator_identity and auth_method and authenticated_at),
            "requested_operator_label": requested_operator_label,
        }

    def _note_from_payload(self, payload: dict[str, Any], *keys: str) -> str | None:
        for key in keys:
            text = str(payload.get(key) or "").strip()
            if text:
                return text
        return None

    def _update_same_underlying_conflict_review_record(
        self,
        *,
        instrument: str,
        current_conflict: dict[str, Any],
        mutate,
        event: str,
        operator_label: str,
        note: str | None,
        auth_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        store = self._load_same_underlying_conflict_review_store()
        records = dict(store.get("records") or {})
        previous_record = dict(records.get(instrument) or _default_same_underlying_conflict_review_record(instrument))
        record = dict(previous_record)
        current_fingerprint = _same_underlying_conflict_fingerprint(current_conflict)
        current_material_state = _same_underlying_conflict_material_state(current_conflict)
        record.update(
            {
                "instrument": instrument,
                "current_conflict_fingerprint": current_fingerprint,
                "current_material_state": current_material_state,
            }
        )
        mutate(record, current_conflict)
        if auth_metadata:
            record.update(
                {
                    "last_local_operator_identity": auth_metadata.get("local_operator_identity"),
                    "last_auth_method": auth_metadata.get("auth_method"),
                    "last_authenticated_at": auth_metadata.get("authenticated_at"),
                    "last_auth_session_id": auth_metadata.get("auth_session_id"),
                    "last_operator_authenticated": bool(auth_metadata.get("operator_authenticated")),
                    "last_requested_operator_label": auth_metadata.get("requested_operator_label"),
                }
            )
        record["reviewed_conflict_fingerprint"] = current_fingerprint
        record["reviewed_material_state"] = current_material_state
        record["severity_at_review"] = current_conflict.get("severity")
        record["conflict_kind_at_review"] = current_conflict.get("conflict_kind")
        record["auto_reopen_required"] = False
        record["stale_since"] = None
        record["reopened_reason"] = None
        record["hold_expired"] = bool(record.get("hold_expired"))
        record["hold_expiry_enforced"] = bool(record.get("hold_expiry_enforced"))
        if record.get("hold_new_entries") is True:
            record["state_status"] = "HOLDING"
            record["hold_effective_now"] = True
            record["hold_state_reason"] = (
                str(record.get("hold_reason") or "").strip()
                or f"New entries held by operator for same-underlying conflict review on {instrument}."
            )
        elif record.get("hold_expired") is True:
            record["state_status"] = "HOLD_EXPIRED"
            record["entry_hold_effective"] = False
            record["hold_effective_now"] = False
        elif record.get("override_observational_only") is True:
            record["state_status"] = "OVERRIDDEN"
            record["entry_hold_effective"] = False
            record["hold_effective_now"] = False
        elif record.get("acknowledged") is True:
            record["state_status"] = "ACKNOWLEDGED"
            record["entry_hold_effective"] = False
            record["hold_effective_now"] = False
        else:
            record["state_status"] = "OPEN"
            record["entry_hold_effective"] = False
            record["hold_effective_now"] = False
        record["exit_actions_still_allowed"] = True
        records[instrument] = record
        updated_at = datetime.now(timezone.utc).isoformat()
        self._write_same_underlying_conflict_review_store(records, updated_at=updated_at)
        self._append_same_underlying_conflict_review_history(
            event=event,
            instrument=instrument,
            previous_state=previous_record,
            current_state=record,
            operator_label=operator_label,
            note=note,
            auth_metadata=auth_metadata,
        )
        self._append_same_underlying_conflict_event(
            event_type=event,
            instrument=instrument,
            current_conflict=current_conflict,
            current_state=record,
            operator_label=operator_label,
            note=note,
            automatic=False,
            auth_metadata=auth_metadata,
        )
        return record

    def _acknowledge_same_underlying_conflict(self, payload: dict[str, Any]) -> dict[str, Any]:
        conflict = self._same_underlying_conflict_row(str(payload.get("instrument") or ""))
        operator_label = self._operator_label_from_payload(payload)
        auth_metadata = self._local_auth_metadata_from_payload(payload)
        note = self._note_from_payload(payload, "acknowledgement_note", "note", "reason")
        instrument = str(conflict.get("instrument") or "")
        self._update_same_underlying_conflict_review_record(
            instrument=instrument,
            current_conflict=conflict,
            mutate=lambda record, _: record.update(
                {
                    "acknowledged": True,
                    "acknowledged_at": datetime.now(timezone.utc).isoformat(),
                    "acknowledged_by": operator_label,
                    "acknowledgement_note": note,
                    "hold_expired": False,
                    "hold_expired_at": None,
                    "hold_expiry_enforced": False,
                    "hold_state_reason": note,
                }
            ),
            event="conflict_acknowledged",
            operator_label=operator_label,
            note=note,
            auth_metadata=auth_metadata,
        )
        return self._result_record(
            action="same-underlying-acknowledge",
            ok=True,
            command=None,
            output=f"Same-underlying conflict on {instrument} acknowledged.",
        )

    def _mark_same_underlying_conflict_observational(self, payload: dict[str, Any]) -> dict[str, Any]:
        conflict = self._same_underlying_conflict_row(str(payload.get("instrument") or ""))
        operator_label = self._operator_label_from_payload(payload)
        auth_metadata = self._local_auth_metadata_from_payload(payload)
        reason = self._note_from_payload(payload, "override_reason", "note", "reason")
        instrument = str(conflict.get("instrument") or "")
        self._update_same_underlying_conflict_review_record(
            instrument=instrument,
            current_conflict=conflict,
            mutate=lambda record, _: record.update(
                {
                    "acknowledged": True,
                    "acknowledged_at": record.get("acknowledged_at") or datetime.now(timezone.utc).isoformat(),
                    "acknowledged_by": record.get("acknowledged_by") or operator_label,
                    "acknowledgement_note": record.get("acknowledgement_note") or reason,
                    "override_observational_only": True,
                    "override_reason": reason,
                    "override_set_at": datetime.now(timezone.utc).isoformat(),
                    "override_set_by": operator_label,
                    "hold_expired": False,
                    "hold_expired_at": None,
                    "hold_expiry_enforced": False,
                    "hold_state_reason": reason,
                }
            ),
            event="conflict_marked_observational_only",
            operator_label=operator_label,
            note=reason,
            auth_metadata=auth_metadata,
        )
        return self._result_record(
            action="same-underlying-mark-observational",
            ok=True,
            command=None,
            output=f"Same-underlying conflict on {instrument} marked observational-only for operator review.",
        )

    def _hold_same_underlying_entries(self, payload: dict[str, Any]) -> dict[str, Any]:
        conflict = self._same_underlying_conflict_row(str(payload.get("instrument") or ""))
        operator_label = self._operator_label_from_payload(payload)
        auth_metadata = self._local_auth_metadata_from_payload(payload)
        hold_reason = self._note_from_payload(payload, "hold_reason", "note", "reason") or (
            f"New entries held by operator for same-underlying conflict review on {conflict.get('instrument')}."
        )
        instrument = str(conflict.get("instrument") or "")
        self._update_same_underlying_conflict_review_record(
            instrument=instrument,
            current_conflict=conflict,
            mutate=lambda record, _: record.update(
                {
                    "acknowledged": True,
                    "acknowledged_at": record.get("acknowledged_at") or datetime.now(timezone.utc).isoformat(),
                    "acknowledged_by": record.get("acknowledged_by") or operator_label,
                    "acknowledgement_note": record.get("acknowledgement_note") or hold_reason,
                    "hold_new_entries": True,
                    "hold_reason": hold_reason,
                    "hold_set_at": datetime.now(timezone.utc).isoformat(),
                    "hold_set_by": operator_label,
                    "hold_expires_at": payload.get("hold_expires_at"),
                    "hold_expired": False,
                    "hold_expired_at": None,
                    "hold_expiry_enforced": False,
                    "hold_state_reason": hold_reason,
                }
            ),
            event="conflict_hold_set",
            operator_label=operator_label,
            note=hold_reason,
            auth_metadata=auth_metadata,
        )
        return self._result_record(
            action="same-underlying-hold-entries",
            ok=True,
            command=None,
            output=f"New entries are now held on {instrument} until the same-underlying conflict is cleared.",
        )

    def _clear_same_underlying_hold(self, payload: dict[str, Any]) -> dict[str, Any]:
        conflict = self._same_underlying_conflict_row(str(payload.get("instrument") or ""))
        operator_label = self._operator_label_from_payload(payload)
        auth_metadata = self._local_auth_metadata_from_payload(payload)
        note = self._note_from_payload(payload, "note", "reason")
        instrument = str(conflict.get("instrument") or "")
        self._update_same_underlying_conflict_review_record(
            instrument=instrument,
            current_conflict=conflict,
            mutate=lambda record, _: record.update(
                {
                    "hold_new_entries": False,
                    "hold_reason": None,
                    "hold_set_at": None,
                    "hold_set_by": None,
                    "hold_expires_at": None,
                    "hold_expired": False,
                    "hold_expired_at": None,
                    "hold_expiry_enforced": False,
                    "hold_state_reason": note or f"Same-underlying entry hold cleared for {instrument}.",
                }
            ),
            event="conflict_hold_cleared",
            operator_label=operator_label,
            note=note,
            auth_metadata=auth_metadata,
        )
        return self._result_record(
            action="same-underlying-clear-hold",
            ok=True,
            command=None,
            output=f"Same-underlying entry hold cleared for {instrument}.",
        )

    def _reset_same_underlying_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        conflict = self._same_underlying_conflict_row(str(payload.get("instrument") or ""))
        operator_label = self._operator_label_from_payload(payload)
        auth_metadata = self._local_auth_metadata_from_payload(payload)
        note = self._note_from_payload(payload, "note", "reason")
        instrument = str(conflict.get("instrument") or "")
        self._update_same_underlying_conflict_review_record(
            instrument=instrument,
            current_conflict=conflict,
            mutate=lambda record, _: record.update(
                {
                    "acknowledged": False,
                    "acknowledged_at": None,
                    "acknowledged_by": None,
                    "acknowledgement_note": None,
                    "hold_new_entries": False,
                    "hold_reason": None,
                    "hold_set_at": None,
                    "hold_set_by": None,
                    "hold_expires_at": None,
                    "override_observational_only": False,
                    "override_reason": None,
                    "override_set_at": None,
                    "override_set_by": None,
                    "hold_expired": False,
                    "hold_expired_at": None,
                    "hold_expiry_enforced": False,
                    "hold_state_reason": None,
                }
            ),
            event="conflict_review_reset",
            operator_label=operator_label,
            note=note,
            auth_metadata=auth_metadata,
        )
        return self._result_record(
            action="same-underlying-reset-review",
            ok=True,
            command=None,
            output=f"Same-underlying review state reset for {instrument}.",
        )

    def _align_approved_models_to_lane_registry(
        self,
        *,
        payload: dict[str, Any],
        lane_registry: dict[str, Any],
    ) -> dict[str, Any]:
        section = _lane_registry_section(lane_registry, "admitted_paper")
        registry_rows = list(section.get("rows", []))
        registry_lane_ids = [str(row.get("lane_id") or "") for row in registry_rows if row.get("lane_id")]
        row_index = {
            str(row.get("lane_id") or ""): row
            for row in list(payload.get("rows", []))
            if row.get("lane_id")
        }
        aligned_rows = [row_index[lane_id] for lane_id in registry_lane_ids if lane_id in row_index]
        aligned_branches = {str(row.get("branch") or "") for row in aligned_rows if row.get("branch")}
        details_by_branch = {
            key: value
            for key, value in dict(payload.get("details_by_branch") or {}).items()
            if key in aligned_branches
        }
        result = dict(payload)
        result["rows"] = aligned_rows
        result["details_by_branch"] = details_by_branch
        result["enabled_count"] = sum(1 for row in aligned_rows if row.get("enabled"))
        result["total_count"] = len(aligned_rows)
        result["surface_alignment"] = {
            "surface_group": "admitted_paper",
            "registry_row_count": len(registry_rows),
            "detail_row_count": len(aligned_rows),
            "registry_lane_ids": registry_lane_ids,
            "aligned": len(registry_rows) == len(aligned_rows),
        }
        return result

    def _align_non_approved_lanes_to_lane_registry(
        self,
        *,
        payload: dict[str, Any],
        lane_registry: dict[str, Any],
    ) -> dict[str, Any]:
        section = _lane_registry_section(lane_registry, "canary")
        registry_rows = list(section.get("rows", []))
        registry_lane_ids = [str(row.get("lane_id") or "") for row in registry_rows if row.get("lane_id")]
        row_index = {
            str(row.get("lane_id") or ""): row
            for row in list(payload.get("rows", []))
            if row.get("lane_id")
        }
        registry_index = {
            str(row.get("lane_id") or ""): row
            for row in registry_rows
            if row.get("lane_id")
        }
        aligned_rows = []
        for lane_id in registry_lane_ids:
            if lane_id not in row_index:
                continue
            detail_row = dict(row_index[lane_id])
            registry_row = dict(registry_index.get(lane_id) or {})
            runtime_instance_present = bool(
                registry_row.get("runtime_instance_present", detail_row.get("runtime_instance_present", False))
            )
            runtime_state_loaded = bool(
                registry_row.get("runtime_state_loaded", detail_row.get("runtime_state_loaded", False))
            )
            can_process_bars = bool(
                registry_row.get("can_process_bars", detail_row.get("can_process_bars", runtime_instance_present))
            )
            snapshot_only = _is_temporary_paper_strategy_row(detail_row) and not runtime_instance_present
            note = str(detail_row.get("note") or "").strip()
            if snapshot_only:
                snapshot_note = "Snapshot Only | Not Loaded In Runtime."
                if snapshot_note not in note:
                    note = f"{snapshot_note} {note}".strip()
            detail_row.update(
                {
                    "runtime_instance_present": runtime_instance_present,
                    "runtime_state_loaded": runtime_state_loaded,
                    "can_process_bars": can_process_bars,
                    "config_source": registry_row.get("config_source") or detail_row.get("config_source"),
                    "runtime_kind": registry_row.get("runtime_kind") or detail_row.get("runtime_kind"),
                    "snapshot_only": snapshot_only,
                    "note": note,
                }
            )
            aligned_rows.append(detail_row)
        result = dict(payload)
        result["rows"] = aligned_rows
        result["total_count"] = len(aligned_rows)
        result["canary_count"] = sum(1 for row in aligned_rows if row.get("is_canary"))
        result["fired_count"] = sum(1 for row in aligned_rows if row.get("fired"))
        result["completed_count"] = sum(1 for row in aligned_rows if row.get("entry_completed") and row.get("exit_completed"))
        result["surface_alignment"] = {
            "surface_group": "canary",
            "registry_row_count": len(registry_rows),
            "detail_row_count": len(aligned_rows),
            "registry_lane_ids": registry_lane_ids,
            "aligned": len(registry_rows) == len(aligned_rows),
        }
        return result

    def _lane_registry_diagnostics(
        self,
        *,
        lane_registry: dict[str, Any],
        approved_quant_baselines: dict[str, Any],
        paper_approved_models: dict[str, Any],
        paper_non_approved_lanes: dict[str, Any],
    ) -> dict[str, Any]:
        approved_quant_section = _lane_registry_section(lane_registry, "approved_quant")
        admitted_paper_section = _lane_registry_section(lane_registry, "admitted_paper")
        canary_section = _lane_registry_section(lane_registry, "canary")
        return {
            "approved_quant": {
                "source_row_count": len(list(approved_quant_baselines.get("rows", []))),
                "registry_row_count": len(list(approved_quant_section.get("rows", []))),
            },
            "admitted_paper": {
                "registry_row_count": len(list(admitted_paper_section.get("rows", []))),
                "detail_row_count": len(list((paper_approved_models.get("rows") or []))),
                "surface_alignment": paper_approved_models.get("surface_alignment") or {},
            },
            "canary": {
                "registry_row_count": len(list(canary_section.get("rows", []))),
                "detail_row_count": len(list((paper_non_approved_lanes.get("rows") or []))),
                "surface_alignment": paper_non_approved_lanes.get("surface_alignment") or {},
            },
        }

    def latest_summary_file(self, runtime: str, format_name: str, session_date: str | None = None) -> tuple[Path | None, str]:
        artifacts_dir = self._runtime_paths(runtime)["artifacts_dir"]
        daily_dir = artifacts_dir / "daily"
        if format_name == "json":
            files = sorted(daily_dir.glob(f"{session_date}.summary.json" if session_date else "*.summary.json"))
            return (files[-1], "application/json; charset=utf-8") if files else (None, "application/json; charset=utf-8")
        if format_name == "md":
            files = sorted(daily_dir.glob(f"{session_date}.summary.md" if session_date else "*.summary.md"))
            return (files[-1], "text/markdown; charset=utf-8") if files else (None, "text/markdown; charset=utf-8")
        if format_name == "blotter":
            files = sorted(daily_dir.glob(f"{session_date}.blotter.csv" if session_date else "*.blotter.csv"))
            return (files[-1], "text/csv; charset=utf-8") if files else (None, "text/csv; charset=utf-8")
        return None, "text/plain; charset=utf-8"

    def operator_artifact_file(self, artifact_name: str) -> tuple[Path | None, str]:
        if artifact_name == "historical-playback-manifest":
            return self._historical_playback_artifact_path("manifest")
        if artifact_name == "historical-playback-summary":
            return self._historical_playback_artifact_path("summary")
        if artifact_name == "historical-playback-trigger-report":
            return self._historical_playback_artifact_path("trigger_report_json")
        if artifact_name == "historical-playback-trigger-report-md":
            return self._historical_playback_artifact_path("trigger_report_markdown")
        if artifact_name == "historical-playback-strategy-study":
            return self._historical_playback_artifact_path("strategy_study_json")
        if artifact_name == "historical-playback-strategy-study-md":
            return self._historical_playback_artifact_path("strategy_study_markdown")
        mapping = {
            "historical-playback-snapshot": (self._historical_playback_snapshot_path, "application/json; charset=utf-8"),
            "paper-run-starts": (self._paper_run_starts_path, "application/x-ndjson; charset=utf-8"),
            "paper-run-start-blocks": (self._paper_run_start_blocks_path, "application/x-ndjson; charset=utf-8"),
            "paper-current-run-start": (self._paper_current_run_path, "application/json; charset=utf-8"),
            "paper-runtime-recovery": (self._paper_runtime_recovery_path, "application/json; charset=utf-8"),
            "paper-runtime-supervisor-events": (self._paper_runtime_supervisor_events_path, "application/x-ndjson; charset=utf-8"),
            "paper-restore-validation": (self._runtime_paths("paper")["artifacts_dir"] / "restore_validation_latest.json", "application/json; charset=utf-8"),
            "paper-restore-validation-events": (self._runtime_paths("paper")["artifacts_dir"] / "restore_validation_events.jsonl", "application/x-ndjson; charset=utf-8"),
            "paper-carry-forward": (self._carry_forward_path, "application/json; charset=utf-8"),
            "paper-pre-session-review": (self._pre_session_review_path, "application/json; charset=utf-8"),
            "paper-session-continuity": (self._paper_continuity_path, "application/json; charset=utf-8"),
            "paper-performance": (self._paper_performance_path, "application/json; charset=utf-8"),
            "paper-strategy-performance": (self._paper_strategy_performance_path, "application/json; charset=utf-8"),
            "paper-strategy-trade-log": (self._paper_strategy_trade_log_path, "application/json; charset=utf-8"),
            "paper-strategy-attribution": (self._paper_strategy_attribution_path, "application/json; charset=utf-8"),
            "paper-signal-intent-fill-audit": (self._paper_signal_intent_fill_audit_path, "application/json; charset=utf-8"),
            "paper-exit-parity-summary": (self._paper_exit_parity_summary_path, "application/json; charset=utf-8"),
            "paper-broker-truth-shadow-validation": (self._paper_broker_truth_shadow_validation_path, "application/json; charset=utf-8"),
            "shadow-live-shadow-summary": (self._shadow_live_shadow_summary_path, "application/json; charset=utf-8"),
            "shadow-live-strategy-pilot-summary": (self._shadow_live_strategy_pilot_summary_path, "application/json; charset=utf-8"),
            "signal-selectivity-analysis": (self._signal_selectivity_analysis_path, "application/json; charset=utf-8"),
            "paper-live-timing-summary": (self._paper_live_timing_summary_path, "application/json; charset=utf-8"),
            "paper-live-timing-validation": (self._paper_live_timing_validation_path, "application/json; charset=utf-8"),
            "paper-soak-validation": (self._paper_soak_validation_path, "application/json; charset=utf-8"),
            "paper-soak-extended": (self._paper_soak_extended_path, "application/json; charset=utf-8"),
            "paper-soak-unattended": (self._paper_soak_unattended_path, "application/json; charset=utf-8"),
            "paper-history": (self._paper_history_path, "application/json; charset=utf-8"),
            "paper-session-shape": (self._paper_session_shape_path, "application/json; charset=utf-8"),
            "paper-session-branch-contribution": (self._paper_session_branch_contribution_path, "application/json; charset=utf-8"),
            "paper-session-event-timeline": (self._paper_session_event_timeline_path, "application/json; charset=utf-8"),
            "paper-readiness": (self._paper_readiness_path, "application/json; charset=utf-8"),
            "paper-approved-models": (self._paper_approved_models_path, "application/json; charset=utf-8"),
            "paper-non-approved-lanes": (self._paper_non_approved_lanes_path, "application/json; charset=utf-8"),
            "paper-temporary-paper-strategies": (
                self._paper_temporary_paper_strategies_path,
                "application/json; charset=utf-8",
            ),
            "paper-tracked-strategies": (self._paper_tracked_strategies_path, "application/json; charset=utf-8"),
            "paper-tracked-strategy-details": (
                self._paper_tracked_strategy_details_path,
                "application/json; charset=utf-8",
            ),
            "strategy-analysis": (self._strategy_analysis_path, "application/json; charset=utf-8"),
            "paper-temporary-paper-runtime-integrity": (
                self._paper_temporary_paper_runtime_integrity_path,
                "application/json; charset=utf-8",
            ),
            "paper-lane-activity": (self._paper_lane_activity_path, "application/json; charset=utf-8"),
            "paper-exceptions": (self._paper_exceptions_path, "application/json; charset=utf-8"),
            "paper-soak-session": (self._paper_soak_session_path, "application/json; charset=utf-8"),
            "paper-soak-evidence-latest-json": (self._paper_soak_evidence_latest_json_path, "application/json; charset=utf-8"),
            "paper-soak-evidence-latest-md": (self._paper_soak_evidence_latest_md_path, "text/markdown; charset=utf-8"),
            "paper-session-close-review": (self._paper_session_close_review_latest_json_path, "application/json; charset=utf-8"),
            "paper-session-close-review-md": (self._paper_session_close_review_latest_md_path, "text/markdown; charset=utf-8"),
            "paper-session-close-review-history": (self._paper_session_close_review_history_json_path, "application/json; charset=utf-8"),
            "paper-session-close-review-history-md": (self._paper_session_close_review_history_md_path, "text/markdown; charset=utf-8"),
            "paper-latest-fills": (self._paper_latest_fills_path, "application/json; charset=utf-8"),
            "paper-latest-intents": (self._paper_latest_intents_path, "application/json; charset=utf-8"),
            "paper-latest-blotter": (self._paper_latest_blotter_path, "application/json; charset=utf-8"),
            "paper-position-state": (self._paper_position_state_path, "application/json; charset=utf-8"),
            "approved-quant-baselines": (self._approved_quant_baselines_path, "application/json; charset=utf-8"),
            "approved-quant-baselines-current-status": (self._approved_quant_current_status_path, "application/json; charset=utf-8"),
            "approved-quant-baselines-current-status-md": (self._approved_quant_current_status_md_path, "text/markdown; charset=utf-8"),
            "experimental-canaries": (self._experimental_canaries_snapshot_path, "application/json; charset=utf-8"),
            "experimental-canaries-md": (self._experimental_canaries_snapshot_md_path, "text/markdown; charset=utf-8"),
            "experimental-canaries-operator-summary": (self._experimental_canaries_operator_summary_path, "text/markdown; charset=utf-8"),
            "operator-surface": (self._operator_surface_path, "application/json; charset=utf-8"),
            "research-daily-capture-status": (self._research_daily_capture_status_path, "application/json; charset=utf-8"),
            "paper-operator-status": (self._runtime_paths("paper")["artifacts_dir"] / "operator_status.json", "application/json; charset=utf-8"),
            "paper-desk-risk-status": (self._runtime_paths("paper")["artifacts_dir"] / "runtime" / "paper_desk_risk_status.json", "application/json; charset=utf-8"),
            "paper-lane-risk-status": (self._runtime_paths("paper")["artifacts_dir"] / "runtime" / "paper_lane_risk_status.json", "application/json; charset=utf-8"),
            "paper-risk-events": (self._runtime_paths("paper")["artifacts_dir"] / "paper_risk_events.jsonl", "application/x-ndjson; charset=utf-8"),
            "paper-config-in-force": (self._runtime_paths("paper")["artifacts_dir"] / "runtime" / "paper_config_in_force.json", "application/json; charset=utf-8"),
            "paper-branch-sources": (self._runtime_paths("paper")["artifacts_dir"] / "branch_sources.jsonl", "application/x-ndjson; charset=utf-8"),
            "paper-rule-blocks": (self._runtime_paths("paper")["artifacts_dir"] / "rule_blocks.jsonl", "application/x-ndjson; charset=utf-8"),
            "paper-alerts": (self._runtime_paths("paper")["artifacts_dir"] / "alerts.jsonl", "application/x-ndjson; charset=utf-8"),
            "paper-reconciliation": (self._runtime_paths("paper")["artifacts_dir"] / "reconciliation_events.jsonl", "application/x-ndjson; charset=utf-8"),
            "market-index-strip": (self._market_index_strip_path, "application/json; charset=utf-8"),
            "market-index-strip-diagnostics": (self._market_index_diagnostics_path, "application/json; charset=utf-8"),
            "treasury-curve": (self._treasury_curve_path, "application/json; charset=utf-8"),
            "treasury-curve-diagnostics": (self._treasury_curve_diagnostics_path, "application/json; charset=utf-8"),
            "treasury-symbol-audit": (self._treasury_symbol_audit_path, "application/json; charset=utf-8"),
        }
        return mapping.get(artifact_name, (None, "text/plain; charset=utf-8"))

    def _historical_playback_artifact_path(self, artifact_key: str) -> tuple[Path | None, str]:
        payload = self._historical_playback_payload()
        run = payload.get("latest_run") or {}
        artifact_path = run.get("artifact_paths", {}).get(artifact_key)
        path = Path(artifact_path) if artifact_path else None
        if artifact_key.endswith("_markdown"):
            return path, "text/markdown; charset=utf-8"
        return path, "application/json; charset=utf-8"

    def _historical_playback_payload(self) -> dict[str, Any]:
        snapshot_artifact = "/api/operator-artifact/historical-playback-snapshot"
        manifest_path = self._latest_historical_playback_manifest_path()
        study_catalog_items = self._historical_playback_study_catalog_items()
        selected_catalog_entry = study_catalog_items[0] if study_catalog_items else None
        study_catalog_payload = _historical_playback_study_catalog_payload(study_catalog_items)
        if manifest_path is None:
            strategy_study_status = _historical_strategy_study_status(
                selected_catalog_entry.get("study") if selected_catalog_entry is not None else None,
                run_loaded=False,
            )
            return {
                "available": False,
                "note": "No historical-playback manifest found under outputs/historical_playback yet.",
                "latest_run": None,
                "artifacts": {"snapshot": snapshot_artifact},
                "strategy_study_status": strategy_study_status,
                "study_catalog": study_catalog_payload,
            }

        manifest = _read_json(manifest_path)
        run_stamp = str(manifest.get("run_stamp") or manifest_path.stem)
        run_timestamp = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc).isoformat()
        symbol_entries = list(manifest.get("symbols") or [])
        rows: list[dict[str, Any]] = []
        symbols: list[str] = []
        total_processed_bars = 0
        total_signals_seen = 0
        total_intents_created = 0
        total_fills_created = 0
        fired_count = 0
        no_fire_count = 0
        blocked_count = 0
        artifact_paths: dict[str, str] = {"manifest": str(manifest_path)}
        replay_summary_payload: dict[str, Any] | None = None
        strategy_study_payload: dict[str, Any] | None = None

        for entry in symbol_entries:
            symbol = str(entry.get("symbol") or "-")
            symbols.append(symbol)
            summary_path = _path_or_none(entry.get("summary_path"))
            trigger_report_path = _path_or_none(entry.get("trigger_report_json_path"))
            trigger_report_markdown_path = _path_or_none(entry.get("trigger_report_markdown_path"))
            strategy_study_json_path = _path_or_none(entry.get("strategy_study_json_path"))
            strategy_study_markdown_path = _path_or_none(entry.get("strategy_study_markdown_path"))
            summary_payload: dict[str, Any] | None = None
            if summary_path is not None and summary_path.exists():
                artifact_paths.setdefault("summary", str(summary_path))
                summary_payload = _read_json(summary_path)
                if replay_summary_payload is None and isinstance(summary_payload.get("aggregate_portfolio_summary"), dict):
                    replay_summary_payload = dict(summary_payload)
                total_processed_bars += int(summary_payload.get("processed_bars") or 0)
                if (
                    strategy_study_json_path is None
                    or strategy_study_markdown_path is None
                    or not strategy_study_json_path.exists()
                    or not strategy_study_markdown_path.exists()
                ):
                    rebuilt_json_path, rebuilt_markdown_path = ensure_strategy_study_artifacts(
                        summary_path=summary_path,
                        summary_payload=summary_payload,
                    )
                    if rebuilt_json_path is not None:
                        strategy_study_json_path = rebuilt_json_path
                    if rebuilt_markdown_path is not None:
                        strategy_study_markdown_path = rebuilt_markdown_path
            else:
                total_processed_bars += int(entry.get("processed_bars") or 0)
            if trigger_report_path is not None and trigger_report_path.exists():
                artifact_paths.setdefault("trigger_report_json", str(trigger_report_path))
                trigger_rows = _read_json_list(trigger_report_path)
            else:
                trigger_rows = []
            if trigger_report_markdown_path is not None and trigger_report_markdown_path.exists():
                artifact_paths.setdefault("trigger_report_markdown", str(trigger_report_markdown_path))
            if strategy_study_json_path is not None and strategy_study_json_path.exists():
                artifact_paths.setdefault("strategy_study_json", str(strategy_study_json_path))
                if strategy_study_payload is None:
                    strategy_study_payload = normalize_strategy_study_payload(_read_json(strategy_study_json_path))
            if strategy_study_markdown_path is not None and strategy_study_markdown_path.exists():
                artifact_paths.setdefault("strategy_study_markdown", str(strategy_study_markdown_path))

            for row in trigger_rows:
                normalized = dict(row)
                normalized["result_status"] = _historical_playback_result_status(normalized)
                rows.append(normalized)
                total_signals_seen += int(normalized.get("signals_seen") or 0)
                total_intents_created += int(normalized.get("intents_created") or 0)
                total_fills_created += int(normalized.get("fills_created") or 0)
                if normalized["result_status"] == "FIRED":
                    fired_count += 1
                elif normalized["result_status"] == "BLOCKED":
                    blocked_count += 1
                else:
                    no_fire_count += 1

        rows.sort(
            key=lambda row: (
                0 if row.get("result_status") == "FIRED" else 1 if row.get("result_status") == "BLOCKED" else 2,
                str(row.get("symbol") or ""),
                str(row.get("lane_family") or ""),
            )
        )
        replay_summary_available = replay_summary_payload is not None
        replay_aggregate = (
            dict(replay_summary_payload.get("aggregate_portfolio_summary") or {})
            if replay_summary_payload is not None
            else {}
        )
        replay_per_strategy = list(replay_summary_payload.get("per_strategy_summaries") or []) if replay_summary_payload is not None else []
        primary_standalone_strategy_id = (
            replay_summary_payload.get("primary_standalone_strategy_id")
            if replay_summary_payload is not None
            else None
        )
        selected_study_payload = (
            dict(selected_catalog_entry.get("study") or {})
            if selected_catalog_entry is not None
            else strategy_study_payload
        )
        selected_study_status = _historical_strategy_study_status(selected_study_payload, run_loaded=True)
        selected_artifact_paths = dict(selected_catalog_entry.get("artifact_paths") or {}) if selected_catalog_entry is not None else {}
        for artifact_key, artifact_value in selected_artifact_paths.items():
            artifact_paths[f"selected_{artifact_key}"] = artifact_value

        return {
            "available": True,
            "note": "Historical playback results are shown separately from strategy-ledger, runtime, and live broker operator state.",
            "artifacts": {"snapshot": snapshot_artifact},
            "strategy_study_status": selected_study_status,
            "study_catalog": study_catalog_payload,
            "selected_study": selected_study_payload,
            "latest_run": {
                "run_stamp": run_stamp,
                "run_timestamp": run_timestamp,
                "symbol_count": len(symbol_entries),
                "symbols": symbols,
                "bars_processed": total_processed_bars,
                "signals_seen": total_signals_seen,
                "intents_created": total_intents_created,
                "fills_created": total_fills_created,
                "fired_count": fired_count,
                "no_fire_count": no_fire_count,
                "blocked_count": blocked_count,
                "rows": rows,
                "truth_label": "REPLAY",
                "replay_summary_available": replay_summary_available,
                "primary_standalone_strategy_id": primary_standalone_strategy_id,
                "aggregate_portfolio_summary": replay_aggregate,
                "per_strategy_summaries": replay_per_strategy,
                "strategy_study": selected_study_payload,
                "strategy_study_status": selected_study_status,
                "strategy_study_available": selected_study_payload is not None,
                "artifact_paths": artifact_paths,
                "artifacts": {
                    "snapshot": snapshot_artifact,
                    "manifest": "/api/operator-artifact/historical-playback-manifest",
                    "summary": "/api/operator-artifact/historical-playback-summary" if artifact_paths.get("summary") else None,
                    "trigger_report_json": (
                        "/api/operator-artifact/historical-playback-trigger-report"
                        if artifact_paths.get("trigger_report_json")
                        else None
                    ),
                    "trigger_report_markdown": (
                        "/api/operator-artifact/historical-playback-trigger-report-md"
                        if artifact_paths.get("trigger_report_markdown")
                        else None
                    ),
                    "strategy_study_json": (
                        "/api/operator-artifact/historical-playback-strategy-study"
                        if artifact_paths.get("strategy_study_json")
                        else None
                    ),
                    "strategy_study_markdown": (
                        "/api/operator-artifact/historical-playback-strategy-study-md"
                        if artifact_paths.get("strategy_study_markdown")
                        else None
                    ),
                },
            },
        }

    def _latest_historical_playback_manifest_path(self) -> Path | None:
        if not self._historical_playback_dir.exists():
            return None
        manifest_paths = sorted(
            self._historical_playback_dir.glob("historical_playback_*.manifest.json"),
            key=lambda path: path.stat().st_mtime,
        )
        return manifest_paths[-1] if manifest_paths else None

    def _historical_playback_study_catalog_items(self) -> list[dict[str, Any]]:
        if not self._historical_playback_dir.exists():
            return []
        manifest_paths = sorted(
            self._historical_playback_dir.glob("historical_playback_*.manifest.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        items: list[dict[str, Any]] = []
        for manifest_path in manifest_paths:
            manifest = _read_json(manifest_path)
            run_stamp = str(manifest.get("run_stamp") or manifest_path.stem)
            run_timestamp = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc).isoformat()
            for entry in list(manifest.get("symbols") or []):
                summary_path = _path_or_none(entry.get("summary_path"))
                summary_payload: dict[str, Any] | None = None
                if summary_path is not None and summary_path.exists():
                    summary_payload = _read_json(summary_path)
                strategy_study_json_path = _path_or_none(entry.get("strategy_study_json_path"))
                strategy_study_markdown_path = _path_or_none(entry.get("strategy_study_markdown_path"))
                if (
                    summary_path is not None
                    and summary_path.exists()
                    and (
                        strategy_study_json_path is None
                        or strategy_study_markdown_path is None
                        or not strategy_study_json_path.exists()
                        or not strategy_study_markdown_path.exists()
                    )
                ):
                    rebuilt_json_path, rebuilt_markdown_path = ensure_strategy_study_artifacts(
                        summary_path=summary_path,
                        summary_payload=summary_payload,
                    )
                    if rebuilt_json_path is not None:
                        strategy_study_json_path = rebuilt_json_path
                    if rebuilt_markdown_path is not None:
                        strategy_study_markdown_path = rebuilt_markdown_path
                if strategy_study_json_path is None or not strategy_study_json_path.exists():
                    continue
                normalized_study = normalize_strategy_study_payload(_read_json(strategy_study_json_path))
                if normalized_study is None:
                    continue
                meta = dict(normalized_study.get("meta") or {})
                timeframe_truth = dict(meta.get("timeframe_truth") or {})
                coverage = dict(meta.get("coverage_range") or {})
                symbol = str(meta.get("symbol") or normalized_study.get("symbol") or entry.get("symbol") or "-")
                strategy_id = meta.get("strategy_id")
                strategy_family = meta.get("strategy_family") or normalized_study.get("strategy_family")
                study_mode = str(meta.get("study_mode") or "baseline_parity_mode")
                scope_label = (
                    "Research Execution"
                    if study_mode == "research_execution_mode"
                    else "Live Execution"
                    if study_mode == "live_execution_mode"
                    else "Legacy Benchmark"
                )
                study_key = str(meta.get("study_id") or f"{run_stamp}:{symbol}:{strategy_id or strategy_family or 'study'}")
                items.append(
                    {
                        "study_key": study_key,
                        "label": " / ".join(
                            part
                            for part in (
                                symbol,
                                str(strategy_id or strategy_family or "study"),
                                study_mode,
                                str(meta.get("entry_model") or ""),
                            )
                            if part
                        ),
                        "run_stamp": run_stamp,
                        "run_timestamp": run_timestamp,
                        "symbol": symbol,
                        "strategy_id": strategy_id,
                        "candidate_id": meta.get("candidate_id"),
                        "scope_label": scope_label,
                        "strategy_family": strategy_family,
                        "contract_version": normalized_study.get("contract_version"),
                        "context_resolution": meta.get("context_resolution"),
                        "execution_resolution": meta.get("execution_resolution"),
                        "timeframe_truth": timeframe_truth,
                        "coverage_start": meta.get("coverage_start") or coverage.get("start_timestamp"),
                        "coverage_end": meta.get("coverage_end") or coverage.get("end_timestamp"),
                        "study_mode": study_mode,
                        "entry_model": meta.get("entry_model"),
                        "supported_entry_models": list(meta.get("supported_entry_models") or []),
                        "entry_model_supported": bool(meta.get("entry_model_supported", True)),
                        "execution_truth_emitter": meta.get("execution_truth_emitter"),
                        "intrabar_execution_authoritative": bool(meta.get("intrabar_execution_authoritative")),
                        "authoritative_intrabar_available": bool(meta.get("authoritative_intrabar_available")),
                        "authoritative_entry_truth_available": bool(meta.get("authoritative_entry_truth_available")),
                        "authoritative_exit_truth_available": bool(meta.get("authoritative_exit_truth_available")),
                        "authoritative_trade_lifecycle_available": bool(meta.get("authoritative_trade_lifecycle_available")),
                        "pnl_truth_basis": meta.get("pnl_truth_basis"),
                        "lifecycle_truth_class": meta.get("lifecycle_truth_class"),
                        "unsupported_reason": meta.get("unsupported_reason"),
                        "active_entry_model": meta.get("active_entry_model") or meta.get("entry_model"),
                        "truth_provenance": dict(meta.get("truth_provenance") or {}),
                        "entry_model_capabilities": list(meta.get("entry_model_capabilities") or []),
                        "available_overlay_flags": dict(meta.get("available_overlay_flags") or {}),
                        "artifact_paths": {
                            "manifest": str(manifest_path),
                            "summary": str(summary_path) if summary_path is not None else None,
                            "strategy_study_json": str(strategy_study_json_path),
                            "strategy_study_markdown": str(strategy_study_markdown_path) if strategy_study_markdown_path is not None else None,
                        },
                        "summary": dict(normalized_study.get("summary") or {}),
                        "study": normalized_study,
                    }
                )
        return items

    def _active_runtime(self, paper: dict[str, Any], shadow: dict[str, Any]) -> dict[str, Any]:
        if paper["running"]:
            return paper
        if shadow["running"]:
            return shadow
        paper_update = paper["status"]["last_update_ts"]
        shadow_update = shadow["status"]["last_update_ts"]
        if paper_update and shadow_update:
            return paper if paper_update >= shadow_update else shadow
        if paper_update:
            return paper
        return shadow

    def _manual_controls_snapshot(self, paper: dict[str, Any]) -> dict[str, Any]:
        paper_status = paper["status"]
        paper_running = paper["running"]
        operator_halt = bool(paper_status["operator_halt"])
        faulted = paper_status["fault_state"] == "FAULTED"
        flatten_pending = paper["operator_state"]["flatten_pending"]
        has_open_position = paper["position"]["side"] != "FLAT"
        stop_after_cycle_requested = bool(paper["operator_state"]["stop_after_cycle_requested"])
        desk_risk = paper.get("desk_risk") or {}
        lane_risk = paper.get("lane_risk") or {}
        active_lane_risk = any(
            str(row.get("risk_state") or "OK").startswith("HALTED")
            for row in (lane_risk.get("lanes") or [])
        )
        desk_risk_active = str(desk_risk.get("desk_risk_state") or "OK") in {"HALT_NEW_ENTRIES", "FLATTEN_AND_HALT"}
        return {
            "mode": "PAPER",
            "controls": [
                {
                    "label": "Buy",
                    "action": None,
                    "enabled": False,
                    "reason": "Manual paper order entry is not wired yet.",
                },
                {
                    "label": "Sell",
                    "action": None,
                    "enabled": False,
                    "reason": "Manual paper order entry is not wired yet.",
                },
                {
                    "label": "Flatten And Halt",
                    "action": "paper-flatten-and-halt",
                    "enabled": paper_running and (has_open_position or paper_status["operator_halt"] is False) and not flatten_pending,
                    "reason": None
                    if paper_running and (has_open_position or paper_status["operator_halt"] is False) and not flatten_pending
                    else "Flatten And Halt is only available when paper runtime is running and no flatten request is already pending.",
                },
                {
                    "label": "Halt Entries",
                    "action": "paper-halt-entries",
                    "enabled": paper_running and not operator_halt,
                    "reason": None
                    if paper_running and not operator_halt
                    else "Paper runtime is not running or entries are already halted.",
                },
                {
                    "label": "Resume Entries",
                    "action": "paper-resume-entries",
                    "enabled": paper_running and operator_halt,
                    "reason": None
                    if paper_running and operator_halt
                    else "Paper runtime is not running or entries are already enabled.",
                },
                {
                    "label": "Acknowledge/Clear Fault",
                    "action": "paper-clear-fault",
                    "enabled": paper_running and faulted,
                    "reason": None
                    if paper_running and faulted
                    else "Clear fault is only available when paper runtime is faulted.",
                },
                {
                    "label": "Clear Risk Halts",
                    "action": "paper-clear-risk-halts",
                    "enabled": paper_running and (desk_risk_active or active_lane_risk),
                    "reason": None
                    if paper_running and (desk_risk_active or active_lane_risk)
                    else "Clear Risk Halts is only available when a paper risk halt is currently active.",
                },
                {
                    "label": "Stop After Current Cycle",
                    "action": "paper-stop-after-cycle",
                    "enabled": paper_running and not stop_after_cycle_requested,
                    "reason": None
                    if paper_running and not stop_after_cycle_requested
                    else "Stop After Current Cycle is only available when paper runtime is running and no stop request is already active.",
                },
                {
                    "label": "Kill Switch",
                    "action": None,
                    "enabled": False,
                    "reason": "Live routing is disabled and a paper kill-switch path is not yet separated.",
                },
            ],
        }

    def _paper_operator_state_snapshot(self, paper: dict[str, Any]) -> dict[str, Any]:
        return paper["operator_state"]

    def _review_payload(self, runtime: dict[str, Any], runtime_name: str) -> dict[str, Any]:
        summary = runtime["daily_summary"]
        return {
            "runtime": runtime_name.upper(),
            "available": summary is not None,
            "summary": summary,
            "links": {
                "json": f"/api/summary/{runtime_name}/json",
                "md": f"/api/summary/{runtime_name}/md",
                "blotter": f"/api/summary/{runtime_name}/blotter",
            },
        }

    def _paper_config_in_force_fallback(self, artifacts_dir: Path, db_path: Path | None) -> dict[str, Any]:
        config_paths = [
            self._repo_root / "config" / "base.yaml",
            self._repo_root / "config" / "live.yaml",
            self._repo_root / "config" / "probationary_pattern_engine.yaml",
            self._repo_root / "config" / "probationary_pattern_engine_paper.yaml",
        ]
        if not all(path.exists() for path in config_paths):
            return {}
        try:
            settings = load_settings_from_files(config_paths)
        except Exception:
            return {}
        lanes: list[dict[str, Any]] = []
        for row in settings.probationary_paper_lane_specs:
            lane_id = str(row.get("lane_id") or "")
            if not lane_id:
                continue
            lanes.append(
                {
                    "lane_id": lane_id,
                    "display_name": row.get("display_name") or lane_id,
                    "symbol": row.get("symbol"),
                    "long_sources": list(row.get("long_sources") or []),
                    "short_sources": list(row.get("short_sources") or []),
                    "session_restriction": row.get("session_restriction"),
                    "point_value": str(row.get("point_value")) if row.get("point_value") is not None else None,
                    "catastrophic_open_loss": (
                        str(row.get("catastrophic_open_loss"))
                        if row.get("catastrophic_open_loss") is not None
                        else None
                    ),
                    "database_url": _derive_probationary_lane_database_url(db_path, lane_id),
                    "artifacts_dir": str(artifacts_dir / "lanes" / lane_id),
                }
            )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "desk_halt_new_entries_loss": str(settings.probationary_paper_desk_halt_new_entries_loss),
            "desk_flatten_and_halt_loss": str(settings.probationary_paper_desk_flatten_and_halt_loss),
            "lane_realized_loser_limit_per_session": settings.probationary_paper_lane_realized_loser_limit_per_session,
            "lane_warning_open_loss": {
                key: str(value) for key, value in settings.probationary_paper_lane_warning_open_loss.items()
            },
            "lanes": lanes,
            "source": "config/probationary_pattern_engine_paper.yaml fallback",
        }

    def _dashboard_base_settings(self):
        config_paths = [
            self._repo_root / "config" / "base.yaml",
            self._repo_root / "config" / "live.yaml",
            self._repo_root / "config" / "probationary_pattern_engine.yaml",
            self._repo_root / "config" / "probationary_pattern_engine_paper.yaml",
        ]
        if not all(path.exists() for path in config_paths):
            return None
        try:
            return load_settings_from_files(config_paths)
        except Exception:
            return None

    def _paper_operator_control_config_paths(self) -> list[Path]:
        return [
            self._repo_root / "config" / "base.yaml",
            self._repo_root / "config" / "live.yaml",
            self._repo_root / "config" / "probationary_pattern_engine.yaml",
            self._repo_root / "config" / "probationary_pattern_engine_paper.yaml",
        ]

    def _standalone_runtime_registry_payload(
        self,
        *,
        config_in_force: dict[str, Any],
        include_approved_quant: bool,
    ) -> dict[str, Any]:
        settings = self._dashboard_base_settings()
        if settings is None:
            return {"rows": [], "row_count": 0}
        runtime_definitions = build_standalone_strategy_definitions(
            settings,
            runtime_lanes=list(config_in_force.get("lanes") or []),
            include_approved_quant_runtime_rows=include_approved_quant,
        )
        rows = [
            {
                **definition.runtime_identity,
                "display_name": definition.display_name,
                "enabled": definition.enabled,
                "allowed_sessions": list(definition.allowed_sessions),
                "trade_size": definition.trade_size,
                "runtime_instance_present": True,
                "can_process_bars": definition.runtime_kind in {
                    "strategy_engine",
                    "approved_quant_strategy_engine",
                    ATPE_CANARY_RUNTIME_KIND,
                    ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
                    GC_MGC_ACCEPTANCE_RUNTIME_KIND,
                },
                "runtime_state_loaded": _standalone_runtime_state_loaded(
                    _resolve_sqlite_database_path(definition.database_url),
                    definition.standalone_strategy_id,
                ),
            }
            for definition in runtime_definitions
        ]
        rows = _annotate_same_underlying_strategy_ambiguity(rows)
        return {
            "rows": rows,
            "row_count": len(rows),
            "summary": _build_runtime_registry_summary(rows),
        }

    def _paper_lane_risk_fallback(self, operator_status: dict[str, Any], config_in_force: dict[str, Any]) -> dict[str, Any]:
        config_lanes = list(config_in_force.get("lanes") or [])
        if not config_lanes:
            return {}
        operator_lane_rows = {
            str(row.get("lane_id")): row
            for row in (operator_status.get("lanes") or [])
            if row.get("lane_id")
        }
        return {
            "updated_at": operator_status.get("updated_at"),
            "session_date": _session_date_from_status(operator_status),
            "lanes": [
                {
                    "lane_id": row.get("lane_id"),
                    "display_name": row.get("display_name"),
                    "symbol": row.get("symbol"),
                    "session_restriction": row.get("session_restriction"),
                    "risk_state": operator_lane_rows.get(str(row.get("lane_id")), {}).get("risk_state", "OK"),
                    "halt_reason": operator_lane_rows.get(str(row.get("lane_id")), {}).get("halt_reason"),
                    "unblock_action": operator_lane_rows.get(str(row.get("lane_id")), {}).get("unblock_action"),
                    "realized_losing_trades": operator_lane_rows.get(str(row.get("lane_id")), {}).get("realized_losing_trades", 0),
                    "catastrophic_open_loss_threshold": row.get("catastrophic_open_loss"),
                    "session_realized_pnl": operator_lane_rows.get(str(row.get("lane_id")), {}).get("session_realized_pnl"),
                    "session_unrealized_pnl": operator_lane_rows.get(str(row.get("lane_id")), {}).get("session_unrealized_pnl"),
                    "session_total_pnl": operator_lane_rows.get(str(row.get("lane_id")), {}).get("session_total_pnl"),
                }
                for row in config_lanes
            ],
            "source": "config fallback",
        }

    def _paper_operator_status_with_lane_fallback(
        self,
        operator_status: dict[str, Any],
        config_in_force: dict[str, Any],
        lane_risk: dict[str, Any],
        artifacts_dir: Path,
        db_path: Path | None,
    ) -> dict[str, Any]:
        lane_rows = list(operator_status.get("lanes") or [])
        if lane_rows:
            return operator_status
        config_lanes = list(config_in_force.get("lanes") or [])
        if not config_lanes:
            return operator_status
        risk_by_lane = {
            str(row.get("lane_id")): row
            for row in (lane_risk.get("lanes") or [])
            if row.get("lane_id")
        }
        merged = dict(operator_status)
        merged["lanes"] = [
            {
                "lane_id": row.get("lane_id"),
                "display_name": row.get("display_name"),
                "symbol": row.get("symbol"),
                "session_restriction": row.get("session_restriction"),
                "approved_long_entry_sources": list(row.get("long_sources") or []),
                "approved_short_entry_sources": list(row.get("short_sources") or []),
                "position_side": "FLAT",
                "entries_enabled": bool(operator_status.get("entries_enabled", False)),
                "operator_halt": bool(operator_status.get("operator_halt", False)),
                "risk_state": risk_by_lane.get(str(row.get("lane_id")), {}).get("risk_state", "OK"),
                "halt_reason": risk_by_lane.get(str(row.get("lane_id")), {}).get("halt_reason"),
                "unblock_action": risk_by_lane.get(str(row.get("lane_id")), {}).get("unblock_action"),
                "realized_losing_trades": risk_by_lane.get(str(row.get("lane_id")), {}).get("realized_losing_trades", 0),
                "catastrophic_open_loss_threshold": (
                    risk_by_lane.get(str(row.get("lane_id")), {}).get("catastrophic_open_loss_threshold")
                    or row.get("catastrophic_open_loss")
                ),
                "artifacts_dir": row.get("artifacts_dir") or str(artifacts_dir / "lanes" / str(row.get("lane_id"))),
                "database_url": row.get("database_url") or _derive_probationary_lane_database_url(db_path, str(row.get("lane_id"))),
            }
            for row in config_lanes
        ]
        merged["paper_lane_count"] = len(merged["lanes"])
        if not merged.get("approved_long_entry_sources"):
            merged["approved_long_entry_sources"] = sorted(
                {
                    source
                    for row in config_lanes
                    for source in list(row.get("long_sources") or [])
                }
            )
        if not merged.get("approved_short_entry_sources"):
            merged["approved_short_entry_sources"] = sorted(
                {
                    source
                    for row in config_lanes
                    for source in list(row.get("short_sources") or [])
                }
            )
        return merged

    def _runtime_snapshot(self, runtime_name: str) -> dict[str, Any]:
        runtime = self._runtime_paths(runtime_name)
        running = _pid_running(runtime["pid_file"])
        operator_status = _read_json(runtime["artifacts_dir"] / "operator_status.json")
        daily_summary = self._latest_daily_summary(runtime["artifacts_dir"])
        blotter_path, full_blotter_rows = self._latest_blotter_dataset(runtime["artifacts_dir"])
        latest_blotter = list(reversed(full_blotter_rows[-20:]))
        latest_events = {
            "branch_sources": _tail_jsonl(runtime["artifacts_dir"] / "branch_sources.jsonl", 20),
            "rule_blocks": _tail_jsonl(runtime["artifacts_dir"] / "rule_blocks.jsonl", 20),
            "alerts": _tail_jsonl(runtime["artifacts_dir"] / "alerts.jsonl", 20),
            "reconciliation": _tail_jsonl(runtime["artifacts_dir"] / "reconciliation_events.jsonl", 20),
            "restore_validation": _tail_jsonl(runtime["artifacts_dir"] / "restore_validation_events.jsonl", 20),
            "operator_controls": _tail_jsonl(runtime["artifacts_dir"] / "operator_controls.jsonl", 20),
        }
        alerts_state = _derive_alert_state_from_jsonl(runtime["artifacts_dir"] / "alerts.jsonl")
        runtime_dir = runtime["artifacts_dir"] / "runtime"
        desk_risk = _read_json(runtime_dir / "paper_desk_risk_status.json") or _read_json(runtime_dir / "paper_desk_risk_snapshot.json")
        lane_risk = _read_json(runtime_dir / "paper_lane_risk_status.json") or _read_json(runtime_dir / "paper_lane_risk_snapshot.json")
        config_in_force = _read_json(runtime_dir / "paper_config_in_force.json")
        db_path = runtime["db_path"]
        if runtime_name == "paper" and (not config_in_force or not list(config_in_force.get("lanes") or [])):
            config_in_force = self._paper_config_in_force_fallback(runtime["artifacts_dir"], db_path)
        if runtime_name == "paper" and (not lane_risk or not list(lane_risk.get("lanes") or [])):
            lane_risk = self._paper_lane_risk_fallback(operator_status, config_in_force)
        if runtime_name == "paper":
            operator_status = self._paper_operator_status_with_lane_fallback(
                operator_status,
                config_in_force,
                lane_risk,
                runtime["artifacts_dir"],
                db_path,
            )
        lane_db_paths = _probationary_lane_database_paths(operator_status, db_path)
        latest_intents = _latest_table_rows_across_paths(lane_db_paths, "order_intents", "created_at", 25)
        latest_fills = _latest_table_rows_across_paths(lane_db_paths, "fills", "fill_timestamp", 25)
        latest_bar = _latest_table_rows_across_paths(lane_db_paths, "bars", "end_ts", 1)
        latest_bar_close = Decimal(str(latest_bar[0]["close"])) if latest_bar else None
        session_date = (
            (daily_summary or {}).get("session_date")
            or _session_date_from_status(operator_status)
            or date.today().isoformat()
        )
        session_intents = _session_table_rows_across_paths(lane_db_paths, "order_intents", "created_at", "created_at", session_date)
        session_fills = _session_table_rows_across_paths(lane_db_paths, "fills", "fill_timestamp", "fill_timestamp", session_date)
        risk_events = _tail_jsonl(runtime["artifacts_dir"] / "paper_risk_events.jsonl", 20) or _tail_jsonl(
            runtime["artifacts_dir"] / "risk_trigger_events.jsonl",
            20,
        )
        freshness = _freshness_semantics(
            operator_status.get("updated_at"),
            poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
            running=running,
        )
        market_data_semantics = _market_data_semantics(
            running=running,
            market_data_ok=bool(_nested_get(operator_status, "health", "market_data_ok", default=False)),
            freshness=freshness["status"],
        )
        reconciliation_semantics = (
            "CLEAN" if bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)) else "DIRTY"
        )
        fault_state = (
            "FAULTED"
            if str(operator_status.get("strategy_status", "")).startswith("FAULT") or operator_status.get("fault_code")
            else "CLEAR"
        )
        position = self._paper_position_payload(operator_status, latest_bar_close, daily_summary, full_blotter_rows)
        latest_operator_control = _read_json(runtime["artifacts_dir"] / "runtime" / "operator_control.json")
        operator_state = self._build_operator_state_payload(position, operator_status, latest_operator_control)
        performance = self._paper_performance_payload(
            session_date=session_date,
            current_session_date=_session_date_from_status(operator_status),
            daily_summary=daily_summary,
            full_blotter_rows=full_blotter_rows,
            blotter_path=blotter_path,
            session_intents=session_intents,
            session_fills=session_fills,
            position=position,
        )
        approved_quant_baselines = (
            load_approved_quant_baselines_snapshot(self._approved_quant_baselines_path)
            if runtime_name == "paper"
            else {}
        )
        runtime_registry = self._standalone_runtime_registry_payload(
            config_in_force=config_in_force,
            include_approved_quant=runtime_name == "paper",
        )
        strategy_performance = self._paper_strategy_performance_payload(
            paper={
                "raw_operator_status": operator_status,
                "config_in_force": config_in_force,
                "lane_risk": lane_risk,
                "position": position,
                "performance": performance,
                "runtime_registry": runtime_registry,
                "status": {
                    "strategy_status": operator_status.get("strategy_status"),
                },
            },
            session_date=session_date,
            root_db_path=db_path,
            approved_quant_baselines=approved_quant_baselines,
        )
        signal_intent_fill_audit = self._paper_signal_intent_fill_audit_payload(
            paper={
                "raw_operator_status": operator_status,
                "config_in_force": config_in_force,
                "lane_risk": lane_risk,
                "position": position,
                "performance": performance,
                "status": {
                    "strategy_status": operator_status.get("strategy_status"),
                    "session_date": session_date,
                },
                "runtime_registry": runtime_registry,
                "strategy_performance": strategy_performance,
            },
            session_date=session_date,
            root_db_path=db_path,
        )
        strategy_runtime_generated_at = datetime.now(timezone.utc).isoformat()
        strategy_runtime_summary = _build_strategy_runtime_summary(
            runtime_registry=runtime_registry,
            strategy_rows=(strategy_performance.get("rows") or []),
            audit_rows=(signal_intent_fill_audit.get("rows") or []),
            generated_at=strategy_runtime_generated_at,
        )
        history = self._paper_history_payload(runtime["artifacts_dir"])
        session_shape = self._paper_session_shape_payload(
            session_date=session_date,
            current_session_date=_session_date_from_status(operator_status),
            daily_summary=daily_summary,
            full_blotter_rows=full_blotter_rows,
            session_intents=session_intents,
            session_fills=session_fills,
            position=position,
            operator_status=operator_status,
        )
        branch_session_contribution = self._paper_session_branch_contribution_payload(
            session_date=session_date,
            current_session_date=_session_date_from_status(operator_status),
            full_blotter_rows=full_blotter_rows,
            session_intents=session_intents,
            session_fills=session_fills,
            position=position,
            operator_status=operator_status,
        )
        live_shadow_summary = (
            self._shadow_live_shadow_summary_payload(
                {
                    "artifacts_dir": str(runtime["artifacts_dir"]),
                    "raw_operator_status": operator_status,
                }
            )
            if runtime_name == "shadow"
            else {}
        )
        live_strategy_pilot_summary = (
            self._shadow_live_strategy_pilot_summary_payload(
                {
                    "artifacts_dir": str(runtime["artifacts_dir"]),
                    "raw_operator_status": operator_status,
                }
            )
            if runtime_name == "shadow"
            else {}
        )

        return {
            "name": runtime_name.upper(),
            "running": running,
            "raw_operator_status": operator_status,
            "pid_file": str(runtime["pid_file"]),
            "log_file": str(runtime["log_file"]),
            "artifacts_dir": str(runtime["artifacts_dir"]),
            "db_path": str(db_path) if db_path is not None else None,
            "process": {
                "pid": _read_pid(runtime["pid_file"]),
                "backgrounded": running,
                "can_stop": running,
                "pid_file": str(runtime["pid_file"]),
                "log_file": str(runtime["log_file"]),
                "artifacts_dir": str(runtime["artifacts_dir"]),
            },
            "status": {
                "health_status": _nested_get(operator_status, "health", "health_status", default="UNKNOWN"),
                "market_data_ok": bool(_nested_get(operator_status, "health", "market_data_ok", default=False)),
                "broker_ok": bool(_nested_get(operator_status, "health", "broker_ok", default=False)),
                "persistence_ok": bool(_nested_get(operator_status, "health", "persistence_ok", default=False)),
                "reconciliation_clean": bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
                "heartbeat_reconciliation": dict(operator_status.get("heartbeat_reconciliation") or {}),
                "heartbeat_reconciliation_status": _nested_get(operator_status, "heartbeat_reconciliation", "status"),
                "heartbeat_reconciliation_classification": _nested_get(operator_status, "heartbeat_reconciliation", "classification"),
                "last_heartbeat_reconcile_at": _nested_get(operator_status, "heartbeat_reconciliation", "last_attempted_at"),
                "order_timeout_watchdog": dict(operator_status.get("order_timeout_watchdog") or {}),
                "order_timeout_watchdog_status": _nested_get(operator_status, "order_timeout_watchdog", "status"),
                "last_order_timeout_check_at": _nested_get(operator_status, "order_timeout_watchdog", "last_checked_at"),
                "startup_restore_validation": dict(operator_status.get("startup_restore_validation") or operator_status.get("startup_restore_validation_summary") or {}),
                "startup_restore_result": _nested_get(operator_status, "startup_restore_validation", "restore_result")
                or _nested_get(operator_status, "startup_restore_validation_summary", "last_restore_result"),
                "last_restore_completed_at": _nested_get(operator_status, "startup_restore_validation", "restore_completed_at")
                or _nested_get(operator_status, "startup_restore_validation_summary", "last_restore_completed_at"),
                "restore_unresolved_issue": bool(
                    _nested_get(operator_status, "startup_restore_validation", "unresolved_restore_issue", default=False)
                    or int(_nested_get(operator_status, "startup_restore_validation_summary", "unresolved_issue_count", default=0) or 0) > 0
                ),
                "strategy_status": operator_status.get("strategy_status", "UNKNOWN"),
                "position_side": operator_status.get("position_side", "UNKNOWN"),
                "last_processed_bar_end_ts": operator_status.get("last_processed_bar_end_ts"),
                "last_update_ts": operator_status.get("updated_at"),
                "session_date": _session_date_from_status(operator_status),
                "entries_enabled": bool(operator_status.get("entries_enabled", False)),
                "operator_halt": bool(operator_status.get("operator_halt", False)),
                "fault_code": operator_status.get("fault_code"),
                "open_paper_order_ids": list(operator_status.get("open_paper_order_ids", []) or []),
                "freshness": freshness["status"],
                "stale": freshness["stale"],
                "artifact_age_seconds": freshness["age_seconds"],
                "market_data_semantics": market_data_semantics,
                "reconciliation_semantics": reconciliation_semantics,
                "fault_state": fault_state,
                "desk_risk_state": operator_status.get("desk_risk_state") or desk_risk.get("desk_risk_state") or "OK",
                "desk_risk_reason": operator_status.get("desk_risk_reason") or desk_risk.get("trigger_reason"),
                "desk_unblock_action": operator_status.get("desk_unblock_action") or desk_risk.get("unblock_action_required"),
            },
            "position": position,
            "operator_state": operator_state,
            "latest_intents": latest_intents,
            "latest_fills": latest_fills,
            "latest_blotter_rows": latest_blotter,
            "full_blotter_rows": full_blotter_rows,
            "blotter_path": str(blotter_path) if blotter_path is not None else None,
            "daily_summary": daily_summary,
            "summary_available": daily_summary is not None,
            "summary_links": {
                "json": f"/api/summary/{runtime_name}/json",
                "md": f"/api/summary/{runtime_name}/md",
                "blotter": f"/api/summary/{runtime_name}/blotter",
            },
            "performance": performance,
            "strategy_performance": strategy_performance,
            "strategy_runtime_summary": strategy_runtime_summary,
            "signal_intent_fill_audit": signal_intent_fill_audit,
            "live_shadow_summary": live_shadow_summary,
            "live_strategy_pilot_summary": live_strategy_pilot_summary,
            "runtime_registry": runtime_registry,
            "history": history,
            "session_shape": session_shape,
            "branch_session_contribution": branch_session_contribution,
            "events": latest_events,
            "alerts_state": alerts_state,
            "restore_validation": dict(operator_status.get("startup_restore_validation") or operator_status.get("startup_restore_validation_summary") or {}),
            "desk_risk": desk_risk,
            "lane_risk": lane_risk,
            "config_in_force": config_in_force,
            "risk_events": risk_events,
            "latest_operator_control": latest_operator_control,
            "controls": {
                "start": f"start-{runtime_name}",
                "stop": f"stop-{runtime_name}",
            },
        }

    def _paper_readiness_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        active_models = [row for row in approved_models.get("rows", []) if row.get("enabled")]
        position = paper.get("position", {})
        operator_state = paper.get("operator_state", {})
        paper_status = paper.get("status", {})
        desk_risk = paper.get("desk_risk") or {}
        lane_risk = paper.get("lane_risk") or {}
        config_in_force = paper.get("config_in_force") or {}
        configured_lanes = list(config_in_force.get("lanes") or [])
        lane_rows = list(lane_risk.get("lanes") or [])
        lane_universe = {
            str(row.get("lane_id")): dict(row)
            for row in self._paper_lane_universe(paper)
            if row.get("lane_id")
        }
        for row in lane_rows:
            lane_id = str(row.get("lane_id") or "")
            if lane_id:
                lane_universe[lane_id] = {**lane_universe.get(lane_id, {}), **dict(row)}
        current_detected_session = (
            (paper.get("raw_operator_status") or {}).get("current_detected_session")
            or paper_status.get("session_phase")
            or "UNKNOWN"
        )
        runtime_lane_ids = {
            str(row.get("lane_id") or "")
            for row in ((paper.get("raw_operator_status") or {}).get("lanes") or [])
            if row.get("lane_id")
        }
        runtime_stale = bool(paper_status.get("stale"))
        runtime_running = bool(paper.get("running"))
        lane_eligibility_rows = []
        lane_status_rows = []
        for row in sorted(
            lane_universe.values(),
            key=lambda entry: (str(entry.get("symbol") or ""), str(entry.get("display_name") or entry.get("lane_id") or "")),
        ):
            lane_id = row.get("lane_id")
            current_strategy_status = str(
                row.get("current_strategy_status")
                or row.get("strategy_status")
                or row.get("state")
                or "UNKNOWN"
            ).strip()
            current_strategy_status_upper = current_strategy_status.upper()
            risk_state = str(row.get("risk_state") or "OK").strip() or "OK"
            risk_state_upper = risk_state.upper()
            eligible_now = bool(row.get("eligible_now"))
            eligibility_reason = row.get("eligibility_reason")
            if not runtime_running:
                eligible_now = False
                eligibility_reason = "stopped_runtime"
            elif runtime_stale:
                eligible_now = False
                eligibility_reason = "stale_runtime"
            loaded_in_runtime = bool(runtime_running and (str(lane_id or "") in runtime_lane_ids or row.get("runtime_instance_present")))
            faulted = current_strategy_status_upper.startswith("FAULT") or str(eligibility_reason or "").strip().lower() == "fault"
            reconciling = current_strategy_status_upper == "RECONCILING" or str(eligibility_reason or "").strip().lower() == "reconciling"
            halted_by_risk = risk_state_upper.startswith("HALTED")
            same_underlying_info_only = (
                bool(row.get("same_underlying_ambiguity"))
                and not bool(row.get("same_underlying_entry_hold"))
                and not halted_by_risk
                and not faulted
                and not reconciling
                and str(row.get("position_side") or "FLAT").upper() in {"", "FLAT", "UNKNOWN"}
                and not bool(row.get("open_broker_order_id"))
            )
            informational_degradation_only = same_underlying_info_only
            session_scoped_halt = (
                halted_by_risk and str(row.get("halt_reason") or "") == "lane_realized_loser_limit_per_session"
            )
            eligible_to_trade = bool(
                loaded_in_runtime
                and eligible_now
                and not halted_by_risk
                and not reconciling
                and not faulted
                and not runtime_stale
            )

            if not loaded_in_runtime:
                tradability_status = "LOADED_CONFIG_ONLY"
                tradability_reason = "Lane is configured but not currently loaded into the active runtime."
                next_action = "Start runtime or wait for lane load."
                manual_action_required = bool(not runtime_running)
            elif faulted:
                tradability_status = "FAULTED"
                fault_detail = str(row.get("fault_code") or eligibility_reason or current_strategy_status or "Fault active").strip()
                tradability_reason = f"Faulted: {fault_detail}"
                next_action = "Clear Fault only after verifying the underlying blocker is no longer real."
                manual_action_required = True
            elif reconciling:
                tradability_status = "RECONCILING"
                tradability_reason = "Reconciliation is unresolved; new entries stay frozen until state is clean."
                next_action = "Inspect reconciliation and wait for a clean/safe-repair result."
                manual_action_required = True
            elif halted_by_risk:
                tradability_status = "HALTED_BY_RISK"
                tradability_reason = str(row.get("halt_reason") or "Lane risk halt is active.")
                next_action = (
                    "Wait for next session reset; the halt latch auto-clears at the session boundary."
                    if session_scoped_halt
                    else "Clear Risk Halts, then Resume Entries once no other blocker remains."
                )
                manual_action_required = not session_scoped_halt
            elif informational_degradation_only:
                tradability_status = "INFORMATIONAL_ONLY"
                tradability_reason = "Multiple strategies share the instrument, but no live overlap or ambiguity is active."
                next_action = "No action needed."
                manual_action_required = False
            elif eligible_to_trade:
                tradability_status = "ELIGIBLE_TO_TRADE"
                tradability_reason = "Loaded, flat, session-allowed, and ready to process the next trade opportunity."
                next_action = "No action needed; already tradable."
                manual_action_required = False
            else:
                tradability_status = "LOADED_NOT_ELIGIBLE"
                reason_map = {
                    "wrong_session": "Loaded in runtime, but outside the lane's allowed session.",
                    "warmup_incomplete": "Loaded in runtime, but warmup is still incomplete.",
                    "no_new_completed_bar": "Loaded in runtime, but waiting for the next completed bar.",
                    "entries_disabled": "Loaded in runtime, but entries are currently disabled.",
                    "operator_halt": "Loaded in runtime, but operator halt is active.",
                    "strategy_not_ready": "Loaded in runtime, but strategy state is not yet tradable.",
                    "stale_runtime": "Loaded in runtime, but runtime status is stale.",
                    "stopped_runtime": "Runtime is stopped, so this lane is not tradable.",
                }
                tradability_reason = reason_map.get(str(eligibility_reason or ""), "Loaded in runtime, but not currently eligible to trade.")
                next_action_map = {
                    "wrong_session": "Wait for the lane's allowed session.",
                    "warmup_incomplete": "Wait for warmup to complete.",
                    "no_new_completed_bar": "Wait for the next completed bar.",
                    "entries_disabled": "Resume Entries if trading should be re-enabled.",
                    "operator_halt": "Resume Entries when you want this runtime trading again.",
                    "strategy_not_ready": "Wait for strategy state to return to READY/flat.",
                    "stale_runtime": "Refresh runtime health before treating this lane as tradable.",
                    "stopped_runtime": "Start runtime.",
                }
                next_action = next_action_map.get(str(eligibility_reason or ""), "No manual action needed unless this state is unexpected.")
                manual_action_required = str(eligibility_reason or "") in {"entries_disabled", "operator_halt", "stopped_runtime"}
            lane_eligibility_rows.append(
                {
                    "lane_id": lane_id,
                    "display_name": row.get("display_name") or lane_id,
                    "symbol": row.get("symbol"),
                    "configured_allowed_sessions": row.get("session_restriction") or "ANY",
                    "current_detected_session": row.get("current_detected_session") or current_detected_session,
                    "eligible_now": eligible_now,
                    "loaded_in_runtime": loaded_in_runtime,
                    "eligible_to_trade": eligible_to_trade,
                    "halted_by_risk": halted_by_risk,
                    "reconciling": reconciling,
                    "faulted": faulted,
                    "informational_degradation_only": informational_degradation_only,
                    "tradability_status": tradability_status,
                    "tradability_reason": tradability_reason,
                    "next_action": next_action,
                    "manual_action_required": manual_action_required,
                    "current_strategy_status": current_strategy_status,
                    "eligibility_reason": eligibility_reason,
                    "eligibility_detail": row.get("eligibility_detail"),
                    "risk_state": risk_state,
                    "halt_reason": row.get("halt_reason"),
                    "session_scoped_halt": session_scoped_halt,
                    "auto_clear_on_session_reset": bool(row.get("auto_clear_on_session_reset")) or session_scoped_halt,
                    "session_reset_auto_cleared": bool(row.get("session_reset_auto_cleared")),
                    "session_reset_auto_cleared_at": row.get("session_reset_auto_cleared_at"),
                    "same_underlying_ambiguity": bool(row.get("same_underlying_ambiguity")),
                    "last_processed_bar_end_ts": row.get("last_processed_bar_end_ts"),
                    "latest_completed_bar_end_ts": row.get("latest_completed_bar_end_ts"),
                    "heartbeat_reconciliation": dict(row.get("heartbeat_reconciliation") or {}),
                    "heartbeat_reconciliation_status": _nested_get(row, "heartbeat_reconciliation", "status"),
                    "heartbeat_reconciliation_classification": _nested_get(row, "heartbeat_reconciliation", "classification"),
                    "last_heartbeat_reconcile_at": _nested_get(row, "heartbeat_reconciliation", "last_attempted_at"),
                    "heartbeat_reconciliation_reason": _nested_get(row, "heartbeat_reconciliation", "reason"),
                    "heartbeat_reconciliation_next_action": _nested_get(row, "heartbeat_reconciliation", "recommended_action"),
                    "heartbeat_reconciliation_active_issue": bool(_nested_get(row, "heartbeat_reconciliation", "active_issue", default=False)),
                    "order_timeout_watchdog": dict(row.get("order_timeout_watchdog") or {}),
                    "order_timeout_watchdog_status": _nested_get(row, "order_timeout_watchdog", "status"),
                    "last_order_timeout_check_at": _nested_get(row, "order_timeout_watchdog", "last_checked_at"),
                    "overdue_ack_count": int(_nested_get(row, "order_timeout_watchdog", "overdue_ack_count", default=0) or 0),
                    "overdue_fill_count": int(_nested_get(row, "order_timeout_watchdog", "overdue_fill_count", default=0) or 0),
                    "order_timeout_reason": _nested_get(row, "order_timeout_watchdog", "reason"),
                    "order_timeout_next_action": _nested_get(row, "order_timeout_watchdog", "recommended_action"),
                    "order_timeout_active_issue": bool(_nested_get(row, "order_timeout_watchdog", "active_issue_count", default=0)),
                    "startup_restore_validation": dict(row.get("startup_restore_validation") or {}),
                    "restore_result": _nested_get(row, "startup_restore_validation", "restore_result"),
                    "last_restore_completed_at": _nested_get(row, "startup_restore_validation", "restore_completed_at"),
                    "restore_safe_cleanup_applied": bool(_nested_get(row, "startup_restore_validation", "safe_cleanup_applied", default=False)),
                    "restore_unresolved_issue": bool(_nested_get(row, "startup_restore_validation", "unresolved_restore_issue", default=False)),
                    "restore_recommended_action": _nested_get(row, "startup_restore_validation", "recommended_action"),
                    "duplicate_action_prevention_held": bool(_nested_get(row, "startup_restore_validation", "duplicate_action_prevention_held", default=True)),
                }
            )
            lane_status_rows.append(
                {
                    "lane_id": lane_id,
                    "display_name": row.get("display_name") or lane_id,
                    "symbol": row.get("symbol"),
                    "loaded_in_runtime": loaded_in_runtime,
                    "eligible_to_trade": eligible_to_trade,
                    "halted_by_risk": halted_by_risk,
                    "reconciling": reconciling,
                    "faulted": faulted,
                    "informational_degradation_only": informational_degradation_only,
                    "tradability_status": tradability_status,
                    "tradability_reason": tradability_reason,
                    "next_action": next_action,
                    "manual_action_required": manual_action_required,
                    "risk_state": risk_state,
                    "halt_reason": row.get("halt_reason"),
                    "heartbeat_reconciliation": dict(row.get("heartbeat_reconciliation") or {}),
                    "heartbeat_reconciliation_status": _nested_get(row, "heartbeat_reconciliation", "status"),
                    "heartbeat_reconciliation_classification": _nested_get(row, "heartbeat_reconciliation", "classification"),
                    "last_heartbeat_reconcile_at": _nested_get(row, "heartbeat_reconciliation", "last_attempted_at"),
                    "heartbeat_reconciliation_reason": _nested_get(row, "heartbeat_reconciliation", "reason"),
                    "heartbeat_reconciliation_next_action": _nested_get(row, "heartbeat_reconciliation", "recommended_action"),
                    "heartbeat_reconciliation_active_issue": bool(_nested_get(row, "heartbeat_reconciliation", "active_issue", default=False)),
                    "heartbeat_reconciliation_cadence_seconds": _nested_get(row, "heartbeat_reconciliation", "cadence_seconds"),
                    "order_timeout_watchdog": dict(row.get("order_timeout_watchdog") or {}),
                    "order_timeout_watchdog_status": _nested_get(row, "order_timeout_watchdog", "status"),
                    "last_order_timeout_check_at": _nested_get(row, "order_timeout_watchdog", "last_checked_at"),
                    "overdue_ack_count": int(_nested_get(row, "order_timeout_watchdog", "overdue_ack_count", default=0) or 0),
                    "overdue_fill_count": int(_nested_get(row, "order_timeout_watchdog", "overdue_fill_count", default=0) or 0),
                    "order_timeout_reason": _nested_get(row, "order_timeout_watchdog", "reason"),
                    "order_timeout_next_action": _nested_get(row, "order_timeout_watchdog", "recommended_action"),
                    "order_timeout_active_issue": bool(_nested_get(row, "order_timeout_watchdog", "active_issue_count", default=0)),
                    "restore_result": _nested_get(row, "startup_restore_validation", "restore_result"),
                    "last_restore_completed_at": _nested_get(row, "startup_restore_validation", "restore_completed_at"),
                    "restore_safe_cleanup_applied": bool(_nested_get(row, "startup_restore_validation", "safe_cleanup_applied", default=False)),
                    "restore_unresolved_issue": bool(_nested_get(row, "startup_restore_validation", "unresolved_restore_issue", default=False)),
                    "restore_recommended_action": _nested_get(row, "startup_restore_validation", "recommended_action"),
                    "duplicate_action_prevention_held": bool(_nested_get(row, "startup_restore_validation", "duplicate_action_prevention_held", default=True)),
                }
            )
        latest_fill_timestamp = None
        if paper.get("latest_fills"):
            latest_fill_timestamp = paper["latest_fills"][0].get("fill_timestamp")
        latest_decision_timestamp = _latest_timestamp_from_rows(
            paper.get("events", {}).get("branch_sources", []),
            "logged_at",
            "bar_end_ts",
        )
        if paper.get("running"):
            if operator_state.get("stop_after_cycle_requested"):
                runtime_phase = "STOPPING"
            elif operator_state.get("operator_halt"):
                runtime_phase = "HALTED"
            else:
                runtime_phase = "RUNNING"
        else:
            runtime_phase = "STOPPED"
        exposure_state = (
            "FLAT"
            if position.get("side") == "FLAT"
            else f"{position.get('side', 'UNKNOWN')} {position.get('quantity', 0)} x {position.get('instrument', 'MGC')}"
        )
        lane_scope = [
            f"{row.get('display_name', row.get('symbol', '-'))} / {row.get('session_restriction', '-')}"
            for row in configured_lanes
        ]
        heartbeat_rows = [
            row for row in lane_status_rows if row.get("heartbeat_reconciliation_status") or row.get("last_heartbeat_reconcile_at")
        ]
        heartbeat_rows.sort(key=lambda row: str(row.get("last_heartbeat_reconcile_at") or ""), reverse=True)
        latest_heartbeat = heartbeat_rows[0] if heartbeat_rows else {}
        active_heartbeat_rows = [
            {
                "lane_id": row.get("lane_id"),
                "display_name": row.get("display_name"),
                "symbol": row.get("symbol"),
                "status": row.get("heartbeat_reconciliation_status"),
                "classification": row.get("heartbeat_reconciliation_classification"),
                "reason": row.get("heartbeat_reconciliation_reason"),
                "recommended_action": row.get("heartbeat_reconciliation_next_action"),
                "last_attempted_at": row.get("last_heartbeat_reconcile_at"),
            }
            for row in heartbeat_rows
            if row.get("heartbeat_reconciliation_active_issue")
        ]
        timeout_rows = [
            row for row in lane_status_rows if row.get("order_timeout_watchdog_status") or row.get("last_order_timeout_check_at")
        ]
        timeout_rows.sort(key=lambda row: str(row.get("last_order_timeout_check_at") or ""), reverse=True)
        latest_timeout = timeout_rows[0] if timeout_rows else {}
        active_timeout_rows = [
            {
                "lane_id": row.get("lane_id"),
                "display_name": row.get("display_name"),
                "symbol": row.get("symbol"),
                "status": row.get("order_timeout_watchdog_status"),
                "reason": row.get("order_timeout_reason"),
                "recommended_action": row.get("order_timeout_next_action"),
                "last_checked_at": row.get("last_order_timeout_check_at"),
                "overdue_ack_count": row.get("overdue_ack_count"),
                "overdue_fill_count": row.get("overdue_fill_count"),
            }
            for row in timeout_rows
            if row.get("order_timeout_active_issue")
        ]
        restore_rows = [row for row in lane_status_rows if row.get("restore_result")]
        active_restore_rows = [row for row in restore_rows if row.get("restore_unresolved_issue")]
        latest_restore = max(
            restore_rows,
            key=lambda row: str(row.get("last_restore_completed_at") or ""),
            default=paper.get("restore_validation") or {},
        )
        return {
            "runtime_running": runtime_running,
            "runtime_phase": runtime_phase,
            "entries_enabled": bool(paper.get("status", {}).get("entries_enabled")),
            "operator_halt": bool(paper.get("status", {}).get("operator_halt")),
            "current_detected_session": current_detected_session,
            "approved_models_active": len(active_models),
            "approved_models_total": len(approved_models.get("rows", [])),
            "approved_models_label": ", ".join(row["branch"] for row in active_models) if active_models else "None enabled",
            "instrument_scope": " | ".join(lane_scope) if lane_scope else f"{position.get('instrument', 'MGC')} / approved promoted branches only",
            "latest_paper_fill_timestamp": latest_fill_timestamp,
            "latest_paper_decision_timestamp": latest_decision_timestamp,
            "flat_state": position.get("side") == "FLAT",
            "open_exposure_state": exposure_state,
            "desk_risk_state": desk_risk.get("desk_risk_state") or paper.get("status", {}).get("desk_risk_state") or "OK",
            "desk_risk_reason": desk_risk.get("trigger_reason") or paper.get("status", {}).get("desk_risk_reason"),
            "desk_unblock_action": desk_risk.get("unblock_action_required") or paper.get("status", {}).get("desk_unblock_action"),
            "session_realized_pnl": desk_risk.get("session_realized_pnl"),
            "session_unrealized_pnl": desk_risk.get("session_unrealized_pnl"),
            "session_total_pnl": desk_risk.get("session_total_pnl"),
            "desk_halt_new_entries_loss": desk_risk.get("desk_halt_new_entries_loss"),
            "desk_flatten_and_halt_loss": desk_risk.get("desk_flatten_and_halt_loss"),
            "lane_risk_rows": lane_rows,
            "lane_eligibility_rows": lane_eligibility_rows,
            "lane_status_rows": lane_status_rows,
            "lane_status_summary": {
                "loaded_in_runtime_count": sum(1 for row in lane_status_rows if row.get("loaded_in_runtime")),
                "eligible_to_trade_count": sum(1 for row in lane_status_rows if row.get("eligible_to_trade")),
                "halted_by_risk_count": sum(1 for row in lane_status_rows if row.get("halted_by_risk")),
                "reconciling_count": sum(1 for row in lane_status_rows if row.get("reconciling")),
                "faulted_count": sum(1 for row in lane_status_rows if row.get("faulted")),
                "informational_only_count": sum(1 for row in lane_status_rows if row.get("informational_degradation_only")),
            },
            "heartbeat_reconciliation_summary": {
                "last_attempted_at": latest_heartbeat.get("last_heartbeat_reconcile_at"),
                "last_status": latest_heartbeat.get("heartbeat_reconciliation_status") or "UNAVAILABLE",
                "last_classification": latest_heartbeat.get("heartbeat_reconciliation_classification"),
                "cadence_seconds": latest_heartbeat.get("heartbeat_reconciliation_cadence_seconds"),
                "active_issue_count": len(active_heartbeat_rows),
                "active_issue_rows": active_heartbeat_rows,
                "reason": latest_heartbeat.get("heartbeat_reconciliation_reason"),
                "recommended_action": latest_heartbeat.get("heartbeat_reconciliation_next_action"),
            },
            "order_timeout_watchdog_summary": {
                "last_checked_at": latest_timeout.get("last_order_timeout_check_at"),
                "last_status": latest_timeout.get("order_timeout_watchdog_status") or "UNAVAILABLE",
                "overdue_ack_count": sum(int(row.get("overdue_ack_count") or 0) for row in timeout_rows),
                "overdue_fill_count": sum(int(row.get("overdue_fill_count") or 0) for row in timeout_rows),
                "active_issue_count": len(active_timeout_rows),
                "active_issue_rows": active_timeout_rows,
                "reason": latest_timeout.get("order_timeout_reason"),
                "recommended_action": latest_timeout.get("order_timeout_next_action"),
            },
            "restore_validation_summary": {
                "last_restore_completed_at": latest_restore.get("last_restore_completed_at") or latest_restore.get("restore_completed_at"),
                "last_restore_result": latest_restore.get("restore_result") or latest_restore.get("last_restore_result") or "UNAVAILABLE",
                "safe_cleanup_count": sum(1 for row in restore_rows if row.get("restore_safe_cleanup_applied")),
                "unresolved_issue_count": len(active_restore_rows),
                "active_issue_rows": active_restore_rows,
                "recommended_action": latest_restore.get("restore_recommended_action") or latest_restore.get("recommended_action"),
                "duplicate_action_prevention_held": (
                    all(bool(row.get("duplicate_action_prevention_held", False)) for row in restore_rows)
                    if restore_rows
                    else bool(latest_restore.get("duplicate_action_prevention_held", True))
                ),
            },
            "last_control_action": paper.get("operator_state", {}).get("last_control_action"),
            "last_control_status": paper.get("operator_state", {}).get("last_control_status"),
            "last_control_timestamp": paper.get("operator_state", {}).get("last_control_timestamp"),
            "halt_reason": paper.get("operator_state", {}).get("halt_reason"),
            "artifacts": {
                "status": "/api/operator-artifact/paper-operator-status",
                "approved_models": "/api/operator-artifact/paper-approved-models",
                "desk_risk": "/api/operator-artifact/paper-desk-risk-status",
                "lane_risk": "/api/operator-artifact/paper-lane-risk-status",
                "risk_events": "/api/operator-artifact/paper-risk-events",
                "config_in_force": "/api/operator-artifact/paper-config-in-force",
                "decisions": "/api/operator-artifact/paper-branch-sources",
                "intents": "/api/operator-artifact/paper-latest-intents",
                "fills": "/api/operator-artifact/paper-latest-fills",
                "blotter": "/api/operator-artifact/paper-latest-blotter",
                "position": "/api/operator-artifact/paper-position-state",
                "blocks": "/api/operator-artifact/paper-rule-blocks",
                "alerts": "/api/operator-artifact/paper-alerts",
                "reconciliation": "/api/operator-artifact/paper-reconciliation",
            },
        }

    def _paper_soak_validation_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No paper soak validation artifact is available yet."}
        payload = _read_json(
            Path(str(artifacts_dir_value)) / "runtime" / "paper_soak_validation" / "paper_soak_validation_latest.json"
        )
        if not payload:
            return {"available": False, "summary_line": "No paper soak validation artifact is available yet."}
        summary = dict(payload.get("summary") or {})
        position_state = dict(summary.get("position_state") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path"),
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "summary": summary,
            "scenario_rows": list(payload.get("scenarios") or []),
            "summary_line": (
                f"{summary.get('result', 'UNKNOWN')} | "
                f"{summary.get('passed_count', 0)}/{summary.get('scenario_count', 0)} scenarios passed | "
                f"phase={summary.get('runtime_phase', 'UNKNOWN')} | "
                f"position={position_state.get('side', 'UNKNOWN')}"
            ),
        }

    def _paper_live_timing_summary_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No live timing summary artifact is available yet."}
        payload = _read_json(Path(str(artifacts_dir_value)) / "live_timing_summary_latest.json")
        if not payload:
            raw_operator_status = dict(paper.get("raw_operator_status") or {})
            payload = dict(raw_operator_status.get("live_timing_summary") or {})
        if not payload:
            return {"available": False, "summary_line": "No live timing summary artifact is available yet."}
        broker_truth = dict(payload.get("broker_truth") or {})
        position_state = dict(payload.get("position_state") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "contract": dict(payload.get("contract") or {}),
            "runtime_phase": payload.get("runtime_phase"),
            "strategy_state": payload.get("strategy_state"),
            "position_state": position_state,
            "evaluated_bar_id": payload.get("evaluated_bar_id"),
            "evaluated_bar_end_ts": payload.get("evaluated_bar_end_ts"),
            "intent_created_at": payload.get("intent_created_at"),
            "submit_attempted_at": payload.get("submit_attempted_at"),
            "broker_ack_at": payload.get("broker_ack_at"),
            "broker_fill_at": payload.get("broker_fill_at"),
            "pending_since": payload.get("pending_since"),
            "pending_reason": payload.get("pending_reason"),
            "pending_stage": payload.get("pending_stage"),
            "reconcile_trigger_source": payload.get("reconcile_trigger_source"),
            "latest_order_intent": dict(payload.get("latest_order_intent") or {}),
            "latest_fill": dict(payload.get("latest_fill") or {}),
            "latest_restore_result": payload.get("latest_restore_result"),
            "entries_disabled_blocker": payload.get("entries_disabled_blocker"),
            "submit_failure": dict(payload.get("submit_failure") or {}),
            "broker_truth": broker_truth,
            "summary_line": (
                f"stage={payload.get('pending_stage', 'UNKNOWN')} | "
                f"bar={payload.get('evaluated_bar_id', 'NONE')} | "
                f"position={position_state.get('side', 'UNKNOWN')} | "
                f"ack={payload.get('broker_ack_at', 'NONE')} | "
                f"fill={payload.get('broker_fill_at', 'NONE')}"
            ),
        }

    def _paper_live_timing_validation_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No live timing validation artifact is available yet."}
        payload = _read_json(
            Path(str(artifacts_dir_value)) / "runtime" / "paper_live_timing_validation" / "paper_live_timing_validation_latest.json"
        )
        if not payload:
            return {"available": False, "summary_line": "No live timing validation artifact is available yet."}
        summary = dict(payload.get("summary") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path"),
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "contract": dict(payload.get("contract") or {}),
            "summary": summary,
            "scenario_rows": list(payload.get("scenarios") or []),
            "representative_summary": dict(payload.get("representative_summary") or {}),
            "summary_line": (
                f"{summary.get('result', 'UNKNOWN')} | "
                f"{summary.get('passed_count', 0)}/{summary.get('scenario_count', 0)} scenarios passed | "
                f"phase={summary.get('final_runtime_phase', 'UNKNOWN')} | "
                f"stage={summary.get('final_pending_stage', 'UNKNOWN')}"
            ),
        }

    def _shadow_live_shadow_summary_payload(self, shadow: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = shadow.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No live shadow summary artifact is available yet."}
        payload = _read_json(Path(str(artifacts_dir_value)) / "live_shadow_summary_latest.json")
        if not payload:
            raw_operator_status = dict(shadow.get("raw_operator_status") or {})
            payload = dict(raw_operator_status.get("live_shadow_summary") or {})
        if not payload:
            return {"available": False, "summary_line": "No live shadow summary artifact is available yet."}
        broker_truth_summary = dict(payload.get("broker_truth_summary") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path") or "mgc-v05l probationary-live-shadow",
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "current_runtime_phase": payload.get("current_runtime_phase"),
            "strategy_state": payload.get("strategy_state"),
            "last_finalized_live_bar_id": payload.get("last_finalized_live_bar_id"),
            "last_finalized_live_bar_end_ts": payload.get("last_finalized_live_bar_end_ts"),
            "session_classification": payload.get("session_classification"),
            "latest_feature_summary": dict(payload.get("latest_feature_summary") or {}),
            "latest_signal_summary": dict(payload.get("latest_signal_summary") or {}),
            "latest_exit_decision": dict(payload.get("latest_exit_decision") or {}),
            "latest_shadow_intent": dict(payload.get("latest_shadow_intent") or {}),
            "submit_would_be_allowed_if_shadow_disabled": payload.get("submit_would_be_allowed_if_shadow_disabled"),
            "entries_disabled_blocker": payload.get("entries_disabled_blocker"),
            "pending_reason": payload.get("pending_reason"),
            "pending_stage": payload.get("pending_stage"),
            "reconcile_trigger_source": payload.get("reconcile_trigger_source"),
            "fault_code": payload.get("fault_code"),
            "broker_truth_summary": broker_truth_summary,
            "summary_line": payload.get("summary_line")
            or (
                f"phase={payload.get('current_runtime_phase', 'UNKNOWN')} | "
                f"submit={'WOULD_SUBMIT' if payload.get('submit_would_be_allowed_if_shadow_disabled') else 'BLOCKED'} | "
                f"blocker={payload.get('entries_disabled_blocker') or 'none'}"
            ),
        }

    def _shadow_live_strategy_pilot_summary_payload(self, shadow: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = shadow.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No live strategy pilot summary artifact is available yet."}
        artifacts_dir = Path(str(artifacts_dir_value))
        payload = _read_json(artifacts_dir / "live_strategy_pilot_summary_latest.json")
        if not payload:
            raw_operator_status = dict(shadow.get("raw_operator_status") or {})
            payload = dict(raw_operator_status.get("live_strategy_pilot_summary") or {})
        if not payload:
            return {"available": False, "summary_line": "No live strategy pilot summary artifact is available yet."}
        signal_observability = dict(payload.get("signal_observability") or {})
        if not signal_observability:
            signal_observability = _read_json(artifacts_dir / "live_strategy_signal_observability_latest.json")
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path") or "mgc-v05l probationary-live-strategy-pilot",
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "live_strategy_pilot_enabled": payload.get("live_strategy_pilot_enabled"),
            "live_strategy_submit_enabled": payload.get("live_strategy_submit_enabled"),
            "live_strategy_single_cycle_mode": payload.get("live_strategy_single_cycle_mode"),
            "pilot_armed": payload.get("pilot_armed"),
            "pilot_rearm_required": payload.get("pilot_rearm_required"),
            "submit_currently_enabled": payload.get("submit_currently_enabled"),
            "cycle_status": payload.get("cycle_status"),
            "remaining_allowed_live_submits": payload.get("remaining_allowed_live_submits"),
            "current_runtime_phase": payload.get("current_runtime_phase"),
            "strategy_state": payload.get("strategy_state"),
            "current_strategy_readiness": payload.get("current_strategy_readiness"),
            "latest_evaluated_bar": dict(payload.get("latest_evaluated_bar") or {}),
            "latest_signal_decision": dict(payload.get("latest_signal_decision") or {}),
            "latest_exit_decision": dict(payload.get("latest_exit_decision") or {}),
            "latest_live_strategy_intent": dict(payload.get("latest_live_strategy_intent") or {}),
            "submit_attempted_at": payload.get("submit_attempted_at"),
            "broker_ack_at": payload.get("broker_ack_at"),
            "broker_fill_at": payload.get("broker_fill_at"),
            "broker_order_id": payload.get("broker_order_id"),
            "pending_stage": payload.get("pending_stage"),
            "pending_reason": payload.get("pending_reason"),
            "reconcile_trigger_source": payload.get("reconcile_trigger_source"),
            "entries_disabled_blocker": payload.get("entries_disabled_blocker"),
            "submit_gate": dict(payload.get("submit_gate") or {}),
            "pilot_cycle": dict(payload.get("pilot_cycle") or {}),
            "broker_truth_summary": dict(payload.get("broker_truth_summary") or {}),
            "position_state": dict(payload.get("position_state") or {}),
            "latest_restore_result": payload.get("latest_restore_result"),
            "latest_fill_sync": dict(payload.get("latest_fill_sync") or {}),
            "signal_selectivity_analysis": self._signal_selectivity_analysis_payload(shadow),
            "signal_observability": signal_observability,
            "fault_code": payload.get("fault_code"),
            "summary_line": payload.get("summary_line")
            or (
                f"pilot={'ENABLED' if payload.get('live_strategy_pilot_enabled') and payload.get('live_strategy_submit_enabled') else 'DISABLED'} | "
                f"phase={payload.get('current_runtime_phase', 'UNKNOWN')} | "
                f"submit={'ELIGIBLE' if payload.get('current_strategy_readiness') else 'BLOCKED'} | "
                f"blocker={payload.get('entries_disabled_blocker') or 'none'}"
            ),
        }

    def _signal_selectivity_analysis_payload(self, shadow: dict[str, Any]) -> dict[str, Any]:
        del shadow
        path = self._repo_root / "outputs" / "probationary_pattern_engine" / "signal_selectivity_analysis" / "signal_selectivity_analysis_latest.json"
        payload = _read_json(path)
        if not payload:
            return {"available": False, "summary_line": "Signal selectivity analysis artifact is not available yet."}
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "dataset_count": payload.get("dataset_count"),
            "summary_line": payload.get("summary_line"),
            "key_findings": list(payload.get("key_findings") or []),
            "live_pilot_focus": dict(payload.get("live_pilot_focus") or {}),
            "before_after_bear_snap_location": dict(payload.get("before_after_bear_snap_location") or {}),
            "bear_snap_up_stretch_ladder": dict(payload.get("bear_snap_up_stretch_ladder") or {}),
            "bear_snap_range_ladder": dict(payload.get("bear_snap_range_ladder") or {}),
            "regime_comparison": dict(payload.get("regime_comparison") or {}),
        }

    def _paper_broker_truth_shadow_validation_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifact_path = self._production_link_service.config.snapshot_path.with_name("broker_truth_schema_validation_latest.json")
        payload = _read_json(artifact_path)
        if not payload:
            return {"available": False, "summary_line": "No broker-truth shadow validation artifact is available yet."}
        summary = dict(payload.get("summary") or {})
        validations = dict(payload.get("validations") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path"),
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "selected_account_hash": payload.get("selected_account_hash"),
            "schemas": dict(payload.get("schemas") or {}),
            "validations": validations,
            "summary": summary,
            "summary_line": (
                summary.get("summary_line")
                or (
                    f"{summary.get('result', 'UNKNOWN')} | "
                    f"classification={summary.get('overall_classification', 'UNKNOWN')}"
                )
            ),
        }

    def _paper_exit_parity_summary_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No exit parity summary artifact is available yet."}
        payload = _read_json(Path(str(artifacts_dir_value)) / "exit_parity_summary_latest.json")
        if not payload:
            raw_operator_status = dict(paper.get("raw_operator_status") or {})
            payload = dict(raw_operator_status.get("exit_parity_summary") or {})
        if not payload:
            return {"available": False, "summary_line": "No exit parity summary artifact is available yet."}
        latest_exit_decision = dict(payload.get("latest_exit_decision") or {})
        stop_refs = dict(payload.get("stop_refs") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "position_side": payload.get("position_side"),
            "current_position_family": payload.get("current_position_family"),
            "latest_exit_decision": latest_exit_decision,
            "stop_refs": stop_refs,
            "break_even": dict(payload.get("break_even") or {}),
            "latest_order_intent": dict(payload.get("latest_order_intent") or {}),
            "latest_fill": dict(payload.get("latest_fill") or {}),
            "latest_restore_result": payload.get("latest_restore_result"),
            "exit_fill_pending": payload.get("exit_fill_pending"),
            "exit_fill_confirmed": payload.get("exit_fill_confirmed"),
            "summary_line": (
                f"family={payload.get('current_position_family', 'NONE')} | "
                f"primary_reason={latest_exit_decision.get('primary_reason', 'NONE')} | "
                f"pending={'YES' if payload.get('exit_fill_pending') else 'NO'} | "
                f"confirmed={'YES' if payload.get('exit_fill_confirmed') else 'NO'}"
            ),
        }

    def _paper_soak_extended_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No extended paper soak artifact is available yet."}
        payload = _read_json(
            Path(str(artifacts_dir_value)) / "runtime" / "paper_soak_extended" / "paper_soak_extended_latest.json"
        )
        if not payload:
            return {"available": False, "summary_line": "No extended paper soak artifact is available yet."}
        summary = dict(payload.get("summary") or {})
        final_position_state = dict(summary.get("final_position_state") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path"),
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "summary": summary,
            "checkpoint_rows": list(payload.get("checkpoint_rows") or []),
            "final_snapshot": dict(payload.get("final_snapshot") or {}),
            "summary_line": (
                f"{summary.get('result', 'UNKNOWN')} | "
                f"bars={summary.get('bars_processed', 0)} | "
                f"restarts={summary.get('restart_count', 0)} | "
                f"drift={'YES' if summary.get('drift_detected') else 'NO'} | "
                f"final_position={final_position_state.get('side', 'UNKNOWN')}"
            ),
        }

    def _paper_soak_unattended_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        if not artifacts_dir_value:
            return {"available": False, "summary_line": "No unattended paper soak artifact is available yet."}
        payload = _read_json(
            Path(str(artifacts_dir_value)) / "runtime" / "paper_soak_unattended" / "paper_soak_unattended_latest.json"
        )
        if not payload:
            return {"available": False, "summary_line": "No unattended paper soak artifact is available yet."}
        summary = dict(payload.get("summary") or {})
        final_position_state = dict(summary.get("final_position_state") or {})
        return {
            "available": True,
            "generated_at": payload.get("generated_at"),
            "operator_path": payload.get("operator_path"),
            "allowed_scope": dict(payload.get("allowed_scope") or {}),
            "summary": summary,
            "checkpoint_rows": list(payload.get("checkpoint_rows") or []),
            "final_snapshot": dict(payload.get("final_snapshot") or {}),
            "summary_line": (
                f"{summary.get('result', 'UNKNOWN')} | "
                f"bars={summary.get('bars_processed', 0)} | "
                f"duration={summary.get('runtime_duration_minutes', 0)}m | "
                f"restarts={summary.get('restart_count', 0)} | "
                f"drift={'YES' if summary.get('drift_detected') else 'NO'} | "
                f"final_position={final_position_state.get('side', 'UNKNOWN')}"
            ),
        }

    def _paper_approved_models_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        latest_branch_events = paper.get("events", {}).get("branch_sources", []) or []
        daily_summary = paper.get("daily_summary") or {}
        latest_fills = paper.get("latest_fills", []) or []
        latest_intents = paper.get("latest_intents", []) or []
        artifacts_dir_value = paper.get("artifacts_dir")
        artifacts_dir = Path(str(artifacts_dir_value)) if artifacts_dir_value else None
        db_path_value = paper.get("db_path")
        db_path = Path(str(db_path_value)) if db_path_value else None
        session_date = paper.get("status", {}).get("session_date") or _session_date_from_status(paper.get("raw_operator_status") or {})
        session_branch_events = (
            _session_jsonl_rows(artifacts_dir / "branch_sources.jsonl", session_date, "logged_at", "bar_end_ts")
            if artifacts_dir is not None
            else []
        )
        session_rule_blocks = (
            _session_jsonl_rows(artifacts_dir / "rule_blocks.jsonl", session_date, "logged_at", "bar_end_ts")
            if artifacts_dir is not None
            else []
        )
        session_operator_controls = (
            _session_jsonl_rows(artifacts_dir / "operator_controls.jsonl", session_date, "requested_at", "applied_at")
            if artifacts_dir is not None
            else []
        )
        session_reconciliation = (
            _session_jsonl_rows(artifacts_dir / "reconciliation_events.jsonl", session_date, "logged_at")
            if artifacts_dir is not None
            else []
        )
        _, full_blotter_rows = self._latest_blotter_dataset(artifacts_dir) if artifacts_dir is not None else (None, [])
        lane_db_paths = _probationary_lane_database_paths(paper.get("raw_operator_status") or {}, db_path)
        session_intents = _session_table_rows_across_paths(lane_db_paths, "order_intents", "created_at", "created_at", session_date)
        session_fills = _session_table_rows_across_paths(lane_db_paths, "fills", "fill_timestamp", "fill_timestamp", session_date)
        approved_long_sources = set((paper.get("raw_operator_status") or {}).get("approved_long_entry_sources", []))
        approved_short_sources = set((paper.get("raw_operator_status") or {}).get("approved_short_entry_sources", []))
        approved_sources = approved_long_sources | approved_short_sources
        operator_lanes = self._paper_lane_universe(paper)
        config_lanes = {
            str(row.get("lane_id")): row
            for row in ((paper.get("config_in_force") or {}).get("lanes") or [])
            if row.get("lane_id")
        }
        source_lane_counts: dict[str, int] = {}
        for lane_row in operator_lanes:
            lane_id = str(lane_row.get("lane_id") or "")
            configured = config_lanes.get(lane_id, {})
            lane_long_sources = list(lane_row.get("approved_long_entry_sources") or configured.get("long_sources") or [])
            lane_short_sources = list(lane_row.get("approved_short_entry_sources") or configured.get("short_sources") or [])
            lane_source = str((lane_long_sources or lane_short_sources or [""])[0])
            if lane_source:
                source_lane_counts[lane_source] = source_lane_counts.get(lane_source, 0) + 1
        lane_risk_rows = {
            str(row.get("lane_id")): row
            for row in ((paper.get("lane_risk") or {}).get("lanes") or [])
            if row.get("lane_id")
        }

        performance_branch_rows = paper.get("performance", {}).get("branch_performance", []) or []
        realized_by_branch = {
            str(row.get("branch")): _decimal_or_none(row.get("realized_pnl"))
            for row in performance_branch_rows
            if row.get("branch")
        }
        blocked_by_source = dict(daily_summary.get("blocked_branch_decisions_by_source", {}) or {})
        if not blocked_by_source:
            blocked_by_source = {
                str(row.get("branch")): int(row.get("blocked", 0) or 0)
                for row in performance_branch_rows
                if row.get("branch")
        }
        position = paper.get("position") or {}
        artifacts = self._paper_model_artifact_links()
        details_by_branch: dict[str, dict[str, Any]] = {}
        out_of_scope_blocked_count = sum(
            1
            for row in session_rule_blocks
            if row.get("source")
            and str(row.get("source")) not in approved_sources
            and "not_allowlisted" in str(row.get("block_reason") or "")
        )

        rows: list[dict[str, Any]] = []
        for lane_row in operator_lanes:
            lane_id = str(lane_row.get("lane_id") or "")
            if not lane_id:
                continue
            configured = config_lanes.get(lane_id, {})
            temporary_paper_strategy = _is_temporary_paper_strategy_row({**dict(configured), **dict(lane_row)})
            lane_display = str(
                lane_row.get("display_name")
                or configured.get("display_name")
                or lane_id
            )
            symbol = str(lane_row.get("symbol") or configured.get("symbol") or "")
            long_sources = list(lane_row.get("approved_long_entry_sources") or configured.get("long_sources") or [])
            short_sources = list(lane_row.get("approved_short_entry_sources") or configured.get("short_sources") or [])
            source = str((long_sources or short_sources or [""])[0])
            if not source:
                continue
            source_is_unique = source_lane_counts.get(source, 0) <= 1
            side = str(PROMOTED_PAPER_MODEL_SIDE_BY_SOURCE.get(source) or ("LONG" if long_sources else "SHORT"))
            enabled = True
            model_signals = [
                row
                for row in session_branch_events
                if str(row.get("source") or "") == source
                and (
                    str(row.get("lane_id") or "") == lane_id
                    or (
                        not row.get("lane_id")
                        and (
                            str(row.get("symbol") or "") == symbol
                            or (source_is_unique and not row.get("symbol"))
                        )
                    )
                )
            ]
            model_blocks = [
                row
                for row in session_rule_blocks
                if str(row.get("source") or "") == source
                and (
                    str(row.get("lane_id") or "") == lane_id
                    or (
                        not row.get("lane_id")
                        and (
                            str(row.get("symbol") or "") == symbol
                            or (source_is_unique and not row.get("symbol"))
                        )
                    )
                )
            ]
            model_intents = [
                row
                for row in session_intents
                if str(row.get("reason_code") or "") == source and str(row.get("symbol") or "") == symbol
            ]
            model_intent_ids = {
                str(row.get("order_intent_id"))
                for row in model_intents
                if row.get("order_intent_id")
            }
            model_fills = [
                row
                for row in session_fills
                if str(row.get("order_intent_id") or "") in model_intent_ids
            ]
            model_blotter = [
                row
                for row in full_blotter_rows
                if str(row.get("setup_family") or "") == source
                and (
                    str(row.get("instrument") or row.get("symbol") or "") == symbol
                    or (
                        source_is_unique
                        and not row.get("instrument")
                        and not row.get("symbol")
                    )
                )
            ]
            lane_open_position = str(lane_row.get("position_side") or "FLAT") != "FLAT"
            lane_position = {
                "side": lane_row.get("position_side") or "FLAT",
                "quantity": lane_row.get("broker_position_qty") or lane_row.get("internal_position_qty"),
                "average_price": lane_row.get("entry_price"),
                "instrument": symbol,
                "unrealized_pnl": lane_risk_rows.get(lane_id, {}).get("session_unrealized_pnl"),
            }
            realized_value = _sum_decimal_field(model_blotter, "net_pnl")
            realized_provenance = "LANE_FILTERED_BLOTTER" if realized_value is not None else None
            if realized_value is None and source_is_unique:
                realized_value = realized_by_branch.get(source)
                if realized_value is not None:
                    realized_provenance = "BRANCH_AGGREGATE_FALLBACK"
            blocked_count = len(model_blocks)
            if blocked_count == 0 and source_is_unique:
                blocked_count = int(blocked_by_source.get(source, 0) or 0)
            detail = self._paper_approved_model_detail_payload(
                branch=lane_display,
                side=side,
                enabled=enabled,
                model_signals=model_signals,
                model_blocks=model_blocks,
                model_intents=model_intents,
                model_fills=model_fills,
                model_blotter=model_blotter,
                model_operator_controls=session_operator_controls,
                reconciliation_events=session_reconciliation,
                open_branch=lane_display if lane_open_position else None,
                position=lane_position,
                paper=paper,
                realized_pnl=realized_value,
                blocked_count=blocked_count,
                artifacts=artifacts,
            )
            detail["lane_id"] = lane_id
            detail["instrument"] = symbol
            detail["session_restriction"] = lane_row.get("session_restriction") or configured.get("session_restriction")
            detail["source_family"] = source
            detail["risk_state"] = lane_risk_rows.get(lane_id, {}).get("risk_state", "OK")
            detail["lane_halt_reason"] = lane_risk_rows.get(lane_id, {}).get("halt_reason")
            detail["lane_unblock_action"] = lane_risk_rows.get(lane_id, {}).get("unblock_action")
            detail["catastrophic_open_loss_threshold"] = lane_risk_rows.get(lane_id, {}).get("catastrophic_open_loss_threshold")
            detail["realized_losing_trades"] = lane_risk_rows.get(lane_id, {}).get("realized_losing_trades", 0)
            detail["realized_pnl_provenance"] = realized_provenance
            detail["unrealized_pnl_provenance"] = (
                "CURRENT_OPEN_POSITION_LINKED_TO_LANE"
                if lane_open_position and lane_position.get("unrealized_pnl") not in {None, "", "N/A"}
                else None
            )
            detail["temporary_paper_strategy"] = temporary_paper_strategy
            detail["paper_strategy_class"] = (
                "temporary_paper_strategy" if temporary_paper_strategy else "approved_or_admitted_paper_strategy"
            )
            details_by_branch[lane_display] = detail
            rows.append(
                {
                    "branch": lane_display,
                    "source_family": source,
                    "lane_id": lane_id,
                    "instrument": symbol,
                    "session_restriction": lane_row.get("session_restriction") or configured.get("session_restriction"),
                    "enabled": enabled,
                    "state": "ENABLED" if enabled else "DISABLED",
                    "side": side,
                    "signal_count": detail.get("signal_count", 0),
                    "signal_only_count": detail.get("signal_only_count", 0),
                    "decision_count": detail.get("decision_count", 0),
                    "last_signal_seen": detail.get("latest_signal_timestamp"),
                    "last_signal_decision": detail.get("latest_signal_decision"),
                    "last_signal_label": detail.get("latest_signal_label"),
                    "intent_count": detail.get("intent_count", 0),
                    "last_intent": detail.get("latest_intent_timestamp"),
                    "last_intent_status": detail.get("latest_intent_status"),
                    "last_intent_label": detail.get("latest_intent_label"),
                    "fill_count": detail.get("fill_count", 0),
                    "last_fill": detail.get("latest_fill_timestamp"),
                    "last_fill_price": detail.get("latest_fill_price"),
                    "last_fill_label": detail.get("latest_fill_label"),
                    "blocked_count": detail.get("blocked_count", blocked_count),
                    "realized_pnl": detail.get("realized_pnl"),
                    "unrealized_pnl": detail.get("unrealized_pnl"),
                    "chain_state": detail.get("chain_state"),
                    "open_position": bool(detail.get("open_position")),
                    "latest_activity_type": detail.get("latest_activity_type", "NO_ACTIVITY"),
                    "latest_activity_timestamp": detail.get("latest_activity_timestamp"),
                    "risk_state": detail.get("risk_state", "OK"),
                    "halt_reason": detail.get("lane_halt_reason"),
                    "temporary_paper_strategy": temporary_paper_strategy,
                    "paper_strategy_class": (
                        "temporary_paper_strategy" if temporary_paper_strategy else "approved_or_admitted_paper_strategy"
                    ),
                }
            )

        enabled_count = sum(1 for row in rows if row["enabled"])
        temporary_count = sum(1 for row in rows if row.get("temporary_paper_strategy"))
        default_branch = next((row["branch"] for row in rows if row.get("open_position")), None)
        if default_branch is None:
            active_rows = [row for row in rows if row.get("latest_activity_timestamp")]
            if active_rows:
                default_branch = max(active_rows, key=lambda row: str(row.get("latest_activity_timestamp") or ""))["branch"]
            elif rows:
                default_branch = rows[0]["branch"]
        return {
            "scope_label": "Shared paper lane operator detail",
            "instrument_scope": (
                f"{enabled_count} shared paper lanes / multi-lane paper mode"
                if not temporary_count
                else f"{enabled_count} shared paper lanes / {temporary_count} temporary paper lanes visible here"
            ),
            "enabled_count": enabled_count,
            "total_count": len(rows),
            "temporary_paper_count": temporary_count,
            "rows": rows,
            "default_branch": default_branch,
            "details_by_branch": details_by_branch,
            "out_of_scope_blocked_count": out_of_scope_blocked_count,
            "out_of_scope_note": (
                (
                    f"{out_of_scope_blocked_count} non-approved branches were blocked out of paper scope in the current session."
                    if out_of_scope_blocked_count
                    else "Non-approved branches remain excluded from paper scope and appear only in blocked/rule artifact trails."
                )
                + (
                    " Temporary paper strategy lanes that are already attached to the paper runtime are surfaced here with explicit experimental labels."
                    if temporary_count
                    else ""
                )
            ),
            "artifacts": artifacts,
            "provenance": {
                "eligibility": "Lane rows come from the active paper runtime lane universe in operator_status/config-in-force, including temporary paper lanes when they are attached to the shared paper runtime.",
                "last_signal": "Derived from persisted paper branch_sources filtered by lane symbol/source and lane_id when present.",
                "last_intent": "Derived from persisted paper order intents filtered by lane instrument plus reason_code.",
                "last_fill": "Derived from persisted paper fills joined to lane-filtered paper order intents by order_intent_id.",
                "blocked": "Derived from lane-filtered paper rule_blocks, with summary fallback only when lane-filtered evidence is absent.",
                "realized": "Derived from lane-filtered paper blotter rows when instrument tagging is present; otherwise falls back to branch aggregate.",
            },
        }

    def _paper_non_approved_lanes_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        artifacts_dir_value = paper.get("artifacts_dir")
        artifacts_dir = Path(str(artifacts_dir_value)) if artifacts_dir_value else None
        session_date = paper.get("status", {}).get("session_date") or _session_date_from_status(paper.get("raw_operator_status") or {})
        latest_events = paper.get("events", {}) or {}
        experimental_canaries = paper.get("experimental_canaries") or load_experimental_canaries_snapshot(self._experimental_canaries_snapshot_path)
        full_blotter_rows = self._latest_blotter_dataset(artifacts_dir)[1] if artifacts_dir is not None else []
        approved_long_sources = set((paper.get("raw_operator_status") or {}).get("approved_long_entry_sources", []))
        approved_short_sources = set((paper.get("raw_operator_status") or {}).get("approved_short_entry_sources", []))
        approved_sources = approved_long_sources | approved_short_sources
        operator_lanes = self._paper_lane_universe(paper)
        config_lanes = {
            str(row.get("lane_id")): row
            for row in ((paper.get("config_in_force") or {}).get("lanes") or [])
            if row.get("lane_id")
        }
        lane_risk_rows = {
            str(row.get("lane_id")): row
            for row in ((paper.get("lane_risk") or {}).get("lanes") or [])
            if row.get("lane_id")
        }
        rows: list[dict[str, Any]] = []
        for lane_row in operator_lanes:
            lane_id = str(lane_row.get("lane_id") or "")
            if not lane_id:
                continue
            configured = config_lanes.get(lane_id, {})
            lane_display = str(lane_row.get("display_name") or configured.get("display_name") or lane_id)
            symbol = str(lane_row.get("symbol") or configured.get("symbol") or "")
            long_sources = list(lane_row.get("approved_long_entry_sources") or configured.get("long_sources") or [])
            short_sources = list(lane_row.get("approved_short_entry_sources") or configured.get("short_sources") or [])
            source = str((long_sources or short_sources or [""])[0])
            lane_mode = str(lane_row.get("lane_mode") or configured.get("lane_mode") or "")
            experimental_status = str(
                lane_row.get("experimental_status")
                or configured.get("experimental_status")
                or ""
            )
            quality_bucket_policy = str(
                lane_row.get("quality_bucket_policy")
                or configured.get("quality_bucket_policy")
                or ""
            )
            observer_side = str(
                lane_row.get("observer_side")
                or configured.get("observer_side")
                or lane_row.get("side")
                or ""
            )
            observer_variant_id = str(
                lane_row.get("observer_variant_id")
                or configured.get("observer_variant_id")
                or ""
            )
            runtime_kind = str(lane_row.get("runtime_kind") or configured.get("runtime_kind") or "")
            is_canary = (
                lane_mode == PAPER_EXECUTION_CANARY_LANE_MODE
                or "canary" in lane_id.lower()
                or "canary" in lane_display.lower()
            )
            temporary_paper_strategy = bool(
                experimental_status in {"experimental_canary", "experimental_temp_paper"}
                or runtime_kind in {
                    ATPE_CANARY_RUNTIME_KIND,
                    ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
                    GC_MGC_ACCEPTANCE_RUNTIME_KIND,
                }
            )
            if source and source in approved_sources and not is_canary:
                continue

            lane_artifacts_dir = _probationary_lane_artifacts_dir(lane_row, artifacts_dir, lane_id)
            lane_signals = _session_jsonl_rows(lane_artifacts_dir / "branch_sources.jsonl", session_date, "logged_at", "bar_end_ts")
            lane_reconciliation = _session_jsonl_rows(lane_artifacts_dir / "reconciliation_events.jsonl", session_date, "logged_at")
            lane_status = _read_json(lane_artifacts_dir / "operator_status.json")
            lane_db_path = _resolve_sqlite_database_path(lane_row.get("database_url")) or _resolve_sqlite_database_path(configured.get("database_url"))
            lane_intents = _session_table_rows(lane_db_path, "order_intents", "created_at", "created_at", session_date)
            lane_fills = _session_table_rows(lane_db_path, "fills", "fill_timestamp", "fill_timestamp", session_date)
            lane_signal_table_rows = (
                _session_table_rows(lane_db_path, "signals", "created_at", "created_at", session_date)
                if temporary_paper_strategy
                else []
            )
            lane_signal_json_rows = (
                _session_jsonl_rows(
                    lane_artifacts_dir / "signals.jsonl",
                    session_date,
                    "signal_timestamp",
                    "created_at",
                    "timestamp",
                )
                if temporary_paper_strategy
                else []
            )
            lane_intent_json_rows = (
                _session_jsonl_rows(lane_artifacts_dir / "order_intents.jsonl", session_date, "created_at")
                if temporary_paper_strategy
                else []
            )
            lane_fill_json_rows = (
                _session_jsonl_rows(lane_artifacts_dir / "fills.jsonl", session_date, "fill_timestamp")
                if temporary_paper_strategy
                else []
            )
            lane_trade_json_rows = (
                _session_jsonl_rows(
                    lane_artifacts_dir / "trades.jsonl",
                    session_date,
                    "exit_timestamp",
                    "entry_timestamp",
                )
                if temporary_paper_strategy
                else []
            )
            lane_blotter = [
                row
                for row in full_blotter_rows
                if str(row.get("instrument") or row.get("symbol") or "") == symbol
                and (
                    is_canary
                    or str(row.get("setup_family") or "") == source
                    or (not source and str(row.get("setup_family") or "").startswith("paperExecutionCanary"))
                )
            ]

            latest_signal = _latest_row(lane_signals, "logged_at", "bar_end_ts") or {}
            latest_signal_timestamp = _row_timestamp(latest_signal, "logged_at", "bar_end_ts")
            latest_signal_table = _latest_row(lane_signal_table_rows, "created_at") or {}
            latest_signal_table_timestamp = _row_timestamp(latest_signal_table, "created_at")
            latest_signal_json = _latest_row(lane_signal_json_rows, "signal_timestamp", "created_at", "timestamp") or {}
            latest_signal_json_timestamp = _row_timestamp(latest_signal_json, "signal_timestamp", "created_at", "timestamp")
            effective_signal_timestamp = max(
                [
                    timestamp
                    for timestamp in (
                        latest_signal_timestamp,
                        latest_signal_table_timestamp,
                        latest_signal_json_timestamp,
                    )
                    if timestamp
                ],
                default=None,
            )
            effective_signal_count = max(
                len(lane_signals),
                len(lane_signal_table_rows),
                len(lane_signal_json_rows),
                int(lane_row.get("recent_signal_count") or lane_status.get("signal_count") or 0),
            )
            if temporary_paper_strategy and lane_intent_json_rows:
                lane_intents = lane_intent_json_rows
            if temporary_paper_strategy and lane_fill_json_rows:
                lane_fills = lane_fill_json_rows
            latest_intent = _latest_row(lane_intents, "created_at") or {}
            latest_fill = _latest_row(lane_fills, "fill_timestamp") or {}
            latest_trade = _latest_row(lane_trade_json_rows, "exit_timestamp", "entry_timestamp") or {}
            latest_blotter = _latest_row(lane_blotter, "exit_ts", "entry_ts") or {}
            latest_activity_timestamp = max(
                [
                    timestamp
                    for timestamp in (
                        effective_signal_timestamp,
                        _row_timestamp(latest_intent, "created_at"),
                        _row_timestamp(latest_fill, "fill_timestamp"),
                        _row_timestamp(latest_trade, "exit_timestamp", "entry_timestamp"),
                        _row_timestamp(latest_blotter, "exit_ts", "entry_ts"),
                        _row_timestamp(lane_status, "updated_at", "last_processed_bar_end_ts"),
                    )
                    if timestamp
                ],
                default=None,
            )

            entry_intents = [
                row
                for row in lane_intents
                if str(row.get("intent_type") or "").upper() in {"BUY_TO_OPEN", "SELL_TO_OPEN"}
            ]
            exit_intents = [
                row
                for row in lane_intents
                if str(row.get("intent_type") or "").upper() in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}
            ]
            entry_fill_ids = {
                str(row.get("order_intent_id"))
                for row in entry_intents
                if row.get("order_intent_id")
            }
            exit_fill_ids = {
                str(row.get("order_intent_id"))
                for row in exit_intents
                if row.get("order_intent_id")
            }
            entry_fills = [
                row
                for row in lane_fills
                if str(row.get("order_intent_id") or "") in entry_fill_ids
                or str(row.get("intent_type") or "").upper() in {"BUY_TO_OPEN", "SELL_TO_OPEN"}
            ]
            exit_fills = [
                row
                for row in lane_fills
                if str(row.get("order_intent_id") or "") in exit_fill_ids
                or str(row.get("intent_type") or "").upper() in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}
            ]

            live_trade_count = max(
                len(lane_trade_json_rows),
                int(lane_row.get("closed_trades") or lane_status.get("closed_trades") or 0),
            )
            live_intent_count = max(
                len(lane_intents),
                int(lane_row.get("intent_count") or lane_status.get("intent_count") or 0),
            )
            live_fill_count = max(
                len(lane_fills),
                int(lane_row.get("fill_count") or lane_status.get("fill_count") or 0),
            )
            open_position = str(lane_row.get("position_side") or lane_status.get("position_side") or "FLAT") != "FLAT"
            fired = bool(
                effective_signal_count
                or entry_intents
                or any(
                    str(row.get("source") or "") == PAPER_EXECUTION_CANARY_SIGNAL_SOURCE
                    and str(row.get("lane_id") or "") == lane_id
                    for row in latest_events.get("branch_sources", [])
                )
            )
            if temporary_paper_strategy:
                entry_completed = bool(entry_fills or live_intent_count or live_fill_count or live_trade_count)
                exit_completed = bool(exit_fills or live_trade_count or latest_trade.get("exit_timestamp") or latest_blotter.get("exit_ts"))
            else:
                entry_completed = bool(entry_fills)
                exit_completed = bool(exit_fills or latest_blotter.get("exit_ts"))
            if exit_completed:
                lifecycle_state = "ENTRY_AND_EXIT_COMPLETE"
            elif entry_completed and open_position:
                lifecycle_state = "OPEN_AFTER_ENTRY"
            elif entry_completed:
                lifecycle_state = "ENTRY_COMPLETE_EXIT_PENDING"
            elif entry_intents:
                lifecycle_state = "ENTRY_INTENT_CREATED"
            elif fired:
                lifecycle_state = "FIRED_NO_INTENT"
            else:
                lifecycle_state = "IDLE"

            rows.append(
                {
                    "lane_id": lane_id,
                    "standalone_strategy_id": lane_id,
                    "branch": lane_display,
                    "display_name": lane_display,
                    "instrument": symbol,
                    "session_restriction": lane_row.get("session_restriction") or configured.get("session_restriction"),
                    "lane_mode": lane_mode or ("PAPER_ONLY_NON_APPROVED" if not source else source),
                    "source_family": source or None,
                    "strategy_family": str(lane_row.get("strategy_family") or configured.get("strategy_family") or source or ""),
                    "non_approved": True,
                    "paper_only": True,
                    "is_canary": is_canary,
                    "temporary_paper_strategy": temporary_paper_strategy,
                    "paper_strategy_class": (
                        "temporary_paper_strategy" if temporary_paper_strategy else "paper_only_non_approved"
                    ),
                    "metrics_bucket": (
                        "experimental_temporary_paper" if temporary_paper_strategy else "paper_only_non_approved"
                    ),
                    "experimental_status": experimental_status or None,
                    "quality_bucket_policy": quality_bucket_policy or None,
                    "side": observer_side or None,
                    "observer_variant_id": observer_variant_id or None,
                    "scope_label": "PAPER ONLY / NON-APPROVED",
                    "state": "ENABLED" if bool(lane_row.get("entries_enabled", True)) else "DISABLED",
                    "entries_enabled": bool(lane_row.get("entries_enabled", True)),
                    "operator_halt": bool(lane_row.get("operator_halt", False)),
                    "warmup_complete": lane_row.get("warmup_complete"),
                    "risk_state": lane_risk_rows.get(lane_id, {}).get("risk_state") or lane_row.get("risk_state") or "OK",
                    "position_side": lane_row.get("position_side") or lane_status.get("position_side") or "FLAT",
                    "open_position": open_position,
                    "fired": fired,
                    "fired_at": effective_signal_timestamp or _row_timestamp(_latest_row(entry_intents, "created_at") or {}, "created_at"),
                    "signal_count": effective_signal_count,
                    "processed_bars": int(lane_status.get("processed_bars") or 0),
                    "intent_count": live_intent_count if temporary_paper_strategy else len(lane_intents),
                    "fill_count": live_fill_count if temporary_paper_strategy else len(lane_fills),
                    "trade_count": live_trade_count,
                    "entry_completed": entry_completed,
                    "exit_completed": exit_completed,
                    "entry_state": "COMPLETE" if entry_completed else ("INTENT_CREATED" if entry_intents else "NOT_STARTED"),
                    "exit_state": "COMPLETE" if exit_completed else ("INTENT_CREATED" if exit_intents else "NOT_STARTED"),
                    "latest_signal_label": _format_signal_label(
                        {
                            "timestamp": effective_signal_timestamp,
                            "decision": latest_signal.get("decision"),
                            "block_reason": latest_signal.get("block_reason"),
                        }
                    ),
                    "latest_intent_label": _format_intent_label(
                        {
                            "timestamp": _row_timestamp(latest_intent, "created_at"),
                            "intent_type": latest_intent.get("intent_type"),
                            "order_status": latest_intent.get("order_status"),
                        }
                    ),
                    "latest_fill_label": _format_fill_label(
                        {
                            "timestamp": _row_timestamp(latest_fill, "fill_timestamp"),
                            "fill_price": latest_fill.get("fill_price"),
                            "intent_type": latest_fill.get("intent_type"),
                        }
                    ),
                    "realized_pnl": (
                        lane_row.get("session_realized_pnl")
                        or lane_status.get("session_realized_pnl")
                        or latest_trade.get("realized_pnl")
                        or latest_blotter.get("net_pnl")
                    ),
                    "exit_reason": latest_trade.get("exit_reason") or latest_blotter.get("exit_reason"),
                    "lifecycle_state": lifecycle_state,
                    "latest_activity_timestamp": latest_activity_timestamp,
                    "reconciliation_clean": _latest_row(lane_reconciliation, "logged_at", "timestamp", "updated_at", "fill_timestamp", "bar_end_ts").get("clean")
                    if lane_reconciliation
                    else None,
                    "note": (
                        "Experimental paper strategy. Paper only, non-approved, lower priority than approved/live strategies."
                        if temporary_paper_strategy
                        else (
                            "Paper-only canary/test lane. Not part of the approved-model allowlist."
                            if is_canary
                            else "Paper-only lane outside the approved-model allowlist."
                        )
                    ),
                    "runtime_instance_present": True,
                    "runtime_state_loaded": bool(
                        lane_row.get("last_processed_bar_end_ts")
                        or lane_status.get("updated_at")
                        or lane_status.get("last_processed_bar_end_ts")
                    ),
                    "can_process_bars": bool(
                        lane_row.get("last_processed_bar_end_ts")
                        or lane_status.get("updated_at")
                        or lane_status.get("last_processed_bar_end_ts")
                    ),
                    "config_source": "paper.config_in_force",
                    "snapshot_only": False,
                    "database_url": lane_row.get("database_url") or configured.get("database_url"),
                    "trade_size": lane_row.get("trade_size") or configured.get("trade_size"),
                    "operator_status_payload": lane_status,
                    "artifacts": {
                        "lane_dir": str(lane_artifacts_dir),
                        "processed_bars": str((lane_artifacts_dir / "processed_bars.jsonl").resolve()),
                        "features": str((lane_artifacts_dir / "features.jsonl").resolve()),
                        "signals": str((lane_artifacts_dir / "signals.jsonl").resolve()),
                        "trades": str((lane_artifacts_dir / "trades.jsonl").resolve()),
                        "events": str((lane_artifacts_dir / "events.jsonl").resolve()),
                        "order_intents": str((lane_artifacts_dir / "order_intents.jsonl").resolve()),
                        "fills": str((lane_artifacts_dir / "fills.jsonl").resolve()),
                        "operator_status": str((lane_artifacts_dir / "operator_status.json").resolve()),
                        "reconciliation": str((lane_artifacts_dir / "reconciliation_events.jsonl").resolve()),
                    },
                }
            )

        rows.extend(self._experimental_canary_detail_rows(experimental_canaries))
        deduped_rows: dict[str, dict[str, Any]] = {}
        for row in rows:
            lane_id = str(row.get("lane_id") or "").strip()
            if not lane_id:
                continue
            existing = deduped_rows.get(lane_id)
            if existing is None:
                deduped_rows[lane_id] = row
                continue
            existing_snapshot_only = bool(existing.get("snapshot_only"))
            incoming_snapshot_only = bool(row.get("snapshot_only"))
            if existing_snapshot_only and not incoming_snapshot_only:
                deduped_rows[lane_id] = {**existing, **row}
                continue
            if incoming_snapshot_only and not existing_snapshot_only:
                merged = {**row, **existing}
                merged["artifacts"] = dict(row.get("artifacts") or existing.get("artifacts") or {})
                deduped_rows[lane_id] = merged
                continue
            deduped_rows[lane_id] = {**existing, **row}
        rows = list(deduped_rows.values())
        rows.sort(
            key=lambda row: (
                0 if _is_temporary_paper_strategy_row(row) else (1 if row.get("is_canary") else 2),
                str(row.get("instrument") or ""),
                str(row.get("branch") or ""),
            )
        )
        fired_count = sum(1 for row in rows if row.get("fired"))
        completed_count = sum(1 for row in rows if row.get("entry_completed") and row.get("exit_completed"))
        enabled_count = sum(1 for row in rows if str(row.get("state") or "").upper() == "ENABLED")
        disabled_count = sum(1 for row in rows if str(row.get("state") or "").upper() != "ENABLED")
        recent_signal_count = sum(int(row.get("recent_signal_count") or row.get("signal_count") or 0) for row in rows)
        recent_event_count = sum(int(row.get("recent_event_count") or row.get("event_count") or 0) for row in rows)
        experimental_count = sum(1 for row in rows if _is_temporary_paper_strategy_row(row))
        kill_switch = experimental_canaries.get("kill_switch") or {}
        kill_switch_active = bool(kill_switch.get("active"))
        operator_state_label = (
            "DISABLED BY KILL SWITCH"
            if kill_switch_active and experimental_count
            else ("ENABLED (PAPER ONLY)" if enabled_count and experimental_count else ("DISABLED" if experimental_count else "NO EXPERIMENTAL CANARY"))
        )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope_label": "Paper-only non-approved lanes",
            "instrument_scope": f"{len(rows)} paper-only non-approved lanes",
            "total_count": len(rows),
            "canary_count": sum(1 for row in rows if row.get("is_canary")),
            "temporary_paper_count": sum(1 for row in rows if _is_temporary_paper_strategy_row(row)),
            "experimental_count": experimental_count,
            "enabled_count": enabled_count,
            "disabled_count": disabled_count,
            "fired_count": fired_count,
            "completed_count": completed_count,
            "recent_signal_count": recent_signal_count,
            "recent_event_count": recent_event_count,
            "kill_switch_active": kill_switch_active,
            "kill_switch_path": kill_switch.get("path"),
            "operator_state_label": operator_state_label,
            "operator_summary_line": experimental_canaries.get("operator_summary_line"),
            "rows": rows,
            "note": (
                "These lanes are visible for operator truth only. They are paper-only, non-approved, and do not change admitted model scope."
                + (
                    f" Includes {experimental_count} experimental canary lane{'s' if experimental_count != 1 else ''} from Active Trend Participation Engine."
                    if experimental_count
                    else ""
                )
                if rows
                else "No paper-only non-approved lanes are active in the current runtime."
            ),
            "artifacts": {
                "snapshot": "/api/operator-artifact/paper-non-approved-lanes",
                "status": "/api/operator-artifact/paper-operator-status",
                "config_in_force": "/api/operator-artifact/paper-config-in-force",
                "decisions": "/api/operator-artifact/paper-branch-sources",
                "intents": "/api/operator-artifact/paper-latest-intents",
                "fills": "/api/operator-artifact/paper-latest-fills",
                "blotter": "/api/operator-artifact/paper-latest-blotter",
                "reconciliation": "/api/operator-artifact/paper-reconciliation",
                "experimental_snapshot": "/api/operator-artifact/experimental-canaries",
                "experimental_snapshot_markdown": "/api/operator-artifact/experimental-canaries-md",
                "experimental_operator_summary": "/api/operator-artifact/experimental-canaries-operator-summary",
            },
            "provenance": {
                "eligibility": "Rows come from active paper supervisor lanes that are outside the approved-model source filter.",
                "signals": "Signal/fired state is derived from lane-specific branch_sources artifacts and lane DB intents when present.",
                "fills": "Entry/exit completion is derived from lane DB order_intents plus fills, with blotter fallback for closed-trade confirmation.",
                "experimental_canaries": "Experimental canary rows are loaded from the isolated Active Trend Participation Engine canary snapshot under outputs/probationary_quant_canaries.",
            },
        }

    def _experimental_canary_detail_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for row in list(payload.get("rows") or []):
            metrics = row.get("metrics") or {}
            symbols = [str(symbol) for symbol in list(row.get("symbols") or []) if symbol]
            instrument_scope = "/".join(symbols) if symbols else "-"
            recent_signal_count = int(row.get("recent_signal_count") or 0)
            recent_event_count = int(row.get("recent_event_count") or 0)
            total_trades = int(metrics.get("total_trades") or 0)
            enabled = bool(row.get("enabled"))
            kill_switch_active = bool(row.get("kill_switch_active"))
            latest_timestamp = (
                row.get("last_update_timestamp")
                or row.get("generated_at")
                or payload.get("generated_at")
            )
            lifecycle_state = (
                "SUPPRESSED_BY_KILL_SWITCH"
                if kill_switch_active
                else ("ENTRY_AND_EXIT_COMPLETE" if total_trades else ("FIRED_NO_TRADE" if recent_signal_count else "IDLE"))
            )
            risk_state = "KILL_SWITCH_ACTIVE" if kill_switch_active else ("DISABLED" if not enabled else "OK")
            allow_block_summary = row.get("allow_block_override_summary") or {}
            operator_summary = row.get("operator_summary") or {}
            latest_atp_state = row.get("latest_atp_state") or {}
            latest_atp_entry_state = row.get("latest_atp_entry_state") or {}
            latest_atp_timing_state = row.get("latest_atp_timing_state") or {}
            rows.append(
                {
                    "lane_id": str(row.get("lane_id") or ""),
                    "standalone_strategy_id": str(row.get("lane_id") or ""),
                    "branch": str(row.get("lane_name") or row.get("display_name") or row.get("lane_id") or ""),
                    "display_name": str(row.get("lane_name") or row.get("display_name") or row.get("lane_id") or ""),
                    "instrument": instrument_scope,
                    "session_restriction": "ALL",
                    "lane_mode": "EXPERIMENTAL_CANARY",
                    "source_family": str(row.get("variant_id") or row.get("lane_name") or "experimental_canary"),
                    "strategy_family": "active_trend_participation_engine" if str(row.get("lane_id") or "").startswith("atpe_") else str(row.get("variant_id") or ""),
                    "runtime_kind": ATPE_CANARY_RUNTIME_KIND,
                    "config_source": "experimental_canary_snapshot",
                    "non_approved": True,
                    "paper_only": True,
                    "is_canary": True,
                    "temporary_paper_strategy": True,
                    "paper_strategy_class": "temporary_paper_strategy",
                    "metrics_bucket": "experimental_temporary_paper",
                    "experimental_status": str(row.get("experimental_status") or "experimental_canary"),
                    "quality_bucket_policy": str(row.get("quality_bucket_policy") or "-"),
                    "scope_label": "EXPERIMENTAL CANARY / PAPER ONLY",
                    "state": "ENABLED" if enabled and not kill_switch_active else "DISABLED",
                    "entries_enabled": enabled and not kill_switch_active,
                    "operator_halt": kill_switch_active,
                    "warmup_complete": None,
                    "risk_state": risk_state,
                    "position_side": str(row.get("side") or "FLAT"),
                    "side": str(row.get("side") or "FLAT"),
                    "open_position": False,
                    "fired": recent_signal_count > 0,
                    "fired_at": latest_timestamp if recent_signal_count > 0 else None,
                    "signal_count": recent_signal_count,
                    "recent_signal_count": recent_signal_count,
                    "event_count": recent_event_count,
                    "recent_event_count": recent_event_count,
                    "trade_count": total_trades,
                    "intent_count": total_trades,
                    "fill_count": total_trades,
                    "entry_completed": total_trades > 0,
                    "exit_completed": total_trades > 0,
                    "entry_state": "COMPLETE" if total_trades > 0 else ("OBSERVATION_ONLY" if recent_signal_count > 0 else "NOT_STARTED"),
                    "exit_state": "COMPLETE" if total_trades > 0 else "NOT_STARTED",
                    "latest_signal_label": allow_block_summary.get("label"),
                    "latest_intent_label": None,
                    "latest_fill_label": None,
                    "realized_pnl": metrics.get("net_pnl_cash"),
                    "exit_reason": None,
                    "lifecycle_state": lifecycle_state,
                    "latest_activity_timestamp": latest_timestamp,
                    "last_update_timestamp": latest_timestamp,
                    "reconciliation_clean": True,
                    "kill_switch_active": kill_switch_active,
                    "override_reason": allow_block_summary.get("top_override_reason"),
                    "allow_block_override_summary": allow_block_summary,
                    "latest_atp_state": latest_atp_state,
                    "latest_atp_entry_state": latest_atp_entry_state,
                    "latest_atp_timing_state": latest_atp_timing_state,
                    "atp_bias_state": latest_atp_state.get("bias_state"),
                    "atp_pullback_state": latest_atp_state.get("pullback_state"),
                    "atp_pullback_reason": latest_atp_state.get("pullback_reason"),
                    "atp_pullback_depth_score": latest_atp_state.get("pullback_depth_score"),
                    "atp_pullback_violence_score": latest_atp_state.get("pullback_violence_score"),
                    "atp_entry_state": latest_atp_entry_state.get("entry_state"),
                    "atp_primary_blocker": latest_atp_entry_state.get("primary_blocker"),
                    "atp_continuation_trigger_state": latest_atp_entry_state.get("continuation_trigger_state"),
                    "atp_timing_state": latest_atp_timing_state.get("timing_state"),
                    "atp_vwap_price_quality_state": latest_atp_timing_state.get("vwap_price_quality_state"),
                    "atp_timing_blocker": latest_atp_timing_state.get("primary_blocker"),
                    "metrics_net_pnl_cash": metrics.get("net_pnl_cash"),
                    "metrics_max_drawdown": metrics.get("max_drawdown"),
                    "operator_status_line": row.get("operator_status_line"),
                    "artifacts": dict(row.get("artifacts") or {}),
                    "runtime_instance_present": False,
                    "runtime_state_loaded": False,
                    "can_process_bars": False,
                    "snapshot_only": True,
                    "database_url": None,
                    "trade_size": 1,
                    "operator_status_payload": dict(row.get("operator_status") or {}),
                    "note": (
                        f"Experimental Paper Strategy | Paper Only | Non-Approved | {operator_summary.get('what_it_is_not')}"
                        if operator_summary.get("what_it_is_not")
                        else "Experimental Paper Strategy | Paper Only | Non-Approved | Lower priority than higher-priority live strategies."
                    ),
                }
            )
        return rows

    def _paper_temporary_paper_strategies_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = [
            dict(row)
            for row in list(payload.get("rows") or [])
            if _is_temporary_paper_strategy_row(row)
        ]
        rows.sort(
            key=lambda row: (
                str(row.get("instrument") or ""),
                str(row.get("display_name") or row.get("branch") or row.get("lane_id") or ""),
            )
        )
        enabled_count = sum(1 for row in rows if str(row.get("state") or "").upper() == "ENABLED")
        disabled_count = len(rows) - enabled_count
        return {
            "generated_at": payload.get("generated_at"),
            "scope_label": "Experimental temporary paper strategies",
            "total_count": len(rows),
            "enabled_count": enabled_count,
            "disabled_count": disabled_count,
            "recent_signal_count": sum(int(row.get("recent_signal_count") or row.get("signal_count") or 0) for row in rows),
            "recent_event_count": sum(int(row.get("recent_event_count") or row.get("event_count") or 0) for row in rows),
            "kill_switch_active": bool(payload.get("kill_switch_active")),
            "metrics_bucket": "experimental_temporary_paper",
            "operator_state_label": payload.get("operator_state_label") or "NO TEMPORARY PAPER STRATEGIES",
            "rows": rows,
            "note": (
                "ATPE is surfaced here as a first-class temporary paper strategy. It remains experimental, paper only, non-approved, and lower priority than approved/live strategies."
                if rows
                else "No temporary paper strategies are active in the current runtime."
            ),
            "artifacts": {
                "snapshot": "/api/operator-artifact/paper-temporary-paper-strategies",
                "strategy_performance": "/api/operator-artifact/paper-strategy-performance",
                "operator_status": "/api/operator-artifact/paper-operator-status",
                "non_approved_snapshot": "/api/operator-artifact/paper-non-approved-lanes",
            },
            "provenance": {
                "rows": "Filtered from paper-only non-approved lane rows where the runtime class is an experimental temporary paper strategy.",
                "metrics_bucket": "Kept separate from approved/admitted paper strategy metrics so temporary testing does not pollute approved strategy reporting.",
            },
        }

    def _enabled_temporary_paper_rows(self, paper: dict[str, Any]) -> list[dict[str, Any]]:
        payload = paper.get("temporary_paper_strategies") or paper.get("non_approved_lanes") or {}
        rows = [
            dict(row)
            for row in list(payload.get("rows") or [])
            if _is_temporary_paper_strategy_row(row) and str(row.get("state") or "").upper() == "ENABLED"
        ]
        rows.sort(key=lambda row: str(row.get("lane_id") or row.get("display_name") or ""))
        return rows

    def _paper_start_command_with_enabled_temp_paper(
        self,
        snapshot: dict[str, Any],
    ) -> tuple[list[str] | None, dict[str, Any]]:
        paper = dict(snapshot.get("paper") or {})
        enabled_rows = self._enabled_temporary_paper_rows(paper)
        requested_flags: list[str] = []
        overlay_labels: list[str] = []
        config_paths: list[str] = []
        unresolved_lane_ids: list[str] = []
        for row in enabled_rows:
            spec = _temporary_paper_overlay_spec_for_row(self._repo_root, row)
            if spec is None:
                unresolved_lane_ids.append(str(row.get("lane_id") or ""))
                continue
            requested_flags.append(str(spec["flag"]))
            overlay_labels.append(str(spec["label"]))
            config_paths.append(str(spec["config_path"]))
        metadata = {
            "enabled_lane_ids": [str(row.get("lane_id") or "") for row in enabled_rows],
            "requested_flags": sorted(set(requested_flags)),
            "overlay_labels": sorted(set(overlay_labels)),
            "config_paths": sorted(set(config_paths)),
            "unresolved_lane_ids": unresolved_lane_ids,
        }
        if unresolved_lane_ids:
            return None, metadata
        return ["bash", "scripts/run_probationary_paper_soak.sh", *sorted(set(requested_flags)), "--background"], metadata

    def _paper_temporary_paper_runtime_integrity_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        payload = paper.get("temporary_paper_strategies") or {}
        rows = [dict(row) for row in list(payload.get("rows") or []) if _is_temporary_paper_strategy_row(row)]
        enabled_rows = [row for row in rows if str(row.get("state") or "").upper() == "ENABLED"]
        runtime_lookup = _runtime_lookup_by_lane_id(paper.get("runtime_registry") or {})
        detail_rows: list[dict[str, Any]] = []
        missing_lane_ids: list[str] = []
        snapshot_only_count = 0
        loaded_in_runtime_count = 0
        runtime_state_loaded_count = 0
        unresolved_start_lane_ids: list[str] = []
        start_flags: set[str] = set()

        for row in rows:
            lane_id = str(row.get("lane_id") or "")
            runtime_row = runtime_lookup.get(lane_id) or {}
            runtime_instance_present = bool(row.get("runtime_instance_present", runtime_row.get("runtime_instance_present", False)))
            runtime_state_loaded = bool(row.get("runtime_state_loaded", runtime_row.get("runtime_state_loaded", False)))
            snapshot_only = not runtime_instance_present
            if snapshot_only:
                snapshot_only_count += 1
            else:
                loaded_in_runtime_count += 1
            if runtime_state_loaded:
                runtime_state_loaded_count += 1
            if str(row.get("state") or "").upper() == "ENABLED" and not runtime_instance_present:
                missing_lane_ids.append(lane_id)
            spec = _temporary_paper_overlay_spec_for_row(self._repo_root, row)
            if spec is None and str(row.get("state") or "").upper() == "ENABLED":
                unresolved_start_lane_ids.append(lane_id)
            elif spec is not None:
                start_flags.add(str(spec["flag"]))
            truth_label = (
                "LIVE RUNTIME"
                if runtime_instance_present and runtime_state_loaded
                else ("RUNTIME INSTANCE ONLY" if runtime_instance_present else "SNAPSHOT ONLY / NOT LOADED IN RUNTIME")
            )
            detail_rows.append(
                {
                    "lane_id": lane_id,
                    "display_name": row.get("display_name") or row.get("branch") or lane_id,
                    "enabled_in_app": str(row.get("state") or "").upper() == "ENABLED",
                    "runtime_instance_present": runtime_instance_present,
                    "runtime_state_loaded": runtime_state_loaded,
                    "snapshot_only": snapshot_only,
                    "truth_label": truth_label,
                    "start_flag": spec.get("flag") if spec is not None else None,
                    "runtime_kind": row.get("runtime_kind") or runtime_row.get("runtime_kind"),
                    "last_update_timestamp": row.get("last_update_timestamp") or row.get("latest_activity_timestamp"),
                }
            )

        mismatch_status = (
            "MISMATCH"
            if missing_lane_ids or unresolved_start_lane_ids
            else ("INSTANCE ONLY" if enabled_rows and runtime_state_loaded_count < loaded_in_runtime_count else "CLEAR")
        )
        summary_line = (
            f"Enabled in app: {len(enabled_rows)} | loaded in runtime: {loaded_in_runtime_count} | "
            f"snapshot only: {snapshot_only_count} | missing lane ids: {', '.join(missing_lane_ids) if missing_lane_ids else 'none'}"
        )
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "enabled_in_app_count": len(enabled_rows),
            "loaded_in_runtime_count": loaded_in_runtime_count,
            "runtime_state_loaded_count": runtime_state_loaded_count,
            "snapshot_only_count": snapshot_only_count,
            "mismatch_status": mismatch_status,
            "missing_lane_ids": missing_lane_ids,
            "unresolved_start_lane_ids": unresolved_start_lane_ids,
            "start_flags": sorted(start_flags),
            "restart_action": "restart-paper-with-temp-paper",
            "summary_line": summary_line,
            "rows": detail_rows,
            "artifacts": {
                "runtime_config": "/api/operator-artifact/paper-operator-status",
                "temporary_paper_rows": "/api/operator-artifact/paper-temporary-paper-strategies",
            },
            "note": (
                "Temporary paper rows are only considered live when they are present in the running paper runtime. "
                "Snapshot-only rows remain visible for audit, but they are not active lanes."
            ),
        }

    def _temporary_paper_runtime_mismatch(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        payload = (
            ((snapshot.get("paper") or {}).get("temporary_paper_runtime_integrity"))
            or {}
        )
        missing_lane_ids = [str(value) for value in list(payload.get("missing_lane_ids") or []) if value]
        unresolved_lane_ids = [str(value) for value in list(payload.get("unresolved_start_lane_ids") or []) if value]
        mismatch = bool(missing_lane_ids or unresolved_lane_ids)
        return {
            "mismatch": mismatch,
            "missing_lane_ids": missing_lane_ids,
            "unresolved_lane_ids": unresolved_lane_ids,
            "summary": str(payload.get("summary_line") or ""),
        }

    def _paper_lane_universe(self, paper: dict[str, Any]) -> list[dict[str, Any]]:
        config_lanes = {
            str(row.get("lane_id")): dict(row)
            for row in ((paper.get("config_in_force") or {}).get("lanes") or [])
            if row.get("lane_id")
        }
        merged = {lane_id: dict(row) for lane_id, row in config_lanes.items()}
        for row in ((paper.get("raw_operator_status") or {}).get("lanes") or []):
            lane_id = str(row.get("lane_id") or "")
            if not lane_id:
                continue
            merged[lane_id] = {**merged.get(lane_id, {}), **dict(row)}
        return list(merged.values())

    def _paper_lane_activity_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        rows = approved_models.get("rows") or []
        details_by_branch = approved_models.get("details_by_branch") or {}
        artifacts = {
            "snapshot": "/api/operator-artifact/paper-lane-activity",
            "approved_models": "/api/operator-artifact/paper-approved-models",
            "decisions": "/api/operator-artifact/paper-branch-sources",
            "blocks": "/api/operator-artifact/paper-rule-blocks",
            "intents": "/api/operator-artifact/paper-latest-intents",
            "fills": "/api/operator-artifact/paper-latest-fills",
            "blotter": "/api/operator-artifact/paper-latest-blotter",
            "lane_risk": "/api/operator-artifact/paper-lane-risk-status",
            "reconciliation": "/api/operator-artifact/paper-reconciliation",
            "status": "/api/operator-artifact/paper-operator-status",
        }

        lane_rows: list[dict[str, Any]] = []
        active_today: list[str] = []
        idle_only: list[str] = []
        blocked: list[str] = []
        filled: list[str] = []
        open_now: list[str] = []

        for row in rows:
            branch = str(row.get("branch") or "-")
            detail = details_by_branch.get(branch) or {}
            verdict = _paper_lane_activity_verdict(detail)
            if verdict == "NO_ACTIVITY_YET":
                idle_only.append(branch)
            else:
                active_today.append(branch)
            if verdict == "BLOCKED":
                blocked.append(branch)
            if int(detail.get("fill_count", 0) or 0) > 0:
                filled.append(branch)
            if detail.get("open_position"):
                open_now.append(branch)

            used_sources = ["operator_status.json", "paper_lane_risk_status.json"]
            if int(detail.get("signal_count", 0) or 0) > 0 or detail.get("latest_decision_timestamp"):
                used_sources.append("branch_sources.jsonl")
            if int(detail.get("blocked_count", 0) or 0) > 0 or detail.get("latest_blocked_timestamp"):
                used_sources.append("rule_blocks.jsonl")
            if int(detail.get("intent_count", 0) or 0) > 0 or detail.get("latest_intent_timestamp"):
                used_sources.append("order_intents")
            if int(detail.get("fill_count", 0) or 0) > 0 or detail.get("latest_fill_timestamp"):
                used_sources.append("fills")
            if detail.get("latest_blotter_timestamp") or detail.get("realized_pnl") not in {None, "", "N/A"}:
                used_sources.append("latest blotter rows")
            if detail.get("open_position") or detail.get("reconciliation_state") == "DIRTY":
                used_sources.append("reconciliation_events.jsonl")

            lane_rows.append(
                {
                    "branch": branch,
                    "lane_id": row.get("lane_id"),
                    "instrument": row.get("instrument"),
                    "source_family": row.get("source_family"),
                    "session_restriction": row.get("session_restriction"),
                    "verdict": verdict,
                    "latest_event_type": detail.get("latest_activity_type", "NO_ACTIVITY"),
                    "latest_event_timestamp": detail.get("latest_activity_timestamp"),
                    "has_signal_or_decision": bool(int(detail.get("decision_count", 0) or 0) > 0),
                    "blocked": bool(int(detail.get("blocked_count", 0) or 0) > 0 or detail.get("latest_blocked_timestamp")),
                    "intent_open": bool(int(detail.get("intent_count", 0) or 0) > 0 and int(detail.get("fill_count", 0) or 0) == 0),
                    "filled": bool(int(detail.get("fill_count", 0) or 0) > 0),
                    "open_position": bool(detail.get("open_position")),
                    "latest_blocking_reason": detail.get("latest_blocked_reason") or detail.get("lane_halt_reason"),
                    "latest_fill_price": detail.get("latest_fill_price"),
                    "risk_state": detail.get("risk_state", "OK"),
                    "reconciliation_state": detail.get("reconciliation_state"),
                    "used_sources": used_sources,
                    "artifacts": artifacts,
                }
            )

        summary = {
            "any_activity_today": bool(active_today),
            "idle_only": idle_only,
            "blocked": blocked,
            "filled": filled,
            "open_now": open_now,
            "active_today": active_today,
            "idle_only_count": len(idle_only),
            "blocked_count": len(blocked),
            "filled_count": len(filled),
            "open_now_count": len(open_now),
        }
        summary_line = " | ".join(
            [
                f"Active: {', '.join(active_today) if active_today else 'None'}",
                f"Idle: {', '.join(idle_only) if idle_only else 'None'}",
                f"Blocked: {', '.join(blocked) if blocked else 'None'}",
                f"Filled: {', '.join(filled) if filled else 'None'}",
                f"Open: {', '.join(open_now) if open_now else 'None'}",
            ]
        )
        return {
            "summary": summary,
            "summary_line": summary_line,
            "rows": lane_rows,
            "artifacts": artifacts,
            "provenance": (
                "Derived lane-by-lane from persisted paper operator_status lane truth, lane-filtered branch_sources, "
                "rule_blocks, order intents, fills, latest blotter rows, lane risk status, and reconciliation events. "
                "No cross-lane fallback attribution is used."
            ),
        }

    def _paper_activity_proof_payload(self, paper: dict[str, Any]) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        rows = approved_models.get("rows") or []
        status = paper.get("status") or {}
        performance = paper.get("performance") or {}
        session_metrics = performance.get("session_metrics") or {}
        processed_bars = _int_or_none(session_metrics.get("processed_bars"))
        runtime_active = bool(paper.get("running")) and not bool(status.get("stale"))
        latest_event_timestamp = max(
            [str(row.get("latest_activity_timestamp") or "") for row in rows if row.get("latest_activity_timestamp")],
            default=None,
        ) or None
        approved_models_seen = [
            str(row.get("branch"))
            for row in rows
            if any(
                [
                    int(row.get("signal_count", 0) or 0) > 0,
                    int(row.get("blocked_count", 0) or 0) > 0,
                    int(row.get("decision_count", 0) or 0) > 0,
                    int(row.get("intent_count", 0) or 0) > 0,
                    int(row.get("fill_count", 0) or 0) > 0,
                    bool(row.get("latest_activity_timestamp")),
                ]
            )
        ]
        total_signals = sum(int(row.get("signal_only_count", 0) or 0) for row in rows)
        total_blocks = sum(int(row.get("blocked_count", 0) or 0) for row in rows)
        total_decisions = sum(int(row.get("decision_count", 0) or 0) for row in rows)
        total_intents = sum(int(row.get("intent_count", 0) or 0) for row in rows)
        total_fills = sum(int(row.get("fill_count", 0) or 0) for row in rows)

        latest_status_time = _parse_iso_datetime(status.get("last_update_ts"))
        latest_activity_time = _parse_iso_datetime(latest_event_timestamp)
        stale_watch_threshold_seconds = 900
        stale_watch = False
        stale_watch_message = None
        if runtime_active and processed_bars and processed_bars > 0:
            if latest_activity_time is None and processed_bars >= 3:
                stale_watch = True
                stale_watch_message = "No approved-model activity observed in current session despite active runtime and processed bars."
            elif latest_status_time and latest_activity_time:
                inactivity_seconds = max(0.0, (latest_status_time - latest_activity_time).total_seconds())
                if inactivity_seconds >= stale_watch_threshold_seconds:
                    stale_watch = True
                    stale_watch_message = (
                        f"No approved-model activity observed for {int(inactivity_seconds // 60)}m while runtime remains active."
                    )

        if runtime_active and (latest_event_timestamp or approved_models_seen):
            verdict = "PAPER DESK SHOWING LIVE MODEL ACTIVITY"
        elif runtime_active and processed_bars and processed_bars > 0:
            verdict = "PAPER DESK RUNNING BUT NO APPROVED-MODEL ACTIVITY YET"
        elif not paper.get("running") or status.get("freshness") in {"STALE", "STOPPED"}:
            verdict = "PAPER DESK NOT ACTUALLY RUNNING / NOT POLLING"
        else:
            verdict = "INSUFFICIENT EVIDENCE"

        return {
            "session_summary": {
                "polling_runtime_active": runtime_active,
                "bars_processed_count": processed_bars,
                "approved_models_seen_count": len(approved_models_seen),
                "total_signals_count": total_signals,
                "total_blocked_count": total_blocks,
                "total_decisions_count": total_decisions,
                "total_intents_count": total_intents,
                "total_fills_count": total_fills,
                "latest_approved_model_event_timestamp": latest_event_timestamp,
            },
            "per_model_rows": [
                {
                    "branch": row.get("branch"),
                    "armed": bool(row.get("enabled")),
                    "latest_activity_type": row.get("latest_activity_type") or "NO_ACTIVITY",
                    "latest_activity_timestamp": row.get("latest_activity_timestamp"),
                    "signals": int(row.get("signal_only_count", 0) or 0),
                    "blocks": int(row.get("blocked_count", 0) or 0),
                    "decisions": int(row.get("decision_count", 0) or 0),
                    "intents": int(row.get("intent_count", 0) or 0),
                    "fills": int(row.get("fill_count", 0) or 0),
                }
                for row in rows
            ],
            "verdict": verdict,
            "stale_watch": stale_watch,
            "stale_watch_message": stale_watch_message,
            "no_trade_note": "No trade yet is not the same as no activity. Signals, blocks, decisions, intents, and fills are tracked separately from persisted session evidence.",
            "provenance": "Derived from paper runtime running/freshness state, processed bars, approved-model branch_sources, rule_blocks, intents, and fills for the current paper session.",
        }

    def _paper_model_artifact_links(self) -> dict[str, str]:
        return {
            "approved_models": "/api/operator-artifact/paper-approved-models",
            "decisions": "/api/operator-artifact/paper-branch-sources",
            "intents": "/api/operator-artifact/paper-latest-intents",
            "fills": "/api/operator-artifact/paper-latest-fills",
            "blotter": "/api/operator-artifact/paper-latest-blotter",
            "position": "/api/operator-artifact/paper-position-state",
            "blocks": "/api/operator-artifact/paper-rule-blocks",
            "reconciliation": "/api/operator-artifact/paper-reconciliation",
            "status": "/api/operator-artifact/paper-operator-status",
        }

    def _paper_approved_model_detail_payload(
        self,
        *,
        branch: str,
        side: str,
        enabled: bool,
        model_signals: list[dict[str, Any]],
        model_blocks: list[dict[str, Any]],
        model_intents: list[dict[str, Any]],
        model_fills: list[dict[str, Any]],
        model_blotter: list[dict[str, Any]],
        model_operator_controls: list[dict[str, Any]],
        reconciliation_events: list[dict[str, Any]],
        open_branch: str | None,
        position: dict[str, Any],
        paper: dict[str, Any],
        realized_pnl: Decimal | None,
        blocked_count: int,
        artifacts: dict[str, str],
    ) -> dict[str, Any]:
        latest_signal = _latest_row(model_signals, "logged_at", "bar_end_ts") or {}
        latest_block = _latest_row(model_blocks, "logged_at", "bar_end_ts") or {}
        latest_intent = _latest_row(model_intents, "created_at") or {}
        latest_fill = _latest_row(model_fills, "fill_timestamp") or {}
        latest_blotter = _latest_row(model_blotter, "exit_ts", "entry_ts") or {}
        latest_blotter_timestamp = _row_timestamp(latest_blotter, "exit_ts", "entry_ts")
        latest_signal_timestamp = _row_timestamp(latest_signal, "logged_at", "bar_end_ts")
        latest_block_timestamp = _row_timestamp(latest_block, "logged_at", "bar_end_ts")
        latest_block_reason = latest_block.get("block_reason")
        if not latest_block_timestamp and str(latest_signal.get("decision") or "").lower() == "blocked":
            latest_block_timestamp = latest_signal_timestamp
            latest_block_reason = latest_signal.get("block_reason")
        latest_intent_timestamp = _row_timestamp(latest_intent, "created_at")
        latest_fill_timestamp = _row_timestamp(latest_fill, "fill_timestamp")
        open_position = bool(open_branch == branch and position.get("side") != "FLAT")
        unresolved_intents = [
            row
            for row in model_intents
            if str(row.get("order_status") or "").upper() not in {"FILLED", "CANCELLED", "REJECTED", "EXPIRED"}
        ]
        unresolved_intent_count = len(unresolved_intents)
        unrealized_pnl = position.get("unrealized_pnl") if open_position else None
        latest_activity_timestamp = max(
            [
                timestamp
                for timestamp in (
                    latest_signal_timestamp,
                    latest_block_timestamp,
                    latest_intent_timestamp,
                    latest_fill_timestamp,
                    latest_blotter_timestamp,
                )
                if timestamp
            ],
            default=None,
        )
        latest_signal_only_timestamp = max(
            [
                _row_timestamp(row, "logged_at", "bar_end_ts")
                for row in model_signals
                if str(row.get("decision") or "").lower() != "blocked"
            ],
            default=None,
        )
        latest_activity_type = "NO_ACTIVITY"
        for event_type, timestamp in (
            ("FILL", latest_blotter_timestamp),
            ("FILL", latest_fill_timestamp),
            ("INTENT", latest_intent_timestamp),
            ("BLOCK", latest_block_timestamp),
            (
                "SIGNAL"
                if str(latest_signal.get("decision") or "").lower() in {"allowed", "signal", "armed"}
                else "DECISION",
                latest_signal_timestamp,
            ),
            ("SIGNAL", latest_signal_only_timestamp),
        ):
            if timestamp and timestamp == latest_activity_timestamp:
                latest_activity_type = event_type
                break
        chain_state = _paper_model_chain_state(
            latest_signal=latest_signal,
            latest_block_timestamp=latest_block_timestamp,
            latest_intent_timestamp=latest_intent_timestamp,
            latest_fill_timestamp=latest_fill_timestamp,
            open_position=open_position,
        )
        event_trail = self._paper_model_event_trail(
            branch=branch,
            model_signals=model_signals,
            model_blocks=model_blocks,
            model_intents=model_intents,
            model_fills=model_fills,
            model_blotter=model_blotter,
            model_operator_controls=model_operator_controls,
            reconciliation_events=reconciliation_events,
            open_position=open_position,
            position=position,
            paper=paper,
            artifacts=artifacts,
        )
        if open_position:
            persistence_state = (
                "CURRENT OPEN STATE PRESENT"
                if paper.get("status", {}).get("reconciliation_clean")
                else "PERSISTED STATE DIRTY"
            )
        elif latest_fill_timestamp or latest_intent_timestamp or latest_signal_timestamp:
            persistence_state = "NO OPEN STATE TO RESTORE"
        else:
            persistence_state = "NO MODEL STATE YET"
        return {
            "branch": branch,
            "enabled": enabled,
            "state": "ENABLED" if enabled else "DISABLED",
            "side": side,
            "signal_count": len(model_signals),
            "signal_only_count": sum(1 for row in model_signals if str(row.get("decision") or "").lower() != "blocked"),
            "decision_count": len(model_signals),
            "intent_count": len(model_intents),
            "fill_count": len(model_fills),
            "latest_signal_timestamp": latest_signal_timestamp,
            "latest_signal_decision": latest_signal.get("decision"),
            "latest_signal_label": _format_signal_label(
                {
                    "timestamp": latest_signal_timestamp,
                    "decision": latest_signal.get("decision"),
                    "block_reason": latest_signal.get("block_reason"),
                }
            ),
            "latest_blocked_timestamp": latest_block_timestamp,
            "latest_blocked_reason": latest_block_reason,
            "latest_decision_timestamp": latest_signal_timestamp,
            "latest_intent_timestamp": latest_intent_timestamp,
            "latest_intent_status": latest_intent.get("order_status"),
            "latest_intent_label": _format_intent_label(
                {
                    "timestamp": latest_intent_timestamp,
                    "intent_type": latest_intent.get("intent_type"),
                    "order_status": latest_intent.get("order_status"),
                }
            ),
            "latest_fill_timestamp": latest_fill_timestamp,
            "latest_fill_price": latest_fill.get("fill_price"),
            "latest_fill_label": _format_fill_label(
                {
                    "timestamp": latest_fill_timestamp,
                    "fill_price": latest_fill.get("fill_price"),
                    "intent_type": latest_fill.get("intent_type"),
                }
            ),
            "matching_intent_count": len(model_intents),
            "matching_fill_count": len(model_fills),
            "matching_blotter_row_count": len(model_blotter),
            "matching_position_row_count": 1 if open_position and position.get("side") != "FLAT" else 0,
            "chain_state": chain_state,
            "open_position": open_position,
            "open_qty": position.get("quantity") if open_position else None,
            "open_average_price": position.get("average_price") if open_position else None,
            "realized_pnl": _decimal_to_string(realized_pnl),
            "unrealized_pnl": unrealized_pnl if open_position else None,
            "blocked_count": blocked_count,
            "unresolved_intent_count": unresolved_intent_count,
            "latest_activity_type": latest_activity_type,
            "latest_activity_timestamp": latest_activity_timestamp,
            "latest_blotter_timestamp": latest_blotter_timestamp,
            "latest_blotter_exit_reason": latest_blotter.get("exit_reason"),
            "persistence_state": persistence_state,
            "reconciliation_state": paper.get("status", {}).get("reconciliation_semantics"),
            "event_trail": event_trail,
            "artifacts": artifacts,
            "chain_note": (
                "Unrealized/open exposure is shown only when the current paper position can be tied to this model through persisted filled open intents."
                if open_position
                else "Model chain state is derived from persisted paper decisions, rule blocks, intents, fills, and blotter rows only."
            ),
        }

    def _paper_model_event_trail(
        self,
        *,
        branch: str,
        model_signals: list[dict[str, Any]],
        model_blocks: list[dict[str, Any]],
        model_intents: list[dict[str, Any]],
        model_fills: list[dict[str, Any]],
        model_blotter: list[dict[str, Any]],
        model_operator_controls: list[dict[str, Any]],
        reconciliation_events: list[dict[str, Any]],
        open_position: bool,
        position: dict[str, Any],
        paper: dict[str, Any],
        artifacts: dict[str, str],
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for row in model_signals:
            timestamp = _row_timestamp(row, "logged_at", "bar_end_ts")
            decision = str(row.get("decision") or "seen").upper()
            details = [decision]
            if row.get("block_reason"):
                details.append(str(row.get("block_reason")))
            if row.get("bar_end_ts"):
                details.append(f"bar {row.get('bar_end_ts')}")
            events.append(
                {
                    "timestamp": timestamp,
                    "category": "signal",
                    "title": "Branch Decision",
                    "details": " • ".join(details),
                    "artifact_label": "Decisions JSONL",
                    "artifact_href": artifacts["decisions"],
                    "provenance": "Direct branch_sources decision row.",
                }
            )
        for row in model_blocks:
            timestamp = _row_timestamp(row, "logged_at", "bar_end_ts")
            events.append(
                {
                    "timestamp": timestamp,
                    "category": "block",
                    "title": "Rule Block",
                    "details": row.get("block_reason") or "Blocked by probationary runtime rule.",
                    "artifact_label": "Rule Blocks JSONL",
                    "artifact_href": artifacts["blocks"],
                    "provenance": "Direct rule_blocks event row.",
                }
            )
        for row in model_intents:
            timestamp = _row_timestamp(row, "created_at")
            events.append(
                {
                    "timestamp": timestamp,
                    "category": "intent",
                    "title": f"Intent {row.get('intent_type') or 'INTENT'}",
                    "details": " • ".join(
                        [
                            f"status {row.get('order_status') or '-'}",
                            f"intent {row.get('order_intent_id') or '-'}",
                            f"broker {row.get('broker_order_id') or '-'}",
                        ]
                    ),
                    "artifact_label": "Latest Intents JSON",
                    "artifact_href": artifacts["intents"],
                    "provenance": "Persisted paper order intent by reason_code.",
                }
            )
        for row in model_fills:
            timestamp = _row_timestamp(row, "fill_timestamp")
            events.append(
                {
                    "timestamp": timestamp,
                    "category": "fill",
                    "title": f"Fill {row.get('intent_type') or 'FILL'}",
                    "details": " • ".join(
                        [
                            f"price {row.get('fill_price') or '-'}",
                            f"status {row.get('order_status') or '-'}",
                            f"intent {row.get('order_intent_id') or '-'}",
                        ]
                    ),
                    "artifact_label": "Latest Fills JSON",
                    "artifact_href": artifacts["fills"],
                    "provenance": "Persisted paper fill joined through order_intent_id.",
                }
            )
        for row in model_blotter:
            timestamp = _row_timestamp(row, "exit_ts", "entry_ts")
            details = [
                row.get("direction") or "-",
                f"entry {row.get('entry_px') or '-'}",
                f"exit {row.get('exit_px') or '-'}",
                f"pnl {row.get('net_pnl') or '-'}",
            ]
            if row.get("exit_reason"):
                details.append(str(row.get("exit_reason")))
            events.append(
                {
                    "timestamp": timestamp,
                    "category": "trade",
                    "title": "Blotter Trade Row",
                    "details": " • ".join(details),
                    "artifact_label": "Latest Blotter JSON",
                    "artifact_href": artifacts["blotter"],
                    "provenance": "Direct paper blotter row grouped by setup_family.",
                }
            )
        if open_position:
            events.append(
                {
                    "timestamp": paper.get("status", {}).get("last_update_ts") or paper.get("status", {}).get("last_processed_bar_end_ts"),
                    "category": "position",
                    "title": "Current Open Exposure",
                    "details": " • ".join(
                        [
                            f"{position.get('side') or 'OPEN'} {position.get('quantity') or 0} x {position.get('instrument') or 'MGC'}",
                            f"avg {position.get('average_price') or '-'}",
                            f"unrealized {position.get('unrealized_pnl') or 'Unavailable'}",
                        ]
                    ),
                    "artifact_label": "Position JSON",
                    "artifact_href": artifacts["position"],
                    "provenance": "Current open paper position inferred from persisted filled open intent and operator status.",
                }
            )
            for row in model_operator_controls:
                timestamp = _row_timestamp(row, "applied_at", "requested_at")
                events.append(
                    {
                        "timestamp": timestamp,
                        "category": "control",
                        "title": _operator_control_title(row.get("action")),
                        "details": " • ".join(
                            [
                                f"status {row.get('status') or '-'}",
                                row.get("message") or "Global paper operator control while this model owns current open exposure.",
                            ]
                        ),
                        "artifact_label": "Operator Status",
                        "artifact_href": artifacts["status"],
                        "provenance": "Direct operator_controls event while this model owns the current paper position.",
                    }
                )
            latest_reconciliation = _latest_row(reconciliation_events, "logged_at")
            if latest_reconciliation:
                issues = latest_reconciliation.get("issues") or []
                detail = "clean yes" if latest_reconciliation.get("clean") else f"issues {len(issues)}"
                events.append(
                    {
                        "timestamp": _row_timestamp(latest_reconciliation, "logged_at"),
                        "category": "reconciliation",
                        "title": "Current Reconciliation State",
                        "details": detail,
                        "artifact_label": "Reconciliation JSONL",
                        "artifact_href": artifacts["reconciliation"],
                        "provenance": "Direct reconciliation event for the current paper runtime state.",
                    }
                )
        events = [event for event in events if event.get("timestamp")]
        events.sort(key=lambda event: str(event.get("timestamp") or ""), reverse=True)
        return events[:16]

    def _paper_exceptions_payload(self, paper: dict[str, Any], review_payload: dict[str, Any]) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        details_by_branch = approved_models.get("details_by_branch") or {}
        position = paper.get("position") or {}
        status = paper.get("status") or {}
        operator_state = paper.get("operator_state") or {}
        artifacts = {
            "snapshot": "/api/operator-artifact/paper-exceptions",
            "approved_models": "/api/operator-artifact/paper-approved-models",
            "intents": "/api/operator-artifact/paper-latest-intents",
            "fills": "/api/operator-artifact/paper-latest-fills",
            "position": "/api/operator-artifact/paper-position-state",
            "blocks": "/api/operator-artifact/paper-rule-blocks",
            "reconciliation": "/api/operator-artifact/paper-reconciliation",
            "status": "/api/operator-artifact/paper-operator-status",
            "blotter": "/api/operator-artifact/paper-latest-blotter",
            "summary_json": review_payload.get("links", {}).get("json"),
            "summary_blotter": review_payload.get("links", {}).get("blotter"),
        }
        reference_time = _parse_iso_datetime(status.get("last_update_ts")) or datetime.now(timezone.utc)

        def _age_seconds(timestamp: str | None) -> float | None:
            parsed = _parse_iso_datetime(timestamp)
            if parsed is None:
                return None
            return max(0.0, (reference_time - parsed).total_seconds())

        def _add_exception(
            *,
            code: str,
            severity: str,
            details: str,
            recommendation: str,
            timestamp: str | None,
            artifact_href: str | None,
            artifact_label: str,
            model_branch: str | None = None,
        ) -> None:
            exceptions.append(
                {
                    "code": code,
                    "severity": severity,
                    "details": details,
                    "recommendation": recommendation,
                    "timestamp": timestamp or status.get("last_update_ts"),
                    "artifact_href": artifact_href,
                    "artifact_label": artifact_label,
                    "model_branch": model_branch,
                }
            )

        exceptions: list[dict[str, Any]] = []
        linger_seconds = DEFAULT_POLL_INTERVAL_SECONDS * 3
        open_exposure = bool(position.get("side") != "FLAT")
        owning_model = next(
            (branch for branch, detail in details_by_branch.items() if detail.get("open_position")),
            None,
        )
        unresolved_intents = sum(int(detail.get("unresolved_intent_count", 0) or 0) for detail in details_by_branch.values())
        activity_seen = any(
            detail.get("signal_count") or detail.get("intent_count") or detail.get("fill_count") or detail.get("realized_pnl")
            for detail in details_by_branch.values()
        )

        for branch, detail in details_by_branch.items():
            decision_age = _age_seconds(detail.get("latest_decision_timestamp"))
            intent_age = _age_seconds(detail.get("latest_intent_timestamp"))
            if detail.get("chain_state") == "DECISION_WITHOUT_INTENT" and decision_age is not None and decision_age >= linger_seconds:
                _add_exception(
                    code="DECISION_WITHOUT_INTENT",
                    severity="WATCH",
                    details=f"{branch} produced a paper decision {int(decision_age)}s ago without a persisted order intent.",
                    recommendation="Inspect latest intents and approved-model drilldown before leaving the runtime unattended.",
                    timestamp=detail.get("latest_decision_timestamp"),
                    artifact_href=artifacts["intents"],
                    artifact_label="Latest Intents JSON",
                    model_branch=branch,
                )
            if detail.get("chain_state") == "INTENT_WITHOUT_FILL" and intent_age is not None and intent_age >= linger_seconds:
                _add_exception(
                    code="INTENT_WITHOUT_FILL",
                    severity="ACTION",
                    details=f"{branch} has a paper intent lingering {int(intent_age)}s without a persisted fill.",
                    recommendation="Inspect latest fills and reconciliation before deciding whether to flatten_and_halt or restart.",
                    timestamp=detail.get("latest_intent_timestamp"),
                    artifact_href=artifacts["fills"],
                    artifact_label="Latest Fills JSON",
                    model_branch=branch,
                )
            if (
                int(detail.get("signal_count", 0) or 0) >= 2
                and int(detail.get("intent_count", 0) or 0) == 0
                and int(detail.get("fill_count", 0) or 0) == 0
            ):
                _add_exception(
                    code="MODEL_SIGNAL_SEEN_BUT_NEVER_PROGRESSING",
                    severity="WATCH",
                    details=f"{branch} has repeated paper signals but no intents or fills in persisted artifacts.",
                    recommendation="Inspect the approved-model drilldown and decision trail to confirm whether the model is blocked or never progressing.",
                    timestamp=detail.get("latest_signal_timestamp"),
                    artifact_href=artifacts["approved_models"],
                    artifact_label="Approved Models JSON",
                    model_branch=branch,
                )
            if int(detail.get("blocked_count", 0) or 0) >= 3:
                _add_exception(
                    code="REPEATED_BLOCKS",
                    severity="INFO",
                    details=f"{branch} has been blocked {detail.get('blocked_count')} times in the current paper session.",
                    recommendation="Inspect rule blocks if this model is expected to participate today.",
                    timestamp=detail.get("latest_blocked_timestamp") or detail.get("latest_signal_timestamp"),
                    artifact_href=artifacts["blocks"],
                    artifact_label="Rule Blocks JSONL",
                    model_branch=branch,
                )

        if status.get("fault_state") == "FAULTED":
            _add_exception(
                code="FAULT_ACTIVE",
                severity="BLOCKING",
                details="Paper runtime is faulted according to persisted operator status.",
                recommendation="Inspect operator status and reconciliation, then use Acknowledge / Clear Fault only after the underlying issue is understood.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["status"],
                artifact_label="Operator Status",
            )
        if not status.get("reconciliation_clean"):
            _add_exception(
                code="RECONCILIATION_DIRTY",
                severity="BLOCKING",
                details="Persisted paper reconciliation is dirty.",
                recommendation="Inspect reconciliation JSON before leaving the paper desk alone or restarting.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["reconciliation"],
                artifact_label="Reconciliation JSONL",
                model_branch=owning_model,
            )
        if open_exposure and owning_model and not status.get("reconciliation_clean"):
            _add_exception(
                code="FILL_WITHOUT_POSITION_RECONCILIATION",
                severity="ACTION",
                details=f"{owning_model} owns current open paper exposure while reconciliation is not clean.",
                recommendation="Inspect latest fills and reconciliation JSON before deciding whether to flatten_and_halt.",
                timestamp=details_by_branch.get(owning_model, {}).get("latest_fill_timestamp") or status.get("last_update_ts"),
                artifact_href=artifacts["reconciliation"],
                artifact_label="Reconciliation JSONL",
                model_branch=owning_model,
            )
        if open_exposure and not owning_model:
            _add_exception(
                code="POSITION_PRESENT_WITHOUT_CLEAR_OWNING_MODEL",
                severity="BLOCKING",
                details="Paper position is present but the dashboard cannot truthfully tie it to a single approved model from persisted filled open intents.",
                recommendation="Inspect position, latest intents, and fills before leaving the paper desk unattended.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["position"],
                artifact_label="Position JSON",
            )
        if open_exposure and not paper.get("running"):
            _add_exception(
                code="OPEN_EXPOSURE_AFTER_RESTART_REQUIRES_REVIEW",
                severity="ACTION",
                details="Paper runtime is stopped while persisted open exposure remains present.",
                recommendation="Restart and verify the restored state before treating the paper desk as safe or clean.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["position"],
                artifact_label="Position JSON",
                model_branch=owning_model,
            )
        if open_exposure and not status.get("entries_enabled"):
            _add_exception(
                code="OPEN_EXPOSURE_WHILE_ENTRIES_HALTED",
                severity="WATCH",
                details=f"Paper exposure remains open while entries are halted{f' under {owning_model}' if owning_model else ''}.",
                recommendation="Confirm whether the open paper position is expected to remain unattended or use flatten_and_halt if intervention is needed.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["position"],
                artifact_label="Position JSON",
                model_branch=owning_model,
            )
        if open_exposure and operator_state.get("stop_after_cycle_requested"):
            _add_exception(
                code="OPEN_EXPOSURE_WHILE_STOP_AFTER_CYCLE_PENDING",
                severity="WATCH",
                details="Paper runtime is waiting to stop after the current cycle while exposure remains open.",
                recommendation="Watch fills/position until flat, or use flatten_and_halt if the desk should be taken flat immediately.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["position"],
                artifact_label="Position JSON",
                model_branch=owning_model,
            )
        if (
            not paper.get("running")
            and activity_seen
            and (not paper.get("summary_available") or not paper.get("blotter_path"))
        ):
            _add_exception(
                code="SUMMARY_BLOTTER_MISSING",
                severity="ACTION",
                details="Paper session activity exists, but the latest paper summary or blotter artifact is missing.",
                recommendation="Generate the paper summary before treating the session as clean or fully reviewed.",
                timestamp=status.get("last_update_ts"),
                artifact_href=artifacts["summary_json"] or artifacts["summary_blotter"],
                artifact_label="Paper Summary",
            )

        exceptions.sort(key=lambda row: str(row.get("timestamp") or ""), reverse=True)
        severity_counts = {
            severity: sum(1 for row in exceptions if row.get("severity") == severity)
            for severity in ("INFO", "WATCH", "ACTION", "BLOCKING")
        }

        if status.get("fault_state") == "FAULTED":
            session_verdict = "FAULTED"
        elif not status.get("reconciliation_clean"):
            session_verdict = "DIRTY_RECONCILIATION"
        elif open_exposure and not paper.get("running"):
            session_verdict = "NEEDS_OPERATOR_REVIEW"
        elif open_exposure and (operator_state.get("operator_halt") or not status.get("entries_enabled")):
            session_verdict = "HALTED_WITH_OPEN_RISK"
        elif open_exposure:
            session_verdict = "RUNNING_WITH_OPEN_RISK" if paper.get("running") else "NEEDS_OPERATOR_REVIEW"
        elif paper.get("running"):
            session_verdict = "RUNNING_CLEAN" if not exceptions else "NEEDS_OPERATOR_REVIEW"
        elif exceptions:
            session_verdict = "NEEDS_OPERATOR_REVIEW"
        else:
            session_verdict = "CLEAN_IDLE"

        verdict_note_map = {
            "FAULTED": "Paper runtime is not safe to leave alone until the fault is understood and cleared.",
            "DIRTY_RECONCILIATION": "Persisted paper state is dirty; inspect reconciliation before restart or sign-off.",
            "HALTED_WITH_OPEN_RISK": "Entries are halted, but open risk is still present and needs monitoring.",
            "RUNNING_WITH_OPEN_RISK": "Paper runtime is running with attributable open risk; keep watching the owning model.",
            "RUNNING_CLEAN": "Paper runtime is running and current persisted artifacts do not show active exceptions.",
            "NEEDS_OPERATOR_REVIEW": "Paper state is not clean enough to ignore; inspect the active exceptions first.",
            "CLEAN_IDLE": "Paper runtime is stopped or idle with no active exception in persisted artifacts.",
        }

        return {
            "session_verdict": session_verdict,
            "verdict_note": verdict_note_map.get(session_verdict, "Paper state requires operator judgment."),
            "summary": {
                "open_exposure": open_exposure,
                "owning_model": owning_model,
                "open_qty": position.get("quantity") if open_exposure else None,
                "open_average_price": position.get("average_price") if open_exposure else None,
                "unresolved_intents": unresolved_intents,
                "reconciliation_state": status.get("reconciliation_semantics"),
                "entries_state": "HALTED" if operator_state.get("operator_halt") or not status.get("entries_enabled") else "ENABLED",
                "flatten_pending": bool(operator_state.get("flatten_pending")),
                "stop_after_cycle_pending": bool(operator_state.get("stop_after_cycle_requested")),
                "position_owner_note": (
                    f"Current open paper exposure is attributable to {owning_model}."
                    if owning_model
                    else ("Open exposure owner is unavailable from persisted artifacts." if open_exposure else "No open paper exposure.")
                ),
            },
            "exceptions": exceptions,
            "severity_counts": severity_counts,
            "artifacts": artifacts,
        }

    def _paper_entry_eligibility_payload(self, paper: dict[str, Any], pre_session_review: dict[str, Any]) -> dict[str, Any]:
        readiness = paper.get("readiness") or {}
        approved_models = paper.get("approved_models") or {}
        exceptions = paper.get("exceptions") or {}
        status = paper.get("status") or {}
        operator_state = paper.get("operator_state") or {}
        position = paper.get("position") or {}
        desk_risk = paper.get("desk_risk") or {}
        rows = approved_models.get("rows") or []
        enabled_rows = [row for row in rows if row.get("enabled")]
        open_exposure = bool(position.get("side") != "FLAT")
        unresolved_intents = sum(
            int((approved_models.get("details_by_branch") or {}).get(branch, {}).get("unresolved_intent_count", 0) or 0)
            for branch in (approved_models.get("details_by_branch") or {}).keys()
        )
        runtime_phase = str(readiness.get("runtime_phase") or "STOPPED")
        operator_halt = bool(status.get("operator_halt"))
        entries_enabled = bool(status.get("entries_enabled"))
        recon_clean = bool(status.get("reconciliation_clean"))
        faulted = status.get("fault_state") == "FAULTED"
        desk_risk_state = str(desk_risk.get("desk_risk_state") or status.get("desk_risk_state") or "OK")
        review_required = bool(pre_session_review.get("required"))
        review_completed = bool(pre_session_review.get("completed"))
        severe_exceptions = [
            row for row in (exceptions.get("exceptions") or []) if row.get("severity") in {"ACTION", "BLOCKING"}
        ]
        latest_severe = severe_exceptions[0] if severe_exceptions else None
        signal_seen = any(int(row.get("signal_count", 0) or 0) > 0 for row in rows)

        if faulted:
            verdict = "NOT ELIGIBLE: FAULTED"
            clear_action = "Acknowledge / Clear Fault"
            primary_reason = "FAULT_ACTIVE"
            state_note = "System/risk stop. Paper runtime is faulted, so approved models must not fire."
        elif not recon_clean:
            verdict = "NOT ELIGIBLE: RECONCILIATION DIRTY"
            clear_action = "Manual inspection required"
            primary_reason = "RECONCILIATION_DIRTY"
            state_note = "System/risk stop. Persisted reconciliation is dirty."
        elif review_required and not review_completed:
            verdict = "NOT ELIGIBLE: STARTUP / REVIEW GATING"
            clear_action = "Complete Pre-Session Review"
            primary_reason = "STARTUP_REVIEW_GATING"
            state_note = "Guarded startup state. The desk is not yet reviewed for a new paper run."
        elif not enabled_rows:
            verdict = "NOT ELIGIBLE: NO APPROVED MODELS ENABLED"
            clear_action = "Manual inspection required"
            primary_reason = "NO_APPROVED_MODELS_ENABLED"
            state_note = "Configuration state. No approved promoted model is currently enabled for paper entries."
        elif desk_risk_state in {"HALT_NEW_ENTRIES", "FLATTEN_AND_HALT"}:
            verdict = "NOT ELIGIBLE: OPEN-RISK / REVIEW REQUIRED"
            clear_action = str(desk_risk.get("unblock_action_required") or "Clear Risk Halts")
            primary_reason = desk_risk_state
            state_note = "Risk-driven hold. A desk-level paper risk guardrail is active."
        elif open_exposure or unresolved_intents > 0:
            verdict = "NOT ELIGIBLE: OPEN-RISK / REVIEW REQUIRED"
            if operator_state.get("stop_after_cycle_requested"):
                clear_action = "Stop After Current Cycle first"
            elif open_exposure:
                clear_action = "Flatten And Halt first"
            else:
                clear_action = "Manual inspection required"
            primary_reason = "OPEN_RISK_REVIEW_REQUIRED"
            state_note = "Risk-driven hold. Open exposure or unresolved intents are still present."
        elif runtime_phase in {"STOPPED", "STOPPING"}:
            verdict = "NOT ELIGIBLE: RUNTIME STOPPED"
            clear_action = "Start Paper Soak"
            primary_reason = "RUNTIME_STOPPED"
            if operator_state.get("last_control_action") in {"flatten_and_halt", "stop_after_cycle"} or operator_state.get("halt_reason"):
                state_note = "Intentional operator stop. The desk may be safe, but approved models cannot fire until paper runtime is started again."
            else:
                state_note = "Runtime is not running, so approved models cannot fire."
        elif operator_halt or not entries_enabled:
            verdict = "NOT ELIGIBLE: ENTRIES HALTED BY OPERATOR"
            clear_action = "Resume Entries"
            primary_reason = "ENTRIES_HALTED_BY_OPERATOR"
            state_note = "Intentional operator hold. Approved models are enabled but entries are halted."
        elif runtime_phase == "RUNNING":
            verdict = "ELIGIBLE TO FIRE"
            clear_action = "No action needed; already eligible"
            primary_reason = None
            state_note = "Eligible but idle is possible. Approved models may act only if they actually trigger."
        else:
            verdict = "UNKNOWN / INSUFFICIENT STATE"
            clear_action = "Manual inspection required"
            primary_reason = "UNKNOWN_STATE"
            state_note = "The dashboard cannot truthfully classify entry eligibility from current persisted state."

        reasons: list[dict[str, str]] = []

        def _reason(label: str, value: str, source: str, *, timestamp: str | None = None) -> None:
            reasons.append(
                {
                    "label": label,
                    "value": value,
                    "source": source,
                    "timestamp": timestamp or "",
                }
            )

        if operator_state.get("last_control_action"):
            _reason(
                "Last control",
                f"{operator_state.get('last_control_action')} • {operator_state.get('last_control_status') or '-'}",
                "paper.operator_state.last_control_action / last_control_status",
                timestamp=operator_state.get("last_control_timestamp"),
            )
        if operator_state.get("halt_reason"):
            _reason(
                "Halt reason",
                str(operator_state.get("halt_reason")),
                "paper.operator_state.halt_reason",
                timestamp=operator_state.get("last_control_timestamp"),
            )
        _reason("Runtime phase", runtime_phase, "paper.readiness.runtime_phase")
        _reason("Entries state", "HALTED" if operator_halt or not entries_enabled else "ENABLED", "paper.status.entries_enabled / operator_halt")
        _reason("Desk risk", desk_risk_state, "paper.desk_risk.desk_risk_state")
        _reason("Open exposure", "YES" if open_exposure else "NO", "paper.position.side / quantity")
        _reason("Reconciliation", status.get("reconciliation_semantics") or "UNKNOWN", "paper.status.reconciliation_semantics")
        _reason("Fault", status.get("fault_state") or "UNKNOWN", "paper.status.fault_state")
        _reason(
            "Approved models enabled",
            f"{len(enabled_rows)} / {len(rows)}",
            "paper.approved_models.rows[].enabled",
        )
        _reason(
            "Review gating",
            (
                "REQUIRED / COMPLETED"
                if review_required and review_completed
                else ("REQUIRED / PENDING" if review_required else "NOT REQUIRED")
            ),
            "paper_pre_session_review.required / completed",
        )
        _reason(
            "Unresolved intents",
            str(unresolved_intents),
            "paper.approved_models.details_by_branch[].unresolved_intent_count",
        )
        if latest_severe:
            _reason(
                "Active severe exception",
                f"{latest_severe.get('code') or '-'} ({latest_severe.get('severity') or '-'})",
                "paper.exceptions.exceptions[severity in ACTION/BLOCKING]",
                timestamp=str(latest_severe.get("timestamp") or ""),
            )
        reasons.sort(key=lambda row: str(row.get("timestamp") or ""), reverse=True)

        eligible_now = verdict == "ELIGIBLE TO FIRE"
        if eligible_now and not signal_seen:
            fireability_detail = "Eligible now; no active signal seen yet."
        elif eligible_now:
            fireability_detail = "Eligible now; eligibility does not imply a signal is currently present."
        else:
            fireability_detail = f"Not eligible now; primary blocker is {primary_reason.replace('_', ' ') if primary_reason else verdict}."

        provenance_fields = [
            "paper.readiness.runtime_phase",
            "paper.status.entries_enabled",
            "paper.status.operator_halt",
            "paper.operator_state.last_control_action",
            "paper.operator_state.halt_reason",
            "paper.position.side",
            "paper.status.reconciliation_semantics",
            "paper.status.fault_state",
            "paper.approved_models.rows[].enabled",
            "paper.approved_models.details_by_branch[].unresolved_intent_count",
            "paper_pre_session_review.required/completed",
        ]
        if latest_severe:
            provenance_fields.append("paper.exceptions.exceptions[]")

        return {
            "verdict": verdict,
            "state_note": state_note,
            "clear_action": clear_action,
            "primary_blocking_reason": primary_reason,
            "approved_models_eligible_now": eligible_now,
            "signal_seen_this_session": signal_seen,
            "fireability_summary": fireability_detail,
            "reasons": reasons,
            "provenance": "Derived from " + ", ".join(provenance_fields) + ".",
        }

    def _paper_soak_session_payload(self, paper: dict[str, Any], review_payload: dict[str, Any]) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        details_by_branch = approved_models.get("details_by_branch") or {}
        exceptions = paper.get("exceptions") or {}
        session_start = _paper_soak_session_start_timestamp(paper, details_by_branch)
        latest_update = paper.get("status", {}).get("last_update_ts")
        duration_label = _duration_between(session_start, latest_update)
        models_seen = sorted(
            branch
            for branch, detail in details_by_branch.items()
            if (
                int(detail.get("signal_count", 0) or 0) > 0
                or int(detail.get("blocked_count", 0) or 0) > 0
                or int(detail.get("intent_count", 0) or 0) > 0
                or int(detail.get("fill_count", 0) or 0) > 0
                or detail.get("open_position")
                or detail.get("realized_pnl") not in {None, ""}
            )
        )
        models_signaled = sorted(branch for branch, detail in details_by_branch.items() if int(detail.get("signal_count", 0) or 0) > 0)
        models_blocked = sorted(branch for branch, detail in details_by_branch.items() if int(detail.get("blocked_count", 0) or 0) > 0)
        models_intents = sorted(branch for branch, detail in details_by_branch.items() if int(detail.get("intent_count", 0) or 0) > 0)
        models_filled = sorted(branch for branch, detail in details_by_branch.items() if int(detail.get("fill_count", 0) or 0) > 0)
        models_open_now = sorted(branch for branch, detail in details_by_branch.items() if detail.get("open_position"))
        severe_exceptions = [
            row
            for row in (exceptions.get("exceptions") or [])
            if row.get("severity") in {"ACTION", "BLOCKING"}
        ]
        end_of_session_verdict = _paper_soak_end_verdict(
            paper=paper,
            review_payload=review_payload,
            models_signaled=models_signaled,
            models_blocked=models_blocked,
            models_intents=models_intents,
            models_filled=models_filled,
            models_open_now=models_open_now,
        )
        latest_evidence = self._load_latest_soak_evidence_metadata()
        return {
            "session_date": paper.get("status", {}).get("session_date"),
            "session_start": session_start,
            "runtime_duration": duration_label,
            "approved_models_seen": models_seen,
            "models_signaled": models_signaled,
            "models_blocked": models_blocked,
            "models_intents": models_intents,
            "models_filled": models_filled,
            "models_open_now": models_open_now,
            "severe_exception_seen": bool(severe_exceptions),
            "severe_exception_count": len(severe_exceptions),
            "current_session_verdict": exceptions.get("session_verdict"),
            "end_of_session_verdict": end_of_session_verdict,
            "summary_generated": bool(paper.get("summary_available")),
            "summary_missing_warning": bool(models_seen) and not paper.get("summary_available") and not paper.get("running"),
            "running": bool(paper.get("running")),
            "evidence_capture_available": True,
            "latest_evidence": latest_evidence,
            "artifacts": {
                "session_snapshot": "/api/operator-artifact/paper-soak-session",
                "approved_models": "/api/operator-artifact/paper-approved-models",
                "exceptions": "/api/operator-artifact/paper-exceptions",
                "latest_json": "/api/operator-artifact/paper-soak-evidence-latest-json"
                if latest_evidence.get("json_available")
                else None,
                "latest_md": "/api/operator-artifact/paper-soak-evidence-latest-md"
                if latest_evidence.get("md_available")
                else None,
            },
            "notes": [
                "Session counters are derived from persisted approved-model decisions, blocks, intents, fills, blotter rows, and current attributed open exposure.",
                "End-of-session verdict is derived from persisted session evidence only and does not assume missing activity.",
            ],
        }

    def _paper_session_close_review_payload(
        self,
        *,
        paper: dict[str, Any],
        review_payload: dict[str, Any],
        closeout_state: dict[str, Any],
    ) -> dict[str, Any]:
        approved_models = paper.get("approved_models") or {}
        rows = approved_models.get("rows") or []
        details_by_branch = approved_models.get("details_by_branch") or {}
        runtime_artifacts_dir = Path(str(paper.get("artifacts_dir")))
        session_date = (
            closeout_state.get("session_date")
            or review_payload.get("summary", {}).get("session_date")
            or paper.get("status", {}).get("session_date")
            or "unknown-session"
        )
        summary_json_path, _ = self.latest_summary_file("paper", "json", session_date=session_date)
        summary_md_path, _ = self.latest_summary_file("paper", "md", session_date=session_date)
        summary_blotter_path, _ = self.latest_summary_file("paper", "blotter", session_date=session_date)
        summary_blotter_rows = _read_csv_rows(summary_blotter_path, limit=None) if summary_blotter_path else []
        source_paths = {
            "operator_status": str((runtime_artifacts_dir / "operator_status.json").resolve()),
            "branch_sources": str((runtime_artifacts_dir / "branch_sources.jsonl").resolve()),
            "rule_blocks": str((runtime_artifacts_dir / "rule_blocks.jsonl").resolve()),
            "reconciliation": str((runtime_artifacts_dir / "reconciliation_events.jsonl").resolve()),
            "lane_risk": str((runtime_artifacts_dir / "runtime" / "paper_lane_risk_status.json").resolve()),
            "summary_json": str(summary_json_path.resolve()) if summary_json_path else None,
            "summary_md": str(summary_md_path.resolve()) if summary_md_path else None,
            "summary_blotter": str(summary_blotter_path.resolve()) if summary_blotter_path else None,
        }
        artifact_links = {
            "snapshot_json": "/api/operator-artifact/paper-session-close-review",
            "snapshot_md": "/api/operator-artifact/paper-session-close-review-md",
            "approved_models": "/api/operator-artifact/paper-approved-models",
            "decisions": "/api/operator-artifact/paper-branch-sources",
            "blocks": "/api/operator-artifact/paper-rule-blocks",
            "intents": "/api/operator-artifact/paper-latest-intents",
            "fills": "/api/operator-artifact/paper-latest-fills",
            "blotter": "/api/operator-artifact/paper-latest-blotter",
            "lane_risk": "/api/operator-artifact/paper-lane-risk-status",
            "reconciliation": "/api/operator-artifact/paper-reconciliation",
            "position": "/api/operator-artifact/paper-position-state",
            "status": "/api/operator-artifact/paper-operator-status",
        }

        review_rows: list[dict[str, Any]] = []
        active_lanes: list[str] = []
        idle_lanes: list[str] = []
        blocked_lanes: list[str] = []
        filled_lanes: list[str] = []
        open_lanes: list[str] = []
        review_required_lanes: list[str] = []
        realized_total = Decimal("0")
        realized_exact_count = 0
        realized_partial_count = 0
        realized_unattributable_count = 0
        exact_open_risk_ownership_count = 0
        ambiguous_open_risk_ownership_count = 0
        unattributed_realized_present = False
        gap_reason_counts: dict[str, int] = {}
        family_counts = {
            family: sum(1 for candidate in rows if str(candidate.get("source_family") or "") == family)
            for family in {str(row.get("source_family") or "") for row in rows if row.get("source_family")}
        }
        desk_open_exposure = paper.get("position", {}).get("side") != "FLAT"

        for row in rows:
            branch = str(row.get("branch") or "-")
            detail = details_by_branch.get(branch) or {}
            verdict = _paper_session_close_lane_verdict(detail, closeout_state)
            signal_count = int(detail.get("signal_count", 0) or 0)
            blocked_count = int(detail.get("blocked_count", 0) or 0)
            intent_count = int(detail.get("intent_count", 0) or 0)
            fill_count = int(detail.get("fill_count", 0) or 0)
            open_position = bool(detail.get("open_position"))
            latest_event_timestamp = detail.get("latest_activity_timestamp")
            if verdict == "IDLE":
                idle_lanes.append(branch)
            else:
                active_lanes.append(branch)
            if blocked_count > 0 or verdict == "BLOCKED_ONLY":
                blocked_lanes.append(branch)
            if fill_count > 0:
                filled_lanes.append(branch)
            if open_position:
                open_lanes.append(branch)
            if verdict in {"FILLED_WITH_OPEN_RISK", "HALTED_BY_RISK", "DIRTY_RECONCILIATION", "UNKNOWN_INSUFFICIENT_EVIDENCE"}:
                review_required_lanes.append(branch)

            family = str(row.get("source_family") or "")
            family_row_count = family_counts.get(family, 0)
            family_blotter_rows = [
                blotter_row
                for blotter_row in summary_blotter_rows
                if str(blotter_row.get("setup_family") or "") == family
            ]
            family_has_only_family_tagged_blotter = bool(family_blotter_rows) and not any(
                str(blotter_row.get("instrument") or blotter_row.get("symbol") or "").strip()
                for blotter_row in family_blotter_rows
            )
            realized_pnl_raw = detail.get("realized_pnl")
            realized_decimal = _decimal_or_none(realized_pnl_raw)
            realized_provenance = str(detail.get("realized_pnl_provenance") or "")
            exact_realized = realized_decimal is not None and realized_provenance in {"LANE_FILTERED_BLOTTER", "BRANCH_AGGREGATE_FALLBACK"}
            gap_reasons: list[str] = []
            if not closeout_state.get("reconciliation_clean"):
                gap_reasons.append("RECONCILIATION_NOT_CLEAN")
            if fill_count > 0 and not exact_realized:
                if family_has_only_family_tagged_blotter:
                    gap_reasons.append("FAMILY_TAGGED_BLOTTER_ONLY")
                if family_row_count > 1:
                    gap_reasons.append("MULTI_LANE_SAME_FAMILY_AMBIGUITY")
                else:
                    gap_reasons.append("MISSING_FILL_TO_LANE_LINK")
            if int(detail.get("decision_count", 0) or 0) > 0 and int(detail.get("intent_count", 0) or 0) == 0 and int(detail.get("fill_count", 0) or 0) == 0:
                gap_reasons.append("INSUFFICIENT_PERSISTED_EVIDENCE")

            open_position = bool(detail.get("open_position"))
            attributable_unrealized = detail.get("unrealized_pnl") if open_position else None
            unrealized_exact = bool(
                open_position
                and detail.get("unrealized_pnl_provenance") == "CURRENT_OPEN_POSITION_LINKED_TO_LANE"
                and closeout_state.get("reconciliation_clean")
            )
            unrealized_partial = bool(
                open_position
                and detail.get("unrealized_pnl_provenance") == "CURRENT_OPEN_POSITION_LINKED_TO_LANE"
                and not closeout_state.get("reconciliation_clean")
            )
            if open_position and detail.get("unrealized_pnl_provenance") != "CURRENT_OPEN_POSITION_LINKED_TO_LANE":
                gap_reasons.append("MISSING_POSITION_TO_LANE_LINK")
            if desk_open_exposure and not any(bool((details_by_branch.get(candidate.get("branch") or "") or {}).get("open_position")) for candidate in rows):
                gap_reasons.append("OPEN_EXPOSURE_OWNER_AMBIGUOUS")
            for reason in gap_reasons:
                gap_reason_counts[reason] = gap_reason_counts.get(reason, 0) + 1

            if exact_realized and family_has_only_family_tagged_blotter and family_row_count > 1:
                realized_status = "PARTIAL"
            elif exact_realized:
                realized_status = "EXACT"
            else:
                realized_status = "UNATTRIBUTABLE"

            if open_position and unrealized_exact:
                unrealized_status = "EXACT"
            elif open_position and unrealized_partial:
                unrealized_status = "PARTIAL"
            else:
                unrealized_status = "UNATTRIBUTABLE"

            unattributed_realized_pnl_present = bool(fill_count > 0 and realized_status != "EXACT")
            unattributed_unrealized_pnl_present = bool(desk_open_exposure and open_position and unrealized_status != "EXACT")
            if exact_realized:
                realized_total += realized_decimal
            if realized_status == "EXACT":
                realized_exact_count += 1
            elif realized_status == "PARTIAL":
                realized_partial_count += 1
            else:
                realized_unattributable_count += 1
            if unrealized_status == "EXACT" and open_position:
                exact_open_risk_ownership_count += 1
            elif open_position and unrealized_status != "EXACT":
                ambiguous_open_risk_ownership_count += 1
            if unattributed_realized_pnl_present:
                unattributed_realized_present = True

            activity_present = bool(signal_count > 0 or blocked_count > 0 or intent_count > 0 or fill_count > 0 or latest_event_timestamp)
            if verdict == "IDLE":
                attribution_confidence = "HIGH"
            elif "RECONCILIATION_NOT_CLEAN" in gap_reasons or "OPEN_EXPOSURE_OWNER_AMBIGUOUS" in gap_reasons:
                attribution_confidence = "NONE"
            elif fill_count > 0:
                if realized_status == "EXACT" and (not open_position or unrealized_status == "EXACT"):
                    attribution_confidence = "HIGH"
                elif realized_status == "PARTIAL" or (open_position and unrealized_status == "PARTIAL"):
                    attribution_confidence = "MEDIUM"
                else:
                    attribution_confidence = "LOW"
            elif activity_present:
                attribution_confidence = "HIGH"
            else:
                attribution_confidence = "NONE"

            review_confidence = {
                "HIGH": "REVIEW_TRUST_HIGH",
                "MEDIUM": "REVIEW_TRUST_MEDIUM",
                "LOW": "REVIEW_TRUST_LOW",
                "NONE": "REVIEW_TRUST_NONE",
            }[attribution_confidence]
            matching_intents = int(detail.get("matching_intent_count", intent_count) or 0)
            matching_fills = int(detail.get("matching_fill_count", fill_count) or 0)
            matching_blotter_rows = int(detail.get("matching_blotter_row_count", 0) or 0)
            matching_position_rows = int(detail.get("matching_position_row_count", 0) or 0)
            ambiguous_family_rows = len(family_blotter_rows) if family_has_only_family_tagged_blotter and family_row_count > 1 else 0
            missing_lane_links = sum(
                1
                for reason in gap_reasons
                if reason in {
                    "MISSING_FILL_TO_LANE_LINK",
                    "MISSING_POSITION_TO_LANE_LINK",
                    "OPEN_EXPOSURE_OWNER_AMBIGUOUS",
                    "INSUFFICIENT_PERSISTED_EVIDENCE",
                }
            )
            evidence_chain_status = _paper_attribution_evidence_chain_status(gap_reasons)
            open_first_label, open_first_href = _paper_attribution_open_first_recommendation(
                gap_reasons,
                open_position,
                fill_count,
            )
            if realized_status == "EXACT":
                realized_evidence_summary = "Realized P/L is directly tied to this lane from persisted lane-linked blotter/fill evidence."
            elif realized_status == "PARTIAL":
                realized_evidence_summary = "Realized P/L exists, but family-tagged or otherwise incomplete evidence prevents a fully lane-exact close attribution."
            elif fill_count > 0:
                realized_evidence_summary = "This lane filled, but realized P/L cannot be assigned safely because the persisted close evidence is ambiguous or missing."
            else:
                realized_evidence_summary = "No realized P/L is attributable for this lane in the current persisted session-close evidence."
            if unrealized_status == "EXACT":
                unrealized_evidence_summary = "Open exposure is linked directly to this lane from persisted current position and reconciliation state."
            elif unrealized_status == "PARTIAL":
                unrealized_evidence_summary = "Open exposure links to this lane, but reconciliation is not clean enough for a fully trusted open-risk attribution."
            elif open_position:
                unrealized_evidence_summary = "Open exposure exists, but the persisted position/reconciliation chain is not sufficient to attribute it exactly to this lane."
            else:
                unrealized_evidence_summary = "No attributable open exposure is present for this lane."
            manual_inspection_files = [open_first_label]
            if evidence_chain_status != "COMPLETE":
                if "FAMILY_TAGGED_BLOTTER_ONLY" in gap_reasons and "Blotter" not in manual_inspection_files:
                    manual_inspection_files.append("Blotter")
                if ("MISSING_FILL_TO_LANE_LINK" in gap_reasons or fill_count > 0) and "Fills" not in manual_inspection_files:
                    manual_inspection_files.append("Fills")
                if ("MISSING_POSITION_TO_LANE_LINK" in gap_reasons or "OPEN_EXPOSURE_OWNER_AMBIGUOUS" in gap_reasons) and "Position" not in manual_inspection_files:
                    manual_inspection_files.append("Position")
                if "RECONCILIATION_NOT_CLEAN" in gap_reasons and "Reconciliation" not in manual_inspection_files:
                    manual_inspection_files.append("Reconciliation")

            review_rows.append(
                {
                    "branch": branch,
                    "lane_id": row.get("lane_id"),
                    "instrument": row.get("instrument"),
                    "source_family": row.get("source_family"),
                    "session_restriction": row.get("session_restriction"),
                    "session_verdict": verdict,
                    "signal_count": signal_count,
                    "blocked_count": blocked_count,
                    "intent_count": intent_count,
                    "fill_count": fill_count,
                    "realized_pnl": realized_pnl_raw,
                    "realized_pnl_attribution_status": realized_status,
                    "attributable_realized_pnl": realized_pnl_raw if realized_status in {"EXACT", "PARTIAL"} else None,
                    "unattributed_realized_pnl_present": unattributed_realized_pnl_present,
                    "unrealized_pnl": attributable_unrealized,
                    "unrealized_pnl_attribution_status": unrealized_status,
                    "attributable_unrealized_pnl": attributable_unrealized if unrealized_status in {"EXACT", "PARTIAL"} else None,
                    "unattributed_unrealized_pnl_present": unattributed_unrealized_pnl_present,
                    "attribution_confidence": attribution_confidence,
                    "review_confidence": review_confidence,
                    "attribution_gap_reason": gap_reasons,
                    "evidence_chain_status": evidence_chain_status,
                    "realized_attribution_evidence_summary": realized_evidence_summary,
                    "unrealized_attribution_evidence_summary": unrealized_evidence_summary,
                    "evidence_links": {
                        "decisions": artifact_links["decisions"],
                        "blocks": artifact_links["blocks"],
                        "intents": artifact_links["intents"],
                        "fills": artifact_links["fills"],
                        "blotter": artifact_links["blotter"],
                        "position": artifact_links["position"],
                        "reconciliation": artifact_links["reconciliation"],
                    },
                    "evidence_counts": {
                        "matching_intents": matching_intents,
                        "matching_fills": matching_fills,
                        "matching_blotter_rows": matching_blotter_rows,
                        "matching_position_rows": matching_position_rows,
                        "ambiguous_family_rows": ambiguous_family_rows,
                        "missing_lane_links": missing_lane_links,
                    },
                    "open_first_recommendation": {
                        "label": open_first_label,
                        "href": open_first_href,
                    },
                    "manual_inspection_files": manual_inspection_files,
                    "open_position": open_position,
                    "latest_event_timestamp": latest_event_timestamp,
                    "latest_halt_reason": detail.get("lane_halt_reason"),
                    "risk_state": detail.get("risk_state", "OK"),
                    "artifacts": artifact_links,
                    "attribution_note": (
                        "Realized/unrealized values are shown only when directly attributable to this lane from persisted blotter/fill/open-position evidence."
                    ),
                }
            )

        review_rows.sort(key=lambda item: (str(item.get("latest_event_timestamp") or ""), item.get("branch") or ""), reverse=True)
        desk_close_verdict = _paper_desk_close_verdict(
            closeout_state=closeout_state,
            paper=paper,
            active_count=len(active_lanes),
            open_count=len(open_lanes),
        )
        if desk_close_verdict in {"DIRTY_CLOSE", "FAULTED_CLOSE"}:
            desk_review_confidence = "NONE"
        elif any(row.get("review_confidence") == "REVIEW_TRUST_NONE" for row in review_rows):
            desk_review_confidence = "NONE"
        elif unattributed_realized_present or ambiguous_open_risk_ownership_count > 0 or any(
            row.get("review_confidence") == "REVIEW_TRUST_LOW" for row in review_rows
        ):
            desk_review_confidence = "LOW"
        elif any(row.get("review_confidence") == "REVIEW_TRUST_MEDIUM" for row in review_rows):
            desk_review_confidence = "MEDIUM"
        else:
            desk_review_confidence = "HIGH"
        reliable_pnl_lanes = [
            row["branch"]
            for row in review_rows
            if row.get("realized_pnl_attribution_status") == "EXACT" and row.get("review_confidence") in {"REVIEW_TRUST_HIGH", "REVIEW_TRUST_MEDIUM"}
        ]
        manual_pnl_inspection_lanes = [
            row["branch"]
            for row in review_rows
            if row.get("realized_pnl_attribution_status") != "EXACT" or row.get("review_confidence") in {"REVIEW_TRUST_LOW", "REVIEW_TRUST_NONE"}
        ]
        complete_chain_lanes = [row["branch"] for row in review_rows if row.get("evidence_chain_status") == "COMPLETE"]
        partial_chain_lanes = [row["branch"] for row in review_rows if row.get("evidence_chain_status") == "PARTIAL"]
        broken_chain_lanes = [row["branch"] for row in review_rows if row.get("evidence_chain_status") == "BROKEN"]
        top_gap_reasons = [
            {"reason": reason, "count": count}
            for reason, count in sorted(gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "session_date": session_date,
            "desk_close_verdict": desk_close_verdict,
            "admitted_lanes_count": len(rows),
            "active_lanes_count": len(active_lanes),
            "blocked_lanes_count": len(blocked_lanes),
            "filled_lanes_count": len(filled_lanes),
            "open_lanes_count": len(open_lanes),
            "active_lanes": active_lanes,
            "idle_lanes": idle_lanes,
            "blocked_lanes": blocked_lanes,
            "filled_lanes": filled_lanes,
            "open_lanes": open_lanes,
            "review_required_lanes": review_required_lanes,
            "total_attributable_realized_pnl": _decimal_to_string(realized_total),
            "realized_attribution_coverage": f"{realized_exact_count}/{len(rows)} lanes exact",
            "desk_attribution_summary": {
                "exact_realized_attribution_count": realized_exact_count,
                "partial_realized_attribution_count": realized_partial_count,
                "unattributable_realized_attribution_count": realized_unattributable_count,
                "exact_open_risk_ownership_count": exact_open_risk_ownership_count,
                "ambiguous_open_risk_ownership_count": ambiguous_open_risk_ownership_count,
                "total_attributable_realized_pnl": _decimal_to_string(realized_total),
                "unattributed_realized_pnl_present": unattributed_realized_present,
                "desk_review_confidence": desk_review_confidence,
                "desk_pnl_completeness": "COMPLETE" if not unattributed_realized_present else "PARTIAL",
                "reliable_pnl_judgment_lanes": reliable_pnl_lanes,
                "manual_pnl_inspection_lanes": manual_pnl_inspection_lanes,
                "complete_evidence_chain_lanes": complete_chain_lanes,
                "partial_evidence_chain_lanes": partial_chain_lanes,
                "broken_evidence_chain_lanes": broken_chain_lanes,
                "top_attribution_gap_reasons": top_gap_reasons,
            },
            "rows": review_rows,
            "source_paths": source_paths,
            "artifacts": artifact_links,
            "notes": [
                "Lane session verdicts are derived from persisted lane-filtered decisions, blocks, intents, fills, blotter rows, lane risk state, reconciliation state, and current open-position evidence only.",
                "Dirty reconciliation or open-risk states dominate clean-close interpretations when present.",
                "Different standalone strategy identities remain separate; no cross-identity P/L attribution is inferred.",
                "Desk-level P/L completeness degrades when any filled lane remains unattributable.",
            ],
        }

    def _paper_session_close_review_with_history(self, review: dict[str, Any]) -> dict[str, Any]:
        current_key = (str(review.get("session_date") or ""), str(review.get("generated_at") or ""))
        prior_reviews = self._load_prior_paper_session_close_reviews(current_key)
        row_history: dict[str, dict[str, Any]] = {}
        top_gap_reason_counts: dict[str, int] = {}
        archived_inventory: list[dict[str, Any]] = []

        for archived in prior_reviews:
            archived_inventory.append(
                {
                    "session_date": archived.get("session_date"),
                    "generated_at": archived.get("generated_at"),
                    "desk_close_verdict": archived.get("desk_close_verdict"),
                    "json_path": archived.get("_archive_json_path"),
                    "md_path": archived.get("_archive_md_path"),
                }
            )
            review_required = set(archived.get("review_required_lanes") or [])
            for row in archived.get("rows") or []:
                branch = str(row.get("branch") or "")
                if not branch:
                    continue
                history = row_history.setdefault(
                    branch,
                    {
                        "prior_close_reviews_found": 0,
                        "complete_chain_close_count": 0,
                        "partial_chain_close_count": 0,
                        "broken_chain_close_count": 0,
                        "exact_attribution_close_count": 0,
                        "partial_attribution_close_count": 0,
                        "unattributable_close_count": 0,
                        "dirty_close_count": 0,
                        "open_risk_close_count": 0,
                        "last_broken_close_ts": None,
                        "last_partial_close_ts": None,
                        "last_manual_review_required_ts": None,
                        "latest_relevant_archived_review_json_path": None,
                        "latest_relevant_archived_review_md_path": None,
                    },
                )
                generated_at = str(archived.get("generated_at") or "")
                history["prior_close_reviews_found"] += 1
                if row.get("evidence_chain_status") == "COMPLETE":
                    history["complete_chain_close_count"] += 1
                elif row.get("evidence_chain_status") == "PARTIAL":
                    history["partial_chain_close_count"] += 1
                    history["last_partial_close_ts"] = max(str(history.get("last_partial_close_ts") or ""), generated_at) or generated_at
                elif row.get("evidence_chain_status") == "BROKEN":
                    history["broken_chain_close_count"] += 1
                    history["last_broken_close_ts"] = max(str(history.get("last_broken_close_ts") or ""), generated_at) or generated_at
                if row.get("realized_pnl_attribution_status") == "EXACT":
                    history["exact_attribution_close_count"] += 1
                elif row.get("realized_pnl_attribution_status") == "PARTIAL":
                    history["partial_attribution_close_count"] += 1
                else:
                    history["unattributable_close_count"] += 1
                if row.get("session_verdict") == "DIRTY_RECONCILIATION" or archived.get("desk_close_verdict") == "DIRTY_CLOSE":
                    history["dirty_close_count"] += 1
                if row.get("open_position") or row.get("session_verdict") in {"FILLED_WITH_OPEN_RISK", "HALTED_BY_RISK"}:
                    history["open_risk_close_count"] += 1
                if branch in review_required or row.get("review_confidence") in {"REVIEW_TRUST_LOW", "REVIEW_TRUST_NONE"}:
                    history["last_manual_review_required_ts"] = max(str(history.get("last_manual_review_required_ts") or ""), generated_at) or generated_at
                history["latest_relevant_archived_review_json_path"] = archived.get("_archive_json_path")
                history["latest_relevant_archived_review_md_path"] = archived.get("_archive_md_path")
                for reason in row.get("attribution_gap_reason") or []:
                    top_gap_reason_counts[reason] = top_gap_reason_counts.get(reason, 0) + 1

        repeat_partial_lanes: list[str] = []
        repeat_broken_lanes: list[str] = []
        repeat_dirty_lanes: list[str] = []
        repeat_open_risk_lanes: list[str] = []
        repeat_unattributed_lanes: list[str] = []
        lanes_with_insufficient_history: list[str] = []
        lanes_with_sufficient_history: list[str] = []

        for row in review.get("rows") or []:
            branch = str(row.get("branch") or "")
            history = {
                "prior_close_reviews_found": 0,
                "complete_chain_close_count": 0,
                "partial_chain_close_count": 0,
                "broken_chain_close_count": 0,
                "exact_attribution_close_count": 0,
                "partial_attribution_close_count": 0,
                "unattributable_close_count": 0,
                "dirty_close_count": 0,
                "open_risk_close_count": 0,
                "last_broken_close_ts": None,
                "last_partial_close_ts": None,
                "last_manual_review_required_ts": None,
                "latest_relevant_archived_review_json_path": None,
                "latest_relevant_archived_review_md_path": None,
            }
            history.update(row_history.get(branch, {}))
            prior_count = int(history.get("prior_close_reviews_found", 0) or 0)
            if prior_count <= 0:
                history_sufficiency_status = "HISTORY_NONE"
            elif prior_count < PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD:
                history_sufficiency_status = "HISTORY_SPARSE"
            else:
                history_sufficiency_status = "HISTORY_SUFFICIENT"
            clean_history_judgment_allowed = history_sufficiency_status == "HISTORY_SUFFICIENT"
            current_chain = str(row.get("evidence_chain_status") or "BROKEN")
            current_dirty = row.get("session_verdict") == "DIRTY_RECONCILIATION"
            current_open_risk = bool(row.get("open_position")) or row.get("session_verdict") in {"FILLED_WITH_OPEN_RISK", "HALTED_BY_RISK"}
            if prior_count == 0:
                repeat_verdict = "NO_REPEAT_ISSUE_SEEN"
                repeat_review_confidence = "LOW"
                history_note = "No repeat issue seen yet, but no prior archived close reviews exist."
            elif (
                int(history.get("broken_chain_close_count", 0) or 0)
                + int(history.get("partial_chain_close_count", 0) or 0)
                + int(history.get("dirty_close_count", 0) or 0)
                + int(history.get("open_risk_close_count", 0) or 0)
                >= 3
            ):
                repeat_verdict = "MANUAL_REVIEW_PATTERN"
                repeat_review_confidence = "HIGH" if clean_history_judgment_allowed else "MEDIUM"
                history_note = f"{prior_count} prior archived close review(s) found with multiple repeat review issues."
            elif current_chain == "BROKEN" and int(history.get("broken_chain_close_count", 0) or 0) >= 1:
                repeat_verdict = "WATCH_REPEAT_BROKEN"
                repeat_review_confidence = "HIGH" if clean_history_judgment_allowed else "MEDIUM"
                history_note = f"{prior_count} prior archived close review(s); broken evidence chain seen before."
            elif current_chain == "PARTIAL" and int(history.get("partial_chain_close_count", 0) or 0) >= 1:
                repeat_verdict = "WATCH_REPEAT_PARTIAL"
                repeat_review_confidence = "HIGH" if clean_history_judgment_allowed else "MEDIUM"
                history_note = f"{prior_count} prior archived close review(s); partial evidence chain seen before."
            elif current_dirty and int(history.get("dirty_close_count", 0) or 0) >= 1:
                repeat_verdict = "WATCH_REPEAT_DIRTY_CLOSE"
                repeat_review_confidence = "HIGH" if clean_history_judgment_allowed else "MEDIUM"
                history_note = f"{prior_count} prior archived close review(s); dirty close seen before."
            elif current_open_risk and int(history.get("open_risk_close_count", 0) or 0) >= 1:
                repeat_verdict = "WATCH_REPEAT_OPEN_RISK"
                repeat_review_confidence = "HIGH" if clean_history_judgment_allowed else "MEDIUM"
                history_note = f"{prior_count} prior archived close review(s); open-risk close seen before."
            else:
                repeat_verdict = "NO_REPEAT_ISSUE_SEEN"
                if history_sufficiency_status == "HISTORY_SPARSE":
                    repeat_review_confidence = "LOW"
                    history_note = f"No repeat issue seen yet, but history is still sparse ({prior_count}/{PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD} archived close reviews)."
                else:
                    repeat_review_confidence = "HIGH"
                    history_note = f"Historically clean across {prior_count} archived close reviews."

            if repeat_verdict == "WATCH_REPEAT_PARTIAL":
                repeat_partial_lanes.append(branch)
            elif repeat_verdict == "WATCH_REPEAT_BROKEN":
                repeat_broken_lanes.append(branch)
            elif repeat_verdict == "WATCH_REPEAT_DIRTY_CLOSE":
                repeat_dirty_lanes.append(branch)
            elif repeat_verdict == "WATCH_REPEAT_OPEN_RISK":
                repeat_open_risk_lanes.append(branch)
            elif repeat_verdict == "MANUAL_REVIEW_PATTERN":
                repeat_partial_lanes.append(branch)
                repeat_broken_lanes.append(branch)
            if int(history.get("unattributable_close_count", 0) or 0) >= 1 and row.get("realized_pnl_attribution_status") == "UNATTRIBUTABLE":
                repeat_unattributed_lanes.append(branch)
            if clean_history_judgment_allowed:
                lanes_with_sufficient_history.append(branch)
            else:
                lanes_with_insufficient_history.append(branch)

            row.update(history)
            row["history_sessions_found"] = prior_count
            row["history_sufficiency_status"] = history_sufficiency_status
            row["minimum_history_threshold_for_clean_judgment"] = PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD
            row["clean_history_judgment_allowed"] = clean_history_judgment_allowed
            row["repeat_review_verdict"] = repeat_verdict
            row["repeat_review_confidence"] = repeat_review_confidence
            row["history_note"] = history_note
            row["latest_relevant_archive"] = {
                "json_path": history.get("latest_relevant_archived_review_json_path"),
                "md_path": history.get("latest_relevant_archived_review_md_path"),
            }
            row["history_artifacts"] = {
                "inventory_json": "/api/operator-artifact/paper-session-close-review-history",
                "inventory_md": "/api/operator-artifact/paper-session-close-review-history-md",
            }

        if not prior_reviews:
            historical_trust_verdict = "CLOSE_HISTORY_MIXED"
            history_note = "No prior archived close reviews found. Historical repeat-pattern confidence is low."
            desk_history_confidence = "LOW"
        elif any(
            row.get("repeat_review_verdict") != "NO_REPEAT_ISSUE_SEEN"
            for row in (review.get("rows") or [])
        ):
            historical_trust_verdict = "CLOSE_HISTORY_REVIEW_REQUIRED"
            history_note = "Archived close-review history shows repeated lane-level review issues."
            desk_history_confidence = "HIGH" if not lanes_with_insufficient_history else "MEDIUM"
        elif lanes_with_insufficient_history:
            historical_trust_verdict = "CLOSE_HISTORY_MIXED"
            history_note = "Archived close-review history exists, but some lanes still have sparse prior coverage."
            desk_history_confidence = "LOW"
        else:
            historical_trust_verdict = "CLOSE_HISTORY_CLEAN"
            history_note = "Archived close-review history shows no repeated lane-level close issues."
            desk_history_confidence = "HIGH"

        review["desk_attribution_summary"].update(
            {
                "repeated_partial_chain_lanes": sorted(set(repeat_partial_lanes)),
                "repeated_broken_chain_lanes": sorted(set(repeat_broken_lanes)),
                "repeated_unattributable_realized_lanes": sorted(set(repeat_unattributed_lanes)),
                "repeated_dirty_close_lanes": sorted(set(repeat_dirty_lanes)),
                "repeated_open_risk_close_lanes": sorted(set(repeat_open_risk_lanes)),
                "top_recurring_attribution_gap_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in sorted(top_gap_reason_counts.items(), key=lambda item: (-item[1], item[0]))
                ],
                "historical_trust_verdict": historical_trust_verdict,
                "lanes_with_insufficient_history": sorted(set(lanes_with_insufficient_history)),
                "lanes_with_sufficient_history": sorted(set(lanes_with_sufficient_history)),
                "desk_history_confidence": desk_history_confidence,
                "history_threshold_note": (
                    f"Clean history judgment requires at least {PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD} prior archived close reviews per lane."
                ),
                "history_sufficiency_note": history_note,
            }
        )
        review["history_summary"] = {
            "generated_at": review.get("generated_at"),
            "session_date": review.get("session_date"),
            "prior_reviews_count": len(prior_reviews),
            "historical_trust_verdict": historical_trust_verdict,
            "desk_history_confidence": desk_history_confidence,
            "history_threshold_note": f"Clean history judgment requires at least {PAPER_CLOSE_HISTORY_MINIMUM_THRESHOLD} prior archived close reviews per lane.",
            "history_sufficiency_note": history_note,
            "lanes_with_insufficient_history": review["desk_attribution_summary"].get("lanes_with_insufficient_history") or [],
            "lanes_with_sufficient_history": review["desk_attribution_summary"].get("lanes_with_sufficient_history") or [],
            "complete_evidence_chain_lanes": review["desk_attribution_summary"].get("complete_evidence_chain_lanes") or [],
            "partial_evidence_chain_lanes": review["desk_attribution_summary"].get("partial_evidence_chain_lanes") or [],
            "broken_evidence_chain_lanes": review["desk_attribution_summary"].get("broken_evidence_chain_lanes") or [],
            "repeated_partial_chain_lanes": review["desk_attribution_summary"].get("repeated_partial_chain_lanes") or [],
            "repeated_broken_chain_lanes": review["desk_attribution_summary"].get("repeated_broken_chain_lanes") or [],
            "repeated_unattributable_realized_lanes": review["desk_attribution_summary"].get("repeated_unattributable_realized_lanes") or [],
            "repeated_dirty_close_lanes": review["desk_attribution_summary"].get("repeated_dirty_close_lanes") or [],
            "repeated_open_risk_close_lanes": review["desk_attribution_summary"].get("repeated_open_risk_close_lanes") or [],
            "top_recurring_attribution_gap_reasons": review["desk_attribution_summary"].get("top_recurring_attribution_gap_reasons") or [],
            "archived_reviews": archived_inventory,
        }
        review["artifacts"]["history_json"] = "/api/operator-artifact/paper-session-close-review-history"
        review["artifacts"]["history_md"] = "/api/operator-artifact/paper-session-close-review-history-md"
        return review

    def _load_prior_paper_session_close_reviews(self, current_key: tuple[str, str]) -> list[dict[str, Any]]:
        reviews: list[dict[str, Any]] = []
        dedupe: set[tuple[str, str]] = set()
        for path in self._iter_prior_paper_session_close_review_paths():
            if path.name == "history_index.json":
                continue
            payload = _read_json(path)
            if not payload:
                continue
            key = (str(payload.get("session_date") or ""), str(payload.get("generated_at") or ""))
            if key == current_key or key in dedupe:
                continue
            dedupe.add(key)
            payload["_archive_json_path"] = str(path.resolve())
            md_candidate = path.with_suffix(".md")
            payload["_archive_md_path"] = str(md_candidate.resolve()) if md_candidate.exists() else None
            reviews.append(payload)
        reviews.sort(key=lambda item: (str(item.get("session_date") or ""), str(item.get("generated_at") or "")), reverse=True)
        return reviews

    def _iter_prior_paper_session_close_review_paths(self) -> list[Path]:
        canonical_by_session: dict[str, Path] = {}
        latest_timestamped_by_session: dict[str, Path] = {}
        passthrough_paths: list[Path] = []
        for path in sorted(self._paper_session_close_review_dir.glob("*.json")):
            if path.name == "history_index.json":
                continue
            canonical_match = _SESSION_CLOSE_REVIEW_CANONICAL_RE.match(path.name)
            if canonical_match:
                canonical_by_session[canonical_match.group("session_date")] = path
                continue
            timestamped_match = _SESSION_CLOSE_REVIEW_TIMESTAMPED_RE.match(path.name)
            if timestamped_match:
                session_date = timestamped_match.group("session_date")
                existing = latest_timestamped_by_session.get(session_date)
                if existing is None or path.name > existing.name:
                    latest_timestamped_by_session[session_date] = path
                continue
            passthrough_paths.append(path)

        selected_paths = list(passthrough_paths)
        session_dates = sorted(set(canonical_by_session) | set(latest_timestamped_by_session))
        for session_date in session_dates:
            selected_paths.append(canonical_by_session.get(session_date) or latest_timestamped_by_session[session_date])
        return sorted(selected_paths)

    def _load_latest_soak_evidence_metadata(self) -> dict[str, Any]:
        json_payload = _read_json(self._paper_soak_evidence_latest_json_path) if self._paper_soak_evidence_latest_json_path.exists() else {}
        return {
            "json_available": self._paper_soak_evidence_latest_json_path.exists(),
            "md_available": self._paper_soak_evidence_latest_md_path.exists(),
            "captured_at": json_payload.get("captured_at"),
            "session_date": json_payload.get("session_date"),
            "end_of_session_verdict": json_payload.get("end_of_session_verdict"),
        }

    def _capture_paper_soak_evidence(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        paper = snapshot["paper"]
        soak = paper.get("soak_session") or {}
        captured_at = datetime.now(timezone.utc).isoformat()
        session_date = soak.get("session_date") or paper.get("status", {}).get("session_date") or "unknown-session"
        capture_id = captured_at.replace(":", "").replace("-", "")
        bundle_json_path = self._paper_soak_evidence_dir / f"{session_date}_{capture_id}.json"
        bundle_md_path = self._paper_soak_evidence_dir / f"{session_date}_{capture_id}.md"
        runtime_artifacts_dir = Path(str(paper.get("artifacts_dir")))
        summary_json_path, _ = self.latest_summary_file("paper", "json", session_date=session_date)
        summary_md_path, _ = self.latest_summary_file("paper", "md", session_date=session_date)
        summary_blotter_path, _ = self.latest_summary_file("paper", "blotter", session_date=session_date)
        bundle = {
            "captured_at": captured_at,
            "session_date": session_date,
            "paper_running": paper.get("running"),
            "current_session_verdict": soak.get("current_session_verdict"),
            "end_of_session_verdict": soak.get("end_of_session_verdict"),
            "summary_generated": soak.get("summary_generated"),
            "source_paths": {
                "operator_status": str((runtime_artifacts_dir / "operator_status.json").resolve()),
                "branch_sources": str((runtime_artifacts_dir / "branch_sources.jsonl").resolve()),
                "rule_blocks": str((runtime_artifacts_dir / "rule_blocks.jsonl").resolve()),
                "reconciliation": str((runtime_artifacts_dir / "reconciliation_events.jsonl").resolve()),
                "paper_summary_json": str(summary_json_path.resolve()) if summary_json_path else None,
                "paper_summary_md": str(summary_md_path.resolve()) if summary_md_path else None,
                "paper_summary_blotter": str(summary_blotter_path.resolve()) if summary_blotter_path else None,
            },
            "session_summary": soak,
            "approved_models_snapshot": paper.get("approved_models"),
            "exceptions_snapshot": paper.get("exceptions"),
            "operator_status": paper.get("raw_operator_status"),
            "latest_intents": {"rows": paper.get("latest_intents") or []},
            "latest_fills": {"rows": paper.get("latest_fills") or []},
            "latest_blotter": {
                "rows": paper.get("latest_blotter_rows") or [],
                "blotter_path": paper.get("blotter_path"),
            },
            "latest_position_state": paper.get("position"),
            "reconciliation_events": _session_jsonl_rows(
                runtime_artifacts_dir / "reconciliation_events.jsonl",
                session_date,
                "logged_at",
            ),
            "rule_blocks": _session_jsonl_rows(
                runtime_artifacts_dir / "rule_blocks.jsonl",
                session_date,
                "logged_at",
                "bar_end_ts",
            ),
            "branch_sources": _session_jsonl_rows(
                runtime_artifacts_dir / "branch_sources.jsonl",
                session_date,
                "logged_at",
                "bar_end_ts",
            ),
            "paper_summary": {
                "available": snapshot.get("review", {}).get("paper", {}).get("available", False),
                "links": snapshot.get("review", {}).get("paper", {}).get("links", {}),
                "summary": _read_json(summary_json_path) if summary_json_path else (paper.get("daily_summary") or {}),
            },
        }
        bundle_json_path.write_text(json.dumps(bundle, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        markdown = _paper_soak_evidence_markdown(bundle)
        bundle_md_path.write_text(markdown, encoding="utf-8")
        self._paper_soak_evidence_latest_json_path.write_text(json.dumps(bundle, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        self._paper_soak_evidence_latest_md_path.write_text(markdown, encoding="utf-8")
        result = self._result_record(
            action="capture-paper-soak-evidence",
            ok=True,
            command=None,
            output=f"Soak evidence captured to {bundle_json_path} and {bundle_md_path}.",
        )
        result["bundle_json_path"] = str(bundle_json_path)
        result["bundle_md_path"] = str(bundle_md_path)
        return result

    def _market_index_strip_payload(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        if self._market_index_cache and self._market_index_cache_at is not None:
            cache_age = (now - self._market_index_cache_at).total_seconds()
            if cache_age <= MARKET_INDEX_CACHE_SECONDS:
                return self._market_index_cache

        specs = _market_index_specs_from_config_path(MARKET_INDEX_CONFIG_PATH)

        base_payload = {
            "feed_source": "Schwab /quotes cash-index fetch.",
            "feed_state": "UNAVAILABLE",
            "feed_label": "INDEX FEED UNAVAILABLE",
            "updated_at": now.isoformat(),
            "age_seconds": None,
            "snapshot_artifact": "/api/operator-artifact/market-index-strip",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
            "symbols": [
                {
                    "label": spec["label"],
                    "name": spec["name"],
                    "external_symbol": spec["external_symbol"],
                    "display_symbol": spec["external_symbol"],
                    "source_type": spec["source_type"],
                    "current_value": None,
                    "absolute_change": None,
                    "percent_change": None,
                    "bid": None,
                    "ask": None,
                    "state": "UNAVAILABLE",
                    "value_state": "UNAVAILABLE",
                    "bid_ask_state": "UNAVAILABLE",
                    "bid_state": "UNAVAILABLE",
                    "ask_state": "UNAVAILABLE",
                    "render_classification": "UNAVAILABLE_NO_PAYLOAD",
                    "fallback_used": False,
                    "field_states": {
                        "current_value": {"available": False, "status": "UNAVAILABLE", "source_field": None},
                        "absolute_change": {"available": False, "status": "UNAVAILABLE", "source_field": None},
                        "percent_change": {"available": False, "status": "UNAVAILABLE", "source_field": None},
                        "bid": {"available": False, "status": "UNAVAILABLE", "source_field": None},
                        "ask": {"available": False, "status": "UNAVAILABLE", "source_field": None},
                    },
                    "diagnostic_codes": ["NO_SYMBOL_PAYLOAD"],
                    "note": "No live market-data context is currently available.",
                }
                for spec in specs
            ],
            "note": "Schwab quote auth/config unavailable, or quote fetch did not succeed.",
            "diagnostics": {
                "fetch_state": "UNAVAILABLE",
                "attempted_symbols": [str(spec["external_symbol"]) for spec in specs],
                "fallback_policy": {
                    "preference_order": [
                        "Direct Schwab cash index",
                        "Explicit labeled alternate only if direct symbol unsupported",
                    ],
                    "fallback_attempted": False,
                    "fallback_symbols_attempted": [],
                },
                "symbols": [],
                "error": "Schwab quote auth/config unavailable, or quote fetch did not succeed.",
            },
        }

        try:
            if not MARKET_INDEX_CONFIG_PATH.exists():
                return base_payload | {"note": f"Schwab config not found at {MARKET_INDEX_CONFIG_PATH}."}
            specs = _market_index_specs_from_config_path(MARKET_INDEX_CONFIG_PATH)
            external_symbols = [str(spec["external_symbol"]) for spec in specs]
            raw_payload, quote_runtime = self._fetch_dashboard_quote_payload(MARKET_INDEX_CONFIG_PATH, external_symbols)
            symbols, diagnostics_rows = _market_index_rows(raw_payload, specs)
            available_count = sum(1 for row in symbols if row["value_state"] in {"LIVE", "DELAYED"})
            payload_missing_count = sum(1 for row in diagnostics_rows if not row["payload_present"])
            primary_value_missing_count = sum(
                1
                for row in diagnostics_rows
                if row["payload_present"] and not row["field_states"]["current_value"]["available"]
            )
            primary_partial = payload_missing_count > 0 or primary_value_missing_count > 0
            bid_ask_missing_count = sum(
                1
                for row in diagnostics_rows
                if not row["field_states"]["bid"]["available"] or not row["field_states"]["ask"]["available"]
            )
            if available_count == 0:
                feed_state = "UNAVAILABLE"
                feed_label = "INDEX FEED UNAVAILABLE"
            elif primary_partial:
                feed_state = "PARTIAL"
                feed_label = "INDEX FEED PARTIAL"
            else:
                feed_state = "LIVE"
                feed_label = "INDEX FEED LIVE"
            note_parts = [
                f"{quote_runtime['source_label']} Primary quote fields live for {available_count}/{len(symbols)} symbols.",
            ]
            if bid_ask_missing_count:
                note_parts.append(f"Bid/ask unavailable for {bid_ask_missing_count}/{len(symbols)} symbols.")
            note_parts.append("Entitlement delay flags are only shown when the feed exposes them.")
            payload = {
                "feed_source": "Direct Schwab /quotes cash-index symbols.",
                "auth_mode": quote_runtime["auth_mode"],
                "token_source": quote_runtime.get("token_source"),
                "feed_state": feed_state,
                "feed_label": feed_label,
                "updated_at": now.isoformat(),
                "age_seconds": 0,
                "snapshot_artifact": "/api/operator-artifact/market-index-strip",
                "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
                "symbols": symbols,
                "note": " ".join(note_parts),
                "diagnostics": {
                    "fetch_state": "SUCCESS",
                    "auth_mode": quote_runtime["auth_mode"],
                    "attempted_symbols": external_symbols,
                    "fallback_policy": {
                        "preference_order": [
                            "Direct Schwab cash index",
                            "Explicit labeled alternate only if direct symbol unsupported",
                        ],
                        "fallback_attempted": any(row["fallback_used"] for row in diagnostics_rows),
                        "fallback_symbols_attempted": [],
                    },
                    "counts": {
                        "requested": len(symbols),
                        "payload_present": len(symbols) - payload_missing_count,
                        "primary_value_available": available_count,
                        "primary_value_missing": primary_value_missing_count,
                        "bid_ask_complete": len(symbols) - bid_ask_missing_count,
                        "bid_ask_partial_or_missing": bid_ask_missing_count,
                    },
                    "symbols": diagnostics_rows,
                    "error": None,
                },
            }
            self._market_index_cache = payload
            self._market_index_cache_at = now
            return payload
        except (SchwabAuthError, FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            if self._market_index_cache and self._market_index_cache_at is not None:
                cache_age = int((now - self._market_index_cache_at).total_seconds())
                stale_payload = {
                    **self._market_index_cache,
                    "feed_state": "STALE",
                    "feed_label": "INDEX FEED STALE",
                    "updated_at": self._market_index_cache.get("updated_at"),
                    "age_seconds": cache_age,
                    "note": f"Using last successful market-index snapshot because the current fetch failed: {exc}",
                    "diagnostics": {
                        **(self._market_index_cache.get("diagnostics", {}) or {}),
                        "fetch_state": "STALE_FALLBACK",
                        "fallback_policy": {
                            **((self._market_index_cache.get("diagnostics", {}) or {}).get("fallback_policy", {})),
                            "stale_snapshot_reused": True,
                        },
                        "error": str(exc),
                    },
                }
                return stale_payload
            diagnostics = dict(base_payload["diagnostics"])
            diagnostics["fetch_state"] = "TOTAL_FETCH_FAILURE"
            diagnostics["error"] = str(exc)
            return base_payload | {"note": f"Market-index fetch failed: {exc}", "diagnostics": diagnostics}

    def _treasury_curve_payload(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        if self._treasury_curve_cache and self._treasury_curve_cache_at is not None:
            cache_age = (now - self._treasury_curve_cache_at).total_seconds()
            if cache_age <= TREASURY_CURVE_CACHE_SECONDS:
                return self._treasury_curve_cache

        specs = _treasury_specs_from_config_path(MARKET_INDEX_CONFIG_PATH)
        base_payload = {
            "panel_title": "Treasury Curve",
            "feed_source": "Direct Schwab /quotes Treasury yield index fetch.",
            "panel_classification": "UNAVAILABLE",
            "feed_state": "UNAVAILABLE",
            "feed_label": "TREASURY BENCHMARKS UNAVAILABLE",
            "updated_at": now.isoformat(),
            "age_seconds": None,
            "snapshot_artifact": "/api/operator-artifact/treasury-curve",
            "diagnostic_artifact": "/api/operator-artifact/treasury-curve-diagnostics",
            "audit_artifact": "/api/operator-artifact/treasury-symbol-audit",
            "curve_note": "Benchmark Treasury panel uses only audited direct Schwab symbols: 3M, 5Y, 10Y, 30Y.",
            "coverage_note": "Full 1M-30Y direct Schwab quote coverage was audited and not verified on this path.",
            "summary": _treasury_curve_empty_summary(),
            "tenors": [_treasury_empty_row(spec) for spec in specs],
            "chart": _treasury_empty_chart(specs),
            "diagnostics": {
                "fetch_state": "UNAVAILABLE",
                "audited_unavailable_tenors": list(TREASURY_AUDITED_UNAVAILABLE_TENORS),
                "requested_tenor_map": [
                    {
                        "tenor": spec["tenor"],
                        "requested_symbol": spec["external_symbol"],
                        "source_note": spec["source_note"],
                    }
                    for spec in specs
                ],
                "counts": {
                    "requested": len(specs),
                    "current_live": 0,
                    "prior_available": 0,
                    "missing_current": len(specs),
                },
                "tenors": [_treasury_diagnostic_from_row(_treasury_empty_row(spec)) for spec in specs],
                "spreads": {
                    "3M10Y": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE"},
                    "5s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE"},
                    "10s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE"},
                },
                "regime_label": "INSUFFICIENT DATA",
                "error": "Treasury curve fetch unavailable.",
            },
        }
        try:
            if not MARKET_INDEX_CONFIG_PATH.exists():
                return base_payload | {"curve_note": f"Schwab config not found at {MARKET_INDEX_CONFIG_PATH}."}
            specs = _treasury_specs_from_config_path(MARKET_INDEX_CONFIG_PATH)
            requested_symbols = [str(spec["external_symbol"]) for spec in specs if str(spec["external_symbol"]).strip()]
            raw_payload, quote_runtime = self._fetch_dashboard_quote_payload(MARKET_INDEX_CONFIG_PATH, requested_symbols)
            rows, diagnostics_rows = _treasury_curve_rows(raw_payload, specs)
            summary = _treasury_curve_summary(rows)
            chart = _treasury_curve_chart(rows)
            current_live = sum(1 for row in rows if row["current_state"] in {"LIVE", "DELAYED"})
            prior_available = sum(1 for row in rows if row["prior_state"] == "AVAILABLE")
            missing_current = len(rows) - current_live
            if current_live == 0:
                panel_classification = "UNAVAILABLE"
                feed_label = "TREASURY BENCHMARKS UNAVAILABLE"
            elif missing_current > 0:
                panel_classification = "PARTIAL"
                feed_label = "TREASURY BENCHMARKS PARTIAL"
            else:
                panel_classification = "LIVE"
                feed_label = "TREASURY BENCHMARKS LIVE"
            curve_note_parts = [
                f"{quote_runtime['source_label']} Benchmark Treasury yields live for {current_live}/{len(rows)} verified tenors.",
                "Prior-session comparison uses the same quote payload closePrice values.",
            ]
            curve_note_parts.append("Full 1M-30Y direct Schwab quote coverage was audited and not verified on this path.")
            payload = {
                "panel_title": "Treasury Benchmarks",
                "feed_source": "Direct Schwab /quotes Treasury yield indices.",
                "auth_mode": quote_runtime["auth_mode"],
                "token_source": quote_runtime.get("token_source"),
                "panel_classification": panel_classification,
                "feed_state": panel_classification,
                "feed_label": feed_label,
                "updated_at": now.isoformat(),
                "age_seconds": 0,
                "snapshot_artifact": "/api/operator-artifact/treasury-curve",
                "diagnostic_artifact": "/api/operator-artifact/treasury-curve-diagnostics",
                "audit_artifact": "/api/operator-artifact/treasury-symbol-audit",
                "curve_note": " ".join(curve_note_parts),
                "coverage_note": "Audited unavailable direct tenors: 1M, 6M, 1Y, 2Y, 3Y, 7Y, 20Y.",
                "summary": summary,
                "tenors": rows,
                "chart": chart,
                "diagnostics": {
                    "fetch_state": "SUCCESS",
                    "auth_mode": quote_runtime["auth_mode"],
                    "audited_unavailable_tenors": list(TREASURY_AUDITED_UNAVAILABLE_TENORS),
                    "requested_tenor_map": [
                        {
                            "tenor": spec["tenor"],
                            "requested_symbol": spec["external_symbol"],
                            "source_note": spec["source_note"],
                        }
                        for spec in specs
                    ],
                    "counts": {
                        "requested": len(rows),
                        "current_live": current_live,
                        "prior_available": prior_available,
                        "missing_current": missing_current,
                    },
                    "tenors": diagnostics_rows,
                    "spreads": summary["spread_diagnostics"],
                    "regime_label": summary["curve_state_label"],
                    "error": None,
                },
            }
            self._treasury_curve_cache = payload
            self._treasury_curve_cache_at = now
            return payload
        except (SchwabAuthError, FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            if self._treasury_curve_cache and self._treasury_curve_cache_at is not None:
                cache_age = int((now - self._treasury_curve_cache_at).total_seconds())
                stale_payload = {
                    **self._treasury_curve_cache,
                    "panel_classification": "STALE",
                    "feed_state": "STALE",
                    "feed_label": "TREASURY BENCHMARKS STALE",
                    "age_seconds": cache_age,
                    "curve_note": f"Using last successful Treasury curve snapshot because the current fetch failed: {exc}",
                    "diagnostics": {
                        **(self._treasury_curve_cache.get("diagnostics", {}) or {}),
                        "fetch_state": "STALE_FALLBACK",
                        "error": str(exc),
                    },
                }
                return stale_payload
            diagnostics = dict(base_payload["diagnostics"])
            diagnostics["fetch_state"] = "TOTAL_FETCH_FAILURE"
            diagnostics["error"] = str(exc)
            return base_payload | {"curve_note": f"Treasury curve fetch failed: {exc}", "diagnostics": diagnostics}

    def _fetch_dashboard_quote_payload(self, config_path: Path, external_symbols: Sequence[str]) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            schwab_config = load_schwab_market_data_config(config_path)
            oauth_client = SchwabOAuthClient(
                config=schwab_config.auth,
                transport=UrllibJsonTransport(),
                token_store=SchwabTokenStore(schwab_config.auth.token_store_path),
            )
            quote_client = SchwabQuoteHttpClient(
                oauth_client=oauth_client,
                market_data_config=schwab_config,
                transport=UrllibJsonTransport(),
            )
            return quote_client.fetch_quotes(external_symbols), {
                "auth_mode": "env_oauth",
                "source_label": "Direct Schwab /quotes via env-backed OAuth.",
                "token_source": str(schwab_config.auth.token_store_path),
            }
        except SchwabAuthError as exc:
            return self._fetch_dashboard_quote_payload_via_token_store(
                config_path=config_path,
                external_symbols=external_symbols,
                env_error=str(exc),
            )

    def _fetch_dashboard_quote_payload_via_token_store(
        self,
        *,
        config_path: Path,
        external_symbols: Sequence[str],
        env_error: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload = json.loads(config_path.read_text(encoding="utf-8")) if config_path.exists() else {}
        token_file = str(payload.get("token_store_path") or ".local/schwab/tokens.json")
        token_path = Path(token_file).expanduser()
        if not token_path.is_absolute():
            token_path = self._repo_root / token_path
        token_store = SchwabTokenStore(token_path.resolve(strict=False))
        token_set = token_store.load()
        if token_set is None:
            raise SchwabAuthError(
                f"{env_error} No token file found at {token_store.path}. Next fix: run Schwab token bootstrap or set SCHWAB_TOKEN_FILE."
            )
        if token_set.is_expired():
            raise SchwabAuthError(
                f"{env_error} Stored access token at {token_store.path} is expired and cannot be refreshed without Schwab auth env. "
                "Next fix: export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL or rerun token bootstrap refresh."
            )
        base_url = str(payload.get("market_data_base_url") or "https://api.schwabapi.com/marketdata/v1").rstrip("/")
        query_param = str(payload.get("quotes_symbol_query_param") or "symbols")
        raw_payload = UrllibJsonTransport().request_json(
            HttpRequest(
                method="GET",
                url=f"{base_url}/quotes",
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token_set.access_token}",
                },
                query={query_param: ",".join(external_symbols)},
            )
        )
        return raw_payload, {
            "auth_mode": "stored_access_token",
            "source_label": "Direct Schwab /quotes via stored access token fallback.",
            "token_source": str(token_store.path),
        }

    def _paper_position_payload(
        self,
        operator_status: dict[str, Any],
        latest_bar_close: Decimal | None,
        daily_summary: dict[str, Any] | None,
        latest_blotter: list[dict[str, Any]],
    ) -> dict[str, Any]:
        reconciliation = operator_status.get("reconciliation", {})
        signed_qty = int(reconciliation.get("broker_position_quantity", 0) or 0)
        average_price_raw = reconciliation.get("broker_average_price")
        average_price = Decimal(str(average_price_raw)) if average_price_raw is not None else None
        point_value = Decimal(str(os.environ.get("REPLAY_POINT_VALUE", "10")))
        unrealized = None
        if average_price is not None and latest_bar_close is not None and signed_qty != 0:
            if signed_qty > 0:
                unrealized = (latest_bar_close - average_price) * Decimal(abs(signed_qty)) * point_value
            else:
                unrealized = (average_price - latest_bar_close) * Decimal(abs(signed_qty)) * point_value

        position_instrument = next(
            (
                str(row.get("instrument") or row.get("symbol") or "")
                for row in reversed(latest_blotter)
                if row.get("instrument") or row.get("symbol")
            ),
            "",
        )
        return {
            "instrument": str(operator_status.get("instrument") or position_instrument or "UNKNOWN"),
            "side": "LONG" if signed_qty > 0 else "SHORT" if signed_qty < 0 else "FLAT",
            "quantity": abs(signed_qty),
            "average_price": str(average_price) if average_price is not None else None,
            "latest_bar_close": str(latest_bar_close) if latest_bar_close is not None else None,
            "realized_pnl": None if daily_summary is None else daily_summary.get("realized_net_pnl"),
            "session_pnl": None if daily_summary is None else daily_summary.get("realized_net_pnl"),
            "unrealized_pnl": None if unrealized is None else str(unrealized),
            "pnl_by_branch": _aggregate_branch_pnl_from_blotter(latest_blotter),
            "provenance": {
                "realized": "Latest generated paper daily summary closed-trade ledger.",
                "unrealized": "Estimated from the latest captured bar close and the deterministic next-bar-open paper fill model.",
                "branch": "Derived by grouping the latest paper blotter by setup_family.",
            },
            "notes": [
                "Unrealized P/L is estimated from the latest captured bar close and the deterministic paper fill model.",
                "Realized/session P/L depends on the latest generated daily paper summary.",
            ],
        }

    def _paper_performance_payload(
        self,
        *,
        session_date: str,
        current_session_date: str | None,
        daily_summary: dict[str, Any] | None,
        full_blotter_rows: list[dict[str, Any]],
        blotter_path: Path | None,
        session_intents: list[dict[str, Any]],
        session_fills: list[dict[str, Any]],
        position: dict[str, Any],
    ) -> dict[str, Any]:
        summary_session_date = (daily_summary or {}).get("session_date")
        realized_direct = _decimal_or_none((daily_summary or {}).get("realized_net_pnl"))
        realized_derived = _sum_decimal_field(full_blotter_rows, "net_pnl")
        realized_value = realized_direct if realized_direct is not None else realized_derived
        realized_provenance = (
            "Direct from the latest paper daily summary."
            if realized_direct is not None
            else "Derived from the latest paper blotter."
            if realized_derived is not None
            else "Unavailable until a paper summary or blotter exists."
        )

        unrealized_value = _decimal_or_none(position.get("unrealized_pnl"))
        total_value = None
        total_scope = "Unavailable until realized or unrealized paper P/L is available."
        total_provenance = "Unavailable."
        if realized_value is not None and unrealized_value is not None:
            total_value = realized_value + unrealized_value
            if summary_session_date and current_session_date and summary_session_date == current_session_date:
                total_scope = f"Current paper session {summary_session_date}."
            else:
                total_scope = "Latest generated paper-session realized P/L plus current open-position unrealized P/L."
            total_provenance = "Mixed: direct realized P/L plus derived current unrealized P/L."
        elif realized_value is not None:
            total_value = realized_value
            total_scope = "Latest generated paper session realized P/L only."
            total_provenance = realized_provenance
        elif unrealized_value is not None:
            total_value = unrealized_value
            total_scope = "Current open-position unrealized P/L only."
            total_provenance = "Derived from the latest captured bar close and the deterministic paper fill model."

        closed_trade_count_direct = _int_or_none((daily_summary or {}).get("closed_trade_count"))
        closed_trade_count = closed_trade_count_direct if closed_trade_count_direct is not None else len(full_blotter_rows)
        fill_count_direct = _int_or_none((daily_summary or {}).get("fill_count"))
        fill_count = fill_count_direct if fill_count_direct is not None else len(session_fills)
        order_intent_count_direct = _int_or_none((daily_summary or {}).get("order_intent_count"))
        order_intent_count = order_intent_count_direct if order_intent_count_direct is not None else len(session_intents)
        processed_bars = _int_or_none((daily_summary or {}).get("processed_bars_session"))
        if processed_bars is None:
            processed_bars = _int_or_none((daily_summary or {}).get("processed_bars_total"))

        win_count, loss_count, flat_trade_count = _trade_outcome_counts(full_blotter_rows)
        largest_win, largest_loss = _largest_trade_outcomes(full_blotter_rows)
        average_realized_per_trade = None
        if realized_value is not None and closed_trade_count:
            average_realized_per_trade = realized_value / Decimal(closed_trade_count)

        allowed_by_source = dict((daily_summary or {}).get("allowed_branch_decisions_by_source", {}) or {})
        blocked_by_source = dict((daily_summary or {}).get("blocked_branch_decisions_by_source", {}) or {})
        fills_by_type = dict((daily_summary or {}).get("fills_by_intent_type", {}) or {})
        signals_generated = sum(int(value) for value in allowed_by_source.values())
        blocked_decisions = sum(int(value) for value in blocked_by_source.values())
        exit_fill_count = sum(
            int(value)
            for intent_type, value in fills_by_type.items()
            if "CLOSE" in str(intent_type).upper()
        )
        open_trade_count = 0 if position.get("side") == "FLAT" else 1
        open_branch = _infer_open_branch_source(position, session_intents)
        fills_by_branch = _fills_by_branch(session_intents, session_fills)
        intents_by_branch = _count_by_key(session_intents, "reason_code")
        branch_rows = self._branch_performance_rows(
            full_blotter_rows=full_blotter_rows,
            allowed_by_source=allowed_by_source,
            blocked_by_source=blocked_by_source,
            intents_by_branch=intents_by_branch,
            fills_by_branch=fills_by_branch,
            open_branch=open_branch,
            unrealized_value=unrealized_value,
        )
        recent_trades = self._recent_trade_rows(full_blotter_rows)

        if position.get("side") == "FLAT":
            exposure_summary = "Flat. No open paper exposure."
        else:
            exposure_summary = " ".join(
                [
                    f"{position.get('side', 'UNKNOWN')} {position.get('quantity', 0)} x {position.get('instrument', 'MGC')}",
                    f"@ {position.get('average_price') or '-'}",
                    f"mark {position.get('latest_bar_close') or 'N/A'}",
                ]
            )

        return {
            "session_date": session_date,
            "current_session_date": current_session_date,
            "summary_session_date": summary_session_date,
            "scope_label": f"Latest paper session {session_date}" if session_date else "Latest paper session",
            "blotter_path": str(blotter_path) if blotter_path is not None else None,
            "realized_pnl": _decimal_to_string(realized_value),
            "realized_scope": (
                f"Latest generated paper session {summary_session_date}."
                if summary_session_date
                else "Latest available paper blotter/session artifacts."
            ),
            "realized_provenance": realized_provenance,
            "unrealized_pnl": _decimal_to_string(unrealized_value),
            "unrealized_scope": "Current open paper position.",
            "unrealized_provenance": "Derived from the latest captured bar close and the deterministic paper fill model.",
            "total_pnl": _decimal_to_string(total_value),
            "total_scope": total_scope,
            "total_provenance": total_provenance,
            "open_exposure_summary": exposure_summary,
            "open_exposure_provenance": "Direct from paper reconciliation state plus the latest captured bar close.",
            "trade_count": closed_trade_count,
            "trade_count_provenance": (
                "Direct from the latest paper daily summary."
                if closed_trade_count_direct is not None
                else "Derived from the latest paper blotter."
            ),
            "fill_count": fill_count,
            "fill_count_provenance": (
                "Direct from the latest paper daily summary."
                if fill_count_direct is not None
                else "Derived from persisted paper fills for the session date."
            ),
            "win_count": win_count,
            "loss_count": loss_count,
            "flat_trade_count": flat_trade_count,
            "win_loss_provenance": "Derived from closed trades in the latest paper blotter.",
            "session_metrics": {
                "processed_bars": processed_bars,
                "signals_generated": signals_generated,
                "blocked_decisions": blocked_decisions,
                "order_intents": order_intent_count,
                "fills": fill_count,
                "exits": exit_fill_count,
                "open_trade_count": open_trade_count,
                "closed_trade_count": closed_trade_count,
                "average_realized_per_trade": _decimal_to_string(average_realized_per_trade),
                "largest_win": _decimal_to_string(largest_win),
                "largest_loss": _decimal_to_string(largest_loss),
                "scope": (
                    f"Latest paper session {summary_session_date}."
                    if summary_session_date
                    else "Latest paper-session artifacts."
                ),
                "provenance": {
                    "processed_bars": "Direct from the latest paper daily summary." if processed_bars is not None else "Unavailable.",
                    "signals_generated": "Direct from allowed branch decision counts in the latest paper daily summary.",
                    "blocked_decisions": "Direct from blocked branch decision counts in the latest paper daily summary.",
                    "fills": "Direct from the latest paper daily summary." if fill_count_direct is not None else "Derived from persisted paper fills for the session date.",
                    "exits": "Direct from fill intent-type counts in the latest paper daily summary.",
                    "average_realized_per_trade": "Derived from realized net P/L divided by closed paper trades.",
                    "largest_trade_outcomes": "Derived from the latest paper blotter.",
                },
            },
            "branch_performance": branch_rows,
            "branch_scope": (
                f"Latest paper session {summary_session_date or session_date}."
            ),
            "branch_provenance": "Signals/blocked counts come from the latest paper daily summary. Realized P/L, win rate, and trade outcomes are derived from the latest paper blotter. Unrealized branch P/L is only shown when a single current open branch can be inferred from persisted paper intents.",
            "recent_trades": recent_trades,
            "recent_trades_scope": f"Latest paper blotter for session {summary_session_date or session_date}.",
            "recent_trades_provenance": "Derived from the latest paper blotter rows, newest first.",
        }

    def _paper_strategy_performance_payload(
        self,
        *,
        paper: dict[str, Any],
        session_date: str,
        root_db_path: Path | None,
        approved_quant_baselines: dict[str, Any],
    ) -> dict[str, Any]:
        generated_at = datetime.now(timezone.utc).isoformat()
        current_session = str(
            _nested_get(paper, "raw_operator_status", "current_detected_session", default="")
            or paper.get("status", {}).get("strategy_status")
            or "UNKNOWN"
        )
        lane_rows = self._paper_lane_universe(paper)
        if not lane_rows:
            lane_rows = [
                {
                    "lane_id": "paper_runtime",
                    "display_name": "Paper Runtime",
                    "symbol": paper.get("position", {}).get("instrument") or "UNKNOWN",
                    "source_family": "UNKNOWN",
                    "database_url": f"sqlite:///{root_db_path}" if root_db_path is not None else None,
                    "position_side": paper.get("position", {}).get("side") or "FLAT",
                    "strategy_status": paper.get("status", {}).get("strategy_status") or "UNKNOWN",
                    "session_realized_pnl": paper.get("performance", {}).get("realized_pnl"),
                    "session_unrealized_pnl": paper.get("performance", {}).get("unrealized_pnl"),
                    "session_total_pnl": paper.get("performance", {}).get("total_pnl"),
                }
            ]

        rows: list[dict[str, Any]] = []
        trade_log_rows: list[dict[str, Any]] = []
        execution_likelihood_rows: list[dict[str, Any]] = []
        missing_mark_rows: list[str] = []
        limited_history_rows: list[str] = []

        for lane_row in lane_rows:
            lane_id = str(lane_row.get("lane_id") or "unknown_lane")
            display_name = str(lane_row.get("display_name") or lane_id)
            instrument = str(lane_row.get("symbol") or lane_row.get("instrument") or "UNKNOWN")
            source_candidates = (
                list(lane_row.get("approved_long_entry_sources") or [])
                or list(lane_row.get("approved_short_entry_sources") or [])
                or list(lane_row.get("long_sources") or [])
                or list(lane_row.get("short_sources") or [])
            )
            source_family = str(lane_row.get("source_family") or (source_candidates[0] if source_candidates else "UNKNOWN"))
            temporary_paper_strategy = _is_temporary_paper_strategy_row(lane_row)
            paper_strategy_class = "temporary_paper_strategy" if temporary_paper_strategy else "approved_or_admitted_paper_strategy"
            metrics_bucket = "experimental_temporary_paper" if temporary_paper_strategy else "approved_or_admitted_paper"
            point_value = _decimal_or_none(lane_row.get("point_value")) or Decimal("1")
            db_path = _resolve_sqlite_database_path(lane_row.get("database_url")) or root_db_path
            all_intents = _all_table_rows(db_path, "order_intents", "created_at")
            all_fills = _all_table_rows(db_path, "fills", "fill_timestamp")
            all_bars = _all_table_rows(db_path, "bars", "end_ts")
            bar_index_by_id = {
                str(row.get("bar_id")): index
                for index, row in enumerate(all_bars)
                if row.get("bar_id")
            }
            ledger = build_trade_ledger(
                all_intents,
                all_fills,
                {},
                point_value=point_value,
                fee_per_fill=Decimal("0"),
                slippage_per_fill=Decimal("0"),
            )
            filled_entry_rows = _filled_entry_history_rows(
                all_intents,
                all_fills,
                bar_index_by_id=bar_index_by_id,
            )
            summary = build_summary_metrics(ledger)
            cumulative_realized = summary.total_net_pnl
            session_realized = _decimal_or_none(lane_row.get("session_realized_pnl"))
            current_unrealized = _decimal_or_none(lane_row.get("session_unrealized_pnl"))
            day_pnl = _decimal_or_none(lane_row.get("session_total_pnl"))
            if day_pnl is None and session_realized is not None and current_unrealized is not None:
                day_pnl = session_realized + current_unrealized
            cumulative_pnl = cumulative_realized + current_unrealized if current_unrealized is not None else cumulative_realized
            max_drawdown = _strategy_max_drawdown_with_open_unrealized(ledger, current_unrealized)
            trade_count = len(ledger)
            latest_fill_timestamp = max((str(row.get("fill_timestamp") or "") for row in all_fills if row.get("fill_timestamp")), default="") or None
            latest_intent_timestamp = max((str(row.get("created_at") or "") for row in all_intents if row.get("created_at")), default="") or None
            latest_activity = max(
                [
                    value
                    for value in [
                        latest_fill_timestamp,
                        latest_intent_timestamp,
                        lane_row.get("entry_timestamp"),
                        lane_row.get("last_processed_bar_end_ts"),
                    ]
                    if value
                ],
                default=None,
            )
            position_side = str(lane_row.get("position_side") or "FLAT")
            entry_price = _decimal_or_none(lane_row.get("entry_price"))
            last_mark = _decimal_or_none(lane_row.get("last_mark"))
            if position_side != "FLAT" and last_mark is None:
                missing_mark_rows.append(display_name)
            if trade_count <= 1:
                limited_history_rows.append(display_name)

            current_status = str(lane_row.get("strategy_status") or "UNKNOWN")
            if lane_row.get("fault_code"):
                current_status = f"FAULT ({lane_row.get('fault_code')})"
            elif position_side != "FLAT":
                current_status = f"OPEN_{position_side}"
            elif str(lane_row.get("risk_state") or "OK") not in {"OK", "CLEAR", "READY", ""}:
                current_status = str(lane_row.get("risk_state"))

            identity = build_standalone_strategy_identity(
                instrument=instrument,
                lane_id=lane_id,
                source_family=source_family,
                strategy_name=display_name,
            )
            strategy_key = lane_id if temporary_paper_strategy else identity["standalone_strategy_id"]
            identity_strategy_family = str(lane_row.get("strategy_family") or identity["strategy_family"])
            identity_root = str(lane_row.get("strategy_identity_root") or identity["standalone_strategy_root"])
            identity_label = display_name if temporary_paper_strategy else identity["standalone_strategy_label"]
            legacy_strategy_key = _legacy_strategy_performance_key(
                lane_id=lane_id,
                instrument=instrument,
                source_family=source_family,
            )
            entry_phase_counts = Counter(str(row.get("entry_session_phase") or "UNKNOWN") for row in filled_entry_rows)
            session_bucket_counts = {
                bucket: sum(1 for row in filled_entry_rows if row.get("entry_session_bucket") == bucket)
                for bucket in STRATEGY_HISTORY_SESSION_BUCKETS
            }
            day_of_week_counts = Counter(str(row.get("day_of_week") or "UNKNOWN") for row in filled_entry_rows)
            entry_timestamps = [row.get("entry_dt") for row in filled_entry_rows if row.get("entry_dt") is not None]
            median_elapsed_seconds = _median_value(
                [
                    (entry_timestamps[index] - entry_timestamps[index - 1]).total_seconds()
                    for index in range(1, len(entry_timestamps))
                ]
            )
            median_bars_between_entries = _median_value(
                [
                    int(filled_entry_rows[index]["bar_index"]) - int(filled_entry_rows[index - 1]["bar_index"])
                    for index in range(1, len(filled_entry_rows))
                    if filled_entry_rows[index].get("bar_index") is not None
                    and filled_entry_rows[index - 1].get("bar_index") is not None
                ]
            )
            most_common_session_bucket, most_common_session_bucket_count = _strategy_most_common_label(
                Counter({bucket: count for bucket, count in session_bucket_counts.items() if count > 0}),
                default="UNKNOWN",
            )
            most_common_entry_phase, most_common_entry_phase_count = _strategy_most_common_label(
                entry_phase_counts,
                default="UNKNOWN",
            )
            last_fire_timestamp = str(filled_entry_rows[-1]["entry_timestamp"]) if filled_entry_rows else None
            last_fire_dt = _parse_iso_datetime(last_fire_timestamp)
            days_since_last_fire = (
                (datetime.now(timezone.utc).date() - last_fire_dt.astimezone(timezone.utc).date()).days
                if last_fire_dt is not None
                else None
            )
            expected_fire_cadence = _strategy_expected_fire_cadence_label(len(filled_entry_rows), median_elapsed_seconds)
            most_likely_next_window = (
                f"{most_common_entry_phase} ({most_common_entry_phase_count}/{len(filled_entry_rows)} entries)"
                if len(filled_entry_rows) >= 3 and most_common_entry_phase != "UNKNOWN"
                else (
                    f"{most_common_session_bucket} ({most_common_session_bucket_count}/{len(filled_entry_rows)} entries)"
                    if len(filled_entry_rows) >= 3 and most_common_session_bucket != "UNKNOWN"
                    else "Insufficient history"
                )
            )
            interpretation_state, interpretation = _strategy_operator_interpretation(
                entry_count=len(filled_entry_rows),
                expected_fire_cadence=expected_fire_cadence,
                current_session=current_session,
                most_common_entry_phase=most_common_entry_phase,
                most_common_session_bucket=most_common_session_bucket,
                entries_enabled=lane_row.get("entries_enabled"),
                operator_halt=lane_row.get("operator_halt"),
                same_underlying_entry_hold=lane_row.get("same_underlying_entry_hold"),
            )
            attribution_family_label = _strategy_attribution_family_label(
                source_family=source_family,
                side=str(lane_row.get("side") or position_side or ""),
            )
            rows.append(
                {
                    "id": strategy_key,
                    "strategy_key": strategy_key,
                    "standalone_strategy_id": strategy_key,
                    "legacy_strategy_key": legacy_strategy_key,
                    "lane_id": lane_id,
                    "strategy_name": display_name,
                    "instrument": instrument,
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": identity_strategy_family,
                    "standalone_strategy_root": identity_root,
                    "standalone_strategy_label": identity_label,
                    "paper_strategy_class": paper_strategy_class,
                    "metrics_bucket": metrics_bucket,
                    "paper_only": bool(lane_row.get("paper_only")),
                    "non_approved": bool(lane_row.get("non_approved")),
                    "experimental_status": lane_row.get("experimental_status"),
                    "signal_family_label": attribution_family_label,
                    "status": current_status,
                    "position_side": position_side,
                    "entry_timestamp": lane_row.get("entry_timestamp"),
                    "entry_price": _decimal_to_string(entry_price),
                    "last_mark": _decimal_to_string(last_mark),
                    "realized_pnl": _decimal_to_string(cumulative_realized),
                    "unrealized_pnl": _decimal_to_string(current_unrealized),
                    "day_pnl": _decimal_to_string(day_pnl),
                    "cumulative_pnl": _decimal_to_string(cumulative_pnl),
                    "max_drawdown": _decimal_to_string(max_drawdown),
                    "trade_count": trade_count,
                    "latest_fill_timestamp": latest_fill_timestamp,
                    "latest_activity_timestamp": latest_activity,
                    "risk_state": lane_row.get("risk_state") or "OK",
                    "halt_reason": lane_row.get("halt_reason"),
                    "session_restriction": lane_row.get("session_restriction"),
                    "entries_enabled": lane_row.get("entries_enabled"),
                    "operator_halt": lane_row.get("operator_halt"),
                    "history_start_timestamp": all_intents[0].get("created_at") if all_intents else None,
                    "history_end_timestamp": latest_activity,
                    "entry_count": len(filled_entry_rows),
                    "total_signal_count": None,
                    "total_signal_count_scope": "Unavailable from the current lane-local SQLite history; historical signal-only rows are not persisted there.",
                    "entries_by_session_bucket": session_bucket_counts,
                    "session_bucket_summary": _strategy_session_bucket_summary(session_bucket_counts),
                    "entries_by_day_of_week": dict(day_of_week_counts),
                    "day_of_week_summary": _strategy_day_of_week_summary(day_of_week_counts),
                    "median_bars_between_entries": round(median_bars_between_entries, 1) if median_bars_between_entries is not None else None,
                    "median_bars_between_entries_label": (
                        f"{median_bars_between_entries:.1f} bars"
                        if median_bars_between_entries is not None
                        else "Unavailable"
                    ),
                    "median_elapsed_between_entries_seconds": median_elapsed_seconds,
                    "median_elapsed_between_entries_label": _format_strategy_gap_label(median_elapsed_seconds) or "Unavailable",
                    "most_common_session_bucket": most_common_session_bucket,
                    "most_common_entry_phase": most_common_entry_phase,
                    "most_likely_next_window": most_likely_next_window,
                    "expected_fire_cadence": expected_fire_cadence,
                    "last_fire_timestamp": last_fire_timestamp,
                    "days_since_last_fire": days_since_last_fire,
                    "operator_interpretation_state": interpretation_state,
                    "operator_interpretation": interpretation,
                    "current_session": current_session,
                    "ledger_history_scope": (
                        "Cumulative realized P/L and trade count are based on persisted lane-local order intents/fills in the current lane database."
                    ),
                    "day_scope": f"Session/day P&L uses current lane session metrics for {session_date}.",
                    "unrealized_scope": (
                        "Current unrealized P/L comes from the lane runtime mark/reference state."
                        if current_unrealized is not None
                        else "Unavailable because the lane does not currently have a trusted open-position mark/reference price."
                    ),
                    "pnl_unavailable_reason": (
                        None
                        if current_unrealized is not None or position_side == "FLAT"
                        else "Trusted open-position mark/reference price is unavailable, so priced unrealized and cumulative P/L remain partial."
                    ),
                    "max_drawdown_method": "Computed from the cumulative closed-trade net P/L curve with current open unrealized P/L appended as the latest provisional equity point when available.",
                }
            )

            for trade in ledger:
                trade_log_rows.append(
                    {
                        "id": f"{strategy_key}:{trade.trade_id}",
                        "strategy_key": strategy_key,
                        "standalone_strategy_id": strategy_key,
                        "legacy_strategy_key": legacy_strategy_key,
                        "lane_id": lane_id,
                        "strategy_name": display_name,
                        "instrument": instrument,
                        "family": source_family,
                        "source_family": source_family,
                        "strategy_family": identity_strategy_family,
                        "standalone_strategy_root": identity_root,
                        "standalone_strategy_label": identity_label,
                        "paper_strategy_class": paper_strategy_class,
                        "metrics_bucket": metrics_bucket,
                        "paper_only": bool(lane_row.get("paper_only")),
                        "non_approved": bool(lane_row.get("non_approved")),
                        "experimental_status": lane_row.get("experimental_status"),
                        "signal_family_label": attribution_family_label,
                        "trade_id": trade.trade_id,
                        "side": trade.direction,
                        "entry_timestamp": trade.entry_ts.isoformat(),
                        "exit_timestamp": trade.exit_ts.isoformat(),
                        "entry_price": _decimal_to_string(trade.entry_px),
                        "exit_price": _decimal_to_string(trade.exit_px),
                        "quantity": trade.qty,
                        "realized_pnl": _decimal_to_string(trade.net_pnl),
                        "gross_pnl": _decimal_to_string(trade.gross_pnl),
                        "fees": _decimal_to_string(trade.fees),
                        "slippage": _decimal_to_string(trade.slippage),
                        "exit_reason": trade.exit_reason,
                        "signal_family": trade.setup_family,
                        "entry_session_phase": trade.entry_session_phase,
                        "exit_session_phase": trade.exit_session_phase,
                        "status": "CLOSED",
                    }
                )

            execution_likelihood_rows.append(
                {
                    "id": strategy_key,
                    "strategy_key": strategy_key,
                    "standalone_strategy_id": strategy_key,
                    "legacy_strategy_key": legacy_strategy_key,
                    "lane_id": lane_id,
                    "strategy_name": display_name,
                    "instrument": instrument,
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": identity_strategy_family,
                    "standalone_strategy_root": identity_root,
                    "standalone_strategy_label": identity_label,
                    "paper_strategy_class": paper_strategy_class,
                    "metrics_bucket": metrics_bucket,
                    "paper_only": bool(lane_row.get("paper_only")),
                    "non_approved": bool(lane_row.get("non_approved")),
                    "experimental_status": lane_row.get("experimental_status"),
                    "signal_family_label": attribution_family_label,
                    "trade_count": trade_count,
                    "total_signal_count": None,
                    "entry_count": len(filled_entry_rows),
                    "entries_by_session_bucket": session_bucket_counts,
                    "session_bucket_summary": _strategy_session_bucket_summary(session_bucket_counts),
                    "entries_by_day_of_week": dict(day_of_week_counts),
                    "day_of_week_summary": _strategy_day_of_week_summary(day_of_week_counts),
                    "median_bars_between_entries": round(median_bars_between_entries, 1) if median_bars_between_entries is not None else None,
                    "median_bars_between_entries_label": (
                        f"{median_bars_between_entries:.1f} bars"
                        if median_bars_between_entries is not None
                        else "Unavailable"
                    ),
                    "median_elapsed_between_entries_seconds": median_elapsed_seconds,
                    "median_elapsed_between_entries_label": _format_strategy_gap_label(median_elapsed_seconds) or "Unavailable",
                    "most_common_session_bucket": most_common_session_bucket,
                    "most_common_entry_phase": most_common_entry_phase,
                    "most_likely_next_window": most_likely_next_window,
                    "expected_fire_cadence": expected_fire_cadence,
                    "last_fire_timestamp": last_fire_timestamp,
                    "days_since_last_fire": days_since_last_fire,
                    "history_sufficient": len(filled_entry_rows) >= 3,
                    "current_session": current_session,
                    "operator_interpretation_state": interpretation_state,
                    "operator_interpretation": interpretation,
                }
            )

        quant_performance = _quant_strategy_performance_payload(
            repo_root=self._repo_root,
            approved_quant_baselines=approved_quant_baselines,
            session_date=session_date,
            current_session=current_session,
        )
        existing_strategy_ids = {
            str(row.get("standalone_strategy_id") or row.get("strategy_key") or "")
            for row in rows
            if row.get("standalone_strategy_id") or row.get("strategy_key")
        }
        rows.extend(
            row for row in quant_performance["rows"]
            if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "") not in existing_strategy_ids
        )
        trade_log_rows.extend(
            row for row in quant_performance["trade_log"]
            if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "") not in existing_strategy_ids
        )
        execution_likelihood_rows.extend(
            row for row in quant_performance["execution_likelihood"]
            if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "") not in existing_strategy_ids
        )
        missing_mark_rows.extend(quant_performance["warnings"].get("missing_mark_rows", []))
        limited_history_rows.extend(quant_performance["warnings"].get("limited_history_rows", []))
        runtime_lookup = {
            str(row.get("standalone_strategy_id") or ""): row
            for row in ((paper.get("runtime_registry") or {}).get("rows") or [])
            if row.get("standalone_strategy_id")
        }
        rows = [_annotate_runtime_identity_state(row, runtime_lookup) for row in rows]
        trade_log_rows = [_annotate_runtime_identity_state(row, runtime_lookup) for row in trade_log_rows]
        execution_likelihood_rows = [_annotate_runtime_identity_state(row, runtime_lookup) for row in execution_likelihood_rows]

        rows = _annotate_same_underlying_strategy_ambiguity(rows)
        trade_log_rows = _annotate_same_underlying_strategy_ambiguity(trade_log_rows)
        execution_likelihood_rows = _annotate_same_underlying_strategy_ambiguity(execution_likelihood_rows)
        rows.sort(key=lambda row: (_sort_decimal_value(row.get("cumulative_pnl")), str(row.get("strategy_name") or "")), reverse=True)
        trade_log_rows.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
        execution_likelihood_rows.sort(
            key=lambda row: (
                str(row.get("expected_fire_cadence") or ""),
                _sort_decimal_value(row.get("trade_count")),
                str(row.get("strategy_name") or ""),
            ),
            reverse=True,
        )
        attribution = _build_strategy_attribution_payload(trade_log_rows)
        return {
            "generated_at": generated_at,
            "session_date": session_date,
            "rows": rows,
            "portfolio_snapshot": _build_strategy_portfolio_snapshot(rows, generated_at=generated_at),
            "metrics_buckets": _build_strategy_metrics_bucket_snapshots(rows, generated_at=generated_at),
            "execution_likelihood": {
                "rows": execution_likelihood_rows,
                "scope": "Descriptive statistics from persisted lane-local entry history only. This does not change completed-bar execution semantics and is not a forward prediction.",
                "notes": [
                    "Completed-bar only: the engine still evaluates completed bars only and does not act on partial bars.",
                    "Expected fire cadence is descriptive/historical, not a guarantee of a future trade.",
                    "Historical signal counts remain unavailable when only lane-local SQLite order/fill history is present.",
                ],
            },
            "trade_log": trade_log_rows,
            "trade_log_scope": "Closed trades paired from persisted lane-local intents and fills. Open positions remain in the strategy line items rather than the closed-trade log.",
            "trade_log_notes": [
                "Each standalone strategy identity is keyed by a canonical standalone_strategy_id resolved from the strategy identity root plus instrument.",
                "Realized P/L comes only from completed closed trades.",
                "Unrealized P/L is current open-position P/L from the lane runtime when a trusted mark/reference price exists.",
            ],
            "attribution": attribution,
            "notes": [
                "Concurrent different-instrument standalone strategies remain separate and first-class in this ledger.",
                "Same-instrument ambiguity is surfaced explicitly and remains constrained; this ledger does not silently net or merge same-underlying identities.",
                "Cumulative metrics are limited to the currently available persisted lane-local history and are not backfilled beyond what exists on disk.",
                "Execution-likelihood fields are descriptive statistics from available persisted history only; no current-bar or intrabar decision logic was added.",
            ],
            "warnings": {
                "missing_mark_rows": missing_mark_rows,
                "limited_history_rows": limited_history_rows,
            },
            "provenance": {
                "strategy_rows": "Derived from supervisor lane operator_status plus each lane-local SQLite order_intents/fills history.",
                "trade_log": "Derived from deterministic pairing of lane-local order intents and fills using the existing replay-first trade ledger helper.",
                "attribution": "Derived by grouping closed trades by exact setup_family and a conservative operator-facing family label.",
                "execution_likelihood": "Derived from persisted lane-local entry fills only; no partial-bar or current-bar logic is used.",
            },
        }

    def _paper_signal_intent_fill_audit_payload(
        self,
        *,
        paper: dict[str, Any],
        session_date: str,
        root_db_path: Path | None,
    ) -> dict[str, Any]:
        generated_at = datetime.now(timezone.utc).isoformat()
        current_session = str(
            _nested_get(paper, "raw_operator_status", "current_detected_session", default="")
            or paper.get("status", {}).get("strategy_status")
            or "UNKNOWN"
        )
        inspection_label = f"Current session date {session_date}" if session_date else "Available persisted session scope"
        lane_rows = self._paper_lane_universe(paper)
        if not lane_rows:
            lane_rows = [
                {
                    "lane_id": "paper_runtime",
                    "display_name": "Paper Runtime",
                    "symbol": paper.get("position", {}).get("instrument") or "UNKNOWN",
                    "database_url": f"sqlite:///{root_db_path}" if root_db_path is not None else None,
                    "position_side": paper.get("position", {}).get("side") or "FLAT",
                    "strategy_status": paper.get("status", {}).get("strategy_status") or "UNKNOWN",
                    "entries_enabled": paper.get("status", {}).get("entries_enabled"),
                    "operator_halt": paper.get("status", {}).get("operator_halt"),
                }
            ]

        strategy_rows: dict[str, dict[str, Any]] = {}
        for row in ((paper.get("strategy_performance") or {}).get("rows") or []):
            for key in (
                row.get("standalone_strategy_id"),
                row.get("strategy_key"),
                row.get("legacy_strategy_key"),
                row.get("id"),
            ):
                key_text = str(key or "")
                if key_text:
                    strategy_rows[key_text] = row
        trade_log_rows = list((paper.get("strategy_performance") or {}).get("trade_log") or [])
        trade_log_counts_by_strategy: dict[str, int] = {}
        for row in trade_log_rows:
            for key in (
                row.get("standalone_strategy_id"),
                row.get("strategy_key"),
                row.get("legacy_strategy_key"),
            ):
                strategy_key = str(key or "")
                if strategy_key:
                    trade_log_counts_by_strategy[strategy_key] = trade_log_counts_by_strategy.get(strategy_key, 0) + 1

        rows: list[dict[str, Any]] = []
        verdict_counts: Counter[str] = Counter()
        inspection_start_candidates: list[str] = []
        inspection_end_candidates: list[str] = []

        for lane_row in lane_rows:
            lane_id = str(lane_row.get("lane_id") or "unknown_lane")
            strategy_name = str(lane_row.get("display_name") or lane_id)
            instrument = str(lane_row.get("symbol") or lane_row.get("instrument") or "UNKNOWN")
            temporary_paper_strategy = _is_temporary_paper_strategy_row(lane_row)
            source_candidates = (
                list(lane_row.get("approved_long_entry_sources") or [])
                or list(lane_row.get("approved_short_entry_sources") or [])
                or list(lane_row.get("long_sources") or [])
                or list(lane_row.get("short_sources") or [])
            )
            source_family = str(lane_row.get("source_family") or (source_candidates[0] if source_candidates else "UNKNOWN"))
            identity = build_standalone_strategy_identity(
                instrument=instrument,
                lane_id=lane_id,
                source_family=source_family,
                strategy_name=strategy_name,
            )
            strategy_key = lane_id if temporary_paper_strategy else identity["standalone_strategy_id"]
            legacy_strategy_key = _legacy_strategy_performance_key(
                lane_id=lane_id,
                instrument=instrument,
                source_family=source_family,
            )
            db_path = _resolve_sqlite_database_path(lane_row.get("database_url")) or root_db_path
            all_bars = _all_table_rows(db_path, "bars", "end_ts")
            bars_by_id = {str(row.get("bar_id")): row for row in all_bars if row.get("bar_id")}
            all_processed_bars = _all_table_rows_safe(db_path, "processed_bars", "end_ts")
            all_signal_rows = _all_table_rows_safe(db_path, "signals", "created_at")
            all_feature_rows = _all_table_rows_safe(db_path, "features", "created_at")
            all_intents = _all_table_rows(db_path, "order_intents", "created_at")
            all_fills = _all_table_rows(db_path, "fills", "fill_timestamp")

            window_processed_bars = _rows_for_session_date(all_processed_bars, session_date, "end_ts")
            window_signal_rows = _rows_for_session_date(all_signal_rows, session_date, "created_at")
            window_feature_rows = _rows_for_session_date(all_feature_rows, session_date, "created_at")
            window_intents = _rows_for_session_date(all_intents, session_date, "created_at")
            window_fills = _rows_for_session_date(all_fills, session_date, "fill_timestamp")

            processed_bar_count = len(all_processed_bars)
            bar_count_in_window = len(window_processed_bars)
            last_processed_bar = _latest_row(all_processed_bars, "end_ts")
            last_processed_bar_id = (str(last_processed_bar.get("bar_id") or "") or None) if last_processed_bar else None
            last_processed_bar_end_ts = _row_timestamp(last_processed_bar, "end_ts")
            if window_processed_bars:
                inspection_start_candidates.append(str(window_processed_bars[0].get("end_ts") or ""))
                inspection_end_candidates.append(str(window_processed_bars[-1].get("end_ts") or ""))

            decoded_all_signal_rows = [
                _decode_signal_audit_row(row, bars_by_id=bars_by_id)
                for row in all_signal_rows
            ]
            decoded_signal_rows = [
                _decode_signal_audit_row(row, bars_by_id=bars_by_id)
                for row in window_signal_rows
            ]
            latest_signal = decoded_all_signal_rows[-1] if decoded_all_signal_rows else None
            last_signal_bar_id = (str(latest_signal.get("bar_id") or "") or None) if latest_signal else None
            last_signal_timestamp = _row_timestamp(latest_signal, "timestamp") if latest_signal else None
            last_signal_family = latest_signal.get("signal_family") if latest_signal else None
            last_actionable_signal = next((row for row in reversed(decoded_signal_rows) if row.get("actionable_entry")), None)
            last_actionable_signal_timestamp = _row_timestamp(last_actionable_signal, "timestamp") if last_actionable_signal else None
            last_actionable_signal_family = last_actionable_signal.get("signal_family") if last_actionable_signal else None
            actionable_entry_signal_count = sum(1 for row in decoded_signal_rows if row.get("actionable_entry"))
            raw_setup_candidate_count = sum(1 for row in decoded_signal_rows if row.get("raw_setup_candidate"))
            last_feature_row = _latest_row(window_feature_rows or all_feature_rows, "created_at")
            last_feature_ts = _row_timestamp(last_feature_row, "created_at")

            latest_intent = _latest_row(window_intents, "created_at")
            latest_fill = _latest_row(window_fills, "fill_timestamp")
            total_intent_count = len(window_intents)
            total_fill_count = len(window_fills)
            latest_intent_timestamp = _row_timestamp(latest_intent, "created_at")
            latest_fill_timestamp = _row_timestamp(latest_fill, "fill_timestamp")

            gating_state = {
                "current_strategy_status": str(lane_row.get("strategy_status") or "UNKNOWN"),
                "entries_enabled": lane_row.get("entries_enabled"),
                "operator_halt": lane_row.get("operator_halt"),
                "same_underlying_entry_hold": lane_row.get("same_underlying_entry_hold"),
                "same_underlying_hold_reason": lane_row.get("same_underlying_hold_reason"),
                "warmup_complete": lane_row.get("warmup_complete"),
                "position_side": str(lane_row.get("position_side") or "FLAT"),
                "open_broker_order_id": _audit_open_broker_order_id(latest_intent, latest_fill, lane_row),
                "latest_fault_or_blocker": _audit_latest_fault_or_blocker(lane_row),
                "eligibility_reason": lane_row.get("eligibility_reason"),
                "eligibility_detail": lane_row.get("eligibility_detail"),
                "risk_state": lane_row.get("risk_state") or "OK",
                "halt_reason": lane_row.get("halt_reason"),
            }
            performance_row = strategy_rows.get(strategy_key)
            surfaced_trade_log_count = trade_log_counts_by_strategy.get(strategy_key, 0)
            verdict, verdict_reason = _signal_intent_fill_audit_verdict(
                processed_bar_count=processed_bar_count,
                bar_count_in_window=bar_count_in_window,
                actionable_entry_signal_count=actionable_entry_signal_count,
                total_intent_count=total_intent_count,
                total_fill_count=total_fill_count,
                gating_state=gating_state,
                strategy_row_exists=performance_row is not None,
                surfaced_trade_log_count=surfaced_trade_log_count,
            )
            verdict_counts[verdict] += 1

            rows.append(
                {
                    "id": strategy_key,
                    "strategy_key": strategy_key,
                    "standalone_strategy_id": strategy_key,
                    "legacy_strategy_key": legacy_strategy_key,
                    "lane_id": lane_id,
                    "strategy_name": strategy_name,
                    "instrument": instrument,
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": identity["strategy_family"],
                    "standalone_strategy_root": identity["standalone_strategy_root"],
                    "standalone_strategy_label": identity["standalone_strategy_label"],
                    "paper_strategy_class": (
                        "temporary_paper_strategy" if temporary_paper_strategy else "approved_or_admitted_paper_strategy"
                    ),
                    "temporary_paper_strategy": temporary_paper_strategy,
                    "experimental_status": lane_row.get("experimental_status"),
                    "paper_only": bool(lane_row.get("paper_only")),
                    "non_approved": bool(lane_row.get("non_approved")),
                    "current_session": lane_row.get("current_detected_session") or current_session,
                    "inspection_start_ts": window_processed_bars[0].get("end_ts") if window_processed_bars else None,
                    "inspection_end_ts": window_processed_bars[-1].get("end_ts") if window_processed_bars else None,
                    "bar_count_in_window": bar_count_in_window,
                    "last_processed_bar_id": last_processed_bar_id,
                    "last_processed_bar_end_ts": last_processed_bar_end_ts,
                    "processed_bar_count": processed_bar_count,
                    "last_feature_ts": last_feature_ts,
                    "last_signal_bar_id": last_signal_bar_id,
                    "last_signal_timestamp": last_signal_timestamp,
                    "last_signal_family": last_signal_family,
                    "last_actionable_signal_timestamp": last_actionable_signal_timestamp,
                    "last_actionable_signal_family": last_actionable_signal_family,
                    "last_long_entry_raw": latest_signal.get("long_entry_raw") if latest_signal else None,
                    "last_short_entry_raw": latest_signal.get("short_entry_raw") if latest_signal else None,
                    "last_long_entry": latest_signal.get("long_entry") if latest_signal else None,
                    "last_short_entry": latest_signal.get("short_entry") if latest_signal else None,
                    "last_recent_long_setup": latest_signal.get("recent_long_setup") if latest_signal else None,
                    "last_recent_short_setup": latest_signal.get("recent_short_setup") if latest_signal else None,
                    "actionable_entry_signal_count": actionable_entry_signal_count,
                    "raw_setup_candidate_count": raw_setup_candidate_count,
                    "last_order_intent_id": latest_intent.get("order_intent_id") if latest_intent else None,
                    "last_intent_timestamp": latest_intent_timestamp,
                    "last_intent_type": latest_intent.get("intent_type") if latest_intent else None,
                    "last_intent_reason_code": latest_intent.get("reason_code") if latest_intent else None,
                    "total_intent_count": total_intent_count,
                    "last_fill_timestamp": latest_fill_timestamp,
                    "last_fill_price": latest_fill.get("fill_price") if latest_fill else None,
                    "last_fill_broker_order_id": latest_fill.get("broker_order_id") if latest_fill else None,
                    "total_fill_count": total_fill_count,
                    "current_strategy_status": gating_state["current_strategy_status"],
                    "eligible_now": lane_row.get("eligible_now"),
                    "auditable_now": db_path is not None and db_path.exists(),
                    "entries_enabled": gating_state["entries_enabled"],
                    "operator_halt": gating_state["operator_halt"],
                    "same_underlying_entry_hold": gating_state["same_underlying_entry_hold"],
                    "same_underlying_hold_reason": gating_state["same_underlying_hold_reason"],
                    "warmup_complete": gating_state["warmup_complete"],
                    "position_side": gating_state["position_side"],
                    "open_broker_order_id": gating_state["open_broker_order_id"],
                    "latest_fault_or_blocker": gating_state["latest_fault_or_blocker"],
                    "audit_verdict": verdict,
                    "audit_reason": verdict_reason,
                    "operator_explanation": _signal_intent_fill_operator_explanation(verdict),
                    "latest_signal_packet_summary": latest_signal,
                    "latest_gating_state": gating_state,
                    "latest_intent_summary": latest_intent,
                    "latest_fill_summary": latest_fill,
                    "strategy_performance_row_exists": performance_row is not None,
                    "performance_row_present": performance_row is not None,
                    "strategy_performance_summary": (
                        {
                            "status": performance_row.get("status"),
                            "trade_count": performance_row.get("trade_count"),
                            "latest_activity_timestamp": performance_row.get("latest_activity_timestamp"),
                            "latest_fill_timestamp": performance_row.get("latest_fill_timestamp"),
                        }
                        if performance_row is not None
                        else None
                    ),
                    "trade_log_rows_exist": surfaced_trade_log_count > 0,
                    "trade_log_present": surfaced_trade_log_count > 0,
                    "trade_log_row_count": surfaced_trade_log_count,
                }
            )

        quant_rows = _quant_signal_intent_fill_audit_rows(
            repo_root=self._repo_root,
            approved_quant_baselines=paper.get("approved_quant_baselines") or {},
            operator_surface=paper.get("operator_surface") or {},
            session_date=session_date,
            current_session=current_session,
            strategy_rows=strategy_rows,
            trade_log_counts_by_strategy=trade_log_counts_by_strategy,
        )
        existing_strategy_ids = {
            str(row.get("standalone_strategy_id") or row.get("strategy_key") or "")
            for row in rows
            if row.get("standalone_strategy_id") or row.get("strategy_key")
        }
        filtered_quant_rows = [
            row for row in quant_rows
            if str(row.get("standalone_strategy_id") or row.get("strategy_key") or "") not in existing_strategy_ids
        ]
        rows.extend(filtered_quant_rows)
        for row in filtered_quant_rows:
            verdict_counts[str(row.get("audit_verdict") or "INSUFFICIENT_HISTORY")] += 1
            if row.get("inspection_start_ts"):
                inspection_start_candidates.append(str(row.get("inspection_start_ts")))
            if row.get("inspection_end_ts"):
                inspection_end_candidates.append(str(row.get("inspection_end_ts")))

        runtime_lookup = {
            str(row.get("standalone_strategy_id") or ""): row
            for row in ((paper.get("runtime_registry") or {}).get("rows") or [])
            if row.get("standalone_strategy_id")
        }
        rows = [_annotate_runtime_identity_state(row, runtime_lookup) for row in rows]
        rows = _annotate_same_underlying_strategy_ambiguity(rows)
        rows.sort(
            key=lambda row: (
                str(row.get("audit_verdict") or ""),
                str(row.get("strategy_name") or ""),
                str(row.get("instrument") or ""),
            )
        )
        return {
            "generated_at": generated_at,
            "inspection_scope": inspection_label,
            "inspection_start_ts": min((value for value in inspection_start_candidates if value), default=None),
            "inspection_end_ts": max((value for value in inspection_end_candidates if value), default=None),
            "bar_count_in_window": sum(int(row.get("bar_count_in_window") or 0) for row in rows),
            "rows": rows,
            "summary": {
                "lane_count": len(rows),
                "verdict_counts": dict(verdict_counts),
            },
            "notes": [
                "Completed-bar only: this audit reads persisted bars, signals, intents, fills, and lane state after finalized-bar processing only.",
                "No current-bar or intrabar logic was added. Replay/paper fills still apply on the next due bar open when due.",
                "This audit is descriptive/operator-facing. It explains whether setups were absent, gated, waiting on fill persistence, or possibly missing from the UI surfacing.",
            ],
            "artifacts": {
                "snapshot": "/api/operator-artifact/paper-signal-intent-fill-audit",
                "strategy_performance": "/api/operator-artifact/paper-strategy-performance",
                "trade_log": "/api/operator-artifact/paper-strategy-trade-log",
                "latest_intents": "/api/operator-artifact/paper-latest-intents",
                "latest_fills": "/api/operator-artifact/paper-latest-fills",
                "operator_status": "/api/operator-artifact/paper-operator-status",
            },
        }

    def _branch_performance_rows(
        self,
        *,
        full_blotter_rows: list[dict[str, Any]],
        allowed_by_source: dict[str, Any],
        blocked_by_source: dict[str, Any],
        intents_by_branch: dict[str, int],
        fills_by_branch: dict[str, int],
        open_branch: str | None,
        unrealized_value: Decimal | None,
    ) -> list[dict[str, Any]]:
        blotter_by_branch: dict[str, list[dict[str, Any]]] = {}
        for row in full_blotter_rows:
            branch = row.get("setup_family") or "UNKNOWN"
            blotter_by_branch.setdefault(branch, []).append(row)

        branch_names = set(blotter_by_branch) | set(allowed_by_source) | set(blocked_by_source) | set(intents_by_branch) | set(fills_by_branch)
        rows: list[dict[str, Any]] = []
        for branch in sorted(branch_names):
            branch_blotter = blotter_by_branch.get(branch, [])
            realized = _sum_decimal_field(branch_blotter, "net_pnl")
            wins, losses, flat_trades = _trade_outcome_counts(branch_blotter)
            closed_trades = len(branch_blotter)
            win_rate = None
            if closed_trades:
                win_rate = (Decimal(wins) / Decimal(closed_trades)) * Decimal("100")
            rows.append(
                {
                    "branch": branch,
                    "signals": int(allowed_by_source.get(branch, 0) or 0),
                    "intents": int(intents_by_branch.get(branch, 0) or 0),
                    "fills": int(fills_by_branch.get(branch, 0) or 0),
                    "blocked": int(blocked_by_source.get(branch, 0) or 0),
                    "closed_trades": closed_trades,
                    "wins": wins,
                    "losses": losses,
                    "flat_trades": flat_trades,
                    "win_rate": f"{win_rate.quantize(Decimal('0.1'))}%" if win_rate is not None else None,
                    "realized_pnl": _decimal_to_string(realized),
                    "unrealized_pnl": _decimal_to_string(unrealized_value) if open_branch == branch else None,
                    "scope": "closed trades" if closed_trades else "signals/intents only",
                }
            )
        rows.sort(key=lambda row: (_sort_decimal_value(row.get("realized_pnl")), row["branch"]), reverse=True)
        return rows

    def _recent_trade_rows(self, full_blotter_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered_rows = sorted(
            full_blotter_rows,
            key=lambda row: row.get("exit_ts") or row.get("entry_ts") or "",
            reverse=True,
        )
        recent: list[dict[str, Any]] = []
        for row in ordered_rows[:12]:
            recent.append(
                {
                    "timestamp": row.get("exit_ts") or row.get("entry_ts"),
                    "instrument": row.get("instrument") or row.get("symbol") or "UNKNOWN",
                    "side": row.get("direction"),
                    "entry_ts": row.get("entry_ts"),
                    "exit_ts": row.get("exit_ts"),
                    "entry_px": row.get("entry_px"),
                    "exit_px": row.get("exit_px"),
                    "realized_pnl": row.get("net_pnl"),
                    "source": row.get("setup_family"),
                    "status": "CLOSED" if row.get("exit_ts") else "OPEN",
                    "exit_reason": row.get("exit_reason"),
                }
            )
        return recent

    def _paper_history_payload(self, artifacts_dir: Path) -> dict[str, Any]:
        session_rows = self._paper_session_history_rows(artifacts_dir, limit=8)
        latest = session_rows[0] if session_rows else None
        prior = session_rows[1] if len(session_rows) > 1 else None
        sessions_with_realized = [row for row in session_rows if row.get("realized_pnl") is not None]
        rolling_sample = sessions_with_realized[1:6]
        rolling_average = _average_decimal_strings([row.get("realized_pnl") for row in rolling_sample])
        recent_average_trades = _average_int_values([row.get("trade_count") for row in session_rows if row.get("trade_count") is not None])
        total_wins = sum(int(row.get("win_count") or 0) for row in session_rows)
        total_closed = sum(int(row.get("trade_count") or 0) for row in session_rows)
        recent_win_rate = None
        if total_closed > 0:
            recent_win_rate = (Decimal(total_wins) / Decimal(total_closed)) * Decimal("100")

        latest_realized = _decimal_or_none(latest.get("realized_pnl")) if latest else None
        prior_realized = _decimal_or_none(prior.get("realized_pnl")) if prior else None
        latest_vs_prior = None
        if latest_realized is not None and prior_realized is not None:
            latest_vs_prior = latest_realized - prior_realized
        latest_vs_average = None
        if latest_realized is not None and rolling_average is not None:
            latest_vs_average = latest_realized - rolling_average
        streak = _session_streak(session_rows)
        distribution = _session_distribution_metrics(session_rows)
        drawdown = _session_drawdown_metrics(session_rows)
        trend = _trend_label(
            latest_realized,
            latest_vs_prior,
            latest_vs_average,
            dispersion=_decimal_or_none(distribution.get("dispersion")),
            sample_size=distribution["sample_size"],
        )
        branch_history = self._aggregate_branch_history(session_rows)

        comparison = {
            "latest_session_date": latest.get("session_date") if latest else None,
            "latest_vs_prior_realized": _decimal_to_string(latest_vs_prior),
            "latest_vs_prior_scope": "Latest completed paper session vs prior completed paper session.",
            "latest_vs_recent_average": _decimal_to_string(latest_vs_average),
            "latest_vs_recent_average_scope": f"Latest completed paper session vs rolling average of {len(rolling_sample)} recent sessions." if rolling_sample else "Insufficient recent sessions.",
            "recent_win_rate": f"{recent_win_rate.quantize(Decimal('0.1'))}%" if recent_win_rate is not None else None,
            "recent_win_rate_scope": f"Derived from {total_closed} closed trades across recent paper blotters." if total_closed else "Unavailable until recent paper blotters contain closed trades.",
            "average_realized_per_session": _decimal_to_string(rolling_average),
            "average_realized_scope": f"Average realized P/L across {len(rolling_sample)} recent paper sessions." if rolling_sample else "Unavailable until recent paper sessions exist.",
            "average_trades_per_session": _decimal_to_string(recent_average_trades) if recent_average_trades is not None else None,
            "average_trades_scope": f"Average closed trades across {len(session_rows)} recent paper sessions." if session_rows else "Unavailable until recent paper sessions exist.",
            "streak": streak,
            "trend": trend,
            "sample_size_note": (
                f"Recent-history sample: {distribution['sample_size']} realized sessions."
                if distribution["sample_size"] > 0
                else "No recent realized-session history."
            ),
        }
        return {
            "history_scope": f"Recent paper sessions ({len(session_rows)} loaded)." if session_rows else "No recent paper sessions loaded.",
            "latest_completed_session": latest.get("session_date") if latest else None,
            "comparison": comparison,
            "distribution": distribution,
            "drawdown": drawdown,
            "recent_sessions": session_rows,
            "branch_history": branch_history,
            "provenance": {
                "sessions": "Derived from persisted paper daily summaries and matching blotters.",
                "comparisons": "Derived from recent completed paper session summaries and blotters only.",
                "distribution": "Derived from recent realized paper sessions only.",
                "drawdown": "Derived from chronological recent realized paper-session outcomes.",
                "branch_history": "Realized P/L and win rate are derived from recent blotters. Signals/intents and blocked counts come from recent paper summaries when available. Stability labels require at least three branch-session observations.",
            },
        }

    def _paper_session_shape_payload(
        self,
        *,
        session_date: str,
        current_session_date: str | None,
        daily_summary: dict[str, Any] | None,
        full_blotter_rows: list[dict[str, Any]],
        session_intents: list[dict[str, Any]],
        session_fills: list[dict[str, Any]],
        position: dict[str, Any],
        operator_status: dict[str, Any],
    ) -> dict[str, Any]:
        points = _build_session_shape_points(
            full_blotter_rows=full_blotter_rows,
            session_intents=session_intents,
            session_fills=session_fills,
            position=position,
            operator_status=operator_status,
        )
        realized_value = _decimal_or_none((daily_summary or {}).get("realized_net_pnl"))
        if realized_value is None:
            realized_value = _sum_decimal_field(full_blotter_rows, "net_pnl")
        latest_value = points[-1]["pnl"] if points else realized_value
        session_start = points[0]["timestamp"] if points else _earliest_timestamp_from_session_artifacts(full_blotter_rows, session_intents, session_fills)
        high_point = max(points, key=lambda point: point["pnl"]) if points else None
        low_point = min(points, key=lambda point: point["pnl"]) if points else None
        first_positive = next((point for point in points if point["pnl"] > 0), None)
        first_negative = next((point for point in points if point["pnl"] < 0), None)
        max_drawdown, max_drawdown_time = _session_path_max_drawdown(points)
        final_flatten_time = _latest_value_from_rows(full_blotter_rows, "exit_ts")
        close_location = _close_location_label(
            latest_value=latest_value,
            high_value=high_point["pnl"] if high_point else None,
            low_value=low_point["pnl"] if low_point else None,
        )
        shape_label = _session_shape_label(points)
        sparkline = _ascii_sparkline([point["pnl"] for point in points])
        return {
            "session_date": session_date,
            "scope": (
                f"Latest paper session {session_date} reconstructed from paper blotter exits plus current open-position estimate."
                if session_date
                else "Latest paper session reconstructed from available artifacts."
            ),
            "session_start": session_start,
            "first_positive_transition": first_positive["timestamp"] if first_positive else None,
            "first_negative_transition": first_negative["timestamp"] if first_negative else None,
            "intraday_high_pnl": _decimal_to_string(high_point["pnl"] if high_point else None),
            "intraday_high_time": high_point["timestamp"] if high_point else None,
            "intraday_low_pnl": _decimal_to_string(low_point["pnl"] if low_point else None),
            "intraday_low_time": low_point["timestamp"] if low_point else None,
            "current_or_latest_pnl": _decimal_to_string(latest_value),
            "end_realized_pnl": _decimal_to_string(realized_value),
            "max_intraday_drawdown": _decimal_to_string(max_drawdown),
            "max_intraday_drawdown_time": max_drawdown_time,
            "close_location": close_location,
            "final_flatten_time": final_flatten_time,
            "shape_label": shape_label,
            "sparkline": sparkline,
            "path_points": [
                {
                    "timestamp": point["timestamp"],
                    "pnl": _decimal_to_string(point["pnl"]),
                    "kind": point["kind"],
                    "label": point["label"],
                }
                for point in points
            ],
            "granularity_note": "Intraday path is reconstructed from closed-trade blotter events and the latest current-position estimate, not bar-by-bar marked equity.",
            "provenance": {
                "path": "Reconstructed from paper blotter entry/exit times, persisted fills/intents, and current paper position state.",
                "timing": "Event times are as precise as persisted paper blotter/fill timestamps.",
                "current": "Current/latest P/L may include derived unrealized P/L from the latest captured bar close.",
            },
            "current_session": bool(current_session_date and current_session_date == session_date),
        }

    def _paper_session_branch_contribution_payload(
        self,
        *,
        session_date: str,
        current_session_date: str | None,
        full_blotter_rows: list[dict[str, Any]],
        session_intents: list[dict[str, Any]],
        session_fills: list[dict[str, Any]],
        position: dict[str, Any],
        operator_status: dict[str, Any],
    ) -> dict[str, Any]:
        session_points = _build_session_shape_points(
            full_blotter_rows=full_blotter_rows,
            session_intents=session_intents,
            session_fills=session_fills,
            position=position,
            operator_status=operator_status,
        )
        session_start = session_points[0]["timestamp"] if session_points else _earliest_timestamp_from_session_artifacts(full_blotter_rows, session_intents, session_fills)
        session_end = session_points[-1]["timestamp"] if session_points else operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at")
        open_branch = _infer_open_branch_source(position, session_intents)
        fills_by_branch = _fills_by_branch(session_intents, session_fills)
        current_unrealized = _decimal_or_none(position.get("unrealized_pnl"))
        contribution_rows = _latest_session_branch_contribution_rows(
            full_blotter_rows=full_blotter_rows,
            session_intents=session_intents,
            session_fills=session_fills,
            open_branch=open_branch,
            current_unrealized=current_unrealized,
            session_start=session_start,
            session_end=session_end,
            latest_event_timestamp=operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at"),
        )
        positive_rows = [row for row in contribution_rows if _decimal_or_none(row.get("total_contribution")) not in {None, Decimal("0")} and _decimal_or_none(row.get("total_contribution")) > 0]
        negative_rows = [row for row in contribution_rows if _decimal_or_none(row.get("total_contribution")) not in {None, Decimal("0")} and _decimal_or_none(row.get("total_contribution")) < 0]
        top_contributor = positive_rows[0] if positive_rows else None
        top_detractor = min(negative_rows, key=lambda row: _decimal_or_none(row.get("total_contribution")) or Decimal("0")) if negative_rows else None
        phase_summary = _branch_phase_summary(contribution_rows)
        return {
            "session_date": session_date,
            "scope": f"Latest paper session {session_date} branch contribution view." if session_date else "Latest paper session branch contribution view.",
            "current_session": bool(current_session_date and current_session_date == session_date),
            "top_contributor": _branch_contribution_card(top_contributor),
            "top_detractor": _branch_contribution_card(top_detractor),
            "phase_summary": phase_summary,
            "rows": contribution_rows,
            "provenance": {
                "realized": "Direct from the latest paper blotter by setup_family.",
                "unrealized": "Only shown when a single current open branch can be inferred from persisted paper intents and current paper position state.",
                "timing": "First/last contribution times are derived from latest-session blotter exit times plus current open-position timestamps when available.",
                "phase": "Early/late/recovery/fade hints are cautious reconstructions from latest-session branch contribution timing, not precise causal decomposition.",
            },
            "granularity_note": "Latest-session branch attribution is event-level from paper blotter exits and current open-position estimate, not bar-by-bar branch-marked equity.",
        }

    def _paper_session_history_rows(self, artifacts_dir: Path, *, limit: int) -> list[dict[str, Any]]:
        daily_dir = artifacts_dir / "daily"
        summary_files = sorted(daily_dir.glob("*.summary.json"), reverse=True)
        rows: list[dict[str, Any]] = []
        for summary_path in summary_files[:limit]:
            summary = _read_json(summary_path)
            session_date = summary.get("session_date") or summary_path.name.split(".")[0]
            blotter_path = daily_dir / f"{session_date}.blotter.csv"
            blotter_rows = _read_csv_rows(blotter_path, limit=None) if blotter_path.exists() else []
            realized_direct = _decimal_or_none(summary.get("realized_net_pnl"))
            realized_derived = _sum_decimal_field(blotter_rows, "net_pnl")
            realized = realized_direct if realized_direct is not None else realized_derived
            flat_at_end = bool(summary.get("flat_at_end", _nested_get(summary, "session_end_assertions", "flat_at_end", default=True)))
            reconciliation_clean = bool(
                summary.get(
                    "reconciliation_clean",
                    _nested_get(summary, "session_end_assertions", "reconciliation_clean", default=True),
                )
            )
            unresolved_open_intents = int(summary.get("unresolved_open_intents", 0) or 0)
            total_pnl = realized if flat_at_end else None
            trade_count = _int_or_none(summary.get("closed_trade_count"))
            if trade_count is None:
                trade_count = len(blotter_rows)
            fill_count = _int_or_none(summary.get("fill_count"))
            wins, losses, flat_trades = _trade_outcome_counts(blotter_rows)
            branch_totals = _aggregate_branch_pnl_decimals(blotter_rows)
            branch_trade_stats = _aggregate_branch_trade_stats(blotter_rows)
            contributors = [
                f"{branch}: {format(pnl, 'f')}"
                for branch, pnl in sorted(branch_totals.items(), key=lambda item: item[1], reverse=True)[:3]
            ]
            rows.append(
                {
                    "session_date": session_date,
                    "summary_path": str(summary_path),
                    "blotter_path": str(blotter_path) if blotter_path.exists() else None,
                    "links": self._summary_links_for_session("paper", session_date),
                    "realized_pnl": _decimal_to_string(realized),
                    "total_pnl": _decimal_to_string(total_pnl),
                    "trade_count": trade_count,
                    "fill_count": fill_count,
                    "win_count": wins if blotter_rows else None,
                    "loss_count": losses if blotter_rows else None,
                    "flat_trade_count": flat_trades if blotter_rows else None,
                    "clean_close": flat_at_end and reconciliation_clean and unresolved_open_intents == 0,
                    "close_state": "CLEAN" if flat_at_end and reconciliation_clean and unresolved_open_intents == 0 else "UNRESOLVED",
                    "not_flat_at_close": not flat_at_end,
                    "reconciliation_dirty": not reconciliation_clean,
                    "unresolved_open_intents": unresolved_open_intents,
                    "major_contributors": contributors,
                    "major_contributors_label": " | ".join(contributors) if contributors else None,
                    "allowed_by_source": dict(summary.get("allowed_branch_decisions_by_source", {}) or {}),
                    "blocked_by_source": dict(summary.get("blocked_branch_decisions_by_source", {}) or {}),
                    "branch_totals": {branch: format(total, "f") for branch, total in branch_totals.items()},
                    "branch_trade_stats": branch_trade_stats,
                    "scope_note": "Total P/L equals realized P/L only when the session ended flat.",
                }
            )
        return rows

    def _aggregate_branch_history(self, session_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregates: dict[str, dict[str, Any]] = {}
        for session in session_rows:
            branch_totals = session.get("branch_totals", {}) or {}
            branch_trade_stats = session.get("branch_trade_stats", {}) or {}
            allowed_by_source = session.get("allowed_by_source", {}) or {}
            blocked_by_source = session.get("blocked_by_source", {}) or {}
            session_branch_names = set(branch_totals) | set(allowed_by_source) | set(blocked_by_source) | set(branch_trade_stats)
            for branch in session_branch_names:
                payload = aggregates.setdefault(
                    branch,
                    {
                        "branch": branch,
                        "sessions_seen": 0,
                        "realized_pnl": Decimal("0"),
                        "closed_trades": 0,
                        "wins": 0,
                        "signals": 0,
                        "blocked": 0,
                        "session_realized_values": [],
                    },
                )
                payload["sessions_seen"] += 1
                branch_realized = _decimal_or_none(branch_totals.get(branch))
                payload["realized_pnl"] += branch_realized or Decimal("0")
                payload["signals"] += int(allowed_by_source.get(branch, 0) or 0)
                payload["blocked"] += int(blocked_by_source.get(branch, 0) or 0)
                branch_metrics = branch_trade_stats.get(branch, {})
                payload["closed_trades"] += int(branch_metrics.get("closed_trades", 0) or 0)
                payload["wins"] += int(branch_metrics.get("wins", 0) or 0)
                payload["session_realized_values"].append(branch_realized or Decimal("0"))

        rows: list[dict[str, Any]] = []
        for branch, payload in sorted(aggregates.items()):
            realized_value = payload["realized_pnl"]
            closed_trades = payload["closed_trades"]
            rows.append(
                {
                    "branch": branch,
                    "sessions_seen": payload["sessions_seen"],
                    "realized_pnl": format(realized_value, "f"),
                    "signals": payload["signals"],
                    "blocked": payload["blocked"],
                    "closed_trades": closed_trades or None,
                    "win_rate": (
                        f"{((Decimal(payload['wins']) / Decimal(closed_trades)) * Decimal('100')).quantize(Decimal('0.1'))}%"
                        if closed_trades > 0
                        else None
                    ),
                    "stability": _branch_stability_label(
                        sessions_seen=payload["sessions_seen"],
                        realized_values=payload["session_realized_values"],
                    ),
                    "scope": "Recent multi-session aggregate",
                }
            )
        rows.sort(key=lambda row: (_sort_decimal_value(row.get("realized_pnl")), row["branch"]), reverse=True)
        return rows

    def _build_operator_state_payload(
        self,
        position: dict[str, Any],
        operator_status: dict[str, Any],
        latest: dict[str, Any],
    ) -> dict[str, Any]:
        flatten_state = latest.get("flatten_state")
        flatten_pending = latest.get("status") == "flatten_pending" or flatten_state == "pending_fill"
        if flatten_state is None:
            if operator_status.get("operator_halt") and position.get("side") == "FLAT":
                flatten_state = "complete"
            else:
                flatten_state = "idle"
        return {
            "entries_enabled": bool(operator_status.get("entries_enabled", False)),
            "operator_halt": bool(operator_status.get("operator_halt", False)),
            "halt_reason": latest.get("halt_reason"),
            "flatten_pending": flatten_pending,
            "flatten_state": flatten_state,
            "flatten_order_intent_id": latest.get("flatten_order_intent_id"),
            "stop_after_cycle_requested": bool(latest.get("stop_after_cycle_requested")) and latest.get("status") != "completed",
            "stop_after_cycle_state": "completed"
            if latest.get("action") == "stop_after_cycle" and latest.get("status") == "completed"
            else ("requested" if bool(latest.get("stop_after_cycle_requested")) else "idle"),
            "last_control_action": latest.get("action"),
            "last_control_status": latest.get("status"),
            "last_control_timestamp": latest.get("completed_at") or latest.get("applied_at") or latest.get("requested_at"),
            "fault_acknowledged": latest.get("action") == "clear_fault" and latest.get("status") == "applied",
            "unresolved_open_order_ids": list(operator_status.get("open_paper_order_ids", []) or []),
        }

    def _paper_risk_state(self, paper: dict[str, Any], review_payload: dict[str, Any]) -> dict[str, Any]:
        summary = review_payload.get("summary") or {}
        assertions = summary.get("session_end_assertions", {})
        reasons: list[str] = []
        if bool(summary) and not bool(summary.get("flat_at_end", assertions.get("flat_at_end", paper["position"]["side"] == "FLAT"))):
            reasons.append("Session ended not flat.")
        if bool(summary) and not bool(
            summary.get("reconciliation_clean", assertions.get("reconciliation_clean", paper["status"]["reconciliation_clean"]))
        ):
            reasons.append("Session reconciliation ended dirty.")
        unresolved_intents = int(summary.get("unresolved_open_intents", 0) or 0)
        if unresolved_intents > 0:
            reasons.append(f"{unresolved_intents} unresolved open intents remain.")
        if paper["status"]["freshness"] == "STALE":
            reasons.append("Paper runtime status is stale.")
        risk_hash = "|".join(
            [
                summary.get("session_date", ""),
                str(summary.get("flat_at_end", assertions.get("flat_at_end", True))),
                str(summary.get("reconciliation_clean", assertions.get("reconciliation_clean", True))),
                str(unresolved_intents),
                paper["status"]["freshness"],
            ]
        )
        ack_payload = _read_json(self._risk_ack_path)
        acknowledged = ack_payload.get("risk_hash") == risk_hash and bool(reasons)
        guidance = (
            "Review unresolved orders, confirm paper blotter, and use Flatten And Halt or Stop After Current Cycle only if the runtime is still active."
            if reasons
            else "No unresolved paper-session risk is currently active."
        )
        return {
            "active": bool(reasons),
            "risk_hash": risk_hash,
            "reasons": reasons,
            "acknowledged": acknowledged,
            "acknowledged_at": ack_payload.get("acknowledged_at") if acknowledged else None,
            "guidance": guidance,
        }

    def _paper_closeout_state(
        self,
        paper: dict[str, Any],
        review_payload: dict[str, Any],
        risk_state: dict[str, Any],
    ) -> dict[str, Any]:
        summary = review_payload.get("summary") or {}
        assertions = summary.get("session_end_assertions", {})
        session_date = summary.get("session_date") or paper["status"]["session_date"] or date.today().isoformat()
        position_flat = paper["position"]["side"] == "FLAT"
        reconciliation_clean = bool(
            summary.get(
                "reconciliation_clean",
                assertions.get("reconciliation_clean", paper["status"]["reconciliation_clean"]),
            )
        )
        unresolved_open_intents = int(
            summary.get(
                "unresolved_open_intents",
                len(paper["operator_state"].get("unresolved_open_order_ids", []) or []),
            )
            or 0
        )
        summary_generated = bool(review_payload.get("available"))
        blotter_generated = bool(paper["latest_blotter_rows"])
        reviewed_payload = _read_json(self._session_signoff_path)
        reviewed = reviewed_payload.get("session_date") == session_date and bool(reviewed_payload.get("reviewed"))
        risk_acknowledged = (not risk_state["active"]) or bool(risk_state["acknowledged"])
        not_flat_at_close = not bool(summary.get("flat_at_end", assertions.get("flat_at_end", position_flat)))

        warnings: list[str] = []
        if paper["running"]:
            warnings.append("Paper runtime is still running.")
        if not position_flat:
            warnings.append("Paper position is not flat.")
        if not reconciliation_clean:
            warnings.append("Paper reconciliation is dirty.")
        if unresolved_open_intents > 0:
            warnings.append(f"{unresolved_open_intents} unresolved paper intents/orders remain.")
        if paper["status"]["fault_state"] == "FAULTED":
            warnings.append("Paper runtime is faulted.")
        if paper["status"]["freshness"] == "STALE":
            warnings.append("Paper runtime state is stale.")
        if not summary_generated:
            warnings.append("Latest paper summary has not been generated.")
        if not blotter_generated:
            warnings.append("Latest paper blotter is missing.")

        checklist = [
            {
                "label": "Halt Entries",
                "status": "done" if paper["status"]["operator_halt"] else "pending",
                "guidance": "Use Halt Entries before closing the paper session if new entries are still enabled.",
            },
            {
                "label": "Stop After Current Cycle",
                "status": "done" if not paper["running"] else "pending_optional",
                "guidance": "Optional. Use Stop After Current Cycle if the runtime is still active and you want a clean paper-only stop.",
            },
            {
                "label": "Verify Flatness",
                "status": "done" if position_flat else "blocked",
                "guidance": "Flat is required for a clean close. If still in position and runtime is active, use Flatten And Halt.",
            },
            {
                "label": "Verify Reconciliation",
                "status": "done" if reconciliation_clean else "blocked",
                "guidance": "Reconciliation must be clean before clean session sign-off.",
            },
            {
                "label": "Generate Paper Summary",
                "status": "done" if summary_generated else "pending",
                "guidance": "Generate the paper daily summary to lock in the latest review bundle and blotter state.",
            },
            {
                "label": "Acknowledge Remaining Risk",
                "status": "done" if risk_acknowledged else "pending",
                "guidance": "Required before sign-off if any unresolved paper session risk remains.",
            },
            {
                "label": "Mark Session Reviewed",
                "status": "done" if reviewed else "pending",
                "guidance": "Sign off only after reviewing flatness, reconciliation, summary, and any acknowledged residual risk.",
            },
        ]

        if not position_flat:
            guidance = "Do not sign off. Flatten the paper position first or keep the session flagged as unresolved."
        elif not reconciliation_clean:
            guidance = "Do not sign off clean. Review reconciliation issues and regenerate the paper summary after resolution."
        elif not summary_generated:
            guidance = "Generate the paper summary before sign-off so the close bundle is complete."
        elif risk_state["active"] and not risk_state["acknowledged"]:
            guidance = "Acknowledge the unresolved paper risk before sign-off, or leave the session explicitly unsigned."
        elif paper["running"]:
            guidance = "Stop the paper runtime before final sign-off to avoid closing review on an active session."
        else:
            guidance = "Session is ready for operator review and sign-off."

        return {
            "session_date": session_date,
            "runtime_running": paper["running"],
            "position_flat": position_flat,
            "position_side": paper["position"]["side"],
            "reconciliation_clean": reconciliation_clean,
            "unresolved_open_intents": unresolved_open_intents,
            "risk_acknowledged": risk_acknowledged,
            "risk_active": risk_state["active"],
            "summary_generated": summary_generated,
            "blotter_generated": blotter_generated,
            "fault_state": paper["status"]["fault_state"],
            "freshness": paper["status"]["freshness"],
            "not_flat_at_close": not_flat_at_close,
            "reviewed": reviewed,
            "reviewed_at": reviewed_payload.get("reviewed_at") if reviewed else None,
            "reviewed_with_active_risk": reviewed_payload.get("reviewed_with_active_risk") if reviewed else False,
            "reviewed_risk_acknowledged": reviewed_payload.get("risk_acknowledged") if reviewed else False,
            "summary_links": review_payload.get("links", {}),
            "warning_reasons": warnings,
            "guidance": guidance,
            "checklist": checklist,
            "sign_off_available": (not paper["running"]) and summary_generated and (risk_acknowledged or not risk_state["active"]),
        }

    def _acknowledge_paper_risk(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        risk = snapshot["paper_risk_state"]
        if not risk["active"]:
            return self._result_record(
                action="acknowledge-paper-risk",
                ok=True,
                command=None,
                output="No unresolved paper-session risk is currently active.",
            )
        ack_payload = {
            "risk_hash": risk["risk_hash"],
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
            "reasons": risk["reasons"],
        }
        self._risk_ack_path.write_text(json.dumps(ack_payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return self._result_record(
            action="acknowledge-paper-risk",
            ok=True,
            command=None,
            output="Unresolved paper-session risk acknowledged for the current review state.",
        )

    def _sign_off_paper_session(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        closeout = snapshot["paper_closeout"]
        risk_state = snapshot["paper_risk_state"]
        if closeout["runtime_running"]:
            return self._result_record(
                action="sign-off-paper-session",
                ok=False,
                command=None,
                output="Paper runtime is still running. Stop the paper session before sign-off.",
            )
        if not closeout["summary_generated"]:
            return self._result_record(
                action="sign-off-paper-session",
                ok=False,
                command=None,
                output="Paper summary has not been generated yet. Generate the paper summary before sign-off.",
            )
        if risk_state["active"] and not risk_state["acknowledged"]:
            return self._result_record(
                action="sign-off-paper-session",
                ok=False,
                command=None,
                output="Unresolved paper session risk must be acknowledged before sign-off.",
            )

        payload = {
            "session_date": closeout["session_date"],
            "reviewed": True,
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
            "not_flat_at_close": closeout["not_flat_at_close"],
            "reconciliation_dirty": not closeout["reconciliation_clean"],
            "unresolved_open_intents": closeout["unresolved_open_intents"],
            "summary_generated": closeout["summary_generated"],
            "blotter_generated": closeout["blotter_generated"],
            "risk_active": closeout["risk_active"],
            "risk_acknowledged": closeout["risk_acknowledged"],
            "reviewed_with_active_risk": closeout["risk_active"],
            "warning_reasons": closeout["warning_reasons"],
        }
        self._session_signoff_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        self._archive_paper_session_lane_history(snapshot=snapshot, signoff_payload=payload)
        return self._result_record(
            action="sign-off-paper-session",
            ok=True,
            command=None,
            output="Paper session review recorded for the current session date.",
        )

    def _archive_paper_session_lane_history(self, *, snapshot: dict[str, Any], signoff_payload: dict[str, Any]) -> dict[str, Any]:
        lane_history = self._paper_session_lane_history_payload(snapshot=snapshot, signoff_payload=signoff_payload)
        archive_timestamp_slug = _safe_archive_timestamp_slug(str(lane_history.get("generated_at") or datetime.now(timezone.utc).isoformat()))
        archive_path = self._paper_session_lane_history_dir / f"{lane_history['session_date']}_{archive_timestamp_slug}.json"
        archive_path.write_text(json.dumps(lane_history, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return lane_history

    def _paper_session_lane_history_payload(
        self,
        *,
        snapshot: dict[str, Any],
        signoff_payload: dict[str, Any],
    ) -> dict[str, Any]:
        paper = snapshot["paper"]
        review_payload = snapshot.get("review", {}).get("paper", {})
        closeout = snapshot["paper_closeout"]
        close_review = snapshot["paper_session_close_review"]
        approved_models = paper.get("approved_models") or {}
        details_by_branch = approved_models.get("details_by_branch") or {}
        review_required_lanes = set(close_review.get("review_required_lanes") or [])
        lanes: list[dict[str, Any]] = []
        active_lane_count = 0
        filled_lane_count = 0
        open_risk_lane_count = 0
        dirty_close_lane_count = 0
        manual_review_lane_count = 0

        for row in close_review.get("rows") or []:
            branch = str(row.get("branch") or "")
            detail = details_by_branch.get(branch) or {}
            signal = int(row.get("signal_count", 0) or 0) > 0
            intent = int(row.get("intent_count", 0) or 0) > 0
            fill = int(row.get("fill_count", 0) or 0) > 0
            blocked = int(row.get("blocked_count", 0) or 0) > 0 or row.get("session_verdict") == "BLOCKED_ONLY"
            active = bool(
                signal
                or intent
                or fill
                or blocked
                or row.get("latest_event_timestamp")
                or row.get("session_verdict") not in {"", "IDLE"}
            )
            open_risk_at_close = bool(row.get("open_position")) or row.get("session_verdict") in {
                "FILLED_WITH_OPEN_RISK",
                "HALTED_BY_RISK",
            }
            halted_by_risk = row.get("session_verdict") == "HALTED_BY_RISK" or str(row.get("risk_state") or "OK") not in {
                "",
                "CLEAR",
                "READY",
                "NONE",
                "OK",
            }
            clean_vs_dirty_close = (
                "DIRTY"
                if row.get("session_verdict") in {
                    "DIRTY_RECONCILIATION",
                    "FILLED_WITH_OPEN_RISK",
                    "HALTED_BY_RISK",
                    "UNKNOWN_INSUFFICIENT_EVIDENCE",
                }
                or close_review.get("desk_close_verdict") in {"DIRTY_CLOSE", "FAULTED_CLOSE"}
                else "CLEAN"
            )
            if active:
                active_lane_count += 1
            if fill:
                filled_lane_count += 1
            if open_risk_at_close:
                open_risk_lane_count += 1
            if clean_vs_dirty_close == "DIRTY":
                dirty_close_lane_count += 1
            if branch in review_required_lanes or row.get("review_confidence") in {"REVIEW_TRUST_LOW", "REVIEW_TRUST_NONE"}:
                manual_review_lane_count += 1
            lanes.append(
                {
                    "session_date": signoff_payload["session_date"],
                    "lane_id": row.get("lane_id"),
                    "instrument": row.get("instrument"),
                    "source_family": row.get("source_family"),
                    "session_pocket": row.get("session_restriction"),
                    "active": active,
                    "blocked": blocked,
                    "signal": signal,
                    "intent": intent,
                    "fill": fill,
                    "open_risk_at_close": open_risk_at_close,
                    "clean_vs_dirty_close": clean_vs_dirty_close,
                    "halted_by_risk": halted_by_risk,
                    "attributable_realized_pnl": row.get("attributable_realized_pnl"),
                    "attribution_coverage_confidence": row.get("attribution_confidence") or "NONE",
                    "primary_gap_reason": (row.get("attribution_gap_reason") or [None])[0],
                    "review_confidence": row.get("review_confidence"),
                    "session_verdict": row.get("session_verdict"),
                    "latest_event_timestamp": row.get("latest_event_timestamp"),
                    "risk_state": row.get("risk_state") or detail.get("risk_state"),
                    "reconciliation_state": detail.get("reconciliation_state"),
                    "manual_review_required": branch in review_required_lanes
                    or row.get("review_confidence") in {"REVIEW_TRUST_LOW", "REVIEW_TRUST_NONE"},
                }
            )

        lanes.sort(key=lambda item: (str(item.get("instrument") or ""), str(item.get("lane_id") or ""), str(item.get("source_family") or "")))
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "session_date": signoff_payload["session_date"],
            "reviewed_at": signoff_payload.get("reviewed_at"),
            "session_close_verdict": close_review.get("desk_close_verdict"),
            "admitted_lane_count": len(lanes),
            "active_lane_count": active_lane_count,
            "filled_lane_count": filled_lane_count,
            "open_risk_lane_count": open_risk_lane_count,
            "dirty_close_lane_count": dirty_close_lane_count,
            "manual_review_lane_count": manual_review_lane_count,
            "source_paths": {
                "session_signoff": str(self._session_signoff_path.resolve()),
                "close_review": str(self._paper_session_close_review_latest_json_path.resolve()),
                "approved_models": str(self._paper_approved_models_path.resolve()),
                "review_state": str(self._review_state_path.resolve()),
                "paper_summary_json": review_payload.get("summary", {}).get("summary_path"),
                "paper_summary_md": None,
                "paper_summary_blotter": paper.get("blotter_path"),
            },
            "desk": {
                "position_flat": closeout.get("position_flat"),
                "position_side": closeout.get("position_side"),
                "reconciliation_clean": closeout.get("reconciliation_clean"),
                "unresolved_open_intents": closeout.get("unresolved_open_intents"),
                "summary_generated": closeout.get("summary_generated"),
                "blotter_generated": closeout.get("blotter_generated"),
                "risk_active": closeout.get("risk_active"),
                "risk_acknowledged": closeout.get("risk_acknowledged"),
                "warning_reasons": closeout.get("warning_reasons") or [],
            },
            "lanes": lanes,
            "notes": [
                "Archived lane history is written only when the paper session is explicitly signed off.",
                "Archived lane history preserves separate standalone strategy identities; no cross-identity attribution is inferred.",
                "No history is backfilled retroactively; this archive starts from newly signed-off paper sessions only.",
            ],
        }

    def _paper_carry_forward_state(self, paper: dict[str, Any], review_payload: dict[str, Any]) -> dict[str, Any]:
        current_session_date = paper["status"]["session_date"] or date.today().isoformat()
        summary = self._prior_paper_summary(paper["artifacts_dir"], current_session_date)
        summary_links = self._summary_links_for_session("paper", summary.get("session_date") if summary else None)
        if not summary:
            persisted = _read_json(self._carry_forward_path)
            if persisted.get("active") or persisted.get("resolved"):
                return persisted
            return self._persist_carry_forward_state(
                {
                    "active": False,
                    "session_date": None,
                    "reasons": [],
                    "guidance": "No prior paper session summary is available to carry forward.",
                    "acknowledged": False,
                    "acknowledged_at": None,
                    "resolution_eligible": False,
                    "resolved": False,
                    "resolved_at": None,
                    "summary_links": {},
                    "reviewed": False,
                    "reviewed_at": None,
                    "summary_generated": False,
                    "blotter_generated": False,
                    "not_flat_at_close": False,
                    "reconciliation_dirty": False,
                    "unresolved_open_intents": 0,
                }
            )

        prior_session_date = summary.get("session_date")
        persisted = _read_json(self._carry_forward_path)
        if not prior_session_date or prior_session_date == current_session_date:
            if persisted.get("session_date") == prior_session_date and persisted.get("resolved_at"):
                return persisted
            return self._persist_carry_forward_state(
                {
                    "active": False,
                    "session_date": prior_session_date,
                    "reasons": [],
                    "guidance": "No unresolved prior paper session is currently being carried forward.",
                    "acknowledged": False,
                    "acknowledged_at": None,
                    "resolution_eligible": False,
                    "resolved": False,
                    "resolved_at": None,
                    "summary_links": summary_links,
                    "reviewed": False,
                    "reviewed_at": None,
                    "summary_generated": True,
                    "blotter_generated": bool(paper["latest_blotter_rows"]),
                    "not_flat_at_close": False,
                    "reconciliation_dirty": False,
                    "unresolved_open_intents": 0,
                }
            )

        signoff = _read_json(self._session_signoff_path)
        signoff_matches = signoff.get("session_date") == prior_session_date
        assertions = summary.get("session_end_assertions", {})
        flat_at_end = bool(summary.get("flat_at_end", assertions.get("flat_at_end", True)))
        reconciliation_clean = bool(summary.get("reconciliation_clean", assertions.get("reconciliation_clean", True)))
        unresolved_open_intents = int(summary.get("unresolved_open_intents", 0) or 0)
        summary_generated = True
        blotter_generated = bool(paper["latest_blotter_rows"])
        reviewed = bool(signoff.get("reviewed")) if signoff_matches else False
        reviewed_at = signoff.get("reviewed_at") if signoff_matches else None
        reviewed_with_active_risk = bool(signoff.get("reviewed_with_active_risk")) if signoff_matches else False
        signoff_not_flat = bool(signoff.get("not_flat_at_close")) if signoff_matches else not flat_at_end
        signoff_recon_dirty = bool(signoff.get("reconciliation_dirty")) if signoff_matches else not reconciliation_clean
        signoff_unresolved = int(signoff.get("unresolved_open_intents", unresolved_open_intents) or 0) if signoff_matches else unresolved_open_intents

        reasons: list[str] = []
        if not reviewed:
            reasons.append("Previous paper session was not reviewed/signed off.")
        if signoff_not_flat:
            reasons.append("Previous paper session ended not flat.")
        if signoff_recon_dirty:
            reasons.append("Previous paper session reconciliation was dirty.")
        if signoff_unresolved > 0:
            reasons.append(f"Previous paper session still shows {signoff_unresolved} unresolved intents/orders.")
        if not summary_generated:
            reasons.append("Previous paper session summary is missing.")
        if not blotter_generated:
            reasons.append("Previous paper session blotter is missing.")
        if reviewed_with_active_risk:
            reasons.append("Previous paper session was reviewed with unresolved risk still active.")

        persisted_matches = persisted.get("session_date") == prior_session_date
        acknowledged = bool(persisted.get("acknowledged_at")) if persisted_matches else False
        acknowledged_at = persisted.get("acknowledged_at") if persisted_matches else None
        resolved = bool(persisted.get("resolved_at")) if persisted_matches else False
        resolved_at = persisted.get("resolved_at") if persisted_matches else None

        if reasons:
            resolved = False
            resolved_at = None
            resolution_eligible = False
            active = True
            guidance = "Review the prior paper summary and blotter, inspect unresolved orders, acknowledge inherited risk, and only resolve after the prior session is truly remediated."
        elif persisted_matches and not resolved:
            resolution_eligible = True
            active = True
            reasons = ["Previous session conditions appear remediated, but inherited risk has not been explicitly cleared."]
            guidance = "The prior session now looks remediated. Explicitly resolve inherited risk to return the desk to operator-clean state."
        else:
            resolution_eligible = False
            active = False
            guidance = "No inherited prior-session risk is currently active."

        payload = {
            "active": active,
            "session_date": prior_session_date,
            "reasons": reasons,
            "guidance": guidance,
            "acknowledged": acknowledged,
            "acknowledged_at": acknowledged_at,
            "resolution_eligible": resolution_eligible,
            "resolved": resolved,
            "resolved_at": resolved_at,
            "summary_links": summary_links,
            "reviewed": reviewed,
            "reviewed_at": reviewed_at,
            "summary_generated": summary_generated,
            "blotter_generated": blotter_generated,
            "not_flat_at_close": signoff_not_flat,
            "reconciliation_dirty": signoff_recon_dirty,
            "unresolved_open_intents": signoff_unresolved,
            "reviewed_with_active_risk": reviewed_with_active_risk,
        }
        return self._persist_carry_forward_state(payload)

    def _summary_links_for_session(self, runtime_name: str, session_date: str | None) -> dict[str, str]:
        if not session_date:
            return {}
        suffix = f"?session_date={session_date}"
        return {
            "json": f"/api/summary/{runtime_name}/json{suffix}",
            "md": f"/api/summary/{runtime_name}/md{suffix}",
            "blotter": f"/api/summary/{runtime_name}/blotter{suffix}",
        }

    def _prior_paper_summary(self, artifacts_dir: str, current_session_date: str) -> dict[str, Any] | None:
        daily_dir = Path(artifacts_dir) / "daily"
        summary_files = sorted(daily_dir.glob("*.summary.json"))
        if not summary_files:
            return None
        latest = _read_json(summary_files[-1])
        latest_session_date = latest.get("session_date")
        target_file = summary_files[-1]
        if latest_session_date == current_session_date and len(summary_files) > 1:
            target_file = summary_files[-2]
        payload = _read_json(target_file)
        payload["summary_path"] = str(target_file)
        return payload

    def _persist_carry_forward_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        _write_json_file(self._carry_forward_path, payload)
        return payload

    def _acknowledge_inherited_risk(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        carry = snapshot["paper_carry_forward"]
        if not carry["active"]:
            return self._result_record(
                action="acknowledge-inherited-risk",
                ok=True,
                command=None,
                output="No inherited prior-session risk is currently active.",
            )
        payload = {
            **carry,
            "acknowledged": True,
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
        }
        self._persist_carry_forward_state(payload)
        return self._result_record(
            action="acknowledge-inherited-risk",
            ok=True,
            command=None,
            output="Inherited prior-session risk acknowledged.",
        )

    def _resolve_inherited_risk(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        carry = snapshot["paper_carry_forward"]
        if not carry["active"]:
            return self._result_record(
                action="resolve-inherited-risk",
                ok=True,
                command=None,
                output="No inherited prior-session risk is currently active.",
            )
        if not carry["resolution_eligible"]:
            return self._result_record(
                action="resolve-inherited-risk",
                ok=False,
                command=None,
                output="Inherited prior-session risk is not yet eligible for resolution. Resolve the underlying prior-session issues first.",
            )
        payload = {
            **carry,
            "active": False,
            "resolved": True,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        self._persist_carry_forward_state(payload)
        return self._result_record(
            action="resolve-inherited-risk",
            ok=True,
            command=None,
            output="Inherited prior-session risk explicitly resolved.",
        )

    def _paper_pre_session_review_state(self, carry_forward: dict[str, Any]) -> dict[str, Any]:
        context_key = f"{carry_forward.get('session_date') or '-'}|{carry_forward.get('resolved_at') or '-'}|{'|'.join(carry_forward.get('reasons', []))}"
        persisted = _read_json(self._pre_session_review_path)
        completed = (
            carry_forward.get("active")
            and persisted.get("context_key") == context_key
            and bool(persisted.get("completed"))
        )
        if carry_forward.get("active"):
            readiness_label = "REVIEW PENDING"
            if completed:
                readiness_label = "GUARDED REVIEWED"
        else:
            readiness_label = "READY FOR RUN"
        payload = {
            "required": bool(carry_forward.get("active")),
            "completed": bool(completed),
            "completed_at": persisted.get("completed_at") if completed else None,
            "context_key": context_key,
            "session_date": carry_forward.get("session_date"),
            "readiness_label": readiness_label,
            "ready_for_run": (not carry_forward.get("active")) or bool(completed),
            "guidance": (
                "Review inherited risk reasons, prior summary/blotter, and guarded startup semantics before treating the desk as ready for a new paper run."
                if carry_forward.get("active")
                else "No guarded startup review is required."
            ),
        }
        _write_json_file(self._pre_session_review_path, payload)
        return payload

    def _complete_pre_session_review(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        carry = snapshot["paper_carry_forward"]
        review = snapshot["paper_pre_session_review"]
        if not carry["active"]:
            return self._result_record(
                action="complete-pre-session-review",
                ok=True,
                command=None,
                output="No guarded startup review is currently required.",
            )
        payload = {
            "context_key": review["context_key"],
            "session_date": carry["session_date"],
            "completed": True,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "carry_forward_reasons": carry["reasons"],
            "summary_links": carry.get("summary_links", {}),
        }
        _write_json_file(self._pre_session_review_path, payload)
        return self._result_record(
            action="complete-pre-session-review",
            ok=True,
            command=None,
            output="Guarded startup pre-session review recorded.",
        )

    def _paper_run_start_state(self) -> dict[str, Any]:
        current = _read_json(self._paper_current_run_path)
        history = _tail_jsonl(self._paper_run_starts_path, 10)
        blocked = _tail_jsonl(self._paper_run_start_blocks_path, 10)
        return {
            "current": current or None,
            "history": history,
            "blocked_history": blocked,
            "artifacts": {
                "starts": str(self._paper_run_starts_path),
                "blocked": str(self._paper_run_start_blocks_path),
                "current": str(self._paper_current_run_path),
            },
            "links": {
                "starts": "/api/operator-artifact/paper-run-starts",
                "blocked": "/api/operator-artifact/paper-run-start-blocks",
                "current": "/api/operator-artifact/paper-current-run-start",
            },
        }

    def _paper_runtime_recovery_state(self) -> dict[str, Any]:
        payload = _read_json(self._paper_runtime_recovery_path)
        return payload if isinstance(payload, dict) else {}

    def _write_paper_runtime_recovery_state(self, payload: dict[str, Any]) -> None:
        _write_json_file(self._paper_runtime_recovery_path, payload)

    def _paper_runtime_supervisor_policy(self) -> dict[str, int]:
        settings = self._dashboard_base_settings()
        return {
            "restart_window_seconds": int(
                getattr(settings, "runtime_supervisor_restart_window_seconds", DEFAULT_RUNTIME_SUPERVISOR_RESTART_WINDOW_SECONDS)
            ),
            "max_auto_restarts_per_window": int(
                getattr(settings, "runtime_supervisor_max_auto_restarts_per_window", DEFAULT_RUNTIME_SUPERVISOR_MAX_AUTO_RESTARTS_PER_WINDOW)
            ),
            "restart_backoff_seconds": int(
                getattr(settings, "runtime_supervisor_restart_backoff_seconds", DEFAULT_RUNTIME_SUPERVISOR_RESTART_BACKOFF_SECONDS)
            ),
            "restart_suppression_seconds": int(
                getattr(settings, "runtime_supervisor_restart_suppression_seconds", DEFAULT_RUNTIME_SUPERVISOR_RESTART_SUPPRESSION_SECONDS)
            ),
            "failure_cooldown_seconds": int(
                getattr(settings, "runtime_supervisor_failure_cooldown_seconds", DEFAULT_RUNTIME_SUPERVISOR_FAILURE_COOLDOWN_SECONDS)
            ),
        }

    def _paper_runtime_supervisor_events(self, limit: int = 12) -> list[dict[str, Any]]:
        return _tail_jsonl(self._paper_runtime_supervisor_events_path, limit)

    def _log_paper_runtime_supervisor_event(
        self,
        *,
        event_type: str,
        supervisor_status: str,
        message: str,
        occurred_at: str,
        payload: dict[str, Any],
    ) -> None:
        record = {
            "event_type": event_type,
            "occurred_at": occurred_at,
            "supervisor_status": supervisor_status,
            "message": message,
        }
        record.update(payload)
        _append_jsonl(self._paper_runtime_supervisor_events_path, record)

    def _paper_runtime_supervisor_attempt_history(
        self,
        current_state: dict[str, Any],
        *,
        now: datetime,
        retention_seconds: int,
    ) -> list[dict[str, Any]]:
        retained: list[dict[str, Any]] = []
        for row in list(current_state.get("restart_attempt_history") or []):
            if not isinstance(row, dict):
                continue
            attempted_at = _parse_iso_datetime(row.get("attempted_at"))
            if attempted_at is None:
                continue
            if (now - attempted_at.astimezone(timezone.utc)) <= timedelta(seconds=retention_seconds):
                retained.append(dict(row))
        retained.sort(key=lambda row: str(row.get("attempted_at") or ""))
        return retained[-50:]

    def _paper_runtime_supervisor_fields(
        self,
        *,
        status: str,
        reason_code: str | None,
        reason: str,
        detail: str,
        operator_message: str,
        next_action: str,
        manual_action_required: bool,
        auto_restart_eligible: bool,
        auto_restart_allowed: bool,
        attempted_at: str | None,
        succeeded_at: str | None,
        failed_at: str | None,
        output: str | None,
        last_runtime_stop_detected_at: str | None,
        last_restart_result: str | None,
        restart_attempt_history: list[dict[str, Any]],
        restart_suppressed_until: str | None,
        restart_backoff_until: str | None,
        policy: dict[str, int],
        recent_events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        restart_window_seconds = int(policy["restart_window_seconds"])
        window_cutoff = now - timedelta(seconds=restart_window_seconds)
        attempts_in_window = [
            row
            for row in restart_attempt_history
            if (_parse_iso_datetime(row.get("attempted_at")) or now) >= window_cutoff
        ]
        suppression_until_dt = _parse_iso_datetime(restart_suppressed_until)
        backoff_until_dt = _parse_iso_datetime(restart_backoff_until)
        suppression_active = bool(
            suppression_until_dt is not None and suppression_until_dt.astimezone(timezone.utc) > now
        )
        backoff_active = bool(backoff_until_dt is not None and backoff_until_dt.astimezone(timezone.utc) > now)
        attempts_used = len(attempts_in_window)
        attempts_max = int(policy["max_auto_restarts_per_window"])
        attempts_remaining = max(attempts_max - attempts_used, 0)
        return {
            "status": status,
            "supervisor_status": status,
            "reason_code": reason_code,
            "reason": reason,
            "detail": detail,
            "operator_message": operator_message,
            "next_action": next_action,
            "manual_action_required": manual_action_required,
            "auto_restart_eligible": auto_restart_eligible,
            "auto_restart_allowed": auto_restart_allowed,
            "attempted_at": attempted_at,
            "succeeded_at": succeeded_at,
            "failed_at": failed_at,
            "output": output,
            "last_runtime_stop_detected_at": last_runtime_stop_detected_at,
            "last_restart_attempt_at": attempted_at,
            "last_restart_result": last_restart_result,
            "restart_attempt_history": restart_attempt_history,
            "restart_attempts_in_window": attempts_used,
            "restart_attempts_remaining_in_window": attempts_remaining,
            "max_auto_restarts_per_window": attempts_max,
            "restart_window_seconds": restart_window_seconds,
            "restart_backoff_seconds": int(policy["restart_backoff_seconds"]),
            "restart_suppression_seconds": int(policy["restart_suppression_seconds"]),
            "failure_cooldown_seconds": int(policy["failure_cooldown_seconds"]),
            "restart_backoff_until": restart_backoff_until,
            "restart_backoff_active": backoff_active,
            "restart_suppressed_until": restart_suppressed_until,
            "restart_suppressed": suppression_active,
            "supervisor_reason": reason,
            "supervisor_reason_code": reason_code,
            "supervisor_manual_action_required": manual_action_required,
            "supervisor_updated_at": now.isoformat(),
            "recent_events": recent_events,
        }

    def _paper_runtime_recovery_snapshot_context(
        self,
        *,
        paper: dict[str, Any],
        carry_forward: dict[str, Any],
        pre_session_review: dict[str, Any],
        closeout_state: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "global": {
                "current_session_date": paper.get("status", {}).get("session_date"),
            },
            "paper": paper,
            "paper_carry_forward": carry_forward,
            "paper_pre_session_review": pre_session_review,
            "paper_closeout": closeout_state,
        }

    def _paper_runtime_recovery_manual_payload(
        self,
        *,
        status: str,
        reason_code: str,
        reason: str,
        next_action: str,
        detail: str,
        current_state: dict[str, Any],
        policy: dict[str, int],
        attempted_at: str | None = None,
        output: str | None = None,
    ) -> dict[str, Any]:
        payload = self._paper_runtime_supervisor_fields(
            status=status,
            reason_code=reason_code,
            reason=reason,
            detail=detail,
            operator_message=f"Paper runtime stopped; manual intervention required because {reason.lower()}",
            next_action=next_action,
            manual_action_required=True,
            auto_restart_eligible=False,
            auto_restart_allowed=False,
            attempted_at=attempted_at or current_state.get("attempted_at"),
            succeeded_at=current_state.get("succeeded_at"),
            failed_at=attempted_at if status in {"AUTO_RESTART_FAILED", "AUTO_RESTART_SUPPRESSED"} else current_state.get("failed_at"),
            output=output or current_state.get("output"),
            last_runtime_stop_detected_at=current_state.get("last_runtime_stop_detected_at"),
            last_restart_result=current_state.get("last_restart_result"),
            restart_attempt_history=list(current_state.get("restart_attempt_history") or []),
            restart_suppressed_until=current_state.get("restart_suppressed_until"),
            restart_backoff_until=current_state.get("restart_backoff_until"),
            policy=policy,
            recent_events=self._paper_runtime_supervisor_events(),
        )
        self._write_paper_runtime_recovery_state(payload)
        return payload

    def _paper_runtime_recovery_payload(
        self,
        *,
        paper: dict[str, Any],
        auth_status: dict[str, Any],
        carry_forward: dict[str, Any],
        pre_session_review: dict[str, Any],
        closeout_state: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
        current_state = self._paper_runtime_recovery_state()
        policy = self._paper_runtime_supervisor_policy()
        now = datetime.now(timezone.utc)
        runtime_phase = str((paper.get("readiness") or {}).get("runtime_phase") or "STOPPED").upper()
        entry_eligibility = paper.get("entry_eligibility") or {}
        operator_state = paper.get("operator_state") or {}
        auth_ready = bool(auth_status.get("runtime_ready"))
        current_status = str(current_state.get("status") or "").upper()
        attempted_at = _parse_iso_datetime(current_state.get("attempted_at"))
        retention_seconds = max(
            int(policy["restart_window_seconds"]),
            int(policy["restart_suppression_seconds"]),
            int(policy["failure_cooldown_seconds"]),
            int(policy["restart_backoff_seconds"]),
            PAPER_RUNTIME_AUTO_RECOVERY_SUCCESS_WINDOW_SECONDS,
        ) * 3
        restart_attempt_history = self._paper_runtime_supervisor_attempt_history(
            current_state,
            now=now,
            retention_seconds=retention_seconds,
        )
        restart_window_cutoff = now - timedelta(seconds=int(policy["restart_window_seconds"]))
        restart_attempts_in_window = [
            row
            for row in restart_attempt_history
            if (_parse_iso_datetime(row.get("attempted_at")) or now) >= restart_window_cutoff
        ]
        recent_attempt = (
            attempted_at is not None
            and (now - attempted_at.astimezone(timezone.utc)) < timedelta(seconds=int(policy["restart_backoff_seconds"]))
        )
        recent_success = (
            current_status == "AUTO_RESTART_SUCCEEDED"
            and attempted_at is not None
            and (now - attempted_at.astimezone(timezone.utc)) < timedelta(seconds=PAPER_RUNTIME_AUTO_RECOVERY_SUCCESS_WINDOW_SECONDS)
        )
        restart_suppressed_until = current_state.get("restart_suppressed_until")
        restart_suppressed_until_dt = _parse_iso_datetime(restart_suppressed_until)
        restart_suppressed = bool(
            restart_suppressed_until_dt is not None and restart_suppressed_until_dt.astimezone(timezone.utc) > now
        )
        restart_backoff_until = current_state.get("restart_backoff_until")
        restart_backoff_until_dt = _parse_iso_datetime(restart_backoff_until)
        restart_backoff_active = bool(
            restart_backoff_until_dt is not None and restart_backoff_until_dt.astimezone(timezone.utc) > now
        )
        last_runtime_stop_detected_at = current_state.get("last_runtime_stop_detected_at")
        last_restart_result = str(current_state.get("last_restart_result") or "").upper() or None
        base_state = dict(current_state)
        base_state["restart_attempt_history"] = restart_attempt_history
        base_state["restart_suppressed_until"] = restart_suppressed_until
        base_state["restart_backoff_until"] = restart_backoff_until
        base_state["last_runtime_stop_detected_at"] = last_runtime_stop_detected_at
        base_state["last_restart_result"] = last_restart_result

        if paper.get("running"):
            payload = self._paper_runtime_supervisor_fields(
                status="AUTO_RESTART_SUCCEEDED" if recent_success else "RUNNING",
                reason_code=None,
                reason="Paper runtime is active.",
                detail="Paper runtime stopped; auto-restart succeeded." if recent_success else "Paper runtime is active.",
                operator_message="Paper runtime stopped; auto-restart succeeded." if recent_success else "Paper runtime is active.",
                next_action="No action needed",
                manual_action_required=False,
                auto_restart_eligible=False,
                auto_restart_allowed=False,
                attempted_at=current_state.get("attempted_at"),
                succeeded_at=current_state.get("succeeded_at") or current_state.get("attempted_at"),
                failed_at=None,
                output=current_state.get("output"),
                last_runtime_stop_detected_at=None,
                last_restart_result="SUCCEEDED" if recent_success else last_restart_result,
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=None,
                restart_backoff_until=None,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, None, None

        if runtime_phase not in {"STOPPED", "STOPPING"}:
            payload = self._paper_runtime_supervisor_fields(
                status="NOT_APPLICABLE",
                reason_code=None,
                reason="Paper runtime is not in a stopped state.",
                detail="Auto-restart is only considered when the runtime is stopped.",
                operator_message="Paper runtime is not in a stopped state.",
                next_action="No action needed",
                manual_action_required=False,
                auto_restart_eligible=False,
                auto_restart_allowed=False,
                attempted_at=current_state.get("attempted_at"),
                succeeded_at=current_state.get("succeeded_at"),
                failed_at=current_state.get("failed_at"),
                output=current_state.get("output"),
                last_runtime_stop_detected_at=last_runtime_stop_detected_at,
                last_restart_result=last_restart_result,
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=restart_suppressed_until,
                restart_backoff_until=restart_backoff_until,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, None, None

        if not last_runtime_stop_detected_at:
            last_runtime_stop_detected_at = now.isoformat()
            self._log_paper_runtime_supervisor_event(
                event_type="runtime_stop_detected",
                supervisor_status="STOPPED",
                message="Paper runtime stop detected; supervisor is evaluating whether auto-restart is safe.",
                occurred_at=last_runtime_stop_detected_at,
                payload={
                    "reason_code": "RUNTIME_STOPPED",
                    "restart_attempts_in_window": len(restart_attempts_in_window),
                },
            )
            base_state["last_runtime_stop_detected_at"] = last_runtime_stop_detected_at

        if restart_suppressed:
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="AUTO_RESTART_SUPPRESSED",
                    reason_code="RESTART_BUDGET_EXHAUSTED",
                    reason="automatic restart is suppressed after repeated runtime stop/restart churn",
                    next_action="Start Runtime",
                    detail=(
                        "Paper runtime stopped; manual intervention required because the automatic restart budget was exhausted "
                        f"and auto-restart is suppressed until {restart_suppressed_until}."
                    ),
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        if not auth_ready:
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="STOPPED_MANUAL_REQUIRED",
                    reason_code="AUTH_NOT_READY",
                    reason="broker/auth readiness is not green",
                    next_action="Auth Gate Check",
                    detail="Paper runtime stopped; manual intervention required because broker/auth readiness is not green yet.",
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        if operator_state.get("last_control_action") in {"flatten_and_halt", "stop_after_cycle", "stop-paper"} or operator_state.get("halt_reason"):
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="STOPPED_MANUAL_REQUIRED",
                    reason_code="OPERATOR_STOP",
                    reason="the runtime was stopped intentionally by operator control",
                    next_action="Start Runtime",
                    detail="Paper runtime stopped after an explicit operator stop or halt, so it will not auto-restart.",
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        primary_reason = str(entry_eligibility.get("primary_blocking_reason") or "").upper()
        if primary_reason != "RUNTIME_STOPPED":
            next_action = str(entry_eligibility.get("clear_action") or "Manual inspection required")
            reason = str(entry_eligibility.get("state_note") or "another safety blocker is still active").strip()
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="STOPPED_MANUAL_REQUIRED",
                    reason_code=primary_reason or "UNSAFE_TO_RESTART",
                    reason=reason,
                    next_action=next_action,
                    detail=f"Paper runtime stopped; manual intervention required because {reason}",
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        if current_status == "AUTO_RESTART_IN_PROGRESS" and recent_attempt:
            payload = self._paper_runtime_supervisor_fields(
                status="AUTO_RESTART_IN_PROGRESS",
                reason_code="AUTO_RESTART_IN_PROGRESS",
                reason="safe runtime auto-restart is already in progress",
                detail="Paper runtime stopped; auto-restart in progress.",
                operator_message="Paper runtime stopped; auto-restart in progress.",
                next_action="Wait for the next readiness refresh",
                manual_action_required=False,
                auto_restart_eligible=True,
                auto_restart_allowed=False,
                attempted_at=current_state.get("attempted_at"),
                succeeded_at=current_state.get("succeeded_at"),
                failed_at=current_state.get("failed_at"),
                output=current_state.get("output"),
                last_runtime_stop_detected_at=last_runtime_stop_detected_at,
                last_restart_result=last_restart_result or "ATTEMPTED",
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=restart_suppressed_until,
                restart_backoff_until=restart_backoff_until,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, None, None

        if restart_backoff_active:
            payload = self._paper_runtime_supervisor_fields(
                status="AUTO_RESTART_BACKOFF",
                reason_code="AUTO_RESTART_BACKOFF",
                reason="the supervisor is waiting for restart backoff before the next automatic attempt",
                detail="Paper runtime stopped; auto-restart backoff is active.",
                operator_message="Paper runtime stopped; auto-restart backoff is active.",
                next_action="Wait for the next readiness refresh",
                manual_action_required=False,
                auto_restart_eligible=True,
                auto_restart_allowed=False,
                attempted_at=current_state.get("attempted_at"),
                succeeded_at=current_state.get("succeeded_at"),
                failed_at=current_state.get("failed_at"),
                output=current_state.get("output"),
                last_runtime_stop_detected_at=last_runtime_stop_detected_at,
                last_restart_result=last_restart_result,
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=restart_suppressed_until,
                restart_backoff_until=restart_backoff_until,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, None, None

        if len(restart_attempts_in_window) >= int(policy["max_auto_restarts_per_window"]):
            suppressed_until = (now + timedelta(seconds=int(policy["restart_suppression_seconds"]))).isoformat()
            base_state["restart_suppressed_until"] = suppressed_until
            base_state["last_restart_result"] = "SUPPRESSED"
            if current_status != "AUTO_RESTART_SUPPRESSED":
                self._log_paper_runtime_supervisor_event(
                    event_type="restart_suppressed",
                    supervisor_status="AUTO_RESTART_SUPPRESSED",
                    message="Automatic restart has been suppressed because the rolling restart budget was exhausted.",
                    occurred_at=now.isoformat(),
                    payload={
                        "reason_code": "RESTART_BUDGET_EXHAUSTED",
                        "restart_attempts_in_window": len(restart_attempts_in_window),
                        "max_auto_restarts_per_window": int(policy["max_auto_restarts_per_window"]),
                        "restart_suppressed_until": suppressed_until,
                    },
                )
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="AUTO_RESTART_SUPPRESSED",
                    reason_code="RESTART_BUDGET_EXHAUSTED",
                    reason="automatic restart is suppressed after repeated runtime stop/restart churn",
                    next_action="Start Runtime",
                    detail=(
                        "Paper runtime stopped; manual intervention required because the automatic restart budget was exhausted "
                        f"and auto-restart is suppressed until {suppressed_until}."
                    ),
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        command, metadata = self._paper_start_command_with_enabled_temp_paper({"paper": paper})
        if command is None:
            unresolved = ", ".join(metadata.get("unresolved_lane_ids") or []) or "none"
            return (
                self._paper_runtime_recovery_manual_payload(
                    status="STOPPED_MANUAL_REQUIRED",
                    reason_code="TEMP_PAPER_STARTUP_MAPPING_MISSING",
                    reason="enabled temporary paper lanes are missing a startup mapping",
                    next_action="Restart Runtime + Temp Paper",
                    detail=f"Paper runtime stopped; manual intervention required because enabled temporary paper lanes are missing a startup mapping ({unresolved}).",
                    current_state=base_state,
                    policy=policy,
                ),
                None,
                None,
            )

        start_context = self._paper_runtime_recovery_snapshot_context(
            paper=paper,
            carry_forward=carry_forward,
            pre_session_review=pre_session_review,
            closeout_state=closeout_state,
        )
        attempted_at_iso = now.isoformat()
        self._log_paper_runtime_supervisor_event(
            event_type="restart_attempted",
            supervisor_status="AUTO_RESTART_IN_PROGRESS",
            message="Paper runtime stopped; supervisor is attempting an automatic restart.",
            occurred_at=attempted_at_iso,
            payload={
                "reason_code": "AUTO_RESTART_ATTEMPTED",
                "restart_attempts_in_window": len(restart_attempts_in_window) + 1,
                "max_auto_restarts_per_window": int(policy["max_auto_restarts_per_window"]),
                "command": " ".join(command),
            },
        )
        in_progress_payload = self._paper_runtime_supervisor_fields(
            status="AUTO_RESTART_IN_PROGRESS",
            reason_code="AUTO_RESTART_IN_PROGRESS",
            reason="safe runtime auto-restart is in progress",
            detail="Paper runtime stopped; auto-restart in progress.",
            operator_message="Paper runtime stopped; auto-restart in progress.",
            next_action="Wait for the next readiness refresh",
            manual_action_required=False,
            auto_restart_eligible=True,
            auto_restart_allowed=False,
            attempted_at=attempted_at_iso,
            succeeded_at=current_state.get("succeeded_at"),
            failed_at=None,
            output=None,
            last_runtime_stop_detected_at=last_runtime_stop_detected_at,
            last_restart_result="ATTEMPTED",
            restart_attempt_history=restart_attempt_history,
            restart_suppressed_until=None,
            restart_backoff_until=None,
            policy=policy,
            recent_events=self._paper_runtime_supervisor_events(),
        )
        self._write_paper_runtime_recovery_state(in_progress_payload)

        completed = subprocess.run(
            command,
            cwd=self._repo_root,
            text=True,
            capture_output=True,
            check=False,
        )
        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()
        output = stdout or stderr or "Command completed without output."
        result = self._result_record(
            action=PAPER_RUNTIME_AUTO_RECOVERY_ACTION,
            ok=completed.returncode == 0,
            command=command,
            output=output,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
        )
        attempt_record = {
            "attempted_at": attempted_at_iso,
            "result": "SUCCEEDED" if completed.returncode == 0 else "FAILED",
            "returncode": completed.returncode,
            "command": " ".join(command),
            "reason_code": "AUTO_RESTART_COMMAND_SUCCEEDED" if completed.returncode == 0 else "AUTO_RESTART_COMMAND_FAILED",
            "output": output[:400],
        }
        restart_attempt_history = [*restart_attempt_history, attempt_record][-50:]
        attempts_after_attempt = [
            row
            for row in restart_attempt_history
            if (_parse_iso_datetime(row.get("attempted_at")) or now) >= restart_window_cutoff
        ]

        if completed.returncode != 0:
            backoff_until = (
                now
                + timedelta(
                    seconds=max(
                        int(policy["restart_backoff_seconds"]),
                        int(policy["failure_cooldown_seconds"]),
                    )
                )
            ).isoformat()
            suppressed_until = (
                (now + timedelta(seconds=int(policy["restart_suppression_seconds"]))).isoformat()
                if len(attempts_after_attempt) >= int(policy["max_auto_restarts_per_window"])
                else None
            )
            self._log_paper_runtime_supervisor_event(
                event_type="restart_failed",
                supervisor_status="AUTO_RESTART_FAILED",
                message="Automatic paper-runtime restart failed.",
                occurred_at=attempted_at_iso,
                payload={
                    "reason_code": "AUTO_RESTART_COMMAND_FAILED",
                    "output": output[:400],
                    "restart_attempts_in_window": len(attempts_after_attempt),
                    "max_auto_restarts_per_window": int(policy["max_auto_restarts_per_window"]),
                    "restart_backoff_until": backoff_until,
                },
            )
            if suppressed_until is not None:
                self._log_paper_runtime_supervisor_event(
                    event_type="restart_suppressed",
                    supervisor_status="AUTO_RESTART_SUPPRESSED",
                    message="Automatic restart has been suppressed because repeated restart failures exhausted the rolling budget.",
                    occurred_at=attempted_at_iso,
                    payload={
                        "reason_code": "RESTART_BUDGET_EXHAUSTED",
                        "restart_attempts_in_window": len(attempts_after_attempt),
                        "max_auto_restarts_per_window": int(policy["max_auto_restarts_per_window"]),
                        "restart_suppressed_until": suppressed_until,
                    },
                )
                payload = self._paper_runtime_recovery_manual_payload(
                    status="AUTO_RESTART_SUPPRESSED",
                    reason_code="RESTART_BUDGET_EXHAUSTED",
                    reason="automatic restart is suppressed after repeated runtime stop/restart churn",
                    next_action="Start Runtime",
                    detail=(
                        "Paper runtime stopped; manual intervention required because repeated auto-restart failures exhausted "
                        f"the rolling restart budget. Auto-restart is suppressed until {suppressed_until}."
                    ),
                    current_state={
                        **base_state,
                        "restart_attempt_history": restart_attempt_history,
                        "restart_suppressed_until": suppressed_until,
                        "restart_backoff_until": backoff_until,
                        "last_runtime_stop_detected_at": last_runtime_stop_detected_at,
                        "last_restart_result": "FAILED",
                    },
                    policy=policy,
                    attempted_at=attempted_at_iso,
                    output=output,
                )
                return payload, None, result
            payload = self._paper_runtime_supervisor_fields(
                status="AUTO_RESTART_BACKOFF",
                reason_code="AUTO_RESTART_COMMAND_FAILED",
                reason="the last automatic restart attempt failed and the supervisor will retry after backoff",
                detail="Paper runtime stopped; last auto-restart failed. Supervisor retry is delayed by backoff.",
                operator_message="Paper runtime stopped; last auto-restart failed. Supervisor will retry after backoff.",
                next_action="Wait for the next readiness refresh",
                manual_action_required=False,
                auto_restart_eligible=True,
                auto_restart_allowed=False,
                attempted_at=attempted_at_iso,
                succeeded_at=current_state.get("succeeded_at"),
                failed_at=attempted_at_iso,
                output=output,
                last_runtime_stop_detected_at=last_runtime_stop_detected_at,
                last_restart_result="FAILED",
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=None,
                restart_backoff_until=backoff_until,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, None, result

        refreshed_paper = self._runtime_snapshot("paper")
        if refreshed_paper.get("running"):
            self._log_paper_runtime_supervisor_event(
                event_type="restart_succeeded",
                supervisor_status="AUTO_RESTART_SUCCEEDED",
                message="Automatic paper-runtime restart succeeded.",
                occurred_at=attempted_at_iso,
                payload={
                    "reason_code": "AUTO_RESTART_SUCCEEDED",
                    "restart_attempts_in_window": len(attempts_after_attempt),
                    "max_auto_restarts_per_window": int(policy["max_auto_restarts_per_window"]),
                },
            )
            payload = self._paper_runtime_supervisor_fields(
                status="AUTO_RESTART_SUCCEEDED",
                reason_code=None,
                reason="paper runtime auto-restart succeeded",
                detail="Paper runtime stopped; auto-restart succeeded.",
                operator_message="Paper runtime stopped; auto-restart succeeded.",
                next_action="No action needed",
                manual_action_required=False,
                auto_restart_eligible=False,
                auto_restart_allowed=False,
                attempted_at=attempted_at_iso,
                succeeded_at=attempted_at_iso,
                failed_at=None,
                output=output,
                last_runtime_stop_detected_at=None,
                last_restart_result="SUCCEEDED",
                restart_attempt_history=restart_attempt_history,
                restart_suppressed_until=None,
                restart_backoff_until=None,
                policy=policy,
                recent_events=self._paper_runtime_supervisor_events(),
            )
            self._write_paper_runtime_recovery_state(payload)
            return payload, refreshed_paper, result

        backoff_until = (now + timedelta(seconds=int(policy["restart_backoff_seconds"]))).isoformat()
        payload = self._paper_runtime_supervisor_fields(
            status="AUTO_RESTART_IN_PROGRESS",
            reason_code="AUTO_RESTART_IN_PROGRESS",
            reason="the runtime start command returned successfully but the process is still coming up",
            detail="Paper runtime stopped; auto-restart in progress.",
            operator_message="Paper runtime stopped; auto-restart in progress.",
            next_action="Wait for the next readiness refresh",
            manual_action_required=False,
            auto_restart_eligible=True,
            auto_restart_allowed=False,
            attempted_at=attempted_at_iso,
            succeeded_at=None,
            failed_at=None,
            output=output,
            last_runtime_stop_detected_at=last_runtime_stop_detected_at,
            last_restart_result="ATTEMPTED",
            restart_attempt_history=restart_attempt_history,
            restart_suppressed_until=None,
            restart_backoff_until=backoff_until,
            policy=policy,
            recent_events=self._paper_runtime_supervisor_events(),
        )
        self._write_paper_runtime_recovery_state(payload)
        return payload, refreshed_paper, result

    def _paper_session_continuity(
        self,
        paper: dict[str, Any],
        review_payload: dict[str, Any],
        carry_forward: dict[str, Any],
        pre_session_review: dict[str, Any],
        paper_run_start: dict[str, Any],
    ) -> dict[str, Any]:
        prior_summary = self._prior_paper_summary(
            paper["artifacts_dir"],
            paper["status"]["session_date"] or date.today().isoformat(),
        )
        signoff = _read_json(self._session_signoff_path)
        entries: list[dict[str, Any]] = []

        if prior_summary:
            entries.append(
                {
                    "kind": "prior_close",
                    "title": "Prior Session Close",
                    "status": "UNRESOLVED" if carry_forward["active"] else "CLEAN",
                    "timestamp": prior_summary.get("session_date"),
                    "details": [
                        f"Session date: {prior_summary.get('session_date', '-')}",
                        f"Flat at end: {prior_summary.get('flat_at_end', 'unknown')}",
                        f"Reconciliation clean: {prior_summary.get('reconciliation_clean', 'unknown')}",
                        f"Unresolved intents: {prior_summary.get('unresolved_open_intents', 0)}",
                    ],
                    "links": carry_forward.get("summary_links", {}),
                }
            )
        else:
            entries.append(
                {
                    "kind": "prior_close",
                    "title": "Prior Session Close",
                    "status": "MISSING",
                    "timestamp": None,
                    "details": ["No prior paper summary is available."],
                    "links": {},
                }
            )

        entries.append(
            {
                "kind": "signoff",
                "title": "Prior Session Sign-Off",
                "status": "REVIEWED" if signoff.get("reviewed") else "UNREVIEWED",
                "timestamp": signoff.get("reviewed_at"),
                "details": [
                    f"Reviewed: {bool(signoff.get('reviewed'))}",
                    f"Reviewed with active risk: {bool(signoff.get('reviewed_with_active_risk'))}",
                    f"Not flat at close: {signoff.get('not_flat_at_close', 'unknown')}",
                    f"Reconciliation dirty: {signoff.get('reconciliation_dirty', 'unknown')}",
                ],
                "links": {},
            }
        )

        entries.append(
            {
                "kind": "carry_forward",
                "title": "Inherited Risk Carry-Forward",
                "status": "ACTIVE" if carry_forward["active"] else "CLEAR",
                "timestamp": carry_forward.get("acknowledged_at") or carry_forward.get("resolved_at"),
                "details": carry_forward.get("reasons") or [carry_forward.get("guidance", "No carry-forward guidance available.")],
                "links": {
                    "carry_forward": "/api/operator-artifact/paper-carry-forward",
                },
            }
        )

        entries.append(
            {
                "kind": "pre_session_review",
                "title": "Pre-Session Review",
                "status": "COMPLETED" if pre_session_review["completed"] else "PENDING" if pre_session_review["required"] else "NOT REQUIRED",
                "timestamp": pre_session_review.get("completed_at"),
                "details": [
                    f"Required: {pre_session_review.get('required')}",
                    f"Completed: {pre_session_review.get('completed')}",
                    pre_session_review.get("guidance", "-"),
                ],
                "links": {
                    "pre_session_review": "/api/operator-artifact/paper-pre-session-review",
                },
            }
        )

        for blocked in (paper_run_start.get("blocked_history") or [])[-3:]:
            entries.append(
                {
                    "kind": "blocked_start",
                    "title": "Blocked Paper Start",
                    "status": "BLOCKED",
                    "timestamp": blocked.get("timestamp"),
                    "details": [
                        f"Desk state: {blocked.get('desk_state_at_attempt', '-')}",
                        f"Reason: {blocked.get('blocked_reason', '-')}",
                    ],
                    "links": {
                        "blocked_starts": "/api/operator-artifact/paper-run-start-blocks",
                    },
                }
            )

        current_run = paper_run_start.get("current")
        if current_run:
            entries.append(
                {
                    "kind": "run_start",
                    "title": "Successful Paper Run Start",
                    "status": current_run.get("desk_state_at_start", "UNKNOWN"),
                    "timestamp": current_run.get("timestamp"),
                    "details": [
                        f"Desk state at start: {current_run.get('desk_state_at_start', '-')}",
                        f"Guarded review completed: {current_run.get('pre_session_review_completed', False)}",
                        f"Reconciliation at start: {current_run.get('reconciliation_state_at_start', '-')}",
                    ],
                    "links": {
                        "current_run": "/api/operator-artifact/paper-current-run-start",
                    },
                }
            )
        else:
            entries.append(
                {
                    "kind": "run_start",
                    "title": "Successful Paper Run Start",
                    "status": "MISSING",
                    "timestamp": None,
                    "details": ["No successful dashboard-path paper run start has been recorded yet."],
                    "links": {
                        "run_starts": "/api/operator-artifact/paper-run-starts",
                    },
                }
            )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "entries": entries,
            "links": {
                "prior_summary_json": carry_forward.get("summary_links", {}).get("json"),
                "prior_summary_md": carry_forward.get("summary_links", {}).get("md"),
                "prior_summary_blotter": carry_forward.get("summary_links", {}).get("blotter"),
                "carry_forward": "/api/operator-artifact/paper-carry-forward",
                "pre_session_review": "/api/operator-artifact/paper-pre-session-review",
                "blocked_starts": "/api/operator-artifact/paper-run-start-blocks",
                "current_run": "/api/operator-artifact/paper-current-run-start",
                "continuity": "/api/operator-artifact/paper-session-continuity",
            },
        }

    def _paper_session_event_timeline_payload(
        self,
        *,
        paper: dict[str, Any],
        review_payload: dict[str, Any],
        risk_state: dict[str, Any],
        carry_forward: dict[str, Any],
        pre_session_review: dict[str, Any],
    ) -> dict[str, Any]:
        session_shape = paper.get("session_shape") or {}
        branch_contribution = paper.get("branch_session_contribution") or {}
        session_date = session_shape.get("session_date") or paper["status"]["session_date"] or date.today().isoformat()
        events: list[dict[str, Any]] = []

        def add_event(
            *,
            timestamp: str | None,
            category: str,
            title: str,
            details: list[str],
            provenance: str,
            badge: str,
            links: dict[str, str] | None = None,
        ) -> None:
            if not timestamp:
                return
            if not _timestamp_matches_session(timestamp, session_date):
                return
            events.append(
                {
                    "timestamp": timestamp,
                    "category": category,
                    "title": title,
                    "details": details,
                    "provenance": provenance,
                    "badge": badge,
                    "links": links or {},
                }
            )

        add_event(
            timestamp=session_shape.get("session_start"),
            category="shape",
            title="Session Start",
            details=[f"Latest paper session {session_date} started from reconstructed artifact timing."],
            provenance="RECONSTRUCTED",
            badge="SESSION",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("first_positive_transition"),
            category="shape",
            title="First Positive Transition",
            details=[f"P/L first moved positive. Latest/current P&L path now {session_shape.get('current_or_latest_pnl') or '-'}."],
            provenance="RECONSTRUCTED",
            badge="SHAPE",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("first_negative_transition"),
            category="shape",
            title="First Negative Transition",
            details=[f"P/L first moved negative. Session path label: {session_shape.get('shape_label') or '-'}."],
            provenance="RECONSTRUCTED",
            badge="SHAPE",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("intraday_high_time"),
            category="shape",
            title="Intraday High-Water",
            details=[
                f"Intraday high P/L: {session_shape.get('intraday_high_pnl') or '-'}",
                f"Close location: {session_shape.get('close_location') or '-'}",
            ],
            provenance="RECONSTRUCTED",
            badge="HIGH",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("intraday_low_time"),
            category="shape",
            title="Intraday Low-Water",
            details=[f"Intraday low P/L: {session_shape.get('intraday_low_pnl') or '-'}"],
            provenance="RECONSTRUCTED",
            badge="LOW",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("max_intraday_drawdown_time"),
            category="shape",
            title="Max Intraday Drawdown",
            details=[f"Max drawdown: {session_shape.get('max_intraday_drawdown') or '-'}"],
            provenance="RECONSTRUCTED",
            badge="DD",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )
        add_event(
            timestamp=session_shape.get("final_flatten_time") or paper["status"].get("last_processed_bar_end_ts") or paper["status"].get("last_update_ts"),
            category="shape",
            title="Latest Session End State",
            details=[
                f"Current/latest P&L: {session_shape.get('current_or_latest_pnl') or '-'}",
                f"End realized P&L: {session_shape.get('end_realized_pnl') or '-'}",
                f"Position side: {paper['position'].get('side') or '-'}",
            ],
            provenance="RECONSTRUCTED",
            badge="END",
            links={"shape": "/api/operator-artifact/paper-session-shape"},
        )

        branch_rows = {row.get("branch"): row for row in branch_contribution.get("rows", []) if row.get("branch")}
        for phase_key, title in [
            ("early_run_up", "Branch Early Run-Up"),
            ("early_drawdown", "Branch Early Drawdown"),
            ("late_recovery", "Branch Late Recovery"),
            ("late_fade", "Branch Late Fade"),
        ]:
            card = (branch_contribution.get("phase_summary") or {}).get(phase_key)
            if not card or not card.get("branch"):
                continue
            branch_row = branch_rows.get(card["branch"], {})
            phase_timestamp = branch_row.get("first_meaningful_time")
            if phase_key in {"late_recovery", "late_fade"}:
                phase_timestamp = branch_row.get("last_meaningful_time") or phase_timestamp
            add_event(
                timestamp=phase_timestamp,
                category="branch",
                title=title,
                details=[
                    f"{card.get('branch')} • {card.get('total_contribution') or '-'}",
                    f"{card.get('timing_hint') or '-'} • {card.get('path_hint') or '-'}",
                ],
                provenance="RECONSTRUCTED",
                badge="BRANCH",
                links={"branch": "/api/operator-artifact/paper-session-branch-contribution"},
            )

        for label, card in [
            ("Top Contributor Active", branch_contribution.get("top_contributor")),
            ("Top Detractor Active", branch_contribution.get("top_detractor")),
        ]:
            if not card or not card.get("branch"):
                continue
            branch_row = branch_rows.get(card["branch"], {})
            add_event(
                timestamp=branch_row.get("first_meaningful_time"),
                category="branch",
                title=label,
                details=[
                    f"{card.get('branch')} • {card.get('total_contribution') or '-'}",
                    f"{card.get('net_effect') or '-'} • {card.get('timing_hint') or '-'}",
                ],
                provenance="RECONSTRUCTED",
                badge="BRANCH",
                links={"branch": "/api/operator-artifact/paper-session-branch-contribution"},
            )
            if branch_row.get("last_meaningful_time") and branch_row.get("last_meaningful_time") != branch_row.get("first_meaningful_time"):
                add_event(
                    timestamp=branch_row.get("last_meaningful_time"),
                    category="branch",
                    title=f"{label} Window End",
                    details=[
                        f"{card.get('branch')} latest meaningful contribution.",
                        f"Path hint: {card.get('path_hint') or '-'}",
                    ],
                    provenance="RECONSTRUCTED",
                    badge="BRANCH",
                    links={"branch": "/api/operator-artifact/paper-session-branch-contribution"},
                )

        for control in paper["events"].get("operator_controls", []):
            add_event(
                timestamp=control.get("requested_at") or control.get("applied_at"),
                category="operator",
                title=_operator_control_title(control.get("action")),
                details=[
                    f"Status: {control.get('status') or '-'}",
                    f"Message: {control.get('message') or '-'}",
                ],
                provenance="DIRECT",
                badge="CONTROL",
            )

        if pre_session_review.get("completed"):
            add_event(
                timestamp=pre_session_review.get("completed_at"),
                category="risk",
                title="Pre-Session Review Completed",
                details=[
                    f"Guarded startup review completed for session {session_date}.",
                    pre_session_review.get("guidance") or "-",
                ],
                provenance="DIRECT",
                badge="REVIEW",
                links={"review": "/api/operator-artifact/paper-pre-session-review"},
            )

        if risk_state.get("acknowledged_at"):
            add_event(
                timestamp=risk_state.get("acknowledged_at"),
                category="risk",
                title="Paper Risk Acknowledged",
                details=risk_state.get("reasons") or ["Current unresolved paper-session risk acknowledged."],
                provenance="DIRECT",
                badge="RISK",
            )

        if carry_forward.get("acknowledged_at"):
            add_event(
                timestamp=carry_forward.get("acknowledged_at"),
                category="risk",
                title="Inherited Risk Acknowledged",
                details=carry_forward.get("reasons") or ["Inherited prior-session risk acknowledged."],
                provenance="DIRECT",
                badge="RISK",
                links={"carry": "/api/operator-artifact/paper-carry-forward"},
            )

        if carry_forward.get("resolved_at"):
            add_event(
                timestamp=carry_forward.get("resolved_at"),
                category="risk",
                title="Inherited Risk Resolved",
                details=["Inherited prior-session risk explicitly cleared."],
                provenance="DIRECT",
                badge="RISK",
                links={"carry": "/api/operator-artifact/paper-carry-forward"},
            )

        for recon in paper["events"].get("reconciliation", []):
            issues = recon.get("issues") or []
            clean = bool(recon.get("clean"))
            if clean and not issues:
                continue
            add_event(
                timestamp=recon.get("logged_at"),
                category="runtime",
                title="Reconciliation Warning",
                details=[
                    f"Clean: {clean}",
                    f"Issues: {' | '.join(str(issue) for issue in issues) if issues else '-'}",
                ],
                provenance="DIRECT",
                badge="RECON",
            )

        for alert in paper["events"].get("alerts", []):
            severity = str(alert.get("severity") or "").upper()
            if severity not in {"WARNING", "ERROR", "CRITICAL", "ACTION", "BLOCKING", "RECOVERY"}:
                continue
            add_event(
                timestamp=alert.get("occurred_at") or alert.get("logged_at"),
                category="runtime",
                title=f"Alert: {alert.get('code') or severity}",
                details=[alert.get("message") or "-", f"Severity: {severity}"],
                provenance="DIRECT",
                badge="ALERT",
            )

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for event in sorted(events, key=lambda item: (_parse_iso_datetime(item["timestamp"]) or datetime.max.replace(tzinfo=timezone.utc), item["title"])):
            key = (event["timestamp"], event["category"], event["title"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(event)

        return {
            "session_date": session_date,
            "scope": f"Latest paper session {session_date} chronological event timeline.",
            "events": deduped,
            "provenance": {
                "shape": "Session-shape markers are reconstructed from latest-session paper blotter exits plus current open-position estimate.",
                "branch": "Branch markers are reconstructed from latest-session branch contribution timing, not bar-by-bar branch-marked equity.",
                "operator": "Operator control, alert, and reconciliation timestamps are direct from persisted runtime/dashboard artifacts.",
            },
            "granularity_note": "This is a latest-session event timeline using persisted event timestamps and reconstructed session markers; it is not a tick timeline.",
        }

    def _record_paper_start_success(
        self,
        pre_snapshot: dict[str, Any],
        post_snapshot: dict[str, Any],
        result: dict[str, Any],
    ) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        pre_global = pre_snapshot["global"]
        pre_carry = pre_snapshot["paper_carry_forward"]
        pre_review = pre_snapshot["paper_pre_session_review"]
        pre_paper = pre_snapshot["paper"]
        pre_closeout = pre_snapshot["paper_closeout"]
        record = {
            "timestamp": timestamp,
            "run_start_id": f"paper-run-{timestamp}",
            "session_date": post_snapshot["global"]["current_session_date"] or pre_global["current_session_date"],
            "desk_state_at_start": "GUARDED" if pre_carry["active"] else "CLEAN",
            "inherited_risk_active": pre_carry["active"],
            "pre_session_review_required": pre_review["required"],
            "pre_session_review_completed": pre_review["completed"],
            "inherited_risk_acknowledged": pre_carry["acknowledged"],
            "runtime_health_at_start": pre_paper["status"]["health_status"],
            "runtime_fault_state_at_start": pre_paper["status"]["fault_state"],
            "reconciliation_state_at_start": pre_paper["status"]["reconciliation_semantics"],
            "runtime_freshness_at_start": pre_paper["status"]["freshness"],
            "unresolved_orders_at_start": len(pre_paper["operator_state"]["unresolved_open_order_ids"]),
            "unresolved_closeout_intents_at_start": pre_closeout["unresolved_open_intents"],
            "operator_action": result["action"],
            "operator_action_label": result["action_label"],
            "started_after_guarded_review": pre_carry["active"] and pre_review["completed"],
            "start_command": result.get("command"),
            "result_kind": result.get("kind"),
        }
        _append_jsonl(self._paper_run_starts_path, record)
        _write_json_file(self._paper_current_run_path, record)

    def _record_paper_start_block(self, snapshot: dict[str, Any], result: dict[str, Any]) -> None:
        global_state = snapshot["global"]
        carry = snapshot["paper_carry_forward"]
        review = snapshot["paper_pre_session_review"]
        paper = snapshot["paper"]
        closeout = snapshot["paper_closeout"]
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_date": global_state["current_session_date"],
            "desk_state_at_attempt": "GUARDED" if carry["active"] else "CLEAN",
            "inherited_risk_active": carry["active"],
            "pre_session_review_required": review["required"],
            "pre_session_review_completed": review["completed"],
            "inherited_risk_acknowledged": carry["acknowledged"],
            "runtime_health_at_attempt": paper["status"]["health_status"],
            "runtime_fault_state_at_attempt": paper["status"]["fault_state"],
            "reconciliation_state_at_attempt": paper["status"]["reconciliation_semantics"],
            "runtime_freshness_at_attempt": paper["status"]["freshness"],
            "unresolved_orders_at_attempt": len(paper["operator_state"]["unresolved_open_order_ids"]),
            "unresolved_closeout_intents_at_attempt": closeout["unresolved_open_intents"],
            "operator_action": result["action"],
            "operator_action_label": result["action_label"],
            "blocked_reason": result.get("output") or result.get("message"),
            "result_kind": result.get("kind"),
        }
        _append_jsonl(self._paper_run_start_blocks_path, record)

    def _load_or_refresh_auth_gate_result(self, run_if_missing: bool) -> dict[str, Any]:
        if self._auth_cache_path.exists():
            payload = _read_json(self._auth_cache_path)
            payload.setdefault("source", "cached_auth_gate")
            return payload
        bootstrap_status = self._bootstrap_auth_status()
        if bootstrap_status is not None:
            bootstrap_status.setdefault("source", "bootstrap_status")
            return bootstrap_status
        if not run_if_missing:
            return {"runtime_ready": False, "source": "missing"}
        completed = subprocess.run(
            ["bash", "scripts/run_schwab_auth_gate.sh"],
            cwd=self._repo_root,
            text=True,
            capture_output=True,
            check=False,
        )
        parsed = _parse_json_output(completed.stdout or completed.stderr)
        if parsed is None:
            return {
                "runtime_ready": False,
                "source": "auth_gate_error",
                "error": (completed.stdout or completed.stderr).strip(),
            }
        _write_json_file(self._auth_cache_path, parsed)
        parsed.setdefault("source", "fresh_auth_gate")
        return parsed

    def _bootstrap_auth_status(self) -> dict[str, Any] | None:
        token_path = Path(os.environ.get("SCHWAB_TOKEN_FILE", self._repo_root / ".local" / "schwab" / "tokens.json"))
        status_path = token_path.resolve().parent / "bootstrap_artifacts" / "latest_status.json"
        if not status_path.exists():
            return None
        return _read_json(status_path)

    def _runtime_paths(self, runtime_name: str) -> dict[str, Path]:
        if runtime_name == "paper":
            return {
                "artifacts_dir": self._repo_root / "outputs" / "probationary_pattern_engine" / "paper_session",
                "pid_file": self._repo_root
                / "outputs"
                / "probationary_pattern_engine"
                / "paper_session"
                / "runtime"
                / "probationary_paper.pid",
                "log_file": self._repo_root
                / "outputs"
                / "probationary_pattern_engine"
                / "paper_session"
                / "runtime"
                / "probationary_paper.log",
                "db_path": _resolve_sqlite_database_path(
                    load_settings_from_files(
                        [
                            self._repo_root / "config/base.yaml",
                            self._repo_root / "config/live.yaml",
                            self._repo_root / "config/probationary_pattern_engine.yaml",
                            self._repo_root / "config/probationary_pattern_engine_paper.yaml",
                        ]
                    ).database_url
                ),
            }
        return {
            "artifacts_dir": self._repo_root / "outputs" / "probationary_pattern_engine",
            "pid_file": self._repo_root
            / "outputs"
            / "probationary_pattern_engine"
            / "runtime"
            / "probationary_shadow.pid",
            "log_file": self._repo_root
            / "outputs"
            / "probationary_pattern_engine"
            / "runtime"
            / "probationary_shadow.log",
            "db_path": _resolve_sqlite_database_path(
                load_settings_from_files(
                    [
                        self._repo_root / "config/base.yaml",
                        self._repo_root / "config/live.yaml",
                        self._repo_root / "config/probationary_pattern_engine.yaml",
                    ]
                ).database_url
            ),
        }

    def _latest_daily_summary(self, artifacts_dir: Path) -> dict[str, Any] | None:
        daily_dir = artifacts_dir / "daily"
        summary_files = sorted(daily_dir.glob("*.summary.json"))
        if not summary_files:
            return None
        payload = _read_json(summary_files[-1])
        payload["summary_path"] = str(summary_files[-1])
        return payload

    def _latest_blotter_dataset(self, artifacts_dir: Path) -> tuple[Path | None, list[dict[str, Any]]]:
        daily_dir = artifacts_dir / "daily"
        blotter_files = sorted(daily_dir.glob("*.blotter.csv"))
        if not blotter_files:
            return None, []
        latest_path = blotter_files[-1]
        return latest_path, _read_csv_rows(latest_path, limit=None)

    def _latest_blotter_rows(self, artifacts_dir: Path) -> list[dict[str, Any]]:
        _, rows = self._latest_blotter_dataset(artifacts_dir)
        return rows[-20:]

    def _prechecked_action_result(self, action: str) -> dict[str, Any] | None:
        paper = self._runtime_snapshot("paper")
        shadow = self._runtime_snapshot("shadow")
        carry_forward = self._paper_carry_forward_state(paper, self._review_payload(paper, "paper"))
        pre_session_review = self._paper_pre_session_review_state(carry_forward)
        paper_faulted = paper["status"]["fault_state"] == "FAULTED"
        paper_halted = paper["status"]["operator_halt"]
        desk_risk_state = str((paper.get("desk_risk") or {}).get("desk_risk_state") or "OK")
        lane_risk_active = any(
            str(row.get("risk_state") or "OK").startswith("HALTED")
            for row in ((paper.get("lane_risk") or {}).get("lanes") or [])
        )

        if action == "start-shadow" and shadow["running"]:
            return self._result_record(action=action, ok=True, command=None, output="Shadow is already running.")
        if action == "start-paper" and paper["running"]:
            return self._result_record(action=action, ok=True, command=None, output="Paper soak is already running.")
        if action == "start-paper" and not pre_session_review["ready_for_run"]:
            return self._result_record(
                action=action,
                ok=False,
                command=None,
                output="Inherited prior-session risk is active and pre-session review is still pending. Complete the review before starting paper soak.",
            )
        if action == "stop-shadow" and not shadow["running"]:
            return self._result_record(action=action, ok=True, command=None, output="Shadow is not running.")
        if action == "stop-paper" and not paper["running"]:
            return self._result_record(action=action, ok=True, command=None, output="Paper soak is not running.")
        if action == "paper-halt-entries" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-resume-entries" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-force-reconcile" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-clear-fault" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-clear-risk-halts" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-force-lane-resume-session-override" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-halt-entries" and paper_halted:
            return self._result_record(action=action, ok=True, command=None, output="Paper entries are already halted.")
        if action == "paper-resume-entries" and not paper_halted:
            return self._result_record(action=action, ok=True, command=None, output="Paper entries are already enabled.")
        if action == "paper-clear-fault" and not paper_faulted:
            return self._result_record(action=action, ok=True, command=None, output="Paper runtime is not faulted.")
        if action == "paper-clear-risk-halts" and desk_risk_state not in {"HALT_NEW_ENTRIES", "FLATTEN_AND_HALT"} and not lane_risk_active:
            return self._result_record(action=action, ok=True, command=None, output="No paper risk halts are currently active.")
        if action == "paper-force-lane-resume-session-override":
            target_lane_id = str(payload.get("lane_id") or "").strip()
            lane_rows = {
                str(row.get("lane_id") or ""): row
                for row in ((paper.get("lane_risk") or {}).get("lanes") or [])
                if row.get("lane_id")
            }
            lane_row = lane_rows.get(target_lane_id)
            if not target_lane_id:
                return self._result_record(action=action, ok=False, command=None, output="No lane id was supplied for the session override.")
            if lane_row is None:
                return self._result_record(action=action, ok=False, command=None, output=f"Lane {target_lane_id} is not active in the current paper runtime.")
            if str(lane_row.get("risk_state") or "") != "HALTED_DEGRADATION" or str(lane_row.get("halt_reason") or "") != REALIZED_LOSER_SESSION_OVERRIDE_REASON:
                return self._result_record(
                    action=action,
                    ok=False,
                    command=None,
                    output="The selected lane is not currently halted by lane_realized_loser_limit_per_session.",
                )
            if bool(lane_row.get("session_override_active")):
                return self._result_record(action=action, ok=True, command=None, output="A same-session override is already active for this lane.")
        if action == "paper-flatten-and-halt" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-flatten-and-halt" and paper["operator_state"]["flatten_pending"]:
            return self._result_record(action=action, ok=True, command=None, output="Flatten And Halt is already pending.")
        if action == "paper-stop-after-cycle" and not paper["running"]:
            return self._result_record(action=action, ok=False, command=None, output="Paper runtime is not running.")
        if action == "paper-stop-after-cycle" and paper["operator_state"]["stop_after_cycle_requested"]:
            return self._result_record(action=action, ok=True, command=None, output="Stop After Current Cycle is already requested.")
        return None

    def _result_record(
        self,
        *,
        action: str,
        ok: bool,
        command: list[str] | None,
        output: str,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> dict[str, Any]:
        return {
            "ok": ok,
            "action": action,
            "action_label": _humanize_action(action),
            "kind": _normalize_action_kind(action, ok, output),
            "command": " ".join(command) if command else None,
            "returncode": returncode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": _normalize_action_message(action, ok, output),
            "stdout_snippet": stdout[:400],
            "stderr_snippet": stderr[:400],
            "output": output[:1200],
        }

    def _queue_paper_lane_session_override(self, payload: dict[str, Any]) -> dict[str, Any]:
        lane_id = str(payload.get("lane_id") or "").strip()
        lane_name = str(payload.get("lane_name") or lane_id or "Unknown lane")
        symbol = str(payload.get("symbol") or "").strip().upper() or "UNKNOWN"
        halt_reason = str(payload.get("halt_reason") or "").strip() or REALIZED_LOSER_SESSION_OVERRIDE_REASON
        local_operator_identity = str(payload.get("local_operator_identity") or "").strip()
        if not lane_id:
            return self._result_record(
                action="paper-force-lane-resume-session-override",
                ok=False,
                command=None,
                output="Force Lane Resume rejected because no lane id was supplied.",
            )
        if not local_operator_identity:
            return self._result_record(
                action="paper-force-lane-resume-session-override",
                ok=False,
                command=None,
                output="Force Lane Resume rejected because no authenticated local operator identity was supplied.",
            )
        config_paths = self._paper_operator_control_config_paths()
        queued = submit_probationary_operator_control(
            config_paths,
            REALIZED_LOSER_SESSION_OVERRIDE_ACTION,
            payload={
                "lane_id": lane_id,
                "lane_name": lane_name,
                "symbol": symbol,
                "halt_reason": halt_reason,
                "local_operator_identity": local_operator_identity,
                "operator_label": payload.get("operator_label") or local_operator_identity,
                "override_note": payload.get("override_note"),
                "session_override_confirmed": bool(payload.get("session_override_confirmed")),
                "session_override_scope": "current_session_only",
                "session_override": True,
                "audit_event_type": "lane_force_resume_session_override",
            },
        )
        result = self._result_record(
            action="paper-force-lane-resume-session-override",
            ok=True,
            command=None,
            output=f"Queued session override for {lane_name} ({symbol}). The lane-local realized-loser gate will be bypassed for the current session only if the runtime accepts the override.",
        )
        result["requested_at"] = queued.requested_at
        result["control_path"] = queued.control_path
        result["lane_id"] = lane_id
        result["lane_name"] = lane_name
        result["symbol"] = symbol
        result["halt_reason"] = halt_reason
        result["local_operator_identity"] = local_operator_identity
        result["session_override"] = True
        result["session_override_scope"] = "current_session_only"
        result["audit_event_type"] = "lane_force_resume_session_override"
        return result

    def _log_action(self, payload: dict[str, Any]) -> None:
        with self._action_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")

    def _market_index_debug_payload(self, market_context: dict[str, Any]) -> dict[str, Any]:
        diagnostics = market_context.get("diagnostics", {}) if isinstance(market_context, dict) else {}
        symbol_rows = diagnostics.get("symbols", []) if isinstance(diagnostics, dict) else []
        return {
            "build_stamp": self._build_stamp,
            "version_label": f"dashboard-{self._build_stamp[:10]}",
            "server_pid": self._server_info.pid if self._server_info else os.getpid(),
            "server_started_at": self._server_info.started_at if self._server_info else None,
            "server_url": self._server_info.url if self._server_info else None,
            "server_host": self._server_info.host if self._server_info else None,
            "server_port": self._server_info.port if self._server_info else None,
            "snapshot_file_path": str(self._market_index_strip_path.resolve()),
            "diagnostics_file_path": str(self._market_index_diagnostics_path.resolve()),
            "snapshot_updated_at": market_context.get("updated_at"),
            "snapshot_artifact": market_context.get("snapshot_artifact"),
            "diagnostic_artifact": market_context.get("diagnostic_artifact"),
            "symbols": [
                {
                    "label": row.get("label"),
                    "requested_symbol": row.get("requested_symbol"),
                    "matched_symbol": row.get("matched_symbol"),
                    "render_classification": row.get("render_classification"),
                    "current_present": bool(_nested_get(row, "field_states", "current_value", "available", default=False)),
                    "change_present": bool(_nested_get(row, "field_states", "absolute_change", "available", default=False)),
                    "percent_change_present": bool(_nested_get(row, "field_states", "percent_change", "available", default=False)),
                    "bid_present": bool(_nested_get(row, "field_states", "bid", "available", default=False)),
                    "ask_present": bool(_nested_get(row, "field_states", "ask", "available", default=False)),
                }
                for row in symbol_rows
            ],
        }


def dashboard_build_stamp() -> str:
    digest = hashlib.sha1()
    for path in (
        Path(__file__),
        ASSET_DIR / "operator_dashboard.html",
        ASSET_DIR / "operator_dashboard.css",
        ASSET_DIR / "operator_dashboard.js",
    ):
        digest.update(str(path).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def run_operator_dashboard_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8790,
    info_file: str | None = None,
    allow_port_fallback: bool = False,
) -> DashboardServerInfo:
    started_at = datetime.now(timezone.utc).isoformat()
    build_stamp = dashboard_build_stamp()
    service = OperatorDashboardService(
        REPO_ROOT,
        server_info=DashboardServerInfo(
            host=host,
            port=port,
            url=f"http://{host}:{port}/",
            pid=os.getpid(),
            started_at=started_at,
            build_stamp=build_stamp,
            info_file=info_file,
        ),
    )
    handler = _build_handler(service)
    httpd, chosen_port = _bind_dashboard_server(host, port, handler, allow_port_fallback=allow_port_fallback)
    url = f"http://{host}:{chosen_port}/"
    info = DashboardServerInfo(
        host=host,
        port=chosen_port,
        url=url,
        pid=os.getpid(),
        started_at=started_at,
        build_stamp=build_stamp,
        info_file=info_file,
    )
    service._server_info = info
    if info_file is not None:
        info_path = Path(info_file)
        info_path.parent.mkdir(parents=True, exist_ok=True)
        info_path.write_text(
            json.dumps(
                {
                    "host": host,
                    "port": chosen_port,
                    "url": url,
                    "pid": info.pid,
                    "started_at": info.started_at,
                    "build_stamp": info.build_stamp,
                    "version_label": f"dashboard-{info.build_stamp[:10]}",
                    "health_url": f"{url}health",
                    "dashboard_api_url": f"{url}api/dashboard",
                    "allow_port_fallback": allow_port_fallback,
                },
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    threading.Thread(target=service.prime_dashboard_health, daemon=True).start()
    print("Operator dashboard listening.")
    print(f"URL: {url}")
    print(f"PID: {info.pid}")
    print(f"Build stamp: {build_stamp}")
    print(f"Started at: {started_at}")
    httpd.serve_forever()
    return info


def _build_handler(service: OperatorDashboardService):
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self._serve_index_html()
                return
            if parsed.path == "/dashboard.css":
                self._serve_asset("operator_dashboard.css", "text/css; charset=utf-8")
                return
            if parsed.path == "/dashboard.js":
                self._serve_asset("operator_dashboard.js", "application/javascript; charset=utf-8")
                return
            if parsed.path == "/api/dashboard":
                try:
                    payload = service.dashboard_snapshot()
                except Exception as exc:
                    self._write_json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {
                            "error": "dashboard_snapshot_failed",
                            "message": f"{type(exc).__name__}: {exc}",
                            "build_stamp": service._build_stamp,
                            "generated_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    return
                self._write_json(HTTPStatus.OK, payload)
                return
            if parsed.path == "/health":
                self._write_json(HTTPStatus.OK, service.health_payload())
                return
            if parsed.path.startswith("/api/operator-artifact/"):
                artifact_name = parsed.path.rsplit("/", 1)[-1]
                artifact_path, content_type = service.operator_artifact_file(artifact_name)
                if artifact_path is None or not artifact_path.exists():
                    self._write_json(HTTPStatus.NOT_FOUND, {"error": "artifact_not_found", "artifact": artifact_name})
                    return
                payload = artifact_path.read_bytes()
                self._write_body(HTTPStatus.OK, payload, content_type)
                return
            if parsed.path.startswith("/api/summary/"):
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4:
                    self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return
                _, _, runtime_name, format_name = parts
                session_date = parse_qs(parsed.query).get("session_date", [None])[0]
                summary_path, content_type = service.latest_summary_file(runtime_name, format_name, session_date=session_date)
                if summary_path is None or not summary_path.exists():
                    self._write_json(
                        HTTPStatus.NOT_FOUND,
                        {"error": "summary_not_found", "runtime": runtime_name, "format": format_name, "session_date": session_date},
                    )
                    return
                payload = summary_path.read_bytes()
                self._write_body(HTTPStatus.OK, payload, content_type)
                return
            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/production-link/"):
                action = parsed.path.rsplit("/", 1)[-1]
                try:
                    body = self._read_json_body()
                    result = service.run_production_action(action, body)
                except ProductionLinkActionError as exc:
                    self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                except json.JSONDecodeError as exc:
                    self._write_json(HTTPStatus.BAD_REQUEST, {"error": f"Invalid JSON payload: {exc}"})
                    return
                self._write_json(HTTPStatus.OK if result.get("ok", False) else HTTPStatus.BAD_REQUEST, result)
                return
            if not parsed.path.startswith("/api/action/"):
                self._write_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            action = parsed.path.rsplit("/", 1)[-1]
            try:
                body = self._read_json_body()
                result = service.run_action(action, body)
            except json.JSONDecodeError as exc:
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": f"Invalid JSON payload: {exc}"})
                return
            except ValueError as exc:
                self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._write_json(HTTPStatus.OK if result.get("ok", False) else HTTPStatus.BAD_REQUEST, result)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _serve_asset(self, file_name: str, content_type: str) -> None:
            path = ASSET_DIR / file_name
            if not path.exists():
                self._write_json(HTTPStatus.NOT_FOUND, {"error": "asset_not_found", "file": file_name})
                return
            payload = path.read_bytes()
            self._write_body(HTTPStatus.OK, payload, content_type)

        def _serve_index_html(self) -> None:
            path = ASSET_DIR / "operator_dashboard.html"
            if not path.exists():
                self._write_json(HTTPStatus.NOT_FOUND, {"error": "asset_not_found", "file": "operator_dashboard.html"})
                return
            build_stamp = service._build_stamp
            payload = path.read_text(encoding="utf-8")
            payload = payload.replace("/dashboard.css", f"/dashboard.css?v={build_stamp}")
            payload = payload.replace("/dashboard.js", f"/dashboard.js?v={build_stamp}")
            payload = _inject_initial_operator_canary_markup(payload, service)
            body = payload.encode("utf-8")
            self._write_body(HTTPStatus.OK, body, "text/html; charset=utf-8")

        def _write_json(self, status: HTTPStatus, payload: Any) -> None:
            body = json.dumps(_json_ready(payload), sort_keys=True).encode("utf-8")
            self._write_body(status, body, "application/json; charset=utf-8")

        def _write_body(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError) as exc:
                self._log_client_disconnect(exc)
            except OSError as exc:
                if exc.errno in {errno.EPIPE, errno.ECONNRESET}:
                    self._log_client_disconnect(exc)
                    return
                raise

        def _read_json_body(self) -> dict[str, Any]:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            if content_length <= 0:
                return {}
            raw = self.rfile.read(content_length).decode("utf-8")
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise json.JSONDecodeError("Top-level JSON body must be an object.", raw, 0)
            return parsed

        def _log_client_disconnect(self, exc: BaseException) -> None:
            _TRANSPORT_LOGGER.info(
                "Dashboard client disconnected during %s %s while sending response (%s).",
                self.command,
                self.path,
                type(exc).__name__,
            )

    return DashboardHandler


def _inject_initial_operator_canary_markup(payload: str, service: "OperatorDashboardService") -> str:
    try:
        dashboard = service.dashboard_snapshot()
    except Exception:
        return payload

    paper = dashboard.get("paper") or {}
    canary = paper.get("non_approved_lanes") or {}
    rows = [
        row
        for row in list(canary.get("rows") or [])
        if row.get("experimental_status") == "experimental_canary" or row.get("is_canary")
    ]
    kill_switch_active = bool(canary.get("kill_switch_active"))
    enabled_count = sum(1 for row in rows if str(row.get("state") or "").upper() == "ENABLED")
    recent_signal_count = sum(int(row.get("recent_signal_count") or row.get("signal_count") or 0) for row in rows)
    recent_event_count = sum(int(row.get("recent_event_count") or row.get("event_count") or 0) for row in rows)
    badge_label = (
        "KILL SWITCH ACTIVE"
        if rows and kill_switch_active
        else ("VISIBLE NOW" if enabled_count else ("DISABLED" if rows else "NO TEMP PAPER"))
    )
    status_line = (
        f"{canary.get('operator_state_label') or 'PAPER ONLY'} • {'Kill switch active' if kill_switch_active else 'Kill switch inactive'}"
        if rows
        else "No temporary paper strategies are surfaced in this runtime."
    )
    note = canary.get("note") or "Experimental temporary paper strategies are visible here for operator monitoring."
    cards_markup = _initial_operator_canary_cards_markup(rows, kill_switch_active=kill_switch_active)

    replacements = {
        '<span id="operator-canary-badge" class="badge badge-warning">NO TEMP PAPER</span>':
            f'<span id="operator-canary-badge" class="badge badge-warning">{html.escape(badge_label)}</span>',
        '<span id="operator-canary-status">-</span>':
            f'<span id="operator-canary-status">{html.escape(status_line)}</span>',
        '<span id="operator-canary-visible-count" class="value mono">-</span>':
            f'<span id="operator-canary-visible-count" class="value mono">{len(rows)}</span>',
        '<span id="operator-canary-enabled-count" class="value mono">-</span>':
            f'<span id="operator-canary-enabled-count" class="value mono">{enabled_count}</span>',
        '<span id="operator-canary-kill-switch" class="value mono">-</span>':
            f'<span id="operator-canary-kill-switch" class="value mono">{"ACTIVE" if kill_switch_active else "INACTIVE"}</span>',
        '<span id="operator-canary-signal-count" class="value mono">-</span>':
            f'<span id="operator-canary-signal-count" class="value mono">{recent_signal_count}</span>',
        '<span id="operator-canary-event-count" class="value mono">-</span>':
            f'<span id="operator-canary-event-count" class="value mono">{recent_event_count}</span>',
        '<span id="operator-canary-note">-</span>':
            f'<span id="operator-canary-note">{html.escape(note)}</span>',
        '<div id="operator-canary-cards" class="operator-canary-card-grid"></div>':
            f'<div id="operator-canary-cards" class="operator-canary-card-grid">{cards_markup}</div>',
    }
    for target, replacement in replacements.items():
        payload = payload.replace(target, replacement)
    return payload


def _initial_operator_canary_cards_markup(rows: list[dict[str, Any]], *, kill_switch_active: bool) -> str:
    if not rows:
        return (
            '<article class="operator-canary-card">'
            '<div class="operator-canary-summary-list">'
            '<span>No ATPE temporary paper strategies are currently surfaced.</span>'
            '<span>Expected lanes: ATPE Long Medium+High Canary and ATPE Short High-Only Canary.</span>'
            "</div>"
            "</article>"
        )

    cards: list[str] = []
    for row in sorted(rows, key=lambda item: str(item.get("display_name") or item.get("branch") or "")):
        title = str(row.get("display_name") or row.get("branch") or row.get("lane_id") or "-")
        lane_id = str(row.get("lane_id") or "-")
        state = str(row.get("state") or "UNKNOWN").upper()
        latest_atp_state = row.get("latest_atp_state") or {}
        latest_atp_entry_state = row.get("latest_atp_entry_state") or {}
        latest_atp_timing_state = row.get("latest_atp_timing_state") or {}
        status_parts = [
            "Experimental" if row.get("experimental_status") == "experimental_canary" else None,
            "Paper Only" if row.get("paper_only") else None,
            "Non-Approved" if row.get("non_approved") else None,
            f"Quality {row.get('quality_bucket_policy')}" if row.get("quality_bucket_policy") else None,
            f"Side {row.get('side')}" if row.get("side") else None,
        ]
        status_summary = " • ".join(part for part in status_parts if part)
        cards.append(
            '<article class="operator-canary-card">'
            '<div class="operator-canary-card-header">'
            '<div>'
            f'<div class="operator-canary-card-title">{html.escape(title)}</div>'
            f'<div class="operator-canary-card-subtitle mono">{html.escape(lane_id)}</div>'
            "</div>"
            f'<span class="badge badge-warning">{html.escape(state)}</span>'
            "</div>"
            '<div class="operator-canary-chip-row">'
            f'<span class="operator-canary-chip"><strong>Status</strong> {html.escape(status_summary or "Experimental • Paper Only • Non-Approved")}</span>'
            f'<span class="operator-canary-chip"><strong>Kill Switch</strong> {"ACTIVE" if kill_switch_active else "INACTIVE"}</span>'
            "</div>"
            '<div class="operator-canary-chip-row">'
            f'<span class="operator-canary-chip"><strong>Signals</strong> {int(row.get("recent_signal_count") or row.get("signal_count") or 0)}</span>'
            f'<span class="operator-canary-chip"><strong>Events</strong> {int(row.get("recent_event_count") or row.get("event_count") or 0)}</span>'
            "</div>"
            '<div class="operator-canary-chip-row">'
            f'<span class="operator-canary-chip"><strong>ATP Bias</strong> {html.escape(str(latest_atp_state.get("bias_state") or "-"))}</span>'
            f'<span class="operator-canary-chip"><strong>ATP Pullback</strong> {html.escape(str(latest_atp_state.get("pullback_state") or "-"))}</span>'
            "</div>"
            '<div class="operator-canary-chip-row">'
            f'<span class="operator-canary-chip"><strong>ATP Entry</strong> {html.escape(str(latest_atp_entry_state.get("entry_state") or "-"))}</span>'
            f'<span class="operator-canary-chip"><strong>ATP Blocker</strong> {html.escape(str(latest_atp_entry_state.get("primary_blocker") or "-"))}</span>'
            "</div>"
            '<div class="operator-canary-chip-row">'
            f'<span class="operator-canary-chip"><strong>ATP Timing</strong> {html.escape(str(latest_atp_timing_state.get("timing_state") or "-"))}</span>'
            f'<span class="operator-canary-chip"><strong>ATP VWAP</strong> {html.escape(str(latest_atp_timing_state.get("vwap_price_quality_state") or "-"))}</span>'
            "</div>"
            '<div class="operator-canary-summary-list">'
            f'<span>{html.escape(str(row.get("operator_status_line") or status_summary or "-"))}</span>'
            f'<span>{html.escape("Depth " + str(latest_atp_state.get("pullback_depth_score")) + " | Violence " + str(latest_atp_state.get("pullback_violence_score")) + (" | " + str(latest_atp_state.get("pullback_reason")) if latest_atp_state.get("pullback_reason") else "") + " | Trigger " + str(latest_atp_entry_state.get("continuation_trigger_state") or "-") + " | Timing " + str(latest_atp_timing_state.get("primary_blocker") or latest_atp_timing_state.get("timing_state") or "-"))}</span>'
            f'<span>{html.escape(str(row.get("note") or "Experimental temporary paper strategy for dashboard observation."))}</span>'
            "</div>"
            "</article>"
        )
    return "".join(cards)


def _bind_dashboard_server(host: str, preferred_port: int, handler, *, allow_port_fallback: bool):
    if not allow_port_fallback:
        try:
            return ThreadingHTTPServer((host, preferred_port), handler), preferred_port
        except OSError as exc:
            if isinstance(exc, PermissionError) or exc.errno == errno.EACCES:
                raise OSError(
                    f"Dashboard could not bind {host}:{preferred_port}; socket bind permission was denied in this environment."
                ) from exc
            conflict = _listening_process_details(preferred_port)
            if conflict:
                message = (
                    f"Dashboard could not bind {host}:{preferred_port}; "
                    f"port is already in use by PID {conflict.get('pid')} ({conflict.get('command') or 'unknown'}) "
                    f"listening on {conflict.get('listener') or preferred_port}."
                )
            else:
                message = f"Dashboard could not bind {host}:{preferred_port}; port is already in use."
            raise OSError(message) from exc
    return _bind_first_available(host, preferred_port, handler)


def _bind_first_available(host: str, preferred_port: int, handler):
    for candidate in range(preferred_port, preferred_port + 200):
        try:
            return ThreadingHTTPServer((host, candidate), handler), candidate
        except OSError:
            continue
    raise OSError(f"Could not bind dashboard server on {host} starting at port {preferred_port}.")


def _listening_process_details(port: int) -> dict[str, str] | None:
    result = subprocess.run(
        ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fpctn"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    details: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if not line:
            continue
        if line.startswith("p") and "pid" not in details:
            details["pid"] = line[1:]
        elif line.startswith("c") and "command" not in details:
            details["command"] = line[1:]
        elif line.startswith("n") and "listener" not in details:
            details["listener"] = line[1:]
        if {"pid", "command", "listener"} <= details.keys():
            break
    return details or None


def _lane_registry_section(lane_registry: dict[str, Any], key: str) -> dict[str, Any]:
    for section in lane_registry.get("sections", []):
        if section.get("key") == key:
            return section
    return {"key": key, "rows": []}


def _is_temporary_paper_strategy_row(row: dict[str, Any]) -> bool:
    if bool(row.get("temporary_paper_strategy")):
        return True
    if str(row.get("paper_strategy_class") or "") == "temporary_paper_strategy":
        return True
    return str(row.get("experimental_status") or "") in {"experimental_canary", "experimental_temp_paper"}


def _atomic_write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(payload)
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return _json_ready(value.value)
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json_file(path: Path, payload: Any) -> None:
    _atomic_write_text(path, json.dumps(_json_ready(payload), sort_keys=True, indent=2) + "\n")


def _snapshot_warning_section(path: Path) -> str:
    path_text = str(path)
    if "historical_playback" in path_text:
        return "historical_playback"
    if "paper_session_close_review" in path_text or "paper_session_close_reviews" in path_text:
        return "paper_session_close_review"
    if "paper_carry_forward" in path_text or "paper_pre_session_review" in path_text or "paper_run_start" in path_text:
        return "paper_readiness"
    if "operator_dashboard" in path_text:
        return "dashboard_state"
    if "probationary_pattern_engine" in path_text:
        return "runtime_artifacts"
    return "artifacts"


def _record_snapshot_warning(path: Path, *, reader: str, detail: str) -> None:
    warnings = _SNAPSHOT_WARNINGS.get()
    entry = {
        "section": _snapshot_warning_section(path),
        "reader": reader,
        "path": str(path),
        "detail": detail,
    }
    if warnings is not None:
        warnings.append(entry)
    _TRANSPORT_LOGGER.info("dashboard snapshot degraded %s %s: %s", reader, path, detail)


def _summarize_snapshot_warnings(warnings: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for warning in warnings:
        section = str(warning.get("section") or "artifacts")
        bucket = grouped.setdefault(
            section,
            {
                "section": section,
                "count": 0,
                "paths": [],
                "latest_detail": None,
            },
        )
        bucket["count"] = int(bucket["count"]) + 1
        path = str(warning.get("path") or "")
        if path and path not in bucket["paths"]:
            bucket["paths"].append(path)
        if warning.get("detail"):
            bucket["latest_detail"] = warning["detail"]
    return sorted(grouped.values(), key=lambda item: (str(item["section"]), str(item["latest_detail"] or "")))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = path.read_text(encoding="utf-8")
    except OSError as exc:
        _record_snapshot_warning(path, reader="json", detail=f"{type(exc).__name__}: {exc}")
        return {}
    if not payload.strip():
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        _record_snapshot_warning(path, reader="json", detail=f"{type(exc).__name__}: {exc}")
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = path.read_text(encoding="utf-8")
    except OSError as exc:
        _record_snapshot_warning(path, reader="json_list", detail=f"{type(exc).__name__}: {exc}")
        return []
    if not payload.strip():
        return []
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        _record_snapshot_warning(path, reader="json_list", detail=f"{type(exc).__name__}: {exc}")
        return []
    return parsed if isinstance(parsed, list) else []


def _path_or_none(value: Any) -> Path | None:
    if not value:
        return None
    return Path(str(value))


def _historical_playback_result_status(row: dict[str, Any]) -> str:
    if int(row.get("fills_created") or 0) > 0:
        return "FIRED"
    block_reason = str(row.get("block_or_fault_reason") or "").strip()
    if block_reason and block_reason != "no_trigger_seen":
        return "BLOCKED"
    return "NO FIRE"


def _tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        _record_snapshot_warning(path, reader="jsonl", detail=f"{type(exc).__name__}: {exc}")
        return []
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError as exc:
            _record_snapshot_warning(path, reader="jsonl", detail=f"{type(exc).__name__} on line {line_number}: {exc}")
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows[-limit:]


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def _read_csv_rows(path: Path, limit: int | None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if limit is None:
        return rows
    return rows[-limit:]


def _latest_table_rows(db_path: Path | None, table_name: str, order_column: str, limit: int) -> list[dict[str, Any]]:
    if db_path is None or not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"select * from {table_name} order by {order_column} desc limit ?",
            (limit,),
        ).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _latest_table_rows_across_paths(
    db_paths: Sequence[Path | None],
    table_name: str,
    order_column: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for db_path in db_paths:
        rows.extend(_latest_table_rows(db_path, table_name, order_column, limit))
    rows.sort(key=lambda row: str(row.get(order_column) or ""), reverse=True)
    return rows[:limit]


def _all_table_rows(db_path: Path | None, table_name: str, order_column: str) -> list[dict[str, Any]]:
    if db_path is None or not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"select * from {table_name} order by {order_column} asc"
        ).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _all_table_rows_safe(db_path: Path | None, table_name: str, order_column: str) -> list[dict[str, Any]]:
    try:
        return _all_table_rows(db_path, table_name, order_column)
    except sqlite3.Error:
        return []


def _session_table_rows(
    db_path: Path | None,
    table_name: str,
    order_column: str,
    timestamp_column: str,
    session_date: str | None,
) -> list[dict[str, Any]]:
    if db_path is None or not db_path.exists() or not session_date:
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            f"select * from {table_name} order by {order_column} desc",
        ).fetchall()
    finally:
        connection.close()
    return [
        dict(row)
        for row in rows
        if _timestamp_matches_session(row[timestamp_column], session_date)
    ]


def _session_table_rows_across_paths(
    db_paths: Sequence[Path | None],
    table_name: str,
    order_column: str,
    timestamp_column: str,
    session_date: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for db_path in db_paths:
        rows.extend(_session_table_rows(db_path, table_name, order_column, timestamp_column, session_date))
    rows.sort(key=lambda row: str(row.get(order_column) or ""), reverse=True)
    return rows


def _session_jsonl_rows(path: Path, session_date: str | None, *timestamp_fields: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    row = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if session_date and timestamp_fields:
                    if not any(_timestamp_matches_session(row.get(field), session_date) for field in timestamp_fields):
                        continue
                rows.append(row)
    except OSError:
        return []
    return rows


def _probationary_lane_artifacts_dir(lane_row: dict[str, Any], root_artifacts_dir: Path | None, lane_id: str) -> Path:
    configured = lane_row.get("artifacts_dir")
    if configured:
        configured_path = Path(str(configured))
        if configured_path.name == lane_id or (configured_path / "operator_status.json").exists():
            return configured_path
    if root_artifacts_dir is not None:
        return root_artifacts_dir / "lanes" / lane_id
    return Path(lane_id)


def _probationary_lane_database_paths(operator_status: dict[str, Any], root_db_path: Path | None) -> list[Path | None]:
    lane_rows = operator_status.get("lanes")
    if not isinstance(lane_rows, list) or not lane_rows:
        return [root_db_path]
    paths: list[Path | None] = []
    seen: set[Path] = set()
    for row in lane_rows:
        db_path = _resolve_sqlite_database_path(row.get("database_url"))
        if db_path is None:
            continue
        if db_path in seen:
            continue
        seen.add(db_path)
        paths.append(db_path)
    return paths or [root_db_path]


def _derive_probationary_lane_database_url(root_db_path: Path | None, lane_id: str) -> str | None:
    if root_db_path is None or not lane_id:
        return None
    suffix = root_db_path.suffix or ".sqlite3"
    derived_path = root_db_path.with_name(f"{root_db_path.stem}__{lane_id}{suffix}")
    return f"sqlite:///{derived_path}"


def _temporary_paper_overlay_spec_for_row(repo_root: Path, row: dict[str, Any]) -> dict[str, Any] | None:
    lane_id = str(row.get("lane_id") or "").strip()
    runtime_kind = str(row.get("runtime_kind") or "").strip()
    source_family = str(row.get("source_family") or row.get("observer_variant_id") or "").strip()
    for spec in _TEMP_PAPER_OVERLAY_SPECS:
        if runtime_kind and runtime_kind in spec["runtime_kinds"]:
            return {
                **spec,
                "config_path": str((repo_root / spec["config_path"]).resolve()),
            }
        if lane_id and any(lane_id.startswith(prefix) for prefix in spec["lane_id_prefixes"]):
            return {
                **spec,
                "config_path": str((repo_root / spec["config_path"]).resolve()),
            }
        if source_family and source_family in spec["source_families"]:
            return {
                **spec,
                "config_path": str((repo_root / spec["config_path"]).resolve()),
            }
    return None


def _runtime_lookup_by_lane_id(runtime_registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in list(runtime_registry.get("rows") or []):
        lane_id = str(
            row.get("lane_id")
            or row.get("standalone_strategy_id")
            or row.get("strategy_key")
            or ""
        ).strip()
        if not lane_id:
            continue
        lookup[lane_id] = dict(row)
    return lookup


def _standalone_runtime_state_loaded(db_path: Path | None, standalone_strategy_id: str) -> bool:
    if db_path is None or not db_path.exists():
        return False
    try:
        connection = sqlite3.connect(db_path)
        try:
            row = connection.execute(
                """
                select 1
                from strategy_state_snapshots
                where standalone_strategy_id = ?
                   or standalone_strategy_id is null
                limit 1
                """,
                (standalone_strategy_id,),
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error:
        return False
    return row is not None


def _legacy_strategy_performance_key(*, lane_id: str, instrument: str, source_family: str) -> str:
    return f"{instrument}|{lane_id}|{source_family}"


def _annotate_runtime_identity_state(
    row: dict[str, Any],
    runtime_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    strategy_key = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
    lane_id = str(row.get("lane_id") or "").strip()
    runtime_row = runtime_lookup.get(strategy_key) or (runtime_lookup.get(lane_id) if lane_id else None) or {}
    if not runtime_row:
        return {
            **row,
            "runtime_instance_present": False,
            "runtime_state_loaded": False,
            "config_source": row.get("config_source"),
            "legacy_derived_identity": row.get("legacy_derived_identity", False),
        }
    return {
        **row,
        "runtime_instance_present": bool(runtime_row.get("runtime_instance_present", True)),
        "runtime_state_loaded": bool(runtime_row.get("runtime_state_loaded", False)),
        "config_source": runtime_row.get("config_source") or row.get("config_source"),
        "legacy_derived_identity": bool(runtime_row.get("legacy_derived_identity", False)),
        "runtime_kind": runtime_row.get("runtime_kind"),
    }


def _annotate_same_underlying_strategy_ambiguity(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter(
        str(row.get("instrument") or "").strip().upper()
        for row in rows
        if row.get("instrument")
    )
    annotated: list[dict[str, Any]] = []
    for row in rows:
        instrument = str(row.get("instrument") or "").strip().upper()
        same_underlying_ambiguity = bool(instrument) and counts[instrument] > 1
        ambiguity_note = (
            "Multiple standalone strategy identities currently share this underlying instrument. Same-underlying order netting and position arbitration remain explicitly constrained."
            if same_underlying_ambiguity
            else None
        )
        annotated.append(
            {
                **row,
                "same_underlying_ambiguity": same_underlying_ambiguity,
                "same_underlying_ambiguity_note": ambiguity_note,
            }
        )
    return annotated


def _build_same_underlying_conflicts(
    *,
    paper: dict[str, Any],
    production_link: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    def _normalized_position_side(value: Any) -> str:
        text = str(value or "").strip().upper()
        if not text or text in {"FLAT", "NONE", "UNKNOWN"}:
            return "NONE"
        if text in {"MULTI", "BOTH", "LONG_SHORT", "SHORT_LONG"}:
            return "BOTH"
        if "LONG" in text and "SHORT" in text:
            return "BOTH"
        if "LONG" in text:
            return "LONG"
        if "SHORT" in text:
            return "SHORT"
        return "UNKNOWN"

    def _pending_order_side(intent_type: Any) -> str:
        text = str(intent_type or "").strip().upper()
        if not text:
            return "UNKNOWN"
        if text in {"BUY_TO_OPEN", "BUY", "BTO"}:
            return "LONG"
        if text in {"SELL_TO_OPEN", "SELL", "STO"}:
            return "SHORT"
        return "UNKNOWN"

    def _side_profile(values: Sequence[Any]) -> str:
        normalized = [_normalized_position_side(value) for value in values]
        if not normalized:
            return "NONE"
        if "BOTH" in normalized:
            return "BOTH"
        long_present = any(value == "LONG" for value in normalized)
        short_present = any(value == "SHORT" for value in normalized)
        if long_present and short_present:
            return "BOTH"
        if long_present:
            return "LONG_ONLY"
        if short_present:
            return "SHORT_ONLY"
        return "UNKNOWN"

    def _operator_workflow_for_conflict(
        *,
        instrument: str,
        conflict_kind: str,
        severity: str,
        broker_overlap_present: bool,
    ) -> list[str]:
        if conflict_kind == "multiple_configured_same_instrument":
            return [
                f"Multiple standalone strategies are configured for {instrument}, but no overlapping runtime or exposure is surfaced yet.",
                "This is informational only in the current state; multiple strategies on the same instrument are allowed.",
                "The app is not auto-merging or auto-arbitrating these strategies if they later create real execution overlap on the same underlying.",
            ]
        if conflict_kind in {"multiple_runtime_instances_same_instrument", "multiple_eligible_same_instrument"}:
            return [
                f"Two or more standalone strategies share {instrument} and are currently active or eligible.",
                "This is informational only; strategy coexistence on one instrument is allowed until real execution ambiguity appears.",
                "The app is not automatically blocking, netting, or choosing between them in this phase.",
            ]
        if conflict_kind == "same_side_in_position_overlap":
            return [
                f"{instrument} has same-side live exposure across multiple standalone strategies.",
                "This is a non-blocking warning: coexistence is allowed, but attribution and exposure should remain visible to the operator.",
                "The app will not auto-net, auto-flatten, or auto-arbitrate these strategies for you.",
            ]
        if conflict_kind == "opposite_side_in_position_overlap":
            return [
                f"{instrument} has opposite-side live exposure across multiple standalone strategies.",
                "This is a blocking execution conflict because the live exposure cannot be interpreted safely without manual review.",
                "Freeze additional entries on this instrument until the opposite-side overlap is reviewed and reconciled.",
            ]
        if conflict_kind == "multiple_pending_orders_same_instrument":
            return [
                f"{instrument} has overlapping pending-order exposure across multiple standalone strategies.",
                "This is execution-relevant now because more than one strategy may progress into the same underlying before manual review.",
                "Treat new entries on this instrument as unsafe until the pending overlap is reviewed and resolved.",
            ]
        if conflict_kind == "mixed_position_and_pending_overlap":
            return [
                f"{instrument} has both open-position exposure and pending-order overlap across standalone strategies.",
                "This is a blocking conflict because the app is not performing same-underlying arbitration or safe netting automatically.",
                "Pause new exposure on this instrument until the position and pending-order state has been reviewed together.",
            ]
        if conflict_kind == "broker_vs_strategy_overlap_mismatch":
            return [
                f"{instrument} has both same-underlying strategy overlap and broker-reported exposure or orders.",
                "This is blocking because the app cannot safely attribute the broker overlap to one standalone strategy automatically.",
                "Review broker positions/orders, strategy ledger rows, and reconciliation for this instrument before allowing more exposure.",
            ]
        return [
            f"{instrument} has a same-underlying conflict requiring operator review.",
            "The app is surfacing the conflict, but it is not auto-merging, auto-netting, or auto-resolving same-underlying overlap in this phase.",
            "Review runtime truth, strategy ledger truth, and broker truth separately before allowing more exposure.",
        ]

    runtime_rows = list(((paper.get("runtime_registry") or {}).get("rows") or []))
    strategy_rows = list(((paper.get("strategy_performance") or {}).get("rows") or []))
    audit_rows = list(((paper.get("signal_intent_fill_audit") or {}).get("rows") or []))

    merged: dict[str, dict[str, Any]] = {}
    for row in runtime_rows:
        strategy_id = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_id:
            continue
        merged[strategy_id] = {
            "standalone_strategy_id": strategy_id,
            "instrument": str(row.get("instrument") or row.get("symbol") or "").strip().upper(),
            "strategy_family": str(row.get("strategy_family") or row.get("family") or row.get("source_family") or "UNKNOWN"),
            "strategy_name": row.get("display_name") or row.get("standalone_strategy_label") or strategy_id,
            "runtime_instance_present": bool(row.get("runtime_instance_present", True)),
            "runtime_state_loaded": bool(row.get("runtime_state_loaded", False)),
            "can_process_bars": bool(row.get("can_process_bars", False)),
            "config_source": row.get("config_source"),
            "enabled": row.get("enabled"),
            "current_strategy_status": None,
            "position_side": "FLAT",
            "eligible_now": None,
            "open_broker_order_id": None,
            "pending_order_present": False,
            "pending_order_side": "NONE",
        }
    for row in strategy_rows:
        strategy_id = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_id:
            continue
        payload = merged.setdefault(
            strategy_id,
            {
                "standalone_strategy_id": strategy_id,
                "instrument": str(row.get("instrument") or "").strip().upper(),
                "strategy_family": str(row.get("strategy_family") or row.get("family") or row.get("source_family") or "UNKNOWN"),
                "strategy_name": row.get("strategy_name") or row.get("standalone_strategy_label") or strategy_id,
                "runtime_instance_present": True,
                "runtime_state_loaded": False,
                "can_process_bars": True,
                "config_source": row.get("config_source"),
                "enabled": row.get("entries_enabled"),
                "current_strategy_status": None,
                "position_side": "FLAT",
                "eligible_now": None,
                "open_broker_order_id": None,
                "pending_order_present": False,
                "pending_order_side": "NONE",
            },
        )
        payload["instrument"] = str(row.get("instrument") or payload.get("instrument") or "").strip().upper()
        payload["strategy_family"] = str(row.get("strategy_family") or payload.get("strategy_family") or "UNKNOWN")
        payload["strategy_name"] = row.get("strategy_name") or payload.get("strategy_name") or strategy_id
        payload["runtime_instance_present"] = bool(payload.get("runtime_instance_present", True))
        payload["current_strategy_status"] = row.get("status") or payload.get("current_strategy_status")
        payload["position_side"] = str(row.get("position_side") or payload.get("position_side") or "FLAT").upper()
        payload["enabled"] = row.get("entries_enabled") if row.get("entries_enabled") is not None else payload.get("enabled")
    for row in audit_rows:
        strategy_id = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        if not strategy_id:
            continue
        payload = merged.setdefault(
            strategy_id,
            {
                "standalone_strategy_id": strategy_id,
                "instrument": str(row.get("instrument") or "").strip().upper(),
                "strategy_family": str(row.get("strategy_family") or row.get("family") or row.get("source_family") or "UNKNOWN"),
                "strategy_name": row.get("strategy_name") or row.get("standalone_strategy_label") or strategy_id,
                "runtime_instance_present": True,
                "runtime_state_loaded": bool(row.get("runtime_state_loaded", False)),
                "can_process_bars": True,
                "config_source": row.get("config_source"),
                "enabled": row.get("entries_enabled"),
                "current_strategy_status": None,
                "position_side": "FLAT",
                "eligible_now": None,
                "open_broker_order_id": None,
                "pending_order_present": False,
                "pending_order_side": "NONE",
            },
        )
        payload["instrument"] = str(row.get("instrument") or payload.get("instrument") or "").strip().upper()
        payload["strategy_family"] = str(row.get("strategy_family") or payload.get("strategy_family") or "UNKNOWN")
        payload["strategy_name"] = row.get("strategy_name") or payload.get("strategy_name") or strategy_id
        payload["runtime_instance_present"] = bool(payload.get("runtime_instance_present", True))
        payload["runtime_state_loaded"] = bool(row.get("runtime_state_loaded", payload.get("runtime_state_loaded", False)))
        payload["current_strategy_status"] = row.get("current_strategy_status") or payload.get("current_strategy_status")
        payload["position_side"] = str(row.get("position_side") or payload.get("position_side") or "FLAT").upper()
        payload["eligible_now"] = row.get("eligible_now") if row.get("eligible_now") is not None else payload.get("eligible_now")
        payload["open_broker_order_id"] = row.get("open_broker_order_id") or payload.get("open_broker_order_id")
        payload["pending_order_present"] = bool(row.get("open_broker_order_id")) or bool(payload.get("pending_order_present"))
        if payload["pending_order_present"]:
            payload["pending_order_side"] = _pending_order_side(row.get("last_intent_type"))
        payload["latest_fault_or_blocker"] = row.get("latest_fault_or_blocker") or payload.get("latest_fault_or_blocker")

    broker_positions = list((((production_link.get("portfolio") or {}).get("positions")) or []))
    broker_open_orders = list((((production_link.get("orders") or {}).get("open_rows")) or []))
    broker_position_counts = Counter(str(row.get("symbol") or "").strip().upper() for row in broker_positions if row.get("symbol"))
    broker_open_order_counts = Counter(str(row.get("symbol") or "").strip().upper() for row in broker_open_orders if row.get("symbol"))
    reconciliation = production_link.get("reconciliation") or {}

    groups: dict[str, list[dict[str, Any]]] = {}
    for payload in merged.values():
        instrument = str(payload.get("instrument") or "").strip().upper()
        if instrument:
            groups.setdefault(instrument, []).append(payload)

    conflict_rows: list[dict[str, Any]] = []
    for instrument, strategies in sorted(groups.items()):
        if len(strategies) < 2:
            continue
        runtime_instance_count = sum(1 for row in strategies if row.get("runtime_instance_present") is True)
        eligible_count = sum(1 for row in strategies if row.get("eligible_now") is True)
        in_position_count = sum(1 for row in strategies if str(row.get("position_side") or "FLAT").upper() not in {"", "FLAT", "UNKNOWN"})
        pending_order_count = sum(1 for row in strategies if row.get("pending_order_present") is True)
        broker_position_count = int(broker_position_counts.get(instrument, 0))
        broker_order_count = int(broker_open_order_counts.get(instrument, 0))
        broker_overlap_present = broker_position_count > 0 or broker_order_count > 0
        in_position_overlap_present = in_position_count > 1
        pending_order_overlap_present = pending_order_count > 1 or broker_order_count > 1
        mixed_overlap_present = (in_position_count > 0 and (pending_order_count > 0 or broker_order_count > 0))
        position_side_profile = _side_profile(
            [row.get("position_side") for row in strategies if _normalized_position_side(row.get("position_side")) != "NONE"]
        )
        pending_order_side_profile = _side_profile(
            [row.get("pending_order_side") for row in strategies if row.get("pending_order_present") is True]
        )
        reconciliation_state = str(reconciliation.get("label") or reconciliation.get("status") or "").strip()
        reconciliation_clear = reconciliation_state.upper() == "CLEAR"

        if broker_overlap_present and (in_position_count > 0 or pending_order_count > 0 or eligible_count > 1 or runtime_instance_count > 1):
            conflict_kind = "broker_vs_strategy_overlap_mismatch"
            severity = "BLOCKING"
            conflict_reason = (
                f"{instrument} has multiple standalone strategies plus broker-reported exposure/orders. Attribution and netting are not automatic for this underlying."
            )
        elif mixed_overlap_present:
            conflict_kind = "mixed_position_and_pending_overlap"
            severity = "BLOCKING"
            conflict_reason = (
                f"{instrument} has both open position exposure and pending-order overlap across multiple standalone strategies. New execution should be treated as unsafe until reviewed."
            )
        elif pending_order_overlap_present:
            conflict_kind = "multiple_pending_orders_same_instrument"
            severity = "BLOCKING"
            conflict_reason = (
                f"{instrument} has overlapping pending-order exposure across multiple standalone strategies. Arbitration is not automatic."
            )
        elif in_position_overlap_present and position_side_profile == "BOTH":
            conflict_kind = "opposite_side_in_position_overlap"
            severity = "BLOCKING"
            conflict_reason = (
                f"{instrument} has opposite-side live exposure across standalone strategies. The app will not auto-arbitrate incompatible same-underlying positions."
            )
        elif in_position_overlap_present:
            conflict_kind = "same_side_in_position_overlap"
            severity = "ACTION"
            conflict_reason = (
                f"{instrument} has same-side live exposure across multiple standalone strategies. Coexistence is allowed, but the overlap remains operator-visible."
            )
        elif eligible_count > 1:
            conflict_kind = "multiple_eligible_same_instrument"
            severity = "INFO"
            conflict_reason = (
                f"{instrument} has multiple standalone strategies eligible now. This is informational until real position, order, or broker overlap appears."
            )
        elif runtime_instance_count > 1:
            conflict_kind = "multiple_runtime_instances_same_instrument"
            severity = "INFO"
            conflict_reason = (
                f"{instrument} has multiple active standalone runtime identities. This is informational until they create real execution overlap."
            )
        else:
            conflict_kind = "multiple_configured_same_instrument"
            severity = "INFO"
            conflict_reason = (
                f"{instrument} has multiple configured standalone strategies, but no current overlapping exposure or pending orders are surfaced."
            )

        execution_risk = severity == "BLOCKING"
        observational_only = severity == "INFO"
        if broker_overlap_present and (runtime_instance_count > 0 or eligible_count > 0 or in_position_count > 0 or pending_order_count > 0):
            overlap_scope = "BROKER_AND_STRATEGY"
        elif broker_overlap_present:
            overlap_scope = "BROKER_ONLY"
        else:
            overlap_scope = "STRATEGY_ONLY"

        conflict_rows.append(
            {
                "id": instrument,
                "instrument": instrument,
                "standalone_strategy_ids": sorted(str(row.get("standalone_strategy_id") or "") for row in strategies),
                "strategy_families": sorted({str(row.get("strategy_family") or "UNKNOWN") for row in strategies}),
                "conflict_kind": conflict_kind,
                "severity": severity,
                "conflict_reason": conflict_reason,
                "operator_action_required": severity == "BLOCKING",
                "execution_risk": execution_risk,
                "observational_only": observational_only,
                "broker_overlap_present": broker_overlap_present,
                "in_position_overlap_present": in_position_overlap_present,
                "pending_order_overlap_present": pending_order_overlap_present,
                "overlap_scope": overlap_scope,
                "configured_count": len(strategies),
                "runtime_instance_count": runtime_instance_count,
                "eligible_count": eligible_count,
                "in_position_count": in_position_count,
                "pending_order_count": pending_order_count,
                "broker_position_count": broker_position_count,
                "broker_order_count": broker_order_count,
                "position_side_profile": position_side_profile,
                "pending_order_side_profile": pending_order_side_profile,
                "reconciliation_state": reconciliation_state,
                "reconciliation_clear": reconciliation_clear,
                "operator_workflow": _operator_workflow_for_conflict(
                    instrument=instrument,
                    conflict_kind=conflict_kind,
                    severity=severity,
                    broker_overlap_present=broker_overlap_present,
                ),
                "strategies": [
                    {
                        "standalone_strategy_id": row.get("standalone_strategy_id"),
                        "strategy_family": row.get("strategy_family"),
                        "strategy_name": row.get("strategy_name"),
                        "runtime_instance_present": row.get("runtime_instance_present"),
                        "runtime_state_loaded": row.get("runtime_state_loaded"),
                        "can_process_bars": row.get("can_process_bars"),
                        "current_strategy_status": row.get("current_strategy_status"),
                        "position_side": row.get("position_side"),
                        "eligible_now": row.get("eligible_now"),
                        "pending_order_present": row.get("pending_order_present"),
                        "pending_order_side": row.get("pending_order_side"),
                        "open_broker_order_id": row.get("open_broker_order_id"),
                        "latest_fault_or_blocker": row.get("latest_fault_or_blocker"),
                    }
                    for row in sorted(strategies, key=lambda item: str(item.get("standalone_strategy_id") or ""))
                ],
            }
        )

    severity_counts = Counter(str(row.get("severity") or "INFO") for row in conflict_rows)
    return {
        "generated_at": generated_at,
        "rows": conflict_rows,
        "summary": {
            "conflict_count": len(conflict_rows),
            "severity_counts": dict(severity_counts),
            "blocking_conflict_count": sum(1 for row in conflict_rows if row.get("severity") == "BLOCKING"),
            "affected_instruments": [row["instrument"] for row in conflict_rows],
        },
        "notes": [
            "Multiple strategies on the same instrument are allowed; only real execution ambiguity is escalated beyond informational coexistence.",
            "Same-underlying conflicts are surfaced explicitly; the app is not silently arbitrating, netting, or flattening them in this phase.",
            "Baseline-parity completed-bar evaluation and NEXT_BAR_OPEN replay-fill semantics remain unchanged.",
        ],
    }


def _default_same_underlying_conflict_review_record(instrument: str) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "current_conflict_fingerprint": None,
        "reviewed_conflict_fingerprint": None,
        "current_material_state": None,
        "reviewed_material_state": None,
        "severity_at_review": None,
        "conflict_kind_at_review": None,
        "acknowledged": False,
        "acknowledged_at": None,
        "acknowledged_by": None,
        "acknowledgement_note": None,
        "hold_new_entries": False,
        "hold_reason": None,
        "hold_set_at": None,
        "hold_set_by": None,
        "hold_expires_at": None,
        "hold_expired": False,
        "hold_expired_at": None,
        "hold_expiry_enforced": False,
        "hold_effective_now": False,
        "hold_state_reason": None,
        "override_observational_only": False,
        "override_reason": None,
        "override_set_at": None,
        "override_set_by": None,
        "state_status": "OPEN",
        "auto_reopen_required": False,
        "stale_since": None,
        "reopened_reason": None,
        "entry_hold_effective": False,
        "exit_actions_still_allowed": True,
    }


def _same_underlying_conflict_material_state(conflict: dict[str, Any]) -> dict[str, Any]:
    return {
        "instrument": str(conflict.get("instrument") or "").strip().upper(),
        "severity": str(conflict.get("severity") or ""),
        "operator_action_required": bool(conflict.get("operator_action_required")),
        "execution_risk": bool(conflict.get("execution_risk")),
        "broker_overlap_present": bool(conflict.get("broker_overlap_present")),
        "broker_position_count": int(conflict.get("broker_position_count") or 0),
        "broker_order_count": int(conflict.get("broker_order_count") or 0),
        "in_position_count": int(conflict.get("in_position_count") or 0),
        "pending_order_count": int(conflict.get("pending_order_count") or 0),
        "position_side_profile": str(conflict.get("position_side_profile") or "NONE"),
        "pending_order_side_profile": str(conflict.get("pending_order_side_profile") or "NONE"),
    }


def _same_underlying_conflict_fingerprint(conflict: dict[str, Any]) -> str:
    material_state = _same_underlying_conflict_material_state(conflict)
    return hashlib.sha256(json.dumps(material_state, sort_keys=True).encode("utf-8")).hexdigest()


def _same_underlying_conflict_side_profile_label(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text == "LONG_ONLY":
        return "long-only"
    if text == "SHORT_ONLY":
        return "short-only"
    if text == "BOTH":
        return "both sides"
    if text == "NONE":
        return "flat"
    return "mixed/unknown"


def _same_underlying_conflict_reopen_changes(
    previous_material_state: dict[str, Any] | None,
    current_material_state: dict[str, Any],
) -> list[str]:
    previous = dict(previous_material_state or {})
    current = dict(current_material_state or {})
    severity_rank = {"INFO": 0, "ACTION": 1, "WARNING": 1, "BLOCKING": 2}
    previous_severity = str(previous.get("severity") or "INFO").upper()
    current_severity = str(current.get("severity") or "INFO").upper()
    changes: list[str] = []

    if severity_rank.get(current_severity, 0) > severity_rank.get(previous_severity, 0):
        changes.append(f"conflict severity escalated from {previous_severity} to {current_severity}")

    previous_position_count = int(previous.get("in_position_count") or 0)
    current_position_count = int(current.get("in_position_count") or 0)
    previous_position_profile = str(previous.get("position_side_profile") or "NONE").upper()
    current_position_profile = str(current.get("position_side_profile") or "NONE").upper()
    if previous_position_count == 0 and current_position_count > 0:
        changes.append(
            f"open strategy exposure is now present ({_same_underlying_conflict_side_profile_label(current_position_profile)})"
        )
    elif current_position_profile == "BOTH" and previous_position_profile != "BOTH":
        changes.append("opposite-side open strategy exposure is now present")
    elif current_position_count > 0 and previous_position_count > 0 and current_position_profile != previous_position_profile:
        changes.append(
            "open strategy exposure shifted from "
            f"{_same_underlying_conflict_side_profile_label(previous_position_profile)} to "
            f"{_same_underlying_conflict_side_profile_label(current_position_profile)}"
        )
    elif current_position_count > previous_position_count:
        changes.append("open strategy overlap increased")

    previous_pending_count = int(previous.get("pending_order_count") or 0)
    current_pending_count = int(current.get("pending_order_count") or 0)
    previous_pending_profile = str(previous.get("pending_order_side_profile") or "NONE").upper()
    current_pending_profile = str(current.get("pending_order_side_profile") or "NONE").upper()
    if previous_pending_count == 0 and current_pending_count > 0:
        changes.append("new pending-order overlap is now present")
    elif current_pending_profile == "BOTH" and previous_pending_profile != "BOTH":
        changes.append("opposite-side pending-order exposure is now present")
    elif current_pending_count > 0 and previous_pending_count > 0 and current_pending_profile != previous_pending_profile:
        changes.append(
            "pending-order exposure shifted from "
            f"{_same_underlying_conflict_side_profile_label(previous_pending_profile)} to "
            f"{_same_underlying_conflict_side_profile_label(current_pending_profile)}"
        )
    elif current_pending_count > previous_pending_count:
        changes.append("pending-order overlap increased")

    previous_broker_position_count = int(previous.get("broker_position_count") or 0)
    current_broker_position_count = int(current.get("broker_position_count") or 0)
    previous_broker_order_count = int(previous.get("broker_order_count") or 0)
    current_broker_order_count = int(current.get("broker_order_count") or 0)
    if previous_broker_position_count == 0 and current_broker_position_count > 0:
        changes.append("broker-reported position exposure is now part of this conflict")
    elif current_broker_position_count > previous_broker_position_count:
        changes.append("broker-reported position exposure increased")
    if previous_broker_order_count == 0 and current_broker_order_count > 0:
        changes.append("broker-reported open orders are now part of this conflict")
    elif current_broker_order_count > previous_broker_order_count:
        changes.append("broker-reported open-order overlap increased")

    if current.get("operator_action_required") is True and previous.get("operator_action_required") is not True:
        changes.append("operator action is now required before allowing more exposure")

    deduped_changes: list[str] = []
    seen: set[str] = set()
    for change in changes:
        if change not in seen:
            seen.add(change)
            deduped_changes.append(change)
    return deduped_changes


def _same_underlying_conflict_reopened_reason(
    *,
    instrument: str,
    previous_material_state: dict[str, Any] | None,
    current_material_state: dict[str, Any],
) -> str:
    changed_fields = _same_underlying_conflict_reopen_changes(previous_material_state, current_material_state)
    if not changed_fields:
        return ""
    if len(changed_fields) == 1:
        changed_summary = changed_fields[0]
    elif len(changed_fields) == 2:
        changed_summary = f"{changed_fields[0]} and {changed_fields[1]}"
    else:
        changed_summary = f"{', '.join(changed_fields[:-1])}, and {changed_fields[-1]}"
    return (
        f"Conflict review auto-reopened for {instrument} because {changed_summary}. "
        "This changes practical same-underlying exposure and needs fresh operator review before allowing more entries."
    )


def _same_underlying_conflict_review_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return {
        "acknowledged_count": sum(1 for row in rows if row.get("acknowledged") is True),
        "holding_count": sum(1 for row in rows if row.get("hold_new_entries") is True),
        "hold_expired_count": sum(1 for row in rows if row.get("hold_expired") is True),
        "stale_count": sum(1 for row in rows if str(row.get("review_state_status") or "").upper() == "STALE"),
        "blocking_unacknowledged_count": sum(
            1
            for row in rows
            if str(row.get("severity") or "").upper() == "BLOCKING" and row.get("acknowledged") is not True
        ),
        "expired_live_conflict_count": sum(
            1
            for row in rows
            if row.get("hold_expired") is True and str(row.get("instrument") or "").strip()
        ),
        "instruments_on_hold": sorted(
            str(row.get("instrument") or "")
            for row in rows
            if row.get("hold_new_entries") is True
        ),
    }


def _same_underlying_conflict_lookup(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        for strategy_id in list(row.get("standalone_strategy_ids") or []):
            key = str(strategy_id or "").strip()
            if key:
                lookup[key] = row
    return lookup


def _same_underlying_entry_block_lookup(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("event_type") or "") != "entry_blocked_by_same_underlying_hold":
            continue
        strategy_id = str(row.get("blocked_standalone_strategy_id") or "").strip()
        if not strategy_id:
            ids = [str(value or "").strip() for value in list(row.get("standalone_strategy_ids") or []) if str(value or "").strip()]
            strategy_id = ids[0] if ids else ""
        if strategy_id and strategy_id not in lookup:
            lookup[strategy_id] = row
    return lookup


def _annotate_same_underlying_conflict_metadata(
    rows: Sequence[dict[str, Any]],
    conflict_lookup: dict[str, dict[str, Any]],
    entry_block_lookup: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    block_lookup = dict(entry_block_lookup or {})
    for row in rows:
        strategy_id = str(row.get("standalone_strategy_id") or row.get("strategy_key") or "").strip()
        conflict = conflict_lookup.get(strategy_id) or {}
        latest_block = block_lookup.get(strategy_id) or {}
        annotated.append(
            {
                **row,
                "same_underlying_conflict_present": bool(conflict),
                "same_underlying_conflict_instrument": conflict.get("instrument"),
                "same_underlying_conflict_kind": conflict.get("conflict_kind"),
                "same_underlying_conflict_severity": conflict.get("severity"),
                "same_underlying_conflict_reason": conflict.get("conflict_reason"),
                "same_underlying_conflict_operator_action_required": conflict.get("operator_action_required", False),
                "same_underlying_conflict_execution_risk": conflict.get("execution_risk", False),
                "same_underlying_conflict_observational_only": conflict.get("observational_only", False),
                "same_underlying_conflict_broker_overlap_present": conflict.get("broker_overlap_present", False),
                "same_underlying_conflict_overlap_scope": conflict.get("overlap_scope"),
                "same_underlying_conflict_review_state": conflict.get("review_state_status"),
                "same_underlying_hold_new_entries": conflict.get("hold_new_entries", False),
                "same_underlying_hold_reason": conflict.get("hold_reason"),
                "same_underlying_entry_block_effective": conflict.get("entry_hold_effective", False),
                "same_underlying_latest_event_type": latest_block.get("event_type"),
                "same_underlying_latest_event_at": latest_block.get("occurred_at"),
                "same_underlying_latest_entry_blocked_at": latest_block.get("occurred_at"),
                "same_underlying_latest_entry_blocked_reason": latest_block.get("blocked_reason") or latest_block.get("note"),
            }
        )
    return annotated


def _build_runtime_registry_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    unique_instruments = {
        str(row.get("instrument") or "").strip().upper()
        for row in rows
        if row.get("instrument")
    }
    return {
        "configured_standalone_strategies": len(rows),
        "runtime_instances_present": sum(1 for row in rows if row.get("runtime_instance_present") is True),
        "runtime_states_loaded": sum(1 for row in rows if row.get("runtime_state_loaded") is True),
        "can_process_bars": sum(1 for row in rows if row.get("can_process_bars") is True),
        "enabled_strategies": sum(1 for row in rows if row.get("enabled") is True),
        "active_instrument_count": len(unique_instruments),
        "same_underlying_ambiguity_count": sum(1 for row in rows if row.get("same_underlying_ambiguity") is True),
    }


def _build_strategy_runtime_summary(
    *,
    runtime_registry: dict[str, Any],
    strategy_rows: Sequence[dict[str, Any]],
    audit_rows: Sequence[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    registry_rows = list(runtime_registry.get("rows") or [])
    runtime_summary = dict(runtime_registry.get("summary") or {})
    runtime_summary = {
        "configured_standalone_strategies": runtime_summary.get("configured_standalone_strategies", len(registry_rows) or len(strategy_rows)),
        "runtime_instances_present": runtime_summary.get("runtime_instances_present", len(registry_rows) or len(strategy_rows)),
        "runtime_states_loaded": runtime_summary.get("runtime_states_loaded", sum(1 for row in registry_rows if row.get("runtime_state_loaded") is True)),
        "can_process_bars": runtime_summary.get("can_process_bars", sum(1 for row in registry_rows if row.get("can_process_bars") is True) or len(strategy_rows)),
        "enabled_strategies": runtime_summary.get("enabled_strategies", sum(1 for row in registry_rows if row.get("enabled") is True) or len(strategy_rows)),
        "active_instrument_count": runtime_summary.get(
            "active_instrument_count",
            len({str(row.get("instrument") or "").strip().upper() for row in strategy_rows if row.get("instrument")}),
        ),
        "same_underlying_ambiguity_count": runtime_summary.get(
            "same_underlying_ambiguity_count",
            sum(1 for row in strategy_rows if row.get("same_underlying_ambiguity") is True),
        ),
    }
    strategy_lookup = {
        str(row.get("standalone_strategy_id") or row.get("strategy_key") or ""): row
        for row in strategy_rows
        if row.get("standalone_strategy_id") or row.get("strategy_key")
    }
    in_position_count = sum(
        1
        for row in strategy_rows
        if str(row.get("position_side") or "FLAT").upper() not in {"", "FLAT", "UNKNOWN"}
    )
    fault_or_blocker_count = sum(
        1
        for row in audit_rows
        if row.get("latest_fault_or_blocker")
        or str(row.get("current_strategy_status") or "").upper().startswith("FAULT")
        or str(row.get("audit_verdict") or "") in {"SETUP_GATED", "SURFACING_MISMATCH_SUSPECTED"}
    )
    active_strategy_count = sum(1 for row in strategy_rows if strategy_lookup.get(str(row.get("standalone_strategy_id") or "")) is not None)
    return {
        "generated_at": generated_at,
        "configured_standalone_strategies": runtime_summary["configured_standalone_strategies"],
        "runtime_instances_present": runtime_summary["runtime_instances_present"],
        "runtime_states_loaded": runtime_summary["runtime_states_loaded"],
        "can_process_bars": runtime_summary["can_process_bars"],
        "enabled_strategies": runtime_summary["enabled_strategies"],
        "active_instrument_count": runtime_summary["active_instrument_count"],
        "same_underlying_ambiguity_count": runtime_summary["same_underlying_ambiguity_count"],
        "in_position_strategies": in_position_count,
        "strategies_with_faults_or_blockers": fault_or_blocker_count,
        "performance_rows_present": len(strategy_rows),
        "auditable_rows_present": len(audit_rows),
        "summary_line": (
            f"{runtime_summary['configured_standalone_strategies']} configured standalone strategies, "
            f"{runtime_summary['runtime_instances_present']} runtime instances, "
            f"{in_position_count} currently in position, "
            f"{fault_or_blocker_count} with surfaced faults/blockers."
        ),
        "notes": [
            "Runtime truth reflects configured standalone strategy identities and whether runtime instances/state are present.",
            "Same-underlying ambiguity remains surfaced separately and is not silently netted or arbitrated here.",
        ],
        "active_strategy_count": active_strategy_count,
    }


def _strategy_attribution_family_label(*, source_family: str, side: str) -> str:
    normalized = str(source_family or "").strip()
    lower = normalized.lower()
    if "vwap" in lower or "reclaim" in lower:
        return "VWAP reclaim"
    if "bear" in lower or "short" in lower:
        return "Bear Snap"
    if "bull" in lower or "long" in lower:
        return "Bull Snap"
    if side.upper() in {"LONG", "BUY"}:
        return "Bull Snap"
    if side.upper() in {"SHORT", "SELL"}:
        return "Bear Snap"
    return normalized or "UNKNOWN"


def _strategy_max_drawdown_with_open_unrealized(
    ledger: Sequence[Any],
    current_unrealized: Decimal | None,
) -> Decimal:
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for row in ledger:
        cumulative += row.net_pnl
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    if current_unrealized is not None:
        current_equity = cumulative + current_unrealized
        drawdown = peak - current_equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _strategy_history_session_bucket(entry_session_phase: str | None) -> str:
    phase = str(entry_session_phase or "").strip().upper()
    if phase in STRATEGY_HISTORY_SESSION_BUCKETS:
        return phase
    return "UNKNOWN"


def _filled_entry_history_rows(
    order_intent_rows: Sequence[dict[str, Any]],
    fill_rows: Sequence[dict[str, Any]],
    *,
    bar_index_by_id: dict[str, int],
) -> list[dict[str, Any]]:
    fills_by_intent_id = {
        str(row.get("order_intent_id")): row
        for row in fill_rows
        if row.get("order_intent_id")
    }
    rows: list[dict[str, Any]] = []
    for intent in order_intent_rows:
        intent_type = str(intent.get("intent_type") or "").upper()
        if intent_type not in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
            continue
        order_intent_id = str(intent.get("order_intent_id") or "")
        if not order_intent_id:
            continue
        fill = fills_by_intent_id.get(order_intent_id)
        if fill is None:
            continue
        entry_timestamp = str(fill.get("fill_timestamp") or intent.get("created_at") or "")
        if not entry_timestamp:
            continue
        entry_dt = _parse_iso_datetime(entry_timestamp)
        phase = label_session_phase(entry_dt) if entry_dt is not None else "UNKNOWN"
        bar_id = str(intent.get("bar_id") or "") or None
        rows.append(
            {
                "order_intent_id": order_intent_id,
                "entry_timestamp": entry_timestamp,
                "entry_dt": entry_dt,
                "entry_session_phase": phase,
                "entry_session_bucket": _strategy_history_session_bucket(phase),
                "signal_family": intent.get("reason_code"),
                "bar_id": bar_id,
                "bar_index": bar_index_by_id.get(bar_id) if bar_id else None,
                "day_of_week": entry_dt.strftime("%A") if entry_dt is not None else "UNKNOWN",
            }
        )
    rows.sort(key=lambda row: str(row.get("entry_timestamp") or ""))
    return rows


def _decode_signal_audit_row(row: dict[str, Any], *, bars_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    payload = _json_dict_from_text(row.get("payload_json"))
    long_source = payload.get("long_entry_source")
    short_source = payload.get("short_entry_source")
    signal_family = long_source or short_source or _signal_candidate_label(payload)
    bar_id = str(row.get("bar_id") or "") or None
    bar_row = bars_by_id.get(bar_id or "")
    timestamp = (
        (bar_row or {}).get("end_ts")
        or row.get("created_at")
    )
    raw_setup_candidate = any(
        bool(payload.get(key))
        for key in (
            "long_entry_raw",
            "short_entry_raw",
            "recent_long_setup",
            "recent_short_setup",
            "bull_snap_turn_candidate",
            "bear_snap_turn_candidate",
            "asia_reclaim_bar_raw",
            "derivative_bear_turn_candidate",
        )
    )
    return {
        "bar_id": bar_id,
        "timestamp": timestamp,
        "signal_family": signal_family,
        "long_entry_raw": bool(payload.get("long_entry_raw")),
        "short_entry_raw": bool(payload.get("short_entry_raw")),
        "long_entry": bool(payload.get("long_entry")),
        "short_entry": bool(payload.get("short_entry")),
        "recent_long_setup": bool(payload.get("recent_long_setup")),
        "recent_short_setup": bool(payload.get("recent_short_setup")),
        "actionable_entry": bool(payload.get("long_entry") or payload.get("short_entry")),
        "raw_setup_candidate": raw_setup_candidate,
        "payload": payload,
    }


def _json_dict_from_text(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _signal_candidate_label(payload: dict[str, Any]) -> str | None:
    candidates = (
        ("asia_reclaim_bar_raw", "asiaVwapReclaim"),
        ("bull_snap_turn_candidate", "bullSnap"),
        ("bear_snap_turn_candidate", "bearSnap"),
        ("derivative_bear_turn_candidate", "derivativeBearTurn"),
    )
    for key, label in candidates:
        if payload.get(key):
            return label
    return None


def _rows_for_session_date(rows: Sequence[dict[str, Any]], session_date: str | None, *timestamp_fields: str) -> list[dict[str, Any]]:
    if not session_date:
        return list(rows)
    matched: list[dict[str, Any]] = []
    for row in rows:
        for field in timestamp_fields:
            value = row.get(field)
            if value and str(value)[:10] == session_date:
                matched.append(dict(row))
                break
    return matched


def _audit_open_broker_order_id(
    latest_intent: dict[str, Any] | None,
    latest_fill: dict[str, Any] | None,
    lane_row: dict[str, Any],
) -> str | None:
    if latest_intent is None:
        return None
    latest_intent_broker_order_id = str(latest_intent.get("broker_order_id") or "") or None
    latest_intent_status = str(latest_intent.get("order_status") or "").upper()
    latest_fill_broker_order_id = str((latest_fill or {}).get("broker_order_id") or "") or None
    if latest_intent_broker_order_id and latest_intent_status not in {"FILLED", "CANCELED", "CANCELLED", "REJECTED"}:
        return latest_intent_broker_order_id
    if lane_row.get("open_order_count") and latest_intent_broker_order_id and latest_intent_broker_order_id != latest_fill_broker_order_id:
        return latest_intent_broker_order_id
    return None


def _audit_latest_fault_or_blocker(lane_row: dict[str, Any]) -> str | None:
    for value in (
        lane_row.get("fault_code"),
        lane_row.get("same_underlying_hold_reason"),
        lane_row.get("halt_reason"),
        lane_row.get("eligibility_reason"),
        lane_row.get("eligibility_detail"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _signal_intent_fill_audit_verdict(
    *,
    processed_bar_count: int,
    bar_count_in_window: int,
    actionable_entry_signal_count: int,
    total_intent_count: int,
    total_fill_count: int,
    gating_state: dict[str, Any],
    strategy_row_exists: bool,
    surfaced_trade_log_count: int,
    require_surface_consistency: bool = True,
) -> tuple[str, str]:
    if processed_bar_count == 0 and actionable_entry_signal_count == 0 and total_intent_count == 0 and total_fill_count == 0:
        return ("INSUFFICIENT_HISTORY", "No processed bars or persisted audit artifacts are available yet for this lane.")
    if total_fill_count > 0:
        position_side = str(gating_state.get("position_side") or "FLAT").upper()
        if require_surface_consistency and (not strategy_row_exists or (position_side == "FLAT" and surfaced_trade_log_count == 0)):
            return (
                "SURFACING_MISMATCH_SUSPECTED",
                "A fill is persisted, but the strategy performance or trade-log surfacing does not line up yet for this lane.",
            )
        return ("FILLED", "A persisted fill exists for this lane in the inspected window.")
    if total_intent_count > 0:
        return (
            "INTENT_NO_FILL_YET",
            "An order intent was created, but no fill is persisted yet. In replay/paper, the next due bar open may still be pending.",
        )
    if actionable_entry_signal_count > 0:
        gate_reason = _audit_gate_reason(gating_state)
        if gate_reason is not None:
            return ("SETUP_GATED", gate_reason)
        return (
            "SURFACING_MISMATCH_SUSPECTED",
            "An actionable entry signal is persisted, but no intent followed even though the visible gates do not explain the gap.",
        )
    if bar_count_in_window == 0:
        return (
            "INSUFFICIENT_HISTORY",
            "No processed bars were recorded in the inspected window, so the dashboard cannot yet judge whether this lane was simply quiet.",
        )
    return (
        "NO_SETUP_OBSERVED",
        "Processed completed bars exist in the inspected window, but no actionable entry signal or order intent was persisted.",
    )


def _audit_gate_reason(gating_state: dict[str, Any]) -> str | None:
    fault_code = str(gating_state.get("latest_fault_or_blocker") or "").strip()
    if fault_code and str(gating_state.get("current_strategy_status") or "").upper().startswith("FAULT"):
        return f"A setup was observed, but no intent was created because the lane is faulted: {fault_code}."
    if str(gating_state.get("risk_state") or "OK").upper() not in {"OK", "READY", "CLEAR", ""} and gating_state.get("halt_reason"):
        return f"A setup was observed, but risk gating blocked entries: {gating_state.get('halt_reason')}."
    if gating_state.get("same_underlying_entry_hold") is True:
        hold_reason = str(gating_state.get("same_underlying_hold_reason") or "").strip()
        if hold_reason:
            return f"A setup was observed, but same-underlying conflict hold blocked new entries: {hold_reason}"
        return "A setup was observed, but same-underlying conflict hold blocked new entries."
    if gating_state.get("operator_halt") is True:
        return "A setup was observed, but operator halt was active, so no order intent was allowed."
    if gating_state.get("entries_enabled") is False:
        return "A setup was observed, but entries were disabled for this lane, so no order intent was created."
    if gating_state.get("warmup_complete") is False:
        return "A setup was observed, but warmup was not complete yet, so the lane could not emit an order intent."
    if str(gating_state.get("position_side") or "FLAT").upper() != "FLAT":
        return f"A setup was observed, but the lane was already {gating_state.get('position_side')}, so no new entry intent was created."
    if str(gating_state.get("current_strategy_status") or "UNKNOWN").upper() not in {"READY"}:
        return f"A setup was observed, but the strategy status was {gating_state.get('current_strategy_status')}, so the lane was not entry-ready."
    eligibility_reason = str(gating_state.get("eligibility_reason") or "").strip()
    if eligibility_reason and eligibility_reason != "no_new_completed_bar":
        detail = str(gating_state.get("eligibility_detail") or "").strip()
        if detail:
            return f"A setup was observed, but the lane was gated by {eligibility_reason}: {detail}."
        return f"A setup was observed, but the lane was gated by {eligibility_reason}."
    return None


def _signal_intent_fill_operator_explanation(verdict: str) -> str:
    explanations = {
        "NO_SETUP_OBSERVED": "Nothing fired because no actionable setup was persisted on any completed bar in the inspected window.",
        "SETUP_GATED": "A setup was seen, but the lane gates blocked order-intent creation.",
        "INTENT_NO_FILL_YET": "An intent exists, but a fill has not been persisted yet.",
        "FILLED": "A fill exists in persisted lane history for the inspected window.",
        "SURFACING_MISMATCH_SUSPECTED": "Persisted intents/fills exist, but the strategy performance or trade-log surfacing does not fully line up yet.",
        "INSUFFICIENT_HISTORY": "There is not enough processed-bar or artifact history in the inspected window to judge the lane yet.",
    }
    return explanations.get(verdict, "Audit verdict unavailable.")


def _temporary_paper_trade_log_rows(
    *,
    non_approved_lanes: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lane_row in list(non_approved_lanes.get("rows") or []):
        if not _is_temporary_paper_strategy_row(lane_row):
            continue
        trades_path_value = _nested_get(lane_row, "artifacts", "trades")
        if not trades_path_value:
            continue
        trades_path = Path(str(trades_path_value))
        if not trades_path.exists():
            continue
        strategy_name = str(lane_row.get("display_name") or lane_row.get("branch") or lane_row.get("lane_id") or "Temporary Paper Strategy")
        lane_id = str(lane_row.get("lane_id") or strategy_name)
        source_family = str(lane_row.get("source_family") or lane_row.get("observer_variant_id") or strategy_name)
        experimental_status = lane_row.get("experimental_status")
        for index, trade in enumerate(_all_jsonl_rows(trades_path), start=1):
            instrument = str(trade.get("symbol") or lane_row.get("instrument") or "UNKNOWN")
            trade_pnl = _decimal_or_none(trade.get("realized_pnl") or trade.get("net_pnl"))
            gross_pnl = _decimal_or_none(trade.get("gross_pnl"))
            fees = _decimal_or_none(trade.get("fees_paid") or trade.get("fees"))
            slippage = _decimal_or_none(trade.get("slippage_cost") or trade.get("slippage"))
            side = str(trade.get("direction") or lane_row.get("position_side") or lane_row.get("side") or "")
            attribution_family_label = _strategy_attribution_family_label(
                source_family=str(trade.get("setup_family") or source_family),
                side=side,
            )
            trade_id = str(trade.get("trade_id") or f"{lane_id}:{index}")
            row_id = trade_id if trade_id.startswith(f"{lane_id}:") else f"{lane_id}:{trade_id}"
            rows.append(
                {
                    "id": row_id,
                    "strategy_key": lane_id,
                    "standalone_strategy_id": lane_id,
                    "legacy_strategy_key": None,
                    "lane_id": lane_id,
                    "strategy_name": strategy_name,
                    "instrument": instrument,
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": str(lane_row.get("strategy_family") or source_family),
                    "standalone_strategy_root": str(lane_row.get("strategy_identity_root") or strategy_name),
                    "standalone_strategy_label": strategy_name,
                    "paper_strategy_class": "temporary_paper_strategy",
                    "metrics_bucket": "experimental_temporary_paper",
                    "paper_only": True,
                    "non_approved": True,
                    "experimental_status": experimental_status,
                    "signal_family_label": attribution_family_label,
                    "trade_id": trade_id,
                    "side": side,
                    "entry_timestamp": trade.get("entry_timestamp"),
                    "exit_timestamp": trade.get("exit_timestamp"),
                    "entry_price": _decimal_to_string(_decimal_or_none(trade.get("entry_price"))),
                    "exit_price": _decimal_to_string(_decimal_or_none(trade.get("exit_price"))),
                    "quantity": trade.get("quantity") or 1,
                    "realized_pnl": _decimal_to_string(trade_pnl),
                    "gross_pnl": _decimal_to_string(gross_pnl if gross_pnl is not None else trade_pnl),
                    "fees": _decimal_to_string(fees),
                    "slippage": _decimal_to_string(slippage),
                    "exit_reason": trade.get("exit_reason"),
                    "signal_family": trade.get("setup_family") or source_family,
                    "entry_session_phase": label_session_phase(_parse_iso_datetime(trade.get("entry_timestamp"))) if trade.get("entry_timestamp") else None,
                    "exit_session_phase": label_session_phase(_parse_iso_datetime(trade.get("exit_timestamp"))) if trade.get("exit_timestamp") else None,
                    "status": "CLOSED" if trade.get("exit_timestamp") else "OPEN",
                    "quality_bucket": trade.get("quality_bucket"),
                    "quality_bucket_policy": trade.get("quality_bucket_policy") or lane_row.get("quality_bucket_policy"),
                }
            )
    rows.sort(key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""), reverse=True)
    return rows


def _quant_strategy_performance_payload(
    *,
    repo_root: Path,
    approved_quant_baselines: dict[str, Any],
    session_date: str,
    current_session: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    trade_log_rows: list[dict[str, Any]] = []
    execution_likelihood_rows: list[dict[str, Any]] = []
    limited_history_rows: list[str] = []

    for baseline_row in list(approved_quant_baselines.get("rows") or []):
        approved_scope = baseline_row.get("approved_scope") or {}
        lane_id = str(baseline_row.get("lane_id") or "")
        lane_name = str(baseline_row.get("lane_name") or lane_id or "unknown_quant_lane")
        source_family = str(approved_scope.get("family") or lane_name or "UNKNOWN")
        point_value = _decimal_or_none(approved_scope.get("point_value"))
        lane_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_id

        for instrument in list(approved_scope.get("symbols") or []):
            trades = [
                row
                for row in _all_jsonl_rows(lane_dir / "trades.jsonl")
                if str(row.get("symbol") or "") == str(instrument)
            ]
            signals = [
                row
                for row in _all_jsonl_rows(lane_dir / "signals.jsonl")
                if str(row.get("symbol") or "") == str(instrument)
            ]
            intents = [
                row
                for row in (_all_jsonl_rows(lane_dir / "order_intents.jsonl") or _all_jsonl_rows(lane_dir / "intents.jsonl"))
                if str(row.get("symbol") or "") == str(instrument)
            ]
            fills = [
                row
                for row in _all_jsonl_rows(lane_dir / "fills.jsonl")
                if str(row.get("symbol") or "") == str(instrument)
            ]

            identity = build_standalone_strategy_identity(
                instrument=instrument,
                lane_id=lane_id,
                lane_name=lane_name,
                source_family=source_family,
                strategy_name=lane_name,
            )
            strategy_key = identity["standalone_strategy_id"]
            explicit_trade_pnls = [_decimal_or_none(row.get("realized_pnl") or row.get("net_pnl")) for row in trades]
            realized_values = [value for value in explicit_trade_pnls if value is not None]
            realized_pnl = sum(realized_values, Decimal("0")) if trades and len(realized_values) == len(trades) else None
            day_values = [
                value
                for row, value in zip(trades, explicit_trade_pnls)
                if value is not None and str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")[:10] == session_date
            ]
            day_pnl = sum(day_values, Decimal("0")) if trades and len(realized_values) == len(trades) else None
            cumulative_pnl = realized_pnl
            max_drawdown = _quant_strategy_max_drawdown(realized_values) if trades and len(realized_values) == len(trades) else None
            latest_signal = _latest_row(signals, "signal_timestamp", "entry_timestamp_planned")
            latest_trade = _latest_row(trades, "exit_timestamp", "entry_timestamp", "signal_timestamp")
            latest_intent = _latest_row(intents, "created_at", "intent_timestamp", "timestamp")
            latest_fill = _latest_row(fills, "fill_timestamp", "timestamp")
            latest_activity = max(
                [
                    value
                    for value in [
                        _row_timestamp(latest_signal, "signal_timestamp", "entry_timestamp_planned"),
                        _row_timestamp(latest_trade, "exit_timestamp", "entry_timestamp", "signal_timestamp"),
                        _row_timestamp(latest_intent, "created_at", "intent_timestamp", "timestamp"),
                        _row_timestamp(latest_fill, "fill_timestamp", "timestamp"),
                    ]
                    if value
                ],
                default=None,
            )
            quant_entry_rows = _quant_entry_history_rows(trades)
            entry_phase_counts = Counter(str(row.get("entry_session_phase") or "UNKNOWN") for row in quant_entry_rows)
            session_bucket_counts = {
                bucket: sum(1 for row in quant_entry_rows if row.get("entry_session_bucket") == bucket)
                for bucket in STRATEGY_HISTORY_SESSION_BUCKETS
            }
            day_of_week_counts = Counter(str(row.get("day_of_week") or "UNKNOWN") for row in quant_entry_rows)
            entry_timestamps = [row.get("entry_dt") for row in quant_entry_rows if row.get("entry_dt") is not None]
            median_elapsed_seconds = _median_value(
                [
                    (entry_timestamps[index] - entry_timestamps[index - 1]).total_seconds()
                    for index in range(1, len(entry_timestamps))
                ]
            )
            most_common_session_bucket, most_common_session_bucket_count = _strategy_most_common_label(
                Counter({bucket: count for bucket, count in session_bucket_counts.items() if count > 0}),
                default="UNKNOWN",
            )
            most_common_entry_phase, most_common_entry_phase_count = _strategy_most_common_label(
                entry_phase_counts,
                default="UNKNOWN",
            )
            expected_fire_cadence = _strategy_expected_fire_cadence_label(len(quant_entry_rows), median_elapsed_seconds)
            most_likely_next_window = (
                f"{most_common_entry_phase} ({most_common_entry_phase_count}/{len(quant_entry_rows)} entries)"
                if len(quant_entry_rows) >= 3 and most_common_entry_phase != "UNKNOWN"
                else (
                    f"{most_common_session_bucket} ({most_common_session_bucket_count}/{len(quant_entry_rows)} entries)"
                    if len(quant_entry_rows) >= 3 and most_common_session_bucket != "UNKNOWN"
                    else "Insufficient history"
                )
            )
            last_fire_timestamp = str(quant_entry_rows[-1]["entry_timestamp"]) if quant_entry_rows else None
            last_fire_dt = _parse_iso_datetime(last_fire_timestamp)
            days_since_last_fire = (
                (datetime.now(timezone.utc).date() - last_fire_dt.astimezone(timezone.utc).date()).days
                if last_fire_dt is not None
                else None
            )
            interpretation_state, interpretation = _strategy_operator_interpretation(
                entry_count=len(quant_entry_rows),
                expected_fire_cadence=expected_fire_cadence,
                current_session=current_session,
                most_common_entry_phase=most_common_entry_phase,
                most_common_session_bucket=most_common_session_bucket,
                entries_enabled=str(baseline_row.get("probation_status") or "unknown").lower() not in {"review", "suspend", "downgraded"},
                operator_halt=False,
                same_underlying_entry_hold=False,
            )
            attribution_family_label = _strategy_attribution_family_label(
                source_family=source_family,
                side=str(approved_scope.get("direction") or ""),
            )
            strategy_name = lane_name
            if len(quant_entry_rows) <= 1:
                limited_history_rows.append(f"{strategy_name} / {instrument}")

            rows.append(
                {
                    "id": strategy_key,
                    "strategy_key": strategy_key,
                    "standalone_strategy_id": strategy_key,
                    "legacy_strategy_key": None,
                    "lane_id": lane_id,
                    "strategy_name": strategy_name,
                    "instrument": str(instrument),
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": identity["strategy_family"],
                    "standalone_strategy_root": identity["standalone_strategy_root"],
                    "standalone_strategy_label": identity["standalone_strategy_label"],
                    "signal_family_label": attribution_family_label,
                    "status": f"APPROVED_QUANT_{str(baseline_row.get('probation_status') or 'unknown').upper()}",
                    "position_side": "FLAT",
                    "entry_timestamp": last_fire_timestamp,
                    "entry_price": None,
                    "last_mark": None,
                    "realized_pnl": _decimal_to_string(realized_pnl),
                    "unrealized_pnl": None,
                    "day_pnl": _decimal_to_string(day_pnl),
                    "cumulative_pnl": _decimal_to_string(cumulative_pnl),
                    "max_drawdown": _decimal_to_string(max_drawdown),
                    "trade_count": len(trades),
                    "latest_fill_timestamp": _row_timestamp(latest_fill, "fill_timestamp", "timestamp", "entry_timestamp", "exit_timestamp"),
                    "latest_activity_timestamp": latest_activity,
                    "risk_state": "OK" if str(baseline_row.get("probation_status") or "").lower() not in {"review", "suspend", "downgraded"} else "BLOCKED",
                    "halt_reason": None,
                    "session_restriction": "/".join(list(approved_scope.get("allowed_sessions") or [])) or None,
                    "entries_enabled": str(baseline_row.get("probation_status") or "").lower() not in {"review", "suspend", "downgraded"},
                    "operator_halt": False,
                    "history_start_timestamp": min(
                        (
                            str(row.get("entry_timestamp") or row.get("signal_timestamp") or "")
                            for row in trades
                            if row.get("entry_timestamp") or row.get("signal_timestamp")
                        ),
                        default=None,
                    ),
                    "history_end_timestamp": latest_activity,
                    "entry_count": len(quant_entry_rows),
                    "total_signal_count": len(signals),
                    "total_signal_count_scope": "Derived from persisted approved-quant signal artifacts when present.",
                    "entries_by_session_bucket": session_bucket_counts,
                    "session_bucket_summary": _strategy_session_bucket_summary(session_bucket_counts),
                    "entries_by_day_of_week": dict(day_of_week_counts),
                    "day_of_week_summary": _strategy_day_of_week_summary(day_of_week_counts),
                    "median_bars_between_entries": None,
                    "median_bars_between_entries_label": "Unavailable",
                    "median_elapsed_between_entries_seconds": median_elapsed_seconds,
                    "median_elapsed_between_entries_label": _format_strategy_gap_label(median_elapsed_seconds) or "Unavailable",
                    "most_common_session_bucket": most_common_session_bucket,
                    "most_common_entry_phase": most_common_entry_phase,
                    "most_likely_next_window": most_likely_next_window,
                    "expected_fire_cadence": expected_fire_cadence,
                    "last_fire_timestamp": last_fire_timestamp,
                    "days_since_last_fire": days_since_last_fire,
                    "operator_interpretation_state": interpretation_state,
                    "operator_interpretation": interpretation,
                    "current_session": current_session,
                    "ledger_history_scope": "Quant strategy rows use persisted approved-quant artifacts for this standalone strategy identity.",
                    "day_scope": f"Day P&L is derived only from explicit persisted trade P/L inside the {session_date} inspection date when available.",
                    "unrealized_scope": "Unavailable for approved-quant baselines in this payload unless an explicit current mark/reference price is persisted.",
                    "max_drawdown_method": "Computed from explicit persisted realized trade P/L only when those values are present in the approved-quant trade archive.",
                }
            )

            for index, trade in enumerate(trades, start=1):
                trade_pnl = _decimal_or_none(trade.get("realized_pnl") or trade.get("net_pnl"))
                trade_log_rows.append(
                    {
                        "id": f"{strategy_key}:quant:{index}",
                        "strategy_key": strategy_key,
                        "standalone_strategy_id": strategy_key,
                        "legacy_strategy_key": None,
                        "lane_id": lane_id,
                        "strategy_name": strategy_name,
                        "instrument": str(instrument),
                        "family": source_family,
                        "source_family": source_family,
                        "strategy_family": identity["strategy_family"],
                        "standalone_strategy_root": identity["standalone_strategy_root"],
                        "standalone_strategy_label": identity["standalone_strategy_label"],
                        "signal_family_label": attribution_family_label,
                        "trade_id": str(trade.get("trade_id") or index),
                        "side": trade.get("direction"),
                        "entry_timestamp": trade.get("entry_timestamp"),
                        "exit_timestamp": trade.get("exit_timestamp"),
                        "entry_price": trade.get("entry_price"),
                        "exit_price": trade.get("exit_price"),
                        "quantity": trade.get("quantity") or 1,
                        "realized_pnl": _decimal_to_string(trade_pnl),
                        "gross_pnl": _decimal_to_string(trade_pnl),
                        "fees": None,
                        "slippage": None,
                        "exit_reason": trade.get("exit_reason"),
                        "signal_family": trade.get("setup_family") or source_family,
                        "entry_session_phase": label_session_phase(_parse_iso_datetime(trade.get("entry_timestamp"))) if trade.get("entry_timestamp") else None,
                        "exit_session_phase": label_session_phase(_parse_iso_datetime(trade.get("exit_timestamp"))) if trade.get("exit_timestamp") else None,
                        "status": "CLOSED" if trade.get("exit_timestamp") else "OPEN",
                    }
                )

            execution_likelihood_rows.append(
                {
                    "id": strategy_key,
                    "strategy_key": strategy_key,
                    "standalone_strategy_id": strategy_key,
                    "legacy_strategy_key": None,
                    "lane_id": lane_id,
                    "strategy_name": strategy_name,
                    "instrument": str(instrument),
                    "family": source_family,
                    "source_family": source_family,
                    "strategy_family": identity["strategy_family"],
                    "standalone_strategy_root": identity["standalone_strategy_root"],
                    "standalone_strategy_label": identity["standalone_strategy_label"],
                    "signal_family_label": attribution_family_label,
                    "trade_count": len(trades),
                    "total_signal_count": len(signals),
                    "entry_count": len(quant_entry_rows),
                    "entries_by_session_bucket": session_bucket_counts,
                    "session_bucket_summary": _strategy_session_bucket_summary(session_bucket_counts),
                    "entries_by_day_of_week": dict(day_of_week_counts),
                    "day_of_week_summary": _strategy_day_of_week_summary(day_of_week_counts),
                    "median_bars_between_entries": None,
                    "median_bars_between_entries_label": "Unavailable",
                    "median_elapsed_between_entries_seconds": median_elapsed_seconds,
                    "median_elapsed_between_entries_label": _format_strategy_gap_label(median_elapsed_seconds) or "Unavailable",
                    "most_common_session_bucket": most_common_session_bucket,
                    "most_common_entry_phase": most_common_entry_phase,
                    "most_likely_next_window": most_likely_next_window,
                    "expected_fire_cadence": expected_fire_cadence,
                    "last_fire_timestamp": last_fire_timestamp,
                    "days_since_last_fire": days_since_last_fire,
                    "history_sufficient": len(quant_entry_rows) >= 3,
                    "current_session": current_session,
                    "operator_interpretation_state": interpretation_state,
                    "operator_interpretation": interpretation,
                }
            )

    return {
        "rows": rows,
        "trade_log": trade_log_rows,
        "execution_likelihood": execution_likelihood_rows,
        "warnings": {
            "missing_mark_rows": [],
            "limited_history_rows": limited_history_rows,
        },
    }


def _quant_entry_history_rows(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in trades:
        entry_timestamp = str(trade.get("entry_timestamp") or trade.get("signal_timestamp") or "")
        if not entry_timestamp:
            continue
        entry_dt = _parse_iso_datetime(entry_timestamp)
        phase = label_session_phase(entry_dt) if entry_dt is not None else "UNKNOWN"
        rows.append(
            {
                "entry_timestamp": entry_timestamp,
                "entry_dt": entry_dt,
                "entry_session_phase": phase,
                "entry_session_bucket": _strategy_history_session_bucket(phase),
                "day_of_week": entry_dt.strftime("%A") if entry_dt is not None else "UNKNOWN",
            }
        )
    rows.sort(key=lambda row: str(row.get("entry_timestamp") or ""))
    return rows


def _quant_strategy_max_drawdown(realized_values: Sequence[Decimal]) -> Decimal:
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for pnl in realized_values:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _quant_signal_intent_fill_audit_rows(
    *,
    repo_root: Path,
    approved_quant_baselines: dict[str, Any],
    operator_surface: dict[str, Any],
    session_date: str | None,
    current_session: str,
    strategy_rows: dict[str, dict[str, Any]],
    trade_log_counts_by_strategy: dict[str, int],
) -> list[dict[str, Any]]:
    lane_rows = [
        dict(row)
        for row in list(operator_surface.get("lane_rows") or [])
        if str(row.get("classification_tag") or row.get("classification") or "") == "approved_quant"
    ]
    baselines_by_lane_id = {
        str(row.get("lane_id") or ""): dict(row)
        for row in list(approved_quant_baselines.get("rows") or [])
        if row.get("lane_id")
    }
    rows: list[dict[str, Any]] = []
    for lane_row in lane_rows:
        lane_id = str(lane_row.get("lane_id") or "")
        instrument = str(lane_row.get("instrument") or "")
        baseline_row = baselines_by_lane_id.get(lane_id, {})
        approved_scope = baseline_row.get("approved_scope") or {}
        lane_dir = repo_root / "outputs" / "probationary_quant_baselines" / "lanes" / lane_id
        signals = [
            row for row in _all_jsonl_rows(lane_dir / "signals.jsonl")
            if not instrument or str(row.get("symbol") or "") == instrument
        ]
        processed_bars = [
            row for row in _all_jsonl_rows(lane_dir / "processed_bars.jsonl")
            if not instrument or str(row.get("symbol") or "") in {"", instrument}
        ]
        features = [
            row for row in _all_jsonl_rows(lane_dir / "features.jsonl")
            if not instrument or str(row.get("symbol") or "") in {"", instrument}
        ]
        trades = [
            row for row in _all_jsonl_rows(lane_dir / "trades.jsonl")
            if not instrument or str(row.get("symbol") or "") == instrument
        ]
        intents = [
            row for row in (_all_jsonl_rows(lane_dir / "order_intents.jsonl") or _all_jsonl_rows(lane_dir / "intents.jsonl"))
            if not instrument or str(row.get("symbol") or "") == instrument
        ]
        fills = [
            row for row in _all_jsonl_rows(lane_dir / "fills.jsonl")
            if not instrument or str(row.get("symbol") or "") == instrument
        ]
        window_processed_bars = _rows_for_session_date(processed_bars, session_date, "end_ts", "timestamp")
        window_signals = _rows_for_session_date(signals, session_date, "signal_timestamp", "entry_timestamp_planned")
        window_features = _rows_for_session_date(features, session_date, "created_at", "timestamp", "feature_timestamp")
        window_trades = _rows_for_session_date(trades, session_date, "entry_timestamp", "exit_timestamp", "signal_timestamp")
        window_intents = _rows_for_session_date(intents, session_date, "created_at", "intent_timestamp", "timestamp")
        window_fills = _rows_for_session_date(fills, session_date, "fill_timestamp", "timestamp")
        latest_processed_bar = _latest_row(processed_bars, "end_ts", "timestamp")
        latest_feature = _latest_row(features, "created_at", "timestamp", "feature_timestamp")
        latest_signal = _latest_row(signals, "signal_timestamp", "entry_timestamp_planned")
        latest_actionable_signal = _latest_row(
            [
                row for row in signals
                if row.get("signal_passed_flag") is True and not row.get("rejection_reason_code")
            ],
            "signal_timestamp",
            "entry_timestamp_planned",
        )
        latest_trade = _latest_row(trades, "entry_timestamp", "exit_timestamp", "signal_timestamp")
        latest_intent = _latest_row(window_intents, "created_at", "intent_timestamp", "timestamp")
        latest_fill = _latest_row(window_fills, "fill_timestamp", "timestamp")
        if latest_fill is None and latest_trade is not None:
            latest_fill = {
                "fill_timestamp": latest_trade.get("entry_timestamp") or latest_trade.get("exit_timestamp"),
                "fill_price": latest_trade.get("entry_price"),
                "broker_order_id": latest_trade.get("broker_order_id"),
                "symbol": latest_trade.get("symbol"),
                "direction": latest_trade.get("direction"),
            }
        actionable_entry_signal_count = sum(
            1
            for row in window_signals
            if row.get("signal_passed_flag") is True and not row.get("rejection_reason_code")
        )
        eligible_now = bool(lane_row.get("enabled")) and not bool(lane_row.get("blocked"))
        auditable_now = any(
            path.exists()
            for path in (
                lane_dir / "processed_bars.jsonl",
                lane_dir / "features.jsonl",
                lane_dir / "signals.jsonl",
                lane_dir / "trades.jsonl",
                lane_dir / "order_intents.jsonl",
                lane_dir / "intents.jsonl",
                lane_dir / "fills.jsonl",
            )
        )
        latest_fault_or_blocker = (
            str(latest_signal.get("rejection_reason_code") or "") if latest_signal is not None else ""
        ) or str(lane_row.get("warning_summary") or "") or None
        gating_state = {
            "current_strategy_status": str(lane_row.get("state") or baseline_row.get("probation_status") or "UNKNOWN").upper(),
            "entries_enabled": eligible_now,
            "operator_halt": False,
            "warmup_complete": None,
            "position_side": "FLAT",
            "open_broker_order_id": str((latest_intent or {}).get("broker_order_id") or "") or None,
            "latest_fault_or_blocker": latest_fault_or_blocker,
            "eligibility_reason": None if eligible_now else "quant_row_blocked",
            "eligibility_detail": latest_fault_or_blocker,
            "risk_state": "OK" if eligible_now else "BLOCKED",
            "halt_reason": latest_fault_or_blocker,
        }
        verdict, verdict_reason = _signal_intent_fill_audit_verdict(
            processed_bar_count=len(processed_bars),
            bar_count_in_window=len(window_processed_bars),
            actionable_entry_signal_count=actionable_entry_signal_count,
            total_intent_count=len(window_intents),
            total_fill_count=len(window_fills) if window_fills else (1 if latest_trade is not None and _rows_for_session_date([latest_trade], session_date, "entry_timestamp", "exit_timestamp", "signal_timestamp") else 0),
            gating_state=gating_state,
            strategy_row_exists=False,
            surfaced_trade_log_count=0,
            require_surface_consistency=False,
        )
        inspection_timestamps = [
            str(value)
            for value in (
                *[row.get("signal_timestamp") or row.get("entry_timestamp_planned") for row in window_signals],
                *[row.get("created_at") or row.get("intent_timestamp") or row.get("timestamp") for row in window_intents],
                *[row.get("fill_timestamp") or row.get("timestamp") for row in window_fills],
                *[row.get("entry_timestamp") or row.get("exit_timestamp") for row in window_trades],
            )
            if value
        ]
        family = str(approved_scope.get("family") or baseline_row.get("lane_name") or lane_row.get("display_name") or lane_id)
        identity = build_standalone_strategy_identity(
            instrument=instrument,
            lane_id=lane_id,
            lane_name=baseline_row.get("lane_name") or lane_row.get("display_name"),
            source_family=family,
            strategy_name=lane_row.get("display_name") or baseline_row.get("lane_name"),
        )
        strategy_key = identity["standalone_strategy_id"]
        performance_row = strategy_rows.get(strategy_key)
        surfaced_trade_log_count = trade_log_counts_by_strategy.get(strategy_key, 0)
        rows.append(
            {
                "id": strategy_key,
                "strategy_key": strategy_key,
                "standalone_strategy_id": strategy_key,
                "legacy_strategy_key": None,
                "lane_id": lane_id,
                "strategy_name": str(lane_row.get("display_name") or baseline_row.get("lane_name") or lane_id),
                "instrument": instrument or None,
                "family": family,
                "source_family": family,
                "strategy_family": identity["strategy_family"],
                "standalone_strategy_root": identity["standalone_strategy_root"],
                "standalone_strategy_label": identity["standalone_strategy_label"],
                "current_session": current_session or "UNKNOWN",
                "inspection_start_ts": min(inspection_timestamps + [str(row.get("end_ts") or row.get("timestamp") or "") for row in window_processed_bars if row.get("end_ts") or row.get("timestamp")]) if inspection_timestamps or window_processed_bars else None,
                "inspection_end_ts": max(inspection_timestamps + [str(row.get("end_ts") or row.get("timestamp") or "") for row in window_processed_bars if row.get("end_ts") or row.get("timestamp")]) if inspection_timestamps or window_processed_bars else None,
                "bar_count_in_window": len(window_processed_bars) if processed_bars else None,
                "last_processed_bar_id": (latest_processed_bar or {}).get("bar_id"),
                "last_processed_bar_end_ts": _row_timestamp(latest_processed_bar, "end_ts", "timestamp"),
                "processed_bar_count": len(processed_bars) if processed_bars else None,
                "last_feature_ts": _row_timestamp(latest_feature, "created_at", "timestamp", "feature_timestamp"),
                "last_signal_bar_id": None,
                "last_signal_timestamp": latest_signal.get("signal_timestamp") if latest_signal else None,
                "last_signal_family": family,
                "last_actionable_signal_timestamp": (
                    latest_actionable_signal.get("signal_timestamp") or latest_actionable_signal.get("entry_timestamp_planned")
                    if latest_actionable_signal
                    else None
                ),
                "last_actionable_signal_family": family if latest_actionable_signal else None,
                "last_long_entry_raw": None,
                "last_short_entry_raw": None,
                "last_long_entry": bool(latest_signal.get("signal_passed_flag")) if latest_signal and str(latest_signal.get("direction") or "").upper() == "LONG" else None,
                "last_short_entry": bool(latest_signal.get("signal_passed_flag")) if latest_signal and str(latest_signal.get("direction") or "").upper() == "SHORT" else None,
                "last_recent_long_setup": None,
                "last_recent_short_setup": None,
                "actionable_entry_signal_count": actionable_entry_signal_count,
                "raw_setup_candidate_count": None,
                "last_order_intent_id": (latest_intent or {}).get("order_intent_id") or (latest_intent or {}).get("intent_id"),
                "last_intent_timestamp": _row_timestamp(latest_intent, "created_at", "intent_timestamp", "timestamp"),
                "last_intent_type": (latest_intent or {}).get("intent_type"),
                "last_intent_reason_code": (latest_intent or {}).get("reason_code") or latest_fault_or_blocker,
                "total_intent_count": len(window_intents),
                "last_fill_timestamp": _row_timestamp(latest_fill, "fill_timestamp", "timestamp", "entry_timestamp", "exit_timestamp"),
                "last_fill_price": (latest_fill or {}).get("fill_price") or (latest_trade or {}).get("entry_price"),
                "last_fill_broker_order_id": (latest_fill or {}).get("broker_order_id"),
                "total_fill_count": len(window_fills) if window_fills else len(window_trades),
                "current_strategy_status": gating_state["current_strategy_status"],
                "eligible_now": eligible_now,
                "auditable_now": auditable_now,
                "entries_enabled": gating_state["entries_enabled"],
                "operator_halt": False,
                "warmup_complete": None,
                "position_side": "FLAT",
                "open_broker_order_id": gating_state["open_broker_order_id"],
                "latest_fault_or_blocker": latest_fault_or_blocker,
                "audit_verdict": verdict,
                "audit_reason": verdict_reason,
                "operator_explanation": _signal_intent_fill_operator_explanation(verdict),
                "latest_signal_packet_summary": latest_signal,
                "latest_gating_state": gating_state,
                "latest_intent_summary": latest_intent,
                "latest_fill_summary": latest_fill or latest_trade,
                "strategy_performance_row_exists": performance_row is not None,
                "performance_row_present": performance_row is not None,
                "strategy_performance_summary": (
                    {
                        "status": performance_row.get("status"),
                        "trade_count": performance_row.get("trade_count"),
                        "latest_activity_timestamp": performance_row.get("latest_activity_timestamp"),
                        "latest_fill_timestamp": performance_row.get("latest_fill_timestamp"),
                    }
                    if performance_row is not None
                    else None
                ),
                "trade_log_rows_exist": surfaced_trade_log_count > 0,
                "trade_log_present": surfaced_trade_log_count > 0,
                "trade_log_row_count": surfaced_trade_log_count,
            }
        )
    return rows


def _all_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except Exception:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _derive_alert_state_from_jsonl(path: Path) -> dict[str, Any]:
    rows = _all_jsonl_rows(path)
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        dedup_key = str(row.get("dedup_key") or row.get("alert_id") or "")
        if not dedup_key:
            continue
        previous = by_key.get(dedup_key) or {}
        occurrence_count = int(previous.get("occurrence_count") or 0) + 1
        by_key[dedup_key] = {
            "dedup_key": dedup_key,
            "category": row.get("category"),
            "severity": row.get("severity"),
            "title": row.get("title"),
            "message": row.get("message"),
            "recommended_action": row.get("recommended_action"),
            "source_subsystem": row.get("source_subsystem"),
            "active": bool(row.get("active")),
            "acknowledged": bool(row.get("acknowledged")),
            "detail": row.get("detail") or row.get("payload") or {},
            "occurred_at": previous.get("occurred_at") or row.get("occurred_at") or row.get("logged_at"),
            "last_seen_at": row.get("occurred_at") or row.get("logged_at"),
            "last_emitted_at": row.get("occurred_at") or row.get("logged_at"),
            "occurrence_count": occurrence_count,
            "state_transition": row.get("state_transition"),
            "code": row.get("code"),
            "alert_id": row.get("alert_id"),
        }
    active_alerts = [
        row for row in by_key.values() if row.get("active") is True
    ]
    active_alerts.sort(key=lambda row: str(row.get("last_seen_at") or ""), reverse=True)
    updated_at = active_alerts[0].get("last_seen_at") if active_alerts else (rows[-1].get("occurred_at") if rows else None)
    return {
        "updated_at": updated_at,
        "by_key": by_key,
        "active_alerts": active_alerts,
    }


def _median_value(values: Sequence[int | float]) -> float | None:
    filtered = [float(value) for value in values]
    if not filtered:
        return None
    return float(statistics.median(filtered))


def _format_strategy_gap_label(total_seconds: float | None) -> str | None:
    if total_seconds is None:
        return None
    if total_seconds < 3600:
        return f"{int(round(total_seconds / 60))}m"
    if total_seconds < 86400:
        return f"{total_seconds / 3600:.1f}h"
    return f"{total_seconds / 86400:.1f}d"


def _strategy_expected_fire_cadence_label(entry_count: int, median_elapsed_seconds: float | None) -> str:
    if entry_count < 3 or median_elapsed_seconds is None:
        return "insufficient history"
    if median_elapsed_seconds <= 24 * 3600:
        return "frequent"
    if median_elapsed_seconds <= 7 * 24 * 3600:
        return "occasional"
    return "rare"


def _strategy_session_bucket_summary(bucket_counts: dict[str, int]) -> str:
    return ", ".join(
        f"{bucket} {bucket_counts.get(bucket, 0)}"
        for bucket in STRATEGY_HISTORY_SESSION_BUCKETS
        if bucket_counts.get(bucket, 0)
    ) or "No historical entries"


def _strategy_day_of_week_summary(day_counts: Counter[str]) -> str:
    ordered_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return ", ".join(f"{day} {day_counts.get(day, 0)}" for day in ordered_days if day_counts.get(day, 0)) or "Unavailable"


def _strategy_most_common_label(counts: Counter[str], *, default: str) -> tuple[str, int]:
    if not counts:
        return default, 0
    label, count = max(counts.items(), key=lambda item: (item[1], item[0]))
    return label, count


def _strategy_operator_interpretation(
    *,
    entry_count: int,
    expected_fire_cadence: str,
    current_session: str,
    most_common_entry_phase: str,
    most_common_session_bucket: str,
    entries_enabled: bool | None,
    operator_halt: bool | None,
    same_underlying_entry_hold: bool | None,
) -> tuple[str, str]:
    if same_underlying_entry_hold:
        return "same_underlying_hold", "Same-underlying conflict hold is active for this instrument, so new entries are intentionally paused until operator review clears the hold."
    if operator_halt:
        return "operator_halt", "Operator halt is active, so inactivity is expected until the halt is cleared."
    if entries_enabled is False:
        return "entries_disabled", "Entries are disabled for this lane, so no new fire is expected."
    if entry_count == 0:
        return "missing_history", "No historical entries exist in the available lane-local history, so no-fire cannot be judged statistically yet."
    if entry_count < 3:
        return "sparse_history", f"Sparse history: only {entry_count} historical entries are available, so no fire yet may be normal."
    comparison_window = most_common_entry_phase if most_common_entry_phase != "UNKNOWN" else most_common_session_bucket
    if comparison_window and current_session and current_session not in {comparison_window, most_common_session_bucket}:
        return (
            "outside_usual_window",
            f"This lane most often fires during {comparison_window}; the current session is {current_session}, so no fire yet may be normal.",
        )
    return (
        "normal_wait",
        f"This lane historically fires {expected_fire_cadence} in the available sample, most often during {comparison_window or 'UNKNOWN'}; no fire yet may still be normal until that completed-bar window develops.",
    )


def _build_strategy_portfolio_snapshot(
    rows: Sequence[dict[str, Any]],
    *,
    generated_at: str,
) -> dict[str, Any]:
    total_realized = sum((_decimal_or_none(row.get("realized_pnl")) or Decimal("0") for row in rows), Decimal("0"))
    total_day = sum((_decimal_or_none(row.get("day_pnl")) or Decimal("0") for row in rows), Decimal("0"))
    total_max_drawdown = sum((_decimal_or_none(row.get("max_drawdown")) or Decimal("0") for row in rows), Decimal("0"))
    unrealized_values = [_decimal_or_none(row.get("unrealized_pnl")) for row in rows]
    available_unrealized = [value for value in unrealized_values if value is not None]
    missing_unrealized_rows = [str(row.get("strategy_name") or row.get("lane_id") or "UNKNOWN") for row, value in zip(rows, unrealized_values) if value is None]
    total_unrealized = sum(available_unrealized, Decimal("0")) if available_unrealized else None
    total_cumulative = total_realized + total_unrealized if total_unrealized is not None else None
    active_strategy_count = sum(1 for row in rows if row.get("entries_enabled") is not False)
    active_instrument_count = len({str(row.get("instrument") or "") for row in rows if row.get("instrument")})
    return {
        "generated_at": generated_at,
        "total_realized_pnl": _decimal_to_string(total_realized),
        "total_unrealized_pnl": _decimal_to_string(total_unrealized),
        "total_day_pnl": _decimal_to_string(total_day),
        "total_cumulative_pnl": _decimal_to_string(total_cumulative),
        "total_max_drawdown": _decimal_to_string(total_max_drawdown),
        "active_strategy_count": active_strategy_count,
        "active_instrument_count": active_instrument_count,
        "unrealized_complete": len(missing_unrealized_rows) == 0,
        "unrealized_missing_strategy_count": len(missing_unrealized_rows),
        "unrealized_missing_strategies": missing_unrealized_rows,
        "summary_line": (
            "Unrealized and cumulative totals are complete across all surfaced strategies."
            if not missing_unrealized_rows
            else f"Unrealized and cumulative totals are partial; {len(missing_unrealized_rows)} strategy rows are missing trusted open-position marks."
        ),
        "provenance": "Aggregated directly from the surfaced per-strategy performance ledger rows on this dashboard refresh.",
    }


def _build_strategy_metrics_bucket_snapshots(
    rows: Sequence[dict[str, Any]],
    *,
    generated_at: str,
) -> dict[str, Any]:
    bucket_rows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        bucket = str(row.get("metrics_bucket") or "unclassified")
        bucket_rows.setdefault(bucket, []).append(dict(row))
    return {
        bucket: _build_strategy_portfolio_snapshot(bucket_specific_rows, generated_at=generated_at)
        for bucket, bucket_specific_rows in bucket_rows.items()
    }


def _build_strategy_attribution_payload(trade_log_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in trade_log_rows:
        label = str(row.get("signal_family_label") or "UNKNOWN")
        payload = grouped.setdefault(
            label,
            {
                "family_label": label,
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "source_families": set(),
                "standalone_strategy_ids": set(),
                "latest_trade_timestamp": None,
            },
        )
        pnl = _decimal_or_none(row.get("realized_pnl")) or Decimal("0")
        payload["trade_count"] += 1
        payload["realized_pnl"] += pnl
        if pnl > 0:
            payload["wins"] += 1
        elif pnl < 0:
            payload["losses"] += 1
        if row.get("signal_family"):
            payload["source_families"].add(str(row.get("signal_family")))
        if row.get("standalone_strategy_id") or row.get("strategy_key"):
            payload["standalone_strategy_ids"].add(str(row.get("standalone_strategy_id") or row.get("strategy_key")))
        latest_trade_timestamp = str(row.get("exit_timestamp") or row.get("entry_timestamp") or "")
        if latest_trade_timestamp and (
            payload["latest_trade_timestamp"] is None
            or latest_trade_timestamp > payload["latest_trade_timestamp"]
        ):
            payload["latest_trade_timestamp"] = latest_trade_timestamp

    rows = [
        {
            "id": key,
            "family_label": payload["family_label"],
            "trade_count": payload["trade_count"],
            "wins": payload["wins"],
            "losses": payload["losses"],
            "realized_pnl": _decimal_to_string(payload["realized_pnl"]),
            "latest_trade_timestamp": payload["latest_trade_timestamp"],
            "source_families": sorted(payload["source_families"]),
            "standalone_strategy_ids": sorted(payload["standalone_strategy_ids"]),
        }
        for key, payload in grouped.items()
    ]
    rows.sort(key=lambda row: (_sort_decimal_value(row.get("realized_pnl")), str(row.get("family_label") or "")), reverse=True)
    return {
        "rows": rows,
        "scope": "Closed-trade realized attribution grouped by operator-facing family labels. Exact persisted setup_family values remain visible for auditability.",
        "provenance": "Derived from the strategy trade log built from lane-local persisted order intents and fills.",
    }


def _aggregate_branch_pnl_from_blotter(rows: list[dict[str, Any]]) -> dict[str, str]:
    totals: dict[str, Decimal] = {}
    for row in rows:
        family = row.get("setup_family") or "UNKNOWN"
        net_pnl_raw = row.get("net_pnl")
        if not net_pnl_raw:
            continue
        totals[family] = totals.get(family, Decimal("0")) + Decimal(str(net_pnl_raw))
    return {key: str(value) for key, value in sorted(totals.items())}


def _aggregate_branch_pnl_decimals(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {}
    for row in rows:
        family = row.get("setup_family") or "UNKNOWN"
        net_pnl = _decimal_or_none(row.get("net_pnl"))
        if net_pnl is None:
            continue
        totals[family] = totals.get(family, Decimal("0")) + net_pnl
    return totals


def _aggregate_branch_trade_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = {}
    for row in rows:
        branch = row.get("setup_family") or "UNKNOWN"
        pnl = _decimal_or_none(row.get("net_pnl"))
        if pnl is None:
            continue
        payload = stats.setdefault(branch, {"closed_trades": 0, "wins": 0, "losses": 0})
        payload["closed_trades"] += 1
        if pnl > 0:
            payload["wins"] += 1
        elif pnl < 0:
            payload["losses"] += 1
    return stats


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, "", "N/A"}:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _sum_decimal_field(rows: list[dict[str, Any]], field_name: str) -> Decimal | None:
    total = Decimal("0")
    found = False
    for row in rows:
        value = _decimal_or_none(row.get(field_name))
        if value is None:
            continue
        total += value
        found = True
    return total if found else None


def _trade_outcome_counts(rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    wins = 0
    losses = 0
    flat = 0
    for row in rows:
        pnl = _decimal_or_none(row.get("net_pnl"))
        if pnl is None:
            continue
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        else:
            flat += 1
    return wins, losses, flat


def _largest_trade_outcomes(rows: list[dict[str, Any]]) -> tuple[Decimal | None, Decimal | None]:
    values = [_decimal_or_none(row.get("net_pnl")) for row in rows]
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None, None
    return max(filtered), min(filtered)


def _int_or_none(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _count_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        if not value:
            continue
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def _fills_by_branch(session_intents: list[dict[str, Any]], session_fills: list[dict[str, Any]]) -> dict[str, int]:
    branch_by_intent_id = {
        str(row.get("order_intent_id")): str(row.get("reason_code"))
        for row in session_intents
        if row.get("order_intent_id") and row.get("reason_code")
    }
    counts: dict[str, int] = {}
    for fill in session_fills:
        branch = branch_by_intent_id.get(str(fill.get("order_intent_id")))
        if branch is None:
            continue
        counts[branch] = counts.get(branch, 0) + 1
    return counts


def _latest_timestamp_from_rows(rows: list[dict[str, Any]], *fields: str) -> str | None:
    latest: str | None = None
    for row in rows:
        for field in fields:
            value = row.get(field)
            if value and (latest is None or str(value) > latest):
                latest = str(value)
    return latest


def _row_timestamp(row: dict[str, Any] | None, *fields: str) -> str | None:
    if not row:
        return None
    latest: str | None = None
    for field in fields:
        value = row.get(field)
        if value and (latest is None or str(value) > latest):
            latest = str(value)
    return latest


def _latest_row(rows: list[dict[str, Any]], *fields: str) -> dict[str, Any] | None:
    latest_item: dict[str, Any] | None = None
    latest_timestamp: str | None = None
    for row in rows:
        timestamp = _row_timestamp(row, *fields)
        if timestamp and (latest_timestamp is None or timestamp >= latest_timestamp):
            latest_item = row
            latest_timestamp = timestamp
    return latest_item


def _latest_branch_signal_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest_by_source: dict[str, dict[str, Any]] = {}
    for row in rows:
        source = row.get("source")
        if not source:
            continue
        timestamp = row.get("logged_at") or row.get("bar_end_ts")
        current = latest_by_source.get(str(source))
        if current is None or str(timestamp or "") >= str(current.get("timestamp") or ""):
            latest_by_source[str(source)] = {
                "timestamp": timestamp,
                "decision": row.get("decision"),
                "block_reason": row.get("block_reason"),
            }
    return latest_by_source


def _latest_fill_rows_by_source(
    session_intents: list[dict[str, Any]],
    session_fills: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    source_by_intent_id = {
        str(row.get("order_intent_id")): str(row.get("reason_code"))
        for row in session_intents
        if row.get("order_intent_id") and row.get("reason_code")
    }
    latest_by_source: dict[str, dict[str, Any]] = {}
    for fill in session_fills:
        source = source_by_intent_id.get(str(fill.get("order_intent_id")))
        if not source:
            continue
        timestamp = fill.get("fill_timestamp")
        current = latest_by_source.get(source)
        if current is None or str(timestamp or "") >= str(current.get("timestamp") or ""):
            latest_by_source[source] = {
                "timestamp": timestamp,
                "fill_price": fill.get("fill_price"),
                "intent_type": fill.get("intent_type"),
            }
    return latest_by_source


def _latest_intent_rows_by_source(session_intents: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest_by_source: dict[str, dict[str, Any]] = {}
    for row in session_intents:
        source = row.get("reason_code")
        if not source:
            continue
        timestamp = row.get("created_at")
        current = latest_by_source.get(str(source))
        if current is None or str(timestamp or "") >= str(current.get("timestamp") or ""):
            latest_by_source[str(source)] = {
                "timestamp": timestamp,
                "intent_type": row.get("intent_type"),
                "order_status": row.get("order_status"),
                "broker_order_id": row.get("broker_order_id"),
            }
    return latest_by_source


def _format_signal_label(payload: dict[str, Any]) -> str | None:
    if not payload:
        return None
    timestamp = payload.get("timestamp") or "-"
    decision = payload.get("decision") or "seen"
    block_reason = payload.get("block_reason")
    if block_reason:
        return f"{timestamp} • {decision} • {block_reason}"
    return f"{timestamp} • {decision}"


def _format_fill_label(payload: dict[str, Any]) -> str | None:
    if not payload:
        return None
    timestamp = payload.get("timestamp") or "-"
    fill_price = payload.get("fill_price") or "-"
    intent_type = payload.get("intent_type") or "FILL"
    return f"{timestamp} • {intent_type} @ {fill_price}"


def _format_intent_label(payload: dict[str, Any]) -> str | None:
    if not payload:
        return None
    timestamp = payload.get("timestamp") or "-"
    intent_type = payload.get("intent_type") or "INTENT"
    order_status = payload.get("order_status") or "-"
    return f"{timestamp} • {intent_type} • {order_status}"


def _paper_chain_state(
    last_signal: dict[str, Any],
    last_intent: dict[str, Any],
    last_fill: dict[str, Any],
) -> str:
    signal_timestamp = str(last_signal.get("timestamp") or "")
    signal_decision = str(last_signal.get("decision") or "").lower()
    intent_timestamp = str(last_intent.get("timestamp") or "")
    fill_timestamp = str(last_fill.get("timestamp") or "")
    if not signal_timestamp:
        return "NO_SIGNAL"
    if signal_decision == "blocked":
        return "BLOCKED"
    if not intent_timestamp or intent_timestamp < signal_timestamp:
        return "DECISION_WITHOUT_INTENT"
    if not fill_timestamp or fill_timestamp < intent_timestamp:
        return "INTENT_WITHOUT_FILL"
    return "FILLED"


def _paper_model_chain_state(
    *,
    latest_signal: dict[str, Any],
    latest_block_timestamp: str | None,
    latest_intent_timestamp: str | None,
    latest_fill_timestamp: str | None,
    open_position: bool,
) -> str:
    latest_signal_timestamp = _row_timestamp(latest_signal, "logged_at", "bar_end_ts")
    signal_decision = str(latest_signal.get("decision") or "").lower()
    if open_position and not latest_fill_timestamp:
        return "UNKNOWN / INSUFFICIENT_ARTIFACTS"
    if open_position and latest_fill_timestamp:
        return "FILLED_OPEN"
    if latest_fill_timestamp:
        return "FILLED_CLOSED"
    if latest_intent_timestamp:
        return "INTENT_WITHOUT_FILL"
    if latest_block_timestamp or signal_decision == "blocked":
        return "BLOCKED"
    if latest_signal_timestamp:
        return "DECISION_WITHOUT_INTENT"
    return "NO_SIGNAL"


def _paper_lane_activity_verdict(detail: dict[str, Any]) -> str:
    risk_state = str(detail.get("risk_state") or "OK")
    if risk_state != "OK" and detail.get("lane_halt_reason"):
        return "HALTED_BY_RISK"
    if detail.get("open_position") and int(detail.get("fill_count", 0) or 0) > 0:
        return "FILLED_OPEN"
    if int(detail.get("fill_count", 0) or 0) > 0:
        return "FILLED_CLOSED"
    if int(detail.get("intent_count", 0) or 0) > 0:
        return "INTENT_OPEN"
    if int(detail.get("blocked_count", 0) or 0) > 0 or detail.get("latest_blocked_timestamp"):
        return "BLOCKED"
    if int(detail.get("decision_count", 0) or 0) > 0 or detail.get("latest_signal_timestamp"):
        return "SIGNAL_ONLY"
    if not detail.get("latest_activity_timestamp"):
        return "NO_ACTIVITY_YET"
    return "UNKNOWN_INSUFFICIENT_EVIDENCE"


def _paper_session_close_lane_verdict(detail: dict[str, Any], closeout_state: dict[str, Any]) -> str:
    signal_count = int(detail.get("signal_count", 0) or 0)
    signal_only_count = int(detail.get("signal_only_count", 0) or 0)
    blocked_count = int(detail.get("blocked_count", 0) or 0)
    intent_count = int(detail.get("intent_count", 0) or 0)
    fill_count = int(detail.get("fill_count", 0) or 0)
    open_position = bool(detail.get("open_position"))
    if not closeout_state.get("reconciliation_clean") and (
        open_position or signal_count > 0 or blocked_count > 0 or intent_count > 0 or fill_count > 0
    ):
        return "DIRTY_RECONCILIATION"
    if str(detail.get("risk_state") or "OK") != "OK" and detail.get("lane_halt_reason"):
        return "HALTED_BY_RISK"
    if open_position and fill_count > 0:
        return "FILLED_WITH_OPEN_RISK"
    if fill_count > 0:
        return "FILLED_AND_FLAT"
    if blocked_count > 0 and signal_only_count == 0 and intent_count == 0 and fill_count == 0:
        return "BLOCKED_ONLY"
    if signal_count > 0 or intent_count > 0 or detail.get("latest_activity_timestamp"):
        if open_position and fill_count == 0:
            return "UNKNOWN_INSUFFICIENT_EVIDENCE"
        return "SIGNAL_NO_FILL"
    return "IDLE"


def _paper_desk_close_verdict(
    *,
    closeout_state: dict[str, Any],
    paper: dict[str, Any],
    active_count: int,
    open_count: int,
) -> str:
    if str(closeout_state.get("fault_state") or "").upper() == "FAULTED":
        return "FAULTED_CLOSE"
    if not closeout_state.get("reconciliation_clean"):
        return "DIRTY_CLOSE"
    if open_count > 0:
        if paper.get("status", {}).get("operator_halt") or not paper.get("operator_state", {}).get("entries_enabled", True):
            return "HALTED_WITH_OPEN_RISK"
        return "OPEN_RISK_REMAINS"
    if active_count == 0:
        return "CLEAN_IDLE"
    if closeout_state.get("position_flat") and closeout_state.get("summary_generated"):
        return "CLEAN_WITH_ACTIVITY"
    return "UNKNOWN_INSUFFICIENT_EVIDENCE"


def _paper_attribution_evidence_chain_status(gap_reasons: list[str]) -> str:
    reason_set = set(gap_reasons)
    if not reason_set:
        return "COMPLETE"
    if reason_set & {
        "MISSING_FILL_TO_LANE_LINK",
        "MISSING_POSITION_TO_LANE_LINK",
        "OPEN_EXPOSURE_OWNER_AMBIGUOUS",
        "INSUFFICIENT_PERSISTED_EVIDENCE",
    }:
        return "BROKEN"
    return "PARTIAL"


def _paper_attribution_open_first_recommendation(gap_reasons: list[str], open_position: bool, fill_count: int) -> tuple[str, str]:
    reason_set = set(gap_reasons)
    if "RECONCILIATION_NOT_CLEAN" in reason_set:
        return ("Reconciliation", "/api/operator-artifact/paper-reconciliation")
    if "OPEN_EXPOSURE_OWNER_AMBIGUOUS" in reason_set or "MISSING_POSITION_TO_LANE_LINK" in reason_set or open_position:
        return ("Position", "/api/operator-artifact/paper-position-state")
    if "FAMILY_TAGGED_BLOTTER_ONLY" in reason_set or "MULTI_LANE_SAME_FAMILY_AMBIGUITY" in reason_set:
        return ("Blotter", "/api/operator-artifact/paper-latest-blotter")
    if "MISSING_FILL_TO_LANE_LINK" in reason_set or fill_count > 0:
        return ("Fills", "/api/operator-artifact/paper-latest-fills")
    if "INSUFFICIENT_PERSISTED_EVIDENCE" in reason_set:
        return ("Decisions", "/api/operator-artifact/paper-branch-sources")
    return ("Lane Risk", "/api/operator-artifact/paper-lane-risk-status")


def _paper_soak_session_start_timestamp(paper: dict[str, Any], details_by_branch: dict[str, dict[str, Any]]) -> str | None:
    candidates: list[str] = []
    for detail in details_by_branch.values():
        for key in (
            "latest_signal_timestamp",
            "latest_blocked_timestamp",
            "latest_intent_timestamp",
            "latest_fill_timestamp",
            "latest_blotter_timestamp",
        ):
            value = detail.get(key)
            if value:
                candidates.append(str(value))
    for event_group in ("branch_sources", "rule_blocks", "operator_controls", "reconciliation"):
        for row in (paper.get("events", {}) or {}).get(event_group, []) or []:
            value = _row_timestamp(row, "logged_at", "bar_end_ts", "requested_at", "applied_at")
            if value:
                candidates.append(value)
    if candidates:
        return min(candidates)
    return paper.get("status", {}).get("last_update_ts")


def _paper_soak_end_verdict(
    *,
    paper: dict[str, Any],
    review_payload: dict[str, Any],
    models_signaled: list[str],
    models_blocked: list[str],
    models_intents: list[str],
    models_filled: list[str],
    models_open_now: list[str],
) -> str:
    status = paper.get("status", {})
    activity_present = bool(models_signaled or models_blocked or models_filled)
    if status.get("fault_state") == "FAULTED":
        return "FAULTED_SESSION"
    if not status.get("reconciliation_clean"):
        return "DIRTY_AT_CLOSE"
    if not activity_present:
        return "NO_ACTIVITY"
    if not models_filled:
        return "BLOCKED_SESSION" if models_blocked and not models_intents else "ACTIVITY_NO_FILL"
    if models_open_now:
        return "FILLED_WITH_OPEN_RISK"
    if not paper.get("running") and (not review_payload.get("available") or not paper.get("summary_available") or not paper.get("blotter_path")):
        return "DIRTY_AT_CLOSE"
    return "FILLED_AND_FLAT"


def _paper_soak_evidence_markdown(bundle: dict[str, Any]) -> str:
    soak = bundle.get("session_summary", {})
    return "\n".join(
        [
            "# Paper Soak Evidence",
            "",
            f"- Captured at: {bundle.get('captured_at') or '-'}",
            f"- Session date: {bundle.get('session_date') or '-'}",
            f"- Current session verdict: {bundle.get('current_session_verdict') or '-'}",
            f"- End-of-session verdict: {bundle.get('end_of_session_verdict') or '-'}",
            f"- Paper running: {'yes' if bundle.get('paper_running') else 'no'}",
            f"- Session start: {soak.get('session_start') or '-'}",
            f"- Runtime duration: {soak.get('runtime_duration') or '-'}",
            f"- Models signaled: {', '.join(soak.get('models_signaled') or []) or 'None'}",
            f"- Models blocked: {', '.join(soak.get('models_blocked') or []) or 'None'}",
            f"- Models intents: {', '.join(soak.get('models_intents') or []) or 'None'}",
            f"- Models filled: {', '.join(soak.get('models_filled') or []) or 'None'}",
            f"- Models open now: {', '.join(soak.get('models_open_now') or []) or 'None'}",
            f"- Severe exception seen: {'yes' if soak.get('severe_exception_seen') else 'no'}",
            f"- Paper summary generated: {'yes' if soak.get('summary_generated') else 'no'}",
            "",
            "Source paths:",
            *(f"- {key}: {value}" for key, value in (bundle.get("source_paths") or {}).items()),
            "",
        ]
    ) + "\n"


def _paper_session_close_review_markdown(bundle: dict[str, Any]) -> str:
    attribution = bundle.get("desk_attribution_summary", {})
    rows = bundle.get("rows") or []
    top_gap_reasons = ", ".join(
        f"{item.get('reason')} ({item.get('count')})"
        for item in (attribution.get("top_attribution_gap_reasons") or [])
    ) or "None"
    lines = [
        "# Multi-Lane Paper Session Close Review",
        "",
        f"- Generated at: {bundle.get('generated_at') or '-'}",
        f"- Session date: {bundle.get('session_date') or '-'}",
        f"- Desk close verdict: {bundle.get('desk_close_verdict') or '-'}",
        f"- Admitted lanes: {bundle.get('admitted_lanes_count') or 0}",
        f"- Active lanes: {bundle.get('active_lanes_count') or 0}",
        f"- Blocked lanes: {bundle.get('blocked_lanes_count') or 0}",
        f"- Filled lanes: {bundle.get('filled_lanes_count') or 0}",
        f"- Open lanes: {bundle.get('open_lanes_count') or 0}",
        f"- Total attributable realized P/L: {bundle.get('total_attributable_realized_pnl') or '-'}",
        f"- Realized attribution coverage: {bundle.get('realized_attribution_coverage') or '-'}",
        f"- Exact realized attribution count: {attribution.get('exact_realized_attribution_count') or 0}",
        f"- Partial realized attribution count: {attribution.get('partial_realized_attribution_count') or 0}",
        f"- Unattributable realized attribution count: {attribution.get('unattributable_realized_attribution_count') or 0}",
        f"- Exact open-risk ownership count: {attribution.get('exact_open_risk_ownership_count') or 0}",
        f"- Ambiguous open-risk ownership count: {attribution.get('ambiguous_open_risk_ownership_count') or 0}",
        f"- Unattributed realized P/L present: {'yes' if attribution.get('unattributed_realized_pnl_present') else 'no'}",
        f"- Desk P/L completeness: {attribution.get('desk_pnl_completeness') or '-'}",
        f"- Desk review confidence: {attribution.get('desk_review_confidence') or '-'}",
        f"- Reliable P/L judgment lanes: {', '.join(attribution.get('reliable_pnl_judgment_lanes') or []) or 'None'}",
        f"- Manual P/L inspection lanes: {', '.join(attribution.get('manual_pnl_inspection_lanes') or []) or 'None'}",
        f"- Complete evidence chains: {', '.join(attribution.get('complete_evidence_chain_lanes') or []) or 'None'}",
        f"- Partial evidence chains: {', '.join(attribution.get('partial_evidence_chain_lanes') or []) or 'None'}",
        f"- Broken evidence chains: {', '.join(attribution.get('broken_evidence_chain_lanes') or []) or 'None'}",
        f"- Historical trust verdict: {attribution.get('historical_trust_verdict') or '-'}",
        f"- History sufficiency: {attribution.get('history_sufficiency_note') or '-'}",
        f"- Repeated partial chains: {', '.join(attribution.get('repeated_partial_chain_lanes') or []) or 'None'}",
        f"- Repeated broken chains: {', '.join(attribution.get('repeated_broken_chain_lanes') or []) or 'None'}",
        f"- Repeated dirty closes: {', '.join(attribution.get('repeated_dirty_close_lanes') or []) or 'None'}",
        f"- Repeated open-risk closes: {', '.join(attribution.get('repeated_open_risk_close_lanes') or []) or 'None'}",
        f"- Lanes with insufficient history: {', '.join(attribution.get('lanes_with_insufficient_history') or []) or 'None'}",
        f"- Lanes with sufficient history: {', '.join(attribution.get('lanes_with_sufficient_history') or []) or 'None'}",
        f"- Desk history confidence: {attribution.get('desk_history_confidence') or '-'}",
        f"- History threshold note: {attribution.get('history_threshold_note') or '-'}",
        f"- Top attribution gap reasons: {top_gap_reasons}",
        f"- Review required lanes: {', '.join(bundle.get('review_required_lanes') or []) or 'None'}",
        "",
        "Lane review rows:",
    ]
    for row in rows:
        lines.extend(
            [
                f"- {row.get('branch') or '-'}",
                f"  session_verdict={row.get('session_verdict') or '-'}",
                f"  realized_attr={row.get('realized_pnl_attribution_status') or '-'}",
                f"  attributable_realized={row.get('attributable_realized_pnl') or '-'}",
                f"  open_attr={row.get('unrealized_pnl_attribution_status') or '-'}",
                f"  attributable_unrealized={row.get('attributable_unrealized_pnl') or '-'}",
                f"  evidence_chain={row.get('evidence_chain_status') or '-'}",
                f"  review_confidence={row.get('review_confidence') or '-'}",
                f"  history_sufficiency={row.get('history_sufficiency_status') or '-'}",
                f"  repeat_review_confidence={row.get('repeat_review_confidence') or '-'}",
                f"  prior_closes={row.get('prior_close_reviews_found') or 0}",
                f"  repeat_review={row.get('repeat_review_verdict') or '-'}",
                f"  last_manual_review={row.get('last_manual_review_required_ts') or '-'}",
                f"  gap_reason={', '.join(row.get('attribution_gap_reason') or []) or 'None'}",
                f"  open_first={row.get('open_first_recommendation', {}).get('label') or '-'}",
            ]
        )
    lines.extend(
        [
            "Source paths:",
            *(f"- {key}: {value}" for key, value in (bundle.get("source_paths") or {}).items()),
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _paper_session_close_review_history_markdown(bundle: dict[str, Any]) -> str:
    archives = bundle.get("archived_reviews") or []
    top_gap_reasons = ", ".join(
        f"{item.get('reason')} ({item.get('count')})"
        for item in (bundle.get("top_recurring_attribution_gap_reasons") or [])
    ) or "None"
    return "\n".join(
        [
            "# Multi-Lane Paper Session Close Review History",
            "",
            f"- Generated at: {bundle.get('generated_at') or '-'}",
            f"- Session date: {bundle.get('session_date') or '-'}",
            f"- Prior reviews count: {bundle.get('prior_reviews_count') or 0}",
            f"- Historical trust verdict: {bundle.get('historical_trust_verdict') or '-'}",
            f"- Desk history confidence: {bundle.get('desk_history_confidence') or '-'}",
            f"- History threshold note: {bundle.get('history_threshold_note') or '-'}",
            f"- History sufficiency: {bundle.get('history_sufficiency_note') or '-'}",
            f"- Lanes with insufficient history: {', '.join(bundle.get('lanes_with_insufficient_history') or []) or 'None'}",
            f"- Lanes with sufficient history: {', '.join(bundle.get('lanes_with_sufficient_history') or []) or 'None'}",
            f"- Repeated partial chains: {', '.join(bundle.get('repeated_partial_chain_lanes') or []) or 'None'}",
            f"- Repeated broken chains: {', '.join(bundle.get('repeated_broken_chain_lanes') or []) or 'None'}",
            f"- Repeated unattributable realized: {', '.join(bundle.get('repeated_unattributable_realized_lanes') or []) or 'None'}",
            f"- Repeated dirty closes: {', '.join(bundle.get('repeated_dirty_close_lanes') or []) or 'None'}",
            f"- Repeated open-risk closes: {', '.join(bundle.get('repeated_open_risk_close_lanes') or []) or 'None'}",
            f"- Top recurring gap reasons: {top_gap_reasons}",
            "",
            "Archived reviews:",
            *(
                f"- {item.get('session_date') or '-'} @ {item.get('generated_at') or '-'} | {item.get('desk_close_verdict') or '-'} | {item.get('json_path') or '-'}"
                for item in archives
            ),
            "",
        ]
    ) + "\n"


def _safe_archive_timestamp_slug(value: str) -> str:
    return (
        str(value or "unknown")
        .replace(":", "-")
        .replace("+", "p")
        .replace("/", "-")
        .replace(" ", "_")
    )


def _infer_open_branch_source(position: dict[str, Any], session_intents: list[dict[str, Any]]) -> str | None:
    if position.get("side") == "FLAT":
        return None
    for intent in session_intents:
        intent_type = str(intent.get("intent_type") or "").upper()
        if "OPEN" not in intent_type:
            continue
        if str(intent.get("order_status") or "").upper() != "FILLED":
            continue
        if intent.get("reason_code"):
            return str(intent.get("reason_code"))
    return None


def _sort_decimal_value(value: Any) -> Decimal:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return Decimal("-999999999")
    return parsed


def _average_decimal_strings(values: list[Any]) -> Decimal | None:
    decimals = [_decimal_or_none(value) for value in values]
    filtered = [value for value in decimals if value is not None]
    if not filtered:
        return None
    return sum(filtered, Decimal("0")) / Decimal(len(filtered))


def _average_int_values(values: list[Any]) -> Decimal | None:
    ints = [_int_or_none(value) for value in values]
    filtered = [value for value in ints if value is not None]
    if not filtered:
        return None
    return Decimal(sum(filtered)) / Decimal(len(filtered))


def _average_decimals(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _stddev_decimal(values: list[Decimal]) -> Decimal | None:
    if len(values) < 2:
        return None
    mean = _average_decimals(values)
    if mean is None:
        return None
    variance = sum((value - mean) * (value - mean) for value in values) / Decimal(len(values))
    return Decimal(str(math.sqrt(float(variance))))


def _session_streak(session_rows: list[dict[str, Any]]) -> str:
    realized_values = [_decimal_or_none(row.get("realized_pnl")) for row in session_rows]
    filtered = [value for value in realized_values if value is not None]
    if not filtered:
        return "No recent realized-session history."
    first = filtered[0]
    if first > 0:
        label = "Positive"
        count = 0
        for value in filtered:
            if value > 0:
                count += 1
            else:
                break
        return f"{label} {count} session streak"
    if first < 0:
        label = "Negative"
        count = 0
        for value in filtered:
            if value < 0:
                count += 1
            else:
                break
        return f"{label} {count} session streak"
    return "Flat latest session"


def _duration_between(start_timestamp: str | None, end_timestamp: str | None) -> str | None:
    start = _parse_iso_datetime(start_timestamp)
    end = _parse_iso_datetime(end_timestamp)
    if start is None or end is None or end < start:
        return None
    delta_seconds = int((end - start).total_seconds())
    hours, remainder = divmod(delta_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _trend_label(
    latest_realized: Decimal | None,
    latest_vs_prior: Decimal | None,
    latest_vs_average: Decimal | None,
    *,
    dispersion: Decimal | None,
    sample_size: int,
) -> str:
    if latest_realized is None:
        return "INSUFFICIENT HISTORY"
    positive_signals = 0
    negative_signals = 0
    for value in (latest_vs_prior, latest_vs_average):
        if value is None:
            continue
        if value > 0:
            positive_signals += 1
        elif value < 0:
            negative_signals += 1
    volatile = bool(dispersion is not None and abs(latest_realized) > 0 and dispersion > abs(latest_realized))
    if sample_size < 3:
        if positive_signals >= 1:
            return "IMPROVING / LOW SAMPLE"
        if negative_signals >= 1:
            return "DETERIORATING / LOW SAMPLE"
        return "MIXED / LOW SAMPLE"
    if positive_signals >= 2:
        return "IMPROVING BUT VOLATILE" if volatile else "IMPROVING AND STABLE"
    if negative_signals >= 2:
        return "DETERIORATING AND UNSTABLE" if volatile else "DETERIORATING"
    return "FLAT / MIXED" if not volatile else "MIXED / VOLATILE"


def _session_distribution_metrics(session_rows: list[dict[str, Any]]) -> dict[str, Any]:
    realized_values = [_decimal_or_none(row.get("realized_pnl")) for row in session_rows]
    filtered = [value for value in realized_values if value is not None]
    sample_size = len(filtered)
    positive_sessions = sum(1 for value in filtered if value > 0)
    negative_sessions = sum(1 for value in filtered if value < 0)
    median = _median_decimal(filtered)
    best = max(filtered) if filtered else None
    worst = min(filtered) if filtered else None
    pnl_range = (best - worst) if best is not None and worst is not None else None
    dispersion = _stddev_decimal(filtered)
    positive_pct = None
    negative_pct = None
    if sample_size > 0:
        positive_pct = (Decimal(positive_sessions) / Decimal(sample_size)) * Decimal("100")
        negative_pct = (Decimal(negative_sessions) / Decimal(sample_size)) * Decimal("100")
    return {
        "sample_size": sample_size,
        "best_session": _decimal_to_string(best),
        "worst_session": _decimal_to_string(worst),
        "median_realized": _decimal_to_string(median),
        "pnl_range": _decimal_to_string(pnl_range),
        "dispersion": _decimal_to_string(dispersion),
        "positive_session_rate": f"{positive_pct.quantize(Decimal('0.1'))}%" if positive_pct is not None else None,
        "negative_session_rate": f"{negative_pct.quantize(Decimal('0.1'))}%" if negative_pct is not None else None,
        "scope": (
            f"Derived from {sample_size} recent realized paper sessions."
            if sample_size > 0
            else "No recent realized paper sessions."
        ),
        "dispersion_note": (
            "Standard deviation of recent realized session P/L."
            if sample_size >= 3
            else "Dispersion is low-confidence until at least three realized sessions exist."
        ),
    }


def _session_drawdown_metrics(session_rows: list[dict[str, Any]]) -> dict[str, Any]:
    chronological = list(reversed(session_rows))
    realized_values = [_decimal_or_none(row.get("realized_pnl")) for row in chronological]
    filtered = [value for value in realized_values if value is not None]
    if not filtered:
        return {
            "worst_drawdown": None,
            "distance_from_high_water": None,
            "negative_run": "No recent realized-session history.",
            "scope": "No recent realized paper sessions.",
        }
    equity = Decimal("0")
    high_water = Decimal("0")
    worst_drawdown = Decimal("0")
    for value in filtered:
        equity += value
        if equity > high_water:
            high_water = equity
        drawdown = high_water - equity
        if drawdown > worst_drawdown:
            worst_drawdown = drawdown
    latest_equity = sum(filtered, Decimal("0"))
    distance_from_high_water = high_water - latest_equity
    return {
        "worst_drawdown": _decimal_to_string(worst_drawdown),
        "distance_from_high_water": _decimal_to_string(distance_from_high_water),
        "negative_run": _session_streak(session_rows) if filtered and filtered[-1] < 0 else _session_streak(session_rows),
        "scope": f"Derived from chronological realized session outcomes across {len(filtered)} recent sessions.",
    }


def _branch_stability_label(*, sessions_seen: int, realized_values: list[Decimal]) -> str:
    if sessions_seen < 3:
        return "MIXED / LOW SAMPLE"
    filtered = realized_values[:sessions_seen]
    positive_sessions = sum(1 for value in filtered if value > 0)
    negative_sessions = sum(1 for value in filtered if value < 0)
    mean = _average_decimals(filtered)
    dispersion = _stddev_decimal(filtered)
    if mean is None:
        return "MIXED / LOW SAMPLE"
    volatile = bool(dispersion is not None and abs(mean) > 0 and dispersion > abs(mean))
    if mean > 0 and positive_sessions >= max(2, math.ceil(sessions_seen * 0.67)):
        return "VOLATILE CONTRIBUTOR" if volatile else "CONSISTENT CONTRIBUTOR"
    if mean < 0 and negative_sessions >= max(2, math.ceil(sessions_seen * 0.67)):
        return "VOLATILE CONTRIBUTOR" if volatile else "CONSISTENTLY WEAK"
    return "MIXED / VOLATILE" if volatile else "MIXED"


def _build_session_shape_points(
    *,
    full_blotter_rows: list[dict[str, Any]],
    session_intents: list[dict[str, Any]],
    session_fills: list[dict[str, Any]],
    position: dict[str, Any],
    operator_status: dict[str, Any],
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    start_timestamp = _earliest_timestamp_from_session_artifacts(full_blotter_rows, session_intents, session_fills)
    if start_timestamp:
        points.append({"timestamp": start_timestamp, "pnl": Decimal("0"), "kind": "start", "label": "Session start"})
    cumulative = Decimal("0")
    ordered_rows = sorted(full_blotter_rows, key=lambda row: row.get("exit_ts") or row.get("entry_ts") or "")
    for row in ordered_rows:
        pnl = _decimal_or_none(row.get("net_pnl"))
        if pnl is None:
            continue
        cumulative += pnl
        timestamp = row.get("exit_ts") or row.get("entry_ts")
        points.append(
            {
                "timestamp": timestamp,
                "pnl": cumulative,
                "kind": "closed_trade",
                "label": row.get("setup_family") or row.get("exit_reason") or "Closed trade",
            }
        )
    current_unrealized = _decimal_or_none(position.get("unrealized_pnl"))
    if current_unrealized is not None and position.get("side") != "FLAT":
        timestamp = operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at")
        points.append(
            {
                "timestamp": timestamp,
                "pnl": cumulative + current_unrealized,
                "kind": "current_open_estimate",
                "label": "Current open-position estimate",
            }
        )
    return points


def _latest_session_branch_contribution_rows(
    *,
    full_blotter_rows: list[dict[str, Any]],
    session_intents: list[dict[str, Any]],
    session_fills: list[dict[str, Any]],
    open_branch: str | None,
    current_unrealized: Decimal | None,
    session_start: str | None,
    session_end: str | None,
    latest_event_timestamp: str | None,
) -> list[dict[str, Any]]:
    blotter_by_branch: dict[str, list[dict[str, Any]]] = {}
    for row in full_blotter_rows:
        branch = row.get("setup_family") or "UNKNOWN"
        blotter_by_branch.setdefault(str(branch), []).append(row)

    intents_by_branch: dict[str, list[dict[str, Any]]] = {}
    for row in session_intents:
        branch = row.get("reason_code")
        if not branch:
            continue
        intents_by_branch.setdefault(str(branch), []).append(row)

    branch_by_intent_id = {
        str(row.get("order_intent_id")): str(row.get("reason_code"))
        for row in session_intents
        if row.get("order_intent_id") and row.get("reason_code")
    }
    fills_by_branch_rows: dict[str, list[dict[str, Any]]] = {}
    for row in session_fills:
        branch = branch_by_intent_id.get(str(row.get("order_intent_id")))
        if branch is None:
            continue
        fills_by_branch_rows.setdefault(branch, []).append(row)

    branch_names = set(blotter_by_branch) | set(intents_by_branch) | set(fills_by_branch_rows)
    if open_branch:
        branch_names.add(open_branch)

    rows: list[dict[str, Any]] = []
    for branch in sorted(branch_names):
        branch_blotter = sorted(
            blotter_by_branch.get(branch, []),
            key=lambda row: row.get("exit_ts") or row.get("entry_ts") or "",
        )
        realized = _sum_decimal_field(branch_blotter, "net_pnl")
        wins, losses, flat_trades = _trade_outcome_counts(branch_blotter)
        branch_points: list[dict[str, Any]] = []
        cumulative = Decimal("0")
        for row in branch_blotter:
            pnl = _decimal_or_none(row.get("net_pnl"))
            if pnl is None:
                continue
            cumulative += pnl
            branch_points.append(
                {
                    "timestamp": row.get("exit_ts") or row.get("entry_ts"),
                    "pnl": cumulative,
                    "kind": "closed_trade",
                }
            )
        unrealized = current_unrealized if open_branch == branch else None
        if unrealized is not None and latest_event_timestamp:
            branch_points.append(
                {
                    "timestamp": latest_event_timestamp,
                    "pnl": cumulative + unrealized,
                    "kind": "current_open_estimate",
                }
            )

        total = None
        if realized is not None and unrealized is not None:
            total = realized + unrealized
        elif realized is not None:
            total = realized
        elif unrealized is not None:
            total = unrealized

        first_time = branch_points[0]["timestamp"] if branch_points else _earliest_branch_timestamp(
            intents_by_branch.get(branch, []),
            fills_by_branch_rows.get(branch, []),
        )
        last_time = branch_points[-1]["timestamp"] if branch_points else first_time
        first_bucket = _session_time_bucket(first_time, session_start, session_end)
        last_bucket = _session_time_bucket(last_time, session_start, session_end)
        rows.append(
            {
                "branch": branch,
                "realized_pnl": _decimal_to_string(realized),
                "unrealized_pnl": _decimal_to_string(unrealized),
                "total_contribution": _decimal_to_string(total),
                "fills": len(fills_by_branch_rows.get(branch, [])),
                "closed_trades": len(branch_blotter),
                "first_meaningful_time": first_time,
                "last_meaningful_time": last_time,
                "net_effect": _branch_net_effect_label(total=total, wins=wins, losses=losses, flat_trades=flat_trades),
                "timing_hint": _branch_timing_hint(branch_points, session_start=session_start, session_end=session_end, total=total),
                "path_hint": _branch_path_hint(branch_points),
                "scope": "Latest paper session only",
                "first_bucket": first_bucket,
                "last_bucket": last_bucket,
            }
        )
    rows.sort(key=lambda row: (_sort_decimal_value(row.get("total_contribution")), _sort_decimal_value(row.get("realized_pnl")), row["branch"]), reverse=True)
    return rows


def _earliest_timestamp_from_session_artifacts(
    full_blotter_rows: list[dict[str, Any]],
    session_intents: list[dict[str, Any]],
    session_fills: list[dict[str, Any]],
) -> str | None:
    candidates = []
    for row in full_blotter_rows:
        if row.get("entry_ts"):
            candidates.append(str(row["entry_ts"]))
    for row in session_intents:
        if row.get("created_at"):
            candidates.append(str(row["created_at"]))
    for row in session_fills:
        if row.get("fill_timestamp"):
            candidates.append(str(row["fill_timestamp"]))
    return min(candidates) if candidates else None


def _earliest_branch_timestamp(intent_rows: list[dict[str, Any]], fill_rows: list[dict[str, Any]]) -> str | None:
    candidates = []
    for row in intent_rows:
        if row.get("created_at"):
            candidates.append(str(row["created_at"]))
    for row in fill_rows:
        if row.get("fill_timestamp"):
            candidates.append(str(row["fill_timestamp"]))
    return min(candidates) if candidates else None


def _latest_value_from_rows(rows: list[dict[str, Any]], key: str) -> str | None:
    values = [str(row[key]) for row in rows if row.get(key)]
    return max(values) if values else None


def _session_path_max_drawdown(points: list[dict[str, Any]]) -> tuple[Decimal | None, str | None]:
    if not points:
        return None, None
    high_water = points[0]["pnl"]
    max_drawdown = Decimal("0")
    max_drawdown_time = points[0]["timestamp"]
    for point in points:
        pnl = point["pnl"]
        if pnl > high_water:
            high_water = pnl
        drawdown = high_water - pnl
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_time = point["timestamp"]
    return max_drawdown, max_drawdown_time


def _close_location_label(*, latest_value: Decimal | None, high_value: Decimal | None, low_value: Decimal | None) -> str:
    if latest_value is None or high_value is None or low_value is None:
        return "Unavailable"
    if high_value == low_value:
        return "Closed in a flat range"
    relative = (latest_value - low_value) / (high_value - low_value)
    if relative >= Decimal("0.67"):
        return "Closed near highs"
    if relative <= Decimal("0.33"):
        return "Closed near lows"
    return "Closed near mid-range"


def _session_shape_label(points: list[dict[str, Any]]) -> str:
    if len(points) < 2:
        return "Mixed / unclear"
    pnls = [point["pnl"] for point in points]
    final_value = pnls[-1]
    high_value = max(pnls)
    low_value = min(pnls)
    high_index = max(range(len(points)), key=lambda index: points[index]["pnl"])
    low_index = min(range(len(points)), key=lambda index: points[index]["pnl"])
    pnl_range = high_value - low_value
    if pnl_range == 0:
        return "Choppy flat"
    latest_is_high = high_index == len(points) - 1
    latest_is_low = low_index == len(points) - 1
    if abs(final_value) < pnl_range / Decimal("4"):
        return "Choppy flat"
    if final_value > 0 and low_value < 0 and latest_is_high:
        if low_index >= len(points) // 2:
            return "Late reversal up"
        return "Early drawdown / recovery"
    if final_value > 0 and latest_is_high and low_value >= 0:
        return "Steady up"
    if final_value > 0 and high_index < len(points) - 1:
        return "Early gain / fade"
    if final_value < 0 and latest_is_low:
        if high_value > 0 and high_index < len(points) - 1:
            return "Late deterioration"
        return "Trend down"
    if final_value < 0 and high_index < len(points) - 1:
        return "Late deterioration"
    return "Mixed / unclear"


def _branch_net_effect_label(*, total: Decimal | None, wins: int, losses: int, flat_trades: int) -> str:
    if total is None and not (wins or losses or flat_trades):
        return "Unavailable"
    if wins and losses:
        if total is not None and total > 0:
            return "Mixed positive"
        if total is not None and total < 0:
            return "Mixed negative"
        return "Mixed"
    if total is not None:
        if total > 0:
            return "Net positive"
        if total < 0:
            return "Net negative"
    if wins:
        return "Net positive"
    if losses:
        return "Net negative"
    return "Flat / mixed"


def _branch_path_hint(points: list[dict[str, Any]]) -> str:
    if len(points) < 2:
        return "Mixed / unclear"
    pnls = [point["pnl"] for point in points]
    final_value = pnls[-1]
    high_value = max(pnls)
    low_value = min(pnls)
    high_index = max(range(len(points)), key=lambda index: points[index]["pnl"])
    low_index = min(range(len(points)), key=lambda index: points[index]["pnl"])
    if high_value == low_value:
        return "Mixed / unclear"
    latest_is_high = high_index == len(points) - 1
    latest_is_low = low_index == len(points) - 1
    if final_value > 0 and low_value < 0 and latest_is_high:
        return "Early negative then recovered" if low_index <= max(1, len(points) // 3) else "Late recovery"
    if final_value > 0 and latest_is_high and low_value >= 0:
        return "Steady positive"
    if final_value > 0 and high_index < len(points) - 1:
        return "Early positive then faded"
    if final_value < 0 and latest_is_low:
        return "Late negative"
    if final_value < 0 and high_value > 0 and high_index < len(points) - 1:
        return "Early positive then faded"
    return "Mixed / unclear"


def _branch_timing_hint(
    points: list[dict[str, Any]],
    *,
    session_start: str | None,
    session_end: str | None,
    total: Decimal | None,
) -> str:
    if not points or total is None:
        return "Unavailable"
    first_bucket = _session_time_bucket(points[0]["timestamp"], session_start, session_end)
    last_bucket = _session_time_bucket(points[-1]["timestamp"], session_start, session_end)
    low_value = min(point["pnl"] for point in points)
    latest_is_high = max(range(len(points)), key=lambda index: points[index]["pnl"]) == len(points) - 1
    if total > 0 and low_value < 0 and latest_is_high:
        return "Recovery contributor"
    if first_bucket == "early" and total > 0 and last_bucket in {"early", "mid"}:
        return "Early contributor"
    if first_bucket == "early" and total < 0 and last_bucket in {"early", "mid"}:
        return "Early detractor"
    if last_bucket == "late" and total > 0:
        return "Late contributor"
    if last_bucket == "late" and total < 0:
        return "Late detractor"
    return "Mixed timing"


def _branch_contribution_card(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "branch": row.get("branch"),
        "total_contribution": row.get("total_contribution"),
        "timing_hint": row.get("timing_hint"),
        "path_hint": row.get("path_hint"),
        "net_effect": row.get("net_effect"),
    }


def _branch_phase_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    positive_rows = [row for row in rows if (_decimal_or_none(row.get("total_contribution")) or Decimal("0")) > 0]
    negative_rows = [row for row in rows if (_decimal_or_none(row.get("total_contribution")) or Decimal("0")) < 0]

    def _pick_best(candidates: list[dict[str, Any]], *, most_negative: bool = False) -> dict[str, Any] | None:
        if not candidates:
            return None
        if most_negative:
            return min(candidates, key=lambda row: _decimal_or_none(row.get("total_contribution")) or Decimal("0"))
        return max(candidates, key=lambda row: _decimal_or_none(row.get("total_contribution")) or Decimal("0"))

    return {
        "early_run_up": _branch_contribution_card(_pick_best([row for row in positive_rows if row.get("first_bucket") == "early"])),
        "early_drawdown": _branch_contribution_card(_pick_best([row for row in negative_rows if row.get("first_bucket") == "early"], most_negative=True)),
        "late_recovery": _branch_contribution_card(_pick_best([row for row in positive_rows if row.get("timing_hint") == "Recovery contributor" or row.get("path_hint") in {"Late recovery", "Early negative then recovered"}])),
        "late_fade": _branch_contribution_card(_pick_best([row for row in negative_rows if row.get("last_bucket") == "late" or row.get("timing_hint") == "Late detractor"], most_negative=True)),
    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_research_capture_status_rows(database_path: Path) -> list[dict[str, Any]]:
    if not database_path.exists():
        return []
    try:
        with sqlite3.connect(database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                select
                  symbol,
                  timeframe,
                  capture_class,
                  data_source,
                  last_attempted_at,
                  last_succeeded_at,
                  last_bar_end_ts,
                  last_status,
                  last_failure_code,
                  last_failure_detail,
                  last_capture_run_id
                from research_capture_status
                order by symbol asc, timeframe asc, capture_class asc
                """
            ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def _load_research_capture_run_rows(database_path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not database_path.exists():
        return []
    try:
        with sqlite3.connect(database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                select
                  capture_run_id,
                  symbol,
                  timeframe,
                  capture_class,
                  data_source,
                  started_at,
                  completed_at,
                  status,
                  previous_last_bar_end_ts,
                  fetched_bar_count,
                  fetched_first_bar_end_ts,
                  fetched_last_bar_end_ts,
                  persisted_last_bar_end_ts,
                  failure_code,
                  failure_detail
                from research_capture_runs
                order by capture_run_id desc
                limit ?
                """,
                (int(limit),),
            ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def _latest_success_timestamp(status_rows: Sequence[dict[str, Any]]) -> str | None:
    latest: datetime | None = None
    latest_text: str | None = None
    for row in status_rows:
        candidate_text = str(row.get("last_succeeded_at") or "").strip() or None
        candidate = _parse_iso_datetime(candidate_text)
        if candidate is None:
            continue
        if latest is None or candidate > latest:
            latest = candidate
            latest_text = candidate_text
    return latest_text


def _research_capture_freshness_state(last_attempted_at: str | None, *, now: datetime) -> str:
    attempted = _parse_iso_datetime(last_attempted_at)
    if attempted is None:
        return "no_run"
    age_seconds = (now - attempted.astimezone(timezone.utc)).total_seconds()
    return "stale" if age_seconds > 36 * 3600 else "current"


def _research_capture_status_line(
    *,
    run_status: str,
    freshness_state: str,
    attempted_symbols: Sequence[str],
    succeeded_symbols: Sequence[str],
    failed_symbols: Sequence[dict[str, Any]],
    last_attempted_at: str | None,
) -> str:
    if freshness_state == "no_run":
        return "No research-history capture run has been recorded yet."
    attempted_count = len(attempted_symbols)
    succeeded_count = len(succeeded_symbols)
    failed_count = len(failed_symbols)
    attempted_label = last_attempted_at or "unknown time"
    if freshness_state == "stale":
        return (
            f"Research-history capture is stale. Last attempted run was at {attempted_label} with status {run_status}. "
            f"Attempted={attempted_count}, succeeded={succeeded_count}, failed={failed_count}."
        )
    if run_status == "success":
        return (
            f"Last daily research-history capture succeeded at {attempted_label}. "
            f"Attempted={attempted_count}, succeeded={succeeded_count}, failed={failed_count}."
        )
    if run_status == "partial_failure":
        return (
            f"Last daily research-history capture completed with partial failures at {attempted_label}. "
            f"Attempted={attempted_count}, succeeded={succeeded_count}, failed={failed_count}."
        )
    if run_status == "failure":
        return (
            f"Last daily research-history capture failed at {attempted_label}. "
            f"Attempted={attempted_count}, succeeded={succeeded_count}, failed={failed_count}."
        )
    return (
        f"Last daily research-history capture status is {run_status} at {attempted_label}. "
        f"Attempted={attempted_count}, succeeded={succeeded_count}, failed={failed_count}."
    )


def _session_time_bucket(timestamp: str | None, session_start: str | None, session_end: str | None) -> str | None:
    ts = _parse_iso_datetime(timestamp)
    start = _parse_iso_datetime(session_start)
    end = _parse_iso_datetime(session_end)
    if ts is None or start is None or end is None or end <= start:
        return None
    span = (end - start).total_seconds()
    if span <= 0:
        return None
    ratio = (ts - start).total_seconds() / span
    if ratio <= 0.34:
        return "early"
    if ratio >= 0.67:
        return "late"
    return "mid"


def _timestamp_matches_session(timestamp: str | None, session_date: str | None) -> bool:
    if not timestamp or not session_date:
        return False
    parsed = _parse_iso_datetime(timestamp)
    if parsed is None:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date().isoformat() == session_date


def _operator_control_title(action: Any) -> str:
    normalized = str(action or "").strip().lower()
    mapping = {
        "halt_entries": "Halt Entries",
        "resume_entries": "Resume Entries",
        "flatten_and_halt": "Flatten And Halt",
        "stop_after_current_cycle": "Stop After Current Cycle",
        "clear_fault": "Clear Fault",
        "clear_risk_halts": "Clear Risk Halts",
        "force_reconcile": "Force Reconcile",
        "acknowledge_fault": "Acknowledge Fault",
    }
    return mapping.get(normalized, str(action or "Operator Control"))


def _treasury_specs_from_config_path(config_path: Path) -> list[dict[str, str]]:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return _default_treasury_specs()
    configured = payload.get("treasury_context_quote_symbols", {}) if isinstance(payload, dict) else {}
    specs: list[dict[str, str]] = []
    for tenor in TREASURY_TENOR_ORDER:
        meta = TREASURY_TENOR_DISPLAY_META[tenor]
        external_symbol = str(configured.get(tenor, "")).strip() or _default_treasury_symbol(tenor)
        specs.append(
            {
                "tenor": tenor,
                "name": meta["name"],
                "external_symbol": external_symbol,
                "source_type": meta["source_type"],
                "source_note": meta["source_note"],
            }
        )
    return specs


def _treasury_specs_from_market_data_config(schwab_config: Any) -> list[dict[str, str]]:
    configured = getattr(schwab_config, "treasury_context_quote_symbols", {}) or {}
    specs: list[dict[str, str]] = []
    for tenor in TREASURY_TENOR_ORDER:
        meta = TREASURY_TENOR_DISPLAY_META[tenor]
        external_symbol = str(configured.get(tenor, "")).strip() or _default_treasury_symbol(tenor)
        specs.append(
            {
                "tenor": tenor,
                "name": meta["name"],
                "external_symbol": external_symbol,
                "source_type": meta["source_type"],
                "source_note": meta["source_note"],
            }
        )
    return specs


def _default_treasury_specs() -> list[dict[str, str]]:
    return [
        {
            "tenor": tenor,
            "name": TREASURY_TENOR_DISPLAY_META[tenor]["name"],
            "external_symbol": _default_treasury_symbol(tenor),
            "source_type": TREASURY_TENOR_DISPLAY_META[tenor]["source_type"],
            "source_note": TREASURY_TENOR_DISPLAY_META[tenor]["source_note"],
        }
        for tenor in TREASURY_TENOR_ORDER
    ]


def _default_treasury_symbol(tenor: str) -> str:
    defaults = {
        "3M": "$IRX",
        "5Y": "$FVX",
        "10Y": "$TNX",
        "30Y": "$TYX",
    }
    return defaults[tenor]


def _treasury_empty_row(spec: dict[str, str]) -> dict[str, Any]:
    return {
        "tenor": spec["tenor"],
        "name": spec["name"],
        "external_symbol": spec["external_symbol"],
        "display_symbol": spec["external_symbol"],
        "source_type": spec["source_type"],
        "source_note": spec["source_note"],
        "current_yield": None,
        "prior_yield": None,
        "day_change_bp": None,
        "current_state": "UNAVAILABLE",
        "prior_state": "UNAVAILABLE",
        "render_classification": "UNAVAILABLE_NO_PAYLOAD",
        "matched_symbol": None,
        "matched_via": None,
        "payload_present": False,
        "yield_scale_divisor": None,
        "note": f"No direct Treasury quote payload returned for {spec['external_symbol']}.",
        "field_states": {
            "current_yield": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
            "prior_yield": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
            "day_change_bp": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
            "bid": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
            "ask": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
        },
        "raw_shape_summary": {
            "top_level_keys": [],
            "quote_keys": [],
            "reference_keys": [],
        },
        "diagnostic_codes": ["NO_SYMBOL_PAYLOAD"],
    }


def _treasury_diagnostic_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenor": row.get("tenor"),
        "requested_symbol": row.get("external_symbol"),
        "matched_symbol": row.get("matched_symbol"),
        "matched_via": row.get("matched_via"),
        "render_classification": row.get("render_classification"),
        "payload_present": row.get("payload_present", False),
        "yield_scale_divisor": row.get("yield_scale_divisor"),
        "field_states": row.get("field_states", {}),
        "diagnostic_codes": row.get("diagnostic_codes", []),
        "note": row.get("note"),
        "raw_shape_summary": row.get("raw_shape_summary"),
    }


def _treasury_curve_rows(
    raw_payload: dict[str, Any],
    specs: tuple[dict[str, str], ...] | list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    invalid_symbols = {
        str(symbol)
        for symbol in _nested_get(raw_payload, "errors", "invalidSymbols", default=[]) or []
    }
    for spec in specs:
        row = _treasury_curve_row(raw_payload, spec, invalid_symbols=invalid_symbols)
        rows.append(row)
        diagnostics.append(_treasury_diagnostic_from_row(row))
    return rows, diagnostics


def _treasury_curve_row(
    raw_payload: dict[str, Any],
    spec: dict[str, str],
    *,
    invalid_symbols: set[str],
) -> dict[str, Any]:
    external_symbol = str(spec["external_symbol"])
    resolved = _resolve_market_quote_payload(raw_payload, external_symbol)
    if resolved is None:
        row = _treasury_empty_row(spec)
        row["raw_shape_summary"]["top_level_keys"] = sorted(str(key) for key in raw_payload.keys())
        if external_symbol in invalid_symbols:
            row["render_classification"] = "UNAVAILABLE_UNSUPPORTED_SYMBOL"
            row["diagnostic_codes"] = ["INVALID_SYMBOL"]
            row["note"] = f"Schwab /quotes rejected direct symbol {external_symbol} for tenor {spec['tenor']}."
        return row

    payload = resolved["payload"]
    quote = payload.get("quote") if isinstance(payload.get("quote"), dict) else {}
    reference = payload.get("reference") if isinstance(payload.get("reference"), dict) else {}
    current_raw, current_source = _first_decimal_with_source(
        ("lastPrice", quote.get("lastPrice")),
        ("mark", quote.get("mark")),
        ("closePrice", quote.get("closePrice")),
    )
    prior_raw, prior_source = _first_decimal_with_source(("closePrice", quote.get("closePrice")),)
    change_raw, change_source = _first_decimal_with_source(("netChange", quote.get("netChange")),)
    bid, bid_source = _first_decimal_with_source(("bidPrice", quote.get("bidPrice")), ("bid", quote.get("bid")))
    ask, ask_source = _first_decimal_with_source(("askPrice", quote.get("askPrice")), ("ask", quote.get("ask")))

    scale_divisor = _treasury_yield_scale_divisor(spec["tenor"], external_symbol, reference)
    trusted_payload = scale_divisor is not None
    current_yield = (current_raw / scale_divisor) if current_raw is not None and scale_divisor is not None else None
    prior_yield = (prior_raw / scale_divisor) if prior_raw is not None and scale_divisor is not None else None
    change_bp_decimal = None
    if change_raw is not None and scale_divisor is not None:
        change_bp_decimal = (change_raw / scale_divisor) * Decimal("100")
    elif current_yield is not None and prior_yield is not None:
        change_bp_decimal = (current_yield - prior_yield) * Decimal("100")

    delayed = _market_quote_delay_flag(payload, quote)
    current_state = "LIVE" if current_yield is not None else "UNAVAILABLE"
    if delayed is True and current_yield is not None:
        current_state = "DELAYED"
    prior_state = "AVAILABLE" if prior_yield is not None else "UNAVAILABLE"
    note_parts = [spec["source_note"]]
    diagnostic_codes: list[str] = []

    if not trusted_payload:
        diagnostic_codes.append("UNTRUSTED_SYMBOL_CONTEXT")
        note_parts.append("Matched payload could not be confirmed as a direct Treasury yield source.")
    if current_yield is None:
        diagnostic_codes.append("CURRENT_YIELD_UNAVAILABLE")
    if prior_yield is None:
        diagnostic_codes.append("PRIOR_CLOSE_UNAVAILABLE")
    if change_bp_decimal is None:
        diagnostic_codes.append("DAY_CHANGE_UNAVAILABLE")
    if bid is None:
        diagnostic_codes.append("BID_UNAVAILABLE")
    if ask is None:
        diagnostic_codes.append("ASK_UNAVAILABLE")
    if delayed is True:
        note_parts.append("Feed flagged delayed by quote payload.")
    elif delayed is None:
        note_parts.append("Delay entitlement flag unavailable in payload.")
    if bid is None or ask is None:
        note_parts.append("Bid/ask unavailable from current payload.")
    render_classification = _treasury_render_classification(
        current_available=current_yield is not None,
        prior_available=prior_yield is not None,
        bid_available=bid is not None,
        ask_available=ask is not None,
        delayed=delayed is True,
        trusted_payload=trusted_payload,
    )
    return {
        "tenor": spec["tenor"],
        "name": spec["name"],
        "external_symbol": external_symbol,
        "display_symbol": str(resolved["matched_symbol"] or external_symbol),
        "source_type": spec["source_type"],
        "source_note": spec["source_note"],
        "current_yield": _yield_string(current_yield),
        "prior_yield": _yield_string(prior_yield),
        "day_change_bp": _basis_points_string(change_bp_decimal),
        "current_state": current_state,
        "prior_state": prior_state,
        "render_classification": render_classification,
        "matched_symbol": resolved["matched_symbol"],
        "matched_via": resolved["matched_via"],
        "payload_present": True,
        "yield_scale_divisor": _decimal_to_string(scale_divisor),
        "note": " ".join(part for part in note_parts if part),
        "field_states": {
            "current_yield": {
                "available": current_yield is not None,
                "status": current_state if current_yield is not None else "UNAVAILABLE",
                "source_field": current_source,
                "value": _yield_string(current_yield),
            },
            "prior_yield": {
                "available": prior_yield is not None,
                "status": prior_state,
                "source_field": prior_source,
                "value": _yield_string(prior_yield),
            },
            "day_change_bp": {
                "available": change_bp_decimal is not None,
                "status": "AVAILABLE" if change_bp_decimal is not None else "UNAVAILABLE",
                "source_field": change_source if change_raw is not None else "derived_from_current_and_prior",
                "value": _basis_points_string(change_bp_decimal),
            },
            "bid": {
                "available": bid is not None,
                "status": "AVAILABLE" if bid is not None else "UNAVAILABLE",
                "source_field": bid_source,
                "value": _decimal_to_string(bid),
            },
            "ask": {
                "available": ask is not None,
                "status": "AVAILABLE" if ask is not None else "UNAVAILABLE",
                "source_field": ask_source,
                "value": _decimal_to_string(ask),
            },
        },
        "raw_shape_summary": {
            "top_level_keys": sorted(str(key) for key in payload.keys()),
            "quote_keys": sorted(str(key) for key in quote.keys()),
            "reference_keys": sorted(str(key) for key in reference.keys()),
            "reference_description": reference.get("description"),
            "security_status": quote.get("securityStatus"),
        },
        "diagnostic_codes": diagnostic_codes or ["TREASURY_FIELDS_AVAILABLE"],
    }


def _treasury_yield_scale_divisor(tenor: str, external_symbol: str, reference: dict[str, Any]) -> Decimal | None:
    symbol = str(external_symbol or "").upper()
    description = str(reference.get("description") or "").upper()
    if symbol in {"$IRX", "$FVX", "$TNX", "$TYX"}:
        return Decimal("10")
    if tenor == "3M" and "13 WK T BILL" in description:
        return Decimal("10")
    if tenor == "5Y" and "5 YEAR T NOTE" in description:
        return Decimal("10")
    if tenor == "10Y" and "10 YR T-NOTE" in description:
        return Decimal("10")
    if tenor == "30Y" and "30 YR T BOND" in description:
        return Decimal("10")
    return None


def _treasury_render_classification(
    *,
    current_available: bool,
    prior_available: bool,
    bid_available: bool,
    ask_available: bool,
    delayed: bool,
    trusted_payload: bool,
) -> str:
    if not trusted_payload:
        return "UNAVAILABLE_UNTRUSTED"
    if not current_available:
        return "UNAVAILABLE_MISSING_CURRENT"
    if delayed:
        return "DELAYED_VALUE_ONLY" if not (bid_available and ask_available) else "DELAYED_WITH_BOOK"
    if prior_available:
        return "LIVE_WITH_COMPARISON" if not (bid_available and ask_available) else "LIVE_WITH_BOOK"
    return "LIVE_VALUE_ONLY" if not (bid_available and ask_available) else "LIVE_WITH_BOOK"


def _treasury_curve_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tenor_map = {row["tenor"]: row for row in rows}
    current_3m = _decimal_or_none(_nested_get(tenor_map.get("3M", {}), "current_yield"))
    current_5y = _decimal_or_none(_nested_get(tenor_map.get("5Y", {}), "current_yield"))
    current_10y = _decimal_or_none(_nested_get(tenor_map.get("10Y", {}), "current_yield"))
    current_30y = _decimal_or_none(_nested_get(tenor_map.get("30Y", {}), "current_yield"))
    prior_3m = _decimal_or_none(_nested_get(tenor_map.get("3M", {}), "prior_yield"))
    prior_5y = _decimal_or_none(_nested_get(tenor_map.get("5Y", {}), "prior_yield"))
    prior_10y = _decimal_or_none(_nested_get(tenor_map.get("10Y", {}), "prior_yield"))
    prior_30y = _decimal_or_none(_nested_get(tenor_map.get("30Y", {}), "prior_yield"))

    spread_3m10y = _treasury_spread_payload(current_3m, current_10y, prior_3m, prior_10y)
    spread_5s30s = _treasury_spread_payload(current_5y, current_30y, prior_5y, prior_30y)
    spread_10s30s = _treasury_spread_payload(current_10y, current_30y, prior_10y, prior_30y)
    regime_label = _treasury_regime_label(
        current_front=current_3m or current_5y,
        prior_front=prior_3m or prior_5y,
        current_long=current_30y or current_10y,
        prior_long=prior_30y or prior_10y,
        preferred_spread=spread_3m10y if spread_3m10y["status"] == "AVAILABLE" else spread_5s30s,
    )
    return {
        "tenor_summary": {
            "3M": _yield_string(current_3m),
            "5Y": _yield_string(current_5y),
            "10Y": _yield_string(current_10y),
            "30Y": _yield_string(current_30y),
        },
        "spreads": {
            "3M10Y": spread_3m10y,
            "5s30s": spread_5s30s,
            "10s30s": spread_10s30s,
        },
        "curve_state_label": regime_label,
        "spread_diagnostics": {
            "3M10Y": spread_3m10y,
            "5s30s": spread_5s30s,
            "10s30s": spread_10s30s,
        },
    }


def _treasury_spread_payload(
    current_short: Decimal | None,
    current_long: Decimal | None,
    prior_short: Decimal | None,
    prior_long: Decimal | None,
) -> dict[str, Any]:
    current_bp = ((current_long - current_short) * Decimal("100")) if current_short is not None and current_long is not None else None
    prior_bp = ((prior_long - prior_short) * Decimal("100")) if prior_short is not None and prior_long is not None else None
    day_change_bp = (current_bp - prior_bp) if current_bp is not None and prior_bp is not None else None
    return {
        "current_bp": _basis_points_string(current_bp),
        "prior_bp": _basis_points_string(prior_bp),
        "day_change_bp": _basis_points_string(day_change_bp),
        "status": "AVAILABLE" if current_bp is not None else "UNAVAILABLE",
        "computed": True,
    }


def _treasury_regime_label(
    *,
    current_front: Decimal | None,
    prior_front: Decimal | None,
    current_long: Decimal | None,
    prior_long: Decimal | None,
    preferred_spread: dict[str, Any],
) -> str:
    spread_change_bp = _decimal_or_none(preferred_spread.get("day_change_bp"))
    if spread_change_bp is None or current_front is None or prior_front is None or current_long is None or prior_long is None:
        return "INSUFFICIENT DATA"
    front_change = current_front - prior_front
    long_change = current_long - prior_long
    if front_change == 0 and long_change == 0:
        return "MIXED"
    slope = "STEEPENING" if spread_change_bp > 0 else "FLATTENING" if spread_change_bp < 0 else "MIXED"
    if slope == "MIXED":
        return "MIXED"
    if front_change > 0 and long_change > 0:
        return f"BEAR {slope}"
    if front_change < 0 and long_change < 0:
        return f"BULL {slope}"
    return "MIXED"


def _treasury_curve_chart(rows: list[dict[str, Any]]) -> dict[str, Any]:
    points = []
    for index, row in enumerate(rows):
        current_yield = _decimal_or_none(row.get("current_yield"))
        prior_yield = _decimal_or_none(row.get("prior_yield"))
        points.append(
            {
                "tenor": row["tenor"],
                "index": index,
                "current_yield": _yield_string(current_yield),
                "prior_yield": _yield_string(prior_yield),
                "current_available": current_yield is not None,
                "prior_available": prior_yield is not None,
                "current_state": row.get("current_state"),
                "prior_state": row.get("prior_state"),
            }
        )
    available_values = [
        value
        for row in rows
        for value in (_decimal_or_none(row.get("current_yield")), _decimal_or_none(row.get("prior_yield")))
        if value is not None
    ]
    y_min = min(available_values) if available_values else None
    y_max = max(available_values) if available_values else None
    return {
        "points": points,
        "y_axis": {
            "min": _yield_string(y_min),
            "max": _yield_string(y_max),
            "format": "yield_percent_3dp",
        },
        "gap_policy": "Break curve segments across unavailable tenors.",
    }


def _treasury_empty_chart(specs: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "points": [
            {
                "tenor": spec["tenor"],
                "index": index,
                "current_yield": None,
                "prior_yield": None,
                "current_available": False,
                "prior_available": False,
                "current_state": "UNAVAILABLE",
                "prior_state": "UNAVAILABLE",
            }
            for index, spec in enumerate(specs)
        ],
        "y_axis": {"min": None, "max": None, "format": "yield_percent_3dp"},
        "gap_policy": "Break curve segments across unavailable tenors.",
    }


def _treasury_curve_empty_summary() -> dict[str, Any]:
    return {
        "tenor_summary": {"3M": None, "5Y": None, "10Y": None, "30Y": None},
        "spreads": {
            "3M10Y": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
            "5s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
            "10s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
        },
        "curve_state_label": "INSUFFICIENT DATA",
        "spread_diagnostics": {
            "3M10Y": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
            "5s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
            "10s30s": {"current_bp": None, "prior_bp": None, "day_change_bp": None, "status": "UNAVAILABLE", "computed": False},
        },
    }


def _yield_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.001")))


def _basis_points_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.1")))


def _market_index_specs_from_config_path(config_path: Path) -> list[dict[str, str]]:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return _default_market_index_specs()
    configured = payload.get("market_context_quote_symbols", {}) if isinstance(payload, dict) else {}
    specs: list[dict[str, str]] = []
    for label in MARKET_INDEX_LABEL_ORDER:
        meta = MARKET_INDEX_DISPLAY_META[label]
        external_symbol = str(configured.get(label, "")).strip()
        if not external_symbol:
            external_symbol = _default_market_index_symbol(label)
        specs.append(
            {
                "label": label,
                "name": meta["name"],
                "external_symbol": external_symbol,
                "source_type": meta["source_type"],
            }
        )
    return specs


def _market_index_specs_from_market_data_config(schwab_config: Any) -> list[dict[str, str]]:
    configured = getattr(schwab_config, "market_context_quote_symbols", {}) or {}
    specs: list[dict[str, str]] = []
    for label in MARKET_INDEX_LABEL_ORDER:
        meta = MARKET_INDEX_DISPLAY_META[label]
        external_symbol = str(configured.get(label, "")).strip()
        if not external_symbol:
            external_symbol = _default_market_index_symbol(label)
        specs.append(
            {
                "label": label,
                "name": meta["name"],
                "external_symbol": external_symbol,
                "source_type": meta["source_type"],
            }
        )
    return specs


def _default_market_index_specs() -> list[dict[str, str]]:
    return [
        {
            "label": label,
            "name": MARKET_INDEX_DISPLAY_META[label]["name"],
            "external_symbol": _default_market_index_symbol(label),
            "source_type": MARKET_INDEX_DISPLAY_META[label]["source_type"],
        }
        for label in MARKET_INDEX_LABEL_ORDER
    ]


def _default_market_index_symbol(label: str) -> str:
    defaults = {
        "DJIA": "$DJI",
        "SPX": "$SPX",
        "NDX": "$NDX",
        "RUT": "$RUT",
        "GOLD": "/GC",
        "VIX": "$VIX",
    }
    return defaults[label]


def _market_index_rows(
    raw_payload: dict[str, Any],
    specs: tuple[dict[str, str], ...] | list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for spec in specs:
        row = _market_index_row(raw_payload, spec)
        rows.append(row)
        diagnostics.append(
            {
                "label": row["label"],
                "requested_symbol": row["external_symbol"],
                "display_symbol": row["display_symbol"],
                "matched_symbol": row.get("matched_symbol"),
                "matched_via": row.get("matched_via"),
                "fallback_used": row.get("fallback_used", False),
                "payload_present": row.get("payload_present", False),
                "asset_main_type": row.get("asset_main_type"),
                "realtime_flag": row.get("realtime_flag"),
                "render_classification": row.get("render_classification"),
                "state": row["state"],
                "value_state": row["value_state"],
                "bid_state": row["bid_state"],
                "ask_state": row["ask_state"],
                "diagnostic_codes": row["diagnostic_codes"],
                "field_states": row["field_states"],
                "raw_shape_summary": row.get("raw_shape_summary"),
                "note": row["note"],
            }
        )
    return rows, diagnostics


def _market_index_row(raw_payload: dict[str, Any], spec: dict[str, str]) -> dict[str, Any]:
    external_symbol = str(spec["external_symbol"])
    resolved = _resolve_market_quote_payload(raw_payload, external_symbol)
    if resolved is None:
        return {
            "label": spec["label"],
            "name": spec["name"],
            "external_symbol": external_symbol,
            "display_symbol": external_symbol,
            "source_type": spec["source_type"],
            "current_value": None,
            "absolute_change": None,
            "percent_change": None,
            "bid": None,
            "ask": None,
            "state": "UNAVAILABLE",
            "value_state": "UNAVAILABLE",
            "bid_ask_state": "UNAVAILABLE",
            "bid_state": "UNAVAILABLE",
            "ask_state": "UNAVAILABLE",
            "render_classification": "UNAVAILABLE_NO_PAYLOAD",
            "matched_symbol": None,
            "matched_via": None,
            "fallback_used": False,
            "payload_present": False,
            "asset_main_type": None,
            "realtime_flag": None,
            "diagnostic_codes": ["NO_SYMBOL_PAYLOAD"],
            "field_states": {
                "current_value": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
                "absolute_change": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
                "percent_change": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
                "bid": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
                "ask": {"available": False, "status": "UNAVAILABLE", "source_field": None, "value": None},
            },
            "raw_shape_summary": {
                "top_level_keys": sorted(str(key) for key in raw_payload.keys()),
                "matched_payload_keys": [],
                "quote_keys": [],
                "reference_keys": [],
            },
            "note": f"No quote payload returned for {external_symbol}.",
        }

    payload = resolved["payload"]
    quote = payload.get("quote") if isinstance(payload.get("quote"), dict) else {}
    reference = payload.get("reference") if isinstance(payload.get("reference"), dict) else {}
    current_value, current_source = _first_decimal_with_source(
        ("lastPrice", quote.get("lastPrice")),
        ("mark", quote.get("mark")),
        ("markPrice", quote.get("markPrice")),
        ("closePrice", quote.get("closePrice")),
    )
    absolute_change, absolute_change_source = _first_decimal_with_source(
        ("netChange", quote.get("netChange")),
        ("markChange", quote.get("markChange")),
    )
    percent_change, percent_change_source = _first_decimal_with_source(
        ("netPercentChange", quote.get("netPercentChange")),
        ("markPercentChange", quote.get("markPercentChange")),
        ("percentChange", quote.get("percentChange")),
        ("futurePercentChange", quote.get("futurePercentChange")),
    )
    if percent_change is None and current_value is not None and absolute_change is not None:
        prior_value = current_value - absolute_change
        if prior_value != 0:
            percent_change = (absolute_change / prior_value) * Decimal("100")
            percent_change_source = "derived_from_current_and_change"
    bid, bid_source = _first_decimal_with_source(
        ("bidPrice", quote.get("bidPrice")),
        ("bid", quote.get("bid")),
    )
    ask, ask_source = _first_decimal_with_source(
        ("askPrice", quote.get("askPrice")),
        ("ask", quote.get("ask")),
    )
    delayed = _market_quote_delay_flag(payload, quote)
    value_state = "LIVE" if current_value is not None else "UNAVAILABLE"
    if delayed is True:
        value_state = "DELAYED"
    state = value_state
    bid_state = "AVAILABLE" if bid is not None else "UNAVAILABLE"
    ask_state = "AVAILABLE" if ask is not None else "UNAVAILABLE"
    bid_ask_state = "AVAILABLE" if bid is not None and ask is not None else "PARTIAL" if bid is not None or ask is not None else "UNAVAILABLE"
    note_parts = ["Cash index quote via Schwab /quotes."]
    diagnostic_codes: list[str] = []
    if delayed is True:
        note_parts = ["Feed flagged delayed by quote payload."]
    elif delayed is None:
        note_parts = ["Direct Schwab /quotes fetch; entitlement delay flag unavailable in payload."]
    if current_value is None:
        diagnostic_codes.append("CURRENT_VALUE_UNAVAILABLE")
    if absolute_change is None:
        diagnostic_codes.append("ABSOLUTE_CHANGE_UNAVAILABLE")
    if percent_change is None:
        diagnostic_codes.append("PERCENT_CHANGE_UNAVAILABLE")
    if bid is None:
        diagnostic_codes.append("BID_UNAVAILABLE")
    if ask is None:
        diagnostic_codes.append("ASK_UNAVAILABLE")
    if bid_ask_state != "AVAILABLE":
        note_parts.append("Bid/ask unavailable from current payload.")
    if not diagnostic_codes:
        diagnostic_codes.append("PRIMARY_FIELDS_AVAILABLE")
    render_classification = _market_index_render_classification(
        payload_present=True,
        current_available=current_value is not None,
        absolute_change_available=absolute_change is not None,
        percent_change_available=percent_change is not None,
        bid_available=bid is not None,
        ask_available=ask is not None,
        delayed=delayed is True,
    )
    note = " ".join(note_parts)
    return {
        "label": spec["label"],
        "name": spec["name"],
        "external_symbol": external_symbol,
        "display_symbol": str(resolved["matched_symbol"] or external_symbol),
        "source_type": spec["source_type"],
        "current_value": _decimal_to_string(current_value),
        "absolute_change": _decimal_to_string(absolute_change),
        "percent_change": _percent_string(percent_change),
        "bid": _decimal_to_string(bid),
        "ask": _decimal_to_string(ask),
        "state": state,
        "value_state": value_state,
        "bid_ask_state": bid_ask_state,
        "bid_state": bid_state,
        "ask_state": ask_state,
        "render_classification": render_classification,
        "matched_symbol": resolved["matched_symbol"],
        "matched_via": resolved["matched_via"],
        "fallback_used": False,
        "payload_present": True,
        "asset_main_type": payload.get("assetMainType"),
        "realtime_flag": payload.get("realtime"),
        "diagnostic_codes": diagnostic_codes,
        "field_states": {
            "current_value": {
                "available": current_value is not None,
                "status": value_state if current_value is not None else "UNAVAILABLE",
                "source_field": current_source,
                "value": _decimal_to_string(current_value),
            },
            "absolute_change": {
                "available": absolute_change is not None,
                "status": "AVAILABLE" if absolute_change is not None else "UNAVAILABLE",
                "source_field": absolute_change_source,
                "value": _decimal_to_string(absolute_change),
            },
            "percent_change": {
                "available": percent_change is not None,
                "status": "AVAILABLE" if percent_change is not None else "UNAVAILABLE",
                "source_field": percent_change_source,
                "value": _percent_string(percent_change),
            },
            "bid": {
                "available": bid is not None,
                "status": bid_state,
                "source_field": bid_source,
                "value": _decimal_to_string(bid),
            },
            "ask": {
                "available": ask is not None,
                "status": ask_state,
                "source_field": ask_source,
                "value": _decimal_to_string(ask),
            },
        },
        "raw_shape_summary": {
            "top_level_keys": sorted(str(key) for key in payload.keys()),
            "quote_keys": sorted(str(key) for key in quote.keys()),
            "reference_keys": sorted(str(key) for key in reference.keys()),
            "security_status": quote.get("securityStatus"),
        },
        "note": note,
    }


def _resolve_market_quote_payload(payload: dict[str, Any], external_symbol: str) -> dict[str, Any] | None:
    candidates = {external_symbol, external_symbol.lstrip("$"), f"${external_symbol.lstrip('$')}"}
    for candidate in candidates:
        value = payload.get(candidate)
        if isinstance(value, dict):
            return {"payload": value, "matched_symbol": candidate, "matched_via": "top_level_key"}
    for value in payload.values():
        if not isinstance(value, dict):
            continue
        reference = value.get("reference") if isinstance(value.get("reference"), dict) else {}
        if reference.get("symbol") in candidates:
            return {"payload": value, "matched_symbol": reference.get("symbol"), "matched_via": "reference.symbol"}
        if reference.get("product") in candidates:
            return {"payload": value, "matched_symbol": reference.get("product"), "matched_via": "reference.product"}
        if value.get("symbol") in candidates:
            return {"payload": value, "matched_symbol": value.get("symbol"), "matched_via": "payload.symbol"}
    return None


def _market_quote_delay_flag(payload: dict[str, Any], quote: dict[str, Any]) -> bool | None:
    for candidate in (
        quote.get("delayed"),
        quote.get("isDelayed"),
        payload.get("delayed"),
        payload.get("isDelayed"),
    ):
        if isinstance(candidate, bool):
            return candidate
        if isinstance(candidate, str):
            normalized = candidate.strip().lower()
            if normalized in {"true", "yes", "1", "delayed"}:
                return True
            if normalized in {"false", "no", "0", "realtime", "real-time"}:
                return False
    return None


def _first_decimal_value(*values: Any) -> Decimal | None:
    for value in values:
        parsed = _decimal_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _first_decimal_with_source(*candidates: tuple[str, Any]) -> tuple[Decimal | None, str | None]:
    for source, value in candidates:
        parsed = _decimal_or_none(value)
        if parsed is not None:
            return parsed, source
    return None, None


def _market_index_render_classification(
    *,
    payload_present: bool,
    current_available: bool,
    absolute_change_available: bool,
    percent_change_available: bool,
    bid_available: bool,
    ask_available: bool,
    delayed: bool,
) -> str:
    if not payload_present:
        return "UNAVAILABLE_NO_PAYLOAD"
    if not current_available:
        return "UNAVAILABLE_NO_CURRENT"
    if not absolute_change_available or not percent_change_available:
        return "PARTIAL_PRIMARY_FIELDS"
    if bid_available and ask_available:
        return "DELAYED_WITH_BOOK" if delayed else "LIVE_WITH_BOOK"
    return "DELAYED_VALUE_ONLY" if delayed else "LIVE_VALUE_ONLY"


def _percent_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.01'))}%"


def _ascii_sparkline(values: list[Decimal]) -> str:
    if not values:
        return "No session path yet."
    if len(values) == 1:
        return "[*]"
    levels = "._-:=+*#"
    low = min(values)
    high = max(values)
    if high == low:
        return "[" + ("=" * min(len(values), 24)) + "]"
    scaled = []
    for value in values[-24:]:
        ratio = float((value - low) / (high - low))
        index = min(len(levels) - 1, max(0, int(round(ratio * (len(levels) - 1)))))
        scaled.append(levels[index])
    return "[" + "".join(scaled) + "]"


def _nested_get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _parse_json_output(output: str) -> dict[str, Any] | None:
    stripped = output.strip()
    if not stripped:
        return None
    for candidate in reversed(stripped.splitlines()):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def _resolve_sqlite_database_path(database_url: str | None) -> Path | None:
    if database_url is None or not database_url.startswith("sqlite:///"):
        return None
    raw_path = database_url.removeprefix("sqlite:///")
    return (REPO_ROOT / raw_path).resolve() if raw_path.startswith("./") else Path(raw_path).resolve()


def _read_pid(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    raw = pid_file.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _pid_running(pid_file: Path) -> bool:
    pid = _read_pid(pid_file)
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _session_date_from_status(operator_status: dict[str, Any]) -> str | None:
    timestamp = operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at")
    if not timestamp:
        return date.today().isoformat()
    parsed = _parse_iso_datetime(timestamp)
    if parsed is None:
        return date.today().isoformat()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date().isoformat()


def _freshness_semantics(last_update_ts: str | None, *, poll_interval_seconds: int, running: bool) -> dict[str, Any]:
    if last_update_ts is None:
        return {
            "status": "IDLE" if not running else "UNKNOWN",
            "stale": False if not running else True,
            "age_seconds": None,
        }
    try:
        updated = datetime.fromisoformat(last_update_ts)
    except ValueError:
        return {"status": "UNKNOWN", "stale": True, "age_seconds": None}
    age_seconds = max(0.0, (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds())
    if not running:
        return {"status": "IDLE", "stale": False, "age_seconds": age_seconds}
    if age_seconds <= poll_interval_seconds * 2:
        return {"status": "FRESH", "stale": False, "age_seconds": age_seconds}
    if age_seconds <= poll_interval_seconds * 4:
        return {"status": "AGING", "stale": False, "age_seconds": age_seconds}
    return {"status": "STALE", "stale": True, "age_seconds": age_seconds}


def _market_data_semantics(*, running: bool, market_data_ok: bool, freshness: str) -> str:
    if not running:
        return "DEAD"
    if not market_data_ok:
        return "DEAD"
    if freshness == "STALE":
        return "STALE"
    return "LIVE"


def _normalize_action_kind(action: str, ok: bool, output: str) -> str:
    lower = output.lower()
    if "already running" in lower:
        return "already_running"
    if "already halted" in lower or "already enabled" in lower or "is not running" in lower or "not faulted" in lower:
        return "no_change"
    if "stopped cleanly" in lower:
        return "stopped"
    if ok and (action.startswith("start-") or action == PAPER_RUNTIME_AUTO_RECOVERY_ACTION):
        return "started"
    if ok and action.startswith("stop-"):
        return "stopped"
    return "success" if ok else "failed"


def _normalize_action_message(action: str, ok: bool, output: str) -> str:
    human = _humanize_action(action)
    if ok:
        return f"{human}: {output}"
    return f"{human} failed: {output}"


def _humanize_action(action: str) -> str:
    labels = {
        "start-shadow": "Start Shadow",
        "stop-shadow": "Stop Shadow",
        "start-paper": "Start Paper Soak",
        "auto-start-paper": "Auto-Start Paper Soak",
        "restart-paper-with-temp-paper": "Restart Paper Soak With Temp Paper",
        "stop-paper": "Stop Paper Soak",
        "generate-daily-summary": "Generate Shadow Summary",
        "generate-paper-summary": "Generate Paper Summary",
        "auth-gate-check": "Auth Gate Check",
        "paper-halt-entries": "Paper Halt Entries",
        "paper-resume-entries": "Paper Resume Entries",
        "paper-clear-fault": "Paper Clear Fault",
        "paper-force-reconcile": "Paper Force Reconcile",
        "paper-force-lane-resume-session-override": "Force Lane Resume (Session Override)",
        "paper-flatten-and-halt": "Paper Flatten And Halt",
        "paper-stop-after-cycle": "Paper Stop After Current Cycle",
        "acknowledge-paper-risk": "Acknowledge Paper Risk",
        "acknowledge-inherited-risk": "Acknowledge Inherited Risk",
        "resolve-inherited-risk": "Resolve Inherited Risk",
        "complete-pre-session-review": "Complete Pre-Session Review",
        "sign-off-paper-session": "Sign Off Paper Session",
        "capture-paper-soak-evidence": "Capture Paper Soak Evidence",
        "refresh-status": "Refresh Status",
    }
    return labels.get(action, action)
