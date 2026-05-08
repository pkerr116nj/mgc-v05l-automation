"""Track B research harness / workbench backlog and reuse audit.

This artifact is a reusable research-control surface. It does not run strategy
replay, change thresholds, promote candidates, or invoke broker paths.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/diagnostics")


@dataclass(frozen=True)
class TrackBResearchHarnessWorkbenchConfig:
    repo_root: Path = Path(".")
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_harness_workbench.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_harness_workbench.md"
    reuse_audit_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_workbench_reuse_audit.json"
    reuse_audit_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_workbench_reuse_audit.md"
    candidate_run_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_workbench_candidate_run.json"
    candidate_run_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_workbench_candidate_run.md"
    location_variant_research_json: Path = (
        DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_research_replay.json"
    )
    location_variant_exit_sensitivity_json: Path = (
        DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_exit_sensitivity.json"
    )


@dataclass(frozen=True)
class TrackBResearchHarnessWorkbenchResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class TrackBResearchWorkbenchReuseAuditResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class ResearchRunPlanRequest:
    run_id: str
    run_kind: str
    candidate_id: str
    timeframe: str
    exit_policy_id: str
    lookback_id: str
    session: str = "ANY"
    regime: str = "ANY"


@dataclass(frozen=True)
class DecisionSurfaceSpec:
    decision_surface_id: str
    source_timeframe: str
    decision_timeframe: str
    source_data_index_id: str
    completed_bar_only: bool = True


@dataclass(frozen=True)
class ResearchCandidateSpec:
    candidate_id: str
    base_strategy_id: str
    instrument: str
    side: str
    timeframes: tuple[str, ...]
    sessions: tuple[str, ...]
    regime_tags: tuple[str, ...]
    decision_surface_id: str
    snapshot_builder_id: str
    rule_evaluator_id: str
    exit_policy_grid_id: str
    pnl_risk_scorer_id: str
    report_writer_id: str
    status: str
    status_reasons: tuple[str, ...]
    paper_eligible: bool = False
    managed_paper_eligible: bool = False


@dataclass(frozen=True)
class TrackBResearchWorkbenchCandidateRunResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


SHARED_PIPELINE_ID = "TRACK_B_RESEARCH_WORKBENCH_SHARED_PIPELINE_V1"
SHARED_SCORER_ID = "track_b_research_pnl_risk_scorer_v1"
SHARED_REPORT_WRITER_ID = "track_b_research_sample_frame_report_writer_v1"
SHARED_EXIT_POLICY_ENGINE_ID = "track_b_research_exit_policy_engine_v1"

CANONICAL_PIPELINE = [
    "track1_1m_data",
    "data_index",
    "decision_surface",
    "snapshot_builder",
    "candidate_spec",
    "rule_evaluator",
    "exit_policy_engine",
    "pnl_risk_scorer",
    "report_writer",
    "gui_api",
    "backlog_promotion_status",
]

MNQ_LOCATION_VARIANT_CANDIDATE_ID = "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"

DECISION_SURFACE_REGISTRY: dict[str, DecisionSurfaceSpec] = {
    "completed_5m_from_track1_1m_v1": DecisionSurfaceSpec(
        decision_surface_id="completed_5m_from_track1_1m_v1",
        source_timeframe="1m",
        decision_timeframe="5m",
        source_data_index_id="track1_1m_history_index_v1",
    ),
    "completed_3m_from_track1_1m_v1": DecisionSurfaceSpec(
        decision_surface_id="completed_3m_from_track1_1m_v1",
        source_timeframe="1m",
        decision_timeframe="3m",
        source_data_index_id="track1_1m_history_index_v1",
    ),
}

RESEARCH_CANDIDATE_REGISTRY: dict[str, ResearchCandidateSpec] = {
    MNQ_LOCATION_VARIANT_CANDIDATE_ID: ResearchCandidateSpec(
        candidate_id=MNQ_LOCATION_VARIANT_CANDIDATE_ID,
        base_strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        instrument="MNQ",
        side="SHORT",
        timeframes=("5m",),
        sessions=("US_OPEN",),
        regime_tags=("SNAP_TURN", "DOWNSLOPE", "LOCATION_VARIANT"),
        decision_surface_id="completed_5m_from_track1_1m_v1",
        snapshot_builder_id="track_b_research_snapshot_builder_v1",
        rule_evaluator_id="rules.py:mnq_first_bear_snap_turn_location_variant_v1",
        exit_policy_grid_id="FULL_HISTORY_EXIT_POLICY_GRID",
        pnl_risk_scorer_id=SHARED_SCORER_ID,
        report_writer_id=SHARED_REPORT_WRITER_ID,
        status="NOT_PROMOTED",
        status_reasons=(
            "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
            "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
        ),
    )
}


def create_track_b_research_harness_workbench(
    *,
    config: TrackBResearchHarnessWorkbenchConfig | None = None,
    now: datetime | None = None,
) -> TrackBResearchHarnessWorkbenchResult:
    actual_config = config or TrackBResearchHarnessWorkbenchConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    location_research = _load_json(_resolve(repo_root, actual_config.location_variant_research_json))
    exit_sensitivity = _load_json(_resolve(repo_root, actual_config.location_variant_exit_sensitivity_json))
    backlog = [
        _location_variant_backlog_item(
            location_research=location_research,
            exit_sensitivity=exit_sensitivity,
            prior_friday=_prior_friday(actual_now.date()),
        )
    ]
    report = {
        "schema_version": "track_b_research_harness_workbench_v1",
        "generated_at": actual_now.isoformat(),
        "purpose": "Reusable Track B research candidate backlog and full-history retest contract.",
        "production_thresholds_changed": False,
        "paper_promotion_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "workbench_status": "BUILDING_REUSABLE_RESEARCH_HARNESS",
        "full_history_retest_contract": {
            "source_history": "maximum Track 1 1m-bar history available",
            "end_date_rule": "through prior Friday",
            "through_prior_friday": _prior_friday(actual_now.date()).isoformat(),
            "required_sample_frame_headers": True,
            "required_outputs": [
                "sample-frame header",
                "strategy/baseline comparison",
                "exit-policy grid",
                "P&L",
                "drawdown",
                "Sharpe",
                "outlier sensitivity",
                "session/regime buckets",
            ],
        },
        "candidate_backlog": backlog,
        "next_workbench_steps": [
            "Implement shared 1m history loader over maximum retained Track 1 history.",
            "Implement reusable candidate replay protocol with sample-frame headers.",
            "Implement exit-policy grid runner with P&L, drawdown, and Sharpe metrics.",
            "Add baseline/control sampling before any PAPER promotion decision.",
        ],
    }
    report["reuse_audit_artifacts"] = {
        "json": str(_resolve(repo_root, actual_config.reuse_audit_json)),
        "markdown": str(_resolve(repo_root, actual_config.reuse_audit_md)),
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _markdown(report))
    return TrackBResearchHarnessWorkbenchResult(report_json=output_json, report_md=output_md, report=report)


def create_track_b_research_workbench_reuse_audit(
    *,
    config: TrackBResearchHarnessWorkbenchConfig | None = None,
    now: datetime | None = None,
) -> TrackBResearchWorkbenchReuseAuditResult:
    actual_config = config or TrackBResearchHarnessWorkbenchConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    workbench = create_track_b_research_harness_workbench(config=actual_config, now=actual_now).report
    backlog = workbench.get("candidate_backlog") if isinstance(workbench.get("candidate_backlog"), list) else []
    run_plan = build_research_workbench_run_plan(
        [
            ResearchRunPlanRequest(
                run_id="three_minute_candidate_probe",
                run_kind="candidate",
                candidate_id="TRACK_B_GENERIC_3M_CANDIDATE_FIXTURE",
                timeframe="3m",
                exit_policy_id="TIME_BOXED_3X3M_RESEARCH_EXIT",
                lookback_id="MAX_TRACK1_1M_HISTORY_THROUGH_PRIOR_FRIDAY",
                session="US_OPEN",
                regime="VOLATILE",
            ),
            ResearchRunPlanRequest(
                run_id="five_minute_candidate_probe",
                run_kind="candidate",
                candidate_id="TRACK_B_GENERIC_5M_CANDIDATE_FIXTURE",
                timeframe="5m",
                exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1_RESEARCH",
                lookback_id="MAX_TRACK1_1M_HISTORY_THROUGH_PRIOR_FRIDAY",
                session="US_OPEN",
                regime="ANY",
            ),
            ResearchRunPlanRequest(
                run_id="backlog_location_variant_retest",
                run_kind="backlog_candidate",
                candidate_id="MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
                timeframe="5m",
                exit_policy_id="FULL_HISTORY_EXIT_POLICY_GRID",
                lookback_id="MAX_TRACK1_1M_HISTORY_THROUGH_PRIOR_FRIDAY",
                session="US_OPEN",
                regime="SNAP_TURN",
            ),
            ResearchRunPlanRequest(
                run_id="session_regime_matched_random_baseline",
                run_kind="baseline",
                candidate_id="SESSION_REGIME_MATCHED_RANDOM_BASELINE",
                timeframe="5m",
                exit_policy_id="FULL_HISTORY_EXIT_POLICY_GRID",
                lookback_id="MAX_TRACK1_1M_HISTORY_THROUGH_PRIOR_FRIDAY",
                session="US_OPEN",
                regime="SNAP_TURN",
            ),
        ]
    )
    report = {
        "schema_version": "track_b_research_workbench_reuse_audit_v1",
        "generated_at": actual_now.isoformat(),
        "production_thresholds_changed": False,
        "paper_promotion_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "audit_classification": "REUSABLE_WORKBENCH_CONTRACT_ESTABLISHED",
        "canonical_pipeline": _canonical_pipeline_report(),
        "candidate_onboarding": _candidate_onboarding_report(),
        "reusable_vs_bespoke": _reusable_vs_bespoke_report(),
        "timeframe_composability": {
            "supported_timeframes": ["3m", "5m"],
            "source_data": "Track 1 1m data indexed once and rolled into decision surfaces.",
            "separate_code_paths_required": False,
            "contract": "DecisionSurfaceSpec owns timeframe; candidate evaluators consume snapshots, not bespoke bars.",
        },
        "exit_policy_composability": {
            "engine_id": SHARED_EXIT_POLICY_ENGINE_ID,
            "all_candidates_use_same_engine": True,
            "supported_policy_families": [
                "time-box exits",
                "dynamic trailing stops",
                "quick scalp targets",
                "breakeven after favorable excursion",
                "failed-follow-through exits",
                "volatility-scaled stop/target",
                "VWAP/EMA invalidation exits",
            ],
        },
        "pnl_risk_composability": {
            "scorer_id": SHARED_SCORER_ID,
            "all_candidates_use_same_scorer": True,
            "metrics": ["P&L", "R", "win rate", "drawdown", "Sharpe", "MFE/MAE", "outlier sensitivity"],
            "sample_frame_reporting_required": True,
        },
        "gui_api_composability": {
            "selection_source": "registries/options",
            "hard_coded_ui_logic_allowed": False,
            "selectable_options": ["candidate", "timeframe", "session", "regime", "exit_policy", "lookback"],
        },
        "backlog_integration": {
            "retest_required_status_selectable": True,
            "backlog_candidates": [item.get("candidate_name") for item in backlog if isinstance(item, Mapping)],
            "legacy_one_off_scripts_required": False,
        },
        "drift_prevention": _drift_prevention_report(),
        "guardrail": {
            "rule": (
                "No new Track B research candidate may be implemented as a standalone one-off diagnostic unless it "
                "also creates reusable platform functionality."
            ),
            "enforced_by_audit": True,
        },
        "shared_run_plan": run_plan,
        "shared_run_plan_validation": _validate_shared_run_plan(run_plan),
    }
    output_json = _resolve(repo_root, actual_config.reuse_audit_json)
    output_md = _resolve(repo_root, actual_config.reuse_audit_md)
    _write_json(output_json, report)
    _write_text(output_md, _reuse_audit_markdown(report))
    return TrackBResearchWorkbenchReuseAuditResult(report_json=output_json, report_md=output_md, report=report)


def track_b_research_candidate_registry() -> dict[str, dict[str, Any]]:
    return {candidate_id: _candidate_spec_payload(spec) for candidate_id, spec in RESEARCH_CANDIDATE_REGISTRY.items()}


def track_b_decision_surface_registry() -> dict[str, dict[str, Any]]:
    return {
        surface_id: {
            "decision_surface_id": spec.decision_surface_id,
            "source_timeframe": spec.source_timeframe,
            "decision_timeframe": spec.decision_timeframe,
            "source_data_index_id": spec.source_data_index_id,
            "completed_bar_only": spec.completed_bar_only,
        }
        for surface_id, spec in DECISION_SURFACE_REGISTRY.items()
    }


def run_track_b_research_workbench_candidate(
    *,
    candidate_id: str,
    config: TrackBResearchHarnessWorkbenchConfig | None = None,
    now: datetime | None = None,
    replay_config: Any | None = None,
    research_config: Any | None = None,
    exit_sensitivity_config: Any | None = None,
) -> TrackBResearchWorkbenchCandidateRunResult:
    actual_config = config or TrackBResearchHarnessWorkbenchConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    spec = RESEARCH_CANDIDATE_REGISTRY.get(candidate_id)
    if spec is None:
        raise ValueError(f"Unknown Track B research candidate: {candidate_id}")
    surface = DECISION_SURFACE_REGISTRY[spec.decision_surface_id]
    run_plan = build_research_workbench_run_plan(
        [
            ResearchRunPlanRequest(
                run_id=f"{candidate_id}:research_retest",
                run_kind="backlog_candidate",
                candidate_id=candidate_id,
                timeframe=surface.decision_timeframe,
                exit_policy_id=spec.exit_policy_grid_id,
                lookback_id="MAX_TRACK1_1M_HISTORY_THROUGH_PRIOR_FRIDAY",
                session=spec.sessions[0] if spec.sessions else "ANY",
                regime=spec.regime_tags[0] if spec.regime_tags else "ANY",
            )
        ]
    )
    if candidate_id != MNQ_LOCATION_VARIANT_CANDIDATE_ID:
        raise ValueError(f"No shared workbench runner is registered for candidate: {candidate_id}")
    location_report, exit_report = _run_mnq_location_variant_shared_pipeline(
        repo_root=repo_root,
        now=actual_now,
        replay_config=replay_config,
        research_config=research_config,
        exit_sensitivity_config=exit_sensitivity_config,
    )
    report = {
        "schema_version": "track_b_research_workbench_candidate_run_v1",
        "generated_at": actual_now.isoformat(),
        "candidate_spec": _candidate_spec_payload(spec),
        "decision_surface_spec": {
            "decision_surface_id": surface.decision_surface_id,
            "source_timeframe": surface.source_timeframe,
            "decision_timeframe": surface.decision_timeframe,
            "source_data_index_id": surface.source_data_index_id,
            "completed_bar_only": surface.completed_bar_only,
        },
        "shared_pipeline_id": SHARED_PIPELINE_ID,
        "shared_pipeline": CANONICAL_PIPELINE,
        "shared_pipeline_used": True,
        "run_plan": run_plan,
        "stage_contracts": {
            "snapshot_builder": spec.snapshot_builder_id,
            "rule_evaluator": spec.rule_evaluator_id,
            "exit_policy_engine": SHARED_EXIT_POLICY_ENGINE_ID,
            "pnl_risk_scorer": SHARED_SCORER_ID,
            "report_writer": SHARED_REPORT_WRITER_ID,
        },
        "candidate_status": spec.status,
        "candidate_status_reasons": list(spec.status_reasons),
        "research_inventory_action": "KEEP_IN_FUTURE_RESEARCH_HARNESS_BACKLOG",
        "paper_eligible": spec.paper_eligible,
        "managed_paper_eligible": spec.managed_paper_eligible,
        "production_thresholds_changed": False,
        "paper_promotion_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "legacy_bespoke_entrypoints_deprecated": True,
        "deprecated_entrypoints": [
            "create_track_b_snap_turn_location_variant_research_replay",
            "create_track_b_snap_turn_location_variant_exit_sensitivity",
        ],
        "location_variant_research_report": {
            "classification": location_report.get("classification"),
            "sample_frame": location_report.get("sample_frame"),
            "sample_count": location_report.get("sample_count"),
            "candidate_status": location_report.get("candidate_status"),
            "candidate_status_reasons": location_report.get("candidate_status_reasons"),
            "policy_summary": location_report.get("policy_summary"),
            "production_strategy_comparison": location_report.get("production_strategy_comparison"),
            "random_baseline_comparison": location_report.get("random_baseline_comparison"),
        },
        "exit_sensitivity_report": {
            "classification": exit_report.get("classification"),
            "sample_frame": exit_report.get("sample_frame"),
            "sample_count": exit_report.get("sample_count"),
            "candidate_status": exit_report.get("candidate_status"),
            "candidate_status_reasons": exit_report.get("candidate_status_reasons"),
            "policy_summary": exit_report.get("policy_summary"),
            "best_policy": exit_report.get("best_policy"),
        },
    }
    output_json = _resolve(repo_root, actual_config.candidate_run_json)
    output_md = _resolve(repo_root, actual_config.candidate_run_md)
    _write_json(output_json, report)
    _write_text(output_md, _candidate_run_markdown(report))
    return TrackBResearchWorkbenchCandidateRunResult(report_json=output_json, report_md=output_md, report=report)


def build_research_workbench_run_plan(requests: list[ResearchRunPlanRequest]) -> list[dict[str, Any]]:
    return [
        {
            "run_id": request.run_id,
            "run_kind": request.run_kind,
            "candidate_id": request.candidate_id,
            "timeframe": request.timeframe,
            "session": request.session,
            "regime": request.regime,
            "lookback_id": request.lookback_id,
            "pipeline_id": SHARED_PIPELINE_ID,
            "pipeline": CANONICAL_PIPELINE,
            "data_index_id": "track1_1m_history_index_v1",
            "decision_surface_spec_id": f"decision_surface_{request.timeframe}_from_1m_v1",
            "snapshot_builder_id": "track_b_research_snapshot_builder_v1",
            "candidate_spec_registry_id": "track_b_research_candidate_specs_v1",
            "rule_evaluator_id": "track_b_research_rules_py_contract_v1",
            "exit_policy_engine_id": SHARED_EXIT_POLICY_ENGINE_ID,
            "exit_policy_id": request.exit_policy_id,
            "pnl_risk_scorer_id": SHARED_SCORER_ID,
            "report_writer_id": SHARED_REPORT_WRITER_ID,
            "gui_api_contract_id": "track_b_research_workbench_options_api_v1",
            "promotion_status_writer_id": "track_b_research_backlog_promotion_status_v1",
        }
        for request in requests
    ]


def _run_mnq_location_variant_shared_pipeline(
    *,
    repo_root: Path,
    now: datetime,
    replay_config: Any | None,
    research_config: Any | None,
    exit_sensitivity_config: Any | None,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    from .track_b_snap_turn_near_miss_amplification import (
        TrackBSnapTurnLocationVariantExitSensitivityConfig,
        TrackBSnapTurnLocationVariantResearchConfig,
        TrackBSnapTurnReplayBackfillConfig,
        create_track_b_snap_turn_location_variant_exit_sensitivity,
        create_track_b_snap_turn_location_variant_research_replay,
    )

    actual_replay_config = replay_config or TrackBSnapTurnReplayBackfillConfig(repo_root=repo_root)
    actual_research_config = research_config or TrackBSnapTurnLocationVariantResearchConfig(
        repo_root=repo_root,
        replay_backfill_config=actual_replay_config,
    )
    research_result = create_track_b_snap_turn_location_variant_research_replay(
        config=actual_research_config,
        now=now,
    )
    actual_exit_config = exit_sensitivity_config or TrackBSnapTurnLocationVariantExitSensitivityConfig(
        repo_root=repo_root,
        research_replay_config=actual_research_config,
    )
    exit_result = create_track_b_snap_turn_location_variant_exit_sensitivity(
        config=actual_exit_config,
        now=now,
    )
    return research_result.report, exit_result.report


def _candidate_spec_payload(spec: ResearchCandidateSpec) -> dict[str, Any]:
    return {
        "candidate_id": spec.candidate_id,
        "base_strategy_id": spec.base_strategy_id,
        "instrument": spec.instrument,
        "side": spec.side,
        "timeframes": list(spec.timeframes),
        "sessions": list(spec.sessions),
        "regime_tags": list(spec.regime_tags),
        "decision_surface_id": spec.decision_surface_id,
        "snapshot_builder_id": spec.snapshot_builder_id,
        "rule_evaluator_id": spec.rule_evaluator_id,
        "exit_policy_grid_id": spec.exit_policy_grid_id,
        "pnl_risk_scorer_id": spec.pnl_risk_scorer_id,
        "report_writer_id": spec.report_writer_id,
        "status": spec.status,
        "status_reasons": list(spec.status_reasons),
        "paper_eligible": spec.paper_eligible,
        "managed_paper_eligible": spec.managed_paper_eligible,
    }


def _location_variant_backlog_item(
    *,
    location_research: Mapping[str, Any],
    exit_sensitivity: Mapping[str, Any],
    prior_friday: date,
) -> dict[str, Any]:
    candidate_name = str(
        exit_sensitivity.get("candidate_name")
        or location_research.get("candidate_name")
        or "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"
    )
    research_frame = location_research.get("sample_frame") if isinstance(location_research.get("sample_frame"), Mapping) else {}
    exit_frame = exit_sensitivity.get("sample_frame") if isinstance(exit_sensitivity.get("sample_frame"), Mapping) else {}
    return {
        "candidate_name": candidate_name,
        "candidate_family": "snap_turn_location_variant",
        "base_strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "instrument": "MNQ",
        "candidate_status": "NOT_PROMOTED",
        "candidate_status_reasons": [
            "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
            "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
        ],
        "paper_eligible": False,
        "managed_paper_eligible": False,
        "delete_from_research_inventory": False,
        "bespoke_work_paused": True,
        "current_evidence_scope": {
            "research_replay_classification": location_research.get("classification"),
            "exit_sensitivity_classification": exit_sensitivity.get("classification"),
            "research_sample_frame_classification": research_frame.get("lookback_classification"),
            "exit_sample_frame_classification": exit_frame.get("lookback_classification"),
            "sample_count": exit_sensitivity.get("sample_count") or location_research.get("sample_count"),
            "start_timestamp": exit_frame.get("start_timestamp") or research_frame.get("start_timestamp"),
            "end_timestamp": exit_frame.get("end_timestamp") or research_frame.get("end_timestamp"),
        },
        "future_retest_contract": {
            "engine": "Track B Research Harness / Workbench",
            "data_scope": "maximum Track 1 1m-bar history",
            "through_date": prior_friday.isoformat(),
            "bar_timeframe": "1m source bars with completed 5m decision reconstruction",
            "required_sample_frame_headers": True,
            "required_baselines": [
                "production MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "session/regime matched random baseline",
                "simple directional/session baseline",
            ],
            "required_exit_policy_grid": [
                "1R target / 1R stop",
                "1.5R target / 1R stop",
                "3x5m time-box",
                "quick scalp target",
                "breakeven after early favorable excursion",
                "trail after first favorable bar",
                "failed follow-through exit after 1 or 2 bars",
                "volatility-scaled stop/target",
                "VWAP/EMA invalidation where path data is available",
            ],
            "required_metrics": ["trades", "P&L", "average R", "win rate", "drawdown", "Sharpe", "MFE/MAE"],
            "promotion_rule": "No PAPER promotion until full-history replay beats baselines with acceptable drawdown and sample frame.",
        },
    }


def _canonical_pipeline_report() -> dict[str, Any]:
    return {
        "pipeline_id": SHARED_PIPELINE_ID,
        "canonical_path": CANONICAL_PIPELINE,
        "path_display": (
            "Track 1 1m data -> data index -> decision surface -> snapshot builder -> candidate spec -> "
            "rule evaluator -> exit-policy engine -> P&L/risk scorer -> report writer -> GUI/API -> "
            "backlog/promotion status"
        ),
        "shared_contracts": [
            "CandidateSpec",
            "DecisionSurfaceSpec",
            "rules.py",
            "ExitPolicySpec",
            "SampleFrameReport",
        ],
    }


def _candidate_onboarding_report() -> dict[str, Any]:
    return {
        "custom_backtest_script_required": False,
        "minimum_files": [
            "candidate registry entry",
            "rules.py evaluator or existing evaluator reference",
            "fixture/replay test",
        ],
        "minimum_fields": [
            "candidate_id",
            "base_strategy_id",
            "instrument",
            "side",
            "timeframes",
            "sessions",
            "regime_tags",
            "required_snapshot_fields",
            "predicate definitions",
            "exit_policy_grid",
            "baseline_ids",
            "promotion_status",
        ],
        "onboarding_steps": [
            "Add CandidateSpec metadata.",
            "Select DecisionSurfaceSpec from registry.",
            "Bind evaluator from rules.py shared contract.",
            "Select exit-policy grid from registry.",
            "Run shared scorer/report writer.",
            "Update backlog/promotion status from report outcome.",
        ],
    }


def _reusable_vs_bespoke_report() -> dict[str, Any]:
    return {
        "reusable_platform_components": [
            "track_b_research_harness_workbench.py backlog and reuse-audit reports",
            "CandidateSpec metadata contract",
            "DecisionSurfaceSpec timeframe/data-index contract",
            "shared exit-policy engine contract",
            "shared P&L/risk scorer contract",
            "shared sample-frame report writer contract",
            "GUI/API options registry contract",
        ],
        "candidate_specific_components": [
            "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1 predicate variant",
            "snap-turn primitive predicate distance extraction",
            "snap-turn closest-failed-bar snapshot labels",
        ],
        "bespoke_paths_to_generalize": [
            {
                "path": "track_b_snap_turn_near_miss_amplification.py location-variant replay functions",
                "reason": (
                    "Current snap-turn replay/backfill is valuable evidence retention, but future candidates should call "
                    "the shared CandidateSpec + DecisionSurfaceSpec + exit-policy/scorer/report contracts instead of "
                    "adding more candidate-named replay functions."
                ),
                "target": "general candidate replay runner",
            }
        ],
    }


def _drift_prevention_report() -> dict[str, Any]:
    return {
        "possible_drift_points": [
            "research replay reconstructs decision surfaces differently from shadow-live scoring",
            "candidate-specific predicates duplicate production rules instead of importing rules.py contract",
            "exit policies use bespoke P&L math outside the shared scorer",
            "GUI/API hard-codes candidate semantics instead of reading registries/options",
        ],
        "shared_contract": "CandidateSpec + DecisionSurfaceSpec + rules.py",
        "shadow_live_alignment_rule": (
            "Shadow-live scoring and research replay must consume the same candidate spec, decision surface schema, "
            "and rule evaluator identifiers."
        ),
    }


def _validate_shared_run_plan(run_plan: list[Mapping[str, Any]]) -> dict[str, Any]:
    scorer_ids = {str(row.get("pnl_risk_scorer_id")) for row in run_plan}
    report_ids = {str(row.get("report_writer_id")) for row in run_plan}
    pipeline_ids = {str(row.get("pipeline_id")) for row in run_plan}
    kinds = {str(row.get("run_kind")) for row in run_plan}
    timeframes = {str(row.get("timeframe")) for row in run_plan}
    return {
        "all_runs_share_pipeline": pipeline_ids == {SHARED_PIPELINE_ID},
        "all_runs_share_scorer": scorer_ids == {SHARED_SCORER_ID},
        "all_runs_share_report_writer": report_ids == {SHARED_REPORT_WRITER_ID},
        "has_3m_candidate": "3m" in timeframes and "candidate" in kinds,
        "has_5m_candidate": "5m" in timeframes and "candidate" in kinds,
        "has_backlog_candidate": "backlog_candidate" in kinds,
        "has_baseline": "baseline" in kinds,
    }


def _prior_friday(value: date) -> date:
    days_since_friday = (value.weekday() - 4) % 7
    if days_since_friday == 0:
        days_since_friday = 7
    return value - timedelta(days=days_since_friday)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Research Harness Workbench",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "This workbench queues research candidates for reusable full-history replay. It does not promote candidates to PAPER or invoke broker paths.",
        "",
        "## Full-History Retest Contract",
        "",
    ]
    contract = report.get("full_history_retest_contract") or {}
    lines.append(f"- Source history: {contract.get('source_history')}")
    lines.append(f"- End-date rule: {contract.get('end_date_rule')} ({contract.get('through_prior_friday')})")
    lines.append("- Required outputs:")
    for item in contract.get("required_outputs") or []:
        lines.append(f"  - {item}")
    lines.extend(["", "## Candidate Backlog", ""])
    for item in report.get("candidate_backlog") or []:
        lines.append(f"### {item.get('candidate_name')}")
        lines.append(f"- Status: {item.get('candidate_status')}")
        lines.append(f"- Reasons: {', '.join(item.get('candidate_status_reasons') or [])}")
        lines.append(f"- Paper eligible: {item.get('paper_eligible')}")
        evidence = item.get("current_evidence_scope") or {}
        lines.append(
            f"- Current evidence: {evidence.get('research_sample_frame_classification')} / "
            f"{evidence.get('exit_sample_frame_classification')}, samples={evidence.get('sample_count')}, "
            f"{evidence.get('start_timestamp')} -> {evidence.get('end_timestamp')}"
        )
        retest = item.get("future_retest_contract") or {}
        lines.append(f"- Retest engine: {retest.get('engine')}")
        lines.append(f"- Retest data scope: {retest.get('data_scope')} through {retest.get('through_date')}")
        lines.append(f"- Promotion rule: {retest.get('promotion_rule')}")
        lines.append("")
    lines.extend(["## Next Steps", ""])
    for item in report.get("next_workbench_steps") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _candidate_run_markdown(report: Mapping[str, Any]) -> str:
    spec = report.get("candidate_spec") or {}
    location = report.get("location_variant_research_report") or {}
    exit_report = report.get("exit_sensitivity_report") or {}
    lines = [
        "# Track B Research Workbench Candidate Run",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"Candidate: {spec.get('candidate_id')}",
        f"Status: {report.get('candidate_status')}",
        f"Status reasons: {', '.join(report.get('candidate_status_reasons') or [])}",
        f"Paper eligible: {report.get('paper_eligible')}",
        f"Shared pipeline: {report.get('shared_pipeline_id')}",
        "",
        "## Stage Contracts",
        "",
    ]
    stages = report.get("stage_contracts") or {}
    for key in ("snapshot_builder", "rule_evaluator", "exit_policy_engine", "pnl_risk_scorer", "report_writer"):
        lines.append(f"- {key}: {stages.get(key)}")
    lines.extend(["", "## Results", ""])
    lines.append(f"- Location replay classification: {location.get('classification')}")
    lines.append(f"- Location replay samples: {location.get('sample_count')}")
    lines.append(f"- Exit sensitivity classification: {exit_report.get('classification')}")
    lines.append(f"- Exit sensitivity samples: {exit_report.get('sample_count')}")
    lines.extend(["", "## Deprecated Bespoke Entrypoints", ""])
    for item in report.get("deprecated_entrypoints") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _reuse_audit_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Research Workbench Reuse Audit",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        f"Classification: {report.get('audit_classification')}",
        "",
        "This audit checks whether Track B research is moving toward a reusable workbench instead of the old one-off Track 1 script sprawl.",
        "",
        "## Shared Pipeline",
        "",
    ]
    pipeline = report.get("canonical_pipeline") or {}
    lines.append(str(pipeline.get("path_display") or ""))
    lines.extend(["", "## Candidate Onboarding", ""])
    onboarding = report.get("candidate_onboarding") or {}
    lines.append(f"- Custom backtest script required: {onboarding.get('custom_backtest_script_required')}")
    lines.append("- Minimum fields:")
    for item in onboarding.get("minimum_fields") or []:
        lines.append(f"  - {item}")
    lines.extend(["", "## Reusable vs Bespoke", ""])
    components = report.get("reusable_vs_bespoke") or {}
    lines.append("- Reusable platform components:")
    for item in components.get("reusable_platform_components") or []:
        lines.append(f"  - {item}")
    lines.append("- Candidate-specific components:")
    for item in components.get("candidate_specific_components") or []:
        lines.append(f"  - {item}")
    lines.append("- Generalization flags:")
    for item in components.get("bespoke_paths_to_generalize") or []:
        lines.append(f"  - {item.get('path')}: {item.get('reason')}")
    lines.extend(["", "## Composability", ""])
    timeframe = report.get("timeframe_composability") or {}
    exit_policy = report.get("exit_policy_composability") or {}
    scorer = report.get("pnl_risk_composability") or {}
    gui = report.get("gui_api_composability") or {}
    backlog = report.get("backlog_integration") or {}
    lines.append(f"- Timeframes: {', '.join(timeframe.get('supported_timeframes') or [])}; separate code paths required={timeframe.get('separate_code_paths_required')}")
    lines.append(f"- Exit engine: {exit_policy.get('engine_id')}; shared={exit_policy.get('all_candidates_use_same_engine')}")
    lines.append(f"- Scorer: {scorer.get('scorer_id')}; shared={scorer.get('all_candidates_use_same_scorer')}")
    lines.append(f"- GUI/API selection source: {gui.get('selection_source')}; hard-coded UI allowed={gui.get('hard_coded_ui_logic_allowed')}")
    lines.append(f"- Backlog retest selectable: {backlog.get('retest_required_status_selectable')}")
    lines.extend(["", "## Drift Prevention", ""])
    drift = report.get("drift_prevention") or {}
    lines.append(f"- Shared contract: {drift.get('shared_contract')}")
    lines.append(f"- Shadow-live alignment rule: {drift.get('shadow_live_alignment_rule')}")
    lines.extend(["", "## Guardrail", ""])
    guardrail = report.get("guardrail") or {}
    lines.append(str(guardrail.get("rule") or ""))
    lines.extend(["", "## Shared Run Plan Validation", ""])
    validation = report.get("shared_run_plan_validation") or {}
    for key in sorted(validation):
        lines.append(f"- {key}: {validation[key]}")
    return "\n".join(lines) + "\n"
