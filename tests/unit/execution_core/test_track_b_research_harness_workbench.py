from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_harness_workbench import (
    MNQ_LOCATION_VARIANT_CANDIDATE_ID,
    ResearchRunPlanRequest,
    TrackBResearchHarnessWorkbenchConfig,
    build_research_workbench_run_plan,
    create_track_b_research_harness_workbench,
    create_track_b_research_workbench_reuse_audit,
    run_track_b_research_workbench_candidate,
    track_b_research_candidate_registry,
)
from mgc_v05l.execution_core.track_b_snap_turn_near_miss_amplification import (
    TrackBSnapTurnLocationVariantResearchConfig,
    TrackBSnapTurnReplayBackfillConfig,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_research_harness_workbench_backlogs_single_window_location_variant(tmp_path: Path) -> None:
    research = _write_json(
        tmp_path / "location_research.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_FALSE_POSITIVE",
            "sample_count": 22,
            "sample_frame": {
                "lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC",
                "start_timestamp": "2026-05-07T00:19:00+00:00",
                "end_timestamp": "2026-05-07T18:05:00+00:00",
            },
        },
    )
    exit_sensitivity = _write_json(
        tmp_path / "exit_sensitivity.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_ALL_TESTED_POLICIES",
            "sample_count": 22,
            "sample_frame": {
                "lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC",
                "start_timestamp": "2026-05-07T00:19:00+00:00",
                "end_timestamp": "2026-05-07T18:05:00+00:00",
            },
        },
    )

    result = create_track_b_research_harness_workbench(
        config=TrackBResearchHarnessWorkbenchConfig(
            repo_root=tmp_path,
            location_variant_research_json=research,
            location_variant_exit_sensitivity_json=exit_sensitivity,
            output_json=tmp_path / "workbench.json",
            output_md=tmp_path / "workbench.md",
        ),
        now=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert result.report["workbench_status"] == "BUILDING_REUSABLE_RESEARCH_HARNESS"
    assert result.report["full_history_retest_contract"]["through_prior_friday"] == "2026-05-01"
    candidate = result.report["candidate_backlog"][0]
    assert candidate["candidate_name"] == "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"
    assert candidate["candidate_status"] == "NOT_PROMOTED"
    assert candidate["candidate_status_reasons"] == [
        "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
        "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
    ]
    assert candidate["paper_eligible"] is False
    assert candidate["delete_from_research_inventory"] is False
    assert candidate["future_retest_contract"]["data_scope"] == "maximum Track 1 1m-bar history"
    assert "Sharpe" in candidate["future_retest_contract"]["required_metrics"]
    assert result.report["broker_commands_invoked"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False


def test_research_workbench_reuse_audit_documents_shared_pipeline_and_guardrail(tmp_path: Path) -> None:
    research = _write_json(
        tmp_path / "location_research.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_FALSE_POSITIVE",
            "sample_count": 22,
            "sample_frame": {"lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC"},
        },
    )
    exit_sensitivity = _write_json(
        tmp_path / "exit_sensitivity.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_ALL_TESTED_POLICIES",
            "sample_count": 22,
            "sample_frame": {"lookback_classification": "SINGLE_WINDOW_DIAGNOSTIC"},
        },
    )

    result = create_track_b_research_workbench_reuse_audit(
        config=TrackBResearchHarnessWorkbenchConfig(
            repo_root=tmp_path,
            location_variant_research_json=research,
            location_variant_exit_sensitivity_json=exit_sensitivity,
            output_json=tmp_path / "workbench.json",
            output_md=tmp_path / "workbench.md",
            reuse_audit_json=tmp_path / "reuse_audit.json",
            reuse_audit_md=tmp_path / "reuse_audit.md",
        ),
        now=datetime(2026, 5, 8, 12, 0, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert result.report["audit_classification"] == "REUSABLE_WORKBENCH_CONTRACT_ESTABLISHED"
    assert result.report["canonical_pipeline"]["canonical_path"] == [
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
    assert result.report["candidate_onboarding"]["custom_backtest_script_required"] is False
    assert result.report["timeframe_composability"]["separate_code_paths_required"] is False
    assert result.report["exit_policy_composability"]["all_candidates_use_same_engine"] is True
    assert result.report["pnl_risk_composability"]["all_candidates_use_same_scorer"] is True
    assert result.report["gui_api_composability"]["hard_coded_ui_logic_allowed"] is False
    assert result.report["backlog_integration"]["retest_required_status_selectable"] is True
    assert "CandidateSpec + DecisionSurfaceSpec + rules.py" in result.report["drift_prevention"]["shared_contract"]
    assert result.report["guardrail"]["enforced_by_audit"] is True
    assert result.report["broker_commands_invoked"] is False
    assert result.report["paper_proof_cli_invoked"] is False


def test_research_workbench_run_plan_reuses_scorer_report_path_for_3m_5m_backlog_and_baseline() -> None:
    plan = build_research_workbench_run_plan(
        [
            ResearchRunPlanRequest(
                run_id="candidate_3m",
                run_kind="candidate",
                candidate_id="CANDIDATE_3M",
                timeframe="3m",
                exit_policy_id="TIME_BOXED_3X3M",
                lookback_id="MAX_TRACK1",
            ),
            ResearchRunPlanRequest(
                run_id="candidate_5m",
                run_kind="candidate",
                candidate_id="CANDIDATE_5M",
                timeframe="5m",
                exit_policy_id="TIME_BOXED_3X5M",
                lookback_id="MAX_TRACK1",
            ),
            ResearchRunPlanRequest(
                run_id="backlog",
                run_kind="backlog_candidate",
                candidate_id="MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
                timeframe="5m",
                exit_policy_id="FULL_GRID",
                lookback_id="MAX_TRACK1",
            ),
            ResearchRunPlanRequest(
                run_id="baseline",
                run_kind="baseline",
                candidate_id="SESSION_REGIME_MATCHED_RANDOM_BASELINE",
                timeframe="5m",
                exit_policy_id="FULL_GRID",
                lookback_id="MAX_TRACK1",
            ),
        ]
    )

    assert {row["pipeline_id"] for row in plan} == {"TRACK_B_RESEARCH_WORKBENCH_SHARED_PIPELINE_V1"}
    assert {row["pnl_risk_scorer_id"] for row in plan} == {"track_b_research_pnl_risk_scorer_v1"}
    assert {row["report_writer_id"] for row in plan} == {"track_b_research_sample_frame_report_writer_v1"}
    assert {row["exit_policy_engine_id"] for row in plan} == {"track_b_research_exit_policy_engine_v1"}
    assert {row["timeframe"] for row in plan} == {"3m", "5m"}
    assert {row["run_kind"] for row in plan} == {"candidate", "backlog_candidate", "baseline"}


def test_location_variant_is_normal_registry_candidate_and_runs_through_workbench(tmp_path: Path) -> None:
    registry = track_b_research_candidate_registry()
    candidate = registry[MNQ_LOCATION_VARIANT_CANDIDATE_ID]
    assert candidate["status"] == "NOT_PROMOTED"
    assert candidate["status_reasons"] == [
        "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
        "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
    ]
    assert candidate["paper_eligible"] is False
    assert candidate["decision_surface_id"] == "completed_5m_from_track1_1m_v1"
    assert candidate["rule_evaluator_id"] == "rules.py:mnq_first_bear_snap_turn_location_variant_v1"

    candles = []
    snapshots = []
    start = datetime(2026, 5, 7, 14, 0, tzinfo=UTC)
    for index in range(24):
        timestamp = (start + timedelta(minutes=index * 5)).isoformat()
        entry = 100 - index
        candles.append(
            {
                "candle_timestamp": timestamp,
                "timestamp": timestamp,
                "open": str(entry),
                "high": str(entry + 0.2),
                "low": str(entry - 2.0),
                "close": str(entry - 1.0),
                "volume": "100",
                "completed": True,
            }
        )
        if index < 21:
            snapshots.append(
                {
                    "decision_bar_timestamp": timestamp,
                    "instrument": "MNQ",
                    "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "side": "SHORT",
                    "session": "US",
                    "regime": "DOWNSLOPE",
                    "eligibility": {"eligible": True},
                    "feature_envelope_available": True,
                    "hard_signal": False,
                    "result": "NO_SIGNAL",
                    "bar_ohlc": {"open": str(entry), "high": str(entry + 1), "low": str(entry - 1), "close": str(entry)},
                    "proposed_entry_price": str(entry),
                    "near_miss_bucket": "ONE_PREDICATE_AWAY",
                    "failed_primitive_predicates": ["bear_snap_location_ok"],
                    "primitive_predicates": [
                        {"predicate": "bear_snap_range_ok", "threshold": "1", "actual": "2", "pass_margin": "1"},
                        {"predicate": "bear_snap_location_ok", "threshold": "1", "actual": "0", "pass_margin": "-1"},
                    ],
                }
            )
    candle_payload = _write_json(tmp_path / "mnq_5m.json", {"instrument_family": "MNQ", "candles": candles})
    snapshots_json = _write_json(tmp_path / "snapshots.json", {"snapshots": snapshots})

    result = run_track_b_research_workbench_candidate(
        candidate_id=MNQ_LOCATION_VARIANT_CANDIDATE_ID,
        config=TrackBResearchHarnessWorkbenchConfig(
            repo_root=tmp_path,
            candidate_run_json=tmp_path / "candidate_run.json",
            candidate_run_md=tmp_path / "candidate_run.md",
        ),
        replay_config=TrackBSnapTurnReplayBackfillConfig(
            repo_root=tmp_path,
            mgc_candle_payloads=(),
            mnq_candle_payloads=(candle_payload,),
        ),
        research_config=TrackBSnapTurnLocationVariantResearchConfig(
            repo_root=tmp_path,
            refresh_replay_backfill=False,
            replay_backfill_config=TrackBSnapTurnReplayBackfillConfig(
                repo_root=tmp_path,
                mgc_candle_payloads=(),
                mnq_candle_payloads=(candle_payload,),
            ),
            snapshots_json=snapshots_json,
            output_json=tmp_path / "research.json",
            output_md=tmp_path / "research.md",
            minimum_sample_count=20,
        ),
        now=datetime(2026, 5, 7, 17, 0, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert result.report["shared_pipeline_used"] is True
    assert result.report["candidate_status"] == "NOT_PROMOTED"
    assert result.report["paper_eligible"] is False
    assert result.report["legacy_bespoke_entrypoints_deprecated"] is True
    assert result.report["stage_contracts"] == {
        "snapshot_builder": "track_b_research_snapshot_builder_v1",
        "rule_evaluator": "rules.py:mnq_first_bear_snap_turn_location_variant_v1",
        "exit_policy_engine": "track_b_research_exit_policy_engine_v1",
        "pnl_risk_scorer": "track_b_research_pnl_risk_scorer_v1",
        "report_writer": "track_b_research_sample_frame_report_writer_v1",
    }
    assert result.report["location_variant_research_report"]["sample_count"] == 21
    assert result.report["location_variant_research_report"]["candidate_status"] == "NOT_PROMOTED"
    assert result.report["exit_sensitivity_report"]["candidate_status"] == "NOT_PROMOTED"
    assert result.report["broker_commands_invoked"] is False
    assert result.report["paper_proof_cli_invoked"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False
