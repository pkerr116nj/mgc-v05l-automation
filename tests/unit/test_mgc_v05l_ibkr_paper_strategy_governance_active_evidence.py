from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    load_paper_strategy_governance_status,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)


ACTIVE_EVIDENCE_LANES = {
    "mnq_us_active_participation_long": "MNQ",
    "mnq_us_active_participation_short": "MNQ",
    "mes_us_active_participation_long": "MES",
    "mes_us_active_participation_short": "MES",
    "mnq_globex_active_participation_long": "MNQ",
    "mnq_globex_active_participation_short": "MNQ",
    "mes_globex_active_participation_long": "MES",
    "mes_globex_active_participation_short": "MES",
}


def _governance_config(tmp_path: Path) -> IbkrPaperStrategyGovernanceConfig:
    return IbkrPaperStrategyGovernanceConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_governance",
        var_status_path=Path("var") / "per_strategy_paper_status.json",
        var_dashboard_path=Path("var") / "strategy_probation_dashboard.json",
        var_performance_path=Path("var") / "per_strategy_paper_performance.csv",
    )


def _write_phase1_reconciliation(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "broker_reconciled": True,
                "review_required_count": 0,
                "track_b_broker_open_order_count": 0,
                "track_b_broker_position_count": 0,
                "track_b_broker_positions": [],
                "track_b_lifecycle_positions": [],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def _write_monitor(tmp_path: Path) -> None:
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "health_classification": "HEALTHY",
                "stale": False,
                "submit_allowed": True,
                "account_id": "DUM882026",
                "broker_position_quantity": 0.0,
                "ledger_position_quantity": 0.0,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "block_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def _write_backend_source_readiness(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "operator_dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_readiness_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "runtime_running": True,
                "paper_runtime_ready": True,
                "paper_trade_allowed": True,
                "market_data_stale_count": 0,
                "bar_authority_unavailable_count": 0,
                "blocking_fault_count": 0,
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "startup_control_plane_snapshot.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "overall_state": "READY"}),
        encoding="utf-8",
    )
    (output_dir / "supervised_paper_operability_snapshot.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "app_usable_for_supervised_paper": True}),
        encoding="utf-8",
    )


def _write_canonical_ready_with_legacy_loop_probe_gap(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "operator_dashboard" / "runtime"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_canonical_readiness.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "canonical_readiness": "READY_SUBMIT_CAPABLE",
                "state": "READY_SUBMIT_CAPABLE",
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "runtime": {
                    "running": True,
                    "healthy": True,
                    "runtime_ingestion_fresh": True,
                },
                "root_guard_summary": {"root_match": True},
                "broker_truth_lease": {
                    "available": True,
                    "lease_state": "ACTIVE",
                    "age_seconds": 0,
                    "broker_reconciled": True,
                    "review_required_count": 0,
                    "live_money_eligible": False,
                    "blockers": [],
                },
                "phase1_reconciliation": {
                    "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                    "fresh": True,
                    "age_seconds": 0,
                    "broker_reconciled": True,
                    "review_required_count": 0,
                    "track_b_broker_open_order_count": 0,
                    "live_money_eligible": False,
                    "blockers": [],
                },
            }
        ),
        encoding="utf-8",
    )
    runtime_truth_path = (
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_runtime_truth.json"
    )
    runtime_truth_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_truth_path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "runtime_instance_id": "paper-stack-test",
                "runtime_mode": "PAPER",
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "writer_authority": "SINGLE_WRITER",
                "freshness_state": "FRESH",
                "heartbeat_state": "HEALTHY",
                "lane_count": len(ACTIVE_EVIDENCE_LANES),
                "duplicate_writer_detection": {
                    "duplicate_writer_detected": False,
                    "duplicate_runtime_submitter_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    control_plane_dir = tmp_path / "outputs" / "track_b_execution_core" / "control_plane"
    control_plane_dir.mkdir(parents=True, exist_ok=True)
    (control_plane_dir / "latest_control_plane_snapshot.json").write_text(
        json.dumps(
            {
                "classification": "CONTROL_PLANE_READY",
                "shared_truth_coherence_status": "COHERENT",
                "control_plane_snapshot_id": "test-control-plane",
            }
        ),
        encoding="utf-8",
    )
    safe_state_dir = tmp_path / "outputs" / "track_b_execution_core" / "safe_state"
    safe_state_dir.mkdir(parents=True, exist_ok=True)
    (safe_state_dir / "latest_runtime_safe_state_envelope.json").write_text(
        json.dumps(
            {
                "classification": "SAFE_STATE_NORMAL",
                "submit_allowed": True,
                "broker_mutation_allowed": True,
                "shared_truth_generation_id": "test-shared-truth",
            }
        ),
        encoding="utf-8",
    )
    for symbol in ("MNQ", "MES"):
        candle_path = (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / symbol
            / "1m"
            / "latest_runtime_candles.json"
        )
        candle_path.parent.mkdir(parents=True, exist_ok=True)
        candle_path.write_text(
            json.dumps(
                {
                    "generated_at": "2999-01-01T00:00:00+00:00",
                    "symbol": symbol,
                    "timeframe": "1M",
                    "completed_candles_only": True,
                    "realtime_feed_confirmed": True,
                    "source_category": "PHASE1_RUNTIME_MARKET_DATA",
                    "bars": [{"timestamp": "2999-01-01T00:00:00+00:00", "close": 1.0}],
                }
            ),
            encoding="utf-8",
        )


def _write_paper_config_in_force(tmp_path: Path) -> None:
    lanes = [
        {
            "lane_id": lane_id,
            "display_name": f"{symbol} active evidence {lane_id}",
            "symbol": symbol,
            "standalone_strategy_id": lane_id,
            "strategy_family": "paper_active_evidence",
            "lane_mode": (
                "PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE"
                if "_globex_" in lane_id
                else "PAPER_ONLY_ACTIVE_EVIDENCE_LANE"
            ),
            "paper_only": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "trade_size": 1,
        }
        for lane_id, symbol in ACTIVE_EVIDENCE_LANES.items()
    ]
    lanes.append(
        {
            "lane_id": "unknown_active_evidence_lane",
            "symbol": "MNQ",
            "strategy_family": "paper_active_evidence",
            "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
            "paper_only": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "trade_size": 1,
        }
    )
    path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"lanes": lanes}), encoding="utf-8")


def _write_clean_governance_inputs(tmp_path: Path) -> None:
    _write_phase1_reconciliation(tmp_path)
    _write_monitor(tmp_path)
    _write_backend_source_readiness(tmp_path)
    _write_paper_config_in_force(tmp_path)


def test_active_evidence_lanes_generate_submit_capable_governance_rows(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    rows = {str(row["strategy_id"]): row for row in artifacts.performance_rows}
    assert ACTIVE_EVIDENCE_LANES.keys() <= rows.keys()
    assert "unknown_active_evidence_lane" not in rows

    for lane_id, symbol in ACTIVE_EVIDENCE_LANES.items():
        row = rows[lane_id]
        assert row["instrument"] == symbol
        assert row["ibkr_bridge_submit_capable"] is True
        assert row["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert row["submit_allowed"] is True
        assert row["bridge_invocation_allowed"] is True
        assert row["live_money_eligible"] is False
        assert row["strategy_status"] == "PROBATION_ACTIVE"
        assert row["submit_block_reasons"] == []

        status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id=lane_id)
        assert status["submit_allowed"] is True
        assert status["selected_strategy"]["strategy_id"] == lane_id
        assert status["selected_strategy"]["live_money_eligible"] is False


def test_active_evidence_governance_uses_canonical_ready_when_legacy_loop_probe_is_stale(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    _write_canonical_ready_with_legacy_loop_probe_gap(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)

    rows = {str(row["strategy_id"]): row for row in artifacts.performance_rows}
    for lane_id in ACTIVE_EVIDENCE_LANES:
        row = rows[lane_id]
        assert "shared_services_authority_not_ready" not in row["backend_source_readiness"]["block_reasons"]
        assert "guarded_paper_loop_process_missing" not in row["submit_block_reasons"]
        assert row["submit_allowed"] is True
        assert row["live_money_eligible"] is False


def test_unknown_lane_without_bridge_adapter_still_blocks(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="unknown_active_evidence_lane")

    assert status["submit_allowed"] is False
    assert status["selected_strategy"] is None
    assert status["block_reasons"] == ["paper_strategy_governance_strategy_missing"]
