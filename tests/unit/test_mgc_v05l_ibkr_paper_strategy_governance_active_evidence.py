from __future__ import annotations

import json
from pathlib import Path

import mgc_v05l.execution.ibkr_paper_strategy_governance as governance_module
from mgc_v05l.execution.ibkr_paper_strategy_governance import (
    GOVERNANCE_SHADOW_ONLY_NOT_BROKER_AUTHORIZED,
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
    "mes_london_late_active_participation_short": "MES",
}
LONDON_LATE_SHADOW_LANE_ID = "mnq_london_late_active_participation_short"
LONDON_LATE_PROMOTED_STRATEGY_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"


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
                "broker_session_authority_classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
                "broker_session_connection_mode": "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
                "broker_session_allowed_uses": {
                    "new_entry": True,
                    "managed_risk_reducing_close": False,
                    "broker_observed_adoption_diagnosis": True,
                },
                "broker_session_authority_blockers": [],
                "broker_session_submit_alignment": "ALIGNED",
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
            "execution_mode": "IBKR_PAPER_BRIDGE",
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "runtime_overlay_params": {
                "execution_mode": "IBKR_PAPER_BRIDGE",
                "current_order_destination": "ibkr_paper_bridge_submit_capable",
            },
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
    path.write_text(json.dumps({"profile": "mnq_mes_full_session_active_evidence", "lanes": lanes}), encoding="utf-8")


def _append_london_late_configured_lane(
    tmp_path: Path,
    *,
    submit_authority: str | None = None,
    standalone_strategy_id: str | None = None,
) -> None:
    path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    row = {
        "lane_id": LONDON_LATE_SHADOW_LANE_ID,
        "display_name": "MNQ London late active participation short",
        "symbol": "MNQ",
        "standalone_strategy_id": standalone_strategy_id,
        "strategy_family": "paper_active_evidence",
        "lane_mode": "PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE",
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "trade_size": 1,
    }
    if submit_authority is not None:
        row["submit_authority"] = submit_authority
    payload.setdefault("lanes", []).append(row)
    path.write_text(json.dumps(payload), encoding="utf-8")


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


def test_exit_coverage_gap_is_diagnostic_for_active_profile_entry_lane(tmp_path: Path, monkeypatch) -> None:
    _write_clean_governance_inputs(tmp_path)

    def _gap_report(**_: object) -> dict:
        return {
            "classification": "TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND",
            "blocked_lanes": ["mnq_us_active_participation_long"],
            "strategies": [
                {
                    "lane_id": "mnq_us_active_participation_long",
                    "classification": "EXIT_POLICY_MISSING",
                    "missing_or_weak_pieces": ["explicit_exit_policy"],
                }
            ],
        }

    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_paper_strategy_governance.build_track_b_strategy_exit_coverage_report",
        _gap_report,
    )

    artifacts = run_ibkr_paper_strategy_governance(config=_governance_config(tmp_path))
    rows = {str(row["strategy_id"]): row for row in artifacts.performance_rows}

    row = rows["mnq_us_active_participation_long"]
    assert row["submit_allowed"] is True
    assert row["bridge_invocation_allowed"] is True
    assert "strategy_exit_coverage_incomplete" not in row["submit_block_reasons"]
    assert "strategy_exit_coverage_incomplete" in row["diagnostic_submit_porting_reasons"]
    assert row["strategy_exit_coverage"]["classification"] == "EXIT_POLICY_MISSING"


def test_active_profile_submit_porting_demotes_stale_porting_and_exit_coverage_reasons(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    lane_id = "mnq_us_active_participation_long"
    row = governance_module._build_governance_row(
        config=_governance_config(tmp_path),
        now="2999-01-01T00:00:00+00:00",
        inventory_row={
            "strategy_id": lane_id,
            "standalone_strategy_id": lane_id,
            "instrument": "MNQ",
            "current_app_runtime_status": "ACTIVE_RUNTIME_READY",
            "current_position_state": "FLAT",
            "current_quantity": 0.0,
            "current_signal_state": "ENTRY_BUY",
            "blockers_to_ibkr_paper_routing": [
                "lane_not_yet_submit_ported",
                "strategy_lane_not_yet_submit_ported",
            ],
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "bridge_adapter_ready": True,
            "entries_enabled": True,
        },
        performance_row={},
        signal_row={},
        intent_row={},
        tracked_details={},
        ledger_positions=[],
        monitor_status={"health_classification": "HEALTHY", "stale": False},
        phase1_reconciliation_gate={"ready": True, "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        strategy_exit_coverage={
            "classification": "TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND",
            "strategies": [
                {
                    "lane_id": lane_id,
                    "classification": "EXIT_POLICY_MISSING",
                    "missing_or_weak_pieces": ["explicit_exit_policy"],
                }
            ],
        },
        trade_stats={},
        shared_strategy_id=None,
        global_monitor_owner="",
    )

    assert row["active_profile_submit_porting_authority"]["classification"] == "ACTIVE_PROFILE_SUBMIT_PORTING_ALLOWED"
    assert row["submit_allowed"] is True
    assert row["bridge_invocation_allowed"] is True
    assert "lane_not_yet_submit_ported" not in row["submit_block_reasons"]
    assert "strategy_exit_coverage_incomplete" not in row["submit_block_reasons"]
    assert set(row["diagnostic_submit_porting_reasons"]) == {
        "lane_not_yet_submit_ported",
        "strategy_lane_not_yet_submit_ported",
        "strategy_exit_coverage_incomplete",
    }


def test_active_profile_submit_porting_still_blocks_unrostered_lane(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)

    row = governance_module._build_governance_row(
        config=_governance_config(tmp_path),
        now="2999-01-01T00:00:00+00:00",
        inventory_row={
            "strategy_id": "unrostered_active_participation_long",
            "standalone_strategy_id": "unrostered_active_participation_long",
            "instrument": "MNQ",
            "current_app_runtime_status": "ACTIVE_RUNTIME_READY",
            "current_position_state": "FLAT",
            "current_quantity": 0.0,
            "current_signal_state": "ENTRY_BUY",
            "blockers_to_ibkr_paper_routing": ["lane_not_yet_submit_ported"],
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "bridge_adapter_ready": True,
            "entries_enabled": True,
        },
        performance_row={},
        signal_row={},
        intent_row={},
        tracked_details={},
        ledger_positions=[],
        monitor_status={"health_classification": "HEALTHY", "stale": False},
        phase1_reconciliation_gate={"ready": True, "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        strategy_exit_coverage={"classification": "TRACK_B_STRATEGY_EXIT_COVERAGE_COMPLETE", "strategies": []},
        trade_stats={},
        shared_strategy_id=None,
        global_monitor_owner="",
    )

    assert row["active_profile_submit_porting_authority"]["classification"] == "ACTIVE_PROFILE_LANE_NOT_LOADED"
    assert row["submit_allowed"] is False
    assert "lane_not_yet_submit_ported" in row["submit_block_reasons"]


def test_active_profile_submit_porting_still_blocks_non_bridge_mode(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    path = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["lanes"][0]["execution_mode"] = "SIMULATION"
    payload["lanes"][0]["runtime_overlay_params"]["execution_mode"] = "SIMULATION"
    path.write_text(json.dumps(payload), encoding="utf-8")
    lane_id = payload["lanes"][0]["lane_id"]

    row = governance_module._build_governance_row(
        config=_governance_config(tmp_path),
        now="2999-01-01T00:00:00+00:00",
        inventory_row={
            "strategy_id": lane_id,
            "standalone_strategy_id": lane_id,
            "instrument": "MNQ",
            "current_app_runtime_status": "ACTIVE_RUNTIME_READY",
            "current_position_state": "FLAT",
            "current_quantity": 0.0,
            "current_signal_state": "ENTRY_BUY",
            "blockers_to_ibkr_paper_routing": ["lane_not_yet_submit_ported"],
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "bridge_adapter_ready": True,
            "entries_enabled": True,
        },
        performance_row={},
        signal_row={},
        intent_row={},
        tracked_details={},
        ledger_positions=[],
        monitor_status={"health_classification": "HEALTHY", "stale": False},
        phase1_reconciliation_gate={"ready": True, "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        strategy_exit_coverage={"classification": "TRACK_B_STRATEGY_EXIT_COVERAGE_COMPLETE", "strategies": []},
        trade_stats={},
        shared_strategy_id=None,
        global_monitor_owner="",
    )

    assert row["active_profile_submit_porting_authority"]["classification"] == "ACTIVE_PROFILE_LANE_WRONG_EXECUTION_MODE"
    assert row["submit_allowed"] is False
    assert "lane_not_yet_submit_ported" in row["submit_block_reasons"]


def test_active_profile_submit_porting_still_blocks_missing_exit_policy(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    lane_id = "custom_loaded_lane"
    path = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["lanes"].append(
        {
            "lane_id": lane_id,
            "symbol": "MNQ",
            "strategy_family": "custom",
            "lane_mode": "CUSTOM_PAPER_LANE",
            "paper_only": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "execution_mode": "IBKR_PAPER_BRIDGE",
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "runtime_overlay_params": {
                "execution_mode": "IBKR_PAPER_BRIDGE",
                "current_order_destination": "ibkr_paper_bridge_submit_capable",
            },
        }
    )
    path.write_text(json.dumps(payload), encoding="utf-8")

    row = governance_module._build_governance_row(
        config=_governance_config(tmp_path),
        now="2999-01-01T00:00:00+00:00",
        inventory_row={
            "strategy_id": lane_id,
            "standalone_strategy_id": lane_id,
            "instrument": "MNQ",
            "current_app_runtime_status": "ACTIVE_RUNTIME_READY",
            "current_position_state": "FLAT",
            "current_quantity": 0.0,
            "current_signal_state": "ENTRY_BUY",
            "blockers_to_ibkr_paper_routing": ["lane_not_yet_submit_ported"],
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "bridge_adapter_ready": True,
            "entries_enabled": True,
        },
        performance_row={},
        signal_row={},
        intent_row={},
        tracked_details={},
        ledger_positions=[],
        monitor_status={"health_classification": "HEALTHY", "stale": False},
        phase1_reconciliation_gate={"ready": True, "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
        strategy_exit_coverage={"classification": "TRACK_B_STRATEGY_EXIT_COVERAGE_COMPLETE", "strategies": []},
        trade_stats={},
        shared_strategy_id=None,
        global_monitor_owner="",
    )

    assert row["active_profile_submit_porting_authority"]["classification"] == "ACTIVE_PROFILE_MANAGED_EXIT_POLICY_MISSING"
    assert row["submit_allowed"] is False
    assert "lane_not_yet_submit_ported" in row["submit_block_reasons"]


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


def test_london_late_shadow_only_lane_is_recognized_but_not_broker_authorized(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    _append_london_late_configured_lane(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    rows = {str(row["strategy_id"]): row for row in artifacts.performance_rows}
    row = rows[LONDON_LATE_SHADOW_LANE_ID]

    assert row["instrument"] == "MNQ"
    assert row["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert row["ibkr_bridge_submit_capable"] is True
    assert row["submit_allowed"] is False
    assert row["bridge_invocation_allowed"] is False
    assert row["strategy_status"] == "WATCHLIST"
    assert row["live_money_eligible"] is False
    assert row["submit_block_reasons"] == [GOVERNANCE_SHADOW_ONLY_NOT_BROKER_AUTHORIZED]

    status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id=LONDON_LATE_SHADOW_LANE_ID)
    assert status["selected_strategy"]["strategy_id"] == LONDON_LATE_SHADOW_LANE_ID
    assert status["submit_allowed"] is False
    assert status["block_reasons"] == [GOVERNANCE_SHADOW_ONLY_NOT_BROKER_AUTHORIZED]
    assert "paper_strategy_governance_strategy_missing" not in status["block_reasons"]


def test_future_approved_london_late_lane_can_have_broker_governance_row(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    _append_london_late_configured_lane(
        tmp_path,
        standalone_strategy_id=LONDON_LATE_PROMOTED_STRATEGY_ID,
        submit_authority="PAPER_ONLY_GUARDED_RUNTIME_AFTER_PROMOTION_CONTRACT",
    )

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    status_by_lane = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id=LONDON_LATE_SHADOW_LANE_ID)
    status_by_promoted_id = load_paper_strategy_governance_status(
        repo_root=tmp_path,
        strategy_id=LONDON_LATE_PROMOTED_STRATEGY_ID,
    )

    assert status_by_lane["selected_strategy"]["standalone_strategy_id"] == LONDON_LATE_PROMOTED_STRATEGY_ID
    assert status_by_lane["submit_allowed"] is True
    assert status_by_lane["block_reasons"] == []
    assert status_by_lane["selected_strategy"]["bridge_invocation_allowed"] is True
    assert status_by_lane["selected_strategy"]["live_money_eligible"] is False

    assert status_by_promoted_id["selected_strategy"]["strategy_id"] == LONDON_LATE_SHADOW_LANE_ID
    assert status_by_promoted_id["submit_allowed"] is True


def test_london_late_shadow_only_is_distinguishable_from_missing_governance(tmp_path: Path) -> None:
    _write_clean_governance_inputs(tmp_path)
    _append_london_late_configured_lane(tmp_path)

    config = _governance_config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    shadow_status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id=LONDON_LATE_SHADOW_LANE_ID)
    missing_status = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="unknown_active_evidence_lane")

    assert shadow_status["selected_strategy"] is not None
    assert shadow_status["block_reasons"] == [GOVERNANCE_SHADOW_ONLY_NOT_BROKER_AUTHORIZED]
    assert missing_status["selected_strategy"] is None
    assert missing_status["block_reasons"] == ["paper_strategy_governance_strategy_missing"]
