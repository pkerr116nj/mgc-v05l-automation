from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    load_paper_strategy_governance_status,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)


def _config(tmp_path: Path) -> IbkrPaperStrategyGovernanceConfig:
    return IbkrPaperStrategyGovernanceConfig(
        repo_root=tmp_path,
        output_dir=Path("outputs") / "reports" / "ibkr_strategy_governance",
        var_status_path=Path("var") / "per_strategy_paper_status.json",
        var_dashboard_path=Path("var") / "strategy_probation_dashboard.json",
        var_performance_path=Path("var") / "per_strategy_paper_performance.csv",
    )


def _write_monitor(tmp_path: Path, **overrides: object) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
        "monitor_running": True,
        "health_classification": "HEALTHY",
        "stale": False,
        "submit_allowed": True,
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "account_id": "DUM882026",
        "broker_position_quantity": 1.0,
        "ledger_position_quantity": 1.0,
        "open_order_count": 0,
        "last_successful_broker_refresh": "2026-04-28T20:14:04.229102+00:00",
        "block_reasons": [],
    }
    payload.update(overrides)
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_phase1_reconciliation(
    tmp_path: Path,
    *,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    review_required_count: int = 0,
    open_order_count: int = 0,
    block_reasons: list[str] | None = None,
) -> None:
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
                "classification": classification,
                "broker_reconciled": broker_reconciled,
                "review_required_count": review_required_count,
                "track_b_broker_open_order_count": open_order_count,
                "track_b_broker_position_count": 0,
                "track_b_broker_positions": [],
                "track_b_lifecycle_positions": [],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": block_reasons or [],
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture(autouse=True)
def _default_phase1_reconciliation(tmp_path: Path) -> None:
    _write_phase1_reconciliation(tmp_path)


def _write_ledger(tmp_path: Path) -> None:
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "account_id": "DUM882026",
                        "symbol": "MGC",
                        "expiry": "20260626",
                        "con_id": 712565978,
                        "local_symbol": "MGCM6",
                        "quantity": 1.0,
                        "side": "LONG",
                        "average_entry_price": 4586.7,
                        "unrealized_pnl": 223.03,
                        "realized_pnl": 14.18,
                    }
                ],
                "orphan_positions": [],
            }
        ),
        encoding="utf-8",
    )


def _write_dashboard(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "dashboard_api_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "paper": {
                    "status": {"stale": False},
                    "readiness": {"current_detected_session": "US_LATE"},
                    "tracked_strategies": {
                        "details_by_strategy_id": {
                            "atp_companion_v1_asia_us": {
                                "strategy_id": "atp_companion_v1_asia_us",
                                "status": "ACTIVE",
                                "realized_pnl": "-444.0",
                                "open_pnl": "223.03",
                                "trade_count": 27,
                                "winner_count": 12,
                                "loser_count": 15,
                                "win_rate": "44.44",
                                "profit_factor": "0.61",
                                "max_drawdown": "642.0",
                                "current_day_pnl": "0",
                                "latest_trade_timestamp": "2026-04-20T20:51:00+00:00",
                            }
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    _write_backend_source_readiness(tmp_path)


def _write_backend_source_readiness(
    tmp_path: Path,
    *,
    generated_at: str = "2999-01-01T00:00:00+00:00",
    runtime_running: bool = True,
    paper_runtime_ready: bool = True,
    paper_trade_allowed: bool = True,
    market_data_stale_count: int = 0,
    bar_authority_unavailable_count: int = 0,
    blocking_fault_count: int = 0,
    startup_state: str = "READY",
    launch_allowed: bool = True,
    supervised_usable: bool = True,
    temp_paper_blocked: bool = False,
    lane_eligibility_rows: list[dict[str, object]] | None = None,
) -> None:
    output_dir = tmp_path / "outputs" / "operator_dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_readiness_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "runtime_running": runtime_running,
                "paper_runtime_ready": paper_runtime_ready,
                "paper_trade_allowed": paper_trade_allowed,
                "market_data_stale_count": market_data_stale_count,
                "bar_authority_unavailable_count": bar_authority_unavailable_count,
                "blocking_fault_count": blocking_fault_count,
                "lane_eligibility_rows": list(lane_eligibility_rows or []),
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "startup_control_plane_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "overall_state": startup_state,
                "launch_allowed": launch_allowed,
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "supervised_paper_operability_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "app_usable_for_supervised_paper": supervised_usable,
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "paper_temporary_paper_runtime_integrity_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "temp_paper_blocked": temp_paper_blocked,
            }
        ),
        encoding="utf-8",
    )



def _write_canonical_readiness(
    tmp_path: Path,
    *,
    generated_at: str = "2999-01-01T00:00:00+00:00",
    state: str = "READY_SUBMIT_CAPABLE",
    runtime_running: bool = True,
    runtime_healthy: bool = True,
    runtime_ingestion_fresh: bool = True,
    root_match: bool = True,
    live_money_eligible: bool = False,
) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "track_b_canonical_readiness_v1",
                "generated_at": generated_at,
                "paper_only": True,
                "canonical_readiness": state,
                "state": state,
                "ready_submit_capable": state == "READY_SUBMIT_CAPABLE",
                "readiness_blockers": [] if state == "READY_SUBMIT_CAPABLE" else [{"code": "runtime_not_submit_capable"}],
                "readiness_warnings": [],
                "root_guard_summary": {"root_match": root_match},
                "runtime": {
                    "running": runtime_running,
                    "healthy": runtime_healthy,
                    "runtime_ingestion_fresh": runtime_ingestion_fresh,
                    "eligible_lane_count": 15 if state == "READY_SUBMIT_CAPABLE" else 0,
                    "loaded_lane_count": 15,
                    "live_money_eligible": live_money_eligible,
                },
                "broker_truth_lease": {"lease_state": "ACTIVE"},
                "phase1_reconciliation": {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
                "live_money_eligible": live_money_eligible,
            }
        ),
        encoding="utf-8",
    )

def _write_signal_audit(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "id": "ATP_COMPANION_V1_ASIA_US",
                        "lane_id": "atp_companion_v1_asia_us",
                        "instrument": "MGC",
                        "family": "active_trend_participation_engine",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "FILLED",
                        "last_actionable_signal_family": "atp_pullback_long",
                        "last_actionable_signal_timestamp": "2026-04-28T17:40:00+00:00",
                        "last_fill_timestamp": "2026-04-28T17:44:00+00:00",
                    },
                    {
                        "id": "asia_london_participation_core_v1__MGC",
                        "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "MGC",
                        "family": "asia_london_participation_core_v1",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "INSUFFICIENT_HISTORY",
                        "last_actionable_signal_family": None,
                        "last_actionable_signal_timestamp": None,
                        "last_fill_timestamp": None,
                    },
                    {
                        "id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "lane_id": "nq_1x_ny_early_core__us_late_long",
                        "instrument": "NQ",
                        "family": "index_futures_ny_intraday_forced_core_v2",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": True,
                        "audit_verdict": "READY",
                        "last_actionable_signal_family": "nyLateLong",
                        "last_actionable_signal_timestamp": "2026-04-28T17:35:00+00:00",
                        "last_fill_timestamp": None,
                        "last_recent_long_setup": True,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_strategy_performance(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "lane_id": "atp_companion_v1_asia_us",
                        "instrument": "MGC",
                        "strategy_family": "active_trend_participation_engine",
                        "standalone_strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "-444.0",
                        "trade_count": 27,
                        "day_pnl": "0",
                        "max_drawdown": "642.0",
                    },
                    {
                        "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "MGC",
                        "strategy_family": "asia_london_participation_core_v1",
                        "standalone_strategy_id": "asia_london_participation_core_v1__MGC",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "0",
                        "trade_count": 0,
                        "day_pnl": "0",
                        "max_drawdown": "0",
                    },
                    {
                        "lane_id": "nq_1x_ny_early_core__us_late_long",
                        "instrument": "NQ",
                        "strategy_family": "index_futures_ny_intraday_forced_core_v2",
                        "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__NQ",
                        "position_side": "FLAT",
                        "status": "READY",
                        "realized_pnl": "1130.0",
                        "trade_count": 7,
                        "day_pnl": "0",
                        "max_drawdown": "520.0",
                    },
                ],
                "trade_log": [
                    {"lane_id": "atp_companion_v1_asia_us", "realized_pnl": "25.0", "exit_ts": "2026-04-20T20:51:00+00:00"},
                    {"lane_id": "atp_companion_v1_asia_us", "realized_pnl": "-104.0", "exit_ts": "2026-04-19T22:11:00+00:00"},
                    {"lane_id": "nq_1x_ny_early_core__us_late_long", "realized_pnl": "675.0", "exit_ts": "2026-04-28T18:02:00+00:00"},
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_paper_config_in_force_with_canary(tmp_path: Path) -> None:
    path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "ibkr_paper_route_canary",
                        "display_name": "PAPER_ROUTE_CANARY",
                        "symbol": "MNQ",
                        "paper_only": True,
                        "non_approved": True,
                        "exclude_from_strategy_performance": True,
                        "experimental_status": "paper_route_canary",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def test_governance_builds_strategy_rows_and_status_payload(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_STRATEGY_GOVERNANCE_PARTIAL"
    atp = next(row for row in artifacts.performance_rows if row["strategy_id"] == "atp_companion_v1_asia_us")
    assert atp["current_state"] == "LONG"
    assert atp["strategy_status"] == "DEGRADED"
    assert atp["submit_allowed"] is True
    mgc = next(row for row in artifacts.performance_rows if row["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5")
    assert "conflicting_owned_position_under_other_strategy" not in mgc["submit_block_reasons"]
    assert mgc["strategy_status"] == "PROBATION_ACTIVE"
    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["strategy_status"] == "PROMISING"
    assert nq["submit_allowed"] is True
    assert nq["backend_source_readiness"]["live_ready"] is True
    assert nq["current_routing_mode"] == "IBKR_ROUTED"


def test_governance_blocks_submit_when_phase1_reconciliation_blocked_even_if_monitor_allows(tmp_path: Path) -> None:
    _write_monitor(tmp_path, submit_allowed=True, broker_position_quantity=0.0, ledger_position_quantity=0.0)
    _write_phase1_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        broker_reconciled=True,
        review_required_count=1,
        block_reasons=["review_required_present"],
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["submit_allowed"] is False
    assert "phase1_broker_reconciliation_not_clear" in nq["submit_block_reasons"]
    assert nq["legacy_monitor_authority"] == "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY"
    assert nq["phase1_broker_reconciliation_gate"]["ready"] is False
    assert artifacts.status_payload["phase1_broker_reconciliation_gate"]["ready"] is False
    assert artifacts.report["phase1_broker_reconciliation_gate"]["ready"] is False


def test_governance_ignores_stale_legacy_monitor_when_phase1_reconciliation_is_clean(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        health_classification="DISCONNECTED",
        stale=True,
        submit_allowed=False,
        block_reasons=[
            "monitor_disconnected",
            "paper_strategy_monitor_not_running",
            "paper_strategy_monitor_runtime_stale",
        ],
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["phase1_broker_reconciliation_gate"]["ready"] is True
    assert nq["legacy_monitor_authority"] == "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY"
    assert nq["submit_allowed"] is True
    assert "paper_monitor_stale" not in nq["submit_block_reasons"]
    assert "paper_monitor_not_healthy" not in nq["submit_block_reasons"]
    assert "monitor_disconnected" not in nq["submit_block_reasons"]


def test_gc_phase1_candidate_is_marked_paper_approved_without_live_money(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_position_quantity=0.0, ledger_position_quantity=0.0)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_strategy_performance(tmp_path)
    audit_path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "id": "asia_london_participation_core_v1__GC",
                        "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                        "instrument": "GC",
                        "family": "asia_london_participation_core_v1",
                        "current_strategy_status": "READY",
                        "entries_enabled": True,
                        "eligible_now": False,
                        "audit_verdict": "READY",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))
    gc = next(row for row in artifacts.performance_rows if row["strategy_id"] == "gc_1x_asia_london_participation__asia_london_long_v5")

    assert gc["strategy_approved"] is True
    assert gc["paper_strategy_approved"] is True
    assert gc["approved_phase1_strategy"] is True
    assert gc["paper_candidate_scope"] == "GC_ONLY"
    assert gc["live_money_eligible"] is False
    assert gc["ibkr_bridge_submit_capable"] is True
    assert gc["submit_allowed"] is True


def test_write_artifacts_and_load_by_bridge_strategy_id(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_strategy_governance"
    assert (output_dir / "per_strategy_paper_performance.csv").exists()
    assert (output_dir / "per_strategy_paper_status.json").exists()
    assert (output_dir / "strategy_probation_dashboard.json").exists()
    assert (output_dir / "strategy_pause_reasons.csv").exists()
    assert (output_dir / "strategy_performance_governance_report.md").exists()

    payload = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="ATP_COMPANION_V1_ASIA_US")
    assert payload["selected_strategy"]["strategy_id"] == "atp_companion_v1_asia_us"
    assert payload["selected_strategy"]["bridge_strategy_id"] == "ATP_COMPANION_V1_ASIA_US"


def test_load_status_blocks_when_file_missing(tmp_path: Path) -> None:
    payload = load_paper_strategy_governance_status(repo_root=tmp_path, strategy_id="ATP_COMPANION_V1_ASIA_US")

    assert payload["submit_allowed"] is False
    assert "paper_strategy_governance_status_missing" in payload["block_reasons"]


def test_load_status_refreshes_stale_payload_before_bridge_consumes_it(tmp_path: Path, monkeypatch) -> None:
    stale_payload = {
        "generated_at": "2026-04-29T12:28:50.338596+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
        "strategies": [],
    }
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(stale_payload), encoding="utf-8")

    refreshed_payload = {
        "generated_at": "2999-01-01T00:00:00+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "strategies": [
            {
                "strategy_id": "nq_1x_ny_early_core__us_midday_long",
                "bridge_strategy_id": "nq_1x_ny_early_core__us_midday_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            }
        ],
    }

    def _fake_run(*, config: IbkrPaperStrategyGovernanceConfig):
        class _Artifacts:
            status_payload = refreshed_payload
            performance_rows = []
            probation_dashboard = {}
            pause_rows = []
            audit_events = []
            classification = "PAPER_STRATEGY_GOVERNANCE_READY"
            report = {}

        return _Artifacts()

    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_paper_strategy_governance.run_ibkr_paper_strategy_governance",
        _fake_run,
    )

    payload = load_paper_strategy_governance_status(
        repo_root=tmp_path,
        strategy_id="nq_1x_ny_early_core__us_midday_long",
    )

    assert payload["classification"] == "PAPER_STRATEGY_GOVERNANCE_READY"
    assert payload["selected_strategy"]["strategy_id"] == "nq_1x_ny_early_core__us_midday_long"
    assert payload["submit_allowed"] is True


def test_load_status_refreshes_when_backend_readiness_artifact_is_newer(tmp_path: Path, monkeypatch) -> None:
    cached_payload = {
        "generated_at": "2999-01-01T00:00:00+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
        "strategies": [
            {
                "strategy_id": "mnq_1x_ny_early_core__us_early_long",
                "bridge_strategy_id": "index_futures_ny_intraday_forced_core_v2__MNQ",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": False,
                "submit_block_reasons": ["backend_or_source_not_live_ready"],
                "backend_source_readiness_detail": "old source stale detail",
            }
        ],
    }
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cached_payload), encoding="utf-8")
    _write_backend_source_readiness(tmp_path, generated_at="2999-01-01T00:01:00+00:00")

    refreshed_payload = {
        "generated_at": "2999-01-01T00:01:30+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "strategies": [
            {
                "strategy_id": "mnq_1x_ny_early_core__us_early_long",
                "bridge_strategy_id": "index_futures_ny_intraday_forced_core_v2__MNQ",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            }
        ],
    }
    calls = {"count": 0}

    def _fake_run(*, config: IbkrPaperStrategyGovernanceConfig):
        calls["count"] += 1

        class _Artifacts:
            status_payload = refreshed_payload
            performance_rows = []
            probation_dashboard = {}
            pause_rows = []
            audit_events = []
            classification = "PAPER_STRATEGY_GOVERNANCE_READY"
            report = {}

        return _Artifacts()

    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_paper_strategy_governance.run_ibkr_paper_strategy_governance",
        _fake_run,
    )

    payload = load_paper_strategy_governance_status(
        repo_root=tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
    )

    assert calls["count"] == 1
    assert payload["classification"] == "PAPER_STRATEGY_GOVERNANCE_READY"
    assert payload["submit_allowed"] is True


def test_governance_uses_operator_readiness_not_monitor_backend_reason_when_fresh(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
        block_reasons=["paper_runtime_stale"],
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["source"] == "operator_dashboard_readiness_artifacts"
    assert nq["backend_source_readiness"]["live_ready"] is True
    assert "paper_runtime_stale" not in nq["submit_block_reasons"]
    assert "backend_or_source_not_live_ready" not in nq["submit_block_reasons"]
    assert nq["submit_allowed"] is True



def test_governance_uses_fresh_canonical_readiness_over_stale_dashboard_snapshot(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        generated_at="2026-04-29T12:28:50.338596+00:00",
        runtime_running=False,
        paper_runtime_ready=False,
        paper_trade_allowed=False,
    )
    _write_canonical_readiness(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["source"] == "canonical_track_b_runtime_readiness"
    assert nq["backend_source_readiness"]["canonical_readiness_authoritative"] is True
    assert nq["backend_source_readiness"]["live_ready"] is True
    assert nq["backend_source_readiness"]["paper_trade_allowed"] is True
    assert nq["live_money_eligible"] is False
    assert "backend_or_source_not_live_ready" not in nq["submit_block_reasons"]
    assert nq["submit_allowed"] is True


def test_governance_blocks_when_canonical_runtime_down_even_if_dashboard_snapshot_allows(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(tmp_path)
    _write_canonical_readiness(
        tmp_path,
        state="READY_OBSERVATION_ONLY",
        runtime_running=False,
        runtime_healthy=False,
        runtime_ingestion_fresh=False,
    )
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["live_ready"] is False
    assert "canonical_readiness_not_submit_capable" in nq["backend_source_readiness"]["block_reasons"]
    assert "paper_runtime_not_running" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["submit_block_reasons"] == ["backend_or_source_not_live_ready"]
    assert nq["submit_allowed"] is False


def test_governance_blocks_when_canonical_readiness_is_stale(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(tmp_path)
    _write_canonical_readiness(tmp_path, generated_at="2026-04-29T12:28:50.338596+00:00")
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["live_ready"] is False
    assert "canonical_readiness_artifact_stale" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["submit_allowed"] is False


def test_governance_blocks_when_canonical_live_money_eligible_is_true(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(tmp_path)
    _write_canonical_readiness(tmp_path, live_money_eligible=True)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["live_ready"] is False
    assert "canonical_live_money_eligible_true" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["live_money_eligible"] is False
    assert nq["submit_allowed"] is False

def test_governance_blocks_healthy_monitor_when_source_readiness_is_not_live(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        paper_trade_allowed=False,
        market_data_stale_count=3,
    )
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["monitor_health"] == "HEALTHY"
    assert nq["monitor_stale"] is False
    assert nq["backend_source_readiness"]["live_ready"] is False
    assert nq["backend_source_readiness"]["paper_trade_allowed"] is False
    assert nq["backend_source_readiness"]["market_data_stale_count"] == 3
    assert "paper_trade_not_allowed" in nq["backend_source_readiness"]["block_reasons"]
    assert "source_market_data_stale" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["submit_block_reasons"] == ["backend_or_source_not_live_ready"]
    assert "market_data_stale_count=3" in nq["backend_source_readiness_detail"]
    assert "freshness_window_seconds=120.0" in nq["backend_source_readiness_detail"]
    assert nq["submit_allowed"] is False



def test_governance_scopes_market_data_stale_faults_to_strategy_instrument(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        market_data_stale_count=1,
        lane_eligibility_rows=[
            {
                "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                "symbol": "MGC",
                "affected_symbols": ["MGC"],
                "market_data_stale": True,
                "reason": "no_mgc_completed_bar",
            }
        ],
    )
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["readiness_scope"] == "instrument_scoped"
    assert nq["backend_source_readiness"]["required_instruments"] == ["NQ"]
    assert nq["backend_source_readiness"]["market_data_stale_count"] == 0
    assert nq["backend_source_readiness"]["global_market_data_stale_count"] == 1
    assert "source_market_data_stale" not in nq["backend_source_readiness"]["block_reasons"]
    assert "backend_or_source_not_live_ready" not in nq["submit_block_reasons"]
    assert nq["submit_allowed"] is True


def test_governance_blocks_strategy_on_own_instrument_market_data_stale_fault(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        market_data_stale_count=1,
        lane_eligibility_rows=[
            {
                "lane_id": "nq_1x_ny_early_core__us_late_long",
                "symbol": "NQ",
                "affected_symbols": ["NQ"],
                "market_data_stale": True,
                "reason": "no_nq_completed_bar",
            }
        ],
    )
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["readiness_scope"] == "instrument_scoped"
    assert nq["backend_source_readiness"]["market_data_stale_count"] == 1
    assert "source_market_data_stale" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["submit_block_reasons"] == ["backend_or_source_not_live_ready"]
    assert nq["submit_allowed"] is False


def test_governance_does_not_block_mgc_on_unrelated_nq_market_data_stale_fault(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        market_data_stale_count=1,
        lane_eligibility_rows=[
            {
                "lane_id": "nq_1x_ny_early_core__us_late_long",
                "symbol": "NQ",
                "affected_symbols": ["NQ"],
                "market_data_stale": True,
                "reason": "no_nq_completed_bar",
            }
        ],
    )
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    mgc = next(row for row in artifacts.performance_rows if row["strategy_id"] == "mgc_1x_asia_london_participation__asia_london_long_v5")
    assert mgc["backend_source_readiness"]["required_instruments"] == ["MGC"]
    assert mgc["backend_source_readiness"]["market_data_stale_count"] == 0
    assert mgc["backend_source_readiness"]["global_market_data_stale_count"] == 1
    assert "source_market_data_stale" not in mgc["backend_source_readiness"]["block_reasons"]
    assert "backend_or_source_not_live_ready" not in mgc["submit_block_reasons"]
    assert mgc["submit_allowed"] is True


def test_governance_enforces_explicit_cross_instrument_market_data_dependency(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(
        tmp_path,
        market_data_stale_count=1,
        lane_eligibility_rows=[
            {
                "lane_id": "mgc_1x_asia_london_participation__asia_london_long_v5",
                "symbol": "MGC",
                "affected_symbols": ["MGC"],
                "market_data_stale": True,
                "reason": "no_mgc_completed_bar",
            }
        ],
    )
    _write_signal_audit(tmp_path)
    signal_path = tmp_path / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
    signal_payload = json.loads(signal_path.read_text(encoding="utf-8"))
    for row in signal_payload["rows"]:
        if row["lane_id"] == "nq_1x_ny_early_core__us_late_long":
            row["required_market_data_symbols"] = ["NQ", "MGC"]
    signal_path.write_text(json.dumps(signal_payload), encoding="utf-8")
    _write_strategy_performance(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    nq = next(row for row in artifacts.performance_rows if row["strategy_id"] == "nq_1x_ny_early_core__us_late_long")
    assert nq["backend_source_readiness"]["required_instruments"] == ["MGC", "NQ"]
    assert nq["backend_source_readiness"]["market_data_stale_count"] == 1
    assert "source_market_data_stale" in nq["backend_source_readiness"]["block_reasons"]
    assert nq["submit_allowed"] is False

def test_load_status_exposes_freshness_threshold_and_age_for_stale_source(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_backend_source_readiness(tmp_path, generated_at="2026-04-29T12:28:50.338596+00:00")
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)

    payload = load_paper_strategy_governance_status(
        repo_root=tmp_path,
        strategy_id="index_futures_ny_intraday_forced_core_v2__NQ",
    )

    assert payload["submit_allowed"] is False
    assert payload["block_reasons"] == ["backend_or_source_not_live_ready"]
    assert "backend_readiness_artifact_stale" in payload["backend_source_readiness"]["block_reasons"]
    assert "freshness_window_seconds=120.0" in payload["detail"]
    assert "paper_readiness_age_seconds=" in payload["detail"]


def test_governance_includes_configured_paper_route_canary_without_performance_row(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_position_quantity=0.0,
        ledger_position_quantity=0.0,
    )
    _write_ledger(tmp_path)
    _write_dashboard(tmp_path)
    _write_signal_audit(tmp_path)
    _write_strategy_performance(tmp_path)
    _write_paper_config_in_force_with_canary(tmp_path)

    artifacts = run_ibkr_paper_strategy_governance(config=_config(tmp_path))

    canary = next(row for row in artifacts.performance_rows if row["strategy_id"] == "ibkr_paper_route_canary")
    assert canary["instrument"] == "MNQ"
    assert canary["submit_allowed"] is True
    assert canary["ibkr_bridge_submit_capable"] is True
    assert canary["current_routing_mode"] == "IBKR_ROUTED"
