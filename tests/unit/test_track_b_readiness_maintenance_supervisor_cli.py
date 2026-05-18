from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app import track_b_readiness_maintenance_supervisor as cli


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def seed_clean_artifacts(repo_root: Path) -> None:
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {
            "generated_at": "2026-05-18T13:30:00+00:00",
            "canonical_readiness": "READY_SUBMIT_CAPABLE",
            "ready_submit_capable": True,
            "live_money_eligible": False,
            "root_guard_summary": {"root_match": True},
            "runtime": {"running": True, "healthy": True, "eligible_lane_count": 3, "loaded_lane_count": 3},
            "market_data": {"fresh": True},
            "lane_quarantine": {"quarantine_count": 0},
            "broker_truth": {"fresh": True, "positions_complete": True, "open_orders_complete": True},
            "broker_truth_lease": {"lease_state": "ACTIVE", "live_money_eligible": False},
            "phase1_reconciliation": {
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "fresh": True,
                "broker_reconciled": True,
                "review_required_count": 0,
                "lifecycle_open_position_count": 0,
            },
        },
    )
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "lease_state": "ACTIVE",
            "submit_entry_allowed": True,
            "submit_exit_allowed": True,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root / "outputs" / "reports" / "ibkr_connectivity_watchdog" / "latest_ibkr_connectivity_watchdog.json",
        {"generated_at": "2026-05-18T13:29:58+00:00", "classification": "IBKR_CONNECTED_READ_ONLY"},
    )
    write_json(
        repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "fresh": True,
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_position_count": 0,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "source_runtime_pid": 123,
            "usable_lane_count": 3,
            "paper_lane_count": 3,
            "health": {"market_data_ok": True},
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_lane_quarantine_status.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "classification": "PAPER_LANE_QUARANTINE_CLEAR",
            "quarantine_count": 0,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "market_data_transport_probe.json",
        {"generated_at": "2026-05-18T13:29:59+00:00", "classification": "MARKET_DATA_FRESH", "fresh": True},
    )


def decision_artifact(repo_root: Path) -> Path:
    return repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"


def test_clean_ready_state_writes_artifact_and_exits_0(tmp_path: Path, capsys) -> None:
    seed_clean_artifacts(tmp_path)

    exit_code = cli.main(["--repo-root", str(tmp_path), "--json"])

    summary = json.loads(capsys.readouterr().out)
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 0
    assert summary["supervisor_state"] == "RECOVERED"
    assert artifact["recommended_actions"] == ["NO_ACTION"]
    assert artifact["live_money_eligible"] is False
    assert "canonical_readiness" in artifact["source_artifact_timestamps"]


def test_stale_legacy_market_probe_does_not_override_fresh_canonical_market_data(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    write_json(
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "market_data_transport_probe.json",
        {
            "generated_at": "2026-05-18T12:00:00+00:00",
            "classification": "DATABENTO_SOCKET_CLOSED",
            "fresh": False,
            "socket_closed": True,
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 0
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "RECOVERED"
    assert artifact["recommended_actions"] == ["NO_ACTION"]
    assert artifact["submit_block_required"] is False
    assert artifact["input_summary"]["market_data"] == "MARKET_DATA_FRESH"
    assert artifact["source_artifacts"]["market_data"].endswith("latest_canonical_readiness.json")


def test_degraded_broker_truth_exits_1(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        {
            "generated_at": "2026-05-18T13:20:00+00:00",
            "classification": "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED",
            "fresh": False,
            "positions_complete": True,
            "open_orders_complete": True,
            "last_successful_broker_truth": {"classification": "BROKER_TRUTH_REFRESH_READY"},
            "live_money_eligible": False,
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 1
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "DEGRADED"
    assert {"REFRESH_BROKER_TRUTH", "BLOCK_SUBMIT"}.issubset(artifact["recommended_actions"])


def test_expired_broker_truth_lease_uses_broker_repair_priority(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["canonical_readiness"] = "NOT_READY_DEPENDENCY"
    canonical["root_guard_summary"] = {
        "root_match": True,
        "processes": [{"name": "broker_truth_refresher", "running": False}],
    }
    canonical["market_data"] = {
        "classification": "DATABENTO_SOCKET_CLOSED",
        "fresh": False,
        "socket_closed": True,
        "source": "phase1_databento_live_listener",
    }
    write_json(canonical_path, canonical)
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {
            "generated_at": "2026-05-18T13:30:00+00:00",
            "lease_state": "EXPIRED_BLOCK_NEW_ENTRIES",
            "submit_entry_allowed": False,
            "submit_exit_allowed": False,
            "live_money_eligible": False,
        },
    )
    write_json(
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "market_data_transport_probe.json",
        {"generated_at": "2026-05-18T13:30:00+00:00", "classification": "DATABENTO_SOCKET_CLOSED", "fresh": False},
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 1
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["recommended_actions"][:3] == ["BLOCK_SUBMIT", "REFRESH_BROKER_TRUTH", "RESTART_SIDECAR"]
    assert "RECONNECT_MARKET_DATA" in artifact["recommended_actions"]
    assert artifact["blockers"][0]["code"] == "broker_truth_lease_expired"


def test_tws_not_listening_exits_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    write_json(
        tmp_path / "outputs" / "reports" / "ibkr_connectivity_watchdog" / "latest_ibkr_connectivity_watchdog.json",
        {"generated_at": "2026-05-18T13:30:00+00:00", "classification": "TWS_NOT_LISTENING"},
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 2
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "OPERATOR_REQUIRED"
    assert {"ALERT_OPERATOR", "BLOCK_SUBMIT"}.issubset(artifact["recommended_actions"])


def test_wrong_root_exits_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["canonical_readiness"] = "NOT_READY_WRONG_ROOT"
    canonical["root_guard_summary"] = {"root_match": False, "wrong_root_processes": [{"pid": 123}]}
    write_json(canonical_path, canonical)

    assert cli.main(["--repo-root", str(tmp_path)]) == 2
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "BLOCKED"
    assert artifact["blockers"][0]["code"] == "wrong_root_process"


def test_quarantined_lane_exits_1(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    write_json(
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_lane_quarantine_status.json",
        {
            "generated_at": "2026-05-18T13:30:00+00:00",
            "classification": "PAPER_LANE_QUARANTINE_RECOMMENDED",
            "lane_scoped_startup_reconciliation_failure": True,
            "quarantine_count": 1,
            "healthy_lane_count": 2,
            "quarantined_lane_ids": ["mnq-long-v5"],
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 1
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["recommended_actions"] == ["QUARANTINE_LANE"]


def test_stale_reconciliation_exits_1(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["canonical_readiness"] = "NOT_READY_RECONCILIATION"
    canonical["phase1_reconciliation"] = {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "fresh": False,
        "broker_reconciled": True,
        "review_required_count": 0,
        "lifecycle_open_position_count": 0,
        "live_money_eligible": False,
    }
    write_json(canonical_path, canonical)
    write_json(
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": "2026-05-18T13:20:00+00:00",
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "fresh": False,
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "live_money_eligible": False,
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 1
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert "REFRESH_RECONCILIATION" in artifact["recommended_actions"]


def test_raw_reconciliation_without_fresh_field_uses_canonical_clean_fresh_status(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    write_json(
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": "2026-05-18T13:29:59+00:00",
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "live_money_eligible": False,
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 0
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "RECOVERED"
    assert "REFRESH_RECONCILIATION" not in artifact["recommended_actions"]
    assert artifact["input_summary"]["reconciliation"] == "TRACK_B_PAPER_BROKER_RECONCILED"


def test_required_phase1_symbol_stale_blocks_even_if_legacy_probe_is_fresh(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["canonical_readiness"] = "NOT_READY_DEPENDENCY"
    canonical["ready_submit_capable"] = False
    canonical["market_data"] = {
        "classification": "MARKET_DATA_NOT_FRESH",
        "fresh": False,
        "market_data_ok": False,
        "source": "phase1_databento_live_listener",
        "required_symbols": ["MGC", "MNQ"],
        "required_blocked_symbols": ["MGC"],
        "blockers": [
            {
                "code": "market_data_not_fresh",
                "detail": "MGC required Phase-1 live bar is stale.",
                "source": "phase1_databento_live_listener",
                "symbol": "MGC",
            }
        ],
    }
    write_json(canonical_path, canonical)
    write_json(
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "market_data_transport_probe.json",
        {"generated_at": "2026-05-18T13:29:59+00:00", "classification": "MARKET_DATA_FRESH", "fresh": True},
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 1
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert {"RECONNECT_MARKET_DATA", "BLOCK_SUBMIT"}.issubset(artifact["recommended_actions"])
    assert artifact["blockers"][0]["code"] == "market_data_degraded"
    assert artifact["source_artifacts"]["market_data"].endswith("latest_canonical_readiness.json")


def test_operator_required_lifecycle_state_exits_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["canonical_readiness"] = "NOT_READY_RECONCILIATION"
    canonical["phase1_reconciliation"] = {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        "fresh": True,
        "broker_reconciled": False,
        "broker_flat": False,
        "lifecycle_open_position_count": 1,
        "review_required_count": 1,
        "live_money_eligible": False,
    }
    write_json(canonical_path, canonical)
    write_json(
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": "2026-05-18T13:30:00+00:00",
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "fresh": True,
            "broker_reconciled": False,
            "broker_flat": False,
            "lifecycle_open_position_count": 1,
            "review_required_count": 1,
            "live_money_eligible": False,
        },
    )

    assert cli.main(["--repo-root", str(tmp_path)]) == 2
    artifact = json.loads(decision_artifact(tmp_path).read_text(encoding="utf-8"))
    assert artifact["supervisor_state"] == "OPERATOR_REQUIRED"
    assert {"ALERT_OPERATOR", "BLOCK_SUBMIT"}.issubset(artifact["recommended_actions"])


def test_summary_exit_code_mapping() -> None:
    assert cli.exit_code_for_state("OBSERVING") == 0
    assert cli.exit_code_for_state("RECOVERED") == 0
    assert cli.exit_code_for_state("DEGRADED") == 1
    assert cli.exit_code_for_state("REPAIRING_RECOMMENDED") == 1
    assert cli.exit_code_for_state("BLOCKED") == 2
    assert cli.exit_code_for_state("OPERATOR_REQUIRED") == 2


def test_cli_source_has_no_broker_order_api_calls() -> None:
    source = Path(cli.__file__).read_text(encoding="utf-8")
    forbidden = (
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
        "globalCancel",
        "place_order",
        "submit_order",
        "broker.submit",
        "broker.cancel",
        "broker.close",
        "closePosition",
        "subprocess.run",
        "os.kill",
        "launchctl",
    )
    assert not any(token in source for token in forbidden)
