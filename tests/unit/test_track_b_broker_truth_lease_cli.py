from __future__ import annotations

import inspect
import json
from pathlib import Path

from mgc_v05l.app import track_b_broker_truth_lease as cli


TRUTH_TIME = "2026-05-18T14:58:00+00:00"
RECON_TIME = "2026-05-18T14:58:30+00:00"


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def seed_clean_artifacts(repo_root: Path) -> None:
    write_json(
        broker_status_path(repo_root),
        {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "generated_at": TRUTH_TIME,
            "positions_complete": True,
            "open_orders_complete": True,
            "positions": [
                {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "0.0"},
                {"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "0.0"},
            ],
            "open_orders": [],
            "last_successful_broker_truth": {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "account": "DUM882026",
                "generated_at": TRUTH_TIME,
                "positions_complete": True,
                "open_orders_complete": True,
                "positions": [
                    {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "0.0"},
                    {"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "0.0"},
                ],
                "open_orders": [],
                "positions_snapshot_path": "outputs/reports/positions.json",
                "open_orders_snapshot_path": "outputs/reports/open_orders.json",
                "live_money_eligible": False,
                "submit_authority": False,
            },
            "latest_attempt_status": {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "account": "DUM882026",
                "generated_at": TRUTH_TIME,
                "positions_complete": True,
                "open_orders_complete": True,
                "positions": [
                    {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "0.0"},
                    {"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "0.0"},
                ],
                "open_orders": [],
                "live_money_eligible": False,
            },
            "live_money_eligible": False,
        },
    )
    write_json(
        reconciliation_path(repo_root),
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": RECON_TIME,
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": True},
            "live_money_eligible": False,
            "submit_authority": False,
        },
    )
    write_json(
        lifecycle_path(repo_root),
        {
            "generated_at": RECON_TIME,
            "open_position_count": 0,
            "open_positions": [],
            "manual_broker_action_detected": False,
            "live_money_eligible": False,
        },
    )
    write_json(
        order_state_path(repo_root),
        {
            "generated_at": RECON_TIME,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {"generated_at": RECON_TIME, "canonical_readiness": "READY_OBSERVATION_ONLY", "live_money_eligible": False},
    )
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json",
        {"generated_at": RECON_TIME, "supervisor_state": "OBSERVING", "live_money_eligible": False},
    )


def base_args(repo_root: Path, current_time: str = "2026-05-18T15:00:00+00:00") -> list[str]:
    return [
        "--repo-root",
        str(repo_root),
        "--current-time",
        current_time,
        "--max-entry-age-seconds",
        "300",
        "--max-exit-age-seconds",
        "900",
    ]


def lease_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"


def history_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "operator_dashboard" / "runtime" / "broker_truth_lease_history.jsonl"


def broker_status_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"


def reconciliation_path(repo_root: Path) -> Path:
    return (
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )


def lifecycle_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"


def order_state_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"


def test_active_lease_writes_artifacts_and_exits_0(tmp_path: Path, capsys) -> None:
    seed_clean_artifacts(tmp_path)

    exit_code = cli.main([*base_args(tmp_path), "--json"])

    summary = json.loads(capsys.readouterr().out)
    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    history_rows = history_path(tmp_path).read_text(encoding="utf-8").splitlines()
    assert exit_code == 0
    assert summary["lease_state"] == "ACTIVE"
    assert lease["lease_state"] == "ACTIVE"


def test_historical_review_count_does_not_block_active_lease(tmp_path: Path, capsys) -> None:
    seed_clean_artifacts(tmp_path)
    reconciliation = json.loads(reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "current_scope_review_required_count": 0,
            "historical_review_required_count": 1,
            "raw_review_required_count": 1,
        }
    )
    write_json(reconciliation_path(tmp_path), reconciliation)

    exit_code = cli.main([*base_args(tmp_path), "--json"])

    summary = json.loads(capsys.readouterr().out)
    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 0
    assert summary["lease_state"] == "ACTIVE"
    assert lease["review_required_count"] == 0
    assert lease["current_scope_review_required_count"] == 0
    assert lease["historical_review_required_count"] == 1


def test_current_scope_review_count_blocks_active_lease(tmp_path: Path, capsys) -> None:
    seed_clean_artifacts(tmp_path)
    reconciliation = json.loads(reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "broker_reconciled": False,
            "review_required_count": 1,
            "current_scope_review_required_count": 1,
            "historical_review_required_count": 0,
        }
    )
    write_json(reconciliation_path(tmp_path), reconciliation)

    exit_code = cli.main([*base_args(tmp_path), "--json"])

    summary = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert summary["lease_state"] == "OPERATOR_REQUIRED"


def test_degraded_but_valid_lease_exits_0(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    broker_status = json.loads(broker_status_path(tmp_path).read_text(encoding="utf-8"))
    broker_status["latest_attempt_status"] = {
        "classification": "BROKER_TRUTH_REFRESH_FAILED",
        "generated_at": "2026-05-18T14:59:00+00:00",
        "last_failure": True,
        "last_success": False,
        "last_error": "TWS paper API error 502",
        "live_money_eligible": False,
    }
    write_json(broker_status_path(tmp_path), broker_status)

    exit_code = cli.main(base_args(tmp_path))

    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 0
    assert lease["lease_state"] == "ACTIVE_DEGRADED_REFRESH_FAILING"
    assert lease["submit_entry_allowed"] is True


def test_expired_entry_lease_exits_1(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)

    exit_code = cli.main(base_args(tmp_path, current_time="2026-05-18T15:04:00+00:00"))

    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 1
    assert lease["lease_state"] == "EXPIRED_BLOCK_NEW_ENTRIES"
    assert lease["submit_entry_allowed"] is False


def test_invalidated_contradiction_exits_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    broker_status = json.loads(broker_status_path(tmp_path).read_text(encoding="utf-8"))
    broker_status["last_successful_broker_truth"]["positions"] = [
        {"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0"}
    ]
    broker_status["latest_attempt_status"] = {}
    write_json(broker_status_path(tmp_path), broker_status)

    exit_code = cli.main(base_args(tmp_path))

    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 2
    assert lease["lease_state"] == "INVALIDATED_CONTRADICTION"
    assert _codes(lease["contradiction_details"]) >= {"unexpected_broker_position"}


def test_unknown_open_orders_invalidate_and_exit_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    reconciliation = json.loads(reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    reconciliation["unknown_broker_open_order_count"] = 1
    write_json(reconciliation_path(tmp_path), reconciliation)

    exit_code = cli.main(base_args(tmp_path))

    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 2
    assert lease["lease_state"] == "INVALIDATED_UNKNOWN_OPEN_ORDERS"


def test_manual_broker_action_invalidation_exits_2(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    lifecycle = json.loads(lifecycle_path(tmp_path).read_text(encoding="utf-8"))
    lifecycle["manual_close_detected"] = True
    lifecycle["stale_after_manual_close"] = True
    write_json(lifecycle_path(tmp_path), lifecycle)

    exit_code = cli.main(base_args(tmp_path))

    lease = json.loads(lease_path(tmp_path).read_text(encoding="utf-8"))
    assert exit_code == 2
    assert lease["lease_state"] == "INVALIDATED_MANUAL_BROKER_ACTION"


def test_human_summary_default(tmp_path: Path, capsys) -> None:
    seed_clean_artifacts(tmp_path)

    assert cli.main(base_args(tmp_path)) == 0

    output = capsys.readouterr().out
    assert "lease_state=ACTIVE" in output
    assert "live_money_eligible=false" in output


def test_no_history_flag_writes_only_latest(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)

    assert cli.main([*base_args(tmp_path), "--no-history"]) == 0

    assert lease_path(tmp_path).exists()
    assert not history_path(tmp_path).exists()


def test_explicit_output_paths(tmp_path: Path) -> None:
    seed_clean_artifacts(tmp_path)
    output_path = tmp_path / "custom" / "lease.json"
    history = tmp_path / "custom" / "lease.jsonl"

    assert cli.main([*base_args(tmp_path), "--output-path", str(output_path), "--history-path", str(history)]) == 0

    assert json.loads(output_path.read_text(encoding="utf-8"))["lease_state"] == "ACTIVE"
    assert history.read_text(encoding="utf-8").strip()


def test_source_has_no_broker_order_or_repair_execution_calls() -> None:
    source = inspect.getsource(cli)

    forbidden = [
        "place" + "Order",
        "cancel" + "Order",
        "global" + "Cancel",
        "req" + "Global" + "Cancel",
        "submit" + "_limit_order",
        "sub" + "process",
        "os" + ".kill",
        "launch" + "ctl",
        "E" + "Client",
    ]
    for token in forbidden:
        assert token not in source

    assert "write_broker_truth_lease" in source
    assert "classify_broker_truth_lease" in source


def _codes(rows: list[dict[str, object]]) -> set[str]:
    return {str(row.get("code")) for row in rows}
