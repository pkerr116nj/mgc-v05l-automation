from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.app.ibkr_broker_truth_refresher import (
    BrokerTruthRefreshConfig,
    build_broker_truth_refresh_status,
    load_broker_truth_refresh_status,
    refresh_seconds_from_env,
    run_broker_truth_refresh_once,
)
from mgc_v05l.execution.ibkr_read_only_verifier import IbkrReadOnlyVerificationArtifacts


def _artifacts(*, classification: str = "IBKR_READ_ONLY_CONNECTED", positions_complete: bool = True, open_orders_complete: bool = True) -> IbkrReadOnlyVerificationArtifacts:
    return IbkrReadOnlyVerificationArtifacts(
        classification=classification,
        connection_report={
            "classification": classification,
            "connection_check": {"host": "127.0.0.1", "port": 7497, "client_id": 9077},
            "account_truth_check": {"ok": classification != "IBKR_READ_ONLY_BLOCKED"},
            "position_truth_check": {"ok": positions_complete},
            "open_order_truth_check": {"ok": open_orders_complete},
        },
        account_truth_snapshot={"selected_account_id": "DUM882026"},
        positions_snapshot={
            "generated_at": "2026-05-11T12:00:00+00:00",
            "selected_account_id": "DUM882026",
            "ok": positions_complete,
            "positions_complete": positions_complete,
            "position_count": 2,
            "positions": [{"symbol": "AAPL"}, {"symbol": "GC"}],
        },
        open_orders_snapshot={
            "generated_at": "2026-05-11T12:00:00+00:00",
            "selected_account_id": "DUM882026",
            "ok": open_orders_complete,
            "open_orders_complete": open_orders_complete,
            "open_order_count": 0,
            "open_orders": [],
        },
        contract_qualification_report={"ok": True},
        market_data_probe_report=None,
    )


def test_refresh_seconds_from_env_defaults_and_accepts_positive_values() -> None:
    assert refresh_seconds_from_env({}) == 60.0
    assert refresh_seconds_from_env({"TRACK_B_BROKER_TRUTH_REFRESH_SECONDS": "15"}) == 15.0
    assert refresh_seconds_from_env({"TRACK_B_BROKER_TRUTH_REFRESH_SECONDS": "nope"}) == 60.0


def test_broker_truth_refresh_once_uses_read_only_verifier_and_writes_status(tmp_path: Path) -> None:
    calls: list[object] = []
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)

    def verifier(*, config):
        calls.append(config)
        return _artifacts()

    def writer(*, output_dir: Path, artifacts: IbkrReadOnlyVerificationArtifacts) -> None:
        calls.append(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "ibkr_positions_snapshot.json").write_text(json.dumps(artifacts.positions_snapshot), encoding="utf-8")
        (output_dir / "ibkr_open_orders_snapshot.json").write_text(json.dumps(artifacts.open_orders_snapshot), encoding="utf-8")

    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
    )

    status = run_broker_truth_refresh_once(config=config, verifier=verifier, artifact_writer=writer, now_fn=lambda: fixed_time)

    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert status["account"] == "DUM882026"
    assert status["host"] == "127.0.0.1"
    assert status["port"] == 7497
    assert status["client_id"] == 9077
    assert status["positions_complete"] is True
    assert status["open_orders_complete"] is True
    assert status["position_count"] == 2
    assert status["open_order_count"] == 0
    assert status["submit_authority"] is False
    assert status["live_money_eligible"] is False
    assert status["paper_proof_invoked"] is False
    assert load_broker_truth_refresh_status(status_path=config.status_path)["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert (tmp_path / "var" / "ibkr_broker_truth_refresh_status.json").exists()
    assert calls


def test_broker_truth_refresh_failure_is_not_treated_as_clear_truth(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    status = build_broker_truth_refresh_status(
        config=BrokerTruthRefreshConfig(repo_root=tmp_path, output_dir=tmp_path),
        started_at=fixed_time,
        completed_at=fixed_time,
        artifacts=_artifacts(classification="IBKR_READ_ONLY_BLOCKED", positions_complete=False, open_orders_complete=False),
        error=None,
    )

    assert status["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert status["last_success"] is False
    assert status["positions_complete"] is False
    assert status["open_orders_complete"] is False
    assert status["submit_authority"] is False
    assert status["live_money_eligible"] is False


def test_broker_truth_refresher_source_has_no_broker_mutation_or_paper_proof() -> None:
    source = Path("src/mgc_v05l/app/ibkr_broker_truth_refresher.py").read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
    assert "reqAutoOpenOrders" not in source
    assert "run_paper_proof" not in source
    assert "live_money_eligible\": True" not in source
