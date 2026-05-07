from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_submit_invocation_diagnostic import (
    run_track_b_managed_submit_invocation_diagnostic,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 13, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def test_managed_submit_diagnostic_reports_submit_flag_not_requested(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "report.json",
        {
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "submit_enabled": False,
            "entry_intent": {"strategy_id": "FIRST_BEAR_SNAP_TURN_V1"},
            "entry_submit_attempt": {
                "submit_attempted": False,
                "primary_blocker": "Managed PAPER submit is not enabled for this lifecycle invocation.",
            },
            "primary_blocker": "Managed PAPER submit is not enabled for this lifecycle invocation.",
        },
    )
    runner_path = write_json(
        tmp_path / "runner.json",
        {
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "paper_submit_requested": False,
            "paper_submit_flags_present": False,
            "managed_lifecycle_invoked": True,
            "managed_lifecycle_report_path": str(lifecycle_path),
        },
    )

    report = run_track_b_managed_submit_invocation_diagnostic(
        runner_report_json=runner_path,
        diagnostic_json=tmp_path / "diagnostics" / "latest.json",
        diagnostic_md=tmp_path / "diagnostics" / "latest.md",
        now=aware_now(),
    )

    assert report["classification"] == "PAPER_SUBMIT_NOT_REQUESTED"
    assert report["managed_paper_submit_enabled"] is False
    assert report["ibkr_adapter_invoked"] is False
    assert report["place_order_called"] is False
    assert report["managed_submit_blocked_reason"] == "SUBMIT_DISABLED"
    assert report["broker_mutation_attempted_by_diagnostic"] is False


def test_managed_submit_diagnostic_preserves_callback_missing_as_inconclusive_until_new_artifacts(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "report.json",
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "entry_intent": {"strategy_id": "FIRST_BULL_SNAP_TURN_V1"},
            "entry_submit_attempt": None,
            "primary_blocker": "Managed PAPER lifecycle stage error: missing openOrder/orderStatus callback",
            "submit_attempted": False,
            "broker_state_mutated": False,
        },
    )
    runner_path = write_json(
        tmp_path / "runner.json",
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "paper_submit_requested": True,
            "paper_submit_flags_present": True,
            "managed_lifecycle_invoked": True,
            "managed_lifecycle_report_path": str(lifecycle_path),
            "primary_blocker": "Managed PAPER lifecycle stage error: missing openOrder/orderStatus callback",
        },
    )

    report = run_track_b_managed_submit_invocation_diagnostic(
        runner_report_json=runner_path,
        diagnostic_json=tmp_path / "diagnostics" / "latest.json",
        diagnostic_md=tmp_path / "diagnostics" / "latest.md",
        now=aware_now(),
    )

    assert report["classification"] == "DIAGNOSTIC_INCONCLUSIVE"
    assert report["paper_submit_requested"] is True
    assert report["entry_submit_attempt_recorded"] is False
    assert report["operator_change_required_for_real_managed_paper_submits"].startswith("Preserve submit-stage diagnostics")


def test_managed_submit_diagnostic_shows_adapter_attempt_when_submit_diagnostics_exist(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "report.json",
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "submit_enabled": True,
            "entry_intent": {"strategy_id": "FIRST_BULL_SNAP_TURN_V1"},
            "entry_submit_attempt": {
                "submit_attempted": True,
                "broker_state_mutated": True,
                "broker_order_id": "1001",
                "primary_blocker": "Managed PAPER adapter submit stage failed: missing openOrder/orderStatus callback",
                "submit_diagnostics": {
                    "place_order_called": True,
                    "order_transmit_flag": True,
                    "broker_order_id_allocated": "1001",
                    "openOrder_seen": False,
                    "orderStatus_seen": False,
                },
            },
            "primary_blocker": "Managed PAPER adapter submit stage failed: missing openOrder/orderStatus callback",
            "submit_attempted": True,
            "broker_state_mutated": True,
        },
    )
    runner_path = write_json(
        tmp_path / "runner.json",
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "paper_submit_requested": True,
            "paper_submit_flags_present": True,
            "managed_lifecycle_invoked": True,
            "managed_lifecycle_report_path": str(lifecycle_path),
        },
    )

    report = run_track_b_managed_submit_invocation_diagnostic(
        runner_report_json=runner_path,
        diagnostic_json=tmp_path / "diagnostics" / "latest.json",
        diagnostic_md=tmp_path / "diagnostics" / "latest.md",
        now=aware_now(),
    )

    assert report["classification"] == "DIAGNOSTIC_INCONCLUSIVE"
    assert report["ibkr_adapter_invoked"] is True
    assert report["place_order_called"] is True
    assert report["transmit_true"] is True
    assert report["order_id_assigned"] is True
    assert report["order_status_callback_received"] is False
