from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig
from mgc_v05l.execution_core.recovery_status import RecoveryStatusClassification, RecoveryStatusConfig, run_recovery_status


def config(tmp_path: Path, **overrides: object) -> RecoveryStatusConfig:
    kwargs = {
        "output_root": tmp_path / "recovery_status",
        "account_id": "DUM882026",
        "client_id": 17077,
        "broker_order_id": "1",
        "perm_id": "736787312",
    }
    kwargs.update(overrides)
    return RecoveryStatusConfig(**kwargs)


def preflight_result(tmp_path: Path, **overrides: object) -> PreflightResult:
    report: dict[str, Any] = {
        "classification": "READY_READ_ONLY",
        "config": {"account_id": "DUM882026"},
        "contract_key": "MGC-202606",
        "position": {"signed_quantity": 0},
        "open_orders": [],
        "unresolved_broker_order_detected": False,
        "paper_route_readiness": True,
    }
    report.update(overrides)
    classification = PreflightClassification(str(report["classification"]))
    return PreflightResult(
        run_id="preflight-recovery",
        classification=classification,
        report_json=tmp_path / "preflight_report.json",
        report_md=tmp_path / "preflight_report.md",
        report=report,
    )


def runner(result: PreflightResult):
    def run(config: ReadOnlyPreflightConfig, run_id: str) -> PreflightResult:  # noqa: ARG001
        return result

    return run


def read_report(result) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return json.loads(result.report_json.read_text(encoding="utf-8"))


def test_pending_cancel_blocks_same_account_contract_submit(tmp_path: Path) -> None:
    result = run_recovery_status(
        config=config(tmp_path),
        preflight_runner=runner(
            preflight_result(
                tmp_path,
                classification="BLOCKED",
                position={"signed_quantity": 0},
                open_orders=[
                    {
                        "broker_order_id": "1",
                        "perm_id": "736787312",
                        "status": "PendingCancel",
                        "remaining_quantity": "1",
                        "filled_quantity": "0",
                    }
                ],
                unresolved_broker_order_detected=True,
                unresolved_broker_order_status="PENDING_CANCEL",
                unresolved_broker_order_id="1",
                unresolved_broker_perm_id="736787312",
                unresolved_remaining_quantity="1",
            )
        ),
        run_id="recovery-pending-cancel",
    )
    payload = read_report(result)

    assert result.classification == RecoveryStatusClassification.BLOCKED_UNRESOLVED_ORDER
    assert payload["lifecycle_classification"] == "PENDING_CANCEL"
    assert payload["blocks_same_account_contract_submit"] is True
    assert payload["matching_broker_order_id"] == "1"
    assert payload["matching_perm_id"] == "736787312"


def test_flat_no_open_orders_is_ready_clean(tmp_path: Path) -> None:
    result = run_recovery_status(
        config=config(tmp_path),
        preflight_runner=runner(preflight_result(tmp_path)),
        run_id="recovery-clean",
    )
    payload = read_report(result)

    assert result.classification == RecoveryStatusClassification.READY_CLEAN
    assert payload["position_quantity"] == "0"
    assert payload["proof_contract_open_orders"] == []
    assert payload["blocks_same_account_contract_submit"] is False


def test_cancelled_terminal_order_with_flat_no_open_orders_is_ready_clean(tmp_path: Path) -> None:
    result = run_recovery_status(
        config=config(tmp_path),
        preflight_runner=runner(
            preflight_result(
                tmp_path,
                position={"signed_quantity": 0},
                open_orders=[],
                unresolved_broker_order_detected=False,
            )
        ),
        run_id="recovery-cancelled-clean",
    )

    assert result.classification == RecoveryStatusClassification.READY_CLEAN


def test_filled_or_partial_unexpected_state_is_ambiguous(tmp_path: Path) -> None:
    result = run_recovery_status(
        config=config(tmp_path),
        preflight_runner=runner(
            preflight_result(
                tmp_path,
                classification="BLOCKED",
                position={"signed_quantity": 0},
                open_orders=[
                    {
                        "broker_order_id": "1",
                        "perm_id": "736787312",
                        "status": "Submitted",
                        "remaining_quantity": "0.5",
                        "filled_quantity": "0.5",
                    }
                ],
            )
        ),
        run_id="recovery-partial-fill",
    )

    assert result.classification == RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert "partial fill" in str(result.report["failure_or_ambiguity"])


def test_conflicting_position_and_open_order_is_ambiguous(tmp_path: Path) -> None:
    result = run_recovery_status(
        config=config(tmp_path),
        preflight_runner=runner(
            preflight_result(
                tmp_path,
                classification="BLOCKED",
                position={"signed_quantity": 1},
                open_orders=[
                    {
                        "broker_order_id": "1",
                        "perm_id": "736787312",
                        "status": "PendingCancel",
                        "remaining_quantity": "1",
                        "filled_quantity": "0",
                    }
                ],
            )
        ),
        run_id="recovery-conflict",
    )

    assert result.classification == RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert "conflicting broker position" in str(result.report["failure_or_ambiguity"])
