from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, OperatorStatusVerdict, create_operator_status_summary
from mgc_v05l.execution_core.operator_status_cli import main as operator_status_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def listener_health(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_shadow_listener_health_v1",
        "generated_at": aware_now().isoformat(),
        "listener_id": "shadow_listener_test",
        "listener_cycle_id": "cycle-ok",
        "health_verdict": "SHADOW_LISTENER_HEALTH_OK",
        "last_cycle_verdict": "SHADOW_LISTENER_CYCLE_COMPLETED",
        "last_cycle_generated_at": aware_now().isoformat(),
        "inbox_dir": str(tmp_path / "inbox"),
        "processing_dir": str(tmp_path / "processing"),
        "processed_dir": str(tmp_path / "processed"),
        "failed_dir": str(tmp_path / "failed"),
        "files_discovered": 1,
        "files_processed": 1,
        "files_succeeded": 1,
        "files_failed": 0,
        "last_success_at": aware_now().isoformat(),
        "last_failure_at": None,
        "last_primary_blocker": None,
        "last_required_next_action": "Review generated no-submit artifacts.",
        "latest_cycle_summary_path": "cycle_summary.json",
        "latest_runner_summary_paths": ["runner_summary.json"],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "health_report_path": "health.json",
        "latest_health_report_path": "latest_health.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "listener_health.json", payload)


def recovery_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "classification": "RECOVERY_READY_CLEAN",
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "Broker state is clean.",
        "submit_allowed": True,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "recovery.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "recovery.json", payload)


def readiness_summary(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "Readiness inputs are clean.",
        "submit_allowed": True,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "readiness.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "readiness.json", payload)


def test_listener_health_ok_produces_ok_for_shadow_review(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-ok",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW
    assert result.report["status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert result.report["shadow_listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert result.report["primary_blocker"] is None
    assert "recovery" in result.report["reports_missing"]
    assert result.report["secondary_blockers"]
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_listener_degraded_produces_degraded_status(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(
                tmp_path,
                health_verdict="SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES",
                files_failed=1,
                last_primary_blocker="One file failed.",
                last_required_next_action="Review failed event report.",
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-degraded",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.DEGRADED_SHADOW_FAILURES
    assert result.report["primary_blocker"] == "One file failed."
    assert result.report["submit_allowed"] is False


def test_recovery_unresolved_broker_state_is_distinct_blocker(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            recovery_report_json=recovery_report(
                tmp_path,
                classification="RECOVERY_BLOCKED_UNRESOLVED_ORDER",
                final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
                primary_blocker="unresolved broker order blocks same account/contract submit",
                required_next_action="Wait for terminal broker order state.",
                submit_allowed=False,
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-broker-block",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_BROKER_STATE
    assert result.report["status_verdict"] == "OPERATOR_STATUS_BLOCKED_BROKER_STATE"
    assert result.report["recovery_verdict"] == "BLOCKED_UNRESOLVED_BROKER_ORDER"
    assert result.report["primary_blocker"] == "unresolved broker order blocks same account/contract submit"
    assert result.report["submit_allowed"] is False


def test_readiness_blocked_is_surfaced_distinctly(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            readiness_summary_json=readiness_summary(
                tmp_path,
                final_readiness_verdict="BLOCKED_UNKNOWN_PROOF_TIMING",
                primary_blocker="Proof timing is unknown.",
                required_next_action="Provide active-session timing evidence.",
                submit_allowed=False,
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-readiness-block",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_READINESS
    assert result.report["status_verdict"] == "OPERATOR_STATUS_BLOCKED_READINESS"
    assert result.report["readiness_verdict"] == "BLOCKED_UNKNOWN_PROOF_TIMING"
    assert result.report["primary_blocker"] == "Proof timing is unknown."
    assert result.report["submit_attempted"] is False


def test_no_inputs_reports_missing_not_ok(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(output_root=tmp_path / "operator_status"),
        status_id="status-missing",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.MISSING_REPORTS
    assert result.report["status_verdict"] == "OPERATOR_STATUS_MISSING_REPORTS"
    assert result.report["reports_missing"]
    assert result.report["primary_blocker"] == "No Track B observer reports were provided."
    assert result.report["submit_allowed"] is False
    assert result.report["paper_proof_cli_called"] is False


def test_operator_status_cli_reads_reports_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    exit_code = operator_status_cli_main(
        [
            "--listener-health-json",
            str(listener_health(tmp_path)),
            "--output-root",
            str(tmp_path / "operator_status_cli"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert output["shadow_listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert Path(output["report_json"]).exists()
