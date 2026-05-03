from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.readiness_summary import ReadinessSummaryConfig, run_readiness_summary
from mgc_v05l.execution_core.readiness_summary_cli import main as summary_cli_main


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def recovery_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, Any] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "submit_allowed": True,
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "position_quantity": "0",
        "primary_blocker": None,
        "required_next_action": "Broker state is clean.",
    }
    payload.update(overrides)
    return write_json(tmp_path / "recovery_report.json", payload)


def preflight_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, Any] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "submit_allowed": True,
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "position_qty": 0,
        "market_data_mode": "DELAYED",
        "production_live_money_readiness": False,
        "primary_blocker": None,
        "required_next_action": "Preflight clean.",
    }
    payload.update(overrides)
    return write_json(tmp_path / "preflight_report.json", payload)


def quote_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, Any] = {
        "classification": "CURRENT_QUOTE_AVAILABLE",
        "quote_status": "CURRENT_QUOTE_AVAILABLE",
        "quote_usable_for_paper_pricing": True,
        "quote_usable_for_live_money_readiness": False,
    }
    payload.update(overrides)
    return write_json(tmp_path / "quote_report.json", payload)


def run_summary(tmp_path: Path, **overrides: object):
    recovery_path = overrides.pop("recovery_report_json", None)
    preflight_path = overrides.pop("preflight_report_json", None)
    quote_path = overrides.pop("quote_report_json", None)
    config = ReadinessSummaryConfig(
        recovery_report_json=recovery_path if recovery_path is not None else recovery_report(tmp_path),
        preflight_report_json=preflight_path if preflight_path is not None else preflight_report(tmp_path),
        quote_report_json=quote_path if quote_path is not None else quote_report(tmp_path),
        proof_timing_status=str(overrides.pop("proof_timing_status", "ACTIVE_SESSION")),
        output_root=tmp_path / "summary",
    )
    return run_readiness_summary(config=config, run_id="summary")


def test_clean_inputs_produce_ready_for_paper_proof_without_submit(tmp_path: Path) -> None:
    result = run_summary(tmp_path)

    assert result.report["final_readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert result.report["submit_allowed"] is True
    assert result.report["submit_attempted"] is False
    assert result.report["submit_enabled"] is False
    assert result.report["place_order_called"] is False
    assert result.report["cancel_called"] is False
    assert result.report["paper_proof_cli_explicit_flags_still_required"] is True
    assert result.report["production_live_money_readiness"] is False


def test_unresolved_broker_order_blocks_summary_readiness(tmp_path: Path) -> None:
    result = run_summary(
        tmp_path,
        recovery_report_json=recovery_report(
            tmp_path,
            final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
            submit_allowed=False,
            primary_blocker="unresolved broker order blocks same account/contract submit",
            required_next_action="Wait for terminal broker order state.",
            broker_order_id="1",
            perm_id="736787312",
            broker_status="PENDING_CANCEL",
        ),
    )

    assert result.report["final_readiness_verdict"] == "BLOCKED_UNRESOLVED_BROKER_ORDER"
    assert result.report["submit_allowed"] is False
    assert result.report["broker_order_id"] == "1"
    assert result.report["perm_id"] == "736787312"
    assert result.report["broker_status"] == "PENDING_CANCEL"


def test_non_flat_position_blocks_summary_readiness(tmp_path: Path) -> None:
    result = run_summary(
        tmp_path,
        preflight_report_json=preflight_report(
            tmp_path,
            final_readiness_verdict="BLOCKED_NON_FLAT_POSITION",
            submit_allowed=False,
            primary_blocker="Proof contract position is not flat.",
            required_next_action="Flatten or reconcile the account.",
            position_qty=1,
        ),
    )

    assert result.report["final_readiness_verdict"] == "BLOCKED_NON_FLAT_POSITION"
    assert result.report["submit_allowed"] is False
    assert result.report["position_qty"] == 1


def test_unknown_or_outside_timing_blocks_summary_readiness(tmp_path: Path) -> None:
    outside = run_summary(tmp_path, proof_timing_status="OUTSIDE_ACTIVE_SESSION")
    unknown = run_summary(tmp_path, proof_timing_status="UNKNOWN")

    assert outside.report["final_readiness_verdict"] == "BLOCKED_OUTSIDE_ACTIVE_SESSION"
    assert outside.report["submit_allowed"] is False
    assert unknown.report["final_readiness_verdict"] == "BLOCKED_UNKNOWN_PROOF_TIMING"
    assert unknown.report["submit_allowed"] is False


def test_quote_unavailable_is_distinct_from_broker_state_block(tmp_path: Path) -> None:
    result = run_summary(
        tmp_path,
        quote_report_json=quote_report(
            tmp_path,
            classification="CURRENT_QUOTE_UNAVAILABLE",
            quote_status="CURRENT_QUOTE_UNAVAILABLE",
            quote_usable_for_paper_pricing=False,
        ),
    )

    assert result.report["final_readiness_verdict"] == "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE"
    assert "Quote is not currently available" in result.report["primary_blocker"]
    assert result.report["submit_allowed"] is False
    assert result.report["production_live_money_readiness"] is False


def test_cli_writes_no_submit_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    exit_code = summary_cli_main(
        [
            "--recovery-report-json",
            str(recovery_report(tmp_path)),
            "--preflight-report-json",
            str(preflight_report(tmp_path)),
            "--quote-report-json",
            str(quote_report(tmp_path)),
            "--proof-timing-status",
            "ACTIVE_SESSION",
            "--output-root",
            str(tmp_path / "summary_cli"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["final_readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert output["submit_allowed"] is True
    assert output["submit_attempted"] is False
