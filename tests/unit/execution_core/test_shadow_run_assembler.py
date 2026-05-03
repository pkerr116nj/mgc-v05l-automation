from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_run_assembler import ShadowRunAssemblerVerdict, assemble_shadow_run
from mgc_v05l.execution_core.shadow_run_assembler_cli import main as shadow_run_assembler_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "shadow_run_mgc_202606_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "MANUAL_INTENT_JSON",
        "output_root": "outputs/track_b_execution_core/shadow_runs/shadow_run_mgc_202606_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def intent(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "instrument_family": "MGC",
        "local_execution_contract_key": "MGC-202606",
        "side": "BUY",
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "time_in_force": "DAY",
        "signal_timestamp": aware_now().isoformat(),
        "decision_timestamp": aware_now().isoformat(),
        "reason": "synthetic shadow run assembler test",
        "source": "unit_test",
        "submit_requested": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def lane(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "lane_status": "PAPER_REVIEW",
        "mode_allowed": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "allowed_instrument_family": "MGC",
        "allowed_sides": ["BUY", "SELL"],
        "allowed_order_types": ["LMT"],
        "allowed_time_in_force": ["DAY"],
        "max_quantity": 1,
    }
    payload.update(overrides)
    return payload


def registry(*lanes: dict[str, object]) -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_test_v1",
        "generated_at": aware_now().isoformat(),
        "lanes": list(lanes or (lane(),)),
    }


def readiness_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "Readiness summary inputs are clean.",
    }
    payload.update(overrides)
    return payload


def test_valid_manifest_registry_and_intent_assemble_shadow_run_for_review(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(),
        intent_payloads=[intent()],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-valid",
        now=aware_now(),
    )

    assert result.verdict == ShadowRunAssemblerVerdict.ASSEMBLED_FOR_REVIEW
    assert result.report["shadow_run_verdict"] == "SHADOW_RUN_ASSEMBLED_FOR_REVIEW"
    assert result.report["manifest_validation_verdict"] == "SHADOW_RUN_MANIFEST_VALID"
    assert result.report["total_intents"] == 1
    assert result.report["intents_validated"] == 1
    assert result.report["intents_authorized_by_lane_registry"] == 1
    assert result.report["order_plans_created"] == 1
    assert result.report["shadow_evaluations_created"] == 1
    assert result.report["blocked_count"] == 0
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert result.report["paper_proof_cli_remains_only_submit_path"] is True
    assert result.report["secondary_blockers"] == []
    assert result.report["output_paths"]["shadow_evaluation_reports"]


def test_invalid_manifest_blocks_shadow_run(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(submit_enabled=True),
        registry_payload=registry(),
        intent_payloads=[intent()],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-invalid-manifest",
        now=aware_now(),
    )

    assert result.verdict == ShadowRunAssemblerVerdict.BLOCKED_INVALID_MANIFEST
    assert result.report["manifest_validation_verdict"] == "SHADOW_RUN_MANIFEST_BLOCKED_SUBMIT_ENABLED"
    assert result.report["total_intents"] == 0
    assert result.report["blocked_count"] == 0
    assert result.report["submit_allowed"] is False


def test_empty_intent_list_blocks_clearly(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(),
        intent_payloads=[],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-empty",
        now=aware_now(),
    )

    assert result.verdict == ShadowRunAssemblerVerdict.BLOCKED_EMPTY_INTENTS
    assert result.report["shadow_run_verdict"] == "SHADOW_RUN_BLOCKED_EMPTY_INTENTS"
    assert result.report["total_intents"] == 0
    assert result.report["primary_blocker"] == "No strategy intent payloads were provided."
    assert result.report["secondary_blockers"] == []
    assert result.report["submit_attempted"] is False


def test_disabled_lane_creates_per_intent_blocker_and_run_blocker_summary(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(lane(lane_status="DISABLED")),
        intent_payloads=[intent()],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-disabled-lane",
        now=aware_now(),
    )
    item_report = json.loads(Path(result.report["output_paths"]["per_intent_reports"][0]).read_text(encoding="utf-8"))

    assert result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["blocked_count"] == 1
    assert result.report["intents_validated"] == 1
    assert result.report["intents_authorized_by_lane_registry"] == 0
    assert result.report["order_plans_created"] == 0
    assert result.report["shadow_evaluations_created"] == 0
    assert item_report["lane_validation_verdict"] == "LANE_BLOCKED_DISABLED"
    assert result.report["secondary_blockers"]
    assert item_report["secondary_blockers"] == []
    assert item_report["submit_allowed"] is False
    assert item_report["submit_attempted"] is False


def test_invalid_intent_creates_per_intent_blocker(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(),
        intent_payloads=[intent(order_type="MKT")],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-invalid-intent",
        now=aware_now(),
    )
    item_report = json.loads(Path(result.report["output_paths"]["per_intent_reports"][0]).read_text(encoding="utf-8"))

    assert result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["intents_validated"] == 0
    assert result.report["blocked_count"] == 1
    assert item_report["intent_validation_verdict"] == "INTENT_BLOCKED_UNSUPPORTED_ORDER_TYPE"
    assert item_report["order_plan_created"] is False
    assert item_report["shadow_evaluation_created"] is False
    assert item_report["submit_allowed"] is False


def test_multiple_intents_produce_correct_counts(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(),
        intent_payloads=[intent(), intent(side="SELL"), intent(quantity=2)],
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-multiple",
        now=aware_now(),
    )

    assert result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["total_intents"] == 3
    assert result.report["intents_validated"] == 3
    assert result.report["intents_authorized_by_lane_registry"] == 2
    assert result.report["order_plans_created"] == 2
    assert result.report["shadow_evaluations_created"] == 2
    assert result.report["blocked_count"] == 1
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_readiness_summary_blocker_is_surfaced(tmp_path: Path) -> None:
    result = assemble_shadow_run(
        manifest_payload=manifest(),
        registry_payload=registry(),
        intent_payloads=[intent()],
        readiness_summary_payload=readiness_summary(
            final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
            primary_blocker="unresolved broker order blocks same account/contract submit",
            required_next_action="Wait for terminal broker order state.",
        ),
        output_root=tmp_path / "shadow_runs",
        run_id="shadow-run-readiness-blocked",
        now=aware_now(),
    )
    item_report = json.loads(Path(result.report["output_paths"]["per_intent_reports"][0]).read_text(encoding="utf-8"))

    assert result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["blocked_count"] == 1
    assert item_report["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_READINESS_BLOCKED"
    assert item_report["primary_blocker"] == "unresolved broker order blocks same account/contract submit"
    assert item_report["submit_allowed"] is False
    assert item_report["live_money_readiness"] is False


def test_shadow_run_assembler_cli_reads_json_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    manifest_json = tmp_path / "manifest.json"
    registry_json = tmp_path / "registry.json"
    intent_json = tmp_path / "intent.json"
    manifest_json.write_text(json.dumps(manifest(), indent=2, sort_keys=True), encoding="utf-8")
    registry_json.write_text(json.dumps(registry(), indent=2, sort_keys=True), encoding="utf-8")
    intent_json.write_text(json.dumps(intent(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = shadow_run_assembler_cli_main(
        [
            "--manifest-json",
            str(manifest_json),
            "--registry-json",
            str(registry_json),
            "--intent-json",
            str(intent_json),
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["shadow_run_verdict"] == "SHADOW_RUN_ASSEMBLED_FOR_REVIEW"
    assert output["manifest_validation_verdict"] == "SHADOW_RUN_MANIFEST_VALID"
    assert output["total_intents"] == 1
    assert output["blocked_count"] == 0
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
