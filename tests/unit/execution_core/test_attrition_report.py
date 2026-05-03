from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.attrition_report import AttritionReportVerdict, create_attrition_report
from mgc_v05l.execution_core.attrition_report_cli import main as attrition_report_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def signal_batch_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_batch_id": "signal_batch_001",
        "shadow_run_id": "shadow_run_001",
        "total_signals": 3,
        "signals_validated": 3,
        "proposal_attempts": 3,
        "proposed_intents_created": 2,
        "blocked_proposals": 1,
        "blockers_count_by_type": {
            "INTENT_PROPOSAL_BLOCKED_STATIC_SCORE_BELOW_THRESHOLD": 1,
        },
        "proposed_intent_output_paths": ["intent_1.json", "intent_2.json"],
        "proposal_report_output_paths": ["proposal_1.json", "proposal_2.json", "proposal_3.json"],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "signal_batch_summary.json",
    }
    payload.update(overrides)
    return payload


def shadow_run_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "shadow_run_id": "shadow_run_001",
        "total_intents": 2,
        "intents_authorized_by_lane_registry": 1,
        "order_plans_created": 1,
        "shadow_evaluations_created": 1,
        "primary_blockers_count_by_type": {
            "Intent quantity exceeds lane max_quantity.": 1,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "output_paths": {
            "summary_report_json": "shadow_run_summary.json",
        },
    }
    payload.update(overrides)
    return payload


def readiness_summary(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "final_readiness_verdict": "BLOCKED_UNRESOLVED_BROKER_ORDER",
        "primary_blocker": "unresolved broker order blocks same account/contract submit",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "readiness_summary.json",
    }
    payload.update(overrides)
    return payload


def test_attrition_report_from_signal_batch_summary_only_marks_missing_stages(tmp_path: Path) -> None:
    result = create_attrition_report(
        signal_batch_summary=signal_batch_summary(),
        output_root=tmp_path / "attrition",
        run_id="attrition-signal-only",
        now=aware_now(),
    )

    assert result.verdict == AttritionReportVerdict.CREATED_WITH_MISSING_STAGES
    assert result.report["total_signals"] == 3
    assert result.report["signals_validated"] == 3
    assert result.report["proposal_attempts"] == 3
    assert result.report["proposed_intents_created"] == 2
    assert result.report["proposals_blocked"] == 1
    assert result.report["intents_submitted_to_registry"] == "NOT_PROVIDED"
    assert result.report["lane_registry_blocked"] == "NOT_PROVIDED"
    assert result.report["missing_stages"] == ["shadow_run", "readiness"]
    assert result.report["primary_blocker"] == "One or more attrition input stages were not provided."
    assert result.report["secondary_blockers"] == ["missing_stage:shadow_run", "missing_stage:readiness"]
    assert result.report["required_next_action"]
    assert result.report["primary_attrition_stage"] == "proposal"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_attrition_report_combines_signal_batch_and_shadow_run_summary(tmp_path: Path) -> None:
    result = create_attrition_report(
        signal_batch_summary=signal_batch_summary(),
        shadow_run_summary=shadow_run_summary(),
        output_root=tmp_path / "attrition",
        run_id="attrition-combined",
        now=aware_now(),
    )

    assert result.verdict == AttritionReportVerdict.CREATED_WITH_MISSING_STAGES
    assert result.report["intents_submitted_to_registry"] == 2
    assert result.report["intents_authorized_by_registry"] == 1
    assert result.report["lane_registry_blocked"] == 1
    assert result.report["order_plans_created"] == 1
    assert result.report["order_plan_blocked"] == 0
    assert result.report["shadow_evaluations_created"] == 1
    assert result.report["blockers_count_by_stage"]["proposal"] == 1
    assert result.report["blockers_count_by_stage"]["lane_registry"] == 1
    assert result.report["primary_attrition_stage"] == "proposal"
    assert result.report["blockers_count_by_type"]["INTENT_PROPOSAL_BLOCKED_STATIC_SCORE_BELOW_THRESHOLD"] == 1
    assert result.report["blockers_count_by_type"]["Intent quantity exceeds lane max_quantity."] == 1
    assert result.report["missing_stages"] == ["readiness"]


def test_attrition_report_includes_readiness_blocker(tmp_path: Path) -> None:
    result = create_attrition_report(
        signal_batch_summary=signal_batch_summary(blocked_proposals=0, blockers_count_by_type={}),
        shadow_run_summary=shadow_run_summary(total_intents=2, intents_authorized_by_lane_registry=2, order_plans_created=2, shadow_evaluations_created=2),
        readiness_summary=readiness_summary(),
        output_root=tmp_path / "attrition",
        run_id="attrition-readiness",
        now=aware_now(),
    )

    assert result.verdict == AttritionReportVerdict.CREATED_FOR_REVIEW
    assert result.report["missing_stages"] == []
    assert result.report["readiness_allowed"] is False
    assert result.report["readiness_blocked"] is True
    assert result.report["blockers_count_by_stage"]["readiness"] == 1
    assert result.report["primary_attrition_stage"] == "readiness"
    assert result.report["blockers_count_by_type"]["BLOCKED_UNRESOLVED_BROKER_ORDER"] == 1
    assert result.report["primary_blocker"] is None
    assert result.report["secondary_blockers"] == []
    assert result.report["required_next_action"]


def test_attrition_report_blocks_no_inputs(tmp_path: Path) -> None:
    result = create_attrition_report(
        output_root=tmp_path / "attrition",
        run_id="attrition-empty",
        now=aware_now(),
    )

    assert result.verdict == AttritionReportVerdict.BLOCKED_NO_INPUTS
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]
    assert result.report["secondary_blockers"] == []


def test_attrition_report_cli_reads_summaries_and_writes_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    signal_path = tmp_path / "signal_batch_summary.json"
    shadow_path = tmp_path / "shadow_run_summary.json"
    signal_path.write_text(json.dumps(signal_batch_summary(), indent=2, sort_keys=True), encoding="utf-8")
    shadow_path.write_text(json.dumps(shadow_run_summary(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = attrition_report_cli_main(
        [
            "--signal-batch-summary-json",
            str(signal_path),
            "--shadow-run-summary-json",
            str(shadow_path),
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["attrition_report_verdict"] == "ATTRITION_REPORT_CREATED_WITH_MISSING_STAGES"
    assert output["primary_attrition_stage"] == "proposal"
    assert output["missing_stages"] == ["readiness"]
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
