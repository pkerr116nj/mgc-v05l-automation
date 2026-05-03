from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_signal import ShadowSignalValidationConfig, ShadowSignalValidationVerdict, validate_shadow_signal
from mgc_v05l.execution_core.signal_batch import SignalBatchVerdict, process_signal_batch
from mgc_v05l.execution_core.shadow_run_assembler import ShadowRunAssemblerVerdict, assemble_shadow_run
from mgc_v05l.execution_core.shadow_run_manifest import ShadowRunManifestVerdict, validate_shadow_run_manifest
from mgc_v05l.execution_core.signal_intent_proposal import IntentProposalVerdict, SignalIntentProposalConfig, propose_intent_from_signal
from mgc_v05l.execution_core.strategy_lane_registry import LaneValidationVerdict, validate_strategy_lane


REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "track_b_shadow_run"


def load_json(name: str) -> dict[str, object]:
    return json.loads((EXAMPLE_ROOT / name).read_text(encoding="utf-8"))


def test_committed_track_b_shadow_run_fixture_set(tmp_path: Path) -> None:
    manifest = load_json("manifest.json")
    registry = load_json("lane_registry.json")
    valid_intent = load_json("intent_valid.json")
    blocked_qty_intent = load_json("intent_blocked_qty.json")
    now = datetime(2026, 5, 1, 20, 5, tzinfo=timezone.utc)

    manifest_result = validate_shadow_run_manifest(
        payload=manifest,
        output_root=tmp_path / "manifest_validation",
        run_id="example-manifest",
        now=now,
    )
    assert manifest_result.verdict == ShadowRunManifestVerdict.VALID
    assert manifest_result.report["submit_allowed"] is False
    assert manifest_result.report["submit_attempted"] is False
    assert manifest_result.report["live_money_readiness"] is False

    valid_lane_result = validate_strategy_lane(
        intent_payload=valid_intent,
        registry_payload=registry,
        output_root=tmp_path / "lane_validation",
        run_id="example-valid-lane",
        now=now,
    )
    assert valid_lane_result.verdict == LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW
    assert valid_lane_result.report["lane_authorized_for_review"] is True
    assert valid_lane_result.report["submit_allowed"] is False

    blocked_lane_result = validate_strategy_lane(
        intent_payload=blocked_qty_intent,
        registry_payload=registry,
        output_root=tmp_path / "lane_validation",
        run_id="example-blocked-lane",
        now=now,
    )
    assert blocked_lane_result.verdict == LaneValidationVerdict.BLOCKED_QUANTITY_EXCEEDS_MAX
    assert blocked_lane_result.report["lane_authorized_for_review"] is False
    assert blocked_lane_result.report["submit_allowed"] is False

    run_result = assemble_shadow_run(
        manifest_payload=manifest,
        registry_payload=registry,
        intent_payloads=[valid_intent, blocked_qty_intent],
        output_root=tmp_path / "shadow_runs",
        run_id="example-shadow-run",
        now=now,
    )

    assert run_result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert run_result.report["shadow_run_verdict"] == "SHADOW_RUN_COMPLETED_WITH_BLOCKERS"
    assert run_result.report["total_intents"] == 2
    assert run_result.report["intents_validated"] == 2
    assert run_result.report["intents_authorized_by_lane_registry"] == 1
    assert run_result.report["order_plans_created"] == 1
    assert run_result.report["shadow_evaluations_created"] == 1
    assert run_result.report["blocked_count"] == 1
    assert run_result.report["submit_allowed"] is False
    assert run_result.report["submit_attempted"] is False
    assert run_result.report["live_money_readiness"] is False

    item_reports = [json.loads(Path(path).read_text(encoding="utf-8")) for path in run_result.report["output_paths"]["per_intent_reports"]]
    assert item_reports[0]["submit_allowed"] is False
    assert item_reports[0]["submit_attempted"] is False
    assert item_reports[0]["live_money_readiness"] is False
    assert item_reports[0]["shadow_evaluation_created"] is True
    assert item_reports[1]["lane_validation_verdict"] == "LANE_BLOCKED_QUANTITY_EXCEEDS_MAX"
    assert item_reports[1]["submit_allowed"] is False
    assert item_reports[1]["submit_attempted"] is False
    assert item_reports[1]["live_money_readiness"] is False


def test_committed_signal_to_shadow_run_fixture_chain(tmp_path: Path) -> None:
    manifest = load_json("manifest.json")
    registry = load_json("lane_registry.json")
    blocked_qty_intent = load_json("intent_blocked_qty.json")
    binary_signal = load_json("signal_binary_valid.json")
    static_signal = load_json("signal_scored_static_valid.json")
    static_below_signal = load_json("signal_scored_static_below_threshold.json")
    binary_policy = load_json("proposal_policy_binary.json")
    static_policy = load_json("proposal_policy_scored_static.json")
    now = datetime(2026, 5, 1, 20, 10, tzinfo=timezone.utc)

    binary_signal_result = validate_shadow_signal(
        payload=binary_signal,
        config=ShadowSignalValidationConfig(expected_account_id="DUM882026", output_root=tmp_path / "shadow_signals"),
        run_id="example-binary-signal",
        now=now,
    )
    assert binary_signal_result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert binary_signal_result.report["submit_allowed"] is False
    assert binary_signal_result.report["submit_attempted"] is False
    assert binary_signal_result.report["live_money_readiness"] is False

    binary_proposal = propose_intent_from_signal(
        signal_payload=binary_signal,
        policy_payload=binary_policy,
        config=SignalIntentProposalConfig(expected_account_id="DUM882026", output_root=tmp_path / "signal_intent_proposals"),
        run_id="example-binary-proposal",
        now=now,
    )
    assert binary_proposal.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert binary_proposal.proposed_intent is not None
    assert binary_proposal.proposed_intent["submit_requested"] is False
    assert binary_proposal.report["submit_allowed"] is False
    assert binary_proposal.report["submit_attempted"] is False
    assert binary_proposal.report["live_money_readiness"] is False

    static_proposal = propose_intent_from_signal(
        signal_payload=static_signal,
        policy_payload=static_policy,
        config=SignalIntentProposalConfig(expected_account_id="DUM882026", output_root=tmp_path / "signal_intent_proposals"),
        run_id="example-static-proposal",
        now=now,
    )
    assert static_proposal.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert static_proposal.proposed_intent is not None
    assert static_proposal.report["scoring_passed_policy"] is True
    assert static_proposal.report["scoring_is_execution_authority"] is False
    assert static_proposal.report["submit_allowed"] is False

    below_threshold_proposal = propose_intent_from_signal(
        signal_payload=static_below_signal,
        policy_payload=static_policy,
        config=SignalIntentProposalConfig(expected_account_id="DUM882026", output_root=tmp_path / "signal_intent_proposals"),
        run_id="example-static-below-proposal",
        now=now,
    )
    assert below_threshold_proposal.verdict == IntentProposalVerdict.BLOCKED_STATIC_SCORE_BELOW_THRESHOLD
    assert below_threshold_proposal.proposed_intent is None
    assert below_threshold_proposal.report["submit_allowed"] is False
    assert below_threshold_proposal.report["submit_attempted"] is False
    assert below_threshold_proposal.report["live_money_readiness"] is False

    run_result = assemble_shadow_run(
        manifest_payload=manifest,
        registry_payload=registry,
        intent_payloads=[
            binary_proposal.proposed_intent,
            static_proposal.proposed_intent,
            blocked_qty_intent,
        ],
        output_root=tmp_path / "shadow_runs",
        run_id="example-signal-to-shadow-run",
        now=now,
    )

    assert run_result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
    assert run_result.report["total_intents"] == 3
    assert run_result.report["intents_validated"] == 3
    assert run_result.report["intents_authorized_by_lane_registry"] == 2
    assert run_result.report["order_plans_created"] == 2
    assert run_result.report["shadow_evaluations_created"] == 2
    assert run_result.report["blocked_count"] == 1
    assert run_result.report["submit_allowed"] is False
    assert run_result.report["submit_attempted"] is False
    assert run_result.report["live_money_readiness"] is False
    assert run_result.report["broker_connection_attempted"] is False
    assert run_result.report["market_data_connection_attempted"] is False

    item_reports = [json.loads(Path(path).read_text(encoding="utf-8")) for path in run_result.report["output_paths"]["per_intent_reports"]]
    assert item_reports[0]["shadow_evaluation_created"] is True
    assert item_reports[1]["shadow_evaluation_created"] is True
    assert item_reports[2]["lane_validation_verdict"] == "LANE_BLOCKED_QUANTITY_EXCEEDS_MAX"
    for item in item_reports:
        assert item["submit_allowed"] is False
        assert item["submit_attempted"] is False
        assert item["live_money_readiness"] is False


def test_committed_signal_batch_fixture(tmp_path: Path) -> None:
    batch_payload = load_json("signal_batch.json")
    static_policy = load_json("proposal_policy_scored_static.json")
    now = datetime(2026, 5, 1, 20, 15, tzinfo=timezone.utc)

    result = process_signal_batch(
        batch_payload=batch_payload,
        policy_payload=static_policy,
        expected_account_id="DUM882026",
        output_root=tmp_path / "signal_batches",
        run_id="example-signal-batch",
        now=now,
    )

    assert result.verdict == SignalBatchVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["batch_validation_verdict"] == "SIGNAL_BATCH_COMPLETED_WITH_BLOCKERS"
    assert result.report["total_signals"] == 3
    assert result.report["signals_validated"] == 3
    assert result.report["proposed_intents_created"] == 2
    assert result.report["blocked_proposals"] == 1
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert len(result.report["proposed_intent_output_paths"]) == 2
