from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_replay_runner import ShadowReplayRunnerVerdict, run_shadow_replay
from mgc_v05l.execution_core.shadow_replay_runner_cli import main as shadow_replay_runner_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 30, tzinfo=timezone.utc)


def signal(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_id": "shadow-replay-signal-001",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "signal_type": "breakout_retest",
        "signal_direction": "LONG",
        "decision_style": "BINARY",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "instrument_family": "MGC",
        "local_execution_contract_key": "MGC-202606",
        "signal_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "source": "unit_test",
        "reason": "synthetic shadow replay signal",
        "submit_requested": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def scoring(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_score": "0.91",
        "score_scale": "ZERO_TO_ONE",
        "probability_win": "0.62",
        "expected_value_r": "0.34",
        "confidence_level": "HIGH",
        "model_version": "static_research_v1",
        "scoring_authority": "INFORMATIONAL_ONLY",
    }
    payload.update(overrides)
    return payload


def signal_batch(*signals: dict[str, object], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "batch_id": "shadow_replay_batch_001",
        "shadow_run_id": "shadow_replay_run_001",
        "mode": "PAPER",
        "expected_account_id": "DUM882026",
        "default_policy_id": "shadow_replay_policy_001",
        "signal_items": [{"signal": row} for row in signals],
        "submit_enabled": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def policy(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "limit_price_source": "fixture",
        "time_in_force": "DAY",
        "source": "unit_test_shadow_replay",
        "min_signal_score": "0.70",
        "min_expected_value_r": "0.10",
        "allowed_confidence_levels": ["MEDIUM", "HIGH"],
    }
    payload.update(overrides)
    return payload


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "shadow_replay_manifest_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/shadow_replay_manifest_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def registry(*lanes: dict[str, object]) -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_test_v1",
        "generated_at": aware_now().isoformat(),
        "lanes": list(lanes or (lane(),)),
    }


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


def test_valid_replay_runner_completes_for_review(tmp_path: Path) -> None:
    result = run_shadow_replay(
        signal_batch_payload=signal_batch(
            signal(signal_id="binary"),
            signal(signal_id="static", decision_style="SCORED_STATIC", scoring=scoring()),
        ),
        proposal_policy_payload=policy(),
        manifest_payload=manifest(),
        registry_payload=registry(),
        output_root=tmp_path / "shadow_replay",
        run_id="replay-valid",
        now=aware_now(),
    )

    assert result.verdict == ShadowReplayRunnerVerdict.COMPLETED_FOR_REVIEW
    assert result.report["runner_verdict"] == "SHADOW_REPLAY_RUN_COMPLETED_FOR_REVIEW"
    assert result.report["total_signals"] == 2
    assert result.report["proposed_intents_created"] == 2
    assert result.report["shadow_run_verdict"] == "SHADOW_RUN_ASSEMBLED_FOR_REVIEW"
    assert result.report["attrition_report_verdict"] == "ATTRITION_REPORT_CREATED_WITH_MISSING_STAGES"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert Path(result.report["signal_batch_summary_path"]).exists()
    assert Path(result.report["shadow_run_summary_path"]).exists()
    assert Path(result.report["attrition_report_path"]).exists()
    assert len(result.report["proposal_report_paths"]) == 2
    assert len(result.report["proposed_intent_paths"]) == 2


def test_mixed_signal_batch_completes_with_blockers_and_attrition(tmp_path: Path) -> None:
    result = run_shadow_replay(
        signal_batch_payload=signal_batch(
            signal(signal_id="binary"),
            signal(signal_id="static-block", decision_style="SCORED_STATIC", scoring=scoring(signal_score="0.20", expected_value_r="0.01")),
        ),
        proposal_policy_payload=policy(),
        manifest_payload=manifest(),
        registry_payload=registry(),
        output_root=tmp_path / "shadow_replay",
        run_id="replay-mixed",
        now=aware_now(),
    )

    assert result.verdict == ShadowReplayRunnerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["proposed_intents_created"] == 1
    assert result.report["shadow_run_verdict"] == "SHADOW_RUN_ASSEMBLED_FOR_REVIEW"
    assert result.report["attrition_report_verdict"]
    assert result.report["primary_blocker"] == "One or more no-submit replay stages completed with blockers."
    assert result.report["secondary_blockers"]
    assert result.report["submit_allowed"] is False


def test_no_proposed_intents_blocks_clearly_with_attrition(tmp_path: Path) -> None:
    result = run_shadow_replay(
        signal_batch_payload=signal_batch(
            signal(signal_id="static-block", decision_style="SCORED_STATIC", scoring=scoring(signal_score="0.20", expected_value_r="0.01")),
        ),
        proposal_policy_payload=policy(),
        manifest_payload=manifest(),
        registry_payload=registry(),
        output_root=tmp_path / "shadow_replay",
        run_id="replay-no-intents",
        now=aware_now(),
    )

    assert result.verdict == ShadowReplayRunnerVerdict.BLOCKED_NO_PROPOSED_INTENTS
    assert result.report["proposed_intents_created"] == 0
    assert result.report["shadow_run_summary_path"] is None
    assert result.report["attrition_report_path"]
    assert result.report["primary_blocker"] == "Signal batch produced no proposed intent artifacts."
    assert result.report["missing_downstream_stages_explicit"] is True
    assert result.report["submit_attempted"] is False


def test_invalid_signal_batch_blocks_runner(tmp_path: Path) -> None:
    result = run_shadow_replay(
        signal_batch_payload=signal_batch(signal(), mode="LIVE"),
        proposal_policy_payload=policy(),
        manifest_payload=manifest(),
        registry_payload=registry(),
        output_root=tmp_path / "shadow_replay",
        run_id="replay-invalid-batch",
        now=aware_now(),
    )

    assert result.verdict == ShadowReplayRunnerVerdict.BLOCKED_SIGNAL_BATCH
    assert result.report["primary_blocker"] == "Signal batch accepts PAPER mode only."
    assert result.report["shadow_run_summary_path"] is None
    assert result.report["submit_allowed"] is False


def test_invalid_manifest_blocks_shadow_assembly_clearly(tmp_path: Path) -> None:
    result = run_shadow_replay(
        signal_batch_payload=signal_batch(signal()),
        proposal_policy_payload=policy(),
        manifest_payload=manifest(submit_enabled=True),
        registry_payload=registry(),
        output_root=tmp_path / "shadow_replay",
        run_id="replay-invalid-manifest",
        now=aware_now(),
    )

    assert result.verdict == ShadowReplayRunnerVerdict.BLOCKED_SHADOW_ASSEMBLY
    assert result.report["shadow_run_verdict"] == "SHADOW_RUN_BLOCKED_INVALID_MANIFEST"
    assert result.report["attrition_report_path"]
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_replay_runner_cli_reads_json_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    batch_json = tmp_path / "signal_batch.json"
    policy_json = tmp_path / "policy.json"
    manifest_json = tmp_path / "manifest.json"
    registry_json = tmp_path / "registry.json"
    batch_json.write_text(json.dumps(signal_batch(signal()), indent=2, sort_keys=True), encoding="utf-8")
    policy_json.write_text(json.dumps(policy(), indent=2, sort_keys=True), encoding="utf-8")
    manifest_json.write_text(json.dumps(manifest(), indent=2, sort_keys=True), encoding="utf-8")
    registry_json.write_text(json.dumps(registry(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = shadow_replay_runner_cli_main(
        [
            "--signal-batch-json",
            str(batch_json),
            "--proposal-policy-json",
            str(policy_json),
            "--manifest-json",
            str(manifest_json),
            "--registry-json",
            str(registry_json),
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["runner_verdict"] == "SHADOW_REPLAY_RUN_COMPLETED_FOR_REVIEW"
    assert output["proposed_intents_created"] == 1
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
