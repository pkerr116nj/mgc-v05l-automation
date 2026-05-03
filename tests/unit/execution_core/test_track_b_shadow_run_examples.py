from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_run_assembler import ShadowRunAssemblerVerdict, assemble_shadow_run
from mgc_v05l.execution_core.shadow_run_manifest import ShadowRunManifestVerdict, validate_shadow_run_manifest
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
