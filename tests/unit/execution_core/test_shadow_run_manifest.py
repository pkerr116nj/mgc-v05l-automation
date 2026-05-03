from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.shadow_run_manifest import ShadowRunManifestVerdict, validate_shadow_run_manifest
from mgc_v05l.execution_core.shadow_run_manifest_cli import main as manifest_cli_main


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
        "required_artifacts": [
            "intent_validation",
            "lane_validation",
            "order_plan",
            "shadow_evaluation",
            "readiness_summary",
        ],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def test_valid_paper_no_submit_manifest_is_valid_for_shadow_run(tmp_path: Path) -> None:
    result = validate_shadow_run_manifest(
        payload=manifest(),
        output_root=tmp_path / "manifest_reports",
        run_id="manifest-valid",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == ShadowRunManifestVerdict.VALID
    assert payload["manifest_validation_verdict"] == "SHADOW_RUN_MANIFEST_VALID"
    assert payload["manifest_allowed_for_shadow_run"] is True
    assert payload["submit_enabled"] is False
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["broker_connection_attempted"] is False
    assert payload["market_data_connection_attempted"] is False
    assert payload["strategy_execution_attempted"] is False
    assert payload["paper_proof_cli_wired"] is False
    assert payload["run_id"] == "shadow_run_mgc_202606_001"
    assert payload["run_mode"] == "PAPER"
    assert payload["expected_account_id"] == "DUM882026"
    assert payload["allowed_local_execution_contract_keys"] == ["MGC-202606"]
    assert payload["registry_version"] == "track_b_lane_registry_test_v1"
    assert payload["input_source_type"] == "MANUAL_INTENT_JSON"
    assert "lane_validation" in payload["required_artifacts"]
    assert payload["manifest_authorizes_lanes"] is False
    assert payload["lane_authorization_required"] is True
    assert payload["registry_governs_lane_authorization"] is True
    assert payload["manifest_governs_run_context_only"] is True


@pytest.mark.parametrize(
    ("overrides", "verdict"),
    [
        ({"run_mode": "LIVE"}, ShadowRunManifestVerdict.BLOCKED_LIVE_MODE),
        ({"submit_enabled": True}, ShadowRunManifestVerdict.BLOCKED_SUBMIT_ENABLED),
        ({"expected_account_id": ""}, ShadowRunManifestVerdict.BLOCKED_MISSING_ACCOUNT),
        ({"allowed_local_execution_contract_keys": []}, ShadowRunManifestVerdict.BLOCKED_NO_CONTRACTS),
        ({"output_root": ""}, ShadowRunManifestVerdict.BLOCKED_MISSING_OUTPUT_ROOT),
    ],
)
def test_manifest_blockers(tmp_path: Path, overrides: dict[str, object], verdict: ShadowRunManifestVerdict) -> None:
    result = validate_shadow_run_manifest(
        payload=manifest(**overrides),
        output_root=tmp_path / "manifest_reports",
        run_id=f"manifest-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["manifest_allowed_for_shadow_run"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]
    assert result.report["required_next_action"]


def test_manifest_schema_error_blocks_without_submit(tmp_path: Path) -> None:
    result = validate_shadow_run_manifest(
        payload={"run_mode": "PAPER"},
        output_root=tmp_path / "manifest_reports",
        run_id="manifest-schema-error",
        now=aware_now(),
    )

    assert result.verdict == ShadowRunManifestVerdict.BLOCKED_SCHEMA_ERROR
    assert result.report["manifest_allowed_for_shadow_run"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False


def test_manifest_cli_reads_json_and_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    manifest_json = tmp_path / "manifest.json"
    manifest_json.write_text(json.dumps(manifest(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = manifest_cli_main(
        [
            "--manifest-json",
            str(manifest_json),
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["manifest_validation_verdict"] == "SHADOW_RUN_MANIFEST_VALID"
    assert output["manifest_allowed_for_shadow_run"] is True
    assert output["submit_enabled"] is False
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
