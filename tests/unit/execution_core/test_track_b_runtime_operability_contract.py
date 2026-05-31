from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_operability_contract import (
    BLOCKED_SAFETY,
    BLOCKED_STALE_TRUTH,
    READY_SUBMIT_CAPABLE,
    RuntimeOperabilityConfig,
    build_runtime_authority_map,
    build_runtime_operability_contract,
    classify_runtime_operability,
    READY_DIAGNOSTIC_ONLY,
)


NOW = datetime(2026, 5, 28, 0, 30, tzinfo=UTC)


def test_ready_submit_capable_uses_canonical_authority_and_quarantines_deprecated_surface(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path)
    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == READY_SUBMIT_CAPABLE
    assert payload["ready_submit_capable"] is True
    assert payload["restart_allowed_if_runtime_down"] is True
    assert payload["live_money_eligible"] is False
    assert payload["paper_proof_invoked"] is False
    quarantine = payload["deprecated_surface_quarantine"]
    assert quarantine["cannot_imply_submit_authority"] is True
    assert quarantine["cannot_override_active_paper_config"] is True
    assert payload["authority_map"]["quarantined_stale_launch_metadata"]["classification"] == "stale_deprecated"


def test_stale_generated_projection_warns_but_does_not_block_runtime(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path)
    stale_time = NOW - timedelta(hours=2)
    projection = config.resolve(config.operator_dashboard_readiness_path)
    projection.parent.mkdir(parents=True, exist_ok=True)
    projection.write_text('{"classification":"NOT_READY_CONFIG"}\n', encoding="utf-8")
    os.utime(projection, (stale_time.timestamp(), stale_time.timestamp()))

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == READY_SUBMIT_CAPABLE
    warning_codes = {row["code"] for row in payload["warnings"]}
    assert "diagnostic_artifact_stale" in warning_codes


def test_stale_authority_artifact_blocks_as_stale_truth(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path)
    stale_time = NOW - timedelta(hours=2)
    canonical = config.resolve(config.canonical_readiness_path)
    os.utime(canonical, (stale_time.timestamp(), stale_time.timestamp()))

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == BLOCKED_STALE_TRUTH
    assert payload["restart_allowed_if_runtime_down"] is False
    assert any(row["code"] == "authority_artifact_stale" for row in payload["blockers"])


def test_paper_safety_flags_remain_hard_blocks(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path, live_money_eligible=True)

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == BLOCKED_SAFETY
    assert payload["restart_allowed_if_runtime_down"] is False
    assert any(row["code"] == "live_money_eligible_true" for row in payload["blockers"])


def test_runtime_down_with_clean_authority_allows_recovery_restart(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path, runtime_running=False)

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == READY_SUBMIT_CAPABLE
    assert payload["restart_allowed_if_runtime_down"] is True
    assert payload["runtime_summary"]["running"] is False


def test_scheduled_market_halt_is_diagnostic_not_infrastructure_block(tmp_path: Path) -> None:
    config = _write_ready_authority(
        tmp_path,
        canonical_readiness="WAITING_FOR_MARKET_REOPEN",
        canonical_extra={
            "ready_submit_capable": False,
            "market_schedule_state": "SCHEDULED_MARKET_HALT",
            "stale_market_data_expected": True,
            "next_expected_reopen_time": "2026-05-31T22:00:00+00:00",
            "market_data_grace_until": "2026-05-31T22:10:00+00:00",
            "readiness_block_is_scheduled_halt": True,
            "readiness_blockers": [],
        },
    )

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == READY_DIAGNOSTIC_ONLY
    assert payload["ready_submit_capable"] is False
    assert payload["restart_allowed_if_runtime_down"] is False
    assert payload["blockers"] == []
    assert payload["market_schedule_state"] == "SCHEDULED_MARKET_HALT"
    assert payload["readiness_block_is_scheduled_halt"] is True
    assert "canonical_readiness_waiting_for_market_reopen" in {row["code"] for row in payload["warnings"]}


def test_fresh_healthy_runtime_truth_with_producer_pid_reports_runtime_up(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path)
    _write(
        config.resolve(config.paper_runtime_truth_path),
        {
            "producer_pid": 1234,
            "lane_count": 7,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "writer_authority": "SINGLE_WRITER",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = build_runtime_operability_contract(config=config, now=NOW)

    assert payload["canonical_state"] == READY_SUBMIT_CAPABLE
    assert payload["runtime_summary"]["running"] is True


def test_classification_does_not_read_quarantined_five_lane_surface_as_authority(tmp_path: Path) -> None:
    config = _write_ready_authority(tmp_path)
    stale_dir = config.resolve(config.quarantined_stale_runtime_dir)
    stale_dir.mkdir(parents=True, exist_ok=True)
    (stale_dir / "paper_runtime_truth.json").write_text(
        '{"running":true,"lane_count":5,"live_money_eligible":true}\n',
        encoding="utf-8",
    )

    surfaces = build_runtime_authority_map(config=config, now=NOW)
    payloads = {
        key: _read(config.resolve(Path(row["path"])))
        for key, row in surfaces.items()
        if row["classification"] in {"active_authority", "active_diagnostic_only", "generated_projection_only"}
    }
    result = classify_runtime_operability(surfaces=surfaces, payloads=payloads, now=NOW)

    assert result["state"] == READY_SUBMIT_CAPABLE
    assert result["config_summary"]["lane_count"] == 7


def _write_ready_authority(
    tmp_path: Path,
    *,
    live_money_eligible: bool = False,
    runtime_running: bool = True,
    canonical_readiness: str = "READY_SUBMIT_CAPABLE",
    canonical_extra: dict | None = None,
) -> RuntimeOperabilityConfig:
    config = RuntimeOperabilityConfig(repo_root=tmp_path)
    _write(
        config.resolve(config.canonical_readiness_path),
        {
            "generated_at": NOW.isoformat(),
            "canonical_readiness": canonical_readiness,
            "live_money_eligible": live_money_eligible,
            "paper_proof_invoked": False,
            "broker_mutation_allowed": False,
            "readiness_blockers": [],
            "runtime": {"eligible_lane_count": 5},
            **(canonical_extra or {}),
        },
    )
    _write(
        config.resolve(config.paper_runtime_truth_path),
        {
            "running": runtime_running,
            "producer_pid": 1234 if runtime_running else None,
            "lane_count": 7,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "writer_authority": "SINGLE_WRITER",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write(
        config.resolve(config.paper_config_in_force_path),
        {
            "lane_count": 7,
            "lanes": [{"lane_id": f"lane_{idx}"} for idx in range(7)],
            "paper_only": True,
            "live_money_eligible": False,
        },
    )
    _write(
        config.resolve(config.broker_reconciliation_path),
        {"classification": "TRACK_B_PAPER_BROKER_RECONCILED", "live_money_eligible": False},
    )
    _write(
        config.resolve(config.runtime_supervisor_authority_path),
        {"classification": "SUPERVISOR_RUNTIME_ALREADY_HEALTHY", "safe_to_start_runtime": True},
    )
    _write(config.resolve(config.hourly_recovery_audit_path), {"classification": "RUNTIME_HEALTHY_NO_ACTION"})
    _write(config.resolve(config.canonical_readiness_summary_path), {"classification": "READY_SUBMIT_CAPABLE"})
    return config


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(__import__("json").dumps(payload) + "\n", encoding="utf-8")
    os.utime(path, (NOW.timestamp(), NOW.timestamp()))


def _read(path: Path) -> dict:
    if not path.exists() or path.is_dir():
        return {}
    return __import__("json").loads(path.read_text(encoding="utf-8"))
