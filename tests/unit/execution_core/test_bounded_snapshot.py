from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.monitoring.logger import StructuredLogger


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_bounded_snapshot_overwrites_instead_of_appending(tmp_path: Path) -> None:
    path = tmp_path / "operator_status.json"
    config = BoundedSnapshotConfig(max_bytes=2000, target_bytes=1000)

    write_bounded_snapshot_json(path, {"generated_at": "first", "value": "A"}, config=config)
    first_size = path.stat().st_size
    write_bounded_snapshot_json(path, {"generated_at": "second", "value": "B"}, config=config)

    payload = _read_json(path)
    assert payload["generated_at"] == "second"
    assert payload["value"] == "B"
    assert path.stat().st_size <= first_size + 20
    assert path.read_text(encoding="utf-8").count("generated_at") == 1


def test_bounded_snapshot_degrades_oversized_payload_and_writes_sidecar(tmp_path: Path) -> None:
    path = tmp_path / "operator_status.json"
    diagnostic = tmp_path / "operator_status_bounded_snapshot_diagnostic.json"
    payload = {
        "schema_version": "operator_status_v1",
        "generated_at": "2026-06-30T00:00:00+00:00",
        "paper_lane_count": 2,
        "active_lane_ids": ["lane_a", "lane_b"],
        "lanes": [
            {"lane_id": "lane_a", "symbol": "MGC", "huge_diagnostics": "x" * 10_000},
            {"lane_id": "lane_b", "symbol": "ES", "huge_diagnostics": "y" * 10_000},
        ],
        "nested_history": [{"payload": "z" * 1000} for _ in range(50)],
    }

    result = write_bounded_snapshot_json(
        path,
        payload,
        config=BoundedSnapshotConfig(max_bytes=2500, target_bytes=1200, max_items=4, max_string_chars=80),
        diagnostic_path=diagnostic,
    )

    assert result.degraded is True
    assert result.original_bytes > result.written_bytes
    assert path.stat().st_size <= 2500
    written = _read_json(path)
    assert written["schema_version"] == "operator_status_v1"
    assert written["generated_at"] == "2026-06-30T00:00:00+00:00"
    assert written["paper_lane_count"] == 2
    assert written["_bounded_snapshot"]["degraded"] is True
    assert "nested_history" in written["_bounded_snapshot"]["omitted_sections"]
    diag = _read_json(diagnostic)
    assert diag["classification"] == "BOUNDED_SNAPSHOT_DEGRADED"
    assert diag["diagnostic_only"] is True
    assert diag["snapshot_path"] == str(path)


def test_bounded_snapshot_atomic_write_removes_temp_file(tmp_path: Path) -> None:
    path = tmp_path / "operator_status.json"

    write_bounded_snapshot_json(path, {"generated_at": "now", "value": "ok"})

    assert path.exists()
    assert not list(tmp_path.glob(".operator_status.json.*.tmp"))


def test_bounded_snapshot_raises_when_even_minimal_payload_exceeds_limit(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="bounded snapshot still exceeds max_bytes"):
        write_bounded_snapshot_json(
            tmp_path / "operator_status.json",
            {"generated_at": "now", "active_lane_ids": ["lane"] * 1000},
            config=BoundedSnapshotConfig(max_bytes=20, target_bytes=10),
        )


def test_structured_logger_operator_status_uses_bounded_snapshot(tmp_path: Path) -> None:
    logger = StructuredLogger(tmp_path)
    payload = {
        "generated_at": "2026-06-30T00:00:00+00:00",
        "paper_lane_count": 1,
        "active_lane_ids": ["mnq_us_active_participation_long"],
        "lanes": [
            {
                "lane_id": "mnq_us_active_participation_long",
                "symbol": "MNQ",
                "huge_diagnostics": "x" * 8_000_000,
            }
        ],
    }

    path = logger.write_operator_status(payload)

    assert path == tmp_path / "operator_status.json"
    assert path.stat().st_size <= BoundedSnapshotConfig().max_bytes
    written = _read_json(path)
    assert written["paper_lane_count"] == 1
    assert written["_bounded_snapshot"]["degraded"] is True
    assert (tmp_path / "operator_status_bounded_snapshot_diagnostic.json").exists()


def test_bounded_snapshot_preserves_blocked_intent_summary_fields(tmp_path: Path) -> None:
    path = tmp_path / "blocked_strategy_intent_latest.json"
    payload = {
        "schema_version": "strategy_managed_blocked_intent_v1",
        "artifact_type": "blocked_strategy_intent",
        "created_at": "2026-06-30T00:00:00+00:00",
        "lane_id": "mnq_us_active_participation_long",
        "strategy_id": "mnq_us_active_participation_long",
        "strategy_family": "ACTIVE_PARTICIPATION",
        "symbol": "MNQ",
        "side": "LONG",
        "order_intent_id": "intent-1",
        "intent_type": "BUY_TO_OPEN",
        "bar_id": "MNQ|1m|2026-06-30T00:00:00Z",
        "submit_allowed": False,
        "submit_attempted": True,
        "blocker_classification": "ROUTE_HEALTH_STALE",
        "exact_blocker_reason": "route health is stale",
        "bridge_classification": "PAPER_STRATEGY_INTENT_BLOCKED",
        "bridge_gate_trace": [{"authority": {"nested": [{"payload": "x" * 1000} for _ in range(100)]}}],
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }

    result = write_bounded_snapshot_json(
        path,
        payload,
        config=BoundedSnapshotConfig(max_bytes=2200, target_bytes=1200, max_items=8, max_string_chars=120),
    )

    assert result.degraded is True
    written = _read_json(path)
    assert written["schema_version"] == "strategy_managed_blocked_intent_v1"
    assert written["artifact_type"] == "blocked_strategy_intent"
    assert written["created_at"] == "2026-06-30T00:00:00+00:00"
    assert written["lane_id"] == "mnq_us_active_participation_long"
    assert written["strategy_id"] == "mnq_us_active_participation_long"
    assert written["symbol"] == "MNQ"
    assert written["blocker_classification"] == "ROUTE_HEALTH_STALE"
    assert written["exact_blocker_reason"] == "route health is stale"
    assert written["submit_allowed"] is False
    assert written["paper_proof_invoked"] is False
    assert written["live_money_readiness"] is False
    assert "bridge_gate_trace" in written["_bounded_snapshot"]["omitted_sections"]


def test_structured_logger_blocked_intent_latest_uses_bounded_snapshot(tmp_path: Path) -> None:
    logger = StructuredLogger(tmp_path)
    payload = {
        "schema_version": "strategy_managed_blocked_intent_v1",
        "artifact_type": "blocked_strategy_intent",
        "created_at": "2026-06-30T00:00:00+00:00",
        "lane_id": "mnq_us_active_participation_long",
        "strategy_id": "mnq_us_active_participation_long",
        "symbol": "MNQ",
        "blocker_classification": "ROUTE_HEALTH_STALE",
        "exact_blocker_reason": "route health is stale",
        "submit_allowed": False,
        "bridge_gate_trace": [{"payload": "x" * 8_000_000}],
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }

    path = logger.write_blocked_strategy_intent_state(payload)

    assert path == tmp_path / "blocked_strategy_intent_latest.json"
    assert path.stat().st_size <= BoundedSnapshotConfig().max_bytes
    written = _read_json(path)
    assert written["blocker_classification"] == "ROUTE_HEALTH_STALE"
    assert written["lane_id"] == "mnq_us_active_participation_long"
    assert written["_bounded_snapshot"]["degraded"] is True
    assert (tmp_path / "blocked_strategy_intent_bounded_snapshot_diagnostic.json").exists()
