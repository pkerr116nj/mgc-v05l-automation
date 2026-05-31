from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import TrackBTruthSnapshotConfig
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType
from mgc_v05l.execution_core.track_b_registry_truth_diagnostics import (
    TRACK_B_DIAGNOSTICS_CLEAN,
    TRACK_B_DIAGNOSTICS_CONFLICT,
    TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED,
    TRACK_B_DIAGNOSTICS_STALE,
    TrackBRegistryTruthDiagnosticsConfig,
    build_track_b_registry_truth_diagnostics,
    write_track_b_registry_truth_diagnostics,
)
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import TradeRegistryReconstructionConfig
from mgc_v05l.execution_core.track_b_trade_registry_shadow_report import TradeRegistryShadowReportConfig


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
BASE_TIME = "2026-05-29T05:54:30+00:00"


def test_clean_flat_state_reports_clean(tmp_path: Path) -> None:
    config = _seed_config(tmp_path)
    _write_jsonl(
        tmp_path / "fixtures/ledger.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_clean",
                lifecycle_id="life_clean",
                order_id="1",
                client_id="17",
                perm_id="2",
                exec_id="3",
            ),
            _event(TradeEventType.RECONCILED_FLAT, trade_id="trade_clean", lifecycle_id="life_clean"),
        ],
    )

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_CLEAN
    assert report.runtime_ready is True
    assert report.recovery_active is True
    assert report.broker_lifecycle_reconciled is True
    assert report.broker_position_count == 0
    assert report.registry_trade_state_counts == {TradeCurrentState.CLOSED_FLAT.value: 1}
    assert report.latest_preflight_hard_failure_count == 0


def test_open_managed_trade_surfaces_open_state(tmp_path: Path) -> None:
    config = _seed_config(tmp_path, broker_positions=[{"symbol": "MNQ", "position": 1}])
    _write_json(
        tmp_path / config.truth_config.managed_position_registry_path,  # type: ignore[union-attr]
        {
            "generated_at": NOW.isoformat(),
            "managed_positions": [
                {
                    "lifecycle_id": "life_open",
                    "account_id": "DUM882026",
                    "exact_lifecycle_account_id": "DUM882026",
                    "con_id": 770561201,
                    "localSymbol": "MNQM6",
                    "qty": 1,
                    "state": "OPEN_MANAGED",
                }
            ],
        },
    )
    _write_jsonl(
        tmp_path / "fixtures/ledger.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_open",
                lifecycle_id="life_open",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                order_id="10",
                client_id="17",
                perm_id="20",
                exec_id="30",
            ),
            _event(
                TradeEventType.LIFECYCLE_OPEN_MANAGED,
                trade_id="trade_open",
                lifecycle_id="life_open",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
            ),
        ],
    )

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_CLEAN
    assert report.broker_position_count == 1
    assert report.lifecycle_open_position_count == 1
    assert report.registry_trade_state_counts == {TradeCurrentState.OPEN_MANAGED.value: 1}


def test_review_required_trade_reports_review_required(tmp_path: Path) -> None:
    config = _seed_config(tmp_path)
    _write_jsonl(
        tmp_path / "fixtures/ledger.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_review",
                lifecycle_id="life_review",
                order_id="1",
                client_id="17",
                perm_id=None,
                exec_id=None,
            )
        ],
    )

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED
    assert report.review_required_trade_ids == ("trade_review",)
    assert "REGISTRY_REVIEW_REQUIRED_TRADE" in report.reason_codes


def test_stale_truth_snapshot_reports_stale(tmp_path: Path) -> None:
    config = _seed_config(tmp_path)
    _write_json(
        tmp_path / config.truth_config.broker_status_path,  # type: ignore[union-attr]
        {
            "generated_at": (NOW - timedelta(minutes=20)).isoformat(),
            "positions_snapshot_path": str(tmp_path / config.truth_config.broker_positions_path),  # type: ignore[union-attr]
            "open_orders_snapshot_path": str(tmp_path / config.truth_config.broker_open_orders_path),  # type: ignore[union-attr]
        },
    )

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_STALE
    assert "TRUTH_AUTHORITY_STALE" in report.reason_codes


def test_registry_reconciliation_disagreement_reports_conflict(tmp_path: Path) -> None:
    config = _seed_config(tmp_path, broker_positions=[{"symbol": "MNQ", "position": 1}])
    _write_jsonl(
        tmp_path / "fixtures/ledger.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_conflict",
                lifecycle_id="life_conflict",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                order_id="1",
                client_id="17",
                perm_id="2",
                exec_id="3",
            ),
            _event(
                TradeEventType.RECONCILED_FLAT,
                trade_id="trade_conflict",
                lifecycle_id="life_conflict",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
            ),
        ],
    )

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_CONFLICT
    assert report.registry_reconciliation_disagreements == ("trade_conflict",)
    assert "REGISTRY_RECONCILIATION_DISAGREEMENT" in report.reason_codes


def test_stress_preflight_hard_failure_blocks_clean_status(tmp_path: Path) -> None:
    config = _seed_config(tmp_path)
    _write_preflight(tmp_path / config.preflight_summary_path, hard_failures=2)

    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    assert report.classification == TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED
    assert report.latest_preflight_hard_failure_count == 2
    assert "LIFECYCLE_STRESS_PREFLIGHT_HARD_FAILURE" in report.reason_codes


def test_diagnostics_report_writer_outputs_json(tmp_path: Path) -> None:
    config = _seed_config(tmp_path)
    report = build_track_b_registry_truth_diagnostics(config=config, now=NOW)

    path = write_track_b_registry_truth_diagnostics(config=config, report=report)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_b_registry_truth_diagnostics_v1"
    assert payload["classification"] == TRACK_B_DIAGNOSTICS_CLEAN
    assert payload["broker_mutation_allowed"] is False
    assert payload["runtime_restart_allowed"] is False
    assert payload["production_gate_wiring_allowed"] is False


def _seed_config(
    tmp_path: Path,
    *,
    broker_positions: list[dict] | None = None,
) -> TrackBRegistryTruthDiagnosticsConfig:
    truth_config = _seed_truth(tmp_path, broker_positions=broker_positions or [])
    reconstruction_config = TradeRegistryReconstructionConfig(
        repo_root=tmp_path,
        submit_intent_ownership_path=Path("fixtures/submit_intents.jsonl"),
        bridge_report_paths=(),
        filled_bridge_result_paths=(),
        lifecycle_ledger_paths=(Path("fixtures/ledger.jsonl"),),
        broker_truth_status_path=truth_config.broker_status_path,
        broker_positions_path=truth_config.broker_positions_path,
        broker_open_orders_path=truth_config.broker_open_orders_path,
        reconciliation_path=truth_config.reconciliation_path,
        managed_position_registry_path=Path("fixtures/reconstruction_managed_positions.json"),
        managed_order_registry_path=Path("fixtures/reconstruction_managed_orders.json"),
    )
    shadow_config = TradeRegistryShadowReportConfig(
        repo_root=tmp_path,
        truth_config=truth_config,
        reconstruction_config=reconstruction_config,
    )
    config = TrackBRegistryTruthDiagnosticsConfig(
        repo_root=tmp_path,
        truth_config=truth_config,
        reconstruction_config=reconstruction_config,
        shadow_config=shadow_config,
        preflight_summary_path=Path("fixtures/preflight.json"),
        output_path=Path("fixtures/diagnostics.json"),
    )
    _write_jsonl(tmp_path / "fixtures/ledger.jsonl", [])
    _write_jsonl(tmp_path / "fixtures/submit_intents.jsonl", [])
    _write_json(tmp_path / "fixtures/reconstruction_managed_positions.json", {"generated_at": NOW.isoformat(), "managed_positions": []})
    _write_json(tmp_path / "fixtures/reconstruction_managed_orders.json", {"generated_at": NOW.isoformat(), "managed_orders": []})
    _write_preflight(tmp_path / config.preflight_summary_path, hard_failures=0)
    return config


def _seed_truth(tmp_path: Path, *, broker_positions: list[dict]) -> TrackBTruthSnapshotConfig:
    config = TrackBTruthSnapshotConfig(repo_root=tmp_path)
    _write_json(
        tmp_path / config.runtime_truth_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE",
            "runtime": {
                "pid": 1234,
                "pid_alive": True,
                "runtime_instance_id": "generation-1",
                "lane_count": 8,
            },
            "canonical_readiness": {"classification": "READY_SUBMIT_CAPABLE", "ready_submit_capable": True},
        },
    )
    _write_json(
        tmp_path / config.recovery_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_ACTIVE",
            "launchd_loaded": True,
            "launchd_enabled": True,
            "last_tick": NOW.isoformat(),
        },
    )
    _write_json(
        tmp_path / config.recovery_audit_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_HEALTHY_NO_ACTION",
            "hourly_supervisor": {"classification": "SUPERVISOR_RUNNING", "active": True},
        },
    )
    _write_json(
        tmp_path / config.broker_status_path,
        {
            "generated_at": NOW.isoformat(),
            "positions_snapshot_path": str(tmp_path / config.broker_positions_path),
            "open_orders_snapshot_path": str(tmp_path / config.broker_open_orders_path),
        },
    )
    _write_json(tmp_path / config.broker_positions_path, {"generated_at": NOW.isoformat(), "positions": broker_positions})
    _write_json(tmp_path / config.broker_open_orders_path, {"generated_at": NOW.isoformat(), "open_orders": []})
    _write_json(tmp_path / config.lifecycle_live_position_path, {"generated_at": NOW.isoformat(), "open_positions": []})
    _write_json(tmp_path / config.managed_position_registry_path, {"generated_at": NOW.isoformat(), "managed_positions": []})
    _write_json(tmp_path / config.managed_order_registry_path, {"generated_at": NOW.isoformat(), "managed_orders": []})
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_LIFECYCLE_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
        },
    )
    _write_json(
        tmp_path / config.safe_state_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SAFE_STATE_NORMAL",
            "submit_allowed": True,
            "runtime_start_allowed": True,
        },
    )
    _write_json(
        tmp_path / config.control_plane_path,
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
        },
    )
    _write_json(
        tmp_path / config.planner_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "PLAN_SCOPED_POSITION_CLEANUP",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
        },
    )
    _write_json(
        tmp_path / config.supervisor_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_decision_id": "supervisor-1",
        },
    )
    _write_json(
        tmp_path / config.contract_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTRACT_ALLOWED",
            "submit_allowed": True,
            "symbol": "MNQ",
            "selected_contract": {"localSymbol": "MNQM6", "conId": 770561201, "expiry": "202606"},
        },
    )
    _write_json(
        tmp_path / config.broker_backed_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "fills": [{"order_id": "1", "client_id": "17", "perm_id": "2", "exec_id": "3"}],
        },
    )
    _write_json(tmp_path / config.local_paper_artifact_path, {"generated_at": NOW.isoformat(), "local_rows": []})
    if config.dashboard_runtime_path is not None:
        _write_json(tmp_path / config.dashboard_runtime_path, {"generated_at": NOW.isoformat(), "diagnostic": True})
    return config


def _write_preflight(path: Path, *, hard_failures: int) -> None:
    _write_json(
        path,
        {
            "generated_at": NOW.isoformat(),
            "passed": hard_failures == 0,
            "summary": {
                "hard_failure_stage_count": hard_failures,
                "unexpected_invariant_failures": 0,
                "reducer_crashes": 0,
                "trade_id_collisions": 0,
                "silent_ambiguity_merges": 0,
                "impossible_states": 0,
                "bad_lifecycles_without_reason_codes": 0,
                "broker_backed_evidence_violations": 0,
            },
        },
    )


def _event(event_type: TradeEventType, **overrides: object) -> dict[str, object]:
    payload = {
        "event_id": f"{event_type.value}_{overrides.get('trade_id', 'trade')}",
        "event_type": event_type.value,
        "generated_at": BASE_TIME,
        "trade_id": "trade_fixture",
        "lifecycle_id": "life_fixture",
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "mnq_us_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY_TO_OPEN",
        "qty": "1",
        "source_artifact_path": "fixtures/ledger.jsonl",
    }
    payload.update(overrides)
    return payload


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""), encoding="utf-8")
