from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import TrackBTruthSnapshotConfig
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    AMBIGUOUS_TRADE_ID,
    SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT,
    TradeRegistryReconstructionConfig,
)
from mgc_v05l.execution_core.track_b_trade_registry_shadow_report import (
    TradeRegistryShadowReportConfig,
    build_trade_registry_shadow_report,
    write_trade_registry_shadow_report,
)


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
BASE_TIME = "2026-05-29T05:54:30+00:00"


def test_shadow_report_compares_clean_mes_lifecycle(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/mes_lifecycle.jsonl"),))
    _write_jsonl(
        tmp_path / "fixtures/mes_lifecycle.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_mes",
                lifecycle_id="mes_lifecycle",
                order_id="1001",
                client_id="17",
                perm_id="2047276068",
                exec_id="0000e1a7.6a2870c7.01.01",
                price="7586.75",
            ),
            _event(TradeEventType.LIFECYCLE_OPEN_MANAGED, trade_id="trade_mes", lifecycle_id="mes_lifecycle"),
            _event(TradeEventType.EXIT_INTENT_CREATED, trade_id="trade_mes", lifecycle_id="mes_lifecycle"),
            _event(
                TradeEventType.EXIT_FILL_BROKER_BACKED,
                trade_id="trade_mes",
                lifecycle_id="mes_lifecycle",
                order_id="1002",
                client_id="17",
                perm_id="2047276077",
                exec_id="0000e1a7.6a2870d0.01.01",
                action="SELL_TO_CLOSE",
                price="7588.25",
            ),
            _event(TradeEventType.RECONCILED_FLAT, trade_id="trade_mes", lifecycle_id="mes_lifecycle"),
        ],
    )

    report = build_trade_registry_shadow_report(config=config, now=NOW)
    row = report.rows[0]

    assert report.read_only is True
    assert report.diagnostic_only is True
    assert report.broker_mutation_allowed is False
    assert row.trade_id == "trade_mes"
    assert row.current_derived_state == TradeCurrentState.CLOSED_FLAT.value
    assert row.broker_backed_entry is True
    assert row.broker_backed_exit is True
    assert row.reconciliation_reconciled is True
    assert row.registry_agrees_with_reconciliation is True
    assert row.truth_conflicts == ()


def test_shadow_report_covers_assisted_mnq_lifecycle(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/mnq_assisted.jsonl"),))
    _write_jsonl(
        tmp_path / "fixtures/mnq_assisted.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                order_id="1101",
                client_id="17",
                perm_id="2047276405",
                exec_id="0000e1a7.6a29f525.01.01",
            ),
            _event(
                TradeEventType.LIFECYCLE_OPEN_MANAGED,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
            ),
            _event(
                TradeEventType.EXIT_INTENT_CREATED,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                action="SELL_TO_CLOSE",
                reason_codes=["ASSISTED_MANAGED_EXIT_REPAIR"],
            ),
            _event(
                TradeEventType.EXIT_FILL_BROKER_BACKED,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                action="SELL_TO_CLOSE",
                order_id="1102",
                client_id="17",
                perm_id="2047276416",
                exec_id="0000e1a7.6a29f5aa.01.01",
            ),
            _event(
                TradeEventType.RECONCILED_FLAT,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                action="SELL_TO_CLOSE",
            ),
        ],
    )

    row = build_trade_registry_shadow_report(config=config, now=NOW).rows[0]

    assert row.symbol == "MNQ"
    assert row.local_symbol == "MNQM6"
    assert row.current_derived_state == TradeCurrentState.CLOSED_FLAT.value
    assert row.registry_agrees_with_reconciliation is True
    assert "ASSISTED_MANAGED_EXIT_REPAIR" in row.event_chain_summary[2]["reason_codes"]


def test_shadow_report_covers_passive_cancel(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path, bridge_report_paths=(Path("fixtures/passive_cancel.json"),))
    base = _row(
        trade_id="trade_passive_cancel",
        lifecycle_id="mnq_cancel_lifecycle",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1201",
        client_id="17",
    )
    _write_json(
        tmp_path / "fixtures/passive_cancel.json",
        {
            "orders": [
                {**base, "classification": "SUBMITTED", "submit_attempt_id": "submit-1"},
                {
                    **base,
                    "classification": "CANCELLED",
                    "created_at": "2026-05-29T05:55:00+00:00",
                    "reason_codes": ["PASSIVE_LIMIT_TIMEOUT_EXPECTED", "LIMIT_PRICE_TOO_PASSIVE"],
                },
            ]
        },
    )

    row = build_trade_registry_shadow_report(config=config, now=NOW).rows[0]

    assert row.current_derived_state == TradeCurrentState.CANCELLED.value
    assert row.broker_backed_entry is False
    assert row.registry_agrees_with_reconciliation is True
    assert row.event_chain_summary[-1]["reason_codes"] == [
        "PASSIVE_LIMIT_TIMEOUT_EXPECTED",
        "LIMIT_PRICE_TOO_PASSIVE",
    ]


def test_shadow_report_marks_no_broker_effect_submit_intent_review_required(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path)
    reconstruction_config = config.reconstruction_config
    assert reconstruction_config is not None
    _write_jsonl(
        tmp_path / reconstruction_config.submit_intent_ownership_path,
        [
            {
                **_row(trade_id="trade_stale_intent", lifecycle_id="stale_lifecycle"),
                "ownership_intent_id": "ownership_stale_1",
                "intent_type": "ENTRY",
                "state": SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT,
                "created_at": BASE_TIME,
            }
        ],
    )

    row = build_trade_registry_shadow_report(config=config, now=NOW).rows[0]

    assert row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value
    assert row.ambiguous_reconstruction is True
    assert row.registry_agrees_with_reconciliation is False
    assert SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT in row.missing_links[0]["reason_codes"]


def test_shadow_report_marks_ambiguous_reconstruction_review_required(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path, bridge_report_paths=(Path("fixtures/ambiguous.json"),))
    _write_json(
        tmp_path / "fixtures/ambiguous.json",
        {
            "orders": [
                {
                    "submit_attempt_id": "ambiguous-1",
                    "classification": "SUBMITTED",
                    "lane_id": "mnq_us_active_participation_long",
                    "strategy_id": "mnq_us_active_participation_long",
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "expiry": "202606",
                    "con_id": 770561201,
                    "side": "LONG",
                    "action": "BUY_TO_OPEN",
                    "qty": "1",
                    "created_at": BASE_TIME,
                }
            ]
        },
    )

    row = build_trade_registry_shadow_report(config=config, now=NOW).rows[0]

    assert row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value
    assert row.ambiguous_reconstruction is True
    assert AMBIGUOUS_TRADE_ID in row.missing_links[0]["reason_codes"]


def test_shadow_report_writer_outputs_json(tmp_path: Path) -> None:
    config = _seed_shadow_config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/closed.jsonl"),))
    _write_jsonl(
        tmp_path / "fixtures/closed.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_writer",
                lifecycle_id="writer_lifecycle",
                order_id="1",
                client_id="17",
                perm_id="2",
                exec_id="3",
            ),
            _event(TradeEventType.RECONCILED_FLAT, trade_id="trade_writer", lifecycle_id="writer_lifecycle"),
        ],
    )

    report = build_trade_registry_shadow_report(config=config, now=NOW)
    path = write_trade_registry_shadow_report(config=config, report=report)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_b_trade_registry_shadow_report_v1"
    assert payload["rows"][0]["trade_id"] == "trade_writer"
    assert payload["summary"]["trade_count"] == 1


def _seed_shadow_config(
    tmp_path: Path,
    *,
    lifecycle_ledger_paths: tuple[Path, ...] = (),
    bridge_report_paths: tuple[Path, ...] = (),
) -> TradeRegistryShadowReportConfig:
    truth_config = _seed_clean_truth(tmp_path)
    reconstruction_config = TradeRegistryReconstructionConfig(
        repo_root=tmp_path,
        submit_intent_ownership_path=Path(
            "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
        ),
        bridge_report_paths=bridge_report_paths,
        filled_bridge_result_paths=(),
        lifecycle_ledger_paths=lifecycle_ledger_paths,
        broker_truth_status_path=truth_config.broker_status_path,
        broker_positions_path=truth_config.broker_positions_path,
        broker_open_orders_path=truth_config.broker_open_orders_path,
        reconciliation_path=truth_config.reconciliation_path,
        managed_position_registry_path=truth_config.managed_position_registry_path,
        managed_order_registry_path=truth_config.managed_order_registry_path,
        output_path=Path("fixtures/reconstruction.json"),
    )
    return TradeRegistryShadowReportConfig(
        repo_root=tmp_path,
        output_path=Path("fixtures/shadow_report.json"),
        reconstruction_config=reconstruction_config,
        truth_config=truth_config,
    )


def _seed_clean_truth(tmp_path: Path) -> TrackBTruthSnapshotConfig:
    config = TrackBTruthSnapshotConfig(repo_root=tmp_path)
    _write_json(
        tmp_path / config.runtime_truth_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE",
            "runtime": {"pid": 1234, "pid_alive": True, "runtime_instance_id": "generation-1", "lane_count": 8},
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
    _write_json(tmp_path / config.broker_positions_path, {"generated_at": NOW.isoformat(), "positions": []})
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
            "fills": [
                {"order_id": "39", "client_id": "7", "perm_id": "2047276405", "exec_id": "0000e1a7.01"}
            ],
        },
    )
    _write_json(tmp_path / config.local_paper_artifact_path, {"generated_at": NOW.isoformat(), "local_rows": []})
    if config.dashboard_runtime_path is not None:
        _write_json(tmp_path / config.dashboard_runtime_path, {"generated_at": NOW.isoformat(), "diagnostic": True})
    return config


def _event(event_type: TradeEventType, **overrides: object) -> dict[str, object]:
    payload = {
        **_row(),
        "event_id": f"{event_type.value}_{overrides.get('trade_id', 'trade')}_{overrides.get('order_id', '0')}",
        "event_type": event_type.value,
        "generated_at": BASE_TIME,
        "source_artifact_path": "fixture.jsonl",
    }
    payload.update(overrides)
    return payload


def _row(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "trade_id": "trade_fixture",
        "lifecycle_id": "fixture_lifecycle",
        "lane_id": "mes_globex_active_participation_long",
        "thesis_strategy_id": "mes_globex_active_participation_long",
        "strategy_id": "mes_globex_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MES",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY_TO_OPEN",
        "qty": "1",
        "created_at": BASE_TIME,
    }
    payload.update(overrides)
    return payload


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
