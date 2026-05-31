from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    AMBIGUOUS_TRADE_ID,
    SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT,
    TradeRegistryReconstructionConfig,
    reconstruct_trade_registry_from_artifacts,
    write_trade_registry_reconstruction_report,
)


BASE_TIME = "2026-05-29T05:54:30+00:00"


def test_reconstructs_mes_full_managed_lifecycle_from_real_artifact_chain(tmp_path: Path) -> None:
    config = _config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/mes_lifecycle.jsonl"),))
    _write_jsonl(
        tmp_path / "fixtures/mes_lifecycle.jsonl",
        [
            _event(TradeEventType.ENTRY_INTENT_CREATED, trade_id="trade_mes", lifecycle_id="mes_lifecycle"),
            _event(
                TradeEventType.ENTRY_ORDER_SUBMITTED,
                trade_id="trade_mes",
                lifecycle_id="mes_lifecycle",
                order_id="1001",
                client_id="17",
            ),
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
                TradeEventType.EXIT_ORDER_SUBMITTED,
                trade_id="trade_mes",
                lifecycle_id="mes_lifecycle",
                order_id="1002",
                client_id="17",
                action="SELL_TO_CLOSE",
            ),
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

    report = reconstruct_trade_registry_from_artifacts(config=config)

    assert report.read_only is True
    assert report.broker_mutation_allowed is False
    assert report.runtime_restart_allowed is False
    assert len(report.records) == 1
    record = report.records[0]
    assert record.current_state == TradeCurrentState.CLOSED_FLAT
    assert record.broker_backed_entry is True
    assert record.broker_backed_exit is True
    assert record.open_qty == 0
    assert report.missing_links == ()


def test_reconstructs_mnq_assisted_lifecycle_close_as_broker_backed_closed_flat(tmp_path: Path) -> None:
    config = _config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/mnq_assisted_lifecycle.jsonl"),))
    _write_jsonl(
        tmp_path / "fixtures/mnq_assisted_lifecycle.jsonl",
        [
            _event(
                TradeEventType.ENTRY_FILL_BROKER_BACKED,
                trade_id="trade_mnq_assisted",
                lifecycle_id="bridge_fill_MNQ_2026-05-29",
                symbol="MNQ",
                local_symbol="MNQM6",
                con_id=770561201,
                perm_id="2047276405",
                exec_id="0000e1a7.6a29f525.01.01",
                order_id="1101",
                client_id="17",
                price="30380.25",
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
                price="30391.75",
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

    record = reconstruct_trade_registry_from_artifacts(config=config).records[0]

    assert record.current_state == TradeCurrentState.CLOSED_FLAT
    assert record.broker_backed_entry is True
    assert record.broker_backed_exit is True
    assert "ASSISTED_MANAGED_EXIT_REPAIR" in record.latest_reason_codes


def test_reconstructs_mnq_passive_entry_cancel_from_bridge_report(tmp_path: Path) -> None:
    config = _config(tmp_path, bridge_report_paths=(Path("fixtures/mnq_passive_bridge.json"),))
    base = _row(
        trade_id="trade_mnq_cancel",
        lifecycle_id="mnq_cancel_lifecycle",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1201",
        client_id="17",
        action="BUY_TO_OPEN",
    )
    _write_json(
        tmp_path / "fixtures/mnq_passive_bridge.json",
        {
            "orders": [
                {**base, "classification": "SUBMITTED", "submit_attempt_id": "mnq_submit_1"},
                {
                    **base,
                    "classification": "CANCELLED",
                    "created_at": "2026-05-29T05:55:00+00:00",
                    "reason_codes": ["PASSIVE_LIMIT_TIMEOUT_EXPECTED", "LIMIT_PRICE_TOO_PASSIVE"],
                },
            ]
        },
    )

    record = reconstruct_trade_registry_from_artifacts(config=config).records[0]

    assert record.current_state == TradeCurrentState.CANCELLED
    assert record.broker_backed_entry is False
    assert [event.event_type for event in record.event_chain] == [
        TradeEventType.ENTRY_ORDER_SUBMITTED,
        TradeEventType.ENTRY_ORDER_CANCELLED,
    ]


def test_stale_no_broker_effect_submit_intent_reconstructs_review_required(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_jsonl(
        tmp_path / config.submit_intent_ownership_path,
        [
            {
                **_row(trade_id="trade_stale_intent", lifecycle_id="stale_lifecycle", order_id=None, client_id=None),
                "ownership_intent_id": "ownership_stale_1",
                "intent_type": "ENTRY",
                "state": SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT,
                "created_at": BASE_TIME,
            }
        ],
    )

    report = reconstruct_trade_registry_from_artifacts(config=config)
    record = report.records[0]

    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert record.broker_backed_entry is False
    assert SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT in record.latest_reason_codes
    assert report.missing_links[0]["trade_id"] == "trade_stale_intent"


def test_ambiguous_reconstruction_is_not_merged_silently(tmp_path: Path) -> None:
    config = _config(tmp_path, bridge_report_paths=(Path("fixtures/ambiguous_bridge.json"),))
    _write_json(
        tmp_path / "fixtures/ambiguous_bridge.json",
        {
            "orders": [
                {
                    "submit_attempt_id": "ambiguous_1",
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

    report = reconstruct_trade_registry_from_artifacts(config=config)
    record = report.records[0]

    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert AMBIGUOUS_TRADE_ID in record.latest_reason_codes
    assert AMBIGUOUS_TRADE_ID in report.ambiguity_reason_codes


def test_report_writer_emits_read_only_report(tmp_path: Path) -> None:
    config = _config(tmp_path, lifecycle_ledger_paths=(Path("fixtures/closed.jsonl"),))
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

    report = reconstruct_trade_registry_from_artifacts(config=config)
    path = write_trade_registry_reconstruction_report(config=config, report=report)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["read_only"] is True
    assert payload["broker_mutation_allowed"] is False
    assert payload["runtime_restart_allowed"] is False
    assert payload["records"][0]["trade_id"] == "trade_writer"


def _config(
    tmp_path: Path,
    *,
    lifecycle_ledger_paths: tuple[Path, ...] = (),
    bridge_report_paths: tuple[Path, ...] = (),
) -> TradeRegistryReconstructionConfig:
    return TradeRegistryReconstructionConfig(
        repo_root=tmp_path,
        submit_intent_ownership_path=Path(
            "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
        ),
        bridge_report_paths=bridge_report_paths,
        filled_bridge_result_paths=(),
        lifecycle_ledger_paths=lifecycle_ledger_paths,
        broker_truth_status_path=Path("fixtures/broker_status.json"),
        broker_positions_path=Path("fixtures/broker_positions.json"),
        broker_open_orders_path=Path("fixtures/broker_open_orders.json"),
        reconciliation_path=Path("fixtures/reconciliation.json"),
        managed_position_registry_path=Path("fixtures/managed_positions.json"),
        managed_order_registry_path=Path("fixtures/managed_orders.json"),
        output_path=Path("fixtures/report.json"),
    )


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


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
