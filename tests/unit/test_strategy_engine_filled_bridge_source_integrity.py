from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.monitoring.logger import StructuredLogger
from mgc_v05l.strategy.strategy_engine import validate_filled_bridge_source_integrity


def test_contract_consistent_fill_passes_source_integrity() -> None:
    payload = _fill_payload(
        instrument="GC",
        local_symbol="GCQ6",
        con_id=732156872,
        exec_id="exec_valid_gc",
        lifecycle_id="bridge_fill_GC|1m|2026-07-17T06:29:00Z|BUY_TO_OPEN",
        fill_price="4001.7",
        limit_price="4001.6",
    )

    result = validate_filled_bridge_source_integrity(payload)

    assert result["classification"] == "SOURCE_INTEGRITY_VALID"
    assert result["valid_for_filled_bridge_persistence"] is True
    assert result["quarantine_required"] is False


def test_foreign_domain_price_evidence_is_quarantined_without_gc_nq_exception() -> None:
    payload = _fill_payload(
        instrument="GC",
        local_symbol="GCQ6",
        con_id=732156872,
        exec_id="0000e1a7.6a690a8e.01.01",
        lifecycle_id="bridge_fill_GC|1m|2026-07-17T06:29:00Z|BUY_TO_OPEN",
        fill_price="28757.0",
        limit_price="4001.7",
    )

    result = validate_filled_bridge_source_integrity(payload)

    assert result["quarantine_required"] is True
    assert "foreign_domain_price_discontinuity" in result["reasons"]
    assert result["price_discontinuities"][0]["reference_source"] == "limit_price"


def test_gc_and_nq_contract_identity_cannot_cross_link() -> None:
    payload = _fill_payload(
        instrument="GC",
        local_symbol="NQU6",
        con_id=770561204,
        exec_id="exec_cross_link",
        lifecycle_id="bridge_fill_GC|1m|2026-07-27T22:05:00Z|SELL_TO_OPEN",
        fill_price="4073.6",
        contract={
            "symbol": "NQ",
            "local_symbol": "NQU6",
            "con_id": 770561204,
            "multiplier": "20",
        },
    )

    result = validate_filled_bridge_source_integrity(payload)

    assert result["quarantine_required"] is True
    assert "contract_symbol_mismatch" in result["reasons"]


def test_duplicate_exec_across_unrelated_cycles_is_rejected() -> None:
    existing = [
        _fill_payload(
            instrument="NQ",
            local_symbol="NQU6",
            con_id=770561204,
            exec_id="0000e1a7.6a72d50c.01.01",
            lifecycle_id="bridge_fill_NQ|1m|2026-07-27T05:16:00Z|BUY_TO_OPEN",
            fill_price="28614.612",
            broker_order_id="1123",
            perm_id="1386665059",
        )
    ]
    payload = _fill_payload(
        instrument="NQ",
        local_symbol="NQU6",
        con_id=770561204,
        exec_id="0000e1a7.6a72d50c.01.01",
        lifecycle_id="bridge_fill_NQ|1m|2026-07-27T22:11:00Z|SELL_TO_OPEN",
        fill_price="4073.6",
        broker_order_id="10",
        perm_id="",
    )

    result = validate_filled_bridge_source_integrity(payload, existing_rows=existing)

    assert result["quarantine_required"] is True
    assert "exec_id_reused_across_unrelated_position_cycles" in result["reasons"]
    assert result["exec_id_conflicts"][0]["existing_lifecycle_id"] == existing[0]["lifecycle_id"]


def test_explicit_partial_fill_reuse_remains_supported() -> None:
    existing = [
        _fill_payload(
            instrument="MES",
            local_symbol="MESU6",
            con_id=123,
            exec_id="partial_exec",
            lifecycle_id="bridge_fill_MES|1m|2026-07-27T05:16:00Z|BUY_TO_OPEN",
            fill_price="6200.25",
            broker_order_id="42",
            perm_id="9001",
            partial_fill_sequence=1,
        )
    ]
    payload = _fill_payload(
        instrument="MES",
        local_symbol="MESU6",
        con_id=123,
        exec_id="partial_exec",
        lifecycle_id="bridge_fill_MES|1m|2026-07-27T05:16:00Z|BUY_TO_OPEN",
        fill_price="6200.5",
        broker_order_id="42",
        perm_id="9001",
        partial_fill_sequence=2,
    )

    result = validate_filled_bridge_source_integrity(payload, existing_rows=existing)

    assert result["classification"] == "SOURCE_INTEGRITY_VALID"
    assert result["exec_id_conflicts"] == []


def test_quarantine_logger_preserves_raw_source_record(tmp_path: Path) -> None:
    logger = StructuredLogger(tmp_path)
    payload = _fill_payload(
        instrument="NQ",
        local_symbol="NQU6",
        con_id=770561204,
        exec_id="0000e1a7.6a72d50c.01.01",
        lifecycle_id="bridge_fill_NQ|1m|2026-07-27T22:11:00Z|SELL_TO_OPEN",
        fill_price="4073.6",
        limit_price="29579",
    )
    validation = validate_filled_bridge_source_integrity(payload)
    quarantine_payload = {
        **payload,
        "classification": "FILLED_BRIDGE_RESULT_QUARANTINED_SOURCE_INTEGRITY",
        "source_integrity": validation,
        "raw_record": dict(payload),
    }

    logger.log_filled_bridge_result_quarantine(quarantine_payload)
    logger.write_filled_bridge_result_quarantine_state(quarantine_payload)

    rows = [json.loads(line) for line in (tmp_path / "filled_bridge_result_quarantine.jsonl").read_text().splitlines()]
    latest = json.loads((tmp_path / "filled_bridge_result_quarantine_latest.json").read_text())
    assert rows[0]["raw_record"]["exec_id"] == payload["exec_id"]
    assert rows[0]["source_integrity"]["quarantine_required"] is True
    assert latest["classification"] == "FILLED_BRIDGE_RESULT_QUARANTINED_SOURCE_INTEGRITY"


def _fill_payload(
    *,
    instrument: str,
    local_symbol: str,
    con_id: int,
    exec_id: str,
    lifecycle_id: str,
    fill_price: str,
    intent_type: str = "BUY_TO_OPEN",
    limit_price: str | None = None,
    broker_order_id: str = "1",
    perm_id: str = "1001",
    contract: dict[str, object] | None = None,
    partial_fill_sequence: int | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "strategy_managed_filled_bridge_result_v1",
        "artifact_type": "filled_bridge_result",
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "instrument": instrument,
        "symbol": instrument,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "lifecycle_id": lifecycle_id,
        "source_trade_id": f"trade_{lifecycle_id.replace('|', '_').replace(':', '_')}",
        "order_intent_id": lifecycle_id.removeprefix("bridge_fill_"),
        "intent_type": intent_type,
        "fill_price": fill_price,
        "fill_timestamp": "2026-07-27T23:39:13.502892+00:00",
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "exec_id": exec_id,
        "contract": contract
        or {
            "symbol": instrument,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "multiplier": "1",
        },
        "source_provenance": {
            "producer_module": "mgc_v05l.strategy.strategy_engine",
            "persistence_function": "_persist_filled_bridge_result",
        },
    }
    if limit_price is not None:
        payload["limit_price"] = limit_price
    if partial_fill_sequence is not None:
        payload["partial_fill_sequence"] = partial_fill_sequence
    return payload
