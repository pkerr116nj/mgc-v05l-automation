from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core import track_b_managed_close_terminal_publisher as publisher


NOW = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


def test_broker_position_flat_and_order_gone_emits_broker_effect_observed_flat(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[])
    _write_open_orders(tmp_path, orders=[])
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    summary = _publish(tmp_path)

    events = _read_jsonl(tmp_path / "terminal.jsonl")
    assert summary["terminal_event_count"] == 1
    assert events[0]["classification"] == publisher.BROKER_EFFECT_OBSERVED_FLAT
    assert events[0]["close_order_id"] == "391"
    assert events[0]["local_symbol"] == "ZNU6"
    assert events[0]["pre_close_signed_qty"] == "1"
    assert events[0]["post_close_signed_qty"] == "0"
    assert events[0]["broker_mutation_allowed"] is False
    assert events[0]["entry_gating"] is False
    assert events[0]["exit_gating"] is False
    assert events[0]["profile_wide_authority"] is False


def test_broker_position_reduced_emits_broker_effect_observed_reduced(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[_position(qty="1")])
    _write_open_orders(tmp_path, orders=[])
    _write_actuator(tmp_path, order_id="392", action="SELL", broker_quantity="2", quantity="1")

    summary = _publish(tmp_path)

    events = _read_jsonl(tmp_path / "terminal.jsonl")
    assert summary["terminal_event_count"] == 1
    assert events[0]["classification"] == publisher.BROKER_EFFECT_OBSERVED_REDUCED
    assert events[0]["pre_close_signed_qty"] == "2"
    assert events[0]["post_close_signed_qty"] == "1"


def test_order_still_open_emits_diagnostic_only(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[_position(qty="1")])
    _write_open_orders(tmp_path, orders=[_open_order(order_id="391", action="SELL", quantity="1")])
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    summary = _publish(tmp_path)

    assert summary["terminal_event_count"] == 0
    assert not (tmp_path / "terminal.jsonl").exists()
    assert summary["diagnostics"][0]["classification"] == publisher.ORDER_NOT_TERMINAL_STILL_OPEN
    assert summary["diagnostics"][0]["diagnostic_only"] is True
    assert summary["diagnostics"][0]["does_not_block_trading"] is True


def test_incomplete_broker_truth_emits_diagnostic_only(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[], complete=False)
    _write_open_orders(tmp_path, orders=[])
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    summary = _publish(tmp_path)

    assert summary["terminal_event_count"] == 0
    assert summary["diagnostics"][0]["classification"] == publisher.BROKER_TRUTH_INCOMPLETE


def test_unknown_orders_present_emits_diagnostic_only(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[])
    _write_open_orders(
        tmp_path,
        orders=[],
        extra={"unknown_open_order_count": 1, "unknown_open_orders": [{"order_id": "mystery"}]},
    )
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    summary = _publish(tmp_path)

    assert summary["terminal_event_count"] == 0
    assert summary["diagnostics"][0]["classification"] == publisher.UNKNOWN_ORDERS_PRESENT


def test_identity_mismatch_emits_diagnostic_only(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[_position(qty="1")])
    _write_open_orders(tmp_path, orders=[_open_order(order_id="391", action="BUY", quantity="1")])
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    summary = _publish(tmp_path)

    assert summary["terminal_event_count"] == 0
    assert summary["diagnostics"][0]["classification"] == publisher.IDENTITY_MISMATCH
    assert "action_mismatch" in summary["diagnostics"][0]["resolution"]["mismatched_open_orders"][0]["blockers"]


def test_terminal_event_publishing_is_idempotent(tmp_path: Path) -> None:
    _write_positions(tmp_path, positions=[])
    _write_open_orders(tmp_path, orders=[])
    _write_actuator(tmp_path, order_id="391", action="SELL", broker_quantity="1")

    first = _publish(tmp_path)
    second = _publish(tmp_path)

    events = _read_jsonl(tmp_path / "terminal.jsonl")
    assert first["terminal_event_count"] == 1
    assert second["terminal_event_count"] == 0
    assert second["duplicate_count"] == 1
    assert len(events) == 1


def test_publisher_has_no_mutation_capable_broker_or_runtime_imports() -> None:
    source = inspect.getsource(publisher)
    forbidden_imports = (
        "ib_insync",
        "ibapi",
        "socket",
        "subprocess",
        "ibkr_broker_truth_refresher",
        "ibkr_paper_strategy_bridge",
        "track_b_managed_exit_actuator",
        "track_b_managed_exit_service",
        "track_b_order_adjustment_planner",
        "track_b_managed_order_modify_in_place",
        "track_b_paper_minimal_startup",
        "track_b_runtime_safe_state_envelope",
        "track_b_broker_position_guardian",
    )
    for token in forbidden_imports:
        assert token not in source


def _publish(tmp_path: Path) -> dict:
    return publisher.publish_managed_close_terminal_events(
        config=publisher.ManagedCloseTerminalPublisherConfig(
            repo_root=tmp_path,
            broker_positions_snapshot_path=Path("positions.json"),
            broker_open_orders_snapshot_path=Path("open_orders.json"),
            managed_exit_actuator_path=Path("actuator.json"),
            trade_registry_events_path=Path("trade_events.jsonl"),
            terminal_events_path=Path("terminal.jsonl"),
            summary_path=Path("summary.json"),
            order_diagnostic_root=Path("orders"),
        ),
        now=NOW,
    )


def _write_positions(tmp_path: Path, *, positions: list[dict], complete: bool = True) -> None:
    _write_json(
        tmp_path / "positions.json",
        {
            "generated_at": NOW.isoformat(),
            "read_only": True,
            "positions_complete": complete,
            "positions": positions,
        },
    )


def _write_open_orders(tmp_path: Path, *, orders: list[dict], extra: dict | None = None) -> None:
    payload = {
        "generated_at": NOW.isoformat(),
        "read_only": True,
        "open_orders_complete": True,
        "open_orders": orders,
    }
    payload.update(extra or {})
    _write_json(tmp_path / "open_orders.json", payload)


def _write_actuator(
    tmp_path: Path,
    *,
    order_id: str,
    action: str,
    broker_quantity: str,
    quantity: str = "1",
) -> None:
    _write_json(
        tmp_path / "actuator.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING",
            "attempted_closes": [
                {
                    "classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
                    "order_id": order_id,
                    "close_candidate": {
                        "account_id": "DUM882026",
                        "symbol": "ZN",
                        "local_symbol": "ZNU6",
                        "con_id": 840227361,
                        "expiry": "20260921",
                        "action": action,
                        "quantity": quantity,
                        "broker_quantity": broker_quantity,
                        "lane_id": "zn_us_active_participation_long",
                        "strategy_id": "zn_us_active_participation_long",
                        "trade_id": "trade_zn_1",
                        "lifecycle_id": "life_zn_1",
                    },
                    "close_submit_attempt": {
                        "broker_order_id": order_id,
                        "client_id": "17086",
                        "broker_order": {
                            "status": "PreSubmitted",
                            "action": action,
                            "quantity": quantity,
                        },
                    },
                }
            ],
        },
    )


def _position(*, qty: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 840227361,
        "expiry": "20260921",
        "position": qty,
    }


def _open_order(*, order_id: str, action: str, quantity: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 840227361,
        "expiry": "20260921",
        "broker_order_id": order_id,
        "perm_id": "865990119",
        "client_id": "17086",
        "action": action,
        "quantity": quantity,
        "status": "Submitted",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

