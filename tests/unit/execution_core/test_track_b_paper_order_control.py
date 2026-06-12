from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_order_control import (
    TrackBPaperOrderRecord,
    append_paper_order_control_record,
    exact_cancel_owned_order,
    exact_cancel_owned_order_with_fallbacks,
    exact_modify_owned_order,
    load_paper_order_control_records,
    resolve_owned_working_order,
)


class FakeOrderControlTransport:
    def __init__(self) -> None:
        self.cancelled: list[dict[str, object]] = []
        self.modified: list[dict[str, object]] = []

    def cancel_order(self, *, order_id: int, client_id: int | None = None, perm_id: int | None = None) -> None:
        self.cancelled.append({"order_id": order_id, "client_id": client_id, "perm_id": perm_id})

    def modify_order(
        self,
        *,
        order_id: int,
        limit_price: float,
        client_id: int | None = None,
        perm_id: int | None = None,
    ) -> None:
        self.modified.append(
            {
                "order_id": order_id,
                "client_id": client_id,
                "perm_id": perm_id,
                "limit_price": limit_price,
            }
        )


def test_owned_working_order_can_be_modified_in_place() -> None:
    transport = FakeOrderControlTransport()

    result = exact_modify_owned_order(
        transport=transport,
        request={"perm_id": 1871421122, "account_id": "DUM882026"},
        broker_open_orders=[_open_order()],
        ownership_records=[_owned_order()],
        new_limit_price=29525.0,
    )

    assert result["classification"] == "EXACT_MODIFY_REQUESTED"
    assert result["match_method"] == "perm_id"
    assert transport.modified == [{"order_id": 4, "client_id": 11107, "perm_id": 1871421122, "limit_price": 29525.0}]


def test_owned_working_order_can_be_exact_cancelled() -> None:
    transport = FakeOrderControlTransport()

    result = exact_cancel_owned_order(
        transport=transport,
        request={"order_id": 4, "client_id": 11107, "account_id": "DUM882026"},
        broker_open_orders=[_open_order()],
        ownership_records=[_owned_order()],
        confirmation_open_orders=[],
    )

    assert result["classification"] == "EXACT_CANCEL_REQUESTED"
    assert result["confirmed"] is True
    assert result["match_method"] == "order_id_client_id"
    assert transport.cancelled == [{"order_id": 4, "client_id": 11107, "perm_id": 1871421122}]


def test_order_survives_restart_and_resolves_by_perm_id(tmp_path: Path) -> None:
    jsonl = tmp_path / "orders.jsonl"
    latest = tmp_path / "latest.json"
    append_paper_order_control_record(_record(), jsonl_path=jsonl, latest_path=latest)

    records = load_paper_order_control_records(jsonl)
    resolution = resolve_owned_working_order(
        request={"perm_id": 1871421122},
        broker_open_orders=[_open_order(order_id=99, client_id=12345)],
        ownership_records=records,
    )

    assert resolution.allowed
    assert resolution.match_method == "perm_id"
    assert resolution.order["order_id"] == "99"


def test_wrong_account_order_is_not_modified_or_cancelled() -> None:
    transport = FakeOrderControlTransport()

    result = exact_cancel_owned_order(
        transport=transport,
        request={"perm_id": 1871421122, "account_id": "DU999999"},
        broker_open_orders=[_open_order(account_id="DU999999")],
        ownership_records=[_owned_order()],
    )

    assert result["classification"] == "EXACT_CANCEL_BLOCKED"
    assert result["reason"] == "wrong_account"
    assert transport.cancelled == []


def test_unknown_unowned_order_is_not_touched() -> None:
    transport = FakeOrderControlTransport()

    result = exact_modify_owned_order(
        transport=transport,
        request={"perm_id": 1871421122},
        broker_open_orders=[_open_order()],
        ownership_records=[],
        new_limit_price=29525.0,
    )

    assert result["classification"] == "EXACT_MODIFY_BLOCKED"
    assert result["reason"] == "open_order_is_not_owned_by_track_b"
    assert transport.modified == []


def test_failed_exact_cancel_emits_operator_required_artifact(tmp_path: Path) -> None:
    transport = FakeOrderControlTransport()
    artifact = tmp_path / "operator_cancel_required.json"

    result = exact_cancel_owned_order(
        transport=transport,
        request={"perm_id": 1871421122},
        broker_open_orders=[_open_order()],
        ownership_records=[_owned_order()],
        confirmation_open_orders=[_open_order()],
        operator_required_path=artifact,
    )

    assert result["classification"] == "OPERATOR_CANCEL_REQUIRED_EXACT_ORDER"
    payload = json.loads(artifact.read_text())
    assert payload["classification"] == "OPERATOR_CANCEL_REQUIRED_EXACT_ORDER"
    assert payload["target_order"]["perm_id"] == "1871421122"
    assert payload["broad_cancel_allowed"] is False


def test_bind_reconnect_fallback_path_can_confirm_exact_cancel() -> None:
    originating = FakeOrderControlTransport()
    bind_reconnect = FakeOrderControlTransport()

    def confirmation(path_name: str) -> list[dict[str, object]]:
        if path_name == "originating_client":
            return [_open_order()]
        return []

    result = exact_cancel_owned_order_with_fallbacks(
        cancel_paths=[
            {"name": "originating_client", "transport": originating},
            {"name": "client0_bind_reconnect", "transport": bind_reconnect},
        ],
        request={"perm_id": 1871421122},
        broker_open_orders=[_open_order()],
        ownership_records=[_owned_order()],
        confirmation_supplier=confirmation,
    )

    assert result["classification"] == "EXACT_CANCEL_CONFIRMED"
    assert result["cancel_path"] == "client0_bind_reconnect"
    assert originating.cancelled == [{"order_id": 4, "client_id": 11107, "perm_id": 1871421122}]
    assert bind_reconnect.cancelled == [{"order_id": 4, "client_id": 11107, "perm_id": 1871421122}]


def _record() -> TrackBPaperOrderRecord:
    return TrackBPaperOrderRecord(
        order_id=4,
        perm_id=1871421122,
        client_id=11107,
        account_id="DUM882026",
        con_id=793356225,
        local_symbol="MNQU6",
        action="BUY",
        quantity=1,
        order_type="LMT",
        limit_price=29462.75,
        order_ref="TRACK_B_API_LIFECYCLE_TEST_20260612T084320Z_MNQ_OID4_BUY_REST_CANCEL",
        originating_component="unit_test",
        lane_id="mnq_london_open_active_participation_long",
        timestamp=datetime(2026, 6, 12, 8, 43, 20, tzinfo=UTC),
        status="Submitted",
    )


def _owned_order(**overrides: object) -> dict[str, object]:
    payload = _record().to_payload()
    payload.update(overrides)
    return payload


def _open_order(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "broker_order_id": 4,
        "perm_id": 1871421122,
        "client_id": 11107,
        "account_id": "DUM882026",
        "con_id": 793356225,
        "local_symbol": "MNQU6",
        "action": "BUY",
        "quantity": "1.0",
        "order_type": "LMT",
        "limit_price": "29462.75",
        "order_ref": "TRACK_B_API_LIFECYCLE_TEST_20260612T084320Z_MNQ_OID4_BUY_REST_CANCEL",
        "status": "Submitted",
    }
    payload.update(overrides)
    return payload
