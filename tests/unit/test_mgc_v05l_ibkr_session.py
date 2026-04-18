from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.brokers.ibkr import IbkrSession, build_default_ibkr_order_id_policy


def test_ibkr_session_tracks_connection_and_heartbeat() -> None:
    session = IbkrSession(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="DU1234567",
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
    )
    connected_at = datetime(2026, 4, 12, 12, 0, tzinfo=timezone.utc)
    heartbeat_at = datetime(2026, 4, 12, 12, 1, tzinfo=timezone.utc)

    session.mark_connected(managed_accounts=("DU1234567",), connected_at=connected_at)
    session.record_heartbeat(occurred_at=heartbeat_at)

    assert session.state.connected is True
    assert session.state.managed_accounts == ("DU1234567",)
    assert session.state.connected_at == connected_at
    assert session.state.last_heartbeat_at == heartbeat_at


def test_ibkr_session_seeds_and_allocates_order_ids() -> None:
    session = IbkrSession(
        host="127.0.0.1",
        port=4002,
        client_id=9,
        account_id="DU7654321",
        gateway_mode="paper",
        read_only=False,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=9, live_orders_enabled=True),
    )

    session.seed_next_valid_order_id(4100)

    assert session.state.next_valid_order_id == 4100
    assert session.allocate_order_id() == 4100
    assert session.state.next_valid_order_id == 4101
