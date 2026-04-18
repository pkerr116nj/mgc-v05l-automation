"""IBKR transport/client boundary scaffolding."""

from __future__ import annotations

from datetime import datetime, timezone

from .ibkr_models import (
    IbkrBalanceRecord,
    IbkrCompletedOrderRecord,
    IbkrConnectionState,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrRawEvent,
    IbkrRequestRecord,
)
from .ibkr_session import IbkrSession


class IbkrClient:
    """Thin orchestrator around the IBKR session.

    Real EClient/EWrapper wiring belongs here in a later pass. For now this
    class defines the narrow surface we expect the adapter/provider layers to
    build against once funded API access is available.
    """

    def __init__(self, session: IbkrSession) -> None:
        self._session = session
        self._events: list[IbkrRawEvent] = []
        self._request_log: list[IbkrRequestRecord] = []
        self._balances: list[IbkrBalanceRecord] = []
        self._positions: list[IbkrPositionRecord] = []
        self._open_orders: list[IbkrOpenOrderRecord] = []
        self._completed_orders: list[IbkrCompletedOrderRecord] = []
        self._executions: list[IbkrExecutionRecord] = []

    @property
    def session(self) -> IbkrSession:
        return self._session

    def build_read_only_callback_adapter(self):  # type: ignore[no-untyped-def]
        from .ibkr_callback_adapter import IbkrReadOnlyCallbackAdapter

        return IbkrReadOnlyCallbackAdapter(self)

    def connection_state(self) -> IbkrConnectionState:
        return self._session.state

    def connect(self) -> None:
        raise NotImplementedError("IBKR transport wiring is deferred until funded API access is available.")

    def disconnect(self) -> None:
        raise NotImplementedError("IBKR transport wiring is deferred until funded API access is available.")

    def request_managed_accounts(self) -> IbkrRequestRecord:
        return self._record_request("managed_accounts")

    def request_open_orders(self) -> IbkrRequestRecord:
        return self._record_request("open_orders")

    def request_completed_orders(self) -> IbkrRequestRecord:
        return self._record_request("completed_orders")

    def request_executions(self) -> IbkrRequestRecord:
        return self._record_request("executions")

    def request_balances(self) -> IbkrRequestRecord:
        return self._record_request("balances")

    def request_positions(self) -> IbkrRequestRecord:
        return self._record_request("positions")

    def request_log(self) -> tuple[IbkrRequestRecord, ...]:
        return tuple(self._request_log)

    def record_event(self, event_type: str, *, payload: dict[str, object] | None = None, occurred_at: datetime | None = None) -> None:
        self._events.append(
            IbkrRawEvent(
                event_type=event_type,
                occurred_at=occurred_at or datetime.now(timezone.utc),
                payload=dict(payload or {}),
            )
        )

    def drain_events(self) -> tuple[IbkrRawEvent, ...]:
        rows = tuple(self._events)
        self._events.clear()
        return rows

    def record_managed_accounts(self, managed_accounts: tuple[str, ...], *, occurred_at: datetime | None = None) -> None:
        self._session.mark_connected(managed_accounts=managed_accounts, connected_at=occurred_at)
        self.record_event(
            "managed_accounts",
            payload={"managed_accounts": list(managed_accounts)},
            occurred_at=occurred_at,
        )

    def replace_balances(self, rows: tuple[IbkrBalanceRecord, ...], *, occurred_at: datetime | None = None) -> None:
        self._balances = list(rows)
        self.record_event("balances", payload={"row_count": len(rows)}, occurred_at=occurred_at)

    def replace_positions(self, rows: tuple[IbkrPositionRecord, ...], *, occurred_at: datetime | None = None) -> None:
        self._positions = list(rows)
        self.record_event("positions", payload={"row_count": len(rows)}, occurred_at=occurred_at)

    def replace_open_orders(self, rows: tuple[IbkrOpenOrderRecord, ...], *, occurred_at: datetime | None = None) -> None:
        self._open_orders = list(rows)
        self.record_event("open_orders", payload={"row_count": len(rows)}, occurred_at=occurred_at)

    def replace_completed_orders(
        self,
        rows: tuple[IbkrCompletedOrderRecord, ...],
        *,
        occurred_at: datetime | None = None,
    ) -> None:
        self._completed_orders = list(rows)
        self.record_event("completed_orders", payload={"row_count": len(rows)}, occurred_at=occurred_at)

    def replace_executions(self, rows: tuple[IbkrExecutionRecord, ...], *, occurred_at: datetime | None = None) -> None:
        self._executions = list(rows)
        self.record_event("executions", payload={"row_count": len(rows)}, occurred_at=occurred_at)

    def balances(self) -> tuple[IbkrBalanceRecord, ...]:
        return tuple(self._balances)

    def positions(self) -> tuple[IbkrPositionRecord, ...]:
        return tuple(self._positions)

    def open_orders(self) -> tuple[IbkrOpenOrderRecord, ...]:
        return tuple(self._open_orders)

    def completed_orders(self) -> tuple[IbkrCompletedOrderRecord, ...]:
        return tuple(self._completed_orders)

    def executions(self) -> tuple[IbkrExecutionRecord, ...]:
        return tuple(self._executions)

    def _record_request(self, request_type: str, *, details: dict[str, object] | None = None) -> IbkrRequestRecord:
        request = IbkrRequestRecord(
            request_type=request_type,
            requested_at=datetime.now(timezone.utc),
            details=dict(details or {}),
        )
        self._request_log.append(request)
        return request
