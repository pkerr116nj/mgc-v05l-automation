"""Dedicated SQLite persistence for Schwab production-link state."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from .models import (
    BrokerAccountIdentity,
    BrokerBalanceSnapshot,
    BrokerOrderEvent,
    BrokerOrderRecord,
    BrokerPositionSnapshot,
    BrokerQuoteSnapshot,
    BrokerReconciliationRecord,
)

_SCHEMA = """
create table if not exists broker_accounts (
  account_hash text primary key,
  account_number text,
  display_name text not null,
  account_type text,
  selected integer not null default 0,
  source text not null,
  updated_at text not null,
  raw_json text not null
);

create table if not exists broker_account_balances (
  account_hash text primary key,
  currency text,
  liquidation_value text,
  buying_power text,
  available_funds text,
  cash_balance text,
  long_market_value text,
  short_market_value text,
  day_trading_buying_power text,
  maintenance_requirement text,
  margin_balance text,
  fetched_at text not null,
  raw_json text not null
);

create table if not exists broker_positions (
  position_key text primary key,
  account_hash text not null,
  symbol text not null,
  description text,
  asset_class text not null,
  quantity text not null,
  side text not null,
  average_cost text,
  mark_price text,
  market_value text,
  current_day_pnl text,
  open_pnl text,
  ytd_pnl text,
  margin_impact text,
  broker_position_id text,
  fetched_at text not null,
  raw_json text not null
);

create table if not exists broker_quotes (
  account_hash text not null,
  symbol text not null,
  external_symbol text not null,
  bid_price text,
  ask_price text,
  last_price text,
  mark_price text,
  close_price text,
  net_change text,
  net_percent_change text,
  delayed integer,
  quote_time text,
  fetched_at text not null,
  source text not null,
  raw_json text not null,
  primary key (account_hash, symbol)
);

create table if not exists broker_orders (
  broker_order_id text primary key,
  account_hash text not null,
  client_order_id text,
  symbol text not null,
  description text,
  asset_class text not null,
  instruction text not null,
  quantity text not null,
  filled_quantity text,
  order_type text not null,
  duration text,
  session text,
  status text not null,
  entered_at text,
  closed_at text,
  updated_at text not null,
  limit_price text,
  stop_price text,
  source text not null,
  raw_json text not null
);

create table if not exists broker_order_events (
  event_id integer primary key autoincrement,
  account_hash text not null,
  broker_order_id text,
  client_order_id text,
  event_type text not null,
  status text,
  occurred_at text not null,
  message text,
  request_json text,
  response_json text,
  source text not null
);

create table if not exists broker_reconciliation_events (
  event_id integer primary key autoincrement,
  account_hash text,
  classification text not null,
  status text not null,
  detail text not null,
  mismatch_count integer not null,
  created_at text not null,
  payload_json text not null
);

create table if not exists broker_manual_validation_events (
  event_id integer primary key autoincrement,
  scenario_type text not null,
  occurred_at text not null,
  payload_json text not null
);

create table if not exists broker_runtime_state (
  state_key text primary key,
  state_value_json text not null,
  updated_at text not null
);
"""


class ProductionLinkStore:
    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @property
    def database_path(self) -> Path:
        return self._database_path

    def save_accounts(self, accounts: Iterable[BrokerAccountIdentity], *, selected_account_hash: str | None) -> None:
        rows = list(accounts)
        with self._connect() as connection:
            if selected_account_hash:
                connection.execute("update broker_accounts set selected = 0")
            for account in rows:
                connection.execute(
                    """
                    insert into broker_accounts (
                      account_hash, account_number, display_name, account_type, selected, source, updated_at, raw_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(account_hash) do update set
                      account_number=excluded.account_number,
                      display_name=excluded.display_name,
                      account_type=excluded.account_type,
                      selected=excluded.selected,
                      source=excluded.source,
                      updated_at=excluded.updated_at,
                      raw_json=excluded.raw_json
                    """,
                    (
                        account.account_hash,
                        account.account_number,
                        account.display_name,
                        account.account_type,
                        1 if account.account_hash == selected_account_hash else 0,
                        account.source,
                        account.updated_at.isoformat(),
                        _json_dumps(account.raw_payload),
                    ),
                )

    def save_portfolio_snapshot(
        self,
        *,
        account_hash: str,
        balances: BrokerBalanceSnapshot | None,
        positions: Iterable[BrokerPositionSnapshot],
    ) -> None:
        position_rows = list(positions)
        with self._connect() as connection:
            if balances is not None:
                connection.execute(
                    """
                    insert into broker_account_balances (
                      account_hash, currency, liquidation_value, buying_power, available_funds, cash_balance,
                      long_market_value, short_market_value, day_trading_buying_power, maintenance_requirement,
                      margin_balance, fetched_at, raw_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(account_hash) do update set
                      currency=excluded.currency,
                      liquidation_value=excluded.liquidation_value,
                      buying_power=excluded.buying_power,
                      available_funds=excluded.available_funds,
                      cash_balance=excluded.cash_balance,
                      long_market_value=excluded.long_market_value,
                      short_market_value=excluded.short_market_value,
                      day_trading_buying_power=excluded.day_trading_buying_power,
                      maintenance_requirement=excluded.maintenance_requirement,
                      margin_balance=excluded.margin_balance,
                      fetched_at=excluded.fetched_at,
                      raw_json=excluded.raw_json
                    """,
                    (
                        balances.account_hash,
                        balances.currency,
                        _decimal_text(balances.liquidation_value),
                        _decimal_text(balances.buying_power),
                        _decimal_text(balances.available_funds),
                        _decimal_text(balances.cash_balance),
                        _decimal_text(balances.long_market_value),
                        _decimal_text(balances.short_market_value),
                        _decimal_text(balances.day_trading_buying_power),
                        _decimal_text(balances.maintenance_requirement),
                        _decimal_text(balances.margin_balance),
                        balances.fetched_at.isoformat(),
                        _json_dumps(balances.raw_payload),
                    ),
                )
            connection.execute("delete from broker_positions where account_hash = ?", (account_hash,))
            for position in position_rows:
                connection.execute(
                    """
                    insert into broker_positions (
                      position_key, account_hash, symbol, description, asset_class, quantity, side, average_cost, mark_price,
                      market_value, current_day_pnl, open_pnl, ytd_pnl, margin_impact, broker_position_id, fetched_at, raw_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        position.position_key,
                        position.account_hash,
                        position.symbol,
                        position.description,
                        position.asset_class,
                        _decimal_text(position.quantity),
                        position.side,
                        _decimal_text(position.average_cost),
                        _decimal_text(position.mark_price),
                        _decimal_text(position.market_value),
                        _decimal_text(position.current_day_pnl),
                        _decimal_text(position.open_pnl),
                        _decimal_text(position.ytd_pnl),
                        _decimal_text(position.margin_impact),
                        position.broker_position_id,
                        position.fetched_at.isoformat(),
                        _json_dumps(position.raw_payload),
                    ),
                )

    def save_quote_snapshot(self, *, account_hash: str, quotes: Iterable[BrokerQuoteSnapshot]) -> None:
        quote_rows = list(quotes)
        with self._connect() as connection:
            connection.execute("delete from broker_quotes where account_hash = ?", (account_hash,))
            for quote in quote_rows:
                connection.execute(
                    """
                    insert into broker_quotes (
                      account_hash, symbol, external_symbol, bid_price, ask_price, last_price, mark_price, close_price,
                      net_change, net_percent_change, delayed, quote_time, fetched_at, source, raw_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        quote.account_hash,
                        quote.symbol,
                        quote.external_symbol,
                        _decimal_text(quote.bid_price),
                        _decimal_text(quote.ask_price),
                        _decimal_text(quote.last_price),
                        _decimal_text(quote.mark_price),
                        _decimal_text(quote.close_price),
                        _decimal_text(quote.net_change),
                        _decimal_text(quote.net_percent_change),
                        None if quote.delayed is None else (1 if quote.delayed else 0),
                        quote.quote_time.isoformat() if quote.quote_time else None,
                        quote.fetched_at.isoformat(),
                        quote.source,
                        _json_dumps(quote.raw_payload),
                    ),
                )

    def upsert_orders(self, orders: Iterable[BrokerOrderRecord], *, event_source: str) -> None:
        order_rows = list(orders)
        if not order_rows:
            return
        with self._connect() as connection:
            existing_status = {
                str(row["broker_order_id"]): str(row["status"])
                for row in connection.execute("select broker_order_id, status from broker_orders").fetchall()
            }
            for order in order_rows:
                connection.execute(
                    """
                    insert into broker_orders (
                      broker_order_id, account_hash, client_order_id, symbol, description, asset_class, instruction,
                      quantity, filled_quantity, order_type, duration, session, status, entered_at, closed_at,
                      updated_at, limit_price, stop_price, source, raw_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(broker_order_id) do update set
                      account_hash=excluded.account_hash,
                      client_order_id=excluded.client_order_id,
                      symbol=excluded.symbol,
                      description=excluded.description,
                      asset_class=excluded.asset_class,
                      instruction=excluded.instruction,
                      quantity=excluded.quantity,
                      filled_quantity=excluded.filled_quantity,
                      order_type=excluded.order_type,
                      duration=excluded.duration,
                      session=excluded.session,
                      status=excluded.status,
                      entered_at=excluded.entered_at,
                      closed_at=excluded.closed_at,
                      updated_at=excluded.updated_at,
                      limit_price=excluded.limit_price,
                      stop_price=excluded.stop_price,
                      source=excluded.source,
                      raw_json=excluded.raw_json
                    """,
                    (
                        order.broker_order_id,
                        order.account_hash,
                        order.client_order_id,
                        order.symbol,
                        order.description,
                        order.asset_class,
                        order.instruction,
                        _decimal_text(order.quantity),
                        _decimal_text(order.filled_quantity),
                        order.order_type,
                        order.duration,
                        order.session,
                        order.status,
                        order.entered_at.isoformat() if order.entered_at else None,
                        order.closed_at.isoformat() if order.closed_at else None,
                        order.updated_at.isoformat(),
                        _decimal_text(order.limit_price),
                        _decimal_text(order.stop_price),
                        order.source,
                        _json_dumps(order.raw_payload),
                    ),
                )
                previous_status = existing_status.get(order.broker_order_id)
                if previous_status != order.status:
                    self._insert_order_event(
                        connection,
                        BrokerOrderEvent(
                            account_hash=order.account_hash,
                            broker_order_id=order.broker_order_id,
                            client_order_id=order.client_order_id,
                            event_type="status_sync" if previous_status else "order_seen",
                            status=order.status,
                            occurred_at=order.updated_at,
                            message=f"{event_source}: {order.status}",
                            request_payload=None,
                            response_payload=order.raw_payload,
                            source=event_source,
                        ),
                    )

    def record_order_event(self, event: BrokerOrderEvent) -> None:
        with self._connect() as connection:
            self._insert_order_event(connection, event)

    def retire_absent_live_open_orders(
        self,
        *,
        account_hash: str,
        live_open_order_ids: Iterable[str],
        occurred_at: datetime,
        closed_status: str,
    ) -> list[str]:
        live_ids = {str(item).strip() for item in live_open_order_ids if str(item).strip()}
        retired_ids: list[str] = []
        with self._connect() as connection:
            rows = connection.execute(
                """
                select broker_order_id, client_order_id, symbol, status, raw_json
                from broker_orders
                where account_hash = ?
                  and source = 'schwab_live'
                  and upper(status) not in ('FILLED', 'CANCELED', 'CANCELLED', 'REJECTED', 'EXPIRED', 'NOT_OPEN_ON_BROKER')
                """,
                (account_hash,),
            ).fetchall()
            for row in rows:
                broker_order_id = str(row["broker_order_id"] or "").strip()
                if not broker_order_id or broker_order_id in live_ids:
                    continue
                connection.execute(
                    """
                    update broker_orders
                    set status = ?, closed_at = ?, updated_at = ?
                    where broker_order_id = ?
                    """,
                    (
                        closed_status,
                        occurred_at.isoformat(),
                        occurred_at.isoformat(),
                        broker_order_id,
                    ),
                )
                self._insert_order_event(
                    connection,
                    BrokerOrderEvent(
                        account_hash=account_hash,
                        broker_order_id=broker_order_id,
                        client_order_id=str(row["client_order_id"] or "").strip() or None,
                        event_type="retired_by_live_sync",
                        status=closed_status,
                        occurred_at=occurred_at,
                        message="Latest live Schwab refresh no longer showed this order as open.",
                        request_payload=None,
                        response_payload=_json_loads(str(row["raw_json"])) if row["raw_json"] else None,
                        source="schwab_sync",
                    ),
                )
                retired_ids.append(broker_order_id)
        return retired_ids

    def resolve_order_terminal_state(
        self,
        *,
        broker_order_id: str,
        account_hash: str,
        status: str,
        occurred_at: datetime,
        source: str,
        message: str,
        response_payload: dict[str, Any] | None,
    ) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                select client_order_id, raw_json
                from broker_orders
                where broker_order_id = ?
                """,
                (broker_order_id,),
            ).fetchone()
            if row is None:
                return False
            raw_payload = response_payload
            if raw_payload is None and row["raw_json"]:
                raw_payload = _json_loads(str(row["raw_json"]))
            connection.execute(
                """
                update broker_orders
                set status = ?, closed_at = ?, updated_at = ?, source = ?, raw_json = ?
                where broker_order_id = ?
                """,
                (
                    status,
                    occurred_at.isoformat(),
                    occurred_at.isoformat(),
                    source,
                    _json_dumps(raw_payload or {}),
                    broker_order_id,
                ),
            )
            self._insert_order_event(
                connection,
                BrokerOrderEvent(
                    account_hash=account_hash,
                    broker_order_id=broker_order_id,
                    client_order_id=str(row["client_order_id"] or "").strip() or None,
                    event_type="terminal_resolution",
                    status=status,
                    occurred_at=occurred_at,
                    message=message,
                    request_payload={"broker_order_id": broker_order_id},
                    response_payload=raw_payload,
                    source=source,
                ),
            )
        return True

    def record_reconciliation(self, record: BrokerReconciliationRecord) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into broker_reconciliation_events (
                  account_hash, classification, status, detail, mismatch_count, created_at, payload_json
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.account_hash,
                    record.classification,
                    record.status,
                    record.detail,
                    record.mismatch_count,
                    record.created_at.isoformat(),
                    _json_dumps(record.payload),
                ),
            )
            connection.execute(
                """
                insert into broker_runtime_state (state_key, state_value_json, updated_at)
                values (?, ?, ?)
                on conflict(state_key) do update set
                  state_value_json=excluded.state_value_json,
                  updated_at=excluded.updated_at
                """,
                ("latest_reconciliation", _json_dumps(asdict(record)), record.created_at.isoformat()),
            )

    def record_manual_validation_event(self, *, scenario_type: str, occurred_at: datetime, payload: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into broker_manual_validation_events (
                  scenario_type, occurred_at, payload_json
                ) values (?, ?, ?)
                """,
                (
                    scenario_type,
                    occurred_at.isoformat(),
                    _json_dumps(payload),
                ),
            )

    def save_runtime_state(self, key: str, payload: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                insert into broker_runtime_state (state_key, state_value_json, updated_at)
                values (?, ?, ?)
                on conflict(state_key) do update set
                  state_value_json=excluded.state_value_json,
                  updated_at=excluded.updated_at
                """,
                (key, _json_dumps(payload), datetime.utcnow().isoformat()),
            )

    def load_runtime_state(self, key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "select state_value_json from broker_runtime_state where state_key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return _json_loads(str(row["state_value_json"]))

    def build_snapshot(self) -> dict[str, Any]:
        with self._connect() as connection:
            accounts = [dict(row) for row in connection.execute(
                "select * from broker_accounts order by selected desc, updated_at desc, account_hash asc"
            ).fetchall()]
            balances = {
                str(row["account_hash"]): dict(row)
                for row in connection.execute("select * from broker_account_balances").fetchall()
            }
            positions = [dict(row) for row in connection.execute(
                "select * from broker_positions order by account_hash asc, symbol asc"
            ).fetchall()]
            quotes = [dict(row) for row in connection.execute(
                "select * from broker_quotes order by account_hash asc, symbol asc"
            ).fetchall()]
            open_orders = [dict(row) for row in connection.execute(
                """
                select * from broker_orders
                where upper(status) not in ('FILLED', 'CANCELED', 'CANCELLED', 'REJECTED', 'EXPIRED', 'NOT_OPEN_ON_BROKER')
                order by updated_at desc
                limit 100
                """
            ).fetchall()]
            recent_fills = [dict(row) for row in connection.execute(
                """
                select * from broker_orders
                where upper(status) in ('FILLED', 'PARTIALLY_FILLED')
                order by updated_at desc
                limit 100
                """
            ).fetchall()]
            recent_events = [dict(row) for row in connection.execute(
                "select * from broker_order_events order by occurred_at desc, event_id desc limit 80"
            ).fetchall()]
            latest_reconciliation = connection.execute(
                """
                select account_hash, classification, status, detail, mismatch_count, created_at, payload_json
                from broker_reconciliation_events
                order by created_at desc, event_id desc
                limit 1
                """
            ).fetchone()
            manual_validation_events = [dict(row) for row in connection.execute(
                """
                select scenario_type, occurred_at, payload_json
                from broker_manual_validation_events
                order by occurred_at desc, event_id desc
                limit 40
                """
            ).fetchall()]
            runtime_state = {
                str(row["state_key"]): _json_loads(str(row["state_value_json"]))
                for row in connection.execute("select state_key, state_value_json from broker_runtime_state").fetchall()
            }

        selected_account_hash = next((str(row["account_hash"]) for row in accounts if int(row["selected"] or 0) == 1), None)
        selected_account = next((row for row in accounts if str(row["account_hash"]) == selected_account_hash), None)
        quote_rows = [_quote_row_json(row) for row in quotes]
        quote_lookup = {
            str(row["symbol"]): row
            for row in quote_rows
            if not selected_account_hash or str(row["account_hash"]) == selected_account_hash
        }
        position_rows = [
            _position_row_json(row, quote_lookup.get(str(row["symbol"])))
            for row in positions
            if not selected_account_hash or str(row["account_hash"]) == selected_account_hash
        ]
        return {
            "accounts": {
                "selected_account_hash": selected_account_hash,
                "selected_account_number": selected_account.get("account_number") if selected_account else None,
                "rows": [
                    {
                        "account_hash": str(row["account_hash"]),
                        "account_number": row["account_number"],
                        "display_name": row["display_name"],
                        "account_type": row["account_type"],
                        "selected": bool(row["selected"]),
                        "source": row["source"],
                        "updated_at": row["updated_at"],
                        "balances": _balance_row_json(balances.get(str(row["account_hash"]))),
                    }
                    for row in accounts
                ],
            },
            "portfolio": {
                "positions": position_rows,
                "balances": _balance_row_json(balances.get(selected_account_hash)) if selected_account_hash else None,
                "account_totals": _portfolio_totals(position_rows, balances.get(selected_account_hash)),
            },
            "quotes": {
                "rows": list(quote_lookup.values()),
                "updated_at": max((row.get("fetched_at") for row in quote_lookup.values()), default=None),
                "symbol_count": len(quote_lookup),
            },
            "orders": {
                "open_rows": [_order_row_json(row) for row in open_orders],
                "recent_fill_rows": [_order_row_json(row) for row in recent_fills],
                "recent_events": [_event_row_json(row) for row in recent_events],
            },
            "broker_state_snapshot": _broker_state_snapshot(
                selected_account_hash=selected_account_hash,
                position_rows=position_rows,
                open_orders=[_order_row_json(row) for row in open_orders if not selected_account_hash or str(row["account_hash"]) == selected_account_hash],
                recent_fills=[_order_row_json(row) for row in recent_fills if not selected_account_hash or str(row["account_hash"]) == selected_account_hash],
                balance_row=_balance_row_json(balances.get(selected_account_hash)) if selected_account_hash else None,
                quote_rows=list(quote_lookup.values()),
            ),
            "reconciliation": (
                {
                    "account_hash": latest_reconciliation["account_hash"],
                    "classification": latest_reconciliation["classification"],
                    "status": latest_reconciliation["status"],
                    "detail": latest_reconciliation["detail"],
                    "mismatch_count": int(latest_reconciliation["mismatch_count"]),
                    "created_at": latest_reconciliation["created_at"],
                    "payload": _json_loads(str(latest_reconciliation["payload_json"])),
                }
                if latest_reconciliation is not None
                else None
            ),
            "manual_validation": {
                "recent_events": [
                    {
                        "scenario_type": str(row["scenario_type"]),
                        "occurred_at": str(row["occurred_at"]),
                        "payload": _json_loads(str(row["payload_json"])),
                    }
                    for row in manual_validation_events
                ],
                "latest_event": (
                    {
                        "scenario_type": str(manual_validation_events[0]["scenario_type"]),
                        "occurred_at": str(manual_validation_events[0]["occurred_at"]),
                        "payload": _json_loads(str(manual_validation_events[0]["payload_json"])),
                    }
                    if manual_validation_events
                    else None
                ),
            },
            "runtime_state": runtime_state,
        }

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _insert_order_event(self, connection: sqlite3.Connection, event: BrokerOrderEvent) -> None:
        connection.execute(
            """
            insert into broker_order_events (
              account_hash, broker_order_id, client_order_id, event_type, status, occurred_at, message,
              request_json, response_json, source
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.account_hash,
                event.broker_order_id,
                event.client_order_id,
                event.event_type,
                event.status,
                event.occurred_at.isoformat(),
                event.message,
                _json_dumps(event.request_payload) if event.request_payload is not None else None,
                _json_dumps(event.response_payload) if event.response_payload is not None else None,
                event.source,
            ),
        )


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, default=_json_default, sort_keys=True)


def _json_loads(payload: str) -> dict[str, Any]:
    loaded = json.loads(payload)
    return loaded if isinstance(loaded, dict) else {"value": loaded}


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _balance_row_json(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "currency": row.get("currency"),
        "liquidation_value": row.get("liquidation_value"),
        "buying_power": row.get("buying_power"),
        "available_funds": row.get("available_funds"),
        "cash_balance": row.get("cash_balance"),
        "long_market_value": row.get("long_market_value"),
        "short_market_value": row.get("short_market_value"),
        "day_trading_buying_power": row.get("day_trading_buying_power"),
        "maintenance_requirement": row.get("maintenance_requirement"),
        "margin_balance": row.get("margin_balance"),
        "fetched_at": row.get("fetched_at"),
        "raw_payload": _json_loads(str(row["raw_json"])) if row.get("raw_json") else None,
    }


def _position_row_json(row: dict[str, Any], quote_row: dict[str, Any] | None = None) -> dict[str, Any]:
    quantity = _to_decimal(row.get("quantity")) or Decimal("0")
    side = str(row.get("side") or "LONG").upper()
    signed_quantity = quantity if side != "SHORT" else quantity * Decimal("-1")
    persisted_mark = _to_decimal(row.get("mark_price"))
    persisted_market_value = _to_decimal(row.get("market_value"))
    quote_mark = _to_decimal((quote_row or {}).get("mark_price")) or _to_decimal((quote_row or {}).get("last_price"))
    multiplier = _position_multiplier(quantity=quantity, persisted_mark=persisted_mark, persisted_market_value=persisted_market_value)
    average_cost = _to_decimal(row.get("average_cost"))
    quote_close = _to_decimal((quote_row or {}).get("close_price"))
    quote_change = _to_decimal((quote_row or {}).get("net_change"))
    mark_price = quote_mark if quote_mark is not None else persisted_mark
    market_value = _overlay_market_value(
        mark_price=mark_price,
        quantity=quantity,
        signed_quantity=signed_quantity,
        multiplier=multiplier,
        persisted_market_value=persisted_market_value,
    )
    open_pnl = _overlay_open_pnl(
        mark_price=mark_price,
        average_cost=average_cost,
        quantity=quantity,
        side=side,
        multiplier=multiplier,
        persisted_open_pnl=_to_decimal(row.get("open_pnl")),
    )
    current_day_pnl = _overlay_day_pnl(
        mark_price=mark_price,
        close_price=quote_close,
        net_change=quote_change,
        quantity=quantity,
        side=side,
        multiplier=multiplier,
        persisted_day_pnl=_to_decimal(row.get("current_day_pnl")),
    )
    return {
        "account_hash": row["account_hash"],
        "position_key": row["position_key"],
        "symbol": row["symbol"],
        "description": row["description"],
        "asset_class": row["asset_class"],
        "quantity": row["quantity"],
        "side": row["side"],
        "average_cost": row["average_cost"],
        "mark_price": _decimal_text(mark_price),
        "market_value": _decimal_text(market_value),
        "current_day_pnl": _decimal_text(current_day_pnl),
        "open_pnl": _decimal_text(open_pnl),
        "ytd_pnl": row["ytd_pnl"],
        "margin_impact": row["margin_impact"],
        "broker_position_id": row["broker_position_id"],
        "fetched_at": row["fetched_at"],
        "quote_fetched_at": quote_row.get("fetched_at") if quote_row else None,
        "quote_state": quote_row.get("freshness_state") if quote_row else None,
        "quote": quote_row,
        "raw_payload": _json_loads(str(row["raw_json"])) if row.get("raw_json") else None,
    }


def _quote_row_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "account_hash": row["account_hash"],
        "symbol": row["symbol"],
        "external_symbol": row["external_symbol"],
        "bid_price": row["bid_price"],
        "ask_price": row["ask_price"],
        "last_price": row["last_price"],
        "mark_price": row["mark_price"],
        "close_price": row["close_price"],
        "net_change": row["net_change"],
        "net_percent_change": row["net_percent_change"],
        "delayed": None if row["delayed"] is None else bool(row["delayed"]),
        "quote_time": row["quote_time"],
        "fetched_at": row["fetched_at"],
        "source": row["source"],
        "freshness_state": "DELAYED" if row["delayed"] else "LIVE",
        "raw_payload": _json_loads(str(row["raw_json"])) if row.get("raw_json") else None,
    }


def _order_row_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "broker_order_id": row["broker_order_id"],
        "account_hash": row["account_hash"],
        "client_order_id": row["client_order_id"],
        "symbol": row["symbol"],
        "description": row["description"],
        "asset_class": row["asset_class"],
        "instruction": row["instruction"],
        "quantity": row["quantity"],
        "filled_quantity": row["filled_quantity"],
        "order_type": row["order_type"],
        "duration": row["duration"],
        "session": row["session"],
        "status": row["status"],
        "entered_at": row["entered_at"],
        "closed_at": row["closed_at"],
        "updated_at": row["updated_at"],
        "limit_price": row["limit_price"],
        "stop_price": row["stop_price"],
        "source": row["source"],
        "raw_payload": _json_loads(str(row["raw_json"])) if row.get("raw_json") else None,
    }


def _event_row_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": int(row["event_id"]),
        "account_hash": row["account_hash"],
        "broker_order_id": row["broker_order_id"],
        "client_order_id": row["client_order_id"],
        "event_type": row["event_type"],
        "status": row["status"],
        "occurred_at": row["occurred_at"],
        "message": row["message"],
        "source": row["source"],
        "request": _json_loads(str(row["request_json"])) if row["request_json"] else None,
        "response": _json_loads(str(row["response_json"])) if row["response_json"] else None,
    }


def _portfolio_totals(position_rows: list[dict[str, Any]], balance_row: dict[str, Any] | None) -> dict[str, Any]:
    total_market_value = sum((_to_decimal(row.get("market_value")) or Decimal("0")) for row in position_rows)
    total_current_day_pnl = sum((_to_decimal(row.get("current_day_pnl")) or Decimal("0")) for row in position_rows)
    total_open_pnl = sum((_to_decimal(row.get("open_pnl")) or Decimal("0")) for row in position_rows)
    return {
        "position_count": len(position_rows),
        "total_market_value": str(total_market_value),
        "total_current_day_pnl": str(total_current_day_pnl),
        "total_open_pnl": str(total_open_pnl),
        "cash_balance": balance_row.get("cash_balance") if balance_row else None,
        "buying_power": balance_row.get("buying_power") if balance_row else None,
        "liquidation_value": balance_row.get("liquidation_value") if balance_row else None,
    }


def _position_multiplier(
    *,
    quantity: Decimal,
    persisted_mark: Decimal | None,
    persisted_market_value: Decimal | None,
) -> Decimal:
    if quantity != 0 and persisted_mark not in (None, Decimal("0")) and persisted_market_value is not None:
        denominator = abs(quantity) * abs(persisted_mark)
        if denominator != 0:
            return abs(persisted_market_value) / denominator
    return Decimal("1")


def _overlay_market_value(
    *,
    mark_price: Decimal | None,
    quantity: Decimal,
    signed_quantity: Decimal,
    multiplier: Decimal,
    persisted_market_value: Decimal | None,
) -> Decimal | None:
    if mark_price is None or quantity == 0:
        return persisted_market_value
    sign = Decimal("1") if signed_quantity >= 0 else Decimal("-1")
    return mark_price * abs(quantity) * multiplier * sign


def _overlay_open_pnl(
    *,
    mark_price: Decimal | None,
    average_cost: Decimal | None,
    quantity: Decimal,
    side: str,
    multiplier: Decimal,
    persisted_open_pnl: Decimal | None,
) -> Decimal | None:
    if mark_price is None or average_cost is None or quantity == 0:
        return persisted_open_pnl
    direction = Decimal("1") if side != "SHORT" else Decimal("-1")
    return (mark_price - average_cost) * abs(quantity) * multiplier * direction


def _overlay_day_pnl(
    *,
    mark_price: Decimal | None,
    close_price: Decimal | None,
    net_change: Decimal | None,
    quantity: Decimal,
    side: str,
    multiplier: Decimal,
    persisted_day_pnl: Decimal | None,
) -> Decimal | None:
    if quantity == 0:
        return persisted_day_pnl
    direction = Decimal("1") if side != "SHORT" else Decimal("-1")
    if net_change is not None:
        return net_change * abs(quantity) * multiplier * direction
    if mark_price is not None and close_price is not None:
        return (mark_price - close_price) * abs(quantity) * multiplier * direction
    return persisted_day_pnl


def _broker_state_snapshot(
    *,
    selected_account_hash: str | None,
    position_rows: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    recent_fills: list[dict[str, Any]],
    balance_row: dict[str, Any] | None,
    quote_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    positions_by_symbol = {
        str(row.get("symbol") or ""): {
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "quantity": row.get("quantity"),
            "average_cost": row.get("average_cost"),
            "mark_price": row.get("mark_price"),
            "market_value": row.get("market_value"),
            "open_pnl": row.get("open_pnl"),
            "current_day_pnl": row.get("current_day_pnl"),
            "fetched_at": row.get("fetched_at"),
            "quote_fetched_at": row.get("quote_fetched_at"),
        }
        for row in position_rows
        if str(row.get("symbol") or "").strip()
    }
    open_orders_by_symbol = _orders_by_symbol_summary(open_orders)
    recent_fills_by_symbol = _orders_by_symbol_summary(recent_fills)
    return {
        "selected_account_hash": selected_account_hash,
        "positions_by_symbol": positions_by_symbol,
        "open_orders_by_symbol": open_orders_by_symbol,
        "recent_fills_by_symbol": recent_fills_by_symbol,
        "latest_execution_at": max((row.get("updated_at") for row in recent_fills), default=None),
        "connection_truth": {
            "has_balances": balance_row is not None,
            "has_positions": bool(position_rows),
            "has_open_orders": bool(open_orders),
            "has_recent_fills": bool(recent_fills),
            "has_quotes": bool(quote_rows),
        },
        "balance_snapshot": balance_row,
    }


def _orders_by_symbol_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            continue
        bucket = summary.setdefault(
            symbol,
            {"symbol": symbol, "count": 0, "quantity": Decimal("0"), "latest_timestamp": None, "statuses": []},
        )
        bucket["count"] = int(bucket["count"]) + 1
        bucket["quantity"] = (bucket["quantity"] if isinstance(bucket["quantity"], Decimal) else Decimal(str(bucket["quantity"]))) + (_to_decimal(row.get("quantity")) or Decimal("0"))
        latest_timestamp = row.get("updated_at") or row.get("closed_at") or row.get("entered_at")
        if latest_timestamp and (bucket["latest_timestamp"] is None or str(latest_timestamp) > str(bucket["latest_timestamp"])):
            bucket["latest_timestamp"] = latest_timestamp
        status = str(row.get("status") or "").strip()
        if status and status not in bucket["statuses"]:
            bucket["statuses"].append(status)
    return {
        symbol: {
            "symbol": symbol,
            "count": data["count"],
            "quantity": _decimal_text(data["quantity"] if isinstance(data["quantity"], Decimal) else Decimal(str(data["quantity"]))),
            "latest_timestamp": data["latest_timestamp"],
            "statuses": data["statuses"],
        }
        for symbol, data in summary.items()
    }
