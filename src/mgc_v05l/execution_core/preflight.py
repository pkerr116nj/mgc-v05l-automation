"""Read-only TWS paper preflight for Track B.

The preflight path is operator-invoked and transport-injected. It never submits,
previews, stages, or places orders.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .ibkr_paper_adapter import (
    IbkrPaperAdapter,
    IbkrPaperAdapterError,
    IbkrPaperConfigError,
    IbkrPaperReadinessError,
)
from .models import BrokerOrder, PositionSource, PositionState, require_aware_datetime, to_jsonable
from .pricing import QuoteObservation


DEFAULT_PREFLIGHT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/preflight")


class PreflightClassification(str, Enum):
    READY_READ_ONLY = "READY_READ_ONLY"
    BLOCKED = "BLOCKED"
    AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"


class ReadOnlyPreflightTransport(Protocol):
    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> Sequence[str]: ...
    def next_valid_id(self) -> int | None: ...
    def qualify_contract(self, *, contract_key: str, allowlist_entry: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def snapshot_position(self, *, run_id: str, account_id: str, contract_key: str, observed_at: datetime) -> PositionState | Mapping[str, Any] | None: ...
    def snapshot_open_orders(self, *, account_id: str, contract_key: str, observed_at: datetime) -> Sequence[BrokerOrder | Mapping[str, Any]]: ...
    def observe_quote(self, *, run_id: str, contract_key: str, observed_at: datetime) -> QuoteObservation | Mapping[str, Any] | None: ...


@dataclass(frozen=True)
class ReadOnlyPreflightConfig:
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 77
    account_id: str = "DU1234567"
    contract_key: str = "MGC-202606"
    output_root: Path = DEFAULT_PREFLIGHT_OUTPUT_ROOT
    observe_quote: bool = True
    contract_allowlist: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "MGC-202606": {
                "symbol": "MGC",
                "security_type": "FUT",
                "exchange": "COMEX",
                "currency": "USD",
                "local_symbol": "MGCJ6",
                "tick_size": "0.1",
            }
        }
    )

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": self.account_id,
            "contract_key": self.contract_key,
            "output_root": str(self.output_root),
            "observe_quote": self.observe_quote,
        }


@dataclass(frozen=True)
class PreflightResult:
    run_id: str
    classification: PreflightClassification
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def run_read_only_preflight(
    *,
    config: ReadOnlyPreflightConfig,
    transport: ReadOnlyPreflightTransport,
    run_id: str | None = None,
    now: datetime | None = None,
) -> PreflightResult:
    """Run read-only TWS paper readiness checks and write reports."""

    observed_at = now or datetime.now(timezone.utc)
    require_aware_datetime(observed_at, "now")
    actual_run_id = run_id or f"preflight_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_run_id / "preflight_report.json"
    report_md = Path(config.output_root) / actual_run_id / "preflight_report.md"
    checks: list[dict[str, Any]] = []
    missing_callbacks: list[str] = []
    broker_errors: list[str] = []
    managed_accounts: tuple[str, ...] = ()
    next_valid_id: int | None = None
    contract: dict[str, Any] | None = None
    position: PositionState | None = None
    open_orders: tuple[BrokerOrder, ...] = ()
    quote: QuoteObservation | None = None
    connected = False
    adapter: IbkrPaperAdapter | None = None

    try:
        adapter = IbkrPaperAdapter(
            mode=config.mode,
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            account_id=config.account_id,
            contract_allowlist=config.contract_allowlist,
            submit_enabled=False,
        )
        _check(checks, "paper_mode", True, "mode is PAPER")
        _check(checks, "paper_host", True, "host is 127.0.0.1")
        _check(checks, "paper_port", True, "port is 7497")
        _check(checks, "explicit_account_id", True, "account_id is explicit")
    except IbkrPaperConfigError as exc:
        _check(checks, "adapter_config", False, str(exc))
        return _write_result(
            run_id=actual_run_id,
            classification=PreflightClassification.BLOCKED,
            config=config,
            report_json=report_json,
            report_md=report_md,
            checks=checks,
            managed_accounts=managed_accounts,
            next_valid_id=next_valid_id,
            contract=contract,
            position=position,
            open_orders=open_orders,
            quote=quote,
            missing_callbacks=missing_callbacks,
            broker_errors=broker_errors,
            connected=connected,
            failure_or_ambiguity=str(exc),
            required_action="Fix explicit Track B paper preflight config.",
        )

    try:
        transport.connect(host=config.host, port=config.port, client_id=config.client_id, readonly=True)
        connected = True
        _check(checks, "transport_connected", True, "read-only transport connected")

        managed_accounts = tuple(str(account or "").strip() for account in transport.managed_accounts() if str(account or "").strip())
        adapter.record_managed_accounts(managed_accounts)
        try:
            adapter.require_configured_account()
            _check(checks, "managed_account_exact_match", True, "configured paper account matched exactly")
        except IbkrPaperReadinessError as exc:
            _check(checks, "managed_account_exact_match", False, str(exc))
            return _write_result(
                run_id=actual_run_id,
                classification=PreflightClassification.BLOCKED,
                config=config,
                report_json=report_json,
                report_md=report_md,
                checks=checks,
                managed_accounts=managed_accounts,
                next_valid_id=next_valid_id,
                contract=contract,
                position=position,
                open_orders=open_orders,
                quote=quote,
                missing_callbacks=missing_callbacks,
                broker_errors=broker_errors,
                connected=connected,
                failure_or_ambiguity=str(exc),
                required_action="Log into TWS paper with the configured account or fix account_id.",
            )

        next_valid_id = transport.next_valid_id()
        try:
            adapter.record_next_valid_id(next_valid_id)
            adapter.require_ready()
            _check(checks, "next_valid_id_observed", True, "nextValidId observed")
        except IbkrPaperReadinessError as exc:
            missing_callbacks.append("nextValidId")
            _check(checks, "next_valid_id_observed", False, str(exc))
            return _write_result(
                run_id=actual_run_id,
                classification=PreflightClassification.BLOCKED,
                config=config,
                report_json=report_json,
                report_md=report_md,
                checks=checks,
                managed_accounts=managed_accounts,
                next_valid_id=next_valid_id,
                contract=contract,
                position=position,
                open_orders=open_orders,
                quote=quote,
                missing_callbacks=missing_callbacks,
                broker_errors=broker_errors,
                connected=connected,
                failure_or_ambiguity=str(exc),
                required_action="Resolve TWS API readiness before running proof.",
            )

        allowlist_entry = config.contract_allowlist.get(config.contract_key)
        if allowlist_entry is None:
            raise IbkrPaperConfigError("contract_key must be explicitly allowlisted")
        contract = dict(transport.qualify_contract(contract_key=config.contract_key, allowlist_entry=allowlist_entry))
        adapter.qualify_contract(run_id=actual_run_id, contract_key=config.contract_key, now=observed_at)
        _check(checks, "contract_qualified", True, "exact allowlisted contract qualified")

        raw_position = transport.snapshot_position(
            run_id=actual_run_id,
            account_id=config.account_id,
            contract_key=config.contract_key,
            observed_at=observed_at,
        )
        position = _position_from_transport(raw_position, run_id=actual_run_id, config=config, observed_at=observed_at)
        _check(checks, "position_snapshot_observed", True, "position snapshot observed")

        raw_orders = transport.snapshot_open_orders(
            account_id=config.account_id,
            contract_key=config.contract_key,
            observed_at=observed_at,
        )
        open_orders = tuple(_broker_order_from_transport(row, run_id=actual_run_id, config=config, observed_at=observed_at) for row in raw_orders)
        _check(checks, "open_orders_snapshot_observed", True, "open orders snapshot observed")

        if config.observe_quote:
            quote = _quote_from_transport(
                transport.observe_quote(run_id=actual_run_id, contract_key=config.contract_key, observed_at=observed_at),
                run_id=actual_run_id,
                config=config,
                observed_at=observed_at,
            )
            if quote is None:
                _check(checks, "quote_observed", False, "quote missing", blocking=False)
            else:
                _check(checks, "quote_observed", True, "quote observed", blocking=False)

        if open_orders:
            _check(checks, "proof_open_orders_clean", False, "existing open order blocks proof readiness")
            return _write_result(
                run_id=actual_run_id,
                classification=PreflightClassification.BLOCKED,
                config=config,
                report_json=report_json,
                report_md=report_md,
                checks=checks,
                managed_accounts=managed_accounts,
                next_valid_id=next_valid_id,
                contract=contract,
                position=position,
                open_orders=open_orders,
                quote=quote,
                missing_callbacks=missing_callbacks,
                broker_errors=broker_errors,
                connected=connected,
                failure_or_ambiguity="existing open order blocks proof readiness",
                required_action="Cancel or resolve existing open orders before proof.",
            )
        _check(checks, "proof_open_orders_clean", True, "no existing open orders")

        if position.signed_quantity != 0:
            _check(checks, "proof_position_flat", False, "existing position blocks proof readiness")
            return _write_result(
                run_id=actual_run_id,
                classification=PreflightClassification.BLOCKED,
                config=config,
                report_json=report_json,
                report_md=report_md,
                checks=checks,
                managed_accounts=managed_accounts,
                next_valid_id=next_valid_id,
                contract=contract,
                position=position,
                open_orders=open_orders,
                quote=quote,
                missing_callbacks=missing_callbacks,
                broker_errors=broker_errors,
                connected=connected,
                failure_or_ambiguity="existing position blocks proof readiness",
                required_action="Flatten or reconcile the account before proof.",
            )
        _check(checks, "proof_position_flat", True, "broker position is flat for exact contract")

        return _write_result(
            run_id=actual_run_id,
            classification=PreflightClassification.READY_READ_ONLY,
            config=config,
            report_json=report_json,
            report_md=report_md,
            checks=checks,
            managed_accounts=managed_accounts,
            next_valid_id=next_valid_id,
            contract=contract,
            position=position,
            open_orders=open_orders,
            quote=quote,
            missing_callbacks=missing_callbacks,
            broker_errors=broker_errors,
            connected=connected,
            failure_or_ambiguity=None,
            required_action=None,
        )
    except IbkrPaperAdapterError as exc:
        _check(checks, "adapter_readiness", False, str(exc))
        return _write_result(
            run_id=actual_run_id,
            classification=PreflightClassification.BLOCKED,
            config=config,
            report_json=report_json,
            report_md=report_md,
            checks=checks,
            managed_accounts=managed_accounts,
            next_valid_id=next_valid_id,
            contract=contract,
            position=position,
            open_orders=open_orders,
            quote=quote,
            missing_callbacks=missing_callbacks,
            broker_errors=broker_errors,
            connected=connected,
            failure_or_ambiguity=str(exc),
            required_action="Resolve preflight blocker before proof.",
        )
    except Exception as exc:  # noqa: BLE001 - report unexpected callback/transport ambiguity.
        broker_errors.append(str(exc))
        _check(checks, "transport_or_callback_error", False, str(exc))
        return _write_result(
            run_id=actual_run_id,
            classification=PreflightClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            config=config,
            report_json=report_json,
            report_md=report_md,
            checks=checks,
            managed_accounts=managed_accounts,
            next_valid_id=next_valid_id,
            contract=contract,
            position=position,
            open_orders=open_orders,
            quote=quote,
            missing_callbacks=missing_callbacks,
            broker_errors=broker_errors,
            connected=connected,
            failure_or_ambiguity=str(exc),
            required_action="Manual TWS/API review required before proof.",
        )
    finally:
        if connected:
            transport.disconnect()


def _check(checks: list[dict[str, Any]], name: str, passed: bool, detail: str, *, blocking: bool = True) -> None:
    checks.append({"name": name, "passed": passed, "blocking": blocking, "detail": detail})


def _position_from_transport(
    raw_position: PositionState | Mapping[str, Any] | None,
    *,
    run_id: str,
    config: ReadOnlyPreflightConfig,
    observed_at: datetime,
) -> PositionState:
    if isinstance(raw_position, PositionState):
        return raw_position
    if raw_position is None:
        return PositionState(
            position_state_id=f"preflight_position_{run_id}_{config.contract_key}",
            run_id=run_id,
            source=PositionSource.BROKER,
            account_id=config.account_id,
            contract_key=config.contract_key,
            signed_quantity=0,
            average_price=None,
            open_order_ids=(),
            observed_at=observed_at,
            raw={"source": "transport_default_flat"},
        )
    return PositionState(
        position_state_id=str(raw_position.get("position_state_id") or f"preflight_position_{run_id}_{config.contract_key}"),
        run_id=run_id,
        source=PositionSource.BROKER,
        account_id=str(raw_position.get("account_id") or config.account_id),
        contract_key=str(raw_position.get("contract_key") or config.contract_key),
        signed_quantity=int(raw_position.get("signed_quantity") or 0),
        average_price=raw_position.get("average_price"),
        open_order_ids=tuple(raw_position.get("open_order_ids") or ()),
        observed_at=observed_at,
        raw=dict(raw_position.get("raw") or {}),
    )


def _broker_order_from_transport(
    raw_order: BrokerOrder | Mapping[str, Any],
    *,
    run_id: str,
    config: ReadOnlyPreflightConfig,
    observed_at: datetime,
) -> BrokerOrder:
    if isinstance(raw_order, BrokerOrder):
        return raw_order
    broker_order_id = str(raw_order.get("broker_order_id") or "UNKNOWN")
    return BrokerOrder(
        broker_order_event_id=str(raw_order.get("broker_order_event_id") or f"preflight_order_{run_id}_{broker_order_id}"),
        run_id=run_id,
        submit_attempt_id=str(raw_order.get("submit_attempt_id") or "preflight_read_only"),
        account_id=str(raw_order.get("account_id") or config.account_id),
        broker_order_id=broker_order_id,
        perm_id=raw_order.get("perm_id"),
        client_id=raw_order.get("client_id"),
        contract_key=str(raw_order.get("contract_key") or config.contract_key),
        action=str(raw_order.get("action") or "BUY"),
        quantity=raw_order.get("quantity") or 1,
        order_type=str(raw_order.get("order_type") or "LMT"),
        limit_price=raw_order.get("limit_price") or "1",
        status=str(raw_order.get("status") or "Submitted"),
        filled_quantity=raw_order.get("filled_quantity") or 0,
        remaining_quantity=raw_order.get("remaining_quantity") or 1,
        average_fill_price=raw_order.get("average_fill_price"),
        observed_at=observed_at,
        raw=dict(raw_order.get("raw") or {}),
    )


def _quote_from_transport(
    raw_quote: QuoteObservation | Mapping[str, Any] | None,
    *,
    run_id: str,
    config: ReadOnlyPreflightConfig,
    observed_at: datetime,
) -> QuoteObservation | None:
    if raw_quote is None:
        return None
    if isinstance(raw_quote, QuoteObservation):
        return raw_quote
    return QuoteObservation(
        quote_id=str(raw_quote.get("quote_id") or f"preflight_quote_{run_id}_{config.contract_key}"),
        run_id=run_id,
        contract_key=str(raw_quote.get("contract_key") or config.contract_key),
        source=str(raw_quote.get("source") or "ibkr_paper_preflight_transport"),
        bid=raw_quote.get("bid"),
        ask=raw_quote.get("ask"),
        last=raw_quote.get("last"),
        observed_at=observed_at,
        raw=dict(raw_quote.get("raw") or {}),
    )


def _write_result(
    *,
    run_id: str,
    classification: PreflightClassification,
    config: ReadOnlyPreflightConfig,
    report_json: Path,
    report_md: Path,
    checks: list[dict[str, Any]],
    managed_accounts: tuple[str, ...],
    next_valid_id: int | None,
    contract: dict[str, Any] | None,
    position: PositionState | None,
    open_orders: tuple[BrokerOrder, ...],
    quote: QuoteObservation | None,
    missing_callbacks: list[str],
    broker_errors: list[str],
    connected: bool,
    failure_or_ambiguity: str | None,
    required_action: str | None,
) -> PreflightResult:
    report = {
        "schema_version": "track_b_read_only_preflight_v1",
        "classification": classification.value,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config.to_report_dict(),
        "connected": connected,
        "managed_accounts": managed_accounts,
        "next_valid_id": next_valid_id,
        "contract_key": config.contract_key,
        "contract": contract,
        "position": position.to_json_dict() if position is not None else None,
        "open_orders": [order.to_json_dict() for order in open_orders],
        "quote": quote.to_json_dict() if quote is not None else None,
        "checks": checks,
        "missing_callbacks": sorted(set(missing_callbacks)),
        "broker_errors": broker_errors,
        "failure_or_ambiguity": failure_or_ambiguity,
        "required_action": required_action,
        "submit_enabled": False,
        "place_order_called": False,
        "report_json_path": str(report_json),
        "report_markdown_path": str(report_md),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    report_md.write_text(_render_markdown(report), encoding="utf-8")
    return PreflightResult(
        run_id=run_id,
        classification=classification,
        report_json=report_json,
        report_md=report_md,
        report=report,
    )


def _render_markdown(report: dict[str, Any]) -> str:
    sections = [
        "# Track B Read-Only TWS Paper Preflight",
        "",
        "## Classification",
        str(report["classification"]),
        "",
        "## Config",
        json.dumps(report["config"], indent=2, sort_keys=True),
        "",
        "## Account",
        json.dumps({"managed_accounts": report["managed_accounts"]}, indent=2, sort_keys=True),
        "",
        "## Readiness",
        json.dumps({"next_valid_id": report["next_valid_id"], "checks": report["checks"]}, indent=2, sort_keys=True),
        "",
        "## Contract",
        json.dumps(report["contract"], indent=2, sort_keys=True),
        "",
        "## Position And Open Orders",
        json.dumps({"position": report["position"], "open_orders": report["open_orders"]}, indent=2, sort_keys=True),
        "",
        "## Quote",
        json.dumps(report["quote"], indent=2, sort_keys=True),
        "",
        "## Broker Errors And Missing Callbacks",
        json.dumps({"broker_errors": report["broker_errors"], "missing_callbacks": report["missing_callbacks"]}, indent=2, sort_keys=True),
        "",
    ]
    if report.get("failure_or_ambiguity"):
        sections.extend(["## Failure Or Ambiguity", str(report["failure_or_ambiguity"]), ""])
    if report.get("required_action"):
        sections.extend(["## Required Action", str(report["required_action"]), ""])
    return "\n".join(sections)
