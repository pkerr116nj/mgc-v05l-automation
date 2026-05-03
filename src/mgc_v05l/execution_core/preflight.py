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
from .models import BrokerOrder, PositionSource, PositionState, broker_order_blocks_same_account_contract_submit, require_aware_datetime, to_jsonable
from .pricing import MarketDataMode, MarketDataRole, QuoteObservation
from .readiness import FinalReadinessVerdict, OperatorReadiness, blocked_readiness, ready_for_paper_proof


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
                "contract_month": "202606",
                "expiry": "20260626",
                "local_symbol": "MGCM6",
                "con_id": 712565978,
                "multiplier": "10",
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
        observed_orders = tuple(_broker_order_from_transport(row, run_id=actual_run_id, config=config, observed_at=observed_at) for row in raw_orders)
        open_orders = tuple(order for order in observed_orders if broker_order_blocks_same_account_contract_submit(order))
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

        unresolved_order = _first_unresolved_broker_order(open_orders)
        if unresolved_order is not None:
            _check(checks, "proof_open_orders_clean", False, "unresolved broker order blocks proof readiness")
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
                failure_or_ambiguity="unresolved broker order blocks proof readiness",
                required_action="Wait for terminal broker order state, then rerun read-only preflight before any new proof submit.",
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
            transport_diagnostics=_transport_diagnostics(transport),
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
            transport_diagnostics=_transport_diagnostics(transport),
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
        market_data_provider=str(raw_quote.get("market_data_provider") or "IBKR"),
        market_data_mode=str(raw_quote.get("market_data_mode") or MarketDataMode.UNKNOWN),
        market_data_role=str(raw_quote.get("market_data_role") or MarketDataRole.DIAGNOSTIC),
        delayed_data_warning_seen=bool(raw_quote.get("delayed_data_warning_seen") or False),
        tick_size=raw_quote.get("tick_size"),
        exchange=raw_quote.get("exchange"),
        currency=raw_quote.get("currency"),
        provider_warnings=tuple(raw_quote.get("provider_warnings") or ()),
        raw=dict(raw_quote.get("raw") or {}),
    )


def _first_unresolved_broker_order(open_orders: tuple[BrokerOrder, ...]) -> BrokerOrder | None:
    for order in open_orders:
        if broker_order_blocks_same_account_contract_submit(order):
            return order
    return None


def _unresolved_broker_order_report(open_orders: tuple[BrokerOrder, ...]) -> dict[str, Any]:
    order = _first_unresolved_broker_order(open_orders)
    if order is None:
        return {
            "unresolved_broker_order_detected": False,
            "unresolved_broker_order_status": None,
            "unresolved_broker_order_id": None,
            "unresolved_broker_perm_id": None,
            "unresolved_remaining_quantity": None,
            "blocks_same_account_contract_submit": False,
            "next_required_action": None,
        }
    return {
        "unresolved_broker_order_detected": True,
        "unresolved_broker_order_status": order.lifecycle_status.value,
        "unresolved_broker_order_id": order.broker_order_id,
        "unresolved_broker_perm_id": order.perm_id,
        "unresolved_remaining_quantity": str(order.remaining_quantity),
        "blocks_same_account_contract_submit": True,
        "next_required_action": "Wait for the broker order to reach terminal state, then rerun read-only preflight before submitting again.",
    }


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
    transport_diagnostics: dict[str, Any] | None = None,
) -> PreflightResult:
    actual_transport_diagnostics = transport_diagnostics or {}
    market_data = _market_data_report(
        quote=quote,
        transport_diagnostics=actual_transport_diagnostics,
        paper_route_readiness=classification == PreflightClassification.READY_READ_ONLY,
    )
    unresolved_report = _unresolved_broker_order_report(open_orders)
    operator_readiness = _operator_readiness_report(
        classification=classification,
        config=config,
        position=position,
        unresolved_report=unresolved_report,
        market_data=market_data,
        failure_or_ambiguity=failure_or_ambiguity,
        required_action=required_action,
    )
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
        **unresolved_report,
        "quote": quote.to_json_dict() if quote is not None else None,
        "market_data": market_data,
        "market_data_provider": market_data["market_data_provider"],
        "market_data_mode": market_data["market_data_mode"],
        "market_data_role": market_data["market_data_role"],
        "delayed_data_warning_seen": market_data["delayed_data_warning_seen"],
        "quote_observed": market_data["quote_observed"],
        "quote_blocking_for_paper": market_data["quote_blocking_for_paper"],
        "quote_blocking_for_live_money": market_data["quote_blocking_for_live_money"],
        "paper_route_readiness": market_data["paper_route_readiness"],
        "production_live_money_readiness": market_data["production_live_money_readiness"],
        "checks": checks,
        "missing_callbacks": sorted(set(missing_callbacks)),
        "broker_errors": broker_errors,
        "failure_or_ambiguity": failure_or_ambiguity,
        "required_action": required_action,
        **operator_readiness.to_report_dict(),
        "transport_diagnostics": actual_transport_diagnostics,
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


def _operator_readiness_report(
    *,
    classification: PreflightClassification,
    config: ReadOnlyPreflightConfig,
    position: PositionState | None,
    unresolved_report: Mapping[str, Any],
    market_data: Mapping[str, Any],
    failure_or_ambiguity: str | None,
    required_action: str | None,
) -> OperatorReadiness:
    position_qty = position.signed_quantity if position is not None else None
    if unresolved_report.get("unresolved_broker_order_detected"):
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_UNRESOLVED_BROKER_ORDER,
            primary_blocker="Unresolved broker order blocks same account/contract submit.",
            required_next_action=str(
                required_action
                or unresolved_report.get("next_required_action")
                or "Wait for terminal broker order state, then rerun read-only preflight."
            ),
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_order_id=unresolved_report.get("unresolved_broker_order_id"),
            perm_id=unresolved_report.get("unresolved_broker_perm_id"),
            broker_status=unresolved_report.get("unresolved_broker_order_status"),
            position_qty=position_qty,
        )
    if position is not None and position.signed_quantity != 0:
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_NON_FLAT_POSITION,
            primary_blocker="Proof contract position is not flat.",
            required_next_action=str(required_action or "Flatten or reconcile the account before proof."),
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
        )
    if classification == PreflightClassification.READY_READ_ONLY:
        return ready_for_paper_proof(
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
        )
    verdict = FinalReadinessVerdict.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    blocker = str(failure_or_ambiguity or "Read-only preflight is not ready.")
    lowered = blocker.lower()
    if "account" in lowered or "contract" in lowered or "allowlist" in lowered:
        verdict = FinalReadinessVerdict.BLOCKED_CONTRACT_OR_ACCOUNT_MISMATCH
    if market_data.get("quote_blocking_for_paper"):
        verdict = FinalReadinessVerdict.BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE
    return blocked_readiness(
        verdict=verdict,
        primary_blocker=blocker,
        required_next_action=str(required_action or "Resolve read-only preflight blocker before any Track B submit."),
        account_id=config.account_id,
        contract_key=config.contract_key,
        position_qty=position_qty,
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
        "## Operator Readiness",
        json.dumps(
            {
                "final_readiness_verdict": report.get("final_readiness_verdict"),
                "submit_allowed": report.get("submit_allowed"),
                "submit_attempted": report.get("submit_attempted"),
                "primary_blocker": report.get("primary_blocker"),
                "required_next_action": report.get("required_next_action"),
                "broker_order_id": report.get("broker_order_id"),
                "perm_id": report.get("perm_id"),
                "broker_status": report.get("broker_status"),
                "position_qty": report.get("position_qty"),
            },
            indent=2,
            sort_keys=True,
        ),
        "",
        "## Contract",
        json.dumps(report["contract"], indent=2, sort_keys=True),
        "",
        "## Position And Open Orders",
        json.dumps({"position": report["position"], "open_orders": report["open_orders"]}, indent=2, sort_keys=True),
        "",
        "## Quote",
        json.dumps({"quote": report["quote"], "market_data": report["market_data"]}, indent=2, sort_keys=True),
        "",
        "## Broker Errors And Missing Callbacks",
        json.dumps({"broker_errors": report["broker_errors"], "missing_callbacks": report["missing_callbacks"]}, indent=2, sort_keys=True),
        "",
        "## Transport Diagnostics",
        json.dumps(report.get("transport_diagnostics", {}), indent=2, sort_keys=True),
        "",
    ]
    if report.get("failure_or_ambiguity"):
        sections.extend(["## Failure Or Ambiguity", str(report["failure_or_ambiguity"]), ""])
    if report.get("required_action"):
        sections.extend(["## Required Action", str(report["required_action"]), ""])
    return "\n".join(sections)


def _transport_diagnostics(transport: ReadOnlyPreflightTransport) -> dict[str, Any]:
    diagnostic_method = getattr(transport, "diagnostics_report", None)
    if not callable(diagnostic_method):
        return {}
    try:
        diagnostics = diagnostic_method()
    except Exception as exc:  # noqa: BLE001 - diagnostics must not hide original failure.
        return {"diagnostics_error": str(exc)}
    return dict(diagnostics or {})


def _market_data_report(
    *,
    quote: QuoteObservation | None,
    transport_diagnostics: Mapping[str, Any],
    paper_route_readiness: bool,
) -> dict[str, Any]:
    provider_warnings = _provider_warnings(transport_diagnostics)
    delayed_warning_seen = _delayed_data_warning_seen(provider_warnings)
    if quote is None:
        provider = "IBKR"
        mode = _market_data_mode_from_diagnostics(transport_diagnostics)
        role = MarketDataRole.DIAGNOSTIC
        quote_observed = False
        tick_size = exchange = currency = None
    else:
        provider = quote.market_data_provider
        mode = quote.market_data_mode
        role = quote.market_data_role
        quote_observed = True
        tick_size = quote.tick_size
        exchange = quote.exchange
        currency = quote.currency
        provider_warnings = tuple(dict.fromkeys((*provider_warnings, *quote.provider_warnings)))
        delayed_warning_seen = delayed_warning_seen or quote.delayed_data_warning_seen
    if delayed_warning_seen and mode == MarketDataMode.UNKNOWN:
        mode = MarketDataMode.DELAYED

    production_live_money_readiness = mode == MarketDataMode.REALTIME and quote_observed
    return {
        "market_data_provider": provider,
        "market_data_mode": mode,
        "market_data_role": role,
        "delayed_data_warning_seen": delayed_warning_seen,
        "quote_observed": quote_observed,
        "quote_blocking_for_paper": False,
        "quote_blocking_for_live_money": mode != MarketDataMode.REALTIME or not quote_observed,
        "paper_route_readiness": paper_route_readiness,
        "production_live_money_readiness": production_live_money_readiness,
        "proves_paper_mechanics_only": not production_live_money_readiness,
        "provider_warnings": list(provider_warnings),
        "tick_size": str(tick_size) if tick_size is not None else None,
        "exchange": exchange,
        "currency": currency,
    }


def _provider_warnings(transport_diagnostics: Mapping[str, Any]) -> tuple[str, ...]:
    warnings: list[str] = []
    for error in transport_diagnostics.get("ibkr_errors", []) or []:
        code = error.get("error_code")
        text = str(error.get("error_string") or "")
        if code in {2103, 2104, 2106, 2158, 10167, 10168} or "farm" in text.lower() or "delayed" in text.lower():
            warnings.append(f"{code}: {text}")
    return tuple(warnings)


def _delayed_data_warning_seen(warnings: Sequence[str]) -> bool:
    return any("delayed" in warning.lower() or warning.startswith("10167:") or warning.startswith("10168:") for warning in warnings)


def _market_data_mode_from_diagnostics(transport_diagnostics: Mapping[str, Any]) -> str:
    callbacks = transport_diagnostics.get("market_data_type_callbacks") or {}
    if isinstance(callbacks, Mapping):
        for raw_value in callbacks.values():
            try:
                market_data_type = int(raw_value)
            except (TypeError, ValueError):
                continue
            if market_data_type == 1:
                return MarketDataMode.REALTIME
            if market_data_type == 3:
                return MarketDataMode.DELAYED
            if market_data_type == 4:
                return MarketDataMode.DELAYED_FROZEN
    requested = str(transport_diagnostics.get("requested_market_data_mode") or "").strip().upper()
    if requested in {
        MarketDataMode.REALTIME,
        MarketDataMode.DELAYED,
        MarketDataMode.DELAYED_FROZEN,
        MarketDataMode.UNKNOWN,
    }:
        return requested
    return MarketDataMode.UNKNOWN
