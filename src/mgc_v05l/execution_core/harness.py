"""Dependency-injected Track B paper proof harness.

The harness is intentionally broker-agnostic. This slice uses the fake adapter
to prove the execution spine without importing IBKR/TWS libraries or submitting
broker orders.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from .fake_adapter import FakePaperAdapter, FakeSubmitResult
from .ledger import JsonlLedger, LedgerEvent
from .models import (
    Action,
    BrokerOrder,
    FillEvent,
    IntentKind,
    OrderIntent,
    PositionState,
    ReconciliationResult,
    ReconciliationStage,
    ReconciliationStatus,
    SignalEvent,
    SubmitAttempt,
    SubmitAttemptState,
    TerminalClassification,
)
from .pricing import PricingError, QuoteObservation, create_marketable_limit_decision
from .reconcile import reconcile_position
from .risk_gate import evaluate_order_gate


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core")


class HarnessAdapter(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> tuple[str, ...]: ...
    def qualify_contract(self, *, run_id: str, contract_key: str, allowlist_entry: dict[str, Any], now: datetime) -> dict[str, Any]: ...
    def observe_quote(self, *, run_id: str, now: datetime) -> QuoteObservation: ...
    def observe_position(self, *, run_id: str, now: datetime, stage: str = "observed") -> PositionState: ...
    def observe_open_orders(self) -> tuple[BrokerOrder, ...]: ...
    def submit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent, now: datetime) -> FakeSubmitResult: ...
    def cancel_order(self, *, run_id: str, submit_attempt: SubmitAttempt, broker_order: BrokerOrder, now: datetime): ...


@dataclass(frozen=True)
class HarnessConfig:
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 77
    account_id: str = "DU1234567"
    contract_key: str = "MGC-202606"
    side: str = "BUY"
    quantity: int = 1
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    fill_offset_ticks: Decimal = Decimal("1")
    max_quote_age_seconds: Decimal = Decimal("30")
    max_distance_ticks: Decimal = Decimal("10")
    max_distance_percent: Decimal = Decimal("0.25")
    open_fill_timeout_seconds: Decimal = Decimal("30")
    close_fill_timeout_seconds: Decimal = Decimal("30")
    cancel_timeout_seconds: Decimal = Decimal("10")
    output_root: Path = DEFAULT_OUTPUT_ROOT
    confirm_paper_only: bool = True
    order_extra_fields: dict[str, Any] = field(default_factory=dict)
    contract_allowlist: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "MGC-202606": {
                "symbol": "MGC",
                "security_type": "FUT",
                "exchange": "COMEX",
                "currency": "USD",
                "local_symbol": "MGCM6",
                "tick_size": "0.1",
            }
        }
    )

    def environment(self) -> dict[str, Any]:
        return {"mode": self.mode, "host": self.host, "port": self.port, "client_id": self.client_id}

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": self.account_id,
            "contract_key": self.contract_key,
            "side": self.side,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "fill_offset_ticks": str(self.fill_offset_ticks),
            "max_quote_age_seconds": str(self.max_quote_age_seconds),
            "max_distance_ticks": str(self.max_distance_ticks),
            "max_distance_percent": str(self.max_distance_percent),
            "open_fill_timeout_seconds": str(self.open_fill_timeout_seconds),
            "close_fill_timeout_seconds": str(self.close_fill_timeout_seconds),
            "cancel_timeout_seconds": str(self.cancel_timeout_seconds),
            "output_root": str(self.output_root),
            "confirm_paper_only": self.confirm_paper_only,
            "order_extra_fields": self.order_extra_fields,
        }


@dataclass(frozen=True)
class HarnessResult:
    run_id: str
    classification: TerminalClassification
    run_dir: Path
    ledger_path: Path
    proof_report_json: Path
    proof_report_md: Path
    event_count: int


def run_fake_paper_proof(
    *,
    config: HarnessConfig,
    adapter: HarnessAdapter | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> HarnessResult:
    """Run a full fake-adapter proof flow and write ledger plus reports."""

    clock_now = now or datetime.now(timezone.utc)
    if clock_now.tzinfo is None or clock_now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    actual_run_id = run_id or f"track_b_{uuid.uuid4().hex}"
    run_dir = Path(config.output_root) / actual_run_id
    ledger = JsonlLedger(run_dir / "ledger.jsonl")
    proof_json = run_dir / "proof_report.json"
    proof_md = run_dir / "proof_report.md"
    actual_adapter = adapter or FakePaperAdapter(account_id=config.account_id, contract_key=config.contract_key)
    context: dict[str, Any] = {
        "run_id": actual_run_id,
        "config": config.to_report_dict(),
        "environment": config.environment(),
        "validated_account_id": None,
        "contract": None,
        "broker_errors": [],
        "missing_callbacks": [],
        "failure_or_ambiguity": None,
        "required_manual_action": None,
    }

    ledger.append_event(run_id=actual_run_id, event_type="run_started", payload={"run_id": actual_run_id})
    ledger.append_event(run_id=actual_run_id, event_type="config_loaded", payload=config.to_report_dict())

    config_errors = _validate_config(config)
    ledger.append_event(
        run_id=actual_run_id,
        event_type="config_validated",
        payload={"valid": not config_errors, "errors": config_errors},
    )
    if config_errors:
        return _finish(
            ledger=ledger,
            context=context,
            classification=TerminalClassification.BLOCKED,
            proof_json=proof_json,
            proof_md=proof_md,
            reason="; ".join(config_errors),
            required_action="Fix explicit Track B config and start a new run.",
        )

    try:
        actual_adapter.connect()
        ledger.append_event(run_id=actual_run_id, event_type="broker_connected", payload={"adapter": "fake", "connected": True})

        managed_accounts = actual_adapter.managed_accounts()
        if config.account_id not in managed_accounts:
            ledger.append_event(
                run_id=actual_run_id,
                event_type="account_validated",
                payload={"valid": False, "configured_account_id": config.account_id, "managed_accounts": managed_accounts},
            )
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.BLOCKED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="configured paper account not present in managed accounts",
                required_action="Start TWS paper with the configured account or fix account_id.",
            )
        context["validated_account_id"] = config.account_id
        ledger.append_event(
            run_id=actual_run_id,
            event_type="account_validated",
            payload={"valid": True, "configured_account_id": config.account_id, "managed_accounts": managed_accounts},
        )

        allowlist_entry = config.contract_allowlist[config.contract_key]
        contract = actual_adapter.qualify_contract(
            run_id=actual_run_id,
            contract_key=config.contract_key,
            allowlist_entry=allowlist_entry,
            now=clock_now,
        )
        context["contract"] = contract
        ledger.append_event(run_id=actual_run_id, event_type="contract_qualified", payload=contract)

        open_pricing = _observe_and_price(
            ledger=ledger,
            adapter=actual_adapter,
            config=config,
            run_id=actual_run_id,
            action=config.side,
            now=clock_now,
            index=1,
        )
        if isinstance(open_pricing, HarnessResult):
            return open_pricing

        broker_position = actual_adapter.observe_position(run_id=actual_run_id, now=clock_now, stage="PRE_OPEN")
        broker_open_orders = actual_adapter.observe_open_orders()
        _append_broker_observations(ledger, actual_run_id, broker_position, broker_open_orders)
        pre_open = _reconcile(
            ledger=ledger,
            run_id=actual_run_id,
            stage=ReconciliationStage.PRE_OPEN,
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
        )
        if pre_open.status != ReconciliationStatus.CLEAN:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.BLOCKED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="PRE_OPEN reconciliation was not clean",
                required_action=pre_open.required_action,
            )

        signal = _signal_event(config=config, run_id=actual_run_id, now=clock_now)
        signal_event = ledger.append_model_event(event_type="signal_event_created", model=signal)
        open_intent = _order_intent(
            config=config,
            run_id=actual_run_id,
            signal_event_id=signal.signal_event_id,
            intent_kind=IntentKind.OPEN,
            action=Action(config.side),
            limit_price=open_pricing.limit_price,
            now=clock_now,
            index=1,
        )
        open_intent_event = ledger.append_model_event(
            event_type="order_intent_created",
            model=open_intent,
            causation_id=signal_event.event_id,
            correlation_id=open_intent.order_intent_id,
        )
        open_gate = evaluate_order_gate(
            order_intent=open_intent,
            environment=config.environment(),
            configured_account_id=config.account_id,
            contract_allowlist=config.contract_allowlist,
            broker_position=broker_position,
            broker_open_orders=broker_open_orders,
            reconciliation=pre_open,
            created_at=clock_now,
        )
        ledger.append_model_event(
            event_type="gate_decision_created",
            model=open_gate,
            causation_id=open_intent_event.event_id,
            correlation_id=open_intent.order_intent_id,
        )
        if not open_gate.passed:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.BLOCKED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason=str(open_gate.blocking_reason),
                required_action="Risk gate rejected before broker submit.",
            )

        open_submit = _submit_attempt(config, actual_run_id, open_intent, pre_open, clock_now, index=1)
        open_submit_event = ledger.append_model_event(
            event_type="submit_attempt_created",
            model=open_submit,
            causation_id=open_intent_event.event_id,
            correlation_id=open_submit.submit_attempt_id,
        )
        open_result = actual_adapter.submit_order(submit_attempt=open_submit, order_intent=open_intent, now=clock_now)
        if open_result.missing_callbacks:
            context["missing_callbacks"].extend(open_result.missing_callbacks)
        if open_result.ambiguous:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason=open_result.failure_reason or "open submit ambiguous",
                required_action="Manual broker review required before any further broker action.",
            )
        if open_result.broker_order is not None:
            ledger.append_model_event(
                event_type="broker_order_observed",
                model=open_result.broker_order,
                causation_id=open_submit_event.event_id,
                correlation_id=open_submit.submit_attempt_id,
            )
        if open_result.fill_event is None:
            return _cancel_open_and_block(
                ledger=ledger,
                adapter=actual_adapter,
                config=config,
                context=context,
                submit_attempt=open_submit,
                broker_order=open_result.broker_order,
                proof_json=proof_json,
                proof_md=proof_md,
                now=clock_now,
            )

        open_fill_event = ledger.append_model_event(
            event_type="fill_event_created",
            model=open_result.fill_event,
            causation_id=open_submit_event.event_id,
            correlation_id=open_submit.submit_attempt_id,
        )
        post_open = _reconcile(
            ledger=ledger,
            run_id=actual_run_id,
            stage=ReconciliationStage.POST_OPEN,
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_position=open_result.broker_position,
            broker_open_orders=open_result.open_orders,
            fill_events=(open_result.fill_event,),
            expected_signed_quantity=1 if open_intent.action == Action.BUY else -1,
        )
        if post_open.status != ReconciliationStatus.CLEAN:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="POST_OPEN reconciliation was not clean",
                required_action=post_open.required_action,
            )

        pre_close = _reconcile(
            ledger=ledger,
            run_id=actual_run_id,
            stage=ReconciliationStage.PRE_CLOSE,
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_position=open_result.broker_position,
            broker_open_orders=open_result.open_orders,
            fill_events=(open_result.fill_event,),
            expected_signed_quantity=1 if open_intent.action == Action.BUY else -1,
        )
        if pre_close.status != ReconciliationStatus.CLEAN:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="PRE_CLOSE reconciliation was not clean",
                required_action=pre_close.required_action,
            )

        close_action = Action.SELL if open_intent.action == Action.BUY else Action.BUY
        close_pricing = _observe_and_price(
            ledger=ledger,
            adapter=actual_adapter,
            config=config,
            run_id=actual_run_id,
            action=close_action,
            now=clock_now,
            index=2,
        )
        if isinstance(close_pricing, HarnessResult):
            return close_pricing
        close_intent = _order_intent(
            config=config,
            run_id=actual_run_id,
            signal_event_id=signal.signal_event_id,
            intent_kind=IntentKind.CLOSE,
            action=close_action,
            limit_price=close_pricing.limit_price,
            now=clock_now,
            index=2,
        )
        close_intent_event = ledger.append_model_event(
            event_type="order_intent_created",
            model=close_intent,
            causation_id=open_fill_event.event_id,
            correlation_id=close_intent.order_intent_id,
        )
        close_gate = evaluate_order_gate(
            order_intent=close_intent,
            environment=config.environment(),
            configured_account_id=config.account_id,
            contract_allowlist=config.contract_allowlist,
            broker_position=open_result.broker_position,
            broker_open_orders=open_result.open_orders,
            reconciliation=pre_close,
            created_at=clock_now,
        )
        ledger.append_model_event(
            event_type="gate_decision_created",
            model=close_gate,
            causation_id=close_intent_event.event_id,
            correlation_id=close_intent.order_intent_id,
        )
        if not close_gate.passed:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason=str(close_gate.blocking_reason),
                required_action="Close risk gate failed after an open fill; manual review required.",
            )

        close_submit = _submit_attempt(config, actual_run_id, close_intent, pre_close, clock_now, index=2)
        close_submit_event = ledger.append_model_event(
            event_type="submit_attempt_created",
            model=close_submit,
            causation_id=close_intent_event.event_id,
            correlation_id=close_submit.submit_attempt_id,
        )
        close_result = actual_adapter.submit_order(submit_attempt=close_submit, order_intent=close_intent, now=clock_now)
        if close_result.missing_callbacks:
            context["missing_callbacks"].extend(close_result.missing_callbacks)
        if close_result.ambiguous:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason=close_result.failure_reason or "close submit ambiguous",
                required_action="Manual broker review required. Do not send a second close order.",
            )
        if close_result.broker_order is not None:
            ledger.append_model_event(
                event_type="broker_order_observed",
                model=close_result.broker_order,
                causation_id=close_submit_event.event_id,
                correlation_id=close_submit.submit_attempt_id,
            )
        if close_result.fill_event is None:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="close fill missing",
                required_action="Manual broker review required. Do not send a second close order.",
            )
        ledger.append_model_event(
            event_type="fill_event_created",
            model=close_result.fill_event,
            causation_id=close_submit_event.event_id,
            correlation_id=close_submit.submit_attempt_id,
        )
        fills = (open_result.fill_event, close_result.fill_event)
        post_close = _reconcile(
            ledger=ledger,
            run_id=actual_run_id,
            stage=ReconciliationStage.POST_CLOSE,
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_position=close_result.broker_position,
            broker_open_orders=close_result.open_orders,
            fill_events=fills,
            expected_signed_quantity=0,
        )
        if post_close.status != ReconciliationStatus.CLEAN:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="POST_CLOSE reconciliation was not clean",
                required_action=post_close.required_action,
            )

        final_position = actual_adapter.observe_position(run_id=actual_run_id, now=clock_now, stage="FINAL")
        final_open_orders = actual_adapter.observe_open_orders()
        final = _reconcile(
            ledger=ledger,
            run_id=actual_run_id,
            stage=ReconciliationStage.FINAL,
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_position=final_position,
            broker_open_orders=final_open_orders,
            fill_events=fills,
            expected_signed_quantity=0,
        )
        if final.status != ReconciliationStatus.CLEAN:
            return _finish(
                ledger=ledger,
                context=context,
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                proof_json=proof_json,
                proof_md=proof_md,
                reason="FINAL reconciliation was not clean",
                required_action=final.required_action,
            )
        return _finish(
            ledger=ledger,
            context=context,
            classification=TerminalClassification.PASSED,
            proof_json=proof_json,
            proof_md=proof_md,
            reason=None,
            required_action=None,
        )
    finally:
        actual_adapter.disconnect()


def _observe_and_price(
    *,
    ledger: JsonlLedger,
    adapter: HarnessAdapter,
    config: HarnessConfig,
    run_id: str,
    action: Action | str,
    now: datetime,
    index: int,
) -> Any:
    quote = adapter.observe_quote(run_id=run_id, now=now)
    ledger.append_model_event(event_type="quote_observed", model=quote, correlation_id=f"pricing_{index}")
    try:
        decision = create_marketable_limit_decision(
            pricing_decision_id=f"pricing_{run_id}_{index}",
            quote=quote,
            action=action,
            tick_size=config.contract_allowlist[config.contract_key].get("tick_size"),
            fill_offset_ticks=config.fill_offset_ticks,
            max_quote_age_seconds=config.max_quote_age_seconds,
            max_distance_ticks=config.max_distance_ticks,
            max_distance_percent=config.max_distance_percent,
            now=now,
        )
    except PricingError as exc:
        return _finish(
            ledger=ledger,
            context={
                "run_id": run_id,
                "config": config.to_report_dict(),
                "environment": config.environment(),
                "validated_account_id": config.account_id,
                "contract": config.contract_allowlist.get(config.contract_key),
                "broker_errors": [],
                "missing_callbacks": [],
            },
            classification=TerminalClassification.BLOCKED,
            proof_json=Path(config.output_root) / run_id / "proof_report.json",
            proof_md=Path(config.output_root) / run_id / "proof_report.md",
            reason=str(exc),
            required_action="Fix quote/pricing preconditions and start a new run.",
        )
    ledger.append_model_event(event_type="pricing_decision_created", model=decision, correlation_id=f"pricing_{index}")
    return decision


def _append_broker_observations(
    ledger: JsonlLedger,
    run_id: str,
    broker_position: PositionState,
    broker_open_orders: tuple[BrokerOrder, ...],
) -> None:
    ledger.append_model_event(event_type="broker_position_observed", model=broker_position)
    ledger.append_event(
        run_id=run_id,
        event_type="broker_open_orders_observed",
        payload={"open_order_ids": [order.broker_order_id for order in broker_open_orders], "count": len(broker_open_orders)},
    )


def _reconcile(
    *,
    ledger: JsonlLedger,
    run_id: str,
    stage: ReconciliationStage,
    account_id: str,
    contract_key: str,
    broker_position: PositionState,
    broker_open_orders: tuple[BrokerOrder, ...],
    fill_events: tuple[FillEvent, ...] = (),
    expected_signed_quantity: int | None = None,
) -> ReconciliationResult:
    ledger_position = ledger.replay_position(run_id=run_id, account_id=account_id, contract_key=contract_key)
    result = reconcile_position(
        run_id=run_id,
        stage=stage,
        account_id=account_id,
        contract_key=contract_key,
        broker_position=broker_position,
        ledger_position=ledger_position,
        broker_open_orders=broker_open_orders,
        fill_events=fill_events,
        expected_signed_quantity=expected_signed_quantity,
    )
    ledger.append_model_event(event_type="reconciliation_created", model=result)
    return result


def _signal_event(*, config: HarnessConfig, run_id: str, now: datetime) -> SignalEvent:
    return SignalEvent(
        signal_event_id=f"signal_{run_id}",
        run_id=run_id,
        source_event_id=f"operator_{run_id}",
        bar_id=f"synthetic_bar_{run_id}",
        strategy_id="TRACK_B_FAKE_PROOF",
        symbol=config.contract_allowlist[config.contract_key]["symbol"],
        contract_key=config.contract_key,
        decision="TRACK_B_FAKE_PAPER_PROOF",
        side=config.side,
        quantity=config.quantity,
        reason="synthetic operator-invoked proof signal",
        occurred_at=now,
        input_digest=f"synthetic:{run_id}",
    )


def _order_intent(
    *,
    config: HarnessConfig,
    run_id: str,
    signal_event_id: str,
    intent_kind: IntentKind,
    action: Action,
    limit_price: Decimal,
    now: datetime,
    index: int,
) -> OrderIntent:
    return OrderIntent(
        order_intent_id=f"intent_{run_id}_{index}",
        signal_event_id=signal_event_id,
        run_id=run_id,
        intent_kind=intent_kind,
        account_id=config.account_id,
        symbol=config.contract_allowlist[config.contract_key]["symbol"],
        contract_key=config.contract_key,
        action=action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price=limit_price,
        time_in_force=config.time_in_force,
        paper_only=True,
        created_at=now,
        reason=f"track b fake {intent_kind.value.lower()} proof",
        extra_fields=config.order_extra_fields,
    )


def _submit_attempt(
    config: HarnessConfig,
    run_id: str,
    order_intent: OrderIntent,
    reconciliation: ReconciliationResult,
    now: datetime,
    *,
    index: int,
) -> SubmitAttempt:
    return SubmitAttempt(
        submit_attempt_id=f"submit_{run_id}_{index}",
        order_intent_id=order_intent.order_intent_id,
        run_id=run_id,
        account_id=config.account_id,
        broker="FAKE",
        environment=config.environment(),
        pre_submit_reconciliation_id=reconciliation.reconciliation_id,
        open_order_baseline_event_id=f"baseline_{run_id}_{index}",
        request_digest=f"{order_intent.order_intent_id}:{order_intent.limit_price}",
        state=SubmitAttemptState.CREATED,
        submitted_at=now,
    )


def _cancel_open_and_block(
    *,
    ledger: JsonlLedger,
    adapter: HarnessAdapter,
    config: HarnessConfig,
    context: dict[str, Any],
    submit_attempt: SubmitAttempt,
    broker_order: BrokerOrder | None,
    proof_json: Path,
    proof_md: Path,
    now: datetime,
) -> HarnessResult:
    if broker_order is None:
        return _finish(
            ledger=ledger,
            context=context,
            classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            proof_json=proof_json,
            proof_md=proof_md,
            reason="open order did not fill and no broker_order_id was available for cancel",
            required_action="Manual broker review required before any further broker action.",
        )
    cancel = adapter.cancel_order(run_id=submit_attempt.run_id, submit_attempt=submit_attempt, broker_order=broker_order, now=now)
    ledger.append_cancel_attempt(cancel, correlation_id=submit_attempt.submit_attempt_id)
    ledger.append_event(
        run_id=submit_attempt.run_id,
        event_type="cancel_status_observed",
        payload={
            "cancel_attempt_id": cancel.cancel_attempt_id,
            "broker_order_id": cancel.broker_order_id,
            "observed_cancel_status": cancel.observed_cancel_status,
            "confirmed_at": cancel.confirmed_at.isoformat() if cancel.confirmed_at else None,
        },
        correlation_id=submit_attempt.submit_attempt_id,
    )
    broker_position = adapter.observe_position(run_id=submit_attempt.run_id, now=now, stage="CANCEL_CONFIRMED")
    broker_open_orders = adapter.observe_open_orders()
    recon = _reconcile(
        ledger=ledger,
        run_id=submit_attempt.run_id,
        stage=ReconciliationStage.PRE_OPEN,
        account_id=config.account_id,
        contract_key=config.contract_key,
        broker_position=broker_position,
        broker_open_orders=broker_open_orders,
        expected_signed_quantity=0,
    )
    return _finish(
        ledger=ledger,
        context=context,
        classification=TerminalClassification.BLOCKED if recon.status == ReconciliationStatus.CLEAN else TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
        proof_json=proof_json,
        proof_md=proof_md,
        reason="open order did not fill and was canceled cleanly" if recon.status == ReconciliationStatus.CLEAN else "open cancel reconciliation ambiguous",
        required_action="No replacement order is allowed in milestone one. Start a new run if desired."
        if recon.status == ReconciliationStatus.CLEAN
        else recon.required_action,
    )


def _finish(
    *,
    ledger: JsonlLedger,
    context: dict[str, Any],
    classification: TerminalClassification,
    proof_json: Path,
    proof_md: Path,
    reason: str | None,
    required_action: str | None,
) -> HarnessResult:
    run_id = context["run_id"]
    context["failure_or_ambiguity"] = reason
    context["required_manual_action"] = required_action
    proof_event = ledger.append_event(
        run_id=run_id,
        event_type="proof_report_written",
        payload={"proof_report_json": str(proof_json), "proof_report_md": str(proof_md), "classification": classification.value},
    )
    terminal_event_type = {
        TerminalClassification.PASSED: "run_passed",
        TerminalClassification.BLOCKED: "run_blocked",
        TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED: "run_ambiguous_manual_review_required",
    }[classification]
    ledger.append_event(
        run_id=run_id,
        event_type=terminal_event_type,
        payload={"classification": classification.value, "reason": reason, "required_action": required_action},
        causation_id=proof_event.event_id,
    )
    events = ledger.read_events(run_id=run_id)
    report = _build_report(
        classification=classification,
        events=events,
        context=context,
        ledger_path=ledger.events_path,
        proof_json=proof_json,
        proof_md=proof_md,
    )
    proof_json.parent.mkdir(parents=True, exist_ok=True)
    proof_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    proof_md.write_text(_render_markdown_report(report), encoding="utf-8")
    return HarnessResult(
        run_id=run_id,
        classification=classification,
        run_dir=proof_json.parent,
        ledger_path=ledger.events_path,
        proof_report_json=proof_json,
        proof_report_md=proof_md,
        event_count=len(events),
    )


def _build_report(
    *,
    classification: TerminalClassification,
    events: tuple[LedgerEvent, ...],
    context: dict[str, Any],
    ledger_path: Path,
    proof_json: Path,
    proof_md: Path,
) -> dict[str, Any]:
    by_type: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        by_type.setdefault(event.event_type, []).append(event.payload)

    def first(event_type: str) -> dict[str, Any] | None:
        rows = by_type.get(event_type, [])
        return rows[0] if rows else None

    reconciliations = by_type.get("reconciliation_created", [])
    by_stage = {str(row.get("stage")): row for row in reconciliations}
    intents = by_type.get("order_intent_created", [])
    submits = by_type.get("submit_attempt_created", [])
    broker_orders = by_type.get("broker_order_observed", [])
    fills = by_type.get("fill_event_created", [])
    gate_checks = [row for row in by_type.get("gate_decision_created", [])]
    return {
        "schema_version": "track_b_fake_harness_v1",
        "classification": classification.value,
        "run_id": context["run_id"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": context.get("environment"),
        "configured_account_id": context.get("config", {}).get("account_id"),
        "validated_account_id": context.get("validated_account_id"),
        "contract_key": context.get("config", {}).get("contract_key"),
        "contract": context.get("contract"),
        "state_machine": [event.event_type for event in events],
        "event_counts": {event_type: len(rows) for event_type, rows in sorted(by_type.items())},
        "event_ids": [{"event_id": event.event_id, "event_type": event.event_type, "sequence": event.sequence} for event in events],
        "config": context.get("config"),
        "quotes": by_type.get("quote_observed", []),
        "pricing_decisions": by_type.get("pricing_decision_created", []),
        "pre_open_reconciliation": by_stage.get("PRE_OPEN"),
        "open_intent": intents[0] if intents else None,
        "open_submit_attempt": submits[0] if submits else None,
        "open_broker_order": broker_orders[0] if broker_orders else None,
        "open_fill": fills[0] if fills else None,
        "post_open_reconciliation": by_stage.get("POST_OPEN"),
        "pre_close_reconciliation": by_stage.get("PRE_CLOSE"),
        "close_intent": intents[1] if len(intents) > 1 else None,
        "close_submit_attempt": submits[1] if len(submits) > 1 else None,
        "close_broker_order": broker_orders[1] if len(broker_orders) > 1 else None,
        "close_fill": fills[1] if len(fills) > 1 else None,
        "post_close_reconciliation": by_stage.get("POST_CLOSE"),
        "final_reconciliation": by_stage.get("FINAL"),
        "cancel_attempts": by_type.get("cancel_attempt_created", []),
        "broker_errors": context.get("broker_errors", []),
        "missing_callbacks": context.get("missing_callbacks", []),
        "account_checks": first("account_validated"),
        "risk_gate_checks": gate_checks,
        "failure_or_ambiguity": context.get("failure_or_ambiguity"),
        "required_manual_action": context.get("required_manual_action"),
        "ledger_path": str(ledger_path),
        "markdown_report_path": str(proof_md),
        "json_report_path": str(proof_json),
    }


def _render_markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Track B Paper Proof Report",
        "",
        "## Classification",
        str(report["classification"]),
        "",
        "## Environment",
        json.dumps(report.get("environment"), indent=2, sort_keys=True),
        "",
        "## Account Validation",
        json.dumps(report.get("account_checks"), indent=2, sort_keys=True),
        "",
        "## Contract",
        json.dumps(report.get("contract"), indent=2, sort_keys=True),
        "",
        "## Quote And Pricing",
        json.dumps({"quotes": report.get("quotes"), "pricing_decisions": report.get("pricing_decisions")}, indent=2, sort_keys=True),
        "",
        "## Event Chain",
        json.dumps(report.get("event_ids"), indent=2, sort_keys=True),
        "",
        "## Open Order",
        json.dumps(
            {
                "intent": report.get("open_intent"),
                "submit": report.get("open_submit_attempt"),
                "broker_order": report.get("open_broker_order"),
                "fill": report.get("open_fill"),
            },
            indent=2,
            sort_keys=True,
        ),
        "",
        "## Post-Open Reconciliation",
        json.dumps(report.get("post_open_reconciliation"), indent=2, sort_keys=True),
        "",
        "## Close Order",
        json.dumps(
            {
                "intent": report.get("close_intent"),
                "submit": report.get("close_submit_attempt"),
                "broker_order": report.get("close_broker_order"),
                "fill": report.get("close_fill"),
            },
            indent=2,
            sort_keys=True,
        ),
        "",
        "## Final Reconciliation",
        json.dumps(report.get("final_reconciliation"), indent=2, sort_keys=True),
        "",
    ]
    if report.get("cancel_attempts"):
        lines.extend(["## Cancel Attempt", json.dumps(report.get("cancel_attempts"), indent=2, sort_keys=True), ""])
    lines.extend(
        [
            "## Broker Errors And Missing Callbacks",
            json.dumps({"broker_errors": report.get("broker_errors"), "missing_callbacks": report.get("missing_callbacks")}, indent=2, sort_keys=True),
            "",
        ]
    )
    if report.get("failure_or_ambiguity"):
        lines.extend(["## Failure Or Ambiguity", str(report.get("failure_or_ambiguity")), ""])
    if report.get("required_manual_action"):
        lines.extend(["## Required Manual Action", str(report.get("required_manual_action")), ""])
    lines.extend(
        [
            "## Ledger Files",
            json.dumps({"ledger_path": report.get("ledger_path"), "markdown_report_path": report.get("markdown_report_path")}, indent=2, sort_keys=True),
            "",
        ]
    )
    return "\n".join(lines)


def _validate_config(config: HarnessConfig) -> list[str]:
    errors: list[str] = []
    if config.mode != "PAPER":
        errors.append("mode must be PAPER")
    if config.host != "127.0.0.1":
        errors.append("host must be 127.0.0.1")
    if int(config.port) != 7497:
        errors.append("port must be 7497")
    if int(config.client_id) <= 0:
        errors.append("client_id must be a positive integer")
    if not str(config.account_id or "").strip():
        errors.append("account_id is required")
    if config.contract_key not in config.contract_allowlist:
        errors.append("contract_key must be explicitly allowlisted")
    if str(config.side).upper() not in {"BUY", "SELL"}:
        errors.append("side must be BUY or SELL")
    if int(config.quantity) != 1:
        errors.append("quantity must be exactly 1")
    if config.order_type != "LMT":
        errors.append("order_type must be LMT")
    if config.time_in_force != "DAY":
        errors.append("time_in_force must be DAY")
    if not config.confirm_paper_only:
        errors.append("confirm_paper_only must be true")
    return errors
