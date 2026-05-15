"""Resolve known Track B PAPER leak-test entry orders.

This module is intentionally narrow. It may cancel exactly one known leak-test
entry order that reconciliation has already attributed; it never broad-cancels
and never acts on unknown broker orders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .ibkr_paper_adapter import IbkrPaperAdapter
from .models import IntentKind, OrderIntent, SubmitAttempt, SubmitAttemptState, to_jsonable
from .track_b_paper_broker_reconciliation import (
    PAPER_ACCOUNT,
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)


LEAK_TEST_ENTRY_ORDER_CANCEL_READY = "LEAK_TEST_ENTRY_ORDER_CANCEL_READY"
LEAK_TEST_ENTRY_ORDER_NOT_FOUND = "LEAK_TEST_ENTRY_ORDER_NOT_FOUND"
LEAK_TEST_ENTRY_ORDER_IDENTITY_MISMATCH = "LEAK_TEST_ENTRY_ORDER_IDENTITY_MISMATCH"
LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED = "LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED"
LEAK_TEST_ENTRY_ORDER_FILLED_BEFORE_CANCEL = "LEAK_TEST_ENTRY_ORDER_FILLED_BEFORE_CANCEL"
LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED = "LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED"

DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "track_b_leak_test_entry_order_resolution"


class LeakTestEntryOrderAdapter(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> tuple[str, ...]: ...
    def require_configured_account(self) -> str: ...
    def register_existing_order_for_cancel(
        self,
        *,
        submit_attempt: SubmitAttempt,
        order_intent: OrderIntent,
        broker_order_id: str,
        created_at: datetime,
    ) -> None: ...
    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None: ...
    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None: ...
    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]: ...


@dataclass(frozen=True)
class LeakTestEntryOrderResolutionConfig:
    repo_root: Path
    broker_order_id: str
    client_id: int | None = None
    perm_id: int | None = None
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    tws_client_id: int = 10942
    account_id: str = PAPER_ACCOUNT
    apply: bool = False
    output_dir: Path = DEFAULT_REPORT_DIR
    cancel_timeout_seconds: float = 30.0

    @property
    def resolved_output_dir(self) -> Path:
        return self.repo_root / self.output_dir if not self.output_dir.is_absolute() else self.output_dir


def resolve_known_leak_test_entry_order(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    now: datetime | None = None,
    adapter_factory: Callable[..., LeakTestEntryOrderAdapter] | None = None,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None = None,
    broker_truth_refresh: Callable[[], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    if broker_truth_refresh is not None:
        broker_truth_refresh()
    reconciliation = _run_reconciliation(config=config, reconciliation_runner=reconciliation_runner)
    ready = _validate_ready(config=config, reconciliation=reconciliation, now=actual_now)
    report = _base_report(config=config, now=actual_now, reconciliation=reconciliation, ready=ready)
    if ready["classification"] != LEAK_TEST_ENTRY_ORDER_CANCEL_READY:
        _write_report(config, report)
        return report
    if not config.apply:
        report["detail"] = "Known leak-test entry order cancel is ready; apply=false so no broker mutation was attempted."
        _write_report(config, report)
        return report

    adapter_factory = adapter_factory or _default_adapter_factory
    adapter = adapter_factory(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.tws_client_id,
        account_id=config.account_id,
        contract_allowlist={ready["contract_key"]: _contract_allowlist_entry(ready["known_order"])},
        submit_enabled=True,
    )
    try:
        adapter.connect()
        adapter.managed_accounts()
        adapter.require_configured_account()
        order_intent = _cancel_order_intent(config=config, ready=ready, now=actual_now)
        submit_attempt = _submit_attempt(config=config, order_intent=order_intent, ready=ready, now=actual_now)
        adapter.register_existing_order_for_cancel(
            submit_attempt=submit_attempt,
            order_intent=order_intent,
            broker_order_id=str(config.broker_order_id),
            created_at=actual_now,
        )
        adapter.cancel_order(submit_attempt_id=submit_attempt.submit_attempt_id, broker_order_id=str(config.broker_order_id))
        adapter.wait_for_cancel(submit_attempt_id=submit_attempt.submit_attempt_id, timeout_seconds=config.cancel_timeout_seconds)
        report["classification"] = LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED
        report["cancel"] = {
            "classification": LEAK_TEST_ENTRY_ORDER_CANCEL_CONFIRMED,
            "broker_order_id": str(config.broker_order_id),
            "client_id": config.client_id,
            "perm_id": config.perm_id,
            "cancelled_at": datetime.now(UTC).isoformat(),
        }
        report["adapter_diagnostics"] = adapter.submit_diagnostics(submit_attempt.submit_attempt_id)
        _write_report(config, report)
        return report
    except Exception as exc:  # noqa: BLE001 - fail closed and preserve diagnostics.
        report["classification"] = LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED
        report["detail"] = str(exc)
        try:
            report["adapter_diagnostics"] = adapter.submit_diagnostics(None)
        except Exception:  # noqa: BLE001
            pass
        _write_report(config, report)
        return report
    finally:
        try:
            adapter.disconnect()
        except Exception:  # noqa: BLE001
            pass


def _run_reconciliation(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None,
) -> Mapping[str, Any]:
    recon_config = ReconciliationConfig(repo_root=config.repo_root)
    if reconciliation_runner is not None:
        return reconciliation_runner(recon_config)
    return reconcile_track_b_paper_broker_truth(config=recon_config)


def _validate_ready(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    reconciliation: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    if reconciliation.get("live_money_eligible") is True:
        return _not_ready("live_money_eligible=true")
    if reconciliation.get("paper_proof_invoked") is True:
        return _not_ready("paper_proof_invoked=true")
    if int(reconciliation.get("review_required_count") or 0) != 0:
        return _not_ready("review_required_count is nonzero")
    if int(reconciliation.get("unknown_broker_open_order_count") or 0) != 0:
        return _not_ready("unknown broker open orders are present")
    known_order = _known_order(config=config, reconciliation=reconciliation)
    if known_order is None:
        if int(reconciliation.get("track_b_broker_position_count") or 0) != 0:
            return {"classification": LEAK_TEST_ENTRY_ORDER_FILLED_BEFORE_CANCEL, "detail": "Broker position exists; the entry order may have filled before cancel."}
        return {"classification": LEAK_TEST_ENTRY_ORDER_NOT_FOUND, "detail": "The target known leak-test entry order is not working in reconciliation."}
    mismatch = _identity_mismatch(config=config, known_order=known_order)
    if mismatch:
        return {"classification": LEAK_TEST_ENTRY_ORDER_IDENTITY_MISMATCH, "detail": "; ".join(mismatch), "known_order": known_order}
    return {
        "classification": LEAK_TEST_ENTRY_ORDER_CANCEL_READY,
        "validated_at": now.isoformat(),
        "known_order": known_order,
        "contract_key": _contract_key(known_order),
    }


def _known_order(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    reconciliation: Mapping[str, Any],
) -> dict[str, Any] | None:
    for row in reconciliation.get("known_leak_test_entry_orders") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("broker_order_id") or row.get("order_id") or "") == str(config.broker_order_id):
            return dict(row)
    return None


def _identity_mismatch(*, config: LeakTestEntryOrderResolutionConfig, known_order: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    if str(known_order.get("account_id") or PAPER_ACCOUNT) != config.account_id:
        failures.append("account_id mismatch")
    if config.client_id is not None and _int_or_none(known_order.get("client_id")) not in {None, config.client_id}:
        failures.append("client_id mismatch")
    if config.perm_id is not None and _int_or_none(known_order.get("perm_id")) not in {None, config.perm_id}:
        failures.append("perm_id mismatch")
    action = str(known_order.get("action") or "").upper()
    if action not in {"BUY", "SELL"}:
        failures.append("entry action missing or invalid")
    quantity = _float_or_none(known_order.get("quantity") or known_order.get("qty"))
    if quantity is not None and abs(quantity) != 1.0:
        failures.append("quantity is not exactly 1")
    return failures


def _not_ready(detail: str) -> dict[str, Any]:
    return {"classification": LEAK_TEST_ENTRY_ORDER_REVIEW_REQUIRED, "detail": detail}


def _base_report(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    now: datetime,
    reconciliation: Mapping[str, Any],
    ready: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_leak_test_entry_order_resolution_v1",
        "generated_at": now.isoformat(),
        "classification": ready.get("classification"),
        "detail": ready.get("detail"),
        "account_id": config.account_id,
        "broker_order_id": str(config.broker_order_id),
        "client_id": config.client_id,
        "perm_id": config.perm_id,
        "apply": config.apply,
        "known_order": ready.get("known_order"),
        "reconciliation_classification": reconciliation.get("classification"),
        "unknown_broker_open_order_count": reconciliation.get("unknown_broker_open_order_count"),
        "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
        "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _cancel_order_intent(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    ready: Mapping[str, Any],
    now: datetime,
) -> OrderIntent:
    known = ready["known_order"]
    return OrderIntent(
        order_intent_id=f"leak_test_entry_cancel_existing_{config.broker_order_id}",
        signal_event_id=str(known.get("lane_id") or config.broker_order_id),
        run_id=f"leak_test_entry_cancel_{config.broker_order_id}",
        intent_kind=IntentKind.OPEN,
        account_id=config.account_id,
        symbol=str(known.get("symbol") or ""),
        contract_key=ready["contract_key"],
        action=str(known.get("action") or ""),
        quantity=known.get("quantity") or known.get("qty") or "1",
        order_type=str(known.get("order_type") or "LMT"),
        limit_price=known.get("limit_price") or "0",
        time_in_force=str(known.get("tif") or "DAY"),
        paper_only=True,
        created_at=now,
        reason="known_leak_test_entry_cancel",
        extra_fields={"guarded_leak_test_entry_order_resolution": "cancel_existing"},
    )


def _submit_attempt(
    *,
    config: LeakTestEntryOrderResolutionConfig,
    order_intent: OrderIntent,
    ready: Mapping[str, Any],
    now: datetime,
) -> SubmitAttempt:
    return SubmitAttempt(
        submit_attempt_id=f"guarded_leak_test_entry_cancel_{order_intent.run_id}_{int(now.timestamp())}",
        order_intent_id=order_intent.order_intent_id,
        run_id=order_intent.run_id,
        account_id=config.account_id,
        broker="IBKR",
        environment={
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.tws_client_id,
            "route": "GUARDED_TRACK_B_PAPER_LEAK_TEST_ENTRY_ORDER_RESOLUTION",
        },
        pre_submit_reconciliation_id=str(ready.get("validated_at") or "guarded_leak_test_entry_order_precheck"),
        open_order_baseline_event_id="guarded_leak_test_entry_order_open_orders",
        request_digest=f"{order_intent.order_intent_id}:{config.broker_order_id}",
        state=SubmitAttemptState.CREATED,
        submitted_at=now,
        broker_order_id=str(config.broker_order_id),
        perm_id=str(config.perm_id or ready["known_order"].get("perm_id") or "") or None,
    )


def _contract_allowlist_entry(known: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": known.get("symbol"),
        "local_symbol": known.get("local_symbol"),
        "expiry": known.get("expiry"),
        "con_id": known.get("con_id"),
        "security_type": known.get("security_type") or "FUT",
        "exchange": known.get("exchange") or "COMEX",
    }


def _contract_key(known: Mapping[str, Any]) -> str:
    symbol = str(known.get("symbol") or "").upper()
    expiry = str(known.get("expiry") or "")
    return f"{symbol}-{expiry[:6]}" if symbol and expiry else symbol


def _default_adapter_factory(**kwargs: Any) -> LeakTestEntryOrderAdapter:
    return IbkrPaperAdapter(**kwargs)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _report_path(config: LeakTestEntryOrderResolutionConfig) -> Path:
    return config.resolved_output_dir / "track_b_leak_test_entry_order_resolution_report.json"


def _write_report(config: LeakTestEntryOrderResolutionConfig, report: Mapping[str, Any]) -> None:
    path = _report_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")

