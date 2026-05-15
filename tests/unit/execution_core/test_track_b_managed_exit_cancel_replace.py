from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.models import BrokerOrder, FillEvent
from mgc_v05l.execution_core.track_b_managed_exit_cancel_replace import (
    GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH,
    GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND,
    GUARDED_CANCEL_REPLACE_READY,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING,
    GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED,
    ManagedExitCancelReplaceConfig,
    run_guarded_managed_exit_cancel_replace,
)


NOW = datetime(2026, 5, 15, 11, 0, tzinfo=timezone.utc)


def test_exact_known_managed_order_proposal_is_accepted(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_READY
    assert report["broker_mutation_performed"] is False
    assert report["ready"]["proposal"]["allowed_route"] == "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY"


def test_dry_run_does_not_construct_adapter(tmp_path: Path) -> None:
    def _raise_if_called(**_kwargs: Any) -> _FakeAdapter:
        raise AssertionError("dry-run must not construct a broker adapter")

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=_raise_if_called,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_READY
    assert report["broker_mutation_attempted"] is False
    assert report["broker_mutation_performed"] is False


def test_unknown_open_order_proposal_is_refused(tmp_path: Path) -> None:
    reconciliation = _reconciliation_report(known_orders=[])
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: reconciliation,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH
    assert "not a known managed exit order" in report["detail"]


def test_mismatched_broker_order_id_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, broker_order_id="2"),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND


def test_mismatched_client_or_perm_id_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, client_id=999),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH


def test_mismatched_contract_identity_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(open_order_overrides={"local_symbol": "MGCM6"}),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH
    assert "local_symbol mismatch" in report["detail"]


def test_live_money_or_paper_proof_state_is_refused(tmp_path: Path) -> None:
    live_report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(live_money_eligible=True),
    )
    proof_report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(paper_proof_invoked=True),
    )

    assert live_report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert proof_report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED


def test_apply_cancels_exact_order_and_persists_working_replacement(tmp_path: Path) -> None:
    fake = _FakeAdapter(fill=None)
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING
    assert report["broker_mutation_performed"] is True
    assert fake.cancelled_order_ids == ["1"]
    assert fake.broad_cancel_called is False
    assert fake.submitted_intents[0].action.value == "SELL"
    state_path = tmp_path / "outputs" / "track_b_execution_core" / "managed_exit_orders" / "latest_known_managed_exit_orders.json"
    assert state_path.exists()
    assert "2" in state_path.read_text(encoding="utf-8")


def test_replacement_fill_persists_lifecycle_close(tmp_path: Path) -> None:
    fake = _FakeAdapter(fill=_fill())
    ledger_calls: list[dict[str, Any]] = []

    @dataclass(frozen=True)
    class _LedgerResult:
        trade_record_written: bool = True
        ledger_jsonl: Path = tmp_path / "ledger.jsonl"

    def _ledger_update(**kwargs: Any) -> _LedgerResult:
        ledger_calls.append(dict(kwargs))
        return _LedgerResult()

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
        ledger_update_runner=_ledger_update,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED
    assert report["lifecycle_close"]["persisted"] is True
    assert ledger_calls[0]["filled_bridge_result"]["intent_type"] == "SELL_TO_CLOSE"
    assert ledger_calls[0]["filled_bridge_result"]["broker_order_id"] == "2"


def test_cancel_succeeded_replacement_failed_is_explicit_review_classification(tmp_path: Path) -> None:
    fake = _FakeAdapter(replacement_error=RuntimeError("submit rejected"))
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED
    assert "submit rejected" in report["detail"]
    assert fake.cancelled_order_ids == ["1"]


def test_original_order_already_gone_does_not_blind_replace(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(open_orders=[]),
        adapter_factory=lambda **_kwargs: _FakeAdapter(fill=None),
    )

    assert report["classification"] == "GUARDED_CANCEL_REPLACE_CANCELLED_OR_FILLED_BEFORE_ACTION"


def _config(tmp_path: Path, **overrides: Any) -> ManagedExitCancelReplaceConfig:
    payload = {
        "repo_root": tmp_path,
        "broker_order_id": "1",
        "client_id": 10815,
        "perm_id": 614029377,
    }
    payload.update(overrides)
    return ManagedExitCancelReplaceConfig(**payload)


def _reconciliation_report(
    *,
    known_orders: list[dict[str, Any]] | None = None,
    open_orders: list[dict[str, Any]] | None = None,
    open_order_overrides: dict[str, Any] | None = None,
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
) -> dict[str, Any]:
    open_order = {
        "account_id": "DUM882026",
        "broker_order_id": 1,
        "client_id": 10815,
        "perm_id": 614029377,
        "symbol": "GC",
        "local_symbol": "GCM6",
        "expiry": "20260626",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1.0",
        "status": "Submitted",
    }
    open_order.update(open_order_overrides or {})
    known_order = {
        **open_order,
        "managed_order_status": "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lane_id": "gc_1x_all_lanes__london_early_long",
        "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
        "exit_reason": "forced_session_initial_stop",
        "order_type": "LMT",
        "limit_price": 4574.7,
        "tif": "DAY",
        "managed_order_policy": {
            "classification": "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
            "recommended_action": "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER",
        },
        "guarded_cancel_replace_proposal": {
            "enabled": False,
            "requires_explicit_operator_authorization": True,
            "broker_mutation_performed": False,
            "allowed_route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
            "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
            "cancel_identity": {
                "account_id": "DUM882026",
                "broker_order_id": 1,
                "client_id": 10815,
                "perm_id": 614029377,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "quantity": "1.0",
            },
            "replacement_order": {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "quantity": "1.0",
                "order_type": "LMT",
                "tif": "DAY",
                "limit_price": 4564.9,
                "min_tick": 0.1,
            },
        },
    }
    actual_open_orders = [open_order] if open_orders is None else open_orders
    return {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER",
        "broker_reconciled": True,
        "live_money_eligible": live_money_eligible,
        "paper_proof_invoked": paper_proof_invoked,
        "review_required_count": 0,
        "unknown_broker_open_order_count": 0,
        "track_b_broker_open_order_count": len(actual_open_orders),
        "track_b_broker_open_orders": actual_open_orders,
        "track_b_broker_position_count": 2,
        "lifecycle_open_position_count": 2,
        "known_managed_exit_order_count": len([known_order] if known_orders is None else known_orders),
        "known_managed_exit_orders": [known_order] if known_orders is None else known_orders,
    }


class _FakeAdapter:
    def __init__(self, *, fill: FillEvent | None = None, replacement_error: Exception | None = None) -> None:
        self.fill = fill
        self.replacement_error = replacement_error
        self.cancelled_order_ids: list[str] = []
        self.submitted_intents: list[Any] = []
        self.broad_cancel_called = False

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> tuple[str, ...]:
        return ("DUM882026",)
    def require_configured_account(self) -> str:
        return "DUM882026"
    def register_existing_order_for_cancel(self, **_kwargs: Any) -> None: ...
    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None:
        self.cancelled_order_ids.append(str(broker_order_id))
    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None: ...
    def submit_limit_order(self, *, submit_attempt: Any, order_intent: Any) -> int:
        if self.replacement_error is not None:
            raise self.replacement_error
        self.submitted_intents.append(order_intent)
        return 2
    def wait_for_broker_order(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> BrokerOrder:
        return BrokerOrder(
            broker_order_event_id="broker-order-2",
            run_id="run",
            submit_attempt_id=submit_attempt_id,
            account_id="DUM882026",
            broker_order_id="2",
            perm_id="614029400",
            client_id=10941,
            contract_key="GC-202606",
            action="SELL",
            quantity=1,
            order_type="LMT",
            limit_price="4564.9",
            status="Submitted",
            filled_quantity=0,
            remaining_quantity=1,
            average_fill_price=None,
            observed_at=NOW,
        )
    def wait_for_fill(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> FillEvent:
        if self.fill is None:
            raise TimeoutError("still working")
        return self.fill
    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]:
        return {}


def _fill() -> FillEvent:
    return FillEvent(
        fill_event_id="fill-2",
        run_id="run",
        submit_attempt_id="replacement",
        order_intent_id="replacement",
        account_id="DUM882026",
        broker_order_id="2",
        perm_id="614029400",
        execution_id="exec-2",
        contract_key="GC-202606",
        action="SELL",
        quantity=1,
        price="4564.9",
        filled_at=NOW,
    )
