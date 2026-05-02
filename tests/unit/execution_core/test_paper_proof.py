from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.harness import HarnessConfig
from mgc_v05l.execution_core.models import Action, BrokerOrder, FillEvent, OrderIntent, SubmitAttempt, TerminalClassification
from mgc_v05l.execution_core.paper_proof import PaperProofConfig, run_ibkr_paper_proof, run_paper_proof
from mgc_v05l.execution_core.preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def config(tmp_path: Path, **overrides: object) -> PaperProofConfig:
    kwargs = {
        "output_root": tmp_path / "paper_proof",
        "account_id": "DUM882026",
        "client_id": 17077,
        "submit_enabled": True,
        "confirm_paper_submit": True,
        "allow_delayed_data_for_paper_proof": True,
    }
    kwargs.update(overrides)
    return PaperProofConfig(**kwargs)


def ready_preflight(tmp_path: Path, **overrides: object) -> PreflightResult:
    report = {
        "classification": "READY_READ_ONLY",
        "config": {"account_id": "DUM882026"},
        "contract_key": "MGC-202606",
        "checks": [
            {"name": "managed_account_exact_match", "passed": True},
            {"name": "contract_qualified", "passed": True},
        ],
        "position": {"signed_quantity": 0},
        "open_orders": [],
        "quote_observed": True,
        "market_data_provider": "IBKR",
        "market_data_mode": "DELAYED",
        "market_data_role": "DIAGNOSTIC",
        "delayed_data_warning_seen": True,
        "paper_route_readiness": True,
        "production_live_money_readiness": False,
    }
    report.update(overrides)
    return PreflightResult(
        run_id="preflight-1",
        classification=PreflightClassification(str(report["classification"])),
        report_json=tmp_path / "preflight_report.json",
        report_md=tmp_path / "preflight_report.md",
        report=report,
    )


def preflight_runner(result: PreflightResult):
    def run(config: ReadOnlyPreflightConfig, run_id: str) -> PreflightResult:  # noqa: ARG001
        return result

    return run


def passing_proof_runner(tmp_path: Path):
    def run(config, run_id):  # type: ignore[no-untyped-def]
        from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter
        from mgc_v05l.execution_core.harness import run_fake_paper_proof

        return run_fake_paper_proof(
            config=config,
            adapter=FakePaperAdapter(account_id=config.account_id, contract_key=config.contract_key, scenario="pass"),
            run_id=run_id,
            now=aware_now(),
        )

    return run


class RecordingPaperAdapter:
    def __init__(self, *, fail_open_order_wait: bool = False) -> None:
        self.submitted: list[OrderIntent] = []
        self.connected = False
        self.disconnected = False
        self.fail_open_order_wait = fail_open_order_wait

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.disconnected = True

    def managed_accounts(self) -> tuple[str, ...]:
        return ("DUM882026",)

    def require_configured_account(self) -> str:
        return "DUM882026"

    def qualify_contract(self, *, run_id: str, contract_key: str, now: datetime):  # type: ignore[no-untyped-def]
        return {"run_id": run_id, "contract_key": contract_key, "local_symbol": "MGCM6"}

    def submit_limit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent) -> int:
        self.submitted.append(order_intent)
        return len(self.submitted)

    def wait_for_broker_order(self, *, submit_attempt_id: str) -> BrokerOrder:
        if self.fail_open_order_wait and submit_attempt_id.endswith("_1"):
            raise RuntimeError("missing openOrder/orderStatus callback")
        intent = self._intent_for(submit_attempt_id)
        index = len([item for item in self.submitted if item.order_intent_id <= intent.order_intent_id])
        return BrokerOrder(
            broker_order_event_id=f"broker_order_{submit_attempt_id}",
            run_id=intent.run_id,
            submit_attempt_id=submit_attempt_id,
            account_id=intent.account_id,
            broker_order_id=f"FAKE-ORDER-{index:04d}",
            perm_id=f"FAKE-PERM-{index:04d}",
            client_id=17077,
            contract_key=intent.contract_key,
            action=intent.action,
            quantity=1,
            order_type="LMT",
            limit_price=intent.limit_price,
            status="Filled",
            filled_quantity=1,
            remaining_quantity=0,
            average_fill_price=intent.limit_price,
            observed_at=aware_now(),
        )

    def wait_for_fill(self, *, submit_attempt_id: str) -> FillEvent:
        intent = self._intent_for(submit_attempt_id)
        index = len([item for item in self.submitted if item.order_intent_id <= intent.order_intent_id])
        return FillEvent(
            fill_event_id=f"fill_{submit_attempt_id}",
            run_id=intent.run_id,
            submit_attempt_id=submit_attempt_id,
            order_intent_id=intent.order_intent_id,
            account_id=intent.account_id,
            broker_order_id=f"FAKE-ORDER-{index:04d}",
            perm_id=f"FAKE-PERM-{index:04d}",
            execution_id=f"FAKE-EXEC-{index:04d}",
            contract_key=intent.contract_key,
            action=intent.action,
            quantity=1,
            price=intent.limit_price,
            filled_at=aware_now(),
        )

    def _intent_for(self, submit_attempt_id: str) -> OrderIntent:
        index = 0 if submit_attempt_id.endswith("_1") else 1
        return self.submitted[index]

    def submit_diagnostics(self, submit_attempt_id: str) -> dict[str, object]:
        intent = self._intent_for(submit_attempt_id)
        return {
            "submit_attempt_id": submit_attempt_id,
            "place_order_called": True,
            "place_order_called_at": aware_now().isoformat(),
            "broker_order_id_allocated": "1" if submit_attempt_id.endswith("_1") else "2",
            "order_transmit_flag": True,
            "order_action": intent.action.value,
            "order_type": intent.order_type,
            "limit_price": str(intent.limit_price),
            "tif": intent.time_in_force,
            "client_id": 17077,
            "account_id": intent.account_id,
            "contract_key": intent.contract_key,
            "contract_local_symbol": "MGCM6",
            "contract_con_id": 712565978,
            "callback_wait_timeout_seconds": 30.0,
            "openOrder_seen": False,
            "orderStatus_seen": False,
            "execDetails_seen": False,
            "completedOrder_seen": False,
            "error_callbacks_after_submit": [
                {
                    "request_id": 1,
                    "error_code": 10268,
                    "error_string": "The 'EtradeOnly' order attribute is not supported.",
                    "raw_args": [],
                }
            ],
            "isConnected_before_placeOrder": True,
            "isConnected_after_placeOrder": True,
            "isConnected_after_callback_wait": True,
        }


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"submit_enabled": False}, "submit_enabled=True"),
        ({"confirm_paper_submit": False}, "confirm_paper_submit"),
        ({"mode": "LIVE"}, "mode must be PAPER"),
        ({"host": "localhost"}, "host must be 127.0.0.1"),
        ({"port": 7496}, "port must be 7497"),
        ({"quantity": 2}, "quantity must be exactly 1"),
        ({"order_type": "MKT"}, "order_type must be LMT"),
        ({"time_in_force": "GTC"}, "time_in_force must be DAY"),
    ],
)
def test_paper_proof_rejects_unsafe_config(tmp_path: Path, overrides: dict[str, object], reason: str) -> None:
    result = run_paper_proof(
        config=config(tmp_path, **overrides),
        preflight_runner=preflight_runner(ready_preflight(tmp_path)),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-config-block",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert reason in str(result.report["failure_or_ambiguity"])


def test_blocks_when_preflight_not_ready(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, classification="BLOCKED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-preflight-block",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "preflight was not ready" in str(result.report["failure_or_ambiguity"])


@pytest.mark.parametrize(
    ("report_overrides", "reason"),
    [
        ({"config": {"account_id": "OTHER"}}, "account_id"),
        ({"checks": [{"name": "managed_account_exact_match", "passed": False}]}, "paper account"),
        ({"contract_key": "MNQ-202606"}, "contract_key"),
        ({"checks": [{"name": "managed_account_exact_match", "passed": True}, {"name": "contract_qualified", "passed": False}]}, "contract"),
        ({"position": {"signed_quantity": 1}}, "not flat"),
        ({"open_orders": [{"broker_order_id": "1001"}]}, "open orders"),
        ({"account_open_orders": [{"broker_order_id": "2001"}]}, "account-wide"),
    ],
)
def test_blocks_on_preflight_account_contract_position_or_order_mismatch(
    tmp_path: Path,
    report_overrides: dict[str, object],
    reason: str,
) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, **report_overrides)),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-preflight-mismatch",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert reason in str(result.report["failure_or_ambiguity"])


def test_delayed_market_data_requires_explicit_paper_approval(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, allow_delayed_data_for_paper_proof=False),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-delayed-not-approved",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "delayed market data" in str(result.report["failure_or_ambiguity"])


def test_missing_or_unknown_quote_blocks_pricing_dependent_proof(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="UNKNOWN")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-missing-quote",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "observed quote" in str(result.report["failure_or_ambiguity"])


def test_missing_quote_with_valid_manual_open_close_prices_proceeds_as_paper_only_manual_price(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, manual_open_limit_price="2345.1", manual_close_limit_price="2344.9"),
        preflight_runner=preflight_runner(
            ready_preflight(
                tmp_path,
                quote=None,
                quote_observed=False,
                market_data_provider="IBKR",
                market_data_mode="DELAYED",
                market_data_role="DIAGNOSTIC",
                delayed_data_warning_seen=True,
                production_live_money_readiness=False,
            )
        ),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-manual-price",
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.classification == TerminalClassification.PASSED
    assert payload["manual_open_limit_price"] == "2345.1"
    assert payload["manual_close_limit_price"] == "2344.9"
    assert payload["pricing_source"] == "OPERATOR_SUPPLIED_MANUAL_LIMIT"
    assert payload["operator_manual_price_acknowledgement"] is True
    assert payload["market_data_provider"] == "IBKR"
    assert payload["market_data_mode"] == "DELAYED"
    assert payload["quote_observed"] is False
    assert payload["paper_route_readiness"] is True
    assert payload["production_live_money_readiness"] is False


@pytest.mark.parametrize(
    ("manual_open_limit_price", "manual_close_limit_price", "reason"),
    [
        ("0", "2344.9", "positive"),
        ("-1", "2344.9", "positive"),
        ("not-a-number", "2344.9", "positive decimal"),
        ("2345.15", "2344.9", "tick_size"),
        ("2345.1", "2344.95", "tick_size"),
    ],
)
def test_invalid_manual_limit_price_blocks_before_proof_runner(
    tmp_path: Path,
    manual_open_limit_price: str,
    manual_close_limit_price: str,
    reason: str,
) -> None:
    called = False

    def proof_runner(config, run_id):  # type: ignore[no-untyped-def]
        nonlocal called
        called = True
        return passing_proof_runner(tmp_path)(config, run_id)

    result = run_paper_proof(
        config=config(
            tmp_path,
            manual_open_limit_price=manual_open_limit_price,
            manual_close_limit_price=manual_close_limit_price,
        ),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="DELAYED")),
        proof_runner=proof_runner,
        run_id="run-invalid-manual-price",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert reason in str(result.report["failure_or_ambiguity"])
    assert called is False


def test_manual_limit_prices_require_delayed_data_paper_approval(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(
            tmp_path,
            manual_open_limit_price="2345.1",
            manual_close_limit_price="2344.9",
            allow_delayed_data_for_paper_proof=False,
        ),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-manual-price-no-approval",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "delayed-data paper-proof approval" in str(result.report["failure_or_ambiguity"])


def test_manual_pricing_requires_both_open_and_close_prices(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, manual_open_limit_price="2345.1"),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-manual-one-price",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "manual_close_limit_price is required" in str(result.report["failure_or_ambiguity"])


def test_deprecated_single_manual_limit_price_is_rejected(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, manual_limit_price="2345.1"),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-manual-deprecated",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "deprecated" in str(result.report["failure_or_ambiguity"])


@pytest.mark.parametrize(
    ("side", "open_action", "close_action"),
    [
        ("BUY", Action.BUY, Action.SELL),
        ("SELL", Action.SELL, Action.BUY),
    ],
)
def test_real_runner_uses_separate_manual_open_and_close_prices(
    tmp_path: Path,
    side: str,
    open_action: Action,
    close_action: Action,
) -> None:
    adapter = RecordingPaperAdapter()

    result = run_ibkr_paper_proof(
        config=HarnessConfig(
            account_id="DUM882026",
            client_id=17077,
            side=side,
            output_root=tmp_path / "proof_runs",
        ),
        run_id=f"run-real-manual-{side.lower()}",
        preflight=ready_preflight(
            tmp_path,
            quote=None,
            quote_observed=False,
            market_data_mode="DELAYED",
            market_data_provider="IBKR",
            market_data_role="DIAGNOSTIC",
            production_live_money_readiness=False,
        ),
        manual_open_limit_price="2345.1",
        manual_close_limit_price="2344.9",
        adapter=adapter,  # type: ignore[arg-type]
    )

    assert result.classification == TerminalClassification.PASSED
    assert adapter.connected is True
    assert adapter.disconnected is True
    assert [(intent.action, str(intent.limit_price)) for intent in adapter.submitted] == [
        (open_action, "2345.1"),
        (close_action, "2344.9"),
    ]
    assert all(intent.order_type == "LMT" and intent.time_in_force == "DAY" for intent in adapter.submitted)


def test_real_runner_reports_submit_diagnostics_when_open_callbacks_are_missing(tmp_path: Path) -> None:
    adapter = RecordingPaperAdapter(fail_open_order_wait=True)

    result = run_ibkr_paper_proof(
        config=HarnessConfig(
            account_id="DUM882026",
            client_id=17077,
            output_root=tmp_path / "proof_runs",
        ),
        run_id="run-open-callback-missing",
        preflight=ready_preflight(
            tmp_path,
            quote=None,
            quote_observed=False,
            market_data_mode="DELAYED",
            market_data_provider="IBKR",
            market_data_role="DIAGNOSTIC",
            production_live_money_readiness=False,
        ),
        manual_open_limit_price="2345.1",
        manual_close_limit_price="2344.9",
        adapter=adapter,  # type: ignore[arg-type]
    )
    payload = json.loads(result.proof_report_json.read_text(encoding="utf-8"))
    diagnostics = payload["open_submit_diagnostics"]

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert len(adapter.submitted) == 1
    assert payload["close_submit_attempt"] is None
    assert diagnostics["place_order_called"] is True
    assert diagnostics["broker_order_id_allocated"] == "1"
    assert diagnostics["order_transmit_flag"] is True
    assert diagnostics["order_action"] == "BUY"
    assert diagnostics["order_type"] == "LMT"
    assert diagnostics["limit_price"] == "2345.1"
    assert diagnostics["tif"] == "DAY"
    assert diagnostics["callback_wait_timeout_seconds"] == 30.0
    assert diagnostics["openOrder_seen"] is False
    assert diagnostics["orderStatus_seen"] is False
    assert diagnostics["execDetails_seen"] is False
    assert diagnostics["completedOrder_seen"] is False
    assert diagnostics["error_callbacks_after_submit"][0]["error_code"] == 10268
    assert "EtradeOnly" in diagnostics["error_callbacks_after_submit"][0]["error_string"]


def test_delayed_data_can_pass_paper_proof_but_not_live_money_readiness(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, allow_delayed_data_for_paper_proof=True),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-delayed-pass",
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.classification == TerminalClassification.PASSED
    assert payload["market_data_mode"] == "DELAYED"
    assert payload["paper_route_readiness"] is True
    assert payload["production_live_money_readiness"] is False


def test_missing_broker_correlation_downgrades_pass_to_ambiguous(tmp_path: Path) -> None:
    def proof_runner(config, run_id):  # type: ignore[no-untyped-def]
        from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter, FakeSubmitResult
        from mgc_v05l.execution_core.harness import run_fake_paper_proof

        class MissingBrokerOrderAdapter(FakePaperAdapter):
            def submit_order(self, **kwargs):  # type: ignore[no-untyped-def]
                result = super().submit_order(**kwargs)
                if result.fill_event is not None:
                    return FakeSubmitResult(
                        broker_order=None,
                        fill_event=result.fill_event,
                        broker_position=result.broker_position,
                        open_orders=result.open_orders,
                        missing_callbacks=result.missing_callbacks,
                        ambiguous=result.ambiguous,
                        failure_reason=result.failure_reason,
                    )
                return result

        return run_fake_paper_proof(
            config=config,
            adapter=MissingBrokerOrderAdapter(account_id=config.account_id, contract_key=config.contract_key, scenario="pass"),
            run_id=run_id,
            now=aware_now(),
        )

    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path)),
        proof_runner=proof_runner,
        run_id="run-missing-correlation",
    )

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
