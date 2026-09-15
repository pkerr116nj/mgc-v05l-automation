from __future__ import annotations

import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mgc_v05l.ndxp_terminal.diagnostics import classify_diagnostics
from mgc_v05l.ndxp_terminal.orders import (
    LockedSchwabMutationGateway,
    NdxpSpreadRequest,
    SpreadValidationError,
    TransmissionDisabledError,
    build_vertical_order_payload,
    validate_spread_request,
)
from mgc_v05l.ndxp_terminal.server import DemoSchwabAdapter, main, run_server
from mgc_v05l.ndxp_terminal.service import NdxpTerminalService


def symbol(strike: int, option_type: str = "C", root: str = "NDXP") -> str:
    return f"{root:<6}260914{option_type}{strike * 1000:08d}"


def request(*, option_type: str = "C", short: int = 29330, long: int = 29340) -> NdxpSpreadRequest:
    return NdxpSpreadRequest(
        account_hash="hash-1",
        short_symbol=symbol(short, option_type),
        long_symbol=symbol(long, option_type),
        quantity=20,
        net_credit=Decimal("3.75"),
    )


def test_call_credit_vertical_payload_and_risk_are_exact() -> None:
    proposed = request()
    risk = validate_spread_request(proposed)
    payload = build_vertical_order_payload(proposed)

    assert risk["gross_width_dollars"] == "20000"
    assert risk["premium_dollars"] == "7500.00"
    assert risk["maximum_loss_dollars"] == "12500.00"
    assert payload["orderType"] == "NET_CREDIT"
    assert payload["complexOrderStrategyType"] == "VERTICAL"
    assert payload["price"] == "3.75"
    assert [leg["instruction"] for leg in payload["orderLegCollection"]] == ["SELL_TO_OPEN", "BUY_TO_OPEN"]


def test_put_credit_vertical_requires_short_higher_than_long() -> None:
    valid = request(option_type="P", short=29330, long=29320)
    assert validate_spread_request(valid)["option_type"] == "PUT"
    with pytest.raises(SpreadValidationError, match="sell the higher strike"):
        validate_spread_request(request(option_type="P", short=29320, long=29330))


@pytest.mark.parametrize(
    "proposed, message",
    [
        (request(short=29330, long=29350), "exact 10-point"),
        (request(short=29340, long=29330), "sell the lower strike"),
        (NdxpSpreadRequest("hash-1", symbol(29330, root="SPXW"), symbol(29340, root="SPXW"), 1, Decimal("1")), "NDX or NDXP"),
        (NdxpSpreadRequest("hash-1", symbol(29330), symbol(29340), 101, Decimal("1")), "between 1 and 100"),
    ],
)
def test_invalid_spreads_fail_closed(proposed: NdxpSpreadRequest, message: str) -> None:
    with pytest.raises(SpreadValidationError, match=message):
        validate_spread_request(proposed)


class CountingBroker:
    def __init__(self) -> None:
        self.calls = 0

    def submit_order(self, *_args, **_kwargs):
        self.calls += 1

    cancel_order = submit_order
    replace_order = submit_order


def test_all_broker_mutations_are_source_locked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MGC_NDXP_LIVE_TRANSMISSION_ENABLED", "1")
    broker = CountingBroker()
    gateway = LockedSchwabMutationGateway(broker)
    with pytest.raises(TransmissionDisabledError, match="source-locked"):
        gateway.submit(request())
    with pytest.raises(TransmissionDisabledError, match="source-locked"):
        gateway.cancel(account_hash="hash-1", broker_order_id="123")
    with pytest.raises(TransmissionDisabledError, match="source-locked"):
        gateway.replace(broker_order_id="123", request=request())
    assert broker.calls == 0


def test_diagnostics_distinguish_client_schwab_and_stale_data() -> None:
    client = classify_diagnostics(
        market={"latency_ms": 50}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=4100, source_age_ms=100, market_poll_age_ms=100,
    )
    slow = classify_diagnostics(
        market={"latency_ms": 3100}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=100, market_poll_age_ms=100,
    )
    stale = classify_diagnostics(
        market={"latency_ms": 50}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=6100, market_poll_age_ms=100,
    )
    assert client["classification"] == "CLIENT_OR_UI_STALL"
    assert slow["classification"] == "SCHWAB_RESPONSE_DELAY"
    assert stale["classification"] == "STALE_MARKET_DATA"


def test_demo_service_builds_preview_but_never_transmits(tmp_path: Path) -> None:
    service = NdxpTerminalService(tmp_path, adapter=DemoSchwabAdapter(), market_interval_seconds=0.05, broker_interval_seconds=0.05)
    service.start()
    try:
        deadline = time.monotonic() + 2
        snapshot = service.snapshot()
        while not snapshot["market"]["expirations"] and time.monotonic() < deadline:
            time.sleep(0.02)
            snapshot = service.snapshot()
        calls = snapshot["market"]["selected_chain"]["CALL"]
        assert calls[0]["net_change"] == 0.0
        assert calls[0]["percent_change"] == 0.0
        short = next(row for row in calls if any(other["strike"] == row["strike"] + 10 for other in calls))
        long = next(row for row in calls if row["strike"] == short["strike"] + 10)
        preview = service.preview(
            {
                "account_hash": "demo-account-hash",
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "quantity": 20,
                "net_credit": "1.25",
            }
        )
        assert preview["preview_only"] is True
        assert preview["summary"]["gross_width_dollars"] == "20000"
        with pytest.raises(TransmissionDisabledError):
            service.mutate("submit", {
                "account_hash": "demo-account-hash",
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "quantity": 20,
                "net_credit": "1.25",
            })
    finally:
        service.stop()


def test_non_loopback_server_refuses_live_or_unapproved_demo(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=False)
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=True)


def test_teleport_demo_requires_demo_flag() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--teleport-demo"])
    assert exc_info.value.code == 2
