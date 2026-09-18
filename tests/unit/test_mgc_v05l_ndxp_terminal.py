from __future__ import annotations

import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mgc_v05l.ndxp_terminal.analytics import derive_expiration_analytics
from mgc_v05l.ndxp_terminal.databento import DatabentoOpraFeed, NdxpDatabentoAdapter
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


class FakeDatabentoLive:
    def __init__(self) -> None:
        self.callback = None
        self.exception_callback = None
        self.subscriptions: list[dict] = []
        self.start_calls = 0
        self.stop_calls = 0

    def add_callback(self, callback, exception_callback) -> None:
        self.callback = callback
        self.exception_callback = exception_callback

    def subscribe(self, **kwargs) -> None:
        self.subscriptions.append(kwargs)

    def start(self) -> None:
        self.start_calls += 1

    def stop(self) -> None:
        self.stop_calls += 1


class SymbolMappingMsg:
    def __init__(self, instrument_id: int, symbol_value: str) -> None:
        self.instrument_id = instrument_id
        self.stype_in_symbol = symbol_value
        self.stype_out_symbol = str(instrument_id)


class _Level:
    def __init__(self, bid: int, ask: int) -> None:
        self.bid_px = bid
        self.ask_px = ask


class Cmbp1Msg:
    def __init__(self, instrument_id: int, bid: float, ask: float, ts_event: int) -> None:
        self.instrument_id = instrument_id
        self.levels = [_Level(int(bid * 1_000_000_000), int(ask * 1_000_000_000))]
        self.ts_event = ts_event


def symbol(strike: int, option_type: str = "C", root: str = "NDXP") -> str:
    return f"{root:<6}260914{option_type}{strike * 1000:08d}"


def request(*, option_type: str = "C", short: int = 29330, long: int = 29340, action: str = "OPEN") -> NdxpSpreadRequest:
    return NdxpSpreadRequest(
        account_hash="hash-1",
        short_symbol=symbol(short, option_type),
        long_symbol=symbol(long, option_type),
        quantity=20,
        limit_price=Decimal("3.75"),
        action=action,
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


def test_close_vertical_is_net_debit_with_opposite_leg_actions() -> None:
    proposed = request(action="CLOSE")
    summary = validate_spread_request(proposed)
    payload = build_vertical_order_payload(proposed)

    assert summary["price_effect"] == "DEBIT"
    assert summary["closing_debit_dollars"] == "7500.00"
    assert "maximum_loss_dollars" not in summary
    assert payload["orderType"] == "NET_DEBIT"
    assert [leg["instruction"] for leg in payload["orderLegCollection"]] == ["BUY_TO_CLOSE", "SELL_TO_CLOSE"]


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


def test_databento_feed_uses_one_session_and_maps_opra_quotes() -> None:
    live = FakeDatabentoLive()
    feed = DatabentoOpraFeed("test-key", live_factory=lambda _key: live)
    option_symbol = symbol(29330)

    feed.ensure_subscribed([option_symbol])
    feed.ensure_subscribed([option_symbol])
    assert live.start_calls == 1
    assert len(live.subscriptions) == 1
    assert live.subscriptions[0]["dataset"] == "OPRA.PILLAR"
    assert live.subscriptions[0]["schema"] == "cmbp-1"
    assert live.subscriptions[0]["stype_in"] == "raw_symbol"
    assert live.subscriptions[0]["snapshot"] is True

    live.callback(SymbolMappingMsg(71, option_symbol))
    event_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    live.callback(Cmbp1Msg(71, 2.10, 2.30, event_ns))
    quote = feed.quotes([option_symbol])[option_symbol]
    assert quote.bid == pytest.approx(2.10)
    assert quote.ask == pytest.approx(2.30)
    assert feed.status()["quote_count"] == 1

    feed.stop()
    assert live.stop_calls == 1


def test_hybrid_adapter_replaces_schwab_option_quotes_with_databento() -> None:
    live = FakeDatabentoLive()
    feed = DatabentoOpraFeed("test-key", live_factory=lambda _key: live)
    adapter = NdxpDatabentoAdapter(Path("."), schwab=DemoSchwabAdapter(), feed=feed)

    first = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    subscribed = live.subscriptions[0]["symbols"]
    assert 100 <= len(subscribed) <= 140
    assert first["databento"]["selected_quote_count"] == 0
    selected_expiration = first["databento"]["selected_expiration"]
    first_selected_map = next(
        value for key, value in first["chain"]["callExpDateMap"].items() if key.startswith(selected_expiration)
    )
    assert all(contract["bid"] is None for contracts in first_selected_map.values() for contract in contracts)

    event_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    for instrument_id, option_symbol in enumerate(subscribed, start=1):
        live.callback(SymbolMappingMsg(instrument_id, option_symbol))
        live.callback(Cmbp1Msg(instrument_id, 2.10, 2.30, event_ns))

    second = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    assert second["market_source"] == "Databento OPRA NBBO · Schwab NDX spot"
    assert second["databento"]["selected_quote_count"] == len(subscribed)
    second_selected_map = next(
        value for key, value in second["chain"]["callExpDateMap"].items() if key.startswith(selected_expiration)
    )
    quoted = [contract for contracts in second_selected_map.values() for contract in contracts if contract["bid"] is not None]
    assert quoted
    assert quoted[0]["bid"] == pytest.approx(2.10)
    assert quoted[0]["ask"] == pytest.approx(2.30)
    assert quoted[0]["quoteSource"] == "DATABENTO_OPRA_NBBO"


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


def test_diagnostics_identify_databento_entitlement_and_partial_quotes() -> None:
    entitlement = classify_diagnostics(
        market=None, broker={"latency_ms": 70},
        market_error="DatabentoConfigurationError: OPRA entitlement denied", broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=None, market_poll_age_ms=None,
    )
    partial = classify_diagnostics(
        market={
            "latency_ms": 50,
            "market_source": "Databento OPRA NBBO · Schwab NDX spot",
            "databento": {"target_symbol_count": 120, "selected_quote_count": 40, "last_error": None},
        },
        broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=100, market_poll_age_ms=100,
    )
    assert entitlement["classification"] == "DATABENTO_STREAM_ERROR"
    assert "SCHWAB_OR_NETWORK_ERROR" not in entitlement["all_classifications"]
    assert partial["classification"] == "DATABENTO_PARTIAL_QUOTES"


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
        puts = snapshot["market"]["selected_chain"]["PUT"]
        spot = snapshot["market"]["spot"]
        shared_strikes = {row["strike"] for row in calls} & {row["strike"] for row in puts}
        assert len([strike for strike in shared_strikes if strike < spot]) >= 25
        assert len([strike for strike in shared_strikes if strike > spot]) >= 25
        assert calls[0]["net_change"] == 0.0
        assert calls[0]["percent_change"] == 0.0
        analytics = snapshot["market"]["analytics"]
        assert analytics["status"] == "VALID"
        assert analytics["independent_of_schwab_greeks"] is True
        assert analytics["atm_iv_percent"] == pytest.approx(14.2, abs=0.05)
        assert analytics["expected_move"] > 0
        assert analytics["ranges"]["1.0"]["lower"] < spot < analytics["ranges"]["1.0"]["upper"]
        # The number of economically valid synthetic mids contracts as 0DTE time
        # elapses; keep the assertion independent of the wall-clock test hour.
        assert len(analytics["spreads"]) >= 30
        model_spread = next(iter(analytics["spreads"].values()))
        assert 0 <= model_spread["probability_beyond_breakeven"] <= 1
        assert model_spread["market_width"] >= 0
        assert model_spread["credit_band"] in {"BELOW_PREFERRED", "PREFERRED", "ELEVATED"}
        short = next(row for row in calls if any(other["strike"] == row["strike"] + 10 for other in calls))
        long = next(row for row in calls if row["strike"] == short["strike"] + 10)
        preview = service.preview(
            {
                "account_hash": "demo-account-hash",
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "quantity": 20,
                "limit_price": "1.25",
                "action": "OPEN",
            }
        )
        assert preview["preview_only"] is True
        assert preview["summary"]["gross_width_dollars"] == "20000"
        positioned_short = next(row for row in calls if row["strike"] == 29330)
        positioned_long = next(row for row in calls if row["strike"] == 29340)
        close_preview = service.preview(
            {
                "account_hash": "demo-account-hash",
                "short_symbol": positioned_short["symbol"],
                "long_symbol": positioned_long["symbol"],
                "quantity": 20,
                "limit_price": "1.25",
                "action": "CLOSE",
            }
        )
        assert close_preview["order_payload"]["orderType"] == "NET_DEBIT"
        assert [leg["instruction"] for leg in close_preview["order_payload"]["orderLegCollection"]] == [
            "BUY_TO_CLOSE",
            "SELL_TO_CLOSE",
        ]
        with pytest.raises(SpreadValidationError, match="exact short leg"):
            service.preview(
                {
                    "account_hash": "demo-account-hash",
                    "short_symbol": short["symbol"],
                    "long_symbol": long["symbol"],
                    "quantity": 20,
                    "limit_price": "1.25",
                    "action": "CLOSE",
                }
            )
        with pytest.raises(TransmissionDisabledError):
            service.mutate("submit", {
                "account_hash": "demo-account-hash",
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "quantity": 20,
                "limit_price": "1.25",
                "action": "OPEN",
            })
    finally:
        service.stop()


def test_expected_range_fails_closed_when_model_quotes_are_stale() -> None:
    adapter = DemoSchwabAdapter()
    raw = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    from mgc_v05l.ndxp_terminal.service import _normalize_market

    normalized = _normalize_market(raw, selected_expiration=None)
    stale_ms = int((datetime.now(timezone.utc).timestamp() - 60) * 1000)
    for side in normalized["selected_chain"].values():
        for contract in side:
            contract["quote_time_ms"] = stale_ms
    analytics = derive_expiration_analytics(
        spot=normalized["spot"],
        expiration=normalized["selected_expiration"],
        chain=normalized["selected_chain"],
        spot_quote_time_ms=stale_ms,
        now=datetime.now(timezone.utc),
    )

    assert analytics["status"] == "UNAVAILABLE"
    assert "stale" in analytics["reason"].lower()
    assert analytics["spreads"] == {}


def test_non_loopback_server_refuses_live_or_unapproved_demo(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=False)
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=True)


def test_teleport_demo_requires_demo_flag() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--teleport-demo"])
    assert exc_info.value.code == 2
