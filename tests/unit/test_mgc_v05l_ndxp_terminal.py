from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import mgc_v05l.ndxp_terminal.server as server_module

from mgc_v05l.ndxp_terminal.analytics import derive_expiration_analytics
from mgc_v05l.ndxp_terminal.databento import DatabentoOpraFeed, NdxpDatabentoAdapter
from mgc_v05l.ndxp_terminal.diagnostics import classify_diagnostics
from mgc_v05l.ndxp_terminal.market_gamma import derive_market_gamma
from mgc_v05l.ndxp_terminal.orders import (
    LockedSchwabMutationGateway,
    NdxpSpreadRequest,
    SpreadValidationError,
    TransmissionDisabledError,
    build_vertical_order_payload,
    validate_spread_request,
)
from mgc_v05l.ndxp_terminal.server import (
    DemoSchwabAdapter,
    client_in_trusted_networks,
    is_loopback_address,
    main,
    parse_trusted_live_networks,
    run_server,
    same_origin_allowed,
)
from mgc_v05l.ndxp_terminal.service import NdxpTerminalService
from mgc_v05l.ndxp_terminal.schwab import NdxpSchwabAdapter
from mgc_v05l.ndxp_terminal.products import get_product


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


def request(
    *, option_type: str = "C", short: int = 29330, long: int = 29340,
    action: str = "OPEN", quantity: int = 20, limit_price: str = "3.75",
) -> NdxpSpreadRequest:
    return NdxpSpreadRequest(
        account_hash="hash-1",
        short_symbol=symbol(short, option_type),
        long_symbol=symbol(long, option_type),
        quantity=quantity,
        limit_price=Decimal(limit_price),
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
        (request(short=29330, long=29337), "must be one of"),
        (request(short=29340, long=29330), "sell the lower strike"),
        (NdxpSpreadRequest("hash-1", symbol(29330, root="SPXW"), symbol(29340, root="RUTW"), 1, Decimal("1")), "same supported NDX"),
        (NdxpSpreadRequest("hash-1", symbol(29330), symbol(29340), 101, Decimal("1")), "between 1 and 100"),
    ],
)
def test_invalid_spreads_fail_closed(proposed: NdxpSpreadRequest, message: str) -> None:
    with pytest.raises(SpreadValidationError, match=message):
        validate_spread_request(proposed)


def test_supported_index_products_and_widths_build_valid_orders() -> None:
    spx = NdxpSpreadRequest("hash-1", symbol(6450, root="SPXW"), symbol(6455, root="SPXW"), 2, Decimal("1.25"))
    rut = NdxpSpreadRequest("hash-1", symbol(2875, option_type="P", root="RUTW"), symbol(2870, option_type="P", root="RUTW"), 3, Decimal("0.80"))

    assert validate_spread_request(spx)["product"] == "SPX"
    assert validate_spread_request(spx)["gross_width_dollars"] == "1000"
    assert validate_spread_request(rut)["product"] == "RUT"
    assert build_vertical_order_payload(rut)["orderLegCollection"][0]["instrument"]["symbol"].startswith("RUTW")


class CountingBroker:
    def __init__(self) -> None:
        self.calls = 0

    def submit_order(self, *_args, **_kwargs):
        self.calls += 1
        return {"status_code": 201, "broker_order_id": "test-order-123"}

    cancel_order = submit_order
    replace_order = submit_order


class CapturingTruthBroker:
    def __init__(self) -> None:
        self.order_calls: list[dict] = []

    def list_account_numbers(self) -> list[dict]:
        return [{"accountNumber": "masked", "hashValue": "hash-1"}]

    def list_accounts(self, *, fields: list[str] | None = None) -> list[dict]:
        assert fields == ["positions"]
        return [{"securitiesAccount": {"type": "MARGIN"}}]

    def get_orders(self, account_hash: str, **kwargs) -> list[dict]:
        assert account_hash == "hash-1"
        self.order_calls.append(kwargs)
        return []


def test_broker_truth_supplies_required_schwab_order_time_window(tmp_path: Path) -> None:
    adapter = object.__new__(NdxpSchwabAdapter)
    adapter._repo_root = tmp_path
    adapter.broker = CapturingTruthBroker()

    snapshot = adapter.fetch_broker_truth()

    assert snapshot["selected_account_hash"] == "hash-1"
    assert len(adapter.broker.order_calls) == 2
    working_call, recent_call = adapter.broker.order_calls
    assert working_call["status"] == "WORKING"
    assert working_call["max_results"] == 100
    from_value = working_call["from_entered_time"]
    to_value = working_call["to_entered_time"]
    assert from_value.endswith(".000Z")
    assert to_value.endswith(".000Z")
    from_time = datetime.fromisoformat(from_value.replace("Z", "+00:00"))
    to_time = datetime.fromisoformat(to_value.replace("Z", "+00:00"))
    assert from_time.tzinfo == timezone.utc
    assert to_time.tzinfo == timezone.utc
    assert to_time - from_time == timedelta(days=60)
    assert "status" not in recent_call
    assert recent_call["max_results"] == 100
    recent_from = datetime.fromisoformat(recent_call["from_entered_time"].replace("Z", "+00:00"))
    recent_to = datetime.fromisoformat(recent_call["to_entered_time"].replace("Z", "+00:00"))
    assert recent_to - recent_from == timedelta(days=1)


def test_terminal_defaults_to_confirmed_schwab_ndx_symbol(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MGC_NDXP_CHAIN_SYMBOL", raising=False)
    service = NdxpTerminalService(tmp_path, adapter=DemoSchwabAdapter())
    assert service.chain_symbol == "$NDX"


def test_terminal_selection_switches_product_and_default_width(tmp_path: Path) -> None:
    service = NdxpTerminalService(tmp_path, adapter=DemoSchwabAdapter())

    snapshot = service.update_selection({"product": "SPX", "option_type": "CALL"})

    assert snapshot["selection"]["product"] == "SPX"
    assert snapshot["selection"]["spread_width"] == 5
    assert snapshot["market"]["product"]["option_roots"] == ("SPX", "SPXW")
    assert service.chain_symbol == "$SPX"
    assert service.quote_symbol == "$SPX"


def test_schwab_open_interest_gamma_is_labeled_estimated_and_finds_walls() -> None:
    expiry = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    chains = {
        expiry: {
            "CALL": [
                {"strike": 6400, "iv": 20.0, "open_interest": 100},
                {"strike": 6500, "iv": 20.0, "open_interest": 900},
            ],
            "PUT": [
                {"strike": 6300, "iv": 22.0, "open_interest": 1200},
                {"strike": 6400, "iv": 21.0, "open_interest": 200},
            ],
        }
    }

    result = derive_market_gamma(spot=6425, chains=chains, product=get_product("SPX"))

    assert result["status"] == "VALID"
    assert result["positioning_is_estimated"] is True
    assert result["open_interest_basis"] == "START_OF_DAY"
    assert result["coverage_ratio"] == 1.0
    assert result["expiration_count"] == 1
    assert result["call_wall"]["strike"] == 6500
    assert result["put_wall"]["strike"] == 6300
    assert result["scenario_profile"]


def test_schwab_chain_canonicalizes_ndx_to_confirmed_index_symbol() -> None:
    class OAuth:
        @staticmethod
        def get_access_token() -> str:
            return "token"

    class CapturingTransport:
        def __init__(self) -> None:
            self.requests: list = []

        def request_json(self, request):
            self.requests.append(request)
            if len(self.requests) < 3:
                return {}
            expiration = request.query["fromDate"]
            return {"callExpDateMap": {f"{expiration}:0": {"29400.0": [{"symbol": symbol(29400)}]}}}

    adapter = object.__new__(NdxpSchwabAdapter)
    adapter.oauth = OAuth()
    adapter.market_config = SimpleNamespace(market_data_base_url="https://api.schwabapi.com/marketdata/v1")
    adapter.transport = CapturingTransport()

    adapter.fetch_chain(chain_symbol="NDX")

    assert len(adapter.transport.requests) == 3
    request_dates = [date.fromisoformat(row.query["fromDate"]) for row in adapter.transport.requests]
    assert request_dates[1] - request_dates[0] == timedelta(days=1)
    assert request_dates[2] - request_dates[1] == timedelta(days=1)
    assert request_dates[0] == datetime.now(ZoneInfo("America/New_York")).date()
    assert all(row.query["symbol"] == "$NDX" for row in adapter.transport.requests)
    assert all(row.query["strikeCount"] == 120 for row in adapter.transport.requests)
    assert all(row.query["fromDate"] == row.query["toDate"] for row in adapter.transport.requests)

    adapter.fetch_chain(chain_symbol="NDX")
    assert len(adapter.transport.requests) == 4
    assert date.fromisoformat(adapter.transport.requests[-1].query["fromDate"]) == request_dates[2]


def test_schwab_gamma_chain_uses_cached_multi_expiration_request() -> None:
    class OAuth:
        @staticmethod
        def get_access_token() -> str:
            return "token"

    class CapturingTransport:
        def __init__(self) -> None:
            self.requests: list = []

        def request_json(self, request):
            self.requests.append(request)
            expiration = request.query["fromDate"]
            return {"callExpDateMap": {f"{expiration}:0": {"6400.0": [{"symbol": symbol(6400, root="SPXW")}]}}}

    adapter = object.__new__(NdxpSchwabAdapter)
    adapter.oauth = OAuth()
    adapter.market_config = SimpleNamespace(market_data_base_url="https://api.schwabapi.com/marketdata/v1")
    adapter.transport = CapturingTransport()

    first = adapter.fetch_gamma_chain(chain_symbol="SPX")
    second = adapter.fetch_gamma_chain(chain_symbol="SPX")

    assert first == second
    assert len(adapter.transport.requests) == 1
    query = adapter.transport.requests[0].query
    assert query["symbol"] == "$SPX"
    assert query["strikeCount"] == 160
    assert date.fromisoformat(query["toDate"]) - date.fromisoformat(query["fromDate"]) == timedelta(days=45)


def test_access_check_uses_aapl_for_quote_connectivity() -> None:
    adapter = object.__new__(NdxpSchwabAdapter)
    calls: list[tuple[str, str]] = []
    adapter.fetch_broker_truth = lambda: {"account_numbers": [{"hashValue": "hash-1"}], "latency_ms": 1.0}
    adapter.fetch_chain = lambda *, chain_symbol: (
        calls.append(("chain", chain_symbol))
        or {"callExpDateMap": {"2026-09-18:0": {"29400.0": [{"symbol": symbol(29400)}]}}}
    )
    adapter.fetch_quote = lambda *, quote_symbol: (
        calls.append(("quote", quote_symbol)) or {"AAPL": {"quote": {"lastPrice": 200.0}}}
    )

    result = adapter.access_check()

    assert calls == [("chain", "$NDX"), ("quote", "AAPL")]
    assert result["ndx_chain_access"] is True
    assert result["quote_probe_symbol"] == "AAPL"
    assert result["quote_access"] is True


def test_live_trading_requires_both_runtime_and_launch_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MGC_NDXP_LIVE_TRANSMISSION_ENABLED", "1")
    broker = CountingBroker()
    gateway = LockedSchwabMutationGateway(broker)
    with pytest.raises(TransmissionDisabledError, match="launch-locked"):
        gateway.submit(request())
    with pytest.raises(TransmissionDisabledError, match="launch-locked"):
        gateway.cancel(account_hash="hash-1", broker_order_id="123")
    with pytest.raises(TransmissionDisabledError, match="launch-locked"):
        gateway.replace(broker_order_id="123", request=request())
    assert broker.calls == 0

    monkeypatch.delenv("MGC_NDXP_LIVE_TRANSMISSION_ENABLED")
    requested = LockedSchwabMutationGateway(broker, live_trading_requested=True)
    with pytest.raises(TransmissionDisabledError, match="runtime-locked"):
        requested.submit(request(quantity=1, limit_price="3.75"))
    assert broker.calls == 0


def test_live_trading_accepts_valid_quantity_close_and_replace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MGC_NDXP_LIVE_TRANSMISSION_ENABLED", "1")
    broker = CountingBroker()
    gateway = LockedSchwabMutationGateway(broker, live_trading_requested=True)

    replaced = gateway.replace(broker_order_id="123", request=request(quantity=1, limit_price="9.95"))
    assert replaced["status_code"] == 201
    assert broker.calls == 1

    opened = gateway.submit(request(quantity=20, limit_price="3.75"))
    closed = gateway.submit(request(quantity=20, limit_price="1.25", action="CLOSE"))
    assert opened["broker_order_id"] == "test-order-123"
    assert closed["broker_order_id"] == "test-order-123"
    assert broker.calls == 3


def test_live_trading_client_and_same_origin_guards() -> None:
    assert is_loopback_address("127.0.0.1") is True
    assert is_loopback_address("::1") is True
    assert is_loopback_address("192.168.1.254") is False
    assert is_loopback_address("192.168.1.42") is False

    networks = parse_trusted_live_networks(("192.168.1.0/24",))
    assert client_in_trusted_networks("192.168.1.42", networks) is True
    assert client_in_trusted_networks("192.168.2.42", networks) is False
    assert same_origin_allowed(
        origin="http://192.168.1.254:8810",
        host="192.168.1.254:8810",
        loopback=False,
        trusted_networks=networks,
    ) is True
    assert same_origin_allowed(
        origin="https://untrusted.example", host="192.168.1.254:8810", loopback=False
    ) is False
    assert same_origin_allowed(
        origin="http://rebind.example:8810",
        host="rebind.example:8810",
        loopback=False,
        trusted_networks=networks,
    ) is False
    assert same_origin_allowed(origin=None, host="192.168.1.254:8810", loopback=False) is False
    assert same_origin_allowed(origin=None, host="127.0.0.1:8810", loopback=True) is True

    with pytest.raises(ValueError, match="must be private"):
        parse_trusted_live_networks(("8.8.8.0/24",))


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
    regular_hours = datetime(2026, 9, 18, 14, 0, tzinfo=timezone.utc)
    client = classify_diagnostics(
        market={"latency_ms": 50}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=4100, source_age_ms=100, market_poll_age_ms=100, now=regular_hours,
    )
    slow = classify_diagnostics(
        market={"latency_ms": 3100}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=100, market_poll_age_ms=100, now=regular_hours,
    )
    stale = classify_diagnostics(
        market={"latency_ms": 50}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=6100, market_poll_age_ms=100, now=regular_hours,
    )
    assert client["classification"] == "CLIENT_OR_UI_STALL"
    assert slow["classification"] == "SCHWAB_RESPONSE_DELAY"
    assert stale["classification"] == "STALE_MARKET_DATA"

    closed = classify_diagnostics(
        market={"latency_ms": 50}, broker={"latency_ms": 70}, market_error=None, broker_error=None,
        worker_gap_ms=100, client_gap_ms=100, source_age_ms=60_000, market_poll_age_ms=100,
        now=datetime(2026, 9, 18, 11, 0, tzinfo=timezone.utc),
    )
    assert closed["classification"] == "MARKET_CLOSED_LATEST_QUOTES"
    assert closed["market_session"] == "OUTSIDE_REGULAR_HOURS"


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
        assert model_spread["short_iv_percent"] > 0
        assert model_spread["short_iv_expected_move"] > 0
        assert model_spread["short_iv_em_multiple"] is not None
        assert model_spread["credit_band"] in {"BELOW_PREFERRED", "PREFERRED", "ELEVATED"}
        assert model_spread["spread_delta"] == pytest.approx(-model_spread["credit_position_delta"])
        if model_spread["option_type"] == "CALL":
            assert model_spread["spread_delta"] > 0
        else:
            assert model_spread["spread_delta"] < 0
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
        broker_adjudicated = service.preview(
            {
                "account_hash": "demo-account-hash",
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "quantity": 20,
                "limit_price": "1.25",
                "action": "CLOSE",
            }
        )
        assert broker_adjudicated["position_effect"]["classification"] == "BROKER_ADJUDICATED"
        assert broker_adjudicated["position_effect"]["reverse_open_quantity"] is None
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


def test_live_trading_uses_exact_single_use_preview_and_selected_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MGC_NDXP_LIVE_TRANSMISSION_ENABLED", "1")
    broker = CountingBroker()
    adapter = DemoSchwabAdapter()
    adapter.broker = broker
    fetch_demo_broker_truth = adapter.fetch_broker_truth

    def fetch_broker_truth_with_working_order() -> dict:
        truth = fetch_demo_broker_truth()
        demo_positions = truth["accounts"][0]["securitiesAccount"]["positions"]
        short_symbol = demo_positions[0]["instrument"]["symbol"]
        long_symbol = demo_positions[1]["instrument"]["symbol"]
        pending_short_symbol = f"{short_symbol[:-8]}{int(short_symbol[-8:]) + 20_000:08d}"
        pending_long_symbol = f"{long_symbol[:-8]}{int(long_symbol[-8:]) + 20_000:08d}"
        truth["working_orders"] = [
            {
                "orderId": "working-order-456",
                "status": "WORKING",
                "orderType": "NET_CREDIT",
                "price": 2.0,
                "enteredTime": datetime.now(timezone.utc).isoformat(),
                "orderLegCollection": [
                    {
                        "instruction": "SELL_TO_OPEN",
                        "quantity": 20,
                        "instrument": {"symbol": short_symbol},
                    },
                    {
                        "instruction": "BUY_TO_OPEN",
                        "quantity": 20,
                        "instrument": {"symbol": long_symbol},
                    },
                ],
            },
            {
                "orderId": "pending-only-order-457",
                "status": "WORKING",
                "orderType": "NET_CREDIT",
                "price": 2.0,
                "enteredTime": datetime.now(timezone.utc).isoformat(),
                "filledQuantity": 0,
                "remainingQuantity": 20,
                "orderLegCollection": [
                    {
                        "instruction": "SELL_TO_OPEN",
                        "quantity": 20,
                        "instrument": {"symbol": pending_short_symbol},
                    },
                    {
                        "instruction": "BUY_TO_OPEN",
                        "quantity": 20,
                        "instrument": {"symbol": pending_long_symbol},
                    },
                ],
            },
        ]
        truth["recent_orders"] = [
            {
                "orderId": "filled-order-789",
                "status": "FILLED",
                "orderType": "NET_CREDIT",
                "price": 2.1,
                "enteredTime": datetime.now(timezone.utc).isoformat(),
                "closeTime": datetime.now(timezone.utc).isoformat(),
                "filledQuantity": 20,
                "orderLegCollection": truth["working_orders"][0]["orderLegCollection"],
            }
        ]
        return truth

    adapter.fetch_broker_truth = fetch_broker_truth_with_working_order
    service = NdxpTerminalService(
        tmp_path,
        adapter=adapter,
        market_interval_seconds=0.05,
        broker_interval_seconds=0.05,
        live_trading_requested=True,
    )
    service.start()
    try:
        deadline = time.monotonic() + 2
        snapshot = service.snapshot()
        while (not snapshot["market"]["selected_chain"]["CALL"] or not snapshot["broker"]["accounts"]) and time.monotonic() < deadline:
            time.sleep(0.02)
            snapshot = service.snapshot()
        calls = snapshot["market"]["selected_chain"]["CALL"]
        short = next(row for row in calls if any(other["strike"] == row["strike"] + 10 for other in calls))
        long = next(row for row in calls if row["strike"] == short["strike"] + 10)
        payload = {
            "account_hash": "demo-account-hash",
            "short_symbol": short["symbol"],
            "long_symbol": long["symbol"],
            "quantity": 20,
            "limit_price": "3.75",
            "action": "OPEN",
        }

        working = snapshot["broker"]["working_orders"][0]
        assert working["editable"] is True
        assert working["action"] == "OPEN"
        assert working["quantity"] == 20
        recent = snapshot["broker"]["recent_orders"][0]
        assert recent["status"] == "FILLED"
        assert recent["filled_quantity"] == 20
        assert recent["editable"] is False

        pending = snapshot["broker"]["working_orders"][1]
        pending_close_payload = {
            **payload,
            "short_symbol": pending["short_symbol"],
            "long_symbol": pending["long_symbol"],
            "limit_price": "1.25",
            "action": "CLOSE",
        }
        pending_close = service.preview(pending_close_payload, allow_live_token=True)
        assert pending_close["position_effect"] == {
            "held_close_quantity": 0.0,
            "pending_close_quantity": 20.0,
            "reverse_open_quantity": 0.0,
            "classification": "PENDING_CLOSE",
        }
        pending_reverse = service.preview(
            {**pending_close_payload, "quantity": 25}, allow_live_token=True
        )
        assert pending_reverse["position_effect"]["classification"] == "CLOSE_AND_REVERSE"
        assert pending_reverse["position_effect"]["reverse_open_quantity"] == 5.0

        preview = service.preview(payload, allow_live_token=True)
        assert preview["transmission_enabled"] is True
        assert preview["preview_token_expires_seconds"] == 60
        submitted = service.mutate("submit", {**payload, "preview_token": preview["preview_token"]})
        assert submitted["broker_order_id"] == "test-order-123"
        assert broker.calls == 1

        with pytest.raises(SpreadValidationError, match="already consumed"):
            service.mutate("submit", {**payload, "preview_token": preview["preview_token"]})
        assert broker.calls == 1

        with pytest.raises(SpreadValidationError, match="currently selected"):
            service.mutate("cancel", {"account_hash": "another-account", "broker_order_id": "test-order-123"})
        assert broker.calls == 1

        positioned_short = next(row for row in calls if row["strike"] == 29330)
        positioned_long = next(row for row in calls if row["strike"] == 29340)
        close_payload = {
            **payload,
            "short_symbol": positioned_short["symbol"],
            "long_symbol": positioned_long["symbol"],
            "limit_price": "1.25",
            "action": "CLOSE",
        }
        close_preview = service.preview(close_payload, allow_live_token=True)
        closed = service.mutate(
            "submit", {**close_payload, "preview_token": close_preview["preview_token"]}
        )
        assert closed["broker_order_id"] == "test-order-123"
        assert broker.calls == 2

        replace_payload = {
            "account_hash": "demo-account-hash",
            "broker_order_id": "working-order-456",
            "short_symbol": positioned_short["symbol"],
            "long_symbol": positioned_long["symbol"],
            "quantity": 20,
            "limit_price": "2.25",
            "action": "OPEN",
        }
        replaced = service.mutate("replace", replace_payload)
        assert replaced["broker_order_id"] == "test-order-123"
        assert broker.calls == 3

        cancelled = service.mutate(
            "cancel", {"account_hash": "demo-account-hash", "broker_order_id": "test-order-123"}
        )
        assert cancelled["status_code"] == 201
        assert broker.calls == 4
        journal = (tmp_path / "outputs" / "ndxp_terminal" / "mutations.jsonl").read_text(encoding="utf-8")
        assert "SUBMIT_ATTEMPT" in journal
        assert "SUBMIT_ACK" in journal
        assert "CANCEL_ACK" in journal
        assert "REPLACE_ACK" in journal
        assert "demo-account-hash" not in journal

        service.stop()
        service._last_market_success_wall = time.time() - 6
        with pytest.raises(SpreadValidationError, match="market poll is over five seconds old"):
            service.preview(payload, allow_live_token=True)

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


def test_closed_session_uses_one_bounded_latest_close_snapshot() -> None:
    adapter = DemoSchwabAdapter()
    raw = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    from mgc_v05l.ndxp_terminal.service import _normalize_market

    normalized = _normalize_market(raw, selected_expiration=None)
    observed_at = datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc)
    option_time_ms = int(datetime(2026, 9, 18, 20, 15, tzinfo=timezone.utc).timestamp() * 1000)
    spot_time_ms = int(datetime(2026, 9, 18, 21, 15, tzinfo=timezone.utc).timestamp() * 1000)
    for side in normalized["selected_chain"].values():
        for contract in side:
            contract["quote_time_ms"] = option_time_ms
    analytics = derive_expiration_analytics(
        spot=normalized["spot"],
        expiration=normalized["selected_expiration"],
        chain=normalized["selected_chain"],
        spot_quote_time_ms=spot_time_ms,
        allow_closed_snapshot=True,
        now=observed_at,
    )

    assert analytics["status"] == "VALID"
    assert analytics["quote_mode"] == "CLOSED_SNAPSHOT"
    assert analytics["model_input_time_ms"] == option_time_ms
    assert analytics["model_input_skew_ms"] == 60 * 60 * 1000
    assert analytics["oldest_model_quote_age_ms"] > 15_000
    assert analytics["spreads"]


def test_closed_snapshot_rejects_inputs_from_different_market_dates() -> None:
    adapter = DemoSchwabAdapter()
    raw = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    from mgc_v05l.ndxp_terminal.service import _normalize_market

    normalized = _normalize_market(raw, selected_expiration=None)
    option_time_ms = int(datetime(2026, 9, 18, 20, 15, tzinfo=timezone.utc).timestamp() * 1000)
    spot_time_ms = int(datetime(2026, 9, 19, 4, 15, tzinfo=timezone.utc).timestamp() * 1000)
    for side in normalized["selected_chain"].values():
        for contract in side:
            contract["quote_time_ms"] = option_time_ms
    analytics = derive_expiration_analytics(
        spot=normalized["spot"],
        expiration=normalized["selected_expiration"],
        chain=normalized["selected_chain"],
        spot_quote_time_ms=spot_time_ms,
        allow_closed_snapshot=True,
        now=datetime(2026, 9, 19, 10, 0, tzinfo=timezone.utc),
    )

    assert analytics["status"] == "UNAVAILABLE"
    assert "Eastern market date" in analytics["reason"]


def test_market_normalization_binds_index_value_to_schwab_quote_timestamp() -> None:
    from mgc_v05l.ndxp_terminal.service import _normalize_market

    quote_time_ms = 1_789_776_000_000
    normalized = _normalize_market(
        {
            "chain": {"symbol": "$NDX", "underlyingPrice": 29_640.0},
            "quote": {
                "$NDX": {
                    "assetMainType": "INDEX",
                    "quote": {
                        "lastPrice": 29_644.17,
                        "tradeTime": quote_time_ms // 1_000,
                        "quoteTime": quote_time_ms // 1_000 + 3_600,
                    },
                }
            },
        },
        selected_expiration=None,
    )

    assert normalized["spot"] == pytest.approx(29_644.17)
    assert normalized["spot_quote_time_ms"] == quote_time_ms
    assert normalized["spot_source"] == "Schwab index last"


def test_independent_gamma_scenarios_use_credit_position_convention() -> None:
    adapter = DemoSchwabAdapter()
    raw = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    from mgc_v05l.ndxp_terminal.service import _normalize_market

    normalized = _normalize_market(raw, selected_expiration=None)
    analytics = derive_expiration_analytics(
        spot=normalized["spot"],
        expiration=normalized["selected_expiration"],
        chain=normalized["selected_chain"],
        spot_quote_time_ms=normalized["spot_quote_time_ms"],
        now=datetime.now(timezone.utc),
    )

    assert analytics["status"] == "VALID"
    spread = next(iter(analytics["spreads"].values()))
    assert spread["credit_position_gamma"] == pytest.approx(-spread["spread_gamma"])
    assert spread["credit_position_delta"] == pytest.approx(-spread["spread_delta"])
    assert [row["spot_move"] for row in spread["gamma_scenarios"]] == [-25.0, -10.0, 0.0, 10.0, 25.0]
    zero = next(row for row in spread["gamma_scenarios"] if row["spot_move"] == 0)
    assert zero["credit_position_gamma"] == pytest.approx(spread["credit_position_gamma"])
    assert zero["credit_position_delta"] == pytest.approx(spread["credit_position_delta"])
    assert "fitted leg IVs held constant" in spread["gamma_method"]
    assert spread["value_class"] in {"THIN", "RICH_GAMMA", "HARVEST", "ORDINARY"}
    ranked_spread = next(row for row in analytics["spreads"].values() if row["value_percentile"] is not None)
    assert "credit/risk percentile" in ranked_spread["value_reason"]


def test_empty_market_poll_retains_last_valid_chain_and_reports_error(tmp_path: Path) -> None:
    adapter = DemoSchwabAdapter()
    valid_market = adapter.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
    empty_market = {
        **valid_market,
        "chain": {
            "symbol": "$NDX",
            "callExpDateMap": {},
            "putExpDateMap": {},
        },
    }
    service = NdxpTerminalService(tmp_path, adapter=adapter)
    service._market = valid_market
    service._selected_expiration = "1999-01-01"
    service._last_market_success_wall = 123.0
    adapter.fetch_market = lambda **_kwargs: empty_market

    service._poll_market()

    assert service._market is not None
    assert service._market["chain"] == valid_market["chain"]
    assert service._last_market_success_wall == 123.0
    assert service._market_error == "Schwab returned no available option contracts; retaining the last valid chain."
    snapshot = service.snapshot()
    assert snapshot["market"]["expirations"]
    assert service._selected_expiration == snapshot["market"]["selected_expiration"]


def test_non_loopback_server_refuses_live_or_unapproved_demo(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=False)
    with pytest.raises(ValueError, match="non-loopback bind"):
        run_server(repo_root=tmp_path, host="0.0.0.0", port=0, open_browser=False, demo=True)


def test_teleport_demo_requires_demo_flag() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--teleport-demo"])
    assert exc_info.value.code == 2


def test_lan_live_allows_schwab_only_and_rejects_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(server_module, "run_server", lambda **kwargs: captured.update(kwargs))

    main(["--lan-live", "--no-browser"])
    assert captured["databento"] is False
    assert captured["allow_lan_live"] is True
    assert captured["host"] == "0.0.0.0"

    main(
        [
            "--lan-live",
            "--live-trading",
            "--trusted-live-subnet",
            "192.168.1.0/24",
            "--no-browser",
        ]
    )
    assert captured["live_trading"] is True
    assert captured["trusted_live_subnets"] == ("192.168.1.0/24",)

    with pytest.raises(SystemExit) as missing_subnet:
        main(["--lan-live", "--live-trading", "--no-browser"])
    assert missing_subnet.value.code == 2
    with pytest.raises(SystemExit) as missing_live_flags:
        main(["--trusted-live-subnet", "192.168.1.0/24", "--no-browser"])
    assert missing_live_flags.value.code == 2

    with pytest.raises(SystemExit) as demo_conflict:
        main(["--demo", "--lan-live"])
    assert demo_conflict.value.code == 2
    with pytest.raises(SystemExit) as live_demo_conflict:
        main(["--demo", "--live-trading"])
    assert live_demo_conflict.value.code == 2


def test_mobile_chain_keeps_strikes_fixed_and_allows_positive_midpoint_sell() -> None:
    static_root = Path(__file__).parents[2] / "src" / "mgc_v05l" / "ndxp_terminal" / "static"
    html = (static_root / "index.html").read_text(encoding="utf-8")
    css = (static_root / "styles.css").read_text(encoding="utf-8")
    javascript = (static_root / "app.js").read_text(encoding="utf-8")

    assert 'id="call-scroll"' in html
    assert 'id="strike-table"' in html
    assert 'id="put-scroll"' in html
    assert 'id="chain-header-frame"' in html
    assert 'id="call-header-table"' in html
    assert 'id="put-header-table"' in html
    assert "--strike-gutter-width:146px" in css
    assert "grid-template-columns:minmax(0,1fr) var(--strike-gutter-width) minmax(0,1fr)" in css
    assert ".side-grid { width:max-content; }" in css
    assert ".call-side-scroll .side-grid,#call-header-table { margin-left:auto; }" in css
    assert ".spread-grid.strike-grid" in css
    assert "width:100%; min-width:0; max-width:100%; overflow:hidden" in css
    assert "position:-webkit-sticky; position:sticky" in css
    assert "position:sticky; top:var(--topbar-height)" in css
    assert ".metric.itm { background:var(--call); }" in css
    assert ".bid-action.itm { background:#512029; }" in css
    assert ".ask-action.itm { background:#174d35; }" in css
    assert ".position-flag.long { color:#54e990; }" in css
    assert ".position-flag.short { color:#ff656d; }" in css
    assert 'ui["call-scroll"].scrollLeft' in javascript
    assert 'ui["put-scroll"].scrollLeft' in javascript
    assert "const tableLeft = headerTable.getBoundingClientRect().left" in javascript
    assert "const contentLeft = headerRect.left - tableLeft" in javascript
    assert "contentLeft + headerRect.width - pane.clientWidth" in javascript
    assert "if (!call && !put) return []" in javascript
    assert '["spread_delta", "Model Δ", 4]' in javascript
    assert '["credit_position_gamma", "Credit Γ", 5]' in javascript
    assert "credit_position_delta" in (static_root.parent / "analytics.py").read_text(encoding="utf-8")
    assert "credit_position_gamma" in (static_root.parent / "analytics.py").read_text(encoding="utf-8")
    assert 'id="quote-age-label"' in html
    assert 'market.spot_source || "NDX value"' in javascript
    assert '"timestamp unavailable"' in javascript
    assert 'timeZone: "America/New_York"' in javascript
    assert "easternTimestamp(market.spot_quote_time_ms)" in javascript
    assert '"MARKET CLOSED · LATEST QUOTES"' in javascript
    assert "selling it reverses the sign" in javascript
    assert "function anchorChainPanes()" in javascript
    assert "Math.abs(nextWidth - chainViewportWidth) > 80" in javascript
    assert 'window.addEventListener("resize", () => handleViewportGeometryChange(false)' in javascript
    assert 'window.addEventListener("orientationchange", () => handleViewportGeometryChange(true)' in javascript
    assert 'document.documentElement.style.setProperty("--topbar-height"' in javascript
    assert 'const callItm = row.call && Number(row.call.short.strike) < Number(spot)' in javascript
    assert 'const putItm = row.put && Number(row.put.short.strike) > Number(spot)' in javascript
    assert 'appendPositionFlag(node, row.call?.short, "short", "call", "low", positions)' in javascript
    assert 'appendPositionFlag(node, row.put?.long, "long", "put", "low", positions)' in javascript
    assert 'marker.textContent = "⚑"' in javascript
    assert "button.disabled = opening ? !positiveMidpoint" in javascript
    assert 'value="20"' in html
    assert 'id="reviewed"' not in html
    assert 'id="spread-gamma"' in html
    assert 'id="position-gamma"' in html
    assert 'id="gamma-flip"' in html
    assert 'id="gamma-summary"' in html
    assert "CLOSED SNAPSHOT" in javascript
    assert 'localStorage.getItem("ndxp-chain-columns-v4")' in javascript
    assert 'styles.css?v=multi-index-gamma-1' in html
    assert 'app.js?v=multi-index-gamma-1' in html
    assert 'id="product"' in html
    assert 'id="market-gamma-regime"' in html
    assert 'id="market-gamma-flip"' in html
    assert "function renderMarketGamma(gamma)" in javascript
    assert "gamma-zone-positive" in css
    assert "value-harvest" in css
    assert "const high = low + selectedWidth()" in javascript
    assert 'classList.toggle("order-sell", opening)' in javascript
    assert 'classList.toggle("order-buy", !opening)' in javascript
    assert ".ticket-modal.order-buy .ticket-banner" in css
    assert 'id="breakeven-distance"' in html
    assert 'id="short-iv-move"' in html
    assert 'id="opening-net"' in html
    assert 'id="trade-pnl"' in html
    assert "Target closing debit" not in html
    assert "Target gross profit" not in html
    assert "OPTION_COMMISSION_PER_LEG_CONTRACT = 0.65" in javascript
    assert "selectedMetrics?.short_iv_expected_move" in javascript
    assert "function refreshOpenTicket(chain, analytics)" in javascript
    assert "ticketPriceFollowsMarket" in javascript
    assert "function pendingOpeningSpreadQuantity(spread)" in javascript
    assert "CLOSE & REVERSE" in javascript
    assert ".risk-grid div.hidden { display:none; }" in css
    assert ".range-strip.snapshot" in css
    assert "white-space:normal" in css
    assert 'data-ticket-price-step="-0.25"' in html
    assert 'data-ticket-price-step="0.25"' in html
    assert "`${payload.action} ${payload.quantity}" in javascript
    assert "ORDER ENTRY · LIVE TRADING" in javascript
    assert "function workingOrderCard(row)" in javascript
    assert "function recentOrderCard(row)" in javascript
    assert 'await lockedAction("replace"' in javascript
    assert 'close.addEventListener("click", () => openTicket("CLOSE"' in javascript
    assert "window.confirm(`Cancel Schwab order" not in javascript
