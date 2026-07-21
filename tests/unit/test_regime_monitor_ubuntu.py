from __future__ import annotations

import builtins
import importlib.util
import json
import subprocess
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution_core.track_b_live_market_data_symbols import load_track_b_live_market_data_symbols
from mgc_v05l.market_data.shared_live_ohlcv_store import SharedLiveOhlcvStore


class _FakeFlask:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def get(self, _path: str) -> object:
        def decorator(func: object) -> object:
            return func

        return decorator

    def post(self, _path: str) -> object:
        def decorator(func: object) -> object:
            return func

        return decorator


def _fake_jsonify(payload: object) -> object:
    return SimpleNamespace(headers={}, payload=payload)


sys.modules.setdefault(
    "flask",
    SimpleNamespace(
        Flask=_FakeFlask,
        Response=lambda value, mimetype=None: value,
        jsonify=_fake_jsonify,
        request=SimpleNamespace(remote_addr="127.0.0.1", get_data=lambda **_kwargs: "{}"),
    ),
)

MODULE_PATH = Path(__file__).resolve().parents[2] / "regime_monitor_ubuntu" / "regime_monitor.py"
SPEC = importlib.util.spec_from_file_location("regime_monitor_ubuntu_app", MODULE_PATH)
assert SPEC is not None
regime_monitor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = regime_monitor
SPEC.loader.exec_module(regime_monitor)


def _agreement_chart_from_closes(closes: list[float]) -> dict[str, object]:
    base = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
    bars: list[dict[str, object]] = []
    previous = closes[0]
    for index, close in enumerate(closes):
        bars.append(
            {
                "time": (base + timedelta(minutes=5 * index)).isoformat(),
                "open": previous,
                "high": max(previous, close) + 1.0,
                "low": min(previous, close) - 1.0,
                "close": close,
                "volume": 100 + index,
            }
        )
        previous = close
    return {"bars": bars}


def _component(score: object, name: str) -> object:
    return next(component for component in score.components if component.name == name)


def _extract_dashboard_function(name: str) -> str:
    html = regime_monitor.DASHBOARD_HTML
    needle = f"    function {name}("
    start = html.find(needle)
    assert start != -1, f"{name} not found"
    brace = html.index("{", start)
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(brace, len(html)):
        char = html[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return html[start : index + 1]
    raise AssertionError(f"{name} body not closed")


def _run_dashboard_js(expression: str, *, functions: tuple[str, ...]) -> object:
    source = "\n".join(
        [
            "const window = globalThis.window = {};",
            'const DASHBOARD_TIME_ZONE = "America/New_York";',
            "const chartAxis = { minXLabelGap: 110 };",
            *(_extract_dashboard_function(name) for name in functions),
            f"console.log(JSON.stringify({expression}));",
        ]
    )
    result = subprocess.run(
        ["node", "-e", source],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_no_databento_price_reports_no_trade() -> None:
    state = regime_monitor.PriceRegimeState()

    snapshot = state.snapshot()

    assert snapshot.regime == "NO_TRADE"
    assert snapshot.confidence == 0
    assert snapshot.connection_status == "STARTING"


def test_databento_px_message_updates_formula() -> None:
    state = regime_monitor.PriceRegimeState()
    state.record_message(SimpleNamespace(px=100_000_000_000))
    state.record_message(SimpleNamespace(px=102_000_000_000))

    snapshot = state.snapshot()

    assert snapshot.regime == "LONG"
    assert snapshot.confidence == 0.0099
    assert snapshot.connection_status == "CONNECTED"


def test_databento_message_updates_candle_state_through_same_ingest_path(tmp_path: Path) -> None:
    candles = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=0,
    )
    state = regime_monitor.PriceRegimeState(candle_state=candles)

    state.record_message(SimpleNamespace(px=100_000_000_000, ts_event="2026-07-19T00:00:05+00:00"))

    payload = candles.payload()
    assert payload["source"] == "regime_monitor_databento_live"
    assert payload["bars"] == [
        {
            "time": "2026-07-19T00:05:00+00:00",
            "start": "2026-07-19T00:00:00+00:00",
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0,
            "volume": 1.0,
            "completed": False,
            "source_bar_count": 1,
        }
    ]
    assert (tmp_path / "candle_state.json").exists()


def test_databento_price_message_falls_back_when_px_absent() -> None:
    state = regime_monitor.PriceRegimeState()
    state.record_message(SimpleNamespace(price=102_000_000_000))
    state.record_message(SimpleNamespace(price=100_000_000_000))

    snapshot = state.snapshot()

    assert snapshot.regime == "SHORT"
    assert snapshot.confidence == 0.0099


def test_price_window_is_bounded_to_latest_20_values() -> None:
    state = regime_monitor.PriceRegimeState()
    for value in range(1, 22):
        state.record_message(SimpleNamespace(px=value * 1_000_000_000))

    snapshot = state.snapshot()

    expected_average = sum(range(2, 22)) / 20
    assert snapshot.regime == "LONG"
    assert snapshot.confidence == round(abs(21 - expected_average) / expected_average, 4)


def test_symbols_from_config_accepts_string_or_list() -> None:
    assert regime_monitor._symbols_from_config("MNQ.v.0, MES.v.0") == ("MNQ.v.0", "MES.v.0")
    assert regime_monitor._symbols_from_config(["MBT.v.0", "  "]) == ("MBT.v.0",)
    assert regime_monitor._symbols_from_config(None) == ("MNQ.v.0", "MES.v.0", "MGC.v.0", "MBT.v.0")


def test_default_instruments_use_verified_continuous_front_month_symbols() -> None:
    assert [(item.key, item.symbol) for item in regime_monitor.DEFAULT_INSTRUMENTS] == [
        ("MNQ", "MNQ.v.0"),
        ("MES", "MES.v.0"),
        ("MGC", "MGC.v.0"),
        ("MBT", "MBT.v.0"),
    ]
    assert regime_monitor.DatabentoFeedConfig(api_key="x").stype_in == "continuous"


def test_default_instruments_are_loaded_from_shared_track_b_catalog() -> None:
    catalog = load_track_b_live_market_data_symbols().by_symbol()
    expected = [
        (key, catalog[key].display_label, catalog[key].databento_symbol)
        for key in regime_monitor.DEFAULT_MONITOR_INSTRUMENT_KEYS
    ]

    assert regime_monitor.MONITOR_ONLY_TRACK_B_INSTRUMENT_KEYS == ("MBT",)
    assert [(item.key, item.name, item.symbol) for item in regime_monitor.DEFAULT_INSTRUMENTS] == expected
    assert regime_monitor.DatabentoFeedConfig(api_key="x").symbols == tuple(symbol for _, _, symbol in expected)


def test_multi_instrument_monitor_charts_prefer_shared_ohlcv_store_and_catalog_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.sqlite3"
    store = SharedLiveOhlcvStore(db_path)
    bars = [
        {
            "bar_start": "2026-07-20T06:00:00+00:00",
            "bar_end": "2026-07-20T06:05:00+00:00",
            "open": "22000",
            "high": "22010",
            "low": "21990",
            "close": "22005",
            "volume": 100,
            "completed": True,
        },
        {
            "bar_start": "2026-07-20T06:05:00+00:00",
            "bar_end": "2026-07-20T06:10:00+00:00",
            "open": "22005",
            "high": "22015",
            "low": "22000",
            "close": "22012",
            "volume": 110,
            "completed": True,
        },
    ]
    store.upsert_mapping_bars(symbol="MNQ", timeframe="5m", bars=bars, source="DATABENTO_REALTIME_PHASE1")

    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        bar_limit=72,
        shared_ohlcv_db_path=db_path,
    )
    payload = state.payload()
    mnq = payload["instruments"]["MNQ"]
    catalog_row = load_track_b_live_market_data_symbols().by_symbol()["MNQ"]

    assert payload["chart_source"]["shared_ohlcv_db_path"] == str(db_path)
    assert mnq["name"] == catalog_row.display_label
    assert mnq["symbol"] == catalog_row.databento_symbol
    assert mnq["chart"]["source"] == "track_b_shared_live_ohlcv_store"
    assert mnq["chart"]["latest_bar_age_seconds"] >= 0
    assert mnq["chart"]["generated_at"]
    assert [row["time"] for row in mnq["chart"]["bars"]] == [bar["bar_end"] for bar in bars]


def test_shared_store_regime_calculation_matches_prior_direct_feed_price_window() -> None:
    prices = [100.0, 102.0]
    direct_state = regime_monitor.PriceRegimeState()
    direct_state.record_message(SimpleNamespace(px=prices[0] * 1_000_000_000))
    direct_state.record_message(SimpleNamespace(px=prices[1] * 1_000_000_000))
    direct_snapshot = direct_state.snapshot()
    now = datetime(2026, 7, 20, 12, 10, tzinfo=timezone.utc)
    chart = {
        "source": "track_b_shared_live_ohlcv_store",
        "timeframe": "5m",
        "latest_bar_ts": "2026-07-20T12:10:00+00:00",
        "bars": [
            {"time": "2026-07-20T12:05:00+00:00", "close": prices[0]},
            {"time": "2026-07-20T12:10:00+00:00", "close": prices[1]},
        ],
    }

    calculation = regime_monitor.calculate_regime_from_chart_payload(chart, now=now)

    assert calculation.decision == direct_snapshot.regime == "LONG"
    assert calculation.confidence == direct_snapshot.confidence == 0.0099
    assert calculation.reason == "latest_close_vs_2_value_average"
    assert calculation.source_bar_timestamp == "2026-07-20T12:10:00+00:00"
    assert calculation.stale_reason is None
    assert calculation.error_reason is None


def test_shared_store_regime_calculation_exposes_unavailable_and_stale_states() -> None:
    now = datetime(2026, 7, 20, 12, 30, tzinfo=timezone.utc)

    missing = regime_monitor.calculate_regime_from_chart_payload(None, now=now)
    stale = regime_monitor.calculate_regime_from_chart_payload(
        {
            "source": "track_b_shared_live_ohlcv_store",
            "timeframe": "5m",
            "latest_bar_ts": "2026-07-20T12:00:00+00:00",
            "bars": [{"time": "2026-07-20T12:00:00+00:00", "close": 100.0}],
        },
        now=now,
    )

    assert missing.decision == "UNAVAILABLE"
    assert missing.confidence is None
    assert missing.error_reason == "MISSING_SHARED_CHART"
    assert stale.decision == "STALE"
    assert stale.confidence is None
    assert stale.stale_reason is not None


def test_multi_instrument_monitor_calculates_regime_from_fresh_shared_store(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.sqlite3"
    store = SharedLiveOhlcvStore(db_path)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    bars = []
    for index, close in enumerate([100.0, 102.0]):
        end = now - timedelta(minutes=5 * (1 - index))
        bars.append(
            {
                "bar_start": (end - timedelta(minutes=5)).isoformat(),
                "bar_end": end.isoformat(),
                "open": str(close),
                "high": str(close + 1),
                "low": str(close - 1),
                "close": str(close),
                "volume": 100 + index,
                "completed": True,
            }
        )
    store.upsert_mapping_bars(symbol="MNQ", timeframe="5m", bars=bars, source="DATABENTO_REALTIME_PHASE1")
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        bar_limit=72,
        shared_ohlcv_db_path=db_path,
    )

    mnq = state.payload()["instruments"]["MNQ"]

    assert mnq["chart"]["source"] == "track_b_shared_live_ohlcv_store"
    assert mnq["chart"]["bar_count"] == 2
    assert mnq["regime"] == "LONG"
    assert mnq["confidence"] == 0.0099
    assert mnq["regime_calculation"]["decision"] == "LONG"
    assert mnq["regime_source_bar_timestamp"] == bars[-1]["bar_end"]
    assert mnq["directional_agreement_score"]["schema_version"] == "regime_monitor_directional_agreement_score_v1"


def test_directional_agreement_score_strong_bullish_alignment() -> None:
    chart = _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)])

    score = regime_monitor.calculate_directional_agreement_score(chart, trend="LONG")

    assert score.score == 100
    assert score.band == "VERY STRONG"
    assert score.points_awarded == score.points_available == 100.0


def test_directional_agreement_score_strong_bearish_alignment() -> None:
    chart = _agreement_chart_from_closes([200.0 - index * 2.0 for index in range(60)])

    score = regime_monitor.calculate_directional_agreement_score(chart, trend="SHORT")

    assert score.score == 100
    assert score.band == "VERY STRONG"
    assert score.points_awarded == score.points_available == 100.0


def test_directional_agreement_score_conflicting_indicators_reduce_score() -> None:
    bullish_chart = _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)])

    score = regime_monitor.calculate_directional_agreement_score(bullish_chart, trend="SHORT")

    assert score.score == 25
    assert score.band == "DEVELOPING"
    assert _component(score, "price_vs_vwap").points == 0
    assert _component(score, "price_vs_ma20").points == 0
    assert _component(score, "mom_sign_magnitude").points == 0


def test_directional_agreement_score_low_adx_caps_strength_component() -> None:
    closes = [100.0 + (((index % 4) - 1.5) * 0.1) for index in range(60)]
    score = regime_monitor.calculate_directional_agreement_score(_agreement_chart_from_closes(closes), trend="LONG")

    assert score.score == 18
    assert score.band == "WEAK"
    assert _component(score, "adx_level").points == 0
    assert _component(score, "adx_level").value < 10
    assert _component(score, "ma20_slope").reason == "aligned_scaled_by_magnitude"
    assert _component(score, "ma20_slope").points < _component(score, "ma20_slope").max_points


def test_directional_agreement_score_missing_inputs_renormalizes_available_components() -> None:
    score = regime_monitor.calculate_directional_agreement_score(
        _agreement_chart_from_closes([100.0, 101.0, 102.0]),
        trend="LONG",
    )

    assert score.score == 90
    assert score.band == "VERY STRONG"
    assert score.points_available == 24.5
    assert {component.name for component in score.components} == {
        "price_vs_vwap",
        "vwap_slope",
        "recent_side_persistence",
    }


def test_directional_agreement_score_exact_zero_and_hundred_bounds() -> None:
    zero = regime_monitor.calculate_directional_agreement_score(
        _agreement_chart_from_closes([100.0] * 60),
        trend="SHORT",
    )
    hundred = regime_monitor.calculate_directional_agreement_score(
        _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)]),
        trend="LONG",
    )

    assert zero.score == 0
    assert zero.band == "WEAK"
    assert hundred.score == 100
    assert hundred.band == "VERY STRONG"


def test_directional_agreement_score_long_short_symmetry() -> None:
    bullish_chart = _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)])
    bearish_chart = _agreement_chart_from_closes([200.0 - index * 2.0 for index in range(60)])

    long_score = regime_monitor.calculate_directional_agreement_score(bullish_chart, trend="LONG")
    short_score = regime_monitor.calculate_directional_agreement_score(bearish_chart, trend="SHORT")

    assert long_score.score == short_score.score == 100
    assert [
        (component.name, component.points, component.max_points)
        for component in long_score.components
    ] == [
        (component.name, component.points, component.max_points)
        for component in short_score.components
    ]


def test_directional_agreement_score_flat_market_earns_low_scaled_alignment() -> None:
    chart = _agreement_chart_from_closes([100.0 + index * 0.001 for index in range(60)])

    score = regime_monitor.calculate_directional_agreement_score(chart, trend="LONG")

    assert score.score < 45
    assert _component(score, "price_vs_vwap").points < _component(score, "price_vs_vwap").max_points
    assert _component(score, "vwap_slope").points < _component(score, "vwap_slope").max_points


def test_trade_quality_score_formula_and_freshness_components() -> None:
    now = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
    chart = _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)])
    chart["generated_at"] = now.isoformat()
    agreement = regime_monitor.calculate_directional_agreement_score(chart, trend="LONG")

    score = regime_monitor.calculate_trade_quality_score(chart, directional_agreement=agreement, now=now)

    assert score.score == 100
    assert score.freshness_state == "fresh"
    assert _component(score, "directional_agreement").points == 60.0
    assert _component(score, "adx_strength").points == 20.0
    assert _component(score, "vwap_distance_atr").points == 15.0
    assert _component(score, "data_freshness").points == 5.0


def test_trade_quality_score_degraded_stale_and_missing_data_behavior() -> None:
    now = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
    chart = _agreement_chart_from_closes([100.0 + index * 2.0 for index in range(60)])
    agreement = regime_monitor.calculate_directional_agreement_score(chart, trend="LONG")

    chart["generated_at"] = (now - timedelta(seconds=5)).isoformat()
    degraded = regime_monitor.calculate_trade_quality_score(chart, directional_agreement=agreement, now=now)
    chart["generated_at"] = (now - timedelta(seconds=11)).isoformat()
    stale = regime_monitor.calculate_trade_quality_score(chart, directional_agreement=agreement, now=now)
    unavailable = regime_monitor.calculate_trade_quality_score({}, directional_agreement=agreement, now=now)

    assert degraded.freshness_state == "degraded"
    assert _component(degraded, "data_freshness").points == 2.0
    assert stale.freshness_state == "stale"
    assert _component(stale, "data_freshness").points == 0.0
    assert unavailable.score is None
    assert unavailable.unavailable_reason == "chart_bars_unavailable"


def test_databento_feed_can_be_disabled_for_shared_store_display_cutover() -> None:
    assert regime_monitor.resolve_databento_feed_enabled(config={}) is True
    assert regime_monitor.resolve_databento_feed_enabled(config={"databento_feed_enabled": False}) is False
    assert regime_monitor.resolve_databento_feed_enabled(config={"databento_feed_enabled": "off"}) is False
    assert regime_monitor.resolve_databento_feed_enabled(config={"databento_feed_enabled": "true"}) is True


def test_successful_databento_stream_clears_package_missing_status() -> None:
    state = regime_monitor.PriceRegimeState()
    state.mark_status("DATABENTO_PACKAGE_MISSING", "old startup failure")
    stop = threading.Event()

    class FakeLiveClient:
        def __init__(self, *, key: str) -> None:
            self.key = key
            self.subscribed = False

        def subscribe(self, **_kwargs: object) -> None:
            self.subscribed = True

        def __iter__(self) -> object:
            yield SimpleNamespace(px=100_000_000_000, ts_event="2026-07-19T00:00:05+00:00")
            stop.set()

        def close(self) -> None:
            pass

    regime_monitor.run_databento_feed(
        config=regime_monitor.DatabentoFeedConfig(api_key="test_key"),
        state=state,
        stop=stop,
        live_factory=FakeLiveClient,
    )

    snapshot = state.snapshot()
    assert snapshot.connection_status == "CONNECTED"
    assert snapshot.error is None


def test_successful_multi_instrument_stream_clears_package_missing_status(tmp_path: Path) -> None:
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        persist_interval=0,
    )
    state.mark_all("DATABENTO_PACKAGE_MISSING", "old startup failure")
    stop = threading.Event()

    class FakeLiveClient:
        def __init__(self, *, key: str) -> None:
            self.key = key

        def subscribe(self, **kwargs: object) -> None:
            assert kwargs["symbols"] == ["MNQ.v.0", "MES.v.0", "MGC.v.0", "MBT.v.0"]
            assert kwargs["stype_in"] == "continuous"

        def __iter__(self) -> object:
            yield SimpleNamespace(symbol="MNQ.v.0", px=100_000_000_000, ts_event="2026-07-19T00:00:05+00:00")
            stop.set()

        def close(self) -> None:
            pass

    regime_monitor.run_databento_feed(
        config=regime_monitor.DatabentoFeedConfig(
            api_key="test_key",
            symbols=tuple(item.symbol for item in regime_monitor.DEFAULT_INSTRUMENTS),
            stype_in="continuous",
        ),
        state=state,
        stop=stop,
        live_factory=FakeLiveClient,
    )

    payload = state.payload()["instruments"]
    assert payload["MNQ"]["connection_status"] == "CONNECTED"
    assert payload["MNQ"]["error"] is None
    assert all(item["connection_status"] != "DATABENTO_PACKAGE_MISSING" for item in payload.values())


def test_databento_import_failure_reports_package_missing(monkeypatch: object) -> None:
    state = regime_monitor.PriceRegimeState()
    original_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "databento":
            exc = ModuleNotFoundError("No module named 'databento'")
            exc.name = "databento"
            raise exc
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    regime_monitor.run_databento_feed(
        config=regime_monitor.DatabentoFeedConfig(api_key="test_key"),
        state=state,
        stop=threading.Event(),
    )

    snapshot = state.snapshot()
    assert snapshot.connection_status == "DATABENTO_PACKAGE_MISSING"
    assert snapshot.error == "Install the databento Python package"


def test_chart_payload_keeps_latest_72_ordered_bars(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=9999,
    )
    for index in range(75):
        source.record_trade(price=100 + index, event_time=_trade_time(index))

    payload = source.payload()

    assert payload["source"] == "regime_monitor_databento_live"
    assert payload["bar_count"] == 72
    assert payload["bars"][0]["time"] == "2026-07-19T00:20:00+00:00"
    assert payload["bars"][-1]["time"] == "2026-07-19T06:15:00+00:00"


def test_chart_payload_updates_forming_bar_from_live_trades(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=9999,
    )
    source.record_trade(price=101, event_time=datetime(2026, 7, 19, 0, 11, tzinfo=timezone.utc))
    source.record_trade(price=103, event_time=datetime(2026, 7, 19, 0, 12, tzinfo=timezone.utc))

    payload = source.payload()

    assert payload["bars"] == [{
        "time": "2026-07-19T00:15:00+00:00",
        "start": "2026-07-19T00:10:00+00:00",
        "open": 101.0,
        "high": 103.0,
        "low": 101.0,
        "close": 103.0,
        "volume": 2.0,
        "completed": False,
        "source_bar_count": 2,
    }]


def test_chart_payload_replaces_active_bar_by_bucket(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=9999,
    )
    source.record_trade(price=100.5, event_time=datetime(2026, 7, 19, 0, 6, tzinfo=timezone.utc))
    first = source.payload()
    source.record_trade(price=104, event_time=datetime(2026, 7, 19, 0, 7, tzinfo=timezone.utc))

    second = source.payload()

    assert first["bars"][-1]["time"] == second["bars"][-1]["time"] == "2026-07-19T00:10:00+00:00"
    assert first["bars"][-1]["close"] == 100.5
    assert second["bars"][-1]["close"] == 104.0
    assert second["bars"][-1]["high"] == 104.0


def test_candle_rollover_marks_previous_five_minute_bar_completed(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=9999,
    )
    source.record_trade(price=100, event_time=datetime(2026, 7, 19, 0, 4, 59, tzinfo=timezone.utc))
    source.record_trade(price=101, event_time=datetime(2026, 7, 19, 0, 5, 0, tzinfo=timezone.utc))

    payload = source.payload()

    assert payload["bars"][0]["time"] == "2026-07-19T00:05:00+00:00"
    assert payload["bars"][0]["completed"] is True
    assert payload["bars"][1]["time"] == "2026-07-19T00:10:00+00:00"
    assert payload["bars"][1]["completed"] is False


def test_candle_state_recovers_after_restart(tmp_path: Path) -> None:
    first = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=0,
    )
    first.record_trade(price=100, event_time=datetime(2026, 7, 19, 0, 0, tzinfo=timezone.utc))
    first.record_trade(price=101, event_time=datetime(2026, 7, 19, 0, 5, tzinfo=timezone.utc))

    second = regime_monitor.RollingCandleState(state_dir=tmp_path, symbol="MBT")

    payload = second.payload()
    assert payload["bar_count"] == 2
    assert payload["bars"][0]["completed"] is True
    assert payload["bars"][1]["completed"] is False


def test_multi_instrument_state_isolates_updates_and_rollovers(tmp_path: Path) -> None:
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        persist_interval=0,
    )

    state.record_message(SimpleNamespace(symbol="MNQ.v.0", px=100_000_000_000, ts_event="2026-07-19T00:04:59+00:00"))
    state.record_message(SimpleNamespace(symbol="MNQ.v.0", px=101_000_000_000, ts_event="2026-07-19T00:05:00+00:00"))
    state.record_message(SimpleNamespace(symbol="MES.v.0", px=50_000_000_000, ts_event="2026-07-19T00:01:00+00:00"))

    instruments = state.payload()["instruments"]

    assert instruments["MNQ"]["chart"]["bar_count"] == 2
    assert instruments["MNQ"]["chart"]["bars"][0]["completed"] is True
    assert instruments["MES"]["chart"]["bar_count"] == 1
    assert instruments["MGC"]["chart"]["bars"] == []
    assert instruments["MBT"]["chart"]["bars"] == []


def test_databento_instrument_id_mapping_routes_mnq_only(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    state.record_message(_db_trade(101, 100, "2026-07-19T00:00:05+00:00"))

    payload = state.payload()
    assert payload["instruments"]["MNQ"]["chart"]["bar_count"] == 1
    assert payload["instruments"]["MNQ"]["resolved_instrument_id"] == 101
    assert payload["instruments"]["MNQ"]["last_source_symbol"] == "MNQ.v.0"
    assert payload["instruments"]["MES"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MGC"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MBT"]["chart"]["bar_count"] == 0


def test_databento_instrument_id_mapping_routes_mes_only(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    state.record_message(_db_trade(102, 200, "2026-07-19T00:00:05+00:00"))

    payload = state.payload()
    assert payload["instruments"]["MES"]["chart"]["bar_count"] == 1
    assert payload["instruments"]["MNQ"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MGC"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MBT"]["chart"]["bar_count"] == 0


def test_databento_instrument_id_mapping_routes_mgc_only(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    state.record_message(_db_trade(103, 300, "2026-07-19T00:00:05+00:00"))

    payload = state.payload()
    assert payload["instruments"]["MGC"]["chart"]["bar_count"] == 1
    assert payload["instruments"]["MNQ"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MES"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MBT"]["chart"]["bar_count"] == 0


def test_databento_instrument_id_mapping_routes_mbt_only(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    state.record_message(_db_trade(104, 64_000, "2026-07-19T00:00:05+00:00"))

    payload = state.payload()
    assert payload["instruments"]["MBT"]["chart"]["bar_count"] == 1
    assert payload["instruments"]["MNQ"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MES"]["chart"]["bar_count"] == 0
    assert payload["instruments"]["MGC"]["chart"]["bar_count"] == 0


def test_interleaved_databento_instrument_ids_remain_isolated(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    state.record_message(_db_trade(104, 64_000, "2026-07-19T00:00:05+00:00"))
    state.record_message(_db_trade(101, 100, "2026-07-19T00:00:06+00:00"))
    state.record_message(_db_trade(102, 200, "2026-07-19T00:00:07+00:00"))
    state.record_message(_db_trade(103, 300, "2026-07-19T00:00:08+00:00"))
    state.record_message(_db_trade(101, 101, "2026-07-19T00:00:09+00:00"))

    payload = state.payload()
    assert _latest_close(payload, "MNQ") == 101.0
    assert _latest_close(payload, "MES") == 200.0
    assert _latest_close(payload, "MGC") == 300.0
    assert _latest_close(payload, "MBT") == 64000.0
    assert {key: item["chart"]["bar_count"] for key, item in payload["instruments"].items()} == {
        "MNQ": 1,
        "MES": 1,
        "MGC": 1,
        "MBT": 1,
    }


def test_unknown_databento_instrument_id_updates_no_panel(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    assert state.record_message(_db_trade(999, 64_000, "2026-07-19T00:00:05+00:00")) is None

    payload = state.payload()
    assert payload["routing"]["unmapped_record_count"] == 1
    assert payload["routing"]["last_unmapped_instrument_id"] == 999
    assert all(item["chart"]["bar_count"] == 0 for item in payload["instruments"].values())


def test_routing_diagnostics_expose_subscription_and_record_decisions(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)
    state.record_subscription(
        regime_monitor.DatabentoFeedConfig(
            api_key="test",
            dataset="GLBX.MDP3",
            schema="trades",
            symbols=("MNQ.v.0", "MES.v.0"),
            stype_in="continuous",
        )
    )

    assert state.record_message(_db_trade(101, 100, "2026-07-19T00:00:05+00:00")) == "MNQ"
    assert state.record_message(_db_trade(999, 200, "2026-07-19T00:00:06+00:00")) is None

    routing = state.payload()["routing"]
    assert routing["subscriptions"][0]["dataset"] == "GLBX.MDP3"
    assert routing["subscriptions"][0]["schema"] == "trades"
    assert routing["subscriptions"][0]["symbols"] == ["MNQ.v.0", "MES.v.0"]
    assert routing["subscriptions"][0]["stype_in"] == "continuous"
    assert routing["instrument_map"]["101"] == "MNQ"
    assert routing["unknown_instrument_ids"] == [999]
    assert routing["records_seen"] == 2
    assert routing["records_routed"] == 1
    assert routing["records_rejected"] == 1
    assert routing["first_records"][0]["accepted"] is True
    assert routing["first_records"][0]["resolved_symbol"] == "MNQ"
    assert routing["first_records"][1]["accepted"] is False
    assert routing["first_records"][1]["rejection_reason"] == "UNMAPPED_INSTRUMENT_ID"


def test_routing_diagnostics_keep_only_first_twenty_record_decisions(tmp_path: Path) -> None:
    state = _mapped_multi_state(tmp_path)

    for index in range(21):
        state.record_message(
            _db_trade(
                101,
                100 + index,
                f"2026-07-19T00:00:{index:02d}+00:00",
            )
        )

    routing = state.payload()["routing"]
    assert routing["records_seen"] == 21
    assert routing["records_routed"] == 21
    assert routing["records_rejected"] == 0
    assert len(routing["first_records"]) == 20
    assert routing["first_records"][0]["resolved_symbol"] == "MNQ"
    assert routing["first_records"][-1]["accepted"] is True


def test_multi_instrument_state_keeps_72_bars_per_instrument(tmp_path: Path) -> None:
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
    )
    for index in range(75):
        state.record_message(
            SimpleNamespace(
                symbol="MGC.v.0",
                px=(200 + index) * 1_000_000_000,
                ts_event=_trade_time(index).isoformat(),
            )
        )

    mgc = state.payload()["instruments"]["MGC"]["chart"]

    assert mgc["bar_count"] == 72
    assert mgc["bars"][0]["time"] == "2026-07-19T00:20:00+00:00"
    assert mgc["bars"][-1]["time"] == "2026-07-19T06:15:00+00:00"


def test_multi_instrument_persistence_recovers_all_four_instruments(tmp_path: Path) -> None:
    first = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        persist_interval=0,
    )
    for index, symbol in enumerate(["MNQ.v.0", "MES.v.0", "MGC.v.0", "MBT.v.0"]):
        first.record_message(
            SimpleNamespace(
                symbol=symbol,
                px=(100 + index) * 1_000_000_000,
                ts_event="2026-07-19T00:00:05+00:00",
            )
        )

    second = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
    )
    payload = second.payload()["instruments"]

    assert (tmp_path / "candle_state.json").exists()
    assert list(tmp_path.glob("*.tmp")) == []
    assert {key: item["chart"]["bar_count"] for key, item in payload.items()} == {
        "MNQ": 1,
        "MES": 1,
        "MGC": 1,
        "MBT": 1,
    }
    assert _latest_close({"instruments": payload}, "MNQ") == 100.0
    assert _latest_close({"instruments": payload}, "MES") == 101.0
    assert _latest_close({"instruments": payload}, "MGC") == 102.0
    assert _latest_close({"instruments": payload}, "MBT") == 103.0


def test_legacy_multi_instrument_state_schema_is_ignored_on_migration(tmp_path: Path) -> None:
    contaminated = {
        "schema_version": "regime_monitor_multi_candle_state_v1",
        "instruments": {
            key: {
                "candles": {
                    "completed_5m": [],
                    "current_5m": {
                        "time": "2026-07-19T00:05:00+00:00",
                        "start": "2026-07-19T00:00:00+00:00",
                        "open": 64000,
                        "high": 64000,
                        "low": 64000,
                        "close": 64000,
                    },
                }
            }
            for key in ("MNQ", "MES", "MGC", "MBT")
        },
    }
    (tmp_path / "candle_state.json").write_text(json.dumps(contaminated), encoding="utf-8")

    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
    )

    assert all(item["chart"]["bar_count"] == 0 for item in state.payload()["instruments"].values())


def test_one_instrument_error_does_not_affect_other_panels(tmp_path: Path) -> None:
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
    )
    state.record_message(SimpleNamespace(symbol="MNQ.v.0", px=100_000_000_000, ts_event="2026-07-19T00:00:05+00:00"))
    state.mark_instrument("MBT", "DISCONNECTED", "single panel failure")

    payload = state.payload()["instruments"]

    assert payload["MNQ"]["connection_status"] == "CONNECTED"
    assert payload["MNQ"]["chart"]["bar_count"] == 1
    assert payload["MBT"]["connection_status"] == "DISCONNECTED"
    assert payload["MBT"]["error"] == "single panel failure"
    assert payload["MES"]["connection_status"] == "STARTING"


def test_candle_state_atomic_write_leaves_no_temp_file(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=0,
    )

    source.record_trade(price=100, event_time=datetime(2026, 7, 19, 0, 0, tzinfo=timezone.utc))

    assert (tmp_path / "candle_state.json").exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_out_of_order_trade_does_not_corrupt_current_candle(tmp_path: Path) -> None:
    source = regime_monitor.RollingCandleState(
        state_dir=tmp_path,
        symbol="MBT",
        persist_interval=9999,
    )
    source.record_trade(price=105, event_time=datetime(2026, 7, 19, 0, 10, tzinfo=timezone.utc))
    source.record_trade(price=99, event_time=datetime(2026, 7, 19, 0, 4, tzinfo=timezone.utc))

    payload = source.payload()

    assert payload["bars"][-1]["time"] == "2026-07-19T00:15:00+00:00"
    assert payload["bars"][-1]["close"] == 105.0
    assert "ignored_out_of_order_trade" in payload["error"]


def test_default_state_dir_and_env_override(monkeypatch: object, tmp_path: Path) -> None:
    monkeypatch.delenv("REGIME_MONITOR_STATE_DIR", raising=False)
    assert regime_monitor.resolve_state_dir(config={}) == Path("/var/lib/regime-monitor")

    monkeypatch.setenv("REGIME_MONITOR_STATE_DIR", str(tmp_path / "state"))
    assert regime_monitor.resolve_state_dir(config={}) == tmp_path / "state"


def test_state_dir_config_override_is_supported_for_non_production_layouts(
    monkeypatch: object,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("REGIME_MONITOR_STATE_DIR", raising=False)

    root = regime_monitor.resolve_state_dir(config={"state_dir": str(tmp_path / "configured")})

    assert root == tmp_path / "configured"


def test_dashboard_axis_labels_are_kiosk_readable_and_spaced() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in html
    assert "grid-template-rows: repeat(2, minmax(0, 1fr))" in html
    assert 'const PANEL_ORDER = ["MNQ", "MES", "MGC", "MBT"]' in html
    assert html.count('class="chart"') == 1
    assert 'yLabelFont: "560 17px system-ui, sans-serif"' in html
    assert 'xLabelFont: "560 15px system-ui, sans-serif"' in html
    assert "calculateTimeLabelIndices(valid, plotWidth)" in html
    assert 'context.textAlign = first ? "left" : last ? "right" : "center"' in html
    assert "context.fillText(formatTimeLabel(bar.time), first ? left : x, height - chartAxis.xLabelBottomGap)" in html
    assert "return new Set(indices)" in html
    assert 'class="top"' in html
    assert 'class="last-price"' in html
    assert 'class="change" data-tone="flat"' in html
    assert 'class="signal-table"' in html
    assert 'class="chart-wrap"' in html
    assert 'class="indicators"' in html
    assert 'Market: <span id="footer-market">--' in html
    assert 'id="footer-session">--' in html
    assert 'AGE: <span id="footer-age">--' in html
    assert 'id="footer-data">--' in html
    assert "function updateFooterStatus(payload, instruments)" in html
    assert "VWAP&nbsp;&nbsp;" in html
    assert "MA20&nbsp;&nbsp;" in html
    assert "bottom: 42px;" in html
    assert 'class="rank-marker"' in html
    assert 'class="quality">QUALITY --' in html
    assert "RSI(14)" in html
    assert "ADX(14)" in html
    assert "MOM(10)" in html
    assert "VWAP Δ" in html
    assert "function instrumentCode(symbol, fallback)" in html
    assert "const regime = payload.regime || (calculation && calculation.decision) || \"UNAVAILABLE\"" in html
    assert "directionalAgreementDisplay(payload.directional_agreement_score)" in html
    assert "tradeQualityDisplay(payload.trade_quality_score)" in html
    assert "rankInstruments(instruments)" in html
    assert "function directionalAgreementDisplay(score)" in html
    assert "const metrics = chartMetrics(payload.chart || null)" in html

    antix_panel_width = (1920 - 24) / 2
    plot_width = antix_panel_width - 28 - 14 - 108
    candle_step = plot_width / 72
    hourly_indices = list(range(12, 72, 12))

    assert candle_step * 12 >= 110
    assert min((b - a) * candle_step for a, b in zip(hourly_indices, hourly_indices[1:])) >= 110


def test_dashboard_live_polling_is_cache_busted_non_overlapping_and_recovering() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert 'const DATA_ENDPOINT = "/data";' in html
    assert "function dataRequestUrl()" in html
    assert 'url.searchParams.set("_", String(Date.now()))' in html
    assert 'url.searchParams.set("seq", String(clientDiagnostics.pollSequence))' in html
    assert 'fetch(requestUrl, {' in html
    assert 'cache: "no-store"' in html
    assert '"Cache-Control": "no-store"' in html
    assert "clientDiagnostics.requestInFlight" in html
    assert "clientDiagnostics.skippedOverlapCount += 1" in html
    assert "window.setTimeout(pollOnce, POLL_INTERVAL_MS)" in html
    assert "setInterval(pollOnce, POLL_INTERVAL_MS)" not in html
    assert "finally {" in html
    assert "clientDiagnostics.requestInFlight = false" in html
    assert "scheduleNextPoll()" in html
    poll_start = html.index("    async function pollOnce()")
    poll_end = html.index("    function scheduleNextPoll()", poll_start)
    assert "DASHBOARD_DISCONNECTED" not in html[poll_start:poll_end]


def test_dashboard_live_polling_has_browser_heartbeat_and_diagnostics() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert 'id="footer-data-heartbeat"' in html
    assert ".data-heartbeat.pulse" in html
    assert "window.__REGIME_MONITOR_CLIENT_DIAGNOSTICS = clientDiagnostics" in html
    assert "lastSuccessfulFetchAt" in html
    assert "lastAcceptedPayloadTimestamp" in html
    assert "consecutiveFailureCount" in html
    assert "lastClientRenderError" in html
    assert "function sendClientDiagnostic(entry)" in html
    assert 'navigator.sendBeacon("/client-diagnostics"' in html
    assert "function pulseHeartbeat(status)" in html
    assert "pulseHeartbeat(clientDiagnostics.lastStatus || \"LIVE\")" in html
    assert "window.__REGIME_MONITOR_DEBUG" in html
    assert "simulateFetchFailureOnce()" in html
    assert "simulateMalformedInstrumentOnce(key = PANEL_ORDER[0])" in html


def test_dashboard_render_errors_are_isolated_to_one_panel() -> None:
    html = regime_monitor.DASHBOARD_HTML
    update_dashboard = _extract_dashboard_function("updateDashboard")

    assert "try {" in update_dashboard
    assert "updatePanel(key, instruments[key] || {}, ranks.get(key) || null)" in update_dashboard
    assert "renderErrors.push({ instrument: key, message: error.message })" in update_dashboard
    assert "recordRenderError(key, error)" in update_dashboard
    assert "renderPanelError(key, instruments[key] || {}, error)" in update_dashboard
    assert "sendClientDiagnostic({ event: \"render\", ...entry })" in update_dashboard
    assert "payload = {" in _extract_dashboard_function("updatePanel")
    assert "MALFORMED_INSTRUMENT_PAYLOAD" in _extract_dashboard_function("updatePanel")


def test_dashboard_latest_source_timestamp_matches_age_candidates() -> None:
    values = _run_dashboard_js(
        """
        (() => {
          const payload = { generated_at: "2026-07-21T12:00:09Z" };
          const instruments = {
            MNQ: { chart: { latest_bar_ts: "2026-07-21T12:00:00Z", generated_at: "2026-07-21T12:00:10Z" } },
            MES: { regime_source_bar_timestamp: "2026-07-21T12:05:00Z", received_at: "2026-07-21T12:00:08Z" },
            MGC: { regime_calculated_at: "2026-07-21T12:00:05Z" }
          };
          return latestSourceTimestamp(payload, instruments);
        })()
        """,
        functions=("newestTimestampValue", "latestSourceTimestamp"),
    )

    assert values == "2026-07-21T12:05:00Z"


def test_dashboard_chart_geometry_keeps_latest_mnq_mes_candles_inside_panel() -> None:
    left = 14
    right = 108
    top = 34
    bottom = 62
    width = 612
    height = 320
    bar_count = 72
    plot_width = width - left - right
    plot_height = height - top - bottom
    price_plot_height = int(plot_height * 0.76)
    candle_step = plot_width / bar_count
    body_width = max(2, min(12, candle_step * 0.58))
    latest_x = left + candle_step * (bar_count - 1) + candle_step / 2

    assert latest_x + body_width / 2 < width - right
    assert latest_x - body_width / 2 > left

    span = 40.0
    padding = max(span * 0.16, span / price_plot_height * 20)
    high_y = top + (padding / (span + 2 * padding)) * price_plot_height
    low_y = top + ((span + padding) / (span + 2 * padding)) * price_plot_height

    assert high_y - top >= 20
    assert (top + price_plot_height) - low_y >= 20

    raw_badge_y = high_y - 10
    clamped_badge_y = max(top + 4, min(top + price_plot_height - 20, raw_badge_y))
    assert top <= clamped_badge_y <= top + price_plot_height - 20


def test_dashboard_panels_prevent_overflow_at_supported_sizes() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert ".panel {" in html
    assert "min-width: 0;" in html
    assert "min-height: 0;" in html
    assert "overflow: hidden;" in html
    assert "grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.7fr)" in html
    assert "grid-template-columns: minmax(0, 1fr) auto" in html
    assert "grid-template-columns: repeat(4, minmax(0, 1fr))" in html
    assert "white-space: nowrap;" in html
    assert "text-overflow: ellipsis;" in html
    assert "font-variant-numeric: tabular-nums;" in html
    assert ".price-badge.hidden { display: none; }" in html


def test_dashboard_preserves_runtime_field_mapping_for_direction_and_chart() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert "payload.regime" in html
    assert "payload.regime_calculation" in html
    assert "payload.directional_agreement_score" in html
    assert "payload.trade_quality_score" in html
    assert "payload.chart || null" in html
    assert "chartMetrics(payload.chart || null)" in html
    assert "payload.regime_source_bar_timestamp || metrics.latestTime" in html
    assert 'if (regime === "LONG") return { trend: "LONG \\\\u2191", bias: "BULLISH", tone: "long" }'.replace("\\\\", "\\") in html
    assert 'if (regime === "SHORT") return { trend: "SHORT \\\\u2193", bias: "BEARISH", tone: "short" }'.replace("\\\\", "\\") in html
    assert 'if (regime === "NO_TRADE") return { trend: "FLAT", bias: "NEUTRAL", tone: "flat" }' in html
    assert '<div class="metric-label">Regime</div>' in html


def test_dashboard_mockup_indicators_are_derived_from_chart_bars() -> None:
    html = regime_monitor.DASHBOARD_HTML

    assert "function movingAverageSeries(bars, period)" in html
    assert "function vwapSeries(bars)" in html
    assert "function calculateTechnicalMetrics(bars)" in html
    assert "function calculateAdx(bars, period)" in html
    assert "const ma20 = movingAverage(valid, 20)" in html
    assert "const vwap = currentVwap(valid)" in html
    assert "const technicals = calculateTechnicalMetrics(valid)" in html
    assert "updateText(nodes, \"rsi\", metrics.rsiText)" in html
    assert "updateText(nodes, \"adx\", metrics.adxText)" in html
    assert "updateText(nodes, \"mom\", metrics.momText)" in html
    assert "updateText(nodes, \"vwapDelta\", metrics.vwapDeltaText)" in html
    assert "(latestClose - vwap) / technicals.atr" in html


def test_dashboard_formats_display_prices_with_two_decimals_and_separators() -> None:
    values = _run_dashboard_js(
        "[23418.25, 6512.75, 3421.8, 117245.5, -117245.5].map(formatPrice)",
        functions=("formatPrice",),
    )

    assert values == ["23,418.25", "6,512.75", "3,421.80", "117,245.50", "-117,245.50"]


def test_dashboard_rsi_boundary_colors_use_numeric_thresholds() -> None:
    tones = _run_dashboard_js(
        "[19.9, 20.0, 39.9, 40.0, 59.9, 60.0, 80.0, 80.1].map(rsiTone)",
        functions=("rsiTone",),
    )

    assert tones == ["short", "orange", "orange", "yellow", "yellow", "lime", "lime", "long"]


def test_dashboard_score_tones_and_rank_tie_breakers_are_deterministic() -> None:
    result = _run_dashboard_js(
        """
        ({
          tones: [0, 24, 25, 44, 45, 64, 65, 79, 80, 100].map(scoreBandTone),
          ranks: Array.from(rankInstruments({
            MES: {
              trade_quality_score: { score: 80, components: [{ name: "adx_strength", value: 25 }] },
              directional_agreement_score: { score: 70 }
            },
            MNQ: {
              trade_quality_score: { score: 80, components: [{ name: "adx_strength", value: 25 }] },
              directional_agreement_score: { score: 75 }
            },
            MGC: {
              trade_quality_score: { score: 80, components: [{ name: "adx_strength", value: 30 }] },
              directional_agreement_score: { score: 75 }
            },
            MBT: {
              trade_quality_score: { score: null, components: [] },
              directional_agreement_score: { score: null }
            }
          }).entries()).map(([key, value]) => [key, value.rank])
        })
        """,
        functions=("scoreBandTone", "numericScore", "tradeQualityComponentValue", "rankInstruments"),
    )

    assert result["tones"] == [
        "weak",
        "weak",
        "developing",
        "developing",
        "moderate",
        "moderate",
        "strong",
        "strong",
        "very-strong",
        "very-strong",
    ]
    assert result["ranks"] == [["MGC", 1], ["MNQ", 2], ["MES", 3], ["MBT", 4]]


def test_dashboard_footer_age_uses_latest_source_timestamp() -> None:
    ages = _run_dashboard_js(
        """
        (() => {
          window.__REGIME_MONITOR_NOW_OVERRIDE = "2026-07-21T12:00:10Z";
          return [
            latestSourceAgeSeconds({}, { MNQ: { chart: { latest_bar_ts: "2026-07-21T12:00:00Z", generated_at: "2026-07-21T12:00:09.600Z" } } }),
            latestSourceAgeSeconds({ generated_at: "2026-07-21T12:00:01Z" }, {
              MNQ: { chart: { latest_bar_ts: "2026-07-21T11:55:00Z", generated_at: "2026-07-21T12:00:06Z" } },
              MES: { regime_source_bar_timestamp: "2026-07-21T12:00:00Z", received_at: "2026-07-21T12:00:08Z" }
            }),
            latestSourceAgeSeconds({}, {})
          ];
        })()
        """,
        functions=("currentDashboardDate", "newestTimestampValue", "latestSourceTimestamp", "latestSourceAgeSeconds"),
    )

    assert ages == [10, 10, None]


def test_dashboard_footer_source_market_age_state_uses_five_minute_bar_tolerance() -> None:
    states = _run_dashboard_js(
        "[0, 389.9, 390, 390.1, 900, 900.1, null].map(sourceMarketAgeState)",
        functions=("sourceMarketAgeState",),
    )

    assert states == ["fresh", "fresh", "fresh", "degraded", "degraded", "stale", "stale"]


def test_dashboard_vwap_delta_tile_displays_signed_atr_distance() -> None:
    result = _run_dashboard_js(
        """
        (() => {
          const upBars = Array.from({ length: 30 }, (_, index) => ({
            time: new Date(Date.parse("2026-07-21T12:00:00Z") + index * 300000).toISOString(),
            open: 100 + index,
            high: 101 + index,
            low: 99 + index,
            close: 100 + index,
            volume: 100
          }));
          const flatBars = Array.from({ length: 30 }, (_, index) => ({
            time: new Date(Date.parse("2026-07-21T12:00:00Z") + index * 300000).toISOString(),
            open: 100,
            high: 100,
            low: 100,
            close: 100,
            volume: 100
          }));
          const up = chartMetrics({ bars: upBars });
          const flat = chartMetrics({ bars: flatBars });
          return {
            upText: up.vwapDeltaText,
            upTone: up.vwapDeltaTone,
            flatText: flat.vwapDeltaText,
            flatTone: flat.vwapDeltaTone
          };
        })()
        """,
        functions=(
            "chartMetrics",
            "movingAverage",
            "currentVwap",
            "vwapSeries",
            "calculateTechnicalMetrics",
            "calculateAdx",
            "formatPrice",
            "formatDelta",
            "formatVolume",
            "rsiTone",
        ),
    )

    assert result["upText"].startswith("+")
    assert result["upText"].endswith(" ATR")
    assert result["upTone"] == "long"
    assert result["flatText"] == "N/A"
    assert result["flatTone"] == "neutral"


def test_dashboard_hourly_tick_labels_stay_on_clock_hours_when_window_shifts() -> None:
    expression = """
      ["2026-07-21T11:23:00Z", "2026-07-21T11:58:00Z", "2026-07-21T12:00:00Z"].map((start) => {
        const startMs = Date.parse(start);
        const bars = Array.from({ length: 36 }, (_, index) => ({ time: new Date(startMs + index * 5 * 60 * 1000).toISOString() }));
        const indices = Array.from(calculateTimeLabelIndices(bars, 1200));
        return indices.map((index) => ({ index, label: formatTimeLabel(bars[index].time), minute: zonedTimeParts(bars[index].time).minute }));
      })
    """
    cases = _run_dashboard_js(
        expression,
        functions=("zonedTimeParts", "calculateTimeLabelIndices", "formatTimeLabel"),
    )

    assert cases[0][0]["label"] == "08:00 AM"
    assert cases[0][0]["minute"] == 3
    assert cases[1][0]["label"] == "08:00 AM"
    assert cases[1][0]["minute"] == 3
    assert cases[2][0] == {"index": 0, "label": "08:00 AM", "minute": 0}
    for case in cases:
        assert all(item["label"][3:5] == "00" for item in case)


def test_dashboard_market_session_classification_regular_schedule() -> None:
    states = _run_dashboard_js(
        """
        [
          "2026-07-21T01:30:00-04:00",
          "2026-07-21T04:00:00-04:00",
          "2026-07-21T08:30:00-04:00",
          "2026-07-21T10:00:00-04:00",
          "2026-07-21T16:30:00-04:00",
          "2026-07-21T17:30:00-04:00",
          "2026-07-25T12:00:00-04:00",
          "2026-07-26T17:30:00-04:00",
          "2026-07-26T18:00:00-04:00"
        ].map((value) => marketSessionState(new Date(value)))
        """,
        functions=("zonedTimeParts", "marketSessionState"),
    )

    assert states == [
        {"market": "OPEN", "session": "ASIA"},
        {"market": "OPEN", "session": "EUROPE"},
        {"market": "OPEN", "session": "US PREMARKET"},
        {"market": "OPEN", "session": "US RTH"},
        {"market": "OPEN", "session": "US AFTER HOURS"},
        {"market": "MAINTENANCE", "session": "MAINTENANCE"},
        {"market": "CLOSED", "session": "CLOSED"},
        {"market": "CLOSED", "session": "CLOSED"},
        {"market": "OPEN", "session": "ASIA"},
    ]


def test_monitor_files_do_not_hard_code_patrick_home_or_magic_output_paths() -> None:
    repo_root = MODULE_PATH.parents[1]
    checked = [
        repo_root / "regime_monitor_ubuntu" / "regime_monitor.py",
        repo_root / "regime_monitor_ubuntu" / "config.json.example",
        repo_root / "regime_monitor_ubuntu" / "README.md",
    ]
    forbidden = (
        "/Users/" + "patrick",
        "/home/" + "patrick",
        "outputs/" + "track_b_execution_core",
        "phase1_runtime_" + "market_data",
        "REGIME_MONITOR_" + "REPO_ROOT",
    )

    for path in checked:
        text = path.read_text(encoding="utf-8")
        for value in forbidden:
            assert value not in text


def _trade_time(index: int) -> datetime:
    return datetime(2026, 7, 19, tzinfo=timezone.utc) + timedelta(minutes=index * 5)


def _mapped_multi_state(tmp_path: Path) -> object:
    state = regime_monitor.MultiInstrumentMonitorState(
        instruments=regime_monitor.DEFAULT_INSTRUMENTS,
        state_dir=tmp_path,
        persist_interval=0,
    )
    for instrument_id, symbol, raw_symbol in (
        (101, "MNQ.v.0", "MNQU6"),
        (102, "MES.v.0", "MESU6"),
        (103, "MGC.v.0", "MGCQ6"),
        (104, "MBT.v.0", "MBTN6"),
    ):
        state.record_message(
            SimpleNamespace(
                hd=SimpleNamespace(instrument_id=instrument_id),
                stype_in_symbol=symbol,
                stype_out_symbol=raw_symbol,
            )
        )
    return state


def _db_trade(instrument_id: int, price: float, ts_event: str) -> object:
    return SimpleNamespace(
        hd=SimpleNamespace(instrument_id=instrument_id),
        px=price * 1_000_000_000,
        ts_event=ts_event,
    )


def _latest_close(payload: dict[str, object], key: str) -> float:
    instruments = payload["instruments"]
    bars = instruments[key]["chart"]["bars"]
    return bars[-1]["close"]
