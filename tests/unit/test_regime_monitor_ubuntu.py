from __future__ import annotations

import builtins
import importlib.util
import json
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


class _FakeFlask:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def get(self, _path: str) -> object:
        def decorator(func: object) -> object:
            return func

        return decorator


def _fake_jsonify(payload: object) -> object:
    return SimpleNamespace(headers={}, payload=payload)


sys.modules.setdefault(
    "flask",
    SimpleNamespace(Flask=_FakeFlask, Response=lambda value, mimetype=None: value, jsonify=_fake_jsonify),
)

MODULE_PATH = Path(__file__).resolve().parents[2] / "regime_monitor_ubuntu" / "regime_monitor.py"
SPEC = importlib.util.spec_from_file_location("regime_monitor_ubuntu_app", MODULE_PATH)
assert SPEC is not None
regime_monitor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = regime_monitor
SPEC.loader.exec_module(regime_monitor)


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
    assert 'yLabelFont: "500 22px system-ui, sans-serif"' in html
    assert 'xLabelFont: "500 20px system-ui, sans-serif"' in html
    assert "calculateTimeLabelIndices(valid.length, plotWidth)" in html
    assert "return new Set(indices)" in html

    antix_panel_width = (1920 - 24) / 2
    plot_width = antix_panel_width - 24 - 94 - 72
    candle_step = plot_width / 72
    max_labels = max(2, int(plot_width // 132))
    tick_every = max(1, (72 + max_labels - 1) // max_labels)
    label_indices = []
    for index in range(0, 72, tick_every):
        distance_to_final = (71 - index) * candle_step
        if index == 0 or distance_to_final >= 132:
            label_indices.append(index)
    if (71 - label_indices[-1]) * candle_step >= 132:
        label_indices.append(71)
    else:
        label_indices[-1] = 71

    assert tick_every >= 6
    assert len(label_indices) <= max_labels
    assert label_indices[-1] == 71
    assert min((b - a) * candle_step for a, b in zip(label_indices, label_indices[1:])) >= 132


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
