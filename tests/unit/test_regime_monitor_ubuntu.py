from __future__ import annotations

import importlib.util
import json
import sys
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
    assert regime_monitor._symbols_from_config("MBT.FUT, MES.FUT") == ("MBT.FUT", "MES.FUT")
    assert regime_monitor._symbols_from_config(["MBT.FUT", "  "]) == ("MBT.FUT",)
    assert regime_monitor._symbols_from_config(None) == ("MBT.FUT",)


def test_chart_payload_keeps_latest_72_ordered_bars(tmp_path: Path) -> None:
    _write_candles(tmp_path, "MBT", "5m", [_bar(index) for index in range(75)])
    _write_candles(tmp_path, "MBT", "1m", [])
    source = regime_monitor.CanonicalCandleSource(
        config=regime_monitor.ChartConfig(runtime_candle_root=tmp_path, symbol="MBT")
    )

    payload = source.payload()

    assert payload["source"] == "execution_core_phase1_runtime_market_data"
    assert payload["bar_count"] == 72
    assert payload["bars"][0]["time"] == "2026-07-19T00:20:00+00:00"
    assert payload["bars"][-1]["time"] == "2026-07-19T06:15:00+00:00"


def test_chart_payload_appends_forming_bar_from_canonical_one_minute(tmp_path: Path) -> None:
    _write_candles(tmp_path, "MBT", "5m", [_bar(0), _bar(1)])
    _write_candles(
        tmp_path,
        "MBT",
        "1m",
        [
            _one_minute_bar("2026-07-19T00:11:00+00:00", 101, 103, 100, 102),
            _one_minute_bar("2026-07-19T00:12:00+00:00", 102, 104, 101, 103),
        ],
    )
    source = regime_monitor.CanonicalCandleSource(
        config=regime_monitor.ChartConfig(runtime_candle_root=tmp_path, symbol="MBT")
    )

    payload = source.payload()

    assert payload["bars"][-1] == {
        "time": "2026-07-19T00:15:00+00:00",
        "start": "2026-07-19T00:10:00+00:00",
        "open": 101.0,
        "high": 104.0,
        "low": 100.0,
        "close": 103.0,
        "volume": 2.0,
        "completed": False,
        "source_bar_count": 2,
    }


def test_chart_payload_replaces_active_bar_by_bucket(tmp_path: Path) -> None:
    _write_candles(tmp_path, "MBT", "5m", [_bar(0)])
    _write_candles(
        tmp_path,
        "MBT",
        "1m",
        [_one_minute_bar("2026-07-19T00:06:00+00:00", 100, 101, 99, 100.5)],
    )
    source = regime_monitor.CanonicalCandleSource(
        config=regime_monitor.ChartConfig(runtime_candle_root=tmp_path, symbol="MBT")
    )
    first = source.payload()
    _write_candles(
        tmp_path,
        "MBT",
        "1m",
        [
            _one_minute_bar("2026-07-19T00:06:00+00:00", 100, 101, 99, 100.5),
            _one_minute_bar("2026-07-19T00:07:00+00:00", 100.5, 105, 100, 104),
        ],
    )

    second = source.payload()

    assert first["bars"][-1]["time"] == second["bars"][-1]["time"] == "2026-07-19T00:10:00+00:00"
    assert first["bar_count"] == second["bar_count"] == 2
    assert first["bars"][-1]["close"] == 100.5
    assert second["bars"][-1]["close"] == 104.0
    assert second["bars"][-1]["high"] == 105.0


def test_chart_payload_has_stable_shape_when_artifacts_missing(tmp_path: Path) -> None:
    source = regime_monitor.CanonicalCandleSource(
        config=regime_monitor.ChartConfig(runtime_candle_root=tmp_path, symbol="MBT")
    )

    payload = source.payload()

    assert payload["schema_version"] == "regime_monitor_canonical_5m_chart_v1"
    assert payload["symbol"] == "MBT"
    assert payload["timeframe"] == "5m"
    assert payload["bar_limit"] == 72
    assert payload["bars"] == []
    assert "missing canonical candle artifact" in payload["error"]


def test_default_chart_root_derives_from_app_root(monkeypatch: object, tmp_path: Path) -> None:
    monkeypatch.delenv("REGIME_MONITOR_REPO_ROOT", raising=False)

    root = regime_monitor.resolve_chart_runtime_candle_root(config={}, app_root=tmp_path / "app")

    assert root == tmp_path / "app" / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"


def test_chart_root_uses_configured_repo_root_env(monkeypatch: object, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setenv("REGIME_MONITOR_REPO_ROOT", str(repo_root))

    root = regime_monitor.resolve_chart_runtime_candle_root(config={}, app_root=tmp_path / "app")

    assert root == repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"


def test_chart_root_relative_override_resolves_under_authoritative_repo_root(
    monkeypatch: object,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    monkeypatch.setenv("REGIME_MONITOR_REPO_ROOT", str(repo_root))

    root = regime_monitor.resolve_chart_runtime_candle_root(
        config={},
        app_root=tmp_path / "app",
        chart_root_override="custom/candles",
    )

    assert root == repo_root / "custom" / "candles"


def test_chart_root_absolute_override_is_preserved(monkeypatch: object, tmp_path: Path) -> None:
    monkeypatch.setenv("REGIME_MONITOR_REPO_ROOT", str(tmp_path / "repo"))
    override = tmp_path / "other" / "candles"

    root = regime_monitor.resolve_chart_runtime_candle_root(
        config={},
        app_root=tmp_path / "app",
        chart_root_override=override,
    )

    assert root == override


def test_monitor_files_do_not_hard_code_patrick_home_paths() -> None:
    repo_root = MODULE_PATH.parents[1]
    checked = [
        repo_root / "regime_monitor_ubuntu" / "regime_monitor.py",
        repo_root / "regime_monitor_ubuntu" / "config.json.example",
        repo_root / "regime_monitor_ubuntu" / "README.md",
    ]
    forbidden = ("/Users/" + "patrick", "/home/" + "patrick")

    for path in checked:
        text = path.read_text(encoding="utf-8")
        for value in forbidden:
            assert value not in text


def _write_candles(root: Path, symbol: str, timeframe: str, bars: list[dict[str, object]]) -> None:
    path = root / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "phase1_runtime_market_data_test",
                "generated_at": "2026-07-19T12:00:00+00:00",
                "symbol": symbol,
                "timeframe": timeframe,
                "bars": bars,
                "bar_count": len(bars),
            }
        ),
        encoding="utf-8",
    )


def _bar(index: int) -> dict[str, object]:
    end = datetime(2026, 7, 19, tzinfo=timezone.utc) + timedelta(minutes=(index + 1) * 5)
    start = end - timedelta(minutes=5)
    return {
        "bar_start": start.isoformat(),
        "bar_end": end.isoformat(),
        "open": 100 + index,
        "high": 101 + index,
        "low": 99 + index,
        "close": 100.5 + index,
        "volume": index,
        "completed": True,
        "source_bar_count": 5,
    }


def _one_minute_bar(bar_end: str, open_: float, high: float, low: float, close: float) -> dict[str, object]:
    return {
        "bar_start": "2026-07-19T00:10:00+00:00",
        "bar_end": bar_end,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1,
        "completed": True,
        "source_bar_count": 1,
    }
