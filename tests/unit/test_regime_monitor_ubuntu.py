from __future__ import annotations

import importlib.util
import sys
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
