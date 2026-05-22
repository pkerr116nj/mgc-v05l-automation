from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import mgc_v05l.app.probationary_runtime as runtime
from mgc_v05l.app.probationary_runtime import (
    PAPER_EXECUTION_TEST_MULE_ENTRY_REASON,
    PAPER_EXECUTION_TEST_MULE_LANE_IDS,
    PAPER_EXECUTION_TEST_MULE_MAX_LOSS_EXIT_REASON,
    PAPER_EXECUTION_TEST_MULE_MODE,
    PAPER_EXECUTION_TEST_MULE_PROFIT_EXIT_REASON,
    PAPER_EXECUTION_TEST_MULE_RUNTIME_KIND,
    PAPER_EXECUTION_TEST_MULE_SIGNAL_SOURCE,
    PAPER_EXECUTION_TEST_MULE_TIMED_EXIT_REASON,
    ProbationaryPaperLaneRuntime,
    _load_probationary_paper_lane_specs,
)
from mgc_v05l.config_models import RuntimeMode, load_settings_from_files
from mgc_v05l.domain.enums import PositionSide
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter


REPO_ROOT = Path(__file__).resolve().parents[2]


def _bar(
    end_ts: datetime,
    *,
    symbol: str = "MGC",
    open_price: str = "100.0",
    close_price: str = "100.2",
    high: str = "100.3",
    low: str = "99.9",
) -> Bar:
    return Bar(
        bar_id=f"{symbol}|1m|{end_ts.astimezone(timezone.utc).isoformat()}",
        symbol=symbol,
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close_price),
        volume=100,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _state(
    *,
    side: PositionSide = PositionSide.FLAT,
    qty: int = 0,
    open_order: str | None = None,
    entry_ts: datetime | None = None,
    entry_price: Decimal | None = None,
    entries_enabled: bool = True,
    exits_enabled: bool = True,
    operator_halt: bool = False,
    same_underlying_entry_hold: bool = False,
    fault_code: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        position_side=side,
        internal_position_qty=qty,
        open_broker_order_id=open_order,
        entry_timestamp=entry_ts,
        entry_price=entry_price,
        entries_enabled=entries_enabled,
        exits_enabled=exits_enabled,
        operator_halt=operator_halt,
        same_underlying_entry_hold=same_underlying_entry_hold,
        fault_code=fault_code,
    )


class _FakeOrderIntents:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self._rows = list(rows or [])

    def list_all(self) -> list[dict[str, object]]:
        return list(self._rows)


class _FakeStrategyEngine:
    def __init__(self, state: SimpleNamespace) -> None:
        self.state = state
        self.entries: list[dict[str, object]] = []
        self.exits: list[dict[str, object]] = []
        self.operator_flatten_calls: list[dict[str, object]] = []

    def submit_runtime_entry_intent(self, bar: Bar, **kwargs):
        self.entries.append({"bar": bar, **kwargs})
        return SimpleNamespace(order_intent_id=f"{bar.bar_id}|entry")

    def submit_runtime_exit_intent(self, occurred_at: datetime, **kwargs):
        self.exits.append({"occurred_at": occurred_at, **kwargs})
        return SimpleNamespace(order_intent_id=f"runtime-exit|{int(occurred_at.timestamp())}")

    def submit_operator_flatten_intent(self, occurred_at: datetime, *, reason_code: str):
        self.operator_flatten_calls.append({"occurred_at": occurred_at, "reason_code": reason_code})
        return SimpleNamespace(order_intent_id=f"runtime-exit|{int(occurred_at.timestamp())}")


def _mule_lane(
    *,
    symbol: str = "MGC",
    state: SimpleNamespace | None = None,
    order_rows: list[dict[str, object]] | None = None,
) -> ProbationaryPaperLaneRuntime:
    lane = object.__new__(ProbationaryPaperLaneRuntime)
    lane.spec = SimpleNamespace(
        lane_id=PAPER_EXECUTION_TEST_MULE_LANE_IDS[symbol],
        symbol=symbol,
        lane_mode=PAPER_EXECUTION_TEST_MULE_MODE,
        runtime_kind=PAPER_EXECUTION_TEST_MULE_RUNTIME_KIND,
        session_restriction="ASIA/LONDON/US",
        paper_only=True,
        runtime_overlay_params={
            "test_mule_enabled": True,
            "live_money_eligible": False,
            "tick_size": "0.25" if symbol == "MNQ" else "0.1",
            "min_body_ticks": 1,
            "frequency_cap_minutes": 20,
            "hold_minutes": 20,
            "profit_target_ticks": 8,
            "max_loss_ticks": 12,
        },
    )
    lane.settings = SimpleNamespace(mode=RuntimeMode.PAPER)
    lane.strategy_engine = _FakeStrategyEngine(state or _state())
    lane.repositories = SimpleNamespace(order_intents=_FakeOrderIntents(order_rows))
    lane._startup_route_enable_after_ts = datetime(2026, 5, 20, 0, 0, tzinfo=timezone.utc)
    return lane


def _empty_reconciliation(_path: Path) -> dict[str, object]:
    return {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "track_b_broker_positions": [],
        "track_b_lifecycle_positions": [],
    }


def _settings_with_mule_enabled(tmp_path: Path):
    override = tmp_path / "mule_enabled.yaml"
    override.write_text("probationary_paper_execution_test_mule_enabled: true\n", encoding="utf-8")
    return load_settings_from_files(
        [
            REPO_ROOT / "config/base.yaml",
            REPO_ROOT / "config/live.yaml",
            REPO_ROOT / "config/probationary_pattern_engine.yaml",
            REPO_ROOT / "config/probationary_pattern_engine_paper.yaml",
            override,
        ]
    )


def test_mule_lanes_are_opt_in_and_bridge_submit_capable(tmp_path: Path) -> None:
    default_settings = load_settings_from_files(
        [
            REPO_ROOT / "config/base.yaml",
            REPO_ROOT / "config/live.yaml",
            REPO_ROOT / "config/probationary_pattern_engine.yaml",
            REPO_ROOT / "config/probationary_pattern_engine_paper.yaml",
        ]
    )
    assert all(
        spec.lane_id not in set(PAPER_EXECUTION_TEST_MULE_LANE_IDS.values())
        for spec in _load_probationary_paper_lane_specs(default_settings)
    )

    enabled = _load_probationary_paper_lane_specs(_settings_with_mule_enabled(tmp_path))
    lanes = {spec.lane_id: spec for spec in enabled if spec.lane_id in set(PAPER_EXECUTION_TEST_MULE_LANE_IDS.values())}
    assert set(lanes) == set(PAPER_EXECUTION_TEST_MULE_LANE_IDS.values())
    for symbol, lane_id in PAPER_EXECUTION_TEST_MULE_LANE_IDS.items():
        spec = lanes[lane_id]
        assert spec.symbol == symbol
        assert spec.trade_size == 1
        assert spec.max_position_quantity == 1
        assert spec.paper_only is True
        assert spec.non_approved is True
        assert spec.exclude_from_strategy_performance is True
        assert spec.runtime_overlay_params["live_money_eligible"] is False
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)
        assert adapter is not None
        assert adapter["source_instrument"] == symbol
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"


def test_mule_creates_order_intent_when_flat_and_eligible(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_read_json", _empty_reconciliation)
    lane = _mule_lane(state=_state())
    bar = _bar(datetime(2026, 5, 20, 20, 1, tzinfo=ZoneInfo("America/New_York")))

    lane._apply_execution_test_mule_lifecycle(bar)

    assert len(lane.strategy_engine.entries) == 1
    entry = lane.strategy_engine.entries[0]
    assert entry["side"] == "LONG"
    assert entry["signal_source"] == PAPER_EXECUTION_TEST_MULE_SIGNAL_SOURCE
    assert entry["reason_code"] == PAPER_EXECUTION_TEST_MULE_ENTRY_REASON
    assert entry["symbol"] == "MGC"


def test_mule_does_not_enter_when_already_holding() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(
        state=_state(side=PositionSide.LONG, qty=1, entry_ts=entry_ts, entry_price=Decimal("100"))
    )
    bar = _bar(entry_ts + timedelta(minutes=5), high="100.2", low="99.9")

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.entries == []
    assert lane.strategy_engine.exits == []


def test_mule_respects_frequency_cap(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_read_json", _empty_reconciliation)
    now = datetime(2026, 5, 20, 20, 10, tzinfo=ZoneInfo("America/New_York"))
    lane = _mule_lane(
        order_rows=[
            {
                "reason_code": PAPER_EXECUTION_TEST_MULE_ENTRY_REASON,
                "intent_type": "BUY_TO_OPEN",
                "created_at": (now - timedelta(minutes=5)).astimezone(timezone.utc).isoformat(),
            }
        ]
    )

    lane._apply_execution_test_mule_lifecycle(_bar(now))

    assert lane.strategy_engine.entries == []


def test_mule_blocks_existing_track_b_symbol_exposure(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "_read_json",
        lambda _path: {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "track_b_broker_positions": [{"symbol": "MGC", "quantity": "1"}],
            "track_b_lifecycle_positions": [],
        },
    )
    lane = _mule_lane(state=_state())
    lane._apply_execution_test_mule_lifecycle(_bar(datetime(2026, 5, 20, 20, 1, tzinfo=ZoneInfo("America/New_York"))))
    assert lane.strategy_engine.entries == []


def test_mule_exits_by_timed_hold() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(state=_state(side=PositionSide.LONG, qty=1, entry_ts=entry_ts, entry_price=Decimal("100")))
    bar = _bar(entry_ts + timedelta(minutes=21), high="100.2", low="99.9")

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.operator_flatten_calls == []
    assert lane.strategy_engine.exits == [
        {
            "occurred_at": bar.end_ts,
            "quantity": 1,
            "reason_code": PAPER_EXECUTION_TEST_MULE_TIMED_EXIT_REASON,
            "symbol": "MGC",
        }
    ]


def test_mule_exits_by_profit_target() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(state=_state(side=PositionSide.LONG, qty=1, entry_ts=entry_ts, entry_price=Decimal("100")))
    bar = _bar(entry_ts + timedelta(minutes=5), high="100.8", low="99.9")

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.operator_flatten_calls == []
    assert lane.strategy_engine.exits == [
        {
            "occurred_at": bar.end_ts,
            "quantity": 1,
            "reason_code": PAPER_EXECUTION_TEST_MULE_PROFIT_EXIT_REASON,
            "symbol": "MGC",
        }
    ]


def test_mule_exits_by_max_loss() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(state=_state(side=PositionSide.LONG, qty=1, entry_ts=entry_ts, entry_price=Decimal("100")))
    bar = _bar(entry_ts + timedelta(minutes=5), close_price="99.0", high="100.1", low="98.8")

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.operator_flatten_calls == []
    assert lane.strategy_engine.exits == [
        {
            "occurred_at": bar.end_ts,
            "quantity": 1,
            "reason_code": PAPER_EXECUTION_TEST_MULE_MAX_LOSS_EXIT_REASON,
            "symbol": "MGC",
        }
    ]


def test_mule_time_boxed_exit_is_strategy_managed_not_operator_flatten() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(state=_state(side=PositionSide.SHORT, qty=1, entry_ts=entry_ts, entry_price=Decimal("100")))
    bar = _bar(entry_ts + timedelta(minutes=21), symbol="MGC", high="100.2", low="99.9")

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.operator_flatten_calls == []
    assert lane.strategy_engine.exits == [
        {
            "occurred_at": bar.end_ts,
            "quantity": 1,
            "reason_code": PAPER_EXECUTION_TEST_MULE_TIMED_EXIT_REASON,
            "symbol": "MGC",
        }
    ]


def test_mnq_mule_time_boxed_exit_is_strategy_managed_not_operator_flatten() -> None:
    entry_ts = datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc)
    lane = _mule_lane(
        symbol="MNQ",
        state=_state(side=PositionSide.LONG, qty=1, entry_ts=entry_ts, entry_price=Decimal("29100")),
    )
    bar = _bar(
        entry_ts + timedelta(minutes=21),
        symbol="MNQ",
        open_price="29100",
        close_price="29101",
        high="29101",
        low="29099",
    )

    lane._apply_execution_test_mule_lifecycle(bar)

    assert lane.strategy_engine.operator_flatten_calls == []
    assert lane.strategy_engine.exits == [
        {
            "occurred_at": bar.end_ts,
            "quantity": 1,
            "reason_code": PAPER_EXECUTION_TEST_MULE_TIMED_EXIT_REASON,
            "symbol": "MNQ",
        }
    ]


def test_mule_blocks_stale_startup_hold_and_live_money(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_read_json", _empty_reconciliation)
    bar = _bar(datetime(2026, 5, 20, 20, 1, tzinfo=ZoneInfo("America/New_York")))
    startup_blocked = _mule_lane()
    startup_blocked._startup_route_enable_after_ts = None
    startup_blocked._apply_execution_test_mule_lifecycle(bar)
    assert startup_blocked.strategy_engine.entries == []

    live_money_blocked = _mule_lane()
    live_money_blocked.spec.runtime_overlay_params["live_money_eligible"] = True
    live_money_blocked._apply_execution_test_mule_lifecycle(bar)
    assert live_money_blocked.strategy_engine.entries == []


def test_mule_wrong_session_and_tiny_body_do_not_route(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "_read_json", _empty_reconciliation)
    off_session = _mule_lane()
    off_session._apply_execution_test_mule_lifecycle(_bar(datetime(2026, 5, 20, 17, 1, tzinfo=ZoneInfo("America/New_York"))))
    assert off_session.strategy_engine.entries == []

    tiny_body = _mule_lane()
    tiny_body._apply_execution_test_mule_lifecycle(
        _bar(datetime(2026, 5, 20, 20, 1, tzinfo=ZoneInfo("America/New_York")), open_price="100.00", close_price="100.05")
    )
    assert tiny_body.strategy_engine.entries == []
