"""Replay, persistence, and paper-execution tests."""

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, ReplayFillPolicy, VwapPolicy
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.order_models import FillEvent, OrderIntent
from mgc_v05l.execution.paper_broker import PaperBroker
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.bar_store import BarStore
from mgc_v05l.market_data.replay_feed import ReplayFeed
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet


def _build_settings(database_path: Path, **overrides) -> StrategySettings:
    settings = StrategySettings(
        symbol="MGC",
        timeframe="5m",
        timezone="America/New_York",
        mode=RuntimeMode.REPLAY,
        database_url=f"sqlite:///{database_path}",
        replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
        vwap_policy=VwapPolicy.SESSION_RESET,
        trade_size=1,
        enable_bull_snap_longs=True,
        enable_bear_snap_shorts=True,
        enable_asia_vwap_longs=True,
        atr_len=14,
        stop_atr_mult=Decimal("0.10"),
        breakeven_at_r=Decimal("1.0"),
        max_bars_long=6,
        max_bars_short=4,
        allow_asia=True,
        allow_london=True,
        allow_us=True,
        asia_start=datetime.strptime("18:00", "%H:%M").time(),
        asia_end=datetime.strptime("23:00", "%H:%M").time(),
        london_start=datetime.strptime("03:00", "%H:%M").time(),
        london_end=datetime.strptime("08:30", "%H:%M").time(),
        us_start=datetime.strptime("08:30", "%H:%M").time(),
        us_end=datetime.strptime("17:00", "%H:%M").time(),
        anti_churn_bars=3,
        use_turn_family=True,
        turn_fast_len=3,
        turn_slow_len=6,
        turn_signal_len=2,
        turn_stretch_lookback=8,
        min_snap_down_stretch_atr=Decimal("1.20"),
        min_snap_bar_range_atr=Decimal("1.00"),
        min_snap_body_atr=Decimal("0.45"),
        min_snap_close_location=Decimal("0.72"),
        min_snap_velocity_delta_atr=Decimal("0.18"),
        snap_cooldown_bars=5,
        use_asia_bull_snap_thresholds=True,
        asia_min_snap_bar_range_atr=Decimal("0.80"),
        asia_min_snap_body_atr=Decimal("0.35"),
        asia_min_snap_velocity_delta_atr=Decimal("0.12"),
        use_bull_snap_location_filter=True,
        bull_snap_max_close_vs_slow_ema_atr=Decimal("0.15"),
        bull_snap_require_close_below_slow_ema=True,
        min_bear_snap_up_stretch_atr=Decimal("1.00"),
        min_bear_snap_bar_range_atr=Decimal("0.90"),
        min_bear_snap_body_atr=Decimal("0.40"),
        max_bear_snap_close_location=Decimal("0.28"),
        min_bear_snap_velocity_delta_atr=Decimal("0.16"),
        bear_snap_cooldown_bars=5,
        use_bear_snap_location_filter=True,
        bear_snap_min_close_vs_slow_ema_atr=Decimal("0.15"),
        bear_snap_require_close_above_slow_ema=True,
        below_vwap_lookback=5,
        require_green_reclaim_bar=True,
        reclaim_close_buffer_atr=Decimal("0.03"),
        min_vwap_bar_range_atr=Decimal("0.45"),
        use_vwap_volume_filter=False,
        min_vwap_vol_ratio=Decimal("1.00"),
        require_hold_close_above_vwap=True,
        require_hold_not_break_reclaim_low=True,
        require_acceptance_close_above_reclaim_high=True,
        require_acceptance_close_above_vwap=True,
        vwap_long_stop_atr_mult=Decimal("0.05"),
        vwap_long_breakeven_at_r=Decimal("0.50"),
        vwap_long_max_bars=4,
        use_vwap_hard_loss_exit=True,
        vwap_weak_close_lookback_bars=2,
        vol_len=20,
        show_debug_labels=False,
    )
    return settings.model_copy(update=overrides)


def _build_bar(end_ts: datetime, open_price: str, high_price: str, low_price: str, close_price: str) -> Bar:
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id("MGC", "5m", end_ts),
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


def test_replay_csv_ingestion_loads_locked_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "replay.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T18:00:00-04:00,100,101,99,100.5,120\n"
        "2026-03-13T18:05:00-04:00,100.5,101.5,100,101,125\n",
        encoding="utf-8",
    )
    settings = _build_settings(tmp_path / "replay.sqlite3")

    feed = ReplayFeed(settings)
    bars = feed.load_csv(csv_path)

    assert len(bars) == 2
    assert bars[0].end_ts.tzinfo is not None
    assert bars[0].bar_id.startswith("MGC|5m|")
    assert bars[1].open == Decimal("100.5")


def test_duplicate_bar_suppression_and_processed_bar_persistence(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite:///{tmp_path / 'processed.sqlite3'}")
    repositories = RepositorySet(engine)
    store = BarStore(repositories.processed_bars)
    ny = ZoneInfo("America/New_York")
    bar = _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100.5")

    assert store.validate_next_bar(bar) is True
    store.mark_processed(bar)
    assert repositories.processed_bars.count() == 1

    restored_store = BarStore(repositories.processed_bars)

    assert restored_store.has_processed(bar.bar_id) is True
    assert restored_store.validate_next_bar(bar) is False


def test_order_intent_and_fill_persistence(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite:///{tmp_path / 'orders.sqlite3'}")
    repositories = RepositorySet(engine)
    now = datetime.now(ZoneInfo("UTC"))
    intent = OrderIntent(
        order_intent_id="intent-1",
        bar_id="bar-1",
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=now,
        reason_code="firstBullSnapTurn",
    )
    fill = FillEvent(
        order_intent_id="intent-1",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=now,
        fill_price=Decimal("100.25"),
        broker_order_id="paper-intent-1",
    )

    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-intent-1")
    repositories.fills.save(fill)

    persisted_intents = repositories.order_intents.list_all()
    persisted_fills = repositories.fills.list_all()

    assert persisted_intents[0]["order_status"] == OrderStatus.ACKNOWLEDGED.value
    assert persisted_intents[0]["broker_order_id"] == "paper-intent-1"
    assert persisted_fills[0]["fill_price"] == "100.25"


def test_next_bar_open_replay_fill_behavior_is_explicit(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path / "replay-fill.sqlite3")
    broker = PaperBroker()
    execution_engine = ExecutionEngine(broker=broker)
    ny = ZoneInfo("America/New_York")
    signal_bar = _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "99", "100", "94", "99")
    next_bar = _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "100", "101", "99", "100.4")
    intent = OrderIntent(
        order_intent_id=f"{signal_bar.bar_id}|BUY_TO_OPEN",
        bar_id=signal_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=signal_bar.end_ts,
        reason_code="firstBullSnapTurn",
    )

    pending = execution_engine.submit_intent(
        intent,
        signal_bar_id=signal_bar.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    assert pending is not None
    assert execution_engine.pop_due_replay_fills(signal_bar, settings) == []

    due = execution_engine.pop_due_replay_fills(next_bar, settings)
    fill = execution_engine.materialize_replay_fill(due[0], next_bar)

    assert fill.fill_price == next_bar.open
    assert fill.fill_timestamp == next_bar.start_ts
    assert broker.get_position().quantity == 1


def test_paper_broker_fills_are_deterministic(tmp_path: Path) -> None:
    broker = PaperBroker()
    broker.connect()
    now = datetime.now(ZoneInfo("UTC"))
    entry_intent = OrderIntent(
        order_intent_id="intent-1",
        bar_id="bar-1",
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=now,
        reason_code="entry",
    )
    exit_intent = OrderIntent(
        order_intent_id="intent-2",
        bar_id="bar-2",
        symbol="MGC",
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        quantity=1,
        created_at=now,
        reason_code="exit",
    )

    entry_broker_order_id = broker.submit_order(entry_intent)
    entry_fill = broker.fill_order(entry_intent, Decimal("100.00"), now)
    exit_broker_order_id = broker.submit_order(exit_intent)
    exit_fill = broker.fill_order(exit_intent, Decimal("101.00"), now + timedelta(minutes=5))

    assert entry_fill.broker_order_id == entry_broker_order_id
    assert exit_fill.broker_order_id == exit_broker_order_id
    assert broker.get_order_status(entry_broker_order_id)["status"] == OrderStatus.FILLED.value
    assert broker.get_position().quantity == 0
