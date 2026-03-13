"""Integration tests for the strategy engine orchestration shell."""

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import LongEntryFamily, PositionSide, ReplayFillPolicy, StrategyStatus, VwapPolicy
from mgc_v05l.domain.events import ExitEvaluatedEvent, FillReceivedEvent, OrderIntentCreatedEvent
from mgc_v05l.domain.models import Bar
from mgc_v05l.app.container import build_application_container
from mgc_v05l.app.runner import StrategyServiceRunner
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.replay_feed import ReplayFeed
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.state_repository import StateRepository
from mgc_v05l.strategy.strategy_engine import StrategyEngine
from mgc_v05l.strategy.trade_state import build_initial_state


def _build_settings() -> StrategySettings:
    return StrategySettings(
        symbol="MGC",
        timeframe="5m",
        timezone="America/New_York",
        mode=RuntimeMode.REPLAY,
        database_url="sqlite:///./mgc_v05l.sqlite3",
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
        show_debug_labels=True,
    )


def _build_replay_settings(database_path: Path) -> StrategySettings:
    base = _build_settings().model_copy(
        update={
            "database_url": f"sqlite:///{database_path}",
            "enable_bear_snap_shorts": False,
            "enable_asia_vwap_longs": False,
            "atr_len": 2,
            "max_bars_long": 2,
            "max_bars_short": 2,
            "anti_churn_bars": 1,
            "turn_fast_len": 1,
            "turn_slow_len": 3,
            "turn_stretch_lookback": 2,
            "min_snap_down_stretch_atr": Decimal("0.10"),
            "min_snap_bar_range_atr": Decimal("0.10"),
            "min_snap_body_atr": Decimal("0.10"),
            "min_snap_close_location": Decimal("0.50"),
            "min_snap_velocity_delta_atr": Decimal("0.00"),
            "snap_cooldown_bars": 1,
            "use_asia_bull_snap_thresholds": False,
            "asia_min_snap_bar_range_atr": Decimal("0.10"),
            "asia_min_snap_body_atr": Decimal("0.10"),
            "asia_min_snap_velocity_delta_atr": Decimal("0.00"),
            "use_bull_snap_location_filter": False,
            "bull_snap_max_close_vs_slow_ema_atr": Decimal("10.0"),
            "bull_snap_require_close_below_slow_ema": False,
            "use_bear_snap_location_filter": False,
            "bear_snap_min_close_vs_slow_ema_atr": Decimal("0.0"),
            "bear_snap_require_close_above_slow_ema": False,
            "below_vwap_lookback": 1,
            "require_green_reclaim_bar": False,
            "reclaim_close_buffer_atr": Decimal("0.0"),
            "min_vwap_bar_range_atr": Decimal("0.10"),
            "require_hold_close_above_vwap": False,
            "require_hold_not_break_reclaim_low": False,
            "require_acceptance_close_above_reclaim_high": False,
            "require_acceptance_close_above_vwap": False,
            "vwap_long_max_bars": 2,
            "vwap_weak_close_lookback_bars": 1,
            "vol_len": 1,
            "show_debug_labels": False,
        }
    )
    return base


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


def test_strategy_engine_creates_exit_intent_for_in_position_time_exit() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    initial_state = replace(
        build_initial_state(datetime.now(ZoneInfo("UTC"))),
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=datetime.now(ZoneInfo("UTC")),
        entry_bar_id="entry-bar",
        long_entry_family=LongEntryFamily.K,
        bars_in_trade=5,
    )
    engine = StrategyEngine(settings=settings, initial_state=initial_state)
    bar = _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "100", "101", "99.5", "100")

    events = engine.process_bar(bar)

    assert any(isinstance(event, ExitEvaluatedEvent) for event in events)
    assert any(isinstance(event, OrderIntentCreatedEvent) for event in events)


def test_strategy_replay_path_runs_bars_to_intents_fills_and_exit(tmp_path: Path) -> None:
    settings = _build_replay_settings(tmp_path / "replay-path.sqlite3")
    repositories = RepositorySet(build_engine(settings.database_url))
    engine = StrategyEngine(settings=settings, repositories=repositories)
    csv_path = tmp_path / "replay.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T17:20:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:25:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:30:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:35:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:40:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:45:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:50:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:55:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:00:00-04:00,99,100,97,98,100\n"
        "2026-03-13T18:05:00-04:00,95,100,94,99,100\n"
        "2026-03-13T18:10:00-04:00,100,100.5,99,100.4,100\n"
        "2026-03-13T18:15:00-04:00,100.2,100.6,99.8,100.1,100\n",
        encoding="utf-8",
    )
    replay_feed = ReplayFeed(settings)

    all_events = []
    for bar in replay_feed.iter_csv(csv_path):
        all_events.extend(engine.process_bar(bar))

    persisted_state = StateRepository(repositories.engine).load_latest()
    persisted_intents = repositories.order_intents.list_all()
    persisted_fills = repositories.fills.list_all()

    assert any(isinstance(event, OrderIntentCreatedEvent) for event in all_events)
    assert any(isinstance(event, FillReceivedEvent) for event in all_events)
    assert repositories.processed_bars.count() == 12
    assert len(persisted_intents) == 2
    assert len(persisted_fills) == 2
    assert persisted_state is not None
    assert persisted_state.position_side is PositionSide.FLAT
    assert persisted_state.internal_position_qty == 0


def test_replay_runner_returns_summary(tmp_path: Path) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "runner.sqlite3"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n'
        "enable_bear_snap_shorts: false\n"
        "enable_asia_vwap_longs: false\n"
        "atr_len: 2\n"
        "max_bars_long: 2\n"
        "max_bars_short: 2\n"
        "anti_churn_bars: 1\n"
        "turn_fast_len: 1\n"
        "turn_slow_len: 3\n"
        "turn_stretch_lookback: 2\n"
        "min_snap_down_stretch_atr: 0.10\n"
        "min_snap_bar_range_atr: 0.10\n"
        "min_snap_body_atr: 0.10\n"
        "min_snap_close_location: 0.50\n"
        "min_snap_velocity_delta_atr: 0.00\n"
        "snap_cooldown_bars: 1\n"
        "use_asia_bull_snap_thresholds: false\n"
        "asia_min_snap_bar_range_atr: 0.10\n"
        "asia_min_snap_body_atr: 0.10\n"
        "asia_min_snap_velocity_delta_atr: 0.00\n"
        "use_bull_snap_location_filter: false\n"
        "bull_snap_max_close_vs_slow_ema_atr: 10.0\n"
        "bull_snap_require_close_below_slow_ema: false\n"
        "use_bear_snap_location_filter: false\n"
        "bear_snap_min_close_vs_slow_ema_atr: 0.0\n"
        "bear_snap_require_close_above_slow_ema: false\n"
        "below_vwap_lookback: 1\n"
        "require_green_reclaim_bar: false\n"
        "reclaim_close_buffer_atr: 0.0\n"
        "min_vwap_bar_range_atr: 0.10\n"
        "require_hold_close_above_vwap: false\n"
        "require_hold_not_break_reclaim_low: false\n"
        "require_acceptance_close_above_reclaim_high: false\n"
        "require_acceptance_close_above_vwap: false\n"
        "vwap_long_max_bars: 2\n"
        "vwap_weak_close_lookback_bars: 1\n"
        "vol_len: 1\n"
        "show_debug_labels: false\n",
        encoding="utf-8",
    )
    csv_path = tmp_path / "runner.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T17:20:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:25:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:30:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:35:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:40:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:45:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:50:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:55:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:00:00-04:00,99,100,97,98,100\n"
        "2026-03-13T18:05:00-04:00,95,100,94,99,100\n"
        "2026-03-13T18:10:00-04:00,100,100.5,99,100.4,100\n"
        "2026-03-13T18:15:00-04:00,100.2,100.6,99.8,100.1,100\n",
        encoding="utf-8",
    )

    container = build_application_container([base_config, replay_config])
    summary = StrategyServiceRunner(container).run_replay(csv_path)

    assert summary.processed_bars == 12
    assert summary.long_entries == 1
    assert summary.short_entries == 0
    assert summary.exits == 1
    assert summary.fills == 2
    assert summary.final_position_side is PositionSide.FLAT
