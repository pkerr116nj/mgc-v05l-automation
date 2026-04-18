"""Tests for typed config loading."""

import json
from pathlib import Path

from mgc_v05l.config_models import DataStoragePolicy, load_data_storage_policy, load_settings_from_files
from mgc_v05l.config_models.settings import RuntimeMode


def test_config_loader_merges_base_and_overlay_files(tmp_path: Path) -> None:
    base_path = tmp_path / "base.yaml"
    overlay_path = tmp_path / "replay.yaml"
    base_path.write_text(
        'symbol: "MGC"\n'
        'timeframe: "5m"\n'
        'timezone: "America/New_York"\n'
        'mode: "paper"\n'
        'database_url: "sqlite:///./base.sqlite3"\n'
        'replay_fill_policy: "NEXT_BAR_OPEN"\n'
        'vwap_policy: "SESSION_RESET"\n'
        "trade_size: 1\n"
        "enable_bull_snap_longs: true\n"
        "enable_bear_snap_shorts: true\n"
        "enable_asia_vwap_longs: true\n"
        "atr_len: 14\n"
        "stop_atr_mult: 0.10\n"
        "breakeven_at_r: 1.0\n"
        "max_bars_long: 6\n"
        "max_bars_short: 4\n"
        "allow_asia: true\n"
        "allow_london: true\n"
        "allow_us: true\n"
        'asia_start: "18:00:00"\n'
        'asia_end: "23:00:00"\n'
        'london_start: "03:00:00"\n'
        'london_end: "08:30:00"\n'
        'us_start: "08:30:00"\n'
        'us_end: "17:00:00"\n'
        "anti_churn_bars: 3\n"
        "use_turn_family: true\n"
        "turn_fast_len: 3\n"
        "turn_slow_len: 6\n"
        "turn_signal_len: 2\n"
        "turn_stretch_lookback: 8\n"
        "min_snap_down_stretch_atr: 1.20\n"
        "min_snap_bar_range_atr: 1.00\n"
        "min_snap_body_atr: 0.45\n"
        "min_snap_close_location: 0.72\n"
        "min_snap_velocity_delta_atr: 0.18\n"
        "snap_cooldown_bars: 5\n"
        "use_asia_bull_snap_thresholds: true\n"
        "asia_min_snap_bar_range_atr: 0.80\n"
        "asia_min_snap_body_atr: 0.35\n"
        "asia_min_snap_velocity_delta_atr: 0.12\n"
        "use_bull_snap_location_filter: true\n"
        "bull_snap_max_close_vs_slow_ema_atr: 0.15\n"
        "bull_snap_require_close_below_slow_ema: true\n"
        "min_bear_snap_up_stretch_atr: 1.00\n"
        "min_bear_snap_bar_range_atr: 0.90\n"
        "min_bear_snap_body_atr: 0.40\n"
        "max_bear_snap_close_location: 0.28\n"
        "min_bear_snap_velocity_delta_atr: 0.16\n"
        "bear_snap_cooldown_bars: 5\n"
        "use_bear_snap_location_filter: true\n"
        "bear_snap_min_close_vs_slow_ema_atr: 0.15\n"
        "bear_snap_require_close_above_slow_ema: true\n"
        "below_vwap_lookback: 5\n"
        "require_green_reclaim_bar: true\n"
        "reclaim_close_buffer_atr: 0.03\n"
        "min_vwap_bar_range_atr: 0.45\n"
        "use_vwap_volume_filter: false\n"
        "min_vwap_vol_ratio: 1.00\n"
        "require_hold_close_above_vwap: true\n"
        "require_hold_not_break_reclaim_low: true\n"
        "require_acceptance_close_above_reclaim_high: true\n"
        "require_acceptance_close_above_vwap: true\n"
        "vwap_long_stop_atr_mult: 0.05\n"
        "vwap_long_breakeven_at_r: 0.50\n"
        "vwap_long_max_bars: 4\n"
        "use_vwap_hard_loss_exit: true\n"
        "vwap_weak_close_lookback_bars: 2\n"
        "vol_len: 20\n"
        "show_debug_labels: false\n",
        encoding="utf-8",
    )
    overlay_path.write_text(
        'mode: "replay"\n'
        'database_url: "sqlite:///./replay.sqlite3"\n',
        encoding="utf-8",
    )

    settings = load_settings_from_files([base_path, overlay_path])

    assert settings.mode is RuntimeMode.REPLAY
    assert settings.database_url == "sqlite:///./replay.sqlite3"
    assert settings.symbol == "MGC"


def test_data_storage_policy_loads_default_repo_policy() -> None:
    repo_root = Path(__file__).resolve().parents[2]

    policy = load_data_storage_policy(repo_root)

    assert isinstance(policy, DataStoragePolicy)
    assert policy.config_path == repo_root / "config" / "data_storage_policy.json"
    assert policy.broker_monitor_database_path == repo_root / "outputs" / "production_link" / "schwab_production_link.sqlite3"


def test_config_loader_concatenates_probationary_paper_lane_payloads(tmp_path: Path) -> None:
    base_path = tmp_path / "base.yaml"
    overlay_path = tmp_path / "overlay.yaml"
    base_path.write_text(
        "\n".join(
            [
                'symbol: "MGC"',
                'timeframe: "5m"',
                'timezone: "America/New_York"',
                'mode: "paper"',
                'database_url: "sqlite:///./base.sqlite3"',
                'replay_fill_policy: "NEXT_BAR_OPEN"',
                'vwap_policy: "SESSION_RESET"',
                "trade_size: 1",
                "enable_bull_snap_longs: true",
                "enable_bear_snap_shorts: true",
                "enable_asia_vwap_longs: true",
                "atr_len: 14",
                "stop_atr_mult: 0.10",
                "breakeven_at_r: 1.0",
                "max_bars_long: 6",
                "max_bars_short: 4",
                "allow_asia: true",
                "allow_london: true",
                "allow_us: true",
                'asia_start: "18:00:00"',
                'asia_end: "23:00:00"',
                'london_start: "03:00:00"',
                'london_end: "08:30:00"',
                'us_start: "08:30:00"',
                'us_end: "17:00:00"',
                "anti_churn_bars: 3",
                "use_turn_family: true",
                "turn_fast_len: 3",
                "turn_slow_len: 6",
                "turn_signal_len: 2",
                "turn_stretch_lookback: 8",
                "min_snap_down_stretch_atr: 1.20",
                "min_snap_bar_range_atr: 1.00",
                "min_snap_body_atr: 0.45",
                "min_snap_close_location: 0.72",
                "min_snap_velocity_delta_atr: 0.18",
                "snap_cooldown_bars: 5",
                "use_asia_bull_snap_thresholds: true",
                "asia_min_snap_bar_range_atr: 0.80",
                "asia_min_snap_body_atr: 0.35",
                "asia_min_snap_velocity_delta_atr: 0.12",
                "use_bull_snap_location_filter: true",
                "bull_snap_max_close_vs_slow_ema_atr: 0.15",
                "bull_snap_require_close_below_slow_ema: true",
                "min_bear_snap_up_stretch_atr: 1.00",
                "min_bear_snap_bar_range_atr: 0.90",
                "min_bear_snap_body_atr: 0.40",
                "max_bear_snap_close_location: 0.28",
                "min_bear_snap_velocity_delta_atr: 0.16",
                "bear_snap_cooldown_bars: 5",
                "use_bear_snap_location_filter: true",
                "bear_snap_min_close_vs_slow_ema_atr: 0.15",
                "bear_snap_require_close_above_slow_ema: true",
                "below_vwap_lookback: 5",
                "require_green_reclaim_bar: true",
                "reclaim_close_buffer_atr: 0.03",
                "min_vwap_bar_range_atr: 0.45",
                "use_vwap_volume_filter: false",
                "min_vwap_vol_ratio: 1.00",
                "require_hold_close_above_vwap: true",
                "require_hold_not_break_reclaim_low: true",
                "require_acceptance_close_above_reclaim_high: true",
                "require_acceptance_close_above_vwap: true",
                "vwap_long_stop_atr_mult: 0.05",
                "vwap_long_breakeven_at_r: 0.50",
                "vwap_long_max_bars: 4",
                "use_vwap_hard_loss_exit: true",
                "vwap_weak_close_lookback_bars: 2",
                "vol_len: 20",
                "show_debug_labels: false",
                f"probationary_paper_lanes_json: '{json.dumps([{'lane_id': 'lane_a'}])}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    overlay_path.write_text(
        f"probationary_paper_lanes_json: '{json.dumps([{'lane_id': 'lane_b'}])}'\n",
        encoding="utf-8",
    )

    settings = load_settings_from_files([base_path, overlay_path])

    assert [lane.get("lane_id") for lane in settings.probationary_paper_lane_specs] == ["lane_a", "lane_b"]
