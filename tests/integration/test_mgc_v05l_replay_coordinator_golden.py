"""Golden acceptance coverage for the multi-strategy replay coordinator."""

from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.container import build_application_container
from mgc_v05l.app.main import main
from mgc_v05l.app.runner import StrategyServiceRunner
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.replay_feed import ReplayFeed


def _write_multi_strategy_replay_config(tmp_path: Path) -> tuple[Path, Path]:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "golden-replay.sqlite3"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        "\n".join(
            [
                'mode: "replay"',
                f'database_url: "sqlite:///{replay_db}"',
                'standalone_strategy_definitions_json: \'['
                '{"display_name":"GC / replayLegacy","symbol":"GC","strategy_family":"replayLegacyGc","strategy_identity_root":"gc_replay_legacy","trade_size":1,"point_value":100},'
                '{"display_name":"PL / replayLegacy","symbol":"PL","strategy_family":"replayLegacyPl","strategy_identity_root":"pl_replay_legacy","trade_size":1,"point_value":50},'
                '{"display_name":"CL / failed_move_no_us_reversal_short","symbol":"CL","strategy_family":"failed_move_reversal","strategy_identity_root":"failed_move_no_us_reversal_short","runtime_kind":"approved_quant_strategy_engine","trade_size":1,"point_value":1000}'
                "]'",
                "enable_bear_snap_shorts: false",
                "enable_asia_vwap_longs: false",
                "atr_len: 2",
                "max_bars_long: 2",
                "max_bars_short: 2",
                "anti_churn_bars: 1",
                "turn_fast_len: 1",
                "turn_slow_len: 3",
                "turn_stretch_lookback: 2",
                "min_snap_down_stretch_atr: 0.10",
                "min_snap_bar_range_atr: 0.10",
                "min_snap_body_atr: 0.10",
                "min_snap_close_location: 0.50",
                "min_snap_velocity_delta_atr: 0.00",
                "snap_cooldown_bars: 1",
                "use_asia_bull_snap_thresholds: false",
                "asia_min_snap_bar_range_atr: 0.10",
                "asia_min_snap_body_atr: 0.10",
                "asia_min_snap_velocity_delta_atr: 0.00",
                "use_bull_snap_location_filter: false",
                "bull_snap_max_close_vs_slow_ema_atr: 10.0",
                "bull_snap_require_close_below_slow_ema: false",
                "use_bear_snap_location_filter: false",
                "bear_snap_min_close_vs_slow_ema_atr: 0.0",
                "bear_snap_require_close_above_slow_ema: false",
                "below_vwap_lookback: 1",
                "require_green_reclaim_bar: false",
                "reclaim_close_buffer_atr: 0.0",
                "min_vwap_bar_range_atr: 0.10",
                "require_hold_close_above_vwap: false",
                "require_hold_not_break_reclaim_low: false",
                "require_acceptance_close_above_reclaim_high: false",
                "require_acceptance_close_above_vwap: false",
                "vwap_long_max_bars: 2",
                "vwap_weak_close_lookback_bars: 1",
                "vol_len: 1",
                "show_debug_labels: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return base_config, replay_config


def test_golden_multi_strategy_replay_fixture_produces_stable_summary(tmp_path: Path) -> None:
    base_config, replay_config = _write_multi_strategy_replay_config(tmp_path)
    csv_path = Path("tests/fixtures/replay_multi_strategy_golden.csv")

    container = build_application_container([base_config, replay_config])
    summary = StrategyServiceRunner(container).run_replay(csv_path)

    assert summary.processed_bars == 36
    assert summary.order_intents == 4
    assert summary.fills == 4
    assert summary.long_entries == 2
    assert summary.short_entries == 0
    assert summary.exits == 2
    assert summary.primary_standalone_strategy_id == "gc_replay_legacy__GC"

    aggregate = summary.aggregate_portfolio_summary
    assert aggregate.standalone_strategy_count == 3
    assert aggregate.strategy_count == 3
    assert aggregate.processed_bars == 36
    assert aggregate.order_intents == 4
    assert aggregate.fills == 4
    assert aggregate.entries == 2
    assert aggregate.exits == 2
    assert aggregate.standalone_strategy_ids == (
        "gc_replay_legacy__GC",
        "pl_replay_legacy__PL",
        "failed_move_no_us_reversal_short__CL",
    )

    per_strategy = {row.standalone_strategy_id: row for row in summary.per_strategy_summaries}
    assert set(per_strategy) == set(aggregate.standalone_strategy_ids)
    assert per_strategy["gc_replay_legacy__GC"].processed_bars == 12
    assert per_strategy["gc_replay_legacy__GC"].order_intents == 2
    assert per_strategy["gc_replay_legacy__GC"].fills == 2
    assert per_strategy["pl_replay_legacy__PL"].processed_bars == 12
    assert per_strategy["pl_replay_legacy__PL"].order_intents == 2
    assert per_strategy["pl_replay_legacy__PL"].fills == 2
    assert per_strategy["failed_move_no_us_reversal_short__CL"].processed_bars == 12
    assert per_strategy["failed_move_no_us_reversal_short__CL"].order_intents == 0
    assert per_strategy["failed_move_no_us_reversal_short__CL"].fills == 0


def test_replay_cli_writes_json_and_markdown_reports_for_golden_fixture(tmp_path: Path, capsys) -> None:
    base_config, replay_config = _write_multi_strategy_replay_config(tmp_path)
    json_output = tmp_path / "golden-replay-summary.json"
    markdown_output = tmp_path / "golden-replay-summary.md"
    csv_path = Path("tests/fixtures/replay_multi_strategy_golden.csv")

    exit_code = main(
        [
            "replay",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--csv",
            str(csv_path),
            "--output",
            str(json_output),
            "--markdown-output",
            str(markdown_output),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    markdown = markdown_output.read_text(encoding="utf-8")

    assert exit_code == 0
    assert json_output.exists()
    assert markdown_output.exists()
    assert payload["processed_bars"] == 36
    assert payload["aggregate_portfolio_summary"]["standalone_strategy_count"] == 3
    assert payload["aggregate_portfolio_summary"]["standalone_strategy_ids"] == [
        "gc_replay_legacy__GC",
        "pl_replay_legacy__PL",
        "failed_move_no_us_reversal_short__CL",
    ]
    assert "Replay Summary" in markdown
    assert "`gc_replay_legacy__GC`" in markdown
    assert "`failed_move_no_us_reversal_short__CL`" in markdown


def test_multi_symbol_replay_feed_accepts_symbol_column_fixture(tmp_path: Path) -> None:
    base_config, replay_config = _write_multi_strategy_replay_config(tmp_path)
    settings = load_settings_from_files([base_config, replay_config])
    bars = ReplayFeed(settings).load_csv(Path("tests/fixtures/replay_multi_strategy_golden.csv"))

    assert bars[0].symbol == "CL"
    assert bars[1].symbol == "GC"
    assert bars[2].symbol == "PL"
    assert len(bars) == 36
    assert all(isinstance(bar, Bar) for bar in bars)


def test_processed_bars_remain_identity_aware_without_duplicate_processing(tmp_path: Path) -> None:
    base_config, replay_config = _write_multi_strategy_replay_config(tmp_path)
    settings = load_settings_from_files([base_config, replay_config])
    registry = build_application_container([base_config, replay_config]).strategy_runtime_registry
    first_bar = ReplayFeed(settings).load_csv(Path("tests/fixtures/replay_multi_strategy_golden.csv"))[1]

    registry.process_bar(first_bar)
    registry.process_bar(first_bar)

    instances = {instance.definition.standalone_strategy_id: instance for instance in registry.instances if instance.repositories is not None}
    assert instances["gc_replay_legacy__GC"].repositories.processed_bars.count() == 1
    assert instances["pl_replay_legacy__PL"].repositories.processed_bars.count() == 0
    assert instances["failed_move_no_us_reversal_short__CL"].repositories.processed_bars.count() == 0
