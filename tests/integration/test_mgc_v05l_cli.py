"""CLI integration coverage for replay research utilities."""

from datetime import datetime, timedelta
import json
import sqlite3
from pathlib import Path
from unittest.mock import Mock

import mgc_v05l.app.schwab_token_bootstrap_web as schwab_token_bootstrap_web_module
from mgc_v05l.app.main import main
from mgc_v05l.app.probationary_runtime import ProbationaryRuntimeTransportFailure


def test_research_causal_report_cli_writes_output(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    report_csv = tmp_path / "report.csv"
    replay_db = tmp_path / "cli.sqlite3"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    replay_csv = tmp_path / "replay.csv"
    replay_csv.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T18:00:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:05:00-04:00,100,103,99,102,100\n"
        "2026-03-13T18:10:00-04:00,102,104,101,103,100\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "research-causal-report",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--csv",
            str(replay_csv),
            "--output",
            str(report_csv),
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    assert payload["rows"] == 3
    assert payload["research_only"] is True
    assert report_csv.exists()


def test_market_data_live_trade_capture_cli_replays_jsonl(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "cli_tick_engine.sqlite3"
    input_jsonl = tmp_path / "ticks.jsonl"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    input_jsonl.write_text(
        "\n".join(
            [
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:30:05-04:00", "price": "100.0", "size": 1}),
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:31:05-04:00", "price": "101.0", "size": 2}),
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:32:05-04:00", "price": "102.0", "size": 3}),
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:33:05-04:00", "price": "103.0", "size": 4}),
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:34:05-04:00", "price": "104.0", "size": 5}),
                json.dumps({"symbol": "MGC", "timestamp": "2026-04-17T09:35:05-04:00", "price": "105.0", "size": 6}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = main(
        [
            "market-data-live-trade-capture",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "MGC",
            "--input-jsonl",
            str(input_jsonl),
            "--derive-timeframe",
            "5m",
            "--raw-data-source",
            "cli_tick_replay",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "jsonl_replay"
    assert payload["trade_count"] == 6
    assert payload["finalized_bar_count"] == 6
    assert payload["per_symbol_trade_count"] == {"MGC": 6}
    assert payload["per_symbol_finalized_bars"] == {"MGC": 6}
    assert payload["derived_timeframes"] == ["5m"]


def test_opening_drive_research_cli_reports_highlighted_session(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "opening_drive.sqlite3"
    output_dir = tmp_path / "report"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    _seed_opening_drive_cli_db(replay_db)

    exit_code = main(
        [
            "es-mes-opening-drive-continuation-research",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "ES",
            "--variant",
            "v2",
            "--start-date",
            "2026-04-17",
            "--end-date",
            "2026-04-17",
            "--inspect-date",
            "2026-04-17",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "es_mes_opening_drive_continuation_research"
    assert payload["highlighted_session_date"] == "2026-04-17"
    assert payload["highlighted_sessions"]["ES"]["entered"] is True
    assert payload["highlighted_sessions"]["ES"]["entry_bar_number"] is not None
    assert payload["highlighted_sessions"]["ES"]["net_pnl_points"] < payload["highlighted_sessions"]["ES"]["pnl_points"]
    assert output_dir.joinpath("es_mes_opening_drive_continuation_research.json").exists()
    assert output_dir.joinpath("es_mes_opening_drive_continuation_research.md").exists()


def test_gc_mgc_segment_regime_research_cli_writes_dataset_and_reports(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "gold_segment.sqlite3"
    output_dir = tmp_path / "report"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    _seed_gold_segment_cli_db(replay_db)

    exit_code = main(
        [
            "gc-mgc-segment-regime-research",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "GC",
            "--start-date",
            "2026-04-21",
            "--end-date",
            "2026-04-21",
            "--inspect-date",
            "2026-04-21",
            "--setup-minutes",
            "5",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_segment_regime_research"
    assert payload["row_count"] == 2
    assert output_dir.joinpath("gc_mgc_segment_regime_dataset.csv").exists()
    assert output_dir.joinpath("gc_mgc_segment_regime_research.json").exists()
    assert output_dir.joinpath("gc_mgc_segment_regime_research.md").exists()


def test_gc_mgc_london_late_long_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "gold_london_late.sqlite3"
    output_dir = tmp_path / "report"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    _seed_gold_london_late_cli_db(replay_db)

    exit_code = main(
        [
            "gc-mgc-london-late-long-research",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "GC",
            "--variant",
            "london_late_long_v1",
            "--start-date",
            "2026-04-21",
            "--end-date",
            "2026-04-21",
            "--inspect-date",
            "2026-04-21",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_london_late_long_research"
    assert payload["highlighted_trade_date"] == "2026-04-21"
    assert output_dir.joinpath("gc_mgc_london_late_long_research.json").exists()
    assert output_dir.joinpath("gc_mgc_london_late_long_research.md").exists()


def test_gc_mgc_london_late_meta_label_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(json.dumps(_synthetic_london_late_meta_label_source()), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-london-late-meta-label-research",
            "--source-json",
            str(source_json),
            "--source-variant",
            "london_late_long_v5_3m_soft",
            "--signal-symbol",
            "GC",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_london_late_meta_label_research"
    assert output_dir.joinpath("gc_mgc_london_late_meta_label_research.json").exists()
    assert output_dir.joinpath("gc_mgc_london_late_meta_label_dataset.csv").exists()


def test_gc_mgc_ny_early_short_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "gold_ny_early.sqlite3"
    output_dir = tmp_path / "report"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    _seed_gold_ny_early_cli_db(replay_db)

    exit_code = main(
        [
            "gc-mgc-ny-early-short-research",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "GC",
            "--variant",
            "ny_early_short_v1_failed_pop_5m",
            "--start-date",
            "2026-04-21",
            "--end-date",
            "2026-04-21",
            "--inspect-date",
            "2026-04-21",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_research"
    assert payload["highlighted_trade_date"] == "2026-04-21"
    assert output_dir.joinpath("gc_mgc_ny_early_short_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_research.md").exists()


def test_gc_mgc_ny_early_short_meta_label_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(json.dumps(_synthetic_ny_early_short_meta_label_source()), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-ny-early-short-meta-label-research",
            "--source-json",
            str(source_json),
            "--source-variant",
            "ny_early_short_v2_failed_pop_3m",
            "--signal-symbol",
            "GC",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_meta_label_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_label_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_label_dataset.csv").exists()


def test_gc_mgc_ny_early_short_exit_overlay_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    meta_json = tmp_path / "meta.json"
    output_dir = tmp_path / "report"
    replay_db = tmp_path / "overlay.sqlite3"
    _seed_gold_ny_early_cli_db(replay_db)
    source_json.write_text(json.dumps(_synthetic_ny_early_short_source_for_overlay(replay_db)), encoding="utf-8")
    dataset_csv = tmp_path / "meta_dataset.csv"
    dataset_csv.write_text(
        "source_variant,split,symbol,trade_date,label,net_pnl_points,predicted_probability,selected,pre_context_to_setup_range_ratio,setup_abs_efficiency,setup_close_location,setup_vwap_displacement,setup_green_share,setup_red_share,setup_volume_ratio,setup_score\n"
        "ny_early_short_v2_failed_pop_3m,test,GC,2026-04-21,1,1.2,0.8,True,0.8,0.7,0.8,0.2,0.7,0.3,1.5,6\n",
        encoding="utf-8",
    )
    meta_json.write_text(
        json.dumps(
            {
                "dataset_path": str(dataset_csv),
                "variant_reports": [{"source_variant": "ny_early_short_v2_failed_pop_3m"}],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "gc-mgc-ny-early-short-exit-overlay-research",
            "--source-json",
            str(source_json),
            "--meta-json",
            str(meta_json),
            "--source-variant",
            "ny_early_short_v2_failed_pop_3m",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_exit_overlay_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_exit_overlay_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_exit_overlay_research.md").exists()


def test_gc_mgc_ny_early_short_confirmation_policy_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    meta_json = tmp_path / "meta.json"
    dataset_csv = tmp_path / "dataset.csv"
    output_dir = tmp_path / "report"
    dataset_csv.write_text(
        "\n".join(
            [
                "source_variant,split,symbol,trade_date,label,net_pnl_points,predicted_probability,selected",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-10,1,5.0,0.80,True",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-10,1,4.5,0.77,True",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-11,0,-3.0,0.66,True",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-11,0,-2.5,0.68,True",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    meta_json.write_text(json.dumps({"dataset_path": str(dataset_csv)}), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-ny-early-short-confirmation-policy-research",
            "--meta-json",
            str(meta_json),
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_confirmation_policy_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_confirmation_policy_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_confirmation_policy_research.md").exists()


def test_gc_mgc_ny_early_short_meta_threshold_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    meta_json = tmp_path / "meta.json"
    dataset_csv = tmp_path / "dataset.csv"
    output_dir = tmp_path / "report"
    dataset_csv.write_text(
        "\n".join(
            [
                "source_variant,split,symbol,trade_date,predicted_probability,net_pnl_points",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-10,0.45,1.0",
                "ny_early_short_v2_failed_pop_3m,test,GC,2026-01-11,0.65,3.0",
                "ny_early_short_v2_failed_pop_3m,test,MGC,2026-01-10,0.70,4.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    meta_json.write_text(json.dumps({"dataset_path": str(dataset_csv)}), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-ny-early-short-meta-threshold-research",
            "--meta-json",
            str(meta_json),
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_meta_threshold_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_threshold_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_meta_threshold_research.md").exists()


def test_gc_mgc_ny_early_short_walkforward_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(json.dumps(_synthetic_ny_early_short_walkforward_source()), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-ny-early-short-walkforward-research",
            "--source-json",
            str(source_json),
            "--source-variant",
            "ny_early_short_v2_failed_pop_3m",
            "--min-train-dates",
            "12",
            "--test-window-dates",
            "4",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_walkforward_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_walkforward_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_walkforward_research.md").exists()


def test_gc_mgc_ny_early_short_forced_session_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    replay_db = tmp_path / "gold_ny_early_forced.sqlite3"
    output_dir = tmp_path / "report"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db}"\n',
        encoding="utf-8",
    )
    _seed_gold_ny_early_cli_db(replay_db)

    exit_code = main(
        [
            "gc-mgc-ny-early-short-forced-session-research",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--symbol",
            "GC",
            "--variant",
            "ny_forced_short_v1_breakdown_or_bar6",
            "--start-date",
            "2026-04-21",
            "--end-date",
            "2026-04-21",
            "--inspect-date",
            "2026-04-21",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_forced_session_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_forced_session_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_forced_session_research.md").exists()


def test_gc_mgc_ny_early_short_forced_session_walkforward_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(json.dumps(_synthetic_ny_early_short_forced_walkforward_source()), encoding="utf-8")

    exit_code = main(
        [
            "gc-mgc-ny-early-short-forced-session-walkforward-research",
            "--source-json",
            str(source_json),
            "--source-variant",
            "ny_forced_short_v2_reclaim_fail_or_bar7",
            "--test-window-dates",
            "5",
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_ny_early_short_forced_session_walkforward_research"
    assert output_dir.joinpath("gc_mgc_ny_early_short_forced_session_walkforward_research.json").exists()
    assert output_dir.joinpath("gc_mgc_ny_early_short_forced_session_walkforward_research.md").exists()


def test_gc_mgc_segment_forced_session_long_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "report"
    exit_code = main(
        [
            "gc-mgc-segment-forced-session-long-research",
            "--segment-id",
            "ASIA_EARLY",
            "--output-dir",
            str(output_dir),
            "--config",
            "config/base.yaml",
            "--config",
            "config/research_archive.yaml",
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-03",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_segment_forced_session_long_research"
    assert output_dir.joinpath("gc_mgc_segment_forced_session_long_research.json").exists()
    assert output_dir.joinpath("gc_mgc_segment_forced_session_long_research.md").exists()


def test_gc_mgc_segment_forced_session_walkforward_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(
        json.dumps(
            {
                "definition": {"segment_id": "ASIA_EARLY"},
                "symbol_reports": {
                    "GC": {
                        "variants": {
                            "segment_forced_long_v1_breakout_or_bar6": {
                                "sessions": [
                                    {"trade_date": f"2026-01-{index + 1:02d}", "entered": True, "net_pnl_points": 1.0}
                                    for index in range(10)
                                ]
                            }
                        }
                    },
                    "MGC": {
                        "variants": {
                            "segment_forced_long_v1_breakout_or_bar6": {
                                "sessions": [
                                    {"trade_date": f"2026-01-{index + 1:02d}", "entered": True, "net_pnl_points": 0.5}
                                    for index in range(10)
                                ]
                            }
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "gc-mgc-segment-forced-session-walkforward-research",
            "--source-json",
            str(source_json),
            "--output-dir",
            str(output_dir),
            "--source-variant",
            "segment_forced_long_v1_breakout_or_bar6",
            "--test-window-dates",
            "4",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_segment_forced_session_walkforward_research"
    assert output_dir.joinpath("gc_mgc_segment_forced_session_walkforward_research.json").exists()
    assert output_dir.joinpath("gc_mgc_segment_forced_session_walkforward_research.md").exists()


def test_gc_mgc_segment_forced_session_short_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "report"
    exit_code = main(
        [
            "gc-mgc-segment-forced-session-short-research",
            "--segment-id",
            "ASIA_EARLY",
            "--output-dir",
            str(output_dir),
            "--config",
            "config/base.yaml",
            "--config",
            "config/research_archive.yaml",
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-03",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_segment_forced_session_short_research"
    assert output_dir.joinpath("gc_mgc_segment_forced_session_short_research.json").exists()
    assert output_dir.joinpath("gc_mgc_segment_forced_session_short_research.md").exists()


def test_gc_mgc_forced_session_portfolio_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(
        json.dumps(
            {
                "symbol_reports": {
                    "GC": {
                        "variants": {
                            "variant_a": {
                                "trade_summary": {"entered_trade_count": 4},
                                "sessions": [
                                    {"trade_date": f"2026-01-{index + 1:02d}", "entered": True, "net_pnl_points": 1.0}
                                    for index in range(4)
                                ],
                            }
                        }
                    },
                    "MGC": {
                        "variants": {
                            "variant_a": {
                                "trade_summary": {"entered_trade_count": 4},
                                "sessions": [
                                    {"trade_date": f"2026-01-{index + 1:02d}", "entered": True, "net_pnl_points": 0.5}
                                    for index in range(4)
                                ],
                            }
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "gc-mgc-forced-session-portfolio-research",
            "--lane",
            f"TEST|{source_json}|variant_a",
            "--output-dir",
            str(output_dir),
            "--test-window-dates",
            "2",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_forced_session_portfolio_research"
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_research.json").exists()
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_research.md").exists()


def test_gc_mgc_forced_session_portfolio_shaping_research_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(
        json.dumps(
            {
                "symbol_reports": {
                    "GC": {
                        "daily_rows": [
                            {
                                "trade_date": "2026-01-01",
                                "lane_count": 2,
                                "daily_net_pnl_points": 1.0,
                                "cumulative_net_pnl_points": 1.0,
                                "drawdown_points": 0.0,
                                "lane_pnls": {"ASIA_EARLY_SHORT": 1.0, "NY_EARLY_SHORT": 0.0},
                            }
                        ]
                    },
                    "MGC": {
                        "daily_rows": [
                            {
                                "trade_date": "2026-01-01",
                                "lane_count": 2,
                                "daily_net_pnl_points": 0.5,
                                "cumulative_net_pnl_points": 0.5,
                                "drawdown_points": 0.0,
                                "lane_pnls": {"ASIA_EARLY_SHORT": 0.5, "NY_EARLY_SHORT": 0.0},
                            }
                        ]
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "gc-mgc-forced-session-portfolio-shaping-research",
            "--source-json",
            str(source_json),
            "--output-dir",
            str(output_dir),
            "--test-window-dates",
            "2",
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_forced_session_portfolio_shaping_research"
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_shaping_research.json").exists()
    assert output_dir.joinpath("gc_mgc_forced_session_portfolio_shaping_research.md").exists()


def test_gc_mgc_forced_session_candidate_system_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "source.json"
    output_dir = tmp_path / "report"
    source_json.write_text(
        json.dumps(
            {
                "symbol_reports": {
                    "GC": {
                        "daily_rows": [
                            {
                                "trade_date": "2026-01-01",
                                "lane_count": 4,
                                "daily_net_pnl_points": 1.0,
                                "cumulative_net_pnl_points": 1.0,
                                "drawdown_points": 0.0,
                                "lane_pnls": {
                                    "ASIA_EARLY_SHORT": 0.1,
                                    "LONDON_EARLY_LONG": 0.2,
                                    "NY_EARLY_SHORT": 0.3,
                                    "NY_LATE_SHORT": 0.4,
                                },
                            }
                        ]
                    },
                    "MGC": {
                        "daily_rows": [
                            {
                                "trade_date": "2026-01-01",
                                "lane_count": 4,
                                "daily_net_pnl_points": 0.5,
                                "cumulative_net_pnl_points": 0.5,
                                "drawdown_points": 0.0,
                                "lane_pnls": {
                                    "ASIA_EARLY_SHORT": 0.05,
                                    "LONDON_EARLY_LONG": 0.1,
                                    "NY_EARLY_SHORT": 0.15,
                                    "NY_LATE_SHORT": 0.2,
                                },
                            }
                        ]
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "gc-mgc-forced-session-candidate-system",
            "--source-json",
            str(source_json),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_forced_session_candidate_system_research"
    assert output_dir.joinpath("gc_mgc_forced_session_candidate_system_research.json").exists()
    assert output_dir.joinpath("gc_mgc_forced_session_candidate_system_research.md").exists()


def test_gc_mgc_forced_session_candidate_admission_plan_cli_writes_reports(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "candidate.json"
    output_dir = tmp_path / "report"
    source_json.write_text(
        json.dumps(
            {
                "candidate_system": {
                    "candidate_id": "gc_mgc_forced_session_baseline_v1",
                    "lane_sequence": [
                        {
                            "lane_id": "ASIA_EARLY_SHORT",
                            "segment_id": "ASIA_EARLY",
                            "side": "SHORT",
                            "session_start_et": "19:00",
                            "session_end_et": "20:30",
                            "source_json": "/tmp/a.json",
                            "source_variant": "variant_a",
                            "entry_family": "failed reclaim",
                            "execution_note": "note",
                        }
                    ],
                },
                "scenario_reports": [
                    {
                        "scenario_id": "mgc_1x_all_lanes",
                        "label": "MGC 1x",
                        "description": "desc",
                        "allocations": [{"lane_id": "ASIA_EARLY_SHORT", "symbol": "MGC", "contracts": 1}],
                        "summary": {"total_net_pnl_dollars": 100.0, "net_profit_factor": 1.5},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "gc-mgc-forced-session-candidate-admission-plan",
            "--candidate-system-json",
            str(source_json),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "gc_mgc_forced_session_candidate_admission_plan"
    assert output_dir.joinpath("gc_mgc_forced_session_candidate_admission_plan.json").exists()
    assert output_dir.joinpath("mgc_1x_all_lanes.paper_package.yaml").exists()


def test_gc_mgc_forced_session_candidate_load_proof_cli_loads_package(tmp_path: Path, capsys) -> None:
    source_json = tmp_path / "candidate.json"
    admission_dir = tmp_path / "admission"
    load_dir = tmp_path / "load"
    base_config = tmp_path / "base.yaml"
    live_config = tmp_path / "live.yaml"
    probationary_config = tmp_path / "probationary.yaml"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    live_config.write_text(Path("config/live.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    probationary_config.write_text(
        Path("config/probationary_pattern_engine.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    source_json.write_text(
        json.dumps(
            {
                "candidate_system": {
                    "candidate_id": "gc_mgc_forced_session_baseline_v1",
                    "lane_sequence": [
                        {
                            "lane_id": "ASIA_EARLY_SHORT",
                            "segment_id": "ASIA_EARLY",
                            "side": "SHORT",
                            "session_start_et": "19:00",
                            "session_end_et": "20:30",
                            "source_json": "/tmp/a.json",
                            "source_variant": "variant_a",
                            "entry_family": "failed reclaim",
                            "execution_note": "note",
                        },
                        {
                            "lane_id": "LONDON_EARLY_LONG",
                            "segment_id": "LONDON_EARLY",
                            "side": "LONG",
                            "session_start_et": "03:00",
                            "session_end_et": "05:30",
                            "source_json": "/tmp/b.json",
                            "source_variant": "variant_b",
                            "entry_family": "dip reclaim",
                            "execution_note": "note",
                        },
                    ],
                },
                "scenario_reports": [
                    {
                        "scenario_id": "gc_1x_all_lanes",
                        "label": "GC 1x",
                        "description": "desc",
                        "allocations": [
                            {"lane_id": "ASIA_EARLY_SHORT", "symbol": "GC", "contracts": 1},
                            {"lane_id": "LONDON_EARLY_LONG", "symbol": "GC", "contracts": 1},
                        ],
                        "summary": {"total_net_pnl_dollars": 100.0, "net_profit_factor": 1.5},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "gc-mgc-forced-session-candidate-admission-plan",
            "--candidate-system-json",
            str(source_json),
            "--output-dir",
            str(admission_dir),
        ]
    )
    assert exit_code == 0
    _ = capsys.readouterr().out

    exit_code = main(
        [
            "gc-mgc-forced-session-candidate-load-proof",
            "--config",
            str(base_config),
            "--config",
            str(live_config),
            "--config",
            str(probationary_config),
            "--package-yaml",
            str(admission_dir / "gc_1x_all_lanes.paper_package.yaml"),
            "--output-dir",
            str(load_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["mode"] == "gc_mgc_forced_session_candidate_load_proof"
    assert payload["all_loaded"] is True
    assert payload["lane_count"] == 2
    assert load_dir.joinpath("gc_mgc_forced_session_candidate_load_proof.json").exists()


def test_probationary_paper_soak_cli_invokes_runner(monkeypatch, capsys, tmp_path: Path) -> None:
    summary = {
        "artifacts_dir": tmp_path / "paper_session",
        "processed_bars": 7,
        "reconciliation_clean": True,
        "stop_reason": None,
    }
    runner = Mock()
    runner.run.return_value = summary
    builder = Mock(return_value=runner)
    monkeypatch.setattr("mgc_v05l.app.main.build_probationary_paper_runner", builder)

    exit_code = main(
        [
            "probationary-paper-soak",
            "--schwab-config",
            "config/custom_schwab.json",
            "--poll-once",
            "--max-cycles",
            "5",
        ]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(stdout)

    assert exit_code == 0
    builder.assert_called_once_with(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        schwab_config_path="config/custom_schwab.json",
    )
    runner.run.assert_called_once_with(poll_once=True, max_cycles=5)
    assert payload == {
        "artifacts_dir": str(tmp_path / "paper_session"),
        "processed_bars": 7,
        "reconciliation_clean": True,
        "stop_reason": None,
    }


def test_probationary_operator_control_cli_routes_shared_lane_target(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_submit(config_paths, action: str, *, payload=None, shared_strategy_identity=None):
        captured["config_paths"] = list(config_paths)
        captured["action"] = action
        captured["payload"] = payload
        captured["shared_strategy_identity"] = shared_strategy_identity
        return {
            "action": action,
            "control_path": "/tmp/operator_control.json",
            "status": "pending",
            "requested_at": "2026-04-01T00:00:00+00:00",
        }

    monkeypatch.setattr("mgc_v05l.app.main.submit_probationary_operator_control", _fake_submit)

    exit_code = main(
        [
            "probationary-operator-control",
            "--action",
            "resume_entries",
            "--lane-id",
            "mgc_us_late_pause_resume_long",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "action": "resume_entries",
        "payload": {"lane_id": "mgc_us_late_pause_resume_long"},
        "shared_strategy_identity": None,
    }
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"


def _seed_opening_drive_cli_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            create table bars (
                symbol text not null,
                timeframe text not null,
                data_source text not null,
                start_ts text not null,
                end_ts text not null,
                open real not null,
                high real not null,
                low real not null,
                close real not null,
                volume integer not null
            )
            """
        )
        rows = _build_opening_drive_seed_rows()
        connection.executemany(
            """
            insert into bars (symbol, timeframe, data_source, start_ts, end_ts, open, high, low, close, volume)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def _seed_gold_segment_cli_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            create table bars (
                symbol text not null,
                timeframe text not null,
                data_source text not null,
                start_ts text not null,
                end_ts text not null,
                open real not null,
                high real not null,
                low real not null,
                close real not null,
                volume integer not null
            )
            """
        )
        rows: list[tuple[object, ...]] = []
        current = datetime.fromisoformat("2026-04-21T07:50:00-04:00")
        price = 3300.0
        for _ in range(30):
            rows.append(
                _one_minute_bar(
                    end_ts=current.isoformat(),
                    open_=price,
                    high=price + 0.2,
                    low=price - 0.2,
                    close=price + 0.1,
                    volume=80,
                    symbol="GC",
                )
            )
            price += 0.05
            current += timedelta(minutes=1)
        current = datetime.fromisoformat("2026-04-21T08:20:00-04:00")
        for open_, high, low, close in [
            (3301.5, 3302.4, 3301.4, 3302.2),
            (3302.2, 3303.0, 3302.0, 3302.9),
            (3302.9, 3303.6, 3302.8, 3303.5),
            (3303.5, 3304.0, 3303.3, 3303.8),
            (3303.8, 3304.4, 3303.7, 3304.2),
            (3304.2, 3304.8, 3304.0, 3304.7),
            (3304.7, 3305.4, 3304.6, 3305.2),
            (3305.2, 3305.9, 3305.0, 3305.7),
            (3305.7, 3306.1, 3305.5, 3306.0),
            (3306.0, 3306.4, 3305.8, 3306.2),
        ]:
            rows.append(
                _one_minute_bar(
                    end_ts=current.isoformat(),
                    open_=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=180,
                    symbol="GC",
                )
            )
            current += timedelta(minutes=1)
        connection.executemany(
            """
            insert into bars (symbol, timeframe, data_source, start_ts, end_ts, open, high, low, close, volume)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def _seed_gold_london_late_cli_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            create table bars (
                symbol text not null,
                timeframe text not null,
                data_source text not null,
                start_ts text not null,
                end_ts text not null,
                open real not null,
                high real not null,
                low real not null,
                close real not null,
                volume integer not null
            )
            """
        )
        rows: list[tuple[object, ...]] = []
        current = datetime.fromisoformat("2026-04-21T05:01:00-04:00")
        price = 3300.0
        for _ in range(30):
            rows.append(
                _one_minute_bar(
                    end_ts=current.isoformat(),
                    open_=price,
                    high=price + 0.12,
                    low=price - 0.10,
                    close=price + 0.03,
                    volume=100,
                    symbol="GC",
                )
            )
            price += 0.01
            current += timedelta(minutes=1)

        current = "2026-04-21T05:35:00-04:00"
        for open_, high, low, close, volume in [
            (3300.2, 3301.0, 3300.1, 3300.9, 180),
            (3300.9, 3302.0, 3300.8, 3301.8, 190),
            (3301.8, 3303.2, 3301.7, 3303.0, 210),
            (3303.0, 3303.8, 3302.9, 3303.5, 220),
            (3303.5, 3305.2, 3303.4, 3304.9, 240),
            (3304.9, 3305.0, 3304.2, 3304.6, 200),
            (3304.6, 3304.7, 3303.9, 3304.1, 190),
        ]:
            rows.extend(_explode_five_minute_bar(current, open_, high, low, close, volume))
            end_dt = datetime.fromisoformat(current) + timedelta(minutes=5)
            current = end_dt.isoformat()

        connection.executemany(
            """
            insert into bars (symbol, timeframe, data_source, start_ts, end_ts, open, high, low, close, volume)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def _seed_gold_ny_early_cli_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE bars (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                data_source TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL
            )
            """
        )
        rows: list[tuple[object, ...]] = []
        current = datetime.fromisoformat("2026-04-21T07:51:00-04:00")
        price = 3300.0
        for _ in range(30):
            rows.append(
                _one_minute_bar(
                    symbol="GC",
                    end_ts=current.isoformat(),
                    open_=price,
                    high=price + 0.10,
                    low=price - 0.12,
                    close=price + 0.01,
                    volume=120,
                )
            )
            price += 0.01
            current += timedelta(minutes=1)
        current = "2026-04-21T08:25:00-04:00"
        for open_, high, low, close, volume in [
            (3300.4, 3302.4, 3300.3, 3302.2, 250),
            (3302.2, 3304.2, 3302.1, 3304.0, 280),
            (3304.0, 3305.0, 3303.8, 3304.6, 310),
            (3304.6, 3304.8, 3300.0, 3300.2, 330),
            (3300.2, 3300.5, 3298.2, 3298.6, 320),
            (3298.6, 3299.7, 3298.4, 3299.0, 210),
        ]:
            rows.extend(_explode_five_minute_bar(current, open_, high, low, close, volume))
            hour, minute = current[11:13], int(current[14:16]) + 5
            next_hour = int(hour) + minute // 60
            next_minute = minute % 60
            current = f"2026-04-21T{next_hour:02d}:{next_minute:02d}:00-04:00"
        connection.executemany(
            "INSERT INTO bars(symbol, timeframe, data_source, start_ts, end_ts, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def _synthetic_london_late_meta_label_source() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(60):
            date = f"2026-01-{index + 1:02d}" if index < 31 else f"2026-02-{index - 30:02d}"
            strong = index % 3 != 0
            sessions.append(
                {
                    "trade_date": date,
                    "entered": True,
                    "net_pnl_points": 2.0 if strong else -1.5,
                    "pre_context_to_setup_range_ratio": 0.9 if strong else 2.2,
                    "setup_abs_efficiency": 0.7 if strong else 0.2,
                    "setup_close_location": 0.9 if strong else 0.55,
                    "setup_vwap_displacement": 0.3 if strong else 0.02,
                    "setup_green_share": 0.75 if strong else 0.25,
                    "setup_volume_ratio": 2.5 if strong else 0.8,
                    "setup_score": 6 if strong else 3,
                }
            )
        symbols[symbol] = {
            "variants": {
                "london_late_long_v5_3m_soft": {
                    "sessions": sessions,
                }
            }
        }
    return {"symbol_reports": symbols}


def _synthetic_ny_early_short_meta_label_source() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(60):
            date = f"2026-01-{index + 1:02d}" if index < 31 else f"2026-02-{index - 30:02d}"
            strong = index % 3 != 0
            sessions.append(
                {
                    "trade_date": date,
                    "entered": True,
                    "net_pnl_points": 1.8 if strong else -1.4,
                    "pre_context_to_setup_range_ratio": 0.8 if strong else 2.0,
                    "setup_abs_efficiency": 0.75 if strong else 0.2,
                    "setup_close_location": 0.85 if strong else 0.35,
                    "setup_vwap_displacement": 0.22 if strong else -0.08,
                    "setup_green_share": 0.75 if strong else 0.25,
                    "setup_red_share": 0.25 if strong else 0.75,
                    "setup_volume_ratio": 1.9 if strong else 0.9,
                    "setup_score": 6 if strong else 3,
                }
            )
        symbols[symbol] = {
            "variants": {
                "ny_early_short_v2_failed_pop_3m": {
                    "sessions": sessions,
                }
            }
        }
    return {"symbol_reports": symbols}


def _synthetic_ny_early_short_source_for_overlay(replay_db: Path) -> dict[str, object]:
    return {
        "database_path": str(replay_db),
        "symbol_reports": {
            "GC": {
                "variants": {
                    "ny_early_short_v2_failed_pop_3m": {
                        "sessions": [
                            {
                                "trade_date": "2026-04-21",
                                "entered": True,
                                "entry_bar_number": 2,
                                "entry_price": 3302.8,
                                "exit_bar_number": 3,
                                "exit_reason": "segment_close",
                                "pnl_points": 0.8,
                                "net_pnl_points": 0.55,
                                "gross_r_multiple": 0.4,
                                "net_r_multiple": 0.275,
                                "mfe_points": 1.1,
                                "mae_points": 0.2,
                                "risk_points": 2.0,
                                "stop_price": 3304.8,
                            }
                        ]
                    }
                }
            }
        },
    }


def _synthetic_ny_early_short_walkforward_source() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(24):
            date = f"2026-01-{index + 1:02d}"
            strong = index % 4 != 0
            sessions.append(
                {
                    "trade_date": date,
                    "entered": True,
                    "net_pnl_points": 2.0 if strong else -1.5,
                    "pre_context_to_setup_range_ratio": 0.8 if strong else 1.8,
                    "setup_abs_efficiency": 0.7 if strong else 0.2,
                    "setup_close_location": 0.8 if strong else 0.35,
                    "setup_vwap_displacement": 0.2 if strong else -0.05,
                    "setup_green_share": 0.7 if strong else 0.25,
                    "setup_red_share": 0.3 if strong else 0.75,
                    "setup_volume_ratio": 1.8 if strong else 0.9,
                    "setup_score": 6 if strong else 3,
                }
            )
        symbols[symbol] = {
            "variants": {
                "ny_early_short_v2_failed_pop_3m": {
                    "sessions": sessions,
                }
            }
        }
    return {"symbol_reports": symbols}


def _synthetic_ny_early_short_forced_walkforward_source() -> dict[str, object]:
    symbols = {}
    for symbol in ("GC", "MGC"):
        sessions = []
        for index in range(15):
            sessions.append(
                {
                    "trade_date": f"2026-01-{index + 1:02d}",
                    "entered": True,
                    "net_pnl_points": 1.5 if index % 3 else -0.5,
                }
            )
        symbols[symbol] = {
            "variants": {
                "ny_forced_short_v2_reclaim_fail_or_bar7": {
                    "sessions": sessions,
                }
            }
        }
    return {"symbol_reports": symbols}


def _build_opening_drive_seed_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    rows.extend(
        [
            _one_minute_bar(
            end_ts=f"2026-04-17T09:{minute:02d}:00-04:00",
            open_=price - 0.25,
            high=price + 0.25,
            low=price - 0.5,
            close=price,
            volume=200,
            )
            for minute, price in [
                (1, 7121.0),
                (2, 7123.0),
                (3, 7122.5),
                (4, 7124.0),
                (5, 7126.5),
                (6, 7125.5),
                (7, 7127.0),
                (8, 7128.5),
                (9, 7129.0),
            ]
        ]
    )
    current = "2026-04-17T09:35:00-04:00"
    for open_, high, low, close in [
        (7124.0, 7133.0, 7123.5, 7132.0),
        (7132.0, 7134.0, 7127.5, 7127.75),
        (7127.75, 7133.25, 7127.5, 7132.75),
        (7132.75, 7133.0, 7129.75, 7131.5),
        (7131.5, 7138.25, 7131.0, 7137.0),
        (7137.0, 7146.0, 7136.75, 7145.25),
        (7145.25, 7150.0, 7143.75, 7149.0),
        (7149.0, 7154.0, 7148.5, 7153.75),
        (7153.75, 7154.25, 7151.5, 7153.25),
        (7153.25, 7153.5, 7151.25, 7152.25),
        (7152.25, 7154.0, 7151.75, 7153.25),
        (7153.25, 7154.5, 7152.75, 7153.5),
        (7153.5, 7160.0, 7153.0, 7159.25),
        (7159.25, 7167.0, 7158.5, 7166.75),
        (7166.75, 7171.5, 7165.75, 7171.0),
        (7171.0, 7171.25, 7168.5, 7169.5),
        (7169.5, 7170.0, 7162.75, 7163.25),
        (7163.25, 7166.0, 7162.75, 7165.25),
    ]:
        rows.extend(_explode_five_minute_bar(current, open_, high, low, close, 2500))
        hour, minute = current[11:13], int(current[14:16]) + 5
        next_hour = int(hour) + minute // 60
        next_minute = minute % 60
        current = f"2026-04-17T{next_hour:02d}:{next_minute:02d}:00-04:00"
    return rows


def _explode_five_minute_bar(end_ts: str, open_: float, high: float, low: float, close: float, volume: int) -> list[tuple[object, ...]]:
    end_dt = datetime.fromisoformat(end_ts)
    minute_end_times = [end_dt - timedelta(minutes=4 - idx) for idx in range(5)]
    midpoint = round((low + close) / 2.0, 4)
    minute_specs = [
        (open_, open_, open_, open_),
        (open_, high, min(open_, high), high),
        (high, high, low, low),
        (low, max(low, midpoint), low, midpoint),
        (midpoint, max(midpoint, close), min(midpoint, close), close),
    ]
    rows: list[tuple[object, ...]] = []
    for minute_end, (minute_open, minute_high, minute_low, minute_close) in zip(minute_end_times, minute_specs, strict=True):
        rows.append(
            _one_minute_bar(
                end_ts=minute_end.isoformat(),
                open_=minute_open,
                high=minute_high,
                low=minute_low,
                close=minute_close,
                volume=volume // 5,
            )
        )
    return rows


def _one_minute_bar(
    *,
    end_ts: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    symbol: str = "ES",
) -> tuple[object, ...]:
    end_dt = datetime.fromisoformat(end_ts)
    start_dt = end_dt - timedelta(minutes=1)
    return (
        symbol,
        "1m",
        "historical_1m_canonical",
        start_dt.isoformat(),
        end_dt.isoformat(),
        open_,
        high,
        low,
        close,
        volume,
    )


def test_probationary_operator_control_cli_routes_shared_strategy_identity(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_submit(config_paths, action: str, *, payload=None, shared_strategy_identity=None):
        captured["config_paths"] = list(config_paths)
        captured["action"] = action
        captured["payload"] = payload
        captured["shared_strategy_identity"] = shared_strategy_identity
        return {
            "action": action,
            "control_path": "/tmp/operator_control.json",
            "status": "pending",
            "requested_at": "2026-04-01T00:00:00+00:00",
        }

    monkeypatch.setattr("mgc_v05l.app.main.submit_probationary_operator_control", _fake_submit)

    exit_code = main(
        [
            "probationary-operator-control",
            "--action",
            "resume_entries",
            "--shared-strategy-identity",
            "ATP_COMPANION_V1_ASIA_US",
            "--payload-json",
            '{"source":"cli-test"}',
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "action": "resume_entries",
        "payload": {"source": "cli-test"},
        "shared_strategy_identity": "ATP_COMPANION_V1_ASIA_US",
    }
    assert payload["action"] == "resume_entries"
    assert payload["status"] == "pending"


def test_probationary_market_data_probe_cli_routes_shared_runtime_probe(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_probe(config_paths, schwab_config_path):
        captured["config_paths"] = list(config_paths)
        captured["schwab_config_path"] = schwab_config_path
        return {"status": "ok", "runtime_ready": True, "artifact_path": "/tmp/probe.json"}

    monkeypatch.setattr("mgc_v05l.app.main.run_probationary_market_data_transport_probe", _fake_probe)

    exit_code = main(["probationary-market-data-probe", "--schwab-config", "config/custom.json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "config_paths": [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ],
        "schwab_config_path": "config/custom.json",
    }
    assert payload["runtime_ready"] is True


def test_probationary_paper_soak_cli_returns_structured_transport_failure(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "mgc_v05l.app.main.build_probationary_paper_runner",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProbationaryRuntimeTransportFailure(
                {
                    "blocker_label": "market_data_transport_failure",
                    "target_host": "api.schwabapi.com",
                    "rendered_url": "https://api.schwabapi.com/marketdata/v1/pricehistory",
                    "exception_text": "dns failed",
                    "runtime_ready": False,
                }
            )
        ),
    )

    exit_code = main(["probationary-paper-soak"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["blocker_label"] == "market_data_transport_failure"
    assert payload["runtime_ready"] is False


def test_schwab_auth_gate_cli_routes_shared_probe(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def check_runtime_ready(self) -> dict[str, object]:
            return {
                "runtime_ready": True,
                "probe_symbol": captured["probe_symbol"],
                "schwab_config_path": str(captured["schwab_config_path"]),
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-auth-gate",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--internal-symbol",
            "MGC",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
    }
    assert payload["runtime_ready"] is True
    assert payload["probe_symbol"] == "MGC"


def test_schwab_token_web_cli_routes_shared_bootstrap_server(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def _fake_run_server(
        *,
        host: str,
        port: int,
        token_file=None,
        open_browser: bool = True,
        info_file=None,
        port_search_limit: int = 25,
        schwab_config_path=None,
        probe_symbol: str = "MGC",
    ) -> dict[str, object]:
        captured.update(
            {
                "host": host,
                "port": port,
                "token_file": token_file,
                "open_browser": open_browser,
                "info_file": info_file,
                "port_search_limit": port_search_limit,
                "schwab_config_path": schwab_config_path,
                "probe_symbol": probe_symbol,
            }
        )
        return {"url": f"http://{host}:{port}/", "port": port}

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "run_schwab_token_bootstrap_server", _fake_run_server)

    exit_code = main(
        [
            "schwab-token-web",
            "--host",
            "127.0.0.1",
            "--port",
            "8765",
            "--token-file",
            "/tmp/test-tokens.json",
            "--info-file",
            "/tmp/bootstrap-info.json",
            "--port-search-limit",
            "7",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
            "--no-browser",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "host": "127.0.0.1",
        "port": 8765,
        "token_file": "/tmp/test-tokens.json",
        "open_browser": False,
        "info_file": "/tmp/bootstrap-info.json",
        "port_search_limit": 7,
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
    }
    assert payload["url"] == "http://127.0.0.1:8765/"
    assert payload["port"] == 8765


def test_schwab_debug_exchange_refresh_cli_routes_shared_backend_harness(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def debug_exchange_refresh(self, code: str) -> dict[str, object]:
            captured["code"] = code
            return {
                "final_state": "RUNTIME_READY",
                "runtime_ready": True,
                "exchange_diagnostic_path": "/tmp/latest_exchange_result.json",
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-debug-exchange-refresh",
            "--code",
            "abc123",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
        "code": "abc123",
    }
    assert payload["final_state"] == "RUNTIME_READY"


def test_schwab_local_authorize_proof_cli_routes_shared_backend_harness(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    class _FakeService:
        def __init__(self, *, token_file=None, schwab_config_path=None, probe_symbol="MGC", **_kwargs) -> None:
            captured["token_file"] = token_file
            captured["schwab_config_path"] = schwab_config_path
            captured["probe_symbol"] = probe_symbol

        def local_authorize_proof(self, *, state: str, scope=None, timeout_seconds: int = 180) -> dict[str, object]:
            captured["state"] = state
            captured["scope"] = scope
            captured["timeout_seconds"] = timeout_seconds
            return {
                "final_state": "RUNTIME_READY",
                "runtime_ready": True,
                "exchange_diagnostic_path": "/tmp/latest_exchange_result.json",
                "refresh_result_artifact_path": "/tmp/latest_refresh_result.json",
            }

    monkeypatch.setattr(schwab_token_bootstrap_web_module, "SchwabTokenBootstrapService", _FakeService)

    exit_code = main(
        [
            "schwab-local-authorize-proof",
            "--token-file",
            "/tmp/test-tokens.json",
            "--schwab-config",
            "config/schwab.local.json",
            "--probe-symbol",
            "MGC",
            "--state",
            "abc-state",
            "--timeout-seconds",
            "240",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured == {
        "token_file": "/tmp/test-tokens.json",
        "schwab_config_path": "config/schwab.local.json",
        "probe_symbol": "MGC",
        "state": "abc-state",
        "scope": None,
        "timeout_seconds": 240,
    }
    assert payload["final_state"] == "RUNTIME_READY"
