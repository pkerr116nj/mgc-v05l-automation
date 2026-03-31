from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.atpe_live_observation import run_atpe_live_observation


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def test_build_atpe_live_observation_report_summarizes_live_temp_paper_activity(tmp_path: Path) -> None:
    lanes_root = tmp_path / "lanes"
    long_lane = lanes_root / "atpe_long_medium_high_canary__MES"
    short_lane = lanes_root / "atpe_short_high_only_canary__MES"

    _write_json(
        long_lane / "operator_status.json",
        {
            "lane_id": "atpe_long_medium_high_canary__MES",
            "lane_name": "ATPE Long Medium+High Canary / MES",
            "side": "LONG",
            "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
            "last_processed_bar_end_ts": "2026-03-24T10:35:00-04:00",
            "generated_at": "2026-03-24T10:35:05-04:00",
        },
    )
    _write_jsonl(
        long_lane / "signals.jsonl",
        [
            {"signal_timestamp": "2026-03-24T10:05:00-04:00"},
            {"signal_timestamp": "2026-03-24T10:10:00-04:00"},
        ],
    )
    _write_jsonl(
        long_lane / "order_intents.jsonl",
        [
            {"created_at": "2026-03-24T10:06:00-04:00"},
            {"created_at": "2026-03-24T10:14:00-04:00"},
        ],
    )
    _write_jsonl(
        long_lane / "fills.jsonl",
        [
            {"fill_timestamp": "2026-03-24T10:06:00-04:00"},
            {"fill_timestamp": "2026-03-24T10:14:00-04:00"},
        ],
    )
    _write_jsonl(
        long_lane / "trades.jsonl",
        [
            {
                "trade_id": "long-1",
                "direction": "LONG",
                "entry_timestamp": "2026-03-24T10:06:00-04:00",
                "exit_timestamp": "2026-03-24T10:14:00-04:00",
                "exit_reason": "atpe_time_stop",
                "realized_pnl": "75.0",
            }
        ],
    )
    _write_jsonl(long_lane / "events.jsonl", [{"timestamp": "2026-03-24T10:14:00-04:00"}])

    _write_json(
        short_lane / "operator_status.json",
        {
            "lane_id": "atpe_short_high_only_canary__MES",
            "lane_name": "ATPE Short High-Only Canary / MES",
            "side": "SHORT",
            "quality_bucket_policy": "HIGH_ONLY",
            "last_processed_bar_end_ts": "2026-03-24T10:35:00-04:00",
            "generated_at": "2026-03-24T10:35:07-04:00",
        },
    )
    _write_jsonl(short_lane / "signals.jsonl", [{"signal_timestamp": "2026-03-24T10:20:00-04:00"}])
    _write_jsonl(short_lane / "order_intents.jsonl", [{"created_at": "2026-03-24T10:21:00-04:00"}])
    _write_jsonl(short_lane / "fills.jsonl", [{"fill_timestamp": "2026-03-24T10:21:00-04:00"}])
    _write_jsonl(
        short_lane / "trades.jsonl",
        [
            {
                "trade_id": "short-1",
                "direction": "SHORT",
                "entry_timestamp": "2026-03-24T10:21:00-04:00",
                "exit_timestamp": "2026-03-24T10:25:00-04:00",
                "exit_reason": "atpe_target",
                "realized_pnl": "25.0",
            }
        ],
    )
    _write_jsonl(short_lane / "events.jsonl", [{"timestamp": "2026-03-24T10:25:00-04:00"}])

    _write_json(
        tmp_path / "paper_session" / "operator_status.json",
        {
            "strategy_status": "RUNNING_MULTI_LANE",
            "updated_at": "2026-03-24T14:35:10+00:00",
            "last_processed_bar_end_ts": "2026-03-24T10:35:00-04:00",
            "paper_lane_count": 10,
        },
    )
    _write_json(
        tmp_path / "paper_strategy_performance_snapshot.json",
        {
            "rows": [
                {
                    "lane_id": "atpe_long_medium_high_canary__MES",
                    "latest_activity_timestamp": "2026-03-24T10:14:00-04:00",
                    "realized_pnl": 75.0,
                    "trade_count": 1,
                },
                {
                    "lane_id": "atpe_short_high_only_canary__MES",
                    "latest_activity_timestamp": "2026-03-24T10:25:00-04:00",
                    "realized_pnl": 25.0,
                    "trade_count": 1,
                },
            ]
        },
    )
    _write_json(
        tmp_path / "paper_strategy_trade_log_snapshot.json",
        {
            "rows": [
                {"lane_id": "atpe_long_medium_high_canary__MES", "trade_id": "long-1"},
                {"lane_id": "atpe_short_high_only_canary__MES", "trade_id": "short-1"},
            ]
        },
    )
    _write_json(
        tmp_path / "paper_temporary_paper_runtime_integrity_snapshot.json",
        {
            "enabled_in_app_count": 2,
            "loaded_in_runtime_count": 2,
            "snapshot_only_count": 0,
            "mismatch_status": "matched",
        },
    )
    _write_json(
        tmp_path / "gc_mgc_london_open_acceptance_live_observation.json",
        {
            "status": "observe_only_keep_definition_unchanged",
            "sample_status": {"label": "runtime_valid_but_no_live_entries_yet"},
            "late_entry_review": {"assessment": "insufficient_live_entry_sample"},
            "recommendation": {"current_action": "keep_branch_unchanged_and_observe_more_live_sessions"},
        },
    )

    artifacts = run_atpe_live_observation(
        output_dir=tmp_path / "out",
        lanes_root_path=lanes_root,
        paper_session_status_path=tmp_path / "paper_session" / "operator_status.json",
        strategy_performance_snapshot_path=tmp_path / "paper_strategy_performance_snapshot.json",
        trade_log_snapshot_path=tmp_path / "paper_strategy_trade_log_snapshot.json",
        temp_paper_integrity_snapshot_path=tmp_path / "paper_temporary_paper_runtime_integrity_snapshot.json",
        gc_mgc_observation_path=tmp_path / "gc_mgc_london_open_acceptance_live_observation.json",
    )
    report = json.loads(artifacts.json_path.read_text(encoding="utf-8"))

    assert report["live_runtime_summary"]["paper_session_strategy_status"] == "RUNNING_MULTI_LANE"
    assert report["live_runtime_summary"]["overall_fill_reliability"] == 1.0
    assert report["trade_quality_summary"]["overall"]["trade_count"] == 2
    assert report["trade_quality_summary"]["overall"]["realized_pnl"] == 100.0
    assert report["trade_quality_summary"]["long"]["realized_pnl"] == 75.0
    assert report["trade_quality_summary"]["short"]["realized_pnl"] == 25.0
    assert report["per_lane"][0]["capture_consistency"]["strategy_performance_row_present"] is True
    assert any(check["name"] == "Runtime Healthy" for check in report["promotion_readiness_checklist"]) is True
