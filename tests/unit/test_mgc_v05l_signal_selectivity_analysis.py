from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from mgc_v05l.app.signal_selectivity_analysis import (
    SignalSelectivityDataset,
    build_signal_selectivity_analysis,
    summarize_signal_selectivity_dataset,
)
from mgc_v05l.config_models import load_settings_from_files


_SCHEMA = """
create table bars (
  bar_id text primary key,
  data_source text,
  ticker text,
  symbol text,
  timeframe text,
  timestamp text,
  start_ts text,
  end_ts text,
  open text,
  high text,
  low text,
  close text,
  volume integer,
  is_final integer,
  session_asia integer,
  session_london integer,
  session_us integer,
  session_allowed integer,
  created_at text
);
create table signals (
  bar_id text primary key,
  payload_json text not null,
  created_at text not null
);
"""


def _write_dataset(path: Path, rows: list[dict[str, object]]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_SCHEMA)
        for row in rows:
            connection.execute(
                "insert into bars values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["bar_id"],
                    "internal",
                    "MGC",
                    "MGC",
                    "5m",
                    row["end_ts"],
                    row["start_ts"],
                    row["end_ts"],
                    row.get("open", "100"),
                    row.get("high", "101"),
                    row.get("low", "99"),
                    row.get("close", "100"),
                    100,
                    1,
                    row.get("session_asia", 0),
                    row.get("session_london", 0),
                    row.get("session_us", 0),
                    row.get("session_allowed", 1),
                    row["end_ts"],
                ),
            )
            connection.execute(
                "insert into signals values (?, ?, ?)",
                (
                    row["bar_id"],
                    json.dumps(row["payload"], sort_keys=True),
                    row["end_ts"],
                ),
            )
        connection.commit()
    finally:
        connection.close()


def _settings():
    return load_settings_from_files([Path("config/base.yaml")])


def test_signal_selectivity_dataset_summary_counts_rates_sessions_and_anti_churn(tmp_path: Path) -> None:
    db_path = tmp_path / "fixture.sqlite3"
    _write_dataset(
        db_path,
        [
            {
                "bar_id": "MGC|5m|2026-03-27T00:05:00-04:00",
                "start_ts": "2026-03-27T00:00:00-04:00",
                "end_ts": "2026-03-27T00:05:00-04:00",
                "session_asia": 1,
                "payload": {
                    "bar_id": "MGC|5m|2026-03-27T00:05:00-04:00",
                    "asia_reclaim_bar_raw": True,
                    "below_vwap_recently": True,
                    "reclaim_range_ok": True,
                    "reclaim_vol_ok": True,
                    "reclaim_color_ok": False,
                    "reclaim_close_ok": True,
                    "asia_hold_bar_ok": False,
                    "asia_acceptance_bar_ok": False,
                    "asia_vwap_long_signal": False,
                    "long_entry_raw": False,
                    "long_entry": False,
                    "recent_long_setup": False,
                },
            },
            {
                "bar_id": "MGC|5m|2026-03-27T03:05:00-04:00",
                "start_ts": "2026-03-27T03:00:00-04:00",
                "end_ts": "2026-03-27T03:05:00-04:00",
                "session_london": 1,
                "payload": {
                    "bar_id": "MGC|5m|2026-03-27T03:05:00-04:00",
                    "bear_snap_turn_candidate": True,
                    "first_bear_snap_turn": True,
                    "short_entry_raw": True,
                    "short_entry": True,
                    "short_entry_source": "firstBearSnapTurn",
                    "bear_snap_up_stretch_ok": True,
                    "bear_snap_range_ok": True,
                    "bear_snap_body_ok": True,
                    "bear_snap_close_weak": True,
                    "bear_snap_velocity_ok": True,
                    "bear_snap_reversal_bar": True,
                    "bear_snap_location_ok": True,
                    "recent_short_setup": False,
                },
            },
            {
                "bar_id": "MGC|5m|2026-03-27T10:05:00-04:00",
                "start_ts": "2026-03-27T10:00:00-04:00",
                "end_ts": "2026-03-27T10:05:00-04:00",
                "session_us": 1,
                "payload": {
                    "bar_id": "MGC|5m|2026-03-27T10:05:00-04:00",
                    "bull_snap_turn_candidate": True,
                    "first_bull_snap_turn": True,
                    "long_entry_raw": True,
                    "long_entry": False,
                    "long_entry_source": None,
                    "recent_long_setup": True,
                    "bull_snap_downside_stretch_ok": True,
                    "bull_snap_range_ok": True,
                    "bull_snap_body_ok": True,
                    "bull_snap_close_strong": True,
                    "bull_snap_velocity_ok": True,
                    "bull_snap_reversal_bar": True,
                    "bull_snap_location_ok": True,
                },
            },
            {
                "bar_id": "MGC|5m|2026-03-27T10:10:00-04:00",
                "start_ts": "2026-03-27T10:05:00-04:00",
                "end_ts": "2026-03-27T10:10:00-04:00",
                "session_us": 1,
                "payload": {
                    "bar_id": "MGC|5m|2026-03-27T10:10:00-04:00",
                    "bear_snap_turn_candidate": True,
                    "first_bear_snap_turn": False,
                    "short_entry_raw": False,
                    "short_entry": False,
                    "recent_short_setup": True,
                    "bear_snap_up_stretch_ok": True,
                    "bear_snap_range_ok": True,
                    "bear_snap_body_ok": True,
                    "bear_snap_close_weak": True,
                    "bear_snap_velocity_ok": True,
                    "bear_snap_reversal_bar": True,
                    "bear_snap_location_ok": True,
                },
            },
        ],
    )

    summary = summarize_signal_selectivity_dataset(
        SignalSelectivityDataset(
            dataset_id="fixture",
            label="Fixture",
            dataset_kind="live",
            database_path=db_path,
        ),
        settings=_settings(),
    )

    assert summary["processed_bars"] == 4
    assert summary["funnel"]["counts"]["asia_reclaim_bar_raw"] == 1
    assert summary["funnel"]["counts"]["firstBearSnapTurn"] == 1
    assert summary["funnel"]["counts"]["longEntryRaw"] == 1
    assert summary["funnel"]["counts"]["shortEntry"] == 1
    assert summary["funnel"]["entries_per_100_bars"]["short"] == 25.0
    assert summary["session_breakdown"]["ASIA"]["completed_bars"] == 1
    assert summary["session_breakdown"]["LONDON"]["funnel_counts"]["shortEntry"] == 1
    assert summary["session_breakdown"]["US"]["funnel_counts"]["longEntryRaw"] == 1
    assert summary["anti_churn"]["suppression_by_family"]["bullSnapLong"]["suppressed_count"] == 1
    assert summary["anti_churn"]["suppression_by_family"]["bullSnapLong"]["suppression_rate_pct"] == 100.0
    assert summary["family_failures"]["asiaVWAPLong"]["primary_blockers"][0]["predicate"] == "below-vwap recent"
    assert any(
        item["predicate"] == "cooldown" and item["count"] == 1
        for item in summary["family_failures"]["bearSnapShort"]["predicate_failure_counts"]
    )


def test_signal_selectivity_before_after_comparison_detects_bear_snap_location_change(tmp_path: Path) -> None:
    before_db = tmp_path / "before.sqlite3"
    after_db = tmp_path / "after.sqlite3"
    shared_rows = [
        {
            "bar_id": "MGC|5m|2026-03-25T10:05:00-04:00",
            "start_ts": "2026-03-25T10:00:00-04:00",
            "end_ts": "2026-03-25T10:05:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:05:00-04:00",
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": False,
                "short_entry_source": None,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-25T10:10:00-04:00",
            "start_ts": "2026-03-25T10:05:00-04:00",
            "end_ts": "2026-03-25T10:10:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:10:00-04:00",
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": False,
                "short_entry_source": None,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
            },
        },
    ]
    _write_dataset(
        before_db,
        [{**row, "payload": {**row["payload"], "bear_snap_location_ok": False}} for row in shared_rows],
    )
    _write_dataset(
        after_db,
        [
            {
                **shared_rows[0],
                "payload": {
                    **shared_rows[0]["payload"],
                    "bear_snap_location_ok": True,
                    "short_entry": True,
                    "short_entry_source": "firstBearSnapTurn",
                },
            },
            {
                **shared_rows[1],
                "payload": {
                    **shared_rows[1]["payload"],
                    "bear_snap_location_ok": True,
                    "short_entry": False,
                    "short_entry_source": None,
                },
            },
        ],
    )

    analysis = build_signal_selectivity_analysis(
        settings=_settings(),
        repo_root=tmp_path,
        dataset_specs=[
            SignalSelectivityDataset("before", "Before", "replay", before_db, comparison_role="before"),
            SignalSelectivityDataset("after", "After", "replay", after_db, comparison_role="after"),
        ],
    )

    before_after = analysis["before_after_bear_snap_location"]
    assert before_after["available"] is True
    assert before_after["counts"]["shortEntryRaw"] == {"before": 2, "after": 2}
    assert before_after["counts"]["shortEntry"] == {"before": 0, "after": 1}
    assert before_after["location_primary_block_before"] == 2
    assert before_after["location_primary_block_after"] == 0
    assert before_after["materially_improved_short_opportunity_rate"] is True


def test_signal_selectivity_bear_snap_up_stretch_ladder_recommends_smallest_meaningful_reduction(tmp_path: Path) -> None:
    replay_dir = tmp_path / "outputs" / "signal_selectivity_analysis" / "replays"
    replay_dir.mkdir(parents=True)

    baseline_rows = [
        {
            "bar_id": "MGC|5m|2026-03-25T10:05:00-04:00",
            "start_ts": "2026-03-25T10:00:00-04:00",
            "end_ts": "2026-03-25T10:05:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:05:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": False,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-25T10:10:00-04:00",
            "start_ts": "2026-03-25T10:05:00-04:00",
            "end_ts": "2026-03-25T10:10:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:10:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": False,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-25T10:15:00-04:00",
            "start_ts": "2026-03-25T10:10:00-04:00",
            "end_ts": "2026-03-25T10:15:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:15:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": False,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": False,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-25T10:20:00-04:00",
            "start_ts": "2026-03-25T10:15:00-04:00",
            "end_ts": "2026-03-25T10:20:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-25T10:20:00-04:00",
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
    ]
    current_090_rows = [
        {
            **baseline_rows[0],
            "payload": {
                **baseline_rows[0]["payload"],
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "bear_snap_up_stretch_ok": True,
            },
        },
        *baseline_rows[1:],
    ]
    current_080_rows = current_090_rows
    current_070_rows = [
        *current_090_rows[:1],
        {
            **baseline_rows[1],
            "payload": {
                **baseline_rows[1]["payload"],
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "bear_snap_up_stretch_ok": True,
            },
        },
        *baseline_rows[2:],
    ]

    datasets = {
        "1_00": baseline_rows,
        "0_90": current_090_rows,
        "0_80": current_080_rows,
        "0_70": current_070_rows,
    }
    for value, rows in datasets.items():
        _write_dataset(replay_dir / f"historical_playback_mgc_bear_snap_up_stretch_{value}.sqlite3", rows)

    analysis = build_signal_selectivity_analysis(
        settings=_settings(),
        repo_root=tmp_path,
        dataset_specs=[],
    )

    ladder = analysis["bear_snap_up_stretch_ladder"]
    assert ladder["available"] is True
    assert ladder["candidate_values_tested"] == ["1.00", "0.90", "0.80", "0.70"]
    assert ladder["recommended_value"] == "0.90"
    assert ladder["range_becomes_next_dominant_blocker"] is True
    candidate_rows = {row["value"]: row for row in ladder["candidate_rows"]}
    assert candidate_rows["1.00"]["counts"]["shortEntry"] == 1
    assert candidate_rows["0.90"]["counts"]["shortEntry"] == 2
    assert candidate_rows["0.80"]["counts"]["shortEntry"] == 2
    assert candidate_rows["0.70"]["counts"]["shortEntry"] == 3
    assert candidate_rows["0.90"]["top_primary_predicate"] == "range"
    assert candidate_rows["1.00"]["top_primary_predicate"] == "upside stretch"


def test_signal_selectivity_bear_snap_range_ladder_recommends_smallest_meaningful_reduction(tmp_path: Path) -> None:
    replay_dir = tmp_path / "outputs" / "signal_selectivity_analysis" / "replays"
    replay_dir.mkdir(parents=True)

    base_rows = [
        {
            "bar_id": "MGC|5m|2026-03-26T10:05:00-04:00",
            "start_ts": "2026-03-26T10:00:00-04:00",
            "end_ts": "2026-03-26T10:05:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-26T10:05:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": False,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": False,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-26T10:10:00-04:00",
            "start_ts": "2026-03-26T10:05:00-04:00",
            "end_ts": "2026-03-26T10:10:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-26T10:10:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": False,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": False,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-26T10:15:00-04:00",
            "start_ts": "2026-03-26T10:10:00-04:00",
            "end_ts": "2026-03-26T10:15:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-26T10:15:00-04:00",
                "bear_snap_turn_candidate": False,
                "first_bear_snap_turn": False,
                "short_entry_raw": False,
                "short_entry": False,
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": False,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
        {
            "bar_id": "MGC|5m|2026-03-26T10:20:00-04:00",
            "start_ts": "2026-03-26T10:15:00-04:00",
            "end_ts": "2026-03-26T10:20:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-26T10:20:00-04:00",
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
    ]
    rows_080 = [
        {
            **base_rows[0],
            "payload": {
                **base_rows[0]["payload"],
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "bear_snap_range_ok": True,
                "bear_snap_reversal_bar": True,
            },
        },
        {
            **base_rows[1],
            "payload": {
                **base_rows[1]["payload"],
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "bear_snap_range_ok": True,
                "bear_snap_reversal_bar": True,
            },
        },
        *base_rows[2:],
    ]
    rows_070 = [
        *rows_080[:2],
        {
            **base_rows[2],
            "payload": {
                **base_rows[2]["payload"],
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "bear_snap_up_stretch_ok": True,
            },
        },
        base_rows[3],
    ]
    rows_060 = [
        *rows_070,
        {
            "bar_id": "MGC|5m|2026-03-26T10:25:00-04:00",
            "start_ts": "2026-03-26T10:20:00-04:00",
            "end_ts": "2026-03-26T10:25:00-04:00",
            "session_us": 1,
            "payload": {
                "bar_id": "MGC|5m|2026-03-26T10:25:00-04:00",
                "bear_snap_turn_candidate": True,
                "first_bear_snap_turn": True,
                "short_entry_raw": True,
                "short_entry": True,
                "short_entry_source": "firstBearSnapTurn",
                "recent_short_setup": False,
                "bear_snap_up_stretch_ok": True,
                "bear_snap_range_ok": True,
                "bear_snap_body_ok": True,
                "bear_snap_close_weak": True,
                "bear_snap_velocity_ok": True,
                "bear_snap_reversal_bar": True,
                "bear_snap_location_ok": True,
            },
        },
    ]

    datasets = {
        "0_90": base_rows,
        "0_80": rows_080,
        "0_70": rows_070,
        "0_60": rows_060,
    }
    for value, rows in datasets.items():
        _write_dataset(replay_dir / f"historical_playback_mgc_bear_snap_range_{value}.sqlite3", rows)

    analysis = build_signal_selectivity_analysis(
        settings=_settings(),
        repo_root=tmp_path,
        dataset_specs=[],
    )

    ladder = analysis["bear_snap_range_ladder"]
    assert ladder["available"] is True
    assert ladder["candidate_values_tested"] == ["0.90", "0.80", "0.70", "0.60"]
    assert ladder["recommended_value"] == "0.80"
    assert ladder["next_dominant_blocker_after_recommended"] == "upside stretch"
    candidate_rows = {row["value"]: row for row in ladder["candidate_rows"]}
    assert candidate_rows["0.90"]["counts"]["shortEntry"] == 1
    assert candidate_rows["0.80"]["counts"]["shortEntry"] == 3
    assert candidate_rows["0.70"]["counts"]["shortEntry"] == 4
    assert candidate_rows["0.60"]["counts"]["shortEntry"] == 5
    assert candidate_rows["0.90"]["top_primary_predicate"] == "range"
    assert candidate_rows["0.80"]["top_primary_predicate"] == "upside stretch"
