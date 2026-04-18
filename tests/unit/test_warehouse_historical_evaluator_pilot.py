from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from mgc_v05l.research.warehouse_historical_evaluator import run_thin_warehouse_slice
from mgc_v05l.research.warehouse_historical_evaluator.warehouse_evaluator import (
    _compact_row_from_warehouse_rows,
    _closed_trade_pnl_points,
    _contract_point_value,
    _is_structural_boundary,
    _resolve_integrity_fail_exit_price,
    _select_entry_timing_row,
    _select_exit_timing_row,
    materialize_lane_candidates_partition,
    resolve_lane_definition,
)


def test_run_thin_warehouse_slice_materializes_real_symbol_shard(tmp_path: Path) -> None:
    root = tmp_path / "warehouse_eval"
    payload = run_thin_warehouse_slice(
        root_dir=root,
        sqlite_path=Path("mgc_v05l.replay.sqlite3"),
        symbol="MGC",
        lane_ids=[
            "mgc_us_late_pause_resume_long_turn__MGC",
            "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
            "mgc_asia_early_pause_resume_short_turn__MGC",
        ],
        shard_id="2024Q1",
        start_ts=datetime.fromisoformat("2024-01-01T00:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-03-31T23:59:00-04:00"),
    )

    assert Path(payload["raw_partition_path"]).exists()
    assert Path(payload["derived_5m_path"]).exists()
    assert Path(payload["derived_10m_path"]).exists()
    assert Path(payload["shared_features_5m_path"]).exists()
    assert Path(payload["shared_features_1m_timing_path"]).exists()
    assert Path(payload["family_event_tables_path"]).exists()
    assert Path(payload["lane_candidates_path"]).exists()
    assert Path(payload["lane_entries_path"]).exists()
    assert Path(payload["lane_closed_trades_path"]).exists()
    assert len(payload["compact_partition_paths"]) == 1
    for compact_path in payload["compact_partition_paths"]:
        assert Path(compact_path).exists()
    assert Path(payload["proof_path"]).exists()

    connection = duckdb.connect(str(root / "catalogs" / "warehouse_historical_evaluator.duckdb"))
    try:
        raw_count = connection.execute("select count(*) from raw_bars_1m").fetchone()[0]
        derived_5m_count = connection.execute("select count(*) from derived_bars_5m").fetchone()[0]
        derived_10m_count = connection.execute("select count(*) from derived_bars_10m").fetchone()[0]
        shared_5m_count = connection.execute("select count(*) from shared_features_5m").fetchone()[0]
        shared_1m_timing_count = connection.execute("select count(*) from shared_features_1m_timing").fetchone()[0]
        family_event_count = connection.execute("select count(*) from family_event_tables").fetchone()[0]
        lane_candidate_count = connection.execute("select count(*) from lane_candidates").fetchone()[0]
        lane_entry_count = connection.execute("select count(*) from lane_entries").fetchone()[0]
        lane_closed_trade_count = connection.execute("select count(*) from lane_closed_trades").fetchone()[0]
        compact_count = connection.execute("select count(*) from lane_compact_results").fetchone()[0]
        partition_count = connection.execute("select count(*) from dataset_partitions").fetchone()[0]
        family_counts = dict(
            connection.execute("select family, count(*) from family_event_tables group by family").fetchall()
        )
        compact_rows = connection.execute(
            """
            select lane_id, execution_model, result_classification
            from lane_compact_results
            order by lane_id
            """
        ).fetchall()
    finally:
        connection.close()

    assert raw_count > 0
    assert derived_5m_count > 0
    assert derived_10m_count > 0
    assert shared_5m_count > 0
    assert shared_1m_timing_count > 0
    assert family_event_count > 0
    assert lane_candidate_count > 0
    assert lane_entry_count > 0
    assert lane_closed_trade_count > 0
    assert compact_count == 3
    assert partition_count == 10
    assert {
        "asiaEarlyNormalBreakoutRetestHoldTurn",
        "asiaEarlyPauseResumeShortTurn",
        "usLatePauseResumeLongTurn",
    }.issubset(set(family_counts))
    assert all(count > 0 for count in family_counts.values())
    assert compact_rows == [
        ("mgc_asia_early_normal_breakout_retest_hold_turn__MGC", "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP", "nonzero_trade"),
        ("mgc_asia_early_pause_resume_short_turn__MGC", "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP", "nonzero_trade"),
        ("mgc_us_late_pause_resume_long_turn__MGC", "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP", "nonzero_trade"),
    ]


def test_select_entry_timing_row_scans_forward_for_first_allowed_bar() -> None:
    decision_ts = datetime.fromisoformat("2024-01-08T19:25:00+00:00")
    timing_rows = [
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
            "long_close_quality": "VWAP_CHASE_RISK",
            "long_neutral_tight_ok": False,
            "short_close_quality": "VWAP_FAVORABLE",
            "short_neutral_tight_ok": True,
        },
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:26:00+00:00"),
            "long_close_quality": "VWAP_FAVORABLE",
            "long_neutral_tight_ok": True,
            "short_close_quality": "VWAP_CHASE_RISK",
            "short_neutral_tight_ok": False,
        },
    ]

    selected = _select_entry_timing_row(
        side="LONG",
        decision_ts=decision_ts,
        explicit_timing_ts=None,
        timing_rows=timing_rows,
        timing_ts_values=[row["timing_ts"] for row in timing_rows],
    )

    assert selected is not None
    assert selected["timing_ts"] == datetime.fromisoformat("2024-01-08T19:26:00+00:00")


def test_select_exit_timing_row_prefers_first_integrity_fail_before_hold_limit() -> None:
    timing_rows = [
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
            "long_close_quality": "VWAP_FAVORABLE",
            "long_neutral_tight_ok": True,
            "short_close_quality": "VWAP_FAVORABLE",
            "short_neutral_tight_ok": True,
            "high_price": 100.5,
            "low_price": 99.8,
            "close_price": 100.1,
        },
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:26:00+00:00"),
            "long_close_quality": "VWAP_FAVORABLE",
            "long_neutral_tight_ok": True,
            "short_close_quality": "VWAP_FAVORABLE",
            "short_neutral_tight_ok": True,
            "high_price": 100.7,
            "low_price": 99.9,
            "close_price": 100.2,
        },
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:27:00+00:00"),
            "long_close_quality": "VWAP_CHASE_RISK",
            "long_neutral_tight_ok": False,
            "short_close_quality": "VWAP_FAVORABLE",
            "short_neutral_tight_ok": True,
            "high_price": 100.4,
            "low_price": 99.2,
            "close_price": 99.6,
        },
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:40:00+00:00"),
            "long_close_quality": "VWAP_FAVORABLE",
            "long_neutral_tight_ok": True,
            "short_close_quality": "VWAP_FAVORABLE",
            "short_neutral_tight_ok": True,
            "high_price": 100.9,
            "low_price": 99.4,
            "close_price": 100.3,
        },
    ]

    exit_row, exit_reason, exit_price = _select_exit_timing_row(
        side="LONG",
        entry_ts=datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
        hold_minutes=15,
        timing_rows=timing_rows,
        timing_ts_index=[row["timing_ts"] for row in timing_rows],
    )

    assert exit_row is not None
    assert exit_row["timing_ts"] == datetime.fromisoformat("2024-01-08T19:40:00+00:00")
    assert exit_reason == "LONG_INTEGRITY_FAIL"
    assert _is_structural_boundary(exit_row["timing_ts"])
    assert exit_price == 99.2


def test_closed_trade_pnl_points_scales_to_cash_with_contract_point_value() -> None:
    pnl_points = _closed_trade_pnl_points(side="LONG", entry_price=2184.1, exit_price=2183.5)
    assert round(pnl_points, 6) == -0.6
    assert _contract_point_value("MGC") == 10.0
    assert round(pnl_points * _contract_point_value("MGC"), 6) == -6.0


def test_resolve_integrity_fail_exit_price_uses_adverse_intrabar_extreme() -> None:
    timing_rows = [
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:27:00+00:00"),
            "high_price": 100.4,
            "low_price": 99.2,
            "close_price": 99.6,
        },
        {
            "timing_ts": datetime.fromisoformat("2024-01-08T19:30:00+00:00"),
            "high_price": 100.1,
            "low_price": 98.9,
            "close_price": 99.4,
        },
    ]
    ts_index = [row["timing_ts"] for row in timing_rows]
    assert _resolve_integrity_fail_exit_price(
        side="LONG",
        failed_timing_ts=datetime.fromisoformat("2024-01-08T19:27:00+00:00"),
        boundary_timing_ts=datetime.fromisoformat("2024-01-08T19:30:00+00:00"),
        timing_rows=timing_rows,
        timing_ts_index=ts_index,
    ) == 98.9


def test_materialize_lane_candidates_partition_preserves_feature_only_candidate_rows(tmp_path: Path) -> None:
    root = tmp_path / "warehouse_eval"
    family_event_path = root / "family_event_tables.parquet"
    shared_features_path = root / "shared_features_5m.parquet"
    root.mkdir(parents=True, exist_ok=True)

    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "event_id": "MGC:2024Q1:bullSnapLongBase:2024-01-08T19:25:00+00:00",
                    "symbol": "MGC",
                    "family": "bullSnapLongBase",
                    "shard_id": "2024Q1",
                    "candidate_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "event_side": "LONG",
                    "event_phase": "US_LATE",
                    "eligibility_label": "candidate_flag_true",
                    "blocker_label": None,
                    "execution_model": "PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP",
                    "decision_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "feature_bar_id": "MGC:5m:2024-01-08T19:25:00+00:00",
                    "timing_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "feature_refs": "{}",
                    "materialized_ts": datetime.fromisoformat("2026-04-05T00:00:00+00:00"),
                    "provenance_tag": "family_event_tables:test",
                }
            ]
        ),
        family_event_path,
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "symbol": "MGC",
                    "shard_id": "2024Q1",
                    "decision_ts": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "bar_id": "MGC:5m:2024-01-08T19:25:00+00:00",
                    "timeframe": "5m",
                    "session_phase": "US_LATE",
                    "atr": 1.0,
                    "bar_range": 1.0,
                    "body_size": 1.0,
                    "vol_ratio": 1.0,
                    "turn_ema_fast": 1.0,
                    "turn_ema_slow": 1.0,
                    "velocity": 1.0,
                    "velocity_delta": 1.0,
                    "vwap": 1.0,
                    "vwap_buffer": 1.0,
                    "downside_stretch": 1.0,
                    "upside_stretch": 1.0,
                    "bull_close_strong": True,
                    "bear_close_weak": False,
                    "bull_snap_turn_candidate": True,
                    "bear_snap_turn_candidate": False,
                    "asia_reclaim_bar_raw": False,
                    "asia_vwap_long_signal": False,
                    "us_late_pause_resume_long_turn_candidate": True,
                    "asia_early_normal_breakout_retest_hold_long_turn_candidate": True,
                    "asia_early_pause_resume_short_turn_candidate": False,
                    "derived_from_version": "raw:test",
                    "materialized_ts": datetime.fromisoformat("2026-04-05T00:00:00+00:00"),
                    "coverage_window_start": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "coverage_window_end": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
                    "provenance_tag": "shared_features_5m:test",
                },
                {
                    "symbol": "MGC",
                    "shard_id": "2024Q1",
                    "decision_ts": datetime.fromisoformat("2024-01-08T20:00:00+00:00"),
                    "bar_id": "MGC:5m:2024-01-08T20:00:00+00:00",
                    "timeframe": "5m",
                    "session_phase": "ASIA_EARLY",
                    "atr": 1.0,
                    "bar_range": 1.0,
                    "body_size": 1.0,
                    "vol_ratio": 1.0,
                    "turn_ema_fast": 1.0,
                    "turn_ema_slow": 1.0,
                    "velocity": 1.0,
                    "velocity_delta": 1.0,
                    "vwap": 1.0,
                    "vwap_buffer": 1.0,
                    "downside_stretch": 1.0,
                    "upside_stretch": 1.0,
                    "bull_close_strong": True,
                    "bear_close_weak": False,
                    "bull_snap_turn_candidate": False,
                    "bear_snap_turn_candidate": False,
                    "asia_reclaim_bar_raw": False,
                    "asia_vwap_long_signal": False,
                    "us_late_pause_resume_long_turn_candidate": False,
                    "asia_early_normal_breakout_retest_hold_long_turn_candidate": True,
                    "asia_early_pause_resume_short_turn_candidate": False,
                    "derived_from_version": "raw:test",
                    "materialized_ts": datetime.fromisoformat("2026-04-05T00:00:00+00:00"),
                    "coverage_window_start": datetime.fromisoformat("2024-01-08T20:00:00+00:00"),
                    "coverage_window_end": datetime.fromisoformat("2024-01-08T20:00:00+00:00"),
                    "provenance_tag": "shared_features_5m:test",
                },
            ]
        ),
        shared_features_path,
    )

    result = materialize_lane_candidates_partition(
        root_dir=root,
        symbol="MGC",
        shard_id="2024Q1",
        year=2024,
        lane_ids=[
            "mgc_us_late_pause_resume_long_turn__MGC",
            "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
        ],
        family_event_tables_partition_path=family_event_path,
        shared_features_5m_partition_path=shared_features_path,
    )

    lane_ids = [row["lane_id"] for row in result["rows"]]
    assert lane_ids == [
        "mgc_us_late_pause_resume_long_turn__MGC",
        "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
        "mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
    ]


def test_compact_row_treats_missing_family_events_in_window_as_zero_trade_not_missing() -> None:
    lane = resolve_lane_definition(
        lane_id="mgc_us_late_pause_resume_long_turn__MGC",
        symbol="MGC",
    )

    compact = _compact_row_from_warehouse_rows(
        lane=lane,
        available_families=set(),
        canonical_input_range={
            "start": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
            "end": datetime.fromisoformat("2024-01-08T20:25:00+00:00"),
            "shard_id": "2024Q1",
        },
        candidate_rows=[],
        entry_rows=[],
        trade_rows=[],
    )

    assert compact["result_classification"] == "zero_trade"
    assert compact["eligibility_status"] == "eligible_no_family_events_in_window:usLatePauseResumeLongTurn"
    assert compact["zero_trade_flag"] is True


def test_compact_row_uses_nonzero_family_specific_candidates_without_matching_event_rows() -> None:
    lane = resolve_lane_definition(
        lane_id="mgc_asia_early_normal_breakout_retest_hold_turn__MGC",
        symbol="MGC",
    )

    candidate_ts = datetime.fromisoformat("2024-01-08T20:00:00+00:00")
    compact = _compact_row_from_warehouse_rows(
        lane=lane,
        available_families=set(),
        canonical_input_range={
            "start": datetime.fromisoformat("2024-01-08T19:25:00+00:00"),
            "end": datetime.fromisoformat("2024-01-08T20:25:00+00:00"),
            "shard_id": "2024Q1",
        },
        candidate_rows=[
            {
                "lane_id": lane.lane_id,
                "candidate_ts": candidate_ts,
            }
        ],
        entry_rows=[],
        trade_rows=[],
    )

    assert compact["result_classification"] == "zero_trade"
    assert compact["eligibility_status"] == "eligible_no_closed_trades"
    assert compact["emitted_compact_start"] == candidate_ts
    assert compact["emitted_compact_end"] == candidate_ts
