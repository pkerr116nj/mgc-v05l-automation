from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.app import track_b_entry_acceptance_research_report as report_module
from mgc_v05l.app.track_b_entry_acceptance_research_report import (
    EXACT_FLAG_COLUMN,
    build_archive_rows,
    build_report,
    score_archive_rows,
)
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset


def test_build_archive_rows_enriches_breakout_retest_hold_context() -> None:
    rows = build_archive_rows(
        instrument="MGC",
        bars=[
            _bar("2024-01-02T00:40:00+00:00", 100.0, 101.0, 99.8, 100.5),
            _bar("2024-01-02T00:45:00+00:00", 100.5, 102.0, 100.4, 101.8),
            _bar("2024-01-02T00:50:00+00:00", 101.8, 102.2, 101.7, 102.1),
        ],
        features=[
            _feature("2024-01-02T00:40:00+00:00", False, atr=1.0, velocity=0.1),
            _feature("2024-01-02T00:45:00+00:00", False, atr=1.6, velocity=0.2),
            _feature("2024-01-02T00:50:00+00:00", True, atr=1.2, velocity=0.15),
        ],
        lane_candidates=[
            {
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "candidate_id": "candidate-1",
                "feature_bar_id": "MGC:5m:2024-01-02T00:50:00+00:00",
                "decision_ts": "2024-01-02T00:50:00+00:00",
            }
        ],
        bars_source_path=Path("outputs/warehouse_historical_evaluator_basket_q1_structural_exit_fix/bars.parquet"),
        features_source_path=Path("outputs/warehouse_historical_evaluator_basket_q1_structural_exit_fix/features.parquet"),
        lane_candidates_source_path=Path("outputs/warehouse_historical_evaluator_basket_q1_structural_exit_fix/candidates.parquet"),
    )

    enriched = rows[-1]
    assert len(rows) == 3
    assert enriched["instrument"] == "MGC"
    assert enriched["session"] == "ASIA_EARLY"
    assert enriched["timeframe"] == "5m"
    assert enriched["candidate_family"] == "asiaEarlyNormalBreakoutRetestHoldLong"
    assert enriched["current_exact_rule_flag"] is True
    assert enriched["candidate_id"] == "candidate-1"
    assert enriched["signal_candle"]["timestamp"] == "2024-01-02T00:50:00+00:00"
    assert len(enriched["scoring_candles"]) == 3
    assert enriched["breakout_breaks_prior_1_high"] is True
    assert enriched["signal_retests_and_holds_breakout_level"] is True
    assert enriched["breakout_level"] == 102.0
    assert enriched["retest_depth"] == pytest.approx(0.3)
    assert enriched["hold_margin"] == pytest.approx(0.1)
    assert enriched["range_expansion_ratio"] == pytest.approx(1.0)
    assert enriched["close_location"] == pytest.approx(0.8)
    assert enriched["body_to_range_ratio"] == pytest.approx(0.6)
    assert enriched["data_quality"]["enriched_field_completeness"] == 1.0
    assert enriched["safety_flags"] == {
        "strategy_authority": False,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "order_intent_created": False,
        "lifecycle_mutated": False,
        "runtime_trade_eligible": False,
    }


def test_build_archive_rows_marks_invalid_when_history_missing() -> None:
    rows = build_archive_rows(
        instrument="GC",
        bars=[_bar("2024-01-02T00:05:00+00:00", 100.0, 101.0, 99.8, 100.5)],
        features=[_feature("2024-01-02T00:05:00+00:00", False, atr=1.0, velocity=0.1)],
    )

    row = rows[0]
    assert row["breakout_breaks_prior_1_high"] is False
    assert row["signal_retests_and_holds_breakout_level"] is False
    assert row["breakout_level"] is None
    assert len(row["scoring_candles"]) == 1
    assert row["data_quality"]["has_min_breakout_history"] is False
    assert row["data_quality"]["enriched_field_completeness"] < 1.0


def test_score_archive_rows_builds_acceptance_distribution(tmp_path: Path) -> None:
    rows = build_archive_rows(
        instrument="MGC",
        bars=[
            _bar("2024-01-02T00:40:00+00:00", 100.0, 101.0, 99.8, 100.5),
            _bar("2024-01-02T00:45:00+00:00", 100.5, 102.0, 100.4, 101.8),
            _bar("2024-01-02T00:50:00+00:00", 101.8, 102.2, 101.7, 102.1),
        ],
        features=[
            _feature("2024-01-02T00:40:00+00:00", False, atr=1.0, velocity=0.1),
            _feature("2024-01-02T00:45:00+00:00", False, atr=1.6, velocity=0.2),
            _feature("2024-01-02T00:50:00+00:00", True, atr=1.2, velocity=0.15),
        ],
    )

    report = score_archive_rows(
        rows,
        archive_path=tmp_path / "archive.jsonl",
        scoring_json=tmp_path / "scoring.json",
        scoring_md=tmp_path / "scoring.md",
        forward_json=tmp_path / "forward.json",
        forward_md=tmp_path / "forward.md",
        backtest_json=tmp_path / "backtest.json",
        backtest_md=tmp_path / "backtest.md",
    )

    assert report["mode"] == "RESEARCH_WAREHOUSE_ARCHIVE_SCORING_ONLY"
    assert report["authoritative_runtime_truth"] is False
    assert report["strategy_behavior_changed"] is False
    assert report["trade_simulation_performed"] is False
    assert report["pnl_calculated"] is False
    assert report["total_rows_scanned"] == 3
    assert report["exact_candidate_flag_acceptance_class_cross_tab"]["True"]
    assert report["forward_return_diagnostic_summary"]["candidate_bucket_counts"]
    assert Path(report["scoring_json"]).exists()
    assert Path(report["scoring_markdown"]).exists()
    assert Path(report["forward_return_diagnostic_json"]).exists()
    assert Path(report["forward_return_diagnostic_markdown"]).exists()
    assert Path(report["diagnostic_backtest_json"]).exists()
    assert Path(report["diagnostic_backtest_markdown"]).exists()
    assert report["diagnostic_backtest_summary"]["episode_counts"]


def test_build_report_writes_warehouse_archive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    warehouse = tmp_path / "outputs" / "warehouse_historical_evaluator_basket_q1_structural_exit_fix"
    _touch_warehouse_sources(warehouse, "MGC")

    def fake_read_parquet(path: Path | None) -> list[dict[str, object]]:
        assert path is not None
        if path.name == "bars.parquet":
            return [
                _bar("2024-01-02T00:40:00+00:00", 100.0, 101.0, 99.8, 100.5),
                _bar("2024-01-02T00:45:00+00:00", 100.5, 102.0, 100.4, 101.8),
                _bar("2024-01-02T00:50:00+00:00", 101.8, 102.2, 101.7, 102.1),
            ]
        if path.name == "features.parquet":
            return [
                _feature("2024-01-02T00:40:00+00:00", False, atr=1.0, velocity=0.1),
                _feature("2024-01-02T00:45:00+00:00", False, atr=1.6, velocity=0.2),
                _feature("2024-01-02T00:50:00+00:00", True, atr=1.2, velocity=0.15),
            ]
        return [
            {
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
                "candidate_id": "candidate-1",
                "feature_bar_id": "MGC:5m:2024-01-02T00:50:00+00:00",
                "decision_ts": "2024-01-02T00:50:00+00:00",
            }
        ]

    monkeypatch.setattr(report_module, "_read_parquet_records", fake_read_parquet)

    report = build_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        instruments=("MGC",),
        year="2024",
        shard_id="2024Q1",
    )

    assert report["mode"] == "RESEARCH_WAREHOUSE_OFFLINE_ONLY"
    assert report["authoritative_runtime_truth"] is False
    assert report["strategy_behavior_changed"] is False
    assert report["trade_simulation_performed"] is False
    assert report["pnl_calculated"] is False
    assert report["total_rows"] == 3
    assert report["row_counts_by_instrument"] == {"MGC": 3}
    assert report["exact_candidate_flag_counts"] == {"False": 2, "True": 1}
    assert report["entry_acceptance_scoring_ready"] is False
    assert report["entry_acceptance_scoring_v2"]["total_rows_scanned"] == 3
    assert Path(report["archive_jsonl"]).exists()
    assert Path(report["acceptance_scoring_report_json"]).exists()
    assert Path(report["forward_return_diagnostic_json"]).exists()
    assert Path(report["diagnostic_backtest_json"]).exists()
    assert Path(report["summary_json"]).exists()
    assert Path(report["summary_markdown"]).exists()


def test_build_report_rejects_runtime_like_source(tmp_path: Path) -> None:
    warehouse = tmp_path / "outputs" / "runtime" / "warehouse_historical_evaluator_basket_q1_structural_exit_fix"

    with pytest.raises(ValueError, match="refusing non-research/live/runtime-like source path"):
        build_report(warehouse_root=warehouse, output_root=tmp_path / "reports", instruments=("MGC",))


def test_read_parquet_records_handles_timezone_aware_timestamps(tmp_path: Path) -> None:
    parquet_path = tmp_path / "candidates.parquet"
    materialize_parquet_dataset(
        parquet_path,
        [
            {
                "candidate_id": "candidate-1",
                "candidate_ts": datetime(2024, 4, 9, 7, 10, tzinfo=UTC),
                "decision_ts": datetime(2024, 4, 9, 7, 10, tzinfo=UTC),
                "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
            }
        ],
    )

    rows = report_module._read_parquet_records(parquet_path)

    assert rows == [
        {
            "candidate_id": "candidate-1",
            "candidate_ts": datetime(2024, 4, 9, 7, 10, tzinfo=UTC),
            "decision_ts": datetime(2024, 4, 9, 7, 10, tzinfo=UTC),
            "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
        }
    ]


def _touch_warehouse_sources(root: Path, instrument: str) -> None:
    for dataset, filename in (
        ("derived_bars_5m", "bars.parquet"),
        ("shared_features_5m", "features.parquet"),
        ("lane_candidates", "candidates.parquet"),
    ):
        path = (
            root
            / "datasets"
            / dataset
            / f"symbol={instrument}"
            / "year=2024"
            / "shard_id=2024Q1"
            / filename
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test", encoding="utf-8")


def _bar(timestamp: str, open_price: float, high: float, low: float, close: float) -> dict[str, object]:
    return {
        "symbol": "MGC",
        "timeframe": "5m",
        "bar_ts": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": 100,
        "source_data_source": "RESEARCH_WAREHOUSE",
        "derived_rule": "5m_closed_bar",
        "materialized_from_raw_version": "test",
        "provenance_tag": "research_fixture",
    }


def _feature(timestamp: str, exact_flag: bool, *, atr: float, velocity: float) -> dict[str, object]:
    return {
        "symbol": "MGC",
        "shard_id": "2024Q1",
        "decision_ts": timestamp,
        "bar_id": f"MGC:5m:{timestamp}",
        "timeframe": "5m",
        "session_phase": "ASIA_EARLY",
        "atr": atr,
        "bar_range": 1.2,
        "body_size": 0.3,
        "vol_ratio": 1.0,
        "velocity": velocity,
        "velocity_delta": 0.0,
        "vwap": 100.0,
        "bull_snap_turn_candidate": False,
        "bear_snap_turn_candidate": False,
        EXACT_FLAG_COLUMN: exact_flag,
        "materialized_ts": "2026-04-05T16:06:06+00:00",
        "provenance_tag": "research_fixture",
    }
