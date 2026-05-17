from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_entry_exit_candidate_replay import (
    EXACT_FLAG_COLUMN,
    build_replay_report,
)
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset


def test_fixed_36_exits_at_36_bars(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1], close_step=0.1)

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        candidate_ids=("track_b_exact_baseline_fixed_36b_exit_v1",),
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    fixed = report["candidate_results"]["track_b_exact_baseline_fixed_36b_exit_v1"]
    assert fixed["episodes"] == 1
    assert fixed["branch_counts"] == {"fixed_36b": 1}
    assert fixed["stats"]["trade_count"] == 1
    assert fixed["stats"]["average_return"] == pytest.approx(3.7)


def test_adaptive_exits_at_24_when_conditions_fail(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1], profile="adaptive_fail")

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        candidate_ids=("track_b_exact_baseline_adaptive_24_to_36_exit_v1",),
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    adaptive = report["candidate_results"]["track_b_exact_baseline_adaptive_24_to_36_exit_v1"]
    assert adaptive["episodes"] == 1
    assert adaptive["branch_counts"] == {"exit_24b": 1}
    assert adaptive["stats"]["average_return"] == pytest.approx(-0.2)


def test_adaptive_extends_to_36_when_conditions_pass(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1], profile="adaptive_pass")

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        candidate_ids=("track_b_exact_baseline_adaptive_24_to_36_exit_v1",),
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    adaptive = report["candidate_results"]["track_b_exact_baseline_adaptive_24_to_36_exit_v1"]
    assert adaptive["episodes"] == 1
    assert adaptive["branch_counts"] == {"extend_to_36b": 1}
    assert adaptive["stats"]["average_return"] == pytest.approx(4.0)


def test_only_exact_rule_flag_rows_are_eligible(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1], close_step=0.1)
    _append_false_feature_only_noise(warehouse)

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        candidate_ids=("track_b_exact_baseline_fixed_36b_exit_v1",),
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    assert report["raw_exact_flag_counts"] == {"GC": 1}
    assert report["episode_counts"] == {"GC": 1}


def test_duplicate_candidate_bars_are_deduped(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1, 5, 20], close_step=0.1)

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        candidate_ids=("track_b_exact_baseline_fixed_36b_exit_v1",),
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    assert report["raw_exact_flag_counts"] == {"GC": 3}
    assert report["episode_counts"] == {"GC": 2}
    fixed = report["candidate_results"]["track_b_exact_baseline_fixed_36b_exit_v1"]
    assert fixed["episodes"] == 2


def test_runtime_like_warehouse_roots_are_rejected(tmp_path: Path) -> None:
    warehouse = tmp_path / "outputs" / "runtime" / "warehouse_historical_evaluator_test"

    with pytest.raises(ValueError, match="refusing non-research/live/runtime-like source path"):
        build_replay_report(
            warehouse_root=warehouse,
            output_root=tmp_path / "reports",
            candidate_ids=("track_b_exact_baseline_fixed_36b_exit_v1",),
            instruments=("GC",),
        )


def test_report_writes_json_markdown_and_false_authority_flags(tmp_path: Path) -> None:
    warehouse = _warehouse_with_rows(tmp_path, exact_indices=[1], close_step=0.1)

    report = build_replay_report(
        warehouse_root=warehouse,
        output_root=tmp_path / "reports",
        instruments=("GC",),
        year="2024",
        shard_id="2024Q1",
    )

    assert Path(report["report_json"]).exists()
    assert Path(report["report_markdown"]).exists()
    assert report["candidate_universe_decision_note"] == (
        "docs/track_b_entry_acceptance_candidate_universe_decision.md"
    )
    assert report["candidate_universe_decision"] == {
        "EXACT_BASELINE_RETAINED": True,
        "NEAR_EXPANSION_PARKED": True,
        "EXIT_PROFILE_PROMOTION_CONTINUES": True,
    }
    assert report["authority_flags"]["broker_state_mutated"] is False
    assert report["authority_flags"]["order_intent_created"] is False
    assert report["authority_flags"]["lifecycle_mutated"] is False
    assert report["authority_flags"]["runtime_trade_eligible"] is False
    for payload in report["candidate_results"].values():
        assert payload["authority_flags"]["strategy_authority"] is False


def _warehouse_with_rows(
    tmp_path: Path,
    *,
    exact_indices: list[int],
    close_step: float = 0.0,
    profile: str | None = None,
) -> Path:
    root = tmp_path / "outputs" / "warehouse_historical_evaluator_replay_fixture"
    bars_path = (
        root
        / "datasets"
        / "derived_bars_5m"
        / "symbol=GC"
        / "year=2024"
        / "shard_id=2024Q1"
        / "bars.parquet"
    )
    features_path = (
        root
        / "datasets"
        / "shared_features_5m"
        / "symbol=GC"
        / "year=2024"
        / "shard_id=2024Q1"
        / "features.parquet"
    )
    bars = [_bar(index, close_step=close_step, profile=profile) for index in range(70)]
    features = [
        _feature(index, exact_flag=index in set(exact_indices))
        for index in range(50)
    ]
    materialize_parquet_dataset(bars_path, bars)
    materialize_parquet_dataset(features_path, features)
    return root


def _append_false_feature_only_noise(root: Path) -> None:
    features_path = (
        root
        / "datasets"
        / "shared_features_5m"
        / "symbol=GC"
        / "year=2024"
        / "shard_id=2024Q1"
        / "features.parquet"
    )
    features = [_feature(index, exact_flag=index == 1) for index in range(50)]
    features.append(_feature(51, exact_flag=False))
    materialize_parquet_dataset(features_path, features)


def _bar(index: int, *, close_step: float, profile: str | None) -> dict[str, object]:
    timestamp = datetime(2024, 1, 2, tzinfo=UTC) + timedelta(minutes=5 * index)
    base = 100.0
    if profile == "adaptive_pass":
        close = _adaptive_pass_close(index)
        high = max(close, base + (2.0 if 2 <= index <= 25 else 0.0))
        low = min(close, base - 0.2)
    elif profile == "adaptive_fail":
        close = base - 0.2 if index <= 25 else base + 0.1
        high = max(close, base + 0.4)
        low = min(close, base - 0.8)
    else:
        close = base + index * close_step
        high = close + 0.2
        low = close - 0.2
    return {
        "symbol": "GC",
        "timeframe": "5m",
        "bar_ts": timestamp,
        "open": base,
        "high": high,
        "low": low,
        "close": close,
        "volume": 100,
        "source_data_source": "RESEARCH_WAREHOUSE",
        "derived_rule": "5m_closed_bar",
        "provenance_tag": "research_fixture",
    }


def _adaptive_pass_close(index: int) -> float:
    if index <= 25:
        return 101.7
    if index == 37:
        return 104.0
    return 102.0


def _feature(index: int, *, exact_flag: bool) -> dict[str, object]:
    timestamp = datetime(2024, 1, 2, tzinfo=UTC) + timedelta(minutes=5 * index)
    return {
        "symbol": "GC",
        "shard_id": "2024Q1",
        "decision_ts": timestamp,
        "bar_id": f"GC:5m:{timestamp.isoformat()}",
        "timeframe": "5m",
        "session_phase": "ASIA_EARLY",
        EXACT_FLAG_COLUMN: exact_flag,
        "provenance_tag": "research_fixture",
    }
