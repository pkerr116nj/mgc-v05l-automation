from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core import phase1_gc_paper_candidate as gc_candidate
from mgc_v05l.execution_core.phase1_gc_paper_candidate import (
    CHOSEN_GC_STRATEGY_ID,
    Phase1GcCandidateConfig,
    build_phase1_gc_paper_candidate,
    inventory_gc_paper_candidate_lanes,
)


NOW = datetime(2026, 5, 9, 14, 0, tzinfo=timezone.utc)


def _runtime_candle_root(root: Path) -> Path:
    return root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"


def _write_seed_artifact(
    root: Path,
    *,
    symbol: str = "GC",
    timeframe: str = "1m",
    bars: list[dict[str, object]] | None = None,
) -> Path:
    path = _runtime_candle_root(root) / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = bars if bars is not None else _bars(timeframe=timeframe)
    payload = {
        "source": "DATABENTO_HISTORICAL_SEED",
        "source_id": "databento_historical_seed:test",
        "generated_at": NOW.isoformat(),
        "symbol": symbol,
        "instrument": symbol,
        "root": symbol,
        "timeframe": timeframe,
        "start_ts": rows[0]["bar_start"],
        "end_ts": rows[-1]["bar_end"],
        "bar_count": len(rows),
        "last_completed_bar_ts": rows[-1]["bar_end"],
        "historical_seed_ready": True,
        "realtime_feed_confirmed": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "completed_candles_only": True,
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "bars": rows,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_gc_seed_bundle(root: Path) -> None:
    for timeframe in ("1m", "3m", "5m"):
        _write_seed_artifact(root, timeframe=timeframe, bars=_bars(timeframe=timeframe))


def _bars(*, timeframe: str, count: int = 12) -> list[dict[str, object]]:
    minutes = {"1m": 1, "3m": 3, "5m": 5}[timeframe]
    start = datetime(2026, 5, 4, 22, 0, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    price = 3300.0
    for index in range(count):
        bar_start = start + timedelta(minutes=minutes * index)
        bar_end = bar_start + timedelta(minutes=minutes)
        close = price + (0.5 if index % 2 == 0 else -0.2)
        rows.append(
            {
                "bar_start": bar_start.isoformat(),
                "bar_end": bar_end.isoformat(),
                "open": round(price, 2),
                "high": round(max(price, close) + 0.8, 2),
                "low": round(min(price, close) - 0.6, 2),
                "close": round(close, 2),
                "volume": 100 + index,
            }
        )
        price = close
    return rows


def _config(tmp_path: Path, **overrides: object) -> Phase1GcCandidateConfig:
    values = {
        "repo_root": gc_candidate.REPO_ROOT,
        "runtime_candle_root": _runtime_candle_root(tmp_path),
        "output_dir": tmp_path / "reports",
        "write_report": False,
        "max_bars": 30,
    }
    values.update(overrides)
    return Phase1GcCandidateConfig(**values)


def test_inventory_recommends_one_gc_asia_london_candidate() -> None:
    rows = inventory_gc_paper_candidate_lanes()
    selected = next(row for row in rows if row.strategy_id == CHOSEN_GC_STRATEGY_ID)

    assert {row.strategy_id for row in rows} == {
        "gc_1x_all_lanes__asia_early_long",
        "gc_1x_all_lanes__asia_early_short",
        "gc_1x_all_lanes__london_early_long",
        "gc_1x_all_lanes__us_early_short",
        "gc_1x_all_lanes__us_midday_short",
        "gc_1x_asia_london_participation__asia_london_long_v5",
        "gc_1x_asia_london_participation__asia_london_short_v2",
    }
    assert selected.family == "asia_london_participation_core_v1"
    assert selected.structural_timeframe == "3m"
    assert selected.execution_timeframe == "1m"


def test_gc_candidate_evaluates_historical_seed_without_paper_watch_readiness(tmp_path: Path) -> None:
    _write_gc_seed_bundle(tmp_path)

    artifacts = build_phase1_gc_paper_candidate(config=_config(tmp_path))

    assert artifacts.report["chosen_strategy"] == CHOSEN_GC_STRATEGY_ID
    assert artifacts.report["instrument_scope"] == "GC_ONLY"
    assert artifacts.evaluation["candidate_evaluation_ready"] is True
    assert artifacts.evaluation["bars_evaluated"] > 0
    assert artifacts.evaluation["historical_seed_ready"] is True
    assert artifacts.evaluation["realtime_feed_confirmed"] is False
    assert artifacts.evaluation["paper_watch_ready"] is False
    assert artifacts.evaluation["paper_watch_block_reason"] == "REALTIME_FEED_NOT_CONFIRMED"
    assert artifacts.evaluation["can_submit"] is False
    assert artifacts.evaluation["submit_attempted"] is False
    assert artifacts.evaluation["live_money_eligible"] is False
    assert artifacts.evaluation["broker_mutation_path_touched"] is False
    assert artifacts.evaluation["paper_proof_invoked"] is False


def test_missing_realtime_feed_fails_closed_even_when_seed_exists(tmp_path: Path) -> None:
    _write_gc_seed_bundle(tmp_path)

    artifacts = build_phase1_gc_paper_candidate(config=_config(tmp_path))

    assert artifacts.report["historical_seed_ready"] is True
    assert artifacts.report["realtime_feed_confirmed"] is False
    assert artifacts.report["runtime_candles_ready"] is False
    assert artifacts.report["paper_watch_ready"] is False
    assert artifacts.report["can_submit"] is False
    assert artifacts.report["live_money_eligible"] is False


def test_missing_seed_blocks_gc_candidate_evaluation(tmp_path: Path) -> None:
    artifacts = build_phase1_gc_paper_candidate(config=_config(tmp_path))

    assert artifacts.evaluation["candidate_evaluation_ready"] is False
    assert artifacts.evaluation["block_reason"] == "HISTORICAL_SEED_MISSING"
    assert artifacts.evaluation["can_submit"] is False
    assert artifacts.evaluation["submit_attempted"] is False


def test_non_gc_tickers_are_not_strategy_promoted(tmp_path: Path) -> None:
    _write_gc_seed_bundle(tmp_path)

    artifacts = build_phase1_gc_paper_candidate(config=_config(tmp_path))

    assert {row["instrument"] for row in artifacts.inventory_rows} == {"GC"}
    assert artifacts.report["non_gc_strategy_promoted"] is False
    assert artifacts.evaluation["non_gc_strategy_promoted"] is False


def test_gc_candidate_module_has_no_direct_broker_mutation_calls() -> None:
    source = Path(gc_candidate.__file__).read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "run_ibkr_manual_paper_submit_test" not in source
    assert "run_ibkr_paper_strategy_bridge" not in source
