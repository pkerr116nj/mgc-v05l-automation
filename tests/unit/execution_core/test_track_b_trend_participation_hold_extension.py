from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trend_participation_hold_extension import (
    EXIT_NOW,
    EXTEND_HOLD,
    INSUFFICIENT_EVIDENCE,
    TrendHoldCandle,
    TrendHoldExtensionConfig,
    build_trend_participation_hold_extension_replay,
    evaluate_trend_participation_hold_extension,
)


NOW = datetime(2026, 6, 15, 14, 0, tzinfo=UTC)


def test_long_continuation_extends_without_using_future_data() -> None:
    due_at = datetime(2026, 6, 15, 14, 15, tzinfo=UTC)
    candles = (
        _candle("2026-06-15T14:00:00+00:00", 100, 103, 99, 102),
        _candle("2026-06-15T14:05:00+00:00", 102, 105, 101, 104),
        _candle("2026-06-15T14:10:00+00:00", 104, 108, 103, 107),
    )
    future_reversal = (_candle("2026-06-15T14:15:00+00:00", 107, 108, 90, 91),)

    result = evaluate_trend_participation_hold_extension(
        side="LONG",
        due_at=due_at,
        completed_candles=candles,
        future_candles_for_offline_replay=future_reversal,
    )

    assert result["recommendation"] == EXTEND_HOLD
    assert result["would_have_extended"] is True
    assert result["features"]["uses_future_data_for_recommendation"] is False
    assert result["hypothetical_offline_results"]["plus_5m"]["points_vs_exit_now"] < 0
    assert result["broker_mutation_allowed"] is False
    assert result["submit_allowed"] is False


def test_short_requires_stronger_evidence_and_exits_on_weak_downside() -> None:
    due_at = datetime(2026, 6, 15, 10, 15, tzinfo=UTC)

    result = evaluate_trend_participation_hold_extension(
        side="SHORT",
        due_at=due_at,
        completed_candles=(
            _candle("2026-06-15T10:00:00+00:00", 100, 101, 98, 99),
            _candle("2026-06-15T10:05:00+00:00", 99, 100, 97, 98),
            _candle("2026-06-15T10:10:00+00:00", 98, 99, 96.75, 97.5),
        ),
    )

    assert result["recommendation"] == EXIT_NOW
    assert "continuation_evidence_insufficient" in result["reason_codes"]


def test_strong_short_continuation_can_extend() -> None:
    due_at = datetime(2026, 6, 15, 10, 15, tzinfo=UTC)

    result = evaluate_trend_participation_hold_extension(
        side="SHORT",
        due_at=due_at,
        completed_candles=(
            _candle("2026-06-15T10:00:00+00:00", 105, 106, 102, 103),
            _candle("2026-06-15T10:05:00+00:00", 103, 104, 98, 99),
            _candle("2026-06-15T10:10:00+00:00", 99, 100, 94, 95),
        ),
    )

    assert result["recommendation"] == EXTEND_HOLD


def test_insufficient_completed_5m_candles_stays_shadow_insufficient() -> None:
    result = evaluate_trend_participation_hold_extension(
        side="LONG",
        due_at=datetime(2026, 6, 15, 14, 10, tzinfo=UTC),
        completed_candles=(
            _candle("2026-06-15T14:00:00+00:00", 100, 101, 99, 100.5),
            _candle("2026-06-15T14:05:00+00:00", 100.5, 101, 100, 100.75),
        ),
    )

    assert result["recommendation"] == INSUFFICIENT_EVIDENCE
    assert result["would_have_extended"] is False
    assert "fewer_than_3_completed_5m_candles" in result["reason_codes"]


def test_replay_report_is_shadow_only_and_summarizes_completed_noisemaker(tmp_path: Path) -> None:
    repo = tmp_path
    lifecycle_id = "reserved_submit_mnq_us_active_participation_long_20260615T140000Z_test"
    lane_id = "mnq_us_active_participation_long"
    lifecycle_path = (
        repo
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write_json(
        lifecycle_path,
        {
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
            "entry_fill": {"filled_at": "2026-06-15T14:00:10+00:00", "price": "100"},
            "close_fill": {"filled_at": "2026-06-15T14:15:05+00:00", "price": "106"},
        },
    )
    ledger_path = repo / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "strategy_id": lane_id,
                "lifecycle_id": lifecycle_id,
                "final_position_status": "CLOSED_FLAT",
                "entry_timestamp": "2026-06-15T14:00:10+00:00",
                "exit_timestamp": "2026-06-15T14:15:05+00:00",
                "paper_lifecycle_report_path": str(lifecycle_path.relative_to(repo)),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _seed_lane_db(repo / f"mgc_v05l.probationary.paper__{lane_id}.sqlite3")

    payload = build_trend_participation_hold_extension_replay(
        config=TrendHoldExtensionConfig(repo_root=repo, trading_date="2026-06-15"),
        now=NOW,
    )

    assert payload["trade_count"] == 1
    assert payload["broker_mutation_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["cancel_attempted"] is False
    assert payload["close_attempted"] is False
    assert payload["summary"]["recommendation_counts"][EXTEND_HOLD] == 1
    trade = payload["trades"][0]
    assert trade["shadow_decision"]["hypothetical_offline_results"]["plus_15m"]["available"] is True


def _candle(ts: str, open_: float, high: float, low: float, close: float) -> TrendHoldCandle:
    return TrendHoldCandle(datetime.fromisoformat(ts).astimezone(UTC), open_, high, low, close)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_lane_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "create table bars (timestamp text, open real, high real, low real, close real, timeframe text, data_source text)"
    )
    rows = [
        ("2026-06-15T14:00:00+00:00", 100, 102, 99, 101),
        ("2026-06-15T14:01:00+00:00", 101, 103, 100, 102),
        ("2026-06-15T14:02:00+00:00", 102, 104, 101, 103),
        ("2026-06-15T14:03:00+00:00", 103, 105, 102, 104),
        ("2026-06-15T14:04:00+00:00", 104, 106, 103, 105),
        ("2026-06-15T14:05:00+00:00", 105, 107, 104, 106),
        ("2026-06-15T14:06:00+00:00", 106, 108, 105, 107),
        ("2026-06-15T14:07:00+00:00", 107, 109, 106, 108),
        ("2026-06-15T14:08:00+00:00", 108, 110, 107, 109),
        ("2026-06-15T14:09:00+00:00", 109, 111, 108, 110),
        ("2026-06-15T14:10:00+00:00", 110, 112, 109, 111),
        ("2026-06-15T14:11:00+00:00", 111, 113, 110, 112),
        ("2026-06-15T14:12:00+00:00", 112, 114, 111, 113),
        ("2026-06-15T14:13:00+00:00", 113, 115, 112, 114),
        ("2026-06-15T14:14:00+00:00", 114, 116, 113, 115),
        ("2026-06-15T14:15:00+00:00", 115, 117, 114, 116),
        ("2026-06-15T14:16:00+00:00", 116, 118, 115, 117),
        ("2026-06-15T14:17:00+00:00", 117, 119, 116, 118),
        ("2026-06-15T14:18:00+00:00", 118, 120, 117, 119),
        ("2026-06-15T14:19:00+00:00", 119, 121, 118, 120),
        ("2026-06-15T14:20:00+00:00", 120, 122, 119, 121),
        ("2026-06-15T14:21:00+00:00", 121, 123, 120, 122),
        ("2026-06-15T14:22:00+00:00", 122, 124, 121, 123),
        ("2026-06-15T14:23:00+00:00", 123, 125, 122, 124),
        ("2026-06-15T14:24:00+00:00", 124, 126, 123, 125),
        ("2026-06-15T14:25:00+00:00", 125, 127, 124, 126),
        ("2026-06-15T14:26:00+00:00", 126, 128, 125, 127),
        ("2026-06-15T14:27:00+00:00", 127, 129, 126, 128),
        ("2026-06-15T14:28:00+00:00", 128, 130, 127, 129),
        ("2026-06-15T14:29:00+00:00", 129, 131, 128, 130),
    ]
    conn.executemany(
        "insert into bars(timestamp, open, high, low, close, timeframe, data_source) values (?, ?, ?, ?, ?, '1m', 'databento_live')",
        rows,
    )
    conn.commit()
    conn.close()
