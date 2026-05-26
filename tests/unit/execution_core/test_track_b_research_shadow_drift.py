from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_shadow_drift import (
    GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA,
    GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE,
    GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE,
    LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT,
    LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE,
    LATE_JOIN_ASIAN_DRIFT_SHADOW_NO_CANDIDATE,
    P0_ENTRY_GRADE_A,
    P0_ENTRY_GRADE_B,
    P0_ENTRY_GRADE_C,
    TrackBResearchShadowDriftConfig,
    build_gap_drift_continuation_shadow,
    build_late_join_asian_drift_shadow,
    build_p0_near_miss_shadow,
    write_research_shadow_drift_artifacts,
)


def now() -> datetime:
    return datetime(2026, 5, 25, 1, 0, tzinfo=timezone.utc)


def bars(*, symbol: str = "MGC", direction: str = "LONG", count: int = 10) -> dict[str, object]:
    rows = []
    price = 100.0
    start = datetime(2026, 5, 25, 0, 0, tzinfo=timezone.utc)
    for index in range(count):
        open_price = price
        close_price = price + (1.2 if direction == "LONG" else -1.2)
        high = max(open_price, close_price) + 0.3
        low = min(open_price, close_price) - 0.3
        rows.append(
            {
                "bar_end": (start + timedelta(minutes=5 * index)).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
                "completed": True,
            }
        )
        price = close_price
    return {
        "generated_at": now().isoformat(),
        "symbol": symbol,
        "timeframe": "5m",
        "realtime_feed_confirmed": True,
        "bars": rows,
    }


def test_strong_late_join_drift_with_missing_anchor_is_shadow_candidate() -> None:
    result = build_late_join_asian_drift_shadow(
        asian_drift_report={
            "late_join_diagnostic": True,
            "anchor_required": True,
            "anchor_observed": False,
            "missing_anchor_reason": "runtime_context_started_after_anchor_window",
            "hypothetical_late_join_score": 4.7,
            "dominant_direction": "LONG",
            "latest_completed_5m_candle_timestamp": "2026-05-25T00:25:00+00:00",
        },
        now=now(),
    )

    assert result["shadow_classification"] == LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE
    assert result["hypothetical_direction"] == "LONG"
    assert result["hypothetical_score"] == 4.7
    assert result["submit_allowed"] is False
    assert result["broker_mutation_allowed"] is False
    assert result["not_order_authority"] is True
    assert result["not_lifecycle_authority"] is True


def test_weak_late_join_drift_is_no_candidate() -> None:
    result = build_late_join_asian_drift_shadow(
        asian_drift_report={
            "anchor_required": True,
            "anchor_observed": False,
            "hypothetical_late_join_score": 1.2,
        },
        now=now(),
    )

    assert result["shadow_classification"] == LATE_JOIN_ASIAN_DRIFT_SHADOW_NO_CANDIDATE
    assert result["submit_allowed"] is False


def test_missing_late_join_context_is_insufficient_context() -> None:
    result = build_late_join_asian_drift_shadow(
        asian_drift_report=None,
        p0_loop_events=(),
        now=now(),
    )

    assert result["shadow_classification"] == LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT
    assert "missing" in result["missing_authority_reason"]


def test_gap_up_continuation_mgc_is_shadow_candidate() -> None:
    result = build_gap_drift_continuation_shadow(
        candle_payloads={"MGC": bars(symbol="MGC", direction="LONG")},
        source_paths={"MGC": Path("phase1/MGC/5m/latest_runtime_candles.json")},
        safe_state={"safe_state_classification": "SAFE_STATE_NORMAL"},
        now=now(),
    )

    assert result["shadow_classification"] == GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE
    assert result["symbol"] == "MGC"
    assert result["hypothetical_direction"] == "LONG"
    assert result["submit_allowed"] is False
    assert result["not_order_authority"] is True


def test_mnq_trend_continuation_can_be_shadow_candidate() -> None:
    result = build_gap_drift_continuation_shadow(
        candle_payloads={"MNQ": bars(symbol="MNQ", direction="SHORT")},
        source_paths={"MNQ": Path("phase1/MNQ/5m/latest_runtime_candles.json")},
        safe_state={"safe_state_classification": "SAFE_STATE_NORMAL"},
        now=now(),
    )

    assert result["shadow_classification"] == GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE
    assert result["symbol"] == "MNQ"
    assert result["hypothetical_direction"] == "SHORT"


def test_gap_drift_insufficient_data() -> None:
    result = build_gap_drift_continuation_shadow(
        candle_payloads={"MGC": bars(symbol="MGC", count=3)},
        source_paths={"MGC": Path("phase1/MGC/5m/latest_runtime_candles.json")},
        safe_state={"safe_state_classification": "SAFE_STATE_NORMAL"},
        now=now(),
    )

    assert result["shadow_classification"] == GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA


def test_safe_state_hard_hold_prevents_shadow_candidate_label() -> None:
    result = build_gap_drift_continuation_shadow(
        candle_payloads={"MGC": bars(symbol="MGC", direction="LONG")},
        source_paths={"MGC": Path("phase1/MGC/5m/latest_runtime_candles.json")},
        safe_state={"safe_state_classification": "SAFE_STATE_HARD_HOLD"},
        now=now(),
    )

    assert result["shadow_classification"] == GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE
    assert result["safe_state_shadow_block"] is True
    assert "SAFE_STATE_HARD_HOLD" in result["missing_authority_reason"]


def test_writer_outputs_latest_and_jsonl(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    asian_report = root / "asian.json"
    mgc = root / "mgc.json"
    mnq = root / "mnq.json"
    safe = root / "safe.json"
    for path, payload in (
        (
            asian_report,
            {
                "late_join_diagnostic": True,
                "anchor_observed": False,
                "hypothetical_late_join_score": 4.2,
                "dominant_direction": "LONG",
            },
        ),
        (mgc, bars(symbol="MGC", direction="LONG")),
        (mnq, bars(symbol="MNQ", direction="LONG")),
        (safe, {"safe_state_classification": "SAFE_STATE_NORMAL"}),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    report = write_research_shadow_drift_artifacts(
        config=TrackBResearchShadowDriftConfig(
            repo_root=tmp_path,
            output_root=Path("outputs/research_shadow"),
            asian_drift_report_path=asian_report,
            mgc_5m_candles_path=mgc,
            mnq_5m_candles_path=mnq,
            safe_state_path=safe,
        ),
        now=now(),
    )

    latest_late = Path(report["latest_late_join_asian_drift_shadow_path"])
    latest_gap = Path(report["latest_gap_drift_continuation_shadow_path"])
    assert latest_late.exists()
    assert latest_gap.exists()
    assert json.loads(latest_late.read_text(encoding="utf-8"))["research_only"] is True
    assert Path(report["latest_p0_near_miss_shadow_path"]).exists()
    assert Path(report["late_join_asian_drift_shadow_events_path"]).read_text(encoding="utf-8").strip()


def test_p0_near_miss_grades_strict_signal_as_a_grade() -> None:
    result = build_p0_near_miss_shadow(
        evaluated_strategies=[
            {
                "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                "decision": "LONG",
                "signal_emitted": True,
                "signal_direction": "LONG",
                "rule_conditions": {"session_allowed": True, "bull_snap_body_ok": True},
            }
        ],
        source_report_path=Path("cycle.json"),
        now=now(),
    )

    row = result["strategies"][0]
    assert row["entry_grade"] == P0_ENTRY_GRADE_A
    assert row["hypothetical_direction"] == "LONG"
    assert row["submit_allowed"] is False
    assert result["entry_grade_summary"]["a_grade_count"] == 1


def test_p0_near_miss_grades_b_candidate_when_directional_pressure_is_strong() -> None:
    result = build_p0_near_miss_shadow(
        evaluated_strategies=[
            {
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "decision": "NO_SIGNAL",
                "signal_emitted": False,
                "rule_conditions": {
                    "allow_asia": True,
                    "asia_early_or_gc_mgc_london_open": False,
                    "breakout_bar_expansion_is_normal": False,
                    "breakout_bar_slope_is_flat": False,
                    "breakout_breaks_prior_1_high": True,
                    "no_first_bull_snap_turn": True,
                    "prior_bars_since_long_setup_gt_anti_churn": True,
                    "signal_retests_and_holds_breakout_level": True,
                },
            }
        ],
        now=now(),
    )

    row = result["strategies"][0]
    assert row["entry_grade"] == P0_ENTRY_GRADE_B
    assert row["directional_confirmation"] is True
    assert row["noncritical_failed_predicates"] == [
        "asia_early_or_gc_mgc_london_open",
        "breakout_bar_expansion_is_normal",
        "breakout_bar_slope_is_flat",
    ]
    assert row["critical_failed_predicates"] == []
    assert result["b_grade_candidate_count"] == 1
    assert result["submit_allowed"] is False


def test_p0_near_miss_grades_c_when_critical_directional_predicates_fail() -> None:
    result = build_p0_near_miss_shadow(
        evaluated_strategies=[
            {
                "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "decision": "NO_SIGNAL",
                "signal_emitted": False,
                "rule_conditions": {
                    "bear_snap_body_ok": False,
                    "bear_snap_close_weak": False,
                    "bear_snap_location_ok": True,
                    "bear_snap_range_ok": False,
                    "bear_snap_raw": False,
                    "bear_snap_turn_candidate": False,
                    "first_bear_snap_turn": False,
                    "session_allowed": True,
                },
            }
        ],
        now=now(),
    )

    row = result["strategies"][0]
    assert row["entry_grade"] == P0_ENTRY_GRADE_C
    assert row["directional_confirmation"] is False
    assert "bear_snap_body_ok" in row["critical_failed_predicates"]
    assert result["b_grade_candidate_count"] == 0
