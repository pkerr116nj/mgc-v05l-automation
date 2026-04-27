from __future__ import annotations

from datetime import datetime

from mgc_v05l.app.atp_companion_asia_drift_enhancement_durability_review import (
    _add_event_detail_rows,
    _baseline_confirmation_rows,
    _classify,
    _concentration_rows,
)


def _candidate_row(
    *,
    trade_id: str,
    add_pnl_cash: float,
    session: str = "ASIA",
    added: bool = True,
    trade_pnl_cash: float = 25.0,
    pnl_cash: float = 30.0,
) -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "decision_ts": datetime(2025, 1, 10, 19, 30),
        "entry_ts": datetime(2025, 1, 10, 19, 31),
        "add_entry_ts": datetime(2025, 1, 10, 19, 33),
        "exit_ts": datetime(2025, 1, 10, 19, 40),
        "add_exit_ts": datetime(2025, 1, 10, 19, 40),
        "session_segment": session,
        "side": "LONG",
        "added": added,
        "trade_pnl_cash": trade_pnl_cash,
        "add_pnl_cash": add_pnl_cash,
        "pnl_cash": pnl_cash,
        "mae_points": 0.5,
        "mfe_points": 2.0,
        "add_hold_minutes": 7.0,
        "exit_reason": "target",
        "add_reason": "PROMOTION_1_EARNED" if added else "SESSION_NOT_ELIGIBLE",
        "add_price_quality_state": "VWAP_FAVORABLE" if added else None,
    }


def test_add_event_detail_rows_capture_improve_vs_worsen() -> None:
    rows = _add_event_detail_rows(
        [
            _candidate_row(trade_id="a", add_pnl_cash=10.0, pnl_cash=35.0),
            _candidate_row(trade_id="b", add_pnl_cash=-3.5, pnl_cash=21.5),
        ]
    )

    assert len(rows) == 2
    assert rows[0]["improved_trade"] is True
    assert rows[0]["worsened_trade"] is False
    assert rows[1]["improved_trade"] is False
    assert rows[1]["worsened_trade"] is True


def test_classification_marks_thin_even_when_positive_but_not_overly_concentrated() -> None:
    add_rows = [_candidate_row(trade_id=str(index), add_pnl_cash=value) for index, value in enumerate([20.0, 18.0, 15.0, 12.0, 10.0, 8.0, 7.0, -2.0, -1.0, -0.5], start=1)]
    detail_rows = _add_event_detail_rows(add_rows)
    concentration = _concentration_rows(detail_rows)

    classification = _classify(
        add_detail_rows=detail_rows,
        concentration_rows=concentration,
        total_add_pnl=sum(float(row["add_contribution_cash"]) for row in detail_rows),
    )

    assert classification == "ENHANCEMENT_PROMISING_BUT_THIN"


def test_baseline_confirmation_rows_require_unchanged_us_contribution() -> None:
    payload = {
        "results": [
            {
                "baseline_rows": [
                    {"session_segment": "ASIA", "pnl_cash": 10.0},
                    {"session_segment": "US", "pnl_cash": 20.0},
                ],
                "candidate_rows": [
                    {"session_segment": "ASIA", "pnl_cash": 12.0},
                    {"session_segment": "US", "pnl_cash": 20.0},
                ],
            }
        ]
    }

    notes = _baseline_confirmation_rows(payload)

    assert "Total trade count unchanged: 2 baseline vs 2 candidate." in notes[0]
    assert "U.S. contribution unchanged: 20.0 baseline vs 20.0 candidate." in notes[1]
