from __future__ import annotations

from pathlib import Path

from mgc_v05l.research.asia_drift.probabilistic_pass7 import _build_comparison_rows, _load_state_lookup


def test_load_state_lookup_maps_pass6_classes_to_decision_states(tmp_path: Path) -> None:
    csv_path = tmp_path / "classification.csv"
    csv_path.write_text(
        "environment_key,classification\n"
        "A,EDGE_ON\n"
        "B,EDGE_OFF\n"
        "C,MIXED\n",
        encoding="utf-8",
    )

    lookup = _load_state_lookup(csv_path)
    assert lookup["A"] == "TRADE_FAVORABLE"
    assert lookup["B"] == "DO_NOT_TRADE"
    assert lookup["C"] == "TRADE_NEUTRAL"


def test_build_comparison_rows_splits_all_favorable_and_excluded_sets() -> None:
    rows = [
        {"sample_split": "holdout", "decision_state": "TRADE_FAVORABLE", "forward_return_60m": 5.0, "mae_60m_points": 2.0, "instrument": "ES"},
        {"sample_split": "holdout", "decision_state": "DO_NOT_TRADE", "forward_return_60m": -4.0, "mae_60m_points": 7.0, "instrument": "ES"},
        {"sample_split": "holdout", "decision_state": "TRADE_NEUTRAL", "forward_return_60m": 1.0, "mae_60m_points": 3.0, "instrument": "NQ"},
    ]

    comparison_rows = _build_comparison_rows(rows)
    keyed = {(row["sample_split"], row["scenario"]): row for row in comparison_rows}
    assert keyed[("holdout", "ALL_TRADES")]["row_count"] == 3
    assert keyed[("holdout", "TRADE_FAVORABLE_ONLY")]["row_count"] == 1
    assert keyed[("holdout", "EXCLUDE_DO_NOT_TRADE")]["row_count"] == 2
