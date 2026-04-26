from __future__ import annotations

import csv
from pathlib import Path

from mgc_v05l.research.asia_drift.position_sizing_monte_carlo import (
    _load_trade_observations,
    _recommend_shadow_risk,
    _risk_multiplier,
    run_position_sizing_monte_carlo,
)


def test_risk_multiplier_halves_nq_only_in_conservative_mode() -> None:
    assert _risk_multiplier("combined_conservative_half_nq", "NQ") == 0.5
    assert _risk_multiplier("combined_conservative_half_nq", "MNQ") == 0.5
    assert _risk_multiplier("combined_conservative_half_nq", "ES") == 1.0
    assert _risk_multiplier("combined_index_basket", "NQ") == 1.0


def test_load_trade_observations_converts_points_to_r(tmp_path: Path) -> None:
    trade_log = tmp_path / "trades.csv"
    with trade_log.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["instrument", "timestamp", "return_points"])
        writer.writeheader()
        writer.writerow({"instrument": "ES", "timestamp": "2026-01-01T00:00:00+00:00", "return_points": "6"})
        writer.writerow({"instrument": "NQ", "timestamp": "2026-01-01T00:05:00+00:00", "return_points": "-20"})
    observations = _load_trade_observations(trade_log)
    assert observations[0].r_multiple == 1.0
    assert observations[1].r_multiple == -1.0


def test_run_position_sizing_monte_carlo_writes_summary(tmp_path: Path) -> None:
    trade_log = tmp_path / "trades.csv"
    with trade_log.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["instrument", "timestamp", "return_points"])
        writer.writeheader()
        for instrument, ret in [
            ("ES", "6"),
            ("MES", "-6"),
            ("NQ", "40"),
            ("MNQ", "-20"),
            ("ES", "3"),
            ("MES", "6"),
            ("NQ", "-10"),
            ("MNQ", "20"),
        ]:
            writer.writerow({"instrument": instrument, "timestamp": "2026-01-01T00:00:00+00:00", "return_points": ret})
    payload = run_position_sizing_monte_carlo(
        trade_log_csv=trade_log,
        output_dir=tmp_path / "out",
        starting_capital=100000.0,
        simulations=20,
        seed=7,
    )
    assert Path(payload["artifacts"]["summary_csv"]).exists()
    assert Path(payload["artifacts"]["drawdown_csv"]).exists()
    assert payload["recommended_shadow_risk_fraction"] in {
        0.0025,
        0.005,
        0.0075,
        0.01,
        0.0125,
        0.015,
        0.02,
    }


def test_recommend_shadow_risk_prefers_highest_safe_conservative_row() -> None:
    rows = [
        {
            "mode": "combined_conservative_half_nq",
            "risk_fraction": 0.005,
            "risk_label": "0.50%",
            "p95_max_drawdown": 0.11,
            "worst_drawdown_observed": 0.22,
        },
        {
            "mode": "combined_conservative_half_nq",
            "risk_fraction": 0.01,
            "risk_label": "1.00%",
            "p95_max_drawdown": 0.14,
            "worst_drawdown_observed": 0.24,
        },
        {
            "mode": "combined_conservative_half_nq",
            "risk_fraction": 0.015,
            "risk_label": "1.50%",
            "p95_max_drawdown": 0.19,
            "worst_drawdown_observed": 0.31,
        },
    ]
    recommendation = _recommend_shadow_risk(rows)
    assert recommendation["risk_fraction"] == 0.01
