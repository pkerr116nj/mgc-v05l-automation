from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.strategy_risk_shape_lab import RiskShapeProfile, run_strategy_risk_shape_lab
from mgc_v05l.app.strategy_risk_shape_lab import publish_strategy_risk_shaped_studies


def _write_study(path: Path, trades: list[dict[str, object]]) -> None:
    payload = {
        "contract_version": "strategy_study_v3",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "symbol": "GC",
        "timeframe": "1m",
        "bars": [],
        "trade_events": [],
        "pnl_points": [],
        "execution_slices": [],
        "meta": {
            "standalone_strategy_id": "demo_strategy",
            "strategy_id": "demo_strategy",
            "study_id": "demo_strategy",
            "display_name": "Demo Strategy",
            "symbol": "GC",
            "strategy_family": "active_trend_participation_engine",
            "study_mode": "research_execution_mode",
            "entry_model": "CURRENT_CANDLE_VWAP",
            "active_entry_model": "CURRENT_CANDLE_VWAP",
            "supported_entry_models": ["CURRENT_CANDLE_VWAP"],
            "entry_model_supported": True,
            "execution_model": "TEST",
            "timeframe_truth": {
                "structural_signal_timeframe": "5m",
                "execution_timeframe": "1m",
                "artifact_timeframe": "1m",
                "execution_timeframe_role": "execution_detail_only",
            },
            "truth_provenance": {},
        },
        "standalone_strategy_id": "demo_strategy",
        "strategy_family": "active_trend_participation_engine",
        "summary": {
            "closed_trade_breakdown": trades,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_strategy_risk_shape_lab_applies_daily_peak_drawdown_cap(tmp_path: Path) -> None:
    study_path = tmp_path / "demo.strategy_study.json"
    _write_study(
        study_path,
        [
            {"trade_id": "t1", "entry_timestamp": "2026-01-02T09:00:00-05:00", "exit_timestamp": "2026-01-02T09:15:00-05:00", "realized_pnl": -3000},
            {"trade_id": "t2", "entry_timestamp": "2026-01-02T09:20:00-05:00", "exit_timestamp": "2026-01-02T09:40:00-05:00", "realized_pnl": -2500},
            {"trade_id": "t3", "entry_timestamp": "2026-01-02T10:00:00-05:00", "exit_timestamp": "2026-01-02T10:10:00-05:00", "realized_pnl": 5000},
            {"trade_id": "t4", "entry_timestamp": "2026-01-03T09:00:00-05:00", "exit_timestamp": "2026-01-03T09:15:00-05:00", "realized_pnl": 1000},
        ],
    )

    result = run_strategy_risk_shape_lab(
        study_json_paths=[study_path],
        report_dir=tmp_path / "report",
        profiles=(
            RiskShapeProfile(profile_id="baseline", label="Baseline"),
            RiskShapeProfile(profile_id="peak_5000", label="Daily Peak DD 5K", daily_peak_drawdown_cap=5000.0),
        ),
    )

    payload = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    rows = payload["results"][0]["profiles"]
    baseline = next(row for row in rows if row["profile_id"] == "baseline")
    capped = next(row for row in rows if row["profile_id"] == "peak_5000")

    assert baseline["net_pnl"] == 500.0
    assert baseline["max_drawdown"] == 5500.0
    assert capped["net_pnl"] == -4500.0
    assert capped["halted_day_count"] == 1
    assert capped["skipped_trade_count"] == 1
    assert capped["first_trigger"] == "daily_peak_drawdown_cap"


def test_strategy_risk_shape_lab_applies_equity_drawdown_cooldown(tmp_path: Path) -> None:
    study_path = tmp_path / "demo.strategy_study.json"
    _write_study(
        study_path,
        [
            {"trade_id": "t1", "entry_timestamp": "2026-01-02T09:00:00-05:00", "exit_timestamp": "2026-01-02T09:15:00-05:00", "realized_pnl": 6000},
            {"trade_id": "t2", "entry_timestamp": "2026-01-03T09:00:00-05:00", "exit_timestamp": "2026-01-03T09:15:00-05:00", "realized_pnl": -3000},
            {"trade_id": "t3", "entry_timestamp": "2026-01-04T09:00:00-05:00", "exit_timestamp": "2026-01-04T09:15:00-05:00", "realized_pnl": -2500},
            {"trade_id": "t4", "entry_timestamp": "2026-01-05T09:00:00-05:00", "exit_timestamp": "2026-01-05T09:15:00-05:00", "realized_pnl": 2000},
            {"trade_id": "t5", "entry_timestamp": "2026-01-06T09:00:00-05:00", "exit_timestamp": "2026-01-06T09:15:00-05:00", "realized_pnl": 1500},
        ],
    )

    result = run_strategy_risk_shape_lab(
        study_json_paths=[study_path],
        report_dir=tmp_path / "report",
        profiles=(
            RiskShapeProfile(profile_id="cooldown", label="Cooldown", equity_peak_drawdown_cap=5000.0, cooldown_sessions_after_equity_breach=2),
        ),
    )

    payload = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    row = payload["results"][0]["profiles"][0]

    assert row["first_trigger"] == "equity_peak_drawdown_cap"
    assert row["halted_day_count"] == 1
    assert row["skipped_trade_count"] == 2
    assert row["net_pnl"] == 500.0


def test_publish_strategy_risk_shaped_studies_writes_parallel_artifacts(tmp_path: Path) -> None:
    study_path = tmp_path / "demo.strategy_study.json"
    _write_study(
        study_path,
        [
            {"trade_id": "t1", "family": "atp", "side": "LONG", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T09:00:00-05:00", "exit_timestamp": "2026-01-02T09:15:00-05:00", "entry_price": 100.0, "exit_price": 101.0, "realized_pnl": 100.0, "exit_reason": "target"},
            {"trade_id": "t2", "family": "atp", "side": "LONG", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T10:00:00-05:00", "exit_timestamp": "2026-01-02T10:15:00-05:00", "entry_price": 101.0, "exit_price": 99.0, "realized_pnl": -200.0, "exit_reason": "stop"},
        ],
    )

    result = publish_strategy_risk_shaped_studies(
        study_json_paths=[study_path],
        profile_id="standard_v2",
        report_dir=tmp_path / "report",
        historical_playback_dir=tmp_path / "playback",
    )

    manifest_path = Path(result["historical_playback_manifest_path"])
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest_path.exists()
    assert payload["studies"]
    assert any("risk_shaped_standard_v2" in str(row.get("strategy_id") or "") for row in payload["studies"])


def test_strategy_risk_shape_lab_applies_realized_pnl_scale(tmp_path: Path) -> None:
    study_path = tmp_path / "demo.strategy_study.json"
    _write_study(
        study_path,
        [
            {"trade_id": "t1", "family": "atp", "side": "LONG", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T09:00:00-05:00", "exit_timestamp": "2026-01-02T09:15:00-05:00", "entry_price": 100.0, "exit_price": 101.0, "realized_pnl": 100.0, "exit_reason": "target"},
            {"trade_id": "t2", "family": "atp", "side": "SHORT", "entry_session_phase": "US", "entry_timestamp": "2026-01-03T09:00:00-05:00", "exit_timestamp": "2026-01-03T09:15:00-05:00", "entry_price": 101.0, "exit_price": 102.0, "realized_pnl": -40.0, "exit_reason": "stop"},
        ],
    )

    result = run_strategy_risk_shape_lab(
        study_json_paths=[study_path],
        report_dir=tmp_path / "report",
        profiles=(
            RiskShapeProfile(profile_id="baseline", label="Baseline"),
            RiskShapeProfile(profile_id="budget_50", label="Budget 50%", realized_pnl_scale=0.5),
        ),
    )

    payload = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    rows = payload["results"][0]["profiles"]
    baseline = next(row for row in rows if row["profile_id"] == "baseline")
    scaled = next(row for row in rows if row["profile_id"] == "budget_50")

    assert baseline["net_pnl"] == 60.0
    assert baseline["max_drawdown"] == 40.0
    assert scaled["net_pnl"] == 30.0
    assert scaled["max_drawdown"] == 20.0
    assert scaled["inputs"]["realized_pnl_scale"] == 0.5


def test_strategy_risk_shape_lab_supports_scoped_loser_streaks(tmp_path: Path) -> None:
    study_path = tmp_path / "demo.strategy_study.json"
    _write_study(
        study_path,
        [
            {"trade_id": "t1", "family": "alpha", "side": "LONG", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T09:00:00-05:00", "exit_timestamp": "2026-01-02T09:15:00-05:00", "entry_price": 100.0, "exit_price": 99.0, "realized_pnl": -100.0, "exit_reason": "stop"},
            {"trade_id": "t2", "family": "beta", "side": "SHORT", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T09:20:00-05:00", "exit_timestamp": "2026-01-02T09:35:00-05:00", "entry_price": 99.0, "exit_price": 100.0, "realized_pnl": -100.0, "exit_reason": "stop"},
            {"trade_id": "t3", "family": "beta", "side": "SHORT", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T09:40:00-05:00", "exit_timestamp": "2026-01-02T09:55:00-05:00", "entry_price": 100.0, "exit_price": 101.0, "realized_pnl": -100.0, "exit_reason": "stop"},
            {"trade_id": "t4", "family": "alpha", "side": "LONG", "entry_session_phase": "ASIA", "entry_timestamp": "2026-01-02T10:00:00-05:00", "exit_timestamp": "2026-01-02T10:15:00-05:00", "entry_price": 101.0, "exit_price": 103.0, "realized_pnl": 200.0, "exit_reason": "target"},
        ],
    )

    result = run_strategy_risk_shape_lab(
        study_json_paths=[study_path],
        report_dir=tmp_path / "report",
        profiles=(
            RiskShapeProfile(profile_id="global", label="Global", max_consecutive_losers=2),
            RiskShapeProfile(profile_id="same_side", label="Same Side", max_consecutive_losers=2, max_consecutive_losers_scope="same_side"),
        ),
    )

    payload = json.loads(Path(result["report_json_path"]).read_text(encoding="utf-8"))
    rows = payload["results"][0]["profiles"]
    global_row = next(row for row in rows if row["profile_id"] == "global")
    scoped_row = next(row for row in rows if row["profile_id"] == "same_side")

    assert global_row["first_trigger"] == "max_consecutive_losers"
    assert global_row["skipped_trade_count"] == 2
    assert global_row["net_pnl"] == -200.0
    assert scoped_row["first_trigger"] == "max_consecutive_losers"
    assert scoped_row["skipped_trade_count"] == 1
    assert scoped_row["net_pnl"] == -300.0
    assert scoped_row["inputs"]["max_consecutive_losers_scope"] == "same_side"
