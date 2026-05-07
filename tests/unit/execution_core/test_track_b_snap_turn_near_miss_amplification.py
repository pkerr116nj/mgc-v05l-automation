from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_snap_turn_near_miss_amplification import (
    TrackBSnapTurnNearMissAmplificationConfig,
    create_track_b_snap_turn_near_miss_amplification,
)


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _runtime_report(path: Path, payload: dict[str, object]) -> Path:
    return _write_json(path / "track_b_multi_strategy_runtime_cycle_report.json", payload)


def _bull_event(path: Path, *, timestamp: str = "2026-05-07T14:00:00+00:00") -> Path:
    return _write_json(
        path,
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "candle_timestamp": timestamp,
            "open": "99",
            "high": "101",
            "low": "98",
            "close": "100",
            "metadata": {
                "feature_diagnostics": {
                    "atr": "2",
                    "bar_range": "3",
                    "body_size": "1",
                    "close_location": "0.70",
                    "downside_stretch": "3",
                    "turn_ema_slow": "101",
                    "velocity_delta": "0.50",
                    "velocity": "0.3",
                },
                "first_bull_snap_turn_state": {
                    "session_allowed": True,
                    "derivative_phase": "US_OPEN",
                    "prior_bars_since_bull_snap_gt_cooldown": True,
                },
                "first_bull_snap_turn_features": {
                    "bull_snap_downside_stretch_ok": True,
                    "bull_snap_min_downside_stretch_atr": "1.20",
                    "bull_snap_range_ok": True,
                    "bull_snap_range_threshold_atr": "1.00",
                    "bull_snap_body_ok": True,
                    "bull_snap_body_threshold_atr": "0.45",
                    "bull_snap_close_strong": False,
                    "bull_snap_velocity_ok": True,
                    "bull_snap_velocity_threshold_atr": "0.18",
                    "bull_snap_location_ok": True,
                    "bull_snap_reversal_bar": True,
                    "bull_snap_raw": False,
                    "bull_snap_turn_candidate": False,
                    "first_bull_snap_turn": False,
                },
            },
        },
    )


def _future_snap_event(path: Path, *, timestamp: str, close: str = "104") -> Path:
    return _write_json(
        path,
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "candle_timestamp": timestamp,
            "open": "100",
            "high": "105",
            "low": "99.5",
            "close": close,
            "metadata": {},
        },
    )


def test_snap_turn_audit_counts_primitive_near_miss_and_mfe_mae(tmp_path: Path) -> None:
    event_path = _bull_event(tmp_path / "events" / "bull.json")
    _future_snap_event(
        tmp_path / "snap_turn_state" / "future" / "first_bull_snap_turn_event_envelope.json",
        timestamp="2026-05-07T14:05:00+00:00",
    )
    _runtime_report(
        tmp_path / "runtime" / "cycle-1",
        {
            "generated_at": "2026-05-07T14:00:01+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                    "input_event_path": str(event_path),
                    "rule_conditions": {
                        "session_allowed": True,
                        "prior_bars_since_bull_snap_gt_cooldown": True,
                        "bull_snap_downside_stretch_ok": True,
                        "bull_snap_range_ok": True,
                        "bull_snap_body_ok": True,
                        "bull_snap_close_strong": False,
                        "bull_snap_velocity_ok": True,
                        "bull_snap_reversal_bar": True,
                        "bull_snap_location_ok": True,
                        "bull_snap_raw": False,
                        "bull_snap_turn_candidate": False,
                        "first_bull_snap_turn": False,
                    },
                }
            ],
        },
    )

    result = create_track_b_snap_turn_near_miss_amplification(
        config=TrackBSnapTurnNearMissAmplificationConfig(
            repo_root=tmp_path,
            runtime_cycle_root=tmp_path / "runtime",
            snap_turn_root=tmp_path / "snap_turn_state",
            mgc_live_5m=tmp_path / "missing_mgc.json",
            mnq_live_5m=tmp_path / "missing_mnq.json",
            output_json=tmp_path / "diagnostic.json",
            output_md=tmp_path / "diagnostic.md",
        ),
        now=datetime(2026, 5, 7, 14, 10, tzinfo=UTC),
    )

    strategy = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1")
    assert strategy["eligible_completed_bars_evaluated"] == 1
    assert strategy["hard_signals"] == 0
    assert strategy["one_predicate_away"] == 1
    predicate = strategy["dominant_failed_primitive_predicates"][0]
    assert predicate["predicate"] == "bull_snap_close_strong"
    assert predicate["classification"] == "VARIANT_CANDIDATE"
    assert predicate["numeric_distance"]["average_pass_margin_points_or_units"] == "-0.02"
    assert predicate["subsequent_excursion_after_near_misses"]["average_mfe_points"] == "5"
    assert predicate["subsequent_excursion_after_near_misses"]["average_mae_points"] == "-0.5"
    assert result.report["paper_proof_cli_invoked"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False


def test_snap_turn_audit_counts_hard_signals_without_broker_routes(tmp_path: Path) -> None:
    event_path = _write_json(
        tmp_path / "events" / "bear.json",
        {
            "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
            "instrument_family": "MNQ",
            "candle_timestamp": "2026-05-07T15:00:00+00:00",
            "open": "200",
            "high": "202",
            "low": "197",
            "close": "198",
            "metadata": {
                "feature_diagnostics": {"atr": "5", "velocity": "-1"},
                "mnq_first_bear_snap_turn_state": {
                    "session_allowed": True,
                    "prior_bars_since_bear_snap_gt_cooldown": True,
                },
                "mnq_first_bear_snap_turn_features": {"first_bear_snap_turn": True},
            },
        },
    )
    _runtime_report(
        tmp_path / "runtime" / "cycle-1",
        {
            "generated_at": "2026-05-07T15:00:01+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "decision": "SHORT",
                    "signal_emitted": True,
                    "input_event_path": str(event_path),
                    "rule_conditions": {"first_bear_snap_turn": True},
                }
            ],
        },
    )

    result = create_track_b_snap_turn_near_miss_amplification(
        config=TrackBSnapTurnNearMissAmplificationConfig(
            repo_root=tmp_path,
            runtime_cycle_root=tmp_path / "runtime",
            snap_turn_root=tmp_path / "snap_turn_state",
            output_json=tmp_path / "diagnostic.json",
            output_md=tmp_path / "diagnostic.md",
        ),
        now=datetime(2026, 5, 7, 15, 5, tzinfo=UTC),
    )

    strategy = next(row for row in result.report["strategies"] if row["strategy_id"] == "MNQ_FIRST_BEAR_SNAP_TURN_V1")
    assert strategy["hard_signals"] == 1
    assert strategy["one_predicate_away"] == 0
    assert strategy["two_predicates_away"] == 0
    assert result.report["broker_commands_invoked"] is False

