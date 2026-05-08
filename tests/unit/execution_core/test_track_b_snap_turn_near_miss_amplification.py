from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_snap_turn_near_miss_amplification import (
    TrackBSnapTurnNearMissAmplificationConfig,
    TrackBSnapTurnLocationVariantExitSensitivityConfig,
    TrackBSnapTurnLocationVariantResearchConfig,
    TrackBSnapTurnReplayBackfillConfig,
    create_track_b_snap_turn_near_miss_amplification,
    create_track_b_snap_turn_location_variant_exit_sensitivity,
    create_track_b_snap_turn_location_variant_research_replay,
    create_track_b_snap_turn_replay_backfill,
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
            scorable_snapshots_jsonl=tmp_path / "snapshots.jsonl",
            latest_scorable_snapshots_json=tmp_path / "latest_snapshots.json",
        ),
        now=datetime(2026, 5, 7, 14, 10, tzinfo=UTC),
    )

    strategy = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1")
    assert strategy["evaluated_completed_bars_total"] == 1
    assert strategy["eligible_completed_bars_evaluated"] == 1
    assert strategy["denominator_validation"]["session_phase_ready_bars"] == 1
    assert strategy["hard_signals"] == 0
    assert strategy["one_predicate_away"] == 1
    assert strategy["closest_failed_bars"][0]["primitive_failed_predicates_count"] == 1
    predicate = strategy["dominant_failed_primitive_predicates"][0]
    assert predicate["predicate"] == "bull_snap_close_strong"
    assert predicate["classification"] == "VARIANT_CANDIDATE"
    assert predicate["numeric_distance"]["average_pass_margin_points_or_units"] == "-0.02"
    assert predicate["subsequent_excursion_after_near_misses"]["average_mfe_points"] == "5"
    assert predicate["subsequent_excursion_after_near_misses"]["average_mae_points"] == "-0.5"
    assert result.report["conclusion"]["posture"] == "SNAP_TURN_VARIANT_CANDIDATE_FOUND"
    assert "EVIDENCE_RETENTION_REPAIRED" in result.report["output_classifications"]
    latest = json.loads((tmp_path / "latest_snapshots.json").read_text(encoding="utf-8"))
    assert latest["snapshot_count"] == 1
    predicates = {item["predicate"]: item for item in latest["snapshots"][0]["primitive_predicates"]}
    assert predicates["bull_snap_close_strong"]["pass_margin"] == "-0.02"
    assert latest["snapshots"][0]["future_excursion"]["mfe_points"] == "5"
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


def test_snap_turn_audit_validates_eligible_denominator_separately(tmp_path: Path) -> None:
    inactive_event = _write_json(
        tmp_path / "events" / "inactive.json",
        {
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "candle_timestamp": "2026-05-07T13:00:00+00:00",
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.5",
            "metadata": {
                "feature_diagnostics": {"atr": "2"},
                "first_bull_snap_turn_state": {
                    "session_allowed": False,
                    "derivative_phase": "OFF_SESSION",
                    "prior_bars_since_bull_snap_gt_cooldown": True,
                },
                "first_bull_snap_turn_features": {
                    "bull_snap_downside_stretch_ok": True,
                    "bull_snap_range_ok": True,
                    "bull_snap_body_ok": True,
                    "bull_snap_close_strong": True,
                    "bull_snap_velocity_ok": True,
                    "bull_snap_reversal_bar": True,
                    "bull_snap_location_ok": True,
                    "bull_snap_raw": True,
                    "bull_snap_turn_candidate": True,
                    "first_bull_snap_turn": False,
                },
            },
        },
    )
    _runtime_report(
        tmp_path / "runtime" / "cycle-1",
        {
            "generated_at": "2026-05-07T13:00:01+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                    "input_event_path": str(inactive_event),
                    "rule_conditions": {"session_allowed": False, "first_bull_snap_turn": False},
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
        now=datetime(2026, 5, 7, 13, 5, tzinfo=UTC),
    )

    strategy = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1")
    assert strategy["evaluated_completed_bars_total"] == 1
    assert strategy["eligible_completed_bars_evaluated"] == 0
    assert strategy["hard_signal_rate_eligible"] is None
    assert strategy["frequency_classification"] == "METHODOLOGY_INCONCLUSIVE"
    assert strategy["denominator_validation"]["ineligible_reason_counts"][0]["reason"] == "SESSION_OR_PHASE_FILTER_INACTIVE"


def test_snap_turn_audit_dedupes_monitor_repeats_by_completed_bar_and_preserves_signal(tmp_path: Path) -> None:
    event_path = _bull_event(tmp_path / "events" / "bull.json", timestamp="2026-05-07T14:00:00+00:00")
    _runtime_report(
        tmp_path / "runtime" / "cycle-1",
        {
            "generated_at": "2026-05-07T14:00:01+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "decision": "LONG",
                    "signal_emitted": True,
                    "input_event_path": str(event_path),
                    "rule_conditions": {"first_bull_snap_turn": True},
                }
            ],
        },
    )
    _runtime_report(
        tmp_path / "runtime" / "cycle-2",
        {
            "generated_at": "2026-05-07T14:00:05+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                    "input_event_path": str(event_path),
                    "rule_conditions": {"first_bull_snap_turn": False},
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
        now=datetime(2026, 5, 7, 14, 5, tzinfo=UTC),
    )

    strategy = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1")
    assert result.report["completed_decision_strategy_rows_after_dedup"] == 1
    assert strategy["evaluated_completed_bars_total"] == 1
    assert strategy["hard_signals"] == 1


def test_snap_turn_audit_marks_missing_envelope_as_replay_backfill_required(tmp_path: Path) -> None:
    missing_event = tmp_path / "events" / "rotated.json"
    _runtime_report(
        tmp_path / "runtime" / "cycle-1",
        {
            "generated_at": "2026-05-07T14:00:01+00:00",
            "evaluated_strategies": [
                {
                    "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                    "decision": "NO_SIGNAL",
                    "signal_emitted": False,
                    "input_event_path": str(missing_event),
                    "rule_conditions": {"first_bull_snap_turn": False},
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
            scorable_snapshots_jsonl=tmp_path / "snapshots.jsonl",
            latest_scorable_snapshots_json=tmp_path / "latest_snapshots.json",
        ),
        now=datetime(2026, 5, 7, 14, 5, tzinfo=UTC),
    )

    assert result.report["scorable_snapshot_retention"]["feature_envelope_missing_is_product_defect"] is True
    assert result.report["scorable_snapshot_retention"]["replay_backfill_required"] is True
    assert "REPLAY_BACKFILL_REQUIRED" in result.report["output_classifications"]
    latest = json.loads((tmp_path / "latest_snapshots.json").read_text(encoding="utf-8"))
    assert latest["snapshots"][0]["feature_envelope_missing_is_product_defect"] is True
    assert latest["snapshots"][0]["no_signal_reason"] == "FEATURE_ENVELOPE_MISSING_REPLAY_BACKFILL_REQUIRED"


def test_snap_turn_replay_backfill_reconstructs_dense_scorable_snapshots(tmp_path: Path) -> None:
    candles = []
    for index in range(10):
        base = 100 + index
        candles.append(
            {
                "candle_timestamp": f"2026-05-07T14:{index * 5:02d}:00+00:00",
                "timestamp": f"2026-05-07T14:{index * 5:02d}:00+00:00",
                "open": str(base),
                "high": str(base + 2),
                "low": str(base - 1),
                "close": str(base + 1),
                "volume": "100",
                "completed": True,
            }
        )
    mgc_payload = _write_json(tmp_path / "mgc_5m.json", {"candles": candles, "instrument_family": "MGC"})

    result = create_track_b_snap_turn_replay_backfill(
        config=TrackBSnapTurnReplayBackfillConfig(
            repo_root=tmp_path,
            mgc_candle_payloads=(mgc_payload,),
            mnq_candle_payloads=(),
            replay_envelope_root=tmp_path / "replay_envelopes",
            output_json=tmp_path / "replay.json",
            output_md=tmp_path / "replay.md",
            scorable_snapshots_jsonl=tmp_path / "replay_snapshots.jsonl",
            latest_scorable_snapshots_json=tmp_path / "latest_replay_snapshots.json",
            min_window_bars=8,
        ),
        now=datetime(2026, 5, 7, 15, 0, tzinfo=UTC),
    )

    assert result.report["broker_commands_invoked"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False
    assert result.report["instrument_reconstruction"]["MGC"]["replay_windows_attempted"] == 3
    assert result.report["instrument_reconstruction"]["MGC"]["strategy_rows_reconstructed"] == 6
    bull = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1")
    bear = next(row for row in result.report["strategies"] if row["strategy_id"] == "FIRST_BEAR_SNAP_TURN_V1")
    assert bull["evaluated_completed_bars_total"] == 3
    assert bear["evaluated_completed_bars_total"] == 3
    latest = json.loads((tmp_path / "latest_replay_snapshots.json").read_text(encoding="utf-8"))
    assert latest["snapshot_count"] == 6
    assert latest["numeric_distance_available_count"] > 0


def test_location_variant_research_replay_promotes_only_to_replay_candidate(tmp_path: Path) -> None:
    snapshots = []
    candles = []
    for index in range(24):
        timestamp = f"2026-05-07T14:{index * 5:02d}:00+00:00"
        entry = 100 - index
        candles.append(
            {
                "candle_timestamp": timestamp,
                "timestamp": timestamp,
                "open": str(entry),
                "high": str(entry + 0.2),
                "low": str(entry - 2.0),
                "close": str(entry - 1.0),
                "volume": "100",
                "completed": True,
            }
        )
        if index < 21:
            snapshots.append(
                {
                    "decision_bar_timestamp": timestamp,
                    "instrument": "MNQ",
                    "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "side": "SHORT",
                    "session": "US",
                    "regime": "DOWNSLOPE",
                    "eligibility": {"eligible": True},
                    "feature_envelope_available": True,
                    "hard_signal": False,
                    "result": "NO_SIGNAL",
                    "bar_ohlc": {"open": str(entry), "high": str(entry + 1), "low": str(entry - 1), "close": str(entry)},
                    "proposed_entry_price": str(entry),
                    "near_miss_bucket": "ONE_PREDICATE_AWAY",
                    "failed_primitive_predicates": ["bear_snap_location_ok"],
                    "primitive_predicates": [
                        {"predicate": "bear_snap_range_ok", "threshold": "1", "actual": "2", "pass_margin": "1"},
                        {"predicate": "bear_snap_location_ok", "threshold": "1", "actual": "0", "pass_margin": "-1"},
                    ],
                }
            )
    payload = _write_json(tmp_path / "mnq_5m.json", {"instrument_family": "MNQ", "candles": candles})
    snapshots_json = _write_json(
        tmp_path / "snapshots.json",
        {"snapshots": snapshots},
    )

    result = create_track_b_snap_turn_location_variant_research_replay(
        config=TrackBSnapTurnLocationVariantResearchConfig(
            repo_root=tmp_path,
            refresh_replay_backfill=False,
            replay_backfill_config=TrackBSnapTurnReplayBackfillConfig(
                repo_root=tmp_path,
                mgc_candle_payloads=(),
                mnq_candle_payloads=(payload,),
            ),
            snapshots_json=snapshots_json,
            output_json=tmp_path / "research.json",
            output_md=tmp_path / "research.md",
            minimum_sample_count=20,
        ),
        now=datetime(2026, 5, 7, 16, 0, tzinfo=UTC),
    )

    assert result.report["candidate_status"] == "RESEARCH_ONLY"
    assert result.report["paper_eligible"] is False
    assert result.report["production_thresholds_changed"] is False
    assert result.report["sample_count"] == 21
    assert result.report["classification"] == "PROMOTE_TO_REPLAY_CANDIDATE"
    assert result.report["policy_summary"]["time_boxed_3x5m"]["average_r"] is not None
    assert result.report["submit_cancel_place_order_invoked"] is False


def test_location_variant_exit_sensitivity_keeps_quick_scalp_research_only(tmp_path: Path) -> None:
    samples = []
    for index, quick_r in enumerate(("0.5", "0.5", "-1", "0.5")):
        samples.append(
            {
                "timestamp": f"2026-05-07T14:{index * 5:02d}:00+00:00",
                "session": "US",
                "regime": "DOWNSLOPE",
                "mfe_points": "1",
                "mae_points": "-1",
                "mfe_occurred_before_mae": True,
                "time_to_mfe_bars": 1,
                "time_to_mae_bars": 2,
                "bar_excursions": {
                    "1": {"available": True, "close_excursion_r": "0.25", "mfe_r": "0.5", "mae_r": "-0.1"},
                    "2": {"available": True, "close_excursion_r": "-0.2", "mfe_r": "0.5", "mae_r": "-0.8"},
                    "3": {"available": True, "close_excursion_r": "-0.3", "mfe_r": "0.5", "mae_r": "-1"},
                    "5": {"available": True, "close_excursion_r": "-0.4", "mfe_r": "0.5", "mae_r": "-1"},
                },
                "early_favorable_move_before_failing": True,
                "loser_failed_immediately": False,
                "policies": {
                    "target_1r_stop_1r": {"outcome": "STOP", "r": "-1", "bars": 2},
                    "target_1_5r_stop_1r": {"outcome": "STOP", "r": "-1", "bars": 2},
                    "time_boxed_3x5m": {"outcome": "TIME_BOX_EXIT", "r": "-0.3", "bars": 3},
                    "quick_scalp_0_5r_stop_1r": {"outcome": "TARGET", "r": quick_r, "bars": 1},
                    "breakeven_after_0_5r": {"outcome": "BREAKEVEN_STOP", "r": "0", "bars": 2},
                    "trail_after_first_favorable_bar": {"outcome": "TRAIL_STOP", "r": "0.1", "bars": 2},
                    "failed_followthrough_exit_1bar": {"outcome": "TIME_BOX_EXIT", "r": "-0.3", "bars": 3},
                    "failed_followthrough_exit_2bar": {"outcome": "FAILED_FOLLOW_THROUGH_2BAR_EXIT", "r": "-0.2", "bars": 2},
                    "vol_scaled_0_75r_target_0_75r_stop": {"outcome": "STOP", "r": "-0.75", "bars": 2},
                    "vwap_ema_invalidation_exit": {"outcome": "NOT_AVAILABLE", "r": None},
                    "time_stop_no_favorable_1bar": {"outcome": "TIME_BOX_EXIT", "r": "-0.3", "bars": 3},
                },
            }
        )
    research_json = _write_json(
        tmp_path / "research.json",
        {
            "candidate_name": "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1",
            "classification": "REJECT_FALSE_POSITIVE",
            "samples": samples,
        },
    )

    result = create_track_b_snap_turn_location_variant_exit_sensitivity(
        config=TrackBSnapTurnLocationVariantExitSensitivityConfig(
            repo_root=tmp_path,
            refresh_research_replay=False,
            research_replay_json=research_json,
            output_json=tmp_path / "exit_sensitivity.json",
            output_md=tmp_path / "exit_sensitivity.md",
            minimum_sample_count=4,
        ),
        now=datetime(2026, 5, 7, 17, 0, tzinfo=UTC),
    )

    assert result.report["candidate_status"] == "RESEARCH_ONLY"
    assert result.report["paper_eligible"] is False
    assert result.report["classification"] == "SCALP_ONLY_CANDIDATE"
    assert result.report["best_policy"]["policy"] == "quick_scalp_0_5r_stop_1r"
    assert result.report["production_thresholds_changed"] is False
    assert result.report["submit_cancel_place_order_invoked"] is False
