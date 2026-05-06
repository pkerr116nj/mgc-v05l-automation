from __future__ import annotations

from mgc_v05l.execution_core.track_b_strategy_registry import (
    TrackBStrategyRegistryEntry,
    TrackBStrategyRegistryVerdict,
    arbitrate_track_b_strategy_candidates,
    get_track_b_strategy_registry,
    resolve_track_b_strategy_registry_entry,
    validate_strategy_event_against_registry,
    validate_track_b_strategy_registry,
)


def test_registry_contains_only_live_money_disabled_entries() -> None:
    blockers = validate_track_b_strategy_registry()

    assert blockers == []
    assert get_track_b_strategy_registry()
    assert all(entry.live_money_eligible is False for entry in get_track_b_strategy_registry())


def test_registry_rejects_missing_required_metadata() -> None:
    blockers = validate_track_b_strategy_registry(
        (
            TrackBStrategyRegistryEntry(
                strategy_id="",
                rule_mode="ASIAN_DRIFT_V1",
                instrument_family="MGC",
                timeframe="5m",
                required_feature_schema=(),
                required_state_schema=(),
                feature_version="",
                calibration_profile="recovery_confirmed",
                paper_eligible=True,
                live_money_eligible=True,
            ),
        )
    )

    assert any("strategy_id" in blocker for blocker in blockers)
    assert any("feature_version" in blocker for blocker in blockers)
    assert any("live_money_eligible=false" in blocker for blocker in blockers)


def test_unregistered_strategy_is_rejected() -> None:
    entry, blocker = validate_strategy_event_against_registry(
        event={"strategy_id": "unknown_strategy"},
        rule_mode="ASIAN_DRIFT_V1",
        rule_id="unknown_rule",
    )

    assert entry is None
    assert blocker is not None
    assert "not registered" in blocker


def test_unregistered_rule_mode_is_rejected() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="ATP_STAGED_ADD_V1",
        rule_id="ATP_STAGED_ADD_V1",
        strategy_id="ATP_STAGED_ADD_V1",
    )

    assert entry is None


def test_asia_early_pause_resume_short_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert "metadata.asia_early_pause_resume_short_features.normalized_curvature" in entry.required_feature_schema
    assert "metadata.asia_early_pause_resume_short_state.derivative_phase" in entry.required_state_schema


def test_asia_early_normal_breakout_retest_hold_long_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert "metadata.asia_early_normal_breakout_retest_hold_long_features.breakout_bar_slope_is_flat" in entry.required_feature_schema
    assert "metadata.asia_early_normal_breakout_retest_hold_long_state.asia_early_or_gc_mgc_london_open" in entry.required_state_schema


def test_first_bull_snap_turn_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="FIRST_BULL_SNAP_TURN_V1",
        rule_id="FIRST_BULL_SNAP_TURN_V1",
        strategy_id="FIRST_BULL_SNAP_TURN_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.feature_version == "first_bull_snap_turn_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.first_bull_snap_turn_features.first_bull_snap_turn" in entry.required_feature_schema
    assert "metadata.first_bull_snap_turn_state.session_allowed" in entry.required_state_schema


def test_first_bear_snap_turn_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="FIRST_BEAR_SNAP_TURN_V1",
        rule_id="FIRST_BEAR_SNAP_TURN_V1",
        strategy_id="FIRST_BEAR_SNAP_TURN_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.feature_version == "first_bear_snap_turn_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.first_bear_snap_turn_features.first_bear_snap_turn" in entry.required_feature_schema
    assert "metadata.first_bear_snap_turn_state.session_allowed" in entry.required_state_schema


def test_london_late_pause_resume_short_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        rule_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.feature_version == "london_late_pause_resume_short_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.london_late_pause_resume_short_features.normalized_slope" in entry.required_feature_schema
    assert "metadata.london_late_pause_resume_short_state.derivative_phase" in entry.required_state_schema


def test_asia_late_flat_pullback_pause_resume_long_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.feature_version == "asia_late_flat_pullback_pause_resume_long_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.asia_late_flat_pullback_pause_resume_long_features.bull_snap_close_strong" in entry.required_feature_schema
    assert "metadata.asia_late_flat_pullback_pause_resume_long_state.derivative_phase" in entry.required_state_schema


def test_us_derivative_bear_turn_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="US_DERIVATIVE_BEAR_TURN_V1",
        rule_id="US_DERIVATIVE_BEAR_TURN_V1",
        strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.evaluation_mode == "COMPLETED_BAR_ONLY"
    assert entry.feature_version == "us_derivative_bear_turn_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.us_derivative_bear_turn_features.normalized_slope" in entry.required_feature_schema
    assert "metadata.us_derivative_bear_turn_state.derivative_bear_phase_ok" in entry.required_state_schema


def test_us_late_pause_resume_long_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="US_LATE_PAUSE_RESUME_LONG_V1",
        rule_id="US_LATE_PAUSE_RESUME_LONG_V1",
        strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MGC"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.evaluation_mode == "COMPLETED_BAR_ONLY"
    assert entry.feature_version == "us_late_pause_resume_long_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.us_late_pause_resume_long_features.signal_ema_location_ok" in entry.required_feature_schema
    assert "metadata.us_late_pause_resume_long_state.session_us_late" in entry.required_state_schema


def test_mnq_first_bear_snap_turn_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        rule_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MNQ"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.evaluation_mode == "COMPLETED_BAR_ONLY"
    assert entry.required_1m_context_bars == 40
    assert entry.required_5m_context_bars == 8
    assert entry.feature_version == "mnq_first_bear_snap_turn_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.mnq_first_bear_snap_turn_features.first_bear_snap_turn" in entry.required_feature_schema
    assert "metadata.mnq_first_bear_snap_turn_state.session_allowed" in entry.required_state_schema


def test_mnq_first_bull_snap_turn_registry_metadata_is_valid() -> None:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode="MNQ_FIRST_BULL_SNAP_TURN_V1",
        rule_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
    )

    assert entry is not None
    assert entry.instrument_family == "MNQ"
    assert entry.timeframe == "5m"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False
    assert entry.evaluation_mode == "COMPLETED_BAR_ONLY"
    assert entry.required_1m_context_bars == 40
    assert entry.required_5m_context_bars == 8
    assert entry.feature_version == "mnq_first_bull_snap_turn_v1_phase1"
    assert entry.calibration_profile == "probationary_baseline_v1"
    assert "metadata.mnq_first_bull_snap_turn_features.first_bull_snap_turn" in entry.required_feature_schema
    assert "metadata.mnq_first_bull_snap_turn_state.session_allowed" in entry.required_state_schema


def test_missing_required_state_fields_are_not_ready() -> None:
    entry, blocker = validate_strategy_event_against_registry(
        event={
            "strategy_id": "asian_drift_v1",
            "feature_version": "asia_drift_v1_phase1",
            "calibration_profile": "recovery_confirmed",
        },
        rule_mode="ASIAN_DRIFT_V1",
        rule_id="asian_drift_v1",
    )

    assert entry is not None
    assert blocker is not None
    assert "NOT_READY" in blocker
    assert "asia_drift_state" in blocker
    assert "hypothetical_entry_ready" in blocker


def test_multi_strategy_arbitration_selects_one_and_suppresses_non_paper_candidate() -> None:
    result = arbitrate_track_b_strategy_candidates(
        (
            {
                "strategy_id": "asian_drift_v1",
                "signal_emitted": True,
                "signal_side": "LONG",
                "paper_eligible": True,
            },
            {
                "strategy_id": "diagnostic_observer",
                "signal_emitted": True,
                "signal_side": "LONG",
                "paper_eligible": False,
            },
        )
    )

    assert result["strategy_arbitration_verdict"] == TrackBStrategyRegistryVerdict.READY.value
    assert result["chosen_candidate"]["strategy_id"] == "asian_drift_v1"
    assert result["paper_candidate_count"] == 1
    assert [item["strategy_id"] for item in result["suppressed_candidates"]] == ["diagnostic_observer"]


def test_multi_strategy_arbitration_blocks_multiple_paper_candidates() -> None:
    result = arbitrate_track_b_strategy_candidates(
        (
            {"strategy_id": "asian_drift_v1", "signal_emitted": True, "signal_direction": "LONG", "paper_eligible": True},
            {"strategy_id": "mgc_ema_momentum_reclaim_long_v1", "signal_emitted": True, "signal_direction": "LONG", "paper_eligible": True},
        )
    )

    assert result["strategy_arbitration_verdict"] == TrackBStrategyRegistryVerdict.BLOCKED_MULTIPLE_PAPER_CANDIDATES.value
    assert result["chosen_candidate"] is None
    assert result["paper_candidate_count"] == 2


def test_conflicting_signals_require_explicit_arbitration() -> None:
    result = arbitrate_track_b_strategy_candidates(
        (
            {"strategy_id": "asian_drift_v1", "signal_emitted": True, "signal_direction": "LONG", "paper_eligible": True},
            {"strategy_id": "other_registered_future", "signal_emitted": True, "signal_direction": "SHORT", "paper_eligible": True},
        )
    )

    assert result["strategy_arbitration_verdict"] == TrackBStrategyRegistryVerdict.BLOCKED_CONFLICTING_SIGNALS.value
    assert result["chosen_candidate"] is None
    assert "explicit arbitration" in result["primary_blocker"]
