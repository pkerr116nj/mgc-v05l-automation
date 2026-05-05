from __future__ import annotations

from mgc_v05l.execution_core.track_b_strategy_registry import (
    TrackBStrategyRegistryEntry,
    TrackBStrategyRegistryVerdict,
    arbitrate_track_b_strategy_candidates,
    get_track_b_strategy_registry,
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
