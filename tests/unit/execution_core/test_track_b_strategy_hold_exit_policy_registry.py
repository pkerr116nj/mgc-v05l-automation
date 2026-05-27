from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES,
    STRATEGY_HOLD_EXIT_POLICY_INVALID,
    STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY,
    STRATEGY_HOLD_EXIT_POLICY_VALID,
    StrategyHoldExitPolicyRegistryConfig,
    build_strategy_hold_exit_policy_registry_audit,
    strategy_hold_exit_policy_for,
    validate_strategy_hold_exit_policy,
)


NOW = datetime(2026, 5, 27, 15, 0, tzinfo=UTC)


def test_every_guarded_paper_strategy_has_hold_exit_policy_mapping(tmp_path: Path) -> None:
    strategy_ids = list(APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES)
    _write_json(tmp_path / "config/track_b_guarded_paper_roster.json", {"enabled_strategy_ids": strategy_ids})

    payload = build_strategy_hold_exit_policy_registry_audit(
        config=StrategyHoldExitPolicyRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY
    assert payload["strategy_count"] == len(strategy_ids)
    assert payload["valid_strategy_count"] == len(strategy_ids)
    assert payload["gap_strategy_count"] == 0
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["lifecycle_authority"] is False


def test_missing_mapping_is_flagged(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {"enabled_strategy_ids": ["UNKNOWN_TRACK_B_STRATEGY"]},
    )

    payload = build_strategy_hold_exit_policy_registry_audit(
        config=StrategyHoldExitPolicyRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] != STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY
    assert payload["gap_strategy_count"] == 1
    assert payload["strategies"][0]["missing_metadata"] == ["strategy_hold_exit_policy_mapping"]


def test_policy_validation_rejects_authority_flags() -> None:
    policy = dict(strategy_hold_exit_policy_for("asian_drift_v1") or {})
    policy["submit_allowed"] = True

    result = validate_strategy_hold_exit_policy(policy)

    assert result["classification"] == STRATEGY_HOLD_EXIT_POLICY_INVALID
    assert "submit_allowed" in result["invalid_metadata"]


def test_strategy_specific_policy_is_readable_for_shadow_engine() -> None:
    policy = strategy_hold_exit_policy_for("MNQ_FIRST_BEAR_SNAP_TURN_V1")

    assert policy is not None
    assert validate_strategy_hold_exit_policy(policy)["classification"] == STRATEGY_HOLD_EXIT_POLICY_VALID
    assert policy["hold_policy_id"] == "SNAP_TURN_QUICK_SCALP_TIMEBOX_SHADOW_V1"
    assert policy["exit_policy_family"] == "TIME_BOX_PLUS_PROFIT_HARVEST_SHADOW"
    assert policy["broker_mutation_allowed"] is False
    assert policy["lifecycle_authority"] is False


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
