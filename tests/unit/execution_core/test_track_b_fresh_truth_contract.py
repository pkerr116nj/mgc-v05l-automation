from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.execution_core.track_b_fresh_truth_contract import (
    EXPIRED_DIAGNOSTIC_ONLY,
    FRESH_AUTHORITY,
    build_authority_freshness_metadata,
    expire_superseded_same_scope_candidates,
    validate_authority_freshness,
)


NOW = datetime(2026, 6, 4, 6, 30, tzinfo=UTC)


def test_stale_heartbeat_cannot_prove_runtime_running() -> None:
    payload = build_authority_freshness_metadata(
        generated_at=NOW - timedelta(minutes=10),
        source_pid=12345,
        ttl_seconds=120,
        authority_scope="runtime_environment_truth",
    )

    decision = validate_authority_freshness(payload, now=NOW, pid_running=lambda _pid: True)

    assert decision.classification == EXPIRED_DIAGNOSTIC_ONLY
    assert decision.fresh is False
    assert "AUTHORITY_TTL_EXPIRED" in decision.reason_codes


def test_absent_pid_cannot_prove_runtime_running_even_with_fresh_timestamp() -> None:
    payload = build_authority_freshness_metadata(
        generated_at=NOW,
        source_pid=12345,
        ttl_seconds=120,
        authority_scope="runtime_environment_truth",
    )

    decision = validate_authority_freshness(payload, now=NOW, pid_running=lambda _pid: False)

    assert decision.classification == EXPIRED_DIAGNOSTIC_ONLY
    assert decision.fresh is False
    assert "SOURCE_PID_ABSENT" in decision.reason_codes


def test_fresh_authority_metadata_passes_contract() -> None:
    payload = build_authority_freshness_metadata(
        generated_at=NOW,
        source_pid=12345,
        ttl_seconds=120,
        authority_scope="runtime_environment_truth",
    )

    decision = validate_authority_freshness(payload, now=NOW, pid_running=lambda _pid: True)

    assert decision.classification == FRESH_AUTHORITY
    assert decision.fresh is True
    assert decision.authority_scope == "runtime_environment_truth"


def test_stale_owner_candidate_expires_before_ambiguity_against_fresh_owner() -> None:
    candidates = [
        {"trade_id": "old", "scope": "DUM882026|MNQM6|770561201|SHORT", "observed_at": NOW - timedelta(days=1)},
        {"trade_id": "fresh", "scope": "DUM882026|MNQM6|770561201|SHORT", "observed_at": NOW},
    ]

    active, expired = expire_superseded_same_scope_candidates(
        candidates,
        scope_key=lambda row: row["scope"],
        observed_at=lambda row: row["observed_at"],
    )

    assert active == [candidates[1]]
    assert expired[0]["classification"] == EXPIRED_DIAGNOSTIC_ONLY
    assert expired[0]["candidate_observed_at"] == (NOW - timedelta(days=1)).isoformat()
