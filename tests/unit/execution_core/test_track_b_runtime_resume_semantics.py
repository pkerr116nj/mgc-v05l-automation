from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_crash_loop_protection import NO_CRASH_LOOP, RESTART_COOLDOWN_ACTIVE
from mgc_v05l.execution_core.track_b_managed_order_registry import NO_MANAGED_ORDERS
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    RESUME_ALLOWED_CLEAN,
    RESUME_BLOCKED_BROKER_EXPOSURE,
    RESUME_BLOCKED_CRASH_LOOP,
    RESUME_BLOCKED_MANAGED_POSITION,
    RESUME_BLOCKED_MARKET_CLOSED,
    RESUME_BLOCKED_OPEN_ORDER,
    RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
    RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE,
    RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE,
    TrackBRuntimeResumeSemanticsConfig,
    build_dashboard_runtime_resume_projection,
    build_track_b_runtime_resume_semantics,
    write_track_b_runtime_resume_semantics,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_proof_ready_allows_clean_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_ALLOWED_CLEAN
    assert payload["allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["safe_to_reuse_previous_runtime_state"] is False
    assert payload["must_start_new_runtime_generation"] is True
    assert payload["read_only"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False


def test_market_closed_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification=MARKET_CLOSED_NO_FRESH_BARS, phase1_reason=MARKET_CLOSED_NO_FRESH_BARS)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_MARKET_CLOSED
    assert payload["allowed"] is False
    assert payload["reason"] == MARKET_CLOSED_NO_FRESH_BARS


def test_active_runtime_blocks_new_resume_but_marks_reuse_safe(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_ACTIVE_TRADE_CAPABLE)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False
    assert payload["safe_to_reuse_previous_runtime_state"] is True
    assert payload["must_start_new_runtime_generation"] is False


def test_broker_exposure_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE, position_classification="ATTENTION_REQUIRED")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_BROKER_EXPOSURE
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False


def test_open_order_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_OPEN_ORDER
    assert payload["blockers"][0]["code"] == "open_order_truth"


def test_managed_position_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, managed_position_classification="OPEN_MANAGED_MATCHED")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_MANAGED_POSITION
    assert payload["blockers"][0]["code"] == "managed_position_registry"


def test_crash_loop_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, crash_loop_classification=RESTART_COOLDOWN_ACTIVE, crash_loop_restart_blocked=True)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_CRASH_LOOP
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False


def test_prior_unsafe_stop_requires_operator_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, previous_broker_safe_at_stop=False)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_OPERATOR_ACK_REQUIRED
    assert payload["required_operator_ack"] is True
    assert payload["previous_broker_safe_at_stop"] is False


def test_missing_evidence_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_agent_health=False)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE
    assert payload["allowed"] is False
    assert any("agent_health" in blocker["detail"] for blocker in payload["blockers"])


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path)
    payload = build_track_b_runtime_resume_semantics(config=config, now=NOW)

    authority_path = write_track_b_runtime_resume_semantics(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    direct_projection = build_dashboard_runtime_resume_projection(authority_payload=payload, authority_path=authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def _seed_base(
    root: Path,
    *,
    proof_classification: str = READY_FOR_PROOF,
    phase1_reason: str = "phase1_runtime_candles_ready",
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    crash_loop_classification: str = NO_CRASH_LOOP,
    crash_loop_restart_blocked: bool = False,
    previous_broker_safe_at_stop: bool = True,
    include_agent_health: bool = True,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {"generated_at": NOW.isoformat(), "classification": proof_classification, "phase1_session_reason": phase1_reason},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json",
        {
            "generated_at": NOW.isoformat(),
            "classifications": {
                "Open Order Truth": open_order_classification,
                "Managed Order Registry": managed_order_classification,
                "Position Truth": position_classification,
                "Runtime Environment Truth": runtime_classification,
                "Managed Position Registry": managed_position_classification,
                "Reconciliation": reconciliation_classification,
                "Broker Truth Lease": broker_lease_classification,
            },
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": runtime_classification},
    )
    if include_agent_health:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "AGENT_HEALTH_READY",
                "agents": [
                    {
                        "agent_id": "track_b_paper_runtime",
                        "category": "runtime",
                        "status": STOPPED_EXPECTED,
                        "reason": runtime_classification,
                        "blocking_for_proof": False,
                        "blocking_for_runtime_submit": False,
                    },
                    {
                        "agent_id": "phase1_databento_live_candles",
                        "category": "market_data",
                        "status": HEALTHY,
                        "reason": phase1_reason,
                        "blocking_for_proof": False,
                        "blocking_for_runtime_submit": False,
                    },
                ],
            },
        )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {"generated_at": NOW.isoformat(), "classification": "RESTART_RUNTIME_ALLOWED", "recommendation": "RESTART_RUNTIME_ALLOWED"},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": crash_loop_classification,
            "restart_blocked": crash_loop_restart_blocked,
            "operator_ack_required": False,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": position_classification,
            "summary": {"overall_classification": position_classification},
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": open_order_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": NOW.isoformat(), "classification": managed_order_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {"generated_at": NOW.isoformat(), "classification": managed_position_classification},
    )
    _write_json(
        root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": reconciliation_classification},
    )
    _write_json(
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": NOW.isoformat(), "classification": broker_lease_classification},
    )
    _write_json(
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "latest_runtime_stop_provenance.json",
        {
            "generated_at": NOW.isoformat(),
            "stop_source": "launcher",
            "stop_reason": "expected_clean_down",
            "runtime_instance_id": "track-b-paper-runtime-test",
            "broker_safe_at_stop": previous_broker_safe_at_stop,
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
