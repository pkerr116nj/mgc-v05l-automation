from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_ATTACH_TIMEOUT,
    MANAGED_EXIT_ACTUATOR_BLOCKED,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_PHASE_RUNNING,
    TrackBManagedExitActuatorConfig,
    run_track_b_managed_exit_actuator,
)


NOW = datetime(2026, 6, 8, 15, 5, tzinfo=UTC)


def _submitted_attach_result(order_id: str = "91") -> dict:
    return {
        "classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED",
        "submit_attempted": True,
        "broker_state_mutated": True,
        "apply_result": {
            "close_submit_attempt": {
                "submitted": True,
                "broker_state_mutated": True,
                "broker_order_id": order_id,
                "perm_id": 123456,
            },
        },
    }


def test_runtime_down_exact_exit_due_position_applies_via_guarded_attach(tmp_path: Path) -> None:
    calls = []

    def _attach(config, now):
        calls.append((config, now))
        return _submitted_attach_result()

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        attach_runner=_attach,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["entry_allowed"] is False
    assert payload["submit_attempted"] is True
    assert payload["submitted_count"] == 1
    assert payload["attempted_closes"][0]["order_id"] == "91"
    assert calls[0][0].apply is True
    assert calls[0][0].operator_authorized_managed_exit is True
    assert calls[0][0].lifecycle_id == "life-mnq"
    assert calls[0][0].local_symbol == "MNQM6"


def test_exit_authority_quantity_overrides_recovery_candidate_for_duplicate_reduction(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    position = inputs["managed_positions"]["managed_positions"][0]
    position["quantity"] = "2"
    position["signed_broker_qty"] = "2"
    position["signed_lifecycle_qty"] = "2"
    position["broker_position"]["quantity"] = "2.0"
    position["required_close_quantity"] = "1"
    position["duplicate_same_lane_exposure"] = True
    position["duplicate_excess_qty"] = "1"
    position["accepted_managed_qty"] = "1"
    inputs["managed_orders"]["managed_orders"][0]["required_close_quantity"] = "2"
    calls = []

    def _attach(config, now):
        calls.append((config, now))
        return _submitted_attach_result()

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=_attach,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["eligible_positions"][0]["close_candidate"]["broker_quantity"] in {"2", "2.0"}
    assert payload["eligible_positions"][0]["close_candidate"]["quantity"] == "1"
    assert calls[0][0].quantity == 1


def test_dry_run_ready_does_not_call_attach(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        attach_runner=lambda config, now: calls.append(config) or {},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_DRY_RUN_READY
    assert payload["eligible_count"] == 1
    assert payload["submit_attempted"] is False
    assert calls == []


def test_actuator_writes_phase_progress_before_candidate_discovery(tmp_path: Path) -> None:
    output_path = tmp_path / "actuator.json"
    seen = []

    def _attach(config, now):
        seen.append(json.loads(output_path.read_text()))
        return _submitted_attach_result()

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(
            repo_root=tmp_path,
            output_path=output_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_closes_per_run=1,
        ),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        attach_runner=_attach,
        write=True,
    )

    assert seen
    assert seen[0]["classification"] == MANAGED_EXIT_ACTUATOR_PHASE_RUNNING
    assert seen[0]["phase"] == "guarded_attach"
    assert seen[0]["submit_attempted"] is False
    assert payload["phase_timings"][0]["phase"] == "startup"
    assert "candidate_discovery" in [row["phase"] for row in payload["phase_timings"]]


def test_default_attach_timeout_fails_closed_before_submit(tmp_path: Path) -> None:
    calls = []

    def _timeout_command(command, repo_root, timeout):
        calls.append(command)
        return subprocess.CompletedProcess(command, 124, stdout="", stderr="attach timed out")

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_closes_per_run=1,
            attach_timeout_seconds=0.01,
        ),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        command_runner=_timeout_command,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert payload["submitted_count"] == 0
    assert payload["attempted_closes"][0]["classification"] == MANAGED_EXIT_ACTUATOR_ATTACH_TIMEOUT
    assert payload["attempted_closes"][0]["primary_blocker"] == "BROKER_HANDSHAKE_OR_ATTACH_TIMEOUT_BEFORE_SUBMIT"
    assert "--skip-control-plane-refresh" in calls[0]


def test_attach_child_nonzero_empty_stdout_reports_stderr_blocker(tmp_path: Path) -> None:
    def _failed_command(command, repo_root, timeout):
        return subprocess.CompletedProcess(command, 2, stdout="", stderr="usage: missing expiry")

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_closes_per_run=1,
        ),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        command_runner=_failed_command,
        write=False,
    )

    attempt = payload["attempted_closes"][0]
    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert attempt["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert attempt["primary_blocker"] == "MANAGED_EXIT_ATTACH_CHILD_FAILED"
    assert attempt["returncode"] == 2
    assert attempt["stderr_tail"] == "usage: missing expiry"
    assert attempt["close_submit_attempt"] is None


def test_default_attach_child_fast_path_remains_apply_eligible(tmp_path: Path) -> None:
    calls = []

    def _ok_command(command, repo_root, timeout):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED",
                    "submit_attempted": True,
                    "broker_state_mutated": True,
                    "apply_result": {"close_submit_attempt": {"broker_order_id": "101", "perm_id": 202}},
                }
            ),
            stderr="",
        )

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_closes_per_run=1,
        ),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        command_runner=_ok_command,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["submit_attempted"] is True
    assert payload["attempted_closes"][0]["order_id"] == "101"
    assert "--skip-control-plane-refresh" in calls[0]


def test_multiple_positions_are_processed_one_at_a_time_with_refresh_between(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_positions"]["managed_positions"].append(
        _position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["managed_orders"]["managed_orders"].append(
        _managed_order(local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(
        _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["guardian"]["managed_close_authority"]["candidates"].append(
        _candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(
        _candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    attach_symbols = []
    refreshes = []

    def _attach(config, now):
        attach_symbols.append(config.local_symbol)
        return _submitted_attach_result()

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=_attach,
        refresh_hook=lambda row: refreshes.append(row["identity"]["local_symbol"]),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert attach_symbols == ["MNQM6", "MESM6"]
    assert refreshes == ["MNQM6", "MESM6"]
    assert payload["submitted_count"] == 2


def test_duplicate_close_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_orders"]["classification"] = "WORKING_CLOSE_ORDER"
    inputs["managed_orders"]["managed_orders"][0]["working"] = True
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: calls.append(config) or {},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["eligible_count"] == 0
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]
    assert calls == []


def test_open_orders_block_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["open_order_truth"]["classification"] = "OPEN_ORDERS_PRESENT"
    inputs["open_order_truth"]["broker_open_orders"] = [{"account_id": "DUM882026", "local_symbol": "MNQM6"}]

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]


def test_unknown_open_orders_block_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["open_order_truth"]["unknown_open_order_count"] = 1
    inputs["open_order_truth"]["unknown_open_orders"] = [{"account_id": "DUM882026", "local_symbol": "MNQM6"}]

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "same_contract_unknown_order_over_close_risk" in payload["blocked_positions"][0]["blockers"]


def test_dirty_reconciliation_is_legacy_diagnostic_when_v11_allows(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["reconciliation"]["classification"] = "BROKER_TRUTH_SETTLEMENT_TIMEOUT"
    inputs["reconciliation"]["broker_reconciled"] = False
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: calls.append(config) or _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["submit_attempted"] is True
    assert calls
    assert "BROKER_LIFECYCLE_RECONCILIATION_NOT_CLEAN" in payload["eligible_positions"][0]["legacy_apply_blockers_diagnostic"]


def test_stale_due_projection_is_diagnostic_when_broker_risk_exit_allowed(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_positions"]["source_freshness"] = {
        "stale": True,
        "stale_sources": ["position_truth"],
    }
    inputs["managed_positions"]["managed_positions"][0]["freshness_state"] = "STALE_DEPENDENCY"
    inputs["managed_positions"]["managed_positions"][0]["exit_due_evidence_stale"] = True
    inputs["managed_positions"]["managed_positions"][0]["apply_authority_degraded"] = True

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["exit_due_count"] == 1
    assert payload["submit_attempted"] is True
    diagnostics = payload["eligible_positions"][0]["legacy_apply_blockers_diagnostic"]
    assert "MANAGED_POSITION_APPLY_AUTHORITY_DEGRADED" in diagnostics
    assert "MANAGED_POSITION_SOURCE_STALE" in diagnostics


def test_ambiguous_ownership_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    duplicate = _position(lifecycle_id="life-other", trade_id="trade-other")
    inputs["managed_positions"]["managed_positions"].append(duplicate)
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(
        _registry_record(lifecycle_id="life-other", trade_id="trade-other")
    )

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "COMPETING_MANAGED_POSITION_CANDIDATE" in payload["blocked_positions"][0]["blockers"]


def test_guardian_safe_state_and_bsa_are_diagnostic_when_v11_exact_close_allows(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["guardian"]["managed_close_authority"]["allowed"] = False
    inputs["safe_state"]["classification"] = "SAFE_STATE_HARD_HOLD"
    inputs["safe_state"]["close_authority"]["allowed"] = False
    inputs["broker_session_authority"]["allowed_uses"]["managed_risk_reducing_close"] = False

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    diagnostics = payload["eligible_positions"][0]["legacy_apply_blockers_diagnostic"]
    assert payload["classification"] == "MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING"
    assert "GUARDIAN_CLOSE_AUTHORITY_NOT_ALLOWED" in diagnostics
    assert "SAFE_STATE_CLOSE_NOT_ALLOWED" in diagnostics
    assert "BROKER_SESSION_MANAGED_RISK_REDUCING_CLOSE_NOT_ALLOWED" in diagnostics
    assert payload["submit_attempted"] is True


def test_live_money_or_paper_proof_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["safe_state"]["live_money_eligible"] = True
    inputs["broker_session_authority"]["paper_proof_invoked"] = True

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    blockers = payload["blocked_positions"][0]["blockers"]
    assert "LIVE_MONEY_ELIGIBLE_TRUE" in blockers
    assert "PAPER_PROOF_INVOKED_TRUE" in blockers
    assert payload["submit_attempted"] is False


def test_guardian_close_candidate_mismatch_is_diagnostic_when_v11_allows(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["guardian"]["managed_close_authority"]["candidates"][0]["action"] = "BUY"

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: _submitted_attach_result(),
        write=False,
    )

    assert payload["submit_attempted"] is True
    assert "GUARDIAN_EXACT_CLOSE_CANDIDATE_MISSING" in payload["eligible_positions"][0]["legacy_apply_blockers_diagnostic"]


def test_attach_config_prefers_current_broker_contract_key_over_stale_row_key(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    position = inputs["managed_positions"]["managed_positions"][0]
    position["contract_key"] = "ES-202606"
    position["symbol"] = "ES"
    position["local_symbol"] = "ESU6"
    position["con_id"] = 649180671
    position["broker_position"].update(
        {
            "symbol": "ES",
            "track_b_root": "ES",
            "local_symbol": "ESU6",
            "con_id": 649180671,
            "contract_key": "ES-202609",
            "expiry": "20260918",
        }
    )
    inputs["managed_orders"]["managed_orders"][0].update({"local_symbol": "ESU6", "con_id": 649180671})
    inputs["guardian"]["managed_close_authority"]["candidates"][0].update(
        {"symbol": "ES", "local_symbol": "ESU6", "con_id": 649180671}
    )
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"][0].update(
        {"symbol": "ES", "local_symbol": "ESU6", "con_id": 649180671}
    )
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"][0].update(
        {"symbol": "ES", "local_symbol": "ESU6", "con_id": 649180671}
    )
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: calls.append(config) or _submitted_attach_result(),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert calls[0].contract_key == "ES-202609"
    assert calls[0].local_symbol == "ESU6"
    assert calls[0].con_id == 649180671


def _inputs(*, runtime_down: bool) -> dict:
    candidate = _candidate()
    return {
        "managed_positions": {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [_position()],
        },
        "managed_orders": {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [_managed_order()],
        },
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "broker_open_orders": [], "unknown_open_orders": []},
        "guardian": {
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {
                "allowed": True,
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "candidates": [candidate],
            },
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "safe_state": {
            "classification": "SAFE_STATE_NORMAL",
            "close_authority": {
                "allowed": True,
                "classification": "MANAGED_CLOSE_MUTATION_ALLOWED",
                "guardian_close_candidates": [candidate],
            },
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "registry_reconciliation": {
                "classification": "REGISTRY_RECONCILIATION_MATCHED",
                "blocking": False,
                "mapped_records": [_registry_record()],
                "review_required_trade_ids": [],
            },
        },
        "broker_truth_lease": {"schema_version": "track_b_broker_truth_lease_v1", "lease_state": "ACTIVE"},
        "broker_session_authority": {
            "schema_version": "track_b_broker_session_authority_v1",
            "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "allowed_uses": {"new_entry": False, "managed_risk_reducing_close": True},
            "degraded_exact_risk_reducing_close_context": {"ready": True},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    }


def _position(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "exit_due": True,
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": f"{symbol.lower()}_globex_active_participation_long",
        "lane_id": f"{symbol.lower()}_globex_active_participation_long",
        "projection_authority_owner_confirmed": True,
        "projection_authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        "working_close_qty": "0",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1.0",
            "expiry": "20260618",
        },
        "lifecycle_position": {
            "account_id": "DUM882026",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1",
            "side": "LONG",
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        },
    }


def _managed_order(
    *,
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "close_order_required_now": True,
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "working": False,
    }


def _registry_record(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "current_state": "OPEN_MANAGED",
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
    }


def _candidate(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "action": "SELL",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
    }
