from __future__ import annotations

import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import mgc_v05l.app.track_b_paper_leak_test as leak_test_module
from mgc_v05l.app.track_b_paper_leak_test import (
    LeakTestExposurePolicy,
    RESULT_CLASSIFICATIONS,
    _pre_apply_readiness_check,
    build_concurrent_plan_report,
    build_plan_only_report,
    build_single_lane_apply_report,
    build_single_lane_dry_run_report,
    build_leak_test_authorization,
    _classify_exit_plan,
    report_to_dict,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LANE_ID = "mnq_1x_ny_early_core__us_midday_long"
MGC_LANE_ID = "mgc_1x_all_lanes__asia_early_long"


def _runtime_command() -> str:
    return f"python -m mgc_v05l.app.main probationary-paper-soak --config {REPO_ROOT / 'config/base.yaml'}"


def _operator_status() -> dict[str, object]:
    return {
        "source_runtime_pid": os.getpid(),
        "source_runtime_cwd": str(REPO_ROOT),
        "source_runtime_repo_root": str(REPO_ROOT),
        "source_runtime_command": _runtime_command(),
    }


def _stale_operator_status() -> dict[str, object]:
    return {
        "source_runtime_pid": 99999999,
        "source_runtime_cwd": str(REPO_ROOT),
        "source_runtime_repo_root": str(REPO_ROOT),
        "source_runtime_command": _runtime_command(),
    }


def _guarded_authority(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "valid": True,
        "source": "guarded_paper_loop_control_plane",
        "pid": os.getpid(),
        "command": (
            f"python -m mgc_v05l.execution_core.track_b_p0_observe_only_loop "
            f"--repo-root {REPO_ROOT} --mode guarded-paper"
        ),
        "runtime_cwd": str(REPO_ROOT),
        "runtime_generation_id": "track-b-paper-runtime-generation-test",
        "duplicate_count": 1,
        "blockers": (),
        "process_count": 1,
    }
    payload.update(overrides)
    return payload


def _clean_flat_reconciliation(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": "DUM882026",
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "broker_reconciled": True,
        "review_required_count": 0,
        "track_b_broker_open_order_count": 0,
        "lifecycle_open_order_count": 0,
        "track_b_broker_position_count": 0,
        "lifecycle_open_position_count": 0,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "blockers": [],
    }
    payload.update(overrides)
    return payload


def _managed_position(*, symbol: str = "PL", lane_id: str = "pl_us_late_pause_resume_long") -> dict[str, object]:
    return {
        "account_id": "DUM882026",
        "instrument_family": symbol,
        "local_symbol": f"{symbol}N6" if symbol == "PL" else "MNQM6",
        "con_id": 644855286 if symbol == "PL" else 770561201,
        "quantity": "1",
        "side": "LONG",
        "strategy_id": f"{lane_id}__{symbol}",
        "lane_id": lane_id,
        "lifecycle_id": f"bridge_fill_{symbol}|1m|2026-05-14T17:52:00Z|BUY_TO_OPEN",
        "final_position_status": "OPEN_MANAGED",
    }


def _clean_managed_position_reconciliation(*, symbol: str = "PL", lane_id: str = "pl_us_late_pause_resume_long", **overrides: object) -> dict[str, object]:
    position = _managed_position(symbol=symbol, lane_id=lane_id)
    payload = _clean_flat_reconciliation(
        track_b_broker_position_count=1,
        lifecycle_open_position_count=1,
        track_b_lifecycle_positions=[position],
        track_b_broker_positions=[
            {
                "account_id": position["account_id"],
                "symbol": symbol,
                "local_symbol": position["local_symbol"],
                "con_id": position["con_id"],
                "quantity": "1",
            }
        ],
    )
    payload.update(overrides)
    return payload


def _lane(report, lane_id: str = LANE_ID):
    return next(lane for lane in report.lanes if lane.lane_id == lane_id)


def _bridge_result(classification: str, *, status: str = "filled") -> dict[str, object]:
    lifecycle: dict[str, object] = {"status": status}
    if status != "blocked":
        lifecycle.update(
            {
                "broker_order_id": "11",
                "client_id": 10940,
                "perm_id": 984270669,
                "fill_price": "29492.75",
                "fill_timestamp": "2026-05-14T13:04:48.414655+00:00",
            }
        )
    return {
        "classification": classification,
        "report": {
            "classification": classification,
            "detail": classification,
            "entry_execution_pricing": {"execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
            "delegated_result": {
                "classification": f"{classification}_DELEGATED",
                "report": {"submit_cancel_lifecycle": lifecycle},
            },
        },
    }


def _unknown_bridge_result(*, submit_attempted: bool) -> dict[str, object]:
    lifecycle: dict[str, object] = {
        "status": "manual_confirmation_unavailable" if submit_attempted else "blocked_before_submit",
        "detail": "Manual confirmation unavailable.",
    }
    if submit_attempted:
        lifecycle.update(
            {
                "submitted_order_id": 1,
                "manual_confirmation": {"state": "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION"},
                "open_order_after_submit": {"open_order_count": 0, "open_orders": []},
            }
        )
    return {
        "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
        "report": {
            "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
            "detail": "Unknown manual state.",
            "entry_execution_pricing": {"execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
            "delegated_result": {
                "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                "report": {
                    "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                    "submit_cancel_lifecycle": lifecycle,
                },
            },
        },
    }


def _unknown_close_bridge_result(*, lifecycle_id: str) -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
        "report": {
            "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
            "detail": "Unknown manual close state.",
            "entry_execution_pricing": {"execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
            "known_managed_exit_order_persistence": {
                "persisted": True,
                "lifecycle_id": lifecycle_id,
                "broker_order_id": "27",
                "client_id": 11940,
                "perm_id": None,
                "action": "SELL",
                "quantity": "1",
            },
            "delegated_result": {
                "classification": "PAPER_CLOSE_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                "report": {
                    "classification": "PAPER_CLOSE_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                    "submit_cancel_lifecycle": {
                        "status": "manual_confirmation_unavailable",
                        "submitted_order_id": 27,
                        "manual_confirmation": {"state": "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION"},
                        "open_order_after_submit": {"open_order_count": 0, "open_orders": []},
                    },
                },
            },
        },
    }


def _blocked_after_submit_bridge_result() -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_INTENT_BLOCKED",
        "report": {
            "classification": "PAPER_STRATEGY_INTENT_BLOCKED",
            "detail": "Delegated manual state blocked after submit.",
            "entry_execution_pricing": {"execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
            "delegated_result": {
                "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                "report": {
                    "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                    "submit_cancel_lifecycle": {
                        "status": "manual_confirmation_unavailable",
                        "submitted_order_id": 1,
                        "manual_confirmation": {"state": "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION"},
                        "open_order_after_submit": {"open_order_count": 0, "open_orders": []},
                    },
                },
            },
        },
    }


def _reader_for(stages: dict[str, dict[str, object]]):
    def _reader(_repo_root: Path, stage: str) -> dict[str, object]:
        return stages.get(stage, _clean_flat_reconciliation())

    return _reader


def _ready_precheck(_repo_root, _lane, _safety):
    return {"classification": "LEAK_TEST_PRECHECK_READY", "ready": True, "blockers": ()}


def _adoption_applied(**_kwargs):
    return {"classification": "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"}


def _adoption_refused(**_kwargs):
    return {"classification": "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED", "failures": ["test refusal"]}


def _adoption_refused_waiting_for_broker_truth(**_kwargs):
    return {
        "classification": "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED",
        "failures": [
            "Expected exactly one matching broker position, found 0.",
            "Partial leak-test adoption requires exact broker position truth.",
        ],
    }


def _stale_precheck(_repo_root, _lane, _safety):
    return {
        "classification": "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY",
        "ready": False,
        "blockers": ("backend_readiness_artifact_stale",),
    }


def _market_stale_precheck(_repo_root, _lane, _safety):
    return {
        "classification": "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE",
        "ready": False,
        "blockers": ("market_data_micro_stale_retryable",),
    }


def _authorization_path(tmp_path: Path, *, lane_id: str = LANE_ID, mutate: dict[str, object] | None = None, expired: bool = False) -> Path:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=lane_id,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report, lane_id)
    created = datetime.now(timezone.utc) - (timedelta(seconds=30) if expired else timedelta(seconds=0))
    auth = build_leak_test_authorization(
        repo_root=REPO_ROOT,
        lane=lane,
        safety=report.safety,
        action="BUY",
        ttl_seconds=1 if expired else 600,
        now=created,
    )
    if mutate:
        auth.update(mutate)
    path = tmp_path / "leak_test_authorization.json"
    path.write_text(json.dumps(auth, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _auth_digest(payload: dict[str, object]) -> str:
    fields = (
        "artifact_type",
        "account_id",
        "mode",
        "lane_id",
        "strategy_id",
        "symbol",
        "local_symbol",
        "expiry",
        "con_id",
        "action",
        "exit_action",
        "qty",
        "repo_root",
        "git_head",
        "created_at",
        "expires_at",
        "safety_snapshot",
    )
    critical = {field: payload.get(field) for field in fields}
    return hashlib.sha256(json.dumps(critical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _readiness_repo(
    tmp_path: Path,
    *,
    selected_row: dict[str, object],
    unrelated_rows: list[dict[str, object]] | None = None,
) -> Path:
    now = datetime.now(timezone.utc).isoformat()
    root = tmp_path
    _write_json(
        root / "outputs/operator_dashboard/paper_readiness_snapshot.json",
        {
            "generated_at": now,
            "runtime_running": True,
            "paper_runtime_ready": True,
            "paper_trade_allowed": True,
            "market_data_stale_count": sum(
                1
                for row in [selected_row, *(unrelated_rows or [])]
                if row.get("bar_state") == "MARKET_DATA_STALE" or row.get("tradability_status") == "MARKET_DATA_STALE"
            ),
            "bar_authority_unavailable_count": 0,
            "blocking_fault_count": 0,
            "lane_eligibility_rows": [selected_row, *(unrelated_rows or [])],
        },
    )
    _write_json(root / "outputs/operator_dashboard/startup_control_plane_snapshot.json", {"generated_at": now, "overall_state": "READY"})
    _write_json(
        root / "outputs/operator_dashboard/supervised_paper_operability_snapshot.json",
        {"generated_at": now, "app_usable_for_supervised_paper": True, "runtime_running": True, "paper_runtime_ready": True},
    )
    _write_json(root / "outputs/operator_dashboard/paper_temporary_paper_runtime_integrity_snapshot.json", {"generated_at": now, "temp_paper_blocked": False})
    _write_json(
        root / "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json",
        {"generated_at": now, "live_money_eligible": False, "source": "DATABENTO_REALTIME_PHASE1"},
    )
    _write_json(
        root / "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_supervisor_status.json",
        {"generated_at": now, "classification": "PHASE1_DATABENTO_LIVE_SUPERVISOR_RUNNING", "live_money_eligible": False},
    )
    return root


def _write_phase1_runtime_candles(root: Path, *, symbol: str = "MNQ", timeframe: str = "1m", stale: bool = False) -> None:
    generated_at = datetime.now(timezone.utc) - (timedelta(minutes=10) if stale else timedelta(seconds=15))
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / timeframe
        / "latest_runtime_candles.json",
        {
            "generated_at": generated_at.isoformat(),
            "symbol": symbol,
            "timeframe": timeframe,
            "source_id": f"DATABENTO_REALTIME_PHASE1_{symbol.lower()}",
            "completed_candles_only": True,
            "historical_seed_ready": False,
            "realtime_feed_confirmed": True,
            "archive_artifact_used": False,
            "research_artifact_used": False,
            "bars": [
                {
                    "bar_start": "2026-05-25T09:00:00+00:00",
                    "bar_end": "2026-05-25T09:01:00+00:00",
                    "open": 1,
                    "high": 2,
                    "low": 1,
                    "close": 2,
                    "completed": True,
                }
            ],
        },
    )


def _selected_lane_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "lane_id": LANE_ID,
        "symbol": "MNQ",
        "data_fresh": True,
        "bar_state": "READY",
        "tradability_status": "READY",
        "execution_timeframe": "1m",
        "observed_bar_arrival_age_seconds": 22.0,
        "market_data_lag_seconds": 0.0,
    }
    row.update(overrides)
    return row


def _unrelated_stale_row() -> dict[str, object]:
    return {
        "lane_id": "atp_companion_v1_pl_asia_us",
        "symbol": "PL",
        "data_fresh": False,
        "bar_state": "MARKET_DATA_STALE",
        "tradability_status": "MARKET_DATA_STALE",
        "execution_timeframe": "1m",
        "expected_completed_bar_end_ts": "2026-05-14T18:45:00-04:00",
        "observed_completed_bar_end_ts": "2026-05-14T22:42:00+00:00",
        "observed_bar_arrival_age_seconds": 180.0,
        "market_data_lag_seconds": 180.0,
    }


def test_plan_mode_lists_guarded_lanes_without_broker_mutation() -> None:
    report = build_plan_only_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    payload = report_to_dict(report)

    assert report.mode == "plan-only"
    assert report.mutation_performed is False
    assert report.live_money_eligible is False
    assert len(report.lanes) > 10
    lane = _lane(report)
    assert lane.symbol == "MNQ"
    assert lane.localSymbol == "MNQM6"
    assert lane.expiry == "202606"
    assert lane.entry_execution_intent == "PARTICIPATE_NOW"
    assert lane.expected_route == "ibkr_paper_bridge_submit_capable"
    assert lane.exit_plan_classification == "STRATEGY_MANAGED_EXIT"
    assert lane.exit_plan_source == "runtime_strategy_exit_policy"
    assert lane.safe_to_test is True
    assert lane.safe_for_isolated_test is True
    assert lane.safe_for_concurrent_test is True
    assert payload["concurrent_scenarios"]
    assert payload["mutation_performed"] is False


def test_atp_lane_without_native_exit_uses_leak_test_controlled_exit() -> None:
    lane_id = "atp_companion_v1_gc_asia_us_production_track_5m"
    report = build_plan_only_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    lane = _lane(report, lane_id=lane_id)

    assert lane.exit_plan_classification == "LEAK_TEST_CONTROLLED_EXIT"
    assert lane.exit_plan_source == "guarded_track_b_paper_leak_test_close"
    assert lane.safe_for_isolated_test is True


def test_lane_without_native_or_controlled_exit_path_blocks() -> None:
    class Spec:
        lane_id = "diagnostic_lane_without_exit"
        standalone_strategy_id = "diagnostic_lane_without_exit"
        runtime_kind = "diagnostic_only"
        strategy_family = "diagnostic_only"

    plan = _classify_exit_plan(spec=Spec(), route="diagnostic_only")

    assert plan["classification"] == "NO_EXIT_PLAN_BLOCKER"
    assert "no_exit_plan_available" in plan["blockers"]


def test_dry_run_performs_no_broker_mutation() -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_DRY_RUN_READY"
    assert report.mutation_performed is False
    assert report.lanes[0].safe_to_test is True


def test_dry_run_writes_authorization_artifact_with_digest(tmp_path: Path) -> None:
    auth_path = tmp_path / "auth.json"
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        write_authorization=True,
        authorization_output_path=auth_path,
    )

    payload = json.loads(auth_path.read_text(encoding="utf-8"))
    assert report.result_classification == "LEAK_TEST_DRY_RUN_READY"
    assert report.mutation_performed is False
    assert report.authorization_artifact is not None
    assert payload["artifact_type"] == "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION"
    assert payload["account_id"] == "DUM882026"
    assert payload["mode"] == "PAPER"
    assert payload["lane_id"] == LANE_ID
    assert payload["symbol"] == "MNQ"
    assert payload["local_symbol"] == "MNQM6"
    assert payload["expiry"] == "202606"
    assert payload["action"] == "BUY"
    assert payload["exit_action"] == "SELL"
    assert payload["qty"] == 1
    assert payload["safety_snapshot"]["live_money_eligible"] is False
    assert payload["safety_snapshot"]["paper_proof_invoked"] is False
    assert len(payload["digest"]) == 64


def test_precheck_selected_lane_fresh_unrelated_stale_lanes_pass_with_warning(tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(),
        unrelated_rows=[_unrelated_stale_row()],
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_READY"
    assert readiness["ready"] is True
    assert readiness["market_data_scope"] == "SELECTED_LANE"
    assert readiness["selected_lane_market_data_fresh"] is True
    assert readiness["selected_lane_required_timeframe"] == "1m"
    assert readiness["unrelated_market_data_stale_count"] == 1
    assert readiness["unrelated_market_data_stale_lanes"][0]["lane_id"] == "atp_companion_v1_pl_asia_us"
    assert "unrelated_market_data_stale" in readiness["warnings"]


def test_precheck_selected_lane_stale_blocks(tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(
            data_fresh=False,
            bar_state="MARKET_DATA_STALE",
            tradability_status="MARKET_DATA_STALE",
            expected_completed_bar_end_ts="2026-05-14T18:45:00-04:00",
            observed_completed_bar_end_ts="2026-05-14T22:42:00+00:00",
            market_data_lag_seconds=180.0,
            observed_bar_arrival_age_seconds=180.0,
        ),
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE"
    assert readiness["ready"] is False
    assert "selected_lane_market_data_stale" in readiness["blockers"]


def test_precheck_selected_lane_missing_blocks(tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(lane_id="different_lane"),
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE"
    assert readiness["ready"] is False
    assert "selected_lane_market_data_unavailable" in readiness["blockers"]


def test_precheck_selected_lane_micro_stale_blocks_retryable(tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(
            data_fresh=False,
            bar_state="MARKET_DATA_STALE",
            tradability_status="MARKET_DATA_STALE",
            expected_completed_bar_end_ts="2026-05-14T22:10:00+00:00",
            observed_completed_bar_end_ts="2026-05-14T22:09:00+00:00",
            market_data_lag_seconds=60.0,
            observed_bar_arrival_age_seconds=30.0,
            bar_state_reason="Wall clock expects a completed execution bar, but no observed completed market-data bar exists beyond grace.",
        ),
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE"
    assert readiness["ready"] is False
    assert "selected_lane_market_data_micro_stale_retryable" in readiness["blockers"]


def test_precheck_uses_shared_services_authority_over_stale_legacy_readiness(monkeypatch, tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(
            data_fresh=False,
            bar_state="MARKET_DATA_STALE",
            tradability_status="MARKET_DATA_STALE",
            artifact_timeframe="3m",
            expected_completed_bar_end_ts="2026-05-25T09:20:00+00:00",
            observed_completed_bar_end_ts="2026-05-22T19:34:00+00:00",
            market_data_lag_seconds=222300.0,
            observed_bar_arrival_age_seconds=222300.0,
        ),
    )
    _write_phase1_runtime_candles(repo, symbol="MNQ", timeframe="1m")
    monkeypatch.setattr(leak_test_module, "_current_guarded_runtime_authority", lambda _repo_root: _guarded_authority())
    monkeypatch.setattr(
        leak_test_module,
        "build_track_b_readiness_authority",
        lambda **_kwargs: {
            "ready": False,
            "blockers": ("canonical_readiness_not_submit_capable", "runtime_not_running", "runtime_ingestion_not_fresh"),
            "runtime_running": False,
            "paper_runtime_ready": False,
            "paper_trade_allowed": False,
        },
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_READY"
    assert readiness["ready"] is True
    assert readiness["shared_services_authority_ready"] is True
    assert readiness["selected_lane_market_data_fresh"] is True
    assert readiness["selected_lane_authority_timeframe"] == "1m"
    assert readiness["selected_lane_market_data"]["source_authority"] == "PHASE1_RUNTIME_MARKET_DATA"
    assert "shared_services_authority_overrode_legacy_readiness" in readiness["warnings"]
    assert "phase1_runtime_market_data_overrode_legacy_lane_readiness" in readiness["warnings"]
    assert not readiness["blockers"]


def test_precheck_blocks_when_shared_services_control_plane_is_blocked(monkeypatch, tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(tmp_path, selected_row=_selected_lane_row())
    _write_phase1_runtime_candles(repo, symbol="MNQ", timeframe="1m")
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(valid=False, blockers=("control_plane_not_ready",)),
    )
    monkeypatch.setattr(
        leak_test_module,
        "build_track_b_readiness_authority",
        lambda **_kwargs: {
            "ready": False,
            "blockers": ("canonical_readiness_not_submit_capable",),
            "runtime_running": False,
            "paper_runtime_ready": False,
            "paper_trade_allowed": False,
        },
    )

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    assert "control_plane_not_ready" in readiness["blockers"]


def test_precheck_legacy_lane_stale_blocks_when_phase1_authority_is_stale(monkeypatch, tmp_path: Path) -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    lane = _lane(report)
    repo = _readiness_repo(
        tmp_path,
        selected_row=_selected_lane_row(data_fresh=False, bar_state="MARKET_DATA_STALE", tradability_status="MARKET_DATA_STALE"),
    )
    _write_phase1_runtime_candles(repo, symbol="MNQ", timeframe="1m", stale=True)
    monkeypatch.setattr(leak_test_module, "_current_guarded_runtime_authority", lambda _repo_root: _guarded_authority())

    readiness = _pre_apply_readiness_check(repo_root=repo, lane=lane, safety=report.safety)

    assert readiness["classification"] == "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE"
    assert "selected_lane_market_data_stale" in readiness["blockers"]
    assert readiness["phase1_runtime_market_data"]["ready"] is False


def test_apply_refuses_when_broker_lifecycle_not_reconciled() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(classification="TRACK_B_PAPER_RECONCILIATION_BLOCKED", broker_reconciled=False),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "broker_lifecycle_not_reconciled" in report.lanes[0].blockers
    assert report.mutation_performed is False


def test_apply_refuses_when_open_orders_exist() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(track_b_broker_open_order_count=1),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "open_orders_present_for_isolated_round_trip" in report.lanes[0].blockers


def test_isolated_round_trip_refuses_when_existing_position_exists() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_managed_position_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "existing_positions_present_for_isolated_round_trip" in report.lanes[0].blockers


def test_clean_managed_position_does_not_automatically_block_concurrent_entry_when_policy_allows() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_managed_position_reconciliation(symbol="PL"),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=1),
    )

    lane = _lane(report)
    assert lane.safe_for_isolated_test is False
    assert "existing_positions_present_for_isolated_round_trip" in lane.isolated_blockers
    assert lane.safe_for_concurrent_test is True
    assert lane.concurrent_blockers == ()


def test_concurrent_tolerant_dry_run_can_authorize_with_clean_managed_position(tmp_path: Path) -> None:
    authorization_path = tmp_path / "leak_test_authorization.json"

    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_managed_position_reconciliation(symbol="PL"),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=1),
        write_authorization=True,
        authorization_output_path=authorization_path,
        concurrent_tolerant=True,
    )

    assert report.result_classification == "LEAK_TEST_DRY_RUN_READY"
    assert report.authorization_artifact is not None
    assert authorization_path.exists()
    assert _lane(report).safe_for_isolated_test is False
    assert _lane(report).safe_for_concurrent_test is True


def test_concurrent_tolerant_apply_uses_concurrent_blockers() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_managed_position_reconciliation(symbol="PL"),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=1),
        concurrent_tolerant=True,
    )

    assert report.result_classification == "LEAK_TEST_AUTHORIZATION_MISSING"
    assert report.apply_result is not None
    assert report.apply_result.pre_apply_blockers == ()


def test_concurrent_tolerant_apply_blocks_duplicate_same_lane_position() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=2, allow_same_symbol_multiple_strategies=True),
        concurrent_tolerant=True,
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert report.apply_result is not None
    assert "duplicate_same_lane_position" in report.apply_result.pre_apply_blockers
    assert "existing_positions_present_for_isolated_round_trip" not in report.apply_result.pre_apply_blockers


def test_unresolved_broker_lifecycle_mismatch_blocks_concurrent_entry() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(track_b_broker_position_count=1, lifecycle_open_position_count=0),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert "broker_lifecycle_position_count_mismatch" in _lane(report).concurrent_blockers


def test_review_required_blocks_concurrent_entry() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(review_required_count=1),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert "review_required_nonzero" in _lane(report).concurrent_blockers


def test_duplicate_same_lane_position_blocks_concurrent_entry() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=2, allow_same_symbol_multiple_strategies=True),
    )

    assert "duplicate_same_lane_position" in _lane(report).concurrent_blockers


def test_same_symbol_second_position_blocks_unless_policy_allows_it() -> None:
    same_symbol_reconciliation = _clean_managed_position_reconciliation(
        symbol="MNQ",
        lane_id="mnq_1x_ny_early_core__us_early_long",
    )
    blocked = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=same_symbol_reconciliation,
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )
    allowed = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=same_symbol_reconciliation,
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, max_positions_per_symbol=2, allow_same_symbol_multiple_strategies=True),
    )

    assert "same_symbol_conflict" in _lane(blocked).concurrent_blockers
    assert _lane(allowed).safe_for_concurrent_test is True


def test_multiple_symbols_allowed_when_policy_allows() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_managed_position_reconciliation(symbol="PL"),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_total_open_positions=3, allow_multiple_symbols=True),
    )

    assert _lane(report).safe_for_concurrent_test is True


def test_open_order_cap_enforced_for_concurrent_plan() -> None:
    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(track_b_broker_open_order_count=1),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        exposure_policy=LeakTestExposurePolicy(max_open_orders_total=0),
    )

    assert "open_order_total_exceeds_policy" in _lane(report).concurrent_blockers


def test_apply_refuses_outside_paper_account() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(account_id="DU123456"),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "account_not_DUM882026" in report.lanes[0].blockers


def test_one_lane_at_a_time_guard_blocks_other_lane() -> None:
    report = build_plan_only_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        active_leak_test={"lane_id": "pl_us_late_pause_resume_long"},
        runtime_command=_runtime_command(),
    )

    assert "another_leak_test_lane_active" in _lane(report).blockers


def test_live_money_and_paper_proof_flags_block() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(live_money_eligible=True, paper_proof_invoked=True),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert "live_money_eligible_true" in report.lanes[0].blockers
    assert "paper_proof_invoked_true" in report.lanes[0].blockers
    assert report.live_money_eligible is False


def test_apply_refuses_when_duplicate_runtime_submitter_exists() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status={**_operator_status(), "duplicate_runtime_submitter_count": 2},
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "duplicate_runtime_submitters" in report.lanes[0].blockers


def test_operator_status_with_active_pid_and_dev_cwd_passes_runtime_verification() -> None:
    report = build_single_lane_dry_run_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
    )

    assert report.safety.runtime_pid_active is True
    assert report.safety.runtime_from_dev_root is True
    assert "runtime_not_verified_from_dev_root" not in report.lanes[0].blockers
    assert "runtime_pid_not_active" not in report.lanes[0].blockers


def test_operator_status_missing_cwd_blocks_runtime_verification(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(valid=False, blockers=("guarded_paper_loop_process_missing",), process_count=0),
    )
    operator_status = {
        "source_runtime_pid": os.getpid(),
        "source_runtime_command": _runtime_command(),
    }

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=operator_status,
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "runtime_not_verified_from_dev_root" in report.lanes[0].blockers


def test_operator_status_dead_pid_blocks_runtime_verification(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(valid=False, blockers=("guarded_paper_loop_process_missing",), process_count=0),
    )
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status={**_operator_status(), "source_runtime_pid": 99999999},
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "runtime_pid_not_active" in report.lanes[0].blockers


def test_stale_legacy_pid_ignored_when_current_guarded_authority_is_valid(monkeypatch) -> None:
    monkeypatch.setattr(leak_test_module, "_current_guarded_runtime_authority", lambda _repo_root: _guarded_authority())

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
    )

    lane = _lane(report)
    assert report.safety.runtime_authority_source == "guarded_paper_loop_control_plane"
    assert report.safety.runtime_pid == os.getpid()
    assert report.safety.runtime_pid_active is True
    assert report.safety.runtime_from_dev_root is True
    assert "runtime_pid_not_active" not in lane.concurrent_blockers
    assert "runtime_not_verified_from_dev_root" not in lane.concurrent_blockers
    assert lane.safe_for_concurrent_test is True


def test_stale_legacy_pid_blocks_when_current_guarded_authority_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(
            valid=False,
            pid=None,
            command=None,
            runtime_cwd=None,
            duplicate_count=0,
            blockers=("guarded_paper_loop_process_missing",),
            process_count=0,
        ),
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
    )

    lane = _lane(report)
    assert "runtime_pid_not_active" in lane.concurrent_blockers
    assert "runtime_not_verified_from_dev_root" in lane.concurrent_blockers


def test_explicit_metals_lane_flow_does_not_require_guarded_loop_when_authority_is_clean(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(
            valid=False,
            pid=None,
            command=None,
            runtime_cwd=None,
            blockers=("guarded_paper_loop_process_missing",),
            process_count=0,
        ),
    )
    monkeypatch.setattr(
        leak_test_module,
        "_explicit_lane_flow_authority",
        lambda *, repo_root, metals_only: {
            "ready": True,
            "source": "explicit_metals_lane_control_plane",
            "blockers": (),
            "warnings": (),
            "metals_only": metals_only,
        },
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
        explicit_lane_flow=True,
        metals_only=True,
    )
    lane = _lane(report, MGC_LANE_ID)

    assert report.safety.runtime_authority_source == "explicit_lane_flow_control_plane"
    assert "runtime_pid_not_active" not in lane.concurrent_blockers
    assert "runtime_not_verified_from_dev_root" not in lane.concurrent_blockers
    assert "guarded_paper_loop_process_missing" not in lane.concurrent_blockers
    assert lane.safe_for_concurrent_test is True


def test_explicit_metals_lane_flow_filters_equity_index_lanes(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_explicit_lane_flow_authority",
        lambda *, repo_root, metals_only: {"ready": True, "blockers": (), "warnings": (), "metals_only": metals_only},
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
        explicit_lane_flow=True,
        metals_only=True,
        now=datetime(2026, 5, 25, 17, 45, tzinfo=timezone.utc),
    )

    symbols = {lane.symbol for lane in report.lanes}
    assert symbols
    assert symbols <= {"MGC", "GC", "PL"}
    assert "MNQ" not in symbols
    assert "PL" in symbols


def test_explicit_metals_lane_flow_excludes_pl_after_230pm_et(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_explicit_lane_flow_authority",
        lambda *, repo_root, metals_only: {"ready": True, "blockers": (), "warnings": (), "metals_only": metals_only},
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
        explicit_lane_flow=True,
        metals_only=True,
        now=datetime(2026, 5, 25, 18, 31, tzinfo=timezone.utc),
    )

    symbols = {lane.symbol for lane in report.lanes}
    assert symbols
    assert symbols <= {"MGC", "GC"}
    assert "PL" not in symbols


def test_explicit_metals_lane_flow_blocks_conflicting_mgc_exposure(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_explicit_lane_flow_authority",
        lambda *, repo_root, metals_only: {"ready": True, "blockers": (), "warnings": (), "metals_only": metals_only},
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_managed_position_reconciliation(symbol="MGC", lane_id="existing_mgc_long"),
        operator_status=_stale_operator_status(),
        explicit_lane_flow=True,
        metals_only=True,
    )
    lane = _lane(report, MGC_LANE_ID)

    assert "max_positions_per_symbol_reached" in lane.concurrent_blockers
    assert "same_symbol_conflict" in lane.concurrent_blockers


def test_current_guarded_authority_wrong_root_blocks(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(
            valid=False,
            runtime_cwd="/Users/patrick/Documents/MGC-v05l-automation",
            blockers=("runtime_not_verified_from_dev_root", "runtime_from_documents_or_icloud"),
        ),
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
    )

    lane = _lane(report)
    assert "runtime_not_verified_from_dev_root" in lane.concurrent_blockers
    assert "runtime_from_documents_or_icloud" in lane.concurrent_blockers


def test_current_guarded_authority_duplicate_writer_blocks(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(
            valid=False,
            duplicate_count=2,
            blockers=("duplicate_runtime_submitters",),
        ),
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
    )

    assert report.safety.duplicate_runtime_submitter_count == 2
    assert "duplicate_runtime_submitters" in _lane(report).concurrent_blockers


def test_current_guarded_authority_control_plane_blocked_blocks(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(
            valid=False,
            blockers=("control_plane_not_ready",),
        ),
    )

    report = build_concurrent_plan_report(
        repo_root=REPO_ROOT,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_stale_operator_status(),
    )

    assert "control_plane_not_ready" in _lane(report).concurrent_blockers


def test_operator_status_documents_cwd_blocks_runtime_verification(monkeypatch) -> None:
    monkeypatch.setattr(
        leak_test_module,
        "_current_guarded_runtime_authority",
        lambda _repo_root: _guarded_authority(valid=False, blockers=("guarded_paper_loop_process_missing",), process_count=0),
    )
    documents_root = "/Users/patrick/Documents/MGC-v05l-automation"
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status={
            **_operator_status(),
            "source_runtime_cwd": documents_root,
            "source_runtime_repo_root": documents_root,
            "source_runtime_command": f"python -m mgc_v05l.app.main probationary-paper-soak --config {documents_root}/config/base.yaml",
        },
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert "runtime_not_verified_from_dev_root" in report.lanes[0].blockers
    assert "runtime_from_documents_or_icloud" in report.lanes[0].blockers


def test_apply_refuses_when_lane_not_candidate() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id="not_a_candidate_lane",
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_LANE_NOT_FOUND"
    assert report.mutation_performed is False


def test_apply_dry_run_mutates_nothing() -> None:
    def _must_not_run(_config):
        raise AssertionError("guarded route must not be invoked in dry-run")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        dry_run=True,
        guarded_route_runner=_must_not_run,
    )

    assert report.result_classification == "LEAK_TEST_DRY_RUN_READY"
    assert report.mutation_performed is False
    assert report.apply_result is not None
    assert report.apply_result.dry_run is True


def test_apply_missing_authorization_blocks_before_bridge() -> None:
    def _must_not_run(_config):
        raise AssertionError("guarded route must not be invoked without leak-test authorization")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        guarded_route_runner=_must_not_run,
        readiness_checker=_ready_precheck,
    )

    assert report.result_classification == "LEAK_TEST_AUTHORIZATION_MISSING"
    assert report.mutation_performed is False
    assert report.apply_result is not None
    assert report.apply_result.authorization_status == "LEAK_TEST_AUTHORIZATION_MISSING"


def test_apply_refuses_expired_authorization_before_bridge(tmp_path: Path) -> None:
    def _must_not_run(_config):
        raise AssertionError("guarded route must not be invoked with expired authorization")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path, expired=True),
        guarded_route_runner=_must_not_run,
        readiness_checker=_ready_precheck,
    )

    assert report.result_classification == "LEAK_TEST_AUTHORIZATION_EXPIRED"
    assert report.mutation_performed is False


def test_apply_refuses_authorization_digest_mismatch_before_bridge(tmp_path: Path) -> None:
    path = _authorization_path(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["symbol"] = "GC"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=path,
        guarded_route_runner=lambda _config: (_ for _ in ()).throw(AssertionError("guarded route must not run")),
        readiness_checker=_ready_precheck,
    )

    assert report.result_classification == "LEAK_TEST_AUTHORIZATION_DIGEST_MISMATCH"
    assert report.mutation_performed is False


def test_apply_refuses_authorization_identity_mismatch_before_bridge(tmp_path: Path) -> None:
    path = _authorization_path(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["account_id"] = "DU123456"
    payload["digest"] = _auth_digest(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=path,
        guarded_route_runner=lambda _config: (_ for _ in ()).throw(AssertionError("guarded route must not run")),
        readiness_checker=_ready_precheck,
    )

    assert report.result_classification == "LEAK_TEST_AUTHORIZATION_IDENTITY_MISMATCH"
    assert report.mutation_performed is False


def test_apply_refuses_stale_governance_before_bridge(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: (_ for _ in ()).throw(AssertionError("guarded route must not run")),
        readiness_checker=_stale_precheck,
    )

    assert report.result_classification == "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    assert report.mutation_performed is False


def test_apply_retry_blocks_micro_stale_market_data_before_bridge(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: (_ for _ in ()).throw(AssertionError("guarded route must not run")),
        readiness_checker=_market_stale_precheck,
    )

    assert report.result_classification == "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE"
    assert report.mutation_performed is False


def test_apply_precheck_only_with_selected_fresh_unrelated_stale_reaches_ready_without_mutation(tmp_path: Path) -> None:
    def _must_not_run(_config):
        raise AssertionError("precheck-only must not invoke the guarded route")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_must_not_run,
        readiness_checker=lambda _repo_root, lane, safety: _pre_apply_readiness_check(
            repo_root=_readiness_repo(
                tmp_path / "readiness",
                selected_row=_selected_lane_row(),
                unrelated_rows=[_unrelated_stale_row()],
            ),
            lane=lane,
            safety=safety,
        ),
        precheck_only=True,
    )

    assert report.result_classification == "LEAK_TEST_PRECHECK_READY"
    assert report.mutation_performed is False
    assert report.apply_result is not None
    assert report.apply_result.pre_apply_readiness is not None
    assert report.apply_result.pre_apply_readiness["unrelated_market_data_stale_count"] == 1


def test_apply_blocked_entry_returns_safe_classification(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _bridge_result("PAPER_STRATEGY_INTENT_BLOCKED", status="blocked"),
        readiness_checker=_ready_precheck,
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert report.mutation_performed is True
    assert report.apply_result is not None
    assert report.apply_result.entry is not None
    assert report.apply_result.entry.classification == "BLOCKED"
    assert report.apply_result.entry.submit_attempted is False


def test_blocked_entry_after_submit_requires_broker_refresh_and_is_not_safe(tmp_path: Path) -> None:
    refresh_calls = []

    def _refresh(_repo_root: Path, stage: str) -> dict[str, object]:
        refresh_calls.append(stage)
        return _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
            track_b_broker_positions=[{"symbol": "GC", "local_symbol": "GCM6", "quantity": "1"}],
        )

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _blocked_after_submit_bridge_result(),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=_refresh,
        lifecycle_adoption_runner=_adoption_refused,
        reconciliation_reader=_reader_for({"after_entry": _clean_flat_reconciliation()}),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP"
    assert refresh_calls == ["entry_blocked_post_submit"]
    assert report.apply_result is not None
    assert report.apply_result.entry is not None
    assert report.apply_result.entry.classification == "BLOCKED"
    assert report.apply_result.entry.submit_attempted is True
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_GAP"


def test_unknown_result_before_submit_can_block_safely_without_refresh(tmp_path: Path) -> None:
    refresh_calls = []

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _unknown_bridge_result(submit_attempted=False),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, stage: refresh_calls.append(stage) or _clean_flat_reconciliation(),
    )

    assert report.result_classification == "LEAK_TEST_PASS_BLOCKED_SAFELY"
    assert refresh_calls == []
    assert report.apply_result is not None
    assert report.apply_result.entry is not None
    assert report.apply_result.entry.submit_attempted is False


def test_unknown_after_submit_requires_broker_refresh_and_stops(tmp_path: Path) -> None:
    refresh_calls = []

    def _refresh(_repo_root: Path, stage: str) -> dict[str, object]:
        refresh_calls.append(stage)
        return _clean_flat_reconciliation()

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _unknown_bridge_result(submit_attempted=True),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=_refresh,
    )

    assert report.result_classification == "LEAK_TEST_UNKNOWN_AFTER_SUBMIT_RESOLVED_NO_BROKER_EFFECT"
    assert refresh_calls == ["entry_unknown_post_submit"]
    assert report.apply_result is not None
    assert report.apply_result.entry is not None
    assert report.apply_result.entry.submit_attempted is True
    assert report.apply_result.exit is None


def test_unknown_after_submit_with_broker_position_is_not_safe_block(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _unknown_bridge_result(submit_attempted=True),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
            track_b_broker_positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1"}],
        ),
        lifecycle_adoption_runner=_adoption_refused,
        reconciliation_reader=_reader_for({"after_entry": _clean_flat_reconciliation()}),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP"
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_GAP"
    assert report.apply_result.exit is None


def test_unknown_after_submit_with_broker_position_adopts_and_runs_exit(tmp_path: Path) -> None:
    calls = []
    adoption_calls = []

    def _runner(config):
        calls.append(config)
        return _unknown_bridge_result(submit_attempted=True) if len(calls) == 1 else _bridge_result("PAPER_STRATEGY_ORDER_FILLED")

    def _adopt(**kwargs):
        adoption_calls.append(kwargs["entry_result"])
        return _adoption_applied()

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_runner,
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
            track_b_broker_positions=[{"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "1"}],
        ),
        lifecycle_adoption_runner=_adopt,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
                "after_exit": _clean_flat_reconciliation(),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_PASS_FULL_ROUND_TRIP"
    assert len(calls) == 2
    assert len(adoption_calls) == 1
    assert adoption_calls[0].classification != "FILLED"
    assert adoption_calls[0].submit_attempted is True
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_MATCHED"
    assert report.apply_result.lifecycle_close_result == "LIFECYCLE_CLOSED_FLAT"


def test_unknown_after_submit_retries_adoption_after_broker_truth_settles(tmp_path: Path) -> None:
    bridge_calls = []
    adoption_calls = []
    refresh_calls = []

    def _runner(config):
        bridge_calls.append(config)
        return _unknown_bridge_result(submit_attempted=True) if len(bridge_calls) == 1 else _bridge_result("PAPER_STRATEGY_ORDER_FILLED")

    def _adopt(**kwargs):
        adoption_calls.append(kwargs["entry_result"])
        return _adoption_refused_waiting_for_broker_truth() if len(adoption_calls) == 1 else _adoption_applied()

    def _refresh(_repo_root, stage):
        refresh_calls.append(stage)
        if len(adoption_calls) >= 2:
            return _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID)
        return _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
            track_b_broker_positions=[{"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "1"}],
        )

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_runner,
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=_refresh,
        lifecycle_adoption_runner=_adopt,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
                "after_exit": _clean_flat_reconciliation(),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_PASS_FULL_ROUND_TRIP"
    assert len(adoption_calls) == 2
    assert any(stage.endswith("after_adoption_attempt") for stage in refresh_calls)
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_MATCHED"
    assert report.apply_result.lifecycle_close_result == "LIFECYCLE_CLOSED_FLAT"


def test_unknown_after_submit_adoption_retry_stops_when_broker_position_stays_unadopted(tmp_path: Path) -> None:
    adoption_calls = []

    def _adopt(**kwargs):
        adoption_calls.append(kwargs["entry_result"])
        return _adoption_refused_waiting_for_broker_truth()

    def _refresh(_repo_root, _stage):
        return _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
            track_b_broker_positions=[{"symbol": "MNQ", "local_symbol": "MNQM6", "quantity": "1"}],
        )

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _unknown_bridge_result(submit_attempted=True),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=_refresh,
        lifecycle_adoption_runner=_adopt,
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP"
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_GAP"
    assert report.apply_result.exit is None
    assert len(adoption_calls) == 2


def test_unknown_after_submit_with_open_order_is_open_order_ambiguity(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _unknown_bridge_result(submit_attempted=True),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_open_order_count=1,
            track_b_broker_open_orders=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1"}],
        ),
    )

    assert report.result_classification == "LEAK_TEST_OPEN_ORDER_AMBIGUITY"
    assert report.apply_result is not None
    assert report.apply_result.exit is None


def test_apply_filled_entry_and_filled_exit_returns_full_round_trip_pass(tmp_path: Path) -> None:
    calls = []

    def _runner(config):
        calls.append(config)
        return _bridge_result("PAPER_STRATEGY_ORDER_FILLED")

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_runner,
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(),
        lifecycle_adoption_runner=_adoption_applied,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
                "after_exit": _clean_flat_reconciliation(),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_PASS_FULL_ROUND_TRIP"
    assert report.mutation_performed is True
    assert len(calls) == 2
    assert calls[0].caller_path == "track_b_paper_leak_test_apply"
    assert calls[0].leak_test_authorization_path is not None
    assert calls[0].caller_metadata["intent_type"] == "BUY_TO_OPEN"
    assert calls[1].caller_metadata["intent_type"] == "SELL_TO_CLOSE"
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_MATCHED"
    assert report.apply_result.lifecycle_close_result == "LIFECYCLE_CLOSED_FLAT"


def test_unknown_managed_exit_with_broker_flat_resolves_lifecycle_close(tmp_path: Path) -> None:
    calls = []
    refresh_calls = []
    resolution_calls = []
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-14T17:52:00Z|BUY_TO_OPEN"

    def _runner(config):
        calls.append(config)
        return _bridge_result("PAPER_STRATEGY_ORDER_FILLED") if len(calls) == 1 else _unknown_close_bridge_result(lifecycle_id=lifecycle_id)

    def _refresh(_repo_root: Path, stage: str) -> dict[str, object]:
        refresh_calls.append(stage)
        if stage.endswith("after_managed_exit_resolution"):
            return _clean_flat_reconciliation()
        return _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=0,
            lifecycle_open_position_count=1,
            track_b_lifecycle_positions=[_managed_position(symbol="MNQ", lane_id=LANE_ID)],
            blockers=["TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"],
        )

    def _resolve(**kwargs):
        resolution_calls.append(kwargs)
        return {"classification": "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP"}

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_runner,
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=_refresh,
        lifecycle_adoption_runner=_adoption_applied,
        managed_exit_resolution_runner=_resolve,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_PASS_FULL_ROUND_TRIP"
    assert refresh_calls == [
        "entry_after_adoption_attempt",
        "exit_unknown_post_submit",
        "exit_unknown_post_submit_after_managed_exit_resolution",
    ]
    assert len(resolution_calls) == 1
    assert resolution_calls[0]["lifecycle_id"] == lifecycle_id
    assert report.apply_result is not None
    assert report.apply_result.exit is not None
    assert report.apply_result.exit.order_id == "27"
    assert report.apply_result.exit.client_id == 11940
    assert report.apply_result.lifecycle_close_result == "LIFECYCLE_CLOSED_FLAT"


def test_unknown_managed_exit_with_working_order_stays_loud(tmp_path: Path) -> None:
    calls = []
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-14T17:52:00Z|BUY_TO_OPEN"

    def _runner(config):
        calls.append(config)
        return _bridge_result("PAPER_STRATEGY_ORDER_FILLED") if len(calls) == 1 else _unknown_close_bridge_result(lifecycle_id=lifecycle_id)

    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=_runner,
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_open_order_count=1,
            unknown_broker_open_order_count=1,
            lifecycle_open_position_count=1,
            track_b_broker_position_count=1,
            track_b_broker_open_orders=[
                {
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "action": "SELL",
                    "quantity": "1",
                    "order_id": "27",
                    "order_status": "Submitted",
                }
            ],
            blockers=[{"code": "UNKNOWN_BROKER_OPEN_ORDER"}],
        ),
        lifecycle_adoption_runner=_adoption_applied,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_OPEN_ORDER_AMBIGUITY"
    assert report.apply_result is not None
    assert report.apply_result.exit is not None
    assert report.apply_result.exit.order_id == "27"
    assert report.apply_result.lifecycle_close_result is None


def test_apply_entry_fill_lifecycle_gap_reports_failure(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _bridge_result("PAPER_STRATEGY_ORDER_FILLED"),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(
            broker_reconciled=False,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            track_b_broker_position_count=1,
            lifecycle_open_position_count=0,
        ),
        lifecycle_adoption_runner=_adoption_refused,
        reconciliation_reader=_reader_for({"after_entry": _clean_flat_reconciliation()}),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP"
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_open_result == "LIFECYCLE_OPEN_GAP"


def test_apply_exit_fill_lifecycle_gap_reports_failure(tmp_path: Path) -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
        authorization_path=_authorization_path(tmp_path),
        guarded_route_runner=lambda _config: _bridge_result("PAPER_STRATEGY_ORDER_FILLED"),
        readiness_checker=_ready_precheck,
        post_submit_broker_state_refresher=lambda _repo_root, _stage: _clean_flat_reconciliation(),
        lifecycle_adoption_runner=_adoption_applied,
        reconciliation_reader=_reader_for(
            {
                "after_entry": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
                "after_exit": _clean_managed_position_reconciliation(symbol="MNQ", lane_id=LANE_ID),
            }
        ),
        max_wait_seconds=0,
    )

    assert report.result_classification == "LEAK_TEST_EXIT_FILL_LIFECYCLE_GAP"
    assert report.apply_result is not None
    assert report.apply_result.lifecycle_close_result == "LIFECYCLE_CLOSE_GAP"


def test_result_classifications_cover_future_round_trip_outcomes() -> None:
    required = {
        "LEAK_TEST_PASS_FULL_ROUND_TRIP",
        "LEAK_TEST_PASS_BLOCKED_SAFELY",
        "LEAK_TEST_ENTRY_NOT_FILLED_CANCELLED",
        "LEAK_TEST_ENTRY_REJECTED",
        "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP",
        "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED",
        "LEAK_TEST_EXIT_REJECTED",
        "LEAK_TEST_EXIT_FILL_LIFECYCLE_GAP",
        "LEAK_TEST_BROKER_LIFECYCLE_MISMATCH",
        "LEAK_TEST_RUNTIME_RESTORE_FAILURE",
        "LEAK_TEST_PORTFOLIO_ARTIFACT_MISMATCH",
        "LEAK_TEST_REVIEW_REQUIRED",
        "LEAK_TEST_PASS_CONCURRENT_OPEN",
        "LEAK_TEST_PASS_CONCURRENT_EXIT_ONE_HOLD_OTHERS",
        "LEAK_TEST_PASS_CONCURRENT_RESTORE",
        "LEAK_TEST_BLOCKED_BY_EXPOSURE_POLICY",
        "LEAK_TEST_BLOCKED_BY_UNRESOLVED_STATE",
        "LEAK_TEST_STRATEGY_OWNERSHIP_COLLISION",
        "LEAK_TEST_SAME_SYMBOL_CONFLICT",
        "LEAK_TEST_PORTFOLIO_MULTI_POSITION_MISMATCH",
        "LEAK_TEST_AUTHORIZATION_MISSING",
        "LEAK_TEST_AUTHORIZATION_EXPIRED",
        "LEAK_TEST_AUTHORIZATION_DIGEST_MISMATCH",
        "LEAK_TEST_AUTHORIZATION_IDENTITY_MISMATCH",
        "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY",
        "LEAK_TEST_PRECHECK_MARKET_DATA_STALE",
        "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE",
        "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE",
        "LEAK_TEST_PRECHECK_READY",
        "LEAK_TEST_SUBMIT_STATE_AMBIGUOUS_BROKER_REFRESH_REQUIRED",
        "LEAK_TEST_ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        "LEAK_TEST_ENTRY_BROKER_FILLED_BUT_RESULT_UNKNOWN",
        "LEAK_TEST_OPEN_ORDER_AMBIGUITY",
        "LEAK_TEST_UNKNOWN_AFTER_SUBMIT_RESOLVED_NO_BROKER_EFFECT",
    }
    assert required.issubset(set(RESULT_CLASSIFICATIONS))


def test_production_governance_scopes_market_data_faults_before_blocking() -> None:
    source = (REPO_ROOT / "src/mgc_v05l/execution/ibkr_paper_strategy_governance.py").read_text(encoding="utf-8")

    assert "_scoped_backend_source_fault_counts" in source
    assert "required_instruments=_backend_source_required_instruments" in source
    assert "global_market_data_stale_count" in source
    assert 'block_reasons.append("source_market_data_stale")' in source


def test_no_live_money_or_forbidden_broker_paths_in_harness_source() -> None:
    source = (REPO_ROOT / "src/mgc_v05l/app/track_b_paper_leak_test.py").read_text(encoding="utf-8")

    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
