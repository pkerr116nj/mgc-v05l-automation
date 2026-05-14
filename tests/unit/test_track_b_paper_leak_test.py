from __future__ import annotations

from pathlib import Path

from mgc_v05l.app.track_b_paper_leak_test import (
    LeakTestExposurePolicy,
    RESULT_CLASSIFICATIONS,
    build_concurrent_plan_report,
    build_plan_only_report,
    build_single_lane_apply_report,
    build_single_lane_dry_run_report,
    report_to_dict,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
LANE_ID = "mnq_1x_ny_early_core__us_midday_long"


def _runtime_command() -> str:
    return f"python -m mgc_v05l.app.main probationary-paper-soak --config {REPO_ROOT / 'config/base.yaml'}"


def _operator_status() -> dict[str, object]:
    return {"source_runtime_pid": 12345}


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
    assert lane.safe_to_test is True
    assert lane.safe_for_isolated_test is True
    assert lane.safe_for_concurrent_test is True
    assert payload["concurrent_scenarios"]
    assert payload["mutation_performed"] is False


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


def test_safe_apply_is_not_implemented_in_first_deliverable() -> None:
    report = build_single_lane_apply_report(
        repo_root=REPO_ROOT,
        lane_id=LANE_ID,
        reconciliation=_clean_flat_reconciliation(),
        operator_status=_operator_status(),
        runtime_command=_runtime_command(),
    )

    assert report.result_classification == "LEAK_TEST_APPLY_REQUIRES_EXPLICIT_APPROVAL_NOT_IMPLEMENTED"
    assert report.mutation_performed is False


def test_result_classifications_cover_future_round_trip_outcomes() -> None:
    required = {
        "LEAK_TEST_PASS_FULL_ROUND_TRIP",
        "LEAK_TEST_PASS_BLOCKED_SAFELY",
        "LEAK_TEST_ENTRY_NOT_FILLED_CANCELLED",
        "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP",
        "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED",
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
    }
    assert required.issubset(set(RESULT_CLASSIFICATIONS))


def test_no_live_money_or_forbidden_broker_paths_in_harness_source() -> None:
    source = (REPO_ROOT / "src/mgc_v05l/app/track_b_paper_leak_test.py").read_text(encoding="utf-8")

    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
