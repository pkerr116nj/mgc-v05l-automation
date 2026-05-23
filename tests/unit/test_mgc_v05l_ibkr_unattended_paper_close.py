from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution.ibkr_unattended_paper_close import (
    IbkrUnattendedPaperCloseArtifacts,
    IbkrUnattendedPaperCloseConfig,
    _classify_lifecycle,
    _derive_marketable_sell_limit,
    _matching_rows_for_order,
    _order_still_working,
    _shared_truth_close_evidence,
    evaluate_unattended_paper_close_caller,
    render_ibkr_unattended_paper_close_markdown,
    run_ibkr_unattended_paper_close,
    write_ibkr_unattended_paper_close_artifacts,
)


def _config(tmp_path: Path, **overrides: object) -> IbkrUnattendedPaperCloseConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9191,
        "unattended_paper": True,
        "account_id": "DUM882026",
    }
    payload.update(overrides)
    return IbkrUnattendedPaperCloseConfig(**payload)


def test_strategy_style_caller_fails_closed() -> None:
    result = evaluate_unattended_paper_close_caller(
        caller_path="unattended_paper_cli",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.paper"}))],
    )

    assert result["passed"] is False


def test_supervised_executor_caller_is_allowed() -> None:
    result = evaluate_unattended_paper_close_caller(
        caller_path="ibkr_paper_strategy_executor",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.execution.ibkr_paper_strategy_executor"}))],
    )

    assert result["passed"] is True


def test_derive_marketable_sell_limit_uses_delayed_bid(tmp_path: Path) -> None:
    limit_price = _derive_marketable_sell_limit(
        quote_context={
            "quote_source_label": "DELAYED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "bid_price": 4590.9,
            "last_price": 4591.0,
        },
        contract_report={"api_contract_details": [{"min_tick": 0.1}]},
        delayed_quote_max_age_seconds=9999.0,
        offset_ticks=1.0,
    )

    assert limit_price == 4590.8


def test_run_blocks_without_unattended_flag(tmp_path: Path) -> None:
    artifacts = run_ibkr_unattended_paper_close(config=_config(tmp_path, unattended_paper=False))

    assert artifacts.classification == "IBKR_UNATTENDED_CLOSE_UNKNOWN"
    assert artifacts.report["lifecycle"]["status"] == "blocked"
    assert artifacts.report["shared_truth_evidence"]["source_authority"] == "execution_core_authority"
    assert artifacts.report["shared_truth_evidence"]["dashboard_projection_consumed"] is False


def test_shared_truth_close_allows_exact_clean_open_managed_target(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path)

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert evidence["blockers"] == []
    assert evidence["classifications"]["open_order_truth"] == "NO_OPEN_ORDERS"
    assert evidence["target_agreement"]["position_truth"]["matching_row_count"] == 1
    assert evidence["target_agreement"]["managed_position_registry"]["matching_open_managed_row_count"] == 1


def test_shared_truth_close_blocks_suspicious_open_order_truth(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path, open_order_truth_classification="SUSPICIOUS_ORDER_STATE")

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert any("Open Order Truth is not safe" in blocker for blocker in evidence["blockers"])


def test_shared_truth_close_blocks_existing_managed_close_order(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path, managed_order_classification="WORKING_CLOSE_ORDER")

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert any("Managed Order Registry is not safe" in blocker for blocker in evidence["blockers"])


def test_shared_truth_close_blocks_order_adjustment_planner_review(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path, order_adjustment_classification="REVIEW_REQUIRED_SUSPICIOUS_STATE")

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert any("Order Adjustment Planner blocks" in blocker for blocker in evidence["blockers"])


def test_shared_truth_close_blocks_runtime_supervisor_manual_review(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path, runtime_supervisor_classification="SUPERVISOR_MANUAL_REVIEW_REQUIRED")

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert any("Runtime Supervisor Authority blocks" in blocker for blocker in evidence["blockers"])


def test_shared_truth_close_blocks_broker_position_mismatch(tmp_path: Path) -> None:
    _write_shared_truth_for_close(tmp_path, quantity="-1", side="SHORT")

    evidence = _shared_truth_close_evidence(config=_config(tmp_path), now=_now())

    assert any("Position Truth does not show the exact broker position" in blocker for blocker in evidence["blockers"])
    assert any("Managed Position Registry does not show a matching OPEN_MANAGED" in blocker for blocker in evidence["blockers"])


def test_unattended_close_does_not_consume_dashboard_projection_as_authority() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "mgc_v05l"
        / "execution"
        / "ibkr_unattended_paper_close.py"
    ).read_text(encoding="utf-8")

    forbidden = [
        "latest_track_b_open_order_truth.json",
        "latest_track_b_managed_orders.json",
        "latest_track_b_position_truth.json",
        "latest_track_b_managed_positions.json",
        "latest_track_b_runtime_supervisor_authority.json",
    ]
    assert all(path not in source for path in forbidden)


def test_classify_lifecycle_variants() -> None:
    assert _classify_lifecycle({"status": "filled_flat"}) == "IBKR_UNATTENDED_CLOSE_FILLED_FLAT"
    assert _classify_lifecycle({"status": "rejected"}) == "IBKR_UNATTENDED_CLOSE_REJECTED"
    assert _classify_lifecycle({"status": "not_filled_cancelled"}) == "IBKR_UNATTENDED_CLOSE_NOT_FILLED_CANCELLED"
    assert _classify_lifecycle({"status": "working_submitted"}) == "IBKR_UNATTENDED_CLOSE_UNKNOWN"
    assert _classify_lifecycle({"status": "unknown_needs_review"}) == "IBKR_UNATTENDED_CLOSE_UNKNOWN"
    assert _classify_lifecycle({"status": "filled_not_flat"}) == "IBKR_UNATTENDED_CLOSE_UNKNOWN"


def test_perm_id_correlation_wins_over_reused_local_order_id() -> None:
    rows = [
        {
            "account_id": "DUM882026",
            "broker_order_id": 1,
            "client_id": 9157,
            "perm_id": 490708929,
            "con_id": 712565978,
            "side": "SLD",
            "quantity": 1.0,
            "executed_at": "2026-04-28T14:26:12.267979+00:00",
        },
        {
            "account_id": "DUM882026",
            "broker_order_id": 1,
            "client_id": 9191,
            "perm_id": 490708950,
            "con_id": 712565978,
            "side": "SLD",
            "quantity": 1.0,
            "executed_at": "2026-04-28T14:26:12.268274+00:00",
        },
    ]

    matched = _matching_rows_for_order(
        rows,
        order_id=1,
        perm_id=490708950,
        account_id="DUM882026",
        con_id=712565978,
        action="SELL",
        quantity=1.0,
        observed_after=datetime.fromisoformat("2026-04-28T14:11:12+00:00"),
    )

    assert len(matched) == 1
    assert matched[0]["perm_id"] == 490708950


def test_historical_rows_ignored_without_perm_id_or_time_window_match() -> None:
    rows = [
        {
            "account_id": "DUM882026",
            "broker_order_id": 1,
            "client_id": 9191,
            "perm_id": None,
            "con_id": 712565978,
            "side": "SLD",
            "quantity": 1.0,
            "executed_at": "2026-04-28T14:00:12+00:00",
        },
        {
            "account_id": "DUM882026",
            "broker_order_id": 1,
            "client_id": 9191,
            "perm_id": None,
            "con_id": 712565978,
            "side": "BOT",
            "quantity": 1.0,
            "executed_at": "2026-04-28T14:12:12+00:00",
        },
    ]

    matched = _matching_rows_for_order(
        rows,
        order_id=1,
        perm_id=None,
        account_id="DUM882026",
        con_id=712565978,
        action="SELL",
        quantity=1.0,
        observed_after=datetime.fromisoformat("2026-04-28T14:11:12+00:00"),
    )

    assert matched == []


def test_prior_positive_working_truth_survives_later_empty_snapshot() -> None:
    assert _order_still_working(
        latest_order_status={"status": "Submitted"},
        open_orders_snapshot={"open_orders": []},
        order_id=1,
        perm_id=490708950,
    ) is True


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    artifacts = IbkrUnattendedPaperCloseArtifacts(
        classification="IBKR_UNATTENDED_CLOSE_FILLED_FLAT",
        report={
            "classification": "IBKR_UNATTENDED_CLOSE_FILLED_FLAT",
            "generated_at": "2026-04-28T14:00:00+00:00",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "account_id": "DUM882026",
            "client_id": 9191,
            "exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "limit_price": 4590.8,
            "lifecycle": {"status": "filled_flat", "detail": "Flat verified.", "submitted_order_id": 1, "submitted_perm_id": 490000002},
            "callback_timeline_event_count": 5,
        },
        audit_events=[{"event_type": "submit_attempted"}],
        open_order_before={"open_order_count": 0},
        open_order_after_submit={"open_order_count": 0},
        open_order_after_cancel={"status": "not_run"},
        callback_timeline=[{"callback_name": "orderStatus"}],
        extra_artifacts={"positions_after_submit": {"positions": []}},
    )

    write_ibkr_unattended_paper_close_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_unattended_paper_close_test_report.json").exists()
    assert (tmp_path / "ibkr_unattended_paper_close_test_report.md").exists()
    assert (tmp_path / "ibkr_unattended_paper_close_test_audit.jsonl").exists()
    payload = json.loads((tmp_path / "ibkr_unattended_paper_close_test_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_unattended_paper_close_markdown(payload)
    assert "IBKR_UNATTENDED_CLOSE_FILLED_FLAT" in markdown


def _now() -> datetime:
    return datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)


def _write_shared_truth_for_close(
    repo: Path,
    *,
    open_order_truth_classification: str = "NO_OPEN_ORDERS",
    managed_order_classification: str = "NO_MANAGED_ORDERS",
    order_adjustment_classification: str = "NO_ACTION_NEEDED",
    runtime_supervisor_classification: str = "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
    quantity: str = "1",
    side: str = "LONG",
) -> None:
    generated_at = _now().isoformat()
    position_row = {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": "DUM882026",
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": quantity,
        "side": side,
        "lifecycle_status": "OPEN_MANAGED",
    }
    managed_position = {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": "DUM882026",
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": quantity,
        "side": side,
        "lifecycle_status": "OPEN_MANAGED",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    }
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {
            "generated_at": generated_at,
            "classification": open_order_truth_classification,
            "order_states": [],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "generated_at": generated_at,
            "classification": managed_order_classification,
            "managed_orders": [],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json",
        {
            "generated_at": generated_at,
            "classification": order_adjustment_classification,
            "plans": [],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": generated_at,
            "classification": "ATTENTION_REQUIRED",
            "summary": {"overall_classification": "ATTENTION_REQUIRED"},
            "position_states": [position_row],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {
            "generated_at": generated_at,
            "classification": "OPEN_MANAGED_MATCHED",
            "managed_positions": [managed_position],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {"generated_at": generated_at, "classification": runtime_supervisor_classification},
    )
    _write_json(
        repo
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": generated_at, "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
    )
    _write_json(
        repo / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": generated_at, "classification": "ACTIVE"},
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
