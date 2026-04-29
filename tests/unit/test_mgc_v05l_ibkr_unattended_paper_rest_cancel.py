from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone

from mgc_v05l.execution.ibkr_unattended_paper_rest_cancel import (
    IbkrUnattendedPaperRestCancelArtifacts,
    IbkrUnattendedPaperRestCancelConfig,
    _build_working_order_acceptance,
    _classify_lifecycle,
    _derive_non_marketable_buy_limit,
    evaluate_unattended_paper_caller,
    render_ibkr_unattended_paper_rest_cancel_markdown,
    run_ibkr_unattended_paper_rest_cancel,
    write_ibkr_unattended_paper_rest_cancel_artifacts,
)


def _config(tmp_path: Path, **overrides: object) -> IbkrUnattendedPaperRestCancelConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9181,
        "unattended_paper": True,
        "account_id": "DUM882026",
    }
    payload.update(overrides)
    return IbkrUnattendedPaperRestCancelConfig(**payload)


def test_strategy_style_caller_fails_closed() -> None:
    result = evaluate_unattended_paper_caller(
        caller_path="unattended_paper_cli",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.paper"}))],
    )

    assert result["passed"] is False


def test_derive_non_marketable_buy_limit_uses_delayed_bid(tmp_path: Path) -> None:
    limit_price = _derive_non_marketable_buy_limit(
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
    artifacts = run_ibkr_unattended_paper_rest_cancel(config=_config(tmp_path, unattended_paper=False))

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["lifecycle"]["status"] == "blocked"


def test_classify_lifecycle_variants() -> None:
    assert _classify_lifecycle({"status": "passed"}) == "IBKR_UNATTENDED_REST_CANCEL_PASSED"
    assert _classify_lifecycle({"status": "dialog_blocked"}) == "IBKR_UNATTENDED_REST_CANCEL_DIALOG_BLOCKED"
    assert _classify_lifecycle({"status": "rejected"}) == "IBKR_UNATTENDED_REST_CANCEL_REJECTED"
    assert _classify_lifecycle({"status": "filled_unexpectedly"}) == "IBKR_UNATTENDED_REST_CANCEL_FILLED_UNEXPECTEDLY"
    assert _classify_lifecycle({"status": "cancel_unknown"}) == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"


def test_working_order_acceptance_prefers_tws_confirmation(tmp_path: Path) -> None:
    accepted = _build_working_order_acceptance(
        config=_config(tmp_path, visible_in_tws=True, canceled_in_tws=True),
        lifecycle_result={
            "status": "passed",
            "submitted_order_id": 1,
            "open_order_after_submit": {"open_orders": [{"broker_order_id": 1}], "open_order_count": 1},
            "latest_order_status": {"status": "Submitted"},
        },
    )

    assert accepted["classification"] == "WORKING_ORDER_API_AND_TWS_VISIBLE"
    assert accepted["api_working_order_visible"] is True


def test_working_order_acceptance_distinguishes_fast_fill(tmp_path: Path) -> None:
    accepted = _build_working_order_acceptance(
        config=_config(tmp_path),
        lifecycle_result={"status": "filled_unexpectedly"},
    )

    assert accepted["classification"] == "WORKING_ORDER_FILLED_BEFORE_VISUAL_CONFIRMATION"


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    artifacts = IbkrUnattendedPaperRestCancelArtifacts(
        classification="IBKR_UNATTENDED_REST_CANCEL_PASSED",
        report={
            "classification": "IBKR_UNATTENDED_REST_CANCEL_PASSED",
            "generated_at": "2026-04-28T14:00:00+00:00",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "account_id": "DUM882026",
            "client_id": 9181,
            "exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "limit_price": 4590.8,
            "quote_context": {"quote_source_label": "DELAYED"},
            "pricing_context": {
                "quote_snapshot": {"bid_price": 4590.9},
                "distance_from_reference_price": 0.1,
                "distance_ticks": 1.0,
            },
            "open_order_before": {"open_order_count": 0},
            "working_order_acceptance": {
                "classification": "WORKING_ORDER_API_AND_TWS_VISIBLE",
                "api_working_order_visible": True,
                "visible_in_tws": True,
                "canceled_in_tws": True,
                "tws_visibility_pause_seconds": 12.0,
            },
            "lifecycle": {"status": "passed", "detail": "Passed.", "submitted_order_id": 1, "submitted_perm_id": 490000001, "latest_order_status": {"status": "Cancelled"}},
            "callback_timeline_event_count": 4,
        },
        audit_events=[{"event_type": "submit_attempted"}],
        open_order_before={"open_order_count": 0},
        open_order_after_submit={"open_order_count": 1},
        open_order_after_cancel={"open_order_count": 0},
        callback_timeline=[{"callback_name": "openOrder"}],
        extra_artifacts={"executions_after_submit": []},
    )

    write_ibkr_unattended_paper_rest_cancel_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_report.json").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_report.md").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_audit.jsonl").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_callback_timeline.jsonl").exists()
    payload = json.loads((tmp_path / "ibkr_unattended_paper_rest_cancel_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_unattended_paper_rest_cancel_markdown(payload)
    assert "IBKR_UNATTENDED_REST_CANCEL_PASSED" in markdown
    assert "WORKING_ORDER_API_AND_TWS_VISIBLE" in markdown
