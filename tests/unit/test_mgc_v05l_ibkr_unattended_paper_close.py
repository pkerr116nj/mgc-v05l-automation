from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution.ibkr_unattended_paper_close import (
    IbkrUnattendedPaperCloseArtifacts,
    IbkrUnattendedPaperCloseConfig,
    _classify_lifecycle,
    _derive_marketable_sell_limit,
    _matching_rows_for_order,
    _order_still_working,
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


def test_derive_marketable_sell_limit_uses_delayed_bid(tmp_path: Path) -> None:
    limit_price = _derive_marketable_sell_limit(
        quote_context={
            "quote_source_label": "DELAYED",
            "updated_at": "2026-04-28T14:00:00+00:00",
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
