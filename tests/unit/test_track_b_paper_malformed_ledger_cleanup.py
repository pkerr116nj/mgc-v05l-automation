from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.app.track_b_paper_malformed_ledger_cleanup import (
    DEFAULT_LIFECYCLE_ID,
    DEFAULT_SOURCE_INTENT_ID,
    MalformedLedgerCleanupConfig,
    run_track_b_paper_malformed_ledger_cleanup,
)
from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
    VOID_MALFORMED_STALE_ARTIFACT,
    build_track_b_paper_trade_summaries,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def test_dry_run_identifies_exact_malformed_row_without_writing(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_DRY_RUN_READY"
    assert result.report["write_plan"]["would_append_reconciliation_record"] is True
    assert result.report["evidence"]["malformed_identity"]["confirmed"] is True
    assert result.report["evidence"]["durable_fill_absence"]["confirmed_absent"] is True
    assert result.report["after_prediction"]["reconciliation_would_clear"] is True
    assert result.audit_path.exists()
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_dry_run_refuses_broad_or_mismatched_cleanup(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, lifecycle_id="bridge_fill_MNQ|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN")

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"
    assert any("Expected exactly one target" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_apply_supersedes_only_exact_malformed_row(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_other_open=True)

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    rows = _read_jsonl(_ledger_path(tmp_path))
    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
    assert len(rows) == 3
    assert rows[-1]["record_type"] == "ARTIFACT_RECONCILIATION"
    assert rows[-1]["lifecycle_id"] == DEFAULT_LIFECYCLE_ID
    assert rows[-1]["new_artifact_classification"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT
    assert rows[-1]["reconciliation_action"] == "SUPERSEDED_MANUAL_RECONCILIATION_REQUIRED"
    assert rows[-1]["historical_broker_backed_exposure_confirmed"] is True
    assert rows[-1]["excluded_from_strategy_managed_pnl"] is True
    assert rows[-1]["manual_reconciliation_review_path"].endswith("mnq_manual_reconciliation_close_review.json")
    assert rows[-1]["submit_attempted"] is False
    assert rows[-1]["place_order_attempted"] is False
    assert rows[1]["lifecycle_id"] == "bridge_fill_MNQ|1m|2026-05-09T12:00:00Z|BUY_TO_OPEN"


def test_second_apply_is_idempotent(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)
    config = MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True)

    first = run_track_b_paper_malformed_ledger_cleanup(config=config, now=NOW)
    second = run_track_b_paper_malformed_ledger_cleanup(config=config, now=NOW)

    assert first.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
    assert second.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_ALREADY_APPLIED"
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 2


def test_refuses_if_broker_truth_shows_mnq_nonzero(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, broker_mnq_qty="1")

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"
    assert any("non-flat MNQ" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_refuses_if_open_orders_exist(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, open_order_count=1)

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"
    assert any("open orders are not zero" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_durable_may8_fill_evidence_is_preserved_when_manual_close_matches(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_may8_filled_bridge_result=True)

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    rows = _read_jsonl(_ledger_path(tmp_path))
    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
    assert result.report["evidence"]["durable_fill_absence"]["matching_filled_bridge_result_count"] == 1
    assert result.report["evidence"]["manual_reconciliation_close"]["confirmed"] is True
    assert rows[-1]["new_artifact_classification"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT


def test_refuses_if_lifecycle_contract_or_conid_mismatch(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, con_id=770561202)

    result = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"
    assert any("identity mismatch" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_reconciliation_remains_blocked_when_malformed_row_is_not_superseded(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_blocked_intent=False)
    _write_initial_summaries(tmp_path)

    cleanup = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )
    report = reconcile_track_b_paper_broker_truth(config=_reconciliation_config(tmp_path), now=NOW)

    assert cleanup.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_REFUSED"
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_reconciliation_clears_after_malformed_row_superseded_and_other_row_closed(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_closed_cleaned_mnq=True)

    cleanup = run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )
    report = reconcile_track_b_paper_broker_truth(config=_reconciliation_config(tmp_path), now=NOW)

    assert cleanup.classification == "TRACK_B_MALFORMED_LEDGER_CLEANUP_APPLIED"
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["track_b_broker_position_count"] == 0
    assert report["lifecycle_open_position_count"] == 0


def test_manual_reconciled_malformed_row_excluded_from_open_positions_and_pnl(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)

    run_track_b_paper_malformed_ledger_cleanup(
        config=MalformedLedgerCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )
    ledger = _ledger_path(tmp_path)
    summaries = build_track_b_paper_trade_summaries(
        ledger_records=_read_jsonl(ledger),
        ledger_jsonl=ledger,
        trade_summary_json=ledger.parent / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=ledger.parent / "latest_track_b_live_position_status.json",
        pnl_summary_json=ledger.parent / "latest_track_b_pnl_summary.json",
        now=NOW,
    )

    assert summaries["trade_summary"]["open_position_count"] == 0
    assert summaries["trade_summary"]["open_position_record_count"] == 0
    assert summaries["trade_summary"]["broker_backed_trade_count"] == 0
    assert summaries["pnl_summary"]["total_realized_pnl_today"] == "0"
    recent = summaries["trade_summary"]["recent_trades"][0]
    assert recent["final_position_status"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT
    assert recent["historical_broker_backed_exposure_confirmed"] is True
    assert recent["broker_backed_position_confirmed"] is False


def test_legacy_void_classification_still_preserves_historical_broker_exposure(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)
    ledger = _ledger_path(tmp_path)
    rows = _read_jsonl(ledger)
    rows.append(
        {
            "record_type": "ARTIFACT_RECONCILIATION",
            "lifecycle_id": DEFAULT_LIFECYCLE_ID,
            "new_artifact_classification": VOID_MALFORMED_STALE_ARTIFACT,
            "created_at": NOW.isoformat(),
        }
    )
    _write_jsonl(ledger, rows)

    summaries = build_track_b_paper_trade_summaries(
        ledger_records=_read_jsonl(ledger),
        ledger_jsonl=ledger,
        trade_summary_json=ledger.parent / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=ledger.parent / "latest_track_b_live_position_status.json",
        pnl_summary_json=ledger.parent / "latest_track_b_pnl_summary.json",
        now=NOW,
    )

    recent = summaries["trade_summary"]["recent_trades"][0]
    assert summaries["trade_summary"]["open_position_count"] == 0
    assert summaries["pnl_summary"]["total_realized_pnl_today"] == "0"
    assert recent["artifact_reconciliation_classification"] == VOID_MALFORMED_STALE_ARTIFACT
    assert recent["historical_broker_backed_exposure_confirmed"] is True
    assert recent["excluded_from_strategy_managed_pnl"] is True


def _write_cleanup_fixture(
    tmp_path: Path,
    *,
    lifecycle_id: str = DEFAULT_LIFECYCLE_ID,
    con_id: int = 770561201,
    broker_mnq_qty: str = "0",
    open_order_count: int = 0,
    include_blocked_intent: bool = True,
    include_may8_filled_bridge_result: bool = False,
    include_other_open: bool = False,
    include_closed_cleaned_mnq: bool = False,
) -> None:
    rows = [_malformed_row(lifecycle_id=lifecycle_id, con_id=con_id)]
    if include_other_open:
        rows.append(_other_open_row())
    if include_closed_cleaned_mnq:
        rows.extend([_may12_open_row(), _may12_close_row()])
    _write_jsonl(_ledger_path(tmp_path), rows)
    if include_blocked_intent:
        _write_jsonl(_lane_path(tmp_path) / "blocked_strategy_intents.jsonl", [_blocked_intent_row()])
    else:
        _write_jsonl(_lane_path(tmp_path) / "blocked_strategy_intents.jsonl", [])
    filled_rows = [_may12_bridge_row()]
    if include_may8_filled_bridge_result:
        filled_rows.append(_may8_bridge_row())
    _write_jsonl(_lane_path(tmp_path) / "filled_bridge_results.jsonl", filled_rows)
    _write_jsonl(_lane_path(tmp_path) / "trades.jsonl", [])
    _write_session_close_review(tmp_path)
    _write_manual_reconciliation_review(tmp_path)
    _write_broker_truth(tmp_path, broker_mnq_qty=broker_mnq_qty, open_order_count=open_order_count)


def _malformed_row(*, lifecycle_id: str = DEFAULT_LIFECYCLE_ID, con_id: int = 770561201) -> dict[str, object]:
    return {
        "ledger_schema_version": "track_b_paper_trade_ledger_v1",
        "trade_id": f"mnq_1x_ny_early_core__us_late_long:{lifecycle_id}",
        "strategy_id": "mnq_1x_ny_early_core__us_late_long",
        "lifecycle_id": lifecycle_id,
        "signal_id": "MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": con_id,
        "side": "LONG",
        "quantity": "1",
        "entry_timestamp": "2026-05-08T17:36:29.205805+00:00",
        "entry_fill_price": "29307.75",
        "exit_fill_price": None,
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "broker_backed_position_confirmed": True,
        "review_required": False,
        "source": "TRACK_B_DIRECT_BRIDGE_FILL_ARTIFACT",
    }


def _other_open_row() -> dict[str, object]:
    row = _malformed_row(lifecycle_id="bridge_fill_MNQ|1m|2026-05-09T12:00:00Z|BUY_TO_OPEN")
    row["trade_id"] = "other:bridge_fill_MNQ|1m|2026-05-09T12:00:00Z|BUY_TO_OPEN"
    row["entry_timestamp"] = "2026-05-09T12:00:30+00:00"
    row["entry_fill_price"] = "29000"
    row["signal_id"] = "MNQ|1m|2026-05-09T12:00:00Z|BUY_TO_OPEN"
    return row


def _may12_open_row() -> dict[str, object]:
    return {
        **_malformed_row(lifecycle_id="bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"),
        "trade_id": "mnq:bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "signal_id": "MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        "entry_timestamp": "2026-05-12T19:05:26.191844+00:00",
        "entry_fill_price": "28981.25",
    }


def _may12_close_row() -> dict[str, object]:
    row = _may12_open_row()
    row.update(
        {
            "exit_timestamp": "2026-05-13T11:00:38.198088+00:00",
            "exit_fill_price": "29389.5",
            "exit_intent_id": "MNQ|1m|2026-05-13T10:59:00Z|SELL_TO_CLOSE",
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "final_position_status": "CLOSED_FLAT",
            "realized_pnl": "816.5",
        }
    )
    return row


def _blocked_intent_row() -> dict[str, object]:
    return {
        "order_intent_id": DEFAULT_SOURCE_INTENT_ID,
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "lane_id": "mnq_1x_ny_early_core__us_late_long",
        "symbol": "MNQ",
        "intent_type": "BUY_TO_OPEN",
        "side": "BUY",
        "submit_allowed": False,
        "blocker_classification": "PRE_SUBMIT_GATE_BLOCKED",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "exact_blocker_reason": "IBKR paper bridge returned a successful classification without a broker_order_id.",
    }


def _may8_bridge_row() -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "order_intent_id": DEFAULT_SOURCE_INTENT_ID,
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": 1,
        "fill_price": "29307.75",
        "fill_timestamp": "2026-05-08T17:36:29.205805+00:00",
    }


def _may12_bridge_row() -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "order_intent_id": "MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": 1,
        "fill_price": "28981.25",
        "fill_timestamp": "2026-05-12T19:05:26.191844+00:00",
    }


def _write_session_close_review(tmp_path: Path) -> None:
    _write_json(
        tmp_path
        / "outputs"
        / "operator_dashboard"
        / "paper_session_close_reviews"
        / "2026-05-08_2026-05-08T17-36-32.json",
        {
            "rows": [
                {
                    "lane_id": "mnq_1x_ny_early_core__us_late_long",
                    "latest_event_timestamp": "2026-05-08T17:36:00+00:00",
                    "session_verdict": "SIGNAL_NO_FILL",
                    "open_position": False,
                    "fill_count": 0,
                }
            ]
        },
    )


def _write_manual_reconciliation_review(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs" / "reports" / "mnq_manual_reconciliation_close" / "mnq_manual_reconciliation_close_review.json",
        {
            "classification": "UNKNOWN_REQUIRES_REVIEW_CLOSED_FLAT",
            "broker_position_after": 0.0,
            "review_required": True,
            "strategy_managed_exit": False,
            "strategy_pnl": False,
            "possible_source_execution": {
                "account_id": "DUM882026",
                "broker_order_id": 2,
                "client_id": 10902,
                "con_id": 770561201,
                "executed_at": "2026-05-08T18:17:41.701603+00:00",
                "execution_id": "0000e1a7.6a015c30.01.01",
                "local_symbol": "MNQM6",
                "perm_id": 895323400,
                "price": 29307.75,
                "quantity": 1.0,
                "side": "BOT",
                "symbol": "MNQ",
            },
            "close_execution": {
                "account_id": "DUM882026",
                "broker_order_id": 1,
                "client_id": 9085,
                "con_id": 770561201,
                "executed_at": "2026-05-08T18:17:41.701706+00:00",
                "execution_id": "0000e1a7.6a016db1.01.01",
                "local_symbol": "MNQM6",
                "perm_id": 895323723,
                "price": 29287.25,
                "quantity": 1.0,
                "side": "SLD",
                "symbol": "MNQ",
            },
        },
    )


def _write_broker_truth(tmp_path: Path, *, broker_mnq_qty: str, open_order_count: int) -> None:
    broker_root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    generated_at = NOW.isoformat()
    positions_path = broker_root / "ibkr_positions_snapshot.json"
    orders_path = broker_root / "ibkr_open_orders_snapshot.json"
    _write_json(
        broker_root / "ibkr_broker_truth_refresh_status.json",
        {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "generated_at": generated_at,
            "latest_refresh_time": generated_at,
            "last_success": True,
            "read_only": True,
            "account": "DUM882026",
            "positions_complete": True,
            "open_orders_complete": True,
            "open_order_count": open_order_count,
            "positions_snapshot_path": str(positions_path),
            "open_orders_snapshot_path": str(orders_path),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        positions_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "positions_complete": True,
            "request_method": "reqPositions",
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "quantity": broker_mnq_qty,
                    "average_cost": "0",
                    "multiplier": "2",
                }
            ],
        },
    )
    _write_json(
        orders_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "open_orders_complete": True,
            "request_method": "reqAllOpenOrders",
            "auto_open_orders_requested": False,
            "order_binding_requested": False,
            "open_order_count": open_order_count,
            "open_orders": [{} for _ in range(open_order_count)],
        },
    )


def _write_initial_summaries(tmp_path: Path) -> None:
    ledger = _ledger_path(tmp_path)
    root = ledger.parent
    summaries = build_track_b_paper_trade_summaries(
        ledger_records=_read_jsonl(ledger),
        ledger_jsonl=ledger,
        trade_summary_json=root / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=root / "latest_track_b_live_position_status.json",
        pnl_summary_json=root / "latest_track_b_pnl_summary.json",
        now=NOW,
    )
    _write_json(root / "latest_track_b_paper_trade_summary.json", summaries["trade_summary"])
    _write_json(root / "latest_track_b_live_position_status.json", summaries["live_position_status"])
    _write_json(root / "latest_track_b_pnl_summary.json", summaries["pnl_summary"])


def _reconciliation_config(tmp_path: Path) -> ReconciliationConfig:
    return ReconciliationConfig(
        repo_root=tmp_path,
        ledger_root=tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        broker_truth_root=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        report_path=tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest.json",
        max_age_seconds=120.0,
    )


def _lane_path(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "lanes"
        / "mnq_1x_ny_early_core__us_late_long"
    )


def _ledger_path(tmp_path: Path) -> Path:
    return tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
