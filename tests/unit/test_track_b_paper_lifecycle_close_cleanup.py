from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.app.track_b_paper_lifecycle_close_cleanup import (
    DEFAULT_ENTRY_LIFECYCLE_ID,
    DEFAULT_EXIT_INTENT_ID,
    LifecycleCloseCleanupConfig,
    run_track_b_paper_lifecycle_close_cleanup,
)
from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)
from mgc_v05l.execution_core.track_b_paper_trade_ledger import build_track_b_paper_trade_summaries


NOW = datetime(2026, 5, 13, 11, 45, tzinfo=timezone.utc)


def test_dry_run_detects_stale_mnq_row_and_would_close_it(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)

    result = run_track_b_paper_lifecycle_close_cleanup(config=LifecycleCloseCleanupConfig(repo_root=tmp_path), now=NOW)

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_DRY_RUN_READY"
    assert result.report["write_plan"]["would_append_close_record"] is True
    assert result.report["close_record"]["exit_intent_id"] == DEFAULT_EXIT_INTENT_ID
    assert result.report["close_record"]["realized_pnl"] == "816.5"
    assert result.report["post_cleanup_prediction"]["reconciliation_would_clear"] is True
    assert result.audit_path.exists()
    ledger_lines = _ledger_path(tmp_path).read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == 1


def test_apply_closes_stale_mnq_row(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)

    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
    rows = _read_jsonl(_ledger_path(tmp_path))
    assert len(rows) == 2
    assert rows[-1]["lifecycle_id"] == DEFAULT_ENTRY_LIFECYCLE_ID
    assert rows[-1]["final_position_status"] == "CLOSED_FLAT"
    assert rows[-1]["exit_perm_id"] == 852752717
    assert rows[-1]["exit_client_id"] == 10877
    assert rows[-1]["live_money_eligible"] is False
    status = json.loads(
        (tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json").read_text(
            encoding="utf-8"
        )
    )
    assert status["open_position_count"] == 0


def test_second_apply_is_idempotent(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)
    config = LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True)

    first = run_track_b_paper_lifecycle_close_cleanup(config=config, now=NOW)
    second = run_track_b_paper_lifecycle_close_cleanup(config=config, now=NOW)

    assert first.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
    assert second.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_ALREADY_APPLIED"
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 2


def test_refuses_when_broker_truth_is_not_flat(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, broker_mnq_qty="1")

    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"
    assert any("non-flat MNQ" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_refuses_on_conid_or_local_symbol_mismatch(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, exit_con_id=770561202)

    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"
    assert any("SELL_TO_CLOSE bridge fill" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_refuses_when_exit_fill_evidence_is_missing(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_exit=False)

    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"
    assert any("SELL_TO_CLOSE bridge fill" in failure for failure in result.report["failures"])
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 1


def test_reconciliation_remains_blocked_if_cleanup_cannot_prove_identity(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_exit=False)
    _write_initial_summaries(tmp_path)

    cleanup = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )
    report = reconcile_track_b_paper_broker_truth(config=_reconciliation_config(tmp_path), now=NOW)

    assert cleanup.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_reconciliation_clears_only_when_broker_truth_and_lifecycle_agree(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path)
    cleanup = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )
    report = reconcile_track_b_paper_broker_truth(config=_reconciliation_config(tmp_path), now=NOW)

    assert cleanup.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["lifecycle_open_position_count"] == 0
    assert report["track_b_broker_position_count"] == 0


def test_pl_close_cleanup_accepts_supervised_bridge_report_exit_evidence(tmp_path: Path) -> None:
    _write_pl_cleanup_fixture(tmp_path)

    result = run_track_b_paper_lifecycle_close_cleanup(config=_pl_cleanup_config(tmp_path, apply=True), now=NOW)

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
    rows = _read_jsonl(_ledger_path(tmp_path))
    assert len(rows) == 2
    assert rows[-1]["instrument_family"] == "PL"
    assert rows[-1]["final_position_status"] == "CLOSED_FLAT"
    assert rows[-1]["exit_perm_id"] == 852752718
    assert rows[-1]["exit_client_id"] == 11087
    assert rows[-1]["exit_fill_price"] == "2167.9"
    assert rows[-1]["realized_pnl"] == "1140"
    report = reconcile_track_b_paper_broker_truth(config=_reconciliation_config(tmp_path), now=NOW)
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"


def test_ambiguous_older_mnq_open_row_is_flagged_and_blocks_cleanup(tmp_path: Path) -> None:
    _write_cleanup_fixture(tmp_path, include_ambiguous_open=True)

    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(repo_root=tmp_path, apply=True),
        now=NOW,
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"
    assert result.report["ledger"]["ambiguous_open_mnq_rows"][0]["classification"] == "STALE_AMBIGUOUS_OPEN_MNQ_ROW"
    assert len(_read_jsonl(_ledger_path(tmp_path))) == 2


def _pl_cleanup_config(tmp_path: Path, *, apply: bool) -> LifecycleCloseCleanupConfig:
    return LifecycleCloseCleanupConfig(
        repo_root=tmp_path,
        lane_id="atp_companion_v1_pl_asia_us",
        strategy_id="atp_companion_v1__paper_pl_asia_us",
        symbol="PL",
        local_symbol="PLN6",
        con_id=644855286,
        side="LONG",
        entry_lifecycle_id="bridge_fill_PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
        entry_fill_time="2026-05-13T00:57:24.570513+00:00",
        entry_price=Decimal("2145.1"),
        exit_intent_id="runtime-exit|1778681404272|SELL_TO_CLOSE",
        exit_price=Decimal("2167.9"),
        exit_fill_time="2026-05-13T14:10:22.193724+00:00",
        exit_client_id=11087,
        exit_perm_id=852752718,
        exit_bridge_report_path=Path("outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us/ibkr_paper_strategy_bridge_report.json"),
        allow_ledger_entry_evidence=True,
        apply=apply,
    )


def _write_pl_cleanup_fixture(tmp_path: Path) -> None:
    _write_jsonl(
        _ledger_path(tmp_path),
        [
            {
                "ledger_schema_version": "track_b_paper_trade_ledger_v1",
                "trade_id": "atp_companion_v1__paper_pl_asia_us:bridge_fill_PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
                "strategy_id": "atp_companion_v1__paper_pl_asia_us",
                "lifecycle_id": "bridge_fill_PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
                "instrument_family": "PL",
                "contract_key": "PL-202607",
                "local_symbol": "PLN6",
                "con_id": 644855286,
                "side": "LONG",
                "quantity": "1",
                "entry_timestamp": "2026-05-13T00:57:24.570513+00:00",
                "entry_fill_price": "2145.1",
                "entry_order_id": "1",
                "entry_perm_id": 1984099439,
                "entry_client_id": 10905,
                "entry_broker_identity": {
                    "account_id": "DUM882026",
                    "broker_order_id": "1",
                    "client_id": 10905,
                    "con_id": 644855286,
                    "exec_id": "0000e1a7.6a06001d.01.01",
                    "local_symbol": "PLN6",
                    "perm_id": 1984099439,
                },
                "exit_fill_price": None,
                "paper_lifecycle_type": "STRATEGY_MANAGED",
                "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
                "final_position_status": "OPEN_MANAGED",
                "broker_backed_position_confirmed": True,
                "review_required": False,
                "source": "TRACK_B_DIRECT_BRIDGE_FILL_ARTIFACT",
            }
        ],
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us/ibkr_paper_strategy_bridge_report.json",
        {
            "classification": "PAPER_STRATEGY_ORDER_FILLED",
            "delegated_result": {
                "classification": "PAPER_CLOSE_FILLED_FLAT",
                "report": {
                    "submit_cancel_lifecycle": {
                        "latest_order_status": {
                            "status": "Filled",
                            "order_id": 1,
                            "client_id": 11087,
                            "perm_id": 852752718,
                            "filled": 1.0,
                            "avg_fill_price": 2167.9,
                        },
                        "fill_verification": {
                            "verified": True,
                            "executions_after_submit": [
                                {
                                    "account_id": "DUM882026",
                                    "broker_order_id": "1",
                                    "executed_at": "2026-05-13T14:10:22.193724+00:00",
                                    "execution_id": "0000e1a7.6a07098f.01.01",
                                    "price": "2167.9",
                                    "quantity": "1.0",
                                    "symbol": "PL",
                                }
                            ],
                        },
                        "close_position_verification": {"verified": True, "exact_position_quantity": 0.0},
                    }
                },
            },
        },
    )
    _write_broker_truth_for_symbol(tmp_path, symbol="PL", local_symbol="PLN6", expiry="20260729", qty="0.0", multiplier="50")


def _write_cleanup_fixture(
    tmp_path: Path,
    *,
    broker_mnq_qty: str = "0.0",
    exit_con_id: int = 770561201,
    include_exit: bool = True,
    include_ambiguous_open: bool = False,
) -> None:
    ledger_rows = []
    if include_ambiguous_open:
        ledger_rows.append(
            {
                "ledger_schema_version": "track_b_paper_trade_ledger_v1",
                "trade_id": "mnq_1x_ny_early_core__us_late_long:bridge_fill_MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN",
                "strategy_id": "mnq_1x_ny_early_core__us_late_long",
                "lifecycle_id": "bridge_fill_MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN",
                "instrument_family": "MNQ",
                "contract_key": "MNQ-202606",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
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
            }
        )
    ledger_rows.append(_open_mnq_ledger_row())
    _write_jsonl(_ledger_path(tmp_path), ledger_rows)
    bridge_rows = [_entry_bridge_row()]
    if include_exit:
        bridge_rows.append(_exit_bridge_row(con_id=exit_con_id))
    _write_jsonl(
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "lanes"
        / "mnq_1x_ny_early_core__us_late_long"
        / "filled_bridge_results.jsonl",
        bridge_rows,
    )
    _write_broker_truth(tmp_path, broker_mnq_qty=broker_mnq_qty)


def _open_mnq_ledger_row() -> dict[str, object]:
    return {
        "ledger_schema_version": "track_b_paper_trade_ledger_v1",
        "trade_id": (
            "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long:"
            "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
        ),
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "lifecycle_id": DEFAULT_ENTRY_LIFECYCLE_ID,
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "LONG",
        "quantity": "1",
        "entry_timestamp": "2026-05-12T19:05:26.191844+00:00",
        "entry_fill_price": "28981.25",
        "entry_order_id": "1",
        "entry_perm_id": 1984091341,
        "entry_client_id": 11121,
        "exit_fill_price": None,
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "broker_backed_position_confirmed": True,
        "review_required": False,
        "source": "TRACK_B_DIRECT_BRIDGE_FILL_ARTIFACT",
    }


def _entry_bridge_row() -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "lane_id": "mnq_1x_ny_early_core__us_late_long",
        "symbol": "MNQ",
        "action": "BUY",
        "quantity": 1,
        "intent_type": "BUY_TO_OPEN",
        "order_intent_id": "MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        "broker_order_id": "1",
        "client_id": 11121,
        "perm_id": 1984091341,
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "28981.25",
        "fill_timestamp": "2026-05-12T19:05:26.191844+00:00",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }


def _exit_bridge_row(*, con_id: int = 770561201) -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "lane_id": "mnq_1x_ny_early_core__us_late_long",
        "symbol": "MNQ",
        "action": "SELL",
        "quantity": 1,
        "intent_type": "SELL_TO_CLOSE",
        "order_intent_id": DEFAULT_EXIT_INTENT_ID,
        "broker_order_id": "1",
        "client_id": 10877,
        "perm_id": 852752717,
        "con_id": con_id,
        "local_symbol": "MNQM6",
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "29389.5",
        "fill_timestamp": "2026-05-13T11:00:38.198088+00:00",
        "position_side": "FLAT",
        "internal_position_qty": 0,
        "broker_position_qty": 0,
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }


def _write_broker_truth(tmp_path: Path, *, broker_mnq_qty: str) -> None:
    _write_broker_truth_for_symbol(
        tmp_path,
        symbol="MNQ",
        local_symbol="MNQM6",
        expiry="20260618",
        qty=broker_mnq_qty,
        multiplier="2",
        average_cost="0.0" if broker_mnq_qty == "0.0" else "57962.5",
    )


def _write_broker_truth_for_symbol(
    tmp_path: Path,
    *,
    symbol: str,
    local_symbol: str,
    expiry: str,
    qty: str,
    multiplier: str,
    average_cost: str = "0.0",
) -> None:
    broker_root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    positions_path = broker_root / "ibkr_positions_snapshot.json"
    orders_path = broker_root / "ibkr_open_orders_snapshot.json"
    generated_at = "2026-05-13T11:44:30+00:00"
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
            "position_count": 1,
            "open_order_count": 0,
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
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "expiry": expiry,
                    "security_type": "FUT",
                    "quantity": qty,
                    "average_cost": average_cost,
                    "multiplier": multiplier,
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
            "open_order_count": 0,
            "open_orders": [],
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
