from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app.track_b_portfolio_state import (
    MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
    TrackBPortfolioConfig,
    build_track_b_portfolio_state,
)

NOW = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)


def test_portfolio_state_reconciled_pl_mnq_positions_with_multiplier_pnl(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=True)

    assert state["account"] == "DUM882026"
    assert state["mode"] == "PAPER"
    assert state["live_money_eligible"] is False
    assert state["source_policy"]["research_artifacts_used_as_runtime_truth"] is False
    assert state["metadata"]["artifact_version"] == "track_b_portfolio_state_phase_a_v1"
    assert state["broker_reconciliation"]["broker_reconciled"] is True
    assert state["broker_reconciliation"]["status"] == "MATCHED"
    assert state["portfolio_summary"]["open_position_count"] == 2
    assert state["portfolio_summary"]["long_contract_count"] == "2"
    assert state["portfolio_summary"]["unrealized_pnl"] == "77"
    assert state["portfolio_summary"]["estimated_total_pnl"] == "-17"
    assert (tmp_path / "outputs/reports/track_b_portfolio/latest_track_b_portfolio_state.json").exists()
    assert (tmp_path / "outputs/reports/track_b_portfolio/calendar/latest_track_b_pnl_calendar.json").exists()

    rows = {row["symbol"]: row for row in state["open_positions"]}
    assert rows["PL"]["entry_source"] == "SUPERVISED_ADOPTION"
    assert rows["PL"]["status"] == "ADOPTED"
    assert rows["PL"]["unrealized_pnl_points"] == "1.5"
    assert rows["PL"]["unrealized_pnl_dollars"] == "75"
    assert rows["PL"]["broker_cost_basis_adjustment"]["broker_minus_lifecycle_points_per_contract"] == "0.0504"
    assert rows["MNQ"]["entry_source"] == "NORMAL_FILL"
    assert rows["MNQ"]["status"] == "MANAGED"
    assert rows["MNQ"]["unrealized_pnl_points"] == "1"
    assert rows["MNQ"]["unrealized_pnl_dollars"] == "2"
    assert any(alert["code"] == "ADOPTED_POSITION" for alert in state["alerts"])
    assert any(alert["code"] == "COST_BASIS_ADJUSTMENT_ABOVE_THRESHOLD" for alert in state["alerts"])


def test_clean_flat_state_is_clean_and_live_money_ineligible(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    broker_status = _read_json(config.broker_status_path)
    broker_status["position_count"] = 0
    _write_json(config.broker_status_path, broker_status)
    positions_snapshot = _read_json(config.positions_snapshot_path)
    positions_snapshot["positions"] = []
    _write_json(config.positions_snapshot_path, positions_snapshot)
    lifecycle_status = _read_json(config.lifecycle_position_status_path)
    lifecycle_status["positions_by_instrument"] = {}
    lifecycle_status["positions_by_strategy"] = {}
    lifecycle_status["open_position_count"] = 0
    _write_json(config.lifecycle_position_status_path, lifecycle_status)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=True)

    assert state["portfolio_summary"]["open_position_count"] == 0
    assert state["portfolio_summary"]["status"] == "CLEAN"
    assert state["broker_reconciliation"]["broker_reconciled"] is True
    assert state["live_money_eligible"] is False
    assert not state["alerts"]
    assert config.portfolio_timeseries_path.exists()


def test_broker_lifecycle_mismatch_creates_red_alert(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path, include_mnq_lifecycle=False)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    assert state["broker_reconciliation"]["status"] == "MISMATCH"
    alert = _alert(state, "BROKER_LIFECYCLE_MISMATCH")
    assert alert["severity"] == "RED"
    assert _alert(state, "BROKER_ONLY_POSITION")["severity"] == "RED"
    mnq = next(row for row in state["open_positions"] if row["symbol"] == "MNQ")
    assert mnq["strategy_id"] is None
    assert mnq["entry_source"] == "UNKNOWN"
    assert mnq["review_required"] is True


def test_lifecycle_only_position_creates_red_alert(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    positions_snapshot = _read_json(config.positions_snapshot_path)
    positions_snapshot["positions"] = [
        row for row in positions_snapshot["positions"] if row["symbol"] != "MNQ"
    ]
    _write_json(config.positions_snapshot_path, positions_snapshot)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    alert = _alert(state, "LIFECYCLE_ONLY_POSITION")
    assert alert["severity"] == "RED"
    assert state["broker_reconciliation"]["broker_reconciled"] is False
    assert state["portfolio_summary"]["status"] == "BLOCKED"


def test_stale_broker_truth_creates_red_alert_when_positions_open(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path, generated_at="2026-05-13T11:00:00+00:00")

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    alert = _alert(state, "STALE_BROKER_TRUTH")
    assert alert["severity"] == "RED"
    assert state["runtime_service_freshness"]["status"] == "DEGRADED"


def test_closed_trades_use_futures_multiplier_and_supervised_flatten_is_separate(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    _write_jsonl(
        config.ledger_jsonl_path,
        [
            {
                "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
                "lane_id": "mnq_1x_ny_early_core__us_late_long",
                "instrument_family": "MNQ",
                "contract_key": "MNQ-202606",
                "side": "LONG",
                "quantity": "1",
                "entry_fill_price": "29472.25",
                "entry_timestamp": "2026-05-13T17:36:19+00:00",
                "exit_fill_price": "29481.0",
                "exit_timestamp": "2026-05-13T17:44:33+00:00",
                "final_position_status": "CLOSED_FLAT",
                "paper_proof_invoked": False,
            },
            {
                "strategy_id": "atp_companion_v1__paper_pl_asia_us",
                "lane_id": "atp_companion_v1_pl_asia_us",
                "instrument_family": "PL",
                "contract_key": "PL-202607",
                "side": "LONG",
                "quantity": "1",
                "entry_fill_price": "2145.1",
                "entry_timestamp": "2026-05-13T00:57:24+00:00",
                "exit_fill_price": "2167.9",
                "exit_timestamp": "2026-05-13T14:10:22+00:00",
                "final_position_status": "CLOSED_FLAT",
                "filled_bridge_result_path": "outputs/reports/track_b_paper_lifecycle_adoption/adopt.json",
            },
        ],
    )

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    trades = {trade["symbol"]: trade for trade in state["closed_trades"]}
    assert trades["MNQ"]["realized_pnl"] == "17.5"
    assert trades["MNQ"]["clean_strategy_pnl_eligible"] is True
    assert trades["PL"]["realized_pnl"] == "1140"
    assert trades["PL"]["category"] == "SUPERVISED_FLATTEN"
    assert trades["PL"]["clean_strategy_pnl_eligible"] is False
    pl_bucket = next(item for item in state["strategy_pnl"] if item["strategy_id"] == "atp_companion_v1__paper_pl_asia_us")
    assert pl_bucket["realized_pnl"] == "0"
    assert pl_bucket["supervised_flatten_pnl"] == "1140"


def test_legacy_void_malformed_row_preserves_broker_backed_manual_event(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    lifecycle_id = "bridge_fill_MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN"
    _write_jsonl(
        config.ledger_jsonl_path,
        [
            {
                "strategy_id": "mnq_1x_ny_early_core__us_late_long",
                "instrument_family": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "entry_fill_price": "29307.75",
                "entry_timestamp": "2026-05-08T17:36:29+00:00",
                "final_position_status": "OPEN_MANAGED",
                "lifecycle_id": lifecycle_id,
                "broker_backed_position_confirmed": True,
            },
            {
                "record_type": "ARTIFACT_RECONCILIATION",
                "reconciliation_action": "VOID_MALFORMED_STALE_ARTIFACT",
                "new_artifact_classification": "VOID_MALFORMED_STALE_ARTIFACT",
                "final_position_status": "VOID_MALFORMED_STALE_ARTIFACT",
                "lifecycle_id": lifecycle_id,
                "strategy_id": "mnq_1x_ny_early_core__us_late_long",
                "instrument_family": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "created_at": "2026-05-13T12:45:20+00:00",
            },
        ],
    )

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    event = state["system_events"][0]
    assert event["classification"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT
    assert event["original_artifact_classification"] == "VOID_MALFORMED_STALE_ARTIFACT"
    assert event["historical_broker_backed_exposure_confirmed"] is True
    assert event["excluded_from_strategy_managed_pnl"] is True
    alert = _alert(state, MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT)
    assert alert["severity"] == "YELLOW"


def test_research_market_data_is_not_used_as_runtime_truth(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path, pl_market_research=True)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    pl = next(row for row in state["open_positions"] if row["symbol"] == "PL")
    assert pl["last_price"] is None
    assert pl["unrealized_pnl_dollars"] is None
    assert state["source_policy"]["research_artifacts_used_as_runtime_truth"] is False
    assert any(alert["code"] == "STALE_MARKET_DATA" for alert in state["alerts"])


def test_live_money_eligible_true_creates_red_alert_but_output_forces_false(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    broker_status = _read_json(config.broker_status_path)
    broker_status["live_money_eligible"] = True
    _write_json(config.broker_status_path, broker_status)

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)

    assert state["live_money_eligible"] is False
    assert state["metadata"]["live_money_eligible"] is False
    alert = _alert(state, "LIVE_MONEY_ELIGIBLE_TRUE")
    assert alert["severity"] == "RED"
    assert state["portfolio_summary"]["status"] == "BLOCKED"


def test_calendar_marks_profitable_contaminated_day_as_not_clean(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    _write_jsonl(
        config.ledger_jsonl_path,
        [
            {
                "strategy_id": "atp_companion_v1__paper_pl_asia_us",
                "lane_id": "atp_companion_v1_pl_asia_us",
                "symbol": "PL",
                "contract_key": "PL-202607",
                "side": "LONG",
                "entry_timestamp": "2026-05-13T00:57:24+00:00",
                "exit_timestamp": "2026-05-13T03:00:00+00:00",
                "realized_pnl": "125",
                "source_artifact_paths": ["outputs/reports/track_b_paper_lifecycle_adoption/adopt.json"],
            }
        ],
    )

    build_track_b_portfolio_state(config=config, now=NOW, write=True)
    calendar = _read_json(config.calendar_path)
    today = next(tile for tile in calendar["daily_tiles"] if tile["date"] == "2026-05-13")

    assert calendar["design"]["pnl_mode_toggles"] == ["realized_only", "realized_plus_unrealized", "closed_trade_only"]
    assert today["realized_pnl"] == "125"
    assert today["pnl_mode_values"]["realized_only"] == "125"
    assert today["sample_quality"] == "CONTAMINATED_PLUMBING_DEBUG_DAY"
    assert any(marker["code"] == "ADOPTED_POSITION" for marker in today["system_quality_markers"])
    assert calendar["drilldowns"]["2026-05-13"]["trade_list"][0]["realized_pnl"] == "125"


def test_calendar_keeps_manual_cleanup_visible_without_counting_as_trade_pnl(tmp_path: Path) -> None:
    config = _write_base_runtime(tmp_path)
    _write_jsonl(
        config.ledger_jsonl_path,
        [
            {
                "record_type": "ARTIFACT_RECONCILIATION",
                "reconciliation_action": "SUPERSEDED_MANUAL_RECONCILIATION_REQUIRED",
                "new_artifact_classification": MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
                "lifecycle_id": "bridge_fill_MGC|5m|2026-05-08T17:36:00+00:00|BUY_TO_OPEN",
                "strategy_id": "mnq_1x_ny_early_core__us_late_long",
                "instrument_family": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "original_entry_timestamp": "2026-05-13T01:00:00+00:00",
                "original_entry_fill_price": "29307.75",
                "created_at": "2026-05-13T12:45:20+00:00",
                "broker_backed_position_confirmed": True,
                "historical_broker_backed_exposure_confirmed": True,
                "excluded_from_strategy_managed_pnl": True,
                "excluded_from_clean_trade_stats": True,
                "reason_codes": [
                    "MALFORMED_LIFECYCLE_ID_BUG",
                    "MANUAL_RECONCILIATION_CLOSE_CONFIRMED",
                    "EXCLUDED_FROM_STRATEGY_PNL",
                ],
                "manual_reconciliation_review_path": (
                    "outputs/reports/mnq_manual_reconciliation_close/mnq_manual_reconciliation_close_review.json"
                ),
            },
            {
                "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
                "lane_id": "mnq_1x_ny_early_core__us_late_long",
                "instrument_family": "MNQ",
                "contract_key": "MNQ-202606",
                "side": "LONG",
                "entry_timestamp": "2026-05-13T02:00:00+00:00",
                "exit_timestamp": "2026-05-13T03:00:00+00:00",
                "realized_pnl": "50",
            },
        ],
    )

    build_track_b_portfolio_state(config=config, now=NOW, write=True)
    calendar = _read_json(config.calendar_path)
    today = next(tile for tile in calendar["daily_tiles"] if tile["date"] == "2026-05-13")
    drilldown = calendar["drilldowns"]["2026-05-13"]

    assert today["realized_pnl"] == "50"
    assert today["trade_count"] == 1
    assert today["sample_quality"] == "CONTAMINATED_PLUMBING_DEBUG_DAY"
    assert any(marker["code"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT for marker in today["system_quality_markers"])
    assert drilldown["trade_list"][0]["realized_pnl"] == "50"
    assert drilldown["incident_system_events"][0]["historical_broker_backed_exposure_confirmed"] is True
    assert drilldown["incident_system_events"][0]["excluded_from_strategy_managed_pnl"] is True

    state = build_track_b_portfolio_state(config=config, now=NOW, write=False)
    assert state["system_events"][0]["classification"] == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT
    assert state["system_events"][0]["excluded_from_strategy_managed_pnl"] is True
    mnq_bucket = next(
        item
        for item in state["strategy_pnl"]
        if item["strategy_id"] == "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long"
    )
    assert mnq_bucket["realized_pnl"] == "50"


def _write_base_runtime(
    tmp_path: Path,
    *,
    generated_at: str = "2026-05-13T11:59:30+00:00",
    include_mnq_lifecycle: bool = True,
    pl_market_research: bool = False,
) -> TrackBPortfolioConfig:
    config = TrackBPortfolioConfig(
        repo_root=tmp_path,
        broker_truth_root=tmp_path / "outputs/reports/ibkr_read_only_verification",
        ledger_root=tmp_path / "outputs/track_b_execution_core/paper_trade_ledger",
        market_data_root=tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data",
        runtime_feed_root=tmp_path / "outputs/track_b_execution_core/databento_live_runtime_feed",
        output_path=tmp_path / "outputs/reports/track_b_portfolio/latest_track_b_portfolio_state.json",
        calendar_path=tmp_path / "outputs/reports/track_b_portfolio/calendar/latest_track_b_pnl_calendar.json",
        daily_history_path=tmp_path / "outputs/reports/track_b_portfolio/calendar/track_b_daily_pnl.jsonl",
        broker_max_age_seconds=120,
        market_data_max_age_seconds=180,
        runtime_max_age_seconds=180,
    )
    config.broker_truth_root.mkdir(parents=True)
    config.ledger_root.mkdir(parents=True)
    _write_json(
        config.broker_status_path,
        {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "generated_at": generated_at,
            "latest_refresh_time": generated_at,
            "last_success": True,
            "read_only": True,
            "account": "DUM882026",
            "positions_complete": True,
            "open_orders_complete": True,
            "position_count": 2,
            "open_order_count": 0,
            "positions_snapshot_path": str(config.positions_snapshot_path),
            "open_orders_snapshot_path": str(config.open_orders_snapshot_path),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        config.positions_snapshot_path,
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
                    "symbol": "PL",
                    "local_symbol": "PLN6",
                    "security_type": "FUT",
                    "expiry": "20260729",
                    "quantity": "1.0",
                    "average_cost": "107257.52",
                    "multiplier": "50",
                },
                {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "security_type": "FUT",
                    "expiry": "20260618",
                    "quantity": "1.0",
                    "average_cost": "57963.12",
                    "multiplier": "2",
                },
            ],
        },
    )
    _write_json(
        config.open_orders_snapshot_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "open_orders_complete": True,
            "request_method": "reqAllOpenOrders",
            "open_orders": [],
        },
    )
    positions = {
        "PL-202607": {
            "strategy_id": "atp_companion_v1__paper_pl_asia_us",
            "lane_id": "atp_companion_v1_pl_asia_us",
            "lifecycle_id": "bridge_fill_PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            "contract_key": "PL-202607",
            "instrument_family": "PL",
            "local_symbol": "PLN6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "2145.1",
            "entry_timestamp": "2026-05-13T00:57:24+00:00",
            "entry_exec_id": "0000e1a7.6a06001d.01.01",
            "entry_perm_id": 1984099439,
            "entry_client_id": 10905,
            "entry_order_id": "1",
            "source_artifact_paths": ["outputs/reports/track_b_paper_lifecycle_adoption/adopt.json"],
        }
    }
    if include_mnq_lifecycle:
        positions["MNQ-202606"] = {
            "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "lane_id": "mnq_1x_ny_early_core__us_late_long",
            "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
            "contract_key": "MNQ-202606",
            "instrument_family": "MNQ",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
            "entry_timestamp": "2026-05-12T19:05:26+00:00",
            "entry_order_id": "1",
        }
    _write_json(
        config.lifecycle_position_status_path,
        {
            "schema_version": "track_b_live_position_status_v1",
            "as_of": generated_at,
            "account_id": "DUM882026",
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "positions_by_instrument": positions,
            "positions_by_strategy": {str(value["strategy_id"]): value for value in positions.values()},
            "open_position_count": len(positions),
            "open_order_count": 0,
            "review_required_positions": [],
            "source_artifact_paths": ["outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl"],
        },
    )
    _write_json(
        config.pnl_summary_path,
        {
            "schema_version": "track_b_pnl_summary_v1",
            "as_of": generated_at,
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "total_realized_pnl_today": "-94",
            "total_unrealized_pnl": "0",
            "review_required_count": 0,
            "by_strategy": {
                "atp_companion_v1__paper_pl_asia_us": {"realized_pnl": "0", "trade_count": 1},
                "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long": {
                    "realized_pnl": "-94",
                    "trade_count": 1,
                },
            },
        },
    )
    _write_json(
        config.trade_summary_path,
        {"as_of": generated_at, "source": "TRACK_B_LIFECYCLE_ARTIFACTS", "review_required_count": 0},
    )
    _write_candles(config, "PL", "2146.6", research=pl_market_research)
    _write_candles(config, "MNQ", "28982.25")
    return config


def _write_candles(config: TrackBPortfolioConfig, root: str, close: str, *, research: bool = False) -> None:
    _write_json(
        config.market_data_root / root / "1m/latest_runtime_candles.json",
        {
            "generated_at": "2026-05-13T11:59:30+00:00",
            "last_completed_bar_ts": "2026-05-13T11:59:00+00:00",
            "root": root,
            "source": "DATABENTO_LIVE_RUNTIME",
            "research_artifact_used": research,
            "source_artifact_paths": ["outputs/historical_playback/research.json"] if research else [],
            "bars": [
                {
                    "bar_start": "2026-05-13T11:58:00+00:00",
                    "bar_end": "2026-05-13T11:59:00+00:00",
                    "close": close,
                }
            ],
        },
    )


def _alert(state: dict[str, object], code: str) -> dict[str, object]:
    return next(alert for alert in state["alerts"] if alert["code"] == code)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
