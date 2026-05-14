"""Read-only Track B PAPER portfolio, P&L, and calendar state artifacts.

This module consumes already-written broker, lifecycle, ledger, and runtime
artifacts. It never connects to a broker and never mutates broker state.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER

REPO_ROOT = Path(__file__).resolve().parents[3]
PAPER_ACCOUNT = "DUM882026"
DEFAULT_BROKER_TRUTH_ROOT = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"
DEFAULT_LEDGER_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
DEFAULT_MARKET_DATA_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_RUNTIME_FEED_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed"
DEFAULT_REPORT_ROOT = REPO_ROOT / "outputs" / "reports" / "track_b_portfolio"
DEFAULT_OUTPUT_PATH = DEFAULT_REPORT_ROOT / "latest_track_b_portfolio_state.json"
DEFAULT_PORTFOLIO_TIMESERIES_PATH = DEFAULT_REPORT_ROOT / "track_b_portfolio_timeseries.jsonl"
DEFAULT_CALENDAR_PATH = DEFAULT_REPORT_ROOT / "calendar" / "latest_track_b_pnl_calendar.json"
DEFAULT_DAILY_HISTORY_PATH = DEFAULT_REPORT_ROOT / "calendar" / "track_b_daily_pnl.jsonl"
DEFAULT_BROKER_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_PORTFOLIO_BROKER_MAX_AGE_SECONDS", "180"))
DEFAULT_MARKET_DATA_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_PORTFOLIO_MARKET_DATA_MAX_AGE_SECONDS", "300"))
DEFAULT_RUNTIME_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_PORTFOLIO_RUNTIME_MAX_AGE_SECONDS", "300"))
DEFAULT_COST_BASIS_ALERT_POINTS = Decimal(os.environ.get("TRACK_B_PORTFOLIO_COST_BASIS_ALERT_POINTS", "0.25"))

FUTURES_MULTIPLIERS: dict[str, Decimal] = {
    "MGC": Decimal("10"),
    "GC": Decimal("100"),
    "MNQ": Decimal("2"),
    "NQ": Decimal("20"),
    "MES": Decimal("5"),
    "ES": Decimal("50"),
    "PL": Decimal("50"),
    "MPL": Decimal("10"),
    "ZB": Decimal("1000"),
    "ZN": Decimal("1000"),
    "ZF": Decimal("1000"),
    "ZT": Decimal("2000"),
}
MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT = (
    "MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT"
)
VOID_MALFORMED_STALE_ARTIFACT = "VOID_MALFORMED_STALE_ARTIFACT"


@dataclass(frozen=True)
class TrackBPortfolioConfig:
    repo_root: Path = REPO_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    ledger_root: Path = DEFAULT_LEDGER_ROOT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    runtime_feed_root: Path = DEFAULT_RUNTIME_FEED_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    portfolio_timeseries_path: Path = DEFAULT_PORTFOLIO_TIMESERIES_PATH
    calendar_path: Path = DEFAULT_CALENDAR_PATH
    daily_history_path: Path = DEFAULT_DAILY_HISTORY_PATH
    account: str = PAPER_ACCOUNT
    mode: str = "PAPER"
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    broker_max_age_seconds: float = DEFAULT_BROKER_MAX_AGE_SECONDS
    market_data_max_age_seconds: float = DEFAULT_MARKET_DATA_MAX_AGE_SECONDS
    runtime_max_age_seconds: float = DEFAULT_RUNTIME_MAX_AGE_SECONDS
    cost_basis_alert_points: Decimal = DEFAULT_COST_BASIS_ALERT_POINTS

    @property
    def broker_status_path(self) -> Path:
        return self.broker_truth_root / "ibkr_broker_truth_refresh_status.json"

    @property
    def positions_snapshot_path(self) -> Path:
        return self.broker_truth_root / "ibkr_positions_snapshot.json"

    @property
    def open_orders_snapshot_path(self) -> Path:
        return self.broker_truth_root / "ibkr_open_orders_snapshot.json"

    @property
    def lifecycle_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_live_position_status.json"

    @property
    def reconciled_lifecycle_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_live_position_status.json"

    @property
    def pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_pnl_summary.json"

    @property
    def reconciled_pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_pnl_summary.json"

    @property
    def trade_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_paper_trade_summary.json"

    @property
    def ledger_jsonl_path(self) -> Path:
        return self.ledger_root / "track_b_paper_trade_ledger.jsonl"

    @property
    def service_status_paths(self) -> tuple[Path, ...]:
        return (
            self.broker_status_path,
            self.runtime_feed_root / "latest_live_feed_process_status.json",
            self.repo_root / "outputs" / "reports" / "phase1_databento_live_runtime_candles" / "latest_phase1_databento_live_supervisor_status.json",
            self.repo_root / "outputs" / "reports" / "paper_strategy_monitor" / "paper_strategy_monitor_runtime_status.json",
            self.repo_root / "var" / "paper_strategy_executor_loop_status.json",
            self.repo_root / "outputs" / "reports" / "track_b_operator_readiness_refresher" / "latest_track_b_operator_readiness_refresher_status.json",
        )


def build_track_b_portfolio_state(
    *,
    config: TrackBPortfolioConfig | None = None,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    config = config or TrackBPortfolioConfig()
    actual_now = _utc(now)
    broker_status = _load_json(config.broker_status_path)
    positions_path = _path_from_payload(broker_status.get("positions_snapshot_path"), default=config.positions_snapshot_path)
    open_orders_path = _path_from_payload(broker_status.get("open_orders_snapshot_path"), default=config.open_orders_snapshot_path)
    positions_snapshot = _load_json(positions_path)
    open_orders_snapshot = _load_json(open_orders_path)
    lifecycle_status = _load_json(config.lifecycle_position_status_path)
    reconciled_lifecycle_status = _load_json(config.reconciled_lifecycle_position_status_path)
    pnl_summary = _select_pnl_summary(config)
    trade_summary = _load_json(config.trade_summary_path)
    git_metadata = _git_metadata(config.repo_root)

    broker_positions = _track_b_broker_positions(positions_snapshot, config.symbols)
    broker_open_orders = _track_b_broker_open_orders(open_orders_snapshot, config.symbols)
    lifecycle_positions = _track_b_lifecycle_positions(lifecycle_status, config.symbols)
    match_report = _match_broker_to_lifecycle(broker_positions, lifecycle_positions, config.symbols)
    alerts: list[dict[str, Any]] = []
    live_money_inputs = _live_money_input_flags(
        broker_status,
        positions_snapshot,
        open_orders_snapshot,
        lifecycle_status,
        reconciled_lifecycle_status,
        pnl_summary,
        trade_summary,
    )
    if live_money_inputs:
        _add_alert(
            alerts,
            severity="RED",
            code="LIVE_MONEY_ELIGIBLE_TRUE",
            message="One or more source artifacts claimed live money eligibility; Track B Portfolio forces live_money_eligible=false.",
            details={"source_flags": live_money_inputs},
        )
    freshness = _freshness_summary(
        config=config,
        now=actual_now,
        broker_status=broker_status,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        positions_path=positions_path,
        open_orders_path=open_orders_path,
        open_position_count=len(broker_positions),
        alerts=alerts,
    )

    if match_report["status"] != "MATCHED":
        _add_alert(
            alerts,
            severity="RED",
            code="BROKER_LIFECYCLE_MISMATCH",
            message="Broker truth and Track B lifecycle disagree; broker truth remains authoritative for position existence.",
            details=match_report,
        )
        for broker_only in match_report.get("unmatched_broker_positions", []):
            if isinstance(broker_only, Mapping):
                _add_alert(
                    alerts,
                    severity="RED",
                    code="BROKER_ONLY_POSITION",
                    message="Broker truth reports a Track B position with no reconciled lifecycle owner.",
                    position_key=_position_key(broker_only),
                    details={"broker_position": broker_only},
                )
        for lifecycle_only in match_report.get("unmatched_lifecycle_positions", []):
            if isinstance(lifecycle_only, Mapping):
                _add_alert(
                    alerts,
                    severity="RED",
                    code="LIFECYCLE_ONLY_POSITION",
                    message="Lifecycle reports an open Track B position that broker truth does not currently hold.",
                    position_key=str(lifecycle_only.get("position_key") or lifecycle_only.get("lifecycle_id") or ""),
                    details={"lifecycle_position": lifecycle_only},
                )
    if broker_open_orders:
        _add_alert(
            alerts,
            severity="YELLOW",
            code="OPEN_ORDERS",
            message="Broker truth reports Track B open orders tied to portfolio positions or symbols.",
            details={"open_order_count": len(broker_open_orders), "open_orders": broker_open_orders},
        )

    market_prices = _load_market_prices(config=config, roots={str(item["track_b_root"]) for item in broker_positions}, now=actual_now, alerts=alerts)
    rows = _open_position_rows(
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
        match_report=match_report,
        market_prices=market_prices,
        config=config,
        now=actual_now,
        positions_path=positions_path,
        open_orders_path=open_orders_path,
        lifecycle_status=lifecycle_status,
        reconciled_lifecycle_status=reconciled_lifecycle_status,
        alerts=alerts,
    )

    if any(row.get("review_required") is True for row in rows) or _review_required_count(trade_summary, pnl_summary, lifecycle_status):
        _add_alert(
            alerts,
            severity="RED",
            code="REVIEW_REQUIRED",
            message="One or more lifecycle or portfolio rows require operator review before the state can be treated as clean.",
            details={"review_required_count": _review_required_count(trade_summary, pnl_summary, lifecycle_status)},
        )
    _runtime_stopped_alert_if_needed(freshness, open_position_count=len(rows), alerts=alerts)

    closed_trades = _closed_trades_from_ledger(config)
    system_events = _system_events_from_ledger(config)
    for event in system_events:
        if event.get("classification") == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT:
            _add_alert(
                alerts,
                severity="YELLOW",
                code="MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT",
                message="A malformed broker-backed manually reconciled lifecycle artifact is preserved as a contaminated system event.",
                details={"lifecycle_id": event.get("lifecycle_id"), "created_at": event.get("created_at")},
            )
    realized_by_trade_date = _realized_pnl_by_trade_date(closed_trades)
    realized = _decimal(pnl_summary.get("total_realized_pnl_today") or pnl_summary.get("realized_pnl_today"))
    if realized is None:
        today_key = actual_now.date().isoformat()
        realized = _decimal(realized_by_trade_date.get(today_key))
    unrealized = _sum_known(row.get("unrealized_pnl_dollars") for row in rows)
    estimated_total = realized + unrealized if realized is not None and unrealized is not None else None
    cost_basis_adjustments = [row["broker_cost_basis_adjustment"] for row in rows if row.get("broker_cost_basis_adjustment")]
    weekly_realized = _weekly_realized_pnl(closed_trades, actual_now.date())
    review_required_count = max(
        sum(1 for row in rows if row.get("review_required") is True),
        _review_required_count(trade_summary, pnl_summary, lifecycle_status),
    )
    portfolio_summary = {
        "open_position_count": len(rows),
        "long_contract_count": _decimal_text(sum((abs(_decimal(row.get("qty")) or Decimal("0")) for row in rows if row.get("side") == "LONG"), Decimal("0"))),
        "short_contract_count": _decimal_text(sum((abs(_decimal(row.get("qty")) or Decimal("0")) for row in rows if row.get("side") == "SHORT"), Decimal("0"))),
        "realized_pnl": _decimal_text(realized),
        "realized_pnl_today": _decimal_text(realized),
        "realized_pnl_by_trade_date": realized_by_trade_date,
        "weekly_realized_pnl": _decimal_text(weekly_realized),
        "unrealized_pnl": _decimal_text(unrealized),
        "estimated_total_pnl": _decimal_text(estimated_total),
        "total_estimated_pnl": _decimal_text(estimated_total),
        "fees": _decimal_text(_sum_known(trade.get("commissions") for trade in closed_trades)),
        "cost_basis_adjustments": cost_basis_adjustments,
        "open_order_count": len(broker_open_orders),
        "review_required_count": review_required_count,
    }
    status = _overall_status(freshness, match_report, alerts)
    portfolio_summary["status"] = _portfolio_status(status)
    broker_reconciliation = {
        "status": match_report["status"],
        "classification": _broker_reconciliation_classification(match_report, freshness, broker_status, trade_summary),
        "broker_reconciled": match_report["status"] == "MATCHED",
        "blockers": _reconciliation_blockers(match_report, freshness, alerts),
        "review_required_count": review_required_count,
        "open_order_count": len(broker_open_orders),
        "broker_position_count": len(broker_positions),
        "lifecycle_position_count": len(lifecycle_positions),
        "freshness": freshness.get("broker_truth"),
        "generated_at": broker_status.get("generated_at") or broker_status.get("latest_refresh_time"),
        "submit_authority": _submit_authority_from_payloads(broker_status, trade_summary, pnl_summary),
        "live_money_eligible": False,
        "broker_open_order_count": len(broker_open_orders),
        "matched_position_count": len(match_report["matches"]),
        "unmatched_broker_positions": match_report["unmatched_broker_positions"],
        "unmatched_lifecycle_positions": match_report["unmatched_lifecycle_positions"],
        "source_artifact_paths": {
            "broker_status": str(config.broker_status_path),
            "positions_snapshot": str(positions_path),
            "open_orders_snapshot": str(open_orders_path),
            "lifecycle_position_status": str(config.lifecycle_position_status_path),
            "reconciled_lifecycle_position_status": str(config.reconciled_lifecycle_position_status_path),
        },
    }
    strategy_pnl = _strategy_pnl_summary(pnl_summary, rows, closed_trades)
    pnl_timeseries_point = {
        "timestamp": actual_now.isoformat(),
        "realized_pnl": _decimal_text(realized),
        "realized_pnl_today": _decimal_text(realized),
        "unrealized_pnl": _decimal_text(unrealized),
        "estimated_total_pnl": _decimal_text(estimated_total),
        "total_estimated_pnl": _decimal_text(estimated_total),
        "open_position_count": len(rows),
        "open_order_count": len(broker_open_orders),
        "review_required_count": portfolio_summary["review_required_count"],
        "status": status,
    }
    state = {
        "schema_version": "track_b_portfolio_state_v1",
        "artifact_version": "track_b_portfolio_state_phase_a_v1",
        "metadata": {
            "generated_at": actual_now.isoformat(),
            "mode": config.mode,
            "account_id": config.account,
            "artifact_version": "track_b_portfolio_state_phase_a_v1",
            "source_root": str(config.repo_root),
            "git_branch": git_metadata.get("branch"),
            "git_head": git_metadata.get("head"),
            "live_money_eligible": False,
        },
        "account": config.account,
        "mode": config.mode,
        "generated_at": actual_now.isoformat(),
        "live_money_eligible": False,
        "status": status,
        "source_policy": {
            "broker_truth_authoritative_for_position_existence": True,
            "lifecycle_authoritative_for_strategy_ownership_only_when_reconciled": True,
            "research_artifacts_used_as_runtime_truth": False,
            "paper_proof_invoked": False,
            "broker_mutation_attempted": False,
        },
        "broker_reconciliation": broker_reconciliation,
        "runtime_services": _runtime_services(config=config, freshness=freshness, broker_status=broker_status),
        "runtime_service_freshness": freshness,
        "portfolio_summary": portfolio_summary,
        "open_positions": rows,
        "closed_trades": closed_trades,
        "system_events": system_events,
        "contaminated_events": system_events,
        "strategy_pnl": strategy_pnl,
        "strategy_lane_pnl_summary": strategy_pnl,
        "pnl_timeseries_point": pnl_timeseries_point,
        "alerts": _dedupe_alerts(alerts),
        "action_queue": _action_queue(_dedupe_alerts(alerts)),
    }
    calendar = build_track_b_pnl_calendar(state=state, config=config, now=actual_now)
    state["calendar_artifact_path"] = str(config.calendar_path)
    if write:
        _write_json_atomic(config.output_path, state)
        _append_portfolio_timeseries(config.portfolio_timeseries_path, pnl_timeseries_point)
        _write_json_atomic(config.calendar_path, calendar)
        _append_daily_history(config.daily_history_path, calendar)
    return state


def build_track_b_pnl_calendar(
    *,
    state: Mapping[str, Any],
    config: TrackBPortfolioConfig,
    now: datetime,
) -> dict[str, Any]:
    month = now.date().replace(day=1)
    next_month = (month.replace(day=28) + timedelta(days=4)).replace(day=1)
    days = (next_month - month).days
    ledger_rows = _load_jsonl(config.ledger_jsonl_path)
    daily = {f"{month.isoformat()[:8]}{day:02d}": _empty_day(f"{month.isoformat()[:8]}{day:02d}") for day in range(1, days + 1)}
    for row in ledger_rows:
        if _row_uses_research_truth(row):
            continue
        if _is_calendar_system_event(row):
            day_key = _day_key(row.get("original_entry_timestamp") or row.get("entry_timestamp") or row.get("created_at"))
            if day_key in daily:
                _accumulate_calendar_system_event(daily[day_key], row)
            continue
        if not _is_closed_trade_row(row):
            continue
        day_key = _day_key(row.get("exit_timestamp") or row.get("entry_timestamp") or row.get("created_at"))
        if day_key not in daily:
            continue
        _accumulate_calendar_trade(daily[day_key], row)
    today_key = now.date().isoformat()
    if today_key in daily:
        today = daily[today_key]
        unrealized = _decimal((state.get("portfolio_summary") or {}).get("unrealized_pnl"))
        today["unrealized_eod_pnl"] = _decimal_text(unrealized)
        today["open_positions_carried_overnight"] = (state.get("portfolio_summary") or {}).get("open_position_count")
        today["system_quality_markers"] = _merge_calendar_markers(
            today["system_quality_markers"],
            _calendar_quality_markers(state),
        )
        today["source_artifacts"].append(str(config.output_path))
    for item in daily.values():
        _finish_calendar_day(item)
    return {
        "schema_version": "track_b_pnl_calendar_v1",
        "generated_at": now.isoformat(),
        "account": config.account,
        "mode": config.mode,
        "live_money_eligible": False,
        "month": month.isoformat()[:7],
        "source_policy": {
            "runtime_truth_only": True,
            "research_artifacts_used_as_runtime_truth": False,
            "accepted_sources": ["broker_truth", "lifecycle", "ledger", "runtime_market_data", "runtime_service_status"],
        },
        "design": {
            "terminal_surface": "Track B Portfolio / Positions & P&L",
            "view": "monthly_heatmap_trading_calendar",
            "tile_fill_represents": "selected_pnl_mode",
            "tile_border_and_markers_represent": "system_quality_review_required_and_data_contamination",
            "tile_visual_contract": {
                "fill_color_bucket": "derived from the selected P&L mode only",
                "border_quality": "derived from worst system_quality_markers severity",
                "profitable_with_quality_markers": "green fill may still carry yellow/red border and markers",
            },
            "pnl_mode_toggles": ["realized_only", "realized_plus_unrealized", "closed_trade_only"],
            "quality_rule": "A profitable day with lifecycle, broker, runtime, or data-quality problems carries warning/error markers and never renders as clean.",
            "sample_classification": {
                "clean_strategy_sample_day": "runtime broker/lifecycle inputs reconciled, no review_required, no plumbing/debug contamination markers",
                "contaminated_plumbing_debug_day": "debug/proof/adoption/review/runtime incident markers present; keep visually distinct from clean strategy samples",
            },
            "tile_fields": [
                "date",
                "daily_estimated_total_pnl",
                "realized_pnl",
                "unrealized_eod_pnl",
                "trade_count",
                "win_count",
                "loss_count",
                "largest_winner",
                "largest_loser",
                "max_intraday_drawdown",
                "open_positions_carried_overnight",
                "system_quality_markers",
            ],
        },
        "daily_tiles": [daily[key] for key in sorted(daily)],
        "drilldowns": {key: _calendar_drilldown(value) for key, value in sorted(daily.items())},
    }


def _open_position_rows(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    match_report: Mapping[str, Any],
    market_prices: Mapping[str, Mapping[str, Any]],
    config: TrackBPortfolioConfig,
    now: datetime,
    positions_path: Path,
    open_orders_path: Path,
    lifecycle_status: Mapping[str, Any],
    reconciled_lifecycle_status: Mapping[str, Any],
    alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    lifecycle_by_broker_key = {
        match["broker_key"]: match["lifecycle_position"]
        for match in match_report.get("matches", [])
        if isinstance(match, Mapping)
    }
    for broker in broker_positions:
        root = str(broker.get("track_b_root") or "")
        qty = _decimal(broker.get("quantity")) or Decimal("0")
        side = "LONG" if qty > 0 else "SHORT" if qty < 0 else "FLAT"
        broker_key = _position_key(broker)
        lifecycle = lifecycle_by_broker_key.get(broker_key)
        lifecycle_reconciled = lifecycle is not None and match_report.get("status") == "MATCHED"
        adoption = _adoption_evidence(
            config=config,
            lifecycle_status=lifecycle_status,
            lifecycle=lifecycle,
            root=root,
            local_symbol=str(broker.get("local_symbol") or broker.get("localSymbol") or ""),
        )
        entry_source = "SUPERVISED_ADOPTION" if adoption else _entry_source(lifecycle)
        entry_time, entry_time_source = _entry_time(lifecycle, entry_source)
        if entry_time is None and adoption:
            entry_time = str(adoption.get("fill_timestamp") or adoption.get("executed_at") or "") or None
            entry_time_source = "FILL_TIME_ANCHOR" if entry_time else entry_time_source
        lifecycle_entry_price = _lifecycle_entry_price(lifecycle)
        broker_average_price = _broker_average_price(broker)
        entry_for_pnl = lifecycle_entry_price if lifecycle_entry_price is not None else broker_average_price
        price = market_prices.get(root, {})
        last_price = _decimal(price.get("last_price"))
        multiplier = _multiplier(root, broker)
        unrealized_points = _unrealized_points(side=side, entry=entry_for_pnl, last_price=last_price)
        unrealized_dollars = (
            unrealized_points * abs(qty) * multiplier
            if unrealized_points is not None and multiplier is not None
            else None
        )
        cost_basis_adjustment = _cost_basis_adjustment(
            broker=broker,
            lifecycle=lifecycle,
            broker_average=broker_average_price,
            lifecycle_entry=lifecycle_entry_price,
            qty=qty,
            root=root,
            multiplier=multiplier,
        )
        if cost_basis_adjustment and _decimal(cost_basis_adjustment.get("absolute_points_per_contract")) >= config.cost_basis_alert_points:
            _add_alert(
                alerts,
                severity="YELLOW",
                code="COST_BASIS_ADJUSTMENT_ABOVE_THRESHOLD",
                message="Broker average price differs from lifecycle entry price above the configured point threshold.",
                position_key=broker_key,
                details=cost_basis_adjustment,
            )
        if entry_source == "SUPERVISED_ADOPTION":
            _add_alert(
                alerts,
                severity="YELLOW",
                code="ADOPTED_POSITION",
                message="Position ownership comes from supervised lifecycle adoption evidence.",
                position_key=broker_key,
                details={"entry_source": entry_source, "strategy_id": _value(lifecycle, "strategy_id")},
            )
        bars_held, bars_warning = _bars_held(root=root, entry_time=entry_time, entry_time_source=entry_time_source, config=config)
        if bars_warning:
            _add_alert(
                alerts,
                severity="YELLOW",
                code="MISSING_FILL_TIME_ANCHOR",
                message=bars_warning,
                position_key=broker_key,
            )
        tied_orders = _orders_tied_to_position(broker, broker_open_orders)
        review_required = bool(_value(lifecycle, "review_required")) or not lifecycle_reconciled
        position_status = _position_status(
            lifecycle_reconciled=lifecycle_reconciled,
            entry_source=entry_source if lifecycle_reconciled else "UNKNOWN",
            review_required=review_required,
        )
        data_quality_flags = _position_data_quality_flags(
            lifecycle_reconciled=lifecycle_reconciled,
            entry_source=entry_source if lifecycle_reconciled else "UNKNOWN",
            bars_warning=bars_warning,
            last_price=last_price,
            review_required=review_required,
        )
        rows.append(
            {
                "symbol": broker.get("symbol") or root,
                "localSymbol": broker.get("local_symbol") or broker.get("localSymbol"),
                "expiry": broker.get("expiry"),
                "side": side,
                "qty": _decimal_text(abs(qty)),
                "strategy_id": _value(lifecycle, "strategy_id") if lifecycle_reconciled else None,
                "lane_id": (_lane_id(lifecycle) or adoption.get("lane_id")) if lifecycle_reconciled else None,
                "status": position_status,
                "entry_source": entry_source if lifecycle_reconciled else "UNKNOWN",
                "entry_time": entry_time,
                "entry_time_source": entry_time_source,
                "lifecycle_entry_price": _decimal_text(lifecycle_entry_price),
                "broker_average_price": _decimal_text(broker_average_price),
                "broker_cost_basis_adjustment": cost_basis_adjustment,
                "last_price": _decimal_text(last_price),
                "last_price_source": price.get("source_artifact_path"),
                "unrealized_pnl_dollars": _decimal_text(unrealized_dollars),
                "unrealized_pnl_points": _decimal_text(unrealized_points),
                "mfe_points": _decimal_text(_first_decimal(lifecycle, "mfe_points", "max_favorable_excursion_points")),
                "mae_points": _decimal_text(_first_decimal(lifecycle, "mae_points", "max_adverse_excursion_points")),
                "bars_held": bars_held,
                "bars_held_warning": bars_warning,
                "reconciliation_status": "MATCHED" if lifecycle_reconciled else "BROKER_ONLY_OR_UNRECONCILED",
                "review_required": review_required,
                "open_orders_tied_to_position": tied_orders,
                "broker_identity": {
                    "account": broker.get("account_id") or broker.get("account"),
                    "conId": broker.get("con_id") or broker.get("conId") or _value(lifecycle, "con_id"),
                    "permId": broker.get("perm_id") or broker.get("permId") or _value(lifecycle, "entry_perm_id"),
                    "clientId": broker.get("client_id") or broker.get("clientId") or _value(lifecycle, "entry_client_id"),
                    "orderId": broker.get("order_id") or broker.get("orderId") or _value(lifecycle, "entry_order_id"),
                },
                "source_artifact_paths": _row_source_paths(
                    lifecycle=lifecycle,
                    lifecycle_status=lifecycle_status,
                    reconciled_lifecycle_status=reconciled_lifecycle_status,
                    positions_path=positions_path,
                    open_orders_path=open_orders_path,
                ),
                "data_quality": data_quality_flags,
            }
        )
    rows.sort(key=lambda row: (str(row.get("symbol") or ""), str(row.get("localSymbol") or "")))
    return rows


def _freshness_summary(
    *,
    config: TrackBPortfolioConfig,
    now: datetime,
    broker_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
    open_position_count: int,
    alerts: list[dict[str, Any]],
) -> dict[str, Any]:
    broker_items = [
        _artifact_freshness("broker_truth_status", config.broker_status_path, broker_status, now, config.broker_max_age_seconds),
        _artifact_freshness("broker_positions_snapshot", positions_path, positions_snapshot, now, config.broker_max_age_seconds),
        _artifact_freshness("broker_open_orders_snapshot", open_orders_path, open_orders_snapshot, now, config.broker_max_age_seconds),
    ]
    broker_stale = any(item["status"] != "FRESH" for item in broker_items)
    if broker_stale:
        _add_alert(
            alerts,
            severity="RED" if open_position_count else "YELLOW",
            code="STALE_BROKER_TRUTH",
            message="Broker truth is stale, missing, or incomplete; portfolio state is degraded and broker existence remains non-inferred.",
            details={"artifacts": broker_items},
        )
    service_items = [
        _artifact_freshness(path.stem, path, _load_json(path), now, config.runtime_max_age_seconds)
        for path in config.service_status_paths
    ]
    return {
        "status": "DEGRADED" if broker_stale or any(item["status"] not in ("FRESH", "MISSING") for item in service_items) else "OK",
        "broker_truth": broker_items,
        "services": service_items,
    }


def _runtime_services(
    *,
    config: TrackBPortfolioConfig,
    freshness: Mapping[str, Any],
    broker_status: Mapping[str, Any],
) -> dict[str, Any]:
    service_payloads = [_load_json(path) for path in config.service_status_paths]
    runtime_payload = next(
        (
            payload
            for payload in service_payloads
            if payload
            and any(
                token in str(payload.get(key) or "").lower()
                for key in ("service", "classification", "status", "process_status")
                for token in ("runtime", "paper", "databento", "operator_readiness")
            )
        ),
        {},
    )
    active_lane_count = _first_present(
        runtime_payload,
        broker_status,
        *service_payloads,
        keys=("active_lane_count", "active_lanes", "restored_active_lanes", "eligible_lane_count"),
    )
    if isinstance(active_lane_count, list):
        active_lane_count = len(active_lane_count)
    return {
        "runtime_running": _runtime_running(service_payloads),
        "runtime_pid": _first_present(*service_payloads, keys=("pid", "process_id")),
        "runtime_cwd": _first_present(*service_payloads, keys=("cwd", "runtime_cwd", "working_directory")),
        "active_lane_count": active_lane_count,
        "latest_evaluated_bar": _first_present(*service_payloads, keys=("latest_evaluated_bar", "latest_bar", "last_completed_bar_ts")),
        "broker_truth_freshness": freshness.get("broker_truth"),
        "databento_freshness": [
            item
            for item in freshness.get("services", [])
            if isinstance(item, Mapping) and "databento" in str(item.get("name") or item.get("path") or "").lower()
        ],
        "operator_readiness_freshness": [
            item
            for item in freshness.get("services", [])
            if isinstance(item, Mapping) and "operator_readiness" in str(item.get("name") or item.get("path") or "").lower()
        ],
        "dashboard_status": _first_present(*service_payloads, keys=("dashboard_status", "api_status", "health", "classification")),
        "legacy_monitor_diagnostic_only": True,
        "service_artifacts": freshness.get("services"),
    }


def _runtime_running(payloads: Sequence[Mapping[str, Any]]) -> bool | None:
    found = False
    for payload in payloads:
        if not payload:
            continue
        text = " ".join(
            str(payload.get(key) or "")
            for key in ("status", "state", "classification", "service_status", "process_status", "runtime_phase")
        ).upper()
        if any(token in text for token in ("STOPPED", "NOT_RUNNING", "DEAD", "EXITED")):
            return False
        if any(token in text for token in ("RUNNING", "READY", "RESPONDING", "HEALTHY", "WARMING")):
            found = True
    return True if found else None


def _first_present(*payloads: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
    return None


def _git_metadata(repo_root: Path) -> dict[str, str | None]:
    git_dir = repo_root / ".git"
    head_path = git_dir / "HEAD"
    try:
        head_text = head_path.read_text(encoding="utf-8").strip()
    except OSError:
        return {"branch": None, "head": None}
    if head_text.startswith("ref: "):
        ref = head_text[5:]
        branch = ref.rsplit("/", 1)[-1]
        try:
            head = (git_dir / ref).read_text(encoding="utf-8").strip()
        except OSError:
            head = None
        return {"branch": branch, "head": head}
    return {"branch": None, "head": head_text or None}


def _live_money_input_flags(*payloads: Mapping[str, Any]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []
    for index, payload in enumerate(payloads):
        if not isinstance(payload, Mapping):
            continue
        if payload.get("live_money_eligible") is True:
            flags.append({"payload_index": index, "field": "live_money_eligible", "value": True})
        if payload.get("mode") == "LIVE" or payload.get("account_mode") == "LIVE":
            flags.append({"payload_index": index, "field": "mode", "value": payload.get("mode") or payload.get("account_mode")})
    return flags


def _broker_reconciliation_classification(
    match_report: Mapping[str, Any],
    freshness: Mapping[str, Any],
    broker_status: Mapping[str, Any],
    trade_summary: Mapping[str, Any],
) -> str:
    if _broker_truth_stale(freshness):
        return "STALE_BROKER_TRUTH"
    if match_report.get("status") != "MATCHED":
        return "BROKER_LIFECYCLE_MISMATCH"
    return str(
        trade_summary.get("broker_reconciliation_classification")
        or broker_status.get("verifier_classification")
        or broker_status.get("classification")
        or "TRACK_B_PAPER_BROKER_RECONCILED"
    )


def _reconciliation_blockers(
    match_report: Mapping[str, Any],
    freshness: Mapping[str, Any],
    alerts: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if match_report.get("status") != "MATCHED":
        blockers.append({"code": "BROKER_LIFECYCLE_MISMATCH", "severity": "RED"})
    if _broker_truth_stale(freshness):
        blockers.append({"code": "STALE_BROKER_TRUTH", "severity": "YELLOW"})
    for alert in alerts:
        if alert.get("severity") == "RED":
            blockers.append({"code": alert.get("code"), "severity": "RED"})
    return _dedupe_alerts(blockers)


def _broker_truth_stale(freshness: Mapping[str, Any]) -> bool:
    return any(
        item.get("status") != "FRESH"
        for item in freshness.get("broker_truth", [])
        if isinstance(item, Mapping)
    )


def _submit_authority_from_payloads(*payloads: Mapping[str, Any]) -> bool | None:
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in ("submit_authority", "can_submit", "paper_trade_allowed"):
            if key in payload:
                return bool(payload.get(key))
    return None


def _portfolio_status(status: str) -> str:
    if status == "RED":
        return "BLOCKED"
    if status == "OK":
        return "CLEAN"
    return "DEGRADED"


def _position_status(
    *,
    lifecycle_reconciled: bool,
    entry_source: str,
    review_required: bool,
) -> str:
    if review_required:
        return "REVIEW_REQUIRED"
    if not lifecycle_reconciled:
        return "BROKER_ONLY"
    if entry_source == "SUPERVISED_ADOPTION":
        return "ADOPTED"
    return "MANAGED"


def _position_data_quality_flags(
    *,
    lifecycle_reconciled: bool,
    entry_source: str,
    bars_warning: str | None,
    last_price: Decimal | None,
    review_required: bool,
) -> list[str]:
    flags: list[str] = []
    if not lifecycle_reconciled:
        flags.append("BROKER_ONLY_OR_UNRECONCILED")
    if entry_source == "SUPERVISED_ADOPTION":
        flags.append("SUPERVISED_ADOPTION")
    if bars_warning:
        flags.append("MISSING_FILL_TIME_ANCHOR")
    if last_price is None:
        flags.append("MISSING_RUNTIME_MARK")
    if review_required:
        flags.append("REVIEW_REQUIRED")
    return flags


def _load_market_prices(
    *,
    config: TrackBPortfolioConfig,
    roots: Iterable[str],
    now: datetime,
    alerts: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    prices: dict[str, dict[str, Any]] = {}
    for root in sorted(set(roots)):
        path = config.market_data_root / root / "1m" / "latest_runtime_candles.json"
        payload = _load_json(path)
        freshness = _artifact_freshness(f"{root}_runtime_candles", path, payload, now, config.market_data_max_age_seconds)
        if _payload_uses_research_truth(payload) or _path_is_research(path):
            freshness["status"] = "RESEARCH_TRUTH_REJECTED"
            freshness["research_artifact_used"] = True
        if freshness["status"] != "FRESH":
            _add_alert(
                alerts,
                severity="YELLOW",
                code="STALE_MARKET_DATA",
                message=f"Runtime market data for {root} is stale, missing, or rejected; P&L for that root may be null.",
                details=freshness,
            )
            prices[root] = {"freshness": freshness, "last_price": None, "source_artifact_path": str(path)}
            continue
        bar = _latest_bar(payload)
        prices[root] = {
            "freshness": freshness,
            "last_price": bar.get("close") if isinstance(bar, Mapping) else None,
            "last_price_time": (bar.get("bar_end") or bar.get("timestamp")) if isinstance(bar, Mapping) else None,
            "source_artifact_path": str(path),
        }
    return prices


def _artifact_freshness(name: str, path: Path, payload: Mapping[str, Any], now: datetime, max_age_seconds: float) -> dict[str, Any]:
    generated_at = _parse_time(
        payload.get("generated_at")
        or payload.get("as_of")
        or payload.get("updated_at")
        or payload.get("last_completed_bar_ts")
        or payload.get("latest_refresh_time")
        or payload.get("completed_at")
    )
    status = "MISSING" if not payload else "FRESH"
    age = None
    if payload and generated_at is None:
        status = "MISSING_TIMESTAMP"
    elif generated_at is not None:
        age = max((now - generated_at).total_seconds(), 0.0)
        if age > max_age_seconds:
            status = "STALE"
    stopped = _payload_indicates_stopped(payload)
    if stopped:
        status = "STOPPED"
    return {
        "name": name,
        "path": str(path),
        "status": status,
        "generated_at": generated_at.isoformat() if generated_at else None,
        "age_seconds": age,
        "max_age_seconds": float(max_age_seconds),
        "classification": payload.get("classification") if isinstance(payload, Mapping) else None,
        "stopped": stopped,
    }


def _track_b_broker_positions(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("positions") if isinstance(snapshot.get("positions"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        qty = _decimal(row.get("quantity"))
        if root is None or qty is None or qty == 0:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_broker_open_orders(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("open_orders") if isinstance(snapshot.get("open_orders"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        contract = row.get("contract")
        if root is None and isinstance(contract, Mapping):
            root = _track_b_root(contract, symbols)
        if root is None:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_lifecycle_positions(lifecycle_status: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = lifecycle_status.get("positions_by_instrument")
    if not isinstance(rows, Mapping):
        return []
    positions: list[dict[str, Any]] = []
    for key, value in rows.items():
        if not isinstance(value, Mapping):
            continue
        root = _track_b_root(value, symbols) or _track_b_root({"contract_key": key}, symbols)
        qty = _decimal(value.get("quantity"))
        if root is None or qty is None or qty == 0:
            continue
        item = dict(value)
        item.setdefault("position_key", str(key))
        item["track_b_root"] = root
        positions.append(item)
    return positions


def _match_broker_to_lifecycle(
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
) -> dict[str, Any]:
    unmatched_lifecycle = [dict(item) for item in lifecycle_positions]
    matches: list[dict[str, Any]] = []
    unmatched_broker: list[dict[str, Any]] = []
    for broker in broker_positions:
        broker_root = _track_b_root(broker, symbols)
        broker_qty = _decimal(broker.get("quantity"))
        broker_key = _position_key(broker)
        found_index = None
        for index, lifecycle in enumerate(unmatched_lifecycle):
            lifecycle_root = _track_b_root(lifecycle, symbols)
            lifecycle_qty = _signed_lifecycle_quantity(lifecycle, _decimal(lifecycle.get("quantity")))
            if broker_root == lifecycle_root and broker_qty == lifecycle_qty and _local_symbols_compatible(broker, lifecycle):
                found_index = index
                break
        if found_index is None:
            unmatched_broker.append(dict(broker))
            continue
        lifecycle = unmatched_lifecycle.pop(found_index)
        matches.append({"broker_key": broker_key, "broker_position": dict(broker), "lifecycle_position": lifecycle})
    status = "MATCHED" if not unmatched_broker and not unmatched_lifecycle else "MISMATCH"
    return {
        "status": status,
        "matches": matches,
        "unmatched_broker_positions": unmatched_broker,
        "unmatched_lifecycle_positions": unmatched_lifecycle,
    }


def _track_b_root(row: Mapping[str, Any], symbols: Sequence[str]) -> str | None:
    ordered = sorted((symbol.upper() for symbol in symbols), key=len, reverse=True)
    fields = (row.get("symbol"), row.get("root"), row.get("instrument"), row.get("contract_key"), row.get("local_symbol"), row.get("localSymbol"))
    values = [str(value).upper().replace(" ", "") for value in fields if value not in (None, "")]
    for symbol in ordered:
        for value in values:
            if value == symbol or value.startswith(symbol):
                return symbol
    return None


def _select_pnl_summary(config: TrackBPortfolioConfig) -> dict[str, Any]:
    reconciled = _load_json(config.reconciled_pnl_summary_path)
    if reconciled.get("source") == "BROKER_RECONCILED":
        return reconciled
    return _load_json(config.pnl_summary_path)


def _strategy_lane_pnl_summary(pnl_summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_strategy = pnl_summary.get("by_strategy") if isinstance(pnl_summary.get("by_strategy"), Mapping) else {}
    open_by_strategy: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        open_by_strategy.setdefault(strategy_id, []).append(row)
    keys = sorted(set(by_strategy) | set(open_by_strategy))
    summary: list[dict[str, Any]] = []
    for key in keys:
        item = by_strategy.get(key) if isinstance(by_strategy, Mapping) and isinstance(by_strategy.get(key), Mapping) else {}
        open_rows = open_by_strategy.get(key, [])
        unrealized = _sum_known(row.get("unrealized_pnl_dollars") for row in open_rows)
        summary.append(
            {
                "strategy_id": key,
                "lane_id": next((row.get("lane_id") for row in open_rows if row.get("lane_id")), item.get("lane_id") if isinstance(item, Mapping) else None),
                "realized_pnl": item.get("realized_pnl") if isinstance(item, Mapping) else None,
                "unrealized_pnl": _decimal_text(unrealized),
                "estimated_total_pnl": _decimal_text((_decimal(item.get("realized_pnl")) if isinstance(item, Mapping) else None) + unrealized if unrealized is not None and isinstance(item, Mapping) and _decimal(item.get("realized_pnl")) is not None else None),
                "open_position_count": len(open_rows),
                "trade_count": item.get("trade_count") or item.get("trades") if isinstance(item, Mapping) else None,
                "review_required_count": item.get("review_required_count") if isinstance(item, Mapping) else sum(1 for row in open_rows if row.get("review_required") is True),
            }
        )
    return summary


def _strategy_pnl_summary(
    pnl_summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    closed_trades: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    by_strategy = pnl_summary.get("by_strategy") if isinstance(pnl_summary.get("by_strategy"), Mapping) else {}
    for strategy_id, payload in by_strategy.items():
        if not isinstance(payload, Mapping):
            continue
        bucket = buckets.setdefault(
            str(strategy_id),
            {
                "strategy_id": str(strategy_id),
                "lane_id": payload.get("lane_id"),
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "total_estimated_pnl": None,
                "clean_trade_count": 0,
                "open_position_count": 0,
                "review_required_count": payload.get("review_required_count") or 0,
                "excluded_realized_pnl": "0",
                "supervised_flatten_pnl": "0",
                "contaminated_event_count": 0,
            },
        )
        bucket["realized_pnl"] = _decimal_text(_decimal(payload.get("realized_pnl_today")) or _decimal(payload.get("realized_pnl")) or Decimal("0"))
    for row in rows:
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        bucket = buckets.setdefault(
            strategy_id,
            {
                "strategy_id": strategy_id,
                "lane_id": row.get("lane_id"),
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "total_estimated_pnl": None,
                "clean_trade_count": 0,
                "open_position_count": 0,
                "review_required_count": 0,
                "excluded_realized_pnl": "0",
                "supervised_flatten_pnl": "0",
                "contaminated_event_count": 0,
            },
        )
        bucket["lane_id"] = bucket.get("lane_id") or row.get("lane_id")
        bucket["open_position_count"] += 1
        bucket["review_required_count"] += 1 if row.get("review_required") is True else 0
        unrealized = (_decimal(bucket.get("unrealized_pnl")) or Decimal("0")) + (_decimal(row.get("unrealized_pnl_dollars")) or Decimal("0"))
        bucket["unrealized_pnl"] = _decimal_text(unrealized)
    clean_realized_by_strategy: dict[str, Decimal] = {}
    strategies_with_closed_trades: set[str] = set()
    for trade in closed_trades:
        strategy_id = str(trade.get("strategy_id") or "UNKNOWN")
        strategies_with_closed_trades.add(strategy_id)
        pnl = _decimal(trade.get("realized_pnl")) or Decimal("0")
        bucket = buckets.setdefault(
            strategy_id,
            {
                "strategy_id": strategy_id,
                "lane_id": trade.get("lane_id"),
                "realized_pnl": "0",
                "unrealized_pnl": "0",
                "total_estimated_pnl": None,
                "clean_trade_count": 0,
                "open_position_count": 0,
                "review_required_count": 0,
                "excluded_realized_pnl": "0",
                "supervised_flatten_pnl": "0",
                "contaminated_event_count": 0,
            },
        )
        bucket["lane_id"] = bucket.get("lane_id") or trade.get("lane_id")
        if trade.get("clean_strategy_pnl_eligible") is True:
            clean_realized_by_strategy[strategy_id] = clean_realized_by_strategy.get(strategy_id, Decimal("0")) + pnl
            bucket["clean_trade_count"] += 1
        else:
            excluded = (_decimal(bucket.get("excluded_realized_pnl")) or Decimal("0")) + pnl
            bucket["excluded_realized_pnl"] = _decimal_text(excluded)
            bucket["contaminated_event_count"] += 1
            if trade.get("category") == "SUPERVISED_FLATTEN":
                supervised = (_decimal(bucket.get("supervised_flatten_pnl")) or Decimal("0")) + pnl
                bucket["supervised_flatten_pnl"] = _decimal_text(supervised)
    for strategy_id in strategies_with_closed_trades:
        buckets[strategy_id]["realized_pnl"] = _decimal_text(clean_realized_by_strategy.get(strategy_id, Decimal("0")))
    for bucket in buckets.values():
        realized = _decimal(bucket.get("realized_pnl"))
        unrealized = _decimal(bucket.get("unrealized_pnl"))
        bucket["total_estimated_pnl"] = _decimal_text(realized + unrealized) if realized is not None and unrealized is not None else None
    return [buckets[key] for key in sorted(buckets)]


def _closed_trades_from_ledger(config: TrackBPortfolioConfig) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for row in _load_jsonl(config.ledger_jsonl_path):
        if _row_uses_research_truth(row) or not _is_closed_trade_row(row):
            continue
        realized = _realized_pnl_for_row(row)
        category = _closed_trade_category(row)
        clean_eligible = category == "CLEAN_STRATEGY_MANAGED"
        trades.append(
            {
                "trade_id": row.get("trade_id"),
                "lifecycle_id": row.get("lifecycle_id"),
                "strategy_id": row.get("strategy_id"),
                "lane_id": row.get("lane_id") or row.get("strategy_id"),
                "symbol": row.get("symbol") or row.get("instrument_family"),
                "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
                "contract_key": row.get("contract_key"),
                "conId": row.get("con_id") or row.get("conId"),
                "side": row.get("side"),
                "qty": row.get("quantity"),
                "entry_time": row.get("entry_timestamp"),
                "exit_time": row.get("exit_timestamp") or row.get("exit_fill_time"),
                "entry_price": row.get("entry_fill_price") or row.get("entry_price"),
                "exit_price": row.get("exit_fill_price") or row.get("exit_price"),
                "points_pnl": _decimal_text(_points_pnl_for_row(row)),
                "realized_pnl": _decimal_text(realized),
                "multiplier": _decimal_text(_multiplier(str(row.get("instrument_family") or row.get("symbol") or ""), row)),
                "category": category,
                "clean_strategy_pnl_eligible": clean_eligible,
                "excluded_from_strategy_managed_pnl": not clean_eligible,
                "review_required": row.get("review_required") is True,
                "broker_backed_position_confirmed": row.get("broker_backed_position_confirmed") is True,
                "source_artifact_paths": _extract_source_paths(row),
            }
        )
    trades.sort(key=lambda item: str(item.get("exit_time") or item.get("entry_time") or ""))
    return trades


def _system_events_from_ledger(config: TrackBPortfolioConfig) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    rows = _load_jsonl(config.ledger_jsonl_path)
    original_by_lifecycle = {
        str(row.get("lifecycle_id") or ""): row
        for row in rows
        if not _is_system_event_row(row) and str(row.get("lifecycle_id") or "")
    }
    for row in rows:
        if _row_uses_research_truth(row) or not _is_system_event_row(row):
            continue
        original = original_by_lifecycle.get(str(row.get("lifecycle_id") or ""), {})
        raw_classification = str(
            row.get("new_artifact_classification")
            or row.get("artifact_reconciliation_classification")
            or row.get("reconciliation_action")
            or row.get("final_position_status")
            or "SYSTEM_EVENT"
        )
        historical_broker_backed = (
            row.get("historical_broker_backed_exposure_confirmed") is True
            or row.get("broker_backed_position_confirmed") is True
            or original.get("broker_backed_position_confirmed") is True
        )
        classification = (
            MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT
            if raw_classification == VOID_MALFORMED_STALE_ARTIFACT and historical_broker_backed
            else raw_classification
        )
        events.append(
            {
                "event_type": row.get("record_type") or "SYSTEM_EVENT",
                "classification": classification,
                "original_artifact_classification": raw_classification if raw_classification != classification else None,
                "reconciliation_action": row.get("reconciliation_action"),
                "lifecycle_id": row.get("lifecycle_id"),
                "strategy_id": row.get("strategy_id"),
                "symbol": row.get("instrument_family") or row.get("symbol"),
                "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
                "con_id": row.get("con_id") or row.get("conId"),
                "created_at": row.get("created_at"),
                "entry_time": row.get("original_entry_timestamp") or row.get("entry_timestamp"),
                "entry_price": row.get("original_entry_fill_price") or row.get("entry_fill_price"),
                "historical_broker_backed_exposure_confirmed": historical_broker_backed,
                "excluded_from_strategy_managed_pnl": row.get("excluded_from_strategy_managed_pnl") is True
                or classification in {MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT, VOID_MALFORMED_STALE_ARTIFACT},
                "excluded_from_clean_trade_stats": row.get("excluded_from_clean_trade_stats") is True
                or classification in {MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT, VOID_MALFORMED_STALE_ARTIFACT},
                "reason_codes": row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else [],
                "source_artifact_paths": _extract_source_paths(row),
            }
        )
    events.sort(key=lambda item: str(item.get("created_at") or item.get("entry_time") or ""))
    return events


def _is_closed_trade_row(row: Mapping[str, Any]) -> bool:
    if _is_system_event_row(row):
        return False
    if row.get("review_required") is True:
        return False
    status = str(row.get("final_position_status") or row.get("paper_lifecycle_classification") or "").upper()
    return bool(row.get("exit_timestamp") or row.get("exit_fill_time") or row.get("exit_price") or row.get("exit_fill_price")) and (
        "CLOSED" in status or "FLAT" in status or row.get("realized_pnl") not in (None, "")
    )


def _is_system_event_row(row: Mapping[str, Any]) -> bool:
    classification = row.get("new_artifact_classification") or row.get("artifact_reconciliation_classification")
    return row.get("record_type") == "ARTIFACT_RECONCILIATION" or classification in {
        MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
        VOID_MALFORMED_STALE_ARTIFACT,
    }


def _closed_trade_category(row: Mapping[str, Any]) -> str:
    if row.get("excluded_from_strategy_managed_pnl") is True or row.get("excluded_from_clean_trade_stats") is True:
        return "CONTAMINATED_OR_MANUAL"
    lifecycle_id = str(row.get("lifecycle_id") or "")
    proof_path = str(row.get("paper_lifecycle_report_path") or "")
    if (
        lifecycle_id.startswith("paper_proof_")
        or "/paper_proof/" in proof_path
        or bool(row.get("paper_proof_classification"))
    ):
        return "PROOF_CANARY_EXCLUDED"
    text = json.dumps(row, sort_keys=True).lower()
    if "lifecycle_adoption" in text or "supervised_adoption" in text:
        return "SUPERVISED_FLATTEN"
    if row.get("close_reconciliation_source") and row.get("paper_proof_invoked") is False:
        return "CLEAN_STRATEGY_MANAGED"
    return "CLEAN_STRATEGY_MANAGED"


def _realized_pnl_for_row(row: Mapping[str, Any]) -> Decimal | None:
    explicit = _decimal(row.get("realized_pnl"))
    if explicit is not None:
        return explicit
    points = _points_pnl_for_row(row)
    qty = _decimal(row.get("quantity")) or Decimal("1")
    multiplier = _multiplier(str(row.get("instrument_family") or row.get("symbol") or ""), row)
    if points is None or multiplier is None:
        return None
    return points * abs(qty) * multiplier


def _points_pnl_for_row(row: Mapping[str, Any]) -> Decimal | None:
    explicit = _decimal(row.get("points_pnl"))
    if explicit is not None:
        return explicit
    entry = _decimal(row.get("entry_fill_price") or row.get("entry_price"))
    exit_price = _decimal(row.get("exit_fill_price") or row.get("exit_price"))
    if entry is None or exit_price is None:
        return None
    side = str(row.get("side") or "").upper()
    raw = exit_price - entry
    return -raw if side == "SHORT" else raw


def _realized_pnl_by_trade_date(closed_trades: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    buckets: dict[str, Decimal] = {}
    for trade in closed_trades:
        if trade.get("clean_strategy_pnl_eligible") is not True:
            continue
        day = _day_key(trade.get("exit_time") or trade.get("entry_time"))
        pnl = _decimal(trade.get("realized_pnl"))
        if day is None or pnl is None:
            continue
        buckets[day] = buckets.get(day, Decimal("0")) + pnl
    return {key: _decimal_text(value) or "0" for key, value in sorted(buckets.items())}


def _weekly_realized_pnl(closed_trades: Sequence[Mapping[str, Any]], today: date) -> Decimal | None:
    start = today - timedelta(days=today.weekday())
    total = Decimal("0")
    found = False
    for trade in closed_trades:
        if trade.get("clean_strategy_pnl_eligible") is not True:
            continue
        day = _day_key(trade.get("exit_time") or trade.get("entry_time"))
        pnl = _decimal(trade.get("realized_pnl"))
        if day is None or pnl is None:
            continue
        try:
            trade_date = date.fromisoformat(day)
        except ValueError:
            continue
        if start <= trade_date <= today:
            total += pnl
            found = True
    return total if found else None


def _entry_source(lifecycle: Mapping[str, Any] | None) -> str:
    if not lifecycle:
        return "UNKNOWN"
    explicit = str(lifecycle.get("entry_source") or lifecycle.get("fill_source") or "").strip().upper()
    if explicit in {"NORMAL_FILL", "WATCHDOG_OBSERVED_FILL", "SUPERVISED_ADOPTION", "UNKNOWN"}:
        return explicit
    text = json.dumps(lifecycle, sort_keys=True).lower()
    if "lifecycle_adoption" in text or "supervised_adoption" in text or "adoption" in text:
        return "SUPERVISED_ADOPTION"
    if "watchdog" in text or "observed_fill" in text:
        return "WATCHDOG_OBSERVED_FILL"
    if lifecycle.get("entry_broker_identity") or lifecycle.get("entry_exec_id") or lifecycle.get("entry_order_id"):
        return "NORMAL_FILL"
    return "UNKNOWN"


def _adoption_evidence(
    *,
    config: TrackBPortfolioConfig,
    lifecycle_status: Mapping[str, Any],
    lifecycle: Mapping[str, Any] | None,
    root: str,
    local_symbol: str,
) -> dict[str, Any]:
    strategy_id = str(_value(lifecycle, "strategy_id") or "")
    for raw_path in _extract_source_paths(lifecycle_status):
        if "lifecycle_adoption" not in raw_path:
            continue
        path = Path(raw_path)
        if not path.is_absolute():
            path = config.repo_root / path
        report = _load_json(path)
        if not report:
            continue
        payload = report.get("filled_bridge_result_payload")
        if not isinstance(payload, Mapping):
            payload = report.get("fill_payload") if isinstance(report.get("fill_payload"), Mapping) else {}
        report_symbol = str(report.get("symbol") or payload.get("symbol") or "").upper()
        report_local = str(report.get("local_symbol") or payload.get("local_symbol") or "").upper()
        report_strategy = str(payload.get("strategy_id") or report.get("strategy_id") or "")
        if report_symbol == root.upper() and (not local_symbol or report_local == local_symbol.upper()):
            if not strategy_id or not report_strategy or report_strategy == strategy_id:
                return {
                    "path": str(path),
                    "lane_id": report.get("lane_id") or payload.get("lane_id"),
                    "fill_timestamp": payload.get("fill_timestamp") or payload.get("executed_at"),
                    "classification": report.get("classification"),
                }
    return {}


def _entry_time(lifecycle: Mapping[str, Any] | None, entry_source: str) -> tuple[str | None, str | None]:
    if not lifecycle:
        return None, None
    for key in ("entry_timestamp", "entry_time", "fill_timestamp", "executed_at", "created_at"):
        value = lifecycle.get(key)
        if value:
            if key in {"entry_timestamp", "fill_timestamp", "executed_at"}:
                source = "FILL_TIME_ANCHOR" if entry_source in {"NORMAL_FILL", "WATCHDOG_OBSERVED_FILL", "SUPERVISED_ADOPTION"} else "LIFECYCLE_TIMESTAMP"
            else:
                source = "LIFECYCLE_CREATED_AT"
            return str(value), source
    return None, None


def _bars_held(*, root: str, entry_time: str | None, entry_time_source: str | None, config: TrackBPortfolioConfig) -> tuple[int | None, str | None]:
    if entry_time_source != "FILL_TIME_ANCHOR":
        return None, "Position is missing a fill-time anchor; bars_held is intentionally null."
    entry_dt = _parse_time(entry_time)
    if entry_dt is None:
        return None, "Position has a fill-time source but the entry time is unparseable; bars_held is null."
    payload = _load_json(config.market_data_root / root / "1m" / "latest_runtime_candles.json")
    bars = payload.get("bars") if isinstance(payload.get("bars"), list) else []
    count = 0
    for bar in bars:
        if not isinstance(bar, Mapping):
            continue
        bar_time = _parse_time(bar.get("bar_end") or bar.get("timestamp"))
        if bar_time and bar_time >= entry_dt:
            count += 1
    return count, None


def _cost_basis_adjustment(
    *,
    broker: Mapping[str, Any],
    lifecycle: Mapping[str, Any] | None,
    broker_average: Decimal | None,
    lifecycle_entry: Decimal | None,
    qty: Decimal,
    root: str,
    multiplier: Decimal | None,
) -> dict[str, Any] | None:
    if lifecycle is None or broker_average is None or lifecycle_entry is None:
        return None
    diff = broker_average - lifecycle_entry
    total_points = diff * abs(qty)
    dollars = total_points * multiplier if multiplier is not None else None
    return {
        "source": "IBKR_AVERAGE_PRICE_MINUS_TRACK_B_LIFECYCLE_ENTRY_PRICE",
        "root": root,
        "broker_local_symbol": broker.get("local_symbol") or broker.get("localSymbol"),
        "lifecycle_local_symbol": lifecycle.get("local_symbol") or lifecycle.get("localSymbol"),
        "quantity": _decimal_text(abs(qty)),
        "lifecycle_average_entry_price": _decimal_text(lifecycle_entry),
        "broker_average_price": _decimal_text(broker_average),
        "broker_minus_lifecycle_points_per_contract": _decimal_text(diff),
        "broker_minus_lifecycle_points_total": _decimal_text(total_points),
        "broker_minus_lifecycle_dollars_total": _decimal_text(dollars),
        "absolute_points_per_contract": _decimal_text(abs(diff)),
        "absolute_points_total": _decimal_text(abs(total_points)),
    }


def _calendar_quality_markers(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for alert in state.get("alerts") or []:
        if not isinstance(alert, Mapping):
            continue
        markers.append({"severity": alert.get("severity"), "code": alert.get("code"), "label": alert.get("message")})
    if not markers:
        markers.append({"severity": "GREEN", "code": "CLEAN_STRATEGY_SAMPLE_DAY", "label": "No broker, lifecycle, runtime, or review-required markers in this snapshot."})
    return markers


def _merge_calendar_markers(
    existing: Sequence[Mapping[str, Any]],
    incoming: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for marker in [*existing, *incoming]:
        if not isinstance(marker, Mapping):
            continue
        key = (str(marker.get("severity") or ""), str(marker.get("code") or ""))
        if key in seen:
            continue
        seen.add(key)
        merged.append(dict(marker))
    return merged


def _empty_day(day: str) -> dict[str, Any]:
    return {
        "date": day,
        "daily_estimated_total_pnl": None,
        "realized_pnl": "0",
        "unrealized_eod_pnl": None,
        "trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "largest_winner": None,
        "largest_loser": None,
        "max_intraday_drawdown": None,
        "open_positions_carried_overnight": 0,
        "system_quality_markers": [],
        "tile_visuals": {"fill_color_bucket": "flat", "border_quality": "none", "marker_codes": []},
        "pnl_mode_values": {"realized_only": "0", "realized_plus_unrealized": None, "closed_trade_only": "0"},
        "sample_quality": "NO_RUNTIME_TRADES",
        "source_artifacts": [],
        "trade_list": [],
        "strategy_lane_pnl_breakdown": {},
        "session_pnl_breakdown": {},
        "open_positions_eod": [],
        "incident_system_events": [],
    }


def _accumulate_calendar_trade(day: dict[str, Any], row: Mapping[str, Any]) -> None:
    pnl = _realized_pnl_for_row(row) or Decimal("0")
    day["trade_count"] += 1
    day["win_count"] += 1 if pnl > 0 else 0
    day["loss_count"] += 1 if pnl < 0 else 0
    day["realized_pnl"] = _decimal_text((_decimal(day["realized_pnl"]) or Decimal("0")) + pnl)
    day["largest_winner"] = _decimal_text(max((_decimal(day["largest_winner"]) or pnl), pnl)) if pnl > 0 else day["largest_winner"]
    day["largest_loser"] = _decimal_text(min((_decimal(day["largest_loser"]) or pnl), pnl)) if pnl < 0 else day["largest_loser"]
    day["trade_list"].append(_compact_calendar_trade(row, pnl))
    strategy = str(row.get("strategy_id") or "UNKNOWN")
    lane = str(row.get("lane_id") or row.get("strategy_id") or "UNKNOWN")
    key = f"{strategy}|{lane}"
    bucket = day["strategy_lane_pnl_breakdown"].setdefault(key, {"strategy_id": strategy, "lane_id": lane, "realized_pnl": "0", "trade_count": 0})
    bucket["realized_pnl"] = _decimal_text((_decimal(bucket["realized_pnl"]) or Decimal("0")) + pnl)
    bucket["trade_count"] += 1
    if row.get("review_required") is True:
        day["system_quality_markers"].append({"severity": "RED", "code": "REVIEW_REQUIRED", "label": "Trade record requires review."})
    for path in _extract_source_paths(row):
        if path not in day["source_artifacts"]:
            day["source_artifacts"].append(path)


def _accumulate_calendar_system_event(day: dict[str, Any], row: Mapping[str, Any]) -> None:
    classification = str(row.get("new_artifact_classification") or row.get("reconciliation_action") or "ARTIFACT_RECONCILIATION")
    marker = {
        "severity": "RED" if classification == MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT else "YELLOW",
        "code": classification,
        "label": "Manual/artifact reconciliation event contaminates this trading day sample.",
    }
    day["system_quality_markers"].append(marker)
    event = {
        "event_type": row.get("record_type") or "SYSTEM_EVENT",
        "classification": classification,
        "reconciliation_action": row.get("reconciliation_action"),
        "lifecycle_id": row.get("lifecycle_id"),
        "strategy_id": row.get("strategy_id"),
        "symbol": row.get("instrument_family") or row.get("symbol"),
        "local_symbol": row.get("local_symbol"),
        "con_id": row.get("con_id"),
        "entry_price": row.get("original_entry_fill_price") or row.get("entry_fill_price"),
        "created_at": row.get("created_at"),
        "historical_broker_backed_exposure_confirmed": row.get("historical_broker_backed_exposure_confirmed") is True
        or row.get("broker_backed_position_confirmed") is True,
        "excluded_from_strategy_managed_pnl": row.get("excluded_from_strategy_managed_pnl") is True,
        "excluded_from_clean_trade_stats": row.get("excluded_from_clean_trade_stats") is True,
        "reason_codes": row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else [],
        "source_artifact_paths": _extract_source_paths(row),
    }
    day["incident_system_events"].append(event)
    for path in event["source_artifact_paths"]:
        if path not in day["source_artifacts"]:
            day["source_artifacts"].append(path)


def _finish_calendar_day(day: dict[str, Any]) -> None:
    realized = _decimal(day.get("realized_pnl"))
    unrealized = _decimal(day.get("unrealized_eod_pnl"))
    estimated = realized + unrealized if realized is not None and unrealized is not None else realized
    day["daily_estimated_total_pnl"] = _decimal_text(estimated)
    day["pnl_mode_values"] = {
        "realized_only": _decimal_text(realized),
        "realized_plus_unrealized": _decimal_text(realized + unrealized) if realized is not None and unrealized is not None else None,
        "closed_trade_only": _decimal_text(realized),
    }
    cumulative = Decimal("0")
    peak = Decimal("0")
    max_dd = Decimal("0")
    for trade in day["trade_list"]:
        cumulative += _decimal(trade.get("realized_pnl")) or Decimal("0")
        peak = max(peak, cumulative)
        max_dd = min(max_dd, cumulative - peak)
    day["max_intraday_drawdown"] = _decimal_text(max_dd) if day["trade_count"] else None
    if any(marker.get("severity") in {"RED", "YELLOW"} for marker in day["system_quality_markers"]):
        day["sample_quality"] = "CONTAMINATED_PLUMBING_DEBUG_DAY"
    elif day["trade_count"]:
        day["sample_quality"] = "CLEAN_STRATEGY_SAMPLE_DAY"
    day["tile_visuals"] = _calendar_tile_visuals(day)


def _calendar_tile_visuals(day: Mapping[str, Any]) -> dict[str, Any]:
    pnl = _decimal(day.get("daily_estimated_total_pnl"))
    if pnl is None or pnl == 0:
        fill = "flat"
    elif pnl > 0:
        fill = "profit_strong" if pnl >= Decimal("250") else "profit"
    else:
        fill = "loss_strong" if pnl <= Decimal("-250") else "loss"
    severities = {str(marker.get("severity") or "").upper() for marker in day.get("system_quality_markers") or [] if isinstance(marker, Mapping)}
    border = "red" if "RED" in severities else "yellow" if "YELLOW" in severities else "clean" if day.get("trade_count") else "none"
    return {
        "fill_color_bucket": fill,
        "pnl_basis": "selected_pnl_mode",
        "border_quality": border,
        "marker_codes": [marker.get("code") for marker in day.get("system_quality_markers") or [] if isinstance(marker, Mapping)],
    }


def _calendar_drilldown(day: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_list": day.get("trade_list") or [],
        "strategy_lane_pnl_breakdown": list((day.get("strategy_lane_pnl_breakdown") or {}).values()),
        "session_pnl_breakdown": day.get("session_pnl_breakdown") or {},
        "open_positions_at_end_of_day": day.get("open_positions_eod") or [],
        "incident_system_events": day.get("incident_system_events") or day.get("system_quality_markers") or [],
        "source_artifacts": day.get("source_artifacts") or [],
    }


def _append_daily_history(path: Path, calendar: Mapping[str, Any]) -> None:
    today = datetime.now(UTC).date().isoformat()
    tiles = [item for item in calendar.get("daily_tiles", []) if isinstance(item, Mapping) and item.get("date") == today]
    if not tiles:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"generated_at": calendar.get("generated_at"), "tile": tiles[0]}, sort_keys=True) + "\n")


def _append_portfolio_timeseries(path: Path, point: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(point), sort_keys=True) + "\n")


def _compact_calendar_trade(row: Mapping[str, Any], pnl: Decimal) -> dict[str, Any]:
    return {
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id") or row.get("strategy_id"),
        "symbol": row.get("symbol") or row.get("instrument_family"),
        "contract_key": row.get("contract_key"),
        "side": row.get("side"),
        "entry_timestamp": row.get("entry_timestamp"),
        "exit_timestamp": row.get("exit_timestamp"),
        "realized_pnl": _decimal_text(pnl),
        "source_artifact_paths": _extract_source_paths(row),
    }


def _is_calendar_system_event(row: Mapping[str, Any]) -> bool:
    classification = row.get("new_artifact_classification") or row.get("artifact_reconciliation_classification")
    return row.get("record_type") == "ARTIFACT_RECONCILIATION" or classification in {
        MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
        VOID_MALFORMED_STALE_ARTIFACT,
    }


def _orders_tied_to_position(position: Mapping[str, Any], open_orders: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    tied: list[dict[str, Any]] = []
    for order in open_orders:
        if _track_b_root(order, [str(position.get("track_b_root") or "")]) == position.get("track_b_root") and _local_symbols_compatible(position, order):
            tied.append(dict(order))
    return tied


def _row_source_paths(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_status: Mapping[str, Any],
    reconciled_lifecycle_status: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
) -> list[str]:
    paths = [str(positions_path), str(open_orders_path)]
    for source in (lifecycle, lifecycle_status, reconciled_lifecycle_status):
        if isinstance(source, Mapping):
            paths.extend(_extract_source_paths(source))
    return sorted(dict.fromkeys(path for path in paths if not _path_is_research(Path(path))))


def _extract_source_paths(row: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for key in (
        "source_artifact_paths",
        "paper_lifecycle_report_path",
        "strategy_paper_runner_report_path",
        "monitor_report_path",
        "filled_bridge_result_json",
        "preflight_report_path",
        "recovery_report_path",
        "manual_reconciliation_review_path",
    ):
        value = row.get(key)
        if isinstance(value, list):
            paths.extend(str(item) for item in value if item)
        elif value:
            paths.append(str(value))
    return [path for path in paths if not _path_is_research(Path(path))]


def _runtime_stopped_alert_if_needed(freshness: Mapping[str, Any], *, open_position_count: int, alerts: list[dict[str, Any]]) -> None:
    if open_position_count <= 0:
        return
    stopped = [item for item in freshness.get("services", []) if isinstance(item, Mapping) and item.get("stopped") is True]
    if stopped:
        _add_alert(
            alerts,
            severity="RED",
            code="RUNTIME_STOPPED_WHILE_POSITIONS_OPEN",
            message="A runtime/service status artifact reports stopped while broker truth has open Track B positions.",
            details={"stopped_services": stopped},
        )


def _payload_indicates_stopped(payload: Mapping[str, Any]) -> bool:
    text = " ".join(str(payload.get(key) or "") for key in ("status", "state", "classification", "service_status", "process_status")).upper()
    return any(token in text for token in ("STOPPED", "NOT_RUNNING", "DEAD", "EXITED"))


def _overall_status(freshness: Mapping[str, Any], match_report: Mapping[str, Any], alerts: Sequence[Mapping[str, Any]]) -> str:
    if any(alert.get("severity") == "RED" for alert in alerts):
        return "RED"
    if freshness.get("status") != "OK" or match_report.get("status") != "MATCHED" or any(alert.get("severity") == "YELLOW" for alert in alerts):
        return "DEGRADED"
    return "OK"


def _action_queue(alerts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "severity": alert.get("severity"),
            "code": alert.get("code"),
            "message": alert.get("message"),
            "position_key": alert.get("position_key"),
            "status": "OPEN",
        }
        for alert in alerts
    ]


def _add_alert(
    alerts: list[dict[str, Any]],
    *,
    severity: str,
    code: str,
    message: str,
    position_key: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    item: dict[str, Any] = {"severity": severity, "code": code, "message": message}
    if position_key:
        item["position_key"] = position_key
    if details is not None:
        item["details"] = dict(details)
    alerts.append(item)


def _dedupe_alerts(alerts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str | None]] = set()
    result: list[dict[str, Any]] = []
    for alert in alerts:
        key = (str(alert.get("code")), str(alert.get("position_key") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(dict(alert))
    return result


def _review_required_count(*payloads: Mapping[str, Any]) -> int:
    values: list[int] = []
    for payload in payloads:
        values.append(_int(payload.get("review_required_count")))
        review_positions = payload.get("review_required_positions")
        if isinstance(review_positions, list):
            values.append(len(review_positions))
    return max(values or [0])


def _lifecycle_entry_price(lifecycle: Mapping[str, Any] | None) -> Decimal | None:
    return _first_decimal(lifecycle, "avg_entry_price", "average_entry_price", "entry_fill_price", "entry_price", "fill_price")


def _broker_average_price(position: Mapping[str, Any]) -> Decimal | None:
    average_price = _decimal(position.get("average_price") or position.get("avg_entry_price"))
    if average_price is not None:
        return average_price
    average_cost = _decimal(position.get("average_cost"))
    multiplier = _decimal(position.get("multiplier"))
    if average_cost is None:
        return None
    if multiplier is None or multiplier == 0:
        return average_cost
    return average_cost / multiplier


def _multiplier(root: str, position: Mapping[str, Any]) -> Decimal | None:
    return _decimal(position.get("multiplier")) or FUTURES_MULTIPLIERS.get(root)


def _unrealized_points(*, side: str, entry: Decimal | None, last_price: Decimal | None) -> Decimal | None:
    if entry is None or last_price is None:
        return None
    raw = last_price - entry
    return raw if side == "LONG" else -raw if side == "SHORT" else None


def _signed_lifecycle_quantity(position: Mapping[str, Any], quantity: Decimal | None) -> Decimal | None:
    if quantity is None:
        return None
    side = str(position.get("side") or position.get("position_side") or "").upper()
    return -quantity if side == "SHORT" and quantity > 0 else quantity


def _local_symbols_compatible(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_symbol = str(left.get("local_symbol") or left.get("localSymbol") or "").strip().upper()
    right_symbol = str(right.get("local_symbol") or right.get("localSymbol") or "").strip().upper()
    if left_symbol and right_symbol:
        return left_symbol == right_symbol
    return True


def _position_key(position: Mapping[str, Any]) -> str:
    return "|".join(
        str(position.get(key) or "")
        for key in ("account_id", "symbol", "local_symbol", "localSymbol", "expiry", "quantity")
    )


def _lane_id(lifecycle: Mapping[str, Any] | None) -> str | None:
    if not lifecycle:
        return None
    return str(lifecycle.get("lane_id") or lifecycle.get("strategy_lane_id") or lifecycle.get("runtime_lane_id") or "") or None


def _latest_bar(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    bars = payload.get("bars") if isinstance(payload.get("bars"), list) else payload.get("candles") if isinstance(payload.get("candles"), list) else []
    return bars[-1] if bars and isinstance(bars[-1], Mapping) else {}


def _payload_uses_research_truth(payload: Mapping[str, Any]) -> bool:
    if payload.get("research_artifact_used") is True:
        return True
    for path in _extract_source_paths(payload):
        if _path_is_research(Path(path)):
            return True
    return False


def _row_uses_research_truth(row: Mapping[str, Any]) -> bool:
    return row.get("research_artifact_used") is True or any(_path_is_research(Path(path)) for path in _extract_source_paths(row))


def _path_is_research(path: Path) -> bool:
    text = str(path).lower()
    return any(token in text for token in ("/research/", "historical_playback", "strategy_study", "backtest", "replay_research"))


def _sum_known(values: Iterable[Any]) -> Decimal | None:
    total = Decimal("0")
    found = False
    for value in values:
        decimal = _decimal(value)
        if decimal is None:
            continue
        total += decimal
        found = True
    return total if found else None


def _first_decimal(row: Mapping[str, Any] | None, *keys: str) -> Decimal | None:
    if not row:
        return None
    for key in keys:
        value = row.get(key)
        decimal = _decimal(value)
        if decimal is not None:
            return decimal
    return None


def _value(row: Mapping[str, Any] | None, key: str) -> Any:
    return row.get(key) if isinstance(row, Mapping) else None


def _day_key(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)[:10]


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc(value: datetime | None) -> datetime:
    actual = value or datetime.now(UTC)
    if actual.tzinfo is None:
        actual = actual.replace(tzinfo=UTC)
    return actual.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f") if value != 0 else "0"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _path_from_payload(value: Any, *, default: Path) -> Path:
    return Path(str(value)) if value not in (None, "") else default


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build read-only Track B PAPER portfolio and P&L calendar artifacts.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--broker-truth-root", type=Path, default=None)
    parser.add_argument("--ledger-root", type=Path, default=None)
    parser.add_argument("--market-data-root", type=Path, default=None)
    parser.add_argument("--runtime-feed-root", type=Path, default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--portfolio-timeseries-path", type=Path, default=None)
    parser.add_argument("--calendar-path", type=Path, default=None)
    parser.add_argument("--daily-history-path", type=Path, default=None)
    parser.add_argument("--account", default=PAPER_ACCOUNT)
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--write", action="store_true", default=True)
    parser.add_argument("--no-write", action="store_false", dest="write")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo = args.repo_root
    config = TrackBPortfolioConfig(
        repo_root=repo,
        broker_truth_root=args.broker_truth_root or repo / "outputs" / "reports" / "ibkr_read_only_verification",
        ledger_root=args.ledger_root or repo / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        market_data_root=args.market_data_root or repo / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        runtime_feed_root=args.runtime_feed_root or repo / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed",
        output_path=args.output_path or repo / "outputs" / "reports" / "track_b_portfolio" / "latest_track_b_portfolio_state.json",
        portfolio_timeseries_path=args.portfolio_timeseries_path
        or repo / "outputs" / "reports" / "track_b_portfolio" / "track_b_portfolio_timeseries.jsonl",
        calendar_path=args.calendar_path or repo / "outputs" / "reports" / "track_b_portfolio" / "calendar" / "latest_track_b_pnl_calendar.json",
        daily_history_path=args.daily_history_path or repo / "outputs" / "reports" / "track_b_portfolio" / "calendar" / "track_b_daily_pnl.jsonl",
        account=str(args.account),
        mode=str(args.mode or "PAPER").upper(),
        symbols=tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip()),
    )
    state = build_track_b_portfolio_state(config=config, write=bool(args.write))
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
