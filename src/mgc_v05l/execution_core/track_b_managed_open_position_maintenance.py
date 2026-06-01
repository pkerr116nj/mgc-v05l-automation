"""Read/write Track B managed PAPER open-position maintenance.

This module re-evaluates already-open strategy-managed PAPER lifecycle
positions. It does not create entries and it never falls back to paper-proof.
If an exit policy becomes eligible, the close leg is submitted only through the
guarded strategy-managed lifecycle close path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_databento_live_runtime_feed import DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
from .track_b_exit_safety import (
    DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS,
    RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME,
    TrackBStaleDataState,
    bridge_terminal_event_grace_state,
    classify_broker_truth_freshness,
    classify_market_data_freshness,
)
from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON,
    update_track_b_paper_trade_ledger_from_runner_report,
)
from .track_b_position_management_manifest import (
    DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    resolve_management_metadata,
)
from .track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    TrackBManagedPaperLifecycleClassification,
    TrackBStrategyManagedPaperLifecycleConfig,
    TrackBStrategyManagedPaperLifecycleResult,
    TrackBStrategyManagedPaperLifecycleStages,
    maintain_open_track_b_strategy_managed_paper_lifecycle,
)


DEFAULT_TRACK_B_MANAGED_OPEN_POSITION_MAINTENANCE_JSON = (
    Path("outputs/track_b_execution_core/diagnostics") / "latest_track_b_managed_open_position_maintenance.json"
)
DEFAULT_TRACK_B_PHASE1_RUNTIME_MARKET_DATA_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data")
DEFAULT_TRACK_B_MANAGED_POSITION_PROJECTION_JSON = (
    Path("outputs/track_b_execution_core/managed_positions") / "latest_managed_positions.json"
)

TICK_SIZE_BY_INSTRUMENT = {
    "MGC": Decimal("0.1"),
    "MNQ": Decimal("0.25"),
}


@dataclass(frozen=True)
class TrackBManagedOpenPositionMaintenanceConfig:
    mode: str = "PAPER"
    account_id: str = "DUM882026"
    expected_account_id: str = "DUM882026"
    submit_enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    live_money_readiness: bool = False
    live_runtime_feed_output_root: Path = DEFAULT_TRACK_B_PHASE1_RUNTIME_MARKET_DATA_ROOT
    managed_lifecycle_output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    position_management_manifest_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT
    lane_registry_paths: tuple[Path, ...] = ()
    paper_trade_summary_json: Path = DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON
    live_position_status_json: Path = DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON
    managed_position_projection_json: Path = DEFAULT_TRACK_B_MANAGED_POSITION_PROJECTION_JSON
    diagnostic_json: Path = DEFAULT_TRACK_B_MANAGED_OPEN_POSITION_MAINTENANCE_JSON
    paper_exit_price_offset_ticks: int = 2
    broker_truth_max_age_seconds: float = 120.0
    bridge_terminal_event_grace_seconds: float = DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS


@dataclass(frozen=True)
class TrackBManagedOpenPositionMaintenanceResult:
    report_json: Path
    report: dict[str, Any]
    lifecycle_results: tuple[TrackBStrategyManagedPaperLifecycleResult, ...]


def run_track_b_managed_open_position_maintenance(
    *,
    config: TrackBManagedOpenPositionMaintenanceConfig | None = None,
    lifecycle_stages: TrackBStrategyManagedPaperLifecycleStages | None = None,
    now: datetime | None = None,
) -> TrackBManagedOpenPositionMaintenanceResult:
    actual_config = config or TrackBManagedOpenPositionMaintenanceConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    live_position_status = _read_json(actual_config.live_position_status_json)
    trade_summary = _read_json(actual_config.paper_trade_summary_json)
    managed_position_projection = _read_json(actual_config.managed_position_projection_json)
    projected_positions = _managed_position_projection_by_lifecycle_id(managed_position_projection)
    source_paths = [
        Path(str(item))
        for item in live_position_status.get("source_artifact_paths", [])
        if str(item).endswith(".json")
    ]
    open_positions = _open_positions(live_position_status)
    lifecycle_results: list[TrackBStrategyManagedPaperLifecycleResult] = []
    position_reports: list[dict[str, Any]] = []

    for position in open_positions:
        lifecycle_id = str(position.get("lifecycle_id") or "")
        lifecycle_report_path = _lifecycle_report_path(
            lifecycle_id=lifecycle_id,
            source_paths=source_paths,
            trade_summary=trade_summary,
            lifecycle_output_root=actual_config.managed_lifecycle_output_root,
        )
        lifecycle_report = _read_json(lifecycle_report_path)
        base_position_report = {
            "lifecycle_id": lifecycle_id,
            "strategy_id": position.get("strategy_id") or lifecycle_report.get("strategy_id"),
            "instrument": position.get("instrument_family") or lifecycle_report.get("instrument_family"),
            "contract_key": position.get("contract_key") or lifecycle_report.get("contract_key"),
            "local_symbol": position.get("local_symbol") or lifecycle_report.get("local_symbol"),
            "entry_timestamp": _entry_timestamp(lifecycle_report),
            "lifecycle_report_path": str(lifecycle_report_path),
        }
        if not lifecycle_report:
            lifecycle_report = _recover_missing_lifecycle_report_from_position(
                position=position,
                lifecycle_report_path=lifecycle_report_path,
                maintenance_config=actual_config,
                now=actual_now,
            )
            if lifecycle_report:
                base_position_report = {
                    **base_position_report,
                    "strategy_id": position.get("strategy_id") or lifecycle_report.get("strategy_id"),
                    "instrument": position.get("instrument_family") or lifecycle_report.get("instrument_family"),
                    "contract_key": position.get("contract_key") or lifecycle_report.get("contract_key"),
                    "local_symbol": position.get("local_symbol") or lifecycle_report.get("local_symbol"),
                    "entry_timestamp": _entry_timestamp(lifecycle_report),
                    "lifecycle_report_recovered": True,
                }
        if not lifecycle_report:
            position_reports.append(
                {
                    **base_position_report,
                    "maintenance_invoked": False,
                    "blocker": "OPEN_MANAGED lifecycle report is missing.",
                }
            )
            continue
        if lifecycle_report.get("paper_lifecycle_classification") != TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value:
            position_reports.append(
                {
                    **base_position_report,
                    "maintenance_invoked": False,
                    "blocker": "Lifecycle is no longer OPEN_MANAGED.",
                    "final_classification": lifecycle_report.get("paper_lifecycle_classification"),
                }
            )
            continue

        projected_position = projected_positions.get(lifecycle_id, {})
        instrument = str(position.get("instrument_family") or lifecycle_report.get("instrument_family") or "")
        completed_payload = _read_json(_completed_5m_path(actual_config.live_runtime_feed_output_root, instrument))
        live_1m_payload = _read_json(_live_1m_path(actual_config.live_runtime_feed_output_root, instrument))
        entry_timestamp = _entry_timestamp(lifecycle_report)
        signal_timestamp = _signal_timestamp(lifecycle_report)
        completed_timestamps = _completed_bar_timestamps_after_entry(
            completed_payload=completed_payload,
            entry_timestamp=entry_timestamp,
        )
        completed_bars_since_entry = len(completed_timestamps)
        projected_bars_since_entry = _int_or_none(
            projected_position.get("completed_bars_since_entry")
            or projected_position.get("bars_since_entry")
            or projected_position.get("bars_since_fill")
        )
        projected_exit_due = _projection_exit_due(projected_position)
        effective_completed_bars_since_entry = max(
            completed_bars_since_entry,
            projected_bars_since_entry or 0,
        )
        completed_bars_since_signal = len(
            _completed_bar_timestamps_after_entry(
                completed_payload=completed_payload,
                entry_timestamp=signal_timestamp,
            )
        )
        market_data_state = classify_market_data_freshness(
            latest_1m_age_seconds=_latest_age_seconds(
                payload=live_1m_payload,
                now=actual_now,
                explicit_age_keys=("latest_1m_age_seconds", "latest_1m_candle_age_seconds"),
            ),
            latest_completed_5m_age_seconds=_latest_age_seconds(
                payload=completed_payload,
                now=actual_now,
                explicit_age_keys=("latest_completed_5m_age_seconds", "latest_completed_5m_candle_age_seconds"),
            ),
            latest_1m_threshold_seconds=RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME["1m"],
            completed_5m_threshold_seconds=RUNTIME_CANDLE_FRESHNESS_SECONDS_BY_TIMEFRAME["5m"],
        )
        close_fill = lifecycle_report.get("close_fill") if isinstance(lifecycle_report.get("close_fill"), Mapping) else None
        broker_truth_state = (
            bridge_terminal_event_grace_state(
                event=close_fill,
                now=actual_now,
                account_id=str(lifecycle_report.get("account_id") or actual_config.account_id),
                contract_key=str(lifecycle_report.get("contract_key") or position.get("contract_key") or ""),
                local_symbol=str(lifecycle_report.get("local_symbol") or position.get("local_symbol") or ""),
                con_id=_int_or_none(lifecycle_report.get("con_id") or position.get("con_id")),
                ttl_seconds=actual_config.bridge_terminal_event_grace_seconds,
            )
            if close_fill
            else classify_broker_truth_freshness(age_seconds=0.0, max_age_seconds=actual_config.broker_truth_max_age_seconds)
        )
        broker_state_value = getattr(broker_truth_state, "state", TrackBStaleDataState.FRESH)
        suppress_discretionary_exit = market_data_state.suppress_discretionary_exits or broker_state_value in {
            TrackBStaleDataState.STALE_RESTRICT_DISCRETIONARY_EXITS,
            TrackBStaleDataState.SEVERE_STALE_EMERGENCY_REVIEW,
        }
        metadata = resolve_management_metadata(
            source={**dict(lifecycle_report), **dict(position)},
            output_root=actual_config.position_management_manifest_root,
            lane_registry_paths=actual_config.lane_registry_paths,
        )
        managed_exit_policy_id = str(metadata.managed_exit_policy_id or "")
        if managed_exit_policy_id:
            lifecycle_report = {**dict(lifecycle_report), "managed_exit_policy_id": managed_exit_policy_id}
        required_bars = int(lifecycle_report.get("managed_exit_policy_max_completed_5m_bars") or 3)
        projected_required_bars = _int_or_none(
            projected_position.get("required_completed_5m_bars")
            or projected_position.get("managed_exit_policy_max_completed_5m_bars")
        )
        if projected_required_bars is not None:
            required_bars = projected_required_bars
            lifecycle_report = {
                **dict(lifecycle_report),
                "managed_exit_policy_max_completed_5m_bars": required_bars,
            }
        exit_eligible = (
            effective_completed_bars_since_entry >= required_bars or projected_exit_due
        ) and not suppress_discretionary_exit
        lifecycle_projection_conflict = (
            projected_exit_due
            and int(lifecycle_report.get("bars_since_fill") or lifecycle_report.get("open_position_age_completed_5m_bars") or 0)
            < required_bars
        )
        if not metadata.complete:
            position_reports.append(
                {
                    **base_position_report,
                    "maintenance_invoked": False,
                    "latest_completed_5m_bar_timestamp": completed_timestamps[-1] if completed_timestamps else None,
                    "completed_bars_since_entry": completed_bars_since_entry,
                    "effective_completed_bars_since_entry": effective_completed_bars_since_entry,
                    "managed_position_projection_classification": projected_position.get("classification"),
                    "managed_position_projection_exit_due": projected_exit_due,
                    "managed_position_projection_bars_since_entry": projected_bars_since_entry,
                    "lifecycle_report_stale_exit_due_conflict": lifecycle_projection_conflict,
                    "bars_since_fill": completed_bars_since_entry,
                    "data_freshness": market_data_state.to_json_dict(),
                    "data_freshness_state": market_data_state.state.value,
                    "broker_truth_state": broker_state_value.value,
                    "suppressed_due_to_stale_data": suppress_discretionary_exit,
                    "exit_policy_id": None,
                    "required_completed_5m_bars": required_bars,
                    "exit_eligible": False,
                    "close_intent_created": False,
                    "close_submitted": False,
                    "close_filled": False,
                    "final_classification": OPEN_MANAGED_METADATA_INCOMPLETE,
                    "final_position_status": "OPEN_MANAGED",
                    "review_required": True,
                    "blocker": OPEN_MANAGED_METADATA_INCOMPLETE,
                    "position_management_metadata_blockers": list(metadata.blockers),
                    "position_management_manifest_path": None
                    if metadata.manifest_path is None
                    else str(metadata.manifest_path),
                }
            )
            continue
        close_limit_price = _derive_close_limit_price(
            live_runtime_feed_output_root=actual_config.live_runtime_feed_output_root,
            instrument=instrument,
            side=str((lifecycle_report.get("entry_intent") or {}).get("side") or lifecycle_report.get("side") or ""),
            tick_size=_tick_size(instrument),
            offset_ticks=actual_config.paper_exit_price_offset_ticks,
        )
        lifecycle_config = _lifecycle_config_from_report(
            maintenance_config=actual_config,
            lifecycle_report=lifecycle_report,
            position=position,
            completed_bars_since_entry=effective_completed_bars_since_entry,
            completed_bars_since_signal=completed_bars_since_signal,
            close_limit_price=close_limit_price,
            data_freshness_state=market_data_state.state.value,
            broker_truth_state=broker_state_value.value,
            suppress_discretionary_exit=suppress_discretionary_exit,
        )
        if actual_config.submit_enabled is not True:
            position_reports.append(
                {
                    **base_position_report,
                    "maintenance_invoked": True,
                    "maintenance_mode": "DIAGNOSTIC_DRY_RUN_SUBMIT_DISABLED",
                    "latest_completed_5m_bar_timestamp": completed_timestamps[-1] if completed_timestamps else None,
                    "completed_bars_since_entry": completed_bars_since_entry,
                    "effective_completed_bars_since_entry": effective_completed_bars_since_entry,
                    "managed_position_projection_classification": projected_position.get("classification"),
                    "managed_position_projection_exit_due": projected_exit_due,
                    "managed_position_projection_bars_since_entry": projected_bars_since_entry,
                    "lifecycle_report_stale_exit_due_conflict": lifecycle_projection_conflict,
                    "bars_since_fill": completed_bars_since_entry,
                    "bars_since_signal": completed_bars_since_signal,
                    "fill_timestamp_source": "BROKER_ENTRY_FILL",
                    "completed_5m_bar_timestamps_since_entry": completed_timestamps,
                    "data_freshness": market_data_state.to_json_dict(),
                    "data_freshness_state": market_data_state.state.value,
                    "broker_truth_state": broker_state_value.value,
                    "broker_truth_freshness": broker_truth_state.to_json_dict(),
                    "bridge_terminal_event_grace": None,
                    "suppressed_due_to_stale_data": suppress_discretionary_exit,
                    "exit_family": "DIAGNOSTIC_TIME" if managed_exit_policy_id else None,
                    "exit_reason": "TIME_BOXED_EXIT" if exit_eligible else None,
                    "hard_exit": False,
                    "discretionary_exit": bool(exit_eligible),
                    "mfe": None,
                    "mae": None,
                    "exit_policy_id": managed_exit_policy_id,
                    "required_completed_5m_bars": required_bars,
                    "exit_eligible": exit_eligible,
                    "close_limit_price": close_limit_price,
                    "close_intent_created": bool(exit_eligible),
                    "close_submitted": False,
                    "close_filled": False,
                    "close_order_id": None,
                    "close_submit_timestamp": None,
                    "close_fill_timestamp": None,
                    "exit_price": None,
                    "realized_pnl": None,
                    "final_classification": lifecycle_report.get("paper_lifecycle_classification"),
                    "final_position_status": lifecycle_report.get("final_position_status") or "OPEN_MANAGED",
                    "review_required": lifecycle_report.get("review_required") is True,
                    "blocker": "SUBMIT_DISABLED_DRY_RUN" if exit_eligible else None,
                }
            )
            continue
        result = maintain_open_track_b_strategy_managed_paper_lifecycle(
            config=lifecycle_config,
            existing_lifecycle_report=lifecycle_report,
            stages=lifecycle_stages,
            now=actual_now,
        )
        lifecycle_results.append(result)
        if result.classification != TrackBManagedPaperLifecycleClassification.OPEN_MANAGED:
            _record_lifecycle_update_in_ledger(
                maintenance_config=actual_config,
                lifecycle_result=result,
                now=actual_now,
            )
        close_submit = result.report.get("close_submit_attempt") or {}
        close_fill = result.report.get("close_fill") or {}
        position_reports.append(
            {
                **base_position_report,
                "maintenance_invoked": True,
                "latest_completed_5m_bar_timestamp": completed_timestamps[-1] if completed_timestamps else None,
                "completed_bars_since_entry": completed_bars_since_entry,
                "effective_completed_bars_since_entry": effective_completed_bars_since_entry,
                "managed_position_projection_classification": projected_position.get("classification"),
                "managed_position_projection_exit_due": projected_exit_due,
                "managed_position_projection_bars_since_entry": projected_bars_since_entry,
                "lifecycle_report_stale_exit_due_conflict": lifecycle_projection_conflict,
                "bars_since_fill": completed_bars_since_entry,
                "bars_since_signal": completed_bars_since_signal,
                "fill_timestamp_source": "BROKER_ENTRY_FILL",
                "completed_5m_bar_timestamps_since_entry": completed_timestamps,
                "data_freshness": market_data_state.to_json_dict(),
                "data_freshness_state": market_data_state.state.value,
                "broker_truth_state": broker_state_value.value,
                "broker_truth_freshness": broker_truth_state.to_json_dict(),
                "bridge_terminal_event_grace": broker_truth_state.to_json_dict() if close_fill else None,
                "suppressed_due_to_stale_data": suppress_discretionary_exit,
                "exit_family": "DIAGNOSTIC_TIME" if managed_exit_policy_id else None,
                "exit_reason": result.report.get("close_intent", {}).get("close_reason") if isinstance(result.report.get("close_intent"), Mapping) else None,
                "hard_exit": bool((result.report.get("close_intent") or {}).get("hard_exit")) if isinstance(result.report.get("close_intent"), Mapping) else False,
                "discretionary_exit": bool((result.report.get("close_intent") or {}).get("discretionary_exit")) if isinstance(result.report.get("close_intent"), Mapping) else False,
                "mfe": None,
                "mae": None,
                "exit_policy_id": managed_exit_policy_id,
                "required_completed_5m_bars": required_bars,
                "exit_eligible": exit_eligible,
                "close_limit_price": close_limit_price,
                "close_intent_created": result.report.get("close_intent") is not None,
                "close_submitted": bool(close_submit.get("submitted") or close_submit.get("submit_attempted")),
                "close_filled": bool(close_fill),
                "close_order_id": close_submit.get("broker_order_id"),
                "close_submit_timestamp": close_submit.get("submitted_at"),
                "close_fill_timestamp": close_fill.get("filled_at"),
                "exit_price": close_fill.get("price") or close_fill.get("avg_price"),
                "realized_pnl": result.report.get("realized_pnl"),
                "final_classification": result.report.get("paper_lifecycle_classification"),
                "final_position_status": result.report.get("final_position_status"),
                "review_required": result.report.get("review_required"),
                "blocker": result.report.get("primary_blocker"),
            }
        )

    report = {
        "schema_version": "track_b_managed_open_position_maintenance_v1",
        "generated_at": actual_now.isoformat(),
        "mode": actual_config.mode,
        "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "open_lifecycle_count": len(open_positions),
        "positions": position_reports,
        "close_intent_created_count": sum(1 for item in position_reports if item.get("close_intent_created") is True),
        "close_submitted_count": sum(1 for item in position_reports if item.get("close_submitted") is True),
        "close_filled_count": sum(1 for item in position_reports if item.get("close_filled") is True),
        "review_required_count": sum(1 for item in position_reports if item.get("review_required") is True),
        "broker_state_mutated": any(result.report.get("broker_state_mutated") is True for result in lifecycle_results),
        "submit_attempted": any(result.report.get("submit_attempted") is True for result in lifecycle_results),
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "diagnostic_json_path": str(actual_config.diagnostic_json),
    }
    _write_json(actual_config.diagnostic_json, report)
    return TrackBManagedOpenPositionMaintenanceResult(
        report_json=actual_config.diagnostic_json,
        report=report,
        lifecycle_results=tuple(lifecycle_results),
    )


def _open_positions(live_position_status: Mapping[str, Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for value in (live_position_status.get("positions_by_instrument") or {}).values():
        if isinstance(value, Mapping) and value.get("lifecycle_id"):
            positions.append(dict(value))
    return positions


def _managed_position_projection_by_lifecycle_id(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw_positions = payload.get("positions") or payload.get("managed_positions") or []
    if not isinstance(raw_positions, list):
        return {}
    projected: dict[str, dict[str, Any]] = {}
    for item in raw_positions:
        if not isinstance(item, Mapping):
            continue
        lifecycle_id = str(item.get("lifecycle_id") or "").strip()
        if lifecycle_id:
            projected[lifecycle_id] = dict(item)
    return projected


def _projection_exit_due(position: Mapping[str, Any]) -> bool:
    if not position:
        return False
    classification = str(
        position.get("classification")
        or position.get("managed_position_classification")
        or position.get("state")
        or ""
    ).upper()
    if classification == "OPEN_MANAGED_EXIT_DUE":
        return True
    return position.get("exit_due") is True or position.get("timebox_due") is True


def _lifecycle_report_path(
    *,
    lifecycle_id: str,
    source_paths: list[Path],
    trade_summary: Mapping[str, Any],
    lifecycle_output_root: Path,
) -> Path:
    for path in source_paths:
        if lifecycle_id and lifecycle_id in str(path):
            return path
    for item in trade_summary.get("recent_trades", []) or []:
        if isinstance(item, Mapping) and str(item.get("lifecycle_id") or "") == lifecycle_id:
            value = item.get("paper_lifecycle_report_path")
            if value:
                return Path(str(value))
    return Path(lifecycle_output_root) / lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"


def _lifecycle_config_from_report(
    *,
    maintenance_config: TrackBManagedOpenPositionMaintenanceConfig,
    lifecycle_report: Mapping[str, Any],
    position: Mapping[str, Any],
    completed_bars_since_entry: int,
    completed_bars_since_signal: int | None,
    close_limit_price: str | None,
    data_freshness_state: str,
    broker_truth_state: str,
    suppress_discretionary_exit: bool,
) -> TrackBStrategyManagedPaperLifecycleConfig:
    entry_intent = lifecycle_report.get("entry_intent") if isinstance(lifecycle_report.get("entry_intent"), Mapping) else {}
    canonical_contract_fields = (
        lifecycle_report.get("canonical_broker_contract_fields")
        if isinstance(lifecycle_report.get("canonical_broker_contract_fields"), Mapping)
        else {}
    )
    instrument = str(lifecycle_report.get("instrument_family") or position.get("instrument_family") or "")
    return TrackBStrategyManagedPaperLifecycleConfig(
        mode=maintenance_config.mode,
        account_id=str(lifecycle_report.get("account_id") or maintenance_config.account_id),
        expected_account_id=str(lifecycle_report.get("expected_account_id") or maintenance_config.expected_account_id),
        strategy_id=str(lifecycle_report.get("strategy_id") or position.get("strategy_id") or ""),
        instrument_family=instrument,
        contract_key=str(lifecycle_report.get("contract_key") or position.get("contract_key") or ""),
        local_symbol=str(lifecycle_report.get("local_symbol") or position.get("local_symbol") or ""),
        con_id=_int_or_none(lifecycle_report.get("con_id") or position.get("con_id")),
        side=str(entry_intent.get("side") or lifecycle_report.get("side") or ""),
        quantity=_int_or_none(position.get("quantity") or entry_intent.get("quantity") or lifecycle_report.get("quantity")),
        signal_timestamp=entry_intent.get("signal_timestamp"),
        signal_reason=entry_intent.get("signal_reason"),
        decision_bar_timestamp=entry_intent.get("decision_bar_timestamp"),
        latest_decision_bar_source=str(entry_intent.get("latest_decision_bar_source") or "DATABENTO_LIVE_ARTIFACT"),
        pricing_policy=str(lifecycle_report.get("pricing_policy") or "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT"),
        entry_limit_price=entry_intent.get("entry_limit_price"),
        close_limit_price=close_limit_price,
        managed_exit_policy_id=str(lifecycle_report.get("managed_exit_policy_id") or entry_intent.get("managed_exit_policy_id") or ""),
        managed_exit_policy_max_completed_5m_bars=int(lifecycle_report.get("managed_exit_policy_max_completed_5m_bars") or 3),
        completed_5m_bars_since_entry=completed_bars_since_entry,
        completed_5m_bars_since_signal=completed_bars_since_signal,
        fill_timestamp_source="BROKER_ENTRY_FILL",
        data_freshness_state=data_freshness_state,
        broker_truth_state=broker_truth_state,
        suppress_discretionary_exits_due_to_stale_data=suppress_discretionary_exit,
        submit_enabled=maintenance_config.submit_enabled,
        host=maintenance_config.host,
        port=maintenance_config.port,
        client_id=maintenance_config.client_id,
        order_type=maintenance_config.order_type,
        time_in_force=maintenance_config.time_in_force,
        exchange=str(canonical_contract_fields.get("exchange") or _default_exchange(instrument)),
        currency=str(canonical_contract_fields.get("currency") or "USD"),
        tick_size=str(_tick_size(instrument)),
        source_id="track_b_managed_open_position_maintenance",
        output_root=maintenance_config.managed_lifecycle_output_root,
        paper_trade_ledger_output_root=maintenance_config.paper_trade_ledger_output_root,
        live_position_status_json=maintenance_config.live_position_status_json,
        live_money_readiness=maintenance_config.live_money_readiness,
        broker_reconciled=False,
    )


def _recover_missing_lifecycle_report_from_position(
    *,
    position: Mapping[str, Any],
    lifecycle_report_path: Path,
    maintenance_config: TrackBManagedOpenPositionMaintenanceConfig,
    now: datetime,
) -> dict[str, Any]:
    metadata = resolve_management_metadata(
        source=dict(position),
        output_root=maintenance_config.position_management_manifest_root,
        lane_registry_paths=maintenance_config.lane_registry_paths,
    )
    if not metadata.complete:
        return {}
    lifecycle_id = str(position.get("lifecycle_id") or "").strip()
    strategy_id = str(position.get("strategy_id") or position.get("lane_id") or "").strip()
    instrument = str(position.get("instrument_family") or position.get("instrument") or "").upper()
    contract_key = str(position.get("contract_key") or "").strip()
    local_symbol = str(position.get("local_symbol") or "").strip()
    con_id = _int_or_none(position.get("con_id"))
    quantity = _int_or_none(position.get("quantity"))
    side = _normalize_side(position.get("side") or position.get("order_action"))
    entry_timestamp = str(position.get("entry_timestamp") or "").strip()
    if not all([lifecycle_id, strategy_id, instrument, contract_key, local_symbol, side, entry_timestamp]) or con_id is None or quantity is None:
        return {}
    broker_identity = (
        position.get("entry_broker_identity")
        if isinstance(position.get("entry_broker_identity"), Mapping)
        else {}
    )
    entry_price = _decimal_text(_decimal(position.get("entry_fill_price") or position.get("avg_entry_price")))
    report = {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "generated_at": now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "trade_id": position.get("trade_id") or f"{strategy_id}:{lifecycle_id}",
        "strategy_id": strategy_id,
        "instrument_family": instrument,
        "contract_key": contract_key,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "account_id": position.get("account_id") or maintenance_config.account_id,
        "expected_account_id": maintenance_config.expected_account_id,
        "mode": maintenance_config.mode,
        "managed_exit_policy_id": metadata.managed_exit_policy_id,
        "managed_exit_policy_max_completed_5m_bars": 3,
        "open_position_age_completed_5m_bars": 0,
        "bars_since_fill": 0,
        "bars_since_signal": 0,
        "fill_timestamp_source": "BROKER_ENTRY_FILL",
        "data_freshness_state": "FRESH",
        "broker_truth_state": "FRESH",
        "suppressed_due_to_stale_data": False,
        "expected_exit_condition": "TIME_BOXED_EXIT_AFTER_3_COMPLETED_5M_BARS",
        "close_intent_status": "WAITING_FOR_EXIT_POLICY_CONDITION",
        "strategy_managed_lifecycle_classification": TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value,
        "paper_lifecycle_classification": TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value,
        "submit_enabled": True,
        "managed_paper_submit_enabled": True,
        "entry_intent": {
            "intent_schema_version": "track_b_strategy_managed_paper_entry_intent_v1",
            "created_at": position.get("signal_timestamp") or entry_timestamp,
            "lifecycle_id": lifecycle_id,
            "trade_id": position.get("trade_id") or f"{strategy_id}:{lifecycle_id}",
            "strategy_id": strategy_id,
            "instrument_family": instrument,
            "contract_key": contract_key,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "account_id": position.get("account_id") or maintenance_config.account_id,
            "expected_account_id": maintenance_config.expected_account_id,
            "side": side,
            "order_action": "BUY" if side == "LONG" else "SELL",
            "quantity": quantity,
            "signal_timestamp": position.get("signal_timestamp"),
            "decision_bar_timestamp": position.get("signal_timestamp"),
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            "managed_exit_policy_id": metadata.managed_exit_policy_id,
            "source_id": "track_b_managed_open_position_maintenance_recovery",
        },
        "entry_submit_attempt": {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": position.get("entry_order_id") or broker_identity.get("broker_order_id"),
            "perm_id": position.get("entry_perm_id") or broker_identity.get("perm_id"),
        },
        "entry_fill": {
            "broker_order_id": position.get("entry_order_id") or broker_identity.get("broker_order_id"),
            "perm_id": position.get("entry_perm_id") or broker_identity.get("perm_id"),
            "execution_id": position.get("entry_exec_id") or broker_identity.get("exec_id"),
            "price": entry_price,
            "quantity": position.get("quantity"),
            "filled_at": entry_timestamp,
        },
        "close_intent": None,
        "close_submit_attempt": None,
        "close_fill": None,
        "realized_pnl": None,
        "pnl_currency": "USD",
        "final_position_status": "OPEN_MANAGED",
        "final_broker_state_classification": TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value,
        "review_required": False,
        "broker_reconciled": False,
        "source": "TRACK_B_MANAGED_OPEN_POSITION_MAINTENANCE_RECOVERY",
        "submit_allowed": True,
        "submit_attempted": True,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "broker_state_mutated": True,
        "live_money_readiness": False,
        "ui_authority": False,
        "hidden_submit": False,
        "primary_blocker": None,
        "required_next_action": "Position is open under strategy-managed PAPER state; wait for strategy exit policy.",
        "report_json_path": str(lifecycle_report_path),
        "latest_report_json_path": str(lifecycle_report_path.parent.parent / "latest_track_b_strategy_managed_paper_lifecycle_report.json"),
        "position_management_manifest_path": None if metadata.manifest_path is None else str(metadata.manifest_path),
    }
    _write_json(lifecycle_report_path, report)
    _write_json(Path(str(report["latest_report_json_path"])), report)
    return report


def _record_lifecycle_update_in_ledger(
    *,
    maintenance_config: TrackBManagedOpenPositionMaintenanceConfig,
    lifecycle_result: TrackBStrategyManagedPaperLifecycleResult,
    now: datetime,
) -> None:
    report = lifecycle_result.report
    runner_report = {
        "strategy_id": report.get("strategy_id"),
        "mode": report.get("mode"),
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": report.get("account_id"),
        "contract_key": report.get("contract_key"),
        "local_symbol": report.get("local_symbol"),
        "con_id": report.get("con_id"),
        "quantity": (report.get("entry_intent") or {}).get("quantity"),
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": report.get("strategy_managed_lifecycle_classification"),
        "managed_lifecycle_report_path": str(lifecycle_result.report_json),
        "managed_exit_policy_id": report.get("managed_exit_policy_id"),
        "managed_open_position_age_completed_5m_bars": report.get("open_position_age_completed_5m_bars"),
        "managed_expected_exit_condition": report.get("expected_exit_condition"),
        "managed_close_intent_status": report.get("close_intent_status"),
        "managed_close_intent": report.get("close_intent"),
        "managed_close_submit_attempt": report.get("close_submit_attempt"),
        "managed_close_fill": report.get("close_fill"),
        "paper_proof_invoked": False,
        "submit_attempted": report.get("submit_attempted"),
        "broker_state_mutated": report.get("broker_state_mutated"),
        "final_position_status": report.get("final_position_status"),
        "final_broker_state_classification": report.get("final_broker_state_classification"),
    }
    update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner_report,
        runner_report_json=maintenance_config.diagnostic_json,
        output_root=maintenance_config.paper_trade_ledger_output_root,
        now=now,
    )


def _completed_5m_path(root: Path, instrument: str) -> Path:
    canonical = Path(root) / str(instrument).upper() / "5m" / "latest_runtime_candles.json"
    if canonical.exists():
        return canonical
    return Path(root) / f"latest_live_{instrument.lower()}_completed_5m_candles.json"


def _live_1m_path(root: Path, instrument: str) -> Path:
    canonical = Path(root) / str(instrument).upper() / "1m" / "latest_runtime_candles.json"
    if canonical.exists():
        return canonical
    return Path(root) / f"latest_live_{instrument.lower()}_1m_candles.json"


def _completed_bar_timestamps_after_entry(
    *,
    completed_payload: Mapping[str, Any],
    entry_timestamp: str | None,
) -> list[str]:
    entry = _parse_time(entry_timestamp)
    values: list[str] = []
    for candle in _payload_bars(completed_payload):
        if not isinstance(candle, Mapping):
            continue
        timestamp = str(candle.get("candle_timestamp") or candle.get("timestamp") or candle.get("bar_end") or "")
        parsed = _parse_time(timestamp)
        if parsed is None:
            continue
        if entry is None or parsed > entry:
            values.append(parsed.astimezone(UTC).isoformat())
    return values


def _derive_close_limit_price(
    *,
    live_runtime_feed_output_root: Path,
    instrument: str,
    side: str,
    tick_size: Decimal,
    offset_ticks: int,
) -> str | None:
    payload = _read_json(_live_1m_path(live_runtime_feed_output_root, instrument))
    candles = _payload_bars(payload)
    if not candles:
        return None
    last = _decimal(candles[-1].get("close") or candles[-1].get("last_price"))
    if last is None:
        return None
    offset = tick_size * Decimal(max(int(offset_ticks), 0))
    close_side = str(side or "").strip().upper()
    price = last - offset if close_side == "LONG" else last + offset
    return _decimal_text(_round_to_tick(price, tick_size))


def _entry_timestamp(lifecycle_report: Mapping[str, Any]) -> str | None:
    entry_fill = lifecycle_report.get("entry_fill") if isinstance(lifecycle_report.get("entry_fill"), Mapping) else {}
    return entry_fill.get("filled_at") or entry_fill.get("timestamp") or lifecycle_report.get("entry_timestamp")


def _signal_timestamp(lifecycle_report: Mapping[str, Any]) -> str | None:
    entry_intent = lifecycle_report.get("entry_intent") if isinstance(lifecycle_report.get("entry_intent"), Mapping) else {}
    return (
        entry_intent.get("signal_timestamp")
        or entry_intent.get("decision_bar_timestamp")
        or lifecycle_report.get("signal_timestamp")
        or lifecycle_report.get("decision_bar_timestamp")
    )


def _latest_age_seconds(
    *,
    payload: Mapping[str, Any],
    now: datetime,
    explicit_age_keys: tuple[str, ...],
) -> float | None:
    for key in explicit_age_keys:
        value = payload.get(key)
        if value not in {None, ""}:
            try:
                return max(0.0, float(value))
            except (TypeError, ValueError):
                continue
    timestamp = _latest_payload_timestamp(payload)
    return None if timestamp is None else max(0.0, (now.astimezone(UTC) - timestamp).total_seconds())


def _latest_payload_timestamp(payload: Mapping[str, Any]) -> datetime | None:
    for key in (
        "latest_1m_timestamp",
        "latest_1m_candle_timestamp",
        "latest_completed_5m_timestamp",
        "latest_completed_5m_candle_timestamp",
        "last_completed_bar_ts",
        "candle_timestamp",
        "last_candle_timestamp",
        "bar_end",
        "generated_at",
    ):
        parsed = _parse_time(payload.get(key))
        if parsed is not None:
            return parsed
    candles = _payload_bars(payload)
    if candles:
        return _parse_time(candles[-1].get("candle_timestamp") or candles[-1].get("timestamp") or candles[-1].get("bar_end"))
    return None


def _payload_bars(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = payload.get("candles") or payload.get("bars") or payload.get("completed_5m_candles") or []
    return [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _read_json(path: Path | str | None) -> dict[str, Any]:
    if path in {None, ""}:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 - malformed market data is a missing price.
        return None


def _round_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _tick_size(instrument: str) -> Decimal:
    return TICK_SIZE_BY_INSTRUMENT.get(str(instrument).upper(), Decimal("0.1"))


def _default_exchange(instrument: str) -> str:
    return "CME" if str(instrument).upper() in {"MNQ", "NQ", "ES", "MES"} else "COMEX"


def _normalize_side(value: object) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"LONG", "BUY", "BUY_TO_OPEN"}:
        return "LONG"
    if raw in {"SHORT", "SELL", "SELL_TO_OPEN"}:
        return "SHORT"
    return raw


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
