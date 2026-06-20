"""Inventory and intent adapters for porting live paper strategies to the IBKR paper bridge."""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ibkr_phase1_futures_scope import (
    phase1_execution_target_for_source,
    supported_phase1_source_instruments,
)
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status

_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_strategy_porting"
_DEFAULT_DASHBOARD_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json"
_DEFAULT_SIGNAL_AUDIT_PATH = Path("outputs") / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
_DEFAULT_STRATEGY_PERFORMANCE_PATH = Path("outputs") / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_BRIDGE_AUDIT_PATH = Path("outputs") / "reports" / "ibkr_strategy_porting" / "ibkr_paper_strategy_bridge_porting_audit.jsonl"
_SUPPORTED_EXECUTABLE_INSTRUMENTS = supported_phase1_source_instruments()
_INITIAL_EXECUTABLE_INSTRUMENT = "MGC"
_ATP_LANE_ID = "atp_companion_v1_asia_us"
_ATP_STRATEGY_ID = "ATP_COMPANION_V1_ASIA_US"
_BATCH1_ACTIVE_EVIDENCE_LANE_SUFFIXES = (
    "us_active_participation_long",
    "us_active_participation_short",
    "globex_active_participation_long",
    "globex_active_participation_short",
    "london_open_active_participation_long",
    "london_open_active_participation_short",
    "london_late_active_participation_short",
)


def _active_evidence_lane_ids(symbol: str) -> tuple[str, ...]:
    return tuple(f"{symbol.lower()}_{suffix}" for suffix in _BATCH1_ACTIVE_EVIDENCE_LANE_SUFFIXES)


_BATCH1_ACTIVE_EVIDENCE_CONTRACTS: dict[str, dict[str, Any]] = {
    "MGC": {
        "symbol": "MGC",
        "contract_month": "202608",
        "expiry": "20260827",
        "con_id": 732156883,
        "local_symbol": "MGCQ6",
        "exchange": "COMEX",
        "currency": "USD",
        "multiplier": "10",
        "min_tick": "0.1",
        "trading_class": "MGC",
    },
    "GC": {
        "symbol": "GC",
        "contract_month": "202608",
        "expiry": "20260827",
        "con_id": 732156872,
        "local_symbol": "GCQ6",
        "exchange": "COMEX",
        "currency": "USD",
        "multiplier": "100",
        "min_tick": "0.1",
        "trading_class": "GC",
    },
    "NQ": {
        "symbol": "NQ",
        "contract_month": "202609",
        "expiry": "20260918",
        "con_id": 770561204,
        "local_symbol": "NQU6",
        "exchange": "CME",
        "currency": "USD",
        "multiplier": "20",
        "min_tick": "0.25",
        "trading_class": "NQ",
    },
    "ES": {
        "symbol": "ES",
        "contract_month": "202609",
        "expiry": "20260918",
        "con_id": 649180671,
        "local_symbol": "ESU6",
        "exchange": "CME",
        "currency": "USD",
        "multiplier": "50",
        "min_tick": "0.25",
        "trading_class": "ES",
    },
    "ZT": {
        "symbol": "ZT",
        "contract_month": "202609",
        "expiry": "20260930",
        "con_id": 842590391,
        "local_symbol": "ZTU6",
        "exchange": "CBOT",
        "currency": "USD",
        "multiplier": "2000",
        "min_tick": "0.00390625",
        "trading_class": "ZT",
    },
    "ZF": {
        "symbol": "ZF",
        "contract_month": "202609",
        "expiry": "20260930",
        "con_id": 842590380,
        "local_symbol": "ZFU6",
        "exchange": "CBOT",
        "currency": "USD",
        "multiplier": "1000",
        "min_tick": "0.0078125",
        "trading_class": "ZF",
    },
    "ZN": {
        "symbol": "ZN",
        "contract_month": "202609",
        "expiry": "20260921",
        "con_id": 840227361,
        "local_symbol": "ZNU6",
        "exchange": "CBOT",
        "currency": "USD",
        "multiplier": "1000",
        "min_tick": "0.015625",
        "trading_class": "ZN",
    },
    "ZB": {
        "symbol": "ZB",
        "contract_month": "202609",
        "expiry": "20260921",
        "con_id": 840227357,
        "local_symbol": "ZBU6",
        "exchange": "CBOT",
        "currency": "USD",
        "multiplier": "1000",
        "min_tick": "0.03125",
        "trading_class": "ZB",
    },
    "BTC": {
        "symbol": "BTC",
        "broker_symbol": "BRR",
        "contract_month": "202609",
        "expiry": "20260925",
        "con_id": 772435574,
        "local_symbol": "BTCU6",
        "exchange": "CME",
        "currency": "USD",
        "multiplier": "5",
        "min_tick": "5",
        "trading_class": "BTC",
    },
    "MBT": {
        "symbol": "MBT",
        "contract_month": "202609",
        "expiry": "20260925",
        "con_id": 772435596,
        "local_symbol": "MBTU6",
        "exchange": "CME",
        "currency": "USD",
        "multiplier": "0.1",
        "min_tick": "5",
        "trading_class": "MBT",
    },
}
_GC_PHASE1_SUBMIT_LANE_IDS = (
    *_active_evidence_lane_ids("gc"),
    "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only",
    "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only_5m",
    "atp_companion_v1_gc_asia_us",
    "atp_companion_v1_gc_asia_us_5m",
    "atp_companion_v1_gc_asia_us_loosened_backfill_20260416",
    "atp_companion_v1_gc_asia_us_production_track",
    "atp_companion_v1_gc_asia_us_production_track_5m",
    "atp_companion_v1_gc_asia_us_production_track_selective_v1",
    "atp_companion_v1_gc_asia_us_selective_v1",
    "gc_1x_all_lanes__asia_early_long",
    "gc_1x_all_lanes__asia_early_short",
    "gc_1x_all_lanes__london_early_long",
    "gc_1x_all_lanes__ny_early_short",
    "gc_1x_all_lanes__ny_late_short",
    "gc_1x_all_lanes__us_early_short",
    "gc_1x_all_lanes__us_midday_short",
    "gc_1x_asia_london_participation__asia_london_long_v5",
    "gc_1x_asia_london_participation__asia_london_short_v2",
    "gc_asia_early_normal_breakout_retest_hold_long",
)
_MGC_PHASE1_SUBMIT_LANE_IDS = (
    *_active_evidence_lane_ids("mgc"),
    "track_b_paper_execution_test_mule_v1__mgc",
    "atp_companion_v1_asia_us",
    "atp_companion_v1_asia_us_5m",
    "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
    "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only_5m",
    "atp_companion_v1_mgc_asia_promotion_edge_v1",
    "mgc_1x_all_lanes__asia_early_long",
    "mgc_1x_all_lanes__asia_early_short",
    "mgc_1x_all_lanes__london_early_long",
    "mgc_1x_all_lanes__ny_early_short",
    "mgc_1x_all_lanes__us_midday_short",
    "mgc_1x_asia_london_participation__asia_london_long_v5",
    "mgc_1x_asia_london_participation__asia_london_short_v2",
    "mgc_asia_early_normal_breakout_retest_hold_long",
    "mgc_asia_early_pause_resume_short",
    "mgc_us_late_pause_resume_long",
    "ibkr_paper_route_canary",
)
_NQ_PHASE1_SUBMIT_LANE_IDS = (
    *_active_evidence_lane_ids("nq"),
    "nq_1x_asia_london_participation__asia_london_long_v5",
    "nq_1x_asia_london_participation__asia_london_long_v6",
    "nq_1x_asia_london_participation__asia_london_short_v2",
    "nq_1x_ny_early_core__us_early_long",
    "nq_1x_ny_early_core__us_early_short_breakdown",
    "nq_1x_ny_early_core__us_early_short_reclaim_fail",
    "nq_1x_ny_early_core__us_late_long",
    "nq_1x_ny_early_core__us_late_short_reclaim_fail",
    "nq_1x_ny_early_core__us_midday_long",
    "nq_1x_ny_early_core__us_midday_short_breakdown",
)
_MNQ_PHASE1_SUBMIT_LANE_IDS = (
    "track_b_paper_execution_test_mule_v1__mnq",
    "mnq_london_open_active_participation_long",
    "mnq_london_open_active_participation_short",
    "mnq_london_late_active_participation_short",
    "mnq_1x_asia_london_participation__asia_london_long_v5",
    "mnq_1x_asia_london_participation__asia_london_long_v6",
    "mnq_1x_asia_london_participation__asia_london_short_v2",
    "mnq_1x_ny_early_core__us_early_long",
    "mnq_1x_ny_early_core__us_early_short_breakdown",
    "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
    "mnq_1x_ny_early_core__us_late_long",
    "mnq_1x_ny_early_core__us_late_short_reclaim_fail",
    "mnq_1x_ny_early_core__us_midday_long",
    "mnq_1x_ny_early_core__us_midday_short_breakdown",
)
_ES_PHASE1_SUBMIT_LANE_IDS = (
    *_active_evidence_lane_ids("es"),
    "es_1x_asia_london_participation__asia_london_long_v6_vol_floor_125",
    "es_1x_ny_early_core__us_early_long",
    "es_1x_ny_early_core__us_early_short_breakdown",
    "es_1x_ny_early_core__us_early_short_reclaim_fail",
    "es_1x_ny_early_core__us_late_long",
    "es_1x_ny_early_core__us_late_short_reclaim_fail",
    "es_1x_ny_early_core__us_midday_long",
    "es_1x_ny_early_core__us_midday_short_breakdown",
)
_MES_PHASE1_SUBMIT_LANE_IDS = (
    "mes_london_open_active_participation_long",
    "mes_london_open_active_participation_short",
    "mes_london_late_active_participation_short",
    "mes_1x_ny_early_core__us_early_long",
    "mes_1x_ny_early_core__us_early_short_breakdown",
    "mes_1x_ny_early_core__us_early_short_reclaim_fail",
    "mes_1x_ny_early_core__us_late_long",
    "mes_1x_ny_early_core__us_late_short_reclaim_fail",
    "mes_1x_ny_early_core__us_midday_long",
    "mes_1x_ny_early_core__us_midday_short_breakdown",
)
_ZT_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("zt"),)
_ZF_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("zf"),)
_ZN_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("zn"),)
_ZB_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("zb"),)
_BTC_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("btc"),)
_MBT_PHASE1_SUBMIT_LANE_IDS = (*_active_evidence_lane_ids("mbt"),)
_PL_PHASE1_SUBMIT_LANE_IDS = (
    "atp_companion_v1_pl_asia_us",
    "atp_companion_v1_pl_asia_us_5m",
    "atp_companion_v1_pl_asia_us_risk_shaped_v1",
    "pl_us_late_pause_resume_long",
)
_GOLD_FORCED_SESSION_LANE_ID_MIGRATIONS: dict[str, dict[str, Any]] = {
    "mgc_1x_all_lanes__us_early_short": {
        "canonical_lane_id": "mgc_1x_all_lanes__us_early_short",
        "legacy_lane_id": "mgc_1x_all_lanes__ny_early_short",
        "strategy_family": "gold_forced_session_baseline_v2",
        "package_id": "mgc_1x_all_lanes",
        "instrument": "MGC",
        "canonical_session": "US_EARLY",
        "legacy_session": "NY_EARLY",
        "source_variant": "nyEarlyShortV2",
        "source_artifact": (
            "outputs/reports/gc_mgc_forced_session_candidate_admission_archive_v4/"
            "mgc_1x_all_lanes.paper_package.json"
        ),
        "migration_reason": (
            "Candidate archive v4 renamed the NY_EARLY forced-session lane to the current "
            "gold segment label US_EARLY while retaining the nyEarlyShortV2 signal source."
        ),
        "review_status": "EXPLICIT_LANE_ID_MIGRATION_REVIEWED",
    }
}
_NEXT_NON_ATP_SUBMIT_LANE_ID = "gc_1x_all_lanes__asia_early_long"
_FIRST_NON_ATP_SUBMIT_LANE_ID = "gc_1x_asia_london_participation__asia_london_long_v5"
_ATP_CONTRACT = {
    "symbol": "MGC",
    "contract_month": "202606",
    "expiry": "20260626",
    "con_id": 712565978,
    "local_symbol": "MGCM6",
}
_REPORT_JSON = "ibkr_paper_strategy_bridge_porting_report.json"
_REPORT_MD = "ibkr_paper_strategy_bridge_porting_report.md"
_ADAPTER_REPORT_MD = "ibkr_strategy_intent_adapter_report.md"
_INVENTORY_CSV = "ibkr_live_paper_strategy_inventory.csv"
_STATUS_CSV = "per_strategy_ibkr_paper_status.csv"
_INTENT_JSONL = "per_strategy_order_intent_examples.jsonl"
_SUBMIT_CAPABLE_LANE_ADAPTERS: dict[str, dict[str, Any]] = {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "GC",
        "bridge_execution_target": dict(phase1_execution_target_for_source("GC") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "GC_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _GC_PHASE1_SUBMIT_LANE_IDS
}
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "MGC",
        "bridge_execution_target": dict(_ATP_CONTRACT),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "MGC_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _MGC_PHASE1_SUBMIT_LANE_IDS
}
for lane_id, migration in _GOLD_FORCED_SESSION_LANE_ID_MIGRATIONS.items():
    if migration.get("instrument") != "MGC":
        continue
    legacy_adapter = _SUBMIT_CAPABLE_LANE_ADAPTERS.get(str(migration.get("legacy_lane_id") or ""))
    if legacy_adapter is None:
        continue
    _SUBMIT_CAPABLE_LANE_ADAPTERS[lane_id] = {
        **legacy_adapter,
        "lane_id": lane_id,
        "lane_id_migration": dict(migration),
        "legacy_lane_id": migration["legacy_lane_id"],
        "canonical_session": migration["canonical_session"],
        "legacy_session": migration["legacy_session"],
    }
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "NQ",
        "bridge_execution_target": dict(phase1_execution_target_for_source("NQ") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "NQ_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _NQ_PHASE1_SUBMIT_LANE_IDS
}
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "MNQ",
        "bridge_execution_target": dict(phase1_execution_target_for_source("MNQ") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "MNQ_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _MNQ_PHASE1_SUBMIT_LANE_IDS
}
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "ES",
        "bridge_execution_target": dict(phase1_execution_target_for_source("ES") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "ES_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _ES_PHASE1_SUBMIT_LANE_IDS
}
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "MES",
        "bridge_execution_target": dict(phase1_execution_target_for_source("MES") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "MES_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _MES_PHASE1_SUBMIT_LANE_IDS
}
for symbol, lane_ids in (
    ("ZT", _ZT_PHASE1_SUBMIT_LANE_IDS),
    ("ZF", _ZF_PHASE1_SUBMIT_LANE_IDS),
    ("ZN", _ZN_PHASE1_SUBMIT_LANE_IDS),
    ("ZB", _ZB_PHASE1_SUBMIT_LANE_IDS),
    ("BTC", _BTC_PHASE1_SUBMIT_LANE_IDS),
    ("MBT", _MBT_PHASE1_SUBMIT_LANE_IDS),
):
    _SUBMIT_CAPABLE_LANE_ADAPTERS |= {
        lane_id: {
            "lane_id": lane_id,
            "source_instrument": symbol,
            "bridge_execution_target": dict(phase1_execution_target_for_source(symbol) or {}),
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
            "bridge_proxy_mode": f"{symbol}_SIGNAL_DIRECT_PHASE1",
        }
        for lane_id in lane_ids
    }
_SUBMIT_CAPABLE_LANE_ADAPTERS |= {
    lane_id: {
        "lane_id": lane_id,
        "source_instrument": "PL",
        "bridge_execution_target": dict(phase1_execution_target_for_source("PL") or {}),
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "PL_SIGNAL_DIRECT_PHASE1",
    }
    for lane_id in _PL_PHASE1_SUBMIT_LANE_IDS
}

for symbol, target in _BATCH1_ACTIVE_EVIDENCE_CONTRACTS.items():
    for lane_id in _active_evidence_lane_ids(symbol):
        adapter = _SUBMIT_CAPABLE_LANE_ADAPTERS.get(lane_id)
        if adapter is not None:
            adapter.update(
                {
                    "source_instrument": symbol,
                    "bridge_execution_target": dict(target),
                    "bridge_proxy_mode": f"{symbol}_SIGNAL_DIRECT_PHASE1",
                    "validated_contract_identity_source": "noisemaker_contract_tick_readiness",
                }
            )

for lane_id in (
    *_NQ_PHASE1_SUBMIT_LANE_IDS,
    *_MNQ_PHASE1_SUBMIT_LANE_IDS,
    *_ES_PHASE1_SUBMIT_LANE_IDS,
    *_MES_PHASE1_SUBMIT_LANE_IDS,
):
    adapter = _SUBMIT_CAPABLE_LANE_ADAPTERS.get(lane_id)
    if adapter is not None and "_ny_early_core__" in lane_id:
        adapter.update(
            {
                "entry_execution_intent": "PARTICIPATE_NOW",
                "entry_working_window_seconds": 60,
                "entry_execution_note": (
                    "Index forced-session core lanes currently emit immediate participation signals; "
                    "no strategy-defined pullback/resting limit is present in the runtime intent."
                ),
            }
        )

for lane_id in (
    *(
        lane_id
        for symbol in _BATCH1_ACTIVE_EVIDENCE_CONTRACTS
        for lane_id in _active_evidence_lane_ids(symbol)
    ),
    "mnq_london_open_active_participation_long",
    "mnq_london_open_active_participation_short",
    "mes_london_open_active_participation_long",
    "mes_london_open_active_participation_short",
    "mes_london_late_active_participation_short",
    "mnq_london_late_active_participation_short",
):
    adapter = _SUBMIT_CAPABLE_LANE_ADAPTERS.get(lane_id)
    if adapter is not None:
        adapter.update(
            {
                "entry_execution_intent": "PARTICIPATE_NOW",
                "entry_execution_policy": "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE",
                "entry_marketable_limit_offset_ticks": 4,
                "entry_execution_note": (
                    "London active-evidence lanes participate from the fresh runtime tape "
                    "with the bounded ordinary PAPER marketable limit cap."
                ),
            }
        )


@dataclass(frozen=True)
class IbkrPaperStrategyPortingConfig:
    repo_root: Path
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    dashboard_snapshot_path: Path = _DEFAULT_DASHBOARD_SNAPSHOT_PATH
    signal_audit_path: Path = _DEFAULT_SIGNAL_AUDIT_PATH
    strategy_performance_path: Path = _DEFAULT_STRATEGY_PERFORMANCE_PATH
    ledger_path: Path = _DEFAULT_LEDGER_PATH
    bridge_audit_path: Path = _DEFAULT_BRIDGE_AUDIT_PATH


@dataclass(frozen=True)
class IbkrPaperStrategyPortingArtifacts:
    classification: str
    adapter_classification: str
    report: dict[str, Any]
    inventory_rows: list[dict[str, Any]]
    intent_rows: list[dict[str, Any]]
    audit_events: list[dict[str, Any]]


def lane_submit_bridge_adapter(*, lane_id: str) -> dict[str, Any] | None:
    if str(lane_id or "").strip() == "ibkr_paper_route_canary":
        canary_symbol = str(os.environ.get("PAPER_ROUTE_CANARY_SYMBOL") or "MGC").strip().upper()
        if canary_symbol == "MNQ":
            return {
                "lane_id": "ibkr_paper_route_canary",
                "source_instrument": "MNQ",
                "bridge_execution_target": dict(phase1_execution_target_for_source("MNQ") or {}),
                "current_order_destination": "ibkr_paper_bridge_submit_capable",
                "bridge_proxy_mode": "MNQ_SIGNAL_DIRECT_PHASE1",
            }
    adapter = _SUBMIT_CAPABLE_LANE_ADAPTERS.get(str(lane_id or "").strip())
    return dict(adapter) if isinstance(adapter, dict) else None


def submit_capable_lane_adapters() -> dict[str, dict[str, Any]]:
    return {lane_id: dict(adapter) for lane_id, adapter in _SUBMIT_CAPABLE_LANE_ADAPTERS.items()}


def run_ibkr_paper_strategy_porting(*, config: IbkrPaperStrategyPortingConfig) -> IbkrPaperStrategyPortingArtifacts:
    audit_events: list[dict[str, Any]] = []
    dashboard_snapshot = _load_json(config.repo_root / config.dashboard_snapshot_path)
    signal_audit = _load_json(config.repo_root / config.signal_audit_path)
    strategy_performance = _load_json(config.repo_root / config.strategy_performance_path)
    ledger = _load_json(config.repo_root / config.ledger_path)
    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    _record_audit(audit_events, "porting_started", "Started live paper strategy inventory and intent adaptation.")

    signal_rows = list(signal_audit.get("rows") or [])
    performance_rows = {str(row.get("lane_id") or ""): dict(row) for row in list(strategy_performance.get("rows") or [])}
    ledger_positions = list(ledger.get("positions") or [])
    inventory_rows: list[dict[str, Any]] = []
    intent_rows: list[dict[str, Any]] = []

    for row in signal_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        performance_row = performance_rows.get(lane_id, {})
        inventory_row = _build_inventory_row(
            signal_row=dict(row),
            performance_row=performance_row,
            ledger_positions=ledger_positions,
            monitor_status=monitor_status,
        )
        intent_row = _build_intent_row(
            inventory_row=inventory_row,
            signal_row=dict(row),
        )
        inventory_rows.append(inventory_row)
        intent_rows.append(intent_row)

    inventory_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))
    intent_rows.sort(key=lambda row: (str(row.get("instrument") or ""), str(row.get("strategy_id") or "")))
    selected_lane = _select_first_lane(intent_rows)
    summary = _build_summary(
        inventory_rows=inventory_rows,
        intent_rows=intent_rows,
        monitor_status=monitor_status,
        selected_lane=selected_lane,
    )
    _record_audit(
        audit_events,
        "porting_summary_built",
        "Built per-lane inventory rows and standardized order-intent examples.",
        extra={"selected_lane": selected_lane, "summary": summary},
    )

    report = {
        "generated_at": _utc_now(),
        "classification": summary["classification"],
        "adapter_classification": summary["adapter_classification"],
        "monitor_status": {
            key: monitor_status.get(key)
            for key in [
                "classification",
                "monitor_running",
                "health_classification",
                "stale",
                "submit_allowed",
                "broker_position_quantity",
                "ledger_position_quantity",
                "open_order_count",
                "last_successful_broker_refresh",
            ]
        },
        "selected_first_lane": selected_lane,
        "summary": summary,
        "inventory_count": len(inventory_rows),
        "inventory_rows": inventory_rows,
        "intent_rows": intent_rows,
    }
    return IbkrPaperStrategyPortingArtifacts(
        classification=str(summary["classification"]),
        adapter_classification=str(summary["adapter_classification"]),
        report=report,
        inventory_rows=inventory_rows,
        intent_rows=intent_rows,
        audit_events=audit_events,
    )


def write_ibkr_paper_strategy_porting_artifacts(
    *,
    config: IbkrPaperStrategyPortingConfig,
    artifacts: IbkrPaperStrategyPortingArtifacts,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / _REPORT_JSON).write_text(json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / _REPORT_MD).write_text(render_ibkr_paper_strategy_porting_markdown(artifacts.report) + "\n", encoding="utf-8")
    (output_dir / _ADAPTER_REPORT_MD).write_text(render_ibkr_strategy_intent_adapter_markdown(artifacts.report) + "\n", encoding="utf-8")
    _write_csv(output_dir / _INVENTORY_CSV, artifacts.inventory_rows)
    _write_csv(output_dir / _STATUS_CSV, [_status_row(row) for row in artifacts.inventory_rows])
    with (output_dir / _INTENT_JSONL).open("w", encoding="utf-8") as handle:
        for row in artifacts.intent_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    audit_path = config.repo_root / config.bridge_audit_path
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("w", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def render_ibkr_paper_strategy_porting_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    selected = dict(report.get("selected_first_lane") or {})
    monitor = dict(report.get("monitor_status") or {})
    remaining = list(summary.get("remaining_supported_unported") or [])
    lines = [
        "# IBKR Paper Strategy Porting",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- adapter classification: `{report.get('adapter_classification')}`",
        f"- live paper strategy rows inventoried: `{report.get('inventory_count')}`",
        f"- monitor classification: `{monitor.get('classification')}`",
        f"- monitor health: `{monitor.get('health_classification')}`",
        f"- monitor stale: `{monitor.get('stale')}`",
        f"- current MGC broker quantity: `{monitor.get('broker_position_quantity')}`",
        f"- current MGC open orders: `{monitor.get('open_order_count')}`",
        f"- executable instruments now: `{', '.join(summary.get('supported_instruments_now') or [])}`",
        f"- submit-capable supported lanes: `{summary.get('submit_capable_supported_lane_count')}`",
        f"- remaining supported unported lanes: `{summary.get('remaining_supported_unported_count')}`",
        f"- first selected lane: `{selected.get('strategy_id')}`",
        f"- selected lane action: `{selected.get('intent_action')}`",
        f"- selected lane bridge submit capable: `{selected.get('bridge_submit_capable')}`",
        f"- selected lane dry-run result: `{selected.get('dry_run_result')}`",
        f"- selected lane blocker: `{selected.get('primary_blocker')}`",
        f"- note: `{summary.get('note')}`",
    ]
    if remaining:
        lines.extend(["", "## Remaining Supported Unported Lanes", ""])
        for row in remaining:
            lines.append(
                f"- `{row.get('strategy_id')}` / `{row.get('instrument')}`: governance=`{row.get('governance_status')}` blockers=`{', '.join(list(row.get('blockers') or [])) or 'none'}`"
            )
    return "\n".join(lines)


def render_ibkr_strategy_intent_adapter_markdown(report: dict[str, Any]) -> str:
    selected = dict(report.get("selected_first_lane") or {})
    lines = [
        "# IBKR Strategy Intent Adapters",
        "",
        "- intent adapters now read current live paper runtime state and emit standardized paper-only intents lane-by-lane.",
        "- adapters do not change strategy logic; they only translate current lane state into `BUY / SELL / HOLD / EXIT / NO_ACTION`-style intents.",
        "- current port is partial because only the ATP lane is already on the IBKR paper broker path, while other live app strategies are still dry-run intent adapters.",
        f"- first selected lane: `{selected.get('strategy_id')}` / `{selected.get('instrument')}`",
        f"- first selected lane action: `{selected.get('intent_action')}`",
        f"- first selected lane reason: `{selected.get('intent_reason')}`",
        f"- first selected lane routing readiness: `{selected.get('can_route_to_ibkr_now')}`",
        f"- first selected lane bridge submit capable: `{selected.get('bridge_submit_capable')}`",
    ]
    return "\n".join(lines)


def _build_inventory_row(
    *,
    signal_row: dict[str, Any],
    performance_row: dict[str, Any],
    ledger_positions: list[dict[str, Any]],
    monitor_status: dict[str, Any],
) -> dict[str, Any]:
    lane_id = str(signal_row.get("lane_id") or "").strip()
    instrument = str(signal_row.get("instrument") or performance_row.get("instrument") or "").strip().upper()
    strategy_key = str(signal_row.get("id") or performance_row.get("standalone_strategy_id") or lane_id).strip()
    ledger_match = _matching_ledger_position(ledger_positions=ledger_positions, lane_id=lane_id, instrument=instrument)
    current_position_state = "FLAT"
    current_quantity = 0.0
    if ledger_match is not None:
        current_position_state = str(ledger_match.get("side") or "UNKNOWN").upper()
        current_quantity = float(ledger_match.get("quantity") or 0.0)
    else:
        current_position_state = str(performance_row.get("position_side") or "FLAT").upper()
        current_quantity = 1.0 if current_position_state == "LONG" else 0.0
    signal_state = _signal_state(signal_row)
    bridge_adapter = lane_submit_bridge_adapter(lane_id=lane_id)
    destination = "legacy_app_paper_runtime"
    if lane_id == _ATP_LANE_ID or strategy_key == _ATP_STRATEGY_ID:
        destination = "ibkr_paper_bridge_adopted_position"
    elif bridge_adapter is not None:
        destination = str(bridge_adapter.get("current_order_destination") or "ibkr_paper_bridge_submit_capable")
    blocker_rows = _lane_blockers(
        lane_id=lane_id,
        instrument=instrument,
        current_position_state=current_position_state,
        current_quantity=current_quantity,
        signal_state=signal_state,
        monitor_status=monitor_status,
        destination=destination,
    )
    can_emit_now = len([b for b in blocker_rows if b.startswith("unknown_")]) == 0
    return {
        "strategy_id": lane_id or strategy_key,
        "standalone_strategy_id": strategy_key,
        "instrument": instrument,
        "strategy_family": signal_row.get("family") or performance_row.get("strategy_family"),
        "current_app_runtime_status": signal_row.get("current_strategy_status") or performance_row.get("status") or "UNKNOWN",
        "current_position_state": current_position_state,
        "current_quantity": current_quantity,
        "current_signal_state": signal_state,
        "entry_exit_capability": _entry_exit_capability(current_position_state=current_position_state, instrument=instrument),
        "current_order_destination": destination,
        "bridge_adapter_ready": bridge_adapter is not None,
        "bridge_execution_target": dict(bridge_adapter.get("bridge_execution_target") or {}) if bridge_adapter is not None else {},
        "bridge_proxy_mode": bridge_adapter.get("bridge_proxy_mode") if bridge_adapter is not None else None,
        "can_emit_standardized_order_intent_now": can_emit_now,
        "blockers_to_ibkr_paper_routing": blocker_rows,
        "entries_enabled": bool(signal_row.get("entries_enabled")),
        "eligible_now": bool(signal_row.get("eligible_now")),
        "last_signal_family": signal_row.get("last_actionable_signal_family"),
        "last_signal_timestamp": signal_row.get("last_actionable_signal_timestamp"),
        "last_fill_timestamp": signal_row.get("last_fill_timestamp"),
        "audit_verdict": signal_row.get("audit_verdict"),
        "monitor_submit_allowed": bool(monitor_status.get("submit_allowed")),
    }


def _build_intent_row(*, inventory_row: dict[str, Any], signal_row: dict[str, Any]) -> dict[str, Any]:
    strategy_id = str(inventory_row.get("strategy_id") or "")
    instrument = str(inventory_row.get("instrument") or "")
    current_position_state = str(inventory_row.get("current_position_state") or "UNKNOWN").upper()
    signal_state = str(inventory_row.get("current_signal_state") or "UNKNOWN")
    blockers = list(inventory_row.get("blockers_to_ibkr_paper_routing") or [])
    action = "NO_ACTION"
    reason = "No actionable entry or exit signal is active in the current live paper runtime state."
    quantity = 0.0
    limit_model: str | None = None
    order_type = "LMT"
    tif = "DAY"
    if blockers and any(blocker.startswith("monitor_") or blocker.startswith("broker_ledger_") for blocker in blockers):
        action = "BLOCKED_NEEDS_REVIEW"
        reason = "Live paper monitor or broker/ledger state blocks safe IBKR paper routing."
    elif current_position_state == "LONG":
        quantity = float(inventory_row.get("current_quantity") or 0.0)
        if signal_state == "EXIT_LONG":
            action = "EXIT"
            reason = "An authoritative strategy exit signal is present for the currently owned strategy position."
            limit_model = "DELAYED_BID_MINUS_1T_MARKETABLE_SELL"
        else:
            action = "HOLD"
            reason = "The strategy owns a current paper position and no authoritative exit signal is active."
    elif current_position_state == "FLAT":
        if signal_state == "ENTRY_BUY":
            action = "BUY"
            quantity = 1.0
            reason = "The live paper runtime marks this lane as eligible now with a current long entry setup."
            limit_model = "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY"
        else:
            action = "NO_ACTION"
            reason = "The lane is flat and no current entry setup is active."
    else:
        action = "BLOCKED_NEEDS_REVIEW"
        reason = "Strategy state is unknown or unsupported for IBKR paper routing."
    contract_target = {
        "symbol": instrument,
        "friendly_label": f"{instrument} 202606",
        "exact_qualified_supported": instrument in _SUPPORTED_EXECUTABLE_INSTRUMENTS,
    }
    if instrument == "MGC":
        contract_target |= dict(_ATP_CONTRACT)
    elif instrument == "GC":
        contract_target |= {
            "symbol": "GC",
            "contract_month": "202606",
        }
    elif instrument in {"MNQ", "NQ", "MES", "ES"}:
        contract_target |= {
            "symbol": instrument,
            "contract_month": dict(inventory_row.get("bridge_execution_target") or {}).get("contract_month"),
        }
    bridge_execution_target = dict(inventory_row.get("bridge_execution_target") or {})
    return {
        "strategy_id": strategy_id,
        "standalone_strategy_id": inventory_row.get("standalone_strategy_id"),
        "action": action,
        "contract_target": contract_target,
        "quantity": quantity,
        "order_type": order_type,
        "time_in_force": tif,
        "limit_price_model": limit_model,
        "reason": reason,
        "signal_id": signal_row.get("last_actionable_signal_family") or signal_row.get("last_signal_family"),
        "timestamp": _utc_now(),
        "current_strategy_state": {
            "runtime_status": inventory_row.get("current_app_runtime_status"),
            "position_state": inventory_row.get("current_position_state"),
            "signal_state": inventory_row.get("current_signal_state"),
            "entries_enabled": inventory_row.get("entries_enabled"),
            "eligible_now": inventory_row.get("eligible_now"),
        },
        "paper_only": True,
        "can_route_to_ibkr_now": _can_route_now(action=action, instrument=instrument, blockers=blockers),
        "bridge_submit_capable": bool(inventory_row.get("bridge_adapter_ready")),
        "bridge_execution_target": bridge_execution_target,
        "route_blockers": blockers,
    }


def _build_summary(
    *,
    inventory_rows: list[dict[str, Any]],
    intent_rows: list[dict[str, Any]],
    monitor_status: dict[str, Any],
    selected_lane: dict[str, Any],
) -> dict[str, Any]:
    supported_now = sorted({str(row.get("instrument") or "") for row in inventory_rows if str(row.get("instrument") or "") in _SUPPORTED_EXECUTABLE_INSTRUMENTS})
    governance_rows = {str(row.get("strategy_id") or ""): dict(row) for row in _load_governance_rows()}
    submit_capable_supported = [
        row for row in inventory_rows
        if str(row.get("instrument") or "") in _SUPPORTED_EXECUTABLE_INSTRUMENTS
        and bool(row.get("bridge_adapter_ready"))
    ]
    remaining_supported_unported = []
    for row in inventory_rows:
        instrument = str(row.get("instrument") or "")
        if instrument not in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
            continue
        strategy_id = str(row.get("strategy_id") or "")
        governance_row = governance_rows.get(strategy_id, {})
        governance_status = str(governance_row.get("strategy_status") or "")
        destination = str(row.get("current_order_destination") or "")
        if destination in {"ibkr_paper_bridge_submit_capable", "ibkr_paper_bridge_adopted_position"}:
            continue
        remaining_supported_unported.append(
            {
                "strategy_id": strategy_id,
                "instrument": instrument,
                "governance_status": governance_status or None,
                "blockers": list(row.get("blockers_to_ibkr_paper_routing") or []),
            }
        )
    route_ready = [row for row in intent_rows if row.get("can_route_to_ibkr_now")]
    overall = "IBKR_PAPER_STRATEGY_PORT_PARTIAL"
    if route_ready and bool(monitor_status.get("submit_allowed")):
        overall = "IBKR_PAPER_STRATEGY_PORT_READY"
    elif not inventory_rows:
        overall = "IBKR_PAPER_STRATEGY_PORT_BLOCKED"
    adapter = "STRATEGY_INTENT_ADAPTER_READY" if inventory_rows else "STRATEGY_INTENT_ADAPTER_BLOCKED"
    note = "Current live paper strategies are inventoried and standardized intents are emitted lane-by-lane. Submit-capable IBKR routing remains partial because only the ATP lane is already broker-proven, while the first non-ATP GC lane is now bridge-capable but still emits NO_ACTION in the live runtime."
    return {
        "classification": overall,
        "adapter_classification": adapter,
        "supported_instruments_now": supported_now,
        "route_ready_count": len(route_ready),
        "submit_capable_supported_lane_count": len(submit_capable_supported),
        "remaining_supported_unported_count": len(remaining_supported_unported),
        "remaining_supported_unported": remaining_supported_unported,
        "selected_lane_id": selected_lane.get("strategy_id"),
        "selected_lane_action": selected_lane.get("intent_action"),
        "note": note,
    }


def _select_first_lane(intent_rows: list[dict[str, Any]]) -> dict[str, Any]:
    preferred = [row for row in intent_rows if bool(row.get("bridge_submit_capable"))]
    if not preferred:
        preferred = [
            row
            for row in intent_rows
            if str(dict(row.get("contract_target") or {}).get("symbol") or "") == _INITIAL_EXECUTABLE_INSTRUMENT
        ]
    row = preferred[0] if preferred else (intent_rows[0] if intent_rows else {})
    return {
        "strategy_id": row.get("strategy_id"),
        "instrument": dict(row.get("contract_target") or {}).get("symbol"),
        "intent_action": row.get("action"),
        "intent_reason": row.get("reason"),
        "can_route_to_ibkr_now": row.get("can_route_to_ibkr_now"),
        "bridge_submit_capable": row.get("bridge_submit_capable"),
        "bridge_execution_target": row.get("bridge_execution_target"),
        "primary_blocker": (list(row.get("route_blockers") or []) or [None])[0],
        "dry_run_result": "NO_ORDER" if str(row.get("action") or "") in {"HOLD", "NO_ACTION", "BLOCKED_NEEDS_REVIEW"} else "SUBMIT_ELIGIBLE",
    }


def _signal_state(signal_row: dict[str, Any]) -> str:
    if bool(signal_row.get("eligible_now")) and bool(signal_row.get("entries_enabled")) and bool(signal_row.get("last_recent_long_setup")):
        return "ENTRY_BUY"
    if bool(signal_row.get("eligible_now")) and bool(signal_row.get("entries_enabled")) and bool(signal_row.get("last_recent_short_setup")):
        return "ENTRY_SELL"
    if str(signal_row.get("last_intent_type") or "").strip().upper() in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}:
        return "EXIT_RECENTLY_FILLED"
    return "NO_ACTION"


def _matching_ledger_position(*, ledger_positions: list[dict[str, Any]], lane_id: str, instrument: str) -> dict[str, Any] | None:
    if lane_id == _ATP_LANE_ID and instrument == "MGC":
        for row in ledger_positions:
            if str(row.get("strategy_id") or "") == _ATP_STRATEGY_ID and str(row.get("symbol") or "").upper() == "MGC":
                return dict(row)
    return None


def _entry_exit_capability(*, current_position_state: str, instrument: str) -> str:
    if instrument in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        if current_position_state == "LONG":
            return "EXIT_ONLY_WHILE_LONG"
        return "ENTRY_SUBMIT_CAPABLE_PHASE1_DIRECT"
    return "UNSUPPORTED_INSTRUMENT"


def _lane_blockers(
    *,
    lane_id: str,
    instrument: str,
    current_position_state: str,
    current_quantity: float,
    signal_state: str,
    monitor_status: dict[str, Any],
    destination: str,
) -> list[str]:
    blockers: list[str] = []
    if not bool(monitor_status.get("monitor_running")):
        blockers.append("monitor_not_running")
    if bool(monitor_status.get("stale")):
        blockers.append("monitor_stale")
    if str(monitor_status.get("health_classification") or "").upper() != "HEALTHY":
        blockers.append("monitor_not_healthy")
    if int(monitor_status.get("open_order_count") or 0) != 0:
        blockers.append("open_order_present")
    if current_position_state not in {"FLAT", "LONG"}:
        blockers.append("unknown_strategy_state")
    if current_position_state == "LONG" and current_quantity <= 0.0:
        blockers.append("broker_ledger_mismatch")
    if instrument not in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        blockers.append("unsupported_instrument_scope")
    if destination not in {"ibkr_paper_bridge_adopted_position", "ibkr_paper_bridge_submit_capable"} and instrument in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        blockers.append("lane_not_yet_submit_ported")
    if lane_id != _ATP_LANE_ID and signal_state in {"ENTRY_BUY", "ENTRY_SELL", "EXIT_LONG"} and destination != "ibkr_paper_bridge_submit_capable":
        blockers.append("strategy_lane_not_yet_submit_ported")
    return blockers


def _can_route_now(*, action: str, instrument: str, blockers: list[str]) -> bool:
    if action in {"NO_ACTION", "HOLD", "BLOCKED_NEEDS_REVIEW"}:
        return False
    if instrument not in _SUPPORTED_EXECUTABLE_INSTRUMENTS:
        return False
    return len(blockers) == 0


def _status_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": row.get("strategy_id"),
        "instrument": row.get("instrument"),
        "runtime_status": row.get("current_app_runtime_status"),
        "position_state": row.get("current_position_state"),
        "signal_state": row.get("current_signal_state"),
        "order_destination": row.get("current_order_destination"),
        "bridge_submit_capable": row.get("bridge_adapter_ready"),
        "intent_adapter_ready": row.get("can_emit_standardized_order_intent_now"),
        "blockers": ";".join(list(row.get("blockers_to_ibkr_paper_routing") or [])),
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _record_audit(audit_events: list[dict[str, Any]], event_type: str, detail: str, extra: dict[str, Any] | None = None) -> None:
    audit_events.append({"event_type": event_type, "observed_at": _utc_now(), "detail": detail, **dict(extra or {})})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_governance_rows() -> list[dict[str, Any]]:
    payload = _load_json(Path("var") / "per_strategy_paper_status.json")
    return list(payload.get("strategies") or [])


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
