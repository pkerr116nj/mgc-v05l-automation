"""Paper-only runtime bridge from research-engine truth into operator-facing artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import timedelta
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from time import sleep
from time import perf_counter
from typing import Any, Iterable, Sequence

from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, ShortEntryFamily
from ..execution.execution_engine import ExecutionEngine, PendingExecution
from ..execution.order_models import OrderIntent
from ..execution.paper_broker import PaperBroker
from ..execution.reconciliation import (
    BrokerReconciliationSnapshot,
    InternalReconciliationSnapshot,
    RECONCILIATION_CLASS_CLEAN,
    ReconciliationCoordinator,
)
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..research.warehouse_historical_evaluator._warehouse_common import read_parquet_rows

REPO_ROOT = Path.cwd().resolve()
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "research_runtime_bridge" / "default_warehouse_paper"
BRIDGE_CONTRACT_VERSION = "research_runtime_bridge_v1"
BRIDGE_MODE_REPLAY = "REPLAY_CLOSED_TRADES"
BRIDGE_MODE_PROSPECTIVE = "PROSPECTIVE_PAPER_RUNTIME"
DEFAULT_BRIDGE_MODE = BRIDGE_MODE_PROSPECTIVE
BRIDGE_EXECUTION_MODE = "PAPER_DRY_RUN_ONLY"
BRIDGE_OPERATING_POLICY_VERSION = "research_runtime_bridge_policy_v1"
BRIDGE_RUNTIME_EVENT_CONTRACT_VERSION = "research_runtime_bridge_runtime_events_v1"
PROSPECTIVE_CYCLE_POLICY_VERSION = "prospective_cycle_policy_v1"
BRIDGE_CADENCE_CONTRACT_VERSION = "research_runtime_bridge_cadence_v1"
BRIDGE_OPERATOR_REVIEW_CONTRACT_VERSION = "research_runtime_bridge_review_v1"
BRIDGE_ANOMALY_QUEUE_CONTRACT_VERSION = "research_runtime_bridge_anomaly_queue_v1"
PROSPECTIVE_STALE_AFTER_CYCLES = 2
PROSPECTIVE_EXPIRE_AFTER_CYCLES = 3
PROSPECTIVE_MIN_FILL_DELAY_CYCLES = 1
PROSPECTIVE_UNRESOLVED_ESCALATE_AFTER_CYCLES = PROSPECTIVE_EXPIRE_AFTER_CYCLES
DEFAULT_PROSPECTIVE_POLL_INTERVAL_SECONDS = 30
DEFAULT_SELECTED_LANES = (
    "gc_asia_early_normal_breakout_retest_hold_turn__GC",
    "mgc_asia_early_pause_resume_short_turn__MGC",
)
DEFAULT_LANE_LABELS = {
    "gc_asia_early_normal_breakout_retest_hold_turn__GC": "Warehouse GC Asia Breakout Retest Hold Turn",
    "mgc_asia_early_pause_resume_short_turn__MGC": "Warehouse MGC Asia Pause Resume Short Turn",
}


def _prospective_cycle_policy(*, entries_enabled: bool, exits_enabled: bool, operator_halt: bool) -> dict[str, Any]:
    return {
        "policy_version": PROSPECTIVE_CYCLE_POLICY_VERSION,
        "mode": "manual_poll_and_advance",
        "poll_source": "warehouse_current_state",
        "evaluation_order": [
            "load_latest_lane_entries",
            "restore_pending_and_open_runtime_state",
            "emit_new_entry_intents",
            "advance_pending_entries_after_min_delay",
            "emit_exit_intents_for_open_positions",
            "advance_pending_exits_after_min_delay",
            "flag_stale_pending_intents",
            "expire_pending_intents_after_threshold",
            "persist_reconciliation_and_operator_snapshot",
        ],
        "entries_enabled": entries_enabled,
        "exits_enabled": exits_enabled,
        "operator_halt": operator_halt,
        "stale_after_cycles": PROSPECTIVE_STALE_AFTER_CYCLES,
        "expire_after_cycles": PROSPECTIVE_EXPIRE_AFTER_CYCLES,
        "min_fill_delay_cycles": PROSPECTIVE_MIN_FILL_DELAY_CYCLES,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="research-runtime-bridge")
    parser.add_argument(
        "--warehouse-root",
        default=str(DEFAULT_WAREHOUSE_ROOT),
        help="Warehouse historical evaluator root containing lane_closed_trades partitions.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Output directory for runtime bridge artifacts.",
    )
    parser.add_argument(
        "--lane-id",
        action="append",
        default=None,
        help="Warehouse lane id to bridge. Repeat to select multiple lanes. Defaults to the first paper-runtime bridge lanes.",
    )
    parser.add_argument(
        "--mode",
        choices=("replay", "prospective"),
        default="prospective",
        help="Bridge mode. replay replays warehouse closed trades, prospective emits paper intents from current warehouse entry state.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset previously persisted runtime bridge artifacts before running.",
    )
    parser.add_argument(
        "--entries-enabled",
        choices=("true", "false"),
        default="true",
        help="Whether new entry emission and pending entry advancement are enabled in prospective mode.",
    )
    parser.add_argument(
        "--exits-enabled",
        choices=("true", "false"),
        default="true",
        help="Whether exit intent emission and pending exit advancement are enabled in prospective mode.",
    )
    parser.add_argument(
        "--operator-halt",
        choices=("true", "false"),
        default="false",
        help="Whether the bridge is operator-halted in prospective mode.",
    )
    parser.add_argument(
        "--cycle-count",
        type=int,
        default=1,
        help="How many prospective bridge cycles to run in this invocation. Replay mode always uses one cycle.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=DEFAULT_PROSPECTIVE_POLL_INTERVAL_SECONDS,
        help="How long the local prospective runner waits between cycles when cycle-count is greater than one.",
    )
    parser.add_argument(
        "--stop-when-settled",
        action="store_true",
        help="Stop a multi-cycle prospective run early once pending/open/alert state settles.",
    )
    return parser


def run_bridge(
    *,
    warehouse_root: Path,
    output_dir: Path,
    selected_lane_ids: Sequence[str] | None = None,
    mode: str = DEFAULT_BRIDGE_MODE,
    reset_state: bool = False,
    entries_enabled: bool = True,
    exits_enabled: bool = True,
    operator_halt: bool = False,
    cycle_count: int = 1,
    poll_interval_seconds: int = DEFAULT_PROSPECTIVE_POLL_INTERVAL_SECONDS,
    stop_when_settled: bool = False,
) -> dict[str, Any]:
    if os.getenv("MGC_ENABLE_LIVE_EXECUTION") == "1":
        raise RuntimeError("research runtime bridge is paper-only and refuses live-execution mode.")
    warehouse_root = warehouse_root.resolve()
    output_dir = output_dir.resolve()
    if not warehouse_root.exists():
        raise FileNotFoundError(f"Warehouse root not found: {warehouse_root}")
    lane_ids = tuple(dict.fromkeys(str(item).strip() for item in (selected_lane_ids or DEFAULT_SELECTED_LANES) if str(item).strip()))
    if not lane_ids:
        raise ValueError("At least one lane id is required for runtime bridging.")
    normalized_mode = str(mode or DEFAULT_BRIDGE_MODE).strip().upper()
    if normalized_mode not in {BRIDGE_MODE_REPLAY, BRIDGE_MODE_PROSPECTIVE}:
        raise ValueError(f"Unsupported research runtime bridge mode: {mode!r}.")

    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "runtime_bridge.sqlite3"
    if normalized_mode == BRIDGE_MODE_REPLAY:
        started = perf_counter()
        if db_path.exists():
            db_path.unlink()
        engine = build_engine(f"sqlite:///{db_path}")
        return _run_replay_bridge(
            warehouse_root=warehouse_root,
            output_dir=output_dir,
            lane_ids=lane_ids,
            engine=engine,
            db_path=db_path,
            started=started,
        )
    normalized_cycle_count = max(1, int(cycle_count or 1))
    normalized_poll_interval_seconds = max(0, int(poll_interval_seconds or 0))
    final_result: dict[str, Any] | None = None
    for cycle_ordinal in range(1, normalized_cycle_count + 1):
        started = perf_counter()
        if db_path.exists():
            db_path.unlink()
        engine = build_engine(f"sqlite:///{db_path}")
        final_result = _run_prospective_bridge(
            warehouse_root=warehouse_root,
            output_dir=output_dir,
            lane_ids=lane_ids,
            engine=engine,
            db_path=db_path,
            started=started,
            reset_state=reset_state and cycle_ordinal == 1,
            entries_enabled=entries_enabled,
            exits_enabled=exits_enabled,
            operator_halt=operator_halt,
            requested_cycle_count=normalized_cycle_count,
            cycle_ordinal=cycle_ordinal,
            poll_interval_seconds=normalized_poll_interval_seconds,
            stop_when_settled=stop_when_settled,
        )
        if cycle_ordinal >= normalized_cycle_count:
            break
        if stop_when_settled and _bridge_run_is_settled(final_result):
            break
        if normalized_poll_interval_seconds > 0:
            sleep(normalized_poll_interval_seconds)
    if final_result is None:
        raise RuntimeError("research runtime bridge did not produce a result.")
    return final_result


def _run_replay_bridge(
    *,
    warehouse_root: Path,
    output_dir: Path,
    lane_ids: Sequence[str],
    engine,
    db_path: Path,
    started: float,
) -> dict[str, Any]:
    trades_started = perf_counter()
    selected_trades = _load_selected_closed_trades(warehouse_root=warehouse_root, lane_ids=lane_ids)
    trade_load_seconds = perf_counter() - trades_started

    bridge_run_id = _stable_hash(
        {
            "bridge_contract_version": BRIDGE_CONTRACT_VERSION,
            "mode": BRIDGE_MODE_REPLAY,
            "warehouse_root": str(warehouse_root),
            "lane_ids": list(lane_ids),
            "trade_count": len(selected_trades),
            "trade_ids": [row["trade_id"] for row in selected_trades],
        },
        length=24,
    )
    generated_at = datetime.now(UTC)

    intents_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    closed_position_rows: list[dict[str, Any]] = []
    open_position_rows: list[dict[str, Any]] = []
    alert_rows: list[dict[str, Any]] = []
    reconciliation_rows: list[dict[str, Any]] = []
    runtime_event_rows: list[dict[str, Any]] = []
    lane_rows: list[dict[str, Any]] = []

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in selected_trades:
        grouped.setdefault(str(row["lane_id"]), []).append(row)

    bridge_started = perf_counter()
    for lane_id in lane_ids:
        runtime_identity = _runtime_identity_for_lane(lane_id)
        lane_trade_rows = grouped.get(lane_id, [])
        lane_result = _bridge_lane(
            bridge_run_id=bridge_run_id,
            generated_at=generated_at,
            engine=engine,
            runtime_identity=runtime_identity,
            lane_id=lane_id,
            lane_label=DEFAULT_LANE_LABELS.get(lane_id, lane_id),
            trade_rows=lane_trade_rows,
        )
        lane_rows.append(lane_result["lane_row"])
        intents_rows.extend(lane_result["intents"])
        fill_rows.extend(lane_result["fills"])
        closed_position_rows.extend(lane_result["closed_positions"])
        open_position_rows.extend(lane_result["open_positions"])
        alert_rows.extend(lane_result["alerts"])
        reconciliation_rows.extend(lane_result["reconciliation_events"])
    bridge_seconds = perf_counter() - bridge_started

    summary = _build_summary(
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        lane_ids=lane_ids,
        lane_rows=lane_rows,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        open_position_rows=open_position_rows,
        pending_rows=[],
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        bridge_mode=BRIDGE_MODE_REPLAY,
        pending_intent_count=0,
        runtime_event_rows=runtime_event_rows,
        cycle_index=1,
    )
    operator_status = _build_operator_status(summary=summary, bridge_mode=BRIDGE_MODE_REPLAY)
    runtime_state = {
        "generated_at": generated_at.isoformat(),
        "bridge_run_id": bridge_run_id,
        "bridge_cycle_index": 1,
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "live_execution_enabled": False,
        "lane_rows": lane_rows,
        "pending_intents": [],
        "open_positions": open_position_rows,
        "closed_positions": closed_position_rows,
        "cadence_state": {},
        "runtime_events": runtime_event_rows,
        "review_events": [],
        "operator_status": operator_status,
        "summary": summary,
    }
    snapshot = _build_snapshot(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        bridge_mode=BRIDGE_MODE_REPLAY,
        selected_tenants=[
            {
                "strategy_family": "warehouse_historical_evaluator",
                "family_label": "Warehouse Historical Evaluator",
                "lane_count": len(lane_ids),
                "lane_ids": list(lane_ids),
            }
        ],
        selected_exclusions=[
            "ATP companion remains historical-only in this tranche.",
            "Approved quant remains deferred until a new family hypothesis exists.",
            "No live broker execution path is enabled by this bridge.",
        ],
        summary=summary,
        operator_status=operator_status,
        lane_rows=lane_rows,
        pending_intents=[],
        recent_intents=intents_rows[:25],
        recent_fills=fill_rows[:25],
        recent_closed_positions=closed_position_rows[:25],
        open_positions=open_position_rows,
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        source_truth={
            "warehouse_root": str(warehouse_root),
            "source_dataset": "lane_closed_trades",
            "selected_lane_ids": list(lane_ids),
            "selected_trade_count": len(selected_trades),
            "bridge_source_class": "replay_closed_trade_truth",
        },
        cycle_policy=None,
        cadence_state={},
        review_event_rows=[],
        timing={
            "trade_load_seconds": round(trade_load_seconds, 6),
            "bridge_seconds": round(bridge_seconds, 6),
            "total_seconds": round(perf_counter() - started, 6),
        },
        cycle_index=1,
    )
    _write_runtime_bridge_outputs(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        bridge_mode=BRIDGE_MODE_REPLAY,
        warehouse_root=warehouse_root,
        lane_ids=lane_ids,
        summary=summary,
        operator_status=operator_status,
        runtime_state=runtime_state,
        snapshot=snapshot,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        review_event_rows=[],
        cadence_state={},
        db_path=db_path,
    )
    return {
        "output_dir": str(output_dir),
        "snapshot_path": str((output_dir / "bridge_snapshot.json").resolve()),
        "operator_status_path": str((output_dir / "operator_status.json").resolve()),
        "runtime_state_path": str((output_dir / "runtime_state.json").resolve()),
        "db_path": str(db_path.resolve()),
        "summary": summary,
        "timing": snapshot["timing"],
    }


def _run_prospective_bridge(
    *,
    warehouse_root: Path,
    output_dir: Path,
    lane_ids: Sequence[str],
    engine,
    db_path: Path,
    started: float,
    reset_state: bool,
    entries_enabled: bool,
    exits_enabled: bool,
    operator_halt: bool,
    requested_cycle_count: int,
    cycle_ordinal: int,
    poll_interval_seconds: int,
    stop_when_settled: bool,
) -> dict[str, Any]:
    previous_state = {} if reset_state else _load_existing_bridge_state(output_dir)
    previous_snapshot = dict(previous_state.get("snapshot") or {})
    if str(previous_snapshot.get("bridge_mode") or "") not in {"", BRIDGE_MODE_PROSPECTIVE}:
        previous_state = {}
    previous_cadence_state = dict(previous_state.get("cadence_state") or {})
    recovered_interrupted_cycle = str(previous_cadence_state.get("cycle_state") or "") == "IN_PROGRESS"

    entries_started = perf_counter()
    selected_entries = _load_selected_lane_entries(warehouse_root=warehouse_root, lane_ids=lane_ids)
    selected_closed_trades = _load_selected_closed_trades(warehouse_root=warehouse_root, lane_ids=lane_ids)
    trade_load_seconds = perf_counter() - entries_started

    latest_entries_by_lane = _latest_entry_rows_by_lane(selected_entries, lane_ids=lane_ids)
    closed_trade_by_entry_id = {
        str(row.get("entry_id") or ""): row
        for row in selected_closed_trades
        if str(row.get("entry_id") or "")
    }

    bridge_cycle_index = int((previous_state.get("runtime_state") or {}).get("bridge_cycle_index") or 0) + 1
    bridge_run_id = _stable_hash(
        {
            "bridge_contract_version": BRIDGE_CONTRACT_VERSION,
            "mode": BRIDGE_MODE_PROSPECTIVE,
            "warehouse_root": str(warehouse_root),
            "lane_ids": list(lane_ids),
            "entry_ids": [row.get("entry_id") for row in latest_entries_by_lane.values()],
            "bridge_cycle_index": bridge_cycle_index,
        },
        length=24,
    )
    generated_at = datetime.now(UTC)
    cycle_policy = _prospective_cycle_policy(
        entries_enabled=entries_enabled,
        exits_enabled=exits_enabled,
        operator_halt=operator_halt,
    )
    cadence_state = _build_cadence_state(
        previous_cadence_state=previous_cadence_state,
        bridge_cycle_index=bridge_cycle_index,
        cycle_ordinal=cycle_ordinal,
        requested_cycle_count=requested_cycle_count,
        poll_interval_seconds=poll_interval_seconds,
        stop_when_settled=stop_when_settled,
        generated_at=generated_at,
        entries_enabled=entries_enabled,
        exits_enabled=exits_enabled,
        operator_halt=operator_halt,
        cycle_state="IN_PROGRESS",
        summary=None,
        blocked_reason=None,
        recovered_interrupted_cycle=recovered_interrupted_cycle,
    )
    _write_json(output_dir / "cadence_state.json", cadence_state)

    intents_by_id = {str(row.get("order_intent_id")): dict(row) for row in previous_state.get("intents_rows") or []}
    fills_by_intent_id = {str(row.get("order_intent_id")): dict(row) for row in previous_state.get("fill_rows") or []}
    closed_positions_by_trade_id = {str(row.get("trade_id")): dict(row) for row in previous_state.get("closed_position_rows") or []}
    alert_rows_by_key = {str(row.get("dedup_key") or _stable_hash(row)): dict(row) for row in previous_state.get("alert_rows") or []}
    reconciliation_rows = list(previous_state.get("reconciliation_rows") or [])
    runtime_events_by_id = {str(row.get("event_id") or _stable_hash(row)): dict(row) for row in previous_state.get("runtime_event_rows") or []}
    previous_runtime_state = dict(previous_state.get("runtime_state") or {})
    previous_pending_rows = list(previous_runtime_state.get("pending_intents") or [])
    previous_open_positions = list(previous_runtime_state.get("open_positions") or [])

    bridge_started = perf_counter()
    lane_rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    open_position_rows: list[dict[str, Any]] = []

    for lane_id in lane_ids:
        runtime_identity = _runtime_identity_for_lane(lane_id)
        lane_result = _bridge_lane_prospective(
            bridge_run_id=bridge_run_id,
            bridge_cycle_index=bridge_cycle_index,
            generated_at=generated_at,
            engine=engine,
            runtime_identity=runtime_identity,
            lane_id=lane_id,
            lane_label=DEFAULT_LANE_LABELS.get(lane_id, lane_id),
            latest_entry_row=latest_entries_by_lane.get(lane_id),
            matching_closed_trade=closed_trade_by_entry_id.get(
                str((latest_entries_by_lane.get(lane_id) or {}).get("entry_id") or "")
            ),
            prior_alert_rows=[row for row in alert_rows_by_key.values() if str(row.get("lane_id")) == lane_id],
            prior_pending_rows=[row for row in previous_pending_rows if str(row.get("lane_id")) == lane_id],
            prior_open_position=next((row for row in previous_open_positions if str(row.get("lane_id")) == lane_id), None),
            prior_closed_position=next((row for row in closed_positions_by_trade_id.values() if str(row.get("lane_id")) == lane_id), None),
            entries_enabled=entries_enabled,
            exits_enabled=exits_enabled,
            operator_halt=operator_halt,
        )
        lane_rows.append(lane_result["lane_row"])
        pending_rows.extend(lane_result["pending_intents"])
        open_position_rows.extend(lane_result["open_positions"])
        for row in lane_result["intents"]:
            intents_by_id[str(row["order_intent_id"])] = row
        for row in lane_result["fills"]:
            fills_by_intent_id[str(row["order_intent_id"])] = row
        for row in lane_result["closed_positions"]:
            closed_positions_by_trade_id[str(row["trade_id"])] = row
        for row in lane_result["alerts"]:
            alert_rows_by_key[str(row["dedup_key"])] = row
        for row in lane_result["runtime_events"]:
            runtime_events_by_id[str(row["event_id"])] = row
        reconciliation_rows.extend(lane_result["reconciliation_events"])
    bridge_seconds = perf_counter() - bridge_started

    intents_rows = sorted(intents_by_id.values(), key=lambda row: str(row.get("created_at") or ""), reverse=True)
    fill_rows = sorted(fills_by_intent_id.values(), key=lambda row: str(row.get("fill_timestamp") or ""), reverse=True)
    closed_position_rows = sorted(
        closed_positions_by_trade_id.values(),
        key=lambda row: str(row.get("exit_ts") or row.get("entry_ts") or ""),
        reverse=True,
    )
    alert_rows = sorted(alert_rows_by_key.values(), key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
    reconciliation_rows.sort(key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
    runtime_event_rows = sorted(runtime_events_by_id.values(), key=lambda row: str(row.get("occurred_at") or ""), reverse=True)
    pending_rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    open_position_rows.sort(key=lambda row: str(row.get("entry_ts") or ""), reverse=True)
    lane_rows.sort(key=lambda row: str(row.get("lane_id") or ""))
    review_event_rows = list(previous_state.get("review_event_rows") or [])

    if recovered_interrupted_cycle:
        runtime_event_rows.insert(
            0,
            _runtime_event_row(
                occurred_at=generated_at,
                runtime_identity=_runtime_identity_for_lane(lane_ids[0]),
                severity="WARN",
                event_type="CYCLE_RECOVERED_AFTER_INTERRUPT",
                classification="cycle_recovered_after_interrupt",
                message="A prior bridge cycle ended while still marked in progress. This run recovered the cadence state and continued safely.",
                lane_id=str(lane_ids[0]),
            ),
        )

    summary = _build_summary(
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        lane_ids=lane_ids,
        lane_rows=lane_rows,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        open_position_rows=open_position_rows,
        pending_rows=pending_rows,
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        bridge_mode=BRIDGE_MODE_PROSPECTIVE,
        pending_intent_count=len(pending_rows),
        runtime_event_rows=runtime_event_rows,
        cycle_index=bridge_cycle_index,
    )
    blocked_reason = _bridge_blocked_reason(
        entries_enabled=entries_enabled,
        exits_enabled=exits_enabled,
        operator_halt=operator_halt,
        summary=summary,
    )
    cadence_state = _build_cadence_state(
        previous_cadence_state=previous_cadence_state,
        bridge_cycle_index=bridge_cycle_index,
        cycle_ordinal=cycle_ordinal,
        requested_cycle_count=requested_cycle_count,
        poll_interval_seconds=poll_interval_seconds,
        stop_when_settled=stop_when_settled,
        generated_at=generated_at,
        entries_enabled=entries_enabled,
        exits_enabled=exits_enabled,
        operator_halt=operator_halt,
        cycle_state=_bridge_cycle_state(blocked_reason=blocked_reason, summary=summary),
        summary=summary,
        blocked_reason=blocked_reason,
        recovered_interrupted_cycle=recovered_interrupted_cycle,
    )
    operator_status = _build_operator_status(summary=summary, bridge_mode=BRIDGE_MODE_PROSPECTIVE)
    runtime_state = {
        "generated_at": generated_at.isoformat(),
        "bridge_run_id": bridge_run_id,
        "bridge_cycle_index": bridge_cycle_index,
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "live_execution_enabled": False,
        "entries_enabled": entries_enabled,
        "exits_enabled": exits_enabled,
        "operator_halt": operator_halt,
        "cycle_policy": cycle_policy,
        "cadence_state": cadence_state,
        "lane_rows": lane_rows,
        "pending_intents": pending_rows,
        "open_positions": open_position_rows,
        "closed_positions": closed_position_rows,
        "runtime_events": runtime_event_rows,
        "anomaly_summary": _build_anomaly_summary(alert_rows=alert_rows, reconciliation_rows=reconciliation_rows),
        "anomaly_queue": _build_anomaly_queue(alert_rows=alert_rows),
        "review_events": review_event_rows,
        "operator_status": operator_status,
        "summary": summary,
    }
    snapshot = _build_snapshot(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        bridge_mode=BRIDGE_MODE_PROSPECTIVE,
        selected_tenants=[
            {
                "strategy_family": "warehouse_historical_evaluator",
                "family_label": "Warehouse Historical Evaluator",
                "lane_count": len(lane_ids),
                "lane_ids": list(lane_ids),
            }
        ],
        selected_exclusions=[
            "ATP companion remains historical-only in this tranche.",
            "Approved quant remains deferred until a new family hypothesis exists.",
            "No live broker execution path is enabled by this bridge.",
        ],
        summary=summary,
        operator_status=operator_status,
        lane_rows=lane_rows,
        pending_intents=pending_rows,
        recent_intents=intents_rows[:25],
        recent_fills=fill_rows[:25],
        recent_closed_positions=closed_position_rows[:25],
        open_positions=open_position_rows,
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        source_truth={
            "warehouse_root": str(warehouse_root),
            "source_dataset": "lane_entries",
            "selected_lane_ids": list(lane_ids),
            "selected_entry_count": len(latest_entries_by_lane),
            "bridge_source_class": "prospective_entry_truth",
            "matching_closed_trade_count": len(closed_trade_by_entry_id),
        },
        cycle_policy=cycle_policy,
        cadence_state=cadence_state,
        review_event_rows=review_event_rows,
        timing={
            "trade_load_seconds": round(trade_load_seconds, 6),
            "bridge_seconds": round(bridge_seconds, 6),
            "total_seconds": round(perf_counter() - started, 6),
        },
        cycle_index=bridge_cycle_index,
    )
    _write_runtime_bridge_outputs(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=bridge_run_id,
        bridge_mode=BRIDGE_MODE_PROSPECTIVE,
        warehouse_root=warehouse_root,
        lane_ids=lane_ids,
        summary=summary,
        operator_status=operator_status,
        runtime_state=runtime_state,
        snapshot=snapshot,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        alert_rows=alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        review_event_rows=review_event_rows,
        cadence_state=cadence_state,
        db_path=db_path,
    )
    return {
        "output_dir": str(output_dir),
        "snapshot_path": str((output_dir / "bridge_snapshot.json").resolve()),
        "operator_status_path": str((output_dir / "operator_status.json").resolve()),
        "runtime_state_path": str((output_dir / "runtime_state.json").resolve()),
        "db_path": str(db_path.resolve()),
        "summary": summary,
        "timing": snapshot["timing"],
    }


def _bridge_lane_prospective(
    *,
    bridge_run_id: str,
    bridge_cycle_index: int,
    generated_at: datetime,
    engine,
    runtime_identity: dict[str, Any],
    lane_id: str,
    lane_label: str,
    latest_entry_row: dict[str, Any] | None,
    matching_closed_trade: dict[str, Any] | None,
    prior_alert_rows: Sequence[dict[str, Any]],
    prior_pending_rows: Sequence[dict[str, Any]],
    prior_open_position: dict[str, Any] | None,
    prior_closed_position: dict[str, Any] | None,
    entries_enabled: bool,
    exits_enabled: bool,
    operator_halt: bool,
) -> dict[str, Any]:
    repositories = RepositorySet(engine, runtime_identity=runtime_identity)
    broker = PaperBroker()
    broker.connect()
    lane_engine = ExecutionEngine(broker=broker)
    lane_state = _initial_lane_state(lane_id=lane_id, lane_label=lane_label, runtime_identity=runtime_identity)
    lane_state["bridge_mode"] = BRIDGE_MODE_PROSPECTIVE
    lane_state["entries_enabled"] = entries_enabled
    lane_state["exits_enabled"] = exits_enabled
    lane_state["operator_halt"] = operator_halt
    prior_alerts_by_key = {
        str(row.get("dedup_key") or row.get("review_key") or ""): dict(row)
        for row in prior_alert_rows
        if str(row.get("dedup_key") or row.get("review_key") or "")
    }

    pending_rows = [dict(row) for row in prior_pending_rows]
    alerts: list[dict[str, Any]] = []
    reconciliation_rows: list[dict[str, Any]] = []
    runtime_events: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    closed_positions: list[dict[str, Any]] = []
    open_positions: list[dict[str, Any]] = []
    intents: list[dict[str, Any]] = []

    _restore_lane_runtime_state(
        lane_engine=lane_engine,
        lane_state=lane_state,
        prior_pending_rows=pending_rows,
        prior_open_position=prior_open_position,
    )
    current_open_position = dict(prior_open_position) if prior_open_position is not None else None
    starting_open_position = dict(prior_open_position) if prior_open_position is not None else None
    prior_pending_entry_rows = [dict(row) for row in pending_rows if str(row.get("phase") or "") == "entry"]
    prior_pending_exit_rows = [dict(row) for row in pending_rows if str(row.get("phase") or "") == "exit"]
    entry_validation_error = _validate_entry_row(latest_entry_row) if latest_entry_row is not None else None
    trade_validation_error = _validate_trade_row(matching_closed_trade) if matching_closed_trade is not None else None

    if entry_validation_error:
        dedup_key = f"invalid-entry-row:{lane_id}:{(latest_entry_row or {}).get('entry_id')}"
        alerts.append(
            _bridge_alert_row(
                occurred_at=generated_at,
                severity="ERROR",
                title="Entry truth unavailable",
                message=entry_validation_error,
                lane_id=lane_id,
                runtime_identity=runtime_identity,
                trade_row=latest_entry_row,
                dedup_key=dedup_key,
                extra={"classification": "DATA_ARTIFACT_UNAVAILABLE"},
            )
        )
        runtime_events.append(
            _runtime_event_row(
                occurred_at=generated_at,
                runtime_identity=runtime_identity,
                severity="ERROR",
                event_type="ANOMALY_DETECTED",
                classification="data_artifact_unavailable",
                message=entry_validation_error,
                lane_id=lane_id,
                research_entry_id=str((latest_entry_row or {}).get("entry_id") or ""),
                related_alert_key=dedup_key,
            )
        )
    if trade_validation_error:
        dedup_key = f"invalid-trade-row:{lane_id}:{(matching_closed_trade or {}).get('trade_id')}"
        alerts.append(
            _bridge_alert_row(
                occurred_at=generated_at,
                severity="ERROR",
                title="Exit truth unavailable",
                message=trade_validation_error,
                lane_id=lane_id,
                runtime_identity=runtime_identity,
                trade_row=matching_closed_trade,
                dedup_key=dedup_key,
                extra={"classification": "DATA_ARTIFACT_UNAVAILABLE"},
            )
        )
        runtime_events.append(
            _runtime_event_row(
                occurred_at=generated_at,
                runtime_identity=runtime_identity,
                severity="ERROR",
                event_type="ANOMALY_DETECTED",
                classification="data_artifact_unavailable",
                message=trade_validation_error,
                lane_id=lane_id,
                research_trade_id=str((matching_closed_trade or {}).get("trade_id") or ""),
                related_alert_key=dedup_key,
            )
        )

    if latest_entry_row is not None and entry_validation_error is None and current_open_position is None and prior_closed_position is None:
        existing_entry_ids = {
            str(row.get("research_entry_id") or "")
            for row in [*pending_rows, *([current_open_position] if current_open_position else [])]
            if str(row.get("research_entry_id") or "")
        }
        latest_entry_id = str(latest_entry_row.get("entry_id") or "")
        if latest_entry_id and latest_entry_id not in existing_entry_ids:
            if operator_halt or not entries_enabled:
                dedup_key = f"entry-emission-blocked:{lane_id}:{latest_entry_id}"
                alerts.append(
                    _bridge_alert_row(
                        occurred_at=generated_at,
                        severity="WARN",
                        title="Entry emission blocked",
                        message="Prospective entry intent emission is currently gated by operator halt or entries-disabled mode.",
                        lane_id=lane_id,
                        runtime_identity=runtime_identity,
                        trade_row=latest_entry_row,
                        dedup_key=dedup_key,
                        extra={"classification": "SESSION_GATE_BLOCKED"},
                    )
                )
                runtime_events.append(
                    _runtime_event_row(
                        occurred_at=generated_at,
                        runtime_identity=runtime_identity,
                        severity="WARN",
                        event_type="ANOMALY_DETECTED",
                        classification="session_gate_blocked",
                        message="Prospective entry intent emission is currently gated by operator halt or entries-disabled mode.",
                        lane_id=lane_id,
                        research_entry_id=latest_entry_id,
                        related_alert_key=dedup_key,
                    )
                )
            else:
                entry_intent = _build_prospective_entry_intent(latest_entry_row)
                pending_execution = lane_engine.submit_intent(entry_intent)
                if pending_execution is None:
                    dedup_key = f"duplicate-entry-intent:{lane_id}:{latest_entry_id}"
                    alerts.append(
                        _bridge_alert_row(
                            occurred_at=_as_datetime(latest_entry_row["entry_ts"]),
                            severity="ERROR",
                            title="Duplicate entry intent",
                            message="The execution engine refused a duplicate or conflicting prospective entry intent.",
                            lane_id=lane_id,
                            runtime_identity=runtime_identity,
                            trade_row=latest_entry_row,
                            dedup_key=dedup_key,
                            extra={"classification": "DUPLICATE_INTENT"},
                        )
                    )
                    runtime_events.append(
                        _runtime_event_row(
                            occurred_at=_as_datetime(latest_entry_row["entry_ts"]),
                            runtime_identity=runtime_identity,
                            severity="ERROR",
                            event_type="ANOMALY_DETECTED",
                            classification="duplicate_intent",
                            message="The execution engine refused a duplicate or conflicting prospective entry intent.",
                            lane_id=lane_id,
                            research_entry_id=latest_entry_id,
                            related_alert_key=dedup_key,
                        )
                    )
                else:
                    repositories.order_intents.save(
                        entry_intent,
                        OrderStatus.ACKNOWLEDGED,
                        broker_order_id=pending_execution.broker_order_id,
                        submitted_at=pending_execution.submitted_at,
                        acknowledged_at=pending_execution.acknowledged_at,
                        broker_order_status=pending_execution.broker_order_status,
                        last_status_checked_at=pending_execution.last_status_checked_at,
                        retry_count=pending_execution.retry_count,
                    )
                    intent_row = _prospective_intent_artifact_row(
                        bridge_run_id=bridge_run_id,
                        bridge_cycle_index=bridge_cycle_index,
                        runtime_identity=runtime_identity,
                        intent=entry_intent,
                        pending_execution=pending_execution,
                        phase="entry",
                        source_row=latest_entry_row,
                        source_dataset="lane_entries",
                        lifecycle_state="PENDING",
                    )
                    pending_rows.append(intent_row)
                    intents.append(intent_row)
                    lane_state["runtime_status"] = "PENDING_ENTRY"
                    lane_state["last_order_intent_id"] = entry_intent.order_intent_id
                    runtime_events.append(
                        _runtime_event_row(
                            occurred_at=_as_datetime(latest_entry_row["entry_ts"]),
                            runtime_identity=runtime_identity,
                            severity="INFO",
                            event_type="INTENT_EMITTED",
                            classification="prospective_entry_pending",
                            message="Prospective paper entry intent emitted and acknowledged.",
                            lane_id=lane_id,
                            order_intent_id=entry_intent.order_intent_id,
                            research_entry_id=latest_entry_id,
                        )
                    )

    for pending_row in prior_pending_entry_rows:
        if bridge_cycle_index - int(pending_row.get("emitted_cycle") or bridge_cycle_index) < PROSPECTIVE_MIN_FILL_DELAY_CYCLES:
            continue
        if operator_halt or not entries_enabled:
            continue
        created_at = _as_datetime(pending_row["created_at"])
        fill_timestamp = _as_datetime(pending_row.get("fill_due_at") or pending_row["created_at"])
        if fill_timestamp < created_at:
            dedup_key = f"entry-timing-inconsistent:{pending_row.get('order_intent_id')}"
            alerts.append(
                _bridge_alert_row(
                    occurred_at=generated_at,
                    severity="ERROR",
                    title="Intent timing inconsistent",
                    message="Pending entry intent fill timestamp precedes intent creation time.",
                    lane_id=lane_id,
                    runtime_identity=runtime_identity,
                    trade_row=None,
                    dedup_key=dedup_key,
                    extra={"classification": "UNEXPECTED_RUNTIME_STATE_TRANSITION", "order_intent_id": pending_row.get("order_intent_id")},
                )
            )
            runtime_events.append(
                _runtime_event_row(
                    occurred_at=generated_at,
                    runtime_identity=runtime_identity,
                    severity="ERROR",
                    event_type="ANOMALY_DETECTED",
                    classification="unexpected_runtime_state_transition",
                    message="Pending entry intent fill timestamp precedes intent creation time.",
                    lane_id=lane_id,
                    order_intent_id=str(pending_row.get("order_intent_id") or ""),
                    research_entry_id=str(pending_row.get("research_entry_id") or ""),
                    related_alert_key=dedup_key,
                )
            )
            continue
        fill_price = _as_decimal(pending_row.get("planned_fill_price") or pending_row.get("entry_price"))
        intent = _intent_from_artifact_row(pending_row)
        fill = lane_engine.broker.fill_order(intent, fill_price=fill_price, fill_timestamp=fill_timestamp)
        lane_engine.clear_intent(intent.order_intent_id)
        repositories.order_intents.save(
            intent,
            OrderStatus.FILLED,
            broker_order_id=fill.broker_order_id,
            submitted_at=_as_datetime(pending_row["created_at"]),
            acknowledged_at=_as_datetime(pending_row.get("acknowledged_at") or pending_row["created_at"]),
            broker_order_status=fill.order_status.value,
            last_status_checked_at=fill_timestamp,
            retry_count=int(pending_row.get("retry_count") or 0),
        )
        repositories.fills.save(fill)
        updated_intent_row = dict(pending_row)
        updated_intent_row.update(
            {
                "order_status": OrderStatus.FILLED.value,
                "broker_order_status": fill.order_status.value,
                "lifecycle_state": "FILLED",
                "fill_timestamp": fill_timestamp.isoformat(),
                "last_status_checked_at": fill_timestamp.isoformat(),
            }
        )
        intents.append(updated_intent_row)
        fills.append(
            _prospective_fill_artifact_row(
                bridge_run_id=bridge_run_id,
                runtime_identity=runtime_identity,
                intent=intent,
                fill=fill,
                source_row=latest_entry_row or pending_row,
                source_dataset="lane_entries",
                phase="entry",
            )
        )
        pending_rows = [row for row in pending_rows if str(row.get("order_intent_id")) != intent.order_intent_id]
        current_open_position = _prospective_open_position_row(
            bridge_run_id=bridge_run_id,
            runtime_identity=runtime_identity,
            source_entry_row=latest_entry_row or pending_row,
            matching_closed_trade=matching_closed_trade,
            entry_fill=fill,
        )
        open_positions = [current_open_position]
        lane_state.update(
            {
                "runtime_status": "IN_POSITION",
                "position_side": current_open_position["side"],
                "expected_signed_quantity": 1 if current_open_position["side"] == "LONG" else -1,
                "internal_position_qty": 1,
                "broker_position_qty": 1,
                "average_price": str(fill.fill_price) if fill.fill_price is not None else None,
                "last_fill_timestamp": fill.fill_timestamp.isoformat(),
                "last_order_intent_id": intent.order_intent_id,
                "open_position": current_open_position,
            }
        )
        runtime_events.append(
            _runtime_event_row(
                occurred_at=fill.fill_timestamp,
                runtime_identity=runtime_identity,
                severity="INFO",
                event_type="INTENT_FILLED",
                classification="prospective_entry_filled",
                message="Prospective paper entry intent filled and position opened.",
                lane_id=lane_id,
                order_intent_id=intent.order_intent_id,
                research_entry_id=str(pending_row.get("research_entry_id") or ""),
                research_trade_id=str((matching_closed_trade or {}).get("trade_id") or ""),
            )
        )
        reconciliation_rows.extend(
            _persist_reconciliation(
                repositories=repositories,
                coordinator=ReconciliationCoordinator(),
                lane_engine=lane_engine,
                lane_state=lane_state,
                runtime_identity=runtime_identity,
                occurred_at=fill.fill_timestamp,
                trigger="prospective_entry_fill",
                alerts=alerts,
            )
        )

    if starting_open_position is not None and matching_closed_trade is not None and trade_validation_error is None and current_open_position is not None:
        has_pending_exit = any(str(row.get("phase")) == "exit" for row in pending_rows)
        if not has_pending_exit and prior_closed_position is None:
            if operator_halt or not exits_enabled:
                dedup_key = f"exit-emission-blocked:{lane_id}:{matching_closed_trade.get('trade_id')}"
                alerts.append(
                    _bridge_alert_row(
                        occurred_at=generated_at,
                        severity="WARN",
                        title="Exit emission blocked",
                        message="Prospective exit intent emission is currently gated by operator halt or exits-disabled mode.",
                        lane_id=lane_id,
                        runtime_identity=runtime_identity,
                        trade_row=matching_closed_trade,
                        dedup_key=dedup_key,
                        extra={"classification": "SESSION_GATE_BLOCKED"},
                    )
                )
                runtime_events.append(
                    _runtime_event_row(
                        occurred_at=generated_at,
                        runtime_identity=runtime_identity,
                        severity="WARN",
                        event_type="ANOMALY_DETECTED",
                        classification="session_gate_blocked",
                        message="Prospective exit intent emission is currently gated by operator halt or exits-disabled mode.",
                        lane_id=lane_id,
                        research_trade_id=str(matching_closed_trade.get("trade_id") or ""),
                        related_alert_key=dedup_key,
                    )
                )
            else:
                exit_intent = _build_prospective_exit_intent(matching_closed_trade)
                pending_execution = lane_engine.submit_intent(exit_intent)
                if pending_execution is not None:
                    repositories.order_intents.save(
                        exit_intent,
                        OrderStatus.ACKNOWLEDGED,
                        broker_order_id=pending_execution.broker_order_id,
                        submitted_at=pending_execution.submitted_at,
                        acknowledged_at=pending_execution.acknowledged_at,
                        broker_order_status=pending_execution.broker_order_status,
                        last_status_checked_at=pending_execution.last_status_checked_at,
                        retry_count=pending_execution.retry_count,
                    )
                    exit_intent_row = _prospective_intent_artifact_row(
                        bridge_run_id=bridge_run_id,
                        bridge_cycle_index=bridge_cycle_index,
                        runtime_identity=runtime_identity,
                        intent=exit_intent,
                        pending_execution=pending_execution,
                        phase="exit",
                        source_row=matching_closed_trade,
                        source_dataset="lane_closed_trades",
                        lifecycle_state="PENDING",
                    )
                    pending_rows.append(exit_intent_row)
                    intents.append(exit_intent_row)
                    lane_state["runtime_status"] = "PENDING_EXIT"
                    lane_state["last_order_intent_id"] = exit_intent.order_intent_id
                    runtime_events.append(
                        _runtime_event_row(
                            occurred_at=_as_datetime(matching_closed_trade["exit_ts"]),
                            runtime_identity=runtime_identity,
                            severity="INFO",
                            event_type="INTENT_EMITTED",
                            classification="prospective_exit_pending",
                            message="Prospective paper exit intent emitted and acknowledged.",
                            lane_id=lane_id,
                            order_intent_id=exit_intent.order_intent_id,
                            research_trade_id=str(matching_closed_trade.get("trade_id") or ""),
                        )
                    )

    for pending_row in prior_pending_exit_rows:
        if bridge_cycle_index - int(pending_row.get("emitted_cycle") or bridge_cycle_index) < PROSPECTIVE_MIN_FILL_DELAY_CYCLES:
            continue
        if operator_halt or not exits_enabled:
            continue
        created_at = _as_datetime(pending_row["created_at"])
        fill_timestamp = _as_datetime(pending_row.get("fill_due_at") or pending_row["created_at"])
        if fill_timestamp < created_at:
            dedup_key = f"exit-timing-inconsistent:{pending_row.get('order_intent_id')}"
            alerts.append(
                _bridge_alert_row(
                    occurred_at=generated_at,
                    severity="ERROR",
                    title="Intent timing inconsistent",
                    message="Pending exit intent fill timestamp precedes intent creation time.",
                    lane_id=lane_id,
                    runtime_identity=runtime_identity,
                    trade_row=None,
                    dedup_key=dedup_key,
                    extra={"classification": "UNEXPECTED_RUNTIME_STATE_TRANSITION", "order_intent_id": pending_row.get("order_intent_id")},
                )
            )
            runtime_events.append(
                _runtime_event_row(
                    occurred_at=generated_at,
                    runtime_identity=runtime_identity,
                    severity="ERROR",
                    event_type="ANOMALY_DETECTED",
                    classification="unexpected_runtime_state_transition",
                    message="Pending exit intent fill timestamp precedes intent creation time.",
                    lane_id=lane_id,
                    order_intent_id=str(pending_row.get("order_intent_id") or ""),
                    research_trade_id=str(pending_row.get("research_trade_id") or ""),
                    related_alert_key=dedup_key,
                )
            )
            continue
        fill_price = _as_decimal(pending_row.get("planned_fill_price") or pending_row.get("exit_price"))
        intent = _intent_from_artifact_row(pending_row)
        fill = lane_engine.broker.fill_order(intent, fill_price=fill_price, fill_timestamp=fill_timestamp)
        lane_engine.clear_intent(intent.order_intent_id)
        repositories.order_intents.save(
            intent,
            OrderStatus.FILLED,
            broker_order_id=fill.broker_order_id,
            submitted_at=_as_datetime(pending_row["created_at"]),
            acknowledged_at=_as_datetime(pending_row.get("acknowledged_at") or pending_row["created_at"]),
            broker_order_status=fill.order_status.value,
            last_status_checked_at=fill_timestamp,
            retry_count=int(pending_row.get("retry_count") or 0),
        )
        repositories.fills.save(fill)
        updated_exit_intent_row = dict(pending_row)
        updated_exit_intent_row.update(
            {
                "order_status": OrderStatus.FILLED.value,
                "broker_order_status": fill.order_status.value,
                "lifecycle_state": "FILLED",
                "fill_timestamp": fill_timestamp.isoformat(),
                "last_status_checked_at": fill_timestamp.isoformat(),
            }
        )
        intents.append(updated_exit_intent_row)
        fills.append(
            _prospective_fill_artifact_row(
                bridge_run_id=bridge_run_id,
                runtime_identity=runtime_identity,
                intent=intent,
                fill=fill,
                source_row=matching_closed_trade or pending_row,
                source_dataset="lane_closed_trades",
                phase="exit",
            )
        )
        pending_rows = [row for row in pending_rows if str(row.get("order_intent_id")) != intent.order_intent_id]
        if matching_closed_trade is not None:
            entry_intent_row = next(
                (
                    row
                    for row in [*intents, *prior_pending_entry_rows]
                    if str(row.get("phase") or "") == "entry"
                    and str(row.get("research_entry_id") or "") == str(matching_closed_trade.get("entry_id") or "")
                ),
                None,
            )
            closed_position_row = _closed_position_row(
                bridge_run_id=bridge_run_id,
                runtime_identity=runtime_identity,
                trade_row=matching_closed_trade,
                entry_intent=(
                    _intent_from_artifact_row(entry_intent_row)
                    if entry_intent_row is not None
                    else _build_prospective_entry_intent(latest_entry_row or pending_row)
                ),
                exit_intent=intent,
            )
            closed_positions.append(closed_position_row)
        current_open_position = None
        open_positions = []
        lane_state.update(
            {
                "runtime_status": "READY",
                "position_side": "FLAT",
                "expected_signed_quantity": 0,
                "internal_position_qty": 0,
                "broker_position_qty": 0,
                "average_price": None,
                "last_fill_timestamp": fill.fill_timestamp.isoformat(),
                "last_order_intent_id": intent.order_intent_id,
                "open_position": None,
            }
        )
        runtime_events.append(
            _runtime_event_row(
                occurred_at=fill.fill_timestamp,
                runtime_identity=runtime_identity,
                severity="INFO",
                event_type="POSITION_CLOSED",
                classification="prospective_exit_filled",
                message="Prospective paper exit intent filled and position closed.",
                lane_id=lane_id,
                order_intent_id=intent.order_intent_id,
                research_trade_id=str((matching_closed_trade or {}).get("trade_id") or ""),
            )
        )
        reconciliation_rows.extend(
            _persist_reconciliation(
                repositories=repositories,
                coordinator=ReconciliationCoordinator(),
                lane_engine=lane_engine,
                lane_state=lane_state,
                runtime_identity=runtime_identity,
                occurred_at=fill.fill_timestamp,
                trigger="prospective_exit_fill",
                alerts=alerts,
            )
        )

    alert_dedup_keys = {str(row.get("dedup_key")) for row in alerts}
    next_pending_rows: list[dict[str, Any]] = []
    for pending_row in pending_rows:
        pending_age_cycles = bridge_cycle_index - int(pending_row.get("emitted_cycle") or bridge_cycle_index)
        phase = str(pending_row.get("phase") or "entry")
        pending_row = {
            **pending_row,
            "age_cycles": pending_age_cycles,
            "last_cycle_seen": bridge_cycle_index,
            "blocked_by": (
                "OPERATOR_HALT" if operator_halt else ("ENTRIES_DISABLED" if phase == "entry" and not entries_enabled else ("EXITS_DISABLED" if phase == "exit" and not exits_enabled else None))
            ),
            "next_transition_hint": "AWAIT_EXIT_FILL" if phase == "exit" else "AWAIT_ENTRY_FILL",
        }
        if pending_age_cycles >= PROSPECTIVE_STALE_AFTER_CYCLES:
            dedup_key = f"stale-pending:{pending_row.get('order_intent_id')}"
            if dedup_key not in alert_dedup_keys:
                alerts.append(
                    _bridge_alert_row(
                        occurred_at=generated_at,
                        severity="WARN",
                        title="Stale pending intent",
                        message="Pending paper intent has remained unresolved across multiple runtime cycles.",
                        lane_id=lane_id,
                        runtime_identity=runtime_identity,
                        trade_row=None,
                        dedup_key=dedup_key,
                        extra={
                            "classification": "STALE_PENDING_INTENT",
                            "order_intent_id": pending_row.get("order_intent_id"),
                            "pending_age_cycles": pending_age_cycles,
                        },
                    )
                )
                alert_dedup_keys.add(dedup_key)
                runtime_events.append(
                    _runtime_event_row(
                        occurred_at=generated_at,
                        runtime_identity=runtime_identity,
                        severity="WARN",
                        event_type="ANOMALY_DETECTED",
                        classification="stale_pending_intent",
                        message="Pending paper intent has remained unresolved across multiple runtime cycles.",
                        lane_id=lane_id,
                        order_intent_id=str(pending_row.get("order_intent_id") or ""),
                        research_trade_id=str(pending_row.get("research_trade_id") or ""),
                        related_alert_key=dedup_key,
                    )
                )
        if pending_age_cycles >= PROSPECTIVE_EXPIRE_AFTER_CYCLES:
            canceled_row = dict(pending_row)
            canceled_row.update(
                {
                    "order_status": OrderStatus.CANCELLED.value,
                    "broker_order_status": OrderStatus.CANCELLED.value,
                    "lifecycle_state": "EXPIRED",
                    "last_status_checked_at": generated_at.isoformat(),
                    "next_transition_hint": "REQUIRES_OPERATOR_REVIEW",
                }
            )
            intents.append(canceled_row)
            runtime_events.append(
                _runtime_event_row(
                    occurred_at=generated_at,
                    runtime_identity=runtime_identity,
                    severity="WARN",
                    event_type="INTENT_EXPIRED",
                    classification="stale_pending_intent_expired",
                    message="Pending paper intent expired after remaining unresolved for too many cycles.",
                    lane_id=lane_id,
                    order_intent_id=str(pending_row.get("order_intent_id") or ""),
                )
            )
            lane_state["runtime_status"] = "ATTENTION_REQUIRED"
            lane_state["operator_action_required"] = True
            continue
        next_pending_rows.append(pending_row)
    pending_rows = next_pending_rows
    alerts = _finalize_lane_alerts(
        alerts=alerts,
        prior_alerts_by_key=prior_alerts_by_key,
        generated_at=generated_at,
    )
    if current_open_position is not None:
        open_positions = [current_open_position]

    final_occurred_at = generated_at
    reconciliation_rows.extend(
        _persist_reconciliation(
            repositories=repositories,
            coordinator=ReconciliationCoordinator(),
            lane_engine=lane_engine,
            lane_state=lane_state,
            runtime_identity=runtime_identity,
            occurred_at=final_occurred_at,
            trigger="final_snapshot",
            alerts=alerts,
        )
    )
    for alert_row in alerts:
        repositories.alerts.save(alert_row, occurred_at=_as_datetime(alert_row["occurred_at"]))

    pending_entry_count = sum(1 for row in pending_rows if str(row.get("phase")) == "entry")
    pending_exit_count = sum(1 for row in pending_rows if str(row.get("phase")) == "exit")
    lane_row = {
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "lane_id": lane_id,
        "strategy_label": lane_label,
        "instrument": runtime_identity["instrument"],
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "source_dataset": "lane_entries",
        "trade_count": 1 if matching_closed_trade is not None else 0,
        "entry_count": 1 if latest_entry_row is not None else 0,
        "intent_count": len(intents),
        "fill_count": len(fills),
        "closed_position_count": len(closed_positions),
        "open_position_count": len(open_positions),
        "pending_intent_count": len(pending_rows),
        "pending_entry_count": pending_entry_count,
        "pending_exit_count": pending_exit_count,
        "reconciliation_issue_count": sum(1 for row in reconciliation_rows if row.get("clean") is not True),
        "active_alert_count": sum(1 for row in alerts if row.get("active")),
        "runtime_status": lane_state["runtime_status"],
        "position_side": lane_state["position_side"],
        "internal_position_qty": lane_state["internal_position_qty"],
        "broker_position_qty": lane_state["broker_position_qty"],
        "average_price": lane_state["average_price"],
        "realized_pnl_cash": round(sum(float(row.get("realized_pnl_cash") or 0.0) for row in closed_positions), 6),
        "latest_fill_timestamp": lane_state["last_fill_timestamp"],
        "latest_reconciliation_classification": lane_state.get("latest_reconciliation_classification"),
        "operator_action_required": lane_state["operator_action_required"],
        "fault_code": lane_state["fault_code"],
        "current_cycle": bridge_cycle_index,
        "entry_ts": latest_entry_row.get("entry_ts") if latest_entry_row is not None else None,
        "exit_ts": matching_closed_trade.get("exit_ts") if matching_closed_trade is not None else None,
        "runtime_view": "prospective",
    }
    return {
        "lane_row": lane_row,
        "pending_intents": pending_rows,
        "intents": intents,
        "fills": fills,
        "closed_positions": closed_positions,
        "open_positions": open_positions,
        "alerts": alerts,
        "reconciliation_events": reconciliation_rows,
        "runtime_events": runtime_events,
    }

def _bridge_lane(
    *,
    bridge_run_id: str,
    generated_at: datetime,
    engine,
    runtime_identity: dict[str, Any],
    lane_id: str,
    lane_label: str,
    trade_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    repositories = RepositorySet(engine, runtime_identity=runtime_identity)
    reconciliation = ReconciliationCoordinator()
    broker = PaperBroker()
    broker.connect()
    lane_engine = ExecutionEngine(broker=broker)
    lane_state = _initial_lane_state(lane_id=lane_id, lane_label=lane_label, runtime_identity=runtime_identity)

    intents_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    closed_positions: list[dict[str, Any]] = []
    alerts: list[dict[str, Any]] = []
    reconciliation_rows: list[dict[str, Any]] = []

    sorted_trades = sorted(trade_rows, key=lambda row: (str(row.get("entry_ts") or ""), str(row.get("trade_id") or "")))

    for trade_row in sorted_trades:
        validation_error = _validate_trade_row(trade_row)
        if validation_error is not None:
            alert_row = _bridge_alert_row(
                occurred_at=generated_at,
                severity="ERROR",
                title="Invalid research trade row",
                message=validation_error,
                lane_id=lane_id,
                runtime_identity=runtime_identity,
                trade_row=trade_row,
                dedup_key=f"invalid-trade:{trade_row.get('trade_id')}",
            )
            alerts.append(alert_row)
            repositories.alerts.save(alert_row, occurred_at=generated_at)
            lane_state["runtime_status"] = "FAULT"
            lane_state["fault_code"] = "invalid_research_trade"
            lane_state["operator_action_required"] = True
            continue

        side = str(trade_row["side"]).upper()
        entry_ts = _as_datetime(trade_row["entry_ts"])
        exit_ts = _as_datetime(trade_row["exit_ts"])
        entry_price = _as_decimal(trade_row["entry_price"])
        exit_price = _as_decimal(trade_row["exit_price"])

        entry_intent = _build_intent(trade_row=trade_row, side=side, phase="entry", quantity=1)
        pending_entry = lane_engine.submit_intent(entry_intent)
        if pending_entry is None:
            alert_row = _bridge_alert_row(
                occurred_at=entry_ts,
                severity="ERROR",
                title="Entry intent rejected",
                message="Execution engine refused the entry intent for the selected research trade.",
                lane_id=lane_id,
                runtime_identity=runtime_identity,
                trade_row=trade_row,
                dedup_key=f"entry-rejected:{trade_row['trade_id']}",
            )
            alerts.append(alert_row)
            repositories.alerts.save(alert_row, occurred_at=entry_ts)
            lane_state["runtime_status"] = "FAULT"
            lane_state["fault_code"] = "entry_intent_rejected"
            lane_state["operator_action_required"] = True
            continue
        repositories.order_intents.save(
            entry_intent,
            OrderStatus.ACKNOWLEDGED,
            broker_order_id=pending_entry.broker_order_id,
            submitted_at=pending_entry.submitted_at,
            acknowledged_at=pending_entry.acknowledged_at,
            broker_order_status=pending_entry.broker_order_status,
            last_status_checked_at=pending_entry.last_status_checked_at,
            retry_count=pending_entry.retry_count,
        )
        entry_fill = lane_engine.broker.fill_order(entry_intent, fill_price=entry_price, fill_timestamp=entry_ts)
        lane_engine.clear_intent(entry_intent.order_intent_id)
        repositories.order_intents.save(
            entry_intent,
            OrderStatus.FILLED,
            broker_order_id=entry_fill.broker_order_id,
            submitted_at=pending_entry.submitted_at,
            acknowledged_at=pending_entry.acknowledged_at or entry_ts,
            broker_order_status=entry_fill.order_status.value,
            last_status_checked_at=entry_ts,
            retry_count=pending_entry.retry_count,
        )
        repositories.fills.save(entry_fill)
        entry_intent_row = _intent_artifact_row(
            bridge_run_id=bridge_run_id,
            runtime_identity=runtime_identity,
            intent=entry_intent,
            broker_order_id=entry_fill.broker_order_id,
            broker_order_status=entry_fill.order_status.value,
            acknowledged_at=entry_ts,
            research_trade=trade_row,
            phase="entry",
        )
        entry_fill_row = _fill_artifact_row(
            bridge_run_id=bridge_run_id,
            runtime_identity=runtime_identity,
            intent=entry_intent,
            fill_timestamp=entry_ts,
            fill_price=entry_price,
            broker_order_id=entry_fill.broker_order_id,
            research_trade=trade_row,
            phase="entry",
        )
        intents_rows.append(entry_intent_row)
        fill_rows.append(entry_fill_row)
        _apply_entry_fill_to_lane_state(lane_state=lane_state, trade_row=trade_row, entry_intent=entry_intent)
        reconciliation_rows.extend(
            _persist_reconciliation(
                repositories=repositories,
                coordinator=reconciliation,
                lane_engine=lane_engine,
                lane_state=lane_state,
                runtime_identity=runtime_identity,
                occurred_at=entry_ts,
                trigger="entry_fill",
                alerts=alerts,
            )
        )

        exit_intent = _build_intent(trade_row=trade_row, side=side, phase="exit", quantity=1)
        pending_exit = lane_engine.submit_intent(exit_intent)
        if pending_exit is None:
            alert_row = _bridge_alert_row(
                occurred_at=exit_ts,
                severity="ERROR",
                title="Exit intent rejected",
                message="Execution engine refused the exit intent for the selected research trade.",
                lane_id=lane_id,
                runtime_identity=runtime_identity,
                trade_row=trade_row,
                dedup_key=f"exit-rejected:{trade_row['trade_id']}",
            )
            alerts.append(alert_row)
            repositories.alerts.save(alert_row, occurred_at=exit_ts)
            lane_state["runtime_status"] = "FAULT"
            lane_state["fault_code"] = "exit_intent_rejected"
            lane_state["operator_action_required"] = True
            continue
        repositories.order_intents.save(
            exit_intent,
            OrderStatus.ACKNOWLEDGED,
            broker_order_id=pending_exit.broker_order_id,
            submitted_at=pending_exit.submitted_at,
            acknowledged_at=pending_exit.acknowledged_at,
            broker_order_status=pending_exit.broker_order_status,
            last_status_checked_at=pending_exit.last_status_checked_at,
            retry_count=pending_exit.retry_count,
        )
        exit_fill = lane_engine.broker.fill_order(exit_intent, fill_price=exit_price, fill_timestamp=exit_ts)
        lane_engine.clear_intent(exit_intent.order_intent_id)
        repositories.order_intents.save(
            exit_intent,
            OrderStatus.FILLED,
            broker_order_id=exit_fill.broker_order_id,
            submitted_at=pending_exit.submitted_at,
            acknowledged_at=pending_exit.acknowledged_at or exit_ts,
            broker_order_status=exit_fill.order_status.value,
            last_status_checked_at=exit_ts,
            retry_count=pending_exit.retry_count,
        )
        repositories.fills.save(exit_fill)
        exit_intent_row = _intent_artifact_row(
            bridge_run_id=bridge_run_id,
            runtime_identity=runtime_identity,
            intent=exit_intent,
            broker_order_id=exit_fill.broker_order_id,
            broker_order_status=exit_fill.order_status.value,
            acknowledged_at=exit_ts,
            research_trade=trade_row,
            phase="exit",
        )
        exit_fill_row = _fill_artifact_row(
            bridge_run_id=bridge_run_id,
            runtime_identity=runtime_identity,
            intent=exit_intent,
            fill_timestamp=exit_ts,
            fill_price=exit_price,
            broker_order_id=exit_fill.broker_order_id,
            research_trade=trade_row,
            phase="exit",
        )
        intents_rows.append(exit_intent_row)
        fill_rows.append(exit_fill_row)
        closed_positions.append(
            _closed_position_row(
                bridge_run_id=bridge_run_id,
                runtime_identity=runtime_identity,
                trade_row=trade_row,
                entry_intent=entry_intent,
                exit_intent=exit_intent,
            )
        )
        _apply_exit_fill_to_lane_state(lane_state=lane_state, trade_row=trade_row, exit_intent=exit_intent)
        reconciliation_rows.extend(
            _persist_reconciliation(
                repositories=repositories,
                coordinator=reconciliation,
                lane_engine=lane_engine,
                lane_state=lane_state,
                runtime_identity=runtime_identity,
                occurred_at=exit_ts,
                trigger="exit_fill",
                alerts=alerts,
            )
        )

    final_occurred_at = _as_datetime(sorted_trades[-1]["exit_ts"]) if sorted_trades else generated_at
    reconciliation_rows.extend(
        _persist_reconciliation(
            repositories=repositories,
            coordinator=reconciliation,
            lane_engine=lane_engine,
            lane_state=lane_state,
            runtime_identity=runtime_identity,
            occurred_at=final_occurred_at,
            trigger="final_snapshot",
            alerts=alerts,
        )
    )

    lane_row = {
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "lane_id": lane_id,
        "strategy_label": lane_label,
        "instrument": runtime_identity["instrument"],
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "trade_count": len(sorted_trades),
        "intent_count": len(intents_rows),
        "fill_count": len(fill_rows),
        "closed_position_count": len(closed_positions),
        "open_position_count": 1 if lane_state.get("open_position") else 0,
        "reconciliation_issue_count": sum(1 for row in reconciliation_rows if row.get("clean") is not True),
        "active_alert_count": sum(1 for row in alerts if row.get("active")),
        "runtime_status": lane_state["runtime_status"],
        "position_side": lane_state["position_side"],
        "internal_position_qty": lane_state["internal_position_qty"],
        "broker_position_qty": lane_state["broker_position_qty"],
        "average_price": lane_state["average_price"],
        "realized_pnl_cash": round(sum(float(row.get("realized_pnl_cash") or 0.0) for row in closed_positions), 6),
        "latest_fill_timestamp": lane_state["last_fill_timestamp"],
        "latest_reconciliation_classification": lane_state.get("latest_reconciliation_classification"),
        "operator_action_required": lane_state["operator_action_required"],
        "fault_code": lane_state["fault_code"],
    }

    open_positions = [dict(lane_state["open_position"])] if lane_state.get("open_position") else []
    return {
        "lane_row": lane_row,
        "intents": intents_rows,
        "fills": fill_rows,
        "closed_positions": closed_positions,
        "open_positions": open_positions,
        "alerts": alerts,
        "reconciliation_events": reconciliation_rows,
    }


def _persist_reconciliation(
    *,
    repositories: RepositorySet,
    coordinator: ReconciliationCoordinator,
    lane_engine: ExecutionEngine,
    lane_state: dict[str, Any],
    runtime_identity: dict[str, Any],
    occurred_at: datetime,
    trigger: str,
    alerts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    pending_order_ids = tuple(sorted(pending.broker_order_id for pending in lane_engine.pending_executions()))
    internal = InternalReconciliationSnapshot(
        strategy_status=str(lane_state["runtime_status"]),
        position_side=str(lane_state["position_side"]),
        expected_signed_quantity=int(lane_state["expected_signed_quantity"]),
        internal_position_qty=int(lane_state["internal_position_qty"]),
        broker_position_qty=int(lane_state["broker_position_qty"]),
        average_price=lane_state["average_price"],
        open_broker_order_id=pending_order_ids[0] if pending_order_ids else None,
        persisted_open_order_ids=pending_order_ids,
        pending_execution_open_order_ids=pending_order_ids,
        last_fill_timestamp=lane_state["last_fill_timestamp"],
        last_order_intent_id=lane_state["last_order_intent_id"],
        entries_enabled=bool(lane_state["entries_enabled"]),
        exits_enabled=bool(lane_state["exits_enabled"]),
        operator_halt=bool(lane_state["operator_halt"]),
        reconcile_required=bool(lane_state["reconcile_required"]),
        fault_code=lane_state["fault_code"],
        open_entry_leg_count=1 if lane_state.get("open_position") else 0,
        open_entry_leg_quantities=(1,) if lane_state.get("open_position") else (),
    )
    broker_snapshot = lane_engine.broker.snapshot_state()
    broker = BrokerReconciliationSnapshot(
        connected=bool(broker_snapshot.get("connected")),
        truth_complete=True,
        position_quantity=int(broker_snapshot.get("position_quantity") or 0),
        side=_signed_qty_to_side(int(broker_snapshot.get("position_quantity") or 0)),
        average_price=str(broker_snapshot.get("average_price")) if broker_snapshot.get("average_price") is not None else None,
        open_order_ids=tuple(sorted(str(item) for item in broker_snapshot.get("open_order_ids") or [])),
        order_status={str(key): str(value) for key, value in dict(broker_snapshot.get("order_status") or {}).items()},
        last_fill_timestamp=str(broker_snapshot.get("last_fill_timestamp")) if broker_snapshot.get("last_fill_timestamp") is not None else None,
    )
    outcome = coordinator.evaluate(trigger=trigger, internal=internal, broker=broker)
    payload = outcome.to_payload(occurred_at=occurred_at.isoformat())
    payload.update(
        {
            "event_type": "research_runtime_bridge_reconciliation",
            "bridge_mode": str(lane_state.get("bridge_mode") or BRIDGE_MODE_REPLAY),
            "execution_mode": BRIDGE_EXECUTION_MODE,
            "paper_only": True,
            "live_execution_enabled": False,
            "runtime_identity": dict(runtime_identity),
            "lane_id": runtime_identity["lane_id"],
            "strategy_family": runtime_identity["strategy_family"],
        }
    )
    repositories.reconciliation_events.save(payload, created_at=occurred_at)
    lane_state["latest_reconciliation_classification"] = outcome.classification
    lane_state["reconcile_required"] = outcome.clean is not True
    if outcome.clean is not True:
        lane_state["operator_action_required"] = True
        if outcome.classification != RECONCILIATION_CLASS_CLEAN:
            lane_state["runtime_status"] = "RECONCILING" if outcome.requires_fault is not True else "FAULT"
            if outcome.requires_fault and outcome.fault_code:
                lane_state["fault_code"] = outcome.fault_code
        alert_row = _bridge_alert_row(
            occurred_at=occurred_at,
            severity="ERROR" if outcome.requires_fault else "WARN",
            title="Research runtime reconciliation anomaly",
            message=outcome.recommended_action,
            lane_id=runtime_identity["lane_id"],
            runtime_identity=runtime_identity,
            trade_row=None,
            dedup_key=f"reconcile:{runtime_identity['lane_id']}:{outcome.classification}:{trigger}",
            extra={
                "classification": outcome.classification,
                "issues": list(outcome.mismatches),
                "repair_actions": list(outcome.repair_actions),
            },
        )
        alerts.append(alert_row)
        repositories.alerts.save(alert_row, occurred_at=occurred_at)
    return [payload]


def _load_existing_bridge_state(output_dir: Path) -> dict[str, Any]:
    snapshot_path = output_dir / "bridge_snapshot.json"
    runtime_state_path = output_dir / "runtime_state.json"
    return {
        "snapshot": _load_json_if_exists(snapshot_path),
        "runtime_state": _load_json_if_exists(runtime_state_path),
        "cadence_state": _load_json_if_exists(output_dir / "cadence_state.json"),
        "intents_rows": _load_jsonl_if_exists(output_dir / "order_intents.jsonl"),
        "fill_rows": _load_jsonl_if_exists(output_dir / "fills.jsonl"),
        "closed_position_rows": _load_jsonl_if_exists(output_dir / "trades.jsonl"),
        "alert_rows": _load_jsonl_if_exists(output_dir / "alerts.jsonl"),
        "reconciliation_rows": _load_jsonl_if_exists(output_dir / "reconciliation_events.jsonl"),
        "runtime_event_rows": _load_jsonl_if_exists(output_dir / "runtime_events.jsonl"),
        "review_event_rows": _load_jsonl_if_exists(output_dir / "operator_review_events.jsonl"),
    }


def review_runtime_bridge_anomaly(
    *,
    output_dir: Path,
    anomaly_key: str,
    operator_label: str,
    note: str | None = None,
    review_state: str = "ACKNOWLEDGED",
) -> dict[str, Any]:
    normalized_key = str(anomaly_key or "").strip()
    if not normalized_key:
        raise ValueError("An anomaly key is required to review a runtime bridge alert.")
    normalized_review_state = str(review_state or "ACKNOWLEDGED").strip().upper()
    if normalized_review_state not in {"ACKNOWLEDGED", "REVIEWED", "RESOLVED"}:
        raise ValueError(f"Unsupported runtime bridge review state: {review_state!r}")

    output_dir = output_dir.resolve()
    previous_state = _load_existing_bridge_state(output_dir)
    snapshot = dict(previous_state.get("snapshot") or {})
    runtime_state = dict(previous_state.get("runtime_state") or {})
    if not snapshot:
        raise FileNotFoundError(f"Runtime bridge snapshot not found under {output_dir}.")

    generated_at = datetime.now(UTC)
    alerts_rows = [dict(row) for row in previous_state.get("alert_rows") or []]
    matched_row: dict[str, Any] | None = None
    updated_alert_rows: list[dict[str, Any]] = []
    for row in alerts_rows:
        if str(row.get("dedup_key") or row.get("review_key") or "") == normalized_key:
            updated = dict(row)
            if normalized_review_state in {"ACKNOWLEDGED", "REVIEWED", "RESOLVED"}:
                updated["acknowledged"] = True
                updated["acknowledged_at"] = generated_at.isoformat()
                updated["acknowledged_by"] = operator_label
                updated["acknowledgement_note"] = note
            if normalized_review_state in {"REVIEWED", "RESOLVED"}:
                updated["reviewed_at"] = generated_at.isoformat()
                updated["reviewed_by"] = operator_label
                updated["review_note"] = note
            if normalized_review_state == "RESOLVED":
                updated["resolved_at"] = generated_at.isoformat()
                updated["resolved_by"] = operator_label
                updated["resolution_note"] = note
                updated["active"] = False
            updated["last_review_action"] = normalized_review_state
            updated["review_state"] = normalized_review_state
            updated["review_required"] = False if normalized_review_state == "RESOLVED" else bool(updated.get("review_required"))
            updated["action_required"] = normalized_review_state != "RESOLVED" and bool(updated.get("active"))
            matched_row = updated
            updated_alert_rows.append(updated)
        else:
            updated_alert_rows.append(dict(row))
    if matched_row is None:
        raise ValueError(f"Runtime bridge anomaly {normalized_key!r} was not found.")

    review_event_rows = [dict(row) for row in previous_state.get("review_event_rows") or []]
    review_event_rows.insert(
        0,
        _bridge_review_event_row(
            occurred_at=generated_at,
            anomaly_row=matched_row,
            operator_label=operator_label,
            note=note,
            review_state=normalized_review_state,
        ),
    )

    runtime_event_rows = [dict(row) for row in previous_state.get("runtime_event_rows") or []]
    for row in runtime_event_rows:
        if str(row.get("related_alert_key") or "") != normalized_key:
            continue
        row["review_state"] = normalized_review_state
        row["reviewed_at"] = generated_at.isoformat()
        row["reviewed_by"] = operator_label

    intents_rows = [dict(row) for row in previous_state.get("intents_rows") or []]
    fill_rows = [dict(row) for row in previous_state.get("fill_rows") or []]
    closed_position_rows = [dict(row) for row in previous_state.get("closed_position_rows") or []]
    reconciliation_rows = [dict(row) for row in previous_state.get("reconciliation_rows") or []]
    lane_rows = [dict(row) for row in runtime_state.get("lane_rows") or snapshot.get("lane_rows") or []]
    pending_rows = [dict(row) for row in runtime_state.get("pending_intents") or snapshot.get("pending_intents") or []]
    open_position_rows = [dict(row) for row in runtime_state.get("open_positions") or snapshot.get("open_positions") or []]
    bridge_mode = str(snapshot.get("bridge_mode") or BRIDGE_MODE_PROSPECTIVE)
    cycle_index = int(runtime_state.get("bridge_cycle_index") or snapshot.get("bridge_cycle_index") or 1)

    for lane_row in lane_rows:
        lane_id = str(lane_row.get("lane_id") or "")
        lane_alert_rows = [row for row in updated_alert_rows if str(row.get("lane_id") or "") == lane_id]
        lane_reconciliation_rows = [row for row in reconciliation_rows if str(row.get("lane_id") or "") == lane_id]
        lane_row["active_alert_count"] = sum(1 for row in lane_alert_rows if row.get("active"))
        lane_row["unreviewed_anomaly_count"] = sum(1 for row in lane_alert_rows if row.get("active") and row.get("acknowledged") is not True)
        lane_row["reconciliation_issue_count"] = sum(1 for row in lane_reconciliation_rows if row.get("clean") is not True)
        lane_row["operator_action_required"] = (
            int(lane_row.get("pending_intent_count") or 0) > 0
            or int(lane_row.get("open_position_count") or 0) > 0
            or int(lane_row.get("unreviewed_anomaly_count") or 0) > 0
            or int(lane_row.get("reconciliation_issue_count") or 0) > 0
        )

    summary = _build_summary(
        generated_at=generated_at,
        bridge_run_id=str(snapshot.get("bridge_run_id") or ""),
        lane_ids=_lane_ids_from_snapshot(snapshot),
        lane_rows=lane_rows,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        open_position_rows=open_position_rows,
        pending_rows=pending_rows,
        alert_rows=updated_alert_rows,
        reconciliation_rows=reconciliation_rows,
        bridge_mode=bridge_mode,
        pending_intent_count=len(pending_rows),
        runtime_event_rows=runtime_event_rows,
        cycle_index=cycle_index,
    )
    operator_status = _build_operator_status(summary=summary, bridge_mode=bridge_mode)
    cadence_state = dict(previous_state.get("cadence_state") or runtime_state.get("cadence_state") or snapshot.get("cadence") or {})
    if cadence_state:
        cadence_state["summary"] = dict(summary)
        cadence_state["last_reviewed_at"] = generated_at.isoformat()
        cadence_state["last_reviewed_anomaly_key"] = normalized_key

    updated_runtime_state = {
        **runtime_state,
        "generated_at": generated_at.isoformat(),
        "summary": summary,
        "operator_status": operator_status,
        "anomaly_summary": _build_anomaly_summary(alert_rows=updated_alert_rows, reconciliation_rows=reconciliation_rows),
        "anomaly_queue": _build_anomaly_queue(alert_rows=updated_alert_rows),
        "runtime_events": runtime_event_rows,
        "review_events": review_event_rows,
        "cadence_state": cadence_state,
    }
    rebuilt_snapshot = _build_snapshot(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=str(snapshot.get("bridge_run_id") or ""),
        bridge_mode=bridge_mode,
        selected_tenants=list(snapshot.get("selected_tenants") or []),
        selected_exclusions=list(snapshot.get("selected_exclusions") or []),
        summary=summary,
        operator_status=operator_status,
        lane_rows=lane_rows,
        pending_intents=pending_rows,
        recent_intents=intents_rows[:25],
        recent_fills=fill_rows[:25],
        recent_closed_positions=closed_position_rows[:25],
        open_positions=open_position_rows,
        alert_rows=updated_alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        source_truth=dict(snapshot.get("source_truth") or {}),
        cycle_policy=dict(snapshot.get("cycle_policy") or {}),
        cadence_state=cadence_state,
        review_event_rows=review_event_rows,
        timing=dict(snapshot.get("timing") or {}),
        cycle_index=cycle_index,
    )
    _write_runtime_bridge_outputs(
        output_dir=output_dir,
        generated_at=generated_at,
        bridge_run_id=str(snapshot.get("bridge_run_id") or ""),
        bridge_mode=bridge_mode,
        warehouse_root=Path(str((snapshot.get("source_truth") or {}).get("warehouse_root") or DEFAULT_WAREHOUSE_ROOT)),
        lane_ids=_lane_ids_from_snapshot(snapshot),
        summary=summary,
        operator_status=operator_status,
        runtime_state=updated_runtime_state,
        snapshot=rebuilt_snapshot,
        intents_rows=intents_rows,
        fill_rows=fill_rows,
        closed_position_rows=closed_position_rows,
        alert_rows=updated_alert_rows,
        reconciliation_rows=reconciliation_rows,
        runtime_event_rows=runtime_event_rows,
        review_event_rows=review_event_rows,
        cadence_state=cadence_state,
        db_path=output_dir / "runtime_bridge.sqlite3",
    )
    return {
        "output_dir": str(output_dir),
        "snapshot_path": str((output_dir / "bridge_snapshot.json").resolve()),
        "reviewed_anomaly_key": normalized_key,
        "review_state": normalized_review_state,
        "summary": summary,
    }


def _lane_ids_from_snapshot(snapshot: dict[str, Any]) -> tuple[str, ...]:
    lane_ids: list[str] = []
    for row in list(snapshot.get("selected_tenants") or []):
        for lane_id in list(dict(row).get("lane_ids") or []):
            normalized = str(lane_id or "").strip()
            if normalized and normalized not in lane_ids:
                lane_ids.append(normalized)
    if lane_ids:
        return tuple(lane_ids)
    return tuple(DEFAULT_SELECTED_LANES)


def _bridge_review_event_row(
    *,
    occurred_at: datetime,
    anomaly_row: dict[str, Any],
    operator_label: str,
    note: str | None,
    review_state: str,
) -> dict[str, Any]:
    return {
        "occurred_at": occurred_at.isoformat(),
        "review_contract_version": BRIDGE_OPERATOR_REVIEW_CONTRACT_VERSION,
        "review_event_id": _stable_hash(
            {
                "occurred_at": occurred_at.isoformat(),
                "review_key": anomaly_row.get("review_key") or anomaly_row.get("dedup_key"),
                "review_state": review_state,
                "operator_label": operator_label,
            },
            length=24,
        ),
        "review_key": anomaly_row.get("review_key") or anomaly_row.get("dedup_key"),
        "dedup_key": anomaly_row.get("dedup_key"),
        "review_state": review_state,
        "severity": anomaly_row.get("severity"),
        "classification": anomaly_row.get("classification"),
        "lane_id": anomaly_row.get("lane_id"),
        "strategy_family": anomaly_row.get("strategy_family"),
        "standalone_strategy_id": anomaly_row.get("standalone_strategy_id"),
        "operator_label": operator_label,
        "note": note,
        "paper_only": True,
        "live_execution_enabled": False,
    }


def _load_selected_lane_entries(*, warehouse_root: Path, lane_ids: Sequence[str]) -> list[dict[str, Any]]:
    lane_set = {str(item).strip() for item in lane_ids if str(item).strip()}
    dataset_root = warehouse_root / "datasets" / "lane_entries"
    if not dataset_root.exists():
        raise FileNotFoundError(f"Warehouse lane-entry dataset root not found: {dataset_root}")
    rows: list[dict[str, Any]] = []
    for parquet_path in sorted(dataset_root.glob("symbol=*/year=*/shard_id=*/entries.parquet")):
        for row in read_parquet_rows(parquet_path):
            lane_id = str(row.get("lane_id") or "").strip()
            if lane_id not in lane_set:
                continue
            rows.append({**row, "source_partition_path": str(parquet_path.resolve())})
    return sorted(rows, key=lambda row: (str(row.get("entry_ts") or ""), str(row.get("entry_id") or "")))


def _latest_entry_rows_by_lane(entry_rows: Sequence[dict[str, Any]], *, lane_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    allowed = {str(item) for item in lane_ids}
    for row in entry_rows:
        lane_id = str(row.get("lane_id") or "")
        if lane_id not in allowed:
            continue
        latest[lane_id] = row
    return latest


def _restore_lane_runtime_state(
    *,
    lane_engine: ExecutionEngine,
    lane_state: dict[str, Any],
    prior_pending_rows: Sequence[dict[str, Any]],
    prior_open_position: dict[str, Any] | None,
) -> None:
    broker = lane_engine.broker
    open_order_ids = [str(row.get("broker_order_id") or "") for row in prior_pending_rows if str(row.get("broker_order_id") or "")]
    order_status = {
        str(row.get("broker_order_id") or ""): OrderStatus(str(row.get("order_status") or OrderStatus.ACKNOWLEDGED.value))
        for row in prior_pending_rows
        if str(row.get("broker_order_id") or "")
    }
    position_qty = 0
    average_price: Decimal | None = None
    last_fill_timestamp: datetime | None = None
    if prior_open_position is not None:
        side = str(prior_open_position.get("side") or "FLAT").upper()
        position_qty = int(prior_open_position.get("quantity") or 1)
        if side == "SHORT":
            position_qty *= -1
        average_price = _as_decimal(prior_open_position.get("entry_price"))
        last_fill_timestamp = _as_datetime(prior_open_position.get("entry_ts"))
        lane_state.update(
            {
                "runtime_status": "IN_POSITION",
                "position_side": side,
                "expected_signed_quantity": position_qty,
                "internal_position_qty": abs(position_qty),
                "broker_position_qty": abs(position_qty),
                "average_price": str(average_price) if average_price is not None else None,
                "last_fill_timestamp": last_fill_timestamp.isoformat(),
                "open_position": dict(prior_open_position),
            }
        )
    broker.restore_state(
        position=broker.get_position().__class__(quantity=position_qty, average_price=average_price),
        open_order_ids=open_order_ids,
        order_status=order_status,
        last_fill_timestamp=last_fill_timestamp,
    )
    for pending_row in prior_pending_rows:
        pending_execution = _pending_execution_from_row(pending_row)
        lane_engine.restore_pending_execution(pending_execution)
        lane_state["last_order_intent_id"] = pending_execution.intent.order_intent_id
        phase = str(pending_row.get("phase") or "")
        lane_state["runtime_status"] = "PENDING_EXIT" if phase == "exit" else "PENDING_ENTRY"


def _pending_execution_from_row(row: dict[str, Any]) -> PendingExecution:
    intent = _intent_from_artifact_row(row)
    acknowledged_at = _as_datetime(row["acknowledged_at"]) if row.get("acknowledged_at") else None
    last_status_checked_at = _as_datetime(row["last_status_checked_at"]) if row.get("last_status_checked_at") else None
    return PendingExecution(
        intent=intent,
        broker_order_id=str(row.get("broker_order_id") or f"paper-{intent.order_intent_id}"),
        submitted_at=_as_datetime(row.get("created_at") or row.get("submitted_at")),
        acknowledged_at=acknowledged_at,
        broker_order_status=str(row.get("broker_order_status") or row.get("order_status") or ""),
        last_status_checked_at=last_status_checked_at,
        retry_count=int(row.get("retry_count") or 0),
        signal_bar_id=str(row.get("bar_id") or ""),
        long_entry_family=LongEntryFamily.NONE,
        short_entry_family=ShortEntryFamily.NONE,
        short_entry_source=None,
    )


def _intent_from_artifact_row(row: dict[str, Any]) -> OrderIntent:
    return OrderIntent(
        order_intent_id=str(row["order_intent_id"]),
        bar_id=str(row.get("bar_id") or ""),
        symbol=str(row["symbol"]),
        intent_type=OrderIntentType(str(row["intent_type"])),
        quantity=int(row.get("quantity") or 1),
        created_at=_as_datetime(row["created_at"]),
        reason_code=str(row["reason_code"]),
    )


def _build_prospective_entry_intent(entry_row: dict[str, Any]) -> OrderIntent:
    side = str(entry_row["side"]).upper()
    intent_type = OrderIntentType.BUY_TO_OPEN if side == "LONG" else OrderIntentType.SELL_TO_OPEN
    return OrderIntent(
        order_intent_id=f"{entry_row['entry_id']}:prospective_entry_intent",
        bar_id=f"{entry_row['entry_id']}:prospective_entry_bar",
        symbol=str(entry_row["symbol"]),
        intent_type=intent_type,
        quantity=1,
        created_at=_as_datetime(entry_row["entry_ts"]),
        reason_code=f"prospective_research_bridge_entry:{entry_row['lane_id']}",
    )


def _build_prospective_exit_intent(trade_row: dict[str, Any]) -> OrderIntent:
    side = str(trade_row["side"]).upper()
    intent_type = OrderIntentType.SELL_TO_CLOSE if side == "LONG" else OrderIntentType.BUY_TO_CLOSE
    return OrderIntent(
        order_intent_id=f"{trade_row['trade_id']}:prospective_exit_intent",
        bar_id=f"{trade_row['trade_id']}:prospective_exit_bar",
        symbol=str(trade_row["symbol"]),
        intent_type=intent_type,
        quantity=1,
        created_at=_as_datetime(trade_row["exit_ts"]),
        reason_code=f"prospective_research_bridge_exit:{trade_row['lane_id']}:{trade_row.get('exit_reason') or 'exit'}",
    )


def _prospective_intent_artifact_row(
    *,
    bridge_run_id: str,
    bridge_cycle_index: int,
    runtime_identity: dict[str, Any],
    intent: OrderIntent,
    pending_execution: PendingExecution,
    phase: str,
    source_row: dict[str, Any],
    source_dataset: str,
    lifecycle_state: str,
) -> dict[str, Any]:
    planned_fill_price = source_row.get("entry_price") if phase == "entry" else source_row.get("exit_price")
    planned_fill_ts = source_row.get("entry_ts") if phase == "entry" else source_row.get("exit_ts")
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "phase": phase,
        "lifecycle_state": lifecycle_state,
        "emitted_cycle": bridge_cycle_index,
        "age_cycles": 0,
        "stale_after_cycles": PROSPECTIVE_STALE_AFTER_CYCLES,
        "expire_after_cycles": PROSPECTIVE_EXPIRE_AFTER_CYCLES,
        "blocked_by": None,
        "last_cycle_seen": bridge_cycle_index,
        "next_transition_hint": "AWAIT_ENTRY_FILL" if phase == "entry" else "AWAIT_EXIT_FILL",
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "order_intent_id": intent.order_intent_id,
        "bar_id": intent.bar_id,
        "symbol": intent.symbol,
        "intent_type": intent.intent_type.value,
        "quantity": intent.quantity,
        "created_at": intent.created_at.isoformat(),
        "acknowledged_at": _as_datetime(pending_execution.acknowledged_at or intent.created_at).isoformat(),
        "reason_code": intent.reason_code,
        "order_status": OrderStatus.ACKNOWLEDGED.value,
        "broker_order_id": pending_execution.broker_order_id,
        "broker_order_status": str(pending_execution.broker_order_status or OrderStatus.ACKNOWLEDGED.value),
        "last_status_checked_at": _as_datetime(pending_execution.last_status_checked_at or intent.created_at).isoformat(),
        "retry_count": int(pending_execution.retry_count),
        "planned_fill_price": planned_fill_price,
        "fill_due_at": _as_datetime(planned_fill_ts).isoformat() if planned_fill_ts is not None else intent.created_at.isoformat(),
        "research_trade_id": source_row.get("trade_id"),
        "research_candidate_id": source_row.get("candidate_id"),
        "research_entry_id": source_row.get("entry_id"),
        "research_side": source_row.get("side"),
        "source_dataset": source_dataset,
        "source_partition_path": source_row.get("source_partition_path"),
        "source_provenance_tag": source_row.get("provenance_tag"),
        "entry_price": source_row.get("entry_price"),
        "exit_price": source_row.get("exit_price"),
    }


def _prospective_fill_artifact_row(
    *,
    bridge_run_id: str,
    runtime_identity: dict[str, Any],
    intent: OrderIntent,
    fill,
    source_row: dict[str, Any],
    source_dataset: str,
    phase: str,
) -> dict[str, Any]:
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "phase": phase,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "order_intent_id": intent.order_intent_id,
        "broker_order_id": fill.broker_order_id,
        "intent_type": intent.intent_type.value,
        "fill_timestamp": fill.fill_timestamp.isoformat(),
        "fill_price": str(fill.fill_price) if fill.fill_price is not None else None,
        "quantity": intent.quantity,
        "order_status": fill.order_status.value,
        "research_trade_id": source_row.get("trade_id"),
        "research_candidate_id": source_row.get("candidate_id"),
        "research_entry_id": source_row.get("entry_id"),
        "research_side": source_row.get("side"),
        "source_dataset": source_dataset,
        "source_partition_path": source_row.get("source_partition_path"),
        "source_provenance_tag": source_row.get("provenance_tag"),
    }


def _prospective_open_position_row(
    *,
    bridge_run_id: str,
    runtime_identity: dict[str, Any],
    source_entry_row: dict[str, Any],
    matching_closed_trade: dict[str, Any] | None,
    entry_fill,
) -> dict[str, Any]:
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "trade_id": matching_closed_trade.get("trade_id") if matching_closed_trade is not None else None,
        "research_trade_id": matching_closed_trade.get("trade_id") if matching_closed_trade is not None else None,
        "research_entry_id": source_entry_row.get("entry_id"),
        "research_candidate_id": source_entry_row.get("candidate_id"),
        "source_dataset": "lane_entries",
        "source_partition_path": source_entry_row.get("source_partition_path"),
        "source_provenance_tag": source_entry_row.get("provenance_tag"),
        "side": source_entry_row.get("side"),
        "entry_ts": entry_fill.fill_timestamp.isoformat(),
        "entry_price": float(source_entry_row.get("entry_price") or 0.0),
        "quantity": 1,
        "expected_exit_ts": matching_closed_trade.get("exit_ts") if matching_closed_trade is not None else None,
        "expected_exit_reason": matching_closed_trade.get("exit_reason") if matching_closed_trade is not None else None,
    }


def _runtime_event_row(
    *,
    occurred_at: datetime,
    runtime_identity: dict[str, Any],
    severity: str,
    event_type: str,
    classification: str,
    message: str,
    lane_id: str,
    order_intent_id: str | None = None,
    research_entry_id: str | None = None,
    research_trade_id: str | None = None,
    related_alert_key: str | None = None,
) -> dict[str, Any]:
    payload = {
        "occurred_at": occurred_at.isoformat(),
        "event_type": event_type,
        "classification": classification,
        "severity": severity,
        "message": message,
        "lane_id": lane_id,
        "strategy_family": runtime_identity["strategy_family"],
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "bridge_mode": BRIDGE_MODE_PROSPECTIVE,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "review_state": "INFO_ONLY" if severity.upper() == "INFO" else "UNREVIEWED",
        "event_id": _stable_hash(
            {
                "occurred_at": occurred_at.isoformat(),
                "event_type": event_type,
                "classification": classification,
                "lane_id": lane_id,
                "order_intent_id": order_intent_id,
                "research_entry_id": research_entry_id,
                "research_trade_id": research_trade_id,
                "related_alert_key": related_alert_key,
            },
            length=24,
        ),
    }
    if order_intent_id:
        payload["order_intent_id"] = order_intent_id
    if research_entry_id:
        payload["research_entry_id"] = research_entry_id
    if research_trade_id:
        payload["research_trade_id"] = research_trade_id
    if related_alert_key:
        payload["related_alert_key"] = related_alert_key
    return payload


def _build_snapshot(
    *,
    output_dir: Path,
    generated_at: datetime,
    bridge_run_id: str,
    bridge_mode: str,
    selected_tenants: list[dict[str, Any]],
    selected_exclusions: list[str],
    summary: dict[str, Any],
    operator_status: dict[str, Any],
    lane_rows: Sequence[dict[str, Any]],
    pending_intents: Sequence[dict[str, Any]],
    recent_intents: Sequence[dict[str, Any]],
    recent_fills: Sequence[dict[str, Any]],
    recent_closed_positions: Sequence[dict[str, Any]],
    open_positions: Sequence[dict[str, Any]],
    alert_rows: Sequence[dict[str, Any]],
    reconciliation_rows: Sequence[dict[str, Any]],
    runtime_event_rows: Sequence[dict[str, Any]],
    source_truth: dict[str, Any],
    cycle_policy: dict[str, Any] | None,
    cadence_state: dict[str, Any] | None,
    review_event_rows: Sequence[dict[str, Any]],
    timing: dict[str, Any],
    cycle_index: int,
) -> dict[str, Any]:
    anomaly_summary = _build_anomaly_summary(alert_rows=alert_rows, reconciliation_rows=reconciliation_rows)
    anomaly_queue = _build_anomaly_queue(alert_rows=alert_rows)
    return {
        "available": True,
        "generated_at": generated_at.isoformat(),
        "contract_version": BRIDGE_CONTRACT_VERSION,
        "operating_policy_version": BRIDGE_OPERATING_POLICY_VERSION,
        "runtime_event_contract_version": BRIDGE_RUNTIME_EVENT_CONTRACT_VERSION,
        "bridge_run_id": bridge_run_id,
        "bridge_cycle_index": cycle_index,
        "bridge_mode": bridge_mode,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "live_execution_enabled": False,
        "selected_tenants": selected_tenants,
        "selected_exclusions": selected_exclusions,
        "summary": summary,
        "operator_status": operator_status,
        "cycle_policy": dict(cycle_policy or {}),
        "cadence": dict(cadence_state or {}),
        "lane_rows": list(lane_rows),
        "pending_intents": list(pending_intents)[:25],
        "recent_intents": list(recent_intents),
        "recent_fills": list(recent_fills),
        "recent_closed_positions": list(recent_closed_positions),
        "open_positions": list(open_positions),
        "alerts": {
            "active_rows": [row for row in alert_rows if row.get("active")],
            "recent_rows": list(alert_rows)[:25],
            "count": len(alert_rows),
        },
        "reconciliation": {
            "recent_rows": list(reconciliation_rows)[:25],
            "anomaly_rows": [row for row in reconciliation_rows if row.get("clean") is not True][:25],
            "count": len(reconciliation_rows),
        },
        "runtime_events": {
            "recent_rows": list(runtime_event_rows)[:25],
            "count": len(runtime_event_rows),
            "severity_counts": _severity_counts(runtime_event_rows),
        },
        "operator_reviews": {
            "recent_rows": list(review_event_rows)[:25],
            "count": len(review_event_rows),
        },
        "anomalies": anomaly_summary,
        "anomaly_queue": anomaly_queue,
        "artifacts": {
            "root_dir": str(output_dir),
            "db_path": str((output_dir / "runtime_bridge.sqlite3").resolve()),
            "snapshot_path": str((output_dir / "bridge_snapshot.json").resolve()),
            "operator_status_path": str((output_dir / "operator_status.json").resolve()),
            "runtime_state_path": str((output_dir / "runtime_state.json").resolve()),
            "cadence_state_path": str((output_dir / "cadence_state.json").resolve()),
            "intents_path": str((output_dir / "order_intents.jsonl").resolve()),
            "fills_path": str((output_dir / "fills.jsonl").resolve()),
            "trades_path": str((output_dir / "trades.jsonl").resolve()),
            "alerts_path": str((output_dir / "alerts.jsonl").resolve()),
            "reconciliation_path": str((output_dir / "reconciliation_events.jsonl").resolve()),
            "runtime_events_path": str((output_dir / "runtime_events.jsonl").resolve()),
            "operator_reviews_path": str((output_dir / "operator_review_events.jsonl").resolve()),
        },
        "source_truth": source_truth,
        "timing": timing,
    }


def _write_runtime_bridge_outputs(
    *,
    output_dir: Path,
    generated_at: datetime,
    bridge_run_id: str,
    bridge_mode: str,
    warehouse_root: Path,
    lane_ids: Sequence[str],
    summary: dict[str, Any],
    operator_status: dict[str, Any],
    runtime_state: dict[str, Any],
    snapshot: dict[str, Any],
    intents_rows: Sequence[dict[str, Any]],
    fill_rows: Sequence[dict[str, Any]],
    closed_position_rows: Sequence[dict[str, Any]],
    alert_rows: Sequence[dict[str, Any]],
    reconciliation_rows: Sequence[dict[str, Any]],
    runtime_event_rows: Sequence[dict[str, Any]],
    review_event_rows: Sequence[dict[str, Any]],
    cadence_state: dict[str, Any] | None,
    db_path: Path,
) -> None:
    _write_json(output_dir / "bridge_snapshot.json", snapshot)
    _write_json(output_dir / "operator_status.json", operator_status)
    _write_json(output_dir / "runtime_state.json", runtime_state)
    _write_json(output_dir / "cadence_state.json", dict(cadence_state or {}))
    _write_json(
        output_dir / "manifest.json",
        {
            "generated_at": generated_at.isoformat(),
            "bridge_run_id": bridge_run_id,
            "contract_version": BRIDGE_CONTRACT_VERSION,
            "operating_policy_version": BRIDGE_OPERATING_POLICY_VERSION,
            "bridge_mode": bridge_mode,
            "execution_mode": BRIDGE_EXECUTION_MODE,
            "paper_only": True,
            "live_execution_enabled": False,
            "warehouse_root": str(warehouse_root),
            "selected_lane_ids": list(lane_ids),
            "summary": summary,
            "operator_status": operator_status,
            "timing": snapshot["timing"],
            "cycle_policy": dict(snapshot.get("cycle_policy") or {}),
            "cadence": dict(snapshot.get("cadence") or {}),
            "anomaly_summary": dict(snapshot.get("anomalies") or {}),
            "db_path": str(db_path.resolve()),
        },
    )
    _write_jsonl(output_dir / "order_intents.jsonl", reversed(list(intents_rows)))
    _write_jsonl(output_dir / "fills.jsonl", reversed(list(fill_rows)))
    _write_jsonl(output_dir / "trades.jsonl", reversed(list(closed_position_rows)))
    _write_jsonl(output_dir / "alerts.jsonl", reversed(list(alert_rows)))
    _write_jsonl(output_dir / "reconciliation_events.jsonl", reversed(list(reconciliation_rows)))
    _write_jsonl(output_dir / "runtime_events.jsonl", reversed(list(runtime_event_rows)))
    _write_jsonl(output_dir / "operator_review_events.jsonl", reversed(list(review_event_rows)))


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl_if_exists(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _severity_counts(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    counts = {"INFO": 0, "WARN": 0, "ERROR": 0}
    for row in rows:
        severity = str(row.get("severity") or "INFO").upper()
        counts[severity] = counts.get(severity, 0) + 1
    return counts


def _bridge_run_is_settled(result: dict[str, Any]) -> bool:
    summary = dict(result.get("summary") or {})
    return (
        str(summary.get("status") or "") == "READY"
        and int(summary.get("pending_intent_count") or 0) == 0
        and int(summary.get("open_position_count") or 0) == 0
        and int(summary.get("active_alert_count") or 0) == 0
        and int(summary.get("reconciliation_issue_count") or 0) == 0
    )


def _bridge_blocked_reason(
    *,
    entries_enabled: bool,
    exits_enabled: bool,
    operator_halt: bool,
    summary: dict[str, Any],
) -> str | None:
    if operator_halt:
        return "OPERATOR_HALT"
    if not entries_enabled and int(summary.get("pending_entry_count") or 0) > 0:
        return "ENTRIES_DISABLED"
    if not exits_enabled and int(summary.get("pending_exit_count") or 0) > 0:
        return "EXITS_DISABLED"
    if int(summary.get("active_alert_count") or 0) > 0:
        return "ACTIVE_ALERTS_PRESENT"
    if int(summary.get("reconciliation_issue_count") or 0) > 0:
        return "RECONCILIATION_ISSUES_PRESENT"
    return None


def _bridge_cycle_state(*, blocked_reason: str | None, summary: dict[str, Any]) -> str:
    if blocked_reason is not None and int(summary.get("pending_intent_count") or 0) > 0:
        return "BLOCKED"
    if _bridge_run_is_settled({"summary": summary}):
        return "SETTLED"
    if int(summary.get("pending_intent_count") or 0) > 0 or int(summary.get("open_position_count") or 0) > 0:
        return "ADVANCING"
    if int(summary.get("active_alert_count") or 0) > 0 or int(summary.get("reconciliation_issue_count") or 0) > 0:
        return "ATTENTION_REQUIRED"
    return "IDLE"


def _build_cadence_state(
    *,
    previous_cadence_state: dict[str, Any],
    bridge_cycle_index: int,
    cycle_ordinal: int,
    requested_cycle_count: int,
    poll_interval_seconds: int,
    stop_when_settled: bool,
    generated_at: datetime,
    entries_enabled: bool,
    exits_enabled: bool,
    operator_halt: bool,
    cycle_state: str,
    summary: dict[str, Any] | None,
    blocked_reason: str | None,
    recovered_interrupted_cycle: bool,
) -> dict[str, Any]:
    previous_recovered_count = int(previous_cadence_state.get("recovered_interrupted_cycle_count") or 0)
    previous_blocked_count = int(previous_cadence_state.get("consecutive_blocked_cycles") or 0)
    next_cycle_not_before = (generated_at + timedelta(seconds=max(0, poll_interval_seconds))).isoformat()
    scheduler_mode = "LOCAL_INTERVAL_LOOP" if requested_cycle_count > 1 else "MANUAL_SINGLE_CYCLE"
    return {
        "contract_version": BRIDGE_CADENCE_CONTRACT_VERSION,
        "scheduler_mode": scheduler_mode,
        "cycle_state": cycle_state,
        "bridge_cycle_index": bridge_cycle_index,
        "cycle_ordinal": cycle_ordinal,
        "requested_cycle_count": requested_cycle_count,
        "poll_interval_seconds": max(0, poll_interval_seconds),
        "stop_when_settled": bool(stop_when_settled),
        "last_cycle_started_at": generated_at.isoformat(),
        "last_cycle_completed_at": None if cycle_state == "IN_PROGRESS" else generated_at.isoformat(),
        "next_cycle_not_before": next_cycle_not_before,
        "entries_enabled": entries_enabled,
        "exits_enabled": exits_enabled,
        "operator_halt": operator_halt,
        "blocked_reason": blocked_reason,
        "consecutive_blocked_cycles": previous_blocked_count + 1 if cycle_state == "BLOCKED" else 0,
        "recovered_interrupted_cycle_count": previous_recovered_count + (1 if recovered_interrupted_cycle else 0),
        "recovered_interrupted_cycle": bool(recovered_interrupted_cycle),
        "last_transition": cycle_state.lower(),
        "last_transition_message": (
            "Bridge cycle started and is persisting in-progress cadence state."
            if cycle_state == "IN_PROGRESS"
            else f"Bridge cycle completed in {cycle_state.lower()} state."
        ),
        "summary": dict(summary or {}),
    }


def _load_selected_closed_trades(*, warehouse_root: Path, lane_ids: Sequence[str]) -> list[dict[str, Any]]:
    lane_set = {str(item).strip() for item in lane_ids if str(item).strip()}
    dataset_root = warehouse_root / "datasets" / "lane_closed_trades"
    if not dataset_root.exists():
        raise FileNotFoundError(f"Warehouse closed-trade dataset root not found: {dataset_root}")
    rows: list[dict[str, Any]] = []
    for parquet_path in sorted(dataset_root.glob("symbol=*/year=*/shard_id=*/closed_trades.parquet")):
        for row in read_parquet_rows(parquet_path):
            lane_id = str(row.get("lane_id") or "").strip()
            if lane_id not in lane_set:
                continue
            rows.append(
                {
                    **row,
                    "source_partition_path": str(parquet_path.resolve()),
                }
            )
    return sorted(rows, key=lambda row: (str(row.get("entry_ts") or ""), str(row.get("trade_id") or "")))


def _runtime_identity_for_lane(lane_id: str) -> dict[str, Any]:
    instrument = lane_id.rsplit("__", 1)[-1] if "__" in lane_id else ""
    return {
        "standalone_strategy_id": f"research_runtime_bridge::{lane_id}",
        "strategy_family": "warehouse_historical_evaluator",
        "instrument": instrument,
        "lane_id": lane_id,
    }


def _initial_lane_state(*, lane_id: str, lane_label: str, runtime_identity: dict[str, Any]) -> dict[str, Any]:
    del lane_label
    return {
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "lane_id": lane_id,
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "runtime_status": "READY",
        "position_side": "FLAT",
        "expected_signed_quantity": 0,
        "internal_position_qty": 0,
        "broker_position_qty": 0,
        "average_price": None,
        "last_fill_timestamp": None,
        "last_order_intent_id": None,
        "entries_enabled": True,
        "exits_enabled": True,
        "operator_halt": False,
        "reconcile_required": False,
        "fault_code": None,
        "operator_action_required": False,
        "latest_reconciliation_classification": RECONCILIATION_CLASS_CLEAN,
        "open_position": None,
    }


def _build_intent(*, trade_row: dict[str, Any], side: str, phase: str, quantity: int) -> OrderIntent:
    created_at = _as_datetime(trade_row["entry_ts"] if phase == "entry" else trade_row["exit_ts"])
    if phase == "entry":
        intent_type = OrderIntentType.BUY_TO_OPEN if side == "LONG" else OrderIntentType.SELL_TO_OPEN
        reason_code = f"research_bridge_entry:{trade_row['lane_id']}"
    else:
        intent_type = OrderIntentType.SELL_TO_CLOSE if side == "LONG" else OrderIntentType.BUY_TO_CLOSE
        reason_code = f"research_bridge_exit:{trade_row['lane_id']}:{trade_row.get('exit_reason') or 'exit'}"
    return OrderIntent(
        order_intent_id=f"{trade_row['trade_id']}:{phase}_intent",
        bar_id=f"{trade_row['trade_id']}:{phase}_bar",
        symbol=str(trade_row["symbol"]),
        intent_type=intent_type,
        quantity=quantity,
        created_at=created_at,
        reason_code=reason_code,
    )


def _apply_entry_fill_to_lane_state(*, lane_state: dict[str, Any], trade_row: dict[str, Any], entry_intent: OrderIntent) -> None:
    side = str(trade_row["side"]).upper()
    position_side = "LONG" if side == "LONG" else "SHORT"
    entry_price = _as_decimal(trade_row["entry_price"])
    lane_state.update(
        {
            "runtime_status": "IN_POSITION",
            "position_side": position_side,
            "expected_signed_quantity": 1 if side == "LONG" else -1,
            "internal_position_qty": 1,
            "broker_position_qty": 1,
            "average_price": str(entry_price),
            "last_fill_timestamp": _as_datetime(trade_row["entry_ts"]).isoformat(),
            "last_order_intent_id": entry_intent.order_intent_id,
            "open_position": {
                "trade_id": trade_row["trade_id"],
                "lane_id": trade_row["lane_id"],
                "strategy_family": "warehouse_historical_evaluator",
                "instrument": trade_row["symbol"],
                "side": side,
                "entry_ts": _as_datetime(trade_row["entry_ts"]).isoformat(),
                "entry_price": float(trade_row["entry_price"]),
                "quantity": 1,
                "research_trade_id": trade_row["trade_id"],
                "source_dataset": "lane_closed_trades",
                "source_partition_path": trade_row["source_partition_path"],
            },
        }
    )


def _apply_exit_fill_to_lane_state(*, lane_state: dict[str, Any], trade_row: dict[str, Any], exit_intent: OrderIntent) -> None:
    lane_state.update(
        {
            "runtime_status": "READY",
            "position_side": "FLAT",
            "expected_signed_quantity": 0,
            "internal_position_qty": 0,
            "broker_position_qty": 0,
            "average_price": None,
            "last_fill_timestamp": _as_datetime(trade_row["exit_ts"]).isoformat(),
            "last_order_intent_id": exit_intent.order_intent_id,
            "open_position": None,
            "reconcile_required": False,
        }
    )


def _intent_artifact_row(
    *,
    bridge_run_id: str,
    runtime_identity: dict[str, Any],
    intent: OrderIntent,
    broker_order_id: str | None,
    broker_order_status: str,
    acknowledged_at: datetime,
    research_trade: dict[str, Any],
    phase: str,
) -> dict[str, Any]:
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "phase": phase,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "order_intent_id": intent.order_intent_id,
        "bar_id": intent.bar_id,
        "symbol": intent.symbol,
        "intent_type": intent.intent_type.value,
        "quantity": intent.quantity,
        "created_at": intent.created_at.isoformat(),
        "acknowledged_at": acknowledged_at.isoformat(),
        "reason_code": intent.reason_code,
        "order_status": OrderStatus.FILLED.value,
        "broker_order_id": broker_order_id,
        "broker_order_status": broker_order_status,
        "research_trade_id": research_trade["trade_id"],
        "research_candidate_id": research_trade.get("candidate_id"),
        "research_entry_id": research_trade.get("entry_id"),
        "research_side": research_trade.get("side"),
        "source_dataset": "lane_closed_trades",
        "source_partition_path": research_trade["source_partition_path"],
        "source_provenance_tag": research_trade.get("provenance_tag"),
    }


def _fill_artifact_row(
    *,
    bridge_run_id: str,
    runtime_identity: dict[str, Any],
    intent: OrderIntent,
    fill_timestamp: datetime,
    fill_price: Decimal,
    broker_order_id: str | None,
    research_trade: dict[str, Any],
    phase: str,
) -> dict[str, Any]:
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "phase": phase,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "order_intent_id": intent.order_intent_id,
        "broker_order_id": broker_order_id,
        "intent_type": intent.intent_type.value,
        "fill_timestamp": fill_timestamp.isoformat(),
        "fill_price": str(fill_price),
        "quantity": intent.quantity,
        "order_status": OrderStatus.FILLED.value,
        "research_trade_id": research_trade["trade_id"],
        "research_candidate_id": research_trade.get("candidate_id"),
        "research_side": research_trade.get("side"),
        "source_dataset": "lane_closed_trades",
        "source_partition_path": research_trade["source_partition_path"],
        "source_provenance_tag": research_trade.get("provenance_tag"),
    }


def _closed_position_row(
    *,
    bridge_run_id: str,
    runtime_identity: dict[str, Any],
    trade_row: dict[str, Any],
    entry_intent: OrderIntent,
    exit_intent: OrderIntent,
) -> dict[str, Any]:
    return {
        "bridge_run_id": bridge_run_id,
        "bridge_mode": BRIDGE_MODE_REPLAY,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "lane_id": runtime_identity["lane_id"],
        "trade_id": trade_row["trade_id"],
        "research_trade_id": trade_row["trade_id"],
        "research_candidate_id": trade_row.get("candidate_id"),
        "source_dataset": "lane_closed_trades",
        "source_partition_path": trade_row["source_partition_path"],
        "source_provenance_tag": trade_row.get("provenance_tag"),
        "side": trade_row["side"],
        "entry_ts": _as_datetime(trade_row["entry_ts"]).isoformat(),
        "exit_ts": _as_datetime(trade_row["exit_ts"]).isoformat(),
        "entry_price": float(trade_row["entry_price"]),
        "exit_price": float(trade_row["exit_price"]),
        "quantity": 1,
        "point_value": float(trade_row.get("point_value") or 0.0),
        "realized_pnl_cash": round(float(trade_row.get("pnl") or 0.0), 6),
        "pnl_points": round(float(trade_row.get("pnl_points") or 0.0), 6),
        "exit_reason": trade_row.get("exit_reason"),
        "hold_minutes": int(trade_row.get("hold_minutes") or 0),
        "entry_order_intent_id": entry_intent.order_intent_id,
        "exit_order_intent_id": exit_intent.order_intent_id,
    }


def _bridge_alert_row(
    *,
    occurred_at: datetime,
    severity: str,
    title: str,
    message: str,
    lane_id: str,
    runtime_identity: dict[str, Any],
    trade_row: dict[str, Any] | None,
    dedup_key: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "occurred_at": occurred_at.isoformat(),
        "category": "research_runtime_bridge",
        "severity": severity,
        "title": title,
        "message": message,
        "source_subsystem": "research_runtime_bridge",
        "dedup_key": dedup_key,
        "review_key": dedup_key,
        "active": severity in {"ERROR", "WARN"},
        "acknowledged": False,
        "acknowledged_at": None,
        "acknowledged_by": None,
        "acknowledgement_note": None,
        "reviewed_at": None,
        "reviewed_by": None,
        "review_note": None,
        "last_review_action": None,
        "review_state": "UNREVIEWED" if severity in {"ERROR", "WARN"} else "INFO_ONLY",
        "review_required": severity == "ERROR",
        "action_required": severity in {"ERROR", "WARN"},
        "lane_id": lane_id,
        "standalone_strategy_id": runtime_identity["standalone_strategy_id"],
        "strategy_family": runtime_identity["strategy_family"],
        "instrument": runtime_identity["instrument"],
        "paper_only": True,
        "live_execution_enabled": False,
    }
    if trade_row is not None:
        payload.update(
            {
                "research_trade_id": trade_row.get("trade_id"),
                "source_partition_path": trade_row.get("source_partition_path"),
            }
        )
    if extra:
        payload.update(extra)
    return payload


def _carry_forward_prior_alert_state(
    *,
    alert_row: dict[str, Any],
    prior_alert_row: dict[str, Any] | None,
    generated_at: datetime,
) -> dict[str, Any]:
    if prior_alert_row is None:
        carried = dict(alert_row)
        carried["first_detected_at"] = carried.get("occurred_at")
        carried["last_seen_at"] = carried.get("occurred_at")
        carried["occurrence_count"] = int(carried.get("occurrence_count") or 0) + 1
        carried["escalated"] = bool(carried.get("escalated"))
        return carried

    recurrence_detected = bool(prior_alert_row.get("active") is False and alert_row.get("active") is True)
    carried = {
        **dict(prior_alert_row),
        **dict(alert_row),
        "occurred_at": prior_alert_row.get("occurred_at") or alert_row.get("occurred_at"),
        "first_detected_at": prior_alert_row.get("first_detected_at") or prior_alert_row.get("occurred_at") or alert_row.get("occurred_at"),
        "last_seen_at": generated_at.isoformat(),
        "occurrence_count": int(prior_alert_row.get("occurrence_count") or 1) + 1,
        "acknowledged": False if recurrence_detected else bool(prior_alert_row.get("acknowledged")),
        "acknowledged_at": None if recurrence_detected else prior_alert_row.get("acknowledged_at"),
        "acknowledged_by": None if recurrence_detected else prior_alert_row.get("acknowledged_by"),
        "acknowledgement_note": None if recurrence_detected else prior_alert_row.get("acknowledgement_note"),
        "reviewed_at": None if recurrence_detected else prior_alert_row.get("reviewed_at"),
        "reviewed_by": None if recurrence_detected else prior_alert_row.get("reviewed_by"),
        "review_note": None if recurrence_detected else prior_alert_row.get("review_note"),
        "resolved_at": None if recurrence_detected else prior_alert_row.get("resolved_at"),
        "resolved_by": None if recurrence_detected else prior_alert_row.get("resolved_by"),
        "resolution_note": None if recurrence_detected else prior_alert_row.get("resolution_note"),
        "last_review_action": None if recurrence_detected else prior_alert_row.get("last_review_action"),
        "review_state": "UNREVIEWED" if recurrence_detected else (prior_alert_row.get("review_state") or alert_row.get("review_state")),
        "review_required": True if recurrence_detected else prior_alert_row.get("review_required", alert_row.get("review_required")),
        "action_required": True if recurrence_detected else prior_alert_row.get("action_required", alert_row.get("action_required")),
        "active": alert_row.get("active", prior_alert_row.get("active")),
        "escalated": False if recurrence_detected else bool(prior_alert_row.get("escalated")),
        "escalated_at": None if recurrence_detected else prior_alert_row.get("escalated_at"),
        "recurrence_detected": recurrence_detected,
    }
    return carried


def _finalize_lane_alerts(
    *,
    alerts: Sequence[dict[str, Any]],
    prior_alerts_by_key: dict[str, dict[str, Any]],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    finalized: list[dict[str, Any]] = []
    for row in alerts:
        dedup_key = str(row.get("dedup_key") or row.get("review_key") or "")
        prior_row = prior_alerts_by_key.get(dedup_key) if dedup_key else None
        merged = _carry_forward_prior_alert_state(
            alert_row=row,
            prior_alert_row=prior_row,
            generated_at=generated_at,
        )
        pending_age_cycles = int(merged.get("pending_age_cycles") or 0)
        review_state = str(merged.get("review_state") or "UNREVIEWED").upper()
        if (
            str(merged.get("classification") or "").upper() == "STALE_PENDING_INTENT"
            and merged.get("active")
            and review_state in {"ACKNOWLEDGED", "REVIEWED"}
            and pending_age_cycles >= PROSPECTIVE_UNRESOLVED_ESCALATE_AFTER_CYCLES
        ):
            merged.update(
                {
                    "severity": "ERROR",
                    "title": "Unresolved pending intent escalated",
                    "message": "A pending paper intent remained unresolved after acknowledgement across multiple cycles and now needs explicit operator resolution.",
                    "classification": "UNRESOLVED_PENDING_INTENT_ESCALATED",
                    "review_required": True,
                    "action_required": True,
                    "escalated": True,
                    "escalated_at": generated_at.isoformat(),
                }
            )
        finalized.append(merged)
    return finalized


def _build_anomaly_summary(
    *,
    alert_rows: Sequence[dict[str, Any]],
    reconciliation_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    severity_counts = {"WARN": 0, "ERROR": 0}
    counts_by_classification: dict[str, int] = {}
    review_state_counts = {"UNREVIEWED": 0, "ACKNOWLEDGED": 0, "REVIEWED": 0, "RESOLVED": 0, "INFO_ONLY": 0}
    unreviewed_count = 0
    unresolved_count = 0
    needs_attention_now_count = 0
    resolved_count = 0
    escalated_count = 0
    acknowledged_pending_count = 0
    reviewed_pending_count = 0
    for row in alert_rows:
        severity = str(row.get("severity") or "").upper()
        if severity in severity_counts:
            severity_counts[severity] += 1
        classification = str(row.get("classification") or row.get("title") or "UNCLASSIFIED").strip() or "UNCLASSIFIED"
        counts_by_classification[classification] = counts_by_classification.get(classification, 0) + 1
        review_state = str(row.get("review_state") or "UNREVIEWED").upper()
        review_state_counts[review_state] = review_state_counts.get(review_state, 0) + 1
        active = bool(row.get("active"))
        escalated = bool(row.get("escalated"))
        unresolved = active and review_state != "RESOLVED"
        needs_attention_now = unresolved and (review_state == "UNREVIEWED" or escalated)
        if unresolved:
            unresolved_count += 1
        if needs_attention_now:
            needs_attention_now_count += 1
        if unresolved and review_state == "ACKNOWLEDGED" and not escalated:
            acknowledged_pending_count += 1
        if unresolved and review_state == "REVIEWED" and not escalated:
            reviewed_pending_count += 1
        if review_state == "RESOLVED" or active is False:
            resolved_count += 1
        if escalated:
            escalated_count += 1
        if active and row.get("acknowledged") is not True and review_state not in {"REVIEWED", "RESOLVED", "INFO_ONLY"}:
            unreviewed_count += 1
    reconciliation_issue_count = sum(1 for row in reconciliation_rows if row.get("clean") is not True)
    return {
        "active_count": sum(1 for row in alert_rows if row.get("active")),
        "unreviewed_count": unreviewed_count,
        "unresolved_count": unresolved_count,
        "needs_attention_now_count": needs_attention_now_count,
        "acknowledged_count": sum(1 for row in alert_rows if row.get("acknowledged") is True),
        "resolved_count": resolved_count,
        "escalated_count": escalated_count,
        "acknowledged_pending_count": acknowledged_pending_count,
        "reviewed_pending_count": reviewed_pending_count,
        "severity_counts": severity_counts,
        "counts_by_classification": counts_by_classification,
        "review_state_counts": review_state_counts,
        "reconciliation_issue_count": reconciliation_issue_count,
    }


def _build_anomaly_queue(*, alert_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    review_rank = {"UNREVIEWED": 0, "ACKNOWLEDGED": 1, "REVIEWED": 2, "RESOLVED": 3, "INFO_ONLY": 4}
    severity_rank = {"ERROR": 0, "WARN": 1, "INFO": 2}
    queue_rows: list[dict[str, Any]] = []
    for row in alert_rows:
        severity = str(row.get("severity") or "INFO").upper()
        review_state = str(row.get("review_state") or ("INFO_ONLY" if severity == "INFO" else "UNREVIEWED")).upper()
        active = bool(row.get("active"))
        unresolved = active and review_state != "RESOLVED"
        escalated = bool(row.get("escalated"))
        needs_attention_now = unresolved and (review_state == "UNREVIEWED" or escalated)
        queue_status = (
            "ESCALATED"
            if unresolved and escalated
            else "NEEDS_ATTENTION_NOW"
            if unresolved and review_state == "UNREVIEWED"
            else "ACKNOWLEDGED_PENDING"
            if unresolved and review_state == "ACKNOWLEDGED"
            else "REVIEWED_PENDING"
            if unresolved and review_state == "REVIEWED"
            else "MONITORING"
            if unresolved
            else "RESOLVED"
        )
        if queue_status == "ESCALATED":
            actionability_reason = "This anomaly stayed unresolved across additional cadence cycles after operator review activity and now needs explicit resolution."
        elif queue_status == "NEEDS_ATTENTION_NOW":
            actionability_reason = "This anomaly is active and has not yet been acknowledged or reviewed by the operator."
        elif queue_status == "ACKNOWLEDGED_PENDING":
            actionability_reason = "The operator acknowledged this anomaly, but the underlying condition still persists and is being monitored."
        elif queue_status == "REVIEWED_PENDING":
            actionability_reason = "The operator reviewed this anomaly, but the underlying condition still persists and remains unresolved."
        elif queue_status == "RESOLVED":
            actionability_reason = "This anomaly is resolved or no longer active in the current paper-runtime state."
        else:
            actionability_reason = "This anomaly is being monitored in the background."
        queue_rows.append(
            {
                **dict(row),
                "severity": severity,
                "review_state": review_state,
                "active": active,
                "unresolved": unresolved,
                "needs_attention_now": needs_attention_now,
                "escalated": escalated,
                "action_required_now": queue_status in {"ESCALATED", "NEEDS_ATTENTION_NOW"},
                "queue_status": queue_status,
                "queue_status_detail": actionability_reason,
                "review_state_detail": (
                    "Operator review has not been recorded yet."
                    if review_state == "UNREVIEWED"
                    else "Operator acknowledgement was recorded; resolution is still pending."
                    if review_state == "ACKNOWLEDGED"
                    else "Operator review was recorded; resolution is still pending."
                    if review_state == "REVIEWED"
                    else "This anomaly was marked resolved by the operator."
                    if review_state == "RESOLVED"
                    else "This event is informational only."
                ),
            }
        )
    queue_rows.sort(
        key=lambda row: (
            0 if row.get("queue_status") == "ESCALATED" else 1,
            0 if row.get("queue_status") == "NEEDS_ATTENTION_NOW" else 1,
            0 if row.get("unresolved") else 1,
            severity_rank.get(str(row.get("severity") or "INFO").upper(), 9),
            review_rank.get(str(row.get("review_state") or "UNREVIEWED").upper(), 9),
            str(row.get("occurred_at") or ""),
        ),
        reverse=False,
    )
    return {
        "contract_version": BRIDGE_ANOMALY_QUEUE_CONTRACT_VERSION,
        "count": len(queue_rows),
        "needs_attention_now_count": sum(1 for row in queue_rows if row.get("needs_attention_now")),
        "unresolved_count": sum(1 for row in queue_rows if row.get("unresolved")),
        "review_pending_count": sum(1 for row in queue_rows if str(row.get("review_state") or "") == "UNREVIEWED"),
        "acknowledged_count": sum(1 for row in queue_rows if str(row.get("review_state") or "") == "ACKNOWLEDGED"),
        "acknowledged_pending_count": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "ACKNOWLEDGED_PENDING"),
        "reviewed_count": sum(1 for row in queue_rows if str(row.get("review_state") or "") == "REVIEWED"),
        "reviewed_pending_count": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "REVIEWED_PENDING"),
        "resolved_count": sum(1 for row in queue_rows if str(row.get("review_state") or "") == "RESOLVED"),
        "escalated_count": sum(1 for row in queue_rows if bool(row.get("escalated"))),
        "counts_by_queue_status": {
            "NEEDS_ATTENTION_NOW": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "NEEDS_ATTENTION_NOW"),
            "ACKNOWLEDGED_PENDING": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "ACKNOWLEDGED_PENDING"),
            "REVIEWED_PENDING": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "REVIEWED_PENDING"),
            "ESCALATED": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "ESCALATED"),
            "RESOLVED": sum(1 for row in queue_rows if str(row.get("queue_status") or "") == "RESOLVED"),
        },
        "recent_rows": queue_rows[:25],
    }


def _build_summary(
    *,
    generated_at: datetime,
    bridge_run_id: str,
    lane_ids: Sequence[str],
    lane_rows: Sequence[dict[str, Any]],
    intents_rows: Sequence[dict[str, Any]],
    fill_rows: Sequence[dict[str, Any]],
    closed_position_rows: Sequence[dict[str, Any]],
    open_position_rows: Sequence[dict[str, Any]],
    pending_rows: Sequence[dict[str, Any]],
    alert_rows: Sequence[dict[str, Any]],
    reconciliation_rows: Sequence[dict[str, Any]],
    bridge_mode: str,
    pending_intent_count: int,
    runtime_event_rows: Sequence[dict[str, Any]],
    cycle_index: int,
) -> dict[str, Any]:
    reconciliation_issue_count = sum(1 for row in reconciliation_rows if row.get("clean") is not True)
    anomaly_summary = _build_anomaly_summary(alert_rows=alert_rows, reconciliation_rows=reconciliation_rows)
    active_alert_count = sum(1 for row in alert_rows if row.get("active"))
    last_activity_at = next(
        (
            value
            for value in [
                fill_rows[0].get("fill_timestamp") if fill_rows else None,
                intents_rows[0].get("created_at") if intents_rows else None,
            ]
            if value
        ),
        None,
    )
    return {
        "generated_at": generated_at.isoformat(),
        "bridge_run_id": bridge_run_id,
        "bridge_cycle_index": cycle_index,
        "bridge_mode": bridge_mode,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "status": (
            "READY"
            if reconciliation_issue_count == 0
            and active_alert_count == 0
            and pending_intent_count == 0
            and anomaly_summary["needs_attention_now_count"] == 0
            else "ATTENTION_REQUIRED"
        ),
        "selected_lane_count": len(lane_ids),
        "selected_family_count": 1,
        "intent_count": len(intents_rows),
        "fill_count": len(fill_rows),
        "closed_position_count": len(closed_position_rows),
        "open_position_count": len(open_position_rows),
        "pending_intent_count": pending_intent_count,
        "pending_entry_count": sum(1 for row in pending_rows if str(row.get("phase") or "") == "entry"),
        "pending_exit_count": sum(1 for row in pending_rows if str(row.get("phase") or "") == "exit"),
        "active_alert_count": active_alert_count,
        "reconciliation_event_count": len(reconciliation_rows),
        "reconciliation_issue_count": reconciliation_issue_count,
        "unreviewed_anomaly_count": anomaly_summary["unreviewed_count"],
        "acknowledged_anomaly_count": anomaly_summary["acknowledged_count"],
        "unresolved_anomaly_count": anomaly_summary["unresolved_count"],
        "needs_attention_now_count": anomaly_summary["needs_attention_now_count"],
        "resolved_anomaly_count": anomaly_summary["resolved_count"],
        "anomaly_counts_by_classification": anomaly_summary["counts_by_classification"],
        "anomaly_review_state_counts": anomaly_summary["review_state_counts"],
        "runtime_event_count": len(runtime_event_rows),
        "runtime_event_severity_counts": _severity_counts(runtime_event_rows),
        "realized_pnl_cash": round(sum(float(row.get("realized_pnl_cash") or 0.0) for row in closed_position_rows), 6),
        "nonflat_lane_count": sum(1 for row in lane_rows if str(row.get("position_side") or "FLAT") != "FLAT"),
        "last_activity_at": last_activity_at,
        "summary_line": (
            f"{len(lane_ids)} warehouse lanes in {bridge_mode.lower()} mode, "
            f"cycle {cycle_index}, "
            f"{pending_intent_count} pending intents, "
            f"{len(closed_position_rows)} closed positions, "
            f"{reconciliation_issue_count} reconciliation anomalies."
        ),
    }


def _build_operator_status(*, summary: dict[str, Any], bridge_mode: str) -> dict[str, Any]:
    attention_required = (
        int(summary.get("needs_attention_now_count") or 0) > 0
        or int(summary.get("reconciliation_issue_count") or 0) > 0
        or int(summary.get("open_position_count") or 0) > 0
        or int(summary.get("pending_intent_count") or 0) > 0
    )
    return {
        "generated_at": summary.get("generated_at"),
        "bridge_mode": bridge_mode,
        "execution_mode": BRIDGE_EXECUTION_MODE,
        "paper_only": True,
        "live_execution_enabled": False,
        "status": "ATTENTION_REQUIRED" if attention_required else "PAPER_READY",
        "label": "Paper Runtime Bridge",
        "operator_action_required": attention_required,
        "selected_lane_count": summary.get("selected_lane_count"),
        "closed_position_count": summary.get("closed_position_count"),
        "open_position_count": summary.get("open_position_count"),
        "pending_intent_count": summary.get("pending_intent_count"),
        "active_alert_count": summary.get("active_alert_count"),
        "reconciliation_issue_count": summary.get("reconciliation_issue_count"),
        "unreviewed_anomaly_count": summary.get("unreviewed_anomaly_count"),
        "last_activity_at": summary.get("last_activity_at"),
        "notes": [
            "Paper-only bridge. No live broker execution path is enabled.",
            (
                "Runtime actions advance through a prospective paper lifecycle sourced from warehouse entry truth."
                if bridge_mode == BRIDGE_MODE_PROSPECTIVE
                else "Runtime actions are deterministic replays of selected research-engine closed trades."
            ),
            (
                f"Pending intents become stale after {PROSPECTIVE_STALE_AFTER_CYCLES} bridge cycles and expire after {PROSPECTIVE_EXPIRE_AFTER_CYCLES} cycles."
                if bridge_mode == BRIDGE_MODE_PROSPECTIVE
                else "Replay mode does not maintain prospective pending/open lifecycle state."
            ),
        ],
    }


def _validate_trade_row(row: dict[str, Any]) -> str | None:
    required = ["trade_id", "lane_id", "symbol", "side", "entry_ts", "exit_ts", "entry_price", "exit_price"]
    missing = [field for field in required if row.get(field) in {None, ""}]
    if missing:
        return f"Warehouse trade row is missing required fields: {', '.join(missing)}."
    side = str(row.get("side") or "").upper()
    if side not in {"LONG", "SHORT"}:
        return f"Warehouse trade row side must be LONG or SHORT, received {row.get('side')!r}."
    entry_ts = _as_datetime(row["entry_ts"])
    exit_ts = _as_datetime(row["exit_ts"])
    if exit_ts <= entry_ts:
        return "Warehouse trade row exit timestamp must be after entry timestamp."
    return None


def _validate_entry_row(row: dict[str, Any]) -> str | None:
    required = ["entry_id", "lane_id", "symbol", "side", "entry_ts", "entry_price"]
    missing = [field for field in required if row.get(field) in {None, ""}]
    if missing:
        return f"Warehouse entry row is missing required fields: {', '.join(missing)}."
    side = str(row.get("side") or "").upper()
    if side not in {"LONG", "SHORT"}:
        return f"Warehouse entry row side must be LONG or SHORT, received {row.get('side')!r}."
    return None


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _as_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _signed_qty_to_side(quantity: int) -> str:
    if quantity > 0:
        return "LONG"
    if quantity < 0:
        return "SHORT"
    return "FLAT"


def _stable_hash(payload: Any, *, length: int = 16) -> str:
    encoded = json.dumps(_json_ready(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:length]


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(row), sort_keys=True) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_bridge(
        warehouse_root=Path(args.warehouse_root),
        output_dir=Path(args.output_dir),
        selected_lane_ids=args.lane_id,
        mode=BRIDGE_MODE_REPLAY if args.mode == "replay" else BRIDGE_MODE_PROSPECTIVE,
        reset_state=bool(args.reset_state),
        entries_enabled=args.entries_enabled == "true",
        exits_enabled=args.exits_enabled == "true",
        operator_halt=args.operator_halt == "true",
        cycle_count=int(args.cycle_count),
        poll_interval_seconds=int(args.poll_interval_seconds),
        stop_when_settled=bool(args.stop_when_settled),
    )
    print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
