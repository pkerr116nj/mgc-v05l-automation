"""Paper-only IBKR strategy exposure attribution and aggregate exposure gating."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .ibkr_paper_strategy_governance import load_paper_strategy_governance_status
from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .track_b_phase1_submit_authority import evaluate_phase1_broker_reconciliation_submit_gate
from ..execution_core.track_b_canonical_truth_snapshot import (
    BROKER_LIFECYCLE_RECONCILIATION_DIRTY,
    BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,
    BROKER_TRUTH_STALE,
    CONTRACT_AMBIGUOUS,
    CONTRACT_DETAILS_STALE,
    CONTRACT_ENTRY_BLOCKED,
    CONTRACT_ENTRY_CLOSE_ONLY,
    CONTRACT_ENTRY_ELIGIBLE,
    EXACT_LIFECYCLE_IDENTITY_MISMATCH,
    LIFECYCLE_TRUTH_STALE,
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)
from ..execution_core.track_b_central_trade_registry import TradeCurrentState, TradeRegistryRecord
from ..execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    load_unresolved_submit_intent_ownership_records,
)
from ..execution_core.track_b_live_trade_registry import (
    DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    load_live_trade_registry_records,
    validate_registry_managed_exit_identity,
)

_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "paper_strategy_exposure"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_REPORT_JSON = "paper_exposure_attribution_report.json"
_DEFAULT_REPORT_MD = "paper_exposure_attribution_report.md"
_DEFAULT_LEDGER_JSON = "paper_strategy_exposure_ledger.json"
_DEFAULT_AGGREGATE_JSON = "paper_aggregate_exposure_state.json"
_DEFAULT_AUDIT_JSONL = "paper_exposure_gate_audit.jsonl"
_DEFAULT_BROKER_POSITIONS_SNAPSHOT = Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
_DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT = Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
_DEFAULT_INDEX_EXPOSURE_SNAPSHOT = Path("outputs") / "reports" / "ibkr_mnq_nq_scope_support" / "paper_index_exposure_state.json"
_DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH = DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL
_DEFAULT_REGISTRY_DIAGNOSTICS_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_track_b_registry_truth_diagnostics.json"
)
_DEFAULT_MANAGED_POSITIONS_PATH = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
_DEFAULT_MANAGED_ORDERS_PATH = Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
_DEFAULT_OPEN_ORDER_TRUTH_PATH = Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
_UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON = "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY"
_SAME_SYMBOL_PENDING_FILL_ANTI_FLIP_REASON = "SAME_SYMBOL_PENDING_FILL_ANTI_FLIP_LOCK"
_SAME_SYMBOL_UNRESOLVED_EXPOSURE_REASON = "SAME_SYMBOL_UNRESOLVED_CURRENT_EXPOSURE_LOCK"
_SAME_SYMBOL_BROKER_QTY_ANTI_FLIP_REASON = "SAME_SYMBOL_BROKER_QTY_ANTI_FLIP_LOCK"
_CANONICAL_TRUTH_UNAVAILABLE_REASON = "CANONICAL_REGISTRY_TRUTH_UNAVAILABLE"
_CANONICAL_TRUTH_AMBIGUOUS_REASON = "CANONICAL_REGISTRY_TRUTH_AMBIGUOUS"
_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON = "CANONICAL_CURRENT_EXPOSURE_PRESENT"
_SUPPORTED_ENTRY_ACTIONS = {"BUY"}
_SUPPORTED_EXIT_ACTIONS = {"SELL", "EXIT"}
_DEFAULT_MAX_TOTAL_MGC_CONTRACTS = 20.0
_DEFAULT_MAX_TOTAL_GC_EQUIVALENT = 2.0
_DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS = 1.0
_DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS = 300.0
_DEFAULT_CANONICAL_CURRENT_SCOPE_MAX_AGE_SECONDS = 300.0
_ENTRY_EXPOSURE_AUTHORITY_REGISTRY_TRUTH = "REGISTRY_TRUTH"
_MANAGED_EXIT_AUTHORITY_REGISTRY_TRUTH = "REGISTRY_TRUTH"
_ENTRY_ACTIVE_REGISTRY_STATES = {
    TradeCurrentState.PENDING_ENTRY,
    TradeCurrentState.WORKING_ENTRY,
    TradeCurrentState.OPEN_MANAGED,
    TradeCurrentState.EXIT_DUE,
    TradeCurrentState.WORKING_EXIT,
}
_ENTRY_CURRENT_HOT_PATH_CONFLICTS = {
    BROKER_TRUTH_STALE,
    BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,
    LIFECYCLE_TRUTH_STALE,
    BROKER_LIFECYCLE_RECONCILIATION_DIRTY,
    EXACT_LIFECYCLE_IDENTITY_MISMATCH,
    CONTRACT_DETAILS_STALE,
    CONTRACT_AMBIGUOUS,
    CONTRACT_ENTRY_BLOCKED,
    CONTRACT_ENTRY_CLOSE_ONLY,
}


@dataclass(frozen=True)
class IbkrPaperStrategyExposureConfig:
    repo_root: Path
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    ledger_path: Path = _DEFAULT_LEDGER_PATH
    strategy_id: str | None = None
    bridge_strategy_id: str | None = None
    executable_symbol: str = "MGC"
    action: str | None = None
    intent_type: str | None = None
    quantity: float = 1.0
    allow_stacking: bool = False
    max_total_mgc_contracts: float | None = _DEFAULT_MAX_TOTAL_MGC_CONTRACTS
    max_total_gc_equivalent: float = _DEFAULT_MAX_TOTAL_GC_EQUIVALENT
    max_per_strategy_mgc_contracts: float = _DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS
    allow_long_and_short_netting: bool = False
    allow_direct_strategy_flip: bool = False
    broker_positions_snapshot_path: Path = _DEFAULT_BROKER_POSITIONS_SNAPSHOT
    broker_open_orders_snapshot_path: Path = _DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT
    index_exposure_snapshot_path: Path = _DEFAULT_INDEX_EXPOSURE_SNAPSHOT
    broker_truth_max_age_seconds: float = _DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS
    account_id: str | None = None
    con_id: int | None = None
    local_symbol: str | None = None
    lifecycle_id: str | None = None
    trade_id: str | None = None
    live_trade_registry_events_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL
    submit_intent_ownership_path: Path = _DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH


@dataclass(frozen=True)
class IbkrPaperStrategyExposureArtifacts:
    classification: str
    report: dict[str, Any]
    strategy_exposure_ledger: dict[str, Any]
    aggregate_exposure_state: dict[str, Any]
    audit_events: list[dict[str, Any]]


@dataclass(frozen=True)
class _IntentSemantics:
    operation: str
    direction: str | None
    broker_action: str
    explicit_intent_type: bool


def run_ibkr_paper_strategy_exposure(
    *,
    config: IbkrPaperStrategyExposureConfig,
) -> IbkrPaperStrategyExposureArtifacts:
    audit_events: list[dict[str, Any]] = []
    now = _utc_now()
    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    phase1_reconciliation_gate = evaluate_phase1_broker_reconciliation_submit_gate(
        repo_root=config.repo_root,
        max_age_seconds=config.broker_truth_max_age_seconds,
    )
    governance_status = load_paper_strategy_governance_status(
        repo_root=config.repo_root,
        strategy_id=config.bridge_strategy_id or config.strategy_id,
    )
    raw_ledger = _load_json(config.repo_root / config.ledger_path)
    strategy_exposure_rows = _build_strategy_exposure_rows_from_phase1_reconciliation(
        phase1_reconciliation_gate
    ) or _build_strategy_exposure_rows(raw_ledger)
    aggregate_state = _build_aggregate_exposure_state(
        config=config,
        monitor_status=monitor_status,
        governance_status=governance_status,
        phase1_reconciliation_gate=phase1_reconciliation_gate,
        strategy_rows=strategy_exposure_rows,
    )
    unresolved_submit_intents = _load_unresolved_submit_intents(config)
    selected_strategy_gate = _evaluate_strategy_gate(
        config=config,
        strategy_rows=strategy_exposure_rows,
        aggregate_state=aggregate_state,
        unresolved_submit_intents=unresolved_submit_intents,
    )
    classification = str(
        selected_strategy_gate.get("classification")
        or aggregate_state.get("classification")
        or "PAPER_EXPOSURE_ATTRIBUTION_READY"
    )
    report = {
        "generated_at": now,
        "classification": classification,
        "config": {
            "allow_stacking": bool(config.allow_stacking),
            "max_total_mgc_contracts": config.max_total_mgc_contracts,
            "max_total_gc_equivalent": float(config.max_total_gc_equivalent),
            "max_per_strategy_mgc_contracts": float(config.max_per_strategy_mgc_contracts),
            "allow_long_and_short_netting": bool(config.allow_long_and_short_netting),
            "allow_direct_strategy_flip": bool(config.allow_direct_strategy_flip),
            "broker_truth_max_age_seconds": float(config.broker_truth_max_age_seconds),
        },
        "selected_strategy_gate": selected_strategy_gate,
        "monitor_status": {
            key: monitor_status.get(key)
            for key in [
                "classification",
                "monitor_running",
                "health_classification",
                "stale",
                "account_id",
                "broker_position_quantity",
                "open_order_count",
                "block_reasons",
            ]
        },
        "governance_status": {
            "classification": governance_status.get("classification"),
            "selected_strategy": governance_status.get("selected_strategy"),
            "submit_allowed": governance_status.get("submit_allowed"),
            "block_reasons": governance_status.get("block_reasons"),
        },
        "phase1_broker_reconciliation_gate": phase1_reconciliation_gate,
        "strategy_exposure_ledger": {
            "strategy_count": len(strategy_exposure_rows),
            "rows": strategy_exposure_rows,
        },
        "aggregate_exposure_state": aggregate_state,
        "unresolved_submit_intent_ownership_count": len(unresolved_submit_intents),
    }
    _record_audit(
        audit_events,
        "exposure_state_evaluated",
        "Evaluated paper exposure attribution, aggregate exposure, and strategy-specific gating.",
        extra={
            "classification": classification,
            "selected_strategy": selected_strategy_gate.get("strategy_id"),
            "selected_action": selected_strategy_gate.get("action"),
            "block_reasons": selected_strategy_gate.get("block_reasons"),
        },
    )
    return IbkrPaperStrategyExposureArtifacts(
        classification=classification,
        report=report,
        strategy_exposure_ledger={
            "generated_at": now,
            "positions": strategy_exposure_rows,
            "persistent_source": str((config.repo_root / config.ledger_path).resolve()),
        },
        aggregate_exposure_state=aggregate_state,
        audit_events=audit_events,
    )


def evaluate_paper_strategy_exposure_gate(
    *,
    repo_root: Path,
    strategy_id: str,
    action: str,
    intent_type: str | None = None,
    quantity: float,
    bridge_strategy_id: str | None = None,
    executable_symbol: str = "MGC",
    allow_stacking: bool = False,
    max_total_mgc_contracts: float | None = None,
    max_total_gc_equivalent: float = _DEFAULT_MAX_TOTAL_GC_EQUIVALENT,
    max_per_strategy_mgc_contracts: float = _DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS,
    allow_long_and_short_netting: bool = False,
    allow_direct_strategy_flip: bool = False,
    broker_positions_snapshot_path: Path = _DEFAULT_BROKER_POSITIONS_SNAPSHOT,
    broker_open_orders_snapshot_path: Path = _DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT,
    index_exposure_snapshot_path: Path = _DEFAULT_INDEX_EXPOSURE_SNAPSHOT,
    broker_truth_max_age_seconds: float = _DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS,
    account_id: str | None = None,
    con_id: int | None = None,
    local_symbol: str | None = None,
    lifecycle_id: str | None = None,
    trade_id: str | None = None,
) -> dict[str, Any]:
    artifacts = run_ibkr_paper_strategy_exposure(
        config=IbkrPaperStrategyExposureConfig(
            repo_root=repo_root,
            strategy_id=strategy_id,
            bridge_strategy_id=bridge_strategy_id,
            executable_symbol=executable_symbol,
            action=action,
            intent_type=intent_type,
            quantity=quantity,
            allow_stacking=allow_stacking,
            max_total_mgc_contracts=_DEFAULT_MAX_TOTAL_MGC_CONTRACTS if max_total_mgc_contracts is None else max_total_mgc_contracts,
            max_total_gc_equivalent=max_total_gc_equivalent,
            max_per_strategy_mgc_contracts=max_per_strategy_mgc_contracts,
            allow_long_and_short_netting=allow_long_and_short_netting,
            allow_direct_strategy_flip=allow_direct_strategy_flip,
            broker_positions_snapshot_path=broker_positions_snapshot_path,
            broker_open_orders_snapshot_path=broker_open_orders_snapshot_path,
            index_exposure_snapshot_path=index_exposure_snapshot_path,
            broker_truth_max_age_seconds=broker_truth_max_age_seconds,
            account_id=account_id,
            con_id=con_id,
            local_symbol=local_symbol,
            lifecycle_id=lifecycle_id,
            trade_id=trade_id,
            submit_intent_ownership_path=_DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH,
        )
    )
    return dict(artifacts.report.get("selected_strategy_gate") or {})


def write_ibkr_paper_strategy_exposure_artifacts(
    *,
    config: IbkrPaperStrategyExposureConfig,
    artifacts: IbkrPaperStrategyExposureArtifacts,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / _DEFAULT_REPORT_JSON).write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / _DEFAULT_REPORT_MD).write_text(
        render_ibkr_paper_strategy_exposure_markdown(artifacts.report) + "\n",
        encoding="utf-8",
    )
    (output_dir / _DEFAULT_LEDGER_JSON).write_text(
        json.dumps(artifacts.strategy_exposure_ledger, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / _DEFAULT_AGGREGATE_JSON).write_text(
        json.dumps(artifacts.aggregate_exposure_state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / _DEFAULT_AUDIT_JSONL).open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def render_ibkr_paper_strategy_exposure_markdown(report: dict[str, Any]) -> str:
    selected = dict(report.get("selected_strategy_gate") or {})
    aggregate = dict(report.get("aggregate_exposure_state") or {})
    lines = [
        "# Paper Strategy Exposure Attribution",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- executable symbol: `{aggregate.get('executable_symbol')}`",
        f"- broker net position: `{aggregate.get('broker_net_position')}`",
        f"- attributed strategy exposure sum: `{aggregate.get('strategy_attributed_position_sum')}`",
        f"- broker minus ledger difference: `{aggregate.get('broker_minus_strategy_difference')}`",
        f"- unmatched/orphan exposure: `{aggregate.get('unmatched_orphan_quantity')}`",
        f"- allow stacking: `{dict(report.get('config') or {}).get('allow_stacking')}`",
        f"- max total GC-equivalent: `{dict(report.get('config') or {}).get('max_total_gc_equivalent')}`",
        f"- max per-strategy MGC contracts: `{dict(report.get('config') or {}).get('max_per_strategy_mgc_contracts')}`",
        f"- max total MGC contracts: `{dict(report.get('config') or {}).get('max_total_mgc_contracts')}`",
        f"- aggregate realized pnl: `{aggregate.get('aggregate_realized_pnl')}`",
        f"- aggregate unrealized pnl: `{aggregate.get('aggregate_unrealized_pnl')}`",
        f"- aggregate total net pnl: `{aggregate.get('aggregate_total_net_pnl')}`",
        "",
        "## Selected Strategy Gate",
        "",
        f"- strategy id: `{selected.get('strategy_id')}`",
        f"- bridge strategy id: `{selected.get('bridge_strategy_id')}`",
        f"- action / qty: `{selected.get('action')} / {selected.get('quantity')}`",
        f"- strategy state: `{selected.get('strategy_state')}`",
        f"- submit allowed: `{selected.get('submit_allowed')}`",
    ]
    for reason in list(selected.get("block_reasons") or []):
        lines.append(f"- block reason: `{reason}`")
    return "\n".join(lines)


def _build_strategy_exposure_rows(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in list(ledger.get("positions") or []):
        position = dict(row)
        quantity = float(position.get("quantity") or 0.0)
        side = str(position.get("side") or "").strip().upper()
        state = _normalize_strategy_state(quantity=quantity, side=side, raw_state=str(position.get("state") or ""))
        signed_quantity = quantity if state == "LONG" else (-quantity if state == "SHORT" else 0.0)
        rows.append(
            {
                "strategy_id": position.get("strategy_id"),
                "account_id": position.get("account_id"),
                "symbol": position.get("symbol"),
                "contract_month": position.get("contract_month"),
                "expiry": position.get("expiry"),
                "con_id": position.get("con_id"),
                "local_symbol": position.get("local_symbol"),
                "direction": state,
                "quantity": quantity,
                "signed_quantity": signed_quantity,
                "average_entry_price": position.get("average_entry_price"),
                "realized_pnl": position.get("realized_pnl"),
                "unrealized_pnl": position.get("unrealized_pnl"),
                "order_id": position.get("order_id"),
                "perm_id": position.get("perm_id"),
                "execution_id": position.get("execution_id"),
                "entry_timestamp": position.get("entry_timestamp"),
                "source_intent_id": position.get("source_intent_id"),
                "state": state if quantity > 0.0 else "FLAT",
                "open_orders": list(position.get("open_orders") or []),
                "pnl_source": position.get("pnl_source"),
                "last_reconciliation_timestamp": position.get("last_reconciliation_timestamp"),
            }
        )
    return rows


def _build_strategy_exposure_rows_from_phase1_reconciliation(
    phase1_reconciliation_gate: dict[str, Any],
) -> list[dict[str, Any]]:
    if not bool(phase1_reconciliation_gate.get("ready")):
        return []
    broker_positions = [
        dict(row)
        for row in list(phase1_reconciliation_gate.get("track_b_broker_positions") or [])
        if isinstance(row, dict)
    ]
    rows: list[dict[str, Any]] = []
    for position in list(phase1_reconciliation_gate.get("track_b_lifecycle_positions") or []):
        quantity = float(position.get("quantity") or 0.0)
        if quantity <= 0.0:
            continue
        side = str(position.get("side") or "").strip().upper()
        state = _normalize_strategy_state(quantity=quantity, side=side, raw_state=side)
        signed_quantity = quantity if state == "LONG" else (-quantity if state == "SHORT" else 0.0)
        strategy_id = str(position.get("strategy_id") or "").strip()
        broker_position = _matching_broker_position_for_lifecycle(position=position, broker_positions=broker_positions)
        rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_aliases": _strategy_aliases_for_phase1_lifecycle_position(position),
                "lane_id": position.get("lane_id"),
                "lane_ids": list(position.get("lane_ids") or []),
                "strategy_ids": list(position.get("strategy_ids") or []),
                "account_id": position.get("account_id"),
                "broker_account_id": broker_position.get("account_id") or broker_position.get("account"),
                "symbol": position.get("track_b_root") or position.get("instrument_family"),
                "contract_month": str(position.get("contract_key") or "").split("-", 1)[-1],
                "expiry": position.get("expiry"),
                "con_id": position.get("con_id"),
                "local_symbol": position.get("local_symbol"),
                "direction": state,
                "quantity": quantity,
                "signed_quantity": signed_quantity,
                "average_entry_price": position.get("avg_entry_price"),
                "realized_pnl": position.get("realized_pnl_today"),
                "unrealized_pnl": position.get("unrealized_pnl"),
                "order_id": position.get("entry_order_id"),
                "perm_id": position.get("entry_perm_id"),
                "execution_id": position.get("entry_exec_id"),
                "entry_timestamp": position.get("as_of"),
                "source_intent_id": position.get("lifecycle_id"),
                "lifecycle_id": position.get("lifecycle_id"),
                "state": state if quantity > 0.0 else "FLAT",
                "open_orders": [],
                "pnl_source": "phase1_broker_reconciliation",
                "last_reconciliation_timestamp": phase1_reconciliation_gate.get("generated_at"),
            }
        )
    return rows


def _matching_broker_position_for_lifecycle(
    *,
    position: dict[str, Any],
    broker_positions: list[dict[str, Any]],
) -> dict[str, Any]:
    requested_con_id = str(position.get("con_id") or "").strip()
    requested_local = str(position.get("local_symbol") or "").strip().upper()
    requested_symbol = str(position.get("track_b_root") or position.get("instrument_family") or "").strip().upper()
    for broker_position in broker_positions:
        broker_con_id = str(broker_position.get("con_id") or broker_position.get("conId") or "").strip()
        broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip().upper()
        broker_symbol = str(broker_position.get("track_b_root") or broker_position.get("symbol") or "").strip().upper()
        if requested_con_id and broker_con_id and requested_con_id == broker_con_id:
            return dict(broker_position)
        if requested_local and broker_local and requested_local == broker_local:
            return dict(broker_position)
        if requested_symbol and broker_symbol and requested_symbol == broker_symbol and not requested_local and not requested_con_id:
            return dict(broker_position)
    return {}


def _strategy_aliases_for_phase1_lifecycle_position(position: dict[str, Any]) -> list[str]:
    normalized = str(position.get("strategy_id") or "").strip()
    aliases: list[str] = []
    if normalized:
        aliases.append(normalized)
    for key in ("lane_id", "strategy_id"):
        value = str(position.get(key) or "").strip()
        if value:
            aliases.append(value)
    for key in ("lane_ids", "strategy_ids"):
        for value in list(position.get(key) or []):
            text = str(value or "").strip()
            if text:
                aliases.append(text)
    for unit in list(position.get("lifecycle_units") or []):
        if not isinstance(unit, dict):
            continue
        for key in ("lane_id", "strategy_id"):
            value = str(unit.get(key) or "").strip()
            if value:
                aliases.append(value)
    if "__" in normalized:
        root, suffix = normalized.split("__", 1)
        if suffix:
            aliases.append(suffix)
        if suffix.startswith("paper_") and root:
            aliases.append(f"{root}_{suffix.removeprefix('paper_')}")
        if root.endswith("_turn") and suffix:
            aliases.append(root.removesuffix("_turn"))
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _build_aggregate_exposure_state(
    *,
    config: IbkrPaperStrategyExposureConfig,
    monitor_status: dict[str, Any],
    governance_status: dict[str, Any],
    phase1_reconciliation_gate: dict[str, Any],
    strategy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    executable_symbol = str(config.executable_symbol or "MGC").strip().upper()
    broker_net, broker_truth = _broker_net_position_for_symbol(
        config=config,
        monitor_status=monitor_status,
        executable_symbol=executable_symbol,
    )
    attributed_rows = [
        dict(row)
        for row in strategy_rows
        if str(row.get("symbol") or "").strip().upper() == executable_symbol
    ]
    aggregate_realized_pnl = round(sum(float(row.get("realized_pnl") or 0.0) for row in attributed_rows), 8)
    aggregate_unrealized_pnl = round(sum(float(row.get("unrealized_pnl") or 0.0) for row in attributed_rows), 8)
    aggregate_total_net_pnl = round(aggregate_realized_pnl + aggregate_unrealized_pnl, 8)
    strategy_sum = round(sum(float(row.get("signed_quantity") or 0.0) for row in attributed_rows), 8)
    difference = round(broker_net - strategy_sum, 8)
    orphan_rows = list(monitor_status.get("orphan_positions") or [])
    unmatched_orphan_quantity = float(sum(float(row.get("quantity") or 0.0) for row in orphan_rows))
    ledger_only_rows: list[dict[str, Any]] = []
    discrepancy_classification = "CLEAN"
    if broker_truth.get("truth_available") is False:
        discrepancy_classification = "BROKER_TRUTH_STALE_OR_MISSING"
    elif orphan_rows or abs(unmatched_orphan_quantity) > 0.0:
        discrepancy_classification = "ORPHAN_BROKER_POSITION"
    elif broker_net == 0.0 and abs(strategy_sum) > 0.0:
        discrepancy_classification = "LEDGER_ONLY_POSITION"
        ledger_only_rows = [dict(row) for row in attributed_rows if float(row.get("quantity") or 0.0) > 0.0]
    elif abs(difference) > 1e-9:
        discrepancy_classification = "LEDGER_BROKER_MISMATCH"
    classification = "PAPER_EXPOSURE_ATTRIBUTION_READY"
    if not bool(phase1_reconciliation_gate.get("ready")):
        classification = "PAPER_EXPOSURE_BLOCKED_PHASE1_RECONCILIATION"
    elif discrepancy_classification == "BROKER_TRUTH_STALE_OR_MISSING":
        classification = "PAPER_EXPOSURE_BLOCKED_BROKER_TRUTH_STALE"
    elif discrepancy_classification == "ORPHAN_BROKER_POSITION":
        classification = "PAPER_EXPOSURE_BLOCKED_ORPHAN_POSITION"
    elif discrepancy_classification in {"LEDGER_ONLY_POSITION", "LEDGER_BROKER_MISMATCH"}:
        classification = "PAPER_EXPOSURE_BLOCKED_LEDGER_BROKER_MISMATCH"
    strategy_open_orders = []
    for row in list(governance_status.get("strategies") or []):
        if int(row.get("open_order_ambiguity_count") or 0) > 0:
            strategy_open_orders.append(
                {
                    "strategy_id": row.get("strategy_id"),
                    "bridge_strategy_id": row.get("bridge_strategy_id"),
                    "open_order_ambiguity_count": row.get("open_order_ambiguity_count"),
                    "submit_block_reasons": list(row.get("submit_block_reasons") or []),
                }
            )
    return {
        "generated_at": _utc_now(),
        "classification": classification,
        "discrepancy_classification": discrepancy_classification,
        "executable_symbol": executable_symbol,
        "broker_net_position": broker_net,
        "strategy_attributed_position_sum": strategy_sum,
        "broker_minus_strategy_difference": difference,
        "broker_truth": broker_truth,
        "unmatched_orphan_quantity": unmatched_orphan_quantity,
        "orphan_positions": orphan_rows,
        "ledger_only_positions": ledger_only_rows,
        "strategy_open_orders": strategy_open_orders,
        "legacy_monitor_authority": "DIAGNOSTIC_ONLY_FOR_PHASE1_SUBMIT_AUTHORITY",
        "phase1_broker_reconciliation_gate": {
            "classification": phase1_reconciliation_gate.get("classification"),
            "ready": phase1_reconciliation_gate.get("ready"),
            "block_reasons": list(phase1_reconciliation_gate.get("block_reasons") or []),
            "detail": phase1_reconciliation_gate.get("detail"),
            "generated_at": phase1_reconciliation_gate.get("generated_at"),
            "age_seconds": phase1_reconciliation_gate.get("age_seconds"),
        },
        "phase1_broker_reconciliation_gate_full": phase1_reconciliation_gate,
        "allow_stacking": bool(config.allow_stacking),
        "max_total_mgc_contracts": config.max_total_mgc_contracts,
        "max_total_gc_equivalent": float(config.max_total_gc_equivalent),
        "max_per_strategy_mgc_contracts": float(config.max_per_strategy_mgc_contracts),
        "allow_long_and_short_netting": bool(config.allow_long_and_short_netting),
        "allow_direct_strategy_flip": bool(config.allow_direct_strategy_flip),
        "aggregate_realized_pnl": aggregate_realized_pnl,
        "aggregate_unrealized_pnl": aggregate_unrealized_pnl,
        "aggregate_total_net_pnl": aggregate_total_net_pnl,
    }


def _evaluate_strategy_gate(
    *,
    config: IbkrPaperStrategyExposureConfig,
    strategy_rows: list[dict[str, Any]],
    aggregate_state: dict[str, Any],
    unresolved_submit_intents: list[dict[str, Any]],
) -> dict[str, Any]:
    requested_strategy = str(config.strategy_id or "").strip()
    requested_bridge_strategy = str(config.bridge_strategy_id or "").strip()
    action = str(config.action or "OBSERVE").strip().upper()
    semantics = _normalize_intent_semantics(action=action, intent_type=config.intent_type)
    quantity = float(config.quantity or 0.0)
    identifiers = {requested_strategy, requested_bridge_strategy}
    identifiers.discard("")
    requested_lifecycle_id = str(config.lifecycle_id or "").strip()
    requested_trade_id = str(config.trade_id or "").strip()
    exact_lifecycle_rows = [
        dict(row)
        for row in strategy_rows
        if requested_lifecycle_id
        and str(row.get("lifecycle_id") or row.get("source_intent_id") or "").strip() == requested_lifecycle_id
    ]
    owned_rows = []
    for row in strategy_rows:
        row_identifiers = {str(row.get("strategy_id") or "").strip()}
        row_identifiers.update(str(alias or "").strip() for alias in list(row.get("strategy_aliases") or []))
        row_identifiers.add(str(row.get("lane_id") or "").strip())
        row_identifiers.update(str(alias or "").strip() for alias in list(row.get("lane_ids") or []))
        row_identifiers.update(str(alias or "").strip() for alias in list(row.get("strategy_ids") or []))
        row_identifiers.discard("")
        if row_identifiers.intersection(identifiers):
            owned_rows.append(dict(row))
    if semantics.operation == "CLOSE" and exact_lifecycle_rows:
        exact_lifecycle_ids = {
            str(row.get("strategy_id") or "").strip()
            for row in exact_lifecycle_rows
            if str(row.get("strategy_id") or "").strip()
        }
        exact_lifecycle_aliases = {
            str(alias or "").strip()
            for row in exact_lifecycle_rows
            for alias in list(row.get("strategy_aliases") or [])
            if str(alias or "").strip()
        }
        if identifiers and not identifiers.intersection(exact_lifecycle_ids | exact_lifecycle_aliases):
            owned_rows = []
        else:
            owned_rows = exact_lifecycle_rows
    registry_exit_validation: dict[str, Any] | None = None
    registry_owner_identity: dict[str, Any] = {}
    if semantics.operation == "CLOSE" and requested_lifecycle_id:
        registry_exit_validation = validate_registry_managed_exit_identity(
            repo_root=config.repo_root,
            trade_id=requested_trade_id,
            lifecycle_id=requested_lifecycle_id,
            account_id=config.account_id,
            con_id=config.con_id,
            local_symbol=config.local_symbol,
            quantity=quantity,
            action=action,
            phase1_reconciliation_gate=dict(aggregate_state.get("phase1_broker_reconciliation_gate_full") or {}),
            jsonl_path=config.live_trade_registry_events_path,
        )
        registry_owner_identity = dict(registry_exit_validation.get("owner_identity") or {})
        if registry_exit_validation.get("allowed") is True:
            owned_rows = [
                _strategy_row_from_registry_owner_identity(
                    registry_owner_identity,
                    lifecycle_id=requested_lifecycle_id,
                )
            ]
    exit_identity_requested = semantics.operation == "CLOSE" and any(
        item not in (None, "")
        for item in (config.account_id, config.con_id, config.local_symbol, config.lifecycle_id, config.trade_id)
    )
    identity_filtered_owned_rows = _filter_owned_rows_for_requested_exit_identity(
        owned_rows=owned_rows,
        account_id=config.account_id,
        con_id=config.con_id,
        local_symbol=config.local_symbol,
        lifecycle_id=config.lifecycle_id,
    )
    owned_rows_for_quantity = identity_filtered_owned_rows if exit_identity_requested else owned_rows
    owned_signed_quantity = round(sum(float(row.get("signed_quantity") or 0.0) for row in owned_rows_for_quantity), 8)
    owned_quantity = round(abs(owned_signed_quantity), 8)
    strategy_state = "FLAT"
    if owned_signed_quantity > 0.0:
        strategy_state = "LONG"
    elif owned_signed_quantity < 0.0:
        strategy_state = "SHORT"

    block_reasons: list[str] = []
    phase1_gate = dict(aggregate_state.get("phase1_broker_reconciliation_gate") or {})
    if not bool(phase1_gate.get("ready")):
        block_reasons.append("phase1_broker_reconciliation_not_clear")
    aggregate_discrepancy = str(aggregate_state.get("discrepancy_classification") or "CLEAN")
    if aggregate_discrepancy == "BROKER_TRUTH_STALE_OR_MISSING":
        block_reasons.append("broker_position_truth_stale_or_missing")
    elif aggregate_discrepancy == "ORPHAN_BROKER_POSITION":
        block_reasons.append("orphan_broker_position")
    elif aggregate_discrepancy == "LEDGER_ONLY_POSITION":
        block_reasons.append("ledger_only_position")
    elif aggregate_discrepancy == "LEDGER_BROKER_MISMATCH":
        block_reasons.append("ledger_broker_mismatch")
    if semantics.explicit_intent_type and semantics.broker_action != action:
        block_reasons.append("intent_type_action_mismatch")

    classification = str(aggregate_state.get("classification") or "PAPER_EXPOSURE_ATTRIBUTION_READY")
    detail = "Exposure attribution is visible and no explicit exposure conflict is present."
    if "phase1_broker_reconciliation_not_clear" in block_reasons:
        classification = "PAPER_EXPOSURE_BLOCKED_PHASE1_RECONCILIATION"
        detail = "Current Phase-1 broker reconciliation is required before exposure can authorize submit."
    submit_allowed = not block_reasons
    stacking_observed = False
    entry_legacy_result: dict[str, Any] | None = None
    entry_registry_truth_result: dict[str, Any] | None = None
    entry_parity_status: str | None = None
    entry_authoritative_source: str | None = None
    entry_diagnostic_reason_codes: list[str] = []
    managed_exit_legacy_result: dict[str, Any] | None = None
    managed_exit_registry_truth_result: dict[str, Any] | None = None
    managed_exit_parity_status: str | None = None
    managed_exit_authoritative_source: str | None = None
    managed_exit_diagnostic_reason_codes: list[str] = []

    if semantics.operation == "OPEN":
        submit_intent_blocker = _unresolved_submit_intent_new_entry_blocker(
            config=config,
            requested_strategy=requested_strategy,
            requested_bridge_strategy=requested_bridge_strategy,
            unresolved_submit_intents=unresolved_submit_intents,
        )
        if submit_intent_blocker:
            block_reasons.append(_UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON)
        if strategy_state in {"LONG", "SHORT"}:
            block_reasons.append("duplicate_strategy_entry_while_position_open")
        aggregate_signed_quantity = round(float(aggregate_state.get("strategy_attributed_position_sum") or 0.0), 8)
        if (
            semantics.direction == "LONG"
            and aggregate_signed_quantity < 0.0
            and not bool(config.allow_long_and_short_netting)
        ):
            block_reasons.append("opposite_direction_strategy_exposure")
        if (
            semantics.direction == "SHORT"
            and aggregate_signed_quantity > 0.0
            and not bool(config.allow_long_and_short_netting)
        ):
            block_reasons.append("opposite_direction_strategy_exposure")
        if float(config.max_per_strategy_mgc_contracts) > 0.0 and owned_quantity + quantity > float(config.max_per_strategy_mgc_contracts):
            block_reasons.append("per_strategy_contract_limit_exceeded")
        if config.max_total_mgc_contracts is not None:
            aggregate_total = abs(float(aggregate_state.get("strategy_attributed_position_sum") or 0.0))
            if aggregate_total + quantity > float(config.max_total_mgc_contracts):
                block_reasons.append("configured_aggregate_contract_limit_exceeded")
        other_strategy_exposure = abs(float(aggregate_state.get("strategy_attributed_position_sum") or 0.0) - owned_signed_quantity)
        stacking_observed = other_strategy_exposure > 0.0
        if not bool(config.allow_stacking) and stacking_observed:
            block_reasons.append("strategy_stacking_disabled")
        if block_reasons:
            submit_allowed = False
            if "phase1_broker_reconciliation_not_clear" in block_reasons:
                classification = "PAPER_EXPOSURE_BLOCKED_PHASE1_RECONCILIATION"
                detail = "Current Phase-1 broker reconciliation is required before exposure can authorize submit."
            elif "broker_position_truth_stale_or_missing" in block_reasons:
                classification = "BROKER_TRUTH_STALE_OR_MISSING"
                detail = "Fresh broker position and open-order truth is required before exposure ownership can be evaluated."
            elif "configured_aggregate_contract_limit_exceeded" in block_reasons:
                classification = "PAPER_EXPOSURE_BLOCKED_AGGREGATE_LIMIT"
                detail = "The requested entry would exceed the explicit aggregate contract cap."
            elif _UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON in block_reasons:
                classification = "PAPER_EXPOSURE_BLOCKED_UNRESOLVED_SUBMIT_INTENT"
                detail = "Unresolved Track B submit-intent ownership must be recovered, adopted, or terminally resolved before a new entry can submit."
            else:
                classification = "PAPER_EXPOSURE_BLOCKED_STRATEGY_LIMIT"
                detail = "The requested entry violates per-strategy exposure ownership or limits."
        elif stacking_observed and bool(config.allow_stacking):
            classification = "PAPER_EXPOSURE_STACK_ALLOWED"
            detail = "Another strategy already owns executable-contract exposure, but this strategy remains flat and stacking is allowed."
        else:
            classification = "PAPER_EXPOSURE_ATTRIBUTION_READY"
            detail = (
                "The strategy may open long exposure from flat."
                if semantics.direction == "LONG"
                else "The strategy may open short exposure from flat."
            )
        entry_legacy_result = {
            "allowed": bool(submit_allowed and not block_reasons),
            "reason_codes": list(dict.fromkeys(block_reasons)),
            "classification": classification,
            "authority_source": "LEGACY_EXPOSURE_ATTRIBUTION",
            "diagnostic_only": True,
        }
        entry_authoritative_source = _ENTRY_EXPOSURE_AUTHORITY_REGISTRY_TRUTH
        entry_registry_truth_result = _entry_registry_truth_result(
            config=config,
            aggregate_state=aggregate_state,
            requested_strategy=requested_strategy,
            requested_bridge_strategy=requested_bridge_strategy,
            executable_symbol=config.executable_symbol,
            quantity=quantity,
            allow_stacking=bool(config.allow_stacking),
            requested_direction=semantics.direction,
            unresolved_submit_intents=unresolved_submit_intents,
        )
        registry_allowed = bool(entry_registry_truth_result.get("allowed"))
        legacy_allowed = bool(entry_legacy_result.get("allowed"))
        if registry_allowed and legacy_allowed:
            entry_parity_status = "MATCH_ALLOWED"
            block_reasons = []
            submit_allowed = True
        elif registry_allowed and not legacy_allowed:
            entry_parity_status = "REGISTRY_TRUTH_ALLOWED_LEGACY_BLOCKED_DIAGNOSTIC"
            entry_diagnostic_reason_codes = list(entry_legacy_result.get("reason_codes") or [])
            block_reasons = []
            submit_allowed = True
        elif not registry_allowed and legacy_allowed:
            entry_parity_status = "LEGACY_ALLOWED_REGISTRY_TRUTH_BLOCKED_FAIL_CLOSED"
            block_reasons = list(entry_registry_truth_result.get("reason_codes") or [])
            block_reasons.append("legacy_allowed_registry_truth_blocked_fail_closed")
            submit_allowed = False
        else:
            entry_parity_status = "MATCH_BLOCKED"
            block_reasons = list(entry_registry_truth_result.get("reason_codes") or [])
            block_reasons.extend(str(reason) for reason in list(entry_legacy_result.get("reason_codes") or []))
            submit_allowed = False

        if block_reasons:
            classification = "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
            detail = "Registry/truth current-hot-path authority blocked this new entry."
        elif entry_parity_status == "REGISTRY_TRUTH_ALLOWED_LEGACY_BLOCKED_DIAGNOSTIC":
            classification = "PAPER_EXPOSURE_ENTRY_ALLOWED_REGISTRY_TRUTH_LEGACY_DIAGNOSTIC"
            detail = "Registry/truth current-hot-path authority allowed this entry; legacy exposure blockers are diagnostic only."
        elif stacking_observed and bool(config.allow_stacking):
            classification = "PAPER_EXPOSURE_STACK_ALLOWED"
            detail = "Another strategy already owns executable-contract exposure, but registry/truth current-hot-path authority allows this entry."
        else:
            classification = "PAPER_EXPOSURE_ENTRY_ALLOWED"
            detail = "Registry/truth current-hot-path authority allows this new entry."
    elif semantics.operation == "CLOSE":
        legacy_block_reasons = list(block_reasons)
        if exit_identity_requested and not requested_lifecycle_id and any(
            str(row.get("lifecycle_id") or row.get("source_intent_id") or "").strip()
            and str(row.get("pnl_source") or "").strip() == "phase1_broker_reconciliation"
            for row in owned_rows
        ):
            legacy_block_reasons.append("missing_lifecycle_identity")
        if exit_identity_requested and not identity_filtered_owned_rows:
            legacy_block_reasons.append("exit_identity_mismatch")
        if semantics.direction == "LONG" and strategy_state != "LONG":
            legacy_block_reasons.append("non_owning_strategy_exit_forbidden")
        if semantics.direction == "SHORT" and strategy_state != "SHORT":
            legacy_block_reasons.append("non_owning_strategy_exit_forbidden")
        if quantity <= 0.0 or quantity > owned_quantity:
            legacy_block_reasons.append("exit_quantity_exceeds_owned_strategy_position")
        broker_net_position = float(aggregate_state.get("broker_net_position") or 0.0)
        if semantics.direction == "LONG" and broker_net_position < quantity:
            legacy_block_reasons.append("broker_position_does_not_support_requested_exit")
        if semantics.direction == "SHORT" and broker_net_position > -quantity:
            legacy_block_reasons.append("broker_position_does_not_support_requested_exit")

        managed_exit_legacy_result = {
            "allowed": not legacy_block_reasons,
            "reason_codes": list(dict.fromkeys(legacy_block_reasons)),
            "classification": (
                "PAPER_EXPOSURE_EXIT_ALLOWED"
                if not legacy_block_reasons
                else (
                    "PAPER_EXPOSURE_BLOCKED_PHASE1_RECONCILIATION"
                    if "phase1_broker_reconciliation_not_clear" in legacy_block_reasons
                    else "PAPER_EXPOSURE_BLOCKED_STRATEGY_LIMIT"
                )
            ),
            "authority_source": "LEGACY_EXPOSURE_ATTRIBUTION",
            "diagnostic_only": registry_exit_validation is not None,
        }

        if registry_exit_validation is not None:
            managed_exit_authoritative_source = _MANAGED_EXIT_AUTHORITY_REGISTRY_TRUTH
            managed_exit_registry_truth_result = _managed_exit_registry_truth_result(
                registry_exit_validation=registry_exit_validation,
                aggregate_state=aggregate_state,
                requested_identifiers=identifiers,
            )
            registry_allowed = bool(managed_exit_registry_truth_result.get("allowed"))
            legacy_allowed = bool(managed_exit_legacy_result.get("allowed"))
            if registry_allowed and legacy_allowed:
                managed_exit_parity_status = "MATCH_ALLOWED"
                block_reasons = []
                submit_allowed = True
            elif registry_allowed and not legacy_allowed:
                managed_exit_parity_status = "REGISTRY_TRUTH_ALLOWED_LEGACY_BLOCKED_DIAGNOSTIC"
                managed_exit_diagnostic_reason_codes = list(managed_exit_legacy_result.get("reason_codes") or [])
                block_reasons = []
                submit_allowed = True
            elif not registry_allowed and legacy_allowed:
                managed_exit_parity_status = "LEGACY_ALLOWED_REGISTRY_TRUTH_BLOCKED_FAIL_CLOSED"
                block_reasons = list(managed_exit_registry_truth_result.get("reason_codes") or [])
                block_reasons.append("legacy_allowed_registry_truth_blocked_fail_closed")
                submit_allowed = False
            else:
                managed_exit_parity_status = "MATCH_BLOCKED"
                block_reasons = list(managed_exit_registry_truth_result.get("reason_codes") or [])
                block_reasons.extend(str(reason) for reason in list(managed_exit_legacy_result.get("reason_codes") or []))
                submit_allowed = False
        else:
            block_reasons = legacy_block_reasons

        if block_reasons:
            submit_allowed = False
            if "phase1_broker_reconciliation_not_clear" in block_reasons:
                classification = "PAPER_EXPOSURE_BLOCKED_PHASE1_RECONCILIATION"
                detail = "Current Phase-1 broker reconciliation is required before exposure can authorize submit."
            else:
                classification = "PAPER_EXPOSURE_BLOCKED_STRATEGY_LIMIT"
                detail = "The requested exit does not align with the owning strategy’s attributed exposure."
        else:
            classification = "PAPER_EXPOSURE_EXIT_ALLOWED"
            detail = "The owning strategy may reduce its attributed exposure."
    elif action != "OBSERVE":
        block_reasons.append("unsupported_or_ambiguous_intent_semantics")
        submit_allowed = False
        classification = "PAPER_EXPOSURE_BLOCKED_STRATEGY_LIMIT"
        detail = "The requested broker action does not declare supported open/close intent semantics."

    return {
        "classification": classification,
        "strategy_id": requested_strategy or None,
        "bridge_strategy_id": requested_bridge_strategy or None,
        "action": action,
        "intent_type": str(config.intent_type or "").strip().upper() or None,
        "intent_operation": semantics.operation,
        "intent_direction": semantics.direction,
        "broker_action": semantics.broker_action,
        "quantity": quantity,
        "strategy_state": strategy_state,
        "owned_strategy_quantity": owned_quantity,
        "owned_strategy_position_count": len(owned_rows_for_quantity),
        "exit_identity_requested": exit_identity_requested,
        "registry_exit_validation": registry_exit_validation,
        "legacy_result": entry_legacy_result if semantics.operation == "OPEN" else managed_exit_legacy_result,
        "registry_truth_result": entry_registry_truth_result if semantics.operation == "OPEN" else managed_exit_registry_truth_result,
        "parity_status": entry_parity_status if semantics.operation == "OPEN" else managed_exit_parity_status,
        "authoritative_source": entry_authoritative_source if semantics.operation == "OPEN" else managed_exit_authoritative_source,
        "diagnostic_reason_codes": list(
            dict.fromkeys(entry_diagnostic_reason_codes if semantics.operation == "OPEN" else managed_exit_diagnostic_reason_codes)
        ),
        "submit_allowed": submit_allowed and not block_reasons,
        "block_reasons": list(dict.fromkeys(block_reasons)),
        "detail": detail,
        "allow_stacking": bool(config.allow_stacking),
        "max_total_mgc_contracts": config.max_total_mgc_contracts,
        "max_total_gc_equivalent": float(config.max_total_gc_equivalent),
        "max_per_strategy_mgc_contracts": float(config.max_per_strategy_mgc_contracts),
        "aggregate_broker_position": aggregate_state.get("broker_net_position"),
        "aggregate_strategy_position_sum": aggregate_state.get("strategy_attributed_position_sum"),
        "broker_truth": aggregate_state.get("broker_truth"),
        "unresolved_submit_intent_ownership_blocker": submit_intent_blocker if semantics.operation == "OPEN" else None,
        "blocker_classification": (
            "PHASE1_BROKER_RECONCILIATION_NOT_CLEAR"
            if "phase1_broker_reconciliation_not_clear" in block_reasons
            else _UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON
            if _UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON in block_reasons
            else "BROKER_LEDGER_POSITION_MISMATCH"
            if any(reason in block_reasons for reason in {"ledger_broker_mismatch", "orphan_broker_position"})
            else ("BROKER_TRUTH_STALE_OR_MISSING" if "broker_position_truth_stale_or_missing" in block_reasons else None)
        ),
        "review_required": any(
            reason in block_reasons
            for reason in {
                "ledger_broker_mismatch",
                "orphan_broker_position",
                "broker_position_truth_stale_or_missing",
                "phase1_broker_reconciliation_not_clear",
                _UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON,
                _SAME_SYMBOL_PENDING_FILL_ANTI_FLIP_REASON,
                _SAME_SYMBOL_UNRESOLVED_EXPOSURE_REASON,
                _SAME_SYMBOL_BROKER_QTY_ANTI_FLIP_REASON,
            }
        ),
    }


def _entry_registry_truth_result(
    *,
    config: IbkrPaperStrategyExposureConfig,
    aggregate_state: dict[str, Any],
    requested_strategy: str,
    requested_bridge_strategy: str,
    executable_symbol: str,
    quantity: float,
    allow_stacking: bool,
    requested_direction: str | None,
    unresolved_submit_intents: list[dict[str, Any]],
) -> dict[str, Any]:
    reason_codes: list[str] = []
    trade_id = _entry_birth_trade_id(config=config, requested_strategy=requested_strategy, executable_symbol=executable_symbol)
    if not trade_id:
        reason_codes.append("missing_or_invalid_trade_id_birth_path")
    if quantity <= 0.0:
        reason_codes.append("entry_quantity_must_be_positive")

    truth = _safe_build_entry_truth_snapshot(config.repo_root)
    records = load_live_trade_registry_records(
        repo_root=config.repo_root,
        jsonl_path=config.live_trade_registry_events_path,
    )
    active_records = tuple(record for record in records if record.current_state in _ENTRY_ACTIVE_REGISTRY_STATES)
    raw_active_registry_trade_count = len(active_records)

    source_paths: dict[str, str] = {}
    contract_entry_status = None
    if truth is not None:
        source_paths = dict(truth.source_paths)
        contract_entry_status = truth.contract_status.entry_status
        reason_codes.extend(_entry_truth_contract_block_reasons(truth))

    canonical_current_scope = _entry_canonical_current_scope_result(config.repo_root)
    reason_codes.extend(str(reason) for reason in list(canonical_current_scope.get("reason_codes") or []))
    if canonical_current_scope.get("usable") is True and canonical_current_scope.get("current_exposure_present") is False:
        active_records = ()
    elif canonical_current_scope.get("current_exposure_present") is False:
        active_records = ()

    same_symbol_pending_fill_lock = _same_symbol_pending_fill_entry_lock(
        config=config,
        requested_direction=requested_direction,
        allow_stacking=allow_stacking,
        unresolved_submit_intents=unresolved_submit_intents,
    )
    if same_symbol_pending_fill_lock:
        reason_codes.extend(str(reason) for reason in list(same_symbol_pending_fill_lock.get("reason_codes") or []))

    same_symbol_broker_qty_lock = _same_symbol_broker_quantity_entry_lock(
        config=config,
        requested_direction=requested_direction,
        allow_stacking=allow_stacking,
    )
    if same_symbol_broker_qty_lock:
        reason_codes.extend(str(reason) for reason in list(same_symbol_broker_qty_lock.get("reason_codes") or []))

    matching_records = _matching_entry_registry_records(
        records=active_records,
        requested_strategy=requested_strategy,
        requested_bridge_strategy=requested_bridge_strategy,
        executable_symbol=executable_symbol,
    )
    if matching_records:
        reason_codes.append("duplicate_same_lane_entry")
    elif active_records and not allow_stacking:
        reason_codes.append("strategy_stacking_disabled")
    broker_net_position = abs(float(aggregate_state.get("broker_net_position") or 0.0))
    if broker_net_position > 0.0 and not active_records:
        reason_codes.append("current_broker_position_without_registry_trade")

    phase1_gate = dict(aggregate_state.get("phase1_broker_reconciliation_gate_full") or {})
    broker_open_order_count = _int_value(
        phase1_gate.get("track_b_broker_open_order_count")
        or phase1_gate.get("broker_open_order_count")
        or phase1_gate.get("open_order_count")
    )
    broker_lifecycle_reconciled = bool(phase1_gate.get("ready"))
    if not broker_lifecycle_reconciled:
        reason_codes.append("broker_lifecycle_reconciliation_not_clean")
    if broker_open_order_count > 0:
        reason_codes.append("current_open_order_conflict")
    aggregate_discrepancy = str(aggregate_state.get("discrepancy_classification") or "CLEAN")
    if aggregate_discrepancy == "BROKER_TRUTH_STALE_OR_MISSING":
        reason_codes.append("broker_truth_stale")
    elif aggregate_discrepancy in {"ORPHAN_BROKER_POSITION", "LEDGER_ONLY_POSITION", "LEDGER_BROKER_MISMATCH"}:
        reason_codes.append("broker_lifecycle_reconciliation_not_clean")

    allowed = not reason_codes
    return {
        "allowed": allowed,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "authority_source": _ENTRY_EXPOSURE_AUTHORITY_REGISTRY_TRUTH,
        "trade_id": trade_id or None,
        "active_registry_trade_count": len(active_records),
        "raw_active_registry_trade_count": raw_active_registry_trade_count,
        "matching_registry_trade_ids": [record.trade_id for record in matching_records],
        "contract_entry_status": contract_entry_status,
        "broker_open_order_count": broker_open_order_count,
        "broker_lifecycle_reconciled": broker_lifecycle_reconciled,
        "canonical_current_scope_result": canonical_current_scope,
        "same_symbol_pending_fill_lock": same_symbol_pending_fill_lock,
        "same_symbol_broker_quantity_lock": same_symbol_broker_qty_lock,
        "source_paths": source_paths,
    }


def _safe_build_entry_truth_snapshot(repo_root: Path) -> TrackBTruthSnapshot | None:
    try:
        return build_track_b_truth_snapshot(
            config=TrackBTruthSnapshotConfig(
                repo_root=repo_root,
                reconciliation_path=Path(
                    "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
                ),
            )
        )
    except Exception:
        return None


def _entry_canonical_current_scope_result(repo_root: Path) -> dict[str, Any]:
    sources = {
        "registry_diagnostics": _DEFAULT_REGISTRY_DIAGNOSTICS_PATH,
        "reconciliation": Path(
            "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
        ),
        "managed_positions": _DEFAULT_MANAGED_POSITIONS_PATH,
        "managed_orders": _DEFAULT_MANAGED_ORDERS_PATH,
        "open_order_truth": _DEFAULT_OPEN_ORDER_TRUTH_PATH,
    }
    source_status: dict[str, Any] = {}
    payloads: dict[str, dict[str, Any]] = {}
    reason_codes: list[str] = []
    blocking_fields: list[dict[str, Any]] = []

    for name, relative_path in sources.items():
        path = relative_path if relative_path.is_absolute() else repo_root / relative_path
        payload = _load_json(path)
        payloads[name] = payload
        freshness = _artifact_freshness(payload, max_age_seconds=_DEFAULT_CANONICAL_CURRENT_SCOPE_MAX_AGE_SECONDS)
        source_status[name] = {
            "artifact_path": str(path),
            "classification": payload.get("classification"),
            "generated_at": payload.get("generated_at"),
            **freshness,
        }
        if not payload or not freshness["fresh"]:
            reason_codes.append(_CANONICAL_TRUTH_UNAVAILABLE_REASON)
            blocking_fields.append(
                {
                    "source": name,
                    "field": "generated_at",
                    "value": payload.get("generated_at") if payload else None,
                    "reason": "missing_or_stale_canonical_current_scope_artifact",
                }
            )

    registry = payloads["registry_diagnostics"]
    registry_classification = str(registry.get("classification") or "").strip()
    registry_current_scope_clean = _registry_diagnostics_current_scope_clean(registry)
    if registry and not registry_current_scope_clean:
        reason_codes.append(_CANONICAL_TRUTH_AMBIGUOUS_REASON)
        blocking_fields.append(
            {
                "source": "registry_diagnostics",
                "field": "classification",
                "value": registry_classification,
                "reason": "registry_diagnostics_not_clean_current_scope",
            }
        )

    reconciliation = payloads["reconciliation"]
    reconciliation_classification = str(reconciliation.get("classification") or "").strip()
    current_review_count = _int_value(reconciliation.get("current_scope_review_required_count") or reconciliation.get("review_required_count"))
    broker_position_count = _int_value(reconciliation.get("track_b_broker_position_count") or reconciliation.get("broker_position_count"))
    broker_open_order_count = _int_value(
        reconciliation.get("track_b_broker_open_order_count")
        or reconciliation.get("broker_open_order_count")
        or reconciliation.get("open_order_count")
    )
    if reconciliation:
        if reconciliation_classification != "TRACK_B_PAPER_BROKER_RECONCILED":
            reason_codes.append(_CANONICAL_TRUTH_AMBIGUOUS_REASON)
            blocking_fields.append(
                {
                    "source": "reconciliation",
                    "field": "classification",
                    "value": reconciliation_classification,
                    "reason": "broker_lifecycle_reconciliation_not_canonical_clean",
                }
            )
        if current_review_count > 0:
            reason_codes.append(_CANONICAL_TRUTH_AMBIGUOUS_REASON)
            blocking_fields.append(
                {
                    "source": "reconciliation",
                    "field": "current_scope_review_required_count",
                    "value": current_review_count,
                    "reason": "current_scope_review_required",
                }
            )

    managed_positions = payloads["managed_positions"]
    managed_positions_summary = dict(managed_positions.get("summary") or {})
    managed_position_count = _int_value(
        managed_positions_summary.get("managed_position_count")
        or managed_positions_summary.get("lifecycle_position_count")
        or managed_positions.get("managed_position_count")
    )
    managed_position_review_count = _int_value(
        managed_positions_summary.get("review_required_count") or managed_positions_summary.get("attention_required_count")
    )
    if managed_position_count > 0:
        reason_codes.append(_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON)
        blocking_fields.append(
            {
                "source": "managed_positions",
                "field": "summary.managed_position_count",
                "value": managed_position_count,
                "reason": "canonical_managed_position_present",
            }
        )
    if managed_position_review_count > 0:
        reason_codes.append(_CANONICAL_TRUTH_AMBIGUOUS_REASON)
        blocking_fields.append(
            {
                "source": "managed_positions",
                "field": "summary.review_required_count",
                "value": managed_position_review_count,
                "reason": "managed_position_review_required",
            }
        )

    managed_orders = payloads["managed_orders"]
    managed_orders_summary = dict(managed_orders.get("summary") or {})
    managed_order_count = _int_value(
        managed_orders_summary.get("managed_order_count")
        or managed_orders_summary.get("working_entry_order_count")
        or managed_orders_summary.get("working_close_order_count")
        or managed_orders_summary.get("position_without_close_order_count")
    )
    if managed_order_count > 0:
        reason_codes.append(_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON)
        blocking_fields.append(
            {
                "source": "managed_orders",
                "field": "summary.managed_order_count",
                "value": managed_order_count,
                "reason": "canonical_managed_order_present",
            }
        )

    open_order_truth = payloads["open_order_truth"]
    open_order_summary = dict(open_order_truth.get("summary") or {})
    open_order_count = _int_value(open_order_summary.get("open_order_count") or open_order_truth.get("open_order_count"))
    if open_order_count > 0:
        reason_codes.append(_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON)
        reason_codes.append("current_open_order_conflict")
        blocking_fields.append(
            {
                "source": "open_order_truth",
                "field": "summary.open_order_count",
                "value": open_order_count,
                "reason": "canonical_open_order_present",
            }
        )
    if broker_position_count > 0:
        reason_codes.append(_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON)
        blocking_fields.append(
            {
                "source": "reconciliation",
                "field": "track_b_broker_position_count",
                "value": broker_position_count,
                "reason": "canonical_broker_position_present",
            }
        )
    if broker_open_order_count > 0:
        reason_codes.append(_CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON)
        reason_codes.append("current_open_order_conflict")
        blocking_fields.append(
            {
                "source": "reconciliation",
                "field": "track_b_broker_open_order_count",
                "value": broker_open_order_count,
                "reason": "canonical_broker_open_order_present",
            }
        )

    unique_reasons = list(dict.fromkeys(reason_codes))
    current_exposure_present = _CANONICAL_CURRENT_EXPOSURE_PRESENT_REASON in unique_reasons
    unavailable = _CANONICAL_TRUTH_UNAVAILABLE_REASON in unique_reasons
    ambiguous = _CANONICAL_TRUTH_AMBIGUOUS_REASON in unique_reasons
    return {
        "usable": not unavailable and not ambiguous,
        "allowed": not unique_reasons,
        "current_exposure_present": current_exposure_present,
        "reason_codes": unique_reasons,
        "blocking_fields": blocking_fields,
        "source_status": source_status,
        "canonical_values": {
            "broker_position_count": broker_position_count,
            "broker_open_order_count": broker_open_order_count,
            "current_scope_review_required_count": current_review_count,
            "managed_position_count": managed_position_count,
            "managed_position_review_required_count": managed_position_review_count,
            "managed_order_count": managed_order_count,
            "open_order_count": open_order_count,
            "registry_current_scope_clean": registry_current_scope_clean,
            "registry_diagnostics_classification": registry_classification or None,
            "reconciliation_classification": reconciliation_classification or None,
        },
    }


def _registry_diagnostics_current_scope_clean(payload: dict[str, Any]) -> bool:
    classification = str(payload.get("classification") or "").strip()
    if not classification:
        return False
    if classification == "TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE":
        return False
    if classification == "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE":
        return True
    if payload.get("diagnostic_only") is not True:
        return False
    if "HISTORICAL" not in classification:
        return False

    current_review_count = _int_value(payload.get("current_scope_review_required_count"))
    current_scope_trade_states = payload.get("current_scope_trade_states")
    current_blockers = payload.get("current_blockers")
    review_required_trade_ids = payload.get("review_required_trade_ids")

    current_evidence_present = (
        "current_scope_review_required_count" in payload
        or "review_required_trade_ids" in payload
        or "current_scope_trade_states" in payload
    )
    if not current_evidence_present:
        return False
    if current_review_count != 0:
        return False
    if _list_value(current_scope_trade_states):
        return False
    if _list_value(current_blockers):
        return False
    if review_required_trade_ids is not None and _list_value(review_required_trade_ids):
        return False

    lifecycle_open_position_count = _int_value(payload.get("lifecycle_open_position_count"))
    lifecycle_open_order_count = _int_value(payload.get("lifecycle_open_order_count"))
    track_b_position_count = _int_value(
        payload.get("track_b_broker_position_count") or payload.get("track_b_managed_futures_position_count")
    )
    track_b_open_order_count = _int_value(
        payload.get("track_b_broker_open_order_count") or payload.get("broker_open_order_count")
    )
    unknown_scope_position_count = _int_value(payload.get("unknown_scope_position_count"))
    broker_lifecycle_reconciled = payload.get("broker_lifecycle_reconciled")
    if broker_lifecycle_reconciled is not None and broker_lifecycle_reconciled is not True:
        return False
    return (
        lifecycle_open_position_count == 0
        and lifecycle_open_order_count == 0
        and track_b_position_count == 0
        and track_b_open_order_count == 0
        and unknown_scope_position_count == 0
    )


def _list_value(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _artifact_freshness(payload: dict[str, Any], *, max_age_seconds: float) -> dict[str, Any]:
    generated_at = payload.get("generated_at") if payload else None
    if not generated_at:
        return {"fresh": False, "age_seconds": None, "max_age_seconds": max_age_seconds, "freshness": "missing"}
    try:
        parsed = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    except ValueError:
        return {"fresh": False, "age_seconds": None, "max_age_seconds": max_age_seconds, "freshness": "invalid"}
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()
    return {
        "fresh": age_seconds <= max_age_seconds,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "freshness": "fresh" if age_seconds <= max_age_seconds else "stale",
    }


def _entry_birth_trade_id(
    *,
    config: IbkrPaperStrategyExposureConfig,
    requested_strategy: str,
    executable_symbol: str,
) -> str:
    explicit = str(config.trade_id or "").strip()
    if explicit:
        return explicit
    strategy = str(requested_strategy or config.strategy_id or "").strip()
    symbol = str(executable_symbol or config.executable_symbol or "").strip().upper()
    if strategy and symbol:
        return f"trade_birth_path_{strategy}_{symbol}".replace("/", "_")
    return ""


def _entry_truth_contract_block_reasons(truth: TrackBTruthSnapshot) -> list[str]:
    reasons: list[str] = []
    if truth.contract_status.entry_status not in {CONTRACT_ENTRY_ELIGIBLE, "ENTRY_ALLOWED", "CONTRACT_ENTRY_ALLOWED"}:
        reasons.append(str(truth.contract_status.entry_status or CONTRACT_ENTRY_BLOCKED))
    for conflict in truth.conflicts:
        if conflict.classification in {CONTRACT_DETAILS_STALE, CONTRACT_AMBIGUOUS, CONTRACT_ENTRY_BLOCKED, CONTRACT_ENTRY_CLOSE_ONLY}:
            reasons.append(conflict.classification)
            reasons.extend(str(code) for code in conflict.reason_codes)
    return list(dict.fromkeys(reasons))


def _matching_entry_registry_records(
    *,
    records: tuple[TradeRegistryRecord, ...],
    requested_strategy: str,
    requested_bridge_strategy: str,
    executable_symbol: str,
) -> tuple[TradeRegistryRecord, ...]:
    identifiers = {str(requested_strategy or "").strip(), str(requested_bridge_strategy or "").strip()}
    identifiers.discard("")
    symbol = str(executable_symbol or "").strip().upper()
    matches: list[TradeRegistryRecord] = []
    for record in records:
        owner = record.ownership_identity
        if owner is None:
            continue
        owner_ids = {str(owner.lane_id or "").strip(), str(owner.thesis_strategy_id or "").strip()}
        owner_ids.discard("")
        if identifiers and owner_ids.intersection(identifiers) and str(owner.symbol or "").strip().upper() == symbol:
            matches.append(record)
    return tuple(matches)


def _filter_owned_rows_for_requested_exit_identity(
    *,
    owned_rows: list[dict[str, Any]],
    account_id: str | None,
    con_id: int | None,
    local_symbol: str | None,
    lifecycle_id: str | None,
) -> list[dict[str, Any]]:
    requested_account = _valid_owner_identity_value(account_id)
    requested_con_id = str(con_id or "").strip()
    requested_local_symbol = str(local_symbol or "").strip().upper()
    requested_lifecycle_id = str(lifecycle_id or "").strip()
    filtered: list[dict[str, Any]] = []
    for row in owned_rows:
        row_account = _valid_owner_identity_value(row.get("account_id")) or _valid_owner_identity_value(row.get("broker_account_id"))
        if requested_account and row_account and row_account != requested_account:
            continue
        if requested_con_id and str(row.get("con_id") or "").strip() != requested_con_id:
            continue
        if requested_local_symbol and str(row.get("local_symbol") or "").strip().upper() != requested_local_symbol:
            continue
        row_lifecycle_id = str(row.get("lifecycle_id") or row.get("source_intent_id") or "").strip()
        if requested_lifecycle_id and row_lifecycle_id != requested_lifecycle_id:
            continue
        filtered.append(dict(row))
    return filtered


def _managed_exit_registry_truth_result(
    *,
    registry_exit_validation: dict[str, Any],
    aggregate_state: dict[str, Any],
    requested_identifiers: set[str],
) -> dict[str, Any]:
    reason_codes = [
        str(reason)
        for reason in list(registry_exit_validation.get("block_reasons") or [])
        if str(reason or "").strip()
    ]
    phase1_gate_full = dict(aggregate_state.get("phase1_broker_reconciliation_gate_full") or {})
    open_order_count = _int_value(
        phase1_gate_full.get("track_b_broker_open_order_count")
        or phase1_gate_full.get("broker_open_order_count")
        or phase1_gate_full.get("open_order_count")
    )
    if open_order_count > 0:
        reason_codes.append("open_order_conflict")
    owner_identity = dict(registry_exit_validation.get("owner_identity") or {})
    owner_identifiers = {
        str(owner_identity.get("lane_id") or "").strip(),
        str(owner_identity.get("strategy_id") or "").strip(),
    }
    owner_identifiers.discard("")
    if requested_identifiers and owner_identifiers and not requested_identifiers.intersection(owner_identifiers):
        reason_codes.append("non_owning_strategy_exit_forbidden")
    allowed = bool(registry_exit_validation.get("allowed")) and not reason_codes
    return {
        "allowed": allowed,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "authority_source": _MANAGED_EXIT_AUTHORITY_REGISTRY_TRUTH,
        "owner_identity": owner_identity,
        "broker_position": dict(registry_exit_validation.get("broker_position") or {}),
        "lifecycle_row": dict(registry_exit_validation.get("lifecycle_row") or {}),
        "open_order_count": open_order_count,
        "safe_state_submit_authority": "DEFERRED_TO_EXISTING_SUBMIT_GATE",
        "control_plane_authority": "DEFERRED_TO_EXISTING_SUBMIT_GATE",
    }


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _strategy_row_from_registry_owner_identity(
    owner: dict[str, Any],
    *,
    lifecycle_id: str,
) -> dict[str, Any]:
    quantity = float(owner.get("quantity") or 0.0)
    side = str(owner.get("side") or "").strip().upper()
    state = _normalize_strategy_state(quantity=quantity, side=side, raw_state=side)
    signed_quantity = quantity if state == "LONG" else (-quantity if state == "SHORT" else 0.0)
    strategy_id = str(owner.get("strategy_id") or "").strip()
    lane_id = str(owner.get("lane_id") or "").strip()
    return {
        "strategy_id": strategy_id,
        "strategy_aliases": [value for value in (strategy_id, lane_id) if value],
        "lane_id": lane_id,
        "lane_ids": [lane_id] if lane_id else [],
        "strategy_ids": [strategy_id] if strategy_id else [],
        "account_id": owner.get("account_id"),
        "broker_account_id": owner.get("account_id"),
        "symbol": owner.get("symbol"),
        "expiry": owner.get("expiry"),
        "con_id": owner.get("con_id"),
        "local_symbol": owner.get("local_symbol"),
        "direction": state,
        "quantity": quantity,
        "signed_quantity": signed_quantity,
        "perm_id": owner.get("entry_perm_id"),
        "execution_id": owner.get("entry_exec_id"),
        "source_intent_id": lifecycle_id,
        "lifecycle_id": lifecycle_id,
        "state": state if quantity > 0.0 else "FLAT",
        "open_orders": [],
        "pnl_source": "central_trade_registry",
    }


def _valid_owner_identity_value(value: Any) -> str:
    text = str(value or "").strip()
    if text.upper() in {"", "MULTIPLE", "MISSING", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def _load_unresolved_submit_intents(config: IbkrPaperStrategyExposureConfig) -> list[dict[str, Any]]:
    path = config.submit_intent_ownership_path
    if not path.is_absolute():
        path = config.repo_root / path
    return [dict(row) for row in load_unresolved_submit_intent_ownership_records(path)]


def _unresolved_submit_intent_new_entry_blocker(
    *,
    config: IbkrPaperStrategyExposureConfig,
    requested_strategy: str,
    requested_bridge_strategy: str,
    unresolved_submit_intents: list[dict[str, Any]],
) -> dict[str, Any] | None:
    matching_records: list[dict[str, Any]] = []
    for record in unresolved_submit_intents:
        match_reason = _unresolved_submit_intent_match_reason(
            record=record,
            config=config,
            requested_strategy=requested_strategy,
            requested_bridge_strategy=requested_bridge_strategy,
        )
        if not match_reason:
            continue
        matching_records.append(
            {
                "ownership_intent_id": record.get("ownership_intent_id"),
                "state": record.get("state"),
                "match_reason": match_reason,
                "account_id": record.get("account_id"),
                "lane_id": record.get("lane_id"),
                "strategy_id": record.get("strategy_id"),
                "symbol": record.get("symbol"),
                "local_symbol": record.get("local_symbol"),
                "expiry": record.get("expiry"),
                "con_id": record.get("con_id"),
                "action": record.get("action"),
                "qty": record.get("qty"),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
            }
        )
    if not matching_records:
        return None
    return {
        "classification": _UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON,
        "detail": "Unresolved Track B submit-intent ownership overlaps this new entry by account/contract, lane/strategy, or instrument.",
        "matching_record_count": len(matching_records),
        "matching_records": matching_records,
    }


def _same_symbol_pending_fill_entry_lock(
    *,
    config: IbkrPaperStrategyExposureConfig,
    requested_direction: str | None,
    allow_stacking: bool,
    unresolved_submit_intents: list[dict[str, Any]],
) -> dict[str, Any] | None:
    requested = str(requested_direction or "").strip().upper()
    if requested not in {"LONG", "SHORT"}:
        return None

    matching_records: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    for record in unresolved_submit_intents:
        if not _unresolved_submit_intent_same_account_contract(record=record, config=config):
            continue
        record_direction = _unresolved_submit_intent_entry_direction(record)
        if record_direction not in {"LONG", "SHORT"}:
            continue

        lock_reason = ""
        if _directions_are_opposite(requested, record_direction):
            lock_reason = _SAME_SYMBOL_PENDING_FILL_ANTI_FLIP_REASON
        elif not allow_stacking and _unresolved_submit_intent_has_broker_effect_or_adoption_risk(record):
            lock_reason = _SAME_SYMBOL_UNRESOLVED_EXPOSURE_REASON
        if not lock_reason:
            continue

        reason_codes.append(lock_reason)
        matching_records.append(
            {
                "ownership_intent_id": record.get("ownership_intent_id"),
                "state": record.get("state"),
                "reason_code": lock_reason,
                "match_reason": "same_account_contract",
                "account_id": record.get("account_id"),
                "lane_id": record.get("lane_id"),
                "strategy_id": record.get("strategy_id"),
                "symbol": record.get("symbol"),
                "local_symbol": record.get("local_symbol"),
                "expiry": record.get("expiry"),
                "con_id": record.get("con_id"),
                "action": record.get("action"),
                "intent_type": record.get("intent_type"),
                "direction": record_direction,
                "qty": record.get("qty"),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
                "trade_id": dict(record.get("extra") or {}).get("trade_id"),
            }
        )

    if not matching_records:
        return None
    return {
        "classification": "SAME_SYMBOL_PENDING_FILL_OR_UNRESOLVED_EXPOSURE_ENTRY_LOCK",
        "detail": (
            "Same-account/contract unresolved submit or broker-effect ownership blocks new entries until "
            "broker ack, fill adoption, and reconciliation are current-scope clean for that symbol."
        ),
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "matching_record_count": len(matching_records),
        "matching_records": matching_records,
    }


def _same_symbol_broker_quantity_entry_lock(
    *,
    config: IbkrPaperStrategyExposureConfig,
    requested_direction: str | None,
    allow_stacking: bool,
) -> dict[str, Any] | None:
    requested = str(requested_direction or "").strip().upper()
    if requested not in {"LONG", "SHORT"}:
        return None

    positions_path = config.repo_root / config.broker_positions_snapshot_path
    positions_payload = _load_json(positions_path)
    positions_fresh = _snapshot_freshness_with_requirements(
        payload=positions_payload,
        path=positions_path,
        max_age_seconds=float(config.broker_truth_max_age_seconds),
        require_ok=True,
        completeness_key="positions_complete",
    )
    if not positions_fresh.get("fresh"):
        return None

    matching_rows: list[dict[str, Any]] = []
    reason_codes: list[str] = []
    for row in list(positions_payload.get("positions") or []):
        if not isinstance(row, dict):
            continue
        if not _broker_position_row_same_account_contract(row=row, config=config, default_account=positions_payload.get("selected_account_id")):
            continue
        qty = _broker_position_quantity(row)
        if qty == 0.0:
            continue

        broker_direction = "LONG" if qty > 0.0 else "SHORT"
        lock_reason = ""
        if _directions_are_opposite(requested, broker_direction):
            lock_reason = _SAME_SYMBOL_BROKER_QTY_ANTI_FLIP_REASON
        elif not allow_stacking:
            lock_reason = _SAME_SYMBOL_UNRESOLVED_EXPOSURE_REASON
        if not lock_reason:
            continue

        reason_codes.append(lock_reason)
        matching_rows.append(
            {
                "reason_code": lock_reason,
                "match_reason": "same_account_contract_broker_position",
                "account_id": row.get("account_id") or row.get("account") or positions_payload.get("selected_account_id"),
                "symbol": row.get("symbol"),
                "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
                "con_id": row.get("con_id") or row.get("conId"),
                "quantity": qty,
                "direction": broker_direction,
                "avg_cost": row.get("avg_cost") or row.get("avgCost") or row.get("average_cost") or row.get("averageCost"),
            }
        )

    if not matching_rows:
        return None
    return {
        "classification": "SAME_SYMBOL_BROKER_QUANTITY_ENTRY_LOCK",
        "detail": (
            "Exact same-account/contract broker quantity is nonzero; new entry orders are blocked until "
            "broker truth is flat/current-scope clean. Risk-reducing managed closes are not blocked by this entry lock."
        ),
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "matching_record_count": len(matching_rows),
        "matching_records": matching_rows,
        "positions_snapshot": positions_fresh,
    }


def _broker_position_row_same_account_contract(
    *,
    row: dict[str, Any],
    config: IbkrPaperStrategyExposureConfig,
    default_account: Any = None,
) -> bool:
    requested_account = _valid_owner_identity_value(config.account_id)
    row_account = _valid_owner_identity_value(row.get("account_id") or row.get("account") or default_account)
    if requested_account and row_account and requested_account != row_account:
        return False
    if requested_account and not row_account:
        return False

    config_con_id = _int_or_none(config.con_id)
    row_con_id = _int_or_none(row.get("con_id") or row.get("conId"))
    if config_con_id is not None:
        return row_con_id == config_con_id

    config_local = str(config.local_symbol or "").strip().upper()
    row_local = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    if config_local:
        return bool(row_local) and row_local == config_local
    return False


def _broker_position_quantity(row: dict[str, Any]) -> float:
    for key in ("quantity", "position", "qty", "position_qty"):
        if key not in row or row.get(key) is None:
            continue
        try:
            return float(row.get(key))
        except (TypeError, ValueError):
            continue
    return 0.0


def _unresolved_submit_intent_entry_direction(record: dict[str, Any]) -> str | None:
    semantics = _normalize_intent_semantics(
        action=str(record.get("action") or "").strip().upper(),
        intent_type=str(record.get("intent_type") or "").strip().upper(),
    )
    if semantics.operation != "OPEN":
        return None
    return semantics.direction


def _directions_are_opposite(left: str, right: str) -> bool:
    return {str(left or "").strip().upper(), str(right or "").strip().upper()} == {"LONG", "SHORT"}


def _unresolved_submit_intent_has_broker_effect_or_adoption_risk(record: dict[str, Any]) -> bool:
    state = str(record.get("state") or "").strip().upper()
    return (
        state in {"BROKER_ORDER_WORKING", "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED", "REVIEW_REQUIRED"}
        or "ADOPTION" in state
        or "BROKER_POSITION" in state
        or "FILL" in state
    )


def _unresolved_submit_intent_match_reason(
    *,
    record: dict[str, Any],
    config: IbkrPaperStrategyExposureConfig,
    requested_strategy: str,
    requested_bridge_strategy: str,
) -> str | None:
    record_lane = str(record.get("lane_id") or "").strip()
    record_strategy = str(record.get("strategy_id") or "").strip()
    requested_ids = {requested_strategy, requested_bridge_strategy}
    requested_ids.discard("")
    if requested_ids.intersection({record_lane, record_strategy}):
        return "same_lane_or_strategy"
    if _unresolved_submit_intent_same_account_contract(record=record, config=config):
        return "same_account_contract"
    if _unresolved_submit_intent_same_instrument(record=record, executable_symbol=config.executable_symbol):
        return "same_instrument"
    return None


def _unresolved_submit_intent_same_account_contract(
    *,
    record: dict[str, Any],
    config: IbkrPaperStrategyExposureConfig,
) -> bool:
    requested_account = _valid_owner_identity_value(config.account_id)
    record_account = _valid_owner_identity_value(record.get("account_id"))
    if requested_account and record_account and requested_account != record_account:
        return False
    if not record_account:
        return False
    config_con_id = _int_or_none(config.con_id)
    record_con_id = _int_or_none(record.get("con_id"))
    if config_con_id is not None and record_con_id is not None:
        return config_con_id == record_con_id
    config_local = str(config.local_symbol or "").strip().upper()
    record_local = str(record.get("local_symbol") or "").strip().upper()
    if config_local and record_local:
        return config_local == record_local
    return False


def _unresolved_submit_intent_same_instrument(*, record: dict[str, Any], executable_symbol: str) -> bool:
    executable = str(executable_symbol or "").strip().upper()
    record_symbol = str(record.get("symbol") or "").strip().upper()
    record_local = str(record.get("local_symbol") or "").strip().upper()
    return bool(executable) and (record_symbol == executable or record_local.startswith(executable))


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _broker_net_position_for_symbol(
    *,
    config: IbkrPaperStrategyExposureConfig,
    monitor_status: dict[str, Any],
    executable_symbol: str,
) -> tuple[float, dict[str, Any]]:
    positions_path = config.repo_root / config.broker_positions_snapshot_path
    positions_payload = _load_json(positions_path)
    positions_fresh = _snapshot_freshness_with_requirements(
        payload=positions_payload,
        path=positions_path,
        max_age_seconds=float(config.broker_truth_max_age_seconds),
        require_ok=True,
        completeness_key="positions_complete",
    )
    open_orders_path = config.repo_root / config.broker_open_orders_snapshot_path
    open_orders_payload = _load_json(open_orders_path)
    open_orders_fresh = _snapshot_freshness_with_requirements(
        payload=open_orders_payload,
        path=open_orders_path,
        max_age_seconds=float(config.broker_truth_max_age_seconds),
        require_ok=True,
        completeness_key="open_orders_complete",
    )
    if positions_fresh["fresh"] and open_orders_fresh["fresh"]:
        return (
            _net_position_from_rows(list(positions_payload.get("positions") or []), executable_symbol),
            {
                "truth_available": True,
                "source": "ibkr_read_only_positions_and_open_orders_snapshot",
                "symbol": executable_symbol,
                "positions_snapshot": positions_fresh,
                "open_orders_snapshot": open_orders_fresh,
                "generated_at": positions_fresh.get("generated_at"),
                "broker_refresh_timestamp": positions_fresh.get("generated_at"),
                "account": positions_payload.get("selected_account_id") or open_orders_payload.get("selected_account_id"),
                "required_freshness_threshold_seconds": float(config.broker_truth_max_age_seconds),
            },
        )

    if executable_symbol == "MGC":
        return (
            float(monitor_status.get("broker_position_quantity") or 0.0),
            {
                "truth_available": True,
                "source": "paper_strategy_monitor_runtime_status",
                "symbol": executable_symbol,
                "generated_at": monitor_status.get("last_successful_broker_refresh"),
                "max_age_seconds": float(config.broker_truth_max_age_seconds),
            },
        )

    index_path = config.repo_root / config.index_exposure_snapshot_path
    index_payload = _load_json(index_path)
    index_fresh = _snapshot_freshness(
        payload=index_payload,
        path=index_path,
        max_age_seconds=float(config.broker_truth_max_age_seconds),
    )
    if executable_symbol == "MNQ" and index_fresh["fresh"]:
        return (
            float(index_payload.get("broker_net_mnq") or 0.0),
            {
                **index_fresh,
                "truth_available": True,
                "source": "mnq_nq_scope_support_index_exposure",
                "symbol": executable_symbol,
            },
        )

    return (
        0.0,
        {
            "truth_available": False,
            "source": "missing_or_stale_non_mgc_broker_truth",
            "symbol": executable_symbol,
            "positions_snapshot": positions_fresh,
            "open_orders_snapshot": open_orders_fresh,
            "index_exposure_snapshot": index_fresh,
            "broker_refresh_timestamp": positions_fresh.get("generated_at"),
            "account": positions_payload.get("selected_account_id") or open_orders_payload.get("selected_account_id"),
            "required_freshness_threshold_seconds": float(config.broker_truth_max_age_seconds),
            "max_age_seconds": float(config.broker_truth_max_age_seconds),
        },
    )


def _snapshot_freshness(*, payload: dict[str, Any], path: Path, max_age_seconds: float) -> dict[str, Any]:
    return _snapshot_freshness_with_requirements(
        payload=payload,
        path=path,
        max_age_seconds=max_age_seconds,
    )


def _snapshot_freshness_with_requirements(
    *,
    payload: dict[str, Any],
    path: Path,
    max_age_seconds: float,
    require_ok: bool = False,
    completeness_key: str | None = None,
) -> dict[str, Any]:
    generated_at = str(payload.get("generated_at") or "").strip()
    if not payload or not generated_at:
        return {
            "fresh": False,
            "path": str(path),
            "generated_at": generated_at or None,
            "age_seconds": None,
            "max_age_seconds": float(max_age_seconds),
            "reason": "missing_snapshot_or_timestamp",
        }
    try:
        generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return {
            "fresh": False,
            "path": str(path),
            "generated_at": generated_at,
            "age_seconds": None,
            "max_age_seconds": float(max_age_seconds),
            "reason": "invalid_snapshot_timestamp",
        }
    if generated_dt.tzinfo is None:
        generated_dt = generated_dt.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (datetime.now(timezone.utc) - generated_dt.astimezone(timezone.utc)).total_seconds())
    if require_ok and payload.get("ok") is not True:
        return {
            "fresh": False,
            "path": str(path),
            "generated_at": generated_at,
            "age_seconds": age_seconds,
            "max_age_seconds": float(max_age_seconds),
            "reason": "snapshot_not_ok",
        }
    if completeness_key is not None and payload.get(completeness_key) is not True:
        return {
            "fresh": False,
            "path": str(path),
            "generated_at": generated_at,
            "age_seconds": age_seconds,
            "max_age_seconds": float(max_age_seconds),
            "reason": f"{completeness_key}_false_or_missing",
        }
    return {
        "fresh": age_seconds <= float(max_age_seconds),
        "path": str(path),
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "max_age_seconds": float(max_age_seconds),
        "reason": "fresh" if age_seconds <= float(max_age_seconds) else "stale_snapshot",
    }


def _net_position_from_rows(rows: list[dict[str, Any]], symbol: str) -> float:
    target = str(symbol or "").strip().upper()
    total = 0.0
    for row in rows:
        row_symbol = str(row.get("symbol") or "").strip().upper()
        local_symbol = str(row.get("local_symbol") or "").strip().upper()
        if row_symbol != target and not local_symbol.startswith(target):
            continue
        total += float(row.get("quantity") or 0.0)
    return round(total, 8)


def _normalize_intent_semantics(*, action: str, intent_type: str | None) -> _IntentSemantics:
    normalized_action = str(action or "").strip().upper()
    normalized_intent_type = str(intent_type or "").strip().upper()
    if normalized_intent_type == "BUY_TO_OPEN":
        return _IntentSemantics(
            operation="OPEN",
            direction="LONG",
            broker_action="BUY",
            explicit_intent_type=True,
        )
    if normalized_intent_type == "SELL_TO_OPEN":
        return _IntentSemantics(
            operation="OPEN",
            direction="SHORT",
            broker_action="SELL",
            explicit_intent_type=True,
        )
    if normalized_intent_type == "SELL_TO_CLOSE":
        return _IntentSemantics(
            operation="CLOSE",
            direction="LONG",
            broker_action="SELL",
            explicit_intent_type=True,
        )
    if normalized_intent_type == "BUY_TO_CLOSE":
        return _IntentSemantics(
            operation="CLOSE",
            direction="SHORT",
            broker_action="BUY",
            explicit_intent_type=True,
        )
    if normalized_action in _SUPPORTED_ENTRY_ACTIONS:
        return _IntentSemantics(
            operation="OPEN",
            direction="LONG",
            broker_action=normalized_action,
            explicit_intent_type=False,
        )
    if normalized_action in _SUPPORTED_EXIT_ACTIONS:
        return _IntentSemantics(
            operation="CLOSE",
            direction="LONG",
            broker_action=normalized_action,
            explicit_intent_type=False,
        )
    return _IntentSemantics(
        operation="UNKNOWN",
        direction=None,
        broker_action=normalized_action,
        explicit_intent_type=False,
    )


def _normalize_strategy_state(*, quantity: float, side: str, raw_state: str) -> str:
    normalized_raw = str(raw_state or "").strip().upper()
    if quantity <= 0.0:
        return "FLAT"
    if side in {"LONG", "BUY", "BOT"}:
        return "LONG"
    if side in {"SHORT", "SELL", "SLD"}:
        return "SHORT"
    if normalized_raw in {"OPEN", "LONG"}:
        return "LONG"
    if normalized_raw == "SHORT":
        return "SHORT"
    return "UNKNOWN"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return dict(json.loads(path.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def _record_audit(
    audit_events: list[dict[str, Any]],
    event_type: str,
    detail: str,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    row = {
        "event_type": event_type,
        "detail": detail,
        "recorded_at": _utc_now(),
    }
    if extra:
        row.update(extra)
    audit_events.append(row)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
