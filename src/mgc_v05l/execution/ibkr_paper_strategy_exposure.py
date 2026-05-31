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
from ..execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    load_unresolved_submit_intent_ownership_records,
)
from ..execution_core.track_b_live_trade_registry import (
    DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
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
_UNRESOLVED_SUBMIT_INTENT_BLOCK_REASON = "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY"
_SUPPORTED_ENTRY_ACTIONS = {"BUY"}
_SUPPORTED_EXIT_ACTIONS = {"SELL", "EXIT"}
_DEFAULT_MAX_TOTAL_MGC_CONTRACTS = 20.0
_DEFAULT_MAX_TOTAL_GC_EQUIVALENT = 2.0
_DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS = 1.0
_DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS = 300.0


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
    elif semantics.operation == "CLOSE":
        if registry_exit_validation is not None and registry_exit_validation.get("allowed") is not True:
            block_reasons.extend(str(reason) for reason in list(registry_exit_validation.get("block_reasons") or []))
        if exit_identity_requested and not requested_lifecycle_id and any(
            str(row.get("lifecycle_id") or row.get("source_intent_id") or "").strip()
            and str(row.get("pnl_source") or "").strip() == "phase1_broker_reconciliation"
            for row in owned_rows
        ):
            block_reasons.append("missing_lifecycle_identity")
        if exit_identity_requested and not identity_filtered_owned_rows:
            block_reasons.append("exit_identity_mismatch")
        if semantics.direction == "LONG" and strategy_state != "LONG":
            block_reasons.append("non_owning_strategy_exit_forbidden")
        if semantics.direction == "SHORT" and strategy_state != "SHORT":
            block_reasons.append("non_owning_strategy_exit_forbidden")
        if quantity <= 0.0 or quantity > owned_quantity:
            block_reasons.append("exit_quantity_exceeds_owned_strategy_position")
        broker_net_position = float(aggregate_state.get("broker_net_position") or 0.0)
        if semantics.direction == "LONG" and broker_net_position < quantity:
            block_reasons.append("broker_position_does_not_support_requested_exit")
        if semantics.direction == "SHORT" and broker_net_position > -quantity:
            block_reasons.append("broker_position_does_not_support_requested_exit")
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
            }
        ),
    }


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
    if str(record.get("account_id") or "") != "DUM882026":
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
