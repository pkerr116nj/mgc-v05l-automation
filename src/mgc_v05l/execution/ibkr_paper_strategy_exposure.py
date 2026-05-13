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
    allow_stacking: bool = True
    max_total_mgc_contracts: float | None = _DEFAULT_MAX_TOTAL_MGC_CONTRACTS
    max_total_gc_equivalent: float = _DEFAULT_MAX_TOTAL_GC_EQUIVALENT
    max_per_strategy_mgc_contracts: float = _DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS
    allow_long_and_short_netting: bool = False
    allow_direct_strategy_flip: bool = False
    broker_positions_snapshot_path: Path = _DEFAULT_BROKER_POSITIONS_SNAPSHOT
    broker_open_orders_snapshot_path: Path = _DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT
    index_exposure_snapshot_path: Path = _DEFAULT_INDEX_EXPOSURE_SNAPSHOT
    broker_truth_max_age_seconds: float = _DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS


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
    selected_strategy_gate = _evaluate_strategy_gate(
        config=config,
        strategy_rows=strategy_exposure_rows,
        aggregate_state=aggregate_state,
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
    allow_stacking: bool = True,
    max_total_mgc_contracts: float | None = None,
    max_total_gc_equivalent: float = _DEFAULT_MAX_TOTAL_GC_EQUIVALENT,
    max_per_strategy_mgc_contracts: float = _DEFAULT_MAX_PER_STRATEGY_MGC_CONTRACTS,
    allow_long_and_short_netting: bool = False,
    allow_direct_strategy_flip: bool = False,
    broker_positions_snapshot_path: Path = _DEFAULT_BROKER_POSITIONS_SNAPSHOT,
    broker_open_orders_snapshot_path: Path = _DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT,
    index_exposure_snapshot_path: Path = _DEFAULT_INDEX_EXPOSURE_SNAPSHOT,
    broker_truth_max_age_seconds: float = _DEFAULT_BROKER_TRUTH_MAX_AGE_SECONDS,
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
    rows: list[dict[str, Any]] = []
    for position in list(phase1_reconciliation_gate.get("track_b_lifecycle_positions") or []):
        quantity = float(position.get("quantity") or 0.0)
        if quantity <= 0.0:
            continue
        side = str(position.get("side") or "").strip().upper()
        state = _normalize_strategy_state(quantity=quantity, side=side, raw_state=side)
        signed_quantity = quantity if state == "LONG" else (-quantity if state == "SHORT" else 0.0)
        strategy_id = str(position.get("strategy_id") or "").strip()
        rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_aliases": _strategy_aliases_for_phase1_lifecycle_position(strategy_id),
                "account_id": position.get("account_id"),
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
                "state": state if quantity > 0.0 else "FLAT",
                "open_orders": [],
                "pnl_source": "phase1_broker_reconciliation",
                "last_reconciliation_timestamp": phase1_reconciliation_gate.get("generated_at"),
            }
        )
    return rows


def _strategy_aliases_for_phase1_lifecycle_position(strategy_id: str) -> list[str]:
    normalized = str(strategy_id or "").strip()
    aliases: list[str] = []
    if normalized:
        aliases.append(normalized)
    if "__" in normalized:
        root, suffix = normalized.split("__", 1)
        if suffix:
            aliases.append(suffix)
        if suffix.startswith("paper_") and root:
            aliases.append(f"{root}_{suffix.removeprefix('paper_')}")
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
) -> dict[str, Any]:
    requested_strategy = str(config.strategy_id or "").strip()
    requested_bridge_strategy = str(config.bridge_strategy_id or "").strip()
    action = str(config.action or "OBSERVE").strip().upper()
    semantics = _normalize_intent_semantics(action=action, intent_type=config.intent_type)
    quantity = float(config.quantity or 0.0)
    identifiers = {requested_strategy, requested_bridge_strategy}
    identifiers.discard("")
    owned_rows = []
    for row in strategy_rows:
        row_identifiers = {str(row.get("strategy_id") or "").strip()}
        row_identifiers.update(str(alias or "").strip() for alias in list(row.get("strategy_aliases") or []))
        row_identifiers.discard("")
        if row_identifiers.intersection(identifiers):
            owned_rows.append(dict(row))
    owned_signed_quantity = round(sum(float(row.get("signed_quantity") or 0.0) for row in owned_rows), 8)
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
        if strategy_state in {"LONG", "SHORT"}:
            block_reasons.append("duplicate_strategy_entry_while_position_open")
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
        "blocker_classification": (
            "PHASE1_BROKER_RECONCILIATION_NOT_CLEAR"
            if "phase1_broker_reconciliation_not_clear" in block_reasons
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
            }
        ),
    }


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
