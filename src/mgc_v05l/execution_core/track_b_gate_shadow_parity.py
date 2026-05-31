"""Read-only registry/truth shadow parity for Track B PAPER gates.

This module compares existing guarded PAPER gate decisions with a shadow
decision derived from the central trade registry and ``TrackBTruthSnapshot``.
It is diagnostic only: it does not submit, cancel, close, flatten, restart, or
mutate broker/runtime state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    CONTROL_PLANE_STALE,
    CONTRACT_ENTRY_BLOCKED,
    CONTRACT_ENTRY_ELIGIBLE,
    SAFE_STATE_SUBMIT_BLOCKED,
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TrackBTruthSnapshot,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeRegistryRecord,
)
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    load_live_trade_registry_records,
)


DEFAULT_GATE_SHADOW_PARITY_REPORT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "gate_shadow_parity"
    / "latest_gate_shadow_parity_report.json"
)

GATE_SHADOW_PARITY_OK = "GATE_SHADOW_PARITY_OK"
GATE_SHADOW_PARITY_MISMATCH = "GATE_SHADOW_PARITY_MISMATCH"
GATE_SHADOW_REVIEW_REQUIRED = "GATE_SHADOW_REVIEW_REQUIRED"


class TrackBGateName(str, Enum):
    ENTRY_EXPOSURE = "entry_exposure_gate"
    MANAGED_EXIT_EXPOSURE = "managed_exit_exposure_gate"
    GOVERNANCE_SUBMIT = "governance_submit_gate"
    SAFE_STATE_SUBMIT = "safe_state_submit_allowed"
    NO_WORKING_ORDER = "no_working_order_gate"
    RUNTIME_AUTHORITY = "duplicate_writer_runtime_authority_gate"


@dataclass(frozen=True)
class TrackBGateShadowContext:
    repo_root: Path
    lane_id: str
    thesis_strategy_id: str
    action: str
    quantity: Decimal
    symbol: str
    trade_id: str | None = None
    lifecycle_id: str | None = None
    existing_gate_results: Mapping[str, Mapping[str, Any]] | None = None
    truth_snapshot: TrackBTruthSnapshot | None = None
    registry_records: Sequence[TradeRegistryRecord] | None = None
    generated_at: datetime | None = None


@dataclass(frozen=True)
class TrackBGateDecision:
    gate_name: str
    allowed: bool
    reason_codes: tuple[str, ...]
    source: str
    lane_id: str | None = None
    trade_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "allowed": self.allowed,
            "reason_codes": list(self.reason_codes),
            "source": self.source,
            "lane_id": self.lane_id,
            "trade_id": self.trade_id,
        }


@dataclass(frozen=True)
class TrackBGateShadowRow:
    gate_name: str
    existing_gate_result: TrackBGateDecision
    registry_truth_gate_result: TrackBGateDecision
    parity: bool
    mismatch_reason_codes: tuple[str, ...]
    lane_id: str
    trade_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "existing_gate_result": self.existing_gate_result.to_dict(),
            "registry_truth_gate_result": self.registry_truth_gate_result.to_dict(),
            "parity": self.parity,
            "mismatch_reason_codes": list(self.mismatch_reason_codes),
            "lane_id": self.lane_id,
            "trade_id": self.trade_id,
        }


@dataclass(frozen=True)
class TrackBGateShadowReport:
    schema_version: str
    generated_at: datetime
    classification: str
    behavior_change_allowed: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    lane_id: str
    trade_id: str | None
    rows: tuple[TrackBGateShadowRow, ...]
    reason_codes: tuple[str, ...]

    @property
    def parity(self) -> bool:
        return all(row.parity for row in self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "classification": self.classification,
            "parity": self.parity,
            "behavior_change_allowed": self.behavior_change_allowed,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "lane_id": self.lane_id,
            "trade_id": self.trade_id,
            "rows": [row.to_dict() for row in self.rows],
            "reason_codes": list(self.reason_codes),
        }


def evaluate_track_b_gate_shadow_parity(context: TrackBGateShadowContext) -> TrackBGateShadowReport:
    """Compare existing gate decisions with registry/truth shadow decisions."""

    generated_at = _ensure_utc(context.generated_at or datetime.now(UTC))
    records = tuple(context.registry_records) if context.registry_records is not None else load_live_trade_registry_records(repo_root=context.repo_root)
    truth = context.truth_snapshot
    if truth is None:
        raise ValueError("truth_snapshot is required for shadow parity evaluation.")

    existing_results = context.existing_gate_results or {}
    rows: list[TrackBGateShadowRow] = []
    for gate in TrackBGateName:
        shadow = _registry_truth_decision(gate=gate, context=context, truth=truth, records=records)
        existing = _existing_decision(
            gate_name=gate.value,
            payload=existing_results.get(gate.value) or existing_results.get(gate.name) or {},
            context=context,
        )
        parity = existing.allowed == shadow.allowed
        mismatch_codes = () if parity else _mismatch_reason_codes(existing=existing, shadow=shadow)
        rows.append(
            TrackBGateShadowRow(
                gate_name=gate.value,
                existing_gate_result=existing,
                registry_truth_gate_result=shadow,
                parity=parity,
                mismatch_reason_codes=mismatch_codes,
                lane_id=context.lane_id,
                trade_id=shadow.trade_id or context.trade_id,
            )
        )

    reason_codes = tuple(
        dict.fromkeys(
            code
            for row in rows
            for code in (
                list(row.registry_truth_gate_result.reason_codes)
                + list(row.existing_gate_result.reason_codes)
                + list(row.mismatch_reason_codes)
            )
        )
    )
    classification = GATE_SHADOW_PARITY_OK if all(row.parity for row in rows) else GATE_SHADOW_PARITY_MISMATCH
    if any(code in reason_codes for code in ("REGISTRY_CONFLICT_REVIEW_REQUIRED", TRUTH_CONFLICT_REVIEW_REQUIRED)):
        classification = GATE_SHADOW_REVIEW_REQUIRED if all(row.parity for row in rows) else GATE_SHADOW_PARITY_MISMATCH

    return TrackBGateShadowReport(
        schema_version="track_b_gate_shadow_parity_v1",
        generated_at=generated_at,
        classification=classification,
        behavior_change_allowed=False,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        lane_id=context.lane_id,
        trade_id=context.trade_id,
        rows=tuple(rows),
        reason_codes=reason_codes,
    )


def write_track_b_gate_shadow_report(*, repo_root: Path, report: TrackBGateShadowReport, path: Path = DEFAULT_GATE_SHADOW_PARITY_REPORT_PATH) -> Path:
    resolved = path if path.is_absolute() else repo_root / path
    write_json_atomic(resolved, report.to_dict())
    return resolved


def _registry_truth_decision(
    *,
    gate: TrackBGateName,
    context: TrackBGateShadowContext,
    truth: TrackBTruthSnapshot,
    records: Sequence[TradeRegistryRecord],
) -> TrackBGateDecision:
    base_reasons = list(_truth_block_reasons(truth))
    open_records = _current_open_records(records)
    requested_open_records = _matching_open_records(records=open_records, context=context)
    requested_record = _requested_record(records=records, context=context)
    operation = _operation_for_action(context.action)

    if gate == TrackBGateName.SAFE_STATE_SUBMIT:
        reasons = []
        if not truth.safe_state.submit_allowed:
            reasons.append(SAFE_STATE_SUBMIT_BLOCKED)
        if not truth.safe_state.fresh:
            reasons.append("SAFE_STATE_STALE")
        return _decision(gate, not reasons, reasons, context)

    if gate == TrackBGateName.NO_WORKING_ORDER:
        reasons = []
        if truth.broker_truth.open_order_count > 0:
            reasons.append("OPEN_WORKING_ORDER_PRESENT")
        if not truth.broker_truth.fresh:
            reasons.append("BROKER_TRUTH_STALE")
        return _decision(gate, not reasons, reasons, context)

    if gate == TrackBGateName.RUNTIME_AUTHORITY:
        reasons = []
        if not truth.runtime.runtime_alive:
            reasons.append("RUNTIME_NOT_ALIVE")
        if not truth.runtime.submit_capable:
            reasons.append("RUNTIME_NOT_SUBMIT_CAPABLE")
        if truth.runtime.duplicate_writer_detected:
            reasons.append("DUPLICATE_WRITER_DETECTED")
        return _decision(gate, not reasons, reasons, context)

    if gate == TrackBGateName.GOVERNANCE_SUBMIT:
        reasons = list(base_reasons)
        if not truth.runtime.submit_capable:
            reasons.append("RUNTIME_NOT_SUBMIT_CAPABLE")
        if not truth.safe_state.submit_allowed:
            reasons.append(SAFE_STATE_SUBMIT_BLOCKED)
        if not truth.control_plane.fresh or not truth.control_plane.coherent:
            reasons.append(CONTROL_PLANE_STALE)
        if truth.broker_truth.open_order_count > 0:
            reasons.append("OPEN_WORKING_ORDER_PRESENT")
        if operation == "OPEN" and truth.contract_status.entry_status != CONTRACT_ENTRY_ELIGIBLE:
            reasons.append(str(truth.contract_status.entry_status or CONTRACT_ENTRY_BLOCKED))
        return _decision(gate, not reasons, reasons, context)

    if gate == TrackBGateName.ENTRY_EXPOSURE:
        reasons = list(base_reasons)
        if operation != "OPEN":
            return _decision(gate, True, ("ENTRY_EXPOSURE_NOT_APPLICABLE",), context)
        if requested_open_records:
            reasons.append("DUPLICATE_STRATEGY_ENTRY_WHILE_POSITION_OPEN")
        elif open_records:
            reasons.append("STRATEGY_STACKING_REQUIRES_LEGACY_POLICY_REVIEW")
        if truth.broker_truth.open_order_count > 0:
            reasons.append("OPEN_WORKING_ORDER_PRESENT")
        return _decision(gate, not reasons, reasons, context)

    if gate == TrackBGateName.MANAGED_EXIT_EXPOSURE:
        if operation != "CLOSE":
            return _decision(gate, True, ("MANAGED_EXIT_NOT_APPLICABLE",), context)
        reasons = list(base_reasons)
        if requested_record is None:
            reasons.append("REGISTRY_TRADE_ID_NOT_OPEN_MANAGED")
        elif requested_record.broker_backed_entry is not True:
            reasons.append("REGISTRY_ENTRY_NOT_BROKER_BACKED")
        elif requested_record.current_state not in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE}:
            reasons.append("REGISTRY_TRADE_NOT_MANAGED_EXIT_ELIGIBLE")
        elif _quantity(context.quantity) > requested_record.open_qty:
            reasons.append("EXIT_QUANTITY_EXCEEDS_REGISTRY_OPEN_QTY")
        if truth.broker_truth.open_order_count > 0:
            reasons.append("OPEN_WORKING_ORDER_PRESENT")
        trade_id = requested_record.trade_id if requested_record is not None else context.trade_id
        return _decision(gate, not reasons, reasons, context, trade_id=trade_id)

    return _decision(gate, False, ("UNKNOWN_GATE",), context)


def _truth_block_reasons(truth: TrackBTruthSnapshot) -> tuple[str, ...]:
    reasons: list[str] = []
    if truth.conflicts:
        reasons.append(TRUTH_CONFLICT_REVIEW_REQUIRED)
        reasons.extend(str(code) for conflict in truth.conflicts for code in conflict.reason_codes)
    if not truth.broker_truth.fresh:
        reasons.append("BROKER_TRUTH_STALE")
    if not truth.lifecycle.fresh:
        reasons.append("LIFECYCLE_TRUTH_STALE")
    if not truth.reconciliation.reconciled:
        reasons.append("BROKER_LIFECYCLE_RECONCILIATION_NOT_CLEAN")
    if truth.reconciliation.review_required_count > 0:
        reasons.append("RECONCILIATION_REVIEW_REQUIRED")
    return tuple(dict.fromkeys(reasons))


def _current_open_records(records: Sequence[TradeRegistryRecord]) -> tuple[TradeRegistryRecord, ...]:
    return tuple(
        record
        for record in records
        if record.current_state in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE, TradeCurrentState.WORKING_EXIT}
    )


def _matching_open_records(*, records: Sequence[TradeRegistryRecord], context: TrackBGateShadowContext) -> tuple[TradeRegistryRecord, ...]:
    lane = context.lane_id.strip()
    strategy = context.thesis_strategy_id.strip()
    symbol = context.symbol.strip().upper()
    matches = []
    for record in records:
        owner = record.ownership_identity
        if owner is None:
            continue
        if context.trade_id and record.trade_id == context.trade_id:
            matches.append(record)
            continue
        owner_ids = {owner.lane_id, owner.thesis_strategy_id}
        if (lane in owner_ids or strategy in owner_ids) and owner.symbol.upper() == symbol:
            matches.append(record)
    return tuple(matches)


def _requested_record(*, records: Sequence[TradeRegistryRecord], context: TrackBGateShadowContext) -> TradeRegistryRecord | None:
    if context.trade_id:
        for record in records:
            if record.trade_id == context.trade_id:
                return record
    matches = _matching_open_records(records=_current_open_records(records), context=context)
    return matches[0] if len(matches) == 1 else None


def _existing_decision(*, gate_name: str, payload: Mapping[str, Any], context: TrackBGateShadowContext) -> TrackBGateDecision:
    allowed = _allowed_from_payload(payload)
    reasons = tuple(str(reason) for reason in (payload.get("reason_codes") or payload.get("block_reasons") or payload.get("reasons") or []) if str(reason))
    return TrackBGateDecision(
        gate_name=gate_name,
        allowed=allowed,
        reason_codes=reasons,
        source="existing_gate_result",
        lane_id=context.lane_id,
        trade_id=context.trade_id,
    )


def _allowed_from_payload(payload: Mapping[str, Any]) -> bool:
    for key in ("allowed", "submit_allowed", "passed", "ready"):
        if key in payload:
            return bool(payload.get(key))
    return False


def _decision(
    gate: TrackBGateName,
    allowed: bool,
    reason_codes: Sequence[str],
    context: TrackBGateShadowContext,
    *,
    trade_id: str | None = None,
) -> TrackBGateDecision:
    return TrackBGateDecision(
        gate_name=gate.value,
        allowed=allowed,
        reason_codes=tuple(dict.fromkeys(str(code) for code in reason_codes if str(code))),
        source="central_registry_track_b_truth_snapshot_shadow",
        lane_id=context.lane_id,
        trade_id=trade_id or context.trade_id,
    )


def _mismatch_reason_codes(*, existing: TrackBGateDecision, shadow: TrackBGateDecision) -> tuple[str, ...]:
    direction = "EXISTING_ALLOWED_SHADOW_BLOCKED" if existing.allowed and not shadow.allowed else "EXISTING_BLOCKED_SHADOW_ALLOWED"
    return tuple(dict.fromkeys((direction, *shadow.reason_codes, *existing.reason_codes)))


def _operation_for_action(action: str) -> str:
    normalized = str(action or "").strip().upper()
    if normalized in {"BUY_TO_OPEN", "SELL_TO_OPEN", "BUY", "SELL"}:
        return "OPEN"
    if normalized in {"SELL_TO_CLOSE", "BUY_TO_CLOSE", "EXIT", "CLOSE"}:
        return "CLOSE"
    return "UNKNOWN"


def _quantity(value: Decimal | int | float | str | None) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
