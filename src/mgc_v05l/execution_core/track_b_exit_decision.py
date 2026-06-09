"""Pure Track B hold/exit decision report.

This phase consumes PositionState rows and optional policy/evidence inputs to
answer what should happen to each current position: hold, reduce, full close,
protect, or reverse-consider. It does not validate exit authority, create
ExitIntent objects, submit/cancel/close, start services, or touch broker state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import JsonSerializable, TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_authority_contract import SourceArtifactRef
from mgc_v05l.execution_core.track_b_position_state import (
    DEFAULT_POSITION_STATE_REPORT,
    TrackBPositionState,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
EXIT_DECISION_SCHEMA_VERSION = "track_b_exit_decision_v1"
EXIT_DECISION_REPORT_SCHEMA_VERSION = "track_b_exit_decision_report_v1"

DEFAULT_EXIT_DECISION_REPORT = (
    Path("outputs") / "track_b_execution_core" / "exit_decision" / "latest_exit_decision.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_PATH = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)


class ExitDecisionAction(str, Enum):
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    FULL_CLOSE = "FULL_CLOSE"
    PROTECT = "PROTECT"
    REVERSE_CONSIDER = "REVERSE_CONSIDER"


class ExitDecisionReportClassification(str, Enum):
    FLAT = "EXIT_DECISION_FLAT"
    DECISIONS_READY = "EXIT_DECISION_READY"
    POSITION_STATE_BLOCKED = "EXIT_DECISION_POSITION_STATE_BLOCKED"
    SOURCE_MISSING = "EXIT_DECISION_SOURCE_MISSING"


class ExitPolicyType(str, Enum):
    TIMEBOX = "TIMEBOX"
    HARD_STOP = "HARD_STOP"
    PROFIT_TARGET = "PROFIT_TARGET"
    TRAILING_STOP = "TRAILING_STOP"
    OPERATOR = "OPERATOR"
    REVERSAL = "REVERSAL"
    PARTIAL_SCALE_OUT = "PARTIAL_SCALE_OUT"


class ExitPolicyEvidenceFreshness(str, Enum):
    FRESH = "FRESH"
    STALE_DEPENDENCY = "STALE_DEPENDENCY"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class TrackBExitPolicyEvidence(JsonSerializable):
    position_key: str
    execution_domain: str
    account_id: str
    con_id: int
    local_symbol: str
    instrument: str
    side: str
    policy_id: str | None
    policy_type: ExitPolicyType | str
    due: bool
    suggested_decision: ExitDecisionAction | str
    close_qty: Decimal | int | str | None
    evidence_freshness: ExitPolicyEvidenceFreshness | str
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...]
    diagnostics: tuple[Mapping[str, Any], ...] = ()
    schema_version: str = "track_b_exit_policy_evidence_v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "position_key", _required_text(self.position_key, "position_key"))
        object.__setattr__(self, "execution_domain", _required_text(self.execution_domain, "execution_domain"))
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        con_id = int(self.con_id)
        if con_id < 0:
            raise TrackBModelError("con_id must not be negative.")
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(self, "local_symbol", _required_text(self.local_symbol, "local_symbol").upper())
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument").upper())
        object.__setattr__(self, "side", _required_text(self.side, "side").upper())
        object.__setattr__(self, "policy_id", _optional_text(self.policy_id))
        object.__setattr__(self, "policy_type", _normalize_policy_type(self.policy_type))
        object.__setattr__(self, "due", bool(self.due))
        object.__setattr__(self, "suggested_decision", _normalize_action(self.suggested_decision))
        if self.close_qty is not None:
            object.__setattr__(self, "close_qty", _positive_decimal(self.close_qty, "close_qty"))
        object.__setattr__(self, "evidence_freshness", _normalize_evidence_freshness(self.evidence_freshness))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_source_ref(row) for row in self.source_artifact_refs))
        object.__setattr__(self, "diagnostics", tuple(dict(row) for row in self.diagnostics))


@dataclass(frozen=True)
class TrackBExitDecision(JsonSerializable):
    decision_id: str
    action: ExitDecisionAction | str
    execution_domain: str
    account_id: str
    con_id: int
    local_symbol: str
    instrument: str
    side: str
    qty: Decimal | int | str
    reason: str
    priority: int
    urgency: str
    source_policy_id: str | None
    position: TrackBPositionState | Mapping[str, Any]
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...]
    diagnostics: tuple[Mapping[str, Any], ...] = ()
    reduce_qty: Decimal | int | str | None = None
    schema_version: str = EXIT_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_id", _required_text(self.decision_id, "decision_id"))
        object.__setattr__(self, "action", _normalize_action(self.action))
        object.__setattr__(self, "execution_domain", _required_text(self.execution_domain, "execution_domain"))
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        con_id = int(self.con_id)
        if con_id <= 0:
            raise TrackBModelError("con_id must be positive.")
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(self, "local_symbol", _required_text(self.local_symbol, "local_symbol").upper())
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument").upper())
        object.__setattr__(self, "side", _required_text(self.side, "side").upper())
        object.__setattr__(self, "qty", _positive_decimal(self.qty, "qty"))
        if self.reduce_qty is not None:
            reduce_qty = _positive_decimal(self.reduce_qty, "reduce_qty")
            if reduce_qty > self.qty:
                raise TrackBModelError("reduce_qty cannot exceed qty.")
            object.__setattr__(self, "reduce_qty", reduce_qty)
        object.__setattr__(self, "reason", _required_text(self.reason, "reason"))
        if int(self.priority) < 0:
            raise TrackBModelError("priority must be non-negative.")
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "urgency", _required_text(self.urgency, "urgency").upper())
        object.__setattr__(self, "source_policy_id", _optional_text(self.source_policy_id))
        object.__setattr__(self, "position", _normalize_position(self.position))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_source_ref(row) for row in self.source_artifact_refs))
        object.__setattr__(self, "diagnostics", tuple(dict(row) for row in self.diagnostics))


@dataclass(frozen=True)
class TrackBExitDecisionReportConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_EXIT_DECISION_REPORT
    position_state_path: Path = DEFAULT_POSITION_STATE_REPORT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_exit_decision_report(
    *,
    config: TrackBExitDecisionReportConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    decision_inputs: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    position_state = dict((input_overrides or {}).get("position_state") or _read_json(config.resolve(config.position_state_path)))
    managed_positions = dict(
        (input_overrides or {}).get("managed_positions")
        or _read_json(config.resolve(config.managed_position_registry_path))
    )
    source_ref = SourceArtifactRef(
        name="position_state",
        path=str(config.resolve(config.position_state_path)),
        generated_at=_parse_dt(position_state.get("generated_at")),
        authority_layer="ExitDecision",
    )
    managed_source_ref = SourceArtifactRef(
        name="managed_positions",
        path=str(config.resolve(config.managed_position_registry_path)),
        generated_at=_parse_dt(managed_positions.get("generated_at")),
        authority_layer="ExitPolicyEvidence",
    )
    position_state_blocked = _position_state_blocked(position_state)
    positions = [] if position_state_blocked else [_mapping(row) for row in _list(position_state.get("positions"))]
    policy_evidence = build_exit_policy_evidence_from_managed_positions(
        positions=positions,
        managed_positions=managed_positions,
        source_ref=managed_source_ref,
    )
    decisions = [
        _decision_for_position(
            position=position,
            source_ref=source_ref,
            policy_evidence=policy_evidence,
            decision_inputs=decision_inputs or {},
        )
        for position in positions
    ]
    classification = _classification(position_state=position_state, decisions=decisions)
    return {
        "schema_version": EXIT_DECISION_REPORT_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "live_money_eligible": position_state.get("live_money_eligible") is True,
        "paper_proof_invoked": position_state.get("paper_proof_invoked") is True,
        "classification": classification.value,
        "position_state_classification": position_state.get("classification"),
        "exit_policy_evidence": [evidence.to_json_dict() for evidence in policy_evidence],
        "decision_count": len(decisions),
        "decisions": [decision.to_json_dict() for decision in decisions],
        "action_counts": {action.value: sum(1 for decision in decisions if decision.action == action) for action in ExitDecisionAction},
        "source_artifact_paths": {
            "position_state": str(config.resolve(config.position_state_path)),
            "managed_positions": str(config.resolve(config.managed_position_registry_path)),
        },
    }


def run_track_b_exit_decision_report(
    *,
    config: TrackBExitDecisionReportConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_exit_decision_report(config=config, now=now)
    if write:
        write_track_b_exit_decision_report(config=config, payload=payload)
    return payload


def write_track_b_exit_decision_report(*, config: TrackBExitDecisionReportConfig, payload: Mapping[str, Any]) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-exit-decision")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_EXIT_DECISION_REPORT)
    parser.add_argument("--position-state-path", type=Path, default=DEFAULT_POSITION_STATE_REPORT)
    parser.add_argument("--managed-position-registry-path", type=Path, default=DEFAULT_MANAGED_POSITION_REGISTRY_PATH)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBExitDecisionReportConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        position_state_path=args.position_state_path,
        managed_position_registry_path=args.managed_position_registry_path,
    )
    payload = run_track_b_exit_decision_report(config=config, write=not args.no_write)
    if args.json or args.no_write:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']} decisions={payload['decision_count']}")
    return 0


def _decision_for_position(
    *,
    position: Mapping[str, Any],
    source_ref: SourceArtifactRef,
    policy_evidence: list[TrackBExitPolicyEvidence],
    decision_inputs: Mapping[str, Mapping[str, Any]],
) -> TrackBExitDecision:
    evidence = _policy_evidence_for_position(position=position, policy_evidence=policy_evidence)
    facts = _facts_for_position(position=position, evidence=evidence, decision_inputs=decision_inputs)
    action = _select_action(position=position, facts=facts)
    reason = _reason_for_action(action=action, facts=facts)
    reduce_qty = facts.get("reduce_qty") if action == ExitDecisionAction.REDUCE else None
    diagnostics = [*_list(position.get("diagnostic_rows"))]
    if evidence:
        diagnostics.extend(evidence.to_json_dict().get("diagnostics", []))
        diagnostics.append(
            {
                "source": "exit_policy_evidence",
                "policy_id": evidence.policy_id,
                "policy_type": evidence.policy_type.value,
                "due": evidence.due,
                "suggested_decision": evidence.suggested_decision.value,
                "evidence_freshness": evidence.evidence_freshness.value,
            }
        )
    else:
        diagnostics.append({"source": "exit_policy_evidence", "classification": "EXIT_POLICY_EVIDENCE_MISSING"})
    return TrackBExitDecision(
        decision_id=_decision_id(position),
        action=action,
        execution_domain=str(position.get("execution_domain") or ""),
        account_id=str(position.get("account_id") or ""),
        con_id=_int(position.get("con_id")),
        local_symbol=str(position.get("local_symbol") or ""),
        instrument=str(position.get("instrument") or ""),
        side=str(position.get("side") or ""),
        qty=position.get("qty") or "0",
        reduce_qty=reduce_qty,
        reason=reason,
        priority=_priority_for_action(action),
        urgency=_urgency_for_action(action),
        source_policy_id=_text_or_none(facts.get("source_policy_id")),
        position=position,
        source_artifact_refs=(source_ref,),
        diagnostics=tuple(diagnostics),
    )


def _select_action(*, position: Mapping[str, Any], facts: Mapping[str, Any]) -> ExitDecisionAction:
    qty = _positive_decimal(position.get("qty") or "0", "qty")
    reduce_qty = _decimal(facts.get("reduce_qty"))
    if _truthy(facts.get("hard_stop_triggered")) or _truthy(facts.get("protective_close_due")):
        return ExitDecisionAction.PROTECT
    if _truthy(facts.get("reversal_signal")) or _truthy(facts.get("reverse_consider")):
        return ExitDecisionAction.REVERSE_CONSIDER
    if _truthy(facts.get("partial_scale_out_due")) or (reduce_qty > Decimal("0") and reduce_qty < qty):
        return ExitDecisionAction.REDUCE
    timebox_bars = _decimal(facts.get("timebox_bars"))
    bars_since_entry = _decimal(facts.get("bars_since_entry"))
    if _truthy(facts.get("exit_due")) or _truthy(facts.get("timebox_due")):
        return ExitDecisionAction.FULL_CLOSE
    if timebox_bars > Decimal("0") and bars_since_entry >= timebox_bars:
        return ExitDecisionAction.FULL_CLOSE
    return ExitDecisionAction.HOLD


def _facts_for_position(
    *,
    position: Mapping[str, Any],
    evidence: TrackBExitPolicyEvidence | None,
    decision_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    keys = [
        str(position.get("local_symbol") or ""),
        str(position.get("lifecycle_id") or ""),
        str(position.get("trade_id") or ""),
        str(position.get("lane_id") or ""),
        str(position.get("strategy_id") or ""),
    ]
    facts: dict[str, Any] = {}
    if evidence is not None:
        facts.update(_facts_from_exit_policy_evidence(evidence))
    for key in keys:
        if key and key in decision_inputs:
            facts.update(dict(decision_inputs[key]))
    inline = _mapping(position.get("exit_decision_inputs"))
    facts.update(inline)
    return facts


def build_exit_policy_evidence_from_managed_positions(
    *,
    positions: list[Mapping[str, Any]],
    managed_positions: Mapping[str, Any],
    source_ref: SourceArtifactRef,
) -> list[TrackBExitPolicyEvidence]:
    evidence: list[TrackBExitPolicyEvidence] = []
    managed_rows = [_mapping(item) for item in _list(managed_positions.get("managed_positions"))]
    for position in positions:
        row = _matching_managed_policy_row(position=position, managed_rows=managed_rows)
        if not row:
            continue
        evidence.append(_exit_policy_evidence(position=position, managed_row=row, source_ref=source_ref))
    return evidence


def _exit_policy_evidence(
    *,
    position: Mapping[str, Any],
    managed_row: Mapping[str, Any],
    source_ref: SourceArtifactRef,
) -> TrackBExitPolicyEvidence:
    policy_id = _policy_id(managed_row)
    policy_type = _policy_type(policy_id=policy_id, managed_row=managed_row)
    due = _managed_row_due(managed_row)
    suggested_decision = _suggested_decision(policy_type=policy_type, due=due, managed_row=managed_row)
    close_qty = _close_qty_for_evidence(position=position, managed_row=managed_row, suggested_decision=suggested_decision)
    diagnostics = _policy_evidence_diagnostics(managed_row=managed_row, due=due)
    return TrackBExitPolicyEvidence(
        position_key=_position_key(position),
        execution_domain=str(position.get("execution_domain") or ""),
        account_id=str(position.get("account_id") or ""),
        con_id=_int(position.get("con_id")),
        local_symbol=str(position.get("local_symbol") or ""),
        instrument=str(position.get("instrument") or ""),
        side=str(position.get("side") or ""),
        policy_id=policy_id,
        policy_type=policy_type,
        due=due,
        suggested_decision=suggested_decision,
        close_qty=close_qty,
        evidence_freshness=_evidence_freshness(managed_row),
        source_artifact_refs=(source_ref,),
        diagnostics=tuple(diagnostics),
    )


def _matching_managed_policy_row(
    *,
    position: Mapping[str, Any],
    managed_rows: list[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    for row in managed_rows:
        if _row_is_diagnostic_only(row):
            continue
        if not _same_position_identity(position=position, row=row):
            continue
        return row
    return None


def _same_position_identity(*, position: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    broker = _mapping(row.get("broker_position"))
    account = str(broker.get("account_id") or row.get("account_id") or "")
    if account and account != str(position.get("account_id") or ""):
        return False
    position_con_id = _int(position.get("con_id"))
    row_con_id = _int(row.get("con_id") or broker.get("con_id") or _mapping(row.get("lifecycle_position")).get("con_id"))
    if position_con_id > 0 and row_con_id > 0 and position_con_id != row_con_id:
        return False
    position_symbol = str(position.get("local_symbol") or "").upper()
    row_symbol = str(row.get("local_symbol") or broker.get("local_symbol") or "").upper()
    if position_symbol and row_symbol and position_symbol != row_symbol:
        return False
    return bool(position_symbol or position_con_id > 0)


def _policy_id(managed_row: Mapping[str, Any]) -> str | None:
    lifecycle = _mapping(managed_row.get("lifecycle_position"))
    return _text_or_none(
        managed_row.get("managed_exit_policy_id")
        or managed_row.get("policy_id")
        or lifecycle.get("managed_exit_policy_id")
        or _first_lifecycle_unit_value(managed_row, "managed_exit_policy_id")
        or _mapping(managed_row.get("hold_exit_shadow")).get("assigned_live_exit_policy")
    )


def _policy_type(*, policy_id: str | None, managed_row: Mapping[str, Any]) -> ExitPolicyType:
    explicit = str(managed_row.get("policy_type") or managed_row.get("exit_policy_type") or "").upper()
    text = explicit or str(policy_id or "").upper()
    if "HARD_STOP" in text or text == "STOP":
        return ExitPolicyType.HARD_STOP
    if "PROFIT" in text or "TARGET" in text:
        return ExitPolicyType.PROFIT_TARGET
    if "TRAIL" in text:
        return ExitPolicyType.TRAILING_STOP
    if "OPERATOR" in text:
        return ExitPolicyType.OPERATOR
    if "REVERS" in text:
        return ExitPolicyType.REVERSAL
    if "PARTIAL" in text or "SCALE" in text:
        return ExitPolicyType.PARTIAL_SCALE_OUT
    return ExitPolicyType.TIMEBOX


def _managed_row_due(managed_row: Mapping[str, Any]) -> bool:
    classification = str(managed_row.get("classification") or "").upper()
    if "EXIT_DUE" in classification:
        return True
    return _truthy(managed_row.get("exit_due")) or str(managed_row.get("exit_due_state") or "").upper() == "EXIT_DUE"


def _suggested_decision(
    *,
    policy_type: ExitPolicyType,
    due: bool,
    managed_row: Mapping[str, Any],
) -> ExitDecisionAction:
    if not due:
        return ExitDecisionAction.HOLD
    if policy_type == ExitPolicyType.PARTIAL_SCALE_OUT:
        return ExitDecisionAction.REDUCE
    if policy_type == ExitPolicyType.REVERSAL:
        return ExitDecisionAction.REVERSE_CONSIDER
    if policy_type == ExitPolicyType.HARD_STOP:
        return ExitDecisionAction.PROTECT
    return ExitDecisionAction.FULL_CLOSE


def _close_qty_for_evidence(
    *,
    position: Mapping[str, Any],
    managed_row: Mapping[str, Any],
    suggested_decision: ExitDecisionAction,
) -> Any:
    if suggested_decision == ExitDecisionAction.HOLD:
        return None
    if suggested_decision == ExitDecisionAction.REDUCE:
        return managed_row.get("close_qty") or managed_row.get("reduce_qty") or managed_row.get("required_close_quantity")
    return managed_row.get("required_close_quantity") or managed_row.get("quantity") or position.get("owned_qty") or position.get("qty")


def _evidence_freshness(managed_row: Mapping[str, Any]) -> ExitPolicyEvidenceFreshness:
    if managed_row.get("exit_due_evidence_stale") is True or str(managed_row.get("freshness_state") or "").upper().startswith("STALE"):
        return ExitPolicyEvidenceFreshness.STALE_DEPENDENCY
    if managed_row.get("freshness_state"):
        return ExitPolicyEvidenceFreshness.FRESH
    return ExitPolicyEvidenceFreshness.UNKNOWN


def _policy_evidence_diagnostics(*, managed_row: Mapping[str, Any], due: bool) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    lifecycle = _mapping(managed_row.get("lifecycle_position"))
    lifecycle_exit_due = lifecycle.get("exit_due")
    if due and lifecycle_exit_due is False:
        diagnostics.append(
            {
                "source": "managed_positions",
                "classification": "CURRENT_MANAGED_POSITION_DUE_SUPERSEDES_STALE_LIFECYCLE_HOLD",
                "detail": "Current managed-position exit_due evidence is due while embedded lifecycle exit_due is false.",
            }
        )
    if managed_row.get("exit_due_evidence_stale") is True:
        diagnostics.append(
            {
                "source": "managed_positions",
                "classification": "EXIT_DUE_EVIDENCE_STALE",
            }
        )
    stale_sources = _list(managed_row.get("stale_dependency_sources"))
    if stale_sources:
        diagnostics.append(
            {
                "source": "managed_positions",
                "classification": "STALE_DEPENDENCY_SOURCES",
                "stale_dependency_sources": stale_sources,
            }
        )
    return diagnostics


def _facts_from_exit_policy_evidence(evidence: TrackBExitPolicyEvidence) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "source_policy_id": evidence.policy_id,
        "exit_reason": _exit_reason_from_policy(evidence),
        "exit_policy_evidence": evidence.to_json_dict(),
    }
    if evidence.suggested_decision == ExitDecisionAction.HOLD:
        return facts
    if evidence.suggested_decision == ExitDecisionAction.FULL_CLOSE:
        facts["exit_due"] = evidence.due
        facts["timebox_due"] = evidence.due and evidence.policy_type == ExitPolicyType.TIMEBOX
    elif evidence.suggested_decision == ExitDecisionAction.REDUCE:
        facts["partial_scale_out_due"] = evidence.due
        facts["reduce_qty"] = evidence.close_qty
    elif evidence.suggested_decision == ExitDecisionAction.PROTECT:
        facts["protective_close_due"] = evidence.due
    elif evidence.suggested_decision == ExitDecisionAction.REVERSE_CONSIDER:
        facts["reverse_consider"] = evidence.due
    return facts


def _exit_reason_from_policy(evidence: TrackBExitPolicyEvidence) -> str:
    if evidence.suggested_decision == ExitDecisionAction.HOLD:
        return "exit_policy_not_due"
    return f"{evidence.policy_type.value.lower()}_exit_due"


def _policy_evidence_for_position(
    *,
    position: Mapping[str, Any],
    policy_evidence: list[TrackBExitPolicyEvidence],
) -> TrackBExitPolicyEvidence | None:
    position_key = _position_key(position)
    for evidence in policy_evidence:
        if evidence.position_key == position_key:
            return evidence
    return None


def _position_key(position: Mapping[str, Any]) -> str:
    return "|".join(
        str(part or "").upper()
        for part in (
            position.get("account_id"),
            position.get("con_id"),
            position.get("local_symbol"),
            position.get("side"),
        )
    )


def _first_lifecycle_unit_value(managed_row: Mapping[str, Any], key: str) -> Any:
    for unit in (_mapping(item) for item in _list(managed_row.get("lifecycle_units"))):
        if unit.get(key) not in (None, ""):
            return unit.get(key)
    lifecycle = _mapping(managed_row.get("lifecycle_position"))
    for unit in (_mapping(item) for item in _list(lifecycle.get("lifecycle_units"))):
        if unit.get(key) not in (None, ""):
            return unit.get(key)
    return None


def _reason_for_action(*, action: ExitDecisionAction, facts: Mapping[str, Any]) -> str:
    if facts.get("exit_reason"):
        return str(facts["exit_reason"])
    return {
        ExitDecisionAction.HOLD: "no_exit_condition_triggered",
        ExitDecisionAction.REDUCE: "partial_scale_out_due",
        ExitDecisionAction.FULL_CLOSE: "timebox_or_exit_due",
        ExitDecisionAction.PROTECT: "protective_exit_due",
        ExitDecisionAction.REVERSE_CONSIDER: "reversal_close_only_consideration",
    }[action]


def _priority_for_action(action: ExitDecisionAction) -> int:
    return {
        ExitDecisionAction.PROTECT: 10,
        ExitDecisionAction.REVERSE_CONSIDER: 20,
        ExitDecisionAction.FULL_CLOSE: 50,
        ExitDecisionAction.REDUCE: 60,
        ExitDecisionAction.HOLD: 100,
    }[action]


def _urgency_for_action(action: ExitDecisionAction) -> str:
    return {
        ExitDecisionAction.PROTECT: "HIGH",
        ExitDecisionAction.REVERSE_CONSIDER: "HIGH",
        ExitDecisionAction.FULL_CLOSE: "NORMAL",
        ExitDecisionAction.REDUCE: "NORMAL",
        ExitDecisionAction.HOLD: "LOW",
    }[action]


def _classification(
    *,
    position_state: Mapping[str, Any],
    decisions: list[TrackBExitDecision],
) -> ExitDecisionReportClassification:
    if not position_state:
        return ExitDecisionReportClassification.SOURCE_MISSING
    state_classification = str(position_state.get("classification") or "")
    if _position_state_blocked(position_state):
        return ExitDecisionReportClassification.POSITION_STATE_BLOCKED
    if not decisions:
        return ExitDecisionReportClassification.FLAT
    return ExitDecisionReportClassification.DECISIONS_READY


def _decision_id(position: Mapping[str, Any]) -> str:
    identity = (
        position.get("execution_domain"),
        position.get("account_id"),
        position.get("con_id"),
        position.get("local_symbol"),
        position.get("lifecycle_id"),
        position.get("trade_id"),
    )
    return "exit_decision_" + "_".join(str(item or "none").lower().replace(" ", "_") for item in identity)


def _normalize_action(value: ExitDecisionAction | str) -> ExitDecisionAction:
    try:
        return value if isinstance(value, ExitDecisionAction) else ExitDecisionAction(str(value))
    except ValueError as exc:
        raise TrackBModelError("exit decision action is not valid.") from exc


def _normalize_policy_type(value: ExitPolicyType | str) -> ExitPolicyType:
    try:
        return value if isinstance(value, ExitPolicyType) else ExitPolicyType(str(value).upper())
    except ValueError as exc:
        raise TrackBModelError("exit policy type is not valid.") from exc


def _normalize_evidence_freshness(value: ExitPolicyEvidenceFreshness | str) -> ExitPolicyEvidenceFreshness:
    try:
        return (
            value
            if isinstance(value, ExitPolicyEvidenceFreshness)
            else ExitPolicyEvidenceFreshness(str(value).upper())
        )
    except ValueError as exc:
        raise TrackBModelError("exit policy evidence freshness is not valid.") from exc


def _normalize_position(value: TrackBPositionState | Mapping[str, Any]) -> TrackBPositionState | dict[str, Any]:
    if isinstance(value, TrackBPositionState):
        return value
    return dict(value) if isinstance(value, Mapping) else {}


def _normalize_source_ref(value: SourceArtifactRef | Mapping[str, Any]) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    return SourceArtifactRef(**dict(value))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    normalized = _decimal(value)
    if normalized <= 0:
        raise TrackBModelError(f"{field_name} must be positive.")
    return normalized


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes"}


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TrackBModelError(f"{field_name} is required.")
    return text


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _text_or_none(value: Any) -> str | None:
    return _optional_text(value)


def _position_state_blocked(position_state: Mapping[str, Any]) -> bool:
    state_classification = str(position_state.get("classification") or "")
    return "BLOCKED" in state_classification or "WRONG_SCOPE" in state_classification


def _row_is_diagnostic_only(row: Mapping[str, Any]) -> bool:
    if row.get("historical_only") is True or row.get("diagnostic_only") is True:
        return True
    scope = str(row.get("current_hot_path_scope") or row.get("scope") or "").upper()
    if "HISTORICAL" in scope or "FULL_AUDIT_ONLY" in scope or "DIAGNOSTIC" in scope:
        return True
    classification = str(row.get("classification") or "").upper()
    return "HISTORICAL" in classification or "STALE_DERIVED" in classification


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
