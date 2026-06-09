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
    source_ref = SourceArtifactRef(
        name="position_state",
        path=str(config.resolve(config.position_state_path)),
        generated_at=_parse_dt(position_state.get("generated_at")),
        authority_layer="ExitDecision",
    )
    position_state_blocked = _position_state_blocked(position_state)
    positions = [] if position_state_blocked else [_mapping(row) for row in _list(position_state.get("positions"))]
    decisions = [
        _decision_for_position(position=position, source_ref=source_ref, decision_inputs=decision_inputs or {})
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
        "decision_count": len(decisions),
        "decisions": [decision.to_json_dict() for decision in decisions],
        "action_counts": {action.value: sum(1 for decision in decisions if decision.action == action) for action in ExitDecisionAction},
        "source_artifact_paths": {"position_state": str(config.resolve(config.position_state_path))},
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
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBExitDecisionReportConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        position_state_path=args.position_state_path,
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
    decision_inputs: Mapping[str, Mapping[str, Any]],
) -> TrackBExitDecision:
    facts = _facts_for_position(position=position, decision_inputs=decision_inputs)
    action = _select_action(position=position, facts=facts)
    reason = _reason_for_action(action=action, facts=facts)
    reduce_qty = facts.get("reduce_qty") if action == ExitDecisionAction.REDUCE else None
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
        diagnostics=tuple(_list(position.get("diagnostic_rows"))),
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
    for key in keys:
        if key and key in decision_inputs:
            facts.update(dict(decision_inputs[key]))
    inline = _mapping(position.get("exit_decision_inputs"))
    facts.update(inline)
    return facts


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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
