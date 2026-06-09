"""Pure Track B exit strategy selector.

This phase consumes ExitDecision candidates and chooses the highest-priority
exit tactic per current position. It does not create ExitIntent objects,
validate authority, submit/cancel/close, start services, restart runtime, or
mutate broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import JsonSerializable, TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_authority_contract import SourceArtifactRef
from mgc_v05l.execution_core.track_b_exit_decision import DEFAULT_EXIT_DECISION_REPORT


REPO_ROOT = Path(__file__).resolve().parents[3]
EXIT_STRATEGY_SELECTOR_SCHEMA_VERSION = "track_b_exit_strategy_selector_v1"
EXIT_STRATEGY_SELECTOR_REPORT_SCHEMA_VERSION = "track_b_exit_strategy_selector_report_v1"

DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT = (
    Path("outputs") / "track_b_execution_core" / "exit_strategy_selector" / "latest_exit_strategy_selector.json"
)


class ExitStrategyType(str, Enum):
    HARD_STOP = "hard_stop"
    OPERATOR_CLOSE = "operator_close"
    PROTECTIVE_CLOSE = "protective_close"
    REVERSAL_CLOSE_ONLY = "reversal_close_only"
    PROFIT_TARGET = "profit_target"
    TRAILING_STOP = "trailing_stop"
    VWAP_RECLAIM_LOSS = "VWAP_reclaim_loss"
    TIMEBOX_CLOSE = "timebox_close"
    PARTIAL_SCALE_OUT = "partial_scale_out"


class ExitStrategySelectionClassification(str, Enum):
    FLAT = "EXIT_STRATEGY_SELECTOR_FLAT"
    NO_ACTIONABLE_EXITS = "EXIT_STRATEGY_SELECTOR_NO_ACTIONABLE_EXITS"
    SELECTED = "EXIT_STRATEGY_SELECTOR_SELECTED"
    REVIEW_REQUIRED = "EXIT_STRATEGY_SELECTOR_REVIEW_REQUIRED"
    DECISION_SOURCE_BLOCKED = "EXIT_STRATEGY_SELECTOR_DECISION_SOURCE_BLOCKED"
    SOURCE_MISSING = "EXIT_STRATEGY_SELECTOR_SOURCE_MISSING"


STRATEGY_PRIORITY: dict[ExitStrategyType, int] = {
    ExitStrategyType.HARD_STOP: 10,
    ExitStrategyType.OPERATOR_CLOSE: 20,
    ExitStrategyType.PROTECTIVE_CLOSE: 30,
    ExitStrategyType.REVERSAL_CLOSE_ONLY: 40,
    ExitStrategyType.PROFIT_TARGET: 50,
    ExitStrategyType.TRAILING_STOP: 60,
    ExitStrategyType.VWAP_RECLAIM_LOSS: 70,
    ExitStrategyType.TIMEBOX_CLOSE: 80,
    ExitStrategyType.PARTIAL_SCALE_OUT: 90,
}


@dataclass(frozen=True)
class TrackBExitStrategySelection(JsonSerializable):
    selection_id: str
    strategy_type: ExitStrategyType | str
    priority: int
    decision_id: str
    execution_domain: str
    account_id: str
    con_id: int
    local_symbol: str
    instrument: str
    side: str
    qty: str
    action: str
    reason: str
    close_only: bool
    entry_allowed: bool
    flip_allowed: bool
    selected_decision: Mapping[str, Any]
    source_artifact_refs: tuple[SourceArtifactRef | Mapping[str, Any], ...]
    diagnostics: tuple[Mapping[str, Any], ...] = ()
    schema_version: str = EXIT_STRATEGY_SELECTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "selection_id", _required_text(self.selection_id, "selection_id"))
        object.__setattr__(self, "strategy_type", _normalize_strategy_type(self.strategy_type))
        if int(self.priority) < 0:
            raise TrackBModelError("priority must be non-negative.")
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "decision_id", _required_text(self.decision_id, "decision_id"))
        object.__setattr__(self, "execution_domain", _required_text(self.execution_domain, "execution_domain"))
        object.__setattr__(self, "account_id", _required_text(self.account_id, "account_id"))
        con_id = int(self.con_id)
        if con_id <= 0:
            raise TrackBModelError("con_id must be positive.")
        object.__setattr__(self, "con_id", con_id)
        object.__setattr__(self, "local_symbol", _required_text(self.local_symbol, "local_symbol").upper())
        object.__setattr__(self, "instrument", _required_text(self.instrument, "instrument").upper())
        object.__setattr__(self, "side", _required_text(self.side, "side").upper())
        object.__setattr__(self, "qty", _required_text(self.qty, "qty"))
        object.__setattr__(self, "action", _required_text(self.action, "action"))
        object.__setattr__(self, "reason", _required_text(self.reason, "reason"))
        object.__setattr__(self, "close_only", bool(self.close_only))
        object.__setattr__(self, "entry_allowed", bool(self.entry_allowed))
        object.__setattr__(self, "flip_allowed", bool(self.flip_allowed))
        if self.strategy_type == ExitStrategyType.REVERSAL_CLOSE_ONLY and (not self.close_only or self.entry_allowed or self.flip_allowed):
            raise TrackBModelError("reversal_close_only must remain close-only and cannot allow entry/flip.")
        object.__setattr__(self, "selected_decision", dict(self.selected_decision))
        object.__setattr__(self, "source_artifact_refs", tuple(_normalize_source_ref(row) for row in self.source_artifact_refs))
        object.__setattr__(self, "diagnostics", tuple(dict(row) for row in self.diagnostics))


@dataclass(frozen=True)
class TrackBExitStrategySelectorConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT
    exit_decision_path: Path = DEFAULT_EXIT_DECISION_REPORT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_exit_strategy_selector_report(
    *,
    config: TrackBExitStrategySelectorConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    exit_decision = dict((input_overrides or {}).get("exit_decision") or _read_json(config.resolve(config.exit_decision_path)))
    source_ref = SourceArtifactRef(
        name="exit_decision",
        path=str(config.resolve(config.exit_decision_path)),
        generated_at=_parse_dt(exit_decision.get("generated_at")),
        authority_layer="ExitStrategySelector",
    )
    if _decision_source_blocked(exit_decision):
        grouped: dict[str, list[dict[str, Any]]] = {}
    else:
        grouped = _group_actionable_decisions(_list(exit_decision.get("decisions")))
    selected: list[TrackBExitStrategySelection] = []
    review_required: list[dict[str, Any]] = []
    held_decisions: list[dict[str, Any]] = []
    for decision in (_mapping(row) for row in _list(exit_decision.get("decisions"))):
        if str(decision.get("action") or "") == "HOLD":
            held_decisions.append({"reason": "hold_decision_not_actionable", "decision": decision})
    for group_key, decisions in sorted(grouped.items()):
        selected_for_group, review = _select_group(group_key=group_key, decisions=decisions, source_ref=source_ref)
        if review is not None:
            review_required.append(review)
        if selected_for_group is not None:
            selected.append(selected_for_group)
    classification = _classification(
        exit_decision=exit_decision,
        selected=selected,
        review_required=review_required,
        held_decisions=held_decisions,
    )
    return {
        "schema_version": EXIT_STRATEGY_SELECTOR_REPORT_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "live_money_eligible": exit_decision.get("live_money_eligible") is True,
        "paper_proof_invoked": exit_decision.get("paper_proof_invoked") is True,
        "classification": classification.value,
        "exit_decision_classification": exit_decision.get("classification"),
        "selected_count": len(selected),
        "selected_decisions": [row.to_json_dict() for row in selected],
        "review_required_count": len(review_required),
        "review_required": review_required,
        "held_decision_count": len(held_decisions),
        "diagnostic_rows": held_decisions,
        "priority_order": [strategy.value for strategy, _priority in sorted(STRATEGY_PRIORITY.items(), key=lambda item: item[1])],
        "source_artifact_paths": {"exit_decision": str(config.resolve(config.exit_decision_path))},
    }


def run_track_b_exit_strategy_selector_report(
    *,
    config: TrackBExitStrategySelectorConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_exit_strategy_selector_report(config=config, now=now)
    if write:
        write_track_b_exit_strategy_selector_report(config=config, payload=payload)
    return payload


def write_track_b_exit_strategy_selector_report(
    *, config: TrackBExitStrategySelectorConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-exit-strategy-selector")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT)
    parser.add_argument("--exit-decision-path", type=Path, default=DEFAULT_EXIT_DECISION_REPORT)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBExitStrategySelectorConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        exit_decision_path=args.exit_decision_path,
    )
    payload = run_track_b_exit_strategy_selector_report(config=config, write=not args.no_write)
    if args.json or args.no_write:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']} selected={payload['selected_count']} review={payload['review_required_count']}")
    return 0


def _select_group(
    *,
    group_key: str,
    decisions: list[dict[str, Any]],
    source_ref: SourceArtifactRef,
) -> tuple[TrackBExitStrategySelection | None, dict[str, Any] | None]:
    ranked = sorted(
        (_selection_candidate(decision=decision, source_ref=source_ref) for decision in decisions),
        key=lambda row: (row.priority, row.selection_id),
    )
    if not ranked:
        return None, None
    best_priority = ranked[0].priority
    tied = [row for row in ranked if row.priority == best_priority]
    if len(tied) > 1:
        return None, {
            "classification": "EXIT_STRATEGY_SELECTION_REVIEW_REQUIRED",
            "reason": "same_priority_exit_strategy_tie",
            "group_key": group_key,
            "priority": best_priority,
            "candidate_decision_ids": [row.decision_id for row in tied],
            "candidate_strategy_types": [row.strategy_type.value for row in tied],
        }
    return ranked[0], None


def _selection_candidate(*, decision: Mapping[str, Any], source_ref: SourceArtifactRef) -> TrackBExitStrategySelection:
    strategy_type = _strategy_type(decision)
    return TrackBExitStrategySelection(
        selection_id=_selection_id(decision=decision, strategy_type=strategy_type),
        strategy_type=strategy_type,
        priority=STRATEGY_PRIORITY[strategy_type],
        decision_id=str(decision.get("decision_id") or ""),
        execution_domain=str(decision.get("execution_domain") or ""),
        account_id=str(decision.get("account_id") or ""),
        con_id=_int(decision.get("con_id")),
        local_symbol=str(decision.get("local_symbol") or ""),
        instrument=str(decision.get("instrument") or ""),
        side=str(decision.get("side") or ""),
        qty=str(decision.get("qty") or ""),
        action=str(decision.get("action") or ""),
        reason=str(decision.get("reason") or ""),
        close_only=True,
        entry_allowed=False,
        flip_allowed=False,
        selected_decision=decision,
        source_artifact_refs=(source_ref,),
        diagnostics=tuple(_list(decision.get("diagnostics"))),
    )


def _group_actionable_decisions(decisions: list[Any]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for decision in (_mapping(row) for row in decisions):
        if not _actionable(decision):
            continue
        groups.setdefault(_position_key(decision), []).append(decision)
    return groups


def _actionable(decision: Mapping[str, Any]) -> bool:
    return str(decision.get("action") or "") in {"REDUCE", "FULL_CLOSE", "PROTECT", "REVERSE_CONSIDER"}


def _strategy_type(decision: Mapping[str, Any]) -> ExitStrategyType:
    explicit = _strategy_from_text(decision.get("exit_strategy_type") or decision.get("strategy_type"))
    if explicit is not None:
        return explicit
    text = " ".join(
        str(value or "")
        for value in (
            decision.get("source_policy_id"),
            decision.get("reason"),
            decision.get("action"),
            _mapping(decision.get("selected_decision")).get("reason"),
        )
    ).lower()
    if "hard_stop" in text or "hard stop" in text:
        return ExitStrategyType.HARD_STOP
    if "operator" in text:
        return ExitStrategyType.OPERATOR_CLOSE
    if "profit" in text or "target" in text:
        return ExitStrategyType.PROFIT_TARGET
    if "trailing" in text:
        return ExitStrategyType.TRAILING_STOP
    if "vwap" in text or "reclaim" in text:
        return ExitStrategyType.VWAP_RECLAIM_LOSS
    if "reverse" in text or str(decision.get("action") or "") == "REVERSE_CONSIDER":
        return ExitStrategyType.REVERSAL_CLOSE_ONLY
    if str(decision.get("action") or "") == "REDUCE" or "partial" in text or "scale" in text:
        return ExitStrategyType.PARTIAL_SCALE_OUT
    if "protect" in text or str(decision.get("action") or "") == "PROTECT":
        return ExitStrategyType.PROTECTIVE_CLOSE
    return ExitStrategyType.TIMEBOX_CLOSE


def _strategy_from_text(value: Any) -> ExitStrategyType | None:
    if not value:
        return None
    normalized = str(value)
    for strategy in ExitStrategyType:
        if normalized == strategy.value:
            return strategy
    return None


def _classification(
    *,
    exit_decision: Mapping[str, Any],
    selected: list[TrackBExitStrategySelection],
    review_required: list[dict[str, Any]],
    held_decisions: list[dict[str, Any]],
) -> ExitStrategySelectionClassification:
    if not exit_decision:
        return ExitStrategySelectionClassification.SOURCE_MISSING
    if _decision_source_blocked(exit_decision):
        return ExitStrategySelectionClassification.DECISION_SOURCE_BLOCKED
    if review_required:
        return ExitStrategySelectionClassification.REVIEW_REQUIRED
    if selected:
        return ExitStrategySelectionClassification.SELECTED
    if held_decisions:
        return ExitStrategySelectionClassification.NO_ACTIONABLE_EXITS
    return ExitStrategySelectionClassification.FLAT


def _decision_source_blocked(exit_decision: Mapping[str, Any]) -> bool:
    classification = str(exit_decision.get("classification") or "")
    return "BLOCKED" in classification or "SOURCE_MISSING" in classification


def _position_key(decision: Mapping[str, Any]) -> str:
    return "|".join(
        str(decision.get(key) or "")
        for key in ("execution_domain", "account_id", "con_id", "local_symbol")
    )


def _selection_id(*, decision: Mapping[str, Any], strategy_type: ExitStrategyType) -> str:
    return f"exit_strategy_selection_{strategy_type.value}_{str(decision.get('decision_id') or 'unknown')}"


def _normalize_strategy_type(value: ExitStrategyType | str) -> ExitStrategyType:
    try:
        return value if isinstance(value, ExitStrategyType) else ExitStrategyType(str(value))
    except ValueError as exc:
        raise TrackBModelError("exit strategy type is not valid.") from exc


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


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TrackBModelError(f"{field_name} is required.")
    return text


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
