"""Pure Track B ExitIntent factory.

This phase consumes PositionState rows through selected ExitDecision /
ExitStrategySelector results and creates ExitIntent V1.1 contracts. It does
not validate authority, submit/cancel/close, start services, restart runtime,
or mutate broker/lifecycle state.
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

from mgc_v05l.execution_core.models import TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    AttributionStatus,
    CloseAction,
    CloseQtySource,
    ExecutionDomain,
    ExitAttribution,
    ExitIntent,
    ExitType,
    ExitUrgency,
    PositionSide,
    SourceArtifactRef,
)
from mgc_v05l.execution_core.track_b_exit_strategy_selector import DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT


REPO_ROOT = Path(__file__).resolve().parents[3]
EXIT_INTENT_FACTORY_REPORT_SCHEMA_VERSION = "track_b_exit_intent_factory_report_v1"

DEFAULT_EXIT_INTENT_FACTORY_REPORT = (
    Path("outputs") / "track_b_execution_core" / "exit_intent_factory" / "latest_exit_intent_factory.json"
)

DEFAULT_PRICE_POLICY: dict[str, Any] = {
    "type": "PLACEHOLDER_GUARDED_PRICE_POLICY",
    "source": "track_b_exit_intent_factory_phase4",
    "requires_current_executable_price_before_apply": True,
}


class ExitIntentFactoryClassification(str, Enum):
    FLAT = "EXIT_INTENT_FACTORY_FLAT"
    INTENTS_READY = "EXIT_INTENT_FACTORY_INTENTS_READY"
    NO_ACTIONABLE_SELECTIONS = "EXIT_INTENT_FACTORY_NO_ACTIONABLE_SELECTIONS"
    BLOCKED = "EXIT_INTENT_FACTORY_BLOCKED"
    SELECTOR_SOURCE_BLOCKED = "EXIT_INTENT_FACTORY_SELECTOR_SOURCE_BLOCKED"
    SOURCE_MISSING = "EXIT_INTENT_FACTORY_SOURCE_MISSING"


@dataclass(frozen=True)
class TrackBExitIntentFactoryConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_EXIT_INTENT_FACTORY_REPORT
    selector_path: Path = DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT
    price_policy: Mapping[str, Any] | None = None
    default_full_close_qty_source: CloseQtySource | str = CloseQtySource.STRATEGY_POLICY

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    @property
    def resolved_price_policy(self) -> dict[str, Any]:
        return dict(self.price_policy or DEFAULT_PRICE_POLICY)


def build_track_b_exit_intent_factory_report(
    *,
    config: TrackBExitIntentFactoryConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    selector = dict(
        (input_overrides or {}).get("exit_strategy_selector")
        or _read_json(config.resolve(config.selector_path))
    )
    source_ref = SourceArtifactRef(
        name="exit_strategy_selector",
        path=str(config.resolve(config.selector_path)),
        generated_at=_parse_dt(selector.get("generated_at")),
        authority_layer="ExitIntentFactory",
    )
    intents: list[ExitIntent] = []
    intent_metadata: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    if not selector:
        classification = ExitIntentFactoryClassification.SOURCE_MISSING
    elif _selector_source_blocked(selector):
        classification = ExitIntentFactoryClassification.SELECTOR_SOURCE_BLOCKED
    else:
        for selection in (_mapping(row) for row in _list(selector.get("selected_decisions"))):
            intent, block = _intent_for_selection(
                selection=selection,
                config=config,
                source_ref=source_ref,
                generated_at=actual_now,
            )
            if intent is not None:
                intents.append(intent)
                intent_metadata.append(_intent_metadata(intent))
            if block is not None:
                blocked.append(block)
        diagnostics.extend(_list(selector.get("diagnostic_rows")))
        classification = _classification(selector=selector, intents=intents, blocked=blocked)
    return {
        "schema_version": EXIT_INTENT_FACTORY_REPORT_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "authority_validated": False,
        "actuator_integrated": False,
        "live_money_eligible": selector.get("live_money_eligible") is True,
        "paper_proof_invoked": selector.get("paper_proof_invoked") is True,
        "classification": classification.value,
        "exit_strategy_selector_classification": selector.get("classification"),
        "intent_count": len(intents),
        "exit_intents": [intent.to_json_dict() for intent in intents],
        "exit_intent_metadata": intent_metadata,
        "blocked_count": len(blocked),
        "blocked_selections": blocked,
        "diagnostic_rows": diagnostics,
        "price_policy": dict(config.resolved_price_policy),
        "source_artifact_paths": {"exit_strategy_selector": str(config.resolve(config.selector_path))},
    }


def run_track_b_exit_intent_factory_report(
    *,
    config: TrackBExitIntentFactoryConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_exit_intent_factory_report(config=config, now=now)
    if write:
        write_track_b_exit_intent_factory_report(config=config, payload=payload)
    return payload


def write_track_b_exit_intent_factory_report(
    *, config: TrackBExitIntentFactoryConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-exit-intent-factory")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_EXIT_INTENT_FACTORY_REPORT)
    parser.add_argument("--selector-path", type=Path, default=DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBExitIntentFactoryConfig(
        repo_root=args.repo_root,
        output_path=args.output_path,
        selector_path=args.selector_path,
    )
    payload = run_track_b_exit_intent_factory_report(config=config, write=not args.no_write)
    if args.json or args.no_write:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']} intents={payload['intent_count']} blocked={payload['blocked_count']}")
    return 0


def _intent_for_selection(
    *,
    selection: Mapping[str, Any],
    config: TrackBExitIntentFactoryConfig,
    source_ref: SourceArtifactRef,
    generated_at: datetime,
) -> tuple[ExitIntent | None, dict[str, Any] | None]:
    decision = _mapping(selection.get("selected_decision"))
    position = _mapping(decision.get("position"))
    action = str(decision.get("action") or selection.get("action") or "").upper()
    if action == "HOLD":
        return None, _blocked(selection=selection, reason="hold_decision_does_not_create_exit_intent")
    if action not in {"REDUCE", "FULL_CLOSE", "PROTECT", "REVERSE_CONSIDER"}:
        return None, _blocked(selection=selection, reason="unsupported_exit_decision_action")

    strategy_type = str(selection.get("strategy_type") or "")
    owned_qty = _owned_qty(position=position, decision=decision)
    close_qty_source, source_error = _close_qty_source(
        selection=selection,
        decision=decision,
        config=config,
        partial=action == "REDUCE" or strategy_type == "partial_scale_out",
    )
    if source_error:
        return None, _blocked(selection=selection, reason=source_error)
    close_qty, qty_error = _close_qty(decision=decision, owned_qty=owned_qty, partial=action == "REDUCE")
    if qty_error:
        return None, _blocked(selection=selection, reason=qty_error)
    side = _position_side(position.get("side") or decision.get("side") or selection.get("side"))
    close_action = CloseAction.SELL if side == PositionSide.LONG else CloseAction.BUY
    exit_type = _exit_type(strategy_type=strategy_type, action=action)
    source_policy_id = _source_policy_id(selection=selection, decision=decision, exit_type=exit_type)
    attribution = _attribution(position)
    try:
        intent = ExitIntent(
            exit_intent_id=_exit_intent_id(selection=selection, source_policy_id=source_policy_id),
            execution_domain=position.get("execution_domain") or decision.get("execution_domain") or selection.get("execution_domain"),
            account_id=str(position.get("account_id") or decision.get("account_id") or selection.get("account_id") or ""),
            instrument=str(position.get("instrument") or decision.get("instrument") or selection.get("instrument") or ""),
            local_symbol=str(position.get("local_symbol") or decision.get("local_symbol") or selection.get("local_symbol") or ""),
            con_id=_int(position.get("con_id") or decision.get("con_id") or selection.get("con_id")),
            position_side=side,
            owned_qty=owned_qty,
            close_action=close_action,
            close_qty=close_qty,
            remaining_qty_after=owned_qty - close_qty,
            close_qty_source=close_qty_source,
            exit_type=exit_type,
            exit_reason=str(decision.get("reason") or selection.get("reason") or exit_type.value),
            priority=_int(selection.get("priority") or decision.get("priority")),
            urgency=str(decision.get("urgency") or _urgency_for_exit_type(exit_type).value),
            price_policy=config.resolved_price_policy,
            idempotency_key="",
            allow_partial=close_qty < owned_qty,
            allow_reverse=False,
            source_policy_id=source_policy_id,
            generated_at=generated_at,
            attribution=attribution,
            lifecycle_id=attribution.lifecycle_id,
            trade_id=attribution.trade_id,
            strategy_id=attribution.strategy_id,
            lane_id=attribution.lane_id,
            source_artifact_refs=tuple(_source_refs(selection, decision, position, source_ref)),
            partial_policy_supported=close_qty < owned_qty,
            live_money_eligible=False,
            paper_proof_invoked=False,
            broad_flatten_allowed=False,
            global_flatten_allowed=False,
        )
    except (TypeError, ValueError, TrackBModelError) as exc:
        return None, _blocked(selection=selection, reason=f"exit_intent_contract_invalid:{exc}")
    return intent, None


def _classification(
    *,
    selector: Mapping[str, Any],
    intents: list[ExitIntent],
    blocked: list[dict[str, Any]],
) -> ExitIntentFactoryClassification:
    if not selector:
        return ExitIntentFactoryClassification.SOURCE_MISSING
    if _selector_source_blocked(selector):
        return ExitIntentFactoryClassification.SELECTOR_SOURCE_BLOCKED
    if intents:
        return ExitIntentFactoryClassification.INTENTS_READY
    if blocked:
        return ExitIntentFactoryClassification.BLOCKED
    if int(selector.get("selected_count") or 0) == 0:
        classification = str(selector.get("classification") or "")
        if "FLAT" in classification:
            return ExitIntentFactoryClassification.FLAT
    return ExitIntentFactoryClassification.NO_ACTIONABLE_SELECTIONS


def _selector_source_blocked(selector: Mapping[str, Any]) -> bool:
    classification = str(selector.get("classification") or "")
    return "SOURCE_MISSING" in classification or "SOURCE_BLOCKED" in classification or "REVIEW_REQUIRED" in classification


def _owned_qty(*, position: Mapping[str, Any], decision: Mapping[str, Any]) -> Decimal:
    value = position.get("owned_qty") or decision.get("owned_qty") or position.get("qty") or decision.get("qty")
    return _positive_decimal(value, "owned_qty")


def _close_qty(
    *,
    decision: Mapping[str, Any],
    owned_qty: Decimal,
    partial: bool,
) -> tuple[Decimal, str | None]:
    if not partial:
        return owned_qty, None
    reduce_qty = decision.get("reduce_qty")
    if reduce_qty in {None, ""}:
        return Decimal("0"), "partial_close_qty_missing"
    close_qty = _positive_decimal(reduce_qty, "reduce_qty")
    if close_qty >= owned_qty:
        return Decimal("0"), "partial_close_qty_must_be_less_than_owned_qty"
    return close_qty, None


def _close_qty_source(
    *,
    selection: Mapping[str, Any],
    decision: Mapping[str, Any],
    config: TrackBExitIntentFactoryConfig,
    partial: bool,
) -> tuple[CloseQtySource | str, str | None]:
    raw = (
        decision.get("close_qty_source")
        or selection.get("close_qty_source")
        or _mapping(decision.get("position")).get("close_qty_source")
    )
    if partial and not raw:
        return CloseQtySource.STRATEGY_POLICY, "partial_close_qty_source_missing"
    return raw or config.default_full_close_qty_source, None


def _exit_type(*, strategy_type: str, action: str) -> ExitType:
    return {
        "hard_stop": ExitType.HARD_STOP,
        "operator_close": ExitType.OPERATOR_CLOSE,
        "protective_close": ExitType.PROTECTIVE_CLOSE,
        "reversal_close_only": ExitType.REVERSAL_EXIT,
        "profit_target": ExitType.PROFIT_TARGET,
        "trailing_stop": ExitType.TRAILING_STOP,
        "VWAP_reclaim_loss": ExitType.VWAP_RECLAIM_LOSS,
        "timebox_close": ExitType.TIMEBOX_CLOSE,
        "partial_scale_out": ExitType.PARTIAL_SCALE_OUT,
    }.get(strategy_type) or (
        ExitType.PARTIAL_SCALE_OUT
        if action == "REDUCE"
        else ExitType.REVERSAL_EXIT
        if action == "REVERSE_CONSIDER"
        else ExitType.TIMEBOX_CLOSE
    )


def _urgency_for_exit_type(exit_type: ExitType) -> ExitUrgency:
    if exit_type in {ExitType.HARD_STOP, ExitType.PROTECTIVE_CLOSE, ExitType.REVERSAL_EXIT, ExitType.OPERATOR_CLOSE}:
        return ExitUrgency.HIGH
    return ExitUrgency.NORMAL


def _source_policy_id(*, selection: Mapping[str, Any], decision: Mapping[str, Any], exit_type: ExitType) -> str:
    return str(decision.get("source_policy_id") or selection.get("source_policy_id") or exit_type.value).strip()


def _exit_intent_id(*, selection: Mapping[str, Any], source_policy_id: str) -> str:
    raw = "|".join(
        str(value or "")
        for value in (
            selection.get("selection_id"),
            selection.get("execution_domain"),
            selection.get("account_id"),
            selection.get("con_id"),
            selection.get("local_symbol"),
            source_policy_id,
        )
    )
    return "exit_intent_" + raw.lower().replace(" ", "_").replace("|", "_")


def _attribution(position: Mapping[str, Any]) -> ExitAttribution:
    return ExitAttribution(
        lifecycle_id=_text_or_none(position.get("lifecycle_id")),
        trade_id=_text_or_none(position.get("trade_id")),
        strategy_id=_text_or_none(position.get("strategy_id")),
        lane_id=_text_or_none(position.get("lane_id")),
    )


def _source_refs(
    selection: Mapping[str, Any],
    decision: Mapping[str, Any],
    position: Mapping[str, Any],
    source_ref: SourceArtifactRef,
) -> list[SourceArtifactRef | Mapping[str, Any]]:
    refs: list[SourceArtifactRef | Mapping[str, Any]] = [source_ref]
    for container in (selection, decision, position):
        refs.extend(_normalize_source_ref(row) for row in _list(container.get("source_artifact_refs")))
    return refs


def _position_side(value: Any) -> PositionSide:
    try:
        return PositionSide(str(value).upper())
    except ValueError as exc:
        raise TrackBModelError("position side must be LONG or SHORT.") from exc


def _blocked(*, selection: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "reason": reason,
        "selection_id": selection.get("selection_id"),
        "decision_id": selection.get("decision_id"),
        "local_symbol": selection.get("local_symbol"),
        "con_id": selection.get("con_id"),
    }


def _intent_metadata(intent: ExitIntent) -> dict[str, Any]:
    return {
        "exit_intent_id": intent.exit_intent_id,
        "attribution_status": intent.attribution.status.value,
        "execution_domain": intent.execution_domain.value,
        "account_id": intent.account_id,
    }


def _normalize_source_ref(value: Any) -> SourceArtifactRef:
    if isinstance(value, SourceArtifactRef):
        return value
    row = _mapping(value)
    if "generated_at" in row:
        row["generated_at"] = _parse_dt(row.get("generated_at"))
    return SourceArtifactRef(**row)


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


def _text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
