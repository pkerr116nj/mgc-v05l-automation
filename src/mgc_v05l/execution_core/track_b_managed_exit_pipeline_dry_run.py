"""Pure end-to-end Track B managed-exit pipeline dry run.

This report wires the pure managed-exit layers together:

PositionState -> ExitDecision -> ExitStrategySelector -> ExitIntentFactory
-> ExitAuthorityValidator V1.1.

It consumes current artifact snapshots only. It does not connect to a broker,
submit, cancel, close, start services, restart runtime, or mutate lifecycle
state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import TrackBModelError, require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_position_guardian import DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_session_authority import DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
from mgc_v05l.execution_core.track_b_current_state_authority import (
    current_state_same_contract,
    normalize_current_broker_position,
    open_order_truth_unknown_count,
    same_contract_unknown_order_count,
    same_contract_working_close_qty,
)
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    AttributionStatus,
    ExecutionDomain,
    ExitAuthorityCurrentState,
    ExitIntent,
    SourceArtifactRef,
    validate_exit_authority,
)
from mgc_v05l.execution_core.track_b_exit_decision import (
    DEFAULT_EXIT_DECISION_REPORT,
    TrackBExitDecisionReportConfig,
    build_track_b_exit_decision_report,
)
from mgc_v05l.execution_core.track_b_exit_intent_factory import (
    DEFAULT_EXIT_INTENT_FACTORY_REPORT,
    TrackBExitIntentFactoryConfig,
    build_track_b_exit_intent_factory_report,
)
from mgc_v05l.execution_core.track_b_exit_strategy_selector import (
    DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT,
    TrackBExitStrategySelectorConfig,
    build_track_b_exit_strategy_selector_report,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_position_state import (
    DEFAULT_POSITION_STATE_REPORT,
    TrackBPositionStateReportConfig,
    build_track_b_position_state_report,
)
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT


REPO_ROOT = Path(__file__).resolve().parents[3]
MANAGED_EXIT_PIPELINE_DRY_RUN_SCHEMA_VERSION = "track_b_managed_exit_pipeline_dry_run_v1"

DEFAULT_MANAGED_EXIT_PIPELINE_DRY_RUN_REPORT = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_pipeline_dry_run"
    / "latest_managed_exit_pipeline_dry_run.json"
)
DEFAULT_RECONCILIATION_REPORT_PATH = (
    Path("outputs") / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_BROKER_POSITIONS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)


class ManagedExitPipelineDryRunClassification(str, Enum):
    NO_POSITIONS = "NO_POSITIONS"
    HOLD_ONLY = "HOLD_ONLY"
    EXIT_INTENT_ALLOWED = "EXIT_INTENT_ALLOWED"
    EXIT_INTENT_BLOCKED = "EXIT_INTENT_BLOCKED"
    PIPELINE_ERROR = "PIPELINE_ERROR"


@dataclass(frozen=True)
class TrackBManagedExitPipelineDryRunConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_MANAGED_EXIT_PIPELINE_DRY_RUN_REPORT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_REPORT_PATH
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    broker_positions_snapshot_path: Path = DEFAULT_BROKER_POSITIONS_SNAPSHOT
    broker_open_orders_snapshot_path: Path = DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
    guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    safe_state_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    position_state_path: Path = DEFAULT_POSITION_STATE_REPORT
    exit_decision_path: Path = DEFAULT_EXIT_DECISION_REPORT
    exit_strategy_selector_path: Path = DEFAULT_EXIT_STRATEGY_SELECTOR_REPORT
    exit_intent_factory_path: Path = DEFAULT_EXIT_INTENT_FACTORY_REPORT
    execution_domain: ExecutionDomain | str = ExecutionDomain.TRACK_B_PAPER
    account_id: str = "DUM882026"
    price_policy: Mapping[str, Any] | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_exit_pipeline_dry_run_report(
    *,
    config: TrackBManagedExitPipelineDryRunConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    decision_inputs: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    inputs = _inputs(config=config, overrides=input_overrides or {})
    position_state: dict[str, Any] = {}
    exit_decision: dict[str, Any] = {}
    selector: dict[str, Any] = {}
    intent_factory: dict[str, Any] = {}
    authority_decisions: list[dict[str, Any]] = []
    pipeline_errors: list[dict[str, Any]] = []
    pipeline_blockers: list[dict[str, Any]] = []
    try:
        position_state = build_track_b_position_state_report(
            config=TrackBPositionStateReportConfig(
                repo_root=config.repo_root,
                output_path=config.position_state_path,
                reconciliation_path=config.reconciliation_path,
                managed_position_registry_path=config.managed_position_registry_path,
                execution_domain=config.execution_domain,
                account_id=config.account_id,
            ),
            now=actual_now,
            input_overrides={
                "reconciliation": inputs["reconciliation"],
                "managed_positions": inputs["managed_positions"],
            },
        )
        exit_decision = _exit_decision_with_policy_qty_sources(
            decision_inputs=decision_inputs or {},
            exit_decision=dict(
            (input_overrides or {}).get("exit_decision")
            or build_track_b_exit_decision_report(
                config=TrackBExitDecisionReportConfig(
                    repo_root=config.repo_root,
                    output_path=config.exit_decision_path,
                    position_state_path=config.position_state_path,
                ),
                now=actual_now,
                input_overrides={
                    "position_state": position_state,
                    "managed_positions": inputs["managed_positions"],
                },
                decision_inputs=decision_inputs or {},
            )
            ),
        )
        selector = build_track_b_exit_strategy_selector_report(
            config=TrackBExitStrategySelectorConfig(
                repo_root=config.repo_root,
                output_path=config.exit_strategy_selector_path,
                exit_decision_path=config.exit_decision_path,
            ),
            now=actual_now,
            input_overrides={"exit_decision": exit_decision},
        )
        intent_factory = build_track_b_exit_intent_factory_report(
            config=TrackBExitIntentFactoryConfig(
                repo_root=config.repo_root,
                output_path=config.exit_intent_factory_path,
                selector_path=config.exit_strategy_selector_path,
                price_policy=config.price_policy,
            ),
            now=actual_now,
            input_overrides={"exit_strategy_selector": selector},
        )
        authority_decisions = _authority_decisions(
            intents=_list(intent_factory.get("exit_intents")),
            inputs={**inputs, "position_state": position_state},
            config=config,
            now=actual_now,
        )
    except TrackBModelError as exc:
        pipeline_blockers = [{"reason": "pipeline_contract_blocked", "detail": str(exc)}]
    except Exception as exc:  # pragma: no cover - defensive report boundary
        pipeline_errors = [{"reason": "pipeline_exception", "detail": str(exc)}]
    classification = _classification(
        position_state=position_state,
        exit_decision=exit_decision,
        selector=selector,
        intent_factory=intent_factory,
        authority_decisions=authority_decisions,
        pipeline_errors=pipeline_errors,
        pipeline_blockers=pipeline_blockers,
    )
    return {
        "schema_version": MANAGED_EXIT_PIPELINE_DRY_RUN_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "dry_run": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "actuator_integrated": False,
        "live_money_eligible": _flag_true(inputs, "live_money_eligible"),
        "paper_proof_invoked": _flag_true(inputs, "paper_proof_invoked"),
        "classification": classification.value,
        "positions": _list(position_state.get("positions")),
        "position_state": position_state,
        "decisions": _list(exit_decision.get("decisions")),
        "exit_decision": exit_decision,
        "selected_strategies": _list(selector.get("selected_decisions")),
        "exit_strategy_selector": selector,
        "generated_exit_intents": _list(intent_factory.get("exit_intents")),
        "exit_intent_factory": intent_factory,
        "exit_authority_decisions": authority_decisions,
        "pipeline_errors": pipeline_errors,
        "pipeline_blockers": pipeline_blockers,
        "source_classifications": {
            "position_state": position_state.get("classification"),
            "exit_decision": exit_decision.get("classification"),
            "exit_strategy_selector": selector.get("classification"),
            "exit_intent_factory": intent_factory.get("classification"),
            "reconciliation": inputs["reconciliation"].get("classification"),
            "managed_positions": inputs["managed_positions"].get("classification"),
            "managed_orders": inputs["managed_orders"].get("classification"),
            "open_order_truth": inputs["open_order_truth"].get("classification"),
            "broker_session_authority": inputs["broker_session_authority"].get("classification"),
            "guardian": inputs["guardian"].get("classification"),
            "safe_state": inputs["safe_state"].get("classification"),
        },
        "source_artifact_paths": _source_artifact_paths(config),
    }


def run_track_b_managed_exit_pipeline_dry_run_report(
    *,
    config: TrackBManagedExitPipelineDryRunConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_managed_exit_pipeline_dry_run_report(config=config, now=now)
    if write:
        write_track_b_managed_exit_pipeline_dry_run_report(config=config, payload=payload)
    return payload


def write_track_b_managed_exit_pipeline_dry_run_report(
    *, config: TrackBManagedExitPipelineDryRunConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def _authority_decisions(
    *,
    intents: Sequence[Any],
    inputs: Mapping[str, Mapping[str, Any]],
    config: TrackBManagedExitPipelineDryRunConfig,
    now: datetime,
) -> list[dict[str, Any]]:
    decisions: list[dict[str, Any]] = []
    source_refs = _source_refs(config=config, inputs=inputs)
    for intent_payload in (_mapping(row) for row in intents):
        intent = _exit_intent_from_payload(intent_payload)
        broker_position = _broker_position_for_intent(intent=intent, inputs=inputs)
        current_state = _current_state_for_intent(
            intent=intent,
            broker_position=broker_position,
            inputs=inputs,
            source_refs=source_refs,
        )
        decision = validate_exit_authority(intent=intent, current_state=current_state, validated_at=now)
        decisions.append(
            {
                "exit_intent_id": intent.exit_intent_id,
                "local_symbol": intent.local_symbol,
                "close_action": intent.close_action.value,
                "close_qty": str(intent.close_qty),
                "decision": decision.decision.value,
                "attribution_status": decision.attribution_status.value,
                "block_reasons": list(decision.block_reasons),
                "authority_decision": decision.to_json_dict(),
            }
        )
    return decisions


def _exit_intent_from_payload(payload: Mapping[str, Any]) -> ExitIntent:
    row = dict(payload)
    row["generated_at"] = _parse_time(row.get("generated_at"))
    refs: list[dict[str, Any]] = []
    for ref in (_mapping(item) for item in _list(row.get("source_artifact_refs"))):
        if "generated_at" in ref:
            ref["generated_at"] = _parse_time(ref.get("generated_at"))
        refs.append(ref)
    row["source_artifact_refs"] = tuple(refs)
    return ExitIntent(**row)


def _exit_decision_with_policy_qty_sources(
    *,
    exit_decision: Mapping[str, Any],
    decision_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    payload = dict(exit_decision)
    decisions: list[dict[str, Any]] = []
    for decision in (_mapping(row) for row in _list(payload.get("decisions"))):
        facts = _facts_for_decision(decision=decision, decision_inputs=decision_inputs)
        if decision.get("action") == "REDUCE" and facts.get("close_qty_source"):
            decision["close_qty_source"] = facts["close_qty_source"]
        decisions.append(decision)
    payload["decisions"] = decisions
    return payload


def _facts_for_decision(
    *,
    decision: Mapping[str, Any],
    decision_inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    position = _mapping(decision.get("position"))
    facts: dict[str, Any] = {}
    for key in (
        decision.get("local_symbol"),
        position.get("local_symbol"),
        position.get("lifecycle_id"),
        position.get("trade_id"),
        position.get("lane_id"),
        position.get("strategy_id"),
    ):
        text = str(key or "")
        if text and text in decision_inputs:
            facts.update(dict(decision_inputs[text]))
    return facts


def _current_state_for_intent(
    *,
    intent: ExitIntent,
    broker_position: Mapping[str, Any],
    inputs: Mapping[str, Mapping[str, Any]],
    source_refs: tuple[SourceArtifactRef, ...],
) -> ExitAuthorityCurrentState:
    broker_qty = _decimal(broker_position.get("quantity"))
    same_contract_unknown = _same_contract_unknown_order_count(intent=intent, inputs=inputs)
    return ExitAuthorityCurrentState(
        execution_domain=intent.execution_domain,
        known_position=bool(broker_position),
        broker_position_side="LONG" if broker_qty > 0 else "SHORT",
        broker_position_qty=str(abs(broker_qty)),
        account_id=str(broker_position.get("account_id") or broker_position.get("account") or intent.account_id),
        local_symbol=str(broker_position.get("local_symbol") or intent.local_symbol),
        con_id=_int(broker_position.get("con_id") or intent.con_id),
        safe_state_hard_halt=_safe_state_hard_halt(inputs["safe_state"]),
        same_contract_working_close_qty=str(_same_contract_working_close_qty(intent=intent, inputs=inputs)),
        unrelated_unknown_order_count=max(_unknown_open_order_count(inputs) - same_contract_unknown, 0),
        same_contract_unknown_order_count=same_contract_unknown,
        same_contract_unknown_order_could_over_close=same_contract_unknown > 0,
        same_contract_unknown_order_over_close_ruled_out=False,
        reconciliation_clean=_reconciliation_clean(inputs["reconciliation"]),
        safe_state_allows_managed_close=_safe_state_allows_managed_close(inputs["safe_state"]),
        guardian_allows_exact_close=_guardian_allows_exact_close(inputs["guardian"]),
        bsa_managed_risk_reducing_close=_bsa_managed_close(inputs["broker_session_authority"]),
        bsa_degraded_exact_close_ready=_bsa_degraded_ready(inputs["broker_session_authority"]),
        live_money_eligible=_flag_true(inputs, "live_money_eligible"),
        live_money_allowed=False,
        paper_proof_invoked=_flag_true(inputs, "paper_proof_invoked"),
        broad_flatten_allowed=_flag_true(inputs, "broad_flatten_allowed"),
        global_flatten_allowed=_flag_true(inputs, "global_flatten_allowed") or _flag_true(inputs, "global_cancel_allowed"),
        attribution_status=_attribution_status(intent),
        attribution_diagnostics={
            "lifecycle_id": intent.lifecycle_id,
            "trade_id": intent.trade_id,
            "strategy_id": intent.strategy_id,
            "lane_id": intent.lane_id,
        },
        diagnostics={
            "historical_debris_is_diagnostic_only": True,
            "current_executable_price_is_placeholder": True,
            "reconciliation_classification": inputs["reconciliation"].get("classification"),
            "open_order_truth_classification": inputs["open_order_truth"].get("classification"),
        },
        source_artifact_refs=source_refs,
    )


def _classification(
    *,
    position_state: Mapping[str, Any],
    exit_decision: Mapping[str, Any],
    selector: Mapping[str, Any],
    intent_factory: Mapping[str, Any],
    authority_decisions: Sequence[Mapping[str, Any]],
    pipeline_errors: Sequence[Mapping[str, Any]],
    pipeline_blockers: Sequence[Mapping[str, Any]],
) -> ManagedExitPipelineDryRunClassification:
    if pipeline_errors:
        return ManagedExitPipelineDryRunClassification.PIPELINE_ERROR
    if pipeline_blockers:
        return ManagedExitPipelineDryRunClassification.EXIT_INTENT_BLOCKED
    if not position_state:
        return ManagedExitPipelineDryRunClassification.PIPELINE_ERROR
    if int(position_state.get("position_count") or 0) == 0 and not _list(intent_factory.get("exit_intents")):
        return ManagedExitPipelineDryRunClassification.NO_POSITIONS
    selector_classification = str(selector.get("classification") or "")
    factory_classification = str(intent_factory.get("classification") or "")
    if "REVIEW_REQUIRED" in selector_classification or "BLOCKED" in factory_classification:
        return ManagedExitPipelineDryRunClassification.EXIT_INTENT_BLOCKED
    if not _list(intent_factory.get("exit_intents")):
        return ManagedExitPipelineDryRunClassification.HOLD_ONLY
    decisions = [str(row.get("decision") or "") for row in authority_decisions]
    if not decisions:
        return ManagedExitPipelineDryRunClassification.PIPELINE_ERROR
    if any(item == "BLOCKED" for item in decisions):
        return ManagedExitPipelineDryRunClassification.EXIT_INTENT_BLOCKED
    return ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED


def _inputs(
    *,
    config: TrackBManagedExitPipelineDryRunConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    paths = {
        "reconciliation": config.reconciliation_path,
        "managed_positions": config.managed_position_registry_path,
        "managed_orders": config.managed_order_registry_path,
        "open_order_truth": config.open_order_truth_path,
        "broker_positions_snapshot": config.broker_positions_snapshot_path,
        "broker_open_orders_snapshot": config.broker_open_orders_snapshot_path,
        "broker_session_authority": config.broker_session_authority_path,
        "guardian": config.guardian_path,
        "safe_state": config.safe_state_path,
    }
    return {name: dict(overrides[name] if name in overrides else _read_json(config.resolve(path))) for name, path in paths.items()}


def _source_artifact_paths(config: TrackBManagedExitPipelineDryRunConfig) -> dict[str, str]:
    return {
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "managed_positions": str(config.resolve(config.managed_position_registry_path)),
        "managed_orders": str(config.resolve(config.managed_order_registry_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "broker_positions_snapshot": str(config.resolve(config.broker_positions_snapshot_path)),
        "broker_open_orders_snapshot": str(config.resolve(config.broker_open_orders_snapshot_path)),
        "broker_session_authority": str(config.resolve(config.broker_session_authority_path)),
        "guardian": str(config.resolve(config.guardian_path)),
        "safe_state": str(config.resolve(config.safe_state_path)),
        "position_state": str(config.resolve(config.position_state_path)),
        "exit_decision": str(config.resolve(config.exit_decision_path)),
        "exit_strategy_selector": str(config.resolve(config.exit_strategy_selector_path)),
        "exit_intent_factory": str(config.resolve(config.exit_intent_factory_path)),
    }


def _source_refs(
    *,
    config: TrackBManagedExitPipelineDryRunConfig,
    inputs: Mapping[str, Mapping[str, Any]],
) -> tuple[SourceArtifactRef, ...]:
    refs: list[SourceArtifactRef] = []
    for name, path in _source_artifact_paths(config).items():
        if name not in inputs:
            continue
        payload = inputs[name]
        refs.append(
            SourceArtifactRef(
                name=name,
                path=path,
                generated_at=_parse_time(payload.get("generated_at")),
                authority_layer="ManagedExitPipelineDryRun",
            )
        )
    return tuple(refs)


def _broker_position_for_intent(
    *,
    intent: ExitIntent,
    inputs: Mapping[str, Mapping[str, Any]] | None = None,
    reconciliation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payloads = inputs or {"reconciliation": reconciliation or {}}
    for row in _position_state_broker_position_candidates(intent=intent, position_state=payloads.get("position_state") or {}):
        return row
    for row in _managed_projection_broker_position_candidates(
        intent=intent,
        managed_positions=payloads.get("managed_positions") or {},
    ):
        return row
    for row in _fresh_broker_snapshot_candidates(
        intent=intent,
        positions_snapshot=payloads.get("broker_positions_snapshot") or {},
    ):
        return row

    reconciliation_payload = reconciliation or payloads.get("reconciliation") or {}
    fallback_candidates: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(reconciliation_payload.get("track_b_broker_positions"))):
        normalized = _normalize_broker_position_candidate(row=row, intent=intent)
        if not normalized:
            continue
        local_symbol = str(normalized.get("local_symbol") or "").upper()
        row_con_id = _int(normalized.get("con_id"))
        if local_symbol == intent.local_symbol and row_con_id == intent.con_id:
            return normalized
        row_instrument = str(normalized.get("track_b_root") or normalized.get("symbol") or "").upper()
        if not local_symbol and row_con_id == 0 and row_instrument == intent.instrument:
            fallback_candidates.append(normalized)
    if len(fallback_candidates) == 1:
        return fallback_candidates[0]
    return {}


def _position_state_broker_position_candidates(
    *,
    intent: ExitIntent,
    position_state: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(position_state.get("positions"))):
        normalized = _normalize_position_state_candidate(row=row, intent=intent)
        if normalized:
            candidates.append(normalized)
    return candidates


def _managed_projection_broker_position_candidates(
    *,
    intent: ExitIntent,
    managed_positions: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(managed_positions.get("managed_positions"))):
        broker_position = _mapping(row.get("broker_position"))
        source = broker_position or row
        normalized = _normalize_broker_position_candidate(row=source, intent=intent)
        if normalized:
            candidates.append(normalized)
    return candidates


def _fresh_broker_snapshot_candidates(
    *,
    intent: ExitIntent,
    positions_snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not _broker_positions_snapshot_complete(positions_snapshot):
        return []
    candidates: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(positions_snapshot.get("positions"))):
        normalized = _normalize_broker_position_candidate(row=row, intent=intent)
        if normalized:
            candidates.append(normalized)
    return candidates


def _normalize_position_state_candidate(*, row: Mapping[str, Any], intent: ExitIntent) -> dict[str, Any]:
    quantity = _decimal(row.get("qty") or row.get("owned_qty"))
    if quantity <= 0:
        return {}
    side = str(row.get("side") or "").upper()
    signed = -quantity if side == "SHORT" else quantity
    candidate = {
        "account_id": row.get("account_id"),
        "local_symbol": row.get("local_symbol"),
        "con_id": row.get("con_id"),
        "symbol": row.get("instrument") or row.get("symbol") or row.get("track_b_root"),
        "track_b_root": row.get("instrument") or row.get("track_b_root") or row.get("symbol"),
        "quantity": str(signed),
    }
    return _normalize_broker_position_candidate(row=candidate, intent=intent)


def _normalize_broker_position_candidate(*, row: Mapping[str, Any], intent: ExitIntent) -> dict[str, Any]:
    return normalize_current_broker_position(
        row,
        account_id=intent.account_id,
        instrument=intent.instrument,
        local_symbol=intent.local_symbol,
        con_id=intent.con_id,
    )


def _broker_positions_snapshot_complete(snapshot: Mapping[str, Any]) -> bool:
    if not snapshot:
        return False
    return (
        snapshot.get("positions_complete") is True
        or snapshot.get("complete") is True
        or str(snapshot.get("classification") or "") in {"BROKER_TRUTH_REFRESH_READY", "IBKR_READ_ONLY_CONNECTED"}
    )


def _same_contract_working_close_qty(*, intent: ExitIntent, inputs: Mapping[str, Mapping[str, Any]]) -> Decimal:
    return same_contract_working_close_qty(
        open_order_truth=inputs["open_order_truth"],
        account_id=intent.account_id,
        local_symbol=intent.local_symbol,
        con_id=intent.con_id,
    )


def _same_contract_unknown_order_count(*, intent: ExitIntent, inputs: Mapping[str, Mapping[str, Any]]) -> int:
    return same_contract_unknown_order_count(
        open_order_truth=inputs["open_order_truth"],
        account_id=intent.account_id,
        local_symbol=intent.local_symbol,
        con_id=intent.con_id,
    )


def _unknown_open_order_count(inputs: Mapping[str, Mapping[str, Any]]) -> int:
    return open_order_truth_unknown_count(inputs["open_order_truth"])


def _same_contract(*, row: Mapping[str, Any], intent: ExitIntent) -> bool:
    return current_state_same_contract(
        row,
        account_id=intent.account_id,
        local_symbol=intent.local_symbol,
        con_id=intent.con_id,
    )


def _reconciliation_clean(reconciliation: Mapping[str, Any]) -> bool:
    return str(reconciliation.get("classification") or "") in {
        "TRACK_B_PAPER_BROKER_RECONCILED",
        "BROKER_LIFECYCLE_RECONCILED",
        "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER",
    } and reconciliation.get("broker_reconciled") is True


def _safe_state_hard_halt(safe_state: Mapping[str, Any]) -> bool:
    classification = str(safe_state.get("classification") or "").upper()
    return "HALT" in classification or "UNSAFE" in classification or ("HARD" in classification and "HOLD" in classification)


def _safe_state_allows_managed_close(safe_state: Mapping[str, Any]) -> bool | None:
    close = _mapping(safe_state.get("close_authority"))
    return close.get("allowed") is True if close else None


def _guardian_allows_exact_close(guardian: Mapping[str, Any]) -> bool | None:
    authority = _mapping(guardian.get("managed_close_authority"))
    return authority.get("allowed") is True if authority else None


def _bsa_managed_close(authority: Mapping[str, Any]) -> bool | None:
    uses = _mapping(authority.get("allowed_uses"))
    return uses.get("managed_risk_reducing_close") is True if uses else None


def _bsa_degraded_ready(authority: Mapping[str, Any]) -> bool | None:
    context = _mapping(authority.get("degraded_exact_risk_reducing_close_context"))
    return context.get("ready") is True if context else None


def _attribution_status(intent: ExitIntent) -> AttributionStatus:
    return intent.attribution.status


def _flag_true(inputs: Mapping[str, Mapping[str, Any]], key: str) -> bool:
    return any(payload.get(key) is True for payload in inputs.values())


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_MANAGED_EXIT_PIPELINE_DRY_RUN_REPORT)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBManagedExitPipelineDryRunConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = run_track_b_managed_exit_pipeline_dry_run_report(config=config, write=not args.no_write)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"positions={len(payload.get('positions') or [])}")
        print(f"intents={len(payload.get('generated_exit_intents') or [])}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
