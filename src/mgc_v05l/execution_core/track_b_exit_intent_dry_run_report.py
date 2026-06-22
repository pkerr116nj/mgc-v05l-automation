"""Read-only ExitIntent dry-run report for Track B broker-scoped exits.

This module reads already-produced authority artifacts and builds candidate
ExitIntent/ExitAuthorityDecision pairs. It never connects to a broker, submits,
cancels, closes, starts services, or mutates runtime/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_position_guardian import DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_session_authority import DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_row
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    AttributionStatus,
    CloseQtySource,
    ExecutionDomain,
    ExitIntent,
    ExitType,
    ExitUrgency,
    SourceArtifactRef,
    validate_exit_authority,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EXIT_INTENT_DRY_RUN_REPORT = (
    Path("outputs") / "track_b_execution_core" / "exit_intent_dry_run" / "latest_exit_intent_dry_run_report.json"
)
DEFAULT_RECONCILIATION_REPORT_PATH = (
    Path("outputs") / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_IBKR_POSITIONS_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)

NO_BROKER_POSITIONS = "NO_BROKER_POSITIONS"
EXIT_INTENT_DRY_RUN_READY = "EXIT_INTENT_DRY_RUN_READY"
EXIT_INTENT_DRY_RUN_DEGRADED = "EXIT_INTENT_DRY_RUN_DEGRADED"
EXIT_INTENT_DRY_RUN_BLOCKED = "EXIT_INTENT_DRY_RUN_BLOCKED"


@dataclass(frozen=True)
class TrackBExitIntentDryRunReportConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_EXIT_INTENT_DRY_RUN_REPORT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_REPORT_PATH
    ibkr_positions_snapshot_path: Path = DEFAULT_IBKR_POSITIONS_SNAPSHOT_PATH
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
    guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    safe_state_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_exit_intent_dry_run_report(
    *,
    config: TrackBExitIntentDryRunReportConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    inputs = _inputs(config=config, overrides=input_overrides or {})
    source_refs = _source_artifact_refs(config=config, inputs=inputs)
    fresh_broker_positions = _broker_positions_from_fresh_broker_snapshot(inputs["ibkr_positions_snapshot"])
    fresh_broker_snapshot_authoritative = _fresh_broker_snapshot_authoritative(inputs["ibkr_positions_snapshot"])
    managed_broker_positions = _broker_positions_from_managed_positions(inputs["managed_positions"])
    reconciliation_broker_positions = [
        row
        for row in (_mapping(item) for item in _list(inputs["reconciliation"].get("track_b_broker_positions")))
        if abs(_decimal(row.get("quantity"))) > Decimal("0")
    ]
    broker_positions = (
        fresh_broker_positions
        if fresh_broker_snapshot_authoritative
        else managed_broker_positions or reconciliation_broker_positions
    )
    candidates = [
        _candidate_report(
            broker_position=position,
            inputs=inputs,
            source_refs=source_refs,
            now=actual_now,
        )
        for position in broker_positions
    ]
    classification = _classification(candidates)
    return {
        "schema_version": "track_b_exit_intent_dry_run_report_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "dry_run": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "live_money_eligible": _flag_true(inputs, "live_money_eligible"),
        "paper_proof_invoked": _flag_true(inputs, "paper_proof_invoked"),
        "classification": classification,
        "candidate_count": len(candidates),
        "allowed_count": sum(1 for row in candidates if row["authority_decision"]["decision"] == "ALLOWED"),
        "degraded_allowed_count": sum(
            1 for row in candidates if row["authority_decision"]["decision"] == "DEGRADED_ALLOWED"
        ),
        "blocked_count": sum(1 for row in candidates if row["authority_decision"]["decision"] == "BLOCKED"),
        "candidate_exit_intents": candidates,
        "source_classifications": {
            "reconciliation": inputs["reconciliation"].get("classification"),
            "ibkr_positions_snapshot": inputs["ibkr_positions_snapshot"].get("classification")
            or inputs["ibkr_positions_snapshot"].get("source"),
            "managed_positions": inputs["managed_positions"].get("classification"),
            "managed_orders": inputs["managed_orders"].get("classification"),
            "open_order_truth": inputs["open_order_truth"].get("classification"),
            "broker_session_authority": inputs["broker_session_authority"].get("classification"),
            "guardian": inputs["guardian"].get("classification"),
            "safe_state": inputs["safe_state"].get("classification"),
        },
        "source_artifact_paths": {
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "ibkr_positions_snapshot": str(config.resolve(config.ibkr_positions_snapshot_path)),
            "managed_positions": str(config.resolve(config.managed_position_registry_path)),
            "managed_orders": str(config.resolve(config.managed_order_registry_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "broker_session_authority": str(config.resolve(config.broker_session_authority_path)),
            "guardian": str(config.resolve(config.guardian_path)),
            "safe_state": str(config.resolve(config.safe_state_path)),
        },
    }


def _fresh_broker_snapshot_authoritative(snapshot: Mapping[str, Any]) -> bool:
    if not snapshot:
        return False
    if snapshot.get("positions_complete") is True:
        return True
    return snapshot.get("ok") is True and isinstance(snapshot.get("positions"), list)


def _broker_positions_from_fresh_broker_snapshot(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(snapshot.get("positions"))):
        quantity = _decimal(row.get("quantity") or row.get("position"))
        if abs(quantity) <= Decimal("0"):
            continue
        if _text(row.get("security_type") or row.get("secType")).upper() not in {"", "FUT"}:
            continue
        account_id = _text(row.get("account_id") or row.get("account") or snapshot.get("selected_account_id") or "DUM882026")
        normalized = normalize_track_b_contract_row(row, account_id=account_id)
        identity = _mapping(normalized.get("contract_identity"))
        local_symbol = _text(normalized.get("local_symbol") or identity.get("local_symbol") or row.get("local_symbol"))
        con_id = _int(normalized.get("con_id") or identity.get("con_id") or row.get("con_id") or row.get("qualified_contract_identifier"))
        if identity.get("resolved") is not True or not local_symbol or con_id <= 0:
            positions.append(
                {
                    "account_id": account_id,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "quantity": str(quantity),
                    "symbol": normalized.get("symbol") or identity.get("symbol") or row.get("symbol"),
                    "track_b_root": normalized.get("track_b_root") or identity.get("track_b_root") or row.get("track_b_root"),
                    "expiry": normalized.get("expiry") or identity.get("expiry") or row.get("expiry"),
                    "contract_identity": identity,
                    "identity_resolved": False,
                    "identity_blockers": list(identity.get("blockers") or ["contract_identity_unresolved"]),
                }
            )
            continue
        positions.append(
            {
                "account_id": account_id,
                "local_symbol": local_symbol,
                "con_id": con_id,
                "quantity": str(quantity),
                "symbol": normalized.get("symbol") or identity.get("symbol"),
                "track_b_root": normalized.get("track_b_root") or identity.get("track_b_root") or identity.get("instrument_family"),
                "expiry": normalized.get("expiry") or identity.get("expiry"),
                "contract_identity": identity,
                "identity_resolved": True,
            }
        )
    return positions


def _broker_positions_from_managed_positions(managed_positions: Mapping[str, Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(managed_positions.get("managed_positions"))):
        if not _managed_position_is_current(row):
            continue
        broker = _mapping(row.get("broker_position"))
        local_symbol = _text(broker.get("local_symbol") or row.get("local_symbol") or row.get("contract"))
        con_id = _int(broker.get("con_id") or row.get("con_id"))
        broker_quantity_raw = broker.get("quantity")
        quantity = _decimal(broker_quantity_raw if broker_quantity_raw is not None else row.get("quantity"))
        if not local_symbol or con_id <= 0 or abs(quantity) <= Decimal("0"):
            continue
        if broker_quantity_raw is None:
            side = _text(row.get("side") or broker.get("side")).upper()
            if side == "SHORT" and quantity > 0:
                quantity = -quantity
            elif side == "LONG" and quantity < 0:
                quantity = abs(quantity)
        positions.append(
            {
                "account_id": broker.get("account_id") or row.get("account_id") or "DUM882026",
                "local_symbol": local_symbol,
                "con_id": con_id,
                "quantity": str(quantity),
                "symbol": broker.get("symbol") or broker.get("track_b_root") or row.get("symbol") or row.get("instrument_family"),
                "track_b_root": broker.get("track_b_root") or row.get("track_b_root") or row.get("symbol"),
                "expiry": broker.get("expiry") or row.get("expiry"),
            }
        )
    return positions


def _managed_position_is_current(row: Mapping[str, Any]) -> bool:
    classification = _text(row.get("classification")).upper()
    if classification not in {"OPEN_MANAGED", "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE"} and row.get("exit_due") is not True:
        return False
    if row.get("projection_authority_owner_confirmed") is False:
        return False
    return True


def run_track_b_exit_intent_dry_run_report(
    *,
    config: TrackBExitIntentDryRunReportConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_exit_intent_dry_run_report(config=config, now=now)
    if write:
        write_track_b_exit_intent_dry_run_report(config=config, payload=payload)
    return payload


def write_track_b_exit_intent_dry_run_report(
    *, config: TrackBExitIntentDryRunReportConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def _candidate_report(
    *,
    broker_position: Mapping[str, Any],
    inputs: Mapping[str, Mapping[str, Any]],
    source_refs: Sequence[SourceArtifactRef],
    now: datetime,
) -> dict[str, Any]:
    if broker_position.get("identity_resolved") is False:
        return _unresolved_identity_candidate(broker_position=broker_position, inputs=inputs, now=now)
    managed_position = _matching_managed_position(broker_position=broker_position, managed_positions=inputs["managed_positions"])
    managed_order = _matching_managed_order(
        broker_position=broker_position,
        managed_position=managed_position,
        managed_orders=inputs["managed_orders"],
    )
    intent = _exit_intent(
        broker_position=broker_position,
        managed_position=managed_position,
        managed_order=managed_order,
        source_refs=source_refs,
        now=now,
    )
    state = _current_state(
        broker_position=broker_position,
        inputs=inputs,
        source_refs=source_refs,
        managed_position=managed_position,
    )
    decision = validate_exit_authority(intent=intent, current_state=state, validated_at=now)
    return {
        "instrument": intent.instrument,
        "local_symbol": intent.local_symbol,
        "con_id": intent.con_id,
        "account_id": intent.account_id,
        "position_side": intent.position_side.value,
        "broker_position_qty": str(abs(_decimal(broker_position.get("quantity")))),
        "candidate_close_action": intent.close_action.value,
        "candidate_close_qty": str(intent.close_qty),
        "close_qty_source": intent.close_qty_source.value,
        "price_policy": intent.price_policy,
        "attribution_status": decision.attribution_status.value,
        "attribution": intent.attribution.to_json_dict(),
        "managed_position_classification": managed_position.get("classification"),
        "managed_order_classification": managed_order.get("classification"),
        "exit_due": managed_position.get("exit_due"),
        "required_close_action": managed_order.get("required_close_action") or managed_position.get("required_close_action"),
        "exit_intent": intent.to_json_dict(),
        "authority_decision": decision.to_json_dict(),
        "block_reasons": list(decision.block_reasons),
        "degraded_reasons": _degraded_reasons(decision.to_json_dict()),
    }


def _unresolved_identity_candidate(
    *,
    broker_position: Mapping[str, Any],
    inputs: Mapping[str, Mapping[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    quantity = _decimal(broker_position.get("quantity"))
    side = "LONG" if quantity > 0 else "SHORT"
    action = "SELL" if side == "LONG" else "BUY"
    blockers = list(broker_position.get("identity_blockers") or ["contract_identity_unresolved"])
    return {
        "instrument": _text(broker_position.get("track_b_root") or broker_position.get("symbol") or broker_position.get("local_symbol")),
        "local_symbol": _text(broker_position.get("local_symbol")),
        "con_id": _int(broker_position.get("con_id")),
        "account_id": _text(broker_position.get("account_id") or "DUM882026"),
        "position_side": side,
        "broker_position_qty": str(abs(quantity)),
        "candidate_close_action": action,
        "candidate_close_qty": str(abs(quantity)),
        "close_qty_source": CloseQtySource.RISK_POLICY.value,
        "price_policy": {
            "type": "BLOCKED_UNRESOLVED_CONTRACT_IDENTITY",
            "source": "fresh_broker_snapshot_identity_normalization",
        },
        "attribution_status": AttributionStatus.UNATTRIBUTED.value,
        "attribution": {},
        "managed_position_classification": inputs["managed_positions"].get("classification"),
        "managed_order_classification": inputs["managed_orders"].get("classification"),
        "exit_due": None,
        "required_close_action": None,
        "exit_intent": {},
        "authority_decision": {
            "schema_version": "track_b_exit_authority_decision_v1_1",
            "exit_intent_id": "blocked_unresolved_contract_identity",
            "decision": "BLOCKED",
            "attribution_status": AttributionStatus.UNATTRIBUTED.value,
            "block_reasons": blockers,
            "validated_at": now.isoformat(),
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "diagnostics": {"contract_identity": _mapping(broker_position.get("contract_identity"))},
        },
        "block_reasons": blockers,
        "degraded_reasons": [],
    }


def _exit_intent(
    *,
    broker_position: Mapping[str, Any],
    managed_position: Mapping[str, Any],
    managed_order: Mapping[str, Any],
    source_refs: Sequence[SourceArtifactRef],
    now: datetime,
) -> ExitIntent:
    qty = abs(_decimal(broker_position.get("quantity")))
    side = "LONG" if _decimal(broker_position.get("quantity")) > 0 else "SHORT"
    action = "SELL" if side == "LONG" else "BUY"
    requested_close_qty = _requested_close_quantity(
        owned_qty=qty,
        managed_position=managed_position,
        managed_order=managed_order,
    )
    is_partial = requested_close_qty < qty
    account_id = _text(broker_position.get("account_id") or broker_position.get("account") or "DUM882026")
    local_symbol = _text(broker_position.get("local_symbol"))
    con_id = _int(broker_position.get("con_id") or managed_position.get("con_id") or managed_order.get("con_id"))
    instrument = _text(broker_position.get("track_b_root") or broker_position.get("symbol") or local_symbol.rstrip("0123456789"))
    lifecycle_id = _optional_text(managed_position.get("lifecycle_id") or managed_order.get("lifecycle_id"))
    trade_id = _optional_text(managed_position.get("trade_id") or managed_order.get("trade_id"))
    strategy_id = _optional_text(managed_position.get("strategy_id") or managed_order.get("strategy_id"))
    lane_id = _optional_text(managed_position.get("lane_id") or managed_order.get("lane_id"))
    payload = {
        "exit_intent_id": _exit_intent_id(account_id=account_id, local_symbol=local_symbol, con_id=con_id),
        "execution_domain": ExecutionDomain.TRACK_B_PAPER,
        "account_id": account_id,
        "instrument": instrument or local_symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "position_side": side,
        "owned_qty": str(qty),
        "close_action": action,
        "close_qty": str(requested_close_qty),
        "remaining_qty_after": str(qty - requested_close_qty),
        "close_qty_source": CloseQtySource.OPERATOR_INSTRUCTION if is_partial else CloseQtySource.RISK_POLICY,
        "exit_type": ExitType.PARTIAL_SCALE_OUT if is_partial else ExitType.FULL_CLOSE,
        "exit_reason": "duplicate_exposure_reduction" if is_partial else "broker_scoped_risk_reduction_dry_run",
        "priority": 50,
        "urgency": ExitUrgency.NORMAL,
        "price_policy": {
            "type": "PLACEHOLDER_GUARDED_LIMIT",
            "source": "read_only_exit_intent_dry_run",
            "requires_current_executable_price_before_apply": True,
        },
        "idempotency_key": "",
        "allow_partial": bool(is_partial),
        "allow_reverse": False,
        "source_policy_id": "TRACK_B_EXIT_INTENT_DRY_RUN_V1",
        "generated_at": now,
        "attribution": {
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "strategy_id": strategy_id,
            "lane_id": lane_id,
        },
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "source_artifact_refs": tuple(source_refs),
        "partial_policy_supported": bool(is_partial),
        "live_money_eligible": False,
        "live_money_allowed": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }
    return ExitIntent(**payload)


def _current_state(
    *,
    broker_position: Mapping[str, Any],
    inputs: Mapping[str, Mapping[str, Any]],
    source_refs: Sequence[SourceArtifactRef],
    managed_position: Mapping[str, Any],
) -> Mapping[str, Any]:
    qty = _decimal(broker_position.get("quantity"))
    same_contract_working_qty = _same_contract_working_close_qty(
        broker_position=broker_position,
        managed_orders=inputs["managed_orders"],
        open_order_truth=inputs["open_order_truth"],
    )
    unknown_open_orders = _unknown_open_order_count(inputs)
    same_contract_unknown_orders = _same_contract_unknown_order_count(
        broker_position=broker_position,
        open_order_truth=inputs["open_order_truth"],
        reconciliation=inputs["reconciliation"],
    )
    return {
        "execution_domain": ExecutionDomain.TRACK_B_PAPER,
        "known_position": True,
        "broker_position_side": "LONG" if qty > 0 else "SHORT",
        "broker_position_qty": str(abs(qty)),
        "account_id": _text(broker_position.get("account_id") or broker_position.get("account") or "DUM882026"),
        "local_symbol": _text(broker_position.get("local_symbol")),
        "con_id": _int(broker_position.get("con_id") or managed_position.get("con_id")),
        "safe_state_hard_halt": _safe_state_hard_halt(inputs["safe_state"]),
        "same_contract_working_close_qty": str(same_contract_working_qty),
        "unrelated_unknown_order_count": max(unknown_open_orders - same_contract_unknown_orders, 0),
        "same_contract_unknown_order_count": same_contract_unknown_orders,
        "same_contract_unknown_order_could_over_close": same_contract_unknown_orders > 0,
        "same_contract_unknown_order_over_close_ruled_out": False,
        "reconciliation_clean": _reconciliation_clean(inputs["reconciliation"]),
        "safe_state_allows_managed_close": _safe_state_allows_managed_close(inputs["safe_state"]),
        "guardian_allows_exact_close": _guardian_allows_exact_close(inputs["guardian"]),
        "bsa_managed_risk_reducing_close": _bsa_managed_close(inputs["broker_session_authority"]),
        "bsa_degraded_exact_close_ready": _bsa_degraded_ready(inputs["broker_session_authority"]),
        "live_money_eligible": _flag_true(inputs, "live_money_eligible"),
        "live_money_allowed": False,
        "paper_proof_invoked": _flag_true(inputs, "paper_proof_invoked"),
        "broad_flatten_allowed": _flag_true(inputs, "broad_flatten_allowed"),
        "global_flatten_allowed": _flag_true(inputs, "global_flatten_allowed") or _flag_true(inputs, "global_cancel_allowed"),
        "attribution_status": _attribution_status(managed_position),
        "attribution_diagnostics": {
            "managed_position_classification": managed_position.get("classification"),
            "lifecycle_id": managed_position.get("lifecycle_id"),
            "trade_id": managed_position.get("trade_id"),
            "strategy_id": managed_position.get("strategy_id"),
            "lane_id": managed_position.get("lane_id"),
        },
        "diagnostics": {
            "managed_position_artifact_classification": inputs["managed_positions"].get("classification"),
            "managed_order_artifact_classification": inputs["managed_orders"].get("classification"),
            "open_order_truth_classification": inputs["open_order_truth"].get("classification"),
            "broker_session_authority_classification": inputs["broker_session_authority"].get("classification"),
        },
        "source_artifact_refs": (),
    }


def _matching_managed_position(
    *, broker_position: Mapping[str, Any], managed_positions: Mapping[str, Any]
) -> dict[str, Any]:
    matches = [
        row
        for row in (_mapping(item) for item in _list(managed_positions.get("managed_positions")))
        if _same_position(row=row, broker_position=broker_position)
    ]
    if len(matches) == 1:
        return matches[0]
    return {}


def _requested_close_quantity(
    *,
    owned_qty: Decimal,
    managed_position: Mapping[str, Any],
    managed_order: Mapping[str, Any],
) -> Decimal:
    if managed_position.get("duplicate_same_lane_exposure") is True:
        raw = managed_position.get("required_close_quantity")
    else:
        raw = managed_order.get("required_close_quantity") or managed_position.get("required_close_quantity")
    requested = _decimal(raw)
    if requested <= 0 or requested > owned_qty:
        return owned_qty
    return requested


def _matching_managed_order(
    *,
    broker_position: Mapping[str, Any],
    managed_position: Mapping[str, Any],
    managed_orders: Mapping[str, Any],
) -> dict[str, Any]:
    lifecycle_id = _text(managed_position.get("lifecycle_id"))
    trade_id = _text(managed_position.get("trade_id"))
    matches = []
    for row in (_mapping(item) for item in _list(managed_orders.get("managed_orders"))):
        if lifecycle_id and _text(row.get("lifecycle_id")) == lifecycle_id:
            matches.append(row)
        elif trade_id and _text(row.get("trade_id")) == trade_id:
            matches.append(row)
        elif _same_position(row=row, broker_position=broker_position):
            matches.append(row)
    return matches[0] if len(matches) == 1 else {}


def _same_position(*, row: Mapping[str, Any], broker_position: Mapping[str, Any]) -> bool:
    broker = _mapping(row.get("broker_position"))
    account = _text(row.get("account_id") or broker.get("account_id"))
    symbol = _text(row.get("local_symbol") or row.get("contract") or broker.get("local_symbol"))
    con_id = _text(row.get("con_id") or broker.get("con_id"))
    broker_account = _text(broker_position.get("account_id") or broker_position.get("account"))
    broker_symbol = _text(broker_position.get("local_symbol"))
    broker_con_id = _text(broker_position.get("con_id"))
    account_matches = not account or not broker_account or account == broker_account
    return account_matches and ((con_id and con_id == broker_con_id) or (symbol and symbol == broker_symbol))


def _same_contract_working_close_qty(
    *,
    broker_position: Mapping[str, Any],
    managed_orders: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> Decimal:
    total = Decimal("0")
    rows = [(row, True) for row in [*_list(open_order_truth.get("broker_open_orders")), *_list(open_order_truth.get("open_orders"))]]
    rows.extend((row, False) for row in _list(managed_orders.get("managed_orders")) if _mapping(row).get("working") is True)
    for item, from_open_order_truth in rows:
        row = _mapping(item)
        if _same_position(row=row, broker_position=broker_position):
            quantity = _decimal(row.get("remaining_quantity") or row.get("quantity") or 0)
            if quantity == Decimal("0") and (row.get("working") is True or from_open_order_truth):
                quantity = abs(_decimal(broker_position.get("quantity")))
            total += abs(quantity)
    return total


def _unknown_open_order_count(inputs: Mapping[str, Mapping[str, Any]]) -> int:
    return max(
        _int(inputs["reconciliation"].get("unknown_broker_open_order_count")),
        _int(inputs["open_order_truth"].get("unknown_open_order_count")),
        len(_list(inputs["open_order_truth"].get("unknown_open_orders"))),
    )


def _same_contract_unknown_order_count(
    *,
    broker_position: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> int:
    rows = [*_list(open_order_truth.get("unknown_open_orders")), *_list(reconciliation.get("unknown_broker_open_orders"))]
    return sum(1 for row in (_mapping(item) for item in rows) if _same_position(row=row, broker_position=broker_position))


def _inputs(
    *,
    config: TrackBExitIntentDryRunReportConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    paths = {
        "reconciliation": config.reconciliation_path,
        "ibkr_positions_snapshot": config.ibkr_positions_snapshot_path,
        "managed_positions": config.managed_position_registry_path,
        "managed_orders": config.managed_order_registry_path,
        "open_order_truth": config.open_order_truth_path,
        "broker_session_authority": config.broker_session_authority_path,
        "guardian": config.guardian_path,
        "safe_state": config.safe_state_path,
    }
    return {name: overrides[name] if name in overrides else _read_json(config.resolve(path)) for name, path in paths.items()}


def _source_artifact_refs(
    *,
    config: TrackBExitIntentDryRunReportConfig,
    inputs: Mapping[str, Mapping[str, Any]],
) -> tuple[SourceArtifactRef, ...]:
    paths = {
        "reconciliation": config.reconciliation_path,
        "ibkr_positions_snapshot": config.ibkr_positions_snapshot_path,
        "managed_positions": config.managed_position_registry_path,
        "managed_orders": config.managed_order_registry_path,
        "open_order_truth": config.open_order_truth_path,
        "broker_session_authority": config.broker_session_authority_path,
        "guardian": config.guardian_path,
        "safe_state": config.safe_state_path,
    }
    refs: list[SourceArtifactRef] = []
    for name, path in paths.items():
        payload = inputs.get(name, {})
        refs.append(
            SourceArtifactRef(
                name=name,
                path=str(config.resolve(path)),
                generated_at=_parse_time(payload.get("generated_at")),
                authority_layer="ExitIntent Dry-Run Report",
            )
        )
    return tuple(refs)


def _classification(candidates: Sequence[Mapping[str, Any]]) -> str:
    if not candidates:
        return NO_BROKER_POSITIONS
    decisions = [str(_mapping(row.get("authority_decision")).get("decision") or "") for row in candidates]
    if any(item == "BLOCKED" for item in decisions):
        return EXIT_INTENT_DRY_RUN_BLOCKED
    if any(item == "DEGRADED_ALLOWED" for item in decisions):
        return EXIT_INTENT_DRY_RUN_DEGRADED
    return EXIT_INTENT_DRY_RUN_READY


def _degraded_reasons(decision: Mapping[str, Any]) -> list[str]:
    if decision.get("decision") != "DEGRADED_ALLOWED":
        return []
    reasons: list[str] = []
    if decision.get("attribution_status") != AttributionStatus.ATTRIBUTED.value:
        reasons.append("attribution_incomplete")
        return reasons
    risk_checks = _mapping(decision.get("conditional_risk_checks"))
    unknown_order_check = _mapping(risk_checks.get("same_contract_unknown_order_risk"))
    if unknown_order_check.get("passed") is True and "unknown_order" in str(unknown_order_check.get("code") or ""):
        reasons.append("same_contract_unknown_order_over_close_ruled_out")
    return reasons


def _attribution_status(managed_position: Mapping[str, Any]) -> AttributionStatus:
    fields = (
        _text(managed_position.get("lifecycle_id")),
        _text(managed_position.get("trade_id")),
        _text(managed_position.get("strategy_id")),
        _text(managed_position.get("lane_id")),
    )
    present = sum(1 for item in fields if item)
    if present == len(fields):
        return AttributionStatus.ATTRIBUTED
    if present:
        return AttributionStatus.PARTIALLY_ATTRIBUTED
    return AttributionStatus.UNATTRIBUTED


def _reconciliation_clean(reconciliation: Mapping[str, Any]) -> bool:
    return str(reconciliation.get("classification") or "") in {
        "TRACK_B_PAPER_BROKER_RECONCILED",
        "BROKER_LIFECYCLE_RECONCILED",
        "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER",
    } and reconciliation.get("broker_reconciled") is True


def _safe_state_hard_halt(safe_state: Mapping[str, Any]) -> bool:
    classification = str(safe_state.get("classification") or "").upper()
    if "HARD" in classification and "HOLD" in classification:
        return True
    if "HALT" in classification or "UNSAFE" in classification:
        return True
    return _mapping(safe_state.get("close_authority")).get("hard_halt") is True


def _safe_state_allows_managed_close(safe_state: Mapping[str, Any]) -> bool | None:
    close = _mapping(safe_state.get("close_authority"))
    if not close:
        return None
    return close.get("allowed") is True


def _guardian_allows_exact_close(guardian: Mapping[str, Any]) -> bool | None:
    authority = _mapping(guardian.get("managed_close_authority"))
    if not authority:
        return None
    return authority.get("allowed") is True


def _bsa_managed_close(authority: Mapping[str, Any]) -> bool | None:
    uses = _mapping(authority.get("allowed_uses"))
    if not uses:
        return None
    return uses.get("managed_risk_reducing_close") is True


def _bsa_degraded_ready(authority: Mapping[str, Any]) -> bool | None:
    context = _mapping(authority.get("degraded_exact_risk_reducing_close_context"))
    if not context:
        return None
    return context.get("ready") is True


def _flag_true(inputs: Mapping[str, Mapping[str, Any]], key: str) -> bool:
    return any(payload.get(key) is True for payload in inputs.values())


def _exit_intent_id(*, account_id: str, local_symbol: str, con_id: int) -> str:
    return f"exit_intent_dry_run_{account_id}_{local_symbol}_{con_id}".lower()


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


def _text(value: Any) -> str:
    return str(value or "").strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
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
    parser.add_argument("--output-path", type=Path, default=DEFAULT_EXIT_INTENT_DRY_RUN_REPORT)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBExitIntentDryRunReportConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = run_track_b_exit_intent_dry_run_report(config=config, write=not args.no_write)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"candidate_count={payload.get('candidate_count')}")
        print(f"blocked_count={payload.get('blocked_count')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
