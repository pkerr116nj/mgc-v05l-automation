"""Track B PAPER broker position guardian.

This authority artifact is read-only. It compares broker position/open-order
truth against managed lifecycle evidence and blocks duplicate closes or
unauthorized reverse exposure before any strategy path can continue.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


BROKER_POSITION_GUARDIAN_READY = "BROKER_POSITION_GUARDIAN_READY"
BROKER_POSITION_GUARDIAN_HARD_HOLD = "BROKER_POSITION_GUARDIAN_HARD_HOLD"
BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING = (
    "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING"
)
DUPLICATE_CLOSE_ORDER_BLOCKED = "DUPLICATE_CLOSE_ORDER_BLOCKED"
UNAUTHORIZED_REVERSE_EXPOSURE = "UNAUTHORIZED_REVERSE_EXPOSURE"
BROKER_LIFECYCLE_POSITION_MISMATCH = "BROKER_LIFECYCLE_POSITION_MISMATCH"
CLOSE_FILLED_LIFECYCLE_NOT_UPDATED = "CLOSE_FILLED_LIFECYCLE_NOT_UPDATED"
OPEN_ORDER_MANAGED_REGISTRY_MISMATCH = "OPEN_ORDER_MANAGED_REGISTRY_MISMATCH"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "broker_position_guardian" / "latest_broker_position_guardian.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs") / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json"
)


@dataclass(frozen=True)
class TrackBBrokerPositionGuardianConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_broker_position_guardian(
    *,
    config: TrackBBrokerPositionGuardianConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    inputs = _inputs(config=config, overrides=input_overrides or {})
    broker_positions = _broker_positions(inputs)
    lifecycle_positions = _lifecycle_positions(inputs)
    open_orders = _open_orders(inputs)
    managed_orders = _managed_orders(inputs)

    findings: list[dict[str, Any]] = []
    findings.extend(_duplicate_close_findings(inputs=inputs, open_orders=open_orders, managed_orders=managed_orders))
    findings.extend(
        _position_mismatch_findings(
            inputs=inputs,
            broker_positions=broker_positions,
            lifecycle_positions=lifecycle_positions,
        )
    )
    findings.extend(_open_order_registry_findings(inputs=inputs, open_orders=open_orders, managed_orders=managed_orders))

    hard_classifications = _dedupe([str(row.get("classification") or "") for row in findings if row.get("hard_hold")])
    classification = BROKER_POSITION_GUARDIAN_HARD_HOLD if hard_classifications else BROKER_POSITION_GUARDIAN_READY
    close_authority = _registry_verified_managed_close_authority(
        inputs=inputs,
        broker_positions=broker_positions,
        open_orders=open_orders,
        managed_orders=managed_orders,
    )
    remediation_plan = _scoped_remediation_plan(findings=findings, broker_positions=broker_positions)
    close_submit_allowed = classification == BROKER_POSITION_GUARDIAN_READY or close_authority.get("allowed") is True
    return {
        "schema_version": "track_b_broker_position_guardian_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "submit_authority": False,
        "cancel_authority": False,
        "flatten_authority": False,
        "broad_cancel_allowed": False,
        "global_flatten_allowed": False,
        "dashboard_projection_consumed": False,
        "classification": classification,
        "hard_classifications": hard_classifications,
        "findings": findings,
        "managed_close_authority": close_authority,
        "broker_positions": broker_positions,
        "open_orders": open_orders,
        "managed_order_rows": managed_orders,
        "lifecycle_positions": lifecycle_positions,
        "new_entries_allowed": classification == BROKER_POSITION_GUARDIAN_READY,
        "lane_progression_allowed": classification == BROKER_POSITION_GUARDIAN_READY,
        "guarded_roster_submit_allowed": classification == BROKER_POSITION_GUARDIAN_READY,
        "close_submit_allowed": close_submit_allowed,
        "managed_close_mutation_allowed": close_authority.get("allowed") is True,
        "managed_close_authority_reason_codes": list(close_authority.get("reason_codes") or []),
        "requires_exact_scoped_remediation_plan": classification == BROKER_POSITION_GUARDIAN_HARD_HOLD,
        "scoped_remediation_plan": remediation_plan,
        "operator_explanation": _operator_explanation(hard_classifications=hard_classifications, remediation_plan=remediation_plan),
        "source_artifact_paths": {
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
        },
    }


def write_track_b_broker_position_guardian(
    *,
    config: TrackBBrokerPositionGuardianConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write read-only Track B broker position guardian authority.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBBrokerPositionGuardianConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = build_track_b_broker_position_guardian(config=config)
    authority_path = write_track_b_broker_position_guardian(config=config, payload=payload)
    summary = {
        "classification": payload.get("classification"),
        "hard_classifications": payload.get("hard_classifications"),
        "new_entries_allowed": payload.get("new_entries_allowed"),
        "lane_progression_allowed": payload.get("lane_progression_allowed"),
        "guarded_roster_submit_allowed": payload.get("guarded_roster_submit_allowed"),
        "close_submit_allowed": payload.get("close_submit_allowed"),
        "managed_close_mutation_allowed": payload.get("managed_close_mutation_allowed"),
        "managed_close_authority_reason_codes": payload.get("managed_close_authority_reason_codes"),
        "requires_exact_scoped_remediation_plan": payload.get("requires_exact_scoped_remediation_plan"),
        "scoped_remediation_plan": payload.get("scoped_remediation_plan"),
        "authority_path": str(authority_path),
        "read_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("classification") == BROKER_POSITION_GUARDIAN_READY else 2


def _inputs(
    *,
    config: TrackBBrokerPositionGuardianConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    paths = {
        "position_truth": config.position_truth_path,
        "open_order_truth": config.open_order_truth_path,
        "managed_order_registry": config.managed_order_registry_path,
        "managed_position_registry": config.managed_position_registry_path,
        "reconciliation": config.reconciliation_path,
    }
    return {name: overrides.get(name) or _read_json(config.resolve(path)) for name, path in paths.items()}


def _duplicate_close_findings(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    open_orders: Sequence[Mapping[str, Any]],
    managed_orders: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    text = " ".join(
        str(value or "")
        for value in (
            inputs["open_order_truth"].get("classification"),
            inputs["managed_order_registry"].get("classification"),
        )
    ).upper()
    duplicate_groups = _list(inputs["open_order_truth"].get("duplicate_close_order_groups")) + _list(
        inputs["open_order_truth"].get("duplicates")
    )
    duplicate_rows = [
        row
        for row in list(open_orders) + list(managed_orders)
        if "DUPLICATE_CLOSE_ORDER" in str(row.get("classification") or "").upper()
    ]
    if "DUPLICATE_CLOSE_ORDER" not in text and not duplicate_groups and not duplicate_rows:
        return []
    return [
        {
            "classification": DUPLICATE_CLOSE_ORDER_BLOCKED,
            "hard_hold": True,
            "detail": "Duplicate managed close order evidence is present; second close submit is blocked.",
            "duplicate_groups": duplicate_groups,
            "orders": [_compact_order(row) for row in duplicate_rows or open_orders],
        }
    ]


def _registry_verified_managed_close_authority(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    broker_positions: Sequence[Mapping[str, Any]],
    open_orders: Sequence[Mapping[str, Any]],
    managed_orders: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reason_codes: list[str] = []
    candidates: list[dict[str, Any]] = []
    reconciliation = inputs["reconciliation"]
    registry = _mapping(reconciliation.get("registry_reconciliation"))
    if registry.get("classification") != "REGISTRY_RECONCILIATION_MATCHED" or registry.get("blocking") is True:
        reason_codes.append("REGISTRY_RECONCILIATION_NOT_MATCHED")
    open_broker_positions = [row for row in broker_positions if _decimal(row.get("quantity")) != Decimal("0")]
    if not open_broker_positions:
        reason_codes.append("BROKER_POSITION_MISSING")
    if _has_conflicting_close_order(open_orders=open_orders, managed_orders=managed_orders):
        reason_codes.append("CONFLICTING_CLOSE_ORDER")
    mapped_records = [_mapping(row) for row in _list(registry.get("mapped_records"))]
    if not mapped_records:
        reason_codes.append("REGISTRY_MAPPED_RECORD_MISSING")

    for broker_position in open_broker_positions:
        matches = [
            record
            for record in mapped_records
            if _same_contract(left=broker_position, right=record)
            and _text(record.get("account_id")) == _text(broker_position.get("account_id"))
        ]
        if len(matches) != 1:
            reason_codes.append("REGISTRY_MAPPED_RECORD_AMBIGUOUS" if matches else "REGISTRY_MAPPED_RECORD_NOT_FOUND")
            continue
        record = matches[0]
        record_reasons = _risk_reducing_close_record_reasons(broker_position=broker_position, record=record)
        if record_reasons:
            reason_codes.extend(record_reasons)
            continue
        broker_qty = _decimal(broker_position.get("quantity"))
        close_action = "SELL" if broker_qty > 0 else "BUY"
        candidates.append(
            {
                "classification": BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING,
                "trade_id": record.get("trade_id"),
                "lifecycle_id": record.get("lifecycle_id"),
                "account_id": broker_position.get("account_id"),
                "symbol": broker_position.get("symbol") or record.get("symbol") or record.get("instrument_family"),
                "local_symbol": broker_position.get("local_symbol") or record.get("local_symbol"),
                "con_id": broker_position.get("con_id") or broker_position.get("conId") or record.get("con_id"),
                "quantity": _decimal_display(min(abs(broker_qty), abs(_decimal(record.get("quantity"))))),
                "action": close_action,
                "reason": "Exact registry-backed managed close reduces current broker exposure.",
            }
        )

    reason_codes = _dedupe(reason_codes)
    allowed = bool(candidates) and not reason_codes
    return {
        "classification": (
            BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING
            if allowed
            else "BROKER_POSITION_GUARDIAN_CLOSE_BLOCKED"
        ),
        "allowed": allowed,
        "risk_reducing_only": allowed,
        "authority_source": "CENTRAL_TRADE_REGISTRY_RECONCILIATION",
        "reason_codes": [] if allowed else reason_codes,
        "candidates": candidates,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }


def _risk_reducing_close_record_reasons(
    *,
    broker_position: Mapping[str, Any],
    record: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not _text(record.get("trade_id")):
        reasons.append("TRADE_ID_MISSING")
    if not _text(record.get("lifecycle_id")):
        reasons.append("LIFECYCLE_ID_MISSING")
    if not (_text(record.get("entry_perm_id")) and _text(record.get("entry_exec_id"))):
        reasons.append("BROKER_BACKED_ENTRY_EVIDENCE_MISSING")
    if _text(record.get("account_id")) != _text(broker_position.get("account_id")):
        reasons.append("ACCOUNT_MISMATCH")
    if not _same_contract(left=broker_position, right=record):
        reasons.append("CONTRACT_MISMATCH")
    broker_qty = abs(_decimal(broker_position.get("quantity")))
    record_qty = abs(_decimal(record.get("quantity")))
    if record_qty == Decimal("0"):
        reasons.append("QUANTITY_MISSING")
    elif record_qty > broker_qty:
        reasons.append("CLOSE_QUANTITY_EXCEEDS_BROKER_POSITION")
    broker_side = _side_from_quantity(broker_position.get("quantity"))
    record_side = str(record.get("side") or "").upper()
    if record_side in {"LONG", "SHORT"} and record_side != broker_side:
        reasons.append("SIDE_MISMATCH")
    state = str(record.get("current_state") or "").upper()
    if state and state not in {"OPEN_MANAGED", "EXIT_DUE", "WORKING_EXIT"}:
        reasons.append("REGISTRY_STATE_NOT_OPEN_MANAGED")
    return reasons


def _has_conflicting_close_order(
    *,
    open_orders: Sequence[Mapping[str, Any]],
    managed_orders: Sequence[Mapping[str, Any]],
) -> bool:
    if open_orders:
        return True
    for order in managed_orders:
        action = str(order.get("action") or "").upper()
        if action not in {"BUY", "SELL"}:
            continue
        if order.get("working") is True or order.get("broker_order_id") or order.get("client_id"):
            return True
    return False


def _position_mismatch_findings(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    reconciliation = inputs["reconciliation"]
    broker_open = [row for row in broker_positions if _decimal(row.get("quantity")) != Decimal("0")]
    lifecycle_open = [row for row in lifecycle_positions if _decimal(row.get("quantity")) != Decimal("0")]
    if _exit_fill_expected_flat_event(reconciliation) and broker_open:
        findings.append(
            {
                "classification": UNAUTHORIZED_REVERSE_EXPOSURE,
                "hard_hold": True,
                "detail": "Broker position remains after a managed exit fill expected broker-flat state.",
                "broker_positions": [_compact_position(row) for row in broker_open],
                "settlement_event": _exit_fill_expected_flat_event(reconciliation),
            }
        )
    for broker_position in broker_open:
        match = _matching_lifecycle_position(broker_position=broker_position, lifecycle_positions=lifecycle_open)
        if match is None:
            findings.append(
                {
                    "classification": BROKER_LIFECYCLE_POSITION_MISMATCH,
                    "hard_hold": True,
                    "detail": "Broker position has no exact active lifecycle owner.",
                    "broker_position": _compact_position(broker_position),
                }
            )
            continue
        if _side_from_quantity(broker_position.get("quantity")) != _side_from_lifecycle(match):
            findings.append(
                {
                    "classification": UNAUTHORIZED_REVERSE_EXPOSURE,
                    "hard_hold": True,
                    "detail": "Broker position side is opposite the lifecycle owner side.",
                    "broker_position": _compact_position(broker_position),
                    "lifecycle_position": _compact_position(match),
                }
            )
            continue
        broker_qty = _decimal(broker_position.get("quantity"))
        lifecycle_qty = _signed_lifecycle_quantity(match)
        if lifecycle_qty != Decimal("0") and broker_qty != lifecycle_qty:
            findings.append(
                {
                    "classification": BROKER_LIFECYCLE_POSITION_MISMATCH,
                    "hard_hold": True,
                    "detail": "Broker aggregate quantity does not match lifecycle aggregate quantity.",
                    "broker_position": _compact_position(broker_position),
                    "lifecycle_position": _compact_position(match),
                    "broker_quantity": _decimal_display(broker_qty),
                    "lifecycle_aggregate_quantity": _decimal_display(lifecycle_qty),
                    "lifecycle_unit_count": match.get("lifecycle_unit_count")
                    or len(match.get("lifecycle_units") or []),
                }
            )
    findings.extend(_managed_registry_quantity_findings(inputs=inputs, broker_open=broker_open))
    for lifecycle_position in lifecycle_open:
        match = _matching_broker_position(lifecycle_position=lifecycle_position, broker_positions=broker_open)
        if match is None:
            findings.append(
                {
                    "classification": BROKER_LIFECYCLE_POSITION_MISMATCH,
                    "hard_hold": True,
                    "detail": "Lifecycle owner is open but broker position is missing.",
                    "lifecycle_position": _compact_position(lifecycle_position),
                }
            )
    if _exit_fill_expected_flat_event(reconciliation) and lifecycle_open:
        findings.append(
            {
                "classification": CLOSE_FILLED_LIFECYCLE_NOT_UPDATED,
                "hard_hold": True,
                "detail": "Close fill expected broker-flat state but lifecycle still reports open exposure.",
                "lifecycle_positions": [_compact_position(row) for row in lifecycle_open],
            }
        )
    return findings


def _managed_registry_quantity_findings(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    broker_open: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    managed_positions = _list(inputs["managed_position_registry"].get("managed_positions"))
    for managed_position in managed_positions:
        broker_position = _matching_broker_position(lifecycle_position=managed_position, broker_positions=broker_open)
        if broker_position is None:
            continue
        broker_qty = _decimal(broker_position.get("quantity"))
        managed_qty = _signed_lifecycle_quantity(managed_position)
        if managed_qty == Decimal("0") or broker_qty == managed_qty:
            continue
        findings.append(
            {
                "classification": BROKER_LIFECYCLE_POSITION_MISMATCH,
                "hard_hold": True,
                "detail": "Managed Position Registry visible aggregate quantity does not match broker truth.",
                "broker_position": _compact_position(broker_position),
                "managed_position": _compact_position(managed_position),
                "broker_quantity": _decimal_display(broker_qty),
                "managed_position_quantity": _decimal_display(managed_qty),
                "lifecycle_unit_count": managed_position.get("lifecycle_unit_count")
                or len(managed_position.get("lifecycle_units") or []),
            }
        )
    return findings


def _open_order_registry_findings(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    open_orders: Sequence[Mapping[str, Any]],
    managed_orders: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    open_count = len(open_orders)
    managed_working_count = sum(1 for row in managed_orders if row.get("working") is True or row.get("broker_order_id"))
    if open_count == managed_working_count:
        return []
    if open_count == 0 and managed_working_count == 0:
        return []
    return [
        {
            "classification": OPEN_ORDER_MANAGED_REGISTRY_MISMATCH,
            "hard_hold": True,
            "detail": "Broker open-order count is inconsistent with Managed Order Registry.",
            "open_order_count": open_count,
            "managed_working_order_count": managed_working_count,
            "open_order_truth_classification": inputs["open_order_truth"].get("classification"),
            "managed_order_registry_classification": inputs["managed_order_registry"].get("classification"),
        }
    ]


def _scoped_remediation_plan(
    *,
    findings: Sequence[Mapping[str, Any]],
    broker_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not any(row.get("classification") == UNAUTHORIZED_REVERSE_EXPOSURE for row in findings):
        return {
            "classification": "NO_SCOPED_BROKER_REMEDIATION_PLAN",
            "apply_enabled": False,
            "broker_mutation_allowed": False,
        }
    open_positions = [row for row in broker_positions if _decimal(row.get("quantity")) != Decimal("0")]
    if len(open_positions) != 1:
        return {
            "classification": "SCOPED_REMEDIATION_BLOCKED_AMBIGUOUS_POSITION",
            "apply_enabled": False,
            "broker_mutation_allowed": False,
            "broker_positions": [_compact_position(row) for row in open_positions],
        }
    position = open_positions[0]
    qty = _decimal(position.get("quantity"))
    action = "BUY" if qty < 0 else "SELL"
    fallback = _identity_fallback(findings)
    return {
        "classification": "SCOPED_REVERSE_EXPOSURE_FLATTEN_PLAN_READY",
        "apply_enabled": False,
        "broker_mutation_allowed": False,
        "requires_operator_authorization": True,
        "broad_flatten_allowed": False,
        "account_id": position.get("account_id"),
        "symbol": position.get("symbol"),
        "local_symbol": position.get("local_symbol"),
        "con_id": position.get("con_id") or fallback.get("con_id"),
        "expiry": position.get("expiry"),
        "action": action,
        "quantity": _decimal_display(abs(qty)),
        "reason": "Exact scoped remediation for contaminated unauthorized reverse exposure.",
    }


def _identity_fallback(findings: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    for finding in findings:
        event = _mapping(finding.get("settlement_event"))
        if event:
            return {
                "con_id": event.get("con_id"),
                "local_symbol": event.get("local_symbol"),
                "contract_key": event.get("contract_key"),
            }
    return {}


def _broker_positions(inputs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    position_truth = inputs["position_truth"]
    reconciliation = inputs["reconciliation"]
    rows = _list(position_truth.get("broker_positions")) or _list(reconciliation.get("track_b_broker_positions"))
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _lifecycle_positions(inputs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    managed_positions = inputs["managed_position_registry"]
    reconciliation = inputs["reconciliation"]
    rows = _list(managed_positions.get("lifecycle_open_positions")) or _list(reconciliation.get("track_b_lifecycle_positions"))
    for item in _list(managed_positions.get("managed_positions")):
        if not isinstance(item, Mapping):
            continue
        lifecycle_position = item.get("lifecycle_position")
        if isinstance(lifecycle_position, Mapping):
            rows.append(lifecycle_position)
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _open_orders(inputs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    open_order_truth = inputs["open_order_truth"]
    rows = (
        _list(open_order_truth.get("open_orders"))
        or _list(open_order_truth.get("order_states"))
        or _list(open_order_truth.get("rows"))
    )
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _managed_orders(inputs: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in _list(inputs["managed_order_registry"].get("managed_orders")) if isinstance(row, Mapping)]


def _exit_fill_expected_flat_event(reconciliation: Mapping[str, Any]) -> Mapping[str, Any]:
    for blocker in _list(reconciliation.get("blockers")):
        if not isinstance(blocker, Mapping):
            continue
        event = _mapping(_mapping(blocker.get("broker_truth_settlement")).get("event"))
        if str(event.get("event_type") or "") == "EXIT_FILL_EXPECTING_BROKER_FLAT":
            return event
    return {}


def _matching_lifecycle_position(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    for row in lifecycle_positions:
        if _same_contract(left=broker_position, right=row):
            return row
    return None


def _matching_broker_position(
    *,
    lifecycle_position: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    for row in broker_positions:
        if _same_contract(left=lifecycle_position, right=row):
            return row
    return None


def _same_contract(*, left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_con_id = _text(left.get("con_id") or left.get("conId"))
    right_con_id = _text(right.get("con_id") or right.get("conId"))
    if left_con_id and right_con_id and left_con_id == right_con_id:
        return True
    return bool(_text(left.get("local_symbol") or left.get("localSymbol")) == _text(right.get("local_symbol") or right.get("localSymbol")))


def _side_from_lifecycle(row: Mapping[str, Any]) -> str:
    side = str(row.get("side") or "").upper()
    if side in {"LONG", "SHORT"}:
        return side
    return _side_from_quantity(row.get("quantity"))


def _signed_lifecycle_quantity(row: Mapping[str, Any]) -> Decimal:
    aggregate = _decimal(row.get("aggregate_qty") or row.get("signed_lifecycle_qty"))
    if aggregate != Decimal("0"):
        return aggregate
    quantity = _decimal(row.get("quantity"))
    side = _side_from_lifecycle(row)
    if side == "SHORT" and quantity > 0:
        return -quantity
    return quantity


def _side_from_quantity(value: Any) -> str:
    qty = _decimal(value)
    if qty > 0:
        return "LONG"
    if qty < 0:
        return "SHORT"
    return "FLAT"


def _compact_order(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": row.get("account_id"),
        "symbol": row.get("symbol"),
        "local_symbol": row.get("local_symbol") or row.get("contract"),
        "con_id": row.get("con_id") or row.get("conId"),
        "action": row.get("action"),
        "quantity": str(row.get("quantity") or ""),
        "broker_order_id": row.get("broker_order_id"),
        "perm_id": row.get("perm_id"),
        "classification": row.get("classification"),
    }


def _compact_position(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": row.get("account_id"),
        "symbol": row.get("symbol") or row.get("instrument_family"),
        "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
        "con_id": row.get("con_id") or row.get("conId"),
        "expiry": row.get("expiry"),
        "quantity": str(row.get("quantity") or ""),
        "side": row.get("side") or _side_from_quantity(row.get("quantity")),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "lifecycle_id": row.get("lifecycle_id"),
    }


def _operator_explanation(*, hard_classifications: Sequence[str], remediation_plan: Mapping[str, Any]) -> str:
    if not hard_classifications:
        return "Broker position guardian is clean; no duplicate close or reverse exposure is detected."
    if UNAUTHORIZED_REVERSE_EXPOSURE in hard_classifications:
        return (
            "Unauthorized reverse exposure detected; block new entries and lane progression until exact scoped "
            f"{remediation_plan.get('action') or ''} {remediation_plan.get('quantity') or ''} "
            f"{remediation_plan.get('local_symbol') or ''} remediation is operator-authorized."
        ).strip()
    if DUPLICATE_CLOSE_ORDER_BLOCKED in hard_classifications:
        return "Duplicate managed close order evidence detected; block second close submit and preserve broker/order evidence."
    return "Broker/lifecycle position mismatch detected; block new entries and require exact scoped remediation planning."


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_display(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return str(value.normalize())


def _text(value: Any) -> str:
    return str(value or "").strip()


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
