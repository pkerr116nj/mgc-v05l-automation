"""Read-only recovery status reporting for unresolved Track B broker orders."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from .models import BrokerOrderLifecycleStatus, classify_broker_order_lifecycle, to_jsonable
from .preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig


DEFAULT_RECOVERY_OUTPUT_ROOT = Path("outputs/track_b_execution_core/recovery_status")


class RecoveryStatusClassification(str, Enum):
    BLOCKED_UNRESOLVED_ORDER = "RECOVERY_BLOCKED_UNRESOLVED_ORDER"
    READY_CLEAN = "RECOVERY_READY_CLEAN"
    AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "RECOVERY_AMBIGUOUS_MANUAL_REVIEW_REQUIRED"


PreflightRunner = Callable[[ReadOnlyPreflightConfig, str], PreflightResult]


@dataclass(frozen=True)
class RecoveryStatusConfig:
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 77
    account_id: str = "DU1234567"
    contract_key: str = "MGC-202606"
    output_root: Path = DEFAULT_RECOVERY_OUTPUT_ROOT
    broker_order_id: str | None = None
    perm_id: str | None = None
    contract_allowlist: dict[str, dict[str, object]] | None = None

    def to_report_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": self.account_id,
            "contract_key": self.contract_key,
            "output_root": str(self.output_root),
            "broker_order_id": self.broker_order_id,
            "perm_id": self.perm_id,
        }


@dataclass(frozen=True)
class RecoveryStatusResult:
    run_id: str
    classification: RecoveryStatusClassification
    report_json: Path
    report_md: Path
    report: dict[str, object]


def run_recovery_status(
    *,
    config: RecoveryStatusConfig,
    preflight_runner: PreflightRunner,
    run_id: str | None = None,
) -> RecoveryStatusResult:
    actual_run_id = run_id or f"recovery_status_{uuid.uuid4().hex}"
    output_dir = Path(config.output_root) / actual_run_id
    report_json = output_dir / "recovery_status_report.json"
    report_md = output_dir / "recovery_status_report.md"

    config_error = _validate_config(config)
    if config_error is not None:
        return _write_result(
            run_id=actual_run_id,
            config=config,
            report_json=report_json,
            report_md=report_md,
            preflight=None,
            classification=RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            reason=config_error,
            next_required_action="Fix explicit read-only recovery status config.",
        )

    preflight_config = ReadOnlyPreflightConfig(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_key=config.contract_key,
        output_root=Path(config.output_root) / "preflight",
        observe_quote=False,
        contract_allowlist=config.contract_allowlist or ReadOnlyPreflightConfig().contract_allowlist,
    )
    preflight = preflight_runner(preflight_config, f"{actual_run_id}_preflight")
    classification, reason, action = _classify_recovery(config=config, preflight=preflight)
    return _write_result(
        run_id=actual_run_id,
        config=config,
        report_json=report_json,
        report_md=report_md,
        preflight=preflight,
        classification=classification,
        reason=reason,
        next_required_action=action,
    )


def _validate_config(config: RecoveryStatusConfig) -> str | None:
    if str(config.mode).upper() != "PAPER":
        return "mode must be PAPER"
    if config.host != "127.0.0.1":
        return "host must be 127.0.0.1"
    if int(config.port) != 7497:
        return "port must be 7497 for TWS paper"
    if int(config.client_id) <= 0:
        return "client_id must be explicit and positive"
    if not str(config.account_id or "").strip():
        return "account_id is required"
    if not str(config.contract_key or "").strip():
        return "contract_key is required"
    return None


def _classify_recovery(
    *,
    config: RecoveryStatusConfig,
    preflight: PreflightResult,
) -> tuple[RecoveryStatusClassification, str | None, str | None]:
    report = preflight.report
    position_quantity = _position_quantity(report.get("position"))
    open_orders = tuple(row for row in report.get("open_orders", []) if isinstance(row, Mapping))
    matching_order = _matching_order(config=config, report=report, open_orders=open_orders)
    lifecycle = _lifecycle_from_order_or_report(report, matching_order)

    if position_quantity != Decimal("0") and open_orders:
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            "conflicting broker position and open-order state",
            "Manual broker review required before any Track B submit.",
        )
    if matching_order is not None and _filled_quantity(matching_order) > Decimal("0"):
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            "unexpected fill or partial fill after unresolved proof run",
            "Manual review required. Do not submit until broker/ledger state is explicitly reconciled.",
        )
    if lifecycle in {BrokerOrderLifecycleStatus.FILLED, BrokerOrderLifecycleStatus.REJECTED}:
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            "prior proof order reached terminal broker state without a completed Track B proof",
            "Manual review required. Do not submit until broker/ledger state is explicitly reconciled.",
        )
    if lifecycle in {
        BrokerOrderLifecycleStatus.PENDING_CANCEL,
        BrokerOrderLifecycleStatus.HELD_OR_PRESUBMITTED,
        BrokerOrderLifecycleStatus.AMBIGUOUS,
        BrokerOrderLifecycleStatus.MANUAL_REVIEW_REQUIRED,
    }:
        return (
            RecoveryStatusClassification.BLOCKED_UNRESOLVED_ORDER,
            "unresolved broker order blocks same account/contract submit",
            "Wait for terminal broker order state, then rerun read-only recovery status.",
        )
    if position_quantity != Decimal("0"):
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            "non-flat broker position after unresolved proof run",
            "Manual broker review required before any Track B submit.",
        )
    if open_orders:
        return (
            RecoveryStatusClassification.BLOCKED_UNRESOLVED_ORDER,
            "open broker order blocks same account/contract submit",
            "Wait for terminal broker order state, then rerun read-only recovery status.",
        )
    if preflight.classification == PreflightClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED:
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            str(report.get("failure_or_ambiguity") or "read-only preflight was ambiguous"),
            "Manual TWS/API review required before any Track B submit.",
        )
    if preflight.classification == PreflightClassification.BLOCKED and report.get("unresolved_broker_order_detected"):
        return (
            RecoveryStatusClassification.BLOCKED_UNRESOLVED_ORDER,
            "unresolved broker order blocks same account/contract submit",
            "Wait for terminal broker order state, then rerun read-only recovery status.",
        )
    if preflight.classification == PreflightClassification.BLOCKED and not _preflight_block_is_only_terminal_clean(report):
        return (
            RecoveryStatusClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
            str(report.get("failure_or_ambiguity") or "read-only preflight was blocked"),
            "Resolve read-only preflight blocker before any Track B submit.",
        )
    return (
        RecoveryStatusClassification.READY_CLEAN,
        None,
        "Broker state is clean for this account/contract. Operator may start a new explicitly confirmed proof run if desired.",
    )


def _matching_order(
    *,
    config: RecoveryStatusConfig,
    report: Mapping[str, object],
    open_orders: tuple[Mapping[str, object], ...],
) -> Mapping[str, object] | None:
    for row in open_orders:
        if config.broker_order_id and str(row.get("broker_order_id") or "") == config.broker_order_id:
            return row
        if config.perm_id and str(row.get("perm_id") or "") == config.perm_id:
            return row
    if report.get("unresolved_broker_order_detected"):
        return {
            "broker_order_id": report.get("unresolved_broker_order_id"),
            "perm_id": report.get("unresolved_broker_perm_id"),
            "status": report.get("unresolved_broker_order_status"),
            "remaining_quantity": report.get("unresolved_remaining_quantity") or 0,
        }
    if len(open_orders) == 1:
        return open_orders[0]
    return None


def _lifecycle_from_order_or_report(report: Mapping[str, object], order: Mapping[str, object] | None) -> BrokerOrderLifecycleStatus | None:
    if order is None:
        return None
    raw_status = str(order.get("status") or report.get("unresolved_broker_order_status") or "")
    remaining = order.get("remaining_quantity") or report.get("unresolved_remaining_quantity") or 0
    if raw_status in BrokerOrderLifecycleStatus.__members__:
        return BrokerOrderLifecycleStatus(raw_status)
    return classify_broker_order_lifecycle(raw_status, remaining)


def _remaining_from_order_or_report(report: Mapping[str, object], order: Mapping[str, object] | None) -> str | None:
    if order is not None and order.get("remaining_quantity") is not None:
        return str(order["remaining_quantity"])
    if report.get("unresolved_remaining_quantity") is not None:
        return str(report["unresolved_remaining_quantity"])
    return None


def _position_quantity(raw_position: object) -> Decimal:
    if not isinstance(raw_position, Mapping):
        return Decimal("0")
    try:
        return Decimal(str(raw_position.get("signed_quantity") or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _filled_quantity(order: Mapping[str, object]) -> Decimal:
    try:
        return Decimal(str(order.get("filled_quantity") or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _preflight_block_is_only_terminal_clean(report: Mapping[str, object]) -> bool:
    return not report.get("open_orders") and _position_quantity(report.get("position")) == Decimal("0")


def _write_result(
    *,
    run_id: str,
    config: RecoveryStatusConfig,
    report_json: Path,
    report_md: Path,
    preflight: PreflightResult | None,
    classification: RecoveryStatusClassification,
    reason: str | None,
    next_required_action: str | None,
) -> RecoveryStatusResult:
    preflight_report = preflight.report if preflight is not None else {}
    open_orders = tuple(row for row in preflight_report.get("open_orders", []) if isinstance(row, Mapping))
    matching_order = _matching_order(config=config, report=preflight_report, open_orders=open_orders)
    lifecycle = _lifecycle_from_order_or_report(preflight_report, matching_order)
    report: dict[str, object] = {
        "schema_version": "track_b_recovery_status_v1",
        "classification": classification.value,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config.to_report_dict(),
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "position_quantity": str(_position_quantity(preflight_report.get("position"))),
        "proof_contract_open_orders": list(open_orders),
        "matching_broker_order_id": _value_from_order_or_report(preflight_report, matching_order, "broker_order_id", "unresolved_broker_order_id"),
        "matching_perm_id": _value_from_order_or_report(preflight_report, matching_order, "perm_id", "unresolved_broker_perm_id"),
        "order_status": matching_order.get("status") if matching_order is not None else preflight_report.get("unresolved_broker_order_status"),
        "remaining_quantity": _remaining_from_order_or_report(preflight_report, matching_order),
        "lifecycle_classification": lifecycle.value if lifecycle is not None else None,
        "blocks_same_account_contract_submit": classification != RecoveryStatusClassification.READY_CLEAN,
        "next_required_action": next_required_action,
        "failure_or_ambiguity": reason,
        "preflight_classification": preflight.classification.value if preflight is not None else None,
        "preflight_report_json": str(preflight.report_json) if preflight is not None else None,
        "preflight_report_md": str(preflight.report_md) if preflight is not None else None,
        "report_json_path": str(report_json),
        "report_markdown_path": str(report_md),
        "submit_enabled": False,
        "place_order_called": False,
        "cancel_called": False,
        "transmit_called": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    report_md.write_text(_render_markdown(report), encoding="utf-8")
    return RecoveryStatusResult(run_id=run_id, classification=classification, report_json=report_json, report_md=report_md, report=report)


def _value_from_order_or_report(
    report: Mapping[str, object],
    order: Mapping[str, object] | None,
    order_key: str,
    report_key: str,
) -> object | None:
    if order is not None and order.get(order_key) is not None:
        return order[order_key]
    return report.get(report_key)


def _render_markdown(report: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Track B Read-Only Recovery Status",
            "",
            "## Classification",
            str(report.get("classification")),
            "",
            "## Broker State",
            json.dumps(
                {
                    "account_id": report.get("account_id"),
                    "contract_key": report.get("contract_key"),
                    "position_quantity": report.get("position_quantity"),
                    "matching_broker_order_id": report.get("matching_broker_order_id"),
                    "matching_perm_id": report.get("matching_perm_id"),
                    "order_status": report.get("order_status"),
                    "remaining_quantity": report.get("remaining_quantity"),
                    "lifecycle_classification": report.get("lifecycle_classification"),
                    "blocks_same_account_contract_submit": report.get("blocks_same_account_contract_submit"),
                },
                indent=2,
                sort_keys=True,
            ),
            "",
            "## Required Action",
            str(report.get("next_required_action")),
            "",
        ]
    )
