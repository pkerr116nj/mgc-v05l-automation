"""Restart precheck authority for the guarded Track B PAPER stack."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any, Mapping


RESTART_ALLOWED_FLAT_RECONCILED = "RESTART_ALLOWED_FLAT_RECONCILED"
RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE = "RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE"
BLOCKED_UNMANAGED_EXPOSURE = "BLOCKED_UNMANAGED_EXPOSURE"
BLOCKED_OPEN_ORDERS = "BLOCKED_OPEN_ORDERS"
BLOCKED_DUPLICATE_WRITER = "BLOCKED_DUPLICATE_WRITER"
BLOCKED_RECOVERY_INACTIVE = "BLOCKED_RECOVERY_INACTIVE"
BLOCKED_LIVE_MONEY_OR_PAPER_PROOF = "BLOCKED_LIVE_MONEY_OR_PAPER_PROOF"
BLOCKED_REVIEW_OVERLAY_ACTIVE = "BLOCKED_REVIEW_OVERLAY_ACTIVE"
BLOCKED_STALE_BROKER_TRUTH = "BLOCKED_STALE_BROKER_TRUTH"


@dataclass(frozen=True)
class PaperStackRestartPrecheck:
    classification: str
    restart_allowed: bool
    detail: str
    reason_codes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "restart_allowed": self.restart_allowed,
            "detail": self.detail,
            "reason_codes": list(self.reason_codes),
        }


def classify_paper_stack_restart_precheck(status: Mapping[str, Any]) -> PaperStackRestartPrecheck:
    """Classify whether an already-running PAPER stack may be restarted.

    This mirrors the runtime watchdog restart authority: flat/reconciled state
    may restart, and exact registry-backed managed exposure may restart when
    the pre-restart resolver proves ownership. It does not infer ownership in
    the shell script itself.
    """

    safety = _mapping(status.get("safety"))
    duplicate = _mapping(status.get("duplicate_writer"))
    recovery = _mapping(status.get("recovery"))
    broker = _mapping(status.get("broker_lifecycle"))
    config = _mapping(status.get("config"))
    diagnostics = _mapping(status.get("registry_truth_diagnostics"))
    runtime_env = _mapping(status.get("live_runtime_environment"))
    restart_policy = _mapping(runtime_env.get("restart_policy"))

    if safety.get("paper_only") is not True or safety.get("live_money_eligible") is not False or safety.get("paper_proof_invoked") is not False:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_LIVE_MONEY_OR_PAPER_PROOF,
            restart_allowed=False,
            detail="PAPER restart requires paper-only state with no live-money or paper_proof authority.",
            reason_codes=("LIVE_MONEY_OR_PAPER_PROOF_RISK",),
        )
    if duplicate.get("duplicate_writer_detected") is True:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_DUPLICATE_WRITER,
            restart_allowed=False,
            detail="Duplicate PAPER runtime writer is present.",
            reason_codes=("DUPLICATE_WRITER_DETECTED",),
        )
    if config.get("review_overlay_active") is True:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_REVIEW_OVERLAY_ACTIVE,
            restart_allowed=False,
            detail="Forbidden review overlay is active.",
            reason_codes=("REVIEW_OVERLAY_ACTIVE",),
        )
    if _recovery_active(recovery) is not True:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_RECOVERY_INACTIVE,
            restart_allowed=False,
            detail="Standalone PAPER recovery must be active before restart.",
            reason_codes=("RECOVERY_INACTIVE",),
        )
    if _broker_open_order_count(status) > 0:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_OPEN_ORDERS,
            restart_allowed=False,
            detail="Broker open orders are present; restart must not proceed.",
            reason_codes=("BROKER_OPEN_ORDERS_PRESENT",),
        )
    if broker.get("broker_truth_fresh") is not True:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_STALE_BROKER_TRUTH,
            restart_allowed=False,
            detail="Broker truth is not fresh enough for restart.",
            reason_codes=("BROKER_TRUTH_NOT_FRESH",),
        )

    if _broker_lifecycle_reconciled(broker):
        return PaperStackRestartPrecheck(
            classification=RESTART_ALLOWED_FLAT_RECONCILED,
            restart_allowed=True,
            detail="Broker/lifecycle state is reconciled and PAPER safety gates are clean.",
        )

    if restart_policy.get("owned_exposure_restart_allowed") is True:
        resolution = str(restart_policy.get("pre_restart_exposure_resolution_classification") or "")
        if resolution in {
            "MANAGED_EXPOSURE_RESOLVED",
            "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED",
            "ADOPTABLE_BROKER_BACKED_EXPOSURE",
        }:
            return PaperStackRestartPrecheck(
                classification=RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE,
                restart_allowed=True,
                detail="Pre-restart resolver proved exact owned managed exposure; restart may reload guarded maintenance.",
                reason_codes=(resolution,),
            )

    current_positions = int(diagnostics.get("track_b_managed_futures_position_count") or 0)
    lifecycle_positions = _current_scope_lifecycle_position_count(diagnostics)
    reason_codes = tuple(str(code) for code in restart_policy.get("reason_codes") or ())
    if current_positions or lifecycle_positions:
        return PaperStackRestartPrecheck(
            classification=BLOCKED_UNMANAGED_EXPOSURE,
            restart_allowed=False,
            detail="Current Track B exposure exists but exact owned managed exposure restart authority was not proven.",
            reason_codes=reason_codes or ("OWNED_EXPOSURE_RESTART_NOT_PROVEN",),
        )

    return PaperStackRestartPrecheck(
        classification=BLOCKED_UNMANAGED_EXPOSURE,
        restart_allowed=False,
        detail="Broker/lifecycle state is not reconciled and no owned managed exposure restart authority is present.",
        reason_codes=reason_codes or ("BROKER_LIFECYCLE_NOT_RECONCILED",),
    )


def _broker_lifecycle_reconciled(broker: Mapping[str, Any]) -> bool:
    return (
        "RECONCILED" in str(broker.get("reconciliation_classification") or "")
        and broker.get("broker_truth_fresh") is True
    )


def _broker_open_order_count(status: Mapping[str, Any]) -> int:
    diagnostics = _mapping(status.get("registry_truth_diagnostics"))
    if diagnostics.get("broker_open_order_count") not in {None, ""}:
        return int(diagnostics.get("broker_open_order_count") or 0)
    return int(_mapping(status.get("broker_lifecycle")).get("track_b_broker_open_order_count") or 0)


def _current_scope_lifecycle_position_count(diagnostics: Mapping[str, Any]) -> int:
    for key in ("current_scope_lifecycle_open_position_count", "current_scope_lifecycle_position_count"):
        if diagnostics.get(key) not in {None, ""}:
            return int(diagnostics.get(key) or 0)
    current_lifecycle = _mapping(diagnostics.get("current_lifecycle_truth"))
    for key in ("current_scope_lifecycle_open_position_count", "current_scope_lifecycle_position_count"):
        if current_lifecycle.get(key) not in {None, ""}:
            return int(current_lifecycle.get(key) or 0)
    return int(diagnostics.get("lifecycle_open_position_count") or 0)


def _recovery_active(recovery: Mapping[str, Any]) -> bool:
    return (
        recovery.get("classification") == "RECOVERY_ACTIVE"
        and recovery.get("recovery_authoritative") is True
        and recovery.get("standalone_recovery_classification") == "RECOVERY_ACTIVE"
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def main() -> int:
    payload = json.loads(sys.stdin.read())
    print(json.dumps(classify_paper_stack_restart_precheck(payload).to_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
