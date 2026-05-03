"""Operator-facing readiness vocabulary for Track B reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FinalReadinessVerdict(str, Enum):
    READY_FOR_PAPER_PROOF = "READY_FOR_PAPER_PROOF"
    BLOCKED_UNRESOLVED_BROKER_ORDER = "BLOCKED_UNRESOLVED_BROKER_ORDER"
    BLOCKED_NON_FLAT_POSITION = "BLOCKED_NON_FLAT_POSITION"
    BLOCKED_OUTSIDE_ACTIVE_SESSION = "BLOCKED_OUTSIDE_ACTIVE_SESSION"
    BLOCKED_UNKNOWN_PROOF_TIMING = "BLOCKED_UNKNOWN_PROOF_TIMING"
    BLOCKED_CONTRACT_OR_ACCOUNT_MISMATCH = "BLOCKED_CONTRACT_OR_ACCOUNT_MISMATCH"
    BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE = "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE"
    AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"


@dataclass(frozen=True)
class OperatorReadiness:
    final_readiness_verdict: FinalReadinessVerdict
    submit_allowed: bool
    submit_attempted: bool
    primary_blocker: str | None
    required_next_action: str | None
    account_id: str | None
    contract_key: str | None
    broker_order_id: object | None = None
    perm_id: object | None = None
    broker_status: object | None = None
    position_qty: object | None = None
    proof_timing_classification: object | None = None
    proof_timing_allowed: object | None = None
    secondary_blockers: tuple[str, ...] = field(default_factory=tuple)

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "final_readiness_verdict": self.final_readiness_verdict.value,
            "submit_allowed": self.submit_allowed,
            "submit_attempted": self.submit_attempted,
            "primary_blocker": self.primary_blocker,
            "secondary_blockers": list(self.secondary_blockers),
            "required_next_action": self.required_next_action,
            "account_id": self.account_id,
            "contract_key": self.contract_key,
            "broker_order_id": self.broker_order_id,
            "perm_id": self.perm_id,
            "broker_status": self.broker_status,
            "position_qty": self.position_qty,
            "proof_timing_classification": self.proof_timing_classification,
            "proof_timing_allowed": self.proof_timing_allowed,
        }


def ready_for_paper_proof(
    *,
    account_id: str | None,
    contract_key: str | None,
    position_qty: object | None = None,
    submit_attempted: bool = False,
    required_next_action: str | None = "Operator may run an explicitly confirmed paper proof during an active session.",
    proof_timing_classification: object | None = None,
    proof_timing_allowed: object | None = None,
) -> OperatorReadiness:
    return OperatorReadiness(
        final_readiness_verdict=FinalReadinessVerdict.READY_FOR_PAPER_PROOF,
        submit_allowed=True,
        submit_attempted=submit_attempted,
        primary_blocker=None,
        required_next_action=required_next_action,
        account_id=account_id,
        contract_key=contract_key,
        position_qty=position_qty,
        proof_timing_classification=proof_timing_classification,
        proof_timing_allowed=proof_timing_allowed,
    )


def blocked_readiness(
    *,
    verdict: FinalReadinessVerdict,
    primary_blocker: str,
    required_next_action: str,
    account_id: str | None,
    contract_key: str | None,
    submit_attempted: bool = False,
    broker_order_id: object | None = None,
    perm_id: object | None = None,
    broker_status: object | None = None,
    position_qty: object | None = None,
    proof_timing_classification: object | None = None,
    proof_timing_allowed: object | None = None,
    secondary_blockers: tuple[str, ...] = (),
) -> OperatorReadiness:
    return OperatorReadiness(
        final_readiness_verdict=verdict,
        submit_allowed=False,
        submit_attempted=submit_attempted,
        primary_blocker=primary_blocker,
        secondary_blockers=secondary_blockers,
        required_next_action=required_next_action,
        account_id=account_id,
        contract_key=contract_key,
        broker_order_id=broker_order_id,
        perm_id=perm_id,
        broker_status=broker_status,
        position_qty=position_qty,
        proof_timing_classification=proof_timing_classification,
        proof_timing_allowed=proof_timing_allowed,
    )
