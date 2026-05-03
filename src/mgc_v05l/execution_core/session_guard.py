"""Session/timing guard for Track B paper-proof submits."""

from __future__ import annotations

from dataclasses import dataclass


class ProofTimingClassification(str):
    ALLOWED = "PROOF_TIMING_ALLOWED"
    BLOCKED_OUTSIDE_ACTIVE_SESSION = "PROOF_TIMING_BLOCKED_OUTSIDE_ACTIVE_SESSION"
    UNKNOWN_BLOCKED = "PROOF_TIMING_UNKNOWN_BLOCKED"


@dataclass(frozen=True)
class ProofTimingDecision:
    classification: str
    source: str
    reason: str
    required_action: str

    @property
    def allowed(self) -> bool:
        return self.classification == ProofTimingClassification.ALLOWED

    def to_report_dict(self) -> dict[str, object]:
        return {
            "proof_timing_classification": self.classification,
            "proof_timing_source": self.source,
            "proof_timing_reason": self.reason,
            "proof_timing_required_action": self.required_action,
            "proof_timing_allowed": self.allowed,
        }


def evaluate_proof_timing(*, status: str | None, source: str | None = None, detail: str | None = None) -> ProofTimingDecision:
    normalized = str(status or "").strip().upper()
    actual_source = str(source or "operator_config").strip() or "operator_config"
    if normalized in {"ACTIVE", "ACTIVE_SESSION", "PROOF_TIMING_ALLOWED", "ALLOWED"}:
        return ProofTimingDecision(
            classification=ProofTimingClassification.ALLOWED,
            source=actual_source,
            reason=str(detail or "Proof timing is marked as active-session."),
            required_action="Continue to preflight and downstream submit gates.",
        )
    if normalized in {"OUTSIDE", "OUTSIDE_ACTIVE_SESSION", "CLOSED", "PROOF_TIMING_BLOCKED_OUTSIDE_ACTIVE_SESSION"}:
        return ProofTimingDecision(
            classification=ProofTimingClassification.BLOCKED_OUTSIDE_ACTIVE_SESSION,
            source=actual_source,
            reason=str(
                detail
                or "Proof timing is outside an active session; submitting now may create held or PreSubmitted outside-session behavior."
            ),
            required_action="Wait for an active exchange session, then rerun read-only preflight before any proof submit.",
        )
    return ProofTimingDecision(
        classification=ProofTimingClassification.UNKNOWN_BLOCKED,
        source=actual_source,
        reason=str(
            detail
            or "Proof timing is unknown; Track B blocks submit rather than risk held or PreSubmitted outside-session behavior."
        ),
        required_action="Provide active-session timing evidence or implement a reviewed session calendar guard before paper proof submit.",
    )
