"""Shared Track B PAPER lifecycle/manifest transition authority.

This module is deliberately pure: it does not read files, write artifacts,
submit orders, or query broker state. Callers pass the evidence they have, and
the authority returns the only managed-position state that evidence supports.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


INTENT_CREATED = "INTENT_CREATED"
SUBMIT_ATTEMPTED = "SUBMIT_ATTEMPTED"
SUBMITTED_PENDING_FILL = "SUBMITTED_PENDING_FILL"
BLOCKED_NO_BROKER_EFFECT = "BLOCKED_NO_BROKER_EFFECT"
BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE = "BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE"
OPEN_MANAGED = "OPEN_MANAGED"
OPEN_MANAGED_METADATA_INCOMPLETE = "OPEN_MANAGED_METADATA_INCOMPLETE"
CLOSED_FLAT = "CLOSED_FLAT"
REVIEW_REQUIRED = "REVIEW_REQUIRED"

BROKER_BACKED_FILL_EVIDENCE_COMPLETE = "BROKER_BACKED_FILL_EVIDENCE_COMPLETE"
TRACK_B_STRATEGY_PAPER_OPEN_MANAGED = "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"


@dataclass(frozen=True)
class TrackBLifecycleTransition:
    """Canonical transition verdict for Track B managed PAPER state."""

    classification: str
    blockers: tuple[str, ...] = ()

    @property
    def open_managed_allowed(self) -> bool:
        return self.classification == OPEN_MANAGED

    @property
    def paper_lifecycle_classification(self) -> str:
        if self.classification == OPEN_MANAGED:
            return TRACK_B_STRATEGY_PAPER_OPEN_MANAGED
        return self.classification

    @property
    def final_position_status(self) -> str:
        if self.classification == OPEN_MANAGED:
            return OPEN_MANAGED
        if self.classification == CLOSED_FLAT:
            return CLOSED_FLAT
        return REVIEW_REQUIRED

    @property
    def review_required(self) -> bool:
        return self.final_position_status == REVIEW_REQUIRED


def classify_managed_position_transition(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    """Classify the lifecycle/manifest state allowed by the supplied evidence."""

    if normalize_no_broker_effect_result(evidence).classification == BLOCKED_NO_BROKER_EFFECT:
        return TrackBLifecycleTransition(BLOCKED_NO_BROKER_EFFECT)

    requested = _text(
        evidence.get("requested_lifecycle_status"),
        evidence.get("lifecycle_status"),
        evidence.get("target_lifecycle_status"),
    )
    if requested == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE:
        return TrackBLifecycleTransition(requested, blockers=broker_backed_fill_evidence_blockers(evidence))
    if requested == OPEN_MANAGED_METADATA_INCOMPLETE:
        return TrackBLifecycleTransition(requested, blockers=open_managed_metadata_blockers(evidence))
    if requested in {BLOCKED_NO_BROKER_EFFECT, CLOSED_FLAT, REVIEW_REQUIRED}:
        return TrackBLifecycleTransition(requested)
    if requested in {INTENT_CREATED, SUBMIT_ATTEMPTED, SUBMITTED_PENDING_FILL}:
        return TrackBLifecycleTransition(requested)

    metadata_blockers = tuple(evidence.get("management_metadata_blockers") or ())
    metadata_complete = evidence.get("management_metadata_complete")
    if metadata_complete is None:
        metadata_blockers = open_managed_metadata_blockers(evidence)
        metadata_complete = not metadata_blockers
    if not bool(metadata_complete):
        return TrackBLifecycleTransition(
            OPEN_MANAGED_METADATA_INCOMPLETE,
            blockers=metadata_blockers or ("managed_exit_policy_id",),
        )

    fill_blockers = tuple(broker_backed_fill_evidence_blockers(evidence))
    if fill_blockers:
        return TrackBLifecycleTransition(BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE, blockers=fill_blockers)

    if requested in {None, "", OPEN_MANAGED, TRACK_B_STRATEGY_PAPER_OPEN_MANAGED}:
        return TrackBLifecycleTransition(OPEN_MANAGED)
    return TrackBLifecycleTransition(str(requested), blockers=())


def validate_open_managed_evidence(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    """Validate whether evidence is sufficient to create OPEN_MANAGED state."""

    normalized = dict(evidence)
    normalized["requested_lifecycle_status"] = OPEN_MANAGED
    return classify_managed_position_transition(normalized)


def broker_backed_fill_evidence_blockers(evidence: Mapping[str, Any]) -> tuple[str, ...]:
    required = {
        "broker_order_id_or_perm_id": _text(
            evidence.get("broker_order_id"),
            evidence.get("order_id"),
            evidence.get("perm_id"),
        ),
        "fill_price": _text(evidence.get("fill_price"), evidence.get("entry_fill_price"), evidence.get("price")),
        "fill_timestamp": _text(
            evidence.get("fill_timestamp"),
            evidence.get("entry_timestamp"),
            evidence.get("filled_at"),
            evidence.get("executed_at"),
        ),
    }
    return tuple(key for key, value in required.items() if not value)


def open_managed_metadata_blockers(evidence: Mapping[str, Any]) -> tuple[str, ...]:
    contract = evidence.get("contract") if isinstance(evidence.get("contract"), Mapping) else {}
    required = {
        "entry_intent_id": _text(
            evidence.get("entry_intent_id"),
            evidence.get("order_intent_id"),
            evidence.get("ownership_id"),
        ),
        "lane_id": _text(evidence.get("lane_id")),
        "strategy_id": _text(evidence.get("strategy_id")),
        "contract": _text(
            evidence.get("contract_key"),
            evidence.get("local_symbol"),
            evidence.get("con_id"),
            contract.get("contract_key"),
            contract.get("local_symbol"),
            contract.get("qualified_contract_identifier"),
            contract.get("con_id"),
        ),
        "side": _text(evidence.get("side"), evidence.get("action"), evidence.get("intent_type")),
        "quantity": _text(evidence.get("quantity")),
        "managed_exit_policy_id": _text(evidence.get("managed_exit_policy_id")),
        "lifecycle_id_or_destination": _text(evidence.get("lifecycle_id"), evidence.get("lifecycle_destination")),
    }
    return tuple(key for key, value in required.items() if not value)


def broker_backed_fill_evidence_complete(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    blockers = broker_backed_fill_evidence_blockers(evidence)
    if blockers:
        return TrackBLifecycleTransition(BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE, blockers=blockers)
    return TrackBLifecycleTransition(BROKER_BACKED_FILL_EVIDENCE_COMPLETE)


def normalize_no_broker_effect_result(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    if is_pre_submit_no_broker_effect(evidence):
        return TrackBLifecycleTransition(BLOCKED_NO_BROKER_EFFECT)
    return TrackBLifecycleTransition(str(_text(evidence.get("lifecycle_status")) or REVIEW_REQUIRED))


def is_pre_submit_no_broker_effect(evidence: Mapping[str, Any]) -> bool:
    effect = str(evidence.get("broker_effect_classification") or evidence.get("broker_effect") or "").strip().upper()
    classification = str(evidence.get("classification") or "").strip().upper()
    if "NO_BROKER_EFFECT" in effect or "NO_BROKER_EFFECT" in classification:
        return True
    submit_sent = evidence.get("submit_sent")
    if (
        submit_sent is False
        and not _text(evidence.get("broker_order_id"), evidence.get("order_id"))
        and not _text(evidence.get("perm_id"))
    ):
        return True
    return False


def ledger_projection_from_transition(
    *,
    transition: TrackBLifecycleTransition,
    broker_bridge_review_required: bool = False,
) -> dict[str, Any]:
    return {
        "paper_lifecycle_classification": transition.paper_lifecycle_classification,
        "final_broker_state_classification": transition.paper_lifecycle_classification,
        "final_position_status": transition.final_position_status,
        "review_required": bool(broker_bridge_review_required) or transition.review_required,
    }


def _text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None
