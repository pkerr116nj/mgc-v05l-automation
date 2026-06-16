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
CLOSE_UNKNOWN = "CLOSE_UNKNOWN"
MANUAL_OR_MALFORMED_CLEANUP = "MANUAL_OR_MALFORMED_CLEANUP"

BROKER_BACKED_FILL_EVIDENCE_COMPLETE = "BROKER_BACKED_FILL_EVIDENCE_COMPLETE"
TRACK_B_STRATEGY_PAPER_OPEN_MANAGED = "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
TRACK_B_STRATEGY_PAPER_CLOSED_FLAT = "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"

_MANUAL_OR_MALFORMED_STATES = {
    "MANUALLY_FLATTENED_REVIEWED",
    "APP_ONLY_UNFILLED_REVIEWED",
    "IBKR_CONTRACT_REJECTED_REVIEWED",
    "LEAK_TEST_ADOPTED_ENTRY_SETTLED_FLAT_REVIEWED",
    "VOID_MALFORMED_STALE_ARTIFACT",
    "MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT",
    "OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED",
}


@dataclass(frozen=True)
class TrackBLifecycleTransition:
    """Canonical transition verdict for Track B managed PAPER state."""

    classification: str
    blockers: tuple[str, ...] = ()
    terminal: bool = False
    broker_backed: bool = False
    no_broker_effect: bool = False
    managed_position_registry_allowed: bool = False
    reconciliation_clean_eligible: bool = False
    operator_action_required: bool = False
    clean_trade_stats_allowed: bool = False

    @property
    def open_managed_allowed(self) -> bool:
        return self.classification == OPEN_MANAGED

    @property
    def paper_lifecycle_classification(self) -> str:
        if self.classification == OPEN_MANAGED:
            return TRACK_B_STRATEGY_PAPER_OPEN_MANAGED
        if self.classification == CLOSED_FLAT:
            return TRACK_B_STRATEGY_PAPER_CLOSED_FLAT
        return self.classification

    @property
    def final_position_status(self) -> str:
        if self.classification == OPEN_MANAGED:
            return OPEN_MANAGED
        if self.classification == CLOSED_FLAT:
            return CLOSED_FLAT
        if self.classification == BLOCKED_NO_BROKER_EFFECT:
            return BLOCKED_NO_BROKER_EFFECT
        return REVIEW_REQUIRED

    @property
    def review_required(self) -> bool:
        return self.operator_action_required or self.final_position_status == REVIEW_REQUIRED


@dataclass(frozen=True)
class TrackBLifecycleStateRule:
    state: str
    allowed_from: tuple[str, ...]
    required_evidence: tuple[str, ...]
    terminal: bool
    broker_backed: bool
    no_broker_effect: bool
    managed_position_registry_allowed: bool
    reconciliation_clean_eligible: bool
    operator_action_required: bool
    clean_trade_stats_allowed: bool


@dataclass(frozen=True)
class TrackBLifecycleTransitionCheck:
    current_state: str
    target_state: str
    transition: TrackBLifecycleTransition
    allowed: bool
    blockers: tuple[str, ...] = ()


def classify_managed_position_transition(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    """Classify the lifecycle/manifest state allowed by the supplied evidence."""

    if normalize_no_broker_effect_result(evidence).classification == BLOCKED_NO_BROKER_EFFECT:
        return _transition(BLOCKED_NO_BROKER_EFFECT)

    requested = _text(
        evidence.get("requested_lifecycle_status"),
        evidence.get("lifecycle_status"),
        evidence.get("target_lifecycle_status"),
    )
    if requested == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE:
        return _transition(requested, blockers=broker_backed_fill_evidence_blockers(evidence))
    if requested == OPEN_MANAGED_METADATA_INCOMPLETE:
        return _transition(requested, blockers=open_managed_metadata_blockers(evidence))
    if requested == BLOCKED_NO_BROKER_EFFECT:
        return _transition(requested)
    if requested == CLOSED_FLAT:
        closed_blockers = closed_flat_evidence_blockers(evidence)
        if closed_blockers:
            return _transition(REVIEW_REQUIRED, blockers=closed_blockers)
        return _transition(CLOSED_FLAT)
    if requested == REVIEW_REQUIRED:
        return _transition(requested, blockers=tuple(evidence.get("review_blockers") or ()))
    if requested == CLOSE_UNKNOWN:
        return _transition(REVIEW_REQUIRED, blockers=("close_state_unknown",))
    if requested == MANUAL_OR_MALFORMED_CLEANUP or requested in _MANUAL_OR_MALFORMED_STATES:
        manual_evidence = _text(
            evidence.get("operator_review_or_malformed_artifact_evidence"),
            evidence.get("manual_reconciliation_reviewed"),
            evidence.get("manual_reconciliation_evidence"),
            evidence.get("malformed_artifact_evidence"),
            evidence.get("broker_backed_malformed_artifact_reviewed"),
        )
        if not manual_evidence:
            return _transition(
                MANUAL_OR_MALFORMED_CLEANUP,
                blockers=("operator_review_or_malformed_artifact_evidence",),
            )
        return _transition(MANUAL_OR_MALFORMED_CLEANUP)
    if requested in {INTENT_CREATED, SUBMIT_ATTEMPTED, SUBMITTED_PENDING_FILL}:
        return _transition(requested)

    metadata_blockers = tuple(evidence.get("management_metadata_blockers") or ())
    metadata_complete = evidence.get("management_metadata_complete")
    if metadata_complete is None:
        metadata_blockers = open_managed_metadata_blockers(evidence)
        metadata_complete = not metadata_blockers
    if not bool(metadata_complete):
        return _transition(
            OPEN_MANAGED_METADATA_INCOMPLETE,
            blockers=metadata_blockers or ("managed_exit_policy_id",),
        )

    fill_blockers = tuple(broker_backed_fill_evidence_blockers(evidence))
    if fill_blockers:
        return _transition(BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE, blockers=fill_blockers)

    if requested in {None, "", OPEN_MANAGED, TRACK_B_STRATEGY_PAPER_OPEN_MANAGED}:
        return _transition(OPEN_MANAGED)
    return _transition(str(requested), blockers=())


def validate_open_managed_evidence(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    """Validate whether evidence is sufficient to create OPEN_MANAGED state."""

    normalized = dict(evidence)
    normalized["requested_lifecycle_status"] = OPEN_MANAGED
    return classify_managed_position_transition(normalized)


def broker_backed_fill_evidence_blockers(evidence: Mapping[str, Any]) -> tuple[str, ...]:
    broker_effect_observation_id = None
    effect = str(evidence.get("broker_effect_classification") or "").strip().upper()
    broker_identity = evidence.get("broker_ownership_identity") if isinstance(evidence.get("broker_ownership_identity"), Mapping) else {}
    if effect == "BROKER_EFFECT_OBSERVED_AFTER_REJECTION":
        broker_effect_observation_id = _text(
            evidence.get("broker_effect_observation_id"),
            broker_identity.get("broker_effect_observation_id"),
        )
    required = {
        "broker_order_id_or_perm_id": _text(
            evidence.get("broker_order_id"),
            evidence.get("order_id"),
            evidence.get("perm_id"),
            broker_effect_observation_id,
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


def closed_flat_evidence_blockers(evidence: Mapping[str, Any]) -> tuple[str, ...]:
    if close_fill_evidence_present(evidence) or broker_flat_proof_present(evidence):
        return ()
    return ("close_fill_or_broker_flat_proof",)


def close_fill_evidence_present(evidence: Mapping[str, Any]) -> bool:
    close_fill = evidence.get("close_fill") if isinstance(evidence.get("close_fill"), Mapping) else {}
    exit_fill = evidence.get("exit_fill") if isinstance(evidence.get("exit_fill"), Mapping) else {}
    price = _text(
        evidence.get("close_fill_price"),
        evidence.get("exit_fill_price"),
        evidence.get("fill_price") if _is_close_intent(evidence) else None,
        close_fill.get("price"),
        close_fill.get("fill_price"),
        exit_fill.get("price"),
        exit_fill.get("fill_price"),
    )
    timestamp = _text(
        evidence.get("close_fill_timestamp"),
        evidence.get("exit_fill_timestamp"),
        evidence.get("exit_timestamp"),
        evidence.get("fill_timestamp") if _is_close_intent(evidence) else None,
        close_fill.get("filled_at"),
        close_fill.get("fill_timestamp"),
        exit_fill.get("filled_at"),
        exit_fill.get("fill_timestamp"),
    )
    order_identity = _text(
        evidence.get("close_broker_order_id"),
        evidence.get("exit_order_id"),
        evidence.get("broker_order_id") if _is_close_intent(evidence) else None,
        evidence.get("close_perm_id"),
        evidence.get("exit_perm_id"),
        evidence.get("perm_id") if _is_close_intent(evidence) else None,
        close_fill.get("broker_order_id"),
        close_fill.get("perm_id"),
        exit_fill.get("broker_order_id"),
        exit_fill.get("perm_id"),
    )
    return bool(price and timestamp and order_identity)


def broker_flat_proof_present(evidence: Mapping[str, Any]) -> bool:
    if evidence.get("broker_flat_proof") is True:
        return True
    if evidence.get("broker_flat_confirmed") is True:
        return True
    if evidence.get("broker_position_flat") is True and evidence.get("open_order_count") in {0, "0", None}:
        return True
    if evidence.get("broker_positions") == [] and evidence.get("open_orders") == []:
        return True
    return False


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
        return _transition(BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE, blockers=blockers)
    return _transition(BROKER_BACKED_FILL_EVIDENCE_COMPLETE)


def normalize_no_broker_effect_result(evidence: Mapping[str, Any]) -> TrackBLifecycleTransition:
    if is_pre_submit_no_broker_effect(evidence):
        return _transition(BLOCKED_NO_BROKER_EFFECT)
    return _transition(str(_text(evidence.get("lifecycle_status")) or REVIEW_REQUIRED))


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
        "managed_position_registry_allowed": transition.managed_position_registry_allowed,
        "reconciliation_clean_eligible": transition.reconciliation_clean_eligible,
        "clean_trade_stats_allowed": transition.clean_trade_stats_allowed,
    }


def lifecycle_state_matrix() -> dict[str, dict[str, Any]]:
    return {
        state: {
            "allowed_from": list(rule.allowed_from),
            "required_evidence": list(rule.required_evidence),
            "terminal": rule.terminal,
            "broker_backed": rule.broker_backed,
            "no_broker_effect": rule.no_broker_effect,
            "managed_position_registry_allowed": rule.managed_position_registry_allowed,
            "reconciliation_clean_eligible": rule.reconciliation_clean_eligible,
            "operator_action_required": rule.operator_action_required,
            "clean_trade_stats_allowed": rule.clean_trade_stats_allowed,
        }
        for state, rule in _STATE_MATRIX.items()
    }


def normalize_lifecycle_state(value: Any) -> str:
    state = str(value or "").strip().upper()
    if state == TRACK_B_STRATEGY_PAPER_OPEN_MANAGED:
        return OPEN_MANAGED
    if state == TRACK_B_STRATEGY_PAPER_CLOSED_FLAT:
        return CLOSED_FLAT
    if state in _MANUAL_OR_MALFORMED_STATES:
        return MANUAL_OR_MALFORMED_CLEANUP
    return state


def lifecycle_state_rule(state: Any) -> TrackBLifecycleStateRule | None:
    return _STATE_MATRIX.get(normalize_lifecycle_state(state))


def is_terminal_state(state: Any) -> bool:
    rule = lifecycle_state_rule(state)
    return bool(rule and rule.terminal)


def is_registry_eligible(state: Any) -> bool:
    rule = lifecycle_state_rule(state)
    return bool(rule and rule.managed_position_registry_allowed)


def requires_operator_action(state: Any) -> bool:
    rule = lifecycle_state_rule(state)
    return bool(rule and rule.operator_action_required)


def is_clean_trade_stat_eligible(state: Any) -> bool:
    rule = lifecycle_state_rule(state)
    return bool(rule and rule.clean_trade_stats_allowed)


def requires_close_fill_or_broker_flat_proof(state: Any) -> bool:
    rule = lifecycle_state_rule(state)
    return bool(rule and "close_fill_or_broker_flat_proof" in rule.required_evidence)


def classify_transition(
    *,
    current_state: Any,
    target_state: Any,
    evidence: Mapping[str, Any],
) -> TrackBLifecycleTransitionCheck:
    current = normalize_lifecycle_state(current_state)
    target = normalize_lifecycle_state(target_state)
    target_rule = lifecycle_state_rule(target)
    if target_rule is None:
        transition = _transition(REVIEW_REQUIRED, blockers=("unknown_target_state",))
        return TrackBLifecycleTransitionCheck(
            current_state=current,
            target_state=target,
            transition=transition,
            allowed=False,
            blockers=("unknown_target_state",),
        )

    allowed_from = tuple(target_rule.allowed_from)
    transition_allowed = "*" in allowed_from or current in allowed_from
    evidence_with_target = dict(evidence)
    evidence_with_target["requested_lifecycle_status"] = target
    transition = classify_managed_position_transition(evidence_with_target)
    blockers = tuple(transition.blockers)
    if not transition_allowed:
        blockers = ("transition_not_allowed",) + blockers
    if transition.classification != target:
        blockers = (f"classified_as_{transition.classification}",) + blockers
    return TrackBLifecycleTransitionCheck(
        current_state=current,
        target_state=target,
        transition=transition,
        allowed=transition_allowed and transition.classification == target and not transition.blockers,
        blockers=blockers,
    )


def clean_trade_stats_allowed(evidence: Mapping[str, Any]) -> bool:
    transition = classify_managed_position_transition(evidence)
    return transition.clean_trade_stats_allowed


def _transition(classification: str, blockers: tuple[str, ...] = ()) -> TrackBLifecycleTransition:
    rule = _STATE_MATRIX.get(classification) or _STATE_MATRIX[REVIEW_REQUIRED]
    return TrackBLifecycleTransition(
        classification=classification if classification in _STATE_MATRIX else REVIEW_REQUIRED,
        blockers=blockers,
        terminal=rule.terminal,
        broker_backed=rule.broker_backed,
        no_broker_effect=rule.no_broker_effect,
        managed_position_registry_allowed=rule.managed_position_registry_allowed,
        reconciliation_clean_eligible=rule.reconciliation_clean_eligible,
        operator_action_required=rule.operator_action_required or bool(blockers),
        clean_trade_stats_allowed=rule.clean_trade_stats_allowed and not blockers,
    )


def _is_close_intent(evidence: Mapping[str, Any]) -> bool:
    intent = str(evidence.get("intent_type") or evidence.get("action") or "").upper()
    return "TO_CLOSE" in intent or str(evidence.get("requested_lifecycle_status") or "").upper() == CLOSED_FLAT


def _text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


_STATE_MATRIX: dict[str, TrackBLifecycleStateRule] = {
    INTENT_CREATED: TrackBLifecycleStateRule(
        state=INTENT_CREATED,
        allowed_from=(),
        required_evidence=("entry_intent_id", "lane_id", "strategy_id", "contract", "side", "quantity"),
        terminal=False,
        broker_backed=False,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
    SUBMIT_ATTEMPTED: TrackBLifecycleStateRule(
        state=SUBMIT_ATTEMPTED,
        allowed_from=(INTENT_CREATED,),
        required_evidence=("submit_attempt_id",),
        terminal=False,
        broker_backed=False,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
    SUBMITTED_PENDING_FILL: TrackBLifecycleStateRule(
        state=SUBMITTED_PENDING_FILL,
        allowed_from=(SUBMIT_ATTEMPTED,),
        required_evidence=("broker_order_id_or_perm_id",),
        terminal=False,
        broker_backed=False,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
    BLOCKED_NO_BROKER_EFFECT: TrackBLifecycleStateRule(
        state=BLOCKED_NO_BROKER_EFFECT,
        allowed_from=(INTENT_CREATED, SUBMIT_ATTEMPTED),
        required_evidence=("pre_submit_block_or_no_broker_effect",),
        terminal=True,
        broker_backed=False,
        no_broker_effect=True,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=True,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE: TrackBLifecycleStateRule(
        state=BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
        allowed_from=(SUBMITTED_PENDING_FILL, INTENT_CREATED),
        required_evidence=("partial_broker_identity",),
        terminal=False,
        broker_backed=True,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=True,
        clean_trade_stats_allowed=False,
    ),
    OPEN_MANAGED_METADATA_INCOMPLETE: TrackBLifecycleStateRule(
        state=OPEN_MANAGED_METADATA_INCOMPLETE,
        allowed_from=(SUBMITTED_PENDING_FILL, INTENT_CREATED),
        required_evidence=("broker_fill_identity",),
        terminal=False,
        broker_backed=True,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=True,
        clean_trade_stats_allowed=False,
    ),
    OPEN_MANAGED: TrackBLifecycleStateRule(
        state=OPEN_MANAGED,
        allowed_from=(SUBMITTED_PENDING_FILL, BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE, OPEN_MANAGED_METADATA_INCOMPLETE),
        required_evidence=(
            "entry_intent_id",
            "lane_id",
            "strategy_id",
            "contract",
            "side",
            "quantity",
            "broker_order_id_or_perm_id",
            "fill_price",
            "fill_timestamp",
            "managed_exit_policy_id",
            "lifecycle_id_or_destination",
        ),
        terminal=False,
        broker_backed=True,
        no_broker_effect=False,
        managed_position_registry_allowed=True,
        reconciliation_clean_eligible=True,
        operator_action_required=False,
        clean_trade_stats_allowed=True,
    ),
    CLOSED_FLAT: TrackBLifecycleStateRule(
        state=CLOSED_FLAT,
        allowed_from=(OPEN_MANAGED,),
        required_evidence=("close_fill_or_broker_flat_proof",),
        terminal=True,
        broker_backed=True,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=True,
        operator_action_required=False,
        clean_trade_stats_allowed=True,
    ),
    REVIEW_REQUIRED: TrackBLifecycleStateRule(
        state=REVIEW_REQUIRED,
        allowed_from=("*",),
        required_evidence=("review_reason",),
        terminal=False,
        broker_backed=False,
        no_broker_effect=False,
        managed_position_registry_allowed=True,
        reconciliation_clean_eligible=False,
        operator_action_required=True,
        clean_trade_stats_allowed=False,
    ),
    MANUAL_OR_MALFORMED_CLEANUP: TrackBLifecycleStateRule(
        state=MANUAL_OR_MALFORMED_CLEANUP,
        allowed_from=(REVIEW_REQUIRED, CLOSED_FLAT, OPEN_MANAGED),
        required_evidence=("operator_review_or_malformed_artifact_evidence",),
        terminal=True,
        broker_backed=False,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=True,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
    BROKER_BACKED_FILL_EVIDENCE_COMPLETE: TrackBLifecycleStateRule(
        state=BROKER_BACKED_FILL_EVIDENCE_COMPLETE,
        allowed_from=("*",),
        required_evidence=("broker_order_id_or_perm_id", "fill_price", "fill_timestamp"),
        terminal=False,
        broker_backed=True,
        no_broker_effect=False,
        managed_position_registry_allowed=False,
        reconciliation_clean_eligible=False,
        operator_action_required=False,
        clean_trade_stats_allowed=False,
    ),
}
