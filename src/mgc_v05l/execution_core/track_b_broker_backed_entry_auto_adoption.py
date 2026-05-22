"""Shared Track B PAPER broker-backed entry auto-adoption.

This module owns the normal broker-effect-confirmed entry convergence path:
manifest update, OPEN_MANAGED lifecycle report, compact ledger projection, and
live-position read model. It is PAPER-only and never submits, cancels, closes,
or queries broker state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime
from .track_b_lifecycle_state_transition import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    OPEN_MANAGED,
    classify_managed_position_transition,
)
from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    TrackBPaperTradeLedgerResult,
    update_track_b_paper_trade_ledger_from_filled_bridge_result,
)
from .track_b_position_management_manifest import (
    DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    create_or_update_position_management_manifest,
)
from .track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
)

AUTO_ADOPTION_APPLIED = "BROKER_BACKED_ENTRY_AUTO_ADOPTED_OPEN_MANAGED"
AUTO_ADOPTION_BLOCKED_NO_BROKER_EFFECT = "BROKER_BACKED_ENTRY_BLOCKED_NO_BROKER_EFFECT"
AUTO_ADOPTION_INCOMPLETE = "BROKER_BACKED_ENTRY_EVIDENCE_INCOMPLETE"
AUTO_ADOPTION_NOT_ENTRY = "BROKER_BACKED_ENTRY_AUTO_ADOPTION_NOT_ENTRY"


@dataclass(frozen=True)
class TrackBBrokerBackedEntryAutoAdoptionResult:
    classification: str
    transition_classification: str
    ledger_result: TrackBPaperTradeLedgerResult | None = None
    manifest_path: Path | None = None
    lifecycle_report_path: Path | None = None
    blockers: tuple[str, ...] = ()

    @property
    def open_managed(self) -> bool:
        return self.classification == AUTO_ADOPTION_APPLIED


def auto_adopt_broker_backed_entry(
    *,
    entry_fill_evidence: Mapping[str, Any],
    evidence_path: Path | None = None,
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    position_management_manifest_root: Path = DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    managed_lifecycle_output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    now: datetime | None = None,
) -> TrackBBrokerBackedEntryAutoAdoptionResult:
    """Converge a broker-effect-confirmed Track B entry into managed state."""

    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    payload = dict(entry_fill_evidence)
    intent_type = str(payload.get("intent_type") or "").upper()
    if intent_type not in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return TrackBBrokerBackedEntryAutoAdoptionResult(
            classification=AUTO_ADOPTION_NOT_ENTRY,
            transition_classification=AUTO_ADOPTION_NOT_ENTRY,
        )

    if payload.get("paper_proof_invoked") is True or payload.get("live_money_readiness") is True:
        return TrackBBrokerBackedEntryAutoAdoptionResult(
            classification=AUTO_ADOPTION_INCOMPLETE,
            transition_classification=BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
            blockers=("paper_only_safety",),
        )

    payload.setdefault("paper_proof_invoked", False)
    payload.setdefault("live_money_readiness", False)
    payload.setdefault("review_required", False)
    payload.setdefault("lifecycle_id", _lifecycle_id(payload))

    transition = classify_managed_position_transition(
        {
            **payload,
            "requested_lifecycle_status": OPEN_MANAGED,
            "entry_intent_id": payload.get("order_intent_id"),
            "side": "LONG" if intent_type == "BUY_TO_OPEN" else "SHORT",
            "lifecycle_id": payload.get("lifecycle_id"),
        }
    )
    if transition.classification == BLOCKED_NO_BROKER_EFFECT:
        _write_blocked_manifest(payload, position_management_manifest_root, actual_now)
        return TrackBBrokerBackedEntryAutoAdoptionResult(
            classification=AUTO_ADOPTION_BLOCKED_NO_BROKER_EFFECT,
            transition_classification=transition.classification,
            blockers=transition.blockers,
        )

    bridge_classification = str(payload.get("bridge_classification") or payload.get("classification") or "").strip()
    if bridge_classification:
        payload["bridge_classification"] = bridge_classification
    payload["classification"] = "PAPER_STRATEGY_ORDER_FILLED_PERSISTED"
    transition = classify_managed_position_transition(
        {
            **payload,
            "requested_lifecycle_status": OPEN_MANAGED,
            "entry_intent_id": payload.get("order_intent_id"),
            "side": "LONG" if intent_type == "BUY_TO_OPEN" else "SHORT",
            "lifecycle_id": payload.get("lifecycle_id"),
        }
    )
    if not transition.open_managed_allowed:
        return TrackBBrokerBackedEntryAutoAdoptionResult(
            classification=AUTO_ADOPTION_INCOMPLETE,
            transition_classification=transition.classification,
            blockers=transition.blockers,
        )

    ledger_result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=payload,
        filled_bridge_result_json=evidence_path,
        output_root=paper_trade_ledger_output_root,
        position_management_manifest_root=position_management_manifest_root,
        managed_lifecycle_output_root=managed_lifecycle_output_root,
        now=actual_now,
    )
    trade_record = ledger_result.trade_record or {}
    return TrackBBrokerBackedEntryAutoAdoptionResult(
        classification=AUTO_ADOPTION_APPLIED
        if str(trade_record.get("final_position_status") or "") == OPEN_MANAGED
        else AUTO_ADOPTION_INCOMPLETE,
        transition_classification=str(trade_record.get("final_position_status") or transition.classification),
        ledger_result=ledger_result,
        manifest_path=Path(str(trade_record.get("position_management_manifest_path")))
        if trade_record.get("position_management_manifest_path")
        else None,
        lifecycle_report_path=Path(str(trade_record.get("paper_lifecycle_report_path")))
        if trade_record.get("paper_lifecycle_report_path")
        else None,
        blockers=tuple(trade_record.get("position_management_metadata_blockers") or ()),
    )


def _write_blocked_manifest(payload: Mapping[str, Any], output_root: Path, now: datetime) -> None:
    intent_id = str(payload.get("order_intent_id") or "").strip()
    if not intent_id:
        return
    create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id=str(payload.get("lane_id") or ""),
        strategy_id=str(payload.get("strategy_id") or payload.get("lane_id") or ""),
        instrument_family=str(payload.get("instrument") or payload.get("symbol") or ""),
        contract_key=str(payload.get("contract_key") or ""),
        local_symbol=str(payload.get("local_symbol") or ""),
        con_id=payload.get("con_id"),
        side="LONG" if str(payload.get("intent_type") or "").upper() == "BUY_TO_OPEN" else "SHORT",
        quantity=payload.get("quantity"),
        managed_exit_policy_id=payload.get("managed_exit_policy_id"),
        lifecycle_status=BLOCKED_NO_BROKER_EFFECT,
        output_root=output_root,
        now=now,
    )


def _lifecycle_id(payload: Mapping[str, Any]) -> str | None:
    explicit = str(payload.get("lifecycle_id") or "").strip()
    if explicit:
        return explicit
    intent_id = str(payload.get("order_intent_id") or "").strip()
    return f"bridge_fill_{intent_id}" if intent_id else None
