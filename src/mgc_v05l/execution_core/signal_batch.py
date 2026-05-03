"""Track B no-submit signal batch processing boundary."""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .signal_intent_proposal import (
    IntentProposalVerdict,
    SignalIntentProposalConfig,
    SignalIntentProposalPolicy,
    SignalIntentProposalResult,
    propose_intent_from_signal,
)


DEFAULT_SIGNAL_BATCH_OUTPUT_ROOT = Path("outputs/track_b_execution_core/signal_batches")


class SignalBatchVerdict(str, Enum):
    PROCESSED_FOR_REVIEW = "SIGNAL_BATCH_PROCESSED_FOR_REVIEW"
    COMPLETED_WITH_BLOCKERS = "SIGNAL_BATCH_COMPLETED_WITH_BLOCKERS"
    BLOCKED_LIVE_MODE = "SIGNAL_BATCH_BLOCKED_LIVE_MODE"
    BLOCKED_SUBMIT_ENABLED = "SIGNAL_BATCH_BLOCKED_SUBMIT_ENABLED"
    BLOCKED_EMPTY_SIGNALS = "SIGNAL_BATCH_BLOCKED_EMPTY_SIGNALS"
    BLOCKED_MISSING_POLICY = "SIGNAL_BATCH_BLOCKED_MISSING_POLICY"
    BLOCKED_SCHEMA_ERROR = "SIGNAL_BATCH_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class SignalBatch:
    batch_id: str
    shadow_run_id: str | None
    mode: str
    expected_account_id: str | None
    signal_items: tuple[Mapping[str, Any], ...]
    default_policy_id: str | None
    proposal_policy_path: str | None
    output_root: str | None
    submit_enabled: bool
    live_money_readiness: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "SignalBatch":
        return cls(
            batch_id=str(payload.get("batch_id") or f"signal_batch_{uuid.uuid4().hex}").strip(),
            shadow_run_id=_optional_str(payload.get("shadow_run_id") or payload.get("run_id")),
            mode=str(payload.get("mode") or "").strip().upper(),
            expected_account_id=_optional_str(payload.get("expected_account_id")),
            signal_items=_signal_items(payload.get("signal_items") or payload.get("signals")),
            default_policy_id=_optional_str(payload.get("default_policy_id")),
            proposal_policy_path=_optional_str(payload.get("proposal_policy_path")),
            output_root=_optional_str(payload.get("output_root")),
            submit_enabled=_bool(payload.get("submit_enabled", False)),
            live_money_readiness=False,
        )


@dataclass(frozen=True)
class SignalBatchResult:
    verdict: SignalBatchVerdict
    report_json: Path
    report: dict[str, Any]


def process_signal_batch(
    *,
    batch_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any] | None = None,
    expected_account_id: str | None = None,
    output_root: Path = DEFAULT_SIGNAL_BATCH_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> SignalBatchResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or str(batch_payload.get("batch_id") or f"signal_batch_{uuid.uuid4().hex}")
    run_root = Path(output_root) / actual_run_id
    report_json = run_root / "signal_batch_summary_report.json"
    try:
        batch = SignalBatch.from_mapping(batch_payload)
        blocking_verdict, blocker, action = _batch_blocker(batch=batch, policy_payload=policy_payload)
        if blocking_verdict is not None:
            return _write_summary(
                report_json=report_json,
                verdict=blocking_verdict,
                now=actual_now,
                batch=batch,
                item_reports=[],
                primary_blocker=blocker,
                required_next_action=action,
            )
        policy = SignalIntentProposalPolicy.from_mapping(policy_payload)
        account_id = expected_account_id or batch.expected_account_id
        item_reports = [
            _process_signal_item(
                index=index,
                signal_payload=signal_payload,
                policy=policy,
                expected_account_id=account_id,
                run_root=run_root,
                run_id=actual_run_id,
                now=actual_now,
            )
            for index, signal_payload in enumerate(batch.signal_items, start=1)
        ]
        blocked = sum(1 for item in item_reports if item.get("intent_proposal_verdict") != IntentProposalVerdict.CREATED_FOR_REVIEW.value)
        verdict = SignalBatchVerdict.PROCESSED_FOR_REVIEW if blocked == 0 else SignalBatchVerdict.COMPLETED_WITH_BLOCKERS
        return _write_summary(
            report_json=report_json,
            verdict=verdict,
            now=actual_now,
            batch=batch,
            item_reports=item_reports,
            primary_blocker=None if blocked == 0 else "One or more signal proposals were blocked.",
            required_next_action=(
                "Feed proposed intent artifacts into shadow_run_assembler for no-submit review."
                if blocked == 0
                else "Review blocked proposal reports before assembling downstream shadow runs."
            ),
        )
    except (TypeError, ValueError, OSError) as exc:
        report = {
            "schema_version": "track_b_signal_batch_v1",
            "generated_at": actual_now.isoformat(),
            "signal_batch_id": actual_run_id,
            "batch_validation_verdict": SignalBatchVerdict.BLOCKED_SCHEMA_ERROR.value,
            "total_signals": 0,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
            "primary_blocker": str(exc),
            "secondary_blockers": [],
            "required_next_action": "Fix signal batch schema before no-submit processing.",
            "report_json_path": str(report_json),
        }
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        return SignalBatchResult(verdict=SignalBatchVerdict.BLOCKED_SCHEMA_ERROR, report_json=report_json, report=report)


def _process_signal_item(
    *,
    index: int,
    signal_payload: Mapping[str, Any],
    policy: SignalIntentProposalPolicy,
    expected_account_id: str | None,
    run_root: Path,
    run_id: str,
    now: datetime,
) -> dict[str, Any]:
    item_id = f"signal_{index:04d}"
    proposal = propose_intent_from_signal(
        signal_payload=signal_payload,
        config=SignalIntentProposalConfig(
            expected_account_id=expected_account_id,
            policy=policy,
            output_root=run_root / "proposal_reports",
        ),
        run_id=f"{run_id}_{item_id}_proposal",
        now=now,
    )
    proposed_intent_path = None
    if proposal.proposed_intent is not None:
        proposed_intent_path = run_root / "proposed_intents" / f"{item_id}_proposed_intent.json"
        proposed_intent_path.parent.mkdir(parents=True, exist_ok=True)
        proposed_intent_path.write_text(json.dumps(to_jsonable(proposal.proposed_intent), indent=2, sort_keys=True), encoding="utf-8")
    item_report_json = run_root / "items" / item_id / "signal_batch_item_report.json"
    item_report = {
        "schema_version": "track_b_signal_batch_item_v1",
        "generated_at": now.isoformat(),
        "signal_batch_item_index": index,
        "intent_proposal_verdict": proposal.verdict.value,
        "signal_validation_verdict": proposal.report.get("signal_validation_verdict"),
        "intent_proposal_created": proposal.proposed_intent is not None,
        "proposal_report_json": str(proposal.report_json),
        "proposed_intent_output_path": None if proposed_intent_path is None else str(proposed_intent_path),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": proposal.report.get("primary_blocker"),
        "secondary_blockers": list(proposal.report.get("secondary_blockers") or ()),
        "required_next_action": proposal.report.get("required_next_action"),
        "signal_id": proposal.report.get("signal_id"),
        "strategy_id": proposal.report.get("strategy_id"),
        "lane_id": proposal.report.get("lane_id"),
        "local_execution_contract_key": proposal.report.get("local_execution_contract_key"),
        "item_report_json": str(item_report_json),
    }
    item_report_json.parent.mkdir(parents=True, exist_ok=True)
    item_report_json.write_text(json.dumps(to_jsonable(item_report), indent=2, sort_keys=True), encoding="utf-8")
    return item_report


def _write_summary(
    *,
    report_json: Path,
    verdict: SignalBatchVerdict,
    now: datetime,
    batch: SignalBatch,
    item_reports: Sequence[Mapping[str, Any]],
    primary_blocker: str | None,
    required_next_action: str,
) -> SignalBatchResult:
    blockers = Counter(str(item.get("intent_proposal_verdict") or "") for item in item_reports if item.get("primary_blocker"))
    report = {
        "schema_version": "track_b_signal_batch_v1",
        "generated_at": now.isoformat(),
        "signal_batch_id": batch.batch_id,
        "shadow_run_id": batch.shadow_run_id,
        "batch_validation_verdict": verdict.value,
        "mode": batch.mode,
        "expected_account_id": batch.expected_account_id,
        "default_policy_id": batch.default_policy_id,
        "proposal_policy_path": batch.proposal_policy_path,
        "total_signals": len(batch.signal_items) if not item_reports else len(item_reports),
        "signals_validated": sum(1 for item in item_reports if item.get("signal_validation_verdict") == "SHADOW_SIGNAL_VALID_FOR_REVIEW"),
        "proposal_attempts": len(item_reports),
        "proposed_intents_created": sum(1 for item in item_reports if item.get("intent_proposal_created")),
        "blocked_proposals": sum(1 for item in item_reports if not item.get("intent_proposal_created")),
        "blockers_count_by_type": dict(blockers),
        "proposed_intent_output_paths": [str(item.get("proposed_intent_output_path")) for item in item_reports if item.get("proposed_intent_output_path")],
        "proposal_report_output_paths": [str(item.get("proposal_report_json")) for item in item_reports],
        "item_report_output_paths": [str(item.get("item_report_json")) for item in item_reports],
        "submit_enabled": batch.submit_enabled,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _summary_secondary_blockers(item_reports),
        "required_next_action": required_next_action,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "dynamic_scoring_implemented": False,
        "paper_proof_cli_wired": False,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return SignalBatchResult(verdict=verdict, report_json=report_json, report=report)


def _batch_blocker(
    *,
    batch: SignalBatch,
    policy_payload: Mapping[str, Any] | None,
) -> tuple[SignalBatchVerdict | None, str | None, str]:
    if batch.mode != "PAPER":
        return SignalBatchVerdict.BLOCKED_LIVE_MODE, "Signal batch accepts PAPER mode only.", "Use PAPER mode for Track B signal batch processing."
    if batch.submit_enabled:
        return SignalBatchVerdict.BLOCKED_SUBMIT_ENABLED, "Signal batch is no-submit; submit_enabled must be false.", "Set submit_enabled=false."
    if not batch.signal_items:
        return SignalBatchVerdict.BLOCKED_EMPTY_SIGNALS, "Signal batch contains no signals.", "Provide one or more signal_items."
    if policy_payload is None:
        return SignalBatchVerdict.BLOCKED_MISSING_POLICY, "Proposal policy is required for signal batch processing.", "Provide --policy-json or policy_payload."
    return None, None, "Process signal batch."


def _summary_secondary_blockers(item_reports: Sequence[Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    seen: set[str] = set()
    for item in item_reports:
        for blocker in (item.get("primary_blocker"), *tuple(item.get("secondary_blockers") or ())):
            if not blocker:
                continue
            text = str(blocker)
            if text not in seen:
                seen.add(text)
                blockers.append(text)
    return blockers


def _signal_items(value: object) -> tuple[Mapping[str, Any], ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("signal_items must be a list.")
    items: list[Mapping[str, Any]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise ValueError("each signal item must be an object.")
        signal_payload = row.get("signal") if isinstance(row.get("signal"), Mapping) else row
        if not isinstance(signal_payload, Mapping):
            raise ValueError("each signal item must contain a signal object.")
        items.append(signal_payload)
    return tuple(items)


def _optional_str(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
