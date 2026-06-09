"""PAPER lifecycle-validation entry authority.

This module owns only controlled PAPER lifecycle-validation entries. It does
not authorize autonomous strategy entries, runtime starts, risk-reducing exits,
live-money behavior, broad flatten, or paper_proof.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping


PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED = "PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED"
PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED = "PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED"
PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED = "PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED"

EXECUTION_DOMAIN_TRACK_B_PAPER = "TRACK_B_PAPER"
DEFAULT_PAPER_ACCOUNT_ALLOWLIST = frozenset({"DUM882026"})
DEFAULT_CONTROLLED_INSTRUMENT_ALLOWLIST = frozenset({"MES", "MNQ", "MGC", "GC"})
DEFAULT_MAX_VALIDATION_QTY = 1.0

_CLEAN_RECONCILIATION_CLASSIFICATIONS = {
    "TRACK_B_PAPER_BROKER_RECONCILED",
    "TRACK_B_PHASE1_BROKER_RECONCILIATION_SUBMIT_GATE_READY",
}
_BROKER_CONNECTED_CLASSIFICATIONS = {
    "BROKER_TRUTH_REFRESH_READY",
    "IBKR_READ_ONLY_CONNECTED",
    "TRACK_B_BROKER_TRUTH_REFRESH_READY",
}
_SAFE_STATE_HARD_HOLD_CLASSIFICATIONS = {
    "SAFE_STATE_HARD_HOLD",
    "SAFE_STATE_BROKER_MUTATION_BLOCKED",
    "SAFE_STATE_LIVE_MONEY_BLOCKED",
}


@dataclass(frozen=True)
class PaperLifecycleValidationEntryAuthorityInput:
    repo_root: Path
    execution_domain: str
    mode: str
    account_id: str
    symbol: str
    local_symbol: str | None
    con_id: int | str | None
    action: str
    intent_type: str
    quantity: float
    paper_only: bool
    caller_path: str
    caller_metadata: Mapping[str, Any]
    authorization_check: Mapping[str, Any]
    authorization_artifact: Mapping[str, Any]
    reconciliation: Mapping[str, Any]
    open_order_truth: Mapping[str, Any]
    broker_truth_status: Mapping[str, Any]
    safe_state: Mapping[str, Any]
    generated_trade_id: str | None = None
    max_validation_qty: float = DEFAULT_MAX_VALIDATION_QTY
    account_allowlist: frozenset[str] = DEFAULT_PAPER_ACCOUNT_ALLOWLIST
    instrument_allowlist: frozenset[str] = DEFAULT_CONTROLLED_INSTRUMENT_ALLOWLIST
    now: datetime | None = None


def evaluate_paper_lifecycle_validation_entry_authority(
    request: PaperLifecycleValidationEntryAuthorityInput,
) -> dict[str, Any]:
    """Return a narrow authority decision for one controlled PAPER test entry."""

    now = _ensure_utc(request.now or datetime.now(UTC))
    hard_checks: list[dict[str, Any]] = []
    conditional_checks: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    metadata = dict(request.caller_metadata or {})
    authorization = dict(request.authorization_artifact or {})
    authorization_check = dict(request.authorization_check or {})
    reconciliation = dict(request.reconciliation or {})
    open_order_truth = dict(request.open_order_truth or {})
    broker_truth_status = dict(request.broker_truth_status or {})
    safe_state = dict(request.safe_state or {})

    symbol = str(request.symbol or "").strip().upper()
    action = str(request.action or "").strip().upper()
    intent_type = str(request.intent_type or "").strip().upper()
    local_symbol = str(request.local_symbol or metadata.get("local_symbol") or authorization.get("local_symbol") or "").strip()
    trade_id = str(metadata.get("trade_id") or request.generated_trade_id or "").strip()
    lifecycle_id = str(
        metadata.get("lifecycle_id")
        or metadata.get("position_lifecycle_id")
        or metadata.get("managed_lifecycle_id")
        or ""
    ).strip()

    _check(hard_checks, "execution_domain_track_b_paper", request.execution_domain == EXECUTION_DOMAIN_TRACK_B_PAPER)
    _check(hard_checks, "mode_paper", str(request.mode or "").strip().upper() == "PAPER")
    _check(hard_checks, "paper_only", request.paper_only is True)
    _check(hard_checks, "paper_account_allowlisted", str(request.account_id or "").strip() in request.account_allowlist)
    _check(hard_checks, "live_money_false", not _any_true(request, authorization, reconciliation, broker_truth_status, safe_state, key="live_money_eligible"))
    _check(hard_checks, "paper_proof_false", not _any_true(request, authorization, reconciliation, broker_truth_status, safe_state, key="paper_proof_invoked"))
    _check(hard_checks, "lifecycle_validation_caller", str(request.caller_path or "").strip() == "track_b_paper_leak_test_apply")
    _check(hard_checks, "authorization_artifact_valid", authorization_check.get("passed") is True)
    _check(hard_checks, "entry_intent", intent_type in {"BUY_TO_OPEN", "SELL_TO_OPEN"} and action in {"BUY", "SELL"})
    _check(hard_checks, "controlled_instrument_allowlisted", symbol in request.instrument_allowlist)
    _check(hard_checks, "local_symbol_known", bool(local_symbol))
    _check(hard_checks, "validation_qty_positive", float(request.quantity or 0.0) > 0.0)
    _check(hard_checks, "validation_qty_within_limit", float(request.quantity or 0.0) <= float(request.max_validation_qty))
    _check(hard_checks, "trade_or_lifecycle_ownership_generated", bool(trade_id or lifecycle_id))

    broker_connected = _broker_connected(broker_truth_status)
    broker_truth_available = _broker_truth_available(broker_truth_status, reconciliation)
    _check(hard_checks, "broker_connected", broker_connected)
    _check(hard_checks, "broker_truth_available", broker_truth_available)
    _check(hard_checks, "reconciliation_clean", _reconciliation_clean(reconciliation))
    _check(hard_checks, "safe_state_no_hard_halt", not _safe_state_hard_halt(safe_state))

    unknown_open_orders = _count_first(
        open_order_truth,
        reconciliation,
        keys=("unknown_open_order_count", "unknown_broker_open_order_count"),
    )
    open_orders = _count_first(
        open_order_truth,
        reconciliation,
        broker_truth_status,
        keys=("open_order_count", "track_b_broker_open_order_count"),
    )
    _check(conditional_checks, "no_unknown_open_orders", unknown_open_orders == 0)
    _check(conditional_checks, "no_conflicting_open_orders", open_orders == 0)

    if request.execution_domain == EXECUTION_DOMAIN_TRACK_B_PAPER and request.mode == "PAPER":
        diagnostics.append(_row("runtime_start_authority_not_required", True))
    if safe_state:
        diagnostics.append(
            _row(
                "safe_state_entry_and_broker_mutation",
                safe_state.get("entry_mutation_allowed") is not False
                and safe_state.get("broker_mutation_allowed") is not False,
                classification=str(safe_state.get("classification") or ""),
            )
        )

    block_reasons = [
        str(row["name"])
        for row in [*hard_checks, *conditional_checks]
        if row.get("passed") is not True
    ]
    degraded_reasons = [
        str(row["name"])
        for row in diagnostics
        if row.get("passed") is False
    ]
    if block_reasons:
        decision = PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
    elif degraded_reasons:
        decision = PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED
    else:
        decision = PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED

    return {
        "schema_version": "track_b_paper_lifecycle_validation_entry_authority_v1",
        "classification": decision,
        "decision": "BLOCKED"
        if decision == PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
        else "DEGRADED_ALLOWED"
        if decision == PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED
        else "ALLOWED",
        "allowed": decision
        in {
            PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED,
            PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED,
        },
        "block_reasons": block_reasons,
        "degraded_reasons": degraded_reasons,
        "hard_required_checks": hard_checks,
        "conditional_checks": conditional_checks,
        "diagnostic_checks": diagnostics,
        "execution_domain": request.execution_domain,
        "account_id": request.account_id,
        "symbol": symbol,
        "local_symbol": local_symbol or None,
        "con_id": request.con_id,
        "action": action,
        "intent_type": intent_type,
        "quantity": request.quantity,
        "trade_id": trade_id or None,
        "lifecycle_id": lifecycle_id or None,
        "authority_owner": "controlled_paper_lifecycle_validation_entry",
        "not_runtime_start_authority": True,
        "not_autonomous_strategy_entry_authority": True,
        "not_exit_authority": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "validated_at": now.isoformat(),
    }


def build_paper_lifecycle_validation_entry_authority_from_repo(
    *,
    repo_root: Path,
    execution_domain: str,
    mode: str,
    account_id: str,
    symbol: str,
    local_symbol: str | None,
    con_id: int | str | None,
    action: str,
    intent_type: str,
    quantity: float,
    paper_only: bool,
    caller_path: str,
    caller_metadata: Mapping[str, Any],
    authorization_check: Mapping[str, Any],
    authorization_artifact: Mapping[str, Any],
    generated_trade_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    root = Path(repo_root)
    request = PaperLifecycleValidationEntryAuthorityInput(
        repo_root=root,
        execution_domain=execution_domain,
        mode=mode,
        account_id=account_id,
        symbol=symbol,
        local_symbol=local_symbol,
        con_id=con_id,
        action=action,
        intent_type=intent_type,
        quantity=quantity,
        paper_only=paper_only,
        caller_path=caller_path,
        caller_metadata=caller_metadata,
        authorization_check=authorization_check,
        authorization_artifact=authorization_artifact,
        reconciliation=_read_json(
            root
            / "outputs"
            / "reports"
            / "track_b_paper_broker_reconciliation"
            / "latest_track_b_paper_broker_reconciliation.json"
        ),
        open_order_truth=_read_json(
            root
            / "outputs"
            / "track_b_execution_core"
            / "open_order_truth"
            / "latest_open_order_truth.json"
        ),
        broker_truth_status=_read_json(
            root
            / "outputs"
            / "reports"
            / "ibkr_read_only_verification"
            / "ibkr_broker_truth_refresh_status.json"
        ),
        safe_state=_read_json(
            root
            / "outputs"
            / "track_b_execution_core"
            / "safe_state"
            / "latest_runtime_safe_state_envelope.json"
        ),
        generated_trade_id=generated_trade_id,
        now=now,
    )
    return evaluate_paper_lifecycle_validation_entry_authority(request)


def _check(rows: list[dict[str, Any]], name: str, passed: bool, **extra: Any) -> None:
    rows.append(_row(name, passed, **extra))


def _row(name: str, passed: bool, **extra: Any) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), **extra}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _any_true(*sources: Any, key: str) -> bool:
    for source in sources:
        if isinstance(source, Mapping) and source.get(key) is True:
            return True
        if hasattr(source, key) and getattr(source, key) is True:
            return True
    return False


def _broker_connected(status: Mapping[str, Any]) -> bool:
    classification = str(status.get("classification") or status.get("verifier_classification") or "").strip()
    if classification in _BROKER_CONNECTED_CLASSIFICATIONS:
        return True
    if status.get("last_success") is True or status.get("positions_complete") is True:
        return True
    return False


def _broker_truth_available(status: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> bool:
    if status.get("positions_complete") is True and status.get("open_orders_complete") is True:
        return True
    if reconciliation.get("broker_reconciled") is True and reconciliation.get("track_b_broker_positions") is not None:
        return True
    return False


def _reconciliation_clean(reconciliation: Mapping[str, Any]) -> bool:
    classification = str(reconciliation.get("classification") or "").strip()
    if classification and classification not in _CLEAN_RECONCILIATION_CLASSIFICATIONS:
        return False
    if reconciliation.get("broker_reconciled") is not True:
        return False
    if int(reconciliation.get("review_required_count") or reconciliation.get("current_scope_review_required_count") or 0) != 0:
        return False
    blockers = list(reconciliation.get("blockers") or reconciliation.get("block_reasons") or [])
    return not blockers


def _safe_state_hard_halt(safe_state: Mapping[str, Any]) -> bool:
    classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "").strip()
    if classification in _SAFE_STATE_HARD_HOLD_CLASSIFICATIONS or "HARD_HOLD" in classification:
        return True
    if safe_state.get("live_money_eligible") is True or safe_state.get("paper_proof_invoked") is True:
        return True
    if safe_state.get("broker_mutation_allowed") is False:
        return True
    return False


def _count_first(*sources: Mapping[str, Any], keys: tuple[str, ...]) -> int:
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in keys:
            if key in source and source.get(key) is not None:
                return int(source.get(key) or 0)
        summary = source.get("summary")
        if isinstance(summary, Mapping):
            for key in keys:
                if key in summary and summary.get(key) is not None:
                    return int(summary.get(key) or 0)
    return 0


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
