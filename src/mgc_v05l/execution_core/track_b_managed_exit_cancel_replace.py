"""Guarded Track B PAPER cancel/replace for known managed exit orders.

This module is intentionally narrow: it only acts on a reconciliation-produced
known managed exit-order proposal, and only when explicitly run with apply=True.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .ibkr_paper_adapter import IbkrPaperAdapter
from .models import IntentKind, OrderIntent, SubmitAttempt, SubmitAttemptState, to_jsonable
from .track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from .track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from .track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from .track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from .track_b_order_adjustment_planner import (
    DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT,
    TARGETED_CANCEL_REPLACE_REQUIRED,
)
from .track_b_paper_broker_reconciliation import (
    PAPER_ACCOUNT,
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)
from .track_b_paper_trade_ledger import update_track_b_paper_trade_ledger_from_filled_bridge_result
from .track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from .track_b_runtime_supervisor_authority import DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT


GUARDED_CANCEL_REPLACE_READY = "GUARDED_CANCEL_REPLACE_READY"
GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND = "GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND"
GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH = "GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH"
GUARDED_CANCEL_REPLACE_CANCELLED_OR_FILLED_BEFORE_ACTION = "GUARDED_CANCEL_REPLACE_CANCELLED_OR_FILLED_BEFORE_ACTION"
GUARDED_CANCEL_REPLACE_CANCEL_CONFIRMED = "GUARDED_CANCEL_REPLACE_CANCEL_CONFIRMED"
GUARDED_CANCEL_REPLACE_REPLACEMENT_SUBMITTED = "GUARDED_CANCEL_REPLACE_REPLACEMENT_SUBMITTED"
GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED = "GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED"
GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING = "GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING"
GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED = "GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED"
GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED = "GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED"

KNOWN_MANAGED_EXIT_STATE_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_orders"
    / "latest_known_managed_exit_orders.json"
)
DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "track_b_managed_exit_cancel_replace"


class ManagedExitCancelReplaceError(RuntimeError):
    """Raised when guarded cancel/replace cannot safely proceed."""


class ManagedExitAdapter(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> tuple[str, ...]: ...
    def require_configured_account(self) -> str: ...
    def register_existing_order_for_cancel(
        self,
        *,
        submit_attempt: SubmitAttempt,
        order_intent: OrderIntent,
        broker_order_id: str,
        created_at: datetime,
    ) -> None: ...
    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None: ...
    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None: ...
    def submit_limit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent) -> int: ...
    def wait_for_broker_order(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> Any: ...
    def wait_for_fill(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> Any: ...
    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ManagedExitCancelReplaceConfig:
    repo_root: Path
    broker_order_id: str
    client_id: int | None = None
    perm_id: int | None = None
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    tws_client_id: int = 10941
    account_id: str = PAPER_ACCOUNT
    apply: bool = False
    output_dir: Path = DEFAULT_REPORT_DIR
    known_managed_exit_state_path: Path = KNOWN_MANAGED_EXIT_STATE_PATH
    reconciliation_report_path: Path | None = None
    cancel_timeout_seconds: float = 20.0
    replacement_ack_timeout_seconds: float = 20.0
    replacement_fill_wait_seconds: float = 5.0
    require_shared_truth_evidence: bool = True
    shared_truth_max_age_seconds: float = 600.0
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT

    @property
    def resolved_output_dir(self) -> Path:
        return self.repo_root / self.output_dir if not self.output_dir.is_absolute() else self.output_dir

    @property
    def resolved_known_managed_exit_state_path(self) -> Path:
        path = self.known_managed_exit_state_path
        return self.repo_root / path if not path.is_absolute() else path


def run_guarded_managed_exit_cancel_replace(
    *,
    config: ManagedExitCancelReplaceConfig,
    now: datetime | None = None,
    adapter_factory: Callable[..., ManagedExitAdapter] | None = None,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None = None,
    broker_truth_refresh: Callable[[], Mapping[str, Any]] | None = None,
    ledger_update_runner: Callable[..., Any] = update_track_b_paper_trade_ledger_from_filled_bridge_result,
) -> dict[str, Any]:
    """Validate and optionally execute one exact guarded cancel/replace."""

    actual_now = now or datetime.now(UTC)
    if broker_truth_refresh is not None:
        broker_truth_refresh()
    reconciliation = _run_reconciliation(config=config, reconciliation_runner=reconciliation_runner)
    ready = _validate_ready(config=config, reconciliation=reconciliation, now=actual_now)
    shared_truth_evidence = _shared_truth_cancel_replace_evidence(
        config=config,
        reconciliation=reconciliation,
        ready=ready,
        now=actual_now,
    )
    if ready["classification"] == GUARDED_CANCEL_REPLACE_READY and shared_truth_evidence["blockers"]:
        ready = _blocked(
            GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED,
            "; ".join(str(item) for item in shared_truth_evidence["blockers"]),
        )
    if ready["classification"] != GUARDED_CANCEL_REPLACE_READY:
        report = _base_report(
            config=config,
            now=actual_now,
            reconciliation=reconciliation,
            ready=ready,
            shared_truth_evidence=shared_truth_evidence,
        )
        _write_report(config, report)
        return report
    report = _base_report(
        config=config,
        now=actual_now,
        reconciliation=reconciliation,
        ready=ready,
        shared_truth_evidence=shared_truth_evidence,
    )
    if not config.apply:
        report["detail"] = "Guarded cancel/replace is ready; apply=false so no broker mutation was attempted."
        _write_report(config, report)
        return report

    adapter_factory = adapter_factory or _default_adapter_factory
    adapter = adapter_factory(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.tws_client_id,
        account_id=config.account_id,
        contract_allowlist={ready["contract_key"]: _contract_allowlist_entry(ready["proposal"])},
        submit_enabled=True,
    )
    try:
        adapter.connect()
        adapter.managed_accounts()
        adapter.require_configured_account()
        cancel_intent = _order_intent_from_known_order(config=config, ready=ready, now=actual_now)
        cancel_attempt = _submit_attempt(
            config=config,
            order_intent=cancel_intent,
            ready=ready,
            now=actual_now,
            stage="cancel",
            broker_order_id=str(config.broker_order_id),
            perm_id=str(config.perm_id or ready["known_order"].get("perm_id") or ""),
        )
        adapter.register_existing_order_for_cancel(
            submit_attempt=cancel_attempt,
            order_intent=cancel_intent,
            broker_order_id=str(config.broker_order_id),
            created_at=actual_now,
        )
        adapter.cancel_order(submit_attempt_id=cancel_attempt.submit_attempt_id, broker_order_id=str(config.broker_order_id))
        adapter.wait_for_cancel(
            submit_attempt_id=cancel_attempt.submit_attempt_id,
            timeout_seconds=config.cancel_timeout_seconds,
        )
        report["classification"] = GUARDED_CANCEL_REPLACE_CANCEL_CONFIRMED
        report["broker_mutation_performed"] = True
        report["cancel"] = {
            "classification": GUARDED_CANCEL_REPLACE_CANCEL_CONFIRMED,
            "broker_order_id": str(config.broker_order_id),
            "client_id": config.client_id,
            "perm_id": config.perm_id,
            "cancelled_at": datetime.now(UTC).isoformat(),
        }
        report["replacement_precondition"] = {
            "old_order_terminal_cancel_confirmed": True,
            "replacement_allowed_after_terminal_cancel": True,
        }

        replacement_intent = _replacement_order_intent(config=config, ready=ready)
        replacement_attempt = _submit_attempt(
            config=config,
            order_intent=replacement_intent,
            ready=ready,
            now=datetime.now(UTC),
            stage="replacement",
        )
        replacement_order_id = adapter.submit_limit_order(
            submit_attempt=replacement_attempt,
            order_intent=replacement_intent,
        )
        replacement_order = adapter.wait_for_broker_order(
            submit_attempt_id=replacement_attempt.submit_attempt_id,
            timeout_seconds=config.replacement_ack_timeout_seconds,
        )
        report["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_SUBMITTED
        report["replacement"] = _replacement_report(
            classification=GUARDED_CANCEL_REPLACE_REPLACEMENT_SUBMITTED,
            ready=ready,
            replacement_order_id=str(replacement_order_id),
            replacement_order=replacement_order,
            replacement_intent=replacement_intent,
            source_report_path=_report_path(config),
        )
        try:
            fill = adapter.wait_for_fill(
                submit_attempt_id=replacement_attempt.submit_attempt_id,
                timeout_seconds=config.replacement_fill_wait_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - no fill in the short window means working, not a failure.
            report["replacement"]["fill_wait_detail"] = str(exc)
            _persist_known_managed_exit_order(config=config, ready=ready, replacement=report["replacement"])
            report["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING
            report["replacement"]["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING
            _write_report(config, report)
            return report

        filled_result = _filled_bridge_result(ready=ready, replacement=report["replacement"], fill=fill)
        filled_result_path = _filled_result_path(config)
        _write_json(filled_result_path, filled_result)
        ledger_result = ledger_update_runner(
            filled_bridge_result=filled_result,
            filled_bridge_result_json=filled_result_path,
            output_root=config.repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
            now=datetime.now(UTC),
        )
        report["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED
        report["replacement"]["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED
        report["replacement"]["fill"] = _jsonable(fill)
        report["lifecycle_close"] = {
            "persisted": bool(getattr(ledger_result, "trade_record_written", False)),
            "ledger_jsonl": str(getattr(ledger_result, "ledger_jsonl", "")),
        }
        _write_report(config, report)
        return report
    except Exception as exc:  # noqa: BLE001 - broker action failures must become audit artifacts.
        report["classification"] = GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED
        report["detail"] = str(exc)
        report["adapter_diagnostics"] = _safe_adapter_diagnostics(adapter)
        _write_report(config, report)
        return report
    finally:
        try:
            adapter.disconnect()
        except Exception:
            pass


def _run_reconciliation(
    *,
    config: ManagedExitCancelReplaceConfig,
    reconciliation_runner: Callable[[ReconciliationConfig], Mapping[str, Any]] | None,
) -> Mapping[str, Any]:
    report_path = config.reconciliation_report_path or (
        config.repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    recon_config = ReconciliationConfig(repo_root=config.repo_root, report_path=report_path)
    runner = reconciliation_runner or (lambda cfg: reconcile_track_b_paper_broker_truth(config=cfg))
    return runner(recon_config)


def _validate_ready(
    *,
    config: ManagedExitCancelReplaceConfig,
    reconciliation: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    if config.mode.upper() != "PAPER" or config.port != 7497 or config.host != "127.0.0.1":
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Executor is locked to local TWS PAPER on 127.0.0.1:7497.")
    if config.account_id != PAPER_ACCOUNT:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Executor is locked to PAPER account DUM882026.")
    if reconciliation.get("live_money_eligible") is not False:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "live_money_eligible must be false.")
    if reconciliation.get("paper_proof_invoked") is not False:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "paper_proof_invoked must be false.")
    if reconciliation.get("broker_reconciled") is not True:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Broker/lifecycle reconciliation must be clean.")
    if int(reconciliation.get("review_required_count") or 0) != 0:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "review_required_count must be zero.")
    if int(reconciliation.get("unknown_broker_open_order_count") or 0) != 0:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Unknown broker open orders block cancel/replace.")
    known_order = _known_order_for_config(config, reconciliation)
    if known_order is None:
        if _order_id_in_open_orders(config.broker_order_id, reconciliation.get("track_b_broker_open_orders") or []):
            return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, "Broker order exists but is not a known managed exit order.")
        return _blocked(GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND, "Known managed exit order was not found.")
    proposal = known_order.get("guarded_cancel_replace_proposal")
    if not isinstance(proposal, Mapping):
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Known managed exit order has no policy proposal.")
    if proposal.get("allowed_route") != "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY":
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Policy proposal is not for the guarded cancel/replace route.")
    if proposal.get("broker_mutation_performed") is not False:
        return _blocked(GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED, "Proposal already records broker mutation.")
    missing = _missing_required_proposal_fields(known_order=known_order, proposal=proposal)
    if missing:
        return _blocked(
            GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH,
            f"Proposal is missing required identity fields: {', '.join(missing)}.",
        )
    current = _matching_open_order(config.broker_order_id, reconciliation.get("track_b_broker_open_orders") or [])
    if current is None:
        return _blocked(GUARDED_CANCEL_REPLACE_CANCELLED_OR_FILLED_BEFORE_ACTION, "Original order is no longer open.")
    mismatch = _identity_mismatch(config=config, known_order=known_order, current=current, proposal=proposal)
    if mismatch:
        return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, mismatch)
    replacement = proposal.get("replacement_order") if isinstance(proposal.get("replacement_order"), Mapping) else {}
    if str(replacement.get("action") or "").upper() != str(known_order.get("action") or "").upper():
        return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, "Replacement action must match original exit action.")
    if str(known_order.get("order_type") or "").upper() != "LMT":
        return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, "Current known managed exit order must be LMT.")
    if str(known_order.get("tif") or known_order.get("time_in_force") or "").upper() != "DAY":
        return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, "Current known managed exit order must be DAY.")
    if _decimal(known_order.get("limit_price") or known_order.get("order_limit_price")) is None:
        return _blocked(GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH, "Current known managed exit order limit price is required.")
    contract_key = _contract_key(proposal)
    return {
        "classification": GUARDED_CANCEL_REPLACE_READY,
        "detail": "Exact known managed exit order proposal is ready for guarded cancel/replace.",
        "known_order": dict(known_order),
        "current_open_order": dict(current),
        "proposal": dict(proposal),
        "contract_key": contract_key,
        "validated_at": now.isoformat(),
    }


def _known_order_for_config(config: ManagedExitCancelReplaceConfig, reconciliation: Mapping[str, Any]) -> dict[str, Any] | None:
    for row in reconciliation.get("known_managed_exit_orders") or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("broker_order_id") or "") != str(config.broker_order_id):
            continue
        if config.client_id is not None and str(row.get("client_id") or "") != str(config.client_id):
            return None
        if config.perm_id is not None and str(row.get("perm_id") or "") != str(config.perm_id):
            return None
        if str(row.get("managed_order_policy", {}).get("recommended_action") or "") != "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER":
            return None
        return dict(row)
    return None


def _missing_required_proposal_fields(*, known_order: Mapping[str, Any], proposal: Mapping[str, Any]) -> list[str]:
    cancel_identity = proposal.get("cancel_identity") if isinstance(proposal.get("cancel_identity"), Mapping) else {}
    replacement = proposal.get("replacement_order") if isinstance(proposal.get("replacement_order"), Mapping) else {}
    required_pairs = [
        ("lifecycle_id", known_order.get("lifecycle_id")),
        ("strategy_id", known_order.get("strategy_id")),
        ("lane_id", known_order.get("lane_id")),
        ("account_id", cancel_identity.get("account_id")),
        ("symbol", cancel_identity.get("symbol")),
        ("local_symbol", cancel_identity.get("local_symbol")),
        ("expiry", cancel_identity.get("expiry")),
        ("con_id", cancel_identity.get("con_id")),
        ("broker_order_id", cancel_identity.get("broker_order_id")),
        ("client_id", cancel_identity.get("client_id")),
        ("perm_id", cancel_identity.get("perm_id")),
        ("action", cancel_identity.get("action")),
        ("quantity", cancel_identity.get("quantity")),
        ("current_order_type", known_order.get("order_type")),
        ("current_limit_price", known_order.get("limit_price") or known_order.get("order_limit_price")),
        ("current_tif", known_order.get("tif") or known_order.get("time_in_force")),
        ("replacement_order_type", replacement.get("order_type")),
        ("replacement_limit_price", replacement.get("limit_price")),
        ("replacement_tif", replacement.get("tif")),
    ]
    return [key for key, value in required_pairs if value in {None, ""}]


def _identity_mismatch(
    *,
    config: ManagedExitCancelReplaceConfig,
    known_order: Mapping[str, Any],
    current: Mapping[str, Any],
    proposal: Mapping[str, Any],
) -> str | None:
    cancel_identity = proposal.get("cancel_identity") if isinstance(proposal.get("cancel_identity"), Mapping) else {}
    checks = [
        ("account_id", config.account_id),
        ("broker_order_id", config.broker_order_id),
        ("client_id", config.client_id),
        ("perm_id", config.perm_id),
        ("symbol", cancel_identity.get("symbol")),
        ("local_symbol", cancel_identity.get("local_symbol")),
        ("expiry", cancel_identity.get("expiry")),
        ("con_id", cancel_identity.get("con_id")),
        ("action", cancel_identity.get("action")),
        ("quantity", cancel_identity.get("quantity")),
    ]
    for key, expected in checks:
        if expected in {None, ""}:
            continue
        actual = current.get(key) or current.get(_camel(key)) or known_order.get(key) or known_order.get(_camel(key))
        if key == "account_id" and not actual:
            actual = config.account_id
        if key in {"quantity"}:
            if _decimal(actual) != _decimal(expected):
                return f"{key} mismatch: expected {expected}, saw {actual}."
        elif str(actual or "").strip().upper() != str(expected or "").strip().upper():
            return f"{key} mismatch: expected {expected}, saw {actual}."
    return None


def _base_report(
    *,
    config: ManagedExitCancelReplaceConfig,
    now: datetime,
    reconciliation: Mapping[str, Any],
    ready: Mapping[str, Any],
    shared_truth_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_exit_cancel_replace_v1",
        "generated_at": now.isoformat(),
        "classification": ready.get("classification", GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED),
        "detail": ready.get("detail"),
        "broker_mutation_attempted": bool(config.apply),
        "broker_mutation_performed": False,
        "apply": bool(config.apply),
        "route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
        "account_id": config.account_id,
        "requested_identity": {
            "broker_order_id": config.broker_order_id,
            "client_id": config.client_id,
            "perm_id": config.perm_id,
        },
        "ready": _jsonable(ready),
        "shared_truth_evidence": _jsonable(shared_truth_evidence),
        "reconciliation_summary": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "broker_position_count": reconciliation.get("track_b_broker_position_count"),
            "lifecycle_position_count": reconciliation.get("lifecycle_open_position_count"),
            "open_order_count": reconciliation.get("track_b_broker_open_order_count"),
            "unknown_open_order_count": reconciliation.get("unknown_broker_open_order_count"),
            "known_managed_exit_order_count": reconciliation.get("known_managed_exit_order_count"),
            "review_required_count": reconciliation.get("review_required_count"),
            "live_money_eligible": reconciliation.get("live_money_eligible"),
            "paper_proof_invoked": reconciliation.get("paper_proof_invoked"),
        },
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _shared_truth_cancel_replace_evidence(
    *,
    config: ManagedExitCancelReplaceConfig,
    reconciliation: Mapping[str, Any],
    ready: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    paths = {
        "open_order_truth": config.open_order_truth_path,
        "managed_order_registry": config.managed_order_registry_path,
        "order_adjustment_plan": config.order_adjustment_plan_path,
        "position_truth": config.position_truth_path,
        "managed_position_registry": config.managed_position_registry_path,
        "runtime_supervisor_authority": config.runtime_supervisor_authority_path,
        "broker_lease": config.broker_lease_path,
    }
    payloads = {name: _read_json(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()}
    classifications = {name: _classification(payload) for name, payload in payloads.items()}
    classifications["reconciliation"] = str(reconciliation.get("classification") or "")
    freshness = {
        name: _freshness(payload=payload, now=now, max_age_seconds=config.shared_truth_max_age_seconds)
        for name, payload in payloads.items()
    }
    blockers: list[str] = []
    if config.require_shared_truth_evidence:
        for name, payload in payloads.items():
            if not payload:
                blockers.append(f"Shared truth authority artifact missing: {name}.")
        for name, state in freshness.items():
            if state["stale_or_missing"]:
                blockers.append(f"Shared truth authority artifact stale/missing: {name}.")

    if classifications["runtime_supervisor_authority"] in {
        "SUPERVISOR_MANUAL_REVIEW_REQUIRED",
        "SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK",
        "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP",
        "SUPERVISOR_SHARED_TRUTH_STALE",
        "SUPERVISOR_UNKNOWN_REVIEW_REQUIRED",
    }:
        blockers.append(
            f"Runtime Supervisor Authority blocks targeted cancel/replace: {classifications['runtime_supervisor_authority']}."
        )
    if classifications["reconciliation"] in {
        "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
        "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED_UNKNOWN_OPEN_ORDERS",
    }:
        blockers.append(f"Reconciliation is unsafe for targeted cancel/replace: {classifications['reconciliation']}.")
    if classifications["broker_lease"] in {
        "INVALIDATED_CONTRADICTION",
        "INVALIDATED_UNKNOWN_OPEN_ORDERS",
        "INVALIDATED_MANUAL_BROKER_ACTION",
        "OPERATOR_REQUIRED",
    }:
        blockers.append(f"Broker Truth Lease is unsafe for targeted cancel/replace: {classifications['broker_lease']}.")
    if classifications["open_order_truth"] in {
        "DUPLICATE_CLOSE_ORDER",
        "UNKNOWN_OPEN_ORDER",
        "SUSPICIOUS_ORDER_STATE",
        "BROKER_FLAT_WITH_OPEN_CLOSE_ORDER",
    }:
        blockers.append(f"Open Order Truth blocks targeted cancel/replace: {classifications['open_order_truth']}.")

    managed_order_match = _matching_managed_order(config=config, payload=payloads["managed_order_registry"])
    plan_match = _matching_adjustment_plan(config=config, payload=payloads["order_adjustment_plan"])
    if managed_order_match is None:
        blockers.append("Managed Order Registry does not contain the exact target order identity.")
    else:
        managed_order_class = str(managed_order_match.get("classification") or "")
        if managed_order_class in {
            "CLOSE_ORDER_SUSPICIOUS",
            "DUPLICATE_CLOSE_ORDER_BLOCKED",
            "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
            "BROKER_FLAT_WITH_WORKING_CLOSE",
        }:
            blockers.append(f"Managed Order Registry blocks targeted cancel/replace: {managed_order_class}.")
    if plan_match is None:
        blockers.append("Order Adjustment Planner does not contain the exact target order identity.")
    else:
        plan_class = str(plan_match.get("classification") or "")
        if plan_class == "REVIEW_REQUIRED_SUSPICIOUS_STATE":
            blockers.append("MANUAL_TWS_REVIEW_REQUIRED: Order Adjustment Planner reports suspicious state.")
        elif plan_class in {"DO_NOT_REPLACE_DUPLICATE_RISK", "BROKER_FLAT_NO_REPLACE"}:
            blockers.append(f"Order Adjustment Planner forbids targeted cancel/replace: {plan_class}.")
        elif plan_class != TARGETED_CANCEL_REPLACE_REQUIRED:
            blockers.append(f"Order Adjustment Planner is not targeted-cancel/replace ready: {plan_class}.")
        if plan_match.get("existing_close_order_live") is True and plan_class != TARGETED_CANCEL_REPLACE_REQUIRED:
            blockers.append("Existing close order is still live; replacement is forbidden until terminal cancel/inactive/fill.")

    position_agreement = _target_position_agreement(config=config, payload=payloads["position_truth"], ready=ready)
    managed_position_agreement = _target_position_agreement(
        config=config,
        payload=payloads["managed_position_registry"],
        ready=ready,
    )
    if position_agreement["conflicting_active_row_count"]:
        blockers.append("Position Truth active rows conflict with requested targeted cancel/replace.")
    if managed_position_agreement["conflicting_active_row_count"]:
        blockers.append("Managed Position Registry active rows conflict with requested targeted cancel/replace.")
    if not position_agreement["matching_row_count"]:
        blockers.append("Position Truth does not show the exact open position for replacement planning.")
    if not managed_position_agreement["matching_row_count"]:
        blockers.append("Managed Position Registry does not show the exact managed position for replacement planning.")

    return {
        "source_authority": "execution_core_authority",
        "dashboard_projection_consumed": False,
        "required": config.require_shared_truth_evidence,
        "max_age_seconds": config.shared_truth_max_age_seconds,
        "artifact_paths": {name: str(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()},
        "classifications": classifications,
        "freshness": freshness,
        "managed_order_match": _jsonable(managed_order_match),
        "order_adjustment_plan_match": _jsonable(plan_match),
        "target_agreement": {
            "position_truth": position_agreement,
            "managed_position_registry": managed_position_agreement,
        },
        "blockers": blockers,
    }


def _matching_managed_order(*, config: ManagedExitCancelReplaceConfig, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    for row in payload.get("managed_orders") or []:
        if isinstance(row, Mapping) and _order_identity_matches_config(config=config, row=row):
            return dict(row)
    return None


def _matching_adjustment_plan(*, config: ManagedExitCancelReplaceConfig, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    for row in payload.get("plans") or []:
        if isinstance(row, Mapping) and _order_identity_matches_config(config=config, row=row):
            return dict(row)
    return None


def _order_identity_matches_config(*, config: ManagedExitCancelReplaceConfig, row: Mapping[str, Any]) -> bool:
    identity = row.get("identity") if isinstance(row.get("identity"), Mapping) else {}
    source_order = row.get("source_order") if isinstance(row.get("source_order"), Mapping) else {}
    broker_order_id = row.get("broker_order_id") or identity.get("broker_order_id") or source_order.get("broker_order_id") or source_order.get("order_id")
    if str(broker_order_id or "") != str(config.broker_order_id):
        return False
    if config.client_id is not None:
        client_id = row.get("client_id") or identity.get("client_id") or source_order.get("client_id")
        if client_id is not None and str(client_id) != str(config.client_id):
            return False
    if config.perm_id is not None:
        perm_id = row.get("perm_id") or identity.get("perm_id") or source_order.get("perm_id")
        if perm_id is not None and str(perm_id) != str(config.perm_id):
            return False
    return True


def _target_position_agreement(
    *,
    config: ManagedExitCancelReplaceConfig,
    payload: Mapping[str, Any],
    ready: Mapping[str, Any],
) -> dict[str, int]:
    proposal = ready.get("proposal") if isinstance(ready.get("proposal"), Mapping) else {}
    replacement = proposal.get("replacement_order") if isinstance(proposal.get("replacement_order"), Mapping) else {}
    rows = _position_rows(payload)
    active = [row for row in rows if _position_row_active(row)]
    matching = [
        row for row in active if _position_row_matches_replacement(config=config, row=row, replacement=replacement)
    ]
    return {
        "row_count": len(rows),
        "active_row_count": len(active),
        "matching_row_count": len(matching),
        "conflicting_active_row_count": len(active) - len(matching),
    }


def _position_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("position_states", "broker_positions", "managed_positions"):
        for row in payload.get(key) or []:
            if isinstance(row, Mapping):
                rows.append(dict(row))
    return rows


def _position_row_active(row: Mapping[str, Any]) -> bool:
    classification = str(row.get("classification") or "").upper()
    if classification in {"FLAT_CLEAN", "NO_MANAGED_POSITIONS", "CLOSED_FLAT"}:
        return False
    status = str(row.get("final_position_status") or row.get("lifecycle_status") or row.get("status") or "").upper()
    return status != "CLOSED_FLAT"


def _position_row_matches_replacement(
    *,
    config: ManagedExitCancelReplaceConfig,
    row: Mapping[str, Any],
    replacement: Mapping[str, Any],
) -> bool:
    symbol = str(replacement.get("symbol") or row.get("symbol") or "").upper()
    row_symbol = str(row.get("symbol") or row.get("instrument_family") or row.get("instrument") or "").upper()
    if symbol and row_symbol and row_symbol != symbol:
        return False
    local_symbol = str(replacement.get("local_symbol") or "").upper()
    row_local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or row.get("contract") or "").upper()
    if local_symbol and row_local_symbol and row_local_symbol != local_symbol:
        return False
    con_id = _decimal(replacement.get("con_id"))
    row_con_id = _decimal(row.get("con_id") or row.get("conId"))
    if con_id is not None and row_con_id is not None and con_id != row_con_id:
        return False
    quantity = _decimal(row.get("signed_quantity") or row.get("broker_quantity") or row.get("quantity") or row.get("qty"))
    replacement_quantity = _decimal(replacement.get("quantity"))
    action = str(replacement.get("action") or "").upper()
    if quantity is not None and replacement_quantity is not None:
        expected_position_quantity = replacement_quantity if action == "SELL" else -replacement_quantity
        if quantity != expected_position_quantity:
            return False
    account = str(row.get("account_id") or row.get("account") or "").strip()
    return not account or account == config.account_id


def _default_adapter_factory(**kwargs: Any) -> ManagedExitAdapter:
    return IbkrPaperAdapter(**kwargs)


def _order_intent_from_known_order(
    *,
    config: ManagedExitCancelReplaceConfig,
    ready: Mapping[str, Any],
    now: datetime,
) -> OrderIntent:
    known = ready["known_order"]
    proposal = ready["proposal"]
    cancel_identity = proposal["cancel_identity"]
    return OrderIntent(
        order_intent_id=f"managed_exit_cancel_existing_{config.broker_order_id}",
        signal_event_id=str(known.get("order_intent_id") or known.get("lifecycle_id") or config.broker_order_id),
        run_id=str(known.get("lifecycle_id") or f"managed_exit_cancel_{config.broker_order_id}"),
        intent_kind=IntentKind.CLOSE,
        account_id=config.account_id,
        symbol=str(cancel_identity.get("symbol") or known.get("symbol")),
        contract_key=ready["contract_key"],
        action=str(cancel_identity.get("action") or known.get("action")),
        quantity=cancel_identity.get("quantity") or known.get("quantity") or "1",
        order_type=str(known.get("order_type") or "LMT"),
        limit_price=known.get("limit_price") or known.get("order_limit_price") or "0",
        time_in_force=str(known.get("tif") or "DAY"),
        paper_only=True,
        created_at=now,
        reason=str(known.get("exit_reason") or "managed_exit_cancel_replace"),
        extra_fields={"guarded_cancel_replace_stage": "cancel_existing"},
    )


def _replacement_order_intent(*, config: ManagedExitCancelReplaceConfig, ready: Mapping[str, Any]) -> OrderIntent:
    known = ready["known_order"]
    replacement = ready["proposal"]["replacement_order"]
    now = datetime.now(UTC)
    return OrderIntent(
        order_intent_id=f"managed_exit_replacement_{known.get('order_intent_id') or config.broker_order_id}_{int(now.timestamp())}",
        signal_event_id=str(known.get("order_intent_id") or known.get("lifecycle_id") or config.broker_order_id),
        run_id=str(known.get("lifecycle_id") or f"managed_exit_replace_{config.broker_order_id}"),
        intent_kind=IntentKind.CLOSE,
        account_id=config.account_id,
        symbol=str(replacement.get("symbol") or known.get("symbol")),
        contract_key=ready["contract_key"],
        action=str(replacement.get("action") or known.get("action")),
        quantity=replacement.get("quantity") or known.get("quantity") or "1",
        order_type=str(replacement.get("order_type") or "LMT"),
        limit_price=replacement.get("limit_price"),
        time_in_force=str(replacement.get("tif") or "DAY"),
        paper_only=True,
        created_at=now,
        reason=str(known.get("exit_reason") or "managed_exit_cancel_replace"),
        extra_fields={"guarded_cancel_replace_stage": "replacement"},
    )


def _submit_attempt(
    *,
    config: ManagedExitCancelReplaceConfig,
    order_intent: OrderIntent,
    ready: Mapping[str, Any],
    now: datetime,
    stage: str,
    broker_order_id: str | None = None,
    perm_id: str | None = None,
) -> SubmitAttempt:
    return SubmitAttempt(
        submit_attempt_id=f"guarded_cancel_replace_{stage}_{order_intent.run_id}_{int(now.timestamp())}",
        order_intent_id=order_intent.order_intent_id,
        run_id=order_intent.run_id,
        account_id=config.account_id,
        broker="IBKR",
        environment={
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.tws_client_id,
            "route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        },
        pre_submit_reconciliation_id=str(ready.get("validated_at") or "guarded_cancel_replace_precheck"),
        open_order_baseline_event_id=f"guarded_cancel_replace_open_orders_{stage}",
        request_digest=f"{order_intent.order_intent_id}:{order_intent.limit_price}",
        state=SubmitAttemptState.CREATED,
        submitted_at=now,
        broker_order_id=broker_order_id,
        perm_id=perm_id or None,
    )


def _replacement_report(
    *,
    classification: str,
    ready: Mapping[str, Any],
    replacement_order_id: str,
    replacement_order: Any,
    replacement_intent: OrderIntent,
    source_report_path: Path,
) -> dict[str, Any]:
    proposal = ready["proposal"]
    known = ready["known_order"]
    order_payload = _jsonable(replacement_order)
    return {
        "classification": classification,
        "source": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        "source_artifact_path": str(source_report_path),
        "original_broker_order_id": known.get("broker_order_id"),
        "broker_order_id": str(getattr(replacement_order, "broker_order_id", None) or order_payload.get("broker_order_id") or replacement_order_id),
        "client_id": getattr(replacement_order, "client_id", None) or order_payload.get("client_id"),
        "perm_id": getattr(replacement_order, "perm_id", None) or order_payload.get("perm_id"),
        "lifecycle_id": known.get("lifecycle_id"),
        "strategy_id": known.get("strategy_id"),
        "lane_id": known.get("lane_id"),
        "order_intent_id": replacement_intent.order_intent_id,
        "account_id": proposal["replacement_order"].get("account_id"),
        "symbol": proposal["replacement_order"].get("symbol"),
        "local_symbol": proposal["replacement_order"].get("local_symbol"),
        "expiry": proposal["replacement_order"].get("expiry"),
        "con_id": proposal["replacement_order"].get("con_id"),
        "action": proposal["replacement_order"].get("action"),
        "quantity": proposal["replacement_order"].get("quantity"),
        "order_type": proposal["replacement_order"].get("order_type"),
        "limit_price": proposal["replacement_order"].get("limit_price"),
        "tif": proposal["replacement_order"].get("tif"),
        "exit_reason": known.get("exit_reason"),
        "submitted_at": datetime.now(UTC).isoformat(),
        "broker_order": order_payload,
    }


def _filled_bridge_result(*, ready: Mapping[str, Any], replacement: Mapping[str, Any], fill: Any) -> dict[str, Any]:
    fill_payload = _jsonable(fill)
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED,
        "intent_type": "SELL_TO_CLOSE" if str(replacement.get("action")).upper() == "SELL" else "BUY_TO_CLOSE",
        "action": replacement.get("action"),
        "instrument": replacement.get("symbol"),
        "symbol": replacement.get("symbol"),
        "strategy_id": replacement.get("strategy_id"),
        "lane_id": replacement.get("lane_id"),
        "order_intent_id": replacement.get("order_intent_id"),
        "account_id": replacement.get("account_id"),
        "quantity": replacement.get("quantity"),
        "fill_price": getattr(fill, "price", None) or fill_payload.get("price"),
        "fill_timestamp": (getattr(fill, "filled_at", None).isoformat() if getattr(fill, "filled_at", None) is not None else fill_payload.get("filled_at")),
        "broker_order_id": getattr(fill, "broker_order_id", None) or fill_payload.get("broker_order_id") or replacement.get("broker_order_id"),
        "perm_id": getattr(fill, "perm_id", None) or fill_payload.get("perm_id") or replacement.get("perm_id"),
        "client_id": replacement.get("client_id"),
        "exec_id": getattr(fill, "execution_id", None) or fill_payload.get("execution_id"),
        "execution_id": getattr(fill, "execution_id", None) or fill_payload.get("execution_id"),
        "local_symbol": replacement.get("local_symbol"),
        "con_id": replacement.get("con_id"),
        "contract": {
            "symbol": replacement.get("symbol"),
            "local_symbol": replacement.get("local_symbol"),
            "expiry": replacement.get("expiry"),
            "qualified_contract_identifier": replacement.get("con_id"),
        },
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
        "created_at": datetime.now(UTC).isoformat(),
        "source": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
    }


def _persist_known_managed_exit_order(
    *,
    config: ManagedExitCancelReplaceConfig,
    ready: Mapping[str, Any],
    replacement: Mapping[str, Any],
) -> None:
    path = config.resolved_known_managed_exit_state_path
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
        "source": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        "source_artifact_path": str(_report_path(config)),
        "lifecycle_id": replacement.get("lifecycle_id"),
        "strategy_id": replacement.get("strategy_id"),
        "lane_id": replacement.get("lane_id"),
        "order_intent_id": replacement.get("order_intent_id"),
        "broker_order_id": replacement.get("broker_order_id"),
        "client_id": replacement.get("client_id"),
        "perm_id": replacement.get("perm_id"),
        "symbol": replacement.get("symbol"),
        "local_symbol": replacement.get("local_symbol"),
        "expiry": replacement.get("expiry"),
        "con_id": replacement.get("con_id"),
        "action": replacement.get("action"),
        "quantity": replacement.get("quantity"),
        "order_type": replacement.get("order_type"),
        "limit_price": replacement.get("limit_price"),
        "tif": replacement.get("tif"),
        "exit_reason": replacement.get("exit_reason"),
        "submitted_at": replacement.get("submitted_at"),
        "original_order": {
            "broker_order_id": ready["known_order"].get("broker_order_id"),
            "client_id": ready["known_order"].get("client_id"),
            "perm_id": ready["known_order"].get("perm_id"),
        },
    }
    payload = {
        "schema_version": "track_b_known_managed_exit_orders_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "known_managed_exit_orders": [row],
    }
    _write_json(path, payload)


def _contract_allowlist_entry(proposal: Mapping[str, Any]) -> dict[str, Any]:
    replacement = proposal["replacement_order"]
    return {
        "symbol": replacement.get("symbol"),
        "security_type": "FUT",
        "exchange": "COMEX",
        "currency": "USD",
        "local_symbol": replacement.get("local_symbol"),
        "con_id": replacement.get("con_id"),
        "expiry": replacement.get("expiry"),
        "contract_month": str(replacement.get("expiry") or "")[:6],
        "multiplier": "100" if str(replacement.get("symbol")).upper() == "GC" else "10",
        "tick_size": str(replacement.get("min_tick") or "0.1"),
    }


def _contract_key(proposal: Mapping[str, Any]) -> str:
    replacement = proposal["replacement_order"]
    symbol = str(replacement.get("symbol") or "").upper()
    expiry = str(replacement.get("expiry") or "")[:6]
    return f"{symbol}-{expiry}"


def _matching_open_order(broker_order_id: str, open_orders: Any) -> dict[str, Any] | None:
    for row in open_orders:
        if isinstance(row, Mapping) and str(row.get("broker_order_id") or row.get("order_id") or "") == str(broker_order_id):
            return dict(row)
    return None


def _order_id_in_open_orders(broker_order_id: str, open_orders: Any) -> bool:
    return _matching_open_order(broker_order_id, open_orders) is not None


def _blocked(classification: str, detail: str) -> dict[str, Any]:
    return {"classification": classification, "detail": detail}


def _safe_adapter_diagnostics(adapter: Any) -> dict[str, Any]:
    try:
        result = adapter.submit_diagnostics()
    except Exception:
        return {}
    return dict(result or {})


def _camel(key: str) -> str:
    parts = key.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    if hasattr(value, "to_json_dict"):
        return value.to_json_dict()
    return to_jsonable(value)


def _classification(payload: Mapping[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    return str(
        payload.get("classification")
        or summary.get("classification")
        or summary.get("overall_classification")
        or ""
    )


def _freshness(*, payload: Mapping[str, Any], now: datetime, max_age_seconds: float) -> dict[str, Any]:
    generated_at = _parse_iso(payload.get("generated_at"))
    if generated_at is None:
        return {"generated_at": None, "age_seconds": None, "stale_or_missing": True}
    age_seconds = max(0.0, (now - generated_at).total_seconds())
    return {
        "generated_at": generated_at.isoformat(),
        "age_seconds": age_seconds,
        "stale_or_missing": age_seconds > float(max_age_seconds),
    }


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _resolve(*, repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _report_path(config: ManagedExitCancelReplaceConfig) -> Path:
    return config.resolved_output_dir / "track_b_managed_exit_cancel_replace_report.json"


def _filled_result_path(config: ManagedExitCancelReplaceConfig) -> Path:
    return config.resolved_output_dir / "track_b_managed_exit_cancel_replace_filled_bridge_result.json"


def _write_report(config: ManagedExitCancelReplaceConfig, report: Mapping[str, Any]) -> None:
    _write_json(_report_path(config), report)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
