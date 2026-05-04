"""Operator-gated Track B paper-proof submit harness.

This layer orchestrates the real paper-proof shape without performing any
broker submit by itself. Tests inject fake preflight/proof runners; the real
submit runner is intentionally not wired in this slice.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Mapping

from .harness import HarnessConfig, HarnessResult
from .ibkr_paper_adapter import IbkrPaperAdapter
from .ledger import JsonlLedger
from .models import Action, BrokerOrder, FillEvent, IntentKind, OrderIntent, PositionSource, PositionState, SignalEvent, SubmitAttempt, SubmitAttemptState, TerminalClassification, to_jsonable
from .pricing import QuoteObservation, create_marketable_limit_decision
from .preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig
from .readiness import FinalReadinessVerdict, OperatorReadiness, blocked_readiness, ready_for_paper_proof
from .session_guard import ProofTimingDecision, evaluate_proof_timing


DEFAULT_PAPER_PROOF_OUTPUT_ROOT = Path("outputs/track_b_execution_core/paper_proof")


class PaperProofConfigError(ValueError):
    """Raised when the operator submit-proof request is not explicit enough."""


class _PaperProofLifecycleStop(RuntimeError):
    def __init__(self, *, classification: TerminalClassification, lifecycle_status: str, reason: str, required_action: str) -> None:
        super().__init__(reason)
        self.classification = classification
        self.lifecycle_status = lifecycle_status
        self.reason = reason
        self.required_action = required_action


PreflightRunner = Callable[[ReadOnlyPreflightConfig, str], PreflightResult]
ProofRunner = Callable[[HarnessConfig, str], HarnessResult]


@dataclass(frozen=True)
class PaperProofConfig:
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 77
    account_id: str = "DU1234567"
    contract_key: str = "MGC-202606"
    side: str = "BUY"
    quantity: int = 1
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    output_root: Path = DEFAULT_PAPER_PROOF_OUTPUT_ROOT
    submit_enabled: bool = False
    confirm_paper_submit: bool = False
    allow_delayed_data_for_paper_proof: bool = False
    manual_open_limit_price: str | Decimal | None = None
    manual_close_limit_price: str | Decimal | None = None
    manual_limit_price: str | Decimal | None = None
    proof_timing_status: str = "UNKNOWN"
    proof_timing_source: str = "operator_config"
    proof_timing_detail: str | None = None
    contract_allowlist: dict[str, dict[str, object]] | None = None

    def to_report_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": self.account_id,
            "contract_key": self.contract_key,
            "side": self.side,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "output_root": str(self.output_root),
            "submit_enabled": self.submit_enabled,
            "confirm_paper_submit": self.confirm_paper_submit,
            "allow_delayed_data_for_paper_proof": self.allow_delayed_data_for_paper_proof,
            "manual_open_limit_price": str(self.manual_open_limit_price) if self.manual_open_limit_price is not None else None,
            "manual_close_limit_price": str(self.manual_close_limit_price) if self.manual_close_limit_price is not None else None,
            "manual_limit_price": str(self.manual_limit_price) if self.manual_limit_price is not None else None,
            "proof_timing_status": self.proof_timing_status,
            "proof_timing_source": self.proof_timing_source,
            "proof_timing_detail": self.proof_timing_detail,
        }


@dataclass(frozen=True)
class PaperProofResult:
    run_id: str
    classification: TerminalClassification
    report_json: Path
    report_md: Path
    report: dict[str, object]
    proof_result: HarnessResult | None = None


def run_paper_proof(
    *,
    config: PaperProofConfig,
    preflight_runner: PreflightRunner,
    proof_runner: ProofRunner | None = None,
    run_id: str | None = None,
) -> PaperProofResult:
    actual_run_id = run_id or f"paper_proof_{uuid.uuid4().hex}"
    output_dir = Path(config.output_root) / actual_run_id
    report_json = output_dir / "paper_proof_report.json"
    report_md = output_dir / "paper_proof_report.md"

    config_error = _validate_config(config)
    if config_error is not None:
        return _write_result(
            run_id=actual_run_id,
            config=config,
            report_json=report_json,
            report_md=report_md,
            classification=TerminalClassification.BLOCKED,
            reason=config_error,
            required_action="Fix explicit paper-proof submit config.",
        )

    timing = evaluate_proof_timing(
        status=config.proof_timing_status,
        source=config.proof_timing_source,
        detail=config.proof_timing_detail,
    )
    if not timing.allowed:
        return _write_result(
            run_id=actual_run_id,
            config=config,
            report_json=report_json,
            report_md=report_md,
            classification=TerminalClassification.BLOCKED,
            reason=timing.reason,
            required_action=timing.required_action,
            timing=timing,
        )

    preflight_config = ReadOnlyPreflightConfig(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_key=config.contract_key,
        output_root=Path(config.output_root) / "preflight",
        observe_quote=True,
        contract_allowlist=config.contract_allowlist or ReadOnlyPreflightConfig().contract_allowlist,
    )
    preflight = preflight_runner(preflight_config, f"{actual_run_id}_preflight")
    preflight_blocker = _preflight_blocker(config=config, preflight=preflight)
    if preflight_blocker is not None:
        return _write_result(
            run_id=actual_run_id,
            config=config,
            report_json=report_json,
            report_md=report_md,
            classification=TerminalClassification.BLOCKED,
            reason=preflight_blocker,
            required_action="Resolve read-only preflight blocker before paper submit.",
            preflight=preflight,
        )

    proof_config = HarnessConfig(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_key=config.contract_key,
        side=config.side,
        quantity=config.quantity,
        order_type=config.order_type,
        time_in_force=config.time_in_force,
        output_root=Path(config.output_root) / "proof_runs",
        contract_allowlist=config.contract_allowlist or HarnessConfig().contract_allowlist,
    )
    actual_proof_runner = proof_runner or (
        lambda cfg, rid: run_ibkr_paper_proof(
            config=cfg,
            run_id=rid,
            preflight=preflight,
            manual_open_limit_price=config.manual_open_limit_price,
            manual_close_limit_price=config.manual_close_limit_price,
        )
    )
    proof = actual_proof_runner(proof_config, actual_run_id)
    proof_payload = _read_json(proof.proof_report_json)
    classification = _classification_from_proof(proof, proof_payload)
    return _write_result(
        run_id=actual_run_id,
        config=config,
        report_json=report_json,
        report_md=report_md,
        classification=classification,
        reason=proof_payload.get("failure_or_ambiguity"),
        required_action=proof_payload.get("required_manual_action"),
        preflight=preflight,
        proof=proof,
        proof_payload=proof_payload,
        timing=timing,
    )


def _validate_config(config: PaperProofConfig) -> str | None:
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
    if not config.submit_enabled:
        return "submit_enabled=True is required for paper-proof submit"
    if not config.confirm_paper_submit:
        return "confirm_paper_submit is required"
    if int(config.quantity) != 1:
        return "quantity must be exactly 1"
    if str(config.order_type).upper() != "LMT":
        return "order_type must be LMT"
    if str(config.time_in_force).upper() != "DAY":
        return "time_in_force must be DAY"
    if config.manual_limit_price is not None:
        return "manual_limit_price is deprecated for paper proof; use manual_open_limit_price and manual_close_limit_price"
    if _has_any_manual_price(config):
        if not config.allow_delayed_data_for_paper_proof:
            return "manual limit prices require delayed-data paper-proof approval"
        manual_price_error = _manual_price_pair_error(config)
        if manual_price_error is not None:
            return manual_price_error
    return None


def _preflight_blocker(*, config: PaperProofConfig, preflight: PreflightResult) -> str | None:
    report = preflight.report
    if preflight.classification != PreflightClassification.READY_READ_ONLY:
        return f"read-only preflight was not ready: {preflight.classification.value}"
    if str(report.get("config", {}).get("account_id") or "") != config.account_id:
        return "preflight account_id does not match configured paper account"
    if not _check_passed(report, "managed_account_exact_match"):
        return "configured paper account did not match managed accounts"
    if str(report.get("contract_key") or "") != config.contract_key:
        return "preflight contract_key does not match configured contract"
    if not _check_passed(report, "contract_qualified"):
        return "exact allowlisted contract was not qualified"
    position = report.get("position") or {}
    if isinstance(position, Mapping) and int(Decimal(str(position.get("signed_quantity", "0")))) != 0:
        return "proof contract is not flat"
    if report.get("unresolved_broker_order_detected"):
        return "unresolved broker order blocks same account/contract submit"
    if report.get("open_orders"):
        return "proof contract has existing open orders"
    account_open_orders = report.get("account_open_orders") or report.get("account_wide_open_orders")
    if account_open_orders:
        return "account-wide open orders exist"
    mode = str(report.get("market_data_mode") or "UNKNOWN").upper()
    quote_observed = bool(report.get("quote_observed"))
    if not quote_observed:
        if not _has_manual_price_pair(config):
            return "pricing-dependent proof submit requires an observed quote or separate manual open/close limit prices"
        if mode != "DELAYED":
            return "manual limit price proof requires delayed-data paper context"
    if mode == "DELAYED" and not config.allow_delayed_data_for_paper_proof:
        return "delayed market data requires explicit paper-proof approval"
    if mode == "UNKNOWN":
        return "unknown market data mode cannot price paper-proof submit"
    return None


def _check_passed(report: Mapping[str, object], name: str) -> bool:
    for check in report.get("checks", []) or []:
        if isinstance(check, Mapping) and check.get("name") == name:
            return bool(check.get("passed"))
    return False


def _classification_from_proof(proof: HarnessResult, proof_payload: Mapping[str, object]) -> TerminalClassification:
    if proof.classification != TerminalClassification.PASSED:
        return proof.classification
    if proof_payload.get("proof_lifecycle_status") == "PROOF_COMPLETE_FLAT":
        return TerminalClassification.PASSED
    if not _proof_pass_chain_complete(proof_payload):
        return TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    return TerminalClassification.PASSED


def _proof_pass_chain_complete(proof_payload: Mapping[str, object]) -> bool:
    required_rows = (
        "open_intent",
        "open_submit_attempt",
        "open_broker_order",
        "open_fill",
        "close_intent",
        "close_submit_attempt",
        "close_broker_order",
        "close_fill",
        "final_reconciliation",
    )
    for key in required_rows:
        if not isinstance(proof_payload.get(key), Mapping):
            return False
    for key in ("open_broker_order", "close_broker_order"):
        row = proof_payload[key]
        if not row.get("broker_order_id") or not row.get("perm_id"):
            return False
    for key in ("open_fill", "close_fill"):
        row = proof_payload[key]
        if not row.get("fill_event_id") or not row.get("execution_id") or not row.get("broker_order_id"):
            return False
    final = proof_payload["final_reconciliation"]
    return final.get("status") == "CLEAN"


def _has_any_manual_price(config: PaperProofConfig) -> bool:
    return config.manual_open_limit_price is not None or config.manual_close_limit_price is not None


def _has_manual_price_pair(config: PaperProofConfig) -> bool:
    return config.manual_open_limit_price is not None and config.manual_close_limit_price is not None


def _manual_price_pair_error(config: PaperProofConfig) -> str | None:
    if config.manual_open_limit_price is None:
        return "manual_open_limit_price is required when using manual paper-proof pricing"
    if config.manual_close_limit_price is None:
        return "manual_close_limit_price is required when using manual paper-proof pricing"
    open_error = _manual_limit_price_error(
        value=config.manual_open_limit_price,
        label="manual_open_limit_price",
        config=config,
    )
    if open_error is not None:
        return open_error
    return _manual_limit_price_error(
        value=config.manual_close_limit_price,
        label="manual_close_limit_price",
        config=config,
    )


def _manual_limit_price_error(*, value: str | Decimal, label: str, config: PaperProofConfig) -> str | None:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return f"{label} must be a positive decimal"
    if not price.is_finite() or price <= 0:
        return f"{label} must be positive"
    allowlist = config.contract_allowlist or HarnessConfig().contract_allowlist
    entry = allowlist.get(config.contract_key)
    if entry is None:
        return f"{label} requires exact allowlisted contract"
    raw_tick_size = entry.get("tick_size")
    if raw_tick_size is None:
        return f"{label} requires configured contract tick_size"
    try:
        tick_size = Decimal(str(raw_tick_size))
    except (InvalidOperation, ValueError):
        return f"{label} requires valid contract tick_size"
    if not tick_size.is_finite() or tick_size <= 0:
        return f"{label} requires positive contract tick_size"
    if price % tick_size != 0:
        return f"{label} must be valid for contract tick_size"
    return None


def _write_result(
    *,
    run_id: str,
    config: PaperProofConfig,
    report_json: Path,
    report_md: Path,
    classification: TerminalClassification,
    reason: object | None,
    required_action: object | None,
    preflight: PreflightResult | None = None,
    proof: HarnessResult | None = None,
    proof_payload: Mapping[str, object] | None = None,
    timing: ProofTimingDecision | None = None,
) -> PaperProofResult:
    preflight_report = preflight.report if preflight is not None else {}
    timing_report = timing.to_report_dict() if timing is not None else {}
    submit_attempted = proof is not None
    operator_readiness = _operator_readiness_report(
        config=config,
        classification=classification,
        reason=reason,
        required_action=required_action,
        preflight_report=preflight_report,
        timing_report=timing_report,
        submit_attempted=submit_attempted,
    )
    report: dict[str, object] = {
        "schema_version": "track_b_paper_proof_submit_harness_v1",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "classification": classification.value,
        "config": config.to_report_dict(),
        "preflight_classification": preflight.classification.value if preflight is not None else None,
        "preflight_report_json": str(preflight.report_json) if preflight is not None else None,
        "preflight_report_md": str(preflight.report_md) if preflight is not None else None,
        "proof_report_json": str(proof.proof_report_json) if proof is not None else None,
        "proof_report_md": str(proof.proof_report_md) if proof is not None else None,
        "market_data_provider": preflight_report.get("market_data_provider"),
        "market_data_mode": preflight_report.get("market_data_mode"),
        "market_data_role": preflight_report.get("market_data_role"),
        "delayed_data_warning_seen": preflight_report.get("delayed_data_warning_seen"),
        "quote_observed": preflight_report.get("quote_observed"),
        "manual_open_limit_price": str(config.manual_open_limit_price) if config.manual_open_limit_price is not None else None,
        "manual_close_limit_price": str(config.manual_close_limit_price) if config.manual_close_limit_price is not None else None,
        "manual_limit_price": str(config.manual_limit_price) if config.manual_limit_price is not None else None,
        "pricing_source": "OPERATOR_SUPPLIED_MANUAL_LIMIT" if _has_any_manual_price(config) else None,
        "operator_manual_price_acknowledgement": _has_any_manual_price(config),
        "paper_route_readiness": preflight_report.get("paper_route_readiness"),
        "production_live_money_readiness": False,
        **timing_report,
        "proof_submit_attempted": submit_attempted,
        **operator_readiness.to_report_dict(),
        "unresolved_broker_order_detected": preflight_report.get("unresolved_broker_order_detected", False),
        "unresolved_broker_order_status": preflight_report.get("unresolved_broker_order_status"),
        "unresolved_broker_order_id": preflight_report.get("unresolved_broker_order_id"),
        "unresolved_broker_perm_id": preflight_report.get("unresolved_broker_perm_id"),
        "unresolved_remaining_quantity": preflight_report.get("unresolved_remaining_quantity"),
        "blocks_same_account_contract_submit": preflight_report.get("blocks_same_account_contract_submit", False),
        "next_required_action": preflight_report.get("next_required_action"),
        "failure_or_ambiguity": reason,
        "required_manual_action": required_action,
        "proof_payload": dict(proof_payload or {}),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    report_md.write_text(_render_markdown(report), encoding="utf-8")
    return PaperProofResult(
        run_id=run_id,
        classification=classification,
        report_json=report_json,
        report_md=report_md,
        report=report,
        proof_result=proof,
    )


def _operator_readiness_report(
    *,
    config: PaperProofConfig,
    classification: TerminalClassification,
    reason: object | None,
    required_action: object | None,
    preflight_report: Mapping[str, object],
    timing_report: Mapping[str, object],
    submit_attempted: bool,
) -> OperatorReadiness:
    position = preflight_report.get("position")
    position_qty = position.get("signed_quantity") if isinstance(position, Mapping) else None
    timing_classification = timing_report.get("proof_timing_classification")
    timing_allowed = timing_report.get("proof_timing_allowed")
    if timing_classification == "PROOF_TIMING_BLOCKED_OUTSIDE_ACTIVE_SESSION":
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_OUTSIDE_ACTIVE_SESSION,
            primary_blocker=str(reason or "Proof timing is outside an active session."),
            required_next_action=str(required_action or timing_report.get("proof_timing_required_action") or "Wait for an active session."),
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=timing_allowed,
            submit_attempted=submit_attempted,
        )
    if timing_classification == "PROOF_TIMING_UNKNOWN_BLOCKED":
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_UNKNOWN_PROOF_TIMING,
            primary_blocker=str(reason or "Proof timing is unknown."),
            required_next_action=str(required_action or timing_report.get("proof_timing_required_action") or "Provide active-session timing evidence."),
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=timing_allowed,
            submit_attempted=submit_attempted,
        )
    if preflight_report.get("unresolved_broker_order_detected"):
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_UNRESOLVED_BROKER_ORDER,
            primary_blocker=str(reason or "Unresolved broker order blocks same account/contract submit."),
            required_next_action=str(required_action or preflight_report.get("next_required_action") or "Wait for terminal broker order state."),
            account_id=config.account_id,
            contract_key=config.contract_key,
            broker_order_id=preflight_report.get("unresolved_broker_order_id"),
            perm_id=preflight_report.get("unresolved_broker_perm_id"),
            broker_status=preflight_report.get("unresolved_broker_order_status"),
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=timing_allowed,
            submit_attempted=submit_attempted,
        )
    if position_qty not in (None, 0, "0", "0.0"):
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_NON_FLAT_POSITION,
            primary_blocker=str(reason or "Proof contract position is not flat."),
            required_next_action=str(required_action or "Flatten or reconcile the account before proof."),
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=timing_allowed,
            submit_attempted=submit_attempted,
        )
    if classification == TerminalClassification.PASSED:
        return ready_for_paper_proof(
            account_id=config.account_id,
            contract_key=config.contract_key,
            position_qty=position_qty,
            submit_attempted=submit_attempted,
            required_next_action="Paper proof completed; no further submit is needed for this run.",
            proof_timing_classification=timing_classification,
            proof_timing_allowed=timing_allowed,
        )
    blocker = str(reason or "Paper proof is blocked or ambiguous.")
    lowered = blocker.lower()
    verdict = FinalReadinessVerdict.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    if "account" in lowered or "contract" in lowered or "allowlist" in lowered:
        verdict = FinalReadinessVerdict.BLOCKED_CONTRACT_OR_ACCOUNT_MISMATCH
    elif "market data" in lowered or "quote" in lowered or "pricing" in lowered:
        verdict = FinalReadinessVerdict.BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE
    return blocked_readiness(
        verdict=verdict,
        primary_blocker=blocker,
        required_next_action=str(required_action or "Resolve paper-proof blocker before any Track B submit."),
        account_id=config.account_id,
        contract_key=config.contract_key,
        position_qty=position_qty,
        proof_timing_classification=timing_classification,
        proof_timing_allowed=timing_allowed,
        submit_attempted=submit_attempted,
    )


def _render_markdown(report: Mapping[str, object]) -> str:
    return "\n".join(
        [
            "# Track B Paper Proof Submit Harness",
            "",
            "## Classification",
            str(report.get("classification")),
            "",
            "## Readiness",
            json.dumps(
                {
                    "preflight_classification": report.get("preflight_classification"),
                    "market_data_provider": report.get("market_data_provider"),
                    "market_data_mode": report.get("market_data_mode"),
                    "paper_route_readiness": report.get("paper_route_readiness"),
                    "production_live_money_readiness": report.get("production_live_money_readiness"),
                    "proof_timing_classification": report.get("proof_timing_classification"),
                    "proof_timing_allowed": report.get("proof_timing_allowed"),
                    "proof_submit_attempted": report.get("proof_submit_attempted"),
                    "final_readiness_verdict": report.get("final_readiness_verdict"),
                    "submit_allowed": report.get("submit_allowed"),
                    "primary_blocker": report.get("primary_blocker"),
                    "required_next_action": report.get("required_next_action"),
                },
                indent=2,
                sort_keys=True,
            ),
            "",
            "## Reports",
            json.dumps(
                {
                    "preflight_report_json": report.get("preflight_report_json"),
                    "proof_report_json": report.get("proof_report_json"),
                },
                indent=2,
                sort_keys=True,
            ),
            "",
            "## Failure Or Ambiguity",
            str(report.get("failure_or_ambiguity")),
            "",
        ]
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def run_ibkr_paper_proof(
    *,
    config: HarnessConfig,
    run_id: str,
    preflight: PreflightResult,
    manual_open_limit_price: str | Decimal | None = None,
    manual_close_limit_price: str | Decimal | None = None,
    adapter: IbkrPaperAdapter | None = None,
) -> HarnessResult:
    """Run the real IBKR paper proof path after paper_proof gates have passed."""

    run_dir = Path(config.output_root) / run_id
    ledger = JsonlLedger(run_dir / "ledger.jsonl")
    proof_json = run_dir / "proof_report.json"
    proof_md = run_dir / "proof_report.md"
    now = datetime.now(timezone.utc)
    actual_adapter = adapter or IbkrPaperAdapter(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_allowlist=config.contract_allowlist,
        submit_enabled=True,
    )
    events: list[dict[str, object]] = []
    submitted_attempt_ids: list[str] = []
    lifecycle_status = "CREATED"
    lifecycle_required_action: str | None = None

    def append(event_type: str, payload: Mapping[str, object]) -> None:
        event = ledger.append_event(run_id=run_id, event_type=event_type, payload=dict(payload))
        events.append({"event_id": event.event_id, "event_type": event.event_type, "sequence": event.sequence})

    def set_lifecycle(status: str, **payload: object) -> None:
        nonlocal lifecycle_status
        lifecycle_status = status
        append("paper_proof_lifecycle_status", {"proof_lifecycle_status": status, **payload})

    def append_submit_diagnostics(submit_attempt_id: str) -> None:
        diagnostic_method = getattr(actual_adapter, "submit_diagnostics", None)
        if not callable(diagnostic_method):
            return
        diagnostics = diagnostic_method(submit_attempt_id)
        if diagnostics:
            append("submit_diagnostics_created", diagnostics)

    append("run_started", {"run_id": run_id})
    append("config_loaded", config.to_report_dict())
    append("config_validated", {"mode": config.mode, "submit_enabled": True})
    try:
        actual_adapter.connect()
        append("broker_connected", {"broker": "IBKR", "environment": config.environment()})
        accounts = actual_adapter.managed_accounts()
        actual_adapter.require_configured_account()
        append("account_validated", {"account_id": config.account_id, "managed_accounts": accounts})
        contract = actual_adapter.qualify_contract(run_id=run_id, contract_key=config.contract_key, now=now)
        append("contract_qualified", contract)

        if manual_open_limit_price is not None and manual_close_limit_price is not None:
            open_limit_price = Decimal(str(manual_open_limit_price))
            close_limit_price = Decimal(str(manual_close_limit_price))
            append(
                "pricing_decision_created",
                {
                    "pricing_decision_id": f"pricing_{run_id}_manual_open",
                    "pricing_source": "OPERATOR_SUPPLIED_MANUAL_LIMIT",
                    "manual_open_limit_price": str(open_limit_price),
                    "market_data_provider": preflight.report.get("market_data_provider") or "IBKR",
                    "market_data_mode": preflight.report.get("market_data_mode") or "DELAYED",
                    "market_data_role": preflight.report.get("market_data_role") or "DIAGNOSTIC",
                    "paper_only": True,
                    "operator_manual_price_acknowledgement": True,
                },
            )
        else:
            preflight_quote = preflight.report.get("quote")
            if not isinstance(preflight_quote, Mapping):
                raise RuntimeError("preflight quote is required for paper proof pricing")
            quote = QuoteObservation(
                quote_id=f"quote_{run_id}_preflight",
                run_id=run_id,
                contract_key=config.contract_key,
                source=str(preflight_quote.get("source") or "preflight"),
                bid=preflight_quote.get("bid"),
                ask=preflight_quote.get("ask"),
                last=preflight_quote.get("last"),
                observed_at=now,
                market_data_provider=str(preflight.report.get("market_data_provider") or "IBKR"),
                market_data_mode=str(preflight.report.get("market_data_mode") or "UNKNOWN"),
                market_data_role=str(preflight.report.get("market_data_role") or "DIAGNOSTIC"),
                delayed_data_warning_seen=bool(preflight.report.get("delayed_data_warning_seen")),
                tick_size=config.contract_allowlist[config.contract_key].get("tick_size"),
                exchange=config.contract_allowlist[config.contract_key].get("exchange"),
                currency=config.contract_allowlist[config.contract_key].get("currency"),
                raw={"source": "preflight_report", "preflight_run_id": preflight.run_id},
            )
            ledger.append_model_event(event_type="quote_observed", model=quote)
            open_pricing = create_marketable_limit_decision(
                pricing_decision_id=f"pricing_{run_id}_open",
                quote=quote,
                action=config.side,
                tick_size=config.contract_allowlist[config.contract_key].get("tick_size"),
                fill_offset_ticks=config.fill_offset_ticks,
                max_quote_age_seconds=config.max_quote_age_seconds,
                max_distance_ticks=config.max_distance_ticks,
                max_distance_percent=config.max_distance_percent,
                now=now,
            )
            ledger.append_model_event(event_type="pricing_decision_created", model=open_pricing)
            open_limit_price = open_pricing.limit_price

        signal = _signal(config=config, run_id=run_id, now=now)
        open_intent = _intent(config=config, run_id=run_id, signal=signal, kind=IntentKind.OPEN, action=Action(config.side), limit_price=open_limit_price, now=now, index=1)
        open_submit = _submit(config=config, run_id=run_id, intent=open_intent, now=now, index=1)
        ledger.append_model_event(event_type="signal_event_created", model=signal)
        ledger.append_model_event(event_type="order_intent_created", model=open_intent)
        ledger.append_model_event(event_type="submit_attempt_created", model=open_submit)
        submitted_attempt_ids.append(open_submit.submit_attempt_id)
        actual_adapter.submit_limit_order(submit_attempt=open_submit, order_intent=open_intent)
        set_lifecycle("OPEN_SUBMITTED", submit_attempt_id=open_submit.submit_attempt_id)
        try:
            open_order = actual_adapter.wait_for_broker_order(submit_attempt_id=open_submit.submit_attempt_id)
            ledger.append_model_event(event_type="broker_order_observed", model=open_order)
            open_fill = actual_adapter.wait_for_fill(submit_attempt_id=open_submit.submit_attempt_id)
            ledger.append_model_event(event_type="fill_event_created", model=open_fill)
            set_lifecycle("OPEN_FILLED", submit_attempt_id=open_submit.submit_attempt_id, fill_source="broker_callback")
        except Exception as exc:  # noqa: BLE001 - reconcile broker truth before deciding whether close is safe.
            append_submit_diagnostics(open_submit.submit_attempt_id)
            close_guard = _evaluate_close_only_guard(
                adapter=actual_adapter,
                config=config,
                run_id=run_id,
                expected_signed_quantity=1 if open_intent.action == Action.BUY else -1,
                now=now,
                stage="PRE_CLOSE_AFTER_OPEN_CALLBACK_GAP",
            )
            append("close_only_guard_evaluated", close_guard)
            if not close_guard["close_allowed"]:
                raise _PaperProofLifecycleStop(
                    classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                    lifecycle_status=str(close_guard["proof_lifecycle_status"]),
                    reason=str(close_guard["primary_blocker"] or exc),
                    required_action=str(close_guard["required_next_action"]),
                ) from exc
            set_lifecycle(
                "OPEN_FILLED",
                submit_attempt_id=open_submit.submit_attempt_id,
                fill_source="broker_position_truth",
                missing_callback_reason=str(exc),
            )

        close_action = Action.SELL if open_intent.action == Action.BUY else Action.BUY
        close_guard = _evaluate_close_only_guard(
            adapter=actual_adapter,
            config=config,
            run_id=run_id,
            expected_signed_quantity=1 if close_action == Action.SELL else -1,
            now=now,
            stage="PRE_CLOSE",
        )
        append("close_only_guard_evaluated", close_guard)
        if not close_guard["close_allowed"]:
            raise _PaperProofLifecycleStop(
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                lifecycle_status=str(close_guard["proof_lifecycle_status"]),
                reason=str(close_guard["primary_blocker"]),
                required_action=str(close_guard["required_next_action"]),
            )
        if manual_open_limit_price is not None and manual_close_limit_price is not None:
            append(
                "pricing_decision_created",
                {
                    "pricing_decision_id": f"pricing_{run_id}_manual_close",
                    "pricing_source": "OPERATOR_SUPPLIED_MANUAL_LIMIT",
                    "manual_close_limit_price": str(close_limit_price),
                    "market_data_provider": preflight.report.get("market_data_provider") or "IBKR",
                    "market_data_mode": preflight.report.get("market_data_mode") or "DELAYED",
                    "market_data_role": preflight.report.get("market_data_role") or "DIAGNOSTIC",
                    "paper_only": True,
                    "operator_manual_price_acknowledgement": True,
                },
            )
        else:
            close_pricing = create_marketable_limit_decision(
                pricing_decision_id=f"pricing_{run_id}_close",
                quote=quote,
                action=close_action,
                tick_size=config.contract_allowlist[config.contract_key].get("tick_size"),
                fill_offset_ticks=config.fill_offset_ticks,
                max_quote_age_seconds=config.max_quote_age_seconds,
                max_distance_ticks=config.max_distance_ticks,
                max_distance_percent=config.max_distance_percent,
                now=now,
            )
            ledger.append_model_event(event_type="pricing_decision_created", model=close_pricing)
            close_limit_price = close_pricing.limit_price
        close_intent = _intent(config=config, run_id=run_id, signal=signal, kind=IntentKind.CLOSE, action=close_action, limit_price=close_limit_price, now=now, index=2)
        close_submit = _submit(config=config, run_id=run_id, intent=close_intent, now=now, index=2)
        ledger.append_model_event(event_type="order_intent_created", model=close_intent)
        ledger.append_model_event(event_type="submit_attempt_created", model=close_submit)
        submitted_attempt_ids.append(close_submit.submit_attempt_id)
        actual_adapter.submit_limit_order(submit_attempt=close_submit, order_intent=close_intent)
        set_lifecycle("CLOSE_SUBMITTED", submit_attempt_id=close_submit.submit_attempt_id)
        try:
            close_order = actual_adapter.wait_for_broker_order(submit_attempt_id=close_submit.submit_attempt_id)
            ledger.append_model_event(event_type="broker_order_observed", model=close_order)
            close_fill = actual_adapter.wait_for_fill(submit_attempt_id=close_submit.submit_attempt_id)
            ledger.append_model_event(event_type="fill_event_created", model=close_fill)
            set_lifecycle("CLOSE_FILLED", submit_attempt_id=close_submit.submit_attempt_id, fill_source="broker_callback")
        except Exception as exc:  # noqa: BLE001 - if close was sent, never retry; reconcile final broker truth.
            append_submit_diagnostics(close_submit.submit_attempt_id)
            final_guard = _evaluate_flat_after_close_guard(
                adapter=actual_adapter,
                config=config,
                run_id=run_id,
                now=now,
                stage="FINAL_AFTER_CLOSE_CALLBACK_GAP",
            )
            append("flat_after_close_guard_evaluated", final_guard)
            if not final_guard["flat_clean"]:
                raise _PaperProofLifecycleStop(
                    classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                    lifecycle_status=str(final_guard["proof_lifecycle_status"]),
                    reason=str(final_guard["primary_blocker"] or exc),
                    required_action=str(final_guard["required_next_action"]),
                ) from exc
            set_lifecycle(
                "CLOSE_FILLED",
                submit_attempt_id=close_submit.submit_attempt_id,
                fill_source="broker_position_truth",
                missing_callback_reason=str(exc),
            )
        final_guard = _evaluate_flat_after_close_guard(
            adapter=actual_adapter,
            config=config,
            run_id=run_id,
            now=now,
            stage="FINAL",
        )
        append("flat_after_close_guard_evaluated", final_guard)
        if not final_guard["flat_clean"]:
            raise _PaperProofLifecycleStop(
                classification=TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
                lifecycle_status=str(final_guard["proof_lifecycle_status"]),
                reason=str(final_guard["primary_blocker"]),
                required_action=str(final_guard["required_next_action"]),
            )
        set_lifecycle("PROOF_COMPLETE_FLAT", final_position_source=final_guard["position_source"])
        classification = TerminalClassification.PASSED
        reason = None
    except _PaperProofLifecycleStop as exc:
        classification = exc.classification
        lifecycle_status = exc.lifecycle_status
        lifecycle_required_action = exc.required_action
        reason = exc.reason
    except Exception as exc:  # noqa: BLE001 - any submit uncertainty fails closed for operator review.
        classification = TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
        reason = str(exc)
        lifecycle_status = lifecycle_status if lifecycle_status != "CREATED" else "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
        for submit_attempt_id in submitted_attempt_ids:
            append_submit_diagnostics(submit_attempt_id)
    finally:
        actual_adapter.disconnect()

    proof_payload = _proof_payload(
        run_id=run_id,
        classification=classification,
        ledger=ledger,
        ledger_path=ledger.events_path,
        proof_json=proof_json,
        proof_md=proof_md,
        reason=reason,
        lifecycle_status=lifecycle_status,
        lifecycle_required_action=lifecycle_required_action,
    )
    proof_json.parent.mkdir(parents=True, exist_ok=True)
    proof_json.write_text(json.dumps(to_jsonable(proof_payload), indent=2, sort_keys=True), encoding="utf-8")
    proof_md.write_text("# Track B IBKR Paper Proof\n\n" + json.dumps(to_jsonable(proof_payload), indent=2, sort_keys=True), encoding="utf-8")
    return HarnessResult(
        run_id=run_id,
        classification=classification,
        run_dir=run_dir,
        ledger_path=ledger.events_path,
        proof_report_json=proof_json,
        proof_report_md=proof_md,
        event_count=len(ledger.read_events(run_id=run_id)),
    )


def _signal(*, config: HarnessConfig, run_id: str, now: datetime) -> SignalEvent:
    return SignalEvent(
        signal_event_id=f"signal_{run_id}",
        run_id=run_id,
        source_event_id=f"operator_{run_id}",
        bar_id=f"synthetic_bar_{run_id}",
        strategy_id="TRACK_B_IBKR_PAPER_PROOF",
        symbol=config.contract_allowlist[config.contract_key]["symbol"],
        contract_key=config.contract_key,
        decision="TRACK_B_IBKR_PAPER_PROOF",
        side=config.side,
        quantity=config.quantity,
        reason="operator-confirmed paper proof",
        occurred_at=now,
        input_digest=f"ibkr-paper-proof:{run_id}",
    )


def _intent(*, config: HarnessConfig, run_id: str, signal: SignalEvent, kind: IntentKind, action: Action, limit_price: Decimal, now: datetime, index: int) -> OrderIntent:
    return OrderIntent(
        order_intent_id=f"intent_{run_id}_{index}",
        signal_event_id=signal.signal_event_id,
        run_id=run_id,
        intent_kind=kind,
        account_id=config.account_id,
        symbol=config.contract_allowlist[config.contract_key]["symbol"],
        contract_key=config.contract_key,
        action=action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price=limit_price,
        time_in_force=config.time_in_force,
        paper_only=True,
        created_at=now,
        reason=f"ibkr paper proof {kind.value.lower()}",
    )


def _submit(*, config: HarnessConfig, run_id: str, intent: OrderIntent, now: datetime, index: int) -> SubmitAttempt:
    return SubmitAttempt(
        submit_attempt_id=f"submit_{run_id}_{index}",
        order_intent_id=intent.order_intent_id,
        run_id=run_id,
        account_id=config.account_id,
        broker="IBKR",
        environment={**config.environment(), "broker": "IBKR", "environment": "PAPER"},
        pre_submit_reconciliation_id=f"pre_submit_recon_{run_id}_{index}",
        open_order_baseline_event_id=f"open_orders_{run_id}_{index}",
        request_digest=f"{intent.order_intent_id}:{intent.limit_price}",
        state=SubmitAttemptState.CREATED,
        submitted_at=now,
    )


def _evaluate_close_only_guard(
    *,
    adapter: object,
    config: HarnessConfig,
    run_id: str,
    expected_signed_quantity: int,
    now: datetime,
    stage: str,
) -> dict[str, object]:
    position = _observe_position_truth(adapter=adapter, config=config, run_id=run_id, now=now, stage=stage)
    open_orders = _observe_open_order_truth(adapter=adapter, config=config)
    working_orders = [order.to_json_dict() if hasattr(order, "to_json_dict") else to_jsonable(order) for order in open_orders]
    observed_qty = _signed_quantity(position)
    if observed_qty != expected_signed_quantity:
        return {
            "proof_lifecycle_status": "BLOCKED_POSITION_NOT_EXPECTED",
            "close_allowed": False,
            "stage": stage,
            "expected_signed_quantity": expected_signed_quantity,
            "observed_signed_quantity": observed_qty,
            "position": position.to_json_dict(),
            "working_orders": working_orders,
            "primary_blocker": f"Close-only proof requires position exactly {expected_signed_quantity}; observed {observed_qty}.",
            "required_next_action": "Do not submit a close order from Track B. Review TWS position and reconcile manually.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    if working_orders:
        return {
            "proof_lifecycle_status": "BLOCKED_WORKING_ORDER_EXISTS",
            "close_allowed": False,
            "stage": stage,
            "expected_signed_quantity": expected_signed_quantity,
            "observed_signed_quantity": observed_qty,
            "position": position.to_json_dict(),
            "working_orders": working_orders,
            "primary_blocker": "Close-only proof refuses while working same-contract broker orders exist.",
            "required_next_action": "Do not submit a close order from Track B. Resolve working broker orders first.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    return {
        "proof_lifecycle_status": "OPEN_FILLED",
        "close_allowed": True,
        "stage": stage,
        "expected_signed_quantity": expected_signed_quantity,
        "observed_signed_quantity": observed_qty,
        "position": position.to_json_dict(),
        "position_source": position.source.value if hasattr(position.source, "value") else str(position.source),
        "working_orders": [],
        "primary_blocker": None,
        "required_next_action": "Submit exactly one close-only order through the Track B paper-proof lifecycle.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _evaluate_flat_after_close_guard(
    *,
    adapter: object,
    config: HarnessConfig,
    run_id: str,
    now: datetime,
    stage: str,
) -> dict[str, object]:
    position = _observe_position_truth(adapter=adapter, config=config, run_id=run_id, now=now, stage=stage)
    open_orders = _observe_open_order_truth(adapter=adapter, config=config)
    working_orders = [order.to_json_dict() if hasattr(order, "to_json_dict") else to_jsonable(order) for order in open_orders]
    observed_qty = _signed_quantity(position)
    if observed_qty != 0:
        return {
            "proof_lifecycle_status": "AMBIGUOUS_MANUAL_REVIEW_REQUIRED",
            "flat_clean": False,
            "stage": stage,
            "expected_signed_quantity": 0,
            "observed_signed_quantity": observed_qty,
            "position": position.to_json_dict(),
            "working_orders": working_orders,
            "primary_blocker": f"Close proof did not leave the contract flat; observed position {observed_qty}.",
            "required_next_action": "Manual broker review required. Do not send another close order from Track B.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    if working_orders:
        return {
            "proof_lifecycle_status": "BLOCKED_WORKING_ORDER_EXISTS",
            "flat_clean": False,
            "stage": stage,
            "expected_signed_quantity": 0,
            "observed_signed_quantity": observed_qty,
            "position": position.to_json_dict(),
            "working_orders": working_orders,
            "primary_blocker": "Close proof left working same-contract broker orders.",
            "required_next_action": "Manual broker review required. Do not send another close order from Track B.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    return {
        "proof_lifecycle_status": "PROOF_COMPLETE_FLAT",
        "flat_clean": True,
        "stage": stage,
        "expected_signed_quantity": 0,
        "observed_signed_quantity": observed_qty,
        "position": position.to_json_dict(),
        "position_source": position.source.value if hasattr(position.source, "value") else str(position.source),
        "working_orders": [],
        "primary_blocker": None,
        "required_next_action": "Paper proof lifecycle completed flat. No further proof submit is needed for this run.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _observe_position_truth(*, adapter: object, config: HarnessConfig, run_id: str, now: datetime, stage: str) -> PositionState:
    refresh = getattr(adapter, "refresh_positions", None)
    if callable(refresh):
        return refresh(contract_key=config.contract_key)
    snapshot = getattr(adapter, "snapshot_position", None)
    if callable(snapshot):
        return snapshot(contract_key=config.contract_key)
    observe = getattr(adapter, "observe_position", None)
    if callable(observe):
        return observe(run_id=run_id, now=now, stage=stage)
    raise RuntimeError("Track B close-only guard requires broker position truth.")


def _observe_open_order_truth(*, adapter: object, config: HarnessConfig) -> tuple[BrokerOrder, ...]:
    refresh = getattr(adapter, "refresh_open_orders", None)
    if callable(refresh):
        return tuple(refresh(contract_key=config.contract_key))
    snapshot = getattr(adapter, "snapshot_open_orders", None)
    if callable(snapshot):
        return tuple(snapshot(contract_key=config.contract_key))
    observe = getattr(adapter, "observe_open_orders", None)
    if callable(observe):
        return tuple(observe())
    raise RuntimeError("Track B close-only guard requires broker open-order truth.")


def _signed_quantity(position: PositionState) -> int:
    return int(Decimal(str(position.signed_quantity)))


def _proof_payload(
    *,
    run_id: str,
    classification: TerminalClassification,
    ledger: JsonlLedger,
    ledger_path: Path,
    proof_json: Path,
    proof_md: Path,
    reason: str | None,
    lifecycle_status: str,
    lifecycle_required_action: str | None,
) -> dict[str, object]:
    by_type: dict[str, list[dict[str, object]]] = {}
    events = ledger.read_events(run_id=run_id)
    for event in events:
        by_type.setdefault(event.event_type, []).append(event.payload)
    intents = by_type.get("order_intent_created", [])
    submits = by_type.get("submit_attempt_created", [])
    orders = by_type.get("broker_order_observed", [])
    fills = by_type.get("fill_event_created", [])
    submit_diagnostics = by_type.get("submit_diagnostics_created", [])
    lifecycle_events = by_type.get("paper_proof_lifecycle_status", [])
    close_guards = by_type.get("close_only_guard_evaluated", [])
    flat_guards = by_type.get("flat_after_close_guard_evaluated", [])
    unresolved_report = _unresolved_order_report_from_payloads(orders)
    open_submit_id = str(submits[0].get("submit_attempt_id")) if submits else None
    close_submit_id = str(submits[1].get("submit_attempt_id")) if len(submits) > 1 else None
    open_diagnostics = _diagnostics_for_submit(submit_diagnostics, open_submit_id)
    close_diagnostics = _diagnostics_for_submit(submit_diagnostics, close_submit_id)
    return {
        "schema_version": "track_b_ibkr_paper_proof_v1",
        "classification": classification.value,
        "proof_lifecycle_status": lifecycle_status,
        "proof_lifecycle_events": lifecycle_events,
        "run_id": run_id,
        "state_machine": [event.event_type for event in events],
        "event_ids": [{"event_id": event.event_id, "event_type": event.event_type, "sequence": event.sequence} for event in events],
        "open_intent": intents[0] if intents else None,
        "open_submit_attempt": submits[0] if submits else None,
        "open_broker_order": orders[0] if orders else None,
        "open_fill": fills[0] if fills else None,
        "open_submit_diagnostics": open_diagnostics,
        "close_intent": intents[1] if len(intents) > 1 else None,
        "close_submit_attempt": submits[1] if len(submits) > 1 else None,
        "close_broker_order": orders[1] if len(orders) > 1 else None,
        "close_fill": fills[1] if len(fills) > 1 else None,
        "close_submit_diagnostics": close_diagnostics,
        "submit_diagnostics": submit_diagnostics,
        "close_only_guard_reports": close_guards,
        "flat_after_close_guard_reports": flat_guards,
        "final_reconciliation": {"status": "CLEAN"} if classification == TerminalClassification.PASSED else None,
        **unresolved_report,
        "failure_or_ambiguity": reason,
        "required_manual_action": lifecycle_required_action or ("Manual TWS review required." if reason else None),
        "paper_account_only": True,
        "paper_proof_cli_submit_path": True,
        "submit_attempted": bool(submits),
        "live_money_readiness": False,
        "production_live_money_readiness": False,
        "ledger_path": str(ledger_path),
        "json_report_path": str(proof_json),
        "markdown_report_path": str(proof_md),
    }


def _diagnostics_for_submit(diagnostics: list[dict[str, object]], submit_attempt_id: str | None) -> dict[str, object] | None:
    if submit_attempt_id is None:
        return None
    for row in diagnostics:
        if str(row.get("submit_attempt_id") or "") == submit_attempt_id:
            return row
    return None


def _unresolved_order_report_from_payloads(orders: list[dict[str, object]]) -> dict[str, object]:
    for order in orders:
        lifecycle = str(order.get("lifecycle_status") or "").strip()
        remaining = Decimal(str(order.get("remaining_quantity") or "0"))
        if not lifecycle:
            lifecycle = _payload_lifecycle_status(str(order.get("status") or ""), remaining)
        if lifecycle in {"HELD_OR_PRESUBMITTED", "PENDING_CANCEL", "AMBIGUOUS", "MANUAL_REVIEW_REQUIRED"}:
            return {
                "unresolved_broker_order_detected": True,
                "unresolved_broker_order_status": lifecycle,
                "unresolved_broker_order_id": order.get("broker_order_id"),
                "unresolved_broker_perm_id": order.get("perm_id"),
                "unresolved_remaining_quantity": str(remaining),
                "blocks_same_account_contract_submit": True,
                "next_required_action": "Wait for terminal broker order state, then rerun read-only preflight before submitting again.",
            }
    return {
        "unresolved_broker_order_detected": False,
        "unresolved_broker_order_status": None,
        "unresolved_broker_order_id": None,
        "unresolved_broker_perm_id": None,
        "unresolved_remaining_quantity": None,
        "blocks_same_account_contract_submit": False,
        "next_required_action": None,
    }


def _payload_lifecycle_status(status: str, remaining: Decimal) -> str:
    normalized = str(status or "").replace("_", "").replace(" ", "").strip().upper()
    if normalized in {"PENDINGCANCEL", "PENDCANCEL"}:
        return "PENDING_CANCEL" if remaining > 0 else "CANCELLED"
    if normalized in {"PRESUBMITTED", "SUBMITTED", "PENDINGSUBMIT", "APIPENDING", "HELD"}:
        return "HELD_OR_PRESUBMITTED" if remaining > 0 else "FILLED"
    if normalized in {"CANCELLED", "CANCELED"}:
        return "CANCELLED"
    if normalized == "FILLED":
        return "FILLED"
    if normalized in {"REJECTED", "INACTIVE"}:
        return "REJECTED"
    if remaining > 0:
        return "MANUAL_REVIEW_REQUIRED"
    return "AMBIGUOUS"
