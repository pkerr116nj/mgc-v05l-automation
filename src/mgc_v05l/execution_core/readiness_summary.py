"""No-submit Track B readiness summary from existing report JSON."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .readiness import FinalReadinessVerdict, OperatorReadiness, blocked_readiness, ready_for_paper_proof
from .session_guard import evaluate_proof_timing


DEFAULT_READINESS_SUMMARY_OUTPUT_ROOT = Path("outputs/track_b_execution_core/readiness_summary")


@dataclass(frozen=True)
class ReadinessSummaryConfig:
    recovery_report_json: Path
    preflight_report_json: Path
    proof_timing_status: str
    output_root: Path = DEFAULT_READINESS_SUMMARY_OUTPUT_ROOT
    quote_report_json: Path | None = None
    proof_timing_source: str = "operator_summary"
    proof_timing_detail: str | None = None


@dataclass(frozen=True)
class ReadinessSummaryResult:
    report_json: Path
    report: dict[str, Any]


def run_readiness_summary(*, config: ReadinessSummaryConfig, run_id: str | None = None) -> ReadinessSummaryResult:
    actual_run_id = run_id or f"readiness_summary_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_run_id / "readiness_summary_report.json"
    recovery = _read_json(config.recovery_report_json)
    preflight = _read_json(config.preflight_report_json)
    quote = _read_json(config.quote_report_json) if config.quote_report_json is not None else None
    timing = evaluate_proof_timing(
        status=config.proof_timing_status,
        source=config.proof_timing_source,
        detail=config.proof_timing_detail,
    )
    operator_readiness = _summarize(
        recovery=recovery,
        preflight=preflight,
        quote=quote,
        timing=timing.to_report_dict(),
    )
    report = {
        "schema_version": "track_b_readiness_summary_v1",
        "run_id": actual_run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        **operator_readiness.to_report_dict(),
        "recovery_verdict": recovery.get("final_readiness_verdict") or recovery.get("classification"),
        "preflight_verdict": preflight.get("final_readiness_verdict") or preflight.get("classification"),
        "proof_timing_classification": timing.classification,
        "proof_timing_allowed": timing.allowed,
        "quote_classification": quote.get("classification") if quote is not None else None,
        "quote_status": quote.get("quote_status") if quote is not None else None,
        "quote_provider_mode": quote.get("quote_provider_mode") if quote is not None else None,
        "current_quote_available": quote.get("current_quote_available") if quote is not None else None,
        "realtime_subscription_attempted": quote.get("realtime_subscription_attempted") if quote is not None else None,
        "realtime_quote_received": quote.get("realtime_quote_received") if quote is not None else None,
        "databento_dependency_status": quote.get("databento_dependency_status") if quote is not None else None,
        "databento_live_api_available": quote.get("databento_live_api_available") if quote is not None else None,
        "provider_error_category": quote.get("provider_error_category") if quote is not None else None,
        "symbol_subscription_attempted": quote.get("symbol_subscription_attempted") if quote is not None else None,
        "symbol_subscription_succeeded": quote.get("symbol_subscription_succeeded") if quote is not None else None,
        "requested_quote_end": quote.get("requested_quote_end") if quote is not None else None,
        "provider_available_end": quote.get("provider_available_end") if quote is not None else None,
        "quote_age_seconds": quote.get("quote_age_seconds") if quote is not None else None,
        "max_current_quote_age_seconds": quote.get("max_current_quote_age_seconds") if quote is not None else None,
        "quote_freshness_verdict": quote.get("quote_freshness_verdict") if quote is not None else None,
        "production_live_money_readiness": False,
        "paper_proof_cli_explicit_flags_still_required": True,
        "submit_enabled": False,
        "place_order_called": False,
        "cancel_called": False,
        "recovery_report_json": str(config.recovery_report_json),
        "preflight_report_json": str(config.preflight_report_json),
        "quote_report_json": str(config.quote_report_json) if config.quote_report_json is not None else None,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return ReadinessSummaryResult(report_json=report_json, report=report)


def _summarize(
    *,
    recovery: Mapping[str, Any],
    preflight: Mapping[str, Any],
    quote: Mapping[str, Any] | None,
    timing: Mapping[str, Any],
) -> OperatorReadiness:
    account_id = str(preflight.get("account_id") or recovery.get("account_id") or "")
    contract_key = str(preflight.get("contract_key") or recovery.get("contract_key") or "")
    position_qty = preflight.get("position_qty") or recovery.get("position_qty") or recovery.get("position_quantity")
    secondary: list[str] = []

    recovery_verdict = str(recovery.get("final_readiness_verdict") or "")
    if recovery_verdict and recovery_verdict != FinalReadinessVerdict.READY_FOR_PAPER_PROOF.value:
        return blocked_readiness(
            verdict=_verdict_from_report(recovery_verdict),
            primary_blocker=str(recovery.get("primary_blocker") or "Recovery status is not clean."),
            required_next_action=str(recovery.get("required_next_action") or "Resolve recovery blocker before any paper proof submit."),
            account_id=account_id,
            contract_key=contract_key,
            broker_order_id=recovery.get("broker_order_id") or recovery.get("matching_broker_order_id"),
            perm_id=recovery.get("perm_id") or recovery.get("matching_perm_id"),
            broker_status=recovery.get("broker_status") or recovery.get("order_status"),
            position_qty=position_qty,
        )

    preflight_verdict = str(preflight.get("final_readiness_verdict") or "")
    if preflight_verdict and preflight_verdict != FinalReadinessVerdict.READY_FOR_PAPER_PROOF.value:
        return blocked_readiness(
            verdict=_verdict_from_report(preflight_verdict),
            primary_blocker=str(preflight.get("primary_blocker") or "Read-only preflight is not clean."),
            required_next_action=str(preflight.get("required_next_action") or "Resolve preflight blocker before any paper proof submit."),
            account_id=account_id,
            contract_key=contract_key,
            broker_order_id=preflight.get("broker_order_id") or preflight.get("unresolved_broker_order_id"),
            perm_id=preflight.get("perm_id") or preflight.get("unresolved_broker_perm_id"),
            broker_status=preflight.get("broker_status") or preflight.get("unresolved_broker_order_status"),
            position_qty=position_qty,
        )

    timing_classification = timing.get("proof_timing_classification")
    if timing_classification == "PROOF_TIMING_BLOCKED_OUTSIDE_ACTIVE_SESSION":
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_OUTSIDE_ACTIVE_SESSION,
            primary_blocker=str(timing.get("proof_timing_reason") or "Proof timing is outside an active session."),
            required_next_action=str(timing.get("proof_timing_required_action") or "Wait for an active session."),
            account_id=account_id,
            contract_key=contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=False,
        )
    if timing_classification == "PROOF_TIMING_UNKNOWN_BLOCKED":
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_UNKNOWN_PROOF_TIMING,
            primary_blocker=str(timing.get("proof_timing_reason") or "Proof timing is unknown."),
            required_next_action=str(timing.get("proof_timing_required_action") or "Provide active-session timing evidence."),
            account_id=account_id,
            contract_key=contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=False,
        )

    quote_provider_mode = str(quote.get("quote_provider_mode") or "").strip().upper() if quote is not None else ""
    if quote is not None and quote_provider_mode != "REALTIME":
        mode_label = quote_provider_mode or "NOT_PROVIDED"
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE,
            primary_blocker=(
                f"Quote report came from {mode_label} provider mode, not the realtime Databento feed; "
                f"quote_freshness_verdict={quote.get('quote_freshness_verdict')}; "
                f"quote_age_seconds={quote.get('quote_age_seconds')}; "
                f"max_current_quote_age_seconds={quote.get('max_current_quote_age_seconds')}"
            ),
            required_next_action="Run the realtime Databento quote provider path before considering paper proof readiness.",
            account_id=account_id,
            contract_key=contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=True,
        )

    if quote is not None and (
        str(quote.get("classification") or quote.get("quote_status") or "") != "CURRENT_QUOTE_AVAILABLE"
        or quote.get("current_quote_available") is not True
    ):
        return blocked_readiness(
            verdict=FinalReadinessVerdict.BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE,
            primary_blocker=(
                f"Quote is not currently available: {quote.get('classification') or quote.get('quote_status')}; "
                f"quote_freshness_verdict={quote.get('quote_freshness_verdict')}; "
                f"quote_age_seconds={quote.get('quote_age_seconds')}; "
                f"max_current_quote_age_seconds={quote.get('max_current_quote_age_seconds')}"
            ),
            required_next_action="Obtain a usable current quote or use explicitly acknowledged manual paper-only limit prices.",
            account_id=account_id,
            contract_key=contract_key,
            position_qty=position_qty,
            proof_timing_classification=timing_classification,
            proof_timing_allowed=True,
        )
    if quote is None:
        secondary.append("No quote report was provided; paper proof must still satisfy quote or manual-price gates.")

    return ready_for_paper_proof(
        account_id=account_id,
        contract_key=contract_key,
        position_qty=position_qty,
        required_next_action="All summary inputs are clean. paper_proof_cli still requires explicit submit flags and active operator confirmation.",
        proof_timing_classification=timing_classification,
        proof_timing_allowed=True,
    )


def _verdict_from_report(value: str) -> FinalReadinessVerdict:
    try:
        return FinalReadinessVerdict(value)
    except ValueError:
        return FinalReadinessVerdict.AMBIGUOUS_MANUAL_REVIEW_REQUIRED


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))
