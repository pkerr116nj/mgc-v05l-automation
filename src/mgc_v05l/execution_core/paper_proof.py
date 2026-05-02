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
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping

from .harness import HarnessConfig, HarnessResult
from .models import TerminalClassification, to_jsonable
from .preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig


DEFAULT_PAPER_PROOF_OUTPUT_ROOT = Path("outputs/track_b_execution_core/paper_proof")


class PaperProofConfigError(ValueError):
    """Raised when the operator submit-proof request is not explicit enough."""


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

    if proof_runner is None:
        return _write_result(
            run_id=actual_run_id,
            config=config,
            report_json=report_json,
            report_md=report_md,
            classification=TerminalClassification.BLOCKED,
            reason="paper proof submit runner is not configured in this slice",
            required_action="Wire the reviewed submit adapter before live paper proof.",
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
    proof = proof_runner(proof_config, actual_run_id)
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
    if report.get("open_orders"):
        return "proof contract has existing open orders"
    account_open_orders = report.get("account_open_orders") or report.get("account_wide_open_orders")
    if account_open_orders:
        return "account-wide open orders exist"
    mode = str(report.get("market_data_mode") or "UNKNOWN").upper()
    quote_observed = bool(report.get("quote_observed"))
    if not quote_observed:
        return "pricing-dependent proof submit requires an observed quote"
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
) -> PaperProofResult:
    preflight_report = preflight.report if preflight is not None else {}
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
        "paper_route_readiness": preflight_report.get("paper_route_readiness"),
        "production_live_money_readiness": False,
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
