"""Read-only diagnostic for Track B managed PAPER submit invocation gates."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_RUNNER_REPORT_JSON = (
    Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")
    / "latest_track_b_strategy_paper_runner_report.json"
)
DEFAULT_DIAGNOSTIC_JSON = (
    Path("outputs/track_b_execution_core/diagnostics")
    / "latest_track_b_managed_submit_invocation_diagnostic.json"
)
DEFAULT_DIAGNOSTIC_MD = (
    Path("outputs/track_b_execution_core/diagnostics")
    / "latest_track_b_managed_submit_invocation_diagnostic.md"
)
SCHEMA_VERSION = "track_b_managed_submit_invocation_diagnostic_v1"


def run_track_b_managed_submit_invocation_diagnostic(
    *,
    runner_report_json: Path = DEFAULT_RUNNER_REPORT_JSON,
    diagnostic_json: Path = DEFAULT_DIAGNOSTIC_JSON,
    diagnostic_md: Path = DEFAULT_DIAGNOSTIC_MD,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    runner = _load_json(runner_report_json)
    lifecycle_report_path = runner.get("managed_lifecycle_report_path")
    lifecycle = _load_json(Path(str(lifecycle_report_path))) if lifecycle_report_path else {}
    entry_submit = _mapping(lifecycle.get("entry_submit_attempt")) or _mapping(runner.get("managed_entry_submit_attempt"))
    submit_diagnostics = _mapping(entry_submit.get("submit_diagnostics"))
    primary_blocker = str(lifecycle.get("primary_blocker") or runner.get("primary_blocker") or "")
    paper_submit_requested = bool(runner.get("paper_submit_requested"))
    submit_flags_present = bool(runner.get("paper_submit_flags_present"))
    submit_enabled = lifecycle.get("submit_enabled")
    if submit_enabled is None:
        submit_enabled = paper_submit_requested
    place_order_called = bool(submit_diagnostics.get("place_order_called"))
    adapter_invoked = bool(
        submit_diagnostics
        or place_order_called
        or entry_submit.get("broker_order_id")
        or entry_submit.get("submit_attempted")
        or entry_submit.get("submitted")
    )
    classification = _classify(
        paper_submit_requested=paper_submit_requested,
        submit_flags_present=submit_flags_present,
        submit_enabled=bool(submit_enabled),
        managed_lifecycle_invoked=bool(runner.get("managed_lifecycle_invoked") or lifecycle),
        entry_submit=entry_submit,
        primary_blocker=primary_blocker,
        place_order_called=place_order_called,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "strategy_id": runner.get("strategy_id") or lifecycle.get("strategy_id"),
        "instrument": runner.get("contract_key") or lifecycle.get("contract_key"),
        "local_symbol": runner.get("local_symbol") or lifecycle.get("local_symbol"),
        "managed_lifecycle_report_path": str(lifecycle_report_path or ""),
        "managed_lifecycle_invoked": bool(runner.get("managed_lifecycle_invoked") or lifecycle),
        "managed_paper_submit_enabled": bool(submit_enabled),
        "paper_submit_requested": paper_submit_requested,
        "paper_submit_flags_present": submit_flags_present,
        "submit_allowed": bool(runner.get("submit_allowed") or lifecycle.get("submit_allowed")),
        "submit_attempted": bool(runner.get("submit_attempted") or lifecycle.get("submit_attempted")),
        "broker_state_mutated": bool(runner.get("broker_state_mutated") or lifecycle.get("broker_state_mutated")),
        "entry_intent_created": bool(lifecycle.get("entry_intent") or runner.get("managed_entry_intent")),
        "entry_submit_attempt_recorded": bool(entry_submit),
        "ibkr_adapter_invoked": adapter_invoked,
        "place_order_called": place_order_called,
        "transmit_true": submit_diagnostics.get("order_transmit_flag"),
        "order_id_assigned": bool(entry_submit.get("broker_order_id") or submit_diagnostics.get("broker_order_id_allocated")),
        "order_status_callback_received": bool(
            submit_diagnostics.get("openOrder_seen") or submit_diagnostics.get("orderStatus_seen")
        ),
        "fill_callback_received": bool(lifecycle.get("entry_fill") or runner.get("managed_entry_fill")),
        "managed_submit_blocked_reason": lifecycle.get("managed_submit_blocked_reason")
        or entry_submit.get("managed_submit_blocked_reason")
        or _blocked_reason_from_classification(classification),
        "primary_blocker": primary_blocker or None,
        "submit_diagnostics": submit_diagnostics,
        "ibkr_adapter_available": lifecycle.get("ibkr_adapter_available"),
        "current_launchd_monitor_intentionally_submit_disabled": classification
        in {"MANAGED_LIFECYCLE_DRY_RUN_MODE", "SUBMIT_FLAG_NOT_SET", "PAPER_SUBMIT_NOT_REQUESTED", "MONITOR_CONFIG_SUBMIT_DISABLED"},
        "operator_change_required_for_real_managed_paper_submits": _operator_change_required(classification),
        "broker_mutation_attempted_by_diagnostic": False,
        "paper_proof_cli_invoked_by_diagnostic": False,
        "source_paths": {"runner_report": str(runner_report_json), "lifecycle_report": str(lifecycle_report_path or "")},
    }
    _write_json(diagnostic_json, report)
    _write_markdown(diagnostic_md, report)
    return report


def _classify(
    *,
    paper_submit_requested: bool,
    submit_flags_present: bool,
    submit_enabled: bool,
    managed_lifecycle_invoked: bool,
    entry_submit: Mapping[str, Any],
    primary_blocker: str,
    place_order_called: bool,
) -> str:
    if not managed_lifecycle_invoked:
        return "DIAGNOSTIC_INCONCLUSIVE"
    if "review-required" in primary_blocker:
        return "REVIEW_REQUIRED_BLOCKED_SUBMIT"
    if "Account guard" in primary_blocker or "local symbol" in primary_blocker or "conId" in primary_blocker:
        return "ACCOUNT_OR_CONTRACT_GUARD_BLOCKED"
    if not paper_submit_requested:
        return "PAPER_SUBMIT_NOT_REQUESTED"
    if not submit_flags_present:
        return "SUBMIT_FLAG_NOT_SET"
    if not submit_enabled:
        return "MONITOR_CONFIG_SUBMIT_DISABLED"
    if "not enabled" in primary_blocker:
        return "MANAGED_LIFECYCLE_DRY_RUN_MODE"
    if not entry_submit and primary_blocker:
        return "DIAGNOSTIC_INCONCLUSIVE"
    if not entry_submit:
        return "IBKR_ADAPTER_NOT_INJECTED"
    if entry_submit.get("primary_blocker") and not place_order_called:
        return "BROKER_PREFLIGHT_BLOCKED_BEFORE_ADAPTER"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _blocked_reason_from_classification(classification: str) -> str | None:
    if classification in {"MANAGED_LIFECYCLE_DRY_RUN_MODE", "SUBMIT_FLAG_NOT_SET", "PAPER_SUBMIT_NOT_REQUESTED", "MONITOR_CONFIG_SUBMIT_DISABLED"}:
        return "SUBMIT_DISABLED"
    if classification == "REVIEW_REQUIRED_BLOCKED_SUBMIT":
        return "REVIEW_REQUIRED_BLOCKED_SUBMIT"
    if classification == "ACCOUNT_OR_CONTRACT_GUARD_BLOCKED":
        return "ACCOUNT_OR_CONTRACT_GUARD_BLOCKED"
    return None


def _operator_change_required(classification: str) -> str:
    if classification in {"MANAGED_LIFECYCLE_DRY_RUN_MODE", "SUBMIT_FLAG_NOT_SET", "PAPER_SUBMIT_NOT_REQUESTED", "MONITOR_CONFIG_SUBMIT_DISABLED"}:
        return "Enable explicit managed PAPER submit flags in the monitor command/config only after broker preflight is clean."
    if classification == "DIAGNOSTIC_INCONCLUSIVE":
        return "Preserve submit-stage diagnostics on the next lifecycle run; current artifact does not prove whether the adapter was invoked."
    return "Resolve the reported blocker before allowing another managed PAPER submit."


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Track B Managed Submit Invocation Diagnostic",
        "",
        f"- Classification: `{report.get('classification')}`",
        f"- Strategy: `{report.get('strategy_id')}`",
        f"- Instrument: `{report.get('instrument')}` / `{report.get('local_symbol')}`",
        f"- Managed PAPER submit enabled: `{report.get('managed_paper_submit_enabled')}`",
        f"- Paper submit requested: `{report.get('paper_submit_requested')}`",
        f"- Submit flags present: `{report.get('paper_submit_flags_present')}`",
        f"- Entry submit attempt recorded: `{report.get('entry_submit_attempt_recorded')}`",
        f"- IBKR adapter invoked: `{report.get('ibkr_adapter_invoked')}`",
        f"- placeOrder called: `{report.get('place_order_called')}`",
        f"- Primary blocker: `{report.get('primary_blocker')}`",
        "",
        str(report.get("operator_change_required_for_real_managed_paper_submits") or ""),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run read-only Track B managed submit invocation diagnostic.")
    parser.add_argument("--runner-report-json", type=Path, default=DEFAULT_RUNNER_REPORT_JSON)
    parser.add_argument("--diagnostic-json", type=Path, default=DEFAULT_DIAGNOSTIC_JSON)
    parser.add_argument("--diagnostic-md", type=Path, default=DEFAULT_DIAGNOSTIC_MD)
    args = parser.parse_args()
    report = run_track_b_managed_submit_invocation_diagnostic(
        runner_report_json=args.runner_report_json,
        diagnostic_json=args.diagnostic_json,
        diagnostic_md=args.diagnostic_md,
    )
    print(json.dumps(to_jsonable(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
