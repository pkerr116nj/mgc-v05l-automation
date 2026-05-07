"""Read-only Track B managed submit/transmission diagnostic.

This report explains whether a strategy-managed PAPER lifecycle reached the
IBKR submit/transmit/fill boundary. It never calls a broker.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    build_track_b_paper_trade_summaries,
)


DEFAULT_TRACK_B_SUBMIT_TRANSMISSION_DIAGNOSTIC_JSON = (
    Path("outputs/track_b_execution_core/diagnostics") / "latest_track_b_submit_transmission_diagnostic.json"
)
DEFAULT_TRACK_B_SUBMIT_TRANSMISSION_DIAGNOSTIC_MD = (
    Path("outputs/track_b_execution_core/diagnostics") / "latest_track_b_submit_transmission_diagnostic.md"
)
DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_REPORT_JSON = (
    Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")
    / "latest_track_b_strategy_paper_runner_report.json"
)
DEFAULT_TRACK_B_STRATEGY_INTENT_JSON = (
    Path("outputs/track_b_execution_core/strategy_trade_intents") / "latest_track_b_strategy_trade_intent.json"
)

DIAGNOSTIC_SCHEMA_VERSION = "track_b_submit_transmission_diagnostic_v1"


def run_track_b_submit_transmission_diagnostic(
    *,
    runner_report_json: Path = DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_REPORT_JSON,
    latest_intent_json: Path = DEFAULT_TRACK_B_STRATEGY_INTENT_JSON,
    ledger_jsonl: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL,
    output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    diagnostic_json: Path = DEFAULT_TRACK_B_SUBMIT_TRANSMISSION_DIAGNOSTIC_JSON,
    diagnostic_md: Path = DEFAULT_TRACK_B_SUBMIT_TRANSMISSION_DIAGNOSTIC_MD,
    rebuild_compact_summaries: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Write a bounded read-only submit/transmission diagnostic.

    ``rebuild_compact_summaries`` only rebuilds ledger read-model JSON from the
    existing JSONL audit log; it does not contact or mutate a broker.
    """

    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    runner_report = _load_json(runner_report_json)
    latest_intent = _load_json(latest_intent_json)
    lifecycle_report_path = runner_report.get("managed_lifecycle_report_path")
    lifecycle_report = _load_json(Path(str(lifecycle_report_path))) if lifecycle_report_path else {}
    ledger_records = _read_jsonl(ledger_jsonl)
    matching_ledger_record = _latest_matching_ledger_record(
        ledger_records=ledger_records,
        lifecycle_id=str(lifecycle_report.get("lifecycle_id") or runner_report.get("managed_lifecycle_id") or ""),
        intent_id=str(latest_intent.get("intent_id") or ""),
    )

    entry_intent = _mapping(lifecycle_report.get("entry_intent")) or _mapping(runner_report.get("managed_entry_intent"))
    entry_submit = _mapping(lifecycle_report.get("entry_submit_attempt")) or _mapping(runner_report.get("managed_entry_submit_attempt"))
    entry_fill = _mapping(lifecycle_report.get("entry_fill")) or _mapping(runner_report.get("managed_entry_fill"))
    submit_diagnostics = _mapping(entry_submit.get("submit_diagnostics"))
    entry_broker_order = _mapping(entry_submit.get("broker_order"))

    strategy_signal_created = bool(runner_report.get("strategy_id") or latest_intent.get("strategy_id"))
    strategy_trade_intent_created = bool(
        runner_report.get("strategy_trade_intent_created") is True
        or latest_intent.get("intent_classification") == "STRATEGY_TRADE_INTENT_CREATED"
    )
    managed_lifecycle_invoked = bool(
        runner_report.get("managed_lifecycle_invoked") is True
        or runner_report.get("strategy_managed_lifecycle_invoked") is True
        or lifecycle_report
    )
    entry_intent_created = bool(entry_intent)
    entry_submit_attempt_recorded = bool(entry_submit)
    order_id_assigned = bool(
        entry_submit.get("broker_order_id")
        or entry_fill.get("broker_order_id")
        or submit_diagnostics.get("broker_order_id")
        or entry_broker_order.get("broker_order_id")
    )
    order_status_callback_received = bool(entry_broker_order or submit_diagnostics.get("order_status_received"))
    fill_callback_received = bool(entry_fill)
    submit_attempted = bool(runner_report.get("submit_attempted") or lifecycle_report.get("submit_attempted"))
    broker_state_mutated = bool(runner_report.get("broker_state_mutated") or lifecycle_report.get("broker_state_mutated"))
    place_order_called = bool(
        submit_diagnostics.get("place_order_called")
        or submit_diagnostics.get("placeOrder_called")
        or submit_diagnostics.get("order_transmit_flag") is True
        or (submit_attempted and broker_state_mutated)
    )
    transmit_true = submit_diagnostics.get("order_transmit_flag")
    if transmit_true is None:
        transmit_true = entry_submit.get("transmit") if entry_submit.get("transmit") is not None else None
    ibkr_adapter_invoked = bool(entry_submit_attempt_recorded or submit_diagnostics or place_order_called)

    pre_summary_open = _load_json(output_root / "latest_track_b_live_position_status.json").get("open_position_count")
    ledger_treated_as_open_position = _ledger_record_looks_open(matching_ledger_record)
    dashboard_treated_as_open_position = bool(_coerce_int(pre_summary_open) and _coerce_int(pre_summary_open) > 0)

    classification = _classify(
        strategy_signal_created=strategy_signal_created,
        strategy_trade_intent_created=strategy_trade_intent_created,
        managed_lifecycle_invoked=managed_lifecycle_invoked,
        entry_intent_created=entry_intent_created,
        entry_submit_attempt_recorded=entry_submit_attempt_recorded,
        ibkr_adapter_invoked=ibkr_adapter_invoked,
        place_order_called=place_order_called,
        transmit_true=transmit_true,
        order_id_assigned=order_id_assigned,
        order_status_callback_received=order_status_callback_received,
        fill_callback_received=fill_callback_received,
        ledger_treated_as_open_position=ledger_treated_as_open_position,
        dashboard_treated_as_open_position=dashboard_treated_as_open_position,
    )

    compact_summaries_updated = False
    post_summary: dict[str, Any] = {}
    if rebuild_compact_summaries:
        trade_summary_json = output_root / "latest_track_b_paper_trade_summary.json"
        live_position_status_json = output_root / "latest_track_b_live_position_status.json"
        pnl_summary_json = output_root / "latest_track_b_pnl_summary.json"
        summaries = build_track_b_paper_trade_summaries(
            ledger_records=ledger_records,
            ledger_jsonl=ledger_jsonl,
            trade_summary_json=trade_summary_json,
            live_position_status_json=live_position_status_json,
            pnl_summary_json=pnl_summary_json,
            now=actual_now,
        )
        _write_json(trade_summary_json, summaries["trade_summary"])
        _write_json(live_position_status_json, summaries["live_position_status"])
        _write_json(pnl_summary_json, summaries["pnl_summary"])
        compact_summaries_updated = True
        post_summary = {
            "open_position_count": summaries["live_position_status"].get("open_position_count"),
            "review_required_count": summaries["trade_summary"].get("review_required_count"),
            "managed_strategy_trade_count": summaries["trade_summary"].get("managed_strategy_trade_count"),
            "meaningful_strategy_trade_count": summaries["trade_summary"].get("meaningful_strategy_trade_count"),
            "paper_trades_attempted_count": summaries["trade_summary"].get("paper_trades_attempted_count"),
            "app_only_position_from_unfilled_entry_count": summaries["trade_summary"].get(
                "app_only_position_from_unfilled_entry_count"
            ),
        }

    report = {
        "schema_version": DIAGNOSTIC_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "strategy_signal_created": strategy_signal_created,
        "strategy_trade_intent_created": strategy_trade_intent_created,
        "strategy_trade_intent_path": str(latest_intent_json),
        "intent_id": latest_intent.get("intent_id"),
        "strategy_id": runner_report.get("strategy_id") or latest_intent.get("strategy_id"),
        "instrument": runner_report.get("contract_key") or latest_intent.get("contract_key"),
        "local_symbol": runner_report.get("local_symbol") or latest_intent.get("local_symbol"),
        "managed_lifecycle_invoked": managed_lifecycle_invoked,
        "managed_lifecycle_report_path": str(lifecycle_report_path or ""),
        "lifecycle_id": lifecycle_report.get("lifecycle_id"),
        "entry_intent_created": entry_intent_created,
        "entry_submit_attempt_recorded": entry_submit_attempt_recorded,
        "ibkr_adapter_invoked": ibkr_adapter_invoked,
        "place_order_called": place_order_called,
        "transmit_true": transmit_true,
        "order_id_assigned": order_id_assigned,
        "order_status_callback_received": order_status_callback_received,
        "fill_callback_received": fill_callback_received,
        "submit_attempted": submit_attempted,
        "broker_state_mutated": broker_state_mutated,
        "ledger_treated_as_open_position_before_rebuild": ledger_treated_as_open_position,
        "dashboard_treated_as_open_position_before_rebuild": dashboard_treated_as_open_position,
        "matching_ledger_trade_id": None if matching_ledger_record is None else matching_ledger_record.get("trade_id"),
        "matching_ledger_lifecycle_id": None if matching_ledger_record is None else matching_ledger_record.get("lifecycle_id"),
        "matching_ledger_entry_fill_price": None if matching_ledger_record is None else matching_ledger_record.get("entry_fill_price"),
        "matching_ledger_entry_order_id": None if matching_ledger_record is None else matching_ledger_record.get("entry_order_id"),
        "compact_summaries_updated": compact_summaries_updated,
        "post_summary": post_summary,
        "broker_mutation_attempted_by_diagnostic": False,
        "paper_proof_cli_invoked_by_diagnostic": False,
        "recommended_next_action": _recommended_next_action(classification),
        "source_paths": {
            "runner_report": str(runner_report_json),
            "intent": str(latest_intent_json),
            "ledger": str(ledger_jsonl),
        },
    }
    _write_json(diagnostic_json, report)
    _write_markdown(diagnostic_md, report)
    return report


def _classify(
    *,
    strategy_signal_created: bool,
    strategy_trade_intent_created: bool,
    managed_lifecycle_invoked: bool,
    entry_intent_created: bool,
    entry_submit_attempt_recorded: bool,
    ibkr_adapter_invoked: bool,
    place_order_called: bool,
    transmit_true: object,
    order_id_assigned: bool,
    order_status_callback_received: bool,
    fill_callback_received: bool,
    ledger_treated_as_open_position: bool,
    dashboard_treated_as_open_position: bool,
) -> str:
    if fill_callback_received:
        return "BROKER_BACKED_POSITION_CONFIRMED"
    if (ledger_treated_as_open_position or dashboard_treated_as_open_position) and not fill_callback_received:
        return "APP_ONLY_POSITION_FROM_UNFILLED_ENTRY"
    if not strategy_signal_created:
        return "DIAGNOSTIC_INCONCLUSIVE"
    if not strategy_trade_intent_created:
        return "SIGNAL_ONLY_NO_INTENT"
    if not managed_lifecycle_invoked:
        return "INTENT_CREATED_NO_LIFECYCLE"
    if not entry_intent_created:
        return "INTENT_CREATED_NO_LIFECYCLE"
    if not entry_submit_attempt_recorded:
        return "LIFECYCLE_CREATED_NO_SUBMIT"
    if not ibkr_adapter_invoked:
        return "IBKR_ADAPTER_NOT_INVOKED"
    if transmit_true is False:
        return "SUBMIT_ATTEMPT_CREATED_NOT_TRANSMITTED"
    if order_id_assigned and not place_order_called:
        return "ORDER_ID_ASSIGNED_NOT_TRANSMITTED"
    if order_id_assigned and not order_status_callback_received:
        return "ORDER_STATUS_MISSING"
    if not fill_callback_received:
        return "FILL_MISSING"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _recommended_next_action(classification: str) -> str:
    if classification == "APP_ONLY_POSITION_FROM_UNFILLED_ENTRY":
        return (
            "Treat the lifecycle as a failed/blocked transmission attempt, not an open PAPER position; "
            "compact summaries should show no broker-backed open position until a fill callback or broker reconciliation exists."
        )
    if classification == "BROKER_BACKED_POSITION_CONFIRMED":
        return "Continue managed lifecycle tracking and broker reconciliation."
    return "Inspect the named missing chain stage before allowing lifecycle state to become position state."


def _ledger_record_looks_open(record: Mapping[str, Any] | None) -> bool:
    if not record:
        return False
    if record.get("paper_lifecycle_type") == "STRATEGY_MANAGED":
        return record.get("entry_fill_price") in {None, ""} and record.get("review_required") is True
    return bool(record.get("final_position_status") not in {"CLOSED_FLAT", "CLEAN", "PROOF_COMPLETE_FLAT"})


def _latest_matching_ledger_record(
    *,
    ledger_records: Iterable[Mapping[str, Any]],
    lifecycle_id: str,
    intent_id: str,
) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for item in ledger_records:
        if item.get("record_type") == "ARTIFACT_RECONCILIATION":
            continue
        if lifecycle_id and str(item.get("lifecycle_id") or "") == lifecycle_id:
            matches.append(dict(item))
        elif intent_id and str(item.get("signal_id") or "") == intent_id:
            matches.append(dict(item))
    return matches[-1] if matches else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _coerce_int(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Track B Submit Transmission Diagnostic",
        "",
        f"- Classification: `{report.get('classification')}`",
        f"- Strategy: `{report.get('strategy_id')}`",
        f"- Instrument: `{report.get('instrument')}` / `{report.get('local_symbol')}`",
        f"- Intent created: `{report.get('strategy_trade_intent_created')}`",
        f"- Managed lifecycle invoked: `{report.get('managed_lifecycle_invoked')}`",
        f"- Entry submit attempt recorded: `{report.get('entry_submit_attempt_recorded')}`",
        f"- IBKR adapter invoked: `{report.get('ibkr_adapter_invoked')}`",
        f"- placeOrder called: `{report.get('place_order_called')}`",
        f"- transmit=true: `{report.get('transmit_true')}`",
        f"- Order id assigned: `{report.get('order_id_assigned')}`",
        f"- Order status callback: `{report.get('order_status_callback_received')}`",
        f"- Fill callback: `{report.get('fill_callback_received')}`",
        f"- Submit attempted flag: `{report.get('submit_attempted')}`",
        f"- Broker state mutated flag: `{report.get('broker_state_mutated')}`",
        "",
        "## Compact Summary Repair",
        "",
        f"- Updated: `{report.get('compact_summaries_updated')}`",
        f"- Post open positions: `{_mapping(report.get('post_summary')).get('open_position_count')}`",
        f"- Post managed strategy trades: `{_mapping(report.get('post_summary')).get('managed_strategy_trade_count')}`",
        "",
        "## Recommendation",
        "",
        str(report.get("recommended_next_action") or ""),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run read-only Track B submit/transmission diagnostic.")
    parser.add_argument("--no-rebuild-compact-summaries", action="store_true")
    args = parser.parse_args()
    report = run_track_b_submit_transmission_diagnostic(
        rebuild_compact_summaries=not args.no_rebuild_compact_summaries,
    )
    print(json.dumps(to_jsonable(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
