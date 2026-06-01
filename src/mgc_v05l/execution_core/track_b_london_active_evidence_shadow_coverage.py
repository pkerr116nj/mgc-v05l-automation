"""Read-only London active-evidence coverage monitor for Track B PAPER.

This module evaluates MNQ/MES active-evidence predicates during London windows
without creating broker-authoritative lanes, routes, orders, or lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.app.track_b_rule_runner_paper_engine import (
    NEW_YORK_TZ,
    _PaperActiveEvidenceSpec,
    _active_evidence_reference_bar,
    _changeover_decision,
    _decimal_or_none,
    _phase1_runtime_bar_history_from_path,
    _recent_close_direction_tag,
    _rolling_vwap_reference,
    _simple_long_continuation_ok,
    _simple_short_continuation_ok,
    _time_inside_active_evidence_window,
)
from mgc_v05l.session_phase_labels import label_session_phase


REPORT_SCHEMA_VERSION = "track_b_london_active_evidence_shadow_coverage_v1"
DEFAULT_OUTPUT_PATH = Path(
    "outputs/track_b_execution_core/london_active_evidence_shadow/latest_london_active_evidence_shadow_coverage.json"
)


@dataclass(frozen=True)
class LondonShadowSpec:
    lane_id: str
    strategy_id: str
    symbol: str
    direction: str
    london_window: str
    active_spec: _PaperActiveEvidenceSpec


LONDON_OPEN_WINDOW = (time(3, 0), time(5, 30))
LONDON_LATE_WINDOW = (time(5, 30), time(8, 20))


def _shadow_spec(
    *,
    symbol: str,
    direction: str,
    london_window: str,
    start_time_et: time,
    end_time_et: time,
    reference_time_et: time,
    reference_label: str,
) -> LondonShadowSpec:
    normalized_symbol = symbol.upper()
    normalized_direction = direction.upper()
    normalized_window = london_window.upper()
    lane_id = (
        f"{normalized_symbol.lower()}_{normalized_window.lower()}_active_evidence_"
        f"{normalized_direction.lower()}_shadow"
    )
    strategy_id = (
        f"PAPER_WATCH_ACTIVE_EVIDENCE_{normalized_symbol}_{normalized_window}_"
        f"{normalized_direction}_SHADOW_V1"
    )
    return LondonShadowSpec(
        lane_id=lane_id,
        strategy_id=strategy_id,
        symbol=normalized_symbol,
        direction=normalized_direction,
        london_window=normalized_window,
        active_spec=_PaperActiveEvidenceSpec(
            strategy_id=strategy_id,
            direction=normalized_direction,
            overlay_label="PAPER_WATCH_LONDON_ACTIVE_EVIDENCE_SHADOW",
            start_time_et=start_time_et,
            end_time_et=end_time_et,
            benchmark_hold_bars_5m=12,
            condition_label=(
                f"{start_time_et:%H:%M}-{end_time_et:%H:%M}_ET_close_"
                f"{'above' if normalized_direction == 'LONG' else 'below'}_vwap_or_{reference_label}"
            ),
            reference_time_et=reference_time_et,
            reference_label=reference_label,
            require_reference_bar=False,
        ),
    )


LONDON_ACTIVE_EVIDENCE_SHADOW_SPECS: tuple[LondonShadowSpec, ...] = tuple(
    _shadow_spec(
        symbol=symbol,
        direction=direction,
        london_window=window,
        start_time_et=start,
        end_time_et=end,
        reference_time_et=reference,
        reference_label=reference_label,
    )
    for symbol in ("MNQ", "MES")
    for window, start, end, reference, reference_label in (
        ("LONDON_OPEN", LONDON_OPEN_WINDOW[0], LONDON_OPEN_WINDOW[1], time(3, 0), "03_00_london_open_reference"),
        ("LONDON_LATE", LONDON_LATE_WINDOW[0], LONDON_LATE_WINDOW[1], time(5, 30), "05_30_london_late_reference"),
    )
    for direction in ("LONG", "SHORT")
)


def build_london_active_evidence_shadow_report(
    *,
    repo_root: Path,
    now: datetime | None = None,
    paper_stack_status: Mapping[str, Any] | None = None,
    registry_diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a read-only London active-evidence shadow report."""
    resolved_root = repo_root.resolve()
    generated_at = now or datetime.now(UTC)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)
    status = dict(paper_stack_status or _read_json(resolved_root / "outputs/track_b_execution_core/paper_stack/latest_paper_stack_status.json"))
    diagnostics = dict(
        registry_diagnostics
        or status.get("registry_truth_diagnostics")
        or _read_json(resolved_root / "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics.json")
    )
    governance_hypothetical = _hypothetical_governance_safe_state(status)
    entry_exposure_hypothetical = _hypothetical_registry_truth_entry_exposure(
        status=status,
        diagnostics=diagnostics,
    )

    rows = [
        _evaluate_shadow_spec(
            spec,
            repo_root=resolved_root,
            entry_exposure_hypothetical=entry_exposure_hypothetical,
            governance_hypothetical=governance_hypothetical,
        )
        for spec in LONDON_ACTIVE_EVIDENCE_SHADOW_SPECS
    ]
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "timezone": "America/New_York",
        "current_session_classification": label_session_phase(generated_at),
        "broker_authoritative_lane_activation": False,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "paper_proof_invoked": False,
        "shadow_lane_count": len(rows),
        "would_be_candidate_count": sum(int(row["would_be_candidate_count"]) for row in rows),
        "would_be_intent_count": sum(int(row["would_be_intent_count"]) for row in rows),
        "rows": rows,
        "existing_london_candidate_comparison": _existing_london_candidate_comparison(resolved_root),
    }


def write_london_active_evidence_shadow_report(
    *,
    repo_root: Path,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    now: datetime | None = None,
) -> Path:
    report = build_london_active_evidence_shadow_report(repo_root=repo_root, now=now)
    path = output_path if output_path.is_absolute() else repo_root / output_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _evaluate_shadow_spec(
    spec: LondonShadowSpec,
    *,
    repo_root: Path,
    entry_exposure_hypothetical: Mapping[str, Any],
    governance_hypothetical: Mapping[str, Any],
) -> dict[str, Any]:
    path = (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / spec.symbol
        / "1m"
        / "latest_runtime_candles.json"
    )
    history = _phase1_runtime_bar_history_from_path(path)
    completed_history = [
        bar
        for bar in history
        if isinstance(getattr(bar, "end_ts", None), datetime)
        and _time_inside_active_evidence_window(
            getattr(bar, "end_ts").astimezone(NEW_YORK_TZ).timetz().replace(tzinfo=None),
            spec=spec.active_spec,
        )
    ]
    first_fail_reasons: Counter[str] = Counter()
    would_be_candidates = 0
    latest_decision: dict[str, Any] | None = None
    latest_bar_ts: str | None = None
    for bar in completed_history:
        current_end = getattr(bar, "end_ts")
        slice_history = [item for item in history if getattr(item, "end_ts", datetime.max.replace(tzinfo=UTC)) <= current_end]
        decision = _paper_active_evidence_shadow_decision(slice_history, spec=spec.active_spec)
        latest_decision = decision
        latest_bar_ts = current_end.astimezone(UTC).isoformat()
        if decision.get("accepted") is True:
            would_be_candidates += 1
        else:
            first_fail_reasons[str(decision.get("primary_blocker") or "unknown_shadow_blocker")] += 1
    current_phase = None
    if completed_history:
        current_phase = label_session_phase(getattr(completed_history[-1], "end_ts"))
    trade_birth_ready = bool(would_be_candidates > 0 and entry_exposure_hypothetical.get("allowed") is True)
    return {
        "lane_id": spec.lane_id,
        "strategy_id": spec.strategy_id,
        "symbol": spec.symbol,
        "direction": spec.direction,
        "london_window": spec.london_window,
        "active_hours_et": f"{spec.active_spec.start_time_et:%H:%M}-{spec.active_spec.end_time_et:%H:%M}",
        "current_session_phase": current_phase,
        "market_data_artifact_path": str(path),
        "bars_available": len(history),
        "bars_evaluated": len(completed_history),
        "latest_eligible_bar_timestamp": latest_bar_ts,
        "would_be_candidate_count": would_be_candidates,
        "would_be_intent_count": would_be_candidates,
        "latest_first_fail_reason": None if latest_decision and latest_decision.get("accepted") else (latest_decision or {}).get("primary_blocker"),
        "first_fail_reasons": dict(first_fail_reasons),
        "latest_decision": latest_decision,
        "conflict_group": _conflict_group_for_symbol(spec.symbol),
        "hypothetical_trade_id_birth_readiness": {
            "ready": trade_birth_ready,
            "trade_id_created": False,
            "trade_id_preview": f"shadow_{spec.lane_id}_{latest_bar_ts}" if trade_birth_ready and latest_bar_ts else None,
            "reason_codes": [] if trade_birth_ready else _merge_reason_codes(entry_exposure_hypothetical, governance_hypothetical),
        },
        "registry_truth_entry_exposure_hypothetical": dict(entry_exposure_hypothetical),
        "governance_safe_state_hypothetical": dict(governance_hypothetical),
        "broker_action": {
            "broker_authority": False,
            "submit_allowed": False,
            "submit_attempted": False,
            "route_created": False,
            "broker_mutation_allowed": False,
            "lifecycle_authority": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    }


def _paper_active_evidence_shadow_decision(
    bar_history: Sequence[Any],
    *,
    spec: _PaperActiveEvidenceSpec,
) -> dict[str, Any]:
    if not bar_history:
        return _changeover_decision(False, "bar_history_missing")
    current = bar_history[-1]
    current_end = getattr(current, "end_ts", None)
    if not isinstance(current_end, datetime):
        return _changeover_decision(False, "current_bar_end_missing")
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    local_time = current_end_et.timetz().replace(tzinfo=None)
    if not _time_inside_active_evidence_window(local_time, spec=spec):
        return _changeover_decision(
            False,
            "not_in_london_active_evidence_shadow_window",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    reference_bar = _active_evidence_reference_bar(bar_history, current_end=current_end, spec=spec)
    current_close = getattr(current, "close", None)
    reference_price = getattr(reference_bar, "open", None) if reference_bar is not None else None
    reference_source = str(getattr(reference_bar, "reference_source", None) or "BAR_HISTORY") if reference_bar is not None else None
    vwap_price = _rolling_vwap_reference(bar_history)
    if reference_source is None and vwap_price is not None:
        reference_source = "ROLLING_VWAP_REFERENCE"
    if reference_bar is None and vwap_price is None:
        return _changeover_decision(
            False,
            f"{spec.reference_label}_or_rolling_vwap_reference_missing",
            current_close=current_close,
            current_bar_end_et=current_end_et.isoformat(),
            reference_recovery_blocker=f"phase1_1m_{spec.reference_label}_reference_missing",
        )
    close_value = _decimal_or_none(current_close)
    reference_value = _decimal_or_none(reference_price)
    if spec.direction == "SHORT":
        reference_ok = close_value is not None and (
            (reference_value is not None and close_value < reference_value)
            or (vwap_price is not None and close_value < vwap_price)
        )
        continuation_ok = _simple_short_continuation_ok(bar_history)
        blocker = None if reference_ok else f"close_not_below_vwap_or_{spec.reference_label}"
        soft_warning = None if continuation_ok else "simple_short_continuation_not_confirmed"
    else:
        reference_ok = close_value is not None and (
            (reference_value is not None and close_value > reference_value)
            or (vwap_price is not None and close_value > vwap_price)
        )
        continuation_ok = _simple_long_continuation_ok(bar_history)
        blocker = None if reference_ok else f"close_not_above_vwap_or_{spec.reference_label}"
        soft_warning = None if continuation_ok else "simple_long_continuation_not_confirmed"
    decision = _changeover_decision(
        blocker is None,
        blocker,
        current_close=current_close,
        session_open_price=reference_price,
        current_bar_end_et=current_end_et.isoformat(),
        reference_source=reference_source,
    )
    decision["vwap_price"] = None if vwap_price is None else str(vwap_price)
    decision["continuation_confirmed"] = continuation_ok
    decision["recent_close_direction_tag"] = _recent_close_direction_tag(bar_history)
    decision["paper_only_soft_warnings"] = [] if soft_warning is None else [soft_warning]
    return decision


def _hypothetical_registry_truth_entry_exposure(
    *,
    status: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    broker_lifecycle = status.get("broker_lifecycle") if isinstance(status.get("broker_lifecycle"), Mapping) else {}
    broker_order_count = int(_safe_int(diagnostics.get("broker_open_order_count")))
    lifecycle_open_count = int(_safe_int(diagnostics.get("lifecycle_open_position_count")))
    track_b_position_count = int(_safe_int(diagnostics.get("track_b_managed_futures_position_count")))
    review_count = int(_safe_int(diagnostics.get("current_scope_review_required_count")))
    clean = (
        diagnostics.get("classification") == "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE"
        and broker_order_count == 0
        and lifecycle_open_count == 0
        and track_b_position_count == 0
        and review_count == 0
        and broker_lifecycle.get("reconciliation_classification") == "TRACK_B_PAPER_BROKER_RECONCILED"
    )
    reason_codes: list[str] = []
    if diagnostics.get("classification") != "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE":
        reason_codes.append(str(diagnostics.get("classification") or "registry_truth_diagnostics_not_clean"))
    if broker_order_count:
        reason_codes.append("current_scope_open_order_conflict")
    if lifecycle_open_count or track_b_position_count:
        reason_codes.append("current_scope_position_conflict")
    if review_count:
        reason_codes.append("current_scope_review_required")
    if broker_lifecycle.get("reconciliation_classification") != "TRACK_B_PAPER_BROKER_RECONCILED":
        reason_codes.append(str(broker_lifecycle.get("reconciliation_classification") or "broker_lifecycle_not_reconciled"))
    return {
        "mode": "HYPOTHETICAL_READ_ONLY",
        "allowed": clean,
        "reason_codes": reason_codes,
        "diagnostics_classification": diagnostics.get("classification"),
        "broker_lifecycle_reconciliation": broker_lifecycle.get("reconciliation_classification"),
    }


def _hypothetical_governance_safe_state(status: Mapping[str, Any]) -> dict[str, Any]:
    readiness = status.get("readiness") if isinstance(status.get("readiness"), Mapping) else {}
    allowed = bool(readiness.get("ready_submit_capable") is True and readiness.get("submit_allowed") is True)
    reason_codes = [str(item) for item in readiness.get("blockers") or ()]
    if not allowed and not reason_codes:
        reason_codes.append(str(readiness.get("canonical_state") or "submit_authority_not_ready"))
    return {
        "mode": "HYPOTHETICAL_READ_ONLY",
        "allowed_if_broker_authoritative": allowed,
        "reason_codes": reason_codes,
        "canonical_state": readiness.get("canonical_state"),
        "market_schedule_state": readiness.get("market_schedule_state"),
        "submit_allowed": readiness.get("submit_allowed"),
        "ready_submit_capable": readiness.get("ready_submit_capable"),
    }


def _existing_london_candidate_comparison(repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    london_late_path = repo_root / "outputs/track_b_execution_core/session_strategy_state/latest_london_late_pause_resume_short_event_envelope.json"
    london_late = _read_json(london_late_path)
    rows.append(
        {
            "candidate": "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            "source_path": str(london_late_path),
            "available": bool(london_late),
            "symbol": london_late.get("symbol"),
            "submit_allowed": london_late.get("submit_allowed"),
            "submit_attempted": london_late.get("submit_attempted"),
            "broker_mutation_allowed": london_late.get("broker_mutation_allowed"),
            "diagnostic_only": True,
        }
    )
    changeover_path = repo_root / "outputs/track_b_execution_core/research_shadow/latest_changeover_shadow_candidates.json"
    changeover = _read_json(changeover_path)
    candidates = changeover.get("candidates") if isinstance(changeover.get("candidates"), list) else []
    rows.append(
        {
            "candidate": "CHANGEOVER_0300_LONG_CONTINUATION_ABOVE_SESSION_OPEN_SHADOW_V1",
            "source_path": str(changeover_path),
            "available": bool(changeover),
            "matching_candidate_count": sum(
                1
                for item in candidates
                if isinstance(item, Mapping)
                and str(item.get("strategy_id") or item.get("candidate_id")) == "CHANGEOVER_0300_LONG_CONTINUATION_ABOVE_SESSION_OPEN_SHADOW_V1"
            ),
            "diagnostic_only": True,
        }
    )
    mnq_london_config = repo_root / "config/probationary_pattern_engine_paper_mnq_mgc_review_combined.yaml"
    config_text = _read_text(mnq_london_config)
    rows.append(
        {
            "candidate": "MNQ Asia-London participation candidates",
            "source_path": str(mnq_london_config),
            "available": bool(config_text),
            "contains_london_participation": "asia_london_participation" in config_text,
            "submit_capable_without_explicit_approval": False,
            "diagnostic_only": True,
        }
    )
    rows.append(
        {
            "candidate": "gc_mgc_london_open_post_04_transition_shadow_v1",
            "source_path": str(repo_root / "docs/london_open_transition_shadow_observer.md"),
            "available": (repo_root / "docs/london_open_transition_shadow_observer.md").exists(),
            "symbols": ["GC", "MGC"],
            "mnq_mes_comparison_only": True,
            "diagnostic_only": True,
        }
    )
    return rows


def _conflict_group_for_symbol(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized == "MNQ":
        return "equity_index_nasdaq_mnq_nq"
    if normalized == "MES":
        return "equity_index_sp_mes_es"
    return f"unknown_conflict_group_{normalized.lower()}"


def _merge_reason_codes(*payloads: Mapping[str, Any]) -> list[str]:
    merged: list[str] = []
    for payload in payloads:
        for code in payload.get("reason_codes") or ():
            if str(code) not in merged:
                merged.append(str(code))
    return merged


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only London active-evidence shadow coverage report.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    report = build_london_active_evidence_shadow_report(repo_root=repo_root)
    if args.write:
        output = args.output_path if args.output_path.is_absolute() else repo_root / args.output_path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        report["report_path"] = str(output)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


__all__ = [
    "LONDON_ACTIVE_EVIDENCE_SHADOW_SPECS",
    "REPORT_SCHEMA_VERSION",
    "build_london_active_evidence_shadow_report",
    "main",
    "write_london_active_evidence_shadow_report",
]


if __name__ == "__main__":
    raise SystemExit(main())
