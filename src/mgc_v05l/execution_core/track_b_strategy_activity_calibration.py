"""Track B strategy activity / trade-frequency calibration diagnostic.

This diagnostic is read-only. It summarizes completed-bar strategy evaluation
artifacts, intent artifacts, lifecycle ledger summaries, and Track 1 preflight
evidence to explain why Track B is quiet without changing thresholds or routes.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_strategy_registry import TrackBStrategyRegistryEntry, get_track_b_strategy_registry


ACTIVE_TRACK_B_STRATEGIES: tuple[str, ...] = (
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    "FIRST_BULL_SNAP_TURN_V1",
    "FIRST_BEAR_SNAP_TURN_V1",
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    "US_DERIVATIVE_BEAR_TURN_V1",
    "US_LATE_PAUSE_RESUME_LONG_V1",
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    "MNQ_FIRST_BULL_SNAP_TURN_V1",
)

DEFAULT_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_RUNTIME_CYCLE_ROOT = Path("outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle")
DEFAULT_INTENTS_JSONL = Path("outputs/track_b_execution_core/strategy_trade_intents/track_b_strategy_trade_intents.jsonl")
DEFAULT_TRADE_SUMMARY_JSON = Path("outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json")
DEFAULT_TRACK1_PREFLIGHT_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track1_trading_stop_preflight.json"
DEFAULT_TRACK1_BREAKPOINT_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track1_signal_to_handoff_breakpoint_audit.json"
DEFAULT_TRACK1_PARITY_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track1_trackb_same_day_parity_audit.json"


@dataclass(frozen=True)
class TrackBStrategyActivityCalibrationConfig:
    repo_root: Path = Path(".")
    diagnostics_root: Path = DEFAULT_DIAGNOSTICS_ROOT
    runtime_cycle_root: Path = DEFAULT_RUNTIME_CYCLE_ROOT
    intents_jsonl: Path = DEFAULT_INTENTS_JSONL
    trade_summary_json: Path = DEFAULT_TRADE_SUMMARY_JSON
    track1_preflight_json: Path = DEFAULT_TRACK1_PREFLIGHT_JSON
    track1_breakpoint_json: Path = DEFAULT_TRACK1_BREAKPOINT_JSON
    track1_parity_json: Path = DEFAULT_TRACK1_PARITY_JSON
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_strategy_activity_calibration.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_strategy_activity_calibration.md"
    max_runtime_reports: int = 1200
    analysis_date: str | None = None


@dataclass(frozen=True)
class TrackBStrategyActivityCalibrationResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def create_track_b_strategy_activity_calibration(
    *,
    config: TrackBStrategyActivityCalibrationConfig | None = None,
    now: datetime | None = None,
) -> TrackBStrategyActivityCalibrationResult:
    actual_config = config or TrackBStrategyActivityCalibrationConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    target_date = _target_date(actual_config.analysis_date, actual_now)
    repo_root = Path(actual_config.repo_root)
    registry = _active_registry()
    runtime_reports = _load_runtime_reports(
        root=_resolve(repo_root, actual_config.runtime_cycle_root),
        max_reports=actual_config.max_runtime_reports,
    )
    today_reports = [item for item in runtime_reports if _date_prefix(item.get("generated_at")) == target_date.isoformat()]
    analysis_reports = today_reports if today_reports else runtime_reports[-min(len(runtime_reports), 200) :]
    intent_rows = _read_jsonl(_resolve(repo_root, actual_config.intents_jsonl))
    trade_summary = _load_json(_resolve(repo_root, actual_config.trade_summary_json))
    track1_preflight = _load_json(_resolve(repo_root, actual_config.track1_preflight_json))
    track1_breakpoint = _load_json(_resolve(repo_root, actual_config.track1_breakpoint_json))
    track1_parity = _load_json(_resolve(repo_root, actual_config.track1_parity_json))
    strategy_rows = _strategy_rows(
        registry=registry,
        runtime_reports=analysis_reports,
        intent_rows=intent_rows,
        trade_summary=trade_summary,
        target_date=target_date,
    )
    instruments = _instrument_rows(strategy_rows)
    recommendations = _recommendations(strategy_rows, track1_preflight, track1_breakpoint, track1_parity)
    report = {
        "schema_version": "track_b_strategy_activity_calibration_v1",
        "generated_at": actual_now.isoformat(),
        "analysis_date": target_date.isoformat(),
        "window_start": _min_time(item.get("generated_at") for item in analysis_reports),
        "window_end": _max_time(item.get("generated_at") for item in analysis_reports),
        "source": "TRACK_B_RUNTIME_CYCLE_ARTIFACTS",
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "broker_state_mutated": False,
        "runtime_reports_scanned": len(analysis_reports),
        "bounded_runtime_report_limit": actual_config.max_runtime_reports,
        "active_strategy_count": len(ACTIVE_TRACK_B_STRATEGIES),
        "active_strategies": list(ACTIVE_TRACK_B_STRATEGIES),
        "instruments": instruments,
        "strategies": strategy_rows,
        "totals": _totals(strategy_rows, trade_summary),
        "track1_expectations": _track1_expectations(track1_preflight, track1_breakpoint, track1_parity),
        "classification_summary": _classification_summary(strategy_rows),
        "recommendations": recommendations,
        "outputs": {
            "json": str(actual_config.output_json),
            "markdown": str(actual_config.output_md),
        },
    }
    _write_json(_resolve(repo_root, actual_config.output_json), report)
    _write_text(_resolve(repo_root, actual_config.output_md), _markdown(report))
    return TrackBStrategyActivityCalibrationResult(
        report_json=_resolve(repo_root, actual_config.output_json),
        report_md=_resolve(repo_root, actual_config.output_md),
        report=report,
    )


def _strategy_rows(
    *,
    registry: Mapping[str, TrackBStrategyRegistryEntry],
    runtime_reports: Iterable[Mapping[str, Any]],
    intent_rows: Iterable[Mapping[str, Any]],
    trade_summary: Mapping[str, Any],
    target_date: date,
) -> list[dict[str, Any]]:
    rows = {strategy_id: _empty_strategy_row(strategy_id, registry.get(strategy_id)) for strategy_id in ACTIVE_TRACK_B_STRATEGIES}
    completed_bars_by_instrument: dict[str, set[str]] = defaultdict(set)
    evaluated_bars_by_strategy: dict[str, set[str]] = defaultdict(set)
    for runtime in runtime_reports:
        generated_at = str(runtime.get("generated_at") or "")
        instrument = _runtime_instrument(runtime)
        if instrument:
            completed_bars_by_instrument[instrument].add(generated_at)
        suppressed_ids = _suppressed_strategy_ids(runtime)
        candidate_ids = _candidate_strategy_ids(runtime)
        handoff_seen = bool(runtime.get("paper_runner_report_path") or runtime.get("submit_attempted"))
        for strategy in runtime.get("evaluated_strategies") or []:
            if not isinstance(strategy, Mapping):
                continue
            strategy_id = _strategy_id(strategy)
            if strategy_id not in rows:
                continue
            row = rows[strategy_id]
            instrument = str(row.get("instrument") or _runtime_instrument(runtime) or "")
            if generated_at:
                evaluated_bars_by_strategy[strategy_id].add(generated_at)
                completed_bars_by_instrument[instrument].add(generated_at)
            result = _strategy_result(strategy, candidate_ids=candidate_ids, suppressed_ids=suppressed_ids)
            blockers = _failed_predicates(strategy)
            missing = _missing_fields(strategy, blockers)
            near = _near_miss(blockers, strategy)
            row["strategy_evaluations"] = int(row["strategy_evaluations"]) + 1
            row["hard_signals"] = int(row["hard_signals"]) + (1 if result == "SIGNAL" else 0)
            row["no_signal_count"] = int(row["no_signal_count"]) + (1 if result == "NO_SIGNAL" else 0)
            row["suppressed_count"] = int(row["suppressed_count"]) + (1 if result == "SUPPRESSED" else 0)
            row["handoff_seen_count"] = int(row["handoff_seen_count"]) + (1 if result == "SIGNAL" and handoff_seen else 0)
            row["_failed"].update(blockers)
            row["_missing"].update(missing)
            row["_session"].update([item for item in blockers if _is_session_predicate(item)])
            row["_state"].update([item for item in blockers if _is_state_predicate(item)])
            row["_structure"].update([item for item in blockers if not _is_session_predicate(item) and not _is_state_predicate(item)])
            session_state = _session_state(strategy, blockers)
            row[f"session_filter_{session_state}_bars"] = int(row.get(f"session_filter_{session_state}_bars") or 0) + 1
            if near["bucket"] == "ONE_PREDICATE_AWAY":
                row["one_predicate_away"] = int(row["one_predicate_away"]) + 1
            if near["bucket"] == "TWO_PREDICATES_AWAY":
                row["two_predicates_away"] = int(row["two_predicates_away"]) + 1
            if near["bucket"] in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
                row.setdefault("_near_examples", []).append(
                    {
                        "timestamp": generated_at,
                        "bucket": near["bucket"],
                        "failed_predicates_count": near["failed_count"],
                        "nearest_failed_predicate": near["nearest_failed_predicate"],
                    }
                )
    intent_counts = _count_by_strategy(intent_rows, target_date, "strategy_id")
    trade_counts = _trade_counts_by_strategy(trade_summary)
    for strategy_id, row in rows.items():
        instrument = str(row.get("instrument") or "")
        row["completed_5m_bars_evaluated_by_instrument"] = len(completed_bars_by_instrument.get(instrument, set()))
        row["evaluated_completed_5m_bars"] = len(evaluated_bars_by_strategy.get(strategy_id, set()))
        row["intent_count"] = int(intent_counts.get(strategy_id, 0))
        row["managed_lifecycle_count"] = int(trade_counts.get(strategy_id, {}).get("managed_lifecycle_count", 0))
        row["meaningful_managed_trade_count"] = int(trade_counts.get(strategy_id, {}).get("meaningful_managed_trade_count", 0))
        row["broker_backed_trade_count"] = int(trade_counts.get(strategy_id, {}).get("broker_backed_trade_count", 0))
        row["top_failed_predicates"] = _counter_rows(row.pop("_failed"), 8)
        row["top_missing_fields"] = _counter_rows(row.pop("_missing"), 8)
        row["top_session_blockers"] = _counter_rows(row.pop("_session"), 8)
        row["top_state_blockers"] = _counter_rows(row.pop("_state"), 8)
        row["top_structure_blockers"] = _counter_rows(row.pop("_structure"), 8)
        row["closest_near_misses"] = (row.pop("_near_examples", []) or [])[:5]
        row["classification"] = classify_quiet_strategy(row)
        row["recommended_minimum_change"] = _minimum_change_for_strategy(row)
    return [rows[strategy_id] for strategy_id in ACTIVE_TRACK_B_STRATEGIES]


def classify_quiet_strategy(row: Mapping[str, Any]) -> str:
    evaluations = int(row.get("strategy_evaluations") or 0)
    signals = int(row.get("hard_signals") or 0)
    suppressed = int(row.get("suppressed_count") or 0)
    missing = int(sum(int(item.get("count") or 0) for item in row.get("top_missing_fields") or []))
    session_inactive = int(row.get("session_filter_inactive_bars") or 0)
    near = int(row.get("one_predicate_away") or 0) + int(row.get("two_predicates_away") or 0)
    top_failed = [str(item.get("reason") or "") for item in row.get("top_failed_predicates") or []]
    if row.get("registered") is not True or row.get("enabled") is not True:
        return "QUIET_DUE_TO_NOT_MIGRATED_OR_NOT_ENABLED"
    if evaluations <= 0:
        return "QUIET_DUE_TO_NOT_MIGRATED_OR_NOT_ENABLED"
    if suppressed > 0:
        return "QUIET_DUE_TO_ARBITRATION"
    if signals > 0 and int(row.get("intent_count") or 0) < signals:
        return "QUIET_DUE_TO_EXTRA_TRACK_B_GATE"
    if missing > 0:
        return "QUIET_DUE_TO_FIELD_OR_SESSION_MISMATCH"
    if evaluations and session_inactive / max(evaluations, 1) >= 0.5:
        return "QUIET_DUE_TO_SESSION_FILTER"
    if any("no_first_" in item or "cooldown" in item or "competing" in item for item in top_failed):
        return "QUIET_DUE_TO_EXTRA_TRACK_B_GATE"
    if near > 0 or top_failed:
        return "QUIET_DUE_TO_PREDICATES_TOO_STRICT"
    return "QUIET_BUT_EXPECTED_FOR_REGIME"


def _empty_strategy_row(strategy_id: str, entry: TrackBStrategyRegistryEntry | None) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "instrument": None if entry is None else entry.instrument_family,
        "side": _side_for_strategy(strategy_id),
        "session": _session_for_strategy(strategy_id, entry),
        "registered": entry is not None,
        "enabled": True,
        "paper_eligible": None if entry is None else entry.paper_eligible,
        "live_money_eligible": None if entry is None else entry.live_money_eligible,
        "managed_exit_policy_id": None if entry is None else entry.managed_exit_policy_id,
        "required_1m_context_bars": None if entry is None else entry.required_1m_context_bars,
        "required_5m_context_bars": None if entry is None else entry.required_5m_context_bars,
        "completed_5m_bars_evaluated_by_instrument": 0,
        "evaluated_completed_5m_bars": 0,
        "strategy_evaluations": 0,
        "hard_signals": 0,
        "no_signal_count": 0,
        "suppressed_count": 0,
        "handoff_seen_count": 0,
        "intent_count": 0,
        "managed_lifecycle_count": 0,
        "meaningful_managed_trade_count": 0,
        "broker_backed_trade_count": 0,
        "one_predicate_away": 0,
        "two_predicates_away": 0,
        "session_filter_active_bars": 0,
        "session_filter_inactive_bars": 0,
        "session_filter_unknown_bars": 0,
        "_failed": Counter(),
        "_missing": Counter(),
        "_session": Counter(),
        "_state": Counter(),
        "_structure": Counter(),
    }


def _instrument_rows(strategy_rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_instrument: dict[str, dict[str, Any]] = {}
    for row in strategy_rows:
        instrument = str(row.get("instrument") or "UNKNOWN")
        item = by_instrument.setdefault(
            instrument,
            {
                "instrument": instrument,
                "enabled_strategy_count": 0,
                "completed_5m_bars_evaluated": int(row.get("completed_5m_bars_evaluated_by_instrument") or 0),
                "strategy_evaluations": 0,
                "hard_signals": 0,
                "one_predicate_away": 0,
                "two_predicates_away": 0,
                "intent_count": 0,
                "meaningful_managed_trade_count": 0,
                "top_failed_predicates": Counter(),
            },
        )
        item["enabled_strategy_count"] += 1
        item["completed_5m_bars_evaluated"] = max(
            int(item["completed_5m_bars_evaluated"]),
            int(row.get("completed_5m_bars_evaluated_by_instrument") or 0),
        )
        for key in ("strategy_evaluations", "hard_signals", "one_predicate_away", "two_predicates_away", "intent_count", "meaningful_managed_trade_count"):
            item[key] += int(row.get(key) or 0)
        item["top_failed_predicates"].update(
            {str(pred.get("reason")): int(pred.get("count") or 0) for pred in row.get("top_failed_predicates") or []}
        )
    return [
        {
            **{key: value for key, value in item.items() if key != "top_failed_predicates"},
            "top_failed_predicates": _counter_rows(item["top_failed_predicates"], 10),
        }
        for item in sorted(by_instrument.values(), key=lambda value: str(value.get("instrument")))
    ]


def _recommendations(
    strategy_rows: list[Mapping[str, Any]],
    track1_preflight: Mapping[str, Any],
    track1_breakpoint: Mapping[str, Any],
    track1_parity: Mapping[str, Any],
) -> dict[str, list[dict[str, str]]]:
    rows_by_class = defaultdict(list)
    for row in strategy_rows:
        rows_by_class[str(row.get("classification"))].append(str(row.get("strategy_id")))
    recommendations: dict[str, list[dict[str, str]]] = {
        "A_fix_parity_or_implementation_mismatch": [],
        "B_adjust_strategy_predicate_or_gate": [],
        "C_add_or_migrate_missing_strategy_coverage": [],
        "D_change_arbitration_or_data_collection_policy": [],
        "E_no_change": [],
    }
    if track1_breakpoint.get("classification") == "HANDOFF_INTENT_NOT_CREATED":
        recommendations["A_fix_parity_or_implementation_mismatch"].append(
            {
                "scope": "legacy signal-to-intent bridge",
                "recommendation": "Keep the new Track B signal-to-intent bridge; do not spend first effort on deep parity until current Track B predicate quietness is separated from the old handoff break.",
            }
        )
    if track1_parity.get("summary", {}).get("primary_classification") == "TRACK1_REFERENCE_UNAVAILABLE":
        recommendations["A_fix_parity_or_implementation_mismatch"].append(
            {
                "scope": "Track 1 references",
                "recommendation": "Export/preserve executable Track 1 replay signal rows for the migrated strategies; current parity audit cannot prove same-day semantic parity.",
            }
        )
    if rows_by_class.get("QUIET_DUE_TO_FIELD_OR_SESSION_MISMATCH"):
        recommendations["A_fix_parity_or_implementation_mismatch"].append(
            {
                "scope": ", ".join(rows_by_class["QUIET_DUE_TO_FIELD_OR_SESSION_MISMATCH"][:6]),
                "recommendation": "Inspect missing/defaulted fields and session label mapping before changing thresholds.",
            }
        )
    if rows_by_class.get("QUIET_DUE_TO_EXTRA_TRACK_B_GATE"):
        recommendations["A_fix_parity_or_implementation_mismatch"].append(
            {
                "scope": ", ".join(rows_by_class["QUIET_DUE_TO_EXTRA_TRACK_B_GATE"][:6]),
                "recommendation": "Audit anti-churn/no-competing/cooldown gates against Track 1; remove only accidental Track B-only gates.",
            }
        )
    if rows_by_class.get("QUIET_DUE_TO_PREDICATES_TOO_STRICT"):
        recommendations["B_adjust_strategy_predicate_or_gate"].append(
            {
                "scope": ", ".join(rows_by_class["QUIET_DUE_TO_PREDICATES_TOO_STRICT"][:8]),
                "recommendation": "Run replay sensitivity on dominant failed predicates and one/two-predicate near misses; do not loosen live thresholds directly.",
            }
        )
    recommendations["C_add_or_migrate_missing_strategy_coverage"].append(
        {
            "scope": "Track 1 GC/MGC lane families and trend-continuation candidates",
            "recommendation": "Track 1 evidence shows GC lane strategies traded; current Track B active set is MGC/MNQ only and largely snap/pause/retest/turn. Migrate validated missing coverage after parity checks.",
        }
    )
    if rows_by_class.get("QUIET_DUE_TO_ARBITRATION"):
        recommendations["D_change_arbitration_or_data_collection_policy"].append(
            {
                "scope": ", ".join(rows_by_class["QUIET_DUE_TO_ARBITRATION"][:6]),
                "recommendation": "Review arbitration suppression reasons; do not change signal predicates until suppression is proven accidental.",
            }
        )
    if rows_by_class.get("QUIET_BUT_EXPECTED_FOR_REGIME"):
        recommendations["E_no_change"].append(
            {
                "scope": ", ".join(rows_by_class["QUIET_BUT_EXPECTED_FOR_REGIME"][:6]),
                "recommendation": "No immediate change from this bounded window; collect more completed decision bars.",
            }
        )
    return recommendations


def _totals(strategy_rows: list[Mapping[str, Any]], trade_summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "completed_5m_bars_evaluated_by_instrument": {
            instrument: max(int(row.get("completed_5m_bars_evaluated_by_instrument") or 0) for row in rows)
            for instrument, rows in _group_by(strategy_rows, "instrument").items()
        },
        "strategy_evaluations": sum(int(row.get("strategy_evaluations") or 0) for row in strategy_rows),
        "hard_signals": sum(int(row.get("hard_signals") or 0) for row in strategy_rows),
        "one_predicate_away": sum(int(row.get("one_predicate_away") or 0) for row in strategy_rows),
        "two_predicates_away": sum(int(row.get("two_predicates_away") or 0) for row in strategy_rows),
        "suppressed": sum(int(row.get("suppressed_count") or 0) for row in strategy_rows),
        "intents": sum(int(row.get("intent_count") or 0) for row in strategy_rows),
        "meaningful_managed_trades": int(trade_summary.get("managed_strategy_trade_count") or 0),
        "open_position_count": int(trade_summary.get("open_position_count") or 0),
        "review_required_count": int(trade_summary.get("review_required_count") or 0),
    }


def _track1_expectations(
    preflight: Mapping[str, Any],
    breakpoint: Mapping[str, Any],
    parity: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = preflight.get("evidence_summary") if isinstance(preflight.get("evidence_summary"), Mapping) else {}
    last_trade = breakpoint.get("last_known_trade_detail") if isinstance(breakpoint.get("last_known_trade_detail"), Mapping) else {}
    return {
        "preflight_classification": preflight.get("classification"),
        "breakpoint_classification": breakpoint.get("classification"),
        "missing_link": breakpoint.get("missing_link"),
        "last_known_track1_trade": last_trade,
        "direct_trade_rows_found": evidence.get("direct_trade_rows_found"),
        "total_signal_count_found": evidence.get("total_signal_count_found"),
        "handoff_artifact_count": preflight.get("handoff_artifact_count"),
        "parity_primary_classification": (parity.get("summary") or {}).get("primary_classification")
        if isinstance(parity.get("summary"), Mapping)
        else parity.get("classification"),
        "interpretation": "Track 1 evidence supports prior strategy activity but points to a legacy signal-to-intent break; current Track B quietness must be calibrated from predicate/session attribution.",
    }


def _classification_summary(strategy_rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(str(row.get("classification")) for row in strategy_rows)
    return _counter_rows(counts, 20)


def _minimum_change_for_strategy(row: Mapping[str, Any]) -> str:
    classification = str(row.get("classification") or "")
    if classification == "QUIET_DUE_TO_SESSION_FILTER":
        return "Verify intended session scope and session-label parity; do not change price predicates yet."
    if classification == "QUIET_DUE_TO_FIELD_OR_SESSION_MISMATCH":
        return "Fix missing/defaulted feature or session field mapping before threshold work."
    if classification == "QUIET_DUE_TO_EXTRA_TRACK_B_GATE":
        return "Audit Track B-only anti-churn/competing-signal/cooldown gate against Track 1."
    if classification == "QUIET_DUE_TO_PREDICATES_TOO_STRICT":
        top = row.get("top_failed_predicates") or []
        reason = None if not top else top[0].get("reason")
        return f"Run bounded replay sensitivity on dominant predicate `{reason}` before changing thresholds."
    if classification == "QUIET_DUE_TO_ARBITRATION":
        return "Inspect arbitration suppression reasons and candidate ordering."
    if classification == "QUIET_DUE_TO_NOT_MIGRATED_OR_NOT_ENABLED":
        return "Wire into completed-bar evaluation or remove from active inventory; register/enable only if Track 1 evidence supports it."
    return "Collect more eligible completed bars or compare against Track 1 reference exports."


def _load_runtime_reports(*, root: Path, max_reports: int) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    paths = sorted(root.glob("*/track_b_multi_strategy_runtime_cycle_report.json"), key=lambda path: path.stat().st_mtime)
    selected = paths[-max(1, int(max_reports)) :]
    reports = [_load_json(path) for path in selected]
    return sorted([item for item in reports if item], key=lambda item: str(item.get("generated_at") or ""))


def _active_registry() -> dict[str, TrackBStrategyRegistryEntry]:
    entries = {entry.strategy_id: entry for entry in get_track_b_strategy_registry()}
    aliases: dict[str, TrackBStrategyRegistryEntry] = {}
    for entry in entries.values():
        aliases[entry.strategy_id] = entry
        aliases[entry.rule_id] = entry
        aliases[entry.rule_mode] = entry
    return aliases


def _runtime_instrument(runtime: Mapping[str, Any]) -> str | None:
    for strategy in runtime.get("evaluated_strategies") or []:
        if not isinstance(strategy, Mapping):
            continue
        metadata = strategy.get("registry_metadata") if isinstance(strategy.get("registry_metadata"), Mapping) else {}
        instrument = metadata.get("strategy_registry_instrument_family") or strategy.get("strategy_registry_instrument_family")
        if instrument:
            return str(instrument)
    for candidate in runtime.get("candidate_signals") or []:
        if isinstance(candidate, Mapping) and candidate.get("instrument_family"):
            return str(candidate.get("instrument_family"))
    return None


def _strategy_id(strategy: Mapping[str, Any]) -> str:
    metadata = strategy.get("registry_metadata") if isinstance(strategy.get("registry_metadata"), Mapping) else {}
    return str(metadata.get("strategy_registry_id") or strategy.get("strategy_registry_id") or strategy.get("strategy_id") or strategy.get("signal_source") or "")


def _strategy_result(strategy: Mapping[str, Any], *, candidate_ids: set[str], suppressed_ids: set[str]) -> str:
    strategy_id = _strategy_id(strategy)
    if strategy_id in suppressed_ids:
        return "SUPPRESSED"
    if strategy_id in candidate_ids or strategy.get("signal_emitted") is True or str(strategy.get("decision") or "").upper() == "SIGNAL":
        return "SIGNAL"
    decision = str(strategy.get("decision") or "").upper()
    return decision if decision in {"NO_SIGNAL", "NOT_READY", "ERROR"} else "NO_SIGNAL"


def _candidate_strategy_ids(runtime: Mapping[str, Any]) -> set[str]:
    return {
        str(item.get("strategy_id") or item.get("signal_source") or "")
        for item in runtime.get("candidate_signals") or []
        if isinstance(item, Mapping)
    }


def _suppressed_strategy_ids(runtime: Mapping[str, Any]) -> set[str]:
    return {
        str(item.get("strategy_id") or item.get("signal_source") or "")
        for item in runtime.get("suppressed_signals") or []
        if isinstance(item, Mapping)
    }


def _failed_predicates(strategy: Mapping[str, Any]) -> list[str]:
    blockers = [str(item).split("=", 1)[0].strip() for item in strategy.get("rule_blockers") or [] if item]
    if blockers:
        return blockers
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    return [str(key) for key, value in conditions.items() if value is False]


def _missing_fields(strategy: Mapping[str, Any], blockers: list[str]) -> list[str]:
    reason = str(strategy.get("primary_blocker") or strategy.get("decision_reason") or "")
    values = list(blockers)
    if reason:
        values.append(reason)
    return [item for item in values if "missing" in item.lower() or "required" in item.lower() or "not_provided" in item.lower()]


def _near_miss(blockers: list[str], strategy: Mapping[str, Any]) -> dict[str, Any]:
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    failed = len(blockers)
    passed = sum(1 for value in conditions.values() if value is True) if conditions else None
    if failed == 1:
        bucket = "ONE_PREDICATE_AWAY"
    elif failed == 2:
        bucket = "TWO_PREDICATES_AWAY"
    elif failed > 2:
        bucket = "MULTI_PREDICATE_FAIL"
    else:
        bucket = "NOT_SCORABLE"
    return {
        "bucket": bucket,
        "failed_count": failed,
        "passed_count": passed,
        "nearest_failed_predicate": blockers[0] if blockers else None,
    }


def _session_state(strategy: Mapping[str, Any], blockers: list[str]) -> str:
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    session_keys = [key for key in conditions if _is_session_predicate(str(key))]
    if any(_is_session_predicate(item) for item in blockers):
        return "inactive"
    if session_keys and all(conditions.get(key) is True for key in session_keys):
        return "active"
    return "unknown"


def _is_session_predicate(value: str) -> bool:
    lowered = value.lower()
    return "session" in lowered or "phase" in lowered or "window" in lowered or "asia_early" in lowered or "london" in lowered


def _is_state_predicate(value: str) -> bool:
    lowered = value.lower()
    return "cooldown" in lowered or "prior_bars" in lowered or "competing" in lowered or "no_first" in lowered or "anti_churn" in lowered


def _strategy_has_session_scope(row: Mapping[str, Any]) -> bool:
    session = str(row.get("session") or "").upper()
    return bool(session and session != "ALL")


def _count_by_strategy(rows: Iterable[Mapping[str, Any]], target_date: date, field: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        if _date_prefix(row.get("created_at") or row.get("generated_at")) != target_date.isoformat():
            continue
        value = row.get(field)
        if value:
            counter[str(value)] += 1
    return counter


def _trade_counts_by_strategy(trade_summary: Mapping[str, Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for trade in trade_summary.get("recent_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        strategy_id = str(trade.get("strategy_id") or "")
        if not strategy_id:
            continue
        counts[strategy_id]["managed_lifecycle_count"] += 1 if trade.get("paper_lifecycle_type") == "STRATEGY_MANAGED" else 0
        counts[strategy_id]["meaningful_managed_trade_count"] += 1 if _is_meaningful_trade(trade) else 0
        counts[strategy_id]["broker_backed_trade_count"] += 1 if trade.get("broker_backed_position_confirmed") is True else 0
    return {strategy_id: dict(value) for strategy_id, value in counts.items()}


def _is_meaningful_trade(trade: Mapping[str, Any]) -> bool:
    return bool(
        trade.get("paper_lifecycle_type") == "STRATEGY_MANAGED"
        and trade.get("broker_backed_position_confirmed") is True
        and str(trade.get("final_position_status") or "") in {"OPEN_MANAGED", "CLOSED_FLAT"}
    )


def _side_for_strategy(strategy_id: str) -> str:
    lowered = strategy_id.lower()
    if "bear" in lowered or "short" in lowered:
        return "SHORT"
    if "bull" in lowered or "long" in lowered or "drift" in lowered:
        return "LONG_OR_STATE_EXPLICIT" if "drift" in lowered else "LONG"
    return "UNKNOWN"


def _session_for_strategy(strategy_id: str, entry: TrackBStrategyRegistryEntry | None) -> str:
    text = " ".join([strategy_id, *(entry.required_state_schema if entry else ())]).lower()
    if "asia_early" in text:
        return "ASIA_EARLY"
    if "asia_late" in text:
        return "ASIA_LATE"
    if "asian" in text or "session_asia" in text:
        return "ASIA"
    if "london" in text:
        return "LONDON"
    if "us_late" in text:
        return "US_LATE"
    if "session_us" in text or "us_derivative" in text:
        return "US"
    if "session_allowed" in text:
        return "REGISTRY_SESSION_ALLOWED"
    return "UNKNOWN"


def _group_by(rows: Iterable[Mapping[str, Any]], key: str) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "UNKNOWN")].append(row)
    return grouped


def _counter_rows(counter: Counter[str], limit: int) -> list[dict[str, Any]]:
    return [{"reason": reason, "count": count} for reason, count in counter.most_common(limit)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    rows = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _target_date(value: str | None, now: datetime) -> date:
    if value:
        return date.fromisoformat(value)
    return now.astimezone(UTC).date()


def _date_prefix(value: object) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)[:10]


def _min_time(values: Iterable[object]) -> str | None:
    strings = sorted(str(value) for value in values if value)
    return strings[0] if strings else None


def _max_time(values: Iterable[object]) -> str | None:
    strings = sorted(str(value) for value in values if value)
    return strings[-1] if strings else None


def _markdown(report: Mapping[str, Any]) -> str:
    totals = report.get("totals") if isinstance(report.get("totals"), Mapping) else {}
    lines = [
        "# Track B Strategy Activity Calibration",
        "",
        f"- Generated: `{report.get('generated_at')}`",
        f"- Analysis date: `{report.get('analysis_date')}`",
        f"- Window: `{report.get('window_start')}` to `{report.get('window_end')}`",
        f"- Runtime reports scanned: `{report.get('runtime_reports_scanned')}`",
        f"- Strategy evaluations: `{totals.get('strategy_evaluations')}`",
        f"- Hard signals: `{totals.get('hard_signals')}`",
        f"- Intents: `{totals.get('intents')}`",
        f"- Meaningful managed trades: `{totals.get('meaningful_managed_trades')}`",
        f"- Open positions: `{totals.get('open_position_count')}`",
        f"- Review required: `{totals.get('review_required_count')}`",
        "",
        "## Plain Answer",
        "",
        _plain_answer(report),
        "",
        "## Instrument Summary",
        "",
    ]
    for item in report.get("instruments") or []:
        lines.append(
            f"- `{item.get('instrument')}`: bars `{item.get('completed_5m_bars_evaluated')}`, "
            f"evals `{item.get('strategy_evaluations')}`, signals `{item.get('hard_signals')}`, "
            f"near misses `{item.get('one_predicate_away')}` one-away / `{item.get('two_predicates_away')}` two-away, "
            f"managed trades `{item.get('meaningful_managed_trade_count')}`."
        )
    lines.extend(["", "## Strategy Matrix", ""])
    for row in report.get("strategies") or []:
        top = row.get("top_failed_predicates") or []
        top_text = ", ".join(f"{item.get('reason')} ({item.get('count')})" for item in top[:3]) or "none"
        lines.append(
            f"- `{row.get('strategy_id')}` [{row.get('instrument')}, {row.get('side')}, {row.get('session')}]: "
            f"evals `{row.get('strategy_evaluations')}`, signals `{row.get('hard_signals')}`, "
            f"intents `{row.get('intent_count')}`, trades `{row.get('meaningful_managed_trade_count')}`, "
            f"near `{row.get('one_predicate_away')}`/`{row.get('two_predicates_away')}`, "
            f"classification `{row.get('classification')}`. Top blockers: {top_text}. "
            f"Minimum change: {row.get('recommended_minimum_change')}"
        )
    lines.extend(["", "## Track 1 Expectation Context", ""])
    track1 = report.get("track1_expectations") or {}
    lines.extend(
        [
            f"- Preflight classification: `{track1.get('preflight_classification')}`",
            f"- Breakpoint classification: `{track1.get('breakpoint_classification')}`",
            f"- Missing link: `{track1.get('missing_link')}`",
            f"- Parity status: `{track1.get('parity_primary_classification')}`",
            f"- Interpretation: {track1.get('interpretation')}",
            "",
            "## Recommendations",
            "",
        ]
    )
    recommendations = report.get("recommendations") if isinstance(report.get("recommendations"), Mapping) else {}
    for section, items in recommendations.items():
        lines.append(f"### {section}")
        if not items:
            lines.append("- None from this bounded diagnostic.")
        for item in items:
            lines.append(f"- `{item.get('scope')}`: {item.get('recommendation')}")
        lines.append("")
    lines.extend(
        [
            "## Safety",
            "",
            "- broker_commands_invoked=false",
            "- paper_proof_cli_invoked=false",
            "- submit_cancel_place_order_invoked=false",
            "- broker_state_mutated=false",
        ]
    )
    return "\n".join(lines) + "\n"


def _plain_answer(report: Mapping[str, Any]) -> str:
    totals = report.get("totals") if isinstance(report.get("totals"), Mapping) else {}
    classifications = {item.get("reason"): item.get("count") for item in report.get("classification_summary") or []}
    if int(totals.get("strategy_evaluations") or 0) <= 0:
        return "Track B did not produce enough completed-bar evaluations in the retained window to calibrate trade frequency."
    if int(totals.get("hard_signals") or 0) > int(totals.get("meaningful_managed_trades") or 0):
        return "Track B produced hard signals, but not all became meaningful broker-backed managed trades; inspect intent/lifecycle rows before changing predicates."
    if classifications.get("QUIET_DUE_TO_PREDICATES_TOO_STRICT") or classifications.get("QUIET_DUE_TO_SESSION_FILTER"):
        return "Track B is evaluating, but the active strategies are mostly muted by session filters and strict structure/predicate gates. The smallest safe next step is parity/field checks plus replay sensitivity on dominant failed predicates, not blind threshold loosening."
    return "Track B is quiet in the retained window, but this diagnostic did not isolate a single dominant blocker."
