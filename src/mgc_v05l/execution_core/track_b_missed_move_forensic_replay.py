"""Read-only missed-move forensic replay for Track B PAPER.

This diagnostic explains why retained MGC/MNQ completed 5m decision bars did
not produce Track B PAPER activity. It reads bounded Track B monitor/runtime
artifacts only and never invokes broker or PAPER lifecycle code.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_JSON = (
    DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT / "latest_track_b_missed_move_forensic_replay.json"
)
DEFAULT_INSTRUMENTS = ("MGC", "MNQ")
DEFAULT_MAX_RUNTIME_REPORTS = 2000
MAX_REPORT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class TrackBMissedMoveForensicReplayResult:
    report_json: Path
    report: dict[str, Any]


def build_track_b_missed_move_forensic_replay(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT,
    instruments: Sequence[str] = DEFAULT_INSTRUMENTS,
    max_runtime_reports: int = DEFAULT_MAX_RUNTIME_REPORTS,
    now: datetime | None = None,
    write: bool = True,
) -> TrackBMissedMoveForensicReplayResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    output_root = Path(output_root)
    instrument_set = {str(item).upper() for item in instruments}
    runtime_root = root / "outputs" / "track_b_execution_core" / "track_b_multi_strategy_runtime_cycle"
    monitor_root = root / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    feed_root = root / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed"
    readiness_path = root / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track_b_startup_readiness_diagnostic.json"

    runtime_reports = _load_today_runtime_reports(
        runtime_root=runtime_root,
        instrument_set=instrument_set,
        now=actual_now,
        limit=max_runtime_reports,
    )
    monitor_reports = _load_today_monitor_reports(monitor_root=monitor_root, now=actual_now, limit=max_runtime_reports)
    readiness = _load_json(readiness_path)
    ohlc_by_instrument = {
        instrument: _load_completed_5m_bars(feed_root, instrument)
        for instrument in sorted(instrument_set)
    }
    live_evaluated_bars = _live_evaluated_bars(monitor_reports)
    enabled_by_instrument = _enabled_by_instrument(monitor_reports, runtime_reports, instrument_set)

    bar_rows: list[dict[str, Any]] = []
    strategy_rows: list[dict[str, Any]] = []
    top_failed_predicates: Counter[str] = Counter()
    failed_by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    blockers_by_gate: Counter[str] = Counter()
    near_miss_counts: Counter[str] = Counter()
    strategy_near_counts: Counter[str] = Counter()
    strategy_eval_counts: Counter[str] = Counter()
    strategy_signal_counts: Counter[str] = Counter()
    strategy_no_signal_counts: Counter[str] = Counter()
    strategy_suppressed_counts: Counter[str] = Counter()
    strategy_handoff_counts: Counter[str] = Counter()

    for runtime in runtime_reports:
        runtime_path = runtime.get("_diagnostic_source_path")
        decision_ts = _decision_bar_timestamp(runtime.get("generated_at"))
        runtime_handoff_attempted = bool(
            runtime.get("paper_runner_report_path") or runtime.get("paper_proof_invoked") or runtime.get("submit_attempted")
        )
        chosen_strategy_id = str(runtime.get("chosen_strategy_id") or "")
        suppressed_ids = {
            str(item.get("strategy_id") or item.get("signal_source") or "")
            for item in runtime.get("suppressed_signals") or []
            if isinstance(item, Mapping)
        }
        candidate_ids = {
            str(item.get("strategy_id") or item.get("signal_source") or "")
            for item in runtime.get("candidate_signals") or []
            if isinstance(item, Mapping)
        }
        rows_for_bar: list[dict[str, Any]] = []
        for strategy in runtime.get("evaluated_strategies") or []:
            if not isinstance(strategy, Mapping):
                continue
            instrument = _strategy_instrument(strategy)
            if instrument not in instrument_set:
                continue
            strategy_id = str(strategy.get("strategy_id") or strategy.get("signal_source") or "UNKNOWN")
            result = _strategy_result(strategy, suppressed_ids=suppressed_ids, candidate_ids=candidate_ids)
            gate_class = _gate_class_for_result(result)
            blockers_by_gate[gate_class] += 1
            failed = _failed_predicates(strategy)
            passed = _passed_predicates(strategy)
            missing_fields = _missing_fields(strategy)
            for predicate in failed:
                top_failed_predicates[predicate] += 1
                failed_by_strategy[strategy_id][predicate] += 1
            near = _near_miss_classification(strategy)
            near_miss_counts[near] += 1
            if near in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
                strategy_near_counts[strategy_id] += 1
            strategy_eval_counts[strategy_id] += 1
            if result == "SIGNAL":
                strategy_signal_counts[strategy_id] += 1
            elif result == "SUPPRESSED":
                strategy_suppressed_counts[strategy_id] += 1
            elif result == "NO_SIGNAL":
                strategy_no_signal_counts[strategy_id] += 1
            if runtime_handoff_attempted and strategy_id == chosen_strategy_id:
                strategy_handoff_counts[strategy_id] += 1
            row = {
                "decision_bar_timestamp": decision_ts,
                "decision_bar_timestamp_source": "INFERRED_FROM_RUNTIME_GENERATED_AT",
                "instrument": instrument,
                "strategy_id": strategy_id,
                "result": result,
                "gate_class": gate_class,
                "decision_reason": strategy.get("decision_reason") or strategy.get("primary_blocker"),
                "failed_predicates": failed,
                "passed_predicates": passed,
                "passed_predicates_count": len(passed) if passed or failed else None,
                "failed_predicates_count": len(failed) if passed or failed else None,
                "missing_fields": missing_fields,
                "near_miss_classification": near,
                "nearest_failed_predicate": failed[0] if failed else None,
                "candidate_reason": _candidate_reason(runtime, strategy_id),
                "suppression_reason": _suppression_reason(runtime, strategy_id),
                "runtime_cycle_report_path": runtime_path,
                "rule_runner_report_path": strategy.get("report_json_path"),
            }
            rows_for_bar.append(row)
            strategy_rows.append(row)
        if rows_for_bar:
            instrument_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows_for_bar:
                instrument_groups[str(row["instrument"])].append(row)
            for instrument, grouped in sorted(instrument_groups.items()):
                ohlc = ohlc_by_instrument.get(instrument, {}).get(decision_ts)
                monitor_state = live_evaluated_bars.get((instrument, decision_ts), {})
                enabled = enabled_by_instrument.get(instrument, [])
                nearest = _nearest_strategy_candidate(grouped)
                natural_long_trend = _natural_long_trend_family_needed(ohlc, enabled)
                if natural_long_trend:
                    blockers_by_gate["COVERAGE_GAP"] += 1
                bar_rows.append(
                    {
                        "decision_bar_timestamp": decision_ts,
                        "instrument": instrument,
                        "bar_ohlc": ohlc,
                        "bar_ohlc_source": "DATABENTO_LIVE_ARTIFACT_COMPLETED_5M" if ohlc else "UNAVAILABLE_IN_RETAINED_ARTIFACTS",
                        "live_monitor_eligible_known": bool(monitor_state),
                        "feature_context_ready": monitor_state.get("feature_context_ready"),
                        "live_execution_approved": monitor_state.get("live_execution_approved"),
                        "replay_execution_context_available": True,
                        "paper_evaluation_allowed_equivalent": monitor_state.get("paper_evaluation_allowed", True),
                        "strategies_evaluated": [item["strategy_id"] for item in grouped],
                        "strategy_results": grouped,
                        "signal_count": sum(1 for item in grouped if item["result"] == "SIGNAL"),
                        "no_signal_count": sum(1 for item in grouped if item["result"] == "NO_SIGNAL"),
                        "not_ready_count": sum(1 for item in grouped if item["result"] == "NOT_READY"),
                        "suppressed_count": sum(1 for item in grouped if item["result"] == "SUPPRESSED"),
                        "nearest_strategy_candidate": nearest,
                        "natural_long_trend_strategy_family_needed": natural_long_trend,
                        "dominant_gate_class": _dominant_gate_class(grouped, natural_long_trend),
                    }
                )

    instrument_summaries = _instrument_summaries(
        instrument_set=instrument_set,
        bar_rows=bar_rows,
        enabled_by_instrument=enabled_by_instrument,
        ohlc_by_instrument=ohlc_by_instrument,
    )
    total_signals = sum(strategy_signal_counts.values())
    total_suppressed = sum(strategy_suppressed_counts.values())
    total_handoffs = sum(strategy_handoff_counts.values())
    eligible_bars = sum(1 for row in bar_rows if row.get("paper_evaluation_allowed_equivalent") is not False)
    trend_gap = any(item.get("trend_continuation_gap") for item in instrument_summaries.values())
    classification = _classify(
        bar_rows=bar_rows,
        eligible_bars=eligible_bars,
        total_signals=total_signals,
        total_suppressed=total_suppressed,
        total_handoffs=total_handoffs,
        near_miss_counts=near_miss_counts,
        trend_gap=trend_gap,
    )
    report_json = output_root / "latest_track_b_missed_move_forensic_replay.json"
    window_times = [item["decision_bar_timestamp"] for item in bar_rows if item.get("decision_bar_timestamp")]
    report = {
        "schema_version": "track_b_missed_move_forensic_replay_v1",
        "generated_at": actual_now.isoformat(),
        "source_tag": "DIAGNOSTIC_REPLAY_ONLY",
        "execution_live_source": False,
        "historical_or_artifact_replay_is_not_execution_live": True,
        "window_start": min(window_times) if window_times else None,
        "window_end": max(window_times) if window_times else None,
        "requested_scope": {
            "instruments": sorted(instrument_set),
            "session_scope": "today retained Track B runtime-cycle reports and available completed 5m artifacts",
            "timeframe": "5m",
            "max_runtime_reports": max_runtime_reports,
        },
        "evidence_limitations": _evidence_limitations(runtime_reports, ohlc_by_instrument),
        "instruments_analyzed": sorted(instrument_set),
        "completed_5m_bars_analyzed": len(bar_rows),
        "eligible_bars": eligible_bars,
        "bars_actually_evaluated_live_if_known": sum(1 for item in bar_rows if item.get("live_monitor_eligible_known")),
        "replay_evaluable_bars": len(bar_rows),
        "total_strategy_evaluations": sum(strategy_eval_counts.values()),
        "total_signals": total_signals,
        "total_no_signals": sum(strategy_no_signal_counts.values()),
        "total_suppressed": total_suppressed,
        "total_handoffs": total_handoffs,
        "top_failed_predicates": _counter_rows(top_failed_predicates, limit=25),
        "top_failed_predicates_by_strategy": {
            strategy_id: _counter_rows(counter, limit=12)
            for strategy_id, counter in sorted(failed_by_strategy.items())
        },
        "top_blockers_by_gate_class": _counter_rows(blockers_by_gate, limit=10),
        "closest_near_misses": _closest_near_misses(strategy_rows),
        "one_predicate_away_count": int(near_miss_counts["ONE_PREDICATE_AWAY"]),
        "two_predicate_away_count": int(near_miss_counts["TWO_PREDICATES_AWAY"]),
        "strategies_that_never_came_close": [
            strategy_id
            for strategy_id in sorted(strategy_eval_counts)
            if strategy_near_counts[strategy_id] == 0
        ],
        "strategies_that_repeatedly_came_close": [
            {"strategy_id": strategy_id, "near_miss_count": int(count)}
            for strategy_id, count in sorted(strategy_near_counts.items(), key=lambda item: (-item[1], item[0]))
            if count > 1
        ],
        "directional_coverage_assessment": {
            instrument: _directional_coverage(enabled_by_instrument.get(instrument, []), ohlc_by_instrument.get(instrument, {}))
            for instrument in sorted(instrument_set)
        },
        "trend_continuation_gap_assessment": _trend_gap_assessment(instrument_summaries),
        "instrument_summaries": instrument_summaries,
        "completed_decision_bars": bar_rows,
        "diagnosis_classification": classification,
        "source_artifact_paths": {
            "runtime_cycle_root": str(runtime_root),
            "monitor_root": str(monitor_root),
            "databento_live_feed_root": str(feed_root),
            "startup_readiness_diagnostic": str(readiness_path),
            "output_json": str(report_json),
        },
        "startup_readiness_snapshot": {
            instrument: (readiness.get("instruments") or {}).get(instrument, {})
            for instrument in sorted(instrument_set)
            if isinstance(readiness.get("instruments"), Mapping)
        },
        "safety": {
            "broker_commands_invoked": False,
            "paper_proof_cli_invoked": False,
            "manual_submit_cancel_place_order_invoked": False,
            "broker_state_mutated_by_diagnostic": False,
            "live_money_readiness": False,
        },
    }
    if write:
        _write_json(report_json, report)
    return TrackBMissedMoveForensicReplayResult(report_json=report_json, report=report)


def _load_today_runtime_reports(
    *,
    runtime_root: Path,
    instrument_set: set[str],
    now: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    paths = sorted(
        runtime_root.glob("*/track_b_multi_strategy_runtime_cycle_report.json"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
    )[-max(0, int(limit)) :]
    today = now.astimezone(UTC).date()
    loaded: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        payload = _load_json(path)
        if not payload:
            continue
        generated_at = _parse_datetime(payload.get("generated_at"))
        if generated_at is None or generated_at.astimezone(UTC).date() != today:
            continue
        if not _runtime_has_instrument(payload, instrument_set):
            continue
        key = str(payload.get("track_b_multi_strategy_runtime_cycle_id") or path)
        if key in seen:
            continue
        seen.add(key)
        payload["_diagnostic_source_path"] = str(path)
        loaded.append(payload)
    return sorted(loaded, key=lambda item: str(item.get("generated_at") or ""))


def _load_today_monitor_reports(*, monitor_root: Path, now: datetime, limit: int) -> list[dict[str, Any]]:
    paths = sorted(
        monitor_root.glob("track_b_shadow_monitor_*/track_b_shadow_monitor_report.json"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
    )[-max(0, int(limit)) :]
    latest = monitor_root / "latest_track_b_shadow_monitor_report.json"
    if latest.exists() and latest not in paths:
        paths.append(latest)
    today = now.astimezone(UTC).date()
    loaded: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        payload = _load_json(path)
        completed_at = _parse_datetime(payload.get("completed_at"))
        if completed_at is None or completed_at.astimezone(UTC).date() != today:
            continue
        key = str(payload.get("cycle_id") or path)
        if key in seen:
            continue
        seen.add(key)
        payload["_diagnostic_source_path"] = str(path)
        loaded.append(payload)
    return sorted(loaded, key=lambda item: str(item.get("completed_at") or ""))


def _load_completed_5m_bars(feed_root: Path, instrument: str) -> dict[str, dict[str, Any]]:
    path = feed_root / f"latest_live_{instrument.lower()}_completed_5m_candles.json"
    payload = _load_json(path)
    bars: dict[str, dict[str, Any]] = {}
    for candle in payload.get("candles") or []:
        if not isinstance(candle, Mapping):
            continue
        ts = _first_text(candle.get("candle_timestamp"), candle.get("timestamp"))
        if not ts:
            continue
        bars[_normalize_iso_minute(ts)] = {
            "open": candle.get("open"),
            "high": candle.get("high"),
            "low": candle.get("low"),
            "close": candle.get("close"),
            "volume": candle.get("volume"),
        }
    return bars


def _live_evaluated_bars(monitor_reports: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for report in monitor_reports:
        for instrument in report.get("instrument_reports") or []:
            if not isinstance(instrument, Mapping):
                continue
            family = str(instrument.get("instrument_family") or "").upper()
            ts = _first_text(
                instrument.get("latest_completed_5m_timestamp"),
                instrument.get("latest_completed_5m_candle_timestamp"),
                instrument.get("latest_decision_bar_timestamp"),
            )
            if not family or not ts:
                continue
            result[(family, _normalize_iso_minute(ts))] = {
                "feature_context_ready": instrument.get("feature_context_ready"),
                "live_execution_approved": instrument.get("live_execution_approved"),
                "paper_evaluation_allowed": instrument.get("paper_evaluation_allowed"),
                "monitor_report_path": report.get("_diagnostic_source_path"),
                "monitor_cycle_index": report.get("cycle_index"),
            }
    return result


def _enabled_by_instrument(
    monitor_reports: Sequence[Mapping[str, Any]],
    runtime_reports: Sequence[Mapping[str, Any]],
    instrument_set: set[str],
) -> dict[str, list[str]]:
    enabled: dict[str, set[str]] = {instrument: set() for instrument in instrument_set}
    for report in monitor_reports:
        for instrument in report.get("instrument_reports") or []:
            if not isinstance(instrument, Mapping):
                continue
            family = str(instrument.get("instrument_family") or "").upper()
            if family in enabled:
                for strategy in instrument.get("enabled_strategies") or []:
                    enabled[family].add(str(strategy))
    for runtime in runtime_reports:
        for strategy in runtime.get("evaluated_strategies") or []:
            if not isinstance(strategy, Mapping):
                continue
            family = _strategy_instrument(strategy)
            if family in enabled:
                enabled[family].add(str(strategy.get("strategy_id") or strategy.get("signal_source") or "UNKNOWN"))
    return {key: sorted(value) for key, value in sorted(enabled.items())}


def _instrument_summaries(
    *,
    instrument_set: set[str],
    bar_rows: Sequence[Mapping[str, Any]],
    enabled_by_instrument: Mapping[str, list[str]],
    ohlc_by_instrument: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for instrument in sorted(instrument_set):
        rows = [row for row in bar_rows if row.get("instrument") == instrument]
        coverage = _directional_coverage(enabled_by_instrument.get(instrument, []), ohlc_by_instrument.get(instrument, {}))
        summaries[instrument] = {
            "completed_5m_bars_analyzed": len(rows),
            "enabled_strategies": enabled_by_instrument.get(instrument, []),
            "enabled_strategy_count": len(enabled_by_instrument.get(instrument, [])),
            "strategy_evaluations": sum(len(row.get("strategy_results") or []) for row in rows),
            "signal_count": sum(int(row.get("signal_count") or 0) for row in rows),
            "no_signal_count": sum(int(row.get("no_signal_count") or 0) for row in rows),
            "suppressed_count": sum(int(row.get("suppressed_count") or 0) for row in rows),
            "trend_continuation_gap": coverage.get("trend_continuation_gap"),
            "directional_coverage": coverage,
        }
    return summaries


def _strategy_instrument(strategy: Mapping[str, Any]) -> str:
    metadata = strategy.get("registry_metadata") if isinstance(strategy.get("registry_metadata"), Mapping) else {}
    return str(
        metadata.get("strategy_registry_instrument_family")
        or strategy.get("instrument_family")
        or _infer_instrument_from_strategy_id(str(strategy.get("strategy_id") or strategy.get("signal_source") or ""))
    ).upper()


def _infer_instrument_from_strategy_id(strategy_id: str) -> str:
    if strategy_id.upper().startswith("MNQ_"):
        return "MNQ"
    if strategy_id.upper().startswith("MGC_"):
        return "MGC"
    return "MGC"


def _runtime_has_instrument(runtime: Mapping[str, Any], instrument_set: set[str]) -> bool:
    return any(
        isinstance(strategy, Mapping) and _strategy_instrument(strategy) in instrument_set
        for strategy in runtime.get("evaluated_strategies") or []
    )


def _strategy_result(
    strategy: Mapping[str, Any],
    *,
    suppressed_ids: set[str],
    candidate_ids: set[str],
) -> str:
    strategy_id = str(strategy.get("strategy_id") or strategy.get("signal_source") or "")
    if strategy_id in suppressed_ids:
        return "SUPPRESSED"
    if strategy_id in candidate_ids or strategy.get("signal_emitted") is True or strategy.get("decision") == "SIGNAL":
        return "SIGNAL"
    verdict_text = " ".join(
        str(strategy.get(key) or "")
        for key in ("decision", "strategy_runtime_verdict", "strategy_rule_runner_verdict", "primary_blocker")
    ).upper()
    if "NOT_READY" in verdict_text:
        return "NOT_READY"
    if "ERROR" in verdict_text:
        return "ERROR"
    return "NO_SIGNAL"


def _gate_class_for_result(result: str) -> str:
    if result in {"NOT_READY", "ERROR"}:
        return "OPERATIONAL_GATE"
    if result == "SUPPRESSED":
        return "ARBITRATION_GATE"
    if result == "SIGNAL":
        return "NONE"
    return "STRATEGY_PREDICATE_GATE"


def _failed_predicates(strategy: Mapping[str, Any]) -> list[str]:
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    if conditions:
        return [str(key) for key, value in conditions.items() if value is False]
    blockers = strategy.get("rule_blockers") if isinstance(strategy.get("rule_blockers"), list) else []
    return [str(item).replace("=false_or_missing", "") for item in blockers]


def _passed_predicates(strategy: Mapping[str, Any]) -> list[str]:
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    return [str(key) for key, value in conditions.items() if value is True]


def _missing_fields(strategy: Mapping[str, Any]) -> list[str]:
    fields: list[str] = []
    for key in ("missing_fields", "required_missing_fields"):
        raw = strategy.get(key)
        if isinstance(raw, list):
            fields.extend(str(item) for item in raw)
    for blocker in strategy.get("rule_blockers") or []:
        text = str(blocker)
        if "missing" in text.lower():
            fields.append(text)
    return sorted(set(fields))


def _near_miss_classification(strategy: Mapping[str, Any]) -> str:
    failed_count = len(_failed_predicates(strategy))
    passed_count = len(_passed_predicates(strategy))
    if failed_count == 0 and passed_count == 0:
        return "NOT_SCORABLE"
    if failed_count == 1:
        return "ONE_PREDICATE_AWAY"
    if failed_count == 2:
        return "TWO_PREDICATES_AWAY"
    return "MULTI_PREDICATE_FAIL"


def _candidate_reason(runtime: Mapping[str, Any], strategy_id: str) -> str | None:
    for candidate in runtime.get("candidate_signals") or []:
        if isinstance(candidate, Mapping) and str(candidate.get("strategy_id") or candidate.get("signal_source") or "") == strategy_id:
            return _first_text(candidate.get("reason"), candidate.get("signal_reason"), candidate.get("primary_blocker"))
    return None


def _suppression_reason(runtime: Mapping[str, Any], strategy_id: str) -> str | None:
    for suppressed in runtime.get("suppressed_signals") or []:
        if isinstance(suppressed, Mapping) and str(suppressed.get("strategy_id") or suppressed.get("signal_source") or "") == strategy_id:
            return _first_text(suppressed.get("reason"), suppressed.get("primary_blocker"))
    return None


def _nearest_strategy_candidate(grouped: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    scored = [
        (
            int(item.get("failed_predicates_count") or 9999),
            str(item.get("strategy_id") or ""),
            item,
        )
        for item in grouped
        if item.get("failed_predicates_count") is not None
    ]
    if not scored:
        return None
    failed_count, _, item = sorted(scored, key=lambda value: (value[0], value[1]))[0]
    return {
        "strategy_id": item.get("strategy_id"),
        "near_miss_classification": item.get("near_miss_classification"),
        "failed_predicates_count": failed_count,
        "nearest_failed_predicate": item.get("nearest_failed_predicate"),
    }


def _dominant_gate_class(grouped: Sequence[Mapping[str, Any]], natural_long_trend: bool) -> str:
    if natural_long_trend:
        return "COVERAGE_GAP"
    counts = Counter(str(item.get("gate_class") or "DIAGNOSTIC_INCONCLUSIVE") for item in grouped)
    return counts.most_common(1)[0][0] if counts else "DIAGNOSTIC_INCONCLUSIVE"


def _natural_long_trend_family_needed(ohlc: Mapping[str, Any] | None, enabled: Sequence[str]) -> bool:
    if not ohlc:
        return False
    open_value = _decimal_or_none(ohlc.get("open"))
    close_value = _decimal_or_none(ohlc.get("close"))
    if open_value is None or close_value is None or close_value <= open_value:
        return False
    return not any(_is_trend_continuation_strategy(strategy_id) and _strategy_side(strategy_id) == "LONG" for strategy_id in enabled)


def _directional_coverage(enabled: Sequence[str], ohlc: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    long_strategies = [item for item in enabled if _strategy_side(item) == "LONG"]
    short_strategies = [item for item in enabled if _strategy_side(item) == "SHORT"]
    trend_long = [item for item in long_strategies if _is_trend_continuation_strategy(item)]
    trend_short = [item for item in short_strategies if _is_trend_continuation_strategy(item)]
    net_change = _net_change(ohlc)
    rally_observed = net_change is not None and net_change > 0
    return {
        "enabled_long_strategies": long_strategies,
        "enabled_short_strategies": short_strategies,
        "long_strategy_count": len(long_strategies),
        "short_strategy_count": len(short_strategies),
        "long_trend_continuation_strategy_count": len(trend_long),
        "short_trend_continuation_strategy_count": len(trend_short),
        "retained_window_net_change": None if net_change is None else str(net_change),
        "rally_observed_in_retained_window": rally_observed,
        "trend_continuation_gap": bool(rally_observed and not trend_long),
        "assessment": (
            "No enabled pure long trend-continuation strategy family is present in retained rally evidence."
            if rally_observed and not trend_long
            else "Directional coverage includes at least one matching trend-continuation family or retained bars did not rally."
        ),
    }


def _strategy_side(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "BULL" in upper or "LONG" in upper:
        return "LONG"
    if "BEAR" in upper or "SHORT" in upper:
        return "SHORT"
    return "UNKNOWN"


def _is_trend_continuation_strategy(strategy_id: str) -> bool:
    upper = strategy_id.upper()
    return any(token in upper for token in ("TREND", "CONTINUATION", "FOLLOW_THROUGH", "MOMENTUM"))


def _net_change(ohlc: Mapping[str, Mapping[str, Any]]) -> Decimal | None:
    if not ohlc:
        return None
    rows = sorted(ohlc.items())
    first_open = _decimal_or_none(rows[0][1].get("open"))
    last_close = _decimal_or_none(rows[-1][1].get("close"))
    if first_open is None or last_close is None:
        return None
    return last_close - first_open


def _trend_gap_assessment(instrument_summaries: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    gaps = [
        instrument
        for instrument, summary in instrument_summaries.items()
        if summary.get("trend_continuation_gap") is True
    ]
    return {
        "trend_continuation_gap_detected": bool(gaps),
        "instruments_with_gap": gaps,
        "assessment": (
            "Retained rally bars show a likely coverage gap: enabled strategies are snap/pause/retest/bear-turn biased rather than pure long trend-continuation."
            if gaps
            else "No retained evidence of a long trend-continuation coverage gap."
        ),
    }


def _classify(
    *,
    bar_rows: Sequence[Mapping[str, Any]],
    eligible_bars: int,
    total_signals: int,
    total_suppressed: int,
    total_handoffs: int,
    near_miss_counts: Counter[str],
    trend_gap: bool,
) -> str:
    if not bar_rows or eligible_bars == 0:
        return "ENGINE_WAS_NOT_ELIGIBLE"
    if total_signals or total_suppressed or total_handoffs:
        return "ARBITRATION_OR_HANDOFF_BLOCKED"
    if near_miss_counts["ONE_PREDICATE_AWAY"] or near_miss_counts["TWO_PREDICATES_AWAY"]:
        return "STRATEGY_GATES_MUTED_NEAR_MISSES"
    if trend_gap:
        return "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION"
    return "ENGINE_ELIGIBLE_STRATEGIES_NO_SIGNAL"


def _closest_near_misses(strategy_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    priority = {"ONE_PREDICATE_AWAY": 0, "TWO_PREDICATES_AWAY": 1, "MULTI_PREDICATE_FAIL": 2, "NOT_SCORABLE": 3}
    rows = sorted(
        strategy_rows,
        key=lambda item: (
            priority.get(str(item.get("near_miss_classification")), 9),
            int(item.get("failed_predicates_count") or 9999),
            str(item.get("decision_bar_timestamp") or ""),
            str(item.get("strategy_id") or ""),
        ),
    )
    result = []
    for item in rows[:10]:
        result.append(
            {
                "decision_bar_timestamp": item.get("decision_bar_timestamp"),
                "instrument": item.get("instrument"),
                "strategy_id": item.get("strategy_id"),
                "near_miss_classification": item.get("near_miss_classification"),
                "failed_predicates_count": item.get("failed_predicates_count"),
                "nearest_failed_predicate": item.get("nearest_failed_predicate"),
            }
        )
    return result


def _evidence_limitations(
    runtime_reports: Sequence[Mapping[str, Any]],
    ohlc_by_instrument: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    limitations = [
        "Diagnostic is read-only and does not invoke broker, paper_proof_cli, or PAPER lifecycle.",
        "Historical/backfill or retained artifact replay is diagnostic-only and is not execution-live.",
    ]
    if not runtime_reports:
        limitations.append("No same-day retained runtime-cycle reports were available for MGC/MNQ.")
    for instrument, bars in ohlc_by_instrument.items():
        if not bars:
            limitations.append(f"{instrument} retained completed 5m OHLC artifact was unavailable.")
        elif len(bars) < 60:
            limitations.append(
                f"{instrument} completed 5m OHLC is limited to {len(bars)} retained bars; full-day OHLC was not present in hot artifacts."
            )
    return limitations


def _decision_bar_timestamp(value: object) -> str | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    parsed = parsed.astimezone(UTC).replace(second=0, microsecond=0)
    minute = parsed.minute - (parsed.minute % 5)
    return parsed.replace(minute=minute).isoformat()


def _normalize_iso_minute(value: str) -> str:
    parsed = _parse_datetime(value)
    if parsed is None:
        return value
    return parsed.astimezone(UTC).replace(second=0, microsecond=0).isoformat()


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _counter_rows(counter: Counter[str], *, limit: int = 10) -> list[dict[str, Any]]:
    return [
        {"reason": reason, "count": int(count)}
        for reason, count in counter.most_common(limit)
    ]


def _first_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists() or path.stat().st_size > MAX_REPORT_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
