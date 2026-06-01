"""Read-only Track B PAPER participation observatory.

This module explains the decision funnel for active PAPER lanes from already
written runtime/status artifacts. It never connects to a broker, never restarts
runtime, and never changes strategy or order state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_participation_observatory_v1"
DEFAULT_OPERATOR_STATUS_PATH = Path("outputs/probationary_pattern_engine/paper_session/operator_status.json")
DEFAULT_MARKET_DATA_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data")
DEFAULT_OUTPUT_JSON = Path("outputs/track_b_execution_core/diagnostics/latest_track_b_participation_observatory.json")
DEFAULT_OUTPUT_MD = Path("outputs/track_b_execution_core/diagnostics/latest_track_b_participation_observatory.md")
DEFAULT_EVENT_JSONL = Path("outputs/track_b_execution_core/diagnostics/track_b_participation_observatory_events.jsonl")

FUNNEL_STAGES = (
    "BAR_AVAILABLE",
    "SESSION_ELIGIBLE",
    "DATA_READY",
    "STRATEGY_CONTEXT_READY",
    "PREDICATES_EVALUATED",
    "CANDIDATE_CREATED",
    "INTENT_CREATED",
    "ENTRY_EXPOSURE_AUTHORIZED",
    "GOVERNANCE_AUTHORIZED",
    "ROUTE_AUTHORIZED",
    "SUBMIT_ATTEMPTED",
    "BROKER_ACK",
    "FILL",
    "LIFECYCLE_ADOPTED",
)


@dataclass(frozen=True)
class TrackBParticipationObservatoryConfig:
    repo_root: Path = REPO_ROOT
    operator_status_path: Path = DEFAULT_OPERATOR_STATUS_PATH
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    output_json_path: Path = DEFAULT_OUTPUT_JSON
    output_md_path: Path = DEFAULT_OUTPUT_MD
    event_jsonl_path: Path | None = DEFAULT_EVENT_JSONL
    max_bars_per_lane: int = 120
    write_event_stream: bool = False
    max_event_stream_rows: int = 5000

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_participation_observatory(
    *,
    config: TrackBParticipationObservatoryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    operator_status = _read_json(config.resolve(config.operator_status_path))
    lanes = [lane for lane in _list(operator_status.get("lanes")) if _is_active_paper_lane(lane)]
    bar_rows: list[dict[str, Any]] = []
    for lane in lanes:
        bars = _relevant_bars(config=config, lane=lane)
        if not bars:
            bars = [_synthetic_lane_bar(lane)]
        for bar in bars[-max(int(config.max_bars_per_lane), 1) :]:
            bar_rows.append(_lane_bar_funnel(lane=lane, bar=bar, now=actual_now))

    lane_reports = _lane_reports(lanes=lanes, bar_rows=bar_rows, now=actual_now)
    first_fail_leaderboard = _first_fail_leaderboard(bar_rows)
    hidden_blockers = _hidden_blockers(lanes=lanes, bar_rows=bar_rows)
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "production_gate_wiring_allowed": False,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "funnel_stages": list(FUNNEL_STAGES),
        "source_paths": {
            "operator_status": str(config.resolve(config.operator_status_path)),
            "market_data_root": str(config.resolve(config.market_data_root)),
        },
        "active_lane_count": len(lanes),
        "bar_evaluation_count": len(bar_rows),
        "lane_reports": lane_reports,
        "bar_evaluations": bar_rows,
        "first_fail_reason_leaderboard": first_fail_leaderboard,
        "session_coverage": _session_coverage(lane_reports),
        "conversion": _conversion_summary(lane_reports),
        "hidden_blockers": hidden_blockers,
        "noise_silence": _noise_silence(lane_reports),
        "classification": _classification(hidden_blockers=hidden_blockers, lane_reports=lane_reports),
    }
    return report


def write_track_b_participation_observatory(
    *,
    config: TrackBParticipationObservatoryConfig,
    report: Mapping[str, Any],
) -> tuple[Path, Path]:
    json_path = config.resolve(config.output_json_path)
    md_path = config.resolve(config.output_md_path)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(json_path, dict(report))
    md_path.write_text(render_participation_observatory_markdown(report), encoding="utf-8")
    if config.write_event_stream and config.event_jsonl_path is not None:
        event_path = config.resolve(config.event_jsonl_path)
        _write_bounded_event_stream(event_path, _list(report.get("bar_evaluations")), max_rows=config.max_event_stream_rows)
    return json_path, md_path


def render_participation_observatory_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Participation Observatory",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- active lanes: `{report.get('active_lane_count')}`",
        f"- bar evaluations: `{report.get('bar_evaluation_count')}`",
        "",
        "## First-Fail Leaderboard",
    ]
    leaderboard = list(report.get("first_fail_reason_leaderboard") or [])
    if not leaderboard:
        lines.append("- none")
    for item in leaderboard[:10]:
        lines.append(f"- `{item.get('reason')}`: {item.get('count')}")
    lines.extend(["", "## Lane Funnel Counts"])
    for lane in report.get("lane_reports") or []:
        counts = lane.get("funnel_counts") or {}
        lines.append(
            f"- `{lane.get('lane_id')}` {lane.get('symbol')} {lane.get('session_window')}: "
            f"bars={lane.get('bars_evaluated')} candidates={counts.get('CANDIDATE_CREATED', 0)} "
            f"intents={counts.get('INTENT_CREATED', 0)} submits={counts.get('SUBMIT_ATTEMPTED', 0)} "
            f"fills={counts.get('FILL', 0)} first_fail=`{lane.get('latest_first_fail_reason')}`"
        )
    lines.extend(["", "## Hidden Blockers"])
    blockers = list(report.get("hidden_blockers") or [])
    if not blockers:
        lines.append("- none")
    for blocker in blockers:
        lines.append(f"- `{blocker.get('code')}` lane=`{blocker.get('lane_id')}` detail={blocker.get('detail')}")
    return "\n".join(lines) + "\n"


def _lane_bar_funnel(*, lane: Mapping[str, Any], bar: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    lane_id = str(lane.get("lane_id") or "")
    strategy_id = str(
        lane.get("strategy_identity_root")
        or _mapping(lane.get("runtime_overlay_params")).get("strategy_id")
        or lane.get("tracked_strategy_id")
        or lane_id
    )
    symbol = str(lane.get("symbol") or bar.get("symbol") or "").upper()
    bar_ts = _bar_timestamp(bar) or _parse_time(lane.get("latest_completed_bar_end_ts")) or now
    in_window = _lane_in_window(lane=lane, bar_ts=bar_ts)
    data_ready = not bool(lane.get("market_data_not_ready")) and _bar_timestamp(bar) is not None
    execution_ts = _parse_time(lane.get("last_execution_bar_evaluated_at"))
    context_ready = execution_ts is not None and execution_ts >= bar_ts
    rule_report = _mapping(lane.get("latest_track_b_rule_runner_report"))
    predicate_results = _predicate_results(rule_report)
    predicates_evaluated = bool(rule_report) and _same_minute(_parse_time(rule_report.get("current_bar_end_et")), bar_ts)
    candidate_created = _candidate_created(lane=lane, rule_report=rule_report, predicates_evaluated=predicates_evaluated)
    live_intent = _mapping(lane.get("latest_live_strategy_intent"))
    blocked_intent = _mapping(lane.get("latest_blocked_strategy_intent"))
    intent = live_intent or blocked_intent
    intent_created = _intent_matches_bar(intent, bar_ts)
    gate_blocker = _gate_blocker(lane=lane, intent=intent)
    entry_exposure_authorized = intent_created and not _has_blocker(gate_blocker, "EXPOSURE")
    governance_authorized = intent_created and not _has_blocker(gate_blocker, "GOVERNANCE")
    route_authorized = intent_created and not _has_blocker(gate_blocker, "ROUTE")
    submit_attempted = bool(intent.get("submit_attempted")) or bool(intent.get("submitted_at"))
    broker_ack = bool(intent.get("broker_order_id") or intent.get("perm_id") or intent.get("acknowledged_at"))
    fill = _fill_reached(lane=lane, intent=intent)
    lifecycle_adopted = _lifecycle_adopted(lane=lane, intent=intent, fill=fill)
    stage_results = {
        "BAR_AVAILABLE": True,
        "SESSION_ELIGIBLE": in_window,
        "DATA_READY": data_ready,
        "STRATEGY_CONTEXT_READY": context_ready,
        "PREDICATES_EVALUATED": predicates_evaluated,
        "CANDIDATE_CREATED": candidate_created,
        "INTENT_CREATED": intent_created,
        "ENTRY_EXPOSURE_AUTHORIZED": entry_exposure_authorized,
        "GOVERNANCE_AUTHORIZED": governance_authorized,
        "ROUTE_AUTHORIZED": route_authorized,
        "SUBMIT_ATTEMPTED": submit_attempted,
        "BROKER_ACK": broker_ack,
        "FILL": fill,
        "LIFECYCLE_ADOPTED": lifecycle_adopted,
    }
    return {
        "timestamp": bar_ts.isoformat(),
        "symbol": symbol,
        "lane_id": lane_id,
        "strategy_id": strategy_id,
        "session_label": lane.get("current_detected_session") or _session_label(bar_ts),
        "session_window": _session_window(lane),
        "in_window": in_window,
        "data_freshness": _data_freshness(lane),
        "execution_context_timestamp": execution_ts.isoformat() if execution_ts else None,
        "predicate_results": predicate_results,
        "session_anchor_status": rule_report.get("session_anchor_status"),
        "session_anchor_reason_code": rule_report.get("session_anchor_reason_code"),
        "session_anchor_source": rule_report.get("session_anchor_source"),
        "session_anchor_source_artifact_path": rule_report.get("session_anchor_source_artifact_path"),
        "first_fail_reason": _first_fail_reason(
            lane=lane,
            stage_results=stage_results,
            gate_blocker=gate_blocker,
            predicates_evaluated=predicates_evaluated,
        ),
        "candidate_created": candidate_created,
        "intent_created": intent_created,
        "trade_id": intent.get("trade_id") or intent.get("strategy_trade_id"),
        "gate_blocker": gate_blocker,
        "submit_status": _submit_status(intent),
        "fill_status": "FILL_REACHED" if fill else "NO_FILL",
        "stage_results": stage_results,
    }


def _lane_reports(*, lanes: Sequence[Mapping[str, Any]], bar_rows: Sequence[Mapping[str, Any]], now: datetime) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for lane in lanes:
        lane_id = str(lane.get("lane_id") or "")
        rows = [row for row in bar_rows if row.get("lane_id") == lane_id]
        counts = {stage: sum(1 for row in rows if _mapping(row.get("stage_results")).get(stage) is True) for stage in FUNNEL_STAGES}
        first_fail_counts: dict[str, int] = {}
        for row in rows:
            reason = str(row.get("first_fail_reason") or "UNKNOWN")
            first_fail_counts[reason] = first_fail_counts.get(reason, 0) + 1
        reports.append(
            {
                "lane_id": lane_id,
                "symbol": lane.get("symbol"),
                "strategy_id": lane.get("strategy_identity_root") or _mapping(lane.get("runtime_overlay_params")).get("strategy_id") or lane_id,
                "session_window": _session_window(lane),
                "currently_in_window": bool(lane.get("allowed_session_match")),
                "bars_evaluated": len(rows),
                "latest_bar_timestamp": rows[-1]["timestamp"] if rows else None,
                "latest_processed_bar_timestamp": lane.get("last_processed_bar_end_ts"),
                "last_execution_bar_evaluated_at": lane.get("last_execution_bar_evaluated_at"),
                "candidate_count": counts.get("CANDIDATE_CREATED", 0),
                "intent_count": int(lane.get("intent_count") or counts.get("INTENT_CREATED", 0) or 0),
                "submit_attempt_count": counts.get("SUBMIT_ATTEMPTED", 0),
                "broker_ack_count": counts.get("BROKER_ACK", 0),
                "fill_count": int(lane.get("fill_count") or counts.get("FILL", 0) or 0),
                "funnel_counts": counts,
                "first_fail_counts": first_fail_counts,
                "latest_first_fail_reason": rows[-1].get("first_fail_reason") if rows else "NO_BARS",
                "latest_session_anchor_status": rows[-1].get("session_anchor_status") if rows else None,
                "latest_session_anchor_reason_code": rows[-1].get("session_anchor_reason_code") if rows else None,
                "noise_silence_classification": _lane_noise_silence(lane=lane, rows=rows, counts=counts),
            }
        )
    return reports


def _hidden_blockers(*, lanes: Sequence[Mapping[str, Any]], bar_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for lane in lanes:
        lane_id = str(lane.get("lane_id") or "")
        processed = _parse_time(lane.get("last_processed_bar_end_ts"))
        execution = _parse_time(lane.get("last_execution_bar_evaluated_at"))
        if processed and (execution is None or processed > execution):
            blockers.append(
                {
                    "code": "BARS_PROCESSED_EXECUTION_CONTEXT_NOT_ADVANCING",
                    "lane_id": lane_id,
                    "detail": f"processed={processed.isoformat()} execution={execution.isoformat() if execution else None}",
                }
            )
        rows = [row for row in bar_rows if row.get("lane_id") == lane_id]
        for row in rows:
            stages = _mapping(row.get("stage_results"))
            predicates_pass = stages.get("PREDICATES_EVALUATED") is True and _predicates_all_true(row.get("predicate_results"))
            if predicates_pass and not stages.get("CANDIDATE_CREATED"):
                blockers.append({"code": "PREDICATES_PASS_NO_CANDIDATE", "lane_id": lane_id, "detail": row.get("timestamp")})
            if stages.get("CANDIDATE_CREATED") and not stages.get("INTENT_CREATED"):
                blockers.append({"code": "CANDIDATE_NO_INTENT", "lane_id": lane_id, "detail": row.get("timestamp")})
            if stages.get("INTENT_CREATED") and not stages.get("SUBMIT_ATTEMPTED"):
                blockers.append({"code": "INTENT_NO_SUBMIT", "lane_id": lane_id, "detail": row.get("gate_blocker") or row.get("timestamp")})
            if stages.get("SUBMIT_ATTEMPTED") and not (stages.get("BROKER_ACK") or stages.get("FILL")):
                blockers.append({"code": "SUBMIT_NO_BROKER_ACK_OR_FILL", "lane_id": lane_id, "detail": row.get("timestamp")})
    return blockers


def _relevant_bars(*, config: TrackBParticipationObservatoryConfig, lane: Mapping[str, Any]) -> list[dict[str, Any]]:
    symbol = str(lane.get("symbol") or "").upper()
    timeframe = str(lane.get("execution_timeframe") or lane.get("primary_context_timeframe") or "1m")
    if not symbol:
        return []
    path = config.resolve(config.market_data_root) / symbol / timeframe / "latest_runtime_candles.json"
    payload = _read_json(path)
    bars = _list(payload.get("bars") or payload.get("candles") or payload.get("candle_history"))
    for bar in bars:
        bar.setdefault("symbol", symbol)
        bar.setdefault("source_artifact_path", str(path))
    return bars


def _synthetic_lane_bar(lane: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": lane.get("symbol"),
        "bar_end": lane.get("latest_completed_bar_end_ts") or lane.get("last_processed_bar_end_ts"),
        "source": "operator_status_no_runtime_bars",
    }


def _classification(*, hidden_blockers: Sequence[Mapping[str, Any]], lane_reports: Sequence[Mapping[str, Any]]) -> str:
    if hidden_blockers:
        return "PARTICIPATION_OBSERVATORY_REVIEW_REQUIRED"
    if any(report.get("noise_silence_classification") == "LANE_SILENT_DURING_ACTIVE_WINDOW" for report in lane_reports):
        return "PARTICIPATION_OBSERVATORY_SILENCE_DETECTED"
    return "PARTICIPATION_OBSERVATORY_OK"


def _session_coverage(lane_reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    windows: dict[str, int] = {}
    in_window = 0
    for report in lane_reports:
        window = str(report.get("session_window") or "UNKNOWN")
        windows[window] = windows.get(window, 0) + 1
        if report.get("currently_in_window") is True:
            in_window += 1
    return {"windows": windows, "in_window_lane_count": in_window, "out_of_window_lane_count": len(lane_reports) - in_window}


def _conversion_summary(lane_reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidates = sum(int(row.get("candidate_count") or 0) for row in lane_reports)
    intents = sum(int(row.get("intent_count") or 0) for row in lane_reports)
    submits = sum(int(row.get("submit_attempt_count") or 0) for row in lane_reports)
    fills = sum(int(row.get("fill_count") or 0) for row in lane_reports)
    return {
        "candidate_count": candidates,
        "intent_count": intents,
        "submit_attempt_count": submits,
        "fill_count": fills,
        "candidate_to_intent_rate": _ratio(intents, candidates),
        "intent_to_submit_rate": _ratio(submits, intents),
        "submit_to_fill_rate": _ratio(fills, submits),
    }


def _noise_silence(lane_reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "silent_active_lanes": [
            row.get("lane_id") for row in lane_reports if row.get("noise_silence_classification") == "LANE_SILENT_DURING_ACTIVE_WINDOW"
        ],
        "high_candidate_no_trade_lanes": [
            row.get("lane_id") for row in lane_reports if row.get("noise_silence_classification") == "HIGH_CANDIDATES_NO_SUBMIT"
        ],
    }


def _lane_noise_silence(*, lane: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], counts: Mapping[str, int]) -> str:
    if lane.get("allowed_session_match") is True and rows and counts.get("CANDIDATE_CREATED", 0) == 0:
        return "LANE_SILENT_DURING_ACTIVE_WINDOW"
    if counts.get("CANDIDATE_CREATED", 0) >= 10 and counts.get("SUBMIT_ATTEMPTED", 0) == 0:
        return "HIGH_CANDIDATES_NO_SUBMIT"
    if lane.get("allowed_session_match") is not True:
        return "OUT_OF_WINDOW"
    return "NORMAL_ACTIVITY"


def _first_fail_leaderboard(bar_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in bar_rows:
        reason = str(row.get("first_fail_reason") or "UNKNOWN")
        counts[reason] = counts.get(reason, 0) + 1
    return [{"reason": key, "count": value} for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def _first_fail_reason(
    *,
    lane: Mapping[str, Any],
    stage_results: Mapping[str, bool],
    gate_blocker: str | None,
    predicates_evaluated: bool,
) -> str:
    if stage_results.get("SESSION_ELIGIBLE") is not True:
        return str(lane.get("eligibility_reason") or "OUT_OF_WINDOW")
    if stage_results.get("DATA_READY") is not True:
        return str(lane.get("market_data_blocker_reason") or "DATA_NOT_READY")
    if stage_results.get("STRATEGY_CONTEXT_READY") is not True:
        return "STRATEGY_CONTEXT_NOT_ADVANCING"
    if not predicates_evaluated:
        return "PREDICATES_NOT_EVALUATED"
    if stage_results.get("CANDIDATE_CREATED") is not True:
        return str(lane.get("latest_track_b_rule_runner_primary_blocker") or "PREDICATES_FAILED")
    if stage_results.get("INTENT_CREATED") is not True:
        return str(lane.get("latest_blocked_strategy_intent_reason") or "CANDIDATE_NO_INTENT")
    if gate_blocker:
        return gate_blocker
    for stage in FUNNEL_STAGES:
        if stage_results.get(stage) is not True:
            return f"{stage}_NOT_REACHED"
    return "FUNNEL_COMPLETE"


def _candidate_created(*, lane: Mapping[str, Any], rule_report: Mapping[str, Any], predicates_evaluated: bool) -> bool:
    if not predicates_evaluated:
        return False
    classification = str(lane.get("latest_track_b_rule_runner_classification") or rule_report.get("classification") or "").upper()
    if "NO_SIGNAL" in classification or "BLOCK" in classification:
        return False
    return bool(classification and classification != "UNKNOWN")


def _predicate_results(rule_report: Mapping[str, Any]) -> dict[str, Any]:
    explicit = rule_report.get("predicate_results")
    if isinstance(explicit, Mapping):
        return dict(explicit)
    keys = (
        "vwap_price",
        "session_open_price",
        "current_close",
        "primary_blocker",
        "classification",
        "condition",
        "session_anchor_status",
        "session_anchor_reason_code",
        "session_anchor_source",
        "session_anchor_source_artifact_path",
    )
    return {key: rule_report.get(key) for key in keys if key in rule_report}


def _predicates_all_true(value: object) -> bool:
    if not isinstance(value, Mapping) or not value:
        return False
    bool_values = [item for item in value.values() if isinstance(item, bool)]
    return bool(bool_values) and all(bool_values)


def _gate_blocker(*, lane: Mapping[str, Any], intent: Mapping[str, Any]) -> str | None:
    return _first_text(
        intent.get("submit_gate_blocker"),
        lane.get("latest_blocked_strategy_intent_reason"),
        lane.get("latest_blocked_strategy_intent_classification"),
    )


def _submit_status(intent: Mapping[str, Any]) -> str:
    if not intent:
        return "NO_INTENT"
    if intent.get("submit_attempted") is True:
        return "SUBMIT_ATTEMPTED"
    if intent.get("submit_suppressed") is True:
        return "SUBMIT_SUPPRESSED"
    return str(intent.get("order_status") or intent.get("broker_order_status") or "NOT_SUBMITTED")


def _fill_reached(*, lane: Mapping[str, Any], intent: Mapping[str, Any]) -> bool:
    if intent.get("exec_id") or intent.get("fill_exec_id") or intent.get("filled_at"):
        return True
    return int(lane.get("fill_count") or 0) > 0 and str(lane.get("position_side") or "").upper() != "FLAT"


def _lifecycle_adopted(*, lane: Mapping[str, Any], intent: Mapping[str, Any], fill: bool) -> bool:
    if intent.get("lifecycle_adopted") is True or intent.get("lifecycle_open_managed_event_present") is True:
        return True
    if not fill:
        return False
    classification = str(
        lane.get("startup_reconciliation_classification")
        or lane.get("registry_reconciliation")
        or lane.get("broker_lifecycle_reconciliation")
        or ""
    ).upper()
    return any(token in classification for token in ("READY", "CLEAN", "MATCHED", "RECONCILED"))


def _intent_matches_bar(intent: Mapping[str, Any], bar_ts: datetime) -> bool:
    if not intent:
        return False
    for key in ("bar_end_ts", "candidate_timestamp", "intent_timestamp", "created_at", "submitted_at", "filled_at"):
        parsed = _parse_time(intent.get(key))
        if parsed is not None and _same_minute(parsed, bar_ts):
            return True
    return False


def _lane_in_window(*, lane: Mapping[str, Any], bar_ts: datetime) -> bool:
    lane_id = str(lane.get("lane_id") or "").lower()
    local = bar_ts.astimezone(_ny_tz())
    minutes = local.hour * 60 + local.minute
    if "_us_" in lane_id:
        return 9 * 60 + 35 <= minutes <= 15 * 60 + 30
    if "globex" in lane_id:
        return minutes >= 18 * 60 + 5 or minutes <= 3 * 60
    return bool(lane.get("allowed_session_match"))


def _session_window(lane: Mapping[str, Any]) -> str:
    lane_id = str(lane.get("lane_id") or "").lower()
    if "_us_" in lane_id:
        return "US_09:35-15:30_ET"
    if "globex" in lane_id:
        return "GLOBEX_18:05-03:00_ET"
    return str(lane.get("session_restriction") or "UNKNOWN")


def _session_label(ts: datetime) -> str:
    local = ts.astimezone(_ny_tz())
    minutes = local.hour * 60 + local.minute
    if 9 * 60 + 35 <= minutes <= 15 * 60 + 30:
        return "US"
    if minutes >= 18 * 60 + 5 or minutes <= 3 * 60:
        return "GLOBEX"
    return "OUT_OF_WINDOW"


def _data_freshness(lane: Mapping[str, Any]) -> dict[str, Any]:
    recovery = _mapping(lane.get("market_data_recovery"))
    return {
        "market_data_not_ready": bool(lane.get("market_data_not_ready")),
        "market_data_blocker_reason": lane.get("market_data_blocker_reason"),
        "recovery_state": recovery.get("market_data_recovery_state"),
        "stale": recovery.get("stale"),
        "latest_observed_raw_bar_timestamp": recovery.get("latest_observed_raw_bar_timestamp"),
    }


def _is_active_paper_lane(lane: Mapping[str, Any]) -> bool:
    if lane.get("paper_only") is False:
        return False
    lane_id = str(lane.get("lane_id") or "")
    family = str(lane.get("source_family") or "")
    mode = str(lane.get("lane_mode") or "")
    return bool(lane_id) and ("active_evidence" in family or "ACTIVE_EVIDENCE" in mode or "_active_participation_" in lane_id)


def _bar_timestamp(bar: Mapping[str, Any]) -> datetime | None:
    for key in ("bar_end", "end", "timestamp", "ts", "datetime", "time"):
        parsed = _parse_time(bar.get(key))
        if parsed is not None:
            return parsed
    return None


def _same_minute(left: datetime | None, right: datetime | None) -> bool:
    if left is None or right is None:
        return False
    left = left.astimezone(UTC).replace(second=0, microsecond=0)
    right = right.astimezone(UTC).replace(second=0, microsecond=0)
    return left == right


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 4)


def _has_blocker(blocker: str | None, text: str) -> bool:
    return bool(blocker and text in blocker.upper())


def _first_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _list(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _write_bounded_event_stream(path: Path, rows: Sequence[Mapping[str, Any]], *, max_rows: int) -> None:
    existing: list[str] = []
    try:
        existing = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except OSError:
        existing = []
    new_lines = [json.dumps(dict(row), sort_keys=True) for row in rows]
    retained = (existing + new_lines)[-max(int(max_rows), 1) :]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text("\n".join(retained) + ("\n" if retained else ""), encoding="utf-8")
    tmp.replace(path)


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _ny_tz():
    from zoneinfo import ZoneInfo

    return ZoneInfo("America/New_York")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build read-only Track B PAPER participation observatory report.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--operator-status-path", type=Path, default=DEFAULT_OPERATOR_STATUS_PATH)
    parser.add_argument("--market-data-root", type=Path, default=DEFAULT_MARKET_DATA_ROOT)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-md", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--max-bars-per-lane", type=int, default=120)
    parser.add_argument("--write-event-stream", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBParticipationObservatoryConfig(
        repo_root=args.repo_root,
        operator_status_path=args.operator_status_path,
        market_data_root=args.market_data_root,
        output_json_path=args.output_json,
        output_md_path=args.output_md,
        max_bars_per_lane=args.max_bars_per_lane,
        write_event_stream=args.write_event_stream,
    )
    report = build_track_b_participation_observatory(config=config)
    json_path, md_path = write_track_b_participation_observatory(config=config, report=report)
    print(json.dumps({"classification": report["classification"], "json_path": str(json_path), "md_path": str(md_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
