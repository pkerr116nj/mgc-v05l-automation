"""Durable Strategy Attrition Funnel observability for Track B PAPER.

This module records already-observed strategy progression events. It must never
own strategy, risk, governance, submit, or exit authority.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.models import to_jsonable


DEFAULT_STRATEGY_FUNNEL_DIR = Path("outputs") / "track_b_execution_core" / "strategy_attrition_funnel"
DEFAULT_STRATEGY_FUNNEL_EVENTS = DEFAULT_STRATEGY_FUNNEL_DIR / "strategy_funnel_events.jsonl"
DEFAULT_PAPER_CONFIG_IN_FORCE = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
)
DEFAULT_PAPER_LANE_ROOT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"

STAGES = {
    "lane_enabled",
    "session_eligible",
    "regime_eligible",
    "candidate_generated",
    "rule_pass",
    "rule_fail",
    "risk_pass",
    "risk_fail",
    "governance_pass",
    "governance_fail",
    "route_hold_diagnostic",
    "submit_attempted",
    "broker_accepted",
    "fill_observed",
    "managed_adopted",
    "exit_due",
    "exit_submitted",
    "exit_filled",
}

PASS_FAIL = {"PASS", "FAIL", "DIAGNOSTIC", "UNKNOWN"}
NOISEMAKER_FAMILY = "paper_active_evidence"


@dataclass(frozen=True)
class StrategyFunnelWriteResult:
    event_path: Path
    events_written: int


def build_strategy_funnel_event(
    *,
    stage: str,
    pass_fail: str,
    timestamp: datetime | str | None = None,
    lane_id: str | None = None,
    strategy_family: str | None = None,
    profile: str | None = None,
    reason: str | None = None,
    blocker_classification: str | None = None,
    candidate_id: str | None = None,
    order_intent_id: str | None = None,
    lifecycle_id: str | None = None,
    trade_id: str | None = None,
    instrument: str | None = None,
    session_phase: str | None = None,
    source_component: str | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_stage = str(stage or "").strip()
    normalized_pass_fail = str(pass_fail or "UNKNOWN").strip().upper()
    if normalized_stage not in STAGES:
        raise ValueError(f"unknown strategy funnel stage: {normalized_stage}")
    if normalized_pass_fail not in PASS_FAIL:
        raise ValueError(f"unknown strategy funnel pass_fail: {normalized_pass_fail}")
    return {
        "schema_version": "track_b_strategy_attrition_funnel_event_v1",
        "timestamp": _iso_timestamp(timestamp),
        "lane_id": lane_id,
        "strategy_family": strategy_family,
        "profile": profile,
        "stage": normalized_stage,
        "pass_fail": normalized_pass_fail,
        "reason": reason,
        "blocker_classification": blocker_classification,
        "candidate_id": candidate_id,
        "order_intent_id": order_intent_id,
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "instrument": instrument,
        "session_phase": session_phase,
        "source_component": source_component,
        "diagnostics": to_jsonable(dict(diagnostics or {})) if diagnostics else None,
    }


def record_strategy_funnel_event(
    *,
    repo_root: Path | str = Path("."),
    event_path: Path | str = DEFAULT_STRATEGY_FUNNEL_EVENTS,
    **event_fields: Any,
) -> Path:
    result = record_strategy_funnel_events(
        [build_strategy_funnel_event(**event_fields)],
        repo_root=repo_root,
        event_path=event_path,
    )
    return result.event_path


def record_strategy_funnel_events(
    events: Iterable[Mapping[str, Any]],
    *,
    repo_root: Path | str = Path("."),
    event_path: Path | str = DEFAULT_STRATEGY_FUNNEL_EVENTS,
) -> StrategyFunnelWriteResult:
    resolved = _resolve(Path(repo_root), Path(event_path))
    rows = [to_jsonable(dict(event)) for event in events]
    if not rows:
        return StrategyFunnelWriteResult(event_path=resolved, events_written=0)
    for row in rows:
        append_bounded_jsonl(resolved, row)
    return StrategyFunnelWriteResult(event_path=resolved, events_written=len(rows))


def try_record_strategy_funnel_events(
    events: Iterable[Mapping[str, Any]],
    *,
    repo_root: Path | str = Path("."),
    event_path: Path | str = DEFAULT_STRATEGY_FUNNEL_EVENTS,
) -> StrategyFunnelWriteResult | None:
    try:
        return record_strategy_funnel_events(events, repo_root=repo_root, event_path=event_path)
    except Exception:
        return None


def events_from_no_trade_diagnostic(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    timestamp = payload.get("bar_timestamp") or payload.get("generated_at")
    lane_id = _str_or_none(payload.get("lane_id"))
    runtime_identity = _mapping(payload.get("runtime_identity"))
    strategy_family = _str_or_none(runtime_identity.get("strategy_family"))
    instrument = _str_or_none(payload.get("symbol") or runtime_identity.get("instrument"))
    session_phase = _str_or_none(payload.get("session"))
    source = "track_b_no_trade_diagnostics"
    common = {
        "timestamp": timestamp,
        "lane_id": lane_id,
        "strategy_family": strategy_family,
        "instrument": instrument,
        "session_phase": session_phase,
        "source_component": source,
        "order_intent_id": _str_or_none(payload.get("order_intent_id")),
    }
    events = [
        build_strategy_funnel_event(
            stage="session_eligible",
            pass_fail="PASS" if payload.get("session_allowed") is not False else "FAIL",
            reason=None if payload.get("session_allowed") is not False else "session_not_allowed",
            **common,
        )
    ]
    setup_detected = payload.get("setup_detected") is True
    final_decision = str(payload.get("final_decision") or "")
    blocker = _str_or_none(payload.get("blocker_reason"))
    if setup_detected:
        events.append(build_strategy_funnel_event(stage="candidate_generated", pass_fail="PASS", **common))
    if final_decision == "NO_SETUP":
        return events
    if final_decision == "FILTER_REJECTED":
        events.append(
            build_strategy_funnel_event(
                stage="rule_fail",
                pass_fail="FAIL",
                reason=blocker or "filter_rejected",
                blocker_classification="FILTER_REJECTED",
                **common,
            )
        )
    elif final_decision in {"GOVERNANCE_BLOCKED", "ROUTE_HELD_UNTIL_READINESS_CONVERGED"}:
        events.append(build_strategy_funnel_event(stage="rule_pass", pass_fail="PASS", **common))
        events.append(
            build_strategy_funnel_event(
                stage="governance_fail",
                pass_fail="FAIL",
                reason=blocker,
                blocker_classification="GOVERNANCE_BLOCKED",
                **common,
            )
        )
    elif final_decision in {"STARTUP_CATCHUP_DIAGNOSTIC_ONLY", "STARTUP_CATCHUP_NOT_ROUTABLE"}:
        events.append(build_strategy_funnel_event(stage="rule_pass", pass_fail="PASS", **common))
        events.append(
            build_strategy_funnel_event(
                stage="route_hold_diagnostic",
                pass_fail="DIAGNOSTIC",
                reason=blocker,
                blocker_classification=final_decision,
                **common,
            )
        )
    elif final_decision == "EXPOSURE_BLOCKED":
        events.append(build_strategy_funnel_event(stage="rule_pass", pass_fail="PASS", **common))
        events.append(
            build_strategy_funnel_event(
                stage="risk_fail",
                pass_fail="FAIL",
                reason=blocker,
                blocker_classification="EXPOSURE_BLOCKED",
                **common,
            )
        )
    elif final_decision in {"ORDER_INTENT_CREATED", "WOULD_ROUTE"}:
        events.append(build_strategy_funnel_event(stage="rule_pass", pass_fail="PASS", **common))
        events.append(build_strategy_funnel_event(stage="risk_pass", pass_fail="PASS", **common))
        events.append(build_strategy_funnel_event(stage="governance_pass", pass_fail="PASS", **common))
    return events


def events_from_blocked_strategy_intent(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    common = _common_from_strategy_payload(payload, source_component="strategy_engine_blocked_intent")
    blocker = _str_or_none(payload.get("blocker_classification") or payload.get("bridge_classification"))
    reason = _str_or_none(payload.get("exact_blocker_reason") or payload.get("bridge_detail"))
    stage = "risk_fail" if _looks_like_risk_blocker(blocker, reason) else "governance_fail"
    events = [
        build_strategy_funnel_event(stage="candidate_generated", pass_fail="PASS", **common),
        build_strategy_funnel_event(stage="rule_pass", pass_fail="PASS", **common),
        build_strategy_funnel_event(
            stage=stage,
            pass_fail="FAIL",
            reason=reason,
            blocker_classification=blocker,
            **common,
        ),
    ]
    if payload.get("submit_attempted") is True:
        events.append(
            build_strategy_funnel_event(
                stage="submit_attempted",
                pass_fail="PASS",
                reason=reason,
                blocker_classification=blocker,
                **common,
            )
        )
    return events


def events_from_submit_attempt(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    common = _common_from_strategy_payload(payload, source_component="strategy_engine_submit")
    return [
        build_strategy_funnel_event(stage="risk_pass", pass_fail="PASS", **common),
        build_strategy_funnel_event(stage="governance_pass", pass_fail="PASS", **common),
        build_strategy_funnel_event(stage="submit_attempted", pass_fail="PASS", **common),
        build_strategy_funnel_event(stage="broker_accepted", pass_fail="PASS", **common),
    ]


def events_from_filled_bridge_result(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    common = _common_from_strategy_payload(payload, source_component="strategy_engine_fill")
    events = [
        build_strategy_funnel_event(stage="broker_accepted", pass_fail="PASS", **common),
        build_strategy_funnel_event(stage="fill_observed", pass_fail="PASS", **common),
    ]
    adoption = _str_or_none(payload.get("broker_backed_entry_auto_adoption"))
    if adoption:
        events.append(
            build_strategy_funnel_event(
                stage="managed_adopted",
                pass_fail="PASS" if "BLOCKED" not in adoption and "FAILED" not in adoption else "FAIL",
                reason=adoption,
                blocker_classification=None if "BLOCKED" not in adoption and "FAILED" not in adoption else adoption,
                **common,
            )
        )
    return events


def events_from_managed_position_registry(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    timestamp = payload.get("generated_at")
    for row in _list(payload.get("managed_positions")):
        classification = _str_or_none(row.get("classification"))
        if not classification:
            continue
        common = {
            "timestamp": timestamp,
            "lane_id": _str_or_none(row.get("lane_id") or row.get("strategy_id")),
            "strategy_family": _str_or_none(row.get("strategy_family")),
            "instrument": _str_or_none(row.get("instrument") or row.get("symbol") or row.get("localSymbol")),
            "lifecycle_id": _str_or_none(row.get("lifecycle_id")),
            "trade_id": _str_or_none(row.get("trade_id")),
            "source_component": "track_b_managed_position_registry",
        }
        if classification in {"OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE", "OPEN_MANAGED_CLOSE_WORKING"}:
            events.append(
                build_strategy_funnel_event(
                    stage="managed_adopted",
                    pass_fail="PASS",
                    reason=classification,
                    **common,
                )
            )
        if row.get("exit_due") is True or classification == "OPEN_MANAGED_EXIT_DUE":
            events.append(
                build_strategy_funnel_event(
                    stage="exit_due",
                    pass_fail="PASS",
                    reason=classification,
                    **common,
                )
            )
    return events


def events_from_managed_exit_service_status(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    plan = _mapping(payload.get("execution_plan"))
    intents = _list(plan.get("executable_intents")) or _list(plan.get("exit_intents"))
    rows = intents or _list(payload.get("attempted_closes")) or [{}]
    submitted = payload.get("submit_attempted") is True or int(payload.get("submitted_count") or 0) > 0
    filled = str(payload.get("classification") or "") == "APPLY_SUCCEEDED" and payload.get("broker_state_mutated") is True
    events: list[dict[str, Any]] = []
    if not submitted and not filled:
        return events
    for row in rows:
        common = {
            "timestamp": payload.get("generated_at"),
            "lane_id": _str_or_none(row.get("lane_id") or row.get("strategy_id")),
            "strategy_family": _str_or_none(row.get("strategy_id") if row.get("lane_id") is None else row.get("strategy_family")),
            "instrument": _str_or_none(row.get("instrument") or row.get("localSymbol") or row.get("local_symbol")),
            "candidate_id": _str_or_none(row.get("exit_intent_id")),
            "lifecycle_id": _str_or_none(row.get("lifecycle_id")),
            "trade_id": _str_or_none(row.get("trade_id")),
            "source_component": "track_b_managed_exit_service",
            "reason": _str_or_none(payload.get("classification")),
        }
        if submitted:
            events.append(build_strategy_funnel_event(stage="exit_submitted", pass_fail="PASS", **common))
        if filled:
            events.append(build_strategy_funnel_event(stage="exit_filled", pass_fail="PASS", **common))
    return events


def summarize_strategy_attrition_funnel(
    *,
    repo_root: Path | str = Path("."),
    event_path: Path | str = DEFAULT_STRATEGY_FUNNEL_EVENTS,
    paper_config_in_force_path: Path | str = DEFAULT_PAPER_CONFIG_IN_FORCE,
    lane_root: Path | str = DEFAULT_PAPER_LANE_ROOT,
    now: datetime | None = None,
    include_legacy_artifacts: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root)
    actual_now = now or datetime.now(UTC)
    active_profile = _load_active_profile(root, Path(paper_config_in_force_path))
    active_lanes = list(active_profile.get("active_lane_ids") or [])
    lane_configs = {
        str(row.get("lane_id")): dict(row)
        for row in _list(active_profile.get("lanes"))
        if row.get("lane_id")
    }
    events = _read_jsonl(_resolve(root, Path(event_path)))
    if include_legacy_artifacts:
        events = [*events, *_legacy_events_from_lane_artifacts(root, Path(lane_root), active_lanes, lane_configs)]
    windows = {
        "today": datetime(actual_now.year, actual_now.month, actual_now.day, tzinfo=UTC),
        "24h": actual_now - timedelta(hours=24),
        "7d": actual_now - timedelta(days=7),
    }
    lane_rows: list[dict[str, Any]] = []
    top_blockers = Counter()
    for lane_id in active_lanes:
        cfg = lane_configs.get(str(lane_id), {})
        lane_events = [event for event in events if event.get("lane_id") == lane_id]
        windows_payload = {
            name: _summarize_window(lane_events, start)
            for name, start in windows.items()
        }
        for payload in windows_payload.values():
            top_blockers.update(payload.get("top_blockers_counter") or {})
            payload.pop("top_blockers_counter", None)
        lane_rows.append(
            {
                "lane_id": lane_id,
                "strategy_family": cfg.get("strategy_family") or cfg.get("source_family"),
                "noisemaker_active_participation": _is_noisemaker_lane(lane_id, cfg),
                "windows": windows_payload,
            }
        )
    regular_active = [
        lane_id for lane_id in active_lanes if not _is_noisemaker_lane(str(lane_id), lane_configs.get(str(lane_id), {}))
    ]
    noisemaker_active = [lane_id for lane_id in active_lanes if lane_id not in regular_active]
    regular_enabled_and_blocked = [
        row["lane_id"]
        for row in lane_rows
        if not row["noisemaker_active_participation"]
        and row["windows"]["24h"]["label"] in {"FILTERED_OUT", "GOVERNANCE_BLOCKED"}
    ]
    return {
        "schema_version": "track_b_strategy_attrition_funnel_summary_v1",
        "generated_at": actual_now.isoformat(),
        "event_path": str(_resolve(root, Path(event_path))),
        "event_count": len(events),
        "profile": active_profile.get("profile") or active_profile.get("profile_name") or "mnq_mes_full_session_active_evidence",
        "active_profile_lane_count": len(active_lanes),
        "regular_strategy_active_count": len(regular_active),
        "noisemaker_active_participation_active_count": len(noisemaker_active),
        "regular_enabled_and_blocked": regular_enabled_and_blocked,
        "top_blocker_categories": top_blockers.most_common(10),
        "lanes": lane_rows,
        "summary_is_read_only": True,
        "broker_connection_attempted": False,
        "submit_attempted": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize Track B strategy attrition funnel events.")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--event-path", type=Path, default=DEFAULT_STRATEGY_FUNNEL_EVENTS)
    parser.add_argument("--no-write", action="store_true", help="Accepted for read-only symmetry; summaries are not written.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary.")
    args = parser.parse_args(argv)
    summary = summarize_strategy_attrition_funnel(repo_root=args.repo_root, event_path=args.event_path)
    if args.json:
        print(json.dumps(to_jsonable(summary), indent=2, sort_keys=True))
    else:
        print(f"active_profile_lane_count={summary['active_profile_lane_count']}")
        print(f"regular_strategy_active_count={summary['regular_strategy_active_count']}")
        print(f"noisemaker_active_participation_active_count={summary['noisemaker_active_participation_active_count']}")
        print(f"top_blocker_categories={summary['top_blocker_categories']}")
    return 0


def _summarize_window(events: Sequence[Mapping[str, Any]], start: datetime) -> dict[str, Any]:
    rows = [event for event in events if (_parse_timestamp(event.get("timestamp")) or datetime.min.replace(tzinfo=UTC)) >= start]
    stage_counts = Counter(str(event.get("stage") or "") for event in rows)
    blockers = Counter(
        str(event.get("blocker_classification") or event.get("reason") or "UNKNOWN")
        for event in rows
        if str(event.get("pass_fail") or "").upper() == "FAIL"
    )
    if stage_counts.get("fill_observed") or stage_counts.get("exit_filled") or stage_counts.get("managed_adopted"):
        label = "TRADING_OK"
    elif stage_counts.get("governance_fail") or stage_counts.get("risk_fail"):
        label = "GOVERNANCE_BLOCKED"
    elif stage_counts.get("rule_fail"):
        label = "FILTERED_OUT"
    elif stage_counts.get("candidate_generated") or stage_counts.get("submit_attempted"):
        label = "SUBMIT_PATH_OK_LOW_FREQUENCY"
    else:
        label = "NO_SIGNALS"
    return {
        "event_count": len(rows),
        "stage_counts": dict(stage_counts),
        "top_blockers": blockers.most_common(5),
        "top_blockers_counter": blockers,
        "label": label,
    }


def _legacy_events_from_lane_artifacts(
    repo_root: Path,
    lane_root: Path,
    active_lanes: Sequence[str],
    lane_configs: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    root = _resolve(repo_root, lane_root)
    for lane_id in active_lanes:
        lane_dir = root / str(lane_id)
        cfg = lane_configs.get(str(lane_id), {})
        for row in _read_jsonl(lane_dir / "blocked_strategy_intents.jsonl"):
            events.extend(events_from_blocked_strategy_intent({**row, "strategy_family": row.get("strategy_family") or cfg.get("strategy_family")}))
        for row in _read_jsonl(lane_dir / "filled_bridge_results.jsonl"):
            events.extend(events_from_filled_bridge_result({**row, "strategy_family": row.get("strategy_family") or cfg.get("strategy_family")}))
        for row in _read_jsonl(lane_dir / "fills.jsonl"):
            common = _common_from_strategy_payload(
                {
                    **row,
                    "lane_id": lane_id,
                    "strategy_family": cfg.get("strategy_family") or cfg.get("source_family"),
                    "created_at": row.get("fill_timestamp") or row.get("filled_at") or row.get("timestamp"),
                },
                source_component="legacy_lane_fills",
            )
            events.append(build_strategy_funnel_event(stage="fill_observed", pass_fail="PASS", **common))
    return events


def _common_from_strategy_payload(payload: Mapping[str, Any], *, source_component: str) -> dict[str, Any]:
    return {
        "timestamp": payload.get("created_at")
        or payload.get("fill_timestamp")
        or payload.get("decision_bar_timestamp")
        or payload.get("signal_timestamp"),
        "lane_id": _str_or_none(payload.get("lane_id") or payload.get("strategy_id")),
        "strategy_family": _str_or_none(payload.get("strategy_family")),
        "profile": _str_or_none(payload.get("profile")),
        "candidate_id": _str_or_none(payload.get("candidate_id") or payload.get("signal_id") or payload.get("bar_id")),
        "order_intent_id": _str_or_none(payload.get("order_intent_id")),
        "lifecycle_id": _str_or_none(payload.get("lifecycle_id")),
        "trade_id": _str_or_none(payload.get("trade_id")),
        "instrument": _str_or_none(payload.get("instrument") or payload.get("symbol")),
        "session_phase": _str_or_none(payload.get("session_phase") or payload.get("session")),
        "source_component": source_component,
    }


def _load_active_profile(repo_root: Path, path: Path) -> dict[str, Any]:
    resolved = _resolve(repo_root, path)
    if not resolved.exists():
        return {"active_lane_ids": [], "lanes": []}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception:
        return {"active_lane_ids": [], "lanes": []}
    return dict(payload) if isinstance(payload, Mapping) else {"active_lane_ids": [], "lanes": []}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, Mapping):
                    rows.append(dict(row))
    except OSError:
        return []
    return rows


def _iso_timestamp(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(UTC).isoformat()
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC).isoformat()
    parsed = _parse_timestamp(value)
    return parsed.isoformat() if parsed else str(value)


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)).astimezone(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value if isinstance(row, Mapping)] if isinstance(value, list) else []


def _looks_like_risk_blocker(blocker: str | None, reason: str | None) -> bool:
    haystack = f"{blocker or ''} {reason or ''}".lower()
    return any(token in haystack for token in ("exposure", "position", "order_conflict", "over-close", "over_close"))


def _is_noisemaker_lane(lane_id: str, config: Mapping[str, Any]) -> bool:
    family = str(config.get("strategy_family") or config.get("source_family") or "").lower()
    mode = str(config.get("lane_mode") or "").lower()
    return NOISEMAKER_FAMILY in family or "active_participation" in lane_id or "active_evidence" in mode


if __name__ == "__main__":
    raise SystemExit(main())
