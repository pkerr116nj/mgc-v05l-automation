"""Targeted paper-engine silent-failure audit.

This report focuses on the live paper path from eligibility through
signal/intent/fill persistence and dashboard rendering freshness.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[3]
NY_TZ = ZoneInfo("America/New_York")

DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "paper_engine_silent_failure_audit"
DEFAULT_AUDIT_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
DEFAULT_OPERATOR_STATUS_PATH = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
DEFAULT_PAPER_CONFIG_PATH = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
DEFAULT_DASHBOARD_API_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "dashboard_api_snapshot.json"
DEFAULT_PAPER_PERFORMANCE_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
DEFAULT_PAPER_TRADE_LOG_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json"
DEFAULT_PAPER_FILLS_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_latest_fills_snapshot.json"


def run_paper_engine_silent_failure_audit(
    *,
    output_dir: str | Path | None = None,
    audit_snapshot_path: str | Path | None = None,
    operator_status_path: str | Path | None = None,
    paper_config_path: str | Path | None = None,
    dashboard_api_snapshot_path: str | Path | None = None,
    paper_performance_path: str | Path | None = None,
    paper_trade_log_path: str | Path | None = None,
    paper_fills_path: str | Path | None = None,
) -> dict[str, Path]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    audit_snapshot = _read_json(audit_snapshot_path or DEFAULT_AUDIT_SNAPSHOT_PATH)
    operator_status = _read_json(operator_status_path or DEFAULT_OPERATOR_STATUS_PATH)
    paper_config = _read_json(paper_config_path or DEFAULT_PAPER_CONFIG_PATH)
    dashboard_api_snapshot = _read_json(dashboard_api_snapshot_path or DEFAULT_DASHBOARD_API_SNAPSHOT_PATH)
    paper_performance = _read_json(paper_performance_path or DEFAULT_PAPER_PERFORMANCE_PATH)
    paper_trade_log = _read_json(paper_trade_log_path or DEFAULT_PAPER_TRADE_LOG_PATH)
    paper_fills = _read_json(paper_fills_path or DEFAULT_PAPER_FILLS_PATH)

    today_ny = datetime.now(NY_TZ).date().isoformat()
    operator_rows = {str(row.get("lane_id")): row for row in operator_status.get("lanes", []) if row.get("lane_id")}
    audit_rows = {str(row.get("lane_id") or row.get("id")): row for row in audit_snapshot.get("rows", []) if row.get("lane_id") or row.get("id")}
    db_rows = _scan_probationary_databases(today_ny)

    eligible_but_silent = []
    status_claims_eligible_without_bar = []
    dashboard_runtime_divergence = []
    for lane_id, operator_row in sorted(operator_rows.items()):
        db_row = db_rows.get(lane_id, {})
        audit_row = audit_rows.get(lane_id, {})
        processed_today = int(db_row.get("processed_bars_today") or 0)
        signals_today = int(db_row.get("signals_today") or 0)
        actionable_signals_today = int(db_row.get("actionable_signals_today") or 0)
        intents_today = int(db_row.get("intents_today") or 0)
        fills_today = int(db_row.get("fills_today") or 0)
        eligible_now = operator_row.get("eligible_now") is True
        entries_enabled = operator_row.get("entries_enabled") is True
        operator_halt = operator_row.get("operator_halt") is True
        if eligible_now and entries_enabled and not operator_halt and processed_today > 0 and actionable_signals_today == 0 and intents_today == 0 and fills_today == 0:
            eligible_but_silent.append(
                {
                    "lane_id": lane_id,
                    "instrument": operator_row.get("instrument"),
                    "family": _lane_family(operator_row),
                    "processed_bars_today": processed_today,
                    "signals_today": signals_today,
                    "actionable_signals_today": actionable_signals_today,
                    "intents_today": intents_today,
                    "fills_today": fills_today,
                    "eligibility_reason": operator_row.get("eligibility_reason"),
                    "latest_fault_or_blocker": operator_row.get("latest_fault_or_blocker"),
                    "decision_stage_counts_today": db_row.get("decision_stage_counts_today") or [],
                    "blocked_reason_counts_today": db_row.get("blocked_reason_counts_today") or [],
                    "raw_setup_candidate_signals_today": int(db_row.get("raw_setup_candidate_signals_today") or 0),
                }
            )
        if eligible_now and entries_enabled and not operator_halt and not operator_row.get("current_bar_timestamp"):
            status_claims_eligible_without_bar.append(
                {
                    "lane_id": lane_id,
                    "instrument": operator_row.get("instrument"),
                    "family": _lane_family(operator_row),
                    "current_detected_session": operator_row.get("current_detected_session"),
                    "latest_signal_timestamp": operator_row.get("latest_signal_timestamp"),
                    "latest_intent_timestamp": operator_row.get("latest_intent_timestamp"),
                    "latest_fill_timestamp": operator_row.get("latest_fill_timestamp"),
                }
            )
        if db_row and processed_today > 0 and (
            audit_row.get("runtime_state_loaded") is False or audit_row.get("runtime_instance_present") is False
        ):
            dashboard_runtime_divergence.append(
                {
                    "lane_id": lane_id,
                    "instrument": operator_row.get("instrument"),
                    "processed_bars_today": processed_today,
                    "audit_runtime_state_loaded": audit_row.get("runtime_state_loaded"),
                    "audit_runtime_instance_present": audit_row.get("runtime_instance_present"),
                    "audit_latest_activity_timestamp": audit_row.get("latest_activity_timestamp"),
                }
            )

    eligibility_reason_counts = Counter(str(row.get("eligibility_reason")) for row in operator_rows.values())
    family_gating_counts = Counter(
        (
            _lane_family(row),
            str(row.get("eligibility_reason") or "None"),
        )
        for row in operator_rows.values()
    )
    operator_halts = [
        {
            "lane_id": lane_id,
            "instrument": row.get("instrument"),
            "family": _lane_family(row),
            "risk_state": row.get("risk_state"),
            "latest_fault_or_blocker": row.get("latest_fault_or_blocker"),
        }
        for lane_id, row in sorted(operator_rows.items())
        if row.get("operator_halt") is True
    ]

    snapshot_freshness = _snapshot_freshness(
        operator_status=operator_status,
        dashboard_api_snapshot=dashboard_api_snapshot,
        audit_snapshot=audit_snapshot,
        paper_performance=paper_performance,
        paper_trade_log=paper_trade_log,
        paper_fills=paper_fills,
    )

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "today_ny": today_ny,
        "live_universe": {
            "paper_config_lane_count": len(paper_config.get("lanes", [])),
            "operator_status_lane_count": len(operator_rows),
            "families": Counter(_lane_family(row) for row in operator_rows.values()),
        },
        "snapshot_freshness": snapshot_freshness,
        "operator_status_summary": {
            "generated_at": operator_status.get("generated_at"),
            "entries_enabled": operator_status.get("entries_enabled"),
            "current_detected_session": operator_status.get("current_detected_session"),
            "halted_lane_count": operator_status.get("halted_lane_count"),
            "faulted_lane_count": operator_status.get("faulted_lane_count"),
            "eligibility_reason_counts": dict(eligibility_reason_counts),
            "family_gating_counts": [
                {
                    "family": family,
                    "eligibility_reason": reason,
                    "count": count,
                }
                for (family, reason), count in sorted(family_gating_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "operator_halts": operator_halts,
        },
        "paper_db_activity_today": {
            "rows": list(db_rows.values()),
            "eligible_but_silent": eligible_but_silent,
            "status_claims_eligible_without_current_bar": status_claims_eligible_without_bar,
            "dashboard_runtime_divergence": dashboard_runtime_divergence,
        },
        "top_causes_of_under_expression_vs_replay": _rank_top_causes(
            eligible_but_silent=eligible_but_silent,
            status_claims_eligible_without_bar=status_claims_eligible_without_bar,
            snapshot_freshness=snapshot_freshness,
            operator_status=operator_status,
            operator_halts=operator_halts,
        ),
        "notes": [
            "This audit intentionally distinguishes live paper runtime truth from dashboard snapshot truth.",
            "Historical replay activity can still exceed paper because replay aggregates many parallel study variants that are not simultaneously loaded in the live paper universe.",
            "A lane that processes bars but emits zero signals/intents/fills in-session is the strongest current indicator of hidden paper under-expression.",
        ],
    }

    json_path = resolved_output_dir / "paper_engine_silent_failure_audit.json"
    markdown_path = resolved_output_dir / "paper_engine_silent_failure_audit.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _read_json(path_like: str | Path) -> dict[str, Any]:
    return json.loads(Path(path_like).read_text(encoding="utf-8"))


def _scan_probationary_databases(today_ny: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(REPO_ROOT.glob("mgc_v05l.probationary.paper__*.sqlite3")):
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        try:
            cursor = connection.cursor()
            lane_id = cursor.execute(
                "select lane_id from strategy_state_snapshots where trim(coalesce(lane_id, '')) <> '' order by updated_at desc limit 1"
            ).fetchone()
            lane_id_value = lane_id[0] if lane_id else path.stem.removeprefix("mgc_v05l.probationary.paper__")
            rows[str(lane_id_value)] = {
                "database_name": path.name,
                "lane_id": str(lane_id_value),
                "processed_bars_total": _scalar(cursor, "select count(*) from processed_bars"),
                "processed_bars_today": _scalar(cursor, "select count(*) from processed_bars where date(end_ts)=?", (today_ny,)),
                "signals_total": _scalar(cursor, "select count(*) from signals"),
                "signals_today": _scalar(cursor, "select count(*) from signals where date(created_at)=?", (today_ny,)),
                "intents_total": _scalar(cursor, "select count(*) from order_intents"),
                "intents_today": _scalar(cursor, "select count(*) from order_intents where date(created_at)=?", (today_ny,)),
                "fills_total": _scalar(cursor, "select count(*) from fills"),
                "fills_today": _scalar(cursor, "select count(*) from fills where date(fill_timestamp)=?", (today_ny,)),
                "last_processed_bar": _scalar(cursor, "select max(end_ts) from processed_bars"),
                "last_signal": _scalar(cursor, "select max(created_at) from signals"),
                "last_intent": _scalar(cursor, "select max(created_at) from order_intents"),
                "last_fill": _scalar(cursor, "select max(fill_timestamp) from fills"),
                "last_state_snapshot": _scalar(cursor, "select max(updated_at) from strategy_state_snapshots"),
            }
            lane_dir = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / str(lane_id_value)
            signal_rows = _read_jsonl(lane_dir / "signals.jsonl")
            intent_rows = _read_jsonl(lane_dir / "order_intents.jsonl") or _read_jsonl(lane_dir / "intents.jsonl")
            fill_rows = _read_jsonl(lane_dir / "fills.jsonl")
            processed_bar_rows = _read_jsonl(lane_dir / "processed_bars.jsonl")
            if signal_rows or intent_rows or fill_rows or processed_bar_rows:
                session_signal_rows = _rows_for_session_date(signal_rows, today_ny, "signal_timestamp", "created_at", "bar_end_ts")
                session_intent_rows = _rows_for_session_date(intent_rows, today_ny, "created_at", "intent_timestamp", "timestamp")
                session_fill_rows = _rows_for_session_date(fill_rows, today_ny, "fill_timestamp", "timestamp")
                session_processed_bar_rows = _rows_for_session_date(processed_bar_rows, today_ny, "end_ts", "timestamp")
                rows[str(lane_id_value)].update(
                    {
                        "processed_bars_today": len(session_processed_bar_rows),
                        "signals_today": len(session_signal_rows),
                        "intents_today": len(session_intent_rows),
                        "fills_today": len(session_fill_rows),
                        "last_signal": _max_timestamp(session_signal_rows or signal_rows, "signal_timestamp", "created_at", "bar_end_ts"),
                        "last_intent": _max_timestamp(session_intent_rows or intent_rows, "created_at", "intent_timestamp", "timestamp"),
                        "last_fill": _max_timestamp(session_fill_rows or fill_rows, "fill_timestamp", "timestamp"),
                    }
                )
                rows[str(lane_id_value)].update(_summarize_signal_rows(session_signal_rows))
            else:
                payload_rows = [
                    row[0]
                    for row in cursor.execute(
                        "select payload_json from signals where date(created_at)=?",
                        (today_ny,),
                    ).fetchall()
                ]
                rows[str(lane_id_value)].update(_summarize_signal_payload_rows(payload_rows))
        finally:
            connection.close()
    return rows


def _scalar(cursor: sqlite3.Cursor, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    return cursor.execute(sql, parameters).fetchone()[0]


def _summarize_signal_payload_rows(payload_json_rows: list[Any]) -> dict[str, Any]:
    payloads = []
    for payload_json in payload_json_rows:
        try:
            payload = json.loads(str(payload_json))
        except Exception:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return _summarize_signal_payload_dicts(payloads)


def _summarize_signal_rows(signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _summarize_signal_payload_dicts(signal_rows)


def _summarize_signal_payload_dicts(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    blocked_reason_counts: Counter[str] = Counter()
    decision_stage_counts: Counter[str] = Counter()
    raw_setup_candidate_signals_today = 0
    actionable_signals_today = 0
    for payload in payloads:
        if bool(payload.get("raw_setup_candidate")):
            raw_setup_candidate_signals_today += 1
        if payload.get("signal_passed_flag") is True and not payload.get("rejection_reason_code"):
            actionable_signals_today += 1
        blocker = str(payload.get("rejection_reason_code") or payload.get("block_reason") or "").strip()
        if blocker:
            blocked_reason_counts[blocker] += 1
        stage = str(payload.get("atp_decision_stage") or payload.get("timing_state") or "").strip()
        if stage:
            decision_stage_counts[stage] += 1
    return {
        "actionable_signals_today": actionable_signals_today,
        "raw_setup_candidate_signals_today": raw_setup_candidate_signals_today,
        "decision_stage_counts_today": [
            {"stage": stage, "count": count}
            for stage, count in decision_stage_counts.most_common(8)
        ],
        "blocked_reason_counts_today": [
            {"reason": reason, "count": count}
            for reason, count in blocked_reason_counts.most_common(8)
        ],
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _rows_for_session_date(rows: list[dict[str, Any]], session_date: str, *timestamp_fields: str) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for row in rows:
        for field in timestamp_fields:
            value = row.get(field)
            if value and _timestamp_matches_session(str(value), session_date):
                matched.append(dict(row))
                break
    return matched


def _timestamp_matches_session(value: str, session_date: str) -> bool:
    parsed = _parse_iso(value)
    if parsed is None:
        return False
    return parsed.astimezone(NY_TZ).date().isoformat() == session_date


def _max_timestamp(rows: list[dict[str, Any]], *fields: str) -> str | None:
    values: list[str] = []
    for row in rows:
        for field in fields:
            value = row.get(field)
            if value:
                values.append(str(value))
                break
    return max(values) if values else None


def _lane_family(row: dict[str, Any]) -> str:
    long_sources = row.get("approved_long_entry_sources") or []
    short_sources = row.get("approved_short_entry_sources") or []
    if long_sources:
        return str(long_sources[0])
    if short_sources:
        return str(short_sources[0])
    lane_id = str(row.get("lane_id") or "")
    if "atp_companion" in lane_id:
        return "active_trend_participation_engine"
    return "UNKNOWN"


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _snapshot_freshness(
    *,
    operator_status: dict[str, Any],
    dashboard_api_snapshot: dict[str, Any],
    audit_snapshot: dict[str, Any],
    paper_performance: dict[str, Any],
    paper_trade_log: dict[str, Any],
    paper_fills: dict[str, Any],
) -> dict[str, Any]:
    reference = _parse_iso(operator_status.get("generated_at"))
    rendered = []
    for label, payload in (
        ("operator_status", operator_status),
        ("dashboard_api_snapshot", dashboard_api_snapshot),
        ("paper_signal_intent_fill_audit", audit_snapshot),
        ("paper_strategy_performance", paper_performance),
        ("paper_strategy_trade_log", paper_trade_log),
        ("paper_latest_fills", paper_fills),
    ):
        generated_at = (
            payload.get("generated_at")
            or payload.get("refreshed_at")
            or payload.get("snapshot_generated_at")
        )
        parsed = _parse_iso(generated_at)
        rendered.append(
            {
                "artifact": label,
                "generated_at": generated_at,
                "minutes_behind_operator_status": (
                    round((reference - parsed).total_seconds() / 60.0, 2)
                    if reference is not None and parsed is not None
                    else None
                ),
            }
        )
    return {
        "reference_operator_status_generated_at": operator_status.get("generated_at"),
        "artifacts": rendered,
    }


def _rank_top_causes(
    *,
    eligible_but_silent: list[dict[str, Any]],
    status_claims_eligible_without_bar: list[dict[str, Any]],
    snapshot_freshness: dict[str, Any],
    operator_status: dict[str, Any],
    operator_halts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    stale_artifacts = [
        row for row in snapshot_freshness.get("artifacts", [])
        if (row.get("minutes_behind_operator_status") or 0) > 5 or row.get("generated_at") is None
    ]
    ranked = [
        {
            "rank": 1,
            "cause": "eligible_lanes_process_bars_but_emit_zero_signals_intents_and_fills",
            "count": len(eligible_but_silent),
            "importance": "highest",
            "why_it_matters": (
                "These lanes are enabled and eligible, and their lane databases show fresh processed bars today, "
                "but they still emitted zero actionable signals, zero intents, and zero fills. This is the strongest direct evidence "
                "of paper under-expression versus replay."
            ),
            "examples": [row["lane_id"] for row in eligible_but_silent[:5]],
        },
        {
            "rank": 2,
            "cause": "status_claims_eligible_without_current_bar_or_recent_signal_state",
            "count": len(status_claims_eligible_without_bar),
            "importance": "high",
            "why_it_matters": (
                "The runtime surface says these lanes are eligible, but they expose no current bar timestamp and no recent signal/intent/fill timestamps. "
                "That suggests the paper engine can look tradable while not actually participating in the current decision loop."
            ),
            "examples": [row["lane_id"] for row in status_claims_eligible_without_bar[:5]],
        },
        {
            "rank": 3,
            "cause": "session_and_warmup_gating_remove_most_of_the_live_universe",
            "count": len(operator_status.get("lanes", [])),
            "importance": "high",
            "why_it_matters": (
                "A large share of the live paper universe is still gated by wrong_session, no_new_completed_bar, or warmup_incomplete. "
                "That is normal for some lanes, but it shrinks the effective live universe far below the broad replay aggregate."
            ),
            "examples": [
                f"{key}={value}"
                for key, value in sorted(Counter(str(row.get('eligibility_reason')) for row in operator_status.get("lanes", [])).items())
            ],
        },
        {
            "rank": 4,
            "cause": "dashboard_snapshot_divergence_hides_or_distorts_runtime_truth",
            "count": len(stale_artifacts),
            "importance": "medium",
            "why_it_matters": (
                "The dashboard artifacts do not refresh together. Some surfaces are current while others lag or omit generated_at entirely, "
                "so fills and day P&L can be real in the engine while lower panels or calendar handoffs still look stale."
            ),
            "examples": [row["artifact"] for row in stale_artifacts[:5]],
        },
        {
            "rank": 5,
            "cause": "persisted_operator_halts_can_recur_and_suppress_expression",
            "count": len(operator_halts),
            "importance": "watch",
            "why_it_matters": (
                "Operator halts are currently cleared, but we have already seen them recur and suppress ATP volume. "
                "Even when not active at audit time, they remain a significant intermittent under-expression risk."
            ),
            "examples": [row["lane_id"] for row in operator_halts[:5]],
        },
    ]
    return ranked


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Paper Engine Silent-Failure Audit",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- New York session day: `{report['today_ny']}`",
        f"- Live paper-config lane count: `{report['live_universe']['paper_config_lane_count']}`",
        f"- Operator-status lane count: `{report['live_universe']['operator_status_lane_count']}`",
        "",
        "## Top Causes",
        "",
    ]
    for row in report["top_causes_of_under_expression_vs_replay"]:
        lines.append(
            f"- #{row['rank']} `{row['cause']}` count={row['count']} importance={row['importance']}: {row['why_it_matters']}"
        )
    lines.extend(
        [
            "",
            "## Eligible But Silent",
            "",
        ]
    )
    for row in report["paper_db_activity_today"]["eligible_but_silent"]:
        lines.append(
            f"- `{row['lane_id']}` instrument=`{row['instrument']}` family=`{row['family']}` processed_bars_today={row['processed_bars_today']} signals_today={row['signals_today']} actionable_signals_today={row['actionable_signals_today']} raw_setup_candidate_signals_today={row['raw_setup_candidate_signals_today']} intents_today={row['intents_today']} fills_today={row['fills_today']} top_blockers={row['blocked_reason_counts_today'][:3]}"
        )
    lines.extend(
        [
            "",
            "## Snapshot Freshness",
            "",
        ]
    )
    for row in report["snapshot_freshness"]["artifacts"]:
        lines.append(
            f"- `{row['artifact']}` generated_at={row['generated_at']} minutes_behind_operator_status={row['minutes_behind_operator_status']}"
        )
    return "\n".join(lines)


def _json_default(value: Any) -> Any:
    if isinstance(value, Counter):
        return dict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
