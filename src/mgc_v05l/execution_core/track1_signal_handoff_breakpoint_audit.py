"""Read-only Track 1 signal-to-handoff breakpoint audit.

This diagnostic starts from the cheap Track 1 trading-stop preflight and then
inspects the narrow post-trade window for the missing link between strategy
signals and paper handoff/lifecycle artifacts. It is artifact-only and never
invokes broker, paper proof, submit, cancel, or placeOrder paths.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_research_offline_metadata import apply_research_offline_metadata


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_JSON = DEFAULT_OUTPUT_ROOT / "latest_track1_signal_to_handoff_breakpoint_audit.json"
DEFAULT_MD = DEFAULT_OUTPUT_ROOT / "latest_track1_signal_to_handoff_breakpoint_audit.md"
DEFAULT_PREFLIGHT = DEFAULT_OUTPUT_ROOT / "latest_track1_trading_stop_preflight.json"
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_ROWS = 5000


@dataclass(frozen=True)
class Track1SignalHandoffBreakpointAuditResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]
    markdown: str


def build_track1_signal_handoff_breakpoint_audit(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    preflight_path: Path | None = None,
    now: datetime | None = None,
    write: bool = True,
) -> Track1SignalHandoffBreakpointAuditResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    output = Path(output_root)
    preflight = _load_json(root / (preflight_path or DEFAULT_PREFLIGHT))
    breakpoint = preflight.get("breakpoint") if isinstance(preflight.get("breakpoint"), Mapping) else {}
    last_trade = _event_dict(breakpoint.get("last_known_track1_trade"))
    last_signal = _event_dict(breakpoint.get("last_known_strategy_signal"))
    window_start = str(last_trade.get("timestamp") or "2026-04-29T08:35:00-04:00")
    window_end = str(last_signal.get("timestamp") or "2026-05-01T04:31:29.459796+00:00")

    last_trade_detail = _last_trade_detail(root, last_trade)
    signal_reviews = _session_review_signals(root, window_start=window_start)
    signal_summary = _summarize_signal_rows(signal_reviews)
    intent_summary = _latest_rows_summary(root / "outputs/operator_dashboard/paper_latest_intents_snapshot.json")
    fill_summary = _latest_rows_summary(root / "outputs/operator_dashboard/paper_latest_fills_snapshot.json")
    blotter_summary = _latest_rows_summary(root / "outputs/operator_dashboard/paper_latest_blotter_snapshot.json")
    config_evidence = _config_evidence(root)
    runner_evidence = _runner_evidence(root)
    broker_block_evidence = _broker_block_evidence(signal_reviews)
    commit_candidates = _commit_candidates(root, window_start=window_start, window_end=window_end)
    classification, missing_link, explanation = _classify(
        signal_summary=signal_summary,
        intent_summary=intent_summary,
        fill_summary=fill_summary,
        config_evidence=config_evidence,
        runner_evidence=runner_evidence,
        broker_block_evidence=broker_block_evidence,
    )
    suspected_commit = _suspected_commit(commit_candidates, window_start)
    report_json = output / DEFAULT_JSON.name
    report_md = output / DEFAULT_MD.name
    report = apply_research_offline_metadata(
        {
            "schema_version": "track1_signal_handoff_breakpoint_audit_v1",
            "generated_at": actual_now.isoformat(),
            "bounded_policy": {
                "broker_commands_invoked": False,
                "paper_proof_cli_invoked": False,
                "submit_cancel_place_order_invoked": False,
                "broker_state_mutated": False,
                "max_file_bytes": MAX_FILE_BYTES,
                "max_rows_per_artifact": MAX_ROWS,
            },
            "source_preflight_path": str(preflight_path or DEFAULT_PREFLIGHT),
            "audit_window": {
                "start": window_start,
                "end": window_end,
                "scope_note": "Narrow window from last direct Track 1-like trade to latest post-trade signal/session-review evidence.",
            },
            "classification": classification,
            "missing_link": missing_link,
            "classification_explanation": explanation,
            "last_known_trade": last_trade,
            "last_known_trade_detail": last_trade_detail,
            "first_known_signal_without_trade": signal_summary.get("first_signal_without_trade"),
            "latest_signal_without_trade": signal_summary.get("latest_signal_without_trade"),
            "post_trade_signal_summary": signal_summary,
            "handoff_intent_summary": intent_summary,
            "fill_summary": fill_summary,
            "blotter_summary": blotter_summary,
            "config_evidence": config_evidence,
            "runner_evidence": runner_evidence,
            "broker_block_evidence": broker_block_evidence,
            "commit_candidates": commit_candidates,
            "suspected_commit_or_config_breakpoint": suspected_commit,
            "current_strategy_managed_lifecycle_v1_assessment": _managed_lifecycle_assessment(classification),
            "recommended_next_action": _recommended_next_action(classification),
        },
        producer="track1_signal_handoff_breakpoint_audit",
        source_paths=[
            preflight_path or DEFAULT_PREFLIGHT,
            "outputs/operator_dashboard/paper_latest_intents_snapshot.json",
            "outputs/operator_dashboard/paper_latest_fills_snapshot.json",
            "outputs/operator_dashboard/paper_latest_blotter_snapshot.json",
            "outputs/operator_dashboard/paper_session_close_reviews",
        ],
        notes=[
            "Track 1 breakpoint diagnostics read dashboard history as offline forensic evidence only.",
            "This report is not Track B shared truth, broker truth, market-data runtime truth, or routing authority.",
        ],
    )
    markdown = _markdown(report)
    if write:
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        report_md.write_text(markdown, encoding="utf-8")
    return Track1SignalHandoffBreakpointAuditResult(
        report_json=report_json,
        report_md=report_md,
        report=report,
        markdown=markdown,
    )


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size > MAX_FILE_BYTES:
        return {}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _event_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _last_trade_detail(root: Path, event: Mapping[str, Any]) -> dict[str, Any]:
    path = event.get("path")
    if not isinstance(path, str):
        return {"found": False, "reason": "last trade path unavailable"}
    payload = _load_json(root / path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    if not rows:
        return {"found": False, "path": path, "reason": "trade log rows unavailable"}
    stamped = [row for row in rows if isinstance(row, Mapping) and row.get("entry_timestamp")]
    latest = max(stamped, key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp"))) if stamped else None
    if not isinstance(latest, Mapping):
        return {"found": False, "path": path, "reason": "no timestamped trade row found"}
    return {
        "found": True,
        "path": path,
        "trade_id": latest.get("trade_id"),
        "strategy_id": latest.get("strategy_key") or latest.get("standalone_strategy_id") or latest.get("lane_id"),
        "lane_id": latest.get("lane_id"),
        "instrument": latest.get("instrument"),
        "side": latest.get("side"),
        "entry_timestamp": latest.get("entry_timestamp"),
        "exit_timestamp": latest.get("exit_timestamp"),
        "entry_price": latest.get("entry_price"),
        "exit_price": latest.get("exit_price"),
        "status": latest.get("status"),
        "exit_reason": latest.get("exit_reason"),
    }


def _session_review_signals(root: Path, *, window_start: str) -> list[dict[str, Any]]:
    review_root = root / "outputs/operator_dashboard/paper_session_close_reviews"
    if not review_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for path in sorted(review_root.glob("*.json")):
        payload = _load_json(path)
        for row in payload.get("rows") if isinstance(payload.get("rows"), list) else []:
            if not isinstance(row, Mapping):
                continue
            signal_count = _as_int(row.get("signal_count"))
            latest_ts = str(row.get("latest_event_timestamp") or row.get("last_broken_close_ts") or "")
            if signal_count <= 0 or (latest_ts and latest_ts <= window_start):
                continue
            unique_key = (str(row.get("lane_id") or ""), latest_ts, str(row.get("session_verdict") or ""))
            if unique_key in seen:
                continue
            seen.add(unique_key)
            rows.append(
                {
                    "path": str(path.relative_to(root)),
                    "lane_id": row.get("lane_id"),
                    "instrument": row.get("instrument"),
                    "session_verdict": row.get("session_verdict"),
                    "signal_count": signal_count,
                    "intent_count": _as_int(row.get("intent_count")),
                    "fill_count": _as_int(row.get("fill_count")),
                    "evidence_chain_status": row.get("evidence_chain_status"),
                    "latest_event_timestamp": row.get("latest_event_timestamp"),
                    "last_broken_close_ts": row.get("last_broken_close_ts"),
                    "risk_state": row.get("risk_state"),
                    "latest_halt_reason": row.get("latest_halt_reason"),
                    "open_first_recommendation": row.get("open_first_recommendation"),
                    "evidence_counts": row.get("evidence_counts"),
                    "source_family": row.get("source_family"),
                }
            )
            if len(rows) >= MAX_ROWS:
                return rows
    return rows


def _summarize_signal_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: str(row.get("latest_event_timestamp") or row.get("last_broken_close_ts") or ""))
    signal_rows = [row for row in sorted_rows if _as_int(row.get("signal_count")) > 0]
    intent_rows = [row for row in signal_rows if _as_int(row.get("intent_count")) > 0]
    fill_rows = [row for row in signal_rows if _as_int(row.get("fill_count")) > 0]
    signal_no_fill = [row for row in signal_rows if str(row.get("session_verdict")) == "SIGNAL_NO_FILL"]
    zero_intent_signal_rows = [row for row in signal_rows if _as_int(row.get("intent_count")) == 0]
    return {
        "source": "paper_session_close_reviews",
        "signal_rows": len(signal_rows),
        "signal_count": sum(_as_int(row.get("signal_count")) for row in signal_rows),
        "intent_rows": len(intent_rows),
        "fill_rows": len(fill_rows),
        "signal_no_fill_rows": len(signal_no_fill),
        "zero_intent_signal_rows": len(zero_intent_signal_rows),
        "broken_evidence_chain_rows": sum(1 for row in signal_rows if str(row.get("evidence_chain_status")).upper() == "BROKEN"),
        "first_signal_without_trade": signal_no_fill[0] if signal_no_fill else (signal_rows[0] if signal_rows else None),
        "latest_signal_without_trade": signal_no_fill[-1] if signal_no_fill else (signal_rows[-1] if signal_rows else None),
        "sample_signal_rows": signal_rows[:20],
        "session_review_only_risk": bool(signal_rows)
        and all(str(row.get("session_verdict")) == "SIGNAL_NO_FILL" for row in signal_rows)
        and all(_as_int(row.get("intent_count")) == 0 for row in signal_rows),
    }


def _latest_rows_summary(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    return {
        "path": str(path),
        "exists": path.exists(),
        "row_count": len(rows),
        "generated_at": payload.get("generated_at"),
        "sample_rows": rows[:5],
    }


def _config_evidence(root: Path) -> dict[str, Any]:
    paths = list((root / "config").glob("*.json")) + list((root / "config").glob("*.yaml")) + list((root / "config").glob("*.yml"))
    findings: list[dict[str, Any]] = []
    for path in sorted(paths)[:200]:
        if path.stat().st_size > MAX_FILE_BYTES:
            continue
        text = path.read_text(encoding="utf-8", errors="replace").lower()
        if any(token in text for token in ("paper_on_signal", "enable_paper", "paper_trading", "paper_proof", "submit_allowed")):
            findings.append(
                {
                    "path": str(path.relative_to(root)),
                    "mentions_paper_on_signal": "paper_on_signal" in text,
                    "mentions_enable_paper": "enable_paper" in text or "paper_trading" in text,
                    "mentions_proof": "paper_proof" in text,
                    "mentions_submit_allowed": "submit_allowed" in text,
                    "disabled_marker": '"enabled": false' in text or "enabled: false" in text,
                }
            )
    return {
        "config_files_scanned": len(paths[:200]),
        "relevant_config_files": findings[:40],
        "handoff_disabled_marker_found": any(item["disabled_marker"] for item in findings),
    }


def _runner_evidence(root: Path) -> dict[str, Any]:
    files = [
        root / "outputs/operator_dashboard/paper_latest_intents_snapshot.json",
        root / "outputs/operator_dashboard/paper_latest_fills_snapshot.json",
        root / "outputs/operator_dashboard/paper_latest_blotter_snapshot.json",
        root / "outputs/operator_dashboard/paper_signal_intent_fill_audit_snapshot.json",
    ]
    runner_rows = []
    for path in files:
        payload = _load_json(path)
        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
        runner_rows.append({"path": str(path.relative_to(root)), "row_count": len(rows), "generated_at": payload.get("generated_at")})
    return {
        "runner_or_intent_artifacts": runner_rows,
        "paper_runner_invocation_evidence_found": any(item["row_count"] for item in runner_rows[:3]),
        "intent_artifact_empty": not any(item["row_count"] for item in runner_rows[:1]),
    }


def _broker_block_evidence(signal_rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(signal_rows)
    blocked = [
        row
        for row in rows
        if row.get("latest_halt_reason")
        or str(row.get("risk_state")).upper() not in {"", "NONE", "OK"}
    ]
    return {
        "broker_or_risk_block_rows": len(blocked),
        "sample_block_rows": blocked[:10],
    }


def _commit_candidates(root: Path, *, window_start: str, window_end: str) -> list[dict[str, str]]:
    # Read-only local git metadata. Failure is non-fatal for tests/temp dirs.
    since = "2026-04-28"
    until = "2026-05-01 05:00"
    try:
        completed = subprocess.run(
            ["git", "log", f"--since={since}", f"--until={until}", "--date=iso", "--pretty=format:%H%x09%ad%x09%s", "--", "src", "scripts", "config", "desktop"],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if completed.returncode != 0:
        return []
    commits: list[dict[str, str]] = []
    for line in completed.stdout.splitlines()[:80]:
        parts = line.split("\t", 2)
        if len(parts) == 3:
            commits.append({"commit": parts[0], "date": parts[1], "subject": parts[2]})
    return commits


def _suspected_commit(commits: list[Mapping[str, str]], window_start: str) -> dict[str, Any] | None:
    if not commits:
        return None
    keywords = ("route", "bridge", "paper", "submit", "authority", "readiness", "runtime")
    ranked = [commit for commit in commits if any(token in commit.get("subject", "").lower() for token in keywords)]
    after = [commit for commit in ranked if commit.get("date", "") > "2026-04-29 08:35"]
    candidate = after[-1] if after else ranked[0]
    return {
        "commit": candidate.get("commit"),
        "date": candidate.get("date"),
        "subject": candidate.get("subject"),
        "confidence": "LOW",
        "note": "Closest relevant read-only git candidate near the artifact breakpoint; not proven causal by this audit.",
    }


def _classify(
    *,
    signal_summary: Mapping[str, Any],
    intent_summary: Mapping[str, Any],
    fill_summary: Mapping[str, Any],
    config_evidence: Mapping[str, Any],
    runner_evidence: Mapping[str, Any],
    broker_block_evidence: Mapping[str, Any],
) -> tuple[str, str, str]:
    if config_evidence.get("handoff_disabled_marker_found"):
        return "HANDOFF_DISABLED_BY_CONFIG", "config", "A relevant config file contains a disabled marker in paper/handoff context."
    if broker_block_evidence.get("broker_or_risk_block_rows"):
        return "BROKER_READINESS_BLOCKED", "broker_readiness", "Post-trade signal rows include risk/halt blockers."
    signal_rows = _as_int(signal_summary.get("signal_rows"))
    zero_intent_rows = _as_int(signal_summary.get("zero_intent_signal_rows"))
    if signal_rows and zero_intent_rows == signal_rows and _as_int(intent_summary.get("row_count")) == 0:
        return (
            "HANDOFF_INTENT_NOT_CREATED",
            "signal_to_intent",
            "Post-trade signal rows show SIGNAL_NO_FILL with intent_count=0/fill_count=0, and the latest intent artifact is empty.",
        )
    if signal_rows and not runner_evidence.get("paper_runner_invocation_evidence_found"):
        return "PAPER_RUNNER_NOT_INVOKED", "intent_to_runner", "Signals exist but no paper runner or intent artifact evidence was found."
    if signal_rows and _as_int(intent_summary.get("row_count")) and not _as_int(fill_summary.get("row_count")):
        return "LIFECYCLE_ROUTING_BROKEN", "runner_to_lifecycle", "Intent evidence exists but no fill/lifecycle evidence follows."
    if signal_summary.get("session_review_only_risk"):
        return "SIGNALS_WERE_SESSION_REVIEW_ONLY", "signal_evidence_kind", "Only session-review signal rows were found."
    return "DIAGNOSTIC_INCONCLUSIVE", "unknown", "Available bounded artifacts do not isolate the missing link."


def _managed_lifecycle_assessment(classification: str) -> dict[str, Any]:
    return {
        "current_strategy_managed_lifecycle_v1_relevance": classification
        in {"LIFECYCLE_ROUTING_BROKEN", "PAPER_RUNNER_NOT_INVOKED", "HANDOFF_INTENT_NOT_CREATED"},
        "assessment": "Managed lifecycle v1 fixes proof-vs-managed lifecycle routing for Track B, but this Track 1 audit points one step earlier: legacy signals did not persist handoff intents in the inspected window.",
    }


def _recommended_next_action(classification: str) -> str:
    if classification == "HANDOFF_INTENT_NOT_CREATED":
        return "Inspect legacy signal-to-intent bridge wiring and runtime mode/config around 2026-04-29 08:35 ET before doing deep strategy parity."
    if classification == "PAPER_RUNNER_NOT_INVOKED":
        return "Inspect paper runner scheduling/invocation and launch/runtime config around the breakpoint."
    if classification == "LIFECYCLE_ROUTING_BROKEN":
        return "Inspect lifecycle routing and proof/managed handoff selection around the breakpoint."
    if classification == "BROKER_READINESS_BLOCKED":
        return "Inspect read-only broker readiness/recovery artifacts around the signal timestamps."
    if classification == "HANDOFF_DISABLED_BY_CONFIG":
        return "Inspect paper handoff enablement flags and config snapshot diffs around the breakpoint."
    return "Export direct signal, intent, runner, and lifecycle artifacts for the break window."


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _markdown(report: Mapping[str, Any]) -> str:
    signal = report.get("post_trade_signal_summary") if isinstance(report.get("post_trade_signal_summary"), Mapping) else {}
    lines = [
        "# Track 1 Signal-to-Handoff Breakpoint Audit",
        "",
        f"- Classification: `{report.get('classification')}`",
        f"- Missing link: `{report.get('missing_link')}`",
        f"- Explanation: {report.get('classification_explanation')}",
        f"- Audit window: {report.get('audit_window')}",
        f"- Last known trade path: {(report.get('last_known_trade') or {}).get('path') if isinstance(report.get('last_known_trade'), Mapping) else None}",
        f"- Last known trade detail: {report.get('last_known_trade_detail')}",
        f"- First known signal-without-trade: {report.get('first_known_signal_without_trade')}",
        f"- Post-trade signal rows: {signal.get('signal_rows')}",
        f"- Post-trade SIGNAL_NO_FILL rows: {signal.get('signal_no_fill_rows')}",
        f"- Zero-intent signal rows: {signal.get('zero_intent_signal_rows')}",
        f"- Latest intent artifact rows: {(report.get('handoff_intent_summary') or {}).get('row_count') if isinstance(report.get('handoff_intent_summary'), Mapping) else None}",
        f"- Latest fill artifact rows: {(report.get('fill_summary') or {}).get('row_count') if isinstance(report.get('fill_summary'), Mapping) else None}",
        f"- Suspected commit/config breakpoint: {report.get('suspected_commit_or_config_breakpoint')}",
        "",
        "## Hard Answer",
        f"- Missing chain edge: {report.get('missing_link')}",
        f"- Current managed lifecycle v1 assessment: {report.get('current_strategy_managed_lifecycle_v1_assessment')}",
        f"- Recommended next action: {report.get('recommended_next_action')}",
        "",
        "## Safety",
        "- broker_commands_invoked=false",
        "- paper_proof_cli_invoked=false",
        "- submit_cancel_place_order_invoked=false",
        "- broker_state_mutated=false",
    ]
    return "\n".join(lines) + "\n"
