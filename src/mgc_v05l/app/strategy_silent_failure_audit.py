"""Silent-failure audit across the current paper/runtime strategy set."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "strategy_silent_failure_audit"
DEFAULT_AUDIT_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
DEFAULT_OPERATOR_STATUS_PATH = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
DEFAULT_PAPER_CONFIG_PATH = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"


def run_strategy_silent_failure_audit(
    *,
    output_dir: str | Path | None = None,
    audit_snapshot_path: str | Path | None = None,
    operator_status_path: str | Path | None = None,
    paper_config_path: str | Path | None = None,
) -> dict[str, Path]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    audit_snapshot = json.loads(Path(audit_snapshot_path or DEFAULT_AUDIT_SNAPSHOT_PATH).read_text(encoding="utf-8"))
    operator_status = json.loads(Path(operator_status_path or DEFAULT_OPERATOR_STATUS_PATH).read_text(encoding="utf-8"))
    paper_config = json.loads(Path(paper_config_path or DEFAULT_PAPER_CONFIG_PATH).read_text(encoding="utf-8"))
    audit_rows = list(audit_snapshot.get("rows", []))
    db_rows = _scan_probationary_databases()

    no_usable_runtime_state = [
        {
            "lane_id": row["id"],
            "family": row.get("family"),
            "instrument": row.get("instrument"),
            "runtime_kind": row.get("runtime_kind"),
            "config_source": row.get("config_source"),
            "runtime_instance_present": row.get("runtime_instance_present"),
            "runtime_state_loaded": row.get("runtime_state_loaded"),
            "processed_bar_count": row.get("processed_bar_count"),
            "audit_reason": row.get("audit_reason"),
        }
        for row in audit_rows
        if not row.get("runtime_state_loaded") and not (row.get("processed_bar_count") or 0)
    ]

    materialization_failures = [
        {
            "database_name": row["database_name"],
            "lane_guess": row["lane_guess"],
            "counts": row["counts"],
        }
        for row in db_rows
        if not (row["counts"].get("processed_bars") or 0)
    ]

    wrong_session_rows = [
        {
            "lane_id": row.get("lane_id"),
            "display_name": row.get("display_name"),
            "symbol": row.get("symbol"),
            "family": _lane_family(row),
            "session_restriction": row.get("session_restriction"),
            "current_detected_session": row.get("current_detected_session"),
            "eligibility_reason": row.get("eligibility_reason"),
        }
        for row in operator_status.get("lanes", [])
        if row.get("eligibility_reason") == "wrong_session"
    ]
    wrong_session_grouped = _group_wrong_session_rows(wrong_session_rows)

    eligible_no_raw_setup = [
        {
            "lane_id": row["id"],
            "family": row.get("family"),
            "instrument": row.get("instrument"),
            "eligible_now": row.get("eligible_now"),
            "processed_bar_count": row.get("processed_bar_count"),
            "raw_setup_candidate_count": row.get("raw_setup_candidate_count"),
            "audit_verdict": row.get("audit_verdict"),
            "audit_reason": row.get("audit_reason"),
        }
        for row in audit_rows
        if row.get("eligible_now") and (row.get("raw_setup_candidate_count") or 0) == 0
    ]

    ranked_blockers = _rank_blockers(
        no_usable_runtime_state=no_usable_runtime_state,
        materialization_failures=materialization_failures,
        wrong_session_rows=wrong_session_rows,
        eligible_no_raw_setup=eligible_no_raw_setup,
    )

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope": "current_probationary_paper_runtime_and_strategy_audit_surfaces",
        "paper_config_lane_count": len(paper_config.get("lanes", [])),
        "audit_snapshot_row_count": len(audit_rows),
        "probationary_database_count": len(db_rows),
        "lanes_with_no_usable_runtime_state": no_usable_runtime_state,
        "lanes_with_processed_bar_or_materialization_failures": materialization_failures,
        "repeated_wrong_session_gating": {
            "rows": wrong_session_rows,
            "grouped": wrong_session_grouped,
        },
        "eligible_but_never_form_raw_setup_candidates": eligible_no_raw_setup,
        "ranked_blockers": ranked_blockers,
    }

    json_path = resolved_output_dir / "strategy_silent_failure_audit.json"
    markdown_path = resolved_output_dir / "strategy_silent_failure_audit.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _scan_probationary_databases() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(REPO_ROOT.glob("mgc_v05l.probationary.paper__*.sqlite3")):
        connection = sqlite3.connect(path)
        try:
            counts: dict[str, int] = {}
            for table in (
                "bars",
                "processed_bars",
                "features",
                "signals",
                "order_intents",
                "fills",
                "strategy_state_snapshots",
            ):
                counts[table] = connection.execute(f"select count(*) from {table}").fetchone()[0]
        finally:
            connection.close()
        rows.append(
            {
                "database_name": path.name,
                "lane_guess": path.stem.removeprefix("mgc_v05l.probationary.paper__"),
                "counts": counts,
            }
        )
    return rows


def _lane_family(row: dict[str, Any]) -> str:
    long_sources = row.get("approved_long_entry_sources") or []
    short_sources = row.get("approved_short_entry_sources") or []
    if long_sources:
        return str(long_sources[0])
    if short_sources:
        return str(short_sources[0])
    return "UNKNOWN"


def _group_wrong_session_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row.get("family") or "UNKNOWN"),
                str(row.get("session_restriction") or "ANY"),
                str(row.get("current_detected_session") or "UNKNOWN"),
            )
        ].append(row)
    rendered: list[dict[str, Any]] = []
    for (family, restriction, current_session), items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        rendered.append(
            {
                "family": family,
                "session_restriction": restriction,
                "current_detected_session": current_session,
                "count": len(items),
                "lane_ids": [item["lane_id"] for item in items],
                "symbols": sorted({str(item.get("symbol") or "") for item in items}),
            }
        )
    return rendered


def _rank_blockers(
    *,
    no_usable_runtime_state: list[dict[str, Any]],
    materialization_failures: list[dict[str, Any]],
    wrong_session_rows: list[dict[str, Any]],
    eligible_no_raw_setup: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ranked = [
        {
            "rank": 1,
            "blocker": "no_usable_runtime_state_or_zero_materialization",
            "count": len(no_usable_runtime_state),
            "likely_importance": "highest",
            "reason": (
                "These lanes never became auditable runtime participants. They look eligible on paper but produced no processed bars, "
                "features, signals, intents, or state snapshots in the current session."
            ),
            "examples": [row["lane_id"] for row in no_usable_runtime_state[:5]],
        },
        {
            "rank": 2,
            "blocker": "repeated_wrong_session_gating",
            "count": len(wrong_session_rows),
            "likely_importance": "high",
            "reason": (
                "The configured paper runtime is repeatedly loading lanes that are structurally out of session. "
                "That suppresses setup formation before any action or ambition logic can matter."
            ),
            "examples": [row["lane_id"] for row in wrong_session_rows[:5]],
        },
        {
            "rank": 3,
            "blocker": "eligible_but_no_raw_setup_candidates",
            "count": len(eligible_no_raw_setup),
            "likely_importance": "medium",
            "reason": (
                "These lanes were marked eligible or near-eligible in the audit surface but still produced zero raw setup candidates, "
                "which points to either upstream session mismatch or structure rules that are too narrow for the active habitat."
            ),
            "examples": [row["lane_id"] for row in eligible_no_raw_setup[:5]],
        },
        {
            "rank": 4,
            "blocker": "persisted_lane_databases_with_zero_processed_bars",
            "count": len(materialization_failures),
            "likely_importance": "medium",
            "reason": (
                "These persisted SQLite lane databases exist on disk but are still empty. That makes silent failure easy to miss because the file presence "
                "looks healthy while the strategy never actually progressed through processing."
            ),
            "examples": [row["lane_guess"] for row in materialization_failures[:5]],
        },
    ]
    return ranked


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Strategy Silent-Failure Audit",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Audit snapshot rows: `{report['audit_snapshot_row_count']}`",
        f"- Current paper-config lanes: `{report['paper_config_lane_count']}`",
        f"- Probationary SQLite DBs scanned: `{report['probationary_database_count']}`",
        "",
        "## Ranked Blockers",
        "",
    ]
    for row in report["ranked_blockers"]:
        lines.append(
            f"- #{row['rank']} `{row['blocker']}` count={row['count']} importance={row['likely_importance']}: {row['reason']}"
        )
    lines.extend(["", "## Wrong Session Gating", ""])
    for row in report["repeated_wrong_session_gating"]["grouped"]:
        lines.append(
            f"- family=`{row['family']}` restriction=`{row['session_restriction']}` current=`{row['current_detected_session']}` count={row['count']} lanes={','.join(row['lane_ids'])}"
        )
    lines.extend(["", "## No Usable Runtime State", ""])
    for row in report["lanes_with_no_usable_runtime_state"]:
        lines.append(
            f"- `{row['lane_id']}` family=`{row['family']}` instrument=`{row['instrument']}` runtime_kind=`{row['runtime_kind']}`"
        )
    return "\n".join(lines)
