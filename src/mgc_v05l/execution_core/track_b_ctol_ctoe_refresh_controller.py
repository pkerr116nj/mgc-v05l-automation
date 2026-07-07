"""Read-only CTOL/CTOE refresh controller for analytics maintenance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_core_expectancy_analytics import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CORE_EXPECTANCY_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_core_expectancy_analytics import run_core_expectancy_analytics
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import run_trade_outcome_enrichment
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_CRFD_ROWS,
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    DEFAULT_SIDE_SESSION_REPLAY,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import run_trade_outcome_layer


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "analytics_maintenance" / "ctol_ctoe_refresh"
DEFAULT_CTOE_ROWS = DEFAULT_CTOE_OUTPUT_DIR / "canonical_trade_outcome_enrichment.jsonl"
DEFAULT_CTOL_ROWS = DEFAULT_CTOL_OUTPUT_DIR / "canonical_trade_outcomes.jsonl"
DEFAULT_CTOL_SUMMARY = DEFAULT_CTOL_OUTPUT_DIR / "latest_trade_outcome_summary.json"
DEFAULT_CTOE_SUMMARY = DEFAULT_CTOE_OUTPUT_DIR / "latest_trade_outcome_enrichment_summary.json"

STATUS_JSON = "am1_refresh_status.json"
DIAGNOSIS_JSON = "am1_ctol_ctoe_auto_refresh_diagnosis.json"
DIAGNOSIS_MD = "am1_ctol_ctoe_auto_refresh_diagnosis.md"
CONTRACT_MD = "am1_refresh_controller_contract.md"
VALIDATION_MD = "am1_refresh_validation_report.md"
SCHEDULER_MD = "am1_recommended_scheduler_integration.md"

SCHEMA_VERSION = "track_b_ctol_ctoe_refresh_controller_v1"


@dataclass(frozen=True)
class RefreshControllerResult:
    status: dict[str, Any]
    status_path: Path
    diagnosis_path: Path
    diagnosis_markdown_path: Path
    contract_path: Path
    validation_path: Path
    scheduler_path: Path


def run_ctol_ctoe_refresh_controller(
    *,
    canonical_records_path: Path = DEFAULT_CANONICAL_TRADE_RECORDS,
    side_session_replay_path: Path = DEFAULT_SIDE_SESSION_REPLAY,
    crfd_rows_path: Path = DEFAULT_CRFD_ROWS,
    ctol_output_dir: Path = DEFAULT_CTOL_OUTPUT_DIR,
    ctoe_output_dir: Path = DEFAULT_CTOE_OUTPUT_DIR,
    core_expectancy_output_dir: Path = DEFAULT_CORE_EXPECTANCY_OUTPUT_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    refresh_if_stale: bool = False,
    include_core_expectancy: bool = False,
    force: bool = False,
    now: datetime | str | None = None,
) -> RefreshControllerResult:
    generated_at = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)

    before = build_refresh_diagnosis(
        canonical_records_path=canonical_records_path,
        ctol_rows_path=ctol_output_dir / "canonical_trade_outcomes.jsonl",
        ctoe_rows_path=ctoe_output_dir / "canonical_trade_outcome_enrichment.jsonl",
        generated_at=generated_at,
    )
    actions: list[dict[str, Any]] = []
    ctol_refreshed = False
    ctoe_refreshed = False
    core_refreshed = False

    should_refresh_ctol = force or bool(before["staleness"]["ctol_stale"])
    should_refresh_ctoe = force or bool(before["staleness"]["ctoe_stale"])

    if refresh_if_stale and should_refresh_ctol:
        ctol_result = run_trade_outcome_layer(
            canonical_records_path=canonical_records_path,
            side_session_replay_path=side_session_replay_path,
            crfd_rows_path=crfd_rows_path,
            output_dir=ctol_output_dir,
            now=generated_at,
        )
        ctol_refreshed = True
        actions.append(
            {
                "action": "refresh_ctol",
                "status": "COMPLETE",
                "outcome_count": ctol_result.summary.get("overall", {}).get("outcome_count"),
                "output_path": str(ctol_result.outcomes_path),
            }
        )
    elif should_refresh_ctol:
        actions.append({"action": "refresh_ctol", "status": "SKIPPED", "reason": "refresh_if_stale_false"})
    else:
        actions.append({"action": "refresh_ctol", "status": "NO_OP", "reason": "ctol_current"})

    after_ctol = build_refresh_diagnosis(
        canonical_records_path=canonical_records_path,
        ctol_rows_path=ctol_output_dir / "canonical_trade_outcomes.jsonl",
        ctoe_rows_path=ctoe_output_dir / "canonical_trade_outcome_enrichment.jsonl",
        generated_at=generated_at,
    )
    should_refresh_ctoe_after_ctol = force or ctol_refreshed or bool(after_ctol["staleness"]["ctoe_stale"])
    if refresh_if_stale and should_refresh_ctoe_after_ctol:
        ctoe_result = run_trade_outcome_enrichment(
            outcomes_path=ctol_output_dir / "canonical_trade_outcomes.jsonl",
            output_dir=ctoe_output_dir,
            now=generated_at,
        )
        ctoe_refreshed = True
        actions.append(
            {
                "action": "refresh_ctoe",
                "status": "COMPLETE",
                "enrichment_count": ctoe_result.summary.get("overall", {}).get("enrichment_count"),
                "output_path": str(ctoe_result.enrichment_path),
            }
        )
    elif should_refresh_ctoe_after_ctol:
        actions.append({"action": "refresh_ctoe", "status": "SKIPPED", "reason": "refresh_if_stale_false"})
    else:
        actions.append({"action": "refresh_ctoe", "status": "NO_OP", "reason": "ctoe_current"})

    if include_core_expectancy and refresh_if_stale and (ctoe_refreshed or force):
        core_result = run_core_expectancy_analytics(
            outcomes_path=ctol_output_dir / "canonical_trade_outcomes.jsonl",
            enrichments_path=ctoe_output_dir / "canonical_trade_outcome_enrichment.jsonl",
            enrichment_summary_path=ctoe_output_dir / "latest_trade_outcome_enrichment_summary.json",
            output_dir=core_expectancy_output_dir,
            now=generated_at,
        )
        core_refreshed = True
        actions.append(
            {
                "action": "refresh_core_expectancy",
                "status": "COMPLETE",
                "outcome_count": core_result.analytics.get("overall", {}).get("count"),
                "output_path": str(core_result.analytics_json_path),
            }
        )
    elif include_core_expectancy:
        actions.append({"action": "refresh_core_expectancy", "status": "NO_OP", "reason": "ctoe_not_refreshed"})

    after = build_refresh_diagnosis(
        canonical_records_path=canonical_records_path,
        ctol_rows_path=ctol_output_dir / "canonical_trade_outcomes.jsonl",
        ctoe_rows_path=ctoe_output_dir / "canonical_trade_outcome_enrichment.jsonl",
        generated_at=generated_at,
    )
    status = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "refresh_if_stale": refresh_if_stale,
        "include_core_expectancy": include_core_expectancy,
        "force": force,
        "before": before,
        "after": after,
        "actions": actions,
        "refreshed": {
            "ctol": ctol_refreshed,
            "ctoe": ctoe_refreshed,
            "core_expectancy": core_refreshed,
        },
        "classification": _classification(after, refresh_if_stale=refresh_if_stale, actions=actions),
        "safety": _safety_contract(),
    }
    status["deterministic_fingerprint"] = _fingerprint(
        {key: value for key, value in status.items() if key not in {"generated_at", "deterministic_fingerprint"}}
    )

    status_path = output_dir / STATUS_JSON
    diagnosis_path = output_dir / DIAGNOSIS_JSON
    diagnosis_md_path = output_dir / DIAGNOSIS_MD
    contract_path = output_dir / CONTRACT_MD
    validation_path = output_dir / VALIDATION_MD
    scheduler_path = output_dir / SCHEDULER_MD
    _write_json(status_path, status)
    _write_json(diagnosis_path, after)
    diagnosis_md_path.write_text(render_diagnosis_markdown(status), encoding="utf-8")
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    validation_path.write_text(render_validation_markdown(status), encoding="utf-8")
    scheduler_path.write_text(render_scheduler_markdown(), encoding="utf-8")
    return RefreshControllerResult(
        status=status,
        status_path=status_path,
        diagnosis_path=diagnosis_path,
        diagnosis_markdown_path=diagnosis_md_path,
        contract_path=contract_path,
        validation_path=validation_path,
        scheduler_path=scheduler_path,
    )


def build_refresh_diagnosis(
    *,
    canonical_records_path: Path,
    ctol_rows_path: Path,
    ctoe_rows_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    canonical_rows = _read_jsonl(canonical_records_path)
    completed_rows = [row for row in canonical_rows if _is_completed_paired_record(row)]
    open_rows = [row for row in canonical_rows if not _is_completed_paired_record(row)]
    ctol_rows = _read_jsonl(ctol_rows_path)
    ctoe_rows = _read_jsonl(ctoe_rows_path)

    canonical_state = _state_from_rows(canonical_records_path, completed_rows, timestamp_field="exit_time")
    ctol_state = _state_from_rows(ctol_rows_path, ctol_rows, timestamp_field="exit_time")
    ctoe_state = _state_from_rows(ctoe_rows_path, ctoe_rows, timestamp_field="exit_time")
    ctol_stale_reasons = _ctol_stale_reasons(canonical_state, ctol_state, expected_count=len(completed_rows))
    ctoe_stale_reasons = _ctoe_stale_reasons(ctol_state, ctoe_state, source_state=canonical_state, expected_source_count=len(completed_rows))
    return {
        "schema_version": "am1_ctol_ctoe_auto_refresh_diagnosis_v1",
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "writers": {
            "canonical_trade_records": "track_b_strategy_performance_attachment / track_b_post_trade_analytics_refresh",
            "ctol": "mgc_v05l.app.track_b_trade_outcome_layer",
            "ctoe": "mgc_v05l.app.track_b_trade_outcome_enrichment",
            "core_expectancy": "mgc_v05l.app.track_b_core_expectancy_analytics",
            "morning_brief": "mgc_v05l.app.track_b_canonical_morning_brief",
        },
        "automation_assessment": {
            "ctol_ctoe_existing_automation": "manual_cli_or_workflow_only",
            "missing_automatic_link": "post_trade_analytics_refresh_updates_canonical_records_but_does_not_trigger_ctol_ctoe",
            "recommended_trigger": "canonical_trade_records_completed_count_or_max_exit_timestamp_advanced",
        },
        "source_state": {
            "canonical_trade_records": canonical_state,
            "completed_paired_record_count": len(completed_rows),
            "open_or_unpaired_record_count": len(open_rows),
            "active_open_positions_excluded": len(open_rows),
        },
        "artifact_state": {
            "ctol": ctol_state,
            "ctoe": ctoe_state,
        },
        "staleness": {
            "ctol_stale": bool(ctol_stale_reasons),
            "ctol_stale_reasons": ctol_stale_reasons,
            "ctoe_stale": bool(ctoe_stale_reasons),
            "ctoe_stale_reasons": ctoe_stale_reasons,
        },
        "risks": {
            "runtime_performance": "avoid_runtime_path; run as research maintenance CLI or scheduled capture hook",
            "partial_active_trades": "safe; CTOL filters to PAIRED/CLOSED records",
            "stale_upstream": "controller reports source/artifact timestamps and no-ops when current",
            "output_churn": "only writes when stale unless forced",
            "concurrent_writers": "scheduler should avoid overlapping post_trade refresh and CTOL/CTOE refresh",
        },
        "safety": _safety_contract(),
    }


def render_diagnosis_markdown(status: Mapping[str, Any]) -> str:
    after = status.get("after") or {}
    source = after.get("source_state", {}).get("canonical_trade_records", {})
    ctol = after.get("artifact_state", {}).get("ctol", {})
    ctoe = after.get("artifact_state", {}).get("ctoe", {})
    stale = after.get("staleness", {})
    lines = [
        "# AM1 CTOL/CTOE Auto Refresh Diagnosis",
        "",
        f"- Generated at: `{status.get('generated_at')}`",
        f"- Classification: `{status.get('classification')}`",
        f"- Canonical completed records: `{after.get('source_state', {}).get('completed_paired_record_count')}`",
        f"- Canonical max exit: `{source.get('max_timestamp')}`",
        f"- CTOL rows: `{ctol.get('row_count')}`, max exit `{ctol.get('max_timestamp')}`",
        f"- CTOE rows: `{ctoe.get('row_count')}`, max exit `{ctoe.get('max_timestamp')}`",
        f"- CTOL stale: `{stale.get('ctol_stale')}` {stale.get('ctol_stale_reasons')}",
        f"- CTOE stale: `{stale.get('ctoe_stale')}` {stale.get('ctoe_stale_reasons')}",
        "",
        "## Diagnosis",
        "",
        "Canonical trade records are produced by the post-trade analytics/strategy-performance path. CTOL, CTOE, core expectancy, and Morning Brief artifacts are separate analytics/reporting surfaces and were not automatically chained after canonical records advanced.",
        "",
        "## Actions",
        "",
    ]
    for action in status.get("actions", []):
        lines.append(f"- `{action.get('action')}`: `{action.get('status')}` {action.get('reason') or ''}")
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- No broker actions.",
            "- No order actions.",
            "- No runtime or Managed Exit restart.",
            "- No strategy changes or gates.",
            "",
        ]
    )
    return "\n".join(lines)


def render_contract_markdown() -> str:
    return "\n".join(
        [
            "# AM1 Refresh Controller Contract",
            "",
            "The CTOL/CTOE refresh controller is an analytics-maintenance component.",
            "",
            "## Trigger",
            "- Completed canonical trade record count changes.",
            "- Completed canonical trade record max exit timestamp advances.",
            "- CTOL count or max exit lags canonical completed records.",
            "- CTOE count or max exit lags CTOL.",
            "",
            "## Refresh Order",
            "1. CTOL from canonical trade records.",
            "2. CTOE from refreshed CTOL.",
            "3. Optional core expectancy analytics from refreshed CTOL/CTOE.",
            "",
            "## Guardrails",
            "- No broker imports or calls.",
            "- No order submit/cancel/modify/close/flatten authority.",
            "- No runtime or Managed Exit restart.",
            "- No strategy changes or trading gates.",
            "- Open/unpaired records remain excluded until paired closed records exist.",
            "",
        ]
    )


def render_validation_markdown(status: Mapping[str, Any]) -> str:
    after = status.get("after") or {}
    stale = after.get("staleness", {})
    return "\n".join(
        [
            "# AM1 Refresh Validation Report",
            "",
            f"- Classification: `{status.get('classification')}`",
            f"- CTOL stale after run: `{stale.get('ctol_stale')}`",
            f"- CTOE stale after run: `{stale.get('ctoe_stale')}`",
            f"- CTOL refreshed: `{status.get('refreshed', {}).get('ctol')}`",
            f"- CTOE refreshed: `{status.get('refreshed', {}).get('ctoe')}`",
            f"- Core expectancy refreshed: `{status.get('refreshed', {}).get('core_expectancy')}`",
            "- JSON parse: covered by validation command.",
            "- Import boundary: covered by focused tests.",
            "",
        ]
    )


def render_scheduler_markdown() -> str:
    return "\n".join(
        [
            "# AM1 Recommended Scheduler Integration",
            "",
            "Preferred integration is outside the runtime trading loop.",
            "",
            "## Recommended",
            "- Add as a research-maintenance CLI after post-trade analytics refresh.",
            "- Or add as a Research Workflow / Morning Brief preparatory step.",
            "- Or run from the existing research daily capture cadence after canonical trade records are updated.",
            "",
            "## Avoid",
            "- Runtime trading loop.",
            "- Broker/order/Managed Exit paths.",
            "- Strategy signal generation.",
            "",
            "## Command",
            "```bash",
            "PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.track_b_ctol_ctoe_refresh_controller --refresh-if-stale --include-core-expectancy",
            "```",
            "",
        ]
    )


def _classification(after: Mapping[str, Any], *, refresh_if_stale: bool, actions: Sequence[Mapping[str, Any]]) -> str:
    stale = after.get("staleness") or {}
    if stale.get("ctol_stale") or stale.get("ctoe_stale"):
        if refresh_if_stale:
            return "REFRESH_INCOMPLETE"
        return "STALE_REFRESH_AVAILABLE"
    if any(action.get("status") == "COMPLETE" for action in actions):
        return "REFRESHED_CURRENT"
    return "CURRENT_NO_OP"


def _ctol_stale_reasons(source: Mapping[str, Any], ctol: Mapping[str, Any], *, expected_count: int) -> list[str]:
    reasons: list[str] = []
    if not ctol.get("exists"):
        reasons.append("ctol_missing")
    if expected_count > int(ctol.get("row_count") or 0):
        reasons.append("ctol_row_count_lags_completed_canonical_records")
    if _timestamp_gt(source.get("max_timestamp"), ctol.get("max_timestamp")):
        reasons.append("ctol_max_exit_lags_canonical_records")
    return reasons


def _ctoe_stale_reasons(
    ctol: Mapping[str, Any],
    ctoe: Mapping[str, Any],
    *,
    source_state: Mapping[str, Any],
    expected_source_count: int,
) -> list[str]:
    reasons: list[str] = []
    if not ctoe.get("exists"):
        reasons.append("ctoe_missing")
    if int(ctol.get("row_count") or 0) > int(ctoe.get("row_count") or 0):
        reasons.append("ctoe_row_count_lags_ctol")
    if _timestamp_gt(ctol.get("max_timestamp"), ctoe.get("max_timestamp")):
        reasons.append("ctoe_max_exit_lags_ctol")
    if expected_source_count > int(ctoe.get("row_count") or 0):
        reasons.append("ctoe_row_count_lags_completed_canonical_records")
    if _timestamp_gt(source_state.get("max_timestamp"), ctoe.get("max_timestamp")):
        reasons.append("ctoe_max_exit_lags_canonical_records")
    return reasons


def _state_from_rows(path: Path, rows: Sequence[Mapping[str, Any]], *, timestamp_field: str) -> dict[str, Any]:
    timestamps = [_parse_datetime(row.get(timestamp_field)) for row in rows]
    timestamps = [ts for ts in timestamps if ts is not None]
    return {
        "path": str(path),
        "exists": path.exists(),
        "row_count": len(rows),
        "min_timestamp": min(timestamps).isoformat() if timestamps else None,
        "max_timestamp": max(timestamps).isoformat() if timestamps else None,
        "fingerprint": _rows_fingerprint(rows),
        "mtime": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat() if path.exists() else None,
    }


def _rows_fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = [
        {
            "trade_id": row.get("trade_id") or row.get("trade_outcome_id"),
            "entry_time": row.get("entry_time"),
            "exit_time": row.get("exit_time"),
            "pairing_status": row.get("pairing_status"),
            "trade_status": row.get("trade_status"),
            "instrument": row.get("symbol") or row.get("instrument"),
            "contract": row.get("local_symbol") or row.get("contract"),
        }
        for row in rows
    ]
    return _fingerprint(payload)


def _is_completed_paired_record(row: Mapping[str, Any]) -> bool:
    return row.get("pairing_status") == "PAIRED" and row.get("trade_status") == "CLOSED"


def _timestamp_gt(left: Any, right: Any) -> bool:
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None:
        return False
    if right_dt is None:
        return True
    return left_dt > right_dt


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return datetime.now(UTC)


def _fingerprint(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _safety_contract() -> dict[str, bool]:
    return {
        "broker_actions": False,
        "order_actions": False,
        "position_changes": False,
        "global_cancel": False,
        "runtime_restart": False,
        "managed_exit_restart": False,
        "strategy_changes": False,
        "trading_gates": False,
        "databento_download": False,
    }


__all__ = [
    "RefreshControllerResult",
    "build_refresh_diagnosis",
    "run_ctol_ctoe_refresh_controller",
]
