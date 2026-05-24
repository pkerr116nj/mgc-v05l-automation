"""Read-only Track B PAPER runtime-generation audit viewer.

This module reconstructs one runtime_generation_id from execution_core authority
artifacts and append-only audit/event logs. It is reporting-only: no runtime,
broker, order, lifecycle, archive, or proof action is executed.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


RUNTIME_GENERATION_AUDIT_READY = "RUNTIME_GENERATION_AUDIT_READY"
RUNTIME_GENERATION_AUDIT_PARTIAL = "RUNTIME_GENERATION_AUDIT_PARTIAL"
RUNTIME_GENERATION_AUDIT_MISSING_GENERATION = "RUNTIME_GENERATION_AUDIT_MISSING_GENERATION"
RUNTIME_GENERATION_AUDIT_INCONSISTENT = "RUNTIME_GENERATION_AUDIT_INCONSISTENT"
RUNTIME_GENERATION_AUDIT_NO_EVENTS = "RUNTIME_GENERATION_AUDIT_NO_EVENTS"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_GENERATION_AUDIT_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_generation_audit"
    / "latest_runtime_generation_audit.json"
)
DEFAULT_RUNTIME_GENERATION_AUDIT_MARKDOWN = (
    Path("outputs") / "reports" / "runtime_generation_audit" / "latest_runtime_generation_audit.md"
)
DEFAULT_CONTROL_PLANE_DIR = Path("outputs") / "track_b_execution_core" / "control_plane"
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = DEFAULT_CONTROL_PLANE_DIR / "latest_control_plane_snapshot.json"
DEFAULT_RUNTIME_SUPERVISOR_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json"
)
DEFAULT_RUNTIME_RESUME_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json"
)
DEFAULT_SAFE_STATE_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
)
DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_recovery_attempt_history.json"
)
DEFAULT_EXECUTOR_EVENT_LOG = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "executor_attempts"
    / "paper_autonomous_recovery_executor_events.jsonl"
)
DEFAULT_LATEST_EXECUTOR_REPORT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "executor_attempts"
    / "latest_paper_autonomous_recovery_executor_attempt.json"
)
DEFAULT_RECOVERY_BUDGET_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "recovery_budget" / "recovery_budget_events.jsonl"
)
DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json"
)
DEFAULT_STRATEGY_BRIDGE_REPORT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "strategy_bridge" / "latest_strategy_bridge_submit_report.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_LEDGER_SUMMARY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)
DEFAULT_RUNTIME_LOG_PATHS = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "headless_supervised_paper.log",
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_soak.log",
)


@dataclass(frozen=True)
class TrackBRuntimeGenerationAuditViewerConfig:
    repo_root: Path = REPO_ROOT
    runtime_generation_id: str | None = None
    output_path: Path = DEFAULT_RUNTIME_GENERATION_AUDIT_ARTIFACT
    markdown_report_path: Path | None = DEFAULT_RUNTIME_GENERATION_AUDIT_MARKDOWN
    control_plane_dir: Path = DEFAULT_CONTROL_PLANE_DIR
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    runtime_supervisor_path: Path = DEFAULT_RUNTIME_SUPERVISOR_ARTIFACT
    runtime_resume_path: Path = DEFAULT_RUNTIME_RESUME_ARTIFACT
    safe_state_path: Path = DEFAULT_SAFE_STATE_ARTIFACT
    recovery_attempt_history_path: Path = DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT
    executor_event_log_path: Path = DEFAULT_EXECUTOR_EVENT_LOG
    latest_executor_report_path: Path = DEFAULT_LATEST_EXECUTOR_REPORT
    recovery_budget_events_path: Path = DEFAULT_RECOVERY_BUDGET_EVENTS
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    strategy_bridge_report_path: Path = DEFAULT_STRATEGY_BRIDGE_REPORT_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    ledger_summary_path: Path = DEFAULT_LEDGER_SUMMARY_ARTIFACT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    runtime_log_paths: tuple[Path, ...] = DEFAULT_RUNTIME_LOG_PATHS

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_runtime_generation_audit(
    *,
    config: TrackBRuntimeGenerationAuditViewerConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = _load_inputs(config)
    generation_id = _resolve_generation_id(config=config, inputs=inputs)
    if not generation_id:
        return _base_payload(
            config=config,
            now=actual_now,
            runtime_generation_id="",
            classification=RUNTIME_GENERATION_AUDIT_MISSING_GENERATION,
            operator_summary="No runtime_generation_id was provided or inferable from authority artifacts.",
            inputs=inputs,
        )

    control_chain = _control_plane_decision_chain(inputs=inputs, runtime_generation_id=generation_id)
    strategy_intents = _strategy_intents(inputs=inputs, runtime_generation_id=generation_id)
    broker_mutations = _broker_mutations_observed(inputs=inputs, runtime_generation_id=generation_id)
    lifecycle_events = _lifecycle_events(inputs=inputs, runtime_generation_id=generation_id)
    recovery_events = _recovery_events(inputs=inputs, runtime_generation_id=generation_id)
    safe_state_transitions = _safe_state_transitions(inputs=inputs, runtime_generation_id=generation_id)
    runtime_log_excerpt = _runtime_log_excerpt(config=config, runtime_generation_id=generation_id)
    source_commit = _source_commit(inputs=inputs, control_chain=control_chain)
    generation_start, generation_end = _generation_bounds(
        control_chain=control_chain,
        strategy_intents=strategy_intents,
        broker_mutations=broker_mutations,
        lifecycle_events=lifecycle_events,
        recovery_events=recovery_events,
        safe_state_transitions=safe_state_transitions,
    )
    final_state = _final_state(
        inputs=inputs,
        control_chain=control_chain,
        safe_state_transitions=safe_state_transitions,
        runtime_generation_id=generation_id,
    )
    unresolved_questions = _unresolved_questions(inputs=inputs, control_chain=control_chain, generation_id=generation_id)
    classification = _audit_classification(
        control_chain=control_chain,
        strategy_intents=strategy_intents,
        broker_mutations=broker_mutations,
        lifecycle_events=lifecycle_events,
        recovery_events=recovery_events,
        safe_state_transitions=safe_state_transitions,
        unresolved_questions=unresolved_questions,
    )
    payload = _base_payload(
        config=config,
        now=actual_now,
        runtime_generation_id=generation_id,
        classification=classification,
        operator_summary=_operator_summary(
            classification=classification,
            runtime_generation_id=generation_id,
            final_state=final_state,
            unresolved_questions=unresolved_questions,
        ),
        inputs=inputs,
    )
    payload.update(
        {
            "generation_start": generation_start,
            "generation_end": generation_end,
            "source_commit": source_commit,
            "control_plane_decision_chain": control_chain,
            "strategy_intents": strategy_intents,
            "broker_mutations_observed": broker_mutations,
            "lifecycle_events": lifecycle_events,
            "recovery_events": recovery_events,
            "safe_state_transitions": safe_state_transitions,
            "runtime_log_excerpt": runtime_log_excerpt,
            "final_state": final_state,
            "unresolved_questions": unresolved_questions,
        }
    )
    return payload


def write_track_b_runtime_generation_audit(
    *,
    config: TrackBRuntimeGenerationAuditViewerConfig,
    payload: Mapping[str, Any],
) -> tuple[Path, Path | None]:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, dict(payload))
    markdown_path: Path | None = None
    if config.markdown_report_path is not None:
        markdown_path = config.resolve(config.markdown_report_path)
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(_markdown_report(payload), encoding="utf-8")
    return output_path, markdown_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a read-only Track B PAPER runtime-generation audit view.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--runtime-generation-id", default=None)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RUNTIME_GENERATION_AUDIT_ARTIFACT)
    parser.add_argument("--markdown-report-path", type=Path, default=DEFAULT_RUNTIME_GENERATION_AUDIT_MARKDOWN)
    parser.add_argument("--no-markdown-report", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRuntimeGenerationAuditViewerConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        runtime_generation_id=args.runtime_generation_id,
        output_path=Path(args.output_path),
        markdown_report_path=None if bool(args.no_markdown_report) else Path(args.markdown_report_path),
    )
    payload = build_track_b_runtime_generation_audit(config=config)
    output_path, markdown_path = write_track_b_runtime_generation_audit(config=config, payload=payload)
    summary = {
        "classification": payload.get("classification"),
        "runtime_generation_id": payload.get("runtime_generation_id"),
        "generation_start": payload.get("generation_start"),
        "generation_end": payload.get("generation_end"),
        "final_state": payload.get("final_state"),
        "unresolved_questions": payload.get("unresolved_questions"),
        "operator_summary": payload.get("operator_summary"),
        "authority_path": str(output_path),
        "markdown_report_path": str(markdown_path) if markdown_path else None,
        "read_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("classification") in {RUNTIME_GENERATION_AUDIT_READY, RUNTIME_GENERATION_AUDIT_NO_EVENTS} else 2


def _base_payload(
    *,
    config: TrackBRuntimeGenerationAuditViewerConfig,
    now: datetime,
    runtime_generation_id: str,
    classification: str,
    operator_summary: str,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_runtime_generation_audit_viewer_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "reporting_only": True,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "submit_authority": False,
        "cancel_authority": False,
        "replace_authority": False,
        "modify_authority": False,
        "flatten_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": _any_live_money(inputs),
        "dashboard_projection_consumed": False,
        "not_routing_authority": True,
        "classification": classification,
        "runtime_generation_id": runtime_generation_id,
        "generation_start": None,
        "generation_end": None,
        "source_commit": "",
        "control_plane_decision_chain": [],
        "strategy_intents": [],
        "broker_mutations_observed": [],
        "lifecycle_events": [],
        "recovery_events": [],
        "safe_state_transitions": [],
        "runtime_log_excerpt": [],
        "final_state": {},
        "unresolved_questions": [],
        "operator_summary": operator_summary,
        "source_artifact_paths": _source_artifact_paths(config),
    }


def _load_inputs(config: TrackBRuntimeGenerationAuditViewerConfig) -> dict[str, Any]:
    control_plane_snapshots = _read_control_plane_snapshots(config)
    return {
        "control_plane_snapshots": control_plane_snapshots,
        "runtime_supervisor": _read_json(config.resolve(config.runtime_supervisor_path)),
        "runtime_resume": _read_json(config.resolve(config.runtime_resume_path)),
        "safe_state": _read_json(config.resolve(config.safe_state_path)),
        "recovery_attempt_history": _read_json(config.resolve(config.recovery_attempt_history_path)),
        "executor_events": _read_jsonl(config.resolve(config.executor_event_log_path)),
        "latest_executor_report": _read_json(config.resolve(config.latest_executor_report_path)),
        "budget_events": _read_jsonl(config.resolve(config.recovery_budget_events_path)),
        "recovery_budget_ledger": _read_json(config.resolve(config.recovery_budget_ledger_path)),
        "strategy_bridge_report": _read_json(config.resolve(config.strategy_bridge_report_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "ledger_summary": _read_json(config.resolve(config.ledger_summary_path)),
        "lifecycle_reports": _read_lifecycle_reports(config.resolve(config.lifecycle_root)),
    }


def _read_control_plane_snapshots(config: TrackBRuntimeGenerationAuditViewerConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    latest = _read_json(config.resolve(config.control_plane_snapshot_path))
    if latest:
        rows.append(latest)
    control_dir = config.resolve(config.control_plane_dir)
    try:
        paths = sorted(control_dir.glob("*.json"))
    except OSError:
        paths = []
    latest_path = config.resolve(config.control_plane_snapshot_path)
    for path in paths:
        if path == latest_path or "operator_dashboard" in str(path):
            continue
        payload = _read_json(path)
        if payload:
            rows.append(payload)
    return _unique_payloads(rows, key="control_plane_snapshot_id")


def _resolve_generation_id(*, config: TrackBRuntimeGenerationAuditViewerConfig, inputs: Mapping[str, Any]) -> str:
    if config.runtime_generation_id:
        return str(config.runtime_generation_id).strip()
    for snapshot in _list(inputs.get("control_plane_snapshots")):
        generation = _first_text(
            _mapping(snapshot).get("runtime_resume_proposed_next_runtime_generation_id"),
            _mapping(snapshot).get("safe_state_runtime_generation_id"),
            _mapping(snapshot).get("runtime_generation_id"),
        )
        if generation:
            return generation
    for key in ("safe_state", "runtime_resume", "latest_executor_report"):
        payload = _mapping(inputs.get(key))
        generation = _first_text(
            payload.get("runtime_generation_id"),
            payload.get("proposed_next_runtime_generation_id"),
            payload.get("previous_runtime_generation_id"),
        )
        if generation:
            return generation
    return ""


def _control_plane_decision_chain(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in _list(inputs.get("control_plane_snapshots")):
        payload = _mapping(snapshot)
        if not _matches_generation(payload, runtime_generation_id):
            continue
        rows.append(
            {
                "generated_at": payload.get("generated_at"),
                "control_plane_snapshot_id": payload.get("control_plane_snapshot_id") or "",
                "shared_truth_generation_id": payload.get("shared_truth_refresh_generation_id") or "",
                "runtime_supervisor_classification": payload.get("runtime_supervisor_classification") or "",
                "supervisor_mode": payload.get("supervisor_mode") or "",
                "runtime_resume_action_policy": payload.get("runtime_resume_action_policy") or "",
                "safe_state_classification": payload.get("safe_state_classification") or "",
                "safe_to_start_runtime": payload.get("safe_to_start_runtime") is True,
                "top_line_classification": payload.get("top_line_classification") or "",
                "operator_explanation": payload.get("operator_explanation") or "",
            }
        )
    return _sort_by_time(rows)


def _strategy_intents(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    report = _mapping(inputs.get("strategy_bridge_report"))
    source_rows = (
        _list(report.get("strategy_intents"))
        + _list(report.get("submit_intents"))
        + _list(report.get("recent_submits"))
        + _list(report.get("submit_attempts"))
    )
    rows = []
    for row in source_rows:
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(
                {
                    "generated_at": payload.get("generated_at") or payload.get("created_at") or payload.get("occurred_at"),
                    "intent_id": payload.get("intent_id") or payload.get("submit_intent_id") or "",
                    "strategy_id": payload.get("strategy_id") or payload.get("lane_id") or "",
                    "symbol": payload.get("symbol") or payload.get("contract_symbol") or "",
                    "action": payload.get("action") or payload.get("side") or payload.get("action_type") or "",
                    "quantity": payload.get("quantity") or payload.get("qty"),
                    "classification": payload.get("classification") or payload.get("status") or "",
                }
            )
    if not rows and report and _matches_generation(report, runtime_generation_id):
        rows.append(
            {
                "generated_at": report.get("generated_at"),
                "intent_id": report.get("intent_id") or "",
                "strategy_id": report.get("strategy_id") or "",
                "symbol": report.get("symbol") or "",
                "action": report.get("action") or "",
                "quantity": report.get("quantity"),
                "classification": report.get("classification") or "",
            }
        )
    return _sort_by_time(rows)


def _broker_mutations_observed(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    report = _mapping(inputs.get("strategy_bridge_report"))
    for row in _list(report.get("broker_mutations")) + _list(report.get("mutation_attempts")):
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_mutation_row(payload, source="strategy_bridge_report"))
    managed_orders = _mapping(inputs.get("managed_order_registry"))
    for row in _list(managed_orders.get("managed_orders")):
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_mutation_row(payload, source="managed_order_registry"))
    open_order_truth = _mapping(inputs.get("open_order_truth"))
    for row in _list(open_order_truth.get("order_states")) + _list(open_order_truth.get("open_orders")):
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_mutation_row(payload, source="open_order_truth"))
    return _sort_by_time(rows)


def _mutation_row(payload: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at") or payload.get("updated_at") or payload.get("created_at"),
        "source": source,
        "action_type": payload.get("action_type") or payload.get("action") or payload.get("side") or "",
        "broker_order_id": payload.get("broker_order_id") or payload.get("order_id") or payload.get("orderId"),
        "perm_id": payload.get("perm_id") or payload.get("permId"),
        "symbol": payload.get("symbol") or payload.get("contract_symbol") or "",
        "quantity": payload.get("quantity") or payload.get("remaining") or payload.get("qty"),
        "classification": payload.get("classification") or payload.get("status") or "",
    }


def _lifecycle_events(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_name in ("ledger_summary", "reconciliation"):
        source = _mapping(inputs.get(source_name))
        source_rows = (
            _list(source.get("lifecycle_events"))
            + _list(source.get("ledger_events"))
            + _list(source.get("track_b_lifecycle_positions"))
        )
        for row in source_rows:
            payload = _mapping(row)
            if payload and _matches_generation(payload, runtime_generation_id):
                rows.append(_lifecycle_row(payload, source=source_name))
    for report in _list(inputs.get("lifecycle_reports")):
        payload = _mapping(report)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_lifecycle_row(payload, source="lifecycle_report"))
    return _sort_by_time(rows)


def _lifecycle_row(payload: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at") or payload.get("updated_at") or payload.get("closed_at"),
        "source": source,
        "lifecycle_id": payload.get("lifecycle_id") or payload.get("position_id") or payload.get("manifest_id") or "",
        "state": payload.get("state") or payload.get("final_position_status") or payload.get("classification") or "",
        "symbol": payload.get("symbol") or payload.get("contract_symbol") or "",
        "reason": payload.get("reason") or payload.get("review_reason") or "",
    }


def _recovery_events(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    history = _mapping(inputs.get("recovery_attempt_history"))
    for row in _list(history.get("recent_attempts")) + _list(inputs.get("executor_events")):
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_recovery_row(payload, source="recovery_attempt"))
    latest = _mapping(inputs.get("latest_executor_report"))
    if latest and _matches_generation(latest, runtime_generation_id):
        rows.append(_recovery_row(latest, source="latest_executor_report"))
    for row in _list(inputs.get("budget_events")):
        payload = _mapping(row)
        if payload and _matches_generation(payload, runtime_generation_id):
            rows.append(_recovery_row(payload, source="recovery_budget_event"))
    return _sort_by_time(_unique_payloads(rows, key="event_id"))


def _recovery_row(payload: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    event_id = _first_text(payload.get("recovery_attempt_id"), payload.get("reservation_id"), payload.get("event_id"))
    return {
        "event_id": event_id,
        "generated_at": payload.get("generated_at") or payload.get("occurred_at") or payload.get("created_at"),
        "source": source,
        "event_type": payload.get("event_type") or payload.get("classification") or "",
        "action_type": payload.get("action_type") or "",
        "classification": payload.get("classification") or payload.get("event_type") or "",
        "budget_key": payload.get("budget_key") or payload.get("recovery_budget_key") or "",
        "execution_enabled": payload.get("execution_enabled") is True,
    }


def _safe_state_transitions(*, inputs: Mapping[str, Any], runtime_generation_id: str) -> list[dict[str, Any]]:
    safe_state = _mapping(inputs.get("safe_state"))
    if not safe_state or not _matches_generation(safe_state, runtime_generation_id):
        return []
    return [
        {
            "generated_at": safe_state.get("generated_at"),
            "safe_state_classification": safe_state.get("safe_state_classification") or safe_state.get("classification"),
            "runtime_generation_id": safe_state.get("runtime_generation_id") or "",
            "observe_only": safe_state.get("observe_only") is True,
            "recovery_only": safe_state.get("recovery_only") is True,
            "tripped_limits": list(safe_state.get("tripped_limits") or []),
            "operator_explanation": safe_state.get("operator_explanation") or "",
        }
    ]


def _runtime_log_excerpt(
    *,
    config: TrackBRuntimeGenerationAuditViewerConfig,
    runtime_generation_id: str,
    max_lines: int = 20,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for relative_path in config.runtime_log_paths:
        path = config.resolve(relative_path)
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        matches = [line for line in lines if runtime_generation_id in line][-max_lines:]
        if matches:
            rows.append({"path": str(path), "matching_line_count": str(len(matches)), "tail": "\n".join(matches)})
    return rows


def _final_state(
    *,
    inputs: Mapping[str, Any],
    control_chain: Sequence[Mapping[str, Any]],
    safe_state_transitions: Sequence[Mapping[str, Any]],
    runtime_generation_id: str,
) -> dict[str, Any]:
    latest_control = control_chain[-1] if control_chain else {}
    raw_safe_state = _mapping(inputs.get("safe_state"))
    latest_safe_state = (
        safe_state_transitions[-1]
        if safe_state_transitions
        else raw_safe_state
        if _matches_generation(raw_safe_state, runtime_generation_id)
        else {}
    )
    return {
        "latest_control_plane_classification": latest_control.get("top_line_classification") or "",
        "latest_runtime_supervisor_classification": latest_control.get("runtime_supervisor_classification")
        or _mapping(inputs.get("runtime_supervisor")).get("classification")
        or "",
        "latest_safe_state_classification": latest_safe_state.get("safe_state_classification")
        or latest_safe_state.get("classification")
        or "",
        "position_truth_classification": _payload_classification(_mapping(inputs.get("position_truth"))),
        "open_order_truth_classification": _payload_classification(_mapping(inputs.get("open_order_truth"))),
        "managed_order_registry_classification": _payload_classification(_mapping(inputs.get("managed_order_registry"))),
        "managed_position_registry_classification": _payload_classification(_mapping(inputs.get("managed_position_registry"))),
        "reconciliation_classification": _payload_classification(_mapping(inputs.get("reconciliation"))),
    }


def _unresolved_questions(
    *,
    inputs: Mapping[str, Any],
    control_chain: Sequence[Mapping[str, Any]],
    generation_id: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    position = _mapping(inputs.get("position_truth"))
    managed_positions = _mapping(inputs.get("managed_position_registry"))
    open_orders = _mapping(inputs.get("open_order_truth"))
    managed_orders = _mapping(inputs.get("managed_order_registry"))
    reconciliation = _mapping(inputs.get("reconciliation"))
    if not control_chain:
        rows.append(
            {
                "code": "missing_control_plane_decision_chain",
                "detail": f"No Control Plane Snapshot row matched runtime_generation_id={generation_id}.",
            }
        )
    if _payload_classification(position) == "CLEAN_FLAT_READY" and _payload_classification(managed_positions) not in {
        "",
        "NO_MANAGED_POSITIONS",
    }:
        rows.append(
            {
                "code": "position_truth_managed_position_mismatch",
                "detail": "Position Truth is clean/flat but Managed Position Registry is not NO_MANAGED_POSITIONS.",
            }
        )
    if _payload_classification(open_orders) == "NO_OPEN_ORDERS" and _payload_classification(managed_orders) not in {
        "",
        "NO_MANAGED_ORDERS",
    }:
        rows.append(
            {
                "code": "open_order_truth_managed_order_mismatch",
                "detail": "Open Order Truth is clear but Managed Order Registry still reports managed order state.",
            }
        )
    if reconciliation and reconciliation.get("broker_reconciled") is False:
        rows.append(
            {
                "code": "broker_reconciliation_not_clean",
                "detail": f"Reconciliation is {_payload_classification(reconciliation) or 'unclassified'} and broker_reconciled=false.",
            }
        )
    return rows


def _audit_classification(
    *,
    control_chain: Sequence[Mapping[str, Any]],
    strategy_intents: Sequence[Mapping[str, Any]],
    broker_mutations: Sequence[Mapping[str, Any]],
    lifecycle_events: Sequence[Mapping[str, Any]],
    recovery_events: Sequence[Mapping[str, Any]],
    safe_state_transitions: Sequence[Mapping[str, Any]],
    unresolved_questions: Sequence[Mapping[str, str]],
) -> str:
    if any(row.get("code", "").endswith("_mismatch") or row.get("code") == "broker_reconciliation_not_clean" for row in unresolved_questions):
        return RUNTIME_GENERATION_AUDIT_INCONSISTENT
    any_events = any(
        [control_chain, strategy_intents, broker_mutations, lifecycle_events, recovery_events, safe_state_transitions]
    )
    if not any_events:
        return RUNTIME_GENERATION_AUDIT_NO_EVENTS
    if not control_chain:
        return RUNTIME_GENERATION_AUDIT_PARTIAL
    return RUNTIME_GENERATION_AUDIT_READY


def _generation_bounds(
    *,
    control_chain: Sequence[Mapping[str, Any]],
    strategy_intents: Sequence[Mapping[str, Any]],
    broker_mutations: Sequence[Mapping[str, Any]],
    lifecycle_events: Sequence[Mapping[str, Any]],
    recovery_events: Sequence[Mapping[str, Any]],
    safe_state_transitions: Sequence[Mapping[str, Any]],
) -> tuple[str | None, str | None]:
    timestamps = sorted(
        filter(
            None,
            [
                str(row.get("generated_at") or "")
                for group in (
                    control_chain,
                    strategy_intents,
                    broker_mutations,
                    lifecycle_events,
                    recovery_events,
                    safe_state_transitions,
                )
                for row in group
            ],
        )
    )
    if not timestamps:
        return None, None
    return timestamps[0], timestamps[-1]


def _source_commit(*, inputs: Mapping[str, Any], control_chain: Sequence[Mapping[str, Any]]) -> str:
    for payload in [
        _mapping(inputs.get("runtime_resume")),
        _mapping(inputs.get("runtime_supervisor")),
        *[_mapping(row) for row in _list(inputs.get("control_plane_snapshots"))],
    ]:
        value = _first_text(payload.get("source_commit"), payload.get("previous_source_commit"))
        if value:
            return value
    for row in control_chain:
        value = _first_text(row.get("source_commit"))
        if value:
            return value
    return ""


def _operator_summary(
    *,
    classification: str,
    runtime_generation_id: str,
    final_state: Mapping[str, Any],
    unresolved_questions: Sequence[Mapping[str, str]],
) -> str:
    if classification == RUNTIME_GENERATION_AUDIT_READY:
        return f"Runtime generation {runtime_generation_id} has a coherent audit view from control-plane evidence."
    if classification == RUNTIME_GENERATION_AUDIT_NO_EVENTS:
        return f"No matching events were found for runtime generation {runtime_generation_id}."
    if classification == RUNTIME_GENERATION_AUDIT_INCONSISTENT:
        reason = unresolved_questions[0]["detail"] if unresolved_questions else "authority evidence disagrees"
        return f"Runtime generation {runtime_generation_id} has inconsistent audit evidence: {reason}"
    if classification == RUNTIME_GENERATION_AUDIT_PARTIAL:
        return f"Runtime generation {runtime_generation_id} has partial audit evidence; control-plane chain is incomplete."
    return f"Runtime generation {runtime_generation_id} audit status is {classification}; final_state={dict(final_state)}."


def _markdown_report(payload: Mapping[str, Any]) -> str:
    unresolved = payload.get("unresolved_questions") or []
    lines = [
        "# Track B Runtime Generation Audit",
        "",
        f"- classification: `{payload.get('classification')}`",
        f"- runtime_generation_id: `{payload.get('runtime_generation_id')}`",
        f"- generation_start: `{payload.get('generation_start')}`",
        f"- generation_end: `{payload.get('generation_end')}`",
        f"- source_commit: `{payload.get('source_commit') or ''}`",
        "",
        "## Operator Summary",
        "",
        str(payload.get("operator_summary") or ""),
        "",
        "## Counts",
        "",
        f"- control_plane_decisions: {len(payload.get('control_plane_decision_chain') or [])}",
        f"- strategy_intents: {len(payload.get('strategy_intents') or [])}",
        f"- broker_mutations_observed: {len(payload.get('broker_mutations_observed') or [])}",
        f"- lifecycle_events: {len(payload.get('lifecycle_events') or [])}",
        f"- recovery_events: {len(payload.get('recovery_events') or [])}",
        f"- safe_state_transitions: {len(payload.get('safe_state_transitions') or [])}",
        "",
        "## Unresolved Questions",
        "",
    ]
    if not unresolved:
        lines.append("- none")
    else:
        for row in unresolved:
            mapped = _mapping(row)
            lines.append(f"- `{mapped.get('code')}`: {mapped.get('detail')}")
    lines.append("")
    return "\n".join(lines)


def _read_lifecycle_reports(root: Path) -> list[dict[str, Any]]:
    try:
        paths = sorted(root.rglob("*.json"))
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for path in paths:
        payload = _read_json(path)
        if payload:
            rows.append(payload)
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _matches_generation(payload: Mapping[str, Any], runtime_generation_id: str) -> bool:
    if not runtime_generation_id:
        return False
    return runtime_generation_id in _generation_values(payload)


def _generation_values(value: Any) -> set[str]:
    values: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            if "generation" in str(key).lower() and item is not None:
                values.add(str(item))
            values.update(_generation_values(item))
    elif isinstance(value, list):
        for item in value:
            values.update(_generation_values(item))
    return values


def _sort_by_time(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted([dict(row) for row in rows], key=lambda row: str(row.get("generated_at") or ""))


def _unique_payloads(rows: Sequence[Mapping[str, Any]], *, key: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        row_key = str(payload.get(key) or json.dumps(payload, sort_keys=True, default=str))
        if row_key in seen:
            continue
        seen.add(row_key)
        unique.append(payload)
    return unique


def _source_artifact_paths(config: TrackBRuntimeGenerationAuditViewerConfig) -> dict[str, str]:
    return {
        "authority": str(config.resolve(config.output_path)),
        "markdown_report": "" if config.markdown_report_path is None else str(config.resolve(config.markdown_report_path)),
        "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
        "runtime_supervisor": str(config.resolve(config.runtime_supervisor_path)),
        "runtime_resume": str(config.resolve(config.runtime_resume_path)),
        "safe_state": str(config.resolve(config.safe_state_path)),
        "recovery_attempt_history": str(config.resolve(config.recovery_attempt_history_path)),
        "executor_event_log": str(config.resolve(config.executor_event_log_path)),
        "latest_executor_report": str(config.resolve(config.latest_executor_report_path)),
        "recovery_budget_events": str(config.resolve(config.recovery_budget_events_path)),
        "recovery_budget_ledger": str(config.resolve(config.recovery_budget_ledger_path)),
        "strategy_bridge_report": str(config.resolve(config.strategy_bridge_report_path)),
        "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "position_truth": str(config.resolve(config.position_truth_path)),
        "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "ledger_summary": str(config.resolve(config.ledger_summary_path)),
        "lifecycle_root": str(config.resolve(config.lifecycle_root)),
    }


def _payload_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


def _any_live_money(value: Any) -> bool:
    if isinstance(value, Mapping):
        if value.get("live_money_eligible") is True:
            return True
        return any(_any_live_money(item) for item in value.values())
    if isinstance(value, list):
        return any(_any_live_money(item) for item in value)
    return False


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return ""


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
