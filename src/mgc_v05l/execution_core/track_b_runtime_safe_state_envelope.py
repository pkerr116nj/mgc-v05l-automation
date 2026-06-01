"""Track B PAPER runtime safe-state envelope authority.

The safe-state envelope is an advisory containment layer. It computes bounded
PAPER runtime posture from execution_core authority artifacts, but it never
starts processes or mutates broker, order, lifecycle, or ledger state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_position_guardian import (
    BROKER_POSITION_GUARDIAN_HARD_HOLD,
    DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT,
)


SAFE_STATE_NORMAL = "SAFE_STATE_NORMAL"
SAFE_STATE_OBSERVE_ONLY = "SAFE_STATE_OBSERVE_ONLY"
SAFE_STATE_RECOVERY_ONLY = "SAFE_STATE_RECOVERY_ONLY"
SAFE_STATE_HARD_HOLD = "SAFE_STATE_HARD_HOLD"
SAFE_STATE_DUPLICATE_INTENT_RISK = "SAFE_STATE_DUPLICATE_INTENT_RISK"
SAFE_STATE_BROKER_MUTATION_LIMIT_HIT = "SAFE_STATE_BROKER_MUTATION_LIMIT_HIT"
SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT = "SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT"
SAFE_STATE_POSITION_LIMIT_HIT = "SAFE_STATE_POSITION_LIMIT_HIT"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
)
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
)
DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json"
)
DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json"
)
DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_recovery_attempt_history.json"
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
DEFAULT_LIFECYCLE_SUMMARY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "lifecycle_state" / "latest_lifecycle_state_summary.json"
)
DEFAULT_LEDGER_SUMMARY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_STRATEGY_BRIDGE_SUBMIT_REPORT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "strategy_bridge" / "latest_strategy_bridge_submit_report.json"
)
DEFAULT_BROKER_POSITION_GUARDIAN_PATH = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT


@dataclass(frozen=True)
class TrackBRuntimeSafeStateEnvelopeConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    runtime_resume_semantics_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    recovery_attempt_history_path: Path = DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    lifecycle_summary_path: Path = DEFAULT_LIFECYCLE_SUMMARY_ARTIFACT
    ledger_summary_path: Path = DEFAULT_LEDGER_SUMMARY_ARTIFACT
    strategy_bridge_submit_report_path: Path = DEFAULT_STRATEGY_BRIDGE_SUBMIT_REPORT_ARTIFACT
    broker_position_guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_PATH
    max_orders_per_runtime_generation_id: int = 4
    max_submits_per_symbol_per_window: int = 3
    max_broker_mutation_attempts_per_window: int = 5
    max_failed_broker_mutations_per_window: int = 2
    max_duplicate_intent_attempts: int = 1
    max_managed_open_positions_per_strategy_lane: int = 1
    max_consecutive_lifecycle_reconciliation_disagreements: int = 2
    max_recovery_attempts_per_runtime_generation: int = 3

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_runtime_safe_state_envelope(
    *,
    config: TrackBRuntimeSafeStateEnvelopeConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = _inputs(config=config, overrides=input_overrides or {})
    counters = _limit_counters(inputs=inputs)
    tripped_limits = _tripped_limits(config=config, inputs=inputs, counters=counters)
    classification = _classify(inputs=inputs, tripped_limits=tripped_limits)
    posture = _posture(classification=classification, inputs=inputs, tripped_limits=tripped_limits)
    close_authority = _managed_close_authority(inputs=inputs, tripped_limits=tripped_limits)
    control_plane = inputs["control_plane_snapshot"]
    runtime_generation_id = _runtime_generation_id(inputs)
    snapshot_id = str(control_plane.get("control_plane_snapshot_id") or "")
    return {
        "schema_version": "track_b_runtime_safe_state_envelope_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "advisory_only": True,
        "containment_authority": True,
        "submit_authority": False,
        "cancel_authority": False,
        "replace_authority": False,
        "modify_authority": False,
        "flatten_authority": False,
        "broker_mutation_execution_performed": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "runtime_stop_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": _live_money_eligible(inputs),
        "safe_state_classification": classification,
        "classification": classification,
        "broker_mutation_allowed": posture["broker_mutation_allowed"],
        "entry_mutation_allowed": posture["entry_mutation_allowed"],
        "managed_close_mutation_allowed": posture["managed_close_mutation_allowed"],
        "close_authority_reason_codes": close_authority["reason_codes"],
        "close_authority": close_authority,
        "runtime_start_allowed": posture["runtime_start_allowed"],
        "submit_allowed": posture["submit_allowed"],
        "observe_only": posture["observe_only"],
        "recovery_only": posture["recovery_only"],
        "tripped_limits": tripped_limits,
        "limit_counters": counters,
        "runtime_generation_id": runtime_generation_id,
        "control_plane_snapshot_id": snapshot_id,
        "shared_truth_generation_id": control_plane.get("shared_truth_refresh_generation_id"),
        "operator_explanation": _operator_explanation(classification=classification, tripped_limits=tripped_limits),
        "recommended_next_step": _recommended_next_step(classification=classification),
        "prohibited_actions": [
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
            "hidden_recovery",
            "dashboard_projection_authority",
            "runtime_restart_execution",
            "broker_mutation_execution",
        ],
        "source_artifact_paths": _source_artifact_paths(config),
    }


def write_track_b_runtime_safe_state_envelope(
    *,
    config: TrackBRuntimeSafeStateEnvelopeConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER runtime safe-state envelope.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRuntimeSafeStateEnvelopeConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = build_track_b_runtime_safe_state_envelope(config=config)
    authority_path = write_track_b_runtime_safe_state_envelope(config=config, payload=payload)
    summary = {
        "safe_state_classification": payload.get("safe_state_classification"),
        "runtime_generation_id": payload.get("runtime_generation_id"),
        "control_plane_snapshot_id": payload.get("control_plane_snapshot_id"),
        "broker_mutation_allowed": payload.get("broker_mutation_allowed"),
        "entry_mutation_allowed": payload.get("entry_mutation_allowed"),
        "managed_close_mutation_allowed": payload.get("managed_close_mutation_allowed"),
        "close_authority_reason_codes": payload.get("close_authority_reason_codes"),
        "runtime_start_allowed": payload.get("runtime_start_allowed"),
        "submit_allowed": payload.get("submit_allowed"),
        "observe_only": payload.get("observe_only"),
        "recovery_only": payload.get("recovery_only"),
        "tripped_limits": payload.get("tripped_limits"),
        "operator_explanation": payload.get("operator_explanation"),
        "recommended_next_step": payload.get("recommended_next_step"),
        "authority_path": str(authority_path),
        "read_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": payload.get("live_money_eligible"),
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("safe_state_classification") == SAFE_STATE_NORMAL else 2


def _inputs(
    *,
    config: TrackBRuntimeSafeStateEnvelopeConfig,
    overrides: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    paths = {
        "control_plane_snapshot": config.control_plane_snapshot_path,
        "runtime_resume_semantics": config.runtime_resume_semantics_path,
        "recovery_budget_ledger": config.recovery_budget_ledger_path,
        "recovery_attempt_history": config.recovery_attempt_history_path,
        "managed_order_registry": config.managed_order_registry_path,
        "open_order_truth": config.open_order_truth_path,
        "position_truth": config.position_truth_path,
        "managed_position_registry": config.managed_position_registry_path,
        "reconciliation": config.reconciliation_path,
        "lifecycle_summary": config.lifecycle_summary_path,
        "ledger_summary": config.ledger_summary_path,
        "strategy_bridge_submit_report": config.strategy_bridge_submit_report_path,
        "broker_position_guardian": config.broker_position_guardian_path,
    }
    return {name: overrides.get(name) or _read_json(config.resolve(path)) for name, path in paths.items()}


def _limit_counters(*, inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    managed_orders = inputs["managed_order_registry"]
    managed_positions = inputs["managed_position_registry"]
    open_order_truth = inputs["open_order_truth"]
    recovery_history = inputs["recovery_attempt_history"]
    strategy_report = inputs["strategy_bridge_submit_report"]
    reconciliation = inputs["reconciliation"]
    lifecycle_summary = inputs["lifecycle_summary"]
    ledger_summary = inputs["ledger_summary"]
    runtime_generation_id = _runtime_generation_id(inputs)
    managed_order_rows = _list(managed_orders.get("managed_orders"))
    order_rows = _matching_generation_rows(
        rows=managed_order_rows or _list(open_order_truth.get("order_states")) or _list(open_order_truth.get("open_orders")),
        runtime_generation_id=runtime_generation_id,
    )
    managed_position_rows = _list(managed_positions.get("managed_positions")) or _list(
        managed_positions.get("positions")
    )
    open_position_counts = _open_position_counts(managed_position_rows)
    recent_attempts = _matching_generation_rows(
        rows=_list(recovery_history.get("recent_attempts")),
        runtime_generation_id=runtime_generation_id,
    )
    return {
        "orders_per_runtime_generation_id": len(order_rows)
        or _summary_count(managed_orders, "managed_order_count")
        or _summary_count(managed_orders, "working_close_order_count"),
        "submits_per_symbol_per_window": _submits_per_symbol(strategy_report),
        "max_submits_per_symbol_per_window_observed": max(_submits_per_symbol(strategy_report).values(), default=0),
        "broker_mutation_attempts_per_window": _first_int(
            strategy_report.get("broker_mutation_attempts_per_window"),
            strategy_report.get("broker_mutation_attempt_count"),
            _summary_count(strategy_report, "broker_mutation_attempt_count"),
        ),
        "failed_broker_mutations_per_window": _first_int(
            strategy_report.get("failed_broker_mutations_per_window"),
            strategy_report.get("failed_broker_mutation_count"),
            _summary_count(strategy_report, "failed_broker_mutation_count"),
        ),
        "duplicate_intent_attempts": _first_int(
            strategy_report.get("duplicate_intent_attempts"),
            _summary_count(strategy_report, "duplicate_intent_attempt_count"),
            len(_list(open_order_truth.get("duplicate_close_order_groups"))),
        ),
        "managed_open_positions_per_strategy_lane": open_position_counts,
        "max_managed_open_positions_per_strategy_lane_observed": max(open_position_counts.values(), default=0),
        "consecutive_lifecycle_reconciliation_disagreements": _first_int(
            lifecycle_summary.get("consecutive_reconciliation_disagreement_count"),
            reconciliation.get("consecutive_lifecycle_reconciliation_disagreements"),
            ledger_summary.get("consecutive_lifecycle_reconciliation_disagreements"),
            _summary_count(reconciliation, "review_required_count"),
        ),
        "recovery_attempts_per_runtime_generation": len(recent_attempts)
        or _first_int(recovery_history.get("runtime_generation_attempt_count"), _summary_count(recovery_history, "attempt_count")),
    }


def _tripped_limits(
    *,
    config: TrackBRuntimeSafeStateEnvelopeConfig,
    inputs: Mapping[str, Mapping[str, Any]],
    counters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if _live_money_eligible(inputs):
        rows.append(_limit("live_money_eligible", SAFE_STATE_HARD_HOLD, True, False, "Live-money route is prohibited."))
    guardian = inputs["broker_position_guardian"]
    if guardian.get("classification") == BROKER_POSITION_GUARDIAN_HARD_HOLD:
        rows.append(
            _limit(
                "broker_position_guardian_hard_hold",
                SAFE_STATE_HARD_HOLD,
                ", ".join(str(item) for item in guardian.get("hard_classifications") or []) or "hard_hold",
                BROKER_POSITION_GUARDIAN_HARD_HOLD,
                str(guardian.get("operator_explanation") or "Broker Position Guardian reports hard hold."),
            )
        )
    if not inputs["control_plane_snapshot"].get("control_plane_snapshot_id"):
        rows.append(
            _limit(
                "control_plane_snapshot_missing",
                SAFE_STATE_OBSERVE_ONLY,
                "missing",
                "present",
                "Control Plane Snapshot evidence is missing; safe-state envelope is diagnostic/observe-only.",
            )
        )
    if _duplicate_writer(inputs):
        rows.append(
            _limit(
                "duplicate_runtime_writer",
                SAFE_STATE_HARD_HOLD,
                _duplicate_writer_count(inputs),
                0,
                "Duplicate Track B PAPER runtime writer evidence is present.",
            )
        )
    _append_if_limit(
        rows,
        "orders_per_runtime_generation_id",
        SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
        _as_int(counters.get("orders_per_runtime_generation_id")),
        config.max_orders_per_runtime_generation_id,
        "Runtime generation order count exceeded the PAPER containment budget.",
    )
    _append_if_limit(
        rows,
        "submits_per_symbol_per_window",
        SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
        _as_int(counters.get("max_submits_per_symbol_per_window_observed")),
        config.max_submits_per_symbol_per_window,
        "Submit attempts per symbol exceeded the PAPER containment budget.",
    )
    _append_if_limit(
        rows,
        "broker_mutation_attempts_per_window",
        SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
        _as_int(counters.get("broker_mutation_attempts_per_window")),
        config.max_broker_mutation_attempts_per_window,
        "Broker mutation attempts exceeded the PAPER containment budget.",
    )
    _append_if_limit(
        rows,
        "failed_broker_mutations_per_window",
        SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
        _as_int(counters.get("failed_broker_mutations_per_window")),
        config.max_failed_broker_mutations_per_window,
        "Failed broker mutation attempts exceeded the PAPER containment budget.",
    )
    duplicate_attempts = _as_int(counters.get("duplicate_intent_attempts"))
    if duplicate_attempts > config.max_duplicate_intent_attempts:
        severity = SAFE_STATE_HARD_HOLD if duplicate_attempts > config.max_duplicate_intent_attempts * 2 else SAFE_STATE_DUPLICATE_INTENT_RISK
        rows.append(
            _limit(
                "duplicate_intent_attempts",
                severity,
                duplicate_attempts,
                config.max_duplicate_intent_attempts,
                "Duplicate submit/close intent attempts exceeded the PAPER containment budget.",
            )
        )
    _append_if_limit(
        rows,
        "managed_open_positions_per_strategy_lane",
        SAFE_STATE_POSITION_LIMIT_HIT,
        _as_int(counters.get("max_managed_open_positions_per_strategy_lane_observed")),
        config.max_managed_open_positions_per_strategy_lane,
        "Managed open positions per strategy/lane exceeded the PAPER containment budget.",
    )
    _append_if_limit(
        rows,
        "consecutive_lifecycle_reconciliation_disagreements",
        SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT,
        _as_int(counters.get("consecutive_lifecycle_reconciliation_disagreements")),
        config.max_consecutive_lifecycle_reconciliation_disagreements,
        "Lifecycle/reconciliation disagreements exceeded the PAPER containment budget.",
    )
    _append_if_limit(
        rows,
        "recovery_attempts_per_runtime_generation",
        SAFE_STATE_RECOVERY_ONLY,
        _as_int(counters.get("recovery_attempts_per_runtime_generation")),
        config.max_recovery_attempts_per_runtime_generation,
        "Recovery attempts for this runtime generation exceeded the PAPER containment budget.",
    )
    return rows


def _classify(*, inputs: Mapping[str, Mapping[str, Any]], tripped_limits: Sequence[Mapping[str, Any]]) -> str:
    classifications = {str(row.get("classification") or "") for row in tripped_limits}
    if SAFE_STATE_HARD_HOLD in classifications:
        return SAFE_STATE_HARD_HOLD
    if SAFE_STATE_POSITION_LIMIT_HIT in classifications:
        return SAFE_STATE_POSITION_LIMIT_HIT
    if SAFE_STATE_BROKER_MUTATION_LIMIT_HIT in classifications:
        return SAFE_STATE_BROKER_MUTATION_LIMIT_HIT
    if SAFE_STATE_DUPLICATE_INTENT_RISK in classifications:
        return SAFE_STATE_DUPLICATE_INTENT_RISK
    if SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT in classifications:
        return SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT
    if SAFE_STATE_RECOVERY_ONLY in classifications:
        return SAFE_STATE_RECOVERY_ONLY
    if SAFE_STATE_OBSERVE_ONLY in classifications:
        return SAFE_STATE_OBSERVE_ONLY
    if _classification(inputs["open_order_truth"]) in {"DUPLICATE_CLOSE_ORDER", "SUSPICIOUS_ORDER_STATE"}:
        return SAFE_STATE_DUPLICATE_INTENT_RISK
    return SAFE_STATE_NORMAL


def _posture(
    *,
    classification: str,
    inputs: Mapping[str, Mapping[str, Any]],
    tripped_limits: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    close_authority = _managed_close_authority(inputs=inputs, tripped_limits=tripped_limits)
    managed_close_allowed = close_authority["allowed"]
    if classification in {SAFE_STATE_HARD_HOLD, SAFE_STATE_POSITION_LIMIT_HIT}:
        return {
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": managed_close_allowed,
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "observe_only": True,
            "recovery_only": False,
        }
    if classification == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT:
        return {
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": False,
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "observe_only": False,
            "recovery_only": True,
        }
    if classification in {SAFE_STATE_DUPLICATE_INTENT_RISK, SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT}:
        return {
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": False,
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "observe_only": True,
            "recovery_only": False,
        }
    if classification == SAFE_STATE_RECOVERY_ONLY:
        return {
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": False,
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "observe_only": False,
            "recovery_only": True,
        }
    if classification == SAFE_STATE_OBSERVE_ONLY:
        return {
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": False,
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "observe_only": True,
            "recovery_only": False,
        }
    return {
        "broker_mutation_allowed": True,
        "entry_mutation_allowed": True,
        "managed_close_mutation_allowed": True,
        "runtime_start_allowed": inputs["control_plane_snapshot"].get("safe_to_start_runtime") is True,
        "submit_allowed": inputs["control_plane_snapshot"].get("safe_to_start_runtime") is True,
        "observe_only": False,
        "recovery_only": False,
    }


def _managed_close_authority(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    tripped_limits: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    guardian = inputs["broker_position_guardian"]
    guardian_close = _mapping(guardian.get("managed_close_authority"))
    reason_codes: list[str] = []
    if guardian_close.get("allowed") is not True:
        reason_codes.extend(str(item) for item in guardian_close.get("reason_codes") or [])
        reason_codes.append("BROKER_POSITION_GUARDIAN_CLOSE_NOT_ALLOWED")
    for limit in tripped_limits:
        limit_id = str(limit.get("limit_id") or "")
        if limit_id == "broker_position_guardian_hard_hold":
            continue
        reason_codes.append(f"SAFE_STATE_LIMIT_BLOCKS_CLOSE:{limit_id or 'UNKNOWN'}")
    reason_codes = _dedupe(reason_codes)
    allowed = not reason_codes
    return {
        "classification": "MANAGED_CLOSE_MUTATION_ALLOWED" if allowed else "MANAGED_CLOSE_MUTATION_BLOCKED",
        "allowed": allowed,
        "authority_source": "BROKER_POSITION_GUARDIAN_REGISTRY_TRUTH",
        "reason_codes": reason_codes,
        "guardian_classification": guardian.get("classification"),
        "guardian_close_classification": guardian_close.get("classification"),
        "guardian_close_candidates": list(guardian_close.get("candidates") or []),
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }


def _operator_explanation(*, classification: str, tripped_limits: Sequence[Mapping[str, Any]]) -> str:
    if classification == SAFE_STATE_NORMAL:
        return "Safe-state envelope is normal; no containment limit has tripped."
    reasons = [str(row.get("reason") or row.get("limit_id") or "") for row in tripped_limits if row]
    detail = " ".join(reason for reason in reasons if reason)
    if classification == SAFE_STATE_HARD_HOLD:
        return f"Hard safe-state hold: {detail}".strip()
    if classification == SAFE_STATE_POSITION_LIMIT_HIT:
        return f"Position containment limit hit; hold runtime and preserve evidence. {detail}".strip()
    if classification == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT:
        return f"Broker mutation containment limit hit; shift to recovery-only/observe posture. {detail}".strip()
    if classification == SAFE_STATE_DUPLICATE_INTENT_RISK:
        return f"Duplicate intent risk detected; observe-only until identity/evidence is clean. {detail}".strip()
    if classification == SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT:
        return f"Lifecycle/reconciliation disagreement limit hit; observe-only and preserve artifacts. {detail}".strip()
    if classification == SAFE_STATE_OBSERVE_ONLY:
        return f"Observe-only safe-state posture: {detail}".strip()
    return f"Recovery budget limit hit; shift to recovery-only posture. {detail}".strip()


def _recommended_next_step(*, classification: str) -> str:
    if classification == SAFE_STATE_NORMAL:
        return "continue normal PAPER observation through the Control Plane Snapshot"
    if classification == SAFE_STATE_HARD_HOLD:
        return "hold runtime/start/submit actions; preserve artifacts and refresh shared truth"
    if classification == SAFE_STATE_POSITION_LIMIT_HIT:
        return "hold runtime and submit paths; inspect managed position identity before any scoped recovery"
    if classification == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT:
        return "disable broker mutation posture; allow only read-only recovery planning and evidence refresh"
    if classification == SAFE_STATE_DUPLICATE_INTENT_RISK:
        return "quarantine duplicate intent evidence; do not submit or modify until exact identity is clean"
    if classification == SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT:
        return "observe only; refresh reconciliation and lifecycle matrix evidence"
    if classification == SAFE_STATE_OBSERVE_ONLY:
        return "observe only; rebuild Control Plane Snapshot before any runtime or broker action"
    return "recovery-only posture; wait for budget reset or policy refresh"


def _runtime_generation_id(inputs: Mapping[str, Mapping[str, Any]]) -> str:
    snapshot = inputs["control_plane_snapshot"]
    resume = inputs["runtime_resume_semantics"]
    return str(
        snapshot.get("runtime_generation_id")
        or snapshot.get("runtime_resume_proposed_next_runtime_generation_id")
        or snapshot.get("runtime_resume_previous_runtime_generation_id")
        or resume.get("proposed_next_runtime_generation_id")
        or resume.get("previous_runtime_generation_id")
        or ""
    )


def _submits_per_symbol(payload: Mapping[str, Any]) -> dict[str, int]:
    direct = payload.get("submits_per_symbol_window") or payload.get("submits_per_symbol_per_window")
    if isinstance(direct, Mapping):
        return {str(key): _as_int(value) for key, value in direct.items()}
    counts: dict[str, int] = {}
    for row in _list(payload.get("recent_submits")) + _list(payload.get("submit_attempts")):
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or row.get("contract_symbol") or row.get("target_symbol") or "UNKNOWN")
        counts[symbol] = counts.get(symbol, 0) + 1
    return counts


def _open_position_counts(rows: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in rows:
        row = _mapping(item)
        classification = str(row.get("classification") or row.get("state") or row.get("status") or "")
        if classification and classification not in {"OPEN_MANAGED", "ACTIVE", "REVIEW_REQUIRED"}:
            continue
        lane = str(row.get("strategy_lane_id") or row.get("lane_id") or row.get("strategy_id") or "unknown")
        counts[lane] = counts.get(lane, 0) + 1
    return counts


def _matching_generation_rows(*, rows: Sequence[Any], runtime_generation_id: str) -> list[Mapping[str, Any]]:
    mapped = [_mapping(row) for row in rows if isinstance(row, Mapping)]
    if not runtime_generation_id:
        return mapped
    matching = [
        row
        for row in mapped
        if str(row.get("runtime_generation_id") or row.get("proposed_next_runtime_generation_id") or "") == runtime_generation_id
    ]
    return matching or mapped


def _live_money_eligible(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    return any(_contains_true(payload, "live_money_eligible") for payload in inputs.values())


def _duplicate_writer(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    snapshot = inputs["control_plane_snapshot"]
    if snapshot.get("agent_health_has_duplicate_writer") is True:
        return True
    if _as_int(snapshot.get("duplicate_process_count")) > 0:
        return True
    for row in _list(snapshot.get("agent_health_top_blockers")) + _list(snapshot.get("prioritized_blockers")):
        mapped = _mapping(row)
        if str(mapped.get("status") or "") == "DUPLICATE_PROCESS":
            return True
    return False


def _duplicate_writer_count(inputs: Mapping[str, Mapping[str, Any]]) -> int:
    snapshot = inputs["control_plane_snapshot"]
    return max(_as_int(snapshot.get("duplicate_process_count")), 1 if _duplicate_writer(inputs) else 0)


def _source_artifact_paths(config: TrackBRuntimeSafeStateEnvelopeConfig) -> dict[str, str]:
    return {
        "authority": str(config.resolve(config.output_path)),
        "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
        "runtime_resume_semantics": str(config.resolve(config.runtime_resume_semantics_path)),
        "recovery_budget_ledger": str(config.resolve(config.recovery_budget_ledger_path)),
        "recovery_attempt_history": str(config.resolve(config.recovery_attempt_history_path)),
        "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "position_truth": str(config.resolve(config.position_truth_path)),
        "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "lifecycle_summary": str(config.resolve(config.lifecycle_summary_path)),
        "ledger_summary": str(config.resolve(config.ledger_summary_path)),
        "strategy_bridge_submit_report": str(config.resolve(config.strategy_bridge_submit_report_path)),
        "broker_position_guardian": str(config.resolve(config.broker_position_guardian_path)),
    }


def _append_if_limit(
    rows: list[dict[str, Any]],
    limit_id: str,
    classification: str,
    observed: int,
    limit: int,
    reason: str,
) -> None:
    if observed > limit:
        rows.append(_limit(limit_id, classification, observed, limit, reason))


def _limit(limit_id: str, classification: str, observed: Any, limit: Any, reason: str) -> dict[str, Any]:
    return {
        "limit_id": limit_id,
        "classification": classification,
        "observed": observed,
        "limit": limit,
        "reason": reason,
    }


def _summary_count(payload: Mapping[str, Any], key: str) -> int:
    return _as_int(_mapping(payload.get("summary")).get(key))


def _first_int(*values: Any) -> int:
    for value in values:
        parsed = _as_int(value)
        if parsed:
            return parsed
    return 0


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _contains_true(value: Any, key: str) -> bool:
    if isinstance(value, Mapping):
        if value.get(key) is True:
            return True
        return any(_contains_true(item, key) for item in value.values())
    if isinstance(value, list):
        return any(_contains_true(item, key) for item in value)
    return False


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
