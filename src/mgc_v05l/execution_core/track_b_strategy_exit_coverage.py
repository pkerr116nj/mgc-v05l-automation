"""Read-only Track B strategy exit-coverage audit.

This module proves that broker-authorized entry lanes have enough managed-exit
metadata to create, track, and recover exact risk-reducing closes. It never
submits, cancels, closes, or mutates broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
)
from mgc_v05l.execution_core.track_b_shadow_promotion_contract import PROMOTION_CANDIDATES
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES,
)
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import TrackBManagedExitPolicy


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PAPER_CONFIG_IN_FORCE_PATH = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_config_in_force.json"
)
DEFAULT_EXIT_COVERAGE_REPORT_PATH = (
    Path("outputs") / "track_b_execution_core" / "strategy_exit_coverage" / "latest_strategy_exit_coverage.json"
)

EXIT_COVERAGE_COMPLETE = "EXIT_COVERAGE_COMPLETE"
EXIT_POLICY_MISSING = "EXIT_POLICY_MISSING"
EXIT_POLICY_PRESENT_ACTUATOR_MISSING = "EXIT_POLICY_PRESENT_ACTUATOR_MISSING"
EXIT_POLICY_PRESENT_BUT_NOT_AUTO_ACTUATED = "EXIT_POLICY_PRESENT_BUT_NOT_AUTO_ACTUATED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"

TRACK_B_STRATEGY_EXIT_COVERAGE_READY = "TRACK_B_STRATEGY_EXIT_COVERAGE_READY"
TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND = "TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND"

_ACTIVE_EVIDENCE_LANE_MODES = {
    "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
    "PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
    "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
    "PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE",
}
_BROKER_AUTHORIZED_SUBMIT_AUTHORITY = "PAPER_ONLY_GUARDED_RUNTIME_AFTER_PROMOTION_CONTRACT"
_SUPPORTED_TIMEBOX_POLICIES = {
    TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
    TrackBManagedExitPolicy.GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1.value,
    TrackBManagedExitPolicy.GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    TrackBManagedExitPolicy.US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    TrackBManagedExitPolicy.FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1.value,
    TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value,
}


@dataclass(frozen=True)
class TrackBStrategyExitCoverageConfig:
    repo_root: Path = REPO_ROOT
    config_in_force_path: Path = DEFAULT_PAPER_CONFIG_IN_FORCE_PATH
    output_path: Path = DEFAULT_EXIT_COVERAGE_REPORT_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_strategy_exit_coverage_report(
    *,
    config: TrackBStrategyExitCoverageConfig,
    now: datetime | None = None,
    config_in_force: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    active_config = dict(config_in_force or _read_json(config.resolve(config.config_in_force_path)))
    lanes = [dict(row) for row in active_config.get("lanes") or [] if isinstance(row, Mapping)]
    rows = [_lane_exit_coverage(row) for row in lanes if _lane_entry_enabled(row)]
    blocked_rows = [row for row in rows if row.get("classification") != EXIT_COVERAGE_COMPLETE]
    return {
        "schema_version": "track_b_strategy_exit_coverage_v1",
        "generated_at": actual_now.isoformat(),
        "classification": TRACK_B_STRATEGY_EXIT_COVERAGE_READY
        if not blocked_rows
        else TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND,
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "active_profile": active_config.get("profile"),
        "config_in_force_path": str(config.resolve(config.config_in_force_path)),
        "strategy_count": len(rows),
        "complete_strategy_count": len(rows) - len(blocked_rows),
        "blocked_strategy_count": len(blocked_rows),
        "blocked_lanes": [row.get("lane_id") for row in blocked_rows],
        "strategies": rows,
    }


def lane_exit_coverage_for(lane_id: str, report: Mapping[str, Any]) -> dict[str, Any]:
    normalized = str(lane_id or "").strip()
    for row in report.get("strategies") or []:
        if isinstance(row, Mapping) and str(row.get("lane_id") or "").strip() == normalized:
            return dict(row)
    return {
        "lane_id": normalized,
        "classification": REVIEW_REQUIRED,
        "entry_enabled": False,
        "missing_or_weak_pieces": ["exit_coverage_row_missing"],
    }


def write_track_b_strategy_exit_coverage_report(
    *, config: TrackBStrategyExitCoverageConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), dict(payload))


def exit_coverage_blocks_submit(report: Mapping[str, Any]) -> bool:
    return str(report.get("classification") or "") == TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND


def _lane_entry_enabled(row: Mapping[str, Any]) -> bool:
    lane_id = str(row.get("lane_id") or "").strip()
    if not lane_id:
        return False
    lane_mode = str(row.get("lane_mode") or "").strip().upper()
    submit_authority = str(row.get("submit_authority") or "").strip().upper()
    return lane_mode in _ACTIVE_EVIDENCE_LANE_MODES or submit_authority == _BROKER_AUTHORIZED_SUBMIT_AUTHORITY


def _lane_exit_coverage(row: Mapping[str, Any]) -> dict[str, Any]:
    lane_id = str(row.get("lane_id") or "").strip()
    adapter = lane_submit_bridge_adapter(lane_id=lane_id) or {}
    candidate = _candidate_for_lane(lane_id)
    strategy_id = str((candidate.promoted_strategy_id if candidate else row.get("strategy_id")) or "").strip()
    policy = APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES.get(strategy_id)
    template = APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES.get(strategy_id)
    exit_policy = (
        str(row.get("managed_exit_policy_id") or "").strip()
        or (candidate.lifecycle_policy_id if candidate else "")
        or (template.managed_exit_policy_id if template else "")
    )
    missing: list[str] = []
    weak: list[str] = []
    if not exit_policy:
        missing.append("explicit_exit_policy")
    if policy is None:
        missing.append("hold_exit_policy_registry_row")
    if candidate is None:
        missing.append("managed_lifecycle_registration")
    if template is None:
        missing.append("position_intent_lifecycle_template")
    if exit_policy and exit_policy not in _SUPPORTED_TIMEBOX_POLICIES:
        weak.append("unsupported_exit_policy_for_actuator")
    if not adapter:
        missing.append("submit_bridge_adapter")
    classification = _coverage_classification(exit_policy=exit_policy, missing=missing, weak=weak)
    return {
        "lane_id": lane_id,
        "strategy_id": strategy_id or None,
        "instrument": str(row.get("symbol") or row.get("instrument") or (candidate.instrument_family if candidate else "")).upper()
        or None,
        "side": (candidate.side if candidate else _side_from_lane(lane_id)),
        "entry_enabled": True,
        "exit_policy_name": exit_policy or None,
        "timebox_or_exit_trigger": _exit_trigger(exit_policy),
        "lifecycle_registration_path": "track_b_strategy_managed_paper_lifecycle"
        if candidate is not None and template is not None
        else None,
        "managed_order_registry_coverage": "track_b_managed_order_registry" if exit_policy and not weak else None,
        "managed_exit_actuator_coverage": "track_b_managed_exit_actuator" if exit_policy and not weak else None,
        "due_exits_auto_actuate_runtime_healthy": classification == EXIT_COVERAGE_COMPLETE,
        "runtime_independent_actuator_recovery": classification == EXIT_COVERAGE_COMPLETE,
        "classification": classification,
        "missing_or_weak_pieces": missing + weak,
        "broker_mutation_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _coverage_classification(*, exit_policy: str, missing: Sequence[str], weak: Sequence[str]) -> str:
    if not exit_policy:
        return EXIT_POLICY_MISSING
    if "managed_lifecycle_registration" in missing or "position_intent_lifecycle_template" in missing:
        return EXIT_POLICY_PRESENT_BUT_NOT_AUTO_ACTUATED
    if weak or "submit_bridge_adapter" in missing:
        return EXIT_POLICY_PRESENT_ACTUATOR_MISSING
    if missing:
        return REVIEW_REQUIRED
    return EXIT_COVERAGE_COMPLETE


def _candidate_for_lane(lane_id: str) -> Any:
    for candidate in PROMOTION_CANDIDATES.values():
        if candidate.lane_id == lane_id:
            return candidate
    return None


def _side_from_lane(lane_id: str) -> str | None:
    normalized = lane_id.lower()
    if normalized.endswith("_long") or "_long_" in normalized:
        return "LONG"
    if normalized.endswith("_short") or "_short_" in normalized:
        return "SHORT"
    return None


def _exit_trigger(policy_id: str) -> str | None:
    if policy_id in {
        TrackBManagedExitPolicy.GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1.value,
        TrackBManagedExitPolicy.GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    }:
        return "3_COMPLETED_5M_BARS"
    if policy_id in {
        TrackBManagedExitPolicy.US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    }:
        return "12_COMPLETED_5M_BARS"
    if policy_id == TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value:
        return "3_COMPLETED_5M_BARS"
    if policy_id == TrackBManagedExitPolicy.FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1.value:
        return "FORCED_SESSION_SEGMENT_LOCAL_EXIT"
    if policy_id == TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value:
        return "IMMEDIATE_DIAGNOSTIC_TIME_EXIT"
    return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit Track B submit lanes for managed-exit coverage.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--config-in-force-path", type=Path, default=DEFAULT_PAPER_CONFIG_IN_FORCE_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_EXIT_COVERAGE_REPORT_PATH)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBStrategyExitCoverageConfig(
        repo_root=args.repo_root.expanduser().resolve(),
        config_in_force_path=args.config_in_force_path,
        output_path=args.output_path,
    )
    payload = build_track_b_strategy_exit_coverage_report(config=config)
    write_track_b_strategy_exit_coverage_report(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"{payload['classification']}: "
            f"{payload['complete_strategy_count']}/{payload['strategy_count']} lanes complete"
        )
    return 0 if payload["classification"] == TRACK_B_STRATEGY_EXIT_COVERAGE_READY else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
