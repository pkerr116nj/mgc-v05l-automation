"""Read-only strategy hold/exit policy registry for Track B PAPER.

The registry maps approved strategy/lane identities to explicit thesis, hold,
exit, order, pyramiding, and attribution policy metadata. It is declarative
context only and does not submit, cancel, close, modify, flatten, or mutate
lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROSTER_PATH = Path("config/track_b_guarded_paper_roster.json")
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_strategy_hold_exit_policy_registry_audit.json"
)

STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY = "STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY"
STRATEGY_HOLD_EXIT_POLICY_REGISTRY_GAPS_FOUND = "STRATEGY_HOLD_EXIT_POLICY_REGISTRY_GAPS_FOUND"
STRATEGY_HOLD_EXIT_POLICY_VALID = "STRATEGY_HOLD_EXIT_POLICY_VALID"
STRATEGY_HOLD_EXIT_POLICY_INVALID = "STRATEGY_HOLD_EXIT_POLICY_INVALID"

NO_AUTHORITY_FLAGS = {
    "read_only": True,
    "shadow_only": True,
    "submit_authority": False,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "lifecycle_authority": False,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "dashboard_projection_consumed": False,
}


@dataclass(frozen=True)
class StrategyHoldExitPolicy:
    strategy_id: str
    lane_id: str
    lane_family: str
    thesis_type: str
    expected_hold_type: str
    hold_policy_id: str
    exit_policy_family: str
    order_policy_id: str
    max_hold_policy: str
    profit_harvest_policy: str | None
    thesis_failure_conditions: tuple[str, ...]
    participation_decay_inputs: tuple[str, ...]
    pyramiding_policy: str
    conflict_group: str
    eligible_for_alpha_exit_analysis: bool = True
    schema_version: str = "track_b_strategy_hold_exit_policy_v1"
    read_only: bool = True
    shadow_only: bool = True
    submit_authority: bool = False
    submit_allowed: bool = False
    broker_mutation_allowed: bool = False
    lifecycle_authority: bool = False
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False


@dataclass(frozen=True)
class StrategyHoldExitPolicyRegistryConfig:
    repo_root: Path = REPO_ROOT
    roster_path: Path = DEFAULT_ROSTER_PATH
    output_path: Path = DEFAULT_OUTPUT_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def _policy(
    *,
    strategy_id: str,
    lane_id: str,
    lane_family: str,
    thesis_type: str,
    expected_hold_type: str,
    hold_policy_id: str,
    exit_policy_family: str,
    profit_harvest_policy: str | None,
    thesis_failure_conditions: tuple[str, ...],
    participation_decay_inputs: tuple[str, ...],
    conflict_group: str,
    order_policy_id: str = "GUARDED_LIMIT_DAY_V1",
    max_hold_policy: str = "3_COMPLETED_5M_BARS",
    pyramiding_policy: str = PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
) -> StrategyHoldExitPolicy:
    return StrategyHoldExitPolicy(
        strategy_id=strategy_id,
        lane_id=lane_id,
        lane_family=lane_family,
        thesis_type=thesis_type,
        expected_hold_type=expected_hold_type,
        hold_policy_id=hold_policy_id,
        exit_policy_family=exit_policy_family,
        order_policy_id=order_policy_id,
        max_hold_policy=max_hold_policy,
        profit_harvest_policy=profit_harvest_policy,
        thesis_failure_conditions=thesis_failure_conditions,
        participation_decay_inputs=participation_decay_inputs,
        pyramiding_policy=pyramiding_policy,
        conflict_group=conflict_group,
    )


APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES: Mapping[str, StrategyHoldExitPolicy] = {
    "asian_drift_v1": _policy(
        strategy_id="asian_drift_v1",
        lane_id="mgc_example_long_lmt_day",
        lane_family="asian_drift",
        thesis_type="DRIFT",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="ASIAN_DRIFT_PARTICIPATION_HOLD_SHADOW_V1",
        exit_policy_family="TIME_BOX_PLUS_PARTICIPATION_DECAY_SHADOW",
        profit_harvest_policy="MGC_DRIFT_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("drift_context_fails", "anchor_context_fails", "opposite_participation_accelerates"),
        participation_decay_inputs=("phase1_5m_closes", "atp_participation_shadow", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": _policy(
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_asia_early_pause_resume_short",
        lane_family="pause_resume",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="PAUSE_RESUME_CONTINUATION_HOLD_SHADOW_V1",
        exit_policy_family="CONTINUATION_DECAY_OR_TIMEBOX_SHADOW",
        profit_harvest_policy="MGC_CONTINUATION_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("pause_resume_context_fails", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "participation_decay", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": _policy(
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        lane_family="breakout_retest",
        thesis_type="BREAKOUT_RETEST",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="BREAKOUT_RETEST_HOLD_SHADOW_V1",
        exit_policy_family="FAILED_RETEST_OR_PROFIT_HARVEST_SHADOW",
        profit_harvest_policy="MGC_BREAKOUT_RETEST_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("failed_retest", "breakout_level_lost", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "breakout_hold_quality", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "FIRST_BULL_SNAP_TURN_V1": _policy(
        strategy_id="FIRST_BULL_SNAP_TURN_V1",
        lane_id="mgc_first_bull_snap_turn",
        lane_family="snap_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="SNAP_TURN_QUICK_SCALP_TIMEBOX_SHADOW_V1",
        exit_policy_family="TIME_BOX_PLUS_PROFIT_HARVEST_SHADOW",
        profit_harvest_policy="MGC_SNAP_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("opposite_signal_confirms", "snap_turn_context_invalidates"),
        participation_decay_inputs=("phase1_5m_closes", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "FIRST_BEAR_SNAP_TURN_V1": _policy(
        strategy_id="FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mgc_first_bear_snap_turn",
        lane_family="snap_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="SNAP_TURN_QUICK_SCALP_TIMEBOX_SHADOW_V1",
        exit_policy_family="TIME_BOX_PLUS_PROFIT_HARVEST_SHADOW",
        profit_harvest_policy="MGC_SNAP_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("opposite_signal_confirms", "snap_turn_context_invalidates"),
        participation_decay_inputs=("phase1_5m_closes", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1": _policy(
        strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_london_late_pause_resume_short",
        lane_family="pause_resume",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="PAUSE_RESUME_CONTINUATION_HOLD_SHADOW_V1",
        exit_policy_family="CONTINUATION_DECAY_OR_TIMEBOX_SHADOW",
        profit_harvest_policy="MGC_CONTINUATION_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("pause_resume_context_fails", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "participation_decay", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1": _policy(
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
        lane_family="pause_resume",
        thesis_type="DRIFT",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="ASIA_LATE_PULLBACK_CONTINUATION_HOLD_SHADOW_V1",
        exit_policy_family="FORCED_SESSION_SEGMENT_PLUS_DECAY_SHADOW",
        profit_harvest_policy="MGC_DRIFT_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("pullback_resume_fails", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "session_segment_context", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "US_DERIVATIVE_BEAR_TURN_V1": _policy(
        strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mgc_us_derivative_bear_turn",
        lane_family="derivative_bear_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="DERIVATIVE_BEAR_TURN_DIRECTIONAL_TIMEBOX_SHADOW_V1",
        exit_policy_family="DIRECTIONAL_DECAY_OR_REVERSAL_INVALIDATION_SHADOW",
        profit_harvest_policy="MGC_DERIVATIVE_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("derivative_turn_reverses", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "derivative_turn_follow_through", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "US_LATE_PAUSE_RESUME_LONG_V1": _policy(
        strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_us_late_pause_resume_long",
        lane_family="pause_resume",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="PARTICIPATION_HOLD",
        hold_policy_id="PAUSE_RESUME_CONTINUATION_HOLD_SHADOW_V1",
        exit_policy_family="CONTINUATION_DECAY_OR_TIMEBOX_SHADOW",
        profit_harvest_policy="MGC_CONTINUATION_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("pause_resume_context_fails", "opposite_signal_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "participation_decay", "microtrend_shadow"),
        conflict_group="gold_mgc_gc",
    ),
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": _policy(
        strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mnq_us_derivative_bear_turn",
        lane_family="derivative_bear_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="DERIVATIVE_BEAR_TURN_DIRECTIONAL_TIMEBOX_SHADOW_V1",
        exit_policy_family="DIRECTIONAL_DECAY_OR_REVERSAL_INVALIDATION_SHADOW",
        profit_harvest_policy="MNQ_DERIVATIVE_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("derivative_turn_reverses", "opposite_snap_turn_confirms"),
        participation_decay_inputs=("phase1_5m_closes", "derivative_turn_follow_through", "microtrend_shadow"),
        conflict_group="equity_index_nasdaq_mnq_nq",
    ),
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": _policy(
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mnq_first_bear_snap_turn",
        lane_family="snap_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="SNAP_TURN_QUICK_SCALP_TIMEBOX_SHADOW_V1",
        exit_policy_family="TIME_BOX_PLUS_PROFIT_HARVEST_SHADOW",
        profit_harvest_policy="MNQ_SNAP_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("opposite_snap_turn_confirms", "snap_turn_context_invalidates"),
        participation_decay_inputs=("phase1_5m_closes", "microtrend_shadow"),
        conflict_group="equity_index_nasdaq_mnq_nq",
    ),
    "MNQ_FIRST_BULL_SNAP_TURN_V1": _policy(
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        lane_id="mnq_first_bull_snap_turn",
        lane_family="snap_turn",
        thesis_type="SNAP_TURN",
        expected_hold_type="QUICK_SCALP",
        hold_policy_id="SNAP_TURN_QUICK_SCALP_TIMEBOX_SHADOW_V1",
        exit_policy_family="TIME_BOX_PLUS_PROFIT_HARVEST_SHADOW",
        profit_harvest_policy="MNQ_SNAP_TURN_PROFIT_HARVEST_SHADOW_V1",
        thesis_failure_conditions=("opposite_snap_turn_confirms", "snap_turn_context_invalidates"),
        participation_decay_inputs=("phase1_5m_closes", "microtrend_shadow"),
        conflict_group="equity_index_nasdaq_mnq_nq",
    ),
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1": _policy(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
        lane_id="mnq_london_open_active_participation_long",
        lane_family="paper_active_evidence",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="TIMEBOXED",
        hold_policy_id="LONDON_OPEN_ACTIVE_EVIDENCE_60M_TIMEBOX_HOLD_V1",
        exit_policy_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_ONLY",
        profit_harvest_policy=None,
        thesis_failure_conditions=("price_loses_london_open_reference", "opposite_recent_close_confirms"),
        participation_decay_inputs=("phase1_1m_closes", "london_0300_open_reference", "vwap_if_available"),
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        max_hold_policy="12_COMPLETED_5M_BARS",
    ),
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1": _policy(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        lane_id="mnq_london_open_active_participation_short",
        lane_family="paper_active_evidence",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="TIMEBOXED",
        hold_policy_id="LONDON_OPEN_ACTIVE_EVIDENCE_60M_TIMEBOX_HOLD_V1",
        exit_policy_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_ONLY",
        profit_harvest_policy=None,
        thesis_failure_conditions=("price_recovers_london_open_reference", "opposite_recent_close_confirms"),
        participation_decay_inputs=("phase1_1m_closes", "london_0300_open_reference", "vwap_if_available"),
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        max_hold_policy="12_COMPLETED_5M_BARS",
    ),
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1": _policy(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
        lane_id="mes_london_open_active_participation_long",
        lane_family="paper_active_evidence",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="TIMEBOXED",
        hold_policy_id="LONDON_OPEN_ACTIVE_EVIDENCE_60M_TIMEBOX_HOLD_V1",
        exit_policy_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_ONLY",
        profit_harvest_policy=None,
        thesis_failure_conditions=("price_loses_london_open_reference", "opposite_recent_close_confirms"),
        participation_decay_inputs=("phase1_1m_closes", "london_0300_open_reference", "vwap_if_available"),
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        max_hold_policy="12_COMPLETED_5M_BARS",
    ),
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1": _policy(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        lane_id="mes_london_open_active_participation_short",
        lane_family="paper_active_evidence",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="TIMEBOXED",
        hold_policy_id="LONDON_OPEN_ACTIVE_EVIDENCE_60M_TIMEBOX_HOLD_V1",
        exit_policy_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_ONLY",
        profit_harvest_policy=None,
        thesis_failure_conditions=("price_recovers_london_open_reference", "opposite_recent_close_confirms"),
        participation_decay_inputs=("phase1_1m_closes", "london_0300_open_reference", "vwap_if_available"),
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        max_hold_policy="12_COMPLETED_5M_BARS",
    ),
    "GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1": _policy(
        strategy_id="GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1",
        lane_id="globex_reopen_mnq_1m_strong_green_second_candle_confirm_shadow",
        lane_family="globex_reopen_first_candle_continuation_shadow",
        thesis_type="TREND_PARTICIPATION",
        expected_hold_type="TIMEBOXED",
        hold_policy_id="GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_HOLD_SHADOW_V1",
        exit_policy_family="GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_EXIT_SHADOW",
        profit_harvest_policy=None,
        thesis_failure_conditions=("second_candle_rejects_first_candle", "first_candle_not_strong_green"),
        participation_decay_inputs=("phase1_1m_closes", "globex_reopen_session_open", "second_candle_confirmation"),
        conflict_group="equity_index_nasdaq_mnq_nq",
        max_hold_policy="12_COMPLETED_5M_BARS",
    ),
}


REQUIRED_POLICY_FIELDS = (
    "strategy_id",
    "lane_id",
    "lane_family",
    "thesis_type",
    "expected_hold_type",
    "hold_policy_id",
    "exit_policy_family",
    "order_policy_id",
    "max_hold_policy",
    "thesis_failure_conditions",
    "participation_decay_inputs",
    "pyramiding_policy",
    "conflict_group",
)


def strategy_hold_exit_policy_for(strategy_id: str) -> dict[str, Any] | None:
    policy = APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES.get(strategy_id)
    return _policy_payload(policy) if policy else None


def validate_strategy_hold_exit_policy(policy: StrategyHoldExitPolicy | Mapping[str, Any] | None) -> dict[str, Any]:
    payload = _policy_payload(policy)
    missing = [field for field in REQUIRED_POLICY_FIELDS if _empty(payload.get(field))]
    invalid: list[str] = []
    for field in ("submit_authority", "submit_allowed", "broker_mutation_allowed", "lifecycle_authority"):
        if payload.get(field) is not False:
            invalid.append(field)
    if payload.get("live_money_eligible") is not False:
        invalid.append("live_money_eligible")
    if payload.get("paper_proof_invoked") is not False:
        invalid.append("paper_proof_invoked")
    classification = STRATEGY_HOLD_EXIT_POLICY_VALID if not missing and not invalid else STRATEGY_HOLD_EXIT_POLICY_INVALID
    return {
        "classification": classification,
        "valid": classification == STRATEGY_HOLD_EXIT_POLICY_VALID,
        "strategy_id": payload.get("strategy_id"),
        "lane_id": payload.get("lane_id"),
        "missing_metadata": missing,
        "invalid_metadata": invalid,
        **NO_AUTHORITY_FLAGS,
    }


def build_strategy_hold_exit_policy_registry_audit(
    *,
    config: StrategyHoldExitPolicyRegistryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    roster = _read_json(config.resolve(config.roster_path))
    enabled_strategy_ids = tuple(str(item) for item in roster.get("enabled_strategy_ids") or [])
    strategy_ids = enabled_strategy_ids or tuple(APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES)
    rows: list[dict[str, Any]] = []
    for strategy_id in strategy_ids:
        policy = APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES.get(strategy_id)
        if policy is None:
            rows.append(
                {
                    "classification": STRATEGY_HOLD_EXIT_POLICY_INVALID,
                    "valid": False,
                    "strategy_id": strategy_id,
                    "lane_id": None,
                    "missing_metadata": ["strategy_hold_exit_policy_mapping"],
                    "invalid_metadata": [],
                    **NO_AUTHORITY_FLAGS,
                }
            )
            continue
        validation = validate_strategy_hold_exit_policy(policy)
        rows.append({**validation, "strategy_hold_exit_policy": _policy_payload(policy)})

    gap_count = sum(1 for row in rows if row.get("valid") is not True)
    classification = (
        STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY
        if gap_count == 0
        else STRATEGY_HOLD_EXIT_POLICY_REGISTRY_GAPS_FOUND
    )
    return {
        "schema_version": "track_b_strategy_hold_exit_policy_registry_audit_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        **NO_AUTHORITY_FLAGS,
        "roster_path": str(config.resolve(config.roster_path)),
        "strategy_count": len(rows),
        "valid_strategy_count": len(rows) - gap_count,
        "gap_strategy_count": gap_count,
        "strategies": rows,
        "integration_targets": {
            "position_intent_contract": {
                "mode": "read_only_context",
                "fields": ["hold_policy_id", "exit_policy_family", "order_policy_id"],
            },
            "hold_exit_shadow_engine": {
                "mode": "read_only_context",
                "fields": ["thesis_failure_conditions", "participation_decay_inputs", "profit_harvest_policy"],
            },
            "exit_attribution_policy_v2": {
                "mode": "read_only_context",
                "fields": ["eligible_for_alpha_exit_analysis", "exit_policy_family", "hold_policy_id"],
            },
        },
        "artifact_paths": {
            "audit": str(config.resolve(config.output_path)),
            "docs": str(config.repo_root / "docs/track_b_strategy_hold_exit_policy_registry.md"),
        },
    }


def write_strategy_hold_exit_policy_registry_audit(
    *,
    config: StrategyHoldExitPolicyRegistryConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, payload)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit read-only Track B strategy hold/exit policy registry.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--roster-path", default=str(DEFAULT_ROSTER_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = StrategyHoldExitPolicyRegistryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        roster_path=Path(args.roster_path),
        output_path=Path(args.output_path),
    )
    payload = build_strategy_hold_exit_policy_registry_audit(config=config)
    write_strategy_hold_exit_policy_registry_audit(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']}: {payload['valid_strategy_count']}/{payload['strategy_count']} policies valid")
    return 0 if payload["classification"] == STRATEGY_HOLD_EXIT_POLICY_REGISTRY_READY else 2


def _policy_payload(policy: StrategyHoldExitPolicy | Mapping[str, Any] | None) -> dict[str, Any]:
    if policy is None:
        return {}
    if isinstance(policy, Mapping):
        return dict(policy)
    return asdict(policy)


def _empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return len(value) == 0
    return False


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
