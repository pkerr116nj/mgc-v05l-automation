"""Synthetic Track B lifecycle stress framework.

The framework drives high-volume fake trade lifecycles through the existing
read-only architecture:

Lifecycle-style synthetic events -> Central Trade Registry -> Canonical Truth
Snapshot -> shadow-report-shaped rows.

It never imports broker adapters, never starts a runtime, and never performs
broker-side order or position actions.
"""

from __future__ import annotations

import json
import random
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    CONTRACT_ENTRY_CLOSE_ONLY,
    CONTROL_PLANE_STALE,
    FILL_NOT_BROKER_BACKED,
    PLANNER_SNAPSHOT_MISMATCH,
    SAFE_STATE_SUBMIT_BLOCKED,
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    TradeRegistryModelError,
    TradeRegistryRecord,
    reduce_trade_events,
)
from mgc_v05l.execution_core.track_b_gate_shadow_parity import (
    TrackBGateName,
    TrackBGateShadowContext,
    evaluate_track_b_gate_shadow_parity,
)
from mgc_v05l.execution_core.track_b_trade_registry_shadow_report import TradeRegistryShadowRow


SCHEMA_VERSION = "track_b_lifecycle_stress_framework_v1"
DEFAULT_STRESS_REPORT_PATH = (
    Path("outputs") / "track_b_execution_core" / "lifecycle_stress" / "latest_lifecycle_stress_report.json"
)
STRESS_PASSED = "STRESS_PASSED"
STRESS_EXPECTED_REVIEW_REQUIRED = "STRESS_EXPECTED_REVIEW_REQUIRED"
STRESS_REVIEW_REQUIRED = "STRESS_REVIEW_REQUIRED"
STRESS_INVARIANT_FAILED = "STRESS_INVARIANT_FAILED"
STRESS_REDUCER_ERROR = "STRESS_REDUCER_ERROR"
_GATE_SHADOW_SAFETY_REASONS = {
    "TRUTH_CONFLICT_REVIEW_REQUIRED",
    "BROKER_LIFECYCLE_RECONCILIATION_NOT_CLEAN",
    "BROKER_LIFECYCLE_RECONCILIATION_DIRTY",
    "SAFE_STATE_SUBMIT_BLOCKED",
    "CONTROL_PLANE_STALE",
    "OPEN_WORKING_ORDER_PRESENT",
    "DUPLICATE_STRATEGY_ENTRY_WHILE_POSITION_OPEN",
    "REGISTRY_ENTRY_NOT_BROKER_BACKED",
    "REGISTRY_TRADE_ID_NOT_OPEN_MANAGED",
    "FILL_NOT_BROKER_BACKED",
}


class LifecycleStressRunMode(str, Enum):
    SMOKE = "smoke"
    KNOWN_SCENARIOS = "known_scenarios"
    LANE_MATRIX = "lane_matrix"
    FUZZ = "fuzz"


class LifecycleStressScenario(str, Enum):
    CLEAN_FULL_LIFECYCLE = "clean_full_lifecycle"
    PASSIVE_ENTRY_CANCEL = "passive_entry_cancel"
    ENTRY_FILL_NOT_ADOPTED = "entry_fill_not_adopted"
    RECOVERY_ADOPTION = "recovery_adoption"
    MANUAL_OPERATOR_CLOSE = "manual_operator_close"
    MANAGED_EXIT_DUE_CLOSE = "managed_exit_due_close"
    STALE_CONTROL_PLANE = "stale_control_plane"
    SAFE_STATE_BLOCKED = "safe_state_blocked"
    PLANNER_SNAPSHOT_MISMATCH = "planner_snapshot_mismatch"
    SCOPED_CLEANUP_EXTRA_DIAGNOSTIC_FIELDS = "scoped_cleanup_extra_diagnostic_fields"
    MISSING_PERM_OR_EXEC = "missing_perm_id_exec_id"
    WRONG_CONTRACT_IDENTITY = "wrong_conId_localSymbol"
    WRONG_LIFECYCLE_ID = "wrong_lifecycle_id"
    DUPLICATE_FILL = "duplicate_fill"
    DUPLICATE_CLOSE = "duplicate_close"
    CONTRACT_CLOSE_ONLY = "contract_close_only_entry_blocked_exit_allowed"
    AMBIGUOUS_RECONSTRUCTION = "ambiguous_reconstruction"
    RECONCILIATION_AMBIGUITY = "reconciliation_ambiguity"
    STALE_ARTIFACT_RESURRECTION = "stale_artifact_resurrection"
    RECOVERY_REGISTRY_BACKED_RESUME = "recovery_registry_backed_resume"
    RECOVERY_MISSING_BROKER_EVIDENCE = "recovery_missing_broker_backed_evidence"
    RECOVERY_AMBIGUOUS_TRADE_IDS = "recovery_ambiguous_trade_ids"
    RECOVERY_LIFECYCLE_OWNER_CONFLICT = "recovery_lifecycle_owner_conflict"
    RECOVERY_STALE_HISTORY_NO_ADOPT = "recovery_stale_history_no_adopt"
    GATE_DUPLICATE_ENTRY_BLOCKED = "gate_duplicate_entry_blocked"
    GATE_OPEN_WORKING_ORDER_BLOCKED = "gate_open_working_order_blocked"


@dataclass(frozen=True)
class StressLane:
    lane_id: str
    thesis_strategy_id: str
    symbol: str
    con_id: int
    local_symbol: str
    expiry: str
    side: str
    entry_action: str
    exit_action: str
    session: str


DEFAULT_LANES: tuple[StressLane, ...] = (
    StressLane("mnq_us_active_participation_long", "mnq_us_active_participation_long", "MNQ", 770561201, "MNQM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "US"),
    StressLane("mnq_us_active_participation_short", "mnq_us_active_participation_short", "MNQ", 770561201, "MNQM6", "202606", "SHORT", "SELL_TO_OPEN", "BUY_TO_CLOSE", "US"),
    StressLane("mes_us_active_participation_long", "mes_us_active_participation_long", "MES", 770561194, "MESM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "US"),
    StressLane("mes_us_active_participation_short", "mes_us_active_participation_short", "MES", 770561194, "MESM6", "202606", "SHORT", "SELL_TO_OPEN", "BUY_TO_CLOSE", "US"),
    StressLane("mnq_globex_active_participation_long", "mnq_globex_active_participation_long", "MNQ", 770561201, "MNQM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "GLOBEX"),
    StressLane("mnq_globex_active_participation_short", "mnq_globex_active_participation_short", "MNQ", 770561201, "MNQM6", "202606", "SHORT", "SELL_TO_OPEN", "BUY_TO_CLOSE", "GLOBEX"),
    StressLane("mes_globex_active_participation_long", "mes_globex_active_participation_long", "MES", 770561194, "MESM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "GLOBEX"),
    StressLane("mes_globex_active_participation_short", "mes_globex_active_participation_short", "MES", 770561194, "MESM6", "202606", "SHORT", "SELL_TO_OPEN", "BUY_TO_CLOSE", "GLOBEX"),
    StressLane("mgc_contract_close_only_fixture", "mgc_contract_close_only_fixture", "MGC", 123456, "MGCM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "CONTRACT_CLOSE_ONLY"),
    StressLane("gc_contract_close_only_fixture", "gc_contract_close_only_fixture", "GC", 654321, "GCM6", "202606", "LONG", "BUY_TO_OPEN", "SELL_TO_CLOSE", "CONTRACT_CLOSE_ONLY"),
)


DEFAULT_SCENARIO_MIX: tuple[LifecycleStressScenario, ...] = tuple(LifecycleStressScenario)
EXPECTED_REVIEW_SCENARIOS = {
    LifecycleStressScenario.ENTRY_FILL_NOT_ADOPTED,
    LifecycleStressScenario.STALE_CONTROL_PLANE,
    LifecycleStressScenario.SAFE_STATE_BLOCKED,
    LifecycleStressScenario.PLANNER_SNAPSHOT_MISMATCH,
    LifecycleStressScenario.MISSING_PERM_OR_EXEC,
    LifecycleStressScenario.WRONG_CONTRACT_IDENTITY,
    LifecycleStressScenario.WRONG_LIFECYCLE_ID,
    LifecycleStressScenario.DUPLICATE_FILL,
    LifecycleStressScenario.DUPLICATE_CLOSE,
    LifecycleStressScenario.CONTRACT_CLOSE_ONLY,
    LifecycleStressScenario.AMBIGUOUS_RECONSTRUCTION,
    LifecycleStressScenario.RECONCILIATION_AMBIGUITY,
    LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION,
    LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
    LifecycleStressScenario.RECOVERY_AMBIGUOUS_TRADE_IDS,
    LifecycleStressScenario.RECOVERY_LIFECYCLE_OWNER_CONFLICT,
    LifecycleStressScenario.RECOVERY_STALE_HISTORY_NO_ADOPT,
    LifecycleStressScenario.GATE_DUPLICATE_ENTRY_BLOCKED,
    LifecycleStressScenario.GATE_OPEN_WORKING_ORDER_BLOCKED,
}
IMPOSSIBLE_STATE_INVARIANTS = {
    "CLOSED_FLAT_WITH_OPEN_QTY",
    "OPEN_MANAGED_WITHOUT_BROKER_BACKED_ENTRY",
    "EXIT_FILL_WITHOUT_OPEN_LIFECYCLE",
}
DEFAULT_COUNTS_BY_MODE = {
    LifecycleStressRunMode.SMOKE: 20,
    LifecycleStressRunMode.KNOWN_SCENARIOS: 260,
    LifecycleStressRunMode.LANE_MATRIX: 1000,
    LifecycleStressRunMode.FUZZ: 10000,
}


@dataclass(frozen=True)
class LifecycleStressConfig:
    mode: LifecycleStressRunMode = LifecycleStressRunMode.LANE_MATRIX
    count: int | None = None
    seed: int = 20260531
    lanes: tuple[StressLane, ...] = DEFAULT_LANES
    scenario_mix: tuple[LifecycleStressScenario, ...] = DEFAULT_SCENARIO_MIX
    generated_at: datetime = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
    output_path: Path = DEFAULT_STRESS_REPORT_PATH

    @property
    def effective_count(self) -> int:
        return self.count if self.count is not None else DEFAULT_COUNTS_BY_MODE[self.mode]


@dataclass(frozen=True)
class LifecycleStressResult:
    index: int
    scenario: str
    lane_id: str
    trade_id: str
    classification: str
    registry_state: str | None
    truth_classification: str | None
    truth_reason_codes: tuple[str, ...]
    truth_conflicts: tuple[str, ...]
    broker_backed_entry: bool
    broker_backed_exit: bool
    expected_review_required: bool
    expected_bad_lifecycle_classified: bool
    invariant_failures: tuple[str, ...]
    unexpected_invariant_failures: tuple[str, ...]
    impossible_states: tuple[str, ...]
    silent_ambiguity_merge: bool
    reducer_error: str | None
    gate_shadow_summary: Mapping[str, Any]
    gate_shadow_report: Mapping[str, Any] | None
    shadow_row: Mapping[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "scenario": self.scenario,
            "lane_id": self.lane_id,
            "trade_id": self.trade_id,
            "classification": self.classification,
            "registry_state": self.registry_state,
            "truth_classification": self.truth_classification,
            "truth_reason_codes": list(self.truth_reason_codes),
            "truth_conflicts": list(self.truth_conflicts),
            "broker_backed_entry": self.broker_backed_entry,
            "broker_backed_exit": self.broker_backed_exit,
            "expected_review_required": self.expected_review_required,
            "expected_bad_lifecycle_classified": self.expected_bad_lifecycle_classified,
            "invariant_failures": list(self.invariant_failures),
            "unexpected_invariant_failures": list(self.unexpected_invariant_failures),
            "impossible_states": list(self.impossible_states),
            "silent_ambiguity_merge": self.silent_ambiguity_merge,
            "reducer_error": self.reducer_error,
            "gate_shadow_summary": dict(self.gate_shadow_summary),
            "gate_shadow_report": dict(self.gate_shadow_report or {}),
            "shadow_row": dict(self.shadow_row or {}),
        }


@dataclass(frozen=True)
class LifecycleStressReport:
    schema_version: str
    generated_at: datetime
    mode: str
    seed: int
    requested_count: int
    read_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    results: tuple[LifecycleStressResult, ...]
    trade_id_collisions: tuple[str, ...]

    @property
    def summary(self) -> dict[str, Any]:
        scenario_breakdown: dict[str, dict[str, int]] = {}
        for result in self.results:
            row = scenario_breakdown.setdefault(
                result.scenario,
                {
                    "total": 0,
                    "passed": 0,
                    "expected_review_required": 0,
                    "review_required": 0,
                    "unexpected_invariant_failures": 0,
                    "reducer_errors": 0,
                    "silent_ambiguity_merges": 0,
                    "impossible_states": 0,
                    "gate_shadow_checks": 0,
                    "gate_shadow_green_path_checks": 0,
                    "gate_shadow_blocked_checks": 0,
                    "gate_shadow_mismatches": 0,
                    "gate_shadow_safety_regressions": 0,
                    "gate_shadow_missing_trade_id_blocks": 0,
                },
            )
            row["total"] += 1
            if result.classification == STRESS_PASSED:
                row["passed"] += 1
            if result.classification == STRESS_EXPECTED_REVIEW_REQUIRED:
                row["expected_review_required"] += 1
            if result.classification == STRESS_REVIEW_REQUIRED:
                row["review_required"] += 1
            if result.unexpected_invariant_failures:
                row["unexpected_invariant_failures"] += 1
            if result.reducer_error:
                row["reducer_errors"] += 1
            if result.silent_ambiguity_merge:
                row["silent_ambiguity_merges"] += 1
            if result.impossible_states:
                row["impossible_states"] += 1
            row["gate_shadow_checks"] += int(result.gate_shadow_summary.get("total_gate_checks") or 0)
            row["gate_shadow_green_path_checks"] += int(result.gate_shadow_summary.get("green_path_checks") or 0)
            row["gate_shadow_blocked_checks"] += int(result.gate_shadow_summary.get("blocked_checks") or 0)
            row["gate_shadow_mismatches"] += int(result.gate_shadow_summary.get("mismatches") or 0)
            row["gate_shadow_safety_regressions"] += int(result.gate_shadow_summary.get("safety_regressions") or 0)
            row["gate_shadow_missing_trade_id_blocks"] += int(result.gate_shadow_summary.get("missing_trade_id_blocks") or 0)
        gate_mismatches_by_gate: dict[str, int] = {}
        gate_mismatches_by_lane_scenario: dict[str, int] = {}
        for result in self.results:
            for gate, count in dict(result.gate_shadow_summary.get("mismatches_by_gate") or {}).items():
                gate_mismatches_by_gate[str(gate)] = gate_mismatches_by_gate.get(str(gate), 0) + int(count)
            if int(result.gate_shadow_summary.get("mismatches") or 0):
                key = f"{result.scenario}|{result.lane_id}"
                gate_mismatches_by_lane_scenario[key] = gate_mismatches_by_lane_scenario.get(key, 0) + int(result.gate_shadow_summary.get("mismatches") or 0)
        return {
            "total_trades": len(self.results),
            "passed": sum(1 for result in self.results if result.classification == STRESS_PASSED),
            "expected_review_required": sum(
                1 for result in self.results if result.classification == STRESS_EXPECTED_REVIEW_REQUIRED
            ),
            "review_required": sum(1 for result in self.results if result.classification == STRESS_REVIEW_REQUIRED),
            "conflicts": sum(1 for result in self.results if result.truth_conflicts),
            "unexpected_invariant_failures": sum(1 for result in self.results if result.unexpected_invariant_failures),
            "reducer_crashes": sum(1 for result in self.results if result.reducer_error),
            "trade_id_collisions": len(self.trade_id_collisions),
            "silent_ambiguity_merges": sum(1 for result in self.results if result.silent_ambiguity_merge),
            "impossible_states": sum(1 for result in self.results if result.impossible_states),
            "bad_lifecycles_without_reason_codes": sum(
                1 for result in self.results if result.expected_review_required and not result.expected_bad_lifecycle_classified
            ),
            "gate_shadow_total_checks": sum(int(result.gate_shadow_summary.get("total_gate_checks") or 0) for result in self.results),
            "gate_shadow_green_path_checks": sum(int(result.gate_shadow_summary.get("green_path_checks") or 0) for result in self.results),
            "gate_shadow_blocked_checks": sum(int(result.gate_shadow_summary.get("blocked_checks") or 0) for result in self.results),
            "gate_shadow_mismatches": sum(int(result.gate_shadow_summary.get("mismatches") or 0) for result in self.results),
            "gate_shadow_safety_regressions": sum(int(result.gate_shadow_summary.get("safety_regressions") or 0) for result in self.results),
            "gate_shadow_missing_trade_id_blocks": sum(int(result.gate_shadow_summary.get("missing_trade_id_blocks") or 0) for result in self.results),
            "gate_shadow_mismatches_by_gate": gate_mismatches_by_gate,
            "gate_shadow_mismatches_by_lane_scenario": gate_mismatches_by_lane_scenario,
            "stage_goal_zero_failures": {
                "zero_crashes": not any(result.reducer_error for result in self.results),
                "zero_silent_merges": not any(result.silent_ambiguity_merge for result in self.results),
                "zero_trade_id_collisions": not self.trade_id_collisions,
                "zero_impossible_states": not any(result.impossible_states for result in self.results),
                "every_bad_lifecycle_has_reason_codes": not any(
                    result.expected_review_required and not result.expected_bad_lifecycle_classified
                    for result in self.results
                ),
                "zero_gate_shadow_mismatches": not any(int(result.gate_shadow_summary.get("mismatches") or 0) for result in self.results),
                "zero_gate_shadow_safety_regressions": not any(int(result.gate_shadow_summary.get("safety_regressions") or 0) for result in self.results),
                "zero_gate_shadow_missing_trade_id_blocks": not any(int(result.gate_shadow_summary.get("missing_trade_id_blocks") or 0) for result in self.results),
            },
            "scenario_breakdown": scenario_breakdown,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "mode": self.mode,
            "seed": self.seed,
            "requested_count": self.requested_count,
            "read_only": self.read_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "summary": self.summary,
            "trade_id_collisions": list(self.trade_id_collisions),
            "results": [result.to_dict() for result in self.results],
        }


def run_lifecycle_stress(config: LifecycleStressConfig | None = None) -> LifecycleStressReport:
    actual_config = config or LifecycleStressConfig()
    count = actual_config.effective_count
    if count <= 0:
        raise ValueError("count must be positive.")
    if not actual_config.lanes:
        raise ValueError("at least one lane is required.")
    if not actual_config.scenario_mix:
        raise ValueError("at least one scenario is required.")

    rng = random.Random(actual_config.seed)
    seen_trade_ids: set[str] = set()
    collisions: list[str] = []
    results: list[LifecycleStressResult] = []
    with tempfile.TemporaryDirectory(prefix="track_b_lifecycle_stress_") as temp_dir:
        root = Path(temp_dir)
        scheduled = tuple(
            (scenario, _lane_for_scenario(lane, scenario))
            for scenario, lane in _schedule(
                mode=actual_config.mode,
                count=count,
                scenario_mix=actual_config.scenario_mix,
                lanes=actual_config.lanes,
                rng=rng,
            )
        )
        for index, (scenario, lane) in enumerate(scheduled):
            trade_id = _trade_id(index=index, scenario=scenario, lane=lane)
            if trade_id in seen_trade_ids:
                collisions.append(trade_id)
            seen_trade_ids.add(trade_id)
            results.append(
                _run_one(
                    index=index,
                    scenario=scenario,
                    lane=lane,
                    trade_id=trade_id,
                    root=root / f"{index:06d}_{scenario.value}",
                    generated_at=actual_config.generated_at + timedelta(seconds=index),
                )
            )

    return LifecycleStressReport(
        schema_version=SCHEMA_VERSION,
        generated_at=_ensure_utc(actual_config.generated_at),
        mode=actual_config.mode.value,
        seed=actual_config.seed,
        requested_count=count,
        read_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        results=tuple(results),
        trade_id_collisions=tuple(collisions),
    )


def write_lifecycle_stress_report(path: Path, report: LifecycleStressReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _schedule(
    *,
    mode: LifecycleStressRunMode,
    count: int,
    scenario_mix: Sequence[LifecycleStressScenario],
    lanes: Sequence[StressLane],
    rng: random.Random,
) -> tuple[tuple[LifecycleStressScenario, StressLane], ...]:
    if mode == LifecycleStressRunMode.SMOKE:
        smoke_scenarios = (
            LifecycleStressScenario.CLEAN_FULL_LIFECYCLE,
            LifecycleStressScenario.PASSIVE_ENTRY_CANCEL,
            LifecycleStressScenario.MISSING_PERM_OR_EXEC,
            LifecycleStressScenario.CONTRACT_CLOSE_ONLY,
        )
        return tuple((smoke_scenarios[index % len(smoke_scenarios)], lanes[index % len(lanes)]) for index in range(count))
    if mode == LifecycleStressRunMode.KNOWN_SCENARIOS:
        scenarios = tuple(scenario_mix)
        return tuple((scenarios[index % len(scenarios)], lanes[index % len(lanes)]) for index in range(count))
    if mode == LifecycleStressRunMode.LANE_MATRIX:
        scenarios = tuple(scenario_mix)
        return tuple((scenarios[index % len(scenarios)], lanes[(index // len(scenarios)) % len(lanes)]) for index in range(count))
    return tuple((rng.choice(tuple(scenario_mix)), rng.choice(tuple(lanes))) for _ in range(count))


def _run_one(
    *,
    index: int,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    trade_id: str,
    root: Path,
    generated_at: datetime,
) -> LifecycleStressResult:
    events = _events_for_scenario(index=index, scenario=scenario, lane=lane, trade_id=trade_id, generated_at=generated_at)
    reducer_error: str | None = None
    record: TradeRegistryRecord | None = None
    try:
        record = reduce_trade_events(events)
    except TradeRegistryModelError as exc:
        reducer_error = str(exc)

    truth = _truth_for_scenario(root=root, scenario=scenario, lane=lane, record=record, generated_at=generated_at)
    gate_shadow_report = _gate_shadow_for_scenario(
        root=root,
        scenario=scenario,
        lane=lane,
        trade_id=trade_id,
        record=record,
        generated_at=generated_at,
    )
    gate_shadow_summary = _gate_shadow_summary(gate_shadow_report)
    invariants = _invariant_failures(record=record, events=events, scenario=scenario)
    unexpected_invariants = _unexpected_invariant_failures(scenario=scenario, invariants=invariants)
    impossible_states = tuple(item for item in invariants if item in IMPOSSIBLE_STATE_INVARIANTS)
    silent_ambiguity_merge = _silent_ambiguity_merge(record=record, scenario=scenario)
    expected_review_required = scenario in EXPECTED_REVIEW_SCENARIOS
    expected_bad_classified = _expected_bad_lifecycle_classified(
        record=record,
        truth=truth,
        expected_review_required=expected_review_required,
    )
    classification = _classification(
        record=record,
        expected_review_required=expected_review_required,
        unexpected_invariants=unexpected_invariants,
        impossible_states=impossible_states,
        silent_ambiguity_merge=silent_ambiguity_merge,
        reducer_error=reducer_error,
    )
    shadow_row = _shadow_row(record=record, truth=truth) if record is not None else None

    return LifecycleStressResult(
        index=index,
        scenario=scenario.value,
        lane_id=lane.lane_id,
        trade_id=trade_id,
        classification=classification,
        registry_state=record.current_state.value if record else None,
        truth_classification=truth.classification,
        truth_reason_codes=truth.reason_codes,
        truth_conflicts=tuple(conflict.classification for conflict in truth.conflicts),
        broker_backed_entry=record.broker_backed_entry if record else False,
        broker_backed_exit=record.broker_backed_exit if record else False,
        expected_review_required=expected_review_required,
        expected_bad_lifecycle_classified=expected_bad_classified,
        invariant_failures=invariants,
        unexpected_invariant_failures=unexpected_invariants,
        impossible_states=impossible_states,
        silent_ambiguity_merge=silent_ambiguity_merge,
        reducer_error=reducer_error,
        gate_shadow_summary=gate_shadow_summary,
        gate_shadow_report=gate_shadow_report.to_dict() if gate_shadow_report is not None else None,
        shadow_row=shadow_row.to_dict() if shadow_row else None,
    )


def _events_for_scenario(
    *,
    index: int,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    trade_id: str,
    generated_at: datetime,
) -> tuple[TradeEvent, ...]:
    lifecycle_id = f"lifecycle_{trade_id}"
    entry_order_id = str(100000 + index)
    exit_order_id = str(200000 + index)
    entry_perm_id = str(300000 + index)
    exit_perm_id = str(400000 + index)
    entry_exec_id = f"exec.entry.{index:06d}"
    exit_exec_id = f"exec.exit.{index:06d}"

    def event(
        event_type: TradeEventType,
        *,
        seconds: int,
        action: str | None = None,
        lifecycle: str | None = lifecycle_id,
        con_id: int | None = None,
        local_symbol: str | None = None,
        order_id: str | None = None,
        perm_id: str | None = None,
        exec_id: str | None = None,
        price: str | None = None,
        reason_codes: Sequence[str] = (),
    ) -> TradeEvent:
        return TradeEvent(
            event_id=f"{trade_id}_{seconds:03d}_{event_type.value}",
            event_type=event_type,
            generated_at=generated_at + timedelta(seconds=seconds),
            trade_id=trade_id,
            lifecycle_id=lifecycle,
            lane_id=lane.lane_id,
            thesis_strategy_id=lane.thesis_strategy_id,
            account_id="DUM882026",
            symbol=lane.symbol,
            con_id=con_id or lane.con_id,
            local_symbol=local_symbol or lane.local_symbol,
            expiry=lane.expiry,
            side=lane.side,
            action=action or lane.entry_action,
            qty=Decimal("1"),
            source_artifact_path=f"synthetic://stress/{scenario.value}/{trade_id}",
            order_id=order_id,
            client_id="17" if order_id else None,
            perm_id=perm_id,
            exec_id=exec_id,
            price=Decimal(price) if price is not None else None,
            reason_codes=tuple(reason_codes),
            metadata={"scenario": scenario.value, "lane_session": lane.session},
        )

    if scenario == LifecycleStressScenario.PASSIVE_ENTRY_CANCEL:
        return (
            event(TradeEventType.ENTRY_INTENT_CREATED, seconds=0),
            event(TradeEventType.ENTRY_ORDER_SUBMITTED, seconds=1, order_id=entry_order_id),
            event(
                TradeEventType.ENTRY_ORDER_CANCELLED,
                seconds=60,
                order_id=entry_order_id,
                reason_codes=("PASSIVE_LIMIT_TIMEOUT_EXPECTED",),
            ),
        )
    if scenario in {
        LifecycleStressScenario.RECOVERY_ADOPTION,
        LifecycleStressScenario.RECOVERY_REGISTRY_BACKED_RESUME,
    }:
        return (
            event(
                TradeEventType.RECOVERY_ADOPTION_RECORDED,
                seconds=0,
                order_id=entry_order_id,
                perm_id=entry_perm_id,
                exec_id=entry_exec_id,
                price="100.00",
                reason_codes=(
                    "RECOVERY_ADOPTION_REGISTRY_BACKED"
                    if scenario == LifecycleStressScenario.RECOVERY_REGISTRY_BACKED_RESUME
                    else "RECOVERY_ADOPTION_RECORDED",
                ),
            ),
            event(TradeEventType.RECONCILED_OPEN, seconds=1),
        )
    if scenario == LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE:
        return (
            event(
                TradeEventType.REVIEW_REQUIRED,
                seconds=0,
                reason_codes=("RECOVERY_ADOPTION_MISSING_BROKER_BACKED_EVIDENCE",),
            ),
        )
    if scenario == LifecycleStressScenario.RECOVERY_AMBIGUOUS_TRADE_IDS:
        return (
            event(
                TradeEventType.REVIEW_REQUIRED,
                seconds=0,
                reason_codes=("RECOVERY_ADOPTION_AMBIGUOUS_TRADE_IDS",),
            ),
        )
    if scenario == LifecycleStressScenario.RECOVERY_LIFECYCLE_OWNER_CONFLICT:
        return (
            event(
                TradeEventType.RECOVERY_ADOPTION_RECORDED,
                seconds=0,
                order_id=entry_order_id,
                perm_id=entry_perm_id,
                exec_id=entry_exec_id,
                price="100.00",
                reason_codes=("RECOVERY_ADOPTION_RECORDED",),
            ),
            event(
                TradeEventType.REVIEW_REQUIRED,
                seconds=1,
                lifecycle=f"conflicting_{lifecycle_id}",
                reason_codes=("RECOVERY_ADOPTION_LIFECYCLE_OWNER_CONFLICT",),
            ),
        )
    if scenario == LifecycleStressScenario.RECOVERY_STALE_HISTORY_NO_ADOPT:
        return (
            event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=0, order_id=entry_order_id, perm_id=entry_perm_id, exec_id=entry_exec_id, price="100.00"),
            event(TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=1),
            event(TradeEventType.EXIT_FILL_BROKER_BACKED, seconds=2, action=lane.exit_action, order_id=exit_order_id, perm_id=exit_perm_id, exec_id=exit_exec_id, price="101.00"),
            event(TradeEventType.RECONCILED_FLAT, seconds=3, action=lane.exit_action),
            event(
                TradeEventType.REVIEW_REQUIRED,
                seconds=4,
                reason_codes=("RECOVERY_STALE_HISTORICAL_ARTIFACT_CANNOT_ADOPT_CURRENT_POSITION",),
            ),
        )
    if scenario == LifecycleStressScenario.GATE_DUPLICATE_ENTRY_BLOCKED:
        return (
            event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=0, order_id=entry_order_id, perm_id=entry_perm_id, exec_id=entry_exec_id, price="100.00"),
            event(TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=1),
            event(TradeEventType.REVIEW_REQUIRED, seconds=2, reason_codes=("GATE_DUPLICATE_ENTRY_BLOCKED",)),
        )
    if scenario == LifecycleStressScenario.GATE_OPEN_WORKING_ORDER_BLOCKED:
        return (
            event(TradeEventType.ENTRY_INTENT_CREATED, seconds=0),
            event(TradeEventType.ENTRY_ORDER_SUBMITTED, seconds=1, order_id=entry_order_id, reason_codes=("OPEN_WORKING_ORDER_PRESENT",)),
            event(TradeEventType.REVIEW_REQUIRED, seconds=2, reason_codes=("GATE_OPEN_WORKING_ORDER_BLOCKED",)),
        )
    if scenario == LifecycleStressScenario.MANUAL_OPERATOR_CLOSE:
        return (
            event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=0, order_id=entry_order_id, perm_id=entry_perm_id, exec_id=entry_exec_id, price="100.00"),
            event(TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=1),
            event(
                TradeEventType.MANUAL_OPERATOR_CLOSE_RECORDED,
                seconds=3600,
                action=lane.exit_action,
                order_id=exit_order_id,
                perm_id=exit_perm_id,
                exec_id=exit_exec_id,
                price="101.00",
            ),
            event(TradeEventType.RECONCILED_FLAT, seconds=3601, action=lane.exit_action),
        )

    chain = [
        event(TradeEventType.ENTRY_INTENT_CREATED, seconds=0),
        event(TradeEventType.ENTRY_ORDER_SUBMITTED, seconds=1, order_id=entry_order_id),
    ]
    if scenario == LifecycleStressScenario.MISSING_PERM_OR_EXEC:
        chain.append(event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=2, order_id=entry_order_id, perm_id="", exec_id="", price="100.00"))
        return tuple(chain)
    chain.append(event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=2, order_id=entry_order_id, perm_id=entry_perm_id, exec_id=entry_exec_id, price="100.00"))

    if scenario == LifecycleStressScenario.ENTRY_FILL_NOT_ADOPTED:
        chain.append(event(TradeEventType.REVIEW_REQUIRED, seconds=3, reason_codes=("ENTRY_FILL_NOT_ADOPTED",)))
        return tuple(chain)
    if scenario == LifecycleStressScenario.WRONG_CONTRACT_IDENTITY:
        chain.append(
            event(
                TradeEventType.LIFECYCLE_OPEN_MANAGED,
                seconds=3,
                con_id=lane.con_id + 999,
                local_symbol=f"{lane.local_symbol}_WRONG",
                reason_codes=("CONTRACT_IDENTITY_MISMATCH",),
            )
        )
        return tuple(chain)

    chain.append(event(TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=3))
    if scenario in {
        LifecycleStressScenario.STALE_CONTROL_PLANE,
        LifecycleStressScenario.SAFE_STATE_BLOCKED,
        LifecycleStressScenario.PLANNER_SNAPSHOT_MISMATCH,
        LifecycleStressScenario.CONTRACT_CLOSE_ONLY,
        LifecycleStressScenario.AMBIGUOUS_RECONSTRUCTION,
        LifecycleStressScenario.RECONCILIATION_AMBIGUITY,
        LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION,
    }:
        reason = {
            LifecycleStressScenario.STALE_CONTROL_PLANE: CONTROL_PLANE_STALE,
            LifecycleStressScenario.SAFE_STATE_BLOCKED: SAFE_STATE_SUBMIT_BLOCKED,
            LifecycleStressScenario.PLANNER_SNAPSHOT_MISMATCH: PLANNER_SNAPSHOT_MISMATCH,
            LifecycleStressScenario.CONTRACT_CLOSE_ONLY: CONTRACT_ENTRY_CLOSE_ONLY,
            LifecycleStressScenario.AMBIGUOUS_RECONSTRUCTION: "AMBIGUOUS_RECONSTRUCTION",
            LifecycleStressScenario.RECONCILIATION_AMBIGUITY: "REGISTRY_AMBIGUOUS_BROKER_POSITION",
            LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION: "STALE_HISTORICAL_ARTIFACT_CANNOT_REOPEN_CLOSED_TRADE",
        }[scenario]
        chain.append(event(TradeEventType.REVIEW_REQUIRED, seconds=4, reason_codes=(reason,)))
        return tuple(chain)
    if scenario == LifecycleStressScenario.SCOPED_CLEANUP_EXTRA_DIAGNOSTIC_FIELDS:
        chain.append(event(TradeEventType.EXIT_INTENT_CREATED, seconds=4, action=lane.exit_action, reason_codes=("SCOPED_CLEANUP_EXTRA_DIAGNOSTIC_FIELDS",)))
    else:
        chain.append(event(TradeEventType.EXIT_INTENT_CREATED, seconds=4, action=lane.exit_action))

    if scenario == LifecycleStressScenario.WRONG_LIFECYCLE_ID:
        chain.append(event(TradeEventType.EXIT_ORDER_SUBMITTED, seconds=5, action=lane.exit_action, lifecycle=f"wrong_{lifecycle_id}", order_id=exit_order_id))
        return tuple(chain)
    chain.append(event(TradeEventType.EXIT_ORDER_SUBMITTED, seconds=5, action=lane.exit_action, order_id=exit_order_id))
    chain.append(event(TradeEventType.EXIT_FILL_BROKER_BACKED, seconds=6, action=lane.exit_action, order_id=exit_order_id, perm_id=exit_perm_id, exec_id=exit_exec_id, price="101.00"))
    if scenario == LifecycleStressScenario.DUPLICATE_FILL:
        chain.insert(3, event(TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=3, order_id=f"{entry_order_id}D", perm_id=f"{entry_perm_id}D", exec_id=f"{entry_exec_id}.D", price="100.25"))
        chain.append(event(TradeEventType.REVIEW_REQUIRED, seconds=7, reason_codes=("DUPLICATE_ENTRY_FILL",)))
        return tuple(chain)
    if scenario == LifecycleStressScenario.DUPLICATE_CLOSE:
        chain.append(event(TradeEventType.EXIT_FILL_BROKER_BACKED, seconds=7, action=lane.exit_action, order_id=f"{exit_order_id}D", perm_id=f"{exit_perm_id}D", exec_id=f"{exit_exec_id}.D", price="101.25"))
        chain.append(event(TradeEventType.REVIEW_REQUIRED, seconds=8, action=lane.exit_action, reason_codes=("DUPLICATE_CLOSE_FILL",)))
        return tuple(chain)
    chain.append(event(TradeEventType.RECONCILED_FLAT, seconds=7, action=lane.exit_action))
    return tuple(chain)


def _truth_for_scenario(
    *,
    root: Path,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    record: TradeRegistryRecord | None,
    generated_at: datetime,
) -> TrackBTruthSnapshot:
    config = TrackBTruthSnapshotConfig(repo_root=root)
    _seed_truth_artifacts(config=config, scenario=scenario, lane=lane, record=record, generated_at=generated_at)
    return build_track_b_truth_snapshot(config=config, now=generated_at)


def _gate_shadow_for_scenario(
    *,
    root: Path,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    trade_id: str,
    record: TradeRegistryRecord | None,
    generated_at: datetime,
):
    gate_action = _gate_action_for_scenario(scenario=scenario, lane=lane)
    gate_record = _gate_record_for_scenario(
        scenario=scenario,
        lane=lane,
        trade_id=trade_id,
        generated_at=generated_at,
        fallback_record=record,
    )
    gate_records = () if gate_record is None else (gate_record,)
    gate_truth = _truth_for_scenario(
        root=root / "gate_shadow",
        scenario=scenario,
        lane=lane,
        record=gate_record,
        generated_at=generated_at,
    )
    owner = gate_record.ownership_identity if gate_record is not None else None
    context = TrackBGateShadowContext(
        repo_root=root,
        lane_id=lane.lane_id,
        thesis_strategy_id=lane.thesis_strategy_id,
        action=gate_action,
        quantity=Decimal("1"),
        symbol=lane.symbol,
        trade_id=gate_record.trade_id if _gate_action_is_close(gate_action) and gate_record is not None else None,
        lifecycle_id=owner.lifecycle_id if _gate_action_is_close(gate_action) and owner is not None else None,
        existing_gate_results=_legacy_gate_results_for_scenario(
            scenario=scenario,
            action=gate_action,
            gate_truth=gate_truth,
        ),
        truth_snapshot=gate_truth,
        registry_records=gate_records,
        generated_at=generated_at,
    )
    return evaluate_track_b_gate_shadow_parity(context)


def _gate_shadow_summary(report) -> dict[str, Any]:
    if report is None:
        return {
            "total_gate_checks": 0,
            "green_path_checks": 0,
            "blocked_checks": 0,
            "mismatches": 0,
            "safety_regressions": 0,
            "missing_trade_id_blocks": 0,
            "mismatches_by_gate": {},
            "mismatches_by_lane_scenario": {},
        }
    mismatches_by_gate: dict[str, int] = {}
    safety_regressions = 0
    missing_trade_id_blocks = 0
    green_path_checks = 0
    blocked_checks = 0
    for row in report.rows:
        existing_allowed = bool(row.existing_gate_result.allowed)
        shadow_allowed = bool(row.registry_truth_gate_result.allowed)
        if existing_allowed and shadow_allowed:
            green_path_checks += 1
        if not existing_allowed and not shadow_allowed:
            blocked_checks += 1
        if not row.parity:
            mismatches_by_gate[row.gate_name] = mismatches_by_gate.get(row.gate_name, 0) + 1
        if not existing_allowed and shadow_allowed and _has_safety_reason(row.existing_gate_result.reason_codes):
            safety_regressions += 1
        if existing_allowed and not shadow_allowed and "REGISTRY_TRADE_ID_NOT_OPEN_MANAGED" in row.registry_truth_gate_result.reason_codes:
            missing_trade_id_blocks += 1
    return {
        "total_gate_checks": len(report.rows),
        "green_path_checks": green_path_checks,
        "blocked_checks": blocked_checks,
        "mismatches": sum(mismatches_by_gate.values()),
        "safety_regressions": safety_regressions,
        "missing_trade_id_blocks": missing_trade_id_blocks,
        "mismatches_by_gate": mismatches_by_gate,
        "mismatches_by_lane_scenario": {f"{report.lane_id}|{report.trade_id or 'entry'}": sum(mismatches_by_gate.values())} if mismatches_by_gate else {},
    }


def _has_safety_reason(reason_codes: Sequence[str]) -> bool:
    return bool({str(code) for code in reason_codes}.intersection(_GATE_SHADOW_SAFETY_REASONS))


def _gate_action_for_scenario(*, scenario: LifecycleStressScenario, lane: StressLane) -> str:
    if scenario in {
        LifecycleStressScenario.MANAGED_EXIT_DUE_CLOSE,
        LifecycleStressScenario.MISSING_PERM_OR_EXEC,
        LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
    }:
        return lane.exit_action
    return lane.entry_action


def _gate_action_is_close(action: str) -> bool:
    return str(action or "").upper() in {"SELL_TO_CLOSE", "BUY_TO_CLOSE", "EXIT", "CLOSE"}


def _gate_record_for_scenario(
    *,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    trade_id: str,
    generated_at: datetime,
    fallback_record: TradeRegistryRecord | None,
) -> TradeRegistryRecord | None:
    if scenario in {
        LifecycleStressScenario.MANAGED_EXIT_DUE_CLOSE,
        LifecycleStressScenario.GATE_DUPLICATE_ENTRY_BLOCKED,
    }:
        return reduce_trade_events(
            tuple(
                event
                for event in _events_for_scenario(
                    index=0,
                    scenario=LifecycleStressScenario.CLEAN_FULL_LIFECYCLE,
                    lane=lane,
                    trade_id=trade_id,
                    generated_at=generated_at,
                )
                if event.event_type
                in {
                    TradeEventType.ENTRY_INTENT_CREATED,
                    TradeEventType.ENTRY_ORDER_SUBMITTED,
                    TradeEventType.ENTRY_FILL_BROKER_BACKED,
                    TradeEventType.LIFECYCLE_OPEN_MANAGED,
                }
            )
        )
    if scenario in {
        LifecycleStressScenario.MISSING_PERM_OR_EXEC,
        LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
        LifecycleStressScenario.RECONCILIATION_AMBIGUITY,
    }:
        return fallback_record
    return None if fallback_record is None or fallback_record.current_state != TradeCurrentState.OPEN_MANAGED else fallback_record


def _legacy_gate_results_for_scenario(
    *,
    scenario: LifecycleStressScenario,
    action: str,
    gate_truth: TrackBTruthSnapshot,
) -> dict[str, dict[str, Any]]:
    entry_allowed = True
    managed_exit_allowed = True
    governance_allowed = True
    safe_allowed = bool(gate_truth.safe_state.submit_allowed and gate_truth.safe_state.fresh)
    no_order_allowed = bool(gate_truth.broker_truth.fresh and gate_truth.broker_truth.open_order_count == 0)
    runtime_allowed = bool(gate_truth.runtime.runtime_alive and gate_truth.runtime.submit_capable and not gate_truth.runtime.duplicate_writer_detected)
    entry_reasons: list[str] = []
    managed_reasons: list[str] = []
    governance_reasons: list[str] = []
    safe_reasons: list[str] = list(gate_truth.safe_state.reason_codes)
    no_order_reasons: list[str] = []
    runtime_reasons: list[str] = list(gate_truth.runtime.reason_codes)

    if _gate_action_is_close(action):
        entry_reasons.append("ENTRY_EXPOSURE_NOT_APPLICABLE")
    else:
        managed_reasons.append("MANAGED_EXIT_NOT_APPLICABLE")

    if gate_truth.conflicts:
        if not _gate_action_is_close(action):
            entry_allowed = False
            entry_reasons.append("TRUTH_CONFLICT_REVIEW_REQUIRED")
        else:
            managed_exit_allowed = False
            managed_reasons.append("TRUTH_CONFLICT_REVIEW_REQUIRED")
        governance_allowed = False
        governance_reasons.append("TRUTH_CONFLICT_REVIEW_REQUIRED")
    if not gate_truth.runtime.submit_capable:
        governance_allowed = False
        runtime_allowed = False
        governance_reasons.append("RUNTIME_NOT_SUBMIT_CAPABLE")
        runtime_reasons.append("RUNTIME_NOT_SUBMIT_CAPABLE")
    if not gate_truth.control_plane.fresh:
        governance_allowed = False
        governance_reasons.append(CONTROL_PLANE_STALE)
    if not gate_truth.safe_state.submit_allowed:
        governance_allowed = False
        governance_reasons.append(SAFE_STATE_SUBMIT_BLOCKED)
    if not safe_allowed:
        safe_reasons.append("SAFE_STATE_STALE" if not gate_truth.safe_state.fresh else SAFE_STATE_SUBMIT_BLOCKED)
    if not no_order_allowed:
        no_order_reasons.append("OPEN_WORKING_ORDER_PRESENT")
        if not _gate_action_is_close(action):
            entry_allowed = False
            entry_reasons.append("OPEN_WORKING_ORDER_PRESENT")
        else:
            managed_exit_allowed = False
            managed_reasons.append("OPEN_WORKING_ORDER_PRESENT")
        governance_allowed = False
        governance_reasons.append("OPEN_WORKING_ORDER_PRESENT")

    if scenario == LifecycleStressScenario.GATE_DUPLICATE_ENTRY_BLOCKED:
        entry_allowed = False
        entry_reasons.append("DUPLICATE_STRATEGY_ENTRY_WHILE_POSITION_OPEN")
    if scenario in {
        LifecycleStressScenario.RECOVERY_ADOPTION,
        LifecycleStressScenario.RECOVERY_REGISTRY_BACKED_RESUME,
    } and not _gate_action_is_close(action):
        entry_allowed = False
        entry_reasons.append("DUPLICATE_STRATEGY_ENTRY_WHILE_POSITION_OPEN")
    if scenario in {
        LifecycleStressScenario.MISSING_PERM_OR_EXEC,
        LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
    }:
        managed_exit_allowed = False
        managed_reasons.append("REGISTRY_ENTRY_NOT_BROKER_BACKED")
        governance_allowed = False
        governance_reasons.append("FILL_NOT_BROKER_BACKED")
    if scenario == LifecycleStressScenario.CONTRACT_CLOSE_ONLY:
        governance_allowed = False
        governance_reasons.append(CONTRACT_ENTRY_CLOSE_ONLY)

    return {
        TrackBGateName.ENTRY_EXPOSURE.value: {"submit_allowed": entry_allowed, "block_reasons": list(dict.fromkeys(entry_reasons))},
        TrackBGateName.MANAGED_EXIT_EXPOSURE.value: {"submit_allowed": managed_exit_allowed, "block_reasons": list(dict.fromkeys(managed_reasons))},
        TrackBGateName.GOVERNANCE_SUBMIT.value: {"submit_allowed": governance_allowed, "block_reasons": list(dict.fromkeys(governance_reasons))},
        TrackBGateName.SAFE_STATE_SUBMIT.value: {"submit_allowed": safe_allowed, "block_reasons": list(dict.fromkeys(safe_reasons))},
        TrackBGateName.NO_WORKING_ORDER.value: {"passed": no_order_allowed, "block_reasons": list(dict.fromkeys(no_order_reasons))},
        TrackBGateName.RUNTIME_AUTHORITY.value: {"ready": runtime_allowed, "block_reasons": list(dict.fromkeys(runtime_reasons))},
    }


def _seed_truth_artifacts(
    *,
    config: TrackBTruthSnapshotConfig,
    scenario: LifecycleStressScenario,
    lane: StressLane,
    record: TradeRegistryRecord | None,
    generated_at: datetime,
) -> None:
    root = config.repo_root
    open_qty = int(record.open_qty) if record is not None else 0
    broker_positions = [{"symbol": lane.symbol, "position": open_qty, "con_id": lane.con_id, "localSymbol": lane.local_symbol}] if open_qty else []
    managed_positions = []
    if open_qty and record and record.ownership_identity:
        managed_positions.append(
            {
                "lifecycle_id": record.ownership_identity.lifecycle_id,
                "account_id": "DUM882026",
                "exact_lifecycle_account_id": "DUM882026",
                "con_id": lane.con_id,
                "localSymbol": lane.local_symbol,
                "qty": open_qty,
                "state": "OPEN_MANAGED",
            }
        )
    reconciled = scenario not in {
        LifecycleStressScenario.ENTRY_FILL_NOT_ADOPTED,
        LifecycleStressScenario.WRONG_CONTRACT_IDENTITY,
        LifecycleStressScenario.WRONG_LIFECYCLE_ID,
        LifecycleStressScenario.DUPLICATE_FILL,
        LifecycleStressScenario.DUPLICATE_CLOSE,
        LifecycleStressScenario.AMBIGUOUS_RECONSTRUCTION,
    }
    stale_cp = scenario == LifecycleStressScenario.STALE_CONTROL_PLANE
    safe_blocked = scenario == LifecycleStressScenario.SAFE_STATE_BLOCKED
    planner_mismatch = scenario == LifecycleStressScenario.PLANNER_SNAPSHOT_MISMATCH
    close_only = scenario == LifecycleStressScenario.CONTRACT_CLOSE_ONLY
    missing_broker_ids = scenario in {
        LifecycleStressScenario.MISSING_PERM_OR_EXEC,
        LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
    }

    _write_json(root / config.runtime_truth_path, {"generated_at": generated_at.isoformat(), "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE", "runtime": {"pid": 1234, "pid_alive": True, "runtime_instance_id": "generation-1", "lane_count": 8}, "canonical_readiness": {"classification": "READY_SUBMIT_CAPABLE", "ready_submit_capable": True}})
    _write_json(root / config.recovery_status_path, {"generated_at": generated_at.isoformat(), "classification": "RECOVERY_ACTIVE", "launchd_loaded": True, "launchd_enabled": True, "last_tick": generated_at.isoformat()})
    _write_json(root / config.recovery_audit_path, {"generated_at": generated_at.isoformat(), "classification": "RUNTIME_HEALTHY_NO_ACTION", "hourly_supervisor": {"classification": "SUPERVISOR_RUNNING", "active": True}})
    _write_json(root / config.broker_status_path, {"generated_at": generated_at.isoformat(), "positions_snapshot_path": str(root / config.broker_positions_path), "open_orders_snapshot_path": str(root / config.broker_open_orders_path)})
    _write_json(root / config.broker_positions_path, {"generated_at": generated_at.isoformat(), "positions": broker_positions})
    open_orders = [
        {
            "symbol": lane.symbol,
            "order_id": "synthetic_working_order",
            "client_id": "17",
            "status": "Submitted",
        }
    ] if scenario == LifecycleStressScenario.GATE_OPEN_WORKING_ORDER_BLOCKED else []
    _write_json(root / config.broker_open_orders_path, {"generated_at": generated_at.isoformat(), "open_orders": open_orders})
    _write_json(root / config.lifecycle_live_position_path, {"generated_at": generated_at.isoformat(), "open_positions": managed_positions})
    _write_json(root / config.managed_position_registry_path, {"generated_at": generated_at.isoformat(), "managed_positions": managed_positions})
    _write_json(root / config.managed_order_registry_path, {"generated_at": generated_at.isoformat(), "managed_orders": []})
    _write_json(root / config.reconciliation_path, {"generated_at": generated_at.isoformat(), "classification": "BROKER_LIFECYCLE_RECONCILED" if reconciled else "DIRTY", "broker_reconciled": reconciled, "review_required_count": 0 if reconciled else 1})
    _write_json(root / config.safe_state_path, {"generated_at": generated_at.isoformat(), "classification": "SAFE_STATE_NORMAL" if not safe_blocked else "SAFE_STATE_BLOCKED", "submit_allowed": not safe_blocked, "runtime_start_allowed": True})
    cp_time = generated_at - timedelta(minutes=20) if stale_cp else generated_at
    _write_json(root / config.control_plane_path, {"generated_at": cp_time.isoformat(), "control_plane_snapshot_id": "snapshot-1", "shared_truth_refresh_generation_id": "generation-1", "shared_truth_coherence_status": "COHERENT"})
    _write_json(root / config.planner_path, {"generated_at": generated_at.isoformat(), "classification": "PLAN_SCOPED_POSITION_CLEANUP", "control_plane_snapshot_id": "stale-snapshot" if planner_mismatch else "snapshot-1", "shared_truth_refresh_generation_id": "generation-1"})
    _write_json(root / config.supervisor_path, {"generated_at": generated_at.isoformat(), "classification": "SUPERVISOR_RUNTIME_START_ALLOWED", "supervisor_decision_id": "supervisor-1"})
    _write_json(root / config.contract_status_path, {"generated_at": generated_at.isoformat(), "classification": "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED" if close_only else "CONTRACT_ALLOWED", "submit_allowed": not close_only, "symbol": lane.symbol, "entry_status": CONTRACT_ENTRY_CLOSE_ONLY if close_only else "CONTRACT_ENTRY_ELIGIBLE", "exit_status": "EXIT_ORIGINAL_CONTRACT_ALLOWED", "selected_contract": {"localSymbol": lane.local_symbol, "conId": lane.con_id, "expiry": lane.expiry}})
    fill = {"order_id": "synthetic", "client_id": "17"}
    if not missing_broker_ids:
        fill.update({"perm_id": "synthetic_perm", "exec_id": "synthetic_exec"})
    _write_json(root / config.broker_backed_evidence_path, {"generated_at": generated_at.isoformat(), "fills": [fill]})
    _write_json(root / config.local_paper_artifact_path, {"generated_at": generated_at.isoformat(), "fills": [fill] if missing_broker_ids else []})
    if config.dashboard_runtime_path is not None:
        _write_json(root / config.dashboard_runtime_path, {"generated_at": generated_at.isoformat(), "diagnostic": True})


def _invariant_failures(
    *,
    record: TradeRegistryRecord | None,
    events: Sequence[TradeEvent],
    scenario: LifecycleStressScenario,
) -> tuple[str, ...]:
    if record is None:
        return ()
    failures: list[str] = []
    if record.current_state == TradeCurrentState.CLOSED_FLAT and record.open_qty != 0:
        failures.append("CLOSED_FLAT_WITH_OPEN_QTY")
    if record.current_state == TradeCurrentState.OPEN_MANAGED and not record.broker_backed_entry:
        failures.append("OPEN_MANAGED_WITHOUT_BROKER_BACKED_ENTRY")
    for event in events:
        if event.event_type in {TradeEventType.ENTRY_FILL_BROKER_BACKED, TradeEventType.EXIT_FILL_BROKER_BACKED} and not event.broker_backed:
            failures.append("BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC")
    entry_fills = [event for event in events if event.event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED]
    exit_fills = [event for event in events if event.event_type == TradeEventType.EXIT_FILL_BROKER_BACKED]
    if len(entry_fills) > 1:
        failures.append("DUPLICATE_ENTRY_FILL")
    if len(exit_fills) > 1:
        failures.append("DUPLICATE_CLOSE_FILL")
    if exit_fills and not any(event.event_type == TradeEventType.LIFECYCLE_OPEN_MANAGED for event in events):
        failures.append("EXIT_FILL_WITHOUT_OPEN_LIFECYCLE")
    if scenario == LifecycleStressScenario.CONTRACT_CLOSE_ONLY and CONTRACT_ENTRY_CLOSE_ONLY not in record.latest_reason_codes:
        failures.append("CONTRACT_CLOSE_ONLY_NOT_CLASSIFIED")
    return tuple(dict.fromkeys(failures))


def _unexpected_invariant_failures(
    *,
    scenario: LifecycleStressScenario,
    invariants: Sequence[str],
) -> tuple[str, ...]:
    expected_by_scenario = {
        LifecycleStressScenario.MISSING_PERM_OR_EXEC: {"BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC"},
        LifecycleStressScenario.DUPLICATE_FILL: {"DUPLICATE_ENTRY_FILL", "EXIT_FILL_WITHOUT_OPEN_LIFECYCLE"},
        LifecycleStressScenario.DUPLICATE_CLOSE: {"DUPLICATE_CLOSE_FILL"},
        LifecycleStressScenario.CONTRACT_CLOSE_ONLY: {"CONTRACT_CLOSE_ONLY_NOT_CLASSIFIED"},
    }
    expected = expected_by_scenario.get(scenario, set())
    return tuple(item for item in invariants if item not in expected)


def _silent_ambiguity_merge(
    *,
    record: TradeRegistryRecord | None,
    scenario: LifecycleStressScenario,
) -> bool:
    if scenario != LifecycleStressScenario.AMBIGUOUS_RECONSTRUCTION or record is None:
        return False
    return record.current_state != TradeCurrentState.REVIEW_REQUIRED or not record.latest_reason_codes


def _expected_bad_lifecycle_classified(
    *,
    record: TradeRegistryRecord | None,
    truth: TrackBTruthSnapshot,
    expected_review_required: bool,
) -> bool:
    if not expected_review_required:
        return True
    if record is None:
        return False
    return bool(record.latest_reason_codes or truth.reason_codes or truth.conflicts)


def _classification(
    *,
    record: TradeRegistryRecord | None,
    expected_review_required: bool,
    unexpected_invariants: Sequence[str],
    impossible_states: Sequence[str],
    silent_ambiguity_merge: bool,
    reducer_error: str | None,
) -> str:
    if reducer_error:
        return STRESS_REDUCER_ERROR
    if unexpected_invariants or impossible_states or silent_ambiguity_merge:
        return STRESS_INVARIANT_FAILED
    if expected_review_required and record is not None and record.current_state == TradeCurrentState.REVIEW_REQUIRED:
        return STRESS_EXPECTED_REVIEW_REQUIRED
    if record is not None and record.current_state == TradeCurrentState.REVIEW_REQUIRED:
        return STRESS_REVIEW_REQUIRED
    return STRESS_PASSED


def _shadow_row(*, record: TradeRegistryRecord, truth: TrackBTruthSnapshot) -> TradeRegistryShadowRow:
    owner = record.ownership_identity
    first = record.event_chain[0]
    return TradeRegistryShadowRow(
        trade_id=record.trade_id,
        lifecycle_id=(owner.lifecycle_id if owner else None) or first.lifecycle_id,
        lane_id=(owner.lane_id if owner else None) or first.lane_id,
        thesis_strategy_id=(owner.thesis_strategy_id if owner else None) or first.thesis_strategy_id,
        symbol=(owner.symbol if owner else None) or first.symbol,
        con_id=(owner.con_id if owner else None) or first.con_id,
        local_symbol=(owner.local_symbol if owner else None) or first.local_symbol,
        event_chain_summary=tuple(
            {
                "event_type": event.event_type.value,
                "generated_at": event.generated_at.isoformat(),
                "perm_id": event.perm_id,
                "exec_id": event.exec_id,
                "reason_codes": list(event.reason_codes),
            }
            for event in record.event_chain
        ),
        current_derived_state=record.current_state.value,
        broker_backed_entry=record.broker_backed_entry,
        broker_backed_exit=record.broker_backed_exit,
        reconciliation_status=truth.reconciliation.classification,
        reconciliation_reconciled=truth.reconciliation.reconciled,
        truth_classification=truth.classification,
        truth_conflicts=tuple({"classification": conflict.classification, "reason_codes": list(conflict.reason_codes)} for conflict in truth.conflicts),
        truth_reason_codes=truth.reason_codes,
        missing_links=tuple({"trade_id": record.trade_id, "reason_codes": list(record.latest_reason_codes)}) if record.current_state == TradeCurrentState.REVIEW_REQUIRED else (),
        ambiguous_reconstruction=record.current_state == TradeCurrentState.REVIEW_REQUIRED,
        registry_agrees_with_reconciliation=truth.reconciliation.reconciled and not truth.conflicts,
        agreement_reason_codes=("STRESS_SHADOW_ROW",),
    )


def _lane_for_scenario(lane: StressLane, scenario: LifecycleStressScenario) -> StressLane:
    if scenario == LifecycleStressScenario.CONTRACT_CLOSE_ONLY and lane.symbol not in {"MGC", "GC"}:
        return DEFAULT_LANES[-2]
    if scenario != LifecycleStressScenario.CONTRACT_CLOSE_ONLY and lane.symbol in {"MGC", "GC"}:
        return DEFAULT_LANES[0]
    return lane


def _trade_id(*, index: int, scenario: LifecycleStressScenario, lane: StressLane) -> str:
    return f"stress_{index:06d}_{scenario.value}_{lane.lane_id}"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(inner) for inner in value]
    return value
