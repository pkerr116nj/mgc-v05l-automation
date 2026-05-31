"""Repeatable Track B lifecycle stress preflight.

The preflight runs the synthetic lifecycle stress ladder as a diagnostic-only
check. It does not import broker adapters, does not start runtime processes, and
does not wire results into production gates.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_lifecycle_stress_framework import (
    LifecycleStressConfig,
    LifecycleStressReport,
    LifecycleStressRunMode,
    run_lifecycle_stress,
)


SCHEMA_VERSION = "track_b_lifecycle_stress_preflight_v1"
DEFAULT_PREFLIGHT_SUMMARY_PATH = (
    Path("outputs") / "track_b_execution_core" / "lifecycle_stress" / "latest_lifecycle_stress_preflight_summary.json"
)
DEFAULT_PREFLIGHT_MARKDOWN_PATH = (
    Path("outputs") / "track_b_execution_core" / "lifecycle_stress" / "latest_lifecycle_stress_preflight_summary.md"
)


class LifecycleStressPreflightProfile(str, Enum):
    ROUTINE = "routine"
    EXTENDED = "extended"


@dataclass(frozen=True)
class LifecycleStressPreflightStage:
    mode: LifecycleStressRunMode
    count: int | None = None


@dataclass(frozen=True)
class LifecycleStressPreflightConfig:
    profile: LifecycleStressPreflightProfile = LifecycleStressPreflightProfile.ROUTINE
    seed: int = 20260531
    output_path: Path = DEFAULT_PREFLIGHT_SUMMARY_PATH
    markdown_path: Path | None = DEFAULT_PREFLIGHT_MARKDOWN_PATH
    routine_fuzz_count: int = 1000
    extended_fuzz_count: int = 100000
    override_fuzz_count: int | None = None
    stop_on_hard_failure: bool = True

    @property
    def stages(self) -> tuple[LifecycleStressPreflightStage, ...]:
        fuzz_count = self.override_fuzz_count
        if fuzz_count is None:
            fuzz_count = self.extended_fuzz_count if self.profile == LifecycleStressPreflightProfile.EXTENDED else self.routine_fuzz_count
        return (
            LifecycleStressPreflightStage(LifecycleStressRunMode.SMOKE),
            LifecycleStressPreflightStage(LifecycleStressRunMode.KNOWN_SCENARIOS),
            LifecycleStressPreflightStage(LifecycleStressRunMode.LANE_MATRIX),
            LifecycleStressPreflightStage(LifecycleStressRunMode.FUZZ, fuzz_count),
        )


@dataclass(frozen=True)
class LifecycleStressPreflightStageResult:
    mode: str
    count: int
    hard_failure: bool
    hard_failure_reasons: tuple[str, ...]
    total_lifecycles: int
    expected_review_required: int
    unexpected_invariant_failures: int
    reducer_crashes: int
    trade_id_collisions: int
    silent_ambiguity_merges: int
    impossible_states: int
    bad_lifecycles_without_reason_codes: int
    broker_backed_evidence_violations: int
    gate_shadow_total_checks: int
    gate_shadow_green_path_checks: int
    gate_shadow_blocked_checks: int
    gate_shadow_mismatches: int
    gate_shadow_safety_regressions: int
    gate_shadow_missing_trade_id_blocks: int
    gate_shadow_mismatches_by_gate: Mapping[str, int]
    gate_shadow_mismatches_by_lane_scenario: Mapping[str, int]
    top_10_reason_codes: tuple[tuple[str, int], ...]
    worst_scenario_lane_combinations: tuple[Mapping[str, Any], ...]
    report_summary: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "count": self.count,
            "hard_failure": self.hard_failure,
            "hard_failure_reasons": list(self.hard_failure_reasons),
            "total_lifecycles": self.total_lifecycles,
            "expected_review_required": self.expected_review_required,
            "unexpected_invariant_failures": self.unexpected_invariant_failures,
            "reducer_crashes": self.reducer_crashes,
            "trade_id_collisions": self.trade_id_collisions,
            "silent_ambiguity_merges": self.silent_ambiguity_merges,
            "impossible_states": self.impossible_states,
            "bad_lifecycles_without_reason_codes": self.bad_lifecycles_without_reason_codes,
            "broker_backed_evidence_violations": self.broker_backed_evidence_violations,
            "gate_shadow_total_checks": self.gate_shadow_total_checks,
            "gate_shadow_green_path_checks": self.gate_shadow_green_path_checks,
            "gate_shadow_blocked_checks": self.gate_shadow_blocked_checks,
            "gate_shadow_mismatches": self.gate_shadow_mismatches,
            "gate_shadow_safety_regressions": self.gate_shadow_safety_regressions,
            "gate_shadow_missing_trade_id_blocks": self.gate_shadow_missing_trade_id_blocks,
            "gate_shadow_mismatches_by_gate": dict(self.gate_shadow_mismatches_by_gate),
            "gate_shadow_mismatches_by_lane_scenario": dict(self.gate_shadow_mismatches_by_lane_scenario),
            "top_10_reason_codes": [[code, count] for code, count in self.top_10_reason_codes],
            "worst_scenario_lane_combinations": list(self.worst_scenario_lane_combinations),
            "report_summary": dict(self.report_summary),
        }


@dataclass(frozen=True)
class LifecycleStressPreflightReport:
    schema_version: str
    generated_at: datetime
    profile: str
    seed: int
    read_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    production_gate_wiring_allowed: bool
    passed: bool
    stopped_early: bool
    stages: tuple[LifecycleStressPreflightStageResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "profile": self.profile,
            "seed": self.seed,
            "read_only": self.read_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "production_gate_wiring_allowed": self.production_gate_wiring_allowed,
            "passed": self.passed,
            "stopped_early": self.stopped_early,
            "stages": [stage.to_dict() for stage in self.stages],
            "summary": {
                "stage_count": len(self.stages),
                "hard_failure_stage_count": sum(1 for stage in self.stages if stage.hard_failure),
                "total_lifecycles": sum(stage.total_lifecycles for stage in self.stages),
                "expected_review_required": sum(stage.expected_review_required for stage in self.stages),
                "unexpected_invariant_failures": sum(stage.unexpected_invariant_failures for stage in self.stages),
                "reducer_crashes": sum(stage.reducer_crashes for stage in self.stages),
                "trade_id_collisions": sum(stage.trade_id_collisions for stage in self.stages),
                "silent_ambiguity_merges": sum(stage.silent_ambiguity_merges for stage in self.stages),
                "impossible_states": sum(stage.impossible_states for stage in self.stages),
                "bad_lifecycles_without_reason_codes": sum(stage.bad_lifecycles_without_reason_codes for stage in self.stages),
                "broker_backed_evidence_violations": sum(stage.broker_backed_evidence_violations for stage in self.stages),
                "gate_shadow_total_checks": sum(stage.gate_shadow_total_checks for stage in self.stages),
                "gate_shadow_green_path_checks": sum(stage.gate_shadow_green_path_checks for stage in self.stages),
                "gate_shadow_blocked_checks": sum(stage.gate_shadow_blocked_checks for stage in self.stages),
                "gate_shadow_mismatches": sum(stage.gate_shadow_mismatches for stage in self.stages),
                "gate_shadow_safety_regressions": sum(stage.gate_shadow_safety_regressions for stage in self.stages),
                "gate_shadow_missing_trade_id_blocks": sum(stage.gate_shadow_missing_trade_id_blocks for stage in self.stages),
            },
        }


def run_lifecycle_stress_preflight(
    *,
    config: LifecycleStressPreflightConfig | None = None,
    now: datetime | None = None,
) -> LifecycleStressPreflightReport:
    actual_config = config or LifecycleStressPreflightConfig()
    generated_at = _ensure_utc(now or datetime.now(UTC))
    stages: list[LifecycleStressPreflightStageResult] = []
    stopped_early = False
    for stage in actual_config.stages:
        stress_report = run_lifecycle_stress(
            LifecycleStressConfig(
                mode=stage.mode,
                count=stage.count,
                seed=actual_config.seed,
                generated_at=generated_at,
            )
        )
        stage_result = _stage_result(stress_report)
        stages.append(stage_result)
        if stage_result.hard_failure and actual_config.stop_on_hard_failure:
            stopped_early = True
            break
    return LifecycleStressPreflightReport(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        profile=actual_config.profile.value,
        seed=actual_config.seed,
        read_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        production_gate_wiring_allowed=False,
        passed=not any(stage.hard_failure for stage in stages),
        stopped_early=stopped_early,
        stages=tuple(stages),
    )


def write_lifecycle_stress_preflight_report(
    *,
    report: LifecycleStressPreflightReport,
    output_path: Path,
    markdown_path: Path | None = None,
) -> tuple[Path, Path | None]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    written_markdown: Path | None = None
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(_markdown_summary(report), encoding="utf-8")
        written_markdown = markdown_path
    return output_path, written_markdown


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the read-only Track B lifecycle stress preflight ladder.")
    parser.add_argument("--profile", choices=[item.value for item in LifecycleStressPreflightProfile], default=LifecycleStressPreflightProfile.ROUTINE.value)
    parser.add_argument("--seed", type=int, default=20260531)
    parser.add_argument("--fuzz-count", type=int, default=None)
    parser.add_argument("--routine-fuzz-count", type=int, default=1000)
    parser.add_argument("--extended-fuzz-count", type=int, default=100000)
    parser.add_argument("--output", type=Path, default=DEFAULT_PREFLIGHT_SUMMARY_PATH)
    parser.add_argument("--markdown-output", type=Path, default=DEFAULT_PREFLIGHT_MARKDOWN_PATH)
    parser.add_argument("--no-markdown", action="store_true")
    parser.add_argument("--no-stop-on-hard-failure", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = LifecycleStressPreflightConfig(
        profile=LifecycleStressPreflightProfile(args.profile),
        seed=args.seed,
        output_path=args.output,
        markdown_path=None if args.no_markdown else args.markdown_output,
        routine_fuzz_count=args.routine_fuzz_count,
        extended_fuzz_count=args.extended_fuzz_count,
        override_fuzz_count=args.fuzz_count,
        stop_on_hard_failure=not args.no_stop_on_hard_failure,
    )
    report = run_lifecycle_stress_preflight(config=config)
    output_path, markdown_path = write_lifecycle_stress_preflight_report(
        report=report,
        output_path=config.output_path,
        markdown_path=config.markdown_path,
    )
    print(json.dumps({"passed": report.passed, "output_path": str(output_path), "markdown_path": str(markdown_path) if markdown_path else None}, sort_keys=True))
    return 0 if report.passed else 1


def _stage_result(report: LifecycleStressReport) -> LifecycleStressPreflightStageResult:
    summary = report.summary
    top_reason_codes = _top_reason_codes(report)
    worst = _worst_scenario_lane_combinations(report)
    broker_violations = _broker_backed_evidence_violations(report)
    hard_fields = {
        "unexpected_invariant_failures": int(summary["unexpected_invariant_failures"]),
        "reducer_crashes": int(summary["reducer_crashes"]),
        "trade_id_collisions": int(summary["trade_id_collisions"]),
        "silent_ambiguity_merges": int(summary["silent_ambiguity_merges"]),
        "impossible_states": int(summary["impossible_states"]),
        "bad_lifecycles_without_reason_codes": int(summary["bad_lifecycles_without_reason_codes"]),
        "broker_backed_evidence_violations": broker_violations,
        "gate_shadow_mismatches": int(summary.get("gate_shadow_mismatches") or 0),
        "gate_shadow_safety_regressions": int(summary.get("gate_shadow_safety_regressions") or 0),
        "gate_shadow_missing_trade_id_blocks": int(summary.get("gate_shadow_missing_trade_id_blocks") or 0),
    }
    reasons = tuple(key for key, value in hard_fields.items() if value > 0)
    return LifecycleStressPreflightStageResult(
        mode=report.mode,
        count=report.requested_count,
        hard_failure=bool(reasons),
        hard_failure_reasons=reasons,
        total_lifecycles=int(summary["total_trades"]),
        expected_review_required=int(summary["expected_review_required"]),
        unexpected_invariant_failures=hard_fields["unexpected_invariant_failures"],
        reducer_crashes=hard_fields["reducer_crashes"],
        trade_id_collisions=hard_fields["trade_id_collisions"],
        silent_ambiguity_merges=hard_fields["silent_ambiguity_merges"],
        impossible_states=hard_fields["impossible_states"],
        bad_lifecycles_without_reason_codes=hard_fields["bad_lifecycles_without_reason_codes"],
        broker_backed_evidence_violations=broker_violations,
        gate_shadow_total_checks=int(summary.get("gate_shadow_total_checks") or 0),
        gate_shadow_green_path_checks=int(summary.get("gate_shadow_green_path_checks") or 0),
        gate_shadow_blocked_checks=int(summary.get("gate_shadow_blocked_checks") or 0),
        gate_shadow_mismatches=hard_fields["gate_shadow_mismatches"],
        gate_shadow_safety_regressions=hard_fields["gate_shadow_safety_regressions"],
        gate_shadow_missing_trade_id_blocks=hard_fields["gate_shadow_missing_trade_id_blocks"],
        gate_shadow_mismatches_by_gate=dict(summary.get("gate_shadow_mismatches_by_gate") or {}),
        gate_shadow_mismatches_by_lane_scenario=dict(summary.get("gate_shadow_mismatches_by_lane_scenario") or {}),
        top_10_reason_codes=top_reason_codes,
        worst_scenario_lane_combinations=worst,
        report_summary=summary,
    )


def _broker_backed_evidence_violations(report: LifecycleStressReport) -> int:
    return sum(
        1
        for result in report.results
        if "BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC" in result.invariant_failures
        and not result.expected_bad_lifecycle_classified
    )


def _top_reason_codes(report: LifecycleStressReport) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for result in report.results:
        for code in (*result.truth_reason_codes, *result.invariant_failures):
            counts[code] = counts.get(code, 0) + 1
    return tuple(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10])


def _worst_scenario_lane_combinations(report: LifecycleStressReport) -> tuple[Mapping[str, Any], ...]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for result in report.results:
        key = (result.scenario, result.lane_id)
        row = rows.setdefault(
            key,
            {
                "scenario": result.scenario,
                "lane_id": result.lane_id,
                "count": 0,
                "hard_failures": 0,
                "reason_counts": {},
            },
        )
        row["count"] += 1
        hard_failure = bool(
            result.reducer_error
            or result.unexpected_invariant_failures
            or result.silent_ambiguity_merge
            or result.impossible_states
            or (result.expected_review_required and not result.expected_bad_lifecycle_classified)
        )
        if hard_failure:
            row["hard_failures"] += 1
        for code in (*result.truth_reason_codes, *result.invariant_failures):
            row["reason_counts"][code] = row["reason_counts"].get(code, 0) + 1
    output = []
    for row in rows.values():
        reason_counts = row.pop("reason_counts")
        output.append(
            {
                **row,
                "top_reason_codes": sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5],
            }
        )
    return tuple(sorted(output, key=lambda item: (-int(item["hard_failures"]), -int(item["count"]), str(item["scenario"])))[:10])


def _markdown_summary(report: LifecycleStressPreflightReport) -> str:
    lines = [
        "# Track B Lifecycle Stress Preflight",
        "",
        f"- Profile: `{report.profile}`",
        f"- Seed: `{report.seed}`",
        f"- Passed: `{str(report.passed).lower()}`",
        f"- Generated at: `{report.generated_at.isoformat()}`",
        "",
        "| Stage | Total | Expected REVIEW/block | Gate checks | Gate mismatches | Unexpected invariants | Crashes | Collisions | Silent merges | Impossible states | Missing reason codes | Broker evidence violations |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for stage in report.stages:
        lines.append(
            f"| {stage.mode} | {stage.total_lifecycles} | {stage.expected_review_required} | "
            f"{stage.gate_shadow_total_checks} | {stage.gate_shadow_mismatches} | "
            f"{stage.unexpected_invariant_failures} | {stage.reducer_crashes} | {stage.trade_id_collisions} | "
            f"{stage.silent_ambiguity_merges} | {stage.impossible_states} | "
            f"{stage.bad_lifecycles_without_reason_codes} | {stage.broker_backed_evidence_violations} |"
        )
    lines.append("")
    lines.append("## Top Reason Codes")
    for stage in report.stages:
        lines.append("")
        lines.append(f"### {stage.mode}")
        for code, count in stage.top_10_reason_codes:
            lines.append(f"- `{code}`: {count}")
    return "\n".join(lines) + "\n"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
