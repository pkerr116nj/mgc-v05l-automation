from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_lifecycle_stress_preflight import (
    LifecycleStressPreflightConfig,
    LifecycleStressPreflightProfile,
    build_parser,
    run_lifecycle_stress_preflight,
    write_lifecycle_stress_preflight_report,
)


def test_routine_preflight_runs_all_ladder_stages_with_fast_fuzz() -> None:
    report = run_lifecycle_stress_preflight(
        config=LifecycleStressPreflightConfig(
            profile=LifecycleStressPreflightProfile.ROUTINE,
            seed=7,
            routine_fuzz_count=25,
        )
    )

    assert report.passed is True
    assert [stage.mode for stage in report.stages] == ["smoke", "known_scenarios", "lane_matrix", "fuzz"]
    assert [stage.total_lifecycles for stage in report.stages] == [20, 260, 1000, 25]
    assert all(stage.hard_failure is False for stage in report.stages)
    assert report.to_dict()["summary"]["bad_lifecycles_without_reason_codes"] == 0
    assert report.to_dict()["summary"]["gate_shadow_mismatches"] == 0
    assert all(stage.gate_shadow_total_checks == stage.total_lifecycles * 6 for stage in report.stages)
    assert report.broker_mutation_allowed is False
    assert report.runtime_restart_allowed is False
    assert report.production_gate_wiring_allowed is False


def test_extended_preflight_uses_extended_fuzz_count_when_not_overridden() -> None:
    config = LifecycleStressPreflightConfig(profile=LifecycleStressPreflightProfile.EXTENDED, extended_fuzz_count=12345)

    assert config.stages[-1].count == 12345


def test_preflight_report_artifacts_include_json_and_markdown(tmp_path: Path) -> None:
    report = run_lifecycle_stress_preflight(
        config=LifecycleStressPreflightConfig(routine_fuzz_count=10, seed=11)
    )
    json_path, markdown_path = write_lifecycle_stress_preflight_report(
        report=report,
        output_path=tmp_path / "preflight.json",
        markdown_path=tmp_path / "preflight.md",
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8") if markdown_path else ""

    assert payload["schema_version"] == "track_b_lifecycle_stress_preflight_v1"
    assert payload["passed"] is True
    assert "hard_failure_reasons" in payload["stages"][0]
    assert payload["summary"]["gate_shadow_mismatches"] == 0
    assert "Track B Lifecycle Stress Preflight" in markdown
    assert "Expected REVIEW/block" in markdown


def test_cli_parser_supports_profile_seed_and_fuzz_override() -> None:
    args = build_parser().parse_args(["--profile", "extended", "--seed", "99", "--fuzz-count", "2500", "--no-markdown"])

    assert args.profile == "extended"
    assert args.seed == 99
    assert args.fuzz_count == 2500
    assert args.no_markdown is True
