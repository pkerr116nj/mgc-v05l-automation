from __future__ import annotations

import json

from validation_layer.reporting.json_report import render_json_report
from validation_layer.reporting.markdown_report import render_markdown_report
from validation_layer.reporting.models import ScorecardEntry, ValidationModuleResult, ValidationReport, ValidationScorecard


def _build_report() -> ValidationReport:
    module_result = ValidationModuleResult(
        module_name="stationarity",
        status="pass",
        summary="Stable enough.",
        metrics={"stationarity_score": 0.8},
        diagnostics={"rolling_mean_stability": {"window_10": 0.8}},
        artifacts={},
        recommendations=["Continue into strategy construction with skepticism."],
    )
    scorecard = ValidationScorecard(
        composite_score=0.8,
        entries=(ScorecardEntry("stationarity", "pass", "stationarity_score", 0.8, {"stationarity_score": 0.8}),),
        pass_count=1,
        warn_count=0,
        fail_count=0,
        error_count=0,
        evidence_coverage=1.0,
    )
    return ValidationReport(
        subject_type="feature",
        subject_name="entropy_feature",
        overall_status="probation",
        module_results=(module_result,),
        scorecard=scorecard,
        blocking_issues=(),
        warnings=(),
        next_actions=("Continue.",),
    )


def test_json_report_is_machine_readable() -> None:
    report = _build_report()
    payload = json.loads(render_json_report(report))

    assert payload["subject_name"] == "entropy_feature"
    assert payload["scorecard"]["composite_score"] == 0.8


def test_markdown_report_contains_expected_sections() -> None:
    report = _build_report()
    markdown = render_markdown_report(report)

    assert "# Validation Report: entropy_feature" in markdown
    assert "## Module Results" in markdown
    assert "## Next Actions" in markdown
    assert "- Diagnostics:" in markdown
    assert "`rolling_mean_stability`:" in markdown
