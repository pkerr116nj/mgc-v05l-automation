"""Markdown rendering for validation reports."""

from __future__ import annotations

from .models import ValidationReport


def _append_structured(lines: list[str], value, *, indent: int) -> None:
    prefix = "  " * indent + "- "
    if isinstance(value, dict):
        for key, item in sorted(value.items()):
            if isinstance(item, (dict, list, tuple)):
                lines.append(f"{prefix}`{key}`:")
                _append_structured(lines, item, indent=indent + 1)
            else:
                lines.append(f"{prefix}`{key}`: `{item}`")
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, (dict, list, tuple)):
                lines.append(f"{prefix}")
                _append_structured(lines, item, indent=indent + 1)
            else:
                lines.append(f"{prefix}`{item}`")
        return
    lines.append(f"{prefix}`{value}`")


def render_markdown_report(report: ValidationReport) -> str:
    lines: list[str] = [
        f"# Validation Report: {report.subject_name}",
        "",
        f"- Subject type: `{report.subject_type}`",
        f"- Overall verdict: `{report.overall_status}`",
        f"- Composite score: `{report.scorecard.composite_score:.3f}`",
        "",
        "## Key Strengths",
    ]
    strengths = [result.summary for result in report.module_results if result.status == "pass"][:5]
    if strengths:
        lines.extend(f"- {summary}" for summary in strengths)
    else:
        lines.append("- No clear strengths yet; treat this as unproven research.")

    lines.extend(["", "## Blocking Issues"])
    if report.blocking_issues:
        lines.extend(f"- {item}" for item in report.blocking_issues)
    else:
        lines.append("- None currently blocking, but evidence may still be incomplete.")

    lines.extend(["", "## Warnings"])
    if report.warnings:
        lines.extend(f"- {item}" for item in report.warnings)
    else:
        lines.append("- None.")

    lines.extend(["", "## Module Results"])
    for result in report.module_results:
        lines.append(f"### {result.module_name}")
        lines.append(f"- Status: `{result.status}`")
        lines.append(f"- Summary: {result.summary}")
        if result.metrics:
            lines.append("- Metrics:")
            for key, value in sorted(result.metrics.items()):
                lines.append(f"  - `{key}`: `{value}`")
        if result.diagnostics:
            lines.append("- Diagnostics:")
            _append_structured(lines, result.diagnostics, indent=1)
        if result.artifacts:
            lines.append("- Artifacts:")
            _append_structured(lines, result.artifacts, indent=1)
        if result.recommendations:
            lines.append("- Recommendations:")
            for recommendation in result.recommendations:
                lines.append(f"  - {recommendation}")

    lines.extend(["", "## Promotion Recommendation", f"- `{report.overall_status}`"])
    lines.extend(["", "## Next Actions"])
    if report.next_actions:
        lines.extend(f"- {item}" for item in report.next_actions)
    else:
        lines.append("- Continue collecting evidence.")
    return "\n".join(lines) + "\n"
