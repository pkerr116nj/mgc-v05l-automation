"""Typed reporting contracts for module and pipeline outputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ModuleStatus = Literal["pass", "warn", "fail", "error"]
OverallStatus = Literal["reject", "insufficient_evidence", "probation", "promote"]


@dataclass(frozen=True)
class ValidationModuleResult:
    module_name: str
    status: ModuleStatus
    summary: str
    metrics: dict[str, float | int | str | bool | None]
    diagnostics: dict[str, Any]
    artifacts: dict[str, Any]
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScorecardEntry:
    module_name: str
    status: ModuleStatus
    primary_score_name: str | None
    primary_score_value: float | None
    secondary_scores: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationScorecard:
    composite_score: float
    entries: tuple[ScorecardEntry, ...]
    pass_count: int
    warn_count: int
    fail_count: int
    error_count: int
    evidence_coverage: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "composite_score": self.composite_score,
            "entries": [entry.to_dict() for entry in self.entries],
            "pass_count": self.pass_count,
            "warn_count": self.warn_count,
            "fail_count": self.fail_count,
            "error_count": self.error_count,
            "evidence_coverage": self.evidence_coverage,
        }


@dataclass(frozen=True)
class ValidationReport:
    subject_type: Literal["feature", "strategy"]
    subject_name: str
    overall_status: OverallStatus
    module_results: tuple[ValidationModuleResult, ...]
    scorecard: ValidationScorecard
    blocking_issues: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_type": self.subject_type,
            "subject_name": self.subject_name,
            "overall_status": self.overall_status,
            "module_results": [result.to_dict() for result in self.module_results],
            "scorecard": self.scorecard.to_dict(),
            "blocking_issues": list(self.blocking_issues),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }
