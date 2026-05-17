"""Formal Track B Entry Acceptance exit research candidates.

These definitions are committed research metadata only. They do not wire a
strategy, submit orders, mutate lifecycle state, or make a candidate eligible
for PAPER/live execution.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class TrackBResearchAuthorityBoundary:
    """Authority guardrails for committed research candidate definitions."""

    research_offline_only: bool
    paper_eligible: bool
    live_eligible: bool
    runtime_wired: bool
    strategy_behavior_changes: bool
    broker_state_mutated: bool
    order_intent_created: bool
    lifecycle_mutated: bool


@dataclass(frozen=True)
class TrackBResearchProvenance:
    """Research study provenance for a candidate definition."""

    study_name: str
    study_root: str
    report_paths: tuple[str, ...]
    date_start: str
    date_end: str
    instruments: tuple[str, ...]
    evidence_scope: str


@dataclass(frozen=True)
class TrackBEntryExitResearchCandidate:
    """Static research candidate metadata for Track B entry/exit variants."""

    candidate_id: str
    label: str
    status: str
    candidate_family: str
    entry_basis: str
    exit_definition: dict[str, Any]
    evidence_metrics: dict[str, Any]
    provenance: TrackBResearchProvenance
    authority_boundary: TrackBResearchAuthorityBoundary
    notes: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        """Return an artifact-friendly dictionary payload."""

        return asdict(self)


_AUTHORITY_BOUNDARY = TrackBResearchAuthorityBoundary(
    research_offline_only=True,
    paper_eligible=False,
    live_eligible=False,
    runtime_wired=False,
    strategy_behavior_changes=False,
    broker_state_mutated=False,
    order_intent_created=False,
    lifecycle_mutated=False,
)

_PROVENANCE = TrackBResearchProvenance(
    study_name="exact-baseline exit improvement and 24-vs-36 extension studies",
    study_root=(
        "outputs/reports/entry_acceptance_research/full_history_batch/"
        "exact_baseline_exit_study"
    ),
    report_paths=(
        (
            "outputs/reports/entry_acceptance_research/full_history_batch/"
            "exact_baseline_exit_study/"
            "exact_baseline_exit_improvement_study_v1.md"
        ),
        (
            "outputs/reports/entry_acceptance_research/full_history_batch/"
            "exact_baseline_exit_study/"
            "exact_baseline_exit_improvement_study_v1.json"
        ),
        (
            "outputs/reports/entry_acceptance_research/full_history_batch/"
            "exact_baseline_exit_study/"
            "exact_baseline_24_vs_36_extension_rule_study_v1.md"
        ),
        (
            "outputs/reports/entry_acceptance_research/full_history_batch/"
            "exact_baseline_exit_study/"
            "exact_baseline_24_vs_36_extension_rule_study_v1.json"
        ),
    ),
    date_start="2020-01-01",
    date_end="2026-04-22",
    instruments=("GC", "MGC"),
    evidence_scope="GC/MGC combined exact-rule baseline episodes",
)


EXACT_BASELINE_FIXED_36B_EXIT = TrackBEntryExitResearchCandidate(
    candidate_id="track_b_exact_baseline_fixed_36b_exit_v1",
    label="Exact baseline plus fixed 36-bar exit",
    status="ACTIVE_RESEARCH_CANDIDATE",
    candidate_family="asiaEarlyNormalBreakoutRetestHoldLong",
    entry_basis="current exact-rule baseline episodes only",
    exit_definition={
        "exit_type": "fixed_horizon",
        "entry_side": "LONG",
        "exit_after_completed_bars": 36,
        "entry_price_basis": "next_bar_open",
        "cost_assumption_points_round_trip": 0.5,
        "position_management": "single_entry_single_exit",
    },
    evidence_metrics={
        "episodes": 856,
        "average_return_points": 0.306308,
        "median_return_points": -0.4,
        "win_rate": 0.46729,
        "profit_factor_proxy": 1.107389,
        "max_drawdown_proxy_points": 254.5,
        "comparison_note": "Best average return among studied exact-baseline fixed exits.",
    },
    provenance=_PROVENANCE,
    authority_boundary=_AUTHORITY_BOUNDARY,
    notes=(
        "Raw-return winner in the exact-baseline exit study.",
        "Research candidate only; not PAPER/live eligible.",
        "Requires later lifecycle-aware validation before any promotion discussion.",
    ),
)


EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT = TrackBEntryExitResearchCandidate(
    candidate_id="track_b_exact_baseline_adaptive_24_to_36_exit_v1",
    label="Exact baseline plus adaptive 24-to-36 risk-managed extension exit",
    status="ACTIVE_RESEARCH_CANDIDATE",
    candidate_family="asiaEarlyNormalBreakoutRetestHoldLong",
    entry_basis="current exact-rule baseline episodes only",
    exit_definition={
        "exit_type": "adaptive_24_to_36_extension",
        "entry_side": "LONG",
        "default_exit_after_completed_bars": 24,
        "extension_exit_after_completed_bars": 36,
        "entry_price_basis": "next_bar_open",
        "cost_assumption_points_round_trip": 0.5,
        "extension_conditions": {
            "net_progress_at_24b_points_gt": 0.0,
            "mfe_mae_ratio_at_24b_gte": 1.1,
            "giveback_pct_at_24b_lte": 0.4,
        },
        "position_management": "single_entry_single_exit",
    },
    evidence_metrics={
        "episodes": 856,
        "average_return_points": 0.273832,
        "median_return_points": -0.4,
        "win_rate": 0.455607,
        "profit_factor_proxy": 1.113051,
        "max_drawdown_proxy_points": 228.2,
        "branch_counts": {
            "exit_24b": 646,
            "extend_to_36b": 210,
        },
        "comparison_note": (
            "Better PF/DD profile than fixed 36; not the raw-return winner."
        ),
    },
    provenance=_PROVENANCE,
    authority_boundary=_AUTHORITY_BOUNDARY,
    notes=(
        "Risk-managed variant selected for improved PF/DD profile.",
        "Adaptive extension is intentionally not represented as the best average-return candidate.",
        "Research candidate only; no runtime or strategy integration authority.",
    ),
)


TRACK_B_ENTRY_EXIT_RESEARCH_CANDIDATES = (
    EXACT_BASELINE_FIXED_36B_EXIT,
    EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT,
)


def track_b_entry_exit_research_candidates() -> tuple[
    TrackBEntryExitResearchCandidate, ...
]:
    """Return formal Track B Entry Acceptance exit research candidates."""

    return TRACK_B_ENTRY_EXIT_RESEARCH_CANDIDATES


def track_b_entry_exit_research_candidate_payloads() -> list[dict[str, Any]]:
    """Return artifact-friendly candidate payloads for research tooling."""

    return [candidate.to_payload() for candidate in TRACK_B_ENTRY_EXIT_RESEARCH_CANDIDATES]


def get_track_b_entry_exit_research_candidate(
    candidate_id: str,
) -> TrackBEntryExitResearchCandidate:
    """Return a research candidate by id."""

    for candidate in TRACK_B_ENTRY_EXIT_RESEARCH_CANDIDATES:
        if candidate.candidate_id == candidate_id:
            return candidate
    raise KeyError(candidate_id)
