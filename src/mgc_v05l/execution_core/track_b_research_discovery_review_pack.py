"""Human-readable review pack for diagnostic research discovery candidates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_research_discovery_engine import (
    CANDIDATES_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_DISCOVERY_DIR,
    SUMMARY_JSON as DISCOVERY_SUMMARY_JSON,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ENRICHMENT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_LAYER_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CANDIDATES_PATH = DEFAULT_DISCOVERY_DIR / CANDIDATES_JSONL
DEFAULT_DISCOVERY_SUMMARY_PATH = DEFAULT_DISCOVERY_DIR / DISCOVERY_SUMMARY_JSON
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_DISCOVERY_DIR

REVIEW_PACK_JSON = "research_discovery_review_pack.json"
REVIEW_PACK_MD = "research_discovery_review_pack.md"
TOP10_MD = "research_discovery_top10.md"
RESEARCH_GRADE_MD = "research_discovery_research_grade_candidates.md"
MANUAL_QUEUE_MD = "research_discovery_manual_review_queue.md"
GUARDRAILS_MD = "research_discovery_review_guardrails.md"

SCHEMA_VERSION = "track_b_research_discovery_review_pack_v1"


@dataclass(frozen=True)
class ResearchDiscoveryReviewPackResult:
    review_pack: dict[str, Any]
    json_path: Path
    markdown_path: Path
    top10_path: Path
    research_grade_path: Path
    manual_queue_path: Path
    guardrails_path: Path


def run_research_discovery_review_pack(
    *,
    candidates_path: Path = DEFAULT_CANDIDATES_PATH,
    discovery_summary_path: Path = DEFAULT_DISCOVERY_SUMMARY_PATH,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> ResearchDiscoveryReviewPackResult:
    generated_at = _coerce_now(now)
    candidates = _read_jsonl(candidates_path)
    discovery_summary = _read_json_mapping(discovery_summary_path)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    review_pack = build_research_discovery_review_pack(
        candidates,
        discovery_summary=discovery_summary,
        outcome_count=len(outcomes),
        enrichment_count=len(enrichments),
        generated_at=generated_at,
        source_paths={
            "research_discovery_candidates": candidates_path,
            "research_discovery_summary": discovery_summary_path,
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_enrichment": enrichments_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = output_dir / REVIEW_PACK_JSON
    write_bounded_snapshot_json(json_path, review_pack, config=config)
    markdown_path = output_dir / REVIEW_PACK_MD
    markdown_path.write_text(render_review_pack_markdown(review_pack), encoding="utf-8")
    top10_path = output_dir / TOP10_MD
    top10_path.write_text(render_candidate_section_markdown("Research Discovery Top 10", review_pack["top10_overall"]), encoding="utf-8")
    research_grade_path = output_dir / RESEARCH_GRADE_MD
    research_grade_path.write_text(
        render_candidate_section_markdown("Research-Grade Candidates", review_pack["research_grade_candidates"]),
        encoding="utf-8",
    )
    manual_queue_path = output_dir / MANUAL_QUEUE_MD
    manual_queue_path.write_text(render_candidate_section_markdown("Manual Review Queue", review_pack["manual_review_queue"]), encoding="utf-8")
    guardrails_path = output_dir / GUARDRAILS_MD
    guardrails_path.write_text(render_review_guardrails_markdown(review_pack), encoding="utf-8")
    return ResearchDiscoveryReviewPackResult(
        review_pack=review_pack,
        json_path=json_path,
        markdown_path=markdown_path,
        top10_path=top10_path,
        research_grade_path=research_grade_path,
        manual_queue_path=manual_queue_path,
        guardrails_path=guardrails_path,
    )


def build_research_discovery_review_pack(
    candidates: Sequence[Mapping[str, Any]],
    *,
    discovery_summary: Mapping[str, Any],
    outcome_count: int,
    enrichment_count: int,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    manual = select_manual_review_candidates(candidates)
    top10 = manual[:10]
    research_grade = [candidate for candidate in candidates if candidate.get("confidence_class") == "RESEARCH_GRADE"]
    category_top = _top_by_family(manual)
    reviewed = _dedupe_candidates([*top10, *research_grade, *category_top])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "input_counts": {
            "candidates": len(candidates),
            "manual_review_candidates": len(manual),
            "research_grade_candidates": len(research_grade),
            "outcomes": outcome_count,
            "enrichments": enrichment_count,
        },
        "discovery_summary_counts": discovery_summary.get("candidate_counts"),
        "top10_overall": [_review_candidate(candidate) for candidate in top10],
        "research_grade_candidates": [_review_candidate(candidate) for candidate in research_grade],
        "manual_review_queue": [_review_candidate(candidate) for candidate in manual],
        "top_by_family": [_review_candidate(candidate) for candidate in category_top],
        "reviewed_candidates": [_review_candidate(candidate) for candidate in reviewed],
        "what_not_to_conclude_yet": _what_not_to_conclude_yet(),
        "next_research_actions": _next_research_actions(),
        "guardrails": _guardrails(),
    }


def select_manual_review_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(candidate) for candidate in candidates if candidate.get("recommendation_level") == "MANUAL_REVIEW"],
        key=lambda row: (
            _sample_rank(row.get("confidence_class")),
            row.get("ranking_score") or 0.0,
            abs(row.get("effect_size_proxy") or 0.0),
            row.get("sample_size") or 0,
        ),
        reverse=True,
    )


def select_top_candidates(candidates: Sequence[Mapping[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    return select_manual_review_candidates(candidates)[:limit]


def _review_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = candidate.get("metrics") or {}
    data_quality_flags = list(candidate.get("data_quality_flags") or [])
    return {
        "candidate_id": candidate.get("candidate_id"),
        "family": candidate.get("family"),
        "hypothesis_text": candidate.get("hypothesis_text"),
        "dimensions": candidate.get("dimensions"),
        "filters": candidate.get("filters"),
        "sample_size": candidate.get("sample_size"),
        "sample_class": candidate.get("confidence_class"),
        "comparison_group": candidate.get("comparison_group"),
        "win_rate": metrics.get("win_rate"),
        "average_pnl_proxy": metrics.get("average_pnl_proxy"),
        "median_realized_points": metrics.get("median_realized_points"),
        "best_trade": metrics.get("best_trade"),
        "worst_trade": metrics.get("worst_trade"),
        "average_hold_seconds": metrics.get("average_hold_seconds"),
        "effect_size_proxy": candidate.get("effect_size_proxy"),
        "data_quality_flags": data_quality_flags,
        "context_validity_requirements": candidate.get("context_validity_requirements"),
        "why_flagged": _why_flagged(candidate),
        "supporting_evidence": _supporting_evidence(candidate),
        "weakening_evidence": _weakening_evidence(candidate),
        "falsification_test": _falsification_test(candidate),
        "recommended_next_research_action": _next_action_for_candidate(candidate),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
        "source_refs": candidate.get("source_refs"),
    }


def _why_flagged(candidate: Mapping[str, Any]) -> str:
    return (
        f"{candidate.get('family')} has sample class {candidate.get('confidence_class')} with effect-size proxy "
        f"{candidate.get('effect_size_proxy')} and recommendation {candidate.get('recommendation_level')}."
    )


def _supporting_evidence(candidate: Mapping[str, Any]) -> list[str]:
    metrics = candidate.get("metrics") or {}
    evidence = [
        f"Sample size: {candidate.get('sample_size')}",
        f"Average P&L proxy: {metrics.get('average_pnl_proxy')}",
        f"Win rate: {metrics.get('win_rate')}",
        f"Effect-size proxy: {candidate.get('effect_size_proxy')}",
    ]
    if candidate.get("context_validity_requirements"):
        evidence.append(f"Context validity requirements: {candidate.get('context_validity_requirements')}")
    return evidence


def _weakening_evidence(candidate: Mapping[str, Any]) -> list[str]:
    flags = list(candidate.get("data_quality_flags") or [])
    evidence = []
    if candidate.get("confidence_class") in {"EXPLORATORY", "PRELIMINARY"}:
        evidence.append(f"Sample class is {candidate.get('confidence_class')}; do not overclaim.")
    if flags:
        evidence.append(f"Data-quality flags: {', '.join(flags)}")
    if not evidence:
        evidence.append("No major automatic weakening evidence beyond standard discovery/confirmation split.")
    return evidence


def _falsification_test(candidate: Mapping[str, Any]) -> str:
    return (
        "Track this cohort on future outcomes or a holdout window; the hypothesis weakens if the effect-size proxy "
        "compresses toward baseline or reverses while data-quality coverage improves."
    )


def _next_action_for_candidate(candidate: Mapping[str, Any]) -> str:
    if candidate.get("confidence_class") == "RESEARCH_GRADE":
        return "Run a follow-up holdout/forward validation report; keep diagnostic-only."
    if "missing_mfe" in set(candidate.get("data_quality_flags") or ()):
        return "Manually review top trade examples and improve MFE/MAE coverage before exit-quality conclusions."
    return "Manual research review; validate against future trade outcomes before any model or policy discussion."


def render_review_pack_markdown(review_pack: Mapping[str, Any]) -> str:
    counts = review_pack.get("input_counts") or {}
    lines = [
        "# Research Discovery Review Pack",
        "",
        f"- Generated at: {review_pack.get('generated_at')}",
        f"- Candidates: {counts.get('candidates')}",
        f"- Manual-review candidates: {counts.get('manual_review_candidates')}",
        f"- Research-grade candidates: {counts.get('research_grade_candidates')}",
        "",
        "## Top 10",
        "",
    ]
    lines.extend(_candidate_bullets(review_pack.get("top10_overall") or []))
    lines.extend(["", "## What Not To Conclude Yet", ""])
    for item in review_pack.get("what_not_to_conclude_yet") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## Next Research Actions", ""])
    for item in review_pack.get("next_research_actions") or []:
        lines.append(f"- {item}")
    lines.extend(["", "No production changes, gates, strategy edits, or broker/runtime actions are recommended.", ""])
    return "\n".join(lines)


def render_candidate_section_markdown(title: str, candidates: Sequence[Mapping[str, Any]]) -> str:
    lines = [f"# {title}", ""]
    if not candidates:
        lines.append("No candidates in this section.")
        lines.append("")
        return "\n".join(lines)
    lines.extend(_candidate_bullets(candidates))
    return "\n".join(lines)


def render_review_guardrails_markdown(review_pack: Mapping[str, Any]) -> str:
    lines = ["# Research Discovery Review Guardrails", ""]
    for key, value in (review_pack.get("guardrails") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "Review-pack outputs are hypotheses only. They must not be used as execution instructions.", ""])
    return "\n".join(lines)


def _candidate_bullets(candidates: Sequence[Mapping[str, Any]]) -> list[str]:
    lines: list[str] = []
    for idx, candidate in enumerate(candidates, start=1):
        lines.extend(
            [
                f"## {idx}. {candidate.get('candidate_id')}",
                "",
                f"- Hypothesis: {candidate.get('hypothesis_text')}",
                f"- Dimensions: {candidate.get('dimensions')}",
                f"- Filters: {candidate.get('filters')}",
                f"- Sample: {candidate.get('sample_size')} ({candidate.get('sample_class')})",
                f"- Win rate: {candidate.get('win_rate')}",
                f"- Average P&L proxy: {candidate.get('average_pnl_proxy')}",
                f"- Median realized points: {candidate.get('median_realized_points')}",
                f"- Effect-size proxy: {candidate.get('effect_size_proxy')}",
                f"- Data-quality flags: {candidate.get('data_quality_flags')}",
                f"- Context validity requirements: {candidate.get('context_validity_requirements')}",
                f"- Why flagged: {candidate.get('why_flagged')}",
                f"- Weakening evidence: {candidate.get('weakening_evidence')}",
                f"- Falsification test: {candidate.get('falsification_test')}",
                f"- Next action: {candidate.get('recommended_next_research_action')}",
                f"- Production recommendation: {candidate.get('production_recommendation')}",
                f"- Trading gate: {candidate.get('trading_gate')}",
                "",
            ]
        )
    return lines


def _top_by_family(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        family = str(candidate.get("family") or "UNKNOWN")
        if family not in selected:
            selected[family] = dict(candidate)
    return list(selected.values())


def _dedupe_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id or candidate_id in seen:
            continue
        seen.add(candidate_id)
        result.append(dict(candidate))
    return result


def _sample_rank(sample_class: Any) -> int:
    return {"RESEARCH_GRADE": 4, "DEVELOPING": 3, "PRELIMINARY": 2, "EXPLORATORY": 1}.get(str(sample_class), 0)


def _what_not_to_conclude_yet() -> list[str]:
    return [
        "Do not infer production readiness from discovery candidates.",
        "Do not infer strategy changes or gates from descriptive cohorts.",
        "R proxy is unavailable, so P&L proxy is not a full risk-adjusted result.",
        "MFE/MAE is sparse, so exit-quality conclusions remain limited.",
        "GRE coverage is valid only for the current Gold-context window; unavailable/stale context is not edge evidence.",
        "Most candidates are exploratory or preliminary and require future/holdout validation.",
    ]


def _next_research_actions() -> list[str]:
    return [
        "Validate top candidates against future trade outcomes.",
        "Rerun discovery after the next provider refresh expands context coverage.",
        "Compare candidates with R proxy once initial risk is available.",
        "Manually review best/worst trade examples for top candidates.",
        "Improve MFE/MAE coverage before drawing exit-quality conclusions.",
    ]


def _guardrails() -> dict[str, bool]:
    return {
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
        "runtime_integration": False,
        "broker_actions": False,
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return rows


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
