"""Evaluate canonical candidate sources for GRE shadow observations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_gre_shadow_observation import OBSERVATIONS_JSONL, SELECTED_AVWAP_ANCHOR
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_shadow_candidate_source_evaluation_v1"
EVALUATION_JSON = "shadow_candidate_source_evaluation.json"
EVALUATION_MD = "shadow_candidate_source_evaluation.md"
JOIN_QUALITY_MD = "shadow_candidate_join_quality.md"
REPRESENTATIVENESS_MD = "shadow_candidate_representativeness.md"
CONTRACT_MD = "canonical_shadow_candidate_contract.md"
RECOMMENDATIONS_MD = "shadow_candidate_s2_recommendations.md"
CONFIDENCE_BANDS = (("0-20", 0, 20), ("20-40", 20, 40), ("40-60", 40, 60), ("60-80", 60, 80), ("80-100", 80, 101))
GOLD_ROOTS = {"GC", "MGC"}


@dataclass(frozen=True)
class CandidateSourceEvaluationResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    join_quality_path: Path
    representativeness_path: Path
    contract_path: Path
    recommendations_path: Path


def run_shadow_candidate_source_evaluation(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    observation_rows_path: Path | None = None,
    historical_rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> CandidateSourceEvaluationResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    observation_source = observation_rows_path or gold_dir / OBSERVATIONS_JSONL
    historical_source = historical_rows_path or gold_dir / BACKFILL_ROWS_JSONL
    crfd_source = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    report = build_shadow_candidate_source_evaluation(
        observation_rows=_read_jsonl(observation_source),
        historical_rows=_read_jsonl(historical_source),
        crfd_rows=_read_jsonl(crfd_source),
        post_governance_rows=_read_post_governance_rows(output_root),
        generated_at=generated_at,
        observation_rows_path=observation_source,
        historical_rows_path=historical_source,
        crfd_rows_path=crfd_source,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / EVALUATION_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / EVALUATION_MD
    markdown_path.write_text(render_evaluation_markdown(report), encoding="utf-8")
    join_path = gold_dir / JOIN_QUALITY_MD
    join_path.write_text(render_join_quality_markdown(report), encoding="utf-8")
    rep_path = gold_dir / REPRESENTATIVENESS_MD
    rep_path.write_text(render_representativeness_markdown(report), encoding="utf-8")
    contract_path = gold_dir / CONTRACT_MD
    contract_path.write_text(render_contract_markdown(report), encoding="utf-8")
    recommendations_path = gold_dir / RECOMMENDATIONS_MD
    recommendations_path.write_text(render_recommendations_markdown(report), encoding="utf-8")
    return CandidateSourceEvaluationResult(report, json_path, markdown_path, join_path, rep_path, contract_path, recommendations_path)


def build_shadow_candidate_source_evaluation(
    *,
    observation_rows: Sequence[Mapping[str, Any]],
    historical_rows: Sequence[Mapping[str, Any]],
    crfd_rows: Sequence[Mapping[str, Any]],
    post_governance_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    observation_rows_path: Path | str,
    historical_rows_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    historical_reference = _historical_reference(historical_rows, crfd_rows)
    populations = [
        _evaluate_population(
            "blocked_intent_diagnostics",
            _from_observation_rows(observation_rows),
            historical_reference=historical_reference,
            description="Current blocked-intent latest/history diagnostics.",
        ),
        _evaluate_population(
            "post_governance_diagnostics",
            _from_post_governance_rows(post_governance_rows),
            historical_reference=historical_reference,
            description="Existing bridge/post-governance diagnostic reports.",
        ),
        _evaluate_population(
            "generated_crfd_observation_points",
            _from_crfd_rows(crfd_rows),
            historical_reference=historical_reference,
            description="Generated CRFD observation timestamps with feature context.",
        ),
        _evaluate_population(
            "gold_only_timestamped_strategy_intent_candidates",
            [row for row in _from_observation_rows(observation_rows) if row.get("is_gold")],
            historical_reference=historical_reference,
            description="Gold-only timestamped candidates filtered from strategy/blocked-intent diagnostics.",
        ),
    ]
    recommendation = _recommend_canonical(populations)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_effect": False,
        "source_artifacts": {
            "r21_observations": str(observation_rows_path),
            "r18_historical_rows": str(historical_rows_path),
            "crfd_rows": str(crfd_rows_path),
        },
        "historical_reference_population": historical_reference,
        "candidate_populations": populations,
        "canonical_recommendation": recommendation,
        "future_architecture": [
            "Strategy diagnostics",
            "Canonical Candidate Adapter",
            "GRE",
            "CRFD",
            "Shadow Policy Evaluation",
            "Shadow Observation Artifact",
        ],
        "safety_contract": {
            "runtime_hook": False,
            "trading_gate": False,
            "broker_actions": False,
            "managed_exit_changes": False,
            "gre_scoring_changes": False,
            "strategy_changes": False,
        },
    }


def _evaluate_population(
    population_id: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    historical_reference: Mapping[str, Any],
    description: str,
) -> dict[str, Any]:
    count = len(rows)
    join = _join_quality(rows)
    representativeness = _representativeness(rows, historical_reference)
    criteria = {
        "observation_count": count,
        "gold_coverage": _rate(sum(1 for row in rows if row.get("is_gold")), count),
        "session_coverage": _count_by(rows, "session"),
        "symbol_coverage": _count_by(rows, "symbol"),
        "timestamp_precision": _timestamp_precision(rows),
        "intended_direction_availability": _rate(sum(1 for row in rows if row.get("intended_direction") in {"LONG", "SHORT"}), count),
        "lane_strategy_linkage": _rate(sum(1 for row in rows if row.get("strategy_id") or row.get("lane_id")), count),
        "ability_to_compute_shadow_policies": _rate(sum(1 for row in rows if row.get("policy_computable")), count),
        "forward_validation_suitability": _forward_validation_suitability(population_id, rows),
        "bounded_writer_compatibility": True,
        "runtime_independence": True,
        "replay_suitability": _replay_suitability(population_id, rows),
    }
    return {
        "population_id": population_id,
        "description": description,
        "criteria": criteria,
        "join_quality": join,
        "representativeness": representativeness,
        "score": _population_score(criteria, join, representativeness),
        "limitations": _limitations(population_id, criteria, join, representativeness),
    }


def _from_observation_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        contract = str(row.get("contract") or row.get("symbol") or "UNKNOWN").upper()
        result.append(
            {
                "source_population": "blocked_intent_diagnostics",
                "timestamp": row.get("generated_at"),
                "symbol": row.get("symbol") or _root_symbol(contract),
                "contract": contract,
                "is_gold": _root_symbol(contract) in GOLD_ROOTS,
                "session": row.get("session") or "UNKNOWN",
                "strategy_id": row.get("strategy_id"),
                "lane_id": row.get("lane_id"),
                "intended_direction": row.get("intended_direction"),
                "gre_label": row.get("gre_label"),
                "gre_confidence": row.get("gre_confidence"),
                "vwap_relation": row.get("vwap_relation"),
                "globex_avwap_relation": row.get(f"avwap_{SELECTED_AVWAP_ANCHOR}_relation"),
                "gre_joined": row.get("gre_label") is not None,
                "crfd_joined": (row.get("source_refs") or {}).get("crfd_observation_time") is not None,
                "policy_computable": row.get("intended_direction") in {"LONG", "SHORT"}
                and row.get("gre_label") in {"LONG", "SHORT"}
                and row.get(f"avwap_{SELECTED_AVWAP_ANCHOR}_relation") in {"above_avwap", "below_avwap", "at_avwap"},
            }
        )
    return result


def _from_crfd_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        contract = str(row.get("contract") or "UNKNOWN").upper()
        relation = row.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}") or "unavailable"
        result.append(
            {
                "source_population": "generated_crfd_observation_points",
                "timestamp": row.get("observation_time"),
                "symbol": row.get("instrument") or _root_symbol(contract),
                "contract": contract,
                "is_gold": _root_symbol(contract) in GOLD_ROOTS,
                "session": row.get("session") or row.get("session_label") or "UNKNOWN",
                "strategy_id": None,
                "lane_id": None,
                "intended_direction": None,
                "gre_label": None,
                "gre_confidence": None,
                "vwap_relation": row.get("vwap_relation") or "unavailable",
                "globex_avwap_relation": relation,
                "gre_joined": False,
                "crfd_joined": True,
                "policy_computable": relation in {"above_avwap", "below_avwap", "at_avwap"},
            }
        )
    return result


def _from_post_governance_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        contract = str(row.get("contract") or row.get("symbol") or "UNKNOWN").upper()
        result.append(
            {
                "source_population": "post_governance_diagnostics",
                "timestamp": row.get("generated_at"),
                "symbol": row.get("symbol") or _root_symbol(contract),
                "contract": contract,
                "is_gold": _root_symbol(contract) in GOLD_ROOTS,
                "session": row.get("session") or "UNKNOWN",
                "strategy_id": row.get("strategy_id"),
                "lane_id": row.get("lane_id"),
                "intended_direction": row.get("intended_direction"),
                "gre_label": None,
                "gre_confidence": None,
                "vwap_relation": "unavailable",
                "globex_avwap_relation": "unavailable",
                "gre_joined": False,
                "crfd_joined": False,
                "policy_computable": False,
            }
        )
    return result


def _historical_reference(historical_rows: Sequence[Mapping[str, Any]], crfd_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    index = _CrfdIndex(crfd_rows)
    enriched: list[dict[str, Any]] = []
    for row in historical_rows:
        crfd = index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=_parse_datetime(row.get("gre_generated_at"))) or {}
        item = {
            "gre_label": row.get("regime_label"),
            "gre_confidence": row.get("confidence"),
            "direction": row.get("directional_bias"),
            "session": crfd.get("session") or row.get("session") or "UNKNOWN",
            "symbol": row.get("contract") or "GC",
            "vwap_relation": crfd.get("vwap_relation") or "unavailable",
            "globex_avwap_relation": crfd.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}") or "unavailable",
            "provider": ((row.get("provider_metadata") or {}).get("provider_kind") if isinstance(row.get("provider_metadata"), Mapping) else None)
            or row.get("source_mode")
            or "unknown",
        }
        enriched.append(item)
    return {
        "observation_count": len(enriched),
        "gre_label_counts": _count_by(enriched, "gre_label"),
        "direction_counts": _count_by(enriched, "direction"),
        "confidence_band_counts": _count_values([_confidence_band(row.get("gre_confidence")) for row in enriched]),
        "session_counts": _count_by(enriched, "session"),
        "symbol_counts": _count_by(enriched, "symbol"),
        "vwap_relation_counts": _count_by(enriched, "vwap_relation"),
        "globex_avwap_relation_counts": _count_by(enriched, "globex_avwap_relation"),
        "provider_counts": _count_by(enriched, "provider"),
    }


def _join_quality(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    gre = sum(1 for row in rows if row.get("gre_joined"))
    crfd = sum(1 for row in rows if row.get("crfd_joined"))
    both = sum(1 for row in rows if row.get("gre_joined") and row.get("crfd_joined"))
    timestamps = sum(1 for row in rows if row.get("timestamp"))
    return {
        "gre_join_rate": _rate(gre, count),
        "crfd_join_rate": _rate(crfd, count),
        "both_join_rate": _rate(both, count),
        "timestamp_alignment_quality": _rate(timestamps, count),
        "missing_reasons": _missing_reasons(rows),
        "stale_data_frequency": None,
    }


def _representativeness(rows: Sequence[Mapping[str, Any]], historical: Mapping[str, Any]) -> dict[str, Any]:
    count = len(rows)
    label_counts = _count_by(rows, "gre_label")
    confidence_counts = _count_values([_confidence_band(row.get("gre_confidence")) for row in rows])
    session_counts = _count_by(rows, "session")
    vwap_counts = _count_by(rows, "vwap_relation")
    avwap_counts = _count_by(rows, "globex_avwap_relation")
    score_parts = [
        _distribution_overlap(label_counts, historical.get("gre_label_counts") or {}),
        _distribution_overlap(confidence_counts, historical.get("confidence_band_counts") or {}),
        _distribution_overlap(session_counts, historical.get("session_counts") or {}),
        _distribution_overlap(vwap_counts, historical.get("vwap_relation_counts") or {}),
        _distribution_overlap(avwap_counts, historical.get("globex_avwap_relation_counts") or {}),
    ]
    return {
        "score": round(sum(score_parts) / len(score_parts), 4) if score_parts else 0.0,
        "observation_count": count,
        "regime_distribution": label_counts,
        "long_short_proportion": _rate(sum(1 for row in rows if row.get("gre_label") in {"LONG", "SHORT"}), count),
        "confidence_distribution": confidence_counts,
        "session_distribution": session_counts,
        "vwap_relation": vwap_counts,
        "globex_avwap_relation": avwap_counts,
        "candidate_frequency": count,
        "provider_coverage": _count_by(rows, "source_population"),
    }


def _recommend_canonical(populations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_id = {row["population_id"]: row for row in populations}
    selected = "gold_only_timestamped_strategy_intent_candidates"
    return {
        "canonical_shadow_observation_candidate_source_v1": selected,
        "why_selected": [
            "It is closest to the future live-shadow question: what would the shadow gate have said for actual Gold strategy-intent candidates?",
            "It preserves lane/strategy linkage and intended direction, unlike generated CRFD points.",
            "It avoids non-Gold dilution in raw blocked-intent diagnostics.",
            "It can remain diagnostic and source-adapter based, so no runtime hook is required for this phase.",
        ],
        "tradeoffs": (by_id.get(selected) or {}).get("limitations", []),
        "limitations": [
            "Current sample is only as good as existing diagnostics and may still use latest GRE context unless timestamped joins are added.",
            "Post-governance artifacts should be added as another adapter input once they expose consistent strategy intent fields.",
            "CRFD points remain useful as research controls, not as canonical live-shadow candidates.",
        ],
        "expected_future_evolution": [
            "Add timestamped GRE/CRFD joins per candidate.",
            "Unify blocked-intent and post-governance diagnostics behind one adapter schema.",
            "Reuse the adapter for NRE, ERE, and TRE by swapping regime plugin/context providers.",
        ],
    }


def _population_score(criteria: Mapping[str, Any], join: Mapping[str, Any], rep: Mapping[str, Any]) -> dict[str, Any]:
    components = {
        "gold_coverage": criteria.get("gold_coverage") or 0.0,
        "intended_direction": criteria.get("intended_direction_availability") or 0.0,
        "lane_strategy_linkage": criteria.get("lane_strategy_linkage") or 0.0,
        "both_join": join.get("both_join_rate") or 0.0,
        "representativeness": rep.get("score") or 0.0,
        "replay_suitability": criteria.get("replay_suitability") or 0.0,
    }
    return {"components": components, "overall": round(sum(components.values()) / len(components), 4)}


def _limitations(population_id: str, criteria: Mapping[str, Any], join: Mapping[str, Any], rep: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    if not criteria.get("observation_count"):
        notes.append("No usable observations found in current artifacts.")
    if (criteria.get("gold_coverage") or 0.0) < 1.0:
        notes.append("Includes non-Gold observations unless filtered.")
    if (criteria.get("intended_direction_availability") or 0.0) < 1.0:
        notes.append("Intended direction is incomplete or absent.")
    if (join.get("both_join_rate") or 0.0) < 1.0:
        notes.append("GRE/CRFD joins are not both complete.")
    if (rep.get("score") or 0.0) < 0.5:
        notes.append("Distribution differs materially from R18 historical research population.")
    if population_id == "generated_crfd_observation_points":
        notes.append("Good research control but not a true strategy-intent population.")
    if population_id == "post_governance_diagnostics":
        notes.append("Current post-governance artifacts are sparse and not consistently candidate-shaped.")
    return notes


def render_evaluation_markdown(report: Mapping[str, Any]) -> str:
    lines = ["# Shadow Candidate Source Evaluation", "", f"- Generated at: {report.get('generated_at')}", ""]
    lines.extend(["| Population | Observations | Gold coverage | Both join | Representativeness | Overall score |", "|---|---:|---:|---:|---:|---:|"])
    for pop in report.get("candidate_populations") or []:
        lines.append(
            f"| {pop.get('population_id')} | {pop.get('criteria', {}).get('observation_count')} | "
            f"{pop.get('criteria', {}).get('gold_coverage')} | {pop.get('join_quality', {}).get('both_join_rate')} | "
            f"{pop.get('representativeness', {}).get('score')} | {pop.get('score', {}).get('overall')} |"
        )
    rec = report.get("canonical_recommendation") or {}
    lines.extend(["", f"Canonical V1: {rec.get('canonical_shadow_observation_candidate_source_v1')}", ""])
    return "\n".join(lines)


def render_join_quality_markdown(report: Mapping[str, Any]) -> str:
    lines = ["# Shadow Candidate Join Quality", "", "| Population | GRE join | CRFD join | Both | Timestamp quality | Missing reasons |", "|---|---:|---:|---:|---:|---|"]
    for pop in report.get("candidate_populations") or []:
        join = pop.get("join_quality") or {}
        lines.append(
            f"| {pop.get('population_id')} | {join.get('gre_join_rate')} | {join.get('crfd_join_rate')} | "
            f"{join.get('both_join_rate')} | {join.get('timestamp_alignment_quality')} | {_format_counts(join.get('missing_reasons'))} |"
        )
    return "\n".join(lines) + "\n"


def render_representativeness_markdown(report: Mapping[str, Any]) -> str:
    lines = ["# Shadow Candidate Representativeness", "", "| Population | Score | LONG/SHORT proportion | Sessions | AVWAP |", "|---|---:|---:|---|---|"]
    for pop in report.get("candidate_populations") or []:
        rep = pop.get("representativeness") or {}
        lines.append(
            f"| {pop.get('population_id')} | {rep.get('score')} | {rep.get('long_short_proportion')} | "
            f"{_format_counts(rep.get('session_distribution'))} | {_format_counts(rep.get('globex_avwap_relation'))} |"
        )
    return "\n".join(lines) + "\n"


def render_contract_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("canonical_recommendation") or {}
    lines = [
        "# Canonical Shadow Candidate Contract",
        "",
        f"Canonical Shadow Observation Candidate Source V1: {rec.get('canonical_shadow_observation_candidate_source_v1')}",
        "",
        "## Flow",
    ]
    for step in report.get("future_architecture") or []:
        lines.append(f"- {step}")
    lines.extend(
        [
            "",
            "## Required Fields",
            "- observation_time",
            "- instrument/contract/symbol",
            "- strategy_id and/or lane_id when available",
            "- intended_direction",
            "- session",
            "- source artifact reference",
            "- diagnostic_only=true",
            "",
            "## Safety",
            "- Adapter remains diagnostic-only.",
            "- No runtime hook, broker action, trading gate, strategy mutation, or Managed Exit integration.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_recommendations_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("canonical_recommendation") or {}
    lines = [
        "# Shadow Candidate S2 Recommendations",
        "",
        f"- Canonical V1: {rec.get('canonical_shadow_observation_candidate_source_v1')}",
        "- R21 should migrate to the canonical adapter later: true",
        "- Runtime hook still unnecessary: true",
        "- Live shadow observation should consume the adapter, not individual artifact types: true",
        "- Adapter should be reusable for NRE/ERE/TRE: true",
        "- Production gate recommended: false",
        "",
        "## Why",
    ]
    for item in rec.get("why_selected") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## Limitations"])
    for item in rec.get("limitations") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(values, key=lambda item: item[0]) for contract, values in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        rows = self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if timestamp is not None and row_ts > timestamp:
                break
            candidate = row
        return candidate


def _read_post_governance_rows(output_root: Path) -> list[dict[str, Any]]:
    outputs_root = output_root.parent if output_root.name == "track_b_execution_core" else output_root
    rows: list[dict[str, Any]] = []
    for path in sorted((outputs_root / "reports" / "ibkr_runtime_route_dispatch").glob("*/ibkr_paper_strategy_bridge_report.json"))[:300]:
        try:
            if path.stat().st_size > 2 * 1024 * 1024:
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        lane = path.parent.name
        rows.append(
            {
                "source_path": str(path),
                "lane_id": payload.get("lane_id") or lane,
                "strategy_id": payload.get("strategy_id"),
                "symbol": payload.get("symbol") or _symbol_from_lane(lane),
                "contract": payload.get("contract") or payload.get("local_symbol") or _symbol_from_lane(lane),
                "intended_direction": _direction_from_payload_or_lane(payload, lane),
                "session": payload.get("session") or _session_from_lane(lane),
                "generated_at": payload.get("generated_at"),
            }
        )
    return rows


def _direction_from_payload_or_lane(payload: Mapping[str, Any], lane: str) -> str | None:
    for key in ("intended_direction", "direction", "side", "action"):
        value = payload.get(key)
        if value:
            text = str(value).upper()
            if text in {"LONG", "BUY", "BULLISH"}:
                return "LONG"
            if text in {"SHORT", "SELL", "BEARISH"}:
                return "SHORT"
    if lane.endswith("_long") or "_long_" in lane:
        return "LONG"
    if lane.endswith("_short") or "_short_" in lane:
        return "SHORT"
    return None


def _symbol_from_lane(lane: str) -> str:
    return str(lane).split("_", 1)[0].upper() if lane else "UNKNOWN"


def _session_from_lane(lane: str) -> str:
    text = str(lane).lower()
    if "london_late" in text:
        return "LONDON_LATE"
    if "london" in text:
        return "LONDON"
    if "globex" in text or "asia" in text:
        return "ASIA"
    if "us" in text or "ny" in text:
        return "US"
    return "UNKNOWN"


def _timestamp_precision(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "none"
    with_ts = sum(1 for row in rows if row.get("timestamp"))
    if with_ts == len(rows):
        return "observation_timestamp_available"
    if with_ts:
        return "partial_timestamp_available"
    return "missing_or_artifact_mtime_only"


def _forward_validation_suitability(population_id: str, rows: Sequence[Mapping[str, Any]]) -> float:
    if population_id == "generated_crfd_observation_points":
        return 1.0 if rows else 0.0
    if population_id == "gold_only_timestamped_strategy_intent_candidates":
        return _rate(sum(1 for row in rows if row.get("timestamp") and row.get("is_gold")), len(rows)) or 0.0
    return _rate(sum(1 for row in rows if row.get("timestamp")), len(rows)) or 0.0


def _replay_suitability(population_id: str, rows: Sequence[Mapping[str, Any]]) -> float:
    if population_id == "generated_crfd_observation_points":
        return 1.0 if rows else 0.0
    return _rate(sum(1 for row in rows if row.get("timestamp") and row.get("intended_direction")), len(rows)) or 0.0


def _missing_reasons(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    reasons: list[str] = []
    for row in rows:
        if not row.get("gre_joined"):
            reasons.append("missing_gre_join")
        if not row.get("crfd_joined"):
            reasons.append("missing_crfd_join")
        if not row.get("timestamp"):
            reasons.append("missing_timestamp")
        if row.get("intended_direction") not in {"LONG", "SHORT"}:
            reasons.append("missing_intended_direction")
        if row.get("globex_avwap_relation") in {None, "unavailable"}:
            reasons.append("missing_globex_avwap")
    return _count_values(reasons)


def _distribution_overlap(a: Mapping[str, int], b: Mapping[str, int]) -> float:
    total_a = sum(a.values())
    total_b = sum(b.values())
    if total_a <= 0 or total_b <= 0:
        return 0.0
    keys = set(a) | set(b)
    overlap = sum(min(a.get(key, 0) / total_a, b.get(key, 0) / total_b) for key in keys)
    return round(overlap, 4)


def _root_symbol(value: Any) -> str:
    root = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    return root or "UNKNOWN"


def _confidence_band(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "UNKNOWN"
    for label, low, high in CONFIDENCE_BANDS:
        if low <= number < high:
            return label
    return "UNKNOWN"


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return _count_values([row.get(key) if row.get(key) is not None else "UNKNOWN" for row in rows])


def _count_values(values: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        text = str(value if value is not None else "UNKNOWN")
        counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _format_counts(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "{}"
    return ", ".join(f"{key}={count}" for key, count in value.items()) or "{}"


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 4)


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


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
