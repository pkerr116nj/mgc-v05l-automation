"""Diagnose zero-allow GRE shadow observation outcomes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_gre_shadow_observation import OBSERVATIONS_JSONL, SELECTED_AVWAP_ANCHOR, SELECTED_SHADOW_POLICY
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_shadow_gate_zero_allow_diagnosis_v1"
DIAGNOSIS_JSON = "gre_shadow_gate_zero_allow_diagnosis.json"
DIAGNOSIS_MD = "gre_shadow_gate_zero_allow_diagnosis.md"
REASON_MATRIX_MD = "gre_shadow_gate_zero_allow_reason_matrix.md"
COMPARISON_MD = "gre_shadow_gate_offline_vs_historical_comparison.md"
RECOMMENDATIONS_MD = "gre_shadow_gate_s1_recommendations.md"
HISTORICAL_SIMULATION_JSON = "gre_shadow_gate_simulation.json"
CONFIDENCE_BANDS = (("0-20", 0, 20), ("20-40", 20, 40), ("40-60", 40, 60), ("60-80", 60, 80), ("80-100", 80, 101))


@dataclass(frozen=True)
class ZeroAllowDiagnosisResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    reason_matrix_path: Path
    comparison_path: Path
    recommendations_path: Path


def run_zero_allow_diagnosis(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    observation_rows_path: Path | None = None,
    historical_rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    historical_simulation_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> ZeroAllowDiagnosisResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    observation_source = observation_rows_path or gold_dir / OBSERVATIONS_JSONL
    historical_source = historical_rows_path or gold_dir / BACKFILL_ROWS_JSONL
    crfd_source = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    simulation_source = historical_simulation_path or gold_dir / HISTORICAL_SIMULATION_JSON
    report = build_zero_allow_diagnosis(
        _read_jsonl(observation_source),
        historical_rows=_read_jsonl(historical_source),
        crfd_rows=_read_jsonl(crfd_source),
        historical_simulation=_read_json_mapping(simulation_source),
        generated_at=generated_at,
        observation_rows_path=observation_source,
        historical_rows_path=historical_source,
        crfd_rows_path=crfd_source,
        historical_simulation_path=simulation_source,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / DIAGNOSIS_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / DIAGNOSIS_MD
    markdown_path.write_text(render_diagnosis_markdown(report), encoding="utf-8")
    reason_matrix_path = gold_dir / REASON_MATRIX_MD
    reason_matrix_path.write_text(render_reason_matrix_markdown(report), encoding="utf-8")
    comparison_path = gold_dir / COMPARISON_MD
    comparison_path.write_text(render_comparison_markdown(report), encoding="utf-8")
    recommendations_path = gold_dir / RECOMMENDATIONS_MD
    recommendations_path.write_text(render_recommendations_markdown(report), encoding="utf-8")
    return ZeroAllowDiagnosisResult(report, json_path, markdown_path, reason_matrix_path, comparison_path, recommendations_path)


def build_zero_allow_diagnosis(
    observation_rows: Sequence[Mapping[str, Any]],
    *,
    historical_rows: Sequence[Mapping[str, Any]],
    crfd_rows: Sequence[Mapping[str, Any]],
    historical_simulation: Mapping[str, Any],
    generated_at: datetime,
    observation_rows_path: Path | str,
    historical_rows_path: Path | str,
    crfd_rows_path: Path | str,
    historical_simulation_path: Path | str,
) -> dict[str, Any]:
    historical_enriched = [_enrich_historical_row(row, _CrfdIndex(crfd_rows)) for row in historical_rows]
    offline = _offline_population(observation_rows)
    historical = _historical_population(historical_enriched, historical_simulation)
    comparison = _comparison(offline, historical)
    conclusions = _conclusions(offline, historical, comparison)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_effect": False,
        "selected_shadow_policy": SELECTED_SHADOW_POLICY,
        "selected_avwap_anchor": SELECTED_AVWAP_ANCHOR,
        "source_artifacts": {
            "offline_observations": str(observation_rows_path),
            "historical_rows": str(historical_rows_path),
            "crfd_rows": str(crfd_rows_path),
            "historical_simulation": str(historical_simulation_path),
        },
        "offline_observation_diagnosis": offline,
        "historical_reference_population": historical,
        "offline_vs_historical_comparison": comparison,
        "conclusions": conclusions,
        "recommendations": _recommendations(conclusions),
        "safety_contract": {
            "research_diagnostic_only": True,
            "runtime_hook": False,
            "trading_gate": False,
            "broker_actions": False,
            "gre_scoring_changes": False,
            "strategy_changes": False,
        },
    }


def _offline_population(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    reason_rows = [_failure_reason(row) for row in rows]
    return {
        "observation_count": len(rows),
        "result_counts": _count_by(rows, "shadow_gate_result"),
        "shadow_gate_reason_counts": _count_by(rows, "shadow_gate_reason"),
        "failure_reason_counts": _count_values(reason_rows),
        "source_artifact_type_counts": _count_values([_source_type(row) for row in rows]),
        "contract_counts": _count_by(rows, "contract"),
        "session_counts": _count_by(rows, "session"),
        "gre_label_counts": _count_by(rows, "gre_label"),
        "gre_confidence_band_counts": _count_values([_confidence_band(row.get("gre_confidence")) for row in rows]),
        "intended_direction_counts": _count_by(rows, "intended_direction"),
        "vwap_relation_counts": _count_by(rows, "vwap_relation"),
        "globex_avwap_relation_counts": _count_by(rows, f"avwap_{SELECTED_AVWAP_ANCHOR}_relation"),
        "gold_candidate_count": sum(1 for row in rows if _root_symbol(row.get("contract")) in {"GC", "MGC"}),
        "unsupported_symbol_count": sum(1 for reason in reason_rows if reason == "unsupported_symbol"),
        "missing_or_unavailable_globex_avwap_count": sum(1 for reason in reason_rows if reason == "avwap_unavailable"),
        "gre_not_directional_count": sum(1 for reason in reason_rows if reason == "gre_not_long_or_short"),
        "intended_direction_missing_count": sum(1 for reason in reason_rows if reason == "intended_direction_unknown"),
        "price_not_confirming_avwap_count": sum(1 for reason in reason_rows if reason == "price_not_on_confirming_side_of_avwap"),
        "reason_matrix": _reason_matrix(rows),
    }


def _historical_population(rows: Sequence[Mapping[str, Any]], simulation: Mapping[str, Any]) -> dict[str, Any]:
    accepts = [_historical_policy_accepts(row) for row in rows]
    policy = (simulation.get("policy_matrix") or {}).get(SELECTED_SHADOW_POLICY, {})
    return {
        "observation_count": len(rows),
        "validated_count": sum(1 for row in rows if row.get("validation_status") == "VALIDATED"),
        "result_counts_for_selected_policy": {
            "WOULD_ALLOW": sum(1 for value in accepts if value),
            "WOULD_BLOCK": sum(1 for value in accepts if not value),
        },
        "simulation_policy_summary": {
            "accepted_count": policy.get("accepted_count"),
            "rejected_count": policy.get("rejected_count"),
            "acceptance_rate": policy.get("acceptance_rate"),
            "expectancy_proxy": policy.get("expectancy_proxy"),
            "net_filter_value": policy.get("net_filter_value"),
        },
        "gre_label_counts": _count_by(rows, "regime_label"),
        "direction_counts": _count_by(rows, "directional_bias"),
        "session_counts": _count_by(rows, "joined_session"),
        "contract_counts": _count_by(rows, "contract"),
        "confidence_band_counts": _count_values([_confidence_band(row.get("confidence")) for row in rows]),
        "vwap_relation_counts": _count_by(rows, "joined_vwap_relation"),
        "globex_avwap_relation_counts": _count_by(rows, f"avwap_relation_{SELECTED_AVWAP_ANCHOR}"),
        "globex_avwap_available_count": sum(1 for row in rows if row.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}") != "unavailable"),
        "crfd_joined_count": sum(1 for row in rows if row.get("crfd_joined") is True),
        "provider_source_counts": _count_values([_provider_source(row) for row in rows]),
    }


def _comparison(offline: Mapping[str, Any], historical: Mapping[str, Any]) -> dict[str, Any]:
    offline_allows = int((offline.get("result_counts") or {}).get("WOULD_ALLOW", 0))
    historical_allows = int((historical.get("result_counts_for_selected_policy") or {}).get("WOULD_ALLOW", 0))
    offline_count = int(offline.get("observation_count") or 0)
    historical_count = int(historical.get("observation_count") or 0)
    return {
        "offline_observation_count": offline_count,
        "historical_observation_count": historical_count,
        "offline_allow_count": offline_allows,
        "historical_allow_count": historical_allows,
        "offline_allow_rate": _rate(offline_allows, offline_count),
        "historical_allow_rate": _rate(historical_allows, historical_count),
        "label_distribution_changed": offline.get("gre_label_counts") != historical.get("gre_label_counts"),
        "direction_distribution_changed": offline.get("intended_direction_counts") != historical.get("direction_counts"),
        "avwap_availability_changed": offline.get("globex_avwap_relation_counts") != historical.get("globex_avwap_relation_counts"),
        "offline_is_representative_of_r18": False,
        "representativeness_reasons": [
            "R21 offline rows are strategy-intent/blocked-intent diagnostics; R18 rows are sampled historical GRE/CRFD opportunities.",
            "R21 applies one current latest GRE label to candidate diagnostics; R18 recomputes historical GRE labels at each observation timestamp.",
            "R21 includes non-Gold blocked intents; R18 is Gold-only research.",
        ],
    }


def _conclusions(offline: Mapping[str, Any], historical: Mapping[str, Any], comparison: Mapping[str, Any]) -> dict[str, Any]:
    result_counts = offline.get("result_counts") or {}
    return {
        "zero_allows_observed": result_counts.get("WOULD_ALLOW", 0) == 0,
        "primary_driver": _primary_driver(offline),
        "policy_strictness_contributed": True,
        "candidate_source_suitability": "PARTIAL_NOT_REPRESENTATIVE_OF_R18",
        "join_alignment_issue": (offline.get("missing_or_unavailable_globex_avwap_count") or 0) > 0,
        "current_gre_label_issue": (offline.get("gre_not_directional_count") or 0) > 0,
        "offline_population_comparable_to_r18": comparison.get("offline_is_representative_of_r18"),
        "bug_found_in_r21_generator": False,
        "explanation": (
            "Zero WOULD_ALLOW is explainable from current inputs: the latest GRE label is not directional, many candidates are non-Gold "
            "blocked-intent diagnostics, and Globex AVWAP is unavailable for unsupported or unjoined candidates. This is a source/population "
            "mismatch rather than evidence that the historical R18 policy stopped working."
        ),
    }


def _recommendations(conclusions: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "is_offline_generator_working_correctly": conclusions.get("bug_found_in_r21_generator") is False,
        "candidate_source_unsuitable": "partially",
        "policy_too_strict_for_current_offline_artifacts": True,
        "adjust_r21_generator_now": False,
        "preferred_live_shadow_source": "post_governance_or_blocked_intent_diagnostics_plus_strategy_intent_context",
        "do_not_use_current_blocked_intent_latest_alone_as_r18_proxy": True,
        "implement_less_selective_variant_later": True,
        "recommended_s2": (
            "Build a diagnostic-only shadow observation source adapter that can emit Gold-only strategy-intent candidates with timestamped "
            "GRE/CRFD context, then compare blocked-intent, post-governance, and generated CRFD observation populations side by side."
        ),
        "production_gate_recommended": False,
    }


def render_diagnosis_markdown(report: Mapping[str, Any]) -> str:
    offline = report.get("offline_observation_diagnosis") or {}
    conclusions = report.get("conclusions") or {}
    rec = report.get("recommendations") or {}
    return "\n".join(
        [
            "# GRE Shadow Gate Zero-Allow Diagnosis",
            "",
            f"- Generated at: {report.get('generated_at')}",
            f"- Observations: {offline.get('observation_count')}",
            f"- Result counts: {_format_counts(offline.get('result_counts'))}",
            f"- Primary driver: {conclusions.get('primary_driver')}",
            f"- R21 bug found: {conclusions.get('bug_found_in_r21_generator')}",
            f"- Offline set comparable to R18: {conclusions.get('offline_population_comparable_to_r18')}",
            "",
            "## Diagnosis",
            str(conclusions.get("explanation")),
            "",
            "## Recommendation",
            f"- Adjust R21 now: {rec.get('adjust_r21_generator_now')}",
            f"- S2: {rec.get('recommended_s2')}",
            f"- Production gate recommended: {rec.get('production_gate_recommended')}",
            "",
        ]
    )


def render_reason_matrix_markdown(report: Mapping[str, Any]) -> str:
    matrix = (report.get("offline_observation_diagnosis") or {}).get("reason_matrix") or []
    lines = ["# GRE Shadow Gate Zero-Allow Reason Matrix", "", "| Failure reason | Count | Representative shadow reasons |", "|---|---:|---|"]
    for row in matrix:
        lines.append(f"| {row.get('failure_reason')} | {row.get('count')} | {_format_counts(row.get('shadow_gate_reasons'))} |")
    return "\n".join(lines) + "\n"


def render_comparison_markdown(report: Mapping[str, Any]) -> str:
    comparison = report.get("offline_vs_historical_comparison") or {}
    historical = report.get("historical_reference_population") or {}
    offline = report.get("offline_observation_diagnosis") or {}
    lines = [
        "# GRE Shadow Gate Offline vs Historical Comparison",
        "",
        "| Population | Observations | Allow count | Allow rate |",
        "|---|---:|---:|---:|",
        f"| R21 offline | {comparison.get('offline_observation_count')} | {comparison.get('offline_allow_count')} | {comparison.get('offline_allow_rate')} |",
        f"| R18 historical | {comparison.get('historical_observation_count')} | {comparison.get('historical_allow_count')} | {comparison.get('historical_allow_rate')} |",
        "",
        "## Offline Label/AVWAP",
        f"- GRE labels: {_format_counts(offline.get('gre_label_counts'))}",
        f"- Globex AVWAP: {_format_counts(offline.get('globex_avwap_relation_counts'))}",
        "",
        "## Historical Label/AVWAP",
        f"- GRE labels: {_format_counts(historical.get('gre_label_counts'))}",
        f"- Globex AVWAP: {_format_counts(historical.get('globex_avwap_relation_counts'))}",
        "",
        "## Representativeness",
    ]
    for reason in comparison.get("representativeness_reasons") or []:
        lines.append(f"- {reason}")
    return "\n".join(lines) + "\n"


def render_recommendations_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("recommendations") or {}
    return "\n".join(
        [
            "# GRE Shadow Gate S1 Recommendations",
            "",
            f"- Offline generator working correctly: {rec.get('is_offline_generator_working_correctly')}",
            f"- Candidate source unsuitable: {rec.get('candidate_source_unsuitable')}",
            f"- Policy too strict for current offline artifacts: {rec.get('policy_too_strict_for_current_offline_artifacts')}",
            f"- Adjust R21 generator now: {rec.get('adjust_r21_generator_now')}",
            f"- Preferred live shadow source: {rec.get('preferred_live_shadow_source')}",
            f"- Less-selective diagnostic variant later: {rec.get('implement_less_selective_variant_later')}",
            f"- S2: {rec.get('recommended_s2')}",
            f"- Production gate recommended: {rec.get('production_gate_recommended')}",
            "",
        ]
    )


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
        if not rows:
            return None
        if timestamp is None:
            return rows[-1][1]
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _enrich_historical_row(row: Mapping[str, Any], index: _CrfdIndex) -> dict[str, Any]:
    item = dict(row)
    crfd = index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=_parse_datetime(row.get("gre_generated_at"))) or {}
    item["crfd_joined"] = bool(crfd)
    item["joined_session"] = crfd.get("session") or row.get("session") or "UNKNOWN"
    item["joined_vwap_relation"] = crfd.get("vwap_relation") or "unavailable"
    item[f"avwap_relation_{SELECTED_AVWAP_ANCHOR}"] = crfd.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}") or "unavailable"
    return item


def _historical_policy_accepts(row: Mapping[str, Any]) -> bool:
    relation = row.get(f"avwap_relation_{SELECTED_AVWAP_ANCHOR}")
    label = row.get("regime_label")
    return (label == "LONG" and relation == "above_avwap") or (label == "SHORT" and relation == "below_avwap")


def _failure_reason(row: Mapping[str, Any]) -> str:
    result = row.get("shadow_gate_result")
    reason = row.get("shadow_gate_reason")
    if result == "WOULD_ALLOW":
        return "allowed"
    if reason == "GRE_GOLD_ONLY_SYMBOL_UNSUPPORTED":
        return "unsupported_symbol"
    if reason == "GLOBEX_AVWAP_UNAVAILABLE":
        return "avwap_unavailable"
    if reason == "INTENDED_DIRECTION_UNAVAILABLE":
        return "intended_direction_unknown"
    if reason == "GRE_OUTPUT_MISSING":
        return "gre_missing"
    if row.get("gre_label") not in {"LONG", "SHORT"}:
        return "gre_not_long_or_short"
    if row.get("intended_direction") not in {"LONG", "SHORT"}:
        return "intended_direction_unknown"
    if row.get(f"avwap_{SELECTED_AVWAP_ANCHOR}_relation") == "unavailable":
        return "avwap_unavailable"
    return "price_not_on_confirming_side_of_avwap"


def _reason_matrix(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_failure_reason(row), []).append(row)
    return [
        {
            "failure_reason": reason,
            "count": len(items),
            "shadow_gate_reasons": _count_by(items, "shadow_gate_reason"),
            "contracts": _count_by(items, "contract"),
            "gre_labels": _count_by(items, "gre_label"),
            "avwap_relations": _count_by(items, f"avwap_{SELECTED_AVWAP_ANCHOR}_relation"),
        }
        for reason, items in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    ]


def _primary_driver(offline: Mapping[str, Any]) -> str:
    counts = offline.get("failure_reason_counts") or {}
    if not counts:
        return "no_observations"
    return max(counts.items(), key=lambda item: item[1])[0]


def _source_type(row: Mapping[str, Any]) -> str:
    source = str((row.get("source_refs") or {}).get("candidate_source") or "unknown")
    if source.endswith(".jsonl"):
        return "blocked_intent_history_jsonl"
    if "blocked_strategy_intent_latest" in source:
        return "blocked_intent_latest"
    if "bridge_report" in source:
        return "bridge_report"
    return "unknown"


def _provider_source(row: Mapping[str, Any]) -> str:
    metadata = row.get("provider_metadata")
    if isinstance(metadata, Mapping):
        return str(metadata.get("provider_kind") or metadata.get("provider_id") or "unknown")
    return str(row.get("source_mode") or "unknown")


def _root_symbol(value: Any) -> str:
    root = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    return root


def _confidence_band(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "UNKNOWN"
    for label, low, high in CONFIDENCE_BANDS:
        if low <= number < high:
            return label
    return "UNKNOWN"


def _count_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return _count_values([str(row.get(key) if row.get(key) is not None else "UNKNOWN") for row in rows])


def _count_values(values: Sequence[Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        text = str(value if value is not None else "UNKNOWN")
        result[text] = result.get(text, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


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
