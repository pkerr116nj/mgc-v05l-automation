"""Diagnostic-only AVWAP evidence research for GRE historical rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_avwap_evidence_research_v1"
AVWAP_RESEARCH_JSON = "gre_avwap_evidence_research.json"
AVWAP_RESEARCH_MD = "gre_avwap_evidence_research.md"
ANCHOR_COMPARISON_MD = "gre_avwap_anchor_comparison.md"
CONFIRMATION_CONFLICT_MD = "gre_avwap_confirmation_conflict_matrix.md"
DISTANCE_BAND_MD = "gre_avwap_distance_band_analysis.md"
RECOMMENDATIONS_MD = "gre_avwap_research_recommendations.md"
AVWAP_ANCHORS = ("globex_session_open_18et", "london_open", "us_rth_open")
HORIZONS = ("5m", "15m", "30m", "60m")
LOW_SAMPLE_THRESHOLD = 20


@dataclass(frozen=True)
class AvwapResearchResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    anchor_comparison_path: Path
    confirmation_conflict_path: Path
    distance_band_path: Path
    recommendations_path: Path


def run_gre_avwap_evidence_research(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> AvwapResearchResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_rows = rows_path or gold_dir / BACKFILL_ROWS_JSONL
    source_crfd = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    rows = _read_jsonl(source_rows)
    crfd_rows = _read_jsonl(source_crfd)
    report = build_gre_avwap_evidence_research(
        rows,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        rows_path=source_rows,
        crfd_rows_path=source_crfd,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / AVWAP_RESEARCH_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / AVWAP_RESEARCH_MD
    markdown_path.write_text(render_avwap_research_markdown(report), encoding="utf-8")
    anchor_path = gold_dir / ANCHOR_COMPARISON_MD
    anchor_path.write_text(render_anchor_comparison_markdown(report), encoding="utf-8")
    matrix_path = gold_dir / CONFIRMATION_CONFLICT_MD
    matrix_path.write_text(render_confirmation_conflict_markdown(report), encoding="utf-8")
    distance_path = gold_dir / DISTANCE_BAND_MD
    distance_path.write_text(render_distance_band_markdown(report), encoding="utf-8")
    recommendations_path = gold_dir / RECOMMENDATIONS_MD
    recommendations_path.write_text(render_recommendations_markdown(report), encoding="utf-8")
    return AvwapResearchResult(
        report=report,
        json_path=json_path,
        markdown_path=markdown_path,
        anchor_comparison_path=anchor_path,
        confirmation_conflict_path=matrix_path,
        distance_band_path=distance_path,
        recommendations_path=recommendations_path,
    )


def build_gre_avwap_evidence_research(
    rows: Sequence[Mapping[str, Any]],
    *,
    crfd_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    rows_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    enriched = [_enrich_row(row, _CrfdIndex(crfd_rows)) for row in rows]
    thresholds = _distance_thresholds(enriched)
    enriched = [_attach_distance_bands(row, thresholds) for row in enriched]
    anchor_analysis = {anchor: _anchor_analysis(enriched, anchor=anchor) for anchor in AVWAP_ANCHORS}
    confirmation_conflict = {anchor: _confirmation_conflict_matrix(enriched, anchor=anchor) for anchor in AVWAP_ANCHORS}
    distance_bands = {anchor: _distance_band_analysis(enriched, anchor=anchor) for anchor in AVWAP_ANCHORS}
    chop_transition = {anchor: _chop_transition_analysis(enriched, anchor=anchor) for anchor in AVWAP_ANCHORS}
    session = {anchor: _session_interaction(enriched, anchor=anchor) for anchor in AVWAP_ANCHORS}
    recommendations = _recommendations(anchor_analysis, confirmation_conflict, distance_bands)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "source_rows_path": str(rows_path),
        "crfd_rows_path": str(crfd_rows_path),
        "overall": {
            "observation_count": len(enriched),
            "validated_count": sum(1 for row in enriched if row.get("validation_status") == "VALIDATED"),
            "crfd_joined_count": sum(1 for row in enriched if row.get("crfd_joined") is True),
        },
        "distance_band_thresholds": thresholds,
        "anchor_analysis": anchor_analysis,
        "confirmation_conflict_matrix": confirmation_conflict,
        "distance_band_analysis": distance_bands,
        "chop_transition_analysis": chop_transition,
        "session_interaction": session,
        "recommendations": recommendations,
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
        "gre_scoring_changed": False,
        "crfd_feature_calculations_changed": False,
    }


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(items, key=lambda item: item[0]) for contract, items in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        if timestamp is None:
            return None
        rows = self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _enrich_row(row: Mapping[str, Any], index: _CrfdIndex) -> dict[str, Any]:
    item = dict(row)
    gre_ts = _parse_datetime(row.get("gre_generated_at"))
    crfd = index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=gre_ts)
    item["crfd_joined"] = crfd is not None
    item["joined_session"] = (crfd or {}).get("session") or row.get("session") or "UNKNOWN"
    item["joined_vwap_relation"] = (crfd or {}).get("vwap_relation") or "unavailable"
    for anchor in AVWAP_ANCHORS:
        item[f"avwap_available_{anchor}"] = bool((crfd or {}).get(f"has_avwap_{anchor}"))
        item[f"avwap_relation_{anchor}"] = (crfd or {}).get(f"avwap_relation_{anchor}") or "unavailable"
        item[f"avwap_distance_{anchor}"] = _number((crfd or {}).get(f"distance_from_avwap_{anchor}_points"))
        item[f"avwap_slope_{anchor}"] = _number((crfd or {}).get(f"avwap_slope_{anchor}"))
    return item


def classify_confirmation_conflict(row: Mapping[str, Any], *, anchor: str) -> str:
    regime = str(row.get("regime_label") or "")
    relation = str(row.get(f"avwap_relation_{anchor}") or "unavailable")
    if relation in {"unavailable", "at_avwap"}:
        return relation
    if regime == "LONG":
        return "confirmation" if relation == "above_avwap" else "conflict"
    if regime == "SHORT":
        return "confirmation" if relation == "below_avwap" else "conflict"
    if regime in {"CHOP", "TRANSITION"}:
        return "diagnostic_balance_or_transition"
    return "not_directional"


def classify_distance_band(distance: float | None, *, near_threshold: float, large_threshold: float) -> str:
    if distance is None:
        return "unavailable"
    value = abs(distance)
    if value <= near_threshold:
        return "near"
    if value <= large_threshold:
        return "modest_extension"
    return "large_extension"


def _distance_thresholds(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float | None]]:
    result: dict[str, dict[str, float | None]] = {}
    for anchor in AVWAP_ANCHORS:
        values = sorted(abs(value) for value in (_number(row.get(f"avwap_distance_{anchor}")) for row in rows) if value is not None)
        result[anchor] = {
            "near_threshold_points": _quantile(values, 0.33),
            "large_threshold_points": _quantile(values, 0.66),
            "method": "descriptive_33_66_abs_distance_quantiles",
        }
    return result


def _attach_distance_bands(row: Mapping[str, Any], thresholds: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    item = dict(row)
    for anchor in AVWAP_ANCHORS:
        config = thresholds.get(anchor, {})
        near = _number(config.get("near_threshold_points")) or 0.0
        large = _number(config.get("large_threshold_points")) or near
        item[f"avwap_distance_band_{anchor}"] = classify_distance_band(_number(row.get(f"avwap_distance_{anchor}")), near_threshold=near, large_threshold=large)
    return item


def _anchor_analysis(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    available = [row for row in rows if row.get(f"avwap_available_{anchor}") is True]
    by_relation = {
        relation: _metrics([row for row in rows if row.get(f"avwap_relation_{anchor}") == relation])
        for relation in ("above_avwap", "below_avwap", "at_avwap", "unavailable")
    }
    directional = [row for row in available if row.get("regime_label") in {"LONG", "SHORT"}]
    false_long_potential = _false_reduction_potential(directional, anchor=anchor, regime="LONG")
    false_short_potential = _false_reduction_potential(directional, anchor=anchor, regime="SHORT")
    return {
        "anchor": anchor,
        "availability_count": len(available),
        "usable_sample_count": len(available),
        "low_sample": len(available) < LOW_SAMPLE_THRESHOLD,
        "relation_distribution": {relation: by_relation[relation]["count"] for relation in by_relation},
        "by_relation": by_relation,
        "direction_correctness": _direction_correctness(directional),
        "false_long_reduction_potential": false_long_potential,
        "false_short_reduction_potential": false_short_potential,
        "candidate_role": _candidate_role(by_relation, len(available)),
    }


def _confirmation_conflict_matrix(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    matrix: dict[str, Any] = {}
    for regime in ("LONG", "SHORT", "CHOP", "TRANSITION", "INSUFFICIENT_EVIDENCE"):
        regime_rows = [row for row in rows if row.get("regime_label") == regime]
        matrix[regime] = {
            bucket: _metrics([row for row in regime_rows if classify_confirmation_conflict(row, anchor=anchor) == bucket])
            for bucket in ("confirmation", "conflict", "diagnostic_balance_or_transition", "at_avwap", "unavailable", "not_directional")
        }
    return matrix


def _distance_band_analysis(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    return {
        band: _metrics([row for row in rows if row.get(f"avwap_distance_band_{anchor}") == band])
        for band in ("near", "modest_extension", "large_extension", "unavailable")
    }


def _chop_transition_analysis(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    diagnostic_rows = [row for row in rows if row.get("regime_label") in {"CHOP", "TRANSITION"}]
    near = [row for row in diagnostic_rows if row.get(f"avwap_distance_band_{anchor}") == "near"]
    vwap_avwap_conflict = [
        row
        for row in diagnostic_rows
        if _opposite_relations(str(row.get("joined_vwap_relation") or ""), str(row.get(f"avwap_relation_{anchor}") or ""))
    ]
    followthrough = [row for row in diagnostic_rows if abs(_number((row.get("forward_returns") or {}).get("60m")) or 0.0) >= 10.0]
    return {
        "diagnostic_count": len(diagnostic_rows),
        "near_avwap": _metrics(near),
        "vwap_avwap_conflict": _metrics(vwap_avwap_conflict),
        "strong_followthrough": _metrics(followthrough),
    }


def _session_interaction(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    sessions = sorted({str(row.get("joined_session") or "UNKNOWN") for row in rows})
    return {
        session: {
            "confirmation": _metrics(
                [row for row in rows if row.get("joined_session") == session and classify_confirmation_conflict(row, anchor=anchor) == "confirmation"]
            ),
            "conflict": _metrics(
                [row for row in rows if row.get("joined_session") == session and classify_confirmation_conflict(row, anchor=anchor) == "conflict"]
            ),
            "near": _metrics([row for row in rows if row.get("joined_session") == session and row.get(f"avwap_distance_band_{anchor}") == "near"]),
        }
        for session in sessions
    }


def _recommendations(
    anchor_analysis: Mapping[str, Any],
    confirmation_conflict: Mapping[str, Any],
    distance_bands: Mapping[str, Any],
) -> dict[str, Any]:
    ranked = sorted(
        (
            {
                "anchor": anchor,
                "availability_count": metrics.get("availability_count"),
                "candidate_role": metrics.get("candidate_role"),
                "low_sample": metrics.get("low_sample"),
            }
            for anchor, metrics in anchor_analysis.items()
        ),
        key=lambda item: (_safe_int(item.get("availability_count")), str(item.get("anchor"))),
        reverse=True,
    )
    best_anchor = ranked[0]["anchor"] if ranked else None
    return {
        "should_add_avwap_to_gre_scoring_now": False,
        "reason": "Use AVWAP first in shadow gate simulation; do not alter GRE scoring from one bounded historical sample.",
        "should_use_in_shadow_gate_simulation_first": True,
        "most_promising_anchor": best_anchor,
        "anchor_ranking": ranked,
        "promising_relationships": _promising_relationships(confirmation_conflict),
        "weak_or_sample_limited_relationships": _weak_relationships(anchor_analysis, distance_bands),
        "recommended_role": "diagnostic confirmation/conflict plus extension-warning evidence",
        "additional_validation_required": "More sessions and out-of-sample weeks before any GRE scoring change.",
    }


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    forward = {horizon: _average(_forward_values(rows, horizon)) for horizon in HORIZONS}
    return {
        "count": len(rows),
        "validated_count": sum(1 for row in rows if row.get("validation_status") in {"VALIDATED", "PARTIAL_FORWARD_DATA"}),
        "low_sample": len(rows) < LOW_SAMPLE_THRESHOLD,
        "average_forward_return": forward,
        "average_mfe": _average([value for value in (_number(row.get("mfe")) for row in rows) if value is not None]),
        "average_mae": _average([value for value in (_number(row.get("mae")) for row in rows) if value is not None]),
        "direction_correctness": _direction_correctness(rows),
    }


def render_avwap_research_markdown(report: Mapping[str, Any]) -> str:
    overall = report.get("overall", {})
    rec = report.get("recommendations", {})
    return (
        "# GRE AVWAP Evidence Research\n\n"
        f"Generated: `{report.get('generated_at')}`\n\n"
        f"Observations: `{overall.get('observation_count')}`, CRFD joined: `{overall.get('crfd_joined_count')}`\n\n"
        f"Add to GRE scoring now: `{rec.get('should_add_avwap_to_gre_scoring_now')}`\n\n"
        f"Shadow first: `{rec.get('should_use_in_shadow_gate_simulation_first')}`\n\n"
        f"Most promising anchor: `{rec.get('most_promising_anchor')}`\n\n"
        f"Recommended role: {rec.get('recommended_role')}\n"
    )


def render_anchor_comparison_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE AVWAP Anchor Comparison\n\n"
    for anchor, metrics in report.get("anchor_analysis", {}).items():
        text += f"- `{anchor}`: available `{metrics.get('availability_count')}`, role `{metrics.get('candidate_role')}`, relations `{metrics.get('relation_distribution')}`\n"
    return text


def render_confirmation_conflict_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE AVWAP Confirmation / Conflict Matrix\n\n"
    for anchor, matrix in report.get("confirmation_conflict_matrix", {}).items():
        text += f"## {anchor}\n\n"
        for regime, buckets in matrix.items():
            confirmation = buckets.get("confirmation", {}).get("count")
            conflict = buckets.get("conflict", {}).get("count")
            text += f"- `{regime}` confirmation `{confirmation}`, conflict `{conflict}`\n"
    return text


def render_distance_band_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE AVWAP Distance Band Analysis\n\n"
    text += f"Thresholds: `{report.get('distance_band_thresholds')}`\n\n"
    for anchor, bands in report.get("distance_band_analysis", {}).items():
        text += f"## {anchor}\n\n"
        for band, metrics in bands.items():
            text += f"- `{band}`: count `{metrics.get('count')}`, 60m `{metrics.get('average_forward_return', {}).get('60m')}`, low sample `{metrics.get('low_sample')}`\n"
    return text


def render_recommendations_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("recommendations", {})
    return (
        "# GRE AVWAP Research Recommendations\n\n"
        f"- Add AVWAP to GRE scoring now: `{rec.get('should_add_avwap_to_gre_scoring_now')}`\n"
        f"- Use in shadow gate simulation first: `{rec.get('should_use_in_shadow_gate_simulation_first')}`\n"
        f"- Reason: {rec.get('reason')}\n"
        f"- Most promising anchor: `{rec.get('most_promising_anchor')}`\n"
        f"- Recommended role: {rec.get('recommended_role')}\n"
        f"- Additional validation required: {rec.get('additional_validation_required')}\n"
        f"- Promising relationships: `{rec.get('promising_relationships')}`\n"
        f"- Weak/sample-limited relationships: `{rec.get('weak_or_sample_limited_relationships')}`\n"
    )


def _forward_values(rows: Sequence[Mapping[str, Any]], horizon: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        forward = row.get("forward_returns")
        if isinstance(forward, Mapping):
            value = _number(forward.get(horizon))
            if value is not None:
                values.append(value)
    return values


def _direction_correctness(rows: Sequence[Mapping[str, Any]]) -> float | None:
    directional = [row for row in rows if row.get("direction_correctness") in {True, False}]
    if not directional:
        return None
    return round(sum(1 for row in directional if row.get("direction_correctness") is True) / len(directional), 4)


def _false_reduction_potential(rows: Sequence[Mapping[str, Any]], *, anchor: str, regime: str) -> dict[str, Any]:
    regime_rows = [row for row in rows if row.get("regime_label") == regime]
    conflicts = [row for row in regime_rows if classify_confirmation_conflict(row, anchor=anchor) == "conflict"]
    false_conflicts = [row for row in conflicts if row.get("direction_correctness") is False]
    return {
        "regime_count": len(regime_rows),
        "conflict_count": len(conflicts),
        "false_conflict_count": len(false_conflicts),
        "low_sample": len(conflicts) < LOW_SAMPLE_THRESHOLD,
    }


def _candidate_role(by_relation: Mapping[str, Mapping[str, Any]], available_count: int) -> str:
    if available_count < LOW_SAMPLE_THRESHOLD:
        return "sample_limited"
    above = (by_relation.get("above_avwap") or {}).get("average_forward_return", {}).get("60m")
    below = (by_relation.get("below_avwap") or {}).get("average_forward_return", {}).get("60m")
    if _number(above) is None or _number(below) is None:
        return "diagnostic_only_insufficient_relation_split"
    if abs((_number(above) or 0) - (_number(below) or 0)) >= 2.0:
        return "confirmation_conflict_candidate"
    return "extension_or_context_candidate"


def _promising_relationships(matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for anchor, by_regime in matrix.items():
        for regime in ("LONG", "SHORT"):
            buckets = by_regime.get(regime, {}) if isinstance(by_regime, Mapping) else {}
            confirmation = buckets.get("confirmation", {}) if isinstance(buckets, Mapping) else {}
            conflict = buckets.get("conflict", {}) if isinstance(buckets, Mapping) else {}
            if _safe_int(confirmation.get("count")) >= LOW_SAMPLE_THRESHOLD:
                items.append(
                    {
                        "anchor": anchor,
                        "regime": regime,
                        "relationship": "confirmation",
                        "count": confirmation.get("count"),
                        "direction_correctness": confirmation.get("direction_correctness"),
                        "average_60m": confirmation.get("average_forward_return", {}).get("60m"),
                    }
                )
            if _safe_int(conflict.get("count")) >= LOW_SAMPLE_THRESHOLD:
                items.append(
                    {
                        "anchor": anchor,
                        "regime": regime,
                        "relationship": "conflict",
                        "count": conflict.get("count"),
                        "direction_correctness": conflict.get("direction_correctness"),
                        "average_60m": conflict.get("average_forward_return", {}).get("60m"),
                    }
                )
    return items[:12]


def _weak_relationships(anchor_analysis: Mapping[str, Any], distance_bands: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for anchor, metrics in anchor_analysis.items():
        if metrics.get("low_sample"):
            items.append({"anchor": anchor, "reason": "anchor_available_sample_below_threshold", "count": metrics.get("availability_count")})
    for anchor, bands in distance_bands.items():
        for band, metrics in bands.items():
            if metrics.get("low_sample") and band != "unavailable":
                items.append({"anchor": anchor, "relationship": band, "reason": "distance_band_sample_below_threshold", "count": metrics.get("count")})
    return items[:20]


def _opposite_relations(vwap: str, avwap: str) -> bool:
    return (vwap == "above_vwap" and avwap == "below_avwap") or (vwap == "below_vwap" and avwap == "above_avwap")


def _average(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * q))))
    return round(float(values[idx]), 6)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = _parse_datetime(value)
    return parsed if parsed else datetime.now(UTC)
