"""Diagnostic-only GRE historical scorecard deep-dive reports."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_historical_scorecard_deep_dive_v1"
DEEP_DIVE_JSON = "gre_historical_scorecard_deep_dive.json"
DEEP_DIVE_MD = "gre_historical_scorecard_deep_dive.md"
REGIME_SESSION_MATRIX_MD = "gre_regime_session_matrix.md"
VWAP_AVWAP_ANALYSIS_MD = "gre_vwap_avwap_diagnostic_analysis.md"
FAILURE_CASE_REVIEW_MD = "gre_failure_case_review.md"
GATE_RESEARCH_RECOMMENDATIONS_MD = "gre_gate_research_recommendations.md"
REGIME_LABELS = ("LONG", "SHORT", "CHOP", "TRANSITION", "INSUFFICIENT_EVIDENCE")
CONFIDENCE_BANDS = (
    ("0-20", 0, 20),
    ("20-40", 20, 40),
    ("40-60", 40, 60),
    ("60-80", 60, 80),
    ("80-100", 80, 101),
)
HORIZONS = ("5m", "15m", "30m", "60m")
AVWAP_ANCHORS = ("globex_session_open_18et", "london_open", "us_rth_open")


@dataclass(frozen=True)
class DeepDiveResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    regime_session_matrix_path: Path
    vwap_avwap_analysis_path: Path
    failure_case_review_path: Path
    gate_research_recommendations_path: Path


def run_gre_historical_scorecard_deep_dive(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> DeepDiveResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_rows = rows_path or gold_dir / BACKFILL_ROWS_JSONL
    source_crfd = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    rows = _read_jsonl(source_rows)
    crfd_rows = _read_jsonl(source_crfd)
    report = build_gre_historical_scorecard_deep_dive(
        rows,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        rows_path=source_rows,
        crfd_rows_path=source_crfd,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / DEEP_DIVE_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / DEEP_DIVE_MD
    markdown_path.write_text(render_deep_dive_markdown(report), encoding="utf-8")
    regime_session_path = gold_dir / REGIME_SESSION_MATRIX_MD
    regime_session_path.write_text(render_regime_session_matrix_markdown(report), encoding="utf-8")
    vwap_path = gold_dir / VWAP_AVWAP_ANALYSIS_MD
    vwap_path.write_text(render_vwap_avwap_markdown(report), encoding="utf-8")
    failure_path = gold_dir / FAILURE_CASE_REVIEW_MD
    failure_path.write_text(render_failure_case_review_markdown(report), encoding="utf-8")
    gate_path = gold_dir / GATE_RESEARCH_RECOMMENDATIONS_MD
    gate_path.write_text(render_gate_research_recommendations_markdown(report), encoding="utf-8")
    return DeepDiveResult(
        report=report,
        json_path=json_path,
        markdown_path=markdown_path,
        regime_session_matrix_path=regime_session_path,
        vwap_avwap_analysis_path=vwap_path,
        failure_case_review_path=failure_path,
        gate_research_recommendations_path=gate_path,
    )


def build_gre_historical_scorecard_deep_dive(
    rows: Sequence[Mapping[str, Any]],
    *,
    crfd_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    rows_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    normalized = [dict(row) for row in rows]
    crfd_index = _CrfdIndex(crfd_rows)
    enriched = [_enrich_row(row, crfd_index) for row in normalized]
    regime = {label: _group_metrics([row for row in enriched if row.get("regime_label") == label]) for label in REGIME_LABELS}
    confidence = {
        name: _group_metrics([row for row in enriched if _confidence_in_band(row.get("confidence"), low, high)])
        for name, low, high in CONFIDENCE_BANDS
    }
    session = _session_breakout(enriched)
    vwap = {
        relation: _vwap_relation_metrics([row for row in enriched if row.get("joined_vwap_relation") == relation])
        for relation in ("above_vwap", "below_vwap", "at_vwap", "unavailable")
    }
    avwap = {
        anchor: _avwap_anchor_metrics(enriched, anchor=anchor)
        for anchor in AVWAP_ANCHORS
    }
    failures = _failure_cases(enriched)
    recommendations = _gate_research_recommendations(
        rows=enriched,
        regime_metrics=regime,
        confidence_metrics=confidence,
        avwap_metrics=avwap,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "source_rows_path": str(rows_path),
        "crfd_rows_path": str(crfd_rows_path),
        "overall": {
            "observation_count": len(enriched),
            "validated_count": sum(1 for row in enriched if row.get("validation_status") == "VALIDATED"),
            "partial_count": sum(1 for row in enriched if row.get("validation_status") == "PARTIAL_FORWARD_DATA"),
            "pending_count": sum(1 for row in enriched if row.get("validation_status") == "PENDING_FORWARD_DATA"),
            "insufficient_count": sum(1 for row in enriched if row.get("validation_status") == "INSUFFICIENT_DATA"),
            "crfd_joined_count": sum(1 for row in enriched if row.get("crfd_joined") is True),
        },
        "regime_label_analysis": regime,
        "confidence_band_analysis": confidence,
        "session_analysis": session,
        "vwap_relation_analysis": vwap,
        "anchored_vwap_analysis": avwap,
        "failure_analysis": failures,
        "gate_research_recommendations": recommendations,
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
        "gre_scoring_changed": False,
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
        self._rows = {contract: sorted(values, key=lambda item: item[0]) for contract, values in by_contract.items()}

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


def _enrich_row(row: Mapping[str, Any], crfd_index: _CrfdIndex) -> dict[str, Any]:
    item = dict(row)
    gre_ts = _parse_datetime(row.get("gre_generated_at"))
    crfd = crfd_index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=gre_ts)
    item["crfd_joined"] = crfd is not None
    if crfd:
        item["joined_session"] = crfd.get("session") or row.get("session") or "UNKNOWN"
        item["joined_session_label"] = crfd.get("session_label")
        item["joined_vwap_relation"] = crfd.get("vwap_relation") or "unavailable"
        item["joined_vwap_distance_points"] = crfd.get("distance_from_vwap_points")
        for anchor in AVWAP_ANCHORS:
            item[f"joined_avwap_relation_{anchor}"] = crfd.get(f"avwap_relation_{anchor}") or "unavailable"
            item[f"joined_has_avwap_{anchor}"] = bool(crfd.get(f"has_avwap_{anchor}"))
            item[f"joined_avwap_distance_{anchor}"] = crfd.get(f"distance_from_avwap_{anchor}_points")
        item["joined_has_anchored_vwap"] = bool(crfd.get("has_anchored_vwap"))
    else:
        item["joined_session"] = row.get("session") or "UNKNOWN"
        item["joined_vwap_relation"] = "unavailable"
        for anchor in AVWAP_ANCHORS:
            item[f"joined_avwap_relation_{anchor}"] = "unavailable"
            item[f"joined_has_avwap_{anchor}"] = False
    return item


def _group_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    validated = [row for row in rows if row.get("validation_status") in {"VALIDATED", "PARTIAL_FORWARD_DATA"}]
    confidence_values = [_number(row.get("confidence")) for row in rows]
    confidence_values = [value for value in confidence_values if value is not None]
    directional = [row for row in validated if row.get("direction_correctness") in {True, False}]
    forward_avg = {horizon: _average(_forward_values(validated, horizon)) for horizon in HORIZONS}
    forward_median = {horizon: _median(_forward_values(validated, horizon)) for horizon in HORIZONS}
    best_horizon, worst_horizon = _best_worst_horizons(forward_avg)
    outcomes = [_outcome(row) for row in validated]
    outcomes = [value for value in outcomes if value is not None]
    return {
        "count": len(rows),
        "validated_count": len(validated),
        "average_confidence": _average(confidence_values),
        "average_forward_return": forward_avg,
        "median_forward_return": forward_median,
        "average_mfe": _average([value for value in (_number(row.get("mfe")) for row in validated) if value is not None]),
        "average_mae": _average([value for value in (_number(row.get("mae")) for row in validated) if value is not None]),
        "percent_positive": round(sum(1 for value in outcomes if value > 0) / len(outcomes), 4) if outcomes else None,
        "percent_negative": round(sum(1 for value in outcomes if value < 0) / len(outcomes), 4) if outcomes else None,
        "direction_correctness_rate": round(sum(1 for row in directional if row.get("direction_correctness") is True) / len(directional), 4)
        if directional
        else None,
        "best_horizon": best_horizon,
        "worst_horizon": worst_horizon,
        "calibration_notes": _calibration_note(rows, directional),
    }


def _session_breakout(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sessions = sorted({str(row.get("joined_session") or row.get("session") or "UNKNOWN") for row in rows})
    result: dict[str, Any] = {}
    for session in sessions:
        session_rows = [row for row in rows if str(row.get("joined_session") or row.get("session") or "UNKNOWN") == session]
        regime_distribution: dict[str, int] = {}
        for row in session_rows:
            regime = str(row.get("regime_label") or "UNKNOWN")
            regime_distribution[regime] = regime_distribution.get(regime, 0) + 1
        by_regime = {regime: _group_metrics([row for row in session_rows if row.get("regime_label") == regime]) for regime in sorted(regime_distribution)}
        result[session] = {
            **_group_metrics(session_rows),
            "regime_distribution": regime_distribution,
            "best_regime": _best_named_group(by_regime),
            "worst_regime": _worst_named_group(by_regime),
            "regimes": by_regime,
        }
    return result


def _vwap_relation_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes: dict[str, int] = {}
    for row in rows:
        regime = str(row.get("regime_label") or "UNKNOWN")
        regimes[regime] = regimes.get(regime, 0) + 1
    metrics = _group_metrics(rows)
    metrics["regime_distribution"] = regimes
    metrics["vwap_confirmed_directional_count"] = sum(1 for row in rows if _vwap_confirms_direction(row))
    metrics["vwap_conflicted_directional_count"] = sum(1 for row in rows if _vwap_conflicts_direction(row))
    return metrics


def _avwap_anchor_metrics(rows: Sequence[Mapping[str, Any]], *, anchor: str) -> dict[str, Any]:
    by_relation: dict[str, Any] = {}
    for relation in ("above_avwap", "below_avwap", "at_avwap", "unavailable"):
        relation_rows = [row for row in rows if row.get(f"joined_avwap_relation_{anchor}") == relation]
        by_relation[relation] = _group_metrics(relation_rows)
    return {
        "anchor": anchor,
        "availability_count": sum(1 for row in rows if row.get(f"joined_has_avwap_{anchor}") is True),
        "relation_counts": {relation: by_relation[relation]["count"] for relation in by_relation},
        "by_relation": by_relation,
        "diagnostic_interpretation": _avwap_interpretation(by_relation),
    }


def _failure_cases(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "worst_false_long_cases": _case_rows(
            sorted(
                [row for row in rows if row.get("regime_label") == "LONG" and _number((row.get("forward_returns") or {}).get("60m")) is not None],
                key=lambda row: _number((row.get("forward_returns") or {}).get("60m")) or 0,
            )[:5]
        ),
        "worst_false_short_cases": _case_rows(
            sorted(
                [row for row in rows if row.get("regime_label") == "SHORT" and _number((row.get("forward_returns") or {}).get("60m")) is not None],
                key=lambda row: -(_number((row.get("forward_returns") or {}).get("60m")) or 0),
            )[:5]
        ),
        "best_missed_long_opportunities": _case_rows(
            sorted(
                [row for row in rows if row.get("regime_label") != "LONG" and _number((row.get("forward_returns") or {}).get("60m")) is not None],
                key=lambda row: _number((row.get("forward_returns") or {}).get("60m")) or 0,
                reverse=True,
            )[:5]
        ),
        "best_missed_short_opportunities": _case_rows(
            sorted(
                [row for row in rows if row.get("regime_label") != "SHORT" and _number((row.get("forward_returns") or {}).get("60m")) is not None],
                key=lambda row: _number((row.get("forward_returns") or {}).get("60m")) or 0,
            )[:5]
        ),
        "transition_cases_with_strong_followthrough": _case_rows(
            sorted(
                [
                    row
                    for row in rows
                    if row.get("regime_label") == "TRANSITION"
                    and abs(_number((row.get("forward_returns") or {}).get("60m")) or 0) >= 10
                ],
                key=lambda row: abs(_number((row.get("forward_returns") or {}).get("60m")) or 0),
                reverse=True,
            )[:5]
        ),
        "chop_cases_that_trended_anyway": _case_rows(
            sorted(
                [
                    row
                    for row in rows
                    if row.get("regime_label") == "CHOP"
                    and abs(_number((row.get("forward_returns") or {}).get("60m")) or 0) >= 10
                ],
                key=lambda row: abs(_number((row.get("forward_returns") or {}).get("60m")) or 0),
                reverse=True,
            )[:5]
        ),
    }


def _gate_research_recommendations(
    *,
    rows: Sequence[Mapping[str, Any]],
    regime_metrics: Mapping[str, Any],
    confidence_metrics: Mapping[str, Any],
    avwap_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    validated = sum(1 for row in rows if row.get("validation_status") == "VALIDATED")
    tradable = [
        regime
        for regime, metrics in regime_metrics.items()
        if regime in {"LONG", "SHORT"}
        and isinstance(metrics, Mapping)
        and _number(metrics.get("direction_correctness_rate")) is not None
        and (_number(metrics.get("direction_correctness_rate")) or 0) >= 0.52
    ]
    diagnostic_only = [regime for regime in REGIME_LABELS if regime not in tradable]
    useful_confidence_bands = [
        band
        for band, metrics in confidence_metrics.items()
        if isinstance(metrics, Mapping) and _safe_int(metrics.get("count")) >= 30 and metrics.get("average_forward_return", {}).get("60m") is not None
    ]
    avwap_available = sum(_safe_int((metrics or {}).get("availability_count")) for metrics in avwap_metrics.values() if isinstance(metrics, Mapping))
    return {
        "ready_for_shadow_gate_simulation": validated >= 100,
        "readiness_reason": "Enough historical validated observations for shadow simulation only." if validated >= 100 else "Sample remains too small for gate simulation.",
        "regimes_that_appear_tradable_for_shadow_only": tradable,
        "regimes_to_keep_diagnostic_only": diagnostic_only,
        "confidence_threshold_assessment": "Confidence bands are useful for descriptive filtering." if useful_confidence_bands else "Confidence thresholds are not yet clearly meaningful.",
        "avwap_recommendation": "Add AVWAP next as diagnostic confirmation/conflict and extension-warning evidence; do not gate yet."
        if avwap_available
        else "Collect/extend AVWAP coverage before adding it to GRE.",
        "next_feature_provider": "anchored VWAP",
        "recommended_next_experiment": "Run shadow gate simulation with current GRE labels plus AVWAP diagnostic overlays; do not change live execution.",
        "scoring_change_recommended_now": False,
    }


def render_deep_dive_markdown(report: Mapping[str, Any]) -> str:
    overall = report.get("overall", {})
    rec = report.get("gate_research_recommendations", {})
    return (
        "# GRE Historical Scorecard Deep Dive\n\n"
        f"Generated: `{report.get('generated_at')}`\n\n"
        f"Observations: `{overall.get('observation_count')}`; validated: `{overall.get('validated_count')}`; CRFD joined: `{overall.get('crfd_joined_count')}`\n\n"
        f"Ready for shadow gate simulation: `{rec.get('ready_for_shadow_gate_simulation')}`\n\n"
        f"Next feature provider: `{rec.get('next_feature_provider')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def render_regime_session_matrix_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE Regime Session Matrix\n\n## Regimes\n\n"
    for regime, metrics in report.get("regime_label_analysis", {}).items():
        text += f"- `{regime}`: count `{metrics.get('count')}`, validated `{metrics.get('validated_count')}`, 60m avg `{metrics.get('average_forward_return', {}).get('60m')}`\n"
    text += "\n## Sessions\n\n"
    for session, metrics in report.get("session_analysis", {}).items():
        text += f"- `{session}`: count `{metrics.get('count')}`, regimes `{metrics.get('regime_distribution')}`\n"
    return text


def render_vwap_avwap_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE VWAP / AVWAP Diagnostic Analysis\n\n## VWAP Relation\n\n"
    for relation, metrics in report.get("vwap_relation_analysis", {}).items():
        text += f"- `{relation}`: count `{metrics.get('count')}`, 60m avg `{metrics.get('average_forward_return', {}).get('60m')}`, regimes `{metrics.get('regime_distribution')}`\n"
    text += "\n## Anchored VWAP\n\n"
    for anchor, metrics in report.get("anchored_vwap_analysis", {}).items():
        text += f"- `{anchor}`: available `{metrics.get('availability_count')}`, relations `{metrics.get('relation_counts')}`, note `{metrics.get('diagnostic_interpretation')}`\n"
    return text


def render_failure_case_review_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE Failure Case Review\n\n"
    for name, cases in report.get("failure_analysis", {}).items():
        text += f"## {name}\n\n"
        for case in cases:
            text += f"- `{case.get('gre_generated_at')}` `{case.get('regime_label')}` confidence `{case.get('confidence')}` 60m `{case.get('forward_60m')}` session `{case.get('session')}`\n"
        if not cases:
            text += "- None\n"
        text += "\n"
    return text


def render_gate_research_recommendations_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("gate_research_recommendations", {})
    return (
        "# GRE Gate Research Recommendations\n\n"
        f"- Ready for shadow gate simulation: `{rec.get('ready_for_shadow_gate_simulation')}`\n"
        f"- Reason: {rec.get('readiness_reason')}\n"
        f"- Shadow-only tradable regimes: `{rec.get('regimes_that_appear_tradable_for_shadow_only')}`\n"
        f"- Diagnostic-only regimes: `{rec.get('regimes_to_keep_diagnostic_only')}`\n"
        f"- Confidence thresholds: {rec.get('confidence_threshold_assessment')}\n"
        f"- AVWAP: {rec.get('avwap_recommendation')}\n"
        f"- Next experiment: {rec.get('recommended_next_experiment')}\n"
        f"- Scoring change recommended now: `{rec.get('scoring_change_recommended_now')}`\n"
    )


def _case_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        forward = row.get("forward_returns") if isinstance(row.get("forward_returns"), Mapping) else {}
        result.append(
            {
                "gre_generated_at": row.get("gre_generated_at"),
                "regime_label": row.get("regime_label"),
                "confidence": row.get("confidence"),
                "session": row.get("joined_session") or row.get("session"),
                "vwap_relation": row.get("joined_vwap_relation"),
                "forward_5m": forward.get("5m"),
                "forward_15m": forward.get("15m"),
                "forward_30m": forward.get("30m"),
                "forward_60m": forward.get("60m"),
                "direction_correctness": row.get("direction_correctness"),
            }
        )
    return result


def _forward_values(rows: Sequence[Mapping[str, Any]], horizon: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        forward = row.get("forward_returns")
        if not isinstance(forward, Mapping):
            continue
        value = _number(forward.get(horizon))
        if value is not None:
            values.append(value)
    return values


def _outcome(row: Mapping[str, Any]) -> float | None:
    forward = row.get("forward_returns")
    if not isinstance(forward, Mapping):
        return None
    return _number(forward.get("60m") if forward.get("60m") is not None else forward.get("30m"))


def _best_worst_horizons(forward_avg: Mapping[str, Any]) -> tuple[str | None, str | None]:
    values = {key: _number(value) for key, value in forward_avg.items()}
    valid = {key: value for key, value in values.items() if value is not None}
    if not valid:
        return None, None
    return max(valid, key=lambda key: valid[key] or 0), min(valid, key=lambda key: valid[key] or 0)


def _best_named_group(groups: Mapping[str, Mapping[str, Any]]) -> str | None:
    candidates = {name: (metrics.get("average_forward_return") or {}).get("60m") for name, metrics in groups.items()}
    valid = {name: _number(value) for name, value in candidates.items() if _number(value) is not None}
    return max(valid, key=lambda key: valid[key] or 0) if valid else None


def _worst_named_group(groups: Mapping[str, Mapping[str, Any]]) -> str | None:
    candidates = {name: (metrics.get("average_forward_return") or {}).get("60m") for name, metrics in groups.items()}
    valid = {name: _number(value) for name, value in candidates.items() if _number(value) is not None}
    return min(valid, key=lambda key: valid[key] or 0) if valid else None


def _confidence_in_band(value: Any, low: int, high: int) -> bool:
    number = _number(value)
    return number is not None and low <= number < high


def _vwap_confirms_direction(row: Mapping[str, Any]) -> bool:
    relation = row.get("joined_vwap_relation")
    regime = row.get("regime_label")
    return (regime == "LONG" and relation == "above_vwap") or (regime == "SHORT" and relation == "below_vwap")


def _vwap_conflicts_direction(row: Mapping[str, Any]) -> bool:
    relation = row.get("joined_vwap_relation")
    regime = row.get("regime_label")
    return (regime == "LONG" and relation == "below_vwap") or (regime == "SHORT" and relation == "above_vwap")


def _avwap_interpretation(by_relation: Mapping[str, Mapping[str, Any]]) -> str:
    above = (by_relation.get("above_avwap") or {}).get("average_forward_return", {}).get("60m")
    below = (by_relation.get("below_avwap") or {}).get("average_forward_return", {}).get("60m")
    if _number(above) is None or _number(below) is None:
        return "Insufficient relation coverage for interpretation."
    if (_number(above) or 0) > (_number(below) or 0):
        return "Above-anchor relation is stronger on this sample; treat as candidate confirmation evidence only."
    return "Below-anchor relation is stronger on this sample; treat as candidate conflict/extension evidence only."


def _calibration_note(rows: Sequence[Mapping[str, Any]], directional: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "No observations."
    if len(rows) < 20:
        return "Small sample; descriptive only."
    if not directional:
        return "No directional correctness observations."
    rate = sum(1 for row in directional if row.get("direction_correctness") is True) / len(directional)
    if rate >= 0.55:
        return "Directional calibration is promising but diagnostic only."
    if rate <= 0.45:
        return "Directional calibration is weak in this bucket."
    return "Directional calibration is mixed."


def _average(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _median(values: Sequence[float]) -> float | None:
    return round(float(statistics.median(values)), 6) if values else None


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
