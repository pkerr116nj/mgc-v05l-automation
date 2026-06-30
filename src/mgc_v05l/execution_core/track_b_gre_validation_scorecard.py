"""Diagnostic-only scorecard for Gold Regime Engine validation history."""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_validation_logger import VALIDATION_ROWS_JSONL


SCHEMA_VERSION = "track_b_gre_validation_scorecard_v1"
LATEST_GRE_SCORECARD_JSON = "latest_gre_scorecard.json"
LATEST_GRE_SCORECARD_MD = "latest_gre_scorecard.md"
REGIME_LABELS = ("LONG", "SHORT", "CHOP", "TRANSITION", "INSUFFICIENT_EVIDENCE")
CONFIDENCE_BANDS = (
    ("0-20", 0, 20),
    ("20-40", 20, 40),
    ("40-60", 40, 60),
    ("60-80", 60, 80),
    ("80-100", 80, 100),
)
HORIZON_KEYS = ("5m", "15m", "30m", "60m")
VALIDATED_STATUSES = {"VALIDATED", "PARTIAL_FORWARD_DATA"}


@dataclass(frozen=True)
class ScorecardResult:
    scorecard: dict[str, Any]
    json_path: Path
    markdown_path: Path


def run_gre_validation_scorecard(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> ScorecardResult:
    generated_at = _coerce_now(now)
    out_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_rows = rows_path or out_dir / VALIDATION_ROWS_JSONL
    rows = _read_jsonl(source_rows)
    scorecard = build_gre_validation_scorecard(rows, generated_at=generated_at, rows_path=source_rows)
    json_path = out_dir / LATEST_GRE_SCORECARD_JSON
    md_path = out_dir / LATEST_GRE_SCORECARD_MD
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    write_bounded_snapshot_json(json_path, scorecard, config=config)
    md_path.write_text(render_gre_scorecard_markdown(scorecard), encoding="utf-8")
    return ScorecardResult(scorecard=scorecard, json_path=json_path, markdown_path=md_path)


def build_gre_validation_scorecard(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    rows_path: Path | str,
) -> dict[str, Any]:
    normalized = [dict(row) for row in rows]
    total = len(normalized)
    validated_rows = [row for row in normalized if row.get("validation_status") in VALIDATED_STATUSES]
    pending_rows = [row for row in normalized if row.get("validation_status") == "PENDING_FORWARD_DATA"]
    insufficient_rows = [row for row in normalized if row.get("validation_status") == "INSUFFICIENT_DATA"]
    regime_breakdown = {
        label: _regime_metrics([row for row in normalized if row.get("regime_label") == label])
        for label in REGIME_LABELS
    }
    confidence_bands = {
        name: _confidence_band_metrics(
            [
                row
                for row in normalized
                if _confidence_in_band(row.get("confidence"), low=low, high=high, include_high=name == "80-100")
            ]
        )
        for name, low, high in CONFIDENCE_BANDS
    }
    feature_ranking = _feature_effectiveness(normalized)
    missing_provider_analysis = _missing_provider_counts(normalized)
    readiness = _readiness_assessment(total=total, validated=len(validated_rows), directional_rows=_directional_rows(validated_rows))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "source_rows_path": str(rows_path),
        "overall_metrics": {
            "total_observations": total,
            "validated_observations": len([row for row in normalized if row.get("validation_status") == "VALIDATED"]),
            "partial_observations": len([row for row in normalized if row.get("validation_status") == "PARTIAL_FORWARD_DATA"]),
            "pending_observations": len(pending_rows),
            "insufficient_observations": len(insufficient_rows),
        },
        "regime_breakdown": regime_breakdown,
        "confidence_bands": confidence_bands,
        "feature_effectiveness": feature_ranking,
        "missing_provider_analysis": missing_provider_analysis,
        "readiness_assessment": readiness,
        "top_strengths": _top_strengths(feature_ranking, regime_breakdown),
        "top_weaknesses": _top_weaknesses(regime_breakdown, confidence_bands),
        "largest_unknowns": _largest_unknowns(total, missing_provider_analysis),
        "recommended_next_feature_provider": _recommended_feature_provider(missing_provider_analysis),
        "recommended_next_research_experiment": _recommended_experiment(readiness),
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def render_gre_scorecard_markdown(scorecard: Mapping[str, Any]) -> str:
    overall = scorecard.get("overall_metrics", {})
    readiness = scorecard.get("readiness_assessment", {})
    text = (
        "# GRE Validation Scorecard\n\n"
        f"Generated: {scorecard.get('generated_at')}\n\n"
        f"Readiness: **{readiness.get('classification')}**\n\n"
        f"Reason: {readiness.get('reason')}\n\n"
        "## Overall Metrics\n\n"
        f"- Total observations: `{overall.get('total_observations')}`\n"
        f"- Validated observations: `{overall.get('validated_observations')}`\n"
        f"- Partial observations: `{overall.get('partial_observations')}`\n"
        f"- Pending observations: `{overall.get('pending_observations')}`\n"
        f"- Insufficient observations: `{overall.get('insufficient_observations')}`\n\n"
        "## Regime Breakdown\n\n"
        "| Regime | Count | Avg Confidence | 5m | 15m | 30m | 60m | Direction Correctness |\n"
        "|---|---:|---:|---:|---:|---:|---:|---:|\n"
    )
    for label, metrics in scorecard.get("regime_breakdown", {}).items():
        text += (
            f"| {label} | {metrics.get('observation_count')} | {metrics.get('average_confidence')} | "
            f"{metrics.get('average_forward_return', {}).get('5m')} | "
            f"{metrics.get('average_forward_return', {}).get('15m')} | "
            f"{metrics.get('average_forward_return', {}).get('30m')} | "
            f"{metrics.get('average_forward_return', {}).get('60m')} | "
            f"{metrics.get('direction_correctness_rate')} |\n"
        )
    text += "\n## Feature Effectiveness\n\n"
    for item in scorecard.get("feature_effectiveness", [])[:10]:
        text += f"- `{item.get('feature')}`: observations `{item.get('observation_count')}`, average outcome `{item.get('average_outcome')}`\n"
    text += "\n## Missing Provider Analysis\n\n"
    for item in scorecard.get("missing_provider_analysis", []):
        text += f"- {item.get('provider')}: `{item.get('count')}`\n"
    text += "\n## Next Step\n\n"
    text += f"- Feature provider: {scorecard.get('recommended_next_feature_provider')}\n"
    text += f"- Research experiment: {scorecard.get('recommended_next_research_experiment')}\n"
    return text


def _regime_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    confidences = [_number(row.get("confidence")) for row in rows]
    confidence_values = [item for item in confidences if item is not None]
    forward = {horizon: _average(_forward_values(rows, horizon)) for horizon in HORIZON_KEYS}
    mfe_values = [_number(row.get("mfe")) for row in rows]
    mae_values = [_number(row.get("mae")) for row in rows]
    directional = [row for row in rows if row.get("direction_correctness") in {True, False}]
    positive_outcomes = [_outcome(row) for row in rows if _outcome(row) is not None and _outcome(row) > 0]
    negative_outcomes = [_outcome(row) for row in rows if _outcome(row) is not None and _outcome(row) < 0]
    return {
        "observation_count": len(rows),
        "average_confidence": _average(confidence_values),
        "confidence_range": _range(confidence_values),
        "average_forward_return": forward,
        "average_mfe": _average([item for item in mfe_values if item is not None]),
        "average_mae": _average([item for item in mae_values if item is not None]),
        "median_holding_window": _median_holding_window(rows),
        "confidence_calibration": _confidence_calibration(rows),
        "direction_correctness_rate": _average([1.0 if row.get("direction_correctness") is True else 0.0 for row in directional])
        if directional
        else None,
        "percentage_positive": round(len(positive_outcomes) / len(rows), 4) if rows else None,
        "percentage_negative": round(len(negative_outcomes) / len(rows), 4) if rows else None,
    }


def _confidence_band_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "observations": len(rows),
        "average_outcome": _average([item for item in (_outcome(row) for row in rows) if item is not None]),
        "average_mfe": _average([item for item in (_number(row.get("mfe")) for row in rows) if item is not None]),
        "average_mae": _average([item for item in (_number(row.get("mae")) for row in rows) if item is not None]),
        "calibration_notes": _band_note(rows),
    }


def _feature_effectiveness(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = {}
    for row in rows:
        outcome = _outcome(row)
        if outcome is None:
            continue
        for evidence_key in ("positive_evidence", "negative_evidence", "conflicts"):
            items = row.get(evidence_key)
            if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
                continue
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                feature = str(item.get("feature") or "UNKNOWN")
                buckets.setdefault(feature, []).append(outcome)
    ranking = [
        {
            "feature": feature,
            "observation_count": len(values),
            "average_outcome": _average(values),
            "positive_rate": round(sum(1 for item in values if item > 0) / len(values), 4) if values else None,
        }
        for feature, values in buckets.items()
    ]
    return sorted(ranking, key=lambda item: (item["average_outcome"] is not None, item["average_outcome"] or 0, item["observation_count"]), reverse=True)


def _missing_provider_counts(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        providers = row.get("missing_providers")
        if not isinstance(providers, Sequence) or isinstance(providers, (str, bytes)):
            continue
        for provider in providers:
            text = str(provider)
            counts[text] = counts.get(text, 0) + 1
    return [{"provider": provider, "count": count} for provider, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def _readiness_assessment(*, total: int, validated: int, directional_rows: int) -> dict[str, Any]:
    if validated < 20 or total < 30:
        return {
            "classification": "RESEARCH_TOO_EARLY",
            "reason": "Fewer than 20 validated observations or fewer than 30 total observations.",
        }
    if validated < 75:
        return {
            "classification": "PROMISING_EARLY_SIGNAL",
            "reason": "Enough validated observations for early descriptive monitoring, not for gate research.",
        }
    if validated >= 150 and directional_rows >= 50:
        return {
            "classification": "READY_FOR_GATE_RESEARCH",
            "reason": "Large enough directional validation sample for a separate gate-research proposal.",
        }
    return {
        "classification": "SUFFICIENT_FOR_SHADOW",
        "reason": "Enough validation observations for ongoing shadow tracking.",
    }


def _top_strengths(feature_ranking: Sequence[Mapping[str, Any]], regime_breakdown: Mapping[str, Any]) -> list[str]:
    strengths: list[str] = []
    if feature_ranking:
        strengths.append(f"Top observed feature association: {feature_ranking[0].get('feature')}.")
    populated = [label for label, metrics in regime_breakdown.items() if metrics.get("observation_count")]
    if populated:
        strengths.append(f"Observed regimes so far: {', '.join(populated)}.")
    return strengths or ["No strengths yet; validation history is empty."]


def _top_weaknesses(regime_breakdown: Mapping[str, Any], confidence_bands: Mapping[str, Any]) -> list[str]:
    empty_regimes = [label for label, metrics in regime_breakdown.items() if not metrics.get("observation_count")]
    weaknesses = []
    if empty_regimes:
        weaknesses.append(f"No observations yet for: {', '.join(empty_regimes)}.")
    sparse_bands = [band for band, metrics in confidence_bands.items() if not metrics.get("observations")]
    if sparse_bands:
        weaknesses.append(f"No observations in confidence bands: {', '.join(sparse_bands)}.")
    return weaknesses or ["No major descriptive weakness detected yet."]


def _largest_unknowns(total: int, missing_providers: Sequence[Mapping[str, Any]]) -> list[str]:
    if total == 0:
        return ["No GRE validation history yet."]
    unknowns = [f"{item.get('provider')} missing in {item.get('count')} observations." for item in missing_providers[:5]]
    return unknowns or ["No missing providers reported."]


def _recommended_feature_provider(missing_providers: Sequence[Mapping[str, Any]]) -> str:
    if not missing_providers:
        return "No missing-provider priority yet."
    provider = str(missing_providers[0].get("provider") or "")
    if "VWAP" in provider and "anchored" not in provider:
        return "Session VWAP provider with explicit futures reset rules."
    if "anchored VWAP" in provider:
        return "Anchored VWAP provider after session VWAP is certified."
    if "overnight" in provider:
        return "Overnight range reference builder."
    if "prior" in provider:
        return "Prior-session range reference builder."
    return provider


def _recommended_experiment(readiness: Mapping[str, Any]) -> str:
    if readiness.get("classification") == "RESEARCH_TOO_EARLY":
        return "Continue diagnostic logging until at least 20 validated and 30 total observations accumulate."
    return "Run a session/side replay comparing GRE labels with 5m/15m/30m/60m outcomes."


def _confidence_calibration(rows: Sequence[Mapping[str, Any]]) -> str:
    if len(rows) < 10:
        return "not enough observations"
    directional = [row for row in rows if row.get("direction_correctness") in {True, False}]
    if len(directional) < 10:
        return "not enough directional observations"
    return "directional calibration available"


def _band_note(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "no observations"
    if len(rows) < 10:
        return "too few observations for calibration"
    return "descriptive only; no statistical claim"


def _median_holding_window(rows: Sequence[Mapping[str, Any]]) -> str | None:
    values: list[int] = []
    for row in rows:
        window = row.get("best_observed_holding_window")
        if isinstance(window, str) and window.endswith("m"):
            try:
                values.append(int(window[:-1]))
            except ValueError:
                continue
    if not values:
        return None
    return f"{int(statistics.median(values))}m"


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
    for horizon in ("60m", "30m", "15m", "5m"):
        value = _number(forward.get(horizon))
        if value is not None:
            return value
    return None


def _directional_rows(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("direction_correctness") in {True, False})


def _confidence_in_band(value: Any, *, low: int, high: int, include_high: bool = False) -> bool:
    number = _number(value)
    if number is None:
        return False
    if include_high:
        return low <= number <= high
    return low <= number < high


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Iterable[float]) -> float | None:
    materialized = [value for value in values if value is not None]
    if not materialized:
        return None
    return round(sum(materialized) / len(materialized), 6)


def _range(values: Sequence[float]) -> dict[str, float] | None:
    if not values:
        return None
    return {"min": min(values), "max": max(values)}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return datetime.now(UTC)
