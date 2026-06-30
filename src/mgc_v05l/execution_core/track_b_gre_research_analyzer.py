"""Diagnostic-only GRE research analyzer."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import (
    DEFAULT_OUTPUT_ROOT,
    GOLD_REGIME_OUTPUT_DIR,
    LATEST_GOLD_REGIME_JSON,
)
from mgc_v05l.execution_core.track_b_gre_validation_logger import (
    LATEST_VALIDATION_SUMMARY_JSON,
    VALIDATION_ROWS_JSONL,
)
from mgc_v05l.execution_core.track_b_gre_validation_scorecard import LATEST_GRE_SCORECARD_JSON
from mgc_v05l.execution_core.track_b_regime_engine_interface import (
    regime_plugin_interface_contract,
    render_regime_plugin_interface_markdown,
    validate_regime_output_compatibility,
)


SCHEMA_VERSION = "track_b_gre_research_analyzer_v1"
LATEST_GRE_RESEARCH_ANALYZER_JSON = "latest_gre_research_analyzer.json"
LATEST_GRE_RESEARCH_ANALYZER_MD = "latest_gre_research_analyzer.md"
REGIME_INTERFACE_JSON = "regime_plugin_interface.json"
REGIME_INTERFACE_MD = "regime_plugin_interface.md"
INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
EARLY_RESEARCH_SIGNAL = "EARLY_RESEARCH_SIGNAL"
SHADOW_RESEARCH_READY = "SHADOW_RESEARCH_READY"


@dataclass(frozen=True)
class ResearchAnalyzerResult:
    analyzer: dict[str, Any]
    json_path: Path
    markdown_path: Path
    interface_json_path: Path
    interface_markdown_path: Path


def run_gre_research_analyzer(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> ResearchAnalyzerResult:
    generated_at = _coerce_now(now)
    out_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    gre = _read_json(out_dir / LATEST_GOLD_REGIME_JSON)
    scorecard = _read_json(out_dir / LATEST_GRE_SCORECARD_JSON)
    validation_summary = _read_json(out_dir / LATEST_VALIDATION_SUMMARY_JSON)
    rows = _read_jsonl(out_dir / VALIDATION_ROWS_JSONL)
    analyzer = build_gre_research_analyzer(
        gre_report=gre,
        scorecard=scorecard,
        validation_summary=validation_summary,
        validation_rows=rows,
        generated_at=generated_at,
    )
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = out_dir / LATEST_GRE_RESEARCH_ANALYZER_JSON
    markdown_path = out_dir / LATEST_GRE_RESEARCH_ANALYZER_MD
    write_bounded_snapshot_json(json_path, analyzer, config=config)
    markdown_path.write_text(render_gre_research_analyzer_markdown(analyzer), encoding="utf-8")
    interface_dir = output_root / "research" / "regime_engine_framework" / "interface"
    interface_dir.mkdir(parents=True, exist_ok=True)
    contract = regime_plugin_interface_contract()
    interface_json_path = interface_dir / REGIME_INTERFACE_JSON
    interface_markdown_path = interface_dir / REGIME_INTERFACE_MD
    write_bounded_snapshot_json(interface_json_path, contract, config=config)
    interface_markdown_path.write_text(render_regime_plugin_interface_markdown(contract), encoding="utf-8")
    return ResearchAnalyzerResult(
        analyzer=analyzer,
        json_path=json_path,
        markdown_path=markdown_path,
        interface_json_path=interface_json_path,
        interface_markdown_path=interface_markdown_path,
    )


def build_gre_research_analyzer(
    *,
    gre_report: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    validation_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    overall = scorecard.get("overall_metrics") if isinstance(scorecard.get("overall_metrics"), Mapping) else {}
    readiness = scorecard.get("readiness_assessment") if isinstance(scorecard.get("readiness_assessment"), Mapping) else {}
    observation_count = _int(overall.get("total_observations"))
    validated_count = _int(overall.get("validated_observations")) + _int(overall.get("partial_observations"))
    sample = _sample_assessment(observation_count=observation_count, validated_count=validated_count)
    missing_rank = _missing_provider_rank(scorecard, validation_rows)
    feature_observations = _feature_observations(scorecard)
    recommendation = _recommend_next_provider(
        sample_status=sample["sample_status"],
        missing_provider_rank=missing_rank,
        scorecard=scorecard,
    )
    confidence_band_usefulness = _confidence_band_usefulness(scorecard)
    regime_label_usefulness = _regime_label_usefulness(scorecard)
    interface_compatibility = validate_regime_output_compatibility(gre_report) if gre_report else {
        "compatible": False,
        "missing_required_fields": ["latest_gold_regime_engine.json"],
        "issues": ["missing_gre_report"],
    }
    warnings = _warnings(sample, scorecard)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "observation_count": observation_count,
        "validated_count": validated_count,
        "readiness_level": readiness.get("classification") or "UNKNOWN",
        "sample_assessment": sample,
        "confidence_band_usefulness": confidence_band_usefulness,
        "regime_label_usefulness": regime_label_usefulness,
        "feature_association_observations": feature_observations,
        "missing_provider_impact": missing_rank,
        "data_quality_concerns": _data_quality_concerns(sample, validation_summary, scorecard),
        "recommended_next_feature_provider": recommendation["provider"],
        "recommended_next_research_experiment": recommendation["experiment"],
        "overfitting_warnings": warnings,
        "generic_regime_interface": {
            "implemented": True,
            "interface_schema_version": regime_plugin_interface_contract()["schema_version"],
            "gre_output_compatible": interface_compatibility,
            "future_plugins": ["GRE", "NRE", "ERE", "TRE"],
        },
        "source_refs": {
            "latest_gold_regime_engine": str(GOLD_REGIME_OUTPUT_DIR / LATEST_GOLD_REGIME_JSON),
            "gre_validation_rows": str(GOLD_REGIME_OUTPUT_DIR / VALIDATION_ROWS_JSONL),
            "latest_gre_validation_summary": str(GOLD_REGIME_OUTPUT_DIR / LATEST_VALIDATION_SUMMARY_JSON),
            "latest_gre_scorecard": str(GOLD_REGIME_OUTPUT_DIR / LATEST_GRE_SCORECARD_JSON),
        },
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
    }


def render_gre_research_analyzer_markdown(analyzer: Mapping[str, Any]) -> str:
    sample = analyzer.get("sample_assessment", {})
    text = (
        "# GRE Research Analyzer\n\n"
        f"Generated: {analyzer.get('generated_at')}\n\n"
        f"Sample status: **{sample.get('sample_status')}**\n\n"
        f"Readiness level: `{analyzer.get('readiness_level')}`\n\n"
        f"Observations: `{analyzer.get('observation_count')}` total, `{analyzer.get('validated_count')}` validated/partial\n\n"
        "## Recommendation\n\n"
        f"- Next feature provider: {analyzer.get('recommended_next_feature_provider')}\n"
        f"- Next research experiment: {analyzer.get('recommended_next_research_experiment')}\n\n"
        "## Confidence Bands\n\n"
        f"{analyzer.get('confidence_band_usefulness')}\n\n"
        "## Regime Labels\n\n"
        f"{analyzer.get('regime_label_usefulness')}\n\n"
        "## Feature Associations\n\n"
    )
    for item in analyzer.get("feature_association_observations", [])[:10]:
        text += f"- `{item.get('feature')}`: observations `{item.get('observation_count')}`, outcome `{item.get('average_outcome')}`\n"
    text += "\n## Missing Provider Impact\n\n"
    for item in analyzer.get("missing_provider_impact", []):
        text += f"- {item.get('provider')}: `{item.get('count')}`\n"
    text += "\n## Data Quality Concerns\n\n"
    for item in analyzer.get("data_quality_concerns", []):
        text += f"- {item}\n"
    text += "\n## Overfitting Warnings\n\n"
    for item in analyzer.get("overfitting_warnings", []):
        text += f"- {item}\n"
    return text


def _sample_assessment(*, observation_count: int, validated_count: int) -> dict[str, Any]:
    if observation_count < 30 or validated_count < 20:
        return {
            "sample_status": INSUFFICIENT_SAMPLE,
            "sample_sufficient": False,
            "reason": "Fewer than 30 total observations or fewer than 20 validated/partial observations.",
        }
    if observation_count < 100 or validated_count < 75:
        return {
            "sample_status": EARLY_RESEARCH_SIGNAL,
            "sample_sufficient": False,
            "reason": "Enough for descriptive monitoring, not enough for scoring changes.",
        }
    return {
        "sample_status": SHADOW_RESEARCH_READY,
        "sample_sufficient": True,
        "reason": "Enough observations for a separate shadow-research experiment proposal.",
    }


def _confidence_band_usefulness(scorecard: Mapping[str, Any]) -> str:
    bands = scorecard.get("confidence_bands")
    if not isinstance(bands, Mapping):
        return "No confidence-band data available."
    populated = [name for name, metrics in bands.items() if isinstance(metrics, Mapping) and _int(metrics.get("observations")) > 0]
    if len(populated) < 3:
        return f"Low usefulness: observations currently occupy only {len(populated)} confidence band(s)."
    return "Useful for descriptive calibration only; no statistical claim yet."


def _regime_label_usefulness(scorecard: Mapping[str, Any]) -> str:
    regimes = scorecard.get("regime_breakdown")
    if not isinstance(regimes, Mapping):
        return "No regime breakdown available."
    populated = [name for name, metrics in regimes.items() if isinstance(metrics, Mapping) and _int(metrics.get("observation_count")) > 0]
    if len(populated) < 3:
        return f"Low usefulness: observed labels so far are limited to {', '.join(populated) or 'none'}."
    return "Useful for descriptive regime comparisons only; still diagnostic."


def _feature_observations(scorecard: Mapping[str, Any]) -> list[dict[str, Any]]:
    items = scorecard.get("feature_effectiveness")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
        return []
    return [dict(item) for item in items if isinstance(item, Mapping)]


def _missing_provider_rank(scorecard: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    items = scorecard.get("missing_provider_analysis")
    if isinstance(items, Sequence) and not isinstance(items, (str, bytes)) and items:
        return [dict(item) for item in items if isinstance(item, Mapping)]
    counts: dict[str, int] = {}
    for row in rows:
        providers = row.get("missing_providers")
        if not isinstance(providers, Sequence) or isinstance(providers, (str, bytes)):
            continue
        for provider in providers:
            text = str(provider)
            counts[text] = counts.get(text, 0) + 1
    return [{"provider": key, "count": value} for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


def _recommend_next_provider(
    *,
    sample_status: str,
    missing_provider_rank: Sequence[Mapping[str, Any]],
    scorecard: Mapping[str, Any],
) -> dict[str, str]:
    if sample_status == INSUFFICIENT_SAMPLE:
        return {
            "provider": "Historical candle/session backfill before scoring changes; keep VWAP as first feature-provider candidate.",
            "experiment": "Continue diagnostic logging and backfill GC/MGC session candles before changing GRE scoring.",
        }
    provider_text = str(missing_provider_rank[0].get("provider") if missing_provider_rank else "")
    if "VWAP" in provider_text and "anchored" not in provider_text:
        provider = "VWAP"
    elif "anchored VWAP" in provider_text:
        provider = "anchored VWAP"
    elif "overnight" in provider_text:
        provider = "overnight high/low"
    elif "prior" in provider_text:
        provider = "prior session high/low"
    else:
        provider = scorecard.get("recommended_next_feature_provider") or "session expansion/chop metric"
    return {
        "provider": str(provider),
        "experiment": "Run a shadow replay comparing current GRE labels with the proposed provider added as diagnostics only.",
    }


def _data_quality_concerns(
    sample: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
    scorecard: Mapping[str, Any],
) -> list[str]:
    concerns: list[str] = []
    if sample.get("sample_status") == INSUFFICIENT_SAMPLE:
        concerns.append("Sample size is too small for feature or scoring conclusions.")
    if validation_summary.get("directional_correctness_rate") is None:
        concerns.append("No directional correctness rate yet; current rows may be CHOP/TRANSITION only.")
    regimes = scorecard.get("regime_breakdown")
    if isinstance(regimes, Mapping):
        missing = [name for name, metrics in regimes.items() if isinstance(metrics, Mapping) and _int(metrics.get("observation_count")) == 0]
        if missing:
            concerns.append(f"No observations yet for regime labels: {', '.join(missing)}.")
    return concerns or ["No data quality concern detected beyond normal diagnostic-only caveats."]


def _warnings(sample: Mapping[str, Any], scorecard: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    if sample.get("sample_status") == INSUFFICIENT_SAMPLE:
        warnings.append("Do not tune GRE scoring from this sample.")
        warnings.append("Do not infer feature effectiveness until more labels and confidence bands are populated.")
    if scorecard.get("readiness_assessment", {}).get("classification") == "RESEARCH_TOO_EARLY":
        warnings.append("Scorecard readiness is RESEARCH_TOO_EARLY; collect more observations or backfill history.")
    return warnings or ["No overfitting warning beyond diagnostic-only status."]


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
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


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
