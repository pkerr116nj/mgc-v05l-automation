"""Generic diagnostic Regime Research Framework contract.

This module describes the reusable research pipeline proven by GRE:

Regime Plugin -> Validation Logger -> Validation Store -> Scorecard ->
Research Analyzer -> Historical Backfill.

It is intentionally descriptive and diagnostic-only. It does not call broker,
runtime, strategy, or Managed Exit paths.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_regime_engine_interface import (
    RegimePluginDescriptor,
    regime_plugin_interface_contract,
    render_regime_plugin_interface_markdown,
    validate_regime_output_compatibility,
)


SCHEMA_VERSION = "track_b_regime_research_framework_v1"
R8_OUTPUT_DIR = Path("research/regime_engine_framework/r8_generic_framework")
REGIME_RESEARCH_FRAMEWORK_JSON = "regime_research_framework.json"
REGIME_RESEARCH_FRAMEWORK_MD = "regime_research_framework.md"
REGIME_PLUGIN_CONTRACT_MD = "regime_plugin_contract.md"
FUTURE_PLUGIN_EXAMPLES_MD = "future_regime_plugin_examples.md"


@dataclass(frozen=True)
class RegimeResearchArtifactPlan:
    plugin_id: str
    engine_name: str
    instrument_family: str
    output_namespace: str
    latest_report: str
    validation_rows: str
    validation_summary: str
    scorecard: str
    analyzer: str
    backfill_observations: str
    backfill_summary: str


def build_regime_research_framework_contract() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "diagnostic_only": True,
        "purpose": "Reusable research pipeline for diagnostic regime engines.",
        "pipeline": [
            {
                "step": "Regime Plugin",
                "responsibility": "Compute instrument-specific regime label, confidence, evidence, and missing providers.",
                "generic": False,
            },
            {
                "step": "Validation Logger",
                "responsibility": "Attach forward outcomes to plugin observations without mutating the plugin output.",
                "generic": True,
            },
            {
                "step": "Validation Store",
                "responsibility": "Persist bounded append-only validation rows.",
                "generic": True,
            },
            {
                "step": "Scorecard Generator",
                "responsibility": "Aggregate validation rows by label, confidence band, outcome, feature, and missing provider.",
                "generic": True,
            },
            {
                "step": "Research Analyzer",
                "responsibility": "Recommend next research actions conservatively from scorecard and validation history.",
                "generic": True,
            },
            {
                "step": "Historical Backfill Runner",
                "responsibility": "Replay plugin scoring over retained/historical candles without lookahead bias.",
                "generic": True,
            },
        ],
        "plugin_responsibilities": [
            "compute_regime",
            "feature_provider_selection",
            "confidence_calculation",
            "explainability",
            "supported_instruments",
            "supported_sessions",
        ],
        "generic_service_responsibilities": [
            "validation row schema",
            "bounded validation storage",
            "scorecard aggregation",
            "research analyzer recommendations",
            "historical observation cadence",
            "lookahead-bias guard",
        ],
        "compatibility": {
            "gre_outputs_unchanged": True,
            "schema_changes": "none",
            "future_plugins": ["GRE", "NRE", "ERE", "TRE"],
        },
        "authority_boundary": {
            "broker_authority": False,
            "runtime_authority": False,
            "managed_exit_authority": False,
            "strategy_authority": False,
            "trading_gate": False,
        },
    }


def build_artifact_plan(descriptor: RegimePluginDescriptor) -> RegimeResearchArtifactPlan:
    namespace = f"research/{descriptor.plugin_id.lower()}_regime_engine"
    prefix = descriptor.plugin_id.lower()
    return RegimeResearchArtifactPlan(
        plugin_id=descriptor.plugin_id,
        engine_name=descriptor.engine_name,
        instrument_family=descriptor.instrument_family,
        output_namespace=namespace,
        latest_report=f"latest_{prefix}_regime_engine.json",
        validation_rows=f"{prefix}_validation_rows.jsonl",
        validation_summary=f"latest_{prefix}_validation_summary.json",
        scorecard=f"latest_{prefix}_scorecard.json",
        analyzer=f"latest_{prefix}_research_analyzer.json",
        backfill_observations=f"{prefix}_backfill_observations.jsonl",
        backfill_summary=f"latest_{prefix}_backfill_summary.json",
    )


def build_future_plugin_examples() -> list[dict[str, Any]]:
    descriptors = [
        RegimePluginDescriptor(
            engine_name="Gold Regime Engine",
            plugin_id="GRE",
            instrument_family="Gold futures",
            feature_providers=("session_label", "trend_persistence", "candle_body_strength", "range_chop"),
        ),
        RegimePluginDescriptor(
            engine_name="Nasdaq Regime Engine",
            plugin_id="NRE",
            instrument_family="Nasdaq equity index futures",
            feature_providers=("session_label", "trend_persistence", "opening_range", "volatility_expansion"),
        ),
        RegimePluginDescriptor(
            engine_name="ES Regime Engine",
            plugin_id="ERE",
            instrument_family="S&P / ES equity index futures",
            feature_providers=("session_label", "opening_range", "range_chop", "prior_session_references"),
        ),
        RegimePluginDescriptor(
            engine_name="Treasury Regime Engine",
            plugin_id="TRE",
            instrument_family="Treasury futures",
            feature_providers=("session_label", "curve_family_context", "trend_persistence", "range_compression"),
        ),
    ]
    return [
        {
            "descriptor": {
                "engine_name": item.engine_name,
                "plugin_id": item.plugin_id,
                "instrument_family": item.instrument_family,
                "regime_labels": list(item.regime_labels),
                "feature_providers": list(item.feature_providers),
                "diagnostic_only": item.diagnostic_only,
            },
            "artifact_plan": build_artifact_plan(item).__dict__,
            "implementation_status": "illustration_only" if item.plugin_id != "GRE" else "implemented_diagnostic_mvp",
        }
        for item in descriptors
    ]


def validate_plugin_report_against_framework(
    report: Mapping[str, Any],
    *,
    descriptor: RegimePluginDescriptor,
) -> dict[str, Any]:
    output = validate_regime_output_compatibility(report)
    descriptor_issues: list[str] = []
    if report.get("plugin_id") != descriptor.plugin_id:
        descriptor_issues.append("plugin_id_descriptor_mismatch")
    if report.get("instrument") not in {descriptor.instrument_family, descriptor.engine_name, "GOLD"} and descriptor.plugin_id == "GRE":
        # GRE historically reports instrument as GOLD; keep that backward-compatible.
        descriptor_issues.append("instrument_descriptor_mismatch")
    return {
        "schema_version": SCHEMA_VERSION,
        "compatible": output["compatible"] and not descriptor_issues,
        "output_compatibility": output,
        "descriptor_issues": descriptor_issues,
    }


def write_regime_research_framework_documents(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> dict[str, str]:
    generated_at = _coerce_now(now)
    out_dir = output_root / R8_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    contract = build_regime_research_framework_contract()
    contract["generated_at"] = generated_at.isoformat()
    contract["plugin_interface"] = regime_plugin_interface_contract()
    contract["future_plugin_examples"] = build_future_plugin_examples()
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = out_dir / REGIME_RESEARCH_FRAMEWORK_JSON
    write_bounded_snapshot_json(json_path, contract, config=config)
    framework_md = out_dir / REGIME_RESEARCH_FRAMEWORK_MD
    plugin_contract_md = out_dir / REGIME_PLUGIN_CONTRACT_MD
    examples_md = out_dir / FUTURE_PLUGIN_EXAMPLES_MD
    framework_md.write_text(render_regime_research_framework_markdown(contract), encoding="utf-8")
    plugin_contract_md.write_text(render_regime_plugin_interface_markdown(contract["plugin_interface"]), encoding="utf-8")
    examples_md.write_text(render_future_plugin_examples_markdown(contract["future_plugin_examples"]), encoding="utf-8")
    return {
        "json": str(json_path),
        "framework_markdown": str(framework_md),
        "plugin_contract_markdown": str(plugin_contract_md),
        "future_examples_markdown": str(examples_md),
    }


def render_regime_research_framework_markdown(contract: Mapping[str, Any]) -> str:
    text = (
        "# Regime Research Framework\n\n"
        f"Schema version: `{contract.get('schema_version')}`\n\n"
        "This framework generalizes the GRE research workflow without changing GRE scoring or production behavior.\n\n"
        "## Pipeline\n\n"
    )
    for step in contract.get("pipeline", []):
        text += f"- **{step.get('step')}**: {step.get('responsibility')} Generic: `{step.get('generic')}`\n"
    text += "\n## Plugin Responsibilities\n\n"
    for item in contract.get("plugin_responsibilities", []):
        text += f"- `{item}`\n"
    text += "\n## Generic Service Responsibilities\n\n"
    for item in contract.get("generic_service_responsibilities", []):
        text += f"- `{item}`\n"
    text += "\n## Authority Boundary\n\n"
    for key, value in contract.get("authority_boundary", {}).items():
        text += f"- `{key}`: `{value}`\n"
    return text


def render_future_plugin_examples_markdown(examples: Sequence[Mapping[str, Any]]) -> str:
    text = "# Future Regime Plugin Examples\n\nIllustrations only. No new plugins are implemented in R8.\n\n"
    for item in examples:
        descriptor = item.get("descriptor", {})
        plan = item.get("artifact_plan", {})
        text += f"## {descriptor.get('plugin_id')} - {descriptor.get('engine_name')}\n\n"
        text += f"- Instrument family: {descriptor.get('instrument_family')}\n"
        text += f"- Feature providers: `{descriptor.get('feature_providers')}`\n"
        text += f"- Output namespace: `{plan.get('output_namespace')}`\n"
        text += f"- Validation rows: `{plan.get('validation_rows')}`\n"
        text += f"- Status: `{item.get('implementation_status')}`\n\n"
    return text


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
