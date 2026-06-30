"""Generic diagnostic regime engine interface contract.

The contract is descriptive and research-only. It gives future regime plugins
the common output shape needed by validation logging and scorecards without
adding runtime, broker, strategy, or Managed Exit authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence


INTERFACE_SCHEMA_VERSION = "track_b_regime_plugin_interface_v1"
SUPPORTED_FUTURE_PLUGINS = ("GRE", "NRE", "ERE", "TRE")
STANDARD_REGIME_LABELS = ("LONG", "SHORT", "CHOP", "TRANSITION", "INSUFFICIENT_EVIDENCE")
REQUIRED_OUTPUT_FIELDS = (
    "schema_version",
    "generated_at",
    "plugin_id",
    "instrument",
    "regime_label",
    "confidence",
    "directional_bias",
    "diagnostic_only",
    "positive_evidence",
    "negative_evidence",
    "conflicting_evidence",
    "missing_evidence",
    "source_refs",
)
VALIDATION_COMPATIBILITY_FIELDS = (
    "generated_at",
    "instrument",
    "symbols",
    "session_label",
    "regime_label",
    "confidence",
    "directional_bias",
    "diagnostic_only",
    "positive_evidence",
    "negative_evidence",
    "conflicting_evidence",
    "missing_evidence",
    "source_refs",
)


@dataclass(frozen=True)
class RegimePluginDescriptor:
    engine_name: str
    plugin_id: str
    instrument_family: str
    regime_labels: tuple[str, ...] = STANDARD_REGIME_LABELS
    feature_providers: tuple[str, ...] = ()
    diagnostic_only: bool = True


class RegimePlugin(Protocol):
    descriptor: RegimePluginDescriptor

    def evaluate(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        """Return a diagnostic-only regime report compatible with this contract."""


def regime_plugin_interface_contract() -> dict[str, Any]:
    return {
        "schema_version": INTERFACE_SCHEMA_VERSION,
        "diagnostic_only": True,
        "purpose": "Shared output contract for diagnostic regime plugins and research validation.",
        "supported_future_plugins": list(SUPPORTED_FUTURE_PLUGINS),
        "standard_regime_labels": list(STANDARD_REGIME_LABELS),
        "required_output_fields": list(REQUIRED_OUTPUT_FIELDS),
        "validation_compatibility_fields": list(VALIDATION_COMPATIBILITY_FIELDS),
        "scorecard_compatibility": {
            "regime_label": "Used for regime aggregation.",
            "confidence": "Used for confidence-band aggregation.",
            "positive_evidence": "Used for descriptive feature association.",
            "negative_evidence": "Used for descriptive feature association.",
            "conflicting_evidence": "Used for conflict analysis.",
            "missing_evidence": "Used for missing-provider analysis.",
        },
        "authority_boundary": {
            "broker_authority": False,
            "runtime_authority": False,
            "managed_exit_authority": False,
            "strategy_authority": False,
            "trading_gate": False,
        },
    }


def validate_regime_output_compatibility(report: Mapping[str, Any]) -> dict[str, Any]:
    missing = [field for field in REQUIRED_OUTPUT_FIELDS if field not in report]
    label = report.get("regime_label")
    confidence = report.get("confidence")
    issues: list[str] = []
    if label not in STANDARD_REGIME_LABELS:
        issues.append("regime_label_not_standard")
    if not isinstance(confidence, int) or not 0 <= confidence <= 100:
        issues.append("confidence_not_bounded_integer")
    if report.get("diagnostic_only") is not True:
        issues.append("diagnostic_only_not_true")
    for key in ("positive_evidence", "negative_evidence", "conflicting_evidence", "missing_evidence"):
        if not isinstance(report.get(key), Sequence) or isinstance(report.get(key), (str, bytes)):
            issues.append(f"{key}_not_sequence")
    return {
        "schema_version": INTERFACE_SCHEMA_VERSION,
        "compatible": not missing and not issues,
        "missing_required_fields": missing,
        "issues": issues,
    }


def render_regime_plugin_interface_markdown(contract: Mapping[str, Any] | None = None) -> str:
    payload = contract or regime_plugin_interface_contract()
    text = (
        "# Regime Plugin Interface\n\n"
        f"Schema version: `{payload.get('schema_version')}`\n\n"
        "This contract is diagnostic-only. It defines the output shape future regime engines need "
        "to share validation logging and scorecard infrastructure without becoming runtime authority.\n\n"
        "## Supported Plugin Families\n\n"
    )
    for plugin in payload.get("supported_future_plugins", []):
        text += f"- `{plugin}`\n"
    text += "\n## Standard Regime Labels\n\n"
    for label in payload.get("standard_regime_labels", []):
        text += f"- `{label}`\n"
    text += "\n## Required Output Fields\n\n"
    for field in payload.get("required_output_fields", []):
        text += f"- `{field}`\n"
    text += "\n## Authority Boundary\n\n"
    for key, value in payload.get("authority_boundary", {}).items():
        text += f"- `{key}`: `{value}`\n"
    return text
