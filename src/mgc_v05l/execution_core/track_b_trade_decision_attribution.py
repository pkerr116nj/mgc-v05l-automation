"""Canonical trade decision attribution for completed Track B outcomes.

This module is research/diagnostic only. It joins completed CTOL/CTOE rows
with canonical trade records and lifecycle evidence to explain, where evidence
exists, who entered/exited each trade and why. It has no broker, runtime,
strategy, Managed Exit, or gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL
DEFAULT_CANONICAL_RECORDS_PATH = DEFAULT_CANONICAL_TRADE_RECORDS
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "ra3_trade_decision_attribution"

ATTRIBUTION_JSONL = "canonical_trade_decision_attribution.jsonl"
CONTRACT_MD = "ra3_trade_decision_attribution_contract.md"
SCHEMA_JSON = "ra3_trade_decision_schema.json"
EXIT_TAXONOMY_MD = "ra3_trade_exit_taxonomy.md"
SAMPLE_JSON = "ra3_sample_trade_decisions.json"
SUMMARY_MD = "ra3_trade_decision_summary.md"
UNKNOWN_MD = "ra3_unknown_reason_inventory.md"
SUMMARY_JSON = "ra3_trade_decision_summary.json"

SCHEMA_VERSION = "canonical_trade_decision_attribution_v1"
UNKNOWN = "UNKNOWN"

EXIT_TAXONOMY = (
    "TIMEBOX",
    "MANAGED_EXIT_TIMEOUT",
    "MANAGED_EXIT_POLICY",
    "STRATEGY_SIGNAL",
    "PROFIT_TARGET",
    "STOP_LOSS",
    "SESSION_CLOSE",
    "SAFE_STATE",
    "MANUAL_OPERATOR",
    "BROKER_RECONCILIATION",
    "UNKNOWN",
)


@dataclass(frozen=True)
class TradeDecisionAttributionResult:
    attributions: list[dict[str, Any]]
    summary: dict[str, Any]
    attribution_path: Path
    summary_json_path: Path
    summary_markdown_path: Path
    contract_path: Path
    schema_path: Path
    taxonomy_path: Path
    sample_path: Path
    unknown_path: Path


def run_trade_decision_attribution(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    canonical_records_path: Path = DEFAULT_CANONICAL_RECORDS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> TradeDecisionAttributionResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    canonical_records = _read_jsonl(canonical_records_path)
    attributions = build_trade_decision_attributions(
        outcomes,
        enrichments=enrichments,
        canonical_records=canonical_records,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "canonical_trade_records": canonical_records_path,
        },
    )
    summary = build_trade_decision_summary(
        attributions,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "canonical_trade_records": canonical_records_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    attribution_path = output_dir / ATTRIBUTION_JSONL
    summary_json_path = output_dir / SUMMARY_JSON
    summary_markdown_path = output_dir / SUMMARY_MD
    contract_path = output_dir / CONTRACT_MD
    schema_path = output_dir / SCHEMA_JSON
    taxonomy_path = output_dir / EXIT_TAXONOMY_MD
    sample_path = output_dir / SAMPLE_JSON
    unknown_path = output_dir / UNKNOWN_MD
    _write_jsonl(attribution_path, attributions)
    _write_json(summary_json_path, summary)
    schema_path.write_text(json.dumps(_schema(), indent=2, sort_keys=True), encoding="utf-8")
    sample_path.write_text(json.dumps(attributions[:10], indent=2, sort_keys=True), encoding="utf-8")
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    taxonomy_path.write_text(render_exit_taxonomy_markdown(), encoding="utf-8")
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    unknown_path.write_text(render_unknown_inventory_markdown(summary), encoding="utf-8")
    return TradeDecisionAttributionResult(
        attributions=attributions,
        summary=summary,
        attribution_path=attribution_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        contract_path=contract_path,
        schema_path=schema_path,
        taxonomy_path=taxonomy_path,
        sample_path=sample_path,
        unknown_path=unknown_path,
    )


def build_trade_decision_attributions(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]] = (),
    canonical_records: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    enrichment_index = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    canonical_index = _CanonicalRecordIndex(canonical_records)
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        enrichment = enrichment_index.get(str(outcome.get("trade_outcome_id")), {})
        canonical = canonical_index.find(outcome)
        lifecycle_entry = _load_json_from_ref(_source_ref(canonical, "managed_lifecycle_report"))
        lifecycle_exit = _load_json_from_ref(_source_ref(canonical, "exit_source_artifact"))
        entry = _entry_attribution(outcome, enrichment=enrichment, canonical=canonical, lifecycle=lifecycle_entry)
        exit_attr = _exit_attribution(outcome, canonical=canonical, lifecycle_entry=lifecycle_entry, lifecycle_exit=lifecycle_exit)
        provenance = _provenance(
            outcome,
            enrichment=enrichment,
            canonical=canonical,
            lifecycle_entry=lifecycle_entry,
            lifecycle_exit=lifecycle_exit,
            source_paths=source_paths or {},
        )
        missing = _missing_reasons(entry, exit_attr, canonical=canonical, lifecycle_entry=lifecycle_entry, lifecycle_exit=lifecycle_exit)
        row = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "trade_decision_attribution_id": _stable_id("trade_decision_attribution", outcome.get("trade_outcome_id"), outcome.get("entry_time"), outcome.get("exit_time")),
            "trade_outcome_id": outcome.get("trade_outcome_id"),
            "entry": entry,
            "exit": exit_attr,
            "attribution_confidence": _confidence(entry, exit_attr, missing),
            "unknown_reasons": missing,
            "provenance": provenance,
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        }
        row["deterministic_fingerprint"] = _fingerprint({k: v for k, v in row.items() if k not in {"generated_at", "deterministic_fingerprint"}})
        rows.append(row)
    rows.sort(key=lambda row: (str(row["entry"].get("entry_timestamp") or ""), str(row.get("trade_outcome_id") or "")))
    return rows


def build_trade_decision_summary(
    attributions: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total = len(attributions)
    exit_reason_distribution = _counts(row.get("exit", {}).get("canonical_exit_reason") for row in attributions)
    exit_authority_distribution = _counts(row.get("exit", {}).get("exit_authority") for row in attributions)
    entry_authority_distribution = _counts(row.get("entry", {}).get("entry_authority") for row in attributions)
    managed_exit_count = sum(1 for row in attributions if row.get("exit", {}).get("managed_exit_involved") is True)
    entry_complete = sum(1 for row in attributions if not _entry_unknown(row))
    exit_complete = sum(1 for row in attributions if not _exit_unknown(row))
    unknown_reasons = _counts(reason for row in attributions for reason in row.get("unknown_reasons", []))
    incomplete = [
        {
            "trade_outcome_id": row.get("trade_outcome_id"),
            "entry_timestamp": row.get("entry", {}).get("entry_timestamp"),
            "lane_id": row.get("entry", {}).get("lane_id"),
            "instrument": row.get("entry", {}).get("instrument"),
            "unknown_reasons": row.get("unknown_reasons", []),
        }
        for row in attributions
        if row.get("unknown_reasons")
    ]
    return {
        "schema_version": "ra3_trade_decision_attribution_summary_v1",
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "completed_trade_attributions": total,
            "entry_attribution_complete": entry_complete,
            "entry_attribution_completeness": _rate(entry_complete, total),
            "exit_attribution_complete": exit_complete,
            "exit_attribution_completeness": _rate(exit_complete, total),
            "managed_exit_involvement_count": managed_exit_count,
            "managed_exit_involvement_rate": _rate(managed_exit_count, total),
            "unknown_attribution_count": len(incomplete),
            "unknown_attribution_rate": _rate(len(incomplete), total),
        },
        "distributions": {
            "canonical_exit_reason": exit_reason_distribution,
            "exit_authority": exit_authority_distribution,
            "entry_authority": entry_authority_distribution,
            "attribution_confidence": _counts(row.get("attribution_confidence") for row in attributions),
        },
        "unknown_reason_inventory": unknown_reasons,
        "trades_with_incomplete_attribution": incomplete[:200],
        "coverage_percentages": {
            "entry_signal_known": _rate(sum(1 for row in attributions if row.get("entry", {}).get("entry_signal") != UNKNOWN), total),
            "entry_regime_context_available": _rate(sum(1 for row in attributions if _has_regime_context(row)), total),
            "exit_lifecycle_evidence_available": _rate(sum(1 for row in attributions if row.get("exit", {}).get("lifecycle_evidence_available") is True), total),
            "managed_exit_involvement": _rate(managed_exit_count, total),
        },
    }


class _CanonicalRecordIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_exact: dict[tuple[Any, ...], Mapping[str, Any]] = {}
        self._by_loose: dict[tuple[Any, ...], Mapping[str, Any]] = {}
        self._by_trade_id: dict[str, Mapping[str, Any]] = {}
        self._rows = []
        for row in rows:
            if row.get("trade_status") != "CLOSED" or row.get("pairing_status") != "PAIRED":
                continue
            self._rows.append(row)
            if row.get("trade_id"):
                self._by_trade_id.setdefault(str(row.get("trade_id")), row)
            exact = (
                _norm_ts(row.get("entry_time")),
                _norm_ts(row.get("exit_time")),
                str(row.get("symbol") or ""),
                str(row.get("local_symbol") or ""),
                str(row.get("lane_id") or ""),
            )
            loose = (_norm_ts(row.get("entry_time")), _norm_ts(row.get("exit_time")), str(row.get("symbol") or ""), str(row.get("lane_id") or ""))
            self._by_exact.setdefault(exact, row)
            self._by_loose.setdefault(loose, row)

    def find(self, outcome: Mapping[str, Any]) -> Mapping[str, Any]:
        refs = outcome.get("source_refs") if isinstance(outcome.get("source_refs"), Mapping) else {}
        source_trade_id = refs.get("source_trade_id")
        if source_trade_id and str(source_trade_id) in self._by_trade_id:
            return self._by_trade_id[str(source_trade_id)]
        exact = (
            _norm_ts(outcome.get("entry_time")),
            _norm_ts(outcome.get("exit_time")),
            str(outcome.get("instrument") or ""),
            str(outcome.get("contract") or ""),
            str(outcome.get("lane_id") or ""),
        )
        loose = (_norm_ts(outcome.get("entry_time")), _norm_ts(outcome.get("exit_time")), str(outcome.get("instrument") or ""), str(outcome.get("lane_id") or ""))
        found = self._by_exact.get(exact) or self._by_loose.get(loose)
        if found:
            return found
        return self._find_with_timestamp_tolerance(outcome)

    def _find_with_timestamp_tolerance(self, outcome: Mapping[str, Any]) -> Mapping[str, Any]:
        entry_time = _parse_ts(outcome.get("entry_time"))
        exit_time = _parse_ts(outcome.get("exit_time"))
        instrument = str(outcome.get("instrument") or "")
        contract = str(outcome.get("contract") or "")
        lane = str(outcome.get("lane_id") or "")
        if entry_time is None or exit_time is None:
            return {}
        for row in self._rows:
            if str(row.get("symbol") or "") != instrument:
                continue
            if contract and str(row.get("local_symbol") or "") != contract:
                continue
            if lane and str(row.get("lane_id") or "") != lane:
                continue
            row_entry = _parse_ts(row.get("entry_time"))
            row_exit = _parse_ts(row.get("exit_time"))
            if row_entry is None or row_exit is None:
                continue
            if abs((row_entry - entry_time).total_seconds()) <= 0.001 and abs((row_exit - exit_time).total_seconds()) <= 0.001:
                return row
        return {}


def _entry_attribution(
    outcome: Mapping[str, Any],
    *,
    enrichment: Mapping[str, Any],
    canonical: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
) -> dict[str, Any]:
    entry_intent = lifecycle.get("entry_intent") if isinstance(lifecycle.get("entry_intent"), Mapping) else {}
    signal = _first_non_unknown(entry_intent.get("signal_reason"), canonical.get("entry_thesis"))
    authority = _entry_authority(canonical, lifecycle)
    return {
        "strategy_candidate": _first_non_unknown(outcome.get("strategy_id"), canonical.get("strategy_id")),
        "lane_id": _first_non_unknown(outcome.get("lane_id"), canonical.get("lane_id")),
        "entry_authority": authority,
        "entry_signal": signal,
        "entry_timestamp": _first_non_unknown(outcome.get("entry_time"), canonical.get("entry_time")),
        "instrument": _first_non_unknown(outcome.get("instrument"), canonical.get("symbol")),
        "contract": _first_non_unknown(outcome.get("contract"), canonical.get("local_symbol")),
        "side": _first_non_unknown(outcome.get("side"), canonical.get("side")),
        "session": _first_non_unknown(outcome.get("session_at_entry"), canonical.get("session_label"), enrichment.get("session")),
        "vix_context": _context(
            available=enrichment.get("market_context_validity_classification") == "VALID",
            fields={
                "vix_regime": enrichment.get("vix_regime"),
                "vix_level": enrichment.get("vix_level"),
                "vix_percentile": enrichment.get("vix_percentile"),
                "timestamp": enrichment.get("market_context_timestamp"),
            },
        ),
        "gre_context": _context(
            available=enrichment.get("gre_validity_classification") == "VALID",
            fields={
                "gre_label": enrichment.get("gre_label"),
                "gre_confidence": enrichment.get("gre_confidence"),
                "gre_timestamp": enrichment.get("gre_timestamp"),
                "validity": enrichment.get("gre_validity_classification"),
            },
        ),
        "crfd_context": _context(
            available=enrichment.get("crfd_validity_classification") == "VALID",
            fields={
                "validity": enrichment.get("crfd_validity_classification"),
                "observation_time": enrichment.get("crfd_observation_time"),
                "vwap_relation": enrichment.get("vwap_relation"),
                "avwap_relation": enrichment.get("avwap_relation"),
            },
        ),
    }


def _exit_attribution(
    outcome: Mapping[str, Any],
    *,
    canonical: Mapping[str, Any],
    lifecycle_entry: Mapping[str, Any],
    lifecycle_exit: Mapping[str, Any],
) -> dict[str, Any]:
    lifecycle = lifecycle_exit or lifecycle_entry
    close_intent = lifecycle.get("close_intent") if isinstance(lifecycle.get("close_intent"), Mapping) else {}
    close_submit = lifecycle.get("close_submit_attempt") if isinstance(lifecycle.get("close_submit_attempt"), Mapping) else {}
    close_fill = lifecycle.get("close_fill") if isinstance(lifecycle.get("close_fill"), Mapping) else {}
    exit_policy = _first_non_unknown(outcome.get("exit_policy"), canonical.get("exit_policy"), close_intent.get("managed_exit_policy_id"), lifecycle.get("managed_exit_policy_id"))
    raw_reason = _first_non_unknown(canonical.get("exit_reason"), outcome.get("exit_reason"), close_intent.get("close_reason"), exit_policy)
    canonical_reason = _canonical_exit_reason(raw_reason, close_intent=close_intent, lifecycle=lifecycle, canonical=canonical)
    return {
        "exit_authority": _exit_authority(canonical=canonical, lifecycle=lifecycle, close_intent=close_intent, close_submit=close_submit),
        "canonical_exit_reason": canonical_reason,
        "raw_exit_reason": raw_reason,
        "exit_policy": exit_policy,
        "lifecycle_classification": _first_non_unknown(lifecycle.get("final_broker_state_classification"), lifecycle.get("final_position_status")),
        "managed_exit_involved": _managed_exit_involved(canonical=canonical, lifecycle=lifecycle, close_intent=close_intent, close_submit=close_submit, close_fill=close_fill),
        "close_reason": _first_non_unknown(close_intent.get("close_reason"), canonical.get("exit_reason"), outcome.get("exit_reason")),
        "exit_family": _first_non_unknown(close_intent.get("exit_family")),
        "required_completed_bars": _first_non_unknown(close_intent.get("required_completed_5m_bars")),
        "elapsed_completed_bars": _first_non_unknown(close_intent.get("elapsed_completed_5m_bars"), lifecycle.get("bars_since_fill")),
        "close_submitted": bool(close_submit.get("submitted") or close_submit.get("broker_order_id")),
        "close_filled": bool(close_fill or canonical.get("exit_exec_id")),
        "exit_timestamp": _first_non_unknown(outcome.get("exit_time"), canonical.get("exit_time")),
        "lifecycle_evidence_available": bool(lifecycle),
    }


def _entry_authority(canonical: Mapping[str, Any], lifecycle: Mapping[str, Any]) -> str:
    if lifecycle.get("entry_submit_attempt") or lifecycle.get("entry_intent"):
        return "TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE"
    refs = canonical.get("source_refs") if isinstance(canonical.get("source_refs"), Mapping) else {}
    if refs.get("entry_fill"):
        return "PAPER_SESSION_BRIDGE_FILL"
    return UNKNOWN


def _exit_authority(
    *,
    canonical: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    close_intent: Mapping[str, Any],
    close_submit: Mapping[str, Any],
) -> str:
    if close_intent or close_submit:
        return "MANAGED_EXIT"
    refs = canonical.get("source_refs") if isinstance(canonical.get("source_refs"), Mapping) else {}
    if refs.get("exit_source_artifact") or refs.get("exit_fill"):
        return "TRACK_B_LIFECYCLE_OR_REGISTRY"
    return UNKNOWN


def _canonical_exit_reason(
    raw_reason: Any,
    *,
    close_intent: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    canonical: Mapping[str, Any],
) -> str:
    text = str(raw_reason or "").upper()
    policy = str(close_intent.get("managed_exit_policy_id") or lifecycle.get("managed_exit_policy_id") or canonical.get("exit_policy") or "").upper()
    source = f"{text} {policy}"
    if "SAFE_STATE" in source:
        return "SAFE_STATE"
    if "MANUAL" in source or "OPERATOR" in source:
        return "MANUAL_OPERATOR"
    if "RECONCILIATION" in source:
        return "BROKER_RECONCILIATION"
    if "STOP" in source:
        return "STOP_LOSS"
    if "TARGET" in source or "PROFIT" in source:
        return "PROFIT_TARGET"
    if "SESSION_CLOSE" in source:
        return "SESSION_CLOSE"
    if close_intent and "TIME" in source:
        return "MANAGED_EXIT_TIMEOUT"
    if "TIMEBOX" in source or "TIME_BOX" in source or "TIME" in source:
        return "TIMEBOX"
    if close_intent:
        return "MANAGED_EXIT_POLICY"
    return UNKNOWN


def _managed_exit_involved(
    *,
    canonical: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    close_intent: Mapping[str, Any],
    close_submit: Mapping[str, Any],
    close_fill: Mapping[str, Any],
) -> bool:
    refs = canonical.get("source_refs") if isinstance(canonical.get("source_refs"), Mapping) else {}
    ref_text = " ".join(str(refs.get(key) or "") for key in ("exit_source_artifact", "exit_fill"))
    return bool(close_intent or close_submit or close_fill or "managed_exit" in ref_text.lower() or "reserved_submit" in ref_text.lower())


def _provenance(
    outcome: Mapping[str, Any],
    *,
    enrichment: Mapping[str, Any],
    canonical: Mapping[str, Any],
    lifecycle_entry: Mapping[str, Any],
    lifecycle_exit: Mapping[str, Any],
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    refs = canonical.get("source_refs") if isinstance(canonical.get("source_refs"), Mapping) else {}
    return {
        "source_paths": {key: str(value) for key, value in source_paths.items()},
        "source_refs": {
            "ctol_trade_outcome_id": outcome.get("trade_outcome_id"),
            "ctoe_present": bool(enrichment),
            "canonical_trade_id": canonical.get("trade_id"),
            "canonical_lifecycle_id": canonical.get("lifecycle_id"),
            "managed_lifecycle_report": refs.get("managed_lifecycle_report"),
            "exit_source_artifact": refs.get("exit_source_artifact"),
            "entry_fill": refs.get("entry_fill"),
            "exit_fill": refs.get("exit_fill"),
        },
        "evidence_present": {
            "ctol": bool(outcome),
            "ctoe": bool(enrichment),
            "canonical_trade_record": bool(canonical),
            "entry_lifecycle_report": bool(lifecycle_entry),
            "exit_lifecycle_report": bool(lifecycle_exit),
        },
    }


def _missing_reasons(
    entry: Mapping[str, Any],
    exit_attr: Mapping[str, Any],
    *,
    canonical: Mapping[str, Any],
    lifecycle_entry: Mapping[str, Any],
    lifecycle_exit: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not canonical:
        reasons.append("missing_canonical_trade_record_join")
    if not lifecycle_entry:
        reasons.append("missing_entry_lifecycle_report")
    if not lifecycle_exit:
        reasons.append("missing_exit_lifecycle_report")
    if entry.get("entry_authority") == UNKNOWN:
        reasons.append("unknown_entry_authority")
    if entry.get("entry_signal") == UNKNOWN:
        reasons.append("unknown_entry_signal")
    if exit_attr.get("exit_authority") == UNKNOWN:
        reasons.append("unknown_exit_authority")
    if exit_attr.get("canonical_exit_reason") == UNKNOWN:
        reasons.append("unknown_exit_reason")
    if exit_attr.get("lifecycle_classification") == UNKNOWN:
        reasons.append("unknown_lifecycle_classification")
    return sorted(set(reasons))


def _confidence(entry: Mapping[str, Any], exit_attr: Mapping[str, Any], missing: Sequence[str]) -> str:
    if not missing:
        return "HIGH"
    critical = {"missing_canonical_trade_record_join", "unknown_exit_reason", "unknown_exit_authority", "unknown_entry_authority"}
    if critical.intersection(missing):
        return "LOW"
    if entry.get("entry_signal") == UNKNOWN or exit_attr.get("lifecycle_classification") == UNKNOWN:
        return "MEDIUM"
    return "MEDIUM"


def _entry_unknown(row: Mapping[str, Any]) -> bool:
    entry = row.get("entry", {})
    return entry.get("entry_authority") == UNKNOWN or entry.get("entry_signal") == UNKNOWN


def _exit_unknown(row: Mapping[str, Any]) -> bool:
    exit_attr = row.get("exit", {})
    return exit_attr.get("exit_authority") == UNKNOWN or exit_attr.get("canonical_exit_reason") == UNKNOWN


def _has_regime_context(row: Mapping[str, Any]) -> bool:
    entry = row.get("entry", {})
    return any(
        entry.get(key, {}).get("available") is True
        for key in ("vix_context", "gre_context", "crfd_context")
        if isinstance(entry.get(key), Mapping)
    )


def _context(*, available: bool, fields: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "available": bool(available),
        "fields": {key: _known_or_unknown(value) for key, value in fields.items()},
    }


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    lines = [
        "# RA3 Trade Decision Attribution Summary",
        "",
        f"Generated: `{summary.get('generated_at')}`",
        "",
        "Research/diagnostic only. This report explains completed trade entry/exit attribution from existing evidence. It is not a production recommendation and creates no trading gate.",
        "",
        "## Overall",
        "",
        f"- Completed trade attributions: `{overall.get('completed_trade_attributions')}`",
        f"- Entry attribution completeness: `{overall.get('entry_attribution_completeness')}`",
        f"- Exit attribution completeness: `{overall.get('exit_attribution_completeness')}`",
        f"- Managed Exit involvement: `{overall.get('managed_exit_involvement_count')}` / `{overall.get('completed_trade_attributions')}`",
        f"- Unknown attribution rate: `{overall.get('unknown_attribution_rate')}`",
        "",
        "## Canonical Exit Reason Distribution",
        "",
        _dict_table(summary.get("distributions", {}).get("canonical_exit_reason", {})),
        "",
        "## Exit Authority Distribution",
        "",
        _dict_table(summary.get("distributions", {}).get("exit_authority", {})),
        "",
        "## Entry Authority Distribution",
        "",
        _dict_table(summary.get("distributions", {}).get("entry_authority", {})),
    ]
    return "\n".join(lines) + "\n"


def render_unknown_inventory_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# RA3 Unknown Reason Inventory",
        "",
        "Unknown values are explicit by design. They indicate unavailable evidence, not inferred failure.",
        "",
        _dict_table(summary.get("unknown_reason_inventory", {})),
        "",
        "## Incomplete Attribution Samples",
        "",
    ]
    samples = summary.get("trades_with_incomplete_attribution", [])[:50]
    if not samples:
        lines.append("No incomplete attribution rows.")
    else:
        lines.append("|trade_outcome_id|entry_timestamp|lane_id|instrument|unknown_reasons|")
        lines.append("|---|---|---|---|---|")
        for row in samples:
            lines.append(
                f"|{row.get('trade_outcome_id')}|{row.get('entry_timestamp')}|{row.get('lane_id')}|{row.get('instrument')}|{', '.join(row.get('unknown_reasons', []))}|"
            )
    return "\n".join(lines) + "\n"


def render_contract_markdown() -> str:
    return """# RA3 Trade Decision Attribution Contract

`CanonicalTradeDecisionAttribution` is a deterministic research enrichment for completed CTOL/CTOE trades.

It answers, to the extent supported by evidence:

- Who entered this trade?
- Why was it entered?
- Who exited it?
- Why was it exited?
- How certain are we?
- Where is the provenance?

Guardrails:

- `diagnostic_only=true`
- `production_recommendation=false`
- `trading_gate=false`
- no broker authority
- no runtime authority
- no strategy authority
- no Managed Exit authority

Unknown values must be emitted as `UNKNOWN`, never inferred.
"""


def render_exit_taxonomy_markdown() -> str:
    lines = [
        "# RA3 Canonical Exit Attribution Taxonomy",
        "",
        "|value|meaning|",
        "|---|---|",
        "|TIMEBOX|Time-based exit policy evidence without specific Managed Exit lifecycle proof.|",
        "|MANAGED_EXIT_TIMEOUT|Managed Exit lifecycle/close intent shows a time-boxed exit.|",
        "|MANAGED_EXIT_POLICY|Managed Exit lifecycle evidence exists, but the policy is not classified as timeout/target/stop/etc.|",
        "|STRATEGY_SIGNAL|Evidence indicates a strategy signal caused the exit.|",
        "|PROFIT_TARGET|Evidence indicates a profit target exit.|",
        "|STOP_LOSS|Evidence indicates a stop-loss/risk-loss exit.|",
        "|SESSION_CLOSE|Evidence indicates a session-close exit.|",
        "|SAFE_STATE|Evidence indicates Safe-State caused or forced the exit.|",
        "|MANUAL_OPERATOR|Evidence indicates manual operator action.|",
        "|BROKER_RECONCILIATION|Evidence indicates broker reconciliation or repair closure.|",
        "|UNKNOWN|Available evidence does not support a deterministic classification.|",
    ]
    return "\n".join(lines) + "\n"


def _schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalTradeDecisionAttribution",
        "type": "object",
        "required": [
            "schema_version",
            "trade_decision_attribution_id",
            "trade_outcome_id",
            "entry",
            "exit",
            "attribution_confidence",
            "provenance",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
        ],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "trade_decision_attribution_id": {"type": "string"},
            "trade_outcome_id": {"type": ["string", "null"]},
            "entry": {"type": "object"},
            "exit": {"type": "object"},
            "attribution_confidence": {"enum": ["HIGH", "MEDIUM", "LOW"]},
            "unknown_reasons": {"type": "array", "items": {"type": "string"}},
            "provenance": {"type": "object"},
            "diagnostic_only": {"const": True},
            "production_recommendation": {"const": False},
            "trading_gate": {"const": False},
        },
    }


def _source_ref(canonical: Mapping[str, Any], key: str) -> str | None:
    refs = canonical.get("source_refs") if isinstance(canonical.get("source_refs"), Mapping) else {}
    value = refs.get(key)
    return str(value) if value else None


def _load_json_from_ref(ref: str | None) -> Mapping[str, Any]:
    if not ref:
        return {}
    path = Path(ref)
    if not path.exists() and str(ref).startswith("/"):
        path = Path(ref)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _norm_ts(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("+00:00", "Z")


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _known_or_unknown(value: Any) -> Any:
    return UNKNOWN if value in (None, "", [], {}) else value


def _first_non_unknown(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return UNKNOWN


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _fingerprint(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value if value not in (None, "") else UNKNOWN)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _rate(count: int, total: int) -> float | None:
    return round(count / total, 6) if total else None


def _dict_table(data: Mapping[str, Any]) -> str:
    if not data:
        return "No rows."
    lines = ["|value|count|", "|---|---:|"]
    for key, value in data.items():
        lines.append(f"|{key}|{value}|")
    return "\n".join(lines)
