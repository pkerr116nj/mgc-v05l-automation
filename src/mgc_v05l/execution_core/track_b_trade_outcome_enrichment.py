"""Canonical research enrichment for Track B trade outcome records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_LAYER_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOMES_JSONL
DEFAULT_CRFD_ROWS = DEFAULT_OUTPUT_ROOT / "research" / "canonical_research_feature_dataset" / "research_feature_dataset.jsonl"
DEFAULT_GRE_REPORT = DEFAULT_OUTPUT_ROOT / "research" / "gold_regime_engine" / "latest_gold_regime_engine.json"
DEFAULT_HISTORICAL_GRE_ROWS = DEFAULT_OUTPUT_ROOT / "research" / "gold_regime_engine" / "gre_backfill_observations.jsonl"
DEFAULT_MARKET_CONTEXT_ROWS = DEFAULT_OUTPUT_ROOT / "research" / "canonical_market_context" / "canonical_market_context.jsonl"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "trade_outcome_enrichment"

ENRICHMENT_JSONL = "canonical_trade_outcome_enrichment.jsonl"
SUMMARY_JSON = "latest_trade_outcome_enrichment_summary.json"
SUMMARY_MD = "latest_trade_outcome_enrichment_summary.md"
CONTRACT_MD = "trade_outcome_enrichment_contract.md"
DATA_QUALITY_MD = "trade_outcome_enrichment_data_quality.md"
MARKET_CONTEXT_REPORT_MD = "market_context_enrichment_report.md"
MARKET_CONTEXT_JOIN_QUALITY_MD = "market_context_join_quality.md"
MARKET_CONTEXT_DATA_QUALITY_MD = "trade_outcome_market_context_data_quality.md"
HISTORICAL_GRE_REPORT_MD = "historical_gre_trade_enrichment_report.md"
HISTORICAL_GRE_JOIN_QUALITY_MD = "historical_gre_join_quality.md"
HISTORICAL_GRE_DATA_QUALITY_MD = "historical_gre_trade_data_quality.md"
CONTEXT_VALIDITY_CONTRACT_MD = "canonical_context_validity_contract.md"
CONTEXT_VALIDITY_CONTRACT_JSON = "canonical_context_validity_contract.json"
CONTEXT_VALIDITY_REPORT_MD = "context_validity_enrichment_report.md"
CONTEXT_VALIDITY_DATA_QUALITY_MD = "context_validity_data_quality.md"
PROVIDER_VALIDITY_MATRIX_MD = "provider_validity_matrix.md"

SCHEMA_VERSION = "track_b_trade_outcome_enrichment_v1"
SUMMARY_SCHEMA_VERSION = "track_b_trade_outcome_enrichment_summary_v1"
DEFAULT_MAX_CRFD_JOIN_AGE_SECONDS = 7 * 24 * 60 * 60
DEFAULT_MAX_GRE_JOIN_AGE_SECONDS = 60 * 60
DEFAULT_MAX_MARKET_CONTEXT_JOIN_AGE_SECONDS = 7 * 24 * 60 * 60
CONTEXT_VALIDITY_SCHEMA_VERSION = "canonical_context_validity_contract_v1"
VALIDITY_VALID = "VALID"
VALIDITY_STALE = "STALE"
VALIDITY_OUTSIDE_PROVIDER_WINDOW = "OUTSIDE_PROVIDER_WINDOW"
VALIDITY_UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class TradeOutcomeEnrichmentResult:
    enrichments: list[dict[str, Any]]
    summary: dict[str, Any]
    enrichment_path: Path
    summary_path: Path
    summary_markdown_path: Path
    contract_path: Path
    data_quality_path: Path
    market_context_report_path: Path
    market_context_join_quality_path: Path
    market_context_data_quality_path: Path
    historical_gre_report_path: Path
    historical_gre_join_quality_path: Path
    historical_gre_data_quality_path: Path
    context_validity_contract_path: Path
    context_validity_contract_json_path: Path
    context_validity_report_path: Path
    context_validity_data_quality_path: Path
    provider_validity_matrix_path: Path


def run_trade_outcome_enrichment(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    crfd_rows_path: Path = DEFAULT_CRFD_ROWS,
    gre_report_path: Path = DEFAULT_GRE_REPORT,
    historical_gre_rows_path: Path = DEFAULT_HISTORICAL_GRE_ROWS,
    market_context_rows_path: Path = DEFAULT_MARKET_CONTEXT_ROWS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
    jsonl_config: BoundedJsonlConfig | None = None,
) -> TradeOutcomeEnrichmentResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    crfd_rows = _read_jsonl(crfd_rows_path)
    gre_report = _read_json_mapping(gre_report_path)
    historical_gre_rows = _read_jsonl(historical_gre_rows_path)
    market_context_rows = _read_jsonl(market_context_rows_path)
    enrichments = build_trade_outcome_enrichments(
        outcomes,
        crfd_rows=crfd_rows,
        gre_report=gre_report,
        historical_gre_rows=historical_gre_rows,
        market_context_rows=market_context_rows,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "crfd_rows": crfd_rows_path,
            "gre_report": gre_report_path,
            "historical_gre_rows": historical_gre_rows_path,
            "market_context_rows": market_context_rows_path,
        },
    )
    summary = build_trade_outcome_enrichment_summary(
        enrichments,
        outcome_count=len(outcomes),
        crfd_row_count=len(crfd_rows),
        gre_report=gre_report,
        historical_gre_row_count=len(historical_gre_rows),
        market_context_row_count=len(market_context_rows),
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "crfd_rows": crfd_rows_path,
            "gre_report": gre_report_path,
            "historical_gre_rows": historical_gre_rows_path,
            "market_context_rows": market_context_rows_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    enrichment_path = output_dir / ENRICHMENT_JSONL
    write_bounded_jsonl(enrichment_path, enrichments, config=jsonl_config)
    snapshot_config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    summary_path = output_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    summary_md = output_dir / SUMMARY_MD
    summary_md.write_text(render_enrichment_summary_markdown(summary), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_enrichment_contract_markdown(), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_enrichment_data_quality_markdown(summary), encoding="utf-8")
    market_context_report_path = output_dir / MARKET_CONTEXT_REPORT_MD
    market_context_report_path.write_text(render_market_context_report_markdown(summary), encoding="utf-8")
    market_context_join_quality_path = output_dir / MARKET_CONTEXT_JOIN_QUALITY_MD
    market_context_join_quality_path.write_text(render_market_context_join_quality_markdown(summary), encoding="utf-8")
    market_context_data_quality_path = output_dir / MARKET_CONTEXT_DATA_QUALITY_MD
    market_context_data_quality_path.write_text(render_market_context_data_quality_markdown(summary), encoding="utf-8")
    historical_gre_report_path = output_dir / HISTORICAL_GRE_REPORT_MD
    historical_gre_report_path.write_text(render_historical_gre_report_markdown(summary), encoding="utf-8")
    historical_gre_join_quality_path = output_dir / HISTORICAL_GRE_JOIN_QUALITY_MD
    historical_gre_join_quality_path.write_text(render_historical_gre_join_quality_markdown(summary), encoding="utf-8")
    historical_gre_data_quality_path = output_dir / HISTORICAL_GRE_DATA_QUALITY_MD
    historical_gre_data_quality_path.write_text(render_historical_gre_data_quality_markdown(summary), encoding="utf-8")
    context_validity_contract = build_context_validity_contract(generated_at)
    context_validity_contract_path = output_dir / CONTEXT_VALIDITY_CONTRACT_MD
    context_validity_contract_path.write_text(render_context_validity_contract_markdown(context_validity_contract), encoding="utf-8")
    context_validity_contract_json_path = output_dir / CONTEXT_VALIDITY_CONTRACT_JSON
    write_bounded_snapshot_json(context_validity_contract_json_path, context_validity_contract, config=snapshot_config)
    context_validity_report_path = output_dir / CONTEXT_VALIDITY_REPORT_MD
    context_validity_report_path.write_text(render_context_validity_report_markdown(summary), encoding="utf-8")
    context_validity_data_quality_path = output_dir / CONTEXT_VALIDITY_DATA_QUALITY_MD
    context_validity_data_quality_path.write_text(render_context_validity_data_quality_markdown(summary), encoding="utf-8")
    provider_validity_matrix_path = output_dir / PROVIDER_VALIDITY_MATRIX_MD
    provider_validity_matrix_path.write_text(render_provider_validity_matrix_markdown(summary), encoding="utf-8")
    return TradeOutcomeEnrichmentResult(
        enrichments=enrichments,
        summary=summary,
        enrichment_path=enrichment_path,
        summary_path=summary_path,
        summary_markdown_path=summary_md,
        contract_path=contract_path,
        data_quality_path=data_quality_path,
        market_context_report_path=market_context_report_path,
        market_context_join_quality_path=market_context_join_quality_path,
        market_context_data_quality_path=market_context_data_quality_path,
        historical_gre_report_path=historical_gre_report_path,
        historical_gre_join_quality_path=historical_gre_join_quality_path,
        historical_gre_data_quality_path=historical_gre_data_quality_path,
        context_validity_contract_path=context_validity_contract_path,
        context_validity_contract_json_path=context_validity_contract_json_path,
        context_validity_report_path=context_validity_report_path,
        context_validity_data_quality_path=context_validity_data_quality_path,
        provider_validity_matrix_path=provider_validity_matrix_path,
    )


def build_trade_outcome_enrichments(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    crfd_rows: Sequence[Mapping[str, Any]] = (),
    gre_report: Mapping[str, Any] | None = None,
    historical_gre_rows: Sequence[Mapping[str, Any]] = (),
    market_context_rows: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
    max_crfd_join_age_seconds: int = DEFAULT_MAX_CRFD_JOIN_AGE_SECONDS,
    max_gre_join_age_seconds: int = DEFAULT_MAX_GRE_JOIN_AGE_SECONDS,
) -> list[dict[str, Any]]:
    crfd_index = _CrfdIndex(crfd_rows)
    historical_gre_index = _HistoricalGreIndex(historical_gre_rows)
    market_context_index = _MarketContextIndex(market_context_rows)
    gre = dict(gre_report or {})
    enrichments: list[dict[str, Any]] = []
    for outcome in outcomes:
        entry_time = _parse_datetime(outcome.get("entry_time"))
        contract = str(outcome.get("contract") or outcome.get("instrument") or "")
        crfd = crfd_index.latest_at_or_before(contract=contract, timestamp=entry_time)
        crfd_age = _age_seconds(crfd.get("observation_time") if crfd else None, entry_time)
        crfd_join_success = crfd is not None and (crfd_age is None or crfd_age <= max_crfd_join_age_seconds)
        if not crfd_join_success:
            crfd = None
            crfd_age = None
        historical_gre = historical_gre_index.latest_at_or_before(outcome=outcome, timestamp=entry_time)
        gre_context = _select_gre_context(
            outcome,
            crfd=crfd,
            gre_report=gre,
            historical_gre=historical_gre,
            entry_time=entry_time,
            max_age_seconds=max_gre_join_age_seconds,
            historical_gre_provider_window=historical_gre_index.provider_window(),
        )
        market_context = market_context_index.latest_at_or_before(entry_time)
        market_context_age = _age_seconds(
            market_context.get("vix_observation_time") if market_context else None,
            entry_time,
        )
        crfd_validity = build_context_validity(
            provider="canonical_research_feature_dataset",
            provider_timestamp=crfd.get("observation_time") if crfd else None,
            observation_timestamp=crfd.get("observation_time") if crfd else None,
            join_timestamp=entry_time,
            age_seconds=crfd_age,
            freshness_window_seconds=max_crfd_join_age_seconds,
            provenance=crfd.get("provider_kind") or crfd.get("provider_id") if crfd else None,
        )
        gre_validity = build_context_validity(
            provider=gre_context.get("provider"),
            provider_timestamp=gre_context.get("timestamp"),
            observation_timestamp=gre_context.get("timestamp"),
            join_timestamp=entry_time,
            age_seconds=gre_context.get("staleness"),
            freshness_window_seconds=max_gre_join_age_seconds,
            provenance=gre_context.get("provenance"),
            provider_window_start=gre_context.get("provider_window_start"),
            provider_window_end=gre_context.get("provider_window_end"),
            provider_window_end_is_hard_boundary=gre_context.get("provider_window_end_is_hard_boundary") is True,
        )
        market_context_validity = build_context_validity(
            provider=market_context.get("provider_name") if market_context else None,
            provider_timestamp=market_context.get("vix_observation_time") if market_context else None,
            observation_timestamp=market_context.get("vix_observation_time") if market_context else None,
            join_timestamp=entry_time,
            age_seconds=market_context_age,
            freshness_window_seconds=DEFAULT_MAX_MARKET_CONTEXT_JOIN_AGE_SECONDS,
            provenance=market_context.get("vix_source") if market_context else None,
        )
        flags = _enrichment_flags(
            outcome=outcome,
            crfd=crfd,
            gre_context=gre_context,
            crfd_age=crfd_age,
            market_context=market_context,
            gre_validity=gre_validity,
            crfd_validity=crfd_validity,
            market_context_validity=market_context_validity,
        )
        enrichments.append(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": generated_at.isoformat(),
                "trade_outcome_id": outcome.get("trade_outcome_id"),
                "strategy_id": outcome.get("strategy_id"),
                "lane_id": outcome.get("lane_id"),
                "instrument": outcome.get("instrument"),
                "contract": outcome.get("contract"),
                "side": outcome.get("side"),
                "entry_time": outcome.get("entry_time"),
                "exit_time": outcome.get("exit_time"),
                "session": _first_non_null(
                    outcome.get("session_at_entry"),
                    crfd.get("session") if crfd else None,
                    crfd.get("session_label") if crfd else None,
                ),
                "crfd_join_success": crfd is not None,
                "crfd_observation_time": crfd.get("observation_time") if crfd else None,
                "crfd_join_age_seconds": crfd_age if crfd is not None else None,
                "crfd_freshness_window_seconds": crfd_validity["freshness_window_seconds"],
                "crfd_validity_classification": crfd_validity["validity_classification"],
                "crfd_context_validity": crfd_validity,
                "research_provider_used": crfd.get("provider_id") or crfd.get("provider_kind") if crfd else None,
                "gre_label": gre_context.get("label"),
                "gre_confidence": gre_context.get("confidence"),
                "gre_provenance": gre_context.get("provenance"),
                "gre_provider": gre_context.get("provider"),
                "gre_provider_version": gre_context.get("provider_version"),
                "gre_timestamp": gre_context.get("timestamp"),
                "gre_join_method": gre_context.get("join_method"),
                "gre_staleness": gre_context.get("staleness"),
                "gre_freshness_window_seconds": gre_validity["freshness_window_seconds"],
                "gre_validity_classification": gre_validity["validity_classification"],
                "gre_context_validity": gre_validity,
                "gre_research_readiness": gre_context.get("research_readiness"),
                "gre_source_refs": gre_context.get("source_refs"),
                "vwap_relation": crfd.get("vwap_relation") if crfd else None,
                "vwap": crfd.get("vwap") if crfd else None,
                "distance_from_vwap_points": _first_non_null(
                    crfd.get("distance_from_vwap_points") if crfd else None,
                    crfd.get("distance_from_vwap") if crfd else None,
                ),
                "avwap_relation": crfd.get("avwap_relation_globex_session_open_18et") if crfd else None,
                "avwap_anchor": "globex_session_open_18et" if crfd else None,
                "avwap": crfd.get("avwap_globex_session_open_18et") if crfd else None,
                "market_context_join_success": market_context is not None,
                "vix_level": market_context.get("vix_level") if market_context else None,
                "vix_regime": market_context.get("vix_regime") if market_context else None,
                "vix_daily_change": market_context.get("vix_daily_change") if market_context else None,
                "vix_percentile": market_context.get("vix_percentile") if market_context else None,
                "vix_ma20": market_context.get("vix_ma_20") if market_context else None,
                "vix_ma50": market_context.get("vix_ma_50") if market_context else None,
                "market_context_source": market_context.get("vix_source") if market_context else None,
                "market_context_provider": market_context.get("provider_name") if market_context else None,
                "market_context_timestamp": market_context.get("vix_observation_time") if market_context else None,
                "market_context_staleness": market_context_age if market_context is not None else None,
                "market_context_freshness_window_seconds": market_context_validity["freshness_window_seconds"],
                "market_context_validity_classification": market_context_validity["validity_classification"],
                "market_context_validity": market_context_validity,
                "market_context_provenance": market_context.get("source_provenance") if market_context else None,
                "data_quality_flags": sorted(set(flags)),
                "source_refs": {
                    "canonical_trade_outcomes": str((source_paths or {}).get("canonical_trade_outcomes", "")),
                    "crfd_rows": str((source_paths or {}).get("crfd_rows", "")) if crfd else None,
                    "gre_report": str((source_paths or {}).get("gre_report", "")) if gre_context.get("provenance") else None,
                    "historical_gre_rows": str((source_paths or {}).get("historical_gre_rows", "")) if gre_context.get("provenance") == "historical_gre_backfill" else None,
                    "market_context_rows": str((source_paths or {}).get("market_context_rows", "")) if market_context else None,
                    "source_outcome_refs": outcome.get("source_refs"),
                },
                "diagnostic_only": True,
            }
        )
    return enrichments


def build_trade_outcome_enrichment_summary(
    enrichments: Sequence[Mapping[str, Any]],
    *,
    outcome_count: int,
    crfd_row_count: int,
    gre_report: Mapping[str, Any] | None,
    historical_gre_row_count: int = 0,
    market_context_row_count: int = 0,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    flags = _counts(flag for row in enrichments for flag in row.get("data_quality_flags", ()))
    gre_count = sum(1 for row in enrichments if row.get("gre_label") is not None)
    historical_gre_count = sum(1 for row in enrichments if row.get("gre_provenance") == "historical_gre_backfill")
    crfd_count = sum(1 for row in enrichments if row.get("crfd_join_success") is True)
    vwap_count = sum(1 for row in enrichments if row.get("vwap_relation") not in (None, "unavailable"))
    avwap_count = sum(1 for row in enrichments if row.get("avwap_relation") not in (None, "unavailable"))
    market_context_count = sum(1 for row in enrichments if row.get("market_context_join_success") is True)
    vix_count = sum(1 for row in enrichments if row.get("vix_level") is not None)
    staleness_values = [
        float(row["market_context_staleness"])
        for row in enrichments
        if row.get("market_context_staleness") is not None
    ]
    market_context_timestamps = [
        str(row.get("market_context_timestamp"))
        for row in enrichments
        if row.get("market_context_timestamp")
    ]
    gre_staleness_values = [
        float(row["gre_staleness"])
        for row in enrichments
        if row.get("gre_staleness") is not None
    ]
    gre_timestamps = [
        str(row.get("gre_timestamp"))
        for row in enrichments
        if row.get("gre_timestamp")
    ]
    validity_summary = _context_validity_summary(enrichments)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "analytics_only": True,
        "diagnostic_only": True,
        "production_effect": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "input_counts": {
            "outcome_count": outcome_count,
            "crfd_row_count": crfd_row_count,
            "historical_gre_row_count": historical_gre_row_count,
            "market_context_row_count": market_context_row_count,
            "gre_report_present": bool(gre_report),
        },
        "overall": {
            "enrichment_count": len(enrichments),
            "gre_coverage": _rate(gre_count, len(enrichments)),
            "historical_gre_coverage": _rate(historical_gre_count, len(enrichments)),
            "crfd_coverage": _rate(crfd_count, len(enrichments)),
            "vwap_coverage": _rate(vwap_count, len(enrichments)),
            "avwap_coverage": _rate(avwap_count, len(enrichments)),
            "vix_coverage": _rate(vix_count, len(enrichments)),
            "market_context_coverage": _rate(market_context_count, len(enrichments)),
            "join_success": _rate(crfd_count, len(enrichments)),
        },
        "coverage_counts": {
            "gre": gre_count,
            "historical_gre": historical_gre_count,
            "crfd": crfd_count,
            "vwap": vwap_count,
            "avwap": avwap_count,
            "market_context": market_context_count,
            "vix": vix_count,
        },
        "market_context": {
            "provider": "VIX",
            "successful_joins": market_context_count,
            "failed_joins": max(len(enrichments) - market_context_count, 0),
            "vix_coverage": _rate(vix_count, len(enrichments)),
            "coverage_window": {
                "start": min(market_context_timestamps) if market_context_timestamps else None,
                "end": max(market_context_timestamps) if market_context_timestamps else None,
            },
            "join_quality": _join_quality_stats(staleness_values),
        },
        "historical_gre": {
            "provider": "historical_gre_backfill",
            "successful_joins": historical_gre_count,
            "failed_joins": max(len(enrichments) - historical_gre_count, 0),
            "gre_coverage": _rate(historical_gre_count, len(enrichments)),
            "coverage_window": {
                "start": min(gre_timestamps) if gre_timestamps else None,
                "end": max(gre_timestamps) if gre_timestamps else None,
            },
            "join_quality": _join_quality_stats(gre_staleness_values),
        },
        "context_validity": validity_summary,
        "missing_reasons": flags,
        "top_enrichment_limitations": _top_limitations(flags),
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "db_mutation": False,
            "market_context_provider_changes": False,
        },
    }


def render_enrichment_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall") or {}
    counts = summary.get("coverage_counts") or {}
    return "\n".join(
        [
            "# Trade Outcome Enrichment Summary",
            "",
            f"- Generated at: {summary.get('generated_at')}",
            f"- Enrichments: {overall.get('enrichment_count')}",
            f"- GRE coverage: {overall.get('gre_coverage')} ({counts.get('gre')})",
            f"- Historical GRE coverage: {overall.get('historical_gre_coverage')} ({counts.get('historical_gre')})",
            f"- CRFD coverage: {overall.get('crfd_coverage')} ({counts.get('crfd')})",
            f"- VWAP coverage: {overall.get('vwap_coverage')} ({counts.get('vwap')})",
            f"- AVWAP coverage: {overall.get('avwap_coverage')} ({counts.get('avwap')})",
            f"- VIX coverage: {overall.get('vix_coverage')} ({counts.get('vix')})",
            f"- Market context coverage: {overall.get('market_context_coverage')} ({counts.get('market_context')})",
            "",
        ]
    )


def render_enrichment_contract_markdown() -> str:
    return "\n".join(
        [
            "# Trade Outcome Enrichment Contract",
            "",
            "The enrichment layer decorates canonical trade outcomes with research context without modifying the outcome schema.",
            "",
            "## Rules",
            "- Preserve one enrichment row per canonical trade outcome.",
            "- Do not fabricate GRE, CRFD, VWAP, AVWAP, or session values.",
            "- Do not fabricate market context values; missing CMC joins are null plus flags.",
            "- Market context joins use nearest-prior observations and never future observations.",
            "- Missing research context is represented as null plus explicit data-quality flags.",
            "- Enrichment rows are diagnostic-only and have no broker, runtime, strategy, or gate authority.",
            "- Context validity classifications are informational only and never suppress, block, submit, cancel, or resize trades.",
            "",
        ]
    )


def build_context_validity_contract(generated_at: datetime) -> dict[str, Any]:
    return {
        "schema_version": CONTEXT_VALIDITY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "purpose": "Provider-independent context freshness and availability classification for analytics enrichment.",
        "diagnostic_only": True,
        "production_effect": False,
        "trading_gate": False,
        "validity_classifications": {
            VALIDITY_VALID: "Context exists and age at join is within its configured freshness window.",
            VALIDITY_STALE: "Context exists but age at join exceeds its configured freshness window.",
            VALIDITY_OUTSIDE_PROVIDER_WINDOW: "Join timestamp is beyond a provider's hard coverage window.",
            VALIDITY_UNAVAILABLE: "No usable provider context was available for the join.",
        },
        "provider_independent_fields": [
            "provider",
            "provider_timestamp",
            "observation_timestamp",
            "join_timestamp",
            "age_seconds",
            "freshness_window_seconds",
            "validity_classification",
            "provenance",
            "provider_window_start",
            "provider_window_end",
        ],
        "provider_examples": {
            "historical_gre": {
                "provider_type": "regime_context",
                "classification_scope": "nearest-prior historical GRE row at trade entry",
                "freshness_window_seconds": DEFAULT_MAX_GRE_JOIN_AGE_SECONDS,
            },
            "canonical_market_context_vix": {
                "provider_type": "market_context",
                "classification_scope": "nearest-prior VIX context row at trade entry",
                "freshness_window_seconds": DEFAULT_MAX_MARKET_CONTEXT_JOIN_AGE_SECONDS,
            },
            "future_treasury_regime_engine": {
                "provider_type": "future_regime_context",
                "classification_scope": "same fields; no provider-specific trade decision semantics",
            },
            "future_equity_regime_engine": {
                "provider_type": "future_regime_context",
                "classification_scope": "same fields; no provider-specific trade decision semantics",
            },
        },
        "non_authority_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "trade_suppression": False,
        },
    }


def render_context_validity_contract_markdown(contract: Mapping[str, Any]) -> str:
    classes = contract.get("validity_classifications") or {}
    lines = [
        "# Canonical Context Validity Contract",
        "",
        "This contract classifies contextual joins for analytics. It does not encode trading permission, suppression, sizing, or exit behavior.",
        "",
        "## Classifications",
        "",
    ]
    for name in (VALIDITY_VALID, VALIDITY_STALE, VALIDITY_OUTSIDE_PROVIDER_WINDOW, VALIDITY_UNAVAILABLE):
        lines.append(f"- {name}: {classes.get(name)}")
    lines.extend(
        [
            "",
            "## Provider Independence",
            "",
            "Historical GRE, Canonical Market Context/VIX, future TRE, and future ERE can all expose the same timestamp, age, freshness, validity, and provenance fields.",
            "",
            "## Non-Authority",
            "",
            "Validity rows are diagnostic-only and have no broker, runtime, strategy, Managed Exit, or trading-gate authority.",
            "",
        ]
    )
    return "\n".join(lines)


def render_context_validity_report_markdown(summary: Mapping[str, Any]) -> str:
    validity = summary.get("context_validity") or {}
    lines = ["# Context Validity Enrichment Report", ""]
    for provider in ("gre", "crfd", "market_context"):
        item = validity.get(provider) or {}
        lines.append(f"## {provider}")
        lines.append("")
        lines.append(f"- Configured freshness window seconds: {item.get('freshness_window_seconds')}")
        for classification, count in (item.get("classification_counts") or {}).items():
            lines.append(f"- {classification}: {count}")
        lines.append("")
    lines.append("Classifications are informational only and do not change enrichment values or trading behavior.")
    lines.append("")
    return "\n".join(lines)


def render_context_validity_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    validity = summary.get("context_validity") or {}
    lines = ["# Context Validity Data Quality", ""]
    for provider, item in validity.items():
        counts = item.get("classification_counts") or {}
        problematic = {
            key: counts.get(key, 0)
            for key in (VALIDITY_STALE, VALIDITY_OUTSIDE_PROVIDER_WINDOW, VALIDITY_UNAVAILABLE)
            if counts.get(key, 0)
        }
        lines.append(f"- {provider}: {problematic or {'no_validity_warnings': 0}}")
    lines.extend(["", "No trade suppression or production gate is attached to these classifications.", ""])
    return "\n".join(lines)


def render_provider_validity_matrix_markdown(summary: Mapping[str, Any]) -> str:
    validity = summary.get("context_validity") or {}
    lines = [
        "# Provider Validity Matrix",
        "",
        "| Provider | VALID | STALE | OUTSIDE_PROVIDER_WINDOW | UNAVAILABLE |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for provider in ("gre", "crfd", "market_context"):
        counts = (validity.get(provider) or {}).get("classification_counts") or {}
        lines.append(
            f"| {provider} | {counts.get(VALIDITY_VALID, 0)} | {counts.get(VALIDITY_STALE, 0)} | "
            f"{counts.get(VALIDITY_OUTSIDE_PROVIDER_WINDOW, 0)} | {counts.get(VALIDITY_UNAVAILABLE, 0)} |"
        )
    lines.extend(
        [
            "",
            "Future TRE/ERE providers should emit the same validity fields and can be added to this matrix without trading semantics.",
            "",
        ]
    )
    return "\n".join(lines)


def render_enrichment_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    lines = ["# Trade Outcome Enrichment Data Quality", "", "## Top Limitations", ""]
    for item in summary.get("top_enrichment_limitations") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## Missing Reasons", ""])
    for reason, count in (summary.get("missing_reasons") or {}).items():
        lines.append(f"- {reason}: {count}")
    lines.append("")
    return "\n".join(lines)


def render_market_context_report_markdown(summary: Mapping[str, Any]) -> str:
    context = summary.get("market_context") or {}
    coverage = context.get("coverage_window") or {}
    return "\n".join(
        [
            "# Market Context Enrichment Report",
            "",
            f"- Generated at: {summary.get('generated_at')}",
            f"- Provider: {context.get('provider')}",
            f"- Enrichment count: {(summary.get('overall') or {}).get('enrichment_count')}",
            f"- VIX coverage: {context.get('vix_coverage')}",
            f"- Successful joins: {context.get('successful_joins')}",
            f"- Failed joins: {context.get('failed_joins')}",
            f"- Coverage start: {coverage.get('start')}",
            f"- Coverage end: {coverage.get('end')}",
            "",
            "Market context is diagnostic-only and has no production effect.",
            "",
        ]
    )


def render_market_context_join_quality_markdown(summary: Mapping[str, Any]) -> str:
    context = summary.get("market_context") or {}
    stats = context.get("join_quality") or {}
    return "\n".join(
        [
            "# Market Context Join Quality",
            "",
            f"- Successful joins: {context.get('successful_joins')}",
            f"- Failed joins: {context.get('failed_joins')}",
            f"- Minimum staleness seconds: {stats.get('min_staleness_seconds')}",
            f"- Maximum staleness seconds: {stats.get('max_staleness_seconds')}",
            f"- Average staleness seconds: {stats.get('avg_staleness_seconds')}",
            f"- Median staleness seconds: {stats.get('median_staleness_seconds')}",
            "",
            "Join rule: nearest prior CMC observation at or before the trade entry timestamp.",
            "",
        ]
    )


def render_market_context_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    flags = summary.get("missing_reasons") or {}
    lines = ["# Trade Outcome Market Context Data Quality", ""]
    if flags.get("missing_market_context"):
        lines.append(f"- missing_market_context: {flags.get('missing_market_context')}")
    else:
        lines.append("- No missing market context joins.")
    if flags.get("missing_vix_context"):
        lines.append(f"- missing_vix_context: {flags.get('missing_vix_context')}")
    else:
        lines.append("- No missing VIX context values.")
    lines.extend(
        [
            "",
            "Remaining limitation: VIX context is historical daily cash-index context, not live intraday VIX.",
            "",
        ]
    )
    return "\n".join(lines)


def render_historical_gre_report_markdown(summary: Mapping[str, Any]) -> str:
    gre = summary.get("historical_gre") or {}
    coverage = gre.get("coverage_window") or {}
    return "\n".join(
        [
            "# Historical GRE Trade Enrichment Report",
            "",
            f"- Generated at: {summary.get('generated_at')}",
            f"- Provider: {gre.get('provider')}",
            f"- Enrichment count: {(summary.get('overall') or {}).get('enrichment_count')}",
            f"- Historical GRE coverage: {gre.get('gre_coverage')}",
            f"- Successful joins: {gre.get('successful_joins')}",
            f"- Failed joins: {gre.get('failed_joins')}",
            f"- Coverage start: {coverage.get('start')}",
            f"- Coverage end: {coverage.get('end')}",
            "",
            "Historical GRE context is diagnostic-only and sourced from the historical GRE backfill corpus.",
            "",
        ]
    )


def render_historical_gre_join_quality_markdown(summary: Mapping[str, Any]) -> str:
    gre = summary.get("historical_gre") or {}
    stats = gre.get("join_quality") or {}
    return "\n".join(
        [
            "# Historical GRE Join Quality",
            "",
            f"- Successful joins: {gre.get('successful_joins')}",
            f"- Failed joins: {gre.get('failed_joins')}",
            f"- Minimum staleness seconds: {stats.get('min_staleness_seconds')}",
            f"- Maximum staleness seconds: {stats.get('max_staleness_seconds')}",
            f"- Average staleness seconds: {stats.get('avg_staleness_seconds')}",
            f"- Median staleness seconds: {stats.get('median_staleness_seconds')}",
            "",
            "Join rule: nearest prior historical GRE observation at or before trade entry timestamp.",
            "",
        ]
    )


def render_historical_gre_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    flags = summary.get("missing_reasons") or {}
    lines = ["# Historical GRE Trade Data Quality", ""]
    if flags.get("missing_gre_context"):
        lines.append(f"- missing_gre_context: {flags.get('missing_gre_context')}")
    else:
        lines.append("- No missing GRE context values.")
    lines.extend(
        [
            "",
            "Remaining limitation: historical GRE coverage depends on the R15 backfill window and only joins trades at or after the first historical GRE observation.",
            "",
        ]
    )
    return "\n".join(lines)


def _select_gre_context(
    outcome: Mapping[str, Any],
    *,
    crfd: Mapping[str, Any] | None,
    gre_report: Mapping[str, Any],
    historical_gre: Mapping[str, Any] | None = None,
    entry_time: datetime | None,
    max_age_seconds: int,
    historical_gre_provider_window: Mapping[str, str | None] | None = None,
) -> dict[str, Any]:
    if outcome.get("gre_label_at_entry") is not None:
        return {
            "label": outcome.get("gre_label_at_entry"),
            "confidence": outcome.get("gre_confidence_at_entry"),
            "provenance": "canonical_trade_outcome",
            "provider": "canonical_trade_outcome",
            "provider_version": outcome.get("schema_version"),
            "timestamp": outcome.get("entry_time"),
            "join_method": "embedded_at_entry",
            "staleness": 0.0,
            "research_readiness": None,
            "source_refs": outcome.get("source_refs"),
            "provider_window_start": None,
            "provider_window_end": None,
            "provider_window_end_is_hard_boundary": False,
        }
    if historical_gre:
        gre_time = _parse_datetime(historical_gre.get("gre_generated_at") or historical_gre.get("classification_candle_max_ts"))
        provider_metadata = historical_gre.get("provider_metadata") or {}
        provider_window = historical_gre_provider_window or {}
        age = _age_seconds(gre_time, entry_time)
        return {
            "label": historical_gre.get("regime_label"),
            "confidence": historical_gre.get("confidence"),
            "provenance": "historical_gre_backfill",
            "provider": provider_metadata.get("provider_kind") or historical_gre.get("source_mode") or "historical_gre_backfill",
            "provider_version": historical_gre.get("schema_version"),
            "timestamp": (gre_time.isoformat() if gre_time else historical_gre.get("gre_generated_at")),
            "join_method": "nearest_prior_historical_gre_observation",
            "staleness": age,
            "research_readiness": historical_gre.get("validation_status"),
            "source_refs": historical_gre.get("source_refs"),
            "provider_window_start": provider_window.get("start"),
            "provider_window_end": provider_window.get("end"),
            "provider_window_end_is_hard_boundary": True,
        }
    if crfd and crfd.get("gre_label") is not None:
        return {
            "label": crfd.get("gre_label"),
            "confidence": crfd.get("gre_confidence"),
            "provenance": "canonical_research_feature_dataset",
            "provider": crfd.get("provider_kind") or crfd.get("provider_id"),
            "provider_version": crfd.get("schema_version"),
            "timestamp": crfd.get("observation_time"),
            "join_method": "crfd_embedded_gre_context",
            "staleness": _age_seconds(crfd.get("observation_time"), entry_time),
            "research_readiness": None,
            "source_refs": crfd.get("source_refs"),
            "provider_window_start": None,
            "provider_window_end": None,
            "provider_window_end_is_hard_boundary": False,
        }
    if not gre_report or not _is_gold_instrument(outcome.get("instrument") or outcome.get("contract")):
        return _missing_gre_context()
    gre_time = _parse_datetime(gre_report.get("generated_at"))
    age = abs((gre_time - entry_time).total_seconds()) if gre_time and entry_time else None
    if age is not None and age <= max_age_seconds:
        return {
            "label": gre_report.get("regime_label"),
            "confidence": gre_report.get("confidence"),
            "provenance": "latest_gold_regime_engine_time_aligned",
            "provider": "latest_gold_regime_engine",
            "provider_version": gre_report.get("schema_version"),
            "timestamp": gre_report.get("generated_at"),
            "join_method": "latest_gre_time_aligned",
            "staleness": age,
            "research_readiness": None,
            "source_refs": gre_report.get("source_refs"),
            "provider_window_start": None,
            "provider_window_end": None,
            "provider_window_end_is_hard_boundary": False,
        }
    return _missing_gre_context()


def _enrichment_flags(
    *,
    outcome: Mapping[str, Any],
    crfd: Mapping[str, Any] | None,
    gre_context: Mapping[str, Any],
    crfd_age: float | None,
    market_context: Mapping[str, Any] | None = None,
    gre_validity: Mapping[str, Any] | None = None,
    crfd_validity: Mapping[str, Any] | None = None,
    market_context_validity: Mapping[str, Any] | None = None,
) -> list[str]:
    flags = list(outcome.get("data_quality_flags") or ())
    if crfd is None:
        flags.append("missing_crfd_context")
    elif crfd_age is not None and crfd_age > DEFAULT_MAX_CRFD_JOIN_AGE_SECONDS:
        flags.append("stale_crfd_context")
    if gre_context.get("label") is None:
        flags.append("missing_gre_context")
    elif (gre_validity or {}).get("validity_classification") == VALIDITY_STALE:
        flags.append("stale_gre_context")
    elif (gre_validity or {}).get("validity_classification") == VALIDITY_OUTSIDE_PROVIDER_WINDOW:
        flags.append("outside_provider_window_gre_context")
    if (crfd_validity or {}).get("validity_classification") == VALIDITY_STALE:
        flags.append("stale_crfd_context")
    if (market_context_validity or {}).get("validity_classification") == VALIDITY_STALE:
        flags.append("stale_market_context")
    if crfd is None or crfd.get("vwap_relation") in (None, "unavailable"):
        flags.append("missing_vwap_context")
    if crfd is None or crfd.get("avwap_relation_globex_session_open_18et") in (None, "unavailable"):
        flags.append("missing_avwap_context")
    if market_context is None:
        flags.append("missing_market_context")
        flags.append("missing_vix_context")
    elif market_context.get("vix_level") is None:
        flags.append("missing_vix_context")
    return flags


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
        rows = self._rows.get(_root_symbol(contract), ()) or self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


class _HistoricalGreIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        values: list[tuple[datetime, Mapping[str, Any]]] = []
        for row in rows:
            if row.get("diagnostic_only") is False:
                continue
            ts = _parse_datetime(row.get("gre_generated_at") or row.get("classification_candle_max_ts"))
            if ts is None:
                continue
            values.append((ts, row))
        self._rows = sorted(values, key=lambda item: item[0])
        self._window_start = self._rows[0][0] if self._rows else None
        self._window_end = self._rows[-1][0] if self._rows else None

    def latest_at_or_before(self, *, outcome: Mapping[str, Any], timestamp: datetime | None) -> Mapping[str, Any] | None:
        if timestamp is None or not _is_gold_instrument(outcome.get("instrument") or outcome.get("contract")):
            return None
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in self._rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate

    def provider_window(self) -> dict[str, str | None]:
        return {
            "start": None if self._window_start is None else self._window_start.isoformat(),
            "end": None if self._window_end is None else self._window_end.isoformat(),
        }


class _MarketContextIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        values: list[tuple[datetime, Mapping[str, Any]]] = []
        for row in rows:
            if row.get("context_key") != "vix" or row.get("vix_available") is False:
                continue
            ts = _parse_datetime(row.get("vix_observation_time"))
            if ts is None:
                continue
            values.append((ts, row))
        self._rows = sorted(values, key=lambda item: item[0])

    def latest_at_or_before(self, timestamp: datetime | None) -> Mapping[str, Any] | None:
        if timestamp is None:
            return None
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in self._rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _top_limitations(flags: Mapping[str, int]) -> list[str]:
    mapping = {
        "missing_gre_context": "GRE context is unavailable for most historical outcomes.",
        "missing_crfd_context": "CRFD context is unavailable or stale for some outcomes.",
        "missing_vwap_context": "VWAP context is missing where CRFD did not join or VWAP was unavailable.",
        "missing_avwap_context": "AVWAP context is missing where anchors were unavailable.",
        "missing_market_context": "Canonical Market Context is unavailable for some outcomes.",
        "missing_vix_context": "VIX context is unavailable for some outcomes.",
    }
    return [mapping[key] for key in mapping if flags.get(key)]


def _missing_gre_context() -> dict[str, Any]:
    return {
        "label": None,
        "confidence": None,
        "provenance": None,
        "provider": None,
        "provider_version": None,
        "timestamp": None,
        "join_method": None,
        "staleness": None,
        "research_readiness": None,
        "source_refs": None,
        "provider_window_start": None,
        "provider_window_end": None,
        "provider_window_end_is_hard_boundary": False,
    }


def build_context_validity(
    *,
    provider: Any,
    provider_timestamp: Any,
    observation_timestamp: Any,
    join_timestamp: Any,
    age_seconds: Any,
    freshness_window_seconds: int | None,
    provenance: Any,
    provider_window_start: Any = None,
    provider_window_end: Any = None,
    provider_window_end_is_hard_boundary: bool = False,
) -> dict[str, Any]:
    join_time = _parse_datetime(join_timestamp)
    provider_time = _parse_datetime(provider_timestamp)
    window_start = _parse_datetime(provider_window_start)
    window_end = _parse_datetime(provider_window_end)
    age = _optional_float(age_seconds)
    classification = VALIDITY_VALID
    if provider_time is None:
        classification = VALIDITY_UNAVAILABLE
    elif window_start is not None and join_time is not None and join_time < window_start:
        classification = VALIDITY_OUTSIDE_PROVIDER_WINDOW
    elif provider_window_end_is_hard_boundary and window_end is not None and join_time is not None and join_time > window_end:
        classification = VALIDITY_OUTSIDE_PROVIDER_WINDOW
    elif freshness_window_seconds is not None and age is not None and age > freshness_window_seconds:
        classification = VALIDITY_STALE
    return {
        "schema_version": CONTEXT_VALIDITY_SCHEMA_VERSION,
        "provider": provider,
        "provider_timestamp": None if provider_time is None else provider_time.isoformat(),
        "observation_timestamp": _iso_datetime_or_none(observation_timestamp),
        "join_timestamp": None if join_time is None else join_time.isoformat(),
        "age_seconds": age,
        "freshness_window_seconds": freshness_window_seconds,
        "validity_classification": classification,
        "provenance": provenance,
        "provider_window_start": None if window_start is None else window_start.isoformat(),
        "provider_window_end": None if window_end is None else window_end.isoformat(),
        "provider_window_end_is_hard_boundary": provider_window_end_is_hard_boundary,
        "diagnostic_only": True,
        "production_effect": False,
        "trading_gate": False,
    }


def _context_validity_summary(enrichments: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    specs = {
        "gre": ("gre_validity_classification", "gre_freshness_window_seconds"),
        "crfd": ("crfd_validity_classification", "crfd_freshness_window_seconds"),
        "market_context": ("market_context_validity_classification", "market_context_freshness_window_seconds"),
    }
    for provider, (classification_key, window_key) in specs.items():
        classifications = [str(row.get(classification_key) or VALIDITY_UNAVAILABLE) for row in enrichments]
        windows = [row.get(window_key) for row in enrichments if row.get(window_key) is not None]
        result[provider] = {
            "classification_counts": _counts(classifications),
            "freshness_window_seconds": windows[0] if windows else None,
            "diagnostic_only": True,
            "production_effect": False,
            "trading_gate": False,
        }
    return result


def _join_quality_stats(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "count": 0,
            "min_staleness_seconds": None,
            "max_staleness_seconds": None,
            "avg_staleness_seconds": None,
            "median_staleness_seconds": None,
        }
    ordered = sorted(float(value) for value in values)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    return {
        "count": len(ordered),
        "min_staleness_seconds": round(ordered[0], 6),
        "max_staleness_seconds": round(ordered[-1], 6),
        "avg_staleness_seconds": round(sum(ordered) / len(ordered), 6),
        "median_staleness_seconds": round(median, 6),
    }


def _age_seconds(source_time: Any, target_time: datetime | None) -> float | None:
    source = _parse_datetime(source_time)
    if source is None or target_time is None:
        return None
    return max((target_time - source).total_seconds(), 0.0)


def _is_gold_instrument(value: Any) -> bool:
    return _root_symbol(value) in {"GC", "MGC"}


def _root_symbol(value: Any) -> str:
    root = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
    if root.startswith("MGC"):
        return "MGC"
    if root.startswith("GC"):
        return "GC"
    if root.startswith("MNQ"):
        return "MNQ"
    if root.startswith("NQ"):
        return "NQ"
    if root.startswith("MES"):
        return "MES"
    if root.startswith("ES"):
        return "ES"
    return root


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso_datetime_or_none(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return None if parsed is None else parsed.isoformat()


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 6)


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


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
