"""Storage and shard/stitch contracts for the warehouse evaluator."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnContract:
    name: str
    logical_type: str
    nullable: bool = True
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DatasetContract:
    dataset_name: str
    truth_class: str
    partitioning: tuple[str, ...]
    description: str
    columns: tuple[ColumnContract, ...]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["columns"] = [column.to_dict() for column in self.columns]
        return payload


@dataclass(frozen=True)
class CatalogTableContract:
    table_name: str
    description: str
    columns: tuple[ColumnContract, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["columns"] = [column.to_dict() for column in self.columns]
        return payload


RAW_1M_COLUMNS = (
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("bar_ts", "timestamp_tz", False, "Completed 1m bar end timestamp."),
    ColumnContract("open", "double", False, "Open price."),
    ColumnContract("high", "double", False, "High price."),
    ColumnContract("low", "double", False, "Low price."),
    ColumnContract("close", "double", False, "Close price."),
    ColumnContract("volume", "bigint", False, "Observed bar volume."),
    ColumnContract("provider", "string", False, "Market-data provider."),
    ColumnContract("dataset", "string", True, "Provider dataset."),
    ColumnContract("schema", "string", True, "Provider schema."),
    ColumnContract("instrument_identity", "string", False, "Raw instrument identity."),
    ColumnContract("data_source", "string", False, "Canonical source tag."),
    ColumnContract("ingest_ts", "timestamp_tz", False, "Ingest timestamp."),
    ColumnContract("coverage_window_start", "timestamp_tz", True, "Requested coverage start."),
    ColumnContract("coverage_window_end", "timestamp_tz", True, "Requested coverage end."),
    ColumnContract("provenance_tag", "string", False, "Persistent provenance tag."),
)

DERIVED_BAR_COLUMNS = (
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("timeframe", "string", False, "Derived timeframe label."),
    ColumnContract("bar_ts", "timestamp_tz", False, "Completed bucket end timestamp."),
    ColumnContract("open", "double", False, "Open price."),
    ColumnContract("high", "double", False, "High price."),
    ColumnContract("low", "double", False, "Low price."),
    ColumnContract("close", "double", False, "Close price."),
    ColumnContract("volume", "bigint", False, "Aggregated bucket volume."),
    ColumnContract("source_data_source", "string", False, "Upstream canonical source."),
    ColumnContract("derived_rule", "string", False, "Deterministic aggregation rule."),
    ColumnContract("materialized_from_raw_version", "string", False, "Raw coverage version fingerprint."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("provenance_tag", "string", False, "Derived provenance tag."),
)

SHARED_FEATURE_5M_COLUMNS = (
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("decision_ts", "timestamp_tz", False, "Structural decision timestamp."),
    ColumnContract("bar_id", "string", False, "Structural bar id."),
    ColumnContract("timeframe", "string", False, "Feature timeframe."),
    ColumnContract("session_phase", "string", True, "Session phase label."),
    ColumnContract("atr", "double", False, "ATR."),
    ColumnContract("bar_range", "double", False, "Bar range."),
    ColumnContract("body_size", "double", False, "Body size."),
    ColumnContract("vol_ratio", "double", False, "Relative volume ratio."),
    ColumnContract("turn_ema_fast", "double", False, "Fast EMA."),
    ColumnContract("turn_ema_slow", "double", False, "Slow EMA."),
    ColumnContract("velocity", "double", False, "EMA velocity."),
    ColumnContract("velocity_delta", "double", False, "EMA velocity delta."),
    ColumnContract("vwap", "double", False, "Session VWAP."),
    ColumnContract("vwap_buffer", "double", False, "VWAP buffer."),
    ColumnContract("downside_stretch", "double", False, "Downside stretch."),
    ColumnContract("upside_stretch", "double", False, "Upside stretch."),
    ColumnContract("bull_close_strong", "boolean", False, "Bull close strength."),
    ColumnContract("bear_close_weak", "boolean", False, "Bear close weakness."),
    ColumnContract("bull_snap_turn_candidate", "boolean", False, "Bull snap candidate."),
    ColumnContract("bear_snap_turn_candidate", "boolean", False, "Bear snap candidate."),
    ColumnContract("asia_reclaim_bar_raw", "boolean", False, "Asia reclaim raw."),
    ColumnContract("asia_vwap_long_signal", "boolean", False, "Asia VWAP long signal."),
    ColumnContract("us_late_pause_resume_long_turn_candidate", "boolean", False, "US late pause/resume candidate."),
    ColumnContract(
        "asia_early_normal_breakout_retest_hold_long_turn_candidate",
        "boolean",
        False,
        "Asia early breakout/retest long candidate.",
    ),
    ColumnContract("asia_early_pause_resume_short_turn_candidate", "boolean", False, "Asia early short candidate."),
    ColumnContract("derived_from_version", "string", False, "Derived layer fingerprint."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("coverage_window_start", "timestamp_tz", True, "Coverage start."),
    ColumnContract("coverage_window_end", "timestamp_tz", True, "Coverage end."),
    ColumnContract("provenance_tag", "string", False, "Derived provenance tag."),
)

SHARED_FEATURE_1M_TIMING_COLUMNS = (
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("timing_ts", "timestamp_tz", False, "Timing bar timestamp."),
    ColumnContract("bar_id", "string", False, "Timing bar id."),
    ColumnContract("timeframe", "string", False, "Timing timeframe."),
    ColumnContract("close_price", "double", False, "Close price proxy."),
    ColumnContract("bar_vwap", "double", False, "Typical-price VWAP proxy."),
    ColumnContract("bar_range_points", "double", False, "Timing bar range."),
    ColumnContract("long_close_quality", "string", False, "Long close VWAP quality."),
    ColumnContract("short_close_quality", "string", False, "Short close VWAP quality."),
    ColumnContract("long_neutral_tight_ok", "boolean", False, "Long neutral-tighter acceptance."),
    ColumnContract("short_neutral_tight_ok", "boolean", False, "Short neutral-tighter acceptance."),
    ColumnContract("materialized_from_raw_version", "string", False, "Raw coverage version fingerprint."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("coverage_window_start", "timestamp_tz", True, "Coverage start."),
    ColumnContract("coverage_window_end", "timestamp_tz", True, "Coverage end."),
    ColumnContract("provenance_tag", "string", False, "Timing provenance tag."),
)

FAMILY_EVENT_TABLE_COLUMNS = (
    ColumnContract("event_id", "string", False, "Stable family event id."),
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("family", "string", False, "Pattern family."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("candidate_ts", "timestamp_tz", False, "Candidate event timestamp."),
    ColumnContract("event_side", "string", True, "Long/short side."),
    ColumnContract("event_phase", "string", True, "Session/phase label."),
    ColumnContract("eligibility_label", "string", True, "Why the event passed."),
    ColumnContract("blocker_label", "string", True, "Why the event was blocked."),
    ColumnContract("execution_model", "string", False, "Execution-model label."),
    ColumnContract("decision_ts", "timestamp_tz", False, "Source decision timestamp."),
    ColumnContract("feature_bar_id", "string", False, "Source feature bar id."),
    ColumnContract("timing_ts", "timestamp_tz", True, "Matched timing timestamp."),
    ColumnContract("feature_refs", "json", True, "Pointers to feature rows."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("provenance_tag", "string", False, "Event-table provenance tag."),
)

LANE_CANDIDATE_COLUMNS = (
    ColumnContract("candidate_id", "string", False, "Stable lane candidate id."),
    ColumnContract("event_id", "string", False, "Source family event id."),
    ColumnContract("source_event_family", "string", False, "Shared source-family event table."),
    ColumnContract("lane_id", "string", False, "Stable lane id."),
    ColumnContract("strategy_key", "string", False, "Strategy key alias for lane id."),
    ColumnContract("family", "string", False, "Lane family."),
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("candidate_ts", "timestamp_tz", False, "Lane candidate timestamp."),
    ColumnContract("decision_ts", "timestamp_tz", False, "Structural decision timestamp."),
    ColumnContract("timing_ts", "timestamp_tz", True, "Executable timing timestamp."),
    ColumnContract("side", "string", False, "Long/short side."),
    ColumnContract("execution_model", "string", False, "Execution semantics label."),
    ColumnContract("eligibility_label", "string", True, "Eligibility pass reason."),
    ColumnContract("blocker_label", "string", True, "Blocker reason if present."),
    ColumnContract("feature_bar_id", "string", False, "Source structural feature bar id."),
    ColumnContract("timing_bar_id", "string", True, "Source 1m timing bar id."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("provenance_tag", "string", False, "Candidate provenance tag."),
)

LANE_ENTRY_COLUMNS = (
    ColumnContract("entry_id", "string", False, "Stable lane entry id."),
    ColumnContract("candidate_id", "string", False, "Upstream lane candidate id."),
    ColumnContract("event_id", "string", False, "Source family event id."),
    ColumnContract("source_event_family", "string", False, "Shared source-family event table."),
    ColumnContract("lane_id", "string", False, "Stable lane id."),
    ColumnContract("strategy_key", "string", False, "Strategy key alias for lane id."),
    ColumnContract("family", "string", False, "Lane family."),
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("entry_ts", "timestamp_tz", False, "Executable entry timestamp."),
    ColumnContract("side", "string", False, "Long/short side."),
    ColumnContract("execution_model", "string", False, "Execution semantics label."),
    ColumnContract("entry_price", "double", False, "Executable entry price."),
    ColumnContract("bar_vwap", "double", False, "1m bar VWAP proxy."),
    ColumnContract("vwap_quality", "string", False, "VWAP price-quality label."),
    ColumnContract("quality_allowed", "boolean", False, "Whether entry passed quality gating."),
    ColumnContract("hold_minutes", "bigint", False, "Configured hold horizon in minutes."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("provenance_tag", "string", False, "Entry provenance tag."),
)

LANE_CLOSED_TRADE_COLUMNS = (
    ColumnContract("trade_id", "string", False, "Stable closed-trade id."),
    ColumnContract("entry_id", "string", False, "Upstream entry id."),
    ColumnContract("candidate_id", "string", False, "Upstream candidate id."),
    ColumnContract("lane_id", "string", False, "Stable lane id."),
    ColumnContract("strategy_key", "string", False, "Strategy key alias for lane id."),
    ColumnContract("family", "string", False, "Lane family."),
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("side", "string", False, "Long/short side."),
    ColumnContract("execution_model", "string", False, "Execution semantics label."),
    ColumnContract("entry_ts", "timestamp_tz", False, "Entry timestamp."),
    ColumnContract("exit_ts", "timestamp_tz", False, "Exit timestamp."),
    ColumnContract("entry_price", "double", False, "Entry price."),
    ColumnContract("exit_price", "double", False, "Exit price."),
    ColumnContract("pnl", "double", False, "Closed-trade P&L."),
    ColumnContract("hold_minutes", "bigint", False, "Configured hold horizon."),
    ColumnContract("vwap_quality", "string", False, "Entry VWAP price quality."),
    ColumnContract("exit_reason", "string", False, "Warehouse exit policy label."),
    ColumnContract("win_flag", "boolean", False, "True when P&L is positive."),
    ColumnContract("materialized_ts", "timestamp_tz", False, "Materialization timestamp."),
    ColumnContract("provenance_tag", "string", False, "Closed-trade provenance tag."),
)

LANE_COMPACT_COLUMNS = (
    ColumnContract("lane_id", "string", False, "Stable lane id."),
    ColumnContract("strategy_key", "string", False, "Strategy key alias for lane id."),
    ColumnContract("family", "string", False, "Strategy family."),
    ColumnContract("symbol", "string", False, "Canonical root or contract symbol."),
    ColumnContract("execution_model", "string", False, "Execution semantics label."),
    ColumnContract("shard_id", "string", False, "Evaluation shard id."),
    ColumnContract("artifact_class", "string", False, "Compact or rich artifact class."),
    ColumnContract("result_classification", "string", False, "nonzero_trade / zero_trade / missing."),
    ColumnContract("trade_count", "bigint", False, "Closed trade count."),
    ColumnContract("net_pnl", "double", False, "Net P&L."),
    ColumnContract("profit_factor", "double", True, "Profit factor."),
    ColumnContract("win_rate", "double", True, "Win rate."),
    ColumnContract("winners", "bigint", False, "Winning trade count."),
    ColumnContract("losers", "bigint", False, "Losing trade count."),
    ColumnContract("canonical_input_start", "timestamp_tz", True, "Canonical raw input coverage start."),
    ColumnContract("canonical_input_end", "timestamp_tz", True, "Canonical raw input coverage end."),
    ColumnContract("emitted_compact_start", "timestamp_tz", True, "Compact output coverage start."),
    ColumnContract("emitted_compact_end", "timestamp_tz", True, "Compact output coverage end."),
    ColumnContract("closed_trade_start", "timestamp_tz", True, "Closed-trade coverage start."),
    ColumnContract("closed_trade_end", "timestamp_tz", True, "Closed-trade coverage end."),
    ColumnContract("eligibility_status", "string", False, "Eligibility result."),
    ColumnContract("zero_trade_flag", "boolean", False, "True when eligible but no closed trades."),
    ColumnContract("reference_lane", "boolean", False, "Benchmark/reference lane flag."),
    ColumnContract("bucket", "string", True, "Review bucket."),
    ColumnContract("status", "string", True, "Lane status."),
    ColumnContract("cohort", "string", True, "Lane cohort."),
)

PUBLICATION_ARTIFACT_COLUMNS = (
    ColumnContract("publication_run_id", "string", False, "Publication run id."),
    ColumnContract("strategy_id", "string", False, "Published lane id."),
    ColumnContract("artifact_type", "string", False, "json/markdown/manifest/catalog."),
    ColumnContract("artifact_path", "string", False, "Artifact path."),
    ColumnContract("execution_model", "string", False, "Execution semantics label."),
    ColumnContract("coverage_start", "timestamp_tz", True, "Published study coverage start."),
    ColumnContract("coverage_end", "timestamp_tz", True, "Published study coverage end."),
    ColumnContract("truth_provenance", "string", False, "Publication truth class."),
)

DEFAULT_DATASET_CONTRACTS: tuple[DatasetContract, ...] = (
    DatasetContract(
        dataset_name="raw_bars_1m",
        truth_class="canonical",
        partitioning=("symbol", "year", "shard_id"),
        description="Canonical 1m market-data truth mirrored into the warehouse.",
        columns=RAW_1M_COLUMNS,
        metadata={"source_data_source": "historical_1m_canonical"},
    ),
    DatasetContract(
        dataset_name="derived_bars_5m",
        truth_class="derived_from_canonical_1m",
        partitioning=("symbol", "year", "shard_id"),
        description="Deterministic 5m bars derived from canonical 1m.",
        columns=DERIVED_BAR_COLUMNS,
        metadata={"timeframe": "5m", "source_data_source": "historical_1m_canonical"},
    ),
    DatasetContract(
        dataset_name="derived_bars_10m",
        truth_class="derived_from_canonical_1m",
        partitioning=("symbol", "year", "shard_id"),
        description="Deterministic 10m bars derived from canonical 1m.",
        columns=DERIVED_BAR_COLUMNS,
        metadata={"timeframe": "10m", "source_data_source": "historical_1m_canonical"},
    ),
    DatasetContract(
        dataset_name="shared_features_5m",
        truth_class="derived_feature_layer",
        partitioning=("symbol", "year", "shard_id"),
        description="Reusable structural/context 5m features computed once per symbol/shard.",
        columns=SHARED_FEATURE_5M_COLUMNS,
        metadata={"grain": "(symbol, shard, decision_ts)", "timeframe": "5m"},
    ),
    DatasetContract(
        dataset_name="shared_features_1m_timing",
        truth_class="derived_timing_feature_layer",
        partitioning=("symbol", "year", "shard_id"),
        description="Reusable 1m executable-timing / VWAP-quality context rows.",
        columns=SHARED_FEATURE_1M_TIMING_COLUMNS,
        metadata={"grain": "(symbol, shard, timing_ts)", "timeframe": "1m"},
    ),
    DatasetContract(
        dataset_name="family_event_tables",
        truth_class="compiled_candidate_events",
        partitioning=("symbol", "year", "shard_id"),
        description="Compiled family event tables built from shared features.",
        columns=FAMILY_EVENT_TABLE_COLUMNS,
        metadata={"grain": "(symbol, shard, family, candidate_ts)"},
    ),
    DatasetContract(
        dataset_name="lane_candidates",
        truth_class="compiled_lane_candidates",
        partitioning=("symbol", "year", "shard_id"),
        description="Lane candidate rows compiled from shared family event tables.",
        columns=LANE_CANDIDATE_COLUMNS,
        metadata={"grain": "(symbol, shard, lane_id, candidate_ts)"},
    ),
    DatasetContract(
        dataset_name="lane_entries",
        truth_class="compiled_lane_entries",
        partitioning=("symbol", "year", "shard_id"),
        description="Executable lane entries compiled from lane candidates and 1m timing features.",
        columns=LANE_ENTRY_COLUMNS,
        metadata={"grain": "(symbol, shard, lane_id, entry_ts)"},
    ),
    DatasetContract(
        dataset_name="lane_closed_trades",
        truth_class="compiled_lane_closed_trades",
        partitioning=("symbol", "year", "shard_id"),
        description="Warehouse closed trades compiled from lane entries.",
        columns=LANE_CLOSED_TRADE_COLUMNS,
        metadata={"grain": "(symbol, shard, lane_id, exit_ts)"},
    ),
    DatasetContract(
        dataset_name="lane_compact_results",
        truth_class="compact_research_output",
        partitioning=("symbol", "year", "shard_id"),
        description="Compact lane-level trade/economic summaries by shard.",
        columns=LANE_COMPACT_COLUMNS,
        metadata={"artifact_mode": "compact_only"},
    ),
    DatasetContract(
        dataset_name="rich_publication_artifacts",
        truth_class="publication_output",
        partitioning=("publication_run_id", "artifact_type"),
        description="Selective rich-study publication artifacts.",
        columns=PUBLICATION_ARTIFACT_COLUMNS,
        metadata={"artifact_mode": "selective_rich"},
    ),
)

DEFAULT_DUCKDB_TABLE_CONTRACTS: tuple[CatalogTableContract, ...] = (
    CatalogTableContract(
        table_name="dataset_contracts",
        description="Registered dataset contracts and storage locations.",
        columns=(
            ColumnContract("dataset_name", "string", False),
            ColumnContract("truth_class", "string", False),
            ColumnContract("storage_path", "string", False),
            ColumnContract("schema_path", "string", False),
            ColumnContract("registered_ts", "timestamp_tz", False),
        ),
    ),
    CatalogTableContract(
        table_name="shard_contracts",
        description="Shard and stitch policy contract registry.",
        columns=(
            ColumnContract("contract_id", "string", False),
            ColumnContract("shard_unit", "string", False),
            ColumnContract("warmup_policy", "string", False),
            ColumnContract("stitch_policy", "json", False),
            ColumnContract("registered_ts", "timestamp_tz", False),
        ),
    ),
    CatalogTableContract(
        table_name="compact_run_registry",
        description="Compact evaluator run registry.",
        columns=(
            ColumnContract("run_id", "string", False),
            ColumnContract("run_ts", "timestamp_tz", False),
            ColumnContract("artifact_mode", "string", False),
            ColumnContract("window_start", "timestamp_tz", True),
            ColumnContract("window_end", "timestamp_tz", True),
            ColumnContract("notes", "string", True),
        ),
    ),
    CatalogTableContract(
        table_name="dataset_partitions",
        description="Materialized Parquet partitions and their coverage metadata.",
        columns=(
            ColumnContract("dataset_name", "string", False),
            ColumnContract("symbol", "string", True),
            ColumnContract("year", "bigint", True),
            ColumnContract("shard_id", "string", True),
            ColumnContract("timeframe", "string", True),
            ColumnContract("partition_path", "string", False),
            ColumnContract("row_count", "bigint", False),
            ColumnContract("coverage_start", "timestamp_tz", True),
            ColumnContract("coverage_end", "timestamp_tz", True),
            ColumnContract("provenance_tag", "string", True),
            ColumnContract("registered_ts", "timestamp_tz", False),
        ),
    ),
    CatalogTableContract(
        table_name="rich_publication_runs",
        description="Selective rich publication run registry.",
        columns=(
            ColumnContract("publication_run_id", "string", False),
            ColumnContract("source_compact_run_id", "string", True),
            ColumnContract("run_ts", "timestamp_tz", False),
            ColumnContract("publication_policy", "json", False),
        ),
    ),
    CatalogTableContract(
        table_name="coverage_audit",
        description="Coverage audit across raw, derived, compact, and publication layers.",
        columns=(
            ColumnContract("layer", "string", False),
            ColumnContract("symbol", "string", True),
            ColumnContract("strategy_id", "string", True),
            ColumnContract("coverage_start", "timestamp_tz", True),
            ColumnContract("coverage_end", "timestamp_tz", True),
            ColumnContract("recorded_ts", "timestamp_tz", False),
            ColumnContract("provenance_tag", "string", True),
        ),
    ),
)

DEFAULT_SHARD_STITCH_CONTRACT: dict[str, Any] = {
    "default_shard_unit": "(symbol, quarter)",
    "default_shard_id_format": "YYYYQ#",
    "warmup_policy": {
        "applies_to": ["derived_bars", "shared_features", "family_events"],
        "rule": "materialize_pre_evaluation_overlap_before_trade_counting",
    },
    "stitch_policy": {
        "sum_metrics": ["trade_count", "net_pnl", "winners", "losers"],
        "recompute_metrics": ["profit_factor", "win_rate", "average_trade"],
        "preserve_coverage_dimensions": [
            "raw_coverage",
            "derived_coverage",
            "compact_coverage",
            "closed_trade_coverage",
            "rich_publication_coverage",
            "app_visible_coverage",
        ],
    },
    "publication_policy": {
        "compact_pass_default": True,
        "rich_pass_separate": True,
    },
}
