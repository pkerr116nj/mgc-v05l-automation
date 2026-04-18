"""Warehouse-backed historical evaluator scaffolding."""

from .contracts import (
    DEFAULT_DATASET_CONTRACTS,
    DEFAULT_DUCKDB_TABLE_CONTRACTS,
    DEFAULT_SHARD_STITCH_CONTRACT,
)
from .raw_materializer import export_canonical_1m_partition
from .derived_materializer import materialize_derived_timeframe_partition
from .compact_results import load_compact_rows_from_report, write_compact_result_partitions
from .shared_features import (
    materialize_shared_features_1m_timing_partition,
    materialize_shared_features_5m_partition,
)
from .family_events import materialize_family_event_tables_partition
from .warehouse_evaluator import (
    materialize_lane_candidates_partition,
    materialize_lane_entries_partition,
    materialize_lane_closed_trades_partition,
    materialize_lane_compact_results_partition,
)
from .multi_symbol_runner import run_multi_symbol_warehouse_shard
from .layout import build_layout


def bootstrap_storage_skeleton(*args, **kwargs):
    from .bootstrap import bootstrap_storage_skeleton as _bootstrap_storage_skeleton

    return _bootstrap_storage_skeleton(*args, **kwargs)


def run_thin_warehouse_slice(*args, **kwargs):
    from .pilot import run_thin_warehouse_slice as _run_thin_warehouse_slice

    return _run_thin_warehouse_slice(*args, **kwargs)

__all__ = [
    "DEFAULT_DATASET_CONTRACTS",
    "DEFAULT_DUCKDB_TABLE_CONTRACTS",
    "DEFAULT_SHARD_STITCH_CONTRACT",
    "bootstrap_storage_skeleton",
    "build_layout",
    "export_canonical_1m_partition",
    "materialize_derived_timeframe_partition",
    "materialize_shared_features_5m_partition",
    "materialize_shared_features_1m_timing_partition",
    "materialize_family_event_tables_partition",
    "materialize_lane_candidates_partition",
    "materialize_lane_entries_partition",
    "materialize_lane_closed_trades_partition",
    "materialize_lane_compact_results_partition",
    "run_multi_symbol_warehouse_shard",
    "load_compact_rows_from_report",
    "write_compact_result_partitions",
    "run_thin_warehouse_slice",
]
