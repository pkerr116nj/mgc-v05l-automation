"""Platform-oriented warehouse historical evaluator review."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from ..research.platform import (
    build_registry_run_row,
    build_research_analytics_views,
    config_payload,
    discover_best_sources,
    ensure_trade_scope_bundle,
    last_source_discovery_metadata,
    register_experiment_run,
    stable_hash,
    write_json_manifest,
)
from ..research.warehouse_historical_evaluator._warehouse_common import read_parquet_rows
from ..research.warehouse_historical_evaluator.layout import build_layout
from ..research.warehouse_historical_evaluator.multi_symbol_runner import (
    DEFAULT_BASELINE_REPORT_PATH,
    DEFAULT_BASKET,
    DEFAULT_END,
    DEFAULT_SHARD_ID,
    DEFAULT_SQLITE_PATH,
    DEFAULT_START,
    run_multi_symbol_warehouse_shard,
)

REPO_ROOT = Path.cwd()
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports"
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_PLATFORM_SCOPE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "strategy_scopes" / "warehouse_historical_evaluator"
DEFAULT_REGISTRY_ROOT = REPO_ROOT / "outputs" / "research_platform" / "registry"
DEFAULT_ANALYTICS_ROOT = REPO_ROOT / "outputs" / "research_platform" / "analytics" / "warehouse_historical_evaluator"
DEFAULT_WAREHOUSE_PUBLISH_MODE = "cumulative"
WAREHOUSE_FEATURE_VERSION = "warehouse_shared_features_v1"
WAREHOUSE_CANDIDATE_VERSION = "warehouse_lane_candidates_v1"
WAREHOUSE_OUTCOME_ENGINE_VERSION = "warehouse_closed_trades_v1"
WAREHOUSE_OPERATING_POLICY_VERSION = "warehouse_operating_policy_v1"


@dataclass(frozen=True)
class WarehousePlatformReviewConfig:
    symbol_lane_map: dict[str, list[str]] = field(
        default_factory=lambda: {symbol: list(lanes) for symbol, lanes in DEFAULT_BASKET.items()}
    )
    shard_id: str = DEFAULT_SHARD_ID
    required_timeframes: tuple[str, ...] = ("1m",)
    publish_registry: bool = True
    publish_analytics: bool = True
    baseline_report_path: str = str(DEFAULT_BASELINE_REPORT_PATH)
    publish_mode: str = DEFAULT_WAREHOUSE_PUBLISH_MODE


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="warehouse-platform-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SQLITE_PATH), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Warehouse substrate output root.")
    parser.add_argument("--baseline-report", default=str(DEFAULT_BASELINE_REPORT_PATH), help="Baseline report path for warehouse comparisons.")
    parser.add_argument("--start", default=DEFAULT_START, help="Inclusive ISO start timestamp.")
    parser.add_argument("--end", default=DEFAULT_END, help="Inclusive ISO end timestamp.")
    parser.add_argument("--shard-id", default=DEFAULT_SHARD_ID, help="Warehouse shard id label.")
    parser.add_argument(
        "--publish-mode",
        choices=("cumulative", "diagnostic", "both"),
        default=DEFAULT_WAREHOUSE_PUBLISH_MODE,
        help="Publish cumulative app-facing truth, isolated diagnostic truth, or both from one warehouse materialization.",
    )
    parser.add_argument("--scope-root", default=str(DEFAULT_PLATFORM_SCOPE_ROOT), help="Scope-bundle root for cumulative publication mode.")
    parser.add_argument("--registry-root", default=str(DEFAULT_REGISTRY_ROOT), help="Registry root for cumulative publication mode.")
    parser.add_argument("--analytics-root", default=str(DEFAULT_ANALYTICS_ROOT), help="Analytics root for cumulative publication mode.")
    return parser


def run_review(
    *,
    source_db: Path,
    output_dir: Path,
    warehouse_root: Path,
    baseline_report_path: Path,
    start_timestamp: datetime,
    end_timestamp: datetime,
    shard_id: str,
    publish_mode: str = DEFAULT_WAREHOUSE_PUBLISH_MODE,
    scope_root: Path | None = None,
    registry_root: Path | None = None,
    analytics_root: Path | None = None,
) -> dict[str, Any]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    output_dir.mkdir(parents=True, exist_ok=True)
    warehouse_root.mkdir(parents=True, exist_ok=True)
    scope_root = (scope_root or DEFAULT_PLATFORM_SCOPE_ROOT).resolve()
    registry_root = (registry_root or DEFAULT_REGISTRY_ROOT).resolve()
    analytics_root = (analytics_root or DEFAULT_ANALYTICS_ROOT).resolve()
    started = perf_counter()
    review_config = WarehousePlatformReviewConfig(
        baseline_report_path=str(baseline_report_path.resolve()),
        shard_id=shard_id,
        publish_mode=publish_mode,
    )
    review_config_payload = config_payload(review_config)
    symbol_lane_map = {symbol.upper(): list(lanes) for symbol, lanes in review_config.symbol_lane_map.items()}

    discovery_started = perf_counter()
    source_index = discover_best_sources(
        symbols=set(symbol_lane_map),
        timeframes=set(review_config.required_timeframes),
        sqlite_paths=[source_db],
    )
    discovery_seconds = perf_counter() - discovery_started

    warehouse_started = perf_counter()
    warehouse_result = run_multi_symbol_warehouse_shard(
        root_dir=warehouse_root,
        sqlite_path=source_db,
        symbol_lane_map=symbol_lane_map,
        shard_id=shard_id,
        start_ts=start_timestamp,
        end_ts=end_timestamp,
        baseline_report_path=baseline_report_path,
    )
    warehouse_seconds = perf_counter() - warehouse_started

    run_generated_at = datetime.now(UTC).isoformat()
    all_compact_rows = _load_warehouse_dataset_rows(root_dir=warehouse_root, dataset_key="lane_compact_results")
    all_closed_trade_rows = _load_warehouse_dataset_rows(root_dir=warehouse_root, dataset_key="lane_closed_trades")
    diagnostic_compact_rows = _load_warehouse_dataset_rows(
        root_dir=warehouse_root,
        dataset_key="lane_compact_results",
        shard_id=shard_id,
    )
    diagnostic_closed_trade_rows = _load_warehouse_dataset_rows(
        root_dir=warehouse_root,
        dataset_key="lane_closed_trades",
        shard_id=shard_id,
    )
    cumulative_compact_rows = _aggregate_compact_rows_by_lane(
        compact_rows=all_compact_rows,
        closed_trade_rows=all_closed_trade_rows,
    )

    diagnostic_root = output_dir / "diagnostic_platform"
    publication_variants: dict[str, dict[str, Any]] = {}
    publication_timings: dict[str, float] = {}

    if publish_mode in {"cumulative", "both"}:
        cumulative_started = perf_counter()
        publication_variants["cumulative"] = _publish_warehouse_variant(
            variant_name="cumulative",
            compact_rows=cumulative_compact_rows,
            closed_trade_rows=all_closed_trade_rows,
            source_index=source_index,
            source_db=source_db,
            scope_root=scope_root,
            registry_root=registry_root,
            analytics_root=analytics_root,
            review_config_payload=review_config_payload,
            symbol_lane_map=symbol_lane_map,
            warehouse_root=warehouse_root,
            warehouse_result=warehouse_result,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            run_generated_at=run_generated_at,
        )
        publication_timings["cumulative_seconds"] = round(perf_counter() - cumulative_started, 6)
    if publish_mode in {"diagnostic", "both"}:
        diagnostic_started = perf_counter()
        publication_variants["diagnostic"] = _publish_warehouse_variant(
            variant_name="diagnostic",
            compact_rows=diagnostic_compact_rows,
            closed_trade_rows=diagnostic_closed_trade_rows,
            source_index=source_index,
            source_db=source_db,
            scope_root=diagnostic_root / "strategy_scopes" / "warehouse_historical_evaluator",
            registry_root=diagnostic_root / "registry",
            analytics_root=diagnostic_root / "analytics" / "warehouse_historical_evaluator",
            review_config_payload=review_config_payload,
            symbol_lane_map=symbol_lane_map,
            warehouse_root=warehouse_root,
            warehouse_result=warehouse_result,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            run_generated_at=run_generated_at,
        )
        publication_timings["diagnostic_seconds"] = round(perf_counter() - diagnostic_started, 6)

    primary_variant_name = "diagnostic" if publish_mode == "both" else publish_mode
    primary_variant = publication_variants[primary_variant_name]

    payload = {
        "study": "Warehouse historical evaluator platform review",
        "generated_at": run_generated_at,
        "publish_mode": publish_mode,
        "methodology": {
            "summary": "Warehouse compact and closed-trade truth adapted into generic trade-scope bundles, then published through the shared registry and analytics contract.",
            "review_config": review_config_payload,
            "source_date_span": {
                "start_timestamp": start_timestamp.isoformat(),
                "end_timestamp": end_timestamp.isoformat(),
            },
            "shared_layers_used": [
                "source_context.discovery",
                "generic_trade_scope_bundle",
                "experiment_registry",
                "research_analytics",
            ],
            "family_specific_layers": [
                "warehouse raw/derived materialization",
                "warehouse lane candidate/entry/closed-trade materialization",
                "warehouse compact result classification",
            ],
        },
        "targets": primary_variant["targets"],
        "warehouse": {
            "root_dir": str(warehouse_root.resolve()),
            "proof_path": warehouse_result["proof_path"],
            "proof_markdown_path": warehouse_result["proof_markdown_path"],
            "duckdb_path": warehouse_result["duckdb_path"],
            "closed_trade_count": primary_variant["closed_trade_count"],
            "compact_result_count": primary_variant["compact_result_count"],
            "materialization_timing": warehouse_result.get("timing", {}),
        },
        "registry": primary_variant["registry"],
        "analytics": primary_variant["analytics"],
        "publication_variants": {
            name: {
                "scope_root": variant["scope_root"],
                "registry": variant["registry"],
                "analytics": variant["analytics"],
                "target_count": len(variant["targets"]),
                "closed_trade_count": variant["closed_trade_count"],
                "compact_result_count": variant["compact_result_count"],
            }
            for name, variant in publication_variants.items()
        },
        "timing": {
            "source_discovery_seconds": round(discovery_seconds, 6),
            "warehouse_materialization_seconds": round(warehouse_seconds, 6),
            **publication_timings,
            "scope_bundle_seconds": primary_variant["timing"]["scope_bundle_seconds"],
            "registry_seconds": primary_variant["timing"]["registry_seconds"],
            "analytics_seconds": primary_variant["timing"]["analytics_seconds"],
            "total_wall_seconds": round(perf_counter() - started, 6),
        },
    }
    json_path = output_dir / "warehouse_platform_review.json"
    markdown_path = output_dir / "warehouse_platform_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(payload).strip() + "\n", encoding="utf-8")
    write_json_manifest(
        output_dir / "warehouse_platform_manifest.json",
        {
            "artifact_version": "warehouse_platform_review_v1",
            "generated_at": run_generated_at,
            "source_db": str(source_db.resolve()),
            "warehouse_root": str(warehouse_root.resolve()),
            "review_config": review_config_payload,
            "publish_mode": publish_mode,
            "registry_manifest_path": primary_variant["registry"]["manifest_path"],
            "analytics_manifest_path": primary_variant["analytics"]["manifest_path"],
            "platform_analytics_manifest_path": primary_variant["analytics"]["platform_manifest_path"],
            "target_count": len(primary_variant["targets"]),
            "publication_variants": {
                name: {
                    "registry_manifest_path": variant["registry"]["manifest_path"],
                    "analytics_manifest_path": variant["analytics"]["manifest_path"],
                    "platform_analytics_manifest_path": variant["analytics"]["platform_manifest_path"],
                    "target_count": len(variant["targets"]),
                }
                for name, variant in publication_variants.items()
            },
        },
    )
    return {
        "json_path": str(json_path.resolve()),
        "markdown_path": str(markdown_path.resolve()),
        "registry_manifest_path": primary_variant["registry"]["manifest_path"],
        "analytics_manifest_path": primary_variant["analytics"]["manifest_path"],
        "platform_analytics_manifest_path": primary_variant["analytics"]["platform_manifest_path"],
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    start_timestamp = datetime.fromisoformat(args.start)
    end_timestamp = datetime.fromisoformat(args.end)
    output_dir = Path(args.output_dir) if args.output_dir else _default_output_dir()
    result = run_review(
        source_db=Path(args.source_db),
        output_dir=output_dir,
        warehouse_root=Path(args.warehouse_root),
        baseline_report_path=Path(args.baseline_report),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        shard_id=str(args.shard_id),
        publish_mode=str(args.publish_mode),
        scope_root=Path(args.scope_root),
        registry_root=Path(args.registry_root),
        analytics_root=Path(args.analytics_root),
    )
    print(json.dumps(result, indent=2, sort_keys=True))


def _default_output_dir() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    return DEFAULT_OUTPUT_ROOT / f"warehouse_platform_review_{stamp}"


def _load_warehouse_dataset_rows(
    *,
    root_dir: Path,
    dataset_key: str,
    shard_id: str | None = None,
) -> list[dict[str, Any]]:
    layout = build_layout(root_dir.resolve())
    dataset_root = Path(layout[dataset_key])
    rows: list[dict[str, Any]] = []
    for parquet_path in sorted(dataset_root.rglob("*.parquet")):
        if parquet_path.name.startswith("_"):
            continue
        dataset_rows = read_parquet_rows(parquet_path)
        if shard_id is not None:
            dataset_rows = [row for row in dataset_rows if str(row.get("shard_id") or "") == shard_id]
        rows.extend(dataset_rows)
    return rows


def _aggregate_compact_rows_by_lane(
    *,
    compact_rows: list[dict[str, Any]],
    closed_trade_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    compact_by_lane: dict[str, list[dict[str, Any]]] = {}
    trades_by_lane: dict[str, list[dict[str, Any]]] = {}
    for row in compact_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if lane_id:
            compact_by_lane.setdefault(lane_id, []).append(row)
    for row in closed_trade_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if lane_id:
            trades_by_lane.setdefault(lane_id, []).append(row)

    aggregated: list[dict[str, Any]] = []
    for lane_id in sorted(set(compact_by_lane) | set(trades_by_lane)):
        lane_rows = sorted(
            compact_by_lane.get(lane_id, []),
            key=lambda row: (
                str(row.get("canonical_input_end") or ""),
                str(row.get("emitted_compact_end") or ""),
                str(row.get("shard_id") or ""),
            ),
        )
        if not lane_rows:
            continue
        latest = dict(lane_rows[-1])
        lane_trade_rows = sorted(
            trades_by_lane.get(lane_id, []),
            key=lambda row: str(row.get("exit_ts") or row.get("entry_ts") or ""),
        )
        pnl_values = [float(row.get("pnl") or 0.0) for row in lane_trade_rows]
        positives = sum(value for value in pnl_values if value > 0.0)
        negatives = -sum(value for value in pnl_values if value < 0.0)
        trade_count = len(lane_trade_rows)
        latest["shard_id"] = "cumulative"
        latest["trade_count"] = trade_count
        latest["net_pnl"] = sum(pnl_values)
        latest["profit_factor"] = None if negatives == 0.0 and positives == 0.0 else (positives / negatives if negatives else None)
        latest["win_rate"] = (sum(1 for value in pnl_values if value > 0.0) / trade_count) if trade_count else 0.0
        latest["result_classification"] = "nonzero_trade" if trade_count else latest.get("result_classification") or "zero_trade"
        latest["eligibility_status"] = "eligible_nonzero_trade" if trade_count else (latest.get("eligibility_status") or "eligible_no_closed_trades")
        latest["zero_trade_flag"] = trade_count == 0
        latest["canonical_input_start"] = _min_timestamp_str(row.get("canonical_input_start") for row in lane_rows)
        latest["canonical_input_end"] = _max_timestamp_str(row.get("canonical_input_end") for row in lane_rows)
        latest["emitted_compact_start"] = _min_timestamp_str(row.get("emitted_compact_start") for row in lane_rows)
        latest["emitted_compact_end"] = _max_timestamp_str(row.get("emitted_compact_end") for row in lane_rows)
        latest["closed_trade_start"] = _min_timestamp_str(row.get("entry_ts") for row in lane_trade_rows)
        latest["closed_trade_end"] = _max_timestamp_str(row.get("exit_ts") for row in lane_trade_rows)
        aggregated.append(latest)
    return aggregated


def _publish_warehouse_variant(
    *,
    variant_name: str,
    compact_rows: list[dict[str, Any]],
    closed_trade_rows: list[dict[str, Any]],
    source_index: dict[str, dict[str, Any]],
    source_db: Path,
    scope_root: Path,
    registry_root: Path,
    analytics_root: Path,
    review_config_payload: dict[str, Any],
    symbol_lane_map: dict[str, list[str]],
    warehouse_root: Path,
    warehouse_result: dict[str, Any],
    start_timestamp: datetime,
    end_timestamp: datetime,
    run_generated_at: str,
) -> dict[str, Any]:
    trades_by_lane: dict[str, list[dict[str, Any]]] = {}
    for row in closed_trade_rows:
        lane_id = str(row.get("lane_id") or "").strip()
        if lane_id:
            trades_by_lane.setdefault(lane_id, []).append(row)

    scope_started = perf_counter()
    scope_rows: list[dict[str, Any]] = []
    target_rows: list[dict[str, Any]] = []
    for compact_row in sorted(compact_rows, key=lambda row: str(row.get("lane_id") or "")):
        lane_id = str(compact_row.get("lane_id") or "").strip()
        symbol = str(compact_row.get("symbol") or "").strip().upper()
        if not lane_id or not symbol:
            continue
        selected_sources = _selected_sources_for_symbol(source_index=source_index, source_db=source_db, symbol=symbol)
        trade_records = [
            _warehouse_trade_record(row)
            for row in sorted(
                trades_by_lane.get(lane_id, []),
                key=lambda row: str(row.get("exit_ts") or row.get("entry_ts") or ""),
            )
        ]
        bundle = ensure_trade_scope_bundle(
            bundle_root=scope_root,
            strategy_family="warehouse_historical_evaluator",
            strategy_variant=lane_id,
            symbol=symbol,
            selected_sources=selected_sources,
            start_timestamp=str(compact_row.get("canonical_input_start") or start_timestamp.isoformat()),
            end_timestamp=str(compact_row.get("canonical_input_end") or end_timestamp.isoformat()),
            allowed_sessions=(),
            execution_model=str(compact_row.get("execution_model") or "warehouse_execution_model"),
            trade_records=trade_records,
            point_value=None,
            metadata={
                "lane_id": lane_id,
                "family": compact_row.get("family"),
                "artifact_class": compact_row.get("artifact_class"),
                "result_classification": compact_row.get("result_classification"),
                "warehouse_root": str(warehouse_root.resolve()),
                "warehouse_publication_mode": variant_name,
                "warehouse_shard_id": compact_row.get("shard_id"),
            },
        )
        summary = _compact_summary_metrics(compact_row)
        label = _warehouse_strategy_label(compact_row)
        config_hash = _target_config_hash(compact_row, review_config_payload["config_hash"])
        scope_rows.append(
            {
                "target_id": lane_id,
                "label": label,
                "lane_id": lane_id,
                "symbol": symbol,
                "allowed_sessions": [],
                "scope_bundle_id": bundle.bundle_id,
                "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                "summary_metrics": summary,
                "config_hash": config_hash,
            }
        )
        target_rows.append(
            {
                "strategy_family": "warehouse_historical_evaluator",
                "strategy_variant": lane_id,
                "target_id": lane_id,
                "label": label,
                "symbol": symbol,
                "allowed_sessions": [],
                "record_kind": "warehouse_strategy_scope",
                "analytics_publish": True,
                "scope_bundle_id": bundle.bundle_id,
                "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                "config_hash": config_hash,
                "target_hash": stable_hash({"lane_id": lane_id, "symbol": symbol, "publication_mode": variant_name}, length=24),
                "summary_metrics": summary,
                "generated_at": run_generated_at,
                "artifacts": {
                    "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                    "warehouse_proof_path": str(warehouse_result["proof_path"]),
                    "warehouse_duckdb_path": str(warehouse_result["duckdb_path"]),
                },
                "lineage": {
                    "warehouse_publication_mode": variant_name,
                },
            }
        )
    scope_seconds = perf_counter() - scope_started

    run_row = build_registry_run_row(
        strategy_family="warehouse_historical_evaluator",
        strategy_variant=f"platform_review_{variant_name}",
        date_span={
            "start_timestamp": start_timestamp.isoformat(),
            "end_timestamp": end_timestamp.isoformat(),
        },
        data_version=stable_hash(
            {
                "source_db": str(source_db.resolve()),
                "source_index": _serializable_source_index(source_index),
                "variant_name": variant_name,
            },
            length=24,
        ),
        feature_version=WAREHOUSE_FEATURE_VERSION,
        candidate_version=WAREHOUSE_CANDIDATE_VERSION,
        outcome_engine_version=WAREHOUSE_OUTCOME_ENGINE_VERSION,
        config_hash=stable_hash(
            {
                "review_config_hash": review_config_payload["config_hash"],
                "variant_name": variant_name,
                "target_configs": {row["target_id"]: row["config_hash"] for row in target_rows},
            },
            length=24,
        ),
        target_hash=stable_hash([row["target_id"] for row in target_rows], length=24),
        bundle_ids={
            "scope_bundle_ids": {row["target_id"]: row["scope_bundle_id"] for row in scope_rows},
            "warehouse_root": str(warehouse_root.resolve()),
            "publication_mode": variant_name,
        },
        summary_metrics={
            "target_count": len(target_rows),
            "warehouse_symbol_count": len(symbol_lane_map),
            "closed_trade_count": len(closed_trade_rows),
        },
        lineage={
            "study": "warehouse_platform_review",
            "review_config_hash": review_config_payload["config_hash"],
            "warehouse_proof_path": str(warehouse_result["proof_path"]),
            "warehouse_publication_mode": variant_name,
        },
        generated_at=run_generated_at,
    )
    registry_started = perf_counter()
    registry_result = register_experiment_run(
        registry_root=registry_root,
        run_row=run_row,
        target_rows=target_rows,
    )
    registry_seconds = perf_counter() - registry_started
    analytics_started = perf_counter()
    analytics_result = build_research_analytics_views(
        registry_root=registry_root,
        analytics_root=analytics_root,
        strategy_family="warehouse_historical_evaluator",
        family_metadata=_warehouse_family_metadata(
            variant_name=variant_name,
            compact_rows=compact_rows,
            closed_trade_rows=closed_trade_rows,
        ),
    )
    analytics_seconds = perf_counter() - analytics_started
    return {
        "targets": scope_rows,
        "registry": registry_result,
        "analytics": analytics_result,
        "scope_root": str(scope_root.resolve()),
        "closed_trade_count": len(closed_trade_rows),
        "compact_result_count": len(compact_rows),
        "timing": {
            "scope_bundle_seconds": round(scope_seconds, 6),
            "registry_seconds": round(registry_seconds, 6),
            "analytics_seconds": round(analytics_seconds, 6),
        },
    }


def _min_timestamp_str(values: Any) -> str | None:
    normalized = sorted(str(value) for value in values if value)
    return normalized[0] if normalized else None


def _max_timestamp_str(values: Any) -> str | None:
    normalized = sorted(str(value) for value in values if value)
    return normalized[-1] if normalized else None


def _selected_sources_for_symbol(
    *,
    source_index: dict[str, dict[str, Any]],
    source_db: Path,
    symbol: str,
) -> dict[str, dict[str, Any]]:
    selection = source_index.get(symbol, {}).get("1m")
    if selection is None:
        return {
            "1m": {
                "data_source": "historical_1m_canonical",
                "sqlite_path": str(source_db.resolve()),
                "row_count": 0,
                "start_ts": None,
                "end_ts": None,
            }
        }
    return {
        "1m": {
            "data_source": selection.data_source,
            "sqlite_path": str(selection.sqlite_path),
            "row_count": selection.row_count,
            "start_ts": selection.start_ts,
            "end_ts": selection.end_ts,
        }
    }


def _warehouse_family_metadata(
    *,
    variant_name: str,
    compact_rows: list[dict[str, Any]],
    closed_trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    year_values = sorted(
        {
            str(timestamp)[:4]
            for timestamp in (
                [row.get("entry_ts") for row in closed_trade_rows]
                + [row.get("exit_ts") for row in closed_trade_rows]
                + [row.get("canonical_input_start") for row in compact_rows]
                + [row.get("canonical_input_end") for row in compact_rows]
            )
            if str(timestamp or "")[:4].isdigit()
        }
    )
    shard_ids = sorted(
        {
            str(shard_id).strip()
            for shard_id in (
                [row.get("shard_id") for row in compact_rows]
                + [row.get("shard_id") for row in closed_trade_rows]
            )
            if str(shard_id or "").strip() and str(shard_id).strip() != "cumulative"
        }
    )
    if not shard_ids and variant_name == "cumulative":
        shard_ids = ["cumulative"]
    return {
        "default_app_visible": variant_name == "cumulative",
        "publication_mode": variant_name,
        "view_role": "default_app_visible_tenant" if variant_name == "cumulative" else "isolated_quarter_diagnostic",
        "diagnostic_only": variant_name == "diagnostic",
        "operating_policy_version": WAREHOUSE_OPERATING_POLICY_VERSION,
        "time_horizon": {
            "mode": "cumulative" if variant_name == "cumulative" else "diagnostic_quarter",
            "years": year_values,
            "shard_ids": shard_ids,
        },
    }


def _warehouse_trade_record(row: dict[str, Any]) -> dict[str, Any]:
    hold_minutes = int(row.get("hold_minutes") or 0)
    pnl_cash = float(row.get("pnl") or 0.0)
    return {
        "instrument": row.get("symbol"),
        "variant_id": row.get("lane_id"),
        "decision_id": row.get("candidate_id"),
        "entry_ts": row.get("entry_ts"),
        "exit_ts": row.get("exit_ts"),
        "pnl_cash": pnl_cash,
        "gross_pnl_cash": pnl_cash,
        "fees_paid": 0.0,
        "slippage_cost": 0.0,
        "exit_reason": row.get("exit_reason"),
        "session_segment": "UNKNOWN",
        "mfe_points": None,
        "mae_points": None,
        "bars_held_1m": hold_minutes,
        "hold_minutes": hold_minutes,
        "entry_price": row.get("entry_price"),
        "exit_price": row.get("exit_price"),
        "regime_bucket": row.get("family"),
        "volatility_bucket": None,
    }


def _compact_summary_metrics(compact_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_count": int(compact_row.get("trade_count") or 0),
        "net_pnl_cash": round(float(compact_row.get("net_pnl") or 0.0), 6),
        "profit_factor": compact_row.get("profit_factor"),
        "win_rate": compact_row.get("win_rate"),
        "result_classification": compact_row.get("result_classification"),
        "eligibility_status": compact_row.get("eligibility_status"),
        "zero_trade_flag": compact_row.get("zero_trade_flag"),
    }


def _target_config_hash(compact_row: dict[str, Any], review_config_hash: str) -> str:
    return stable_hash(
        {
            "review_config_hash": review_config_hash,
            "lane_id": compact_row.get("lane_id"),
            "family": compact_row.get("family"),
            "execution_model": compact_row.get("execution_model"),
            "artifact_class": compact_row.get("artifact_class"),
            "shard_id": compact_row.get("shard_id"),
        },
        length=24,
    )


def _warehouse_strategy_label(compact_row: dict[str, Any]) -> str:
    family = str(compact_row.get("family") or "warehouse").replace("_", " ").strip().title()
    symbol = str(compact_row.get("symbol") or "").strip().upper()
    lane_id = str(compact_row.get("lane_id") or "").strip()
    if family and symbol:
        return f"{family} / {symbol} / {lane_id}"
    return lane_id or symbol or family or "Warehouse lane"


def _serializable_source_index(source_index: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    for symbol, timeframe_rows in source_index.items():
        payload[symbol] = {
            timeframe: {
                "data_source": selection.data_source,
                "sqlite_path": str(selection.sqlite_path),
                "row_count": selection.row_count,
                "start_ts": selection.start_ts,
                "end_ts": selection.end_ts,
            }
            for timeframe, selection in timeframe_rows.items()
        }
    return payload


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Warehouse Platform Review",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Targets: `{len(payload.get('targets') or [])}`",
        f"- Warehouse root: `{payload['warehouse']['root_dir']}`",
        f"- Registry manifest: `{payload['registry']['manifest_path']}`",
        f"- Analytics manifest: `{payload['analytics']['manifest_path']}`",
        "",
        "## Timings",
    ]
    for key, value in (payload.get("timing") or {}).items():
        lines.append(f"- {key}: `{value}`")
    lines.append("")
    lines.append("## Targets")
    for row in list(payload.get("targets") or []):
        summary = dict(row.get("summary_metrics") or {})
        lines.append(
            f"- `{row['target_id']}`: net `{summary.get('net_pnl_cash')}`, trades `{summary.get('trade_count')}`, classification `{summary.get('result_classification')}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
