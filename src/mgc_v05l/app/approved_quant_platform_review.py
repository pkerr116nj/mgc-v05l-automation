"""Platform-oriented approved-quant review using shared registry and analytics layers."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from .strategy_universe_retest import _discover_best_sources, _evaluate_approved_quant_lane_symbol
from .approved_quant_lanes.runtime_boundary import APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION
from .approved_quant_lanes.specs import approved_quant_lane_scope_fingerprint, approved_quant_lane_specs
from ..research.platform import (
    build_registry_run_row,
    build_research_analytics_views,
    config_payload,
    ensure_trade_scope_bundle,
    register_experiment_run,
    stable_hash,
    write_json_manifest,
)

REPO_ROOT = Path.cwd()
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports"
DEFAULT_PLATFORM_SCOPE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "strategy_scopes" / "approved_quant"
DEFAULT_REGISTRY_ROOT = REPO_ROOT / "outputs" / "research_platform" / "registry"
DEFAULT_ANALYTICS_ROOT = REPO_ROOT / "outputs" / "research_platform" / "analytics" / "approved_quant"
APPROVED_QUANT_FEATURE_VERSION = "approved_quant_feature_rows_v1"
APPROVED_QUANT_CANDIDATE_VERSION = "approved_quant_signal_state_v1"
APPROVED_QUANT_OUTCOME_ENGINE_VERSION = "approved_quant_outcome_engine_v1"


@dataclass(frozen=True)
class ApprovedQuantPlatformReviewConfig:
    required_timeframes: tuple[str, ...] = ("1m", "5m")
    execution_model: str = "APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP"
    publish_registry: bool = True
    publish_analytics: bool = True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="approved-quant-platform-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def run_review(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    output_dir.mkdir(parents=True, exist_ok=True)
    started = perf_counter()
    review_config = ApprovedQuantPlatformReviewConfig()
    review_config_payload = config_payload(review_config)
    specs = approved_quant_lane_specs()
    symbol_set = {symbol for spec in specs for symbol in spec.symbols}
    discovery_started = perf_counter()
    source_index = _discover_best_sources(
        symbols=symbol_set,
        timeframes=set(review_config.required_timeframes),
        sqlite_paths=[source_db],
    )
    discovery_seconds = perf_counter() - discovery_started

    target_rows: list[dict[str, Any]] = []
    scope_rows: list[dict[str, Any]] = []
    execution_started = perf_counter()
    for spec in specs:
        for symbol in spec.symbols:
            result = _evaluate_approved_quant_lane_symbol(
                spec=spec,
                symbol=symbol,
                bar_source_index=source_index,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            if result is None:
                continue
            selected_sources = {
                timeframe: {
                    "data_source": selection.data_source,
                    "sqlite_path": str(selection.sqlite_path),
                    "row_count": selection.row_count,
                    "start_ts": selection.start_ts,
                    "end_ts": selection.end_ts,
                }
                for timeframe, selection in source_index.get(symbol, {}).items()
            }
            trade_records = [
                _approved_quant_trade_record(
                    row=row,
                    symbol=symbol,
                    strategy_variant=f"{spec.variant_id}::{symbol}",
                )
                for row in result["trade_rows"]
            ]
            range_start = start_timestamp.isoformat() if start_timestamp is not None else (
                result["bars_1m"][0].end_ts.isoformat() if result["bars_1m"] else None
            )
            range_end = end_timestamp.isoformat() if end_timestamp is not None else (
                result["bars_1m"][-1].end_ts.isoformat() if result["bars_1m"] else None
            )
            bundle = ensure_trade_scope_bundle(
                bundle_root=DEFAULT_PLATFORM_SCOPE_ROOT,
                strategy_family="approved_quant",
                strategy_variant=f"{spec.variant_id}::{symbol}",
                symbol=symbol,
                selected_sources=selected_sources,
                start_timestamp=range_start,
                end_timestamp=range_end,
                allowed_sessions=spec.allowed_sessions,
                execution_model=review_config.execution_model,
                trade_records=trade_records,
                point_value=None,
                metadata={
                    "lane_id": spec.lane_id,
                    "lane_name": spec.lane_name,
                    "family": spec.family,
                    "approval_contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
                },
            )
            summary = dict(result["summary"])
            target_id = f"{spec.variant_id}::{symbol}"
            scope_rows.append(
                {
                    "target_id": target_id,
                    "label": f"{spec.lane_name} / {symbol}",
                    "lane_id": spec.lane_id,
                    "symbol": symbol,
                    "allowed_sessions": list(spec.allowed_sessions),
                    "scope_bundle_id": bundle.bundle_id,
                    "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                    "summary_metrics": summary,
                    "config_hash": approved_quant_lane_scope_fingerprint(spec),
                }
            )
            target_rows.append(
                {
                    "strategy_family": "approved_quant",
                    "strategy_variant": target_id,
                    "target_id": target_id,
                    "label": f"{spec.lane_name} / {symbol}",
                    "symbol": symbol,
                    "allowed_sessions": list(spec.allowed_sessions),
                    "record_kind": "strategy_scope",
                    "analytics_publish": True,
                    "scope_bundle_id": bundle.bundle_id,
                    "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                    "config_hash": approved_quant_lane_scope_fingerprint(spec),
                    "target_hash": stable_hash({"target_id": target_id, "symbol": symbol}, length=24),
                    "summary_metrics": summary,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "artifacts": {
                        "scope_bundle_manifest_path": str(bundle.manifest_path.resolve()),
                    },
                }
            )
    execution_seconds = perf_counter() - execution_started

    run_generated_at = datetime.now(UTC).isoformat()
    run_row = build_registry_run_row(
        strategy_family="approved_quant",
        strategy_variant="baseline_review",
        date_span={
            "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
            "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
        },
        data_version=stable_hash(source_index, length=24),
        feature_version=APPROVED_QUANT_FEATURE_VERSION,
        candidate_version=APPROVED_QUANT_CANDIDATE_VERSION,
        outcome_engine_version=APPROVED_QUANT_OUTCOME_ENGINE_VERSION,
        config_hash=stable_hash(
            {
                "review_config_hash": review_config_payload["config_hash"],
                "target_configs": {row["target_id"]: row["config_hash"] for row in target_rows},
            },
            length=24,
        ),
        target_hash=stable_hash([row["target_id"] for row in target_rows], length=24),
        bundle_ids={"scope_bundle_ids": {row["target_id"]: row["scope_bundle_id"] for row in scope_rows}},
        summary_metrics={"target_count": len(target_rows)},
        lineage={
            "study": "approved_quant_platform_review",
            "review_config_hash": review_config_payload["config_hash"],
        },
        generated_at=run_generated_at,
    )
    registry_started = perf_counter()
    registry_result = register_experiment_run(
        registry_root=DEFAULT_REGISTRY_ROOT,
        run_row=run_row,
        target_rows=target_rows,
    )
    registry_seconds = perf_counter() - registry_started
    analytics_started = perf_counter()
    analytics_result = build_research_analytics_views(
        registry_root=DEFAULT_REGISTRY_ROOT,
        analytics_root=DEFAULT_ANALYTICS_ROOT,
        strategy_family="approved_quant",
    )
    analytics_seconds = perf_counter() - analytics_started

    payload = {
        "study": "Approved Quant platform starter review",
        "generated_at": run_generated_at,
        "methodology": {
            "summary": "Approved quant historical trade truth persisted as generic strategy scope bundles and published through the shared registry/analytics platform.",
            "execution_model": review_config.execution_model,
            "runtime_contract_version": APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
            "review_config": review_config_payload,
            "source_date_span": {
                "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
                "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
            },
        },
        "targets": scope_rows,
        "registry": registry_result,
        "analytics": analytics_result,
        "timing": {
            "source_discovery_seconds": round(discovery_seconds, 6),
            "execution_seconds": round(execution_seconds, 6),
            "registry_seconds": round(registry_seconds, 6),
            "analytics_seconds": round(analytics_seconds, 6),
            "total_wall_seconds": round(perf_counter() - started, 6),
        },
    }
    json_path = output_dir / "approved_quant_platform_review.json"
    markdown_path = output_dir / "approved_quant_platform_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_markdown(payload).strip() + "\n", encoding="utf-8")
    write_json_manifest(
        output_dir / "approved_quant_platform_manifest.json",
        {
            "artifact_version": "approved_quant_platform_review_v1",
            "generated_at": run_generated_at,
            "source_db": str(source_db.resolve()),
            "review_config": review_config_payload,
            "registry_manifest_path": registry_result["manifest_path"],
            "analytics_manifest_path": analytics_result["manifest_path"],
            "target_count": len(scope_rows),
        },
    )
    return {
        "json_path": str(json_path.resolve()),
        "markdown_path": str(markdown_path.resolve()),
        "registry_manifest_path": registry_result["manifest_path"],
        "analytics_manifest_path": analytics_result["manifest_path"],
    }


def _approved_quant_trade_record(*, row: dict[str, Any], symbol: str, strategy_variant: str) -> dict[str, Any]:
    entry_ts = str(row.get("entry_timestamp") or "")
    exit_ts = str(row.get("exit_timestamp") or "")
    hold_minutes = None
    if entry_ts and exit_ts:
        try:
            hold_minutes = round((datetime.fromisoformat(exit_ts) - datetime.fromisoformat(entry_ts)).total_seconds() / 60.0, 6)
        except ValueError:
            hold_minutes = None
    return {
        "instrument": symbol,
        "variant_id": strategy_variant,
        "decision_id": str(row.get("trade_id") or row.get("entry_timestamp") or ""),
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "pnl_cash": float(row.get("realized_pnl") or 0.0),
        "gross_pnl_cash": float(row.get("realized_pnl") or 0.0),
        "fees_paid": 0.0,
        "slippage_cost": 0.0,
        "exit_reason": row.get("exit_reason"),
        "session_segment": row.get("entry_session_phase"),
        "family": row.get("family"),
        "side": row.get("side"),
        "entry_price": row.get("entry_price"),
        "exit_price": row.get("exit_price"),
        "mfe_points": None,
        "mae_points": None,
        "bars_held_1m": None,
        "hold_minutes": hold_minutes,
        "regime_bucket": None,
        "volatility_bucket": None,
        "truth_source": "approved_quant_research_execution",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Platform Review",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Runtime contract: `{payload['methodology']['runtime_contract_version']}`",
        f"- Source discovery seconds: `{payload['timing']['source_discovery_seconds']}`",
        f"- Execution seconds: `{payload['timing']['execution_seconds']}`",
        f"- Registry seconds: `{payload['timing']['registry_seconds']}`",
        f"- Analytics seconds: `{payload['timing']['analytics_seconds']}`",
        "",
        "## Targets",
    ]
    for row in payload["targets"]:
        lines.extend(
            [
                f"### {row['label']}",
                f"- Target id: `{row['target_id']}`",
                f"- Lane id: `{row['lane_id']}`",
                f"- Scope bundle id: `{row['scope_bundle_id']}`",
                f"- Net P&L: `{row['summary_metrics'].get('net_pnl')}`",
                f"- Trade count: `{row['summary_metrics'].get('trade_count')}`",
                "",
            ]
        )
    return "\n".join(lines)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (
        DEFAULT_OUTPUT_ROOT / f"approved_quant_platform_review_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    )
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_review(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
