"""Reusable compact shard-result contract helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..trend_participation.storage import materialize_parquet_dataset
from .layout import build_layout
from .raw_materializer import coerce_timestamp

LANE_FAMILY_EVENT_REQUIREMENTS: dict[str, set[str]] = {
    "usLatePauseResumeLongTurn": {"usLatePauseResumeLongTurn"},
    "asiaEarlyNormalBreakoutRetestHoldTurn": {"asiaEarlyNormalBreakoutRetestHoldTurn"},
    "asiaEarlyPauseResumeShortTurn": {"asiaEarlyPauseResumeShortTurn"},
}


def load_compact_rows_from_report(
    *,
    compact_report_path: Path,
    lane_ids: list[str],
    symbol: str,
    shard_id: str,
    canonical_input_range: dict[str, Any],
    available_families: set[str] | None = None,
) -> list[dict[str, Any]]:
    payload = json.loads(compact_report_path.read_text(encoding="utf-8"))
    report_rows = payload.get("results", [])
    indexed = {(row.get("strategy_id"), row.get("symbol")): row for row in report_rows}
    compact_rows: list[dict[str, Any]] = []
    for lane_id in lane_ids:
        key = (lane_id, symbol)
        if key not in indexed:
            raise RuntimeError(f"Lane {lane_id} for {symbol} not found in {compact_report_path}.")
        if available_families is not None:
            lane_family = str(indexed[key].get("family") or "")
            required_families = LANE_FAMILY_EVENT_REQUIREMENTS.get(lane_family, {lane_family} if lane_family else set())
            if required_families and not required_families.intersection(available_families):
                raise RuntimeError(
                    f"Lane {lane_id} family {lane_family} has no available family-event substrate in {symbol} {shard_id}."
                )
        compact_rows.append(
            compact_row_from_report_row(
                row=indexed[key],
                shard_id=shard_id,
                canonical_input_range=canonical_input_range,
            )
        )
    return compact_rows


def write_compact_result_partitions(
    *,
    root_dir: Path,
    compact_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    layout = build_layout(root_dir.resolve())
    written: list[dict[str, Any]] = []
    for row in compact_rows:
        lane_id = str(row["lane_id"])
        shard_id = str(row["shard_id"])
        partition_path = (
            layout["lane_compact_results"] / f"lane_id={lane_id}" / f"shard_id={shard_id}" / "result.parquet"
        )
        materialize_parquet_dataset(partition_path, [row])
        written.append(
            {
                "lane_id": lane_id,
                "shard_id": shard_id,
                "partition_path": partition_path,
                "row": row,
            }
        )
    return written


def compact_row_from_report_row(
    *,
    row: dict[str, Any],
    shard_id: str,
    canonical_input_range: dict[str, Any],
) -> dict[str, Any]:
    metrics = row.get("metrics", {})
    emitted = row.get("coverage", {}).get("derived_playback") or {}
    closed = row.get("coverage", {}).get("closed_trade_economics") or {}
    trade_count = int(metrics.get("trade_count") or 0)
    eligibility_status = str(row.get("eligibility_status") or "unknown")
    result_classification = classify_compact_result(
        eligibility_status=eligibility_status,
        trade_count=trade_count,
    )
    lane_id = str(row["strategy_id"])
    return {
        "lane_id": lane_id,
        "strategy_key": lane_id,
        "family": row.get("family"),
        "symbol": row.get("symbol"),
        "execution_model": row.get("execution_model"),
        "shard_id": shard_id,
        "artifact_class": "compact_summary",
        "result_classification": result_classification,
        "trade_count": trade_count,
        "net_pnl": float(metrics.get("net_pnl") or 0.0),
        "profit_factor": metrics.get("profit_factor"),
        "win_rate": metrics.get("win_rate"),
        "winners": int(metrics.get("winners") or 0),
        "losers": int(metrics.get("losers") or 0),
        "canonical_input_start": canonical_input_range["start"],
        "canonical_input_end": canonical_input_range["end"],
        "emitted_compact_start": coerce_timestamp(emitted["start"]) if emitted.get("start") else None,
        "emitted_compact_end": coerce_timestamp(emitted["end"]) if emitted.get("end") else None,
        "closed_trade_start": coerce_timestamp(closed["start"]) if closed.get("start") else None,
        "closed_trade_end": coerce_timestamp(closed["end"]) if closed.get("end") else None,
        "eligibility_status": eligibility_status,
        "zero_trade_flag": result_classification == "zero_trade",
        "reference_lane": bool(row.get("reference_lane")),
        "bucket": row.get("bucket"),
        "status": row.get("status"),
        "cohort": row.get("cohort"),
    }


def classify_compact_result(*, eligibility_status: str, trade_count: int) -> str:
    if eligibility_status.startswith("missing"):
        return "missing"
    if trade_count == 0:
        return "zero_trade"
    return "nonzero_trade"
