"""Presentation-facing research analytics built from registered research truth."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Mapping, Sequence

from .datasets import json_ready, read_jsonl_dataset, register_duckdb_catalog, stable_hash, write_dataset_bundle, write_json_manifest
from .registry import latest_target_entries

ANALYTICS_ARTIFACT_VERSION = "research_analytics_v1"
ANALYTICS_CONTRACT_VERSION = "research_analytics_contract_v2"
ANALYTICS_FAMILY_INDEX_VERSION = "research_analytics_family_index_v1"
RESEARCH_ANALYTICS_DATASETS = (
    "strategy_catalog",
    "daily_pnl",
    "strategy_summaries",
    "equity_curve",
    "drawdown_curve",
    "trade_blotter",
    "exit_reason_breakdown",
    "session_breakdown",
)
FULL_APP_REQUIRED_DATASETS = (
    "strategy_catalog",
    "daily_pnl",
    "strategy_summaries",
    "equity_curve",
    "drawdown_curve",
    "trade_blotter",
)
APP_COMPATIBLE_OPTIONAL_DATASETS = (
    "exit_reason_breakdown",
    "session_breakdown",
)


def build_research_analytics_views(
    *,
    registry_root: Path,
    analytics_root: Path,
    strategy_family: str | None = None,
    family_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    started = perf_counter()
    analytics_root = analytics_root.resolve()
    analytics_root.mkdir(parents=True, exist_ok=True)
    targets_started = perf_counter()
    target_entries = latest_target_entries(
        registry_root=registry_root,
        strategy_family=strategy_family,
        analytics_publishable_only=True,
    )
    targets_seconds = perf_counter() - targets_started

    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    exit_reason_rows: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    strategy_catalog_rows: list[dict[str, Any]] = []

    build_started = perf_counter()
    for entry in target_entries:
        trades = _load_trade_records_for_target(entry)
        strategy_id = _strategy_id(entry)
        strategy_family_value = str(entry.get("strategy_family") or "unknown")
        strategy_label = _strategy_label(entry)
        strategy_key = analytics_strategy_key(strategy_family=strategy_family_value, strategy_id=strategy_id)
        symbol = str(entry.get("symbol") or "")
        sessions = list(entry.get("allowed_sessions") or [])

        strategy_catalog_rows.append(
            {
                "strategy_key": strategy_key,
                "strategy_family": strategy_family_value,
                "family_label": _family_label(strategy_family_value),
                "strategy_id": strategy_id,
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "label": entry.get("label") or strategy_label,
                "symbol": symbol,
                "allowed_sessions": sessions,
                "scope_bundle_id": entry.get("scope_bundle_id"),
                "run_id": entry.get("run_id"),
                "record_kind": entry.get("record_kind"),
            }
        )
        daily_rows.extend(_daily_rows(trades=trades, entry=entry))
        summary_rows.append(_summary_row(trades=trades, entry=entry))
        if trades:
            equity_rows.extend(_equity_rows(trades=trades, entry=entry))
            drawdown_rows.extend(_drawdown_rows(trades=trades, entry=entry))
            trade_rows.extend(_trade_blotter_rows(trades=trades, entry=entry))
            exit_reason_rows.extend(_exit_reason_breakdown_rows(trades=trades, entry=entry))
            session_rows.extend(_session_breakdown_rows(trades=trades, entry=entry))
    build_seconds = perf_counter() - build_started

    datasets_dir = analytics_root / "datasets"
    dataset_started = perf_counter()
    specs = {
        "strategy_catalog": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="strategy_catalog", rows=strategy_catalog_rows),
        "daily_pnl": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="daily_pnl", rows=daily_rows),
        "strategy_summaries": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="strategy_summaries", rows=summary_rows),
        "equity_curve": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="equity_curve", rows=equity_rows),
        "drawdown_curve": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="drawdown_curve", rows=drawdown_rows),
        "trade_blotter": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="trade_blotter", rows=trade_rows),
        "exit_reason_breakdown": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="exit_reason_breakdown", rows=exit_reason_rows),
        "session_breakdown": write_dataset_bundle(bundle_dir=datasets_dir, dataset_name="session_breakdown", rows=session_rows),
    }
    dataset_seconds = perf_counter() - dataset_started
    duckdb_started = perf_counter()
    duckdb_path = register_duckdb_catalog(
        duckdb_path=analytics_root / "catalog.duckdb",
        view_to_parquet={name: Path(spec["parquet_path"]) for name, spec in specs.items()},
    )
    duckdb_seconds = perf_counter() - duckdb_started
    manifest = {
        "artifact_version": ANALYTICS_ARTIFACT_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "strategy_family": strategy_family,
        "contract_version": ANALYTICS_CONTRACT_VERSION,
        "family_metadata": json_ready(dict(family_metadata or {})),
        "strategy_count": len(strategy_catalog_rows),
        "datasets": specs,
        "duckdb_catalog_path": str(duckdb_path.resolve()),
        "registry_root": str(registry_root.resolve()),
        "strategy_family_counts": _strategy_family_counts(strategy_catalog_rows),
        "dataset_contract": {
            "strategy_catalog": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "strategy_family", "strategy_id", "strategy_label"],
            },
            "daily_pnl": {
                "selection_key": "strategy_key",
                "required_fields": ["date", "strategy_key", "strategy_family", "strategy_id", "net_pnl_day"],
            },
            "strategy_summaries": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "strategy_family", "strategy_id", "trade_count", "net_pnl_cash"],
            },
            "equity_curve": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "timestamp", "trade_index", "equity_pnl_cash"],
            },
            "drawdown_curve": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "timestamp", "trade_index", "drawdown_cash"],
            },
            "trade_blotter": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "entry_ts", "exit_ts", "pnl_cash"],
            },
            "exit_reason_breakdown": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "exit_reason", "trade_count", "net_pnl_cash"],
            },
            "session_breakdown": {
                "selection_key": "strategy_key",
                "required_fields": ["strategy_key", "session_segment", "trade_count", "net_pnl_cash"],
            },
        },
        "query_examples": {
            "one_strategy": {
                "dataset": "daily_pnl",
                "filter": {"strategy_key": strategy_catalog_rows[0]["strategy_key"] if strategy_catalog_rows else None},
            },
            "selected_many": {
                "dataset": "daily_pnl",
                "filter": {
                    "strategy_keys": [row["strategy_key"] for row in strategy_catalog_rows[:2]],
                    "combine": False,
                },
            },
            "all_combined": {
                "dataset": "daily_pnl",
                "filter": {"combine": True},
            },
        },
    }
    manifest_started = perf_counter()
    write_json_manifest(analytics_root / "manifest.json", manifest)
    manifest_seconds = perf_counter() - manifest_started
    app_payload_started = perf_counter()
    write_json_manifest(
        analytics_root / "app_payload.json",
        build_research_analytics_payload(analytics_root=analytics_root),
    )
    app_payload_seconds = perf_counter() - app_payload_started
    platform_index_started = perf_counter()
    platform_index = refresh_research_analytics_family_index(analytics_platform_root=analytics_root.parent)
    platform_index_seconds = perf_counter() - platform_index_started
    return {
        "manifest_path": str((analytics_root / "manifest.json").resolve()),
        "app_payload_path": str((analytics_root / "app_payload.json").resolve()),
        "duckdb_catalog_path": str(duckdb_path.resolve()),
        "platform_manifest_path": platform_index["manifest_path"],
        "platform_app_payload_path": platform_index["app_payload_path"],
        "timing": {
            "target_entry_load_seconds": round(targets_seconds, 6),
            "analytics_row_build_seconds": round(build_seconds, 6),
            "dataset_bundle_seconds": round(dataset_seconds, 6),
            "duckdb_seconds": round(duckdb_seconds, 6),
            "manifest_write_seconds": round(manifest_seconds, 6),
            "app_payload_write_seconds": round(app_payload_seconds, 6),
            "platform_index_seconds": round(platform_index_seconds, 6),
            "total_seconds": round(perf_counter() - started, 6),
        },
    }


def build_research_analytics_payload(*, analytics_root: Path) -> dict[str, Any]:
    return _build_research_analytics_payload_from_roots(_resolve_analytics_roots(analytics_root=analytics_root))


def build_multi_research_analytics_payload(
    *,
    analytics_roots_by_family: Mapping[str, Path],
) -> dict[str, Any]:
    return _build_research_analytics_payload_from_roots(_resolve_analytics_roots(analytics_roots_by_family=analytics_roots_by_family))


def build_discovered_research_analytics_payload(*, analytics_platform_root: Path) -> dict[str, Any]:
    platform_root = Path(analytics_platform_root).resolve()
    payload_path = platform_root / "app_payload.json"
    if payload_path.exists():
        return json.loads(payload_path.read_text(encoding="utf-8"))
    return _build_research_analytics_payload_from_roots(
        discover_research_analytics_roots(analytics_platform_root=platform_root)
    )


def query_daily_pnl(
    *,
    analytics_root: Path | None = None,
    analytics_platform_root: Path | None = None,
    analytics_roots_by_family: Mapping[str, Path] | None = None,
    strategy_ids: Sequence[str] | None = None,
    strategy_keys: Sequence[str] | None = None,
    strategy_families: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    combine: bool = False,
) -> list[dict[str, Any]]:
    rows = read_research_analytics_dataset(
        analytics_root=analytics_root,
        analytics_platform_root=analytics_platform_root,
        analytics_roots_by_family=analytics_roots_by_family,
        dataset_name="daily_pnl",
        strategy_families=strategy_families,
    )
    if not rows:
        return []
    selected_ids = set(strategy_ids or [])
    selected_keys = set(strategy_keys or [])
    filtered = [
        row
        for row in rows
        if (not selected_ids or str(row.get("strategy_id")) in selected_ids)
        and (not selected_keys or str(row.get("strategy_key") or "") in selected_keys)
        and (start_date is None or str(row.get("date")) >= start_date)
        and (end_date is None or str(row.get("date")) <= end_date)
    ]
    if not combine:
        return filtered
    aggregates: dict[str, dict[str, Any]] = {}
    for row in filtered:
        bucket = aggregates.setdefault(
            str(row["date"]),
            {
                "date": row["date"],
                "strategy_family": "combined",
                "strategy_id": "combined",
                "strategy_key": "combined",
                "strategy_label": "Combined",
                "net_pnl_day": 0.0,
                "gross_pnl_day": 0.0,
                "fees_day": 0.0,
                "slippage_day": 0.0,
                "trade_count_day": 0,
            },
        )
        bucket["net_pnl_day"] = round(float(bucket["net_pnl_day"]) + float(row.get("net_pnl_day") or 0.0), 6)
        bucket["gross_pnl_day"] = round(float(bucket["gross_pnl_day"]) + float(row.get("gross_pnl_day") or 0.0), 6)
        bucket["fees_day"] = round(float(bucket["fees_day"]) + float(row.get("fees_day") or 0.0), 6)
        bucket["slippage_day"] = round(float(bucket["slippage_day"]) + float(row.get("slippage_day") or 0.0), 6)
        bucket["trade_count_day"] = int(bucket["trade_count_day"]) + int(row.get("trade_count_day") or 0)
    return [aggregates[key] for key in sorted(aggregates)]


def read_research_analytics_dataset(
    *,
    analytics_root: Path | None = None,
    analytics_platform_root: Path | None = None,
    analytics_roots_by_family: Mapping[str, Path] | None = None,
    dataset_name: str,
    strategy_families: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    roots = _resolve_analytics_roots(
        analytics_root=analytics_root,
        analytics_platform_root=analytics_platform_root,
        analytics_roots_by_family=analytics_roots_by_family,
    )
    family_filter = set(str(family).strip() for family in (strategy_families or []) if str(family).strip())
    rows: list[dict[str, Any]] = []
    for family, root in roots.items():
        if family_filter and family not in family_filter:
            continue
        manifest = _load_manifest(root)
        if not manifest:
            continue
        dataset = as_mapping((manifest.get("datasets") or {}).get(dataset_name))
        jsonl_path = str(dataset.get("jsonl_path") or "").strip()
        if not jsonl_path:
            continue
        rows.extend(read_jsonl_dataset(Path(jsonl_path)))
    return rows


def _load_trade_records_for_target(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    manifest_path = entry.get("scope_bundle_manifest_path")
    if not manifest_path:
        return []
    path = Path(str(manifest_path))
    if not path.exists():
        return []
    manifest = json.loads(path.read_text(encoding="utf-8"))
    dataset = ((manifest.get("datasets") or {}).get("trade_records") or {})
    return read_jsonl_dataset(Path(str(dataset.get("jsonl_path") or "")))


def as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def analytics_strategy_key(*, strategy_family: str | None, strategy_id: str | None) -> str:
    family = str(strategy_family or "").strip()
    identifier = str(strategy_id or "").strip()
    if not family:
        return identifier
    if not identifier:
        return family
    return f"{family}::{identifier}"


def _daily_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    strategy_key = analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id)
    buckets: dict[str, dict[str, Any]] = {}
    for trade in trades:
        day = str(str(trade.get("exit_ts") or trade.get("entry_ts")).split("T")[0])
        bucket = buckets.setdefault(
            day,
            {
                "date": day,
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": strategy_key,
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "symbol": entry.get("symbol"),
                "allowed_sessions": entry.get("allowed_sessions"),
                "net_pnl_day": 0.0,
                "gross_pnl_day": 0.0,
                "fees_day": 0.0,
                "slippage_day": 0.0,
                "trade_count_day": 0,
            },
        )
        bucket["net_pnl_day"] = round(float(bucket["net_pnl_day"]) + float(trade.get("pnl_cash") or 0.0), 6)
        bucket["gross_pnl_day"] = round(float(bucket["gross_pnl_day"]) + float(trade.get("gross_pnl_cash") or 0.0), 6)
        bucket["fees_day"] = round(float(bucket["fees_day"]) + float(trade.get("fees_paid") or 0.0), 6)
        bucket["slippage_day"] = round(float(bucket["slippage_day"]) + float(trade.get("slippage_cost") or 0.0), 6)
        bucket["trade_count_day"] = int(bucket["trade_count_day"]) + 1
    return [buckets[key] for key in sorted(buckets)]


def _summary_row(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> dict[str, Any]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    total_net = sum(float(trade.get("pnl_cash") or 0.0) for trade in trades)
    winners = [trade for trade in trades if float(trade.get("pnl_cash") or 0.0) > 0.0]
    losers = [trade for trade in trades if float(trade.get("pnl_cash") or 0.0) <= 0.0]
    gross_profit = sum(float(trade.get("pnl_cash") or 0.0) for trade in winners)
    gross_loss = abs(sum(float(trade.get("pnl_cash") or 0.0) for trade in losers))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for trade in sorted(trades, key=lambda row: str(row.get("exit_ts") or row.get("entry_ts"))):
        equity += float(trade.get("pnl_cash") or 0.0)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "strategy_family": strategy_family,
        "family_label": _family_label(strategy_family),
        "strategy_id": strategy_id,
        "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
        "strategy_label": strategy_label,
        "target_id": entry.get("target_id"),
        "symbol": entry.get("symbol"),
        "allowed_sessions": entry.get("allowed_sessions"),
        "scope_bundle_id": entry.get("scope_bundle_id"),
        "run_id": entry.get("run_id"),
        "trade_count": len(trades),
        "net_pnl_cash": round(total_net, 6),
        "average_trade_pnl_cash": round(total_net / len(trades), 6) if trades else 0.0,
        "profit_factor": round((gross_profit / gross_loss), 6) if gross_loss else None,
        "win_rate": round((len(winners) / len(trades)) * 100.0, 6) if trades else 0.0,
        "max_drawdown": round(max_dd, 6),
    }


def _equity_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    rows: list[dict[str, Any]] = []
    equity = 0.0
    for index, trade in enumerate(sorted(trades, key=lambda row: str(row.get("exit_ts") or row.get("entry_ts"))), start=1):
        equity = round(equity + float(trade.get("pnl_cash") or 0.0), 6)
        rows.append(
            {
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "trade_index": index,
                "timestamp": trade.get("exit_ts") or trade.get("entry_ts"),
                "equity_pnl_cash": equity,
            }
        )
    return rows


def _drawdown_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    rows: list[dict[str, Any]] = []
    equity = 0.0
    peak = 0.0
    for index, trade in enumerate(sorted(trades, key=lambda row: str(row.get("exit_ts") or row.get("entry_ts"))), start=1):
        equity = round(equity + float(trade.get("pnl_cash") or 0.0), 6)
        peak = max(peak, equity)
        rows.append(
            {
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "trade_index": index,
                "timestamp": trade.get("exit_ts") or trade.get("entry_ts"),
                "equity_pnl_cash": equity,
                "drawdown_cash": round(peak - equity, 6),
            }
        )
    return rows


def _trade_blotter_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    rows: list[dict[str, Any]] = []
    for trade in trades:
        rows.append(
            {
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "instrument": trade.get("instrument"),
                "variant_id": trade.get("variant_id"),
                "decision_id": trade.get("decision_id"),
                "entry_ts": trade.get("entry_ts"),
                "exit_ts": trade.get("exit_ts"),
                "entry_price": trade.get("entry_price"),
                "exit_price": trade.get("exit_price"),
                "pnl_cash": trade.get("pnl_cash"),
                "gross_pnl_cash": trade.get("gross_pnl_cash"),
                "fees_paid": trade.get("fees_paid"),
                "slippage_cost": trade.get("slippage_cost"),
                "mfe_points": trade.get("mfe_points"),
                "mae_points": trade.get("mae_points"),
                "exit_reason": trade.get("exit_reason"),
                "session_segment": trade.get("session_segment"),
                "regime_bucket": trade.get("regime_bucket"),
                "volatility_bucket": trade.get("volatility_bucket"),
                "bars_held_1m": trade.get("bars_held_1m"),
                "hold_minutes": trade.get("hold_minutes"),
            }
        )
    return rows


def _exit_reason_breakdown_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    counts: dict[str, dict[str, Any]] = {}
    for trade in trades:
        exit_reason = str(trade.get("exit_reason") or "UNKNOWN")
        bucket = counts.setdefault(
            exit_reason,
            {
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "exit_reason": exit_reason,
                "trade_count": 0,
                "net_pnl_cash": 0.0,
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"]) + 1
        bucket["net_pnl_cash"] = round(float(bucket["net_pnl_cash"]) + float(trade.get("pnl_cash") or 0.0), 6)
    return [counts[key] for key in sorted(counts)]


def _session_breakdown_rows(*, trades: Sequence[Mapping[str, Any]], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    strategy_family = str(entry.get("strategy_family") or "unknown")
    strategy_id = _strategy_id(entry)
    strategy_label = _strategy_label(entry)
    counts: dict[str, dict[str, Any]] = {}
    for trade in trades:
        session_segment = str(trade.get("session_segment") or "UNKNOWN")
        bucket = counts.setdefault(
            session_segment,
            {
                "strategy_family": strategy_family,
                "family_label": _family_label(strategy_family),
                "strategy_id": strategy_id,
                "strategy_key": analytics_strategy_key(strategy_family=strategy_family, strategy_id=strategy_id),
                "strategy_label": strategy_label,
                "target_id": entry.get("target_id"),
                "session_segment": session_segment,
                "trade_count": 0,
                "net_pnl_cash": 0.0,
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"]) + 1
        bucket["net_pnl_cash"] = round(float(bucket["net_pnl_cash"]) + float(trade.get("pnl_cash") or 0.0), 6)
    return [counts[key] for key in sorted(counts)]


def _strategy_id(entry: Mapping[str, Any]) -> str:
    return str(entry.get("strategy_variant") or entry.get("target_id") or "").strip()


def _strategy_label(entry: Mapping[str, Any]) -> str:
    return str(entry.get("label") or entry.get("target_id") or entry.get("strategy_variant") or "").strip()


def _family_label(strategy_family: str | None) -> str:
    family = str(strategy_family or "").strip()
    if not family:
        return "Unknown"
    return family.replace("_", " ").strip().title()


def _strategy_family_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("strategy_family") or "unknown")] += 1
    return dict(sorted(counts.items()))


def _resolve_analytics_roots(
    *,
    analytics_root: Path | None = None,
    analytics_platform_root: Path | None = None,
    analytics_roots_by_family: Mapping[str, Path] | None = None,
) -> dict[str, Path]:
    if analytics_roots_by_family:
        return {
            str(family).strip(): Path(root).resolve()
            for family, root in analytics_roots_by_family.items()
            if str(family).strip()
        }
    if analytics_platform_root is not None:
        return discover_research_analytics_roots(analytics_platform_root=analytics_platform_root)
    if analytics_root is None:
        return {}
    resolved = Path(analytics_root).resolve()
    family = str(resolved.name or "unknown").strip()
    return {family: resolved}


def discover_research_analytics_roots(*, analytics_platform_root: Path) -> dict[str, Path]:
    platform_root = Path(analytics_platform_root).resolve()
    manifest = _load_platform_manifest(platform_root)
    if manifest:
        discovered: dict[str, Path] = {}
        for row in list(manifest.get("families") or []):
            family = str(row.get("strategy_family") or "").strip()
            root = str(row.get("analytics_root") or "").strip()
            if family and root:
                discovered[family] = Path(root).resolve()
        if discovered:
            return dict(sorted(discovered.items()))

    discovered = {}
    if not platform_root.exists():
        return discovered
    for child in sorted(platform_root.iterdir()):
        if not child.is_dir():
            continue
        manifest = _load_manifest(child)
        if not manifest:
            continue
        family = str(manifest.get("strategy_family") or child.name or "").strip()
        if family:
            discovered[family] = child.resolve()
    return dict(sorted(discovered.items()))


def refresh_research_analytics_family_index(*, analytics_platform_root: Path) -> dict[str, Any]:
    platform_root = Path(analytics_platform_root).resolve()
    platform_root.mkdir(parents=True, exist_ok=True)
    family_roots = _scan_family_roots(platform_root)
    families: list[dict[str, Any]] = []
    for declared_family, root in family_roots.items():
        manifest = _load_manifest(root)
        if not manifest:
            continue
        family_name = str(manifest.get("strategy_family") or declared_family or root.name or "unknown").strip()
        datasets = {
            name: {
                "jsonl_path": spec.get("jsonl_path"),
                "parquet_path": spec.get("parquet_path"),
                "row_count": int(spec.get("row_count") or 0),
            }
            for name, spec in (manifest.get("datasets") or {}).items()
        }
        families.append(
            {
                "strategy_family": family_name,
                "family_label": _family_label(family_name),
                "analytics_root": str(root.resolve()),
                "manifest_path": str((root / "manifest.json").resolve()),
                "app_payload_path": str((root / "app_payload.json").resolve()),
                "generated_at": manifest.get("generated_at"),
                "contract_version": manifest.get("contract_version"),
                "strategy_count": int(manifest.get("strategy_count") or 0),
                "family_metadata": json_ready(dict(manifest.get("family_metadata") or {})),
                "datasets": datasets,
                "tenant_class": _tenant_class(datasets),
                "full_app_compatible": _tenant_class(datasets) == "full_app_tenant",
                "default_app_visible": bool((manifest.get("family_metadata") or {}).get("default_app_visible")),
            }
        )
    families.sort(key=lambda row: str(row.get("strategy_family") or ""))
    roots = {
        str(row["strategy_family"]): Path(str(row["analytics_root"])).resolve()
        for row in families
    }
    combined_payload = _build_research_analytics_payload_from_roots(roots)
    manifest = {
        "artifact_version": ANALYTICS_FAMILY_INDEX_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "contract_version": ANALYTICS_CONTRACT_VERSION,
        "family_count": len(families),
        "families": families,
        "default_family_views": _default_family_views(families),
        "shared_app_contract": shared_research_analytics_contract(),
    }
    write_json_manifest(platform_root / "manifest.json", manifest)
    write_json_manifest(platform_root / "app_payload.json", combined_payload)
    return {
        "manifest_path": str((platform_root / "manifest.json").resolve()),
        "app_payload_path": str((platform_root / "app_payload.json").resolve()),
        "family_count": len(families),
    }


def shared_research_analytics_contract() -> dict[str, Any]:
    return {
        "contract_version": ANALYTICS_CONTRACT_VERSION,
        "identity_fields": {
            "required": ["strategy_key", "strategy_family", "strategy_id", "strategy_label"],
            "optional": ["target_id", "symbol", "allowed_sessions", "scope_bundle_id", "run_id"],
        },
        "tenant_classes": {
            "full_app_tenant": {
                "required_datasets": list(FULL_APP_REQUIRED_DATASETS),
                "optional_datasets": list(APP_COMPATIBLE_OPTIONAL_DATASETS),
                "description": "Supports calendar, deep-dive, and strategy-analysis surfaces without replay fallback.",
            },
            "analytics_lite_tenant": {
                "required_datasets": ["strategy_catalog", "strategy_summaries"],
                "optional_datasets": [name for name in RESEARCH_ANALYTICS_DATASETS if name not in {"strategy_catalog", "strategy_summaries"}],
                "description": "Visible in registry/app discovery, but some app surfaces may remain summary-only.",
            },
        },
        "dataset_types": {
            "required_for_app_compatibility": list(FULL_APP_REQUIRED_DATASETS),
            "optional_for_extended_breakdowns": list(APP_COMPATIBLE_OPTIONAL_DATASETS),
        },
        "family_metadata_fields": {
            "required": [],
            "optional": [
                "default_app_visible",
                "publication_mode",
                "view_role",
                "diagnostic_only",
                "time_horizon",
                "operating_policy_version",
            ],
        },
    }


def _load_manifest(analytics_root: Path) -> dict[str, Any] | None:
    manifest_path = analytics_root.resolve() / "manifest.json"
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(payload.get("artifact_version") or "") != ANALYTICS_ARTIFACT_VERSION:
        return None
    if "strategy_catalog" not in (payload.get("datasets") or {}):
        return None
    return payload


def _load_platform_manifest(analytics_platform_root: Path) -> dict[str, Any] | None:
    manifest_path = Path(analytics_platform_root).resolve() / "manifest.json"
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(payload.get("artifact_version") or "") != ANALYTICS_FAMILY_INDEX_VERSION:
        return None
    return payload


def _scan_family_roots(platform_root: Path) -> dict[str, Path]:
    discovered: dict[str, Path] = {}
    if not platform_root.exists():
        return discovered
    for child in sorted(platform_root.iterdir()):
        if not child.is_dir():
            continue
        manifest = _load_manifest(child)
        if not manifest:
            continue
        family = str(manifest.get("strategy_family") or child.name or "").strip()
        if family:
            discovered[family] = child.resolve()
    return discovered


def _tenant_class(datasets: Mapping[str, Mapping[str, Any]]) -> str:
    dataset_names = {str(name).strip() for name in datasets.keys() if str(name).strip()}
    if all(name in dataset_names for name in FULL_APP_REQUIRED_DATASETS):
        return "full_app_tenant"
    return "analytics_lite_tenant"


def _default_family_views(families: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family in families:
        metadata = as_mapping(family.get("family_metadata"))
        if not bool(metadata.get("default_app_visible")):
            continue
        rows.append(
            {
                "strategy_family": family.get("strategy_family"),
                "family_label": family.get("family_label"),
                "publication_mode": metadata.get("publication_mode"),
                "view_role": metadata.get("view_role"),
                "time_horizon": metadata.get("time_horizon"),
            }
        )
    return rows


def _build_research_analytics_payload_from_roots(analytics_roots_by_family: Mapping[str, Path]) -> dict[str, Any]:
    family_payloads: list[dict[str, Any]] = []
    combined_catalog: list[dict[str, Any]] = []
    combined_summaries: list[dict[str, Any]] = []
    combined_datasets: dict[str, dict[str, Any]] = {}
    generated_at_values: list[str] = []
    for declared_family, root in analytics_roots_by_family.items():
        manifest = _load_manifest(root)
        if not manifest:
            continue
        strategy_rows = read_jsonl_dataset(Path(manifest["datasets"]["strategy_catalog"]["jsonl_path"]))
        summary_rows = read_jsonl_dataset(Path(manifest["datasets"]["strategy_summaries"]["jsonl_path"]))
        family_name = str(manifest.get("strategy_family") or declared_family or root.name or "unknown")
        generated_at = str(manifest.get("generated_at") or "")
        if generated_at:
            generated_at_values.append(generated_at)
        family_payloads.append(
            {
                "strategy_family": family_name,
                "family_label": _family_label(family_name),
                "analytics_root": str(root.resolve()),
                "generated_at": generated_at,
                "strategy_count": len(strategy_rows),
                "family_metadata": json_ready(dict(manifest.get("family_metadata") or {})),
                "tenant_class": _tenant_class(manifest.get("datasets") or {}),
                "full_app_compatible": _tenant_class(manifest.get("datasets") or {}) == "full_app_tenant",
                "default_app_visible": bool((manifest.get("family_metadata") or {}).get("default_app_visible")),
                "datasets": {
                    name: {
                        "jsonl_path": spec["jsonl_path"],
                        "parquet_path": spec["parquet_path"],
                        "row_count": spec["row_count"],
                    }
                    for name, spec in (manifest.get("datasets") or {}).items()
                },
            }
        )
        combined_catalog.extend(strategy_rows)
        combined_summaries.extend(summary_rows)
        for dataset_name, spec in (manifest.get("datasets") or {}).items():
            combined = combined_datasets.setdefault(
                dataset_name,
                {
                    "row_count": 0,
                    "families": [],
                },
            )
            combined["row_count"] = int(combined["row_count"]) + int(spec.get("row_count") or 0)
            if family_name not in combined["families"]:
                combined["families"].append(family_name)
    if not family_payloads:
        return {"available": False, "reason": "research analytics not materialized"}
    for dataset in combined_datasets.values():
        dataset["families"] = sorted(dataset["families"])
    family_payloads.sort(key=lambda row: str(row.get("strategy_family") or ""))
    return {
        "available": True,
        "artifact_version": ANALYTICS_ARTIFACT_VERSION,
        "contract_version": ANALYTICS_CONTRACT_VERSION,
        "generated_at": max(generated_at_values) if generated_at_values else None,
        "strategy_count": len(combined_catalog),
        "strategy_family_counts": _strategy_family_counts(combined_catalog),
        "strategy_catalog": combined_catalog,
        "strategy_summaries": combined_summaries,
        "datasets": combined_datasets,
        "families": family_payloads,
        "default_family_views": _default_family_views(family_payloads),
        "selection_key": "strategy_key",
        "selection_modes": {
            "one": "Filter dataset rows by one strategy_key or strategy_id.",
            "many": "Filter dataset rows by provided strategy_key or strategy_id lists.",
            "all": "Use aggregate helpers to combine all matching strategies across families.",
        },
        "app_surfaces": {
            "pnl_calendar_dataset": "daily_pnl",
            "strategy_deep_dive_datasets": [
                "strategy_summaries",
                "equity_curve",
                "drawdown_curve",
                "trade_blotter",
                "exit_reason_breakdown",
                "session_breakdown",
            ],
        },
        "shared_contract": shared_research_analytics_contract(),
        "api_base_path": "/api/research-analytics",
    }
