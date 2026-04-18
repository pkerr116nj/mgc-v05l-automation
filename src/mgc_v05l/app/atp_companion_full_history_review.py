"""Full-history ATP Companion candidate review over the shared GC/MGC span."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .strategy_universe_retest import (
    _discover_best_sources,
    _evaluate_atp_lane,
)
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.atp_promotion_add_review import (
    _candidate_session_acceptability,
    _candidate_session_breakdown,
    _trade_window_bars,
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.platform import ensure_symbol_context_bundle, last_source_discovery_metadata, stable_hash, write_json_manifest
from ..research.platform.analytics import build_research_analytics_views
from ..research.platform.registry import build_registry_run_row, register_experiment_run
from ..research.trend_participation.performance_validation import _trade_metrics
from ..research.trend_participation.substrate import (
    ATP_CANDIDATE_VERSION,
    ATP_FEATURE_VERSION,
    ATP_OUTCOME_ENGINE_VERSION,
    ensure_atp_feature_bundle,
    ensure_atp_scope_bundle,
)
from ..research.trend_participation.experiment_configs import FullHistoryReviewConfig, config_payload

REPO_ROOT = Path.cwd()
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports"
DEFAULT_RESEARCH_PLATFORM_ROOT = REPO_ROOT / "outputs" / "research_platform"
DEFAULT_PLATFORM_SUBSTRATE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "atp_substrate"
DEFAULT_EXPERIMENT_REGISTRY_ROOT = DEFAULT_RESEARCH_PLATFORM_ROOT / "registry"
DEFAULT_RESEARCH_ANALYTICS_ROOT = DEFAULT_RESEARCH_PLATFORM_ROOT / "analytics" / "atp_companion"


@dataclass(frozen=True)
class EvaluationTarget:
    target_id: str
    label: str
    symbol: str
    allowed_sessions: tuple[str, ...]
    point_value: float
    target_kind: str
    candidate_id: str | None = None
    config_path: str | None = None
    lane_id: str | None = None
    standalone_strategy_id: str | None = None


@dataclass(frozen=True)
class MaterializedSymbolTruth:
    symbol: str
    source_db: Path
    selected_sources: dict[str, Any]
    start_timestamp: datetime
    end_timestamp: datetime
    bars_1m: Sequence[Any]
    rolling_scope_feature_rows: Sequence[Any]
    load_seconds: float
    feature_seconds: float
    feature_bundle_id: str
    context_bundle_id: str | None


@dataclass(frozen=True)
class MaterializedScopeTruth:
    symbol: str
    allowed_sessions: tuple[str, ...]
    point_value: float
    bars_1m: Sequence[Any]
    trade_rows: Sequence[dict[str, Any]]
    evaluation_seconds: float
    entry_state_seconds: float
    timing_state_seconds: float
    trade_rebuild_seconds: float
    bar_count: int
    scope_bundle_id: str


def build_targets() -> list[EvaluationTarget]:
    return [
        EvaluationTarget(
            target_id="atp_companion_v1__benchmark_mgc_asia_us",
            label="Frozen ATP Companion Baseline v1 / MGC / Asia+US",
            symbol="MGC",
            allowed_sessions=("ASIA", "US"),
            point_value=10.0,
            target_kind="frozen_benchmark",
            config_path="config/atp_companion_baseline_v1_asia_us.yaml",
            lane_id="atp_companion_v1_asia_us",
            standalone_strategy_id="atp_companion_v1__benchmark_mgc_asia_us",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__promotion_1_050r_neutral_plus",
            label="ATP Companion / promotion_1_050r_neutral_plus",
            symbol="MGC",
            allowed_sessions=("ASIA", "US"),
            point_value=10.0,
            target_kind="promotion_add_candidate",
            candidate_id="promotion_1_050r_neutral_plus",
            config_path="config/atp_promotion_add_candidate_registry.yaml",
            standalone_strategy_id="atp_companion_v1__promotion_1_050r_neutral_plus",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__promotion_1_075r_neutral_plus",
            label="ATP Companion / promotion_1_075r_neutral_plus",
            symbol="MGC",
            allowed_sessions=("ASIA", "US"),
            point_value=10.0,
            target_kind="promotion_add_candidate",
            candidate_id="promotion_1_075r_neutral_plus",
            config_path="config/atp_promotion_add_candidate_registry.yaml",
            standalone_strategy_id="atp_companion_v1__promotion_1_075r_neutral_plus",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__promotion_1_075r_favorable_only",
            label="ATP Companion / promotion_1_075r_favorable_only",
            symbol="MGC",
            allowed_sessions=("ASIA", "US"),
            point_value=10.0,
            target_kind="promotion_add_candidate",
            candidate_id="promotion_1_075r_favorable_only",
            config_path="config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml",
            standalone_strategy_id="atp_companion_v1__promotion_1_075r_favorable_only",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__candidate_gc_asia_us",
            label="ATP Companion Candidate v1 / GC / Asia+US",
            symbol="GC",
            allowed_sessions=("ASIA", "US"),
            point_value=100.0,
            target_kind="lane_candidate",
            config_path="config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml",
            lane_id="atp_companion_v1_gc_asia_us",
            standalone_strategy_id="atp_companion_v1__candidate_gc_asia_us",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
            label="ATP Companion Candidate / GC / Asia Only / promotion_1_075r_favorable_only",
            symbol="GC",
            allowed_sessions=("ASIA",),
            point_value=100.0,
            target_kind="promotion_add_lane_variant",
            candidate_id="promotion_1_075r_favorable_only",
            config_path="config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_promotion_1_075r_favorable_only.yaml",
            lane_id="atp_companion_v1_gc_asia_promotion_1_075r_favorable_only",
            standalone_strategy_id="atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only",
        ),
        EvaluationTarget(
            target_id="atp_companion_v1__mgc_asia__promotion_1_075r_favorable_only",
            label="ATP Companion Candidate / MGC / Asia Only / promotion_1_075r_favorable_only",
            symbol="MGC",
            allowed_sessions=("ASIA",),
            point_value=10.0,
            target_kind="promotion_add_lane_variant",
            candidate_id="promotion_1_075r_favorable_only",
            config_path="config/probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml",
            lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
            standalone_strategy_id="atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only",
        ),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-full-history-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument(
        "--mode",
        choices=("optimized", "legacy"),
        default="optimized",
        help="Use the optimized materialized-current-truth path or the legacy direct review path.",
    )
    return parser


def _date_key(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    return datetime.fromisoformat(str(value)).date().isoformat()


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _source_selection_coverage_row(selection: Any) -> dict[str, Any]:
    if selection is None:
        return {
            "symbol": None,
            "timeframe": None,
            "data_source": None,
            "start_timestamp": None,
            "end_timestamp": None,
            "row_count": 0,
        }
    return {
        "symbol": getattr(selection, "symbol", None),
        "timeframe": getattr(selection, "timeframe", None),
        "data_source": getattr(selection, "data_source", None),
        "start_timestamp": getattr(selection, "start_ts", None),
        "end_timestamp": getattr(selection, "end_ts", None),
        "row_count": int(getattr(selection, "row_count", 0) or 0),
    }


def _shared_1m_coverage_from_source_index(
    *,
    bar_source_index: dict[str, dict[str, Any]],
    instruments: tuple[str, ...],
) -> tuple[datetime, datetime]:
    starts: list[datetime] = []
    ends: list[datetime] = []
    for symbol in instruments:
        selection = bar_source_index.get(symbol, {}).get("1m")
        if selection is None or not getattr(selection, "start_ts", None) or not getattr(selection, "end_ts", None):
            raise ValueError(f"Missing 1m coverage metadata for {symbol}")
        starts.append(datetime.fromisoformat(str(selection.start_ts)))
        ends.append(datetime.fromisoformat(str(selection.end_ts)))
    return max(starts), min(ends)


def _base_position_rows(trade_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in trade_rows:
        trade = row["trade_record"]
        rows.append(
            {
                "trade_id": row["trade_id"],
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "decision_ts": trade.decision_ts,
                "entry_price": float(trade.entry_price),
                "exit_price": float(trade.exit_price),
                "stop_price": float(trade.stop_price),
                "pnl_cash": float(trade.pnl_cash),
                "mfe_points": float(trade.mfe_points),
                "mae_points": float(trade.mae_points),
                "hold_minutes": float(trade.hold_minutes),
                "bars_held_1m": int(trade.bars_held_1m),
                "side": trade.side,
                "session_segment": trade.session_segment,
                "family": trade.family,
                "exit_reason": trade.exit_reason,
                "added": False,
                "add_pnl_cash": 0.0,
                "add_reason": None,
                "add_price_quality_state": None,
            }
        )
    return rows


def _candidate_position_rows(
    *,
    candidate,
    bars_1m: Sequence[Any],
    trade_rows: Sequence[dict[str, Any]],
    point_value: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(bars_1m)}
    for row in trade_rows:
        trade = row["trade_record"]
        trade_bars = _trade_window_bars(trade=trade, bars_1m=bars_1m, bars_by_end_ts=bars_by_end_ts)
        rows.append(
            evaluate_promotion_add_candidate(
                trade=trade,
                minute_bars=trade_bars,
                candidate=candidate,
                point_value=point_value,
            )
        )
    return rows


def _trade_windows_by_id(
    *,
    bars_1m: Sequence[Any],
    trade_rows: Sequence[dict[str, Any]],
) -> dict[str, list[Any]]:
    bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(bars_1m)}
    windows: dict[str, list[Any]] = {}
    for row in trade_rows:
        trade = row["trade_record"]
        windows[str(row["trade_id"])] = _trade_window_bars(
            trade=trade,
            bars_1m=bars_1m,
            bars_by_end_ts=bars_by_end_ts,
        )
    return windows


def _candidate_position_rows_from_windows(
    *,
    candidate,
    trade_rows: Sequence[dict[str, Any]],
    trade_windows_by_id: dict[str, list[Any]],
    point_value: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in trade_rows:
        trade = row["trade_record"]
        rows.append(
            evaluate_promotion_add_candidate(
                trade=trade,
                minute_bars=trade_windows_by_id.get(str(row["trade_id"])) or [],
                candidate=candidate,
                point_value=point_value,
            )
        )
    return rows


def _normalize_session_breakdown(session_breakdown: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for session in ("ASIA", "US"):
        row = dict(session_breakdown.get(session) or {})
        normalized[session] = {
            "trade_count": int(row.get("trade_count") or 0),
            "add_count": int(row.get("add_count") or 0),
            "net_pnl_cash": round(float(row.get("net_pnl_cash") or 0.0), 4),
            "add_contribution_net_pnl_cash": round(float(row.get("add_contribution_net_pnl_cash") or 0.0), 4),
        }
    return normalized


def _add_only_metrics(rows: Sequence[dict[str, Any]], *, bar_count: int) -> dict[str, Any]:
    add_only_rows = [
        {
            "entry_ts": row.get("add_entry_ts"),
            "decision_ts": row.get("decision_ts"),
            "pnl_cash": row.get("add_pnl_cash"),
            "mfe_points": 0.0,
            "mae_points": 0.0,
            "hold_minutes": row.get("add_hold_minutes"),
            "bars_held_1m": row.get("bars_held_1m"),
            "side": row.get("side"),
            "session_segment": row.get("session_segment"),
        }
        for row in rows
        if row.get("added")
    ]
    return _trade_metrics(add_only_rows, bar_count=bar_count)


def _daily_rows(
    *,
    position_rows: Sequence[dict[str, Any]],
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> list[dict[str, Any]]:
    rows_by_date: dict[str, dict[str, Any]] = {}
    for row in position_rows:
        exit_ts = row.get("exit_ts")
        day = _date_key(exit_ts)
        if day is None:
            continue
        bucket = rows_by_date.setdefault(
            day,
            {
                "date": day,
                "trade_count": 0,
                "net_pnl_cash": 0.0,
                "add_count": 0,
                "add_only_net_pnl_cash": 0.0,
                "winning_trades": 0,
                "losing_trades": 0,
            },
        )
        pnl = float(row.get("pnl_cash") or 0.0)
        bucket["trade_count"] += 1
        bucket["net_pnl_cash"] = round(bucket["net_pnl_cash"] + pnl, 4)
        if row.get("added"):
            bucket["add_count"] += 1
            bucket["add_only_net_pnl_cash"] = round(
                bucket["add_only_net_pnl_cash"] + float(row.get("add_pnl_cash") or 0.0),
                4,
            )
        if pnl > 0.0:
            bucket["winning_trades"] += 1
        elif pnl < 0.0:
            bucket["losing_trades"] += 1

    daily_rows: list[dict[str, Any]] = []
    cursor = start_timestamp.date()
    end_date = end_timestamp.date()
    cumulative = 0.0
    while cursor <= end_date:
        key = cursor.isoformat()
        base = dict(rows_by_date.get(key) or {
            "date": key,
            "trade_count": 0,
            "net_pnl_cash": 0.0,
            "add_count": 0,
            "add_only_net_pnl_cash": 0.0,
            "winning_trades": 0,
            "losing_trades": 0,
        })
        cumulative = round(cumulative + float(base["net_pnl_cash"]), 4)
        base["cumulative_net_pnl_cash"] = cumulative
        daily_rows.append(base)
        cursor += timedelta(days=1)
    return daily_rows


def _delta_vs_baseline(metrics: dict[str, Any], baseline_metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_count_delta": int(metrics["total_trades"]) - int(baseline_metrics["total_trades"]),
        "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
        "average_trade_pnl_cash_delta": round(
            float(metrics["average_trade_pnl_cash"]) - float(baseline_metrics["average_trade_pnl_cash"]),
            4,
        ),
        "profit_factor_delta": round(float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 4),
        "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
        "win_rate_delta": round(float(metrics["win_rate"]) - float(baseline_metrics["win_rate"]), 4),
    }


def _result_payload(
    *,
    target: EvaluationTarget,
    position_rows: Sequence[dict[str, Any]],
    bar_count: int,
    start_timestamp: datetime,
    end_timestamp: datetime,
    baseline_metrics: dict[str, Any],
) -> dict[str, Any]:
    metrics = _trade_metrics(position_rows, bar_count=bar_count)
    session_breakdown = _normalize_session_breakdown(_candidate_session_breakdown(position_rows))
    add_count = sum(1 for row in position_rows if row.get("added"))
    add_only_metrics = _add_only_metrics(position_rows, bar_count=bar_count)
    return {
        "target_id": target.target_id,
        "label": target.label,
        "target_kind": target.target_kind,
        "symbol": target.symbol,
        "allowed_sessions": list(target.allowed_sessions),
        "point_value": target.point_value,
        "candidate_id": target.candidate_id,
        "lane_id": target.lane_id,
        "standalone_strategy_id": target.standalone_strategy_id,
        "config_path": target.config_path,
        "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        "date_span": {
            "start_timestamp": _serialize_datetime(start_timestamp),
            "end_timestamp": _serialize_datetime(end_timestamp),
        },
        "metrics": metrics,
        "add_count": add_count,
        "add_rate_percent": round((add_count / metrics["total_trades"]) * 100.0, 4) if metrics["total_trades"] else 0.0,
        "add_only_metrics": add_only_metrics,
        "session_breakdown": session_breakdown,
        "delta_vs_frozen_benchmark": _delta_vs_baseline(metrics, baseline_metrics),
        "daily_rows": _daily_rows(position_rows=position_rows, start_timestamp=start_timestamp, end_timestamp=end_timestamp),
        "position_rows": [
            {
                key: _json_ready(value)
                for key, value in row.items()
                if key not in {"candidate_label"}
            }
            for row in position_rows
        ],
    }


def _promotion_recommendation(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    promotion_rows = [row for row in results if row["target_kind"] in {"promotion_add_candidate", "promotion_add_lane_variant"}]
    if not promotion_rows:
        return {"recommended_target_id": None, "verdict": "NO_PROMOTION_ROWS", "reason": "No promotion rows were evaluated."}
    ranked = sorted(
        promotion_rows,
        key=lambda row: (
            float(row["delta_vs_frozen_benchmark"]["net_pnl_cash_delta"]),
            float(row["metrics"]["profit_factor"]),
            -float(row["metrics"]["max_drawdown"]),
        ),
        reverse=True,
    )
    best = ranked[0]
    operational_note = None
    if best["target_kind"] != "lane_candidate":
        session_acceptability = _candidate_session_acceptability(
            {
                "session_breakdown": best["session_breakdown"],
                "quality_verdict": "QUALITY_IMPROVED"
                if float(best["delta_vs_frozen_benchmark"]["net_pnl_cash_delta"]) > 0.0
                else "QUANTITY_UP_QUALITY_MIXED",
            }
        )
        operational_note = session_acceptability
    return {
        "recommended_target_id": best["target_id"],
        "recommended_label": best["label"],
        "verdict": "PROMOTE_RESEARCH_FAVORITE" if float(best["delta_vs_frozen_benchmark"]["net_pnl_cash_delta"]) > 0.0 else "NO_PROMOTION_YET",
        "reason": (
            f"Highest net cash delta vs frozen benchmark among requested branches ({best['delta_vs_frozen_benchmark']['net_pnl_cash_delta']})."
        ),
        "operational_validity": operational_note,
    }


def _comparison_rows(results: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in results:
        metrics = row["metrics"]
        add_only = row["add_only_metrics"]
        delta = row["delta_vs_frozen_benchmark"]
        rows.append(
            {
                "rank_label": row["label"],
                "target_id": row["target_id"],
                "target_kind": row["target_kind"],
                "symbol": row["symbol"],
                "sessions": "/".join(row["allowed_sessions"]),
                "trade_count": metrics["total_trades"],
                "net_pnl_cash": metrics["net_pnl_cash"],
                "average_trade_pnl_cash": metrics["average_trade_pnl_cash"],
                "profit_factor": metrics["profit_factor"],
                "max_drawdown": metrics["max_drawdown"],
                "win_rate": metrics["win_rate"],
                "add_count": row["add_count"],
                "add_rate_percent": row["add_rate_percent"],
                "add_only_net_pnl_cash": add_only["net_pnl_cash"],
                "asia_net_pnl_cash": row["session_breakdown"]["ASIA"]["net_pnl_cash"],
                "us_net_pnl_cash": row["session_breakdown"]["US"]["net_pnl_cash"],
                "delta_vs_benchmark_net_pnl_cash": delta["net_pnl_cash_delta"],
                "delta_vs_benchmark_profit_factor": delta["profit_factor_delta"],
                "execution_model": row["execution_model"],
            }
        )
    return sorted(rows, key=lambda item: float(item["delta_vs_benchmark_net_pnl_cash"]), reverse=True)


def _timing_payload(
    *,
    started_at: float,
    discovery_seconds: float,
    coverage_seconds: float,
    candidate_registry_seconds: float,
    symbol_truths: dict[str, MaterializedSymbolTruth],
    scope_truths: dict[tuple[str, tuple[str, ...]], MaterializedScopeTruth],
    overlay_seconds_by_target: dict[str, float],
    report_write_seconds: float,
) -> dict[str, Any]:
    scope_rows = []
    for key, truth in sorted(scope_truths.items()):
        scope_rows.append(
            {
                "symbol": truth.symbol,
                "allowed_sessions": list(truth.allowed_sessions),
                "bar_count": truth.bar_count,
                "evaluation_seconds": round(truth.evaluation_seconds, 6),
                "entry_state_seconds": round(truth.entry_state_seconds, 6),
                "timing_state_seconds": round(truth.timing_state_seconds, 6),
                "trade_rebuild_seconds": round(truth.trade_rebuild_seconds, 6),
                "scope_bundle_id": truth.scope_bundle_id,
            }
        )
    symbol_rows = []
    for symbol, truth in sorted(symbol_truths.items()):
        symbol_rows.append(
            {
                "symbol": symbol,
                "bar_count_1m": len(truth.bars_1m),
                "load_seconds": round(truth.load_seconds, 6),
                "feature_seconds": round(truth.feature_seconds, 6),
                "feature_bundle_id": truth.feature_bundle_id,
            }
        )
    return {
        "total_wall_seconds": round(perf_counter() - started_at, 6),
        "source_discovery_seconds": round(discovery_seconds, 6),
        "coverage_seconds": round(coverage_seconds, 6),
        "candidate_registry_seconds": round(candidate_registry_seconds, 6),
        "symbol_materialization": symbol_rows,
        "scope_evaluation": scope_rows,
        "candidate_overlay_seconds_by_target": {
            key: round(value, 6) for key, value in sorted(overlay_seconds_by_target.items())
        },
        "artifact_write_seconds": round(report_write_seconds, 6),
    }


def _write_review_artifacts(
    *,
    payload: dict[str, Any],
    output_dir: Path,
    source_db: Path,
    run_start: datetime,
    run_end: datetime,
) -> dict[str, Any]:
    comparison_rows = payload["comparison_rows"]
    review_config_payload = dict(payload.get("methodology", {}).get("review_config") or {})
    json_path = output_dir / "atp_companion_full_history_review.json"
    markdown_path = output_dir / "atp_companion_full_history_review.md"
    comparison_csv_path = output_dir / "atp_companion_full_history_comparison.csv"
    materialized_baseline_path = output_dir / "atp_companion_materialized_baseline_truth.json"
    manifest_path = output_dir / "atp_companion_full_history_manifest.json"

    json_write_started = perf_counter()
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_write_seconds = perf_counter() - json_write_started

    markdown_lines = [
        "# ATP Companion Full-History Candidate Review",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Execution model: `ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP`",
        f"- Frozen benchmark semantics changed: `False`",
        f"- Review mode: `{payload['methodology'].get('mode')}`",
        f"- Wall time seconds: `{payload['timing']['total_wall_seconds']}`",
        "",
        "## Comparison Table",
        "",
        *_markdown_table(comparison_rows),
        "",
        "## Recommendation",
        f"- Recommended target: `{payload['recommendation'].get('recommended_target_id')}`",
        f"- Verdict: `{payload['recommendation'].get('verdict')}`",
        f"- Reason: `{payload['recommendation'].get('reason')}`",
        "",
        "## Timing",
        f"- Source discovery: `{payload['timing']['source_discovery_seconds']}`",
        f"- Coverage: `{payload['timing']['coverage_seconds']}`",
        f"- Candidate registry: `{payload['timing']['candidate_registry_seconds']}`",
        f"- Artifact writing: `{payload['timing']['artifact_write_seconds']}`",
        "",
        "## Targets",
    ]
    for row in payload["targets"]:
        metrics = row["metrics"]
        add_only = row["add_only_metrics"]
        delta = row["delta_vs_frozen_benchmark"]
        markdown_lines.extend(
            [
                f"### {row['label']}",
                f"- Target id: `{row['target_id']}`",
                f"- Kind: `{row['target_kind']}`",
                f"- Symbol / sessions: `{row['symbol']}` / `{'/'.join(row['allowed_sessions'])}`",
                f"- Trades: `{metrics['total_trades']}`",
                f"- Net P&L cash: `{metrics['net_pnl_cash']}`",
                f"- Average trade P&L cash: `{metrics['average_trade_pnl_cash']}`",
                f"- Profit factor: `{metrics['profit_factor']}`",
                f"- Max drawdown: `{metrics['max_drawdown']}`",
                f"- Win rate: `{metrics['win_rate']}`",
                f"- Add count / rate: `{row['add_count']}` / `{row['add_rate_percent']}`",
                f"- Add-only net P&L cash: `{add_only['net_pnl_cash']}`",
                f"- Asia session net / adds: `{row['session_breakdown']['ASIA']['net_pnl_cash']}` / `{row['session_breakdown']['ASIA']['add_count']}`",
                f"- US session net / adds: `{row['session_breakdown']['US']['net_pnl_cash']}` / `{row['session_breakdown']['US']['add_count']}`",
                f"- Delta vs frozen benchmark net / PF / drawdown: `{delta['net_pnl_cash_delta']}` / `{delta['profit_factor_delta']}` / `{delta['max_drawdown_delta']}`",
                "",
            ]
        )
    markdown_started = perf_counter()
    markdown_path.write_text("\n".join(markdown_lines).strip() + "\n", encoding="utf-8")
    markdown_seconds = perf_counter() - markdown_started

    csv_started = perf_counter()
    with comparison_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
        writer.writeheader()
        writer.writerows(comparison_rows)
    csv_seconds = perf_counter() - csv_started

    materialized_payload = {
        "shared_date_span": payload["methodology"]["shared_date_span"],
        "execution_model": payload["methodology"]["execution_model"],
        "materialized_targets": [
            {
                "target_id": row["target_id"],
                "label": row["label"],
                "target_kind": row["target_kind"],
                "symbol": row["symbol"],
                "allowed_sessions": row["allowed_sessions"],
                "point_value": row["point_value"],
                "lane_id": row["lane_id"],
                "standalone_strategy_id": row["standalone_strategy_id"],
                "candidate_id": row["candidate_id"],
                "config_path": row["config_path"],
                "metrics": row["metrics"],
                "session_breakdown": row["session_breakdown"],
                "position_rows": row["position_rows"],
            }
            for row in payload["targets"]
        ],
    }
    materialized_started = perf_counter()
    materialized_baseline_path.write_text(
        json.dumps(_json_ready(materialized_payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    materialized_seconds = perf_counter() - materialized_started
    manifest_payload = {
        "artifact_version": "atp_full_history_review_v2",
        "run_id": stable_hash(
            {
                "study": payload["study"],
                "generated_at": payload["generated_at"],
                "shared_date_span": payload["methodology"]["shared_date_span"],
                "target_ids": [row["target_id"] for row in payload["targets"]],
                "platform_substrate": payload.get("platform_substrate"),
            }
        ),
        "generated_at": payload["generated_at"],
        "source_db": str(source_db.resolve()),
        "source_date_span": payload["methodology"]["shared_date_span"],
        "execution_model": payload["methodology"]["execution_model"],
        "review_config": review_config_payload,
        "feature_bundle_ids": payload.get("platform_substrate", {}).get("feature_bundle_ids", {}),
        "context_bundle_ids": payload.get("platform_substrate", {}).get("context_bundle_ids", {}),
        "scope_bundle_ids": payload.get("platform_substrate", {}).get("scope_bundle_ids", {}),
        "target_hash": stable_hash([row["target_id"] for row in payload["targets"]]),
        "config_hash": stable_hash(
            {
                "review_config_hash": review_config_payload.get("config_hash"),
                "target_config_paths": {row["target_id"]: row.get("config_path") for row in payload["targets"]},
            }
        ),
        "artifacts": {
            "json_path": str(json_path.resolve()),
            "markdown_path": str(markdown_path.resolve()),
            "comparison_csv_path": str(comparison_csv_path.resolve()),
            "materialized_baseline_path": str(materialized_baseline_path.resolve()),
        },
    }
    manifest_started = perf_counter()
    write_json_manifest(manifest_path, manifest_payload)
    manifest_seconds = perf_counter() - manifest_started
    return {
        "json_path": json_path,
        "markdown_path": markdown_path,
        "comparison_csv_path": comparison_csv_path,
        "materialized_baseline_path": materialized_baseline_path,
        "manifest_path": manifest_path,
        "write_timing": {
            "json_write_seconds": round(json_write_seconds, 6),
            "markdown_write_seconds": round(markdown_seconds, 6),
            "comparison_csv_write_seconds": round(csv_seconds, 6),
            "materialized_truth_write_seconds": round(materialized_seconds, 6),
            "manifest_write_seconds": round(manifest_seconds, 6),
        },
    }


def _scope_bundle_manifest_path(bundle_id: str) -> Path:
    return DEFAULT_PLATFORM_SUBSTRATE_ROOT / "scope_bundles" / str(bundle_id) / "manifest.json"


def _artifact_payload(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value.resolve())
    return _json_ready(value)


def _register_review_payload(
    *,
    payload: dict[str, Any],
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    methodology = dict(payload.get("methodology") or {})
    shared_date_span = dict(methodology.get("shared_date_span") or {})
    review_config_payload = dict(methodology.get("review_config") or {})
    run_row = build_registry_run_row(
        strategy_family="atp_companion",
        strategy_variant="full_history_review",
        date_span=shared_date_span,
        data_version=stable_hash(payload.get("data_substrate") or {}, length=24),
        feature_version=ATP_FEATURE_VERSION,
        candidate_version=ATP_CANDIDATE_VERSION,
        outcome_engine_version=ATP_OUTCOME_ENGINE_VERSION,
        config_hash=str(
            review_config_payload.get("config_hash")
            or stable_hash({row["target_id"]: row.get("config_path") for row in payload.get("targets") or []}, length=24)
        ),
        control_hash="baseline_and_candidate_overlay_set_v1",
        target_hash=stable_hash([row["target_id"] for row in payload.get("targets") or []], length=24),
        bundle_ids=dict(payload.get("platform_substrate") or {}),
        artifacts={key: _artifact_payload(value) for key, value in artifacts.items()},
        summary_metrics={"recommendation": payload.get("recommendation")},
        lineage={
            "study": payload.get("study"),
            "execution_model": methodology.get("execution_model"),
            "command": methodology.get("command"),
        },
        generated_at=payload.get("generated_at"),
    )
    scope_bundle_ids = dict((payload.get("platform_substrate") or {}).get("scope_bundle_ids") or {})
    target_rows = []
    for row in payload.get("targets") or []:
        session_key = "/".join(row.get("allowed_sessions") or [])
        scope_key = f"{row.get('symbol')}:{session_key}"
        scope_bundle_id = scope_bundle_ids.get(scope_key)
        metrics = dict(row.get("metrics") or {})
        target_rows.append(
            {
                "strategy_family": "atp_companion",
                "strategy_variant": row.get("target_id"),
                "target_id": row.get("target_id"),
                "label": row.get("label"),
                "symbol": row.get("symbol"),
                "allowed_sessions": row.get("allowed_sessions"),
                "record_kind": "strategy_scope",
                "analytics_publish": True,
                "scope_bundle_id": scope_bundle_id,
                "scope_bundle_manifest_path": str(_scope_bundle_manifest_path(scope_bundle_id).resolve()) if scope_bundle_id else None,
                "feature_bundle_id": (payload.get("platform_substrate") or {}).get("feature_bundle_ids", {}).get(row.get("symbol")),
                "config_path": row.get("config_path"),
                "config_hash": stable_hash(
                    {
                        "target_id": row.get("target_id"),
                        "config_path": row.get("config_path"),
                    },
                    length=24,
                ),
                "target_hash": stable_hash(
                    {
                        "target_id": row.get("target_id"),
                        "symbol": row.get("symbol"),
                        "allowed_sessions": row.get("allowed_sessions"),
                    },
                    length=24,
                ),
                "generated_at": payload.get("generated_at"),
                "summary_metrics": {
                    "trade_count": metrics.get("total_trades"),
                    "net_pnl_cash": metrics.get("net_pnl_cash"),
                    "average_trade_pnl_cash": metrics.get("average_trade_pnl_cash"),
                    "profit_factor": metrics.get("profit_factor"),
                    "max_drawdown": metrics.get("max_drawdown"),
                    "win_rate": metrics.get("win_rate"),
                },
                "artifacts": {key: _artifact_payload(value) for key, value in artifacts.items()},
            }
        )
    registry_result = register_experiment_run(
        registry_root=DEFAULT_EXPERIMENT_REGISTRY_ROOT,
        run_row=run_row,
        target_rows=target_rows,
    )
    analytics_result = build_research_analytics_views(
        registry_root=DEFAULT_EXPERIMENT_REGISTRY_ROOT,
        analytics_root=DEFAULT_RESEARCH_ANALYTICS_ROOT,
        strategy_family="atp_companion",
    )
    return {
        "registry": registry_result,
        "analytics": analytics_result,
    }


def _materialize_symbol_truth(
    *,
    bundle_root: Path = DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    context_bundle_root: Path | None = None,
    source_db: Path = DEFAULT_SOURCE_DB,
    symbol: str,
    bar_source_index: dict[str, dict[str, Any]],
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> MaterializedSymbolTruth:
    load_started = perf_counter()
    resolved_context_bundle_root = (context_bundle_root or (bundle_root.parent / "source_context")).resolve()
    loaded_context = ensure_symbol_context_bundle(
        bundle_root=resolved_context_bundle_root,
        symbol=symbol,
        bar_source_index=bar_source_index,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    load_seconds = perf_counter() - load_started
    if loaded_context is None:
        raise RuntimeError(f"Unable to load ATP symbol context for {symbol}")
    selected_sources = dict(loaded_context.get("selected_sources") or {})
    feature_started = perf_counter()
    try:
        feature_bundle = ensure_atp_feature_bundle(
            bundle_root=bundle_root,
            source_db=source_db,
            symbol=symbol,
            selected_sources=selected_sources,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            feature_rows=None,
        )
    except FileNotFoundError:
        rolling_features = build_feature_states(
            bars_5m=loaded_context["combined_rolling_5m"],
            bars_1m=loaded_context["bars_1m"],
        )
        rolling_ts = {bar.end_ts for bar in loaded_context["rolling_5m"]}
        rolling_scope_feature_rows = [row for row in rolling_features if row.decision_ts in rolling_ts]
        feature_bundle = ensure_atp_feature_bundle(
            bundle_root=bundle_root,
            source_db=source_db,
            symbol=symbol,
            selected_sources=selected_sources,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            feature_rows=rolling_scope_feature_rows,
        )
    feature_seconds = perf_counter() - feature_started
    return MaterializedSymbolTruth(
        symbol=symbol,
        source_db=source_db,
        selected_sources=selected_sources,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        bars_1m=loaded_context["bars_1m"],
        rolling_scope_feature_rows=feature_bundle.feature_rows,
        load_seconds=load_seconds,
        feature_seconds=feature_seconds,
        feature_bundle_id=feature_bundle.bundle_id,
        context_bundle_id=loaded_context.get("context_bundle_id"),
    )


def _evaluate_materialized_scope(
    *,
    bundle_root: Path = DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    symbol_truth: MaterializedSymbolTruth,
    allowed_sessions: tuple[str, ...],
    point_value: float,
) -> MaterializedScopeTruth:
    scope_started = perf_counter()
    feature_bundle = ensure_atp_feature_bundle(
        bundle_root=bundle_root,
        source_db=symbol_truth.source_db,
        symbol=symbol_truth.symbol,
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=symbol_truth.start_timestamp,
        end_timestamp=symbol_truth.end_timestamp,
        feature_rows=symbol_truth.rolling_scope_feature_rows,
    )
    bundle_started = perf_counter()
    scope_bundle = ensure_atp_scope_bundle(
        bundle_root=bundle_root,
        source_db=symbol_truth.source_db,
        symbol=symbol_truth.symbol,
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=symbol_truth.start_timestamp,
        end_timestamp=symbol_truth.end_timestamp,
        allowed_sessions=allowed_sessions,
        point_value=float(point_value),
        bars_1m=symbol_truth.bars_1m,
        feature_bundle=feature_bundle,
    )
    bundle_seconds = perf_counter() - bundle_started
    return MaterializedScopeTruth(
        symbol=symbol_truth.symbol,
        allowed_sessions=allowed_sessions,
        point_value=point_value,
        bars_1m=symbol_truth.bars_1m,
        trade_rows=scope_bundle.trade_rows,
        evaluation_seconds=perf_counter() - scope_started,
        entry_state_seconds=0.0,
        timing_state_seconds=0.0,
        trade_rebuild_seconds=bundle_seconds,
        bar_count=len(symbol_truth.bars_1m),
        scope_bundle_id=scope_bundle.bundle_id,
    )


def _markdown_table(rows: Sequence[dict[str, Any]]) -> list[str]:
    header = [
        "| Rank | Target | Symbol | Sessions | Trades | Net P&L | Avg Trade | PF | Max DD | Win Rate | Adds | Add % | Add-only Net | Delta vs Benchmark |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    body = []
    for index, row in enumerate(rows, start=1):
        body.append(
            "| "
            + " | ".join(
                [
                    str(index),
                    str(row["rank_label"]),
                    str(row["symbol"]),
                    str(row["sessions"]),
                    str(row["trade_count"]),
                    f"{float(row['net_pnl_cash']):.4f}",
                    f"{float(row['average_trade_pnl_cash']):.4f}",
                    f"{float(row['profit_factor']):.4f}",
                    f"{float(row['max_drawdown']):.4f}",
                    f"{float(row['win_rate']):.4f}",
                    str(row["add_count"]),
                    f"{float(row['add_rate_percent']):.4f}",
                    f"{float(row['add_only_net_pnl_cash']):.4f}",
                    f"{float(row['delta_vs_benchmark_net_pnl_cash']):.4f}",
                ]
            )
            + " |"
        )
    return header + body


def run_full_history_review_legacy(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    started_at = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    targets = build_targets()
    symbol_set = {target.symbol for target in targets}
    discovery_started = perf_counter()
    bar_source_index = _discover_best_sources(symbols=symbol_set, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    discovery_seconds = perf_counter() - discovery_started
    source_discovery_metadata = last_source_discovery_metadata()
    coverage_started = perf_counter()
    shared_start, shared_end = _shared_1m_coverage_from_source_index(
        bar_source_index=bar_source_index,
        instruments=("MGC", "GC"),
    )
    coverage_seconds = perf_counter() - coverage_started
    run_start = max(shared_start, start_timestamp) if start_timestamp is not None else shared_start
    run_end = min(shared_end, end_timestamp) if end_timestamp is not None else shared_end
    candidate_registry_started = perf_counter()
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    candidate_registry_seconds = perf_counter() - candidate_registry_started

    symbol_truths: dict[str, MaterializedSymbolTruth] = {}
    loaded_contexts = {}
    for symbol in symbol_set:
        load_started = perf_counter()
        loaded_contexts[symbol] = _load_symbol_context(
            symbol=symbol,
            bar_source_index=bar_source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
        )
        load_seconds = perf_counter() - load_started
        if loaded_contexts[symbol] is None:
            raise RuntimeError(f"Unable to load ATP symbol context for {symbol}")
        symbol_truths[symbol] = MaterializedSymbolTruth(
            symbol=symbol,
            bars_1m=loaded_contexts[symbol]["bars_1m"],
            rolling_scope_feature_rows=[],
            load_seconds=load_seconds,
            feature_seconds=0.0,
        )

    base_lane_results: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
    scope_truths: dict[tuple[str, tuple[str, ...]], MaterializedScopeTruth] = {}
    for symbol, sessions, point_value in {
        ("MGC", ("ASIA", "US"), 10.0),
        ("MGC", ("ASIA",), 10.0),
        ("GC", ("ASIA", "US"), 100.0),
        ("GC", ("ASIA",), 100.0),
    }:
        eval_started = perf_counter()
        result = _evaluate_atp_lane(
            symbol=symbol,
            allowed_sessions=set(sessions),
            point_value=point_value,
            bar_source_index=bar_source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
            loaded_context=loaded_contexts[symbol],
        )
        if result is None:
            raise RuntimeError(f"Unable to evaluate ATP lane for {symbol} / {sessions}")
        base_lane_results[(symbol, sessions)] = result
        scope_truths[(symbol, sessions)] = MaterializedScopeTruth(
            symbol=symbol,
            allowed_sessions=sessions,
            point_value=point_value,
            bars_1m=result["bars_1m"],
            trade_rows=result["trade_rows"],
            evaluation_seconds=perf_counter() - eval_started,
            entry_state_seconds=0.0,
            timing_state_seconds=0.0,
            trade_rebuild_seconds=0.0,
            bar_count=len(result["bars_1m"]),
        )

    baseline_target = next(target for target in targets if target.target_kind == "frozen_benchmark")
    baseline_base = base_lane_results[(baseline_target.symbol, baseline_target.allowed_sessions)]
    baseline_position_rows = _base_position_rows(baseline_base["trade_rows"])
    baseline_payload = _result_payload(
        target=baseline_target,
        position_rows=baseline_position_rows,
        bar_count=len(baseline_base["bars_1m"]),
        start_timestamp=run_start,
        end_timestamp=run_end,
        baseline_metrics=_trade_metrics(baseline_position_rows, bar_count=len(baseline_base["bars_1m"])),
    )
    baseline_payload["delta_vs_frozen_benchmark"] = {
        "trade_count_delta": 0,
        "net_pnl_cash_delta": 0.0,
        "average_trade_pnl_cash_delta": 0.0,
        "profit_factor_delta": 0.0,
        "max_drawdown_delta": 0.0,
        "win_rate_delta": 0.0,
    }
    baseline_metrics = baseline_payload["metrics"]

    results = [baseline_payload]
    overlay_seconds_by_target: dict[str, float] = {}
    for target in targets[1:]:
        base_result = base_lane_results[(target.symbol, target.allowed_sessions)]
        overlay_started = perf_counter()
        if target.target_kind == "lane_candidate":
            position_rows = _base_position_rows(base_result["trade_rows"])
        else:
            candidate = candidate_defs[str(target.candidate_id)]
            position_rows = _candidate_position_rows(
                candidate=candidate,
                bars_1m=base_result["bars_1m"],
                trade_rows=base_result["trade_rows"],
                point_value=target.point_value,
            )
        overlay_seconds_by_target[target.target_id] = perf_counter() - overlay_started
        results.append(
            _result_payload(
                target=target,
                position_rows=position_rows,
                bar_count=len(base_result["bars_1m"]),
                start_timestamp=run_start,
                end_timestamp=run_end,
                baseline_metrics=baseline_metrics,
            )
        )

    comparison_rows = _comparison_rows(results)
    source_selection = {
        symbol: {
            timeframe: {
                "data_source": selection.data_source,
                "sqlite_path": str(selection.sqlite_path),
                "row_count": selection.row_count,
                "start_timestamp": selection.start_ts,
                "end_timestamp": selection.end_ts,
            }
            for timeframe, selection in by_timeframe.items()
        }
        for symbol, by_timeframe in sorted(bar_source_index.items())
    }
    data_substrate = {
        "source_db": str(source_db.resolve()),
        "selected_sources": source_selection,
        "coverage": {
            "MGC_1m": _source_selection_coverage_row(bar_source_index.get("MGC", {}).get("1m")),
            "GC_1m": _source_selection_coverage_row(bar_source_index.get("GC", {}).get("1m")),
            "MGC_5m": _source_selection_coverage_row(bar_source_index.get("MGC", {}).get("5m")),
            "GC_5m": _source_selection_coverage_row(bar_source_index.get("GC", {}).get("5m")),
        },
        "shared_full_history_start": run_start.isoformat(),
        "shared_full_history_end": run_end.isoformat(),
        "backfill_change_required": False,
        "backfill_note": "No backfill widening was required for this run because shared MGC/GC 1m history already covers 2024-01-01 through the latest available 2026 replay rows.",
    }

    payload = {
        "study": "ATP Companion full-history candidate review",
        "generated_at": datetime.now(UTC).isoformat(),
        "methodology": {
            "summary": "5m context with 1m executable timing across the shared MGC/GC full-history span; promotion branches inherit frozen baseline exits and only overlay explicit earned-add logic.",
            "shared_date_span": {
                "start_timestamp": run_start.isoformat(),
                "end_timestamp": run_end.isoformat(),
            },
            "mode": "legacy",
            "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
            "frozen_benchmark_semantics_changed": False,
            "command": " ".join(__import__('sys').argv),
        },
        "targets": results,
        "comparison_rows": comparison_rows,
        "recommendation": _promotion_recommendation(results),
        "data_substrate": data_substrate,
        "timing": _timing_payload(
            started_at=started_at,
            discovery_seconds=discovery_seconds,
            coverage_seconds=coverage_seconds,
            candidate_registry_seconds=candidate_registry_seconds,
            symbol_truths=symbol_truths,
            scope_truths=scope_truths,
            overlay_seconds_by_target=overlay_seconds_by_target,
            report_write_seconds=0.0,
        ),
    }
    payload["timing"]["source_discovery_breakdown"] = dict(source_discovery_metadata)
    return payload


def run_full_history_review(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    started_at = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    targets = build_targets()
    symbol_set = {target.symbol for target in targets}
    discovery_started = perf_counter()
    bar_source_index = _discover_best_sources(symbols=symbol_set, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    discovery_seconds = perf_counter() - discovery_started
    source_discovery_metadata = last_source_discovery_metadata()
    coverage_started = perf_counter()
    shared_start, shared_end = _shared_1m_coverage_from_source_index(
        bar_source_index=bar_source_index,
        instruments=("MGC", "GC"),
    )
    coverage_seconds = perf_counter() - coverage_started
    run_start = max(shared_start, start_timestamp) if start_timestamp is not None else shared_start
    run_end = min(shared_end, end_timestamp) if end_timestamp is not None else shared_end
    candidate_registry_started = perf_counter()
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    candidate_registry_seconds = perf_counter() - candidate_registry_started

    symbol_materialization_started = perf_counter()
    symbol_truths = {
        symbol: _materialize_symbol_truth(
            bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
            source_db=source_db,
            symbol=symbol,
            bar_source_index=bar_source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
        )
        for symbol in sorted(symbol_set)
    }
    symbol_materialization_total_seconds = perf_counter() - symbol_materialization_started

    scope_truths: dict[tuple[str, tuple[str, ...]], MaterializedScopeTruth] = {}
    scope_materialization_started = perf_counter()
    for symbol, sessions, point_value in {
        ("MGC", ("ASIA", "US"), 10.0),
        ("MGC", ("ASIA",), 10.0),
        ("GC", ("ASIA", "US"), 100.0),
        ("GC", ("ASIA",), 100.0),
    }:
        scope_truths[(symbol, sessions)] = _evaluate_materialized_scope(
            bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
            symbol_truth=symbol_truths[symbol],
            allowed_sessions=sessions,
            point_value=point_value,
        )
    scope_materialization_total_seconds = perf_counter() - scope_materialization_started

    trade_window_started = perf_counter()
    trade_windows_by_scope = {
        key: _trade_windows_by_id(
            bars_1m=truth.bars_1m,
            trade_rows=truth.trade_rows,
        )
        for key, truth in scope_truths.items()
    }
    trade_window_seconds = perf_counter() - trade_window_started

    baseline_target = next(target for target in targets if target.target_kind == "frozen_benchmark")
    baseline_scope = scope_truths[(baseline_target.symbol, baseline_target.allowed_sessions)]
    baseline_payload_started = perf_counter()
    baseline_position_rows = _base_position_rows(baseline_scope.trade_rows)
    baseline_metrics = _trade_metrics(baseline_position_rows, bar_count=baseline_scope.bar_count)
    baseline_payload = _result_payload(
        target=baseline_target,
        position_rows=baseline_position_rows,
        bar_count=baseline_scope.bar_count,
        start_timestamp=run_start,
        end_timestamp=run_end,
        baseline_metrics=baseline_metrics,
    )
    baseline_payload["delta_vs_frozen_benchmark"] = {
        "trade_count_delta": 0,
        "net_pnl_cash_delta": 0.0,
        "average_trade_pnl_cash_delta": 0.0,
        "profit_factor_delta": 0.0,
        "max_drawdown_delta": 0.0,
        "win_rate_delta": 0.0,
    }
    baseline_payload_seconds = perf_counter() - baseline_payload_started

    results = [baseline_payload]
    overlay_seconds_by_target: dict[str, float] = {}
    result_payload_started = perf_counter()
    for target in targets[1:]:
        base_scope = scope_truths[(target.symbol, target.allowed_sessions)]
        overlay_started = perf_counter()
        if target.target_kind == "lane_candidate":
            position_rows = _base_position_rows(base_scope.trade_rows)
        else:
            candidate = candidate_defs[str(target.candidate_id)]
            position_rows = _candidate_position_rows_from_windows(
                candidate=candidate,
                trade_rows=base_scope.trade_rows,
                trade_windows_by_id=trade_windows_by_scope[(target.symbol, target.allowed_sessions)],
                point_value=target.point_value,
            )
        overlay_seconds_by_target[target.target_id] = perf_counter() - overlay_started
        results.append(
            _result_payload(
                target=target,
                position_rows=position_rows,
                bar_count=base_scope.bar_count,
                start_timestamp=run_start,
                end_timestamp=run_end,
                baseline_metrics=baseline_metrics,
            )
        )
    result_payload_seconds = perf_counter() - result_payload_started

    comparison_started = perf_counter()
    comparison_rows = _comparison_rows(results)
    comparison_seconds = perf_counter() - comparison_started
    source_selection = {
        symbol: {
            timeframe: {
                "data_source": selection.data_source,
                "sqlite_path": str(selection.sqlite_path),
                "row_count": selection.row_count,
                "start_timestamp": selection.start_ts,
                "end_timestamp": selection.end_ts,
            }
            for timeframe, selection in by_timeframe.items()
        }
        for symbol, by_timeframe in sorted(bar_source_index.items())
    }
    data_substrate_started = perf_counter()
    data_substrate = {
        "source_db": str(source_db.resolve()),
        "selected_sources": source_selection,
        "coverage": {
            "MGC_1m": _source_selection_coverage_row(bar_source_index.get("MGC", {}).get("1m")),
            "GC_1m": _source_selection_coverage_row(bar_source_index.get("GC", {}).get("1m")),
            "MGC_5m": _source_selection_coverage_row(bar_source_index.get("MGC", {}).get("5m")),
            "GC_5m": _source_selection_coverage_row(bar_source_index.get("GC", {}).get("5m")),
        },
        "shared_full_history_start": run_start.isoformat(),
        "shared_full_history_end": run_end.isoformat(),
        "backfill_change_required": False,
        "backfill_note": "No backfill widening was required for this run because shared MGC/GC 1m history already covers 2024-01-01 through the latest available 2026 replay rows.",
    }
    data_substrate_seconds = perf_counter() - data_substrate_started
    recommendation_started = perf_counter()
    recommendation = _promotion_recommendation(results)
    recommendation_seconds = perf_counter() - recommendation_started
    payload = {
        "study": "ATP Companion full-history candidate review",
        "generated_at": datetime.now(UTC).isoformat(),
        "methodology": {
            "summary": "Materialized current ATP baseline truth once per symbol, then reused scope-specific trades and candidate overlays without rerunning prior comparator passes.",
            "shared_date_span": {
                "start_timestamp": run_start.isoformat(),
                "end_timestamp": run_end.isoformat(),
            },
            "mode": "optimized",
            "review_config": config_payload(
                FullHistoryReviewConfig(
                    mode="optimized",
                    required_timeframes=("1m", "5m"),
                    publish_registry=True,
                    publish_analytics=True,
                )
            ),
            "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
            "frozen_benchmark_semantics_changed": False,
            "command": " ".join(__import__('sys').argv),
        },
        "targets": results,
        "comparison_rows": comparison_rows,
        "recommendation": recommendation,
        "data_substrate": data_substrate,
        "platform_substrate": {
            "bundle_root": str(DEFAULT_PLATFORM_SUBSTRATE_ROOT.resolve()),
            "context_bundle_root": str((DEFAULT_RESEARCH_PLATFORM_ROOT / "source_context").resolve()),
            "context_bundle_ids": {symbol: truth.context_bundle_id for symbol, truth in sorted(symbol_truths.items())},
            "feature_bundle_ids": {symbol: truth.feature_bundle_id for symbol, truth in sorted(symbol_truths.items())},
            "scope_bundle_ids": {
                f"{symbol}:{'/'.join(sessions)}": truth.scope_bundle_id
                for (symbol, sessions), truth in sorted(scope_truths.items())
            },
        },
        "timing": _timing_payload(
            started_at=started_at,
            discovery_seconds=discovery_seconds,
            coverage_seconds=coverage_seconds,
            candidate_registry_seconds=candidate_registry_seconds,
            symbol_truths=symbol_truths,
            scope_truths=scope_truths,
            overlay_seconds_by_target=overlay_seconds_by_target,
            report_write_seconds=0.0,
        ),
    }
    payload["timing"]["source_discovery_breakdown"] = dict(source_discovery_metadata)
    payload["timing"]["orchestration_breakdown"] = {
        "symbol_materialization_total_seconds": round(symbol_materialization_total_seconds, 6),
        "scope_materialization_total_seconds": round(scope_materialization_total_seconds, 6),
        "trade_window_index_seconds": round(trade_window_seconds, 6),
        "baseline_payload_seconds": round(baseline_payload_seconds, 6),
        "candidate_result_payload_seconds": round(result_payload_seconds, 6),
        "comparison_rows_seconds": round(comparison_seconds, 6),
        "data_substrate_seconds": round(data_substrate_seconds, 6),
        "recommendation_seconds": round(recommendation_seconds, 6),
    }
    return payload


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).resolve()
    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        output_dir = (DEFAULT_OUTPUT_ROOT / f"atp_companion_full_history_review_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}").resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    if args.mode == "legacy":
        payload = run_full_history_review_legacy(
            source_db=source_db,
            output_dir=output_dir,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    else:
        payload = run_full_history_review(
            source_db=source_db,
            output_dir=output_dir,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
    write_started = perf_counter()
    result = _write_review_artifacts(
        payload=payload,
        output_dir=output_dir,
        source_db=source_db,
        run_start=datetime.fromisoformat(payload["methodology"]["shared_date_span"]["start_timestamp"]),
        run_end=datetime.fromisoformat(payload["methodology"]["shared_date_span"]["end_timestamp"]),
    )
    payload["timing"]["artifact_write_seconds"] = round(perf_counter() - write_started, 6)
    payload["timing"]["artifact_write_breakdown"] = dict(result.get("write_timing") or {})
    registry_result = _register_review_payload(payload=payload, artifacts=result)
    payload["timing"]["registry_seconds"] = registry_result["registry"]["timing"]["total_seconds"]
    payload["timing"]["registry_breakdown"] = dict(registry_result["registry"]["timing"])
    payload["timing"]["analytics_seconds"] = registry_result["analytics"]["timing"]["total_seconds"]
    payload["timing"]["analytics_breakdown"] = dict(registry_result["analytics"]["timing"])
    json_path = Path(result["json_path"])
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(
        json.dumps(
            {
                "registry_path": registry_result["registry"]["manifest_path"],
                "analytics_path": registry_result["analytics"]["manifest_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
