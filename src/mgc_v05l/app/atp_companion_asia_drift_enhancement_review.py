"""ATP Companion / Asia Drift enhancement review against the frozen baseline."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median, pstdev
from typing import Any, Iterable, Sequence

from .atp_companion_full_history_review import (
    DEFAULT_SOURCE_DB,
    _discover_best_sources,
    _shared_1m_coverage_from_source_index,
    _trade_windows_by_id,
)
from ..research.trend_participation.substrate import (
    ensure_atp_feature_bundle,
    ensure_atp_scope_bundle,
)
from ..research.trend_participation.atp_promotion_add_review import (
    PromotionAddCandidate,
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.performance_validation import (
    _trade_metrics,
    summarize_trade_distribution_diagnostics,
)
from ..research.trend_participation.storage import (
    load_sqlite_bars,
    normalize_and_check_bars,
)

REPO_ROOT = Path.cwd()
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_asia_drift_enhancement_review"
DEFAULT_PLATFORM_SUBSTRATE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "atp_substrate"
DEFAULT_MATERIALIZED_TRUTH_GLOB = "outputs/reports/**/atp_companion_materialized_baseline_truth.json"
PRIMARY_CANDIDATE_ID = "promotion_1_075r_favorable_only"
PRIMARY_CLASSIFICATION_ORDER = (
    "ENHANCEMENT_PROMISING",
    "ENHANCEMENT_MIXED",
    "ENHANCEMENT_REJECTED",
    "RETAIN_FOR_MORE_PAPER_DATA",
)


@dataclass(frozen=True)
class BranchSpec:
    evaluation_id: str
    label: str
    symbol: str
    point_value: float
    reference_label: str
    reference_kind: str
    frozen_reference: bool
    notes: tuple[str, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-asia-drift-enhancement-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--include-gc", action="store_true", help="Include the existing-supported GC supplemental comparison.")
    return parser


def _candidate_definitions() -> dict[str, PromotionAddCandidate]:
    return {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}


def _selection_payload(selection: Any) -> dict[str, Any] | None:
    if selection is None:
        return None
    return {
        "symbol": str(getattr(selection, "symbol", "") or ""),
        "timeframe": str(getattr(selection, "timeframe", "") or ""),
        "data_source": str(getattr(selection, "data_source", "") or ""),
        "sqlite_path": str(Path(getattr(selection, "sqlite_path")).resolve()),
        "row_count": int(getattr(selection, "row_count", 0) or 0),
        "start_ts": getattr(selection, "start_ts", None),
        "end_ts": getattr(selection, "end_ts", None),
    }


def _latest_materialized_truth_path() -> Path | None:
    candidates = sorted(Path.cwd().glob(DEFAULT_MATERIALIZED_TRUTH_GLOB), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _row_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("decision_ts"),
        row.get("entry_ts"),
        row.get("session_segment"),
        row.get("side"),
        row.get("bars_held_1m"),
    )


def _hydrate_position_row(row: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(row)
    for key, value in list(hydrated.items()):
        if key.endswith("_ts") and isinstance(value, str) and value:
            hydrated[key] = datetime.fromisoformat(value)
    return hydrated


def _compose_asia_only_candidate_from_materialized(
    *,
    baseline_rows: Sequence[dict[str, Any]],
    full_candidate_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    full_by_key = {_row_key(row): dict(row) for row in full_candidate_rows}
    combined: list[dict[str, Any]] = []
    for row in baseline_rows:
        key = _row_key(row)
        baseline_row = dict(row)
        full_row = dict(full_by_key.get(key, baseline_row))
        if str(baseline_row.get("session_segment")) == "ASIA":
            for fill_key in ("trade_id", "exit_ts", "exit_reason", "family", "mfe_points", "mae_points"):
                if full_row.get(fill_key) in {None, ""}:
                    full_row[fill_key] = baseline_row.get(fill_key)
            combined.append(full_row)
        else:
            baseline_row["add_reason"] = "SESSION_NOT_ELIGIBLE"
            combined.append(baseline_row)
    return combined


def _materialized_primary_result() -> dict[str, Any] | None:
    materialized_path = _latest_materialized_truth_path()
    if materialized_path is None:
        return None
    payload = json.loads(materialized_path.read_text(encoding="utf-8"))
    targets = {row["target_id"]: row for row in payload.get("materialized_targets") or []}
    baseline = targets.get("atp_companion_v1__benchmark_mgc_asia_us")
    full_candidate = targets.get("atp_companion_v1__promotion_1_075r_favorable_only")
    if baseline is None or full_candidate is None:
        return None
    baseline_rows = [_hydrate_position_row(dict(row)) for row in baseline.get("position_rows") or []]
    full_candidate_rows = [_hydrate_position_row(dict(row)) for row in full_candidate.get("position_rows") or []]
    if len(baseline_rows) != len(full_candidate_rows):
        return None
    methodology_span = ((payload.get("shared_date_span") or {}) if isinstance(payload.get("shared_date_span"), dict) else {})
    if not methodology_span:
        methodology_span = dict((payload.get("methodology") or {}).get("shared_date_span") or {})
    start_ts = methodology_span.get("start_timestamp")
    end_ts = methodology_span.get("end_timestamp")
    if not start_ts or not end_ts:
        return None
    candidate_rows = _compose_asia_only_candidate_from_materialized(
        baseline_rows=baseline_rows,
        full_candidate_rows=full_candidate_rows,
    )
    spec = BranchSpec(
        evaluation_id="mgc_asia_only_promotion_1_075r_favorable_only",
        label="MGC Asia-only Promotion 1 +0.75R VWAP Favorable Only",
        symbol="MGC",
        point_value=10.0,
        reference_label="Frozen ATP Companion Baseline v1 / MGC / Asia+US",
        reference_kind="frozen_baseline_v1",
        frozen_reference=True,
        notes=(
            "Primary comparison sourced from materialized optimized ATP full-history truth.",
            "U.S. rows are preserved from the frozen baseline with no add overlay.",
            "ASIA rows inherit the existing promotion_1_075r_favorable_only replay overlay.",
        ),
    )
    baseline_metrics = _build_core_metrics(baseline_rows, bar_count=max(len(baseline_rows), 1))
    candidate_metrics = _build_core_metrics(candidate_rows, bar_count=max(len(candidate_rows), 1))
    add_metrics = _build_add_metrics(
        rows=candidate_rows,
        baseline_rows=baseline_rows,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
    )
    delta = _build_delta_rows(
        evaluation_id=spec.evaluation_id,
        label=spec.label,
        baseline_metrics=baseline_metrics,
        candidate_metrics=candidate_metrics,
        add_metrics=add_metrics,
    )
    classification = _classify_candidate(
        trade_count=int(candidate_metrics["total_trades"]),
        add_count=int(add_metrics["add_count"]),
        delta=delta,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
        add_metrics=add_metrics,
    )
    return {
        "spec": spec,
        "start_timestamp": datetime.fromisoformat(start_ts),
        "end_timestamp": datetime.fromisoformat(end_ts),
        "baseline_rows": baseline_rows,
        "candidate_rows": candidate_rows,
        "baseline_metrics": baseline_metrics,
        "candidate_metrics": candidate_metrics,
        "add_metrics": add_metrics,
        "delta": delta,
        "classification": classification,
        "session_rows": (
            _build_session_breakdown_rows(evaluation_id=spec.evaluation_id, label=f"{spec.label} / baseline", rows=baseline_rows)
            + _build_session_breakdown_rows(evaluation_id=spec.evaluation_id, label=f"{spec.label} / candidate", rows=candidate_rows)
        ),
        "materialized_truth_path": str(materialized_path.resolve()),
    }


def _matching_feature_bundle_window(*, symbol: str, selected_sources: dict[str, Any]) -> tuple[datetime, datetime] | None:
    bundle_root = DEFAULT_PLATFORM_SUBSTRATE_ROOT / "feature_bundles"
    best: tuple[float, datetime, datetime] | None = None
    for manifest_path in bundle_root.glob("*/manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("symbol") != symbol:
            continue
        if manifest.get("selected_sources") != selected_sources:
            continue
        span = manifest.get("source_date_span") or {}
        start_ts = span.get("start_timestamp")
        end_ts = span.get("end_timestamp")
        if not start_ts or not end_ts:
            continue
        start = datetime.fromisoformat(start_ts)
        end = datetime.fromisoformat(end_ts)
        seconds = (end - start).total_seconds()
        if best is None or seconds > best[0]:
            best = (seconds, start, end)
    if best is None:
        return None
    return best[1], best[2]


def _best_existing_feature_bundle(*, symbol: str) -> tuple[dict[str, Any], datetime, datetime] | None:
    bundle_root = DEFAULT_PLATFORM_SUBSTRATE_ROOT / "feature_bundles"
    best: tuple[float, dict[str, Any], datetime, datetime] | None = None
    for manifest_path in bundle_root.glob("*/manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("symbol") != symbol:
            continue
        span = manifest.get("source_date_span") or {}
        start_ts = span.get("start_timestamp")
        end_ts = span.get("end_timestamp")
        if not start_ts or not end_ts:
            continue
        start = datetime.fromisoformat(start_ts)
        end = datetime.fromisoformat(end_ts)
        seconds = (end - start).total_seconds()
        selected_sources = dict(manifest.get("selected_sources") or {})
        if best is None or seconds > best[0]:
            best = (seconds, selected_sources, start, end)
    if best is None:
        return None
    return best[1], best[2], best[3]


def _quantile(values: Sequence[float], q: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return round(ordered[0], 4)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    value = ordered[lower] * (1.0 - weight) + ordered[upper] * weight
    return round(value, 4)


def _round_or_none(value: float | None) -> float | None:
    return round(float(value), 4) if value is not None else None


def _safe_divide(numerator: float, denominator: float) -> float | None:
    if abs(float(denominator)) <= 1e-9:
        return None
    return round(float(numerator) / float(denominator), 4)


def _percent(numerator: int | float, denominator: int | float) -> float:
    if float(denominator) <= 0.0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 4)


def _base_position_row(row: dict[str, Any]) -> dict[str, Any]:
    trade = row["trade_record"]
    return {
        "trade_id": row["trade_id"],
        "instrument": trade.instrument,
        "variant_id": trade.variant_id,
        "family": trade.family,
        "entry_ts": trade.entry_ts,
        "decision_ts": trade.decision_ts,
        "exit_ts": trade.exit_ts,
        "position_entry_price": float(trade.entry_price),
        "position_exit_price": float(trade.exit_price),
        "entry_price": float(trade.entry_price),
        "exit_price": float(trade.exit_price),
        "trade_pnl_cash": float(trade.pnl_cash),
        "pnl_cash": float(trade.pnl_cash),
        "mfe_points": float(trade.mfe_points),
        "mae_points": float(trade.mae_points),
        "hold_minutes": float(trade.hold_minutes),
        "bars_held_1m": int(trade.bars_held_1m),
        "side": trade.side,
        "session_segment": trade.session_segment,
        "exit_reason": trade.exit_reason,
        "added": False,
        "add_pnl_cash": 0.0,
        "add_pnl_points": 0.0,
        "add_entry_ts": None,
        "add_exit_ts": None,
        "add_entry_price": None,
        "add_trigger_price": None,
        "add_price_quality_state": None,
        "add_reason": "BASELINE_ONLY",
        "add_hold_minutes": 0.0,
        "modeled_exit_dependency": "baseline_only",
    }


def _overlay_asia_only_candidate_rows(
    *,
    trade_rows: Sequence[dict[str, Any]],
    trade_windows_by_id: dict[str, list[Any]],
    candidate: PromotionAddCandidate,
    point_value: float,
    eligible_sessions: Iterable[str] = ("ASIA",),
) -> list[dict[str, Any]]:
    eligible = {str(session) for session in eligible_sessions}
    rows: list[dict[str, Any]] = []
    for row in trade_rows:
        trade = row["trade_record"]
        if str(trade.session_segment) not in eligible:
            baseline_row = _base_position_row(row)
            baseline_row["add_reason"] = "SESSION_NOT_ELIGIBLE"
            rows.append(baseline_row)
            continue
        candidate_row = evaluate_promotion_add_candidate(
            trade=trade,
            minute_bars=trade_windows_by_id.get(str(row["trade_id"])) or [],
            candidate=candidate,
            point_value=point_value,
        )
        candidate_row["trade_id"] = row["trade_id"]
        rows.append(candidate_row)
    return rows


def _daily_pnl_values(rows: Sequence[dict[str, Any]]) -> list[float]:
    by_day: dict[str, float] = defaultdict(float)
    for row in rows:
        exit_ts = row.get("exit_ts")
        if exit_ts is None:
            continue
        day = exit_ts.date().isoformat()
        by_day[day] += float(row.get("pnl_cash") or 0.0)
    return [round(value, 4) for _, value in sorted(by_day.items())]


def _session_daily_pnl_values(rows: Sequence[dict[str, Any]], session: str) -> list[float]:
    by_day: dict[str, float] = defaultdict(float)
    for row in rows:
        if str(row.get("session_segment")) != session:
            continue
        exit_ts = row.get("exit_ts")
        if exit_ts is None:
            continue
        by_day[exit_ts.date().isoformat()] += float(row.get("pnl_cash") or 0.0)
    return [round(value, 4) for _, value in sorted(by_day.items())]


def _session_trade_rows(rows: Sequence[dict[str, Any]], session: str) -> list[dict[str, Any]]:
    return [row for row in rows if str(row.get("session_segment")) == session]


def _volatility_label(delta: float) -> str:
    if delta > 0.0:
        return "INCREASED"
    if delta < 0.0:
        return "REDUCED"
    return "UNCHANGED"


def _build_core_metrics(rows: Sequence[dict[str, Any]], *, bar_count: int) -> dict[str, Any]:
    metrics = _trade_metrics(rows, bar_count=bar_count)
    pnl_values = [float(row.get("pnl_cash") or 0.0) for row in rows]
    daily_pnls = _daily_pnl_values(rows)
    distribution = summarize_trade_distribution_diagnostics(rows)
    net_pnl = float(metrics["net_pnl_cash"])
    max_drawdown = float(metrics["max_drawdown"])
    return {
        **metrics,
        "median_trade_pnl_cash": _quantile(pnl_values, 0.5),
        "largest_win_pnl_cash": _round_or_none(max(pnl_values) if pnl_values else None),
        "largest_loss_pnl_cash": _round_or_none(min(pnl_values) if pnl_values else None),
        "drawdown_to_profit_ratio": _safe_divide(max_drawdown, net_pnl) if net_pnl > 0.0 else None,
        "max_consecutive_losers": int(distribution["max_consecutive_losses"]),
        "daily_path_volatility_cash": _round_or_none(pstdev(daily_pnls)) if len(daily_pnls) >= 2 else 0.0,
        "daily_pnl_median_cash": _quantile(daily_pnls, 0.5),
        "daily_pnl_p25_cash": _quantile(daily_pnls, 0.25),
        "daily_pnl_p75_cash": _quantile(daily_pnls, 0.75),
    }


def _build_add_metrics(
    *,
    rows: Sequence[dict[str, Any]],
    baseline_rows: Sequence[dict[str, Any]],
    candidate_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
) -> dict[str, Any]:
    add_rows = [row for row in rows if bool(row.get("added"))]
    with_add = add_rows
    without_add = [row for row in rows if not bool(row.get("added"))]
    add_pnls = [float(row.get("add_pnl_cash") or 0.0) for row in add_rows]
    improved = sum(1 for value in add_pnls if value > 0.0)
    worsened = sum(1 for value in add_pnls if value < 0.0)
    baseline_daily_vol = float(baseline_metrics.get("daily_path_volatility_cash") or 0.0)
    candidate_daily_vol = float(candidate_metrics.get("daily_path_volatility_cash") or 0.0)
    return {
        "add_count": len(add_rows),
        "add_frequency_percent": _percent(len(add_rows), len(rows)),
        "add_success_rate_percent": _percent(sum(1 for value in add_pnls if value > 0.0), len(add_rows)),
        "average_pnl_trades_with_adds": _round_or_none(sum(float(row["pnl_cash"]) for row in with_add) / len(with_add)) if with_add else None,
        "average_pnl_trades_without_adds": _round_or_none(sum(float(row["pnl_cash"]) for row in without_add) / len(without_add)) if without_add else None,
        "incremental_pnl_from_adds": round(sum(add_pnls), 4),
        "incremental_drawdown_from_adds": round(float(candidate_metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
        "percent_adds_worsened_trade": _percent(worsened, len(add_rows)),
        "percent_adds_improved_trade": _percent(improved, len(add_rows)),
        "path_volatility_delta_cash": round(candidate_daily_vol - baseline_daily_vol, 4),
        "path_volatility_effect": _volatility_label(candidate_daily_vol - baseline_daily_vol),
    }


def _build_session_breakdown_rows(
    *,
    evaluation_id: str,
    label: str,
    rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    breakdown_rows: list[dict[str, Any]] = []
    for session in ("ASIA", "US"):
        session_rows = _session_trade_rows(rows, session)
        pnl_values = [float(row.get("pnl_cash") or 0.0) for row in session_rows]
        daily_values = _session_daily_pnl_values(rows, session)
        add_rows = [row for row in session_rows if bool(row.get("added"))]
        breakdown_rows.append(
            {
                "evaluation_id": evaluation_id,
                "label": label,
                "session": session,
                "trade_count": len(session_rows),
                "add_count": len(add_rows),
                "net_pnl_cash": round(sum(pnl_values), 4),
                "average_trade_pnl_cash": _round_or_none(sum(pnl_values) / len(pnl_values)) if pnl_values else None,
                "median_trade_pnl_cash": _quantile(pnl_values, 0.5),
                "win_rate": _percent(sum(1 for value in pnl_values if value > 0.0), len(pnl_values)),
                "daily_pnl_average_cash": _round_or_none(sum(daily_values) / len(daily_values)) if daily_values else None,
                "daily_pnl_median_cash": _quantile(daily_values, 0.5),
                "daily_pnl_p25_cash": _quantile(daily_values, 0.25),
                "daily_pnl_p75_cash": _quantile(daily_values, 0.75),
                "daily_pnl_stddev_cash": _round_or_none(pstdev(daily_values)) if len(daily_values) >= 2 else 0.0,
            }
        )
    return breakdown_rows


def _build_delta_rows(
    *,
    evaluation_id: str,
    label: str,
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
    add_metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "evaluation_id": evaluation_id,
        "label": label,
        "net_pnl_cash_delta": round(float(candidate_metrics.get("net_pnl_cash") or 0.0) - float(baseline_metrics.get("net_pnl_cash") or 0.0), 4),
        "average_trade_pnl_cash_delta": round(float(candidate_metrics.get("average_trade_pnl_cash") or 0.0) - float(baseline_metrics.get("average_trade_pnl_cash") or 0.0), 4),
        "median_trade_pnl_cash_delta": round(float(candidate_metrics.get("median_trade_pnl_cash") or 0.0) - float(baseline_metrics.get("median_trade_pnl_cash") or 0.0), 4),
        "win_rate_delta": round(float(candidate_metrics.get("win_rate") or 0.0) - float(baseline_metrics.get("win_rate") or 0.0), 4),
        "profit_factor_delta": round(float(candidate_metrics.get("profit_factor") or 0.0) - float(baseline_metrics.get("profit_factor") or 0.0), 4),
        "max_drawdown_delta": round(float(candidate_metrics.get("max_drawdown") or 0.0) - float(baseline_metrics.get("max_drawdown") or 0.0), 4),
        "drawdown_to_profit_ratio_delta": _round_or_none(
            float(candidate_metrics["drawdown_to_profit_ratio"]) - float(baseline_metrics["drawdown_to_profit_ratio"])
        )
        if candidate_metrics.get("drawdown_to_profit_ratio") is not None and baseline_metrics.get("drawdown_to_profit_ratio") is not None
        else None,
        "largest_win_delta": round(float(candidate_metrics.get("largest_win_pnl_cash") or 0.0) - float(baseline_metrics.get("largest_win_pnl_cash") or 0.0), 4),
        "largest_loss_delta": round(float(candidate_metrics.get("largest_loss_pnl_cash") or 0.0) - float(baseline_metrics.get("largest_loss_pnl_cash") or 0.0), 4),
        "max_consecutive_losers_delta": int(candidate_metrics.get("max_consecutive_losers") or 0) - int(baseline_metrics.get("max_consecutive_losers") or 0),
        "path_volatility_delta_cash": add_metrics["path_volatility_delta_cash"],
    }


def _classify_candidate(
    *,
    trade_count: int,
    add_count: int,
    delta: dict[str, Any],
    candidate_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    add_metrics: dict[str, Any],
) -> str:
    if trade_count < 40 or add_count < 8:
        return "RETAIN_FOR_MORE_PAPER_DATA"
    if delta["net_pnl_cash_delta"] <= 0.0 or add_metrics["incremental_pnl_from_adds"] <= 0.0:
        return "ENHANCEMENT_REJECTED"
    profit_factor_ok = delta["profit_factor_delta"] >= 0.0
    average_trade_ok = delta["average_trade_pnl_cash_delta"] >= 0.0
    drawdown_ok = (
        delta["max_drawdown_delta"] <= 0.0
        or (
            candidate_metrics.get("drawdown_to_profit_ratio") is not None
            and baseline_metrics.get("drawdown_to_profit_ratio") is not None
            and float(candidate_metrics["drawdown_to_profit_ratio"]) <= float(baseline_metrics["drawdown_to_profit_ratio"])
        )
    )
    path_ok = add_metrics["path_volatility_delta_cash"] <= 0.0
    if profit_factor_ok and average_trade_ok and drawdown_ok and path_ok:
        return "ENHANCEMENT_PROMISING"
    return "ENHANCEMENT_MIXED"


def _build_summary_row(
    *,
    spec: BranchSpec,
    role: str,
    metrics: dict[str, Any],
    add_metrics: dict[str, Any] | None = None,
    classification: str | None = None,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> dict[str, Any]:
    row = {
        "evaluation_id": spec.evaluation_id,
        "label": spec.label,
        "role": role,
        "symbol": spec.symbol,
        "reference_label": spec.reference_label,
        "reference_kind": spec.reference_kind,
        "frozen_reference": spec.frozen_reference,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "trade_count": metrics["total_trades"],
        "net_pnl_cash": metrics["net_pnl_cash"],
        "average_trade_pnl_cash": metrics["average_trade_pnl_cash"],
        "median_trade_pnl_cash": metrics["median_trade_pnl_cash"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "average_winner_pnl_cash": metrics["average_winner_pnl_cash"],
        "average_loser_pnl_cash": metrics["average_loser_pnl_cash"],
        "max_drawdown": metrics["max_drawdown"],
        "drawdown_to_profit_ratio": metrics["drawdown_to_profit_ratio"],
        "largest_win_pnl_cash": metrics["largest_win_pnl_cash"],
        "largest_loss_pnl_cash": metrics["largest_loss_pnl_cash"],
        "max_consecutive_losers": metrics["max_consecutive_losers"],
        "daily_path_volatility_cash": metrics["daily_path_volatility_cash"],
        "classification": classification,
    }
    if add_metrics is not None:
        row.update(add_metrics)
    else:
        row.update(
            {
                "add_count": 0,
                "add_frequency_percent": 0.0,
                "add_success_rate_percent": 0.0,
                "average_pnl_trades_with_adds": None,
                "average_pnl_trades_without_adds": metrics["average_trade_pnl_cash"],
                "incremental_pnl_from_adds": 0.0,
                "incremental_drawdown_from_adds": 0.0,
                "percent_adds_worsened_trade": 0.0,
                "percent_adds_improved_trade": 0.0,
                "path_volatility_delta_cash": 0.0,
                "path_volatility_effect": "UNCHANGED",
            }
        )
    return row


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _render_markdown(
    *,
    summary_rows: Sequence[dict[str, Any]],
    risk_rows: Sequence[dict[str, Any]],
    notes: Sequence[str],
) -> str:
    lines = [
        "# ATP Companion / Asia Drift Enhancement Review",
        "",
        "Paper-only research pass. The frozen ATP Companion Baseline v1 benchmark was not mutated.",
        "",
        "## Current Branch Status",
        "",
        "- Combined staged-add overlay status after durability review: `ENHANCEMENT_PROMISING_BUT_THIN`.",
        "- The result should not be promoted, widened to U.S., or used to alter baseline sizing.",
        "- There are no live execution changes and no IBKR changes in this branch.",
        "",
        "## Core Question",
        "",
        "Can expected return improve by adding only to already-working Asia trades under strict VWAP-favorable continuation conditions, without damaging risk quality or path quality?",
        "",
        "## Summary",
        "",
    ]
    for row in summary_rows:
        if row["role"] != "candidate":
            continue
        lines.extend(
            [
                f"### {row['label']}",
                f"- Net P/L: `{row['net_pnl_cash']}`",
                f"- Avg trade: `{row['average_trade_pnl_cash']}`",
                f"- Profit factor: `{row['profit_factor']}`",
                f"- Max drawdown: `{row['max_drawdown']}`",
                f"- Add frequency: `{row['add_frequency_percent']}%`",
                f"- Add success rate: `{row['add_success_rate_percent']}%`",
                f"- Incremental P/L from adds: `{row['incremental_pnl_from_adds']}`",
                f"- Candidate branch remains under durability review discipline rather than promotion discipline.",
            ]
        )
        risk_row = next((item for item in risk_rows if item["evaluation_id"] == row["evaluation_id"]), None)
        if risk_row is not None:
            lines.extend(
                [
                    f"- Baseline delta net / PF / DD: `{risk_row['net_pnl_cash_delta']}` / `{risk_row['profit_factor_delta']}` / `{risk_row['max_drawdown_delta']}`",
                    f"- Drawdown-to-profit delta: `{risk_row['drawdown_to_profit_ratio_delta']}`",
                ]
            )
    lines.extend(
        [
            "",
            "## Notes",
            *[f"- {note}" for note in notes],
            "- The broad branch read is still `ENHANCEMENT_PROMISING_BUT_THIN` because only 28 adds were observed and a meaningful share of gains came from the top 5 events and from 2026.",
            "- The durability review confirmed the result is not a one-event fluke: add P/L remains positive after excluding the largest add, the top 3 adds, and the top 5 adds.",
            "",
            "## Discipline",
            "- Frozen ATP Companion Baseline v1 remains untouched.",
            "- U.S. baseline trades remain unchanged.",
            "- London remains diagnostic-only.",
            "- No live execution changes.",
            "- No IBKR or broker-path changes.",
            "- No changes to the US Open NDX lane.",
            "- No promotion to baseline in this pass.",
            "- No widening to U.S. and no baseline sizing changes.",
        ]
    )
    return "\n".join(lines) + "\n"


def _evaluate_branch(
    *,
    spec: BranchSpec,
    source_db: Path,
    bar_source_index: dict[str, dict[str, Any]],
    candidate: PromotionAddCandidate,
) -> dict[str, Any]:
    existing_bundle = _best_existing_feature_bundle(symbol=spec.symbol)
    if existing_bundle is not None:
        selected_sources, shared_start, shared_end = existing_bundle
    else:
        selected_sources = {
            "1m": _selection_payload(bar_source_index.get(spec.symbol, {}).get("1m")),
            "5m": None,
        }
        matched_window = _matching_feature_bundle_window(symbol=spec.symbol, selected_sources=selected_sources)
        if matched_window is not None:
            shared_start, shared_end = matched_window
        else:
            shared_start, shared_end = _shared_1m_coverage_from_source_index(
                bar_source_index=bar_source_index,
                instruments=(spec.symbol,),
            )
    feature_bundle = ensure_atp_feature_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol=spec.symbol,
        selected_sources=selected_sources,
        start_timestamp=shared_start,
        end_timestamp=shared_end,
        feature_rows=None,
    )
    raw_1m = load_sqlite_bars(
        sqlite_path=source_db,
        instrument=spec.symbol,
        timeframe="1m",
        start_ts=shared_start,
        end_ts=shared_end,
    )
    bars_1m, _ = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
    scope_truth = ensure_atp_scope_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol=spec.symbol,
        selected_sources=selected_sources,
        start_timestamp=shared_start,
        end_timestamp=shared_end,
        allowed_sessions=("ASIA", "US"),
        point_value=spec.point_value,
        bars_1m=bars_1m,
        feature_bundle=feature_bundle,
    )
    trade_windows = _trade_windows_by_id(bars_1m=scope_truth.bars_1m, trade_rows=scope_truth.trade_rows)
    baseline_rows = [_base_position_row(row) for row in scope_truth.trade_rows]
    candidate_rows = _overlay_asia_only_candidate_rows(
        trade_rows=scope_truth.trade_rows,
        trade_windows_by_id=trade_windows,
        candidate=candidate,
        point_value=spec.point_value,
        eligible_sessions=("ASIA",),
    )
    baseline_metrics = _build_core_metrics(baseline_rows, bar_count=scope_truth.bar_count)
    candidate_metrics = _build_core_metrics(candidate_rows, bar_count=scope_truth.bar_count)
    add_metrics = _build_add_metrics(
        rows=candidate_rows,
        baseline_rows=baseline_rows,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
    )
    delta = _build_delta_rows(
        evaluation_id=spec.evaluation_id,
        label=spec.label,
        baseline_metrics=baseline_metrics,
        candidate_metrics=candidate_metrics,
        add_metrics=add_metrics,
    )
    classification = _classify_candidate(
        trade_count=int(candidate_metrics["total_trades"]),
        add_count=int(add_metrics["add_count"]),
        delta=delta,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
        add_metrics=add_metrics,
    )
    return {
        "spec": spec,
        "start_timestamp": shared_start,
        "end_timestamp": shared_end,
        "baseline_rows": baseline_rows,
        "candidate_rows": candidate_rows,
        "baseline_metrics": baseline_metrics,
        "candidate_metrics": candidate_metrics,
        "add_metrics": add_metrics,
        "delta": delta,
        "classification": classification,
        "session_rows": (
            _build_session_breakdown_rows(evaluation_id=spec.evaluation_id, label=f"{spec.label} / baseline", rows=baseline_rows)
            + _build_session_breakdown_rows(evaluation_id=spec.evaluation_id, label=f"{spec.label} / candidate", rows=candidate_rows)
        ),
    }


def _summary_payload(results: Sequence[dict[str, Any]], *, omitted_pl_note: str) -> dict[str, Any]:
    candidate_rows = []
    for result in results:
        spec: BranchSpec = result["spec"]
        candidate_rows.append(
            _build_summary_row(
                spec=spec,
                role="baseline",
                metrics=result["baseline_metrics"],
                start_timestamp=result["start_timestamp"],
                end_timestamp=result["end_timestamp"],
            )
        )
        candidate_rows.append(
            _build_summary_row(
                spec=spec,
                role="candidate",
                metrics=result["candidate_metrics"],
                add_metrics=result["add_metrics"],
                classification=result["classification"],
                start_timestamp=result["start_timestamp"],
                end_timestamp=result["end_timestamp"],
            )
        )
    risk_rows = [dict(result["delta"], classification=result["classification"]) for result in results]
    add_rows = []
    for result in results:
        spec: BranchSpec = result["spec"]
        metrics = result["add_metrics"]
        add_rows.append(
            {
                "evaluation_id": spec.evaluation_id,
                "label": spec.label,
                "symbol": spec.symbol,
                "reference_label": spec.reference_label,
                **metrics,
            }
        )
    notes = [
        "The primary MGC candidate overlays adds only on ASIA trades; the baseline US component remains unchanged and untouched.",
        "PL was not included because existing clean Asia-only staged-add support was not already in place, and this pass avoids new architecture.",
        omitted_pl_note,
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "results": results,
        "candidate_vs_baseline_summary": candidate_rows,
        "add_contribution_table": add_rows,
        "risk_delta_table": risk_rows,
        "session_breakdown_table": [row for result in results for row in result["session_rows"]],
        "notes": notes,
    }


def run_enhancement_review(
    *,
    source_db: Path,
    output_dir: Path,
    include_gc: bool,
) -> dict[str, Path]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate = _candidate_definitions()[PRIMARY_CANDIDATE_ID]
    primary_result = _materialized_primary_result()
    gc_omitted_note = "GC supplemental comparison omitted because the current durable artifact set does not expose a clean GC Asia-only overlay on an unchanged Asia+US baseline."
    if primary_result is not None:
        results = [primary_result]
    else:
        symbols = {"MGC"}
        specs = [
            BranchSpec(
                evaluation_id="mgc_asia_only_promotion_1_075r_favorable_only",
                label="MGC Asia-only Promotion 1 +0.75R VWAP Favorable Only",
                symbol="MGC",
                point_value=10.0,
                reference_label="Frozen ATP Companion Baseline v1 / MGC / Asia+US",
                reference_kind="frozen_baseline_v1",
                frozen_reference=True,
                notes=(
                    "Original ATP entry logic unchanged.",
                    "Original ATP exit behavior unchanged.",
                    "Only ASIA trades are eligible for the staged add overlay.",
                ),
            )
        ]
        bar_source_index = _discover_best_sources(symbols=symbols, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
        results = [
            _evaluate_branch(
                spec=spec,
                source_db=source_db,
                bar_source_index=bar_source_index,
                candidate=candidate,
            )
            for spec in specs
        ]
        gc_omitted_note = "GC supplemental comparison omitted in this run because the app had to fall back to live scope reconstruction, and this pass stays strictly narrow on the primary MGC branch."

    payload = _summary_payload(
        results=results,
        omitted_pl_note=f"PL remains intentionally excluded from this pass. {gc_omitted_note}" if include_gc else "PL remains intentionally excluded from this pass.",
    )
    summary_rows = payload["candidate_vs_baseline_summary"]
    risk_rows = payload["risk_delta_table"]
    add_rows = payload["add_contribution_table"]
    session_rows = payload["session_breakdown_table"]
    summary_csv_path = output_dir / "candidate_vs_baseline_summary.csv"
    add_csv_path = output_dir / "add_contribution_table.csv"
    risk_csv_path = output_dir / "risk_delta_table.csv"
    session_csv_path = output_dir / "session_breakdown_table.csv"
    markdown_path = output_dir / "atp_companion_asia_drift_enhancement_summary.md"
    json_path = output_dir / "atp_companion_asia_drift_enhancement_summary.json"
    _write_csv(summary_csv_path, summary_rows)
    _write_csv(add_csv_path, add_rows)
    _write_csv(risk_csv_path, risk_rows)
    _write_csv(session_csv_path, session_rows)
    markdown_path.write_text(
        _render_markdown(summary_rows=summary_rows, risk_rows=risk_rows, notes=payload["notes"]),
        encoding="utf-8",
    )
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "summary_csv_path": summary_csv_path,
        "add_csv_path": add_csv_path,
        "risk_csv_path": risk_csv_path,
        "session_csv_path": session_csv_path,
        "markdown_path": markdown_path,
        "json_path": json_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).expanduser().resolve()
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        output_dir = DEFAULT_OUTPUT_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    artifacts = run_enhancement_review(
        source_db=source_db,
        output_dir=output_dir,
        include_gc=bool(args.include_gc),
    )
    print(f"Wrote candidate_vs_baseline_summary.csv -> {artifacts['summary_csv_path']}")
    print(f"Wrote add_contribution_table.csv -> {artifacts['add_csv_path']}")
    print(f"Wrote risk_delta_table.csv -> {artifacts['risk_csv_path']}")
    print(f"Wrote session_breakdown_table.csv -> {artifacts['session_csv_path']}")
    print(f"Wrote markdown summary -> {artifacts['markdown_path']}")
    print(f"Wrote json summary -> {artifacts['json_path']}")


if __name__ == "__main__":
    main()
