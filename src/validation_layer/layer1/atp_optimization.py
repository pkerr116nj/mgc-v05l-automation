"""Bounded ATP promotion/add optimization-history attachment for validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from mgc_v05l.app.atp_companion_full_history_review import DEFAULT_SOURCE_DB
from mgc_v05l.research.trend_participation.atp_promotion_add_review import (
    DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG,
    DEFAULT_CANDIDATE_LANE_CONFIG,
    DEFAULT_FEE_PER_ADD,
    STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
    PromotionAddCandidate,
    _run_window_candidate_sample,
    _shared_1m_coverage,
    default_atp_promotion_add_candidates,
)
from mgc_v05l.research.trend_participation.models import ResearchBar
from mgc_v05l.research.trend_participation.performance_validation import _trade_metrics
from mgc_v05l.research.trend_participation.phase4 import build_rolling_windows

from ..data.contracts import (
    EquityPoint,
    OptimizationLinkage,
    OptimizationRun,
    OptimizationTrial,
    PositionPoint,
    PriceBar,
    SplitWindow,
    StrategyBacktest,
    TradeRecord,
)
from .producer_contract import (
    build_data_provenance,
    build_execution_assumptions,
    build_strategy_backtest_provenance,
    build_trade_validation_metadata,
)


@dataclass(frozen=True)
class AtpPromotionOptimizationBundle:
    strategy_backtest: StrategyBacktest
    optimization_run: OptimizationRun
    history_payload: dict[str, Any]


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path.cwd() / resolved
    return resolved.resolve(strict=False)


def _candidate_parameter_values(candidate: PromotionAddCandidate) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "progress_r_multiple": float(candidate.progress_r_multiple),
        "allowed_vwap_price_quality": tuple(candidate.allowed_price_quality_states),
        "require_positive_reacceleration": bool(candidate.require_positive_reacceleration),
        "require_low_above_entry": bool(candidate.require_low_above_entry),
        "max_adds_per_trade": int(candidate.max_adds_per_trade),
    }


def _search_space(candidates: Sequence[PromotionAddCandidate]) -> dict[str, Any]:
    return {
        "candidate_id": [candidate.candidate_id for candidate in candidates],
        "progress_r_multiple": sorted({float(candidate.progress_r_multiple) for candidate in candidates}),
        "allowed_vwap_price_quality": [tuple(candidate.allowed_price_quality_states) for candidate in candidates],
        "require_positive_reacceleration": sorted({bool(candidate.require_positive_reacceleration) for candidate in candidates}),
        "require_low_above_entry": sorted({bool(candidate.require_low_above_entry) for candidate in candidates}),
        "max_adds_per_trade": sorted({int(candidate.max_adds_per_trade) for candidate in candidates}),
    }


def _optimization_id(strategy_name: str, chosen_parameters: dict[str, Any], split_count: int) -> str:
    payload = {"strategy_name": strategy_name, "chosen_parameters": chosen_parameters, "split_count": split_count}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"atp_promotion_add:{digest[:16]}"


def build_optimization_run_from_atp_window_history(
    *,
    strategy_name: str,
    candidates: Sequence[PromotionAddCandidate],
    chosen_candidate_id: str,
    split_rows: Sequence[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
) -> OptimizationRun:
    candidate_map = {candidate.candidate_id: candidate for candidate in candidates}
    if chosen_candidate_id not in candidate_map:
        raise ValueError(f"Chosen ATP candidate {chosen_candidate_id} is not present in the candidate set.")

    trials: list[OptimizationTrial] = []
    for split_row in split_rows:
        split_id = str(split_row["split_id"])
        ordered = sorted(
            split_row["candidates"],
            key=lambda item: (
                float(item["objective_value"]),
                float(item["profit_factor_delta"]),
                float(item["average_trade_pnl_cash_delta"]),
                -float(item["max_drawdown_delta"]),
            ),
            reverse=True,
        )
        for rank, row in enumerate(ordered, start=1):
            candidate = candidate_map[str(row["candidate_id"])]
            trials.append(
                OptimizationTrial(
                    parameter_values=_candidate_parameter_values(candidate),
                    in_sample_metrics={
                        "net_pnl_cash": float(row["net_pnl_cash"]),
                        "net_pnl_cash_delta": float(row["objective_value"]),
                        "average_trade_pnl_cash": float(row["average_trade_pnl_cash"]),
                        "average_trade_pnl_cash_delta": float(row["average_trade_pnl_cash_delta"]),
                        "profit_factor": float(row["profit_factor"]),
                        "profit_factor_delta": float(row["profit_factor_delta"]),
                        "max_drawdown": float(row["max_drawdown"]),
                        "max_drawdown_delta": float(row["max_drawdown_delta"]),
                        "trade_count": float(row["trade_count"]),
                    },
                    out_of_sample_metrics={},
                    split_id=split_id,
                    rank=rank,
                    objective_value=float(row["objective_value"]),
                    metadata={
                        "window_label": split_row["window_label"],
                        "candidate_label": candidate.label,
                        "bars_processed": int(split_row["bars_processed"]),
                    },
                )
            )

    if not trials:
        raise ValueError("ATP optimization attachment requires at least one split row with candidate trials.")

    chosen_parameters = _candidate_parameter_values(candidate_map[chosen_candidate_id])
    run_id = _optimization_id(strategy_name, chosen_parameters, len(split_rows))
    return OptimizationRun(
        strategy_name=strategy_name,
        search_space=_search_space(candidates),
        trials=tuple(trials),
        chosen_parameters=chosen_parameters,
        objective_name="net_pnl_cash_delta_vs_frozen_baseline",
        metadata={
            "optimization_id": run_id,
            "source_family": "atp_promotion_add_review",
            "chosen_candidate_id": chosen_candidate_id,
            **(metadata or {}),
        },
    )


def _price_bars_from_research_bars(bars: Sequence[ResearchBar]) -> tuple[PriceBar, ...]:
    deduped: dict[tuple[str, datetime], ResearchBar] = {}
    for bar in bars:
        deduped[(bar.instrument, bar.end_ts)] = bar
    return tuple(
        PriceBar(
            timestamp=bar.end_ts,
            open=float(bar.open),
            high=float(bar.high),
            low=float(bar.low),
            close=float(bar.close),
            volume=float(bar.volume),
            session_label=str(bar.session_segment),
            metadata={
                "instrument": bar.instrument,
                "timeframe": bar.timeframe,
                "source": bar.source,
                "provenance": bar.provenance,
            },
        )
        for bar in sorted(deduped.values(), key=lambda item: (item.instrument, item.end_ts))
    )


def _build_equity_curve(trades: Sequence[TradeRecord], start: datetime, starting_equity: float = 100_000.0) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    curve = [EquityPoint(timestamp=start, equity=equity)]
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += float(trade.net_pnl)
        curve.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(curve)


def _build_position_series_from_candidate_rows(rows: Sequence[dict[str, Any]], start: datetime) -> tuple[PositionPoint, ...]:
    deltas: dict[datetime, float] = {start: 0.0}
    for row in rows:
        entry_time = row["entry_ts"]
        exit_time = row["exit_ts"]
        deltas[entry_time] = deltas.get(entry_time, 0.0) + 1.0
        add_entry = row.get("add_entry_ts")
        if add_entry is not None:
            deltas[add_entry] = deltas.get(add_entry, 0.0) + 1.0
            deltas[exit_time] = deltas.get(exit_time, 0.0) - 2.0
        else:
            deltas[exit_time] = deltas.get(exit_time, 0.0) - 1.0

    running = 0.0
    series: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running += deltas[timestamp]
        series.append(PositionPoint(timestamp=timestamp, position=running, exposure=running))
    return tuple(series)


def build_strategy_backtest_from_atp_candidate_history(
    *,
    strategy_name: str,
    candidate: PromotionAddCandidate,
    candidate_rows: Sequence[dict[str, Any]],
    price_bars: Sequence[ResearchBar],
    split_windows: Sequence[SplitWindow],
    data_version: str,
    code_version: str,
    source_sqlite_path: str | Path,
    optimization_run: OptimizationRun | None = None,
    artifact_references: dict[str, str] | None = None,
    fee_per_add: float = DEFAULT_FEE_PER_ADD,
) -> StrategyBacktest:
    if not candidate_rows:
        raise ValueError("ATP candidate backtest adaptation requires at least one evaluated candidate row.")

    ordered_rows = sorted(candidate_rows, key=lambda row: row["entry_ts"])
    price_bar_rows = _price_bars_from_research_bars(price_bars)
    start = min(price_bar_rows[0].timestamp if price_bar_rows else ordered_rows[0]["entry_ts"], ordered_rows[0]["entry_ts"])
    end = max(price_bar_rows[-1].timestamp if price_bar_rows else ordered_rows[-1]["exit_ts"], ordered_rows[-1]["exit_ts"])

    converted_trades: list[TradeRecord] = []
    for row in ordered_rows:
        added = bool(row.get("added"))
        entry_time = row["entry_ts"]
        exit_time = row["exit_ts"]
        converted_trades.append(
            TradeRecord(
                entry_time=entry_time,
                exit_time=exit_time,
                direction=str(row["side"]).lower(),
                qty=1.0,
                gross_pnl=float(row["pnl_cash"]),
                net_pnl=float(row["pnl_cash"]),
                mae=float(row["mae_points"]),
                mfe=float(row["mfe_points"]),
                holding_bars=int(row["bars_held_1m"]),
                holding_minutes=float(row["hold_minutes"]),
                session_label=str(row["session_segment"]),
                validation_metadata=build_trade_validation_metadata(
                    trade_id=f"{row['candidate_id']}:{row['instrument']}:{entry_time.isoformat()}",
                    signal_id=f"{row['candidate_id']}:{row['instrument']}:{row['decision_ts'].isoformat()}",
                    setup_family=str(row["family"]),
                    setup_variant=str(row["variant_id"]),
                    decision_time=row["decision_ts"],
                    exit_reason=str(row["exit_reason"]),
                    execution_model="atp_promotion_add_review",
                    fill_policy="baseline_entry_plus_earned_add_trigger",
                    fee_cost=fee_per_add if added else 0.0,
                    tags={
                        "candidate_id": row["candidate_id"],
                        "instrument": row["instrument"],
                        "added": added,
                        "add_price_quality_state": row.get("add_price_quality_state"),
                        "excursion_source": "baseline_position_reused_for_add_candidate",
                    },
                ),
                metadata={
                    "instrument": row["instrument"],
                    "candidate_id": row["candidate_id"],
                    "candidate_label": row["candidate_label"],
                    "variant_id": row["variant_id"],
                    "entry_price": float(row["position_entry_price"]),
                    "exit_price": float(row["position_exit_price"]),
                    "added": added,
                    "add_entry_ts": None if row.get("add_entry_ts") is None else row["add_entry_ts"].isoformat(),
                    "add_exit_ts": None if row.get("add_exit_ts") is None else row["add_exit_ts"].isoformat(),
                    "add_entry_price": row.get("add_entry_price"),
                    "add_pnl_cash": float(row["add_pnl_cash"]),
                    "effective_position_units": 2.0 if added else 1.0,
                    "gross_equals_net_reason": "baseline trade costs are inherited net and cannot be cleanly re-expanded from review rows",
                },
            )
        )

    optimization_linkage = None
    if optimization_run is not None:
        chosen_hash = hashlib.sha256(
            json.dumps(optimization_run.chosen_parameters, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        optimization_linkage = OptimizationLinkage(
            optimization_id=str(optimization_run.metadata.get("optimization_id") or _optimization_id(strategy_name, optimization_run.chosen_parameters, len(split_windows))),
            objective_name=optimization_run.objective_name,
            chosen_parameter_hash=chosen_hash,
            source_artifact_id=str((artifact_references or {}).get("optimization_history", "atp_promotion_add_window_history")),
            split_family="rolling_window_research_evaluation",
        )

    provenance = build_strategy_backtest_provenance(
        producer_id="trend_participation.atp_promotion_add_review",
        producer_family="atp_promotion_add_review",
        study_mode="research_replay_candidate_optimization",
        split_method="rolling_window_research_evaluation",
        split_windows=tuple(split_windows),
        execution_assumptions=build_execution_assumptions(
            fill_policy="baseline_entry_plus_earned_add_trigger",
            entry_fill_basis="frozen_baseline_entry_plus_trigger_price_max_open",
            exit_fill_basis="inherits_frozen_baseline_exit",
            execution_mode="research_replay",
            slippage_model_name="embedded_baseline_cost_plus_fee_per_add",
            slippage_model_version="v1",
            slippage_parameters={"baseline_trade_costs_embedded": True, "additional_slippage_per_add": 0.0},
            fee_model_name="fixed_fee_per_add_only",
            fee_model_version="v1",
            fee_parameters={"fee_per_add": fee_per_add, "baseline_trade_fees_embedded": True},
            conservative_bias="candidate rows inherit frozen baseline exits and keep the explicit per-add fee haircut",
            notes=(candidate.label, DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG, DEFAULT_CANDIDATE_LANE_CONFIG),
        ),
        data_provenance=build_data_provenance(
            source_type="sqlite_replay_rerun",
            data_source="trend_participation.atp_promotion_add_review",
            data_version=data_version,
            provenance_id=f"atp_promotion_add:{candidate.candidate_id}:{data_version}",
            feature_version="ATP_PROMOTION_ADD_REVIEW_V1",
            code_version=code_version,
            coverage_start=start,
            coverage_end=end,
            artifact_ids={"candidate_id": candidate.candidate_id, "source_db": str(_resolve_path(source_sqlite_path).name)},
            tags=("atp_promotion_add_review", "layer1_ready", "optimization_history_attached"),
        ),
        optimization_linkage=optimization_linkage,
        artifact_references=artifact_references or {},
        tags=("layer1_ready", "atp_promotion_add_review", candidate.candidate_id),
    )
    return StrategyBacktest(
        strategy_name=strategy_name,
        symbol=str(ordered_rows[0]["instrument"]),
        timeframe="1m",
        parameters=_candidate_parameter_values(candidate),
        bar_data=price_bar_rows,
        trades=tuple(converted_trades),
        equity_curve=_build_equity_curve(converted_trades, start=start),
        position_series=_build_position_series_from_candidate_rows(ordered_rows, start=start),
        provenance=provenance,
        metadata={
            "pilot_family": "atp_promotion_add_review",
            "candidate_id": candidate.candidate_id,
            "candidate_label": candidate.label,
            "review_scope": {"source_sqlite_path": str(_resolve_path(source_sqlite_path)), "window_count": len(split_windows)},
        },
    )


def rerun_atp_promotion_add_validation_bundle(
    *,
    source_sqlite_path: str | Path = DEFAULT_SOURCE_DB,
    instruments: tuple[str, ...] = ("MGC",),
    point_value: float = 10.0,
    window_days: int = 1,
    max_windows: int | None = 8,
    chosen_candidate_id: str = STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
    strategy_name: str | None = None,
    code_version: str = "pilot",
) -> AtpPromotionOptimizationBundle:
    source_db = _resolve_path(source_sqlite_path)
    candidates = tuple(default_atp_promotion_add_candidates())
    candidate_map = {candidate.candidate_id: candidate for candidate in candidates}
    if chosen_candidate_id not in candidate_map:
        raise ValueError(f"Chosen ATP candidate {chosen_candidate_id} is not present in the default candidate set.")

    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_db, instruments=instruments)
    windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=window_days)
    if max_windows is not None:
        windows = windows[-max_windows:]

    split_rows: list[dict[str, Any]] = []
    chosen_candidate_rows: list[dict[str, Any]] = []
    all_price_bars: list[ResearchBar] = []
    split_windows: list[SplitWindow] = []
    for index, window in enumerate(windows, start=1):
        run_payload = _run_window_candidate_sample(
            source_sqlite_path=source_db,
            instruments=instruments,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            higher_priority_signals=(),
            point_value=point_value,
            candidates=candidates,
        )
        if int(run_payload["bar_count"]) <= 0:
            continue
        baseline_metrics = _trade_metrics(run_payload["baseline_rows"], bar_count=int(run_payload["bar_count"]))
        split_id = f"promotion_window_{index}"
        split_windows.append(
            SplitWindow(
                split_id=split_id,
                fold_index=index - 1,
                role="evaluation",
                train_start=window.start_ts,
                train_end=window.end_ts,
                test_start=window.start_ts,
                test_end=window.end_ts,
                notes=(f"window_label={run_payload['summary_row']['label']}",),
            )
        )
        all_price_bars.extend(run_payload.get("price_bars") or [])
        split_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            rows = list(run_payload["candidate_rows"][candidate.candidate_id])
            metrics = _trade_metrics(rows, bar_count=int(run_payload["bar_count"]))
            objective_value = round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 6)
            split_candidates.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_label": candidate.label,
                    "net_pnl_cash": float(metrics["net_pnl_cash"]),
                    "trade_count": int(metrics["total_trades"]),
                    "average_trade_pnl_cash": float(metrics["average_trade_pnl_cash"]),
                    "profit_factor": float(metrics["profit_factor"]),
                    "max_drawdown": float(metrics["max_drawdown"]),
                    "objective_value": objective_value,
                    "average_trade_pnl_cash_delta": round(
                        float(metrics["average_trade_pnl_cash"]) - float(baseline_metrics["average_trade_pnl_cash"]),
                        6,
                    ),
                    "profit_factor_delta": round(
                        float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]),
                        6,
                    ),
                    "max_drawdown_delta": round(
                        float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]),
                        6,
                    ),
                }
            )
            if candidate.candidate_id == chosen_candidate_id:
                chosen_candidate_rows.extend(rows)
        split_rows.append(
            {
                "split_id": split_id,
                "window_label": run_payload["summary_row"]["label"],
                "bars_processed": int(run_payload["bar_count"]),
                "baseline_metrics": baseline_metrics,
                "candidates": split_candidates,
            }
        )

    strategy_id = strategy_name or f"atp_promotion_add::{chosen_candidate_id}::{'_'.join(symbol.lower() for symbol in instruments)}"
    optimization_run = build_optimization_run_from_atp_window_history(
        strategy_name=strategy_id,
        candidates=candidates,
        chosen_candidate_id=chosen_candidate_id,
        split_rows=split_rows,
        metadata={
            "source_sqlite_path": str(source_db),
            "instruments": list(instruments),
            "window_days": window_days,
            "window_count": len(split_rows),
            "candidate_registry_config": DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG,
            "candidate_config": DEFAULT_CANDIDATE_LANE_CONFIG,
        },
    )
    optimization_history_id = str(optimization_run.metadata.get("optimization_id") or "atp_promotion_add_history")
    strategy_backtest = build_strategy_backtest_from_atp_candidate_history(
        strategy_name=strategy_id,
        candidate=candidate_map[chosen_candidate_id],
        candidate_rows=chosen_candidate_rows,
        price_bars=all_price_bars,
        split_windows=split_windows,
        data_version=f"{chosen_candidate_id}:{len(split_rows)}_windows",
        code_version=code_version,
        source_sqlite_path=source_db,
        optimization_run=optimization_run,
        artifact_references={
            "source_sqlite_path": str(source_db),
            "candidate_registry_config": DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG,
            "candidate_config": DEFAULT_CANDIDATE_LANE_CONFIG,
            "optimization_history": optimization_history_id,
        },
    )
    history_payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "strategy_name": strategy_id,
        "chosen_candidate_id": chosen_candidate_id,
        "source_family": "atp_promotion_add_review",
        "source_sqlite_path": str(source_db),
        "instruments": list(instruments),
        "window_days": window_days,
        "window_count": len(split_rows),
        "candidate_registry_config": DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG,
        "candidate_config": DEFAULT_CANDIDATE_LANE_CONFIG,
        "candidates": [
            {
                "candidate_id": candidate.candidate_id,
                "label": candidate.label,
                "parameters": _candidate_parameter_values(candidate),
            }
            for candidate in candidates
        ],
        "split_rows": split_rows,
    }
    return AtpPromotionOptimizationBundle(
        strategy_backtest=strategy_backtest,
        optimization_run=optimization_run,
        history_payload=history_payload,
    )
