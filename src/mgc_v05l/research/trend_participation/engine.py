"""End-to-end orchestration for Active Trend Participation Engine research."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .backtest import backtest_decisions_with_audit, rank_variants_for_training, summarize_performance
from .features import build_feature_states
from .models import (
    HigherPrioritySignal,
    TrendParticipationArtifacts,
    VariantEvaluation,
    WalkForwardFoldResult,
)
from .patterns import default_pattern_variants, generate_signal_decisions, summarize_signal_contexts
from .phase2_continuation import (
    build_phase2_replay_package,
    summarize_phase2_entry_diagnostics,
    write_phase2_artifacts,
)
from .phase3_timing import build_phase3_replay_package, summarize_phase3_timing_diagnostics, write_phase3_artifacts
from .performance_validation import (
    build_atp_performance_validation_report,
    write_atp_performance_validation_artifacts,
)
from .report import build_report_payload, write_report_files
from .state_layers import render_atp_state_diagnostics_markdown, summarize_atp_state_diagnostics
from .storage import (
    build_layout,
    load_sqlite_bars,
    materialize_parquet_dataset,
    normalize_and_check_bars,
    register_duckdb_views,
    resample_bars_from_1m,
    serialize_rows,
    write_storage_manifest,
)


DEFAULT_POINT_VALUES = {
    "MES": 5.0,
    "MNQ": 2.0,
}


def run_trend_participation_engine(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MES", "MNQ"),
    mode: str = "research",
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    materialize_storage: bool = True,
    variant_profile: str = "phase3_full",
) -> TrendParticipationArtifacts:
    layout = build_layout(output_dir)
    all_quality_issues = []
    all_feature_rows = []
    all_decisions = []
    all_shadow_trades = []
    all_live_trades = []
    all_phase2_entry_states = []
    all_phase2_decisions = []
    all_phase2_shadow_trades = []
    all_phase3_timing_states = []
    all_phase3_shadow_trades = []
    per_instrument_summary: dict[str, Any] = {}

    for instrument in instruments:
        raw_1m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalized_1m, issues_1m = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
        raw_5m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="5m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not raw_5m and normalized_1m:
            raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        normalized_5m, issues_5m = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
        if normalized_1m:
            first_1m_ts = normalized_1m[0].end_ts
            last_1m_ts = normalized_1m[-1].end_ts
            normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        all_quality_issues.extend(issues_1m)
        all_quality_issues.extend(issues_5m)

        feature_rows = build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m)
        variants = default_pattern_variants(profile=variant_profile)
        decisions = generate_signal_decisions(
            feature_rows=feature_rows,
            variants=variants,
            higher_priority_signals=higher_priority_signals,
        )
        variants_by_id = {variant.variant_id: variant for variant in variants}
        shadow_trades, shadow_audit = backtest_decisions_with_audit(
            decisions=decisions,
            bars_1m=normalized_1m,
            variants_by_id=variants_by_id,
            point_values=DEFAULT_POINT_VALUES,
            include_shadow_only=True,
        )
        live_trades, live_audit = backtest_decisions_with_audit(
            decisions=decisions,
            bars_1m=normalized_1m,
            variants_by_id=variants_by_id,
            point_values=DEFAULT_POINT_VALUES,
            include_shadow_only=False,
        )
        phase2_package = build_phase2_replay_package(
            feature_rows=feature_rows,
            bars_1m=normalized_1m,
            higher_priority_signals=higher_priority_signals,
            point_value=DEFAULT_POINT_VALUES.get(instrument, 5.0),
        )
        phase3_package = build_phase3_replay_package(
            entry_states=phase2_package["entry_states"],
            bars_1m=normalized_1m,
            point_value=DEFAULT_POINT_VALUES.get(instrument, 5.0),
            old_proxy_trade_count=len(phase2_package["shadow_trades"]),
        )

        all_feature_rows.extend(feature_rows)
        all_decisions.extend(decisions)
        all_shadow_trades.extend(shadow_trades)
        all_live_trades.extend(live_trades)
        all_phase2_entry_states.extend(phase2_package["entry_states"])
        all_phase2_decisions.extend(phase2_package["decisions"])
        all_phase2_shadow_trades.extend(phase2_package["shadow_trades"])
        all_phase3_timing_states.extend(phase3_package["timing_states"])
        all_phase3_shadow_trades.extend(phase3_package["shadow_trades"])

        per_instrument_summary[instrument] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_5m": len(normalized_5m),
            "feature_count": len(feature_rows),
            "signal_count": len(decisions),
            "atp_phase2_entry_state_count": len(phase2_package["entry_states"]),
            "atp_phase2_signal_count": len(phase2_package["decisions"]),
            "atp_phase2_trade_count": len(phase2_package["shadow_trades"]),
            "atp_phase3_timing_state_count": len(phase3_package["timing_states"]),
            "atp_phase3_trade_count": len(phase3_package["shadow_trades"]),
            "quality_issue_count": len(issues_1m) + len(issues_5m),
            "provenance_sources": sorted({bar.source for bar in normalized_1m + normalized_5m}),
            "trading_calendars": sorted({bar.trading_calendar for bar in normalized_1m + normalized_5m}),
            "shadow_execution_audit": serialize_rows(shadow_audit),
            "live_execution_audit": serialize_rows(live_audit),
            "atp_phase2_shadow_execution_audit": serialize_rows(phase2_package["shadow_audit"]),
            "atp_phase2_diagnostics": phase2_package["diagnostics"],
            "atp_phase3_diagnostics": phase3_package["diagnostics"],
        }

        if materialize_storage:
            _materialize_instrument_storage(
                layout=layout,
                instrument=instrument,
                bars_1m=normalized_1m,
                bars_5m=normalized_5m,
                feature_rows=feature_rows,
                decisions=[item for item in decisions if item.instrument == instrument],
                shadow_trades=[item for item in shadow_trades if item.instrument == instrument],
                live_trades=[item for item in live_trades if item.instrument == instrument],
                phase2_entry_states=[item for item in phase2_package["entry_states"] if item.instrument == instrument],
                phase2_decisions=[item for item in phase2_package["decisions"] if item.instrument == instrument],
                phase2_shadow_trades=[item for item in phase2_package["shadow_trades"] if item.instrument == instrument],
                phase3_timing_states=[item for item in phase3_package["timing_states"] if item.instrument == instrument],
                phase3_shadow_trades=[item for item in phase3_package["shadow_trades"] if item.instrument == instrument],
            )

    storage_manifest = {
        "mode": mode,
        "source_sqlite_path": str(source_sqlite_path.resolve()),
        "layout": {key: str(value) for key, value in layout.items()},
        "instruments": list(instruments),
        "variant_profile": variant_profile,
        "instrument_summary": per_instrument_summary,
        "quality_issues": serialize_rows(all_quality_issues),
        "dependency_materialization": materialize_storage,
    }
    write_storage_manifest(layout["storage_manifest"], storage_manifest)

    phase1_diagnostics = summarize_atp_state_diagnostics(all_feature_rows)
    phase1_diagnostics_json_path = layout["reports"] / "atp_phase1_state_diagnostics.json"
    phase1_diagnostics_markdown_path = layout["reports"] / "atp_phase1_state_diagnostics.md"
    phase1_diagnostics_json_path.write_text(
        json.dumps(phase1_diagnostics, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    phase1_diagnostics_markdown_path.write_text(
        render_atp_state_diagnostics_markdown(phase1_diagnostics),
        encoding="utf-8",
    )
    phase2_diagnostics = summarize_phase2_entry_diagnostics(
        feature_rows=all_feature_rows,
        entry_states=all_phase2_entry_states,
        decisions=all_phase2_decisions,
        trades=all_phase2_shadow_trades,
    )
    phase2_diagnostics_json_path, phase2_diagnostics_markdown_path = write_phase2_artifacts(
        reports_dir=layout["reports"],
        diagnostics=phase2_diagnostics,
    )
    phase3_diagnostics = summarize_phase3_timing_diagnostics(
        timing_states=all_phase3_timing_states,
        trades=all_phase3_shadow_trades,
        old_proxy_trade_count=len(all_phase2_shadow_trades),
    )
    phase3_diagnostics_json_path, phase3_diagnostics_markdown_path = write_phase3_artifacts(
        reports_dir=layout["reports"],
        diagnostics=phase3_diagnostics,
    )
    performance_validation = build_atp_performance_validation_report(
        bar_count=len(all_feature_rows),
        entry_states=all_phase2_entry_states,
        timing_states=all_phase3_timing_states,
        atp_trades=all_phase3_shadow_trades,
        legacy_proxy_trades=all_phase2_shadow_trades,
    )
    performance_validation_json_path, performance_validation_markdown_path = write_atp_performance_validation_artifacts(
        reports_dir=layout["reports"],
        payload=performance_validation,
    )

    evaluations = _evaluate_variants(all_decisions=all_decisions, all_shadow_trades=all_shadow_trades, all_live_trades=all_live_trades)
    payload = build_report_payload(
        evaluations=evaluations,
        data_summary={
            "mode": mode,
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "instrument_summary": per_instrument_summary,
            "quality_issue_count": len(all_quality_issues),
            "signal_contexts": summarize_signal_contexts(all_decisions),
            "atp_phase1_state_diagnostics": phase1_diagnostics,
            "atp_phase1_state_diagnostics_artifacts": {
                "json_path": str(phase1_diagnostics_json_path),
                "markdown_path": str(phase1_diagnostics_markdown_path),
            },
            "atp_phase2_entry_diagnostics": phase2_diagnostics,
            "atp_phase2_entry_diagnostics_artifacts": {
                "json_path": str(phase2_diagnostics_json_path),
                "markdown_path": str(phase2_diagnostics_markdown_path),
            },
            "atp_phase3_timing_diagnostics": phase3_diagnostics,
            "atp_phase3_timing_diagnostics_artifacts": {
                "json_path": str(phase3_diagnostics_json_path),
                "markdown_path": str(phase3_diagnostics_markdown_path),
            },
            "atp_performance_validation": performance_validation,
            "atp_performance_validation_artifacts": {
                "json_path": str(performance_validation_json_path),
                "markdown_path": str(performance_validation_markdown_path),
            },
        },
        storage_manifest=storage_manifest,
    )
    report_json_path = layout["reports"] / "trend_participation_engine_report.json"
    report_markdown_path = layout["reports"] / "trend_participation_engine_report.md"
    write_report_files(json_path=report_json_path, markdown_path=report_markdown_path, payload=payload)
    return TrendParticipationArtifacts(
        root_dir=layout["root"],
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
        storage_manifest_path=layout["storage_manifest"],
        report=payload,
        storage_manifest=storage_manifest,
        phase1_diagnostics_json_path=phase1_diagnostics_json_path,
        phase1_diagnostics_markdown_path=phase1_diagnostics_markdown_path,
        phase2_diagnostics_json_path=phase2_diagnostics_json_path,
        phase2_diagnostics_markdown_path=phase2_diagnostics_markdown_path,
        phase3_diagnostics_json_path=phase3_diagnostics_json_path,
        phase3_diagnostics_markdown_path=phase3_diagnostics_markdown_path,
        performance_validation_json_path=performance_validation_json_path,
        performance_validation_markdown_path=performance_validation_markdown_path,
    )


def _evaluate_variants(*, all_decisions, all_shadow_trades, all_live_trades) -> tuple[VariantEvaluation, ...]:
    variants = {variant.variant_id: variant for variant in default_pattern_variants()}
    decisions_by_variant: dict[str, list] = defaultdict(list)
    shadow_trades_by_variant: dict[str, list] = defaultdict(list)
    live_trades_by_variant: dict[str, list] = defaultdict(list)
    for decision in all_decisions:
        decisions_by_variant[decision.variant_id].append(decision)
    for trade in all_shadow_trades:
        shadow_trades_by_variant[trade.variant_id].append(trade)
    for trade in all_live_trades:
        live_trades_by_variant[trade.variant_id].append(trade)

    fold_results = _walk_forward_results(all_decisions=all_decisions, all_shadow_trades=all_shadow_trades)
    folds_by_variant: dict[str, list[WalkForwardFoldResult]] = defaultdict(list)
    for fold in fold_results:
        for variant_id in fold.selected_variants:
            folds_by_variant[variant_id].append(fold)

    evaluations = []
    for variant_id, variant in variants.items():
        decision_bucket = decisions_by_variant.get(variant_id, [])
        conflict_breakdown = Counter(str(item.conflict_outcome.value) for item in decision_bucket)
        shadow_metrics = summarize_performance(shadow_trades_by_variant.get(variant_id, []))
        live_metrics = summarize_performance(live_trades_by_variant.get(variant_id, []))
        oos_metrics = _aggregate_fold_metrics(fold_results=fold_results, variant_id=variant_id)
        stability = _parameter_stability(variant=variant, metrics_by_variant={key: summarize_performance(value) for key, value in shadow_trades_by_variant.items()})
        notes = []
        if live_metrics.trade_count < shadow_metrics.trade_count:
            notes.append("priority_conflicts_reduce_live_trade_count")
        if shadow_metrics.profit_factor <= 1.0:
            notes.append("shadow_profit_factor_needs_improvement")
        if shadow_metrics.trades_per_day < 1.0:
            notes.append("participation_rate_low_for_active_v1")
        evaluations.append(
            VariantEvaluation(
                variant=variant,
                shadow_metrics=shadow_metrics,
                live_metrics=live_metrics,
                out_of_sample_metrics=oos_metrics,
                fold_results=tuple(folds_by_variant.get(variant_id, [])),
                conflict_breakdown=dict(conflict_breakdown),
                parameter_stability=stability,
                robustness_notes=tuple(notes),
            )
        )
    return tuple(evaluations)


def _walk_forward_results(*, all_decisions, all_shadow_trades) -> tuple[WalkForwardFoldResult, ...]:
    dates = sorted({decision.session_date.isoformat() for decision in all_decisions})
    if len(dates) < 3:
        return ()
    bucket_size = max(1, len(dates) // 3)
    date_buckets = [dates[index : index + bucket_size] for index in range(0, len(dates), bucket_size)]
    if len(date_buckets) < 2:
        return ()

    trades_by_variant: dict[str, list] = defaultdict(list)
    for trade in all_shadow_trades:
        trades_by_variant[trade.variant_id].append(trade)

    folds: list[WalkForwardFoldResult] = []
    for index in range(1, len(date_buckets)):
        train_dates = tuple(date for bucket in date_buckets[:index] for date in bucket)
        test_dates = tuple(date_buckets[index])
        train_trades_by_variant = {
            variant_id: [
                trade
                for trade in trades
                if trade.decision_ts.date().isoformat() in train_dates
            ]
            for variant_id, trades in trades_by_variant.items()
        }
        rankings = rank_variants_for_training(trades_by_variant=train_trades_by_variant)
        selected = tuple(row["variant_id"] for row in rankings[:6] if row["trade_count"] > 0)
        oos_metrics = {}
        for variant_id in selected:
            bucket = [
                trade
                for trade in trades_by_variant.get(variant_id, [])
                if trade.decision_ts.date().isoformat() in test_dates
            ]
            oos_metrics[variant_id] = summarize_performance(bucket)
        folds.append(
            WalkForwardFoldResult(
                fold_id=f"fold_{index}",
                train_dates=train_dates,
                test_dates=test_dates,
                selected_variants=selected,
                train_rankings=tuple(rankings[:10]),
                oos_variant_metrics=oos_metrics,
            )
        )
    return tuple(folds)


def _aggregate_fold_metrics(*, fold_results: tuple[WalkForwardFoldResult, ...], variant_id: str):
    metrics = [fold.oos_variant_metrics[variant_id] for fold in fold_results if variant_id in fold.oos_variant_metrics]
    if not metrics:
        return summarize_performance([])
    trade_count = sum(metric.trade_count for metric in metrics)
    active_days = sum(metric.active_days for metric in metrics)
    gross_profit = sum(metric.gross_profit for metric in metrics)
    gross_loss = sum(metric.gross_loss for metric in metrics)
    net_pnl_cash = sum(metric.net_pnl_cash for metric in metrics)
    total_fees = sum(metric.total_fees for metric in metrics)
    total_slippage_cost = sum(metric.total_slippage_cost for metric in metrics)
    gross_pnl_before_cost = sum(metric.gross_pnl_before_cost for metric in metrics)
    reentry_trade_count = sum(metric.reentry_trade_count for metric in metrics)
    return type(metrics[0])(
        trade_count=trade_count,
        active_days=active_days,
        trades_per_day=trade_count / max(active_days, 1),
        expectancy=net_pnl_cash / max(trade_count, 1),
        expectancy_per_hour=sum(metric.expectancy_per_hour for metric in metrics) / len(metrics),
        profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else gross_profit,
        max_drawdown=max(metric.max_drawdown for metric in metrics),
        win_rate=sum(metric.win_rate for metric in metrics) / len(metrics),
        avg_win=sum(metric.avg_win for metric in metrics) / len(metrics),
        avg_loss=sum(metric.avg_loss for metric in metrics) / len(metrics),
        avg_hold_minutes=sum(metric.avg_hold_minutes for metric in metrics) / len(metrics),
        stopout_rate=sum(metric.stopout_rate for metric in metrics) / len(metrics),
        reentry_trade_count=reentry_trade_count,
        reentry_expectancy=sum(metric.reentry_expectancy for metric in metrics) / len(metrics),
        net_pnl_cash=net_pnl_cash,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        gross_pnl_before_cost=gross_pnl_before_cost,
        total_fees=total_fees,
        total_slippage_cost=total_slippage_cost,
        long_trade_count=sum(metric.long_trade_count for metric in metrics),
        short_trade_count=sum(metric.short_trade_count for metric in metrics),
        by_session={"OOS": {"trade_count": float(trade_count), "trades_per_day": trade_count / max(active_days, 1)}},
        by_regime={"OOS": {"trade_count": float(trade_count), "trades_per_day": trade_count / max(active_days, 1)}},
        by_volatility={"OOS": {"trade_count": float(trade_count), "trades_per_day": trade_count / max(active_days, 1)}},
    )


def _parameter_stability(*, variant, metrics_by_variant) -> float:
    sibling_variants = [
        key
        for key in metrics_by_variant
        if key.startswith(f"trend_participation.{variant.family}.{variant.side.lower()}.")
    ]
    if not sibling_variants:
        return 0.0
    positive_neighbors = sum(1 for key in sibling_variants if metrics_by_variant[key].expectancy >= 0.0)
    return positive_neighbors / len(sibling_variants)


def _materialize_instrument_storage(
    *,
    layout: dict[str, Path],
    instrument: str,
    bars_1m,
    bars_5m,
    feature_rows,
    decisions,
    shadow_trades,
    live_trades,
    phase2_entry_states,
    phase2_decisions,
    phase2_shadow_trades,
    phase3_timing_states,
    phase3_shadow_trades,
) -> None:
    raw_1m_path = layout["raw"] / f"{instrument.lower()}_1m.parquet"
    raw_5m_path = layout["raw"] / f"{instrument.lower()}_5m.parquet"
    features_path = layout["features"] / f"{instrument.lower()}_5m_features.parquet"
    signals_path = layout["signals"] / f"{instrument.lower()}_signals.parquet"
    phase2_entry_states_path = layout["signals"] / f"{instrument.lower()}_atp_phase2_entry_states.parquet"
    phase2_signals_path = layout["signals"] / f"{instrument.lower()}_atp_phase2_signals.parquet"
    phase3_timing_states_path = layout["signals"] / f"{instrument.lower()}_atp_phase3_timing_states.parquet"
    shadow_trades_path = layout["trades"] / f"{instrument.lower()}_shadow_trades.parquet"
    live_trades_path = layout["trades"] / f"{instrument.lower()}_live_trades.parquet"
    phase2_shadow_trades_path = layout["trades"] / f"{instrument.lower()}_atp_phase2_shadow_trades.parquet"
    phase3_shadow_trades_path = layout["trades"] / f"{instrument.lower()}_atp_phase3_shadow_trades.parquet"

    materialize_parquet_dataset(raw_1m_path, serialize_rows(bars_1m))
    materialize_parquet_dataset(raw_5m_path, serialize_rows(bars_5m))
    materialize_parquet_dataset(features_path, serialize_rows(feature_rows))
    materialize_parquet_dataset(signals_path, serialize_rows(decisions))
    materialize_parquet_dataset(phase2_entry_states_path, serialize_rows(phase2_entry_states))
    materialize_parquet_dataset(phase2_signals_path, serialize_rows(phase2_decisions))
    materialize_parquet_dataset(phase3_timing_states_path, serialize_rows(phase3_timing_states))
    materialize_parquet_dataset(shadow_trades_path, serialize_rows(shadow_trades))
    materialize_parquet_dataset(live_trades_path, serialize_rows(live_trades))
    materialize_parquet_dataset(phase2_shadow_trades_path, serialize_rows(phase2_shadow_trades))
    materialize_parquet_dataset(phase3_shadow_trades_path, serialize_rows(phase3_shadow_trades))
    register_duckdb_views(
        duckdb_path=layout["duckdb"],
        parquet_map={
            f"raw_{instrument.lower()}_1m": raw_1m_path,
            f"raw_{instrument.lower()}_5m": raw_5m_path,
            f"features_{instrument.lower()}": features_path,
            f"signals_{instrument.lower()}": signals_path,
            f"atp_phase2_entry_states_{instrument.lower()}": phase2_entry_states_path,
            f"atp_phase2_signals_{instrument.lower()}": phase2_signals_path,
            f"atp_phase3_timing_states_{instrument.lower()}": phase3_timing_states_path,
            f"shadow_trades_{instrument.lower()}": shadow_trades_path,
            f"live_trades_{instrument.lower()}": live_trades_path,
            f"atp_phase2_shadow_trades_{instrument.lower()}": phase2_shadow_trades_path,
            f"atp_phase3_shadow_trades_{instrument.lower()}": phase3_shadow_trades_path,
        },
    )
