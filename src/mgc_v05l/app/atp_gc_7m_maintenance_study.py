"""GC-only 7-minute maintenance exit study guarded by control reconciliation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..research.trend_participation.models import TradeRecord
from ..research.trend_participation.phase3_timing import (
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_2OF3,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_LOOSE_HOLD,
    ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_STRICT_WEAK,
)
from .atp_gc_exit_evolution import (
    BASELINE_REFERENCE_ID,
    BASELINE_REFERENCE_LABEL,
    DEFAULT_POINT_VALUE,
    DEFAULT_SCOPE_BUNDLE_MANIFEST,
    _baseline_confirmation_profile,
    _replay_trade_records_under_exit_policy,
)
from .atp_scope_replay_probe import (
    _apply_confirmation_sizing_profile,
    _load_bars_for_intervals,
    _merged_replay_intervals,
    load_scope_replay_probe_bundle,
)

DEFAULT_OUTPUT_DIR = Path("outputs/reports/atp_gc_7m_maintenance_study")
DEFAULT_REFERENCE_EXIT_EVOLUTION_JSON = Path(
    "outputs/reports/atp_gc_exit_evolution_20260419/atp_gc_exit_evolution.json"
)
CONTROL_15M_ID = "checkpoint100_no_traction_abort_15m_2of3_control_reproduced"
CONTROL_10M_ID = "checkpoint075_no_traction_abort_10m_double_weak_close_control_reproduced"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-gc-7m-maintenance-study")
    parser.add_argument(
        "--scope-bundle-manifest",
        default=str(DEFAULT_SCOPE_BUNDLE_MANIFEST),
        help="Path to the GC ATP scope-bundle manifest to replay.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for the GC 7m maintenance study.",
    )
    parser.add_argument(
        "--reference-exit-evolution-json",
        default=str(DEFAULT_REFERENCE_EXIT_EVOLUTION_JSON),
        help="Prior GC exit-evolution JSON used to verify control reproduction.",
    )
    parser.add_argument(
        "--point-value",
        type=float,
        default=DEFAULT_POINT_VALUE,
        help="Optional point-value override. Defaults to GC $100/point.",
    )
    return parser


def run_gc_7m_maintenance_study(
    *,
    scope_bundle_manifest: Path,
    output_dir: Path,
    reference_exit_evolution_json: Path,
    point_value: float = DEFAULT_POINT_VALUE,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_scope_replay_probe_bundle(scope_bundle_manifest)
    merged_intervals = _merged_replay_intervals(
        timing_states=bundle.timing_states,
        window_minutes=480,
    )
    bars_1m = _load_bars_for_intervals(
        sqlite_path=Path(bundle.manifest["source_db"]),
        symbol=str(bundle.manifest["symbol"]).upper(),
        intervals=merged_intervals,
    )
    baseline_trades, baseline_sizing_summary = _apply_confirmation_sizing_profile(
        trades=bundle.trade_records,
        bars_1m=bars_1m,
        point_value=point_value,
        profile=_baseline_confirmation_profile(),
    )
    baseline_metrics = _variant_metrics(baseline_trades, point_value=point_value)

    results: list[dict[str, Any]] = []
    for spec in _variant_specs():
        replayed = _replay_trade_records_under_exit_policy(
            trades=bundle.trade_records,
            bars_1m=bars_1m,
            point_value=point_value,
            exit_policy=spec["exit_policy"],
        )
        sized_trades, sizing_summary = _apply_confirmation_sizing_profile(
            trades=replayed,
            bars_1m=bars_1m,
            point_value=point_value,
            profile=_baseline_confirmation_profile(),
        )
        metrics = _variant_metrics(sized_trades, point_value=point_value)
        results.append(
            {
                **spec,
                "metrics": metrics,
                "probe_summary": {
                    "trade_source": "bundle_trade_records_replayed_exit_policy",
                    "bars_loaded": len(bars_1m),
                    "merged_intervals": len(merged_intervals),
                    "confirmation_add_trade_count": sizing_summary["confirmation_add_trade_count"],
                    "confirmation_add_net_pnl": sizing_summary["confirmation_add_net_pnl"],
                },
                "comparison_vs_baseline": _comparison_metrics(
                    baseline=baseline_metrics,
                    candidate=metrics,
                ),
            }
        )

    by_id = {row["variant_id"]: row for row in results}
    control_15m = by_id[CONTROL_15M_ID]
    for row in results:
        row["comparison_vs_15m_control"] = _comparison_metrics(
            baseline=control_15m["metrics"],
            candidate=row["metrics"],
        )

    reference_status = _control_reconciliation_status(
        scope_bundle_manifest=scope_bundle_manifest,
        reference_exit_evolution_json=reference_exit_evolution_json,
        control_15m=control_15m,
        control_10m=by_id.get(CONTROL_10M_ID),
    )
    best_7m = _best_7m_variant(results)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study": "atp_gc_7m_maintenance_study",
        "baseline_reference": {
            "strategy_id": BASELINE_REFERENCE_ID,
            "label": BASELINE_REFERENCE_LABEL,
            "scope_bundle_manifest": str(scope_bundle_manifest.resolve()),
        },
        "implementation_summary": {
            "files_touched": [
                "src/mgc_v05l/research/trend_participation/phase3_timing.py",
                "src/mgc_v05l/app/atp_gc_7m_maintenance_study.py",
                "src/mgc_v05l/app/main.py",
                "tests/unit/test_trend_participation_engine.py",
                "tests/unit/test_mgc_v05l_atp_gc_7m_maintenance_study.py",
            ],
            "how_7m_bars_were_built": (
                "Completed 7-minute bars are aggregated from the same replay 1m bar substrate used by the clean exit-evolution pass. "
                "Buckets are aligned to absolute UTC clock-time boundaries via the shared maintenance bucket helper, not session-start offsets."
            ),
            "bar_anchoring_assumptions": [
                "Aggregation source is the replay runner's 1m bar set loaded from the scope bundle's source DB.",
                "Bar boundaries are absolute UTC 7-minute buckets produced by the shared maintenance helper.",
                "Only completed bars are eligible to trigger maintenance exits.",
                "Session handling matches the original replay: trades are bounded to their current session segment.",
            ],
            "result_grade": (
                "decision-grade"
                if reference_status["control_reproduced_cleanly"]["answer"] == "yes"
                else "exploratory"
            ),
        },
        "basis_reconciliation": reference_status,
        "baseline_metrics": baseline_metrics,
        "baseline_probe_summary": {
            "trade_source": "bundle_trade_records_with_confirmation_add_overlay",
            "bars_loaded": len(bars_1m),
            "confirmation_add_trade_count": baseline_sizing_summary["confirmation_add_trade_count"],
            "confirmation_add_net_pnl": baseline_sizing_summary["confirmation_add_net_pnl"],
        },
        "tested_variants": [
            {
                "variant_id": row["variant_id"],
                "label": row["label"],
                "exit_policy": row["exit_policy"],
            }
            for row in results
        ],
        "results": results,
        "best_7m_variant": {
            "variant_id": best_7m["variant_id"],
            "label": best_7m["label"],
            "metrics": best_7m["metrics"],
            "comparison_vs_15m_control": best_7m["comparison_vs_15m_control"],
            "comparison_vs_baseline": best_7m["comparison_vs_baseline"],
        },
        "judgment": _judgment(results=results, best_7m=best_7m, reference_status=reference_status),
    }
    json_path = output_dir / "atp_gc_7m_maintenance_study.json"
    markdown_path = output_dir / "atp_gc_7m_maintenance_study.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(render_gc_7m_maintenance_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _variant_specs() -> list[dict[str, Any]]:
    return [
        {
            "variant_id": CONTROL_15M_ID,
            "label": "Reproduced checkpoint100 + 15m 2-of-3 control",
            "exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3,
            "maintenance_timeframe": "15m",
            "notes": "Faithful checkpoint100 15m reference on the current replay substrate.",
            "is_control_15m": True,
            "is_7m": False,
        },
        {
            "variant_id": CONTROL_10M_ID,
            "label": "Reproduced checkpoint075 + 10m double weak close",
            "exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK,
            "maintenance_timeframe": "10m",
            "notes": "Prior twitchy-reference comparison on the same replay substrate.",
            "is_control_15m": False,
            "is_7m": False,
        },
        {
            "variant_id": "checkpoint100_no_traction_abort_7m_2of3",
            "label": "Checkpoint 1.00R + 7m 2-of-3 maintenance",
            "exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_2OF3,
            "maintenance_timeframe": "7m",
            "notes": "Direct 7m analog of the favored 15m 2-of-3 rule.",
            "is_control_15m": False,
            "is_7m": True,
        },
        {
            "variant_id": "checkpoint100_no_traction_abort_7m_strict_weak_close",
            "label": "Checkpoint 1.00R + 7m strict weak close",
            "exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_STRICT_WEAK,
            "maintenance_timeframe": "7m",
            "notes": "7m stricter deterioration requiring a decisive weak close or two consecutive weak closes.",
            "is_control_15m": False,
            "is_7m": True,
        },
        {
            "variant_id": "checkpoint100_no_traction_abort_7m_loose_hold",
            "label": "Checkpoint 1.00R + 7m loose hold",
            "exit_policy": ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_LOOSE_HOLD,
            "maintenance_timeframe": "7m",
            "notes": "7m looser maintenance requiring the full deterioration stack before exit.",
            "is_control_15m": False,
            "is_7m": True,
        },
    ]


def _variant_metrics(trades: Sequence[TradeRecord], *, point_value: float) -> dict[str, Any]:
    from ..research.trend_participation.backtest import summarize_performance

    performance = summarize_performance(trades)
    promoted = [trade for trade in trades if trade.participation_promoted]
    promoted_maintenance_exits = [trade for trade in promoted if str(trade.exit_reason).startswith("htf_")]
    winner_pnls = sorted((float(trade.pnl_cash) for trade in trades if float(trade.pnl_cash) > 0.0), reverse=True)
    top_decile_count = max(int(len(winner_pnls) * 0.10), 1) if winner_pnls else 0
    return {
        "trade_count": performance.trade_count,
        "net_pnl_cash": round(float(performance.net_pnl_cash), 6),
        "max_drawdown": round(float(performance.max_drawdown), 6),
        "profit_factor": round(float(performance.profit_factor), 6),
        "win_rate": round(float(performance.win_rate), 6),
        "expectancy": round(float(performance.expectancy), 6),
        "average_hold_minutes": round(float(performance.avg_hold_minutes), 6),
        "median_hold_minutes": round(_median(float(trade.hold_minutes) for trade in trades), 6),
        "promoted_trade_count": len(promoted),
        "promoted_trade_net_pnl_cash": round(sum(float(trade.pnl_cash) for trade in promoted), 6),
        "promoted_trade_expectancy": round(_mean(float(trade.pnl_cash) for trade in promoted), 6),
        "promoted_trade_average_hold_minutes": round(_mean(float(trade.hold_minutes) for trade in promoted), 6),
        "average_additional_pnl_after_checkpoint_cash": round(
            _mean(
                (float(trade.pnl_points) - float(trade.pnl_points_at_promotion or 0.0)) * point_value
                for trade in promoted
                if trade.pnl_points_at_promotion is not None
            ),
            6,
        ),
        "average_giveback_after_checkpoint_cash": round(
            _mean(
                max(float(trade.post_promotion_peak_open_profit_points or 0.0) - float(trade.pnl_points), 0.0) * point_value
                for trade in promoted
            ),
            6,
        ),
        "promoted_maintenance_exit_count": len(promoted_maintenance_exits),
        "top_winners": {
            "largest_winner_cash": round(winner_pnls[0], 6) if winner_pnls else 0.0,
            "top5_winner_sum_cash": round(sum(winner_pnls[:5]), 6),
            "top10_winner_sum_cash": round(sum(winner_pnls[:10]), 6),
            "top_decile_winner_sum_cash": round(sum(winner_pnls[:top_decile_count]), 6),
            "top_decile_winner_mean_cash": round(_mean(winner_pnls[:top_decile_count]), 6),
        },
        "exit_reason_counts": dict(sorted(Counter(str(trade.exit_reason) for trade in trades).items())),
    }


def _comparison_metrics(*, baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline_top = baseline.get("top_winners") or {}
    candidate_top = candidate.get("top_winners") or {}
    return {
        "net_pnl_cash_delta": round(float(candidate["net_pnl_cash"]) - float(baseline["net_pnl_cash"]), 6),
        "max_drawdown_delta": round(float(candidate["max_drawdown"]) - float(baseline["max_drawdown"]), 6),
        "profit_factor_delta": round(float(candidate["profit_factor"]) - float(baseline["profit_factor"]), 6),
        "win_rate_delta": round(float(candidate["win_rate"]) - float(baseline["win_rate"]), 6),
        "expectancy_delta": round(float(candidate["expectancy"]) - float(baseline["expectancy"]), 6),
        "average_hold_minutes_delta": round(
            float(candidate["average_hold_minutes"]) - float(baseline["average_hold_minutes"]),
            6,
        ),
        "promoted_trade_net_pnl_delta": round(
            float(candidate["promoted_trade_net_pnl_cash"]) - float(baseline["promoted_trade_net_pnl_cash"]),
            6,
        ),
        "average_additional_pnl_after_checkpoint_delta": round(
            float(candidate["average_additional_pnl_after_checkpoint_cash"])
            - float(baseline["average_additional_pnl_after_checkpoint_cash"]),
            6,
        ),
        "average_giveback_after_checkpoint_delta": round(
            float(candidate["average_giveback_after_checkpoint_cash"])
            - float(baseline["average_giveback_after_checkpoint_cash"]),
            6,
        ),
        "promoted_maintenance_exit_count_delta": int(candidate["promoted_maintenance_exit_count"]) - int(
            baseline["promoted_maintenance_exit_count"]
        ),
        "top5_winner_sum_cash_delta": round(
            float(candidate_top.get("top5_winner_sum_cash", 0.0)) - float(baseline_top.get("top5_winner_sum_cash", 0.0)),
            6,
        ),
        "top10_winner_sum_cash_delta": round(
            float(candidate_top.get("top10_winner_sum_cash", 0.0)) - float(baseline_top.get("top10_winner_sum_cash", 0.0)),
            6,
        ),
        "top_decile_winner_sum_cash_delta": round(
            float(candidate_top.get("top_decile_winner_sum_cash", 0.0))
            - float(baseline_top.get("top_decile_winner_sum_cash", 0.0)),
            6,
        ),
    }


def _control_reconciliation_status(
    *,
    scope_bundle_manifest: Path,
    reference_exit_evolution_json: Path,
    control_15m: dict[str, Any],
    control_10m: dict[str, Any] | None,
) -> dict[str, Any]:
    status = {
        "trade_population_same_as_prior_checkpoint100_study": {
            "answer": "yes",
            "reason": "The 7m study reuses the same scope bundle trade records and replay interval construction as the original GC exit-evolution pass.",
        },
        "promoted_bucket_definition_same_as_prior_checkpoint100_study": {
            "answer": "yes",
            "reason": "Promotion still comes from the same checkpoint-driven exit-policy replay on the same trade records.",
        },
        "add_overlay_applied_at_same_stage": {
            "answer": "yes",
            "reason": "Confirmation/add sizing is still applied after exit-policy replay using the same confirmation sizing profile.",
        },
        "maintenance_bars_derived_on_same_completed_bar_substrate": {
            "answer": "yes",
            "reason": "Maintenance bars are still built from the same replay 1m bars using the shared completed-bar maintenance helper.",
        },
        "reference_exit_evolution_json": str(reference_exit_evolution_json.resolve()),
        "control_reproduced_cleanly": {
            "answer": "no",
            "reason": "Reference artifact not found.",
        },
    }
    if not reference_exit_evolution_json.exists():
        status["reproduction_grade"] = "exploratory"
        return status

    reference = json.loads(reference_exit_evolution_json.read_text(encoding="utf-8"))
    reference_results = {row["variant_id"]: row for row in reference.get("results", [])}
    reference_15m = reference_results.get("checkpoint100_no_traction_abort_15m_2of3")
    reference_10m = reference_results.get("checkpoint075_no_traction_abort_10m_double_weak_close")

    deltas_15m = _reproduction_delta(reference_15m, control_15m)
    deltas_10m = _reproduction_delta(reference_10m, control_10m) if reference_10m and control_10m else None
    clean = _is_reproduced_cleanly(deltas_15m) and (deltas_10m is None or _is_reproduced_cleanly(deltas_10m))
    status["control_reproduced_cleanly"] = {
        "answer": "yes" if clean else "no",
        "reason": (
            "The reproduced controls match the earlier exit-evolution artifact on the current replay substrate."
            if clean
            else "At least one reproduced control still differs materially from the earlier exit-evolution artifact."
        ),
    }
    status["control_reconciliation"] = {
        "checkpoint100_15m": {
            "reference_variant_id": "checkpoint100_no_traction_abort_15m_2of3",
            "study_variant_id": CONTROL_15M_ID,
            "metric_deltas": deltas_15m,
        },
        "checkpoint075_10m": (
            {
                "reference_variant_id": "checkpoint075_no_traction_abort_10m_double_weak_close",
                "study_variant_id": CONTROL_10M_ID,
                "metric_deltas": deltas_10m,
            }
            if deltas_10m is not None
            else None
        ),
    }
    status["reproduction_grade"] = "decision-grade" if clean else "exploratory"
    return status


def _reproduction_delta(reference_row: dict[str, Any] | None, study_row: dict[str, Any] | None) -> dict[str, float] | None:
    if reference_row is None or study_row is None:
        return None
    reference_metrics = reference_row["metrics"]
    study_metrics = study_row["metrics"]
    return {
        "net_pnl_cash": round(float(study_metrics["net_pnl_cash"]) - float(reference_metrics["net_pnl_cash"]), 6),
        "max_drawdown": round(float(study_metrics["max_drawdown"]) - float(reference_metrics["max_drawdown"]), 6),
        "profit_factor": round(float(study_metrics["profit_factor"]) - float(reference_metrics["profit_factor"]), 6),
        "win_rate": round(float(study_metrics["win_rate"]) - float(reference_metrics["win_rate"]), 6),
        "expectancy": round(float(study_metrics["expectancy"]) - float(reference_metrics["expectancy"]), 6),
        "average_hold_minutes": round(
            float(study_metrics["average_hold_minutes"]) - float(reference_metrics["average_hold_minutes"]),
            6,
        ),
        "median_hold_minutes": round(
            float(study_metrics["median_hold_minutes"]) - float(reference_metrics["median_hold_minutes"]),
            6,
        ),
        "trade_count": int(study_metrics["trade_count"]) - int(reference_metrics["trade_count"]),
    }


def _is_reproduced_cleanly(deltas: dict[str, float] | None) -> bool:
    if deltas is None:
        return False
    tolerances = {
        "net_pnl_cash": 1e-6,
        "max_drawdown": 1e-6,
        "profit_factor": 1e-6,
        "win_rate": 1e-6,
        "expectancy": 1e-6,
        "average_hold_minutes": 1e-6,
        "median_hold_minutes": 1e-6,
        "trade_count": 0,
    }
    return all(abs(float(deltas[key])) <= tolerance for key, tolerance in tolerances.items())


def _best_7m_variant(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    candidates = [row for row in results if row["is_7m"]]
    return max(
        candidates,
        key=lambda row: (
            float(row["metrics"]["net_pnl_cash"]),
            float(row["metrics"]["profit_factor"]),
            -float(row["metrics"]["max_drawdown"]),
        ),
    )


def _judgment(
    *,
    results: Sequence[dict[str, Any]],
    best_7m: dict[str, Any],
    reference_status: dict[str, Any],
) -> dict[str, Any]:
    by_id = {row["variant_id"]: row for row in results}
    control_15m = by_id[CONTROL_15M_ID]
    control_10m = by_id.get(CONTROL_10M_ID)
    best_vs_15m = best_7m["comparison_vs_15m_control"]
    best_vs_10m = _comparison_metrics(
        baseline=control_10m["metrics"],
        candidate=best_7m["metrics"],
    ) if control_10m is not None else None

    seven_better_than_ten = (
        best_vs_10m is not None
        and float(best_vs_10m["net_pnl_cash_delta"]) > 0.0
        and float(best_vs_10m["profit_factor_delta"]) >= 0.0
    )
    seven_better_than_fifteen = (
        float(best_vs_15m["net_pnl_cash_delta"]) > 0.0
        and float(best_vs_15m["profit_factor_delta"]) >= 0.0
    )
    seven_real_edge = seven_better_than_fifteen and float(best_vs_15m["top10_winner_sum_cash_delta"]) > 0.0
    best_id = best_7m["variant_id"]
    if best_id.endswith("strict_weak_close"):
        behavior = "more like 10m"
    elif best_id.endswith("loose_hold"):
        behavior = "between 15m and 20m, with extra slack"
    else:
        behavior = "closest to 15m, but faster"
    return {
        "was_control_reproduced_cleanly": reference_status["control_reproduced_cleanly"],
        "best_7m_variant_id": best_7m["variant_id"],
        "was_7m_better_than_10m": {
            "answer": "yes" if seven_better_than_ten else "no",
            "comparison": best_vs_10m,
        },
        "was_7m_better_than_15m": {
            "answer": "yes" if seven_better_than_fifteen else "no",
            "comparison": best_vs_15m,
        },
        "does_7m_look_like_real_edge_or_curiosity": {
            "answer": "real edge" if seven_real_edge else "curiosity",
            "reason": (
                "The best 7m variant beats the 15m control on both economics and winner capture."
                if seven_real_edge
                else "The best 7m variant does not clearly beat the 15m control on strategy-level economics."
            ),
        },
        "frame_behavior_interpretation": {
            "best_variant_id": best_7m["variant_id"],
            "behavior": behavior,
        },
    }


def render_gc_7m_maintenance_markdown(payload: dict[str, Any]) -> str:
    basis = payload["basis_reconciliation"]
    best = payload["best_7m_variant"]
    judgment = payload["judgment"]
    lines = [
        "# ATP GC 7m Maintenance Study",
        "",
        f"1. Was the control reproduced cleanly or not? `{judgment['was_control_reproduced_cleanly']['answer']}`",
        f"2. Which 7m variant performed best? `{best['variant_id']}`",
        f"3. Was 7m better than 10m? `{judgment['was_7m_better_than_10m']['answer']}`",
        f"4. Was 7m better than 15m? `{judgment['was_7m_better_than_15m']['answer']}`",
        f"5. Does 7m look like a real edge or just a curiosity? `{judgment['does_7m_look_like_real_edge_or_curiosity']['answer']}`",
        "",
        "## Basis Reconciliation",
        "",
        f"- Control reproduction grade: `{basis['reproduction_grade']}`",
        f"- Trade population same: `{basis['trade_population_same_as_prior_checkpoint100_study']['answer']}`",
        f"- Promoted bucket same: `{basis['promoted_bucket_definition_same_as_prior_checkpoint100_study']['answer']}`",
        f"- Add overlay same stage: `{basis['add_overlay_applied_at_same_stage']['answer']}`",
        f"- Completed-bar substrate same: `{basis['maintenance_bars_derived_on_same_completed_bar_substrate']['answer']}`",
        "",
        "## Comparison Table",
        "",
        "| Variant | Net P&L | Max DD | PF | Win rate | Expectancy | Avg hold | Median hold | Promoted count | Promoted net | Avg addl after promo | Avg giveback after promo | HTF exits |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["results"]:
        metrics = row["metrics"]
        lines.append(
            "| "
            + " | ".join(
                [
                    row["variant_id"],
                    f"{metrics['net_pnl_cash']:.2f}",
                    f"{metrics['max_drawdown']:.2f}",
                    f"{metrics['profit_factor']:.3f}",
                    f"{metrics['win_rate']:.3f}",
                    f"{metrics['expectancy']:.2f}",
                    f"{metrics['average_hold_minutes']:.2f}",
                    f"{metrics['median_hold_minutes']:.2f}",
                    str(metrics["promoted_trade_count"]),
                    f"{metrics['promoted_trade_net_pnl_cash']:.2f}",
                    f"{metrics['average_additional_pnl_after_checkpoint_cash']:.2f}",
                    f"{metrics['average_giveback_after_checkpoint_cash']:.2f}",
                    str(metrics["promoted_maintenance_exit_count"]),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def _mean(values: Sequence[float] | Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(sum(items) / len(items))


def _median(values: Sequence[float] | Any) -> float:
    items = list(values)
    if not items:
        return 0.0
    return float(median(items))
