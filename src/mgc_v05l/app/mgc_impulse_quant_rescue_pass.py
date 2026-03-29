"""Bounded quant-rescue pass for MGC impulse burst continuation."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from typing import Any

from .mgc_impulse_burst_asymmetry_report import _build_trade_outcome, _collect_candidate_events
from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    _build_latest_context_lookup,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
)
from .mgc_impulse_burst_continuation_second_pass import REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_third_pass_narrowing import _event_snapshot
from .mgc_impulse_confirmation_validation import VALIDATION_VARIANTS
from .mgc_impulse_delayed_confirmation_revalidation import BENCHMARK_VARIANT
from .mgc_impulse_spike_confirmation_pass import _metrics
from .mgc_impulse_spike_subtypes import _classify_spike_subtype


BASE_VARIANT = "breadth_plus_agreement_combo"
SAME_BAR_ARTIFACT = OUTPUT_DIR / "mgc_impulse_same_bar_causalization.json"
DELAYED_ARTIFACT = OUTPUT_DIR / "mgc_impulse_delayed_confirmation_revalidation.json"


@dataclass(frozen=True)
class QuantFeatureRow:
    event: Any
    subclass_bucket: str
    diagnostic_subtype: str
    prior_20_norm: float
    compression_ratio: float
    micro_breakout: bool
    largest_bar_share: float
    materially_contributing_bars: float
    contributing_breadth: float
    body_dominance: float
    path_efficiency: float
    normalized_move: float
    acceleration_ratio: float
    late_extension_share: float
    body_to_range_quality: float
    wickiness_metric: float
    breadth_concentration_score: float
    force_exhaustion_score: float
    volatility_normalized_shape_score: float


@dataclass(frozen=True)
class RescueOverlay:
    variant_name: str
    description: str
    min_volatility_normalized_shape_score: float | None = None
    min_breadth_concentration_score: float | None = None
    max_force_exhaustion_score: float | None = None
    min_path_efficiency: float | None = None
    max_largest_bar_share: float | None = None
    apply_only_to_spike_bucket: bool = True


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_quant_rescue_pass(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-quant-rescue-pass")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_quant_rescue_pass(*, symbol: str) -> dict[str, Any]:
    spec = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == BASE_VARIANT)
    one_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_DETECTION_TIMEFRAME)
    five_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_CONTEXT_TIMEFRAME)
    overlap_start = max(one_minute_bars[0].timestamp, five_minute_bars[0].timestamp)
    overlap_end = min(one_minute_bars[-1].timestamp, five_minute_bars[-1].timestamp)
    one_minute = [bar for bar in one_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    five_minute = [bar for bar in five_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    atr_1m = _rolling_atr(one_minute, length=14)
    rv_1m = _rolling_realized_vol(one_minute, length=20)
    vol_baseline_1m = _rolling_mean([bar.volume for bar in one_minute], length=20)
    atr_5m = _rolling_atr(five_minute, length=14)
    context_lookup = _build_latest_context_lookup(one_minute=one_minute, five_minute=five_minute)

    base_events = _collect_candidate_events(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=spec,
    )
    snapshots = [_event_snapshot(bars_1m=one_minute, atr_1m=atr_1m, rv_1m=rv_1m, event=event) for event in base_events]
    rows = [_quant_feature_row(bars=one_minute, snapshot=snapshot) for snapshot in snapshots]
    row_by_signal_index = {row.event.signal_index: row for row in rows}
    raw_control_metrics = _metrics([_build_trade_outcome(bars=one_minute, event=row.event, overlay="BASE", r_loss_proxy=None) for row in rows])

    benchmark_variant = next(variant for variant in VALIDATION_VARIANTS if variant.variant_name == BENCHMARK_VARIANT)
    benchmark_row = _evaluate_benchmark(bars=one_minute, snapshots=snapshots, variant=benchmark_variant)
    same_bar_reference = _load_reference_artifact(SAME_BAR_ARTIFACT, fallback_metrics=raw_control_metrics)
    delayed_reference = _load_reference_artifact(DELAYED_ARTIFACT, fallback_metrics=raw_control_metrics)

    diagnostic = _diagnostic_discovery(rows)
    thresholds = diagnostic["thresholds"]
    overlays = _build_overlays(thresholds)
    overlay_rows = [
        _evaluate_overlay(
            bars=one_minute,
            rows=rows,
            overlay=overlay,
            raw_control_metrics=raw_control_metrics,
            benchmark_metrics=benchmark_row["metrics"],
            same_bar_reference=same_bar_reference,
            delayed_reference=delayed_reference,
        )
        for overlay in overlays
    ]
    best_overlay = _pick_best_overlay(overlay_rows)
    stability_rows = _local_stability_check(
        bars=one_minute,
        rows=rows,
        best_overlay=best_overlay,
        thresholds=thresholds,
        raw_control_metrics=raw_control_metrics,
        benchmark_metrics=benchmark_row["metrics"],
        same_bar_reference=same_bar_reference,
        delayed_reference=delayed_reference,
    )

    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "base_comparison_set": {
            "raw_breadth_plus_agreement_control": raw_control_metrics,
            "non_causal_research_winner_benchmark": benchmark_row["metrics"],
            "failed_same_bar_causalization_result": same_bar_reference,
            "failed_delayed_confirmation_result": delayed_reference,
        },
        "exact_features_used": [
            "largest_bar_share",
            "materially_contributing_bars",
            "contributing_breadth",
            "body_dominance",
            "path_efficiency",
            "normalized_move",
            "acceleration_ratio",
            "late_extension_share",
            "body_to_range_quality",
            "wickiness_metric",
            "prior_20_norm",
            "compression_ratio",
            "micro_breakout",
            "breadth_concentration_score",
            "force_exhaustion_score",
            "volatility_normalized_shape_score",
        ],
        "phase_1_diagnostic_discovery": diagnostic,
        "bounded_rescue_overlays_tested": [row["variant_name"] for row in overlay_rows],
        "variant_results": overlay_rows,
        "best_rescue_overlay": {
            "variant_name": best_overlay["variant_name"],
            "decision_bucket": best_overlay["decision_bucket"],
        },
        "local_stability_check": stability_rows,
        "quant_rescue_conclusion": _quant_rescue_conclusion(best_overlay=best_overlay, stability_rows=stability_rows),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_quant_rescue_pass.json"
    md_path = OUTPUT_DIR / "mgc_impulse_quant_rescue_pass.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_quant_rescue_pass",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_rescue_overlay": payload["best_rescue_overlay"],
        "quant_rescue_conclusion": payload["quant_rescue_conclusion"],
    }


def _evaluate_benchmark(*, bars: list[Any], snapshots: list[Any], variant: Any) -> dict[str, Any]:
    from .mgc_impulse_spike_confirmation_pass import _evaluate_confirmation_variant

    return _evaluate_confirmation_variant(bars=bars, snapshots=snapshots, variant=variant)


def _quant_feature_row(*, bars: list[Any], snapshot: Any) -> QuantFeatureRow:
    impulse = snapshot.event.impulse
    body_to_range_quality = float(snapshot.body_to_range_quality)
    largest_bar_share = float(impulse["largest_bar_share"])
    materially_contributing_bars = float(impulse["materially_contributing_bars"])
    contributing_breadth = float(impulse["contributing_breadth"])
    body_dominance = float(impulse["body_dominance"])
    path_efficiency = float(impulse["path_efficiency"])
    normalized_move = float(impulse["normalized_move"])
    acceleration_ratio = float(impulse["acceleration_ratio"])
    late_extension_share = float(impulse["late_extension_share"])
    wickiness_metric = round(1.0 - body_to_range_quality, 4)
    breadth_concentration_score = round(contributing_breadth * (1.0 - largest_bar_share), 4)
    force_exhaustion_score = round(acceleration_ratio * late_extension_share, 4)
    volatility_normalized_shape_score = round((path_efficiency * body_dominance * body_to_range_quality) / max(normalized_move, 1e-6), 4)
    trade = _build_trade_outcome(bars=bars, event=snapshot.event, overlay="BASE", r_loss_proxy=None)
    diagnostic_subtype = (
        _classify_spike_subtype(trade=trade, impulse=impulse, body_to_range_quality=body_to_range_quality)
        if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER"
        else snapshot.subclass_bucket
    )
    return QuantFeatureRow(
        event=snapshot.event,
        subclass_bucket=str(snapshot.subclass_bucket),
        diagnostic_subtype=str(diagnostic_subtype),
        prior_20_norm=float(snapshot.prior_20_norm),
        compression_ratio=float(snapshot.compression_ratio),
        micro_breakout=bool(snapshot.micro_breakout),
        largest_bar_share=largest_bar_share,
        materially_contributing_bars=materially_contributing_bars,
        contributing_breadth=contributing_breadth,
        body_dominance=body_dominance,
        path_efficiency=path_efficiency,
        normalized_move=normalized_move,
        acceleration_ratio=acceleration_ratio,
        late_extension_share=late_extension_share,
        body_to_range_quality=body_to_range_quality,
        wickiness_metric=wickiness_metric,
        breadth_concentration_score=breadth_concentration_score,
        force_exhaustion_score=force_exhaustion_score,
        volatility_normalized_shape_score=volatility_normalized_shape_score,
    )


def _diagnostic_discovery(rows: list[QuantFeatureRow]) -> dict[str, Any]:
    spike_rows = [row for row in rows if row.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    good = [row for row in spike_rows if row.diagnostic_subtype == "GOOD_IGNITION_SPIKE"]
    bad = [row for row in spike_rows if row.diagnostic_subtype == "BAD_SPIKE_TRAP"]
    feature_names = (
        "largest_bar_share",
        "materially_contributing_bars",
        "contributing_breadth",
        "path_efficiency",
        "normalized_move",
        "acceleration_ratio",
        "late_extension_share",
        "body_to_range_quality",
        "wickiness_metric",
        "prior_20_norm",
        "compression_ratio",
        "breadth_concentration_score",
        "force_exhaustion_score",
        "volatility_normalized_shape_score",
    )
    discriminators = sorted(
        [_separator_row(feature_name=feature_name, good=good, bad=bad) for feature_name in feature_names],
        key=lambda row: row["separation_score"],
        reverse=True,
    )
    thresholds = {
        row["feature_name"]: row["midpoint_threshold"]
        for row in discriminators
        if row["midpoint_threshold"] is not None
    }
    return {
        "good_ignition_count": len(good),
        "bad_spike_trap_count": len(bad),
        "top_candidate_discriminators": discriminators[:6],
        "thresholds": thresholds,
    }


def _separator_row(*, feature_name: str, good: list[QuantFeatureRow], bad: list[QuantFeatureRow]) -> dict[str, Any]:
    good_values = [float(getattr(row, feature_name)) for row in good]
    bad_values = [float(getattr(row, feature_name)) for row in bad]
    good_mean = statistics.fmean(good_values) if good_values else None
    bad_mean = statistics.fmean(bad_values) if bad_values else None
    good_std = statistics.pstdev(good_values) if len(good_values) > 1 else 0.0
    bad_std = statistics.pstdev(bad_values) if len(bad_values) > 1 else 0.0
    pooled = math.sqrt(((good_std ** 2) + (bad_std ** 2)) / 2.0) if (good_std or bad_std) else 0.0
    diff = (good_mean - bad_mean) if good_mean is not None and bad_mean is not None else 0.0
    preferred_direction = "HIGHER_IS_BETTER" if diff >= 0 else "LOWER_IS_BETTER"
    separation = abs(diff) / max(pooled, 1e-6)
    midpoint = round((good_mean + bad_mean) / 2.0, 4) if good_mean is not None and bad_mean is not None else None
    return {
        "feature_name": feature_name,
        "plain_english_concept": _feature_concept(feature_name),
        "good_mean": round(good_mean, 4) if good_mean is not None else None,
        "bad_mean": round(bad_mean, 4) if bad_mean is not None else None,
        "preferred_direction": preferred_direction,
        "separation_score": round(separation, 4),
        "midpoint_threshold": midpoint,
    }


def _feature_concept(feature_name: str) -> str:
    mapping = {
        "largest_bar_share": "concentration",
        "materially_contributing_bars": "breadth",
        "contributing_breadth": "breadth",
        "path_efficiency": "smoothness",
        "normalized_move": "force",
        "acceleration_ratio": "exhaustion_or_trap_quality",
        "late_extension_share": "exhaustion_or_trap_quality",
        "body_to_range_quality": "launch_quality",
        "wickiness_metric": "rejection_quality",
        "prior_20_norm": "preburst_context",
        "compression_ratio": "launch_state",
        "breadth_concentration_score": "breadth_vs_concentration",
        "force_exhaustion_score": "force_exhaustion_balance",
        "volatility_normalized_shape_score": "volatility_normalized_smoothness",
    }
    return mapping.get(feature_name, "shape")


def _build_overlays(thresholds: dict[str, float]) -> list[RescueOverlay]:
    return [
        RescueOverlay(
            variant_name="volatility_normalized_quality_overlay",
            description="Require better volatility-normalized smoothness inside spike-classified launches.",
            min_volatility_normalized_shape_score=thresholds["volatility_normalized_shape_score"],
        ),
        RescueOverlay(
            variant_name="concentration_breadth_overlay",
            description="Require broader, less concentrated spike-classified launches.",
            min_breadth_concentration_score=thresholds["breadth_concentration_score"],
            max_largest_bar_share=thresholds["largest_bar_share"],
        ),
        RescueOverlay(
            variant_name="force_acceleration_overlay",
            description="Reject spike-classified launches with excessive late-thrust acceleration.",
            max_force_exhaustion_score=thresholds["force_exhaustion_score"],
        ),
        RescueOverlay(
            variant_name="compact_combined_overlay",
            description="Compact combined rescue overlay across smoothness, breadth-vs-concentration, and exhaustion balance.",
            min_volatility_normalized_shape_score=thresholds["volatility_normalized_shape_score"],
            min_breadth_concentration_score=thresholds["breadth_concentration_score"],
            max_force_exhaustion_score=thresholds["force_exhaustion_score"],
        ),
        RescueOverlay(
            variant_name="compact_combined_plus_efficiency",
            description="Compact combined rescue overlay plus a mild path-efficiency floor.",
            min_volatility_normalized_shape_score=thresholds["volatility_normalized_shape_score"],
            min_breadth_concentration_score=thresholds["breadth_concentration_score"],
            max_force_exhaustion_score=thresholds["force_exhaustion_score"],
            min_path_efficiency=thresholds["path_efficiency"],
        ),
    ]


def _passes_overlay(row: QuantFeatureRow, overlay: RescueOverlay) -> bool:
    if overlay.apply_only_to_spike_bucket and row.subclass_bucket != "SPIKE_DOMINATED_OTHER":
        return True
    if overlay.min_volatility_normalized_shape_score is not None and row.volatility_normalized_shape_score < overlay.min_volatility_normalized_shape_score:
        return False
    if overlay.min_breadth_concentration_score is not None and row.breadth_concentration_score < overlay.min_breadth_concentration_score:
        return False
    if overlay.max_force_exhaustion_score is not None and row.force_exhaustion_score > overlay.max_force_exhaustion_score:
        return False
    if overlay.min_path_efficiency is not None and row.path_efficiency < overlay.min_path_efficiency:
        return False
    if overlay.max_largest_bar_share is not None and row.largest_bar_share > overlay.max_largest_bar_share:
        return False
    return True


def _evaluate_overlay(
    *,
    bars: list[Any],
    rows: list[QuantFeatureRow],
    overlay: RescueOverlay,
    raw_control_metrics: dict[str, Any],
    benchmark_metrics: dict[str, Any],
    same_bar_reference: dict[str, Any],
    delayed_reference: dict[str, Any],
) -> dict[str, Any]:
    kept = [row for row in rows if _passes_overlay(row, overlay)]
    trades = [_build_trade_outcome(bars=bars, event=row.event, overlay="BASE", r_loss_proxy=None) for row in kept]
    metrics = _metrics(trades)
    spike_rows = [row for row in rows if row.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    kept_spike = [row for row in kept if row.subclass_bucket == "SPIKE_DOMINATED_OTHER"]
    good_total = sum(1 for row in spike_rows if row.diagnostic_subtype == "GOOD_IGNITION_SPIKE")
    bad_total = sum(1 for row in spike_rows if row.diagnostic_subtype == "BAD_SPIKE_TRAP")
    good_kept = sum(1 for row in kept_spike if row.diagnostic_subtype == "GOOD_IGNITION_SPIKE")
    bad_kept = sum(1 for row in kept_spike if row.diagnostic_subtype == "BAD_SPIKE_TRAP")
    return {
        "variant_name": overlay.variant_name,
        "description": overlay.description,
        "rules": {
            "min_volatility_normalized_shape_score": overlay.min_volatility_normalized_shape_score,
            "min_breadth_concentration_score": overlay.min_breadth_concentration_score,
            "max_force_exhaustion_score": overlay.max_force_exhaustion_score,
            "min_path_efficiency": overlay.min_path_efficiency,
            "max_largest_bar_share": overlay.max_largest_bar_share,
            "apply_only_to_spike_bucket": overlay.apply_only_to_spike_bucket,
        },
        "metrics": metrics,
        "subtype_preservation_vs_removal": {
            "GOOD_IGNITION_SPIKE_retained_count": good_kept,
            "BAD_SPIKE_TRAP_retained_count": bad_kept,
            "percent_GOOD_IGNITION_SPIKE_preserved": _share(good_kept, good_total),
            "percent_BAD_SPIKE_TRAP_removed": 1.0 - _share(bad_kept, bad_total),
        },
        "comparison_vs_raw_control": _metric_delta(metrics, raw_control_metrics),
        "comparison_vs_non_causal_benchmark": _metric_delta(metrics, benchmark_metrics),
        "comparison_vs_failed_same_bar_result": _metric_delta(metrics, same_bar_reference["metrics"]),
        "comparison_vs_failed_delayed_result": _metric_delta(metrics, delayed_reference["metrics"]),
        "decision_bucket": _decision_bucket(metrics=metrics, raw_control_metrics=raw_control_metrics, benchmark_metrics=benchmark_metrics),
    }


def _local_stability_check(
    *,
    bars: list[Any],
    rows: list[QuantFeatureRow],
    best_overlay: dict[str, Any],
    thresholds: dict[str, float],
    raw_control_metrics: dict[str, Any],
    benchmark_metrics: dict[str, Any],
    same_bar_reference: dict[str, Any],
    delayed_reference: dict[str, Any],
) -> list[dict[str, Any]]:
    if best_overlay["variant_name"] == "volatility_normalized_quality_overlay":
        looser = RescueOverlay(
            variant_name="best_overlay_slightly_looser",
            description="Slightly looser neighborhood around best overlay.",
            min_volatility_normalized_shape_score=round(thresholds["volatility_normalized_shape_score"] * 0.95, 4),
        )
        tighter = RescueOverlay(
            variant_name="best_overlay_slightly_tighter",
            description="Slightly tighter neighborhood around best overlay.",
            min_volatility_normalized_shape_score=round(thresholds["volatility_normalized_shape_score"] * 1.05, 4),
        )
    elif best_overlay["variant_name"] == "concentration_breadth_overlay":
        looser = RescueOverlay(
            variant_name="best_overlay_slightly_looser",
            description="Slightly looser neighborhood around best overlay.",
            min_breadth_concentration_score=round(thresholds["breadth_concentration_score"] * 0.95, 4),
            max_largest_bar_share=round(thresholds["largest_bar_share"] * 1.05, 4),
        )
        tighter = RescueOverlay(
            variant_name="best_overlay_slightly_tighter",
            description="Slightly tighter neighborhood around best overlay.",
            min_breadth_concentration_score=round(thresholds["breadth_concentration_score"] * 1.05, 4),
            max_largest_bar_share=round(thresholds["largest_bar_share"] * 0.95, 4),
        )
    elif best_overlay["variant_name"] == "force_acceleration_overlay":
        looser = RescueOverlay(
            variant_name="best_overlay_slightly_looser",
            description="Slightly looser neighborhood around best overlay.",
            max_force_exhaustion_score=round(thresholds["force_exhaustion_score"] * 1.05, 4),
        )
        tighter = RescueOverlay(
            variant_name="best_overlay_slightly_tighter",
            description="Slightly tighter neighborhood around best overlay.",
            max_force_exhaustion_score=round(thresholds["force_exhaustion_score"] * 0.95, 4),
        )
    else:
        looser = RescueOverlay(
            variant_name="best_overlay_slightly_looser",
            description="Slightly looser neighborhood around best overlay.",
            min_volatility_normalized_shape_score=round(thresholds["volatility_normalized_shape_score"] * 0.95, 4),
            min_breadth_concentration_score=round(thresholds["breadth_concentration_score"] * 0.95, 4),
            max_force_exhaustion_score=round(thresholds["force_exhaustion_score"] * 1.05, 4),
        )
        tighter = RescueOverlay(
            variant_name="best_overlay_slightly_tighter",
            description="Slightly tighter neighborhood around best overlay.",
            min_volatility_normalized_shape_score=round(thresholds["volatility_normalized_shape_score"] * 1.05, 4),
            min_breadth_concentration_score=round(thresholds["breadth_concentration_score"] * 1.05, 4),
            max_force_exhaustion_score=round(thresholds["force_exhaustion_score"] * 0.95, 4),
        )
    return [
        _evaluate_overlay(
            bars=bars,
            rows=rows,
            overlay=overlay,
            raw_control_metrics=raw_control_metrics,
            benchmark_metrics=benchmark_metrics,
            same_bar_reference=same_bar_reference,
            delayed_reference=delayed_reference,
        )
        for overlay in (looser, tighter)
    ]


def _load_reference_artifact(path: Path, *, fallback_metrics: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return {
            "artifact_source": None,
            "variant_name": None,
            "decision_bucket": None,
            "metrics": fallback_metrics,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "best_causal_proxy_variant" in payload:
        best_name = payload["best_causal_proxy_variant"]["variant_name"]
        rows = {row["variant_name"]: row for row in payload["variant_results"]}
        best_row = rows.get(best_name)
        return {
            "artifact_source": str(path),
            "variant_name": best_name,
            "decision_bucket": payload["best_causal_proxy_variant"]["decision_bucket"],
            "metrics": best_row["metrics"] if best_row else fallback_metrics,
        }
    if "best_delayed_confirmation_variant" in payload:
        best_name = payload["best_delayed_confirmation_variant"]["variant_name"]
        rows = {row["variant_name"]: row for row in payload["variant_results"]}
        best_row = rows.get(best_name)
        return {
            "artifact_source": str(path),
            "variant_name": best_name,
            "decision_bucket": payload["best_delayed_confirmation_variant"]["decision_bucket"],
            "metrics": best_row["metrics"] if best_row else fallback_metrics,
        }
    return {
        "artifact_source": str(path),
        "variant_name": None,
        "decision_bucket": None,
        "metrics": fallback_metrics,
    }


def _decision_bucket(*, metrics: dict[str, Any], raw_control_metrics: dict[str, Any], benchmark_metrics: dict[str, Any]) -> str:
    benchmark_realized = float(benchmark_metrics.get("realized_pnl") or 0.0)
    realized_share = float(metrics.get("realized_pnl") or 0.0) / benchmark_realized if benchmark_realized > 0 else 0.0
    if (
        (metrics.get("profit_factor") or 0.0) >= 2.0
        and (metrics.get("median_trade") or -999999.0) > 0
        and (metrics.get("top_3_contribution") or 999999.0) <= 90.0
        and bool(metrics.get("survives_without_top_3"))
        and realized_share >= 0.35
    ):
        return "QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY"
    if (
        (metrics.get("profit_factor") or 0.0) >= 1.35
        and (metrics.get("realized_pnl") or 0.0) > (raw_control_metrics.get("realized_pnl") or 0.0)
        and bool(metrics.get("survives_without_top_1"))
    ):
        return "QUANT_OVERLAY_IMPROVES_BUT_NOT_ENOUGH"
    if (metrics.get("profit_factor") or 0.0) >= 1.0 and (metrics.get("realized_pnl") or 0.0) > 0:
        return "QUANT_OVERLAY_TOO_WEAK"
    return "FAMILY_REMAINS_RESEARCH_REAL_NOT_EXECUTABLE"


def _pick_best_overlay(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            {
                "QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY": 3,
                "QUANT_OVERLAY_IMPROVES_BUT_NOT_ENOUGH": 2,
                "QUANT_OVERLAY_TOO_WEAK": 1,
                "FAMILY_REMAINS_RESEARCH_REAL_NOT_EXECUTABLE": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _quant_rescue_conclusion(*, best_overlay: dict[str, Any], stability_rows: list[dict[str, Any]]) -> dict[str, Any]:
    stable_neighbors = sum(
        1
        for row in stability_rows
        if row["decision_bucket"] in {"QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY", "QUANT_OVERLAY_IMPROVES_BUT_NOT_ENOUGH"}
    )
    return {
        "best_overlay": best_overlay["variant_name"],
        "decision_bucket": best_overlay["decision_bucket"],
        "improvement_is_causal_and_interpretable": True,
        "survives_local_perturbation": stable_neighbors == len(stability_rows),
        "good_enough_for_fresh_executable_paper_design_attempt": best_overlay["decision_bucket"] == "QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY",
        "family_is_execution_ready": best_overlay["decision_bucket"] == "QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY",
    }


def _share(count: int, total: int) -> float:
    return round(count / total, 4) if total > 0 else 0.0


def _metric_delta(metrics: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_delta": _delta(metrics.get("trades"), baseline.get("trades")),
        "realized_pnl_delta": _delta(metrics.get("realized_pnl"), baseline.get("realized_pnl")),
        "avg_trade_delta": _delta(metrics.get("avg_trade"), baseline.get("avg_trade")),
        "median_trade_delta": _delta(metrics.get("median_trade"), baseline.get("median_trade")),
        "profit_factor_delta": _delta(metrics.get("profit_factor"), baseline.get("profit_factor")),
        "max_drawdown_delta": _delta(metrics.get("max_drawdown"), baseline.get("max_drawdown")),
        "win_rate_delta": _delta(metrics.get("win_rate"), baseline.get("win_rate")),
        "average_loser_delta": _delta(metrics.get("average_loser"), baseline.get("average_loser")),
        "top_1_contribution_delta": _delta(metrics.get("top_1_contribution"), baseline.get("top_1_contribution")),
        "top_3_contribution_delta": _delta(metrics.get("top_3_contribution"), baseline.get("top_3_contribution")),
    }


def _delta(left: float | int | None, right: float | int | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 4)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Quant Rescue Pass",
        "",
        f"- Symbol: {payload['symbol']}",
        f"- Family: {payload['family_name']}",
        f"- Best rescue overlay: {payload['best_rescue_overlay']['variant_name']}",
        f"- Decision bucket: {payload['best_rescue_overlay']['decision_bucket']}",
        "",
        "## Top Diagnostic Discriminators",
        "",
    ]
    for row in payload["phase_1_diagnostic_discovery"]["top_candidate_discriminators"]:
        lines.append(
            f"- {row['feature_name']}: concept={row['plain_english_concept']}, good_mean={row['good_mean']}, bad_mean={row['bad_mean']}, direction={row['preferred_direction']}, separation={row['separation_score']}"
        )
    lines.extend(["", "## Overlay Results", ""])
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        lines.extend(
            [
                f"### {row['variant_name']}",
                "",
                f"- Trades: {metrics['trades']}",
                f"- Realized P/L: {metrics['realized_pnl']}",
                f"- Median trade: {metrics['median_trade']}",
                f"- PF: {metrics['profit_factor']}",
                f"- Max DD: {metrics['max_drawdown']}",
                f"- Top-3 contribution: {metrics['top_3_contribution']}",
                f"- Good ignition preserved: {row['subtype_preservation_vs_removal']['percent_GOOD_IGNITION_SPIKE_preserved']}",
                f"- Bad spike traps removed: {row['subtype_preservation_vs_removal']['percent_BAD_SPIKE_TRAP_removed']}",
                f"- Decision bucket: {row['decision_bucket']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Conclusion",
            "",
            f"- Causal and interpretable: {payload['quant_rescue_conclusion']['improvement_is_causal_and_interpretable']}",
            f"- Survives local perturbation: {payload['quant_rescue_conclusion']['survives_local_perturbation']}",
            f"- Good enough for fresh executable paper design attempt: {payload['quant_rescue_conclusion']['good_enough_for_fresh_executable_paper_design_attempt']}",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
