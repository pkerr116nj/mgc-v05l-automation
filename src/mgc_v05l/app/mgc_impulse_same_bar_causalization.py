"""Same-bar causal proxy pass for the MGC impulse continuation family."""

from __future__ import annotations

import argparse
import json
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
from .mgc_impulse_spike_confirmation_pass import _evaluate_confirmation_variant, _metrics


BASE_VARIANT = "breadth_plus_agreement_combo"
BENCHMARK_VARIANT = "base_raw_minimal_confirmation_control"


@dataclass(frozen=True)
class CausalProxyVariant:
    variant_name: str
    description: str
    min_acceleration_ratio: float | None = None
    min_body_to_range_quality: float | None = None
    max_largest_bar_share: float | None = None
    min_body_dominance: float | None = None
    min_path_efficiency: float | None = None
    min_contributing_breadth: float | None = None
    max_prior_20_norm: float | None = None
    max_wickiness_metric: float | None = None
    require_micro_breakout: bool = False
    max_compression_ratio: float | None = None
    min_quality_score: float | None = None


CAUSAL_PROXY_VARIANTS: tuple[CausalProxyVariant, ...] = (
    CausalProxyVariant(
        variant_name="raw_breadth_plus_agreement_control",
        description="Raw breadth_plus_agreement_combo accepted-event control with no extra same-bar causal proxy gate.",
    ),
    CausalProxyVariant(
        variant_name="minimal_same_bar_proxy_rule",
        description="Require simple same-bar force plus clean burst-body quality.",
        min_acceleration_ratio=1.03,
        min_body_to_range_quality=0.56,
    ),
    CausalProxyVariant(
        variant_name="quality_weighted_same_bar_proxy_rule",
        description="Use an interpretable same-bar quality blend across breadth, efficiency, body quality, and concentration.",
        min_quality_score=0.64,
        max_wickiness_metric=0.44,
    ),
    CausalProxyVariant(
        variant_name="best_judgment_compact_causal_combo",
        description="Compact causal combo using same-bar quality plus mild anti-chase context and micro-breakout launch state.",
        min_acceleration_ratio=1.03,
        min_body_to_range_quality=0.56,
        max_largest_bar_share=0.42,
        min_contributing_breadth=0.50,
        max_prior_20_norm=1.05,
        require_micro_breakout=True,
        max_compression_ratio=1.00,
    ),
    CausalProxyVariant(
        variant_name="mild_context_quality_proxy",
        description="Moderate same-bar launch quality with mild prior-run suppression and cleaner burst structure.",
        min_body_dominance=0.78,
        min_path_efficiency=0.55,
        max_largest_bar_share=0.44,
        max_prior_20_norm=1.10,
        min_body_to_range_quality=0.54,
    ),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_same_bar_causalization(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-same-bar-causalization")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_same_bar_causalization(*, symbol: str) -> dict[str, Any]:
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
    raw_snapshots = [_event_snapshot(bars_1m=one_minute, atr_1m=atr_1m, rv_1m=rv_1m, event=event) for event in base_events]

    benchmark_variant = next(variant for variant in VALIDATION_VARIANTS if variant.variant_name == BENCHMARK_VARIANT)
    benchmark_row = _evaluate_confirmation_variant(bars=one_minute, snapshots=raw_snapshots, variant=benchmark_variant)
    raw_control_metrics = _metrics([_build_trade_outcome(bars=one_minute, event=snapshot.event, overlay="BASE", r_loss_proxy=None) for snapshot in raw_snapshots])

    variant_rows: list[dict[str, Any]] = []
    for variant in CAUSAL_PROXY_VARIANTS:
        kept = [snapshot for snapshot in raw_snapshots if _passes_causal_proxy(snapshot, variant)]
        trades = [_build_trade_outcome(bars=one_minute, event=snapshot.event, overlay="BASE", r_loss_proxy=None) for snapshot in kept]
        metrics = _metrics(trades)
        variant_rows.append(
            {
                "variant_name": variant.variant_name,
                "description": variant.description,
                "rules": _variant_rule_summary(variant),
                "metrics": metrics,
                "comparison_vs_raw_control": _metric_delta(metrics, raw_control_metrics),
                "comparison_vs_non_causal_benchmark": _metric_delta(metrics, benchmark_row["metrics"]),
                "decision_bucket": _decision_bucket(metrics=metrics, raw_control_metrics=raw_control_metrics, benchmark_metrics=benchmark_row["metrics"]),
            }
        )

    best_variant = _pick_best_variant(variant_rows)
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "base_candidate_definition": {
            "population": "raw breadth_plus_agreement_combo",
            "benchmark_confirmation_rule": "minimal_post_trigger_confirmation_rule",
        },
        "same_bar_features_used": [
            "prior_10_bar_net_move_normalized",
            "prior_20_bar_net_move_normalized",
            "pre_burst_range_compression_or_expansion",
            "local_micro_range_breakout_flag",
            "largest_bar_concentration_metric",
            "materially_contributing_bar_count",
            "contributing_bar_breadth_metric",
            "same_direction_share",
            "body_dominance",
            "path_efficiency",
            "normalized_move",
            "acceleration_ratio",
            "late_extension_share",
            "body_to_range_quality",
            "wickiness_metric",
            "causal_quality_score",
        ],
        "causal_proxy_variants_tested": [variant.variant_name for variant in CAUSAL_PROXY_VARIANTS],
        "raw_control_metrics": raw_control_metrics,
        "non_causal_benchmark": {
            "variant_name": BENCHMARK_VARIANT,
            "metrics": benchmark_row["metrics"],
        },
        "variant_results": variant_rows,
        "best_causal_proxy_variant": {
            "variant_name": best_variant["variant_name"],
            "decision_bucket": best_variant["decision_bucket"],
        },
        "causalization_conclusion": _causalization_conclusion(best_variant=best_variant, benchmark_metrics=benchmark_row["metrics"]),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_same_bar_causalization.json"
    md_path = OUTPUT_DIR / "mgc_impulse_same_bar_causalization.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_same_bar_causalization",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_causal_proxy_variant": payload["best_causal_proxy_variant"],
        "causalization_conclusion": payload["causalization_conclusion"],
    }


def _passes_causal_proxy(snapshot: Any, variant: CausalProxyVariant) -> bool:
    if variant.variant_name == "raw_breadth_plus_agreement_control":
        return True
    impulse = snapshot.event.impulse
    largest_bar_share = float(impulse["largest_bar_share"])
    materially_contributing_bars = float(impulse["materially_contributing_bars"])
    contributing_breadth = float(impulse["contributing_breadth"])
    same_direction_share = float(impulse["same_direction_share"])
    body_dominance = float(impulse["body_dominance"])
    path_efficiency = float(impulse["path_efficiency"])
    normalized_move = float(impulse["normalized_move"])
    acceleration_ratio = float(impulse["acceleration_ratio"])
    late_extension_share = float(impulse["late_extension_share"])
    body_to_range_quality = float(snapshot.body_to_range_quality)
    wickiness_metric = round(1.0 - body_to_range_quality, 4)

    if variant.min_acceleration_ratio is not None and acceleration_ratio < variant.min_acceleration_ratio:
        return False
    if variant.min_body_to_range_quality is not None and body_to_range_quality < variant.min_body_to_range_quality:
        return False
    if variant.max_largest_bar_share is not None and largest_bar_share > variant.max_largest_bar_share:
        return False
    if variant.min_body_dominance is not None and body_dominance < variant.min_body_dominance:
        return False
    if variant.min_path_efficiency is not None and path_efficiency < variant.min_path_efficiency:
        return False
    if variant.min_contributing_breadth is not None and contributing_breadth < variant.min_contributing_breadth:
        return False
    if variant.max_prior_20_norm is not None and snapshot.prior_20_norm > variant.max_prior_20_norm:
        return False
    if variant.max_wickiness_metric is not None and wickiness_metric > variant.max_wickiness_metric:
        return False
    if variant.require_micro_breakout and not bool(snapshot.micro_breakout):
        return False
    if variant.max_compression_ratio is not None and snapshot.compression_ratio > variant.max_compression_ratio:
        return False
    if variant.min_quality_score is not None:
        score = _causal_quality_score(
            same_direction_share=same_direction_share,
            body_dominance=body_dominance,
            path_efficiency=path_efficiency,
            contributing_breadth=contributing_breadth,
            normalized_move=normalized_move,
            acceleration_ratio=acceleration_ratio,
            largest_bar_share=largest_bar_share,
            body_to_range_quality=body_to_range_quality,
            wickiness_metric=wickiness_metric,
            prior_20_norm=snapshot.prior_20_norm,
            late_extension_share=late_extension_share,
            materially_contributing_bars=materially_contributing_bars,
        )
        if score < variant.min_quality_score:
            return False
    return True


def _causal_quality_score(
    *,
    same_direction_share: float,
    body_dominance: float,
    path_efficiency: float,
    contributing_breadth: float,
    normalized_move: float,
    acceleration_ratio: float,
    largest_bar_share: float,
    body_to_range_quality: float,
    wickiness_metric: float,
    prior_20_norm: float,
    late_extension_share: float,
    materially_contributing_bars: float,
) -> float:
    normalized_move_score = min(normalized_move / 1.8, 1.0)
    acceleration_score = min(acceleration_ratio / 1.3, 1.0)
    concentration_score = max(0.0, 1.0 - (largest_bar_share / 0.6))
    wickiness_score = max(0.0, 1.0 - wickiness_metric)
    prior_run_score = max(0.0, 1.0 - max(prior_20_norm - 0.6, 0.0) / 1.2)
    late_extension_score = max(0.0, 1.0 - max(late_extension_share - 0.30, 0.0) / 0.50)
    material_bar_score = min(materially_contributing_bars / 4.0, 1.0)
    score = (
        0.12 * same_direction_share
        + 0.14 * body_dominance
        + 0.12 * path_efficiency
        + 0.10 * contributing_breadth
        + 0.08 * normalized_move_score
        + 0.10 * acceleration_score
        + 0.10 * concentration_score
        + 0.10 * body_to_range_quality
        + 0.05 * wickiness_score
        + 0.04 * prior_run_score
        + 0.03 * late_extension_score
        + 0.02 * material_bar_score
    )
    return round(score, 4)


def _metric_delta(metrics: dict[str, Any], reference: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_delta": _delta(metrics.get("trades"), reference.get("trades")),
        "realized_pnl_delta": _delta(metrics.get("realized_pnl"), reference.get("realized_pnl")),
        "avg_trade_delta": _delta(metrics.get("avg_trade"), reference.get("avg_trade")),
        "median_trade_delta": _delta(metrics.get("median_trade"), reference.get("median_trade")),
        "profit_factor_delta": _delta(metrics.get("profit_factor"), reference.get("profit_factor")),
        "max_drawdown_delta": _delta(metrics.get("max_drawdown"), reference.get("max_drawdown")),
        "top_3_contribution_delta": _delta(metrics.get("top_3_contribution"), reference.get("top_3_contribution")),
    }


def _delta(current: Any, reference: Any) -> float | None:
    if current is None or reference is None:
        return None
    return round(float(current) - float(reference), 4)


def _decision_bucket(*, metrics: dict[str, Any], raw_control_metrics: dict[str, Any], benchmark_metrics: dict[str, Any]) -> str:
    trades = int(metrics.get("trades") or 0)
    if trades <= 0:
        return "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION"
    trade_ratio_vs_benchmark = trades / max(int(benchmark_metrics.get("trades") or 1), 1)
    pnl_ratio_vs_benchmark = (float(metrics.get("realized_pnl") or 0.0) / max(float(benchmark_metrics.get("realized_pnl") or 1.0), 1.0))
    if (
        (metrics.get("profit_factor") or 0.0) >= 2.0
        and (metrics.get("median_trade") or -999999.0) > 0
        and (metrics.get("top_3_contribution") or 999999.0) <= 60.0
        and bool(metrics.get("survives_without_top_3"))
        and trade_ratio_vs_benchmark >= 0.5
        and pnl_ratio_vs_benchmark >= 0.45
    ):
        return "CAUSAL_PROXY_RECOVERS_ENOUGH"
    if (
        (metrics.get("profit_factor") or 0.0) >= 1.4
        and (metrics.get("realized_pnl") or 0.0) > (raw_control_metrics.get("realized_pnl") or 0.0)
        and (metrics.get("top_3_contribution") or 999999.0) <= (raw_control_metrics.get("top_3_contribution") or 999999.0)
    ):
        return "CAUSAL_PROXY_PROMISING_BUT_WEAKER"
    if (metrics.get("profit_factor") or 0.0) >= 1.0 and (metrics.get("realized_pnl") or 0.0) > 0:
        return "CAUSAL_PROXY_TOO_WEAK_USE_DELAYED_ENTRY"
    return "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION"


def _pick_best_variant(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            {
                "CAUSAL_PROXY_RECOVERS_ENOUGH": 3,
                "CAUSAL_PROXY_PROMISING_BUT_WEAKER": 2,
                "CAUSAL_PROXY_TOO_WEAK_USE_DELAYED_ENTRY": 1,
                "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION": 0,
            }[row["decision_bucket"]],
            float(row["metrics"]["realized_pnl"] or 0.0),
            float(row["metrics"]["profit_factor"] or 0.0),
            -float(row["metrics"]["top_3_contribution"] or 999999.0),
        ),
        reverse=True,
    )[0]


def _causalization_conclusion(*, best_variant: dict[str, Any], benchmark_metrics: dict[str, Any]) -> dict[str, Any]:
    benchmark_trades = float(benchmark_metrics.get("trades") or 0.0)
    best_trades = float(best_variant["metrics"].get("trades") or 0.0)
    return {
        "best_variant": best_variant["variant_name"],
        "best_bucket": best_variant["decision_bucket"],
        "benchmark_trade_recovery_ratio": round(best_trades / benchmark_trades, 4) if benchmark_trades > 0 else None,
        "delayed_confirmation_entry_should_be_next_step": best_variant["decision_bucket"] in {
            "CAUSAL_PROXY_TOO_WEAK_USE_DELAYED_ENTRY",
            "FAMILY_DOES_NOT_SURVIVE_CAUSALIZATION",
        },
    }


def _variant_rule_summary(variant: CausalProxyVariant) -> dict[str, Any]:
    return {
        "min_acceleration_ratio": variant.min_acceleration_ratio,
        "min_body_to_range_quality": variant.min_body_to_range_quality,
        "max_largest_bar_share": variant.max_largest_bar_share,
        "min_body_dominance": variant.min_body_dominance,
        "min_path_efficiency": variant.min_path_efficiency,
        "min_contributing_breadth": variant.min_contributing_breadth,
        "max_prior_20_norm": variant.max_prior_20_norm,
        "max_wickiness_metric": variant.max_wickiness_metric,
        "require_micro_breakout": variant.require_micro_breakout,
        "max_compression_ratio": variant.max_compression_ratio,
        "min_quality_score": variant.min_quality_score,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Same-Bar Causalization",
        "",
        f"- Symbol: `{payload['symbol']}`",
        f"- Family: `{payload['family_name']}`",
        f"- Sample: `{payload['sample_start_date']}` to `{payload['sample_end_date']}`",
        "",
        "## Variant Results",
        "",
        "| Variant | Bucket | Trades | PnL | PF | Median | Max DD | Top-3 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        lines.append(
            f"| {row['variant_name']} | {row['decision_bucket']} | {metrics['trades']} | {metrics['realized_pnl']} | "
            f"{metrics['profit_factor']} | {metrics['median_trade']} | {metrics['max_drawdown']} | {metrics['top_3_contribution']} |"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            f"- Best causal proxy: `{payload['best_causal_proxy_variant']['variant_name']}`",
            f"- Best bucket: `{payload['best_causal_proxy_variant']['decision_bucket']}`",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
