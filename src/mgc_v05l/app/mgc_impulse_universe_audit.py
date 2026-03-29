"""Universe-design audit for the MGC impulse burst continuation branch."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .mgc_impulse_burst_asymmetry_report import _build_trade_outcome, _collect_candidate_events
from .mgc_impulse_burst_continuation_research import (
    COMMON_CONTEXT_TIMEFRAME,
    COMMON_DETECTION_TIMEFRAME,
    COMMON_SYMBOL,
    COMMON_WINDOW_DESCRIPTION,
    OUTPUT_DIR,
    Bar,
    _build_latest_context_lookup,
    _rolling_atr,
    _rolling_mean,
    _rolling_realized_vol,
)
from .mgc_impulse_burst_continuation_second_pass import BASE_SPEC, REFINEMENT_SPECS, _load_bars
from .mgc_impulse_burst_loser_archetypes import _feature_row
from .mgc_impulse_burst_third_pass_narrowing import _event_snapshot
from .mgc_impulse_delayed_confirmation_revalidation import (
    _build_delayed_trade_outcome,
    _resolve_confirmation_resolution,
)
from .mgc_impulse_same_bar_causalization import CAUSAL_PROXY_VARIANTS, _passes_causal_proxy
from .mgc_impulse_spike_confirmation_pass import _metrics
from .mgc_impulse_spike_subtypes import _spike_feature_row


SCREENED_VARIANT = "breadth_plus_agreement_combo"
SAME_BAR_REFERENCE_VARIANT = "mild_context_quality_proxy"
DELAYED_ENTRY_REFERENCE = "NEXT_OPEN_AFTER_CONFIRM"


@dataclass(frozen=True)
class UniverseAuditRow:
    event: Any
    subclass_bucket: str
    spike_subtype: str | None
    same_bar_proxy_pass: bool
    delayed_confirmation_satisfied: bool
    trade: Any
    feature_row: Any


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_universe_audit(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-universe-audit")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_universe_audit(*, symbol: str) -> dict[str, Any]:
    base_spec = BASE_SPEC
    screened_spec = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == SCREENED_VARIANT)
    same_bar_variant = next(variant for variant in CAUSAL_PROXY_VARIANTS if variant.variant_name == SAME_BAR_REFERENCE_VARIANT)

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

    original_events = _collect_candidate_events(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=base_spec,
    )
    screened_events = _collect_candidate_events(
        bars_1m=one_minute,
        bars_5m=five_minute,
        atr_1m=atr_1m,
        rv_1m=rv_1m,
        vol_baseline_1m=vol_baseline_1m,
        atr_5m=atr_5m,
        context_lookup=context_lookup,
        spec=screened_spec,
    )
    screened_signal_indices = {event.signal_index for event in screened_events}
    original_signal_indices = {event.signal_index for event in original_events}
    screened_resequenced_only = len(screened_signal_indices - original_signal_indices)

    original_rows = [
        _audit_row(
            bars=one_minute,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            event=event,
            same_bar_variant=same_bar_variant,
        )
        for event in original_events
    ]
    screened_rows = [row for row in original_rows if row.event.signal_index in screened_signal_indices]
    excluded_rows = [row for row in original_rows if row.event.signal_index not in screened_signal_indices]

    populations = {
        "original_broader_universe": original_rows,
        "screened_branch_universe": screened_rows,
        "excluded_by_screening": excluded_rows,
    }
    population_summary = {name: _population_summary(rows, bars=one_minute) for name, rows in populations.items()}

    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "screening_layers_identified": _screening_layers(base_spec=base_spec, screened_spec=screened_spec),
        "population_sizes": {
            "original_broader_universe": len(original_rows),
            "screened_branch_direct_universe": len(screened_events),
            "screened_branch_retained_from_original": len(screened_rows),
            "excluded_or_displaced_from_original": len(excluded_rows),
            "screened_resequenced_only": screened_resequenced_only,
        },
        "population_comparison": population_summary,
        "key_composition_differences": _key_composition_differences(population_summary),
        "verdict": _audit_verdict(population_summary=population_summary),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_universe_audit.json"
    md_path = OUTPUT_DIR / "mgc_impulse_universe_audit.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_universe_audit",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "verdict": payload["verdict"],
    }


def _audit_row(*, bars: list[Bar], atr_1m: list[float | None], rv_1m: list[float | None], event: Any, same_bar_variant: Any) -> UniverseAuditRow:
    snapshot = _event_snapshot(bars_1m=bars, atr_1m=atr_1m, rv_1m=rv_1m, event=event)
    feature = _feature_row(bars, atr_1m, rv_1m, event)
    trade = _build_trade_outcome(bars=bars, event=event, overlay="BASE", r_loss_proxy=None)
    spike_row = _spike_feature_row(bars, snapshot) if snapshot.subclass_bucket == "SPIKE_DOMINATED_OTHER" else None
    delayed_resolution = _resolve_confirmation_resolution(
        bars=bars,
        signal_index=event.signal_index,
        direction=str(event.impulse["direction"]),
        entry_mode=DELAYED_ENTRY_REFERENCE,
    )
    return UniverseAuditRow(
        event=event,
        subclass_bucket=str(snapshot.subclass_bucket),
        spike_subtype=spike_row.subtype if spike_row else None,
        same_bar_proxy_pass=_passes_causal_proxy(snapshot, same_bar_variant),
        delayed_confirmation_satisfied=delayed_resolution is not None,
        trade=trade,
        feature_row=feature,
    )


def _population_summary(rows: list[UniverseAuditRow], *, bars: list[Bar]) -> dict[str, Any]:
    trades = [row.trade for row in rows]
    pnls = _metrics(trades)
    subclass_counts = Counter(row.subclass_bucket for row in rows)
    spike_rows = [row for row in rows if row.spike_subtype is not None]
    spike_counts = Counter(row.spike_subtype for row in spike_rows)
    delayed_trades = []
    for row in rows:
        resolution = _resolve_confirmation_resolution(
            bars=bars,
            signal_index=row.event.signal_index,
            direction=str(row.event.impulse["direction"]),
            entry_mode=DELAYED_ENTRY_REFERENCE,
        )
        if resolution is None:
            continue
        delayed_trade = _build_delayed_trade_outcome(
            bars=bars,
            event=row.event,
            resolution=resolution,
            entry_mode=DELAYED_ENTRY_REFERENCE,
        )
        if delayed_trade is not None:
            delayed_trades.append(delayed_trade)
    same_bar_pass_rows = [row for row in rows if row.same_bar_proxy_pass]
    return {
        "count": len(rows),
        "base_trade_metrics": pnls,
        "subclass_mix": _share_counter(subclass_counts),
        "spike_subtype_mix": _share_counter(spike_counts),
        "disproportionately_executable_characteristics": {
            "same_bar_proxy_pass_rate": _share(sum(1 for row in rows if row.same_bar_proxy_pass), len(rows)),
            "delayed_confirmation_satisfied_rate": _share(sum(1 for row in rows if row.delayed_confirmation_satisfied), len(rows)),
            "new_extension_within_2_bars_rate": _share(sum(1 for row in rows if row.feature_row.new_extension_within_2_bars_flag >= 1.0), len(rows)),
            "confirmation_bar_count_first_3_ge_2_rate": _share(sum(1 for row in rows if row.delayed_confirmation_satisfied), len(rows)),
            "mean_largest_bar_concentration": _mean([row.feature_row.largest_bar_concentration_metric for row in rows]),
            "mean_contributing_bar_breadth": _mean([row.feature_row.contributing_bar_breadth_metric for row in rows]),
            "mean_body_to_range_quality": _mean([row.feature_row.body_to_range_quality for row in rows]),
            "mean_late_extension_share": _mean([row.feature_row.late_extension_share for row in rows]),
        },
        "compact_benchmark_checks": {
            "same_bar_proxy_subset_metrics": _metrics([row.trade for row in same_bar_pass_rows]),
            "delayed_confirmation_next_open_metrics": _metrics(delayed_trades),
        },
    }


def _screening_layers(*, base_spec: Any, screened_spec: Any) -> list[dict[str, Any]]:
    return [
        {
            "layer": "agreement_tightening",
            "filters": [
                {"metric": "same_direction_share_min", "base": base_spec.same_direction_share_min, "screened": screened_spec.same_direction_share_min},
                {"metric": "body_dominance_min", "base": base_spec.body_dominance_min, "screened": screened_spec.body_dominance_min},
                {"metric": "path_efficiency_min", "base": base_spec.path_efficiency_min, "screened": screened_spec.path_efficiency_min},
            ],
        },
        {
            "layer": "anti_spike_breadth_screen",
            "filters": [
                {"metric": "largest_bar_share_max", "base": base_spec.largest_bar_share_max, "screened": screened_spec.largest_bar_share_max},
                {"metric": "min_material_bars", "base": base_spec.min_material_bars, "screened": screened_spec.min_material_bars},
                {"metric": "material_bar_share_min", "base": base_spec.material_bar_share_min, "screened": screened_spec.material_bar_share_min},
            ],
        },
        {
            "layer": "unchanged_core_detection",
            "filters": [
                {"metric": "normalized_move_threshold", "base": base_spec.normalized_move_threshold, "screened": screened_spec.normalized_move_threshold},
                {"metric": "window_size", "base": 8, "screened": 8},
                {"metric": "5m_context_filter", "base": "unchanged", "screened": "unchanged"},
            ],
        },
    ]


def _key_composition_differences(population_summary: dict[str, Any]) -> dict[str, Any]:
    original = population_summary["original_broader_universe"]
    screened = population_summary["screened_branch_universe"]
    excluded = population_summary["excluded_by_screening"]
    return {
        "screened_vs_original_subclass_shift": _share_diff(
            original["subclass_mix"],
            screened["subclass_mix"],
        ),
        "excluded_vs_original_subclass_shift": _share_diff(
            original["subclass_mix"],
            excluded["subclass_mix"],
        ),
        "excluded_spike_subtype_overrepresentation": _share_diff(
            original["spike_subtype_mix"],
            excluded["spike_subtype_mix"],
        ),
        "executable_plausibility_shift": {
            "same_bar_proxy_pass_rate_delta_excluded_minus_screened": _delta(
                excluded["disproportionately_executable_characteristics"]["same_bar_proxy_pass_rate"],
                screened["disproportionately_executable_characteristics"]["same_bar_proxy_pass_rate"],
            ),
            "delayed_confirmation_rate_delta_excluded_minus_screened": _delta(
                excluded["disproportionately_executable_characteristics"]["delayed_confirmation_satisfied_rate"],
                screened["disproportionately_executable_characteristics"]["delayed_confirmation_satisfied_rate"],
            ),
            "delayed_next_open_profit_factor_delta_excluded_minus_screened": _delta(
                excluded["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["profit_factor"],
                screened["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["profit_factor"],
            ),
            "delayed_next_open_median_trade_delta_excluded_minus_screened": _delta(
                excluded["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["median_trade"],
                screened["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["median_trade"],
            ),
        },
    }


def _audit_verdict(*, population_summary: dict[str, Any]) -> str:
    screened = population_summary["screened_branch_universe"]
    excluded = population_summary["excluded_by_screening"]
    same_bar_delta = _delta(
        excluded["disproportionately_executable_characteristics"]["same_bar_proxy_pass_rate"],
        screened["disproportionately_executable_characteristics"]["same_bar_proxy_pass_rate"],
    ) or 0.0
    delayed_pf_delta = _delta(
        excluded["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["profit_factor"],
        screened["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["profit_factor"],
    ) or 0.0
    delayed_median_delta = _delta(
        excluded["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["median_trade"],
        screened["compact_benchmark_checks"]["delayed_confirmation_next_open_metrics"]["median_trade"],
    ) or 0.0
    bad_trap_shift = _share_lookup(excluded["spike_subtype_mix"], "BAD_SPIKE_TRAP") - _share_lookup(screened["spike_subtype_mix"], "BAD_SPIKE_TRAP")

    if same_bar_delta >= 0.08 and delayed_pf_delta >= 0.2 and delayed_median_delta >= 2.0:
        return "SCREENING_MATERIALLY_BIASED_RESULTS"
    if same_bar_delta <= 0.03 and delayed_pf_delta <= 0.05 and delayed_median_delta <= 1.0 and bad_trap_shift >= 0.0:
        return "SCREENING_HELPFUL_NO_MATERIAL_BIAS"
    return "MIXED_BIAS_ONE_MORE_ORIGINAL_UNIVERSE_PASS_JUSTIFIED"


def _share_counter(counter: Counter[str]) -> dict[str, Any]:
    total = sum(counter.values())
    if total <= 0:
        return {}
    return {key: {"count": count, "share": round(count / total, 4)} for key, count in sorted(counter.items())}


def _share_diff(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    keys = set(left) | set(right)
    return {
        key: round(_share_lookup(right, key) - _share_lookup(left, key), 4)
        for key in sorted(keys)
    }


def _share_lookup(counter_share: dict[str, Any], key: str) -> float:
    value = counter_share.get(key)
    if not value:
        return 0.0
    return float(value["share"])


def _mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 4) if values else None


def _share(count: int, total: int) -> float:
    return round(count / total, 4) if total > 0 else 0.0


def _delta(left: float | int | None, right: float | int | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 4)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Universe Audit",
        "",
        f"- Symbol: {payload['symbol']}",
        f"- Family: {payload['family_name']}",
        f"- Verdict: {payload['verdict']}",
        "",
        "## Population Sizes",
        "",
        f"- Original broader universe: {payload['population_sizes']['original_broader_universe']}",
        f"- Screened branch direct universe: {payload['population_sizes']['screened_branch_direct_universe']}",
        f"- Screened branch retained from original: {payload['population_sizes']['screened_branch_retained_from_original']}",
        f"- Excluded or displaced from original: {payload['population_sizes']['excluded_or_displaced_from_original']}",
        f"- Screened resequenced only: {payload['population_sizes']['screened_resequenced_only']}",
        "",
        "## Key Composition Differences",
        "",
    ]
    for key, value in payload["key_composition_differences"].items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
