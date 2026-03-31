"""Phase 2A focused refinement for the leading futures quant candidates."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean
from typing import Any, Callable

from .quant_futures import (
    CandidateSpec,
    StrategyResearchArtifacts,
    _FrameSeries,
    _align_timestamps,
    _available_symbols,
    _avg,
    _build_candidate_specs,
    _build_feature_rows,
    _expectancy,
    _load_symbol_payload,
    _max_drawdown,
    _median,
    _profit_factor,
    _rank_score,
    _resolve_exit,
    _sharpe_proxy,
    _top_positive_share,
    _walk_forward_positive_ratio,
    _win_rate,
)


TARGET_BREAKOUT_ID = "breakout_acceptance.long.gated.tight"
TARGET_FAILED_REVERSAL_ID = "failed_move_reversal.short.ungated.tight"


@dataclass(frozen=True)
class Phase2ATradeRecord:
    variant_id: str
    family: str
    direction: str
    symbol: str
    entry_ts: str
    exit_ts: str
    entry_session: str
    r_multiple: float
    holding_bars: int
    exit_reason: str
    feature_snapshot: dict[str, float | str | bool]


@dataclass(frozen=True)
class Phase2AVariantSpec:
    variant_id: str
    family: str
    direction: str
    label: str
    candidate_origin: str
    params: dict[str, float]
    hold_bars: int
    stop_r: float
    target_r: float
    gating_mode: str
    score_field: str | None
    score_threshold: float | None


def run_quant_futures_phase2a(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    symbols: tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "quant_futures_phase2a").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_symbols = symbols or _available_symbols(resolved_database_path, execution_timeframe)
    symbol_store = _build_symbol_store(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        symbols=selected_symbols,
    )

    targets = [
        _analyze_target(
            target_id=TARGET_BREAKOUT_ID,
            symbol_store=symbol_store,
            selected_symbols=selected_symbols,
        ),
        _analyze_target(
            target_id=TARGET_FAILED_REVERSAL_ID,
            symbol_store=symbol_store,
            selected_symbols=selected_symbols,
        ),
    ]

    revised_ranking = _build_revised_ranking(targets)
    soft_gating_results = _build_soft_gating_summary(targets)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_path": str(resolved_database_path),
        "execution_timeframe": execution_timeframe,
        "symbols_tested": list(selected_symbols),
        "phase": "2A",
        "priorities": [
            "Replace hard gating with continuous regime/context scores.",
            "Deepen robustness work on the leading breakout and failed-move candidates.",
            "Push each candidate toward a smaller, more interpretable rule set without promoting it yet.",
        ],
        "revised_ranking": revised_ranking,
        "soft_gating_results": soft_gating_results,
        "targets": targets,
        "overall_recommendation": _build_overall_recommendation(targets),
    }

    json_path = resolved_output_dir / "quant_futures_phase2a_report.json"
    markdown_path = resolved_output_dir / "quant_futures_phase2a_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_phase2a_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(
        json_path=json_path,
        markdown_path=markdown_path,
        report=report,
    )


def _build_symbol_store(
    *,
    database_path: Path,
    execution_timeframe: str,
    symbols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    store: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        payload = _load_symbol_payload(
            database_path=database_path,
            symbol=symbol,
            execution_timeframe=execution_timeframe,
        )
        if payload is None:
            continue
        features = _build_feature_rows(
            execution=payload["execution"],
            higher=payload["higher"],
            alignments=payload["alignments"],
        )
        _augment_soft_scores(
            features=features,
            higher=payload["higher"],
            alignments=payload["alignments"],
        )
        store[symbol] = {
            "execution": payload["execution"],
            "features": features,
        }
    return store


def _augment_soft_scores(
    *,
    features: list[dict[str, Any]],
    higher: dict[str, _FrameSeries],
    alignments: dict[str, list[int]],
) -> None:
    for idx, feature in enumerate(features):
        if not feature.get("ready"):
            continue
        idx60 = alignments["60m"][idx]
        idx240 = alignments["240m"][idx]
        idx720 = alignments["720m"][idx]
        idx1440 = alignments["1440m"][idx]
        if min(idx60, idx240, idx720, idx1440) < 0:
            feature["ready"] = False
            continue

        range_60 = higher["60m"].mean_range(idx60, 8)
        range_240 = higher["240m"].mean_range(idx240, 6)
        range_1440 = higher["1440m"].mean_range(idx1440, 20)
        if range_60 is None or range_240 is None or range_1440 is None:
            feature["ready"] = False
            continue

        vol_ratio_60_240 = range_60 / max(range_240, 1e-9)
        vol_ratio_240_1440 = range_240 / max(range_1440, 1e-9)
        continuation_long_score = _continuation_score(
            direction="LONG",
            slope_240=float(feature["slope_240"]),
            slope_720=float(feature["slope_720"]),
            slope_1440=float(feature["slope_1440"]),
            eff_240=float(feature["eff_240"]),
            eff_720=float(feature["eff_720"]),
            dist_240=float(feature["dist_240"]),
            vol_ratio_60_240=vol_ratio_60_240,
            vol_ratio_240_1440=vol_ratio_240_1440,
        )
        continuation_short_score = _continuation_score(
            direction="SHORT",
            slope_240=float(feature["slope_240"]),
            slope_720=float(feature["slope_720"]),
            slope_1440=float(feature["slope_1440"]),
            eff_240=float(feature["eff_240"]),
            eff_720=float(feature["eff_720"]),
            dist_240=float(feature["dist_240"]),
            vol_ratio_60_240=vol_ratio_60_240,
            vol_ratio_240_1440=vol_ratio_240_1440,
        )
        reversal_short_score = _reversal_score(
            direction="SHORT",
            slope_240=float(feature["slope_240"]),
            slope_720=float(feature["slope_720"]),
            eff_240=float(feature["eff_240"]),
            dist_240=float(feature["dist_240"]),
            dist_1440=float(feature["dist_1440"]),
            vol_ratio_60_240=vol_ratio_60_240,
        )
        reversal_long_score = _reversal_score(
            direction="LONG",
            slope_240=float(feature["slope_240"]),
            slope_720=float(feature["slope_720"]),
            eff_240=float(feature["eff_240"]),
            dist_240=float(feature["dist_240"]),
            dist_1440=float(feature["dist_1440"]),
            vol_ratio_60_240=vol_ratio_60_240,
        )
        feature["vol_ratio_60_240"] = round(vol_ratio_60_240, 6)
        feature["vol_ratio_240_1440"] = round(vol_ratio_240_1440, 6)
        feature["continuation_score_long"] = continuation_long_score
        feature["continuation_score_short"] = continuation_short_score
        feature["reversal_score_short"] = reversal_short_score
        feature["reversal_score_long"] = reversal_long_score


def _continuation_score(
    *,
    direction: str,
    slope_240: float,
    slope_720: float,
    slope_1440: float,
    eff_240: float,
    eff_720: float,
    dist_240: float,
    vol_ratio_60_240: float,
    vol_ratio_240_1440: float,
) -> float:
    sign = 1.0 if direction == "LONG" else -1.0
    slope240_component = _to_unit_interval(sign * slope_240, -0.15, 1.50)
    slope720_component = _to_unit_interval(sign * slope_720, -0.10, 1.00)
    slope1440_component = _to_unit_interval(sign * slope_1440, -0.20, 2.00)
    persistence240_component = _to_unit_interval(eff_240, 0.30, 0.80)
    persistence720_component = _to_unit_interval(eff_720, 0.25, 0.75)
    distance_center = 0.45 if direction == "LONG" else -0.45
    distance_component = _moderate_unit_interval(dist_240, distance_center, 1.25)
    vol_component = (
        _moderate_unit_interval(vol_ratio_60_240, 1.0, 0.60)
        + _moderate_unit_interval(vol_ratio_240_1440, 1.0, 0.75)
    ) / 2.0
    score = (
        0.26 * slope240_component
        + 0.18 * slope720_component
        + 0.08 * slope1440_component
        + 0.18 * persistence240_component
        + 0.10 * persistence720_component
        + 0.12 * distance_component
        + 0.08 * vol_component
    )
    return round(score, 6)


def _reversal_score(
    *,
    direction: str,
    slope_240: float,
    slope_720: float,
    eff_240: float,
    dist_240: float,
    dist_1440: float,
    vol_ratio_60_240: float,
) -> float:
    if direction == "SHORT":
        extension_component = _to_unit_interval(dist_240, 0.35, 1.80)
        longer_component = _to_unit_interval(slope_720, -0.10, 0.90)
        mature_component = 1.0 - _to_unit_interval(eff_240, 0.45, 0.85)
        higher_extension = _to_unit_interval(dist_1440, 0.15, 1.40)
    else:
        extension_component = _to_unit_interval(-dist_240, 0.35, 1.80)
        longer_component = _to_unit_interval(-slope_720, -0.10, 0.90)
        mature_component = 1.0 - _to_unit_interval(eff_240, 0.45, 0.85)
        higher_extension = _to_unit_interval(-dist_1440, 0.15, 1.40)
    vol_component = _to_unit_interval(vol_ratio_60_240, 0.90, 1.80)
    slope_component = _to_unit_interval(abs(slope_240), 0.20, 1.50)
    score = (
        0.28 * extension_component
        + 0.16 * higher_extension
        + 0.18 * longer_component
        + 0.16 * mature_component
        + 0.12 * vol_component
        + 0.10 * slope_component
    )
    return round(score, 6)


def _analyze_target(
    *,
    target_id: str,
    symbol_store: dict[str, dict[str, Any]],
    selected_symbols: tuple[str, ...],
) -> dict[str, Any]:
    specs = {spec.candidate_id: spec for spec in _build_candidate_specs()}
    baseline_spec = specs[target_id]
    perturbation_variants = _build_perturbation_variants(baseline_spec)
    soft_gating_variants = _build_soft_gating_variants(baseline_spec)
    approximation_variants = _build_approximation_variants(baseline_spec)
    asymmetry_variants = _build_asymmetry_variants(baseline_spec)

    soft_results = [_evaluate_variant(symbol_store=symbol_store, selected_symbols=selected_symbols, variant=variant) for variant in soft_gating_variants]
    perturbation_results = [_evaluate_variant(symbol_store=symbol_store, selected_symbols=selected_symbols, variant=variant) for variant in perturbation_variants]
    approximation_results = [_evaluate_variant(symbol_store=symbol_store, selected_symbols=selected_symbols, variant=variant) for variant in approximation_variants]
    asymmetry_results = [_evaluate_variant(symbol_store=symbol_store, selected_symbols=selected_symbols, variant=variant) for variant in asymmetry_variants]

    soft_results.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    perturbation_results.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    best_result = _select_target_best_result(soft_results)
    feature_dependency = _feature_dependency_diagnostics(
        trades=best_result["trades"],
        family=baseline_spec.family,
        direction=baseline_spec.direction,
    )
    interpretability = _build_interpretability_findings(
        approximation_results=approximation_results,
        best_result=best_result,
    )
    robustness = _build_robustness_findings(
        best_result=best_result,
        perturbation_results=perturbation_results,
    )
    recommendation = _candidate_recommendation(
        best_result=best_result,
        interpretability=interpretability,
    )

    return {
        "target_candidate_id": target_id,
        "family": baseline_spec.family,
        "direction": baseline_spec.direction,
        "revised_best_variant": {
            "variant_id": best_result["variant"]["variant_id"],
            "label": best_result["variant"]["label"],
            "summary": best_result["summary"],
        },
        "soft_gating_results": {
            "variants": [
                {
                    "variant": row["variant"],
                    "summary": row["summary"],
                }
                for row in soft_results
            ],
            "comparison_note": _soft_comparison_note(soft_results),
        },
        "robustness_findings": robustness,
        "interpretability_findings": interpretability,
        "feature_dependency_diagnostics": feature_dependency,
        "instrument_level_breakdown": best_result["summary"]["per_symbol"],
        "walk_forward_analysis": best_result["summary"]["walk_forward_windows"],
        "session_dependence_analysis": best_result["summary"]["session_breakdown"],
        "trade_concentration_analysis": best_result["summary"]["concentration"],
        "cost_sensitivity": best_result["summary"]["cost_sensitivity"],
        "long_short_asymmetry_review": {
            "counterpart_ranked_variants": [
                {
                    "variant": row["variant"],
                    "summary": row["summary"],
                }
                for row in asymmetry_results[:4]
            ],
            "relevant_takeaway": _asymmetry_takeaway(
                primary=best_result["summary"],
                asymmetry_results=asymmetry_results,
                primary_direction=baseline_spec.direction,
            ),
        },
        "approaching_promotion_shortlist_quality": _approaching_shortlist_quality(best_result["summary"]),
        "explicit_recommendation": recommendation,
    }


def _build_soft_gating_variants(spec: CandidateSpec) -> list[Phase2AVariantSpec]:
    variants = [
        _variant_from_candidate_spec(
            spec,
            variant_id=f"{spec.candidate_id}.baseline",
            label="baseline",
            gating_mode="hard" if spec.gated else "ungated",
            score_field=None,
            score_threshold=None,
        )
    ]
    if spec.family == "breakout_acceptance":
        structural_ungated = _variant_from_candidate_spec(
            spec,
            variant_id=f"{spec.family}.{spec.direction.lower()}.phase2a.ungated_structural",
            label="ungated_structural",
            gating_mode="ungated",
            score_field=None,
            score_threshold=None,
        )
        variants.append(structural_ungated)
        thresholds = (0.48, 0.56, 0.64)
        for threshold in thresholds:
            variants.append(
                _variant_from_candidate_spec(
                    spec,
                    variant_id=f"{spec.family}.{spec.direction.lower()}.phase2a.soft_{str(threshold).replace('.', '_')}",
                    label=f"soft_score_{threshold:.2f}",
                    gating_mode="soft",
                    score_field="continuation_score_long" if spec.direction == "LONG" else "continuation_score_short",
                    score_threshold=threshold,
                )
            )
    else:
        hard_compare = _variant_from_candidate_spec(
            spec,
            variant_id=f"{spec.family}.{spec.direction.lower()}.phase2a.hard_compare",
            label="hard_context_compare",
            gating_mode="hard",
            score_field=None,
            score_threshold=None,
        )
        variants.append(hard_compare)
        thresholds = (0.30, 0.40, 0.50, 0.60)
        score_field = "reversal_score_short" if spec.direction == "SHORT" else "reversal_score_long"
        for threshold in thresholds:
            variants.append(
                _variant_from_candidate_spec(
                    spec,
                    variant_id=f"{spec.family}.{spec.direction.lower()}.phase2a.soft_{str(threshold).replace('.', '_')}",
                    label=f"soft_score_{threshold:.2f}",
                    gating_mode="soft",
                    score_field=score_field,
                    score_threshold=threshold,
                )
            )
    return variants


def _build_perturbation_variants(spec: CandidateSpec) -> list[Phase2AVariantSpec]:
    variants: list[Phase2AVariantSpec] = []
    if spec.family == "breakout_acceptance" and spec.direction == "LONG":
        for compression_60_max in (0.82, 0.90, 0.98):
            for breakout_min in (0.24, 0.30, 0.36):
                for close_pos_min in (0.64, 0.68, 0.72):
                    for soft_threshold in (0.48, 0.56, 0.64):
                        params = dict(spec.params)
                        params["compression_60_max"] = compression_60_max
                        params["breakout_min"] = breakout_min
                        params["close_pos_min"] = close_pos_min
                        variants.append(
                            Phase2AVariantSpec(
                                variant_id=(
                                    "phase2a.breakout.long."
                                    f"c60_{compression_60_max:.2f}."
                                    f"bo_{breakout_min:.2f}."
                                    f"cp_{close_pos_min:.2f}."
                                    f"soft_{soft_threshold:.2f}"
                                ),
                                family=spec.family,
                                direction=spec.direction,
                                label="local_perturbation",
                                candidate_origin=spec.candidate_id,
                                params=params,
                                hold_bars=spec.hold_bars,
                                stop_r=spec.stop_r,
                                target_r=spec.target_r,
                                gating_mode="soft",
                                score_field="continuation_score_long",
                                score_threshold=soft_threshold,
                            )
                        )
    else:
        for close_pos_min in (0.62, 0.66, 0.70):
            for body_r_min in (0.22, 0.28, 0.34):
                for dist_240_extreme in (0.90, 1.05, 1.20):
                    for soft_threshold in (0.30, 0.40, 0.50, 0.60):
                        params = dict(spec.params)
                        params["close_pos_min"] = close_pos_min
                        params["body_r_min"] = body_r_min
                        params["dist_240_extreme"] = dist_240_extreme
                        variants.append(
                            Phase2AVariantSpec(
                                variant_id=(
                                    "phase2a.failed.short."
                                    f"cp_{close_pos_min:.2f}."
                                    f"body_{body_r_min:.2f}."
                                    f"dist_{dist_240_extreme:.2f}."
                                    f"soft_{soft_threshold:.2f}"
                                ),
                                family=spec.family,
                                direction=spec.direction,
                                label="local_perturbation",
                                candidate_origin=spec.candidate_id,
                                params=params,
                                hold_bars=spec.hold_bars,
                                stop_r=spec.stop_r,
                                target_r=spec.target_r,
                                gating_mode="soft",
                                score_field="reversal_score_short",
                                score_threshold=soft_threshold,
                            )
                        )
    return variants


def _build_approximation_variants(spec: CandidateSpec) -> list[Phase2AVariantSpec]:
    variants = []
    if spec.family == "breakout_acceptance" and spec.direction == "LONG":
        variants.extend(
            [
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.breakout.long.approx.core_plus_soft",
                    label="approx_core_plus_soft",
                    gating_mode="soft",
                    score_field="continuation_score_long",
                    score_threshold=0.56,
                    params_override={"compression_60_max": 10.0, "slope_60_min": -10.0},
                ),
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.breakout.long.approx.minimal",
                    label="approx_minimal",
                    gating_mode="soft",
                    score_field="continuation_score_long",
                    score_threshold=0.56,
                    params_override={"compression_60_max": 10.0, "compression_5_max": 10.0, "slope_60_min": -10.0},
                ),
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.breakout.long.approx.structural_only",
                    label="approx_structural_only",
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    params_override={"compression_60_max": 10.0, "slope_60_min": -10.0},
                ),
            ]
        )
    else:
        variants.extend(
            [
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.failed.short.approx.core_plus_soft",
                    label="approx_core_plus_soft",
                    gating_mode="soft",
                    score_field="reversal_score_short",
                    score_threshold=0.40,
                    params_override={"dist_240_extreme": 0.0},
                ),
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.failed.short.approx.minimal",
                    label="approx_minimal",
                    gating_mode="soft",
                    score_field="reversal_score_short",
                    score_threshold=0.40,
                    params_override={"dist_240_extreme": 0.0, "body_r_min": 0.0},
                ),
                _variant_from_candidate_spec(
                    spec,
                    variant_id="phase2a.failed.short.approx.structural_only",
                    label="approx_structural_only",
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    params_override={"dist_240_extreme": 0.0},
                ),
            ]
        )
    return variants


def _build_asymmetry_variants(spec: CandidateSpec) -> list[Phase2AVariantSpec]:
    opposite_specs = {item.candidate_id: item for item in _build_candidate_specs()}
    if spec.family == "breakout_acceptance":
        compare_ids = [
            "breakout_acceptance.short.gated.tight",
            "breakout_acceptance.short.ungated.tight",
        ]
        variants = [
            _variant_from_candidate_spec(
                opposite_specs[item_id],
                variant_id=f"{item_id}.compare",
                label="counterpart_compare",
                gating_mode="hard" if opposite_specs[item_id].gated else "ungated",
                score_field=None,
                score_threshold=None,
            )
            for item_id in compare_ids
        ]
        variants.append(
            _variant_from_candidate_spec(
                opposite_specs["breakout_acceptance.short.gated.tight"],
                variant_id="phase2a.breakout.short.soft_compare",
                label="counterpart_soft_compare",
                gating_mode="soft",
                score_field="continuation_score_short",
                score_threshold=0.56,
            )
        )
        return variants

    compare_ids = [
        "failed_move_reversal.long.gated.tight",
        "failed_move_reversal.long.ungated.tight",
    ]
    variants = [
        _variant_from_candidate_spec(
            opposite_specs[item_id],
            variant_id=f"{item_id}.compare",
            label="counterpart_compare",
            gating_mode="hard" if opposite_specs[item_id].gated else "ungated",
            score_field=None,
            score_threshold=None,
        )
        for item_id in compare_ids
    ]
    variants.append(
        _variant_from_candidate_spec(
            opposite_specs["failed_move_reversal.long.ungated.tight"],
            variant_id="phase2a.failed.long.soft_compare",
            label="counterpart_soft_compare",
            gating_mode="soft",
            score_field="reversal_score_long",
            score_threshold=0.40,
        )
    )
    return variants


def _variant_from_candidate_spec(
    spec: CandidateSpec,
    *,
    variant_id: str,
    label: str,
    gating_mode: str,
    score_field: str | None,
    score_threshold: float | None,
    params_override: dict[str, float] | None = None,
) -> Phase2AVariantSpec:
    params = dict(spec.params)
    if params_override:
        params.update(params_override)
    return Phase2AVariantSpec(
        variant_id=variant_id,
        family=spec.family,
        direction=spec.direction,
        label=label,
        candidate_origin=spec.candidate_id,
        params=params,
        hold_bars=spec.hold_bars,
        stop_r=spec.stop_r,
        target_r=spec.target_r,
        gating_mode=gating_mode,
        score_field=score_field,
        score_threshold=score_threshold,
    )


def _evaluate_variant(
    *,
    symbol_store: dict[str, dict[str, Any]],
    selected_symbols: tuple[str, ...],
    variant: Phase2AVariantSpec,
) -> dict[str, Any]:
    trades: list[Phase2ATradeRecord] = []
    for symbol, payload in symbol_store.items():
        trades.extend(
            _simulate_variant(
                variant=variant,
                symbol=symbol,
                execution=payload["execution"],
                features=payload["features"],
            )
        )
    summary = _summarize_phase2a_trades(trades=trades, selected_symbols=selected_symbols)
    return {
        "variant": asdict(variant),
        "trades": trades,
        "summary": summary,
    }


def _simulate_variant(
    *,
    variant: Phase2AVariantSpec,
    symbol: str,
    execution: _FrameSeries,
    features: list[dict[str, Any]],
) -> list[Phase2ATradeRecord]:
    trades: list[Phase2ATradeRecord] = []
    next_available_index = 0
    for index, feature in enumerate(features):
        if index < next_available_index or index + 1 >= len(execution.bars):
            continue
        if not feature.get("ready"):
            continue
        if not _phase2a_signal_matches(variant, feature):
            continue

        entry_index = index + 1
        entry_price = execution.opens[entry_index]
        risk = max(float(feature["risk_unit"]), 1e-6)
        stop_price = entry_price - variant.stop_r * risk if variant.direction == "LONG" else entry_price + variant.stop_r * risk
        target_price = entry_price + variant.target_r * risk if variant.direction == "LONG" else entry_price - variant.target_r * risk
        exit_index, exit_price, exit_reason = _resolve_exit(
            direction=variant.direction,
            execution=execution,
            entry_index=entry_index,
            hold_bars=variant.hold_bars,
            stop_price=stop_price,
            target_price=target_price,
        )
        signed_move = exit_price - entry_price if variant.direction == "LONG" else entry_price - exit_price
        r_multiple = signed_move / risk
        feature_snapshot = _feature_snapshot(variant=variant, feature=feature)
        trades.append(
            Phase2ATradeRecord(
                variant_id=variant.variant_id,
                family=variant.family,
                direction=variant.direction,
                symbol=symbol,
                entry_ts=execution.timestamps[entry_index].isoformat(),
                exit_ts=execution.timestamps[exit_index].isoformat(),
                entry_session=str(feature["session_label"]),
                r_multiple=r_multiple,
                holding_bars=max(exit_index - entry_index + 1, 1),
                exit_reason=exit_reason,
                feature_snapshot=feature_snapshot,
            )
        )
        next_available_index = exit_index + 1
    return trades


def _phase2a_signal_matches(variant: Phase2AVariantSpec, feature: dict[str, Any]) -> bool:
    params = variant.params
    if variant.family == "breakout_acceptance" and variant.direction == "LONG":
        if not (
            feature["compression_60"] <= params["compression_60_max"]
            and feature["compression_5"] <= params["compression_5_max"]
            and feature["breakout_up"] >= params["breakout_min"]
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["slope_60"] >= params["slope_60_min"]
        ):
            return False
        return _phase2a_context_pass(variant, feature)

    if variant.family == "breakout_acceptance" and variant.direction == "SHORT":
        if not (
            feature["compression_60"] <= params["compression_60_max"]
            and feature["compression_5"] <= params["compression_5_max"]
            and feature["breakout_down"] >= params["breakout_min"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["slope_60"] <= -params["slope_60_min"]
        ):
            return False
        return _phase2a_context_pass(variant, feature)

    if variant.family == "failed_move_reversal" and variant.direction == "SHORT":
        if not (
            feature["failed_breakout_short"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
            and feature["dist_240"] >= params["dist_240_extreme"]
        ):
            return False
        return _phase2a_context_pass(variant, feature)

    if variant.family == "failed_move_reversal" and variant.direction == "LONG":
        if not (
            feature["failed_breakout_long"]
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
            and feature["dist_240"] <= -params["dist_240_extreme"]
        ):
            return False
        return _phase2a_context_pass(variant, feature)

    return False


def _phase2a_context_pass(variant: Phase2AVariantSpec, feature: dict[str, Any]) -> bool:
    if variant.gating_mode == "ungated":
        return True
    if variant.gating_mode == "hard":
        if variant.family == "breakout_acceptance":
            return bool(feature["regime_up"] if variant.direction == "LONG" else feature["regime_down"])
        if variant.family == "failed_move_reversal":
            if variant.direction == "SHORT":
                return bool(feature["extended_up"] or feature["regime_neutral"] or feature["regime_up"])
            return bool(feature["extended_down"] or feature["regime_neutral"] or feature["regime_down"])
    if variant.gating_mode == "soft":
        if variant.score_field is None or variant.score_threshold is None:
            return True
        return float(feature[variant.score_field]) >= float(variant.score_threshold)
    return True


def _feature_snapshot(variant: Phase2AVariantSpec, feature: dict[str, Any]) -> dict[str, float | str | bool]:
    base_keys = {
        "session_label",
        "compression_60",
        "compression_5",
        "breakout_up",
        "breakout_down",
        "close_pos",
        "slope_60",
        "slope_240",
        "slope_720",
        "dist_240",
        "dist_1440",
        "body_r",
        "vol_ratio_60_240",
        "continuation_score_long",
        "continuation_score_short",
        "reversal_score_short",
        "reversal_score_long",
        "failed_breakout_short",
        "failed_breakout_long",
    }
    return {key: feature[key] for key in base_keys if key in feature}


def _summarize_phase2a_trades(
    *,
    trades: list[Phase2ATradeRecord],
    selected_symbols: tuple[str, ...],
) -> dict[str, Any]:
    r_values = [trade.r_multiple for trade in trades]
    per_symbol = []
    for symbol in selected_symbols:
        symbol_trades = [trade for trade in trades if trade.symbol == symbol]
        if not symbol_trades:
            continue
        symbol_r = [trade.r_multiple for trade in symbol_trades]
        per_symbol.append(
            {
                "symbol": symbol,
                "trade_count": len(symbol_trades),
                "expectancy_r": _expectancy(symbol_r),
                "win_rate": _win_rate(symbol_r),
                "profit_factor": _profit_factor(symbol_r),
                "max_drawdown_r": _max_drawdown(symbol_r),
                "walk_forward_positive_ratio": _walk_forward_positive_ratio(symbol_trades),  # type: ignore[arg-type]
            }
        )
    per_symbol.sort(key=lambda row: (-row["expectancy_r"], -row["trade_count"], row["symbol"]))
    positive_symbol_share = (
        sum(1 for row in per_symbol if row["expectancy_r"] > 0.0) / float(max(len(selected_symbols), 1))
    )
    cost_005 = _expectancy([value - 0.05 for value in r_values]) if r_values else 0.0
    cost_010 = _expectancy([value - 0.10 for value in r_values]) if r_values else 0.0
    phase2a_score = _rank_score(
        expectancy=_expectancy(r_values),
        positive_symbol_share=positive_symbol_share,
        walk_forward_positive_ratio=_walk_forward_positive_ratio(trades),  # type: ignore[arg-type]
        trade_count=len(r_values),
        max_drawdown=_max_drawdown(r_values),
        cost_005=cost_005,
        top_3_positive_share=_top_positive_share(r_values, 3),
        parameter_neighbor_stability=None,
        gated=False,
    )
    return {
        "trade_count": len(r_values),
        "expectancy_r": _expectancy(r_values),
        "win_rate": _win_rate(r_values),
        "avg_win_r": _avg([value for value in r_values if value > 0.0]),
        "avg_loss_r": _avg([value for value in r_values if value < 0.0]),
        "profit_factor": _profit_factor(r_values),
        "max_drawdown_r": _max_drawdown(r_values),
        "sharpe_proxy": _sharpe_proxy(r_values),
        "avg_holding_bars": _avg([trade.holding_bars for trade in trades]),
        "positive_symbol_share": round(positive_symbol_share, 6),
        "walk_forward_positive_ratio": _walk_forward_positive_ratio(trades),  # type: ignore[arg-type]
        "cost_expectancy_r_005": cost_005,
        "cost_expectancy_r_010": cost_010,
        "phase2a_score": phase2a_score,
        "per_symbol": per_symbol,
        "walk_forward_windows": _walk_forward_windows(trades),
        "session_breakdown": _session_breakdown(trades),
        "concentration": _concentration_summary(r_values),
        "cost_sensitivity": _cost_sensitivity(r_values),
    }


def _walk_forward_windows(trades: list[Phase2ATradeRecord], window_count: int = 6) -> list[dict[str, Any]]:
    if not trades:
        return []
    ordered = sorted(trades, key=lambda row: row.entry_ts)
    bucket_size = max(len(ordered) // window_count, 1)
    rows = []
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        if not bucket:
            continue
        r_values = [trade.r_multiple for trade in bucket]
        rows.append(
            {
                "start_entry_ts": bucket[0].entry_ts,
                "end_entry_ts": bucket[-1].entry_ts,
                "trade_count": len(bucket),
                "expectancy_r": _expectancy(r_values),
                "win_rate": _win_rate(r_values),
                "profit_factor": _profit_factor(r_values),
                "max_drawdown_r": _max_drawdown(r_values),
            }
        )
    return rows


def _session_breakdown(trades: list[Phase2ATradeRecord]) -> list[dict[str, Any]]:
    rows = []
    for session in ("ASIA", "LONDON", "US", "UNKNOWN"):
        session_trades = [trade for trade in trades if trade.entry_session == session]
        if not session_trades:
            continue
        r_values = [trade.r_multiple for trade in session_trades]
        rows.append(
            {
                "session": session,
                "trade_count": len(session_trades),
                "expectancy_r": _expectancy(r_values),
                "win_rate": _win_rate(r_values),
                "profit_factor": _profit_factor(r_values),
                "share_of_trades": round(len(session_trades) / float(len(trades)), 6),
            }
        )
    rows.sort(key=lambda row: row["session"])
    return rows


def _concentration_summary(r_values: list[float]) -> dict[str, Any]:
    ordered = sorted(r_values, reverse=True)
    total = sum(r_values)
    top1 = ordered[:1]
    top3 = ordered[:3]
    top5 = ordered[:5]
    return {
        "top_1_share_of_total_r": round(sum(top1) / total, 6) if total != 0.0 else 0.0,
        "top_3_share_of_total_r": round(sum(top3) / total, 6) if total != 0.0 else 0.0,
        "top_5_share_of_total_r": round(sum(top5) / total, 6) if total != 0.0 else 0.0,
        "returns_without_top_1_r": round(sum(ordered[1:]), 6) if len(ordered) > 1 else round(sum(ordered), 6),
        "returns_without_top_3_r": round(sum(ordered[3:]), 6) if len(ordered) > 3 else 0.0,
        "top_positive_share": _top_positive_share(r_values, 3),
    }


def _cost_sensitivity(r_values: list[float]) -> list[dict[str, Any]]:
    rows = []
    for cost in (0.00, 0.05, 0.10, 0.15, 0.20):
        adjusted = [value - cost for value in r_values]
        rows.append(
            {
                "cost_r": cost,
                "expectancy_r": _expectancy(adjusted),
                "profit_factor": _profit_factor(adjusted),
            }
        )
    return rows


def _build_robustness_findings(
    *,
    best_result: dict[str, Any],
    perturbation_results: list[dict[str, Any]],
) -> dict[str, Any]:
    positive_results = [row for row in perturbation_results if row["summary"]["expectancy_r"] > 0.0]
    cost_positive_results = [row for row in perturbation_results if row["summary"]["cost_expectancy_r_010"] > 0.0]
    perturbation_spread = _avg([row["summary"]["expectancy_r"] for row in perturbation_results])
    parameter_sensitivity = _parameter_sensitivity(perturbation_results)
    return {
        "best_variant_summary": best_result["summary"],
        "local_perturbation_count": len(perturbation_results),
        "positive_expectancy_share": round(len(positive_results) / float(max(len(perturbation_results), 1)), 6),
        "positive_after_010_cost_share": round(len(cost_positive_results) / float(max(len(perturbation_results), 1)), 6),
        "median_perturbation_expectancy_r": _median([row["summary"]["expectancy_r"] for row in perturbation_results]),
        "average_perturbation_expectancy_r": perturbation_spread,
        "top_perturbations": [
            {
                "variant": row["variant"],
                "summary": row["summary"],
            }
            for row in perturbation_results[:8]
        ],
        "parameter_sensitivity": parameter_sensitivity,
    }


def _parameter_sensitivity(perturbation_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not perturbation_results:
        return []
    parameter_values: dict[str, dict[str, list[float]]] = {}
    for row in perturbation_results:
        params = row["variant"]["params"]
        expectancy = row["summary"]["expectancy_r"]
        for key, value in params.items():
            parameter_values.setdefault(key, {})
            bucket = f"{float(value):.4f}"
            parameter_values[key].setdefault(bucket, []).append(expectancy)
    rows = []
    for key, bucket_map in parameter_values.items():
        averaged = {bucket: _avg(values) for bucket, values in bucket_map.items()}
        span = max(averaged.values()) - min(averaged.values()) if averaged else 0.0
        threshold_sensitive = span > 0.03 or (min(averaged.values()) < 0.0 < max(averaged.values()) if averaged else False)
        rows.append(
            {
                "parameter": key,
                "average_expectancy_by_value": averaged,
                "expectancy_span_r": round(span, 6),
                "classification": "threshold_sensitive" if threshold_sensitive else "structural_support",
            }
        )
    rows.sort(key=lambda row: (-row["expectancy_span_r"], row["parameter"]))
    return rows


def _feature_dependency_diagnostics(
    *,
    trades: list[Phase2ATradeRecord],
    family: str,
    direction: str,
) -> dict[str, Any]:
    if family == "breakout_acceptance":
        feature_names = ["continuation_score_long", "compression_5", "compression_60", "breakout_up", "close_pos", "slope_60"]
    else:
        feature_names = ["reversal_score_short", "dist_240", "body_r", "close_pos", "slope_240", "vol_ratio_60_240"]
    rows = []
    for feature_name in feature_names:
        values = [
            float(trade.feature_snapshot[feature_name])
            for trade in trades
            if feature_name in trade.feature_snapshot
        ]
        outcomes = [
            trade.r_multiple
            for trade in trades
            if feature_name in trade.feature_snapshot
        ]
        if not values:
            continue
        rows.append(
            {
                "feature": feature_name,
                "correlation_to_r": _correlation(values, outcomes),
                "quartile_buckets": _feature_buckets(values, outcomes),
            }
        )
    return {
        "analyzed_features": rows,
        "smallest_feature_set_hypothesis": (
            ["continuation_score_long", "breakout_up", "close_pos", "compression_5"]
            if family == "breakout_acceptance"
            else ["reversal_score_short", "failed_breakout_short", "close_pos", "body_r"]
        ),
    }


def _feature_buckets(values: list[float], outcomes: list[float], bucket_count: int = 4) -> list[dict[str, Any]]:
    ordered = sorted(zip(values, outcomes, strict=False), key=lambda item: item[0])
    if not ordered:
        return []
    bucket_size = max(len(ordered) // bucket_count, 1)
    rows = []
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        if not bucket:
            continue
        bucket_values = [item[0] for item in bucket]
        bucket_outcomes = [item[1] for item in bucket]
        rows.append(
            {
                "value_min": round(bucket_values[0], 6),
                "value_max": round(bucket_values[-1], 6),
                "trade_count": len(bucket),
                "expectancy_r": _expectancy(bucket_outcomes),
            }
        )
    return rows


def _build_interpretability_findings(
    *,
    approximation_results: list[dict[str, Any]],
    best_result: dict[str, Any],
) -> dict[str, Any]:
    approximation_results.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    best_score = best_result["summary"]["phase2a_score"]
    best_expectancy = best_result["summary"]["expectancy_r"]
    best_breadth = best_result["summary"]["positive_symbol_share"]
    smallest = None
    for row in approximation_results:
        score = row["summary"]["phase2a_score"]
        expectancy = row["summary"]["expectancy_r"]
        breadth = row["summary"]["positive_symbol_share"]
        if score >= 0.80 * best_score and expectancy >= 0.75 * best_expectancy and breadth >= 0.75 * best_breadth:
            smallest = row
            break
    return {
        "approximation_variants": [
            {
                "variant": row["variant"],
                "summary": row["summary"],
            }
            for row in approximation_results
        ],
        "smallest_rule_retaining_most_edge": {
            "variant": smallest["variant"],
            "summary": smallest["summary"],
        }
        if smallest is not None
        else None,
        "structural_vs_threshold_sensitive": (
            "A smaller rule approximation retained most of the edge, so the candidate looks structurally real rather than purely threshold-fitted."
            if smallest is not None
            else "No smaller approximation retained enough of the edge, so this candidate still looks threshold-sensitive."
        ),
        "simplification_note": (
            "The next pass should simplify toward the smallest approximation and treat the removed conditions as secondary quality filters."
            if smallest is not None
            else "The next pass should keep the structure but simplify through score smoothing instead of removing conditions outright."
        ),
    }


def _candidate_recommendation(
    *,
    best_result: dict[str, Any],
    interpretability: dict[str, Any],
) -> dict[str, Any]:
    summary = best_result["summary"]
    positive_symbols = [row for row in summary["per_symbol"] if row["expectancy_r"] > 0.0]
    recommendation = "continue"
    reason = "The candidate remains viable enough to deserve another refinement pass."
    if summary["expectancy_r"] <= 0.0 or summary["positive_symbol_share"] < 0.25:
        recommendation = "discard"
        reason = "The candidate is still too weak after refinement to justify more work."
    elif len(positive_symbols) <= 3 or summary["concentration"]["top_3_share_of_total_r"] > 0.80:
        recommendation = "split by instrument/family"
        reason = "The edge looks too uneven across the universe, so it should be narrowed before more global optimization."
    elif interpretability["smallest_rule_retaining_most_edge"] is not None:
        recommendation = "simplify"
        reason = "A materially simpler rule held onto most of the edge, so simplification is the highest-value next step."
    return {
        "action": recommendation,
        "reason": reason,
    }


def _soft_comparison_note(soft_results: list[dict[str, Any]]) -> str:
    if len(soft_results) < 2:
        return "Insufficient variants to compare soft scoring."
    best = soft_results[0]
    baseline = next((row for row in soft_results if row["variant"]["label"] == "baseline"), soft_results[-1])
    delta = best["summary"]["expectancy_r"] - baseline["summary"]["expectancy_r"]
    if best["variant"]["label"] == "baseline":
        return "The baseline remained best; soft score filtering did not improve this candidate enough yet."
    if delta > 0.0:
        return f"The best soft-score variant improved expectancy by {round(delta, 4)}R versus the baseline."
    return f"The best soft-score variant did not exceed the baseline; delta={round(delta, 4)}R."


def _asymmetry_takeaway(
    *,
    primary: dict[str, Any],
    asymmetry_results: list[dict[str, Any]],
    primary_direction: str,
) -> str:
    if not asymmetry_results:
        return "No asymmetry comparison was available."
    asymmetry_results.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    best_counterpart = asymmetry_results[0]["summary"]
    delta = primary["expectancy_r"] - best_counterpart["expectancy_r"]
    side = "same-family opposite-direction"
    return (
        f"The {side} comparison favored the primary side by {round(delta, 4)}R of expectancy."
        if delta >= 0.0
        else f"The {side} comparison favored the opposite direction by {round(-delta, 4)}R of expectancy."
    )


def _approaching_shortlist_quality(summary: dict[str, Any]) -> bool:
    return bool(
        summary["expectancy_r"] >= 0.05
        and summary["positive_symbol_share"] >= 0.55
        and summary["walk_forward_positive_ratio"] >= 0.60
        and summary["cost_expectancy_r_010"] > 0.0
        and summary["concentration"]["top_3_share_of_total_r"] <= 0.75
    )


def _build_revised_ranking(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for target in targets:
        for row in target["soft_gating_results"]["variants"]:
            rows.append(
                {
                    "target_candidate_id": target["target_candidate_id"],
                    "variant": row["variant"],
                    "summary": row["summary"],
                }
            )
    rows.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    return rows[:12]


def _build_soft_gating_summary(targets: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for target in targets:
        variants = target["soft_gating_results"]["variants"]
        baseline = next((row for row in variants if row["variant"]["label"] == "baseline"), variants[0])
        best = max(variants, key=lambda row: row["summary"]["phase2a_score"])
        rows.append(
            {
                "target_candidate_id": target["target_candidate_id"],
                "baseline_variant": baseline["variant"]["variant_id"],
                "best_variant": best["variant"]["variant_id"],
                "baseline_expectancy_r": baseline["summary"]["expectancy_r"],
                "best_expectancy_r": best["summary"]["expectancy_r"],
                "expectancy_delta_r": round(best["summary"]["expectancy_r"] - baseline["summary"]["expectancy_r"], 6),
                "baseline_positive_symbol_share": baseline["summary"]["positive_symbol_share"],
                "best_positive_symbol_share": best["summary"]["positive_symbol_share"],
            }
        )
    return {
        "target_comparisons": rows,
        "conclusion": (
            "At least one leading candidate improved with soft score filtering."
            if any(row["expectancy_delta_r"] > 0.0 for row in rows)
            else "Soft score filtering did not beat the baselines in this refinement pass."
        ),
    }


def _build_overall_recommendation(targets: list[dict[str, Any]]) -> dict[str, Any]:
    approaching = [target for target in targets if target["approaching_promotion_shortlist_quality"]]
    if approaching:
        status = "continue_refinement"
        note = "One or more candidates are approaching shortlist quality, but more simplification and second-pass robustness work is still required."
    else:
        status = "continue_but_not_close"
        note = "The leading candidates are still research-worthy, but neither is close enough to justify a promotion conversation yet."
    return {
        "status": status,
        "note": note,
    }


def _select_target_best_result(soft_results: list[dict[str, Any]]) -> dict[str, Any]:
    if not soft_results:
        raise ValueError("soft_results must not be empty")
    baseline = next((row for row in soft_results if row["variant"]["label"] == "baseline"), soft_results[0])
    contenders = [row for row in soft_results if row is not baseline]
    if not contenders:
        return baseline
    contenders.sort(key=lambda row: (-row["summary"]["phase2a_score"], row["variant"]["variant_id"]))
    best_contender = contenders[0]
    baseline_summary = baseline["summary"]
    contender_summary = best_contender["summary"]
    improved = (
        contender_summary["expectancy_r"] > baseline_summary["expectancy_r"]
        and contender_summary["cost_expectancy_r_005"] >= baseline_summary["cost_expectancy_r_005"]
        and contender_summary["positive_symbol_share"] >= baseline_summary["positive_symbol_share"] - 0.05
    )
    return best_contender if improved else baseline


def _render_phase2a_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quant Futures Phase 2A",
        "",
        f"Execution timeframe: {report['execution_timeframe']}",
        f"Symbols tested: {', '.join(report['symbols_tested'])}",
        "",
        "## Revised Ranking",
    ]
    for row in report["revised_ranking"][:8]:
        summary = row["summary"]
        lines.append(
            f"- {row['variant']['variant_id']}: score={round(summary['phase2a_score'], 2)}, expectancy={round(summary['expectancy_r'], 4)}R, "
            f"positive_symbol_share={round(summary['positive_symbol_share'], 4)}, walk_forward={round(summary['walk_forward_positive_ratio'], 4)}, trades={summary['trade_count']}"
        )
    lines.extend(["", "## Soft Gating"])
    for row in report["soft_gating_results"]["target_comparisons"]:
        lines.append(
            f"- {row['target_candidate_id']}: baseline={row['baseline_variant']} ({round(row['baseline_expectancy_r'], 4)}R) "
            f"best={row['best_variant']} ({round(row['best_expectancy_r'], 4)}R) delta={round(row['expectancy_delta_r'], 4)}R"
        )
    lines.append(f"- Conclusion: {report['soft_gating_results']['conclusion']}")
    for target in report["targets"]:
        lines.extend(["", f"## {target['target_candidate_id']}"])
        best = target["revised_best_variant"]
        lines.append(
            f"- Best Phase 2A variant: {best['variant_id']} with expectancy={round(best['summary']['expectancy_r'], 4)}R, "
            f"positive_symbol_share={round(best['summary']['positive_symbol_share'], 4)}, walk_forward={round(best['summary']['walk_forward_positive_ratio'], 4)}"
        )
        lines.append(f"- Soft gating note: {target['soft_gating_results']['comparison_note']}")
        lines.append(
            f"- Robustness: positive perturbation share={round(target['robustness_findings']['positive_expectancy_share'], 4)}, "
            f"positive after 0.10R cost share={round(target['robustness_findings']['positive_after_010_cost_share'], 4)}"
        )
        lines.append(
            f"- Interpretability: {target['interpretability_findings']['structural_vs_threshold_sensitive']}"
        )
        smallest = target["interpretability_findings"]["smallest_rule_retaining_most_edge"]
        if smallest is not None:
            lines.append(
                f"- Simplest retained rule: {smallest['variant']['variant_id']} with expectancy={round(smallest['summary']['expectancy_r'], 4)}R"
            )
        lines.append(
            f"- Recommendation: {target['explicit_recommendation']['action']} because {target['explicit_recommendation']['reason']}"
        )
        lines.append(
            f"- Approaching promotion-shortlist quality: {target['approaching_promotion_shortlist_quality']}"
        )
    lines.extend(["", "## Overall Recommendation", f"- {report['overall_recommendation']['note']}"])
    return "\n".join(lines)


def _to_unit_interval(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _moderate_unit_interval(value: float, center: float, tolerance: float) -> float:
    if tolerance <= 0.0:
        return 0.0
    return max(0.0, 1.0 - abs(value - center) / tolerance)


def _correlation(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    mean_x = fmean(xs)
    mean_y = fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    denominator = denom_x * denom_y
    if denominator <= 0.0:
        return 0.0
    return round(numerator / denominator, 6)
