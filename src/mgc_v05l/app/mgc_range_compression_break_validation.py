"""Research-only validation pass for the MGC range_compression_break archetype."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from mgc_v05l.app.mgc_entry_archetype_discovery import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_RANDOM_CONTROL_COUNT,
    DEFAULT_SYMBOL,
    DiscoveryConfig,
    MinuteBar,
    _build_session_coverage_rows,
    _coerce_now,
    _combine_research_bars,
    _eligible_session_map_from_rows,
    _extract_pre_entry_features,
    _forward_outcomes,
    _future_window,
    _label_archetypes,
    _load_canonical_sqlite_bars,
    _load_research_bars,
    _read_json,
    _resolve_canonical_sqlite_path,
    _resolve_output_root,
    _session_key,
    _write_csv,
)
from mgc_v05l.research.trend_participation.storage import build_layout, write_storage_manifest

SOURCE = "MGC_RANGE_COMPRESSION_BREAK_VALIDATION_RESEARCH_ONLY"
ARCHETYPE = "range_compression_break"
TICK_SIZE_POINTS = 0.1
HORIZONS = (15, 30, 60)
COST_TICKS = (0, 1, 2, 3)
VARIANTS = (
    "base",
    "stronger_compression",
    "stronger_breakout_bar",
    "expansion_after_compression",
    "minimum_prior_range_percentile",
    "base_us_session",
)


@dataclass(frozen=True, slots=True)
class ValidationConfig:
    repo_root: Path = Path(__file__).resolve().parents[3]
    output_root: Path = DEFAULT_OUTPUT_ROOT
    symbol: str = DEFAULT_SYMBOL
    raw_bars_root: Path | None = None
    quality_audit_path: Path | None = None
    canonical_sqlite_path: Path | None = None
    random_control_count: int = DEFAULT_RANDOM_CONTROL_COUNT
    random_seed: int = 90210
    now: datetime | None = None


@dataclass(frozen=True, slots=True)
class ValidationHit:
    event_id: str
    variant: str
    panel: str
    decision_ts: datetime
    year: int
    session: str
    direction: str
    forward_return_15m: float
    forward_return_30m: float
    forward_return_60m: float
    mfe_15m: float
    mfe_30m: float
    mfe_60m: float
    mae_15m: float
    mae_30m: float
    mae_60m: float


def run_range_compression_break_validation(*, config: ValidationConfig) -> dict[str, Any]:
    now = _coerce_now(config.now)
    symbol = config.symbol.upper()
    output_root = _resolve_output_root(
        DiscoveryConfig(repo_root=config.repo_root, output_root=config.output_root, symbol=symbol)
    )
    raw_root = config.raw_bars_root or output_root / "raw_bars" / "databento_minute_backfill" / f"symbol={symbol}"
    quality_path = config.quality_audit_path or output_root / "reports" / f"latest_databento_research_minute_backfill_quality_audit_{symbol}.json"
    canonical_sqlite_path = _resolve_canonical_sqlite_path(
        DiscoveryConfig(repo_root=config.repo_root, canonical_sqlite_path=config.canonical_sqlite_path)
    )

    pre2020_bars = _load_research_bars(raw_root=raw_root, symbol=symbol)
    canonical_bars = _load_canonical_sqlite_bars(sqlite_path=canonical_sqlite_path, symbol=symbol)
    bars = _combine_research_bars(pre2020_bars=pre2020_bars, canonical_bars=canonical_bars)
    session_rows = _build_session_coverage_rows(bars)
    eligible_sessions = _eligible_session_map_from_rows(session_rows)

    hits = _scan_hits(
        bars=bars,
        eligible_sessions=eligible_sessions,
    )
    discovery_frequencies = _load_discovery_frequencies(output_root)

    metric_rows = _build_metric_rows(hits)
    threshold_rows = _build_threshold_rows(hits, discovery_frequencies)
    cohort_frequency_rows = _build_cohort_frequency_rows(discovery_frequencies)
    classification_rows = _build_classification_rows(hits, discovery_frequencies)

    layout = build_layout(output_root)
    report_dir = layout["reports"]
    manifest_dir = layout["manifests"]
    summary_json = report_dir / "mgc_range_compression_break_validation_summary.json"
    summary_md = report_dir / "mgc_range_compression_break_validation_summary.md"
    metrics_csv = report_dir / "mgc_range_compression_break_validation_metrics.csv"
    thresholds_csv = report_dir / "mgc_range_compression_break_validation_thresholds.csv"
    cohort_csv = report_dir / "mgc_range_compression_break_validation_cohort_frequency.csv"
    classification_csv = report_dir / "mgc_range_compression_break_validation_classification.csv"
    manifest_path = manifest_dir / "mgc_range_compression_break_validation_manifest.json"

    final_classification = _final_classification(classification_rows)
    summary = {
        "schema_version": "mgc_range_compression_break_validation_v1",
        "generated_at": now.isoformat(),
        "source": SOURCE,
        "symbol": symbol,
        "archetype": ARCHETYPE,
        "research_artifact": True,
        "runtime_artifact": False,
        "runtime_preflight_dashboard_truth": False,
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "data_sources_used": [
            {
                "source_id": "pre2020_databento_backfill",
                "path": str(raw_root),
                "bar_count": len(pre2020_bars),
                "first_bar": pre2020_bars[0].ts.isoformat() if pre2020_bars else None,
                "last_bar": pre2020_bars[-1].ts.isoformat() if pre2020_bars else None,
            },
            {
                "source_id": "canonical_2020plus",
                "path": str(canonical_sqlite_path),
                "bar_count": len(canonical_bars),
                "first_bar": canonical_bars[0].ts.isoformat() if canonical_bars else None,
                "last_bar": canonical_bars[-1].ts.isoformat() if canonical_bars else None,
            },
            {
                "source_id": "combined_full_dataset",
                "bar_count": len(bars),
                "first_bar": bars[0].ts.isoformat() if bars else None,
                "last_bar": bars[-1].ts.isoformat() if bars else None,
                "dedupe_rule": "symbol+timestamp, canonical_2020plus wins on overlap",
            },
        ],
        "session_quality": {
            "eligible_session_count": len(eligible_sessions),
            "total_session_rows": len(session_rows),
            "quality_audit_path": str(quality_path),
            "pre2020_quality_audit": {
                "eligible_count": int((_read_json(quality_path).get("session_coverage_summary") or {}).get("eligible_count") or 0)
                if quality_path.exists()
                else 0,
            },
        },
        "event_counts": {
            "range_compression_break_base_hits": sum(1 for hit in hits if hit.variant == "base" and hit.panel == "combined_full_dataset"),
        },
        "cohort_context": {
            "source": "mgc_entry_archetype_discovery_candidate_classification.csv",
            "note": "Base archetype adverse/control frequencies are reused from the completed full-dataset discovery run; stricter-threshold variants are evaluated on hit returns and occurrence breakdowns.",
        },
        "cost_model": {
            "tick_size_points": TICK_SIZE_POINTS,
            "cost_ticks": list(COST_TICKS),
            "interpretation": "Cost/slippage is modeled as a total round-trip points haircut of ticks * 0.1 on directional forward return.",
        },
        "artifact_paths": {
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_md),
            "metrics_csv": str(metrics_csv),
            "thresholds_csv": str(thresholds_csv),
            "cohort_frequency_csv": str(cohort_csv),
            "classification_csv": str(classification_csv),
            "manifest": str(manifest_path),
        },
        "classification_rows": classification_rows,
        "final_classification": final_classification,
    }

    _write_csv(metrics_csv, metric_rows)
    _write_csv(thresholds_csv, threshold_rows)
    _write_csv(cohort_csv, cohort_frequency_rows)
    _write_csv(classification_csv, classification_rows)
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    summary_md.write_text(_render_markdown(summary, classification_rows, metric_rows, threshold_rows), encoding="utf-8")
    write_storage_manifest(
        manifest_path,
        {
            "schema_version": "mgc_range_compression_break_validation_manifest_v1",
            "generated_at": summary["generated_at"],
            "source": SOURCE,
            "symbol": symbol,
            "archetype": ARCHETYPE,
            "artifact_paths": summary["artifact_paths"],
            "research_artifact": True,
            "runtime_artifact": False,
            "runtime_preflight_dashboard_truth": False,
            "can_submit": False,
            "paper_trade_allowed": False,
            "live_money_eligible": False,
            "final_classification": final_classification,
        },
    )
    return summary


def _scan_hits(
    *,
    bars: Sequence[MinuteBar],
    eligible_sessions: dict[tuple[str, str, str], dict[str, Any]],
) -> list[ValidationHit]:
    grouped: dict[tuple[str, str, str], list[MinuteBar]] = defaultdict(list)
    for bar in bars:
        key = _session_key(symbol=bar.symbol, bar_end=bar.ts)
        if key in eligible_sessions:
            grouped[key].append(bar)

    hits: list[ValidationHit] = []
    for key, session_bars in sorted(grouped.items()):
        session_bars.sort(key=lambda item: item.ts)
        for index, current in enumerate(session_bars):
            if index + 1 >= len(session_bars) or index + 1 < 60:
                continue
            if not _future_window(session_bars, index=index, horizon_minutes=60):
                continue
            prefilter = _range_compression_prefilter(session_bars=session_bars, index=index)
            if not prefilter["any_candidate"]:
                continue
            local = current.ts
            for direction in ("LONG", "SHORT"):
                if not prefilter[direction]:
                    continue
                features = _extract_pre_entry_features(session_bars=session_bars, index=index, direction=direction, lookbacks=(5, 15, 30, 60))
                archetypes = tuple(_label_archetypes(features=features, direction=direction)) or ("unclassified",)
                returns, mfes, maes = _forward_outcomes(session_bars=session_bars, index=index, direction=direction, horizons=HORIZONS)
                for variant in _variant_labels(features=features, session_bars=session_bars, index=index, session=key[2], direction=direction):
                    hits.extend(
                        _panel_hits(
                            event_id=f"{current.symbol}|{current.ts.isoformat()}|{direction}",
                            variant=variant,
                            source_panel=current.source_panel,
                            decision_ts=current.ts,
                            year=current.ts.year,
                            session=key[2],
                            direction=direction,
                            returns=returns,
                            mfes=mfes,
                            maes=maes,
                        )
                    )
    return hits


def _range_compression_prefilter(*, session_bars: Sequence[MinuteBar], index: int) -> dict[str, bool]:
    current = session_bars[index]
    prior15 = session_bars[index - 15 : index]
    prior30 = session_bars[index - 30 : index]
    window15 = session_bars[index - 14 : index + 1]
    window30 = session_bars[index - 29 : index + 1]
    if len(prior15) < 15 or len(prior30) < 30:
        return {"LONG": False, "SHORT": False, "any_candidate": False}
    ranges15 = [max(bar.high - bar.low, 0.0) for bar in window15]
    ranges30 = [max(bar.high - bar.low, 0.0) for bar in window30]
    compression15 = (statistics.fmean(ranges15[-5:]) / max(statistics.fmean(ranges15), 1e-9)) <= 0.72
    compression30 = (statistics.fmean(ranges30[-5:]) / max(statistics.fmean(ranges30), 1e-9)) <= 0.78
    if not (compression15 or compression30):
        return {"LONG": False, "SHORT": False, "any_candidate": False}
    long_break15 = current.close > max(bar.high for bar in prior15)
    long_break30 = current.close > max(bar.high for bar in prior30)
    short_break15 = current.close < min(bar.low for bar in prior15)
    short_break30 = current.close < min(bar.low for bar in prior30)
    long_candidate = (long_break30 and compression15) or (long_break15 and compression30)
    short_candidate = (short_break30 and compression15) or (short_break15 and compression30)
    return {"LONG": long_candidate, "SHORT": short_candidate, "any_candidate": long_candidate or short_candidate}


def _variant_labels(
    *,
    features: dict[str, Any],
    session_bars: Sequence[MinuteBar],
    index: int,
    session: str,
    direction: str,
) -> list[str]:
    labels = _label_archetypes(features=features, direction=direction)
    if ARCHETYPE not in labels:
        return []
    current = session_bars[index]
    current_range = max(current.high - current.low, 0.0)
    prior_ranges = [max(bar.high - bar.low, 0.0) for bar in session_bars[index - 60 : index]]
    range_percentile = _percentile_rank(prior_ranges, current_range)
    variants = ["base"]
    if float(features.get("range_contraction_ratio_15m") or 1.0) <= 0.60 or float(features.get("range_contraction_ratio_30m") or 1.0) <= 0.68:
        variants.append("stronger_compression")
    if float(features.get("directional_close_location") or 0.0) >= 0.75 and float(features.get("current_body_to_range") or 0.0) >= 0.55:
        variants.append("stronger_breakout_bar")
    if current_range > float(features.get("avg_range_15m") or 0.0) * 1.15:
        variants.append("expansion_after_compression")
    if range_percentile >= 0.60:
        variants.append("minimum_prior_range_percentile")
    if session == "US":
        variants.append("base_us_session")
    return variants


def _panel_hits(
    *,
    event_id: str,
    variant: str,
    source_panel: str,
    decision_ts: datetime,
    year: int,
    session: str,
    direction: str,
    returns: dict[int, float | None],
    mfes: dict[int, float],
    maes: dict[int, float],
) -> list[ValidationHit]:
    panels = ["combined_full_dataset", source_panel]
    return [
        ValidationHit(
            event_id=event_id,
            variant=variant,
            panel=panel,
            decision_ts=decision_ts,
            year=year,
            session=session,
            direction=direction,
            forward_return_15m=float(returns.get(15) or 0.0),
            forward_return_30m=float(returns.get(30) or 0.0),
            forward_return_60m=float(returns.get(60) or 0.0),
            mfe_15m=float(mfes.get(15) or 0.0),
            mfe_30m=float(mfes.get(30) or 0.0),
            mfe_60m=float(mfes.get(60) or 0.0),
            mae_15m=float(maes.get(15) or 0.0),
            mae_30m=float(maes.get(30) or 0.0),
            mae_60m=float(maes.get(60) or 0.0),
        )
        for panel in panels
    ]


def _build_metric_rows(hits: Sequence[ValidationHit]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dimensions = [
        ("panel", lambda hit: hit.panel),
        ("year", lambda hit: str(hit.year)),
        ("session", lambda hit: hit.session),
        ("direction", lambda hit: hit.direction),
    ]
    for variant in VARIANTS:
        variant_hits = [hit for hit in hits if hit.variant == variant]
        for horizon in HORIZONS:
            for cost_ticks in COST_TICKS:
                rows.append(_metric_row(variant=variant, dimension="ALL", bucket="ALL", horizon=horizon, cost_ticks=cost_ticks, hits=variant_hits))
            for dimension, key_fn in dimensions:
                grouped: dict[str, list[ValidationHit]] = defaultdict(list)
                for hit in variant_hits:
                    grouped[key_fn(hit)].append(hit)
                for bucket, bucket_hits in sorted(grouped.items()):
                    rows.append(_metric_row(variant=variant, dimension=dimension, bucket=bucket, horizon=horizon, cost_ticks=0, hits=bucket_hits))
    return rows


def _build_threshold_rows(hits: Sequence[ValidationHit], frequencies: dict[tuple[str, str], float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in VARIANTS:
        combined = [hit for hit in hits if hit.variant == variant and hit.panel == "combined_full_dataset"]
        row = _metric_row(variant=variant, dimension="variant", bucket=variant, horizon=60, cost_ticks=0, hits=combined)
        row.update(
            {
                "favorable_top1000_frequency": frequencies.get((variant, "favorable_top1000"), 0.0),
                "adverse_bottom1000_frequency": frequencies.get((variant, "adverse_bottom1000"), 0.0),
                "control_frequency": frequencies.get((variant, "random_control"), 0.0),
                "control_too_similar": _control_too_similar(
                    favorable=frequencies.get((variant, "favorable_top1000"), 0.0),
                    control=frequencies.get((variant, "random_control"), 0.0),
                ),
            }
        )
        rows.append(row)
    return rows


def _build_cohort_frequency_rows(frequency: dict[tuple[str, str], float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in VARIANTS:
        for cohort_name in ("favorable_top500", "favorable_top1000", "favorable_top2000", "adverse_bottom500", "adverse_bottom1000", "adverse_bottom2000", "random_control"):
            rows.append(
                {
                    "variant": variant,
                    "cohort": cohort_name,
                    "cohort_event_count": "",
                    "match_count": "",
                    "frequency": frequency.get((variant, cohort_name), 0.0),
                    "frequency_source": "full_discovery_base_archetype" if variant == "base" else "not_recomputed_for_threshold_variant",
                }
            )
    return rows


def _build_classification_rows(hits: Sequence[ValidationHit], frequencies: dict[tuple[str, str], float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant in VARIANTS:
        combined = [hit for hit in hits if hit.variant == variant and hit.panel == "combined_full_dataset"]
        zero_cost = _summarize_returns(combined, horizon=60, cost_ticks=0)
        three_cost = _summarize_returns(combined, horizon=60, cost_ticks=3)
        fav = frequencies.get((variant, "favorable_top1000"), 0.0)
        adv = frequencies.get((variant, "adverse_bottom1000"), 0.0)
        ctrl = frequencies.get((variant, "random_control"), 0.0)
        classification = _classify_variant(
            occurrence_count=len(combined),
            zero_cost=zero_cost,
            three_cost=three_cost,
            favorable_frequency=fav,
            adverse_frequency=adv,
            control_frequency=ctrl,
        )
        rows.append(
            {
                "variant": variant,
                "classification": classification,
                "occurrence_count": len(combined),
                "avg_return_60m_0_ticks": zero_cost["average_return"],
                "avg_return_60m_3_ticks": three_cost["average_return"],
                "profit_factor_0_ticks": zero_cost["profit_factor"],
                "profit_factor_3_ticks": three_cost["profit_factor"],
                "win_rate_0_ticks": zero_cost["win_rate"],
                "win_rate_3_ticks": three_cost["win_rate"],
                "top3_positive_return_share": zero_cost["top3_positive_return_share"],
                "favorable_top1000_frequency": fav,
                "adverse_bottom1000_frequency": adv,
                "control_frequency": ctrl,
                "control_too_similar": _control_too_similar(favorable=fav, control=ctrl),
            }
        )
    return rows


def _metric_row(
    *,
    variant: str,
    dimension: str,
    bucket: str,
    horizon: int,
    cost_ticks: int,
    hits: Sequence[ValidationHit],
) -> dict[str, Any]:
    summary = _summarize_returns(hits, horizon=horizon, cost_ticks=cost_ticks)
    return {
        "variant": variant,
        "dimension": dimension,
        "bucket": bucket,
        "horizon_minutes": horizon,
        "cost_ticks": cost_ticks,
        **summary,
    }


def _summarize_returns(hits: Sequence[ValidationHit], *, horizon: int, cost_ticks: int) -> dict[str, Any]:
    cost_points = cost_ticks * TICK_SIZE_POINTS
    returns = [_return_for_horizon(hit, horizon) - cost_points for hit in hits]
    mfes = [_mfe_for_horizon(hit, horizon) for hit in hits]
    maes = [_mae_for_horizon(hit, horizon) for hit in hits]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value < 0.0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    top3_share = (sum(sorted(wins, reverse=True)[:3]) / gross_profit) if gross_profit > 0 else 0.0
    return {
        "occurrence_count": len(hits),
        "average_return": round(statistics.fmean(returns), 6) if returns else 0.0,
        "median_return": round(statistics.median(returns), 6) if returns else 0.0,
        "win_rate": round(len(wins) / max(len(returns), 1), 6),
        "profit_factor": round(gross_profit / gross_loss, 6) if gross_loss > 0 else (round(gross_profit, 6) if gross_profit > 0 else 0.0),
        "max_drawdown": round(_max_drawdown(returns), 6),
        "average_mfe": round(statistics.fmean(mfes), 6) if mfes else 0.0,
        "average_mae": round(statistics.fmean(maes), 6) if maes else 0.0,
        "max_mfe": round(max(mfes), 6) if mfes else 0.0,
        "max_mae": round(max(maes), 6) if maes else 0.0,
        "top3_positive_return_share": round(top3_share, 6),
    }


def _load_discovery_frequencies(output_root: Path) -> dict[tuple[str, str], float]:
    path = output_root / "reports" / "mgc_entry_archetype_discovery_candidate_classification.csv"
    frequencies: dict[tuple[str, str], float] = {}
    if not path.exists():
        return frequencies
    import csv

    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("archetype") != ARCHETYPE:
                continue
            frequencies[("base", "favorable_top500")] = _float(row.get("top500_frequency"))
            frequencies[("base", "favorable_top1000")] = _float(row.get("top1000_frequency"))
            frequencies[("base", "favorable_top2000")] = _float(row.get("top2000_frequency"))
            frequencies[("base", "adverse_bottom1000")] = _float(row.get("adverse1000_frequency"))
            frequencies[("base", "random_control")] = _float(row.get("control_frequency"))
    return frequencies


def _classify_variant(
    *,
    occurrence_count: int,
    zero_cost: dict[str, Any],
    three_cost: dict[str, Any],
    favorable_frequency: float,
    adverse_frequency: float,
    control_frequency: float,
) -> str:
    if occurrence_count < 100:
        return "WEAK_BUT_RETAINED"
    if float(zero_cost["top3_positive_return_share"]) > 0.25:
        return "OUTLIER_DEPENDENT"
    if _control_too_similar(favorable=favorable_frequency, control=control_frequency):
        return "CONTROL_TOO_SIMILAR"
    if float(three_cost["average_return"]) <= 0.0 or float(three_cost["profit_factor"]) < 1.0:
        return "FAILS_AFTER_COSTS"
    if (
        float(zero_cost["average_return"]) > 0.0
        and float(three_cost["average_return"]) > 0.0
        and favorable_frequency > adverse_frequency
        and favorable_frequency > control_frequency
    ):
        return "VALIDATION_CANDIDATE"
    return "WEAK_BUT_RETAINED"


def _final_classification(classification_rows: Sequence[dict[str, Any]]) -> str:
    base = next((row for row in classification_rows if row["variant"] == "base"), None)
    if not base:
        return "WEAK_BUT_RETAINED"
    return str(base["classification"])


def _control_too_similar(*, favorable: float, control: float) -> bool:
    if favorable <= 0:
        return control > 0
    return control >= favorable * 0.80


def _return_for_horizon(hit: ValidationHit, horizon: int) -> float:
    if horizon == 15:
        return hit.forward_return_15m
    if horizon == 30:
        return hit.forward_return_30m
    return hit.forward_return_60m


def _mfe_for_horizon(hit: ValidationHit, horizon: int) -> float:
    if horizon == 15:
        return hit.mfe_15m
    if horizon == 30:
        return hit.mfe_30m
    return hit.mfe_60m


def _mae_for_horizon(hit: ValidationHit, horizon: int) -> float:
    if horizon == 15:
        return hit.mae_15m
    if horizon == 30:
        return hit.mae_30m
    return hit.mae_60m


def _max_drawdown(returns: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _percentile_rank(values: Sequence[float], value: float) -> float:
    if not values:
        return 0.0
    return sum(1 for item in values if item <= value) / len(values)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _render_markdown(
    summary: dict[str, Any],
    classification_rows: Sequence[dict[str, Any]],
    metric_rows: Sequence[dict[str, Any]],
    threshold_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        "# MGC Range Compression Break Validation",
        "",
        f"- final_classification: `{summary['final_classification']}`",
        f"- live_money_eligible: `{summary['live_money_eligible']}`",
        f"- base_hits: `{summary['event_counts']['range_compression_break_base_hits']}`",
        "",
        "## Classification",
        "",
        "| variant | classification | hits | avg 60m 0t | avg 60m 3t | PF 0t | PF 3t | fav1000 | adverse1000 | control |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in classification_rows:
        lines.append(
            f"| {row['variant']} | {row['classification']} | {row['occurrence_count']} | {row['avg_return_60m_0_ticks']} | {row['avg_return_60m_3_ticks']} | {row['profit_factor_0_ticks']} | {row['profit_factor_3_ticks']} | {row['favorable_top1000_frequency']} | {row['adverse_bottom1000_frequency']} | {row['control_frequency']} |"
        )
    lines.extend(["", "## Base By Panel Session Direction Horizon", "", "| bucket | horizon | count | avg | median | win | PF | MFE | MAE |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for row in metric_rows:
        if row["variant"] == "base" and row["cost_ticks"] == 0 and row["dimension"] in {"panel", "session", "direction"}:
            lines.append(
                f"| {row['dimension']}={row['bucket']} | {row['horizon_minutes']} | {row['occurrence_count']} | {row['average_return']} | {row['median_return']} | {row['win_rate']} | {row['profit_factor']} | {row['average_mfe']} | {row['average_mae']} |"
            )
    lines.extend(["", "## Threshold Variants", "", "| variant | count | avg 60m | PF | control too similar |", "|---|---:|---:|---:|---|"])
    for row in threshold_rows:
        lines.append(
            f"| {row['variant']} | {row['occurrence_count']} | {row['average_return']} | {row['profit_factor']} | {row['control_too_similar']} |"
        )
    return "\n".join(lines) + "\n"


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--raw-bars-root", type=Path, default=None)
    parser.add_argument("--quality-audit-path", type=Path, default=None)
    parser.add_argument("--canonical-sqlite-path", type=Path, default=None)
    parser.add_argument("--random-control-count", type=int, default=DEFAULT_RANDOM_CONTROL_COUNT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_range_compression_break_validation(
        config=ValidationConfig(
            output_root=args.output_root,
            symbol=args.symbol,
            raw_bars_root=args.raw_bars_root,
            quality_audit_path=args.quality_audit_path,
            canonical_sqlite_path=args.canonical_sqlite_path,
            random_control_count=args.random_control_count,
        )
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=_json_ready))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
