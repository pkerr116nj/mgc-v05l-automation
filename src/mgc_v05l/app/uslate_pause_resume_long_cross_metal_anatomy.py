"""Cross-metal anatomy pass for usLatePauseResumeLongTurn on MGC and GC."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
LATEST_JSON_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_cross_metal_anatomy.json"
LATEST_MD_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_cross_metal_anatomy.md"

FAMILY = "usLatePauseResumeLongTurn"

MGC_LEDGER_PATH = (
    REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv"
)
GC_LEDGER_PATH = (
    REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.trade_ledger.csv"
)
MGC_US_HOURS_REVIEW_PATH = REPORT_DIR / "us_hours_candidate_review.json"
GC_INCLUSION_READINESS_PATH = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.json"
GC_BEST_INCLUSION_REVIEW_PATH = REPORT_DIR / "best_inclusion_candidate_review.json"

NUMERIC_FIELDS = (
    "net_pnl",
    "mae",
    "mfe",
    "bars_held",
    "time_to_mfe",
    "time_to_mae",
    "mfe_capture_pct",
    "entry_efficiency_3",
    "entry_efficiency_5",
    "entry_efficiency_10",
    "initial_adverse_3bar",
    "initial_favorable_3bar",
    "entry_distance_fast_ema_atr",
    "entry_distance_slow_ema_atr",
    "entry_distance_vwap_atr",
)

SEPARATOR_FEATURES = (
    "entry_efficiency_5",
    "entry_efficiency_10",
    "initial_favorable_3bar",
    "initial_adverse_3bar",
    "mfe",
    "mae",
    "bars_held",
    "mfe_capture_pct",
    "entry_distance_fast_ema_atr",
    "entry_distance_slow_ema_atr",
    "entry_distance_vwap_atr",
)


@dataclass(frozen=True)
class InstrumentPaths:
    instrument: str
    ledger_path: Path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _float_or_none(values: list[float]) -> float | None:
    return mean(values) if values else None


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered[lower]
    weight = pos - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _parse_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["setup_family"] != FAMILY:
                continue
            parsed = dict(row)
            for field in NUMERIC_FIELDS:
                parsed[field] = float(parsed[field])
            entry_ts = datetime.fromisoformat(parsed["entry_ts"])
            parsed["entry_date"] = entry_ts.date().isoformat()
            parsed["entry_hour_et"] = entry_ts.hour + entry_ts.minute / 60.0
            if 14 <= parsed["entry_hour_et"] < 15:
                parsed["sub_pocket"] = "14:00-15:00 ET"
            elif 15 <= parsed["entry_hour_et"] < 16:
                parsed["sub_pocket"] = "15:00-16:00 ET"
            elif 16 <= parsed["entry_hour_et"] < 17:
                parsed["sub_pocket"] = "16:00-17:00 ET"
            else:
                parsed["sub_pocket"] = "Outside 14:00-17:00 ET"
            rows.append(parsed)
    rows.sort(key=lambda row: row["exit_ts"])
    return rows


def _economic_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [row["net_pnl"] for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [abs(pnl) for pnl in pnls if pnl < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    realized_pnl = sum(pnls)
    winners_sorted = sorted((pnl for pnl in pnls if pnl > 0), reverse=True)
    top_1 = winners_sorted[0] if winners_sorted else None
    top_3 = sum(winners_sorted[:3]) if winners_sorted else None
    median_loser = median(losses) if losses else None
    large_threshold = (median_loser or 0.0) * 3.0
    very_large_threshold = (median_loser or 0.0) * 5.0
    return {
        "sample_start": rows[0]["entry_ts"] if rows else None,
        "sample_end": rows[-1]["exit_ts"] if rows else None,
        "trades": len(rows),
        "realized_pnl": realized_pnl,
        "avg_trade": mean(pnls) if pnls else None,
        "median_trade": median(pnls) if pnls else None,
        "profit_factor": (gross_profit / gross_loss) if gross_loss else None,
        "max_drawdown": max_drawdown if pnls else None,
        "win_rate": (len(wins) / len(pnls)) if pnls else None,
        "average_loser": mean(losses) if losses else None,
        "median_loser": median_loser,
        "p95_loser": _quantile(losses, 0.95),
        "worst_loser": max(losses) if losses else None,
        "average_winner": mean(wins) if wins else None,
        "avg_winner_over_avg_loser": (
            (mean(wins) / mean(losses)) if wins and losses else None
        ),
        "top_1_contribution": ((top_1 / realized_pnl) * 100.0) if realized_pnl and top_1 is not None else None,
        "top_3_contribution": ((top_3 / realized_pnl) * 100.0) if realized_pnl and top_3 is not None else None,
        "survives_without_top_1": bool(realized_pnl and top_1 is not None and realized_pnl - top_1 > 0),
        "survives_without_top_3": bool(realized_pnl and top_3 is not None and realized_pnl - top_3 > 0),
        "large_winner_count": sum(1 for pnl in wins if pnl >= large_threshold) if large_threshold else 0,
        "very_large_winner_count": sum(1 for pnl in wins if pnl >= very_large_threshold) if very_large_threshold else 0,
    }


def _bucket_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    wins = sorted((row for row in rows if row["net_pnl"] > 0), key=lambda row: row["net_pnl"], reverse=True)
    losses = sorted((row for row in rows if row["net_pnl"] < 0), key=lambda row: row["net_pnl"])
    strongest_count = max(1, math.ceil(len(wins) * 0.25)) if wins else 0
    poison_count = max(1, math.ceil(len(losses) * 0.33)) if losses else 0
    mediocre_count = max(1, math.ceil(len(rows) * 0.25)) if rows else 0
    strongest_winners = wins[:strongest_count]
    normal_winners = wins[strongest_count:]
    fragile_losers = losses[:poison_count]
    clean_losers = losses[poison_count:]
    mediocre_trades = sorted(rows, key=lambda row: abs(row["net_pnl"]))[:mediocre_count]
    return {
        "strongest_winners": strongest_winners,
        "normal_winners": normal_winners,
        "mediocre_trades": mediocre_trades,
        "clean_losers": clean_losers,
        "fragile_losers": fragile_losers,
    }


def _bucket_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"trades": 0}
    exit_reasons = Counter(row["exit_reason"] for row in rows)
    return {
        "trades": len(rows),
        "realized_pnl": sum(row["net_pnl"] for row in rows),
        "median_trade": median(row["net_pnl"] for row in rows),
        "mean_mae": mean(row["mae"] for row in rows),
        "mean_mfe": mean(row["mfe"] for row in rows),
        "mean_bars_held": mean(row["bars_held"] for row in rows),
        "mean_initial_favorable_3bar": mean(row["initial_favorable_3bar"] for row in rows),
        "mean_initial_adverse_3bar": mean(row["initial_adverse_3bar"] for row in rows),
        "mean_entry_efficiency_5": mean(row["entry_efficiency_5"] for row in rows),
        "dominant_exit_reasons": dict(exit_reasons.most_common(3)),
        "sub_pocket_distribution": dict(Counter(row["sub_pocket"] for row in rows)),
    }


def _separator_summary(
    positive_group: list[dict[str, Any]], baseline_group: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not positive_group or not baseline_group:
        return []
    ranked: list[tuple[float, str, float, float]] = []
    for feature in SEPARATOR_FEATURES:
        positive_mean = mean(row[feature] for row in positive_group)
        baseline_mean = mean(row[feature] for row in baseline_group)
        positive_sd = pstdev(row[feature] for row in positive_group)
        baseline_sd = pstdev(row[feature] for row in baseline_group)
        pooled = math.sqrt((positive_sd * positive_sd + baseline_sd * baseline_sd) / 2.0) if (positive_sd or baseline_sd) else 1.0
        ranked.append((abs((positive_mean - baseline_mean) / pooled if pooled else 0.0), feature, positive_mean, baseline_mean))
    ranked.sort(reverse=True)
    return [
        {
            "feature": feature,
            "positive_group_mean": round(positive_mean, 4),
            "baseline_group_mean": round(baseline_mean, 4),
        }
        for _, feature, positive_mean, baseline_mean in ranked[:5]
    ]


def _top_trade_rows(rows: list[dict[str, Any]], winners: bool) -> list[dict[str, Any]]:
    filtered = [row for row in rows if (row["net_pnl"] > 0 if winners else row["net_pnl"] < 0)]
    ordered = sorted(filtered, key=lambda row: row["net_pnl"], reverse=winners)
    top = ordered[:3 if winners else 4]
    return [
        {
            "entry_ts": row["entry_ts"],
            "sub_pocket": row["sub_pocket"],
            "net_pnl": row["net_pnl"],
            "exit_reason": row["exit_reason"],
            "initial_favorable_3bar": row["initial_favorable_3bar"],
            "initial_adverse_3bar": row["initial_adverse_3bar"],
            "entry_efficiency_5": row["entry_efficiency_5"],
            "bars_held": row["bars_held"],
        }
        for row in top
    ]


def _sub_pocket_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: list[dict[str, Any]] = []
    for bucket in ("14:00-15:00 ET", "15:00-16:00 ET", "16:00-17:00 ET", "Outside 14:00-17:00 ET"):
        bucket_rows = [row for row in rows if row["sub_pocket"] == bucket]
        if not bucket_rows:
            continue
        bucket_pnls = [row["net_pnl"] for row in bucket_rows]
        buckets.append(
            {
                "sub_pocket": bucket,
                "trades": len(bucket_rows),
                "realized_pnl": sum(bucket_pnls),
                "median_trade": median(bucket_pnls),
            }
        )
    return buckets


def _overlap_dates(mgc_rows: list[dict[str, Any]], gc_rows: list[dict[str, Any]]) -> list[str]:
    mgc_top_dates = {row["entry_date"] for row in sorted((r for r in mgc_rows if r["net_pnl"] > 0), key=lambda r: r["net_pnl"], reverse=True)[:5]}
    gc_top_dates = {row["entry_date"] for row in sorted((r for r in gc_rows if r["net_pnl"] > 0), key=lambda r: r["net_pnl"], reverse=True)[:5]}
    return sorted(mgc_top_dates & gc_top_dates)


def _instrument_payload(instrument: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    buckets = _bucket_rows(rows)
    metrics = _economic_metrics(rows)
    wins = [row for row in rows if row["net_pnl"] > 0]
    losses = [row for row in rows if row["net_pnl"] < 0]
    concentration_source = (
        "weak_mediocre_trade_base_plus_shared_trend_day_winners"
        if (metrics["top_3_contribution"] or 0.0) > 100.0
        else "healthy_middle_plus_recurring_winners"
    )
    return {
        "economic_replay_quality": metrics,
        "sub_pocket_profile": _sub_pocket_summary(rows),
        "anatomy_buckets": {name: _bucket_summary(group) for name, group in buckets.items()},
        "winner_separators": _separator_summary(buckets["strongest_winners"], buckets["normal_winners"]),
        "loser_separators": _separator_summary(buckets["fragile_losers"], buckets["clean_losers"]),
        "top_winner_examples": _top_trade_rows(rows, winners=True),
        "poison_loser_examples": _top_trade_rows(rows, winners=False),
        "concentration_anatomy": {
            "top_1_contribution": metrics["top_1_contribution"],
            "top_3_contribution": metrics["top_3_contribution"],
            "survives_without_top_1": metrics["survives_without_top_1"],
            "survives_without_top_3": metrics["survives_without_top_3"],
            "likely_source": concentration_source,
            "plain_language": (
                "The best days run cleanly for seven bars and overwhelm a middle that is only slightly positive or slightly negative."
                if instrument == "MGC"
                else "The same standout trend days exist, but GC has a weaker middle and slightly harsher poison losers, so concentration bites harder."
            ),
        },
    }


def build_report() -> dict[str, Any]:
    mgc_rows = _parse_rows(MGC_LEDGER_PATH)
    gc_rows = _parse_rows(GC_LEDGER_PATH)
    mgc_payload = _instrument_payload("MGC", mgc_rows)
    gc_payload = _instrument_payload("GC", gc_rows)

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_scope": "research_only",
        "family": FAMILY,
        "instruments": ["MGC", "GC"],
        "source_artifacts": {
            "mgc_trade_ledger": str(MGC_LEDGER_PATH),
            "gc_trade_ledger": str(GC_LEDGER_PATH),
            "mgc_us_hours_candidate_review": str(MGC_US_HOURS_REVIEW_PATH),
            "gc_inclusion_readiness": str(GC_INCLUSION_READINESS_PATH),
            "gc_best_inclusion_review": str(GC_BEST_INCLUSION_REVIEW_PATH),
        },
        "replay_analysis_paths_used": {
            "MGC": {
                "trade_ledger": str(MGC_LEDGER_PATH),
                "validated_reference": str(MGC_US_HOURS_REVIEW_PATH),
            },
            "GC": {
                "trade_ledger": str(GC_LEDGER_PATH),
                "validated_reference": str(GC_INCLUSION_READINESS_PATH),
            },
        },
        "cross_metal_common_structure": {
            "shared_session_behavior": (
                "Both metals are overwhelmingly late-US long families. The meaningful action clusters in 14:00-16:00 ET, "
                "with the strongest wins coming from resumed continuation rather than open-drive behavior."
            ),
            "shared_trade_shape": (
                "The best winners in both metals are seven-bar holds with very strong 5-bar entry efficiency, "
                "large early favorable movement, and very limited initial adverse movement."
            ),
            "shared_likely_edge_source": (
                "Both lanes appear to monetize late-session pause-then-resume continuation days when the resumed push keeps extending cleanly."
            ),
            "shared_standout_winner_dates": _overlap_dates(mgc_rows, gc_rows),
        },
        "cross_metal_differences": {
            "gc_vs_mgc": (
                "GC is not just a random weaker copy. It expresses the same family on many of the same standout dates, "
                "but its mediocre middle is weaker and its poison-loser pocket is harsher, so concentration fragility is worse."
            ),
            "concentration_difference": (
                "MGC still depends on big winners, but its base is healthier and it survives without the top trade. "
                "GC fails without both the top trade and top three trades because its middle does not carry enough of the load."
            ),
            "sub_pocket_difference": (
                "MGC gets an extra tail contribution from a small outside-14:00-17:00 bucket, while GC is more purely inside the core US_LATE window."
            ),
        },
        "metals": {
            "MGC": mgc_payload,
            "GC": gc_payload,
        },
        "refinement_guidance": {
            "shared_or_instrument_specific": (
                "Start with a shared metals refinement hypothesis. The same winner shape and the same poison-loser signature show up in both ledgers."
            ),
            "best_likely_lever": (
                "Entry-quality cleanup and trap-state exclusion look stronger than trade-management changes. "
                "The winners already hold cleanly; the weak spot is the recurring pocket of low-efficiency, high-initial-adverse trades."
            ),
            "no_refinement_case": (
                "MGC is already strong enough to keep the raw family. GC may still deserve narrow cleanup later, "
                "but the anatomy does not argue for a broad redesign."
            ),
        },
        "verdict_bucket": "FAMILY_CENTER_OF_GRAVITY_CONFIRMED",
        "direct_answers": {
            "is_usLatePauseResumeLongTurn_now_clearly_the_center_of_gravity_family_for_the_metals_work": (
                "Yes. It is the clearest shared late-US metals family, and both MGC and GC express the same continuation core."
            ),
            "is_gc_best_understood_as_a_meaningful_second_lane_or_just_a_weaker_copy_of_mgc": (
                "Meaningful second lane. It is weaker than MGC, but it shares the same standout continuation days and failure modes rather than behaving like noise."
            ),
            "single_biggest_source_of_concentration_fragility": (
                "The middle of the trade distribution is too weak relative to a few standout trend days, especially once early trap losses are included."
            ),
            "if_we_refine_later_should_we_first_try_a_shared_metals_refinement_or_separate_mgc_gc_refinements": (
                "Start with a shared metals refinement hypothesis, then instrument-specific cleanup only if GC remains materially weaker after shared trap-state analysis."
            ),
        },
    }
    return report


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    if isinstance(value, bool):
        return str(value)
    return f"{float(value):.{digits}f}"


def render_markdown(report: dict[str, Any]) -> str:
    mgc = report["metals"]["MGC"]["economic_replay_quality"]
    gc = report["metals"]["GC"]["economic_replay_quality"]
    lines = [
        "# usLatePauseResumeLongTurn Cross-Metal Anatomy",
        "",
        "## Paths Used",
        f"- MGC replay ledger: `{report['replay_analysis_paths_used']['MGC']['trade_ledger']}`",
        f"- GC replay ledger: `{report['replay_analysis_paths_used']['GC']['trade_ledger']}`",
        f"- MGC validated reference: `{report['replay_analysis_paths_used']['MGC']['validated_reference']}`",
        f"- GC validated reference: `{report['replay_analysis_paths_used']['GC']['validated_reference']}`",
        "",
        "## Common Structure",
        f"- Shared session behavior: {report['cross_metal_common_structure']['shared_session_behavior']}",
        f"- Shared trade shape: {report['cross_metal_common_structure']['shared_trade_shape']}",
        f"- Shared likely edge source: {report['cross_metal_common_structure']['shared_likely_edge_source']}",
        f"- Shared standout winner dates: {', '.join(report['cross_metal_common_structure']['shared_standout_winner_dates']) or 'None'}",
        "",
        "## MGC vs GC",
        f"- MGC: trades `{mgc['trades']}`, pnl `{_fmt(mgc['realized_pnl'])}`, median `{_fmt(mgc['median_trade'])}`, PF `{_fmt(mgc['profit_factor'])}`, top-1 `{_fmt(mgc['top_1_contribution'])}%`, top-3 `{_fmt(mgc['top_3_contribution'])}%`",
        f"- GC: trades `{gc['trades']}`, pnl `{_fmt(gc['realized_pnl'])}`, median `{_fmt(gc['median_trade'])}`, PF `{_fmt(gc['profit_factor'])}`, top-1 `{_fmt(gc['top_1_contribution'])}%`, top-3 `{_fmt(gc['top_3_contribution'])}%`",
        f"- Difference read: {report['cross_metal_differences']['gc_vs_mgc']}",
        f"- Concentration difference: {report['cross_metal_differences']['concentration_difference']}",
        f"- Sub-pocket difference: {report['cross_metal_differences']['sub_pocket_difference']}",
        "",
        "## Concentration Anatomy",
        f"- MGC source: {report['metals']['MGC']['concentration_anatomy']['plain_language']}",
        f"- GC source: {report['metals']['GC']['concentration_anatomy']['plain_language']}",
        "",
        "## Refinement Guidance",
        f"- Shared vs instrument-specific: {report['refinement_guidance']['shared_or_instrument_specific']}",
        f"- Best likely lever: {report['refinement_guidance']['best_likely_lever']}",
        f"- Raw-family note: {report['refinement_guidance']['no_refinement_case']}",
        "",
        "## Verdict",
        f"- `{report['verdict_bucket']}`",
        "",
        "## Direct Answers",
        f"1. {report['direct_answers']['is_usLatePauseResumeLongTurn_now_clearly_the_center_of_gravity_family_for_the_metals_work']}",
        f"2. {report['direct_answers']['is_gc_best_understood_as_a_meaningful_second_lane_or_just_a_weaker_copy_of_mgc']}",
        f"3. {report['direct_answers']['single_biggest_source_of_concentration_fragility']}",
        f"4. {report['direct_answers']['if_we_refine_later_should_we_first_try_a_shared_metals_refinement_or_separate_mgc_gc_refinements']}",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(render_markdown(report), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the usLatePauseResumeLongTurn cross-metal anatomy report.")
    parser.parse_args()
    report = build_report()
    write_outputs(report)


if __name__ == "__main__":
    main()
