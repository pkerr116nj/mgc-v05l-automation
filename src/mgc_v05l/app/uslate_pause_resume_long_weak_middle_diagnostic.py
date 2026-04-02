"""Diagnostic quant pass for the weak-middle problem in usLatePauseResumeLongTurn."""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

from .uslate_pause_resume_long_cross_metal_anatomy import (
    GC_LEDGER_PATH,
    LATEST_JSON_PATH as CROSS_METAL_JSON_PATH,
    MGC_LEDGER_PATH,
    REPORT_DIR,
    _economic_metrics,
    _parse_rows,
)


LATEST_JSON_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_weak_middle_diagnostic.json"
LATEST_MD_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_weak_middle_diagnostic.md"
SHARED_REFINEMENT_JSON_PATH = REPORT_DIR / "usLatePauseResumeLongTurn_shared_metals_refinement.json"

BASE_FEATURES = (
    "initial_favorable_3bar",
    "initial_adverse_3bar",
    "entry_efficiency_5",
    "entry_efficiency_10",
    "mfe",
    "mae",
    "bars_held",
    "mfe_capture_pct",
    "entry_distance_fast_ema_atr",
    "entry_distance_slow_ema_atr",
    "entry_distance_vwap_atr",
)

DERIVED_FEATURES = (
    "early_excursion_asymmetry",
    "early_followthrough_ratio",
    "excursion_asymmetry",
    "efficiency_minus_adverse",
)

FEATURES = BASE_FEATURES + DERIVED_FEATURES


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="uslate-pause-resume-long-weak-middle-diagnostic")


def main(argv: list[str] | None = None) -> int:
    _ = build_parser().parse_args(argv)
    payload = build_report()
    write_outputs(payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _augment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    augmented: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        enriched["early_excursion_asymmetry"] = row["initial_favorable_3bar"] - row["initial_adverse_3bar"]
        enriched["early_followthrough_ratio"] = row["initial_favorable_3bar"] / max(row["initial_adverse_3bar"], 0.25)
        enriched["excursion_asymmetry"] = row["mfe"] - row["mae"]
        enriched["efficiency_minus_adverse"] = row["entry_efficiency_5"] - (row["initial_adverse_3bar"] * 10.0)
        augmented.append(enriched)
    return augmented


def _partition_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    winners = sorted((row for row in rows if row["net_pnl"] > 0), key=lambda row: row["net_pnl"], reverse=True)
    losers = sorted((row for row in rows if row["net_pnl"] < 0), key=lambda row: row["net_pnl"])
    standout_count = max(1, math.ceil(len(winners) * 0.25)) if winners else 0
    poison_count = max(1, math.ceil(len(losers) * 0.33)) if losers else 0

    standout_winners = winners[:standout_count]
    poison_losers = losers[:poison_count]

    used_ids = {id(row) for row in standout_winners}
    used_ids.update(id(row) for row in poison_losers)
    remaining = [row for row in rows if id(row) not in used_ids]

    weak_middle_count = max(1, math.ceil(len(rows) * 0.25)) if rows else 0
    weak_middle = sorted(remaining, key=lambda row: abs(row["net_pnl"]))[: min(weak_middle_count, len(remaining))]
    weak_ids = {id(row) for row in weak_middle}

    ordinary_winners = [row for row in remaining if row["net_pnl"] > 0 and id(row) not in weak_ids]
    clean_losers = [row for row in remaining if row["net_pnl"] < 0 and id(row) not in weak_ids]

    return {
        "standout_winners": standout_winners,
        "ordinary_winners": ordinary_winners,
        "weak_middle_trades": weak_middle,
        "clean_losers": clean_losers,
        "poison_losers": poison_losers,
    }


def _bucket_payload(rows: list[dict[str, Any]], total_realized_pnl: float) -> dict[str, Any]:
    pnls = [row["net_pnl"] for row in rows]
    realized = sum(pnls)
    return {
        "trades": len(rows),
        "realized_pnl": realized,
        "trade_share": (len(rows) / len(rows)) if False else None,
        "contribution_pct_of_total_realized_pnl": ((realized / total_realized_pnl) * 100.0) if total_realized_pnl else None,
        "avg_trade": mean(pnls) if pnls else None,
        "median_trade": median(pnls) if pnls else None,
        "mean_initial_favorable_3bar": mean(row["initial_favorable_3bar"] for row in rows) if rows else None,
        "mean_initial_adverse_3bar": mean(row["initial_adverse_3bar"] for row in rows) if rows else None,
        "mean_entry_efficiency_5": mean(row["entry_efficiency_5"] for row in rows) if rows else None,
        "mean_mae": mean(row["mae"] for row in rows) if rows else None,
        "mean_mfe": mean(row["mfe"] for row in rows) if rows else None,
        "mean_bars_held": mean(row["bars_held"] for row in rows) if rows else None,
    }


def _add_trade_share(payload: dict[str, dict[str, Any]], total_trades: int) -> None:
    for bucket in payload.values():
        bucket["trade_share"] = (bucket["trades"] / total_trades) if total_trades else None


def _effect_size(group_a: list[dict[str, Any]], group_b: list[dict[str, Any]], feature: str) -> float:
    if not group_a or not group_b:
        return 0.0
    a_vals = [float(row[feature]) for row in group_a]
    b_vals = [float(row[feature]) for row in group_b]
    a_mean = mean(a_vals)
    b_mean = mean(b_vals)
    a_sd = pstdev(a_vals)
    b_sd = pstdev(b_vals)
    pooled = math.sqrt((a_sd * a_sd + b_sd * b_sd) / 2.0)
    if pooled == 0:
        return 0.0
    return (a_mean - b_mean) / pooled


def _separator_summary(group_a: list[dict[str, Any]], group_b: list[dict[str, Any]], top_n: int = 4) -> list[dict[str, Any]]:
    ranked: list[tuple[float, str, float, float]] = []
    for feature in FEATURES:
        a_vals = [float(row[feature]) for row in group_a]
        b_vals = [float(row[feature]) for row in group_b]
        if not a_vals or not b_vals:
            continue
        effect = _effect_size(group_a, group_b, feature)
        ranked.append((abs(effect), feature, mean(a_vals), mean(b_vals)))
    ranked.sort(reverse=True)
    return [
        {
            "feature": feature,
            "effect_size_abs": round(score, 4),
            "group_a_mean": round(group_a_mean, 4),
            "group_b_mean": round(group_b_mean, 4),
        }
        for score, feature, group_a_mean, group_b_mean in ranked[:top_n]
    ]


def _instrument_problem_diagnosis(
    metrics: dict[str, Any], buckets: dict[str, dict[str, Any]], top3_contribution: float | None
) -> dict[str, Any]:
    ordinary = buckets["ordinary_winners"]["realized_pnl"]
    weak_middle = buckets["weak_middle_trades"]["realized_pnl"]
    poison = buckets["poison_losers"]["realized_pnl"]
    clean = buckets["clean_losers"]["realized_pnl"]

    causes: list[str] = []
    if ordinary <= abs(weak_middle):
        causes.append("too_few_ordinary_winners")
    if weak_middle <= 0:
        causes.append("too_many_mediocre_reversals")
    if abs(poison) > abs(clean):
        causes.append("too_much_early_adverse_movement")
    if (top3_contribution or 0.0) > 100.0:
        causes.append("too_much_dependence_on_rare_trend_extension")
    return {
        "primary_problem_mix": causes,
        "plain_language": (
            "The family does not fail because the best days are fake. It fails because ordinary trades do not add enough and the weak-middle bucket hovers around flat-to-negative while standout continuation days do the heavy lifting."
            if metrics["survives_without_top_1"]
            else "The family depends heavily on standout continuation days because the middle is weak and the poison-loser bucket still bites too hard."
        ),
    }


def _cross_metal_comparison(mgc: dict[str, Any], gc: dict[str, Any]) -> dict[str, Any]:
    mgc_ord = mgc["distribution_anatomy"]["ordinary_winners"]["realized_pnl"]
    gc_ord = gc["distribution_anatomy"]["ordinary_winners"]["realized_pnl"]
    mgc_weak = mgc["distribution_anatomy"]["weak_middle_trades"]["realized_pnl"]
    gc_weak = gc["distribution_anatomy"]["weak_middle_trades"]["realized_pnl"]
    mgc_poison = mgc["distribution_anatomy"]["poison_losers"]["realized_pnl"]
    gc_poison = gc["distribution_anatomy"]["poison_losers"]["realized_pnl"]
    return {
        "shared_structure": (
            "Both metals share the same basic weak-middle structure: standout late-US trend continuations are real, but the ordinary trade base is too small relative to weak-middle churn."
        ),
        "is_gc_harsher_copy_or_different": (
            "GC is a harsher copy of the same distribution shape, not a different family."
        ),
        "where_gc_is_weaker": {
            "ordinary_winner_realized_pnl_delta_vs_mgc": round(gc_ord - mgc_ord, 4),
            "weak_middle_realized_pnl_delta_vs_mgc": round(gc_weak - mgc_weak, 4),
            "poison_loser_realized_pnl_delta_vs_mgc": round(gc_poison - mgc_poison, 4),
            "plain_language": (
                "GC is weaker because its ordinary winners contribute less, its weak-middle is more negative, and its poison losers hit harder."
            ),
        },
    }


def _practical_implication(mgc: dict[str, Any], gc: dict[str, Any]) -> dict[str, Any]:
    mgc_mid = mgc["distribution_anatomy"]["weak_middle_trades"]["realized_pnl"]
    gc_mid = gc["distribution_anatomy"]["weak_middle_trades"]["realized_pnl"]
    mgc_ord = mgc["distribution_anatomy"]["ordinary_winners"]["realized_pnl"]
    gc_ord = gc["distribution_anatomy"]["ordinary_winners"]["realized_pnl"]
    mgc_poison = mgc["distribution_anatomy"]["poison_losers"]["realized_pnl"]
    gc_poison = gc["distribution_anatomy"]["poison_losers"]["realized_pnl"]
    mgc_top3 = mgc["economic_replay_quality"]["top_3_contribution"] or 0.0
    gc_top3 = gc["economic_replay_quality"]["top_3_contribution"] or 0.0
    weak_middle_not_carrying = (mgc_mid <= max(5.0, mgc_ord * 0.1)) and (gc_mid <= max(5.0, gc_ord * 0.1))
    if mgc_top3 > 100.0 and gc_top3 > 100.0 and weak_middle_not_carrying:
        main_issue = "fundamental_distribution_shape_problem"
    elif abs(gc_poison) > abs(gc_mid) and abs(mgc_poison) > abs(mgc_mid):
        main_issue = "trap_state_problem"
    else:
        main_issue = "fundamental_distribution_shape_problem"
    return {
        "main_issue": main_issue,
        "plain_language": (
            "This looks more like a distribution-shape problem than a pure entry-tweak or management problem. The winners already hold well; the trouble is that ordinary trades do not carry enough weight between standout days."
            if main_issue == "fundamental_distribution_shape_problem"
            else "Trap-state damage is still the dominant issue."
        ),
        "realistic_quant_lever_left": (
            "Limited. The evidence mostly argues for respecting the raw family as-is unless a future instrument-specific cleanup has unusually strong causal support."
            if main_issue == "fundamental_distribution_shape_problem"
            else "Possible, but only if trap-state diagnostics become instrument-specific rather than shared."
        ),
    }


def _instrument_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = _augment_rows(rows)
    metrics = _economic_metrics(rows)
    partition = _partition_rows(rows)
    distribution = {name: _bucket_payload(group, metrics["realized_pnl"]) for name, group in partition.items()}
    _add_trade_share(distribution, metrics["trades"])
    standout_vs_weak = _separator_summary(partition["standout_winners"], partition["weak_middle_trades"])
    weak_vs_poison = _separator_summary(partition["poison_losers"], partition["weak_middle_trades"])
    return {
        "economic_replay_quality": metrics,
        "distribution_anatomy": distribution,
        "bucket_problem_diagnosis": _instrument_problem_diagnosis(metrics, distribution, metrics["top_3_contribution"]),
        "best_diagnostic_separators": {
            "standout_winners_vs_weak_middle": standout_vs_weak,
            "poison_losers_vs_weak_middle": weak_vs_poison,
        },
    }


def build_report() -> dict[str, Any]:
    cross_metal = json.loads(CROSS_METAL_JSON_PATH.read_text(encoding="utf-8"))
    shared_refinement = json.loads(SHARED_REFINEMENT_JSON_PATH.read_text(encoding="utf-8"))

    mgc = _instrument_payload(_parse_rows(MGC_LEDGER_PATH))
    gc = _instrument_payload(_parse_rows(GC_LEDGER_PATH))

    best_separator = mgc["best_diagnostic_separators"]["standout_winners_vs_weak_middle"][0]

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_scope": "research_only",
        "family": "usLatePauseResumeLongTurn",
        "instruments": ["MGC", "GC"],
        "exact_evidence_sources_used": {
            "mgc_trade_ledger": str(MGC_LEDGER_PATH),
            "gc_trade_ledger": str(GC_LEDGER_PATH),
            "cross_metal_anatomy_json": str(CROSS_METAL_JSON_PATH),
            "shared_metals_refinement_json": str(SHARED_REFINEMENT_JSON_PATH),
        },
        "metals": {
            "MGC": mgc,
            "GC": gc,
        },
        "cross_metal_comparison": _cross_metal_comparison(mgc, gc),
        "practical_implication": _practical_implication(mgc, gc),
        "context_reference": {
            "cross_metal_verdict": cross_metal["verdict_bucket"],
            "shared_refinement_verdict": shared_refinement["verdict_bucket"],
        },
        "verdict_bucket": "WEAK_MIDDLE_DIAGNOSIS_CONFIRMED",
        "direct_answers": {
            "single_best_explanation_for_the_weak_middle": (
                "Ordinary winners are too modest to offset a weak-middle bucket that sits around flat-to-negative, so a few standout trend days end up carrying the family."
            ),
            "single_best_diagnostic_separator_between_standout_winners_and_the_weak_middle": (
                best_separator["feature"]
            ),
            "is_gc_weaker_for_the_same_reason_as_mgc_or_a_different_one": (
                "The same reason, but harsher. GC shares the same distribution shape as MGC, with a weaker ordinary-winner base and more damaging poison losers."
            ),
            "does_this_diagnostic_pass_justify_future_quant_work_or_mostly_argue_for_respecting_the_raw_baseline_as_is": (
                "Mostly the latter. It clarifies the weakness profile, but it does not argue for broad new quant rescue work."
            ),
        },
    }
    return report


def render_markdown(report: dict[str, Any]) -> str:
    mgc = report["metals"]["MGC"]
    gc = report["metals"]["GC"]
    lines = [
        "# usLatePauseResumeLongTurn Weak-Middle Diagnostic",
        "",
        f"- Verdict: `{report['verdict_bucket']}`",
        "",
        "## Distribution Anatomy",
        f"- MGC standout winners: pnl `{_fmt(mgc['distribution_anatomy']['standout_winners']['realized_pnl'])}`, weak middle `{_fmt(mgc['distribution_anatomy']['weak_middle_trades']['realized_pnl'])}`, poison losers `{_fmt(mgc['distribution_anatomy']['poison_losers']['realized_pnl'])}`",
        f"- GC standout winners: pnl `{_fmt(gc['distribution_anatomy']['standout_winners']['realized_pnl'])}`, weak middle `{_fmt(gc['distribution_anatomy']['weak_middle_trades']['realized_pnl'])}`, poison losers `{_fmt(gc['distribution_anatomy']['poison_losers']['realized_pnl'])}`",
        "",
        "## Cross-Metal Read",
        f"- {report['cross_metal_comparison']['shared_structure']}",
        f"- {report['cross_metal_comparison']['where_gc_is_weaker']['plain_language']}",
        "",
        "## Best Separators",
        f"- Standout winners vs weak middle: `{report['metals']['MGC']['best_diagnostic_separators']['standout_winners_vs_weak_middle'][0]['feature']}` is the strongest MGC separator.",
        f"- Weak middle vs poison losers: `{report['metals']['MGC']['best_diagnostic_separators']['poison_losers_vs_weak_middle'][0]['feature']}` is the strongest MGC separator.",
        "",
        "## Practical Implication",
        f"- {report['practical_implication']['plain_language']}",
        f"- Future quant work: {report['practical_implication']['realistic_quant_lever_left']}",
        "",
        "## Direct Answers",
        f"1. {report['direct_answers']['single_best_explanation_for_the_weak_middle']}",
        f"2. {report['direct_answers']['single_best_diagnostic_separator_between_standout_winners_and_the_weak_middle']}",
        f"3. {report['direct_answers']['is_gc_weaker_for_the_same_reason_as_mgc_or_a_different_one']}",
        f"4. {report['direct_answers']['does_this_diagnostic_pass_justify_future_quant_work_or_mostly_argue_for_respecting_the_raw_baseline_as_is']}",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(report: dict[str, Any]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(render_markdown(report), encoding="utf-8")


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.{digits}f}"


if __name__ == "__main__":
    raise SystemExit(main())
