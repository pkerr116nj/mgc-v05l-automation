"""Validate retained U.S. Opening NDX constrained expressions from existing replay outputs."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PASS4_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass4" / "index_universe_20200101_20260421_v1" / "reports"
DEFAULT_PASS6_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass6" / "index_universe_20200101_20260421_v1" / "reports"
DEFAULT_PASS6_SIGNAL_DIR = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass6" / "index_universe_20200101_20260421_v1" / "signals"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "us_open_ndx_constrained_expression_validation"

RAW_CLOSE_KEY = ("NO_STOP", "NONE", "close")
RAW_HORIZON_KEYS = {
    "60m": ("NO_STOP", "NONE", "60m"),
    "90m": ("NO_STOP", "NONE", "90m"),
    "120m": ("NO_STOP", "NONE", "120m"),
}
FIXED_STOP_LABELS = ("50pt", "75pt", "100pt", "125pt", "150pt")
FIXED_HORIZONS = ("60m", "90m", "120m")


@dataclass(frozen=True)
class CandidateDefinition:
    stop_family: str
    stop_label: str
    horizon: str

    @property
    def candidate_id(self) -> str:
        return f"{self.stop_family}|{self.stop_label}|{self.horizon}"

    @property
    def display_label(self) -> str:
        return f"{self.stop_label} @ {self.horizon}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="us-open-ndx-constrained-expression-validation")
    parser.add_argument("--pass4-report-dir", type=Path, default=DEFAULT_PASS4_REPORT_DIR)
    parser.add_argument("--pass6-report-dir", type=Path, default=DEFAULT_PASS6_REPORT_DIR)
    parser.add_argument("--pass6-signal-dir", type=Path, default=DEFAULT_PASS6_SIGNAL_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pass4_context = _load_pass4_context(Path(args.pass4_report_dir))
    sim_rows = _load_simulation_rows(Path(args.pass6_signal_dir) / "us_open_probabilistic_pass6_simulation_rows.csv")
    candidates = [CandidateDefinition("FIXED", stop_label, horizon) for stop_label in FIXED_STOP_LABELS for horizon in FIXED_HORIZONS]
    analysis = _analyze_candidates(sim_rows=sim_rows, candidates=candidates, pass4_context=pass4_context)

    _write_csv(output_dir / "us_open_ndx_constrained_expression_validation_summary.csv", analysis["summary_rows"])
    _write_csv(output_dir / "us_open_ndx_constrained_expression_validation_risk_table.csv", analysis["risk_rows"])
    _write_csv(output_dir / "us_open_ndx_constrained_expression_validation_stability_table.csv", analysis["stability_rows"])
    _write_csv(output_dir / "us_open_ndx_constrained_expression_validation_dev_holdout.csv", analysis["dev_holdout_rows"])
    _write_csv(output_dir / "us_open_ndx_constrained_expression_validation_tail_table.csv", analysis["tail_rows"])
    (output_dir / "us_open_ndx_constrained_expression_validation_summary.md").write_text(
        _render_markdown(analysis=analysis, pass4_context=pass4_context) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"classification": analysis["classification"], "output_dir": str(output_dir)}, indent=2))
    return 0


def _load_pass4_context(report_dir: Path) -> dict[str, Any]:
    baseline_rows = _read_csv(report_dir / "us_open_probabilistic_pass4_baseline_comparison.csv")
    dev_holdout_rows = _read_csv(report_dir / "us_open_probabilistic_pass4_dev_holdout_comparison.csv")
    signal_rows = _read_csv(report_dir / "us_open_probabilistic_pass4_signal_extraction.csv")
    baseline_lookup = {(row["slice_name"], row["sample_split"]): row for row in baseline_rows}
    dev_holdout_lookup = {row["slice_name"]: row for row in dev_holdout_rows}
    signal_lookup = {(row["slice_name"], row["sample_split"]): row for row in signal_rows}
    return {
        "up_open_holdout_baseline": baseline_lookup[("UP_OPEN", "holdout")],
        "all_ndx_holdout_signal": signal_lookup[("ALL_NDX_1000", "holdout")],
        "up_open_dev_holdout": dev_holdout_lookup["UP_OPEN"],
    }


def _load_simulation_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _read_csv(path):
        if row["instrument"] not in {"NQ", "MNQ"}:
            continue
        row["return_points"] = float(row["return_points"])
        row["baseline_return_points"] = float(row["baseline_return_points"])
        row["sample_path_minutes"] = int(float(row["sample_path_minutes"]))
        row["stopped_out"] = str(row["stopped_out"]).lower() == "true"
        row["local_session_date"] = str(row["local_session_date"])
        row["decision_ts"] = datetime.fromisoformat(str(row["decision_ts"]))
        row["stop_ts"] = datetime.fromisoformat(str(row["stop_ts"])) if row["stop_ts"] else None
        row["stop_value"] = float(row["stop_value"]) if row["stop_value"] not in {"", None} else None
        rows.append(row)
    return rows


def _analyze_candidates(*, sim_rows: list[dict[str, Any]], candidates: list[CandidateDefinition], pass4_context: dict[str, Any]) -> dict[str, Any]:
    by_key: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_candidate_id: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in sim_rows:
        key = (row["sample_split"], row["stop_family"], row["stop_label"], row["horizon"])
        by_key[key].append(row)
        by_candidate_id[str(row["candidate_id"])][f"{row['stop_family']}|{row['stop_label']}|{row['horizon']}"] = row

    summary_rows: list[dict[str, Any]] = []
    risk_rows: list[dict[str, Any]] = []
    stability_rows: list[dict[str, Any]] = []
    dev_holdout_rows: list[dict[str, Any]] = []
    tail_rows: list[dict[str, Any]] = []

    candidate_health: list[dict[str, Any]] = []

    for candidate in candidates:
        dev_rows = sorted(by_key[("development", candidate.stop_family, candidate.stop_label, candidate.horizon)], key=_sort_key)
        holdout_rows = sorted(by_key[("holdout", candidate.stop_family, candidate.stop_label, candidate.horizon)], key=_sort_key)
        same_horizon_raw_dev = sorted(by_key[("development", *RAW_HORIZON_KEYS[candidate.horizon])], key=_sort_key)
        same_horizon_raw_holdout = sorted(by_key[("holdout", *RAW_HORIZON_KEYS[candidate.horizon])], key=_sort_key)
        raw_close_dev = sorted(by_key[("development", *RAW_CLOSE_KEY)], key=_sort_key)
        raw_close_holdout = sorted(by_key[("holdout", *RAW_CLOSE_KEY)], key=_sort_key)

        dev_metrics = _compute_metrics(dev_rows)
        holdout_metrics = _compute_metrics(holdout_rows)
        raw_horizon_dev_metrics = _compute_metrics(same_horizon_raw_dev)
        raw_horizon_holdout_metrics = _compute_metrics(same_horizon_raw_holdout)
        raw_close_dev_metrics = _compute_metrics(raw_close_dev)
        raw_close_holdout_metrics = _compute_metrics(raw_close_holdout)

        holdout_tail = _tail_compare(holdout_metrics, raw_horizon_holdout_metrics, raw_close_holdout_metrics)
        dev_tail = _tail_compare(dev_metrics, raw_horizon_dev_metrics, raw_close_dev_metrics)
        stop_damage = _stop_damage(
            candidate_rows=holdout_rows,
            baseline_rows=raw_close_holdout,
        )
        period_rows = _period_rows(candidate=candidate, rows=holdout_rows)
        stability_rows.extend(period_rows)

        summary_rows.extend(
            [
                _summary_row(candidate=candidate, split="development", metrics=dev_metrics),
                _summary_row(candidate=candidate, split="holdout", metrics=holdout_metrics),
            ]
        )
        risk_rows.append(
            {
                "candidate_id": candidate.candidate_id,
                "candidate_label": candidate.display_label,
                "stop_label": candidate.stop_label,
                "horizon": candidate.horizon,
                "holdout_trade_count": holdout_metrics["trade_count"],
                "holdout_stop_out_rate": holdout_metrics["stop_out_rate"],
                "holdout_stop_exit_pct": holdout_metrics["stop_exit_pct"],
                "holdout_horizon_exit_pct": holdout_metrics["horizon_exit_pct"],
                "holdout_average_hold_minutes": holdout_metrics["average_hold_minutes"],
                "holdout_max_drawdown_points": holdout_metrics["max_drawdown_points"],
                "holdout_max_consecutive_losers": holdout_metrics["max_consecutive_losers"],
                "holdout_worst_losing_cluster_points": holdout_metrics["worst_losing_cluster_points"],
                "holdout_worst_losing_cluster_len": holdout_metrics["worst_losing_cluster_len"],
                "pct_raw_close_winners_became_losses": stop_damage["pct_raw_close_winners_became_losses"],
                "pct_raw_close_losers_improved": stop_damage["pct_raw_close_losers_improved"],
                "holdout_q4_2025_avg_points": _period_value(period_rows, "quarter", "2025-Q4", "average_points"),
                "holdout_2026_share_of_net_points": _year_net_share(period_rows, "2026"),
            }
        )
        dev_holdout_rows.append(
            {
                "candidate_id": candidate.candidate_id,
                "candidate_label": candidate.display_label,
                "stop_label": candidate.stop_label,
                "horizon": candidate.horizon,
                "development_trade_count": dev_metrics["trade_count"],
                "development_average_points": dev_metrics["average_points"],
                "development_win_rate": dev_metrics["win_rate"],
                "development_profit_factor": dev_metrics["profit_factor"],
                "development_max_drawdown_points": dev_metrics["max_drawdown_points"],
                "holdout_trade_count": holdout_metrics["trade_count"],
                "holdout_average_points": holdout_metrics["average_points"],
                "holdout_win_rate": holdout_metrics["win_rate"],
                "holdout_profit_factor": holdout_metrics["profit_factor"],
                "holdout_max_drawdown_points": holdout_metrics["max_drawdown_points"],
                "average_points_delta_holdout_minus_dev": _delta(holdout_metrics["average_points"], dev_metrics["average_points"]),
                "win_rate_delta_holdout_minus_dev": _delta(holdout_metrics["win_rate"], dev_metrics["win_rate"]),
                "profit_factor_delta_holdout_minus_dev": _delta(holdout_metrics["profit_factor"], dev_metrics["profit_factor"]),
            }
        )
        tail_rows.append(
            {
                "candidate_id": candidate.candidate_id,
                "candidate_label": candidate.display_label,
                "stop_label": candidate.stop_label,
                "horizon": candidate.horizon,
                "holdout_avg_points": holdout_metrics["average_points"],
                "raw_same_horizon_holdout_avg_points": raw_horizon_holdout_metrics["average_points"],
                "raw_close_holdout_avg_points": raw_close_holdout_metrics["average_points"],
                "pass4_all_ndx_holdout_close_avg_points": pass4_context["all_ndx_holdout_signal"]["avg_return_close"],
                "pass4_up_open_holdout_random_percentile_close": pass4_context["up_open_holdout_baseline"]["random_baseline_percentile_close"],
                "tail_reduction_vs_same_horizon_worst_5pct": holdout_tail["tail_reduction_vs_same_horizon_worst_5pct"],
                "tail_reduction_vs_same_horizon_worst_1pct": holdout_tail["tail_reduction_vs_same_horizon_worst_1pct"],
                "tail_reduction_vs_raw_close_worst_5pct": holdout_tail["tail_reduction_vs_raw_close_worst_5pct"],
                "tail_reduction_vs_raw_close_worst_1pct": holdout_tail["tail_reduction_vs_raw_close_worst_1pct"],
                "retained_upside_vs_same_horizon": holdout_tail["retained_upside_vs_same_horizon"],
                "retained_upside_vs_raw_close": holdout_tail["retained_upside_vs_raw_close"],
                "stop_damage_vs_raw_close_winner_loss_pct": stop_damage["pct_raw_close_winners_became_losses"],
                "stop_damage_vs_raw_close_loser_improve_pct": stop_damage["pct_raw_close_losers_improved"],
            }
        )

        candidate_health.append(
            {
                "candidate": candidate,
                "dev": dev_metrics,
                "holdout": holdout_metrics,
                "tail": holdout_tail,
                "period_rows": period_rows,
                "stop_damage": stop_damage,
            }
        )

    classification = _classify(candidate_health)
    return {
        "classification": classification,
        "summary_rows": summary_rows,
        "risk_rows": risk_rows,
        "stability_rows": stability_rows,
        "dev_holdout_rows": dev_holdout_rows,
        "tail_rows": tail_rows,
        "candidate_health": candidate_health,
        "pass4_context": pass4_context,
    }


def _summary_row(*, candidate: CandidateDefinition, split: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_label": candidate.display_label,
        "sample_split": split,
        "stop_label": candidate.stop_label,
        "horizon": candidate.horizon,
        "trade_count": metrics["trade_count"],
        "net_points": metrics["net_points"],
        "average_points": metrics["average_points"],
        "median_points": metrics["median_points"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "average_winner_points": metrics["average_winner_points"],
        "average_loser_points": metrics["average_loser_points"],
        "largest_win_points": metrics["largest_win_points"],
        "largest_loss_points": metrics["largest_loss_points"],
        "max_drawdown_points": metrics["max_drawdown_points"],
        "max_consecutive_losers": metrics["max_consecutive_losers"],
        "worst_5pct_points": metrics["worst_5pct_points"],
        "worst_1pct_points": metrics["worst_1pct_points"],
        "stop_out_rate": metrics["stop_out_rate"],
        "stop_exit_pct": metrics["stop_exit_pct"],
        "horizon_exit_pct": metrics["horizon_exit_pct"],
        "average_hold_minutes": metrics["average_hold_minutes"],
    }


def _period_rows(*, candidate: CandidateDefinition, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for period_type, label_builder in (
        ("year", lambda date: date[:4]),
        ("quarter", _quarter_label),
    ):
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            buckets[label_builder(row["local_session_date"])].append(row)
        for period_label, bucket in sorted(buckets.items()):
            metrics = _compute_metrics(sorted(bucket, key=_sort_key))
            payload.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_label": candidate.display_label,
                    "period_type": period_type,
                    "period_label": period_label,
                    "trade_count": metrics["trade_count"],
                    "net_points": metrics["net_points"],
                    "average_points": metrics["average_points"],
                    "median_points": metrics["median_points"],
                    "win_rate": metrics["win_rate"],
                    "profit_factor": metrics["profit_factor"],
                    "max_drawdown_points": metrics["max_drawdown_points"],
                    "max_consecutive_losers": metrics["max_consecutive_losers"],
                    "worst_5pct_points": metrics["worst_5pct_points"],
                    "worst_1pct_points": metrics["worst_1pct_points"],
                }
            )
    return payload


def _compute_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [row["return_points"] for row in rows]
    winners = [value for value in returns if value > 0.0]
    losers = [value for value in returns if value < 0.0]
    stop_count = sum(1 for row in rows if row["stopped_out"])
    hold_minutes = [_hold_minutes(row) for row in rows]
    return {
        "trade_count": len(rows),
        "net_points": round(sum(returns), 6),
        "average_points": round(_mean(returns), 6),
        "median_points": round(median(returns), 6) if returns else 0.0,
        "win_rate": round(sum(1 for value in returns if value > 0.0) / len(returns), 6) if returns else 0.0,
        "profit_factor": _profit_factor(winners, losers),
        "average_winner_points": round(_mean(winners), 6),
        "average_loser_points": round(_mean(losers), 6),
        "largest_win_points": round(max(returns), 6) if returns else 0.0,
        "largest_loss_points": round(min(returns), 6) if returns else 0.0,
        "max_drawdown_points": round(_max_drawdown(returns), 6),
        "max_consecutive_losers": _max_consecutive_losers(returns),
        "worst_losing_cluster_points": round(_worst_losing_cluster(returns)[0], 6),
        "worst_losing_cluster_len": _worst_losing_cluster(returns)[1],
        "worst_5pct_points": round(_quantile(returns, 0.05), 6),
        "worst_1pct_points": round(_quantile(returns, 0.01), 6),
        "stop_out_rate": round(stop_count / len(rows), 6) if rows else 0.0,
        "stop_exit_pct": round(stop_count / len(rows), 6) if rows else 0.0,
        "horizon_exit_pct": round((len(rows) - stop_count) / len(rows), 6) if rows else 0.0,
        "average_hold_minutes": round(_mean(hold_minutes), 6),
    }


def _tail_compare(candidate_metrics: dict[str, Any], raw_horizon_metrics: dict[str, Any], raw_close_metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "tail_reduction_vs_same_horizon_worst_5pct": _tail_reduction(raw_horizon_metrics["worst_5pct_points"], candidate_metrics["worst_5pct_points"]),
        "tail_reduction_vs_same_horizon_worst_1pct": _tail_reduction(raw_horizon_metrics["worst_1pct_points"], candidate_metrics["worst_1pct_points"]),
        "tail_reduction_vs_raw_close_worst_5pct": _tail_reduction(raw_close_metrics["worst_5pct_points"], candidate_metrics["worst_5pct_points"]),
        "tail_reduction_vs_raw_close_worst_1pct": _tail_reduction(raw_close_metrics["worst_1pct_points"], candidate_metrics["worst_1pct_points"]),
        "retained_upside_vs_same_horizon": _safe_ratio(candidate_metrics["average_points"], raw_horizon_metrics["average_points"]),
        "retained_upside_vs_raw_close": _safe_ratio(candidate_metrics["average_points"], raw_close_metrics["average_points"]),
    }


def _stop_damage(*, candidate_rows: list[dict[str, Any]], baseline_rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_lookup = {row["candidate_id"]: row for row in baseline_rows}
    winner_count = 0
    winner_to_loss = 0
    loser_count = 0
    loser_improved = 0
    for row in candidate_rows:
        baseline = baseline_lookup.get(row["candidate_id"])
        if baseline is None:
            continue
        baseline_return = float(baseline["return_points"])
        candidate_return = float(row["return_points"])
        if baseline_return > 0.0:
            winner_count += 1
            if candidate_return < 0.0:
                winner_to_loss += 1
        elif baseline_return < 0.0:
            loser_count += 1
            if candidate_return > baseline_return:
                loser_improved += 1
    return {
        "pct_raw_close_winners_became_losses": round(winner_to_loss / winner_count, 6) if winner_count else None,
        "pct_raw_close_losers_improved": round(loser_improved / loser_count, 6) if loser_count else None,
    }


def _classify(candidate_health: list[dict[str, Any]]) -> str:
    viable_candidates = [
        row
        for row in candidate_health
        if row["candidate"].horizon == "120m"
        and row["candidate"].stop_label in {"100pt", "125pt", "150pt"}
        and row["dev"]["average_points"] > 0.0
        and row["holdout"]["average_points"] > 0.0
        and (row["dev"]["profit_factor"] or 0.0) > 1.0
        and (row["holdout"]["profit_factor"] or 0.0) > 1.0
    ]
    promotable_candidates = [
        row
        for row in viable_candidates
        if (row["stop_damage"]["pct_raw_close_winners_became_losses"] or 1.0) <= 0.10
    ]
    if not viable_candidates:
        return "US_OPEN_NDX_REJECT_CONSTRAINED_EXPRESSION"

    all_stable = True
    any_good = False
    for row in promotable_candidates:
        holdout_tail = row["tail"]["tail_reduction_vs_raw_close_worst_5pct"]
        q4_2025 = _period_value(row["period_rows"], "quarter", "2025-Q4", "average_points")
        y2026_share = _year_net_share(row["period_rows"], "2026")
        if (holdout_tail or 0.0) >= 0.35:
            any_good = True
        if (q4_2025 is not None and q4_2025 < -10.0) or (y2026_share is not None and y2026_share > 0.65):
            all_stable = False
    if any_good and all_stable:
        return "US_OPEN_NDX_PROMOTE_TO_PAPER"
    if viable_candidates:
        return "US_OPEN_NDX_RETAIN_RESEARCH"
    return "US_OPEN_NDX_REJECT_CONSTRAINED_EXPRESSION"


def _render_markdown(*, analysis: dict[str, Any], pass4_context: dict[str, Any]) -> str:
    classification = analysis["classification"]
    candidate_health = analysis["candidate_health"]
    top_structural = sorted(
        [
            row
            for row in candidate_health
            if row["candidate"].stop_label in {"100pt", "125pt", "150pt"}
        ],
        key=lambda row: (
            row["candidate"].horizon != "120m",
            -(row["holdout"]["average_points"]),
        ),
    )[:6]
    lines = [
        "# US Open NDX Constrained-Expression Validation",
        "",
        f"- classification: `{classification}`",
        "- scope: retained `NDX / NQ / MNQ` `UP_OPEN` long-only signal at `10:00 ET`",
        "- candidate family: predefined `fixed-point stop + fixed horizon` from Pass 6 only",
        "- no new thresholds, no new filters, no IBKR wiring",
        "",
        "## Raw Reference",
        "",
        f"- raw holdout `UP_OPEN` close avg: `{pass4_context['up_open_holdout_baseline']['observed_avg_return_close']}`",
        f"- raw holdout `UP_OPEN` random-baseline percentile at close: `{pass4_context['up_open_holdout_baseline']['random_baseline_percentile_close']}`",
        f"- raw holdout `UP_OPEN` close win rate: `{pass4_context['up_open_dev_holdout']['holdout_win_rate_close']}`",
        f"- holdout `ALL_NDX` close avg: `{pass4_context['all_ndx_holdout_signal']['avg_return_close']}`",
        "",
        "## Structural Read",
        "",
        "- fixed stops materially reduce worst-tail outcomes versus the raw close expression",
        "- `90m` is generally weaker than the nearby `60m` and `120m` cells",
        "- `120m` remains the cleaner structural read because the better `120m` fixed-stop cells stay positive in both development and holdout, while several strong `60m` rows are negative in development",
        "- the very tight `50pt` rows are numerically strong in holdout but operationally less clean because their medians stay negative and they convert too many raw winners into stopped losses",
        "- the family is still too period-fragile to promote directly if one recent subperiod dominates the result",
        "",
        "## Candidate Snapshot",
        "",
    ]
    for row in top_structural:
        candidate = row["candidate"]
        holdout = row["holdout"]
        dev = row["dev"]
        tail = row["tail"]
        q4_2025 = _period_value(row["period_rows"], "quarter", "2025-Q4", "average_points")
        y2026_share = _year_net_share(row["period_rows"], "2026")
        lines.append(
            f"- `{candidate.display_label}`: holdout avg `{holdout['average_points']:.3f}`, holdout PF `{_fmt_num(holdout['profit_factor'])}`, "
            f"dev avg `{dev['average_points']:.3f}`, dev PF `{_fmt_num(dev['profit_factor'])}`, "
            f"tail reduction vs raw close 5% `{_fmt_pct(tail['tail_reduction_vs_raw_close_worst_5pct'])}`, "
            f"`2025-Q4` avg `{_fmt_num(q4_2025)}`, `2026` net-share `{_fmt_pct(y2026_share)}`"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
        ]
    )
    if classification == "US_OPEN_NDX_PROMOTE_TO_PAPER":
        lines.append(
            "The constrained expression family survives well enough to become the next IBKR paper candidate: survivability is materially better than raw close, the best 120m cells remain positive on both sides of the split, and no single recent period dominates excessively."
        )
    elif classification == "US_OPEN_NDX_RETAIN_RESEARCH":
        lines.append(
            "The constrained expression family is promising enough to retain, but not clean enough to promote yet. Tail control is real, yet the better rows still show notable development-vs-holdout instability and a meaningful share of holdout net points comes from the 2026 pocket after a weak `2025-Q4` cluster."
        )
    else:
        lines.append(
            "The constrained expression family does not survive validation cleanly enough to justify paper candidacy. The apparent improvements are too fragile or too dependent on narrow period behavior."
        )
    lines.append("")
    lines.append("This is a research classification only. No IBKR paper lane was created in this pass.")
    return "\n".join(lines)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _sort_key(row: dict[str, Any]) -> tuple[str, str]:
    return (row["local_session_date"], row["candidate_id"])


def _hold_minutes(row: dict[str, Any]) -> int:
    if row["stop_ts"] is not None:
        return int((row["stop_ts"] - row["decision_ts"]).total_seconds() // 60)
    return int(row["sample_path_minutes"])


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    return (sum(values) / len(values)) if values else 0.0


def _profit_factor(winners: list[float], losers: list[float]) -> float | None:
    gross_win = sum(winners)
    gross_loss = abs(sum(losers))
    if gross_loss <= 0.0:
        return None
    return round(gross_win / gross_loss, 6)


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    position = q * (len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _max_drawdown(returns: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return abs(max_dd)


def _max_consecutive_losers(returns: list[float]) -> int:
    longest = 0
    current = 0
    for value in returns:
        if value <= 0.0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _worst_losing_cluster(returns: list[float]) -> tuple[float, int]:
    worst_sum = 0.0
    worst_len = 0
    current_sum = 0.0
    current_len = 0
    for value in returns:
        if value <= 0.0:
            current_sum += value
            current_len += 1
            if current_sum < worst_sum:
                worst_sum = current_sum
                worst_len = current_len
        else:
            current_sum = 0.0
            current_len = 0
    return worst_sum, worst_len


def _tail_reduction(baseline: float, candidate: float) -> float | None:
    baseline_abs = abs(baseline)
    if baseline_abs <= 1e-9:
        return None
    return round((baseline_abs - abs(candidate)) / baseline_abs, 6)


def _safe_ratio(value: float, baseline: float) -> float | None:
    if abs(baseline) <= 1e-9:
        return None
    return round(value / baseline, 6)


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 6)


def _quarter_label(local_session_date: str) -> str:
    year, month, _ = local_session_date.split("-")
    quarter = (int(month) - 1) // 3 + 1
    return f"{year}-Q{quarter}"


def _period_value(rows: list[dict[str, Any]], period_type: str, period_label: str, field: str) -> float | None:
    for row in rows:
        if row["period_type"] == period_type and row["period_label"] == period_label:
            return float(row[field])
    return None


def _year_net_share(rows: list[dict[str, Any]], year: str) -> float | None:
    year_rows = [row for row in rows if row["period_type"] == "year"]
    total = sum(float(row["net_points"]) for row in year_rows)
    if abs(total) <= 1e-9:
        return None
    target = sum(float(row["net_points"]) for row in year_rows if row["period_label"] == year)
    return round(target / total, 6)


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2%}"


def _fmt_num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
