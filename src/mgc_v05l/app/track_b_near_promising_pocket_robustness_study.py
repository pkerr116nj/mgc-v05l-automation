"""Research-only robustness study for promising Track B NEAR pockets."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.app.track_b_near_candidate_narrowing_study import DEDUPE_COOLDOWN_BARS
from mgc_v05l.app.track_b_near_regime_session_filter_study import (
    AUTHORITY_FLAGS,
    COST_POINTS,
    DEFAULT_INPUT_ROOT,
    RESEARCH_MODE,
    RegimeCandidate,
    _adaptive_trade,
    _bucket_predicates,
    _candidate_records,
    _discover_archive_paths,
    _float_or_none,
    _json_ready,
    _load_archive_rows,
    _parse_datetime,
    _regime_filters,
    _rows_by_instrument,
    _score_rows,
    _time_bucket,
    _trade,
    _year_quarter,
)


SCHEMA_VERSION = "track_b_near_promising_pocket_robustness_study_v1"
DEFAULT_OUTPUT_ROOT = Path(
    "outputs/reports/entry_acceptance_research/full_history_batch/"
    "near_promising_pocket_robustness_study"
)
EXITS = ("fixed_24b", "fixed_36b", "adaptive_24_36")
BASELINE_REFERENCE = {
    "exact_fixed_36b": {
        "average_return": 0.306308,
        "profit_factor_proxy": 1.107389,
        "max_drawdown_proxy": 254.5,
        "episodes": 856,
    },
    "exact_adaptive_24_36": {
        "average_return": 0.273832,
        "profit_factor_proxy": 1.113051,
        "max_drawdown_proxy": 228.2,
        "episodes": 856,
    },
}
POCKET_SPECS = {
    "soft_retest_hold_miss_only__atr_high_tercile": (
        "soft_retest_hold_miss_only",
        "atr_high_tercile",
    ),
    "soft_retest_hold_miss_only__range_compressed": (
        "soft_retest_hold_miss_only",
        "range_compressed",
    ),
    "soft_retest_hold_miss_only__no_elevated_failure_risk": (
        "soft_retest_hold_miss_only",
        "no_elevated_failure_risk",
    ),
    "structural_soft_fail__early_asia_first_segment": (
        "structural_soft_fail",
        "time_early_asia_first_segment",
    ),
}


def build_near_promising_pocket_robustness_study(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    """Evaluate robustness of NEAR pockets that survived initial filtering."""

    input_root = Path(input_root)
    output_root = Path(output_root)
    archive_paths = _discover_archive_paths(input_root)
    rows = _load_archive_rows(archive_paths)
    scored_rows = _score_rows(rows)
    records = _candidate_records(rows=rows, scored_rows=scored_rows)
    rows_by_instrument = _rows_by_instrument(rows)
    bucket_predicates = _bucket_predicates()
    regime_filters = _regime_filters(records)
    pockets = {
        pocket_name: _pocket_report(
            candidates=[
                candidate
                for candidate in records
                if bucket_predicates[bucket_name](candidate)
                and regime_filters[filter_name](candidate)
            ],
            rows_by_instrument=rows_by_instrument,
        )
        for pocket_name, (bucket_name, filter_name) in POCKET_SPECS.items()
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "mode": RESEARCH_MODE,
        "input_root": str(input_root),
        "output_root": str(output_root),
        "archive_count": len(archive_paths),
        "rows_scanned": len(rows),
        "coverage": _coverage(rows),
        "scope": {
            "family": "asiaEarlyNormalBreakoutRetestHoldLong",
            "instruments": sorted({str(row.get("instrument")) for row in rows}),
            "round_trip_cost_points": COST_POINTS,
            "dedupe_cooldown_bars": DEDUPE_COOLDOWN_BARS,
            "exits": list(EXITS),
        },
        "authority_flags": dict(AUTHORITY_FLAGS),
        "baseline_reference": BASELINE_REFERENCE,
        "pockets": pockets,
        "verdict_summary": _verdict_summary(pockets),
        "conclusion": {
            "BROAD_NEAR_PARKED": True,
            "NO_NEAR_POCKET_PROMOTED": True,
            "SOFT_RETEST_HOLD_ATR_HIGH_RESEARCH_BRANCH": (
                pockets["soft_retest_hold_miss_only__atr_high_tercile"]["pocket_verdict"]
                == "RESEARCH_BRANCH_CONTINUE"
            ),
            "SOFT_RETEST_HOLD_NO_ELEVATED_FAILURE_RISK_RESEARCH_BRANCH": (
                pockets["soft_retest_hold_miss_only__no_elevated_failure_risk"]["pocket_verdict"]
                == "RESEARCH_BRANCH_CONTINUE"
            ),
        },
    }
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "near_promising_pocket_robustness_study_v1.json"
    md_path = output_root / "near_promising_pocket_robustness_study_v1.md"
    report["report_json"] = str(json_path)
    report["report_markdown"] = str(md_path)
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(_markdown_report(report), encoding="utf-8")
    return report


def _pocket_report(
    *,
    candidates: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    episodes = _dedupe_candidates(candidates, cooldown=DEDUPE_COOLDOWN_BARS)
    exit_results = {
        exit_name: _exit_report(trades)
        for exit_name, trades in _trades_by_exit(episodes, rows_by_instrument).items()
    }
    best_exit = max(
        exit_results,
        key=lambda name: _float_or_none(exit_results[name]["overall"].get("average_return")) or -math.inf,
    )
    return {
        "candidate_count": len(candidates),
        "episode_count": len(episodes),
        "time_bucket_counts": dict(sorted(Counter(_time_bucket(item) for item in candidates).items())),
        "gap_primary_counts": dict(sorted(Counter(item.primary_gap for item in candidates).items())),
        "exit_results": exit_results,
        "best_exit_by_average": best_exit,
        "pocket_verdict": exit_results[best_exit]["verdict"],
    }


def _dedupe_candidates(
    candidates: Sequence[RegimeCandidate],
    *,
    cooldown: int,
) -> list[RegimeCandidate]:
    selected: list[RegimeCandidate] = []
    last_by_instrument: dict[str, int] = {}
    for candidate in sorted(candidates, key=lambda item: (item.instrument, item.bar_index)):
        previous = last_by_instrument.get(candidate.instrument)
        if previous is None or candidate.bar_index - previous > cooldown:
            selected.append(candidate)
            last_by_instrument[candidate.instrument] = candidate.bar_index
    return selected


def _trades_by_exit(
    episodes: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {exit_name: [] for exit_name in EXITS}
    for candidate in episodes:
        fixed_24 = _trade(candidate, rows_by_instrument=rows_by_instrument, horizon=24)
        fixed_36 = _trade(candidate, rows_by_instrument=rows_by_instrument, horizon=36)
        adaptive = _adaptive_trade(candidate, rows_by_instrument=rows_by_instrument)
        if fixed_24 is not None:
            fixed_24["exit_branch"] = "fixed_24b"
            output["fixed_24b"].append(_with_candidate_metadata(fixed_24, candidate))
        if fixed_36 is not None:
            fixed_36["exit_branch"] = "fixed_36b"
            output["fixed_36b"].append(_with_candidate_metadata(fixed_36, candidate))
        if adaptive is not None:
            output["adaptive_24_36"].append(_with_candidate_metadata(adaptive, candidate))
    return output


def _with_candidate_metadata(
    trade: Mapping[str, Any],
    candidate: RegimeCandidate,
) -> dict[str, Any]:
    output = dict(trade)
    output["candidate_timestamp"] = candidate.timestamp
    output["acceptance_score"] = candidate.acceptance_score
    output["gap_flags"] = list(candidate.gap_flags)
    return output


def _exit_report(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    overall = _stats(trades)
    robustness = {
        "instrument": _period_robustness(trades, "instrument"),
        "year": _period_robustness(trades, "year"),
        "quarter": _period_robustness(trades, "quarter"),
        "month": _period_robustness(trades, "month"),
    }
    drivers = _driver_checks(trades)
    return {
        "overall": overall,
        "by_instrument": _group_stats(trades, "instrument"),
        "by_year": _group_stats(trades, "year"),
        "by_quarter": _group_stats(trades, "quarter"),
        "by_month": _group_stats(trades, "month"),
        "robustness": robustness,
        "contribution_concentration": _concentration(trades),
        "driver_checks": drivers,
        "verdict": _verdict(overall, robustness, drivers),
    }


def _stats(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [float(trade["net_return"]) for trade in trades]
    if not returns:
        return {
            "trade_count": 0,
            "average_return": None,
            "median_return": None,
            "win_rate": None,
            "profit_factor_proxy": None,
            "max_drawdown_proxy": 0.0,
        }
    positives = [value for value in returns if value > 0]
    negatives = [value for value in returns if value < 0]
    return {
        "trade_count": len(returns),
        "average_return": round(sum(returns) / len(returns), 6),
        "median_return": round(median(returns), 6),
        "win_rate": round(len(positives) / len(returns), 6),
        "profit_factor_proxy": _profit_factor(positives, negatives),
        "max_drawdown_proxy": _max_drawdown(returns),
        "best_trade": round(max(returns), 6),
        "worst_trade": round(min(returns), 6),
    }


def _group_stats(
    trades: Sequence[Mapping[str, Any]],
    period: str,
) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[_trade_period(trade, period)].append(trade)
    return {key: _stats(group) for key, group in sorted(groups.items())}


def _period_robustness(
    trades: Sequence[Mapping[str, Any]],
    period: str,
) -> dict[str, Any]:
    grouped = _group_stats(trades, period)
    rows = [
        {"period": key, **value}
        for key, value in grouped.items()
        if int(value.get("trade_count") or 0) > 0
    ]
    positive = [row for row in rows if (_float_or_none(row.get("average_return")) or 0.0) > 0]
    return {
        "period_count": len(rows),
        "positive_period_count": len(positive),
        "positive_period_share": round(len(positive) / len(rows), 6) if rows else None,
        "worst_period": min(rows, key=lambda row: row["average_return"]) if rows else None,
        "best_period": max(rows, key=lambda row: row["average_return"]) if rows else None,
        "periods": grouped,
    }


def _concentration(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [float(trade["net_return"]) for trade in trades]
    total = sum(returns)
    positives = sorted((value for value in returns if value > 0), reverse=True)
    gross_profit = sum(positives)
    gross_loss = abs(sum(value for value in returns if value < 0))
    return {
        "net_total_return": round(total, 6),
        "gross_profit": round(gross_profit, 6),
        "gross_loss": round(gross_loss, 6),
        "positive_trade_count": len(positives),
        "top_positive_contribution_share_of_gross_profit": {
            f"top_{count}": _share(sum(positives[:count]), gross_profit)
            for count in (1, 3, 5, 10)
        },
        "top_positive_contribution_share_of_net": {
            f"top_{count}": _share(sum(positives[:count]), total)
            for count in (1, 3, 5, 10)
        },
    }


def _driver_checks(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_instrument = _group_stats(trades, "instrument")
    by_year = _group_stats(trades, "year")
    by_quarter = _group_stats(trades, "quarter")
    total_net = sum(float(trade["net_return"]) for trade in trades)
    instrument_shares = _net_contribution_shares(by_instrument, total_net)
    year_shares = _net_contribution_shares(by_year, total_net)
    quarter_shares = _net_contribution_shares(by_quarter, total_net)
    return {
        "instrument_contribution_share_of_net": instrument_shares,
        "year_contribution_share_of_net": year_shares,
        "quarter_contribution_share_of_net": quarter_shares,
        "largest_instrument_by_net": _largest_period(by_instrument),
        "largest_year_by_net": _largest_period(by_year),
        "largest_quarter_by_net": _largest_period(by_quarter),
        "one_instrument_driven": _dominant_share(instrument_shares, threshold=0.70),
        "one_year_driven": _dominant_share(year_shares, threshold=0.50),
        "one_quarter_driven": _dominant_share(quarter_shares, threshold=0.35),
        "few_outlier_trades_driven": _few_outliers(trades),
    }


def _verdict(
    overall: Mapping[str, Any],
    robustness: Mapping[str, Any],
    drivers: Mapping[str, Any],
) -> str:
    trade_count = int(overall.get("trade_count") or 0)
    avg = _float_or_none(overall.get("average_return")) or -math.inf
    pf = _float_or_none(overall.get("profit_factor_proxy")) or 0.0
    positive_quarter_share = _float_or_none(
        robustness.get("quarter", {}).get("positive_period_share")
    ) or 0.0
    if trade_count < 100 or avg <= 0 or pf < 1.0:
        return "REJECTED"
    if (
        avg > BASELINE_REFERENCE["exact_fixed_36b"]["average_return"]
        and pf > BASELINE_REFERENCE["exact_fixed_36b"]["profit_factor_proxy"]
        and positive_quarter_share >= 0.60
        and not drivers["few_outlier_trades_driven"]
    ):
        return "PROMOTION_CANDIDATE"
    if (
        avg > BASELINE_REFERENCE["exact_fixed_36b"]["average_return"]
        and pf > 1.1
        and positive_quarter_share >= 0.45
    ):
        return "RESEARCH_BRANCH_CONTINUE"
    if avg > 0 and pf >= 1.0:
        return "PARKED_UNSTABLE"
    return "REJECTED"


def _verdict_summary(pockets: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    by_pocket = {
        name: {
            "best_exit_by_average": payload["best_exit_by_average"],
            "pocket_verdict": payload["pocket_verdict"],
            "exit_verdicts": {
                exit_name: exit_payload["verdict"]
                for exit_name, exit_payload in payload["exit_results"].items()
            },
        }
        for name, payload in pockets.items()
    }
    verdict_counts = Counter(item["pocket_verdict"] for item in by_pocket.values())
    return {
        "by_pocket": by_pocket,
        "verdict_counts": dict(sorted(verdict_counts.items())),
        "notes": [
            "Broad NEAR remains parked.",
            "No NEAR pocket is promoted by this robustness study.",
            "soft_retest_hold_miss_only + ATR high remains a research branch.",
            "soft_retest_hold_miss_only + no_elevated_failure_risk remains a research branch.",
        ],
    }


def _trade_period(trade: Mapping[str, Any], period: str) -> str:
    timestamp = _parse_datetime(trade["candidate_timestamp"])
    if period == "instrument":
        return str(trade["instrument"])
    if period == "year":
        return str(timestamp.year)
    if period == "quarter":
        return _year_quarter(timestamp)
    if period == "month":
        return f"{timestamp.year}-{timestamp.month:02d}"
    raise ValueError(f"unsupported period: {period}")


def _net_contribution_shares(
    grouped_stats: Mapping[str, Mapping[str, Any]],
    total_net: float,
) -> dict[str, float | None]:
    return {
        key: _share(
            (float(value.get("average_return") or 0.0) * int(value.get("trade_count") or 0)),
            total_net,
        )
        for key, value in grouped_stats.items()
    }


def _largest_period(grouped_stats: Mapping[str, Mapping[str, Any]]) -> dict[str, Any] | None:
    if not grouped_stats:
        return None
    scored = [
        (
            float(value.get("average_return") or 0.0) * int(value.get("trade_count") or 0),
            key,
            value,
        )
        for key, value in grouped_stats.items()
    ]
    net, key, value = max(scored, key=lambda item: item[0])
    return {"period": key, "net_return": round(net, 6), **value}


def _dominant_share(shares: Mapping[str, float | None], *, threshold: float) -> bool:
    values = [abs(value) for value in shares.values() if value is not None]
    return bool(values and max(values) >= threshold)


def _few_outliers(trades: Sequence[Mapping[str, Any]]) -> bool:
    share = _concentration(trades)["top_positive_contribution_share_of_gross_profit"].get("top_5")
    return bool(share is not None and share >= 0.35)


def _profit_factor(positives: Sequence[float], negatives: Sequence[float]) -> float | None:
    if not negatives:
        return math.inf if positives else None
    return round(sum(positives) / abs(sum(negatives)), 6)


def _max_drawdown(returns: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return round(max_drawdown, 6)


def _share(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, str | None]:
    timestamps = [_parse_datetime(row["timestamp"]) for row in rows if row.get("timestamp")]
    return {
        "start": min(timestamps).isoformat() if timestamps else None,
        "end": max(timestamps).isoformat() if timestamps else None,
    }


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B NEAR Promising-Pocket Robustness Study v1",
        "",
        f"- mode: `{report['mode']}`",
        f"- coverage: `{report['coverage']}`",
        f"- rows_scanned: `{report['rows_scanned']}`",
        f"- conclusion: `{report['conclusion']}`",
        f"- authority_flags: `{report['authority_flags']}`",
        "",
        "## Pocket Summary",
        "",
        "| Pocket | Candidates | Episodes | Best Exit | Verdict | 24b Avg/PF/DD | 36b Avg/PF/DD | Adaptive Avg/PF/DD |",
        "| --- | ---: | ---: | --- | --- | --- | --- | --- |",
    ]
    for name, payload in report["pockets"].items():
        lines.append(_pocket_table_row(name, payload))
    lines.extend(["", "## Verdict Notes", ""])
    for note in report["verdict_summary"]["notes"]:
        lines.append(f"- {note}")
    lines.extend(
        [
            "",
            "Safety: research/offline only; no broker commands, lane execution, paper proof, or strategy behavior changes.",
            "",
        ]
    )
    return "\n".join(lines)


def _pocket_table_row(name: str, payload: Mapping[str, Any]) -> str:
    return (
        f"| {name} | {payload['candidate_count']} | {payload['episode_count']} | "
        f"{payload['best_exit_by_average']} | {payload['pocket_verdict']} | "
        f"{_triple(payload['exit_results']['fixed_24b']['overall'])} | "
        f"{_triple(payload['exit_results']['fixed_36b']['overall'])} | "
        f"{_triple(payload['exit_results']['adaptive_24_36']['overall'])} |"
    )


def _triple(stats: Mapping[str, Any]) -> str:
    return (
        f"{stats.get('average_return')}/"
        f"{stats.get('profit_factor_proxy')}/"
        f"{stats.get('max_drawdown_proxy')}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    report = build_near_promising_pocket_robustness_study(
        input_root=args.input_root,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "report_json": report["report_json"],
                "report_markdown": report["report_markdown"],
                "coverage": report["coverage"],
                "verdict_summary": report["verdict_summary"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
