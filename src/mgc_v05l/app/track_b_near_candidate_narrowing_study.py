"""Research-only NEAR candidate narrowing study for Track B Entry Acceptance."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable, Mapping, Sequence

from mgc_v05l.app.track_b_entry_acceptance_research_report import (
    _scored_row,
    _scorer_payload_from_archive_row,
)
from mgc_v05l.execution_core.track_b_entry_acceptance import (
    EntryAcceptanceThresholds,
    build_entry_acceptance_state,
)


SCHEMA_VERSION = "track_b_near_candidate_narrowing_study_v1"
RESEARCH_MODE = "RESEARCH_OFFLINE_ONLY"
NEAR_CLASS = "NEAR_STRUCTURAL_MATCH"
SCORE_THRESHOLDS = (0.80, 0.85, 0.90, 0.95)
EXITS = (24, 36)
COST_POINTS = 0.5
DEDUPE_COOLDOWN_BARS = 12
DEFAULT_INPUT_ROOT = Path("outputs/reports/entry_acceptance_research/full_history_batch/quarterly_reports")
DEFAULT_OUTPUT_ROOT = Path("outputs/reports/entry_acceptance_research/full_history_batch/near_narrowing_study")
AUTHORITY_FLAGS = {
    "research_offline_only": True,
    "broker_state_mutated": False,
    "submit_attempted": False,
    "order_intent_created": False,
    "lifecycle_mutated": False,
    "runtime_trade_eligible": False,
    "strategy_behavior_changed": False,
    "paper_eligible": False,
    "live_eligible": False,
}


@dataclass(frozen=True)
class StudyCandidate:
    row: dict[str, Any]
    scored: dict[str, Any]
    instrument: str
    timestamp: datetime
    bar_index: int
    acceptance_score: float
    gap_flags: tuple[str, ...]
    primary_gap: str


def build_near_narrowing_study(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    """Run the conservative NEAR narrowing study over archived research rows."""

    input_root = Path(input_root)
    output_root = Path(output_root)
    _reject_non_research_path(input_root)
    archive_paths = _discover_archive_paths(input_root)
    rows = _load_archive_rows(archive_paths)
    scored_rows = _score_rows(rows)
    indexed_rows = _index_rows(rows=rows, scored_rows=scored_rows)
    near_candidates = [
        item
        for item in indexed_rows
        if item.scored.get("acceptance_class") == NEAR_CLASS
        and item.row.get("current_exact_rule_flag") is not True
    ]

    threshold_results = {
        _threshold_name(threshold): _evaluate_subset(
            candidates=[item for item in near_candidates if item.acceptance_score >= threshold],
            rows_by_instrument=_rows_by_instrument(rows),
        )
        for threshold in SCORE_THRESHOLDS
    }
    subset_results = {
        name: _evaluate_subset(
            candidates=[item for item in near_candidates if predicate(item)],
            rows_by_instrument=_rows_by_instrument(rows),
        )
        for name, predicate in _subset_predicates().items()
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "mode": RESEARCH_MODE,
        "input_root": str(input_root),
        "output_root": str(output_root),
        "archive_count": len(archive_paths),
        "rows_scanned": len(rows),
        "near_candidate_count": len(near_candidates),
        "coverage": _coverage(rows),
        "scope": {
            "family": "asiaEarlyNormalBreakoutRetestHoldLong",
            "instruments": sorted({str(row.get("instrument")) for row in rows}),
            "candidate_class": NEAR_CLASS,
            "entry": "next_bar_open",
            "exits_bars": list(EXITS),
            "cost_points_round_trip": COST_POINTS,
            "dedupe_cooldown_bars": DEDUPE_COOLDOWN_BARS,
        },
        "authority_flags": dict(AUTHORITY_FLAGS),
        "score_threshold_sweep": threshold_results,
        "exact_rule_gap_analysis": _gap_analysis(near_candidates),
        "conservative_subsets": subset_results,
        "baseline_reference": _baseline_reference(),
        "interpretation": _interpretation(threshold_results, subset_results),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "near_candidate_narrowing_study_v1.json"
    md_path = output_root / "near_candidate_narrowing_study_v1.md"
    report["report_json"] = str(json_path)
    report["report_markdown"] = str(md_path)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    md_path.write_text(_markdown_report(report), encoding="utf-8")
    return report


def _discover_archive_paths(input_root: Path) -> list[Path]:
    paths = sorted(input_root.glob("*/*/asia_early_normal_breakout_retest_hold_enriched_candidate_archive.jsonl"))
    if not paths:
        raise FileNotFoundError(f"no enriched candidate archives found under {input_root}")
    return paths


def _load_archive_rows(paths: Sequence[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            row["_archive_path"] = str(path)
            rows.append(row)
    return sorted(rows, key=lambda row: (str(row.get("instrument")), _parse_datetime(row.get("timestamp"))))


def _score_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    thresholds = EntryAcceptanceThresholds(
        min_completed_candles=3,
        preferred_completed_candles=8,
        stale_after_intervals=999999,
        max_missing_gap_intervals=999999,
    )
    scored: list[dict[str, Any]] = []
    for row in rows:
        archive_path = Path(str(row.get("_archive_path") or "outputs/reports/entry_acceptance_research/archive.jsonl"))
        payload = _scorer_payload_from_archive_row(row, archive_path=archive_path)
        state = build_entry_acceptance_state(payload, now=row.get("timestamp"), thresholds=thresholds)
        scored.append(_scored_row(row, state))
    return scored


def _index_rows(*, rows: Sequence[Mapping[str, Any]], scored_rows: Sequence[Mapping[str, Any]]) -> list[StudyCandidate]:
    counters: dict[str, int] = {}
    indexed: list[StudyCandidate] = []
    for row, scored in zip(rows, scored_rows, strict=False):
        instrument = str(row.get("instrument"))
        index = counters.get(instrument, 0)
        counters[instrument] = index + 1
        if scored.get("acceptance_class") != NEAR_CLASS:
            continue
        flags = _gap_flags(row=row, scored=scored)
        indexed.append(
            StudyCandidate(
                row=dict(row),
                scored=dict(scored),
                instrument=instrument,
                timestamp=_parse_datetime(row.get("timestamp")),
                bar_index=index,
                acceptance_score=_float_or_none(scored.get("acceptance_score")) or 0.0,
                gap_flags=tuple(flags),
                primary_gap=_primary_gap(flags),
            )
        )
    return indexed


def _gap_flags(*, row: Mapping[str, Any], scored: Mapping[str, Any]) -> list[str]:
    failures = {str(item) for item in scored.get("failure_reasons") or []}
    flags: list[str] = []
    if _missing_or_provenance_issue(row=row, failures=failures):
        flags.append("missing_provenance_issue")
    if "WRONG_SIDE_DIRECTIONAL_CONTRADICTION" in failures or _close_location(row) < 0.35:
        flags.append("wrong_side_directional_contradiction")
    if _churn_or_snap(row):
        flags.append("anti_churn_snap_turn_conflict")
    if str(row.get("session")) != "ASIA_EARLY":
        flags.append("timing_session_miss")
    if _marginal_range(row):
        flags.append("marginal_range_expansion")
    if _soft_retest_hold_miss(row):
        flags.append("soft_retest_hold_miss")
    if _missing_structure(row):
        flags.append("missing_breakout_retest_hold_structure")
    return flags or ["uncategorized_near_gap"]


def _primary_gap(flags: Sequence[str]) -> str:
    priority = (
        "missing_provenance_issue",
        "wrong_side_directional_contradiction",
        "anti_churn_snap_turn_conflict",
        "timing_session_miss",
        "missing_breakout_retest_hold_structure",
        "soft_retest_hold_miss",
        "marginal_range_expansion",
        "uncategorized_near_gap",
    )
    for item in priority:
        if item in flags:
            return item
    return "uncategorized_near_gap"


def _subset_predicates() -> dict[str, Callable[[StudyCandidate], bool]]:
    return {
        "A_high_score_only_near_gte_0_90": lambda item: item.acceptance_score >= 0.90,
        "B_no_directional_contradiction_near_gte_0_80": lambda item: item.acceptance_score >= 0.80
        and "wrong_side_directional_contradiction" not in item.gap_flags,
        "C_no_churn_snap_conflict_near_gte_0_80": lambda item: item.acceptance_score >= 0.80
        and "anti_churn_snap_turn_conflict" not in item.gap_flags,
        "D_soft_retest_hold_miss_only_near_gte_0_80": lambda item: item.acceptance_score >= 0.80
        and set(item.gap_flags).issubset({"soft_retest_hold_miss", "marginal_range_expansion"}),
        "E_structural_soft_fail_near_gte_0_80": lambda item: item.acceptance_score >= 0.80
        and _structural_soft_fail(item.row, item.gap_flags),
    }


def _evaluate_subset(
    *,
    candidates: Sequence[StudyCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    episodes = _dedupe_candidates(candidates, cooldown=DEDUPE_COOLDOWN_BARS)
    trades_by_exit = {
        f"fixed_{horizon}b": [
            trade
            for trade in (
                _trade(candidate, rows_by_instrument=rows_by_instrument, horizon=horizon)
                for candidate in episodes
            )
            if trade is not None
        ]
        for horizon in EXITS
    }
    return {
        "candidate_count": len(candidates),
        "episode_count": len(episodes),
        "gap_primary_counts": dict(sorted(Counter(item.primary_gap for item in candidates).items())),
        "by_instrument_counts": dict(sorted(Counter(item.instrument for item in candidates).items())),
        "results": {
            exit_name: _trade_stats(trades)
            for exit_name, trades in trades_by_exit.items()
        },
        "by_instrument": {
            instrument: {
                exit_name: _trade_stats([trade for trade in trades if trade["instrument"] == instrument])
                for exit_name, trades in trades_by_exit.items()
            }
            for instrument in sorted({item.instrument for item in candidates})
        },
        "by_year_quarter": _by_year_quarter(episodes=episodes, trades_by_exit=trades_by_exit),
    }


def _dedupe_candidates(candidates: Sequence[StudyCandidate], *, cooldown: int) -> list[StudyCandidate]:
    selected: list[StudyCandidate] = []
    last_by_instrument: dict[str, int] = {}
    for candidate in sorted(candidates, key=lambda item: (item.instrument, item.bar_index)):
        previous = last_by_instrument.get(candidate.instrument)
        if previous is None or candidate.bar_index - previous > cooldown:
            selected.append(candidate)
            last_by_instrument[candidate.instrument] = candidate.bar_index
    return selected


def _trade(
    candidate: StudyCandidate,
    *,
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
    horizon: int,
) -> dict[str, Any] | None:
    rows = rows_by_instrument.get(candidate.instrument, ())
    entry_index = candidate.bar_index + 1
    exit_index = entry_index + horizon - 1
    if entry_index >= len(rows) or exit_index >= len(rows):
        return None
    entry_candle = _signal_candle(rows[entry_index])
    exit_candle = _signal_candle(rows[exit_index])
    entry_price = _float_or_none(entry_candle.get("open"))
    exit_price = _float_or_none(exit_candle.get("close"))
    if entry_price is None or exit_price is None:
        return None
    window = [_signal_candle(row) for row in rows[entry_index : exit_index + 1]]
    highs = [_float_or_none(row.get("high")) for row in window]
    lows = [_float_or_none(row.get("low")) for row in window]
    valid_highs = [item for item in highs if item is not None]
    valid_lows = [item for item in lows if item is not None]
    if not valid_highs or not valid_lows:
        return None
    gross = exit_price - entry_price
    return {
        "instrument": candidate.instrument,
        "year_quarter": _year_quarter(candidate.timestamp),
        "gross_return": gross,
        "net_return": gross - COST_POINTS,
        "mfe": max(valid_highs) - entry_price,
        "mae": min(valid_lows) - entry_price,
    }


def _trade_stats(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [float(trade["net_return"]) for trade in trades]
    positives = [item for item in returns if item > 0]
    negatives = [item for item in returns if item < 0]
    mfes = [float(trade["mfe"]) for trade in trades]
    maes = [float(trade["mae"]) for trade in trades]
    return {
        "trade_count": len(trades),
        "average_return": _round(sum(returns) / len(returns)) if returns else None,
        "median_return": _round(median(returns)) if returns else None,
        "win_rate": _round(len(positives) / len(returns)) if returns else None,
        "profit_factor_proxy": _profit_factor_proxy(positives, negatives),
        "max_drawdown_proxy": _round(_max_drawdown(returns)),
        "avg_mfe": _round(sum(mfes) / len(mfes)) if mfes else None,
        "avg_mae": _round(sum(maes) / len(maes)) if maes else None,
    }


def _gap_analysis(candidates: Sequence[StudyCandidate]) -> dict[str, Any]:
    all_flags = Counter(flag for item in candidates for flag in item.gap_flags)
    primary = Counter(item.primary_gap for item in candidates)
    by_threshold = {
        _threshold_name(threshold): {
            "candidate_count": len([item for item in candidates if item.acceptance_score >= threshold]),
            "primary_gap_counts": dict(
                sorted(Counter(item.primary_gap for item in candidates if item.acceptance_score >= threshold).items())
            ),
        }
        for threshold in SCORE_THRESHOLDS
    }
    return {
        "all_gap_flag_counts": dict(sorted(all_flags.items())),
        "primary_gap_counts": dict(sorted(primary.items())),
        "by_score_threshold": by_threshold,
    }


def _by_year_quarter(
    *,
    episodes: Sequence[StudyCandidate],
    trades_by_exit: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    periods = sorted({_year_quarter(item.timestamp) for item in episodes})
    return {
        period: {
            "episode_count": sum(1 for item in episodes if _year_quarter(item.timestamp) == period),
            "results": {
                exit_name: _trade_stats([trade for trade in trades if trade["year_quarter"] == period])
                for exit_name, trades in trades_by_exit.items()
            },
        }
        for period in periods
    }


def _rows_by_instrument(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    output: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        output.setdefault(str(row.get("instrument")), []).append(row)
    for instrument, instrument_rows in output.items():
        output[instrument] = sorted(instrument_rows, key=lambda row: _parse_datetime(row.get("timestamp")))
    return output


def _signal_candle(row: Mapping[str, Any]) -> Mapping[str, Any]:
    candle = row.get("signal_candle")
    return candle if isinstance(candle, Mapping) else row


def _structural_soft_fail(row: Mapping[str, Any], gap_flags: Sequence[str]) -> bool:
    return (
        row.get("breakout_breaks_prior_1_high") is True
        and _retest_mostly_present(row)
        and _hold_mostly_present(row)
        and "wrong_side_directional_contradiction" not in gap_flags
        and "anti_churn_snap_turn_conflict" not in gap_flags
        and _acceptable_range(row)
    )


def _missing_or_provenance_issue(*, row: Mapping[str, Any], failures: set[str]) -> bool:
    data_quality = row.get("data_quality") if isinstance(row.get("data_quality"), Mapping) else {}
    return (
        "MISSING_CANDLES" in failures
        or "THIN_DATA" in failures
        or "STALE_INPUT" in failures
        or data_quality.get("has_min_breakout_history") is False
    )


def _soft_retest_hold_miss(row: Mapping[str, Any]) -> bool:
    return row.get("breakout_breaks_prior_1_high") is True and (
        row.get("signal_retests_and_holds_breakout_level") is not True
    ) and (_retest_mostly_present(row) or _hold_mostly_present(row))


def _missing_structure(row: Mapping[str, Any]) -> bool:
    return row.get("breakout_breaks_prior_1_high") is not True or (
        not _retest_mostly_present(row) and not _hold_mostly_present(row)
    )


def _marginal_range(row: Mapping[str, Any]) -> bool:
    ratio = _float_or_none(row.get("range_expansion_ratio"))
    return ratio is not None and (0.70 <= ratio < 0.85 or 1.25 < ratio <= 1.50)


def _acceptable_range(row: Mapping[str, Any]) -> bool:
    ratio = _float_or_none(row.get("range_expansion_ratio"))
    return ratio is not None and 0.75 <= ratio <= 1.35


def _churn_or_snap(row: Mapping[str, Any]) -> bool:
    return (_float_or_none(row.get("churn_score")) or 0.0) >= 0.50 or (
        _float_or_none(row.get("snap_turn_conflict_strength")) or 0.0
    ) >= 0.50


def _retest_mostly_present(row: Mapping[str, Any]) -> bool:
    depth = _float_or_none(row.get("retest_depth_normalized"))
    return depth is not None and depth >= -0.15


def _hold_mostly_present(row: Mapping[str, Any]) -> bool:
    margin = _float_or_none(row.get("hold_margin_normalized"))
    return margin is not None and margin >= -0.15


def _close_location(row: Mapping[str, Any]) -> float:
    return _float_or_none(row.get("close_location")) or 0.0


def _baseline_reference() -> dict[str, Any]:
    return {
        "exact_baseline_24b": {"episodes": 856, "average_return": 0.137617, "profit_factor_proxy": 1.057154},
        "exact_baseline_36b": {"episodes": 856, "average_return": 0.306308, "profit_factor_proxy": 1.107389},
        "decision_prior": "NEAR_EXPANSION_PARKED unless a narrowed subset clearly improves costed diagnostics.",
    }


def _interpretation(threshold_results: Mapping[str, Any], subset_results: Mapping[str, Any]) -> list[str]:
    notes = [
        "This is a research/offline narrowing study over NEAR_STRUCTURAL_MATCH candidates only.",
        "All returns are next-bar-open with 0.5 point round-trip cost and 12-bar de-duped episodes.",
    ]
    baseline_36 = _baseline_reference()["exact_baseline_36b"]["average_return"]
    best_name = None
    best_exit = None
    best_avg = -math.inf
    for group_name, group in {**threshold_results, **subset_results}.items():
        for exit_name, stats in group.get("results", {}).items():
            avg = _float_or_none(stats.get("average_return"))
            if avg is not None and avg > best_avg:
                best_name = group_name
                best_exit = exit_name
                best_avg = avg
    if best_name is not None:
        notes.append(f"Best narrowed NEAR result is {best_name} / {best_exit} with avg {round(best_avg, 6)}.")
        if best_avg > baseline_36:
            notes.append("A narrowed NEAR subset beats the exact fixed-36 baseline average and warrants further exit testing.")
        else:
            notes.append("No narrowed NEAR subset beats the exact fixed-36 baseline average in this pass.")
    return notes


def _coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, str | None]:
    timestamps = [_parse_datetime(row.get("timestamp")) for row in rows if row.get("timestamp")]
    return {
        "start": min(timestamps).isoformat() if timestamps else None,
        "end": max(timestamps).isoformat() if timestamps else None,
    }


def _threshold_name(threshold: float) -> str:
    return f"near_gte_{str(threshold).replace('.', '_')}"


def _profit_factor_proxy(positives: Sequence[float], negatives: Sequence[float]) -> float | None:
    if not negatives:
        return math.inf if positives else None
    return _round(sum(positives) / abs(sum(negatives)))


def _max_drawdown(returns: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _year_quarter(timestamp: datetime) -> str:
    quarter = ((timestamp.month - 1) // 3) + 1
    return f"{timestamp.year}Q{quarter}"


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(resolved):
        return None
    return resolved


def _round(value: float | int | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isinf(value):
        return value
    return round(float(value), digits)


def _reject_non_research_path(path: Path) -> None:
    text = str(path)
    blocked = ("/runtime/", "operator_dashboard", "paper_session", "broker_truth", "paper_leak_test")
    if any(token in text for token in blocked):
        raise ValueError("refusing non-research/live/runtime-like source path.")
    if "entry_acceptance_research" not in text:
        raise ValueError("source must be an Entry Acceptance research artifact path.")


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and math.isinf(value):
        return "Infinity"
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B NEAR Candidate Narrowing Study v1",
        "",
        f"- mode: `{report['mode']}`",
        f"- rows_scanned: `{report['rows_scanned']}`",
        f"- near_candidate_count: `{report['near_candidate_count']}`",
        f"- coverage: `{report['coverage']}`",
        f"- authority_flags: `{report['authority_flags']}`",
        "",
        "## Score Threshold Sweep",
        "",
        "| Subset | Candidates | Episodes | 24b Avg | 24b PF | 36b Avg | 36b PF |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, result in report["score_threshold_sweep"].items():
        lines.append(_summary_table_row(name, result))
    lines.extend(
        [
            "",
            "## Conservative Subsets",
            "",
            "| Subset | Candidates | Episodes | 24b Avg | 24b PF | 36b Avg | 36b PF |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, result in report["conservative_subsets"].items():
        lines.append(_summary_table_row(name, result))
    lines.extend(
        [
            "",
            "## Gap Analysis",
            "",
            f"- primary_gap_counts: `{report['exact_rule_gap_analysis']['primary_gap_counts']}`",
            f"- all_gap_flag_counts: `{report['exact_rule_gap_analysis']['all_gap_flag_counts']}`",
            "",
            "## Interpretation",
            "",
            *[f"- {note}" for note in report["interpretation"]],
            "",
            "Safety: research/offline only; no broker commands, lane execution, paper proof, or strategy behavior changes.",
            "",
        ]
    )
    return "\n".join(lines)


def _summary_table_row(name: str, result: Mapping[str, Any]) -> str:
    fixed_24 = result.get("results", {}).get("fixed_24b", {})
    fixed_36 = result.get("results", {}).get("fixed_36b", {})
    return (
        f"| {name} | {result.get('candidate_count')} | {result.get('episode_count')} | "
        f"{fixed_24.get('average_return')} | {fixed_24.get('profit_factor_proxy')} | "
        f"{fixed_36.get('average_return')} | {fixed_36.get('profit_factor_proxy')} |"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    report = build_near_narrowing_study(input_root=args.input_root, output_root=args.output_root)
    print(
        json.dumps(
            {
                "report_json": report["report_json"],
                "report_markdown": report["report_markdown"],
                "near_candidate_count": report["near_candidate_count"],
                "coverage": report["coverage"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
