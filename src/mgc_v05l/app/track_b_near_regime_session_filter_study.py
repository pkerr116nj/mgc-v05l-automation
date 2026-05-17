"""Research-only regime/session filter study for Track B NEAR candidates."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.app.track_b_near_candidate_narrowing_study import (
    AUTHORITY_FLAGS,
    COST_POINTS,
    DEDUPE_COOLDOWN_BARS,
    DEFAULT_INPUT_ROOT,
    NEAR_CLASS,
    _acceptable_range,
    _dedupe_candidates,
    _discover_archive_paths,
    _float_or_none,
    _gap_flags,
    _json_ready,
    _load_archive_rows,
    _parse_datetime,
    _profit_factor_proxy,
    _reject_non_research_path,
    _rows_by_instrument,
    _score_rows,
    _signal_candle,
    _structural_soft_fail,
    _trade,
    _trade_stats,
    _year_quarter,
)


SCHEMA_VERSION = "track_b_near_regime_session_filter_study_v1"
RESEARCH_MODE = "RESEARCH_OFFLINE_ONLY"
DEFAULT_OUTPUT_ROOT = Path("outputs/reports/entry_acceptance_research/full_history_batch/near_regime_session_filter_study")
NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class RegimeCandidate:
    row: dict[str, Any]
    scored: dict[str, Any]
    instrument: str
    timestamp: datetime
    bar_index: int
    acceptance_score: float
    gap_flags: tuple[str, ...]
    primary_gap: str
    bucket: str


def build_near_regime_session_filter_study(
    *,
    input_root: Path = DEFAULT_INPUT_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    """Run branch-D style NEAR regime/session filter diagnostics."""

    input_root = Path(input_root)
    output_root = Path(output_root)
    _reject_non_research_path(input_root)
    archive_paths = _discover_archive_paths(input_root)
    rows = _load_archive_rows(archive_paths)
    scored_rows = _score_rows(rows)
    rows_by_instrument = _rows_by_instrument(rows)
    candidates = _candidate_records(rows=rows, scored_rows=scored_rows)

    bucket_predicates = _bucket_predicates()
    buckets = {
        name: [candidate for candidate in candidates if predicate(candidate)]
        for name, predicate in bucket_predicates.items()
    }
    regime_filters = _regime_filters(candidates)
    bucket_results = {
        bucket_name: _bucket_report(
            candidates=bucket_candidates,
            rows_by_instrument=rows_by_instrument,
            regime_filters=regime_filters,
        )
        for bucket_name, bucket_candidates in buckets.items()
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
            "entry": "next_bar_open",
            "exits": ["fixed_24b", "fixed_36b", "adaptive_24_36"],
            "round_trip_cost_points": COST_POINTS,
            "dedupe_cooldown_bars": DEDUPE_COOLDOWN_BARS,
            "time_bucket_timezone": "America/New_York",
            "time_bucket_definitions": _time_bucket_definitions(),
        },
        "authority_flags": dict(AUTHORITY_FLAGS),
        "candidate_counts": dict(sorted(Counter(candidate.bucket for candidate in candidates).items())),
        "bucket_results": bucket_results,
        "baseline_reference": {
            "exact_baseline_fixed_36b": {
                "episodes": 856,
                "average_return": 0.306308,
                "profit_factor_proxy": 1.107389,
                "max_drawdown_proxy": 254.5,
            },
            "exact_baseline_adaptive_24_36": {
                "episodes": 856,
                "average_return": 0.273832,
                "profit_factor_proxy": 1.113051,
                "max_drawdown_proxy": 228.2,
            },
        },
        "interpretation": _interpretation(bucket_results),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "near_regime_session_filter_study_v1.json"
    md_path = output_root / "near_regime_session_filter_study_v1.md"
    report["report_json"] = str(json_path)
    report["report_markdown"] = str(md_path)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    md_path.write_text(_markdown_report(report), encoding="utf-8")
    return report


def _candidate_records(
    *,
    rows: Sequence[Mapping[str, Any]],
    scored_rows: Sequence[Mapping[str, Any]],
) -> list[RegimeCandidate]:
    counters: dict[str, int] = {}
    records: list[RegimeCandidate] = []
    for row, scored in zip(rows, scored_rows, strict=False):
        instrument = str(row.get("instrument"))
        index = counters.get(instrument, 0)
        counters[instrument] = index + 1
        flags = tuple(_gap_flags(row=row, scored=scored))
        acceptance_class = str(scored.get("acceptance_class"))
        exact = row.get("current_exact_rule_flag") is True
        if exact:
            bucket = "exact_baseline_reference"
        elif acceptance_class == NEAR_CLASS:
            bucket = "near_candidate"
        else:
            continue
        records.append(
            RegimeCandidate(
                row=dict(row),
                scored=dict(scored),
                instrument=instrument,
                timestamp=_parse_datetime(row.get("timestamp")),
                bar_index=index,
                acceptance_score=_float_or_none(scored.get("acceptance_score")) or 0.0,
                gap_flags=flags,
                primary_gap=_primary_gap(flags),
                bucket=bucket,
            )
        )
    return records


def _bucket_predicates() -> dict[str, Callable[[RegimeCandidate], bool]]:
    return {
        "NEAR_gte_0_80": lambda item: item.bucket == "near_candidate"
        and item.acceptance_score >= 0.80,
        "soft_retest_hold_miss_only": lambda item: item.bucket == "near_candidate"
        and item.acceptance_score >= 0.80
        and set(item.gap_flags).issubset({"soft_retest_hold_miss", "marginal_range_expansion"}),
        "structural_soft_fail": lambda item: item.bucket == "near_candidate"
        and item.acceptance_score >= 0.80
        and _structural_soft_fail(item.row, item.gap_flags),
        "exact_baseline_reference": lambda item: item.bucket == "exact_baseline_reference",
    }


def _bucket_report(
    *,
    candidates: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
    regime_filters: Mapping[str, Callable[[RegimeCandidate], bool]],
) -> dict[str, Any]:
    filtered_candidates = {
        name: [item for item in candidates if predicate(item)]
        for name, predicate in regime_filters.items()
    }
    return {
        "overall": _evaluate(candidates=candidates, rows_by_instrument=rows_by_instrument),
        "by_instrument": {
            instrument: _evaluate(
                candidates=[item for item in candidates if item.instrument == instrument],
                rows_by_instrument=rows_by_instrument,
            )
            for instrument in ("GC", "MGC")
        },
        "by_year": _grouped_report(candidates, rows_by_instrument, lambda item: str(item.timestamp.year)),
        "by_year_quarter": _grouped_report(candidates, rows_by_instrument, lambda item: _year_quarter(item.timestamp)),
        "by_time_bucket": _grouped_report(candidates, rows_by_instrument, _time_bucket),
        "by_regime_filter": {
            name: _evaluate(
                candidates=filtered,
                rows_by_instrument=rows_by_instrument,
            )
            for name, filtered in filtered_candidates.items()
        },
        "regime_filter_stability": {
            name: _stability_report(candidates=filtered, rows_by_instrument=rows_by_instrument)
            for name, filtered in filtered_candidates.items()
        },
    }


def _evaluate(
    *,
    candidates: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    episodes = _dedupe_candidates(candidates, cooldown=DEDUPE_COOLDOWN_BARS)
    fixed_24 = [
        trade
        for trade in (_trade(candidate, rows_by_instrument=rows_by_instrument, horizon=24) for candidate in episodes)
        if trade is not None
    ]
    fixed_36 = [
        trade
        for trade in (_trade(candidate, rows_by_instrument=rows_by_instrument, horizon=36) for candidate in episodes)
        if trade is not None
    ]
    adaptive = [
        trade
        for trade in (_adaptive_trade(candidate, rows_by_instrument=rows_by_instrument) for candidate in episodes)
        if trade is not None
    ]
    return {
        "candidate_count": len(candidates),
        "episode_count": len(episodes),
        "gap_primary_counts": dict(sorted(Counter(item.primary_gap for item in candidates).items())),
        "time_bucket_counts": dict(sorted(Counter(_time_bucket(item) for item in candidates).items())),
        "results": {
            "fixed_24b": _trade_stats(fixed_24),
            "fixed_36b": _trade_stats(fixed_36),
            "adaptive_24_36": _trade_stats(adaptive),
        },
        "adaptive_branch_counts": dict(sorted(Counter(str(trade["exit_branch"]) for trade in adaptive).items())),
    }


def _adaptive_trade(
    candidate: RegimeCandidate,
    *,
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any] | None:
    rows = rows_by_instrument.get(candidate.instrument, ())
    entry_index = candidate.bar_index + 1
    decision_index = entry_index + 24 - 1
    extension_index = entry_index + 36 - 1
    if entry_index >= len(rows) or decision_index >= len(rows):
        return None
    entry_candle = _signal_candle(rows[entry_index])
    decision_candle = _signal_candle(rows[decision_index])
    entry_price = _float_or_none(entry_candle.get("open"))
    decision_close = _float_or_none(decision_candle.get("close"))
    if entry_price is None or decision_close is None:
        return None
    window = [_signal_candle(row) for row in rows[entry_index : decision_index + 1]]
    highs = [_float_or_none(row.get("high")) for row in window]
    lows = [_float_or_none(row.get("low")) for row in window]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    if not valid_highs or not valid_lows:
        return None
    progress = decision_close - entry_price
    mfe = max(valid_highs) - entry_price
    adverse_mae = max(0.0, entry_price - min(valid_lows))
    ratio = math.inf if adverse_mae <= 0 and mfe > 0 else (mfe / adverse_mae if adverse_mae > 0 else 0.0)
    giveback = 0.0 if mfe <= 0 and progress > 0 else (1.0 if mfe <= 0 else max(0.0, mfe - max(progress, 0.0)) / mfe)
    horizon = 36 if progress > 0 and ratio >= 1.1 and giveback <= 0.4 and extension_index < len(rows) else 24
    trade = _trade(candidate, rows_by_instrument=rows_by_instrument, horizon=horizon)
    if trade is not None:
        trade["exit_branch"] = "extend_to_36b" if horizon == 36 else "exit_24b"
    return trade


def _grouped_report(
    candidates: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
    key_func: Callable[[RegimeCandidate], str],
) -> dict[str, Any]:
    keys = sorted({key_func(item) for item in candidates})
    return {
        key: _evaluate(
            candidates=[item for item in candidates if key_func(item) == key],
            rows_by_instrument=rows_by_instrument,
        )
        for key in keys
    }


def _stability_report(
    *,
    candidates: Sequence[RegimeCandidate],
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    by_year = _grouped_report(candidates, rows_by_instrument, lambda item: str(item.timestamp.year))
    by_quarter = _grouped_report(candidates, rows_by_instrument, lambda item: _year_quarter(item.timestamp))
    return {
        "by_year": _period_stability(by_year),
        "by_year_quarter": _period_stability(by_quarter),
    }


def _period_stability(period_results: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for exit_name in ("fixed_24b", "fixed_36b", "adaptive_24_36"):
        period_rows = []
        for period, payload in period_results.items():
            stats = payload.get("results", {}).get(exit_name, {})
            trade_count = int(stats.get("trade_count") or 0)
            avg = _float_or_none(stats.get("average_return"))
            if trade_count <= 0 or avg is None:
                continue
            period_rows.append(
                {
                    "period": period,
                    "trade_count": trade_count,
                    "average_return": avg,
                    "profit_factor_proxy": stats.get("profit_factor_proxy"),
                    "max_drawdown_proxy": stats.get("max_drawdown_proxy"),
                }
            )
        positive = [row for row in period_rows if row["average_return"] > 0]
        output[exit_name] = {
            "periods_traded": len(period_rows),
            "positive_avg_periods": len(positive),
            "positive_avg_share": round(len(positive) / len(period_rows), 6) if period_rows else None,
            "best_period": max(period_rows, key=lambda row: row["average_return"]) if period_rows else None,
            "worst_period": min(period_rows, key=lambda row: row["average_return"]) if period_rows else None,
        }
    return output


def _regime_filters(candidates: Sequence[RegimeCandidate]) -> dict[str, Callable[[RegimeCandidate], bool]]:
    atr_values = [_atr(item) for item in candidates]
    atr_values = [value for value in atr_values if value is not None]
    atr_low, atr_high = _terciles(atr_values)
    return {
        "time_early_asia_first_segment": lambda item: _time_bucket(item) == "early_asia_first_segment",
        "time_later_asia": lambda item: _time_bucket(item) == "later_asia",
        "time_london_overlap_open_edge": lambda item: _time_bucket(item) == "london_overlap_open_edge",
        "instrument_GC": lambda item: item.instrument == "GC",
        "instrument_MGC": lambda item: item.instrument == "MGC",
        "atr_low_tercile": lambda item: (_atr(item) is not None and _atr(item) <= atr_low),
        "atr_mid_tercile": lambda item: (_atr(item) is not None and atr_low < _atr(item) <= atr_high),
        "atr_high_tercile": lambda item: (_atr(item) is not None and _atr(item) > atr_high),
        "range_compressed": lambda item: (_range_ratio(item) is not None and _range_ratio(item) < 0.85),
        "range_normal": lambda item: (_range_ratio(item) is not None and 0.85 <= _range_ratio(item) <= 1.25),
        "range_expanded": lambda item: (_range_ratio(item) is not None and _range_ratio(item) > 1.25),
        "low_churn_only": lambda item: (_float_or_none(item.row.get("churn_score")) or 0.0) < 0.25,
        "no_snap_turn_conflict": lambda item: (_float_or_none(item.row.get("snap_turn_conflict_strength")) or 0.0) < 0.50,
        "no_wrong_side_directional_contradiction": lambda item: "wrong_side_directional_contradiction" not in item.gap_flags,
        "no_elevated_failure_risk": lambda item: _no_elevated_failure_risk(item),
    }


def _no_elevated_failure_risk(item: RegimeCandidate) -> bool:
    return (
        "wrong_side_directional_contradiction" not in item.gap_flags
        and "anti_churn_snap_turn_conflict" not in item.gap_flags
        and "missing_provenance_issue" not in item.gap_flags
        and (_float_or_none(item.row.get("churn_score")) or 0.0) < 0.25
        and (_float_or_none(item.row.get("snap_turn_conflict_strength")) or 0.0) < 0.50
        and _acceptable_range(item.row)
    )


def _time_bucket(item: RegimeCandidate) -> str:
    local = item.timestamp.astimezone(NY_TZ)
    minutes = local.hour * 60 + local.minute
    if 18 * 60 <= minutes < 20 * 60:
        return "early_asia_first_segment"
    if minutes >= 20 * 60 or minutes < 2 * 60:
        return "later_asia"
    if 2 * 60 <= minutes < 4 * 60:
        return "london_overlap_open_edge"
    return "other_time_window"


def _time_bucket_definitions() -> dict[str, str]:
    return {
        "early_asia_first_segment": "18:00-20:00 America/New_York",
        "later_asia": "20:00-02:00 America/New_York",
        "london_overlap_open_edge": "02:00-04:00 America/New_York",
        "other_time_window": "outside the explicit study windows",
    }


def _atr(item: RegimeCandidate) -> float | None:
    source = item.row.get("source_feature_values")
    if not isinstance(source, Mapping):
        return None
    return _float_or_none(source.get("atr"))


def _range_ratio(item: RegimeCandidate) -> float | None:
    return _float_or_none(item.row.get("range_expansion_ratio"))


def _terciles(values: Sequence[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    ordered = sorted(values)
    return ordered[len(ordered) // 3], ordered[(len(ordered) * 2) // 3]


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


def _coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, str | None]:
    timestamps = [_parse_datetime(row.get("timestamp")) for row in rows if row.get("timestamp")]
    return {
        "start": min(timestamps).isoformat() if timestamps else None,
        "end": max(timestamps).isoformat() if timestamps else None,
    }


def _best_result(bucket_results: Mapping[str, Any]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for bucket_name, bucket in bucket_results.items():
        for section_name, section in {"overall": bucket["overall"], **bucket["by_regime_filter"]}.items():
            for exit_name, stats in section.get("results", {}).items():
                avg = _float_or_none(stats.get("average_return"))
                if avg is None:
                    continue
                if best is None or avg > best["average_return"]:
                    best = {
                        "bucket": bucket_name,
                        "section": section_name,
                        "exit": exit_name,
                        "average_return": avg,
                        "profit_factor_proxy": stats.get("profit_factor_proxy"),
                        "max_drawdown_proxy": stats.get("max_drawdown_proxy"),
                        "trade_count": stats.get("trade_count"),
                    }
    return best


def _interpretation(bucket_results: Mapping[str, Any]) -> list[str]:
    best = _best_result(bucket_results)
    notes = [
        "This is a research/offline study over NEAR candidates and exact baseline references only.",
        "All returns are next-bar-open with 0.5 point round-trip cost and 12-bar de-duped episodes.",
    ]
    if best is None:
        notes.append("No evaluated subset produced complete trades.")
        return notes
    notes.append(
        "Best studied subset is "
        f"{best['bucket']} / {best['section']} / {best['exit']} "
        f"avg {round(best['average_return'], 6)} PF {best['profit_factor_proxy']} DD {best['max_drawdown_proxy']}."
    )
    if best["average_return"] > 0.306308 and (best.get("trade_count") or 0) >= 30:
        notes.append("At least one subset beats exact fixed 36b average with a minimally useful sample size.")
    else:
        notes.append("No NEAR subset with a minimally useful sample size beats exact fixed 36b in this pass.")
    return notes


def _markdown_report(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B NEAR Regime/Session Filter Study v1",
        "",
        f"- mode: `{report['mode']}`",
        f"- rows_scanned: `{report['rows_scanned']}`",
        f"- coverage: `{report['coverage']}`",
        f"- candidate_counts: `{report['candidate_counts']}`",
        f"- authority_flags: `{report['authority_flags']}`",
        "",
        "## Overall Bucket Results",
        "",
        "| Bucket | Candidates | Episodes | 24b Avg/PF/DD | 36b Avg/PF/DD | Adaptive Avg/PF/DD | Adaptive Branches |",
        "| --- | ---: | ---: | --- | --- | --- | --- |",
    ]
    for bucket_name, payload in report["bucket_results"].items():
        lines.append(_bucket_row(bucket_name, payload["overall"]))
    lines.extend(
        [
            "",
            "## Best Regime Slices",
            "",
            "| Bucket | Slice | Exit | Trades | Avg | PF | DD |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for bucket_name, payload in report["bucket_results"].items():
        for row in _top_slice_rows(bucket_name, payload["by_regime_filter"]):
            lines.append(row)
    lines.extend(
        [
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


def _bucket_row(bucket_name: str, result: Mapping[str, Any]) -> str:
    results = result["results"]
    return (
        f"| {bucket_name} | {result['candidate_count']} | {result['episode_count']} | "
        f"{_triple(results['fixed_24b'])} | {_triple(results['fixed_36b'])} | "
        f"{_triple(results['adaptive_24_36'])} | `{result['adaptive_branch_counts']}` |"
    )


def _top_slice_rows(bucket_name: str, slices: Mapping[str, Any]) -> list[str]:
    ranked: list[tuple[float, str]] = []
    for slice_name, result in slices.items():
        for exit_name, stats in result.get("results", {}).items():
            avg = _float_or_none(stats.get("average_return"))
            if avg is not None:
                ranked.append((avg, _slice_row(bucket_name, slice_name, exit_name, stats)))
    return [row for _, row in sorted(ranked, reverse=True)[:8]]


def _slice_row(bucket_name: str, slice_name: str, exit_name: str, stats: Mapping[str, Any]) -> str:
    return (
        f"| {bucket_name} | {slice_name} | {exit_name} | {stats.get('trade_count')} | "
        f"{stats.get('average_return')} | {stats.get('profit_factor_proxy')} | {stats.get('max_drawdown_proxy')} |"
    )


def _triple(stats: Mapping[str, Any]) -> str:
    return f"{stats.get('average_return')}/{stats.get('profit_factor_proxy')}/{stats.get('max_drawdown_proxy')}"


def _sharpe_like(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    avg = sum(values) / len(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    std = math.sqrt(variance)
    if std == 0:
        return math.inf if avg > 0 else None
    return round(avg / std, 6)


def _median(values: Sequence[float]) -> float | None:
    return round(median(values), 6) if values else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    report = build_near_regime_session_filter_study(input_root=args.input_root, output_root=args.output_root)
    print(
        json.dumps(
            {
                "report_json": report["report_json"],
                "report_markdown": report["report_markdown"],
                "candidate_counts": report["candidate_counts"],
                "coverage": report["coverage"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
