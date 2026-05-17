"""Research-only replay evaluator for Track B entry/exit candidates.

This command evaluates registered research candidates against historical
warehouse bars/features. It is offline only and does not modify strategy,
PAPER/live, broker, lane, or lifecycle state.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.research.track_b_entry_exit_candidates import (
    TrackBEntryExitResearchCandidate,
    get_track_b_entry_exit_research_candidate,
    track_b_entry_exit_research_candidates,
)
from mgc_v05l.research.warehouse_historical_evaluator._warehouse_common import read_parquet_rows


EXACT_FLAG_COLUMN = "asia_early_normal_breakout_retest_hold_long_turn_candidate"
TIMEFRAME = "5m"
DEDUPE_COOLDOWN_BARS = 12
RESEARCH_MODE = "TRACK_B_ENTRY_EXIT_CANDIDATE_REPLAY_RESEARCH_ONLY"
SCHEMA_VERSION = "track_b_entry_exit_candidate_replay_v1"
DECISION_NOTE_PATH = "docs/track_b_entry_acceptance_candidate_universe_decision.md"
UNIVERSE_DECISION = {
    "EXACT_BASELINE_RETAINED": True,
    "NEAR_EXPANSION_PARKED": True,
    "EXIT_PROFILE_PROMOTION_CONTINUES": True,
}
AUTHORITY_FLAGS = {
    "research_offline_only": True,
    "strategy_authority": False,
    "broker_state_mutated": False,
    "submit_attempted": False,
    "order_intent_created": False,
    "lifecycle_mutated": False,
    "runtime_trade_eligible": False,
    "paper_eligible": False,
    "live_eligible": False,
    "strategy_behavior_changed": False,
}


@dataclass(frozen=True)
class WarehousePartition:
    instrument: str
    bars_path: Path
    features_path: Path
    year: str
    shard_id: str


@dataclass(frozen=True)
class ExactEpisode:
    instrument: str
    timestamp: datetime
    bar_index: int
    year: str
    shard_id: str


def build_replay_report(
    *,
    warehouse_root: Path,
    output_root: Path,
    candidate_ids: Sequence[str] | None = None,
    instruments: Sequence[str] = ("GC", "MGC"),
    year: str | None = None,
    shard_id: str | None = None,
) -> dict[str, Any]:
    """Evaluate registered Track B entry/exit candidates over warehouse partitions."""

    warehouse_root = Path(warehouse_root)
    output_root = Path(output_root)
    _reject_non_research_warehouse_root(warehouse_root)
    candidates = _resolve_candidates(candidate_ids)
    partitions = _discover_partitions(
        warehouse_root=warehouse_root,
        instruments=instruments,
        year=year,
        shard_id=shard_id,
    )
    episodes_by_partition: list[dict[str, Any]] = []
    episodes_by_instrument: dict[str, list[ExactEpisode]] = {}
    bars_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for partition in partitions:
        bars = _read_bars(partition.bars_path)
        features = _read_features(partition.features_path)
        episodes = _exact_episodes(
            instrument=partition.instrument,
            bars=bars,
            features=features,
            year=partition.year,
            shard_id=partition.shard_id,
        )
        bars_by_instrument.setdefault(partition.instrument, []).extend(bars)
        episodes_by_instrument.setdefault(partition.instrument, []).extend(episodes)
        episodes_by_partition.append(
            {
                "instrument": partition.instrument,
                "year": partition.year,
                "shard_id": partition.shard_id,
                "bars": len(bars),
                "exact_flag_rows": len(episodes),
            }
        )

    normalized_bars = {
        instrument: sorted(rows, key=lambda row: _parse_datetime(row["bar_ts"]))
        for instrument, rows in bars_by_instrument.items()
    }
    deduped_episodes = {
        instrument: _dedupe_episodes(episodes, cooldown=DEDUPE_COOLDOWN_BARS)
        for instrument, episodes in episodes_by_instrument.items()
    }
    candidate_results = {
        candidate.candidate_id: _evaluate_candidate(
            candidate=candidate,
            bars_by_instrument=normalized_bars,
            episodes_by_instrument=deduped_episodes,
        )
        for candidate in candidates
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "mode": RESEARCH_MODE,
        "warehouse_root": str(warehouse_root),
        "output_root": str(output_root),
        "candidate_ids": [candidate.candidate_id for candidate in candidates],
        "candidate_universe_decision_note": DECISION_NOTE_PATH,
        "candidate_universe_decision": dict(UNIVERSE_DECISION),
        "timeframe": TIMEFRAME,
        "entry_semantics": "next_bar_open_5m",
        "eligible_rows": "exact_rule_flag_true_only",
        "dedupe_cooldown_bars": DEDUPE_COOLDOWN_BARS,
        "authority_flags": dict(AUTHORITY_FLAGS),
        "partitions": episodes_by_partition,
        "episode_counts": {
            instrument: len(episodes)
            for instrument, episodes in sorted(deduped_episodes.items())
        },
        "raw_exact_flag_counts": {
            instrument: len(episodes)
            for instrument, episodes in sorted(episodes_by_instrument.items())
        },
        "candidate_results": candidate_results,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "track_b_entry_exit_candidate_replay_v1.json"
    md_path = output_root / "track_b_entry_exit_candidate_replay_v1.md"
    report["report_json"] = str(json_path)
    report["report_markdown"] = str(md_path)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    md_path.write_text(_markdown_report(report), encoding="utf-8")
    return report


def _resolve_candidates(candidate_ids: Sequence[str] | None) -> tuple[TrackBEntryExitResearchCandidate, ...]:
    if not candidate_ids:
        return track_b_entry_exit_research_candidates()
    return tuple(get_track_b_entry_exit_research_candidate(candidate_id) for candidate_id in candidate_ids)


def _discover_partitions(
    *,
    warehouse_root: Path,
    instruments: Sequence[str],
    year: str | None,
    shard_id: str | None,
) -> list[WarehousePartition]:
    partitions: list[WarehousePartition] = []
    for instrument in instruments:
        normalized = instrument.upper()
        pattern = f"datasets/derived_bars_5m/symbol={normalized}/year=*/shard_id=*/bars.parquet"
        for bars_path in sorted(warehouse_root.glob(pattern)):
            parts = bars_path.parts
            discovered_year = _partition_value(parts, "year")
            discovered_shard = _partition_value(parts, "shard_id")
            if year is not None and discovered_year != str(year):
                continue
            if shard_id is not None and discovered_shard != str(shard_id):
                continue
            features_path = (
                warehouse_root
                / "datasets"
                / "shared_features_5m"
                / f"symbol={normalized}"
                / f"year={discovered_year}"
                / f"shard_id={discovered_shard}"
                / "features.parquet"
            )
            if features_path.exists():
                partitions.append(
                    WarehousePartition(
                        instrument=normalized,
                        bars_path=bars_path,
                        features_path=features_path,
                        year=discovered_year,
                        shard_id=discovered_shard,
                    )
                )
    if not partitions:
        raise FileNotFoundError("no compatible Entry Acceptance warehouse partitions found.")
    return partitions


def _read_bars(path: Path) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in read_parquet_rows(path)
        if str(row.get("timeframe") or TIMEFRAME) == TIMEFRAME
    ]
    return sorted(rows, key=lambda row: _parse_datetime(row["bar_ts"]))


def _read_features(path: Path) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in read_parquet_rows(path)],
        key=lambda row: _parse_datetime(row["decision_ts"]),
    )


def _exact_episodes(
    *,
    instrument: str,
    bars: Sequence[Mapping[str, Any]],
    features: Sequence[Mapping[str, Any]],
    year: str,
    shard_id: str,
) -> list[ExactEpisode]:
    index_by_ts = {_iso(row["bar_ts"]): index for index, row in enumerate(bars)}
    episodes: list[ExactEpisode] = []
    for row in features:
        if bool(row.get(EXACT_FLAG_COLUMN)) is not True:
            continue
        ts = _iso(row.get("decision_ts"))
        if ts not in index_by_ts:
            continue
        episodes.append(
            ExactEpisode(
                instrument=instrument,
                timestamp=_parse_datetime(ts),
                bar_index=index_by_ts[ts],
                year=year,
                shard_id=shard_id,
            )
        )
    return episodes


def _dedupe_episodes(episodes: Sequence[ExactEpisode], *, cooldown: int) -> list[ExactEpisode]:
    selected: list[ExactEpisode] = []
    last_index: int | None = None
    for episode in sorted(episodes, key=lambda item: (item.timestamp, item.bar_index)):
        if last_index is None or episode.bar_index - last_index > cooldown:
            selected.append(episode)
            last_index = episode.bar_index
    return selected


def _evaluate_candidate(
    *,
    candidate: TrackBEntryExitResearchCandidate,
    bars_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
    episodes_by_instrument: Mapping[str, Sequence[ExactEpisode]],
) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    for instrument, episodes in episodes_by_instrument.items():
        bars = bars_by_instrument.get(instrument, ())
        for episode in episodes:
            trade = _candidate_trade(candidate=candidate, episode=episode, bars=bars)
            if trade is not None:
                trades.append(trade)
    by_instrument = {
        instrument: _trade_stats([trade for trade in trades if trade["instrument"] == instrument])
        for instrument in sorted({str(trade["instrument"]) for trade in trades})
    }
    return {
        "candidate_id": candidate.candidate_id,
        "label": candidate.label,
        "status": candidate.status,
        "authority_flags": dict(AUTHORITY_FLAGS),
        "episodes": len(trades),
        "stats": _trade_stats(trades),
        "branch_counts": dict(Counter(str(trade.get("exit_branch") or "unknown") for trade in trades)),
        "by_instrument": by_instrument,
    }


def _candidate_trade(
    *,
    candidate: TrackBEntryExitResearchCandidate,
    episode: ExactEpisode,
    bars: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    exit_type = str(candidate.exit_definition.get("exit_type") or "")
    if exit_type == "fixed_horizon":
        horizon = int(candidate.exit_definition["exit_after_completed_bars"])
        return _fixed_horizon_trade(episode=episode, bars=bars, horizon=horizon, exit_branch=f"fixed_{horizon}b")
    if exit_type == "adaptive_24_to_36_extension":
        return _adaptive_24_to_36_trade(candidate=candidate, episode=episode, bars=bars)
    raise ValueError(f"Unsupported entry/exit candidate exit_type: {exit_type}")


def _fixed_horizon_trade(
    *,
    episode: ExactEpisode,
    bars: Sequence[Mapping[str, Any]],
    horizon: int,
    exit_branch: str,
) -> dict[str, Any] | None:
    entry_index = episode.bar_index + 1
    exit_index = entry_index + horizon - 1
    if entry_index >= len(bars) or exit_index >= len(bars):
        return None
    entry_bar = bars[entry_index]
    exit_bar = bars[exit_index]
    entry_price = _float_or_none(entry_bar.get("open"))
    exit_price = _float_or_none(exit_bar.get("close"))
    if entry_price is None or exit_price is None:
        return None
    window = bars[entry_index : exit_index + 1]
    excursions = _window_excursions(window=window, entry_price=entry_price)
    if excursions is None:
        return None
    return {
        "instrument": episode.instrument,
        "candidate_timestamp": episode.timestamp,
        "entry_timestamp": _parse_datetime(entry_bar["bar_ts"]),
        "exit_timestamp": _parse_datetime(exit_bar["bar_ts"]),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_return": exit_price - entry_price,
        "mfe": excursions["mfe"],
        "mae": -excursions["adverse_mae"],
        "adverse_mae": excursions["adverse_mae"],
        "horizon_bars": horizon,
        "exit_branch": exit_branch,
        "year": episode.year,
        "shard_id": episode.shard_id,
    }


def _adaptive_24_to_36_trade(
    *,
    candidate: TrackBEntryExitResearchCandidate,
    episode: ExactEpisode,
    bars: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    default_horizon = int(candidate.exit_definition["default_exit_after_completed_bars"])
    extension_horizon = int(candidate.exit_definition["extension_exit_after_completed_bars"])
    entry_index = episode.bar_index + 1
    decision_index = entry_index + default_horizon - 1
    extension_index = entry_index + extension_horizon - 1
    if entry_index >= len(bars) or decision_index >= len(bars):
        return None
    entry_price = _float_or_none(bars[entry_index].get("open"))
    decision_close = _float_or_none(bars[decision_index].get("close"))
    if entry_price is None or decision_close is None:
        return None
    decision_window = bars[entry_index : decision_index + 1]
    decision_excursions = _window_excursions(window=decision_window, entry_price=entry_price)
    if decision_excursions is None:
        return None
    diagnostics = _extension_diagnostics(
        progress=decision_close - entry_price,
        mfe=decision_excursions["mfe"],
        adverse_mae=decision_excursions["adverse_mae"],
    )
    conditions = candidate.exit_definition["extension_conditions"]
    should_extend = (
        diagnostics["progress"] > float(conditions["net_progress_at_24b_points_gt"])
        and diagnostics["mfe_mae_ratio"] >= float(conditions["mfe_mae_ratio_at_24b_gte"])
        and diagnostics["giveback_pct"] <= float(conditions["giveback_pct_at_24b_lte"])
        and extension_index < len(bars)
    )
    horizon = extension_horizon if should_extend else default_horizon
    branch = f"extend_to_{extension_horizon}b" if should_extend else f"exit_{default_horizon}b"
    trade = _fixed_horizon_trade(episode=episode, bars=bars, horizon=horizon, exit_branch=branch)
    if trade is not None:
        trade["adaptive_decision"] = diagnostics
    return trade


def _window_excursions(*, window: Sequence[Mapping[str, Any]], entry_price: float) -> dict[str, float] | None:
    highs = [_float_or_none(row.get("high")) for row in window]
    lows = [_float_or_none(row.get("low")) for row in window]
    valid_highs = [value for value in highs if value is not None]
    valid_lows = [value for value in lows if value is not None]
    if not valid_highs or not valid_lows:
        return None
    return {
        "mfe": max(valid_highs) - entry_price,
        "adverse_mae": max(0.0, entry_price - min(valid_lows)),
    }


def _extension_diagnostics(*, progress: float, mfe: float, adverse_mae: float) -> dict[str, float]:
    if adverse_mae <= 0:
        mfe_mae_ratio = math.inf if mfe > 0 else 0.0
    else:
        mfe_mae_ratio = mfe / adverse_mae
    if mfe <= 0:
        giveback_pct = 0.0 if progress > 0 else 1.0
    else:
        giveback_pct = max(0.0, mfe - max(progress, 0.0)) / mfe
    return {
        "progress": round(progress, 6),
        "mfe": round(mfe, 6),
        "adverse_mae": round(adverse_mae, 6),
        "mfe_mae_ratio": mfe_mae_ratio if math.isinf(mfe_mae_ratio) else round(mfe_mae_ratio, 6),
        "giveback_pct": round(giveback_pct, 6),
    }


def _trade_stats(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    returns = [float(trade["gross_return"]) for trade in trades]
    positives = [value for value in returns if value > 0]
    negatives = [value for value in returns if value < 0]
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
        "median_mfe": _round(median(mfes)) if mfes else None,
        "avg_mae": _round(sum(maes) / len(maes)) if maes else None,
        "median_mae": _round(median(maes)) if maes else None,
    }


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


def _reject_non_research_warehouse_root(path: Path) -> None:
    text = str(path)
    blocked_tokens = ("/runtime/", "operator_dashboard", "paper_leak_test", "paper_session", "broker_truth")
    if any(token in text for token in blocked_tokens):
        raise ValueError("refusing non-research/live/runtime-like source path.")
    if "warehouse_historical_evaluator" not in text:
        raise ValueError("source must be a warehouse_historical_evaluator research artifact.")


def _partition_value(parts: Sequence[str], key: str) -> str:
    prefix = f"{key}="
    for part in parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    raise ValueError(f"partition key missing from path: {key}")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


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
        "# Track B Entry/Exit Candidate Replay v1",
        "",
        f"- mode: `{report['mode']}`",
        f"- timeframe: `{report['timeframe']}`",
        f"- entry_semantics: `{report['entry_semantics']}`",
        f"- eligible_rows: `{report['eligible_rows']}`",
        f"- candidate_universe_decision_note: `{report['candidate_universe_decision_note']}`",
        f"- candidate_universe_decision: `{report['candidate_universe_decision']}`",
        f"- dedupe_cooldown_bars: `{report['dedupe_cooldown_bars']}`",
        f"- authority_flags: `{report['authority_flags']}`",
        "",
        "## Candidate Results",
        "",
    ]
    for candidate_id, payload in report["candidate_results"].items():
        lines.extend(
            [
                f"### {candidate_id}",
                "",
                f"- episodes: `{payload['episodes']}`",
                f"- branch_counts: `{payload['branch_counts']}`",
                f"- stats: `{payload['stats']}`",
                f"- by_instrument: `{payload['by_instrument']}`",
                "",
            ]
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--candidate-id", action="append", dest="candidate_ids")
    parser.add_argument("--instrument", action="append", dest="instruments")
    parser.add_argument("--year", default=None)
    parser.add_argument("--shard-id", default=None)
    args = parser.parse_args(argv)
    report = build_replay_report(
        warehouse_root=args.warehouse_root,
        output_root=args.output_root,
        candidate_ids=args.candidate_ids,
        instruments=tuple(args.instruments or ("GC", "MGC")),
        year=args.year,
        shard_id=args.shard_id,
    )
    print(
        json.dumps(
            {
                "report_json": report["report_json"],
                "report_markdown": report["report_markdown"],
                "candidate_ids": report["candidate_ids"],
                "episode_counts": report["episode_counts"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
