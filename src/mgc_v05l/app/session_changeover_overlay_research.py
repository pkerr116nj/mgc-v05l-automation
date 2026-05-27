"""Research-only session changeover overlay study.

This module measures whether the 03:00 ET and 07:00 ET futures handoffs behave
like distinct liquidity regimes. It reads existing replay/research artifacts and
never creates broker, order, lifecycle, or runtime authority.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from mgc_v05l.session_phase_labels import label_session_phase


NEW_YORK = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
DEFAULT_BAR_ROOT = Path(
    "outputs/reports/entry_acceptance_research/full_history_batch/"
    "warehouse_historical_evaluator_partitions"
)
DEFAULT_OUTPUT_ROOT = Path("outputs/reports/session_changeover_overlay_research")
DEFAULT_DOC_PATH = Path("docs/track_b_session_changeover_overlay_research.md")
CHANGEOVER_SPECS = (
    {"label": "ASIA_TO_EUROPE_CHANGEOVER", "anchor_et": "03:00", "anchor_time": time(3, 0)},
    {"label": "EUROPE_TO_US_CHANGEOVER", "anchor_et": "07:00", "anchor_time": time(7, 0)},
)
WINDOW_SPECS = (
    {"window": "tight", "minutes": 15},
    {"window": "medium", "minutes": 30},
    {"window": "wide", "minutes": 60},
)


@dataclass(frozen=True)
class NormalizedTrade:
    source_path: str
    source_family: str
    strategy_id: str
    lane_id: str
    instrument: str
    direction: str
    entry_ts: datetime
    exit_ts: datetime | None
    entry_price: float | None
    exit_price: float | None
    pnl_cash: float | None
    pnl_points: float | None
    mfe_points: float | None
    mae_points: float | None


def run_session_changeover_overlay_research(
    *,
    repo_root: Path,
    symbols: Sequence[str] | None = None,
    bar_root: Path = DEFAULT_BAR_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    doc_path: Path = DEFAULT_DOC_PATH,
) -> dict[str, Any]:
    """Run the research overlay and write JSON/markdown artifacts."""

    repo_root = Path(repo_root)
    discovered_symbols = tuple(symbols) if symbols is not None else discover_replay_symbols(repo_root=repo_root, bar_root=bar_root)
    bars = load_replay_bars(repo_root=repo_root, symbols=discovered_symbols, bar_root=bar_root)
    trades = load_normalized_trades(repo_root=repo_root)
    summary = build_summary(bars=bars, trades=trades, repo_root=repo_root, symbols=discovered_symbols)

    output_dir = repo_root / output_root
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest_session_changeover_overlay_research.json"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")

    doc_full_path = repo_root / doc_path
    doc_full_path.parent.mkdir(parents=True, exist_ok=True)
    doc_full_path.write_text(render_markdown(summary), encoding="utf-8")

    summary["artifact_paths"] = {
        "json_summary": str(json_path),
        "markdown_report": str(doc_full_path),
    }
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")
    return summary


def discover_replay_symbols(*, repo_root: Path, bar_root: Path) -> tuple[str, ...]:
    root = repo_root / bar_root
    if not root.exists():
        return ()
    symbols = [
        path.name
        for path in root.iterdir()
        if path.is_dir() and any(path.glob("*/datasets/derived_bars_5m/**/bars.parquet"))
    ]
    return tuple(sorted(symbols))


def load_replay_bars(*, repo_root: Path, symbols: Sequence[str], bar_root: Path) -> pd.DataFrame:
    """Load existing 5m replay bars for the requested symbols."""

    frames: list[pd.DataFrame] = []
    root = repo_root / bar_root
    for symbol in symbols:
        for path in sorted((root / symbol).glob("*/datasets/derived_bars_5m/**/bars.parquet")):
            frame = pd.read_parquet(path)
            if frame.empty:
                continue
            frame = frame.copy()
            frame["source_path"] = str(path)
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    bars = pd.concat(frames, ignore_index=True)
    bars["bar_ts"] = pd.to_datetime(bars["bar_ts"], utc=True, errors="coerce")
    bars = bars.dropna(subset=["bar_ts", "open", "high", "low", "close"])
    bars = bars[bars["timeframe"].astype(str).str.lower().eq("5m")]
    bars = bars.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
    return bars


def discover_trade_paths(repo_root: Path) -> list[Path]:
    """Return broad existing trade/replay JSONL paths without depending on one strategy family."""

    roots = [
        repo_root / "outputs/reports",
        repo_root / "outputs/probationary_pattern_engine/paper_session/lanes",
        repo_root / "outputs/research_platform/analytics",
        repo_root / "outputs/research_runtime_bridge",
        repo_root / "outputs/track_b_execution_core/paper_trade_ledger",
    ]
    paths: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        paths.extend(root.rglob("*trades.jsonl"))
        paths.extend(root.rglob("trade_blotter.jsonl"))
        paths.extend(root.rglob("*trade_ledger.jsonl"))
    return sorted({path for path in paths if path.is_file()})


def load_normalized_trades(*, repo_root: Path) -> list[NormalizedTrade]:
    rows: list[NormalizedTrade] = []
    for path in discover_trade_paths(repo_root):
        for raw in _read_jsonl(path):
            trade = normalize_trade(raw, source_path=path)
            if trade is not None:
                rows.append(trade)
    return rows


def normalize_trade(raw: Mapping[str, Any], *, source_path: Path) -> NormalizedTrade | None:
    """Best-effort normalization across existing research/runtime trade artifacts."""

    nested = raw.get("trade_record") if isinstance(raw.get("trade_record"), Mapping) else {}
    entry_ts = _coerce_datetime(
        _first_present(
            raw,
            nested,
            "entry_ts",
            "entry_timestamp",
            "decision_ts",
            "timestamp",
            "submitted_at",
            "created_at",
        )
    )
    if entry_ts is None:
        return None
    exit_ts = _coerce_datetime(_first_present(raw, nested, "exit_ts", "exit_timestamp", "closed_at", "fill_ts"))
    entry_price = _coerce_float(_first_present(raw, nested, "entry_price", "fill_price", "reference_price"))
    exit_price = _coerce_float(_first_present(raw, nested, "exit_price", "close_price"))
    direction = str(_first_present(raw, nested, "direction", "side", default="UNKNOWN") or "UNKNOWN").upper()
    if direction in {"BUY", "LONG_ENTRY"}:
        direction = "LONG"
    elif direction in {"SELL", "SHORT_ENTRY"}:
        direction = "SHORT"
    pnl_cash = _coerce_float(
        _first_present(
            raw,
            nested,
            "pnl_cash",
            "baseline_pnl_cash",
            "realized_pnl",
            "realized_pnl_cash",
            "net_pnl_cash",
        )
    )
    pnl_points = _coerce_float(_first_present(raw, nested, "pnl_points", "net_pnl_points"))
    if pnl_points is None and entry_price is not None and exit_price is not None and direction in {"LONG", "SHORT"}:
        pnl_points = exit_price - entry_price if direction == "LONG" else entry_price - exit_price
    instrument = str(_first_present(raw, nested, "instrument", "symbol", "contract_family", default="UNKNOWN") or "UNKNOWN").upper()
    return NormalizedTrade(
        source_path=str(source_path),
        source_family=_source_family(source_path, raw=raw),
        strategy_id=str(_first_present(raw, nested, "strategy_id", "family", "candidate_family", default="UNKNOWN") or "UNKNOWN"),
        lane_id=str(_first_present(raw, nested, "lane_id", "lane", default=source_path.parent.name) or source_path.parent.name),
        instrument=instrument,
        direction=direction,
        entry_ts=entry_ts,
        exit_ts=exit_ts,
        entry_price=entry_price,
        exit_price=exit_price,
        pnl_cash=pnl_cash,
        pnl_points=pnl_points,
        mfe_points=_coerce_float(_first_present(raw, nested, "mfe_points", "mfe")),
        mae_points=_coerce_float(_first_present(raw, nested, "mae_points", "mae")),
    )


def build_summary(
    *, bars: pd.DataFrame, trades: Sequence[NormalizedTrade], repo_root: Path, symbols: Sequence[str]
) -> dict[str, Any]:
    generated_at = datetime.now(tz=UTC).isoformat()
    bar_summary = summarize_bar_behavior(bars)
    trade_summary = summarize_trade_behavior(trades)
    recommendations = build_recommendations(bar_summary=bar_summary, trade_summary=trade_summary)
    return {
        "schema_version": "session_changeover_overlay_research_v1",
        "generated_at": generated_at,
        "repo_root": str(repo_root),
        "research_only": True,
        "live_trading_authority_changed": False,
        "active_paper_config_changed": False,
        "strategy_thresholds_changed": False,
        "runtime_restarted": False,
        "broker_mutation_allowed": False,
        "submit_allowed": False,
        "paper_proof_invoked": False,
        "symbols_requested": list(symbols),
        "bar_evidence": {
            "row_count": int(len(bars)),
            "symbols": sorted(bars["symbol"].dropna().astype(str).unique().tolist()) if not bars.empty else [],
            "start_ts": _series_min_iso(bars["bar_ts"]) if not bars.empty else None,
            "end_ts": _series_max_iso(bars["bar_ts"]) if not bars.empty else None,
            "changeover_comparisons": bar_summary,
        },
        "trade_evidence": {
            "normalized_trade_count": len(trades),
            "source_path_count": len({trade.source_path for trade in trades}),
            "changeover_comparisons": trade_summary,
        },
        "entry_implications": recommendations["entry_implications"],
        "hold_exit_implications": recommendations["hold_exit_implications"],
        "session_label_recommendation": recommendations["session_label_recommendation"],
        "limitations": [
            "This is a strategy-family agnostic observational overlay on existing replay/research artifacts, not a new backtest engine.",
            "Trade performance attribution depends on fields present in existing JSONL trade artifacts.",
            "Hold-through implications are observational unless an existing trade artifact spans the anchor.",
        ],
    }


def summarize_bar_behavior(bars: pd.DataFrame) -> list[dict[str, Any]]:
    if bars.empty:
        return []
    enriched = bars.copy()
    enriched["range_points"] = enriched["high"] - enriched["low"]
    enriched["body_points"] = (enriched["close"] - enriched["open"]).abs()
    enriched["return_points"] = enriched["close"] - enriched["open"]
    enriched["abs_return_points"] = enriched["return_points"].abs()
    enriched["close_location"] = (enriched["close"] - enriched["low"]) / (enriched["range_points"].replace(0, pd.NA))
    enriched["session_phase"] = enriched["bar_ts"].map(lambda value: label_session_phase(value.to_pydatetime()))
    enriched["next_close"] = enriched.groupby("symbol")["close"].shift(-1)
    enriched["next_return_points"] = enriched["next_close"] - enriched["close"]
    range_thresholds = enriched.groupby("symbol")["range_points"].quantile(0.75).to_dict()

    results: list[dict[str, Any]] = []
    for spec in CHANGEOVER_SPECS:
        offsets = enriched["bar_ts"].map(lambda value: minutes_from_anchor(value.to_pydatetime(), spec["anchor_time"]))
        for window in WINDOW_SPECS:
            minutes = int(window["minutes"])
            inside = enriched[offsets.abs() <= minutes]
            adjacent = enriched[(offsets.abs() > minutes) & (offsets.abs() <= minutes * 2)]
            for symbol in sorted(enriched["symbol"].dropna().astype(str).unique()):
                symbol_inside = inside[inside["symbol"].astype(str).eq(symbol)]
                symbol_adjacent = adjacent[adjacent["symbol"].astype(str).eq(symbol)]
                inside_metrics = _bar_metrics(symbol_inside, range_threshold=range_thresholds.get(symbol, math.inf))
                adjacent_metrics = _bar_metrics(symbol_adjacent, range_threshold=range_thresholds.get(symbol, math.inf))
                results.append(
                    {
                        "changeover_label": spec["label"],
                        "anchor_et": spec["anchor_et"],
                        "window": window["window"],
                        "window_minutes": minutes,
                        "comparison": "inside_vs_adjacent_non_changeover",
                        "instrument": symbol,
                        "inside": inside_metrics,
                        "adjacent_non_changeover": adjacent_metrics,
                        "deltas": _metric_deltas(inside_metrics, adjacent_metrics),
                    }
                )
    return results


def summarize_trade_behavior(trades: Sequence[NormalizedTrade]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not trades:
        return results
    for spec in CHANGEOVER_SPECS:
        for window in WINDOW_SPECS:
            minutes = int(window["minutes"])
            inside = [
                trade
                for trade in trades
                if abs(minutes_from_anchor(trade.entry_ts, spec["anchor_time"])) <= minutes
            ]
            adjacent = [
                trade
                for trade in trades
                if minutes < abs(minutes_from_anchor(trade.entry_ts, spec["anchor_time"])) <= minutes * 2
            ]
            families = sorted({trade.source_family for trade in inside + adjacent})
            for family in families:
                family_inside = [trade for trade in inside if trade.source_family == family]
                family_adjacent = [trade for trade in adjacent if trade.source_family == family]
                if not family_inside and not family_adjacent:
                    continue
                results.append(
                    {
                        "changeover_label": spec["label"],
                        "anchor_et": spec["anchor_et"],
                        "window": window["window"],
                        "window_minutes": minutes,
                        "source_family": family,
                        "inside": _trade_metrics(family_inside, anchor_time=spec["anchor_time"], window_minutes=minutes),
                        "adjacent_non_changeover": _trade_metrics(
                            family_adjacent, anchor_time=spec["anchor_time"], window_minutes=minutes
                        ),
                    }
                )
    return results


def build_recommendations(*, bar_summary: Sequence[Mapping[str, Any]], trade_summary: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    entry_implications: list[dict[str, Any]] = []
    hold_exit_implications: list[dict[str, Any]] = []
    label_votes: dict[str, int] = {spec["label"]: 0 for spec in CHANGEOVER_SPECS}
    for row in bar_summary:
        deltas = row.get("deltas", {})
        range_delta = _coerce_float(deltas.get("avg_range_points"))
        false_delta = _coerce_float(deltas.get("false_move_frequency"))
        breakout_delta = _coerce_float(deltas.get("breakout_frequency"))
        if range_delta is not None and abs(range_delta) >= 0.15:
            label_votes[str(row["changeover_label"])] += 1
        if breakout_delta is not None and breakout_delta > 0.03:
            label_votes[str(row["changeover_label"])] += 1
            entry_implications.append(
                {
                    "changeover_label": row["changeover_label"],
                    "window": row["window"],
                    "instrument": row["instrument"],
                    "implication": "breakout_or_continuation_setups_may_need_changeover_context",
                    "evidence": {"breakout_frequency_delta": breakout_delta, "avg_range_delta": range_delta},
                }
            )
        if false_delta is not None and false_delta > 0.03:
            label_votes[str(row["changeover_label"])] += 1
            hold_exit_implications.append(
                {
                    "changeover_label": row["changeover_label"],
                    "window": row["window"],
                    "instrument": row["instrument"],
                    "implication": "consider_harvest_or_tighter_thesis_check_before_changeover_if_false_moves_elevated",
                    "evidence": {"false_move_frequency_delta": false_delta, "avg_range_delta": range_delta},
                }
            )

    for row in trade_summary:
        inside = row.get("inside", {})
        adjacent = row.get("adjacent_non_changeover", {})
        if int(inside.get("trade_count", 0) or 0) >= 5:
            pnl_delta = _coerce_float(inside.get("avg_pnl_cash"))
            adjacent_pnl = _coerce_float(adjacent.get("avg_pnl_cash"))
            if pnl_delta is not None and adjacent_pnl is not None and abs(pnl_delta - adjacent_pnl) > 5.0:
                label_votes[str(row["changeover_label"])] += 1

    labels = []
    for label, votes in label_votes.items():
        labels.append(
            {
                "label": label,
                "recommendation": "ADD_RESEARCH_OVERLAY_LABEL_SHADOW_ONLY" if votes else "COLLECT_MORE_EVIDENCE",
                "evidence_votes": votes,
            }
        )
    return {
        "entry_implications": entry_implications[:20],
        "hold_exit_implications": hold_exit_implications[:20],
        "session_label_recommendation": {
            "recommended_live_rule_change": False,
            "recommended_labels": labels,
            "summary": "Add labels only as overlapping research/session-context tags until forward/live PAPER evidence supports using them in entries or exits.",
        },
    }


def minutes_from_anchor(timestamp: datetime, anchor_time: time) -> int:
    local = timestamp.astimezone(NEW_YORK) if timestamp.tzinfo else timestamp.replace(tzinfo=NEW_YORK)
    return local.hour * 60 + local.minute - (anchor_time.hour * 60 + anchor_time.minute)


def classify_window(timestamp: datetime, *, anchor_time: time, window_minutes: int) -> str:
    offset = abs(minutes_from_anchor(timestamp, anchor_time))
    if offset <= window_minutes:
        return "inside_changeover"
    if offset <= window_minutes * 2:
        return "adjacent_non_changeover"
    return "outside"


def render_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Session Changeover Overlay Research",
        "",
        "Strategy-family agnostic, research-only overlay for 03:00 ET Asia-to-Europe and 07:00 ET Europe-to-US changeovers.",
        "",
        "## Safety Scope",
        "",
        "- Research only: true",
        "- Live trading authority changed: false",
        "- Active PAPER config changed: false",
        "- Strategy thresholds changed: false",
        "- Runtime restarted: false",
        "- Broker mutation allowed: false",
        "- paper_proof invoked: false",
        "",
        "## Evidence",
        "",
        f"- Replay bar rows: {summary.get('bar_evidence', {}).get('row_count', 0)}",
        f"- Replay symbols: {', '.join(summary.get('bar_evidence', {}).get('symbols', []))}",
        f"- Replay window: {summary.get('bar_evidence', {}).get('start_ts')} to {summary.get('bar_evidence', {}).get('end_ts')}",
        f"- Normalized trade rows: {summary.get('trade_evidence', {}).get('normalized_trade_count', 0)}",
        f"- Trade source paths: {summary.get('trade_evidence', {}).get('source_path_count', 0)}",
        "",
        "## Bar Behavior Summary",
        "",
        "| Changeover | Window | Instrument | Inside bars | Adjacent bars | Avg range delta | Breakout delta | False-move delta |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary.get("bar_evidence", {}).get("changeover_comparisons", [])[:24]:
        inside = row.get("inside", {})
        adjacent = row.get("adjacent_non_changeover", {})
        deltas = row.get("deltas", {})
        lines.append(
            "| {label} | {window} | {instrument} | {inside_count} | {adjacent_count} | {range_delta} | {breakout_delta} | {false_delta} |".format(
                label=row.get("changeover_label"),
                window=row.get("window"),
                instrument=row.get("instrument"),
                inside_count=inside.get("bar_count"),
                adjacent_count=adjacent.get("bar_count"),
                range_delta=_fmt(deltas.get("avg_range_points")),
                breakout_delta=_fmt(deltas.get("breakout_frequency")),
                false_delta=_fmt(deltas.get("false_move_frequency")),
            )
        )
    lines.extend(["", "## Strategy And Shadow Trade Overlay", ""])
    lines.append("| Changeover | Window | Source family | Inside trades | Inside win rate | Inside PF | Adjacent trades | Adjacent win rate | Adjacent PF |")
    lines.append("|---|---:|---|---:|---:|---:|---:|---:|---:|")
    for row in summary.get("trade_evidence", {}).get("changeover_comparisons", [])[:40]:
        inside = row.get("inside", {})
        adjacent = row.get("adjacent_non_changeover", {})
        lines.append(
            "| {label} | {window} | {family} | {inside_count} | {inside_win} | {inside_pf} | {adjacent_count} | {adjacent_win} | {adjacent_pf} |".format(
                label=row.get("changeover_label"),
                window=row.get("window"),
                family=row.get("source_family"),
                inside_count=inside.get("trade_count"),
                inside_win=_fmt(inside.get("win_rate")),
                inside_pf=_fmt(inside.get("profit_factor")),
                adjacent_count=adjacent.get("trade_count"),
                adjacent_win=_fmt(adjacent.get("win_rate")),
                adjacent_pf=_fmt(adjacent.get("profit_factor")),
            )
        )
    lines.extend(["", "## Candidate Implications For Entries", ""])
    for item in summary.get("entry_implications", []) or [{"implication": "No strong entry implication detected yet."}]:
        lines.append(f"- {item.get('changeover_label', 'ALL')}: {item.get('implication')}")
    lines.extend(["", "## Candidate Implications For Hold/Exit Policy", ""])
    for item in summary.get("hold_exit_implications", []) or [{"implication": "No strong hold/exit implication detected yet."}]:
        lines.append(f"- {item.get('changeover_label', 'ALL')}: {item.get('implication')}")
    lines.extend(["", "## Session Label Recommendation", ""])
    rec = summary.get("session_label_recommendation", {})
    lines.append(str(rec.get("summary", "")))
    for item in rec.get("recommended_labels", []):
        lines.append(f"- {item.get('label')}: {item.get('recommendation')} ({item.get('evidence_votes')} evidence votes)")
    lines.extend(["", "## Limitations", ""])
    for item in summary.get("limitations", []):
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def _bar_metrics(frame: pd.DataFrame, *, range_threshold: float) -> dict[str, Any]:
    if frame.empty:
        return {
            "bar_count": 0,
            "avg_return_points": None,
            "avg_abs_return_points": None,
            "avg_range_points": None,
            "breakout_frequency": None,
            "false_move_frequency": None,
            "continuation_frequency": None,
            "reversal_frequency": None,
            "avg_mfe_proxy_points": None,
            "avg_mae_proxy_points": None,
        }
    breakout = frame["range_points"] >= range_threshold
    false_move = breakout & (
        frame["close_location"].between(0.35, 0.65, inclusive="both")
        | ((frame["body_points"] / frame["range_points"].replace(0, pd.NA)) < 0.25)
    )
    direction = frame["return_points"].map(_sign)
    next_direction = frame["next_return_points"].map(_sign)
    continuation = (direction != 0) & (direction == next_direction)
    reversal = (direction != 0) & (next_direction != 0) & (direction != next_direction)
    favorable = frame.apply(_bar_favorable_excursion, axis=1)
    adverse = frame.apply(_bar_adverse_excursion, axis=1)
    return {
        "bar_count": int(len(frame)),
        "avg_return_points": _mean(frame["return_points"]),
        "median_return_points": _median(frame["return_points"]),
        "avg_abs_return_points": _mean(frame["abs_return_points"]),
        "avg_range_points": _mean(frame["range_points"]),
        "breakout_frequency": _mean_bool(breakout),
        "false_move_frequency": _mean_bool(false_move),
        "continuation_frequency": _mean_bool(continuation),
        "reversal_frequency": _mean_bool(reversal),
        "avg_mfe_proxy_points": _mean(favorable),
        "avg_mae_proxy_points": _mean(adverse),
        "top_session_phases": _top_counts(frame["session_phase"]),
    }


def _trade_metrics(
    trades: Sequence[NormalizedTrade], *, anchor_time: time, window_minutes: int
) -> dict[str, Any]:
    pnls = [trade.pnl_cash for trade in trades if trade.pnl_cash is not None]
    point_pnls = [trade.pnl_points for trade in trades if trade.pnl_points is not None]
    held_through = [
        trade
        for trade in trades
        if trade.exit_ts is not None
        and minutes_from_anchor(trade.entry_ts, anchor_time) < 0
        and minutes_from_anchor(trade.exit_ts, anchor_time) > 0
        and abs(minutes_from_anchor(trade.entry_ts, anchor_time)) <= window_minutes
    ]
    return {
        "trade_count": len(trades),
        "pnl_evidence_count": len(pnls),
        "win_count": sum(1 for pnl in pnls if pnl > 0),
        "loss_count": sum(1 for pnl in pnls if pnl < 0),
        "flat_count": sum(1 for pnl in pnls if pnl == 0),
        "win_rate": (sum(1 for pnl in pnls if pnl > 0) / len(pnls)) if pnls else None,
        "avg_pnl_cash": _plain_mean(pnls),
        "avg_pnl_points": _plain_mean(point_pnls),
        "profit_factor": _profit_factor(pnls),
        "avg_mfe_points": _plain_mean([trade.mfe_points for trade in trades if trade.mfe_points is not None]),
        "avg_mae_points": _plain_mean([trade.mae_points for trade in trades if trade.mae_points is not None]),
        "held_through_changeover_count": len(held_through),
        "held_through_avg_pnl_cash": _plain_mean([trade.pnl_cash for trade in held_through if trade.pnl_cash is not None]),
    }


def _metric_deltas(inside: Mapping[str, Any], adjacent: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "avg_return_points",
        "avg_abs_return_points",
        "avg_range_points",
        "breakout_frequency",
        "false_move_frequency",
        "continuation_frequency",
        "reversal_frequency",
        "avg_mfe_proxy_points",
        "avg_mae_proxy_points",
    )
    deltas: dict[str, Any] = {}
    for key in keys:
        left = _coerce_float(inside.get(key))
        right = _coerce_float(adjacent.get(key))
        deltas[key] = None if left is None or right is None else left - right
    return deltas


def _read_jsonl(path: Path) -> Iterable[Mapping[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, Mapping):
                    yield value
    except OSError:
        return


def _source_family(path: Path, *, raw: Mapping[str, Any]) -> str:
    explicit = raw.get("family") or raw.get("strategy_id") or raw.get("candidate_family")
    if explicit:
        return str(explicit)
    if path.parent.name != "datasets":
        return path.parent.name
    return path.parent.parent.name


def _first_present(primary: Mapping[str, Any], secondary: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in primary and primary[name] is not None:
            return primary[name]
        if name in secondary and secondary[name] is not None:
            return secondary[name]
    return default


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _sign(value: Any) -> int:
    number = _coerce_float(value)
    if number is None or number == 0:
        return 0
    return 1 if number > 0 else -1


def _bar_favorable_excursion(row: pd.Series) -> float:
    if row["close"] >= row["open"]:
        return float(row["high"] - row["open"])
    return float(row["open"] - row["low"])


def _bar_adverse_excursion(row: pd.Series) -> float:
    if row["close"] >= row["open"]:
        return float(row["open"] - row["low"])
    return float(row["high"] - row["open"])


def _mean(series: Any) -> float | None:
    value = pd.Series(series).dropna().mean()
    return None if pd.isna(value) else float(value)


def _median(series: Any) -> float | None:
    value = pd.Series(series).dropna().median()
    return None if pd.isna(value) else float(value)


def _mean_bool(series: Any) -> float | None:
    values = pd.Series(series).dropna()
    if values.empty:
        return None
    return float(values.astype(bool).mean())


def _plain_mean(values: Sequence[float]) -> float | None:
    return None if not values else float(sum(values) / len(values))


def _profit_factor(pnls: Sequence[float]) -> float | None:
    gross_profit = sum(pnl for pnl in pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in pnls if pnl < 0))
    if gross_loss == 0:
        return None
    return gross_profit / gross_loss


def _top_counts(series: pd.Series, *, limit: int = 5) -> list[dict[str, Any]]:
    counts = series.value_counts().head(limit)
    return [{"value": str(index), "count": int(value)} for index, value in counts.items()]


def _series_min_iso(series: pd.Series) -> str | None:
    value = series.min()
    return None if pd.isna(value) else value.isoformat()


def _series_max_iso(series: pd.Series) -> str | None:
    value = series.max()
    return None if pd.isna(value) else value.isoformat()


def _fmt(value: Any) -> str:
    number = _coerce_float(value)
    if number is None:
        return ""
    return f"{number:.4f}"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and math.isinf(value):
        return "Infinity"
    return str(value)


if __name__ == "__main__":
    run_session_changeover_overlay_research(repo_root=Path.cwd())
