"""Read-only same-day Track B PAPER trade and missed-opportunity postmortem.

This diagnostic reconstructs the current PAPER lifecycle outcome, compares it
with retained completed-bar evidence, and writes a compact JSON plus markdown
report. It never invokes broker, paper-proof, or submit paths.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .track_b_missed_move_forensic_replay import (
    DEFAULT_INSTRUMENTS,
    DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT,
    build_track_b_missed_move_forensic_replay,
)


DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_JSON = (
    DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_OUTPUT_ROOT / "latest_track_b_same_day_postmortem.json"
)
DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_MD = (
    DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_OUTPUT_ROOT / "latest_track_b_same_day_postmortem.md"
)
DEFAULT_MAX_CANDLE_CONTEXT_FILES = 4000
MAX_JSON_BYTES = 3 * 1024 * 1024
POINT_VALUE_BY_INSTRUMENT = {
    "MGC": Decimal("10"),
    "MNQ": Decimal("2"),
}


@dataclass(frozen=True)
class TrackBSameDayPostmortemResult:
    report_json: Path
    report_markdown: Path
    report: dict[str, Any]
    markdown: str


def build_track_b_same_day_postmortem(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_OUTPUT_ROOT,
    instruments: Sequence[str] = DEFAULT_INSTRUMENTS,
    max_candle_context_files: int = DEFAULT_MAX_CANDLE_CONTEXT_FILES,
    now: datetime | None = None,
    write: bool = True,
) -> TrackBSameDayPostmortemResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    output_root = Path(output_root)
    instrument_set = {str(item).upper() for item in instruments}

    forensic = build_track_b_missed_move_forensic_replay(
        repo_root=root,
        output_root=output_root,
        instruments=tuple(sorted(instrument_set)),
        now=actual_now,
        write=write,
    ).report
    trade_summary = _load_json(
        root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
    )
    pnl_summary = _load_json(
        root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_pnl_summary.json"
    )
    position_status = _load_json(
        root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
    )
    trades = _trade_lifecycle_reconstruction(root, trade_summary, forensic)
    bars_by_instrument = {
        instrument: _collect_completed_5m_bars(root, instrument, max_files=max_candle_context_files)
        for instrument in sorted(instrument_set)
    }
    missed_rally = {
        instrument: _missed_rally_review(instrument, bars_by_instrument.get(instrument, []), forensic)
        for instrument in sorted(instrument_set)
    }
    baselines = _baseline_comparison(
        instruments=sorted(instrument_set),
        bars_by_instrument=bars_by_instrument,
        track_b_realized_pnl=_decimal_or_none(pnl_summary.get("total_realized_pnl_today")),
        now=actual_now,
    )
    classifications = _classify_postmortem(
        trades=trades,
        forensic=forensic,
        missed_rally=missed_rally,
        baselines=baselines,
    )
    report = {
        "schema_version": "track_b_same_day_postmortem_v1",
        "generated_at": actual_now.isoformat(),
        "source_tag": "DIAGNOSTIC_POSTMORTEM_ONLY",
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked_by_postmortem": False,
        "manual_submit_cancel_place_order_invoked": False,
        "broker_state_mutated_by_postmortem": False,
        "live_money_readiness": False,
        "report_paths": {
            "json": str(output_root / DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_JSON.name),
            "markdown": str(output_root / DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_MD.name),
            "missed_move_forensic_replay": str(
                output_root / DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT.name
                if output_root.name != "diagnostics"
                else output_root / "latest_track_b_missed_move_forensic_replay.json"
            ),
        },
        "trade_lifecycle_reconstruction": trades,
        "paper_trade_summary": {
            "source": trade_summary.get("source"),
            "broker_reconciled": trade_summary.get("broker_reconciled"),
            "paper_trades_attempted_count": trade_summary.get("paper_trades_attempted_count"),
            "completed_trade_count": trade_summary.get("completed_trade_count"),
            "open_position_count": trade_summary.get("open_position_count"),
            "last_trade_strategy": trade_summary.get("last_trade_strategy"),
            "last_trade_pnl": trade_summary.get("last_trade_pnl"),
            "review_required_count": trade_summary.get("review_required_count"),
        },
        "position_status": {
            "source": position_status.get("source"),
            "broker_reconciled": position_status.get("broker_reconciled"),
            "open_position_count": position_status.get("open_position_count"),
            "open_order_count": position_status.get("open_order_count"),
            "total_unrealized_pnl": position_status.get("total_unrealized_pnl"),
            "broker_truth_warning": position_status.get("broker_truth_warning"),
        },
        "missed_rally_window_review": missed_rally,
        "simple_baseline_comparison": baselines,
        "forensic_replay_summary": _forensic_summary(forensic),
        "classifications": classifications,
        "primary_conclusion": _primary_conclusion(classifications),
        "recommended_next_review": _recommended_next_review(classifications),
        "limitations": [
            "Postmortem is read-only and uses retained Track B artifacts plus bounded candle context files.",
            "Historical/bounded artifact replay is diagnostic only and is not an execution-live approval source.",
            "Baseline comparisons are simple long-only diagnostics, not trading instructions.",
        ],
    }
    markdown = _render_markdown(report)
    report_json = output_root / DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_JSON.name
    report_md = output_root / DEFAULT_TRACK_B_SAME_DAY_POSTMORTEM_MD.name
    if write:
        output_root.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        report_md.write_text(markdown, encoding="utf-8")
    return TrackBSameDayPostmortemResult(report_json=report_json, report_markdown=report_md, report=report, markdown=markdown)


def _trade_lifecycle_reconstruction(
    repo_root: Path,
    trade_summary: Mapping[str, Any],
    forensic: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in trade_summary.get("recent_trades") or []:
        if not isinstance(trade, Mapping):
            continue
        lifecycle_path = _resolve_repo_path(repo_root, trade.get("paper_lifecycle_report_path"))
        lifecycle = _load_json(lifecycle_path) if lifecycle_path else {}
        proof_payload = lifecycle.get("proof_payload") if isinstance(lifecycle.get("proof_payload"), Mapping) else {}
        entry_ts = _parse_dt(trade.get("entry_timestamp"))
        exit_ts = _parse_dt(trade.get("exit_timestamp"))
        signal_context = _matching_signal_context(forensic, trade)
        exit_reason = _exit_reason(lifecycle, proof_payload)
        row = {
            "trade_id": trade.get("trade_id"),
            "lifecycle_id": trade.get("lifecycle_id"),
            "signal_id": trade.get("signal_id"),
            "strategy": trade.get("strategy_id"),
            "instrument": trade.get("instrument_family"),
            "contract_key": trade.get("contract_key"),
            "local_symbol": trade.get("local_symbol"),
            "con_id": trade.get("con_id"),
            "side": trade.get("side"),
            "order_action": trade.get("order_action"),
            "quantity": trade.get("quantity"),
            "entry_timestamp": trade.get("entry_timestamp"),
            "entry_price": trade.get("entry_fill_price") or trade.get("entry_price"),
            "exit_timestamp": trade.get("exit_timestamp"),
            "exit_price": trade.get("exit_fill_price") or trade.get("exit_price"),
            "hold_duration_seconds": (exit_ts - entry_ts).total_seconds() if entry_ts and exit_ts else None,
            "realized_pnl": trade.get("realized_pnl"),
            "ticks_pnl": trade.get("ticks_pnl"),
            "points_pnl": trade.get("points_pnl"),
            "entry_predicates": signal_context.get("passed_predicates", []),
            "entry_signal_reason": signal_context.get("decision_reason"),
            "entry_rule_runner_report_path": signal_context.get("rule_runner_report_path"),
            "runtime_cycle_report_path": signal_context.get("runtime_cycle_report_path"),
            "exit_reason": exit_reason,
            "closed_by_normal_strategy_logic": False,
            "closed_by_guarded_paper_proof_lifecycle": exit_reason == "lifecycle_guardrail",
            "abnormal_safety_rule_close": False,
            "final_state_flat": trade.get("final_position_status") == "CLEAN"
            or proof_payload.get("proof_lifecycle_status") == "PROOF_COMPLETE_FLAT",
            "final_broker_state_classification": trade.get("final_broker_state_classification")
            or lifecycle.get("classification"),
            "paper_lifecycle_classification": trade.get("paper_lifecycle_classification"),
            "review_required": trade.get("review_required"),
            "paper_lifecycle_report_path": trade.get("paper_lifecycle_report_path"),
            "paper_lifecycle_summary": {
                "classification": lifecycle.get("classification"),
                "proof_lifecycle_status": proof_payload.get("proof_lifecycle_status"),
                "open_order_id": _nested(proof_payload, "open_fill", "broker_order_id"),
                "close_order_id": _nested(proof_payload, "close_fill", "broker_order_id"),
                "open_intent_reason": _nested(proof_payload, "open_intent", "reason"),
                "close_intent_reason": _nested(proof_payload, "close_intent", "reason"),
                "final_reconciliation": proof_payload.get("final_reconciliation"),
            },
        }
        rows.append(row)
    return rows


def _missed_rally_review(
    instrument: str,
    bars: Sequence[Mapping[str, Any]],
    forensic: Mapping[str, Any],
) -> dict[str, Any]:
    rally = _main_rally_window(bars)
    rows = [
        row
        for row in forensic.get("completed_decision_bars") or []
        if isinstance(row, Mapping) and row.get("instrument") == instrument
    ]
    window_rows = _rows_in_window(rows, rally.get("start_timestamp"), rally.get("end_timestamp")) if rally else rows
    strategy_results = [
        item
        for row in window_rows
        for item in row.get("strategy_results", [])
        if isinstance(item, Mapping)
    ]
    failed = Counter(
        pred
        for item in strategy_results
        for pred in item.get("failed_predicates", [])
        if isinstance(pred, str)
    )
    near_one = [item for item in strategy_results if item.get("near_miss_classification") == "ONE_PREDICATE_AWAY"]
    near_two = [item for item in strategy_results if item.get("near_miss_classification") == "TWO_PREDICATES_AWAY"]
    directional = dict((forensic.get("directional_coverage_assessment") or {}).get(instrument, {}))
    if rally and not directional.get("long_trend_continuation_strategy_count"):
        directional["trend_continuation_gap"] = True
        directional["assessment"] = (
            "No enabled pure long trend-continuation strategy family is present in retained rally evidence."
        )
    return {
        "instrument": instrument,
        "main_rally_window": rally,
        "completed_5m_bars_during_rally": len(_bars_in_window(bars, rally)) if rally else 0,
        "eligible_bars": sum(1 for row in window_rows if row.get("paper_evaluation_allowed_equivalent") is not False),
        "evaluated_bars": len(window_rows),
        "strategy_evaluations": len(strategy_results),
        "signals": sum(1 for item in strategy_results if item.get("result") == "SIGNAL"),
        "near_misses": {
            "one_predicate_away": len(near_one),
            "two_predicates_away": len(near_two),
        },
        "top_failed_predicates": _counter_rows(failed, limit=12),
        "one_predicate_away_examples": _near_examples(near_one),
        "two_predicate_away_examples": _near_examples(near_two),
        "active_strategy_set_directionally_aligned": bool(directional.get("long_strategy_count"))
        and not directional.get("trend_continuation_gap"),
        "long_side_trend_continuation_coverage_exists": bool(
            directional.get("long_trend_continuation_strategy_count")
        ),
        "directional_coverage_assessment": directional,
        "instrument_eligible_during_rally": any(
            row.get("paper_evaluation_allowed_equivalent") is not False
            or (row.get("feature_context_ready") is True and row.get("live_execution_approved") is True)
            for row in window_rows
        ),
        "dominant_blocker": _dominant_missed_rally_blocker(window_rows, directional),
    }


def _baseline_comparison(
    *,
    instruments: Sequence[str],
    bars_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]],
    track_b_realized_pnl: Decimal | None,
    now: datetime,
) -> dict[str, Any]:
    today = now.date()
    yesterday = (now.date()).toordinal() - 1
    result: dict[str, Any] = {
        "source_tag": "DIAGNOSTIC_BASELINE_ONLY_NOT_TRADING_INSTRUCTIONS",
        "track_b_realized_pnl_today": str(track_b_realized_pnl) if track_b_realized_pnl is not None else None,
        "by_day": {},
    }
    for day_label, day in (("today", today), ("yesterday", datetime.fromordinal(yesterday).date())):
        day_rows: dict[str, Any] = {}
        for instrument in instruments:
            bars = [
                bar
                for bar in bars_by_instrument.get(instrument, [])
                if (_parse_dt(bar.get("timestamp")) or datetime.min.replace(tzinfo=UTC)).date() == day
            ]
            day_rows[instrument] = {
                "instrument": instrument,
                "available_completed_5m_bars": len(bars),
                "buy_at_session_start_exit_latest": _long_baseline(instrument, bars, start_kind="session_start"),
                "buy_at_us_open_exit_latest": _long_baseline(instrument, bars, start_kind="us_open"),
            }
        result["by_day"][day_label] = day_rows
    result["missed_opportunity_magnitude"] = _missed_opportunity(result, track_b_realized_pnl)
    return result


def _collect_completed_5m_bars(repo_root: Path, instrument: str, *, max_files: int) -> list[dict[str, Any]]:
    one_minute: dict[str, dict[str, Any]] = {}
    roots = [
        repo_root / "outputs" / "track_b_execution_core" / "track_b_runtime_candle_capture",
        repo_root / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed",
    ]
    paths: list[Path] = []
    for root in roots:
        if root.exists():
            paths.extend(root.rglob(f"*{instrument.lower()}*1m_candles.json"))
            paths.extend(root.rglob(f"*{instrument.upper()}*1m_candles.json"))
    paths = sorted(set(paths), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True)[:max_files]
    for path in paths:
        payload = _load_json(path)
        for raw in _extract_candles(payload):
            ts = _parse_dt(raw.get("candle_timestamp") or raw.get("timestamp") or raw.get("ts_event"))
            if not ts:
                continue
            if ts.date() < datetime(2026, 5, 5, tzinfo=UTC).date():
                continue
            one_minute[ts.isoformat()] = _normalized_bar(raw, ts, source_path=path)
    return _aggregate_1m_to_5m(sorted(one_minute.values(), key=lambda item: item["timestamp"]))


def _aggregate_1m_to_5m(bars: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[Mapping[str, Any]]] = defaultdict(list)
    for bar in bars:
        ts = _parse_dt(bar.get("timestamp"))
        if not ts:
            continue
        bucket = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
        buckets[bucket].append(bar)
    completed: list[dict[str, Any]] = []
    for bucket, bucket_bars in sorted(buckets.items()):
        ordered = sorted(bucket_bars, key=lambda item: str(item.get("timestamp")))
        if not ordered:
            continue
        opens = _decimal_or_none(ordered[0].get("open"))
        closes = _decimal_or_none(ordered[-1].get("close"))
        highs = [_decimal_or_none(item.get("high")) for item in ordered]
        lows = [_decimal_or_none(item.get("low")) for item in ordered]
        if opens is None or closes is None or any(item is None for item in highs + lows):
            continue
        completed.append(
            {
                "timestamp": bucket.isoformat(),
                "open": str(opens),
                "high": str(max(item for item in highs if item is not None)),
                "low": str(min(item for item in lows if item is not None)),
                "close": str(closes),
                "bar_count": len(ordered),
                "source_tag": "BOUNDED_TRACK_B_CONTEXT_AGGREGATE_DIAGNOSTIC_ONLY",
            }
        )
    return completed


def _main_rally_window(bars: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if len(bars) < 2:
        return None
    best: dict[str, Any] | None = None
    min_open = _decimal_or_none(bars[0].get("open"))
    min_index = 0
    for idx, bar in enumerate(bars):
        open_value = _decimal_or_none(bar.get("open"))
        close_value = _decimal_or_none(bar.get("close"))
        if open_value is not None and (min_open is None or open_value < min_open):
            min_open = open_value
            min_index = idx
        if close_value is None or min_open is None or idx <= min_index:
            continue
        change = close_value - min_open
        if best is None or change > best["point_change_decimal"]:
            best = {
                "start_timestamp": bars[min_index].get("timestamp"),
                "end_timestamp": bar.get("timestamp"),
                "start_price": str(min_open),
                "end_price": str(close_value),
                "point_change": str(change),
                "point_change_decimal": change,
                "completed_5m_bars": idx - min_index + 1,
            }
    if not best:
        return None
    best.pop("point_change_decimal", None)
    return best


def _long_baseline(instrument: str, bars: Sequence[Mapping[str, Any]], *, start_kind: str) -> dict[str, Any]:
    if not bars:
        return {"available": False, "reason": "NO_BARS_AVAILABLE"}
    ordered = sorted(bars, key=lambda item: str(item.get("timestamp")))
    if start_kind == "us_open":
        start_bar = next((bar for bar in ordered if _is_at_or_after_us_open(bar.get("timestamp"))), None)
        if start_bar is None:
            return {"available": False, "reason": "NO_US_OPEN_OR_LATER_BAR_AVAILABLE"}
    else:
        start_bar = ordered[0]
    exit_bar = ordered[-1]
    entry = _decimal_or_none(start_bar.get("open"))
    exit_price = _decimal_or_none(exit_bar.get("close"))
    multiplier = POINT_VALUE_BY_INSTRUMENT.get(instrument, Decimal("1"))
    if entry is None or exit_price is None:
        return {"available": False, "reason": "PRICE_UNAVAILABLE"}
    points = exit_price - entry
    return {
        "available": True,
        "entry_timestamp": start_bar.get("timestamp"),
        "entry_price": str(entry),
        "exit_timestamp": exit_bar.get("timestamp"),
        "exit_price": str(exit_price),
        "points_pnl": str(points),
        "pnl": str(points * multiplier),
        "point_value": str(multiplier),
        "note": "Diagnostic long-only baseline, not a trading instruction.",
    }


def _classify_postmortem(
    *,
    trades: Sequence[Mapping[str, Any]],
    forensic: Mapping[str, Any],
    missed_rally: Mapping[str, Mapping[str, Any]],
    baselines: Mapping[str, Any],
) -> list[str]:
    classes: list[str] = []
    if trades:
        classes.append("EXECUTION_PATH_WORKED_BUT_STRATEGY_FAILED_TO_PARTICIPATE")
    if any(item.get("long_side_trend_continuation_coverage_exists") is False for item in missed_rally.values()):
        classes.append("STRATEGY_COVERAGE_GAP_TREND_CONTINUATION")
    if forensic.get("one_predicate_away_count") or forensic.get("two_predicate_away_count"):
        classes.append("STRATEGY_GATES_MUTED_NEAR_MISSES")
    if any(item.get("instrument_eligible_during_rally") is False for item in missed_rally.values()):
        classes.append("OPERATIONAL_READINESS_MISSED_RALLY")
    if any(_decimal_or_none(item.get("hold_duration_seconds")) is not None and Decimal(str(item.get("hold_duration_seconds"))) < Decimal("60") for item in trades):
        classes.append("TRADE_LIFECYCLE_TOO_SHORT_OR_OVER_FLATTENED")
    if not classes:
        classes.append("DIAGNOSTIC_INCONCLUSIVE")
    return classes


def _render_markdown(report: Mapping[str, Any]) -> str:
    trade_summary = report.get("paper_trade_summary", {})
    forensic = report.get("forensic_replay_summary", {})
    lines = [
        "# Track B Same-Day PAPER Postmortem",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "## Bottom Line",
        "",
        str(report.get("primary_conclusion")),
        "",
        "## PAPER Trade",
        "",
        f"- Trades attempted: {trade_summary.get('paper_trades_attempted_count')}",
        f"- Completed trades: {trade_summary.get('completed_trade_count')}",
        f"- Last strategy: {trade_summary.get('last_trade_strategy')}",
        f"- Last P&L: {trade_summary.get('last_trade_pnl')}",
    ]
    for trade in report.get("trade_lifecycle_reconstruction") or []:
        lines.extend(
            [
                "",
                f"### {trade.get('trade_id')}",
                "",
                f"- Instrument: {trade.get('instrument')} {trade.get('local_symbol')}",
                f"- Side / quantity: {trade.get('side')} {trade.get('quantity')}",
                f"- Entry: {trade.get('entry_timestamp')} @ {trade.get('entry_price')}",
                f"- Exit: {trade.get('exit_timestamp')} @ {trade.get('exit_price')}",
                f"- Hold duration seconds: {trade.get('hold_duration_seconds')}",
                f"- Realized P&L: {trade.get('realized_pnl')}",
                f"- Entry reason: {trade.get('entry_signal_reason')}",
                f"- Exit reason: {trade.get('exit_reason')}",
                f"- Final flat: {trade.get('final_state_flat')}",
                f"- Review required: {trade.get('review_required')}",
            ]
        )
    lines.extend(
        [
            "",
            "## Missed Rally Evidence",
            "",
            f"- Completed 5m bars analyzed: {forensic.get('completed_5m_bars_analyzed')}",
            f"- Strategy evaluations: {forensic.get('total_strategy_evaluations')}",
            f"- Signals: {forensic.get('total_signals')}",
            f"- Suppressed: {forensic.get('total_suppressed')}",
            f"- Handoffs: {forensic.get('total_handoffs')}",
            f"- One-predicate-away near misses: {forensic.get('one_predicate_away_count')}",
            f"- Two-predicate-away near misses: {forensic.get('two_predicate_away_count')}",
            "",
            "## Instrument Rally Windows",
            "",
        ]
    )
    for instrument, item in (report.get("missed_rally_window_review") or {}).items():
        rally = item.get("main_rally_window") or {}
        lines.extend(
            [
                f"### {instrument}",
                "",
                f"- Main retained rally: {rally.get('start_timestamp')} to {rally.get('end_timestamp')}",
                f"- Point change: {rally.get('point_change')}",
                f"- Strategy evaluations in window: {item.get('strategy_evaluations')}",
                f"- Signals in window: {item.get('signals')}",
                f"- Long trend-continuation coverage exists: {item.get('long_side_trend_continuation_coverage_exists')}",
                f"- Dominant blocker: {item.get('dominant_blocker')}",
                "",
            ]
        )
    lines.extend(["## Baselines", ""])
    for day_label, by_instrument in (report.get("simple_baseline_comparison", {}).get("by_day") or {}).items():
        lines.append(f"### {day_label}")
        for instrument, item in by_instrument.items():
            session = item.get("buy_at_session_start_exit_latest", {})
            us_open = item.get("buy_at_us_open_exit_latest", {})
            lines.append(
                f"- {instrument}: session baseline P&L={session.get('pnl')} "
                f"US-open baseline P&L={us_open.get('pnl')} bars={item.get('available_completed_5m_bars')}"
            )
        lines.append("")
    lines.extend(
        [
            "## Classification",
            "",
            *(f"- {item}" for item in report.get("classifications") or []),
            "",
            "## Safety",
            "",
            "- This postmortem did not run broker commands, paper_proof_cli, submit/cancel/placeOrder, or mutate broker state.",
        ]
    )
    return "\n".join(lines) + "\n"


def _forensic_summary(forensic: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "diagnosis_classification",
        "window_start",
        "window_end",
        "completed_5m_bars_analyzed",
        "eligible_bars",
        "bars_actually_evaluated_live_if_known",
        "total_strategy_evaluations",
        "total_signals",
        "total_no_signals",
        "total_suppressed",
        "total_handoffs",
        "one_predicate_away_count",
        "two_predicate_away_count",
        "top_failed_predicates",
        "top_blockers_by_gate_class",
        "strategies_that_repeatedly_came_close",
        "strategies_that_never_came_close",
        "trend_continuation_gap_assessment",
        "directional_coverage_assessment",
    ]
    return {key: forensic.get(key) for key in keys}


def _primary_conclusion(classifications: Sequence[str]) -> str:
    if "TRADE_LIFECYCLE_TOO_SHORT_OR_OVER_FLATTENED" in classifications:
        return (
            "Track B did execute one guarded PAPER lifecycle, but it was an immediate proof-style open/close, "
            "not meaningful participation in the directional rally."
        )
    if "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION" in classifications:
        return "Track B was largely evaluating, but the enabled strategy set lacked pure long trend-continuation coverage."
    return "The retained artifacts are insufficient for a single confident failure mode."


def _recommended_next_review(classifications: Sequence[str]) -> list[str]:
    items: list[str] = []
    if "TRADE_LIFECYCLE_TOO_SHORT_OR_OVER_FLATTENED" in classifications:
        items.append("Separate proof-style lifecycle close behavior from strategy-managed PAPER trade hold/exit policy.")
    if "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION" in classifications:
        items.append("Review validated Track 1 long trend-continuation families for Track B migration without loosening predicates.")
    if "STRATEGY_GATES_MUTED_NEAR_MISSES" in classifications:
        items.append("Inspect one- and two-predicate-away bars before considering any threshold changes.")
    return items or ["Collect more retained completed-bar evidence."]


def _matching_signal_context(forensic: Mapping[str, Any], trade: Mapping[str, Any]) -> dict[str, Any]:
    strategy = trade.get("strategy_id")
    instrument = trade.get("instrument_family")
    entry_ts = _parse_dt(trade.get("entry_timestamp") or trade.get("signal_timestamp"))
    best: dict[str, Any] | None = None
    best_delta: float | None = None
    for row in forensic.get("completed_decision_bars") or []:
        if not isinstance(row, Mapping) or row.get("instrument") != instrument:
            continue
        row_ts = _parse_dt(row.get("decision_bar_timestamp"))
        for item in row.get("strategy_results") or []:
            if not isinstance(item, Mapping):
                continue
            if item.get("strategy_id") != strategy or item.get("result") != "SIGNAL":
                continue
            delta = abs((entry_ts - row_ts).total_seconds()) if entry_ts and row_ts else 0
            if best is None or best_delta is None or delta < best_delta:
                best = dict(item)
                best_delta = delta
    return best or {}


def _exit_reason(lifecycle: Mapping[str, Any], proof_payload: Mapping[str, Any]) -> str:
    status = proof_payload.get("proof_lifecycle_status")
    close_reason = _nested(proof_payload, "close_intent", "reason")
    if status == "PROOF_COMPLETE_FLAT" and close_reason:
        return "lifecycle_guardrail"
    if lifecycle.get("primary_blocker"):
        return "lifecycle_guardrail"
    return "unknown"


def _dominant_missed_rally_blocker(rows: Sequence[Mapping[str, Any]], directional: Mapping[str, Any]) -> str:
    if directional.get("trend_continuation_gap"):
        return "COVERAGE_GAP"
    if not rows:
        return "NO_RETAINED_EVALUATION_ROWS"
    if not any(row.get("feature_context_ready") is True or row.get("feature_context_ready") is None for row in rows):
        return "OPERATIONAL_GATE"
    return "STRATEGY_PREDICATE_GATE"


def _rows_in_window(rows: Sequence[Mapping[str, Any]], start: Any, end: Any) -> list[Mapping[str, Any]]:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if not start_dt or not end_dt:
        return list(rows)
    return [
        row
        for row in rows
        if (ts := _parse_dt(row.get("decision_bar_timestamp") or row.get("timestamp"))) is not None
        and start_dt <= ts <= end_dt
    ]


def _bars_in_window(bars: Sequence[Mapping[str, Any]], rally: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    if not rally:
        return []
    return _rows_in_window(bars, rally.get("start_timestamp"), rally.get("end_timestamp"))


def _near_examples(rows: Sequence[Mapping[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    return [
        {
            "decision_bar_timestamp": item.get("decision_bar_timestamp"),
            "instrument": item.get("instrument"),
            "strategy_id": item.get("strategy_id"),
            "nearest_failed_predicate": item.get("nearest_failed_predicate"),
            "failed_predicates_count": item.get("failed_predicates_count"),
        }
        for item in rows[:limit]
    ]


def _missed_opportunity(report: Mapping[str, Any], track_b_realized_pnl: Decimal | None) -> dict[str, Any]:
    best: dict[str, Any] | None = None
    for by_instrument in (report.get("by_day") or {}).values():
        for instrument, item in by_instrument.items():
            for key in ("buy_at_session_start_exit_latest", "buy_at_us_open_exit_latest"):
                baseline = item.get(key, {})
                pnl = _decimal_or_none(baseline.get("pnl"))
                if pnl is None:
                    continue
                if best is None or pnl > best["baseline_pnl_decimal"]:
                    best = {
                        "instrument": instrument,
                        "baseline": key,
                        "baseline_pnl": str(pnl),
                        "baseline_pnl_decimal": pnl,
                    }
    if not best:
        return {"available": False}
    track_b = track_b_realized_pnl or Decimal("0")
    best["track_b_realized_pnl"] = str(track_b)
    best["missed_vs_best_available_baseline"] = str(best["baseline_pnl_decimal"] - track_b)
    best.pop("baseline_pnl_decimal", None)
    best["available"] = True
    return best


def _is_at_or_after_us_open(value: Any) -> bool:
    ts = _parse_dt(value)
    return bool(ts and ts.time() >= time(13, 30))


def _extract_candles(payload: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                yield item
    elif isinstance(payload, Mapping):
        for key in ("candles", "candle_history", "bars", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, Mapping):
                        yield item


def _normalized_bar(raw: Mapping[str, Any], ts: datetime, *, source_path: Path) -> dict[str, Any]:
    return {
        "timestamp": ts.isoformat(),
        "open": str(raw.get("open")),
        "high": str(raw.get("high")),
        "low": str(raw.get("low")),
        "close": str(raw.get("close")),
        "volume": str(raw.get("volume")) if raw.get("volume") is not None else None,
        "source_path": str(source_path),
    }


def _counter_rows(counter: Counter[str], *, limit: int = 10) -> list[dict[str, Any]]:
    return [{"reason": key, "count": value} for key, value in counter.most_common(limit)]


def _load_json(path: Path | None) -> Any:
    if path is None or not path.exists() or path.stat().st_size > MAX_JSON_BYTES:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _resolve_repo_path(repo_root: Path, value: Any) -> Path | None:
    if not value:
        return None
    path = Path(str(value))
    return path if path.is_absolute() else repo_root / path


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current
