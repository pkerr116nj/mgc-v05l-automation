"""Canonical trade path reconstruction and excursion capture.

This module is research/diagnostic only. It reconstructs entry-to-exit and
post-exit price paths from existing analytics artifacts where market data has
already been retained. It does not import or call broker, runtime, strategy,
Managed Exit, or gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_STRATEGY_PERFORMANCE_DIR = DEFAULT_OUTPUT_ROOT / "strategy_performance"
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_SIDE_SESSION_REPLAY_PATH = DEFAULT_STRATEGY_PERFORMANCE_DIR / "side_session_attribution" / "side_session_trade_replay.jsonl"
DEFAULT_FORWARD_CAPTURE_PATH = DEFAULT_STRATEGY_PERFORMANCE_DIR / "side_session_attribution" / "forward_path_capture.jsonl"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "trade_path_reconstruction"

PATH_JSONL = "canonical_trade_path_reconstruction.jsonl"
SUMMARY_JSON = "latest_trade_path_reconstruction_summary.json"
SUMMARY_MD = "latest_trade_path_reconstruction_summary.md"
CONTRACT_MD = "trade_path_reconstruction_contract.md"
EXCURSION_MD = "excursion_capture_coverage.md"
FORWARD_MD = "post_exit_forward_path_report.md"
READINESS_MD = "counterfactual_readiness_report.md"

SCHEMA_VERSION = "canonical_trade_path_reconstruction_v1"
SUMMARY_SCHEMA_VERSION = "trade_path_reconstruction_summary_v1"
FORWARD_WINDOWS_MINUTES = (15, 30, 60, 120)


@dataclass(frozen=True)
class TradePathReconstructionResult:
    rows: list[dict[str, Any]]
    summary: dict[str, Any]
    path_jsonl_path: Path
    summary_json_path: Path
    summary_markdown_path: Path
    contract_path: Path
    excursion_report_path: Path
    forward_report_path: Path
    readiness_report_path: Path


def run_trade_path_reconstruction(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    side_session_replay_path: Path = DEFAULT_SIDE_SESSION_REPLAY_PATH,
    forward_capture_path: Path = DEFAULT_FORWARD_CAPTURE_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> TradePathReconstructionResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    replay_rows = _read_jsonl(side_session_replay_path)
    forward_rows = _read_jsonl(forward_capture_path)
    rows = build_trade_path_reconstructions(
        outcomes,
        replay_rows=replay_rows,
        forward_capture_rows=forward_rows,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "side_session_replay": side_session_replay_path,
            "forward_path_capture": forward_capture_path,
        },
    )
    summary = build_trade_path_reconstruction_summary(
        rows,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "side_session_replay": side_session_replay_path,
            "forward_path_capture": forward_capture_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    path_jsonl_path = output_dir / PATH_JSONL
    summary_json_path = output_dir / SUMMARY_JSON
    summary_markdown_path = output_dir / SUMMARY_MD
    contract_path = output_dir / CONTRACT_MD
    excursion_report_path = output_dir / EXCURSION_MD
    forward_report_path = output_dir / FORWARD_MD
    readiness_report_path = output_dir / READINESS_MD
    _write_jsonl(path_jsonl_path, rows)
    _write_json(summary_json_path, summary)
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    excursion_report_path.write_text(render_excursion_markdown(summary), encoding="utf-8")
    forward_report_path.write_text(render_forward_markdown(summary), encoding="utf-8")
    readiness_report_path.write_text(render_readiness_markdown(summary), encoding="utf-8")
    return TradePathReconstructionResult(
        rows=rows,
        summary=summary,
        path_jsonl_path=path_jsonl_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        contract_path=contract_path,
        excursion_report_path=excursion_report_path,
        forward_report_path=forward_report_path,
        readiness_report_path=readiness_report_path,
    )


def build_trade_path_reconstructions(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    replay_rows: Sequence[Mapping[str, Any]] = (),
    forward_capture_rows: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    replay_index = _ReplayIndex(replay_rows)
    forward_index = _ForwardCaptureIndex(forward_capture_rows)
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        replay = replay_index.find(outcome)
        entry_path = _normalized_entry_path(replay)
        entry_metrics = _entry_path_metrics(outcome=outcome, entry_path=entry_path, replay=replay)
        post_exit = _post_exit_windows(outcome=outcome, forward_index=forward_index)
        readiness = _counterfactual_readiness(entry_path=entry_path, entry_metrics=entry_metrics, post_exit=post_exit)
        row = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "trade_path_reconstruction_id": _stable_id("trade_path", outcome.get("trade_outcome_id"), outcome.get("entry_time"), outcome.get("exit_time")),
            "trade_outcome_id": outcome.get("trade_outcome_id"),
            "source_trade_id": _source_trade_id(outcome),
            "lane_id": outcome.get("lane_id"),
            "strategy_id": outcome.get("strategy_id"),
            "instrument": outcome.get("instrument"),
            "contract": outcome.get("contract"),
            "side": outcome.get("side"),
            "session": outcome.get("session_at_entry"),
            "entry_time": outcome.get("entry_time"),
            "exit_time": outcome.get("exit_time"),
            "entry_price": outcome.get("entry_price"),
            "exit_price": outcome.get("exit_price"),
            "realized_pnl_proxy": outcome.get("realized_pnl_proxy"),
            "realized_points": outcome.get("realized_points"),
            "entry_to_exit_path_status": "AVAILABLE" if entry_path else "MISSING",
            "entry_to_exit_path_bar_count": len(entry_path),
            "entry_to_exit_path": entry_path,
            "mfe_points": entry_metrics.get("mfe_points"),
            "mae_points": entry_metrics.get("mae_points"),
            "time_to_mfe_seconds": entry_metrics.get("time_to_mfe_seconds"),
            "time_to_mae_seconds": entry_metrics.get("time_to_mae_seconds"),
            "profitable_before_exit": entry_metrics.get("profitable_before_exit"),
            "profitable_before_exit_but_gave_back": entry_metrics.get("profitable_before_exit_but_gave_back"),
            "exit_capture_ratio": entry_metrics.get("exit_capture_ratio"),
            "post_exit_forward_windows": post_exit,
            "counterfactual_readiness": readiness,
            "data_quality_flags": _data_quality_flags(entry_path=entry_path, entry_metrics=entry_metrics, post_exit=post_exit),
            "source_refs": {
                "ctol_trade_outcome_id": outcome.get("trade_outcome_id"),
                "replay_trade_id": replay.get("trade_id") if replay else None,
                "replay_candle_source": replay.get("candle_source") if replay else None,
                "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
            },
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        }
        row["deterministic_fingerprint"] = _fingerprint({k: v for k, v in row.items() if k not in {"generated_at", "deterministic_fingerprint"}})
        rows.append(row)
    rows.sort(key=lambda row: (str(row.get("exit_time") or ""), str(row.get("trade_outcome_id") or "")))
    return rows


def build_trade_path_reconstruction_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total = len(rows)
    entry_path_count = sum(1 for row in rows if row.get("entry_to_exit_path_status") == "AVAILABLE")
    mfe_count = sum(1 for row in rows if row.get("mfe_points") is not None)
    mae_count = sum(1 for row in rows if row.get("mae_points") is not None)
    forward_coverage = {
        f"{minutes}m": sum(1 for row in rows if row.get("post_exit_forward_windows", {}).get(f"{minutes}m", {}).get("available") is True)
        for minutes in FORWARD_WINDOWS_MINUTES
    }
    lane_summary = _lane_summary(rows)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "completed_trades": total,
            "entry_to_exit_path_available_count": entry_path_count,
            "entry_to_exit_path_coverage": _rate(entry_path_count, total),
            "mfe_available_count": mfe_count,
            "mfe_coverage": _rate(mfe_count, total),
            "mae_available_count": mae_count,
            "mae_coverage": _rate(mae_count, total),
            "profitable_before_exit_count": sum(1 for row in rows if row.get("profitable_before_exit") is True),
            "gave_back_count": sum(1 for row in rows if row.get("profitable_before_exit_but_gave_back") is True),
            "timebox_counterfactual_ready_count": sum(1 for row in rows if row.get("counterfactual_readiness", {}).get("timebox_grid") == "READY"),
            "trailing_exit_ready_count": sum(1 for row in rows if row.get("counterfactual_readiness", {}).get("trailing_exit") == "READY"),
            "post_exit_forward_coverage": {key: _rate(value, total) for key, value in forward_coverage.items()},
        },
        "distributions": {
            "entry_to_exit_path_status": _counts(row.get("entry_to_exit_path_status") for row in rows),
            "counterfactual_timebox_grid": _counts(row.get("counterfactual_readiness", {}).get("timebox_grid") for row in rows),
            "data_quality_flags": _counts(flag for row in rows for flag in row.get("data_quality_flags", [])),
            "instrument": _counts(row.get("instrument") for row in rows),
            "session": _counts(row.get("session") for row in rows),
        },
        "lane_path_coverage": lane_summary,
        "top_giveback_trades": _top_giveback_trades(rows),
        "post_exit_forward_availability": forward_coverage,
        "next_steps": [
            "Populate timestamped in-trade candle paths for all closed trades from historical Parquet/backfill sources.",
            "Persist post-exit forward windows by trade, not only symbol snapshots.",
            "Feed this artifact into RA4 so timebox grids, trailing exits, ATR exits, and VWAP/AVWAP exits become testable.",
        ],
    }


class _ReplayIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_trade_id: dict[str, Mapping[str, Any]] = {}
        self._by_tuple: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
        for row in rows:
            trade_id = row.get("trade_id")
            if trade_id:
                self._by_trade_id.setdefault(str(trade_id), row)
            key = (
                _norm_ts(row.get("entry_time")),
                _norm_ts(row.get("exit_time")),
                str(row.get("symbol") or ""),
                str(row.get("lane_id") or ""),
            )
            self._by_tuple.setdefault(key, row)

    def find(self, outcome: Mapping[str, Any]) -> Mapping[str, Any]:
        source_trade_id = _source_trade_id(outcome)
        if source_trade_id and source_trade_id in self._by_trade_id:
            return self._by_trade_id[source_trade_id]
        key = (
            _norm_ts(outcome.get("entry_time")),
            _norm_ts(outcome.get("exit_time")),
            str(outcome.get("instrument") or ""),
            str(outcome.get("lane_id") or ""),
        )
        return self._by_tuple.get(key, {})


class _ForwardCaptureIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_symbol: dict[str, dict[str, Mapping[str, Any]]] = {}
        for row in rows:
            if row.get("event_type") != "FORWARD_PATH_CANDLE_CAPTURE":
                continue
            if str(row.get("timeframe") or "") != "1m":
                continue
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            symbol_rows = by_symbol.setdefault(symbol, {})
            bars = row.get("bars") if isinstance(row.get("bars"), list) else []
            for bar in bars:
                if not isinstance(bar, Mapping):
                    continue
                ts = _parse_ts(bar.get("bar_end") or bar.get("timestamp"))
                if ts is None:
                    continue
                symbol_rows[ts.isoformat()] = bar
        self._bars = {
            symbol: sorted(rows.values(), key=lambda bar: _parse_ts(bar.get("bar_end") or bar.get("timestamp")) or datetime.min.replace(tzinfo=UTC))
            for symbol, rows in by_symbol.items()
        }

    def bars_after(self, *, symbol: str, start: datetime, end: datetime) -> list[Mapping[str, Any]]:
        selected = []
        for bar in self._bars.get(symbol.upper(), []):
            ts = _parse_ts(bar.get("bar_end") or bar.get("timestamp"))
            if ts is not None and start < ts <= end:
                selected.append(bar)
        return selected


def _normalized_entry_path(replay: Mapping[str, Any]) -> list[dict[str, Any]]:
    path = replay.get("entry_to_exit_path") if isinstance(replay.get("entry_to_exit_path"), list) else []
    normalized = []
    for item in path:
        if not isinstance(item, Mapping):
            continue
        normalized.append(
            {
                "bar_end": item.get("bar_end"),
                "high": _float_or_none(item.get("high")),
                "low": _float_or_none(item.get("low")),
                "close": _float_or_none(item.get("close")),
                "favorable_excursion_points": _float_or_none(item.get("favorable_excursion_points")),
                "adverse_excursion_points": _float_or_none(item.get("adverse_excursion_points")),
                "close_pnl_points": _float_or_none(item.get("close_pnl_points")),
            }
        )
    return normalized


def _entry_path_metrics(*, outcome: Mapping[str, Any], entry_path: Sequence[Mapping[str, Any]], replay: Mapping[str, Any]) -> dict[str, Any]:
    if not entry_path:
        return {
            "mfe_points": None,
            "mae_points": None,
            "time_to_mfe_seconds": None,
            "time_to_mae_seconds": None,
            "profitable_before_exit": None,
            "profitable_before_exit_but_gave_back": None,
            "exit_capture_ratio": None,
        }
    mfe = max((_float_or_none(row.get("favorable_excursion_points")) for row in entry_path), default=None)
    mae = min((_float_or_none(row.get("adverse_excursion_points")) for row in entry_path), default=None)
    time_to_mfe = replay.get("time_to_mfe_seconds")
    time_to_mae = replay.get("time_to_mae_seconds")
    realized_points = _float_or_none(outcome.get("realized_points"))
    profitable_before = mfe is not None and mfe > 0
    gave_back = bool(profitable_before and realized_points is not None and (realized_points <= 0 or realized_points < mfe * 0.25))
    capture_ratio = None
    if mfe is not None and mfe > 0 and realized_points is not None:
        capture_ratio = _round(realized_points / mfe)
    return {
        "mfe_points": _round(mfe),
        "mae_points": _round(mae),
        "time_to_mfe_seconds": _int_or_none(time_to_mfe),
        "time_to_mae_seconds": _int_or_none(time_to_mae),
        "profitable_before_exit": profitable_before,
        "profitable_before_exit_but_gave_back": gave_back,
        "exit_capture_ratio": capture_ratio,
    }


def _post_exit_windows(*, outcome: Mapping[str, Any], forward_index: _ForwardCaptureIndex) -> dict[str, Any]:
    exit_time = _parse_ts(outcome.get("exit_time"))
    entry_price = _float_or_none(outcome.get("entry_price"))
    side = str(outcome.get("side") or "").upper()
    symbol = str(outcome.get("instrument") or "").upper()
    if exit_time is None or entry_price is None or not symbol:
        return {
            f"{minutes}m": {"available": False, "bar_count": 0, "pnl_points": None, "best_favorable_points": None, "worst_adverse_points": None, "missing_reason": "missing_trade_or_symbol_context"}
            for minutes in FORWARD_WINDOWS_MINUTES
        }
    windows: dict[str, Any] = {}
    for minutes in FORWARD_WINDOWS_MINUTES:
        bars = forward_index.bars_after(symbol=symbol, start=exit_time, end=exit_time + timedelta(minutes=minutes))
        if not bars:
            windows[f"{minutes}m"] = {
                "available": False,
                "bar_count": 0,
                "pnl_points": None,
                "best_favorable_points": None,
                "worst_adverse_points": None,
                "missing_reason": "missing_post_exit_forward_bars",
            }
            continue
        last = bars[-1]
        last_close = _float_or_none(last.get("close"))
        pnl = _signed_points(side=side, entry_price=entry_price, price=last_close)
        favorable = []
        adverse = []
        for bar in bars:
            high = _float_or_none(bar.get("high"))
            low = _float_or_none(bar.get("low"))
            if high is None or low is None:
                continue
            if side == "SHORT":
                favorable.append(entry_price - low)
                adverse.append(entry_price - high)
            else:
                favorable.append(high - entry_price)
                adverse.append(low - entry_price)
        windows[f"{minutes}m"] = {
            "available": True,
            "bar_count": len(bars),
            "last_bar_end": bars[-1].get("bar_end") or bars[-1].get("timestamp"),
            "pnl_points": _round(pnl),
            "best_favorable_points": _round(max(favorable)) if favorable else None,
            "worst_adverse_points": _round(min(adverse)) if adverse else None,
            "bars": _compact_bars(bars),
        }
    return windows


def _counterfactual_readiness(
    *,
    entry_path: Sequence[Mapping[str, Any]],
    entry_metrics: Mapping[str, Any],
    post_exit: Mapping[str, Any],
) -> dict[str, str]:
    has_entry = bool(entry_path)
    has_mfe_mae = entry_metrics.get("mfe_points") is not None and entry_metrics.get("mae_points") is not None
    has_forward_60 = post_exit.get("60m", {}).get("available") is True
    return {
        "timebox_grid": "READY" if has_entry and has_forward_60 else "BLOCKED_MISSING_ENTRY_PATH_OR_FORWARD_PATH",
        "mfe_mae": "READY" if has_mfe_mae else "BLOCKED_MISSING_ENTRY_PATH",
        "trailing_exit": "READY" if has_entry and has_mfe_mae else "BLOCKED_MISSING_EXCURSION_CURVE",
        "atr_exit": "BLOCKED_MISSING_ATR_SERIES",
        "vwap_exit": "BLOCKED_MISSING_VWAP_AVWAP_EXIT_STATE",
    }


def _data_quality_flags(*, entry_path: Sequence[Mapping[str, Any]], entry_metrics: Mapping[str, Any], post_exit: Mapping[str, Any]) -> list[str]:
    flags = []
    if not entry_path:
        flags.append("missing_entry_to_exit_path")
    if entry_metrics.get("mfe_points") is None:
        flags.append("missing_mfe")
    if entry_metrics.get("mae_points") is None:
        flags.append("missing_mae")
    if not any(window.get("available") for window in post_exit.values()):
        flags.append("missing_post_exit_forward_path")
    if post_exit.get("60m", {}).get("available") is not True:
        flags.append("missing_60m_post_exit_forward_path")
    flags.extend(["missing_atr_series", "missing_vwap_avwap_exit_state"])
    return sorted(set(flags))


def _lane_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("lane_id") or "UNKNOWN"), []).append(row)
    summaries = []
    for lane, lane_rows in groups.items():
        path_count = sum(1 for row in lane_rows if row.get("entry_to_exit_path_status") == "AVAILABLE")
        gave_back = sum(1 for row in lane_rows if row.get("profitable_before_exit_but_gave_back") is True)
        pnls = [_float_or_none(row.get("realized_pnl_proxy")) for row in lane_rows]
        pnls = [value for value in pnls if value is not None]
        summaries.append(
            {
                "lane_id": lane,
                "trade_count": len(lane_rows),
                "instrument": _mode(row.get("instrument") for row in lane_rows),
                "session": _mode(row.get("session") for row in lane_rows),
                "path_available_count": path_count,
                "path_coverage": _rate(path_count, len(lane_rows)),
                "gave_back_count": gave_back,
                "average_pnl_proxy": _round(sum(pnls) / len(pnls)) if pnls else None,
                "median_pnl_proxy": _round(median(pnls)) if pnls else None,
                "counterfactual_ready_count": sum(1 for row in lane_rows if row.get("counterfactual_readiness", {}).get("timebox_grid") == "READY"),
            }
        )
    summaries.sort(key=lambda row: (row["counterfactual_ready_count"], row["path_available_count"], row["trade_count"]), reverse=True)
    return summaries


def _top_giveback_trades(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    candidates = [row for row in rows if row.get("profitable_before_exit_but_gave_back") is True]
    candidates.sort(key=lambda row: _float_or_none(row.get("exit_capture_ratio")) if row.get("exit_capture_ratio") is not None else -999)
    return [
        {
            "trade_outcome_id": row.get("trade_outcome_id"),
            "lane_id": row.get("lane_id"),
            "instrument": row.get("instrument"),
            "mfe_points": row.get("mfe_points"),
            "realized_points": row.get("realized_points"),
            "exit_capture_ratio": row.get("exit_capture_ratio"),
        }
        for row in candidates[:25]
    ]


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    return "\n".join(
        [
            "# Trade Path Reconstruction Summary",
            "",
            "Research/diagnostic only. This artifact reconstructs price paths and excursion metrics from already-retained market data.",
            "",
            f"- Completed trades: `{overall.get('completed_trades')}`",
            f"- Entry-to-exit path coverage: `{overall.get('entry_to_exit_path_available_count')}` / `{overall.get('completed_trades')}` (`{overall.get('entry_to_exit_path_coverage')}`)",
            f"- MFE coverage: `{overall.get('mfe_coverage')}`",
            f"- MAE coverage: `{overall.get('mae_coverage')}`",
            f"- Timebox-grid counterfactual ready: `{overall.get('timebox_counterfactual_ready_count')}`",
            f"- Trailing-exit ready: `{overall.get('trailing_exit_ready_count')}`",
        ]
    ) + "\n"


def render_contract_markdown() -> str:
    return """# Trade Path Reconstruction Contract

`canonical_trade_path_reconstruction_v1` is a read-only research artifact.

It may include:

- entry-to-exit timestamped path rows when retained market data exists
- MFE/MAE and time-to-MFE/MAE
- post-exit forward windows when retained forward bars exist
- counterfactual readiness flags

It must not submit, cancel, modify, close, flatten, restart, gate, or change strategy behavior.
"""


def render_excursion_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Excursion Capture Coverage",
        "",
        "|metric|value|",
        "|---|---:|",
    ]
    overall = summary.get("overall", {})
    for key in ("mfe_available_count", "mfe_coverage", "mae_available_count", "mae_coverage", "profitable_before_exit_count", "gave_back_count"):
        lines.append(f"|{key}|{overall.get(key)}|")
    return "\n".join(lines) + "\n"


def render_forward_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Post-Exit Forward Path Report",
        "",
        "|window|available_count|coverage|",
        "|---|---:|---:|",
    ]
    counts = summary.get("post_exit_forward_availability", {})
    coverage = summary.get("overall", {}).get("post_exit_forward_coverage", {})
    for key in sorted(counts):
        lines.append(f"|{key}|{counts.get(key)}|{coverage.get(key)}|")
    return "\n".join(lines) + "\n"


def render_readiness_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Counterfactual Readiness Report",
        "",
        "Diagnostic readiness only. This is not a live exit recommendation.",
        "",
        "## Readiness Distribution",
        "",
        _dict_table(summary.get("distributions", {}).get("counterfactual_timebox_grid", {})),
        "",
        "## Lane Coverage",
        "",
        "|lane|trades|path|ready|avg_pnl|",
        "|---|---:|---:|---:|---:|",
    ]
    for row in summary.get("lane_path_coverage", [])[:30]:
        lines.append(
            f"|{row.get('lane_id')}|{row.get('trade_count')}|{row.get('path_available_count')}|{row.get('counterfactual_ready_count')}|{row.get('average_pnl_proxy')}|"
        )
    return "\n".join(lines) + "\n"


def _compact_bars(bars: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "bar_end": bar.get("bar_end") or bar.get("timestamp"),
            "high": _float_or_none(bar.get("high")),
            "low": _float_or_none(bar.get("low")),
            "close": _float_or_none(bar.get("close")),
        }
        for bar in bars
    ]


def _signed_points(*, side: str, entry_price: float, price: float | None) -> float | None:
    if price is None:
        return None
    if side == "SHORT":
        return entry_price - price
    return price - entry_price


def _source_trade_id(outcome: Mapping[str, Any]) -> str | None:
    refs = outcome.get("source_refs") if isinstance(outcome.get("source_refs"), Mapping) else {}
    value = refs.get("source_trade_id")
    return str(value) if value else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _norm_ts(value: Any) -> str:
    parsed = _parse_ts(value)
    return parsed.isoformat() if parsed else str(value or "")


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value if value not in (None, "") else "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _mode(values: Any) -> str | None:
    counts = _counts(value for value in values if value not in (None, ""))
    if not counts:
        return None
    return next(iter(counts))


def _dict_table(values: Mapping[str, Any]) -> str:
    if not values:
        return "No rows."
    lines = ["|key|count|", "|---|---:|"]
    for key, value in values.items():
        lines.append(f"|{key}|{value}|")
    return "\n".join(lines)


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
