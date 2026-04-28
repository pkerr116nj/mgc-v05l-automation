"""Build a path-complete replay-safe GC Asia-only substrate from source bars."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .atp_companion_full_history_review import (
    DEFAULT_SOURCE_DB,
    _discover_best_sources,
    _minute_path_rows,
    _trade_record_replay_row,
)
from .atp_companion_replay_baseline_v2 import (
    DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    _load_scope_bundle,
    _manifest_1m_span,
    _scope_bundle_manifest_path,
    _write_jsonl,
)
from ..research.platform import ensure_symbol_context_bundle
from ..research.trend_participation.substrate import _read_manifest

REPO_ROOT = Path.cwd()
DEFAULT_GC_MANIFEST = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2_gc_expression"
    / "gc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_gc_manifest.json"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_only_path_complete_replay"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-only-path-complete-replay")
    parser.add_argument("--gc-manifest", default=str(DEFAULT_GC_MANIFEST), help="Committed replay-safe GC manifest path.")
    parser.add_argument("--source-db", default=None, help="Optional explicit SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _load_gc_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _trade_window_with_same_bar_support(*, trade: Any, bars_1m: Sequence[Any], bars_by_end_ts: dict[Any, int]) -> list[Any]:
    start_index = bars_by_end_ts.get(trade.entry_ts)
    end_index = bars_by_end_ts.get(trade.exit_ts)
    if start_index is None or end_index is None:
        return []
    if end_index < start_index:
        return []
    if end_index == start_index:
        return [bars_1m[end_index]]
    return list(bars_1m[start_index + 1 : end_index + 1])


def _enrich_minute_path(*, trade: Any, minute_path: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    if not minute_path:
        return []
    side = str(trade.side)
    initial_risk = max(abs(float(trade.entry_price) - float(trade.stop_price)), 1e-9)
    running_mfe_points = 0.0
    running_mae_points = 0.0
    running_peak_favorable_r = 0.0
    output: list[dict[str, Any]] = []
    reached_1r = False
    for row in minute_path:
        high_r = float(row["high_progress_r_multiple"])
        low_r = float(row["low_progress_r_multiple"])
        favorable_r = high_r if side == "LONG" else high_r
        adverse_r = max(0.0, -low_r) if side == "LONG" else max(0.0, -low_r)
        running_peak_favorable_r = max(running_peak_favorable_r, favorable_r)
        running_mfe_points = max(running_mfe_points, favorable_r * initial_risk)
        running_mae_points = max(running_mae_points, adverse_r * initial_risk)
        close_distance_points = float(row["close"]) - float(trade.entry_price) if side == "LONG" else float(trade.entry_price) - float(row["close"])
        baseline_stop_touched = float(row["low"]) <= float(trade.stop_price) if side == "LONG" else float(row["high"]) >= float(trade.stop_price)
        baseline_target_touched = (
            float(trade.target_price) is not None
            and ((float(row["high"]) >= float(trade.target_price)) if side == "LONG" else (float(row["low"]) <= float(trade.target_price)))
        )
        reached_1r = reached_1r or favorable_r >= 1.0
        reference_breakeven_after_1r_touched = reached_1r and ((float(row["low"]) <= float(trade.entry_price)) if side == "LONG" else (float(row["high"]) >= float(trade.entry_price)))
        reference_trailing_stop_1r_price = (
            float(trade.entry_price) + max(running_peak_favorable_r - 1.0, 0.0) * initial_risk
            if side == "LONG"
            else float(trade.entry_price) - max(running_peak_favorable_r - 1.0, 0.0) * initial_risk
        )
        reference_trailing_stop_1r_touched = (
            reached_1r
            and ((float(row["low"]) <= reference_trailing_stop_1r_price) if side == "LONG" else (float(row["high"]) >= reference_trailing_stop_1r_price))
        )
        reference_giveback_05r_after_1r_touched = reached_1r and ((running_peak_favorable_r - low_r) >= 0.5)
        output.append(
            {
                **row,
                "timestamp": row["end_ts"],
                "distance_from_entry_close_points": round(float(close_distance_points), 6),
                "running_mfe_points": round(float(running_mfe_points), 6),
                "running_mae_points": round(float(running_mae_points), 6),
                "reached_plus_0_5r": favorable_r >= 0.5 or running_peak_favorable_r >= 0.5,
                "reached_plus_1_0r": favorable_r >= 1.0 or running_peak_favorable_r >= 1.0,
                "reached_plus_1_5r": favorable_r >= 1.5 or running_peak_favorable_r >= 1.5,
                "baseline_stop_touched": bool(baseline_stop_touched),
                "baseline_target_touched": bool(baseline_target_touched),
                "reference_breakeven_after_1r_touched": bool(reference_breakeven_after_1r_touched),
                "reference_trailing_stop_1r_price": round(float(reference_trailing_stop_1r_price), 6),
                "reference_trailing_stop_1r_touched": bool(reference_trailing_stop_1r_touched),
                "reference_giveback_0_5r_after_1r_touched": bool(reference_giveback_05r_after_1r_touched),
            }
        )
    return output


def _trade_row_payload(*, row: dict[str, Any], minute_path: Sequence[dict[str, Any]]) -> dict[str, Any]:
    trade = row["trade_record"]
    initial_risk = max(abs(float(trade.entry_price) - float(trade.stop_price)), 1e-9)
    return {
        "trade_id": str(row["trade_id"]),
        "instrument": trade.instrument,
        "session": trade.session_segment,
        "direction": trade.side,
        "decision_ts": trade.decision_ts,
        "entry_ts": trade.entry_ts,
        "exit_ts": trade.exit_ts,
        "entry_price": float(trade.entry_price),
        "exit_price": float(trade.exit_price),
        "baseline_pnl_cash": float(trade.pnl_cash),
        "stop_price": float(trade.stop_price),
        "target_price": float(trade.target_price) if trade.target_price is not None else None,
        "initial_risk_points": round(float(initial_risk), 6),
        "r_unit_points": round(float(initial_risk), 6),
        "point_value": 100.0,
        "bars_held_1m": int(trade.bars_held_1m),
        "hold_minutes": float(trade.hold_minutes),
        "exit_reason": trade.exit_reason,
        "family": trade.family,
        "trade_record": _trade_record_replay_row(trade),
        "minute_path": list(minute_path),
    }


def _trade_report_row(*, row: dict[str, Any], minute_path: Sequence[dict[str, Any]]) -> dict[str, Any]:
    trade = row["trade_record"]
    path_count = len(minute_path)
    expected_path_count = max(int(trade.bars_held_1m) - 1, 1)
    first_row = minute_path[0] if minute_path else None
    last_row = minute_path[-1] if minute_path else None
    entry_boundary_ok = bool(first_row and str(first_row["start_ts"]) <= str(trade.entry_ts) <= str(first_row["end_ts"]))
    exit_boundary_ok = bool(last_row and str(last_row["end_ts"]) == str(trade.exit_ts))
    baseline_stop_touched = any(bool(path_row.get("baseline_stop_touched")) for path_row in minute_path)
    baseline_target_touched = any(bool(path_row.get("baseline_target_touched")) for path_row in minute_path)
    if trade.exit_reason == "stop":
        exit_reproduced = baseline_stop_touched
    elif trade.exit_reason == "target":
        exit_reproduced = baseline_target_touched
    elif trade.exit_reason == "time_stop":
        exit_reproduced = exit_boundary_ok
    else:
        exit_reproduced = False
    coverage_ratio = (path_count / max(expected_path_count, 1)) if expected_path_count > 0 else 0.0
    return {
        "report_scope": "TRADE",
        "trade_id": str(row["trade_id"]),
        "entry_ts": trade.entry_ts,
        "exit_ts": trade.exit_ts,
        "exit_reason": trade.exit_reason,
        "bars_held_1m_expected": int(trade.bars_held_1m),
        "minute_path_expected_count": expected_path_count,
        "minute_path_count": path_count,
        "non_empty_minute_path": bool(path_count > 0),
        "entry_boundary_ok": bool(entry_boundary_ok),
        "exit_boundary_ok": bool(exit_boundary_ok),
        "path_start_ts": first_row["start_ts"] if first_row else "",
        "path_end_ts": last_row["end_ts"] if last_row else "",
        "path_coverage_ratio": round(float(coverage_ratio), 4),
        "baseline_stop_touched": bool(baseline_stop_touched),
        "baseline_target_touched": bool(baseline_target_touched),
        "exit_reason_reproduced_from_path": bool(exit_reproduced),
        "missing_reason": "" if path_count > 0 else "NO_1M_WINDOW",
    }


def _overall_report_row(trade_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    trade_count = len(trade_rows)
    non_empty = sum(1 for row in trade_rows if bool(row["non_empty_minute_path"]))
    missing = trade_count - non_empty
    entry_ok = sum(1 for row in trade_rows if bool(row["entry_boundary_ok"]))
    exit_ok = sum(1 for row in trade_rows if bool(row["exit_boundary_ok"]))
    reproduced = sum(1 for row in trade_rows if bool(row["exit_reason_reproduced_from_path"]))
    return {
        "report_scope": "OVERALL",
        "trade_id": "",
        "entry_ts": "",
        "exit_ts": "",
        "exit_reason": "",
        "bars_held_1m_expected": "",
        "minute_path_expected_count": "",
        "minute_path_count": "",
        "non_empty_minute_path": "",
        "entry_boundary_ok": "",
        "exit_boundary_ok": "",
        "path_start_ts": "",
        "path_end_ts": "",
        "path_coverage_ratio": "",
        "baseline_stop_touched": "",
        "baseline_target_touched": "",
        "exit_reason_reproduced_from_path": "",
        "missing_reason": "",
        "trade_count": trade_count,
        "non_empty_trade_count": non_empty,
        "missing_path_count": missing,
        "pct_non_empty_minute_path": round((non_empty / max(trade_count, 1) * 100.0), 4),
        "pct_entry_boundary_ok": round((entry_ok / max(trade_count, 1) * 100.0), 4),
        "pct_exit_boundary_ok": round((exit_ok / max(trade_count, 1) * 100.0), 4),
        "pct_exit_reproduced_from_path": round((reproduced / max(trade_count, 1) * 100.0), 4),
    }


def _classification(overall: dict[str, Any]) -> str:
    pct_non_empty = float(overall["pct_non_empty_minute_path"])
    pct_entry_ok = float(overall["pct_entry_boundary_ok"])
    pct_exit_ok = float(overall["pct_exit_boundary_ok"])
    pct_reproduced = float(overall["pct_exit_reproduced_from_path"])
    if pct_non_empty >= 99.0 and pct_entry_ok >= 95.0 and pct_exit_ok >= 95.0 and pct_reproduced >= 85.0:
        return "PATH_COMPLETE_REPLAY_BUILT"
    if pct_non_empty >= 70.0:
        return "PATH_COMPLETE_REPLAY_PARTIAL"
    return "PATH_COMPLETE_REPLAY_BLOCKED"


def _render_markdown(*, classification: str, overall: dict[str, Any], gc_manifest: dict[str, Any]) -> str:
    lines = [
        "# ATP Companion GC Asia-Only Path-Complete Replay",
        "",
        f"- Classification: `{classification}`",
        "- This pass builds a path-complete replay-safe GC Asia-only substrate from source bars.",
        "- No strategy logic, thresholds, exits, staged adds, live execution, IBKR, or broker code were changed.",
        f"- Source GC scope bundle: `{((gc_manifest.get('bundle_contract') or {}).get('scope_bundle_ids') or {}).get('GC:ASIA/US', '')}`",
        "",
        "## Completeness",
        f"- Trade count: `{overall['trade_count']}`",
        f"- Non-empty minute paths: `{overall['non_empty_trade_count']}` (`{overall['pct_non_empty_minute_path']}%`)",
        f"- Missing path count: `{overall['missing_path_count']}`",
        f"- Entry boundary checks passing: `{overall['pct_entry_boundary_ok']}%`",
        f"- Exit boundary checks passing: `{overall['pct_exit_boundary_ok']}%`",
        f"- Exit reason approximately reproduced from path: `{overall['pct_exit_reproduced_from_path']}%`",
        "",
        "## Discipline",
        "- Do not run mirrored short or exit-redesign counterfactuals unless the path completeness classification is acceptable.",
        "- Do not approximate intratrade paths from summary trade rows.",
    ]
    return "\n".join(lines) + "\n"


def build_path_complete_replay(*, gc_manifest_path: Path, source_db: Path | None, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gc_manifest = _load_gc_manifest(gc_manifest_path)
    scope_bundle_id = (((gc_manifest.get("bundle_contract") or {}).get("scope_bundle_ids") or {}).get("GC:ASIA/US"))
    if not scope_bundle_id:
        raise RuntimeError("GC manifest does not contain a GC:ASIA/US scope bundle id.")
    source_db_path = Path(source_db or gc_manifest.get("source_db") or DEFAULT_SOURCE_DB).expanduser().resolve()
    scope_manifest = _read_manifest(_scope_bundle_manifest_path(scope_bundle_id))
    trade_rows = _load_scope_bundle(scope_bundle_id)[1]
    asia_trade_rows = [row for row in trade_rows if getattr(row["trade_record"], "session_segment", None) == "ASIA"]
    run_start, run_end = _manifest_1m_span(scope_manifest)

    bar_source_index = _discover_best_sources(symbols={"GC"}, timeframes={"1m", "5m"}, sqlite_paths=[source_db_path])
    context_bundle = ensure_symbol_context_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        symbol="GC",
        bar_source_index=bar_source_index,
        start_timestamp=run_start,
        end_timestamp=run_end,
    )
    if context_bundle is None:
        raise RuntimeError("GC Asia-only path-complete replay could not materialize the GC 1m/5m context bundle for the selected span.")

    bars_1m = context_bundle["bars_1m"]
    bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(bars_1m)}
    replay_rows: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []
    for row in asia_trade_rows:
        trade = row["trade_record"]
        minute_bars = _trade_window_with_same_bar_support(trade=trade, bars_1m=bars_1m, bars_by_end_ts=bars_by_end_ts)
        minute_path = _enrich_minute_path(trade=trade, minute_path=_minute_path_rows(trade=trade, minute_bars=minute_bars))
        replay_rows.append(_trade_row_payload(row=row, minute_path=minute_path))
        report_rows.append(_trade_report_row(row=row, minute_path=minute_path))
    overall = _overall_report_row(report_rows)
    classification = _classification(overall)
    for row in report_rows:
        row["classification"] = classification
    overall["classification"] = classification
    report_with_overall = [overall, *report_rows]

    replay_jsonl_path = output_dir / "atp_gc_asia_only_path_complete_replay_trades.jsonl"
    report_csv_path = output_dir / "atp_gc_asia_only_path_completeness_report.csv"
    summary_md_path = output_dir / "atp_gc_asia_only_path_completeness_summary.md"

    _write_jsonl(replay_jsonl_path, iter(replay_rows))
    _write_csv(report_csv_path, report_with_overall)
    _write_markdown(summary_md_path, _render_markdown(classification=classification, overall=overall, gc_manifest=gc_manifest))

    return {
        "replay_jsonl_path": replay_jsonl_path,
        "report_csv_path": report_csv_path,
        "summary_md_path": summary_md_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_path_complete_replay(
        gc_manifest_path=Path(args.gc_manifest).expanduser().resolve(),
        source_db=Path(args.source_db).expanduser().resolve() if args.source_db else None,
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
