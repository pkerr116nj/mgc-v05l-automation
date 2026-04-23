"""Walk-forward stability research for forced-session GC/MGC gold segment playbooks."""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_segment_forced_session_walkforward"
DEFAULT_TEST_WINDOW_DATES = 20


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-segment-forced-session-walkforward-research")
    parser.add_argument("--source-json", required=True, help="Path to gc_mgc_segment_forced_session_long_research JSON artifact.")
    parser.add_argument("--source-variant", action="append", default=None, help="Forced-session variant id to evaluate. May be supplied multiple times.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    parser.add_argument("--test-window-dates", type=int, default=DEFAULT_TEST_WINDOW_DATES, help="Number of trade dates per walk-forward fold.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_segment_forced_session_walkforward_research(
        source_json=Path(args.source_json),
        source_variants=tuple(args.source_variant) if args.source_variant else None,
        output_dir=Path(args.output_dir),
        test_window_dates=int(args.test_window_dates),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_segment_forced_session_walkforward_research(
    *,
    source_json: Path,
    source_variants: tuple[str, ...] | None,
    output_dir: Path | None = None,
    test_window_dates: int = DEFAULT_TEST_WINDOW_DATES,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report = json.loads(resolved_source_json.read_text(encoding="utf-8"))
    resolved_variants = tuple(source_variants or report["symbol_reports"]["GC"]["variants"].keys())

    variant_reports: list[dict[str, Any]] = []
    for source_variant in resolved_variants:
        sessions = _extract_entered_sessions(report=report, source_variant=source_variant)
        unique_dates = sorted({row["trade_date"] for row in sessions})
        folds = _build_folds(unique_dates=unique_dates, test_window_dates=test_window_dates)
        fold_reports: list[dict[str, Any]] = []
        aggregate_vals: list[float] = []
        for fold_index, fold in enumerate(folds, start=1):
            fold_rows = [row for row in sessions if row["trade_date"] in fold]
            fold_vals = [row["net_pnl_points"] for row in fold_rows]
            aggregate_vals.extend(fold_vals)
            by_symbol = {}
            for symbol in sorted({row["symbol"] for row in fold_rows}):
                symbol_vals = [row["net_pnl_points"] for row in fold_rows if row["symbol"] == symbol]
                by_symbol[symbol] = _performance_summary(symbol_vals)
            fold_reports.append(
                {
                    "fold_index": fold_index,
                    "test_start_date": fold[0],
                    "test_end_date": fold[-1],
                    "test_date_count": len(fold),
                    "summary": _performance_summary(fold_vals),
                    "by_symbol": by_symbol,
                }
            )
        variant_reports.append(
            {
                "source_variant": source_variant,
                "aggregate_summary": _performance_summary(aggregate_vals),
                "fold_count": len(fold_reports),
                "fold_reports": fold_reports,
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_segment_forced_session_walkforward",
        "source_json": str(resolved_source_json),
        "segment_id": report.get("definition", {}).get("segment_id"),
        "source_variants": list(resolved_variants),
        "test_window_dates": test_window_dates,
        "variant_reports": variant_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_segment_forced_session_walkforward_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_segment_forced_session_walkforward_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_segment_forced_session_walkforward_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "variant_reports": variant_reports,
    }


def _extract_entered_sessions(*, report: dict[str, Any], source_variant: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol, payload in report["symbol_reports"].items():
        sessions = payload["variants"][source_variant]["sessions"]
        for session in sessions:
            if not session.get("entered") or session.get("net_pnl_points") is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": str(session["trade_date"]),
                    "net_pnl_points": float(session["net_pnl_points"]),
                }
            )
    rows.sort(key=lambda row: (row["trade_date"], row["symbol"]))
    return rows


def _build_folds(*, unique_dates: list[str], test_window_dates: int) -> list[list[str]]:
    folds: list[list[str]] = []
    for start in range(0, len(unique_dates), test_window_dates):
        fold = unique_dates[start : start + test_window_dates]
        if fold:
            folds.append(fold)
    return folds


def _performance_summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {
            "trade_count": 0,
            "average_net_pnl_points": None,
            "median_net_pnl_points": None,
            "net_profit_factor": None,
            "win_rate": None,
        }
    winners = [value for value in values if value > 0.0]
    losers = [value for value in values if value <= 0.0]
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    return {
        "trade_count": len(values),
        "average_net_pnl_points": round(statistics.fmean(values), 4),
        "median_net_pnl_points": round(statistics.median(values), 4),
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "win_rate": round(len(winners) / len(values), 4),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# GC/MGC {payload.get('segment_id') or 'SEGMENT'} Forced Session Walk-Forward Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Source JSON: `{payload['source_json']}`",
        f"- Test window dates: `{payload['test_window_dates']}`",
        "",
    ]
    for variant in payload["variant_reports"]:
        lines.append(f"## {variant['source_variant']}")
        lines.append("")
        lines.append(f"- Aggregate: `{variant['aggregate_summary']}`")
        lines.append(f"- Fold count: `{variant['fold_count']}`")
        for fold in variant["fold_reports"][:12]:
            lines.append(
                f"- Fold {fold['fold_index']} `{fold['test_start_date']}` to `{fold['test_end_date']}`: `{fold['summary']}`"
            )
        lines.append("")
    return "\n".join(lines)
