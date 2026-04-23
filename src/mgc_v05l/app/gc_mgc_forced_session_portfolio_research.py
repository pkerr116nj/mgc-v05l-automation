"""Portfolio-level analytics for combined GC/MGC forced-session segment studies."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_forced_session_portfolio"


@dataclass(frozen=True)
class PortfolioLaneSpec:
    lane_id: str
    source_json: Path
    source_variant: str


DEFAULT_LANES: tuple[PortfolioLaneSpec, ...] = (
    PortfolioLaneSpec(
        lane_id="ASIA_EARLY_LONG",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_asia_early_long_forced_session_archive_v2"
        / "gc_mgc_segment_forced_session_long_research.json",
        source_variant="segment_forced_long_v5_dip_reclaim_or_bar8",
    ),
    PortfolioLaneSpec(
        lane_id="ASIA_EARLY_SHORT",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_asia_early_short_forced_session_archive_v1"
        / "gc_mgc_segment_forced_session_short_research.json",
        source_variant="segment_forced_short_v2_reclaim_fail_or_bar7",
    ),
    PortfolioLaneSpec(
        lane_id="US_EARLY_SHORT",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_ny_early_short_forced_session_archive_v1"
        / "gc_mgc_ny_early_short_forced_session_research.json",
        source_variant="ny_forced_short_v2_reclaim_fail_or_bar7",
    ),
    PortfolioLaneSpec(
        lane_id="LONDON_EARLY_LONG",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_london_early_long_forced_session_archive_v2"
        / "gc_mgc_segment_forced_session_long_research.json",
        source_variant="segment_forced_long_v5_dip_reclaim_or_bar8",
    ),
    PortfolioLaneSpec(
        lane_id="US_MIDDAY_SHORT",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_ny_late_short_forced_session_archive_v1"
        / "gc_mgc_segment_forced_session_short_research.json",
        source_variant="segment_forced_short_v2_reclaim_fail_or_bar7",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-forced-session-portfolio-research")
    parser.add_argument(
        "--lane",
        action="append",
        default=None,
        help="Lane spec in lane_id|source_json|source_variant form. May be supplied multiple times.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    parser.add_argument(
        "--test-window-dates",
        type=int,
        default=20,
        help="Number of trade dates per portfolio walk-forward fold.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_forced_session_portfolio_research(
        lane_specs=_parse_lane_specs(args.lane) if args.lane else None,
        output_dir=Path(args.output_dir),
        test_window_dates=int(args.test_window_dates),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_forced_session_portfolio_research(
    *,
    lane_specs: tuple[PortfolioLaneSpec, ...] | None = None,
    output_dir: Path | None = None,
    test_window_dates: int = 20,
) -> dict[str, Any]:
    resolved_lanes = tuple(lane_specs or DEFAULT_LANES)
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    loaded_lanes = [_load_lane(spec) for spec in resolved_lanes]
    symbols = sorted({symbol for lane in loaded_lanes for symbol in lane["by_symbol"].keys()})
    symbol_reports: dict[str, Any] = {}
    for symbol in symbols:
        daily_rows = _build_daily_rows(loaded_lanes=loaded_lanes, symbol=symbol)
        symbol_reports[symbol] = {
            "lane_summaries": [
                dict(lane["by_symbol"][symbol]["summary"], lane_id=lane["lane_id"], source_variant=lane["source_variant"])
                for lane in loaded_lanes
                if symbol in lane["by_symbol"]
            ],
            "portfolio_summary": _portfolio_summary(daily_rows),
            "walkforward_summary": _walkforward_summary(daily_rows, test_window_dates=test_window_dates),
            "daily_rows": daily_rows,
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_forced_session_portfolio",
        "definition": {
            "description": "Combined portfolio analytics for selected forced-session GC/MGC lanes, aggregated by trade date.",
            "lane_specs": [
                {"lane_id": spec.lane_id, "source_json": str(spec.source_json), "source_variant": spec.source_variant}
                for spec in resolved_lanes
            ],
            "test_window_dates": test_window_dates,
        },
        "symbol_reports": symbol_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_forced_session_portfolio_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_forced_session_portfolio_research.md"
    gc_csv_path = resolved_output_dir / "gc_portfolio_daily.csv"
    mgc_csv_path = resolved_output_dir / "mgc_portfolio_daily.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    _write_daily_csv(gc_csv_path, symbol_reports.get("GC", {}).get("daily_rows", []))
    _write_daily_csv(mgc_csv_path, symbol_reports.get("MGC", {}).get("daily_rows", []))
    return {
        "mode": "gc_mgc_forced_session_portfolio_research",
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
            "gc_daily_csv": str(gc_csv_path),
            "mgc_daily_csv": str(mgc_csv_path),
        },
        "symbol_reports": {
            symbol: {
                "portfolio_summary": report["portfolio_summary"],
                "walkforward_summary": report["walkforward_summary"],
            }
            for symbol, report in symbol_reports.items()
        },
    }


def _parse_lane_specs(values: list[str]) -> tuple[PortfolioLaneSpec, ...]:
    lanes: list[PortfolioLaneSpec] = []
    for raw in values:
        lane_id, source_json, source_variant = raw.split("|", 2)
        lanes.append(
            PortfolioLaneSpec(
                lane_id=str(lane_id).strip(),
                source_json=Path(str(source_json).strip()).resolve(),
                source_variant=str(source_variant).strip(),
            )
        )
    return tuple(lanes)


def _load_lane(spec: PortfolioLaneSpec) -> dict[str, Any]:
    report = json.loads(spec.source_json.read_text(encoding="utf-8"))
    by_symbol: dict[str, Any] = {}
    for symbol, payload in report["symbol_reports"].items():
        variant_payload = payload["variants"][spec.source_variant]
        sessions = [
            {
                "trade_date": str(session["trade_date"]),
                "net_pnl_points": float(session["net_pnl_points"]),
                "entry_reason": session.get("entry_reason"),
                "variant_id": spec.source_variant,
            }
            for session in variant_payload["sessions"]
            if session.get("entered") and session.get("net_pnl_points") is not None
        ]
        by_symbol[symbol] = {
            "summary": dict(variant_payload["trade_summary"]),
            "sessions": sessions,
        }
    return {"lane_id": spec.lane_id, "source_variant": spec.source_variant, "by_symbol": by_symbol}


def _build_daily_rows(*, loaded_lanes: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = defaultdict(lambda: {"trade_date": "", "lane_pnls": {}, "lane_count": 0, "daily_net_pnl_points": 0.0})
    for lane in loaded_lanes:
        symbol_payload = lane["by_symbol"].get(symbol)
        if symbol_payload is None:
            continue
        for session in symbol_payload["sessions"]:
            row = by_date[session["trade_date"]]
            row["trade_date"] = session["trade_date"]
            row["lane_pnls"][lane["lane_id"]] = round(float(session["net_pnl_points"]), 4)
    ordered_dates = sorted(by_date)
    output: list[dict[str, Any]] = []
    cumulative = 0.0
    peak = 0.0
    for trade_date in ordered_dates:
        row = by_date[trade_date]
        daily_net = round(sum(float(value) for value in row["lane_pnls"].values()), 4)
        cumulative = round(cumulative + daily_net, 4)
        peak = max(peak, cumulative)
        drawdown = round(peak - cumulative, 4)
        output.append(
            {
                "trade_date": trade_date,
                "lane_count": len(row["lane_pnls"]),
                "daily_net_pnl_points": daily_net,
                "cumulative_net_pnl_points": cumulative,
                "drawdown_points": drawdown,
                "lane_pnls": dict(sorted(row["lane_pnls"].items())),
            }
        )
    return output


def _portfolio_summary(daily_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not daily_rows:
        return {
            "trade_date_count": 0,
            "average_daily_net_pnl_points": None,
            "median_daily_net_pnl_points": None,
            "net_profit_factor": None,
            "positive_day_rate": None,
            "total_net_pnl_points": None,
            "max_drawdown_points": None,
            "best_day_points": None,
            "worst_day_points": None,
        }
    values = [float(row["daily_net_pnl_points"]) for row in daily_rows]
    winners = [value for value in values if value > 0.0]
    losers = [value for value in values if value <= 0.0]
    max_drawdown = max(float(row["drawdown_points"]) for row in daily_rows)
    return {
        "trade_date_count": len(daily_rows),
        "average_daily_net_pnl_points": round(statistics.fmean(values), 4),
        "median_daily_net_pnl_points": round(statistics.median(values), 4),
        "net_profit_factor": round(sum(winners) / abs(sum(losers)), 4) if losers and abs(sum(losers)) > 0 else None,
        "positive_day_rate": round(len(winners) / len(values), 4),
        "total_net_pnl_points": round(sum(values), 4),
        "max_drawdown_points": round(max_drawdown, 4),
        "best_day_points": round(max(values), 4),
        "worst_day_points": round(min(values), 4),
    }


def _walkforward_summary(daily_rows: list[dict[str, Any]], *, test_window_dates: int) -> dict[str, Any]:
    dates = [str(row["trade_date"]) for row in daily_rows]
    folds: list[dict[str, Any]] = []
    for start in range(0, len(dates), test_window_dates):
        fold_rows = daily_rows[start : start + test_window_dates]
        if not fold_rows:
            continue
        summary = _portfolio_summary(fold_rows)
        folds.append(
            {
                "fold_index": len(folds) + 1,
                "test_start_date": fold_rows[0]["trade_date"],
                "test_end_date": fold_rows[-1]["trade_date"],
                "summary": summary,
            }
        )
    positive_folds = sum(1 for fold in folds if (fold["summary"]["average_daily_net_pnl_points"] or 0.0) > 0.0)
    return {
        "fold_count": len(folds),
        "positive_folds": positive_folds,
        "non_positive_folds": len(folds) - positive_folds,
        "best_fold": max(folds, key=lambda fold: float(fold["summary"]["average_daily_net_pnl_points"] or -9999.0)) if folds else None,
        "worst_fold": min(folds, key=lambda fold: float(fold["summary"]["average_daily_net_pnl_points"] or 9999.0)) if folds else None,
    }


def _write_daily_csv(path: Path, daily_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not daily_rows:
        path.write_text("", encoding="utf-8")
        return
    lane_ids = sorted({lane_id for row in daily_rows for lane_id in row["lane_pnls"].keys()})
    fieldnames = ["trade_date", "lane_count", "daily_net_pnl_points", "cumulative_net_pnl_points", "drawdown_points", *lane_ids]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in daily_rows:
            output = {key: row[key] for key in fieldnames if key in row}
            for lane_id in lane_ids:
                output[lane_id] = row["lane_pnls"].get(lane_id)
            writer.writerow(output)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC Forced Session Portfolio Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        "",
        "## Lanes",
        "",
    ]
    for lane in payload["definition"]["lane_specs"]:
        lines.append(f"- `{lane['lane_id']}` from `{lane['source_variant']}`")
    lines.extend(["", "## Symbol Portfolios", ""])
    for symbol, report in payload["symbol_reports"].items():
        summary = report["portfolio_summary"]
        walk = report["walkforward_summary"]
        lines.append(f"### {symbol}")
        lines.append("")
        lines.append(
            f"- Total / avg day / PF / max DD: `{summary['total_net_pnl_points']}` / `{summary['average_daily_net_pnl_points']}` / `{summary['net_profit_factor']}` / `{summary['max_drawdown_points']}`"
        )
        lines.append(
            f"- Positive days / best / worst: `{summary['positive_day_rate']}` / `{summary['best_day_points']}` / `{summary['worst_day_points']}`"
        )
        lines.append(
            f"- Walk-forward positive folds: `{walk['positive_folds']}` of `{walk['fold_count']}`"
        )
        lines.append("")
    return "\n".join(lines)
