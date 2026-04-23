"""Risk-shaping overlays for the GC/MGC forced-session portfolio."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_forced_session_portfolio_archive_v1"
    / "gc_mgc_forced_session_portfolio_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_forced_session_portfolio_shaping"
DEFAULT_TEST_WINDOW_DATES = 20
SESSION_EXECUTION_ORDER = (
    "ASIA_EARLY_SHORT",
    "LONDON_EARLY_LONG",
    "US_EARLY_SHORT",
    "US_MIDDAY_SHORT",
)


@dataclass(frozen=True)
class PortfolioShapeProfile:
    profile_id: str
    label: str
    enabled_lanes: tuple[str, ...] | None = None
    lane_weights: dict[str, float] = field(default_factory=dict)
    daily_realized_loss_cap_points: float | None = None
    conditional_weight_rules: tuple[dict[str, Any], ...] = ()


DEFAULT_PROFILES: tuple[PortfolioShapeProfile, ...] = (
    PortfolioShapeProfile(profile_id="baseline_full4", label="Baseline Full 4"),
    PortfolioShapeProfile(
        profile_id="conditional_sequence_guard",
        label="Conditional Sequence Guard",
        conditional_weight_rules=(
            {
                "rule_id": "asia_loss_halves_later",
                "trigger_lane_id": "ASIA_EARLY_SHORT",
                "threshold_points": -2.0,
                "target_lanes": ("LONDON_EARLY_LONG", "US_EARLY_SHORT", "US_MIDDAY_SHORT"),
                "target_weight": 0.5,
            },
            {
                "rule_id": "london_loss_halves_ny",
                "trigger_lane_id": "LONDON_EARLY_LONG",
                "threshold_points": -2.0,
                "target_lanes": ("US_EARLY_SHORT", "US_MIDDAY_SHORT"),
                "target_weight": 0.5,
            },
            {
                "rule_id": "ny_early_loss_skips_ny_late",
                "trigger_lane_id": "US_EARLY_SHORT",
                "threshold_points": -2.0,
                "target_lanes": ("US_MIDDAY_SHORT",),
                "target_weight": 0.0,
            },
        ),
    ),
    PortfolioShapeProfile(
        profile_id="conditional_late_guard",
        label="Conditional Late Guard",
        conditional_weight_rules=(
            {
                "rule_id": "asia_loss_halves_ny",
                "trigger_lane_id": "ASIA_EARLY_SHORT",
                "threshold_points": -1.5,
                "target_lanes": ("US_EARLY_SHORT", "US_MIDDAY_SHORT"),
                "target_weight": 0.5,
            },
            {
                "rule_id": "london_loss_skips_ny_late",
                "trigger_lane_id": "LONDON_EARLY_LONG",
                "threshold_points": -1.5,
                "target_lanes": ("US_MIDDAY_SHORT",),
                "target_weight": 0.0,
            },
            {
                "rule_id": "ny_early_loss_skips_ny_late",
                "trigger_lane_id": "US_EARLY_SHORT",
                "threshold_points": -1.5,
                "target_lanes": ("US_MIDDAY_SHORT",),
                "target_weight": 0.0,
            },
        ),
    ),
    PortfolioShapeProfile(
        profile_id="asia_half_full4",
        label="Asia Half Weight",
        lane_weights={"ASIA_EARLY_SHORT": 0.5},
    ),
    PortfolioShapeProfile(
        profile_id="core3_no_asia",
        label="Core 3 No Asia",
        enabled_lanes=("LONDON_EARLY_LONG", "US_EARLY_SHORT", "US_MIDDAY_SHORT"),
    ),
    PortfolioShapeProfile(
        profile_id="core3_no_asia_cap12",
        label="Core 3 No Asia + 12pt Day Cap",
        enabled_lanes=("LONDON_EARLY_LONG", "US_EARLY_SHORT", "US_MIDDAY_SHORT"),
        daily_realized_loss_cap_points=12.0,
    ),
    PortfolioShapeProfile(
        profile_id="full4_cap12",
        label="Full 4 + 12pt Day Cap",
        daily_realized_loss_cap_points=12.0,
    ),
    PortfolioShapeProfile(
        profile_id="full4_cap8",
        label="Full 4 + 8pt Day Cap",
        daily_realized_loss_cap_points=8.0,
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-forced-session-portfolio-shaping-research")
    parser.add_argument("--source-json", default=str(DEFAULT_SOURCE_JSON), help="Portfolio research JSON artifact path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    parser.add_argument("--test-window-dates", type=int, default=DEFAULT_TEST_WINDOW_DATES, help="Number of trade dates per walk-forward fold.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_forced_session_portfolio_shaping_research(
        source_json=Path(args.source_json),
        output_dir=Path(args.output_dir),
        test_window_dates=int(args.test_window_dates),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_forced_session_portfolio_shaping_research(
    *,
    source_json: Path | None = None,
    output_dir: Path | None = None,
    test_window_dates: int = DEFAULT_TEST_WINDOW_DATES,
    profiles: tuple[PortfolioShapeProfile, ...] | None = None,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_profiles = tuple(profiles or DEFAULT_PROFILES)
    source_payload = json.loads(resolved_source_json.read_text(encoding="utf-8"))

    symbol_reports: dict[str, Any] = {}
    for symbol, payload in (source_payload.get("symbol_reports") or {}).items():
        base_daily_rows = [dict(row) for row in list(payload.get("daily_rows") or [])]
        profile_reports = []
        for profile in resolved_profiles:
            shaped_rows = _apply_profile(base_daily_rows=base_daily_rows, profile=profile)
            profile_reports.append(
                {
                    "profile_id": profile.profile_id,
                    "label": profile.label,
                    "config": asdict(profile),
                    "portfolio_summary": _portfolio_summary(shaped_rows),
                    "walkforward_summary": _walkforward_summary(shaped_rows, test_window_dates=test_window_dates),
                    "daily_rows": shaped_rows,
                }
            )
        profile_reports.sort(
            key=lambda report: (
                float(report["portfolio_summary"]["total_net_pnl_points"] or -999999.0),
                float(report["portfolio_summary"]["net_profit_factor"] or 0.0),
            ),
            reverse=True,
        )
        symbol_reports[symbol] = {
            "profile_reports": profile_reports,
            "profile_ranking": [
                {
                    "profile_id": report["profile_id"],
                    "label": report["label"],
                    **report["portfolio_summary"],
                    "positive_folds": report["walkforward_summary"]["positive_folds"],
                    "fold_count": report["walkforward_summary"]["fold_count"],
                }
                for report in profile_reports
            ],
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_forced_session_portfolio_shaping",
        "source_json": str(resolved_source_json),
        "test_window_dates": test_window_dates,
        "profiles": [asdict(profile) for profile in resolved_profiles],
        "symbol_reports": symbol_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_forced_session_portfolio_shaping_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_forced_session_portfolio_shaping_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_forced_session_portfolio_shaping_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "symbol_reports": {
            symbol: {"profile_ranking": report["profile_ranking"]}
            for symbol, report in symbol_reports.items()
        },
    }


def _apply_profile(*, base_daily_rows: list[dict[str, Any]], profile: PortfolioShapeProfile) -> list[dict[str, Any]]:
    enabled_set = set(profile.enabled_lanes or [])
    output: list[dict[str, Any]] = []
    cumulative = 0.0
    peak = 0.0
    for row in base_daily_rows:
        realized = 0.0
        halted = False
        effective_weights = {str(key): float(value) for key, value in profile.lane_weights.items()}
        weight_rule_reasons: dict[str, list[str]] = defaultdict(list)
        shaped_lane_pnls: dict[str, float] = {}
        skipped_lanes: list[str] = []
        for lane_id in SESSION_EXECUTION_ORDER:
            if lane_id not in row.get("lane_pnls", {}):
                continue
            if enabled_set and lane_id not in enabled_set:
                skipped_lanes.append(f"{lane_id}:disabled")
                continue
            if halted:
                skipped_lanes.append(f"{lane_id}:day_cap_halt")
                continue
            raw_pnl = float(row["lane_pnls"][lane_id])
            weight = float(effective_weights.get(lane_id, 1.0))
            if weight <= 0.0:
                reasons = ",".join(weight_rule_reasons.get(lane_id, [])) or "conditional_weight_zero"
                skipped_lanes.append(f"{lane_id}:{reasons}")
                continue
            shaped_pnl = round(raw_pnl * weight, 4)
            shaped_lane_pnls[lane_id] = shaped_pnl
            realized = round(realized + shaped_pnl, 4)
            for rule in profile.conditional_weight_rules:
                if str(rule.get("trigger_lane_id")) != lane_id:
                    continue
                threshold = float(rule.get("threshold_points", 0.0))
                if shaped_pnl > threshold:
                    continue
                target_weight = float(rule.get("target_weight", 1.0))
                rule_id = str(rule.get("rule_id") or "conditional_rule")
                for target_lane in tuple(rule.get("target_lanes") or ()):
                    current_weight = float(effective_weights.get(str(target_lane), 1.0))
                    effective_weights[str(target_lane)] = min(current_weight, target_weight)
                    weight_rule_reasons[str(target_lane)].append(rule_id)
            if (
                profile.daily_realized_loss_cap_points is not None
                and realized <= -abs(float(profile.daily_realized_loss_cap_points))
            ):
                halted = True
        cumulative = round(cumulative + realized, 4)
        peak = max(peak, cumulative)
        drawdown = round(peak - cumulative, 4)
        output.append(
            {
                "trade_date": row["trade_date"],
                "lane_count": len(shaped_lane_pnls),
                "daily_net_pnl_points": realized,
                "cumulative_net_pnl_points": cumulative,
                "drawdown_points": drawdown,
                "lane_pnls": shaped_lane_pnls,
                "skipped_lanes": skipped_lanes,
                "effective_weights": dict(sorted((key, round(value, 4)) for key, value in effective_weights.items())),
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
            "average_lane_count": None,
            "halted_day_count": None,
        }
    values = [float(row["daily_net_pnl_points"]) for row in daily_rows]
    winners = [value for value in values if value > 0.0]
    losers = [value for value in values if value <= 0.0]
    max_drawdown = max(float(row["drawdown_points"]) for row in daily_rows)
    halted_day_count = sum(1 for row in daily_rows if any("day_cap_halt" in item for item in row.get("skipped_lanes", [])))
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
        "average_lane_count": round(statistics.fmean(float(row["lane_count"]) for row in daily_rows), 4),
        "halted_day_count": halted_day_count,
    }


def _walkforward_summary(daily_rows: list[dict[str, Any]], *, test_window_dates: int) -> dict[str, Any]:
    folds: list[dict[str, Any]] = []
    for start in range(0, len(daily_rows), test_window_dates):
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


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC Forced Session Portfolio Shaping Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Source JSON: `{payload['source_json']}`",
        "",
    ]
    for symbol, report in payload["symbol_reports"].items():
        lines.append(f"## {symbol}")
        lines.append("")
        for row in report["profile_ranking"]:
            lines.append(
                f"- `{row['profile_id']}`: total `{row['total_net_pnl_points']}`, avg day `{row['average_daily_net_pnl_points']}`, PF `{row['net_profit_factor']}`, max DD `{row['max_drawdown_points']}`, positive folds `{row['positive_folds']}/{row['fold_count']}`"
            )
        lines.append("")
    return "\n".join(lines)
