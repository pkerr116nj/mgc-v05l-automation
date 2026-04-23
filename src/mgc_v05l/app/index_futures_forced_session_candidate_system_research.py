"""Candidate-system formalization for promoted stock-index forced-session edges."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "index_futures_forced_session_research_aligned_20240101_20260420"
    / "index_futures_forced_session_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "index_futures_forced_session_candidate_system_archive_v1"
POINT_VALUE_BY_SYMBOL = {
    "ES": 50.0,
    "MES": 5.0,
    "NQ": 20.0,
    "MNQ": 2.0,
}


@dataclass(frozen=True)
class CandidateLaneDefinition:
    lane_id: str
    segment_id: str
    side: str
    session_start_et: str
    session_end_et: str
    source_variant: str
    entry_family: str
    execution_note: str


@dataclass(frozen=True)
class CandidateDeploymentScenario:
    scenario_id: str
    label: str
    symbol: str
    contracts: int
    description: str


DEFAULT_LANE_DEFINITIONS: tuple[CandidateLaneDefinition, ...] = (
    CandidateLaneDefinition(
        lane_id="US_EARLY_LONG",
        segment_id="US_EARLY",
        side="LONG",
        session_start_et="08:20",
        session_end_et="11:00",
        source_variant="US_EARLY__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
        entry_family="dip reclaim or timed bar 8 long",
        execution_note="Highest-confidence stock-index forced-session long across ES/MES and NQ/MNQ.",
    ),
    CandidateLaneDefinition(
        lane_id="US_EARLY_SHORT_BREAKDOWN",
        segment_id="US_EARLY",
        side="SHORT",
        session_start_et="08:20",
        session_end_et="11:00",
        source_variant="US_EARLY__SHORT__segment_forced_short_v4_breakdown_or_bar7",
        entry_family="breakdown or timed bar 7 short",
        execution_note="Best performing NY-early breakdown short family across the full four-symbol stock-index sample.",
    ),
    CandidateLaneDefinition(
        lane_id="US_EARLY_SHORT_RECLAIM_FAIL",
        segment_id="US_EARLY",
        side="SHORT",
        session_start_et="08:20",
        session_end_et="11:00",
        source_variant="US_EARLY__SHORT__segment_forced_short_v2_reclaim_fail_or_bar7",
        entry_family="reclaim fail or timed bar 7 short",
        execution_note="Secondary NY-early short family with strong expectancy and a different failure shape than the breakdown lane.",
    ),
    CandidateLaneDefinition(
        lane_id="US_MIDDAY_LONG",
        segment_id="US_MIDDAY",
        side="LONG",
        session_start_et="11:00",
        session_end_et="13:30",
        source_variant="US_MIDDAY__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
        entry_family="dip reclaim or timed bar 8 long",
        execution_note="Best-promoted U.S.-midday stock-index long family, added to extend participation through the midday window.",
    ),
    CandidateLaneDefinition(
        lane_id="US_MIDDAY_SHORT_BREAKDOWN",
        segment_id="US_MIDDAY",
        side="SHORT",
        session_start_et="11:00",
        session_end_et="13:30",
        source_variant="US_MIDDAY__SHORT__segment_forced_short_v4_breakdown_or_bar7",
        entry_family="breakdown or timed bar 7 short",
        execution_note="Best-promoted U.S.-midday stock-index short family, retained for midday reversal participation.",
    ),
    CandidateLaneDefinition(
        lane_id="US_LATE_LONG",
        segment_id="US_LATE",
        side="LONG",
        session_start_et="13:30",
        session_end_et="16:00",
        source_variant="US_LATE__LONG__segment_forced_long_v5_dip_reclaim_or_bar8",
        entry_family="dip reclaim or timed bar 8 long",
        execution_note="Best-promoted true late-U.S. stock-index long family for afternoon participation.",
    ),
    CandidateLaneDefinition(
        lane_id="US_LATE_SHORT_RECLAIM_FAIL",
        segment_id="US_LATE",
        side="SHORT",
        session_start_et="13:30",
        session_end_et="16:00",
        source_variant="US_LATE__SHORT__segment_forced_short_v2_reclaim_fail_or_bar7",
        entry_family="reclaim fail or timed bar 7 short",
        execution_note="Best-promoted true late-U.S. stock-index short family for afternoon reversal participation.",
    ),
)


DEFAULT_SCENARIOS: tuple[CandidateDeploymentScenario, ...] = (
    CandidateDeploymentScenario(
        scenario_id="es_1x_ny_early_core",
        label="ES 1x US Intraday Core",
        symbol="ES",
        contracts=1,
        description="Run the promoted seven-lane stock package across U.S. early, U.S. midday, and true U.S. late windows on one ES contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="mes_1x_ny_early_core",
        label="MES 1x US Intraday Core",
        symbol="MES",
        contracts=1,
        description="Run the promoted seven-lane stock package across U.S. early, U.S. midday, and true U.S. late windows on one MES contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="nq_1x_ny_early_core",
        label="NQ 1x US Intraday Core",
        symbol="NQ",
        contracts=1,
        description="Run the promoted seven-lane stock package across U.S. early, U.S. midday, and true U.S. late windows on one NQ contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="mnq_1x_ny_early_core",
        label="MNQ 1x US Intraday Core",
        symbol="MNQ",
        contracts=1,
        description="Run the promoted seven-lane stock package across U.S. early, U.S. midday, and true U.S. late windows on one MNQ contract.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="index-futures-forced-session-candidate-system")
    parser.add_argument("--source-json", default=str(DEFAULT_SOURCE_JSON), help="Forced-session stock-index research JSON.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_index_futures_forced_session_candidate_system_research(
        source_json=Path(args.source_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_index_futures_forced_session_candidate_system_research(
    *,
    source_json: Path | None = None,
    output_dir: Path | None = None,
    lane_definitions: tuple[CandidateLaneDefinition, ...] = DEFAULT_LANE_DEFINITIONS,
    scenarios: tuple[CandidateDeploymentScenario, ...] = DEFAULT_SCENARIOS,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    research = json.loads(resolved_source_json.read_text(encoding="utf-8"))
    all_symbol_rows = {row["variant_id"]: row for row in research["all_symbol_ranking"]}
    lane_sequence = []
    for lane in lane_definitions:
        ranking_row = _resolve_ranking_row(all_symbol_rows=all_symbol_rows, source_variant=lane.source_variant)
        lane_sequence.append(
            {
                **asdict(lane),
                "ranking_summary": {
                    "min_average_net_pnl_points": ranking_row["min_average_net_pnl_points"],
                    "min_net_profit_factor": ranking_row["min_net_profit_factor"],
                    "min_entered_trade_count": ranking_row["min_entered_trade_count"],
                    "symbols": ranking_row["symbols"],
                },
            }
        )

    scenario_reports = []
    for scenario in scenarios:
        lane_reports = []
        total_avg_points = 0.0
        total_trades = None
        min_pf = None
        for lane in lane_definitions:
            row = _resolve_ranking_row(all_symbol_rows=all_symbol_rows, source_variant=lane.source_variant)
            symbol_metrics = row["symbol_metrics"][scenario.symbol]
            total_avg_points += float(symbol_metrics["average_net_pnl_points"])
            total_trades = symbol_metrics["entered_trade_count"] if total_trades is None else min(total_trades, symbol_metrics["entered_trade_count"])
            metric_pf = float(symbol_metrics["net_profit_factor"])
            min_pf = metric_pf if min_pf is None else min(min_pf, metric_pf)
            lane_reports.append(
                {
                    "lane_id": lane.lane_id,
                    "average_net_pnl_points": symbol_metrics["average_net_pnl_points"],
                    "net_profit_factor": symbol_metrics["net_profit_factor"],
                    "entered_trade_count": symbol_metrics["entered_trade_count"],
                }
            )
        scenario_reports.append(
            {
                "scenario_id": scenario.scenario_id,
                "label": scenario.label,
                "symbol": scenario.symbol,
                "contracts": scenario.contracts,
                "description": scenario.description,
                "summary": {
                    "lane_count": len(lane_definitions),
                    "minimum_trade_count": int(total_trades or 0),
                    "minimum_lane_profit_factor": round(float(min_pf or 0.0), 4),
                    "aggregate_average_net_pnl_points": round(total_avg_points, 4),
                    "aggregate_average_net_pnl_dollars": round(
                        total_avg_points * POINT_VALUE_BY_SYMBOL[scenario.symbol] * scenario.contracts,
                        2,
                    ),
                },
                "lane_reports": lane_reports,
            }
        )

    scenario_reports.sort(
        key=lambda row: (
            float(row["summary"]["aggregate_average_net_pnl_dollars"]),
            float(row["summary"]["minimum_lane_profit_factor"]),
        ),
        reverse=True,
    )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "index_futures_forced_session_candidate_system",
        "candidate_system": {
            "candidate_id": "index_futures_ny_intraday_forced_core_v2",
            "description": (
                "Promoted stock-index forced-session core built from the strongest US_EARLY edges, the best U.S.-midday pair, "
                "and the first true U.S.-late pair across ES/MES and NQ/MNQ."
            ),
            "source_research_json": str(resolved_source_json),
            "lane_sequence": lane_sequence,
            "operating_assumptions": {
                "session_open_et": "18:00-19:00",
                "promoted_windows_et": ["08:20-11:00", "11:00-13:30"],
                "entries_per_lane_per_trade_date": 1,
                "positions_flatten_within_source_segment": True,
                "max_modeled_lane_overlap": 5,
                "notes": [
                    "This stock-index package promotes the strongest US_EARLY trio plus the clearest US_MIDDAY and US_LATE families.",
                    "Both US_EARLY short families are retained because they represent different failure shapes with strong broad-sample expectancy.",
                    "US_MIDDAY remains a useful extension bucket, but the first-pass true US_LATE research was also strong enough to justify afternoon coverage.",
                ],
            },
        },
        "scenario_reports": scenario_reports,
    }

    json_path = resolved_output_dir / "index_futures_forced_session_candidate_system_research.json"
    markdown_path = resolved_output_dir / "index_futures_forced_session_candidate_system_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "index_futures_forced_session_candidate_system_research",
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
        },
        "candidate_id": payload["candidate_system"]["candidate_id"],
        "scenario_reports": [
            {"scenario_id": row["scenario_id"], "summary": row["summary"]}
            for row in scenario_reports
        ],
    }


def _resolve_ranking_row(*, all_symbol_rows: dict[str, Any], source_variant: str) -> dict[str, Any]:
    direct = all_symbol_rows.get(source_variant)
    if direct is not None:
        return direct
    alias = source_variant.replace("US_MIDDAY__", "NY_LATE__", 1)
    aliased = all_symbol_rows.get(alias)
    if aliased is not None:
        return aliased
    alias = source_variant.replace("US_EARLY__", "NY_EARLY__", 1)
    aliased = all_symbol_rows.get(alias)
    if aliased is not None:
        return aliased
    raise KeyError(f"Unable to resolve forced-session stock variant: {source_variant}")


def _render_markdown(payload: dict[str, Any]) -> str:
    candidate = payload["candidate_system"]
    lines = [
        "# Index Futures Forced Session Candidate System",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Candidate id: `{candidate['candidate_id']}`",
        f"- Source research: `{candidate['source_research_json']}`",
        "",
        "## Promoted Lanes",
        "",
    ]
    for lane in candidate["lane_sequence"]:
        summary = lane["ranking_summary"]
        lines.append(
            f"- `{lane['lane_id']}` `{lane['session_start_et']}-{lane['session_end_et']} ET`: `{lane['source_variant']}` "
            f"(min avg `{summary['min_average_net_pnl_points']}`, min PF `{summary['min_net_profit_factor']}`, min trades `{summary['min_entered_trade_count']}`)"
        )
    lines.extend(["", "## Scenario Ranking", ""])
    for scenario in payload["scenario_reports"]:
        summary = scenario["summary"]
        lines.append(
            f"- `{scenario['scenario_id']}`: avg `${summary['aggregate_average_net_pnl_dollars']}` per trade date "
            f"from `{summary['aggregate_average_net_pnl_points']}` points, min lane PF `{summary['minimum_lane_profit_factor']}`, "
            f"min trade count `{summary['minimum_trade_count']}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
