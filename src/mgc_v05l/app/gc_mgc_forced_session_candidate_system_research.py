"""Candidate-system formalization for the promoted GC/MGC forced-session portfolio."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_forced_session_portfolio_archive_v2_core5"
    / "gc_mgc_forced_session_portfolio_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_forced_session_candidate_system_archive_v2"
POINT_VALUE_BY_SYMBOL = {"GC": 100.0, "MGC": 10.0}


@dataclass(frozen=True)
class CandidateLaneDefinition:
    lane_id: str
    segment_id: str
    side: str
    session_start_et: str
    session_end_et: str
    source_json: Path
    source_variant: str
    entry_family: str
    execution_note: str


@dataclass(frozen=True)
class ScenarioLaneAllocation:
    lane_id: str
    symbol: str
    contracts: int


@dataclass(frozen=True)
class CandidateDeploymentScenario:
    scenario_id: str
    label: str
    description: str
    allocations: tuple[ScenarioLaneAllocation, ...]


DEFAULT_LANE_DEFINITIONS: tuple[CandidateLaneDefinition, ...] = (
    CandidateLaneDefinition(
        lane_id="ASIA_EARLY_LONG",
        segment_id="ASIA_EARLY",
        side="LONG",
        session_start_et="19:00",
        session_end_et="20:30",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_asia_early_long_forced_session_archive_v2"
        / "gc_mgc_segment_forced_session_long_research.json",
        source_variant="segment_forced_long_v5_dip_reclaim_or_bar8",
        entry_family="dip reclaim or timed bar 8 long",
        execution_note="Use the upgraded Asia-early long recovery lane with the delayed bar 8 fallback.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_EARLY_SHORT",
        segment_id="ASIA_EARLY",
        side="SHORT",
        session_start_et="19:00",
        session_end_et="20:30",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_asia_early_short_forced_session_archive_v1"
        / "gc_mgc_segment_forced_session_short_research.json",
        source_variant="segment_forced_short_v2_reclaim_fail_or_bar7",
        entry_family="failed reclaim or timed bar 7 short",
        execution_note="Use the promoted forced-session short baseline with one trade attempt inside Asia early.",
    ),
    CandidateLaneDefinition(
        lane_id="LONDON_EARLY_LONG",
        segment_id="LONDON_EARLY",
        side="LONG",
        session_start_et="03:00",
        session_end_et="05:30",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_london_early_long_forced_session_archive_v2"
        / "gc_mgc_segment_forced_session_long_research.json",
        source_variant="segment_forced_long_v5_dip_reclaim_or_bar8",
        entry_family="dip reclaim or timed bar 8 long",
        execution_note="Use the upgraded London-early long lane with the stronger delayed fallback profile.",
    ),
    CandidateLaneDefinition(
        lane_id="US_EARLY_SHORT",
        segment_id="US_EARLY",
        side="SHORT",
        session_start_et="08:20",
        session_end_et="11:00",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_ny_early_short_forced_session_archive_v1"
        / "gc_mgc_ny_early_short_forced_session_research.json",
        source_variant="ny_forced_short_v2_reclaim_fail_or_bar7",
        entry_family="reclaim fail or timed bar 7 short",
        execution_note="Use the stronger NY-early failed-pop / reclaim-fail short baseline.",
    ),
    CandidateLaneDefinition(
        lane_id="US_MIDDAY_SHORT",
        segment_id="US_MIDDAY",
        side="SHORT",
        session_start_et="11:00",
        session_end_et="13:30",
        source_json=REPO_ROOT
        / "outputs"
        / "reports"
        / "gc_mgc_ny_late_short_forced_session_archive_v1"
        / "gc_mgc_segment_forced_session_short_research.json",
        source_variant="segment_forced_short_v2_reclaim_fail_or_bar7",
        entry_family="failed reclaim or timed bar 7 short",
        execution_note="Use the promoted U.S.-midday short baseline with one trade attempt inside the midday window.",
    ),
)


DEFAULT_SCENARIOS: tuple[CandidateDeploymentScenario, ...] = (
    CandidateDeploymentScenario(
        scenario_id="gc_1x_all_lanes",
        label="GC 1x All Lanes",
        description="One full-size GC contract in each promoted lane.",
        allocations=tuple(
            ScenarioLaneAllocation(lane_id=lane.lane_id, symbol="GC", contracts=1) for lane in DEFAULT_LANE_DEFINITIONS
        ),
    ),
    CandidateDeploymentScenario(
        scenario_id="mgc_1x_all_lanes",
        label="MGC 1x All Lanes",
        description="One micro-gold contract in each promoted lane for the lowest operational footprint.",
        allocations=tuple(
            ScenarioLaneAllocation(lane_id=lane.lane_id, symbol="MGC", contracts=1) for lane in DEFAULT_LANE_DEFINITIONS
        ),
    ),
    CandidateDeploymentScenario(
        scenario_id="mgc_10x_all_lanes_gc_equivalent",
        label="MGC 10x All Lanes",
        description="Ten MGC contracts per lane to approximate GC notional with micro execution flexibility.",
        allocations=tuple(
            ScenarioLaneAllocation(lane_id=lane.lane_id, symbol="MGC", contracts=10) for lane in DEFAULT_LANE_DEFINITIONS
        ),
    ),
    CandidateDeploymentScenario(
        scenario_id="hybrid_core_gc_asia_mgc",
        label="Hybrid Core GC + Asia MGC",
        description="Use GC on the London/NY core and MGC on both Asia-early lanes.",
        allocations=(
            ScenarioLaneAllocation(lane_id="ASIA_EARLY_LONG", symbol="MGC", contracts=1),
            ScenarioLaneAllocation(lane_id="ASIA_EARLY_SHORT", symbol="MGC", contracts=1),
            ScenarioLaneAllocation(lane_id="LONDON_EARLY_LONG", symbol="GC", contracts=1),
            ScenarioLaneAllocation(lane_id="US_EARLY_SHORT", symbol="GC", contracts=1),
            ScenarioLaneAllocation(lane_id="US_MIDDAY_SHORT", symbol="GC", contracts=1),
        ),
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-forced-session-candidate-system")
    parser.add_argument(
        "--source-json",
        default=str(DEFAULT_SOURCE_JSON),
        help="Baseline portfolio JSON artifact. Defaults to the promoted archive-backed baseline portfolio.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_forced_session_candidate_system_research(
        source_json=Path(args.source_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_forced_session_candidate_system_research(
    *,
    source_json: Path | None = None,
    output_dir: Path | None = None,
    lane_definitions: tuple[CandidateLaneDefinition, ...] = DEFAULT_LANE_DEFINITIONS,
    scenarios: tuple[CandidateDeploymentScenario, ...] = DEFAULT_SCENARIOS,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    portfolio = json.loads(resolved_source_json.read_text(encoding="utf-8"))
    symbol_daily_map = _build_symbol_daily_map(portfolio)

    scenario_reports: list[dict[str, Any]] = []
    combined_daily_rows: list[dict[str, Any]] = []
    for scenario in scenarios:
        daily_rows = _build_scenario_daily_rows(
            scenario=scenario,
            symbol_daily_map=symbol_daily_map,
        )
        scenario_summary = _scenario_summary(daily_rows=daily_rows, scenario=scenario)
        scenario_reports.append(
            {
                "scenario_id": scenario.scenario_id,
                "label": scenario.label,
                "description": scenario.description,
                "allocations": [asdict(item) for item in scenario.allocations],
                "summary": scenario_summary,
            }
        )
        combined_daily_rows.extend(
            {
                "scenario_id": scenario.scenario_id,
                "trade_date": row["trade_date"],
                "daily_net_pnl_dollars": row["daily_net_pnl_dollars"],
                "cumulative_net_pnl_dollars": row["cumulative_net_pnl_dollars"],
                "drawdown_dollars": row["drawdown_dollars"],
                "contract_turns": row["contract_turns"],
                "lane_count": row["lane_count"],
            }
            for row in daily_rows
        )

    scenario_reports.sort(
        key=lambda row: (
            float(row["summary"]["total_net_pnl_dollars"] or 0.0),
            float(row["summary"]["net_profit_factor"] or 0.0),
        ),
        reverse=True,
    )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_forced_session_candidate_system",
        "candidate_system": {
            "candidate_id": "gc_mgc_forced_session_baseline_v2",
            "description": (
                "Promoted five-lane gold session portfolio using the upgraded Asia-early and London-early long baselines "
                "alongside the retained short core after walk-forward and portfolio research."
            ),
            "source_portfolio_json": str(resolved_source_json),
            "lane_sequence": [_json_ready(asdict(lane)) for lane in lane_definitions],
            "operating_assumptions": {
                "entries_per_lane_per_trade_date": 1,
                "positions_flatten_within_source_segment": True,
                "max_modeled_lane_overlap": 2,
                "daily_caps_or_conditional_suppression": "none",
                "session_order_et": [f"{lane.lane_id}@{lane.session_start_et}-{lane.session_end_et}" for lane in lane_definitions],
                "notes": [
                    "The promoted v2 candidate keeps the unshaped portfolio because shaping overlays reduced return too much.",
                    "Asia early now contains both long and short forced-session lanes, so same-segment overlap is possible in the model.",
                    "Each promoted lane is modeled as one forced-session trade attempt and exits within its own source segment.",
                    "Contract sizing is evaluated separately from lane discovery so the trade logic stays unchanged across deployment scenarios.",
                ],
            },
        },
        "scenario_reports": scenario_reports,
    }

    json_path = resolved_output_dir / "gc_mgc_forced_session_candidate_system_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_forced_session_candidate_system_research.md"
    csv_path = resolved_output_dir / "gc_mgc_forced_session_candidate_system_daily.csv"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    _write_daily_csv(csv_path, combined_daily_rows)
    return {
        "mode": "gc_mgc_forced_session_candidate_system_research",
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
            "daily_csv": str(csv_path),
        },
        "candidate_id": payload["candidate_system"]["candidate_id"],
        "scenario_reports": [
            {
                "scenario_id": row["scenario_id"],
                "summary": row["summary"],
            }
            for row in scenario_reports
        ],
    }


def _build_symbol_daily_map(portfolio: dict[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for symbol, payload in (portfolio.get("symbol_reports") or {}).items():
        rows: dict[str, dict[str, Any]] = {}
        for row in payload.get("daily_rows") or []:
            lane_pnls = dict(row.get("lane_pnls") or {})
            if "NY_EARLY_SHORT" in lane_pnls and "US_EARLY_SHORT" not in lane_pnls:
                lane_pnls["US_EARLY_SHORT"] = lane_pnls["NY_EARLY_SHORT"]
            if "NY_LATE_SHORT" in lane_pnls and "US_MIDDAY_SHORT" not in lane_pnls:
                lane_pnls["US_MIDDAY_SHORT"] = lane_pnls["NY_LATE_SHORT"]
            rows[str(row["trade_date"])] = {
                "lane_pnls": lane_pnls,
                "daily_net_pnl_points": float(row.get("daily_net_pnl_points") or 0.0),
            }
        output[str(symbol).upper()] = rows
    return output


def _build_scenario_daily_rows(
    *,
    scenario: CandidateDeploymentScenario,
    symbol_daily_map: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    all_dates = sorted(
        {
            trade_date
            for symbol_rows in symbol_daily_map.values()
            for trade_date in symbol_rows.keys()
        }
    )
    cumulative = 0.0
    peak = 0.0
    rows: list[dict[str, Any]] = []
    for trade_date in all_dates:
        lane_dollars: dict[str, float] = {}
        lane_contracts: dict[str, int] = {}
        for allocation in scenario.allocations:
            symbol = allocation.symbol.upper()
            symbol_rows = symbol_daily_map.get(symbol, {})
            date_payload = symbol_rows.get(trade_date, {})
            lane_points = float((date_payload.get("lane_pnls") or {}).get(allocation.lane_id, 0.0))
            lane_dollars[allocation.lane_id] = round(
                lane_points * POINT_VALUE_BY_SYMBOL[symbol] * allocation.contracts,
                2,
            )
            lane_contracts[allocation.lane_id] = allocation.contracts
        daily_net = round(sum(lane_dollars.values()), 2)
        cumulative = round(cumulative + daily_net, 2)
        peak = max(peak, cumulative)
        drawdown = round(peak - cumulative, 2)
        rows.append(
            {
                "trade_date": trade_date,
                "lane_count": len(scenario.allocations),
                "contract_turns": sum(lane_contracts.values()),
                "daily_net_pnl_dollars": daily_net,
                "cumulative_net_pnl_dollars": cumulative,
                "drawdown_dollars": drawdown,
                "lane_dollars": dict(sorted(lane_dollars.items())),
            }
        )
    return rows


def _scenario_summary(
    *,
    daily_rows: list[dict[str, Any]],
    scenario: CandidateDeploymentScenario,
) -> dict[str, Any]:
    if not daily_rows:
        return {
            "trade_date_count": 0,
            "average_daily_net_pnl_dollars": None,
            "median_daily_net_pnl_dollars": None,
            "positive_day_rate": None,
            "net_profit_factor": None,
            "total_net_pnl_dollars": None,
            "max_drawdown_dollars": None,
            "best_day_dollars": None,
            "worst_day_dollars": None,
            "average_daily_contract_turns": None,
            "max_simultaneous_contracts": None,
        }
    values = [float(row["daily_net_pnl_dollars"]) for row in daily_rows]
    winners = [value for value in values if value > 0.0]
    losers = [value for value in values if value <= 0.0]
    max_drawdown = max(float(row["drawdown_dollars"]) for row in daily_rows)
    return {
        "trade_date_count": len(daily_rows),
        "average_daily_net_pnl_dollars": round(statistics.fmean(values), 2),
        "median_daily_net_pnl_dollars": round(statistics.median(values), 2),
        "positive_day_rate": round(len(winners) / len(values), 4),
        "net_profit_factor": round(sum(winners) / abs(sum(losers)), 4) if losers and abs(sum(losers)) > 0 else None,
        "total_net_pnl_dollars": round(sum(values), 2),
        "max_drawdown_dollars": round(max_drawdown, 2),
        "best_day_dollars": round(max(values), 2),
        "worst_day_dollars": round(min(values), 2),
        "average_daily_contract_turns": round(statistics.fmean(float(row["contract_turns"]) for row in daily_rows), 2),
        "max_simultaneous_contracts": max(allocation.contracts for allocation in scenario.allocations),
    }


def _write_daily_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "scenario_id",
                "trade_date",
                "daily_net_pnl_dollars",
                "cumulative_net_pnl_dollars",
                "drawdown_dollars",
                "contract_turns",
                "lane_count",
            ),
        )
        writer.writeheader()
        writer.writerows(rows)


def _render_markdown(payload: dict[str, Any]) -> str:
    candidate = payload["candidate_system"]
    lines = [
        "# GC/MGC Forced Session Candidate System",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Candidate id: `{candidate['candidate_id']}`",
        f"- Source portfolio: `{candidate['source_portfolio_json']}`",
        "",
        "## Lane Sequence",
        "",
    ]
    for lane in candidate["lane_sequence"]:
        lines.append(
            f"- `{lane['lane_id']}` `{lane['session_start_et']}-{lane['session_end_et']} ET`: {lane['side']} via `{lane['source_variant']}`"
        )
    lines.extend(["", "## Scenario Ranking", ""])
    for scenario in payload["scenario_reports"]:
        summary = scenario["summary"]
        lines.append(
            f"- `{scenario['scenario_id']}`: total `${summary['total_net_pnl_dollars']}`, avg day `${summary['average_daily_net_pnl_dollars']}`, "
            f"PF `{summary['net_profit_factor']}`, max DD `${summary['max_drawdown_dollars']}`, avg contract turns `{summary['average_daily_contract_turns']}`"
        )
    return "\n".join(lines)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
