"""Candidate-system formalization for promoted Asia-to-London participation lanes."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GC_SOURCE_JSON = Path("/private/tmp/asia_london_opt_gc_mgc/asia_london_participation_optimization.json")
DEFAULT_NQ_SOURCE_JSON = Path("/private/tmp/asia_london_opt_nq_mnq/asia_london_participation_optimization.json")
DEFAULT_ES_SOURCE_JSON = REPO_ROOT / "outputs" / "reports" / "es_asia_london_candidate_validation_v1" / "es_asia_london_candidate_validation.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "asia_london_participation_candidate_system_archive_v2"
POINT_VALUE_BY_SYMBOL = {"GC": 100.0, "MGC": 10.0, "NQ": 20.0, "MNQ": 2.0, "ES": 50.0}


@dataclass(frozen=True)
class CandidateLaneDefinition:
    lane_id: str
    segment_id: str
    side: str
    symbol_group: str
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
        lane_id="ASIA_LONDON_LONG_V5",
        segment_id="ASIA_EARLY",
        side="LONG",
        symbol_group="GC_MGC",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base",
        entry_family="dip reclaim / bar 8 hold-through long",
        execution_note="Best overnight long across GC/MGC; enter in Asia early and hold through Europe with LONDON_LATE flat rule.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_LONDON_SHORT_V2",
        segment_id="ASIA_EARLY",
        side="SHORT",
        symbol_group="GC_MGC",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base",
        entry_family="reclaim-fail / bar 7 hold-through short",
        execution_note="Best overnight short across GC/MGC under apples-to-apples optimization review.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_LONDON_LONG_V6",
        segment_id="ASIA_EARLY",
        side="LONG",
        symbol_group="NQ_MNQ",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="LONG__segment_forced_long_v6_contextual_fallback__base",
        entry_family="contextual fallback long hold-through",
        execution_note="Best overnight long across NQ/MNQ; strongest expectancy and PF in the final gating pass.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_LONDON_LONG_V5",
        segment_id="ASIA_EARLY",
        side="LONG",
        symbol_group="NQ_MNQ",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base",
        entry_family="dip reclaim / bar 8 hold-through long",
        execution_note="Close second-best overnight NQ/MNQ long with similar EV but a different fallback shape than v6.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_LONDON_SHORT_V2",
        segment_id="ASIA_EARLY",
        side="SHORT",
        symbol_group="NQ_MNQ",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base",
        entry_family="reclaim-fail / bar 7 hold-through short",
        execution_note="Positive overnight NQ/MNQ short lane retained as the only acceptable short-side participation family in the final gate.",
    ),
    CandidateLaneDefinition(
        lane_id="ASIA_LONDON_LONG_V6_VOL_FLOOR_125",
        segment_id="ASIA_EARLY",
        side="LONG",
        symbol_group="ES_ONLY",
        session_start_et="19:00",
        session_end_et="08:20",
        source_variant="LONG__segment_forced_long_v6_contextual_fallback__vol_floor_1p25",
        entry_family="contextual fallback long hold-through + 1.25 setup-range/ATR floor",
        execution_note="Best standalone ES overnight candidate; positive expectancy and PF after filtering out quieter Asia setups.",
    ),
)

DEFAULT_SCENARIOS: tuple[CandidateDeploymentScenario, ...] = (
    CandidateDeploymentScenario(
        scenario_id="gc_1x_asia_london_participation",
        label="GC 1x Asia-London Participation",
        symbol="GC",
        contracts=1,
        description="Run the promoted overnight participation pair on one GC contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="mgc_1x_asia_london_participation",
        label="MGC 1x Asia-London Participation",
        symbol="MGC",
        contracts=1,
        description="Run the promoted overnight participation pair on one MGC contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="nq_1x_asia_london_participation",
        label="NQ 1x Asia-London Participation",
        symbol="NQ",
        contracts=1,
        description="Run the promoted overnight participation set on one NQ contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="mnq_1x_asia_london_participation",
        label="MNQ 1x Asia-London Participation",
        symbol="MNQ",
        contracts=1,
        description="Run the promoted overnight participation set on one MNQ contract.",
    ),
    CandidateDeploymentScenario(
        scenario_id="es_1x_asia_london_participation",
        label="ES 1x Asia-London Participation",
        symbol="ES",
        contracts=1,
        description="Run the promoted standalone ES overnight participation candidate on one ES contract.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-london-participation-candidate-system")
    parser.add_argument("--gc-source-json", default=str(DEFAULT_GC_SOURCE_JSON))
    parser.add_argument("--nq-source-json", default=str(DEFAULT_NQ_SOURCE_JSON))
    parser.add_argument("--es-source-json", default=str(DEFAULT_ES_SOURCE_JSON))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_london_participation_candidate_system_research(
        gc_source_json=Path(args.gc_source_json),
        nq_source_json=Path(args.nq_source_json),
        es_source_json=Path(args.es_source_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_asia_london_participation_candidate_system_research(
    *,
    gc_source_json: Path | None = None,
    nq_source_json: Path | None = None,
    es_source_json: Path | None = None,
    output_dir: Path | None = None,
    lane_definitions: tuple[CandidateLaneDefinition, ...] = DEFAULT_LANE_DEFINITIONS,
    scenarios: tuple[CandidateDeploymentScenario, ...] = DEFAULT_SCENARIOS,
) -> dict[str, Any]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    sources = {
        "GC_MGC": json.loads(Path(gc_source_json or DEFAULT_GC_SOURCE_JSON).resolve().read_text(encoding="utf-8")),
        "NQ_MNQ": json.loads(Path(nq_source_json or DEFAULT_NQ_SOURCE_JSON).resolve().read_text(encoding="utf-8")),
        "ES_ONLY": json.loads(Path(es_source_json or DEFAULT_ES_SOURCE_JSON).resolve().read_text(encoding="utf-8")),
    }

    lane_sequence: list[dict[str, Any]] = []
    for lane in lane_definitions:
        source_payload = sources[lane.symbol_group]
        if lane.symbol_group == "ES_ONLY":
            ranking_rows = {row["variant_key"]: row for row in source_payload["ranked_candidates"]}
            ranking_row = ranking_rows[lane.source_variant]
            ranking_summary = {
                "min_average_net_pnl_points": ranking_row["average_net_pnl_points"],
                "min_net_profit_factor": ranking_row["net_profit_factor"],
                "min_entered_trade_count": ranking_row["entered_trade_count"],
                "max_pair_drawdown_points": ranking_row["max_drawdown_points"],
            }
        else:
            pair_rows = {row["variant_key"]: row for row in source_payload["pair_rankings"][lane.symbol_group]}
            ranking_row = pair_rows[lane.source_variant]
            ranking_summary = {
                "min_average_net_pnl_points": ranking_row["min_average_net_pnl_points"],
                "min_net_profit_factor": ranking_row["min_net_profit_factor"],
                "min_entered_trade_count": ranking_row["min_entered_trade_count"],
                "max_pair_drawdown_points": ranking_row["max_pair_drawdown_points"],
            }
        lane_sequence.append(
            {
                **asdict(lane),
                "ranking_summary": ranking_summary,
            }
        )

    scenario_reports: list[dict[str, Any]] = []
    for scenario in scenarios:
        relevant_lanes = [lane for lane in lane_definitions if scenario.symbol in lane.symbol_group]
        lane_reports: list[dict[str, Any]] = []
        total_avg_points = 0.0
        min_pf = None
        max_drawdown = None
        entered_trade_count = None
        for lane in relevant_lanes:
            source_payload = sources[lane.symbol_group]
            if lane.symbol_group == "ES_ONLY":
                variant = next(
                    row for row in source_payload["ranked_candidates"] if row["variant_key"] == lane.source_variant
                )
                summary = {
                    "average_net_pnl_points": variant["average_net_pnl_points"],
                    "net_profit_factor": variant["net_profit_factor"],
                    "entered_trade_count": variant["entered_trade_count"],
                    "max_drawdown_points": variant["max_drawdown_points"],
                }
            else:
                variant = source_payload["symbol_reports"][scenario.symbol]["variants"][lane.source_variant]
                summary = variant["trade_summary"]
            total_avg_points += float(summary["average_net_pnl_points"] or 0.0)
            metric_pf = float(summary["net_profit_factor"] or 0.0)
            min_pf = metric_pf if min_pf is None else min(min_pf, metric_pf)
            metric_drawdown = float(summary["max_drawdown_points"] or 0.0)
            max_drawdown = metric_drawdown if max_drawdown is None else max(max_drawdown, metric_drawdown)
            metric_trades = int(summary["entered_trade_count"])
            entered_trade_count = metric_trades if entered_trade_count is None else min(entered_trade_count, metric_trades)
            lane_reports.append(
                {
                    "lane_id": lane.lane_id,
                    "average_net_pnl_points": summary["average_net_pnl_points"],
                    "net_profit_factor": summary["net_profit_factor"],
                    "entered_trade_count": summary["entered_trade_count"],
                    "max_drawdown_points": summary["max_drawdown_points"],
                }
            )
        scenario_reports.append(
            {
                "scenario_id": scenario.scenario_id,
                "label": scenario.label,
                "description": scenario.description,
                "symbol": scenario.symbol,
                "contracts": scenario.contracts,
                "lane_reports": lane_reports,
                "summary": {
                    "aggregate_average_net_pnl_dollars": round(total_avg_points * POINT_VALUE_BY_SYMBOL[scenario.symbol] * scenario.contracts, 2),
                    "minimum_lane_profit_factor": round(min_pf or 0.0, 4),
                    "max_lane_drawdown_dollars": round((max_drawdown or 0.0) * POINT_VALUE_BY_SYMBOL[scenario.symbol] * scenario.contracts, 2),
                    "minimum_entered_trade_count": int(entered_trade_count or 0),
                },
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_system": {
            "candidate_id": "asia_london_participation_core_v1",
            "family_name": "asia_london_participation_core_v1",
            "lane_sequence": lane_sequence,
        },
        "source_artifacts": {
            "gc_mgc_optimization_json": str(Path(gc_source_json or DEFAULT_GC_SOURCE_JSON).resolve()),
            "nq_mnq_optimization_json": str(Path(nq_source_json or DEFAULT_NQ_SOURCE_JSON).resolve()),
            "es_validation_json": str(Path(es_source_json or DEFAULT_ES_SOURCE_JSON).resolve()),
        },
        "scenario_reports": scenario_reports,
    }
    json_path = resolved_output_dir / "asia_london_participation_candidate_system_research.json"
    md_path = resolved_output_dir / "asia_london_participation_candidate_system_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "candidate_id": payload["candidate_system"]["candidate_id"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "scenario_reports": scenario_reports,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia-London Participation Candidate System",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Candidate id: `{payload['candidate_system']['candidate_id']}`",
        "",
        "## Lanes",
        "",
    ]
    for lane in payload["candidate_system"]["lane_sequence"]:
        lines.append(
            f"- `{lane['lane_id']}` `{lane['source_variant']}` avg `{lane['ranking_summary']['min_average_net_pnl_points']}` PF `{lane['ranking_summary']['min_net_profit_factor']}` DD `{lane['ranking_summary']['max_pair_drawdown_points']}`"
        )
    lines.extend(["", "## Scenarios", ""])
    for scenario in payload["scenario_reports"]:
        summary = scenario["summary"]
        lines.append(
            f"- `{scenario['scenario_id']}`: avg `${summary['aggregate_average_net_pnl_dollars']}`, min PF `{summary['minimum_lane_profit_factor']}`, max lane DD `${summary['max_lane_drawdown_dollars']}`, min trades `{summary['minimum_entered_trade_count']}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
