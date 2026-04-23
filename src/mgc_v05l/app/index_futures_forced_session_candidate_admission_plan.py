"""Paper-admission packages for the promoted stock-index forced-session baseline."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .gc_mgc_forced_session_candidate_admission_plan import _render_yaml_package
from .index_futures_forced_session_candidate_system_research import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CANDIDATE_SYSTEM_OUTPUT_DIR,
)
from .index_futures_forced_session_runtime import (
    INDEX_FUTURES_FORCED_SESSION_FAMILY,
    INDEX_FUTURES_FORCED_SESSION_RUNTIME_KIND,
    INDEX_NY_EARLY_LONG_SOURCE,
    INDEX_NY_EARLY_SHORT_BREAKDOWN_SOURCE,
    INDEX_NY_EARLY_SHORT_RECLAIM_FAIL_SOURCE,
    INDEX_NY_LATE_LONG_SOURCE,
    INDEX_NY_LATE_SHORT_BREAKDOWN_SOURCE,
    INDEX_US_LATE_LONG_SOURCE,
    INDEX_US_LATE_SHORT_RECLAIM_FAIL_SOURCE,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_SYSTEM_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "index_futures_forced_session_candidate_system_archive_v1"
    / "index_futures_forced_session_candidate_system_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "index_futures_forced_session_candidate_admission_archive_v1"
DEFAULT_BASE_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "live.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
)
FAMILY_NAME = INDEX_FUTURES_FORCED_SESSION_FAMILY
STRATEGY_IDENTITY_ROOT = "INDEX_FUTURES_NY_INTRADAY_FORCED_CORE_V2"
LANE_MODE = "INDEX_FUTURES_FORCED_SESSION_CANDIDATE"
EXPERIMENTAL_STATUS = "paper_candidate"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="index-futures-forced-session-candidate-admission-plan")
    parser.add_argument("--candidate-system-json", default=str(DEFAULT_CANDIDATE_SYSTEM_JSON), help="Candidate-system JSON artifact.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_index_futures_forced_session_candidate_admission_plan(
        candidate_system_json=Path(args.candidate_system_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_index_futures_forced_session_candidate_admission_plan(
    *,
    candidate_system_json: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_candidate_json = Path(candidate_system_json or DEFAULT_CANDIDATE_SYSTEM_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    candidate_payload = json.loads(resolved_candidate_json.read_text(encoding="utf-8"))
    candidate = candidate_payload["candidate_system"]
    lane_sequence = list(candidate["lane_sequence"])
    scenario_rows = list(candidate_payload["scenario_reports"])

    package_scenarios: list[dict[str, Any]] = []
    combined_lane_rows: list[dict[str, Any]] = []
    for scenario in scenario_rows:
        config_payload = _build_scenario_config_payload(
            scenario_id=str(scenario["scenario_id"]),
            label=str(scenario["label"]),
            description=str(scenario["description"]),
            symbol=str(scenario["symbol"]),
            contracts=int(scenario["contracts"]),
            lane_sequence=lane_sequence,
        )
        json_path = resolved_output_dir / f"{scenario['scenario_id']}.paper_package.json"
        yaml_path = resolved_output_dir / f"{scenario['scenario_id']}.paper_package.yaml"
        json_path.write_text(json.dumps(config_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        yaml_path.write_text(_render_yaml_package(config_payload).strip() + "\n", encoding="utf-8")
        package_scenarios.append(
            {
                "scenario_id": scenario["scenario_id"],
                "label": scenario["label"],
                "description": scenario["description"],
                "symbol": scenario["symbol"],
                "contracts": scenario["contracts"],
                "summary": scenario["summary"],
                "config_json_path": str(json_path),
                "config_yaml_path": str(yaml_path),
                "lane_count": len(config_payload["probationary_paper_lanes"]),
            }
        )
        combined_lane_rows.extend(config_payload["probationary_paper_lanes"])

    combined_payload = {
        "package_id": "index_futures_all_variants_1x",
        "package_label": "Index Futures All Variants 1x",
        "package_description": "All promoted ES, MES, NQ, and MNQ forced-session variants across NY early, U.S. midday, and U.S. late at base size 1.",
        "base_config_paths": [str(path) for path in DEFAULT_BASE_CONFIG_PATHS],
        "probationary_paper_lanes": combined_lane_rows,
    }
    combined_json_path = resolved_output_dir / "index_futures_all_variants_1x.paper_package.json"
    combined_yaml_path = resolved_output_dir / "index_futures_all_variants_1x.paper_package.yaml"
    combined_json_path.write_text(json.dumps(combined_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    combined_yaml_path.write_text(_render_yaml_package(combined_payload).strip() + "\n", encoding="utf-8")

    payload = {
        "mode": "index_futures_forced_session_candidate_admission_plan",
        "generated_at": datetime.now(UTC).isoformat(),
        "family_name": FAMILY_NAME,
        "candidate_id": candidate["candidate_id"],
        "candidate_system_json": str(resolved_candidate_json),
        "admission_status": "PACKAGE_READY_AND_RUNTIME_WIRING_REQUIRED",
        "frozen_source_identifiers": _frozen_source_identifiers(lane_sequence),
        "package_scenarios": package_scenarios,
        "combined_package": {
            "scenario_id": "index_futures_all_variants_1x",
            "config_json_path": str(combined_json_path),
            "config_yaml_path": str(combined_yaml_path),
            "lane_count": len(combined_lane_rows),
        },
        "runtime_wiring_plan": _runtime_wiring_plan(),
    }

    json_path = resolved_output_dir / "index_futures_forced_session_candidate_admission_plan.json"
    md_path = resolved_output_dir / "index_futures_forced_session_candidate_admission_plan.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "package_scenarios": [
            {
                "scenario_id": row["scenario_id"],
                "config_yaml_path": row["config_yaml_path"],
                "summary": row["summary"],
            }
            for row in package_scenarios
        ],
        "combined_package_yaml": str(combined_yaml_path),
    }


def _build_scenario_config_payload(
    *,
    scenario_id: str,
    label: str,
    description: str,
    symbol: str,
    contracts: int,
    lane_sequence: list[dict[str, Any]],
) -> dict[str, Any]:
    lane_rows: list[dict[str, Any]] = []
    for lane in lane_sequence:
        lane_id = str(lane["lane_id"])
        side = str(lane["side"]).upper()
        source_id = _lane_source_id(lane_id)
        artifacts_slug = f"{scenario_id}__{lane_id.lower()}"
        lane_rows.append(
            {
                "lane_id": f"{scenario_id}__{lane_id.lower()}",
                "display_name": f"{symbol} / {lane_id} / x{contracts}",
                "symbol": symbol,
                "standalone_strategy_id": f"{STRATEGY_IDENTITY_ROOT.lower()}__{scenario_id}__{lane_id.lower()}",
                "identity_components": ["paper", symbol.lower(), scenario_id, lane_id.lower()],
                "long_sources": [source_id] if side == "LONG" else [],
                "short_sources": [source_id] if side == "SHORT" else [],
                "session_restriction": str(lane["segment_id"]),
                "allowed_sessions": [str(lane["segment_id"])],
                "point_value": str(_point_value_for_symbol(symbol)),
                "trade_size": contracts,
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "max_concurrent_entries": 1,
                "max_position_quantity": contracts,
                "max_adds_after_entry": 0,
                "add_direction_policy": "SAME_DIRECTION_ONLY",
                "catastrophic_open_loss": str(_catastrophic_open_loss_for_symbol(symbol)),
                "lane_mode": LANE_MODE,
                "strategy_family": FAMILY_NAME,
                "strategy_identity_root": STRATEGY_IDENTITY_ROOT,
                "runtime_kind": INDEX_FUTURES_FORCED_SESSION_RUNTIME_KIND,
                "structural_signal_timeframe": "3m",
                "execution_timeframe": "1m",
                "artifact_timeframe": "3m",
                "context_timeframes": ["3m"],
                "live_poll_lookback_minutes": 1440,
                "observed_instruments": [symbol],
                "experimental_status": EXPERIMENTAL_STATUS,
                "paper_only": True,
                "non_approved": True,
                "package_id": scenario_id,
                "package_label": label,
                "artifacts_dir": f"./outputs/probationary_pattern_engine/paper_session/lanes/{artifacts_slug}",
                "database_url": f"sqlite:///./mgc_v05l.probationary.paper__{artifacts_slug}.sqlite3",
            }
        )
    return {
        "package_id": scenario_id,
        "package_label": label,
        "package_description": description,
        "base_config_paths": [str(path) for path in DEFAULT_BASE_CONFIG_PATHS],
        "probationary_paper_lanes": lane_rows,
    }


def _point_value_for_symbol(symbol: str) -> int:
    return {"ES": 50, "MES": 5, "NQ": 20, "MNQ": 2}[str(symbol).upper()]


def _catastrophic_open_loss_for_symbol(symbol: str) -> int:
    return {"ES": -1000, "MES": -300, "NQ": -1000, "MNQ": -300}[str(symbol).upper()]


def _lane_source_id(lane_id: str) -> str:
    mapping = {
        "US_EARLY_LONG": INDEX_NY_EARLY_LONG_SOURCE,
        "US_EARLY_SHORT_BREAKDOWN": INDEX_NY_EARLY_SHORT_BREAKDOWN_SOURCE,
        "US_EARLY_SHORT_RECLAIM_FAIL": INDEX_NY_EARLY_SHORT_RECLAIM_FAIL_SOURCE,
        "US_MIDDAY_LONG": INDEX_NY_LATE_LONG_SOURCE,
        "US_MIDDAY_SHORT_BREAKDOWN": INDEX_NY_LATE_SHORT_BREAKDOWN_SOURCE,
        "US_LATE_LONG": INDEX_US_LATE_LONG_SOURCE,
        "US_LATE_SHORT_RECLAIM_FAIL": INDEX_US_LATE_SHORT_RECLAIM_FAIL_SOURCE,
    }
    return mapping[str(lane_id)]


def _frozen_source_identifiers(lane_sequence: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "lane_id": str(lane["lane_id"]),
            "segment_id": str(lane["segment_id"]),
            "side": str(lane["side"]).upper(),
            "source_identifier": _lane_source_id(str(lane["lane_id"])),
        }
        for lane in lane_sequence
    ]


def _runtime_wiring_plan() -> dict[str, Any]:
    return {
        "exact_files_to_change": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/index_futures_forced_session_runtime.py"),
                "purpose": "Executable runtime for ES/MES/NQ/MNQ NY-early forced-session candidate lanes.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "purpose": "Instantiate the stock-index forced-session runtime kind inside probationary paper execution.",
            },
        ]
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Index Futures Forced Session Candidate Admission Plan",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Candidate ID: `{payload['candidate_id']}`",
        "",
        "## Package Scenarios",
        "",
    ]
    for row in payload["package_scenarios"]:
        summary = row["summary"]
        lines.append(
            f"- `{row['scenario_id']}`: avg `${summary['aggregate_average_net_pnl_dollars']}`, min lane PF `{summary['minimum_lane_profit_factor']}`, yaml `{row['config_yaml_path']}`"
        )
    lines.append("")
    lines.append(f"- `index_futures_all_variants_1x`: yaml `{payload['combined_package']['config_yaml_path']}`")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
