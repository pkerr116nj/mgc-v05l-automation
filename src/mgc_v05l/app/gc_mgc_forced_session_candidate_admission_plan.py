"""Paper-admission package for the promoted GC/MGC forced-session baseline."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .gc_mgc_forced_session_candidate_system_research import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CANDIDATE_SYSTEM_OUTPUT_DIR,
)
from .gc_mgc_forced_session_runtime import (
    ASIA_EARLY_LONG_SOURCE,
    ASIA_EARLY_SHORT_SOURCE,
    GC_MGC_FORCED_SESSION_FAMILY,
    GC_MGC_FORCED_SESSION_RUNTIME_KIND,
    LONDON_EARLY_LONG_SOURCE,
    NY_EARLY_SHORT_SOURCE,
    NY_LATE_SHORT_SOURCE,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_SYSTEM_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_forced_session_candidate_system_archive_v2"
    / "gc_mgc_forced_session_candidate_system_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_forced_session_candidate_admission_archive_v2"
DEFAULT_BASE_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "live.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
)
FAMILY_NAME = GC_MGC_FORCED_SESSION_FAMILY
STRATEGY_IDENTITY_ROOT = "GC_MGC_FORCED_SESSION_BASELINE_V2"
LANE_MODE = "GC_MGC_FORCED_SESSION_CANDIDATE"
EXPERIMENTAL_STATUS = "paper_candidate"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-forced-session-candidate-admission-plan")
    parser.add_argument(
        "--candidate-system-json",
        default=str(DEFAULT_CANDIDATE_SYSTEM_JSON),
        help="Candidate-system JSON artifact path.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_forced_session_candidate_admission_plan(
        candidate_system_json=Path(args.candidate_system_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_forced_session_candidate_admission_plan(
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

    scenario_configs: list[dict[str, Any]] = []
    for scenario in scenario_rows:
        config_payload = _build_scenario_config_payload(
            scenario_id=str(scenario["scenario_id"]),
            label=str(scenario["label"]),
            description=str(scenario["description"]),
            allocations=list(scenario["allocations"]),
            lane_sequence=lane_sequence,
        )
        json_path = resolved_output_dir / f"{scenario['scenario_id']}.paper_package.json"
        yaml_path = resolved_output_dir / f"{scenario['scenario_id']}.paper_package.yaml"
        json_path.write_text(json.dumps(config_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        yaml_path.write_text(_render_yaml_package(config_payload).strip() + "\n", encoding="utf-8")
        scenario_configs.append(
            {
                "scenario_id": scenario["scenario_id"],
                "label": scenario["label"],
                "description": scenario["description"],
                "summary": scenario["summary"],
                "config_json_path": str(json_path),
                "config_yaml_path": str(yaml_path),
                "lane_count": len(config_payload["probationary_paper_lanes"]),
            }
        )

    payload = {
        "mode": "gc_mgc_forced_session_candidate_admission_plan",
        "generated_at": datetime.now(UTC).isoformat(),
        "family_name": FAMILY_NAME,
        "candidate_id": candidate["candidate_id"],
        "candidate_system_json": str(resolved_candidate_json),
        "admission_status": "PACKAGE_READY_AND_RUNTIME_WIRED",
        "frozen_source_identifiers": _frozen_source_identifiers(lane_sequence),
        "package_scenarios": scenario_configs,
        "runtime_wiring_plan": _runtime_wiring_plan(),
        "operator_readiness_notes": [
            "These package files now target the custom forced-session runtime directly and load as native probationary paper-lane configs.",
            "The strongest current deployment candidate remains GC 1x all lanes; MGC 1x remains the lowest-footprint rollout option.",
            "No active runtime config is modified by this package generator; it only publishes candidate package artifacts and isolated package YAMLs.",
        ],
    }

    json_path = resolved_output_dir / "gc_mgc_forced_session_candidate_admission_plan.json"
    md_path = resolved_output_dir / "gc_mgc_forced_session_candidate_admission_plan.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "admission_status": payload["admission_status"],
        "package_scenarios": [
            {
                "scenario_id": row["scenario_id"],
                "config_yaml_path": row["config_yaml_path"],
                "summary": row["summary"],
            }
            for row in scenario_configs
        ],
    }


def _frozen_source_identifiers(lane_sequence: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for lane in lane_sequence:
        lane_id = str(lane["lane_id"])
        side = str(lane["side"]).upper()
        source_id = _lane_source_id(lane_id)
        rows.append(
            {
                "lane_id": lane_id,
                "segment_id": str(lane["segment_id"]),
                "side": side,
                "source_identifier": source_id,
            }
        )
    return rows


def _build_scenario_config_payload(
    *,
    scenario_id: str,
    label: str,
    description: str,
    allocations: list[dict[str, Any]],
    lane_sequence: list[dict[str, Any]],
) -> dict[str, Any]:
    lane_lookup = {str(lane["lane_id"]): dict(lane) for lane in lane_sequence}
    lane_rows: list[dict[str, Any]] = []
    for allocation in allocations:
        lane_id = str(allocation["lane_id"])
        lane = lane_lookup[lane_id]
        symbol = str(allocation["symbol"]).upper()
        contracts = int(allocation["contracts"])
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
                "point_value": "100" if symbol == "GC" else "10",
                "trade_size": contracts,
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "max_concurrent_entries": 1,
                "max_position_quantity": contracts,
                "max_adds_after_entry": 0,
                "add_direction_policy": "SAME_DIRECTION_ONLY",
                "catastrophic_open_loss": "-1000" if symbol == "GC" else "-500",
                "lane_mode": LANE_MODE,
                "strategy_family": FAMILY_NAME,
                "strategy_identity_root": STRATEGY_IDENTITY_ROOT,
                "runtime_kind": GC_MGC_FORCED_SESSION_RUNTIME_KIND,
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


def _lane_source_id(lane_id: str) -> str:
    mapping = {
        "ASIA_EARLY_LONG": ASIA_EARLY_LONG_SOURCE,
        "ASIA_EARLY_SHORT": ASIA_EARLY_SHORT_SOURCE,
        "LONDON_EARLY_LONG": LONDON_EARLY_LONG_SOURCE,
        "US_EARLY_SHORT": NY_EARLY_SHORT_SOURCE,
        "US_MIDDAY_SHORT": NY_LATE_SHORT_SOURCE,
    }
    try:
        return mapping[str(lane_id)]
    except KeyError as exc:
        raise ValueError(f"Unsupported forced-session lane_id: {lane_id}") from exc


def _runtime_wiring_plan() -> dict[str, Any]:
    return {
        "summary": "The forced-session baseline is now wired into the probationary paper runtime through a custom runtime kind and gold-native session matching.",
        "exact_files_to_change": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/gc_mgc_forced_session_runtime.py"),
                "purpose": "Executable custom runtime that evaluates the promoted 3m forced-session gold lanes and manages segment-local exits.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "purpose": "Instantiate the custom runtime kind and treat gold-native session restrictions as first-class operator eligibility windows.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/strategy/strategy_engine.py"),
                "purpose": "Respect gold-native session restrictions at bar evaluation time for probationary paper lanes.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/gc_mgc_forced_session_candidate_admission_plan.py"),
                "purpose": "Publish package YAMLs with the custom runtime kind and the promoted 3m structural / 1m execution cadence.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/gc_mgc_forced_session_candidate_load_proof.py"),
                "purpose": "Load a published package, instantiate the custom engines, and emit a proof artifact without starting live polling.",
            },
        ],
        "validation_surface": [
            str(REPO_ROOT / "tests/unit/test_gc_mgc_forced_session_candidate_admission_plan.py"),
            str(REPO_ROOT / "tests/unit/test_gc_mgc_forced_session_candidate_load_proof.py"),
            str(REPO_ROOT / "tests/integration/test_mgc_v05l_cli.py"),
        ],
    }


def _render_yaml_package(config_payload: dict[str, Any]) -> str:
    lanes_json = json.dumps(config_payload["probationary_paper_lanes"], separators=(",", ":"))
    lines = [
        'mode: "paper"',
        'environment_mode: "live_execution_mode"',
        'structural_signal_timeframe: "3m"',
        'execution_timeframe: "1m"',
        'artifact_timeframe: "3m"',
        'context_timeframes: ["3m"]',
        'execution_timeframe_role: "execution_detail_only"',
        'database_url: "sqlite:///./mgc_v05l.probationary.paper.sqlite3"',
        'probationary_artifacts_dir: "./outputs/probationary_pattern_engine/paper_session"',
        "probationary_paper_runtime_exclusive_config: true",
        f"probationary_paper_lanes_json: '{lanes_json}'",
        "probationary_paper_execution_canary_enabled: false",
        "probationary_paper_disable_loss_halts: true",
        "probationary_paper_desk_halt_new_entries_loss: -1500",
        "probationary_paper_desk_flatten_and_halt_loss: -2500",
        "probationary_paper_lane_realized_loser_limit_per_session: 2",
    ]
    return "\n".join(lines)


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC Forced Session Candidate Admission Plan",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Candidate ID: `{payload['candidate_id']}`",
        f"- Candidate system JSON: `{payload['candidate_system_json']}`",
        f"- Admission status: `{payload['admission_status']}`",
        "",
        "## Frozen Sources",
        "",
    ]
    for row in payload["frozen_source_identifiers"]:
        lines.append(f"- `{row['lane_id']}` -> `{row['source_identifier']}`")
    lines.extend(["", "## Package Scenarios", ""])
    for row in payload["package_scenarios"]:
        summary = row["summary"]
        lines.append(
            f"- `{row['scenario_id']}`: total `${summary['total_net_pnl_dollars']}`, PF `{summary['net_profit_factor']}`, yaml `{row['config_yaml_path']}`"
        )
    lines.extend(["", "## Wiring Plan", ""])
    for row in payload["runtime_wiring_plan"]["exact_files_to_change"]:
        lines.append(f"- `{row['path']}`: {row['purpose']}")
    return "\n".join(lines)
