"""Paper-admission package for promoted Asia-to-London participation candidates."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .asia_london_participation_candidate_system_research import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CANDIDATE_SYSTEM_OUTPUT_DIR,
)
from .asia_london_participation_runtime import (
    ASIA_LONDON_PARTICIPATION_FAMILY,
    ASIA_LONDON_PARTICIPATION_RUNTIME_KIND,
    ES_ASIA_LONDON_LONG_V6_VOL_FLOOR_125_SOURCE,
    GC_ASIA_LONDON_LONG_V5_SOURCE,
    GC_ASIA_LONDON_SHORT_V2_SOURCE,
    NQ_ASIA_LONDON_LONG_V5_SOURCE,
    NQ_ASIA_LONDON_LONG_V6_SOURCE,
    NQ_ASIA_LONDON_SHORT_V2_SOURCE,
)
from .gc_mgc_forced_session_candidate_admission_plan import _render_yaml_package


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CANDIDATE_SYSTEM_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "asia_london_participation_candidate_system_archive_v2"
    / "asia_london_participation_candidate_system_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "asia_london_participation_candidate_admission_archive_v2"
DEFAULT_BASE_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "live.yaml",
    REPO_ROOT / "config" / "probationary_pattern_engine.yaml",
)
STRATEGY_IDENTITY_ROOT = "ASIA_LONDON_PARTICIPATION_CORE_V1"
LANE_MODE = "ASIA_LONDON_PARTICIPATION_CANDIDATE"
EXPERIMENTAL_STATUS = "paper_candidate"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-london-participation-candidate-admission-plan")
    parser.add_argument("--candidate-system-json", default=str(DEFAULT_CANDIDATE_SYSTEM_JSON))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_london_participation_candidate_admission_plan(
        candidate_system_json=Path(args.candidate_system_json),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_asia_london_participation_candidate_admission_plan(
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
            lane_sequence=[lane for lane in lane_sequence if scenario["symbol"] in lane["symbol_group"]],
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
        "package_id": "asia_london_participation_all_variants_1x",
        "package_label": "Asia London Participation All Variants 1x",
        "package_description": "Promoted Asia-to-London participation lanes across GC/MGC, NQ/MNQ, and standalone ES at base size 1.",
        "base_config_paths": [str(path) for path in DEFAULT_BASE_CONFIG_PATHS],
        "probationary_paper_lanes": combined_lane_rows,
    }
    combined_json_path = resolved_output_dir / "asia_london_participation_all_variants_1x.paper_package.json"
    combined_yaml_path = resolved_output_dir / "asia_london_participation_all_variants_1x.paper_package.yaml"
    combined_json_path.write_text(json.dumps(combined_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    combined_yaml_path.write_text(_render_yaml_package(combined_payload).strip() + "\n", encoding="utf-8")

    payload = {
        "mode": "asia_london_participation_candidate_admission_plan",
        "generated_at": datetime.now(UTC).isoformat(),
        "family_name": ASIA_LONDON_PARTICIPATION_FAMILY,
        "candidate_id": candidate["candidate_id"],
        "candidate_system_json": str(resolved_candidate_json),
        "admission_status": "PACKAGE_READY_AND_RUNTIME_WIRING_REQUIRED",
        "frozen_source_identifiers": _frozen_source_identifiers(lane_sequence),
        "package_scenarios": package_scenarios,
        "combined_package": {
            "scenario_id": "asia_london_participation_all_variants_1x",
            "config_json_path": str(combined_json_path),
            "config_yaml_path": str(combined_yaml_path),
            "lane_count": len(combined_lane_rows),
        },
    }
    json_path = resolved_output_dir / "asia_london_participation_candidate_admission_plan.json"
    md_path = resolved_output_dir / "asia_london_participation_candidate_admission_plan.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "combined_package_yaml": str(combined_yaml_path),
        "package_scenarios": package_scenarios,
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
        source_id = _lane_source_id(lane_id=lane_id, symbol=symbol)
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
                "session_restriction": "ASIA_EARLY/ASIA_LATE/LONDON_EARLY/LONDON_LATE",
                "allowed_sessions": ["ASIA_EARLY", "ASIA_LATE", "LONDON_EARLY", "LONDON_LATE"],
                "point_value": str(_point_value_for_symbol(symbol)),
                "trade_size": contracts,
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "max_concurrent_entries": 1,
                "max_position_quantity": contracts,
                "max_adds_after_entry": 0,
                "add_direction_policy": "SAME_DIRECTION_ONLY",
                "catastrophic_open_loss": str(_catastrophic_open_loss_for_symbol(symbol)),
                "lane_mode": LANE_MODE,
                "strategy_family": ASIA_LONDON_PARTICIPATION_FAMILY,
                "strategy_identity_root": STRATEGY_IDENTITY_ROOT,
                "runtime_kind": ASIA_LONDON_PARTICIPATION_RUNTIME_KIND,
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
    return {"GC": 100, "MGC": 10, "NQ": 20, "MNQ": 2, "ES": 50}[str(symbol).upper()]


def _catastrophic_open_loss_for_symbol(symbol: str) -> int:
    return {"GC": -1000, "MGC": -500, "NQ": -1000, "MNQ": -300, "ES": -1000}[str(symbol).upper()]


def _lane_source_id(*, lane_id: str, symbol: str) -> str:
    mapping = {
        ("GC", "ASIA_LONDON_LONG_V5"): GC_ASIA_LONDON_LONG_V5_SOURCE,
        ("MGC", "ASIA_LONDON_LONG_V5"): GC_ASIA_LONDON_LONG_V5_SOURCE,
        ("GC", "ASIA_LONDON_SHORT_V2"): GC_ASIA_LONDON_SHORT_V2_SOURCE,
        ("MGC", "ASIA_LONDON_SHORT_V2"): GC_ASIA_LONDON_SHORT_V2_SOURCE,
        ("NQ", "ASIA_LONDON_LONG_V6"): NQ_ASIA_LONDON_LONG_V6_SOURCE,
        ("MNQ", "ASIA_LONDON_LONG_V6"): NQ_ASIA_LONDON_LONG_V6_SOURCE,
        ("NQ", "ASIA_LONDON_LONG_V5"): NQ_ASIA_LONDON_LONG_V5_SOURCE,
        ("MNQ", "ASIA_LONDON_LONG_V5"): NQ_ASIA_LONDON_LONG_V5_SOURCE,
        ("NQ", "ASIA_LONDON_SHORT_V2"): NQ_ASIA_LONDON_SHORT_V2_SOURCE,
        ("MNQ", "ASIA_LONDON_SHORT_V2"): NQ_ASIA_LONDON_SHORT_V2_SOURCE,
        ("ES", "ASIA_LONDON_LONG_V6_VOL_FLOOR_125"): ES_ASIA_LONDON_LONG_V6_VOL_FLOOR_125_SOURCE,
    }
    return mapping[(str(symbol).upper(), str(lane_id))]


def _frozen_source_identifiers(lane_sequence: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for lane in lane_sequence:
        rows.append(
            {
                "lane_id": str(lane["lane_id"]),
                "segment_id": str(lane["segment_id"]),
                "side": str(lane["side"]).upper(),
                "symbol_group": str(lane["symbol_group"]),
                "source_variant": str(lane["source_variant"]),
            }
        )
    return rows


if __name__ == "__main__":
    raise SystemExit(main())
