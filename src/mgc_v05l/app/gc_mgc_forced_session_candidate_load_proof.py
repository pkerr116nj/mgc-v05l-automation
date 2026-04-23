"""Load-proof utility for the promoted GC/MGC forced-session probationary package."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

from ..config_models import load_settings_from_files
from ..execution.execution_engine import ExecutionEngine
from ..execution.paper_broker import PaperBroker
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.logger import StructuredLogger
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from .gc_mgc_forced_session_candidate_admission_plan import (
    DEFAULT_BASE_CONFIG_PATHS,
    DEFAULT_OUTPUT_DIR as DEFAULT_ADMISSION_OUTPUT_DIR,
)
from .probationary_runtime import (
    ProbationaryLaneStructuredLogger,
    _build_probationary_paper_lane_settings,
    _build_probationary_strategy_engine,
    _load_probationary_paper_lane_specs,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PACKAGE_YAML = DEFAULT_ADMISSION_OUTPUT_DIR / "gc_1x_all_lanes.paper_package.yaml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_forced_session_candidate_load_proof_archive_v2"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-forced-session-candidate-load-proof")
    parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Base config paths. Defaults to base/live/probationary configs; later files override earlier ones.",
    )
    parser.add_argument(
        "--package-yaml",
        default=str(DEFAULT_PACKAGE_YAML),
        help="Package YAML published by the admission-plan command.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_forced_session_candidate_load_proof(
        config_paths=[Path(path) for path in args.config] if args.config else None,
        package_yaml=Path(args.package_yaml),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_forced_session_candidate_load_proof(
    *,
    config_paths: Sequence[Path] | None = None,
    package_yaml: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_package_yaml = Path(package_yaml or DEFAULT_PACKAGE_YAML).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_config_paths = [Path(path).resolve() for path in (config_paths or DEFAULT_BASE_CONFIG_PATHS)]
    settings = load_settings_from_files([*resolved_config_paths, resolved_package_yaml])
    specs = _load_probationary_paper_lane_specs(settings)
    root_logger = StructuredLogger(resolved_output_dir / "root_logger")
    runtime_root = resolved_output_dir / "isolated_runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)

    lane_rows: list[dict[str, Any]] = []
    for spec in specs:
        isolated_artifacts = runtime_root / "lanes" / spec.lane_id
        isolated_db = runtime_root / f"{spec.lane_id}.sqlite3"
        lane_settings = _build_probationary_paper_lane_settings(settings, spec).model_copy(
            update={
                "database_url": f"sqlite:///{isolated_db}",
                "probationary_artifacts_dir": str(isolated_artifacts),
            }
        )
        repositories = RepositorySet(
            build_engine(lane_settings.database_url),
            runtime_identity=_runtime_identity_for_spec(spec),
        )
        logger = ProbationaryLaneStructuredLogger(
            lane_id=spec.lane_id,
            symbol=spec.symbol,
            root_logger=root_logger,
            lane_logger=StructuredLogger(lane_settings.probationary_artifacts_path),
        )
        engine = _build_probationary_strategy_engine(
            spec=spec,
            settings=lane_settings,
            repositories=repositories,
            execution_engine=ExecutionEngine(broker=PaperBroker()),
            structured_logger=logger,
            alert_dispatcher=AlertDispatcher(logger),
            runtime_identity=_runtime_identity_for_spec(spec),
        )
        lane_rows.append(
            {
                "lane_id": spec.lane_id,
                "symbol": spec.symbol,
                "runtime_kind": spec.runtime_kind,
                "engine_class": type(engine).__name__,
                "strategy_family": spec.strategy_family,
                "session_restriction": spec.session_restriction,
                "long_sources": list(spec.long_sources),
                "short_sources": list(spec.short_sources),
                "structural_signal_timeframe": lane_settings.resolved_structural_signal_timeframe,
                "execution_timeframe": lane_settings.resolved_execution_timeframe,
                "artifact_timeframe": lane_settings.resolved_artifact_timeframe,
                "context_timeframes": list(lane_settings.resolved_context_timeframes),
                "approved_long_entry_sources": sorted(lane_settings.approved_long_entry_sources),
                "approved_short_entry_sources": sorted(lane_settings.approved_short_entry_sources),
            }
        )

    payload = {
        "mode": "gc_mgc_forced_session_candidate_load_proof",
        "generated_at": datetime.now(UTC).isoformat(),
        "package_yaml": str(resolved_package_yaml),
        "config_paths": [str(path) for path in resolved_config_paths],
        "probationary_paper_runtime_exclusive_config": settings.probationary_paper_runtime_exclusive_config,
        "lane_count": len(lane_rows),
        "lane_ids": [row["lane_id"] for row in lane_rows],
        "all_loaded": bool(lane_rows),
        "load_scope": "lane_specs_and_engines_only",
        "notes": [
            "This proof loads the published package into probationary lane specs and instantiates the strategy engines.",
            "It does not start live polling or the full paper supervisor.",
            "Per-lane databases and artifacts are redirected into an isolated proof directory.",
        ],
        "lanes": lane_rows,
    }
    json_path = resolved_output_dir / "gc_mgc_forced_session_candidate_load_proof.json"
    md_path = resolved_output_dir / "gc_mgc_forced_session_candidate_load_proof.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "lane_count": payload["lane_count"],
        "lane_ids": payload["lane_ids"],
        "all_loaded": payload["all_loaded"],
    }


def _runtime_identity_for_spec(spec: Any) -> dict[str, Any]:
    standalone_strategy_id = str(getattr(spec, "standalone_strategy_id", None) or spec.lane_id)
    return {
        "standalone_strategy_id": standalone_strategy_id,
        "strategy_family": spec.strategy_family,
        "instrument": spec.symbol,
        "lane_id": spec.lane_id,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC Forced Session Candidate Load Proof",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Package YAML: `{payload['package_yaml']}`",
        f"- Runtime-exclusive package: `{payload['probationary_paper_runtime_exclusive_config']}`",
        f"- Lane count: `{payload['lane_count']}`",
        "",
        "## Lanes",
        "",
    ]
    for lane in payload["lanes"]:
        lines.append(
            f"- `{lane['lane_id']}`: `{lane['engine_class']}` on `{lane['symbol']}` with "
            f"`{lane['structural_signal_timeframe']}` -> `{lane['execution_timeframe']}`"
        )
    return "\n".join(lines)
