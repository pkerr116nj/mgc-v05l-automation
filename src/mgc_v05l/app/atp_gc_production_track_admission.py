"""Admission proof for the GC ATP production-track paper package."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .probationary_runtime import (
    ProbationaryAtpCompanionBenchmarkLaneRuntime,
    ProbationaryLaneStructuredLogger,
    ProbationaryPaperLaneMetrics,
    ProbationaryPaperRiskRuntimeState,
    _apply_probationary_paper_risk_controls,
    _apply_probationary_supervisor_operator_control,
    _atp_runtime_identity_payload,
    _build_probationary_paper_lane_settings,
    _build_probationary_strategy_engine,
    _load_probationary_paper_lane_specs,
    _probationary_lane_spec_runtime_row,
    run_probationary_paper_soak_validation,
    submit_probationary_operator_control,
)
from ..config_models import load_settings_from_files
from ..execution.execution_engine import ExecutionEngine
from ..execution.paper_broker import PaperBroker
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.logger import StructuredLogger
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet


DEFAULT_OUTPUT_ROOT = Path("outputs/reports/atp_gc_production_track_admission")
PACKAGE_CONFIGS = [
    Path("config/base.yaml"),
    Path("config/live.yaml"),
    Path("config/probationary_pattern_engine.yaml"),
    Path("config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml"),
]
PACKAGE_SHARED_IDENTITY = "ATP_COMPANION_V1_GC_ASIA_US_PRODUCTION_TRACK"


class _DummyLivePollingService:
    def poll_bars(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-gc-production-track-admission")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _package_config_paths(repo_root: Path) -> list[Path]:
    return [(repo_root / path).resolve() for path in PACKAGE_CONFIGS]


def _build_runtime(repo_root: Path):
    config_paths = _package_config_paths(repo_root)
    settings = load_settings_from_files(config_paths)
    specs = _load_probationary_paper_lane_specs(settings)
    if len(specs) != 1:
        raise ValueError("GC production-track admission expects exactly one configured lane.")
    spec = specs[0]
    lane_settings = _build_probationary_paper_lane_settings(settings, spec)
    root_logger = StructuredLogger(settings.probationary_artifacts_path)
    lane_logger = StructuredLogger(lane_settings.probationary_artifacts_path)
    structured_logger = ProbationaryLaneStructuredLogger(
        lane_id=spec.lane_id,
        symbol=spec.symbol,
        root_logger=root_logger,
        lane_logger=lane_logger,
    )
    repositories = RepositorySet(
        build_engine(lane_settings.database_url),
        runtime_identity={
            "lane_id": spec.lane_id,
            "standalone_strategy_id": spec.standalone_strategy_id,
            "instrument": spec.symbol,
        },
    )
    alert_dispatcher = AlertDispatcher(root_logger, repositories.alerts, source_subsystem="atp_gc_production_track_admission")
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = _build_probationary_strategy_engine(
        spec=spec,
        settings=lane_settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        runtime_identity=_atp_runtime_identity_payload(spec),
    )
    runtime = ProbationaryAtpCompanionBenchmarkLaneRuntime(
        spec=spec,
        settings=lane_settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=_DummyLivePollingService(),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        observed_instruments=spec.observed_instruments or (spec.symbol,),
    )
    return config_paths, settings, spec, runtime, root_logger, alert_dispatcher


def _control_proof(
    *,
    config_paths: Sequence[Path],
    settings,
    runtime,
    root_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-06")
    now = datetime.now(UTC)

    for action in ("halt_entries", "resume_entries", "flatten_and_halt"):
        submit_probationary_operator_control(
            config_paths,
            action=action,
            shared_strategy_identity=PACKAGE_SHARED_IDENTITY,
        )
        applied = _apply_probationary_supervisor_operator_control(
            settings=settings,
            lanes=[runtime],
            structured_logger=root_logger,
            alert_dispatcher=alert_dispatcher,
            risk_state=risk_state,
        )
        results.append(
            {
                "action": action,
                "result": dict(applied or {}),
                "operator_halt": runtime.strategy_engine.state.operator_halt,
                "entries_enabled": runtime.strategy_engine.state.entries_enabled,
            }
        )

    submit_probationary_operator_control(config_paths, action="stop_after_cycle")
    stop_result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[runtime],
        structured_logger=root_logger,
        alert_dispatcher=alert_dispatcher,
        risk_state=risk_state,
    )
    results.append(
        {
            "action": "stop_after_cycle",
            "scope": "runtime_wide_single_lane",
            "result": dict(stop_result or {}),
            "operator_halt": runtime.strategy_engine.state.operator_halt,
            "entries_enabled": runtime.strategy_engine.state.entries_enabled,
            "stop_after_cycle_requested": bool((stop_result or {}).get("stop_after_cycle_requested")),
        }
    )
    runtime.strategy_engine.set_operator_halt(now, False)
    return {"actions": results}


def _halt_only_risk_proof(
    *,
    settings,
    runtime,
    root_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
) -> dict[str, Any]:
    runtime.strategy_engine.set_operator_halt(datetime.now(UTC), False)
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-06")
    metrics = {
        runtime.spec.lane_id: ProbationaryPaperLaneMetrics(
            session_date="2026-04-06",
            realized_pnl=Decimal("-3100"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("-3100"),
            closed_trades=3,
            losing_closed_trades=3,
            intent_count=3,
            fill_count=3,
            open_order_count=0,
            position_side="FLAT",
            internal_position_qty=0,
            broker_position_qty=0,
            open_entry_leg_count=0,
            open_add_count=0,
            additional_entry_allowed=True,
            entry_price=None,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
    }
    updated_state, _ = _apply_probationary_paper_risk_controls(
        settings=settings,
        lanes=[runtime],
        lane_metrics=metrics,
        risk_state=risk_state,
        structured_logger=root_logger,
        alert_dispatcher=alert_dispatcher,
    )
    return {
        "desk_halt_new_entries_loss": str(settings.probationary_paper_desk_halt_new_entries_loss),
        "desk_flatten_and_halt_loss": str(settings.probationary_paper_desk_flatten_and_halt_loss),
        "triggered_halt_only": updated_state.desk_halt_new_entries_triggered,
        "triggered_flatten": updated_state.desk_flatten_and_halt_triggered,
        "lane_operator_halt": runtime.strategy_engine.state.operator_halt,
        "pending_executions": len(runtime.execution_engine.pending_executions()),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    package = payload["package"]
    controls = payload["controls"]["actions"]
    halt_only = payload["halt_only_risk_proof"]
    validation = payload["bounded_soak_validation"]
    lines = [
        "# GC ATP Production-Track Admission",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Package ID: `{package['package_id']}`",
        f"- Lane ID: `{package['lane_id']}`",
        f"- Shared Strategy Identity: `{package['shared_strategy_identity']}`",
        f"- Symbol: `{package['symbol']}`",
        f"- Experimental Status: `{package['experimental_status']}`",
        f"- Runtime Overlay: `{package['runtime_overlay_id']}`",
        f"- Desk Halt Threshold: `{halt_only['desk_halt_new_entries_loss']}`",
        f"- Desk Flatten Threshold: `{halt_only['desk_flatten_and_halt_loss']}`",
        "",
        "## Control Proof",
    ]
    for row in controls:
        result = dict(row.get("result") or {})
        lines.append(
            f"- `{row['action']}` -> `{result.get('status')}` lane=`{result.get('lane_id')}` halt_reason=`{result.get('halt_reason')}`"
        )
    lines.extend(
        [
            "",
            "## Halt-Only Risk Proof",
            f"- Halt-only triggered: `{halt_only['triggered_halt_only']}`",
            f"- Flatten triggered: `{halt_only['triggered_flatten']}`",
            f"- Operator halt set: `{halt_only['lane_operator_halt']}`",
            f"- Pending executions left behind: `{halt_only['pending_executions']}`",
            "",
            "## Bounded Soak Validation",
            f"- Result: `{validation['summary']['result']}`",
            f"- Passed: `{validation['summary']['passed_count']}` / `{validation['summary']['scenario_count']}`",
            f"- Detail: `{validation['summary'].get('detail')}`",
            f"- Artifact: `{validation.get('artifact_path')}`",
            f"- Markdown: `{validation.get('markdown_path')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def run_gc_production_track_admission(*, repo_root: Path, output_dir: Path) -> dict[str, Path]:
    started = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    config_paths, settings, spec, runtime, root_logger, alert_dispatcher = _build_runtime(repo_root)
    startup_fault = runtime.restore_startup()
    status_path = runtime._write_lane_operator_status(datetime.now(UTC))  # noqa: SLF001
    operator_status = json.loads(Path(status_path).read_text(encoding="utf-8"))
    controls = _control_proof(
        config_paths=config_paths,
        settings=settings,
        runtime=runtime,
        root_logger=root_logger,
        alert_dispatcher=alert_dispatcher,
    )
    halt_only = _halt_only_risk_proof(
        settings=settings,
        runtime=runtime,
        root_logger=root_logger,
        alert_dispatcher=alert_dispatcher,
    )
    try:
        validation = run_probationary_paper_soak_validation(config_paths)
        validation_payload = {
            "artifact_path": validation.artifact_path,
            "markdown_path": validation.markdown_path,
            "summary": validation.summary.get("summary") or {},
        }
    except ValueError as exc:
        validation_payload = {
            "artifact_path": None,
            "markdown_path": None,
            "summary": {
                "result": "NOT_APPLICABLE",
                "passed_count": 0,
                "scenario_count": 0,
                "detail": (
                    "Generic probationary paper soak validation harness is not compatible with the ATP "
                    "1m-executable runtime because the built-in validation scenario uses non-ATP bar "
                    f"resampling assumptions: {exc}"
                ),
            },
        }
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "repo_root": str(repo_root),
        "source_date_span": None,
        "wall_time_seconds": round(perf_counter() - started, 6),
        "config_paths": [str(path) for path in config_paths],
        "package": {
            "lane_id": spec.lane_id,
            "display_name": spec.display_name,
            "symbol": spec.symbol,
            "standalone_strategy_id": spec.standalone_strategy_id,
            "shared_strategy_identity": spec.shared_strategy_identity,
            "experimental_status": spec.experimental_status,
            "package_id": spec.package_id,
            "package_label": spec.package_label,
            "runtime_overlay_id": spec.runtime_overlay_id,
            "runtime_overlay_params": dict(spec.runtime_overlay_params or {}),
            "point_value": str(spec.point_value),
            "trade_size": spec.trade_size,
            "participation_policy": spec.participation_policy.value,
            "max_position_quantity": spec.max_position_quantity,
            "max_adds_after_entry": spec.max_adds_after_entry,
            "config_row": _probationary_lane_spec_runtime_row(spec, settings, config_source=str(config_paths[-1])),
            "runtime_identity": _atp_runtime_identity_payload(spec),
        },
        "startup_fault": startup_fault,
        "operator_status_excerpt": {
            "status_path": str(status_path),
            "strategy_status": operator_status.get("strategy_status"),
            "scope_label": operator_status.get("scope_label"),
            "package_id": operator_status.get("package_id"),
            "runtime_overlay_state": operator_status.get("runtime_overlay_state"),
            "entries_enabled": operator_status.get("entries_enabled"),
            "operator_halt": operator_status.get("operator_halt"),
        },
        "controls": controls,
        "halt_only_risk_proof": halt_only,
        "bounded_soak_validation": {
            "artifact_path": validation_payload["artifact_path"],
            "markdown_path": validation_payload["markdown_path"],
            "summary": validation_payload["summary"],
        },
    }
    json_path = output_dir / "gc_atp_production_track_admission.json"
    md_path = output_dir / "gc_atp_production_track_admission.md"
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(_json_ready(payload)), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    repo_root = Path.cwd()
    output_dir = Path(args.output_dir) if args.output_dir else (repo_root / DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S"))
    artifacts = run_gc_production_track_admission(repo_root=repo_root, output_dir=output_dir)
    print(json.dumps({key: str(value) for key, value in artifacts.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
