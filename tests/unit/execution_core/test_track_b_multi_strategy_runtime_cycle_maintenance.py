from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_managed_open_position_maintenance import (
    TrackBManagedOpenPositionMaintenanceConfig,
    TrackBManagedOpenPositionMaintenanceResult,
)
from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyInput,
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleStages,
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner import TrackBStrategyPaperRunnerResult
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerResult,
    TrackBStrategyRuleRunnerVerdict,
)


def aware_now() -> datetime:
    return datetime(2026, 6, 1, 23, 45, tzinfo=timezone.utc)


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def cycle_config(tmp_path: Path, **overrides: object) -> TrackBMultiStrategyRuntimeCycleConfig:
    payload: dict[str, object] = {
        "inbox_dir": tmp_path / "inbox",
        "output_root": tmp_path / "cycle",
        "strategy_rule_output_root": tmp_path / "rules",
        "strategy_paper_runner_output_root": tmp_path / "paper",
        "repo_root": tmp_path,
        "runtime_generation_id": "runtime-generation-1",
        "submit_paper": True,
        "confirm_paper_submit": True,
        "side": "SELL",
        "quantity": 1,
        "manual_open_limit_price": "30480.0",
        "manual_close_limit_price": "30470.0",
        "control_plane_snapshot_max_age_seconds": 10_000_000,
    }
    payload.update(overrides)
    return TrackBMultiStrategyRuntimeCycleConfig(**payload)


def strategy_result(tmp_path: Path, strategy_input: TrackBMultiStrategyInput) -> TrackBStrategyRuleRunnerResult:
    report = {
        "strategy_rule_runner_verdict": TrackBStrategyRuleRunnerVerdict.NO_SIGNAL.value,
        "strategy_registry_id": strategy_input.strategy_id,
        "strategy_registry_rule_id": strategy_input.rule_id,
        "strategy_registry_rule_mode": strategy_input.rule_mode,
        "strategy_registry_instrument_family": "MNQ",
        "strategy_registry_timeframe": "1m",
        "strategy_registry_feature_version": "test_feature_v1",
        "strategy_registry_calibration_profile": "test_calibration",
        "strategy_registry_paper_eligible": True,
        "strategy_registry_live_money_eligible": False,
        "strategy_id": strategy_input.strategy_id,
        "rule_mode": strategy_input.rule_mode,
        "signal_source": strategy_input.rule_mode,
        "real_strategy_signal": True,
        "decision": "NO_SIGNAL",
        "signal_emitted": False,
        "primary_blocker": None,
        "required_next_action": "strategy next",
    }
    path = _write_json(tmp_path / "rules" / f"{strategy_input.strategy_id}.json", report)
    return TrackBStrategyRuleRunnerResult(
        verdict=TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        report_json=path,
        report={**report, "report_json_path": str(path)},
        downstream_strategy_adapter_report_json=None,
        downstream_candle_producer_report_json=None,
        downstream_signal_batch_writer_report_json=None,
        output_batch_json=None,
    )


def test_runtime_cycle_invokes_managed_open_position_maintenance(tmp_path: Path) -> None:
    calls = {"maintenance": 0, "paper": 0}

    def strategy_stage(
        strategy_input: TrackBMultiStrategyInput,
        _config: TrackBMultiStrategyRuntimeCycleConfig,
    ) -> TrackBStrategyRuleRunnerResult:
        return strategy_result(tmp_path, strategy_input)

    def paper_stage(
        _config: TrackBMultiStrategyRuntimeCycleConfig,
        _strategy_input: TrackBMultiStrategyInput,
        _chosen_signal: dict[str, object],
    ) -> TrackBStrategyPaperRunnerResult:
        calls["paper"] += 1
        raise AssertionError("no entry submit should run for a no-signal cycle")

    def maintenance_stage(
        config: TrackBManagedOpenPositionMaintenanceConfig,
        now: datetime,
    ) -> TrackBManagedOpenPositionMaintenanceResult:
        calls["maintenance"] += 1
        assert config.submit_enabled is True
        report = {
            "schema_version": "track_b_managed_open_position_maintenance_v1",
            "generated_at": now.isoformat(),
            "positions": [
                {
                    "trade_id": "trade-exit-due",
                    "lifecycle_id": "life-exit-due",
                    "final_position_status": "OPEN_MANAGED",
                    "exit_eligible": True,
                    "close_intent_created": True,
                    "close_submitted": True,
                    "close_filled": False,
                    "review_required": False,
                }
            ],
            "close_intent_created_count": 1,
            "close_submitted_count": 1,
            "close_filled_count": 0,
            "review_required_count": 0,
            "broker_state_mutated": True,
            "submit_attempted": True,
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        }
        report_json = _write_json(
            tmp_path / "diagnostics" / "latest_track_b_managed_open_position_maintenance.json",
            report,
        )
        return TrackBManagedOpenPositionMaintenanceResult(
            report_json=report_json,
            report=report,
            lifecycle_results=(),
        )

    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path),
        stages=TrackBMultiStrategyRuntimeCycleStages(
            strategy_rule=strategy_stage,
            paper_runner=paper_stage,
            managed_open_position_maintenance=maintenance_stage,
        ),
        cycle_id="cycle-maintenance",
        now=aware_now(),
    )

    assert calls["maintenance"] == 1
    assert calls["paper"] == 0
    assert result.report["managed_open_position_maintenance_close_intent_created_count"] == 1
    assert result.report["managed_open_position_maintenance_close_submitted_count"] == 1
    assert result.report["managed_open_position_maintenance_submit_attempted"] is True
    assert result.report["managed_open_position_maintenance_broker_state_mutated"] is True
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
