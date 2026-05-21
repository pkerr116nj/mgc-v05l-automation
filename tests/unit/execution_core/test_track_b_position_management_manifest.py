from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_management_manifest import (
    OPEN_MANAGED_METADATA_INCOMPLETE,
    create_or_update_position_management_manifest,
    lifecycle_metadata_complete,
    manifest_path_for_intent,
    resolve_management_metadata,
    update_manifest_from_filled_bridge_result,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 21, 15, 7, tzinfo=timezone.utc)


def test_manifest_creation_before_submit_contains_management_contract(tmp_path: Path) -> None:
    result = create_or_update_position_management_manifest(
        entry_intent_id="MNQ|1m|2026-05-21T15:07:00Z|BUY_TO_OPEN",
        lane_id="mnq_1x_ny_early_core__us_midday_long",
        strategy_id="index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_midday_long",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="INTENT_CREATED",
        runtime_instance_id="runtime-1",
        restart_generation=3,
        source_commit="abc123",
        config_fingerprint="cfg",
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result.manifest_path == manifest_path_for_intent(
        "MNQ|1m|2026-05-21T15:07:00Z|BUY_TO_OPEN",
        output_root=tmp_path,
    )
    assert result.manifest["runtime_instance_id"] == "runtime-1"
    assert result.manifest["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert json.loads(result.manifest_path.read_text(encoding="utf-8"))["lifecycle_status"] == "INTENT_CREATED"


def test_fill_updates_manifest_to_open_managed_with_broker_identity(tmp_path: Path) -> None:
    intent_id = "MNQ|1m|2026-05-21T15:07:00Z|BUY_TO_OPEN"
    create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id="lane",
        strategy_id="strategy",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="INTENT_CREATED",
        output_root=tmp_path,
        now=aware_now(),
    )

    result = update_manifest_from_filled_bridge_result(
        filled_bridge_result={
            "order_intent_id": intent_id,
            "lane_id": "lane",
            "strategy_id": "strategy",
            "instrument": "MNQ",
            "action": "BUY",
            "quantity": 1,
            "broker_order_id": "1",
            "perm_id": 1948367784,
            "local_symbol": "MNQM6",
            "con_id": 770561201,
        },
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["lifecycle_status"] == "OPEN_MANAGED"
    assert result.manifest["broker_ownership_identity"]["broker_order_id"] == "1"


def test_metadata_recovers_from_lane_registry_when_manifest_missing(tmp_path: Path) -> None:
    registry = tmp_path / "paper_config_in_force.json"
    registry.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mnq_1x_ny_early_core__us_midday_long",
                        "standalone_strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_midday_long",
                        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = resolve_management_metadata(
        source={"lane_id": "mnq_1x_ny_early_core__us_midday_long"},
        output_root=tmp_path / "manifests",
        lane_registry_paths=(registry,),
    )

    assert result.complete is True
    assert result.source == "lane_registry"
    assert result.managed_exit_policy_id == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


def test_incomplete_open_managed_metadata_is_classified() -> None:
    result = lifecycle_metadata_complete(
        {
            "lane_id": "lane",
            "strategy_id": "strategy",
            "entry_intent_id": "intent",
            "side": "LONG",
            "quantity": 1,
            "contract_key": "MNQ-202606",
        }
    )

    assert result.classification == OPEN_MANAGED_METADATA_INCOMPLETE
    assert "managed_exit_policy_id" in result.blockers
