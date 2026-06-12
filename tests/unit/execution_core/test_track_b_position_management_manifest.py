from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution_core.track_b_position_management_manifest import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    broker_backed_fill_evidence_complete,
    create_manifest_from_order_intent,
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
            "fill_price": "29150.0",
            "fill_timestamp": "2026-05-21T15:08:57.079226+00:00",
        },
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["lifecycle_status"] == "OPEN_MANAGED"
    assert result.manifest["lifecycle_id"] == f"bridge_fill_{intent_id}"
    assert result.manifest["broker_ownership_identity"]["broker_order_id"] == "1"


def test_no_broker_effect_manifest_does_not_enter_open_managed(tmp_path: Path) -> None:
    intent_id = "MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN"
    create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id="track_b_paper_execution_test_mule_v1__mgc",
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
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
            "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
            "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
            "instrument": "MGC",
            "action": "BUY",
            "quantity": 1,
            "submit_sent": False,
            "broker_effect_classification": "PRE_SUBMIT_BLOCKED_NO_BROKER_EFFECT",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        },
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["lifecycle_status"] == BLOCKED_NO_BROKER_EFFECT
    assert result.manifest["broker_ownership_identity"]["broker_order_id"] is None


def test_open_managed_requires_broker_fill_identity(tmp_path: Path) -> None:
    result = create_or_update_position_management_manifest(
        entry_intent_id="MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN",
        lane_id="track_b_paper_execution_test_mule_v1__mgc",
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="OPEN_MANAGED",
        broker_ownership_identity={"broker_order_id": "1", "perm_id": 1948412706},
        lifecycle_id="bridge_fill_MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN",
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result.manifest["lifecycle_status"] == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE
    assert result.manifest["lifecycle_status_blockers"] == ["fill_price", "fill_timestamp"]
    evidence = broker_backed_fill_evidence_complete(result.manifest["broker_ownership_identity"])
    assert evidence.classification == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE


def test_fill_update_persists_lifecycle_and_fill_identity(tmp_path: Path) -> None:
    intent_id = "MGC|1m|2026-05-21T19:46:00Z|BUY_TO_OPEN"
    create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id="track_b_paper_execution_test_mule_v1__mgc",
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
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
            "lifecycle_id": f"bridge_fill_{intent_id}",
            "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
            "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
            "instrument": "MGC",
            "action": "BUY",
            "quantity": 1,
            "broker_order_id": "20",
            "perm_id": 1948384228,
            "client_id": 17086,
            "exec_id": "exec-1",
            "account_id": "DUM882026",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "fill_price": "4543",
            "fill_timestamp": "2026-05-21T20:21:35.305374+00:00",
        },
        output_root=tmp_path,
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["lifecycle_status"] == "OPEN_MANAGED"
    assert result.manifest["lifecycle_id"] == f"bridge_fill_{intent_id}"
    assert result.manifest["broker_ownership_identity"] == {
        "account_id": "DUM882026",
        "broker_order_id": "20",
        "client_id": 17086,
        "exec_id": "exec-1",
        "fill_price": "4543",
        "fill_timestamp": "2026-05-21T20:21:35.305374+00:00",
        "perm_id": 1948384228,
    }


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


def test_entry_manifest_resolves_active_participation_policy_from_lane_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mnq_globex_active_participation_short",
                        "standalone_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
                        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    intent = OrderIntent(
        order_intent_id="MNQ|1m|2026-06-10T23:29:00Z|SELL_TO_OPEN",
        bar_id="MNQ|1m|2026-06-10T23:29:00Z",
        symbol="MNQ",
        intent_type=OrderIntentType.SELL_TO_OPEN,
        quantity=1,
        created_at=aware_now(),
        reason_code="firstBearSnapTurn",
    )

    result = create_manifest_from_order_intent(
        order_intent=intent,
        runtime_identity={
            "lane_id": "mnq_globex_active_participation_short",
            "standalone_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
            "instrument": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
        },
        output_root=tmp_path / "manifests",
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert result.manifest["lifecycle_status"] == "INTENT_CREATED"
    assert result.manifest["policy_config_refs"]["source"] == "management_metadata_resolution"


def test_entry_manifest_resolves_active_evidence_policy_from_runtime_lane_family(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": "mes_globex_active_participation_short",
                        "source_family": "paper_active_evidence",
                        "strategy_family": "paper_active_evidence",
                        "lane_mode": "PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
                        "runtime_overlay_params": {
                            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
                            "rule_id": "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
                        },
                        "short_sources": ["PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    intent = OrderIntent(
        order_intent_id="MES|1m|2026-06-10T23:29:00Z|SELL_TO_OPEN",
        bar_id="MES|1m|2026-06-10T23:29:00Z",
        symbol="MES",
        intent_type=OrderIntentType.SELL_TO_OPEN,
        quantity=1,
        created_at=aware_now(),
        reason_code="firstBearSnapTurn",
    )

    result = create_manifest_from_order_intent(
        order_intent=intent,
        runtime_identity={
            "lane_id": "mes_globex_active_participation_short",
            "standalone_strategy_id": "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
            "instrument": "MES",
            "local_symbol": "MESM6",
            "con_id": 770561194,
        },
        output_root=tmp_path / "manifests",
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert result.manifest["policy_config_refs"]["metadata_resolution_source"] == "lane_registry"


def test_entry_manifest_without_configured_policy_remains_incomplete(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    intent = OrderIntent(
        order_intent_id="MNQ|1m|2026-06-10T23:29:00Z|SELL_TO_OPEN",
        bar_id="MNQ|1m|2026-06-10T23:29:00Z",
        symbol="MNQ",
        intent_type=OrderIntentType.SELL_TO_OPEN,
        quantity=1,
        created_at=aware_now(),
        reason_code="firstBearSnapTurn",
    )

    result = create_manifest_from_order_intent(
        order_intent=intent,
        runtime_identity={
            "lane_id": "unconfigured_lane",
            "standalone_strategy_id": "UNCONFIGURED_STRATEGY",
            "instrument": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
        },
        output_root=tmp_path / "manifests",
        now=aware_now(),
    )

    assert result is not None
    assert result.manifest["managed_exit_policy_id"] is None
    assert result.manifest["lifecycle_status"] == "INTENT_CREATED"
    assert result.manifest["policy_config_refs"]["metadata_resolution_classification"] == OPEN_MANAGED_METADATA_INCOMPLETE
    assert "managed_exit_policy_id" in result.manifest["policy_config_refs"]["metadata_resolution_blockers"]


def test_metadata_recovers_test_mule_policy_without_registry() -> None:
    result = resolve_management_metadata(
        source={
            "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
            "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
            "order_intent_id": "intent-1",
            "side": "LONG",
            "quantity": 1,
            "local_symbol": "MGCM6",
        },
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
