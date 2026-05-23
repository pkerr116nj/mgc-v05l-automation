from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app.track_b_paper_lifecycle_adoption import (
    LifecycleAdoptionConfig,
    run_track_b_paper_lifecycle_adoption,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    SubmitIntentOwnershipRecord,
    SubmitIntentOwnershipState,
    append_submit_intent_ownership_record,
    load_unresolved_submit_intent_ownership_records,
)
from mgc_v05l.execution_core.track_b_position_management_manifest import (
    create_or_update_position_management_manifest,
    manifest_path_for_intent,
)


def test_pl_lifecycle_adoption_reconstructs_fill_trade_and_ledger(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    fill_rows = _read_jsonl(repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl")
    trade_rows = _read_jsonl(repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/trades.jsonl")
    assert len(fill_rows) == 1
    assert len(trade_rows) == 1
    assert fill_rows[0]["account_id"] == "DUM882026"
    assert fill_rows[0]["symbol"] == "PL"
    assert fill_rows[0]["local_symbol"] == "PLN6"
    assert fill_rows[0]["con_id"] == 644855286
    assert fill_rows[0]["perm_id"] == 1984099439
    assert fill_rows[0]["client_id"] == 10905
    assert fill_rows[0]["execution_id"] == "0000e1a7.6a06001d.01.01"
    assert fill_rows[0]["broker_cost_basis_adjustment"] == "0.0504"
    assert trade_rows[0]["final_position_status"] == "OPEN_MANAGED"
    assert trade_rows[0]["entry_exec_id"] == "0000e1a7.6a06001d.01.01"

    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    position = live_positions["positions_by_instrument"]["PL-202607"]
    assert position["local_symbol"] == "PLN6"
    assert position["con_id"] == 644855286
    assert position["entry_perm_id"] == 1984099439
    assert result.report["shared_truth_evidence"]["source_authority"] == "execution_core_authority"
    assert result.report["shared_truth_evidence"]["dashboard_projection_consumed"] is False


def test_lifecycle_adoption_refuses_identity_mismatch(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path, broker_local_symbol="PLM6")

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Expected exactly one matching broker position, found 0." in result.report["failures"]
    fills_path = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl"
    assert _read_jsonl(fills_path) == []


def test_lifecycle_adoption_refuses_missing_bridge_evidence(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    bridge_path = repo / "outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us/ibkr_paper_strategy_bridge_report.json"
    bridge_path.unlink()

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Missing bridge report evidence." in result.report["failures"]
    fills_path = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us/fills.jsonl"
    assert _read_jsonl(fills_path) == []


def test_lifecycle_adoption_is_idempotent(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    config = LifecycleAdoptionConfig(
        repo_root=repo,
        order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
        apply=True,
    )

    first = run_track_b_paper_lifecycle_adoption(config=config, now=_now())
    second = run_track_b_paper_lifecycle_adoption(config=config, now=_now())

    assert first.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    assert second.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us"
    assert len(_read_jsonl(lane_dir / "fills.jsonl")) == 1
    assert len(_read_jsonl(lane_dir / "trades.jsonl")) == 1
    assert len(_read_jsonl(repo / "outputs/track_b_execution_core/paper_trade_ledger/track_b_paper_trade_ledger.jsonl")) == 1
    assert second.report["post_adoption"]["fill_written"] is False
    assert second.report["post_adoption"]["trade_written"] is False
    assert second.report["post_adoption"]["compact_ledger_trade_record_written"] is False


def test_gc_leak_test_lifecycle_adoption_synthesizes_intent_and_reconstructs_ledger(tmp_path: Path) -> None:
    repo = _write_gc_leak_test_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_gc_asia_us_production_track_selective_v1",
            symbol="GC",
            local_symbol="GCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="3",
            expected_client_id=11940,
            expected_perm_id=614029068,
            expected_exec_id="0000e1a7.6a0cd6e8.01.01",
            expected_fill_price="4578.5",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track_selective_v1"
    fill_rows = _read_jsonl(lane_dir / "fills.jsonl")
    trade_rows = _read_jsonl(lane_dir / "trades.jsonl")
    assert len(fill_rows) == 1
    assert len(trade_rows) == 1
    assert fill_rows[0]["leak_test"] is True
    assert fill_rows[0]["entry_source"] == "LEAK_TEST_ENTRY"
    assert fill_rows[0]["strategy_id"] == "atp_companion_v1__production_track_gc_asia_us_selective_v1"
    assert fill_rows[0]["broker_order_id"] == "3"
    assert fill_rows[0]["perm_id"] == 614029068
    assert fill_rows[0]["client_id"] == 11940
    assert fill_rows[0]["execution_id"] == "0000e1a7.6a0cd6e8.01.01"
    assert fill_rows[0]["entry_execution_intent"] == "PARTICIPATE_NOW"
    assert fill_rows[0]["entry_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert trade_rows[0]["final_position_status"] == "OPEN_MANAGED"
    assert trade_rows[0]["leak_test"] is True
    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    position = live_positions["positions_by_instrument"]["GC-202606"]
    assert position["local_symbol"] == "GCM6"
    assert position["entry_perm_id"] == 614029068
    assert position["strategy_id"] == "atp_companion_v1__production_track_gc_asia_us_selective_v1"


def test_gc_leak_test_lifecycle_adoption_refuses_exact_identity_mismatch(tmp_path: Path) -> None:
    repo = _write_gc_leak_test_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_gc_asia_us_production_track_selective_v1",
            symbol="GC",
            local_symbol="GCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="3",
            expected_client_id=11940,
            expected_perm_id=614029999,
            expected_exec_id="0000e1a7.6a0cd6e8.01.01",
            expected_fill_price="4578.5",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Bridge latest perm id does not match expected perm_id." in result.report["failures"]
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track_selective_v1"
    assert _read_jsonl(lane_dir / "fills.jsonl") == []


def test_gc_leak_test_unknown_after_submit_adopts_partial_broker_confirmed_fill(tmp_path: Path) -> None:
    repo = _write_gc_leak_test_evidence(tmp_path)
    bridge_path = repo / "outputs/reports/track_b_paper_leak_test/atp_companion_v1_gc_asia_us_production_track_selective_v1/ibkr_paper_strategy_bridge_report.json"
    bridge = _read_json(bridge_path)
    bridge["classification"] = "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW"
    bridge["delegated_result"] = {
        "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
        "report": {
            "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
            "submit_cancel_lifecycle": {
                "status": "manual_confirmation_unavailable",
                "submitted_order_id": 4,
                "manual_confirmation": {"state": "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION"},
                "open_order_after_submit": {"open_order_count": 0, "open_orders": []},
            },
        },
    }
    bridge["intent"]["intent_id"] = "fe71aeca-0141-460a-9fb7-3be1c27be2c5"
    bridge["entry_execution_pricing"]["limit_price"] = 4556.0
    bridge["entry_execution_pricing"]["runtime_last_or_close"] = 4555.9
    _write_json(bridge_path, bridge)
    broker_path = repo / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json"
    broker = _read_json(broker_path)
    broker["positions"][0]["average_cost"] = "455512.52"
    broker["positions"][0]["updated_at"] = "2026-05-15T12:00:34.123456+00:00"
    _write_json(broker_path, broker)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_gc_asia_us_production_track_selective_v1",
            symbol="GC",
            local_symbol="GCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="4",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track_selective_v1"
    fill_rows = _read_jsonl(lane_dir / "fills.jsonl")
    assert len(fill_rows) == 1
    assert fill_rows[0]["broker_order_id"] == "4"
    assert fill_rows[0]["fill_price"] == "4555.1252"
    assert fill_rows[0]["fill_price_source"] == "BROKER_POSITION_AVERAGE_PRICE"
    assert fill_rows[0]["identity_completeness"] == "PARTIAL"
    assert fill_rows[0]["missing_broker_identity_fields"] == ["client_id", "perm_id", "execution_id"]
    assert fill_rows[0]["evidence_classification"] == "LEAK_TEST_BROKER_POSITION_CONFIRMED_PARTIAL_IDENTITY"
    assert fill_rows[0]["leak_test"] is True
    assert fill_rows[0]["review_required"] is False
    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    assert live_positions["positions_by_instrument"]["GC-202606"]["entry_order_id"] == "4"


def test_mgc_leak_test_unknown_after_submit_adopts_exact_order_client_perm_identity(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="28",
            expected_client_id=11940,
            expected_perm_id=614044377,
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    assert result.report["adoption_input_classification"] == "LEAK_TEST_BROKER_BACKED_ENTRY_REQUIRES_LIFECYCLE_ADOPTION"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us"
    fill_rows = _read_jsonl(lane_dir / "fills.jsonl")
    assert len(fill_rows) == 1
    fill = fill_rows[0]
    assert fill["broker_order_id"] == "28"
    assert fill["client_id"] == 11940
    assert fill["perm_id"] == 614044377
    assert fill["con_id"] == 712565978
    assert fill["local_symbol"] == "MGCM6"
    assert fill["fill_price"] == "4543.297"
    assert fill["fill_price_source"] == "BROKER_POSITION_AVERAGE_PRICE"
    assert fill["broker_position_confirmed"] is True
    assert fill["adoption_input_classification"] == "LEAK_TEST_BROKER_BACKED_ENTRY_REQUIRES_LIFECYCLE_ADOPTION"
    assert fill["evidence_classification"] == "LEAK_TEST_BROKER_POSITION_CONFIRMED_PARTIAL_IDENTITY"
    assert fill["identity_completeness"] == "PARTIAL"
    assert fill["missing_broker_identity_fields"] == ["execution_id"]
    assert fill["paper_proof_invoked"] is False
    assert fill["broker_mutated_by_adoption"] is False
    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    position = live_positions["positions_by_instrument"]["MGC-202606"]
    assert position["entry_order_id"] == "28"
    assert position["entry_client_id"] == 11940
    assert position["entry_perm_id"] == 614044377
    assert position["strategy_id"] == "atp_companion_v1__benchmark_mgc_asia_us"


def test_unknown_after_submit_matching_submit_intent_adopts_reserved_lifecycle_id(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)
    bridge_path = repo / "outputs/reports/track_b_paper_leak_test/atp_companion_v1_asia_us/ibkr_paper_strategy_bridge_report.json"
    bridge_path.unlink()
    ownership = _write_mgc_submit_intent_ownership(repo)
    create_or_update_position_management_manifest(
        entry_intent_id=str(ownership["ownership_intent_id"]),
        lane_id="atp_companion_v1_asia_us",
        strategy_id="atp_companion_v1__benchmark_mgc_asia_us",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="INTENT_CREATED",
        output_root=repo / "outputs/track_b_execution_core/position_management_manifests",
        now=_now(),
    )

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="28",
            expected_client_id=11940,
            expected_perm_id=614044377,
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    assert result.report["submit_intent_ownership_evidence"]["selected"]["ownership_intent_id"] == ownership["ownership_intent_id"]
    ownership_resolution = result.report["post_adoption"]["submit_intent_ownership_resolution"]
    assert ownership_resolution["classification"] == "SUBMIT_INTENT_OWNERSHIP_RESOLVED_LIFECYCLE_OPEN_PERSISTED"
    assert ownership_resolution["ownership_intent_id"] == ownership["ownership_intent_id"]
    assert ownership_resolution["state"] == "LIFECYCLE_OPEN_PERSISTED"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us"
    fill_rows = _read_jsonl(lane_dir / "fills.jsonl")
    trade_rows = _read_jsonl(lane_dir / "trades.jsonl")
    assert fill_rows[0]["ownership_intent_id"] == ownership["ownership_intent_id"]
    assert fill_rows[0]["reserved_lifecycle_id"] == ownership["lifecycle_id"]
    assert fill_rows[0]["lifecycle_id"] == ownership["lifecycle_id"]
    assert fill_rows[0]["submit_intent_ownership_evidence"] is True
    assert fill_rows[0]["broker_order_id"] == "28"
    assert fill_rows[0]["client_id"] == 11940
    assert fill_rows[0]["perm_id"] == 614044377
    assert trade_rows[0]["lifecycle_id"] == ownership["lifecycle_id"]
    assert trade_rows[0]["ownership_intent_id"] == ownership["ownership_intent_id"]
    assert trade_rows[0]["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    manifest_path = manifest_path_for_intent(
        str(ownership["ownership_intent_id"]),
        output_root=repo / "outputs/track_b_execution_core/position_management_manifests",
    )
    manifest = _read_json(manifest_path)
    assert manifest["lifecycle_status"] == "OPEN_MANAGED"
    assert manifest["lifecycle_id"] == ownership["lifecycle_id"]
    assert manifest["broker_ownership_identity"]["broker_order_id"] == "28"
    assert manifest["broker_ownership_identity"]["perm_id"] == 614044377
    live_positions = _read_json(repo / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json")
    assert live_positions["open_position_count"] == 1
    ownership_path = repo / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
    unresolved = load_unresolved_submit_intent_ownership_records(ownership_path)
    assert unresolved == []
    latest_ownership = _read_json(repo / "outputs/track_b_execution_core/submit_intent_ownership/latest_track_b_submit_intent_ownership.json")
    assert latest_ownership["unresolved_count"] == 0
    assert latest_ownership["latest_record"]["state"] == "LIFECYCLE_OPEN_PERSISTED"


def test_bridge_report_fallback_still_adopts_without_submit_intent(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="28",
            expected_client_id=11940,
            expected_perm_id=614044377,
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    assert result.report["submit_intent_ownership_evidence"]["selected"] is None
    fill_rows = _read_jsonl(repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us/fills.jsonl")
    assert fill_rows[0].get("ownership_intent_id") is None


def test_lifecycle_adoption_refuses_competing_submit_intent_ownership_records(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)
    _write_mgc_submit_intent_ownership(repo, broker_order_id="28")
    _write_mgc_submit_intent_ownership(repo, broker_order_id="29")

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Competing unresolved submit-intent ownership records" in " ".join(result.report["failures"])


def test_lifecycle_adoption_refuses_mismatched_submit_intent_identity(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)
    _write_mgc_submit_intent_ownership(repo, action="SELL")

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="28",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "account/qty/side/order identity did not match" in " ".join(result.report["failures"])


def test_lifecycle_adoption_refuses_unsafe_submit_intent_ownership_record(tmp_path: Path) -> None:
    repo = _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path)
    record = _write_mgc_submit_intent_ownership(repo)
    path = repo / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
    tampered = dict(record)
    tampered["live_money_eligible"] = True
    path.write_text(json.dumps(tampered, sort_keys=True) + "\n", encoding="utf-8")

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_asia_us",
            symbol="MGC",
            local_symbol="MGCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="28",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "unsafe live_money_eligible/paper_proof flags" in " ".join(result.report["failures"])


def test_lifecycle_adoption_blocks_suspicious_open_order_truth(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": _now().isoformat(), "classification": "SUSPICIOUS_ORDER_STATE", "order_states": []},
    )

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert any("Open Order Truth is not safe for adoption" in failure for failure in result.report["failures"])


def test_lifecycle_adoption_blocks_managed_order_registry_warning(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": _now().isoformat(), "classification": "CLOSE_ORDER_SUSPICIOUS", "managed_orders": []},
    )

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert any("Managed Order Registry is not safe for adoption" in failure for failure in result.report["failures"])


def test_lifecycle_adoption_blocks_runtime_supervisor_manual_review(tmp_path: Path) -> None:
    repo = _write_pl_evidence(tmp_path)
    _write_json(
        repo
        / "outputs"
        / "track_b_execution_core"
        / "runtime_supervisor"
        / "latest_runtime_supervisor_authority.json",
        {"generated_at": _now().isoformat(), "classification": "SUPERVISOR_MANUAL_REVIEW_REQUIRED"},
    )

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert any("Runtime Supervisor Authority blocks" in failure for failure in result.report["failures"])


def test_lifecycle_adoption_does_not_consume_dashboard_projections_as_authority() -> None:
    source = Path("src/mgc_v05l/app/track_b_paper_lifecycle_adoption.py").read_text(encoding="utf-8")
    forbidden_projection_paths = [
        "latest_track_b_open_order_truth.json",
        "latest_track_b_managed_orders.json",
        "latest_track_b_position_truth.json",
        "latest_track_b_managed_positions.json",
        "latest_track_b_runtime_supervisor_authority.json",
    ]

    assert [path for path in forbidden_projection_paths if path in source] == []


def test_lifecycle_adoption_module_has_no_broker_mutation_symbols() -> None:
    source = Path("src/mgc_v05l/app/track_b_paper_lifecycle_adoption.py").read_text(encoding="utf-8")
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source


def test_gc_leak_test_partial_adoption_refuses_order_id_mismatch(tmp_path: Path) -> None:
    repo = _write_gc_leak_test_evidence(tmp_path)
    bridge_path = repo / "outputs/reports/track_b_paper_leak_test/atp_companion_v1_gc_asia_us_production_track_selective_v1/ibkr_paper_strategy_bridge_report.json"
    bridge = _read_json(bridge_path)
    bridge["classification"] = "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW"
    bridge["delegated_result"] = {
        "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
        "report": {
            "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
            "submit_cancel_lifecycle": {
                "status": "manual_confirmation_unavailable",
                "submitted_order_id": 4,
            },
        },
    }
    _write_json(bridge_path, bridge)

    result = run_track_b_paper_lifecycle_adoption(
        config=LifecycleAdoptionConfig(
            repo_root=repo,
            lane_id="atp_companion_v1_gc_asia_us_production_track_selective_v1",
            symbol="GC",
            local_symbol="GCM6",
            expiry="20260626",
            bridge_root=Path("outputs/reports/track_b_paper_leak_test"),
            allow_leak_test_synthetic_intent=True,
            expected_broker_order_id="5",
            apply=True,
        ),
        now=_now(),
    )

    assert result.classification == "TRACK_B_PAPER_LIFECYCLE_ADOPTION_REFUSED"
    assert "Bridge submitted order id does not match expected broker_order_id." in result.report["failures"]
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_gc_asia_us_production_track_selective_v1"
    assert _read_jsonl(lane_dir / "fills.jsonl") == []


def _write_pl_evidence(tmp_path: Path, *, broker_local_symbol: str = "PLN6") -> Path:
    repo = tmp_path
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_pl_asia_us"
    bridge_dir = repo / "outputs/reports/ibkr_runtime_route_dispatch/atp_companion_v1_pl_asia_us"
    broker_dir = repo / "outputs/reports/ibkr_read_only_verification"
    lane_dir.mkdir(parents=True, exist_ok=True)
    bridge_dir.mkdir(parents=True, exist_ok=True)
    broker_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "fills.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "trades.jsonl").write_text("", encoding="utf-8")
    _write_jsonl(
        lane_dir / "order_intents.jsonl",
        [
            {
                "order_intent_id": "PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
                "lane_id": "atp_companion_v1_pl_asia_us",
                "standalone_strategy_id": "atp_companion_v1__paper_pl_asia_us",
                "symbol": "PL",
                "intent_type": "BUY_TO_OPEN",
                "quantity": 1,
                "broker_order_id": "1",
                "broker_order_status": "FILLED",
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                "reason_code": "trend_participation.atp_v1_long_pullback_continuation.long.base",
            }
        ],
    )
    _write_json(
        broker_dir / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-05-13T02:41:07.588344+00:00",
            "mode": "PAPER",
            "positions": [
                {
                    "account_id": "DUM882026",
                    "average_cost": "107257.52",
                    "currency": "USD",
                    "expiry": "20260729",
                    "local_symbol": broker_local_symbol,
                    "multiplier": "50",
                    "quantity": "1.0",
                    "security_type": "FUT",
                    "symbol": "PL",
                    "updated_at": "2026-05-13T02:41:07.581358+00:00",
                }
            ],
        },
    )
    _write_json(
        bridge_dir / "ibkr_paper_strategy_bridge_report.json",
        {
            "classification": "PAPER_STRATEGY_ORDER_FILLED",
            "selected_account_id": "DUM882026",
            "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 10905},
            "strategy_identity": {"strategy_id": "atp_companion_v1_pl_asia_us"},
            "intent": {
                "action": "BUY",
                "strategy_id": "atp_companion_v1_pl_asia_us",
                "symbol": "PL",
                "quantity": 1.0,
                "paper_only": True,
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            },
            "exact_contract_report": {
                "exact_contract": {
                    "broker_symbol": "PL",
                    "con_id": 644855286,
                    "expiry": "20260729",
                    "local_symbol": "PLN6",
                    "multiplier": "50",
                }
            },
            "qualified_contract_report": {
                "qualified_contract": {
                    "broker_symbol": "PL",
                    "con_id": 644855286,
                    "expiry": "20260729",
                    "local_symbol": "PLN6",
                    "multiplier": "50",
                }
            },
            "delegated_result": {
                "classification": "PAPER_ORDER_FILLED",
                "report": {
                    "preview_payload": {
                        "contract": {
                            "symbol": "PL",
                            "expiry": "202607",
                            "local_symbol": "PLN6",
                            "multiplier": "50",
                            "qualified_contract_identifier": 644855286,
                        }
                    },
                    "submit_cancel_lifecycle": {
                        "latest_order_status": {
                            "status": "Filled",
                            "order_id": 1,
                            "perm_id": 1984099439,
                            "client_id": 10905,
                            "filled": 1.0,
                            "avg_fill_price": 2145.1,
                            "updated_at": "2026-05-13T00:57:24.472827+00:00",
                        },
                        "executions_after_submit": [
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.470589+00:00",
                                "execution_id": "0000e1a7.6a06001d.01.01",
                                "price": "2145.1",
                                "quantity": "1.0",
                                "symbol": "PL",
                            },
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.570324+00:00",
                                "execution_id": "0000e1a7.6a04620e.01.01",
                                "price": "4703.3",
                                "quantity": "1.0",
                                "symbol": "GC",
                            },
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "1",
                                "executed_at": "2026-05-13T00:57:24.570513+00:00",
                                "execution_id": "0000e1a7.6a06001d.01.01",
                                "price": "2145.1",
                                "quantity": "1.0",
                                "symbol": "PL",
                            },
                        ],
                    },
                },
            },
        },
    )
    _write_shared_truth_for_adoption(
        repo,
        symbol="PL",
        local_symbol="PLN6",
        con_id=644855286,
        quantity="1",
        order_intent_id="PL|1m|2026-05-13T00:41:00Z|BUY_TO_OPEN",
    )
    return repo


def _write_gc_leak_test_evidence(tmp_path: Path) -> Path:
    repo = tmp_path
    lane_id = "atp_companion_v1_gc_asia_us_production_track_selective_v1"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes" / lane_id
    bridge_dir = repo / "outputs/reports/track_b_paper_leak_test" / lane_id
    broker_dir = repo / "outputs/reports/ibkr_read_only_verification"
    lane_dir.mkdir(parents=True, exist_ok=True)
    bridge_dir.mkdir(parents=True, exist_ok=True)
    broker_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "order_intents.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "fills.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "trades.jsonl").write_text("", encoding="utf-8")
    _write_json(
        broker_dir / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-05-15T06:34:20.588499+00:00",
            "mode": "PAPER",
            "positions": [
                {
                    "account_id": "DUM882026",
                    "average_cost": "457852.52",
                    "currency": "USD",
                    "expiry": "20260626",
                    "local_symbol": "GCM6",
                    "multiplier": "100",
                    "quantity": "1.0",
                    "security_type": "FUT",
                    "symbol": "GC",
                    "updated_at": "2026-05-15T06:34:20.436452+00:00",
                }
            ],
        },
    )
    _write_json(
        bridge_dir / "ibkr_paper_strategy_bridge_report.json",
        {
            "classification": "PAPER_STRATEGY_ORDER_FILLED",
            "selected_account_id": "DUM882026",
            "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 10940},
            "caller_metadata": {
                "leak_test": True,
                "strategy_id": "atp_companion_v1__production_track_gc_asia_us_selective_v1",
                "lane_id": lane_id,
                "authorization_digest": "b6e18cf95416671adfcc8659b56170074ff5e025d77ac148c45ea09c5ed5ef34",
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            },
            "strategy_identity": {"strategy_id": lane_id},
            "intent": {
                "action": "BUY",
                "intent_id": "3c23e0f6-b19f-42e5-9582-28dee7b600b7",
                "strategy_id": lane_id,
                "symbol": "GC",
                "quantity": 1.0,
                "paper_only": True,
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                "reason": "LEAK_TEST_ENTRY",
                "risk_tags": ["TRACK_B_LEAK_TEST", "BUY_TO_OPEN"],
                "timestamp": "2026-05-15T06:29:12.256499+00:00",
            },
            "entry_execution_pricing": {
                "entry_execution_intent": "PARTICIPATE_NOW",
                "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
                "limit_price": 4578.5,
                "runtime_last_or_close": 4578.4,
            },
            "exact_contract_report": {
                "exact_contract": {
                    "broker_symbol": "GC",
                    "con_id": 430360630,
                    "expiry": "20260626",
                    "local_symbol": "GCM6",
                    "multiplier": "100",
                }
            },
            "delegated_result": {
                "classification": "PAPER_ORDER_FILLED",
                "report": {
                    "preview_payload": {
                        "contract": {
                            "symbol": "GC",
                            "expiry": "202606",
                            "local_symbol": "GCM6",
                            "multiplier": "100",
                            "qualified_contract_identifier": 430360630,
                        }
                    },
                    "submit_cancel_lifecycle": {
                        "status": "filled",
                        "submitted_order_id": 3,
                        "latest_order_status": {
                            "status": "Filled",
                            "order_id": 3,
                            "perm_id": 614029068,
                            "client_id": 11940,
                            "filled": 1.0,
                            "avg_fill_price": 4578.5,
                            "updated_at": "2026-05-15T06:29:56.936703+00:00",
                        },
                        "executions_after_submit": [
                            {
                                "account_id": "DUM882026",
                                "broker_order_id": "3",
                                "executed_at": "2026-05-15T06:29:56.936139+00:00",
                                "execution_id": "0000e1a7.6a0cd6e8.01.01",
                                "price": "4578.5",
                                "quantity": "1.0",
                                "symbol": "GC",
                            }
                        ],
                    },
                },
            },
        },
    )
    _write_shared_truth_for_adoption(
        repo,
        symbol="GC",
        local_symbol="GCM6",
        con_id=430360630,
        quantity="1",
        order_intent_id="3c23e0f6-b19f-42e5-9582-28dee7b600b7",
    )
    return repo


def _write_mgc_leak_test_unknown_after_submit_evidence(tmp_path: Path) -> Path:
    repo = tmp_path
    lane_id = "atp_companion_v1_asia_us"
    lane_dir = repo / "outputs/probationary_pattern_engine/paper_session/lanes" / lane_id
    bridge_dir = repo / "outputs/reports/track_b_paper_leak_test" / lane_id
    broker_dir = repo / "outputs/reports/ibkr_read_only_verification"
    lane_dir.mkdir(parents=True, exist_ok=True)
    bridge_dir.mkdir(parents=True, exist_ok=True)
    broker_dir.mkdir(parents=True, exist_ok=True)
    (lane_dir / "order_intents.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "fills.jsonl").write_text("", encoding="utf-8")
    (lane_dir / "trades.jsonl").write_text("", encoding="utf-8")
    _write_json(
        broker_dir / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "generated_at": "2026-05-15T20:51:46.339292+00:00",
            "mode": "PAPER",
            "positions": [
                {
                    "account_id": "DUM882026",
                    "average_cost": "45432.97",
                    "currency": "USD",
                    "expiry": "20260626",
                    "local_symbol": "MGCM6",
                    "multiplier": "10",
                    "quantity": "1.0",
                    "security_type": "FUT",
                    "symbol": "MGC",
                    "updated_at": "2026-05-15T20:51:45.944387+00:00",
                }
            ],
        },
    )
    _write_json(
        bridge_dir / "ibkr_paper_strategy_bridge_report.json",
        {
            "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
            "selected_account_id": "DUM882026",
            "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 10940},
            "caller_metadata": {
                "leak_test": True,
                "strategy_id": "atp_companion_v1__benchmark_mgc_asia_us",
                "lane_id": lane_id,
                "authorization_digest": "80c2b3d4c8193aa7b7d055520e542b9a74454cd19a7d90491e942f77daa466ce",
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            },
            "strategy_identity": {"strategy_id": lane_id},
            "intent": {
                "action": "BUY",
                "intent_id": "39c8aa13-3875-42a8-a027-e31ffb085bc4",
                "strategy_id": lane_id,
                "symbol": "MGC",
                "quantity": 1.0,
                "paper_only": True,
                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                "reason": "LEAK_TEST_ENTRY",
                "risk_tags": ["TRACK_B_LEAK_TEST", "BUY_TO_OPEN"],
                "timestamp": "2026-05-15T20:49:47.393217+00:00",
            },
            "entry_execution_pricing": {
                "entry_execution_intent": "PARTICIPATE_NOW",
                "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
                "limit_price": 4543.2,
                "runtime_last_or_close": 4543.1,
            },
            "exact_contract_report": {
                "exact_contract": {
                    "broker_symbol": "MGC",
                    "con_id": 712565978,
                    "expiry": "20260626",
                    "local_symbol": "MGCM6",
                    "multiplier": "10",
                }
            },
            "qualified_contract_report": {
                "qualified_contract": {
                    "broker_symbol": "MGC",
                    "con_id": 712565978,
                    "expiry": "20260626",
                    "local_symbol": "MGCM6",
                    "multiplier": "10",
                }
            },
            "delegated_result": {
                "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                "report": {
                    "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                    "preview_payload": {
                        "contract": {
                            "symbol": "MGC",
                            "expiry": "202606",
                            "local_symbol": "MGCM6",
                            "multiplier": "10",
                            "qualified_contract_identifier": 712565978,
                        }
                    },
                    "submit_cancel_lifecycle": {
                        "status": "manual_confirmation_unavailable",
                        "submitted_order_id": 28,
                        "client_id": 11940,
                        "open_order_after_submit": {
                            "client_id": 11940,
                            "open_order_count": 1,
                        },
                    },
                },
            },
        },
    )
    _write_shared_truth_for_adoption(
        repo,
        symbol="MGC",
        local_symbol="MGCM6",
        con_id=712565978,
        quantity="1",
        order_intent_id="39c8aa13-3875-42a8-a027-e31ffb085bc4",
    )
    return repo


def _write_shared_truth_for_adoption(
    repo: Path,
    *,
    symbol: str,
    local_symbol: str,
    con_id: int,
    quantity: str,
    order_intent_id: str,
) -> None:
    generated_at = _now().isoformat()
    position_row = {
        "classification": "BROKER_POSITION_REQUIRES_ADOPTION",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": quantity,
        "order_intent_id": order_intent_id,
    }
    managed_position = {
        "classification": "BROKER_BACKED_ADOPTION_REQUIRED",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": quantity,
        "order_intent_id": order_intent_id,
        "attention_required": True,
    }
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": generated_at, "classification": "NO_OPEN_ORDERS", "order_states": []},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": generated_at, "classification": "NO_MANAGED_ORDERS", "managed_orders": []},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": generated_at,
            "classification": "ATTENTION_REQUIRED",
            "summary": {"overall_classification": "ATTENTION_REQUIRED"},
            "position_states": [position_row],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {
            "generated_at": generated_at,
            "classification": "BROKER_BACKED_ADOPTION_REQUIRED",
            "managed_positions": [managed_position],
        },
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {"generated_at": generated_at, "classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME"},
    )
    _write_json(
        repo
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": generated_at, "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT"},
    )
    _write_json(
        repo / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": generated_at, "classification": "ACTIVE"},
    )
    _write_control_plane_snapshot(repo, generated_at=generated_at)


def _write_control_plane_snapshot(repo: Path, *, generated_at: str | None = None) -> None:
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
        {
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": "test-control-plane-snapshot",
            "shared_truth_refresh_generation_id": "test-shared-truth-generation",
            "shared_truth_coherence_status": "COHERENT",
            "generated_at": generated_at or _now().isoformat(),
            "live_money_eligible": False,
            "duplicate_writer_count": 0,
        },
    )


def _write_mgc_submit_intent_ownership(
    repo: Path,
    *,
    broker_order_id: str = "28",
    action: str = "BUY",
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
) -> dict[str, object]:
    record = SubmitIntentOwnershipRecord(
        mode="PAPER",
        account_id="DUM882026",
        lane_id="atp_companion_v1_asia_us",
        strategy_id="atp_companion_v1__benchmark_mgc_asia_us",
        intent_type="BUY_TO_OPEN" if action == "BUY" else "SELL_TO_OPEN",
        action=action,
        symbol="MGC",
        local_symbol="MGCM6",
        expiry="20260626",
        con_id=712565978,
        qty=1,
        order_type="LMT",
        limit_price="4543.2",
        time_in_force="DAY",
        repo_root=str(repo),
        git_head="abc123",
        created_at=_now().replace(microsecond=int(broker_order_id)),
        state=SubmitIntentOwnershipState.BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED,
        lifecycle_id=f"reserved_submit_atp_companion_v1_asia_us_20260515T204947393217Z_test_{broker_order_id}",
        lifecycle_id_reserved_only=True,
        lifecycle_position_open=False,
        caller_path="track_b_paper_leak_test_apply",
        caller_type="track_b_paper_leak_test",
        authorization_path=str(repo / "outputs/reports/track_b_paper_leak_test/atp_companion_v1_asia_us/track_b_paper_leak_test_authorization.json"),
        authorization_digest="80c2b3d4c8193aa7b7d055520e542b9a74454cd19a7d90491e942f77daa466ce",
        execution_price_source="RUNTIME_DATABENTO_1M_CLOSE",
        runtime_reference_price="4543.1",
        pre_submit_reconciliation_classification="PHASE1_BROKER_RECONCILIATION_CLEAR",
        governance_classification="PAPER_STRATEGY_GOVERNANCE_READY",
        exposure_classification="PAPER_EXPOSURE_ENTRY_ALLOWED",
        open_order_count=0,
        unknown_open_order_count=0,
        review_required_count=0,
        live_money_eligible=live_money_eligible,
        paper_proof_invoked=paper_proof_invoked,
        broker_order_id=broker_order_id,
        client_id=11940,
        perm_id=614044377,
        source_artifact_paths=(
            str(repo / "outputs/reports/track_b_paper_leak_test/atp_companion_v1_asia_us/ibkr_paper_strategy_bridge_report.json"),
        ),
        extra={
            "reason": "LEAK_TEST_ENTRY",
            "delegated_classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
            "bridge_classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
        },
    )
    result = append_submit_intent_ownership_record(
        record,
        jsonl_path=repo / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl",
        latest_path=repo / "outputs/track_b_execution_core/submit_intent_ownership/latest_track_b_submit_intent_ownership.json",
    )
    return result.record


def _now() -> datetime:
    return datetime(2026, 5, 13, 3, 0, tzinfo=UTC)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
