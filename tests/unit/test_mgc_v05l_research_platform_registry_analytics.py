from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.atp_experiment_registry import register_atp_report_output
from mgc_v05l.research.platform import (
    build_discovered_research_analytics_payload,
    build_multi_research_analytics_payload,
    build_registry_run_row,
    build_research_analytics_payload,
    build_research_analytics_views,
    discover_research_analytics_roots,
    ensure_trade_scope_bundle,
    query_daily_pnl,
    read_research_analytics_dataset,
    register_experiment_run,
    shared_research_analytics_contract,
    write_dataset_bundle,
    write_json_manifest,
)


def _fake_scope_bundle(tmp_path: Path) -> Path:
    bundle_dir = tmp_path / "scope_bundles" / "scope123"
    dataset = write_dataset_bundle(
        bundle_dir=bundle_dir / "datasets",
        dataset_name="trade_records",
        rows=[
            {
                "instrument": "GC",
                "variant_id": "atp.variant",
                "decision_id": "a",
                "entry_ts": "2024-01-02T10:00:00+00:00",
                "exit_ts": "2024-01-02T10:15:00+00:00",
                "pnl_cash": 150.0,
                "gross_pnl_cash": 160.0,
                "fees_paid": 5.0,
                "slippage_cost": 5.0,
                "exit_reason": "target",
                "session_segment": "ASIA",
                "mfe_points": 2.0,
                "mae_points": 0.5,
                "bars_held_1m": 15,
                "hold_minutes": 15.0,
                "entry_price": 2000.0,
                "exit_price": 2001.5,
                "regime_bucket": "trend",
                "volatility_bucket": "normal",
            },
            {
                "instrument": "GC",
                "variant_id": "atp.variant",
                "decision_id": "b",
                "entry_ts": "2024-01-03T10:00:00+00:00",
                "exit_ts": "2024-01-03T10:10:00+00:00",
                "pnl_cash": -50.0,
                "gross_pnl_cash": -40.0,
                "fees_paid": 5.0,
                "slippage_cost": 5.0,
                "exit_reason": "stop",
                "session_segment": "US",
                "mfe_points": 0.25,
                "mae_points": 1.0,
                "bars_held_1m": 10,
                "hold_minutes": 10.0,
                "entry_price": 2005.0,
                "exit_price": 2004.5,
                "regime_bucket": "trend",
                "volatility_bucket": "normal",
            },
        ],
    )
    manifest_path = bundle_dir / "manifest.json"
    write_json_manifest(
        manifest_path,
        {
            "bundle_id": "scope123",
            "datasets": {
                "trade_records": dataset,
            },
        },
    )
    return manifest_path


def test_registry_and_analytics_publish_app_ready_views(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    analytics_root = tmp_path / "analytics"
    scope_manifest_path = _fake_scope_bundle(tmp_path)

    run_row = build_registry_run_row(
        strategy_family="atp_companion",
        strategy_variant="full_history_review",
        date_span={"start_timestamp": "2024-01-01T00:00:00+00:00", "end_timestamp": "2024-01-04T00:00:00+00:00"},
        feature_version="feature_v1",
        candidate_version="candidate_v1",
        outcome_engine_version="outcome_v1",
        config_hash="cfg123",
        target_hash="tgt123",
    )
    register_experiment_run(
        registry_root=registry_root,
        run_row=run_row,
        target_rows=[
            {
                "strategy_family": "atp_companion",
                "strategy_variant": "atp_companion_v1__candidate_gc_asia_us",
                "target_id": "atp_companion_v1__candidate_gc_asia_us",
                "symbol": "GC",
                "allowed_sessions": ["ASIA", "US"],
                "scope_bundle_id": "scope123",
                "scope_bundle_manifest_path": str(scope_manifest_path),
                "analytics_publish": True,
                "record_kind": "strategy_scope",
                "summary_metrics": {"net_pnl_cash": 100.0, "trade_count": 2},
            },
            {
                "strategy_family": "atp_companion",
                "strategy_variant": "atp_companion_v1__candidate_gc_asia_us::experiment_overlay",
                "target_id": "atp_companion_v1__candidate_gc_asia_us",
                "symbol": "GC",
                "allowed_sessions": ["ASIA", "US"],
                "analytics_publish": False,
                "record_kind": "experiment_row",
                "summary_metrics": {"net_pnl_cash": 999.0, "trade_count": 99},
            }
        ],
    )

    result = build_research_analytics_views(
        registry_root=registry_root,
        analytics_root=analytics_root,
        strategy_family="atp_companion",
        family_metadata={
            "default_app_visible": True,
            "publication_mode": "cumulative",
            "view_role": "default_app_visible_tenant",
        },
    )

    payload = build_research_analytics_payload(analytics_root=analytics_root)
    assert payload["available"] is True
    assert payload["contract_version"] == "research_analytics_contract_v2"
    assert payload["shared_contract"]["contract_version"] == "research_analytics_contract_v2"
    assert payload["datasets"]["daily_pnl"]["row_count"] == 2
    assert payload["datasets"]["trade_blotter"]["row_count"] == 2
    assert payload["api_base_path"] == "/api/research-analytics"
    assert Path(result["manifest_path"]).exists()
    assert Path(result["platform_manifest_path"]).exists()
    assert len(read_research_analytics_dataset(analytics_root=analytics_root, dataset_name="daily_pnl")) == 2
    discovered_roots = discover_research_analytics_roots(analytics_platform_root=analytics_root.parent)
    assert discovered_roots == {"atp_companion": analytics_root.resolve()}

    daily = query_daily_pnl(
        analytics_root=analytics_root,
        strategy_ids=["atp_companion_v1__candidate_gc_asia_us"],
        combine=False,
    )
    combined = query_daily_pnl(
        analytics_root=analytics_root,
        strategy_ids=["atp_companion_v1__candidate_gc_asia_us"],
        combine=True,
    )
    assert len(daily) == 2
    assert sum(float(row["net_pnl_day"]) for row in daily) == 100.0
    assert len(combined) == 2
    assert daily[0]["strategy_key"] == "atp_companion::atp_companion_v1__candidate_gc_asia_us"
    platform_manifest = json.loads(Path(result["platform_manifest_path"]).read_text(encoding="utf-8"))
    assert platform_manifest["default_family_views"] == [
        {
            "strategy_family": "atp_companion",
            "family_label": "Atp Companion",
            "publication_mode": "cumulative",
            "view_role": "default_app_visible_tenant",
            "time_horizon": None,
        }
    ]


def test_register_atp_report_output_registers_experiment_rows(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    payload_path = tmp_path / "atp_report.json"
    payload_path.write_text(
        json.dumps(
            {
                "study": "ATP smoke",
                "generated_at": "2026-04-07T00:00:00+00:00",
                "manifest": {
                    "feature_version": "feature_v1",
                    "candidate_version": "candidate_v1",
                    "target_hashes": {"target_a": "hash_a"},
                    "source_date_span": {
                        "start_timestamp": "2024-01-01T00:00:00+00:00",
                        "end_timestamp": "2024-01-31T00:00:00+00:00",
                    },
                },
                "results": [
                    {
                        "target_id": "target_a",
                        "label": "Target A",
                        "control_id": "control_x",
                        "symbol": "GC",
                        "allowed_sessions": ["ASIA", "US"],
                        "config": {"config_hash": "cfg_x"},
                        "metrics": {"net_pnl_cash": 12.5, "trade_count": 3},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = register_atp_report_output(
        strategy_variant="us_fast_fail_review",
        payload_json_path=payload_path,
        artifacts={"json_path": payload_path, "source_dir": tmp_path},
        registry_root=registry_root,
    )

    registry = json.loads((registry_root / "manifest.json").read_text(encoding="utf-8"))
    assert result["run_id"] == registry["latest_run_id"]
    target_rows = [
        json.loads(line)
        for line in (registry_root / "targets.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(target_rows) == 1
    assert target_rows[0]["strategy_variant"] == "us_fast_fail_review::target_a::control_x"
    assert target_rows[0]["record_kind"] == "experiment_row"
    assert target_rows[0]["analytics_publish"] is False
    assert target_rows[0]["config_hash"] == "cfg_x"


def test_generic_trade_scope_bundle_writes_manifest_and_trade_records(tmp_path: Path) -> None:
    bundle = ensure_trade_scope_bundle(
        bundle_root=tmp_path / "scopes",
        strategy_family="approved_quant",
        strategy_variant="lane_a::GC",
        symbol="GC",
        selected_sources={"1m": {"data_source": "historical_1m_canonical"}},
        start_timestamp="2024-01-01T00:00:00+00:00",
        end_timestamp="2024-01-02T00:00:00+00:00",
        allowed_sessions=("US",),
        execution_model="APPROVED_QUANT_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        trade_records=[
            {
                "instrument": "GC",
                "variant_id": "lane_a::GC",
                "decision_id": "t1",
                "entry_ts": "2024-01-01T10:00:00+00:00",
                "exit_ts": "2024-01-01T10:15:00+00:00",
                "pnl_cash": 100.0,
                "gross_pnl_cash": 100.0,
                "fees_paid": 0.0,
                "slippage_cost": 0.0,
                "exit_reason": "time_exit",
                "session_segment": "US",
            }
        ],
        metadata={"lane_id": "lane_a"},
    )

    assert bundle.manifest_path.exists()
    assert len(bundle.trade_records) == 1
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert manifest["bundle_type"] == "generic_trade_scope_bundle"
    assert manifest["strategy_family"] == "approved_quant"
    assert manifest["datasets"]["trade_records"]["row_count"] == 1


def test_multi_family_research_analytics_payload_merges_family_roots(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    atp_root = tmp_path / "analytics" / "atp_companion"
    approved_root = tmp_path / "analytics" / "approved_quant"
    scope_manifest_path = _fake_scope_bundle(tmp_path)

    run_row = build_registry_run_row(
        strategy_family="atp_companion",
        strategy_variant="full_history_review",
        date_span={"start_timestamp": "2024-01-01T00:00:00+00:00", "end_timestamp": "2024-01-04T00:00:00+00:00"},
        feature_version="feature_v1",
        candidate_version="candidate_v1",
        outcome_engine_version="outcome_v1",
        config_hash="cfg123",
        target_hash="tgt123",
    )
    register_experiment_run(
        registry_root=registry_root,
        run_row=run_row,
        target_rows=[
            {
                "strategy_family": "atp_companion",
                "strategy_variant": "atp_companion_v1__candidate_gc_asia_us",
                "target_id": "atp_companion_v1__candidate_gc_asia_us",
                "label": "ATP GC Asia+US",
                "symbol": "GC",
                "allowed_sessions": ["ASIA", "US"],
                "scope_bundle_id": "scope123",
                "scope_bundle_manifest_path": str(scope_manifest_path),
                "analytics_publish": True,
                "record_kind": "strategy_scope",
            },
            {
                "strategy_family": "approved_quant",
                "strategy_variant": "phase2c.breakout.metals_only.us_unknown.baseline::GC",
                "target_id": "phase2c.breakout.metals_only.us_unknown.baseline::GC",
                "label": "Approved Quant Breakout / GC",
                "symbol": "GC",
                "allowed_sessions": ["US", "UNKNOWN"],
                "scope_bundle_id": "scope123",
                "scope_bundle_manifest_path": str(scope_manifest_path),
                "analytics_publish": True,
                "record_kind": "strategy_scope",
            },
        ],
    )
    build_research_analytics_views(
        registry_root=registry_root,
        analytics_root=atp_root,
        strategy_family="atp_companion",
    )
    build_research_analytics_views(
        registry_root=registry_root,
        analytics_root=approved_root,
        strategy_family="approved_quant",
    )

    combined_payload = build_multi_research_analytics_payload(
        analytics_roots_by_family={
            "atp_companion": atp_root,
            "approved_quant": approved_root,
        }
    )
    assert combined_payload["available"] is True
    assert combined_payload["strategy_family_counts"] == {"approved_quant": 1, "atp_companion": 1}
    assert len(combined_payload["families"]) == 2
    assert all(row["tenant_class"] == "full_app_tenant" for row in combined_payload["families"])

    discovered_payload = build_discovered_research_analytics_payload(analytics_platform_root=tmp_path / "analytics")
    assert discovered_payload["available"] is True
    assert discovered_payload["strategy_family_counts"] == {"approved_quant": 1, "atp_companion": 1}
    discovered_roots = discover_research_analytics_roots(analytics_platform_root=tmp_path / "analytics")
    assert discovered_roots == {
        "approved_quant": approved_root.resolve(),
        "atp_companion": atp_root.resolve(),
    }

    combined_daily = read_research_analytics_dataset(
        analytics_platform_root=tmp_path / "analytics",
        dataset_name="daily_pnl",
    )
    assert len(combined_daily) == 4
    assert {
        row["strategy_key"]
        for row in combined_daily
    } == {
        "atp_companion::atp_companion_v1__candidate_gc_asia_us",
        "approved_quant::phase2c.breakout.metals_only.us_unknown.baseline::GC",
    }

    approved_only = read_research_analytics_dataset(
        analytics_platform_root=tmp_path / "analytics",
        dataset_name="daily_pnl",
        strategy_families=["approved_quant"],
    )
    assert len(approved_only) == 2
    assert all(row["strategy_family"] == "approved_quant" for row in approved_only)


def test_multi_family_research_analytics_payload_handles_empty_roots() -> None:
    combined_payload = build_multi_research_analytics_payload(
        analytics_roots_by_family={
            "atp_companion": Path("/tmp/does-not-exist-atp"),
            "approved_quant": Path("/tmp/does-not-exist-approved"),
        }
    )

    assert combined_payload["available"] is False
    assert combined_payload["reason"] == "research analytics not materialized"


def test_shared_research_analytics_contract_declares_full_app_requirements() -> None:
    contract = shared_research_analytics_contract()

    assert contract["contract_version"] == "research_analytics_contract_v2"
    assert "strategy_key" in contract["identity_fields"]["required"]
    assert "daily_pnl" in contract["tenant_classes"]["full_app_tenant"]["required_datasets"]
    assert "default_app_visible" in contract["family_metadata_fields"]["optional"]
