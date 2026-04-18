from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app import warehouse_platform_review as review
from mgc_v05l.research.platform import SourceSelection, build_discovered_research_analytics_payload
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset, write_storage_manifest
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout


def test_warehouse_platform_review_registers_and_publishes_family(tmp_path: Path, monkeypatch) -> None:
    source_db = tmp_path / "bars.sqlite3"
    source_db.write_text("", encoding="utf-8")
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "reports"
    analytics_root = tmp_path / "analytics" / "warehouse_historical_evaluator"
    registry_root = tmp_path / "registry"
    scope_root = tmp_path / "strategy_scopes"

    monkeypatch.setattr(review, "DEFAULT_ANALYTICS_ROOT", analytics_root)
    monkeypatch.setattr(review, "DEFAULT_REGISTRY_ROOT", registry_root)
    monkeypatch.setattr(review, "DEFAULT_PLATFORM_SCOPE_ROOT", scope_root)

    def _fake_discover_best_sources(**_: object):
        return {
            "GC": {
                "1m": SourceSelection(
                    symbol="GC",
                    timeframe="1m",
                    data_source="historical_1m_canonical",
                    sqlite_path=source_db,
                    row_count=12,
                    start_ts="2024-01-02T00:00:00+00:00",
                    end_ts="2024-01-03T00:00:00+00:00",
                )
            }
        }

    def _fake_last_source_discovery_metadata():
        return {"inventory_cache_hit": True, "selection_count": 1}

    def _fake_run_multi_symbol_warehouse_shard(**kwargs: object):
        root_dir = Path(kwargs["root_dir"])
        shard_id = str(kwargs["shard_id"])
        layout = build_layout(root_dir)
        materialize_parquet_dataset(
            layout["lane_closed_trades"] / "symbol=GC" / "year=2024" / f"shard_id={shard_id}" / "closed_trades.parquet",
            [
                {
                    "trade_id": "gc_lane_1:closed",
                    "entry_id": "entry-1",
                    "candidate_id": "candidate-1",
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "shard_id": shard_id,
                    "side": "LONG",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "entry_ts": "2024-01-02T10:00:00+00:00",
                    "exit_ts": "2024-01-02T10:15:00+00:00",
                    "entry_price": 2000.0,
                    "exit_price": 2001.0,
                    "pnl": 100.0,
                    "hold_minutes": 15,
                    "vwap_quality": "acceptable",
                    "exit_reason": "time_exit",
                    "win_flag": True,
                }
            ],
        )
        materialize_parquet_dataset(
            layout["lane_compact_results"] / "symbol=GC" / "year=2024" / f"shard_id={shard_id}" / "results.parquet",
            [
                {
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "shard_id": shard_id,
                    "artifact_class": "compact_summary",
                    "result_classification": "nonzero_trade",
                    "trade_count": 1,
                    "net_pnl": 100.0,
                    "profit_factor": None,
                    "win_rate": 100.0,
                    "eligibility_status": "eligible_nonzero_trade",
                    "zero_trade_flag": False,
                    "canonical_input_start": "2024-01-02T00:00:00+00:00",
                    "canonical_input_end": "2024-01-03T00:00:00+00:00",
                    "emitted_compact_start": "2024-01-02T10:00:00+00:00",
                    "emitted_compact_end": "2024-01-02T10:15:00+00:00",
                },
                {
                    "lane_id": "gc_lane_2",
                    "strategy_key": "gc_lane_2",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "shard_id": shard_id,
                    "artifact_class": "compact_summary",
                    "result_classification": "zero_trade",
                    "trade_count": 0,
                    "net_pnl": 0.0,
                    "profit_factor": None,
                    "win_rate": None,
                    "eligibility_status": "eligible_no_closed_trades",
                    "zero_trade_flag": True,
                    "canonical_input_start": "2024-01-02T00:00:00+00:00",
                    "canonical_input_end": "2024-01-03T00:00:00+00:00",
                    "emitted_compact_start": "2024-01-02T10:20:00+00:00",
                    "emitted_compact_end": "2024-01-02T10:20:00+00:00",
                },
            ],
        )
        proof_path = layout["manifests"] / "multi_symbol_quarter_proof.json"
        proof_path.parent.mkdir(parents=True, exist_ok=True)
        write_storage_manifest(
            proof_path,
            {"generated_at": datetime.now(UTC).isoformat(), "basket_symbols": ["GC"], "shard_id": shard_id},
        )
        duckdb_path = layout["duckdb"]
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        duckdb_path.write_text("", encoding="utf-8")
        proof_md_path = layout["manifests"] / "multi_symbol_quarter_proof.md"
        proof_md_path.write_text("# proof\n", encoding="utf-8")
        return {
            "proof_path": str(proof_path),
            "proof_markdown_path": str(proof_md_path),
            "duckdb_path": str(duckdb_path),
            "basket_symbols": ["GC"],
            "shard_id": shard_id,
        }

    monkeypatch.setattr(review, "discover_best_sources", _fake_discover_best_sources)
    monkeypatch.setattr(review, "last_source_discovery_metadata", _fake_last_source_discovery_metadata)
    monkeypatch.setattr(review, "run_multi_symbol_warehouse_shard", _fake_run_multi_symbol_warehouse_shard)

    result = review.run_review(
        source_db=source_db,
        output_dir=output_dir,
        warehouse_root=warehouse_root,
        baseline_report_path=tmp_path / "baseline.json",
        start_timestamp=datetime.fromisoformat("2024-01-02T00:00:00+00:00"),
        end_timestamp=datetime.fromisoformat("2024-01-03T00:00:00+00:00"),
        shard_id="2024Q1",
    )

    assert Path(result["json_path"]).exists()
    assert Path(result["analytics_manifest_path"]).exists()
    assert Path(result["platform_analytics_manifest_path"]).exists()

    payload = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
    assert len(payload["targets"]) == 2
    assert payload["warehouse"]["closed_trade_count"] == 1

    combined_payload = build_discovered_research_analytics_payload(analytics_platform_root=analytics_root.parent)
    assert combined_payload["available"] is True
    assert combined_payload["strategy_family_counts"] == {"warehouse_historical_evaluator": 2}
    assert len(combined_payload["strategy_catalog"]) == 2
    assert combined_payload["default_family_views"] == [
        {
            "strategy_family": "warehouse_historical_evaluator",
            "family_label": "Warehouse Historical Evaluator",
            "publication_mode": "cumulative",
            "view_role": "default_app_visible_tenant",
            "time_horizon": {
                "mode": "cumulative",
                "years": ["2024"],
                "shard_ids": ["2024Q1"],
            },
        }
    ]


def test_warehouse_platform_review_supports_cumulative_and_diagnostic_publish_modes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_db = tmp_path / "bars.sqlite3"
    source_db.write_text("", encoding="utf-8")
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "reports"
    analytics_root = tmp_path / "analytics" / "warehouse_historical_evaluator"
    registry_root = tmp_path / "registry"
    scope_root = tmp_path / "strategy_scopes"

    monkeypatch.setattr(review, "DEFAULT_ANALYTICS_ROOT", analytics_root)
    monkeypatch.setattr(review, "DEFAULT_REGISTRY_ROOT", registry_root)
    monkeypatch.setattr(review, "DEFAULT_PLATFORM_SCOPE_ROOT", scope_root)

    def _fake_discover_best_sources(**_: object):
        return {
            "GC": {
                "1m": SourceSelection(
                    symbol="GC",
                    timeframe="1m",
                    data_source="historical_1m_canonical",
                    sqlite_path=source_db,
                    row_count=24,
                    start_ts="2024-01-01T00:00:00+00:00",
                    end_ts="2024-06-30T23:59:00+00:00",
                )
            }
        }

    def _fake_last_source_discovery_metadata():
        return {"inventory_cache_hit": True, "selection_count": 1}

    def _fake_run_multi_symbol_warehouse_shard(**kwargs: object):
        root_dir = Path(kwargs["root_dir"])
        requested_shard_id = str(kwargs["shard_id"])
        layout = build_layout(root_dir)
        materialize_parquet_dataset(
            layout["lane_closed_trades"] / "symbol=GC" / "year=2024" / "shard_id=2024Q1" / "closed_trades.parquet",
            [
                {
                    "trade_id": "gc_lane_1:q1",
                    "entry_id": "entry-q1",
                    "candidate_id": "candidate-q1",
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "shard_id": "2024Q1",
                    "side": "LONG",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "entry_ts": "2024-01-10T10:00:00+00:00",
                    "exit_ts": "2024-01-10T10:15:00+00:00",
                    "entry_price": 2000.0,
                    "exit_price": 2001.0,
                    "pnl": 100.0,
                    "hold_minutes": 15,
                    "vwap_quality": "acceptable",
                    "exit_reason": "time_exit",
                    "win_flag": True,
                }
            ],
        )
        materialize_parquet_dataset(
            layout["lane_closed_trades"] / "symbol=GC" / "year=2024" / "shard_id=2024Q2" / "closed_trades.parquet",
            [
                {
                    "trade_id": "gc_lane_1:q2",
                    "entry_id": "entry-q2",
                    "candidate_id": "candidate-q2",
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "shard_id": "2024Q2",
                    "side": "LONG",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "entry_ts": "2024-04-10T10:00:00+00:00",
                    "exit_ts": "2024-04-10T10:15:00+00:00",
                    "entry_price": 2010.0,
                    "exit_price": 2012.0,
                    "pnl": 200.0,
                    "hold_minutes": 15,
                    "vwap_quality": "acceptable",
                    "exit_reason": "time_exit",
                    "win_flag": True,
                }
            ],
        )
        materialize_parquet_dataset(
            layout["lane_compact_results"] / "symbol=GC" / "year=2024" / "shard_id=2024Q1" / "results.parquet",
            [
                {
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "shard_id": "2024Q1",
                    "artifact_class": "compact_summary",
                    "result_classification": "nonzero_trade",
                    "trade_count": 1,
                    "net_pnl": 100.0,
                    "profit_factor": None,
                    "win_rate": 100.0,
                    "eligibility_status": "eligible_nonzero_trade",
                    "zero_trade_flag": False,
                    "canonical_input_start": "2024-01-01T00:00:00+00:00",
                    "canonical_input_end": "2024-03-31T23:59:00+00:00",
                    "emitted_compact_start": "2024-01-10T10:00:00+00:00",
                    "emitted_compact_end": "2024-01-10T10:15:00+00:00",
                }
            ],
        )
        materialize_parquet_dataset(
            layout["lane_compact_results"] / "symbol=GC" / "year=2024" / "shard_id=2024Q2" / "results.parquet",
            [
                {
                    "lane_id": "gc_lane_1",
                    "strategy_key": "gc_lane_1",
                    "family": "warehouse_breakout",
                    "symbol": "GC",
                    "execution_model": "WAREHOUSE_EXECUTION",
                    "shard_id": "2024Q2",
                    "artifact_class": "compact_summary",
                    "result_classification": "nonzero_trade",
                    "trade_count": 1,
                    "net_pnl": 200.0,
                    "profit_factor": None,
                    "win_rate": 100.0,
                    "eligibility_status": "eligible_nonzero_trade",
                    "zero_trade_flag": False,
                    "canonical_input_start": "2024-04-01T00:00:00+00:00",
                    "canonical_input_end": "2024-06-30T23:59:00+00:00",
                    "emitted_compact_start": "2024-04-10T10:00:00+00:00",
                    "emitted_compact_end": "2024-04-10T10:15:00+00:00",
                }
            ],
        )
        proof_path = layout["manifests"] / "multi_symbol_quarter_proof.json"
        proof_path.parent.mkdir(parents=True, exist_ok=True)
        write_storage_manifest(
            proof_path,
            {"generated_at": datetime.now(UTC).isoformat(), "basket_symbols": ["GC"], "shard_id": requested_shard_id},
        )
        duckdb_path = layout["duckdb"]
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        duckdb_path.write_text("", encoding="utf-8")
        proof_md_path = layout["manifests"] / "multi_symbol_quarter_proof.md"
        proof_md_path.write_text("# proof\n", encoding="utf-8")
        return {
            "proof_path": str(proof_path),
            "proof_markdown_path": str(proof_md_path),
            "duckdb_path": str(duckdb_path),
            "basket_symbols": ["GC"],
            "shard_id": requested_shard_id,
        }

    monkeypatch.setattr(review, "discover_best_sources", _fake_discover_best_sources)
    monkeypatch.setattr(review, "last_source_discovery_metadata", _fake_last_source_discovery_metadata)
    monkeypatch.setattr(review, "run_multi_symbol_warehouse_shard", _fake_run_multi_symbol_warehouse_shard)

    result = review.run_review(
        source_db=source_db,
        output_dir=output_dir,
        warehouse_root=warehouse_root,
        baseline_report_path=tmp_path / "baseline.json",
        start_timestamp=datetime.fromisoformat("2024-04-01T00:00:00+00:00"),
        end_timestamp=datetime.fromisoformat("2024-06-30T23:59:00+00:00"),
        shard_id="2024Q2",
        publish_mode="both",
        scope_root=scope_root,
        registry_root=registry_root,
        analytics_root=analytics_root,
    )

    payload = json.loads(Path(result["json_path"]).read_text(encoding="utf-8"))
    assert payload["publish_mode"] == "both"
    assert len(payload["targets"]) == 1
    assert payload["targets"][0]["summary_metrics"]["trade_count"] == 1
    assert payload["publication_variants"]["cumulative"]["target_count"] == 1
    assert payload["publication_variants"]["diagnostic"]["target_count"] == 1

    combined_payload = build_discovered_research_analytics_payload(analytics_platform_root=analytics_root.parent)
    assert combined_payload["strategy_family_counts"] == {"warehouse_historical_evaluator": 1}
    assert len(combined_payload["strategy_summaries"]) == 1
    assert combined_payload["strategy_summaries"][0]["trade_count"] == 2
    assert combined_payload["families"][0]["default_app_visible"] is True
    assert combined_payload["families"][0]["family_metadata"]["publication_mode"] == "cumulative"

    diagnostic_manifest = json.loads(
        (output_dir / "diagnostic_platform" / "analytics" / "warehouse_historical_evaluator" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert diagnostic_manifest["family_metadata"]["diagnostic_only"] is True
    assert diagnostic_manifest["family_metadata"]["publication_mode"] == "diagnostic"
