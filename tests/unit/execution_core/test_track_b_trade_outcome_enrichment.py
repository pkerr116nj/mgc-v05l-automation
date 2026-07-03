from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    VALIDITY_OUTSIDE_PROVIDER_WINDOW,
    VALIDITY_STALE,
    VALIDITY_UNAVAILABLE,
    VALIDITY_VALID,
    build_trade_outcome_enrichment_summary,
    build_context_validity,
    build_trade_outcome_enrichments,
    run_trade_outcome_enrichment,
)


NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


def test_enrichment_joins_crfd_vwap_and_avwap() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6")],
        crfd_rows=[_crfd("GC", vwap_relation="above_vwap", avwap_relation="above_avwap")],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["crfd_join_success"] is True
    assert row["vwap_relation"] == "above_vwap"
    assert row["avwap_relation"] == "above_avwap"
    assert "missing_crfd_context" not in row["data_quality_flags"]


def test_enrichment_joins_nearest_prior_market_context() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T10:05:00Z")],
        crfd_rows=[_crfd("GC")],
        market_context_rows=[
            _vix_context("2026-06-30T20:15:00Z", level=16.45),
            _vix_context("2026-07-01T20:15:00Z", level=16.59),
        ],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["market_context_join_success"] is True
    assert row["vix_level"] == 16.45
    assert row["vix_regime"] == "LOW"
    assert row["vix_daily_change"] == -1.2
    assert row["vix_percentile"] == 0.31
    assert row["vix_ma20"] == 18.1
    assert row["vix_ma50"] == 17.6
    assert row["market_context_provider"] == "VIX"
    assert row["market_context_source"] == "cboe_official_daily_history"
    assert row["market_context_timestamp"] == "2026-06-30T20:15:00+00:00"
    assert row["market_context_staleness"] == 49800.0
    assert row["market_context_freshness_window_seconds"] == 604800
    assert row["market_context_validity_classification"] == VALIDITY_VALID
    assert "missing_market_context" not in row["data_quality_flags"]
    assert row["source_refs"]["market_context_rows"] == ""


def test_enrichment_joins_nearest_prior_historical_gre() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T10:05:00Z")],
        historical_gre_rows=[
            _historical_gre("2026-07-01T09:55:00Z", label="LONG", confidence=71),
            _historical_gre("2026-07-01T10:10:00Z", label="SHORT", confidence=80),
        ],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["gre_label"] == "LONG"
    assert row["gre_confidence"] == 71
    assert row["gre_provenance"] == "historical_gre_backfill"
    assert row["gre_provider"] == "historical_databento_parquet"
    assert row["gre_provider_version"] == "track_b_gre_validation_row_v1"
    assert row["gre_timestamp"] == "2026-07-01T09:55:00+00:00"
    assert row["gre_join_method"] == "nearest_prior_historical_gre_observation"
    assert row["gre_staleness"] == 600.0
    assert row["gre_freshness_window_seconds"] == 3600
    assert row["gre_validity_classification"] == VALIDITY_VALID
    assert row["gre_context_validity"]["provenance"] == "historical_gre_backfill"
    assert row["gre_research_readiness"] == "VALIDATED"
    assert row["gre_source_refs"] == {"gre_report": "BACKFILL_GENERATED_GRE_OBSERVATION"}
    assert "missing_gre_context" not in row["data_quality_flags"]


def test_historical_gre_join_does_not_use_future_observation() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T10:05:00Z")],
        historical_gre_rows=[_historical_gre("2026-07-01T10:10:00Z", label="SHORT", confidence=80)],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["gre_label"] is None
    assert row["gre_timestamp"] is None
    assert row["gre_validity_classification"] == VALIDITY_UNAVAILABLE
    assert "missing_gre_context" in row["data_quality_flags"]


def test_missing_historical_gre_is_null_and_flagged() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6")],
        historical_gre_rows=[],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["gre_label"] is None
    assert row["gre_confidence"] is None
    assert row["gre_provider"] is None
    assert row["gre_timestamp"] is None
    assert row["gre_staleness"] is None
    assert "missing_gre_context" in row["data_quality_flags"]


def test_empty_historical_gre_dataset_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    outcomes.write_text(json_line(_outcome("GC", "GCQ6")) + "\n", encoding="utf-8")
    historical_gre = tmp_path / "gre.jsonl"
    historical_gre.write_text("", encoding="utf-8")

    result = run_trade_outcome_enrichment(
        outcomes_path=outcomes,
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        gre_report_path=tmp_path / "missing_gre.json",
        historical_gre_rows_path=historical_gre,
        market_context_rows_path=tmp_path / "missing_context.jsonl",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.summary["coverage_counts"]["historical_gre"] == 0
    assert result.summary["historical_gre"]["failed_joins"] == 1
    assert result.historical_gre_report_path.exists()
    assert result.historical_gre_join_quality_path.exists()
    assert result.historical_gre_data_quality_path.exists()


def test_market_context_join_does_not_use_future_observation() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T10:05:00Z")],
        market_context_rows=[_vix_context("2026-07-01T20:15:00Z", level=16.59)],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["market_context_join_success"] is False
    assert row["vix_level"] is None
    assert "missing_market_context" in row["data_quality_flags"]


def test_missing_market_context_is_null_and_flagged() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6")],
        crfd_rows=[_crfd("GC")],
        market_context_rows=[],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["market_context_join_success"] is False
    assert row["vix_level"] is None
    assert row["vix_regime"] is None
    assert row["market_context_source"] is None
    assert row["market_context_provider"] is None
    assert row["market_context_timestamp"] is None
    assert row["market_context_staleness"] is None
    assert row["market_context_validity_classification"] == VALIDITY_UNAVAILABLE
    assert "missing_market_context" in row["data_quality_flags"]
    assert "missing_vix_context" in row["data_quality_flags"]


def test_missing_gre_context_is_flagged() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6")],
        crfd_rows=[_crfd("GC")],
        gre_report={},
        generated_at=NOW,
    )

    assert enrichments[0]["gre_label"] is None
    assert "missing_gre_context" in enrichments[0]["data_quality_flags"]


def test_time_aligned_gre_report_can_enrich_gold() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T10:05:00Z")],
        crfd_rows=[_crfd("GC", observation_time="2026-07-01T10:00:00Z")],
        gre_report={"generated_at": "2026-07-01T10:10:00Z", "regime_label": "LONG", "confidence": 72},
        generated_at=NOW,
    )

    assert enrichments[0]["gre_label"] == "LONG"
    assert enrichments[0]["gre_confidence"] == 72
    assert enrichments[0]["gre_provenance"] == "latest_gold_regime_engine_time_aligned"


def test_missing_crfd_vwap_avwap_are_flagged_and_null() -> None:
    enrichments = build_trade_outcome_enrichments([_outcome("NQ", "NQU6")], crfd_rows=[], generated_at=NOW)
    row = enrichments[0]

    assert row["crfd_join_success"] is False
    assert row["vwap_relation"] is None
    assert row["avwap_relation"] is None
    assert "missing_crfd_context" in row["data_quality_flags"]
    assert "missing_vwap_context" in row["data_quality_flags"]
    assert "missing_avwap_context" in row["data_quality_flags"]


def test_unavailable_vwap_and_avwap_are_not_fabricated() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6")],
        crfd_rows=[_crfd("GC", vwap_relation="unavailable", avwap_relation="unavailable")],
        generated_at=NOW,
    )

    assert enrichments[0]["vwap_relation"] == "unavailable"
    assert enrichments[0]["avwap_relation"] == "unavailable"
    assert "missing_vwap_context" in enrichments[0]["data_quality_flags"]
    assert "missing_avwap_context" in enrichments[0]["data_quality_flags"]


def test_summary_generation() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6"), _outcome("NQ", "NQU6")],
        crfd_rows=[_crfd("GC", vwap_relation="above_vwap", avwap_relation="above_avwap")],
        market_context_rows=[_vix_context("2026-06-30T20:15:00Z", level=16.45)],
        generated_at=NOW,
    )
    summary = build_trade_outcome_enrichment_summary(
        enrichments,
        outcome_count=2,
        crfd_row_count=1,
        gre_report={},
        market_context_row_count=1,
        generated_at=NOW,
    )

    assert summary["overall"]["enrichment_count"] == 2
    assert summary["coverage_counts"]["crfd"] == 1
    assert summary["coverage_counts"]["vwap"] == 1
    assert summary["coverage_counts"]["avwap"] == 1
    assert summary["coverage_counts"]["market_context"] == 2
    assert summary["coverage_counts"]["vix"] == 2
    assert summary["overall"]["vix_coverage"] == 1.0
    assert summary["market_context"]["successful_joins"] == 2
    assert summary["market_context"]["failed_joins"] == 0
    assert summary["missing_reasons"]["missing_gre_context"] == 2
    assert summary["context_validity"]["market_context"]["classification_counts"][VALIDITY_VALID] == 2
    assert summary["context_validity"]["gre"]["classification_counts"][VALIDITY_UNAVAILABLE] == 2


def test_context_validity_marks_stale_join() -> None:
    validity = build_context_validity(
        provider="historical_gre_backfill",
        provider_timestamp="2026-07-01T10:00:00Z",
        observation_timestamp="2026-07-01T10:00:00Z",
        join_timestamp="2026-07-01T12:00:01Z",
        age_seconds=7201,
        freshness_window_seconds=3600,
        provenance="historical_gre_backfill",
    )

    assert validity["validity_classification"] == VALIDITY_STALE
    assert validity["provenance"] == "historical_gre_backfill"


def test_context_validity_marks_outside_provider_window() -> None:
    validity = build_context_validity(
        provider="historical_gre_backfill",
        provider_timestamp="2026-07-01T13:05:00Z",
        observation_timestamp="2026-07-01T13:05:00Z",
        join_timestamp="2026-07-01T18:07:53Z",
        age_seconds=18173,
        freshness_window_seconds=3600,
        provenance="historical_gre_backfill",
        provider_window_end="2026-07-01T13:05:00Z",
        provider_window_end_is_hard_boundary=True,
    )

    assert validity["validity_classification"] == VALIDITY_OUTSIDE_PROVIDER_WINDOW


def test_context_validity_marks_unavailable_context() -> None:
    validity = build_context_validity(
        provider=None,
        provider_timestamp=None,
        observation_timestamp=None,
        join_timestamp="2026-07-01T18:07:53Z",
        age_seconds=None,
        freshness_window_seconds=3600,
        provenance=None,
    )

    assert validity["validity_classification"] == VALIDITY_UNAVAILABLE


def test_historical_gre_tail_is_classified_outside_provider_window() -> None:
    enrichments = build_trade_outcome_enrichments(
        [_outcome("GC", "GCQ6", entry_time="2026-07-01T18:07:53Z")],
        historical_gre_rows=[
            _historical_gre("2026-07-01T13:05:00Z", label="LONG", confidence=71),
        ],
        generated_at=NOW,
    )

    row = enrichments[0]
    assert row["gre_label"] == "LONG"
    assert row["gre_validity_classification"] == VALIDITY_OUTSIDE_PROVIDER_WINDOW
    assert "outside_provider_window_gre_context" in row["data_quality_flags"]


def test_empty_dataset_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    outcomes.write_text("", encoding="utf-8")
    result = run_trade_outcome_enrichment(
        outcomes_path=outcomes,
        crfd_rows_path=tmp_path / "missing_crfd.jsonl",
        gre_report_path=tmp_path / "missing_gre.json",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.enrichments == []
    assert result.summary["overall"]["enrichment_count"] == 0
    assert result.enrichment_path.exists()
    assert result.summary_path.exists()
    assert result.market_context_report_path.exists()
    assert result.market_context_join_quality_path.exists()
    assert result.market_context_data_quality_path.exists()
    assert result.historical_gre_report_path.exists()
    assert result.historical_gre_join_quality_path.exists()
    assert result.historical_gre_data_quality_path.exists()


def test_trade_outcome_enrichment_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_outcome_enrichment.py"),
        Path("src/mgc_v05l/app/track_b_trade_outcome_enrichment.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _outcome(instrument: str, contract: str, *, entry_time: str = "2026-07-01T10:05:00Z") -> dict:
    return {
        "schema_version": "track_b_canonical_trade_outcome_v1",
        "trade_outcome_id": f"outcome_{contract}",
        "strategy_id": "strategy",
        "lane_id": "lane",
        "instrument": instrument,
        "contract": contract,
        "side": "LONG",
        "entry_time": entry_time,
        "exit_time": "2026-07-01T10:30:00Z",
        "session_at_entry": "LONDON",
        "data_quality_flags": ["missing_realized_r_proxy"],
        "diagnostic_only": True,
    }


def _crfd(
    contract: str,
    *,
    observation_time: str = "2026-07-01T10:00:00Z",
    vwap_relation: str = "above_vwap",
    avwap_relation: str = "above_avwap",
) -> dict:
    return {
        "schema_version": "track_b_research_feature_dataset_v1",
        "observation_time": observation_time,
        "contract": contract,
        "session": "LONDON",
        "session_label": "LONDON_OPEN",
        "provider_id": "parquet",
        "provider_kind": "historical_databento_parquet",
        "vwap": 4000.0,
        "vwap_relation": vwap_relation,
        "distance_from_vwap_points": 2.0,
        "avwap_globex_session_open_18et": 3998.0,
        "avwap_relation_globex_session_open_18et": avwap_relation,
        "diagnostic_only": True,
    }


def _vix_context(observation_time: str, *, level: float) -> dict:
    normalized_time = observation_time.replace("Z", "+00:00")
    return {
        "schema_version": "track_b_canonical_market_context_v1",
        "provider_name": "VIX",
        "provider_kind": "historical_vix",
        "context_key": "vix",
        "symbol": "VIX",
        "vix_available": True,
        "vix_level": level,
        "vix_observation_time": normalized_time,
        "vix_source": "cboe_official_daily_history",
        "vix_daily_change": -1.2,
        "vix_regime": "LOW",
        "vix_percentile": 0.31,
        "vix_ma_20": 18.1,
        "vix_ma_50": 17.6,
        "source_provenance": {"source_ref": "vix_fixture"},
        "diagnostic_only": True,
        "production_effect": False,
    }


def _historical_gre(observation_time: str, *, label: str, confidence: int) -> dict:
    normalized_time = observation_time.replace("Z", "+00:00")
    return {
        "schema_version": "track_b_gre_validation_row_v1",
        "gre_generated_at": normalized_time,
        "classification_candle_max_ts": normalized_time,
        "instrument": "GOLD",
        "contract": "GC",
        "regime_label": label,
        "confidence": confidence,
        "directional_bias": "BULLISH" if label == "LONG" else "BEARISH" if label == "SHORT" else "NEUTRAL",
        "validation_status": "VALIDATED",
        "provider_metadata": {
            "provider_id": "parquet",
            "provider_kind": "historical_databento_parquet",
            "diagnostic_only": True,
        },
        "source_refs": {"gre_report": "BACKFILL_GENERATED_GRE_OBSERVATION"},
        "diagnostic_only": True,
    }


def json_line(payload: dict) -> str:
    import json

    return json.dumps(payload, sort_keys=True)
