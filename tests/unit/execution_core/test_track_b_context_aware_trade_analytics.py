from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_context_aware_trade_analytics import (
    aggregate_context_groups,
    build_context_aware_trade_analytics,
    run_context_aware_trade_analytics,
)


NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


def test_vix_regime_aggregation() -> None:
    analytics = build_context_aware_trade_analytics(
        [_outcome("a", pnl=10.0), _outcome("b", pnl=-4.0), _outcome("c", pnl=7.0)],
        enrichments=[_enrichment("a", regime="LOW"), _enrichment("b", regime="LOW"), _enrichment("c", regime="ELEVATED")],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in analytics["scorecards"]["vix_regime"]["groups"]}

    assert groups["LOW"]["trade_count"] == 2
    assert groups["LOW"]["average_pnl_proxy"] == 3.0
    assert groups["ELEVATED"]["trade_count"] == 1
    assert analytics["overall"]["vix_coverage"] == 1.0


def test_vix_percentile_bucket_aggregation() -> None:
    analytics = build_context_aware_trade_analytics(
        [_outcome("a", pnl=10.0), _outcome("b", pnl=-4.0), _outcome("c", pnl=7.0)],
        enrichments=[
            _enrichment("a", percentile=0.1),
            _enrichment("b", percentile=0.35),
            _enrichment("c", percentile=0.85),
        ],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in analytics["scorecards"]["vix_percentile_buckets"]["groups"]}

    assert groups["0-20"]["trade_count"] == 1
    assert groups["20-40"]["trade_count"] == 1
    assert groups["80-100"]["trade_count"] == 1


def test_cross_tab_aggregation() -> None:
    analytics = build_context_aware_trade_analytics(
        [
            _outcome("a", pnl=10.0, strategy="s1", lane="l1", session="LONDON", instrument="GC", exit_policy="TIMEBOX"),
            _outcome("b", pnl=-4.0, strategy="s1", lane="l1", session="LONDON", instrument="GC", exit_policy="TIMEBOX"),
            _outcome("c", pnl=7.0, strategy="s2", lane="l2", session="US", instrument="NQ", exit_policy="SIGNAL"),
        ],
        enrichments=[_enrichment("a", regime="LOW"), _enrichment("b", regime="LOW"), _enrichment("c", regime="NORMAL")],
        generated_at=NOW,
    )

    strategy = {row["key"]: row for row in analytics["scorecards"]["strategy_by_vix_regime"]["groups"]}
    session = {row["key"]: row for row in analytics["scorecards"]["session_by_vix_regime"]["groups"]}
    instrument = {row["key"]: row for row in analytics["scorecards"]["instrument_by_vix_regime"]["groups"]}
    exits = {row["key"]: row for row in analytics["scorecards"]["exit_policy_by_vix_regime"]["groups"]}

    assert strategy["s1 | l1 | LOW"]["trade_count"] == 2
    assert session["LONDON | LOW"]["trade_count"] == 2
    assert instrument["GC | GCQ6 | LOW"]["trade_count"] == 2
    assert exits["TIMEBOX | LOW"]["trade_count"] == 2


def test_sample_size_warnings() -> None:
    groups = aggregate_context_groups([_merged("a", pnl=10.0)], key_fields=("vix_regime",))

    assert groups[0]["sample_size_warning"] == "TOO_THIN"
    assert groups[0]["context_label"] == "TOO_THIN"


def test_missing_vix_context_handled_safely() -> None:
    analytics = build_context_aware_trade_analytics(
        [_outcome("a", pnl=10.0), _outcome("b", pnl=-4.0)],
        enrichments=[_enrichment("a"), {"trade_outcome_id": "b", "diagnostic_only": True}],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in analytics["scorecards"]["vix_regime"]["groups"]}

    assert analytics["overall"]["vix_coverage"] == 0.5
    assert groups["UNKNOWN"]["trade_count"] == 1
    assert analytics["data_quality"]["missing_vix_context_count"] == 1


def test_empty_enriched_dataset_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    outcomes.write_text("", encoding="utf-8")
    enrichments.write_text("", encoding="utf-8")

    result = run_context_aware_trade_analytics(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        enrichment_summary_path=tmp_path / "missing_summary.json",
        t3_scorecard_path=tmp_path / "missing_t3.json",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.analytics["overall"]["joined_outcome_count"] == 0
    assert result.json_path.exists()
    assert result.strategy_path.exists()
    assert result.vix_percentile_path.exists()


def test_context_aware_trade_analytics_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_context_aware_trade_analytics.py"),
        Path("src/mgc_v05l/app/track_b_context_aware_trade_analytics.py"),
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


def _outcome(
    trade_id: str,
    *,
    pnl: float,
    strategy: str = "strategy",
    lane: str = "lane",
    session: str = "LONDON",
    instrument: str = "GC",
    exit_policy: str = "TIMEBOX",
) -> dict:
    return {
        "schema_version": "track_b_canonical_trade_outcome_v1",
        "trade_outcome_id": trade_id,
        "strategy_id": strategy,
        "lane_id": lane,
        "instrument": instrument,
        "contract": f"{instrument}Q6",
        "side": "LONG",
        "entry_time": "2026-07-01T10:00:00Z",
        "exit_time": "2026-07-01T10:30:00Z",
        "hold_seconds": 1800.0,
        "realized_points": pnl,
        "realized_pnl_proxy": pnl,
        "exit_policy": exit_policy,
        "session_at_entry": session,
        "data_quality_flags": ["missing_realized_r_proxy"],
        "diagnostic_only": True,
    }


def _enrichment(
    trade_id: str,
    *,
    regime: str = "LOW",
    percentile: float = 0.25,
    level: float = 16.5,
) -> dict:
    return {
        "schema_version": "track_b_trade_outcome_enrichment_v1",
        "trade_outcome_id": trade_id,
        "market_context_join_success": True,
        "vix_level": level,
        "vix_regime": regime,
        "vix_daily_change": 0.2,
        "vix_percentile": percentile,
        "vix_ma20": 18.0,
        "vix_ma50": 17.0,
        "market_context_source": "vix_fixture",
        "market_context_provider": "VIX",
        "market_context_timestamp": "2026-06-30T20:15:00Z",
        "market_context_staleness": 49500.0,
        "data_quality_flags": [],
        "diagnostic_only": True,
    }


def _merged(trade_id: str, *, pnl: float) -> dict:
    row = _outcome(trade_id, pnl=pnl)
    row.update(_enrichment(trade_id))
    return row

