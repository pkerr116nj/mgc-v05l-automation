from __future__ import annotations

from mgc_v05l.execution_core.track_b_canonical_analytics_engine import (
    AnalyticsFilter,
    CanonicalAnalyticsRequest,
    ContextValidityRule,
    aggregate_metric_group,
    percentile_distribution,
    run_canonical_analytics,
    sample_class_for_count,
)


def test_canonical_engine_groups_by_dimensions() -> None:
    result = run_canonical_analytics(
        [_row("a", strategy="s1", pnl=10.0), _row("b", strategy="s1", pnl=-4.0), _row("c", strategy="s2", pnl=2.0)],
        CanonicalAnalyticsRequest(name="fixture", dimensions=("strategy_id",)),
    )

    groups = {row["key"]: row for row in result["groups"]}
    assert result["matched_count"] == 3
    assert groups["s1"]["count"] == 2
    assert groups["s1"]["win_rate"] == 0.5
    assert groups["s1"]["average_pnl_proxy"] == 3.0
    assert groups["s2"]["best_trade"]["trade_outcome_id"] == "c"


def test_canonical_engine_applies_filters() -> None:
    result = run_canonical_analytics(
        [_row("a", strategy="s1", pnl=10.0, side="LONG"), _row("b", strategy="s2", pnl=-4.0, side="SHORT")],
        CanonicalAnalyticsRequest(name="long_only", dimensions=("side",), filters=(AnalyticsFilter("side", "eq", "LONG"),)),
    )

    assert result["matched_count"] == 1
    assert result["groups"][0]["key"] == "LONG"


def test_canonical_engine_applies_validity_rules() -> None:
    result = run_canonical_analytics(
        [_row("a", pnl=10.0, gre_validity="VALID"), _row("b", pnl=-4.0, gre_validity="STALE")],
        CanonicalAnalyticsRequest(
            name="valid_gre",
            dimensions=("gre_label",),
            validity_rules=(ContextValidityRule("gre_validity_classification"),),
        ),
    )

    assert result["matched_count"] == 1
    assert result["groups"][0]["key"] == "LONG"


def test_canonical_metric_group_and_percentiles() -> None:
    group = aggregate_metric_group("fixture", [_row("a", pnl=1.0), _row("b", pnl=3.0), _row("c", pnl=5.0)])

    assert group["median_pnl_proxy"] == 3.0
    assert group["pnl_percentiles"]["p50"] == 3.0
    assert percentile_distribution([1.0, 2.0, 3.0])["p100"] == 3.0


def test_canonical_sample_classes_and_empty_input() -> None:
    result = run_canonical_analytics([], CanonicalAnalyticsRequest(name="empty", dimensions=("strategy_id",)))

    assert result["matched_count"] == 0
    assert result["groups"] == []
    assert sample_class_for_count(1) == "EXPLORATORY"
    assert sample_class_for_count(10) == "PRELIMINARY"
    assert sample_class_for_count(30) == "DEVELOPING"
    assert sample_class_for_count(100) == "RESEARCH_GRADE"


def test_canonical_engine_guardrail_flags() -> None:
    result = run_canonical_analytics([_row("a", pnl=1.0)], CanonicalAnalyticsRequest(name="guardrails"))

    assert result["diagnostic_only"] is True
    assert result["production_recommendation"] is False
    assert result["trading_gate"] is False


def _row(
    trade_id: str,
    *,
    pnl: float,
    strategy: str = "strategy_a",
    side: str = "LONG",
    gre_validity: str = "VALID",
) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "strategy_id": strategy,
        "lane_id": "lane_a",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": side,
        "entry_time": "2026-07-01T10:00:00Z",
        "realized_points": pnl,
        "realized_pnl_proxy": pnl,
        "hold_seconds": 120.0,
        "gre_label": "LONG",
        "gre_validity_classification": gre_validity,
        "data_quality_flags": ["missing_realized_r_proxy"],
        "enrichment_data_quality_flags": [],
    }
