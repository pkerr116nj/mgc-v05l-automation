from __future__ import annotations

from mgc_v05l.execution_core.track_b_canonical_analytics_engine import (
    AnalyticsFilter,
    CanonicalAnalyticsEngine,
    CanonicalAnalyticsQuery,
    CanonicalAnalyticsRequest,
    ContextValidityRule,
    aggregate_metric_group,
    default_dimension_registry,
    default_filter_registry,
    default_metric_registry,
    percentile_distribution,
    run_canonical_analytics,
    run_canonical_query,
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


def test_query_layer_uses_dimension_and_metric_registries() -> None:
    result = run_canonical_query(
        [_row("a", strategy="s1", pnl=10.0), _row("b", strategy="s1", pnl=-4.0)],
        CanonicalAnalyticsQuery(
            name="strategy_expectancy",
            dimensions=("strategy",),
            metrics=("trade_count", "win_rate", "expectancy_proxy", "sample_class"),
        ),
        generated_at="2026-07-03T12:00:00Z",
        provenance={"source": "unit_test"},
    )

    assert result.schema_version == "track_b_canonical_analytics_result_v1"
    assert result.summary["matched_count"] == 2
    assert result.provenance["source"] == "unit_test"
    assert result.grouped_rows[0]["key"] == "s1"
    assert result.grouped_rows[0]["trade_count"] == 2
    assert result.grouped_rows[0]["expectancy_proxy"] == 3.0


def test_query_layer_named_validity_filter() -> None:
    result = run_canonical_query(
        [_row("a", pnl=10.0, gre_validity="VALID"), _row("b", pnl=-4.0, gre_validity="UNAVAILABLE")],
        CanonicalAnalyticsQuery(
            name="valid_gre",
            dimensions=("gre_label",),
            metrics=("trade_count",),
            named_filters=("valid_gre_only",),
        ),
    )

    assert result.summary["matched_count"] == 1
    assert result.validity_metadata["resolved_validity_rules"][0]["field"] == "gre_validity_classification"
    assert result.grouped_rows[0]["trade_count"] == 1


def test_query_layer_minimum_sample_rules() -> None:
    result = run_canonical_query(
        [_row("a", strategy="s1", pnl=10.0), _row("b", strategy="s2", pnl=-4.0)],
        CanonicalAnalyticsQuery(
            name="sample_gate",
            dimensions=("strategy",),
            metrics=("trade_count",),
            min_sample_size=2,
        ),
    )

    assert result.summary["matched_count"] == 2
    assert result.summary["pre_sample_filter_group_count"] == 2
    assert result.summary["group_count"] == 0
    assert result.grouped_rows == []


def test_query_layer_date_window_filter() -> None:
    result = run_canonical_query(
        [_row("a", pnl=10.0, entry_time="2026-07-01T10:00:00Z"), _row("b", pnl=-4.0, entry_time="2026-07-02T10:00:00Z")],
        CanonicalAnalyticsQuery(
            name="date_window",
            dimensions=("side",),
            metrics=("trade_count",),
            filters=(AnalyticsFilter("entry_time", "date_lte", "2026-07-01T23:59:59Z"),),
        ),
    )

    assert result.summary["matched_count"] == 1
    assert result.grouped_rows[0]["trade_count"] == 1


def test_registries_expose_expected_names_and_reject_unknown_metric() -> None:
    assert "strategy" in default_dimension_registry()
    assert "expectancy_proxy" in default_metric_registry()
    assert "valid_vix_only" in default_filter_registry()

    try:
        CanonicalAnalyticsEngine().run([_row("a", pnl=1.0)], CanonicalAnalyticsQuery(name="bad", metrics=("not_real",)))
    except ValueError as exc:
        assert "Unsupported canonical analytics metric" in str(exc)
    else:
        raise AssertionError("unsupported metric did not raise")


def test_advanced_profit_factor_normal_case() -> None:
    result = run_canonical_query(
        [_row("a", pnl=10.0), _row("b", pnl=5.0), _row("c", pnl=-3.0), _row("d", pnl=-2.0)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("profit_factor_proxy",)),
    )

    row = result.grouped_rows[0]
    assert row["profit_factor_proxy"] == 3.0
    assert row["metric_data_quality_flags"] == {}


def test_advanced_profit_factor_no_loss_returns_null_and_flag() -> None:
    result = run_canonical_query(
        [_row("a", pnl=10.0), _row("b", pnl=5.0)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("profit_factor_proxy",)),
    )

    row = result.grouped_rows[0]
    assert row["profit_factor_proxy"] is None
    assert row["metric_data_quality_flags"]["profit_factor_no_losses"] == 1


def test_advanced_payoff_ratio_normal_case() -> None:
    result = run_canonical_query(
        [_row("a", pnl=10.0), _row("b", pnl=20.0), _row("c", pnl=-5.0), _row("d", pnl=-15.0)],
        CanonicalAnalyticsQuery(
            name="advanced",
            metrics=("average_winner_pnl_proxy", "average_loser_pnl_proxy", "payoff_ratio_proxy"),
        ),
    )

    row = result.grouped_rows[0]
    assert row["average_winner_pnl_proxy"] == 15.0
    assert row["average_loser_pnl_proxy"] == -10.0
    assert row["payoff_ratio_proxy"] == 1.5


def test_advanced_percentile_metrics() -> None:
    result = run_canonical_query(
        [_row(str(i), pnl=float(i)) for i in range(1, 6)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("p10_pnl_proxy", "p25_pnl_proxy", "p75_pnl_proxy", "p90_pnl_proxy")),
    )

    row = result.grouped_rows[0]
    assert row["p10_pnl_proxy"] == 1.4
    assert row["p25_pnl_proxy"] == 2.0
    assert row["p75_pnl_proxy"] == 4.0
    assert row["p90_pnl_proxy"] == 4.6


def test_advanced_tail_means_use_bounded_decile_subset() -> None:
    values = [-10.0, -5.0, -2.0, 1.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0]
    result = run_canonical_query(
        [_row(str(i), pnl=value) for i, value in enumerate(values)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("downside_tail_mean_proxy", "upside_tail_mean_proxy")),
    )

    row = result.grouped_rows[0]
    assert row["downside_tail_mean_proxy"] == -10.0
    assert row["upside_tail_mean_proxy"] == 34.0


def test_advanced_streak_metrics_are_order_aware() -> None:
    rows = [
        _row("a", pnl=1.0, entry_time="2026-07-01T10:00:00Z", exit_time="2026-07-01T10:05:00Z"),
        _row("b", pnl=2.0, entry_time="2026-07-01T10:01:00Z", exit_time="2026-07-01T10:06:00Z"),
        _row("c", pnl=-1.0, entry_time="2026-07-01T10:02:00Z", exit_time="2026-07-01T10:07:00Z"),
        _row("d", pnl=-2.0, entry_time="2026-07-01T10:03:00Z", exit_time="2026-07-01T10:08:00Z"),
        _row("e", pnl=-3.0, entry_time="2026-07-01T10:04:00Z", exit_time="2026-07-01T10:09:00Z"),
        _row("f", pnl=4.0, entry_time="2026-07-01T10:05:00Z", exit_time="2026-07-01T10:10:00Z"),
    ]
    result = run_canonical_query(
        rows,
        CanonicalAnalyticsQuery(name="advanced", metrics=("consecutive_win_streak_max", "consecutive_loss_streak_max")),
    )

    row = result.grouped_rows[0]
    assert row["consecutive_win_streak_max"] == 2
    assert row["consecutive_loss_streak_max"] == 3


def test_advanced_missing_pnl_proxy_handled_safely() -> None:
    result = run_canonical_query(
        [_row("a", pnl=None), _row("b", pnl=None)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("profit_factor_proxy", "p10_pnl_proxy", "downside_tail_mean_proxy")),
    )

    row = result.grouped_rows[0]
    assert row["profit_factor_proxy"] is None
    assert row["p10_pnl_proxy"] is None
    assert row["downside_tail_mean_proxy"] is None
    assert row["metric_data_quality_flags"]["missing_pnl_proxy_for_profit_factor"] == 1


def test_advanced_sample_confidence_label() -> None:
    result = run_canonical_query(
        [_row(str(i), pnl=1.0) for i in range(10)],
        CanonicalAnalyticsQuery(name="advanced", metrics=("sample_confidence_label",)),
    )

    assert result.grouped_rows[0]["sample_confidence_label"] == "PRELIMINARY_SAMPLE_LOW_CONFIDENCE"


def _row(
    trade_id: str,
    *,
    pnl: float | None,
    strategy: str = "strategy_a",
    side: str = "LONG",
    gre_validity: str = "VALID",
    entry_time: str = "2026-07-01T10:00:00Z",
    exit_time: str | None = None,
) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "strategy_id": strategy,
        "lane_id": "lane_a",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": side,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "realized_points": pnl,
        "realized_pnl_proxy": pnl,
        "hold_seconds": 120.0,
        "gre_label": "LONG",
        "gre_validity_classification": gre_validity,
        "data_quality_flags": ["missing_realized_r_proxy"],
        "enrichment_data_quality_flags": [],
    }
