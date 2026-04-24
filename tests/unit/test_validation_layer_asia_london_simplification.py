from __future__ import annotations

from pathlib import Path

from validation_layer.orchestration.asia_london_simplification import (
    DEFAULT_SIMPLIFIED_LONG_VARIANTS,
    DEFAULT_SIMPLIFIED_SHORT_VARIANTS,
    SIMPLIFIED_CANDIDATES,
    TUNABLE_DEGREES_OF_FREEDOM,
    _build_candidate_system_payload,
    _comparison_summary,
)


def test_simplification_surface_is_bounded_and_base_only() -> None:
    assert DEFAULT_SIMPLIFIED_LONG_VARIANTS == (
        "segment_forced_long_v4_breakout_or_bar7",
        "segment_forced_long_v5_dip_reclaim_or_bar8",
    )
    assert DEFAULT_SIMPLIFIED_SHORT_VARIANTS == (
        "segment_forced_short_v2_reclaim_fail_or_bar7",
        "segment_forced_short_v4_breakdown_or_bar7",
    )
    assert len(SIMPLIFIED_CANDIDATES) == 4
    assert any(item["name"] == "gate_mode" for item in TUNABLE_DEGREES_OF_FREEDOM)


def test_simplification_candidate_system_payload_covers_gc_mgc_and_nq_mnq() -> None:
    payload = _build_candidate_system_payload(
        gc_mgc_json=Path("/tmp/gc_mgc.json"),
        nq_mnq_json=Path("/tmp/nq_mnq.json"),
    )

    rows = payload["candidate_system"]["lane_sequence"]
    assert len(rows) == 8
    assert sorted({row["symbol_group"] for row in rows}) == ["GC_MGC", "NQ_MNQ"]
    assert sorted({row["source_variant"] for row in rows}) == sorted(
        definition.source_variant for definition in SIMPLIFIED_CANDIDATES
    )


def test_comparison_summary_recognizes_bias_improvement_without_flattening() -> None:
    tuned_rows = {
        "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {
            "overall_status": "probation",
            "train_bias_status": "fail",
            "train_bias_score": 0.20,
            "selection_bias_status": "warn",
            "cscv_pbo_status": "pass",
            "monte_carlo_status": "warn",
            "drawdown_status": "pass",
            "trade_normalization_status": "pass",
        },
        "LONG__segment_forced_long_v6_contextual_fallback__base": {
            "overall_status": "probation",
            "train_bias_status": "fail",
            "train_bias_score": 0.18,
            "selection_bias_status": "warn",
            "cscv_pbo_status": "pass",
            "monte_carlo_status": "pass",
            "drawdown_status": "pass",
            "trade_normalization_status": "pass",
        },
    }
    simplified_rows = {
        "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base": {
            "overall_status": "probation",
            "train_bias_status": "warn",
            "train_bias_score": 0.42,
            "selection_bias_status": "warn",
            "cscv_pbo_status": "pass",
            "monte_carlo_status": "pass",
            "drawdown_status": "pass",
            "trade_normalization_status": "pass",
        },
        "LONG__segment_forced_long_v4_breakout_or_bar7__base": {
            "overall_status": "probation",
            "train_bias_status": "warn",
            "train_bias_score": 0.48,
            "selection_bias_status": "warn",
            "cscv_pbo_status": "pass",
            "monte_carlo_status": "pass",
            "drawdown_status": "pass",
            "trade_normalization_status": "pass",
        },
    }

    summary = _comparison_summary(tuned_rows=tuned_rows, simplified_rows=simplified_rows)

    assert summary["conclusion"] == "possible_but_needs_more_evidence"
    overlapping = {
        row["source_variant"]: row
        for row in summary["rows"]
        if row["source_variant"] == "LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base"
    }
    assert overlapping["LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base"]["comparison"]["train_bias_improved"] is True
