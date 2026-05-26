from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_overfiltering_missing_regime_audit import (
    OverfilteringMissingRegimeAuditConfig,
    build_overfiltering_missing_regime_audit,
    create_overfiltering_missing_regime_audit,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_audit_prioritizes_atp_activation_and_overfiltering_before_sub80(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_strategy_underperformance_review.json",
        {
            "strategy_inventory": ["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"],
            "rejection_attribution": {
                "total_near_misses": 3,
                "total_one_gate_away": 1,
                "total_two_gate_away": 2,
                "ranked_blockers": [{"blocker": "BREAKOUT_RETEST_STRICTNESS", "count": 2}],
                "per_strategy": [
                    {
                        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                        "evaluations": 10,
                        "near_miss_count": 3,
                        "one_gate_away_count": 1,
                        "two_gate_away_count": 2,
                        "top_failed_predicates": [{"predicate": "signal_retests_breakout", "count": 2}],
                        "ranked_blockers": [{"blocker": "BREAKOUT_RETEST_STRICTNESS", "count": 2}],
                    }
                ],
            },
            "forward_outcome_simulation": {
                "outcome_counts": {"MISSED_WINNER": 1},
                "interpretation": "near miss produced upside",
                "late_join_strong_drift_count": 1,
                "max_late_join_score": 4.5,
                "simulations": [
                    {
                        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                        "outcome_classification": "MISSED_WINNER",
                    }
                ],
            },
            "regime_coverage_audit": {
                "observed_by_symbol": {"MGC": {"primary_regime": "TREND_PARTICIPATION"}},
                "uncovered_regime_counts": {"TREND_PARTICIPATION": 1},
                "interpretation": "continuation pressure",
            },
            "ab_shadow_lane_generator_plan": {
                "generated_shadow_variants": [
                    {"shadow_variant_id": "ASIAN_DRIFT_LATE_JOIN_B_GRADE_SHADOW_V1"}
                ]
            },
            "threshold_pressure_analysis": {
                "evidence_summary": ">=0.80 was negative; defer sub80.",
            },
        },
    )
    _write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {"enabled_strategy_ids": ["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"]},
    )
    atp_config = tmp_path / "config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"
    atp_config.parent.mkdir(parents=True, exist_ok=True)
    atp_config.write_text(
        "probationary_paper_lanes_json: '[{\"lane_id\":\"atp_companion_v1_gc_asia_us\","
        "\"standalone_strategy_id\":\"atp_companion_v1__paper_gc_asia_us\","
        "\"strategy_family\":\"active_trend_participation_engine\","
        "\"strategy_identity_root\":\"ATP_COMPANION_V1\","
        "\"symbol\":\"GC\",\"quality_bucket_policy\":\"MEDIUM_HIGH_ONLY\","
        "\"experimental_status\":\"paper_candidate\",\"paper_only\":true,\"non_approved\":true}]'\n",
        encoding="utf-8",
    )

    audit = build_overfiltering_missing_regime_audit(
        config=OverfilteringMissingRegimeAuditConfig(repo_root=tmp_path),
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert audit["broker_mutation_allowed"] is False
    assert audit["paper_proof_allowed"] is False
    assert audit["overfiltering_audit"]["classification"] == "OVERFILTERING_REQUIRES_SHADOW_REMEDIATION"
    assert audit["atp_trend_participation_activation_audit"]["guarded_roster_active_count"] == 0
    assert audit["sub80_fit_mining_scope"]["classification"].startswith("SUB80_MINING_DEFERRED")
    assert audit["top_implementation_candidates"][0]["action"] == "ACTIVATE_EXISTING_ATP_SHADOW"


def test_create_writes_overfiltering_markdown_and_json(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_strategy_underperformance_review.json",
        {
            "strategy_inventory": ["asian_drift_v1"],
            "rejection_attribution": {"per_strategy": [], "total_near_misses": 0},
            "forward_outcome_simulation": {},
            "regime_coverage_audit": {},
        },
    )
    config = OverfilteringMissingRegimeAuditConfig(repo_root=tmp_path)

    json_path, md_path, audit = create_overfiltering_missing_regime_audit(
        config=config,
        now=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert json_path.exists()
    assert md_path.exists()
    assert audit["analysis_only"] is True
    assert "Track B Over-Filtering / Missing-Regime Audit" in md_path.read_text(encoding="utf-8")
