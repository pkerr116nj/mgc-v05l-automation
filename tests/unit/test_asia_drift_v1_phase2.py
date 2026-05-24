from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from tests.unit.test_asia_drift_v1_phase1 import _uptrend_session_with_break

from mgc_v05l.research.asia_drift import (
    AsiaDriftReplayBarWindow,
    BALANCED_REVISION,
    BRANCH_PROFILE_CANDIDATE_DISCOVERY,
    BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,
    BRANCH_PROFILE_STRICT_CURRENT,
    COMPRESSION_FORMING,
    COMPRESSION_READY,
    CONTINUATION_CONFIRMED,
    DEFAULT_PREFILL_COMPARISON_PROFILES,
    DEFAULT_REFINED_REPLAY_WINDOWS,
    DEFAULT_WIDER_REPLAY_WINDOWS,
    DRIFT_CONTEXT,
    ENTRY_MODEL_CONFIRMATION_REACCEL,
    ENTRY_MODEL_LIMIT_LESS_PASSIVE,
    ENTRY_MODEL_LIMIT_PULLBACK,
    ENTRY_MODEL_SHALLOW_PARTICIPATION,
    ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
    EXIT_PROFILE_EARLY_PROTECTION,
    EXIT_PROFILE_SPEC_BASELINE,
    FAST_DEEP_DISQUALIFYING,
    FAST_SHALLOW_VALID,
    FALSE_BREAK_OR_CHOP,
    PREFILL_PROFILE_PERSISTENCE_MEDIUM,
    PREFILL_PROFILE_PERSISTENCE_SHORT,
    PREFILL_PROFILE_RECOVERY_CONFIRMED,
    PERSISTENCE_CONFIRMED,
    REJECTION_CONFIRMED,
    RECOVERY_CONFIRMED,
    ES_MES_COMPRESSION_CONTINUATION,
    METALS_COMPRESSION_CONTINUATION,
    SESSION_TIMEOUT,
    SESSION_TIMEOUT_UNRESOLVED,
    STATE_DRIFT_AT_RISK,
    STATE_ENTRY_ARMED,
    STATE_RECOVERED_DRIFT,
    STATE_REQUALIFIED_CANDIDATE,
    STATE_THESIS_INVALIDATED,
    STRICT_CURRENT,
    UNRESOLVED_PENDING,
    assemble_entry_setups,
    evaluate_entry_models,
    run_calibration_comparison_from_bars,
    run_asia_drift_phase1_from_bars,
    run_asia_drift_phase2_from_bars,
    run_asia_drift_opportunity_taxonomy_from_bars,
    run_asia_drift_open_cluster_discovery_from_bars,
    run_asia_drift_multi_year_discovery_from_bars,
    run_asia_drift_pattern_discovery_from_bars,
    run_cross_asset_confirmation_from_bars,
    run_cross_asset_trade_mapping_from_rows,
    run_compression_continuation_detector_from_bars,
    run_prefill_persistence_comparison_from_bars,
    run_refined_shallow_research_from_bars,
    run_refined_shallow_wider_research_from_bars,
    get_branch_control_profile,
    get_compression_profile,
)
from mgc_v05l.research.asia_drift.compression_continuation import (
    _candidate_lifecycle_outcome,
    _compression_compatible_chop,
    _compression_warmup_persistence,
    _false_break,
)
from mgc_v05l.research.asia_drift.data_continuity_audit import (
    _compress_date_ranges,
    _monthly_gap_rows,
    _root_cause_summary,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


NY = ZoneInfo("America/New_York")


def _bar(*, end_ts: datetime, open_: float, high: float, low: float, close: float, instrument: str = "MGC") -> ResearchBar:
    return ResearchBar(
        instrument=instrument,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="ASIA",
        session_segment="ASIA",
        source="synthetic",
        provenance="unit_test",
    )


def _bars_from_closes(*, closes: list[float]) -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    for index, close in enumerate(closes):
        if index < 8:
            open_ = price + 0.22
            close_value = open_ + 0.38
            low = open_ - 0.08
            high = close_value + 0.06
        else:
            close_value = close
            open_ = price - 0.02 if close_value <= price else price + 0.03
            low = min(close_value, open_) - 0.04
            high = max(close_value, open_) + 0.03
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close_value))
        price = close_value
    return bars


def _early_invalidation_then_continuation_session() -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    for index in range(14):
        if index < 8:
            open_ = price + 0.22
            close = open_ + 0.38
            low = open_ - 0.08
            high = close + 0.06
        elif index in (8, 9):
            open_ = price + 0.08
            close = open_ - 0.26
            low = close - 0.10
            high = open_ + 0.05
        elif index == 10:
            open_ = price + 0.03
            close = open_ + 0.08
            low = open_ - 0.05
            high = close + 0.04
        else:
            open_ = price + 0.18
            close = open_ + 0.34
            low = open_ - 0.06
            high = close + 0.06
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close))
        price = close
    return bars


def _stable_armed_session() -> list[ResearchBar]:
    return _bars_from_closes(closes=[0.0] * 8 + [104.65, 104.50, 104.35, 104.45, 104.55, 104.65, 104.75])


def _confirmation_target_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8
        + [104.65, 104.50, 104.35, 104.45, 104.678, 104.95, 105.22, 105.46, 105.70, 105.95, 106.20, 106.45, 106.70]
    )


def _confirmation_failure_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8
        + [104.65, 104.50, 104.35, 104.45, 104.678, 103.90, 103.40, 103.10, 102.80]
    )


def _vwap_touch_then_reclaim_session() -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    for index in range(12):
        if index < 8:
            open_ = price + 0.22
            close = open_ + 0.38
            low = open_ - 0.08
            high = close + 0.06
        elif index == 8:
            open_ = price - 0.02
            close = price - 0.02
            low = close - 0.55
            high = open_ + 0.05
        elif index == 9:
            open_ = price - 0.05
            close = price - 0.35
            low = close - 0.08
            high = open_ + 0.03
        else:
            open_ = price + 0.08
            close = open_ + 0.16
            low = open_ - 0.04
            high = close + 0.03
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close))
        price = close
    return bars


def _confirmed_failure_session() -> list[ResearchBar]:
    bars = _uptrend_session_with_break()
    last = bars[-1]
    bars.append(
        _bar(
            end_ts=last.end_ts + timedelta(minutes=5),
            open_=last.close - 0.12,
            high=last.close + 0.04,
            low=last.close - 0.85,
            close=last.close - 0.50,
        )
    )
    last = bars[-1]
    bars.append(
        _bar(
            end_ts=last.end_ts + timedelta(minutes=5),
            open_=last.close - 0.08,
            high=last.close + 0.02,
            low=last.close - 0.80,
            close=last.close - 0.45,
        )
    )
    return bars


def _recovery_requalification_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8 + [104.65, 104.50, 104.36, 104.34, 104.64, 104.85, 105.00]
    )


def _recovery_confirmed_failure_session() -> list[ResearchBar]:
    bars = _confirmed_failure_session()
    last = bars[-1]
    bars.append(
        _bar(
            end_ts=last.end_ts + timedelta(minutes=5),
            open_=last.close - 0.05,
            high=last.close + 0.01,
            low=last.close - 0.70,
            close=last.close - 0.20,
        )
    )
    return bars


def _fast_shallow_valid_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8 + [104.65, 104.34, 104.58, 104.82, 105.02, 105.18]
    )


def _fast_deep_disqualifying_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8 + [104.65, 103.70, 103.62, 103.55, 103.48]
    )


def _with_instrument(bars: list[ResearchBar], instrument: str) -> list[ResearchBar]:
    return [replace(bar, instrument=instrument) for bar in bars]


def _compression_continuation_session(instrument: str = "MGC") -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    templates = [
        (0.22, 0.44, -0.07, 0.05),
        (0.20, 0.42, -0.07, 0.05),
        (0.18, 0.40, -0.06, 0.05),
        (0.18, 0.36, -0.06, 0.05),
        (0.16, 0.34, -0.05, 0.05),
        (0.14, 0.32, -0.05, 0.04),
        (0.12, 0.28, -0.05, 0.04),
        (0.10, 0.26, -0.04, 0.04),
        (0.02, 0.06, -0.03, 0.03),
        (0.01, 0.05, -0.02, 0.02),
        (0.00, 0.04, -0.02, 0.02),
        (0.18, 0.34, -0.03, 0.05),
        (0.20, 0.38, -0.04, 0.05),
        (0.18, 0.36, -0.04, 0.05),
    ]
    for index, (open_bump, close_bump, low_pad, high_pad) in enumerate(templates):
        open_ = price + open_bump
        close = open_ + close_bump
        low = open_ + low_pad
        high = close + high_pad
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close, instrument=instrument))
        price = close
    return bars


def _compression_false_break_session(instrument: str = "MGC") -> list[ResearchBar]:
    bars = _compression_continuation_session(instrument=instrument)
    first_break = bars[-3]
    bars[-3] = _bar(
        end_ts=first_break.end_ts,
        open_=first_break.close - 0.04,
        high=first_break.close + 0.02,
        low=first_break.close - 1.05,
        close=first_break.close - 0.72,
        instrument=instrument,
    )
    second_break = bars[-2]
    bars[-2] = _bar(
        end_ts=second_break.end_ts,
        open_=second_break.close - 0.03,
        high=second_break.close + 0.02,
        low=second_break.close - 0.88,
        close=second_break.close - 0.46,
        instrument=instrument,
    )
    return bars


def _compression_pending_timeout_session(instrument: str = "MGC") -> list[ResearchBar]:
    base = datetime(2026, 3, 2, 18, 0, tzinfo=NY)
    bars: list[ResearchBar] = []
    price = 100.0
    templates = [
        (0.22, 0.42, -0.07, 0.05),
        (0.20, 0.40, -0.07, 0.05),
        (0.18, 0.38, -0.06, 0.05),
        (0.16, 0.36, -0.06, 0.04),
        (0.14, 0.32, -0.05, 0.04),
        (0.12, 0.28, -0.05, 0.04),
        (0.10, 0.24, -0.04, 0.03),
        (0.08, 0.20, -0.04, 0.03),
        (0.01, 0.03, -0.02, 0.02),
        (0.00, 0.02, -0.02, 0.02),
        (-0.01, 0.02, -0.02, 0.02),
        (0.00, 0.01, -0.02, 0.02),
        (-0.01, 0.01, -0.02, 0.02),
        (0.00, 0.01, -0.02, 0.02),
    ]
    for index, (open_bump, close_bump, low_pad, high_pad) in enumerate(templates):
        open_ = price + open_bump
        close = open_ + close_bump
        low = open_ + low_pad
        high = close + high_pad
        bars.append(_bar(end_ts=base + timedelta(minutes=index * 5), open_=open_, high=high, low=low, close=close, instrument=instrument))
        price = close
    return bars


def _compression_post_spike_candidate_session(instrument: str = "MGC") -> list[ResearchBar]:
    bars = _compression_pending_timeout_session(instrument=instrument)
    adjusted = list(bars)
    ref = adjusted[8]
    adjusted[8] = _bar(
        end_ts=ref.end_ts,
        open_=ref.open,
        high=ref.high + 1.1,
        low=ref.low - 0.9,
        close=ref.close - 0.2,
        instrument=instrument,
    )
    return adjusted


def _compression_session_timeout_unresolved_session(instrument: str = "MGC") -> list[ResearchBar]:
    base = datetime(2026, 3, 3, 1, 50, tzinfo=NY)
    bars: list[ResearchBar] = []
    template = _compression_pending_timeout_session(instrument=instrument)
    for index, ref in enumerate(template):
        bars.append(
            _bar(
                end_ts=base + timedelta(minutes=index * 5),
                open_=ref.open,
                high=ref.high,
                low=ref.low,
                close=ref.close,
                instrument=instrument,
            )
        )
    return bars


def _compression_followthrough_false_break_session(instrument: str = "MGC") -> list[ResearchBar]:
    bars = _compression_continuation_session(instrument=instrument)
    replacement_specs = [
        (-4, -0.03, 0.02, -0.62, -0.34),
        (-3, -0.03, 0.02, -0.78, -0.46),
        (-2, -0.04, 0.02, -0.92, -0.58),
    ]
    for offset, open_delta, high_delta, low_delta, close_delta in replacement_specs:
        ref = bars[offset]
        bars[offset] = _bar(
            end_ts=ref.end_ts,
            open_=ref.close + open_delta,
            high=ref.close + high_delta,
            low=ref.close + low_delta,
            close=ref.close + close_delta,
            instrument=instrument,
        )
    return bars


def _fast_shallow_chase_cap_session() -> list[ResearchBar]:
    return _bars_from_closes(
        closes=[0.0] * 8 + [104.65, 104.36, 105.35, 105.60, 105.82]
    )


def _prefill_regime_warning_rows(*, recovered: bool) -> tuple[list[ResearchBar], callable]:
    bars = _stable_armed_session()

    def mutate_rows(rows):
        modified = list(rows)
        modified[9] = replace(
            modified[9],
            regime="NO_TRADE",
            pullback_state="NORMAL_PULLBACK",
            fast_pullback_class=FAST_SHALLOW_VALID,
            recovery_score=0.54,
            regime_persistence_score=0.46,
            pullback_depth_atr=0.62,
            pullback_depth_fraction=0.16,
            pullback_vwap_interaction="VWAP_TAG",
            pullback_structure_break=False,
            chop_veto=False,
            post_spike_instability=False,
            thesis_invalidated_flag=False,
        )
        modified[10] = replace(
            modified[10],
            regime="ASIA_DRIFT_LONG" if recovered else "NO_TRADE",
            pullback_state="NORMAL_PULLBACK",
            fast_pullback_class=FAST_SHALLOW_VALID,
            recovery_score=0.66 if recovered else 0.50,
            regime_persistence_score=0.60 if recovered else 0.42,
            pullback_depth_atr=0.54,
            pullback_depth_fraction=0.14,
            pullback_vwap_interaction="FAVORABLE_SIDE_OF_VWAP",
            pullback_structure_break=False,
            close=104.74 if recovered else 104.40,
            high=104.77 if recovered else 104.44,
            low=104.60 if recovered else 104.34,
            close_location=0.78 if recovered else 0.34,
            signed_vwap_displacement_long=3.05 if recovered else 2.10,
            chop_veto=False,
            post_spike_instability=False,
            thesis_invalidated_flag=False,
        )
        return modified

    return bars, mutate_rows


def test_confirmation_entry_and_profit_target_exit(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_confirm_target",
        bars_5m=_confirmation_target_session(),
        source_label="confirm_target",
    )

    confirmation_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_CONFIRMATION_REACCEL)
    assert confirmation_eval.accepted is True
    assert confirmation_eval.entry_ts is not None

    exit_reasons = {row.exit_profile: row.exit_reason for row in run.trade_records}
    assert exit_reasons[EXIT_PROFILE_SPEC_BASELINE] == "profit_target_hit"
    assert exit_reasons[EXIT_PROFILE_EARLY_PROTECTION] == "profit_target_hit"


def test_exit_model_detects_post_entry_invalidation(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_confirm_fail",
        bars_5m=_confirmation_failure_session(),
        source_label="confirm_fail",
    )

    confirmation_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_CONFIRMATION_REACCEL)
    assert confirmation_eval.accepted is True
    assert all(row.exit_reason in {"depth_exceeds_limit", "hard_stop_close_breach"} for row in run.trade_records)


def test_limit_entry_model_triggers_when_close_lands_inside_zone(tmp_path: Path) -> None:
    phase1_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase2_limit_phase1",
        bars_5m=_stable_armed_session(),
        source_label="stable_armed",
    )
    setups = assemble_entry_setups(feature_rows=phase1_run.feature_rows, state_rows=phase1_run.state_rows)
    assert setups
    setup = setups[0]
    fill_index = next(index for index, row in enumerate(phase1_run.feature_rows) if row.decision_ts > setup.armed_ts)
    fill_price = setup.entry_limit_price
    assert fill_price is not None

    modified_rows = list(phase1_run.feature_rows)
    modified_rows[fill_index] = replace(
        modified_rows[fill_index],
        close=fill_price,
        high=fill_price + 0.02,
        low=fill_price - 0.02,
    )
    evaluations = evaluate_entry_models(
        feature_rows=modified_rows,
        state_rows=phase1_run.state_rows,
        setups=setups,
    )
    limit_eval = next(row for row in evaluations if row.entry_model == ENTRY_MODEL_LIMIT_PULLBACK)

    assert limit_eval.accepted is True
    assert limit_eval.entry_price == fill_price


def test_invalidation_diagnostics_show_continuation_after_reject(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_early_invalidation",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="early_invalidation",
        calibration_profile_name=STRICT_CURRENT,
    )

    audit = run.diagnostics["premature_invalidation_audit"]
    assert audit["event_count"] >= 1
    assert audit["resumed_after_invalidation_count"] >= 1


def test_pullback_veto_false_veto_summary_is_populated(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_false_veto",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="false_veto",
        calibration_profile_name=STRICT_CURRENT,
    )

    pullback = run.diagnostics["pullback_veto_diagnostics"]
    assert pullback["rejected_pullback_count"] >= 1
    assert pullback["false_veto_count"] >= 1
    sample = pullback["sample_false_vetoes"][0]
    assert "veto_reason" in sample
    assert "veto_category" in sample
    assert "bars_to_continuation" in sample


def test_phase2_artifacts_write_expected_shapes(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_artifacts",
        bars_5m=_confirmation_target_session(),
        source_label="artifact_shape",
    )
    summary_payload = json.loads(run.artifacts.summary_json_path.read_text(encoding="utf-8"))
    diagnostics_payload = json.loads(run.artifacts.diagnostics_json_path.read_text(encoding="utf-8"))

    assert run.artifacts.entry_setups_path.exists()
    assert run.artifacts.entry_evaluations_path.exists()
    assert run.artifacts.trade_records_path.exists()
    assert "entry_model_summary" in summary_payload
    assert "trade_matrix_summary" in summary_payload
    assert "pullback_veto_diagnostics" in diagnostics_payload


def test_vwap_touch_vs_reclaim_are_distinct(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_vwap_distinction",
        bars_5m=_uptrend_session_with_break(),
        source_label="vwap_distinction",
        calibration_profile_name=BALANCED_REVISION,
    )
    interactions = [row.pullback_vwap_interaction for row in run.feature_rows]

    assert "VWAP_TAG" in interactions
    assert "CLOSE_THROUGH_VWAP_AND_SLOW_EMA" in interactions


def test_balanced_revision_delays_hard_invalidation(tmp_path: Path) -> None:
    strict_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_strict_invalidation",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="strict_invalidation",
        calibration_profile_name=STRICT_CURRENT,
    )
    balanced_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_balanced_invalidation",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="balanced_invalidation",
        calibration_profile_name=BALANCED_REVISION,
    )

    strict_states = [row.state for row in strict_run.state_rows]
    balanced_states = [row.state for row in balanced_run.state_rows]
    assert STATE_THESIS_INVALIDATED in strict_states
    assert STATE_ENTRY_ARMED in balanced_states
    if STATE_THESIS_INVALIDATED in balanced_states:
        assert balanced_states.index(STATE_THESIS_INVALIDATED) > strict_states.index(STATE_THESIS_INVALIDATED)
    else:
        assert STATE_ENTRY_ARMED in balanced_states


def test_persistence_confirmed_recovers_from_at_risk_before_invalidation(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_persistence_recovery",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="persistence_recovery",
        calibration_profile_name=PERSISTENCE_CONFIRMED,
    )
    states = [row.state for row in run.state_rows]
    assert STATE_ENTRY_ARMED in states
    assert STATE_THESIS_INVALIDATED not in states


def test_persistence_confirmed_requires_confirmed_failure_for_invalidation(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_persistence_break",
        bars_5m=_confirmed_failure_session(),
        source_label="persistence_break",
        calibration_profile_name=PERSISTENCE_CONFIRMED,
    )
    states = [row.state for row in run.state_rows]
    assert STATE_DRIFT_AT_RISK in states
    assert STATE_THESIS_INVALIDATED in states
    assert states.index(STATE_THESIS_INVALIDATED) > states.index(STATE_DRIFT_AT_RISK)


def test_recovery_confirmed_requalifies_after_temporary_regime_damage(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_recovery_requalify",
        bars_5m=_recovery_requalification_session(),
        source_label="recovery_requalify",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    states = [row.state for row in run.state_rows]
    assert STATE_DRIFT_AT_RISK in states
    assert STATE_REQUALIFIED_CANDIDATE in states


def test_countertrend_failure_supports_requalification(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_recovery_features",
        bars_5m=_recovery_requalification_session(),
        source_label="recovery_features",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    requalified_index = next(index for index, row in enumerate(run.state_rows) if row.state == STATE_REQUALIFIED_CANDIDATE)
    feature = run.feature_rows[requalified_index]
    assert feature.countertrend_extension_failed is True
    assert feature.recovery_label == "REQUALIFY"


def test_recovery_confirmed_invalidates_after_failed_recovery(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_recovery_failure",
        bars_5m=_recovery_confirmed_failure_session(),
        source_label="recovery_failure",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    states = [row.state for row in run.state_rows]
    assert STATE_DRIFT_AT_RISK in states
    assert STATE_THESIS_INVALIDATED in states
    assert states.index(STATE_THESIS_INVALIDATED) > states.index(STATE_DRIFT_AT_RISK)


def test_balanced_revision_widens_stretched_but_valid_tolerance(tmp_path: Path) -> None:
    strict_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_strict_break",
        bars_5m=_uptrend_session_with_break(),
        source_label="strict_break",
        calibration_profile_name=STRICT_CURRENT,
    )
    balanced_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_balanced_break",
        bars_5m=_uptrend_session_with_break(),
        source_label="balanced_break",
        calibration_profile_name=BALANCED_REVISION,
    )

    strict_disq = sum(1 for row in strict_run.feature_rows if row.pullback_state == "DISQUALIFYING_PULLBACK")
    balanced_disq = sum(1 for row in balanced_run.feature_rows if row.pullback_state == "DISQUALIFYING_PULLBACK")
    assert balanced_disq <= strict_disq


def test_profile_comparison_output_shape(tmp_path: Path) -> None:
    result = run_calibration_comparison_from_bars(
        output_dir=tmp_path / "phase1_compare_profiles",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="compare_profiles",
        profile_names=(STRICT_CURRENT, BALANCED_REVISION),
    )
    payload = json.loads(Path(result["artifacts"]["comparison_json_path"]).read_text(encoding="utf-8"))

    assert "profile_comparison_rows" in payload
    assert len(payload["profile_comparison_rows"]) == 2
    assert "recommendation" in payload
    assert "recommended_next_default" in payload["recommendation"]
    assert "warning_recovery_rate" in payload["profile_comparison_rows"][0]
    assert "requalified_candidate_count" in payload["profile_comparison_rows"][0]


def test_phase2_warning_state_audit_is_emitted(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_warning_audit",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="warning_audit",
        calibration_profile_name=PERSISTENCE_CONFIRMED,
    )
    audit = run.diagnostics["warning_state_audit"]
    assert audit["event_count"] >= 0
    assert "sample_events" in audit


def test_entry_geometry_diagnostics_capture_fill_proximity(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_entry_geometry",
        bars_5m=_confirmation_target_session(),
        source_label="entry_geometry",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    diagnostics = run.diagnostics["entry_geometry_diagnostics"]
    strict_limit_event = next(
        row for row in diagnostics["sample_cancellations"] if row["entry_model"] == ENTRY_MODEL_LIMIT_PULLBACK
    )
    assert strict_limit_event["closest_fill_distance_atr"] is not None
    assert strict_limit_event["closest_fill_distance_atr"] > 0.0


def test_reexpanded_without_fill_diagnostics_are_populated(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_reexpanded_diagnostics",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="reexpanded_diagnostics",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    audit = run.diagnostics["entry_geometry_diagnostics"]["reexpanded_without_fill_audit"]
    assert audit["count"] >= 1
    assert audit["sample_events"][0]["bars_to_reexpansion"] is not None


def test_alternative_entry_geometry_comparison_is_emitted(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_alt_geometry",
        bars_5m=_stable_armed_session(),
        source_label="alt_geometry",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    geometry = run.diagnostics["entry_geometry_diagnostics"]["hypothetical_fill_counts_by_geometry"]
    assert ENTRY_MODEL_LIMIT_PULLBACK in geometry
    assert ENTRY_MODEL_CONFIRMATION_REACCEL in geometry
    assert ENTRY_MODEL_LIMIT_LESS_PASSIVE in geometry
    assert ENTRY_MODEL_SHALLOW_PARTICIPATION in geometry
    assert ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED in geometry
    assert geometry[ENTRY_MODEL_SHALLOW_PARTICIPATION] >= 1


def test_cancellation_reason_artifact_shape_includes_entry_geometry(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_cancel_shape",
        bars_5m=_confirmation_target_session(),
        source_label="cancel_shape",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    payload = json.loads(run.artifacts.diagnostics_json_path.read_text(encoding="utf-8"))
    geometry = payload["entry_geometry_diagnostics"]
    assert "cancellation_reason_breakdown" in geometry
    assert "sample_cancellations" in geometry
    assert "fill_proximity_distribution" in geometry


def test_phase2_runner_stays_within_output_dir_artifacts(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_side_effects",
        bars_5m=_confirmation_target_session(),
        source_label="side_effects",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    assert str(run.artifacts.root_dir).startswith(str(tmp_path))
    assert str(run.phase1_run.artifacts.root_dir).startswith(str(tmp_path))


def test_fast_shallow_valid_classification_is_emitted(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_fast_shallow_valid",
        bars_5m=_early_invalidation_then_continuation_session(),
        source_label="fast_shallow_valid",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    assert any(row.fast_pullback_class == FAST_SHALLOW_VALID for row in run.feature_rows)


def test_fast_deep_disqualifying_classification_is_emitted(tmp_path: Path) -> None:
    run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "phase1_fast_deep_disq",
        bars_5m=_fast_deep_disqualifying_session(),
        source_label="fast_deep_disq",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    assert any(row.fast_pullback_class == FAST_DEEP_DISQUALIFYING for row in run.feature_rows)


def test_refined_shallow_participation_qualifies_fast_shallow_reset(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_refined_shallow_accept",
        bars_5m=_fast_shallow_valid_session(),
        source_label="refined_shallow_accept",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    refined_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.prerequisites_met is True
    assert refined_eval.accepted is True


def test_refined_shallow_participation_rejects_when_chase_cap_is_exceeded(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_refined_shallow_chase",
        bars_5m=_fast_shallow_chase_cap_session(),
        source_label="refined_shallow_chase",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    refined_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.accepted is False
    assert refined_eval.cancellation_reason == "chase_cap_exceeded"


def test_refined_shallow_participation_preserves_deep_pullback_disqualification(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_refined_shallow_deep_disq",
        bars_5m=_fast_deep_disqualifying_session(),
        source_label="refined_shallow_deep_disq",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    refined_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.accepted is False
    assert refined_eval.cancellation_reason in {"fast_deep_disqualifying", "depth_exceeds_limit", "pullback_disqualified"}


def test_phase2_entry_geometry_report_includes_pullback_class_sections(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "phase2_pullback_class_report",
        bars_5m=_fast_shallow_valid_session(),
        source_label="pullback_class_report",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    payload = json.loads(run.artifacts.diagnostics_json_path.read_text(encoding="utf-8"))
    geometry = payload["entry_geometry_diagnostics"]
    assert "pullback_class_outcomes" in geometry
    assert "shallow_participation_trade_summary" in geometry


def test_multi_window_refined_runner_emits_window_rows_and_aggregate_summary(tmp_path: Path) -> None:
    result = run_refined_shallow_research_from_bars(
        output_dir=tmp_path / "refined_broader_replay",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
        ),
    )
    payload = result["payload"]
    assert payload["primary_entry_model"] == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED
    assert len(payload["window_summaries"]) == 2
    assert "MGC" in payload["aggregate_summary"]


def test_refined_broader_replay_handles_zero_entry_windows(tmp_path: Path) -> None:
    result = run_refined_shallow_research_from_bars(
        output_dir=tmp_path / "refined_zero_entry_windows",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_deep_disqualifying_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
        ),
    )
    aggregate = result["payload"]["aggregate_summary"]["MGC"]
    assert aggregate["accepted_entries"] == 0
    assert aggregate["windows_with_accepted_entries"] == 0


def test_refined_broader_replay_artifact_shape_is_written(tmp_path: Path) -> None:
    result = run_refined_shallow_research_from_bars(
        output_dir=tmp_path / "refined_artifact_shape",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),),
    )
    artifacts = result["artifacts"]
    aggregate_payload = json.loads(Path(artifacts["aggregate_json_path"]).read_text(encoding="utf-8"))
    assert "window_summaries" in aggregate_payload
    assert "aggregate_summary" in aggregate_payload
    assert "recommendation" in aggregate_payload


def test_refined_broader_replay_stays_research_only(tmp_path: Path) -> None:
    result = run_refined_shallow_research_from_bars(
        output_dir=tmp_path / "refined_research_only",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),),
    )
    assert str(Path(result["artifacts"]["aggregate_json_path"]).resolve()).startswith(str(tmp_path))
    assert DEFAULT_REFINED_REPLAY_WINDOWS


def test_wider_replay_runner_emits_context_tags_and_clustering(tmp_path: Path) -> None:
    result = run_refined_shallow_wider_research_from_bars(
        output_dir=tmp_path / "wider_replay",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            AsiaDriftReplayBarWindow(label="window_c", bars_5m=_fast_shallow_valid_session()),
        ),
    )
    payload = result["payload"]
    row = payload["window_summaries"][0]
    aggregate = payload["aggregate_summary"]["MGC"]
    assert row["context_realized_volatility_bucket"] in {"quiet", "normal", "elevated", "spiky", "unknown"}
    assert row["context_directional_efficiency_bucket"] in {"low", "medium", "high", "unknown"}
    assert row["context_market_texture"] in {"chop_heavy", "directional", "mixed"}
    assert "accepted_entry_clustering" in aggregate
    assert "context_breakdown" in aggregate


def test_wider_replay_handles_zero_entry_windows(tmp_path: Path) -> None:
    result = run_refined_shallow_wider_research_from_bars(
        output_dir=tmp_path / "wider_zero_entries",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_deep_disqualifying_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
        ),
    )
    aggregate = result["payload"]["aggregate_summary"]["MGC"]
    clustering = aggregate["accepted_entry_clustering"]
    assert aggregate["accepted_entries"] == 0
    assert clustering["accepted_window_count"] == 0
    assert clustering["entries_clustered_in_single_window"] is False


def test_wider_replay_context_tags_flow_into_session_manifest(tmp_path: Path) -> None:
    result = run_refined_shallow_wider_research_from_bars(
        output_dir=tmp_path / "wider_context_manifest",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),),
    )
    payload = result["payload"]
    session_row = payload["session_manifest"][0]
    assert "realized_volatility_bucket" in session_row
    assert "directional_efficiency_bucket" in session_row
    assert "session_texture_tag" in session_row
    assert "post_spike_context_tag" in session_row


def test_wider_replay_stays_research_only_and_uses_default_windows() -> None:
    assert DEFAULT_WIDER_REPLAY_WINDOWS


def test_opportunity_taxonomy_emits_session_classification_shape(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_shape",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
        ),
    )
    payload = result["payload"]
    session_row = payload["session_taxonomy_manifest"][0]
    assert "opportunity_type" in session_row
    assert "directional_continuation" in session_row
    assert "best_participation_point_type" in session_row
    assert "blocked_by_current_v1" in session_row
    assert "instrument_family" in session_row


def test_opportunity_taxonomy_handles_sessions_with_no_continuation(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_no_continuation",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_deep_disqualifying_session()),),
    )
    session_row = result["payload"]["session_taxonomy_manifest"][0]
    assert session_row["directional_continuation"] is False


def test_opportunity_taxonomy_emits_missed_pattern_audit_for_continuation_without_entry(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_missed_pattern",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_chase_cap_session()),),
    )
    audit_rows = result["payload"]["missed_pattern_audit"]
    assert audit_rows
    assert audit_rows[0]["blocked_by_current_v1"] in {"ENTRY_GEOMETRY", "REGIME_LOSS", "PULLBACK_CLASSIFIER", "CHOP_VETO", "POST_SPIKE_VETO", "DRIFT_FILTER"}


def test_opportunity_taxonomy_artifacts_stay_research_only(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_research_only",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),),
    )
    assert str(Path(result["artifacts"]["summary_json_path"]).resolve()).startswith(str(tmp_path))


def test_opportunity_taxonomy_aggregate_summary_includes_context_and_recommendation(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_aggregate_shape",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_shallow_chase_cap_session()),
        ),
    )
    payload = result["payload"]
    aggregate = payload["aggregate_summary"]["MGC"]
    assert "context_breakdown" in aggregate
    assert "candidate_opportunity_types_worth_research" in aggregate
    assert "branch_reference_counts" in aggregate
    assert "recommendation" in payload


def test_opportunity_taxonomy_family_and_transfer_shapes_exist(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_family_shape",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_shallow_chase_cap_session()),
        ),
    )
    payload = result["payload"]
    assert "family_summary" in payload
    assert "metals" in payload["family_summary"]
    assert "branch_transfer_summary" in payload
    assert "detector_transfer_audit" in payload


def test_opportunity_taxonomy_tags_index_family_when_primary_is_mes(tmp_path: Path) -> None:
    result = run_asia_drift_opportunity_taxonomy_from_bars(
        output_dir=tmp_path / "taxonomy_indices",
        primary_instrument="MES",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_fast_shallow_valid_session(), "MES")),),
    )
    payload = result["payload"]
    session_row = payload["session_taxonomy_manifest"][0]
    assert session_row["instrument_family"] == "indices"
    assert payload["primary_instrument_family"] == "indices"


def test_compression_profile_selection_is_family_specific() -> None:
    assert get_compression_profile("MGC").name == METALS_COMPRESSION_CONTINUATION
    assert get_compression_profile("ES").name == ES_MES_COMPRESSION_CONTINUATION


def test_compression_compatible_chop_supports_controlled_coil_without_false_break() -> None:
    profile = get_compression_profile("MGC")
    row = SimpleNamespace(
        chop_veto=True,
        post_spike_instability=False,
        dominant_direction="LONG",
        long_drift_score=0.92,
        short_drift_score=0.10,
        regime_persistence_score=0.31,
        bar_overlap_ratio_8=0.78,
        reversal_frequency_12=0.18,
        realized_volatility_ratio=0.86,
        pullback_depth_atr=0.54,
        pullback_expansion_ratio=0.84,
        countertrend_extension_failed=True,
        compression_followed_by_drift_expansion=False,
        bars_since_last_drift_impulse=1,
        renewed_signed_vwap_displacement=0.09,
        pullback_state="STRETCHED_BUT_VALID",
        close=101.2,
        atr=1.0,
        session_vwap=101.0,
        slow_ema=101.0,
        slope_combo_long=0.12,
        slope_combo_short=-0.12,
    )

    compatible = _compression_compatible_chop(row=row, direction="LONG", profile=profile)
    branch_profile = get_branch_control_profile(BRANCH_PROFILE_FALSE_BREAK_CONFIRMED)
    false_break = _false_break(
        row=row,
        direction="LONG",
        profile=profile,
        branch_profile=branch_profile,
        compression_low=100.7,
        compression_high=101.4,
        family="metals",
        overlap_label="COMPRESSION_COMPATIBLE_OVERLAP",
    )

    assert compatible["active"] is True
    assert "compression_compatible_chop" in compatible["reasons"]
    assert false_break["active"] is False


def test_compression_warmup_persistence_keeps_branch_alive_for_early_coil_bar() -> None:
    profile = get_compression_profile("MGC")
    row = SimpleNamespace(
        chop_veto=True,
        post_spike_instability=False,
        bar_overlap_ratio_8=0.74,
        pullback_depth_atr=0.62,
        pullback_expansion_ratio=0.82,
        realized_volatility_ratio=1.02,
        bars_since_last_drift_impulse=1,
    )
    warmup = _compression_warmup_persistence(
        row=row,
        previous_state=DRIFT_CONTEXT,
        compression_score=0.42,
        profile=profile,
    )
    assert warmup["active"] is True
    assert "compression_warmup_persistence" in warmup["reasons"]


def test_compression_branch_score_and_state_progression(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_progression",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session()),),
    )
    session = result["payload"]["per_session_summary"][0]
    assert session["branch_detected_compression_ready"] is True
    assert session["branch_detected_continuation_confirmed"] is True
    states = {row["state"] for row in result["payload"]["_per_bar_rows"]}
    assert DRIFT_CONTEXT in states
    assert COMPRESSION_FORMING in states or COMPRESSION_READY in states
    assert CONTINUATION_CONFIRMED in states


def test_compression_branch_false_break_rejection(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_false_break",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_false_break_session()),),
    )
    session = result["payload"]["per_session_summary"][0]
    assert session["branch_false_break_or_chop"] is True
    states = {row["state"] for row in result["payload"]["_per_bar_rows"]}
    assert FALSE_BREAK_OR_CHOP in states


def test_compression_branch_maturation_profile_does_not_immediately_resolve_overlap_as_chop(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_maturation",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_false_break_session()),),
    )
    states = [row["state"] for row in result["payload"]["_per_bar_rows"]]
    false_break_index = states.index(FALSE_BREAK_OR_CHOP)
    ready_index = states.index(COMPRESSION_READY)
    assert ready_index < false_break_index


def test_compression_branch_confirms_false_break_after_follow_through(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_followthrough_break",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_followthrough_false_break_session()),),
    )
    states = [row["state"] for row in result["payload"]["_per_bar_rows"]]
    assert COMPRESSION_FORMING in states or COMPRESSION_READY in states
    assert FALSE_BREAK_OR_CHOP in states


def test_compression_branch_handles_family_specific_index_profile(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_mes",
        primary_instrument="MES",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),),
    )
    session = result["payload"]["per_session_summary"][0]
    assert session["compression_profile"] == ES_MES_COMPRESSION_CONTINUATION
    assert session["instrument_family"] == "indices"


def test_compression_branch_artifact_shape(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_artifacts",
        primary_instrument="MGC",
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session()),),
    )
    payload = result["payload"]
    assert "instrument_summary" in payload
    assert "family_summary" in payload
    assert "evaluation" in payload
    assert "recommendation" in payload
    assert "profile_comparison_rows" in payload
    assert "funnel_audit" in payload


def test_candidate_discovery_non_confirmation_becomes_unresolved_pending_not_rejection(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_timeout_unresolved",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_pending_timeout_session()),),
    )
    lifecycle_row = result["payload"]["_candidate_lifecycle_rows"][0]
    assert lifecycle_row["lifecycle_outcome"] == UNRESOLVED_PENDING
    assert lifecycle_row["candidate_cleanliness"] == "unresolved_clean_candidate"
    assert lifecycle_row["rejection_confirmed"] is False


def test_candidate_discovery_session_timeout_maps_to_timeout_unresolved() -> None:
    outcome = _candidate_lifecycle_outcome(
        per_bar_rows=[
            {"state": COMPRESSION_FORMING},
            {"state": COMPRESSION_READY},
            {"state": SESSION_TIMEOUT},
        ],
        compression_forming=True,
        compression_ready=True,
        continuation_confirmed=False,
        false_break_or_chop=False,
        killed_by_post_spike=False,
        killed_by_deep_damage=False,
    )
    assert outcome == SESSION_TIMEOUT_UNRESOLVED


def test_confirmed_rejection_requires_adverse_evidence(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_rejection_confirmed",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_followthrough_false_break_session()),),
    )
    lifecycle_row = result["payload"]["_candidate_lifecycle_rows"][0]
    assert lifecycle_row["lifecycle_outcome"] == REJECTION_CONFIRMED
    assert lifecycle_row["adverse_rejection_reason_tags"]


def test_contaminated_candidate_classification_is_explicit(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_contaminated_candidate",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_post_spike_candidate_session()),),
    )
    lifecycle_row = result["payload"]["_candidate_lifecycle_rows"][0]
    assert lifecycle_row["candidate_cleanliness"] in {"post_spike_contaminated_candidate", "deep_damage_contaminated_candidate"}


def test_candidate_maturation_artifact_shape_is_present(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_maturation_artifacts",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_pending_timeout_session()),),
    )
    payload = result["payload"]
    artifacts = result["artifacts"]
    assert "candidate_maturation_summary" in payload
    assert Path(artifacts["maturation_json_path"]).exists()
    assert Path(artifacts["pending_unresolved_manifest_path"]).exists()
    assert Path(artifacts["confirmed_rejection_manifest_path"]).exists()
    assert Path(artifacts["candidate_lifecycle_rows_path"]).exists()


def test_candidate_discovery_profile_is_labeled_as_detector_stage_only(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_candidate_discovery",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session()),),
    )
    payload = result["payload"]
    assert payload["profile_comparison_rows"][0]["branch_profile"] == BRANCH_PROFILE_CANDIDATE_DISCOVERY
    assert payload["profile_comparison_rows"][0]["branch_profile_role"] == "candidate_discovery"


def test_compression_funnel_stage_accounting_has_expected_keys(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_funnel",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_false_break_session()),),
    )
    funnel = result["payload"]["funnel_audit"]
    assert "total_bars_loaded" in funnel
    assert "total_asia_sessions_scanned" in funnel
    assert "sessions_with_possible_compression_behavior" in funnel
    assert "taxonomy_target_stage_failures" in funnel


def test_taxonomy_recall_metric_shape_is_present_for_detector_profiles(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_recall_shape",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY, BRANCH_PROFILE_FALSE_BREAK_CONFIRMED),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_false_break_session()),),
    )
    comparison_rows = result["payload"]["profile_comparison_rows"]
    assert "candidate_detection_recall_against_taxonomy" in comparison_rows[0]
    assert "ready_recall_against_taxonomy" in comparison_rows[0]


def test_date_range_and_coverage_reporting_shape_is_present(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_coverage_shape",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session()),),
    )
    payload = result["payload"]
    assert "study_window_coverage" in payload
    assert "database_coverage" in payload
    assert "coverage_rows" in payload["database_coverage"]


def test_candidate_discovery_preserves_deep_damage_exclusion(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_deep_damage",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_CANDIDATE_DISCOVERY,),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_deep_disqualifying_session()),),
    )
    stage_row = result["payload"]["_session_stage_rows"][0]
    assert stage_row["killed_by_deep_damage"] is True


def test_compression_branch_missed_target_audit_shape(tmp_path: Path) -> None:
    result = run_compression_continuation_detector_from_bars(
        output_dir=tmp_path / "compression_branch_missed_audit",
        primary_instrument="MGC",
        branch_profile_names=(BRANCH_PROFILE_STRICT_CURRENT, BRANCH_PROFILE_FALSE_BREAK_CONFIRMED),
        windows=(AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_false_break_session()),),
    )
    strict_rows = result["payload"]["profile_results"][BRANCH_PROFILE_STRICT_CURRENT]["artifacts_for_review"]["missed_target_audit"]
    assert strict_rows
    assert "failed_stage" in strict_rows[0]
    assert "rejection_confirmed" in strict_rows[0]


def test_prefill_warning_window_recovers_into_accepted_entry(tmp_path: Path) -> None:
    bars, mutate = _prefill_regime_warning_rows(recovered=True)
    phase1_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "prefill_warning_phase1",
        bars_5m=bars,
        source_label="prefill_warning_recovery",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    setups = assemble_entry_setups(feature_rows=phase1_run.feature_rows, state_rows=phase1_run.state_rows)
    evaluations = evaluate_entry_models(
        feature_rows=mutate(phase1_run.feature_rows),
        state_rows=phase1_run.state_rows,
        setups=setups,
        refined_prefill_profile_name=PREFILL_PROFILE_PERSISTENCE_SHORT,
    )
    refined_eval = next(row for row in evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.accepted is True
    assert "prefill_recovered_after_warning" in refined_eval.reason_tags


def test_prefill_warning_window_hard_cancels_after_failed_recovery(tmp_path: Path) -> None:
    bars, mutate = _prefill_regime_warning_rows(recovered=False)
    phase1_run = run_asia_drift_phase1_from_bars(
        output_dir=tmp_path / "prefill_fail_phase1",
        bars_5m=bars,
        source_label="prefill_warning_failure",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    setups = assemble_entry_setups(feature_rows=phase1_run.feature_rows, state_rows=phase1_run.state_rows)
    evaluations = evaluate_entry_models(
        feature_rows=mutate(phase1_run.feature_rows),
        state_rows=phase1_run.state_rows,
        setups=setups,
        refined_prefill_profile_name=PREFILL_PROFILE_PERSISTENCE_SHORT,
    )
    refined_eval = next(row for row in evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.accepted is False
    assert refined_eval.cancellation_reason == "regime_lost_before_fill"
    assert "prefill_warning_failed_to_recover" in refined_eval.reason_tags


def test_prefill_profiles_preserve_deep_damage_rejection(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "prefill_deep_damage",
        bars_5m=_fast_deep_disqualifying_session(),
        source_label="prefill_deep_damage",
        calibration_profile_name=RECOVERY_CONFIRMED,
        refined_prefill_profile_name=PREFILL_PROFILE_PERSISTENCE_MEDIUM,
    )
    refined_eval = next(row for row in run.entry_evaluations if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED)
    assert refined_eval.accepted is False
    assert refined_eval.cancellation_reason in {"fast_deep_disqualifying", "depth_exceeds_limit", "pullback_disqualified"}


def test_prefill_regime_loss_audit_is_emitted(tmp_path: Path) -> None:
    run = run_asia_drift_phase2_from_bars(
        output_dir=tmp_path / "prefill_audit",
        bars_5m=_stable_armed_session(),
        source_label="prefill_audit",
        calibration_profile_name=RECOVERY_CONFIRMED,
    )
    audit = run.diagnostics["prefill_regime_loss_audit"]
    assert "event_count" in audit
    assert "source_category_counts" in audit


def test_prefill_profile_comparison_artifact_shape(tmp_path: Path) -> None:
    result = run_prefill_persistence_comparison_from_bars(
        output_dir=tmp_path / "prefill_compare",
        primary_instrument="MGC",
        windows=(
            AsiaDriftReplayBarWindow(label="window_a", bars_5m=_fast_shallow_valid_session()),
            AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
        ),
        profile_names=(
            PREFILL_PROFILE_RECOVERY_CONFIRMED,
            PREFILL_PROFILE_PERSISTENCE_SHORT,
        ),
    )
    payload = result["payload"]
    assert len(payload["profile_rows"]) == 2
    assert "recommendation" in payload
    assert DEFAULT_PREFILL_COMPARISON_PROFILES


def test_pattern_discovery_taxonomy_output_shape(tmp_path: Path) -> None:
    result = run_asia_drift_pattern_discovery_from_bars(
        output_dir=tmp_path / "pattern_discovery_shape",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    payload = result["payload"]
    assert "candidate_pattern_taxonomy" in payload
    assert "top_candidate_patterns" in payload
    assert "recommended_next_research_queue" in payload
    assert payload["session_pattern_manifest"]
    row = payload["candidate_pattern_taxonomy"][0]
    assert "pattern_name" in row
    assert "plain_english_description" in row
    assert "measurable_definition" in row
    assert "confidence_level" in row


def test_pattern_discovery_feature_summary_generation_shape(tmp_path: Path) -> None:
    result = run_asia_drift_pattern_discovery_from_bars(
        output_dir=tmp_path / "pattern_discovery_features",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
            "MES": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),),
        },
    )
    summary = result["payload"]["feature_family_summary"]
    assert "metals" in summary
    assert "indices" in summary
    assert "directional_continuation_rate" in summary["metals"]
    assert "pattern_breakdown" in summary["indices"]


def test_pattern_discovery_clustering_risk_is_high_when_one_window_dominates(tmp_path: Path) -> None:
    result = run_asia_drift_pattern_discovery_from_bars(
        output_dir=tmp_path / "pattern_discovery_cluster",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
        },
    )
    taxonomy = result["payload"]["candidate_pattern_taxonomy"]
    compression_row = next(row for row in taxonomy if row["pattern_name"] == "COMPRESSION_THEN_EXPANSION")
    assert compression_row["clustering_risk"] == "HIGH"
    assert compression_row["clustered_in_one_window_only"] is True


def test_pattern_discovery_runner_stays_research_only(tmp_path: Path) -> None:
    result = run_asia_drift_pattern_discovery_from_bars(
        output_dir=tmp_path / "pattern_discovery_research_only",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
            "NQ": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_fast_deep_disqualifying_session(), "NQ")),),
        },
    )
    artifacts = result["artifacts"]
    assert str(Path(artifacts["summary_json_path"]).resolve()).startswith(str(tmp_path))
    assert Path(artifacts["research_queue_path"]).exists()


def test_open_cluster_feature_matrix_generation_shape(tmp_path: Path) -> None:
    result = run_asia_drift_open_cluster_discovery_from_bars(
        output_dir=tmp_path / "open_cluster_features",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    payload = result["payload"]
    assert payload["feature_matrix_rows"]
    row = payload["feature_matrix_rows"][0]
    assert "instrument_family" in row
    assert "abs_session_displacement_atr" in row
    assert "compression_duration_fraction" in row
    assert "cross_asset_sync_count" in row


def test_open_cluster_assignment_output_shape(tmp_path: Path) -> None:
    result = run_asia_drift_open_cluster_discovery_from_bars(
        output_dir=tmp_path / "open_cluster_assignments",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    rows = result["payload"]["cluster_assignment_rows"]
    assert rows
    assert "cluster_id" in rows[0]
    assert "cluster_interpretive_name" in rows[0]
    assert "cluster_status" in rows[0]


def test_open_cluster_outcome_overlay_shape(tmp_path: Path) -> None:
    result = run_asia_drift_open_cluster_discovery_from_bars(
        output_dir=tmp_path / "open_cluster_overlays",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    summary = result["payload"]["cluster_summaries"][0]
    assert "follow_through_tendency" in summary
    assert "reversal_rate" in summary
    assert "contamination_risk" in summary
    assert "feature_centroid" in summary


def test_open_cluster_template_vs_cluster_comparison_shape(tmp_path: Path) -> None:
    result = run_asia_drift_open_cluster_discovery_from_bars(
        output_dir=tmp_path / "open_cluster_template_compare",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_shallow_valid_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    comparison = result["payload"]["template_vs_cluster_comparison"]
    assert "cluster_to_template" in comparison
    assert "template_to_clusters" in comparison
    assert "templates_cut_across_multiple_clusters" in comparison
    assert "natural_clusters_missed_or_only_loosely_captured_by_templates" in comparison


def test_open_cluster_runner_stays_research_only(tmp_path: Path) -> None:
    result = run_asia_drift_open_cluster_discovery_from_bars(
        output_dir=tmp_path / "open_cluster_research_only",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
            "NQ": (AsiaDriftReplayBarWindow(label="window_b", bars_5m=_with_instrument(_fast_deep_disqualifying_session(), "NQ")),),
        },
    )
    artifacts = result["artifacts"]
    assert str(Path(artifacts["summary_json_path"]).resolve()).startswith(str(tmp_path))
    assert Path(artifacts["feature_matrix_path"]).exists()
    assert Path(artifacts["cluster_assignments_path"]).exists()
    assert Path(artifacts["template_comparison_markdown_path"]).exists()


def test_cross_asset_confirmation_emits_session_labels_and_tiers(tmp_path: Path) -> None:
    result = run_cross_asset_confirmation_from_bars(
        output_dir=tmp_path / "cross_asset_confirmation_shape",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "ES": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "ES")),
            ),
            "NQ": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_fast_deep_disqualifying_session(), "NQ")),
            ),
        },
    )
    payload = result["payload"]
    assert payload["session_confirmation_rows"]
    row = payload["session_confirmation_rows"][0]
    assert "confirmation_tier" in row
    assert "lead_lag_category" in row
    assert "signed_vwap_alignment" in row
    assert "metals_only_signal" in row
    assert payload["tier_summary"]


def test_cross_asset_confirmation_baseline_and_lead_lag_shapes(tmp_path: Path) -> None:
    result = run_cross_asset_confirmation_from_bars(
        output_dir=tmp_path / "cross_asset_confirmation_baselines",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "GC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "GC")),
            ),
            "ES": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "ES")),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    payload = result["payload"]
    baseline_row = payload["baseline_comparison"][0]
    assert "group_name" in baseline_row
    assert "directional_resolution_rate" in baseline_row
    assert "contamination_rate" in baseline_row
    lead_lag = payload["lead_lag_report"]
    assert "overall_distribution" in lead_lag
    assert "resolution_rate_by_category" in lead_lag


def test_cross_asset_confirmation_runner_stays_research_only(tmp_path: Path) -> None:
    result = run_cross_asset_confirmation_from_bars(
        output_dir=tmp_path / "cross_asset_confirmation_research_only",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
            "ES": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_with_instrument(_compression_continuation_session(), "ES")),),
        },
    )
    artifacts = result["artifacts"]
    assert str(Path(artifacts["summary_json_path"]).resolve()).startswith(str(tmp_path))
    assert Path(artifacts["tier_summary_json_path"]).exists()
    assert Path(artifacts["lead_lag_json_path"]).exists()


def test_multi_year_discovery_feature_matrix_and_cluster_shape(tmp_path: Path) -> None:
    result = run_asia_drift_multi_year_discovery_from_bars(
        output_dir=tmp_path / "multi_year_shape",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    payload = result["payload"]
    assert payload["feature_matrix_rows"]
    assert payload["cluster_assignment_rows"]
    assert payload["cluster_summaries"]
    assert "coverage_report" in payload
    assert "actual_cluster_count" in payload


def test_multi_year_discovery_positive_vs_control_and_discriminator_shape(tmp_path: Path) -> None:
    result = run_asia_drift_multi_year_discovery_from_bars(
        output_dir=tmp_path / "multi_year_directional_analysis",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_shallow_valid_session()),
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_d", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    payload = result["payload"]
    comparison = payload["positive_vs_control_comparison"]
    assert "positive_session_count" in comparison
    assert "matched_control_session_count" in comparison
    assert "matching_rows" in comparison
    assert "future_leakage_safe" in comparison
    discriminator_row = payload["discriminator_analysis"][0]
    assert "feature_name" in discriminator_row
    assert "standardized_effect" in discriminator_row


def test_multi_year_discovery_candidate_condition_set_shape(tmp_path: Path) -> None:
    result = run_asia_drift_multi_year_discovery_from_bars(
        output_dir=tmp_path / "multi_year_condition_sets",
        instrument_windows={
            "MGC": (
                AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),
                AsiaDriftReplayBarWindow(label="window_b", bars_5m=_fast_shallow_valid_session()),
                AsiaDriftReplayBarWindow(label="window_c", bars_5m=_fast_deep_disqualifying_session()),
            ),
            "MES": (
                AsiaDriftReplayBarWindow(label="window_d", bars_5m=_with_instrument(_compression_continuation_session(), "MES")),
            ),
        },
    )
    condition_rows = result["payload"]["candidate_condition_sets"]
    assert condition_rows
    row = condition_rows[0]
    assert "condition_set_name" in row
    assert "positive_hit_rate" in row
    assert "false_positive_rate" in row
    assert "future_leakage_safe" in row


def test_multi_year_discovery_runner_stays_research_only(tmp_path: Path) -> None:
    result = run_asia_drift_multi_year_discovery_from_bars(
        output_dir=tmp_path / "multi_year_research_only",
        instrument_windows={
            "MGC": (AsiaDriftReplayBarWindow(label="window_a", bars_5m=_compression_continuation_session("MGC")),),
            "NQ": (AsiaDriftReplayBarWindow(label="window_b", bars_5m=_with_instrument(_fast_deep_disqualifying_session(), "NQ")),),
        },
    )
    artifacts = result["artifacts"]
    assert str(Path(artifacts["summary_json_path"]).resolve()).startswith(str(tmp_path))
    assert Path(artifacts["positive_control_json_path"]).exists()
    assert Path(artifacts["candidate_condition_sets_json_path"]).exists()


def test_cross_asset_trade_mapping_maps_trade_to_session_without_future_leakage(tmp_path: Path) -> None:
    confirmation_rows = [
        {
            "primary_session_id": "mgc__2026-04-15__asia_drift_v1",
            "primary_instrument": "MGC",
            "local_session_date": "2026-04-15",
            "confirmation_tier": "TIER_A_STRONG_ALIGNMENT",
            "contamination_flag": False,
            "confirmation_partner_instrument": "ES",
            "confirmation_partner_session_id": "es__2026-04-15__asia_drift_v1",
            "direction_agreement": True,
            "signed_vwap_alignment": True,
            "directional_efficiency_alignment": True,
            "drift_context_agreement": True,
            "impulse_timing_agreement": True,
            "failed_mean_reversion_alignment": True,
            "vwap_noise_suppressed": True,
            "lead_lag_category": "SIMULTANEOUS",
            "lead_asset": "SIMULTANEOUS",
            "directional_resolution": True,
            "continuation_magnitude_atr": 4.2,
        }
    ]
    availability = {
        "mgc__2026-04-15__asia_drift_v1": {"early_window_end_ts": "2026-04-15T23:00:00+00:00"},
        "es__2026-04-15__asia_drift_v1": {"early_window_end_ts": "2026-04-15T23:05:00+00:00"},
    }
    early_features = {
        "mgc__2026-04-15__asia_drift_v1": {
            "asia_drift_session_id": "mgc__2026-04-15__asia_drift_v1",
            "early_signed_vwap_displacement_peak": "1.2",
            "early_drift_context_fraction": "0.5",
            "early_failed_countertrend_rate": "0.2",
            "early_vwap_reclaim_rate": "0.0",
            "early_post_spike_fraction": "0.0",
            "early_deep_damage_fraction": "0.0",
        }
    }
    trade_rows = [
        {
            "source_name": "synthetic_closed_trades",
            "trade_id": "after",
            "lane_id": "lane_a",
            "instrument": "MGC",
            "entry_ts": "2026-04-15T19:10:00-04:00",
            "exit_ts": "2026-04-15T19:30:00-04:00",
            "side": "LONG",
            "net_pnl_cash": 25.0,
            "hold_minutes": 20.0,
            "outcome_available": True,
        },
        {
            "source_name": "synthetic_closed_trades",
            "trade_id": "before",
            "lane_id": "lane_a",
            "instrument": "MGC",
            "entry_ts": "2026-04-15T18:30:00-04:00",
            "exit_ts": "2026-04-15T18:45:00-04:00",
            "side": "LONG",
            "net_pnl_cash": -10.0,
            "hold_minutes": 15.0,
            "outcome_available": True,
        },
    ]
    result = run_cross_asset_trade_mapping_from_rows(
        output_dir=tmp_path / "cross_asset_trade_mapping_leakage",
        confirmation_rows=confirmation_rows,
        availability_by_session=availability,
        early_features_by_session=early_features,
        trade_inventory_rows=[{"source_name": "synthetic_closed_trades", "trade_count": 2}],
        trade_rows=trade_rows,
        recent_session_count=10,
    )
    mapped = result["payload"]["mapped_trade_rows"]
    after_row = next(row for row in mapped if row["trade_id"] == "after")
    before_row = next(row for row in mapped if row["trade_id"] == "before")
    assert after_row["pre_entry_signal_available"] is True
    assert after_row["pre_entry_confirmation_tier"] == "TIER_A_STRONG_ALIGNMENT"
    assert after_row["early_signed_vwap_displacement_peak"] == "1.2"
    assert before_row["pre_entry_signal_available"] is False
    assert before_row["pre_entry_confirmation_bucket"] == "INSUFFICIENT_SIGNAL"
    assert before_row["pre_entry_confirmation_tier"] is None


def test_cross_asset_trade_mapping_tier_summary_and_recent_shape(tmp_path: Path) -> None:
    confirmation_rows = [
        {
            "primary_session_id": "gc__2026-04-12__asia_drift_v1",
            "primary_instrument": "GC",
            "local_session_date": "2026-04-12",
            "confirmation_tier": "TIER_B_MODERATE_ALIGNMENT",
            "contamination_flag": False,
            "confirmation_partner_instrument": "MES",
            "confirmation_partner_session_id": "mes__2026-04-12__asia_drift_v1",
            "direction_agreement": True,
            "signed_vwap_alignment": True,
            "directional_efficiency_alignment": True,
            "drift_context_agreement": False,
            "impulse_timing_agreement": False,
            "failed_mean_reversion_alignment": True,
            "vwap_noise_suppressed": True,
            "lead_lag_category": "ES_MES_LEAD",
            "lead_asset": "ES_MES",
            "directional_resolution": True,
            "continuation_magnitude_atr": 2.4,
        },
        {
            "primary_session_id": "gc__2026-04-16__asia_drift_v1",
            "primary_instrument": "GC",
            "local_session_date": "2026-04-16",
            "confirmation_tier": "NO_CONFIRMATION",
            "contamination_flag": True,
            "confirmation_partner_instrument": "ES",
            "confirmation_partner_session_id": "es__2026-04-16__asia_drift_v1",
            "direction_agreement": False,
            "signed_vwap_alignment": False,
            "directional_efficiency_alignment": False,
            "drift_context_agreement": False,
            "impulse_timing_agreement": False,
            "failed_mean_reversion_alignment": False,
            "vwap_noise_suppressed": False,
            "lead_lag_category": "SIMULTANEOUS",
            "lead_asset": "SIMULTANEOUS",
            "directional_resolution": False,
            "continuation_magnitude_atr": 0.4,
        },
    ]
    availability = {
        "gc__2026-04-12__asia_drift_v1": {"early_window_end_ts": "2026-04-12T23:00:00+00:00"},
        "mes__2026-04-12__asia_drift_v1": {"early_window_end_ts": "2026-04-12T23:00:00+00:00"},
        "gc__2026-04-16__asia_drift_v1": {"early_window_end_ts": "2026-04-16T23:00:00+00:00"},
        "es__2026-04-16__asia_drift_v1": {"early_window_end_ts": "2026-04-16T23:00:00+00:00"},
    }
    trade_rows = [
        {
            "source_name": "synthetic",
            "trade_id": "good",
            "lane_id": "lane_good",
            "instrument": "GC",
            "entry_ts": "2026-04-12T19:10:00-04:00",
            "exit_ts": "2026-04-12T19:25:00-04:00",
            "side": "LONG",
            "net_pnl_cash": 100.0,
            "hold_minutes": 15.0,
            "outcome_available": True,
        },
        {
            "source_name": "synthetic",
            "trade_id": "bad",
            "lane_id": "lane_bad",
            "instrument": "GC",
            "entry_ts": "2026-04-16T19:10:00-04:00",
            "exit_ts": "2026-04-16T19:30:00-04:00",
            "side": "LONG",
            "net_pnl_cash": -50.0,
            "hold_minutes": 20.0,
            "outcome_available": True,
        },
    ]
    result = run_cross_asset_trade_mapping_from_rows(
        output_dir=tmp_path / "cross_asset_trade_mapping_shape",
        confirmation_rows=confirmation_rows,
        availability_by_session=availability,
        early_features_by_session={},
        trade_inventory_rows=[{"source_name": "synthetic", "trade_count": 2}],
        trade_rows=trade_rows,
        insufficient_rows=[{"source_name": "thin_snapshot", "reason": "no_outcome"}],
        recent_session_count=20,
    )
    payload = result["payload"]
    assert payload["tier_outcome_summary"]["mapped_trade_count"] == 2
    bucket_names = {row["bucket_name"] for row in payload["tier_outcome_summary"]["bucket_summaries"]}
    assert "CONFIRMED_TIER" in bucket_names
    assert "CONTAMINATED" in bucket_names
    assert payload["counterfactual_filter_summary"]["allowed_tier_ab_count"] == 1
    assert "status" in payload["recent_signal_summary"]
    assert payload["insufficient_data_report"]["insufficient_sources"][0]["source_name"] == "thin_snapshot"


def test_cross_asset_trade_mapping_runner_stays_research_only(tmp_path: Path) -> None:
    result = run_cross_asset_trade_mapping_from_rows(
        output_dir=tmp_path / "cross_asset_trade_mapping_research_only",
        confirmation_rows=[],
        availability_by_session={},
        early_features_by_session={},
        trade_inventory_rows=[{"source_name": "synthetic", "trade_count": 0}],
        trade_rows=[],
        recent_session_count=10,
    )
    payload = result["payload"]
    assert "research-only" in payload["objective"].lower()
    assert payload["research_only"] is True
    assert payload["offline_diagnostic"] is True
    assert payload["not_runtime_authority"] is True
    assert payload["not_broker_truth"] is True
    assert payload["not_market_data_runtime_truth"] is True
    assert payload["not_routing_authority"] is True
    assert payload["research_offline_metadata"]["source_category"] == "research/offline"
    assert payload["recommendation"]["recommendation"] == "more_paper_collection_only"


def test_data_continuity_audit_compresses_contiguous_date_ranges() -> None:
    ranges = _compress_date_ranges(
        ["2026-03-01", "2026-03-02", "2026-03-04", "2026-03-05", "2026-03-07"]
    )
    assert ranges == [
        {"start": "2026-03-01", "end": "2026-03-02", "count": 2},
        {"start": "2026-03-04", "end": "2026-03-05", "count": 2},
        {"start": "2026-03-07", "end": "2026-03-07", "count": 1},
    ]


def test_data_continuity_audit_monthly_gap_rows_reports_missing_months() -> None:
    gaps = _monthly_gap_rows(
        months=["2025-11", "2026-01", "2026-03"],
        start_month="2025-11",
        end_month="2026-03",
    )
    assert gaps == [{"gap_month": "2025-12"}, {"gap_month": "2026-02"}]


def test_data_continuity_audit_root_cause_flags_trade_discontinuity() -> None:
    replay_audit = {
        "coverage_rows": [
            {
                "symbol": "GC",
                "timeframe": "1m",
                "data_source": "historical_1m_canonical",
                "earliest_ts": "2024-01-01T18:01:00-05:00",
                "latest_ts": "2026-04-20T23:59:00-04:00",
                "bar_count": 1,
            }
        ]
    }
    warehouse_audit = {
        "dataset_reports": {
            "raw_bars_1m": {
                "overall_rows": [
                    {
                        "symbol": "GC",
                        "latest_ts": "2025-12-28T23:59:00-05:00",
                    }
                ]
            }
        }
    }
    trade_audit = {
        "latest_trade_ts": "2025-12-28T19:35:00-05:00",
    }
    alignment = {
        "latest_replay_canonical_ts": "2026-04-20T23:59:00-04:00",
        "latest_trade_artifact_ts": "2025-12-28T19:35:00-05:00",
        "data_source_overlap_after_2026_02": [{"symbol": "GC", "month": "2026-03"}],
    }

    root_cause = _root_cause_summary(
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_audit=trade_audit,
        alignment=alignment,
    )

    assert root_cause["verdict"] == "trade_artifact_discontinuity_confirmed"
    assert root_cause["replay_extends_beyond_trades"] is True
    assert root_cause["warehouse_stops_before_replay"] is True
    assert root_cause["mixed_source_replay_overlap_present"] is True
    assert "invalidated" in root_cause["march_april_clustering_assessment"]
