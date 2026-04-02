from decimal import Decimal

from mgc_v05l.app.open_late_additive_separator_analysis import (
    OpenLateObservation,
    _compression_label,
    _extension_state,
    _rank_numeric_differences,
)


def _observation(**overrides) -> OpenLateObservation:
    base = {
        "cohort": "profitable_additive_us_open_late",
        "source": "trade",
        "timestamp": overrides.pop("timestamp"),
        "slice_name": "recent",
        "session_phase": "US_OPEN_LATE",
        "time_bucket": "10:15",
        "net_pnl": Decimal("10"),
        "bars_held": 2,
        "exit_reason": "SHORT_INTEGRITY_FAIL",
        "entry_efficiency_5": Decimal("80"),
        "entry_distance_vwap_atr": Decimal("-1.2"),
        "entry_distance_fast_ema_atr": Decimal("-0.4"),
        "entry_distance_slow_ema_atr": Decimal("-0.5"),
        "signal_body_atr": Decimal("0.6"),
        "signal_range_atr": Decimal("1.0"),
        "signal_close_location": Decimal("0.2"),
        "normalized_slope": Decimal("-0.2"),
        "normalized_curvature": Decimal("-0.2"),
        "prior_2_bar_avg_slope": Decimal("-0.05"),
        "prior_3_bar_avg_slope": Decimal("-0.03"),
        "prior_5_bar_avg_slope": Decimal("-0.02"),
        "prior_2_bar_avg_curvature": Decimal("-0.04"),
        "prior_3_bar_avg_curvature": Decimal("-0.03"),
        "prior_5_bar_avg_curvature": Decimal("-0.02"),
        "prior_1_bar_vwap_extension": Decimal("-0.4"),
        "prior_3_bar_avg_vwap_extension": Decimal("-0.5"),
        "prior_3_bar_min_vwap_extension": Decimal("-0.6"),
        "prior_3_bar_drop_from_high_atr": Decimal("0.8"),
        "prior_3_bar_avg_range_atr": Decimal("0.4"),
        "prior_3_bar_avg_body_atr": Decimal("0.2"),
        "compression_before_signal": "compressed_then_expand",
        "extension_state": "pause_then_accelerate",
        "followthrough_1bar": Decimal("6"),
        "followthrough_2bar": Decimal("10"),
        "followthrough_3bar": Decimal("12"),
        "adverse_1bar": Decimal("1"),
        "adverse_2bar": Decimal("1"),
        "adverse_3bar": Decimal("2"),
    }
    base.update(overrides)
    return OpenLateObservation(**base)


def test_extension_state_identifies_continuous_extension() -> None:
    prior = [
        {"normalized_slope": Decimal("-0.2"), "vwap_extension": Decimal("-1.0")},
        {"normalized_slope": Decimal("-0.3"), "vwap_extension": Decimal("-1.2")},
        {"normalized_slope": Decimal("-0.4"), "vwap_extension": Decimal("-1.4")},
    ]
    current = {"normalized_curvature": Decimal("-0.2"), "vwap_extension": Decimal("-1.5")}

    assert _extension_state(prior, current) == "continuous_extension"


def test_compression_label_identifies_compressed_then_expand() -> None:
    prior = [
        {"signal_range_atr": Decimal("0.4"), "signal_body_atr": Decimal("0.2")},
        {"signal_range_atr": Decimal("0.5"), "signal_body_atr": Decimal("0.2")},
        {"signal_range_atr": Decimal("0.4"), "signal_body_atr": Decimal("0.3")},
    ]

    assert _compression_label(prior, Decimal("1.0"), Decimal("0.6")) == "compressed_then_expand"


def test_rank_numeric_differences_surfaces_followthrough_gap() -> None:
    from datetime import datetime

    good = [
        _observation(timestamp=datetime.fromisoformat("2026-03-12T10:15:00-04:00")),
        _observation(timestamp=datetime.fromisoformat("2026-02-10T10:25:00-05:00"), followthrough_3bar=Decimal("20")),
    ]
    weak = [
        _observation(
            timestamp=datetime.fromisoformat("2025-11-17T10:00:00-05:00"),
            cohort="weak_middle_us_open_late",
            followthrough_3bar=Decimal("4"),
            prior_3_bar_avg_range_atr=Decimal("0.9"),
        )
    ]

    ranked = _rank_numeric_differences(good, weak)

    assert ranked[0]["feature"] in {"followthrough_3bar", "prior_3_bar_avg_range_atr"}
