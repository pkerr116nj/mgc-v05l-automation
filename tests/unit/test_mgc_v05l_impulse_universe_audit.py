from collections import Counter

from mgc_v05l.app.mgc_impulse_universe_audit import (
    _audit_verdict,
    _key_composition_differences,
    _screening_layers,
)
from mgc_v05l.app.mgc_impulse_burst_continuation_second_pass import BASE_SPEC, REFINEMENT_SPECS


def test_screening_layers_capture_agreement_and_breadth_narrowing() -> None:
    screened = next(spec for spec in REFINEMENT_SPECS if spec.variant_name == "breadth_plus_agreement_combo")
    layers = _screening_layers(base_spec=BASE_SPEC, screened_spec=screened)
    assert layers[0]["layer"] == "agreement_tightening"
    assert any(item["metric"] == "largest_bar_share_max" for item in layers[1]["filters"])


def test_key_composition_differences_reports_executable_shift() -> None:
    diff = _key_composition_differences(
        {
            "original_broader_universe": {
                "subclass_mix": {"A": {"count": 5, "share": 0.5}},
                "spike_subtype_mix": {"BAD_SPIKE_TRAP": {"count": 2, "share": 0.2}},
                "disproportionately_executable_characteristics": {
                    "same_bar_proxy_pass_rate": 0.3,
                    "delayed_confirmation_satisfied_rate": 0.4,
                },
                "compact_benchmark_checks": {
                    "delayed_confirmation_next_open_metrics": {"profit_factor": 1.0, "median_trade": -4.0}
                },
            },
            "screened_branch_universe": {
                "subclass_mix": {"A": {"count": 3, "share": 0.3}},
                "spike_subtype_mix": {"BAD_SPIKE_TRAP": {"count": 1, "share": 0.1}},
                "disproportionately_executable_characteristics": {
                    "same_bar_proxy_pass_rate": 0.25,
                    "delayed_confirmation_satisfied_rate": 0.35,
                },
                "compact_benchmark_checks": {
                    "delayed_confirmation_next_open_metrics": {"profit_factor": 1.1, "median_trade": -3.0}
                },
            },
            "excluded_by_screening": {
                "subclass_mix": {"A": {"count": 2, "share": 0.6}},
                "spike_subtype_mix": {"BAD_SPIKE_TRAP": {"count": 1, "share": 0.3}},
                "disproportionately_executable_characteristics": {
                    "same_bar_proxy_pass_rate": 0.4,
                    "delayed_confirmation_satisfied_rate": 0.45,
                },
                "compact_benchmark_checks": {
                    "delayed_confirmation_next_open_metrics": {"profit_factor": 1.4, "median_trade": 1.0}
                },
            },
        }
    )
    assert diff["executable_plausibility_shift"]["same_bar_proxy_pass_rate_delta_excluded_minus_screened"] == 0.15


def test_audit_verdict_marks_mixed_bias_when_signals_conflict() -> None:
    verdict = _audit_verdict(
        population_summary={
            "screened_branch_universe": {
                "spike_subtype_mix": {"BAD_SPIKE_TRAP": {"count": 2, "share": 0.1}},
                "disproportionately_executable_characteristics": {
                    "same_bar_proxy_pass_rate": 0.30,
                },
                "compact_benchmark_checks": {
                    "delayed_confirmation_next_open_metrics": {"profit_factor": 1.1, "median_trade": -3.0}
                },
            },
            "excluded_by_screening": {
                "spike_subtype_mix": {"BAD_SPIKE_TRAP": {"count": 4, "share": 0.2}},
                "disproportionately_executable_characteristics": {
                    "same_bar_proxy_pass_rate": 0.34,
                },
                "compact_benchmark_checks": {
                    "delayed_confirmation_next_open_metrics": {"profit_factor": 1.18, "median_trade": -2.5}
                },
            },
        }
    )
    assert verdict == "MIXED_BIAS_ONE_MORE_ORIGINAL_UNIVERSE_PASS_JUSTIFIED"
