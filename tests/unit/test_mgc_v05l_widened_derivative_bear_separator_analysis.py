from mgc_v05l.app.widened_derivative_bear_separator_analysis import best_next_gate_hypothesis


def test_best_next_gate_hypothesis_returns_concrete_band_gate() -> None:
    hypothesis = best_next_gate_hypothesis(
        {
            "best_next_gate_hypothesis": (
                "Test a VWAP-extension band only for US_OPEN_LATE widened trades."
            )
        }
    )

    assert "VWAP-extension band" in hypothesis
    assert "US_OPEN_LATE" in hypothesis
