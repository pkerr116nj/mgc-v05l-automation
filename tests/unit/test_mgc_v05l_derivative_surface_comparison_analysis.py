from mgc_v05l.app.derivative_surface_comparison_analysis import _comparison_verdict


def test_comparison_verdict_marks_both_inconclusive_when_neither_surface_helps() -> None:
    verdict = _comparison_verdict(
        {"london_late": {"helpful": False}},
        {"london_late": {"helpful": False}},
    )

    assert verdict["verdict"] == "both inconclusive"
    assert verdict["should_continue_prioritizing_3m_now"] is False
