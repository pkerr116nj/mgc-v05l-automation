from mgc_v05l.app.mgc_impulse_paper_executable_feasibility import (
    VERDICT,
    _causality_audit,
    _lane_isolation_proof,
    _multi_timeframe_feasibility,
)


def test_feasibility_verdict_is_not_executable_as_frozen() -> None:
    assert VERDICT == "NOT_EXECUTABLE_AS_FROZEN"


def test_multi_timeframe_and_causality_audits_flag_hard_blockers() -> None:
    mtf = _multi_timeframe_feasibility()
    causal = _causality_audit()

    assert mtf["answer"] == "NO"
    assert causal["answer"] == "NO"
    assert "post-trigger" in causal["lookahead_verdict"]


def test_lane_isolation_is_supported_even_though_admission_is_blocked() -> None:
    isolation = _lane_isolation_proof()
    assert isolation["answer"] == "YES"
