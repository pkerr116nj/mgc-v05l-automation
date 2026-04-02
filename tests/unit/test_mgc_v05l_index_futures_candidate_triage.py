from mgc_v05l.app.index_futures_candidate_triage import (
    PairMetrics,
    _cleaner_on_instrument,
    _more_promising_than_impulse,
    _verdict_bucket,
)


def test_cleaner_on_instrument_reports_unknown_without_direct_artifact() -> None:
    direct = PairMetrics(None, None, None, None, None, None, None, None, None, None, None, None, None)
    mgc = PairMetrics("a", "b", 10, 100.0, 10.0, 1.0, 2.0, 20.0, 0.5, 10.0, 20.0, True, True)
    assert _cleaner_on_instrument(direct, mgc) == "UNKNOWN_NO_DIRECT_INDEX_ARTIFACT"


def test_more_promising_than_impulse_marks_structural_but_unproven_without_direct_artifact() -> None:
    direct = PairMetrics(None, None, None, None, None, None, None, None, None, None, None, None, None)
    impulse = {"failed_same_bar_metrics": {"profit_factor": 1.1918, "median_trade": -8.0}}
    note = {"fit_regular_us_hours": True}
    assert _more_promising_than_impulse(direct, note, impulse, "NO_DIRECT_INDEX_REPLAY_ARTIFACT") == "STRUCTURALLY_MORE_EXECUTABLE_BUT_UNPROVEN"


def test_verdict_bucket_keeps_mnq_derivative_as_serious_candidate_without_direct_artifact() -> None:
    metrics = PairMetrics(None, None, None, None, None, None, None, None, None, None, None, None, None)
    assert _verdict_bucket(
        instrument="MNQ",
        family="usDerivativeBearTurn",
        evidence_status="NO_DIRECT_INDEX_REPLAY_ARTIFACT",
        metrics=metrics,
        cleaner_on_instrument="UNKNOWN_NO_DIRECT_INDEX_ARTIFACT",
    ) == "SERIOUS_NEXT_CANDIDATE"


def test_verdict_bucket_deprioritizes_negative_mes_us_late_portability() -> None:
    metrics = PairMetrics("a", "b", 19, -20.0, -1.05, -12.5, 0.931, 95.0, 0.36, -325.0, -812.5, False, False)
    assert _verdict_bucket(
        instrument="MES",
        family="usLatePauseResumeLongTurn",
        evidence_status="DIRECT_INDEX_REPLAY_AVAILABLE",
        metrics=metrics,
        cleaner_on_instrument="NO",
    ) == "DEPRIORITIZE"
