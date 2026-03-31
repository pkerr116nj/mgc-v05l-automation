from __future__ import annotations

import json

from mgc_v05l.app.uslate_pause_resume_long_weak_middle_diagnostic import (
    LATEST_JSON_PATH,
    build_report,
    write_outputs,
)


def _load_payload() -> dict:
    payload = build_report()
    write_outputs(payload)
    return json.loads(LATEST_JSON_PATH.read_text(encoding="utf-8"))


def test_weak_middle_diagnostic_writes_expected_verdict() -> None:
    payload = _load_payload()

    assert payload["verdict_bucket"] == "WEAK_MIDDLE_DIAGNOSIS_CONFIRMED"
    assert payload["direct_answers"]["single_best_explanation_for_the_weak_middle"]


def test_weak_middle_diagnostic_confirms_gc_is_harsher_same_shape() -> None:
    payload = _load_payload()

    assert "harsher copy" in payload["cross_metal_comparison"]["is_gc_harsher_copy_or_different"]
    assert payload["metals"]["GC"]["economic_replay_quality"]["survives_without_top_1"] is False
    assert payload["metals"]["MGC"]["economic_replay_quality"]["survives_without_top_1"] is True


def test_weak_middle_diagnostic_reports_separator_lists() -> None:
    payload = _load_payload()

    mgc_separators = payload["metals"]["MGC"]["best_diagnostic_separators"]
    assert len(mgc_separators["standout_winners_vs_weak_middle"]) >= 1
    assert len(mgc_separators["poison_losers_vs_weak_middle"]) >= 1
