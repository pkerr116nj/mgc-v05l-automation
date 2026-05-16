from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_exit_context_cli import LATEST_EXIT_CONTEXT_FILENAME, main
from mgc_v05l.execution_core.track_b_exit_context import (
    DEFENSIVE_TIGHT,
    MALFORMED_CANDLES,
    MIXED_TIMEFRAME,
    NO_EXIT_LOW_CONFIDENCE,
    PARTICIPATION_COLLAPSE_EXIT,
    PATIENT_CONTINUATION,
    STALE_INPUT,
    THIN_DATA,
    TIME_BOXED,
)


FIXTURE_ROOT = Path("tests/fixtures/exit_context")
FRESH_NOW = "2026-05-08T15:00:00+00:00"


@pytest.mark.parametrize(
    ("fixture_name", "expected_profile", "expected_urgency"),
    [
        ("healthy_pullback_no_exit.json", PATIENT_CONTINUATION, "LOW"),
        ("impulse_decay_elevated_urgency.json", DEFENSIVE_TIGHT, "ELEVATED"),
        ("participation_collapse_high_urgency.json", PARTICIPATION_COLLAPSE_EXIT, "HIGH"),
        ("time_box_expiry.json", TIME_BOXED, "ELEVATED"),
    ],
)
def test_cli_writes_advisory_exit_context_for_offline_fixtures(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    fixture_name: str,
    expected_profile: str,
    expected_urgency: str,
) -> None:
    output_dir = tmp_path / fixture_name.removesuffix(".json")

    exit_code = main(["--input-json", str(FIXTURE_ROOT / fixture_name), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_EXIT_CONTEXT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["wrote_artifact"] is True
    assert captured["exit_profile_context"] == expected_profile
    assert captured["exit_urgency_context"] == expected_urgency
    assert written["exit_profile_context"] == expected_profile
    assert written["strategy_authority"] is False
    assert written["broker_state_mutated"] is False
    assert written["submit_attempted"] is False
    assert written["order_intent_created"] is False
    assert written["lifecycle_mutated"] is False
    assert written["runtime_trade_eligible"] is False


def test_cli_defaults_missing_provenance_to_test_fixture_mode(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(FIXTURE_ROOT / "healthy_pullback_no_exit.json"), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_EXIT_CONTEXT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["failure_reasons"] == []
    assert written["input_mode"] == "TEST"
    assert written["input_source_category"] == "TEST_FIXTURE"


def test_cli_refuses_stale_thin_data_without_writing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output_dir = tmp_path / "out"

    exit_code = main(
        [
            "--input-json",
            str(FIXTURE_ROOT / "low_confidence_stale_thin_data.json"),
            "--output-dir",
            str(output_dir),
            "--now",
            "2026-05-08T16:00:00+00:00",
        ]
    )
    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert captured["wrote_artifact"] is False
    assert captured["exit_profile_context"] == NO_EXIT_LOW_CONFIDENCE
    assert STALE_INPUT in captured["failure_reasons"]
    assert THIN_DATA in captured["failure_reasons"]
    assert not (output_dir / LATEST_EXIT_CONTEXT_FILENAME).exists()


def test_cli_refuses_research_source_presented_as_runtime_truth(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = _load_fixture("healthy_pullback_no_exit.json")
    payload["input_mode"] = "RUNTIME_DECISION"
    payload["input_source_category"] = "RESEARCH"
    payload["input_source_path"] = "outputs/track_b_research/latest.json"
    input_path = tmp_path / "research_runtime.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert captured["wrote_artifact"] is False
    assert "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE" in captured["failure_reasons"]
    assert not (output_dir / LATEST_EXIT_CONTEXT_FILENAME).exists()


def test_cli_refuses_malformed_input_without_writing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = _load_fixture("healthy_pullback_no_exit.json")
    del payload["candles"][3]["high"]
    input_path = tmp_path / "malformed.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert captured["wrote_artifact"] is False
    assert MALFORMED_CANDLES in captured["failure_reasons"]
    assert not (output_dir / LATEST_EXIT_CONTEXT_FILENAME).exists()


def test_cli_refuses_mixed_timeframe_input_without_writing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = _load_fixture("healthy_pullback_no_exit.json")
    payload["timeframe_context"]["timeframe_alignment_status"] = "MIXED"
    input_path = tmp_path / "mixed_timeframe.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert captured["wrote_artifact"] is False
    assert MIXED_TIMEFRAME in captured["failure_reasons"]
    assert not (output_dir / LATEST_EXIT_CONTEXT_FILENAME).exists()


def test_cli_has_no_broker_runtime_lifecycle_reconciliation_imports() -> None:
    source = Path("src/mgc_v05l/app/track_b_exit_context_cli.py").read_text(encoding="utf-8")

    assert "ibapi" not in source
    assert "mgc_v05l.execution." not in source
    assert "mgc_v05l.strategy" not in source
    assert "run_operator" not in source
    assert "paper_proof" not in source
    assert "placeOrder" not in source
    assert "create_order_intent" not in source
    assert "mutate_lifecycle" not in source


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))
