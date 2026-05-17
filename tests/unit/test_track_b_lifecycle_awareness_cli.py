from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_lifecycle_awareness_cli import LATEST_LIFECYCLE_AWARENESS_FILENAME, main
from mgc_v05l.execution_core.track_b_lifecycle_awareness import (
    ADVERSE_DOMINANCE,
    FAVORABLE_EXPANSION,
    HEALTHY_PULLBACK,
    LOW_CONFIDENCE_STALE_OR_UNRECONCILED,
    MALFORMED_CANDLES,
    MIXED_TIMEFRAME,
    STALE_INPUT,
    UNRECONCILED_POSITION_CONTEXT,
)


FIXTURE_ROOT = Path("tests/fixtures/lifecycle_awareness")
FRESH_NOW = "2026-05-08T14:40:00+00:00"


@pytest.mark.parametrize(
    ("fixture_name", "expected_state", "expected_hold"),
    [
        ("favorable_expansion.json", FAVORABLE_EXPANSION, "STRONG_HOLD"),
        ("healthy_pullback.json", HEALTHY_PULLBACK, "CONSTRUCTIVE_HOLD"),
        ("adverse_dominance.json", ADVERSE_DOMINANCE, "WEAK_HOLD"),
    ],
)
def test_cli_writes_lifecycle_awareness_for_offline_fixtures(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    fixture_name: str,
    expected_state: str,
    expected_hold: str,
) -> None:
    output_dir = tmp_path / fixture_name.removesuffix(".json")

    exit_code = main(["--input-json", str(FIXTURE_ROOT / fixture_name), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["wrote_artifact"] is True
    assert captured["offline_only"] is True
    assert captured["lifecycle_awareness_state"] == expected_state
    assert captured["hold_quality_context"] == expected_hold
    assert captured["failure_reasons"] == []
    assert written["lifecycle_awareness_state"] == expected_state
    assert written["hold_quality_context"] == expected_hold
    assert written["artifact_path"] == str((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).resolve())
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_stale_unreconciled_fixture(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "fail_closed"

    exit_code = main(
        [
            "--input-json",
            str(FIXTURE_ROOT / "stale_unreconciled_fail_closed.json"),
            "--output-dir",
            str(output_dir),
            "--now",
            "2026-05-08T16:00:00+00:00",
        ]
    )
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["wrote_artifact"] is True
    assert captured["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
    assert written["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
    assert STALE_INPUT in written["failure_reasons"]
    assert UNRECONCILED_POSITION_CONTEXT in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_malformed_input(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = _load_fixture("healthy_pullback.json")
    del payload["candles"][3]["high"]
    input_path = tmp_path / "malformed.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
    assert MALFORMED_CANDLES in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_mixed_timeframe_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _load_fixture("healthy_pullback.json")
    payload["timeframe_context"]["timeframe_alignment_status"] = "MIXED_TIMEFRAME_BLOCKED"
    payload["candles"][2]["timeframe"] = "1m"
    input_path = tmp_path / "mixed_timeframe.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
    assert MIXED_TIMEFRAME in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_defaults_missing_provenance_to_test_fixture_mode(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = _load_fixture("favorable_expansion.json")
    payload.pop("input_source_category", None)
    payload.pop("input_mode", None)
    input_path = tmp_path / "missing_provenance_defaults.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_LIFECYCLE_AWARENESS_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["failure_reasons"] == []
    assert written["input_mode"] == "TEST"
    assert written["input_source_category"] == "TEST_FIXTURE"


def test_cli_import_boundary() -> None:
    path = Path("src/mgc_v05l/app/track_b_lifecycle_awareness_cli.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    allowed_from_imports = {"mgc_v05l.execution_core.track_b_lifecycle_awareness"}
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "placeOrder",
        "create_order_intent",
        "mutate_lifecycle",
    }
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module not in allowed_from_imports and node.module.startswith(forbidden_import_roots):
                violations.append(node.module)
        elif isinstance(node, ast.Call):
            call_name: str | None = None
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            if call_name in forbidden_call_names:
                violations.append(call_name)

    assert violations == []


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _assert_safety_flags_false(report: dict) -> None:
    for field in (
        "strategy_authority",
        "broker_state_mutated",
        "submit_attempted",
        "order_intent_created",
        "lifecycle_mutated",
        "runtime_trade_eligible",
    ):
        assert report[field] is False
