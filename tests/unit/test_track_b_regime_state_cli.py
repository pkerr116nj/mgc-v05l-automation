from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_regime_state_cli import LATEST_REGIME_SESSION_FILENAME, main
from mgc_v05l.execution_core.track_b_regime_state import (
    CHOP_BALANCED,
    COMPRESSION_COILING,
    INCOMPLETE_CANDLES,
    MALFORMED_CANDLES,
    MIXED_TIMEFRAME,
    RANGE_COMPRESSED,
    RANGE_EXPANDED,
    REGIME_CHOP_BALANCED,
    REGIME_COMPRESSION,
    REGIME_EXPANSION,
    REGIME_THIN_OR_STALE,
    STALE_INPUT,
    THIN_DATA,
)


FIXTURE_ROOT = Path("tests/fixtures/regime_state")
FRESH_NOW = "2026-05-08T15:00:00+00:00"


@pytest.mark.parametrize(
    ("fixture_name", "expected_regime", "expected_range", "expected_trend"),
    [
        ("trending_expansion.json", REGIME_EXPANSION, RANGE_EXPANDED, "EXPANSION_DIRECTIONAL"),
        ("chop_balanced.json", REGIME_CHOP_BALANCED, "RANGE_NORMAL", CHOP_BALANCED),
        ("compression.json", REGIME_COMPRESSION, RANGE_COMPRESSED, COMPRESSION_COILING),
    ],
)
def test_cli_writes_regime_state_for_offline_fixtures(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    fixture_name: str,
    expected_regime: str,
    expected_range: str,
    expected_trend: str,
) -> None:
    output_dir = tmp_path / fixture_name.removesuffix(".json")

    exit_code = main(["--input-json", str(FIXTURE_ROOT / fixture_name), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["wrote_artifact"] is True
    assert captured["offline_only"] is True
    assert captured["market_regime_state"] == expected_regime
    assert captured["volatility_range_state"] == expected_range
    assert captured["trend_chop_state"] == expected_trend
    assert captured["failure_reasons"] == []
    assert written["market_regime_state"] == expected_regime
    assert written["artifact_path"] == str((output_dir / LATEST_REGIME_SESSION_FILENAME).resolve())
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_stale_thin_fixture(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "fail_closed"

    exit_code = main(
        [
            "--input-json",
            str(FIXTURE_ROOT / "stale_thin_fail_closed.json"),
            "--output-dir",
            str(output_dir),
            "--now",
            "2026-05-08T16:00:00+00:00",
        ]
    )
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["wrote_artifact"] is True
    assert captured["market_regime_state"] == REGIME_THIN_OR_STALE
    assert THIN_DATA in written["failure_reasons"]
    assert STALE_INPUT in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_malformed_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _load_fixture("trending_expansion.json")
    del payload["candles"][3]["high"]
    input_path = tmp_path / "malformed.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["wrote_artifact"] is True
    assert MALFORMED_CANDLES in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_mixed_timeframe_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _load_fixture("trending_expansion.json")
    payload["timeframe_context"]["timeframe_alignment_status"] = "MIXED_TIMEFRAME_BLOCKED"
    payload["candles"][2]["timeframe"] = "1m"
    input_path = tmp_path / "mixed_timeframe.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["wrote_artifact"] is True
    assert MIXED_TIMEFRAME in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_state_for_incomplete_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _load_fixture("trending_expansion.json")
    payload["candles"][-1]["completed"] = False
    input_path = tmp_path / "incomplete.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["wrote_artifact"] is True
    assert INCOMPLETE_CANDLES in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_defaults_missing_provenance_to_test_fixture_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = _load_fixture("trending_expansion.json")
    payload.pop("input_source_path", None)
    payload.pop("input_source_category", None)
    payload.pop("input_mode", None)
    input_path = tmp_path / "missing_provenance_defaults.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir), "--now", FRESH_NOW])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_REGIME_SESSION_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["failure_reasons"] == []
    assert written["input_mode"] == "TEST"
    assert written["input_source_category"] == "TEST_FIXTURE"
    _assert_safety_flags_false(written)


def test_cli_import_boundary() -> None:
    path = Path("src/mgc_v05l/app/track_b_regime_state_cli.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    allowed_from_imports = {"mgc_v05l.execution_core.track_b_regime_state"}
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
