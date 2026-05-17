from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_sizing_position_management_cli import (
    LATEST_SIZING_POSITION_MANAGEMENT_FILENAME,
    MALFORMED_INPUT,
    main,
)
from mgc_v05l.execution_core.track_b_sizing_position_management import (
    BASE_UNIT,
    DO_NOT_ADD,
    HOLD_FULL,
    NO_POSITION,
    NOT_ALLOWED_V1,
    REDUCE_PARTIAL,
    REDUCED_UNIT,
    UNKNOWN,
)


FIXTURE_ROOT = Path("tests/fixtures/sizing_position_management")


@pytest.mark.parametrize(
    ("fixture_name", "expected_initial", "expected_in_position"),
    [
        ("clean_base_unit.json", BASE_UNIT, HOLD_FULL),
        ("reduced_degraded_context.json", REDUCED_UNIT, DO_NOT_ADD),
        ("in_position_hold_full.json", BASE_UNIT, HOLD_FULL),
        ("in_position_reduce_partial.json", REDUCED_UNIT, REDUCE_PARTIAL),
    ],
)
def test_cli_writes_sizing_context_for_offline_fixtures(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    fixture_name: str,
    expected_initial: str,
    expected_in_position: str,
) -> None:
    output_dir = tmp_path / fixture_name.removesuffix(".json")

    exit_code = main(["--input-json", str(FIXTURE_ROOT / fixture_name), "--output-dir", str(output_dir)])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["wrote_artifact"] is True
    assert captured["offline_only"] is True
    assert captured["initial_size_context"] == expected_initial
    assert captured["in_position_size_context"] == expected_in_position
    assert captured["add_size_context"] == NOT_ALLOWED_V1
    assert captured["failure_reasons"] == []
    assert written["initial_size_context"] == expected_initial
    assert written["in_position_size_context"] == expected_in_position
    assert written["add_size_context"] == NOT_ALLOWED_V1
    assert written["artifact_path"] == str((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).resolve())
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_no_position_context(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "fail_closed"

    exit_code = main(
        [
            "--input-json",
            str(FIXTURE_ROOT / "fail_closed_no_position.json"),
            "--output-dir",
            str(output_dir),
        ]
    )
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["initial_size_context"] == NO_POSITION
    assert captured["in_position_size_context"] == UNKNOWN
    assert captured["add_size_context"] == NOT_ALLOWED_V1
    assert "STALE_INPUT" in written["failure_reasons"]
    assert "UNRECONCILED_POSITION_CONTEXT" in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_for_malformed_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "malformed.json"
    input_path.write_text("{not-json", encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir)])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["initial_size_context"] == NO_POSITION
    assert captured["in_position_size_context"] == UNKNOWN
    assert captured["add_size_context"] == NOT_ALLOWED_V1
    assert MALFORMED_INPUT in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_writes_fail_closed_for_non_object_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "non_object.json"
    input_path.write_text("[1, 2, 3]", encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir)])
    written = json.loads((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).read_text(encoding="utf-8"))
    capsys.readouterr()

    assert exit_code == 2
    assert written["initial_size_context"] == NO_POSITION
    assert MALFORMED_INPUT in written["failure_reasons"]
    _assert_safety_flags_false(written)


def test_cli_defaults_missing_provenance_to_test_fixture_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = json.loads((FIXTURE_ROOT / "clean_base_unit.json").read_text(encoding="utf-8"))
    payload.pop("input_source_path", None)
    payload.pop("input_source_category", None)
    payload.pop("input_mode", None)
    input_path = tmp_path / "missing_provenance_defaults.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    output_dir = tmp_path / "out"

    exit_code = main(["--input-json", str(input_path), "--output-dir", str(output_dir)])
    captured = json.loads(capsys.readouterr().out)
    written = json.loads((output_dir / LATEST_SIZING_POSITION_MANAGEMENT_FILENAME).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert captured["failure_reasons"] == []
    assert written["input_mode"] == "TEST"
    assert written["input_source_category"] == "TEST_FIXTURE"
    _assert_safety_flags_false(written)


def test_cli_import_boundary() -> None:
    path = Path("src/mgc_v05l/app/track_b_sizing_position_management_cli.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    allowed_from_imports = {"mgc_v05l.execution_core.track_b_sizing_position_management"}
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
