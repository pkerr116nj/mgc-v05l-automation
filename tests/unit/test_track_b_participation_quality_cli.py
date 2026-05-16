from __future__ import annotations

import ast
import json
from pathlib import Path

from mgc_v05l.app.track_b_participation_quality_cli import main as participation_quality_cli_main
from mgc_v05l.execution_core.track_b_participation_quality import (
    INPUT_MODE_OFFLINE_EVALUATION,
    LOW_CONFIDENCE_THIN_DATA,
    PERSISTENT_BULLISH_PRESSURE,
    RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE,
)


FIXTURE_PATH = Path("tests/fixtures/participation_quality/bullish_5m_fixture.json")


def test_participation_quality_cli_writes_latest_state(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    output_dir = tmp_path / "participation_quality"

    exit_code = participation_quality_cli_main(
        [
            "--candle-json",
            str(FIXTURE_PATH),
            "--output-dir",
            str(output_dir),
            "--now",
            "2026-05-08T14:05:00+00:00",
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    latest_state_path = output_dir / "latest_participation_quality_state.json"
    state = json.loads(latest_state_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert latest_state_path.exists()
    assert printed["classification"] == PERSISTENT_BULLISH_PRESSURE
    assert printed["input_mode"] == INPUT_MODE_OFFLINE_EVALUATION
    assert printed["offline_only"] is True
    assert printed["strategy_authority"] is False
    assert printed["runtime_trade_eligible"] is False
    assert printed["broker_state_mutated"] is False
    assert printed["submit_attempted"] is False
    assert printed["cancel_attempted"] is False
    assert printed["close_attempted"] is False
    assert printed["place_order_attempted"] is False
    assert state["artifact_path"] == str(latest_state_path.resolve())
    assert state["participation_state"] == PERSISTENT_BULLISH_PRESSURE
    assert state["input_source_category"] == "TEST_FIXTURE"


def test_participation_quality_cli_refuses_research_source_as_runtime_truth(
    tmp_path: Path,
    capsys,  # type: ignore[no-untyped-def]
) -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    payload["input_mode"] = "RUNTIME_DECISION"
    payload["input_source_category"] = "RESEARCH"
    payload["input_source_path"] = "outputs/track_b_research/snapshots/latest_decision_bar_snapshots.jsonl"
    research_fixture = tmp_path / "research_runtime_claim.json"
    research_fixture.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    exit_code = participation_quality_cli_main(
        [
            "--candle-json",
            str(research_fixture),
            "--output-dir",
            str(tmp_path / "participation_quality"),
            "--now",
            "2026-05-08T14:05:00+00:00",
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    state = json.loads(Path(printed["state_path"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert printed["classification"] == LOW_CONFIDENCE_THIN_DATA
    assert printed["refused_research_runtime_truth"] is True
    assert RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE in printed["confidence_failure_reasons"]
    assert state["runtime_trade_eligible"] is False
    assert state["broker_state_mutated"] is False
    assert state["submit_attempted"] is False


def test_participation_quality_cli_import_boundary() -> None:
    path = Path("src/mgc_v05l/app/track_b_participation_quality_cli.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.brokers",
        "ibapi",
    )
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten"}
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith(forbidden_import_roots):
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
