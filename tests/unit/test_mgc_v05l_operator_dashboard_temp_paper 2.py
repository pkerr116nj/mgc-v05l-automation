from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.app.operator_dashboard import (
    OperatorDashboardService,
    _initial_operator_canary_cards_markup,
)


def test_paper_start_command_blocks_exclusive_temp_paper_overlay(tmp_path: Path) -> None:
    repo_root = tmp_path
    outputs = repo_root / "outputs" / "operator_dashboard"
    outputs.mkdir(parents=True, exist_ok=True)
    config_dir = repo_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "base.yaml").write_text("", encoding="utf-8")
    (config_dir / "probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml").write_text(
        'probationary_paper_runtime_exclusive_config: true\n',
        encoding="utf-8",
    )
    service = OperatorDashboardService(repo_root)

    command, metadata = service._paper_start_command_with_enabled_temp_paper(  # noqa: SLF001
        {
            "paper": {
                "temporary_paper_strategies": {
                    "rows": [
                        {
                            "lane_id": "atp_companion_v1_asia_us",
                            "state": "ENABLED",
                            "temporary_paper_strategy": True,
                            "runtime_kind": "atp_companion_benchmark_paper",
                        }
                    ]
                }
            }
        }
    )

    assert command is None
    assert metadata["incompatible_exclusive_lane_ids"] == ["atp_companion_v1_asia_us"]
    assert str(config_dir / "probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml") in metadata["incompatible_exclusive_config_paths"]


def test_empty_temporary_paper_payload_uses_zero_enabled_language(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)

    payload = service._paper_temporary_paper_strategies_payload({"rows": []})  # noqa: SLF001

    assert payload["enabled_count"] == 0
    assert payload["note"] == "No temporary paper strategies are enabled in the current runtime."


def test_empty_operator_canary_markup_does_not_imply_expected_lanes() -> None:
    markup = _initial_operator_canary_cards_markup([], kill_switch_active=False)

    assert "No temporary paper strategies are currently enabled." in markup
    assert "Expected lanes:" not in markup
