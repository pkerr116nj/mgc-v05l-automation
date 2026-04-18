from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.app.probationary_runtime import (
    ATP_COMPANION_LIVE_ENTRY_PILOT_RUNTIME_KIND,
    _live_strategy_pilot_operator_exit_allowed,
    _resolve_live_strategy_pilot_lane_spec,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.order_models import OrderIntent


def _load_live_pilot_settings(tmp_path: Path, extra_lines: list[str] | None = None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "live_pilot_override.yaml"
    lines = [
        f'database_url: "sqlite:///{tmp_path / "probationary.live.sqlite3"}"',
        f'probationary_artifacts_dir: "{tmp_path / "live_artifacts"}"',
        "live_poll_lookback_minutes: 60",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    override_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_live_atp_companion_v1_gc_asia_us_pilot.yaml"),
            override_path,
        ]
    )


def test_live_strategy_pilot_config_resolves_exact_atp_scope(tmp_path: Path) -> None:
    settings = _load_live_pilot_settings(tmp_path)

    spec = _resolve_live_strategy_pilot_lane_spec(settings)

    assert spec.lane_id == "atp_companion_v1_gc_asia_us"
    assert spec.symbol == "GC"
    assert spec.trade_size == 1
    assert spec.max_adds_after_entry == 0
    assert spec.runtime_kind == ATP_COMPANION_LIVE_ENTRY_PILOT_RUNTIME_KIND
    assert spec.shared_strategy_identity == "ATP_COMPANION_V1_GC_ASIA_US"
    assert spec.standalone_strategy_id == "atp_companion_v1__paper_gc_asia_us"
    assert str(spec.point_value) == "100"
    assert tuple(spec.observed_instruments) == ("GC",)
    assert tuple(spec.long_sources) == ("trend_participation.pullback_continuation.long.conservative",)
    assert tuple(spec.short_sources) == ()
    assert set(spec.allowed_sessions) == {"ASIA", "US"}


def test_live_strategy_pilot_config_rejects_out_of_scope_symbol(tmp_path: Path) -> None:
    bad_lane = json.dumps(
        [
            {
                "lane_id": "atp_companion_v1_gc_asia_us",
                "display_name": "Bad Live Pilot",
                "symbol": "MGC",
                "standalone_strategy_id": "atp_companion_v1__paper_gc_asia_us",
                "identity_components": ["live_pilot", "gc", "asia_us"],
                "long_sources": ["trend_participation.pullback_continuation.long.conservative"],
                "short_sources": [],
                "session_restriction": "ASIA/US",
                "allowed_sessions": ["ASIA", "US"],
                "point_value": "100",
                "trade_size": 1,
                "participation_policy": "SINGLE_ENTRY_ONLY",
                "max_concurrent_entries": 1,
                "max_position_quantity": 1,
                "max_adds_after_entry": 0,
                "add_direction_policy": "SAME_DIRECTION_ONLY",
                "catastrophic_open_loss": "-500",
                "runtime_kind": ATP_COMPANION_LIVE_ENTRY_PILOT_RUNTIME_KIND,
                "lane_mode": "ATP_COMPANION_LIVE_ENTRY_PILOT",
                "strategy_family": "active_trend_participation_engine",
                "strategy_identity_root": "ATP_COMPANION_V1",
                "observed_instruments": ["GC"],
                "shared_strategy_identity": "ATP_COMPANION_V1_GC_ASIA_US",
            }
        ]
    )
    settings = _load_live_pilot_settings(tmp_path, [f"probationary_paper_lanes_json: '{bad_lane}'"])

    with pytest.raises(ValueError, match="symbol GC"):
        _resolve_live_strategy_pilot_lane_spec(settings)


def test_live_strategy_pilot_only_allows_operator_exit_automation() -> None:
    operator_exit = OrderIntent(
        order_intent_id="exit-1",
        bar_id="GC-2026-04-05T20:00:00+00:00",
        symbol="GC",
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        quantity=1,
        created_at=datetime.now(timezone.utc),
        reason_code="operator_flatten_and_halt",
    )
    strategy_exit = OrderIntent(
        order_intent_id="exit-2",
        bar_id="GC-2026-04-05T20:05:00+00:00",
        symbol="GC",
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        quantity=1,
        created_at=datetime.now(timezone.utc),
        reason_code="strategy_time_exit",
    )

    assert _live_strategy_pilot_operator_exit_allowed(operator_exit) is True
    assert _live_strategy_pilot_operator_exit_allowed(strategy_exit) is False
