from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track1_trading_stop_preflight import build_track1_trading_stop_preflight


NOW = datetime(2026, 5, 7, 12, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def build(tmp_path: Path):
    return build_track1_trading_stop_preflight(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )


def test_missing_track1_artifacts_produces_reference_missing(tmp_path: Path) -> None:
    result = build(tmp_path)

    assert result.report["classification"] == "TRACK1_REFERENCE_ARTIFACTS_MISSING"
    assert result.report["full_parity_audit_justified"] is False
    assert result.report["bounded_policy"]["broker_commands_invoked"] is False


def test_trade_artifacts_with_stop_date_produce_breakpoint_classification(tmp_path: Path) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json",
        {
            "track": "Track 1 paper",
            "trade_count": 3,
            "strategy_id": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "last_trade_at": "2026-04-20T15:00:00+00:00",
            "rows": [{"filled_at": "2026-04-20T15:00:00+00:00", "strategy_id": "asiaEarlyNormalBreakoutRetestHoldTurn"}],
        },
    )
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json",
        {
            "track": "Track 1 paper",
            "signal_count": 0,
            "generated_at": "2026-04-21T15:00:00+00:00",
            "runtime": "no trades after prior activity",
        },
    )

    result = build(tmp_path)

    assert result.report["classification"] == "TRACK1_ARTIFACTS_FOUND_NO_CLEAR_BREAKPOINT"
    assert result.report["breakpoint"]["last_known_track1_trade"]["path"].endswith("paper_strategy_trade_log_snapshot.json")
    assert result.report["evidence_summary"]["direct_trade_count"] == 1


def test_dashboard_trade_counters_do_not_prove_direct_track1_trade(tmp_path: Path) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_tracked_strategies_snapshot.json",
        {
            "track": "Track 1 paper",
            "generated_at": "2026-04-22T14:00:00+00:00",
            "rows": [{"strategy_id": "firstBullSnapTurn", "trades": 12}],
        },
    )

    result = build(tmp_path)

    assert result.report["classification"] == "TRACK1_REFERENCE_ARTIFACTS_MISSING"
    assert result.report["evidence_summary"]["direct_trade_count"] == 0
    assert result.report["breakpoint"]["last_known_track1_trade"] is None


def test_signals_continue_but_trades_stop_classifies_handoff_break(tmp_path: Path) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json",
        {
            "track": "Track 1 paper",
            "trade_count": 1,
            "strategy_id": "firstBullSnapTurn",
            "filled_at": "2026-04-18T14:00:00+00:00",
        },
    )
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_latest_intents_snapshot.json",
        {
            "track": "Track 1 paper",
            "signal_count": 2,
            "strategy_id": "firstBullSnapTurn",
            "signal_timestamp": "2026-04-22T14:00:00+00:00",
        },
    )

    result = build(tmp_path)

    assert result.report["classification"] == "TRACK1_SIGNALS_CONTINUED_HANDOFF_BROKE"


def test_config_disabled_evidence_produces_config_classification(tmp_path: Path) -> None:
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json",
        {
            "track": "Track 1 paper",
            "trade_count": 1,
            "strategy_id": "firstBullSnapTurn",
            "filled_at": "2026-04-18T14:00:00+00:00",
        },
    )
    write_json(
        tmp_path / "config" / "paper_track1_config_snapshot.json",
        {
            "track": "Track 1 paper",
            "enabled": False,
            "strategy_id": "firstBullSnapTurn",
            "generated_at": "2026-04-19T14:00:00+00:00",
        },
    )

    result = build(tmp_path)

    assert result.report["classification"] == "TRACK1_CONFIG_DISABLED_OR_MISMATCHED"
    assert result.report["evidence_summary"]["config_disabled_count"] >= 1


def test_preflight_does_not_invoke_broker_or_proof_paths(tmp_path: Path) -> None:
    result = build(tmp_path)

    policy = result.report["bounded_policy"]
    assert policy["broker_commands_invoked"] is False
    assert policy["paper_proof_cli_invoked"] is False
    assert policy["submit_cancel_place_order_invoked"] is False
    assert policy["broker_state_mutated"] is False
