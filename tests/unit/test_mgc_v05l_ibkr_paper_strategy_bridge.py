from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution.ibkr_paper_strategy_bridge import (
    IbkrPaperStrategyBridgeConfig,
    IbkrPaperStrategyOrderIntent,
    _build_static_preflight_checks,
    evaluate_strategy_bridge_caller,
    render_ibkr_paper_strategy_bridge_markdown,
    run_ibkr_paper_strategy_bridge,
    strategy_order_intent_schema,
    write_ibkr_paper_strategy_bridge_artifacts,
    write_strategy_order_intent_schema_file,
)
from mgc_v05l.execution.ibkr_paper_order_preview import evaluate_paper_preview_environment_lock


def _healthy_governance() -> dict[str, object]:
    return {
        "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "selected_strategy": {
            "strategy_id": "atp_companion_v1_asia_us",
            "bridge_strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "strategy_status": "PROBATION_ACTIVE",
            "submit_allowed": True,
            "submit_block_reasons": [],
        },
    }


def _config(tmp_path: Path, **overrides: object) -> IbkrPaperStrategyBridgeConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9211,
        "account_id": "DUM882026",
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "symbol": "MGC",
        "contract_month": "202606",
        "action": "BUY",
        "quantity": 1.0,
        "order_type": "LMT",
        "limit_price_model": "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY",
        "time_in_force": "DAY",
        "reason": "GC_ASIA_ONLY_CANDIDATE_PAPER_INTENT",
        "risk_tags": ("GC_ASIA_ONLY_CANDIDATE", "PAPER_ONLY"),
        "paper_only": True,
        "output_dir": tmp_path / "bridge-out",
    }
    payload.update(overrides)
    return IbkrPaperStrategyBridgeConfig(**payload)


def test_schema_contains_required_fields() -> None:
    schema = strategy_order_intent_schema()

    assert schema["properties"]["strategy_id"]["type"] == "string"
    assert schema["properties"]["action"]["enum"] == ["BUY", "EXIT", "HOLD", "NO_ACTION", "SELL"]
    assert "paper_only" in schema["required"]
    assert "current_strategy_state" in schema["properties"]
    assert "contract_target" in schema["properties"]


def test_write_schema_file(tmp_path: Path) -> None:
    path = write_strategy_order_intent_schema_file(repo_root=tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["title"] == "IBKR Paper Strategy Order Intent"


def test_strategy_style_caller_fails_closed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="manual_strategy_bridge_cli",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.runtime"}))],
    )

    assert result["passed"] is False


def test_invalid_symbol_fails_before_connect(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_bridge(config=_config(tmp_path, symbol="GC"))

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "Phase 1 executable contract is MGC only" in json.dumps(artifacts.report)


def test_submit_requires_manual_harness_bundle(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_bridge(config=_config(tmp_path, submit=True))

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "manual-harness frozen preview" in json.dumps(artifacts.report)


def test_submit_is_blocked_when_paper_strategy_monitor_disallows_submit(tmp_path: Path) -> None:
    status_path = tmp_path / "var"
    status_path.mkdir(parents=True, exist_ok=True)
    (status_path / "paper_strategy_monitor_runtime_status.json").write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_PARTIAL",
                "monitor_running": True,
                "submit_allowed": False,
                "health_classification": "STALE",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "block_reasons": ["paper_runtime_stale", "working_open_order_present"],
                "detail": "Paper runtime is stale and a broker order is already open.",
                "last_broker_refresh_timestamp": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            submit=True,
            manual_frozen_preview_path=tmp_path / "preview.json",
            approval_digest="digest",
            approval_phrase="phrase",
        )
    )

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "Paper runtime is stale" in json.dumps(artifacts.report)


def test_preflight_blocks_when_monitor_runtime_is_missing(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        manual_frozen_preview_path=tmp_path / "preview.json",
        approval_digest="digest",
        approval_phrase="phrase",
    )
    intent = IbkrPaperStrategyOrderIntent(
        strategy_id=config.strategy_id,
        symbol=config.symbol,
        contract_month=config.contract_month,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price_model=config.limit_price_model,
        time_in_force=config.time_in_force,
        reason=config.reason,
        timestamp="2026-04-28T00:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "manual cli"},
        monitor_status={},
        governance_status={},
    )

    failure = next(row for row in checks if row["name"] == "paper_strategy_monitor_runtime_present")
    assert failure["passed"] is False


def test_preflight_passes_healthy_monitor_gate(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        manual_frozen_preview_path=tmp_path / "preview.json",
        approval_digest="digest",
        approval_phrase="phrase",
    )
    intent = IbkrPaperStrategyOrderIntent(
        strategy_id=config.strategy_id,
        symbol=config.symbol,
        contract_month=config.contract_month,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price_model=config.limit_price_model,
        time_in_force=config.time_in_force,
        reason=config.reason,
        timestamp="2026-04-28T00:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "manual cli"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status=_healthy_governance(),
    )

    assert all(
        row["passed"]
        for row in checks
        if row["name"].startswith("paper_strategy_monitor_") or row["name"].startswith("paper_strategy_governance_")
    )


def test_preflight_blocks_when_monitor_contract_mismatches(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        manual_frozen_preview_path=tmp_path / "preview.json",
        approval_digest="digest",
        approval_phrase="phrase",
    )
    intent = IbkrPaperStrategyOrderIntent(
        strategy_id=config.strategy_id,
        symbol=config.symbol,
        contract_month=config.contract_month,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price_model=config.limit_price_model,
        time_in_force=config.time_in_force,
        reason=config.reason,
        timestamp="2026-04-28T00:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "manual cli"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260926", "con_id": 712565978, "local_symbol": "MGCU6"},
            "block_reasons": [],
        },
        governance_status=_healthy_governance(),
    )

    failure = next(row for row in checks if row["name"] == "paper_strategy_monitor_contract_match")
    assert failure["passed"] is False


def test_preflight_blocks_when_governance_status_is_paused(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        manual_frozen_preview_path=tmp_path / "preview.json",
        approval_digest="digest",
        approval_phrase="phrase",
    )
    intent = IbkrPaperStrategyOrderIntent(
        strategy_id=config.strategy_id,
        symbol=config.symbol,
        contract_month=config.contract_month,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price_model=config.limit_price_model,
        time_in_force=config.time_in_force,
        reason=config.reason,
        timestamp="2026-04-28T00:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "manual cli"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["paused"],
            "selected_strategy": {
                "strategy_id": "atp_companion_v1_asia_us",
                "bridge_strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "strategy_status": "PAUSED",
                "submit_allowed": False,
                "submit_block_reasons": ["paused"],
            },
        },
    )

    failure = next(row for row in checks if row["name"] == "paper_strategy_governance_status_allowed")
    assert failure["passed"] is False


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    report = {
        "classification": "PAPER_STRATEGY_BRIDGE_READY",
        "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497},
        "selected_account_id": "DUM882026",
        "intent": {
            "strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "symbol": "MGC",
            "action": "BUY",
            "quantity": 1.0,
            "order_type": "LMT",
            "time_in_force": "DAY",
            "limit_price_model": "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY",
        },
        "qualified_contract_report": {
            "qualified_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"}
        },
        "current_position_quantity": 0.0,
        "open_orders": {"open_order_count": 0},
        "preflight_checks": [{"name": "paper_environment_lock", "passed": True, "detail": "ok"}],
    }
    artifacts = type(
        "Artifacts",
        (),
        {"report": report, "audit_events": [{"event_type": "intent_ready"}]},
    )()

    write_ibkr_paper_strategy_bridge_artifacts(output_dir=tmp_path, artifacts=artifacts)  # type: ignore[arg-type]

    payload = json.loads((tmp_path / "ibkr_paper_strategy_bridge_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_paper_strategy_bridge_markdown(payload)
    assert "PAPER_STRATEGY_BRIDGE_READY" in markdown
    assert (tmp_path / "per_strategy_paper_status_summary.json").exists()
    assert (tmp_path / "ibkr_paper_strategy_bridge_audit.jsonl").exists()
