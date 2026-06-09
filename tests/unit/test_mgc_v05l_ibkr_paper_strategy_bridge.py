from __future__ import annotations

import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mgc_v05l.execution.ibkr_paper_strategy_bridge as bridge_module
from mgc_v05l.execution.ibkr_manual_paper_submit import (
    IbkrManualPaperSubmitArtifacts,
    frozen_preview_path_for_config,
)
from mgc_v05l.execution.ibkr_paper_strategy_bridge import (
    IbkrPaperStrategyBridgeConfig,
    IbkrPaperStrategyOrderIntent,
    _build_entry_attempt_memory,
    _build_preflight_checks,
    _build_static_preflight_checks,
    _entry_execution_pricing_for_bridge,
    _exit_attempt_policy_for_bridge,
    _map_delegate_classification,
    _quote_is_fresh,
    evaluate_strategy_bridge_caller,
    render_ibkr_paper_strategy_bridge_markdown,
    run_ibkr_paper_strategy_bridge,
    strategy_order_intent_schema,
    write_ibkr_paper_strategy_bridge_artifacts,
    write_strategy_order_intent_schema_file,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    load_unresolved_submit_intent_ownership_records,
)
from mgc_v05l.execution.ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from mgc_v05l.execution.ibkr_paper_order_preview import evaluate_paper_preview_environment_lock
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter

_PLAN_STRATEGY_BRIDGE_SUBMIT = "PLAN_STRATEGY_BRIDGE_SUBMIT"
_ACTION_STRATEGY_BRIDGE_SUBMIT = "STRATEGY_BRIDGE_SUBMIT"


def _healthy_governance() -> dict[str, object]:
    selected = {
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "bridge_strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "strategy_status": "PROBATION_ACTIVE",
        "submit_allowed": True,
        "submit_block_reasons": [],
    }
    return {
        "generated_at": "2999-01-01T00:00:00+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "selected_strategy": selected,
        "strategies": [selected],
    }


def _stale_phase1_governance(*, extra_block_reasons: list[str] | None = None) -> dict[str, object]:
    reasons = ["phase1_broker_reconciliation_not_clear"] + list(extra_block_reasons or [])
    selected_reasons = list(reasons)
    return {
        "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
        "submit_allowed": False,
        "block_reasons": reasons,
        "detail": "Paper strategy governance blocked submit: " + ", ".join(reasons),
        "selected_strategy": {
            "strategy_id": "atp_companion_v1_asia_us",
            "bridge_strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "strategy_status": "PROBATION_ACTIVE",
            "submit_allowed": False,
            "submit_block_reasons": selected_reasons,
            "phase1_broker_reconciliation_gate": {
                "classification": "PHASE1_BROKER_RECONCILIATION_STALE",
                "ready": False,
                "block_reasons": ["phase1_broker_reconciliation_stale"],
                "generated_at": "2000-01-01T00:00:00+00:00",
                "age_seconds": 999999.0,
            },
        },
    }


def _stale_phase1_governance_with_top_level_only_blocker() -> dict[str, object]:
    payload = _stale_phase1_governance()
    payload["block_reasons"] = ["phase1_broker_reconciliation_not_clear", "backend_or_source_not_live_ready"]
    return payload


def test_cancelled_close_delegate_maps_to_terminal_non_fill_classification() -> None:
    delegated = {
        "classification": "PAPER_CLOSE_NOT_FILLED_CANCELLED",
        "report": {
            "submit_cancel_lifecycle": {
                "status": "fill_timeout_cancelled",
                "submitted_order_id": 1,
                "latest_order_status": {"status": "Cancelled"},
            }
        },
    }

    assert _map_delegate_classification(delegated) == "PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED"


def _healthy_lane_governance() -> dict[str, object]:
    selected = {
        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
        "bridge_strategy_id": "asia_london_participation_core_v1__GC",
        "strategy_status": "PROBATION_ACTIVE",
        "submit_allowed": True,
        "submit_block_reasons": [],
    }
    return {
        "generated_at": "2999-01-01T00:00:00+00:00",
        "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "selected_strategy": selected,
        "strategies": [selected],
    }


def _healthy_exposure() -> dict[str, object]:
    return {
        "classification": "PAPER_EXPOSURE_ATTRIBUTION_READY",
        "submit_allowed": True,
        "block_reasons": [],
        "detail": "Exposure attribution allows submit.",
    }


def _write_runtime_files(
    tmp_path: Path,
    *,
    monitor_status: dict[str, object] | None = None,
    governance_status: dict[str, object] | None = None,
    ledger_positions: list[dict[str, object]] | None = None,
) -> None:
    var_dir = tmp_path / "var"
    var_dir.mkdir(parents=True, exist_ok=True)
    (var_dir / "paper_strategy_monitor_runtime_status.json").write_text(
        json.dumps(
            monitor_status
            or {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "health_classification": "HEALTHY",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "block_reasons": [],
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )
    (var_dir / "per_strategy_paper_status.json").write_text(
        json.dumps(
            governance_status
            or {
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
                "strategies": [],
            }
        ),
        encoding="utf-8",
    )
    (var_dir / "paper_strategy_position_ledger.json").write_text(
        json.dumps({"positions": ledger_positions or [], "orphan_positions": []}),
        encoding="utf-8",
    )


def _write_phase1_reconciliation(
    tmp_path: Path,
    *,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    review_required_count: int = 0,
    open_order_count: int = 0,
    block_reasons: list[str] | None = None,
    generated_at: str = "2999-01-01T00:00:00+00:00",
) -> None:
    path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "classification": classification,
                "broker_reconciled": broker_reconciled,
                "review_required_count": review_required_count,
                "current_scope_review_required_count": review_required_count,
                "track_b_broker_open_order_count": open_order_count,
                "track_b_broker_position_count": 0,
                "track_b_broker_positions": [],
                "track_b_lifecycle_positions": [],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": block_reasons or [],
            }
        ),
        encoding="utf-8",
    )


def _write_canonical_current_scope(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "current_scope_review_required_count": 0,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "NO_MANAGED_POSITIONS",
            "summary": {
                "managed_position_count": 0,
                "lifecycle_position_count": 0,
                "review_required_count": 0,
                "attention_required_count": 0,
            },
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "NO_MANAGED_ORDERS",
            "summary": {
                "managed_order_count": 0,
                "working_entry_order_count": 0,
                "working_close_order_count": 0,
                "position_without_close_order_count": 0,
            },
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "NO_OPEN_ORDERS",
            "summary": {"open_order_count": 0, "unknown_open_order_count": 0},
            "open_order_count": 0,
            "unknown_open_order_count": 0,
        },
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "positions_complete": True,
            "open_orders_complete": True,
            "position_count": 0,
            "open_order_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "SAFE_STATE_NORMAL",
            "broker_mutation_allowed": True,
            "submit_allowed": True,
            "entry_mutation_allowed": True,
            "observe_only": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )


def _write_strategy_bridge_snapshot(
    tmp_path: Path,
    *,
    target_identity: dict[str, object] | None = None,
    supervisor_classification: str = "SUPERVISOR_RUNTIME_START_ALLOWED",
    safe_to_start_runtime: bool = True,
) -> dict[str, str]:
    generated_at = datetime.now(timezone.utc).isoformat()
    snapshot_id = "snapshot-strategy-bridge-submit"
    generation_id = "generation-strategy-bridge-submit"
    supervisor_decision_id = "supervisor-strategy-bridge-submit"
    _write_json(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": generated_at,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_decision_id": supervisor_decision_id,
            "runtime_supervisor_classification": supervisor_classification,
            "safe_to_start_runtime": safe_to_start_runtime,
            "live_money_eligible": False,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": generated_at,
            "supervisor_decision_id": supervisor_decision_id,
            "classification": supervisor_classification,
            "live_money_eligible": False,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": generated_at,
            "classification": _PLAN_STRATEGY_BRIDGE_SUBMIT,
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "execution_enabled": False,
            "proposed_actions": [
                {
                    "action_id": "strategy_bridge_submit",
                    "action_type": _ACTION_STRATEGY_BRIDGE_SUBMIT,
                    "target_identity": target_identity or _strategy_bridge_target_identity(),
                    "execution_enabled": False,
                }
            ],
        },
    )
    return {
        "control_plane_snapshot_id": snapshot_id,
        "shared_truth_refresh_generation_id": generation_id,
        "runtime_supervisor_decision_id": supervisor_decision_id,
    }


def _strategy_bridge_target_identity() -> dict[str, object]:
    return {
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "symbol": "MGC",
        "contract_month": "202606",
        "contract": "MGCM6",
        "con_id": "712565978",
        "action": "BUY",
        "quantity": "1.0",
        "caller_path": "manual_strategy_bridge_cli",
    }


def _runtime_bridge_metadata(*, snapshot: dict[str, str]) -> dict[str, object]:
    return {
        "caller_type": "supervised_paper_runtime",
        "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
        "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
        "source_instrument": "GC",
        "executable_proxy": "GC",
        "paper_only": True,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "account_id": "DUM882026",
        "runtime_pid": 12345,
        "runtime_cwd": str(Path.cwd()),
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_proxy_mode": "GC_SIGNAL_DIRECT_PHASE1",
        "intent_action": "BUY",
        "intent_type": "BUY_TO_OPEN",
        **snapshot,
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_fresh_broker_truth(tmp_path: Path) -> None:
    broker_root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    broker_root.mkdir(parents=True, exist_ok=True)
    generated_at = "2999-01-01T00:00:00+00:00"
    (broker_root / "ibkr_positions_snapshot.json").write_text(
        json.dumps(
            {
                "ok": True,
                "positions_complete": True,
                "generated_at": generated_at,
                "selected_account_id": "DUM882026",
                "positions": [],
            }
        ),
        encoding="utf-8",
    )
    (broker_root / "ibkr_open_orders_snapshot.json").write_text(
        json.dumps(
            {
                "ok": True,
                "open_orders_complete": True,
                "generated_at": generated_at,
                "selected_account_id": "DUM882026",
                "open_orders": [],
            }
        ),
        encoding="utf-8",
    )


def _write_contract_status(tmp_path: Path, *, entry_status: str = "CONTRACT_ENTRY_ELIGIBLE") -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/contract_resolver/latest_contract_resolver_status.json",
        {
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "CONTRACT_ALLOWED" if entry_status == "CONTRACT_ENTRY_ELIGIBLE" else "CONTRACT_BLOCKED",
            "entry_status": entry_status,
            "exit_status": "EXIT_ORIGINAL_CONTRACT_ALLOWED",
            "symbol": "MGC",
            "selected_contract": {"localSymbol": "MGCM6", "conId": 712565978, "expiry": "202606"},
        },
    )


@pytest.fixture(autouse=True)
def _default_phase1_reconciliation(tmp_path: Path) -> None:
    _write_phase1_reconciliation(tmp_path)
    _write_contract_status(tmp_path)
    _write_canonical_current_scope(tmp_path)


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


class _HandshakeFailureTransport:
    def __init__(self, *, client: object, collector: object, config: object, module_loader: object | None = None) -> None:
        del client, module_loader
        self.collector = collector
        self.config = config
        self.disconnected = False

    def connect(self) -> None:
        self.collector.error(
            code=502,
            message="Couldn't connect to TWS",
            request_id=-1,
        )

    def run_loop(self) -> None:
        return None

    def is_connected(self) -> bool:
        return False

    def server_version(self) -> int | None:
        return None

    def tws_connection_time(self) -> str | None:
        return None

    def disconnect(self) -> None:
        self.disconnected = True


def _approved_runtime_metadata(
    *,
    strategy_id: str,
    source_instrument: str = "GC",
    executable_proxy: str = "GC",
    action: str = "BUY",
    intent_type: str = "BUY_TO_OPEN",
    route_destination: str = "ibkr_paper_bridge_submit_capable",
    mode: str = "PAPER",
    bridge_proxy_mode: str = "GC_SIGNAL_DIRECT_PHASE1",
    runtime_pid: int | None = None,
    runtime_cwd: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "caller_type": "supervised_paper_runtime",
        "strategy_id": strategy_id,
        "lane_id": strategy_id,
        "source_instrument": source_instrument,
        "executable_proxy": executable_proxy,
        "paper_only": True,
        "mode": mode,
        "host": "127.0.0.1",
        "port": 7497,
        "account_id": "DUM882026",
        "route_destination": route_destination,
        "bridge_proxy_mode": bridge_proxy_mode,
        "intent_action": action,
        "intent_type": intent_type,
    }
    if runtime_pid is not None:
        payload["runtime_pid"] = runtime_pid
    if runtime_cwd is not None:
        payload["runtime_cwd"] = runtime_cwd
    return payload


def _leak_auth_digest(payload: dict[str, object]) -> str:
    fields = (
        "artifact_type",
        "account_id",
        "mode",
        "lane_id",
        "strategy_id",
        "symbol",
        "local_symbol",
        "expiry",
        "con_id",
        "action",
        "exit_action",
        "qty",
        "repo_root",
        "git_head",
        "created_at",
        "expires_at",
        "safety_snapshot",
    )
    critical = {field: payload.get(field) for field in fields}
    return hashlib.sha256(json.dumps(critical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _write_leak_authorization(tmp_path: Path, *, action: str = "BUY") -> tuple[Path, str]:
    now = datetime.now(timezone.utc)
    payload: dict[str, object] = {
        "artifact_type": "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION",
        "account_id": "DUM882026",
        "mode": "PAPER",
        "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
        "strategy_id": "asia_london_participation_core_v1__GC",
        "symbol": "GC",
        "local_symbol": "GCM6",
        "expiry": "202606",
        "con_id": None,
        "action": action,
        "exit_action": "SELL" if action == "BUY" else "BUY",
        "qty": 1,
        "repo_root": str(tmp_path),
        "git_head": None,
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
        "safety_snapshot": {
            "reconciliation_classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "positions_count": 0,
            "lifecycle_positions_count": 0,
            "open_orders_count": 0,
            "review_required_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    payload["digest"] = _leak_auth_digest(payload)
    path = tmp_path / "leak_authorization.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path, str(payload["digest"])


def _intent_from_config(config: IbkrPaperStrategyBridgeConfig) -> IbkrPaperStrategyOrderIntent:
    return IbkrPaperStrategyOrderIntent(
        strategy_id=config.strategy_id,
        symbol=config.symbol,
        contract_month=config.contract_month,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price_model=config.limit_price_model,
        time_in_force=config.time_in_force,
        reason=config.reason,
        timestamp="2026-05-14T12:26:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )


def _write_runtime_1m_candle(
    repo_root: Path,
    *,
    symbol: str,
    close: float,
    bar_end: str = "2026-05-14T12:25:00+00:00",
) -> Path:
    path = (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / "1m"
        / "latest_runtime_candles.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": bar_end,
                "source": "DATABENTO_LIVE_RUNTIME",
                "symbol": symbol,
                "timeframe": "1m",
                "bars": [
                    {
                        "bar_start": "2026-05-14T12:24:00+00:00",
                        "bar_end": bar_end,
                        "open": close - 2.0,
                        "high": close + 1.0,
                        "low": close - 3.0,
                        "close": close,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _quote_context(*, bid: float = 29713.5, ask: float = 29714.0, last: float = 29713.75) -> dict[str, object]:
    return {
        "quote_source_label": "DELAYED",
        "updated_at": "2026-05-14T12:25:20+00:00",
        "bid_price": bid,
        "ask_price": ask,
        "last_price": last,
    }


def _qualified_contract_report(*, min_tick: float = 0.25) -> dict[str, object]:
    return {
        "ok": True,
        "qualified_contract": {
            "symbol": "MNQ",
            "expiry": "202606",
            "con_id": 770561201,
            "local_symbol": "MNQM6",
            "min_tick": min_tick,
        },
        "api_contract_details": [{"min_tick": min_tick, "multiplier": "2"}],
    }


def test_entry_pricing_prefers_fresh_runtime_candle_when_delayed_ask_is_too_low(tmp_path: Path) -> None:
    runtime_path = _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["limit_price"] == 29752.25
    assert pricing["runtime_last_or_close"] == 29752.0
    assert pricing["marketable_by_runtime_context"] is True
    assert pricing["delayed_quote_limit_price"] == 29714.25
    assert pricing["delayed_quote_limit_marketable_by_delayed_quote"] is True
    assert pricing["delayed_quote_limit_marketable_by_runtime_context"] is False
    assert pricing["limit_vs_runtime_price_points"] == 0.25
    assert pricing["limit_vs_broker_delayed_ask_points"] == 38.25
    assert pricing["runtime_source_artifact_path"] == str(runtime_path)
    assert pricing["live_money_eligible"] is False


def test_entry_pricing_honors_bounded_runtime_marketable_offset_metadata(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_globex_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="mnq_globex_active_participation_long",
                source_instrument="MNQ",
                executable_proxy="MNQ",
                bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
            ),
            "entry_marketable_limit_offset_ticks": 4,
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["limit_offset_ticks"] == 4.0
    assert pricing["limit_price"] == 29753.0
    assert pricing["limit_vs_runtime_price_points"] == 1.0
    assert pricing["block_submit"] is False


def test_entry_pricing_caps_runtime_marketable_offset_metadata(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_globex_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="mnq_globex_active_participation_long",
                source_instrument="MNQ",
                executable_proxy="MNQ",
                bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
            ),
            "entry_marketable_limit_offset_ticks": 99,
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["limit_offset_ticks"] == 4.0
    assert pricing["limit_price"] == 29753.0


def test_active_evidence_long_prefers_fresh_live_ask_reference(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context={
            "quote_source_label": "LIVE",
            "updated_at": "2026-05-14T12:25:25+00:00",
            "bid_price": 29752.0,
            "ask_price": 29752.25,
            "last_price": 29752.0,
        },
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["pricing_source"] == "IBKR_LIVE_ASK"
    assert pricing["pricing_reference_price"] == 29752.25
    assert pricing["pricing_reference_ts"] == "2026-05-14T12:25:25+00:00"
    assert pricing["pricing_reference_age_seconds"] == 5.0
    assert pricing["marketable_limit_offset_ticks"] == 1.0
    assert pricing["max_slippage_ticks"] == 4.0
    assert pricing["limit_price"] == 29752.5
    assert pricing["block_submit"] is False


def test_active_evidence_short_prefers_fresh_live_bid_reference(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_short",
        symbol="MNQ",
        contract_month="202606",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_short",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
            intent_type="SELL_TO_OPEN",
        ),
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context={
            "quote_source_label": "LIVE",
            "updated_at": "2026-05-14T12:25:25+00:00",
            "bid_price": 29751.75,
            "ask_price": 29752.0,
            "last_price": 29751.75,
        },
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["pricing_source"] == "IBKR_LIVE_BID"
    assert pricing["pricing_reference_price"] == 29751.75
    assert pricing["marketable_limit_offset_ticks"] == 1.0
    assert pricing["limit_price"] == 29751.5
    assert pricing["limit_vs_runtime_price_points"] == 0.5
    assert pricing["marketable_by_runtime_context"] is True
    assert pricing["block_submit"] is False


def test_active_evidence_runtime_reference_near_edge_uses_wider_bounded_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 26, 15, tzinfo=timezone.utc),
    )

    assert pricing["pricing_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["pricing_reference_age_seconds"] == 75.0
    assert pricing["marketable_limit_offset_ticks"] == 4.0
    assert pricing["limit_price"] == 29753.0
    assert pricing["stale_reference_blocker"] is None
    assert pricing["block_submit"] is False


def test_active_evidence_runtime_reference_too_stale_blocks_submit(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 26, 40, tzinfo=timezone.utc),
    )

    assert pricing["pricing_reference_age_seconds"] == 100.0
    assert pricing["limit_price"] is None
    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "ACTIVE_EVIDENCE_ENTRY_REFERENCE_STALE"
    assert pricing["stale_reference_blocker"] == "ACTIVE_EVIDENCE_ENTRY_REFERENCE_STALE"


def test_active_evidence_offset_is_capped_by_max_slippage_ticks(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="mnq_us_active_participation_long",
                source_instrument="MNQ",
                executable_proxy="MNQ",
                bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
            ),
            "entry_marketable_limit_offset_ticks": 99,
            "entry_max_slippage_ticks": 2,
        },
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 26, 15, tzinfo=timezone.utc),
    )

    assert pricing["max_slippage_ticks"] == 2.0
    assert pricing["marketable_limit_offset_ticks"] == 2.0
    assert pricing["limit_price"] == 29752.5
    assert pricing["block_submit"] is False


def test_non_active_evidence_lane_keeps_existing_runtime_pricing_window(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 26, 40, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["limit_offset_ticks"] == 1.0
    assert pricing["limit_price"] == 29752.25
    assert pricing["block_submit"] is False


def test_leak_test_buy_entry_uses_bounded_marketable_runtime_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29321.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        symbol="MNQ",
        contract_month="202606",
        caller_path="track_b_paper_leak_test_apply",
        caller_metadata={
            "leak_test": True,
            "intent_type": "BUY_TO_OPEN",
            "entry_execution_policy": "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE",
            "entry_execution_intent": "PARTICIPATE_NOW",
            "leak_test_marketable_limit_offset_ticks": 16,
            "live_money_eligible": False,
            "paper_only": True,
        },
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29308.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["limit_offset_ticks"] == 16.0
    assert pricing["runtime_last_or_close"] == 29321.0
    assert pricing["limit_price"] == 29325.0
    assert pricing["limit_vs_runtime_price_points"] == 4.0
    assert pricing["marketable_by_runtime_context"] is True
    assert pricing["block_submit"] is False
    assert pricing["live_money_eligible"] is False


def test_leak_test_sell_entry_uses_bounded_marketable_runtime_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29321.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_short",
        symbol="MNQ",
        contract_month="202606",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="track_b_paper_leak_test_apply",
        caller_metadata={
            "leak_test": True,
            "intent_type": "SELL_TO_OPEN",
            "entry_execution_policy": "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE",
            "entry_execution_intent": "PARTICIPATE_NOW",
            "leak_test_marketable_limit_offset_ticks": 16,
            "live_money_eligible": False,
            "paper_only": True,
        },
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(bid=29330.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["limit_offset_ticks"] == 16.0
    assert pricing["runtime_last_or_close"] == 29321.0
    assert pricing["limit_price"] == 29317.0
    assert pricing["limit_vs_runtime_price_points"] == 4.0
    assert pricing["marketable_by_runtime_context"] is True
    assert pricing["block_submit"] is False


def test_leak_test_entry_blocks_when_runtime_reference_is_stale(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29321.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        symbol="MNQ",
        contract_month="202606",
        caller_path="track_b_paper_leak_test_apply",
        caller_metadata={
            "leak_test": True,
            "intent_type": "BUY_TO_OPEN",
            "entry_execution_policy": "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE",
            "entry_execution_intent": "PARTICIPATE_NOW",
            "leak_test_marketable_limit_offset_ticks": 16,
            "live_money_eligible": False,
            "paper_only": True,
        },
    )

    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29308.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        now=datetime(2026, 5, 14, 12, 30, 1, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "IBKR_DELAYED_DIAGNOSTIC_ONLY"
    assert pricing["runtime_data_fresh"] is False
    assert pricing["limit_price"] is None
    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "DELAYED_QUOTE_NOT_EXECUTION_SAFE"


def test_entry_pricing_blocks_marketable_entry_when_only_delayed_quote_is_available(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29714.0),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "IBKR_DELAYED_DIAGNOSTIC_ONLY"
    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "DELAYED_QUOTE_NOT_EXECUTION_SAFE"
    assert pricing["delayed_quote_limit_marketable_by_delayed_quote"] is True
    assert pricing["marketable_by_runtime_context"] is False
    assert pricing["live_money_eligible"] is False


def test_resting_pullback_entry_uses_strategy_defined_limit_and_longer_window(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="custom_pullback_lane",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="custom_pullback_lane",
                source_instrument="MNQ",
                executable_proxy="MNQ",
            ),
            "entry_execution_intent": "RESTING_PULLBACK_LIMIT",
            "entry_limit_price": "29751.0",
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29752.25),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["entry_execution_intent"] == "RESTING_PULLBACK_LIMIT"
    assert pricing["execution_policy"] == "PASSIVE_LIMIT"
    assert pricing["execution_price_source"] == "STRATEGY_DEFINED_ENTRY_LIMIT"
    assert pricing["limit_price"] == 29751.0
    assert pricing["fill_timeout_seconds"] == 300.0
    assert pricing["passive_miss_is_failure"] is False
    assert pricing["block_submit"] is False


def test_resting_pullback_entry_can_derive_limit_from_runtime_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="custom_pullback_lane",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="custom_pullback_lane",
                source_instrument="MNQ",
                executable_proxy="MNQ",
            ),
            "entry_execution_intent": "RESTING_PULLBACK_LIMIT",
            "entry_pullback_offset_points": "1.0",
            "entry_working_window_seconds": "240",
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29752.25),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_PULLBACK_LIMIT"
    assert pricing["limit_price"] == 29751.0
    assert pricing["pullback_offset_points"] == 1.0
    assert pricing["fill_timeout_seconds"] == 240.0
    assert "working_window_expired" in pricing["cancel_on"]


def test_resting_pullback_intent_can_be_inferred_from_explicit_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="custom_pullback_lane",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="custom_pullback_lane",
                source_instrument="MNQ",
                executable_proxy="MNQ",
            ),
            "entry_pullback_offset_points": "1.0",
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29752.25),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["entry_execution_intent"] == "RESTING_PULLBACK_LIMIT"
    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_PULLBACK_LIMIT"
    assert pricing["limit_price"] == 29751.0


def test_retest_lane_name_does_not_silently_infer_resting_without_price_metadata(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="GC", close=3350.0)
    config = _config(
        tmp_path,
        strategy_id="gc_asia_early_normal_breakout_retest_hold_long",
        symbol="GC",
        contract_month="202606",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_asia_early_normal_breakout_retest_hold_long",
            source_instrument="GC",
            executable_proxy="GC",
        ),
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=3349.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.1),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["entry_execution_intent"] == "PARTICIPATE_NOW"
    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["block_submit"] is False


def test_resting_pullback_entry_blocks_without_strategy_limit_or_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="custom_pullback_lane",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="custom_pullback_lane",
                source_instrument="MNQ",
                executable_proxy="MNQ",
            ),
            "entry_execution_intent": "RESTING_PULLBACK_LIMIT",
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29752.25),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "RESTING_ENTRY_LIMIT_NOT_DEFINED"


def test_passive_only_entry_blocks_without_strategy_limit_or_offset(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="custom_passive_lane",
        symbol="MNQ",
        contract_month="202606",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="custom_passive_lane",
                source_instrument="MNQ",
                executable_proxy="MNQ",
            ),
            "entry_execution_intent": "PASSIVE_ONLY",
        },
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(ask=29752.25),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "PASSIVE_ENTRY_LIMIT_NOT_DEFINED"
    assert pricing["live_money_eligible"] is False


def test_passive_entry_miss_maps_to_accepted_not_failure() -> None:
    delegated = {
        "classification": "PAPER_ORDER_SUBMITTED_NOT_FILLED_CANCELLED",
        "report": {"submit_cancel_lifecycle": {"status": "fill_timeout_cancelled"}},
        "entry_execution_pricing": {
            "is_entry": True,
            "entry_execution_intent": "PASSIVE_ONLY",
        },
    }

    assert _map_delegate_classification(delegated) == "PAPER_STRATEGY_ENTRY_MISSED_CANCELLED_ACCEPTED"


def test_mnq_midday_long_declares_participate_now_entry_intent() -> None:
    adapter = lane_submit_bridge_adapter(lane_id="mnq_1x_ny_early_core__us_midday_long")

    assert adapter is not None
    assert adapter["entry_execution_intent"] == "PARTICIPATE_NOW"
    assert adapter["entry_working_window_seconds"] == 60


def test_entry_execution_pricing_failure_is_a_blocking_preflight_check(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        caller_metadata=_approved_runtime_metadata(strategy_id="ATP_COMPANION_V1_ASIA_US"),
    )
    pricing = {
        "is_entry": True,
        "execution_price_source": "IBKR_DELAYED_DIAGNOSTIC_ONLY",
        "block_submit": True,
        "block_reason": "DELAYED_QUOTE_NOT_EXECUTION_SAFE",
    }
    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=0.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.1),
        audit_events=[],
        entry_execution_pricing=pricing,
    )

    failure = next(row for row in checks if row["name"] == "entry_execution_price_source")
    assert failure["passed"] is False
    assert failure["blocking"] is True
    assert failure["detail"] == "DELAYED_QUOTE_NOT_EXECUTION_SAFE"


def test_entry_preflight_blocks_opposite_direction_when_broker_quantity_nonzero(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mes_us_active_participation_short",
        symbol="MES",
        action="SELL",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mes_us_active_participation_short",
            source_instrument="MES",
            executable_proxy="MES",
            action="SELL",
            intent_type="SELL_TO_OPEN",
        ),
    )
    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=1.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        audit_events=[],
        entry_execution_pricing={"is_entry": True, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE", "block_submit": False},
    )

    failure = next(row for row in checks if row["name"] == "same_symbol_broker_qty_anti_flip")
    assert failure["passed"] is False
    assert failure["blocking"] is True
    assert "SAME_SYMBOL_BROKER_QTY_ANTI_FLIP_LOCK" in failure["detail"]


def test_entry_preflight_blocks_same_direction_stacking_when_broker_quantity_nonzero(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long_2",
        symbol="MNQ",
        action="BUY",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_long_2",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="BUY",
            intent_type="BUY_TO_OPEN",
        ),
    )
    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=1.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        audit_events=[],
        entry_execution_pricing={"is_entry": True, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE", "block_submit": False},
    )

    failure = next(row for row in checks if row["name"] == "same_symbol_broker_qty_anti_flip")
    assert failure["passed"] is False
    assert failure["blocking"] is True


def test_entry_preflight_allows_flat_broker_quantity(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_us_active_participation_short",
        symbol="MNQ",
        action="SELL",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_us_active_participation_short",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="SELL",
            intent_type="SELL_TO_OPEN",
        ),
    )
    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=0.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        audit_events=[],
        entry_execution_pricing={"is_entry": True, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE", "block_submit": False},
    )

    row = next(row for row in checks if row["name"] == "same_symbol_broker_qty_anti_flip")
    assert row["passed"] is True
    assert row["blocking"] is True


def test_managed_close_preflight_not_blocked_by_entry_broker_quantity_lock(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mes_us_active_participation_long",
        symbol="MES",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="mes_us_active_participation_long",
                source_instrument="MES",
                executable_proxy="MES",
                action="SELL",
                intent_type="SELL_TO_CLOSE",
            ),
            "lifecycle_id": "managed-mes-long",
        },
    )
    intent = _intent_from_config(config)
    policy = _exit_attempt_policy_for_bridge(
        config=config,
        intent=intent,
        history_events=[],
        current_position_quantity=1.0,
        open_orders={"open_order_count": 0},
        phase1_gate={"ready": True},
    )
    checks = _build_preflight_checks(
        config=config,
        intent=intent,
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=1.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.25),
        audit_events=[],
        exit_attempt_policy=policy,
        entry_execution_pricing={"is_close": True, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE", "block_submit": False},
    )

    assert not any(row["name"] == "same_symbol_broker_qty_anti_flip" for row in checks)
    close_row = next(row for row in checks if row["name"] == "broker_position_present_for_close")
    assert close_row["passed"] is True


def test_futures_contract_resolver_blocks_new_mgc_june_entry_before_submit(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        caller_metadata={
            **_approved_runtime_metadata(strategy_id="ATP_COMPANION_V1_ASIA_US", source_instrument="MGC", executable_proxy="MGC"),
            "intent_type": "BUY_TO_OPEN",
        },
    )
    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=0.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report={
            "ok": True,
            "qualified_contract": {
                "symbol": "MGC",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "min_tick": 0.1,
            },
            "qualified_contract_identifier": 712565978,
            "api_contract_details": [
                {
                    "symbol": "MGC",
                    "expiry": "20260626",
                    "con_id": 712565978,
                    "local_symbol": "MGCM6",
                    "min_tick": 0.1,
                    "multiplier": "10",
                    "updated_at": "2999-01-01T00:00:00+00:00",
                }
            ],
        },
        audit_events=[],
        entry_execution_pricing={
            "is_entry": True,
            "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
            "block_submit": False,
        },
    )

    resolver = next(row for row in checks if row["name"] == "futures_contract_resolver")
    assert resolver["passed"] is False
    assert resolver["blocking"] is True
    assert resolver["blocker"] == "CONTRACT_NEAR_EXPIRY"
    assert resolver["recommended_contract"]["contract_month"] == "202608"


def test_futures_contract_resolver_does_not_rewrite_existing_lifecycle_exit_contract(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata={
            **_approved_runtime_metadata(
                strategy_id="ATP_COMPANION_V1_ASIA_US",
                source_instrument="MGC",
                executable_proxy="MGC",
                action="SELL",
                intent_type="SELL_TO_CLOSE",
            ),
            "lifecycle_id": "existing-mgc-lifecycle",
        },
    )
    intent = _intent_from_config(config)
    policy = _exit_attempt_policy_for_bridge(
        config=config,
        intent=intent,
        history_events=[],
        current_position_quantity=1.0,
        open_orders={"open_order_count": 0},
        phase1_gate={"ready": True},
    )
    checks = _build_preflight_checks(
        config=config,
        intent=intent,
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=1.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report={
            "ok": True,
            "qualified_contract": {
                "symbol": "MGC",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "min_tick": 0.1,
            },
            "api_contract_details": [],
        },
        audit_events=[],
        exit_attempt_policy=policy,
        entry_execution_pricing={
            "is_close": True,
            "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
            "block_submit": False,
        },
    )

    resolver = next(row for row in checks if row["name"] == "futures_contract_resolver")
    assert resolver["passed"] is True
    assert resolver["blocker"] == "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED"


def test_stop_exit_uses_hard_policy_and_runtime_price_source(tmp_path: Path) -> None:
    _write_runtime_1m_candle(
        tmp_path,
        symbol="MGC",
        close=4570.8,
        bar_end="2026-05-14T12:25:00+00:00",
    )
    metadata = {
        **_approved_runtime_metadata(
            strategy_id="mgc_1x_all_lanes__london_early_long",
            source_instrument="MGC",
            executable_proxy="MGC",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            bridge_proxy_mode="MGC_SIGNAL_DIRECT_PHASE1",
        ),
        "lifecycle_id": "bridge_fill_MGC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "exit_reason": "LONG_TIME_EXIT",
        "all_true_reasons": ["LONG_STOP", "LONG_INTEGRITY_FAIL", "LONG_TIME_EXIT"],
        "hard_exit": False,
    }
    config = _config(
        tmp_path,
        strategy_id="mgc_1x_all_lanes__london_early_long",
        symbol="MGC",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata=metadata,
        reason="segment_overrun",
    )
    intent = _intent_from_config(config)
    policy = _exit_attempt_policy_for_bridge(
        config=config,
        intent=intent,
        history_events=[],
        current_position_quantity=1.0,
        open_orders={"open_order_count": 0},
        phase1_gate={"ready": True},
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=intent,
        quote_context=_quote_context(bid=4560.1, ask=4560.3, last=4560.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.1),
        exit_attempt_policy=policy,
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    assert policy.hard_exit is True
    assert policy.discretionary_exit is False
    assert policy.execution_policy == "HARD_MARKETABLE_LIMIT_1T"
    assert pricing["exit_urgency"] == "HARD_PROTECTIVE"
    assert pricing["hard_exit_reason_matches"] == ["LONG_STOP", "LONG_INTEGRITY_FAIL", "STOP", "INTEGRITY_FAIL"]
    assert pricing["selected_exit_policy"] == "HARD_MARKETABLE_LIMIT_1T"
    assert pricing["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert pricing["runtime_reference_price"] == 4570.8
    assert pricing["limit_price"] == 4570.7
    assert pricing["marketable_by_runtime_context"] is True
    assert pricing["block_submit"] is False
    assert pricing["live_money_eligible"] is False


def test_delayed_only_stop_exit_blocks_as_low_confidence_price_source(tmp_path: Path) -> None:
    metadata = {
        **_approved_runtime_metadata(
            strategy_id="mgc_1x_all_lanes__london_early_long",
            source_instrument="MGC",
            executable_proxy="MGC",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            bridge_proxy_mode="MGC_SIGNAL_DIRECT_PHASE1",
        ),
        "lifecycle_id": "bridge_fill_MGC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "exit_reason": "LONG_STOP",
    }
    config = _config(
        tmp_path,
        strategy_id="mgc_1x_all_lanes__london_early_long",
        symbol="MGC",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata=metadata,
        reason="segment_overrun",
    )
    intent = _intent_from_config(config)
    policy = _exit_attempt_policy_for_bridge(
        config=config,
        intent=intent,
        history_events=[],
        current_position_quantity=1.0,
        open_orders={"open_order_count": 0},
        phase1_gate={"ready": True},
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=intent,
        quote_context=_quote_context(bid=4570.1, ask=4570.3, last=4570.0),
        qualified_contract_report=_qualified_contract_report(min_tick=0.1),
        exit_attempt_policy=policy,
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )
    checks = _build_preflight_checks(
        config=config,
        intent=intent,
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 0},
        current_position_quantity=1.0,
        quote_context=_quote_context(bid=4570.1, ask=4570.3, last=4570.0),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(min_tick=0.1),
        audit_events=[],
        exit_attempt_policy=policy,
        entry_execution_pricing=pricing,
    )

    assert policy.hard_exit is True
    assert pricing["execution_price_source"] == "IBKR_DELAYED_DIAGNOSTIC_ONLY"
    assert pricing["block_submit"] is True
    assert pricing["block_reason"] == "LOW_CONFIDENCE_PRICE_SOURCE"
    assert pricing["delayed_quote_limit_marketable_by_delayed_quote"] is True
    failure = next(row for row in checks if row["name"] == "exit_execution_price_source")
    assert failure["passed"] is False
    assert failure["blocking"] is True
    assert failure["detail"] == "LOW_CONFIDENCE_PRICE_SOURCE"


def test_entry_attempt_memory_counts_repeated_not_filled_cancelled_attempts(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
        ),
    )
    intent = _intent_from_config(config)
    history = [
        {
            "event_type": "delegated_manual_harness_completed",
            "delegated_classification": "PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED",
            "intent": {**intent.to_dict(), "reason": config.reason},
            "caller_metadata": dict(config.caller_metadata or {}),
        },
        {
            "event_type": "delegated_manual_harness_completed",
            "delegated_classification": "PAPER_STRATEGY_ORDER_FILLED",
            "intent": {**intent.to_dict(), "reason": config.reason},
            "caller_metadata": dict(config.caller_metadata or {}),
        },
    ]

    memory = _build_entry_attempt_memory(history_events=history, config=config, intent=intent)

    assert memory["entry_attempt_count"] == 2
    assert memory["not_filled_cancelled_count"] == 1
    assert memory["last_cancel_reason"] == "PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED"
    assert memory["same_setup_retry_count"] == 2


def test_working_order_still_blocks_duplicate_entry_even_with_runtime_pricing(tmp_path: Path) -> None:
    _write_runtime_1m_candle(tmp_path, symbol="MNQ", close=29752.0)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
        ),
    )
    pricing = _entry_execution_pricing_for_bridge(
        config=config,
        intent=_intent_from_config(config),
        quote_context=_quote_context(),
        qualified_contract_report=_qualified_contract_report(),
        now=datetime(2026, 5, 14, 12, 25, 30, tzinfo=timezone.utc),
    )

    checks = _build_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        selected_account_id="DUM882026",
        open_orders={"open_order_count": 1},
        current_position_quantity=0.0,
        quote_context=_quote_context(),
        exact_contract_report={"exact_contract": {}},
        qualified_contract_report=_qualified_contract_report(),
        audit_events=[],
        entry_execution_pricing=pricing,
    )

    duplicate_guard = next(row for row in checks if row["name"] == "no_working_orders")
    assert duplicate_guard["passed"] is False
    assert duplicate_guard["blocking"] is True


def test_schema_contains_required_fields() -> None:
    schema = strategy_order_intent_schema()

    assert schema["properties"]["strategy_id"]["type"] == "string"
    assert schema["properties"]["action"]["enum"] == ["BUY", "EXIT", "HOLD", "NO_ACTION", "SELL"]
    assert "paper_only" in schema["required"]
    assert "current_strategy_state" in schema["properties"]
    assert "contract_target" in schema["properties"]


def test_quote_is_fresh_accepts_delayed_frozen_label() -> None:
    assert _quote_is_fresh(
        {
            "updated_at": "2999-01-01T00:00:00+00:00",
            "quote_source_label": "DELAYED_FROZEN",
        }
    )


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


def test_probationary_runtime_caller_is_allowed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.probationary_runtime"}))],
    )

    assert result["passed"] is True


def test_unknown_caller_fails_closed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="unknown_runtime_submitter",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.probationary_runtime"}))],
    )

    assert result["passed"] is False
    assert "non-manual caller path" in result["detail"]


def test_live_style_caller_fails_closed_even_with_runtime_caller_path() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.live.runtime"}))],
    )

    assert result["passed"] is False
    assert "forbidden caller frames" in result["detail"]


def test_scheduler_style_caller_fails_closed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.paper_scheduler"}))],
    )

    assert result["passed"] is False
    assert "forbidden caller frames" in result["detail"]


def test_supervised_runtime_strategy_engine_frame_is_allowed_with_valid_metadata() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(strategy_id="gc_1x_all_lanes__asia_early_long"),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.strategy_engine"}))],
    )

    assert result["passed"] is True


def test_strategy_engine_frame_without_runtime_metadata_fails_closed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.strategy_engine"}))],
    )

    assert result["passed"] is False
    assert "forbidden caller frames" in result["detail"]


def test_live_mode_strategy_engine_frame_fails_closed_even_with_metadata() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_1x_all_lanes__asia_early_long",
            mode="LIVE",
        ),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.strategy_engine"}))],
    )

    assert result["passed"] is False
    assert "forbidden caller frames" in result["detail"]


def test_scheduler_style_runtime_with_valid_metadata_still_fails_closed() -> None:
    result = evaluate_strategy_bridge_caller(
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(strategy_id="gc_1x_all_lanes__asia_early_long"),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.paper_scheduler"}))],
    )

    assert result["passed"] is False
    assert "forbidden caller frames" in result["detail"]


def test_invalid_symbol_fails_before_connect(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_bridge(config=_config(tmp_path, symbol="CL"))

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "No approved phase-1 execution target exists" in json.dumps(artifacts.report)


def test_ported_gc_lane_passes_static_submit_gate_with_full_size_gc_execution_target(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
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
            "exact_contract": {"symbol": "GC", "expiry": "202606", "con_id": 712565978, "local_symbol": "GCM6"},
            "block_reasons": [],
        },
        governance_status=_healthy_lane_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "strategy_allowlist")["passed"] is True
    assert next(row for row in checks if row["name"] == "selected_lane_adapter_present")["passed"] is True


def test_ported_es_lane_passes_static_submit_gate_with_full_size_es_execution_target(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="es_1x_ny_early_core__us_midday_long",
        symbol="ES",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="es_1x_ny_early_core__us_midday_long",
            source_instrument="ES",
            executable_proxy="ES",
            bridge_proxy_mode="ES_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-30T12:26:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "ES", "expiry": "20260619", "con_id": 123, "local_symbol": "ESM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "es_1x_ny_early_core__us_midday_long",
                "bridge_strategy_id": "es_1x_ny_early_core__us_midday_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert all(row["passed"] for row in checks)


def test_supervised_runtime_route_uses_lane_authoritative_target_matrix_even_with_stale_monitor_exact_contract(tmp_path: Path) -> None:
    cases = [
        (
            "gc_1x_asia_london_participation__asia_london_long_v5",
            "GC",
            "GC",
            "GC_SIGNAL_DIRECT_PHASE1",
            "GC",
            "202606",
            "GCM6",
        ),
        (
            "mgc_1x_asia_london_participation__asia_london_long_v5",
            "MGC",
            "MGC",
            "MGC_SIGNAL_DIRECT_PHASE1",
            "MGC",
            "20260626",
            "MGCM6",
        ),
        (
            "es_1x_ny_early_core__us_midday_long",
            "ES",
            "ES",
            "ES_SIGNAL_DIRECT_PHASE1",
            "ES",
            "20260619",
            "ESM6",
        ),
        (
            "mes_1x_ny_early_core__us_midday_long",
            "MES",
            "MES",
            "MES_SIGNAL_DIRECT_PHASE1",
            "MES",
            "20260619",
            "MESM6",
        ),
        (
            "nq_1x_asia_london_participation__asia_london_long_v5",
            "NQ",
            "NQ",
            "NQ_SIGNAL_DIRECT_PHASE1",
            "NQ",
            "20260619",
            "NQM6",
        ),
        (
            "mnq_1x_asia_london_participation__asia_london_long_v5",
            "MNQ",
            "MNQ",
            "MNQ_SIGNAL_DIRECT_PHASE1",
            "MNQ",
            "20260619",
            "MNQM6",
        ),
    ]
    for strategy_id, source_instrument, executable_proxy, proxy_mode, exact_symbol, exact_expiry, exact_local_symbol in cases:
        config = _config(
            tmp_path,
            submit=True,
            strategy_id=strategy_id,
            symbol=executable_proxy,
            caller_path="probationary_paper_runtime_lane",
            caller_metadata=_approved_runtime_metadata(
                strategy_id=strategy_id,
                source_instrument=source_instrument,
                executable_proxy=executable_proxy,
                bridge_proxy_mode=proxy_mode,
            ),
            manual_frozen_preview_path=None,
            approval_digest=None,
            approval_phrase=None,
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
            timestamp="2026-05-01T12:30:00+00:00",
            risk_tags=config.risk_tags,
            paper_only=config.paper_only,
        )
        checks = _build_static_preflight_checks(
            config=config,
            intent=intent,
            environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
            caller_gate={"passed": True, "detail": "approved runtime caller"},
            monitor_status={
                "monitor_running": True,
                "submit_allowed": True,
                "health_classification": "HEALTHY",
                "account_id": "DUM882026",
                # Deliberately stale global snapshot; current supervised PAPER route
                # must use the lane-authoritative execution target instead.
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "block_reasons": [],
            },
            governance_status={
                "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
                "submit_allowed": True,
                "block_reasons": [],
                "selected_strategy": {
                    "strategy_id": strategy_id,
                    "bridge_strategy_id": strategy_id,
                    "strategy_status": "PROBATION_ACTIVE",
                    "submit_allowed": True,
                    "submit_block_reasons": [],
                },
            },
            exposure_status=_healthy_exposure(),
        )
        assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True
        assert next(row for row in checks if row["name"] == "executable_contract_whitelist")["passed"] is True
        assert next(row for row in checks if row["name"] == "contract_month_lock")["passed"] is True
        assert next(row for row in checks if row["name"] == "paper_strategy_monitor_contract_match")["passed"] is True
        assert all(row["passed"] for row in checks), strategy_id
        assert all("MGC only" not in str(row["detail"]) for row in checks), strategy_id


def test_live_style_mes_and_nq_runtime_routes_ignore_stale_global_monitor_contract_snapshot(tmp_path: Path) -> None:
    cases = [
        (
            "mes_1x_ny_early_core__us_midday_short_breakdown",
            "MES",
            "MES",
            "MES_SIGNAL_DIRECT_PHASE1",
            "SELL",
            "SELL_TO_OPEN",
        ),
        (
            "nq_1x_ny_early_core__us_midday_long",
            "NQ",
            "NQ",
            "NQ_SIGNAL_DIRECT_PHASE1",
            "BUY",
            "BUY_TO_OPEN",
        ),
    ]
    for strategy_id, source_instrument, executable_proxy, proxy_mode, action, intent_type in cases:
        config = _config(
            tmp_path,
            submit=True,
            strategy_id=strategy_id,
            symbol=executable_proxy,
            action=action,
            caller_path="probationary_paper_runtime_lane",
            caller_metadata=_approved_runtime_metadata(
                strategy_id=strategy_id,
                source_instrument=source_instrument,
                executable_proxy=executable_proxy,
                action=action,
                intent_type=intent_type,
                bridge_proxy_mode=proxy_mode,
            ),
            manual_frozen_preview_path=None,
            approval_digest=None,
            approval_phrase=None,
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
            timestamp="2026-05-01T16:23:00+00:00",
            risk_tags=config.risk_tags,
            paper_only=config.paper_only,
        )
        checks = _build_static_preflight_checks(
            config=config,
            intent=intent,
            environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
            caller_gate={"passed": True, "detail": "approved runtime caller"},
            monitor_status={
                "monitor_running": True,
                "submit_allowed": True,
                "health_classification": "HEALTHY",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "block_reasons": [],
                "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
                "broker_position_quantity": 0.0,
                "ledger_position_quantity": 0.0,
                "open_order_count": 0,
            },
            governance_status={
                "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
                "submit_allowed": True,
                "block_reasons": [],
                "selected_strategy": {
                    "strategy_id": strategy_id,
                    "bridge_strategy_id": strategy_id,
                    "strategy_status": "PROBATION_ACTIVE",
                    "submit_allowed": True,
                    "submit_block_reasons": [],
                },
            },
            exposure_status=_healthy_exposure(),
        )

        assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True
        assert next(row for row in checks if row["name"] == "paper_strategy_monitor_contract_match")["passed"] is True


def test_bridge_gate_does_not_fail_just_because_schwab_is_unavailable(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("SCHWAB_APP_KEY", raising=False)
    monkeypatch.delenv("SCHWAB_APP_SECRET", raising=False)
    monkeypatch.delenv("SCHWAB_CALLBACK_URL", raising=False)
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "fallback_unavailable")
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
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
            "exact_contract": {"symbol": "GC", "expiry": "202606", "con_id": 712565978, "local_symbol": "GCM6"},
            "block_reasons": [],
        },
        governance_status=_healthy_lane_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert all(row["passed"] for row in checks)


def test_submit_requires_manual_harness_bundle(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_bridge(config=_config(tmp_path, submit=True))

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "manual-harness frozen preview" in json.dumps(artifacts.report)
    assert not (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "submit_intent_ownership"
        / "track_b_submit_intent_ownership.jsonl"
    ).exists()


def test_direct_bridge_submit_blocked_without_snapshot(tmp_path: Path) -> None:
    _write_runtime_files(tmp_path, governance_status=_healthy_governance())
    manual_bundle = tmp_path / "frozen_preview.json"

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            submit=True,
            manual_frozen_preview_path=manual_bundle,
            approval_digest="digest",
            approval_phrase="phrase",
        )
    )

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert artifacts.report["bridge_direct_invocation"] is True
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"


def test_direct_bridge_submit_blocked_on_snapshot_target_mismatch(tmp_path: Path) -> None:
    _write_runtime_files(tmp_path, governance_status=_healthy_governance())
    _write_strategy_bridge_snapshot(
        tmp_path,
        target_identity={**_strategy_bridge_target_identity(), "symbol": "MNQ"},
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            submit=True,
            manual_frozen_preview_path=tmp_path / "frozen_preview.json",
            approval_digest="digest",
            approval_phrase="phrase",
        )
    )

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH"


def test_direct_bridge_submit_blocked_when_supervisor_not_trade_capable(tmp_path: Path) -> None:
    _write_runtime_files(tmp_path, governance_status=_healthy_governance())
    _write_strategy_bridge_snapshot(
        tmp_path,
        supervisor_classification="SUPERVISOR_WAIT_MARKET_CLOSED",
        safe_to_start_runtime=False,
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            submit=True,
            manual_frozen_preview_path=tmp_path / "frozen_preview.json",
            approval_digest="digest",
            approval_phrase="phrase",
        )
    )

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    trade_gate = artifacts.report["pre_action_snapshot_validation"]["snapshot_trade_capable"]
    assert trade_gate["passed"] is False
    assert "safe_to_start_runtime" in trade_gate["detail"]


def test_lifecycle_validation_entry_uses_dedicated_authority_not_runtime_start_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path)
    _write_runtime_files(tmp_path, governance_status=_healthy_lane_governance())
    _write_strategy_bridge_snapshot(
        tmp_path,
        target_identity={
            "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "symbol": "GC",
            "contract_month": "202606",
            "contract": "GCM6",
            "con_id": "",
            "action": "BUY",
            "quantity": "1.0",
            "caller_path": "track_b_paper_leak_test_apply",
        },
        supervisor_classification="SUPERVISOR_RUNTIME_ALREADY_HEALTHY",
        safe_to_start_runtime=False,
    )
    monkeypatch.setattr(
        bridge_module,
        "_build_runtime",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("runtime next gate reached")),
    )
    monkeypatch.setattr(
        bridge_module,
        "evaluate_paper_strategy_exposure_gate",
        lambda **_kwargs: _healthy_exposure(),
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            submit=True,
            caller_path="track_b_paper_leak_test_apply",
            leak_test_authorization_path=auth_path,
            leak_test_authorization_digest=digest,
            caller_metadata={
                "caller_type": "track_b_paper_leak_test",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "strategy_id": "asia_london_participation_core_v1__GC",
                "route_destination": "ibkr_paper_bridge_submit_capable",
                "intent_type": "BUY_TO_OPEN",
                "intent_action": "BUY",
                "account_id": "DUM882026",
                "mode": "PAPER",
                "host": "127.0.0.1",
                "port": 7497,
                "local_symbol": "GCM6",
                "paper_only": True,
                "live_money_eligible": False,
            },
        )
    )

    validation = artifacts.report["pre_action_snapshot_validation"]
    assert validation["classification"] == "LIFECYCLE_VALIDATION_ENTRY_AUTHORITY_VALID"
    assert validation["authority_classification"] == "PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED"
    assert validation["not_runtime_start_authority"] is True
    assert "runtime next gate reached" in artifacts.report["detail"]


def test_runtime_supervised_bridge_reaches_existing_next_gate_with_snapshot(monkeypatch, tmp_path: Path) -> None:
    _write_runtime_files(tmp_path, governance_status=_healthy_lane_governance())
    snapshot = _write_strategy_bridge_snapshot(tmp_path)
    metadata = _runtime_bridge_metadata(snapshot=snapshot)
    monkeypatch.setattr(
        bridge_module,
        "_build_runtime",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("runtime next gate reached")),
    )
    monkeypatch.setattr(
        bridge_module,
        "evaluate_paper_strategy_exposure_gate",
        lambda **_kwargs: _healthy_exposure(),
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            submit=True,
            caller_path="probationary_paper_runtime_lane",
            caller_metadata=metadata,
        ),
        stack_provider=lambda: [
            SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.probationary_runtime"}))
        ],
    )

    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert artifacts.report["runtime_supervised"] is True
    assert artifacts.report["runtime_control_plane_authorization"]["classification"] == "RUNTIME_CONTROL_PLANE_AUTHORIZATION_VALID"
    assert "runtime next gate reached" in artifacts.report["detail"]


def test_bridge_test_harness_path_non_mutating_by_default(tmp_path: Path) -> None:
    _write_runtime_files(tmp_path)

    artifacts = run_ibkr_paper_strategy_bridge(config=_config(tmp_path, submit=False))

    assert artifacts.report.get("pre_action_snapshot_validation", {}) == {}
    assert artifacts.report.get("delegated_result") is None


def test_bridge_does_not_consume_dashboard_projection_as_authority() -> None:
    source = Path(bridge_module.__file__).read_text(encoding="utf-8")
    assert "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json" not in source


def test_leak_test_caller_requires_valid_authorization(tmp_path: Path) -> None:
    _write_runtime_files(tmp_path, governance_status=_healthy_lane_governance())
    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            submit=True,
            caller_path="track_b_paper_leak_test_apply",
            caller_metadata={
                "caller_type": "track_b_paper_leak_test",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "strategy_id": "asia_london_participation_core_v1__GC",
                "route_destination": "ibkr_paper_bridge_submit_capable",
                "intent_type": "BUY_TO_OPEN",
                "intent_action": "BUY",
                "account_id": "DUM882026",
                "mode": "PAPER",
                "host": "127.0.0.1",
                "port": 7497,
                "local_symbol": "GCM6",
                "paper_only": True,
                "live_money_eligible": False,
            },
        )
    )

    checks = {row["name"]: row for row in artifacts.report["preflight_checks"]}
    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert checks["approved_paper_caller_path"]["passed"] is True
    assert checks["leak_test_authorization"]["passed"] is False
    assert checks["manual_harness_bundle_present_for_submit"]["passed"] is False


def test_leak_test_caller_with_valid_authorization_satisfies_manual_bundle_gate(tmp_path: Path) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path)
    _write_runtime_files(tmp_path, governance_status=_healthy_lane_governance())
    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            submit=True,
            caller_path="track_b_paper_leak_test_apply",
            leak_test_authorization_path=auth_path,
            leak_test_authorization_digest=digest,
            caller_metadata={
                "caller_type": "track_b_paper_leak_test",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "strategy_id": "asia_london_participation_core_v1__GC",
                "route_destination": "ibkr_paper_bridge_submit_capable",
                "intent_type": "BUY_TO_OPEN",
                "intent_action": "BUY",
                "account_id": "DUM882026",
                "mode": "PAPER",
                "host": "127.0.0.1",
                "port": 7497,
                "local_symbol": "GCM6",
                "paper_only": True,
                "live_money_eligible": False,
            },
        )
    )

    checks = {row["name"]: row for row in artifacts.report["preflight_checks"]}
    assert checks["approved_paper_caller_path"]["passed"] is True
    assert checks["leak_test_authorization"]["passed"] is True
    assert checks["manual_harness_bundle_present_for_submit"]["passed"] is True


def test_explicit_metals_leak_test_flow_bypasses_only_legacy_runtime_loop_governance_block(tmp_path: Path) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path)
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
        contract_month="202606",
        caller_path="track_b_paper_leak_test_apply",
        leak_test_authorization_path=auth_path,
        leak_test_authorization_digest=digest,
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "source_instrument": "GC",
            "executable_proxy": "GC",
            "paper_only": True,
            "mode": "PAPER",
            "account_id": "DUM882026",
            "route_destination": "ibkr_paper_bridge_submit_capable",
            "intent_action": "BUY",
            "intent_type": "BUY_TO_OPEN",
            "local_symbol": "GCM6",
            "explicit_lane_flow": True,
            "metals_only": True,
            "runtime_loop_required": False,
        },
    )
    governance = {
        "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
        "submit_allowed": False,
        "block_reasons": ["backend_or_source_not_live_ready"],
        "selected_strategy": {
            "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "bridge_strategy_id": "asia_london_participation_core_v1__GC",
            "strategy_status": "PROBATION_ACTIVE",
            "submit_allowed": False,
            "submit_block_reasons": ["backend_or_source_not_live_ready"],
        },
    }

    checks = _build_static_preflight_checks(
        config=config,
        intent=_intent_from_config(config),
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved leak-test caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "GC", "expiry": "202606", "con_id": None, "local_symbol": "GCM6"},
            "block_reasons": [],
        },
        governance_status=governance,
        exposure_status=_healthy_exposure(),
    )

    by_name = {row["name"]: row for row in checks}
    assert by_name["leak_test_authorization"]["passed"] is True
    assert by_name["paper_strategy_governance_submit_gate"]["passed"] is True
    assert "explicit metals-only Leak Test v2 lane flow" in by_name["paper_strategy_governance_submit_gate"]["detail"]


def test_leak_test_caller_uses_lifecycle_validation_entry_authority_without_pre_action_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path)
    _write_runtime_files(tmp_path, governance_status=_healthy_lane_governance())
    _write_strategy_bridge_snapshot(tmp_path)
    plan_path = tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json"
    plan_path.unlink()
    monkeypatch.setattr(
        bridge_module,
        "_build_runtime",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("runtime next gate reached")),
    )
    monkeypatch.setattr(
        bridge_module,
        "evaluate_paper_strategy_exposure_gate",
        lambda **_kwargs: _healthy_exposure(),
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            submit=True,
            caller_path="track_b_paper_leak_test_apply",
            leak_test_authorization_path=auth_path,
            leak_test_authorization_digest=digest,
            caller_metadata={
                "caller_type": "track_b_paper_leak_test",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "strategy_id": "asia_london_participation_core_v1__GC",
                "route_destination": "ibkr_paper_bridge_submit_capable",
                "intent_type": "BUY_TO_OPEN",
                "intent_action": "BUY",
                "account_id": "DUM882026",
                "mode": "PAPER",
                "host": "127.0.0.1",
                "port": 7497,
                "local_symbol": "GCM6",
                "paper_only": True,
                "live_money_eligible": False,
            },
        )
    )

    validation = artifacts.report["pre_action_snapshot_validation"]
    assert not plan_path.exists()
    assert validation["classification"] == "LIFECYCLE_VALIDATION_ENTRY_AUTHORITY_VALID"
    assert validation["authority_classification"] == "PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED"
    assert "runtime next gate reached" in artifacts.report["detail"]


def test_leak_test_close_authorization_checks_exit_action_not_entry_action(tmp_path: Path) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path, action="BUY")
    config = _config(
        tmp_path,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
        contract_month="202606",
        action="SELL",
        submit=True,
        caller_path="track_b_paper_leak_test_apply",
        leak_test_authorization_path=auth_path,
        leak_test_authorization_digest=digest,
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "route_destination": "ibkr_paper_bridge_submit_capable",
            "intent_type": "SELL_TO_CLOSE",
            "intent_action": "SELL",
            "account_id": "DUM882026",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "local_symbol": "GCM6",
            "paper_only": True,
            "live_money_eligible": False,
        },
    )

    check = bridge_module._leak_test_authorization_check(config=config, intent=_intent_from_config(config))

    assert check["passed"] is True


def test_leak_test_close_authorization_rejects_wrong_exit_action(tmp_path: Path) -> None:
    auth_path, _digest = _write_leak_authorization(tmp_path, action="BUY")
    payload = json.loads(auth_path.read_text(encoding="utf-8"))
    payload["exit_action"] = "BUY"
    payload["digest"] = _leak_auth_digest(payload)
    auth_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    config = _config(
        tmp_path,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
        contract_month="202606",
        action="SELL",
        submit=True,
        caller_path="track_b_paper_leak_test_apply",
        leak_test_authorization_path=auth_path,
        leak_test_authorization_digest=str(payload["digest"]),
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "route_destination": "ibkr_paper_bridge_submit_capable",
            "intent_type": "SELL_TO_CLOSE",
            "intent_action": "SELL",
            "account_id": "DUM882026",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "local_symbol": "GCM6",
            "paper_only": True,
            "live_money_eligible": False,
        },
    )

    check = bridge_module._leak_test_authorization_check(config=config, intent=_intent_from_config(config))

    assert check["passed"] is False
    assert "exit_action" in str(check["detail"])


def test_leak_test_close_uses_lifecycle_owner_strategy_for_exposure(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        action="SELL",
        caller_path="track_b_paper_leak_test_apply",
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "intent_type": "SELL_TO_CLOSE",
        },
    )

    assert bridge_module._exposure_strategy_id_for_bridge(config=config) == "asia_london_participation_core_v1__GC"


def test_leak_test_entry_uses_lane_strategy_for_exposure(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        caller_path="track_b_paper_leak_test_apply",
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "intent_type": "BUY_TO_OPEN",
        },
    )

    assert bridge_module._exposure_strategy_id_for_bridge(config=config) == "gc_1x_asia_london_participation__asia_london_long_v5"


def test_close_submit_unknown_persists_known_managed_exit_order_state(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
        contract_month="202606",
        action="SELL",
        submit=True,
        caller_path="track_b_paper_leak_test_apply",
        output_dir=Path("outputs") / "reports" / "track_b_paper_leak_test" / "gc_lane",
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "strategy_id": "asia_london_participation_core_v1__GC",
            "intent_type": "SELL_TO_CLOSE",
            "account_id": "DUM882026",
            "local_symbol": "GCM6",
            "lifecycle_id": "bridge_fill_gc-test",
        },
    )

    persistence = bridge_module._persist_known_managed_exit_order_after_submit(
        config=config,
        intent=_intent_from_config(config),
        delegated_result={
            "classification": "PAPER_CLOSE_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
            "report": {
                "submit_cancel_lifecycle": {
                    "status": "manual_confirmation_unavailable",
                    "submitted_order_id": 5,
                    "submitted_perm_id": None,
                    "open_order_after_submit": {"client_id": 11940},
                }
            },
        },
        qualified_contract_report=_qualified_contract_report(),
        entry_execution_pricing={"limit_price": 4556.0},
        exit_attempt_policy=None,
    )

    assert persistence == {
        "persisted": True,
        "path": str(tmp_path / "outputs" / "track_b_execution_core" / "managed_exit_orders" / "latest_known_managed_exit_orders.json"),
        "broker_order_id": "5",
        "client_id": 11940,
        "perm_id": None,
        "lifecycle_id": "bridge_fill_gc-test",
        "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
    }
    state_path = tmp_path / "outputs" / "track_b_execution_core" / "managed_exit_orders" / "latest_known_managed_exit_orders.json"
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    row = payload["known_managed_exit_orders"][0]
    assert row["broker_order_id"] == "5"
    assert row["strategy_id"] == "asia_london_participation_core_v1__GC"
    assert row["lane_id"] == "gc_1x_asia_london_participation__asia_london_long_v5"
    assert row["local_symbol"] == "GCM6"
    assert row["action"] == "SELL"
    assert row["limit_price"] == 4556.0
    assert row["paper_proof_invoked"] is False
    assert row["live_money_eligible"] is False


def test_leak_test_entry_unknown_persists_known_entry_order_state(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="gc_1x_all_lanes__london_early_long",
        symbol="GC",
        contract_month="202606",
        action="BUY",
        submit=True,
        caller_path="track_b_paper_leak_test_apply",
        output_dir=Path("outputs") / "reports" / "track_b_paper_leak_test" / "gc_1x_all_lanes__london_early_long",
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": "gc_1x_all_lanes__london_early_long",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
            "intent_type": "BUY_TO_OPEN",
            "account_id": "DUM882026",
            "local_symbol": "GCM6",
            "con_id": 430360630,
            "leak_test": True,
        },
    )

    persistence = bridge_module._persist_known_leak_test_entry_order_after_submit(
        config=config,
        intent=_intent_from_config(config),
        delegated_result={
            "classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
            "report": {
                "submit_cancel_lifecycle": {
                    "status": "manual_confirmation_unavailable",
                    "submitted_order_id": 12,
                    "submitted_perm_id": 614043263,
                    "open_order_after_submit": {"client_id": 11940},
                }
            },
        },
        qualified_contract_report=_qualified_contract_report(),
        entry_execution_pricing={
            "limit_price": 4556.0,
            "entry_execution_intent": "PARTICIPATE_NOW",
            "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
        },
    )

    state_path = tmp_path / "outputs" / "track_b_execution_core" / "leak_test_entry_orders" / "latest_known_leak_test_entry_orders.json"
    assert persistence == {
        "persisted": True,
        "path": str(state_path),
        "broker_order_id": "12",
        "client_id": 11940,
        "perm_id": 614043263,
        "managed_order_status": "KNOWN_LEAK_TEST_ENTRY_ORDER_WORKING",
    }
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    row = payload["known_leak_test_entry_orders"][0]
    assert row["broker_order_id"] == "12"
    assert row["strategy_id"] == "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long"
    assert row["lane_id"] == "gc_1x_all_lanes__london_early_long"
    assert row["local_symbol"] == "GCM6"
    assert row["action"] == "BUY"
    assert row["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert row["paper_proof_invoked"] is False
    assert row["live_money_eligible"] is False


def test_submit_intent_ownership_pre_submit_writes_reserved_entry_context(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        submit=True,
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            intent_type="BUY_TO_OPEN",
        )
        | {"git_head": "abc123"},
    )
    record = bridge_module._persist_submit_intent_ownership_before_delegate(
        config=config,
        intent=_intent_from_config(config),
        qualified_contract_report=_qualified_contract_report(),
        positions={"review_required_count": 0},
        open_orders={"open_order_count": 0, "unknown_open_order_count": 0},
        paper_strategy_governance_status={"classification": "PAPER_STRATEGY_GOVERNANCE_READY"},
        paper_strategy_exposure_status={"classification": "PAPER_EXPOSURE_ENTRY_ALLOWED"},
        entry_execution_pricing={
            "limit_price": 29752.25,
            "entry_execution_intent": "PARTICIPATE_NOW",
            "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
            "runtime_reference_price": 29752.0,
        },
        phase1_gate={"classification": "PHASE1_BROKER_RECONCILIATION_CLEAR", "review_required_count": 0},
    )

    jsonl = tmp_path / "outputs" / "track_b_execution_core" / "submit_intent_ownership" / "track_b_submit_intent_ownership.jsonl"
    latest = tmp_path / "outputs" / "track_b_execution_core" / "submit_intent_ownership" / "latest_track_b_submit_intent_ownership.json"
    payload = json.loads(jsonl.read_text(encoding="utf-8").splitlines()[0])
    assert record["state"] == "PRE_SUBMIT_INTENT_DURABLE"
    assert payload["state"] == "PRE_SUBMIT_INTENT_DURABLE"
    assert payload["lifecycle_id"].startswith("reserved_submit_")
    assert payload["lifecycle_id_reserved_only"] is True
    assert payload["lifecycle_position_open"] is False
    assert payload["symbol"] == "MNQ"
    assert payload["local_symbol"] == "MNQM6"
    assert payload["execution_price_source"] == "RUNTIME_DATABENTO_1M_CLOSE"
    assert latest.exists()
    assert not (tmp_path / "var" / "paper_strategy_position_ledger.json").exists()


def test_submit_intent_ownership_delegate_exception_leaves_recoverable_intent(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        submit=True,
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            intent_type="BUY_TO_OPEN",
        )
        | {"git_head": "abc123"},
    )
    pre_submit = bridge_module._persist_submit_intent_ownership_before_delegate(
        config=config,
        intent=_intent_from_config(config),
        qualified_contract_report=_qualified_contract_report(),
        positions={},
        open_orders={},
        paper_strategy_governance_status={"classification": "PAPER_STRATEGY_GOVERNANCE_READY"},
        paper_strategy_exposure_status={"classification": "PAPER_EXPOSURE_ENTRY_ALLOWED"},
        entry_execution_pricing={"limit_price": 29752.25, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
        phase1_gate={"classification": "PHASE1_BROKER_RECONCILIATION_CLEAR"},
    )

    update = bridge_module._persist_submit_intent_ownership_delegate_exception(
        config=config,
        pre_submit_record=pre_submit,
        exc=RuntimeError("delegate crashed after durable intent"),
    )

    assert update["state"] == "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED"
    assert update["ownership_intent_id"] == pre_submit["ownership_intent_id"]
    jsonl = tmp_path / "outputs" / "track_b_execution_core" / "submit_intent_ownership" / "track_b_submit_intent_ownership.jsonl"
    records = [json.loads(line) for line in jsonl.read_text(encoding="utf-8").splitlines()]
    assert [row["state"] for row in records] == [
        "PRE_SUBMIT_INTENT_DURABLE",
        "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED",
    ]
    assert "delegate crashed" in records[-1]["extra"]["delegated_exception_message"]


def test_pre_submit_no_broker_effect_exception_resolves_submit_ownership(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        symbol="MGC",
        contract_month="202606",
        submit=True,
        caller_metadata=_approved_runtime_metadata(
            strategy_id="track_b_paper_execution_test_mule_v1__mgc",
            source_instrument="MGC",
            executable_proxy="MGC",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            bridge_proxy_mode="MGC_SIGNAL_DIRECT_PHASE1",
        )
        | {"git_head": "abc123"},
    )
    pre_submit = bridge_module._persist_submit_intent_ownership_before_delegate(
        config=config,
        intent=_intent_from_config(config),
        qualified_contract_report={
            "ok": True,
            "qualified_contract": {
                "symbol": "MGC",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "min_tick": 0.1,
            },
            "qualified_contract_identifier": "712565978",
            "api_contract_details": [{"min_tick": 0.1, "multiplier": "10"}],
        },
        positions={},
        open_orders={},
        paper_strategy_governance_status={"classification": "PAPER_STRATEGY_GOVERNANCE_READY"},
        paper_strategy_exposure_status={"classification": "PAPER_EXPOSURE_ENTRY_ALLOWED"},
        entry_execution_pricing={"limit_price": 4526.3, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
        phase1_gate={"classification": "PHASE1_BROKER_RECONCILIATION_CLEAR"},
    )

    update = bridge_module._persist_submit_intent_ownership_delegate_exception(
        config=config,
        pre_submit_record=pre_submit,
        exc=bridge_module.IbkrPaperStrategyPreSubmitNoBrokerEffectError(
            "Paper strategy bridge could not prepare a valid frozen manual submit bundle."
        ),
    )

    jsonl = tmp_path / "outputs" / "track_b_execution_core" / "submit_intent_ownership" / "track_b_submit_intent_ownership.jsonl"
    records = [json.loads(line) for line in jsonl.read_text(encoding="utf-8").splitlines()]
    assert update["state"] == "NO_BROKER_EFFECT_CONFIRMED"
    assert update["broker_effect_classification"] == "PRE_SUBMIT_BLOCKED_NO_BROKER_EFFECT"
    assert records[-1]["extra"]["submit_sent"] is False
    assert records[-1]["extra"]["broker_effect"] is False
    assert load_unresolved_submit_intent_ownership_records(jsonl) == []


def test_mgc_sell_to_open_mule_prepares_entry_fill_bundle(monkeypatch, tmp_path: Path) -> None:
    captured: list[dict[str, object]] = []

    def fake_manual_submit(*, config, stack_provider):
        del stack_provider
        preview_path = frozen_preview_path_for_config(config)
        assert preview_path is not None
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        preview_path.write_text("{}", encoding="utf-8")
        captured.append(
            {
                "action": config.action,
                "symbol": config.symbol,
                "test_mode": config.test_mode,
                "submit": config.submit,
            }
        )
        return IbkrManualPaperSubmitArtifacts(
            classification="PAPER_FILL_TEST_PREVIEW_READY",
            report={
                "preview": {
                    "preview_digest": "digest-123",
                    "expected_approval_phrase": "APPROVE PAPER DIGEST digest-123",
                },
                "submit_cancel_lifecycle": {"detail": "preview ready"},
            },
            audit_events=[],
            open_order_before={},
            open_order_after_submit={},
            open_order_after_cancel={},
        )

    monkeypatch.setattr(bridge_module, "run_ibkr_manual_paper_submit_test", fake_manual_submit)
    config = _config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        symbol="MGC",
        contract_month="202606",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="track_b_paper_execution_test_mule_v1__mgc",
            source_instrument="MGC",
            executable_proxy="MGC",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            bridge_proxy_mode="MGC_SIGNAL_DIRECT_PHASE1",
        ),
    )

    intent = _intent_from_config(config)
    bundle = bridge_module._prepare_manual_submit_bundle(
        config=config,
        intent=intent,
        exit_attempt_policy=_exit_attempt_policy_for_bridge(
            config=config,
            intent=intent,
            history_events=[],
            current_position_quantity=0.0,
            open_orders={"open_order_count": 0},
            phase1_gate={"ready": True},
        ),
        entry_execution_pricing={
            "is_entry": True,
            "limit_price": 4526.3,
            "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
        },
    )

    assert bundle["preview_digest"] == "digest-123"
    assert captured == [
        {
            "action": "SELL",
            "symbol": "MGC",
            "test_mode": "PAPER_FILL_TEST",
            "submit": False,
        }
    ]


@pytest.mark.parametrize(
    ("delegated_result", "expected_state"),
    [
        (
            {
                "classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
                "report": {"lifecycle": {"status": "unknown_needs_review", "submitted_order_id": 28, "client_id": 11940}},
            },
            "BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED",
        ),
        (
            {
                "classification": "PAPER_ORDER_WORKING",
                "report": {
                    "lifecycle": {
                        "status": "submitted",
                        "submitted_order_id": 29,
                        "client_id": 11940,
                        "submitted_perm_id": 614044377,
                    }
                },
            },
            "BROKER_ORDER_WORKING",
        ),
        (
            {
                "classification": "PAPER_ORDER_SUBMITTED_NOT_FILLED_CANCELLED",
                "report": {"lifecycle": {"status": "fill_timeout_cancelled", "submitted_order_id": 30}},
            },
            "NOT_FILLED_CANCELLED",
        ),
        (
            {
                "classification": "PAPER_ORDER_REJECTED",
                "report": {"lifecycle": {"status": "rejected", "submitted_order_id": 31}},
            },
            "REJECTED",
        ),
        (
            {
                "classification": "PAPER_FILL_TEST_PASSED",
                "report": {
                    "lifecycle": {
                        "status": "filled",
                        "submitted_order_id": 32,
                        "client_id": 11940,
                        "submitted_perm_id": 614044378,
                        "exec_id": "0000e1a7.test",
                    }
                },
            },
            "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED",
        ),
    ],
)
def test_submit_intent_ownership_post_delegate_state_updates(
    tmp_path: Path,
    delegated_result: dict[str, object],
    expected_state: str,
) -> None:
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_midday_long",
        symbol="MNQ",
        contract_month="202606",
        submit=True,
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_midday_long",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            intent_type="BUY_TO_OPEN",
        )
        | {"git_head": "abc123"},
    )
    intent = _intent_from_config(config)
    pre_submit = bridge_module._persist_submit_intent_ownership_before_delegate(
        config=config,
        intent=intent,
        qualified_contract_report=_qualified_contract_report(),
        positions={},
        open_orders={},
        paper_strategy_governance_status={"classification": "PAPER_STRATEGY_GOVERNANCE_READY"},
        paper_strategy_exposure_status={"classification": "PAPER_EXPOSURE_ENTRY_ALLOWED"},
        entry_execution_pricing={"limit_price": 29752.25, "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE"},
        phase1_gate={"classification": "PHASE1_BROKER_RECONCILIATION_CLEAR"},
    )

    update = bridge_module._persist_submit_intent_ownership_after_delegate(
        config=config,
        intent=intent,
        pre_submit_record=pre_submit,
        delegated_result=delegated_result,  # type: ignore[arg-type]
    )

    assert update["state"] == expected_state
    assert update["ownership_intent_id"] == pre_submit["ownership_intent_id"]
    if expected_state in {"BROKER_ORDER_WORKING", "BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED"}:
        assert update["broker_order_id"] is not None


def test_leak_test_submit_handshake_failure_reports_paper_connection_config(tmp_path: Path) -> None:
    auth_path, digest = _write_leak_authorization(tmp_path)
    governance_status = _healthy_lane_governance()
    governance_status["generated_at"] = "2999-01-01T00:00:00+00:00"
    governance_status["strategies"] = [governance_status["selected_strategy"]]
    _write_runtime_files(tmp_path, governance_status=governance_status)
    _write_fresh_broker_truth(tmp_path)
    _write_strategy_bridge_snapshot(
        tmp_path,
        target_identity={
            "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
            "symbol": "GC",
            "contract_month": "202606",
            "action": "BUY",
            "quantity": "1.0",
            "intent_type": "BUY_TO_OPEN",
            "caller_path": "track_b_paper_leak_test_apply",
        },
    )

    artifacts = run_ibkr_paper_strategy_bridge(
        config=_config(
            tmp_path,
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            symbol="GC",
            contract_month="202606",
            client_id=10940,
            submit=True,
            timeout_seconds=0.01,
            caller_path="track_b_paper_leak_test_apply",
            leak_test_authorization_path=auth_path,
            leak_test_authorization_digest=digest,
            caller_metadata={
                "caller_type": "track_b_paper_leak_test",
                "lane_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "strategy_id": "asia_london_participation_core_v1__GC",
                "route_destination": "ibkr_paper_bridge_submit_capable",
                "intent_type": "BUY_TO_OPEN",
                "intent_action": "BUY",
                "account_id": "DUM882026",
                "mode": "PAPER",
                "host": "127.0.0.1",
                "port": 7497,
                "local_symbol": "GCM6",
                "paper_only": True,
                "live_money_eligible": False,
            },
        ),
        transport_factory=_HandshakeFailureTransport,
        sleep_fn=lambda _seconds: None,
    )

    report = artifacts.report
    diagnostics = report["connection_diagnostics"]
    assert artifacts.classification == "PAPER_STRATEGY_INTENT_BLOCKED"
    assert "handshake failed" in report["detail"]
    assert report["environment"]["host"] == "127.0.0.1"
    assert report["environment"]["port"] == 7497
    assert report["environment"]["client_id"] == 10940
    assert report["environment"]["account_id"] == "DUM882026"
    assert report["caller_metadata"]["caller_type"] == "track_b_paper_leak_test"
    assert diagnostics["host"] == "127.0.0.1"
    assert diagnostics["port"] == 7497
    assert diagnostics["client_id"] == 10940
    assert diagnostics["account_id"] == "DUM882026"
    assert diagnostics["read_only_preflight"] is True
    assert diagnostics["session_read_only"] is True
    assert diagnostics["session_gateway_mode"] == "paper"
    assert diagnostics["session_live_orders_enabled"] is False
    assert diagnostics["transport_class"] == "_HandshakeFailureTransport"
    assert diagnostics["connection_error_type"] == "IbkrPaperStrategyBridgeError"
    assert diagnostics["latest_error"]["code"] == 502
    assert report["errors"][0]["code"] == 502


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


def test_scope_obsolete_legacy_monitor_cannot_authorize_submit_without_phase1_reconciliation(tmp_path: Path) -> None:
    _write_phase1_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        broker_reconciled=False,
        review_required_count=1,
        block_reasons=["review_required_present"],
    )
    _write_runtime_files(tmp_path)
    config = _config(
        tmp_path,
        strategy_id="mnq_1x_asia_london_participation__asia_london_long_v6",
        symbol="MNQ",
        contract_month="202606",
        reason="MNQ_ASIA_LONDON_CANDIDATE_PAPER_INTENT",
        risk_tags=("MNQ_ASIA_LONDON_CANDIDATE", "PAPER_ONLY"),
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_asia_london_participation__asia_london_long_v6",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
        submit=True,
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
        timestamp="2026-04-29T16:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    monitor_status = load_paper_strategy_monitor_status(repo_root=tmp_path)

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status=monitor_status,
        governance_status=_healthy_lane_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "paper_strategy_submit_gate")["passed"] is True
    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert "review_required_present" in phase1["detail"]


def test_phase1_reconciliation_blocked_overrides_legacy_monitor_submit_allowed(tmp_path: Path) -> None:
    _write_phase1_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        broker_reconciled=True,
        open_order_count=1,
        block_reasons=["track_b_open_order_present"],
    )
    _write_runtime_files(tmp_path)
    config = _config(
        tmp_path,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(strategy_id="ATP_COMPANION_V1_ASIA_US"),
        submit=True,
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
        timestamp="2026-04-29T16:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status=load_paper_strategy_monitor_status(repo_root=tmp_path),
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "paper_strategy_submit_gate")["passed"] is True
    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert "track_b_open_order_present" in phase1["detail"]


def test_supervised_exit_can_bypass_entry_readiness_governance_block(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="atp_companion_v1_pl_asia_us",
        symbol="PL",
        contract_month="202607",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="atp_companion_v1_pl_asia_us",
            source_instrument="PL",
            executable_proxy="PL",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            bridge_proxy_mode="PL_SIGNAL_DIRECT_PHASE1",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-13T14:03:34+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["backend_or_source_not_live_ready"],
            "detail": "Paper strategy governance blocked submit: backend_or_source_not_live_ready",
            "selected_strategy": {
                "strategy_id": "atp_companion_v1_pl_asia_us",
                "bridge_strategy_id": "active_trend_participation_engine__PL",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": False,
                "submit_block_reasons": ["backend_or_source_not_live_ready"],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert governance_gate["passed"] is True
    assert "supervised PAPER exit only" in governance_gate["detail"]


def test_supervised_exit_cannot_bypass_entry_readiness_without_current_runtime_identity(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="atp_companion_v1_pl_asia_us",
        symbol="PL",
        contract_month="202607",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="atp_companion_v1_pl_asia_us",
            source_instrument="PL",
            executable_proxy="PL",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            bridge_proxy_mode="PL_SIGNAL_DIRECT_PHASE1",
        ),
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
        timestamp="2026-05-13T14:03:34+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["backend_or_source_not_live_ready"],
            "selected_strategy": {
                "strategy_id": "atp_companion_v1_pl_asia_us",
                "bridge_strategy_id": "active_trend_participation_engine__PL",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": False,
                "submit_block_reasons": ["backend_or_source_not_live_ready"],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert governance_gate["passed"] is False


def test_managed_exit_refreshes_stale_phase1_reconciliation_before_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")
    calls: list[Path] = []

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        calls.append(config.repo_root)
        _write_phase1_reconciliation(tmp_path)
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="atp_companion_v1_pl_asia_us",
        symbol="PL",
        contract_month="202607",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="atp_companion_v1_pl_asia_us",
            source_instrument="PL",
            executable_proxy="PL",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            bridge_proxy_mode="PL_SIGNAL_DIRECT_PHASE1",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert calls == [tmp_path]
    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is True
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert phase1["exit_allowed_after_refresh"] is True


def test_managed_exit_blocks_if_stale_phase1_reconciliation_refresh_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_FAILED"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert phase1["exit_allowed_after_refresh"] is False
    assert "phase1_broker_reconciliation_stale" in str(phase1["exit_block_reason"])


def test_managed_exit_blocks_if_refreshed_phase1_reconciliation_has_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(
            tmp_path,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            broker_reconciled=False,
            block_reasons=["TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"],
        )
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_BLOCKED"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" in str(phase1["exit_block_reason"])


def test_managed_exit_blocks_if_refreshed_phase1_reconciliation_has_open_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(
            tmp_path,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            broker_reconciled=False,
            open_order_count=1,
            block_reasons=["TRACK_B_BROKER_OPEN_ORDER_PRESENT"],
        )
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_BLOCKED"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert "TRACK_B_BROKER_OPEN_ORDER_PRESENT" in str(phase1["exit_block_reason"])


def test_new_entry_refreshes_stale_phase1_reconciliation_before_block(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")
    calls: list[Path] = []

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        calls.append(config.repo_root)
        _write_phase1_reconciliation(tmp_path)
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert calls == [tmp_path]
    assert phase1["passed"] is True
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert phase1["submit_allowed_after_refresh"] is True


def test_new_entry_blocks_if_stale_phase1_reconciliation_refresh_is_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(
            tmp_path,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            broker_reconciled=False,
            block_reasons=["BROKER_TRUTH_STATUS_FLAG_MISMATCH"],
        )
        return {
            "classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_BLOCKED",
            "broker_truth_refresh_classification": "BROKER_TRUTH_REFRESH_FAILED",
        }

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1["passed"] is False
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert phase1["submit_allowed_after_refresh"] is False
    assert "BROKER_TRUTH_STATUS_FLAG_MISMATCH" in str(phase1["exit_block_reason"])


def test_stale_governance_reconciliation_block_clears_after_successful_bridge_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(tmp_path)
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_stale_phase1_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert phase1["passed"] is True
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert phase1["submit_allowed_after_refresh"] is True
    assert governance_gate["passed"] is True
    assert "consumed the bridge read-only Phase-1 reconciliation refresh" in governance_gate["detail"]


def test_stale_governance_reconciliation_block_remains_if_no_successful_refresh(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_stale_phase1_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert phase1["passed"] is True
    assert phase1["stale_reconciliation_refresh_attempted"] is False
    assert governance_gate["passed"] is False
    assert "phase1_broker_reconciliation_not_clear" in governance_gate["detail"]


def test_stale_governance_reconciliation_block_remains_if_refresh_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(
            tmp_path,
            classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            broker_reconciled=False,
            block_reasons=["BROKER_TRUTH_STATUS_FLAG_MISMATCH"],
        )
        return {
            "classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_BLOCKED",
            "broker_truth_refresh_classification": "BROKER_TRUTH_REFRESH_FAILED",
        }

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_stale_phase1_governance(),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert phase1["passed"] is False
    assert phase1["stale_reconciliation_refresh_attempted"] is True
    assert governance_gate["passed"] is False
    assert "phase1_broker_reconciliation_not_clear" in governance_gate["detail"]


def test_stale_governance_refresh_keeps_non_reconciliation_blockers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(tmp_path)
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_stale_phase1_governance(extra_block_reasons=["backend_or_source_not_live_ready"]),
        exposure_status=_healthy_exposure(),
    )

    phase1 = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert phase1["passed"] is True
    assert phase1["submit_allowed_after_refresh"] is True
    assert governance_gate["passed"] is False
    assert "backend_or_source_not_live_ready" in governance_gate["detail"]
    assert "phase1_broker_reconciliation_not_clear" not in governance_gate["detail"]


def test_stale_governance_refresh_keeps_top_level_only_non_reconciliation_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_phase1_reconciliation(tmp_path, generated_at="2000-01-01T00:00:00+00:00")

    def _fake_refresh(*, config: IbkrPaperStrategyBridgeConfig) -> dict[str, object]:
        del config
        _write_phase1_reconciliation(tmp_path)
        return {"classification": "TRACK_B_PHASE1_RECONCILIATION_REFRESH_CLEAN"}

    monkeypatch.setattr(bridge_module, "_refresh_phase1_broker_reconciliation_artifacts", _fake_refresh)
    config = _config(
        tmp_path,
        submit=True,
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ATP_COMPANION_V1_ASIA_US",
            runtime_pid=os.getpid(),
            runtime_cwd=str(tmp_path),
        ),
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
        timestamp="2026-05-15T07:48:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status=_stale_phase1_governance_with_top_level_only_blocker(),
        exposure_status=_healthy_exposure(),
    )

    governance_gate = next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")
    assert governance_gate["passed"] is False
    assert "backend_or_source_not_live_ready" in governance_gate["detail"]
    assert "phase1_broker_reconciliation_not_clear" not in governance_gate["detail"]


def test_entry_cannot_bypass_backend_readiness_governance_block(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="atp_companion_v1_pl_asia_us",
        symbol="PL",
        contract_month="202607",
        action="BUY",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="atp_companion_v1_pl_asia_us",
            source_instrument="PL",
            executable_proxy="PL",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            bridge_proxy_mode="PL_SIGNAL_DIRECT_PHASE1",
        ),
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
        timestamp="2026-05-13T14:03:34+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={},
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_PARTIAL",
            "submit_allowed": False,
            "block_reasons": ["backend_or_source_not_live_ready"],
            "selected_strategy": {
                "strategy_id": "atp_companion_v1_pl_asia_us",
                "bridge_strategy_id": "active_trend_participation_engine__PL",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": False,
                "submit_block_reasons": ["backend_or_source_not_live_ready"],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")["passed"] is False


def test_preflight_treats_missing_unscoped_legacy_monitor_as_diagnostic(tmp_path: Path) -> None:
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
        exposure_status={},
    )

    monitor_gate = next(row for row in checks if row["name"] == "paper_strategy_monitor_runtime_present")
    assert monitor_gate["passed"] is True
    assert "diagnostic for this route" in monitor_gate["detail"]
    phase1_gate = next(row for row in checks if row["name"] == "phase1_broker_reconciliation_submit_gate")
    assert phase1_gate["passed"] is True


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
        exposure_status=_healthy_exposure(),
    )

    assert all(
        row["passed"]
        for row in checks
        if row["name"].startswith("paper_strategy_monitor_") or row["name"].startswith("paper_strategy_governance_")
    )


def test_preflight_ignores_stale_service_overlay_for_flat_paper_submit_gate(tmp_path: Path) -> None:
    _write_runtime_files(
        tmp_path,
        monitor_status={
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
            "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
            "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
            "freshness_window_seconds": 60.0,
        },
        governance_status=_healthy_governance(),
    )
    service_status_path = tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_monitor_service_status_report.json"
    service_status_path.parent.mkdir(parents=True, exist_ok=True)
    service_status_path.write_text(
        json.dumps(
            {
                "generated_at": "2000-01-01T00:00:00+00:00",
                "classification": "PAPER_MONITOR_SERVICE_READY",
                "service_process_running": True,
                "monitor_running": True,
                "bridge_allowed": False,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "stale": False,
                "runtime_classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "last_successful_broker_refresh": "2000-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "var" / "paper_strategy_monitor_service.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

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
        timestamp="2026-04-30T07:06:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    monitor_status = load_paper_strategy_monitor_status(repo_root=tmp_path)
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "manual cli"},
        monitor_status=monitor_status,
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    monitor_gate = next(row for row in checks if row["name"] == "paper_strategy_submit_gate")
    assert monitor_status["submit_allowed"] is True
    assert monitor_gate["passed"] is True


def test_supervised_runtime_preflight_allows_fresh_flat_state_with_preserved_atp_ownership(tmp_path: Path) -> None:
    _write_runtime_files(
        tmp_path,
        monitor_status={
            "generated_at": "2999-01-01T00:00:00+00:00",
            "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
            "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
            "broker_position_quantity": 0.0,
            "ledger_position_quantity": 0.0,
            "open_order_count": 0,
            "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
            "freshness_window_seconds": 60.0,
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_asia_london_participation__asia_london_long_v5",
                "bridge_strategy_id": "asia_london_participation_core_v1__GC",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
            "strategies": [],
        },
    )
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_1x_asia_london_participation__asia_london_long_v5",
            source_instrument="GC",
            executable_proxy="GC",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            bridge_proxy_mode="GC_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-01T02:20:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    monitor_status = load_paper_strategy_monitor_status(repo_root=tmp_path)
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status=monitor_status,
        governance_status=_healthy_lane_governance(),
        exposure_status=_healthy_exposure(),
    )

    assert monitor_status["submit_allowed"] is True
    assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True
    assert next(row for row in checks if row["name"] == "paper_strategy_submit_gate")["passed"] is True


def test_supervised_runtime_preflight_allows_clean_flat_state_even_if_monitor_submit_bool_is_false_for_preserved_atp_detail(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="ibkr_paper_route_canary",
        symbol="MNQ",
        contract_month="202606",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ibkr_paper_route_canary",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-01T18:59:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
            "monitor_running": True,
            "submit_allowed": False,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
            "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
            "broker_position_quantity": 0.0,
            "ledger_position_quantity": 0.0,
            "broker_ledger_match": "MATCH",
            "open_order_count": 0,
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "ibkr_paper_route_canary",
                "bridge_strategy_id": "ibkr_paper_route_canary",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    gate = next(row for row in checks if row["name"] == "paper_strategy_submit_gate")
    assert gate["passed"] is True
    assert "Legacy paper strategy monitor is diagnostic for this route" in gate["detail"]


def test_supervised_runtime_preflight_allows_preserved_atp_detail_with_live_monitor_field_names(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="ibkr_paper_route_canary",
        symbol="MNQ",
        contract_month="202606",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="ibkr_paper_route_canary",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-01T19:47:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )

    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
            "monitor_running": True,
            "submit_allowed": False,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
            "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
            "current_broker_mgc_position": 0.0,
            "strategy_ledger_mgc_position": 0.0,
            "broker_ledger_match": "MATCH",
            "open_mgc_orders": 0,
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "ibkr_paper_route_canary",
                "bridge_strategy_id": "ibkr_paper_route_canary",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    gate = next(row for row in checks if row["name"] == "paper_strategy_submit_gate")
    assert gate["passed"] is True
    assert "Legacy paper strategy monitor is diagnostic for this route" in gate["detail"]


def test_preflight_blocks_dirty_broker_ledger_state(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__us_midday_short",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_1x_all_lanes__us_midday_short",
            action="SELL",
            intent_type="SELL_TO_OPEN",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T15:05:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "strategy_id": "gc_1x_all_lanes__us_midday_short",
            "monitor_running": True,
            "submit_allowed": False,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "GC", "expiry": "202606", "con_id": None, "local_symbol": None},
            "block_reasons": ["ledger_broker_mismatch"],
            "detail": "Broker and paper ledger disagree.",
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__us_midday_short",
                "bridge_strategy_id": "gc_1x_all_lanes__us_midday_short",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    failure = next(row for row in checks if row["name"] == "paper_strategy_submit_gate")
    assert failure["passed"] is False
    assert "disagree" in failure["detail"]


def test_preflight_blocks_unsupported_execution_contract_for_approved_runtime_lane(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="es_1x_ny_early_core__us_midday_long",
        symbol="M2K",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="es_1x_ny_early_core__us_midday_long",
            source_instrument="ES",
            executable_proxy="M2K",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-30T12:26:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "M2K", "expiry": "20260619", "con_id": 123, "local_symbol": "M2KM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "es_1x_ny_early_core__us_midday_long",
                "bridge_strategy_id": "es_1x_ny_early_core__us_midday_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    failure = next(row for row in checks if row["name"] == "executable_contract_whitelist")
    assert failure["passed"] is False
    assert "approved phase-1 execution target" in failure["detail"]


def test_preflight_blocks_wrong_contract_month_for_approved_runtime_lane(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="es_1x_ny_early_core__us_midday_long",
        symbol="ES",
        contract_month="202609",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="es_1x_ny_early_core__us_midday_long",
            source_instrument="ES",
            executable_proxy="ES",
            bridge_proxy_mode="ES_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-01T12:31:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "es_1x_ny_early_core__us_midday_long",
                "bridge_strategy_id": "es_1x_ny_early_core__us_midday_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    failure = next(row for row in checks if row["name"] == "contract_month_lock")
    assert failure["passed"] is False


def test_preflight_blocks_live_metadata_for_current_supervised_paper_route(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="es_1x_ny_early_core__us_midday_long",
        symbol="ES",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="es_1x_ny_early_core__us_midday_long",
            source_instrument="ES",
            executable_proxy="ES",
            mode="LIVE",
            bridge_proxy_mode="ES_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-01T12:32:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "ES", "expiry": "20260619", "con_id": 123, "local_symbol": "ESM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "es_1x_ny_early_core__us_midday_long",
                "bridge_strategy_id": "es_1x_ny_early_core__us_midday_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    failure = next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")
    assert failure["passed"] is False


def test_preflight_treats_contract_mismatched_legacy_monitor_as_diagnostic(tmp_path: Path) -> None:
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
            "strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260926", "con_id": 712565978, "local_symbol": "MGCU6"},
            "block_reasons": [],
        },
        governance_status=_healthy_governance(),
        exposure_status=_healthy_exposure(),
    )

    monitor_gate = next(row for row in checks if row["name"] == "paper_strategy_monitor_contract_match")
    assert monitor_gate["passed"] is True
    assert "diagnostic for this route" in monitor_gate["detail"]


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
        exposure_status=_healthy_exposure(),
    )

    failure = next(row for row in checks if row["name"] == "paper_strategy_governance_status_allowed")
    assert failure["passed"] is False


def test_supervised_runtime_caller_requires_metadata(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__us_midday_short",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T16:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__us_midday_short",
                "bridge_strategy_id": "gc_1x_all_lanes__us_midday_short",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    metadata_failure = next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")
    assert metadata_failure["passed"] is False


def test_supervised_runtime_caller_metadata_passes_for_midday_gold_lane(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__us_midday_short",
        action="SELL",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_1x_all_lanes__us_midday_short",
            action="SELL",
            intent_type="SELL_TO_OPEN",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T16:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__us_midday_short",
                "bridge_strategy_id": "gc_1x_all_lanes__us_midday_short",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "approved_paper_caller_path")["passed"] is True
    assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True
    assert next(row for row in checks if row["name"] == "manual_harness_bundle_present_for_submit")["passed"] is True


def test_supervised_runtime_caller_metadata_passes_for_gc_asia_early_long_lane(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(strategy_id="gc_1x_all_lanes__asia_early_long"),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T23:06:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__asia_early_long",
                "bridge_strategy_id": "gc_1x_all_lanes__asia_early_long",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True


def test_supervised_runtime_caller_metadata_passes_for_gc_asia_early_short_lane(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__asia_early_short",
        action="SELL",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="gc_1x_all_lanes__asia_early_short",
            action="SELL",
            intent_type="SELL_TO_OPEN",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T23:05:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__asia_early_short",
                "bridge_strategy_id": "gc_1x_all_lanes__asia_early_short",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True


def test_mnq_short_entry_lane_reaches_guarded_submit_boundary_when_fresh(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="mnq_1x_ny_early_core__us_early_short_reclaim_fail",
        symbol="MNQ",
        contract_month="202606",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_early_short_reclaim_fail",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-08T14:28:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
                "bridge_strategy_id": "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert all(row["passed"] for row in checks), checks
    assert next(row for row in checks if row["name"] == "paper_strategy_governance_submit_gate")["passed"] is True
    assert next(row for row in checks if row["name"] == "paper_strategy_exposure_gate")["passed"] is True


def test_submit_preflight_blocks_deprecated_documents_repo_root(tmp_path: Path) -> None:
    deprecated_root = Path("/Users/patrick/Documents/MGC-v05l-automation")
    config = _config(
        tmp_path,
        repo_root=deprecated_root,
        submit=True,
        strategy_id="mnq_1x_ny_early_core__us_early_short_reclaim_fail",
        symbol="MNQ",
        contract_month="202606",
        action="SELL",
        limit_price_model="DELAYED_BID_MINUS_1T_MARKETABLE_SELL",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(
            strategy_id="mnq_1x_ny_early_core__us_early_short_reclaim_fail",
            source_instrument="MNQ",
            executable_proxy="MNQ",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            bridge_proxy_mode="MNQ_SIGNAL_DIRECT_PHASE1",
        ),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-05-08T14:28:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "monitor_running": True,
            "submit_allowed": True,
            "health_classification": "HEALTHY",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "block_reasons": [],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
                "bridge_strategy_id": "mnq_1x_ny_early_core__us_early_short_reclaim_fail",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    root_gate = next(row for row in checks if row["name"] == "deprecated_submit_root_block")
    assert root_gate["passed"] is False
    assert "deprecated" in root_gate["detail"]
    assert "Documents/MGC-v05l-automation" in root_gate["detail"]


def test_runtime_caller_still_blocks_when_monitor_health_fails(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        submit=True,
        strategy_id="gc_1x_all_lanes__us_midday_short",
        symbol="GC",
        caller_path="probationary_paper_runtime_lane",
        caller_metadata=_approved_runtime_metadata(strategy_id="gc_1x_all_lanes__us_midday_short"),
        manual_frozen_preview_path=None,
        approval_digest=None,
        approval_phrase=None,
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
        timestamp="2026-04-29T16:00:00+00:00",
        risk_tags=config.risk_tags,
        paper_only=config.paper_only,
    )
    checks = _build_static_preflight_checks(
        config=config,
        intent=intent,
        environment_lock=evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port),
        caller_gate={"passed": True, "detail": "approved runtime caller"},
        monitor_status={
            "strategy_id": "gc_1x_all_lanes__us_midday_short",
            "monitor_running": True,
            "submit_allowed": False,
            "health_classification": "STALE",
            "account_id": "DUM882026",
            "exact_contract": {"symbol": "GC", "expiry": "202606", "con_id": None, "local_symbol": None},
            "block_reasons": ["paper_runtime_stale"],
        },
        governance_status={
            "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
            "submit_allowed": True,
            "block_reasons": [],
            "selected_strategy": {
                "strategy_id": "gc_1x_all_lanes__us_midday_short",
                "bridge_strategy_id": "gc_1x_all_lanes__us_midday_short",
                "strategy_status": "PROBATION_ACTIVE",
                "submit_allowed": True,
                "submit_block_reasons": [],
            },
        },
        exposure_status=_healthy_exposure(),
    )

    assert next(row for row in checks if row["name"] == "approved_runtime_caller_metadata")["passed"] is True
    assert next(row for row in checks if row["name"] == "paper_strategy_monitor_health")["passed"] is False


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


def test_submit_blocks_when_exposure_gate_rejects_duplicate_strategy_buy(tmp_path: Path) -> None:
    _write_runtime_files(
        tmp_path,
        ledger_positions=[
            {
                "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "symbol": "MGC",
                "contract_month": "202606",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "quantity": 1.0,
                "side": "LONG",
                "state": "OPEN",
            }
        ],
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
    assert "duplicate_strategy_entry_while_position_open" in json.dumps(artifacts.report)
