"""Scoped Track B PAPER broker-position guardian remediation.

This boundary exists only for exact contaminated reverse-exposure remediation.
It is not a flatten tool: it accepts one guardian-generated scoped plan, checks
the exact PAPER contract identity immediately before apply, and can only submit
the single offsetting order named in that plan.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.app.ibkr_broker_truth_refresher import BrokerTruthRefreshConfig, run_broker_truth_refresh_once
from mgc_v05l.execution_core.ibkr_paper_adapter import IbkrPaperAdapter
from mgc_v05l.execution_core.models import Action, IntentKind, OrderIntent, SubmitAttempt, SubmitAttemptState, to_jsonable
from mgc_v05l.execution_core.preflight import ReadOnlyPreflightConfig
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_position_guardian import (
    BROKER_POSITION_GUARDIAN_HARD_HOLD,
    DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT,
    UNAUTHORIZED_REVERSE_EXPOSURE,
    TrackBBrokerPositionGuardianConfig,
    build_track_b_broker_position_guardian,
    write_track_b_broker_position_guardian,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "broker_position_guardian"
    / "latest_scoped_guardian_remediation.json"
)
DEFAULT_PHASE1_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"

SCOPED_GUARDIAN_REMEDIATION_READY = "SCOPED_GUARDIAN_REMEDIATION_READY"
SCOPED_GUARDIAN_REMEDIATION_APPLIED_OR_PENDING = "SCOPED_GUARDIAN_REMEDIATION_APPLIED_OR_PENDING"
SCOPED_GUARDIAN_REMEDIATION_FILLED = "SCOPED_GUARDIAN_REMEDIATION_FILLED"
SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY = "SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY"
SCOPED_GUARDIAN_REMEDIATION_BLOCKED_OPEN_ORDERS = "SCOPED_GUARDIAN_REMEDIATION_BLOCKED_OPEN_ORDERS"
SCOPED_GUARDIAN_REMEDIATION_BLOCKED_FLAGS = "SCOPED_GUARDIAN_REMEDIATION_BLOCKED_FLAGS"
SCOPED_GUARDIAN_REMEDIATION_APPLY_DISABLED = "SCOPED_GUARDIAN_REMEDIATION_APPLY_DISABLED"


@dataclass(frozen=True)
class TrackBBrokerPositionGuardianRemediationConfig:
    repo_root: Path = REPO_ROOT
    mode: str = "PAPER"
    account_id: str = "DUM882026"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17087
    expected_symbol: str = "MNQ"
    expected_contract_key: str = "MNQ-202606"
    expected_local_symbol: str = "MNQM6"
    expected_con_id: int = 770561201
    expected_expiry: str = "20260618"
    expected_position_quantity: int = -1
    remediation_action: str = "BUY"
    remediation_quantity: int = 1
    tick_size: str = "0.25"
    marketable_limit_ticks: int = 20
    limit_price: str | None = None
    apply: bool = False
    operator_authorized_scoped_remediation: bool = False
    output_path: Path = DEFAULT_OUTPUT_PATH
    guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    phase1_market_data_root: Path = DEFAULT_PHASE1_ROOT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def run_track_b_broker_position_guardian_remediation(
    *,
    config: TrackBBrokerPositionGuardianRemediationConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    broker_refresh = _refresh_broker_truth(config=config)
    guardian_config = TrackBBrokerPositionGuardianConfig(repo_root=config.repo_root, output_path=config.guardian_path)
    guardian = build_track_b_broker_position_guardian(config=guardian_config, now=actual_now)
    write_track_b_broker_position_guardian(config=guardian_config, payload=guardian)
    limit_price = config.limit_price or _default_marketable_limit(config=config)
    payload: dict[str, Any] = {
        "schema_version": "track_b_scoped_guardian_remediation_v1",
        "generated_at": actual_now.isoformat(),
        "mode": config.mode,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_cancel_allowed": False,
        "global_flatten_allowed": False,
        "dashboard_projection_consumed": False,
        "apply_requested": config.apply,
        "operator_authorized_scoped_remediation": config.operator_authorized_scoped_remediation,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "broker_truth_refresh": {
            "classification": broker_refresh.get("classification"),
            "generated_at": broker_refresh.get("generated_at"),
            "open_order_count": broker_refresh.get("open_order_count"),
            "position_count": broker_refresh.get("position_count"),
            "live_money_eligible": broker_refresh.get("live_money_eligible"),
            "paper_proof_invoked": broker_refresh.get("paper_proof_invoked"),
        },
        "guardian_classification": guardian.get("classification"),
        "guardian_hard_classifications": guardian.get("hard_classifications"),
        "guardian_scoped_remediation_plan": guardian.get("scoped_remediation_plan"),
        "target_identity": _target_identity(config=config, limit_price=limit_price),
        "classification": SCOPED_GUARDIAN_REMEDIATION_READY,
        "apply_boundary_classification": SCOPED_GUARDIAN_REMEDIATION_APPLY_DISABLED,
        "blockers": [],
    }
    blockers = _pre_apply_blockers(config=config, guardian=guardian, broker_refresh=broker_refresh)
    payload["blockers"] = blockers
    if blockers:
        payload["classification"] = str(blockers[0].get("classification") or SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY)
        write_json_atomic(config.resolve(config.output_path), to_jsonable(payload))
        return payload
    if not config.apply or not config.operator_authorized_scoped_remediation:
        payload["classification"] = SCOPED_GUARDIAN_REMEDIATION_READY
        payload["required_next_action"] = "Rerun with --apply --operator-authorized-scoped-remediation only if exact identity remains current."
        write_json_atomic(config.resolve(config.output_path), to_jsonable(payload))
        return payload

    apply_result = _apply_exact_scoped_buy(config=config, limit_price=limit_price, now=actual_now)
    payload["apply_result"] = apply_result
    payload["submit_attempted"] = bool(apply_result.get("submit_attempted"))
    payload["broker_state_mutated"] = bool(apply_result.get("broker_state_mutated"))
    payload["classification"] = str(apply_result.get("classification") or SCOPED_GUARDIAN_REMEDIATION_APPLIED_OR_PENDING)
    payload["apply_boundary_classification"] = payload["classification"]
    write_json_atomic(config.resolve(config.output_path), to_jsonable(payload))
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply exact scoped Broker Position Guardian PAPER remediation.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator-authorized-scoped-remediation", action="store_true")
    parser.add_argument("--limit-price", default=None)
    parser.add_argument("--client-id", type=int, default=17087)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBBrokerPositionGuardianRemediationConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        apply=bool(args.apply),
        operator_authorized_scoped_remediation=bool(args.operator_authorized_scoped_remediation),
        limit_price=args.limit_price,
        client_id=int(args.client_id),
    )
    payload = run_track_b_broker_position_guardian_remediation(config=config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("classification") in {SCOPED_GUARDIAN_REMEDIATION_READY, SCOPED_GUARDIAN_REMEDIATION_FILLED} else 2


def _refresh_broker_truth(*, config: TrackBBrokerPositionGuardianRemediationConfig) -> dict[str, Any]:
    return run_broker_truth_refresh_once(
        config=BrokerTruthRefreshConfig(
            mode=config.mode,
            host=config.host,
            port=config.port,
            client_id=9077,
            account_id=config.account_id,
            read_only=True,
            timeout_seconds=8.0,
            output_dir=config.repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
            status_path=config.repo_root
            / "outputs"
            / "reports"
            / "ibkr_read_only_verification"
            / "ibkr_broker_truth_refresh_status.json",
            var_status_path=config.repo_root / "var" / "ibkr_broker_truth_refresh_status.json",
        )
    )


def _pre_apply_blockers(
    *,
    config: TrackBBrokerPositionGuardianRemediationConfig,
    guardian: Mapping[str, Any],
    broker_refresh: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if bool(broker_refresh.get("live_money_eligible")):
        blockers.append({"classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_FLAGS, "reason": "live_money_eligible=true"})
    if bool(broker_refresh.get("paper_proof_invoked")):
        blockers.append({"classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_FLAGS, "reason": "paper_proof_invoked=true"})
    if int(broker_refresh.get("open_order_count") or 0) != 0:
        blockers.append(
            {
                "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_OPEN_ORDERS,
                "reason": "Fresh broker truth reports open orders; exact scoped remediation requires open_order_count=0.",
            }
        )
    plan = _mapping(guardian.get("scoped_remediation_plan"))
    hard = {str(item) for item in guardian.get("hard_classifications") or []}
    if guardian.get("classification") != BROKER_POSITION_GUARDIAN_HARD_HOLD or UNAUTHORIZED_REVERSE_EXPOSURE not in hard:
        blockers.append(
            {
                "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY,
                "reason": "Guardian is not in unauthorized-reverse hard-hold state.",
            }
        )
    expected = {
        "account_id": config.account_id,
        "action": config.remediation_action,
        "quantity": str(config.remediation_quantity),
        "local_symbol": config.expected_local_symbol,
        "con_id": int(config.expected_con_id),
        "expiry": config.expected_expiry,
    }
    for key, value in expected.items():
        observed = plan.get(key)
        if str(observed) != str(value):
            blockers.append(
                {
                    "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY,
                    "reason": f"Guardian scoped plan {key} mismatch: expected {value}, observed {observed}.",
                }
            )
    return blockers


def _apply_exact_scoped_buy(
    *,
    config: TrackBBrokerPositionGuardianRemediationConfig,
    limit_price: str,
    now: datetime,
) -> dict[str, Any]:
    adapter = IbkrPaperAdapter(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_allowlist={config.expected_contract_key: _contract_allowlist_entry(config)},
        submit_enabled=True,
        request_timeout_seconds=20.0,
        fill_timeout_seconds=45.0,
    )
    run_id = f"guardian_scoped_remediation_{now.strftime('%Y%m%dT%H%M%SZ')}"
    order_intent = OrderIntent(
        order_intent_id=f"{run_id}_intent",
        signal_event_id=f"{run_id}_unauthorized_reverse_exposure",
        run_id=run_id,
        intent_kind=IntentKind.CLOSE,
        account_id=config.account_id,
        symbol=config.expected_symbol,
        contract_key=config.expected_contract_key,
        action=Action.BUY,
        quantity=config.remediation_quantity,
        order_type="LMT",
        limit_price=limit_price,
        time_in_force="DAY",
        paper_only=True,
        created_at=now,
        reason=f"Exact scoped guardian remediation for unauthorized reverse {config.expected_symbol} exposure.",
    )
    submit_attempt = SubmitAttempt(
        submit_attempt_id=f"{run_id}_submit",
        order_intent_id=order_intent.order_intent_id,
        run_id=run_id,
        account_id=config.account_id,
        broker="IBKR",
        environment={
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "broker": "IBKR",
            "environment": "PAPER",
            "remediation_boundary": "BROKER_POSITION_GUARDIAN_SCOPED",
        },
        pre_submit_reconciliation_id=f"{run_id}_pre_submit_truth",
        open_order_baseline_event_id=f"{run_id}_open_order_baseline",
        request_digest=f"{order_intent.order_intent_id}:{order_intent.action.value}:{order_intent.limit_price}",
        state=SubmitAttemptState.CREATED,
        submitted_at=now,
    )
    try:
        adapter.connect()
        adapter.managed_accounts()
        adapter.require_configured_account()
        open_orders = adapter.refresh_open_orders(contract_key=config.expected_contract_key)
        if open_orders:
            return {
                "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_OPEN_ORDERS,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "working_order_count": len(open_orders),
                "open_orders": [row.to_json_dict() for row in open_orders],
            }
        position = adapter.refresh_positions(contract_key=config.expected_contract_key)
        if int(position.signed_quantity) != int(config.expected_position_quantity):
            return {
                "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "reason": "Exact TWS position callback quantity mismatch.",
                "expected_signed_quantity": config.expected_position_quantity,
                "broker_position": position.to_json_dict(),
            }
        broker_order_id = adapter.submit_limit_order(submit_attempt=submit_attempt, order_intent=order_intent)
        broker_order = adapter.wait_for_broker_order(submit_attempt_id=submit_attempt.submit_attempt_id)
        result: dict[str, Any] = {
            "classification": SCOPED_GUARDIAN_REMEDIATION_APPLIED_OR_PENDING,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": str(broker_order_id),
            "broker_order": broker_order.to_json_dict(),
            "submit_attempt_id": submit_attempt.submit_attempt_id,
            "order_intent": order_intent.to_json_dict(),
            "submit_diagnostics": adapter.submit_diagnostics(submit_attempt.submit_attempt_id),
        }
        try:
            fill = adapter.wait_for_fill(submit_attempt_id=submit_attempt.submit_attempt_id, timeout_seconds=45.0)
            result["classification"] = SCOPED_GUARDIAN_REMEDIATION_FILLED
            result["fill"] = fill.to_json_dict()
        except Exception as exc:  # noqa: BLE001 - pending orders remain auditable.
            result["fill_wait_exception"] = repr(exc)
        return result
    except Exception as exc:  # noqa: BLE001 - broker boundary must be artifacted.
        diagnostics = adapter.submit_diagnostics(submit_attempt.submit_attempt_id)
        return {
            "classification": SCOPED_GUARDIAN_REMEDIATION_BLOCKED_IDENTITY,
            "submit_attempted": bool(diagnostics.get("place_order_called") or diagnostics.get("broker_order_id_allocated")),
            "broker_state_mutated": bool(diagnostics.get("place_order_called")),
            "adapter_exception": repr(exc),
            "submit_diagnostics": diagnostics,
        }
    finally:
        adapter.disconnect()


def _target_identity(*, config: TrackBBrokerPositionGuardianRemediationConfig, limit_price: str) -> dict[str, Any]:
    return {
        "account_id": config.account_id,
        "symbol": config.expected_symbol,
        "contract_key": config.expected_contract_key,
        "local_symbol": config.expected_local_symbol,
        "con_id": config.expected_con_id,
        "expiry": config.expected_expiry,
        "expected_current_position_quantity": str(config.expected_position_quantity),
        "action": config.remediation_action,
        "quantity": str(config.remediation_quantity),
        "order_type": "LMT",
        "limit_price": limit_price,
    }


def _default_marketable_limit(*, config: TrackBBrokerPositionGuardianRemediationConfig) -> str:
    latest = _latest_phase1_price(config=config)
    tick = Decimal(str(config.tick_size))
    return _decimal_text(latest + tick * Decimal(str(config.marketable_limit_ticks)))


def _latest_phase1_price(*, config: TrackBBrokerPositionGuardianRemediationConfig) -> Decimal:
    path = config.resolve(config.phase1_market_data_root) / config.expected_symbol / "1m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    bars = payload.get("bars") if isinstance(payload.get("bars"), list) else []
    if not bars:
        raise ValueError(f"Missing Phase-1 1m bars for remediation limit price: {path}")
    latest = _mapping(bars[-1])
    for key in ("close", "last", "price"):
        if latest.get(key) is not None:
            return Decimal(str(latest[key]))
    raise ValueError(f"Missing close price in latest Phase-1 1m bar: {path}")


def _contract_allowlist_entry(config: TrackBBrokerPositionGuardianRemediationConfig) -> dict[str, Any]:
    canonical = dict(ReadOnlyPreflightConfig().contract_allowlist.get(config.expected_contract_key) or {})
    fallback = {
        "symbol": config.expected_symbol,
        "security_type": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "contract_month": "202606",
        "expiry": config.expected_expiry,
        "local_symbol": config.expected_local_symbol,
        "con_id": config.expected_con_id,
        "multiplier": "2",
        "tick_size": config.tick_size,
    }
    fallback.update({key: value for key, value in canonical.items() if value not in {None, ""}})
    return fallback


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


if __name__ == "__main__":
    raise SystemExit(main())
