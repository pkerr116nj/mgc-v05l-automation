"""Dry-run Track B PAPER managed-order adjustment planner.

The planner is read-only. It consumes execution_core authority artifacts and
produces an operator-authorized plan for modify-in-place versus targeted
cancel/replace decisions. It never submits, modifies, cancels, replaces, closes,
or restarts anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic

from .track_b_exit_execution_policy import (
    EXIT_CLASS_RISK_REDUCING,
    build_exit_limit_policy,
    classify_exit_execution,
    validate_final_marketable_close_limit,
)
from .track_b_exit_strategy_roster import (
    ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
)
from .track_b_futures_tick_metadata import futures_tick_metadata
from .track_b_managed_order_registry import (
    BROKER_FLAT_WITH_WORKING_CLOSE,
    CLOSE_ORDER_CANCEL_REPLACE_REQUIRED,
    CLOSE_ORDER_MODIFIABLE,
    CLOSE_ORDER_NOT_MARKETABLE,
    CLOSE_ORDER_SUSPICIOUS,
    DUPLICATE_CLOSE_ORDER_BLOCKED,
    NO_MANAGED_ORDERS,
    ORDER_TERMINAL_CANCELLED,
    ORDER_TERMINAL_FILLED,
    POSITION_WITHOUT_CLOSE_ORDER,
    WORKING_CLOSE_ORDER,
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
)
from .track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
from .track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from .track_b_shared_truth_refresh_cli import TrackBSharedTruthRefreshConfig, refresh_track_b_shared_truth


NO_ACTION_NEEDED = "NO_ACTION_NEEDED"
MODIFY_IN_PLACE_ELIGIBLE = "MODIFY_IN_PLACE_ELIGIBLE"
TARGETED_CANCEL_REPLACE_REQUIRED = "TARGETED_CANCEL_REPLACE_REQUIRED"
WAIT_FOR_WORKING_ORDER = "WAIT_FOR_WORKING_ORDER"
DO_NOT_REPLACE_DUPLICATE_RISK = "DO_NOT_REPLACE_DUPLICATE_RISK"
REVIEW_REQUIRED_SUSPICIOUS_STATE = "REVIEW_REQUIRED_SUSPICIOUS_STATE"
BROKER_FLAT_NO_REPLACE = "BROKER_FLAT_NO_REPLACE"
ORDER_NOT_FOUND = "ORDER_NOT_FOUND"

DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json"
)
DEFAULT_MARKET_DATA_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"

_TERMINAL_CANCELLED_STATUSES = {"CANCELLED", "APICANCELLED", "INACTIVE"}
_TERMINAL_FILLED_STATUSES = {"FILLED"}
_SENTINEL_FILLED_QUANTITY = Decimal("1e100")


@dataclass(frozen=True)
class TrackBOrderAdjustmentPlannerConfig:
    repo_root: Path
    output_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_order_adjustment_plan(
    *,
    config: TrackBOrderAdjustmentPlannerConfig,
    now: datetime | None = None,
    shared_truth_refresh: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    managed_order_registry = _read_json(config.resolve(config.managed_order_registry_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    managed_orders = _list(managed_order_registry.get("managed_orders"))

    plans = [
        _plan_for_managed_order(
            order=order,
            open_order_truth=open_order_truth,
            position_truth=position_truth,
            config=config,
            now=actual_now,
        )
        for order in managed_orders
    ]
    classification = _overall_classification(plans=plans, managed_order_registry=managed_order_registry)
    payload = {
        "schema_version": "track_b_order_adjustment_plan_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "dry_run": True,
        "read_only": True,
        "submit_authority": False,
        "mutation_authority": False,
        "operator_authorization_required": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": classification,
        "plans": plans,
        "summary": {
            "classification": classification,
            "plan_count": len(plans),
            "modify_in_place_eligible_count": sum(
                1 for plan in plans if plan.get("classification") == MODIFY_IN_PLACE_ELIGIBLE
            ),
            "targeted_cancel_replace_required_count": sum(
                1 for plan in plans if plan.get("classification") == TARGETED_CANCEL_REPLACE_REQUIRED
            ),
            "review_required_count": sum(
                1 for plan in plans if plan.get("classification") == REVIEW_REQUIRED_SUSPICIOUS_STATE
            ),
            "duplicate_risk_count": sum(1 for plan in plans if plan.get("classification") == DO_NOT_REPLACE_DUPLICATE_RISK),
        },
        "managed_order_registry": _authority_summary(
            managed_order_registry,
            config.resolve(config.managed_order_registry_path),
        ),
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "position_truth": _authority_summary(position_truth, config.resolve(config.position_truth_path)),
        "shared_truth_refresh": _shared_truth_summary(shared_truth_refresh or {}),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "market_data_root": str(config.resolve(config.market_data_root)),
        },
    }
    return payload


def write_track_b_order_adjustment_plan(
    *,
    config: TrackBOrderAdjustmentPlannerConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    _write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a dry-run Track B PAPER managed-order adjustment plan.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--json", action="store_true", help="Print JSON instead of compact summary.")
    parser.add_argument(
        "--skip-shared-truth-refresh",
        action="store_true",
        help="Use existing authority artifacts without first refreshing shared truth.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    refresh_result: Mapping[str, Any] | None = None
    if not bool(args.skip_shared_truth_refresh):
        refresh_result = refresh_track_b_shared_truth(
            config=TrackBSharedTruthRefreshConfig(repo_root=repo_root, broker_lease_history_path=None)
        )
    config = TrackBOrderAdjustmentPlannerConfig(repo_root=repo_root)
    payload = build_track_b_order_adjustment_plan(config=config, shared_truth_refresh=refresh_result)
    output_path = write_track_b_order_adjustment_plan(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"plan_count={_mapping(payload.get('summary')).get('plan_count')}")
        print(f"artifact_path={output_path}")
    return 0


def _plan_for_managed_order(
    *,
    order: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    config: TrackBOrderAdjustmentPlannerConfig,
    now: datetime,
) -> dict[str, Any]:
    source_classification = str(order.get("classification") or "")
    status = str(order.get("broker_status") or "").upper()
    marketability = _mapping(order.get("marketability"))
    source_order = _mapping(order.get("source_order"))
    position_open = _position_open(order=order, position_truth=position_truth)
    identity = _identity(order=order, source_order=source_order)
    suspicious_reasons = _suspicious_reasons(order=order, source_order=source_order)
    tolerated_status_gaps = _tolerated_ibkr_status_gaps(
        source_classification=source_classification,
        suspicious_reasons=suspicious_reasons,
        identity=identity,
        position_open=position_open,
    )
    blocking_suspicious_reasons = [
        reason for reason in suspicious_reasons if reason not in set(tolerated_status_gaps)
    ]
    legacy_market_reference = marketability.get("market_reference")
    reference = _phase1_market_reference(
        config=config,
        symbol=str(order.get("symbol") or ""),
        now=now,
    )
    close_reprice_policy = _managed_close_reprice_policy(order=order, reference=reference)
    classification: str
    recommended_action: str
    rationale: str

    if source_classification == DUPLICATE_CLOSE_ORDER_BLOCKED:
        classification = DO_NOT_REPLACE_DUPLICATE_RISK
        recommended_action = "DO_NOT_REPLACE"
        rationale = "Duplicate close-order evidence is present."
    elif source_classification == BROKER_FLAT_WITH_WORKING_CLOSE:
        classification = BROKER_FLAT_NO_REPLACE
        recommended_action = "REVIEW_BROKER_FLAT_WITH_OPEN_CLOSE"
        rationale = "Broker is flat while a close order appears working; replacement is forbidden."
    elif blocking_suspicious_reasons:
        classification = REVIEW_REQUIRED_SUSPICIOUS_STATE
        recommended_action = "OPERATOR_REVIEW_OR_MANUAL_TWS_PATH"
        rationale = "Suspicious order evidence requires review; automatic replacement is not planned."
    elif source_classification == ORDER_TERMINAL_FILLED or status in _TERMINAL_FILLED_STATUSES:
        classification = BROKER_FLAT_NO_REPLACE if not position_open else REVIEW_REQUIRED_SUSPICIOUS_STATE
        recommended_action = "DO_NOT_REPLACE"
        rationale = "Close order is terminal filled."
    elif source_classification == ORDER_TERMINAL_CANCELLED or status in _TERMINAL_CANCELLED_STATUSES:
        if position_open:
            classification = TARGETED_CANCEL_REPLACE_REQUIRED
            recommended_action = "TARGETED_CANCEL_REPLACE_CANDIDATE"
            rationale = "Prior close order is terminal and the broker position remains open."
        else:
            classification = NO_ACTION_NEEDED
            recommended_action = "WAIT"
            rationale = "Prior order is terminal and no broker position remains open."
    elif source_classification == POSITION_WITHOUT_CLOSE_ORDER:
        classification = ORDER_NOT_FOUND if position_open else NO_ACTION_NEEDED
        recommended_action = "REVIEW_REQUIRED" if position_open else "WAIT"
        rationale = "Broker position exists without a current managed close order."
    elif source_classification == CLOSE_ORDER_NOT_MARKETABLE:
        supervised_ready, supervised_reason = _supervised_non_marketable_modify_ready(
            order=order,
            open_order_truth=open_order_truth,
            position_truth=position_truth,
            identity=identity,
            close_reprice_policy=close_reprice_policy,
            reference=reference,
        )
        if supervised_ready:
            classification = MODIFY_IN_PLACE_ELIGIBLE
            recommended_action = "MODIFY_IN_PLACE_CANDIDATE"
            rationale = "Non-marketable close order is exact, risk-reducing, and eligible for supervised modify-in-place."
        else:
            classification = REVIEW_REQUIRED_SUSPICIOUS_STATE
            recommended_action = "OPERATOR_REVIEW"
            rationale = f"Non-marketable close order requires review: {supervised_reason}."
    elif source_classification in {CLOSE_ORDER_MODIFIABLE, CLOSE_ORDER_CANCEL_REPLACE_REQUIRED, CLOSE_ORDER_SUSPICIOUS} or (
        source_classification == WORKING_CLOSE_ORDER and marketability.get("marketable") is not True
    ):
        if _identity_complete(identity):
            classification = MODIFY_IN_PLACE_ELIGIBLE
            recommended_action = "MODIFY_IN_PLACE_CANDIDATE"
            rationale = "Working close order identity is complete; modify-in-place is preferred before any cancel/replace path."
        else:
            classification = REVIEW_REQUIRED_SUSPICIOUS_STATE
            recommended_action = "REVIEW_REQUIRED"
            rationale = "Working close order is missing identity required for modify-in-place planning."
    elif source_classification == WORKING_CLOSE_ORDER:
        classification = WAIT_FOR_WORKING_ORDER
        recommended_action = "WAIT"
        rationale = "Working close order is live; replacement is forbidden until terminal state is confirmed."
    else:
        classification = ORDER_NOT_FOUND if order else NO_ACTION_NEEDED
        recommended_action = "REVIEW_REQUIRED" if order else "WAIT"
        rationale = "No actionable managed close order was found."

    return {
        "classification": classification,
        "recommended_operator_action": recommended_action,
        "rationale": rationale,
        "dry_run": True,
        "mutation_planned": False,
        "operator_authorization_required": classification
        in {MODIFY_IN_PLACE_ELIGIBLE, TARGETED_CANCEL_REPLACE_REQUIRED, REVIEW_REQUIRED_SUSPICIOUS_STATE},
        "symbol": order.get("symbol"),
        "contract": order.get("contract") or order.get("local_symbol"),
        "con_id": order.get("con_id"),
        "action": order.get("action"),
        "quantity": order.get("quantity"),
        "broker_order_id": order.get("broker_order_id"),
        "perm_id": order.get("perm_id"),
        "broker_status": order.get("broker_status"),
        "limit_price": order.get("limit_price"),
        "market_reference": reference,
        "legacy_market_reference_diagnostic": legacy_market_reference if isinstance(legacy_market_reference, Mapping) else {},
        "managed_close_reprice_policy": close_reprice_policy,
        "identity": identity,
        "identity_complete_for_modify": _identity_complete(identity),
        "position_open": position_open,
        "existing_close_order_live": _working_order_status(status) and source_classification not in _TERMINAL_CLASSES(),
        "terminal_state_confirmed": source_classification in {ORDER_TERMINAL_CANCELLED, ORDER_TERMINAL_FILLED}
        or status in _TERMINAL_CANCELLED_STATUSES
        or status in _TERMINAL_FILLED_STATUSES,
        "suspicious_reasons": suspicious_reasons,
        "blocking_suspicious_reasons": blocking_suspicious_reasons,
        "tolerated_ibkr_status_gaps": tolerated_status_gaps,
        "source_managed_order_classification": source_classification,
        "source_recommended_next_action": order.get("recommended_next_action"),
        "supervised_modify_boundary": (
            _supervised_modify_boundary_evidence(
                open_order_truth=open_order_truth,
                position_truth=position_truth,
                close_reprice_policy=close_reprice_policy,
                reference=reference,
                identity=identity,
            )
            if source_classification == CLOSE_ORDER_NOT_MARKETABLE
            else {}
        ),
        "lifecycle_id": order.get("lifecycle_id"),
        "manifest_id": order.get("manifest_id"),
        "ownership_id": order.get("ownership_id"),
        "source_order": source_order,
    }


def _overall_classification(*, plans: list[dict[str, Any]], managed_order_registry: Mapping[str, Any]) -> str:
    if not plans:
        return NO_ACTION_NEEDED
    priority = [
        DO_NOT_REPLACE_DUPLICATE_RISK,
        BROKER_FLAT_NO_REPLACE,
        REVIEW_REQUIRED_SUSPICIOUS_STATE,
        TARGETED_CANCEL_REPLACE_REQUIRED,
        MODIFY_IN_PLACE_ELIGIBLE,
        WAIT_FOR_WORKING_ORDER,
        ORDER_NOT_FOUND,
        NO_ACTION_NEEDED,
    ]
    classes = {str(plan.get("classification") or "") for plan in plans}
    if not classes and managed_order_registry.get("classification") == NO_MANAGED_ORDERS:
        return NO_ACTION_NEEDED
    for classification in priority:
        if classification in classes:
            return classification
    return REVIEW_REQUIRED_SUSPICIOUS_STATE


def _position_open(*, order: Mapping[str, Any], position_truth: Mapping[str, Any]) -> bool:
    symbol = str(order.get("symbol") or "").upper()
    contract = str(order.get("contract") or order.get("local_symbol") or "").upper()
    for row in _list(position_truth.get("broker_positions")):
        if symbol and str(row.get("symbol") or row.get("track_b_root") or "").upper() != symbol:
            continue
        row_contract = str(row.get("local_symbol") or row.get("contract") or "").upper()
        if contract and row_contract and row_contract != contract:
            continue
        if _quantity(row) != Decimal("0"):
            return True
    summary = _mapping(position_truth.get("summary"))
    return summary.get("broker_exposure_present") is True and bool(symbol)


def _supervised_non_marketable_modify_ready(
    *,
    order: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    identity: Mapping[str, Any],
    close_reprice_policy: Mapping[str, Any],
    reference: Mapping[str, Any],
) -> tuple[bool, str]:
    if not _identity_complete(identity):
        return False, "identity_incomplete"
    if open_order_truth.get("canonical_refresh_scope") != "GLOBAL_COMPLETE":
        return False, "open_order_truth_not_global_complete"
    if _unknown_order_count(open_order_truth) != 0:
        return False, "unknown_orders_present"
    if _duplicate_close_group_count(open_order_truth) != 0:
        return False, "duplicate_close_group_present"
    broker_position = _matching_broker_position(identity=identity, order=order, position_truth=position_truth)
    if not broker_position:
        return False, "current_broker_position_missing"
    if not _risk_reducing_against_position(identity=identity, broker_position=broker_position):
        return False, "close_order_not_risk_reducing"
    if close_reprice_policy.get("classification") != "MANAGED_CLOSE_PRICED":
        return False, str(close_reprice_policy.get("stale_reference_blocker") or "reprice_policy_not_priced")
    final_check = validate_final_marketable_close_limit(
        limit_price=close_reprice_policy.get("limit_price"),
        reference=reference,
        close_action=str(identity.get("action") or ""),
        tick_size=str(close_reprice_policy.get("tick_size") or _tick_size_for_symbol(str(order.get("symbol") or ""))),
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
    )
    if final_check.get("classification") != "MANAGED_CLOSE_FINAL_MARKETABILITY_READY":
        return False, str(final_check.get("block_reason") or "final_marketability_failed")
    return True, "supervised_modify_ready"


def _supervised_modify_boundary_evidence(
    *,
    open_order_truth: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    close_reprice_policy: Mapping[str, Any],
    reference: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "canonical_open_order_truth_global_complete": open_order_truth.get("canonical_refresh_scope") == "GLOBAL_COMPLETE",
        "unknown_order_count": _unknown_order_count(open_order_truth),
        "duplicate_close_group_count": _duplicate_close_group_count(open_order_truth),
        "broker_position_open": bool(_matching_broker_position(identity=identity, order={}, position_truth=position_truth)),
        "risk_reducing": _risk_reducing_against_position(
            identity=identity,
            broker_position=_matching_broker_position(identity=identity, order={}, position_truth=position_truth) or {},
        ),
        "new_limit": close_reprice_policy.get("limit_price"),
        "reprice_policy_classification": close_reprice_policy.get("classification"),
        "reference_price": reference.get("reference_price"),
        "reference_age_seconds": reference.get("reference_age_seconds"),
        "reference_source": reference.get("reference_source"),
    }


def _matching_broker_position(
    *,
    identity: Mapping[str, Any],
    order: Mapping[str, Any],
    position_truth: Mapping[str, Any],
) -> dict[str, Any]:
    symbol = str(order.get("symbol") or "").strip().upper()
    contract = str(identity.get("contract") or "").strip().upper()
    con_id = str(identity.get("con_id") or "").strip()
    account = str(identity.get("account_id") or "").strip()
    for row in _list(position_truth.get("broker_positions")):
        if account and str(row.get("account_id") or row.get("account") or "").strip() not in {"", account}:
            continue
        row_symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
        if symbol and row_symbol and row_symbol != symbol:
            continue
        row_contract = str(row.get("local_symbol") or row.get("localSymbol") or row.get("contract") or "").strip().upper()
        if contract and row_contract and row_contract != contract:
            continue
        row_con_id = str(row.get("con_id") or row.get("conId") or "").strip()
        if con_id and row_con_id and row_con_id != con_id:
            continue
        if _quantity(row) != Decimal("0"):
            return dict(row)
    return {}


def _risk_reducing_against_position(*, identity: Mapping[str, Any], broker_position: Mapping[str, Any]) -> bool:
    action = str(identity.get("action") or "").strip().upper()
    order_qty = _decimal_or_none(identity.get("quantity"))
    broker_qty = _quantity(broker_position)
    if order_qty is None or order_qty <= 0 or broker_qty == 0:
        return False
    if action == "BUY":
        return broker_qty < 0 and order_qty <= abs(broker_qty)
    if action == "SELL":
        return broker_qty > 0 and order_qty <= abs(broker_qty)
    return False


def _unknown_order_count(open_order_truth: Mapping[str, Any]) -> int:
    summary = _mapping(open_order_truth.get("summary"))
    for value in (
        open_order_truth.get("unknown_open_order_count"),
        open_order_truth.get("unknown_order_count"),
        summary.get("unknown_open_order_count"),
        summary.get("unknown_order_count"),
    ):
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return len(_list(open_order_truth.get("unknown_open_orders")))


def _duplicate_close_group_count(open_order_truth: Mapping[str, Any]) -> int:
    summary = _mapping(open_order_truth.get("summary"))
    for value in (
        open_order_truth.get("duplicate_close_order_group_count"),
        summary.get("duplicate_close_order_group_count"),
    ):
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return len(_list(open_order_truth.get("duplicate_close_order_groups")))


def _identity(*, order: Mapping[str, Any], source_order: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": source_order.get("account_id") or order.get("account_id"),
        "contract": order.get("contract") or order.get("local_symbol") or source_order.get("local_symbol"),
        "con_id": order.get("con_id") or source_order.get("con_id"),
        "broker_order_id": order.get("broker_order_id") or source_order.get("broker_order_id") or source_order.get("order_id"),
        "perm_id": order.get("perm_id") or source_order.get("perm_id"),
        "action": order.get("action") or source_order.get("action"),
        "quantity": order.get("quantity") or source_order.get("quantity"),
    }


def _identity_complete(identity: Mapping[str, Any]) -> bool:
    required = ("account_id", "contract", "broker_order_id", "perm_id", "action", "quantity")
    return all(str(identity.get(key) or "").strip() for key in required) and (
        bool(str(identity.get("con_id") or "").strip()) or bool(str(identity.get("contract") or "").strip())
    )


def _suspicious_reasons(*, order: Mapping[str, Any], source_order: Mapping[str, Any]) -> list[str]:
    reasons = [str(item) for item in _list(order.get("suspicious_reasons")) if str(item)]
    filled = _decimal_or_none(source_order.get("filled_quantity") or source_order.get("filled"))
    remaining = source_order.get("remaining_quantity") if "remaining_quantity" in source_order else source_order.get("remaining")
    if filled is not None and abs(filled) >= _SENTINEL_FILLED_QUANTITY:
        reasons.append("sentinel_filled_quantity")
    if order.get("working") is True and remaining in {None, ""}:
        reasons.append("missing_remaining_quantity")
    return sorted(set(reasons))


def _tolerated_ibkr_status_gaps(
    *,
    source_classification: str,
    suspicious_reasons: Sequence[str],
    identity: Mapping[str, Any],
    position_open: bool,
) -> list[str]:
    tolerable = {"sentinel_filled_quantity", "missing_remaining_quantity"}
    reasons = {str(reason) for reason in suspicious_reasons}
    if (
        source_classification in {WORKING_CLOSE_ORDER, CLOSE_ORDER_CANCEL_REPLACE_REQUIRED, CLOSE_ORDER_SUSPICIOUS}
        and position_open
        and _identity_complete(identity)
        and reasons
        and reasons <= tolerable
    ):
        return sorted(reasons)
    return []


def _phase1_market_reference(*, config: TrackBOrderAdjustmentPlannerConfig, symbol: str, now: datetime) -> dict[str, Any]:
    if not symbol:
        return {}
    path = config.resolve(config.market_data_root) / symbol.upper() / "1m" / "latest_runtime_candles.json"
    payload = _read_json(path)
    bars = _list(payload.get("candles") or payload.get("bars"))
    if not bars:
        return {}
    last = _mapping(bars[-1])
    source_symbol = str(payload.get("symbol") or payload.get("instrument_family") or last.get("symbol") or "").strip().upper()
    expected_symbol = str(symbol or "").strip().upper()
    if source_symbol and expected_symbol and source_symbol != expected_symbol:
        return {
            "classification": "RUNTIME_MARKET_REFERENCE_WRONG_SYMBOL",
            "reference_price": None,
            "source_symbol": source_symbol,
            "expected_symbol": expected_symbol,
            "reference_source": str(path),
            "reference_source_type": "phase1_runtime_market_data",
            "pricing_source": "DATABENTO_RUNTIME_1M",
            "reference_age_seconds": None,
        }
    generated_at = _parse_datetime(payload.get("generated_at"))
    bar_end = _parse_datetime(last.get("bar_end") or last.get("timestamp") or last.get("candle_timestamp"))
    freshness_anchor = generated_at or bar_end
    age_seconds = None if freshness_anchor is None else max((now - freshness_anchor).total_seconds(), 0.0)
    return {
        "reference_price": last.get("ask_price") or last.get("bid_price") or last.get("last_price") or last.get("close"),
        "ask_price": last.get("ask_price"),
        "bid_price": last.get("bid_price"),
        "last_price": last.get("last_price"),
        "close": last.get("close"),
        "reference_source": str(path),
        "reference_source_type": "phase1_runtime_market_data",
        "pricing_source": "DATABENTO_RUNTIME_1M",
        "reference_age_seconds": age_seconds,
        "bar_end": last.get("bar_end") or last.get("timestamp") or last.get("candle_timestamp"),
        "generated_at": payload.get("generated_at"),
    }


def _managed_close_reprice_policy(*, order: Mapping[str, Any], reference: Mapping[str, Any]) -> dict[str, Any]:
    symbol = str(order.get("symbol") or "").upper()
    tick_size = _tick_size_for_symbol(symbol)
    execution_class = classify_exit_execution(
        execution_class=order.get("exit_execution_class") or order.get("execution_class"),
        exit_type=order.get("exit_type") or order.get("strategy_type"),
        reason=" ".join(
            str(item or "")
            for item in (
                order.get("reason"),
                order.get("classification"),
                "managed_position_maintenance",
            )
        ),
        policy_id=order.get("managed_exit_policy_id"),
    )
    return _paper_marketable_close_policy(
        reference=reference,
        close_action=str(order.get("action") or ""),
        tick_size=tick_size,
        stale_reference_seconds=_int_or_default(
            order.get("managed_close_stale_reference_seconds"),
            ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        ),
        reprice_attempts=_int_or_default(order.get("reprice_attempt_count") or order.get("modify_attempt_count"), 0),
        execution_class=execution_class,
    )


def _tick_size_for_symbol(symbol: str) -> str:
    metadata = futures_tick_metadata(str(symbol or "").upper())
    if metadata is not None:
        return str(metadata.min_tick)
    return "0.25" if str(symbol or "").upper() in {"MNQ", "MES", "NQ", "ES"} else "0.1"


def _paper_marketable_close_policy(
    *,
    reference: Mapping[str, Any],
    close_action: str,
    tick_size: str,
    stale_reference_seconds: int,
    reprice_attempts: int = 0,
    execution_class: str = EXIT_CLASS_RISK_REDUCING,
) -> dict[str, Any]:
    return build_exit_limit_policy(
        reference=reference,
        close_action=close_action,
        tick_size=tick_size,
        stale_reference_seconds=stale_reference_seconds,
        execution_class=execution_class,
        base_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_attempts=reprice_attempts,
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
    )


def _authority_summary(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    return {
        "artifact_path": str(path),
        "schema_version": payload.get("schema_version"),
        "classification": payload.get("classification") or _mapping(payload.get("summary")).get("overall_classification"),
        "generated_at": payload.get("generated_at"),
        "projection_only": payload.get("projection_only") is True,
    }


def _shared_truth_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at"),
        "exit_code": payload.get("exit_code"),
        "classifications": payload.get("classifications") or {},
        "unsafe_blockers": payload.get("unsafe_blockers") or [],
    }


def _parse_datetime(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _working_order_status(status: str) -> bool:
    if not status:
        return True
    return status not in _TERMINAL_CANCELLED_STATUSES and status not in _TERMINAL_FILLED_STATUSES


def _TERMINAL_CLASSES() -> set[str]:
    return {ORDER_TERMINAL_CANCELLED, ORDER_TERMINAL_FILLED}


def _quantity(row: Mapping[str, Any]) -> Decimal:
    for key in ("quantity", "position", "qty"):
        value = _decimal_or_none(row.get(key))
        if value is not None:
            return value
    return Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
