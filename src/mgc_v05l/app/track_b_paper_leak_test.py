"""Read-only Track B PAPER leak-test planning harness."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ..config_models import load_settings_from_files
from ..execution.ibkr_paper_strategy_bridge import (
    IbkrPaperStrategyBridgeConfig,
    run_ibkr_paper_strategy_bridge,
    write_ibkr_paper_strategy_bridge_artifacts,
)
from ..execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from .probationary_runtime import _active_probationary_paper_lane_specs


PAPER_ACCOUNT_ID = "DUM882026"
RECONCILED_CLASSIFICATION = "TRACK_B_PAPER_BROKER_RECONCILED"
DEFAULT_CONFIGS = (
    "config/base.yaml",
    "config/live.yaml",
    "config/probationary_pattern_engine.yaml",
    "config/probationary_pattern_engine_paper.yaml",
    "config/probationary_pattern_engine_paper_track_b_restored.yaml",
)
RECONCILIATION_PATH = Path("outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json")
OPERATOR_STATUS_PATH = Path("outputs/probationary_pattern_engine/paper_session/operator_status.json")
ACTIVE_LEAK_TEST_PATH = Path("outputs/reports/track_b_paper_leak_test/active_lane.json")
PAPER_READINESS_SNAPSHOT_PATH = Path("outputs/operator_dashboard/paper_readiness_snapshot.json")
STARTUP_CONTROL_PLANE_SNAPSHOT_PATH = Path("outputs/operator_dashboard/startup_control_plane_snapshot.json")
SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH = Path("outputs/operator_dashboard/supervised_paper_operability_snapshot.json")
TEMP_PAPER_RUNTIME_INTEGRITY_SNAPSHOT_PATH = Path("outputs/operator_dashboard/paper_temporary_paper_runtime_integrity_snapshot.json")
DATABENTO_LISTENER_STATUS_PATH = Path("outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json")
DATABENTO_SUPERVISOR_STATUS_PATH = Path("outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_supervisor_status.json")
AUTHORIZATION_ARTIFACT_TYPE = "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION"
AUTHORIZATION_DIGEST_FIELDS = (
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
READINESS_FRESHNESS_WINDOW_SECONDS = 120.0
RESULT_CLASSIFICATIONS = (
    "LEAK_TEST_PASS_FULL_ROUND_TRIP",
    "LEAK_TEST_PASS_BLOCKED_SAFELY",
    "LEAK_TEST_ENTRY_NOT_FILLED_CANCELLED",
    "LEAK_TEST_ENTRY_REJECTED",
    "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP",
    "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED",
    "LEAK_TEST_EXIT_REJECTED",
    "LEAK_TEST_EXIT_FILL_LIFECYCLE_GAP",
    "LEAK_TEST_BROKER_LIFECYCLE_MISMATCH",
    "LEAK_TEST_RUNTIME_RESTORE_FAILURE",
    "LEAK_TEST_PORTFOLIO_ARTIFACT_MISMATCH",
    "LEAK_TEST_REVIEW_REQUIRED",
    "LEAK_TEST_PASS_CONCURRENT_OPEN",
    "LEAK_TEST_PASS_CONCURRENT_EXIT_ONE_HOLD_OTHERS",
    "LEAK_TEST_PASS_CONCURRENT_RESTORE",
    "LEAK_TEST_BLOCKED_BY_EXPOSURE_POLICY",
    "LEAK_TEST_BLOCKED_BY_UNRESOLVED_STATE",
    "LEAK_TEST_STRATEGY_OWNERSHIP_COLLISION",
    "LEAK_TEST_SAME_SYMBOL_CONFLICT",
    "LEAK_TEST_PORTFOLIO_MULTI_POSITION_MISMATCH",
    "LEAK_TEST_PLAN_ONLY",
    "LEAK_TEST_CONCURRENT_PLAN_ONLY",
    "LEAK_TEST_DRY_RUN_READY",
    "LEAK_TEST_LANE_NOT_FOUND",
    "LEAK_TEST_APPLY_REQUIRES_EXPLICIT_APPROVAL_NOT_IMPLEMENTED",
    "LEAK_TEST_AUTHORIZATION_MISSING",
    "LEAK_TEST_AUTHORIZATION_EXPIRED",
    "LEAK_TEST_AUTHORIZATION_DIGEST_MISMATCH",
    "LEAK_TEST_AUTHORIZATION_IDENTITY_MISMATCH",
    "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY",
    "LEAK_TEST_PRECHECK_MARKET_DATA_STALE",
    "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE",
    "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE",
    "LEAK_TEST_PRECHECK_READY",
)
LEAK_TEST_OUTPUT_ROOT = Path("outputs") / "reports" / "track_b_paper_leak_test"
PORTFOLIO_STATE_PATH = Path("outputs") / "reports" / "track_b_portfolio" / "latest_track_b_portfolio_state.json"


@dataclass(frozen=True)
class LeakTestExposurePolicy:
    max_total_open_positions: int = 4
    max_positions_per_symbol: int = 1
    max_positions_per_lane: int = 1
    allow_multiple_symbols: bool = True
    allow_same_symbol_multiple_strategies: bool = False
    allow_micro_and_full_same_underlying: bool = False
    max_open_orders_total: int = 0
    max_open_orders_per_symbol: int = 0


@dataclass(frozen=True)
class LeakTestSafetySnapshot:
    account_id: str | None
    classification: str | None
    broker_reconciled: bool
    review_required_count: int
    open_order_count: int
    broker_position_count: int
    lifecycle_position_count: int
    live_money_eligible: bool
    paper_proof_invoked: bool
    runtime_pid: int | None
    runtime_cwd: str | None
    runtime_command: str | None
    runtime_from_dev_root: bool
    runtime_from_documents_or_icloud: bool
    duplicate_runtime_submitter_count: int
    active_leak_test_lane_id: str | None
    existing_positions: tuple[dict[str, Any], ...]
    existing_open_orders: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class LeakTestLanePlan:
    strategy_id: str
    lane_id: str
    display_name: str
    symbol: str
    localSymbol: str | None
    expiry: str | None
    conId: int | None
    entry_execution_intent: str
    entry_price_source: str
    expected_route: str
    expected_contract: dict[str, Any]
    safe_to_test: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    safe_for_isolated_test: bool
    isolated_blockers: tuple[str, ...]
    safe_for_concurrent_test: bool
    concurrent_blockers: tuple[str, ...]
    max_qty: int
    live_money_eligible: bool
    runtime_pid: int | None
    runtime_cwd: str | None


@dataclass(frozen=True)
class LeakTestConcurrentScenario:
    scenario_id: str
    description: str
    lane_ids: tuple[str, ...]
    symbols: tuple[str, ...]
    strategy_families: tuple[str, ...]
    safe_to_plan: bool
    blockers: tuple[str, ...]
    expected_open_positions: int
    expected_open_orders: int
    validation_steps: tuple[str, ...]
    expected_result_classifications: tuple[str, ...]


@dataclass(frozen=True)
class LeakTestOrderResult:
    phase: str
    classification: str
    terminal_status: str
    order_id: str | None
    client_id: int | None
    perm_id: int | None
    fill_price: float | None
    fill_timestamp: str | None
    execution_price_source: str | None
    raw_classification: str | None
    detail: str | None
    report: dict[str, Any]


@dataclass(frozen=True)
class LeakTestApplyResult:
    lane_id: str
    strategy_id: str
    symbol: str
    localSymbol: str | None
    expiry: str | None
    conId: int | None
    entry_execution_intent: str
    entry_execution_intent_source: str
    pre_apply_blockers: tuple[str, ...]
    authorization_status: str | None
    authorization_path: str | None
    pre_apply_readiness: dict[str, Any] | None
    dry_run: bool
    entry: LeakTestOrderResult | None
    lifecycle_open_result: str | None
    reconciliation_after_entry: dict[str, Any] | None
    exit_policy: str | None
    exit_reason: str | None
    exit: LeakTestOrderResult | None
    lifecycle_close_result: str | None
    reconciliation_after_exit: dict[str, Any] | None
    realized_pnl_estimate: float | None
    portfolio_artifact_status: str | None
    runtime_pid: int | None
    runtime_cwd: str | None
    live_money_eligible: bool
    mutation_performed: bool


@dataclass(frozen=True)
class LeakTestReport:
    mode: str
    generated_at: str
    account_id: str
    live_money_eligible: bool
    mutation_performed: bool
    safety: LeakTestSafetySnapshot
    exposure_policy: LeakTestExposurePolicy
    lanes: tuple[LeakTestLanePlan, ...]
    concurrent_scenarios: tuple[LeakTestConcurrentScenario, ...]
    recommended_first_isolated_sequence: tuple[str, ...]
    recommended_first_concurrent_scenario_id: str | None
    apply_result: LeakTestApplyResult | None
    authorization_artifact: dict[str, Any] | None
    result_classification: str
    notes: tuple[str, ...]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _git_head(repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    head = completed.stdout.strip()
    return head or None


def _canonical_payload(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _authorization_digest_payload(authorization: Mapping[str, Any]) -> dict[str, Any]:
    return {field: authorization.get(field) for field in AUTHORIZATION_DIGEST_FIELDS}


def _authorization_digest(authorization: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_payload(_authorization_digest_payload(authorization))).hexdigest()


def _artifact_age_seconds(payload: Mapping[str, Any], *, now: datetime) -> float | None:
    generated_at = _parse_datetime(payload.get("generated_at"))
    if generated_at is None:
        return None
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now - generated_at.astimezone(timezone.utc)).total_seconds())


def _artifact_status(
    *,
    repo_root: Path,
    relative_path: Path,
    label: str,
    now: datetime,
    freshness_window_seconds: float = READINESS_FRESHNESS_WINDOW_SECONDS,
    required: bool = True,
) -> dict[str, Any]:
    path = repo_root / relative_path
    payload = _read_json(path)
    age_seconds = _artifact_age_seconds(payload, now=now) if payload else None
    fresh = bool(payload) and age_seconds is not None and age_seconds <= freshness_window_seconds
    return {
        "label": label,
        "path": str(path),
        "present": bool(payload),
        "required": required,
        "generated_at": payload.get("generated_at"),
        "age_seconds": age_seconds,
        "freshness_window_seconds": freshness_window_seconds,
        "fresh": fresh,
    }


def _int_value(value: object) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def _float_value(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _runtime_command_for_pid(pid: int | None) -> str | None:
    if pid is None or pid <= 0:
        return None
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    command = completed.stdout.strip()
    return command or None


def _duplicate_runtime_submitter_count(*, repo_root: Path, operator_status: Mapping[str, Any]) -> int:
    explicit = operator_status.get("duplicate_runtime_submitter_count")
    if explicit is not None:
        return _int_value(explicit)
    try:
        completed = subprocess.run(
            ["ps", "-efww"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return 0
    repo_text = str(repo_root)
    count = 0
    for line in completed.stdout.splitlines():
        if "probationary-paper-soak" not in line:
            continue
        if repo_text not in line:
            continue
        if "Documents/MGC-v05l" in line or "Mobile Documents" in line:
            continue
        count += 1
    return count


def _contract_local_symbol(symbol: str, contract_month: str | None) -> str | None:
    month_codes = {
        "01": "F",
        "02": "G",
        "03": "H",
        "04": "J",
        "05": "K",
        "06": "M",
        "07": "N",
        "08": "Q",
        "09": "U",
        "10": "V",
        "11": "X",
        "12": "Z",
    }
    text = str(contract_month or "").strip()
    if len(text) < 6:
        return None
    code = month_codes.get(text[-2:])
    if code is None:
        return None
    return f"{symbol.upper()}{code}{text[3]}"


def _account_from_reconciliation(reconciliation: Mapping[str, Any]) -> str | None:
    match_report = reconciliation.get("position_match_report")
    if isinstance(match_report, Mapping):
        matches = match_report.get("matches")
        if isinstance(matches, list):
            for row in matches:
                if not isinstance(row, Mapping):
                    continue
                broker_position = row.get("broker_position")
                if isinstance(broker_position, Mapping) and broker_position.get("account_id"):
                    return str(broker_position["account_id"])
    if reconciliation.get("account_id"):
        return str(reconciliation["account_id"])
    if reconciliation.get("account"):
        return str(reconciliation["account"])
    return None


def _list_payload(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict(row) for row in value if isinstance(row, Mapping))


def _existing_positions_from_reconciliation(reconciliation: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    lifecycle_rows = _list_payload(reconciliation.get("track_b_lifecycle_positions"))
    broker_rows_by_identity: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in _list_payload(reconciliation.get("track_b_broker_positions")):
        key = (
            str(row.get("account_id") or row.get("account") or ""),
            str(row.get("con_id") or row.get("conId") or ""),
            str(row.get("local_symbol") or row.get("localSymbol") or ""),
        )
        broker_rows_by_identity[key] = row
    positions: list[dict[str, Any]] = []
    for row in lifecycle_rows:
        status = str(row.get("final_position_status") or row.get("position_status") or "").upper()
        if status and "OPEN" not in status:
            continue
        account_id = str(row.get("account_id") or row.get("account") or "")
        con_id = str(row.get("con_id") or row.get("conId") or "")
        local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "")
        broker_row = broker_rows_by_identity.get((account_id, con_id, local_symbol)) or {}
        positions.append(
            {
                "account_id": account_id or broker_row.get("account_id") or broker_row.get("account"),
                "symbol": str(row.get("instrument_family") or row.get("symbol") or broker_row.get("symbol") or "").upper(),
                "localSymbol": local_symbol or broker_row.get("local_symbol") or broker_row.get("localSymbol"),
                "conId": _int_value(con_id or broker_row.get("con_id") or broker_row.get("conId")),
                "qty": _int_value(row.get("quantity") or broker_row.get("quantity")),
                "side": row.get("side"),
                "strategy_id": row.get("strategy_id"),
                "lane_id": row.get("lane_id"),
                "lifecycle_id": row.get("lifecycle_id"),
                "matched": True,
                "strategy_owned": bool(row.get("strategy_id") or row.get("lane_id")),
            }
        )
    return tuple(positions)


def _existing_open_orders_from_reconciliation(reconciliation: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return _list_payload(reconciliation.get("track_b_broker_open_orders"))


def build_safety_snapshot(
    *,
    repo_root: Path,
    reconciliation: Mapping[str, Any] | None = None,
    operator_status: Mapping[str, Any] | None = None,
    active_leak_test: Mapping[str, Any] | None = None,
    runtime_command: str | None = None,
) -> LeakTestSafetySnapshot:
    reconciliation_payload = dict(reconciliation or _read_json(repo_root / RECONCILIATION_PATH))
    operator_payload = dict(operator_status or _read_json(repo_root / OPERATOR_STATUS_PATH))
    active_payload = dict(active_leak_test or _read_json(repo_root / ACTIVE_LEAK_TEST_PATH))
    runtime_pid = _int_value(operator_payload.get("source_runtime_pid")) or None
    command = runtime_command if runtime_command is not None else _runtime_command_for_pid(runtime_pid)
    runtime_cwd = operator_payload.get("source_runtime_cwd") or operator_payload.get("runtime_cwd")
    runtime_cwd_text = str(runtime_cwd) if runtime_cwd else None
    repo_root_text = str(repo_root)
    command_text = str(command or "")
    cwd_or_command = f"{runtime_cwd_text or ''} {command_text}"
    return LeakTestSafetySnapshot(
        account_id=_account_from_reconciliation(reconciliation_payload),
        classification=str(reconciliation_payload.get("classification") or "") or None,
        broker_reconciled=bool(reconciliation_payload.get("broker_reconciled") is True),
        review_required_count=_int_value(reconciliation_payload.get("review_required_count")),
        open_order_count=_int_value(reconciliation_payload.get("track_b_broker_open_order_count"))
        + _int_value(reconciliation_payload.get("lifecycle_open_order_count")),
        broker_position_count=_int_value(reconciliation_payload.get("track_b_broker_position_count")),
        lifecycle_position_count=_int_value(reconciliation_payload.get("lifecycle_open_position_count")),
        live_money_eligible=bool(reconciliation_payload.get("live_money_eligible") is True),
        paper_proof_invoked=bool(reconciliation_payload.get("paper_proof_invoked") is True),
        runtime_pid=runtime_pid,
        runtime_cwd=runtime_cwd_text,
        runtime_command=command,
        runtime_from_dev_root=repo_root_text in cwd_or_command,
        runtime_from_documents_or_icloud=("Documents/MGC-v05l" in cwd_or_command or "Mobile Documents" in cwd_or_command),
        duplicate_runtime_submitter_count=_duplicate_runtime_submitter_count(
            repo_root=repo_root,
            operator_status=operator_payload,
        ),
        active_leak_test_lane_id=str(active_payload.get("lane_id") or "").strip() or None,
        existing_positions=_existing_positions_from_reconciliation(reconciliation_payload),
        existing_open_orders=_existing_open_orders_from_reconciliation(reconciliation_payload),
    )


def _unresolved_state_blockers(*, repo_root: Path, safety: LeakTestSafetySnapshot) -> list[str]:
    blockers: list[str] = []
    if str(repo_root.resolve()) != "/Users/patrick/Dev/MGC-v05l-automation":
        blockers.append("repo_root_not_dev_checkout")
    if safety.account_id not in {None, PAPER_ACCOUNT_ID}:
        blockers.append("account_not_DUM882026")
    if safety.classification != RECONCILED_CLASSIFICATION or not safety.broker_reconciled:
        blockers.append("broker_lifecycle_not_reconciled")
    if safety.review_required_count != 0:
        blockers.append("review_required_nonzero")
    if safety.broker_position_count != safety.lifecycle_position_count:
        blockers.append("broker_lifecycle_position_count_mismatch")
    if safety.live_money_eligible:
        blockers.append("live_money_eligible_true")
    if safety.paper_proof_invoked:
        blockers.append("paper_proof_invoked_true")
    if safety.runtime_pid is None:
        blockers.append("runtime_pid_missing")
    if not safety.runtime_from_dev_root:
        blockers.append("runtime_not_verified_from_dev_root")
    if safety.runtime_from_documents_or_icloud:
        blockers.append("runtime_from_documents_or_icloud")
    if safety.duplicate_runtime_submitter_count > 1:
        blockers.append("duplicate_runtime_submitters")
    return blockers


def _underlying_family(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized in {"GC", "MGC"}:
        return "GC"
    if normalized in {"NQ", "MNQ"}:
        return "NQ"
    if normalized in {"ES", "MES"}:
        return "ES"
    return normalized


def _lane_strategy_family(strategy_id: str) -> str:
    if strategy_id.startswith("gc_mgc_forced_session_baseline"):
        return "gc_mgc_forced_session_baseline"
    if strategy_id.startswith("asia_london_participation_core"):
        return "asia_london_participation_core"
    if strategy_id.startswith("index_futures_ny_intraday_forced_core"):
        return "index_futures_ny_intraday_forced_core"
    if strategy_id.startswith("atp_companion_v1__production_track"):
        return "atp_companion_production_track"
    if strategy_id.startswith("atp_companion_v1__paper"):
        return "atp_companion_paper_candidate"
    if "__" in strategy_id:
        return strategy_id.split("__", 1)[0]
    return strategy_id


def _position_count_for_symbol(safety: LeakTestSafetySnapshot, symbol: str) -> int:
    return sum(1 for row in safety.existing_positions if str(row.get("symbol") or "").upper() == symbol.upper())


def _position_count_for_lane(safety: LeakTestSafetySnapshot, lane_id: str) -> int:
    return sum(1 for row in safety.existing_positions if str(row.get("lane_id") or "") == lane_id)


def _exposure_policy_blockers(
    *,
    safety: LeakTestSafetySnapshot,
    policy: LeakTestExposurePolicy,
    lane_id: str,
    symbol: str,
    isolated: bool,
) -> list[str]:
    blockers: list[str] = []
    if safety.active_leak_test_lane_id and safety.active_leak_test_lane_id != lane_id:
        blockers.append("another_leak_test_lane_active")
    if isolated:
        if safety.open_order_count != 0:
            blockers.append("open_orders_present_for_isolated_round_trip")
        if safety.broker_position_count != 0 or safety.lifecycle_position_count != 0:
            blockers.append("existing_positions_present_for_isolated_round_trip")
        return blockers

    if safety.open_order_count > policy.max_open_orders_total:
        blockers.append("open_order_total_exceeds_policy")
    if safety.broker_position_count >= policy.max_total_open_positions:
        blockers.append("max_total_open_positions_reached")
    if _position_count_for_lane(safety, lane_id) >= policy.max_positions_per_lane:
        blockers.append("duplicate_same_lane_position")
    same_symbol_count = _position_count_for_symbol(safety, symbol)
    if same_symbol_count >= policy.max_positions_per_symbol:
        blockers.append("max_positions_per_symbol_reached")
    if same_symbol_count and not policy.allow_same_symbol_multiple_strategies:
        blockers.append("same_symbol_conflict")
    existing_symbols = {str(row.get("symbol") or "").upper() for row in safety.existing_positions if row.get("symbol")}
    if existing_symbols and symbol.upper() not in existing_symbols and not policy.allow_multiple_symbols:
        blockers.append("multiple_symbols_not_allowed")
    existing_underlyings = {_underlying_family(existing) for existing in existing_symbols}
    if (
        existing_underlyings
        and _underlying_family(symbol) in existing_underlyings
        and symbol.upper() not in existing_symbols
        and not policy.allow_micro_and_full_same_underlying
    ):
        blockers.append("micro_and_full_same_underlying_not_allowed")
    return blockers


def _lane_plan(
    *,
    spec: Any,
    safety: LeakTestSafetySnapshot,
    unresolved_blockers: Sequence[str],
    exposure_policy: LeakTestExposurePolicy,
) -> LeakTestLanePlan | None:
    adapter = lane_submit_bridge_adapter(lane_id=spec.lane_id)
    if not adapter:
        return None
    target = dict(adapter.get("bridge_execution_target") or {})
    route = str(adapter.get("current_order_destination") or "unknown")
    symbol = str(target.get("symbol") or spec.symbol).upper()
    contract_month = str(target.get("contract_month") or "").strip() or None
    entry_intent = str(adapter.get("entry_execution_intent") or "PARTICIPATE_NOW_INFERRED").strip()
    base_blockers = list(unresolved_blockers)
    isolated_blockers = list(base_blockers)
    isolated_blockers.extend(
        _exposure_policy_blockers(
            safety=safety,
            policy=exposure_policy,
            lane_id=str(spec.lane_id),
            symbol=symbol,
            isolated=True,
        )
    )
    concurrent_blockers = list(base_blockers)
    concurrent_blockers.extend(
        _exposure_policy_blockers(
            safety=safety,
            policy=exposure_policy,
            lane_id=str(spec.lane_id),
            symbol=symbol,
            isolated=False,
        )
    )
    if int(getattr(spec, "trade_size", 1) or 1) > 1:
        isolated_blockers.append("max_qty_exceeds_1")
        concurrent_blockers.append("max_qty_exceeds_1")
    if route != "ibkr_paper_bridge_submit_capable":
        isolated_blockers.append("route_not_guarded_paper_bridge")
        concurrent_blockers.append("route_not_guarded_paper_bridge")
    if not _contract_local_symbol(symbol, contract_month) or not contract_month:
        isolated_blockers.append("contract_identity_unresolved")
        concurrent_blockers.append("contract_identity_unresolved")
    warnings: list[str] = []
    if getattr(spec, "non_approved", False):
        warnings.append("lane_marked_non_approved_candidate")
    if entry_intent == "PARTICIPATE_NOW_INFERRED":
        warnings.append("entry_execution_intent_inferred_from_guarded_route")
    return LeakTestLanePlan(
        strategy_id=str(getattr(spec, "standalone_strategy_id", None) or spec.lane_id),
        lane_id=str(spec.lane_id),
        display_name=str(spec.display_name),
        symbol=symbol,
        localSymbol=_contract_local_symbol(symbol, contract_month),
        expiry=contract_month,
        conId=None,
        entry_execution_intent=entry_intent,
        entry_price_source="PLAN_ONLY_NOT_PRICED",
        expected_route=route,
        expected_contract=target,
        safe_to_test=not isolated_blockers,
        blockers=tuple(dict.fromkeys(isolated_blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
        safe_for_isolated_test=not isolated_blockers,
        isolated_blockers=tuple(dict.fromkeys(isolated_blockers)),
        safe_for_concurrent_test=not concurrent_blockers,
        concurrent_blockers=tuple(dict.fromkeys(concurrent_blockers)),
        max_qty=1,
        live_money_eligible=False,
        runtime_pid=safety.runtime_pid,
        runtime_cwd=safety.runtime_cwd,
    )


def _scenario_for_lanes(
    *,
    scenario_id: str,
    description: str,
    lanes: Sequence[LeakTestLanePlan],
    safety: LeakTestSafetySnapshot,
    exposure_policy: LeakTestExposurePolicy,
    expected_result_classifications: Sequence[str],
) -> LeakTestConcurrentScenario:
    blockers: list[str] = []
    symbols = tuple(dict.fromkeys(lane.symbol for lane in lanes))
    families = tuple(dict.fromkeys(_lane_strategy_family(lane.strategy_id) for lane in lanes))
    blockers.extend(blocker for lane in lanes for blocker in lane.concurrent_blockers)
    if len(lanes) + safety.broker_position_count > exposure_policy.max_total_open_positions:
        blockers.append("scenario_exceeds_max_total_open_positions")
    symbol_counts: dict[str, int] = {}
    for lane in lanes:
        symbol_counts[lane.symbol] = symbol_counts.get(lane.symbol, 0) + 1
    for symbol, count in symbol_counts.items():
        if count > exposure_policy.max_positions_per_symbol:
            blockers.append(f"scenario_max_positions_per_symbol:{symbol}")
    if len(symbols) > 1 and not exposure_policy.allow_multiple_symbols:
        blockers.append("scenario_multiple_symbols_not_allowed")
    underlying_counts: dict[str, set[str]] = {}
    for symbol in symbols:
        underlying_counts.setdefault(_underlying_family(symbol), set()).add(symbol)
    if not exposure_policy.allow_micro_and_full_same_underlying:
        for underlying, underlying_symbols in underlying_counts.items():
            if len(underlying_symbols) > 1:
                blockers.append(f"scenario_micro_and_full_same_underlying_not_allowed:{underlying}")
    return LeakTestConcurrentScenario(
        scenario_id=scenario_id,
        description=description,
        lane_ids=tuple(lane.lane_id for lane in lanes),
        symbols=symbols,
        strategy_families=families,
        safe_to_plan=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        expected_open_positions=safety.broker_position_count + len(lanes),
        expected_open_orders=0,
        validation_steps=(
            "enter each lane through the guarded PAPER route only",
            "verify broker/lifecycle reconciliation after each entry",
            "exit one position while the others remain open",
            "verify portfolio artifact contains all remaining managed positions",
            "restart/restore runtime and verify every open position remains managed",
        ),
        expected_result_classifications=tuple(expected_result_classifications),
    )


def _first_lane_for_symbol(lanes: Sequence[LeakTestLanePlan], symbol: str) -> LeakTestLanePlan | None:
    approved = [
        lane
        for lane in lanes
        if lane.symbol == symbol
        and lane.safe_for_concurrent_test
        and "lane_marked_non_approved_candidate" not in lane.warnings
    ]
    if approved:
        return approved[0]
    fallback = [lane for lane in lanes if lane.symbol == symbol and lane.safe_for_concurrent_test]
    return fallback[0] if fallback else None


def _build_concurrent_scenarios(
    *,
    lanes: Sequence[LeakTestLanePlan],
    safety: LeakTestSafetySnapshot,
    exposure_policy: LeakTestExposurePolicy,
) -> tuple[LeakTestConcurrentScenario, ...]:
    by_symbol = {symbol: _first_lane_for_symbol(lanes, symbol) for symbol in ("GC", "MGC", "MNQ", "PL")}
    scenarios: list[LeakTestConcurrentScenario] = []
    gc = by_symbol.get("GC")
    mgc = by_symbol.get("MGC")
    mnq = by_symbol.get("MNQ")
    pl = by_symbol.get("PL")
    if gc and mnq and pl:
        scenarios.append(
            _scenario_for_lanes(
                scenario_id="first_concurrent_gc_mnq_pl",
                description="Open one GC, one MNQ, and one PL position; then exit one while holding the others.",
                lanes=(gc, mnq, pl),
                safety=safety,
                exposure_policy=exposure_policy,
                expected_result_classifications=(
                    "LEAK_TEST_PASS_CONCURRENT_OPEN",
                    "LEAK_TEST_PASS_CONCURRENT_EXIT_ONE_HOLD_OTHERS",
                    "LEAK_TEST_PASS_CONCURRENT_RESTORE",
                ),
            )
        )
    if mgc and mnq and pl:
        scenarios.append(
            _scenario_for_lanes(
                scenario_id="micro_concurrent_mgc_mnq_pl",
                description="Lower-notional concurrent test using MGC, MNQ, and PL.",
                lanes=(mgc, mnq, pl),
                safety=safety,
                exposure_policy=exposure_policy,
                expected_result_classifications=(
                    "LEAK_TEST_PASS_CONCURRENT_OPEN",
                    "LEAK_TEST_PASS_CONCURRENT_EXIT_ONE_HOLD_OTHERS",
                    "LEAK_TEST_PASS_CONCURRENT_RESTORE",
                ),
            )
        )
    if gc and mgc:
        scenarios.append(
            _scenario_for_lanes(
                scenario_id="same_underlying_gc_mgc_conflict_probe",
                description="Policy probe for simultaneous GC and MGC exposure on the same underlying.",
                lanes=(gc, mgc),
                safety=safety,
                exposure_policy=exposure_policy,
                expected_result_classifications=("LEAK_TEST_SAME_SYMBOL_CONFLICT", "LEAK_TEST_BLOCKED_BY_EXPOSURE_POLICY"),
            )
        )
    return tuple(scenarios)


def build_plan_only_report(
    *,
    repo_root: Path,
    reconciliation: Mapping[str, Any] | None = None,
    operator_status: Mapping[str, Any] | None = None,
    active_leak_test: Mapping[str, Any] | None = None,
    runtime_command: str | None = None,
    exposure_policy: LeakTestExposurePolicy | None = None,
) -> LeakTestReport:
    policy = exposure_policy or LeakTestExposurePolicy()
    settings = load_settings_from_files([repo_root / path for path in DEFAULT_CONFIGS])
    safety = build_safety_snapshot(
        repo_root=repo_root,
        reconciliation=reconciliation,
        operator_status=operator_status,
        active_leak_test=active_leak_test,
        runtime_command=runtime_command,
    )
    unresolved_blockers = _unresolved_state_blockers(repo_root=repo_root, safety=safety)
    lanes = tuple(
        lane
        for spec in _active_probationary_paper_lane_specs(settings)
        for lane in [
            _lane_plan(spec=spec, safety=safety, unresolved_blockers=unresolved_blockers, exposure_policy=policy)
        ]
        if lane is not None
    )
    scenarios = _build_concurrent_scenarios(lanes=lanes, safety=safety, exposure_policy=policy)
    recommended_isolated = tuple(lane.lane_id for lane in lanes if lane.safe_for_isolated_test)[:3]
    recommended_concurrent = next((scenario.scenario_id for scenario in scenarios if scenario.safe_to_plan), None)
    return LeakTestReport(
        mode="plan-only",
        generated_at=datetime.now(timezone.utc).isoformat(),
        account_id=PAPER_ACCOUNT_ID,
        live_money_eligible=False,
        mutation_performed=False,
        safety=safety,
        exposure_policy=policy,
        lanes=lanes,
        concurrent_scenarios=scenarios,
        recommended_first_isolated_sequence=recommended_isolated,
        recommended_first_concurrent_scenario_id=recommended_concurrent,
        apply_result=None,
        authorization_artifact=None,
        result_classification="LEAK_TEST_PLAN_ONLY",
        notes=(
            "Plan-only mode performs no broker mutation.",
            "Single-lane apply exists but is broker-mutating unless --dry-run is used.",
            "Existing clean managed positions may coexist when exposure policy allows; unresolved broker/lifecycle state still blocks.",
        ),
    )


def build_concurrent_plan_report(
    *,
    repo_root: Path,
    reconciliation: Mapping[str, Any] | None = None,
    operator_status: Mapping[str, Any] | None = None,
    active_leak_test: Mapping[str, Any] | None = None,
    runtime_command: str | None = None,
    exposure_policy: LeakTestExposurePolicy | None = None,
) -> LeakTestReport:
    plan = build_plan_only_report(
        repo_root=repo_root,
        reconciliation=reconciliation,
        operator_status=operator_status,
        active_leak_test=active_leak_test,
        runtime_command=runtime_command,
        exposure_policy=exposure_policy,
    )
    return LeakTestReport(
        mode="concurrent-plan",
        generated_at=plan.generated_at,
        account_id=plan.account_id,
        live_money_eligible=False,
        mutation_performed=False,
        safety=plan.safety,
        exposure_policy=plan.exposure_policy,
        lanes=plan.lanes,
        concurrent_scenarios=plan.concurrent_scenarios,
        recommended_first_isolated_sequence=plan.recommended_first_isolated_sequence,
        recommended_first_concurrent_scenario_id=plan.recommended_first_concurrent_scenario_id,
        apply_result=None,
        authorization_artifact=None,
        result_classification="LEAK_TEST_CONCURRENT_PLAN_ONLY",
        notes=(
            "Concurrent-plan mode proposes multi-position scenarios only.",
            "No broker mutation is implemented or executed by this mode.",
        ),
    )


def build_single_lane_dry_run_report(
    *,
    repo_root: Path,
    lane_id: str,
    reconciliation: Mapping[str, Any] | None = None,
    operator_status: Mapping[str, Any] | None = None,
    active_leak_test: Mapping[str, Any] | None = None,
    runtime_command: str | None = None,
    exposure_policy: LeakTestExposurePolicy | None = None,
    write_authorization: bool = False,
    authorization_ttl_seconds: float = 600.0,
    authorization_output_path: Path | None = None,
) -> LeakTestReport:
    plan = build_plan_only_report(
        repo_root=repo_root,
        reconciliation=reconciliation,
        operator_status=operator_status,
        active_leak_test=active_leak_test,
        runtime_command=runtime_command,
        exposure_policy=exposure_policy,
    )
    lanes = tuple(lane for lane in plan.lanes if lane.lane_id == lane_id)
    classification = "LEAK_TEST_PASS_BLOCKED_SAFELY"
    if lanes and lanes[0].safe_to_test:
        classification = "LEAK_TEST_DRY_RUN_READY"
    authorization_artifact: dict[str, Any] | None = None
    if write_authorization and lanes and lanes[0].safe_to_test:
        lane = lanes[0]
        authorization_artifact = build_leak_test_authorization(
            repo_root=repo_root,
            lane=lane,
            safety=plan.safety,
            action=_bridge_action_for_lane(lane),
            ttl_seconds=authorization_ttl_seconds,
        )
        output_path = authorization_output_path or _authorization_output_path(repo_root, lane.lane_id)
        authorization_artifact["authorization_path"] = str(output_path)
        _write_json(output_path, authorization_artifact)
    return LeakTestReport(
        mode="single-lane-dry-run",
        generated_at=plan.generated_at,
        account_id=plan.account_id,
        live_money_eligible=False,
        mutation_performed=False,
        safety=plan.safety,
        exposure_policy=plan.exposure_policy,
        lanes=lanes,
        concurrent_scenarios=plan.concurrent_scenarios,
        recommended_first_isolated_sequence=plan.recommended_first_isolated_sequence,
        recommended_first_concurrent_scenario_id=plan.recommended_first_concurrent_scenario_id,
        apply_result=None,
        authorization_artifact=authorization_artifact,
        result_classification=classification if lanes else "LEAK_TEST_LANE_NOT_FOUND",
        notes=(
            "Dry-run validates preconditions and does not call the broker route.",
            "Authorization artifact written for one explicit lane/action." if authorization_artifact else "No authorization artifact written.",
        ),
    )


def _guarded_bridge_route(config: IbkrPaperStrategyBridgeConfig) -> dict[str, Any]:
    artifacts = run_ibkr_paper_strategy_bridge(config=config)
    output_dir = config.repo_root / (config.output_dir or LEAK_TEST_OUTPUT_ROOT / "bridge")
    write_ibkr_paper_strategy_bridge_artifacts(output_dir=output_dir, artifacts=artifacts)
    return {"classification": artifacts.classification, "report": artifacts.report}


def _default_reconciliation_reader(repo_root: Path, stage: str) -> dict[str, Any]:
    del stage
    return _read_json(repo_root / RECONCILIATION_PATH)


def _bridge_action_for_lane(lane: LeakTestLanePlan) -> str:
    text = f"{lane.lane_id} {lane.display_name} {lane.strategy_id}".upper()
    if "SHORT" in text:
        return "SELL"
    return "BUY"


def _close_action_for_entry(entry_action: str) -> str:
    return "SELL" if entry_action == "BUY" else "BUY"


def _intent_type_for_action(action: str, *, close: bool) -> str:
    if close:
        return "SELL_TO_CLOSE" if action == "SELL" else "BUY_TO_CLOSE"
    return "BUY_TO_OPEN" if action == "BUY" else "SELL_TO_OPEN"


def _limit_price_model_for_action(action: str) -> str:
    if action == "SELL":
        return "DELAYED_BID_MINUS_1T_MARKETABLE_SELL"
    return "DELAYED_ASK_PLUS_1T_MARKETABLE_BUY"


def _authorization_output_path(repo_root: Path, lane_id: str) -> Path:
    return repo_root / LEAK_TEST_OUTPUT_ROOT / lane_id / "track_b_paper_leak_test_authorization.json"


def _safety_snapshot_for_authorization(safety: LeakTestSafetySnapshot) -> dict[str, Any]:
    return {
        "reconciliation_classification": safety.classification,
        "broker_reconciled": safety.broker_reconciled,
        "positions_count": safety.broker_position_count,
        "lifecycle_positions_count": safety.lifecycle_position_count,
        "open_orders_count": safety.open_order_count,
        "review_required_count": safety.review_required_count,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_leak_test_authorization(
    *,
    repo_root: Path,
    lane: LeakTestLanePlan,
    safety: LeakTestSafetySnapshot,
    action: str,
    ttl_seconds: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    created = now or _utc_now()
    expires = created + timedelta(seconds=max(1.0, float(ttl_seconds)))
    authorization: dict[str, Any] = {
        "artifact_type": AUTHORIZATION_ARTIFACT_TYPE,
        "account_id": PAPER_ACCOUNT_ID,
        "mode": "PAPER",
        "lane_id": lane.lane_id,
        "strategy_id": lane.strategy_id,
        "symbol": lane.symbol,
        "local_symbol": lane.localSymbol,
        "expiry": lane.expiry,
        "con_id": lane.conId,
        "action": action,
        "exit_action": _close_action_for_entry(action),
        "qty": 1,
        "repo_root": str(repo_root),
        "git_head": _git_head(repo_root),
        "created_at": created.isoformat(),
        "expires_at": expires.isoformat(),
        "safety_snapshot": _safety_snapshot_for_authorization(safety),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    authorization["digest"] = _authorization_digest(authorization)
    return authorization


def _authorization_identity_mismatches(
    *,
    authorization: Mapping[str, Any],
    repo_root: Path,
    lane: LeakTestLanePlan,
    action: str,
) -> tuple[str, ...]:
    expected = {
        "artifact_type": AUTHORIZATION_ARTIFACT_TYPE,
        "account_id": PAPER_ACCOUNT_ID,
        "mode": "PAPER",
        "lane_id": lane.lane_id,
        "strategy_id": lane.strategy_id,
        "symbol": lane.symbol,
        "local_symbol": lane.localSymbol,
        "expiry": lane.expiry,
        "con_id": lane.conId,
        "action": action,
        "qty": 1,
        "repo_root": str(repo_root),
    }
    mismatches = [key for key, value in expected.items() if authorization.get(key) != value]
    current_head = _git_head(repo_root)
    auth_head = str(authorization.get("git_head") or "").strip()
    if auth_head and current_head and auth_head != current_head:
        mismatches.append("git_head")
    safety = dict(authorization.get("safety_snapshot") or {})
    if safety.get("live_money_eligible") is not False:
        mismatches.append("safety_snapshot.live_money_eligible")
    if safety.get("paper_proof_invoked") is not False:
        mismatches.append("safety_snapshot.paper_proof_invoked")
    return tuple(mismatches)


def validate_leak_test_authorization(
    *,
    authorization_path: Path | None,
    repo_root: Path,
    lane: LeakTestLanePlan,
    action: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    if authorization_path is None:
        return {"classification": "LEAK_TEST_AUTHORIZATION_MISSING", "valid": False, "detail": "Missing --authorization-path."}
    path = Path(authorization_path)
    authorization = _read_json(path)
    if not authorization:
        return {
            "classification": "LEAK_TEST_AUTHORIZATION_MISSING",
            "valid": False,
            "path": str(path),
            "detail": "Authorization artifact is missing or unreadable.",
        }
    expected_digest = _authorization_digest(authorization)
    if str(authorization.get("digest") or "") != expected_digest:
        return {
            "classification": "LEAK_TEST_AUTHORIZATION_DIGEST_MISMATCH",
            "valid": False,
            "path": str(path),
            "expected_digest": expected_digest,
            "actual_digest": authorization.get("digest"),
            "detail": "Authorization digest does not match the critical fields.",
        }
    expires_at = _parse_datetime(authorization.get("expires_at"))
    current = now or _utc_now()
    if expires_at is None or expires_at.astimezone(timezone.utc) <= current.astimezone(timezone.utc):
        return {
            "classification": "LEAK_TEST_AUTHORIZATION_EXPIRED",
            "valid": False,
            "path": str(path),
            "expires_at": authorization.get("expires_at"),
            "detail": "Authorization artifact is expired.",
        }
    mismatches = _authorization_identity_mismatches(
        authorization=authorization,
        repo_root=repo_root,
        lane=lane,
        action=action,
    )
    if mismatches:
        return {
            "classification": "LEAK_TEST_AUTHORIZATION_IDENTITY_MISMATCH",
            "valid": False,
            "path": str(path),
            "mismatches": mismatches,
            "detail": f"Authorization identity mismatch: {', '.join(mismatches)}.",
        }
    return {
        "classification": "LEAK_TEST_AUTHORIZATION_VALID",
        "valid": True,
        "path": str(path),
        "digest": authorization.get("digest"),
        "authorization": authorization,
        "detail": "Leak-test authorization artifact is valid for this lane/action/account.",
    }


def _seconds_between(left: object, right: object) -> float | None:
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None or right_dt is None:
        return None
    return abs((left_dt.astimezone(timezone.utc) - right_dt.astimezone(timezone.utc)).total_seconds())


def _lane_market_data_micro_stale(row: Mapping[str, Any], *, safety: LeakTestSafetySnapshot) -> bool:
    if safety.broker_position_count or safety.lifecycle_position_count:
        return False
    if str(row.get("bar_state") or "").upper() != "MARKET_DATA_STALE":
        return False
    bar_gap = _seconds_between(row.get("expected_completed_bar_end_ts"), row.get("observed_completed_bar_end_ts"))
    return (
        bar_gap is not None
        and bar_gap <= 60.0
        and _float_value(row.get("market_data_lag_seconds"), 999999.0) <= 60.0
        and _float_value(row.get("observed_bar_arrival_age_seconds"), 999999.0) <= 180.0
        and "completed execution bar" in str(row.get("bar_state_reason") or "")
    )


def _lane_market_data_stale(row: Mapping[str, Any]) -> bool:
    return (
        bool(row.get("market_data_stale"))
        or str(row.get("bar_state") or "").strip().upper() == "MARKET_DATA_STALE"
        or str(row.get("tradability_status") or "").strip().upper() == "MARKET_DATA_STALE"
    )


def _lane_market_data_age_seconds(row: Mapping[str, Any]) -> float | None:
    for key in ("observed_bar_arrival_age_seconds", "latest_bar_age_seconds", "market_data_lag_seconds"):
        if row.get(key) is not None:
            value = _float_value(row.get(key), -1.0)
            if value >= 0:
                return value
    return None


def _lane_required_timeframe(row: Mapping[str, Any], lane: LeakTestLanePlan) -> str | None:
    for key in ("execution_timeframe", "artifact_timeframe", "resolved_execution_timeframe", "structural_signal_timeframe"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    text = f"{lane.lane_id} {lane.display_name}".lower()
    for timeframe in ("1m", "3m", "5m"):
        if timeframe in text:
            return timeframe
    return None


def _market_data_stale_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "lane_id": row.get("lane_id") or row.get("strategy_id"),
        "strategy_id": row.get("strategy_id"),
        "symbol": row.get("symbol") or row.get("instrument"),
        "bar_state": row.get("bar_state"),
        "tradability_status": row.get("tradability_status"),
        "expected_completed_bar_end_ts": row.get("expected_completed_bar_end_ts"),
        "observed_completed_bar_end_ts": row.get("observed_completed_bar_end_ts"),
        "market_data_lag_seconds": row.get("market_data_lag_seconds"),
        "observed_bar_arrival_age_seconds": row.get("observed_bar_arrival_age_seconds"),
    }


def _pre_apply_readiness_check(
    *,
    repo_root: Path,
    lane: LeakTestLanePlan,
    safety: LeakTestSafetySnapshot,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or _utc_now()
    paper_readiness = _read_json(repo_root / PAPER_READINESS_SNAPSHOT_PATH)
    startup = _read_json(repo_root / STARTUP_CONTROL_PLANE_SNAPSHOT_PATH)
    supervised = _read_json(repo_root / SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH)
    temp_integrity = _read_json(repo_root / TEMP_PAPER_RUNTIME_INTEGRITY_SNAPSHOT_PATH)
    listener = _read_json(repo_root / DATABENTO_LISTENER_STATUS_PATH)
    supervisor = _read_json(repo_root / DATABENTO_SUPERVISOR_STATUS_PATH)
    artifacts = {
        "paper_readiness": _artifact_status(
            repo_root=repo_root,
            relative_path=PAPER_READINESS_SNAPSHOT_PATH,
            label="paper_readiness_snapshot",
            now=current,
        ),
        "startup_control_plane": _artifact_status(
            repo_root=repo_root,
            relative_path=STARTUP_CONTROL_PLANE_SNAPSHOT_PATH,
            label="startup_control_plane_snapshot",
            now=current,
        ),
        "supervised_paper_operability": _artifact_status(
            repo_root=repo_root,
            relative_path=SUPERVISED_PAPER_OPERABILITY_SNAPSHOT_PATH,
            label="supervised_paper_operability_snapshot",
            now=current,
        ),
        "temporary_paper_runtime_integrity": _artifact_status(
            repo_root=repo_root,
            relative_path=TEMP_PAPER_RUNTIME_INTEGRITY_SNAPSHOT_PATH,
            label="paper_temporary_paper_runtime_integrity_snapshot",
            now=current,
            required=False,
        ),
    }
    required_stale = [
        name
        for name in ("paper_readiness", "startup_control_plane", "supervised_paper_operability")
        if not bool(artifacts[name].get("fresh"))
    ]
    if temp_integrity and not bool(artifacts["temporary_paper_runtime_integrity"].get("fresh")):
        required_stale.append("temporary_paper_runtime_integrity")
    lane_rows = [
        row
        for row in list(paper_readiness.get("lane_eligibility_rows") or [])
        if isinstance(row, Mapping)
        and (row.get("lane_id") == lane.lane_id or row.get("strategy_id") in {lane.lane_id, lane.strategy_id})
    ]
    all_lane_rows = [row for row in list(paper_readiness.get("lane_eligibility_rows") or []) if isinstance(row, Mapping)]
    lane_row = dict(lane_rows[0]) if lane_rows else {}
    market_data_stale_count = _int_value(paper_readiness.get("market_data_stale_count"))
    selected_lane_stale = _lane_market_data_stale(lane_row)
    selected_lane_micro_stale = selected_lane_stale and _lane_market_data_micro_stale(lane_row, safety=safety)
    unrelated_stale_rows = [
        dict(row)
        for row in all_lane_rows
        if _lane_market_data_stale(row)
        and str(row.get("lane_id") or row.get("strategy_id") or "") not in {lane.lane_id, lane.strategy_id}
    ]
    selected_lane_data_fresh = bool(lane_row) and not selected_lane_stale and bool(lane_row.get("data_fresh", True))
    listener_running = bool(listener) and listener.get("live_money_eligible") is False
    supervisor_running = str(supervisor.get("classification") or "").upper().endswith("_RUNNING")
    blockers: list[str] = []
    warnings: list[str] = []
    classification = "LEAK_TEST_PRECHECK_READY"
    if required_stale:
        blockers.append("backend_readiness_artifact_stale")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    elif not lane_row:
        blockers.append("selected_lane_market_data_unavailable")
        classification = "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE"
    elif selected_lane_micro_stale and listener_running and supervisor_running:
        blockers.append("selected_lane_market_data_micro_stale_retryable")
        classification = "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE"
    elif selected_lane_stale:
        blockers.append("selected_lane_market_data_stale")
        classification = "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE"
    elif market_data_stale_count > 0 and unrelated_stale_rows:
        warnings.append("unrelated_market_data_stale")
    if not safety.broker_reconciled or safety.classification != RECONCILED_CLASSIFICATION:
        blockers.append("broker_lifecycle_not_reconciled")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    if safety.open_order_count:
        blockers.append("open_orders_present")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    if safety.review_required_count:
        blockers.append("review_required_nonzero")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    if safety.live_money_eligible:
        blockers.append("live_money_eligible_true")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    if safety.paper_proof_invoked:
        blockers.append("paper_proof_invoked_true")
        classification = "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY"
    return {
        "classification": classification,
        "ready": classification == "LEAK_TEST_PRECHECK_READY",
        "blockers": tuple(dict.fromkeys(blockers)),
        "warnings": tuple(dict.fromkeys(warnings)),
        "artifacts": artifacts,
        "market_data_scope": "SELECTED_LANE",
        "market_data_stale_count": market_data_stale_count,
        "selected_lane_market_data_fresh": selected_lane_data_fresh,
        "selected_lane_market_data_age_seconds": _lane_market_data_age_seconds(lane_row),
        "selected_lane_required_timeframe": _lane_required_timeframe(lane_row, lane),
        "unrelated_market_data_stale_count": len(unrelated_stale_rows),
        "unrelated_market_data_stale_lanes": tuple(_market_data_stale_summary(row) for row in unrelated_stale_rows),
        "bar_authority_unavailable_count": _int_value(paper_readiness.get("bar_authority_unavailable_count")),
        "blocking_fault_count": _int_value(paper_readiness.get("blocking_fault_count")),
        "runtime_running": bool(paper_readiness.get("runtime_running")),
        "paper_runtime_ready": bool(paper_readiness.get("paper_runtime_ready")),
        "paper_trade_allowed": bool(paper_readiness.get("paper_trade_allowed")),
        "startup_overall_state": startup.get("overall_state"),
        "supervised_paper_usable": bool(supervised.get("app_usable_for_supervised_paper")),
        "temp_paper_blocked": bool(temp_integrity.get("temp_paper_blocked")),
        "databento_listener_running": listener_running,
        "databento_supervisor_running": supervisor_running,
        "selected_lane_market_data": lane_row,
    }


def _bridge_config_for_apply(
    *,
    repo_root: Path,
    lane: LeakTestLanePlan,
    action: str,
    intent_type: str,
    reason: str,
    max_wait_seconds: float,
    safety: LeakTestSafetySnapshot,
    authorization_validation: Mapping[str, Any] | None,
    lifecycle_id: str | None = None,
) -> IbkrPaperStrategyBridgeConfig:
    auth_path = str((authorization_validation or {}).get("path") or "").strip()
    auth_digest = str((authorization_validation or {}).get("digest") or "").strip()
    return IbkrPaperStrategyBridgeConfig(
        repo_root=repo_root,
        mode="PAPER",
        host="127.0.0.1",
        port=7497,
        client_id=10940,
        account_id=PAPER_ACCOUNT_ID,
        strategy_id=lane.lane_id,
        symbol=lane.symbol,
        contract_month=str(lane.expiry or ""),
        action=action,
        quantity=1.0,
        order_type="LMT",
        limit_price_model=_limit_price_model_for_action(action),
        time_in_force="DAY",
        reason=reason,
        risk_tags=("TRACK_B_LEAK_TEST", intent_type),
        paper_only=True,
        submit=True,
        timeout_seconds=max_wait_seconds,
        daily_order_cap=1,
        caller_path="track_b_paper_leak_test_apply",
        output_dir=LEAK_TEST_OUTPUT_ROOT / lane.lane_id,
        leak_test_authorization_path=Path(auth_path) if auth_path else None,
        leak_test_authorization_digest=auth_digest or None,
        caller_metadata={
            "caller_type": "track_b_paper_leak_test",
            "lane_id": lane.lane_id,
            "strategy_id": lane.strategy_id,
            "route_destination": lane.expected_route,
            "intent_type": intent_type,
            "intent_action": action,
            "account_id": PAPER_ACCOUNT_ID,
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "con_id": lane.conId,
            "local_symbol": lane.localSymbol,
            "lifecycle_id": lifecycle_id,
            "runtime_pid": safety.runtime_pid,
            "runtime_cwd": safety.runtime_cwd,
            "leak_test": True,
            "live_money_eligible": False,
            "paper_only": True,
            "authorization_digest": auth_digest or None,
        },
    )


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _order_result_from_bridge(*, phase: str, route_result: Mapping[str, Any]) -> LeakTestOrderResult:
    report = dict(route_result.get("report") or {})
    raw_classification = str(route_result.get("classification") or report.get("classification") or "").strip()
    delegated = dict(report.get("delegated_result") or {})
    delegated_report = dict(delegated.get("report") or {})
    lifecycle = dict(
        delegated_report.get("submit_cancel_lifecycle")
        or delegated_report.get("lifecycle")
        or delegated.get("submit_cancel_lifecycle")
        or {}
    )
    pricing = dict(report.get("entry_execution_pricing") or delegated.get("entry_execution_pricing") or {})
    status = str(lifecycle.get("status") or "").strip().lower()
    if raw_classification == "PAPER_STRATEGY_ORDER_FILLED" or status in {"filled", "filled_flat", "passed"}:
        classification = "FILLED"
        terminal_status = "filled"
    elif raw_classification in {"PAPER_STRATEGY_ORDER_NOT_FILLED_CANCELLED", "PAPER_STRATEGY_ENTRY_MISSED_CANCELLED_ACCEPTED"}:
        classification = "NOT_FILLED_CANCELLED"
        terminal_status = "not_filled_cancelled"
    elif raw_classification == "PAPER_STRATEGY_ORDER_REJECTED":
        classification = "REJECTED"
        terminal_status = "rejected"
    elif raw_classification == "PAPER_STRATEGY_INTENT_BLOCKED":
        classification = "BLOCKED"
        terminal_status = "blocked"
    else:
        classification = "UNKNOWN"
        terminal_status = "unknown"
    return LeakTestOrderResult(
        phase=phase,
        classification=classification,
        terminal_status=terminal_status,
        order_id=str(
            lifecycle.get("order_id")
            or lifecycle.get("broker_order_id")
            or delegated_report.get("order_id")
            or delegated.get("broker_order_id")
            or ""
        )
        or None,
        client_id=_int_value(
            lifecycle.get("client_id")
            or delegated_report.get("client_id")
            or delegated.get("client_id")
            or _nested(report, "caller_metadata", "client_id")
        )
        or None,
        perm_id=_int_value(lifecycle.get("perm_id") or delegated_report.get("perm_id") or delegated.get("perm_id")) or None,
        fill_price=_float_or_none(
            lifecycle.get("fill_price")
            or lifecycle.get("filled_avg_price")
            or delegated_report.get("fill_price")
            or delegated.get("fill_price")
        ),
        fill_timestamp=str(
            lifecycle.get("fill_timestamp")
            or lifecycle.get("filled_at")
            or delegated_report.get("fill_timestamp")
            or delegated.get("fill_timestamp")
            or ""
        )
        or None,
        execution_price_source=str(pricing.get("execution_price_source") or "") or None,
        raw_classification=raw_classification or None,
        detail=str(report.get("detail") or delegated.get("detail") or lifecycle.get("detail") or "") or None,
        report=report,
    )


def _reconciliation_ok(reconciliation: Mapping[str, Any]) -> bool:
    return (
        str(reconciliation.get("classification") or "") == RECONCILED_CLASSIFICATION
        and reconciliation.get("broker_reconciled") is True
        and _int_value(reconciliation.get("review_required_count")) == 0
    )


def _lifecycle_open_matches_lane(reconciliation: Mapping[str, Any], lane: LeakTestLanePlan) -> bool:
    for row in _list_payload(reconciliation.get("track_b_lifecycle_positions")):
        if str(row.get("lane_id") or "") == lane.lane_id:
            return True
        if lane.lane_id in str(row.get("strategy_id") or ""):
            return True
    return False


def _lifecycle_id_for_lane(reconciliation: Mapping[str, Any], lane: LeakTestLanePlan) -> str | None:
    for row in _list_payload(reconciliation.get("track_b_lifecycle_positions")):
        if str(row.get("lane_id") or "") == lane.lane_id or lane.lane_id in str(row.get("strategy_id") or ""):
            lifecycle_id = str(row.get("lifecycle_id") or "").strip()
            if lifecycle_id:
                return lifecycle_id
    return None


def _reconciliation_is_flat(reconciliation: Mapping[str, Any]) -> bool:
    return (
        _reconciliation_ok(reconciliation)
        and _int_value(reconciliation.get("track_b_broker_position_count")) == 0
        and _int_value(reconciliation.get("lifecycle_open_position_count")) == 0
        and _int_value(reconciliation.get("track_b_broker_open_order_count"))
        + _int_value(reconciliation.get("lifecycle_open_order_count"))
        == 0
    )


def _wait_for_reconciliation(
    *,
    repo_root: Path,
    stage: str,
    max_wait_seconds: float,
    reader: Callable[[Path, str], dict[str, Any]],
    predicate: Callable[[Mapping[str, Any]], bool],
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.0, max_wait_seconds)
    latest = reader(repo_root, stage)
    while time.monotonic() <= deadline:
        latest = reader(repo_root, stage)
        if predicate(latest):
            return latest
        if max_wait_seconds <= 0:
            return latest
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return latest


def _portfolio_status(repo_root: Path) -> str | None:
    payload = _read_json(repo_root / PORTFOLIO_STATE_PATH)
    if not payload:
        return None
    return str((payload.get("portfolio_summary") or {}).get("status") or payload.get("status") or "") or None


def _realized_pnl_estimate(*, exit_result: LeakTestOrderResult | None, portfolio_status: str | None) -> float | None:
    del portfolio_status
    if exit_result is None:
        return None
    value = _nested(exit_result.report, "delegated_result", "realized_pnl")
    if value is None:
        value = _nested(exit_result.report, "delegated_result", "report", "realized_pnl")
    return _float_or_none(value)


def _empty_order_result(phase: str, classification: str, detail: str | None = None) -> LeakTestOrderResult:
    return LeakTestOrderResult(
        phase=phase,
        classification=classification,
        terminal_status=classification.lower(),
        order_id=None,
        client_id=None,
        perm_id=None,
        fill_price=None,
        fill_timestamp=None,
        execution_price_source=None,
        raw_classification=None,
        detail=detail,
        report={},
    )


def build_single_lane_apply_report(
    *,
    repo_root: Path,
    lane_id: str,
    reconciliation: Mapping[str, Any] | None = None,
    operator_status: Mapping[str, Any] | None = None,
    active_leak_test: Mapping[str, Any] | None = None,
    runtime_command: str | None = None,
    exposure_policy: LeakTestExposurePolicy | None = None,
    dry_run: bool = False,
    precheck_only: bool = False,
    max_wait_seconds: float = 90.0,
    force_exit_after_entry: bool = True,
    authorization_path: Path | None = None,
    guarded_route_runner: Callable[[IbkrPaperStrategyBridgeConfig], dict[str, Any]] = _guarded_bridge_route,
    reconciliation_reader: Callable[[Path, str], dict[str, Any]] = _default_reconciliation_reader,
    readiness_checker: Callable[[Path, LeakTestLanePlan, LeakTestSafetySnapshot], dict[str, Any]] | None = None,
) -> LeakTestReport:
    dry_run_report = build_single_lane_dry_run_report(
        repo_root=repo_root,
        lane_id=lane_id,
        reconciliation=reconciliation,
        operator_status=operator_status,
        active_leak_test=active_leak_test,
        runtime_command=runtime_command,
        exposure_policy=exposure_policy,
    )
    if not dry_run_report.lanes:
        classification = "LEAK_TEST_LANE_NOT_FOUND"
        apply_result = None
    elif not dry_run_report.lanes[0].safe_to_test:
        classification = "LEAK_TEST_PASS_BLOCKED_SAFELY"
        lane = dry_run_report.lanes[0]
        apply_result = LeakTestApplyResult(
            lane_id=lane.lane_id,
            strategy_id=lane.strategy_id,
            symbol=lane.symbol,
            localSymbol=lane.localSymbol,
            expiry=lane.expiry,
            conId=lane.conId,
            entry_execution_intent=lane.entry_execution_intent,
            entry_execution_intent_source="inferred" if lane.entry_execution_intent.endswith("_INFERRED") else "explicit",
            pre_apply_blockers=lane.isolated_blockers,
            authorization_status=None,
            authorization_path=str(authorization_path) if authorization_path is not None else None,
            pre_apply_readiness=None,
            dry_run=False,
            entry=None,
            lifecycle_open_result=None,
            reconciliation_after_entry=None,
            exit_policy=None,
            exit_reason=None,
            exit=None,
            lifecycle_close_result=None,
            reconciliation_after_exit=None,
            realized_pnl_estimate=None,
            portfolio_artifact_status=None,
            runtime_pid=dry_run_report.safety.runtime_pid,
            runtime_cwd=dry_run_report.safety.runtime_cwd,
            live_money_eligible=False,
            mutation_performed=False,
        )
    elif dry_run:
        lane = dry_run_report.lanes[0]
        classification = "LEAK_TEST_DRY_RUN_READY"
        apply_result = LeakTestApplyResult(
            lane_id=lane.lane_id,
            strategy_id=lane.strategy_id,
            symbol=lane.symbol,
            localSymbol=lane.localSymbol,
            expiry=lane.expiry,
            conId=lane.conId,
            entry_execution_intent=lane.entry_execution_intent,
            entry_execution_intent_source="inferred" if lane.entry_execution_intent.endswith("_INFERRED") else "explicit",
            pre_apply_blockers=(),
            authorization_status=None,
            authorization_path=str(authorization_path) if authorization_path is not None else None,
            pre_apply_readiness=None,
            dry_run=True,
            entry=_empty_order_result("entry", "DRY_RUN", "Dry-run did not invoke the guarded PAPER route."),
            lifecycle_open_result=None,
            reconciliation_after_entry=None,
            exit_policy=None,
            exit_reason=None,
            exit=None,
            lifecycle_close_result=None,
            reconciliation_after_exit=None,
            realized_pnl_estimate=None,
            portfolio_artifact_status=None,
            runtime_pid=dry_run_report.safety.runtime_pid,
            runtime_cwd=dry_run_report.safety.runtime_cwd,
            live_money_eligible=False,
            mutation_performed=False,
        )
    else:
        lane = dry_run_report.lanes[0]
        entry_action = _bridge_action_for_lane(lane)
        entry_intent_type = _intent_type_for_action(entry_action, close=False)
        authorization_validation = validate_leak_test_authorization(
            authorization_path=authorization_path,
            repo_root=repo_root,
            lane=lane,
            action=entry_action,
        )
        pre_apply_readiness: dict[str, Any] | None = None
        if not bool(authorization_validation.get("valid")):
            classification = str(authorization_validation.get("classification") or "LEAK_TEST_AUTHORIZATION_MISSING")
            entry_result = _empty_order_result("entry", "BLOCKED", str(authorization_validation.get("detail") or classification))
            mutation_performed = False
            lifecycle_open_result = None
            reconciliation_after_entry = None
            exit_result = None
            lifecycle_close_result = None
            reconciliation_after_exit = reconciliation_reader(repo_root, "authorization_blocked")
            exit_policy = None
            exit_reason = None
        else:
            checker = readiness_checker or (
                lambda root, readiness_lane, readiness_safety: _pre_apply_readiness_check(
                    repo_root=root,
                    lane=readiness_lane,
                    safety=readiness_safety,
                )
            )
            pre_apply_readiness = checker(repo_root, lane, dry_run_report.safety)
            if not bool(pre_apply_readiness.get("ready")):
                classification = str(pre_apply_readiness.get("classification") or "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY")
                entry_result = _empty_order_result("entry", "BLOCKED", f"Pre-apply readiness blocked: {classification}.")
                mutation_performed = False
                lifecycle_open_result = None
                reconciliation_after_entry = None
                exit_result = None
                lifecycle_close_result = None
                reconciliation_after_exit = reconciliation_reader(repo_root, "pre_apply_readiness_blocked")
                exit_policy = None
                exit_reason = None
            elif precheck_only:
                classification = "LEAK_TEST_PRECHECK_READY"
                entry_result = _empty_order_result("entry", "PRECHECK_ONLY", "Precheck-only did not invoke the guarded PAPER route.")
                mutation_performed = False
                lifecycle_open_result = None
                reconciliation_after_entry = None
                exit_result = None
                lifecycle_close_result = None
                reconciliation_after_exit = reconciliation_reader(repo_root, "precheck_only")
                exit_policy = None
                exit_reason = None
            else:
                entry_config = _bridge_config_for_apply(
                    repo_root=repo_root,
                    lane=lane,
                    action=entry_action,
                    intent_type=entry_intent_type,
                    reason="LEAK_TEST_ENTRY",
                    max_wait_seconds=max_wait_seconds,
                    safety=dry_run_report.safety,
                    authorization_validation=authorization_validation,
                )
                entry_result = _order_result_from_bridge(
                    phase="entry",
                    route_result=guarded_route_runner(entry_config),
                )
                mutation_performed = True
                lifecycle_open_result = None
                reconciliation_after_entry = None
                exit_result = None
                lifecycle_close_result = None
                reconciliation_after_exit = None
                exit_policy = None
                exit_reason = None
        if not mutation_performed and classification in {
            "LEAK_TEST_AUTHORIZATION_MISSING",
            "LEAK_TEST_AUTHORIZATION_EXPIRED",
            "LEAK_TEST_AUTHORIZATION_DIGEST_MISMATCH",
            "LEAK_TEST_AUTHORIZATION_IDENTITY_MISMATCH",
            "LEAK_TEST_PRECHECK_GOVERNANCE_NOT_READY",
            "LEAK_TEST_PRECHECK_MARKET_DATA_STALE",
            "LEAK_TEST_PRECHECK_SELECTED_LANE_MARKET_DATA_STALE",
            "LEAK_TEST_MARKET_DATA_MICRO_STALE_RETRYABLE",
            "LEAK_TEST_PRECHECK_READY",
        }:
            pass
        elif entry_result.classification == "BLOCKED":
            classification = "LEAK_TEST_PASS_BLOCKED_SAFELY"
            reconciliation_after_exit = reconciliation_reader(repo_root, "entry_blocked")
        elif entry_result.classification == "REJECTED":
            classification = "LEAK_TEST_ENTRY_REJECTED"
            reconciliation_after_exit = reconciliation_reader(repo_root, "entry_rejected")
        elif entry_result.classification == "NOT_FILLED_CANCELLED":
            classification = "LEAK_TEST_ENTRY_NOT_FILLED_CANCELLED"
            reconciliation_after_exit = reconciliation_reader(repo_root, "entry_not_filled_cancelled")
        elif entry_result.classification != "FILLED":
            classification = "LEAK_TEST_PASS_BLOCKED_SAFELY"
            reconciliation_after_exit = reconciliation_reader(repo_root, "entry_unknown")
        else:
            reconciliation_after_entry = _wait_for_reconciliation(
                repo_root=repo_root,
                stage="after_entry",
                max_wait_seconds=max_wait_seconds,
                reader=reconciliation_reader,
                predicate=lambda payload: _reconciliation_ok(payload) and _lifecycle_open_matches_lane(payload, lane),
            )
            lifecycle_open_result = (
                "LIFECYCLE_OPEN_MATCHED"
                if _reconciliation_ok(reconciliation_after_entry) and _lifecycle_open_matches_lane(reconciliation_after_entry, lane)
                else "LIFECYCLE_OPEN_GAP"
            )
            if lifecycle_open_result != "LIFECYCLE_OPEN_MATCHED":
                classification = "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP"
            elif not force_exit_after_entry:
                classification = "LEAK_TEST_PASS_CONCURRENT_OPEN"
            else:
                lifecycle_id = _lifecycle_id_for_lane(reconciliation_after_entry, lane)
                exit_action = _close_action_for_entry(entry_action)
                exit_intent_type = _intent_type_for_action(exit_action, close=True)
                exit_policy = "GUARDED_PAPER_CLOSE"
                exit_reason = "LEAK_TEST_CONTROLLED_EXIT"
                exit_config = _bridge_config_for_apply(
                    repo_root=repo_root,
                    lane=lane,
                    action=exit_action,
                    intent_type=exit_intent_type,
                    reason=exit_reason,
                    max_wait_seconds=max_wait_seconds,
                    safety=dry_run_report.safety,
                    authorization_validation=authorization_validation,
                    lifecycle_id=lifecycle_id,
                )
                exit_result = _order_result_from_bridge(
                    phase="exit",
                    route_result=guarded_route_runner(exit_config),
                )
                if exit_result.classification == "REJECTED":
                    classification = "LEAK_TEST_EXIT_REJECTED"
                    reconciliation_after_exit = reconciliation_reader(repo_root, "exit_rejected")
                elif exit_result.classification in {"BLOCKED", "NOT_FILLED_CANCELLED"}:
                    classification = "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED"
                    reconciliation_after_exit = reconciliation_reader(repo_root, "exit_not_filled_cancelled")
                elif exit_result.classification != "FILLED":
                    classification = "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED"
                    reconciliation_after_exit = reconciliation_reader(repo_root, "exit_unknown")
                else:
                    reconciliation_after_exit = _wait_for_reconciliation(
                        repo_root=repo_root,
                        stage="after_exit",
                        max_wait_seconds=max_wait_seconds,
                        reader=reconciliation_reader,
                        predicate=_reconciliation_is_flat,
                    )
                    lifecycle_close_result = (
                        "LIFECYCLE_CLOSED_FLAT"
                        if _reconciliation_is_flat(reconciliation_after_exit)
                        else "LIFECYCLE_CLOSE_GAP"
                    )
                    classification = (
                        "LEAK_TEST_PASS_FULL_ROUND_TRIP"
                        if lifecycle_close_result == "LIFECYCLE_CLOSED_FLAT"
                        else "LEAK_TEST_EXIT_FILL_LIFECYCLE_GAP"
                    )
        if reconciliation_after_exit is not None and not _reconciliation_ok(reconciliation_after_exit):
            if _int_value(reconciliation_after_exit.get("review_required_count")):
                classification = "LEAK_TEST_REVIEW_REQUIRED"
            elif classification == "LEAK_TEST_PASS_FULL_ROUND_TRIP":
                classification = "LEAK_TEST_BROKER_LIFECYCLE_MISMATCH"
        portfolio_status = _portfolio_status(repo_root)
        apply_result = LeakTestApplyResult(
            lane_id=lane.lane_id,
            strategy_id=lane.strategy_id,
            symbol=lane.symbol,
            localSymbol=lane.localSymbol,
            expiry=lane.expiry,
            conId=lane.conId,
            entry_execution_intent=lane.entry_execution_intent,
            entry_execution_intent_source="inferred" if lane.entry_execution_intent.endswith("_INFERRED") else "explicit",
            pre_apply_blockers=(),
            authorization_status=str(authorization_validation.get("classification") or ""),
            authorization_path=str(authorization_path) if authorization_path is not None else None,
            pre_apply_readiness=pre_apply_readiness,
            dry_run=False,
            entry=entry_result,
            lifecycle_open_result=lifecycle_open_result,
            reconciliation_after_entry=reconciliation_after_entry,
            exit_policy=exit_policy,
            exit_reason=exit_reason,
            exit=exit_result,
            lifecycle_close_result=lifecycle_close_result,
            reconciliation_after_exit=reconciliation_after_exit,
            realized_pnl_estimate=_realized_pnl_estimate(exit_result=exit_result, portfolio_status=portfolio_status),
            portfolio_artifact_status=portfolio_status,
            runtime_pid=dry_run_report.safety.runtime_pid,
            runtime_cwd=dry_run_report.safety.runtime_cwd,
            live_money_eligible=False,
            mutation_performed=mutation_performed,
        )
    return LeakTestReport(
        mode="single-lane-apply",
        generated_at=dry_run_report.generated_at,
        account_id=dry_run_report.account_id,
        live_money_eligible=False,
        mutation_performed=bool(apply_result and apply_result.mutation_performed),
        safety=dry_run_report.safety,
        exposure_policy=dry_run_report.exposure_policy,
        lanes=dry_run_report.lanes,
        concurrent_scenarios=dry_run_report.concurrent_scenarios,
        recommended_first_isolated_sequence=dry_run_report.recommended_first_isolated_sequence,
        recommended_first_concurrent_scenario_id=dry_run_report.recommended_first_concurrent_scenario_id,
        apply_result=apply_result,
        authorization_artifact=None,
        result_classification=classification,
        notes=(
            "Single-lane apply uses only the existing guarded PAPER bridge route.",
            "Dry-run mode performs no broker mutation.",
        ),
    )


def report_to_dict(report: LeakTestReport) -> dict[str, Any]:
    payload = asdict(report)
    payload["lanes"] = [asdict(lane) for lane in report.lanes]
    payload["safety"] = asdict(report.safety)
    return payload


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track B PAPER leak-test planning harness.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--mode",
        choices=("plan-only", "concurrent-plan", "single-lane-dry-run", "single-lane-apply"),
        default="plan-only",
    )
    parser.add_argument("--lane-id", default="")
    parser.add_argument("--max-wait-seconds", type=float, default=90.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--precheck-only", action="store_true")
    parser.add_argument("--write-authorization", action="store_true")
    parser.add_argument("--authorization-path", type=Path, default=None)
    parser.add_argument("--authorization-ttl-seconds", type=float, default=600.0)
    parser.add_argument("--force-exit-after-entry", action="store_true", default=True)
    parser.add_argument("--no-force-exit-after-entry", dest="force_exit_after_entry", action="store_false")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    if args.mode == "plan-only":
        report = build_plan_only_report(repo_root=repo_root)
    elif args.mode == "concurrent-plan":
        report = build_concurrent_plan_report(repo_root=repo_root)
    elif args.mode == "single-lane-dry-run":
        report = build_single_lane_dry_run_report(
            repo_root=repo_root,
            lane_id=str(args.lane_id),
            write_authorization=bool(args.write_authorization),
            authorization_ttl_seconds=float(args.authorization_ttl_seconds),
            authorization_output_path=args.authorization_path,
        )
    else:
        report = build_single_lane_apply_report(
            repo_root=repo_root,
            lane_id=str(args.lane_id),
            dry_run=bool(args.dry_run),
            precheck_only=bool(args.precheck_only),
            max_wait_seconds=float(args.max_wait_seconds),
            force_exit_after_entry=bool(args.force_exit_after_entry),
            authorization_path=args.authorization_path,
        )
    print(json.dumps(report_to_dict(report), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
