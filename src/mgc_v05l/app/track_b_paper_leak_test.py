"""Read-only Track B PAPER leak-test planning harness."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..config_models import load_settings_from_files
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
RESULT_CLASSIFICATIONS = (
    "LEAK_TEST_PASS_FULL_ROUND_TRIP",
    "LEAK_TEST_PASS_BLOCKED_SAFELY",
    "LEAK_TEST_ENTRY_NOT_FILLED_CANCELLED",
    "LEAK_TEST_ENTRY_FILL_LIFECYCLE_GAP",
    "LEAK_TEST_EXIT_NOT_FILLED_CANCELLED",
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
)


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
    result_classification: str
    notes: tuple[str, ...]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _int_value(value: object) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


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
        active_leak_test_lane_id=str(active_payload.get("lane_id") or "").strip() or None,
        existing_positions=_existing_positions_from_reconciliation(reconciliation_payload),
        existing_open_orders=_existing_open_orders_from_reconciliation(reconciliation_payload),
    )


def _unresolved_state_blockers(safety: LeakTestSafetySnapshot) -> list[str]:
    blockers: list[str] = []
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
    unresolved_blockers = _unresolved_state_blockers(safety)
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
        result_classification="LEAK_TEST_PLAN_ONLY",
        notes=(
            "Plan-only mode performs no broker mutation.",
            "Apply mode is intentionally not implemented in this first deliverable.",
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
        result_classification=classification if lanes else "LEAK_TEST_LANE_NOT_FOUND",
        notes=("Dry-run validates preconditions and does not call the broker route.",),
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
) -> LeakTestReport:
    dry_run = build_single_lane_dry_run_report(
        repo_root=repo_root,
        lane_id=lane_id,
        reconciliation=reconciliation,
        operator_status=operator_status,
        active_leak_test=active_leak_test,
        runtime_command=runtime_command,
        exposure_policy=exposure_policy,
    )
    if not dry_run.lanes:
        classification = "LEAK_TEST_LANE_NOT_FOUND"
    elif not dry_run.lanes[0].safe_to_test:
        classification = "LEAK_TEST_PASS_BLOCKED_SAFELY"
    else:
        classification = "LEAK_TEST_APPLY_REQUIRES_EXPLICIT_APPROVAL_NOT_IMPLEMENTED"
    return LeakTestReport(
        mode="single-lane-apply",
        generated_at=dry_run.generated_at,
        account_id=dry_run.account_id,
        live_money_eligible=False,
        mutation_performed=False,
        safety=dry_run.safety,
        exposure_policy=dry_run.exposure_policy,
        lanes=dry_run.lanes,
        concurrent_scenarios=dry_run.concurrent_scenarios,
        recommended_first_isolated_sequence=dry_run.recommended_first_isolated_sequence,
        recommended_first_concurrent_scenario_id=dry_run.recommended_first_concurrent_scenario_id,
        result_classification=classification,
        notes=("No broker mutation is implemented in this first deliverable.",),
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    if args.mode == "plan-only":
        report = build_plan_only_report(repo_root=repo_root)
    elif args.mode == "concurrent-plan":
        report = build_concurrent_plan_report(repo_root=repo_root)
    elif args.mode == "single-lane-dry-run":
        report = build_single_lane_dry_run_report(repo_root=repo_root, lane_id=str(args.lane_id))
    else:
        report = build_single_lane_apply_report(repo_root=repo_root, lane_id=str(args.lane_id))
    print(json.dumps(report_to_dict(report), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
