#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="/Users/patrick/Dev/MGC-v05l-automation"
OUT_DIR="${REPO_ROOT}/outputs/reports/track_b_paper_preflight"
OUT_JSON="${OUT_DIR}/latest_track_b_paper_preflight.json"
REQUIRED_PATCH_COMMITS="d7b126c10d 35064b698c 94695132b1 520bd7313f 57402b55a7 155cea2bfa a47a6f6806 83ed3578c2"

usage() {
  echo "Usage: $0 --mode weekend-static|monday-live" >&2
}

MODE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --mode=*)
      MODE="${1#--mode=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if [[ "${MODE}" != "weekend-static" && "${MODE}" != "monday-live" ]]; then
  usage
  exit 2
fi

if [[ ! -d "${REPO_ROOT}" ]]; then
  echo "Repo root missing: ${REPO_ROOT}" >&2
  exit 2
fi

mkdir -p "${OUT_DIR}"
cd "${REPO_ROOT}" || exit 2

PREFLIGHT_MODE="${MODE}" \
PREFLIGHT_REPO_ROOT="${REPO_ROOT}" \
PREFLIGHT_OUT_JSON="${OUT_JSON}" \
PREFLIGHT_REQUIRED_PATCH_COMMITS="${REQUIRED_PATCH_COMMITS}" \
./.venv/bin/python - <<'PY'
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MODE = os.environ["PREFLIGHT_MODE"]
REPO_ROOT = Path(os.environ["PREFLIGHT_REPO_ROOT"])
OUT_JSON = Path(os.environ["PREFLIGHT_OUT_JSON"])
REQUIRED_PATCH_COMMITS = tuple(
    commit.strip()
    for commit in os.environ["PREFLIGHT_REQUIRED_PATCH_COMMITS"].split()
    if commit.strip()
)
OLD_ROOT_PATTERNS = (
    "/Users/patrick/Documents/MGC-v05l-automation",
    "Mobile Documents",
    "iCloud",
)
TRADING_PROCESS_PATTERNS = (
    "probationary-paper-soak",
    "paper_strategy_monitor",
    "run_probationary_paper_soak",
    "run_supervised_paper",
    "headless_supervised_paper",
)
MUTATING_TERMS = ("placeOrder", "paper_proof")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], timeout: int = 30) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return {
            "cmd": cmd,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:  # pragma: no cover - defensive for operator script
        return {"cmd": cmd, "returncode": 999, "stdout": "", "stderr": str(exc)}


def read_json(path: Path) -> tuple[dict[str, Any] | list[Any] | None, str | None]:
    try:
        return json.loads(path.read_text()), None
    except FileNotFoundError:
        return None, f"missing: {path}"
    except Exception as exc:
        return None, f"{path}: {exc}"


def parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def current_age_seconds(value: Any) -> float | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    return max((datetime.now(timezone.utc) - parsed).total_seconds(), 0.0)


def decimalish(value: Any) -> str:
    text = str(value if value is not None else "0").strip()
    try:
        from decimal import Decimal

        return str(Decimal(text).normalize())
    except Exception:
        return text


def position_root(row: dict[str, Any]) -> str:
    return str(row.get("track_b_root") or row.get("symbol") or row.get("contract_symbol") or "").upper()


def position_quantity(row: dict[str, Any]) -> str:
    return decimalish(row.get("quantity") if row.get("quantity") is not None else row.get("position"))


def infer_lane_symbol(row: dict[str, Any]) -> str:
    direct = str(row.get("symbol") or row.get("instrument") or "").strip().upper()
    if direct:
        return direct
    lane_id = str(row.get("lane_id") or row.get("strategy_id") or "").strip().lower()
    for candidate in ("mnq", "mgc", "gc", "pl", "nq", "es", "mes", "zt", "zf", "zn", "zb"):
        if lane_id == candidate or lane_id.startswith(f"{candidate}_") or f"_{candidate}_" in lane_id:
            return candidate.upper()
    return ""


checks: list[dict[str, Any]] = []
warnings: list[str] = []
blocking: list[str] = []


def add(name: str, passed: bool, fail_blocks: bool, detail: str, **extra: Any) -> None:
    status = "PASS" if passed else ("FAIL" if fail_blocks else "WARN")
    item = {"name": name, "status": status, "detail": detail}
    item.update(extra)
    checks.append(item)
    if not passed:
        if fail_blocks:
            blocking.append(f"{name}: {detail}")
        else:
            warnings.append(f"{name}: {detail}")


branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
branch_name = branch["stdout"]
add(
    "branch_is_track_b_paper_execution_core",
    branch["returncode"] == 0 and branch_name == "track-b-paper-execution-core",
    True,
    branch_name or branch["stderr"],
)

for required_commit in REQUIRED_PATCH_COMMITS:
    commit = run(["git", "merge-base", "--is-ancestor", required_commit, "HEAD"])
    add(
        f"required_patch_commit_present_{required_commit}",
        commit["returncode"] == 0,
        True,
        f"{required_commit} is ancestor of HEAD" if commit["returncode"] == 0 else commit["stderr"],
    )

ps = run(["ps", "-ef"])
old_process_hits: list[str] = []
if ps["returncode"] == 0:
    for line in ps["stdout"].splitlines():
        if any(old in line for old in OLD_ROOT_PATTERNS) and any(
            marker in line for marker in TRADING_PROCESS_PATTERNS
        ):
            old_process_hits.append(line)
add(
    "no_old_root_trading_processes",
    not old_process_hits,
    True,
    "no submit-capable old-root process found" if not old_process_hits else "\n".join(old_process_hits),
    hits=old_process_hits,
)

scan_paths = [
    "config",
    "scripts",
    "src",
    "launchd",
    str(Path.home() / "Library" / "LaunchAgents"),
]
rg = run(
    [
        "rg",
        "-n",
        "/Users/patrick/Documents/MGC-v05l-automation|Mobile Documents|iCloud",
        *scan_paths,
    ],
    timeout=45,
)
submit_capable_old_root_hits: list[str] = []
if rg["returncode"] in (0, 1):
    for line in rg["stdout"].splitlines():
        lowered = line.lower()
        if any(term.lower() in lowered for term in ("paper", "submit", "strategy_monitor", "probationary")):
            submit_capable_old_root_hits.append(line)
add(
    "no_submit_capable_old_root_config_or_launchd",
    not submit_capable_old_root_hits,
    True,
    "no submit-capable old-root config/launchd references found"
    if not submit_capable_old_root_hits
    else "\n".join(submit_capable_old_root_hits[:20]),
    hit_count=len(submit_capable_old_root_hits),
)

runtime_config_hits: list[str] = []
runtime_files = [
    REPO_ROOT / "outputs/probationary_pattern_engine/paper_session/operator_status.json",
    REPO_ROOT / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid",
]
operator_status, operator_err = read_json(runtime_files[0])
if isinstance(operator_status, dict):
    runtime_config_hits.append(str(REPO_ROOT))
    source_pid = operator_status.get("source_runtime_pid")
    if source_pid:
        proc = run(["ps", "-p", str(source_pid), "-o", "command="])
        if proc["returncode"] == 0:
            runtime_config_hits.append(proc["stdout"])
configs_point_dev = all(
    not any(old in hit for old in OLD_ROOT_PATTERNS) and str(REPO_ROOT) in hit
    for hit in runtime_config_hits
) and bool(runtime_config_hits)
add(
    "runtime_package_configs_point_to_dev_root",
    configs_point_dev,
    True,
    "runtime/package references point to Dev root"
    if configs_point_dev
    else (operator_err or "runtime Dev-root evidence missing/stale"),
    evidence=runtime_config_hits,
)

compile_cmd = [
    "./.venv/bin/python",
    "-m",
    "compileall",
    "src/mgc_v05l/app/probationary_runtime.py",
    "src/mgc_v05l/strategy/strategy_engine.py",
    "src/mgc_v05l/monitoring/logger.py",
]
compile_result = run(compile_cmd, timeout=60)
add(
    "compileall_patched_modules",
    compile_result["returncode"] == 0,
    True,
    compile_result["stdout"] or compile_result["stderr"],
)

pytest_cmd = [
    "./.venv/bin/python",
    "-m",
    "pytest",
    "tests/unit/test_mgc_v05l_probationary_runtime.py",
    "-k",
    "filled_bridge or bridge_block_does_not_create_local_fill or submit_capable_lane_entry_invokes_ibkr_bridge",
]
pytest_result = run(pytest_cmd, timeout=120)
add(
    "filled_result_invariant_tests",
    pytest_result["returncode"] == 0,
    True,
    pytest_result["stdout"].splitlines()[-1] if pytest_result["stdout"] else pytest_result["stderr"],
)

proof_scan = run(
    [
        "rg",
        "-n",
        '"paper_proof_invoked"\\s*:\\s*true|paper_proof_invoked=true|paper_proof.*placeOrder',
        "outputs/probationary_pattern_engine",
        "outputs/operator_dashboard",
        "outputs/reports",
    ],
    timeout=45,
)
proof_hits = proof_scan["stdout"].splitlines() if proof_scan["returncode"] == 0 else []
add(
    "paper_proof_not_invoked",
    not proof_hits,
    True,
    "no paper_proof invocation evidence" if not proof_hits else "\n".join(proof_hits[:20]),
    hit_count=len(proof_hits),
)

expected_phase1_symbols = ("GC", "NQ", "ES", "MGC", "MNQ", "MES", "ZT", "ZF", "ZN", "ZB", "PL")
try:
    from mgc_v05l.app.phase1_ticker_readiness_matrix import (
        Phase1TickerReadinessMatrixConfig,
        build_phase1_ticker_readiness_matrix,
    )

    matrix_artifacts = build_phase1_ticker_readiness_matrix(
        config=Phase1TickerReadinessMatrixConfig(repo_root=REPO_ROOT)
    )
    matrix_rows = list(matrix_artifacts.rows)
    matrix_symbols = tuple(str(row.get("approved_phase1_symbol") or "") for row in matrix_rows)
    matrix_live_money_false = all(row.get("live_money_eligible") is False for row in matrix_rows)
    allowed_guarded_candidate_submit_rows = [
        row
        for row in matrix_rows
        if row.get("can_submit") is True
        and str(row.get("approved_phase1_symbol") or "") == "GC"
        and str(row.get("paper_candidate_strategy_id") or "")
        == "gc_1x_asia_london_participation__asia_london_long_v5"
        and row.get("paper_watch_ready") is True
        and row.get("guarded_route_authorized") is True
        and row.get("strategy_approved") is True
        and row.get("live_money_eligible") is False
    ]
    unauthorized_can_submit_rows = [
        row
        for row in matrix_rows
        if row.get("can_submit") is True and row not in allowed_guarded_candidate_submit_rows
    ]
    add(
        "phase1_ticker_readiness_matrix_static",
        len(matrix_rows) == 11
        and matrix_symbols == expected_phase1_symbols
        and matrix_live_money_false
        and not unauthorized_can_submit_rows,
        True,
        (
            f"row_count={len(matrix_rows)}; can_submit_count={matrix_artifacts.report.get('can_submit_count')}; "
            f"authorized_gc_candidate_can_submit_count={len(allowed_guarded_candidate_submit_rows)}; "
            f"unauthorized_can_submit_count={len(unauthorized_can_submit_rows)}; "
            f"symbols={list(matrix_symbols)}; live_money_eligible_false={matrix_live_money_false}"
        ),
        row_count=len(matrix_rows),
        can_submit_count=matrix_artifacts.report.get("can_submit_count"),
        authorized_gc_candidate_can_submit_count=len(allowed_guarded_candidate_submit_rows),
        unauthorized_can_submit_count=len(unauthorized_can_submit_rows),
        symbols=list(matrix_symbols),
    )
except Exception as exc:
    add(
        "phase1_ticker_readiness_matrix_static",
        False,
        True,
        f"phase-1 ticker readiness matrix check failed: {exc}",
    )
    matrix_rows = []

try:
    from mgc_v05l.execution_core.phase1_gc_paper_candidate import (
        CHOSEN_GC_STRATEGY_ID,
        Phase1GcCandidateConfig,
        build_phase1_gc_paper_candidate,
    )

    gc_candidate_artifacts = build_phase1_gc_paper_candidate(
        config=Phase1GcCandidateConfig(
            repo_root=REPO_ROOT,
            write_report=False,
            max_bars=240,
            guarded_route_authorized=False,
        )
    )
    gc_candidate_report = dict(gc_candidate_artifacts.report)
    gc_candidate_eval = dict(gc_candidate_artifacts.evaluation)
    gc_candidate_realtime_condition = (
        gc_candidate_report.get("realtime_feed_confirmed") is not True
        if MODE == "weekend-static"
        else True
    )
    gc_candidate_passed = (
        gc_candidate_report.get("chosen_strategy") == CHOSEN_GC_STRATEGY_ID
        and gc_candidate_eval.get("candidate_evaluation_ready") is True
        and gc_candidate_report.get("paper_candidate_approved") is True
        and gc_candidate_report.get("paper_watch_ready") is False
        and gc_candidate_report.get("can_submit") is False
        and gc_candidate_report.get("live_money_eligible") is False
        and gc_candidate_realtime_condition
    )
    add(
        "gc_phase1_paper_candidate_visible_no_submit",
        gc_candidate_passed,
        True,
        (
            f"strategy={gc_candidate_report.get('chosen_strategy')}; "
            f"candidate_evaluation_ready={gc_candidate_eval.get('candidate_evaluation_ready')}; "
            f"paper_candidate_approved={gc_candidate_report.get('paper_candidate_approved')}; "
            f"paper_watch_ready={gc_candidate_report.get('paper_watch_ready')}; "
            f"can_submit={gc_candidate_report.get('can_submit')}; "
            f"live_money_eligible={gc_candidate_report.get('live_money_eligible')}; "
            f"realtime_feed_confirmed={gc_candidate_report.get('realtime_feed_confirmed')}"
        ),
        strategy_id=gc_candidate_report.get("chosen_strategy"),
        paper_watch_ready=gc_candidate_report.get("paper_watch_ready"),
        can_submit=gc_candidate_report.get("can_submit"),
    )
except Exception as exc:
    add(
        "gc_phase1_paper_candidate_visible_no_submit",
        False,
        True,
        f"GC Phase-1 paper candidate visibility check failed: {exc}",
    )

try:
    from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
        Phase1RuntimeDataReadinessConfig,
        build_phase1_runtime_data_readiness,
    )

    runtime_data_artifacts = build_phase1_runtime_data_readiness(
        config=Phase1RuntimeDataReadinessConfig(repo_root=REPO_ROOT)
    )
    runtime_data_rows = list(runtime_data_artifacts.rows)
    runtime_data_symbols = tuple(str(row.get("symbol") or "") for row in runtime_data_rows)
    runtime_data_symbol_check = (
        len(runtime_data_rows) == len(expected_phase1_symbols)
        and runtime_data_symbols == expected_phase1_symbols
        and int(runtime_data_artifacts.report.get("ready_ticker_count", -1)) >= 0
        and runtime_data_artifacts.report.get("research_artifact_used") is False
        and runtime_data_artifacts.report.get("archive_artifact_used") is False
    )
    add(
        "phase1_runtime_data_readiness_static",
        runtime_data_symbol_check,
        True,
        (
            f"row_count={len(runtime_data_rows)}; "
            f"ready_ticker_count={runtime_data_artifacts.report.get('ready_ticker_count')}; "
            f"symbols={list(runtime_data_symbols)}; "
            f"research_artifact_used={runtime_data_artifacts.report.get('research_artifact_used')}; "
            f"archive_artifact_used={runtime_data_artifacts.report.get('archive_artifact_used')}"
        ),
        row_count=len(runtime_data_rows),
        ready_ticker_count=runtime_data_artifacts.report.get("ready_ticker_count"),
        symbols=list(runtime_data_symbols),
    )

    strategy_required_symbols = {
        str(row.get("approved_phase1_symbol") or "")
        for row in matrix_rows
        if bool(row.get("strategy_approved")) or bool(row.get("can_submit"))
    }
    runtime_not_ready = [
        str(row.get("symbol") or "")
        for row in runtime_data_rows
        if row.get("runtime_candles_ready") is not True
    ]
    runtime_required_not_ready = [
        symbol for symbol in runtime_not_ready if symbol in strategy_required_symbols
    ]
    runtime_nonrequired_not_ready = [
        symbol for symbol in runtime_not_ready if symbol not in strategy_required_symbols
    ]
    runtime_reasons = {
        str(row.get("symbol") or ""): row.get("runtime_candles_block_reason")
        for row in runtime_data_rows
        if row.get("runtime_candles_ready") is not True
    }
    runtime_data_required_passed = not runtime_required_not_ready
    runtime_data_all_passed = runtime_data_required_passed and not runtime_nonrequired_not_ready
    add(
        "phase1_runtime_candles_ready_for_strategy_approved_symbols",
        runtime_data_all_passed,
        not runtime_data_required_passed,
        (
            f"strategy_required_symbols={sorted(strategy_required_symbols)}; "
            f"required_not_ready={runtime_required_not_ready}; "
            f"nonrequired_not_ready={runtime_nonrequired_not_ready}; "
            f"reasons={runtime_reasons}"
        ),
        strategy_required_symbols=sorted(strategy_required_symbols),
        required_not_ready=runtime_required_not_ready,
        nonrequired_not_ready=runtime_nonrequired_not_ready,
        reasons=runtime_reasons,
    )
except Exception as exc:
    add(
        "phase1_runtime_data_readiness_static",
        False,
        True,
        f"phase-1 runtime data readiness check failed: {exc}",
    )

governance_freshness_window_seconds = 120.0
try:
    from mgc_v05l.execution.ibkr_paper_strategy_governance import (
        IbkrPaperStrategyGovernanceConfig,
        run_ibkr_paper_strategy_governance,
        write_ibkr_paper_strategy_governance_artifacts,
    )

    governance_config = IbkrPaperStrategyGovernanceConfig(repo_root=REPO_ROOT)
    governance_freshness_window_seconds = float(governance_config.freshness_window_seconds)
    if MODE == "monday-live":
        governance_artifacts = run_ibkr_paper_strategy_governance(config=governance_config)
        write_ibkr_paper_strategy_governance_artifacts(
            config=governance_config,
            artifacts=governance_artifacts,
        )
        add(
            "governance_artifacts_regenerated",
            True,
            True,
            "regenerated var/per_strategy_paper_status.json and var/strategy_probation_dashboard.json from local runtime artifacts",
            classification=governance_artifacts.classification,
        )
except Exception as exc:
    if MODE == "monday-live":
        add(
            "governance_artifacts_regenerated",
            False,
            True,
            f"governance refresh failed before PAPER watch: {exc}",
        )
    else:
        checks.append(
            {
                "name": "governance_artifacts_regenerated",
                "status": "SKIP",
                "detail": "weekend-static does not regenerate governance artifacts",
            }
        )

governance_status_path = REPO_ROOT / "var/per_strategy_paper_status.json"
governance_dashboard_path = REPO_ROOT / "var/strategy_probation_dashboard.json"
governance_payload, governance_err = read_json(governance_status_path)
governance_dashboard, governance_dashboard_err = read_json(governance_dashboard_path)
governance_blocks = MODE == "monday-live"

def validate_runtime_json_artifact(
    *,
    name: str,
    path: Path,
    payload: dict[str, Any] | list[Any] | None,
    err: str | None,
    row_key: str,
) -> datetime | None:
    is_object = isinstance(payload, dict)
    generated_at = parse_datetime(payload.get("generated_at")) if is_object else None
    age_seconds = None if generated_at is None else max(0.0, (datetime.now(timezone.utc) - generated_at).total_seconds())
    rows = payload.get(row_key) if is_object else None
    row_count = len(rows) if isinstance(rows, list) else 0
    passed = (
        err is None
        and is_object
        and generated_at is not None
        and age_seconds is not None
        and age_seconds <= governance_freshness_window_seconds
        and row_count > 0
    )
    detail = (
        f"{path} generated_at={generated_at.isoformat() if generated_at else None}; "
        f"age_seconds={age_seconds}; freshness_window_seconds={governance_freshness_window_seconds}; "
        f"{row_key}_count={row_count}"
        if err is None
        else err
    )
    add(name, passed, governance_blocks, detail, path=str(path), age_seconds=age_seconds, row_count=row_count)
    return generated_at


governance_generated_at = validate_runtime_json_artifact(
    name="governance_status_artifact_fresh",
    path=governance_status_path,
    payload=governance_payload,
    err=governance_err,
    row_key="strategies",
)
dashboard_generated_at = validate_runtime_json_artifact(
    name="governance_dashboard_artifact_fresh",
    path=governance_dashboard_path,
    payload=governance_dashboard,
    err=governance_dashboard_err,
    row_key="active_rows",
)
timestamp_delta_seconds = (
    None
    if governance_generated_at is None or dashboard_generated_at is None
    else abs((governance_generated_at - dashboard_generated_at).total_seconds())
)
add(
    "governance_artifact_timestamps_match",
    timestamp_delta_seconds is not None and timestamp_delta_seconds <= 5.0,
    governance_blocks,
    f"timestamp_delta_seconds={timestamp_delta_seconds}; tolerance_seconds=5.0",
)

readiness_path = REPO_ROOT / "outputs/operator_dashboard/paper_readiness_snapshot.json"
readiness, readiness_err = read_json(readiness_path)
paper_trade_allowed = None
market_data_stale_count = None
paper_runtime_running = None
if isinstance(readiness, dict):
    paper_trade_allowed = readiness.get("paper_trade_allowed")
    market_data_stale_count = readiness.get("market_data_stale_count")
    paper_runtime_running = readiness.get("runtime_running")
stale_is_blocking = MODE == "monday-live"
paper_trade_allowed_blocks = stale_is_blocking and paper_runtime_running is not False
add(
    "paper_trade_allowed_true",
    paper_trade_allowed is True,
    paper_trade_allowed_blocks,
    (
        f"paper_trade_allowed={paper_trade_allowed}; runtime_running={paper_runtime_running}"
        if readiness_err is None
        else readiness_err
    ),
)
add(
    "market_data_not_stale",
    market_data_stale_count == 0,
    stale_is_blocking,
    f"market_data_stale_count={market_data_stale_count}" if readiness_err is None else readiness_err,
)

phase1_reconciliation_path = (
    REPO_ROOT
    / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)
phase1_reconciliation, phase1_reconciliation_err = read_json(phase1_reconciliation_path)
phase1_reconciliation_age = None
phase1_reconciliation_max_age = 120.0
phase1_reconciliation_green = False
reconciled_quantities_by_symbol: dict[str, list[str]] = {}
if isinstance(phase1_reconciliation, dict):
    try:
        phase1_reconciliation_max_age = float(phase1_reconciliation.get("max_age_seconds") or 120.0)
    except Exception:
        phase1_reconciliation_max_age = 120.0
    phase1_reconciliation_age = current_age_seconds(phase1_reconciliation.get("generated_at"))
    phase1_reconciliation_green = (
        phase1_reconciliation.get("classification") == "TRACK_B_PAPER_BROKER_RECONCILED"
        and phase1_reconciliation.get("broker_reconciled") is True
        and int(phase1_reconciliation.get("track_b_broker_open_order_count") or 0) == 0
        and int(phase1_reconciliation.get("review_required_count") or 0) == 0
        and phase1_reconciliation_age is not None
        and phase1_reconciliation_age <= phase1_reconciliation_max_age
    )
    for row in phase1_reconciliation.get("track_b_broker_positions") or []:
        if not isinstance(row, dict):
            continue
        root = position_root(row)
        if not root:
            continue
        reconciled_quantities_by_symbol.setdefault(root, []).append(position_quantity(row))
for quantities in reconciled_quantities_by_symbol.values():
    quantities.sort()
add(
    "phase1_broker_reconciliation_green_for_managed_positions",
    phase1_reconciliation_green,
    MODE == "monday-live",
    (
        f"classification={phase1_reconciliation.get('classification') if isinstance(phase1_reconciliation, dict) else None}; "
        f"age_seconds={phase1_reconciliation_age}; max_age_seconds={phase1_reconciliation_max_age}; "
        f"matched_positions={reconciled_quantities_by_symbol}"
    )
    if phase1_reconciliation_err is None
    else phase1_reconciliation_err,
    path=str(phase1_reconciliation_path),
    age_seconds=phase1_reconciliation_age,
)

monitor_path = REPO_ROOT / "outputs/reports/paper_strategy_monitor/paper_strategy_monitor_runtime_status.json"
monitor, monitor_err = read_json(monitor_path)
monitor_required = False
if MODE == "monday-live" and isinstance(monitor, dict):
    bridge_allowed_value = monitor.get("bridge_allowed")
    if bridge_allowed_value is None:
        bridge_allowed_value = monitor.get("submit_allowed")
    add(
        "monitor_healthy",
        monitor.get("health_classification") == "HEALTHY",
        monitor_required,
        f"health_classification={monitor.get('health_classification')}",
    )
    add(
        "monitor_not_stale",
        monitor.get("stale") is False,
        monitor_required,
        f"stale={monitor.get('stale')}",
    )
    add(
        "monitor_submit_allowed",
        monitor.get("submit_allowed") is True,
        monitor_required,
        f"submit_allowed={monitor.get('submit_allowed')}",
    )
    add(
        "bridge_allowed",
        bridge_allowed_value is True,
        monitor_required,
        f"bridge_allowed={bridge_allowed_value}",
    )
elif MODE == "monday-live":
    add("monitor_status_available", False, monitor_required, monitor_err or "monitor missing")

strategies_required = MODE == "monday-live"
if isinstance(operator_status, dict):
    lanes = operator_status.get("lanes") or {}
    if isinstance(lanes, list):
        lane_rows = lanes
    elif isinstance(lanes, dict):
        lane_rows = list(lanes.values())
    else:
        lane_rows = []
    active_lane_ids = operator_status.get("active_lane_ids") or []
    status = operator_status.get("strategy_status")
    entries_enabled = operator_status.get("entries_enabled")
    position_side = operator_status.get("position_side")
    phase1_lanes_ready_or_managing_reconciled_positions = True
    managed_position_symbols: set[str] = set()
    for lane in lane_rows:
        lane_id = str(lane.get("lane_id") or lane.get("strategy_id") or "")
        lane_status = lane.get("strategy_status") or lane.get("status")
        lane_side = lane.get("position_side")
        lane_symbol = infer_lane_symbol(lane)
        lane_qty = decimalish(lane.get("broker_position_qty") or lane.get("internal_position_qty") or 0)
        lane_open_order_count = int(lane.get("open_order_count") or 0)
        lane_fault = lane.get("fault_code")
        lane_flat_ready = lane_status in ("READY", "RUNNING", "RUNNING_MULTI_LANE") and lane_side == "FLAT"
        lane_has_position = lane_side in ("LONG", "SHORT") or lane_qty not in ("0", "0.0")
        if lane_has_position and lane_symbol:
            managed_position_symbols.add(lane_symbol)
        lane_managing_reconciled_position = (
            phase1_reconciliation_green
            and lane_symbol in reconciled_quantities_by_symbol
            and lane_qty in reconciled_quantities_by_symbol.get(lane_symbol, [])
            and lane_side in ("LONG", "SHORT")
            and lane_open_order_count == 0
            and lane_fault in (None, "", "reconciliation_unsafe_ambiguity")
            and (
                lane_status in ("READY", "RUNNING", "RUNNING_MULTI_LANE", "RECONCILING")
                or str(lane_status or "").startswith("IN_")
                or str(lane_status or "").startswith("RUNNING_")
            )
        )
        if lane_has_position and not lane_managing_reconciled_position:
            phase1_lanes_ready_or_managing_reconciled_positions = False
    add(
        "strategies_evaluating",
        bool(active_lane_ids) and status in ("RUNNING_MULTI_LANE", "RUNNING", "READY"),
        strategies_required,
        f"status={status}, active_lane_count={len(active_lane_ids)}",
    )
    add(
        "phase1_lanes_ready_or_managing_reconciled_positions",
        phase1_lanes_ready_or_managing_reconciled_positions
        and (
            position_side == "FLAT"
            or (
                phase1_reconciliation_green
                and managed_position_symbols
                and managed_position_symbols.issubset(set(reconciled_quantities_by_symbol))
            )
        ),
        strategies_required,
        (
            f"aggregate_position_side={position_side}, entries_enabled={entries_enabled}, "
            f"managed_position_symbols={sorted(managed_position_symbols)}, "
            f"reconciled_symbols={sorted(reconciled_quantities_by_symbol)}"
        ),
    )
else:
    add("operator_status_available", False, strategies_required, operator_err or "operator status missing")

ibkr_cmd = [
    "./.venv/bin/python",
    "-m",
    "mgc_v05l.app.ibkr_read_only_verify",
    "--mode",
    "PAPER",
    "--host",
    "127.0.0.1",
    "--port",
    "7497",
    "--client-id",
    "9071",
    "--account-id",
    "DUM882026",
    "--read-only",
    "--timeout-seconds",
    "8",
    "--skip-market-data-probe",
    "--skip-duplicate-client-id-probe",
    "--overwrite",
]
ibkr = run(ibkr_cmd, timeout=20)
broker_required = MODE == "monday-live"
broker_available = ibkr["returncode"] == 0
add(
    "ibkr_read_only_available",
    broker_available,
    broker_required,
    "read-only broker verification succeeded" if broker_available else ibkr["stderr"] or ibkr["stdout"],
)

phase1_broker_symbols = ("GC", "NQ", "ES", "MGC", "MNQ", "MES", "ZT", "ZF", "ZN", "ZB", "PL")
positions_path = REPO_ROOT / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json"
positions, positions_err = read_json(positions_path)
flat_symbols: dict[str, Any] = {}
if broker_available and isinstance(positions, dict):
    rows = positions.get("positions") or positions.get("rows") or []
elif broker_available and isinstance(positions, list):
    rows = positions
else:
    rows = []
for symbol in phase1_broker_symbols:
    qtys = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_symbol = str(row.get("symbol") or row.get("contract_symbol") or "")
        local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "")
        if row_symbol == symbol or local_symbol.startswith(symbol):
            try:
                qtys.append(float(row.get("quantity") or row.get("position") or 0))
            except Exception:
                qtys.append(row.get("quantity"))
    flat_symbols[symbol] = qtys
    current_qtys = sorted(decimalish(qty) for qty in qtys)
    reconciled_qtys = reconciled_quantities_by_symbol.get(symbol, [])
    flat_or_reconciled = all(q == 0 for q in qtys) or (
        phase1_reconciliation_green
        and bool(current_qtys)
        and current_qtys == reconciled_qtys
    )
    add(
        f"broker_{symbol.lower()}_flat_or_reconciled_if_connected",
        (not broker_available and MODE == "weekend-static") or flat_or_reconciled,
        broker_required,
        (
            f"{symbol} quantities={qtys}; reconciled_quantities={reconciled_qtys}; "
            f"phase1_reconciliation_green={phase1_reconciliation_green}"
        )
        if broker_available
        else "broker read-only unavailable in weekend mode",
    )

open_orders_path = REPO_ROOT / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json"
open_orders, open_orders_err = read_json(open_orders_path)
open_rows = []
if broker_available and isinstance(open_orders, dict):
    open_rows = open_orders.get("open_orders") or open_orders.get("orders") or open_orders.get("rows") or []
elif broker_available and isinstance(open_orders, list):
    open_rows = open_orders
symbol_open_orders = []
open_orders_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in phase1_broker_symbols}
for row in open_rows:
    if not isinstance(row, dict):
        continue
    row_symbol = str(row.get("symbol") or row.get("contract_symbol") or "").upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").upper()
    matched_symbols = [
        symbol
        for symbol in phase1_broker_symbols
        if row_symbol == symbol or local_symbol.startswith(symbol)
    ]
    if matched_symbols:
        symbol_open_orders.append(row)
        for symbol in matched_symbols:
            open_orders_by_symbol[symbol].append(row)
for symbol in phase1_broker_symbols:
    count = len(open_orders_by_symbol.get(symbol) or [])
    add(
        f"broker_{symbol.lower()}_open_orders_zero_if_connected",
        (not broker_available and MODE == "weekend-static") or count == 0,
        broker_required,
        f"{symbol} open_orders={count}" if broker_available else "broker read-only unavailable in weekend mode",
    )
add(
    "broker_phase1_open_orders_zero_if_connected",
    (not broker_available and MODE == "weekend-static") or not symbol_open_orders,
    broker_required,
    f"Phase-1 open_orders={len(symbol_open_orders)}" if broker_available else "broker read-only unavailable in weekend mode",
)

current_review_required = False
review_sources = []
for path in (
    REPO_ROOT / "outputs/probationary_pattern_engine/paper_session/operator_status.json",
    REPO_ROOT / "outputs/operator_dashboard/paper_readiness_snapshot.json",
):
    payload, err = read_json(path)
    if isinstance(payload, dict) and payload.get("review_required") is True:
        current_review_required = True
        review_sources.append(str(path))
add(
    "no_current_review_required",
    not current_review_required,
    MODE == "monday-live",
    "no current review_required flag found" if not current_review_required else ", ".join(review_sources),
)

if blocking:
    status = "FAIL"
elif warnings:
    status = "WARN"
else:
    status = "PASS"

governance_artifact_check_names = {
    "governance_artifacts_regenerated",
    "governance_status_artifact_fresh",
    "governance_dashboard_artifact_fresh",
    "governance_artifact_timestamps_match",
}
governance_artifact_failure = any(
    check.get("name") in governance_artifact_check_names and check.get("status") == "FAIL"
    for check in checks
)
weekend_status = (
    "PASS"
    if MODE == "weekend-static" and not blocking
    else status
    if MODE == "weekend-static"
    else "NOT_APPLICABLE"
)
monday_status = "NOT_APPLICABLE" if MODE == "weekend-static" else status
result = {
    "schema_version": "track_b_paper_preflight_v1",
    "generated_at": now_iso(),
    "mode": MODE,
    "repo_root": str(REPO_ROOT),
    "required_patch_commits": list(REQUIRED_PATCH_COMMITS),
    "final_submit_path_baseline_commit": "35064b698c",
    "final_execution_scope_baseline_commit": "94695132b1",
    "final_rates_scope_baseline_commit": "520bd7313f",
    "final_ticker_readiness_matrix_baseline_commit": "57402b55a7",
    "final_runtime_ticker_registry_baseline_commit": "155cea2bfa",
    "final_runtime_data_readiness_baseline_commit": "a47a6f6806",
    "final_runtime_data_matrix_integration_baseline_commit": "83ed3578c2",
    "weekend_static_dry_run": weekend_status,
    "monday_live_preflight": monday_status,
    "monday_blocked_classification": (
        "MONDAY_BLOCKED_READINESS_ARTIFACTS"
        if MODE == "monday-live" and governance_artifact_failure
        else None
    ),
    "blocking_reasons": blocking,
    "warnings": warnings,
    "next_action": (
        "Run monday-live preflight before PAPER watch"
        if MODE == "weekend-static" and status == "PASS"
        else "Review warnings, then run monday-live preflight before PAPER watch"
        if MODE == "weekend-static" and status == "WARN"
        else "Start PAPER watch"
        if status == "PASS"
        else "Review nonblocking warnings; GC guarded PAPER watch is not blocked by required-symbol readiness"
        if MODE == "monday-live" and status == "WARN" and not blocking
        else "Resolve blocking reasons before PAPER watch"
    ),
    "checks": checks,
}
OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
OUT_JSON.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(json.dumps(result, indent=2, sort_keys=True))

raise SystemExit(0 if status in ("PASS", "WARN") else 1)
PY
