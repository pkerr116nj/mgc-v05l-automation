"""Read-only Track B runtime/research parity audit.

The audit intentionally observes persisted PAPER artifacts only. It does not
import runtime services, connect to brokers, restart processes, or mutate order
lifecycle state.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.session_phase_labels import label_session_phase


DEFAULT_RUNTIME_DIR = Path("outputs/probationary_pattern_engine/paper_session")
DEFAULT_DASHBOARD_READINESS = Path("outputs/operator_dashboard/runtime/operator_dashboard_readiness.json")
DEFAULT_WAREHOUSE_DB = Path(
    "outputs/warehouse_historical_evaluator_basket_q1_candidate_alignment_fix/catalogs/"
    "warehouse_historical_evaluator.duckdb"
)
DEFAULT_ENTRY_ACCEPTANCE_ROOT = Path("outputs/reports/entry_acceptance_research/full_history_batch")
DEFAULT_OUTPUT_DIR = Path("outputs/reports/track_b_parity_audit")
ENTRY_INTENT_TYPES = {"BUY_TO_OPEN", "SELL_SHORT", "SELL_TO_OPEN"}
BLOCKED_ORDER_STATUSES = {"REJECTED", "BLOCKED", "CANCELLED", "ERROR"}
ROUTE_HEALTH_REASONS = {"ROUTE_HEALTH_STALE", "BROKER_ROUTE_UNHEALTHY", "ROUTE_HEALTH_BLOCKED"}
GOVERNANCE_REJECTION_HINTS = (
    "GOVERNANCE",
    "EXPOSURE",
    "PRE_SUBMIT_GATE",
    "SAME_UNDERLYING",
    "MAX_CONCURRENT",
    "POSITION_LIMIT",
    "ENTRY_DISABLED",
)


@dataclass(frozen=True)
class LaneFiles:
    lane_id: str
    artifacts_dir: Path
    database_path: Path


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
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


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def _safe_label_session(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    try:
        return label_session_phase(ts)
    except Exception:
        return None


def _sqlite_connect_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "select 1 from sqlite_master where type='table' and name=? limit 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _scalar(connection: sqlite3.Connection, query: str, params: Sequence[Any] = ()) -> int:
    row = connection.execute(query, params).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"pragma table_info({table_name})")}


def _resolve_database_path(repo_root: Path, lane: Mapping[str, Any]) -> Path:
    database_url = str(lane.get("database_url") or "")
    if database_url.startswith("sqlite:///"):
        database_url = database_url.removeprefix("sqlite:///")
    candidate = Path(database_url) if database_url else Path(f"mgc_v05l.probationary.paper__{lane['lane_id']}.sqlite3")
    if not candidate.is_absolute():
        candidate = repo_root / candidate
    return candidate


def _resolve_lane_files(repo_root: Path, lane: Mapping[str, Any]) -> LaneFiles:
    lane_id = str(lane["lane_id"])
    artifacts_dir = Path(str(lane.get("artifacts_dir") or DEFAULT_RUNTIME_DIR / "lanes" / lane_id))
    if not artifacts_dir.is_absolute():
        artifacts_dir = repo_root / artifacts_dir
    return LaneFiles(lane_id=lane_id, artifacts_dir=artifacts_dir, database_path=_resolve_database_path(repo_root, lane))


def _count_setup_signals(connection: sqlite3.Connection) -> tuple[int, int, Counter[str], Counter[str]]:
    if not _table_exists(connection, "signals"):
        return 0, 0, Counter(), Counter()
    candidates = 0
    near_misses = 0
    sessions: Counter[str] = Counter()
    predicate_counter: Counter[str] = Counter()
    for created_at, payload_json in connection.execute("select created_at, payload_json from signals"):
        ts = _parse_ts(created_at)
        try:
            payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        true_keys = {str(key) for key, value in payload.items() if value is True}
        entry_true = any(key in true_keys for key in ("long_entry", "short_entry"))
        raw_true = any(key in true_keys for key in ("long_entry_raw", "short_entry_raw"))
        candidate_true = entry_true or raw_true or any(key.endswith("_turn_candidate") for key in true_keys)
        if candidate_true:
            candidates += 1
            session = _safe_label_session(ts)
            if session:
                sessions[session] += 1
            for key in sorted(true_keys):
                if (
                    key.endswith("_turn_candidate")
                    or key.endswith("_entry")
                    or key.endswith("_entry_raw")
                    or key.startswith("recent_")
                ):
                    predicate_counter[key] += 1
        if raw_true and not entry_true:
            near_misses += 1
    return candidates, candidates, sessions, predicate_counter


def _intent_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(connection, "order_intents"):
        return []
    columns = _column_names(connection, "order_intents")
    requested = [
        "order_intent_id",
        "intent_type",
        "created_at",
        "submitted_at",
        "acknowledged_at",
        "reason_code",
        "order_status",
        "broker_order_status",
        "broker_order_id",
        "timeout_classification",
    ]
    select_columns = [column for column in requested if column in columns]
    rows = []
    for raw in connection.execute(f"select {', '.join(select_columns)} from order_intents"):
        rows.append(dict(zip(select_columns, raw)))
    return rows


def _count_restored_or_catchup(intent_ts: datetime | None, runtime_started_at: datetime | None) -> bool:
    if intent_ts is None or runtime_started_at is None:
        return False
    return intent_ts < runtime_started_at


def _latest_snapshot_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(connection, "strategy_state_snapshots"):
        return {}
    row = connection.execute(
        "select payload_json from strategy_state_snapshots order by updated_at desc limit 1"
    ).fetchone()
    if not row:
        return {}
    try:
        payload = json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _reconciliation_summary(connection: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(connection, "reconciliation_events"):
        return {"count": 0, "clean_count": 0, "latest_classification": None}
    count = 0
    clean_count = 0
    latest_created_at: datetime | None = None
    latest_classification: str | None = None
    for created_at, payload_json in connection.execute("select created_at, payload_json from reconciliation_events"):
        count += 1
        try:
            payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if isinstance(payload, dict) and payload.get("clean") is True:
            clean_count += 1
        ts = _parse_ts(created_at)
        if ts is not None and (latest_created_at is None or ts > latest_created_at):
            latest_created_at = ts
            latest_classification = str(payload.get("classification")) if isinstance(payload, dict) else None
    return {"count": count, "clean_count": clean_count, "latest_classification": latest_classification}


def _lane_audit(
    *,
    repo_root: Path,
    lane: Mapping[str, Any],
    runtime_started_at: datetime | None,
    readiness_converged_at: datetime | None,
    operator_lane: Mapping[str, Any] | None,
) -> dict[str, Any]:
    files = _resolve_lane_files(repo_root, lane)
    result: dict[str, Any] = {
        "lane_id": files.lane_id,
        "symbol": lane.get("symbol"),
        "strategy_family": lane.get("strategy_family") or lane.get("source_family"),
        "session_restriction": lane.get("session_restriction"),
        "allowed_sessions": lane.get("allowed_sessions") or [],
        "paper_only": bool(lane.get("paper_only")),
        "market_data_source": lane.get("probationary_paper_market_data_source"),
        "artifacts_dir": str(files.artifacts_dir),
        "database_path": str(files.database_path),
        "database_exists": files.database_path.exists(),
    }
    if operator_lane:
        result.update(
            {
                "runtime_freshness_state": operator_lane.get("market_data_recovery", {}).get("market_data_recovery_state")
                if isinstance(operator_lane.get("market_data_recovery"), dict)
                else None,
                "runtime_freshness_root_cause": operator_lane.get("market_data_recovery", {}).get("recovery_root_cause")
                if isinstance(operator_lane.get("market_data_recovery"), dict)
                else None,
                "latest_processed_bar_end_ts": operator_lane.get("last_processed_bar_end_ts"),
                "latest_completed_bar_end_ts": operator_lane.get("latest_completed_bar_end_ts"),
                "eligibility_reason": operator_lane.get("eligibility_reason"),
                "eligible_now": operator_lane.get("eligible_now"),
                "entries_enabled": operator_lane.get("entries_enabled"),
                "startup_restore_validation_clean": (
                    operator_lane.get("startup_restore_validation", {}).get("clean")
                    if isinstance(operator_lane.get("startup_restore_validation"), dict)
                    else None
                ),
                "startup_duplicate_prevention_action": (
                    operator_lane.get("startup_duplicate_prevention", {}).get("action")
                    if isinstance(operator_lane.get("startup_duplicate_prevention"), dict)
                    else None
                ),
            }
        )
    if not files.database_path.exists():
        result["dominant_no_trade_reason"] = "database_missing"
        return result

    with _sqlite_connect_ro(files.database_path) as connection:
        result["bars_evaluated"] = _scalar(connection, "select count(*) from processed_bars") if _table_exists(connection, "processed_bars") else 0
        if _table_exists(connection, "processed_bars"):
            bar_bounds = connection.execute("select min(end_ts), max(end_ts) from processed_bars").fetchone()
            result["first_evaluated_bar_ts"] = bar_bounds[0] if bar_bounds else None
            result["last_evaluated_bar_ts"] = bar_bounds[1] if bar_bounds else None
        candidates, qualified, sessions, predicates = _count_setup_signals(connection)
        result["setup_candidates_observed"] = candidates
        result["setup_qualified_signals"] = qualified
        result["setup_candidate_session_counts"] = dict(sorted(sessions.items()))
        result["rejected_predicates"] = dict(predicates.most_common(20))
        result["near_misses"] = 0

        intents = _intent_rows(connection)
        entry_intents = [row for row in intents if str(row.get("intent_type")) in ENTRY_INTENT_TYPES]
        blocked = [
            row
            for row in entry_intents
            if str(row.get("order_status")) in BLOCKED_ORDER_STATUSES
            or str(row.get("broker_order_status") or "").upper().endswith("BLOCKED")
        ]
        route_health = [
            row
            for row in blocked
            if str(row.get("timeout_classification") or row.get("broker_order_status") or "") in ROUTE_HEALTH_REASONS
        ]
        governance = [
            row
            for row in blocked
            if any(
                hint in str(row.get("timeout_classification") or row.get("broker_order_status") or row.get("reason_code") or "").upper()
                for hint in GOVERNANCE_REJECTION_HINTS
            )
        ]
        route_attempts = [row for row in entry_intents if row.get("submitted_at") or row.get("broker_order_id")]
        successful_routes = list(route_attempts)
        post_convergence = [
            row
            for row in entry_intents
            if readiness_converged_at is not None
            and (created_at := _parse_ts(row.get("created_at"))) is not None
            and created_at >= readiness_converged_at
        ]
        pre_readiness = [
            row
            for row in entry_intents
            if readiness_converged_at is not None
            and (created_at := _parse_ts(row.get("created_at"))) is not None
            and created_at < readiness_converged_at
        ]
        restored = [
            row for row in entry_intents if _count_restored_or_catchup(_parse_ts(row.get("created_at")), runtime_started_at)
        ]
        post_convergence_blocked = [row for row in blocked if row in post_convergence]

        result["entry_intents"] = len(entry_intents)
        result["blocked_entry_intents"] = len(blocked)
        result["route_health_rejections"] = len(route_health)
        result["governance_exposure_rejections"] = len(governance)
        result["broker_route_attempts"] = len(route_attempts)
        result["successful_routed_intents"] = len(successful_routes)
        result["post_convergence_entry_intents"] = len(post_convergence)
        result["post_convergence_blocked_entry_intents"] = len(post_convergence_blocked)
        result["pre_readiness_entry_intents"] = len(pre_readiness)
        result["restored_or_catchup_entry_intents"] = len(restored)
        result["fills"] = _scalar(connection, "select count(*) from fills") if _table_exists(connection, "fills") else 0
        result["post_convergence_fills"] = (
            _scalar(connection, "select count(*) from fills where fill_timestamp >= ?", (_iso(readiness_converged_at),))
            if _table_exists(connection, "fills") and readiness_converged_at is not None
            else 0
        )
        result["blocked_reason_counts"] = dict(
            Counter(
                str(row.get("timeout_classification") or row.get("broker_order_status") or row.get("reason_code") or "UNKNOWN")
                for row in blocked
            ).most_common()
        )
        result["latest_snapshot"] = {
            key: value
            for key, value in _latest_snapshot_payload(connection).items()
            if key
            in {
                "entries_enabled",
                "fault_code",
                "internal_position_qty",
                "broker_position_qty",
                "last_order_intent_id",
                "last_signal_bar_id",
                "operator_halt",
                "position_side",
                "reconcile_required",
            }
        }
        result["reconciliation"] = _reconciliation_summary(connection)

    if result.get("post_convergence_blocked_entry_intents"):
        dominant = "POST_CONVERGENCE_SETUP_BLOCKED"
    elif result.get("post_convergence_entry_intents"):
        dominant = "POST_CONVERGENCE_SETUP_ROUTED_OR_ACTIVE"
    elif result.get("setup_candidates_observed", 0) == 0:
        dominant = str(result.get("eligibility_reason") or "no_setup_observed")
    elif result.get("blocked_reason_counts"):
        dominant = next(iter(result["blocked_reason_counts"]))
    elif result.get("fills", 0) > 0:
        dominant = "TRADED_OR_FILLED_PREVIOUSLY"
    else:
        dominant = str(result.get("eligibility_reason") or "no_trade_after_setup")
    result["dominant_no_trade_reason"] = dominant
    return result


def _aggregate_lanes(lanes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    totals: defaultdict[str, int] = defaultdict(int)
    numeric_keys = (
        "bars_evaluated",
        "setup_candidates_observed",
        "setup_qualified_signals",
        "near_misses",
        "entry_intents",
        "blocked_entry_intents",
        "route_health_rejections",
        "governance_exposure_rejections",
        "broker_route_attempts",
        "successful_routed_intents",
        "fills",
        "post_convergence_entry_intents",
        "post_convergence_blocked_entry_intents",
        "post_convergence_fills",
        "pre_readiness_entry_intents",
        "restored_or_catchup_entry_intents",
    )
    for lane in lanes:
        for key in numeric_keys:
            totals[key] += int(lane.get(key) or 0)
    dominant = Counter(str(lane.get("dominant_no_trade_reason") or "UNKNOWN") for lane in lanes)
    totals_dict = dict(totals)
    totals_dict["dominant_no_trade_reasons"] = dict(dominant.most_common())
    return totals_dict


def _operator_lanes(operator_status: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    lanes = operator_status.get("lanes")
    if not isinstance(lanes, list):
        return {}
    return {str(lane.get("lane_id")): lane for lane in lanes if isinstance(lane, dict) and lane.get("lane_id")}


def _runtime_process_scan(repo_root: Path) -> dict[str, Any]:
    try:
        completed = subprocess.run(["ps", "-efww"], check=False, text=True, capture_output=True)
    except OSError as exc:
        return {
            "available": False,
            "error": f"{exc.__class__.__name__}: {exc}",
            "active_process_count": None,
            "processes": [],
        }
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        if "probationary-paper-soak" not in line:
            continue
        if str(repo_root) not in line:
            continue
        parts = line.split(None, 7)
        rows.append(
            {
                "uid": parts[0] if len(parts) > 0 else None,
                "pid": int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None,
                "ppid": int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None,
                "command": parts[7] if len(parts) > 7 else line,
            }
        )
    return {"available": True, "error": None, "active_process_count": len(rows), "processes": rows}


def _research_participation(warehouse_db: Path | None) -> dict[str, Any]:
    if warehouse_db is None or not warehouse_db.exists():
        return {"warehouse_db": str(warehouse_db) if warehouse_db else None, "available": False}
    try:
        import duckdb  # type: ignore[import-not-found]
    except ImportError:
        return {"warehouse_db": str(warehouse_db), "available": False, "error": "duckdb_unavailable"}
    summary: dict[str, Any] = {"warehouse_db": str(warehouse_db), "available": True}
    connection = duckdb.connect(str(warehouse_db), read_only=True)
    try:
        tables = {row[0] for row in connection.execute("show tables").fetchall()}
        for table, key in (
            ("family_event_tables", "raw_family_events"),
            ("lane_candidates", "lane_candidates"),
            ("lane_entries", "lane_entries"),
            ("lane_closed_trades", "closed_trades"),
            ("lane_compact_results", "compact_lanes"),
        ):
            if table in tables:
                summary[key] = int(connection.execute(f"select count(*) from {table}").fetchone()[0])
        if "lane_compact_results" in tables:
            cols = {row[0] for row in connection.execute("describe lane_compact_results").fetchall()}
            if "trade_count" in cols:
                summary["zero_trade_compact_lanes"] = int(
                    connection.execute("select count(*) from lane_compact_results where coalesce(trade_count, 0) = 0").fetchone()[0]
                )
        raw = int(summary.get("raw_family_events") or 0)
        candidates = int(summary.get("lane_candidates") or 0)
        entries = int(summary.get("lane_entries") or 0)
        closed = int(summary.get("closed_trades") or 0)
        summary["candidate_to_raw_retention"] = candidates / raw if raw else None
        summary["entry_to_candidate_retention"] = entries / candidates if candidates else None
        summary["closed_to_entry_retention"] = closed / entries if entries else None
    finally:
        connection.close()
    return summary


def _parquet_dataset_count(root: Path, dataset_key: str) -> tuple[int, int]:
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError:
        return 0, 0
    total = 0
    file_count = 0
    for path in root.glob(f"*/*/datasets/{dataset_key}/**/*.parquet"):
        if path.name.startswith("_schema"):
            continue
        try:
            total += int(pq.ParquetFile(path).metadata.num_rows)
        except Exception:
            continue
        file_count += 1
    return total, file_count


def _json_get(path: Path, *keys: str) -> Any:
    if not path.exists():
        return None
    payload = _read_json(path)
    cursor: Any = payload
    for key in keys:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def _entry_acceptance_materialization(entry_acceptance_root: Path | None) -> dict[str, Any]:
    if entry_acceptance_root is None or not entry_acceptance_root.exists():
        return {"available": False, "root": str(entry_acceptance_root) if entry_acceptance_root else None}
    partition_root = entry_acceptance_root / "warehouse_historical_evaluator_partitions"
    near_path = entry_acceptance_root / "near_narrowing_study" / "near_candidate_narrowing_study_v1.json"
    combined_path = entry_acceptance_root / "combined_cross_instrument_summary.json"
    datasets: dict[str, Any] = {}
    for key in (
        "raw_bars_1m",
        "derived_bars_5m",
        "shared_features_5m",
        "family_event_tables",
        "lane_candidates",
        "lane_entries",
        "lane_closed_trades",
        "lane_compact_results",
    ):
        row_count, file_count = _parquet_dataset_count(partition_root, key)
        datasets[key] = {"rows": row_count, "files": file_count}
    near_candidate_count = _json_get(near_path, "near_candidate_count")
    rows_scanned = _json_get(near_path, "rows_scanned")
    score_sweep = _json_get(near_path, "score_threshold_sweep") or {}
    combined_rows = _json_get(combined_path, "rows")
    exact_rule_flags = _json_get(combined_path, "exact_rule_flags")
    baseline_episodes = _json_get(combined_path, "baseline_episodes")
    near_gte_0_80_episodes = _json_get(combined_path, "near_gte_0_80_episodes")
    return {
        "available": True,
        "root": str(entry_acceptance_root),
        "partition_root": str(partition_root),
        "near_narrowing_study": str(near_path),
        "combined_cross_instrument_summary": str(combined_path),
        "datasets": datasets,
        "rows_scanned": rows_scanned,
        "combined_rows": combined_rows,
        "near_candidate_count": near_candidate_count,
        "exact_rule_flags": exact_rule_flags,
        "baseline_episodes": baseline_episodes,
        "near_gte_0_80_episodes": near_gte_0_80_episodes,
        "score_threshold_counts": {
            key: {
                "candidate_count": value.get("candidate_count"),
                "episode_count": value.get("episode_count"),
            }
            for key, value in score_sweep.items()
            if isinstance(value, dict)
        },
        "candidate_retention": {
            "near_to_rows_scanned": (near_candidate_count / rows_scanned)
            if isinstance(near_candidate_count, (int, float)) and isinstance(rows_scanned, (int, float)) and rows_scanned
            else None,
            "exact_rule_to_near": (exact_rule_flags / near_candidate_count)
            if isinstance(exact_rule_flags, (int, float)) and isinstance(near_candidate_count, (int, float)) and near_candidate_count
            else None,
            "baseline_episode_to_near": (baseline_episodes / near_candidate_count)
            if isinstance(baseline_episodes, (int, float)) and isinstance(near_candidate_count, (int, float)) and near_candidate_count
            else None,
        },
    }


def build_track_b_parity_audit(
    *,
    repo_root: Path,
    runtime_dir: Path = DEFAULT_RUNTIME_DIR,
    dashboard_readiness_path: Path = DEFAULT_DASHBOARD_READINESS,
    warehouse_db: Path | None = DEFAULT_WAREHOUSE_DB,
    entry_acceptance_root: Path | None = DEFAULT_ENTRY_ACCEPTANCE_ROOT,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    runtime_dir = runtime_dir if runtime_dir.is_absolute() else repo_root / runtime_dir
    dashboard_readiness_path = (
        dashboard_readiness_path if dashboard_readiness_path.is_absolute() else repo_root / dashboard_readiness_path
    )
    warehouse_db = warehouse_db if warehouse_db is None or warehouse_db.is_absolute() else repo_root / warehouse_db
    entry_acceptance_root = (
        entry_acceptance_root
        if entry_acceptance_root is None or entry_acceptance_root.is_absolute()
        else repo_root / entry_acceptance_root
    )
    config = _read_json(runtime_dir / "runtime" / "paper_config_in_force.json")
    operator_status = _read_json(runtime_dir / "operator_status.json")
    readiness = _read_json(dashboard_readiness_path)
    runtime_started_at = _parse_ts(config.get("generated_at"))
    readiness_converged_at = _parse_ts((readiness.get("stability") or {}).get("stable_since"))
    active_lanes = config.get("lanes") if isinstance(config.get("lanes"), list) else []
    operator_by_lane = _operator_lanes(operator_status)
    lane_reports = [
        _lane_audit(
            repo_root=repo_root,
            lane=lane,
            runtime_started_at=runtime_started_at,
            readiness_converged_at=readiness_converged_at,
            operator_lane=operator_by_lane.get(str(lane.get("lane_id"))),
        )
        for lane in active_lanes
        if isinstance(lane, dict) and lane.get("lane_id")
    ]
    totals = _aggregate_lanes(lane_reports)
    process_scan = _runtime_process_scan(repo_root)
    return {
        "metadata": {
            "mode": "READ_ONLY_PAPER_AUDIT",
            "paper_only": True,
            "broker_mutation": False,
            "runtime_mutation": False,
            "repo_root": str(repo_root),
            "runtime_dir": str(runtime_dir),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "process_confirmation": process_scan,
        "runtime": {
            "config_generated_at": config.get("generated_at"),
            "operator_generated_at": operator_status.get("generated_at"),
            "readiness_generated_at": readiness.get("generated_at"),
            "readiness_state": readiness.get("readiness_state"),
            "readiness_reason_code": readiness.get("reason_code"),
            "readiness_converged_at": _iso(readiness_converged_at),
            "operator_current_session": operator_status.get("current_detected_session"),
            "active_lane_count": len(config.get("active_lane_ids") or []),
            "active_lane_ids": config.get("active_lane_ids") or [],
            "expected_v2_overlay_active": len(config.get("active_lane_ids") or []) == 15,
            "config_source_runtime_artifact": str(runtime_dir / "runtime" / "paper_config_in_force.json"),
            "operator_status_path": str(runtime_dir / "operator_status.json"),
            "dashboard_readiness_path": str(dashboard_readiness_path),
        },
        "summary": totals,
        "lanes": lane_reports,
        "research_participation": _research_participation(warehouse_db),
        "entry_acceptance_materialization": _entry_acceptance_materialization(entry_acceptance_root),
        "verdict": {
            "post_convergence_missed_setup_finding_holds": totals.get("post_convergence_blocked_entry_intents", 0) == 0,
            "post_convergence_entry_intents": totals.get("post_convergence_entry_intents", 0),
            "post_convergence_blocked_entry_intents": totals.get("post_convergence_blocked_entry_intents", 0),
            "post_convergence_fills": totals.get("post_convergence_fills", 0),
        },
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    runtime = report.get("runtime", {})
    summary = report.get("summary", {})
    verdict = report.get("verdict", {})
    process = report.get("process_confirmation", {})
    lines = [
        "# Track B Parity Audit",
        "",
        "## Runtime Confirmation",
        f"- repo root: `{report.get('metadata', {}).get('repo_root')}`",
        f"- process scan available inside audit CLI: `{process.get('available')}`",
        f"- active runtime processes from audit CLI scan: `{process.get('active_process_count')}`",
    ]
    if process.get("error"):
        lines.append(f"- process scan note: `{process.get('error')}`")
    for row in process.get("processes", []) or []:
        lines.append(f"- PID `{row.get('pid')}` command: `{row.get('command')}`")
    lines.extend(
        [
            f"- active lanes: `{runtime.get('active_lane_count')}`",
            f"- v2 15-lane overlay active: `{runtime.get('expected_v2_overlay_active')}`",
            f"- readiness state: `{runtime.get('readiness_state')}`",
            f"- readiness converged at: `{runtime.get('readiness_converged_at')}`",
            f"- operator session: `{runtime.get('operator_current_session')}`",
            "",
            "## Runtime Verdict",
            f"- post-convergence entry intents: `{summary.get('post_convergence_entry_intents', 0)}`",
            f"- post-convergence blocked entry intents: `{summary.get('post_convergence_blocked_entry_intents', 0)}`",
            f"- post-convergence fills: `{summary.get('post_convergence_fills', 0)}`",
            f"- 0 post-convergence missed-setup finding holds: `{verdict.get('post_convergence_missed_setup_finding_holds')}`",
            f"- bars evaluated: `{summary.get('bars_evaluated', 0)}`",
            f"- setup candidates observed: `{summary.get('setup_candidates_observed', 0)}`",
            f"- setup-qualified signals: `{summary.get('setup_qualified_signals', 0)}`",
            f"- broker route attempts: `{summary.get('broker_route_attempts', 0)}`",
            f"- successful routed intents: `{summary.get('successful_routed_intents', 0)}`",
            "",
            "## Lane Funnel",
        ]
    )
    for lane in report.get("lanes", []) or []:
        lines.append(
            "- `{lane_id}` {symbol} {sessions}: bars={bars} candidates={candidates} "
            "qualified={qualified} intents={intents} blocked={blocked} fills={fills} "
            "post_conv_blocked={post_blocked} dominant=`{dominant}`".format(
                lane_id=lane.get("lane_id"),
                symbol=lane.get("symbol"),
                sessions=lane.get("session_restriction"),
                bars=lane.get("bars_evaluated", 0),
                candidates=lane.get("setup_candidates_observed", 0),
                qualified=lane.get("setup_qualified_signals", 0),
                intents=lane.get("entry_intents", 0),
                blocked=lane.get("blocked_entry_intents", 0),
                fills=lane.get("fills", 0),
                post_blocked=lane.get("post_convergence_blocked_entry_intents", 0),
                dominant=lane.get("dominant_no_trade_reason"),
            )
        )
    research = report.get("research_participation", {})
    entry_acceptance = report.get("entry_acceptance_materialization", {})
    lines.extend(
        [
            "",
            "## Research Participation",
            f"- warehouse db: `{research.get('warehouse_db')}`",
            f"- available: `{research.get('available')}`",
            f"- raw family events: `{research.get('raw_family_events')}`",
            f"- lane candidates: `{research.get('lane_candidates')}`",
            f"- lane entries: `{research.get('lane_entries')}`",
            f"- closed trades: `{research.get('closed_trades')}`",
            f"- compact lanes: `{research.get('compact_lanes')}`",
            f"- zero-trade compact lanes: `{research.get('zero_trade_compact_lanes')}`",
            f"- candidate/raw retention: `{research.get('candidate_to_raw_retention')}`",
            "",
            "## Entry Acceptance Materialization",
            f"- root: `{entry_acceptance.get('root')}`",
            f"- available: `{entry_acceptance.get('available')}`",
            f"- rows scanned: `{entry_acceptance.get('rows_scanned')}`",
            f"- NEAR candidate count: `{entry_acceptance.get('near_candidate_count')}`",
            f"- exact-rule flags: `{entry_acceptance.get('exact_rule_flags')}`",
            f"- baseline episodes: `{entry_acceptance.get('baseline_episodes')}`",
            f"- NEAR >=0.80 episodes: `{entry_acceptance.get('near_gte_0_80_episodes')}`",
            f"- candidate retention: `{entry_acceptance.get('candidate_retention')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def write_report(report: Mapping[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "track_b_parity_audit_report.json"
    md_path = output_dir / "track_b_parity_audit_report.md"
    csv_path = output_dir / "track_b_parity_audit_lanes.csv"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    lane_rows = list(report.get("lanes", []) or [])
    fieldnames = [
        "lane_id",
        "symbol",
        "session_restriction",
        "bars_evaluated",
        "setup_candidates_observed",
        "setup_qualified_signals",
        "near_misses",
        "entry_intents",
        "blocked_entry_intents",
        "route_health_rejections",
        "governance_exposure_rejections",
        "broker_route_attempts",
        "successful_routed_intents",
        "fills",
        "post_convergence_entry_intents",
        "post_convergence_blocked_entry_intents",
        "post_convergence_fills",
        "pre_readiness_entry_intents",
        "restored_or_catchup_entry_intents",
        "dominant_no_trade_reason",
        "runtime_freshness_state",
        "runtime_freshness_root_cause",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in lane_rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return {"json": str(json_path), "markdown": str(md_path), "csv": str(csv_path)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    parser.add_argument("--dashboard-readiness", type=Path, default=DEFAULT_DASHBOARD_READINESS)
    parser.add_argument("--warehouse-db", type=Path, default=DEFAULT_WAREHOUSE_DB)
    parser.add_argument("--entry-acceptance-root", type=Path, default=DEFAULT_ENTRY_ACCEPTANCE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    output_dir = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    report = build_track_b_parity_audit(
        repo_root=repo_root,
        runtime_dir=args.runtime_dir,
        dashboard_readiness_path=args.dashboard_readiness,
        warehouse_db=args.warehouse_db,
        entry_acceptance_root=args.entry_acceptance_root,
    )
    paths = write_report(report, output_dir)
    print(json.dumps({"paths": paths, "verdict": report["verdict"], "runtime": report["runtime"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
