"""Read-only Track B operational certification reporter.

This module publishes an operator/O&M summary only. It must not become an
authority surface for runtime, Managed Exit, broker, or strategy behavior.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_OUTPUT_DIR = Path("outputs/track_b_execution_core/operations_maintenance/operational_certification")
DEFAULT_JSON = DEFAULT_OUTPUT_DIR / "latest_operational_certification.json"
DEFAULT_MD = DEFAULT_OUTPUT_DIR / "latest_operational_certification.md"
DEFAULT_EXPECTED_LANE_COUNT = 71
DEFAULT_FRESHNESS_SECONDS = 300.0
CAP_BYTES = 5 * 1024 * 1024


@dataclass(frozen=True)
class OperationalCertificationResult:
    json_path: Path
    md_path: Path
    report: dict[str, Any]
    markdown: str


def build_operational_certification(
    *,
    repo_root: Path = Path("."),
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    expected_lane_count: int = DEFAULT_EXPECTED_LANE_COUNT,
    freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
    now: datetime | None = None,
    write: bool = True,
) -> OperationalCertificationResult:
    actual_now = now or datetime.now(UTC)
    repo_root = Path(repo_root)
    contexts = _build_domains(
        repo_root=repo_root,
        now=actual_now,
        expected_lane_count=expected_lane_count,
        freshness_seconds=freshness_seconds,
    )
    overall = aggregate_operational_status(contexts)
    report = {
        "schema_version": "track_b_operational_certification_v1",
        "generated_at": actual_now.isoformat(),
        "classification": overall["classification"],
        "summary": overall,
        "domains": contexts,
        "read_only_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "cleanup_or_archive": False,
            "trading_gate": False,
        },
    }
    markdown = render_operational_certification_markdown(report)
    json_path = repo_root / output_dir / DEFAULT_JSON.name
    md_path = repo_root / output_dir / DEFAULT_MD.name
    if write:
        _write_json_atomic(json_path, report)
        _write_text_atomic(md_path, markdown)
    return OperationalCertificationResult(json_path=json_path, md_path=md_path, report=report, markdown=markdown)


def aggregate_operational_status(domains: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []
    for domain_name, domain in domains.items():
        for item in _list(domain.get("checks")):
            status = str(_mapping(item).get("status") or "").upper()
            severity = str(_mapping(item).get("severity") or "warning").lower()
            code = str(_mapping(item).get("code") or f"{domain_name}_check")
            if status == "FAIL" and severity == "critical":
                failures.append(code)
            elif status in {"FAIL", "WARN"}:
                warnings.append(code)
        domain_status = str(domain.get("status") or "").upper()
        if domain_status == "FAIL":
            failures.append(f"{domain_name}_domain_failed")
        elif domain_status == "WARN":
            warnings.append(f"{domain_name}_domain_warning")
    failures = sorted(dict.fromkeys(failures))
    warnings = sorted(dict.fromkeys(warnings))
    if failures:
        classification = "PLATFORM_NOT_CERTIFIED"
    elif warnings:
        classification = "PLATFORM_CERTIFIED_WITH_WARNINGS"
    else:
        classification = "PLATFORM_CERTIFIED"
    return {
        "classification": classification,
        "critical_failure_count": len(failures),
        "warning_count": len(warnings),
        "critical_failures": failures,
        "warnings": warnings,
    }


def render_operational_certification_markdown(report: Mapping[str, Any]) -> str:
    summary = _mapping(report.get("summary"))
    lines = [
        "# Track B Operational Certification",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- classification: `{report.get('classification')}`",
        f"- critical failures: `{summary.get('critical_failure_count')}`",
        f"- warnings: `{summary.get('warning_count')}`",
        "",
        "## Domain Summary",
        "",
        "| Domain | Status | Critical checks | Warning checks |",
        "|---|---:|---:|---:|",
    ]
    domains = _mapping(report.get("domains"))
    for name in sorted(domains):
        domain = _mapping(domains.get(name))
        checks = [_mapping(row) for row in _list(domain.get("checks"))]
        critical = len([row for row in checks if row.get("status") == "FAIL" and row.get("severity") == "critical"])
        warning = len([row for row in checks if row.get("status") in {"FAIL", "WARN"} and row.get("severity") != "critical"])
        lines.append(f"| `{name}` | `{domain.get('status')}` | `{critical}` | `{warning}` |")
    lines.extend(["", "## Key Findings", ""])
    for code in _list(summary.get("critical_failures")):
        lines.append(f"- critical: `{code}`")
    for code in _list(summary.get("warnings")):
        lines.append(f"- warning: `{code}`")
    if not summary.get("critical_failures") and not summary.get("warnings"):
        lines.append("- no findings")
    return "\n".join(lines) + "\n"


def _build_domains(
    *,
    repo_root: Path,
    now: datetime,
    expected_lane_count: int,
    freshness_seconds: float,
) -> dict[str, dict[str, Any]]:
    runtime_truth_path = Path("outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json")
    runtime_status_path = Path("outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_detached_child_status.json")
    runtime_progress_sources = _runtime_progress_sources(repo_root)
    managed_exit_path = Path("outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json")
    open_order_truth_path = Path("outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")
    position_truth_path = Path("outputs/track_b_execution_core/position_truth/latest_position_truth.json")
    managed_orders_path = Path("outputs/track_b_execution_core/managed_orders/latest_managed_orders.json")
    managed_positions_path = Path("outputs/track_b_execution_core/managed_positions/latest_managed_positions.json")
    safe_state_path = Path("outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json")
    guardian_path = Path("outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json")
    broker_lease_path = Path("outputs/operator_dashboard/runtime/latest_broker_truth_lease.json")

    runtime = _load_json(repo_root / runtime_truth_path)
    runtime_status = _load_json(repo_root / runtime_status_path)
    managed_exit = _load_json(repo_root / managed_exit_path)
    open_order_truth = _load_json(repo_root / open_order_truth_path)
    position_truth = _load_json(repo_root / position_truth_path)
    managed_orders = _load_json(repo_root / managed_orders_path)
    managed_positions = _load_json(repo_root / managed_positions_path)
    safe_state = _load_json(repo_root / safe_state_path)
    guardian = _load_json(repo_root / guardian_path)
    broker_lease = _load_json(repo_root / broker_lease_path)

    return {
        "runtime": _runtime_domain(
            runtime=runtime,
            runtime_status=runtime_status,
            runtime_truth_path=runtime_truth_path,
            runtime_status_path=runtime_status_path,
            runtime_progress_sources=runtime_progress_sources,
            now=now,
            freshness_seconds=freshness_seconds,
            expected_lane_count=expected_lane_count,
        ),
        "managed_exit": _managed_exit_domain(
            payload=managed_exit,
            source_path=managed_exit_path,
            now=now,
            freshness_seconds=freshness_seconds,
        ),
        "broker_orders": _broker_orders_domain(
            position_truth=position_truth,
            open_order_truth=open_order_truth,
            managed_orders=managed_orders,
            managed_positions=managed_positions,
            position_truth_path=position_truth_path,
            open_order_truth_path=open_order_truth_path,
            managed_orders_path=managed_orders_path,
            managed_positions_path=managed_positions_path,
            now=now,
            freshness_seconds=freshness_seconds,
        ),
        "safe_state_guardian": _safe_state_guardian_domain(
            safe_state=safe_state,
            guardian=guardian,
            safe_state_path=safe_state_path,
            guardian_path=guardian_path,
            now=now,
            freshness_seconds=freshness_seconds,
        ),
        "dmc": _dmc_domain(
            managed_orders=managed_orders,
            managed_positions=managed_positions,
            broker_lease=broker_lease,
            guardian=guardian,
            safe_state=safe_state,
            paths={
                "managed_orders": managed_orders_path,
                "managed_positions": managed_positions_path,
                "broker_truth_lease": broker_lease_path,
                "broker_position_guardian": guardian_path,
                "safe_state": safe_state_path,
            },
        ),
        "wmc": _wmc_domain(repo_root=repo_root, runtime_started_at=_parse_dt(runtime.get("runtime_started_at") or runtime.get("generated_at"))),
        "arc": _arc_domain(repo_root=repo_root),
        "storage_resources": _storage_resource_domain(repo_root=repo_root, runtime=runtime, managed_exit=managed_exit),
    }


def _runtime_domain(
    *,
    runtime: Mapping[str, Any],
    runtime_status: Mapping[str, Any],
    runtime_truth_path: Path,
    runtime_status_path: Path,
    runtime_progress_sources: list[dict[str, Any]],
    now: datetime,
    freshness_seconds: float,
    expected_lane_count: int,
) -> dict[str, Any]:
    pid = _int_or_none(runtime.get("producer_pid") or runtime.get("runtime_pid") or runtime.get("pid"))
    commit = runtime.get("source_commit") or runtime.get("commit")
    lane_count = _int_or_none(runtime.get("lane_count"))
    generated_at = _parse_dt(runtime.get("generated_at"))
    freshest_progress = _freshest_runtime_progress(runtime_progress_sources)
    progress_at = _parse_dt(freshest_progress.get("observed_at")) if freshest_progress else None
    checks = [
        _check("runtime_process_alive", _pid_alive(pid), "critical", {"pid": pid}),
        _check(
            "runtime_heartbeat_fresh",
            _fresh(progress_at, now, freshness_seconds),
            "critical",
            {"progress_source": freshest_progress},
        ),
        _check(
            "runtime_identity_fresh",
            _fresh(generated_at, now, freshness_seconds),
            "warning",
            {"generated_at": _iso(generated_at), "source_path": str(runtime_truth_path)},
        ),
        _check("runtime_commit_loaded", bool(commit), "critical", {"commit": commit}),
        _check("runtime_lane_count_expected", lane_count == expected_lane_count, "critical", {"lane_count": lane_count, "expected_lane_count": expected_lane_count}),
        _check("runtime_trading_loop_entered", _contains_value(runtime_status, "TRADING_LOOP_ENTERED") or _contains_value(runtime, "TRADING_LOOP_ENTERED"), "critical", {"source_path": str(runtime_status_path)}),
    ]
    return _domain(
        checks,
        source_artifacts=[
            _source(runtime_truth_path, runtime),
            _source(runtime_status_path, runtime_status),
            *[
                {
                    "path": str(item.get("path")),
                    "generated_at": item.get("generated_at"),
                    "mtime": item.get("mtime"),
                    "source_type": item.get("source_type"),
                }
                for item in runtime_progress_sources
            ],
        ],
        details={
            "pid": pid,
            "commit": commit,
            "lane_count": lane_count,
            "expected_lane_count": expected_lane_count,
            "identity_generated_at": _iso(generated_at),
            "progress_source": freshest_progress,
        },
    )


def _managed_exit_domain(
    *,
    payload: Mapping[str, Any],
    source_path: Path,
    now: datetime,
    freshness_seconds: float,
) -> dict[str, Any]:
    pid = _int_or_none(payload.get("pid") or payload.get("service_pid"))
    generated_at = _parse_dt(payload.get("generated_at"))
    classification = payload.get("classification")
    checks = [
        _check("managed_exit_process_alive", _pid_alive(pid), "critical", {"pid": pid}),
        _check("managed_exit_heartbeat_fresh", _fresh(generated_at, now, freshness_seconds), "critical", {"generated_at": _iso(generated_at)}),
        _check("managed_exit_not_wedged", not _looks_wedged(payload), "critical", {"classification": classification}),
    ]
    return _domain(
        checks,
        source_artifacts=[_source(source_path, payload)],
        details={
            "pid": pid,
            "classification": classification,
            "exit_due_count": payload.get("exit_due_count"),
            "eligible_count": payload.get("eligible_count"),
        },
    )


def _broker_orders_domain(
    *,
    position_truth: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_orders: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    position_truth_path: Path,
    open_order_truth_path: Path,
    managed_orders_path: Path,
    managed_positions_path: Path,
    now: datetime,
    freshness_seconds: float,
) -> dict[str, Any]:
    open_orders = _rows(open_order_truth, "open_orders", "broker_open_orders")
    unknown_count = _count(open_order_truth, "unknown_order_count", "unknown_broker_open_order_count")
    duplicate_groups = _rows(open_order_truth, "duplicate_close_order_groups")
    review_required = _count(managed_orders, "review_required_count") + _count(managed_positions, "review_required_count")
    live_money = _truthy_recursive([position_truth, open_order_truth, managed_orders, managed_positions], {"live_money", "live_money_eligible"})
    paper_proof = _truthy_recursive([position_truth, open_order_truth, managed_orders, managed_positions], {"paper_proof", "paper_proof_invoked"})
    open_generated_at = _parse_dt(open_order_truth.get("generated_at"))
    pos_generated_at = _parse_dt(position_truth.get("generated_at"))
    checks = [
        _check("broker_truth_fresh", _fresh(pos_generated_at, now, freshness_seconds), "critical", {"generated_at": _iso(pos_generated_at)}),
        _check("open_order_truth_global_complete", str(open_order_truth.get("canonical_refresh_scope") or "").upper() == "GLOBAL_COMPLETE", "critical", {"scope": open_order_truth.get("canonical_refresh_scope")}),
        _check("open_order_truth_fresh", _fresh(open_generated_at, now, freshness_seconds), "critical", {"generated_at": _iso(open_generated_at)}),
        _check("unknown_orders_zero", unknown_count == 0, "critical", {"unknown_order_count": unknown_count}),
        _check("duplicate_close_groups_zero", len(duplicate_groups) == 0, "critical", {"duplicate_close_group_count": len(duplicate_groups)}),
        _check("review_required_zero", review_required == 0, "critical", {"review_required_count": review_required}),
        _check("live_money_false", not live_money, "critical", {}),
        _check("paper_proof_false", not paper_proof, "critical", {}),
    ]
    return _domain(
        checks,
        source_artifacts=[
            _source(position_truth_path, position_truth),
            _source(open_order_truth_path, open_order_truth),
            _source(managed_orders_path, managed_orders),
            _source(managed_positions_path, managed_positions),
        ],
        details={
            "broker_positions": _summarize_positions(position_truth),
            "open_order_count": len(open_orders),
            "unknown_order_count": unknown_count,
            "duplicate_close_group_count": len(duplicate_groups),
            "review_required_count": review_required,
        },
    )


def _safe_state_guardian_domain(
    *,
    safe_state: Mapping[str, Any],
    guardian: Mapping[str, Any],
    safe_state_path: Path,
    guardian_path: Path,
    now: datetime,
    freshness_seconds: float,
) -> dict[str, Any]:
    safe_generated_at = _parse_dt(safe_state.get("generated_at"))
    guardian_generated_at = _parse_dt(guardian.get("generated_at"))
    hard = _rows(guardian, "hard_classifications")
    checks = [
        _check("safe_state_fresh", _fresh(safe_generated_at, now, freshness_seconds), "critical", {"generated_at": _iso(safe_generated_at)}),
        _check("safe_state_normal", safe_state.get("classification") == "SAFE_STATE_NORMAL", "critical", {"classification": safe_state.get("classification")}),
        _check("safe_state_submit_allowed", safe_state.get("submit_allowed") is True, "critical", {"submit_allowed": safe_state.get("submit_allowed")}),
        _check("safe_state_managed_close_allowed", safe_state.get("managed_close_mutation_allowed") is True, "critical", {"managed_close_mutation_allowed": safe_state.get("managed_close_mutation_allowed")}),
        _check("guardian_fresh", _fresh(guardian_generated_at, now, freshness_seconds), "critical", {"generated_at": _iso(guardian_generated_at)}),
        _check("guardian_ready", guardian.get("classification") == "BROKER_POSITION_GUARDIAN_READY", "critical", {"classification": guardian.get("classification")}),
        _check("guardian_hard_classifications_empty", len(hard) == 0, "critical", {"hard_classification_count": len(hard)}),
    ]
    return _domain(
        checks,
        source_artifacts=[_source(safe_state_path, safe_state), _source(guardian_path, guardian)],
        details={
            "safe_state_classification": safe_state.get("classification"),
            "submit_allowed": safe_state.get("submit_allowed"),
            "managed_close_mutation_allowed": safe_state.get("managed_close_mutation_allowed"),
            "guardian_classification": guardian.get("classification"),
            "hard_classifications": hard,
        },
    )


def _dmc_domain(
    *,
    managed_orders: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    broker_lease: Mapping[str, Any],
    guardian: Mapping[str, Any],
    safe_state: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    checks = [
        _check("dmc_managed_orders_distinguish_current_truth", _has_dmc_surface(managed_orders), "warning", {}),
        _check("dmc_managed_positions_distinguish_current_truth", _has_dmc_surface(managed_positions), "warning", {}),
        _check("dmc_broker_truth_lease_distinguish_current_truth", _has_dmc_surface(broker_lease), "warning", {}),
        _check("dmc_guardian_distinguish_current_truth", _has_dmc_surface(guardian), "warning", {}),
        _check("dmc_safe_state_active_authority_filter_visible", safe_state.get("classification") is not None, "warning", {}),
    ]
    return _domain(
        checks,
        source_artifacts=[_source(path, {}) for path in paths.values()],
        details={
            "managed_orders_current_truth_invalidation_present": "current_truth_invalidation" in managed_orders,
            "managed_positions_current_truth_invalidation_present": "current_truth_invalidation" in managed_positions,
            "broker_lease_current_truth_invalidation_present": "current_truth_invalidation" in broker_lease,
            "guardian_current_truth_invalidation_present": "current_truth_invalidation" in guardian,
        },
    )


def _wmc_domain(*, repo_root: Path, runtime_started_at: datetime | None) -> dict[str, Any]:
    families = [
        _wmc_family(
            repo_root,
            "operator_status.json",
            [
                repo_root / "outputs/probationary_pattern_engine/paper_session/operator_status.json",
                *(repo_root / "outputs/probationary_pattern_engine/paper_session/lanes").glob("*/operator_status.json"),
            ],
            runtime_started_at,
        ),
        _wmc_family(
            repo_root,
            "blocked_strategy_intent_latest.json",
            [
                repo_root / "outputs/probationary_pattern_engine/paper_session/blocked_strategy_intent_latest.json",
                *(repo_root / "outputs/probationary_pattern_engine/paper_session/lanes").glob("*/blocked_strategy_intent_latest.json"),
            ],
            runtime_started_at,
        ),
        _wmc_family(
            repo_root,
            "ibkr_paper_strategy_bridge_report.json",
            (repo_root / "outputs/reports/ibkr_runtime_route_dispatch").glob("*/ibkr_paper_strategy_bridge_report.json"),
            runtime_started_at,
        ),
        _wmc_family(repo_root, "strategy_probation_dashboard.json", [Path("outputs/reports/ibkr_strategy_governance/strategy_probation_dashboard.json"), Path("var/strategy_probation_dashboard.json")], runtime_started_at),
    ]
    checks = [
        _check(f"wmc_{fam['family']}_certified", fam["certification_status"] == "certified", "warning", fam)
        for fam in families
    ]
    return _domain(checks, source_artifacts=[], details={"families": families})


def _arc_domain(*, repo_root: Path) -> dict[str, Any]:
    arc_path = Path("outputs/track_b_execution_core/operations_maintenance/o5a_archive_retention_contract/archive_retention_contract.json")
    arc = _load_json(repo_root / arc_path)
    families = _list(arc.get("artifact_families"))
    eligible = [
        _mapping(row).get("artifact_family")
        for row in families
        if str(_mapping(row).get("current_archive_readiness") or "").startswith("candidate")
        and str(_mapping(row).get("certification_status") or "") == "certified"
    ]
    pending = [
        _mapping(row).get("artifact_family")
        for row in families
        if str(_mapping(row).get("certification_status") or "").startswith("awaiting")
        or str(_mapping(row).get("current_archive_readiness") or "").startswith("not_eligible")
    ]
    checks = [
        _check("arc_contract_present", bool(arc), "warning", {"source_path": str(arc_path)}),
        _check("arc_no_execution_required", True, "warning", {}),
    ]
    return _domain(
        checks,
        source_artifacts=[_source(arc_path, arc)],
        details={"archive_eligible_families": eligible, "pending_certification_families": pending},
    )


def _storage_resource_domain(*, repo_root: Path, runtime: Mapping[str, Any], managed_exit: Mapping[str, Any]) -> dict[str, Any]:
    usage = shutil.disk_usage(repo_root)
    o1_path = Path("outputs/track_b_execution_core/operations_maintenance/phase_o1_hot_path_storage_audit/operations_maintenance_hot_path_audit.json")
    o1 = _load_json(repo_root / o1_path)
    runtime_pid = _int_or_none(runtime.get("producer_pid") or runtime.get("runtime_pid"))
    managed_exit_pid = _int_or_none(managed_exit.get("pid") or managed_exit.get("service_pid"))
    details = {
        "disk_free_bytes": usage.free,
        "disk_total_bytes": usage.total,
        "disk_free_percent": round((usage.free / usage.total) * 100, 3) if usage.total else None,
        "outputs_size_from_o1": _nested_get(o1, "sizes", "outputs") or _nested_get(o1, "storage", "outputs_size"),
        "largest_hot_path_files_from_o1_present": bool(o1),
        "runtime_rss_kb": _rss_kb(runtime_pid),
        "managed_exit_rss_kb": _rss_kb(managed_exit_pid),
    }
    checks = [
        _check("disk_free_above_5_percent", (details["disk_free_percent"] or 0) > 5.0, "warning", {"disk_free_percent": details["disk_free_percent"]}),
        _check("o1_report_available", bool(o1), "warning", {"source_path": str(o1_path)}),
    ]
    return _domain(checks, source_artifacts=[_source(o1_path, o1)], details=details)


def _wmc_family(repo_root: Path, family: str, paths: Iterable[Path], runtime_started_at: datetime | None) -> dict[str, Any]:
    existing = []
    for rel in paths:
        path = repo_root / rel
        if path.exists():
            existing.append(path)
    if not existing:
        return {"family": family, "certification_status": "pending", "reason": "no matching artifact found"}
    newest = max(existing, key=lambda p: p.stat().st_mtime)
    newest_stat = newest.stat()
    after_reload = runtime_started_at is not None and datetime.fromtimestamp(newest_stat.st_mtime, UTC) >= runtime_started_at
    under_cap = newest_stat.st_size <= CAP_BYTES
    sidecar = newest.with_name(f"{newest.stem}_bounded_snapshot_diagnostic.json")
    if family == "strategy_probation_dashboard.json":
        certified = after_reload and under_cap
    else:
        certified = after_reload and under_cap
    return {
        "family": family,
        "certification_status": "certified" if certified else "pending",
        "newest_path": str(newest.relative_to(repo_root)),
        "newest_size_bytes": newest_stat.st_size,
        "newest_mtime": datetime.fromtimestamp(newest_stat.st_mtime, UTC).isoformat(),
        "after_runtime_reload": after_reload,
        "under_5mib_cap": under_cap,
        "diagnostic_sidecar_exists": sidecar.exists(),
        "cap_bytes": CAP_BYTES,
    }


def _runtime_progress_sources(repo_root: Path) -> list[dict[str, Any]]:
    candidates = [
        ("runtime_log", Path("outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.log")),
        ("live_timing_summary", Path("outputs/probationary_pattern_engine/paper_session/live_timing_summary_latest.json")),
        ("blocked_strategy_intent_latest", Path("outputs/probationary_pattern_engine/paper_session/blocked_strategy_intent_latest.json")),
        ("filled_bridge_results", Path("outputs/probationary_pattern_engine/paper_session/filled_bridge_results.jsonl")),
        ("filled_bridge_result_latest", Path("outputs/probationary_pattern_engine/paper_session/filled_bridge_result_latest.json")),
        ("runtime_cycle", Path("outputs/probationary_pattern_engine/paper_session/runtime/latest_runtime_cycle.json")),
        ("runtime_cycle", Path("outputs/probationary_pattern_engine/paper_session/runtime/runtime_cycle_latest.json")),
    ]
    sources: list[dict[str, Any]] = []
    for source_type, rel_path in candidates:
        path = repo_root / rel_path
        if not path.exists() or not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        mtime = datetime.fromtimestamp(stat.st_mtime, UTC)
        generated_at = None
        if path.suffix == ".json" and stat.st_size <= CAP_BYTES:
            payload = _load_json(path)
            generated_at = _parse_dt(payload.get("generated_at") or payload.get("latest_record_at"))
        observed_at = generated_at or mtime
        sources.append(
            {
                "path": str(rel_path),
                "source_type": source_type,
                "observed_at": observed_at.isoformat(),
                "generated_at": _iso(generated_at),
                "mtime": mtime.isoformat(),
                "size_bytes": stat.st_size,
            }
        )
    return sources


def _freshest_runtime_progress(sources: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    freshest: dict[str, Any] = {}
    freshest_at: datetime | None = None
    for source in sources:
        observed_at = _parse_dt(source.get("observed_at"))
        if observed_at is None:
            continue
        if freshest_at is None or observed_at > freshest_at:
            freshest_at = observed_at
            freshest = dict(source)
    return freshest


def _domain(checks: list[dict[str, Any]], *, source_artifacts: list[dict[str, Any]], details: dict[str, Any]) -> dict[str, Any]:
    if any(row["status"] == "FAIL" and row["severity"] == "critical" for row in checks):
        status = "FAIL"
    elif any(row["status"] in {"FAIL", "WARN"} for row in checks):
        status = "WARN"
    else:
        status = "PASS"
    return {"status": status, "checks": checks, "source_artifacts": source_artifacts, "details": details}


def _check(code: str, passed: bool, severity: str, details: Mapping[str, Any]) -> dict[str, Any]:
    return {"code": code, "status": "PASS" if passed else "FAIL", "severity": severity, "details": dict(details)}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _source(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {"path": str(path), "generated_at": payload.get("generated_at"), "classification": payload.get("classification")}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _rows(payload: Mapping[str, Any], *keys: str) -> list[Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _count(payload: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, int):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    rows = _rows(payload, *keys)
    return len(rows)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _fresh(value: datetime | None, now: datetime, freshness_seconds: float) -> bool:
    if value is None:
        return False
    return max(0.0, (now.astimezone(UTC) - value.astimezone(UTC)).total_seconds()) <= freshness_seconds


def _pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _rss_kb(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True, timeout=2)
    except Exception:
        return None
    return _int_or_none(out.strip())


def _contains_value(value: Any, needle: str) -> bool:
    if value == needle:
        return True
    if isinstance(value, Mapping):
        return any(_contains_value(child, needle) for child in value.values())
    if isinstance(value, list):
        return any(_contains_value(child, needle) for child in value)
    return False


def _truthy_recursive(payloads: Iterable[Any], keys: set[str]) -> bool:
    for payload in payloads:
        if _truthy_recursive_one(payload, keys):
            return True
    return False


def _truthy_recursive_one(value: Any, keys: set[str]) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in keys and child is True:
                return True
            if _truthy_recursive_one(child, keys):
                return True
    elif isinstance(value, list):
        return any(_truthy_recursive_one(child, keys) for child in value)
    return False


def _looks_wedged(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or "")
    return "STALE" in classification or "WEDGED" in classification


def _has_dmc_surface(payload: Mapping[str, Any]) -> bool:
    return "current_truth_invalidation" in payload or "dmc_metadata" in payload or "source_freshness" in payload


def _summarize_positions(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _rows(payload, "positions", "broker_positions")
    out = []
    for row in rows:
        item = _mapping(row)
        out.append(
            {
                "symbol": item.get("local_symbol") or item.get("symbol"),
                "qty": item.get("quantity") or item.get("signed_qty") or item.get("qty"),
                "account_id": item.get("account_id"),
            }
        )
    return out


def _nested_get(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-operational-certification")
    parser.add_argument("--repo-root", default=".", help="Repository root to read artifacts from.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory, relative to repo root unless absolute.")
    parser.add_argument("--expected-lane-count", type=int, default=DEFAULT_EXPECTED_LANE_COUNT)
    parser.add_argument("--freshness-seconds", type=float, default=DEFAULT_FRESHNESS_SECONDS)
    parser.add_argument("--json", action="store_true", help="Print the full JSON report.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser()
    if output_dir.is_absolute():
        output_dir = output_dir.relative_to(repo_root) if str(output_dir).startswith(str(repo_root)) else output_dir
    result = build_operational_certification(
        repo_root=repo_root,
        output_dir=output_dir,
        expected_lane_count=int(args.expected_lane_count),
        freshness_seconds=float(args.freshness_seconds),
    )
    if args.json:
        print(json.dumps(result.report, indent=2, sort_keys=True))
    else:
        print(result.markdown)
    return 0 if result.report.get("classification") != "PLATFORM_NOT_CERTIFIED" else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
