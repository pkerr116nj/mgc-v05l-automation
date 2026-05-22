"""Dry-run weekly data maintenance classifier for Track B artifacts.

The v1 maintenance command is intentionally report-only. It classifies HOT
runtime data, WARM evidence/state, COLD archive candidates, deferred research,
and disposable build metadata without deleting, moving, or treating archives as
runtime truth.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER
from mgc_v05l.paths import ARCHIVED_ROOT_FRAGMENTS, PROJECT_ROOT

REPO_ROOT = PROJECT_ROOT
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "weekly_data_maintenance"
DEFAULT_ARCHIVE_STAGING_ROOT = PROJECT_ROOT.parent / "_mgc_v05l_archive_staging"
DEFAULT_HOT_ROOT = Path("var") / "runtime_market_data"
DEFAULT_WARM_ROOT = Path("outputs") / "reports"
ACTIVE_RUNTIME_ROOTS = (
    Path("outputs") / "track_b_execution_core",
    Path("outputs") / "operator_dashboard",
    Path("outputs") / "probationary_pattern_engine",
    Path("var"),
)
OLD_ROOT_PATTERNS = ARCHIVED_ROOT_FRAGMENTS
RUNTIME_PROCESS_PATTERNS = (
    "probationary-paper-soak",
    "run_probationary_paper_soak",
    "paper_strategy_monitor",
    "track_b_databento_live_runtime_feed",
    "track_b_shadow_monitor",
    "operator_dashboard",
)
BROKER_EVIDENCE_TERMS = (
    "broker",
    "order",
    "fill",
    "reconciliation",
    "reconcile",
    "review_required",
    "manual_close",
    "ibkr",
    "ledger",
)


@dataclass(frozen=True)
class WeeklyMaintenanceConfig:
    repo_root: Path = REPO_ROOT
    mode: str = "dry-run"
    week_ending: date | None = None
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    output_root: Path = DEFAULT_OUTPUT_ROOT
    archive_staging_root: Path = DEFAULT_ARCHIVE_STAGING_ROOT
    hot_root: Path = DEFAULT_HOT_ROOT
    warm_root: Path = DEFAULT_WARM_ROOT
    detail_limit: int = 50
    include_full_paths: bool = False
    category_filter: tuple[str, ...] = ()
    confirm_disposable_build_cleanup: bool = False


def build_weekly_data_maintenance_report(*, config: WeeklyMaintenanceConfig) -> dict[str, Any]:
    if config.mode not in {"dry-run", "apply"}:
        return _apply_not_implemented_report(config=config)

    repo_root = Path(config.repo_root)
    week_end = config.week_ending or date.today()
    week_start = week_end - timedelta(days=6)
    hot_roots = _runtime_roots(config=config)
    warm_root = _resolve(repo_root, config.warm_root)

    active_processes = _detect_active_runtime_processes()
    stale_root_hits = _detect_old_root_hits(repo_root=repo_root)
    inspected_paths = _collect_candidate_paths(repo_root=repo_root, hot_roots=hot_roots, warm_root=warm_root)
    all_classifications = [_classify_path(path=path, repo_root=repo_root, hot_roots=hot_roots, warm_root=warm_root, week_start=week_start, week_end=week_end) for path in inspected_paths]
    classifications = _filter_classifications(all_classifications, config.category_filter)

    delete_candidates = [row for row in classifications if row["classification"] == "DISPOSABLE_BUILD"]
    archive_candidates = [row for row in classifications if row["classification"] == "COLD_ARCHIVE_CANDIDATE"]
    preserve_candidates = [row for row in classifications if row["classification"] in {"HOT_DECISION_RUNTIME_DATA", "WARM_EVIDENCE_STATE"}]
    deferred_research = [row for row in classifications if row["classification"] == "DEFERRED_RESEARCH_OFFLINE"]

    review_required = bool(stale_root_hits)
    runtime_active = bool(active_processes)
    final_verdict = (
        "DRY_RUN_ONLY_REVIEW_REQUIRED"
        if review_required
        else "DRY_RUN_ONLY_RUNTIME_ACTIVE"
        if runtime_active
        else "DRY_RUN_ONLY_READY"
    )
    report = {
        "schema_version": "weekly_data_maintenance_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": config.mode,
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "symbols": list(config.symbols),
        "phase1_symbol_count": len(config.symbols),
        "hot_runtime_data_paths_inspected": [str(path) for path in hot_roots],
        "warm_evidence_paths_inspected": [str(warm_root)],
        "cold_archive_staging_root": str(config.archive_staging_root),
        "delete_candidates_count": len(delete_candidates),
        "archive_candidates_count": len(archive_candidates),
        "preserve_candidates_count": len(preserve_candidates),
        "deferred_research_count": len(deferred_research),
        "total_classified_count": len(classifications),
        "total_classified_before_filter": len(all_classifications),
        "category_filter": list(config.category_filter),
        "detail_limit": max(0, int(config.detail_limit)),
        "include_full_paths": config.include_full_paths,
        "runtime_active": runtime_active,
        "active_runtime_processes": active_processes,
        "review_required": review_required,
        "old_root_hits": stale_root_hits,
        "final_verdict": final_verdict,
        "policy": {
            "dry_run_only": True,
            "delete_files": False,
            "move_files": False,
            "broker_mutation": False,
            "cold_archive_runtime_truth": False,
        },
        "summary_by_classification": _summary_by_classification(classifications),
        "summary_by_retention_tier": _summary_by_key(classifications, "retention_tier"),
        "summary_by_top_level_directory": _summary_by_key(classifications, "top_level_directory"),
        "summary_by_age_bucket": _summary_by_key(classifications, "age_bucket"),
        "summary_by_reason": _summary_by_key(classifications, "reason_key"),
        "detail_lists": _detail_lists(
            classifications,
            detail_limit=max(0, int(config.detail_limit)),
            include_full_paths=config.include_full_paths,
        ),
    }
    if config.include_full_paths:
        report["classifications"] = classifications
    if config.mode == "apply":
        _apply_disposable_build_cleanup(
            report=report,
            classifications=classifications,
            repo_root=repo_root,
            config=config,
        )
    else:
        _assert_safety_invariants(classifications)
    return report


def write_weekly_data_maintenance_report(*, config: WeeklyMaintenanceConfig, report: dict[str, Any]) -> dict[str, Path]:
    output_root = _resolve(Path(config.repo_root), config.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    week = str(report.get("week_end") or "unknown")
    latest_json = output_root / "latest_weekly_data_maintenance_report.json"
    weekly_md = output_root / f"weekly_data_maintenance_{week}.md"
    latest_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    weekly_md.write_text(_render_markdown(report), encoding="utf-8")
    return {"json": latest_json, "markdown": weekly_md}


def _apply_not_implemented_report(*, config: WeeklyMaintenanceConfig) -> dict[str, Any]:
    return {
        "schema_version": "weekly_data_maintenance_v1",
        "mode": config.mode,
        "final_verdict": "APPLY_MODE_NOT_IMPLEMENTED",
        "review_required": True,
        "error": "APPLY_MODE_NOT_IMPLEMENTED",
    }


def _apply_disposable_build_cleanup(
    *,
    report: dict[str, Any],
    classifications: Sequence[dict[str, Any]],
    repo_root: Path,
    config: WeeklyMaintenanceConfig,
) -> None:
    report["policy"]["dry_run_only"] = False
    report["apply_mode"] = {
        "allowed_scope": "DISPOSABLE_BUILD_ONLY",
        "confirmation_present": config.confirm_disposable_build_cleanup,
        "deleted_count": 0,
        "deleted_paths": [],
        "skipped_count": 0,
        "skipped_paths": [],
    }
    category_filter = tuple(item.strip().upper() for item in config.category_filter if item.strip())
    blockers: list[str] = []
    if category_filter != ("DISPOSABLE_BUILD",):
        blockers.append("CATEGORY_FILTER_MUST_EQUAL_DISPOSABLE_BUILD")
    if not config.confirm_disposable_build_cleanup:
        blockers.append("CONFIRM_DISPOSABLE_BUILD_CLEANUP_REQUIRED")

    delete_candidates = [row for row in classifications if row.get("classification") == "DISPOSABLE_BUILD"]
    skipped_paths: list[dict[str, str]] = []
    for row in delete_candidates:
        safety_error = _disposable_apply_safety_error(row=row, repo_root=repo_root)
        if safety_error:
            skipped_paths.append({"path": str(row.get("path") or ""), "reason": safety_error})
    if skipped_paths:
        blockers.append("UNSAFE_DELETE_CANDIDATES_PRESENT")

    if blockers:
        report["final_verdict"] = "APPLY_DISPOSABLE_BUILD_BLOCKED"
        report["review_required"] = True
        report["blocking_reasons"] = blockers
        report["apply_mode"]["skipped_count"] = len(delete_candidates)
        report["apply_mode"]["skipped_paths"] = skipped_paths or [
            {"path": str(row.get("path") or ""), "reason": "APPLY_PRECONDITION_BLOCKED"}
            for row in delete_candidates
        ]
        return

    deleted_paths: list[str] = []
    deletion_failures: list[dict[str, str]] = []
    for row in delete_candidates:
        relative = Path(str(row.get("path") or ""))
        target = repo_root / relative
        try:
            if target.is_dir():
                shutil.rmtree(target)
            elif target.exists():
                target.unlink()
            else:
                deletion_failures.append({"path": str(relative), "reason": "PATH_MISSING_AT_DELETE_TIME"})
                continue
        except Exception as exc:  # pragma: no cover - platform/filesystem defensive path
            deletion_failures.append({"path": str(relative), "reason": f"DELETE_FAILED: {exc}"})
            continue
        deleted_paths.append(str(relative))

    report["apply_mode"]["deleted_count"] = len(deleted_paths)
    report["apply_mode"]["deleted_paths"] = deleted_paths
    report["apply_mode"]["skipped_count"] = len(deletion_failures)
    report["apply_mode"]["skipped_paths"] = deletion_failures
    if deletion_failures:
        report["final_verdict"] = "APPLY_DISPOSABLE_BUILD_BLOCKED"
        report["review_required"] = True
        report["blocking_reasons"] = ["DELETE_FAILURES_PRESENT"]
        return

    report["final_verdict"] = "APPLY_DISPOSABLE_BUILD_COMPLETE"
    report["review_required"] = False
    report["policy"]["delete_files"] = True


def _disposable_apply_safety_error(*, row: dict[str, Any], repo_root: Path) -> str | None:
    relative = Path(str(row.get("path") or ""))
    path_text = str(relative).lower()
    parts = tuple(part.lower() for part in relative.parts)
    suffix = relative.suffix.lower()
    if row.get("classification") != "DISPOSABLE_BUILD":
        return "NOT_DISPOSABLE_BUILD_CLASSIFICATION"
    if row.get("retention_tier") != "DISPOSABLE_BUILD":
        return "NOT_DISPOSABLE_BUILD_TIER"
    if row.get("reason_key") != "disposable_build_metadata":
        return "NOT_DISPOSABLE_BUILD_REASON"
    if relative.is_absolute() or ".." in relative.parts:
        return "UNSAFE_PATH_SHAPE"
    if not (repo_root / relative).exists():
        return "PATH_MISSING"
    if parts and parts[0] in {"var", "outputs", "docs", "examples"}:
        return "PROTECTED_TOP_LEVEL_PATH"
    if "research" in parts:
        return "RESEARCH_PATH_PROTECTED"
    if suffix in {".py", ".json", ".jsonl", ".yaml", ".yml", ".toml", ".md", ".csv", ".sqlite", ".sqlite3", ".db"}:
        return "SOURCE_CONFIG_REPORT_OR_DATA_FILE_PROTECTED"
    if any(term in path_text for term in BROKER_EVIDENCE_TERMS):
        return "BROKER_OR_REVIEW_EVIDENCE_PROTECTED"
    target = repo_root / relative
    if target.is_dir() and (
        target.name == "__pycache__"
        or target.name == ".pytest_cache"
        or target.name.endswith(".egg-info")
        or any(part == ".pytest_cache" for part in parts)
    ):
        return None
    if target.is_file() and (suffix == ".pyc" or "__pycache__" in parts or ".pytest_cache" in parts):
        return None
    return "NOT_RECOGNIZED_DISPOSABLE_BUILD_METADATA"


def _runtime_roots(*, config: WeeklyMaintenanceConfig) -> tuple[Path, ...]:
    repo_root = Path(config.repo_root)
    roots = [_resolve(repo_root, config.hot_root)]
    roots.extend(_resolve(repo_root, root) for root in ACTIVE_RUNTIME_ROOTS)
    return tuple(dict.fromkeys(roots))


def _collect_candidate_paths(*, repo_root: Path, hot_roots: Sequence[Path], warm_root: Path) -> list[Path]:
    roots: list[Path] = list(hot_roots) + [warm_root, repo_root / "docs", repo_root / "src" / "mgc_v05l" / "app", repo_root / "src" / "mgc_v05l" / "research", repo_root / "examples" / "track_b_shadow_listener"]
    roots.extend(repo_root.glob("src/*.egg-info"))
    roots.extend([repo_root / ".pytest_cache", repo_root / "src" / "mgc_v05l" / "app" / "__pycache__"])
    paths: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        if not root.exists():
            continue
        if root.is_file():
            paths.append(root)
            continue
        if _is_disposable_path(root, repo_root=repo_root):
            paths.append(root)
            continue
        for child in root.rglob("*"):
            if child.is_file():
                paths.append(child)
    return _collapse_disposable_children(sorted(dict.fromkeys(paths)), repo_root=repo_root)


def _classify_path(*, path: Path, repo_root: Path, hot_roots: Sequence[Path], warm_root: Path, week_start: date, week_end: date) -> dict[str, Any]:
    relative = _relative(path, repo_root)
    classification = "UNKNOWN_REVIEW"
    retention_tier = "PRESERVE"
    reason_key = "unknown_preserve"
    reason = "No policy bucket matched."
    action = "review"

    if any(_is_relative_to(path, root) for root in hot_roots):
        classification = "HOT_DECISION_RUNTIME_DATA"
        retention_tier = "HOT"
        reason_key = "active_runtime_state"
        reason = "Active runtime/HOT data root; never delete in dry-run maintenance."
        action = "preserve"
    elif _is_deferred_research_path(relative):
        classification = "DEFERRED_RESEARCH_OFFLINE"
        retention_tier = "DEFERRED_RESEARCH"
        reason_key = "deferred_research"
        reason = "Research/offline path; classify only until archive server policy exists."
        action = "defer"
    elif _is_disposable_path(path, repo_root=repo_root):
        classification = "DISPOSABLE_BUILD"
        retention_tier = "DISPOSABLE_BUILD"
        reason_key = "disposable_build_metadata"
        reason = "Rebuildable cache/build metadata."
        action = "delete_candidate_dry_run_only"
    elif _is_relative_to(path, warm_root):
        lower = str(relative).lower()
        if any(term in lower for term in BROKER_EVIDENCE_TERMS) or "preflight" in lower or "cleanup" in lower:
            classification = "COLD_ARCHIVE_CANDIDATE"
            retention_tier = "COLD_ARCHIVE_CANDIDATE"
            reason_key = "broker_or_review_evidence" if any(term in lower for term in BROKER_EVIDENCE_TERMS) else "archive_candidate"
            reason = "WARM evidence/state that should be staged later, not moved in v1."
            action = "archive_candidate_dry_run_only"
        else:
            classification = "WARM_EVIDENCE_STATE"
            retention_tier = "WARM"
            reason_key = "current_week_data" if _mtime_date_in_week(path, week_start=week_start, week_end=week_end) else "unknown_preserve"
            reason = "WARM report/dashboard/operator state; preserve under rolling retention."
            action = "preserve"

    if _is_broker_or_review_evidence(relative) and classification == "DISPOSABLE_BUILD":
        classification = "WARM_EVIDENCE_STATE"
        retention_tier = "WARM"
        reason_key = "broker_or_review_evidence"
        reason = "Broker/review evidence may never be a delete candidate."
        action = "preserve"

    stat_info = _safe_stat(path)
    size_bytes = stat_info.st_size if stat_info is not None and path.is_file() else 0
    mtime = (
        datetime.fromtimestamp(stat_info.st_mtime, tz=timezone.utc)
        if stat_info is not None
        else None
    )
    return {
        "path": str(relative),
        "classification": classification,
        "retention_tier": retention_tier,
        "top_level_directory": _top_level_directory(relative),
        "age_bucket": _age_bucket(mtime),
        "reason_key": reason_key,
        "reason": reason,
        "recommended_action": action,
        "size_bytes": size_bytes,
        "mtime": mtime.isoformat() if mtime is not None else None,
    }


def _assert_safety_invariants(rows: Sequence[dict[str, Any]]) -> None:
    for row in rows:
        path = str(row.get("path") or "").lower()
        if row.get("classification") == "DISPOSABLE_BUILD" and any(term in path for term in BROKER_EVIDENCE_TERMS):
            raise AssertionError(f"broker/review evidence classified as disposable: {row}")
        if row.get("classification") == "DISPOSABLE_BUILD" and (
            path.startswith("var/") or path.startswith("outputs/track_b_execution_core/")
        ):
            raise AssertionError(f"active runtime path classified as disposable: {row}")


def _summary_by_classification(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    return _summary_by_key(rows, "classification")


def _summary_by_key(rows: Sequence[dict[str, Any]], key: str) -> dict[str, int]:
    summary: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        summary[value] = summary.get(value, 0) + 1
    return dict(sorted(summary.items()))


def _filter_classifications(rows: Sequence[dict[str, Any]], category_filter: Sequence[str]) -> list[dict[str, Any]]:
    filters = {item.strip().upper() for item in category_filter if item.strip()}
    if not filters:
        return list(rows)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        values = {
            str(row.get("classification") or "").upper(),
            str(row.get("retention_tier") or "").upper(),
            str(row.get("reason_key") or "").upper(),
            str(row.get("top_level_directory") or "").upper(),
        }
        if values & filters:
            filtered.append(row)
    return filtered


def _detail_lists(
    rows: Sequence[dict[str, Any]],
    *,
    detail_limit: int,
    include_full_paths: bool,
) -> dict[str, list[dict[str, Any]]]:
    preserve_rows = [
        row for row in rows if row.get("classification") in {"HOT_DECISION_RUNTIME_DATA", "WARM_EVIDENCE_STATE", "UNKNOWN_REVIEW"}
    ]
    stale_warm = [
        row
        for row in rows
        if row.get("classification") == "WARM_EVIDENCE_STATE" and row.get("age_bucket") in {"31-90d", ">90d"}
    ]
    return {
        "top_delete_candidates": _cap_details(
            [row for row in rows if row.get("classification") == "DISPOSABLE_BUILD"],
            detail_limit=detail_limit,
            include_full_paths=include_full_paths,
        ),
        "top_archive_candidates": _cap_details(
            [row for row in rows if row.get("classification") == "COLD_ARCHIVE_CANDIDATE"],
            detail_limit=detail_limit,
            include_full_paths=include_full_paths,
        ),
        "largest_preserve_candidates": _cap_details(
            sorted(preserve_rows, key=lambda row: int(row.get("size_bytes") or 0), reverse=True),
            detail_limit=detail_limit,
            include_full_paths=include_full_paths,
        ),
        "stale_warm_candidates": _cap_details(
            sorted(stale_warm, key=lambda row: str(row.get("mtime") or "")),
            detail_limit=detail_limit,
            include_full_paths=include_full_paths,
        ),
    }


def _cap_details(
    rows: Sequence[dict[str, Any]],
    *,
    detail_limit: int,
    include_full_paths: bool,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for row in rows[:detail_limit]:
        detail = {
            "path_hint": Path(str(row.get("path") or "")).name,
            "top_level_directory": row.get("top_level_directory"),
            "classification": row.get("classification"),
            "retention_tier": row.get("retention_tier"),
            "reason_key": row.get("reason_key"),
            "age_bucket": row.get("age_bucket"),
            "size_bytes": row.get("size_bytes"),
            "recommended_action": row.get("recommended_action"),
        }
        if include_full_paths:
            detail["path"] = row.get("path")
        details.append(detail)
    return details


def _detect_active_runtime_processes() -> list[str]:
    try:
        completed = subprocess.run(["ps", "-ef"], text=True, capture_output=True, check=False, timeout=10)
    except Exception:
        return []
    if completed.returncode != 0:
        return []
    hits: list[str] = []
    for line in completed.stdout.splitlines():
        if any(pattern in line for pattern in RUNTIME_PROCESS_PATTERNS):
            hits.append(line)
    return hits


def _safe_stat(path: Path):
    try:
        return path.stat()
    except OSError:
        return None


def _mtime_date_in_week(path: Path, *, week_start: date, week_end: date) -> bool:
    stat_info = _safe_stat(path)
    if stat_info is None:
        return False
    mtime_date = datetime.fromtimestamp(stat_info.st_mtime, tz=timezone.utc).date()
    return week_start <= mtime_date <= week_end


def _age_bucket(mtime: datetime | None) -> str:
    if mtime is None:
        return "unknown"
    age_days = max(0, int((datetime.now(timezone.utc) - mtime).total_seconds() // 86400))
    if age_days <= 1:
        return "0-1d"
    if age_days <= 7:
        return "2-7d"
    if age_days <= 30:
        return "8-30d"
    if age_days <= 90:
        return "31-90d"
    return ">90d"


def _top_level_directory(relative: Path) -> str:
    if not relative.parts:
        return "other"
    top = relative.parts[0]
    return top if top in {"var", "outputs", "docs", "src", "tests", "examples"} else "other"


def _detect_old_root_hits(*, repo_root: Path) -> list[str]:
    hits: list[str] = []
    hits.extend(line for line in _detect_active_runtime_processes() if any(pattern in line for pattern in OLD_ROOT_PATTERNS))
    for root in (repo_root / "config", repo_root / "scripts", repo_root / "src"):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            relative = _relative(path, repo_root)
            if _is_disposable_path(path, repo_root=repo_root) or _is_deferred_research_path(relative):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                if not any(pattern in line for pattern in OLD_ROOT_PATTERNS):
                    continue
                lowered = line.lower()
                if "old_root_patterns" in lowered or "old-root" in lowered or "no_old_root" in lowered or "refuse" in lowered:
                    continue
                if any(pattern in line for pattern in RUNTIME_PROCESS_PATTERNS) or "launchagent" in lowered or ".plist" in lowered:
                    hits.append(f"{relative}:{line_number}")
    return sorted(dict.fromkeys(hits))


def _is_deferred_research_path(relative: Path) -> bool:
    text = str(relative)
    name = relative.name
    return (
        text.startswith("docs/atp_")
        or text.startswith("docs/us_open_")
        or text.startswith("src/mgc_v05l/app/atp_")
        or text.startswith("src/mgc_v05l/app/us_open_")
        or text.startswith("src/mgc_v05l/research/")
        or text.startswith("examples/track_b_shadow_listener/")
        or name.startswith("atp_")
        or name.startswith("us_open_")
    )


def _is_broker_or_review_evidence(relative: Path) -> bool:
    text = str(relative).lower()
    return any(term in text for term in BROKER_EVIDENCE_TERMS)


def _is_disposable_path(path: Path, *, repo_root: Path) -> bool:
    relative = _relative(path, repo_root)
    parts = set(relative.parts)
    return (
        path.suffix == ".pyc"
        or "__pycache__" in parts
        or ".pytest_cache" in parts
        or any(part.endswith(".egg-info") for part in relative.parts)
    )


def _collapse_disposable_children(paths: Sequence[Path], *, repo_root: Path) -> list[Path]:
    collapsed: list[Path] = []
    disposable_roots: list[Path] = []
    for path in paths:
        if any(_is_relative_to(path, root) and path != root for root in disposable_roots):
            continue
        collapsed.append(path)
        if path.is_dir() and _is_disposable_path(path, repo_root=repo_root):
            disposable_roots.append(path)
    return collapsed


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _relative(path: Path, repo_root: Path) -> Path:
    try:
        return path.relative_to(repo_root)
    except ValueError:
        return path


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(symbol.strip().upper() for symbol in value.split(",") if symbol.strip())
    return symbols or PHASE1_RUNTIME_TICKER_ORDER


def _parse_category_filter(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _parse_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weekly Data Maintenance Dry Run",
        "",
        f"- week_start: `{report.get('week_start')}`",
        f"- week_end: `{report.get('week_end')}`",
        f"- symbols: `{', '.join(report.get('symbols', []))}`",
        f"- final_verdict: `{report.get('final_verdict')}`",
        f"- runtime_active: `{report.get('runtime_active')}`",
        f"- review_required: `{report.get('review_required')}`",
        "",
        "## Counts",
        "",
        f"- preserve_candidates_count: `{report.get('preserve_candidates_count')}`",
        f"- archive_candidates_count: `{report.get('archive_candidates_count')}`",
        f"- deferred_research_count: `{report.get('deferred_research_count')}`",
        f"- delete_candidates_count: `{report.get('delete_candidates_count')}`",
        "",
        "## Policy",
        "",
        "- v1 deletes nothing.",
        "- v1 moves nothing.",
        "- Cold archive staging is not runtime truth.",
        "- Research/offline artifacts are classified only.",
        "",
        "## Summary By Classification",
        "",
    ]
    for key, count in report.get("summary_by_classification", {}).items():
        lines.append(f"- {key}: `{count}`")
    lines.extend(["", "## Summary By Retention Tier", ""])
    for key, count in report.get("summary_by_retention_tier", {}).items():
        lines.append(f"- {key}: `{count}`")
    lines.extend(["", "## Summary By Reason", ""])
    for key, count in report.get("summary_by_reason", {}).items():
        lines.append(f"- {key}: `{count}`")
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Track B weekly generated-artifact maintenance dry run.")
    parser.add_argument("--mode", choices=("dry-run", "apply"), default="dry-run")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--week-ending")
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--archive-staging-root", default=str(DEFAULT_ARCHIVE_STAGING_ROOT))
    parser.add_argument("--hot-root", default=str(DEFAULT_HOT_ROOT))
    parser.add_argument("--warm-root", default=str(DEFAULT_WARM_ROOT))
    parser.add_argument("--json-output")
    parser.add_argument("--detail-limit", type=int, default=50)
    parser.add_argument("--include-full-paths", action="store_true")
    parser.add_argument("--category-filter")
    parser.add_argument("--confirm-disposable-build-cleanup", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = WeeklyMaintenanceConfig(
        repo_root=Path(args.repo_root),
        mode=args.mode,
        week_ending=_parse_date(args.week_ending),
        symbols=_parse_symbols(args.symbols),
        output_root=Path(args.output_root),
        archive_staging_root=Path(args.archive_staging_root),
        hot_root=Path(args.hot_root),
        warm_root=Path(args.warm_root),
        detail_limit=max(0, int(args.detail_limit)),
        include_full_paths=bool(args.include_full_paths),
        category_filter=_parse_category_filter(args.category_filter),
        confirm_disposable_build_cleanup=bool(args.confirm_disposable_build_cleanup),
    )
    report = build_weekly_data_maintenance_report(config=config)
    if report.get("final_verdict") == "APPLY_MODE_NOT_IMPLEMENTED":
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    written = write_weekly_data_maintenance_report(config=config, report=report)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"final_verdict": report["final_verdict"], "json": str(written["json"]), "markdown": str(written["markdown"])}, sort_keys=True))
    return 1 if report.get("final_verdict") == "APPLY_DISPOSABLE_BUILD_BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
