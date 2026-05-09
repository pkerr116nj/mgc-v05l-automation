"""Dry-run weekly data maintenance classifier for Track B artifacts.

The v1 maintenance command is intentionally report-only. It classifies HOT
runtime data, WARM evidence/state, COLD archive candidates, deferred research,
and disposable build metadata without deleting, moving, or treating archives as
runtime truth.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "weekly_data_maintenance"
DEFAULT_ARCHIVE_STAGING_ROOT = Path("/Users/patrick/Dev/_mgc_v05l_archive_staging")
DEFAULT_HOT_ROOT = Path("var") / "runtime_market_data"
DEFAULT_WARM_ROOT = Path("outputs") / "reports"
ACTIVE_RUNTIME_ROOTS = (
    Path("outputs") / "track_b_execution_core",
    Path("outputs") / "operator_dashboard",
    Path("outputs") / "probationary_pattern_engine",
    Path("var"),
)
OLD_ROOT_PATTERNS = (
    "/Users/patrick/Documents/MGC-v05l-automation",
    "Mobile Documents",
    "iCloud",
)
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


def build_weekly_data_maintenance_report(*, config: WeeklyMaintenanceConfig) -> dict[str, Any]:
    if config.mode != "dry-run":
        return {
            "schema_version": "weekly_data_maintenance_v1",
            "mode": config.mode,
            "final_verdict": "APPLY_MODE_NOT_IMPLEMENTED",
            "review_required": True,
            "error": "APPLY_MODE_NOT_IMPLEMENTED",
        }

    repo_root = Path(config.repo_root)
    week_end = config.week_ending or date.today()
    week_start = week_end - timedelta(days=6)
    hot_roots = _runtime_roots(config=config)
    warm_root = _resolve(repo_root, config.warm_root)

    active_processes = _detect_active_runtime_processes()
    stale_root_hits = _detect_old_root_hits(repo_root=repo_root)
    inspected_paths = _collect_candidate_paths(repo_root=repo_root, hot_roots=hot_roots, warm_root=warm_root)
    classifications = [_classify_path(path=path, repo_root=repo_root, hot_roots=hot_roots, warm_root=warm_root) for path in inspected_paths]

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
        "classifications": classifications,
        "summary_by_classification": _summary_by_classification(classifications),
    }
    _assert_safety_invariants(report)
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


def _classify_path(*, path: Path, repo_root: Path, hot_roots: Sequence[Path], warm_root: Path) -> dict[str, Any]:
    relative = _relative(path, repo_root)
    classification = "UNKNOWN_REVIEW"
    reason = "No policy bucket matched."
    action = "review"

    if any(_is_relative_to(path, root) for root in hot_roots):
        classification = "HOT_DECISION_RUNTIME_DATA"
        reason = "Active runtime/HOT data root; never delete in dry-run maintenance."
        action = "preserve"
    elif _is_deferred_research_path(relative):
        classification = "DEFERRED_RESEARCH_OFFLINE"
        reason = "Research/offline path; classify only until archive server policy exists."
        action = "defer"
    elif _is_disposable_path(path, repo_root=repo_root):
        classification = "DISPOSABLE_BUILD"
        reason = "Rebuildable cache/build metadata."
        action = "delete_candidate_dry_run_only"
    elif _is_relative_to(path, warm_root):
        lower = str(relative).lower()
        if any(term in lower for term in BROKER_EVIDENCE_TERMS) or "preflight" in lower or "cleanup" in lower:
            classification = "COLD_ARCHIVE_CANDIDATE"
            reason = "WARM evidence/state that should be staged later, not moved in v1."
            action = "archive_candidate_dry_run_only"
        else:
            classification = "WARM_EVIDENCE_STATE"
            reason = "WARM report/dashboard/operator state; preserve under rolling retention."
            action = "preserve"

    if _is_broker_or_review_evidence(relative) and classification == "DISPOSABLE_BUILD":
        classification = "WARM_EVIDENCE_STATE"
        reason = "Broker/review evidence may never be a delete candidate."
        action = "preserve"

    return {
        "path": str(relative),
        "classification": classification,
        "reason": reason,
        "recommended_action": action,
    }


def _assert_safety_invariants(report: dict[str, Any]) -> None:
    for row in report.get("classifications", []):
        path = str(row.get("path") or "").lower()
        if row.get("classification") == "DISPOSABLE_BUILD" and any(term in path for term in BROKER_EVIDENCE_TERMS):
            raise AssertionError(f"broker/review evidence classified as disposable: {row}")
        if row.get("classification") == "DISPOSABLE_BUILD" and (
            path.startswith("var/") or path.startswith("outputs/track_b_execution_core/")
        ):
            raise AssertionError(f"active runtime path classified as disposable: {row}")


def _summary_by_classification(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for row in rows:
        key = str(row.get("classification") or "UNKNOWN_REVIEW")
        summary[key] = summary.get(key, 0) + 1
    return dict(sorted(summary.items()))


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
    )
    report = build_weekly_data_maintenance_report(config=config)
    if report.get("final_verdict") == "APPLY_MODE_NOT_IMPLEMENTED":
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    written = write_weekly_data_maintenance_report(config=config, report=report)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"final_verdict": report["final_verdict"], "json": str(written["json"]), "markdown": str(written["markdown"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
