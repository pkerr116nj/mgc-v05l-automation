"""Manual-first ops orchestration for Asia Drift shadow trading."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..regime.vix_regime_builder import load_vol_regime_rows
from ..trend_participation.storage import build_layout, write_storage_manifest
from ..warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout
from .shadow_reporting import run_asia_drift_shadow_report
from .shadow_trading import run_asia_drift_shadow_trading


NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
REQUIRED_DATASETS = (
    "derived_bars_5m",
    "derived_bars_15m",
    "derived_bars_60m",
    "derived_bars_240m",
    "derived_bars_daily",
    "vol_regime_daily",
)


def run_asia_drift_shadow_ops(
    *,
    start_date: date | None,
    end_date: date | None,
    latest_session: bool,
    warehouse_root: Path,
    pass6_classification_csv: Path,
    shadow_output_dir: Path,
    report_output_dir: Path,
    health_output_dir: Path,
    fail_on_warning: bool = False,
    skip_health: bool = False,
    skip_shadow: bool = False,
    skip_report: bool = False,
    overwrite: bool = False,
) -> dict[str, Any]:
    warehouse_root = warehouse_root.resolve()
    pass6_classification_csv = pass6_classification_csv.resolve()
    shadow_output_dir = shadow_output_dir.resolve()
    report_output_dir = report_output_dir.resolve()
    health_output_dir = health_output_dir.resolve()

    resolved_start, resolved_end = _resolve_trade_date_range(
        start_date=start_date,
        end_date=end_date,
        latest_session=latest_session,
        warehouse_root=warehouse_root,
    )

    health_layout = build_layout(health_output_dir)
    health_result = _run_health_check(
        start_date=resolved_start,
        end_date=resolved_end,
        warehouse_root=warehouse_root,
        pass6_classification_csv=pass6_classification_csv,
        shadow_output_dir=shadow_output_dir,
        report_output_dir=report_output_dir,
        health_output_dir=health_output_dir,
        overwrite=overwrite,
        skip_shadow=skip_shadow,
        skip_report=skip_report,
    )

    shadow_result: dict[str, Any] | None = None
    report_result: dict[str, Any] | None = None
    overall_status = health_result["overall_status"]

    should_stop = False
    if not skip_health:
        if overall_status == "failed":
            should_stop = True
        elif overall_status == "warning" and fail_on_warning:
            overall_status = "failed"
            should_stop = True

    if not should_stop and not skip_shadow:
        raw_shadow_result = run_asia_drift_shadow_trading(
            warehouse_root=warehouse_root,
            pass6_classification_csv=pass6_classification_csv,
            output_dir=shadow_output_dir,
            start_ts=_shadow_start_ts(resolved_start),
            end_ts=_shadow_end_ts(resolved_end),
        )
        shadow_result = {
            "output_dir": str(shadow_output_dir),
            "artifacts": raw_shadow_result["artifacts"],
            "row_counts": raw_shadow_result["summary"]["row_counts"],
            "scope": raw_shadow_result["summary"]["scope"],
        }

    if not should_stop and not skip_report:
        report_result = run_asia_drift_shadow_report(
            start_date=resolved_start,
            end_date=resolved_end,
            output_dir=report_output_dir,
            shadow_output_dir=shadow_output_dir,
            warehouse_root=warehouse_root,
        )
        report_status = str(report_result["process_status"])
        overall_status = _merge_status(overall_status, report_status)
        if report_status == "warning" and fail_on_warning:
            overall_status = "failed"

    payload = {
        "module": "asia_drift_shadow_ops",
        "overall_status": overall_status,
        "scope": {
            "start_date": resolved_start.isoformat(),
            "end_date": resolved_end.isoformat(),
            "warehouse_root": str(warehouse_root),
            "pass6_classification_csv": str(pass6_classification_csv),
            "shadow_output_dir": str(shadow_output_dir),
            "report_output_dir": str(report_output_dir),
            "health_output_dir": str(health_output_dir),
            "fail_on_warning": fail_on_warning,
            "skip_health": skip_health,
            "skip_shadow": skip_shadow,
            "skip_report": skip_report,
            "overwrite": overwrite,
        },
        "health": health_result,
        "shadow": shadow_result,
        "report": report_result,
    }

    summary_json = health_layout["reports"] / "asia_drift_shadow_ops_summary.json"
    summary_markdown = health_layout["reports"] / "asia_drift_shadow_ops_summary.md"
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown.write_text(_render_ops_markdown(payload), encoding="utf-8")
    write_storage_manifest(
        health_layout["storage_manifest"],
        {
            "module": "asia_drift_shadow_ops",
            "summary_json": str(summary_json),
            "summary_markdown": str(summary_markdown),
            "overall_status": overall_status,
        },
    )
    payload["artifacts"] = {
        "summary_json": str(summary_json),
        "summary_markdown": str(summary_markdown),
        "storage_manifest": str(health_layout["storage_manifest"]),
    }
    return payload


def _resolve_trade_date_range(
    *,
    start_date: date | None,
    end_date: date | None,
    latest_session: bool,
    warehouse_root: Path,
) -> tuple[date, date]:
    if latest_session:
        vol_rows = load_vol_regime_rows(warehouse_root)
        if not vol_rows:
            raise RuntimeError("Cannot resolve --latest-session without VIX regime rows.")
        latest_trade_date = date.fromisoformat(str(vol_rows[-1]["vix_trade_date"]))
        return latest_trade_date, latest_trade_date
    if start_date is None or end_date is None:
        raise ValueError("Either provide both --start-date and --end-date, or use --latest-session.")
    return start_date, end_date


def _run_health_check(
    *,
    start_date: date,
    end_date: date,
    warehouse_root: Path,
    pass6_classification_csv: Path,
    shadow_output_dir: Path,
    report_output_dir: Path,
    health_output_dir: Path,
    overwrite: bool,
    skip_shadow: bool,
    skip_report: bool,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    warehouse_layout = build_warehouse_layout(warehouse_root)

    sane = start_date <= end_date and end_date <= date.today()
    checks.append(
        {
            "name": "date_range_sane",
            "status": "ok" if sane else "fail",
            "details": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "today": date.today().isoformat()},
        }
    )

    missing_datasets = [name for name in REQUIRED_DATASETS if not _dataset_has_parquet(warehouse_layout[name])]
    checks.append(
        {
            "name": "required_datasets_exist",
            "status": "ok" if not missing_datasets else "fail",
            "details": {"missing_datasets": missing_datasets},
        }
    )

    vol_rows = load_vol_regime_rows(warehouse_root)
    latest_vix_trade_date = date.fromisoformat(str(vol_rows[-1]["vix_trade_date"])) if vol_rows else None
    expected_last = _last_business_date(start_date, end_date)
    vix_fresh = latest_vix_trade_date is not None and latest_vix_trade_date >= expected_last
    checks.append(
        {
            "name": "vix_regime_fresh_enough",
            "status": "ok" if vix_fresh else "fail",
            "details": {
                "latest_vix_trade_date": None if latest_vix_trade_date is None else latest_vix_trade_date.isoformat(),
                "expected_at_least": expected_last.isoformat(),
            },
        }
    )

    checks.append(
        {
            "name": "pass6_classification_exists",
            "status": "ok" if pass6_classification_csv.exists() else "fail",
            "details": {"path": str(pass6_classification_csv)},
        }
    )

    writable_results = {
        "shadow_output_dir": _dir_writable(shadow_output_dir),
        "report_output_dir": _dir_writable(report_output_dir),
        "health_output_dir": _dir_writable(health_output_dir),
    }
    checks.append(
        {
            "name": "output_dirs_writable",
            "status": "ok" if all(writable_results.values()) else "fail",
            "details": writable_results,
        }
    )

    overwrite_conflicts: list[str] = []
    if not overwrite:
        if not skip_shadow and _dir_has_artifacts(shadow_output_dir):
            overwrite_conflicts.append("shadow_output_dir")
        if not skip_report and _dir_has_artifacts(report_output_dir):
            overwrite_conflicts.append("report_output_dir")
        if _dir_has_artifacts(health_output_dir):
            overwrite_conflicts.append("health_output_dir")
    checks.append(
        {
            "name": "overwrite_protection",
            "status": "ok" if not overwrite_conflicts else "fail",
            "details": {"overwrite": overwrite, "conflicts": overwrite_conflicts},
        }
    )

    overall_status = "healthy"
    if any(check["status"] == "fail" for check in checks):
        overall_status = "failed"
    elif any(check["status"] == "warn" for check in checks):
        overall_status = "warning"
    return {
        "overall_status": overall_status,
        "checks": checks,
    }


def _shadow_start_ts(start_date: date) -> datetime:
    session_anchor = datetime.combine(start_date - timedelta(days=1), time(hour=18, minute=0), tzinfo=NY)
    return session_anchor


def _shadow_end_ts(end_date: date) -> datetime:
    return datetime.combine(end_date, time(hour=23, minute=59), tzinfo=NY)


def _dataset_has_parquet(path: Path) -> bool:
    return any(candidate.name != "_schema.parquet" for candidate in path.rglob("*.parquet"))


def _dir_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def _dir_has_artifacts(path: Path) -> bool:
    if not path.exists():
        return False
    return any(child for child in path.rglob("*") if child.is_file())


def _last_business_date(start_date: date, end_date: date) -> date:
    current = end_date
    while current >= start_date:
        if current.weekday() < 5:
            return current
        current -= timedelta(days=1)
    return end_date


def _merge_status(left: str, right: str) -> str:
    ordering = {"healthy": 0, "warning": 1, "failed": 2}
    return max((left, right), key=lambda item: ordering[item])


def _render_ops_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Shadow Ops",
        "",
        f"- Overall status: {payload['overall_status']}",
        f"- Range: {payload['scope']['start_date']} -> {payload['scope']['end_date']}",
        "",
        "## Health",
    ]
    for check in payload["health"]["checks"]:
        lines.append(f"- {check['name']}: {check['status']}")
    shadow = payload.get("shadow")
    if shadow is not None:
        lines.extend(
            [
                "",
                "## Shadow",
                f"- Output dir: {shadow['output_dir']}",
                f"- Candidate rows: {shadow['row_counts']['candidate_rows']}",
                f"- Simulated trades: {shadow['row_counts']['simulated_trade_rows']}",
            ]
        )
    report = payload.get("report")
    if report is not None:
        lines.extend(
            [
                "",
                "## Report",
                f"- Process status: {report['process_status']}",
                f"- Daily rows: {report['row_counts']['daily_rows']}",
                f"- Rolling rows: {report['row_counts']['rolling_rows']}",
            ]
        )
    return "\n".join(lines) + "\n"
