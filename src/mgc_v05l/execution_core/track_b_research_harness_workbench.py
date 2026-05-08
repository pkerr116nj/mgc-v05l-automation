"""Track B research harness / workbench backlog.

This artifact is a reusable research-control surface. It does not run strategy
replay, change thresholds, promote candidates, or invoke broker paths.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/diagnostics")


@dataclass(frozen=True)
class TrackBResearchHarnessWorkbenchConfig:
    repo_root: Path = Path(".")
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_harness_workbench.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_research_harness_workbench.md"
    location_variant_research_json: Path = (
        DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_research_replay.json"
    )
    location_variant_exit_sensitivity_json: Path = (
        DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_snap_turn_location_variant_exit_sensitivity.json"
    )


@dataclass(frozen=True)
class TrackBResearchHarnessWorkbenchResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def create_track_b_research_harness_workbench(
    *,
    config: TrackBResearchHarnessWorkbenchConfig | None = None,
    now: datetime | None = None,
) -> TrackBResearchHarnessWorkbenchResult:
    actual_config = config or TrackBResearchHarnessWorkbenchConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    location_research = _load_json(_resolve(repo_root, actual_config.location_variant_research_json))
    exit_sensitivity = _load_json(_resolve(repo_root, actual_config.location_variant_exit_sensitivity_json))
    backlog = [
        _location_variant_backlog_item(
            location_research=location_research,
            exit_sensitivity=exit_sensitivity,
            prior_friday=_prior_friday(actual_now.date()),
        )
    ]
    report = {
        "schema_version": "track_b_research_harness_workbench_v1",
        "generated_at": actual_now.isoformat(),
        "purpose": "Reusable Track B research candidate backlog and full-history retest contract.",
        "production_thresholds_changed": False,
        "paper_promotion_changed": False,
        "broker_commands_invoked": False,
        "paper_proof_cli_invoked": False,
        "submit_cancel_place_order_invoked": False,
        "workbench_status": "BUILDING_REUSABLE_RESEARCH_HARNESS",
        "full_history_retest_contract": {
            "source_history": "maximum Track 1 1m-bar history available",
            "end_date_rule": "through prior Friday",
            "through_prior_friday": _prior_friday(actual_now.date()).isoformat(),
            "required_sample_frame_headers": True,
            "required_outputs": [
                "sample-frame header",
                "strategy/baseline comparison",
                "exit-policy grid",
                "P&L",
                "drawdown",
                "Sharpe",
                "outlier sensitivity",
                "session/regime buckets",
            ],
        },
        "candidate_backlog": backlog,
        "next_workbench_steps": [
            "Implement shared 1m history loader over maximum retained Track 1 history.",
            "Implement reusable candidate replay protocol with sample-frame headers.",
            "Implement exit-policy grid runner with P&L, drawdown, and Sharpe metrics.",
            "Add baseline/control sampling before any PAPER promotion decision.",
        ],
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    _write_json(output_json, report)
    _write_text(output_md, _markdown(report))
    return TrackBResearchHarnessWorkbenchResult(report_json=output_json, report_md=output_md, report=report)


def _location_variant_backlog_item(
    *,
    location_research: Mapping[str, Any],
    exit_sensitivity: Mapping[str, Any],
    prior_friday: date,
) -> dict[str, Any]:
    candidate_name = str(
        exit_sensitivity.get("candidate_name")
        or location_research.get("candidate_name")
        or "MNQ_FIRST_BEAR_SNAP_TURN_LOCATION_VARIANT_RESEARCH_V1"
    )
    research_frame = location_research.get("sample_frame") if isinstance(location_research.get("sample_frame"), Mapping) else {}
    exit_frame = exit_sensitivity.get("sample_frame") if isinstance(exit_sensitivity.get("sample_frame"), Mapping) else {}
    return {
        "candidate_name": candidate_name,
        "candidate_family": "snap_turn_location_variant",
        "base_strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "instrument": "MNQ",
        "candidate_status": "NOT_PROMOTED",
        "candidate_status_reasons": [
            "REJECTED_IN_SINGLE_WINDOW_DIAGNOSTIC",
            "RETEST_REQUIRED_ON_FULL_HISTORY_RESEARCH_ENGINE",
        ],
        "paper_eligible": False,
        "managed_paper_eligible": False,
        "delete_from_research_inventory": False,
        "bespoke_work_paused": True,
        "current_evidence_scope": {
            "research_replay_classification": location_research.get("classification"),
            "exit_sensitivity_classification": exit_sensitivity.get("classification"),
            "research_sample_frame_classification": research_frame.get("lookback_classification"),
            "exit_sample_frame_classification": exit_frame.get("lookback_classification"),
            "sample_count": exit_sensitivity.get("sample_count") or location_research.get("sample_count"),
            "start_timestamp": exit_frame.get("start_timestamp") or research_frame.get("start_timestamp"),
            "end_timestamp": exit_frame.get("end_timestamp") or research_frame.get("end_timestamp"),
        },
        "future_retest_contract": {
            "engine": "Track B Research Harness / Workbench",
            "data_scope": "maximum Track 1 1m-bar history",
            "through_date": prior_friday.isoformat(),
            "bar_timeframe": "1m source bars with completed 5m decision reconstruction",
            "required_sample_frame_headers": True,
            "required_baselines": [
                "production MNQ_FIRST_BEAR_SNAP_TURN_V1",
                "session/regime matched random baseline",
                "simple directional/session baseline",
            ],
            "required_exit_policy_grid": [
                "1R target / 1R stop",
                "1.5R target / 1R stop",
                "3x5m time-box",
                "quick scalp target",
                "breakeven after early favorable excursion",
                "trail after first favorable bar",
                "failed follow-through exit after 1 or 2 bars",
                "volatility-scaled stop/target",
                "VWAP/EMA invalidation where path data is available",
            ],
            "required_metrics": ["trades", "P&L", "average R", "win rate", "drawdown", "Sharpe", "MFE/MAE"],
            "promotion_rule": "No PAPER promotion until full-history replay beats baselines with acceptable drawdown and sample frame.",
        },
    }


def _prior_friday(value: date) -> date:
    days_since_friday = (value.weekday() - 4) % 7
    if days_since_friday == 0:
        days_since_friday = 7
    return value - timedelta(days=days_since_friday)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Research Harness Workbench",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "This workbench queues research candidates for reusable full-history replay. It does not promote candidates to PAPER or invoke broker paths.",
        "",
        "## Full-History Retest Contract",
        "",
    ]
    contract = report.get("full_history_retest_contract") or {}
    lines.append(f"- Source history: {contract.get('source_history')}")
    lines.append(f"- End-date rule: {contract.get('end_date_rule')} ({contract.get('through_prior_friday')})")
    lines.append("- Required outputs:")
    for item in contract.get("required_outputs") or []:
        lines.append(f"  - {item}")
    lines.extend(["", "## Candidate Backlog", ""])
    for item in report.get("candidate_backlog") or []:
        lines.append(f"### {item.get('candidate_name')}")
        lines.append(f"- Status: {item.get('candidate_status')}")
        lines.append(f"- Reasons: {', '.join(item.get('candidate_status_reasons') or [])}")
        lines.append(f"- Paper eligible: {item.get('paper_eligible')}")
        evidence = item.get("current_evidence_scope") or {}
        lines.append(
            f"- Current evidence: {evidence.get('research_sample_frame_classification')} / "
            f"{evidence.get('exit_sample_frame_classification')}, samples={evidence.get('sample_count')}, "
            f"{evidence.get('start_timestamp')} -> {evidence.get('end_timestamp')}"
        )
        retest = item.get("future_retest_contract") or {}
        lines.append(f"- Retest engine: {retest.get('engine')}")
        lines.append(f"- Retest data scope: {retest.get('data_scope')} through {retest.get('through_date')}")
        lines.append(f"- Promotion rule: {retest.get('promotion_rule')}")
        lines.append("")
    lines.extend(["## Next Steps", ""])
    for item in report.get("next_workbench_steps") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"
