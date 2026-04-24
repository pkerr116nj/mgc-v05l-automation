"""Targeted calibration comparison for Asia Drift pullback and invalidation rules."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .backtest import AsiaDriftPhase2Run, run_asia_drift_phase2, run_asia_drift_phase2_from_bars
from .features import BALANCED_REVISION, LOOSE_DIAGNOSTIC, PERSISTENCE_CONFIRMED, RECOVERY_CONFIRMED, STRICT_CURRENT, get_calibration_profile


DEFAULT_COMPARISON_PROFILES = (STRICT_CURRENT, BALANCED_REVISION, PERSISTENCE_CONFIRMED, RECOVERY_CONFIRMED, LOOSE_DIAGNOSTIC)


def run_calibration_comparison(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MGC",),
    start_ts=None,
    end_ts=None,
    profile_names: tuple[str, ...] = DEFAULT_COMPARISON_PROFILES,
) -> dict[str, Any]:
    profile_runs: dict[str, AsiaDriftPhase2Run] = {}
    for profile_name in profile_names:
        profile_runs[profile_name] = run_asia_drift_phase2(
            source_sqlite_path=source_sqlite_path,
            output_dir=output_dir / profile_name,
            instruments=instruments,
            start_ts=start_ts,
            end_ts=end_ts,
            calibration_profile_name=profile_name,
        )

    comparison_payload = _build_comparison_payload(
        profile_runs=profile_runs,
        source_sqlite_path=source_sqlite_path,
        instruments=instruments,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    comparison_artifacts = _write_comparison_artifacts(output_dir=output_dir, payload=comparison_payload)
    return {
        "comparison": comparison_payload,
        "artifacts": comparison_artifacts,
    }


def run_calibration_comparison_from_bars(
    *,
    output_dir: Path,
    bars_5m: list[ResearchBar],
    bars_1m: list[ResearchBar] | None = None,
    source_label: str = "synthetic",
    profile_names: tuple[str, ...] = DEFAULT_COMPARISON_PROFILES,
) -> dict[str, Any]:
    profile_runs: dict[str, AsiaDriftPhase2Run] = {}
    for profile_name in profile_names:
        profile_runs[profile_name] = run_asia_drift_phase2_from_bars(
            output_dir=output_dir / profile_name,
            bars_5m=bars_5m,
            bars_1m=bars_1m,
            source_label=source_label,
            calibration_profile_name=profile_name,
        )
    comparison_payload = _build_comparison_payload(
        profile_runs=profile_runs,
        source_sqlite_path=Path(source_label),
        instruments=tuple(sorted({bar.instrument for bar in bars_5m})),
        start_ts=bars_5m[0].end_ts if bars_5m else None,
        end_ts=bars_5m[-1].end_ts if bars_5m else None,
    )
    comparison_artifacts = _write_comparison_artifacts(output_dir=output_dir, payload=comparison_payload)
    return {
        "comparison": comparison_payload,
        "artifacts": comparison_artifacts,
    }


def _build_comparison_payload(
    *,
    profile_runs: dict[str, AsiaDriftPhase2Run],
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts,
    end_ts,
) -> dict[str, Any]:
    profile_summaries: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    for profile_name, run in sorted(profile_runs.items()):
        diagnostics = run.diagnostics
        phase1_summary = json.loads(run.phase1_run.artifacts.summary_json_path.read_text(encoding="utf-8"))
        false_veto_rate = diagnostics["pullback_veto_diagnostics"]["continuation_after_rejected_pullback_rate"]
        resumed_rate = diagnostics["premature_invalidation_audit"]["resumed_after_invalidation_rate"]
        accepted_entries = diagnostics["headline_counts"]["accepted_entries"]
        setups = diagnostics["headline_counts"]["entry_setups"]
        rows.append(
            {
                "profile": profile_name,
                "description": get_calibration_profile(profile_name).description,
                "setups": setups,
                "entry_evaluations": diagnostics["headline_counts"]["entry_evaluations"],
                "accepted_entries": accepted_entries,
                "false_veto_rate": false_veto_rate,
                "resumed_after_invalidation_rate": resumed_rate,
                "pullback_veto_count": diagnostics["headline_counts"]["pullback_veto_events"],
                "premature_invalidation_count": diagnostics["premature_invalidation_audit"]["event_count"],
                "warning_recovery_rate": diagnostics["warning_state_audit"]["recovered_rate"],
                "warning_event_count": diagnostics["warning_state_audit"]["event_count"],
                "requalified_candidate_count": diagnostics["recovery_requalification_audit"]["requalified_candidate_count"],
                "continuation_after_recovery_count": diagnostics["recovery_requalification_audit"]["continuation_after_recovery_count"],
                "chop_veto_bar_count": phase1_summary["failure_mode_counts"].get("chop_veto", 0),
                "post_spike_bar_count": phase1_summary["failure_mode_counts"].get("post_spike_instability", 0),
                "state_engine_assessment": diagnostics["interpretation"]["state_engine_assessment"],
                "phase2_summary_markdown_path": str(run.artifacts.summary_markdown_path),
            }
        )
        profile_summaries[profile_name] = {
            "profile": get_calibration_profile(profile_name).__dict__,
            "phase1_summary_path": str(run.phase1_run.artifacts.summary_json_path),
            "phase2_summary_path": str(run.artifacts.summary_json_path),
            "phase2_markdown_path": str(run.artifacts.summary_markdown_path),
            "pullback_veto_diagnostics": diagnostics["pullback_veto_diagnostics"],
            "premature_invalidation_audit": diagnostics["premature_invalidation_audit"],
            "warning_state_audit": diagnostics["warning_state_audit"],
            "recovery_requalification_audit": diagnostics["recovery_requalification_audit"],
            "entry_model_summary": diagnostics["entry_model_summary"],
            "interpretation": diagnostics["interpretation"],
        }

    recommendation = _recommend_profile(rows)
    return {
        "module": "Asia Drift v1 Calibration Comparison",
        "objective": (
            "Targeted calibration comparison focused on regime persistence, recovery, and requalification timing, "
            "using a small named profile set to reduce false kills without dropping chop/post-spike discipline."
        ),
        "source_summary": {
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "instruments": list(instruments),
            "start_ts": start_ts.isoformat() if start_ts is not None else None,
            "end_ts": end_ts.isoformat() if end_ts is not None else None,
        },
        "profiles": profile_summaries,
        "profile_comparison_rows": rows,
        "recommendation": recommendation,
    }


def _recommend_profile(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommended_profile": None, "reason": "no_rows"}

    def diagnostic_score(row: dict[str, Any]) -> tuple[float, float, int, int]:
        return (
            -(row["false_veto_rate"] + row["resumed_after_invalidation_rate"]),
            row["accepted_entries"],
            row["chop_veto_bar_count"],
            row["post_spike_bar_count"],
        )

    best_diagnostic = max(rows, key=diagnostic_score)
    default_candidates = [row for row in rows if row["profile"] != LOOSE_DIAGNOSTIC] or rows
    best_default = min(
        default_candidates,
        key=lambda row: (
            row["resumed_after_invalidation_rate"],
            -row["requalified_candidate_count"],
            -row["warning_recovery_rate"],
            row["pullback_veto_count"],
            -row["accepted_entries"],
            0 if row["profile"] == RECOVERY_CONFIRMED else (1 if row["profile"] == PERSISTENCE_CONFIRMED else 2),
        ),
    )
    return {
        "recommended_next_default": best_default["profile"],
        "recommended_diagnostic_ceiling": best_diagnostic["profile"],
        "reason": (
            "Next default favors the least destructive non-diagnostic revision, while the diagnostic ceiling reports "
            "the loosest profile that best exposes remaining false-veto pressure."
        ),
    }


def _write_comparison_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    comparison_json_path = layout["reports"] / "asia_drift_calibration_comparison.json"
    comparison_markdown_path = layout["reports"] / "asia_drift_calibration_comparison.md"
    comparison_rows_path = layout["reports"] / "asia_drift_calibration_comparison_rows.csv"

    comparison_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(comparison_rows_path, payload.get("profile_comparison_rows") or [])
    comparison_markdown_path.write_text(_render_comparison_markdown(payload), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_calibration_comparison",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "comparison_json_path": str(comparison_json_path),
                "comparison_markdown_path": str(comparison_markdown_path),
                "comparison_rows_path": str(comparison_rows_path),
            },
        },
    )
    return {
        "comparison_json_path": str(comparison_json_path),
        "comparison_markdown_path": str(comparison_markdown_path),
        "comparison_rows_path": str(comparison_rows_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_comparison_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Calibration Comparison",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Profile Comparison",
    ]
    for row in payload.get("profile_comparison_rows") or []:
        lines.append(
            f"- {row['profile']}: setups={row['setups']} accepted_entries={row['accepted_entries']} "
            f"false_veto_rate={row['false_veto_rate']} resumed_after_invalidation_rate={row['resumed_after_invalidation_rate']} "
            f"warning_recovery_rate={row['warning_recovery_rate']} requalified_candidate_count={row['requalified_candidate_count']} "
            f"chop_veto_bar_count={row['chop_veto_bar_count']} post_spike_bar_count={row['post_spike_bar_count']}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommended_next_default: {recommendation.get('recommended_next_default')}")
    lines.append(f"- recommended_diagnostic_ceiling: {recommendation.get('recommended_diagnostic_ceiling')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
