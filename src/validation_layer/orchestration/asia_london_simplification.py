"""Bounded de-optimization pass for Asia-London participation research."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.app.asia_london_participation_optimization import run_asia_london_participation_optimization

from ..layer1.pilot import Layer1PilotArtifacts, run_asia_london_participation_family_pilot_rerun


REPO_ROOT = Path.cwd()
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "validation_layer" / "asia_london_simplification"
DEFAULT_TUNED_ROOT = REPO_ROOT / "outputs" / "validation_layer" / "asia_london_participation_family_pilot"
DEFAULT_SIMPLIFIED_SYMBOLS: tuple[str, ...] = ("GC", "MGC", "NQ", "MNQ")
DEFAULT_SIMPLIFIED_LONG_VARIANTS: tuple[str, ...] = (
    "segment_forced_long_v4_breakout_or_bar7",
    "segment_forced_long_v5_dip_reclaim_or_bar8",
)
DEFAULT_SIMPLIFIED_SHORT_VARIANTS: tuple[str, ...] = (
    "segment_forced_short_v2_reclaim_fail_or_bar7",
    "segment_forced_short_v4_breakdown_or_bar7",
)


@dataclass(frozen=True)
class SimplifiedCandidateDefinition:
    source_variant: str
    side: str
    simplification_class: str
    rationale: str


SIMPLIFIED_CANDIDATES: tuple[SimplifiedCandidateDefinition, ...] = (
    SimplifiedCandidateDefinition(
        source_variant="LONG__segment_forced_long_v4_breakout_or_bar7__base",
        side="LONG",
        simplification_class="fixed_breakout",
        rationale="Removes contextual fallback logic and locks the long trigger to a simple breakout-or-bar-7 rule.",
    ),
    SimplifiedCandidateDefinition(
        source_variant="LONG__segment_forced_long_v5_dip_reclaim_or_bar8__base",
        side="LONG",
        simplification_class="fixed_reclaim",
        rationale="Keeps the reclaim idea but drops ATP gating and contextual fallback branches.",
    ),
    SimplifiedCandidateDefinition(
        source_variant="SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__base",
        side="SHORT",
        simplification_class="fixed_reclaim_fail",
        rationale="Keeps the reclaim-fail short but removes the ATP gate and any wider candidate surface.",
    ),
    SimplifiedCandidateDefinition(
        source_variant="SHORT__segment_forced_short_v4_breakdown_or_bar7__base",
        side="SHORT",
        simplification_class="fixed_breakdown",
        rationale="Locks the short side to a simple breakdown-or-bar-7 rule instead of reclaim-fail nuance.",
    ),
)

TUNABLE_DEGREES_OF_FREEDOM: tuple[dict[str, Any], ...] = (
    {
        "name": "entry_family",
        "description": "Breakout, dip-reclaim, reclaim-fail, or contextual fallback entry shape.",
        "current_values": [
            "segment_forced_long_v5_dip_reclaim_or_bar8",
            "segment_forced_long_v6_contextual_fallback",
            "segment_forced_short_v2_reclaim_fail_or_bar7",
        ],
    },
    {
        "name": "fallback_timing",
        "description": "Fallback entry bar when the preferred trigger does not appear.",
        "current_values": ["bar7", "bar8", "contextual_bar6_bar7_bar8"],
    },
    {
        "name": "contextual_branching",
        "description": "Setup-dependent fallback branches based on close location, VWAP displacement, and setup range.",
        "current_values": ["present_in_long_v6", "absent_in_long_v5", "absent_in_short_v2"],
    },
    {
        "name": "gate_mode",
        "description": "Base overnight rule versus ATP-style bias gate at the setup bar.",
        "current_values": ["base", "atp_bias_gate"],
    },
    {
        "name": "side_choice",
        "description": "Long versus short candidate selection within the family optimization surface.",
        "current_values": ["long", "short"],
    },
)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_candidate_system_payload(
    *,
    gc_mgc_json: Path,
    nq_mnq_json: Path,
) -> dict[str, Any]:
    lane_sequence: list[dict[str, Any]] = []
    for definition in SIMPLIFIED_CANDIDATES:
        for symbol_group in ("GC_MGC", "NQ_MNQ"):
            lane_sequence.append(
                {
                    "lane_id": f"SIMPLIFIED_{definition.side}_{definition.simplification_class}".upper(),
                    "symbol_group": symbol_group,
                    "source_variant": definition.source_variant,
                    "side": definition.side,
                    "simplification_class": definition.simplification_class,
                    "rationale": definition.rationale,
                }
            )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_system": {
            "candidate_id": "asia_london_participation_core_v1_simplified_surface",
            "family_name": "asia_london_participation_core_v1",
            "lane_sequence": lane_sequence,
        },
        "source_artifacts": {
            "gc_mgc_optimization_json": str(gc_mgc_json),
            "nq_mnq_optimization_json": str(nq_mnq_json),
        },
        "scenario_reports": [],
    }


def _build_admission_payload(candidate_system_json: Path) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_id": "asia_london_participation_core_v1_simplified_surface",
        "family_name": "asia_london_participation_core_v1",
        "admission_status": "RESEARCH_SIMPLIFICATION_PASS_ONLY",
        "candidate_system_json": str(candidate_system_json),
        "combined_package": {},
        "package_scenarios": [{"symbol": symbol} for symbol in DEFAULT_SIMPLIFIED_SYMBOLS],
    }


def _report_metrics(report_path: Path) -> dict[str, Any]:
    report = _load_json(report_path)
    module_map = {row["module_name"]: row for row in report["module_results"]}
    return {
        "overall_status": report["overall_status"],
        "train_bias_status": module_map.get("train_bias", {}).get("status"),
        "train_bias_score": module_map.get("train_bias", {}).get("metrics", {}).get("training_bias_score"),
        "selection_bias_status": module_map.get("selection_bias", {}).get("status"),
        "selection_bias_score": module_map.get("selection_bias", {}).get("metrics", {}).get("selection_bias_score"),
        "cscv_pbo_status": module_map.get("cscv_pbo", {}).get("status"),
        "cscv_pbo_score": module_map.get("cscv_pbo", {}).get("metrics", {}).get("cscv_pbo_score"),
        "bootstrap_status": module_map.get("bootstrap", {}).get("status"),
        "monte_carlo_status": module_map.get("monte_carlo", {}).get("status"),
        "drawdown_status": module_map.get("drawdown", {}).get("status"),
        "trade_normalization_status": module_map.get("trade_normalization", {}).get("status"),
    }


def _load_tuned_rows(root_dir: Path) -> dict[str, dict[str, Any]]:
    summary = _load_json(root_dir / "asia_london_family_summary.json")
    return {row["source_variant"]: row for row in summary["rows"]}


def _candidate_definition_map() -> dict[str, SimplifiedCandidateDefinition]:
    return {definition.source_variant: definition for definition in SIMPLIFIED_CANDIDATES}


def _comparison_summary(*, tuned_rows: dict[str, dict[str, Any]], simplified_rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    comparison_rows: list[dict[str, Any]] = []
    definitions = _candidate_definition_map()
    all_keys = sorted(set(tuned_rows) | set(simplified_rows))
    for key in all_keys:
        tuned = tuned_rows.get(key)
        simplified = simplified_rows.get(key)
        row = {
            "source_variant": key,
            "tuned": tuned,
            "simplified": simplified,
            "comparison": None,
        }
        if tuned and simplified:
            tuned_score = tuned.get("train_bias_score")
            simplified_score = simplified.get("train_bias_score")
            bias_change = None
            if tuned_score is not None and simplified_score is not None:
                bias_change = round(float(simplified_score) - float(tuned_score), 6)
            row["comparison"] = {
                "train_bias_improved": None if bias_change is None else bias_change > 0.1,
                "train_bias_score_delta": bias_change,
                "selection_bias_status_change": {
                    "from": tuned.get("selection_bias_status"),
                    "to": simplified.get("selection_bias_status"),
                },
                "cscv_pbo_status_change": {
                    "from": tuned.get("cscv_pbo_status"),
                    "to": simplified.get("cscv_pbo_status"),
                },
                "monte_carlo_status_change": {
                    "from": tuned.get("monte_carlo_status"),
                    "to": simplified.get("monte_carlo_status"),
                },
            }
        if key in definitions:
            row["simplification_definition"] = {
                "simplification_class": definitions[key].simplification_class,
                "rationale": definitions[key].rationale,
            }
        comparison_rows.append(row)

    tuned_inflated = sum(1 for row in tuned_rows.values() if row.get("train_bias_status") == "fail")
    simplified_inflated = sum(1 for row in simplified_rows.values() if row.get("train_bias_status") == "fail")
    durable_survivors = [
        key
        for key, row in simplified_rows.items()
        if row.get("train_bias_status") == "pass"
        and row.get("selection_bias_status") == "pass"
        and row.get("cscv_pbo_status") == "pass"
        and row.get("monte_carlo_status") == "pass"
        and row.get("drawdown_status") == "pass"
        and row.get("trade_normalization_status") == "pass"
    ]
    provisional_survivors = [
        key
        for key, row in simplified_rows.items()
        if row.get("train_bias_status") != "fail"
        and row.get("selection_bias_status") != "fail"
        and row.get("cscv_pbo_status") == "pass"
        and row.get("monte_carlo_status") != "fail"
        and row.get("drawdown_status") == "pass"
        and row.get("trade_normalization_status") == "pass"
    ]
    conclusion = "likely_tuned_artifact"
    if durable_survivors:
        conclusion = "durable_simplified_core_exists"
    elif provisional_survivors or simplified_inflated < tuned_inflated:
        conclusion = "possible_but_needs_more_evidence"

    return {
        "conclusion": conclusion,
        "tuned_train_bias_fail_count": tuned_inflated,
        "simplified_train_bias_fail_count": simplified_inflated,
        "durable_survivors": durable_survivors,
        "provisional_survivors": provisional_survivors,
        "rows": comparison_rows,
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia-London Simplification Pass",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Conclusion: `{payload['comparison']['conclusion']}`",
        "",
        "## Tunable Degrees Of Freedom",
    ]
    for item in payload["tunable_degrees_of_freedom"]:
        lines.append(f"- `{item['name']}`: {item['description']} Current values: `{item['current_values']}`")
    lines.extend([
        "",
        "## Simplified Candidate Definitions",
    ])
    for definition in SIMPLIFIED_CANDIDATES:
        lines.append(f"- `{definition.source_variant}`: {definition.rationale}")
    lines.extend(["", "## Comparison"])
    for row in payload["comparison"]["rows"]:
        lines.append(f"### {row['source_variant']}")
        if row.get("simplification_definition"):
            lines.append(f"- Simplification class: `{row['simplification_definition']['simplification_class']}`")
        if row.get("tuned"):
            lines.append(
                f"- Tuned context: overall `{row['tuned'].get('overall_status')}`, train_bias `{row['tuned'].get('train_bias_status')}`, selection_bias `{row['tuned'].get('selection_bias_status')}`, monte_carlo `{row['tuned'].get('monte_carlo_status')}`"
            )
        if row.get("simplified"):
            lines.append(
                f"- Simplified context: overall `{row['simplified'].get('overall_status')}`, train_bias `{row['simplified'].get('train_bias_status')}`, selection_bias `{row['simplified'].get('selection_bias_status')}`, monte_carlo `{row['simplified'].get('monte_carlo_status')}`"
            )
        if row.get("comparison"):
            lines.append(
                f"- Train-bias score delta: `{row['comparison']['train_bias_score_delta']}`; improved: `{row['comparison']['train_bias_improved']}`"
            )
    return "\n".join(lines) + "\n"


def run_asia_london_simplification_pass(
    *,
    output_dir: str | Path = DEFAULT_OUTPUT_ROOT,
    tuned_root: str | Path = DEFAULT_TUNED_ROOT,
) -> dict[str, Any]:
    root_dir = Path(output_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)

    tuned_root_path = Path(tuned_root).resolve()
    if not (tuned_root_path / "asia_london_family_summary.json").exists():
        run_asia_london_participation_family_pilot_rerun(output_dir=tuned_root_path)

    simplified_opt_root = root_dir / "simplified_optimization"
    simplified_opt_root.mkdir(parents=True, exist_ok=True)
    gc_mgc_run = run_asia_london_participation_optimization(
        symbols=("GC", "MGC"),
        output_dir=simplified_opt_root / "gc_mgc",
        long_variant_ids=DEFAULT_SIMPLIFIED_LONG_VARIANTS,
        short_variant_ids=DEFAULT_SIMPLIFIED_SHORT_VARIANTS,
        gate_modes=("base",),
    )
    nq_mnq_run = run_asia_london_participation_optimization(
        symbols=("NQ", "MNQ"),
        output_dir=simplified_opt_root / "nq_mnq",
        long_variant_ids=DEFAULT_SIMPLIFIED_LONG_VARIANTS,
        short_variant_ids=DEFAULT_SIMPLIFIED_SHORT_VARIANTS,
        gate_modes=("base",),
    )

    candidate_system_json = _write_json(
        root_dir / "asia_london_simplified_candidate_system.json",
        _build_candidate_system_payload(
            gc_mgc_json=Path(gc_mgc_run["artifact_paths"]["json"]),
            nq_mnq_json=Path(nq_mnq_run["artifact_paths"]["json"]),
        ),
    )
    admission_plan_json = _write_json(
        root_dir / "asia_london_simplified_admission_plan.json",
        _build_admission_payload(candidate_system_json),
    )

    pilot_artifacts: Layer1PilotArtifacts = run_asia_london_participation_family_pilot_rerun(
        output_dir=root_dir / "validation",
        candidate_system_json=candidate_system_json,
        admission_plan_json=admission_plan_json,
        gc_mgc_optimization_json=Path(gc_mgc_run["artifact_paths"]["json"]),
        nq_mnq_optimization_json=Path(nq_mnq_run["artifact_paths"]["json"]),
    )

    tuned_rows = _load_tuned_rows(tuned_root_path)
    simplified_summary = _load_json(Path(pilot_artifacts.root_dir) / "asia_london_family_summary.json")
    simplified_rows: dict[str, dict[str, Any]] = {}
    for row in simplified_summary["rows"]:
        simplified_rows[row["source_variant"]] = {
            **row,
            **(
                _report_metrics(Path(row["report_paths"]["validation_report_json"]))
                if row.get("report_paths")
                else {}
            ),
        }
    tuned_enriched: dict[str, dict[str, Any]] = {}
    for key, row in tuned_rows.items():
        enriched = dict(row)
        if row.get("report_paths"):
            enriched.update(_report_metrics(Path(row["report_paths"]["validation_report_json"])))
        tuned_enriched[key] = enriched

    comparison = _comparison_summary(tuned_rows=tuned_enriched, simplified_rows=simplified_rows)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "tuned_root": str(tuned_root_path),
        "simplified_validation_root": str(pilot_artifacts.root_dir),
        "tunable_degrees_of_freedom": list(TUNABLE_DEGREES_OF_FREEDOM),
        "simplified_candidate_definitions": [
            {
                "source_variant": definition.source_variant,
                "side": definition.side,
                "simplification_class": definition.simplification_class,
                "rationale": definition.rationale,
            }
            for definition in SIMPLIFIED_CANDIDATES
        ],
        "comparison": comparison,
    }
    json_path = root_dir / "asia_london_simplification_summary.json"
    md_path = root_dir / "asia_london_simplification_summary.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "summary_json": str(json_path),
        "summary_markdown": str(md_path),
        "simplified_validation_root": str(pilot_artifacts.root_dir),
        "comparison": comparison,
    }
