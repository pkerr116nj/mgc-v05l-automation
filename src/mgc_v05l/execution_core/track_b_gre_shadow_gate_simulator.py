"""Diagnostic-only GRE shadow gate simulator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import BACKFILL_ROWS_JSONL
from mgc_v05l.execution_core.track_b_research_feature_dataset import RESEARCH_FEATURE_DATASET_DIR, RESEARCH_FEATURE_DATASET_JSONL


SCHEMA_VERSION = "track_b_gre_shadow_gate_simulation_v1"
SHADOW_GATE_JSON = "gre_shadow_gate_simulation.json"
SHADOW_GATE_MD = "gre_shadow_gate_simulation.md"
POLICY_MATRIX_MD = "gre_shadow_gate_policy_matrix.md"
AVWAP_POLICY_MD = "gre_shadow_gate_avwap_policy_review.md"
RECOMMENDATIONS_MD = "gre_shadow_gate_recommendations.md"
AVWAP_ANCHORS = ("globex_session_open_18et", "london_open", "us_rth_open")
CONFIDENCE_THRESHOLDS = (40, 50, 60, 70)
HORIZONS = ("5m", "15m", "30m", "60m")
LOW_SAMPLE_THRESHOLD = 30


@dataclass(frozen=True)
class ShadowGateResult:
    report: dict[str, Any]
    json_path: Path
    markdown_path: Path
    policy_matrix_path: Path
    avwap_policy_review_path: Path
    recommendations_path: Path


def run_gre_shadow_gate_simulator(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    now: datetime | str | None = None,
    rows_path: Path | None = None,
    crfd_rows_path: Path | None = None,
    max_snapshot_bytes: int | None = None,
) -> ShadowGateResult:
    generated_at = _coerce_now(now)
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    source_rows = rows_path or gold_dir / BACKFILL_ROWS_JSONL
    source_crfd = crfd_rows_path or output_root / RESEARCH_FEATURE_DATASET_DIR / RESEARCH_FEATURE_DATASET_JSONL
    rows = _read_jsonl(source_rows)
    crfd_rows = _read_jsonl(source_crfd)
    report = build_gre_shadow_gate_simulation(
        rows,
        crfd_rows=crfd_rows,
        generated_at=generated_at,
        rows_path=source_rows,
        crfd_rows_path=source_crfd,
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    json_path = gold_dir / SHADOW_GATE_JSON
    write_bounded_snapshot_json(json_path, report, config=config)
    markdown_path = gold_dir / SHADOW_GATE_MD
    markdown_path.write_text(render_shadow_gate_markdown(report), encoding="utf-8")
    policy_path = gold_dir / POLICY_MATRIX_MD
    policy_path.write_text(render_policy_matrix_markdown(report), encoding="utf-8")
    avwap_path = gold_dir / AVWAP_POLICY_MD
    avwap_path.write_text(render_avwap_policy_review_markdown(report), encoding="utf-8")
    recommendations_path = gold_dir / RECOMMENDATIONS_MD
    recommendations_path.write_text(render_recommendations_markdown(report), encoding="utf-8")
    return ShadowGateResult(
        report=report,
        json_path=json_path,
        markdown_path=markdown_path,
        policy_matrix_path=policy_path,
        avwap_policy_review_path=avwap_path,
        recommendations_path=recommendations_path,
    )


def build_gre_shadow_gate_simulation(
    rows: Sequence[Mapping[str, Any]],
    *,
    crfd_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    rows_path: Path | str,
    crfd_rows_path: Path | str,
) -> dict[str, Any]:
    enriched = [_enrich_row(row, _CrfdIndex(crfd_rows)) for row in rows]
    baseline = evaluate_policy(enriched, policy_id="baseline_no_gate", description="Accept all opportunities.", accept_fn=lambda row: True)
    policies = [baseline, _regime_only_policy(enriched)]
    policies.extend(_confidence_policies(enriched))
    policies.extend(_avwap_confirmation_policies(enriched))
    policies.extend(_conflict_warning_policies(enriched))
    recommendations = _recommendations(policies, baseline)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "source_rows_path": str(rows_path),
        "crfd_rows_path": str(crfd_rows_path),
        "overall": {
            "opportunity_count": len(enriched),
            "validated_count": sum(1 for row in enriched if row.get("validation_status") == "VALIDATED"),
            "crfd_joined_count": sum(1 for row in enriched if row.get("crfd_joined") is True),
        },
        "baseline": baseline,
        "policies": policies,
        "policy_matrix": {policy["policy_id"]: policy for policy in policies},
        "avwap_policy_review": [policy for policy in policies if "avwap" in str(policy.get("policy_type"))],
        "recommendations": recommendations,
        "broker_authority": False,
        "runtime_authority": False,
        "managed_exit_authority": False,
        "strategy_authority": False,
        "trading_gate": False,
        "production_behavior_changed": False,
        "gre_scoring_changed": False,
    }


def evaluate_policy(
    rows: Sequence[Mapping[str, Any]],
    *,
    policy_id: str,
    description: str,
    accept_fn: Callable[[Mapping[str, Any]], bool],
    policy_type: str = "diagnostic",
) -> dict[str, Any]:
    accepted = [row for row in rows if accept_fn(row)]
    rejected = [row for row in rows if not accept_fn(row)]
    baseline_outcomes = [_signed_outcome(row) for row in rows]
    accepted_outcomes = [_signed_outcome(row) for row in accepted]
    rejected_outcomes = [_signed_outcome(row) for row in rejected]
    missed_winners = [row for row in rejected if (_signed_outcome(row) or 0.0) > 0]
    avoided_losers = [row for row in rejected if (_signed_outcome(row) or 0.0) < 0]
    baseline_expectancy = _average([value for value in baseline_outcomes if value is not None])
    accepted_expectancy = _average([value for value in accepted_outcomes if value is not None])
    rejected_expectancy = _average([value for value in rejected_outcomes if value is not None])
    return {
        "policy_id": policy_id,
        "policy_type": policy_type,
        "description": description,
        "opportunities_evaluated": len(rows),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "acceptance_rate": round(len(accepted) / len(rows), 4) if rows else None,
        "average_forward_return": {horizon: _average(_forward_values(accepted, horizon)) for horizon in HORIZONS},
        "average_mfe": _average([value for value in (_number(row.get("mfe")) for row in accepted) if value is not None]),
        "average_mae": _average([value for value in (_number(row.get("mae")) for row in accepted) if value is not None]),
        "direction_correctness": _direction_correctness(accepted),
        "expectancy_proxy": accepted_expectancy,
        "baseline_expectancy_proxy": baseline_expectancy,
        "rejected_expectancy_proxy": rejected_expectancy,
        "missed_winners": len(missed_winners),
        "avoided_losers": len(avoided_losers),
        "net_filter_value": round((accepted_expectancy or 0.0) - (baseline_expectancy or 0.0), 6)
        if accepted_expectancy is not None and baseline_expectancy is not None
        else None,
        "sample_size_warning": len(accepted) < LOW_SAMPLE_THRESHOLD,
    }


def _regime_only_policy(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return evaluate_policy(
        rows,
        policy_id="regime_only_directional",
        policy_type="regime_only",
        description="Allow only GRE LONG and SHORT; block CHOP, TRANSITION, and INSUFFICIENT_EVIDENCE.",
        accept_fn=lambda row: row.get("regime_label") in {"LONG", "SHORT"},
    )


def _confidence_policies(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for threshold in CONFIDENCE_THRESHOLDS:
        result.append(
            evaluate_policy(
                rows,
                policy_id=f"confidence_threshold_{threshold}",
                policy_type="confidence_threshold",
                description=f"Allow GRE LONG/SHORT only when confidence >= {threshold}.",
                accept_fn=lambda row, threshold=threshold: row.get("regime_label") in {"LONG", "SHORT"}
                and (_number(row.get("confidence")) or 0.0) >= threshold,
            )
        )
    return result


def _avwap_confirmation_policies(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for anchor in AVWAP_ANCHORS:
        result.append(
            evaluate_policy(
                rows,
                policy_id=f"avwap_confirmation_{anchor}",
                policy_type="avwap_confirmation",
                description=f"Allow LONG only above {anchor} AVWAP and SHORT only below it.",
                accept_fn=lambda row, anchor=anchor: _avwap_confirms(row, anchor),
            )
        )
    return result


def _conflict_warning_policies(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for anchor in AVWAP_ANCHORS:
        result.append(
            evaluate_policy(
                rows,
                policy_id=f"avwap_conflict_warning_{anchor}",
                policy_type="avwap_conflict_warning",
                description=f"Block GRE LONG/SHORT only when direction conflicts with {anchor} AVWAP.",
                accept_fn=lambda row, anchor=anchor: row.get("regime_label") in {"LONG", "SHORT"} and not _avwap_conflicts(row, anchor),
            )
        )
    return result


class _CrfdIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_contract: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = {}
        for row in rows:
            ts = _parse_datetime(row.get("observation_time"))
            contract = str(row.get("contract") or "").upper()
            if ts is None or not contract:
                continue
            by_contract.setdefault(contract, []).append((ts, row))
        self._rows = {contract: sorted(items, key=lambda item: item[0]) for contract, items in by_contract.items()}

    def latest_at_or_before(self, *, contract: str, timestamp: datetime | None) -> Mapping[str, Any] | None:
        if timestamp is None:
            return None
        rows = self._rows.get(str(contract or "").upper(), ())
        candidate: Mapping[str, Any] | None = None
        for row_ts, row in rows:
            if row_ts > timestamp:
                break
            candidate = row
        return candidate


def _enrich_row(row: Mapping[str, Any], index: _CrfdIndex) -> dict[str, Any]:
    item = dict(row)
    crfd = index.latest_at_or_before(contract=str(row.get("contract") or "GC"), timestamp=_parse_datetime(row.get("gre_generated_at")))
    item["crfd_joined"] = crfd is not None
    for anchor in AVWAP_ANCHORS:
        item[f"avwap_relation_{anchor}"] = (crfd or {}).get(f"avwap_relation_{anchor}") or "unavailable"
        item[f"has_avwap_{anchor}"] = bool((crfd or {}).get(f"has_avwap_{anchor}"))
    return item


def _avwap_confirms(row: Mapping[str, Any], anchor: str) -> bool:
    regime = row.get("regime_label")
    relation = row.get(f"avwap_relation_{anchor}")
    return (regime == "LONG" and relation == "above_avwap") or (regime == "SHORT" and relation == "below_avwap")


def _avwap_conflicts(row: Mapping[str, Any], anchor: str) -> bool:
    regime = row.get("regime_label")
    relation = row.get(f"avwap_relation_{anchor}")
    return (regime == "LONG" and relation == "below_avwap") or (regime == "SHORT" and relation == "above_avwap")


def _recommendations(policies: Sequence[Mapping[str, Any]], baseline: Mapping[str, Any]) -> dict[str, Any]:
    candidates = [
        policy
        for policy in policies
        if policy.get("policy_id") != "baseline_no_gate"
        and policy.get("sample_size_warning") is False
        and policy.get("net_filter_value") is not None
    ]
    ranked = sorted(candidates, key=lambda item: (_number(item.get("net_filter_value")) or -999.0, _safe_int(item.get("accepted_count"))), reverse=True)
    best = ranked[0] if ranked else None
    avwap_policies = [policy for policy in ranked if "avwap" in str(policy.get("policy_type"))]
    return {
        "is_gre_ready_for_live_shadow_gating_research": bool(ranked),
        "recommended_scope": "runtime_shadow_observation_later_only_no_gate" if ranked else "continue_offline_research",
        "most_promising_policy": best.get("policy_id") if best else None,
        "most_promising_policy_net_filter_value": best.get("net_filter_value") if best else None,
        "sample_limited_policies": [policy.get("policy_id") for policy in policies if policy.get("sample_size_warning") is True],
        "does_avwap_improve_gate": bool(avwap_policies and (_number(avwap_policies[0].get("net_filter_value")) or 0.0) > 0),
        "best_avwap_policy": avwap_policies[0].get("policy_id") if avwap_policies else None,
        "additional_data_needed": "More out-of-sample weeks and real trade alignment before any production gate proposal.",
        "production_gate_recommended_now": False,
        "baseline_expectancy_proxy": baseline.get("expectancy_proxy"),
    }


def render_shadow_gate_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("recommendations", {})
    return (
        "# GRE Shadow Gate Simulation\n\n"
        f"Generated: `{report.get('generated_at')}`\n\n"
        f"Opportunities: `{report.get('overall', {}).get('opportunity_count')}`\n\n"
        f"Most promising policy: `{rec.get('most_promising_policy')}`\n\n"
        f"AVWAP improves gate: `{rec.get('does_avwap_improve_gate')}`\n\n"
        f"Production gate recommended now: `{rec.get('production_gate_recommended_now')}`\n\n"
        "Diagnostic only: `true`\n"
    )


def render_policy_matrix_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE Shadow Gate Policy Matrix\n\n"
    text += "| Policy | Accepted | Rejected | Acceptance | Expectancy | Net Filter | Warning |\n"
    text += "|---|---:|---:|---:|---:|---:|---|\n"
    for policy in report.get("policies", []):
        text += (
            f"| `{policy.get('policy_id')}` | {policy.get('accepted_count')} | {policy.get('rejected_count')} | "
            f"{policy.get('acceptance_rate')} | {policy.get('expectancy_proxy')} | {policy.get('net_filter_value')} | "
            f"{policy.get('sample_size_warning')} |\n"
        )
    return text


def render_avwap_policy_review_markdown(report: Mapping[str, Any]) -> str:
    text = "# GRE Shadow Gate AVWAP Policy Review\n\n"
    for policy in report.get("avwap_policy_review", []):
        text += (
            f"- `{policy.get('policy_id')}`: accepted `{policy.get('accepted_count')}`, "
            f"expectancy `{policy.get('expectancy_proxy')}`, net `{policy.get('net_filter_value')}`, "
            f"missed winners `{policy.get('missed_winners')}`, avoided losers `{policy.get('avoided_losers')}`\n"
        )
    return text


def render_recommendations_markdown(report: Mapping[str, Any]) -> str:
    rec = report.get("recommendations", {})
    return (
        "# GRE Shadow Gate Recommendations\n\n"
        f"- Ready for live shadow gating research: `{rec.get('is_gre_ready_for_live_shadow_gating_research')}`\n"
        f"- Recommended scope: `{rec.get('recommended_scope')}`\n"
        f"- Most promising policy: `{rec.get('most_promising_policy')}`\n"
        f"- Does AVWAP improve the gate: `{rec.get('does_avwap_improve_gate')}`\n"
        f"- Best AVWAP policy: `{rec.get('best_avwap_policy')}`\n"
        f"- Sample-limited policies: `{rec.get('sample_limited_policies')}`\n"
        f"- Additional data needed: {rec.get('additional_data_needed')}\n"
        f"- Production gate recommended now: `{rec.get('production_gate_recommended_now')}`\n"
    )


def _signed_outcome(row: Mapping[str, Any]) -> float | None:
    forward = row.get("forward_returns")
    if not isinstance(forward, Mapping):
        return None
    raw = _number(forward.get("60m") if forward.get("60m") is not None else forward.get("30m"))
    if raw is None:
        return None
    if row.get("regime_label") == "SHORT":
        return -raw
    return raw


def _forward_values(rows: Sequence[Mapping[str, Any]], horizon: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        forward = row.get("forward_returns")
        if isinstance(forward, Mapping):
            value = _number(forward.get(horizon))
            if value is not None:
                values.append(value)
    return values


def _direction_correctness(rows: Sequence[Mapping[str, Any]]) -> float | None:
    directional = [row for row in rows if row.get("direction_correctness") in {True, False}]
    if not directional:
        return None
    return round(sum(1 for row in directional if row.get("direction_correctness") is True) / len(directional), 4)


def _average(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = _parse_datetime(value)
    return parsed if parsed else datetime.now(UTC)
