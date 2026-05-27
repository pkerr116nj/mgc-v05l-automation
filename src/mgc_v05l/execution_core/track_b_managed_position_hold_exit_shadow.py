"""Read-only Hold/Exit shadow visibility for active managed positions.

This companion artifact attaches PositionIntent, strategy policy, and
Hold-State / Exit-Selection shadow recommendations to managed-position rows.
It is visibility only: it does not submit, cancel, close, modify, flatten, or
mutate lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_hold_exit_shadow_engine import (
    INSUFFICIENT_EVIDENCE_STATE,
    NO_AUTHORITY_FLAGS,
    NO_RECOMMENDATION,
    evaluate_hold_exit_shadow,
)
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    DEFAULT_OUTPUT_PATH as DEFAULT_POSITION_INTENT_AUDIT_PATH,
)
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    strategy_hold_exit_policy_for,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANAGED_POSITION_REGISTRY_PATH = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_LATEST_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_managed_position_hold_exit_shadow.json"
)
DEFAULT_EVENTS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "managed_position_hold_exit_shadow_events.jsonl"
)

MANAGED_POSITION_HOLD_EXIT_SHADOW_READY = "MANAGED_POSITION_HOLD_EXIT_SHADOW_READY"
MANAGED_POSITION_HOLD_EXIT_SHADOW_NO_OPEN_POSITIONS = "MANAGED_POSITION_HOLD_EXIT_SHADOW_NO_OPEN_POSITIONS"

BUG_FIX_CONTAMINATION_FLAGS = {
    "BUG_FIX_EXIT",
    "LEAK_TEST_TRADE",
    "REMEDIATION_TRADE",
    "OPERATOR_SUPERVISED_CLEANUP",
    "LIFECYCLE_BUG_RESOLUTION",
    "DUPLICATE_EXIT_RESOLUTION",
    "AGGREGATE_CLOSE_REPAIR",
    "BROKER_FLAT_RECONCILIATION_CLEANUP",
    "MANUAL_INTERVENTION_REQUIRED",
}


@dataclass(frozen=True)
class ManagedPositionHoldExitShadowConfig:
    repo_root: Path = REPO_ROOT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_PATH
    position_intent_audit_path: Path = DEFAULT_POSITION_INTENT_AUDIT_PATH
    latest_output_path: Path = DEFAULT_LATEST_OUTPUT_PATH
    events_path: Path = DEFAULT_EVENTS_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_managed_position_hold_exit_shadow(
    *,
    config: ManagedPositionHoldExitShadowConfig,
    managed_position_registry: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    registry = dict(managed_position_registry or _read_json(config.resolve(config.managed_position_registry_path)))
    intent_audit = _read_json(config.resolve(config.position_intent_audit_path))
    intents_by_strategy = _intents_by_strategy(intent_audit)
    managed_positions = _open_managed_positions(registry)
    rows = [
        build_managed_position_shadow_row(
            position=position,
            intents_by_strategy=intents_by_strategy,
            generated_at=actual_now,
        )
        for position in managed_positions
    ]
    classification = (
        MANAGED_POSITION_HOLD_EXIT_SHADOW_READY
        if rows
        else MANAGED_POSITION_HOLD_EXIT_SHADOW_NO_OPEN_POSITIONS
    )
    return {
        "schema_version": "track_b_managed_position_hold_exit_shadow_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        **NO_AUTHORITY_FLAGS,
        "summary": {
            "managed_position_count": len(managed_positions),
            "recommendation_count": len(rows),
            "eligible_for_alpha_exit_analysis_count": sum(
                1 for row in rows if row.get("eligible_for_alpha_exit_analysis") is True
            ),
            "ineligible_for_alpha_exit_analysis_count": sum(
                1 for row in rows if row.get("eligible_for_alpha_exit_analysis") is False
            ),
            "hold_state_counts": dict(Counter(str(row.get("hold_state")) for row in rows)),
            "recommendation_counts": dict(Counter(str(row.get("shadow_exit_recommendation")) for row in rows)),
            "source_managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "source_position_intent_audit": str(config.resolve(config.position_intent_audit_path)),
        },
        "managed_position_recommendations": rows,
        "evidence_gaps": sorted({gap for row in rows for gap in row.get("evidence_gaps", [])}),
        "artifact_paths": {
            "latest": str(config.resolve(config.latest_output_path)),
            "events": str(config.resolve(config.events_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "position_intent_audit": str(config.resolve(config.position_intent_audit_path)),
        },
    }


def build_managed_position_shadow_row(
    *,
    position: Mapping[str, Any],
    intents_by_strategy: Mapping[tuple[str, str], Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    intent = _intent_for_position(position, intents_by_strategy)
    evidence = _evidence_for_managed_position(position)
    recommendation = evaluate_hold_exit_shadow(
        position_intent=intent,
        evidence=evidence,
        generated_at=generated_at,
    )
    contamination_flags = _contamination_flags(position=position, intent=intent)
    eligible = _eligible_for_alpha_exit_analysis(position=position, intent=intent, contamination_flags=contamination_flags)
    evidence_gaps = _evidence_gaps(recommendation)
    strategy_policy = _mapping(intent.get("strategy_hold_exit_policy")) or strategy_hold_exit_policy_for(
        str(position.get("strategy_id") or "")
    )
    return {
        "generated_at": generated_at.isoformat(),
        "strategy_id": position.get("strategy_id") or intent.get("strategy_id"),
        "lane_id": position.get("lane_id") or intent.get("lane_id"),
        "lifecycle_id": position.get("lifecycle_id"),
        "classification": position.get("classification"),
        "instrument_family": intent.get("instrument_family") or position.get("symbol"),
        "local_symbol": position.get("local_symbol"),
        "con_id": position.get("con_id"),
        "side": position.get("side") or intent.get("side"),
        "quantity": position.get("quantity") or intent.get("quantity"),
        "assigned_live_exit_profile": _nested(intent, "exit_policy", "exit_profile_id"),
        "assigned_live_exit_policy": position.get("managed_exit_policy_id")
        or _nested(intent, "exit_policy", "managed_exit_policy_id"),
        "position_intent_summary": _position_intent_summary(intent),
        "policy_registry_mapping": strategy_policy,
        "hold_state": recommendation.get("hold_state"),
        "shadow_exit_recommendation": recommendation.get("recommendation"),
        "recommendation_reason": recommendation.get("basis") or [],
        "evidence_used": {
            "completed_5m_bars_since_entry": recommendation.get("completed_5m_bars_since_entry"),
            "mfe_points": recommendation.get("mfe_points"),
            "mae_points": recommendation.get("mae_points"),
            "current_net_points": recommendation.get("current_net_points"),
            "giveback_from_mfe_points": recommendation.get("giveback_from_mfe_points"),
            "participation_state": recommendation.get("participation_state"),
            "microtrend_state": recommendation.get("microtrend_state"),
            "session_label": recommendation.get("session_label"),
            "regime_label": recommendation.get("regime_label"),
        },
        "evidence_gaps": evidence_gaps,
        "eligible_for_alpha_exit_analysis": eligible,
        "exclusion_reason": None if eligible else _exclusion_reason(contamination_flags),
        "contamination_flags": contamination_flags,
        "recommendation_only": True,
        **NO_AUTHORITY_FLAGS,
    }


def decorate_managed_positions_with_hold_exit_shadow(
    *,
    managed_positions: Sequence[Mapping[str, Any]],
    config: ManagedPositionHoldExitShadowConfig,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    intent_audit = _read_json(config.resolve(config.position_intent_audit_path))
    intents_by_strategy = _intents_by_strategy(intent_audit)
    decorated: list[dict[str, Any]] = []
    for position in managed_positions:
        row = dict(position)
        row["hold_exit_shadow"] = build_managed_position_shadow_row(
            position=position,
            intents_by_strategy=intents_by_strategy,
            generated_at=actual_now,
        )
        decorated.append(row)
    return decorated


def write_managed_position_hold_exit_shadow(
    *,
    config: ManagedPositionHoldExitShadowConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.latest_output_path)
    write_json_atomic(output_path, to_jsonable(dict(payload)))
    _append_jsonl(config.resolve(config.events_path), payload)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build read-only managed-position Hold/Exit shadow visibility.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ManagedPositionHoldExitShadowConfig(repo_root=Path(args.repo_root).expanduser().resolve())
    payload = build_managed_position_hold_exit_shadow(config=config)
    write_managed_position_hold_exit_shadow(config=config, payload=payload)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']}: {payload['summary']['recommendation_count']} recommendations")
    return 0


def _open_managed_positions(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for position in registry.get("managed_positions") or []:
        if not isinstance(position, Mapping):
            continue
        classification = str(position.get("classification") or "")
        if classification == "NO_MANAGED_POSITIONS":
            continue
        rows.append(dict(position))
    return rows


def _intents_by_strategy(audit: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in audit.get("strategies") or []:
        if not isinstance(row, Mapping):
            continue
        intent = row.get("position_intent")
        if not isinstance(intent, Mapping):
            continue
        payload = dict(intent)
        strategy_policy = row.get("strategy_hold_exit_policy")
        if isinstance(strategy_policy, Mapping):
            payload["strategy_hold_exit_policy"] = dict(strategy_policy)
        result[(str(payload.get("strategy_id") or ""), str(payload.get("lane_id") or ""))] = payload
    return result


def _intent_for_position(
    position: Mapping[str, Any],
    intents_by_strategy: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    strategy_id = str(position.get("strategy_id") or "")
    lane_id = str(position.get("lane_id") or "")
    if (strategy_id, lane_id) in intents_by_strategy:
        return dict(intents_by_strategy[(strategy_id, lane_id)])
    for (known_strategy, _known_lane), intent in intents_by_strategy.items():
        if known_strategy == strategy_id:
            return dict(intent)
    return {
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "instrument_family": position.get("symbol") or position.get("instrument_family"),
        "side": position.get("side"),
        "quantity": position.get("quantity"),
        "strategy_hold_exit_policy": strategy_hold_exit_policy_for(strategy_id),
    }


def _evidence_for_managed_position(position: Mapping[str, Any]) -> dict[str, Any]:
    mfe = _first(position, "mfe_points", "mfe", "max_favorable_excursion")
    current_net = _first(position, "current_net_points", "unrealized_points", "unrealized_pnl_points")
    return {
        "strategy_id": position.get("strategy_id"),
        "lane_id": position.get("lane_id"),
        "source_type": "MANAGED_POSITION_REGISTRY",
        "lifecycle_id": position.get("lifecycle_id"),
        "instrument_family": position.get("symbol") or position.get("instrument_family"),
        "side": position.get("side"),
        "quantity": position.get("quantity"),
        "completed_5m_bars_since_entry": _first(
            position,
            "completed_5m_bars_since_entry",
            "bars_since_entry",
            "completed_5m_bars_since_signal",
        ),
        "mfe_points": mfe,
        "mae_points": _first(position, "mae_points", "mae", "max_adverse_excursion"),
        "current_net_points": current_net,
        "giveback_from_mfe_points": _giveback(mfe=mfe, current_net=current_net),
        "participation_state": _first(position, "participation_state", "continuation_participation_state"),
        "microtrend_state": _first(position, "microtrend_state", "continuation_microtrend_state"),
        "session_label": position.get("session_label") or position.get("session"),
        "regime_label": position.get("regime_label") or position.get("regime"),
        "actual_exit_policy": position.get("managed_exit_policy_id") or position.get("exit_policy_id"),
    }


def _position_intent_summary(intent: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": intent.get("strategy_id"),
        "lane_id": intent.get("lane_id"),
        "thesis_type": _nested(intent, "trade_thesis", "thesis_type"),
        "thesis_summary": _nested(intent, "trade_thesis", "thesis_summary"),
        "expected_hold_type": _nested(intent, "hold_policy", "expected_hold_type"),
        "intended_exit_family": _nested(intent, "exit_policy", "intended_exit_family"),
        "pyramiding_policy": intent.get("pyramiding_policy"),
        "conflict_group": intent.get("conflict_group"),
    }


def _contamination_flags(*, position: Mapping[str, Any], intent: Mapping[str, Any]) -> list[str]:
    flags: set[str] = set()
    raw_flags = position.get("contamination_flags")
    if isinstance(raw_flags, Sequence) and not isinstance(raw_flags, (str, bytes, bytearray)):
        flags.update(str(item).upper() for item in raw_flags if str(item).strip())
    tags = _mapping(intent.get("attribution_tags"))
    tag_flags = tags.get("contamination_flags")
    if isinstance(tag_flags, Sequence) and not isinstance(tag_flags, (str, bytes, bytearray)):
        flags.update(str(item).upper() for item in tag_flags if str(item).strip())
    marker_fields = {
        "leak_test_trade": "LEAK_TEST_TRADE",
        "remediation_trade": "REMEDIATION_TRADE",
        "operator_supervised_cleanup": "OPERATOR_SUPERVISED_CLEANUP",
        "lifecycle_bug_resolution": "LIFECYCLE_BUG_RESOLUTION",
        "duplicate_exit_resolution": "DUPLICATE_EXIT_RESOLUTION",
        "aggregate_close_repair": "AGGREGATE_CLOSE_REPAIR",
        "broker_flat_reconciliation_cleanup": "BROKER_FLAT_RECONCILIATION_CLEANUP",
        "manual_intervention_required": "MANUAL_INTERVENTION_REQUIRED",
    }
    for field, flag in marker_fields.items():
        if position.get(field) is True:
            flags.add(flag)
    return sorted(flags)


def _eligible_for_alpha_exit_analysis(
    *,
    position: Mapping[str, Any],
    intent: Mapping[str, Any],
    contamination_flags: Sequence[str],
) -> bool:
    if any(str(flag).upper() in BUG_FIX_CONTAMINATION_FLAGS for flag in contamination_flags):
        return False
    if position.get("eligible_for_alpha_exit_analysis") is False:
        return False
    tags = _mapping(intent.get("attribution_tags"))
    return tags.get("eligible_for_alpha_exit_analysis") is not False


def _exclusion_reason(contamination_flags: Sequence[str]) -> str:
    if contamination_flags:
        return "contamination_flags_present"
    return "position_or_intent_ineligible_for_alpha_exit_analysis"


def _evidence_gaps(recommendation: Mapping[str, Any]) -> list[str]:
    if recommendation.get("hold_state") != INSUFFICIENT_EVIDENCE_STATE:
        return []
    gaps: list[str] = []
    for item in recommendation.get("basis") or []:
        text = str(item)
        if text.startswith("missing_evidence:"):
            gaps.append(text.removeprefix("missing_evidence:"))
    return gaps or ["insufficient_hold_exit_evidence"]


def _nested(payload: Mapping[str, Any], parent: str, field: str) -> Any:
    nested = payload.get(parent)
    return nested.get(field) if isinstance(nested, Mapping) else None


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in {None, ""}:
            return value
    return None


def _giveback(*, mfe: Any, current_net: Any) -> str | None:
    mfe_dec = _decimal(mfe)
    net_dec = _decimal(current_net)
    if mfe_dec is None or net_dec is None:
        return None
    return str(max(Decimal("0"), mfe_dec - net_dec).normalize())


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(dict(payload)), sort_keys=True) + "\n")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
