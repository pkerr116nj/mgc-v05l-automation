"""Read-only Track B autonomous lifecycle validation report.

The report validates the instrument-agnostic managed lifecycle path across
active Track B instruments without submitting, cancelling, closing, starting
services, restarting runtime, or mutating broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_managed_exit_pipeline_dry_run import (
    TrackBManagedExitPipelineDryRunConfig,
    build_track_b_managed_exit_pipeline_dry_run_report,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
LIFECYCLE_VALIDATION_SCHEMA_VERSION = "track_b_lifecycle_validation_report_v1"

DEFAULT_LIFECYCLE_VALIDATION_REPORT = (
    Path("outputs")
    / "track_b_execution_core"
    / "lifecycle_validation"
    / "latest_track_b_lifecycle_validation.json"
)
DEFAULT_APPROVED_PROFILE_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_recovery" / "approved_paper_stack_profile.json"
)
DEFAULT_PAPER_RUNTIME_DIR = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime"
DEFAULT_PAPER_CONFIG_PATH = Path("config") / "probationary_pattern_engine_paper.yaml"
DEFAULT_ODS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "operator_decision_surface"
    / "latest_operator_decision_surface.json"
)

GROUP_1 = {"MES", "MNQ", "MGC"}
GROUP_2 = {"ES", "NQ", "GC"}


class LifecycleValidationClassification(str, Enum):
    WAITING_FOR_ENTRY = "WAITING_FOR_ENTRY"
    ENTRY_OPEN_POSITION_SEEN = "ENTRY_OPEN_POSITION_SEEN"
    HOLDING = "HOLDING"
    EXIT_INTENT_READY = "EXIT_INTENT_READY"
    EXIT_AUTHORITY_ALLOWED = "EXIT_AUTHORITY_ALLOWED"
    EXIT_AUTHORITY_BLOCKED = "EXIT_AUTHORITY_BLOCKED"
    CLOSE_SUBMITTED_PENDING_SETTLEMENT = "CLOSE_SUBMITTED_PENDING_SETTLEMENT"
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class TrackBLifecycleValidationConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_LIFECYCLE_VALIDATION_REPORT
    approved_profile_path: Path = DEFAULT_APPROVED_PROFILE_ARTIFACT
    paper_runtime_dir: Path = DEFAULT_PAPER_RUNTIME_DIR
    paper_config_path: Path = DEFAULT_PAPER_CONFIG_PATH
    ods_path: Path = DEFAULT_ODS_PATH
    execution_domain: str = "TRACK_B_PAPER"
    account_id: str = "DUM882026"

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_lifecycle_validation_report(
    *,
    config: TrackBLifecycleValidationConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Any] | None = None,
    decision_inputs: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = require_aware_datetime(now or datetime.now(UTC), "now")
    overrides = dict(input_overrides or {})
    pipeline = dict(
        overrides.get("managed_exit_pipeline")
        or build_track_b_managed_exit_pipeline_dry_run_report(
            config=TrackBManagedExitPipelineDryRunConfig(
                repo_root=config.repo_root,
                execution_domain=config.execution_domain,
                account_id=config.account_id,
            ),
            now=actual_now,
            input_overrides=_pipeline_overrides(overrides),
            decision_inputs=decision_inputs or {},
        )
    )
    ods = dict(overrides.get("ods") or _read_json(config.resolve(config.ods_path)))
    active_lanes = _active_lanes(config=config, overrides=overrides)
    active_instruments = _active_instruments(active_lanes=active_lanes, pipeline=pipeline, overrides=overrides)
    rows = [
        _validation_row(
            instrument=instrument,
            active_lanes=active_lanes,
            pipeline=pipeline,
            ods=ods,
            overrides=overrides,
        )
        for instrument in active_instruments
    ]
    return {
        "schema_version": LIFECYCLE_VALIDATION_SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "service_started": False,
        "runtime_restarted": False,
        "live_money_eligible": _flag_true(pipeline, "live_money_eligible") or _flag_true(ods, "live_money_eligible"),
        "paper_proof_invoked": _flag_true(pipeline, "paper_proof_invoked") or _flag_true(ods, "paper_proof_invoked"),
        "classification_counts": _classification_counts(rows),
        "instrument_count": len(rows),
        "active_instruments": active_instruments,
        "groups": {
            "1": [row["instrument"] for row in rows if row["group"] == 1],
            "2": [row["instrument"] for row in rows if row["group"] == 2],
            "3": [row["instrument"] for row in rows if row["group"] == 3],
        },
        "rows": rows,
        "managed_exit_pipeline": pipeline,
        "ods_summary": _ods_summary(ods),
        "source_artifact_paths": {
            "approved_profile": str(config.resolve(config.approved_profile_path)),
            "paper_runtime_dir": str(config.resolve(config.paper_runtime_dir)),
            "paper_config": str(config.resolve(config.paper_config_path)),
            "ods": str(config.resolve(config.ods_path)),
            "output": str(config.resolve(config.output_path)),
        },
    }


def run_track_b_lifecycle_validation_report(
    *,
    config: TrackBLifecycleValidationConfig,
    now: datetime | None = None,
    write: bool = True,
) -> dict[str, Any]:
    payload = build_track_b_lifecycle_validation_report(config=config, now=now)
    if write:
        write_track_b_lifecycle_validation_report(config=config, payload=payload)
    return payload


def write_track_b_lifecycle_validation_report(
    *, config: TrackBLifecycleValidationConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def _validation_row(
    *,
    instrument: str,
    active_lanes: Sequence[Mapping[str, Any]],
    pipeline: Mapping[str, Any],
    ods: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> dict[str, Any]:
    instrument = instrument.upper()
    positions = _rows_for_instrument(_list(pipeline.get("positions")), instrument)
    decisions = _rows_for_instrument(_list(pipeline.get("decisions")), instrument)
    selections = _rows_for_instrument(_list(pipeline.get("selected_strategies")), instrument)
    intents = _rows_for_instrument(_list(pipeline.get("generated_exit_intents")), instrument)
    authority = _rows_for_instrument(_list(pipeline.get("exit_authority_decisions")), instrument)
    close_execution = _close_execution_summary(instrument=instrument, pipeline=pipeline, overrides=overrides)
    settlement = _settlement_summary(instrument=instrument, pipeline=pipeline, ods=ods, overrides=overrides)
    classification, failure_class = _row_classification(
        positions=positions,
        decisions=decisions,
        selections=selections,
        intents=intents,
        authority=authority,
        close_execution=close_execution,
        settlement=settlement,
        pipeline=pipeline,
    )
    return {
        "instrument": instrument,
        "group": _rollout_group(instrument),
        "execution_domain": str(pipeline.get("execution_domain") or "TRACK_B_PAPER"),
        "active_lane_count": len(_lanes_for_instrument(active_lanes, instrument)),
        "active_lanes": _lanes_for_instrument(active_lanes, instrument),
        "entry_evidence": _entry_evidence_summary(positions=positions, close_execution=close_execution),
        "position_state": _position_state_summary(positions),
        "exit_decision": _exit_decision_summary(decisions),
        "selected_exit_strategy": _selected_strategy_summary(selections),
        "exit_intent": _exit_intent_summary(intents),
        "exit_authority_decision": _authority_summary(authority),
        "close_execution": close_execution,
        "settlement": settlement,
        "ods": _ods_summary(ods),
        "classification": classification.value,
        "failure_class": failure_class,
        "operator_action_detected": _operator_action_detected(instrument=instrument, pipeline=pipeline, overrides=overrides),
        "historical_debris_diagnostic_only": True,
    }


def _row_classification(
    *,
    positions: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    selections: Sequence[Mapping[str, Any]],
    intents: Sequence[Mapping[str, Any]],
    authority: Sequence[Mapping[str, Any]],
    close_execution: Mapping[str, Any],
    settlement: Mapping[str, Any],
    pipeline: Mapping[str, Any],
) -> tuple[LifecycleValidationClassification, str | None]:
    if settlement.get("settled_flat") is True:
        if settlement.get("ods_flat") is True:
            return LifecycleValidationClassification.PASS, None
        return LifecycleValidationClassification.FAIL, "ods_projection_failed"
    if close_execution.get("close_submitted") is True and close_execution.get("close_filled") is not True:
        return LifecycleValidationClassification.CLOSE_SUBMITTED_PENDING_SETTLEMENT, None
    if not positions:
        return LifecycleValidationClassification.WAITING_FOR_ENTRY, None
    if _pipeline_failed(pipeline):
        return LifecycleValidationClassification.FAIL, "position_not_normalized"
    if not decisions:
        return LifecycleValidationClassification.FAIL, "exit_decision_not_generated"
    if all(str(row.get("action") or "") == "HOLD" for row in decisions):
        return LifecycleValidationClassification.HOLDING, None
    if _selector_failed(pipeline) or (not selections and any(str(row.get("action") or "") != "HOLD" for row in decisions)):
        return LifecycleValidationClassification.FAIL, "selector_failed"
    if not intents:
        return LifecycleValidationClassification.FAIL, "exit_intent_missing"
    if not authority:
        return LifecycleValidationClassification.EXIT_INTENT_READY, None
    if any(str(row.get("decision") or "") == "BLOCKED" for row in authority):
        return LifecycleValidationClassification.EXIT_AUTHORITY_BLOCKED, "v1_1_blocked"
    return LifecycleValidationClassification.EXIT_AUTHORITY_ALLOWED, None


def _active_lanes(
    *,
    config: TrackBLifecycleValidationConfig,
    overrides: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if isinstance(overrides.get("active_lanes"), list):
        return [_lane_summary(_mapping(row)) for row in _list(overrides.get("active_lanes"))]
    lanes = _lanes_from_approved_profile(config)
    if lanes:
        return lanes
    return _lanes_from_config(config.resolve(config.paper_config_path))


def _active_instruments(
    *,
    active_lanes: Sequence[Mapping[str, Any]],
    pipeline: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> list[str]:
    instruments: set[str] = {str(item).upper() for item in _list(overrides.get("active_instruments")) if str(item or "").strip()}
    for lane in active_lanes:
        instruments.update(str(item).upper() for item in _list(lane.get("observed_instruments")) if str(item or "").strip())
        symbol = str(lane.get("instrument") or "").strip().upper()
        if symbol:
            instruments.add(symbol)
    for key in ("positions", "decisions", "selected_strategies", "generated_exit_intents", "exit_authority_decisions"):
        for row in (_mapping(item) for item in _list(pipeline.get(key))):
            symbol = _instrument_from_row(row)
            if symbol:
                instruments.add(symbol)
    return sorted(instruments, key=lambda symbol: (_rollout_group(symbol), symbol))


def _lanes_from_approved_profile(config: TrackBLifecycleValidationConfig) -> list[dict[str, Any]]:
    approved = _read_json(config.resolve(config.approved_profile_path))
    profile = str(approved.get("approved_profile") or approved.get("recovery_requested_profile") or "").strip()
    if not profile:
        return []
    roster = _read_json(config.resolve(config.paper_runtime_dir) / f"paper_stack_{profile}_guarded_roster.json")
    strategy_ids = [
        *[str(item) for item in _list(roster.get("enabled_strategy_ids"))],
        *[str(item) for item in _list(roster.get("shadow_only_strategy_ids"))],
    ]
    lanes = [_lane_from_strategy_id(strategy_id) for strategy_id in strategy_ids]
    return [lane for lane in lanes if lane.get("instrument")]


def _lanes_from_config(path: Path) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    match = re.search(r"probationary_paper_lanes_json:\s*'(.+)'", text, flags=re.DOTALL)
    if not match:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    return [_lane_summary(_mapping(row)) for row in _list(payload)]


def _lane_from_strategy_id(strategy_id: str) -> dict[str, Any]:
    tokens = [token for token in re.split(r"[^A-Z0-9]+", strategy_id.upper()) if token]
    instrument = next((token for token in tokens if token in {"MES", "MNQ", "MGC", "ES", "NQ", "GC"}), "")
    return {
        "lane_id": strategy_id,
        "strategy_id": strategy_id,
        "instrument": instrument,
        "observed_instruments": [instrument] if instrument else [],
        "source": "guarded_roster_strategy_id",
    }


def _lane_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    observed = [str(item).upper() for item in _list(row.get("observed_instruments")) if str(item or "").strip()]
    symbol = str(row.get("symbol") or row.get("instrument") or (observed[0] if observed else "")).upper()
    return {
        "lane_id": row.get("lane_id") or row.get("strategy_id") or row.get("standalone_strategy_id"),
        "strategy_id": row.get("strategy_id") or row.get("standalone_strategy_id"),
        "instrument": symbol,
        "observed_instruments": observed or ([symbol] if symbol else []),
        "managed_exit_policy_id": row.get("managed_exit_policy_id"),
        "paper_only": row.get("paper_only"),
        "live_money_eligible": row.get("live_money_eligible") is True,
    }


def _entry_evidence_summary(*, positions: Sequence[Mapping[str, Any]], close_execution: Mapping[str, Any]) -> dict[str, Any]:
    if not positions:
        return {"entry_seen": False, "source": None, "fill": None, "operator_action": False}
    first = _mapping(positions[0])
    return {
        "entry_seen": True,
        "source": "position_state",
        "fill": _first_present(first, ("entry_fill", "fill", "entry")),
        "operator_action": bool(close_execution.get("operator_action_detected")),
    }


def _position_state_summary(positions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not positions:
        return {"position_count": 0, "classification": "NO_CURRENT_POSITION"}
    return {
        "position_count": len(positions),
        "classification": "CURRENT_POSITION",
        "positions": [
            {
                "local_symbol": row.get("local_symbol"),
                "con_id": row.get("con_id"),
                "side": row.get("side"),
                "qty": row.get("qty"),
                "attribution_status": row.get("attribution_status"),
                "lifecycle_id": row.get("lifecycle_id"),
                "trade_id": row.get("trade_id"),
            }
            for row in positions
        ],
    }


def _exit_decision_summary(decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not decisions:
        return {"decision_count": 0, "actions": []}
    return {"decision_count": len(decisions), "actions": [row.get("action") for row in decisions], "decisions": list(decisions)}


def _selected_strategy_summary(selections: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not selections:
        return {"selected_count": 0, "strategy_types": []}
    return {"selected_count": len(selections), "strategy_types": [row.get("strategy_type") for row in selections], "selected": list(selections)}


def _exit_intent_summary(intents: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not intents:
        return {"intent_count": 0}
    return {
        "intent_count": len(intents),
        "intents": [
            {
                "exit_intent_id": row.get("exit_intent_id"),
                "local_symbol": row.get("local_symbol"),
                "close_action": row.get("close_action"),
                "close_qty": row.get("close_qty"),
                "exit_type": row.get("exit_type"),
                "idempotency_key": row.get("idempotency_key"),
            }
            for row in intents
        ],
    }


def _authority_summary(authority: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not authority:
        return {"decision_count": 0}
    return {
        "decision_count": len(authority),
        "decisions": [
            {
                "exit_intent_id": row.get("exit_intent_id"),
                "decision": row.get("decision"),
                "attribution_status": row.get("attribution_status"),
                "block_reasons": row.get("block_reasons") or [],
            }
            for row in authority
        ],
    }


def _close_execution_summary(
    *,
    instrument: str,
    pipeline: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> dict[str, Any]:
    explicit = _mapping(_mapping(overrides.get("close_execution_by_instrument")).get(instrument))
    if explicit:
        return {
            "close_submitted": explicit.get("close_submitted") is True,
            "close_filled": explicit.get("close_filled") is True,
            "order_id": explicit.get("order_id"),
            "perm_id": explicit.get("perm_id"),
            "source": explicit.get("source") or "override",
            "operator_action_detected": explicit.get("operator_action_detected") is True,
        }
    managed_orders = _mapping(pipeline.get("managed_exit_pipeline", {})).get("managed_orders")
    rows = _rows_for_instrument(_list(_mapping(managed_orders).get("managed_orders")), instrument)
    submitted = next((row for row in rows if _mapping(row.get("close_submit_attempt")).get("submitted") is True), None)
    working = next((row for row in rows if row.get("working") is True), None)
    return {
        "close_submitted": bool(submitted or working),
        "close_filled": False,
        "order_id": _mapping(_mapping(submitted or {}).get("close_submit_attempt")).get("broker_order_id"),
        "perm_id": _mapping(_mapping(submitted or {}).get("close_submit_attempt")).get("perm_id"),
        "source": "managed_orders" if rows else None,
        "operator_action_detected": False,
    }


def _settlement_summary(
    *,
    instrument: str,
    pipeline: Mapping[str, Any],
    ods: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> dict[str, Any]:
    explicit = _mapping(_mapping(overrides.get("settlement_by_instrument")).get(instrument))
    if explicit:
        settled_flat = explicit.get("settled_flat") is True
        return {
            "settled_flat": settled_flat,
            "reconciliation_clean": explicit.get("reconciliation_clean") is True,
            "managed_clean": explicit.get("managed_clean") is True,
            "ods_flat": _ods_flat(ods) if "ods_flat" not in explicit else explicit.get("ods_flat") is True,
            "source": explicit.get("source") or "override",
        }
    positions = _rows_for_instrument(_list(pipeline.get("positions")), instrument)
    settled_flat = not positions and _ods_flat(ods) and _pipeline_flat_clean(pipeline)
    return {
        "settled_flat": settled_flat and _has_close_evidence(instrument=instrument, overrides=overrides),
        "reconciliation_clean": _source_clean(pipeline, "reconciliation"),
        "managed_clean": _source_clean(pipeline, "managed_positions") and _source_clean(pipeline, "managed_orders"),
        "ods_flat": _ods_flat(ods),
        "source": "current_authority",
    }


def _ods_summary(ods: Mapping[str, Any]) -> dict[str, Any]:
    submit = _mapping(ods.get("submit_allowed"))
    return {
        "broker_state": ods.get("broker_state"),
        "first_blocker": ods.get("first_blocker"),
        "next_safe_action": ods.get("next_safe_action"),
        "submit_allowed": submit.get("submit_allowed") if submit else ods.get("submit_allowed"),
        "generated_at": ods.get("generated_at"),
    }


def _operator_action_detected(*, instrument: str, pipeline: Mapping[str, Any], overrides: Mapping[str, Any]) -> bool:
    explicit = _mapping(_mapping(overrides.get("operator_actions_by_instrument")).get(instrument))
    if explicit:
        return explicit.get("operator_action_detected") is True
    return _mapping(_close_execution_summary(instrument=instrument, pipeline=pipeline, overrides=overrides)).get("operator_action_detected") is True


def _pipeline_overrides(overrides: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    names = {
        "reconciliation",
        "managed_positions",
        "managed_orders",
        "open_order_truth",
        "broker_session_authority",
        "guardian",
        "safe_state",
        "exit_decision",
    }
    return {name: _mapping(overrides[name]) for name in names if isinstance(overrides.get(name), Mapping)}


def _rows_for_instrument(rows: Sequence[Any], instrument: str) -> list[dict[str, Any]]:
    return [row for row in (_mapping(item) for item in rows) if _instrument_from_row(row) == instrument]


def _lanes_for_instrument(lanes: Sequence[Mapping[str, Any]], instrument: str) -> list[dict[str, Any]]:
    return [
        dict(lane)
        for lane in lanes
        if str(lane.get("instrument") or "").upper() == instrument
        or instrument in {str(item).upper() for item in _list(lane.get("observed_instruments"))}
    ]


def _instrument_from_row(row: Mapping[str, Any]) -> str:
    for key in ("instrument", "symbol", "track_b_root"):
        text = str(row.get(key) or "").strip().upper()
        if text:
            return text
    local_symbol = str(row.get("local_symbol") or row.get("contract") or "").strip().upper()
    return _instrument_from_local_symbol(local_symbol)


def _instrument_from_local_symbol(local_symbol: str) -> str:
    match = re.match(r"^([A-Z]+?)([FGHJKMNQUVXZ][0-9]+)$", local_symbol)
    return (match.group(1) if match else local_symbol).upper()


def _rollout_group(instrument: str) -> int:
    if instrument in GROUP_1:
        return 1
    if instrument in GROUP_2:
        return 2
    return 3


def _source_clean(pipeline: Mapping[str, Any], source_name: str) -> bool:
    classification = str(_mapping(pipeline.get("source_classifications")).get(source_name) or "")
    return classification in {
        "TRACK_B_PAPER_BROKER_RECONCILED",
        "NO_MANAGED_POSITIONS",
        "NO_MANAGED_ORDERS",
        "NO_OPEN_ORDERS",
    }


def _pipeline_flat_clean(pipeline: Mapping[str, Any]) -> bool:
    return str(pipeline.get("classification") or "") == "NO_POSITIONS" and all(
        _source_clean(pipeline, name) for name in ("reconciliation", "managed_positions", "managed_orders", "open_order_truth")
    )


def _has_close_evidence(*, instrument: str, overrides: Mapping[str, Any]) -> bool:
    evidence = _mapping(_mapping(overrides.get("settlement_by_instrument")).get(instrument))
    return evidence.get("settled_flat") is True


def _ods_flat(ods: Mapping[str, Any]) -> bool:
    return str(ods.get("broker_state") or "") == "FLAT"


def _pipeline_failed(pipeline: Mapping[str, Any]) -> bool:
    return str(pipeline.get("classification") or "") == "PIPELINE_ERROR"


def _selector_failed(pipeline: Mapping[str, Any]) -> bool:
    selector = _mapping(pipeline.get("exit_strategy_selector"))
    return str(selector.get("classification") or "") == "EXIT_STRATEGY_SELECTOR_REVIEW_REQUIRED"


def _first_present(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if row.get(key) is not None:
            return row.get(key)
    return None


def _classification_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        classification.value: sum(1 for row in rows if row.get("classification") == classification.value)
        for classification in LifecycleValidationClassification
    }


def _flag_true(payload: Mapping[str, Any], key: str) -> bool:
    return payload.get(key) is True


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_LIFECYCLE_VALIDATION_REPORT)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBLifecycleValidationConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = run_track_b_lifecycle_validation_report(config=config, write=not args.no_write)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"instrument_count={payload.get('instrument_count')}")
        print(f"classification_counts={payload.get('classification_counts')}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
