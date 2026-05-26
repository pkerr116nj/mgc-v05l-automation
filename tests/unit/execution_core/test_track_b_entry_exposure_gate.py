from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_entry_exposure_gate import (
    ENTRY_EXPOSURE_BLOCKED_BROKER_LIFECYCLE_MISMATCH,
    ENTRY_EXPOSURE_GATE_ALLOWED,
    INSTRUMENT_EXPOSURE_CAP_BLOCKED,
    OPPOSITE_SIDE_EXPOSURE_BLOCKED,
    PYRAMIDING_ALLOWED,
    SAME_LANE_REENTRY_BLOCKED_MAX_UNITS_PER_LANE,
    SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED,
    TrackBEntryExposureGateConfig,
    evaluate_track_b_entry_exposure_gate,
)


def test_same_lane_second_entry_blocked_when_pyramiding_not_allowed(tmp_path: Path) -> None:
    _write_registry(tmp_path, [_unit("life-1", strategy_id="s1", lane_id="lane-a", side="SHORT")])

    result = evaluate_track_b_entry_exposure_gate(
        _config(tmp_path, strategy_id="s1", lane_id="lane-a", side="SHORT")
    )

    assert result["classification"] == SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED
    assert result["allowed"] is False
    assert result["current_units_for_lane"] == 1
    assert result["pyramiding_policy"] == "PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED"


def test_strategy_id_lane_fallback_still_blocks_same_lane_reentry(tmp_path: Path) -> None:
    _write_registry(
        tmp_path,
        [
            _unit(
                "life-1",
                strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
                lane_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
                side="SHORT",
            )
        ],
    )

    result = evaluate_track_b_entry_exposure_gate(
        _config(
            tmp_path,
            strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            lane_id="mnq_first_bear_snap_turn",
            side="SHORT",
        )
    )

    assert result["classification"] == SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED
    assert result["same_lane_open_unit_count"] == 1


def test_same_lane_reentry_allowed_when_pyramiding_allowed_under_max_units(tmp_path: Path) -> None:
    _write_registry(tmp_path, [_unit("life-1", strategy_id="s1", lane_id="lane-a", side="SHORT")])

    result = evaluate_track_b_entry_exposure_gate(
        _config(
            tmp_path,
            strategy_id="s1",
            lane_id="lane-a",
            side="SHORT",
            pyramiding_policy=PYRAMIDING_ALLOWED,
            max_units_per_lane=3,
        )
    )

    assert result["classification"] == ENTRY_EXPOSURE_GATE_ALLOWED
    assert result["allowed"] is True
    assert result["current_units_for_lane"] == 1


def test_same_lane_reentry_blocks_when_pyramiding_max_units_exceeded(tmp_path: Path) -> None:
    _write_registry(
        tmp_path,
        [
            _unit("life-1", strategy_id="s1", lane_id="lane-a", side="SHORT"),
            _unit("life-2", strategy_id="s1", lane_id="lane-a", side="SHORT"),
        ],
    )

    result = evaluate_track_b_entry_exposure_gate(
        _config(
            tmp_path,
            strategy_id="s1",
            lane_id="lane-a",
            side="SHORT",
            pyramiding_policy=PYRAMIDING_ALLOWED,
            max_units_per_lane=2,
        )
    )

    assert result["classification"] == SAME_LANE_REENTRY_BLOCKED_MAX_UNITS_PER_LANE
    assert result["allowed"] is False


def test_distinct_lane_same_direction_allowed_within_instrument_max(tmp_path: Path) -> None:
    _write_registry(tmp_path, [_unit("life-1", strategy_id="s1", lane_id="lane-a", side="SHORT")])

    result = evaluate_track_b_entry_exposure_gate(
        _config(tmp_path, strategy_id="s2", lane_id="lane-b", side="SHORT")
    )

    assert result["classification"] == ENTRY_EXPOSURE_GATE_ALLOWED
    assert result["instrument_family_exposure"] == "-1"


def test_opposite_side_same_contract_blocks(tmp_path: Path) -> None:
    _write_registry(tmp_path, [_unit("life-1", strategy_id="s1", lane_id="lane-a", side="LONG")])

    result = evaluate_track_b_entry_exposure_gate(
        _config(tmp_path, strategy_id="s2", lane_id="lane-b", side="SHORT")
    )

    assert result["classification"] == OPPOSITE_SIDE_EXPOSURE_BLOCKED
    assert result["opposite_side_open_unit_count"] == 1


def test_mnq_paper_exposure_cap_enforced(tmp_path: Path) -> None:
    _write_registry(
        tmp_path,
        [_unit(f"life-{idx}", strategy_id=f"s{idx}", lane_id=f"lane-{idx}", side="SHORT") for idx in range(5)],
    )

    result = evaluate_track_b_entry_exposure_gate(
        _config(tmp_path, strategy_id="s6", lane_id="lane-6", side="SHORT")
    )

    assert result["classification"] == INSTRUMENT_EXPOSURE_CAP_BLOCKED
    assert result["max_paper_exposure_for_instrument"] == 5


def test_broker_lifecycle_mismatch_blocks(tmp_path: Path) -> None:
    unit = _unit("life-1", strategy_id="s1", lane_id="lane-a", side="SHORT")
    unit["broker_qty_match"] = False
    _write_registry(tmp_path, [unit])

    result = evaluate_track_b_entry_exposure_gate(
        _config(tmp_path, strategy_id="s2", lane_id="lane-b", side="SHORT")
    )

    assert result["classification"] == ENTRY_EXPOSURE_BLOCKED_BROKER_LIFECYCLE_MISMATCH
    assert result["allowed"] is False


def _config(tmp_path: Path, **overrides: object) -> TrackBEntryExposureGateConfig:
    payload = {
        "repo_root": tmp_path,
        "account_id": "DUM882026",
        "strategy_id": "s2",
        "lane_id": "lane-b",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "SHORT",
        "quantity": 1,
    }
    payload.update(overrides)
    return TrackBEntryExposureGateConfig(**payload)


def _unit(lifecycle_id: str, *, strategy_id: str, lane_id: str, side: str) -> dict[str, object]:
    signed_qty = "-1" if side == "SHORT" else "1"
    return {
        "lifecycle_id": lifecycle_id,
        "entry_intent_id": lifecycle_id,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "account_id": "DUM882026",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": side,
        "quantity": "1",
        "signed_qty": signed_qty,
        "exit_status": "OPEN_MANAGED",
    }


def _write_registry(tmp_path: Path, units: list[dict[str, object]]) -> None:
    position = {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": "DUM882026",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "SHORT",
        "aggregate_qty": str(sum(int(str(unit["signed_qty"])) for unit in units)),
        "lifecycle_unit_count": len(units),
        "lifecycle_units": units,
        "broker_qty_match": True,
    }
    path = tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "OPEN_MANAGED_MATCHED",
                "managed_positions": [position],
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
