from __future__ import annotations

import os
from pathlib import Path

from mgc_v05l.app.track_b_paper_leak_test import (
    LeakTestSafetySnapshot,
    _unresolved_state_blockers,
)


def _clean_safety_snapshot(**overrides: object) -> LeakTestSafetySnapshot:
    payload = {
        "account_id": "DUM882026",
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "broker_reconciled": True,
        "review_required_count": 0,
        "open_order_count": 0,
        "broker_position_count": 0,
        "lifecycle_position_count": 0,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "runtime_pid": os.getpid(),
        "runtime_pid_active": True,
        "runtime_cwd": None,
        "runtime_command": None,
        "runtime_from_dev_root": True,
        "runtime_from_documents_or_icloud": False,
        "duplicate_runtime_submitter_count": 0,
        "active_leak_test_lane_id": None,
        "existing_positions": (),
        "existing_open_orders": (),
    }
    payload.update(overrides)
    return LeakTestSafetySnapshot(**payload)


def test_archived_repo_root_blocks_even_when_it_resolves_to_active_root() -> None:
    blockers = _unresolved_state_blockers(
        repo_root=Path("/Users/patrick/Documents/MGC-v05l-automation"),
        safety=_clean_safety_snapshot(),
    )

    assert "repo_root_not_dev_checkout" in blockers
