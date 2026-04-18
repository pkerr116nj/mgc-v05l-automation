from __future__ import annotations

import json

from mgc_v05l.app.ibkr_milestone_a_check import main


def test_ibkr_milestone_a_check_reads_snapshot_and_returns_result(tmp_path) -> None:
    snapshot_path = tmp_path / "ibkr_snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "provider_id": "ibkr_execution",
                "selected_account_id": "DU1234567",
                "accounts": [{"account_id": "DU1234567"}],
                "balances": [{"account_id": "DU1234567"}],
                "positions": [],
                "open_orders": [],
                "completed_orders": [],
                "executions": [],
                "health": {"connected": True, "checked_at": "2026-04-12T16:00:00+00:00"},
                "generated_at": "2026-04-12T16:00:00+00:00",
                "metadata": {"visibility_scope": {"managed_accounts": ["DU1234567"]}},
                "connected": True,
                "truth_complete": True,
                "position_quantity": 0,
                "open_order_ids": [],
                "order_status": {},
                "last_fill_timestamp": None,
            }
        ),
        encoding="utf-8",
    )

    result = main(["--snapshot", str(snapshot_path)])

    assert result["ready"] is True
