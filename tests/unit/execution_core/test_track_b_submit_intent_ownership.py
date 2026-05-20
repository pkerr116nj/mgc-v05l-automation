from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    SubmitIntentOwnershipError,
    SubmitIntentOwnershipRecord,
    SubmitIntentOwnershipState,
    append_submit_intent_ownership_record,
    load_unresolved_submit_intent_ownership_records,
    submit_intent_ownership_digest,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)


def base_record(**overrides: object) -> SubmitIntentOwnershipRecord:
    values = {
        "mode": "PAPER",
        "account_id": "DUM882026",
        "lane_id": "atp_companion_v1_asia_us",
        "strategy_id": "atp_companion_v1__benchmark_mgc_asia_us",
        "intent_type": "BUY_TO_OPEN",
        "action": "BUY",
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "expiry": "20260626",
        "con_id": 712565978,
        "qty": 1,
        "order_type": "LMT",
        "limit_price": "4543.2",
        "time_in_force": "DAY",
        "repo_root": "/Users/patrick/Dev/MGC-v05l-automation",
        "git_head": "abc123",
        "created_at": NOW,
        "caller_path": "track_b_paper_leak_test_apply",
        "authorization_digest": "digest-1",
        "execution_price_source": "RUNTIME_DATABENTO_1M_CLOSE",
        "pre_submit_reconciliation_classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "governance_classification": "PAPER_STRATEGY_GOVERNANCE_READY",
        "exposure_classification": "PAPER_EXPOSURE_ENTRY_ALLOWED",
    }
    values.update(overrides)
    return SubmitIntentOwnershipRecord(**values)


def test_valid_paper_intent_writes_jsonl_and_latest_view(tmp_path: Path) -> None:
    result = append_submit_intent_ownership_record(
        base_record(),
        jsonl_path=tmp_path / "ownership.jsonl",
        latest_path=tmp_path / "latest.json",
    )

    rows = [json.loads(line) for line in result.jsonl_path.read_text(encoding="utf-8").splitlines()]
    latest = json.loads(result.latest_path.read_text(encoding="utf-8"))

    assert len(rows) == 1
    assert rows[0]["schema_version"] == "track_b_submit_intent_ownership_v1"
    assert rows[0]["state"] == "PRE_SUBMIT_INTENT_DURABLE"
    assert rows[0]["account_id"] == "DUM882026"
    assert rows[0]["live_money_eligible"] is False
    assert rows[0]["paper_proof_invoked"] is False
    assert rows[0]["digest"]
    assert latest["record_count"] == 1
    assert latest["unresolved_count"] == 1
    assert latest["latest_record"]["ownership_intent_id"] == rows[0]["ownership_intent_id"]


def test_live_money_eligible_true_rejected() -> None:
    with pytest.raises(SubmitIntentOwnershipError, match="live_money_eligible=false"):
        base_record(live_money_eligible=True).to_payload()


def test_paper_proof_invoked_true_rejected() -> None:
    with pytest.raises(SubmitIntentOwnershipError, match="paper_proof_invoked=false"):
        base_record(paper_proof_invoked=True).to_payload()


@pytest.mark.parametrize(
    "field",
    ["account_id", "mode", "local_symbol", "action", "qty"],
)
def test_missing_account_mode_contract_action_qty_rejected(field: str) -> None:
    with pytest.raises(SubmitIntentOwnershipError):
        base_record(**{field: ""}).to_payload()


def test_digest_changes_if_critical_identity_changes() -> None:
    first = base_record().to_payload()
    second = base_record(local_symbol="MNQM6", symbol="MNQ", con_id=770561201).to_payload()

    assert submit_intent_ownership_digest(first) == first["digest"]
    assert submit_intent_ownership_digest(second) == second["digest"]
    assert first["digest"] != second["digest"]


def test_reserved_lifecycle_id_does_not_imply_open_position() -> None:
    payload = base_record().to_payload()

    assert payload["lifecycle_id"].startswith("reserved_submit_")
    assert payload["lifecycle_id_reserved_only"] is True
    assert payload["lifecycle_position_open"] is False
    assert payload["state"] == SubmitIntentOwnershipState.PRE_SUBMIT_INTENT_DURABLE.value


def test_unresolved_intent_loading_uses_latest_state_per_ownership_id(tmp_path: Path) -> None:
    jsonl = tmp_path / "ownership.jsonl"
    latest = tmp_path / "latest.json"
    first = append_submit_intent_ownership_record(base_record(), jsonl_path=jsonl, latest_path=latest).record
    resolved = dict(first)
    resolved["state"] = "NO_BROKER_EFFECT_CONFIRMED"
    append_submit_intent_ownership_record(resolved, jsonl_path=jsonl, latest_path=latest)
    append_submit_intent_ownership_record(
        base_record(lane_id="other_lane", strategy_id="other_strategy"),
        jsonl_path=jsonl,
        latest_path=latest,
    )

    unresolved = load_unresolved_submit_intent_ownership_records(jsonl)
    latest_payload = json.loads(latest.read_text(encoding="utf-8"))

    assert len(unresolved) == 1
    assert unresolved[0]["lane_id"] == "other_lane"
    assert latest_payload["unresolved_count"] == 1
    assert latest_payload["latest_ownership_record_count"] == 2
    assert [row["lane_id"] for row in latest_payload["unresolved_submit_intent_ownership"]] == ["other_lane"]


def test_module_has_no_broker_imports_or_mutation_symbols() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_submit_intent_ownership.py").read_text(encoding="utf-8")

    assert "import ibkr" not in source.lower()
    assert "from ..brokers" not in source
    assert "from mgc_v05l.brokers" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
    assert "submit_limit_order" not in source
