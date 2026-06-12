from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_intent_contract import (
    POSITION_INTENT_CONTRACT_READY,
    POSITION_INTENT_INVALID,
    POSITION_INTENT_VALID,
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
    PositionIntentContractAuditConfig,
    build_position_intent_contract_audit,
    position_intent_from_template,
    validate_position_intent,
)


def test_valid_strategy_position_intent_passes() -> None:
    intent = position_intent_from_template(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["asian_drift_v1"])

    result = validate_position_intent(intent)

    assert result["classification"] == POSITION_INTENT_VALID
    assert result["valid"] is True
    assert result["submit_authority"] is False
    assert result["broker_mutation_allowed"] is False
    assert result["lifecycle_authority"] is False
    assert result["live_money_eligible"] is False
    assert result["paper_proof_invoked"] is False


def test_missing_exit_family_is_flagged() -> None:
    intent = position_intent_from_template(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["asian_drift_v1"])
    payload = _payload(intent)
    payload["exit_policy"]["intended_exit_family"] = ""

    result = validate_position_intent(payload)

    assert result["classification"] == POSITION_INTENT_INVALID
    assert "exit_policy.intended_exit_family" in result["missing_metadata"]


def test_missing_pyramiding_policy_is_flagged() -> None:
    intent = position_intent_from_template(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["MNQ_FIRST_BEAR_SNAP_TURN_V1"])
    payload = _payload(intent)
    payload["pyramiding_policy"] = ""

    result = validate_position_intent(payload)

    assert result["classification"] == POSITION_INTENT_INVALID
    assert "pyramiding_policy" in result["missing_metadata"]


def test_live_money_and_submit_authority_are_invalid_for_contract() -> None:
    intent = position_intent_from_template(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["asian_drift_v1"])
    payload = _payload(intent)
    payload["live_money_eligible"] = True
    payload["submit_authority"] = True

    result = validate_position_intent(payload)

    assert result["classification"] == POSITION_INTENT_INVALID
    assert "live_money_eligible" in result["invalid_metadata"]
    assert "read_only_authority_flags" in result["invalid_metadata"]


def test_current_roster_contract_audit_is_read_only(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {
            "enabled_strategy_ids": ["asian_drift_v1", "MNQ_FIRST_BEAR_SNAP_TURN_V1"],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = build_position_intent_contract_audit(
        config=PositionIntentContractAuditConfig(repo_root=tmp_path)
    )

    assert payload["classification"] == POSITION_INTENT_CONTRACT_READY
    assert payload["strategy_count"] == 2
    assert payload["valid_strategy_count"] == 2
    assert payload["submit_authority"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["lifecycle_authority"] is False
    assert payload["integration_targets"]["exit_attribution_framework"]["mode"] == "read_only_context"
    assert payload["integration_targets"]["hold_exit_shadow_engine"]["mode"] == "read_only_context"
    assert payload["strategies"][0]["strategy_hold_exit_policy"]["hold_policy_id"]
    assert payload["strategies"][0]["strategy_hold_exit_policy"]["submit_allowed"] is False


def test_roster_strategy_without_contract_template_reports_gap(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {"enabled_strategy_ids": ["NEW_STRATEGY_WITHOUT_CONTRACT"]},
    )

    payload = build_position_intent_contract_audit(
        config=PositionIntentContractAuditConfig(repo_root=tmp_path)
    )

    assert payload["classification"] != POSITION_INTENT_CONTRACT_READY
    assert payload["gap_strategy_count"] == 1
    assert payload["strategies"][0]["missing_metadata"] == ["strategy_contract_template"]


def test_london_open_active_evidence_position_intents_use_canonical_current_contracts() -> None:
    mnq = position_intent_from_template(
        APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"]
    )
    mes = position_intent_from_template(
        APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"]
    )

    assert mnq.local_symbol == "MNQM6"
    assert mnq.con_id == 770561201
    assert mnq.expiry == "20260618"
    assert mes.local_symbol == "MESM6"
    assert mes.con_id == 770561194
    assert mes.expiry == "20260618"
    assert mnq.conflict_group == "equity_index_mnq_mes_london_open_active_evidence"
    assert mes.conflict_group == "equity_index_mnq_mes_london_open_active_evidence"


def test_london_late_mnq_short_position_intent_uses_canonical_current_contract() -> None:
    intent = position_intent_from_template(
        APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"]
    )

    assert intent.local_symbol == "MNQM6"
    assert intent.con_id == 770561201
    assert intent.expiry == "20260618"
    assert intent.side == "SHORT"
    assert intent.quantity == 1
    assert intent.conflict_group == "equity_index_mnq_mes_london_late_active_evidence"


def test_london_late_mes_short_position_intent_uses_canonical_current_contract() -> None:
    intent = position_intent_from_template(
        APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES["PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1"]
    )

    assert intent.local_symbol == "MESM6"
    assert intent.con_id == 770561194
    assert intent.expiry == "20260618"
    assert intent.side == "SHORT"
    assert intent.quantity == 1
    assert intent.conflict_group == "equity_index_mnq_mes_london_late_active_evidence"


def _payload(intent) -> dict:
    return json.loads(json.dumps(intent, default=lambda value: getattr(value, "__dict__", str(value))))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
