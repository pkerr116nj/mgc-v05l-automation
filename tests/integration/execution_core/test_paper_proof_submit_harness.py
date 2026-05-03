from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter
from mgc_v05l.execution_core.harness import run_fake_paper_proof
from mgc_v05l.execution_core.models import TerminalClassification
from mgc_v05l.execution_core.paper_proof import PaperProofConfig, run_paper_proof
from mgc_v05l.execution_core.preflight import PreflightClassification, PreflightResult


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def config(tmp_path: Path, **overrides: object) -> PaperProofConfig:
    kwargs = {
        "output_root": tmp_path / "paper_proof",
        "account_id": "DUM882026",
        "client_id": 17077,
        "submit_enabled": True,
        "confirm_paper_submit": True,
        "allow_delayed_data_for_paper_proof": True,
        "proof_timing_status": "ACTIVE_SESSION",
    }
    kwargs.update(overrides)
    return PaperProofConfig(**kwargs)


def preflight(tmp_path: Path) -> PreflightResult:
    return PreflightResult(
        run_id="preflight-integration",
        classification=PreflightClassification.READY_READ_ONLY,
        report_json=tmp_path / "preflight.json",
        report_md=tmp_path / "preflight.md",
        report={
            "classification": "READY_READ_ONLY",
            "config": {"account_id": "DUM882026"},
            "contract_key": "MGC-202606",
            "checks": [
                {"name": "managed_account_exact_match", "passed": True},
                {"name": "contract_qualified", "passed": True},
            ],
            "position": {"signed_quantity": 0},
            "open_orders": [],
            "quote_observed": True,
            "market_data_provider": "IBKR",
            "market_data_mode": "DELAYED",
            "market_data_role": "DIAGNOSTIC",
            "delayed_data_warning_seen": True,
            "paper_route_readiness": True,
            "production_live_money_readiness": False,
        },
    )


def preflight_runner(result: PreflightResult):
    def run(config, run_id):  # type: ignore[no-untyped-def]
        return result

    return run


def proof_runner_for(adapter: FakePaperAdapter):
    def run(config, run_id):  # type: ignore[no-untyped-def]
        adapter.account_id = config.account_id
        adapter.contract_key = config.contract_key
        return run_fake_paper_proof(config=config, adapter=adapter, run_id=run_id, now=aware_now())

    return run


def test_final_clean_fake_open_close_flow_passes_with_complete_id_chain(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(preflight(tmp_path)),
        proof_runner=proof_runner_for(FakePaperAdapter(account_id="DUM882026", scenario="pass")),
        run_id="paper-proof-pass",
    )
    payload = result.report["proof_payload"]
    state_machine = payload["state_machine"]

    assert result.classification == TerminalClassification.PASSED
    assert state_machine.index("order_intent_created") < state_machine.index("submit_attempt_created")
    assert state_machine.index("submit_attempt_created") < state_machine.index("broker_order_observed")
    assert payload["open_submit_attempt"]["submit_attempt_id"] == payload["open_broker_order"]["submit_attempt_id"]
    assert payload["open_broker_order"]["broker_order_id"] == "FAKE-ORDER-0001"
    assert payload["open_broker_order"]["perm_id"] == "FAKE-PERM-0001"
    assert payload["open_fill"]["execution_id"] == "FAKE-EXEC-0001"
    assert payload["close_broker_order"]["broker_order_id"] == "FAKE-ORDER-0002"
    assert payload["close_fill"]["execution_id"] == "FAKE-EXEC-0002"
    assert payload["final_reconciliation"]["status"] == "CLEAN"
    assert result.report["production_live_money_readiness"] is False


def test_open_rests_then_clean_cancel_blocks_without_replacement_order(tmp_path: Path) -> None:
    adapter = FakePaperAdapter(account_id="DUM882026", scenario="open_rests_cancel_clean")

    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(preflight(tmp_path)),
        proof_runner=proof_runner_for(adapter),
        run_id="paper-proof-open-rests",
    )
    payload = result.report["proof_payload"]

    assert result.classification == TerminalClassification.BLOCKED
    assert adapter.submit_count == 1
    assert adapter.cancel_count == 1
    assert payload["cancel_attempts"][0]["observed_cancel_status"] == "Cancelled"
    assert payload["close_submit_attempt"] is None


def test_close_ambiguous_is_manual_review_and_no_second_close(tmp_path: Path) -> None:
    adapter = FakePaperAdapter(account_id="DUM882026", scenario="close_submit_ambiguous")

    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(preflight(tmp_path)),
        proof_runner=proof_runner_for(adapter),
        run_id="paper-proof-close-ambiguous",
    )
    payload = result.report["proof_payload"]

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert adapter.submit_count == 2
    assert payload["close_submit_attempt"]["submit_attempt_id"]
    assert payload["close_broker_order"] is None
    assert "ambiguous" in str(payload["failure_or_ambiguity"]).lower()
