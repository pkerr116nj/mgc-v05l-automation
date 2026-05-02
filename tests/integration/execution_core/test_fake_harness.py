from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter
from mgc_v05l.execution_core.harness import HarnessConfig, run_fake_paper_proof
from mgc_v05l.execution_core.ledger import JsonlLedger
from mgc_v05l.execution_core.models import TerminalClassification


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def config(tmp_path, **overrides: object) -> HarnessConfig:  # type: ignore[no-untyped-def]
    kwargs = {"output_root": tmp_path / "track_b_execution_core"}
    kwargs.update(overrides)
    return HarnessConfig(**kwargs)


def report(result) -> dict[str, object]:  # type: ignore[no-untyped-def]
    return json.loads(result.proof_report_json.read_text(encoding="utf-8"))


def event_types(result) -> list[str]:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(result.ledger_path)
    return [event.event_type for event in ledger.read_events(run_id=result.run_id)]


def test_fake_harness_passes_full_open_close_spine(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="pass")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-pass", now=aware_now())
    payload = report(result)
    types = event_types(result)

    assert result.classification == TerminalClassification.PASSED
    assert payload["classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert payload["final_reconciliation"]["status"] == "CLEAN"
    assert payload["open_fill"]["execution_id"] == "EXEC-1"
    assert payload["close_fill"]["execution_id"] == "EXEC-2"
    assert adapter.submit_count == 2
    assert types[:3] == ["run_started", "config_loaded", "config_validated"]
    for required in (
        "broker_connected",
        "account_validated",
        "contract_qualified",
        "quote_observed",
        "pricing_decision_created",
        "broker_position_observed",
        "broker_open_orders_observed",
        "reconciliation_created",
        "signal_event_created",
        "order_intent_created",
        "gate_decision_created",
        "submit_attempt_created",
        "broker_order_observed",
        "fill_event_created",
        "proof_report_written",
        "run_passed",
    ):
        assert required in types


def test_missing_account_blocks_before_submit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(account_id="DU1234567")

    result = run_fake_paper_proof(
        config=config(tmp_path, account_id=""),
        adapter=adapter,
        run_id="run-missing-account",
        now=aware_now(),
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert adapter.submit_count == 0
    assert "submit_attempt_created" not in event_types(result)
    assert "account_id is required" in str(report(result)["failure_or_ambiguity"])


def test_stale_quote_blocks_before_submit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(quote_observed_at=aware_now() - timedelta(seconds=31))

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-stale-quote", now=aware_now())

    assert result.classification == TerminalClassification.BLOCKED
    assert adapter.submit_count == 0
    assert "submit_attempt_created" not in event_types(result)
    assert "stale" in str(report(result)["failure_or_ambiguity"])


def test_risk_gate_rejection_blocks_before_submit(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter()

    result = run_fake_paper_proof(
        config=config(tmp_path, order_extra_fields={"parent_id": "forbidden"}),
        adapter=adapter,
        run_id="run-risk-gate",
        now=aware_now(),
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert adapter.submit_count == 0
    assert "submit_attempt_created" not in event_types(result)
    assert "bracket/OCO/parent/child/algo" in str(report(result)["failure_or_ambiguity"])


def test_open_order_rests_cancel_confirmed_blocks_without_replacement(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="open_rests_cancel_clean")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-rest-cancel", now=aware_now())
    payload = report(result)
    types = event_types(result)

    assert result.classification == TerminalClassification.BLOCKED
    assert adapter.submit_count == 1
    assert adapter.cancel_count == 1
    assert "cancel_attempt_created" in types
    assert "cancel_status_observed" in types
    assert payload["cancel_attempts"][0]["observed_cancel_status"] == "Cancelled"


def test_submit_sent_but_missing_order_truth_is_ambiguous(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="submit_missing_order_truth")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-missing-truth", now=aware_now())
    payload = report(result)

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert adapter.submit_count == 1
    assert "orderStatus" in payload["missing_callbacks"]
    assert "broker_order_observed" not in event_types(result)


def test_open_fill_post_open_mismatch_is_ambiguous_and_skips_close(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="post_open_reconciliation_mismatch")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-post-open-mismatch", now=aware_now())
    types = event_types(result)

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert adapter.submit_count == 1
    assert types.count("submit_attempt_created") == 1
    assert "POST_OPEN reconciliation" in str(report(result)["failure_or_ambiguity"])


def test_close_submit_ambiguous_does_not_send_second_close(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="close_submit_ambiguous")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-close-ambiguous", now=aware_now())

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert adapter.submit_count == 2
    assert event_types(result).count("submit_attempt_created") == 2
    assert "Do not send a second close order" in str(report(result)["required_manual_action"])


def test_final_reconciliation_mismatch_is_ambiguous(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = FakePaperAdapter(scenario="final_reconciliation_mismatch")

    result = run_fake_paper_proof(config=config(tmp_path), adapter=adapter, run_id="run-final-mismatch", now=aware_now())
    payload = report(result)

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert adapter.submit_count == 2
    assert payload["post_close_reconciliation"]["status"] == "CLEAN"
    assert payload["final_reconciliation"]["status"] == "AMBIGUOUS"
    assert "FINAL reconciliation" in str(payload["failure_or_ambiguity"])
