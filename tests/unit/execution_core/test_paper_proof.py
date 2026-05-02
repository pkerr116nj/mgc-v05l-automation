from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.models import TerminalClassification
from mgc_v05l.execution_core.paper_proof import PaperProofConfig, run_paper_proof
from mgc_v05l.execution_core.preflight import PreflightClassification, PreflightResult, ReadOnlyPreflightConfig


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
    }
    kwargs.update(overrides)
    return PaperProofConfig(**kwargs)


def ready_preflight(tmp_path: Path, **overrides: object) -> PreflightResult:
    report = {
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
    }
    report.update(overrides)
    return PreflightResult(
        run_id="preflight-1",
        classification=PreflightClassification(str(report["classification"])),
        report_json=tmp_path / "preflight_report.json",
        report_md=tmp_path / "preflight_report.md",
        report=report,
    )


def preflight_runner(result: PreflightResult):
    def run(config: ReadOnlyPreflightConfig, run_id: str) -> PreflightResult:  # noqa: ARG001
        return result

    return run


def passing_proof_runner(tmp_path: Path):
    def run(config, run_id):  # type: ignore[no-untyped-def]
        from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter
        from mgc_v05l.execution_core.harness import run_fake_paper_proof

        return run_fake_paper_proof(
            config=config,
            adapter=FakePaperAdapter(account_id=config.account_id, contract_key=config.contract_key, scenario="pass"),
            run_id=run_id,
            now=aware_now(),
        )

    return run


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"submit_enabled": False}, "submit_enabled=True"),
        ({"confirm_paper_submit": False}, "confirm_paper_submit"),
        ({"mode": "LIVE"}, "mode must be PAPER"),
        ({"host": "localhost"}, "host must be 127.0.0.1"),
        ({"port": 7496}, "port must be 7497"),
        ({"quantity": 2}, "quantity must be exactly 1"),
        ({"order_type": "MKT"}, "order_type must be LMT"),
        ({"time_in_force": "GTC"}, "time_in_force must be DAY"),
    ],
)
def test_paper_proof_rejects_unsafe_config(tmp_path: Path, overrides: dict[str, object], reason: str) -> None:
    result = run_paper_proof(
        config=config(tmp_path, **overrides),
        preflight_runner=preflight_runner(ready_preflight(tmp_path)),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-config-block",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert reason in str(result.report["failure_or_ambiguity"])


def test_blocks_when_preflight_not_ready(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, classification="BLOCKED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-preflight-block",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "preflight was not ready" in str(result.report["failure_or_ambiguity"])


@pytest.mark.parametrize(
    ("report_overrides", "reason"),
    [
        ({"config": {"account_id": "OTHER"}}, "account_id"),
        ({"checks": [{"name": "managed_account_exact_match", "passed": False}]}, "paper account"),
        ({"contract_key": "MNQ-202606"}, "contract_key"),
        ({"checks": [{"name": "managed_account_exact_match", "passed": True}, {"name": "contract_qualified", "passed": False}]}, "contract"),
        ({"position": {"signed_quantity": 1}}, "not flat"),
        ({"open_orders": [{"broker_order_id": "1001"}]}, "open orders"),
        ({"account_open_orders": [{"broker_order_id": "2001"}]}, "account-wide"),
    ],
)
def test_blocks_on_preflight_account_contract_position_or_order_mismatch(
    tmp_path: Path,
    report_overrides: dict[str, object],
    reason: str,
) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, **report_overrides)),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-preflight-mismatch",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert reason in str(result.report["failure_or_ambiguity"])


def test_delayed_market_data_requires_explicit_paper_approval(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, allow_delayed_data_for_paper_proof=False),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-delayed-not-approved",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "delayed market data" in str(result.report["failure_or_ambiguity"])


def test_missing_or_unknown_quote_blocks_pricing_dependent_proof(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, quote_observed=False, market_data_mode="UNKNOWN")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-missing-quote",
    )

    assert result.classification == TerminalClassification.BLOCKED
    assert "observed quote" in str(result.report["failure_or_ambiguity"])


def test_delayed_data_can_pass_paper_proof_but_not_live_money_readiness(tmp_path: Path) -> None:
    result = run_paper_proof(
        config=config(tmp_path, allow_delayed_data_for_paper_proof=True),
        preflight_runner=preflight_runner(ready_preflight(tmp_path, market_data_mode="DELAYED")),
        proof_runner=passing_proof_runner(tmp_path),
        run_id="run-delayed-pass",
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.classification == TerminalClassification.PASSED
    assert payload["market_data_mode"] == "DELAYED"
    assert payload["paper_route_readiness"] is True
    assert payload["production_live_money_readiness"] is False


def test_missing_broker_correlation_downgrades_pass_to_ambiguous(tmp_path: Path) -> None:
    def proof_runner(config, run_id):  # type: ignore[no-untyped-def]
        from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter, FakeSubmitResult
        from mgc_v05l.execution_core.harness import run_fake_paper_proof

        class MissingBrokerOrderAdapter(FakePaperAdapter):
            def submit_order(self, **kwargs):  # type: ignore[no-untyped-def]
                result = super().submit_order(**kwargs)
                if result.fill_event is not None:
                    return FakeSubmitResult(
                        broker_order=None,
                        fill_event=result.fill_event,
                        broker_position=result.broker_position,
                        open_orders=result.open_orders,
                        missing_callbacks=result.missing_callbacks,
                        ambiguous=result.ambiguous,
                        failure_reason=result.failure_reason,
                    )
                return result

        return run_fake_paper_proof(
            config=config,
            adapter=MissingBrokerOrderAdapter(account_id=config.account_id, contract_key=config.contract_key, scenario="pass"),
            run_id=run_id,
            now=aware_now(),
        )

    result = run_paper_proof(
        config=config(tmp_path),
        preflight_runner=preflight_runner(ready_preflight(tmp_path)),
        proof_runner=proof_runner,
        run_id="run-missing-correlation",
    )

    assert result.classification == TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
