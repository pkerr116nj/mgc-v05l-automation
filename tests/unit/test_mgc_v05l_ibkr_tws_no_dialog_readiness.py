from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.brokers.ibkr import IbkrQualifiedContract
from mgc_v05l.execution.ibkr_tws_no_dialog_readiness import (
    IbkrTwsNoDialogReadinessConfig,
    _classify_readiness,
    render_ibkr_tws_no_dialog_readiness_markdown,
    run_ibkr_tws_no_dialog_readiness_preflight,
    write_ibkr_tws_no_dialog_readiness_artifacts,
)


def _config(tmp_path: Path) -> IbkrTwsNoDialogReadinessConfig:
    return IbkrTwsNoDialogReadinessConfig(
        repo_root=tmp_path,
        mode="PAPER",
        host="127.0.0.1",
        port=7497,
        client_id=9171,
        read_only=True,
        account_id="DUM882026",
    )


def _runtime() -> SimpleNamespace:
    class _Transport:
        def connect(self) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def req_account_updates(self, *, subscribe: bool, account_id: str) -> None:
            return None

        def server_version(self) -> int:
            return 157

        def tws_connection_time(self) -> str:
            return "20260428 13:55:00 EST"

        def req_positions(self) -> None:
            return None

    return SimpleNamespace(
        session=SimpleNamespace(
            state=SimpleNamespace(
                connected=True,
                connected_at=None,
                account_id="DUM882026",
            )
        ),
        client=SimpleNamespace(
            request_positions=lambda: None,
        ),
        collector=SimpleNamespace(
            positions_ready=SimpleNamespace(clear=lambda: None),
            errors=[],
            latest_error=lambda codes=None: None,
        ),
        transport=_Transport(),
    )


def _exact_contract() -> IbkrQualifiedContract:
    return IbkrQualifiedContract(
        internal_symbol="MGC",
        broker_symbol="MGC",
        local_symbol="MGCM6",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="20260626",
        multiplier="10",
        trading_class="MGC",
        con_id=712565978,
        metadata={"contract_month": "202606"},
    )


def test_classify_readiness_unverified_when_manual_settings_unknown() -> None:
    classification = _classify_readiness(
        selected_account_id="DUM882026",
        contract_report={"ok": True},
        open_orders={"open_order_count": 0},
        matching_mgc_open_orders=[],
        manual_setting_rows=[{"setting": "Bypass Order Precautions for API orders", "status": "must_verify_manually"}],
    )

    assert classification == "TWS_NO_DIALOG_UNVERIFIED"


def test_classify_readiness_blocked_when_working_mgc_exists() -> None:
    classification = _classify_readiness(
        selected_account_id="DUM882026",
        contract_report={"ok": True},
        open_orders={"open_order_count": 1},
        matching_mgc_open_orders=[{"broker_order_id": 1}],
        manual_setting_rows=[{"setting": "Bypass Order Precautions for API orders", "status": "must_verify_manually"}],
    )

    assert classification == "TWS_NO_DIALOG_BLOCKED"


def test_run_preflight_reports_unverified_when_read_only_checks_pass(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._build_runtime",
        lambda **_: _runtime(),
    )
    monkeypatch.setattr("mgc_v05l.execution.ibkr_tws_no_dialog_readiness._start_runtime", lambda runtime: None)
    monkeypatch.setattr("mgc_v05l.execution.ibkr_tws_no_dialog_readiness._wait_for_connection_ready", lambda **_: True)
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._collect_managed_account_context",
        lambda **_: "DUM882026",
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._collect_exact_contract_context",
        lambda **_: (_exact_contract(), {"ok": True, "exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"}}),
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._collect_account_truth",
        lambda **_: ({"buying_power": {"value": "1.0", "currency": "USD"}}, {}),
    )
    monkeypatch.setattr("mgc_v05l.execution.ibkr_tws_no_dialog_readiness._wait_for_event", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._build_positions_snapshot",
        lambda **_: {"position_count": 0, "positions": []},
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_tws_no_dialog_readiness._refresh_open_orders_snapshot",
        lambda **_: {"open_order_count": 0, "open_orders": []},
    )
    monkeypatch.setattr("mgc_v05l.execution.ibkr_tws_no_dialog_readiness._build_callback_timeline", lambda runtime: [])

    artifacts = run_ibkr_tws_no_dialog_readiness_preflight(config=_config(tmp_path))

    assert artifacts.classification == "TWS_NO_DIALOG_UNVERIFIED"
    assert artifacts.report["working_mgc_order_check"]["matching_working_order_count"] == 0
    assert artifacts.report["readiness_summary"]["code_preflight_ready_except_tws_no_dialog_uncertainty"] is True


def test_run_preflight_blocks_without_read_only(tmp_path: Path) -> None:
    config = IbkrTwsNoDialogReadinessConfig(
        repo_root=tmp_path,
        mode="PAPER",
        host="127.0.0.1",
        port=7497,
        client_id=9171,
        read_only=False,
        account_id="DUM882026",
    )

    artifacts = run_ibkr_tws_no_dialog_readiness_preflight(config=config)

    assert artifacts.classification == "TWS_NO_DIALOG_BLOCKED"


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    report = {
        "classification": "TWS_NO_DIALOG_UNVERIFIED",
        "generated_at": "2026-04-28T14:00:00+00:00",
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "account_id": "DUM882026",
        "connection_check": {"client_id": 9171},
        "contract_qualification": {"exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"}, "ok": True},
        "open_orders": {"open_order_count": 0},
        "working_mgc_order_check": {"matching_working_order_count": 0},
        "manual_tws_setting_review": {
            "status": "unverified_from_api_only",
            "conclusion": "Manual setting inspection required.",
            "settings": [{"setting": "Bypass Order Precautions for API orders", "status": "must_verify_manually", "detail": "Must be enabled."}],
        },
        "readiness_summary": {
            "code_preflight_ready_except_tws_no_dialog_uncertainty": True,
            "unattended_submit_allowed_now": False,
            "next_required_step": "Inspect TWS settings.",
        },
        "account_truth": {},
    }
    artifacts = SimpleNamespace(classification="TWS_NO_DIALOG_UNVERIFIED", report=report)

    write_ibkr_tws_no_dialog_readiness_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_tws_no_dialog_readiness_report.json").exists()
    assert (tmp_path / "ibkr_tws_no_dialog_readiness_report.md").exists()
    payload = json.loads((tmp_path / "ibkr_tws_no_dialog_readiness_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_tws_no_dialog_readiness_markdown(payload)
    assert "TWS_NO_DIALOG_UNVERIFIED" in markdown
    assert "Bypass Order Precautions for API orders" in markdown
