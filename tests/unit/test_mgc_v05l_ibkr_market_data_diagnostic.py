from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_market_data_diagnostic import (
    IbkrMarketDataDiagnosticArtifacts,
    IbkrMarketDataDiagnosticConfig,
    build_market_data_diagnostic_note,
    classify_market_data_diagnostic,
    render_ibkr_market_data_diagnostic_markdown,
    run_ibkr_market_data_diagnostic,
    write_ibkr_market_data_diagnostic_artifacts,
)


def test_classify_market_data_connected_when_live_ticks_are_available() -> None:
    classification = classify_market_data_diagnostic(
        probe_rows=[
            {
                "market_data_type_requested": 1,
                "any_tick_returned": True,
                "response_indication": "data_returned",
            }
        ],
        contract_report={"ok": True},
        connected=True,
    )

    assert classification == "IBKR_MARKET_DATA_CONNECTED"


def test_classify_market_data_delayed_only_when_only_delayed_ticks_are_available() -> None:
    classification = classify_market_data_diagnostic(
        probe_rows=[
            {
                "market_data_type_requested": 1,
                "any_tick_returned": False,
                "response_indication": "delayed_only",
            },
            {
                "market_data_type_requested": 3,
                "any_tick_returned": True,
                "response_indication": "delayed_only",
            },
        ],
        contract_report={"ok": True},
        connected=True,
    )

    assert classification == "IBKR_MARKET_DATA_DELAYED_ONLY"


def test_classify_market_data_delayed_only_when_frozen_request_falls_back_to_delayed_ticks() -> None:
    classification = classify_market_data_diagnostic(
        probe_rows=[
            {
                "market_data_type_requested": 2,
                "any_tick_returned": True,
                "response_indication": "delayed_only",
            }
        ],
        contract_report={"ok": True},
        connected=True,
    )

    assert classification == "IBKR_MARKET_DATA_DELAYED_ONLY"


def test_classify_market_data_permission_blocked_when_no_ticks_and_only_permission_signals() -> None:
    classification = classify_market_data_diagnostic(
        probe_rows=[
            {
                "market_data_type_requested": 1,
                "any_tick_returned": False,
                "response_indication": "delayed_only",
            },
            {
                "market_data_type_requested": 3,
                "any_tick_returned": False,
                "response_indication": "no_permission",
            },
        ],
        contract_report={"ok": True},
        connected=True,
    )

    assert classification == "IBKR_MARKET_DATA_PERMISSION_BLOCKED"


def test_diagnostic_fails_closed_when_read_only_is_false() -> None:
    artifacts = run_ibkr_market_data_diagnostic(
        config=IbkrMarketDataDiagnosticConfig(
            repo_root=Path("."),
            mode="PAPER",
            host="127.0.0.1",
            port=7497,
            client_id=9072,
            read_only=False,
        ),
        sleep_fn=lambda seconds: None,
    )

    assert artifacts.classification == "IBKR_MARKET_DATA_BLOCKED"
    assert artifacts.report["environment_lock_check"]["fail_closed"] is True


def test_write_market_data_diagnostic_artifacts_serializes_outputs(tmp_path: Path) -> None:
    artifacts = IbkrMarketDataDiagnosticArtifacts(
        classification="IBKR_MARKET_DATA_PERMISSION_BLOCKED",
        report={
            "classification": "IBKR_MARKET_DATA_PERMISSION_BLOCKED",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "connection_check": {
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9072,
                "server_version": 157,
            },
            "environment_lock_check": {
                "configured_mode": "PAPER",
                "configured_host": "127.0.0.1",
                "configured_port": 7497,
                "read_only": True,
            },
            "contract_qualification_check": {
                "ok": True,
                "contracts": [
                    {"symbol": "GC", "requested_expiry": "202606", "ok": True},
                    {"symbol": "MGC", "requested_expiry": "202606", "ok": True},
                ],
            },
            "mode_probes": [
                {
                    "probe_label": "live_snapshot",
                    "contract_symbol": "GC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 1,
                    "response_code": 10167,
                    "response_indication": "delayed_only",
                    "any_tick_returned": False,
                }
            ],
            "diagnostic_note": {
                "summary_lines": ["TWS/API path works: yes"],
                "manual_next_checks": ["Check subscriptions."],
            },
        },
        mode_probe_rows=[
            {
                "contract_symbol": "GC",
                "contract_local_symbol": "GCM26",
                "requested_expiry": "202606",
                "probe_label": "live_snapshot",
                "request_type": "snapshot",
                "market_data_type_requested": 1,
                "market_data_type_reported": 3,
                "response_code": 10167,
                "response_message": "Delayed only.",
                "response_indication": "delayed_only",
                "any_bid_tick": False,
                "any_ask_tick": False,
                "any_last_tick": False,
                "any_close_tick": False,
                "any_tick_returned": False,
                "bid_price": None,
                "ask_price": None,
                "last_price": None,
                "close_price": None,
                "status": "no_ticks",
            }
        ],
    )

    write_ibkr_market_data_diagnostic_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_market_data_diagnostic_report.json").exists()
    assert (tmp_path / "ibkr_market_data_diagnostic_report.md").exists()
    assert (tmp_path / "ibkr_market_data_mode_probe.csv").exists()
    payload = json.loads((tmp_path / "ibkr_market_data_diagnostic_report.json").read_text(encoding="utf-8"))
    assert payload["classification"] == "IBKR_MARKET_DATA_PERMISSION_BLOCKED"


def test_diagnostic_note_points_to_entitlement_issue_when_delayed_only() -> None:
    note = build_market_data_diagnostic_note(
        classification="IBKR_MARKET_DATA_DELAYED_ONLY",
        probe_rows=[
            {
                "response_indication": "delayed_only",
                "any_tick_returned": False,
            }
        ],
        contract_report={"ok": True},
        host="127.0.0.1",
        port=7497,
    )

    assert note["tws_api_path_works"] is True
    assert note["issue_appears_entitlement_or_settings"] is True
    assert note["project_can_proceed_with_truth_without_market_data"] is True


def test_markdown_render_mentions_read_only_and_manual_checks() -> None:
    markdown = render_ibkr_market_data_diagnostic_markdown(
        {
            "classification": "IBKR_MARKET_DATA_DELAYED_ONLY",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "connection_check": {
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9072,
                "server_version": 157,
            },
            "environment_lock_check": {
                "configured_mode": "PAPER",
                "configured_host": "127.0.0.1",
                "configured_port": 7497,
                "read_only": True,
            },
            "contract_qualification_check": {
                "ok": True,
                "contracts": [
                    {"symbol": "GC", "requested_expiry": "202606", "ok": True},
                    {"symbol": "MGC", "requested_expiry": "202606", "ok": True},
                ],
            },
            "mode_probes": [
                {
                    "probe_label": "live_snapshot",
                    "contract_symbol": "GC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 1,
                    "response_code": 10167,
                    "response_indication": "delayed_only",
                    "any_tick_returned": False,
                    "market_data_type_reported": None,
                },
                {
                    "probe_label": "live_snapshot",
                    "contract_symbol": "MGC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 1,
                    "response_code": 10168,
                    "response_indication": "no_permission",
                    "any_tick_returned": False,
                    "market_data_type_reported": None,
                },
                {
                    "probe_label": "delayed_snapshot",
                    "contract_symbol": "GC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 3,
                    "market_data_type_reported": 3,
                    "response_code": None,
                    "response_indication": "delayed_only",
                    "any_tick_returned": True,
                },
                {
                    "probe_label": "delayed_snapshot",
                    "contract_symbol": "MGC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 3,
                    "market_data_type_reported": 3,
                    "response_code": None,
                    "response_indication": "delayed_only",
                    "any_tick_returned": True,
                },
                {
                    "probe_label": "frozen_snapshot",
                    "contract_symbol": "GC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 2,
                    "market_data_type_reported": 3,
                    "response_code": None,
                    "response_indication": "delayed_only",
                    "any_tick_returned": True,
                },
                {
                    "probe_label": "delayed_frozen_snapshot",
                    "contract_symbol": "MGC",
                    "requested_expiry": "202606",
                    "market_data_type_requested": 4,
                    "market_data_type_reported": 3,
                    "response_code": None,
                    "response_indication": "delayed_only",
                    "any_tick_returned": True,
                }
            ],
            "diagnostic_note": {
                "summary_lines": ["TWS/API path works: yes"],
                "manual_next_checks": ["Check subscriptions."],
                "issue_appears_entitlement_or_settings": True,
                "project_can_proceed_with_truth_without_market_data": True,
            },
        }
    )

    assert "environment lock: `mode=PAPER, host=127.0.0.1, port=7497, read_only=true`" in markdown
    assert "no orders were placed" in markdown
    assert "ATP/GC and live execution behavior were not touched" in markdown
    assert "live snapshots failed with permission-style responses" in markdown
    assert "delayed snapshots returned usable bid/ask/last/close ticks for both contracts" in markdown
    assert "frozen/delayed-frozen requests fell back to delayed type 3" in markdown
    assert "this points to a market-data entitlement/settings gap rather than a code/request-mode problem" in markdown
    assert "account/position/open-order truth can proceed without live market data" in markdown
    assert "Check subscriptions." in markdown
