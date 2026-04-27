from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.app import tradestation_account_truth as cli
from mgc_v05l.app.tradestation_account_truth import main


def _write_fixture(tmp_path: Path) -> Path:
    payload = {
        "sim": {
            "accounts": [
                {"AccountID": "EQ-1", "AccountType": "Margin", "Alias": "Equities Margin"},
                {"AccountID": "FU-1", "AccountTypeDescription": "Futures", "Alias": "Index Futures"},
            ],
            "balances": {
                "EQ-1": [{"NetLiquidation": "150000", "CashBalance": "50000", "BuyingPower": "200000"}],
                "FU-1": [{"NetLiquidation": "80000", "CashBalance": "30000", "BuyingPower": "90000"}],
            },
            "positions": {
                "EQ-1": [{"Symbol": "AAPL", "Quantity": "100", "AveragePrice": "180", "AssetType": "STK"}],
                "FU-1": [{"Symbol": "NQ", "Quantity": "2", "AveragePrice": "18300", "AssetType": "FUT"}],
            },
            "orders": {"EQ-1": [], "FU-1": []},
        }
    }
    path = tmp_path / "fixture.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_auth_bootstrap_generates_read_only_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "client-123")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret-123")
    monkeypatch.setenv("TRADESTATION_REDIRECT_URI", "http://127.0.0.1:8766/callback")
    output_dir = tmp_path / "auth"

    result = main(
        [
            "--environment",
            "sim",
            "--auth-bootstrap",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result == 0
    summary = json.loads((output_dir / "sim" / "auth_bootstrap" / "reports" / "tradestation_account_truth_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "ready_for_manual_auth_bootstrap"
    assert "READ-ONLY MODE ONLY" in summary["operator_safety_message"]
    assert summary["auth"]["authorize_url"].startswith("https://signin.tradestation.com/authorize?")


def test_auth_bootstrap_rejects_order_capable_scopes(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Order-capable scopes"):
        main(
            [
                "--environment",
                "sim",
                "--auth-bootstrap",
                "--scopes",
                "openid profile offline_access ReadAccount Trade",
                "--output-dir",
                str(tmp_path / "auth"),
            ]
        )


def test_discover_accounts_and_select_accounts_from_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = _write_fixture(tmp_path)
    selected_path = tmp_path / "selected_accounts.json"
    monkeypatch.setenv("TRADESTATION_SELECTED_ACCOUNTS_PATH", str(selected_path))

    discover_dir = tmp_path / "discover"
    result = main(
        [
            "--environment",
            "sim",
            "--discover-accounts",
            "--fixture-json",
            str(fixture),
            "--output-dir",
            str(discover_dir),
        ]
    )
    assert result == 0
    summary = json.loads((discover_dir / "sim" / "discover_accounts" / "reports" / "tradestation_account_truth_summary.json").read_text(encoding="utf-8"))
    assert summary["account_counts"]["margin_equities_options"] == 1
    assert summary["account_counts"]["futures"] == 1

    select_dir = tmp_path / "select"
    result = main(
        [
            "--environment",
            "sim",
            "--fixture-json",
            str(fixture),
            "--select-margin-account",
            "EQ-1",
            "--select-futures-account",
            "FU-1",
            "--output-dir",
            str(select_dir),
        ]
    )
    assert result == 0
    persisted = json.loads(selected_path.read_text(encoding="utf-8"))
    assert persisted["sim"]["selected_margin_account_id"] == "EQ-1"
    assert persisted["sim"]["selected_futures_account_id"] == "FU-1"


def test_read_only_truth_generates_normalized_truth_from_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = _write_fixture(tmp_path)
    selected_path = tmp_path / "selected_accounts.json"
    selected_path.write_text(
        json.dumps({"sim": {"selected_margin_account_id": "EQ-1", "selected_futures_account_id": "FU-1"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADESTATION_SELECTED_ACCOUNTS_PATH", str(selected_path))
    output_dir = tmp_path / "truth"

    result = main(
        [
            "--environment",
            "sim",
            "--read-only-truth",
            "--fixture-json",
            str(fixture),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result == 0
    summary = json.loads((output_dir / "sim" / "read_only_truth" / "reports" / "tradestation_account_truth_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "ready"
    assert summary["combined_summary"]["application_level_aggregation"] is True
    assert summary["combined_summary"]["total_net_liquidation"] == "230000"
    assert summary["combined_summary"]["positions_by_symbol"]["NQ"]["asset_class"] == "FUTURE"


def test_real_connection_is_blocked_without_fixture(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="blocked unless --allow-real-api|not approved or implemented"):
        main(
            [
                "--environment",
                "sim",
                "--discover-accounts",
                "--output-dir",
                str(tmp_path / "blocked"),
            ]
        )


def test_missing_credential_config_fails_closed_for_real_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRADESTATION_CLIENT_ID", raising=False)
    monkeypatch.delenv("TRADESTATION_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("TRADESTATION_REDIRECT_URI", raising=False)
    with pytest.raises(RuntimeError, match="credential readiness failed"):
        main(
            [
                "--environment",
                "sim",
                "--discover-accounts",
                "--allow-real-api",
                "--output-dir",
                str(tmp_path / "real"),
            ]
        )
    audit = json.loads(
        (tmp_path / "real" / "sim" / "discover_accounts" / "reports" / "tradestation_account_truth_audit.json").read_text(
            encoding="utf-8"
        )
    )
    assert audit["status"] == "failed"
    assert audit["allow_real_api"] is True


def test_endpoint_error_is_reported_cleanly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "client-123")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret-123")
    monkeypatch.setenv("TRADESTATION_REDIRECT_URI", "http://127.0.0.1:8766/callback")
    monkeypatch.setenv("TRADESTATION_SIM_TOKEN_STORE_PATH", str(tmp_path / "sim_tokens.json"))
    monkeypatch.setenv("TRADESTATION_SELECTED_ACCOUNTS_PATH", str(tmp_path / "selected_accounts.json"))

    class _BadService:
        def discover_accounts(self, environment):
            raise cli.TradeStationEndpointVerificationError("unexpected shape from accounts endpoint")

    monkeypatch.setattr(cli, "_build_real_service", lambda args: _BadService())

    with pytest.raises(RuntimeError, match="unexpected shape from accounts endpoint"):
        main(
            [
                "--environment",
                "sim",
                "--discover-accounts",
                "--allow-real-api",
                "--output-dir",
                str(tmp_path / "bad_endpoint"),
            ]
        )


def test_token_path_separation_sim_vs_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "client-123")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret-123")
    monkeypatch.setenv("TRADESTATION_REDIRECT_URI", "http://127.0.0.1:8766/callback")
    monkeypatch.setenv("TRADESTATION_SIM_TOKEN_STORE_PATH", str(tmp_path / "sim_tokens.json"))
    monkeypatch.setenv("TRADESTATION_LIVE_TOKEN_STORE_PATH", str(tmp_path / "live_tokens.json"))

    main(
        [
            "--environment",
            "sim",
            "--auth-bootstrap",
            "--output-dir",
            str(tmp_path / "auth"),
        ]
    )
    sim_summary = json.loads(
        (tmp_path / "auth" / "sim" / "auth_bootstrap" / "reports" / "tradestation_account_truth_summary.json").read_text(
            encoding="utf-8"
        )
    )

    main(
        [
            "--environment",
            "live",
            "--auth-bootstrap",
            "--output-dir",
            str(tmp_path / "auth"),
        ]
    )
    live_summary = json.loads(
        (tmp_path / "auth" / "live" / "auth_bootstrap" / "reports" / "tradestation_account_truth_summary.json").read_text(
            encoding="utf-8"
        )
    )

    assert sim_summary["auth"]["sim_token_store_path"].endswith("sim_tokens.json")
    assert live_summary["auth"]["live_token_store_path"].endswith("live_tokens.json")
