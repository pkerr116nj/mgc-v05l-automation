"""Manual-first read-only TradeStation account-truth CLI scaffold."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import load_tradestation_config
from ..execution.tradestation_auth import (
    TradeStationEnvironment,
    TradeStationOAuthClient,
    TradeStationOAuthConfig,
    TradeStationSelectedAccounts,
    TradeStationSelectedAccountsStore,
)
from ..execution.tradestation_broker_family import (
    ACCOUNT_TYPE_FUTURES,
    ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS,
    TradeStationBrokerFamilyService,
    classify_tradestation_account,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "tradestation_account_truth"
DEFAULT_SCOPES = "openid profile offline_access ReadAccount"
SAFETY_MESSAGE = (
    "READ-ONLY MODE ONLY. This command does not submit, cancel, replace, flatten, or preview orders. "
    "Real API connection remains blocked until explicitly approved."
)


class TradeStationReadOnlyConnectionBlocked(RuntimeError):
    """Raised when a real API connection is attempted before explicit approval."""


class TradeStationAccountTruthCliError(RuntimeError):
    """Raised when CLI inputs or fixture data are invalid."""


class _FixtureTradeStationClient:
    def __init__(self, *, environment: TradeStationEnvironment, payload: dict[str, Any]) -> None:
        self.environment = environment
        self._payload = dict(payload)

    def list_accounts(self) -> Any:
        return list(_coerce_rows(self._payload.get("accounts")))

    def get_balances(self, account_id: str) -> Any:
        return list((self._payload.get("balances") or {}).get(account_id, []))

    def get_positions(self, account_id: str) -> Any:
        return list((self._payload.get("positions") or {}).get(account_id, []))

    def get_open_orders(self, account_id: str) -> Any:
        return list((self._payload.get("orders") or {}).get(account_id, []))


def _parse_environment(value: str) -> TradeStationEnvironment:
    return TradeStationEnvironment(str(value).strip().lower())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tradestation-account-truth")
    parser.add_argument("--environment", required=True, type=_parse_environment, choices=list(TradeStationEnvironment))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--fixture-json", type=Path, default=None, help="Local fixture payload for safe dry-run testing.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting an existing output directory.")
    parser.add_argument("--state", default="mgc-v05l-tradestation-readonly", help="Opaque OAuth state for auth bootstrap.")
    parser.add_argument("--scopes", default=DEFAULT_SCOPES, help="Space-delimited OAuth scopes for auth bootstrap.")
    parser.add_argument("--auth-bootstrap", action="store_true", help="Emit a local authorize URL and token-path instructions.")
    parser.add_argument("--discover-accounts", action="store_true", help="Discover accessible accounts from local fixture or future real API.")
    parser.add_argument("--select-margin-account", default=None, help="Persist the selected margin equities/options account id.")
    parser.add_argument("--select-futures-account", default=None, help="Persist the selected futures account id.")
    parser.add_argument("--read-only-truth", action="store_true", help="Build normalized per-account and combined truth artifacts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mode = _resolve_mode(args)
    output_dir = Path(args.output_dir)
    if mode != "select_accounts":
        output_dir = output_dir / args.environment.value / mode
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))

    if mode == "auth_bootstrap":
        payload = _run_auth_bootstrap(args)
    elif mode == "discover_accounts":
        payload = _run_discover_accounts(args)
    elif mode == "select_accounts":
        payload = _run_select_accounts(args)
    elif mode == "read_only_truth":
        payload = _run_read_only_truth(args)
    else:
        raise TradeStationAccountTruthCliError(f"Unsupported mode: {mode}")

    payload.setdefault("operator_safety_message", SAFETY_MESSAGE)
    payload.setdefault("mode", mode)
    payload.setdefault("environment", args.environment.value)
    payload.setdefault("generated_at", datetime.now(timezone.utc).isoformat())

    _write_output_artifacts(output_dir=output_dir, payload=payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _resolve_mode(args: argparse.Namespace) -> str:
    mode_flags = [
        bool(args.auth_bootstrap),
        bool(args.discover_accounts),
        bool(args.read_only_truth),
        bool(args.select_margin_account or args.select_futures_account),
    ]
    if sum(1 for flag in mode_flags if flag) != 1:
        raise TradeStationAccountTruthCliError(
            "Exactly one mode is required: --auth-bootstrap, --discover-accounts, --read-only-truth, or account-selection flags."
        )
    if args.auth_bootstrap:
        return "auth_bootstrap"
    if args.discover_accounts:
        return "discover_accounts"
    if args.read_only_truth:
        return "read_only_truth"
    return "select_accounts"


def _run_auth_bootstrap(args: argparse.Namespace) -> dict[str, Any]:
    config = load_tradestation_config()
    oauth_client = TradeStationOAuthClient(
        TradeStationOAuthConfig(
            client_id=config.client_id,
            client_secret=config.client_secret,
            redirect_uri=config.redirect_uri,
        )
    )
    scopes = [item for item in str(args.scopes).split(" ") if item.strip()]
    authorize_url = None
    if config.client_id and config.redirect_uri:
        authorize_url = oauth_client.build_authorize_url(state=str(args.state), scopes=scopes)
    return {
        "status": "ready_for_manual_auth_bootstrap",
        "operator_safety_message": SAFETY_MESSAGE,
        "auth": {
            "authorize_url": authorize_url,
            "client_id_present": bool(config.client_id),
            "client_secret_present": bool(config.client_secret),
            "redirect_uri": config.redirect_uri,
            "requested_scopes": scopes,
            "sim_token_store_path": str(config.sim_token_store_path),
            "live_token_store_path": str(config.live_token_store_path),
            "selected_accounts_path": str(config.selected_accounts_path),
            "exchange_code_supported_now": False,
            "detail": "This scaffold only builds the local authorize URL and token-path instructions. No network exchange occurs yet.",
        },
    }


def _run_discover_accounts(args: argparse.Namespace) -> dict[str, Any]:
    service = _build_fixture_service(args)
    discovered = service.discover_accounts(args.environment)
    payload = {
        "status": "ready" if discovered else "warning",
        "accounts": [asdict(account) for account in discovered],
        "account_counts": {
            "margin_equities_options": sum(1 for account in discovered if account.account_type == ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS),
            "futures": sum(1 for account in discovered if account.account_type == ACCOUNT_TYPE_FUTURES),
            "unknown": sum(1 for account in discovered if account.account_type not in {ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS, ACCOUNT_TYPE_FUTURES}),
        },
        "selection_required": True,
    }
    return payload


def _run_select_accounts(args: argparse.Namespace) -> dict[str, Any]:
    service = _build_fixture_service(args)
    discovered = service.discover_accounts(args.environment)
    if not discovered:
        raise TradeStationAccountTruthCliError("No accounts were discovered for selection.")
    margin_id = str(args.select_margin_account or "").strip() or None
    futures_id = str(args.select_futures_account or "").strip() or None
    if margin_id is not None:
        _validate_selected_account(discovered, margin_id, ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS)
    if futures_id is not None:
        _validate_selected_account(discovered, futures_id, ACCOUNT_TYPE_FUTURES)
    config = load_tradestation_config()
    store = TradeStationSelectedAccountsStore(config.selected_accounts_path)
    current = store.load_environment(args.environment)
    selection = TradeStationSelectedAccounts(
        selected_margin_account_id=margin_id or current.selected_margin_account_id,
        selected_futures_account_id=futures_id or current.selected_futures_account_id,
    )
    store.save_environment(args.environment, selection)
    return {
        "status": "ready",
        "selected_accounts": selection.to_payload(),
        "persisted_path": str(config.selected_accounts_path),
        "discovered_accounts": [asdict(account) for account in discovered],
    }


def _run_read_only_truth(args: argparse.Namespace) -> dict[str, Any]:
    service = _build_fixture_service(args)
    snapshot = service.snapshot_state(args.environment)
    snapshot["status"] = "ready"
    return snapshot


def _build_fixture_service(args: argparse.Namespace) -> TradeStationBrokerFamilyService:
    fixture_payload = _load_fixture_payload(args.fixture_json, environment=args.environment)
    config = load_tradestation_config()
    store = TradeStationSelectedAccountsStore(config.selected_accounts_path)
    sim_payload = fixture_payload if args.environment == TradeStationEnvironment.SIM else {}
    live_payload = fixture_payload if args.environment == TradeStationEnvironment.LIVE else {}
    return TradeStationBrokerFamilyService(
        sim_client=_FixtureTradeStationClient(environment=TradeStationEnvironment.SIM, payload=sim_payload),
        live_client=_FixtureTradeStationClient(environment=TradeStationEnvironment.LIVE, payload=live_payload),
        selected_accounts_store=store,
    )


def _load_fixture_payload(path: Path | None, *, environment: TradeStationEnvironment) -> dict[str, Any]:
    if path is None:
        raise TradeStationReadOnlyConnectionBlocked(
            "Real TradeStation API connection is not approved or implemented in this Stage 1B scaffold. "
            "Use --fixture-json for safe local validation only."
        )
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TradeStationAccountTruthCliError("Fixture payload must be a JSON object.")
    env_payload = payload.get(environment.value)
    if isinstance(env_payload, dict):
        return env_payload
    return payload


def _validate_selected_account(discovered: list[Any], account_id: str, expected_type: str) -> None:
    matches = [account for account in discovered if account.account_id == account_id]
    if not matches:
        raise TradeStationAccountTruthCliError(f"Selected account {account_id} was not discovered.")
    if matches[0].account_type != expected_type:
        raise TradeStationAccountTruthCliError(
            f"Selected account {account_id} is classified as {matches[0].account_type}, not {expected_type}."
        )


def _ensure_output_dir(path: Path, *, overwrite: bool) -> None:
    if path.exists() and any(path.iterdir()) and not overwrite:
        raise TradeStationAccountTruthCliError(
            f"Output directory {path} already exists and is not empty. Use --overwrite to replace artifacts."
        )
    path.mkdir(parents=True, exist_ok=True)


def _write_output_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> None:
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary_json = reports_dir / "tradestation_account_truth_summary.json"
    summary_md = reports_dir / "tradestation_account_truth_summary.md"
    summary_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_md.write_text(_render_markdown_summary(payload), encoding="utf-8")


def _render_markdown_summary(payload: dict[str, Any]) -> str:
    lines = [
        "# TradeStation Account Truth",
        "",
        f"- Mode: `{payload.get('mode')}`",
        f"- Environment: `{payload.get('environment')}`",
        f"- Status: `{payload.get('status')}`",
        f"- Safety: {payload.get('operator_safety_message')}",
    ]
    selected_accounts = payload.get("selected_accounts")
    if isinstance(selected_accounts, dict):
        lines.extend(
            [
                "",
                "## Selected Accounts",
                "",
                f"- Margin equities/options: `{selected_accounts.get('selected_margin_account_id')}`",
                f"- Futures: `{selected_accounts.get('selected_futures_account_id')}`",
            ]
        )
    account_counts = payload.get("account_counts")
    if isinstance(account_counts, dict):
        lines.extend(
            [
                "",
                "## Discovered Accounts",
                "",
                f"- Margin equities/options: `{account_counts.get('margin_equities_options')}`",
                f"- Futures: `{account_counts.get('futures')}`",
                f"- Unknown: `{account_counts.get('unknown')}`",
            ]
        )
    combined = payload.get("combined_summary")
    if isinstance(combined, dict):
        lines.extend(
            [
                "",
                "## Combined Summary",
                "",
                f"- Application-level aggregation: `{combined.get('application_level_aggregation')}`",
                f"- Total net liquidation: `{combined.get('total_net_liquidation')}`",
                f"- Total cash balance: `{combined.get('total_cash_balance')}`",
                f"- Total buying power: `{combined.get('total_buying_power')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def _coerce_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [dict(payload)]
    return []


if __name__ == "__main__":
    raise SystemExit(main())
