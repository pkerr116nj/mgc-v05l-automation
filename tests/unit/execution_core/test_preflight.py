from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core import preflight
from mgc_v05l.execution_core.harness import HarnessConfig
from mgc_v05l.execution_core.ibkr_readonly_transport import detect_contract_allowlist_ambiguities
from mgc_v05l.execution_core.preflight import (
    PreflightClassification,
    ReadOnlyPreflightConfig,
    run_read_only_preflight,
)


DEFAULT_QUOTE = object()


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def config(tmp_path: Path, **overrides: object) -> ReadOnlyPreflightConfig:
    kwargs = {"output_root": tmp_path / "track_b_execution_core" / "preflight"}
    kwargs.update(overrides)
    return ReadOnlyPreflightConfig(**kwargs)


class FakeReadOnlyTransport:
    def __init__(
        self,
        *,
        managed_accounts: Sequence[str] = ("DU1234567",),
        next_valid_id: int | None = 1001,
        position: Mapping[str, Any] | None = None,
        open_orders: Sequence[Mapping[str, Any]] = (),
        quote: Mapping[str, Any] | None | object = DEFAULT_QUOTE,
    ) -> None:
        self._managed_accounts = tuple(managed_accounts)
        self._next_valid_id = next_valid_id
        self._position = position if position is not None else {"signed_quantity": 0}
        self._open_orders = tuple(open_orders)
        self._quote = {"bid": "2345.0", "ask": "2345.1", "last": "2345.05"} if quote is DEFAULT_QUOTE else quote
        self.connected_with: dict[str, Any] | None = None
        self.disconnect_count = 0
        self.place_order_called = False
        self.submit_called = False
        self.next_valid_id_source = "initial_passive"
        self.next_valid_id_requested = False

    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
        self.connected_with = {
            "host": host,
            "port": port,
            "client_id": client_id,
            "readonly": readonly,
        }

    def disconnect(self) -> None:
        self.disconnect_count += 1

    def managed_accounts(self) -> Sequence[str]:
        return self._managed_accounts

    def next_valid_id(self) -> int | None:
        return self._next_valid_id

    def qualify_contract(self, *, contract_key: str, allowlist_entry: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"contract_key": contract_key, **dict(allowlist_entry), "con_id": allowlist_entry.get("con_id") or "12345"}

    def snapshot_position(
        self,
        *,
        run_id: str,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Mapping[str, Any] | None:
        if self._position is None:
            return None
        return {"account_id": account_id, "contract_key": contract_key, **dict(self._position)}

    def snapshot_open_orders(
        self,
        *,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Sequence[Mapping[str, Any]]:
        return tuple({"account_id": account_id, "contract_key": contract_key, **dict(order)} for order in self._open_orders)

    def observe_quote(
        self,
        *,
        run_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Mapping[str, Any] | None:
        if self._quote is None:
            return None
        return {"run_id": run_id, "contract_key": contract_key, **dict(self._quote)}

    def placeOrder(self, *args: object, **kwargs: object) -> None:  # noqa: N802 - IB API spelling.
        self.place_order_called = True
        raise AssertionError("preflight must never call placeOrder")

    def submit_limit_order(self, *args: object, **kwargs: object) -> None:
        self.submit_called = True
        raise AssertionError("preflight must never submit")

    def diagnostics_report(self) -> dict[str, Any]:
        return {
            "next_valid_id": self._next_valid_id,
            "next_valid_id_source": self.next_valid_id_source,
            "next_valid_id_requested": self.next_valid_id_requested,
        }


def read_report(result) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return json.loads(result.report_json.read_text(encoding="utf-8"))


def test_read_only_preflight_reports_ready_without_submit(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport()

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-ready",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.READY_READ_ONLY
    assert payload["classification"] == "READY_READ_ONLY"
    assert payload["submit_enabled"] is False
    assert payload["position"]["signed_quantity"] == 0
    assert payload["open_orders"] == []
    assert payload["transport_diagnostics"]["next_valid_id"] == 1001
    assert payload["transport_diagnostics"]["next_valid_id_source"] == "initial_passive"
    assert payload["transport_diagnostics"]["next_valid_id_requested"] is False
    assert transport.connected_with == {"host": "127.0.0.1", "port": 7497, "client_id": 77, "readonly": True}
    assert transport.disconnect_count == 1
    assert transport.place_order_called is False
    assert transport.submit_called is False
    assert result.report_md.exists()


def test_account_mismatch_blocks_read_only_preflight(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport(managed_accounts=("DU7654321",))

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-account-mismatch",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.BLOCKED
    assert "configured paper account" in str(payload["failure_or_ambiguity"])
    assert transport.place_order_called is False
    assert transport.submit_called is False


def test_missing_next_valid_id_blocks_read_only_preflight(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport(next_valid_id=None)

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-missing-next-valid-id",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.BLOCKED
    assert "nextValidId" in payload["missing_callbacks"]
    assert "nextValidId" in str(payload["failure_or_ambiguity"])
    assert transport.place_order_called is False
    assert transport.submit_called is False


def test_existing_open_order_is_reported_and_blocks_proof_readiness(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport(
        open_orders=(
            {
                "broker_order_id": "1001",
                "perm_id": "9001",
                "client_id": 77,
                "action": "BUY",
                "quantity": 1,
                "order_type": "LMT",
                "limit_price": "2345.2",
                "status": "Submitted",
                "filled_quantity": 0,
                "remaining_quantity": 1,
            },
        )
    )

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-open-order",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.BLOCKED
    assert payload["open_orders"][0]["broker_order_id"] == "1001"
    assert "existing open order" in str(payload["failure_or_ambiguity"])
    assert transport.place_order_called is False
    assert transport.submit_called is False


def test_existing_position_is_reported_and_blocks_proof_readiness(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport(position={"signed_quantity": 1, "average_price": "2345.0"})

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-position",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.BLOCKED
    assert payload["position"]["signed_quantity"] == 1
    assert "existing position" in str(payload["failure_or_ambiguity"])
    assert transport.place_order_called is False
    assert transport.submit_called is False


def test_quote_missing_is_reported_without_submit(tmp_path: Path) -> None:
    transport = FakeReadOnlyTransport(quote=None)

    result = run_read_only_preflight(
        config=config(tmp_path),
        transport=transport,
        run_id="preflight-missing-quote",
        now=aware_now(),
    )
    payload = read_report(result)

    assert result.classification == PreflightClassification.READY_READ_ONLY
    assert payload["quote"] is None
    assert {"name": "quote_observed", "passed": False, "blocking": False, "detail": "quote missing"} in payload["checks"]
    assert transport.place_order_called is False
    assert transport.submit_called is False


def test_runtime_allowlists_include_confirmed_mgc_202606_metadata() -> None:
    expected = {
        "symbol": "MGC",
        "security_type": "FUT",
        "exchange": "COMEX",
        "currency": "USD",
        "contract_month": "202606",
        "expiry": "20260626",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "multiplier": "10",
        "tick_size": "0.1",
    }

    preflight_entry = ReadOnlyPreflightConfig().contract_allowlist["MGC-202606"]
    harness_entry = HarnessConfig().contract_allowlist["MGC-202606"]

    assert preflight_entry == expected
    assert harness_entry == expected
    assert detect_contract_allowlist_ambiguities(
        contract_key="MGC-202606",
        allowlist_entry=preflight_entry,
    ) == ()
    assert detect_contract_allowlist_ambiguities(
        contract_key="MGC-202606",
        allowlist_entry=harness_entry,
    ) == ()


def test_preflight_source_has_no_submit_or_place_order_call_path() -> None:
    source = inspect.getsource(preflight)

    assert "placeOrder(" not in source
    assert ".placeOrder" not in source
    assert "submit_limit_order(" not in source
    assert "submit_enabled=True" not in source
