"""Read-only MNQ/NQ quote availability diagnostics for IBKR paper."""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..execution.ibkr_manual_paper_submit import (
    IbkrManualPaperSubmitConfig,
    IbkrManualPaperSubmitTransport,
    _build_runtime,
    _qualify_futures_contract,
    _request_market_data_snapshot,
    _start_runtime,
)
from ..execution.ibkr_phase1_futures_scope import active_index_contract_month, phase1_execution_target_for_symbol
from ..execution.ibkr_read_only_verifier import _wait_for_connection_ready

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_mnq_nq_scope_support"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497
DEFAULT_CLIENT_ID = 9472
DEFAULT_ACCOUNT_ID = "DUM882026"
DEFAULT_TIMEOUT_SECONDS = 15.0

QUOTE_REPORT_JSON = "ibkr_mnq_nq_quote_diagnostic_report.json"
QUOTE_REPORT_MD = "ibkr_mnq_nq_quote_diagnostic_report.md"
QUOTE_PROBE_CSV = "ibkr_mnq_nq_quote_probe_table.csv"
MARKET_DATA_REPORT_MD = "ibkr_mnq_nq_market_data_report.md"

_PROBE_MODES = (
    ("live", 1),
    ("delayed", 3),
    ("frozen", 2),
    ("delayed-frozen", 4),
)
_PERMISSION_ERROR_CODES = {354, 10168}
_DELAYED_ONLY_ERROR_CODES = {10167}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only MNQ/NQ quote diagnostics against IBKR paper.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID)
    parser.add_argument("--account-id", default=DEFAULT_ACCOUNT_ID)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if args.overwrite:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    elif OUTPUT_DIR.exists() and any(OUTPUT_DIR.iterdir()):
        raise RuntimeError(f"{OUTPUT_DIR} is not empty; rerun with --overwrite to refresh artifacts.")

    artifacts = run_quote_diagnostic(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        account_id=args.account_id,
        timeout_seconds=args.timeout_seconds,
        sleep_fn=time.sleep,
    )
    write_artifacts(artifacts=artifacts)
    print(f"classification={artifacts['classification']}")
    print(f"probe_rows={len(artifacts['probe_rows'])}")
    return 0


def run_quote_diagnostic(
    *,
    host: str,
    port: int,
    client_id: int,
    account_id: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    started_at = _utc_now()
    runtime = _build_runtime(
        config=IbkrManualPaperSubmitConfig(
            repo_root=REPO_ROOT,
            mode="PAPER",
            host=host,
            port=port,
            client_id=client_id,
            account_id=account_id,
            symbol="MNQ",
            expiry=active_index_contract_month(datetime.now(timezone.utc)),
            action="BUY",
            quantity=1.0,
            order_type="LMT",
            time_in_force="DAY",
            submit=False,
        ),
        transport_factory=IbkrManualPaperSubmitTransport,
        module_loader=None,
    )

    try:
        runtime.transport.connect()
        _start_runtime(runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            raise RuntimeError("IBKR paper runtime did not become connection-ready within the timeout window.")

        contract_specs = (
            ("MGC", str(phase1_execution_target_for_symbol("MGC").get("contract_month") or "202606")),
            ("MNQ", active_index_contract_month(datetime.now(timezone.utc))),
            ("NQ", active_index_contract_month(datetime.now(timezone.utc))),
        )
        contracts: dict[str, dict[str, Any]] = {}
        probe_rows: list[dict[str, Any]] = []
        for symbol, expiry in contract_specs:
            contract_report = _qualify_futures_contract(
                transport=runtime.transport,
                collector=runtime.collector,
                symbol=symbol,
                expiry=expiry,
                timeout_seconds=timeout_seconds,
                sleep_fn=sleep_fn,
            )
            contracts[symbol] = contract_report
            if not contract_report.get("ok"):
                continue
            contract = contract_report["qualified_contract_object"]
            for mode_label, market_data_type in _PROBE_MODES:
                request_id = _request_id_for(symbol=symbol, market_data_type=market_data_type)
                probe = _request_market_data_snapshot(
                    transport=runtime.transport,
                    collector=runtime.collector,
                    contract=contract,
                    request_id=request_id,
                    market_data_type=market_data_type,
                    timeout_seconds=timeout_seconds,
                    sleep_fn=sleep_fn,
                )
                probe_rows.append(
                    _probe_row(
                        symbol=symbol,
                        contract=contract_report,
                        mode_label=mode_label,
                        market_data_type=market_data_type,
                        probe=probe,
                    )
                )
        errors = list(runtime.collector.errors)
    finally:
        try:
            runtime.transport.disconnect()
        except Exception:
            pass

    classification = _classify(contracts=contracts, probe_rows=probe_rows)
    report = {
        "classification": classification,
        "generated_at": _utc_now(),
        "started_at": started_at,
        "environment": {
            "mode": "PAPER",
            "host": host,
            "port": port,
            "account_id": account_id,
            "client_id": client_id,
        },
        "contracts": {
            symbol: _contract_summary(row)
            for symbol, row in contracts.items()
        },
        "probe_rows": probe_rows,
        "comparison": _build_comparison(probe_rows),
        "errors": errors,
        "summary": _build_summary(classification=classification, contracts=contracts, probe_rows=probe_rows),
    }
    return {
        "classification": classification,
        "contracts": contracts,
        "probe_rows": probe_rows,
        "report": report,
    }


def write_artifacts(*, artifacts: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = dict(artifacts["report"])
    probe_rows = list(artifacts["probe_rows"])

    (OUTPUT_DIR / QUOTE_REPORT_JSON).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (OUTPUT_DIR / QUOTE_REPORT_MD).write_text(_render_markdown(report) + "\n", encoding="utf-8")
    _write_probe_csv(OUTPUT_DIR / QUOTE_PROBE_CSV, probe_rows)
    (OUTPUT_DIR / MARKET_DATA_REPORT_MD).write_text(_render_market_data_report(report) + "\n", encoding="utf-8")


def _request_id_for(*, symbol: str, market_data_type: int) -> int:
    base = {"MGC": 9710, "MNQ": 9720, "NQ": 9730}[str(symbol).upper()]
    return base + int(market_data_type)


def _probe_row(
    *,
    symbol: str,
    contract: dict[str, Any],
    mode_label: str,
    market_data_type: int,
    probe: dict[str, Any],
) -> dict[str, Any]:
    qualified = dict(contract.get("qualified_contract") or {})
    latest_error = None
    errors = list(probe.get("errors") or [])
    if errors:
        latest_error = dict(errors[-1])
    response_code = probe.get("response_code")
    response_message = probe.get("response_message")
    if latest_error is not None and response_code is None:
        response_code = latest_error.get("code")
        response_message = latest_error.get("message")
    return {
        "symbol": symbol,
        "mode_label": mode_label,
        "market_data_type_requested": market_data_type,
        "market_data_type_reported": probe.get("market_data_type_reported"),
        "has_quote": bool(probe.get("any_tick_returned")),
        "response_indication": probe.get("response_indication"),
        "response_code": response_code,
        "response_message": response_message,
        "bid_price": probe.get("bid_price"),
        "ask_price": probe.get("ask_price"),
        "last_price": probe.get("last_price"),
        "close_price": probe.get("close_price"),
        "exchange": qualified.get("exchange"),
        "currency": qualified.get("currency"),
        "local_symbol": qualified.get("local_symbol"),
        "expiry": qualified.get("expiry"),
        "multiplier": qualified.get("multiplier"),
        "trading_class": qualified.get("trading_class"),
        "con_id": qualified.get("con_id"),
        "raw_errors": errors,
    }


def _contract_summary(row: dict[str, Any]) -> dict[str, Any]:
    qualified = dict(row.get("qualified_contract") or {})
    return {
        "ok": bool(row.get("ok")),
        "detail": row.get("detail"),
        "contract": {
            "internal_symbol": qualified.get("internal_symbol"),
            "broker_symbol": qualified.get("broker_symbol"),
            "exchange": qualified.get("exchange"),
            "currency": qualified.get("currency"),
            "local_symbol": qualified.get("local_symbol"),
            "lastTradeDateOrContractMonth": qualified.get("expiry"),
            "multiplier": qualified.get("multiplier"),
            "tradingClass": qualified.get("trading_class"),
            "conId": qualified.get("con_id"),
        },
    }


def _build_comparison(probe_rows: list[dict[str, Any]]) -> dict[str, Any]:
    def summarize(symbol: str) -> dict[str, Any]:
        rows = [row for row in probe_rows if row.get("symbol") == symbol]
        return {
            "any_quote": any(bool(row.get("has_quote")) for row in rows),
            "delayed_mode_quote": any(bool(row.get("has_quote")) for row in rows if row.get("mode_label") == "delayed"),
            "live_mode_quote": any(bool(row.get("has_quote")) for row in rows if row.get("mode_label") == "live"),
            "response_codes": sorted(
                {
                    int(row["response_code"])
                    for row in rows
                    if row.get("response_code") not in (None, "")
                }
            ),
        }

    return {
        "mgc": summarize("MGC"),
        "mnq": summarize("MNQ"),
        "nq": summarize("NQ"),
    }


def _classify(*, contracts: dict[str, dict[str, Any]], probe_rows: list[dict[str, Any]]) -> str:
    if not contracts.get("MNQ", {}).get("ok") or not contracts.get("NQ", {}).get("ok"):
        return "IBKR_MNQ_NQ_QUOTES_BLOCKED_CONTRACT"

    mnq_nq_rows = [row for row in probe_rows if row.get("symbol") in {"MNQ", "NQ"}]
    if any(bool(row.get("has_quote")) and row.get("response_indication") == "data_returned" for row in mnq_nq_rows):
        return "IBKR_MNQ_NQ_QUOTES_READY"
    if any(bool(row.get("has_quote")) for row in mnq_nq_rows):
        return "IBKR_MNQ_NQ_QUOTES_DELAYED_ONLY"

    mnq_nq_codes = {
        int(row["response_code"])
        for row in mnq_nq_rows
        if row.get("response_code") not in (None, "")
    }
    mgc_rows = [row for row in probe_rows if row.get("symbol") == "MGC"]
    mgc_has_quote = any(bool(row.get("has_quote")) for row in mgc_rows)
    if mnq_nq_codes & (_PERMISSION_ERROR_CODES | _DELAYED_ONLY_ERROR_CODES):
        return "IBKR_MNQ_NQ_QUOTES_BLOCKED_ENTITLEMENT" if mgc_has_quote else "IBKR_MNQ_NQ_QUOTES_UNAVAILABLE_UNKNOWN"
    return "IBKR_MNQ_NQ_QUOTES_UNAVAILABLE_UNKNOWN"


def _build_summary(*, classification: str, contracts: dict[str, dict[str, Any]], probe_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "classification": classification,
        "mnq_contract_ok": bool(contracts.get("MNQ", {}).get("ok")),
        "nq_contract_ok": bool(contracts.get("NQ", {}).get("ok")),
        "mnq_any_quote": any(bool(row.get("has_quote")) for row in probe_rows if row.get("symbol") == "MNQ"),
        "nq_any_quote": any(bool(row.get("has_quote")) for row in probe_rows if row.get("symbol") == "NQ"),
        "mgc_any_quote": any(bool(row.get("has_quote")) for row in probe_rows if row.get("symbol") == "MGC"),
        "mnq_nq_errors": [
            {
                "symbol": row.get("symbol"),
                "mode_label": row.get("mode_label"),
                "response_code": row.get("response_code"),
                "response_message": row.get("response_message"),
            }
            for row in probe_rows
            if row.get("symbol") in {"MNQ", "NQ"} and row.get("response_code") not in (None, "")
        ],
    }


def _write_probe_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "mode_label",
                "market_data_type_requested",
                "market_data_type_reported",
                "has_quote",
                "response_indication",
                "response_code",
                "response_message",
                "bid_price",
                "ask_price",
                "last_price",
                "close_price",
                "exchange",
                "currency",
                "local_symbol",
                "expiry",
                "multiplier",
                "trading_class",
                "con_id",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in writer.fieldnames})


def _render_markdown(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    comparison = dict(report.get("comparison") or {})
    lines = [
        "# IBKR MNQ/NQ Quote Diagnostic",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- MGC comparison quote available: `{summary.get('mgc_any_quote')}`",
        f"- MNQ any quote: `{summary.get('mnq_any_quote')}`",
        f"- NQ any quote: `{summary.get('nq_any_quote')}`",
        "",
        "## Contract Check",
        "",
    ]
    for symbol in ("MGC", "MNQ", "NQ"):
        contract = dict((report.get("contracts") or {}).get(symbol) or {})
        detail = dict(contract.get("contract") or {})
        lines.extend(
            [
                f"- `{symbol}` ok: `{contract.get('ok')}`",
                f"  contract: `{detail.get('local_symbol')} / conId={detail.get('conId')} / exchange={detail.get('exchange')} / expiry={detail.get('lastTradeDateOrContractMonth')}`",
                f"  detail: {contract.get('detail')}",
            ]
        )
    lines.extend(
        [
            "",
            "## Comparison",
            "",
            f"- MGC response codes: `{comparison.get('mgc', {}).get('response_codes')}`",
            f"- MNQ response codes: `{comparison.get('mnq', {}).get('response_codes')}`",
            f"- NQ response codes: `{comparison.get('nq', {}).get('response_codes')}`",
            "",
            "## Interpretation",
            "",
            _interpretation_line(str(report.get("classification") or "")),
        ]
    )
    return "\n".join(lines)


def _render_market_data_report(report: dict[str, Any]) -> str:
    summary = dict(report.get("summary") or {})
    return "\n".join(
        [
            "# IBKR MNQ/NQ Market Data Report",
            "",
            f"- classification: `{report.get('classification')}`",
            "- exact qualified contracts were probed in live, delayed, frozen, and delayed-frozen request modes",
            f"- MGC comparison quote path available: `{summary.get('mgc_any_quote')}`",
            f"- MNQ quote available in any mode: `{summary.get('mnq_any_quote')}`",
            f"- NQ quote available in any mode: `{summary.get('nq_any_quote')}`",
            "- actionable MNQ/NQ LMT DAY submits remain blocked until quote source is usable",
            "- lanes remain structurally ported even when quote-blocked",
        ]
    )


def _interpretation_line(classification: str) -> str:
    if classification == "IBKR_MNQ_NQ_QUOTES_READY":
        return "- MNQ/NQ returned usable non-delayed quote data. Quote gating can treat the index scope as ready."
    if classification == "IBKR_MNQ_NQ_QUOTES_DELAYED_ONLY":
        return "- MNQ/NQ returned delayed quote data only. LMT pricing can use delayed quotes with explicit delayed labeling."
    if classification == "IBKR_MNQ_NQ_QUOTES_BLOCKED_ENTITLEMENT":
        return "- MNQ/NQ contracts qualify, but quote requests are blocked by market-data permissions or delayed-data settings rather than bridge routing."
    if classification == "IBKR_MNQ_NQ_QUOTES_BLOCKED_CONTRACT":
        return "- MNQ/NQ quote probing is blocked by contract qualification rather than entitlement."
    return "- MNQ/NQ quote probing did not return a conclusive contract or permission failure. Leave quote gating blocked pending further investigation."


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
