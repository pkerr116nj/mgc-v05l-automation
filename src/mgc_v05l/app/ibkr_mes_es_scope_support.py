"""Read-only MES/ES IBKR paper scope support audit and porting pass."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..execution.ibkr_manual_paper_submit import (
    IbkrManualPaperSubmitConfig,
    IbkrManualPaperSubmitTransport,
    _build_runtime,
    _probe_delayed_quote_context,
    _qualify_futures_contract,
    _refresh_open_orders_snapshot,
    _refresh_positions_snapshot,
    _start_runtime,
)
from ..execution.ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    run_ibkr_paper_strategy_porting,
    write_ibkr_paper_strategy_porting_artifacts,
)
from ..execution.ibkr_phase1_futures_scope import active_index_contract_month
from ..execution.ibkr_read_only_verifier import _wait_for_connection_ready
from .ibkr_mnq_nq_scope_support import (
    _exact_symbol_open_order_count,
    _exact_symbol_position_quantity,
    _lane_inventory_row,
    _load_json,
    _utc_now,
    _write_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_mes_es_scope_support"
PORTING_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting"
LEDGER_PATH = REPO_ROOT / "var" / "paper_strategy_position_ledger.json"
MES_ES_INSTRUMENTS = {"MES", "ES"}
EXECUTION_SYMBOL = "MES"
DEFAULT_ACCOUNT_ID = "DUM882026"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497
DEFAULT_CLIENT_ID = 9481
DEFAULT_TIMEOUT_SECONDS = 15.0
LANE_INVENTORY_CSV = "ibkr_mes_es_lane_inventory.csv"
CONTRACT_REPORT_MD = "ibkr_mes_es_contract_qualification_report.md"
MARKET_DATA_REPORT_MD = "ibkr_mes_es_market_data_report.md"
SCOPE_REPORT_MD = "ibkr_mes_es_scope_support_report.md"
PORTING_REPORT_MD = "ibkr_mes_es_lane_porting_report.md"
EXPOSURE_JSON = "paper_mes_es_exposure_state.json"
EXPOSURE_AUDIT = "paper_mes_es_exposure_gate_audit.jsonl"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the read-only MES/ES IBKR paper scope support pass.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID)
    parser.add_argument("--account-id", default=DEFAULT_ACCOUNT_ID)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    contract_month = active_index_contract_month(datetime.now(timezone.utc))
    runtime = _build_runtime(
        config=IbkrManualPaperSubmitConfig(
            repo_root=REPO_ROOT,
            mode="PAPER",
            host=args.host,
            port=args.port,
            client_id=args.client_id,
            account_id=args.account_id,
            symbol=EXECUTION_SYMBOL,
            expiry=contract_month,
            action="BUY",
            quantity=1.0,
            order_type="LMT",
            time_in_force="DAY",
            submit=False,
        ),
        transport_factory=IbkrManualPaperSubmitTransport,
        module_loader=None,
    )
    audit_events: list[dict[str, Any]] = []
    try:
        runtime.transport.connect()
        _start_runtime(runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=args.timeout_seconds,
            sleep_fn=time.sleep,
        ):
            raise RuntimeError("IBKR paper runtime did not become connection-ready within the timeout window.")

        qualified_contracts: dict[str, dict[str, Any]] = {}
        quote_reports: dict[str, dict[str, Any]] = {}
        for symbol in ("MES", "ES"):
            contract_report = _qualify_futures_contract(
                transport=runtime.transport,
                collector=runtime.collector,
                symbol=symbol,
                expiry=contract_month,
                timeout_seconds=args.timeout_seconds,
                sleep_fn=time.sleep,
            )
            qualified_contracts[symbol] = contract_report
            quote_reports[symbol] = (
                _probe_delayed_quote_context(
                    transport=runtime.transport,
                    collector=runtime.collector,
                    contract=contract_report["qualified_contract_object"],
                    timeout_seconds=args.timeout_seconds,
                    sleep_fn=time.sleep,
                )
                if contract_report.get("ok")
                else {}
            )
            audit_events.append(
                {
                    "event_type": "contract_scope_probe",
                    "observed_at": _utc_now(),
                    "symbol": symbol,
                    "contract_ok": bool(contract_report.get("ok")),
                    "quote_source_label": quote_reports[symbol].get("quote_source_label"),
                    "detail": contract_report.get("detail"),
                }
            )

        positions = _refresh_positions_snapshot(
            runtime=runtime,
            selected_account_id=args.account_id,
            timeout_seconds=args.timeout_seconds,
            sleep_fn=time.sleep,
        )
        open_orders = _refresh_open_orders_snapshot(
            runtime=runtime,
            selected_account_id=args.account_id,
            timeout_seconds=args.timeout_seconds,
            sleep_fn=time.sleep,
        )
    finally:
        try:
            runtime.transport.disconnect()
        except Exception:
            pass

    porting_config = IbkrPaperStrategyPortingConfig(repo_root=REPO_ROOT, output_dir=PORTING_OUTPUT_DIR.relative_to(REPO_ROOT))
    porting_artifacts = run_ibkr_paper_strategy_porting(config=porting_config)
    write_ibkr_paper_strategy_porting_artifacts(config=porting_config, artifacts=porting_artifacts)

    inventory = [
        dict(row)
        for row in porting_artifacts.inventory_rows
        if str(row.get("instrument") or "").upper() in MES_ES_INSTRUMENTS
    ]
    strategy_ids = {str(row.get("strategy_id") or "") for row in inventory}
    intent_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in porting_artifacts.intent_rows
        if str(row.get("strategy_id") or "") in strategy_ids
    }
    governance_rows = {
        str(row.get("strategy_id") or ""): dict(row)
        for row in list(_load_json(REPO_ROOT / "var" / "per_strategy_paper_status.json").get("strategies") or [])
    }
    lane_rows = [
        _lane_inventory_row(
            row,
            intent_rows.get(str(row.get("strategy_id") or ""), {}),
            governance_rows.get(str(row.get("strategy_id") or ""), {}),
        )
        for row in inventory
    ]

    ledger = _load_json(LEDGER_PATH)
    broker_net_mes = _exact_symbol_position_quantity(positions, EXECUTION_SYMBOL)
    ledger_rows = [dict(row) for row in list(ledger.get("positions") or []) if str(row.get("symbol") or "").upper() == EXECUTION_SYMBOL]
    strategy_attributed_mes = round(sum(float(row.get("quantity") or 0.0) for row in ledger_rows), 8)
    exposure_state = {
        "generated_at": _utc_now(),
        "executable_symbol": EXECUTION_SYMBOL,
        "allow_stacking": True,
        "max_total_mes_contracts": 20.0,
        "max_per_strategy_mes_contracts": 1.0,
        "max_total_es_equivalent": 2.0,
        "allow_direct_strategy_flip": False,
        "broker_net_mes": broker_net_mes,
        "strategy_attributed_mes": strategy_attributed_mes,
        "broker_minus_ledger_difference": round(broker_net_mes - strategy_attributed_mes, 8),
        "orphan_exposure": round(max(broker_net_mes - strategy_attributed_mes, 0.0), 8),
        "open_mes_orders": _exact_symbol_open_order_count(open_orders, EXECUTION_SYMBOL),
    }
    classification = _scope_classification(
        qualified_contracts=qualified_contracts,
        quote_reports=quote_reports,
        lane_rows=lane_rows,
        exposure_state=exposure_state,
    )

    _write_csv(OUTPUT_DIR / LANE_INVENTORY_CSV, lane_rows)
    (OUTPUT_DIR / CONTRACT_REPORT_MD).write_text(_render_contract_report(contract_month, qualified_contracts) + "\n", encoding="utf-8")
    (OUTPUT_DIR / MARKET_DATA_REPORT_MD).write_text(_render_market_data_report(quote_reports) + "\n", encoding="utf-8")
    (OUTPUT_DIR / SCOPE_REPORT_MD).write_text(
        _render_scope_support_report(classification, contract_month, qualified_contracts, quote_reports, lane_rows, exposure_state) + "\n",
        encoding="utf-8",
    )
    (OUTPUT_DIR / PORTING_REPORT_MD).write_text(_render_lane_porting_report(classification, lane_rows) + "\n", encoding="utf-8")
    (OUTPUT_DIR / EXPOSURE_JSON).write_text(json.dumps(exposure_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (OUTPUT_DIR / EXPOSURE_AUDIT).open("w", encoding="utf-8") as handle:
        for row in audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")

    print(f"classification={classification}")
    print(f"mes_es_lane_count={len(lane_rows)}")
    print(f"broker_net_mes={broker_net_mes}")
    print(f"strategy_attributed_mes={strategy_attributed_mes}")
    return 0


def _scope_classification(
    *,
    qualified_contracts: dict[str, dict[str, Any]],
    quote_reports: dict[str, dict[str, Any]],
    lane_rows: list[dict[str, Any]],
    exposure_state: dict[str, Any],
) -> str:
    contracts_ok = all(bool(row.get("ok")) for row in qualified_contracts.values())
    quotes_usable = all(bool(row.get("has_quote")) for row in quote_reports.values())
    adapters_ready = bool(lane_rows) and all(bool(row.get("bridge_submit_capable")) for row in lane_rows)
    exposure_clean = (
        float(exposure_state.get("broker_minus_ledger_difference") or 0.0) == 0.0
        and float(exposure_state.get("orphan_exposure") or 0.0) == 0.0
    )
    if contracts_ok and quotes_usable and adapters_ready and exposure_clean:
        return "IBKR_MES_ES_SCOPE_READY"
    if lane_rows:
        return "IBKR_MES_ES_SCOPE_PARTIAL"
    return "IBKR_MES_ES_SCOPE_BLOCKED"


def _render_contract_report(contract_month: str, qualified_contracts: dict[str, dict[str, Any]]) -> str:
    lines = [
        "# IBKR MES/ES Contract Qualification",
        "",
        f"- requested contract month: `{contract_month}`",
    ]
    for symbol in ("MES", "ES"):
        report = dict(qualified_contracts.get(symbol) or {})
        contract = dict(report.get("qualified_contract") or {})
        lines.extend(
            [
                f"- `{symbol}` ok: `{report.get('ok')}`",
                f"- `{symbol}` detail: `{report.get('detail')}`",
                f"- `{symbol}` exact qualified contract: `{contract.get('broker_symbol')} {contract.get('expiry')} / conId={contract.get('con_id')} / localSymbol={contract.get('local_symbol')}`",
                f"- `{symbol}` exchange/currency/multiplier: `{contract.get('exchange')} / {contract.get('currency')} / {contract.get('multiplier')}`",
            ]
        )
    return "\n".join(lines)


def _render_market_data_report(quote_reports: dict[str, dict[str, Any]]) -> str:
    lines = ["# IBKR MES/ES Market Data", ""]
    for symbol in ("MES", "ES"):
        quote = dict(quote_reports.get(symbol) or {})
        lines.extend(
            [
                f"- `{symbol}` quote source: `{quote.get('quote_source_label')}`",
                f"- `{symbol}` live market data available: `{quote.get('live_market_data_available')}`",
                f"- `{symbol}` bid / ask / last / close: `{quote.get('bid_price')} / {quote.get('ask_price')} / {quote.get('last_price')} / {quote.get('close_price')}`",
                f"- `{symbol}` updated at: `{quote.get('updated_at')}`",
                f"- `{symbol}` warning: `{quote.get('live_market_data_warning')}`",
            ]
        )
    return "\n".join(lines)


def _render_scope_support_report(
    classification: str,
    contract_month: str,
    qualified_contracts: dict[str, dict[str, Any]],
    quote_reports: dict[str, dict[str, Any]],
    lane_rows: list[dict[str, Any]],
    exposure_state: dict[str, Any],
) -> str:
    ready_no_action = len([row for row in lane_rows if row.get("classification") == "PAPER_LANE_SUBMIT_READY_NO_ACTION"])
    quote_usable = all(bool(row.get("has_quote")) for row in quote_reports.values())
    lines = [
        "# IBKR MES/ES Scope Support",
        "",
        f"- classification: `{classification}`",
        f"- preferred phase-1 execution proxy: `{EXECUTION_SYMBOL}`",
        f"- active contract month: `{contract_month}`",
        f"- contract qualification succeeded for both MES and ES: `{all(bool(row.get('ok')) for row in qualified_contracts.values())}`",
        f"- delayed/live quote path usable for LMT pricing right now: `{quote_usable}`",
        f"- mes/es lanes inventoried: `{len(lane_rows)}`",
        f"- submit-capable/no-action lanes: `{ready_no_action}`",
        f"- broker net MES: `{exposure_state.get('broker_net_mes')}`",
        f"- strategy-attributed MES: `{exposure_state.get('strategy_attributed_mes')}`",
        f"- broker-minus-ledger difference: `{exposure_state.get('broker_minus_ledger_difference')}`",
        f"- orphan exposure: `{exposure_state.get('orphan_exposure')}`",
    ]
    if not quote_usable:
        lines.append("- note: `MES/ES contract identity and lane adapters are ready, but the current paper session is not returning usable delayed/live quotes for LMT pricing, so actionable submits should remain fail-closed until quote availability improves.`")
    return "\n".join(lines)


def _render_lane_porting_report(classification: str, lane_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# IBKR MES/ES Lane Porting",
        "",
        f"- scope classification: `{classification}`",
        f"- total lanes processed: `{len(lane_rows)}`",
    ]
    for row in lane_rows:
        lines.append(
            f"- `{row.get('strategy_id')}` / `{row.get('source_instrument')}`: classification=`{row.get('classification')}` intent=`{row.get('standardized_intent_action')}` target=`{row.get('bridge_execution_target_symbol')} {row.get('bridge_execution_target_contract_month')}` blocker=`{row.get('blocker') or 'none'}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
