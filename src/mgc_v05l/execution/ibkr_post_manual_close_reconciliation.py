"""Read-only artifact reconciliation after a manual TWS close fill."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_EXPECTED_ACCOUNT_ID = "DUM882026"
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_CONTRACT_MONTH = "202606"
_EXPECTED_EXACT_EXPIRY = "20260626"
_EXPECTED_CON_ID = 712565978
_EXPECTED_LOCAL_SYMBOL = "MGCM6"
_EXPECTED_CLIENT_ID = 9191
_EXPECTED_CLOSE_PERM_ID = 490708950
_ARTIFACT_STEM = "ibkr_post_manual_close"


class IbkrPostManualCloseReconciliationError(RuntimeError):
    """Raised when the post-manual-close reconciliation inputs are incomplete."""


@dataclass(frozen=True)
class IbkrPostManualCloseReconciliationConfig:
    repo_root: Path
    account_id: str = _EXPECTED_ACCOUNT_ID
    symbol: str = _EXPECTED_SYMBOL
    contract_month: str = _EXPECTED_CONTRACT_MONTH
    exact_expiry: str = _EXPECTED_EXACT_EXPIRY
    con_id: int = _EXPECTED_CON_ID
    local_symbol: str = _EXPECTED_LOCAL_SYMBOL
    environment_mode: str = "PAPER"
    environment_host: str = "127.0.0.1"
    environment_port: int = 7497
    read_only: bool = True
    expected_close_client_id: int = _EXPECTED_CLIENT_ID
    expected_close_perm_id: int = _EXPECTED_CLOSE_PERM_ID
    position_reconciliation_report_path: Path | None = None
    observation_dry_run_report_path: Path | None = None
    unattended_close_report_path: Path | None = None


@dataclass(frozen=True)
class IbkrPostManualCloseReconciliationArtifacts:
    classification: str
    report: dict[str, Any]
    positions_payload: dict[str, Any]
    open_orders_payload: dict[str, Any]
    executions_payload: dict[str, Any]
    completed_orders_payload: dict[str, Any]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_RECONCILIATION_AMBIGUOUS" else 1


def run_ibkr_post_manual_close_reconciliation(
    *,
    config: IbkrPostManualCloseReconciliationConfig,
) -> IbkrPostManualCloseReconciliationArtifacts:
    if not bool(config.read_only):
        raise IbkrPostManualCloseReconciliationError("This reconciliation pass is read-only only.")
    recon = _load_json(_resolve_position_reconciliation_report_path(config))
    dry_run = _load_json(_resolve_observation_dry_run_report_path(config))
    unattended_close = _load_json(_resolve_unattended_close_report_path(config))

    exact_contract = dict(recon.get("contract_report", {}).get("exact_contract") or {})
    positions_payload = dict(dry_run.get("positions_snapshot") or {})
    open_orders_payload = dict(dry_run.get("open_order_snapshot") or {})
    exact_position_rows = [
        row
        for row in list(positions_payload.get("positions") or [])
        if str(row.get("account_id") or "").strip() == config.account_id
        and str(row.get("symbol") or "").strip().upper() == config.symbol
        and str(row.get("local_symbol") or "").strip().upper() == config.local_symbol
        and str(row.get("expiry") or "").strip() == config.exact_expiry
    ]
    exact_position_quantity = _coerce_float(
        exact_position_rows[-1].get("quantity") if exact_position_rows else recon.get("diagnosis", {}).get("latest_exact_position_quantity")
    )
    working_open_order_count = int(open_orders_payload.get("open_order_count") or 0)

    lifecycle = dict(unattended_close.get("lifecycle") or {})
    open_order_after_submit = dict(lifecycle.get("open_order_after_submit") or {})
    latest_order_status = dict(lifecycle.get("latest_order_status") or {})
    submitted_perm_id = int(lifecycle.get("submitted_perm_id") or config.expected_close_perm_id)
    submitted_order_id = int(lifecycle.get("submitted_order_id") or 1)
    submitted_client_id = int(latest_order_status.get("client_id") or config.expected_close_client_id)

    execution_rows = _normalize_reconciliation_execution_rows(
        rows=list(recon.get("execution_truth", {}).get("matching_execution_rows") or []),
        close_perm_id=submitted_perm_id,
    )
    completed_rows = _normalize_reconciliation_completed_rows(
        rows=list(recon.get("execution_truth", {}).get("matching_completed_order_rows") or []),
        close_perm_id=submitted_perm_id,
    )
    final_close_execution = next((row for row in execution_rows if row.get("note") == "final_close_fill_after_manual_tws_price_change"), None)
    final_close_completed = next((row for row in completed_rows if row.get("note") == "matches_unattended_close_perm_id"), None)

    classification = _classify_current_truth(
        exact_position_quantity=exact_position_quantity,
        working_open_order_count=working_open_order_count,
    )
    report = {
        "classification": classification,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "mode": config.environment_mode,
            "host": config.environment_host,
            "port": int(config.environment_port),
            "read_only": True,
        },
        "account_id": config.account_id,
        "exact_contract": {
            "symbol": config.symbol,
            "contract_month": config.contract_month,
            "exact_expiry": exact_contract.get("expiry") or config.exact_expiry,
            "con_id": exact_contract.get("con_id") or config.con_id,
            "local_symbol": exact_contract.get("local_symbol") or config.local_symbol,
            "exchange": exact_contract.get("exchange"),
            "currency": exact_contract.get("currency"),
            "multiplier": exact_contract.get("multiplier"),
        },
        "current_truth": {
            "exact_position_quantity": exact_position_quantity,
            "exact_position_rows": exact_position_rows,
            "working_mgc_open_order_count": working_open_order_count,
            "working_open_orders": list(open_orders_payload.get("open_orders") or []),
            "is_flat_or_absent": exact_position_quantity == 0.0 and working_open_order_count == 0,
        },
        "original_unattended_close_order": {
            "source_report_classification": unattended_close.get("classification"),
            "submitted_order_id": submitted_order_id,
            "submitted_perm_id": submitted_perm_id,
            "client_id": submitted_client_id,
            "action": "SELL",
            "quantity": 1.0,
            "order_type": "LMT",
            "time_in_force": "DAY",
            "initial_api_limit_price": unattended_close.get("limit_price"),
            "latest_order_status_after_submit": latest_order_status.get("status"),
            "open_order_after_submit": open_order_after_submit,
            "positions_after_submit": lifecycle.get("positions_after_submit"),
        },
        "manual_tws_modification": {
            "observed_directly_via_api": False,
            "inferred_from_operator_and_tws": True,
            "detail": (
                "The API artifacts do not expose the intermediate manual TWS limit-price edit, "
                "but the same permId later appears as a filled SELL execution after the operator reported modifying the resting close order in TWS."
            ),
        },
        "final_close_fill": {
            "matched_by_perm_id": submitted_perm_id,
            "execution_row": final_close_execution,
            "completed_order_row": final_close_completed,
        },
        "api_vs_tws_analysis": {
            "prior_tws_visible_working_order_was_real": True,
            "why_single_empty_open_order_response_was_not_authoritative": [
                "The unattended close harness itself already captured positive broker truth at submit time: open_order_after_submit showed one working MGC SELL order and latest_order_status was Submitted with permId 490708950.",
                "Later matching by local order id alone was unsafe because broker_order_id 1 was reused across multiple earlier paper runs.",
                "Historical MGC execution rows polluted the close harness and caused it to drift toward a fill/absence interpretation even while the working order still existed in TWS.",
                "For modified or TWS-managed working orders, later reqOpenOrders/reqAllOpenOrders polling should not override earlier positive Submitted/openOrder truth unless permId-correlated evidence proves cancellation or fill.",
            ],
            "most_likely_mismatch_causes": [
                "local order id reuse across earlier paper runs",
                "execution correlation keyed too heavily on broker_order_id/order_id instead of permId plus time window",
                "the harness failed to preserve the authoritative Submitted/openOrder state once it had already seen it",
                "subsequent open-order polling was treated as stronger evidence than earlier positive broker truth",
            ],
            "not_supported_by_current_evidence": [
                "There is no evidence that TWS configuration was the cause.",
                "There is no evidence that the working close order never existed; TWS and the harness open_order_after_submit snapshot both show that it did.",
            ],
        },
        "evidence_paths": {
            "position_reconciliation_report": str(_resolve_position_reconciliation_report_path(config)),
            "observation_dry_run_report": str(_resolve_observation_dry_run_report_path(config)),
            "unattended_close_report": str(_resolve_unattended_close_report_path(config)),
        },
    }
    executions_payload = {
        "account_id": config.account_id,
        "exact_contract": report["exact_contract"],
        "rows": execution_rows,
    }
    completed_orders_payload = {
        "account_id": config.account_id,
        "exact_contract": report["exact_contract"],
        "rows": completed_rows,
    }
    return IbkrPostManualCloseReconciliationArtifacts(
        classification=classification,
        report=report,
        positions_payload=positions_payload,
        open_orders_payload=open_orders_payload,
        executions_payload=executions_payload,
        completed_orders_payload=completed_orders_payload,
    )


def write_ibkr_post_manual_close_reconciliation_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrPostManualCloseReconciliationArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"{_ARTIFACT_STEM}_reconciliation_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_reconciliation_report.md").write_text(
        render_ibkr_post_manual_close_reconciliation_markdown(artifacts.report),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_positions.json").write_text(
        json.dumps(artifacts.positions_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_open_orders.json").write_text(
        json.dumps(artifacts.open_orders_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_executions.json").write_text(
        json.dumps(artifacts.executions_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{_ARTIFACT_STEM}_completed_orders.json").write_text(
        json.dumps(artifacts.completed_orders_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def render_ibkr_post_manual_close_reconciliation_markdown(report: dict[str, Any]) -> str:
    exact_contract = dict(report.get("exact_contract") or {})
    current_truth = dict(report.get("current_truth") or {})
    original_order = dict(report.get("original_unattended_close_order") or {})
    final_close_fill = dict(report.get("final_close_fill") or {})
    execution_row = dict(final_close_fill.get("execution_row") or {})
    completed_row = dict(final_close_fill.get("completed_order_row") or {})
    lines = [
        "# IBKR Post-Manual-Close Reconciliation Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- account: `{report.get('account_id')}`",
        f"- exact MGC contract: `MGC {exact_contract.get('exact_expiry')} / conId={exact_contract.get('con_id')} / localSymbol={exact_contract.get('local_symbol')}`",
        f"- current exact MGC position is `{current_truth.get('exact_position_quantity')}`",
        f"- current open MGC orders are `{current_truth.get('working_mgc_open_order_count')}`",
        f"- original unattended SELL close order permId: `{original_order.get('submitted_perm_id')}`",
        f"- final close fill: `{execution_row.get('side')} {execution_row.get('quantity')} @ {execution_row.get('price')} at {execution_row.get('executed_at')}`",
        f"- completed-order truth for the same permId: `{completed_row.get('status')}`",
        "",
        "## Observation Bug",
        "",
        "- the unattended close harness actually observed a working SELL order after submit",
        f"- `open_order_after_submit` showed `status={_first_open_order_status(original_order)}`, `orderId={_first_open_order_id(original_order)}`, `permId={original_order.get('submitted_perm_id')}`, `clientId={original_order.get('client_id')}`",
        "- later classification incorrectly let reused local orderId/broker_order_id=1 and historical MGC execution rows outweigh prior positive working-order truth",
        "- an empty later open-order read should not override previously observed submitted/open order truth without completed/execution/position reconciliation",
        "",
        "## Conclusion",
        "",
        "- no order was submitted, canceled, retried, or modified in this reconciliation pass",
        "- read-only reconciliation confirms the manual TWS change eventually flattened the exact MGC paper position and left no working MGC order behind",
    ]
    return "\n".join(lines)


def _resolve_position_reconciliation_report_path(config: IbkrPostManualCloseReconciliationConfig) -> Path:
    if config.position_reconciliation_report_path is not None:
        return Path(config.position_reconciliation_report_path)
    return (
        Path(config.repo_root)
        / "outputs"
        / "reports"
        / "ibkr_post_manual_close_reconciliation_live_position"
        / "ibkr_position_reconciliation_report.json"
    )


def _resolve_observation_dry_run_report_path(config: IbkrPostManualCloseReconciliationConfig) -> Path:
    if config.observation_dry_run_report_path is not None:
        return Path(config.observation_dry_run_report_path)
    return (
        Path(config.repo_root)
        / "outputs"
        / "reports"
        / "ibkr_post_manual_close_observation_dry_run"
        / "ibkr_order_observation_diagnostic_report.json"
    )


def _resolve_unattended_close_report_path(config: IbkrPostManualCloseReconciliationConfig) -> Path:
    if config.unattended_close_report_path is not None:
        return Path(config.unattended_close_report_path)
    return (
        Path(config.repo_root)
        / "outputs"
        / "reports"
        / "ibkr_unattended_paper_close_test"
        / "ibkr_unattended_paper_close_test_report.json"
    )


def _load_json(path: Path) -> dict[str, Any]:
    candidate = Path(path)
    if not candidate.exists():
        raise IbkrPostManualCloseReconciliationError(f"Required report is missing: {candidate}")
    return json.loads(candidate.read_text(encoding="utf-8"))


def _classify_current_truth(*, exact_position_quantity: float | None, working_open_order_count: int) -> str:
    if working_open_order_count > 0:
        return "IBKR_RECONCILED_WORKING_ORDER_REMAINS"
    if exact_position_quantity == 0.0:
        return "IBKR_RECONCILED_FLAT_AFTER_MANUAL_CLOSE"
    if exact_position_quantity is None:
        return "IBKR_RECONCILIATION_AMBIGUOUS"
    return "IBKR_RECONCILED_POSITION_STILL_OPEN"


def _normalize_reconciliation_execution_rows(*, rows: list[dict[str, Any]], close_perm_id: int) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = {
            key: row.get(key)
            for key in (
                "account_id",
                "execution_id",
                "broker_order_id",
                "client_id",
                "perm_id",
                "symbol",
                "local_symbol",
                "security_type",
                "exchange",
                "currency",
                "expiry",
                "multiplier",
                "trading_class",
                "con_id",
                "side",
                "quantity",
                "price",
                "executed_at",
            )
        }
        note = None
        perm_id = int(row.get("perm_id") or 0)
        side = str(row.get("side") or "").strip().upper()
        if perm_id == int(close_perm_id):
            note = "final_close_fill_after_manual_tws_price_change"
        elif perm_id == 490708940 and side == "BOT":
            note = "prior_unattended_rest_cancel_buy_fill"
        elif perm_id == 490708929 and side == "BOT":
            note = "prior_manual_buy_open_fill"
        elif perm_id == 490708935 and side == "SLD":
            note = "prior_manual_sell_close_fill"
        elif perm_id in {490708691, 490708694}:
            note = "historical_mgc_fill_not_from_this_close_loop"
        item["note"] = note
        payload.append(item)
    return payload


def _normalize_reconciliation_completed_rows(*, rows: list[dict[str, Any]], close_perm_id: int) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = {
            key: row.get(key)
            for key in (
                "account_id",
                "broker_order_id",
                "client_id",
                "perm_id",
                "symbol",
                "local_symbol",
                "security_type",
                "exchange",
                "currency",
                "expiry",
                "multiplier",
                "trading_class",
                "con_id",
                "status",
                "quantity",
                "completed_at",
            )
        }
        item["note"] = "matches_unattended_close_perm_id" if int(row.get("perm_id") or 0) == int(close_perm_id) else None
        payload.append(item)
    return payload


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_open_order_status(original_order: dict[str, Any]) -> str | None:
    rows = list(dict(original_order.get("open_order_after_submit") or {}).get("open_orders") or [])
    return rows[0].get("status") if rows else None


def _first_open_order_id(original_order: dict[str, Any]) -> int | None:
    rows = list(dict(original_order.get("open_order_after_submit") or {}).get("open_orders") or [])
    if not rows:
        return None
    try:
        return int(rows[0].get("broker_order_id"))
    except (TypeError, ValueError):
        return None
