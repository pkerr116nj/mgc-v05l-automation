from __future__ import annotations

import argparse
import csv
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_paper_strategy_stack_readiness"

GOVERNANCE_JSON = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_governance" / "per_strategy_paper_status.json"
PROVIDER_MATRIX_JSON = REPO_ROOT / "outputs" / "reports" / "schwab_runtime_dependency_purge" / "provider_readiness_matrix.json"
MONITOR_SERVICE_STATUS_JSON = REPO_ROOT / "outputs" / "reports" / "paper_strategy_monitor" / "paper_monitor_service_status_report.json"
MGC_EXPOSURE_JSON = REPO_ROOT / "outputs" / "reports" / "paper_runtime_startup" / "current_paper_exposure_state.json"
MNQ_EXPOSURE_JSON = REPO_ROOT / "outputs" / "reports" / "ibkr_mnq_nq_scope_support" / "paper_index_exposure_state.json"
MES_EXPOSURE_JSON = REPO_ROOT / "outputs" / "reports" / "ibkr_mes_es_scope_support" / "paper_mes_es_exposure_state.json"
READINESS_DASHBOARD_JSON = REPO_ROOT / "outputs" / "reports" / "local_strategy_audit" / "strategy_live_readiness_dashboard.json"
SCORECARD_CSV = REPO_ROOT / "outputs" / "reports" / "local_strategy_audit" / "short_window_strategy_performance_scorecard.csv"
RATES_ROADMAP_MD = REPO_ROOT / "outputs" / "reports" / "ibkr_scope_roadmap" / "ibkr_rates_scope_readiness_plan.md"

GC_QUOTE_MODE = "DELAYED"
MNQ_QUOTE_MODE = "DELAYED_ONLY"
MES_QUOTE_MODE = "DELAYED_ONLY"


@dataclass(frozen=True)
class FamilyConfig:
    family: str
    symbols: tuple[str, ...]
    executable_proxy: str
    quote_mode: str
    exposure_path: Path
    exposure_cap_label: str


FAMILY_CONFIGS = (
    FamilyConfig(
        family="GC/MGC",
        symbols=("GC", "MGC"),
        executable_proxy="MGC",
        quote_mode=GC_QUOTE_MODE,
        exposure_path=MGC_EXPOSURE_JSON,
        exposure_cap_label="max_total_mgc_contracts=20; max_total_gc_equivalent=2; max_per_strategy_mgc_contracts=1",
    ),
    FamilyConfig(
        family="MNQ/NQ",
        symbols=("MNQ", "NQ"),
        executable_proxy="MNQ",
        quote_mode=MNQ_QUOTE_MODE,
        exposure_path=MNQ_EXPOSURE_JSON,
        exposure_cap_label="max_total_mnq_contracts=20; max_total_nq_equivalent=2; max_per_strategy_mnq_contracts=1",
    ),
    FamilyConfig(
        family="MES/ES",
        symbols=("MES", "ES"),
        executable_proxy="MES",
        quote_mode=MES_QUOTE_MODE,
        exposure_path=MES_EXPOSURE_JSON,
        exposure_cap_label="max_total_mes_contracts=20; max_total_es_equivalent=2; max_per_strategy_mes_contracts=1",
    ),
)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any) -> float:
    if value in (None, "", "None"):
        return 0.0
    return float(value)


def _family_for_symbol(symbol: str) -> str:
    for config in FAMILY_CONFIGS:
        if symbol in config.symbols:
            return config.family
    return "OTHER"


def _proxy_for_symbol(symbol: str) -> str:
    for config in FAMILY_CONFIGS:
        if symbol in config.symbols:
            return config.executable_proxy
    return ""


def _quote_mode_for_symbol(symbol: str) -> str:
    for config in FAMILY_CONFIGS:
        if symbol in config.symbols:
            return config.quote_mode
    return ""


def _exposure_summary(config: FamilyConfig) -> dict[str, Any]:
    data = _load_json(config.exposure_path)
    if config.family == "GC/MGC":
        return {
            "allow_stacking": data["allow_stacking"],
            "allow_direct_strategy_flip": data["allow_direct_strategy_flip"],
            "broker_net_position": data["broker_net_position"],
            "strategy_attributed_position": data["strategy_attributed_position_sum"],
            "broker_minus_ledger_difference": data["broker_minus_strategy_difference"],
            "orphan_exposure": data["unmatched_orphan_quantity"],
            "open_orders": len(data["strategy_open_orders"]),
            "classification": data["classification"],
            "cap_label": config.exposure_cap_label,
        }
    if config.family == "MNQ/NQ":
        return {
            "allow_stacking": data["allow_stacking"],
            "allow_direct_strategy_flip": data["allow_direct_strategy_flip"],
            "broker_net_position": data["broker_net_mnq"],
            "strategy_attributed_position": data["strategy_attributed_mnq"],
            "broker_minus_ledger_difference": data["broker_minus_ledger_difference"],
            "orphan_exposure": data["orphan_exposure"],
            "open_orders": data["open_mnq_orders"],
            "classification": "PAPER_INDEX_EXPOSURE_READY",
            "cap_label": config.exposure_cap_label,
        }
    return {
        "allow_stacking": data["allow_stacking"],
        "allow_direct_strategy_flip": data["allow_direct_strategy_flip"],
        "broker_net_position": data["broker_net_mes"],
        "strategy_attributed_position": data["strategy_attributed_mes"],
        "broker_minus_ledger_difference": data["broker_minus_ledger_difference"],
        "orphan_exposure": data["orphan_exposure"],
        "open_orders": data["open_mes_orders"],
        "classification": "PAPER_INDEX_EXPOSURE_READY",
        "cap_label": config.exposure_cap_label,
    }


def _load_monitor_status() -> dict[str, Any]:
    if MONITOR_SERVICE_STATUS_JSON.exists():
        return _load_json(MONITOR_SERVICE_STATUS_JSON)
    result = subprocess.run(
        ["bash", "scripts/status-paper-monitor"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _live_money_class(
    row: dict[str, Any],
    top_probation_ids: set[str],
) -> str:
    strategy_id = row["strategy_id"]
    status = row["strategy_status"]
    routed = row["current_routing_mode"] == "IBKR_ROUTED"
    if strategy_id in top_probation_ids and routed:
        return "PAPER_PROBATION"
    if status == "KILL_CANDIDATE":
        return "DIAGNOSTIC_ONLY"
    if status == "DEGRADED":
        return "NOT_READY"
    if status in {"PROMISING", "PROBATION_ACTIVE"} and routed:
        return "WATCHLIST"
    if status in {"PAUSED", "DISABLED"}:
        return "NOT_READY"
    return "WATCHLIST"


def _lane_readiness_bucket(row: dict[str, Any]) -> str:
    if row["current_routing_mode"] != "IBKR_ROUTED":
        return "internal-only diagnostic"
    if not row["submit_allowed"]:
        reasons = row["submit_block_reasons"] or row["route_blockers"]
        reason_text = ",".join(reasons)
        if any(key in reason_text for key in ("monitor", "backend", "source")):
            return "blocked by monitor/backend/source"
        if "unsupported_instrument_scope" in reason_text:
            return "blocked by unsupported instrument"
        if "quote" in reason_text:
            return "blocked by missing quote"
        return "blocked by governance"
    if row["intent_action"] in {"BUY", "SELL", "EXIT"}:
        return "ready now"
    return "ready but waiting for signal"


def _current_blocker(row: dict[str, Any]) -> str:
    reasons = row["submit_block_reasons"] or row["route_blockers"]
    return ",".join(reasons)


def _last_broker_trade_time(row: dict[str, Any]) -> str:
    if row.get("recent_trade_route_kind") == "BROKER_PATH":
        return row.get("last_trade_time") or ""
    return ""


def generate_report(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    monitor = _load_monitor_status()
    provider_matrix = _load_json(PROVIDER_MATRIX_JSON)
    governance = _load_json(GOVERNANCE_JSON)
    strategies = governance["strategies"]
    scorecard_rows = {row["lane_id"]: row for row in _load_csv_rows(SCORECARD_CSV)}
    readiness_dashboard = _load_json(READINESS_DASHBOARD_JSON)
    top_probation_ids = {
        row["lane_id"] for row in readiness_dashboard.get("top_paper_probation_candidates", [])
    }

    family_summaries: list[dict[str, Any]] = []
    current_broker_positions: dict[str, float] = {}
    current_open_orders: dict[str, int] = {}
    broker_ledger_differences: dict[str, float] = {}
    orphan_exposure: dict[str, float] = {}

    for config in FAMILY_CONFIGS:
        exposure = _exposure_summary(config)
        family_rows = [row for row in strategies if row["instrument"] in config.symbols]
        family_summary = {
            "family": config.family,
            "executable_proxy": config.executable_proxy,
            "submit_capable_lane_count": sum(1 for row in family_rows if row["ibkr_bridge_submit_capable"]),
            "quote_mode": config.quote_mode,
            "exposure_cap": exposure["cap_label"],
            "current_actionable_lanes": [
                row["strategy_id"] for row in family_rows if row["intent_action"] in {"BUY", "SELL", "EXIT"}
            ],
            "broker_net_position": exposure["broker_net_position"],
            "strategy_attributed_position": exposure["strategy_attributed_position"],
            "broker_minus_ledger_difference": exposure["broker_minus_ledger_difference"],
            "orphan_exposure": exposure["orphan_exposure"],
            "open_orders": exposure["open_orders"],
        }
        family_summaries.append(family_summary)
        current_broker_positions[config.executable_proxy] = exposure["broker_net_position"]
        current_open_orders[config.executable_proxy] = exposure["open_orders"]
        broker_ledger_differences[config.executable_proxy] = exposure["broker_minus_ledger_difference"]
        orphan_exposure[config.executable_proxy] = exposure["orphan_exposure"]

    lane_rows: list[dict[str, Any]] = []
    live_money_tracker_rows: list[dict[str, Any]] = []
    operator_review_rows: list[dict[str, Any]] = []
    review_counts = Counter()
    broker_path_pnl_by_strategy = []
    internal_only_pnl_by_strategy = []
    ready_counts = Counter()

    for row in strategies:
        readiness_bucket = _lane_readiness_bucket(row)
        ready_counts[readiness_bucket] += 1
        lane_entry = {
            "lane_id": row["strategy_id"],
            "source_instrument": row["instrument"],
            "executable_proxy": _proxy_for_symbol(row["instrument"]),
            "routing_mode": row["current_routing_mode"],
            "governance_status": row["strategy_status"],
            "current_intent": row["intent_action"],
            "submit_capable": row["ibkr_bridge_submit_capable"],
            "current_blocker": _current_blocker(row),
            "broker_path_pnl": row["broker_path_pnl"],
            "internal_only_pnl": row["internal_sim_pnl"],
            "last_signal_time": row["last_signal_time"] or "",
            "last_local_trade_time": row["last_trade_time"] or "",
            "last_ibkr_broker_path_trade_time": _last_broker_trade_time(row),
            "readiness_bucket": readiness_bucket,
        }
        lane_rows.append(lane_entry)

        if _to_float(row["broker_path_pnl"]) != 0.0:
            broker_path_pnl_by_strategy.append(
                {
                    "lane_id": row["strategy_id"],
                    "broker_path_pnl": row["broker_path_pnl"],
                    "routing_mode": row["current_routing_mode"],
                }
            )
        if _to_float(row["internal_sim_pnl"]) != 0.0:
            internal_only_pnl_by_strategy.append(
                {
                    "lane_id": row["strategy_id"],
                    "internal_only_pnl": row["internal_sim_pnl"],
                    "recent_trade_route_kind": row["recent_trade_route_kind"],
                }
            )

        live_money_class = _live_money_class(row, top_probation_ids)
        scorecard = scorecard_rows.get(row["strategy_id"], {})
        live_money_tracker_rows.append(
            {
                "lane_id": row["strategy_id"],
                "source_instrument": row["instrument"],
                "routing_mode": row["current_routing_mode"],
                "governance_status": row["strategy_status"],
                "live_money_readiness_class": live_money_class,
                "broker_path_pnl": row["broker_path_pnl"],
                "internal_only_pnl": row["internal_sim_pnl"],
                "total_net_pnl": row["total_net_pnl"],
                "average_trade": row["average_trade"] or scorecard.get("average_trade", ""),
                "win_rate": row["win_rate"] or scorecard.get("win_rate", ""),
                "max_drawdown": row["max_drawdown"] or scorecard.get("max_drawdown", ""),
                "reconciliation_error_count": row["reconciliation_error_count"],
                "order_rejection_count": row["rejection_count"],
                "notes": "top_paper_probation_candidate" if row["strategy_id"] in top_probation_ids else "",
            }
        )

        review_reasons = []
        if row["strategy_status"] in {"DEGRADED", "KILL_CANDIDATE", "PAUSED", "DISABLED"}:
            review_reasons.append(row["strategy_status"])
        if row["reconciliation_error_count"] > 0:
            review_reasons.append("reconciliation_errors")
        if row["rejection_count"] > 0:
            review_reasons.append("order_rejections")
        if row["open_order_ambiguity_count"] > 0:
            review_reasons.append("open_order_ambiguity")
        if _to_float(row["broker_path_pnl"]) < 0:
            review_reasons.append("negative_broker_path_pnl")
        if review_reasons:
            for reason in review_reasons:
                review_counts[reason] += 1
            operator_review_rows.append(
                {
                    "lane_id": row["strategy_id"],
                    "source_instrument": row["instrument"],
                    "governance_status": row["strategy_status"],
                    "routing_mode": row["current_routing_mode"],
                    "review_reasons": ",".join(review_reasons),
                    "broker_path_pnl": row["broker_path_pnl"],
                    "internal_only_pnl": row["internal_sim_pnl"],
                    "last_signal_time": row["last_signal_time"] or "",
                    "last_local_trade_time": row["last_trade_time"] or "",
                }
            )

    top_candidates = readiness_dashboard.get("top_paper_probation_candidates", [])
    recent_local_only = [
        row["strategy_id"]
        for row in strategies
        if row["recent_local_trades_occurred"] and row["recent_trade_route_kind"] == "INTERNAL_ONLY"
    ]
    broker_path_trade_rows = [
        row["strategy_id"]
        for row in strategies
        if row["recent_trade_route_kind"] == "BROKER_PATH" or _to_float(row["broker_path_pnl"]) != 0.0
    ]
    kill_candidates = [
        {
            "lane_id": row["strategy_id"],
            "routing_mode": row["current_routing_mode"],
            "recent_trade_route_kind": row["recent_trade_route_kind"],
        }
        for row in strategies
        if row["strategy_status"] == "KILL_CANDIDATE"
    ]

    classification = "IBKR_PAPER_STACK_READY_FOR_SUPERVISED_EVALUATION"
    if not monitor["bridge_allowed"]:
        classification = "IBKR_PAPER_STACK_BLOCKED"
    elif any(value != 0.0 for value in orphan_exposure.values()):
        classification = "IBKR_PAPER_STACK_PARTIAL"

    summary = {
        "classification": classification,
        "generated_at": _now_iso(),
        "paper_only": True,
        "broker_runtime_readiness": {
            "tws_paper_connection_status": monitor["ibkr_connection_state"],
            "account": monitor["account_id"],
            "monitor_running": monitor["monitor_running"],
            "monitor_health": monitor["health_classification"],
            "monitor_stale": monitor["stale"],
            "backend_source_readiness": {
                "bridge_allowed": monitor["bridge_allowed"],
                "bridge_blocked": monitor["bridge_blocked"],
                "exact_block_reason": monitor["exact_block_reason"],
            },
            "schwab_dependency_status": provider_matrix["readiness"]["market_data_fallback"],
            "provider_roles": provider_matrix["provider_roles"],
            "current_broker_positions_by_executable_contract": current_broker_positions,
            "current_open_orders_by_executable_contract": current_open_orders,
            "broker_ledger_differences": broker_ledger_differences,
            "orphan_exposure": orphan_exposure,
        },
        "supported_instrument_status": family_summaries + [
            {
                "family": "Rates",
                "status": "roadmap only",
                "routing_implemented": False,
            }
        ],
        "performance_governance": {
            "top_paper_probation_candidates": top_candidates,
            "kill_candidate_lanes": kill_candidates,
            "status_counts": dict(Counter(row["strategy_status"] for row in strategies)),
            "strategies_with_recent_local_only_trades": recent_local_only,
            "strategies_with_broker_path_trades": broker_path_trade_rows,
            "broker_path_pnl_by_strategy": broker_path_pnl_by_strategy,
            "internal_only_pnl_by_strategy": internal_only_pnl_by_strategy,
            "operator_review_counts": dict(review_counts),
        },
        "readiness_for_hands_off_paper_evaluation": dict(ready_counts),
        "live_money_readiness_tracker": {
            "counts": dict(Counter(row["live_money_readiness_class"] for row in live_money_tracker_rows)),
            "top_three": [row["lane_id"] for row in top_candidates],
        },
        "strategy_lanes": lane_rows,
    }

    json_path = output_dir / "ibkr_paper_strategy_stack_readiness.json"
    json_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    scope_csv_path = output_dir / "ibkr_supported_instrument_scope_status.csv"
    with scope_csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "family",
                "executable_proxy",
                "submit_capable_lane_count",
                "quote_mode",
                "exposure_cap",
                "current_actionable_lane_count",
                "broker_net_position",
                "strategy_attributed_position",
                "broker_minus_ledger_difference",
                "orphan_exposure",
                "open_orders",
            ],
        )
        writer.writeheader()
        for row in family_summaries:
            writer.writerow(
                {
                    "family": row["family"],
                    "executable_proxy": row["executable_proxy"],
                    "submit_capable_lane_count": row["submit_capable_lane_count"],
                    "quote_mode": row["quote_mode"],
                    "exposure_cap": row["exposure_cap"],
                    "current_actionable_lane_count": len(row["current_actionable_lanes"]),
                    "broker_net_position": row["broker_net_position"],
                    "strategy_attributed_position": row["strategy_attributed_position"],
                    "broker_minus_ledger_difference": row["broker_minus_ledger_difference"],
                    "orphan_exposure": row["orphan_exposure"],
                    "open_orders": row["open_orders"],
                }
            )
        writer.writerow(
            {
                "family": "Rates",
                "executable_proxy": "",
                "submit_capable_lane_count": 0,
                "quote_mode": "",
                "exposure_cap": "roadmap only",
                "current_actionable_lane_count": 0,
                "broker_net_position": "",
                "strategy_attributed_position": "",
                "broker_minus_ledger_difference": "",
                "orphan_exposure": "",
                "open_orders": "",
            }
        )

    lane_csv_path = output_dir / "ibkr_all_lane_broker_path_status.csv"
    with lane_csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(lane_rows[0].keys()))
        writer.writeheader()
        writer.writerows(lane_rows)

    tracker_csv_path = output_dir / "strategy_live_money_candidate_tracker.csv"
    with tracker_csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(live_money_tracker_rows[0].keys()))
        writer.writeheader()
        writer.writerows(live_money_tracker_rows)

    review_csv_path = output_dir / "strategy_operator_review_queue.csv"
    with review_csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(operator_review_rows[0].keys()))
        writer.writeheader()
        writer.writerows(operator_review_rows)

    md_lines = [
        "# IBKR Paper Strategy Stack Readiness",
        "",
        f"- classification: `{classification}`",
        f"- generated at: `{summary['generated_at']}`",
        f"- TWS paper connection: `{monitor['ibkr_connection_state']}`",
        f"- account: `{monitor['account_id']}`",
        f"- monitor: `running={monitor['monitor_running']}` `health={monitor['health_classification']}` `stale={monitor['stale']}`",
        f"- Schwab fallback: `{provider_matrix['readiness']['market_data_fallback']['status']}`",
        f"- market data primary: `{provider_matrix['provider_roles']['market_data_primary']}`",
        f"- broker truth provider: `{provider_matrix['provider_roles']['broker_truth_provider']}`",
        "",
        "## Broker Runtime",
        "",
        f"- current broker positions: `MGC={current_broker_positions['MGC']}` `MNQ={current_broker_positions['MNQ']}` `MES={current_broker_positions['MES']}`",
        f"- current open orders: `MGC={current_open_orders['MGC']}` `MNQ={current_open_orders['MNQ']}` `MES={current_open_orders['MES']}`",
        f"- broker-minus-ledger: `MGC={broker_ledger_differences['MGC']}` `MNQ={broker_ledger_differences['MNQ']}` `MES={broker_ledger_differences['MES']}`",
        f"- orphan exposure: `MGC={orphan_exposure['MGC']}` `MNQ={orphan_exposure['MNQ']}` `MES={orphan_exposure['MES']}`",
        "",
        "## Supported Scope",
        "",
    ]
    for row in family_summaries:
        md_lines.extend(
            [
                f"- `{row['family']}` proxy=`{row['executable_proxy']}` submit-capable=`{row['submit_capable_lane_count']}` quote=`{row['quote_mode']}` exposure=`{row['exposure_cap']}` actionable=`{len(row['current_actionable_lanes'])}`",
            ]
        )
    md_lines.extend(
        [
            "- `Rates` roadmap only; no routing implemented",
            "",
            "## Performance Governance",
            "",
            f"- top paper-probation candidates: `{', '.join(item['lane_id'] for item in top_candidates)}`",
            f"- KILL_CANDIDATE lanes: `{', '.join(item['lane_id'] for item in kill_candidates)}`",
            f"- strategies with recent local-only trades: `{len(recent_local_only)}`",
            f"- strategies with broker-path trades: `{len(broker_path_trade_rows)}`",
            f"- readiness buckets: `{dict(ready_counts)}`",
            f"- live-money tracker counts: `{dict(Counter(row['live_money_readiness_class'] for row in live_money_tracker_rows))}`",
            "",
            "## Notes",
            "",
            "- delayed quote modes are treated as paper-only pricing inputs, not live market data",
            "- internal-only performance remains separated from broker-path paper performance",
            "- no order was placed in this report pass",
        ]
    )
    (output_dir / "ibkr_paper_strategy_stack_readiness_report.md").write_text("\n".join(md_lines) + "\n")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate consolidated IBKR paper strategy readiness report.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    summary = generate_report(args.output_dir)
    print(f"classification={summary['classification']}")
    print(f"lane_count={len(summary['strategy_lanes'])}")
    print(f"top_probation_count={len(summary['performance_governance']['top_paper_probation_candidates'])}")


if __name__ == "__main__":
    main()
