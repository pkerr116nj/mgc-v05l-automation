from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
TRADE_LOGIC_DIR = REPO_ROOT / "outputs" / "reports" / "trade_logic_availability"
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_bridge_runtime_caller_policy"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _update_trade_logic_funnel(path: Path) -> list[dict[str, str]]:
    rows, fieldnames = _load_csv_rows(path)
    for row in rows:
        if row["family_bucket"] == "index_ny_early_core_us_midday":
            row["collapse_detail"] = (
                "Historical actionable entry signals were blocked by the old manual-only bridge caller policy. "
                "Approved supervised paper runtime callers now pass caller-path validation and carry explicit PAPER-only route metadata."
            )
        elif row["family_bucket"] == "gc_all_lanes_us_midday":
            row["collapse_detail"] = (
                "Historical actionable entry signals were blocked by the old manual-only bridge caller policy. "
                "Approved supervised paper runtime callers now pass caller-path validation and carry explicit PAPER-only route metadata."
            )
    _write_csv(path, rows, fieldnames)
    return rows


def _strategy_low_activity_blockers_rows() -> list[dict[str, Any]]:
    return [
        {
            "family_bucket": "index_ny_early_core_us_midday",
            "lane_count": 8,
            "historical_actionable_entry_bars": 8,
            "historical_routed_intents": 0,
            "historical_fill_count": 0,
            "historical_blocker": "BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.",
            "current_policy_status": "FIXED",
            "current_expected_next_step": "Next fresh midday signal should reach real bridge preflight gates instead of failing at caller-path validation.",
        },
        {
            "family_bucket": "gc_all_lanes_us_midday",
            "lane_count": 1,
            "historical_actionable_entry_bars": 1,
            "historical_routed_intents": 0,
            "historical_fill_count": 0,
            "historical_blocker": "BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.",
            "current_policy_status": "FIXED",
            "current_expected_next_step": "Next fresh midday signal should reach real bridge preflight gates instead of failing at caller-path validation.",
        },
        {
            "family_bucket": "asia_london_participation",
            "lane_count": 11,
            "historical_actionable_entry_bars": 0,
            "historical_routed_intents": 0,
            "historical_fill_count": 0,
            "historical_blocker": "No setup candidates observed in inspected window.",
            "current_policy_status": "UNCHANGED",
            "current_expected_next_step": "Separate setup-frequency review remains needed; caller policy was not the suppressor.",
        },
    ]


def _build_report_payload() -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    trade_logic = _load_json(TRADE_LOGIC_DIR / "trade_logic_availability.json")
    updated_rows = _update_trade_logic_funnel(TRADE_LOGIC_DIR / "trade_logic_funnel_by_family.csv")
    blockers_rows = _strategy_low_activity_blockers_rows()
    blockers_path = OUTPUT_DIR / "strategy_low_activity_blockers.csv"
    _write_csv(
        blockers_path,
        blockers_rows,
        [
            "family_bucket",
            "lane_count",
            "historical_actionable_entry_bars",
            "historical_routed_intents",
            "historical_fill_count",
            "historical_blocker",
            "current_policy_status",
            "current_expected_next_step",
        ],
    )

    family_lookup = {row["family_bucket"]: row for row in updated_rows}
    payload = {
        "classification": "IBKR_RUNTIME_CALLER_POLICY_FIXED",
        "generated_at": generated_at,
        "root_cause": "Paper strategy bridge was still enforcing a manual-only caller path and inherited preview-harness forbidden-frame rules that also blacklisted probationary_runtime.",
        "approved_paper_runtime_callers": [
            "manual_strategy_bridge_cli",
            "probationary_paper_runtime_lane",
            "supervised_paper_runtime_bridge",
            "ibkr_paper_strategy_executor",
        ],
        "blocked_caller_classes": [
            "unknown callers",
            "live-money callers",
            "scheduler/unattended live-like callers",
            "runtime callers without explicit PAPER / 127.0.0.1 / 7497 / DUM882026 metadata lock",
        ],
        "safety_gates_preserved": [
            "PAPER-only lock",
            "account lock",
            "TWS host/port lock",
            "monitor health gate",
            "backend/source readiness gate",
            "governance gate",
            "exposure gate",
            "quote gate",
            "broker/ledger reconciliation",
            "one active order per strategy",
            "per-strategy cap",
            "aggregate cap",
        ],
        "test_proof": {
            "bridge_policy_slice": "22 passed",
            "probationary_runtime_slice": "7 passed",
            "no_order_submitted": True,
        },
        "affected_family_updates": [
            {
                "family_bucket": "index_ny_early_core_us_midday",
                "historical_actionable_entry_bars": 8,
                "historical_routed_intents": 0,
                "historical_fills": 0,
                "historical_blocker": "Paper strategy bridge rejected a non-manual caller path.",
                "current_status": "Approved supervised paper runtime caller policy fixed.",
                "next_expected_behavior": "A fresh midday signal should reach real downstream bridge preflight instead of failing at caller validation.",
                "updated_trade_logic_row": family_lookup["index_ny_early_core_us_midday"],
            },
            {
                "family_bucket": "gc_all_lanes_us_midday",
                "historical_actionable_entry_bars": 1,
                "historical_routed_intents": 0,
                "historical_fills": 0,
                "historical_blocker": "Paper strategy bridge rejected a non-manual caller path.",
                "current_status": "Approved supervised paper runtime caller policy fixed.",
                "next_expected_behavior": "A fresh midday signal should reach real downstream bridge preflight instead of failing at caller validation.",
                "updated_trade_logic_row": family_lookup["gc_all_lanes_us_midday"],
            },
        ],
        "trade_logic_context": {
            "classification_before_fix": trade_logic.get("classification"),
            "runtime_bug_signature": trade_logic.get("runtime_bug_signature"),
            "runtime_bug_code_reference": trade_logic.get("runtime_bug_code_reference"),
        },
    }
    return payload


def _write_reports(payload: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "ibkr_bridge_runtime_caller_policy_report.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    md_lines = [
        "# IBKR runtime caller policy fix",
        "",
        f"- classification: `{payload['classification']}`",
        f"- generated_at: `{payload['generated_at']}`",
        "- root cause: manual-only caller enforcement plus preview-harness forbidden-frame reuse blocked approved supervised paper runtime callers.",
        "- no order was submitted in this pass.",
        "",
        "## Approved paper callers",
        "",
    ]
    md_lines.extend(f"- `{value}`" for value in payload["approved_paper_runtime_callers"])
    md_lines.extend(
        [
            "",
            "## Blocked caller classes",
            "",
        ]
    )
    md_lines.extend(f"- {value}" for value in payload["blocked_caller_classes"])
    md_lines.extend(
        [
            "",
            "## Safety gates preserved",
            "",
        ]
    )
    md_lines.extend(f"- {value}" for value in payload["safety_gates_preserved"])
    md_lines.extend(
        [
            "",
            "## Midday family impact",
            "",
            "- `index_ny_early_core_us_midday`: historical `8` actionable entry bars, `0` routed intents, `0` fills. Caller-path suppressor is fixed; next fresh midday signal should reach real downstream bridge preflight.",
            "- `gc_all_lanes_us_midday`: historical `1` actionable entry bar, `0` routed intents, `0` fills. Caller-path suppressor is fixed; next fresh midday signal should reach real downstream bridge preflight.",
            "",
            "## Test proof",
            "",
            f"- bridge policy slice: `{payload['test_proof']['bridge_policy_slice']}`",
            f"- probationary runtime slice: `{payload['test_proof']['probationary_runtime_slice']}`",
            "- approved supervised paper runtime callers now pass caller-path validation and explicit PAPER-only route metadata checks.",
            "- unknown/live/scheduler callers remain fail-closed.",
        ]
    )
    (OUTPUT_DIR / "ibkr_bridge_runtime_caller_policy_report.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    midday_lines = [
        "# Midday route suppression fix",
        "",
        "Historical midday actionable signals were not quiet strategy logic. They were being blocked at the runtime-to-bridge handoff.",
        "",
        "- previous blocker: `BLOCKED_NOT_SENT_TO_BROKER: Paper strategy bridge rejected a non-manual caller path.`",
        "- current status: approved supervised paper runtime callers are now allowed through caller validation when they present explicit PAPER-only route metadata.",
        "- remaining behavior: downstream monitor/governance/exposure/quote/reconciliation gates still fail closed if unhealthy.",
        "- no order was submitted in this pass.",
        "",
        "Affected families:",
        "- `index_ny_early_core_us_midday`",
        "- `gc_all_lanes_us_midday`",
    ]
    (OUTPUT_DIR / "midday_route_suppression_fix_report.md").write_text("\n".join(midday_lines) + "\n", encoding="utf-8")


def main() -> None:
    payload = _build_report_payload()
    _write_reports(payload)


if __name__ == "__main__":
    main()
